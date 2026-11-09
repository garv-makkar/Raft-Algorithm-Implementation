"""Launch 1-9 Raft nodes and a live local web dashboard from one terminal."""
import argparse
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

from cluster_config import addresses, base_port, node_count


def choose_count():
    while True:
        try:
            return node_count(input("How many Raft nodes? (1-9): ").strip())
        except argparse.ArgumentTypeError as error:
            print(error)


def check_ports(count, port):
    # Fail before launching anything if an older cluster owns these ports.
    reserved = []
    try:
        for number in range(port, port + count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            reserved.append(sock)
            if os.name == "nt":
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            sock.bind(("127.0.0.1", number))
    except OSError as error:
        raise RuntimeError(f"Port {number} is unavailable. Stop the old cluster or use --base-port.") from error
    finally:
        for sock in reserved:
            sock.close()


def stop_nodes(processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run_cluster(count, port, *, web=False, dashboard_port=8080, open_browser=True):
    import grpc
    from Client import Client

    root = Path(__file__).resolve().parent
    data = root / ".raft" / f"cluster-{count}-{port}"
    check_ports(count, port)
    data.mkdir(parents=True, exist_ok=True)
    processes, output_files = [], []
    try:
        def launch_node(node_id):
            output = (data / f"node-{node_id}.console.log").open("a", encoding="utf-8")
            output_files.append(output)
            process = subprocess.Popen(
                [sys.executable, "-u", str(root / "Raft.py"), "--id", str(node_id),
                 "--nodes", str(count), "--base-port", str(port), "--data-dir", str(data)],
                cwd=root, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            if node_id < len(processes):
                processes[node_id] = process
            else:
                processes.append(process)

        for node_id in range(count):
            launch_node(node_id)
        print(f"Starting {count} nodes; majority = {count // 2 + 1}.")
        print(f"Node output and saved state: {data}")
        # Check both process health and gRPC readiness before accepting commands.
        deadline = time.monotonic() + 20
        for node_id, address in enumerate(addresses(count, port)):
            with grpc.insecure_channel(address) as channel:
                ready = grpc.channel_ready_future(channel)
                while True:
                    if any(process.poll() is not None for process in processes):
                        raise RuntimeError("A node failed to start; inspect its console.log file.")
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"Node {node_id} did not start; inspect its console.log file.")
                    try:
                        ready.result(timeout=0.2)
                        break
                    except grpc.FutureTimeoutError:
                        pass
        print("Nodes started. Leader election can take about 10 seconds.")
        if web:
            from dashboard import run_dashboard
            run_dashboard(addresses(count, port), processes, launch_node,
                          dashboard_port, open_browser)
        else:
            print("Type EXIT or press Ctrl+C to stop the entire cluster.")
            Client(nodes=addresses(count, port)).letsGoo()
    finally:
        stop_nodes(processes)
        for output in output_files:
            output.close()
        print("All launched nodes stopped. Saved state is retained.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=node_count, help="Number of nodes (1-9); prompts if omitted")
    parser.add_argument("--base-port", type=base_port, default=50051)
    parser.add_argument("--cli", action="store_true", help="Use the terminal client instead of the web dashboard")
    parser.add_argument("--dashboard-port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true", help="Print the dashboard URL without opening it")
    args = parser.parse_args()
    if not 0 <= args.dashboard_port <= 65535:
        parser.error("Dashboard port must be from 0 to 65535 (0 selects an available port)")
    try:
        count = args.nodes if args.nodes is not None else choose_count()
        run_cluster(count, args.base_port, web=not args.cli,
                    dashboard_port=args.dashboard_port, open_browser=not args.no_browser)
    except (EOFError, KeyboardInterrupt):
        print("\nStopped.")
    except ModuleNotFoundError as error:
        print(f"Missing dependency: {error.name}. Run: python -m pip install -r requirements.txt", file=sys.stderr)
        return 1
    except (OSError, RuntimeError) as error:
        print(f"Cannot start cluster: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
