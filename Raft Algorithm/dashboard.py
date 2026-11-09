"""Local HTTP dashboard; uses the existing gRPC service for actual node state."""
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import threading
import time
import webbrowser


def run_dashboard(nodes, processes, launch_node, port=8080, open_browser=True):
    import grpc
    import raft_pb2
    import raft_pb2_grpc
    from Client import Client

    client = Client(nodes=nodes)
    action_lock = threading.Lock()
    command_lock = threading.Lock()
    channels = [grpc.insecure_channel(address) for address in nodes]
    stubs = [raft_pb2_grpc.RaftStub(channel) for channel in channels]
    pool = ThreadPoolExecutor(max_workers=len(nodes))
    started = time.monotonic()

    def node_status(node_id):
        base = {"id": node_id, "address": nodes[node_id], "online": False,
                "process_running": processes[node_id].poll() is None}
        if not base["process_running"]:
            return base
        try:
            response = stubs[node_id].ServeClient(
                raft_pb2.ServeClientArgs(Request="STATUS"), timeout=0.6)
            base.update(json.loads(response.Data))
            base["online"] = True
        except (grpc.RpcError, ValueError):
            pass
        return base

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, status, payload, content_type="application/json"):
            if content_type == "application/json":
                payload = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type + "; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path == "/":
                self.send(200, (Path(__file__).parent / "dashboard.html").read_bytes(), "text/html")
            elif self.path == "/api/status":
                self.send(200, {"nodes": list(pool.map(node_status, range(len(nodes)))),
                                "majority": len(nodes) // 2 + 1,
                                "uptime": round(time.monotonic() - started)})
            else:
                self.send(404, {"error": "Not found"})

        def do_POST(self):
            # Browser writes must originate from this local dashboard.
            origin = self.headers.get("Origin")
            expected = f"http://127.0.0.1:{self.server.server_port}"
            if origin and origin != expected:
                self.send(403, {"error": "Use the local dashboard URL"})
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
                if not 0 < length <= 4096:
                    raise ValueError("Request body must be from 1 to 4096 bytes")
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise ValueError("Expected a JSON object")
                if self.path == "/api/command":
                    operation, key, value = body.get("operation"), body.get("key"), body.get("value")
                    if operation not in ("SET", "GET"):
                        raise ValueError("Choose SET or GET")
                    if not isinstance(key, str) or len(key.split()) != 1:
                        raise ValueError("Enter a single-word key")
                    if operation == "SET" and (not isinstance(value, str) or len(value.split()) != 1):
                        raise ValueError("Enter a single-word value")
                    command = f"{operation} {key.strip()}"
                    if operation == "SET":
                        command += " " + value.strip()
                    with command_lock:
                        response = client.sendRequest(command)
                    self.send(200, {"message": response.Data, "leader": response.LeaderID})
                elif self.path == "/api/node":
                    node_id, action = body.get("id"), body.get("action")
                    if type(node_id) is not int or not 0 <= node_id < len(nodes):
                        raise ValueError("Invalid node ID")
                    if action not in ("stop", "restart"):
                        raise ValueError("Choose stop or restart")
                    with action_lock:
                        process = processes[node_id]
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait()
                        if action == "restart":
                            launch_node(node_id)
                    self.send(200, {"message": f"Node {node_id} {action} requested"})
                elif self.path == "/api/shutdown":
                    self.send(200, {"message": "Cluster stopped. You can close this tab."})
                    threading.Thread(target=self.server.shutdown, daemon=True).start()
                else:
                    self.send(404, {"error": "Not found"})
            except (ValueError, TypeError) as error:
                self.send(400, {"error": str(error)})
            except TimeoutError as error:
                self.send(503, {"error": str(error)})
            except OSError as error:
                self.send(500, {"error": str(error)})

    server = None
    try:
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        # Finish any active control request before the launcher cleans up children.
        server.daemon_threads = False
        url = f"http://127.0.0.1:{server.server_port}"
        print(f"Dashboard: {url}\nPress Ctrl+C or use Stop cluster in the dashboard to exit.")
        if open_browser:
            webbrowser.open(url)
        server.serve_forever(poll_interval=0.2)
    finally:
        if server is not None:
            server.server_close()
        pool.shutdown(wait=True)
        for channel in channels + client.channels:
            channel.close()
