import grpc
import raft_pb2
import raft_pb2_grpc
import random
import time
from cluster_config import addresses, base_port, node_count

nodeAddresses = addresses()


class Client:
    def __init__(self, clientAddress=None, nodes=None):
        self.clientAddress = clientAddress
        self.leader = None
        self.client_id = str(random.randint(1, 1000))
        self.nodes = nodeAddresses if nodes is None else nodes
        self.channels = [grpc.insecure_channel(node) for node in self.nodes]
        self.stubs = [raft_pb2_grpc.RaftStub(
            channel) for channel in self.channels]

    def letsGoo(self):
        print("Commands: SET key value | GET key | EXIT")
        try:
            while True:
                query = input("> ").strip()
                if query.upper() in {"EXIT", "QUIT"}:
                    break
                parts = query.split()
                if not parts:
                    continue
                parts[0] = parts[0].upper()
                if not ((parts[0] == "SET" and len(parts) == 3) or
                        (parts[0] == "GET" and len(parts) == 2)):
                    print("Use SET key value or GET key (single-word keys and values)")
                    continue
                try:
                    print(self.sendRequest(" ".join(parts)).Data)
                except (TimeoutError, ValueError) as error:
                    print(error)
        except (EOFError, KeyboardInterrupt):
            print("\nClient stopped.")
        finally:
            for channel in self.channels:
                channel.close()

    def get(self, key):
        return self.sendRequest(f"GET {key}")

    def set(self, key, value):
        return self.sendRequest(f"SET {key} {value}")

    def sendRequest(self, request, timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            order = list(range(len(self.stubs)))
            if self.leader is not None:
                order.remove(self.leader)
                order.insert(0, self.leader)
            for peer in order:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    response = self.stubs[peer].ServeClient(
                        raft_pb2.ServeClientArgs(Request=request), timeout=min(5, remaining))
                    known = response.LeaderID
                    self.leader = int(known) if known in {str(i) for i in range(len(self.stubs))} else None
                    if response.Success:
                        return response
                except grpc.RpcError as error:
                    if error.code() == grpc.StatusCode.INVALID_ARGUMENT:
                        raise ValueError(error.details()) from error
                    if self.leader == peer:
                        self.leader = None
            time.sleep(0.1)
        raise TimeoutError("No majority/leader available. A timed-out SET may still commit.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Connect to a local Raft cluster")
    parser.add_argument("--nodes", type=node_count, default=5)
    parser.add_argument("--base-port", type=base_port, default=50051)
    args = parser.parse_args()
    client = Client(nodes=addresses(args.nodes, args.base_port))
    client.letsGoo()
