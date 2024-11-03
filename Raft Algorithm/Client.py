import grpc
import raft_pb2
import raft_pb2_grpc
import random

nodeAddresses = []
for i in range(5):
    nodeAddresses.append(f"localhost:{50051 + i}")


class Client:
    def __init__(self, clientAddress):
        self.clientAddress = clientAddress
        self.leader = None
        self.client_id = str(random.randint(1, 1000))
        self.channels = [grpc.insecure_channel(node) for node in nodeAddresses]
        self.stubs = [raft_pb2_grpc.RaftStub(
            channel) for channel in self.channels]

    def letsGoo(self):
        while True:
            operation, key, value = self.userInput()
            if operation == "GET":
                response = self.get(key)
                print(response.Data)
            elif operation == "SET":
                response = self.set(key, value)
                print(response.Data)
            else:
                print("Invalid input format.")

    def userInput(self):
        print("Enter your query")
        input_string = input().split()
        operation = input_string[0]
        key = input_string[1] if len(input_string) > 1 else None
        value = input_string[2] if len(input_string) == 3 else None
        return operation, key, value

    def get(self, key):
        return self.sendRequest(f"GET {key}")

    def set(self, key, value):
        return self.sendRequest(f"SET {key} {value}")

    def sendRequest(self, request):
        while True:
            response = self.sendReqToLeader(request)
            if response:
                return response

    def sendReqToLeader(self, request):
        if self.leader is None:
            return self.findLeaderAndSendReq(request)
        return self.sendReqToCurrLeader(request)

    def findLeaderAndSendReq(self, request):
        for i, stub in enumerate(self.stubs):
            try:
                response = stub.ServeClient(
                    raft_pb2.ServeClientArgs(Request=request))
                if response.Success:
                    self.leader = str(response.LeaderID)
                    print("Leader is: ", self.leader)
                    return response
                elif response.LeaderID:
                    self.leader = response.LeaderID
                    break
            except grpc.RpcError:
                # print(f"Unable to connect to the node: {i}, it may be down")
                pass
        return None

    def sendReqToCurrLeader(self, request):
        try:
            response = self.stubs[int(self.leader)].ServeClient(
                raft_pb2.ServeClientArgs(Request=request))
            if response.Success:
                return response
            else:
                self.leader = None if not response.LeaderID else response.LeaderID
        except grpc.RpcError:
            self.leader = None
        return None


if __name__ == "__main__":
    client = Client("localhost:50050")
    client.letsGoo()
