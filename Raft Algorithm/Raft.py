# Importing the required libraries
import grpc
import raft_pb2
import raft_pb2_grpc
import time
import random
import os
import threading
from concurrent import futures
from pathlib import Path
from cluster_config import addresses, base_port, node_count

# Node addresses
nodeAddress = addresses()


# Raft Node Class
class Raft(raft_pb2_grpc.RaftServicer):

    # Constructor
    def __init__(self, id, curr_node_address, nodes, data_dir="."):

        # Node identity and network information
        self.id = id
        self.curr_node_address = curr_node_address
        self.nodes = nodes

        # Term and election attributes
        self.currentTerm = 0
        self.votedFor = None
        self.voteCount = 0
        self.votesReceived = {}
        self.stateLock = threading.RLock()
        self.changed = threading.Condition(self.stateLock)
        self.peerLocks = {node: threading.Lock() for node in nodes}

        # Role and leader attributes
        self.currentRole = "Follower"
        self.leaderID = None
        self.leaderLease = 0

        # Timing and timeout attributes
        self.electionDeadline = time.monotonic() + random.uniform(5, 10)
        self.heartbeatTimeout = 1
        self.leaderLeaseTimeout = 5

        # Log management
        self.logs = []
        self.commitLength = 0
        self.sentLength = {}
        self.ackedLength = {}
        self.values = {}
        self.appliedLength = 0
        self.directory = Path(data_dir) / f"logs_node_{self.id}"
        self.logFile = str(self.directory / "logs.txt")

        # Dump management
        self.dump = []
        self.dumpFile = str(self.directory / "dump.txt")

        # Metadata and filesystem initialization
        self.metadata = str(self.directory / "metadata.txt")
        os.makedirs(os.path.dirname(self.logFile), exist_ok=True)

        # Initialize sent and acked lengths for all nodes
        for node in self.nodes:
            self.sentLength[node] = len(self.logs)
            self.ackedLength[node] = len(self.logs)

    # Function to run the election timer
    def startElectionTimer(self):
        while True:
            time.sleep(0.1)
            with self.stateLock:
                if (self.currentRole != "Leader" and
                        time.monotonic() >= self.electionDeadline):
                    print(f"Node {self.id}: starting election")
                    self.election()

    def election(self):
        with self.stateLock:
            self.initElection()
            last_log_index, last_log_term = self.lastLogInfo()
            self.sendVoteReqToAllNodes(last_log_index, last_log_term)
            self.resetElectionTimer()

    # Function to initialize the election state
    def initElection(self):
        with self.stateLock:
            self.currentTerm += 1
            self.currentRole = "Candidate"
            self.votedFor = self.id
            self.votesReceived = {self.id}
            self.voteCount = 1
            self.saveMetadata()
            # A one-node cluster already has its majority after voting for itself.
            if self.voteCount >= len(self.nodes) // 2 + 1:
                self.roleLeader()

    # Function to get the last log index and term
    def lastLogInfo(self):
        last_log_index = len(self.logs)
        last_log_term = self.logs[-1].term if self.logs else 0
        return last_log_index, last_log_term

    # Function to send vote requests to all other nodes
    def sendVoteReqToAllNodes(self, last_log_index, last_log_term):
        msg = (self.id, self.currentTerm, last_log_index, last_log_term)
        for node in self.nodes:
            if node != self.curr_node_address:
                threading.Thread(target=self.reqVote,
                                 args=(node, msg), daemon=True).start()

    # Function to request vote from a node
    def reqVote(self, node, msg):
        try:
            response = self.sendVoteReqToANode(node, msg)
            self.voteResponseHandler(response, node, msg[1])
        except grpc.FutureTimeoutError:
            print("Timeout occurred!")
        except grpc.RpcError:
            print(f"Node {node} is unavailable.")

    # Function to send vote request to a node
    def sendVoteReqToANode(self, node, msg):
        with grpc.insecure_channel(node) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            return stub.RequestVote(raft_pb2.RequestVoteArgs(
                term=msg[1], candidate_id=msg[0], last_log_index=msg[2],
                last_log_term=msg[3]), timeout=1)

    # Function to handle vote response
    def RequestVote(self, request, context):
        with self.stateLock:
            print(f"RequestVote received from {
                  request.candidate_id} for term {request.term}")
            if request.term > self.currentTerm:
                self.roleFollower(request.term)
            elif request.term < self.currentTerm:
                self.voteDenial(request)
                return self.voteReply(False)

            if self.voteGrantCheck(request):
                self.votedFor = request.candidate_id
                self.saveMetadata()
                self.resetElectionTimer()
                self.voteGrant(request)
                return self.voteReply(True)
            else:
                self.voteDenial(request)
                return self.voteReply(False)

    # Function to log and append vote denial
    def voteDenial(self, request):
        message = f"Vote denied for Node {
            request.candidate_id} in term {request.term}."
        print(message)
        self.dump.append(message)

    # Function to log and append vote grant
    def voteGrant(self, request):
        message = f"Vote granted for Node {
            request.candidate_id} in term {request.term}."
        print(message)
        self.dump.append(message)

    # Function to check if vote should be granted
    def voteGrantCheck(self, request):
        last_log_term = self.logs[-1].term if self.logs else 0
        log_ok = (request.last_log_term > last_log_term) or \
            (request.last_log_term ==
             last_log_term and request.last_log_index >= len(self.logs))
        return request.term == self.currentTerm and log_ok and \
            (self.votedFor is None or self.votedFor == request.candidate_id)

    # Function to create vote reply
    def voteReply(self, vote_granted):
        return raft_pb2.RequestVoteReply(term=self.currentTerm, vote_granted=vote_granted, remaining_lease=self.leaderLease)

    # Function to handle vote response (need to change a bit)
    def voteResponseHandler(self, response, node, requested_term):
        with self.stateLock:
            if response.term > self.currentTerm:
                self.roleFollower(response.term)
            elif (self.currentRole == "Candidate" and
                  response.term == self.currentTerm == requested_term and response.vote_granted):
                self.votesReceived.add(node)
                self.voteCount = len(self.votesReceived)
                if self.voteCount >= len(self.nodes) // 2 + 1:
                    self.roleLeader()

    # Function to set the node's role to follower
    def roleFollower(self, term=None):
        with self.stateLock:

            if self.currentRole == "Leader":
                print(f"{self.id} is stepping down from leadership.")
                self.dump.append(f"{self.id} is stepping down from leadership.")

            self.currentRole = "Follower"
            if term is not None and term > self.currentTerm:
                self.currentTerm = term
                self.votedFor = None
                self.saveMetadata()
            self.leaderID = None
            self.resetElectionTimer()

    # Function to set the node's role to leader
    def roleLeader(self):
        with self.stateLock:
            print(f"Node {self.id} has become the leader for term {
                  self.currentTerm}.")
            self.dump.append(f"Node {self.id} has become the leader for term {
                             self.currentTerm}.")

            self.currentRole = "Leader"
            self.leaderID = self.id
            self.leaderLease = self.leaderLeaseTimeout
            self.electionTimerInifinity()

            for follower in self.nodes:
                if follower != self.curr_node_address:
                    self.sentLength[follower] = len(self.logs)
                    self.ackedLength[follower] = 0
            self.broadcast("NO-OP")

    # Function to reset the election timeout (need to change a bit)
    def resetElectionTimer(self):
        self.electionDeadline = time.monotonic() + random.uniform(5, 10)

    # Function to cancel the election timer (need to change a bit)
    def electionTimerInifinity(self):
        self.electionDeadline = float('inf')

    # Function to handle client requests
    def ServeClient(self, request, context):
        if request.Request == "STATUS":
            import json
            with self.stateLock:
                snapshot = {
                    "id": self.id, "address": self.curr_node_address,
                    "role": self.currentRole, "term": self.currentTerm,
                    "leader": self.leaderID, "voted_for": self.votedFor,
                    "commit_length": self.commitLength, "log_length": len(self.logs),
                    "values": dict(self.values),
                    "logs": [{"index": entry.index, "term": entry.term,
                              "operation": entry.operation,
                              "committed": offset < self.commitLength}
                             for offset, entry in enumerate(self.logs)
                             if offset >= max(0, len(self.logs) - 20)],
                }
                return self.serveClientReplyHandler(json.dumps(snapshot), True)
        parts = request.Request.split()
        if not ((len(parts) == 3 and parts[0] == "SET") or
                (len(parts) == 2 and parts[0] == "GET")):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Use SET key value or GET key")
        with self.stateLock:
            if self.currentRole == "Leader":
                return self.serveClientForLeader(request, context)
            return self.serveClientForFollower()

    # Function to handle client requests as a leader
    def serveClientForLeader(self, request, context):
        request_formatted = request.Request.split()[0]
        print(f"Node {self.id} (leader) received an {
              request.Request} request.")
        self.dump.append(f"Node {self.id} (leader) received an {
                         request.Request} request.")

        if request_formatted == "SET":
            return self.handleSetReq(request, context)
        elif request_formatted == "GET":
            return self.handleGetReq(request, context)

    # Function to process a SET request
    def waitForCommit(self, operation, context):
        term = self.currentTerm
        self.broadcast(operation)
        target = len(self.logs)
        deadline = time.monotonic() + min(4.0, context.time_remaining() or 4.0)
        while self.commitLength < target:
            if self.currentRole != "Leader" or self.currentTerm != term:
                return False
            if not context.is_active() or time.monotonic() >= deadline:
                context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,
                              "No majority; a timed-out SET may still commit")
            self.changed.wait(timeout=0.1)
        return self.currentRole == "Leader" and self.currentTerm == term

    def handleSetReq(self, request, context):
        if self.waitForCommit(request.Request, context):
            return self.serveClientReplyHandler("SET request processed", True)
        return self.serveClientReplyHandler("Leader changed", False)

    def handleGetReq(self, request, context):
        if not self.waitForCommit("NO-OP", context):
            return self.serveClientReplyHandler("Leader changed", False)
        value = self.values.get(request.Request.split()[1])
        return self.serveClientReplyHandler(
            "Key not found" if value is None else f"Value: {value}", True)

    # Function to create a serve client reply
    def serveClientReplyHandler(self, data, success, value=None):
        stringleader = "" if self.leaderID is None else str(self.leaderID)
        return raft_pb2.ServeClientReply(Data=data, LeaderID=stringleader, Success=success)

    # Function to respond to client requests as a follower
    def serveClientForFollower(self):
        stringleader = "" if self.leaderID is None else str(self.leaderID)
        if self.leaderID is not None:
            return self.serveClientReplyHandler("Forwarding request to leader", False)
        else:
            return self.serveClientReplyHandler("Leader not known", False)

    # Function to broadcast a message to all nodes
    def broadcast(self, msg):
        if self.currentRole == "Leader":
            self.broadcastAsLeader(msg)

    # Function to broadcast a message as a leader
    def broadcastAsLeader(self, msg):
        # Append the new log entry for the leader node
        log_entry = raft_pb2.LogEntry(
            term=self.currentTerm, operation=msg, index=len(self.logs) + 1)
        self.logs.append(log_entry)
        self.saveLogs()
        self.ackedLength[self.curr_node_address] = len(self.logs)
        # Also commit locally when there are no followers (cluster size one).
        self.commitLogEntries()

        # Replicate the log entry to all followers
        for follower in self.nodes:
            if follower != self.curr_node_address:  # Skip the leader itself
                threading.Thread(target=self.replicateLog,
                                 args=(follower,), daemon=True).start()

    # Function to replicate log entries to a follower
    def replicateLog(self, follower):
        if not self.peerLocks[follower].acquire(blocking=False):
            return
        try:
            with self.stateLock:
                if self.currentRole != "Leader":
                    return
                term = self.currentTerm
                prefixLen, prefixTerm, suffix = self.prepLogEntries(follower)
            self.sendLogEntries(follower, prefixLen, prefixTerm, suffix, term)
        finally:
            self.peerLocks[follower].release()

    # Function to prepare log entries to be sent to a follower
    def prepLogEntries(self, follower):
        prefixLen = self.sentLength[follower]
        suffix = self.logs[prefixLen:]  # The log entries to be sent
        prefixTerm = self.logs[prefixLen - 1].term if prefixLen > 0 else 0
        return prefixLen, prefixTerm, suffix

    # Function to send log entries to a follower
    def sendLogEntries(self, follower, prefixLen, prefixTerm, suffix, term):
        channel = grpc.insecure_channel(follower)
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
            future = stub.AppendEntries.future(
                raft_pb2.AppendEntriesArgs(
                    term=term,
                    leader_id=self.id,
                    prev_log_index=prefixLen - 1,
                    prev_log_term=prefixTerm,
                    entries=suffix,
                    leader_commit=self.commitLength,
                    remaining_lease=self.leaderLease
                )
            )
            response = future.result(timeout=1)  # Timeout in seconds
            with self.stateLock:
                if response.term > self.currentTerm:
                    self.roleFollower(response.term)
                elif self.currentRole == "Leader" and self.currentTerm == term:
                    if response.success:
                        self.sentLength[follower] = prefixLen + len(suffix)
                        self.ackedLength[follower] = prefixLen + len(suffix)
                        self.commitLogEntries()
                        self.leaderLease = self.leaderLeaseTimeout
                    else:
                        self.sentLength[follower] = max(0, prefixLen - 1)
        except grpc.FutureTimeoutError:
            self.errorHandlerTimeoutLog(follower)
        except grpc.RpcError:
            pass
        finally:
            channel.close()

    # Function to print timeout errors
    def errorHandlerTimeoutLog(self, follower):
        print(f"Timeout occurred while sending RPC to Node {follower}.")
        self.dump.append(
            f"Timeout occurred while sending RPC to Node {follower}.")

    # Function to handle append entries
    def AppendEntries(self, request, context):
        with self.stateLock:
            print(f"AppendEntries received from {request.leader_id}")

            if self.checkAndUpdateDetailsAppendEntries(request):
                self.resetElectionTimer()

                if self.checkLog(request):
                    self.updateLog(request)

                    return self.appendEntriesReplyHandler(True)
                else:
                    return self.appendEntriesReplyHandler(False)
            else:
                return self.appendEntriesReplyHandler(False)

    # Function to check and update details for append entries
    def checkAndUpdateDetailsAppendEntries(self, request):
        if request.term > self.currentTerm:
            self.roleFollower(request.term)
            self.leaderID = request.leader_id
            return True
        elif request.term < self.currentTerm:
            self.rejectEntryLog(request.leader_id)
            return False
        self.currentRole = "Follower"
        self.leaderID = request.leader_id
        return True

    # Function to check the log for append entries
    def checkLog(self, request):
        prefixLen = request.prev_log_index + 1
        logOk = (0 <= prefixLen <= len(self.logs)) and \
                (prefixLen ==
                 0 or self.logs[prefixLen - 1].term == request.prev_log_term)
        if not logOk:
            self.rejectEntryLog(request.leader_id)
        return logOk

    # Function to update the log for append entries
    def updateLog(self, request):
        prefixLen = request.prev_log_index + 1
        self.appendEntriesHelper(
            prefixLen, request.leader_commit, request.entries)
        self.leaderLease = request.remaining_lease

    # Function to create an append entries reply
    def appendEntriesReplyHandler(self, success):
        return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=success)

    # Function to rejected entries in log
    def rejectEntryLog(self, leader_id):
        message = f"Node {
            self.id} rejected AppendEntries RPC from {leader_id}."
        print(message)
        self.dump.append(message)

    # Function to help append entries
    def appendEntriesHelper(self, prefixLen, leaderCommit, suffix):
        self.removeConflicts(prefixLen, suffix)
        self.addEntries(prefixLen, suffix)

        matched = prefixLen + len(suffix)
        if leaderCommit > self.commitLength:
            self.commitLength = max(self.commitLength, min(leaderCommit, matched))
            self.deliverLogEntries()
        self.saveLogs()

    # Function to remove conflicts in log
    def removeConflicts(self, prefixLen, suffix):
        if suffix:
            startIndex = prefixLen
            endIndex = min(startIndex + len(suffix), len(self.logs))
            for i in range(startIndex, endIndex):
                if self.logs[i].term != suffix[i - prefixLen].term:
                    if i < self.commitLength:
                        raise ValueError("Refusing to overwrite a committed entry")
                    self.logs = self.logs[:i]
                    break

    # Function to add entries to log
    def addEntries(self, prefixLen, suffix):
        for entry in suffix[len(self.logs) - prefixLen:]:
            self.logs.append(entry)
            print(f"Node {self.id} accepted AppendEntries RPC from {
                  self.leaderID}.")
            self.dump.append(f"Node {self.id} accepted AppendEntries RPC from {
                             self.leaderID}.")

    # Function to deliver log entries
    def deliverLogEntries(self):
        for entry in self.logs[self.appliedLength:self.commitLength]:
            parts = entry.operation.split()
            if parts[0] == "SET":
                self.values[parts[1]] = parts[2]
        self.appliedLength = self.commitLength
        self.saveLogs()
        self.changed.notify_all()

    # Function to commit log entries
    def commitLogEntries(self):
        self.ackedLength[self.curr_node_address] = len(self.logs)
        matched = sorted(self.ackedLength.values(), reverse=True)
        target = matched[len(self.nodes) // 2]
        if target > self.commitLength and self.logs[target - 1].term == self.currentTerm:
            self.commitLength = target
            self.deliverLogEntries()

    # Function to handle heartbeat
    def handleHeartbeats(self):
        while True:
            time.sleep(self.heartbeatTimeout)
            if self.currentRole == "Leader":
                self.heartbeat()

    # Function to send heartbeat
    def heartbeat(self):
        print(f"Leader {self.id} sending heartbeat")
        self.dump.append(
            f"Leader {self.id} sending heartbeat")
        for follower in self.nodes:
            if follower != self.curr_node_address:
                threading.Thread(target=self.replicateLog,
                                 args=(follower,), daemon=True).start()

    # Function to save everything
    def saveEverything(self):
        self.saveMetadata()
        self.saveLogs()
        self.saveDump()
        print("Server stopped and all files persisted")

    # Function to save metadata
    def saveMetadata(self):
        self.saveLogs()

    def saveLogs(self):
        import json
        with self.stateLock:
            path = str(self.directory / "state.json")
            temporary = path + ".tmp"
            with open(temporary, "w", encoding="utf-8") as output:
                json.dump({"nodes": self.nodes, "term": self.currentTerm, "voted_for": self.votedFor,
                           "commit_length": self.commitLength,
                           "logs": [{"term": e.term, "operation": e.operation,
                                     "index": e.index} for e in self.logs]}, output)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)

    # Function to save dump
    def saveDump(self):
        with open(self.dumpFile, "w") as dump_file:
            for dump in self.dump:
                dump_file.write(f"{dump}\n")


# Function to serve the server
def serve():
    global nodeAddress
    print("Server started...")
    import argparse
    parser = argparse.ArgumentParser(description="Run one node in a local Raft cluster")
    parser.add_argument("--nodes", type=node_count, default=5)
    parser.add_argument("--base-port", type=base_port, default=50051)
    parser.add_argument("--data-dir", default=".")
    parser.add_argument("--id", type=int)
    args = parser.parse_args()
    nodeAddress = addresses(args.nodes, args.base_port)
    node_id = args.id if args.id is not None else int(input(f"Enter node id (0-{args.nodes - 1}): "))
    checkNodeId(node_id)
    raft_node = Raft(node_id, nodeAddress[node_id], nodeAddress, args.data_dir)
    getSavedData(raft_node)
    with raft_node.stateLock:
        raft_node.deliverLogEntries()

    server = startServergrpc(raft_node, node_id)
    backgroundTasks(raft_node)
    letsGoo(server, raft_node)


def checkNodeId(node_id):
    if node_id < 0 or node_id >= len(nodeAddress):
        print("Invalid node ID. Exiting.")
        raise SystemExit(1)


def startServergrpc(raft_node, node_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16),
                         options=(("grpc.so_reuseport", 0),))
    raft_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    if not server.add_insecure_port(nodeAddress[node_id]):
        raise RuntimeError(f"Cannot bind {nodeAddress[node_id]}")
    server.start()
    return server


def getSavedData(raft_node):
    import json
    path = str(raft_node.directory / "state.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as source:
            saved = json.load(source)
        if saved.get("nodes", nodeAddress) != raft_node.nodes:
            raise ValueError("Saved state belongs to a different cluster; use a separate --data-dir")
        raft_node.currentTerm = saved["term"]
        raft_node.votedFor = saved["voted_for"]
        raft_node.commitLength = saved["commit_length"]
        raft_node.logs = [raft_pb2.LogEntry(**entry) for entry in saved["logs"]]
    else:
        try:
            getLogData(raft_node)
            getMetadata(raft_node)
        except FileNotFoundError:
            print("No previous state found.")


def getLogData(raft_node):
    with open(raft_node.logFile, "r") as f:
        for line in f:
            term, operation = logLine(line)
            raft_node.logs.append(raft_pb2.LogEntry(
                term=term, operation=operation, index=len(raft_node.logs) + 1))


def logLine(line):
    line = line.strip().split()
    term = int(line[-1])
    operation = " ".join(line[:-1])
    return term, operation


def getMetadata(raft_node):
    with open(raft_node.metadata, "r") as f:
        line = f.readline().strip().split()
        raft_node.commitLength, raft_node.currentTerm, raft_node.votedFor, raft_node.leaderID = metadataLine(
            line)


def metadataLine(line):
    commitLength = int(line[0])
    currentTerm = int(line[1])
    votedFor = int(line[2]) if line[2] != "None" else None
    leaderID = int(line[3]) if line[3] != "None" else None
    return commitLength, currentTerm, votedFor, leaderID


def backgroundTasks(raft_node):
    threading.Thread(target=raft_node.startElectionTimer, daemon=True).start()
    threading.Thread(target=raft_node.handleHeartbeats, daemon=True).start()


def letsGoo(server, raft_node):
    print(f"Server started at {nodeAddress[raft_node.id]}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0).wait()
        raft_node.saveEverything()


if __name__ == "__main__":
    serve()
