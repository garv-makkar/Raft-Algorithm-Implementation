# Importing the required libraries
import grpc
import raft_pb2
import raft_pb2_grpc
import time
import random
import os
import threading
from concurrent import futures

# Node addresses
nodeAddress = []
for i in range(5):
    nodeAddress.append(f"localhost:{50051 + i}")


# Raft Node Class
class Raft(raft_pb2_grpc.RaftServicer):

    # Constructor
    def __init__(self, id, curr_node_address, nodes):

        # Node identity and network information
        self.id = id
        self.curr_node_address = curr_node_address
        self.nodes = nodes

        # Term and election attributes
        self.currentTerm = 0
        self.votedFor = None
        self.voteCount = 0
        self.votesReceived = {}
        self.voteLock = threading.Lock()

        # Role and leader attributes
        self.currentRole = "Follower"
        self.leaderID = None
        self.leaderLease = 0

        # Timing and timeout attributes
        self.electionTimeout = random.randint(5, 10)
        self.heartbeatTimeout = 1
        self.leaderLeaseTimeout = 5

        # Log management
        self.logs = []
        self.commitLength = 0
        self.sentLength = {}
        self.ackedLength = {}
        self.ackCount = set()
        self.logFile = f"./logs_node_{self.id}/logs.txt"
        self.logLock = threading.Lock()

        # Dump management
        self.dump = []
        self.dumpFile = f"./logs_node_{self.id}/dump.txt"

        # Metadata and filesystem initialization
        self.metadata = f"./logs_node_{self.id}/metadata.txt"
        os.makedirs(os.path.dirname(self.logFile), exist_ok=True)

        # Initialize sent and acked lengths for all nodes
        for node in self.nodes:
            self.sentLength[node] = len(self.logs)
            self.ackedLength[node] = len(self.logs)

    # Function to run the election timer
    def startElectionTimer(self):
        while True:
            if self.currentRole in ["Follower", "Candidate"]:
                time.sleep(1)
                self.electionTimeout -= 1
                if self.electionTimeout <= 0:
                    print(
                        f"Node {self.id} election timer timed out, starting election.")
                    self.dump.append(
                        f"Node {self.id} election timer timed out, starting election.")
                    self.election()

    # Function to run the leader lease timer
    def startLeaderLeaseTimer(self):
        while True:
            time.sleep(1)
            self.leaderLease -= 1
            if self.leaderLease <= 0:
                self.leaderLease = 0
                if self.currentRole == "Leader":
                    print(
                        f"Leader {self.id}'s lease renewal failed. Stepping down.")
                    self.dump.append(
                        f"Leader {self.id}'s lease renewal failed. Stepping down.")
                    self.roleFollower()

    # Function to start the election process
    def election(self):
        self.initElection()
        last_log_index, last_log_term = self.lastLogInfo()
        self.sendVoteReqToAllNodes(last_log_index, last_log_term)
        self.resetElectionTimer()

    # Function to initialize the election state
    def initElection(self):
        self.currentTerm += 1
        self.currentRole = "Candidate"
        self.votedFor = self.id
        self.votesReceived = {self.id}
        self.voteCount = 1

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
            self.voteResponseHandler(response)
        except grpc.FutureTimeoutError:
            print("Timeout occurred!")
        except Exception as e:
            print(f"No response from node: {node}. It is detected to be down.")

    # Function to send vote request to a node
    def sendVoteReqToANode(self, node, msg):
        channel = grpc.insecure_channel(node)
        stub = raft_pb2_grpc.RaftStub(channel)
        future = stub.RequestVote.future(raft_pb2.RequestVoteArgs(
            term=msg[1],
            candidate_id=msg[0],
            last_log_index=msg[2],
            last_log_term=msg[3]
        ))
        return future.result(timeout=1)

    # Function to handle vote response
    def RequestVote(self, request, context):
        print(f"RequestVote received from {
              request.candidate_id} for term {request.term}")
        if request.term > self.currentTerm:
            self.roleFollower(request.term)
        elif request.term < self.currentTerm:
            self.voteDenial(request)
            return self.voteReply(False)

        if self.voteGrantCheck(request):
            self.votedFor = request.candidate_id
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
    def voteResponseHandler(self, response):
        print(f"Vote response received: {response.vote_granted}")
        self.dump.append(f"Vote response received: {response.vote_granted}")

        if response.term > self.currentTerm:
            self.roleFollower(response.term)
        elif self.currentRole == "Candidate" and response.term == self.currentTerm and response.vote_granted:
            with self.voteLock:
                self.votesReceived.add(response.term)
                self.voteCount += 1
                self.leaderLease = max(
                    self.leaderLease, response.remaining_lease)
                if self.voteCount >= (len(self.nodes) + 1) // 2:
                    if self.leaderLease <= 0:
                        self.roleLeader()
                    else:
                        print("New Leader waiting for Old Leader Lease to timeout.")
                        self.dump.append(
                            "New Leader waiting for Old Leader Lease to timeout.")
                        start_time = time.time()
                        while time.time() - start_time < self.leaderLeaseTimeout:
                            time.sleep(1)
                        self.roleLeader()

    # Function to set the node's role to follower
    def roleFollower(self, term=None):

        if self.currentRole == "Leader":
            print(f"{self.id} is stepping down from leadership.")
            self.dump.append(f"{self.id} is stepping down from leadership.")

        self.currentRole = "Follower"
        self.currentTerm = term or self.currentTerm
        self.votedFor = None
        self.leaderID = None
        self.resetElectionTimer()

    # Function to set the node's role to leader
    def roleLeader(self):
        print(f"Node {self.id} has become the leader for term {
              self.currentTerm}.")
        self.dump.append(f"Node {self.id} has become the leader for term {
                         self.currentTerm}.")

        self.currentRole = "Leader"
        self.leaderID = self.id
        self.leaderLease = self.leaderLeaseTimeout
        self.ackCount.add(self.id)
        self.electionTimerInifinity()

        if not self.logs or self.logs[-1].term != self.currentTerm:
            self.broadcast("NO-OP")

        for follower in self.nodes:
            if follower != self.curr_node_address:
                self.sentLength[follower] = len(self.logs)
                self.ackedLength[follower] = 0
                threading.Thread(target=self.replicateLog,
                                 args=(follower,), daemon=True).start()

    # Function to reset the election timeout (need to change a bit)
    def resetElectionTimer(self):
        self.electionTimeout = random.randint(5, 10)

    # Function to cancel the election timer (need to change a bit)
    def electionTimerInifinity(self):
        self.electionTimeout = float('inf')

    # Function to handle client requests
    def ServeClient(self, request, context):
        if self.currentRole == "Leader":
            return self.serveClientForLeader(request)
        else:
            return self.serveClientForFollower()

    # Function to handle client requests as a leader
    def serveClientForLeader(self, request):
        request_formatted = request.Request.split()[0]
        print(f"Node {self.id} (leader) received an {
              request.Request} request.")
        self.dump.append(f"Node {self.id} (leader) received an {
                         request.Request} request.")

        if request_formatted == "SET":
            return self.handleSetReq(request)
        elif request_formatted == "GET":
            return self.handleGetReq(request)

    # Function to process a SET request
    def handleSetReq(self, request):
        msg = "SET " + " ".join(request.Request.split()[1:])
        self.broadcast(msg)
        if self.majorityAck():
            return self.serveClientReplyHandler("SET request processed", True)
        else:
            return self.serveClientReplyHandler("Leader changed", False)

    # Function to wait for majority acks
    def majorityAck(self):
        while len(self.ackCount) < (len(self.nodes) + 1) // 2:
            if self.currentRole != "Leader":
                return False
        return True

    # Function to process a GET request
    def handleGetReq(self, request):
        key = request.Request.split()[1]
        value = self.valueForKey(key)
        if value is not None:
            return self.serveClientReplyHandler(f"Value: {value}", True, value)
        else:
            return self.serveClientReplyHandler("Key not found", False)

    # Function to get the value for a key
    def valueForKey(self, key):
        for log_entry in reversed(self.logs):
            operation = log_entry.operation.split()
            if operation[0] == "SET" and operation[1] == key:
                return operation[2]
        return None

    # Function to create a serve client reply
    def serveClientReplyHandler(self, data, success, value=None):
        stringleader = str(self.leaderID)
        return raft_pb2.ServeClientReply(Data=data, LeaderID=stringleader, Success=success)

    # Function to respond to client requests as a follower
    def serveClientForFollower(self):
        stringleader = str(self.leaderID)
        if self.leaderID is not None:
            return self.serveClientReplyHandler("Forwarding request to leader", False)
        else:
            return self.serveClientReplyHandler("Leader not known", False)

    # Function to broadcast a message to all nodes
    def broadcast(self, msg):
        if self.currentRole == "Leader":
            self.broadcastAsLeader(msg)
        else:
            return self.broadcastIfNotLeader(msg)

    # Function to broadcast a message as a leader
    def broadcastAsLeader(self, msg):
        # Append the new log entry for the leader node
        log_entry = raft_pb2.LogEntry(
            term=self.currentTerm, operation=msg, index=len(self.logs) + 1)
        self.logs.append(log_entry)

        # Replicate the log entry to all followers
        for follower in self.nodes:
            if follower != self.curr_node_address:  # Skip the leader itself
                self.ackedLength[follower] = len(self.logs)
                threading.Thread(target=self.replicateLog,
                                 args=(follower,), daemon=True).start()

    # Function to broadcast a message if the node is not the leader
    def broadcastIfNotLeader(self, msg):
        print("Node is not the leader, forwarding request to the leader.")
        stringleader = str(self.leaderID)
        if self.leaderID is not None:
            return self.sendBroadcastToLeader(msg, stringleader)
        else:
            return raft_pb2.ServeClientReply(Data="Leader not known", LeaderID=stringleader, Success=False)

    # Function to send a broadcast message to the leader
    def sendBroadcastToLeader(self, msg, stringleader):
        channel = grpc.insecure_channel(self.leaderID)
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
            future = stub.ServeClient.future(
                raft_pb2.ServeClientArgs(Request=msg))
            response = future.result(timeout=1)
            return raft_pb2.ServeClientReply(Data=response.Data, LeaderID=stringleader, Success=response.Success)
        except grpc.FutureTimeoutError:
            print("Timeout occurred!")
        except Exception as e:
            print(f"Error forwarding request to leader: {e}")
            return raft_pb2.ServeClientReply(Data="Error forwarding request to leader", LeaderID=stringleader, Success=False)

    # Function to replicate log entries to a follower
    def replicateLog(self, follower):
        prefixLen, prefixTerm, suffix = self.prepLogEntries(follower)
        self.sendLogEntries(follower, prefixLen, prefixTerm, suffix)

    # Function to prepare log entries to be sent to a follower
    def prepLogEntries(self, follower):
        prefixLen = self.sentLength[follower]
        suffix = self.logs[prefixLen:]  # The log entries to be sent
        prefixTerm = self.logs[prefixLen - 1].term if prefixLen > 0 else 0
        return prefixLen, prefixTerm, suffix

    # Function to send log entries to a follower
    def sendLogEntries(self, follower, prefixLen, prefixTerm, suffix):
        channel = grpc.insecure_channel(follower)
        stub = raft_pb2_grpc.RaftStub(channel)
        try:
            future = stub.AppendEntries.future(
                raft_pb2.AppendEntriesArgs(
                    term=self.currentTerm,
                    leader_id=self.id,
                    prev_log_index=prefixLen - 1,
                    prev_log_term=prefixTerm,
                    entries=suffix,
                    leader_commit=self.commitLength,
                    remaining_lease=self.leaderLease
                )
            )
            response = future.result(timeout=1)  # Timeout in seconds
            self.appendEntriesResponseHandler(response, follower)
        except grpc.FutureTimeoutError:
            self.errorHandlerTimeoutLog(follower)
        except Exception as e:
            pass

    # Function to print timeout errors
    def errorHandlerTimeoutLog(self, follower):
        print(f"Timeout occurred while sending RPC to Node {follower}.")
        self.dump.append(
            f"Timeout occurred while sending RPC to Node {follower}.")

    # Function to handle append entries
    def AppendEntries(self, request, context):
        print(f"AppendEntries received from {request.leader_id}")

        if self.checkAndUpdateDetailsAppendEntries(request):
            self.resetElectionTimer()

            if self.checkLog(request):
                self.updateLog(request)
                self.commitLength = min(request.leader_commit, len(self.logs))

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
        self.leaderID = request.leader_id
        return True

    # Function to check the log for append entries
    def checkLog(self, request):
        prefixLen = request.prev_log_index + 1
        logOk = (len(self.logs) >= prefixLen) and \
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

        if leaderCommit > self.commitLength:
            self.commitLength = min(leaderCommit, len(self.logs))
            self.deliverLogEntries()

    # Function to remove conflicts in log
    def removeConflicts(self, prefixLen, suffix):
        if suffix:
            startIndex = prefixLen
            endIndex = min(startIndex + len(suffix), len(self.logs))
            for i in range(startIndex, endIndex):
                if self.logs[i].term != suffix[i - prefixLen].term:
                    self.logs = self.logs[:prefixLen]
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
        for i in range(self.commitLength):
            log_entry = self.logs[i]
            print(f"Node {self.id} ({self.currentRole}) committed the entry {
                  log_entry.operation} to the state machine.")
            self.dump.append(f"Node {self.id} ({self.currentRole}) committed the entry {
                             log_entry.operation} to the state machine.")

        self.saveLogs()

    # Function to handle append entries response
    def saveLogs(self):
        with self.logLock:
            with open(self.logFile, "w") as f:
                for log in self.logs:
                    f.write(f"{log.operation} {log.term}\n")

    # Function to handle append entries response
    def appendEntriesResponseHandler(self, response, follower):
        if response.term > self.currentTerm:
            self.roleFollower(response.term)
        elif self.currentRole == "Leader" and response.success:
            self.responseOk(follower)
        elif self.currentRole == "Leader":
            self.sentLengthHandlerandReplication(follower)

    # Function to handle correct response
    def responseOk(self, follower):
        self.ackCount.add(follower)
        if len(self.ackCount) >= (len(self.nodes) + 1) // 2:
            self.acksHandlerResponseOk(follower)

    # Function to handle correct response for acks
    def acksHandlerResponseOk(self, follower):
        self.ackedLength[follower] = max(
            self.ackedLength[follower], len(self.logs))
        self.leaderLease = self.leaderLeaseTimeout  # Assuming this resets the lease
        self.commitLogEntries()
        self.ackCount.clear()
        self.ackCount.add(self.leaderID)

    # Function to handle sent length and replication
    def sentLengthHandlerandReplication(self, follower):
        if self.sentLength[follower] > 0:
            self.sentLength[follower] -= 1
        self.replicateLog(follower)

    # Function to commit log entries
    def commitLogEntries(self):
        minAcks = (len(self.nodes) + 1) // 2
        # Filter nodes that have acknowledged up to the last log entry
        acks = {node: length for node, length in self.ackedLength.items()
                if length >= len(self.logs)}

        if acks and self.checkEntries(acks):
            self.commitEntries(acks)

    # Function to check entries
    def checkEntries(self, acks):
        latest_ack = max(acks.values())
        return len(acks) >= (len(self.nodes) + 1) // 2 and latest_ack > self.commitLength and \
            self.logs[latest_ack - 1].term == self.currentTerm

    # Function to commit entries
    def commitEntries(self, acks):
        latest_ack = max(acks.values())
        for i in range(self.commitLength, latest_ack):
            self.deliver_log_entry(self.logs[i])
        self.commitLength = latest_ack

    # Function to handle heartbeat
    def handleHeartbeats(self):
        while True:
            time.sleep(self.heartbeatTimeout)
            if self.currentRole == "Leader":
                self.heartbeat()

    # Function to send heartbeat
    def heartbeat(self):
        print(f"Leader {self.id} sending heartbeat & Renewing Lease")
        self.dump.append(
            f"Leader {self.id} sending heartbeat & Renewing Lease")
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
        os._exit(0)

    # Function to save metadata
    def saveMetadata(self):
        with open(self.metadata, "w") as metadata_file:
            metadata_file.write(f"{self.commitLength} {self.currentTerm} {
                                self.votedFor} {self.leaderID}\n")

    # Function to save logs
    def saveLogs(self):
        with open(self.logFile, "w") as logFile:
            for log in self.logs:
                logFile.write(f"{log.operation} {log.term}\n")

    # Function to save dump
    def saveDump(self):
        with open(self.dumpFile, "w") as dump_file:
            for dump in self.dump:
                dump_file.write(f"{dump}\n")


# Function to serve the server
def serve():
    print("Server started...")
    node_id = int(input("Enter node id: "))
    checkNodeId(node_id)
    raft_node = Raft(node_id, nodeAddress[node_id], nodeAddress)
    server = startServergrpc(raft_node, node_id)

    getSavedData(raft_node)
    eraseLogData(raft_node)

    backgroundTasks(raft_node)
    letsGoo(server, raft_node)


def checkNodeId(node_id):
    if node_id < 0 or node_id >= len(nodeAddress):
        print("Invalid node ID. Exiting.")
        os._exit(0)


def startServergrpc(raft_node, node_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    raft_pb2_grpc.add_RaftServicer_to_server(raft_node, server)
    server.add_insecure_port(nodeAddress[node_id])
    server.start()
    return server


def getSavedData(raft_node):
    try:
        getLogData(raft_node)
        getMetadata(raft_node)
    except FileNotFoundError:
        print("No previous logs found.")


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


def eraseLogData(raft_node):
    try:
        os.remove(raft_node.logFile)
    except FileNotFoundError:
        pass


def backgroundTasks(raft_node):
    threading.Thread(target=raft_node.startElectionTimer, daemon=True).start()
    threading.Thread(target=raft_node.handleHeartbeats, daemon=True).start()
    threading.Thread(target=raft_node.startLeaderLeaseTimer,
                     daemon=True).start()


def letsGoo(server, raft_node):
    print(f"Server started at {nodeAddress[raft_node.id]}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        raft_node.saveEverything()
        server.stop(0)


if __name__ == "__main__":
    serve()
