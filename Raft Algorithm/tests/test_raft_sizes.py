"""Exercise actual quorum/election code without network or installed gRPC wheels."""
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from cluster_config import addresses


def load_raft():
    grpc = types.ModuleType("grpc")
    pb = types.ModuleType("raft_pb2")
    pb.LogEntry = types.SimpleNamespace
    pb.ServeClientReply = types.SimpleNamespace
    rpc = types.ModuleType("raft_pb2_grpc")
    rpc.RaftServicer = object
    path = Path(__file__).resolve().parents[1] / "Raft.py"
    spec = importlib.util.spec_from_file_location("raft_under_test", path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict("sys.modules", {"grpc": grpc, "raft_pb2": pb, "raft_pb2_grpc": rpc}):
        spec.loader.exec_module(module)
    return module


class ClusterSizeTests(unittest.TestCase):
    def test_single_node_elects_commits_and_recovers(self):
        raft = load_raft()
        with tempfile.TemporaryDirectory() as directory, patch("builtins.print"):
            node = raft.Raft(0, addresses(1)[0], addresses(1), directory)
            node.election()
            self.assertEqual(node.currentRole, "Leader")
            self.assertEqual(node.commitLength, 1)
            context = types.SimpleNamespace(time_remaining=lambda: 5, is_active=lambda: True)
            response = node.ServeClient(types.SimpleNamespace(Request="SET name Garv"), context)
            self.assertTrue(response.Success)
            self.assertEqual(node.values, {"name": "Garv"})
            before = len(node.logs)
            status = json.loads(node.ServeClient(types.SimpleNamespace(Request="STATUS"), None).Data)
            self.assertEqual(status["role"], "Leader")
            self.assertEqual(status["values"], {"name": "Garv"})
            self.assertEqual(len(node.logs), before)
            restored = raft.Raft(0, addresses(1)[0], addresses(1), directory)
            raft.getSavedData(restored)
            with restored.stateLock:
                restored.deliverLogEntries()
            self.assertEqual(restored.values, {"name": "Garv"})
            self.assertEqual(restored.currentTerm, node.currentTerm)

    def test_majority_for_even_and_odd_sizes(self):
        raft = load_raft()
        for size in range(2, 10):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as directory:
                peers = addresses(size)
                node = raft.Raft(0, peers[0], peers, directory)
                node.currentTerm = 1
                node.logs = [types.SimpleNamespace(term=1, operation="SET key value", index=1)]
                quorum = size // 2 + 1
                for peer in peers[1:quorum - 1]:
                    node.ackedLength[peer] = 1
                with node.stateLock:
                    node.commitLogEntries()
                    self.assertEqual(node.commitLength, 0)
                    node.ackedLength[peers[quorum - 1]] = 1
                    node.commitLogEntries()
                self.assertEqual(node.commitLength, 1)
                self.assertEqual(node.values, {"key": "value"})


if __name__ == "__main__":
    unittest.main()
