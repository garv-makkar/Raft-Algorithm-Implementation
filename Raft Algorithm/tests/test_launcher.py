import argparse
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

from cluster_config import addresses, node_count
import start


class ConfigurationTests(unittest.TestCase):
    def test_every_supported_size(self):
        for count in range(1, 10):
            with self.subTest(count=count):
                nodes = addresses(str(count))
                self.assertEqual(len(nodes), count)
                self.assertEqual(nodes[-1], f"localhost:{50050 + count}")

    def test_invalid_counts(self):
        for value in ("", "abc", "1.5", "0", "-1", "10", "100"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                node_count(value)

    def test_prompt_recovers_from_invalid_input(self):
        with patch("builtins.input", side_effect=["0", "wrong", "9"]), patch("builtins.print"):
            self.assertEqual(start.choose_count(), 9)


class LifecycleTests(unittest.TestCase):
    def exercise_launcher(self, fail=False):
        # No live cluster: fake RPC readiness and child processes, retain real log files.
        grpc = types.ModuleType("grpc")
        grpc.insecure_channel = MagicMock()
        grpc.channel_ready_future = MagicMock()
        grpc.FutureTimeoutError = TimeoutError
        client_module = types.ModuleType("Client")
        client_module.Client = MagicMock()
        process = MagicMock()
        process.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory:
            effects = [process, OSError("cannot spawn")] if fail else [MagicMock() for _ in range(9)]
            for child in effects:
                if isinstance(child, MagicMock):
                    child.poll.return_value = None
            with (patch.dict("sys.modules", {"grpc": grpc, "Client": client_module}),
                  patch.object(start, "__file__", str(Path(directory) / "start.py")),
                  patch.object(start, "check_ports"),
                  patch.object(start.subprocess, "Popen", side_effect=effects) as spawn,
                  patch("builtins.print")):
                if fail:
                    with self.assertRaises(OSError):
                        start.run_cluster(9, 50051)
                    process.terminate.assert_called_once()
                    process.wait.assert_called_once()
                else:
                    start.run_cluster(9, 50051)
                    self.assertEqual(spawn.call_count, 9)
                    client_module.Client.assert_called_once_with(nodes=addresses(9))
                    client_module.Client.return_value.letsGoo.assert_called_once()
                    for call in spawn.call_args_list:
                        self.assertIn("--nodes", call.args[0])
                        self.assertTrue(call.kwargs["stdout"].closed)
                    for child in effects:
                        child.terminate.assert_called_once()
                        child.wait.assert_called_once()

    def test_normal_exit_stops_every_node(self):
        self.exercise_launcher()

    def test_partial_startup_failure_stops_existing_children(self):
        self.exercise_launcher(fail=True)


if __name__ == "__main__":
    unittest.main()
