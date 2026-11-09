from http.server import ThreadingHTTPServer
import json
import threading
import types
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import dashboard


class DashboardTests(unittest.TestCase):
    def test_http_status_commands_controls_and_shutdown(self):
        # Real HTTP routes with simulated RPCs and child processes; no cluster needed.
        grpc = types.ModuleType("grpc")
        grpc.RpcError = RuntimeError
        grpc.insecure_channel = MagicMock()
        pb = types.ModuleType("raft_pb2")
        pb.ServeClientArgs = types.SimpleNamespace
        rpc = types.ModuleType("raft_pb2_grpc")
        rpc.RaftStub = MagicMock()
        rpc.RaftStub.return_value.ServeClient.return_value = types.SimpleNamespace(
            Data=json.dumps({"id": 0, "role": "Leader", "term": 1, "logs": [], "values": {}}))
        client_module = types.ModuleType("Client")
        client_module.Client = MagicMock()
        client_module.Client.return_value.channels = []
        client_module.Client.return_value.sendRequest.return_value = types.SimpleNamespace(
            Data="SET request processed", LeaderID="0")
        process, launch = MagicMock(), MagicMock()
        process.poll.return_value = None
        ready, captured = threading.Event(), []

        def make_server(*args, **kwargs):
            server = ThreadingHTTPServer(*args, **kwargs)
            captured.append(server)
            ready.set()
            return server

        with (patch.dict("sys.modules", {"grpc": grpc, "raft_pb2": pb,
                                        "raft_pb2_grpc": rpc, "Client": client_module}),
              patch.object(dashboard, "ThreadingHTTPServer", side_effect=make_server),
              patch("builtins.print")):
            worker = threading.Thread(target=dashboard.run_dashboard,
                                      args=(["localhost:50051"], [process], launch, 0, False))
            worker.start()
            self.assertTrue(ready.wait(timeout=5))
            root = f"http://127.0.0.1:{captured[0].server_port}"

            def post(path, body, origin=None):
                headers = {"Content-Type": "application/json"}
                if origin:
                    headers["Origin"] = origin
                request = Request(root + path, data=json.dumps(body).encode(), headers=headers)
                with urlopen(request, timeout=5) as response:
                    return json.load(response)

            try:
                with urlopen(root + "/api/status", timeout=5) as response:
                    status = json.load(response)
                self.assertEqual(status["majority"], 1)
                self.assertTrue(status["nodes"][0]["online"])
                self.assertEqual(status["nodes"][0]["role"], "Leader")
                with urlopen(root, timeout=5) as response:
                    self.assertIn(b"Raft Observatory", response.read())
                result = post("/api/command", {"operation": "SET", "key": "name", "value": "Garv"})
                self.assertEqual(result["message"], "SET request processed")
                with self.assertRaises(HTTPError) as error:
                    post("/api/command", {"operation": "BAD", "key": "name"})
                self.assertEqual(error.exception.code, 400)
                error.exception.close()
                with self.assertRaises(HTTPError) as error:
                    post("/api/node", {"id": 0, "action": "stop"}, "https://example.com")
                self.assertEqual(error.exception.code, 403)
                error.exception.close()
                post("/api/node", {"id": 0, "action": "restart"})
                process.terminate.assert_called_once()
                launch.assert_called_once_with(0)
            finally:
                post("/api/shutdown", {})
                worker.join(timeout=5)
                self.assertFalse(worker.is_alive())


if __name__ == "__main__":
    unittest.main()
