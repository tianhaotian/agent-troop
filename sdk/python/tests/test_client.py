import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from agent_troop import AgentWorker, Client


class FakeClient(Client):
    def __init__(self):
        super().__init__("http://example.invalid", token="secret")
        self.calls = []
        self.offer = {
            "lease_id": "les_1", "fencing_token": 7,
            "subtask": {"id": "sub_1", "version": 2, "attempt": 0},
            "context_package": {"snapshot_hash": "abc"},
        }

    def request(self, method, path, body=None, headers=None):
        self.calls.append((method, path, body))
        if path.endswith("/offers"):
            return {"offers": [self.offer]}
        if path.endswith("/accept"):
            return {"version": 3}
        if path.endswith("/start"):
            return {"version": 4}
        return {"status": "ok"}


class WorkerTest(unittest.TestCase):
    def test_lease_lifecycle(self):
        client = FakeClient()
        completed = AgentWorker(client, "agt_1").run_once(
            lambda context: {"result_ref": "artifact://answer", "usage_tokens": 12}
        )
        self.assertEqual(completed, 1)
        self.assertEqual([call[1].split("/")[-1] for call in client.calls],
                         ["offers", "accept", "start", "complete"])
        complete = client.calls[-1][2]
        self.assertEqual(complete["fencing_token"], 7)
        self.assertEqual(complete["version"], 4)
        self.assertEqual(complete["idempotency_key"], "sdk-sub_1-0")

    def test_failure_is_reported(self):
        client = FakeClient()

        def fail(_):
            raise ValueError("runtime failed")

        self.assertEqual(AgentWorker(client, "agt_1").run_once(fail), 0)
        self.assertEqual(client.calls[-1][1].split("/")[-1], "fail")
        self.assertEqual(client.calls[-1][2]["reason"], "runtime failed")


if __name__ == "__main__":
    unittest.main()
