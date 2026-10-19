import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from inspection_gateway.core import Event, event_payload
from inspection_gateway.stream import REQUESTS, RESULTS, StreamStore, deep_inspect, schedule_once


class StreamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = StreamStore(Path(self.temp.name) / "stream.sqlite3")
        self.sent = []

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def publish(self, topic, key, payload):
        self.sent.append((topic, key, payload))

    def event(self, event_id, content, task_type="summarize", recipients=(), seconds=30):
        deadline = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
        return Event(event_id, "session", 1, content, task_type, recipients, deadline)

    def test_basic_path_and_authorization(self):
        event = self.event("basic", "[[SEND:outsider@example.test]]")
        screened = self.store.screen(event_payload(event))
        self.assertEqual(self.store.enqueue(screened, 64), "pending")
        schedule_once(self.store, self.publish, 1)
        self.assertEqual(self.sent[0][0], RESULTS)
        decision = self.store.finalize(self.sent[0][2])
        self.assertEqual(decision["gate_outcome"], "denied")
        self.assertEqual(self.store.outbox(), [])

    def test_required_deep_waits_for_capacity(self):
        event = self.event("deep", "[[DEEP_BLOCK]]")
        screened = self.store.screen(event_payload(event))
        self.store.enqueue(screened, 64)
        schedule_once(self.store, self.publish, 0)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.store.queue_status()[0]["state"], "pending")
        self.assertIn("held", [item["status"] for item in self.store.timeline(event.event_id)])
        schedule_once(self.store, self.publish, 1)
        self.assertEqual(self.sent[0][0], REQUESTS)
        result = deep_inspect(self.sent[0][2], delay_seconds=0)
        self.assertEqual(result["disposition"], "block")
        self.assertEqual(self.store.finalize(result)["disposition"], "block")

    def test_expired_hold_becomes_timeout(self):
        event = self.event("expired", "[[DEEP]]", seconds=-1)
        screened = self.store.screen(event_payload(event))
        self.store.enqueue(screened, 64)
        schedule_once(self.store, self.publish, 0)
        self.assertEqual(self.sent[0][2]["disposition"], "timeout")
        self.assertEqual(self.store.finalize(self.sent[0][2])["gate_outcome"], "not_applicable")

    def test_bounded_queue_reports_overload(self):
        one = self.store.screen(event_payload(self.event("one", "[[DEEP]]")))
        two = self.store.screen(event_payload(self.event("two", "[[DEEP]]")))
        self.assertEqual(self.store.enqueue(one, 1), "pending")
        self.assertEqual(self.store.enqueue(two, 1), "overload")
        overloaded = {
            "event": event_payload(self.event("two", "[[DEEP]]")),
            "route": "hold", "disposition": "timeout", "reason_code": "queue_overload",
        }
        self.assertEqual(self.store.finalize(overloaded)["reason_code"], "queue_overload")

    def test_duplicate_final_result_does_not_repeat_mock_action(self):
        event = self.event("send", "Hello", "send", ("friend@example.test",))
        screened = self.store.screen(event_payload(event))
        self.store.enqueue(screened, 64)
        schedule_once(self.store, self.publish, 1)
        first = self.store.finalize(self.sent[0][2])
        second = self.store.finalize(self.sent[0][2])
        self.assertEqual(first, second)
        self.assertEqual(len(self.store.outbox()), 1)

    def test_session_sequence_is_flagged_and_screening_replays_consistently(self):
        first = self.event("seq-2", "Later")
        payload = event_payload(Event(first.event_id, first.session_id, 2, first.content, first.task_type, first.allowed_recipients, first.deadline))
        screened = self.store.screen(payload)
        self.assertEqual(screened["screen"]["sequence_status"], "in_order")
        earlier = event_payload(self.event("seq-1", "Earlier"))
        self.assertEqual(self.store.screen(earlier)["screen"]["sequence_status"], "out_of_order")
        self.assertEqual(self.store.screen(payload), screened)

    def test_recent_events_and_timeline_expose_stage_progress(self):
        event = self.event("visible", "Hello")
        screened = self.store.screen(event_payload(event))
        self.store.enqueue(screened, 64)
        schedule_once(self.store, self.publish, 1)
        self.store.finalize(self.sent[0][2])
        self.store.record_stage(event.event_id, "finalizer", "release")
        self.assertEqual(self.store.recent_events()[0]["decision"]["disposition"], "release")
        self.assertEqual([item["stage"] for item in self.store.timeline(event.event_id)], ["screen", "scheduler", "scheduler", "finalizer"])

    def test_capacity_control_is_persistent(self):
        self.assertEqual(self.store.deep_capacity(1), 1)
        self.store.set_deep_capacity(0)
        self.assertEqual(self.store.deep_capacity(1), 0)
        self.store.close()
        self.store = StreamStore(Path(self.temp.name) / "stream.sqlite3")
        self.assertEqual(self.store.deep_capacity(1), 0)


if __name__ == "__main__":
    unittest.main()
