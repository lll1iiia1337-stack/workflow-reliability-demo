import unittest
from unittest.mock import patch

from workflow import SyntheticSink, Workflow, WorkflowError


NOW = "2026-10-17T09:00:00Z"


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = Workflow()
        self.addCleanup(self.workflow.close)
        self.create = {"event_id": "e1", "meeting_id": "meeting", "type": "create",
                       "start_at": "2026-10-17T13:00:00+03:00"}

    def change(self, kind, version=1, **extra):
        return {"event_id": "e-" + kind, "meeting_id": "meeting", "type": kind,
                "expected_version": version, **extra}

    def test_duplicate_does_not_add_deliveries_even_after_meeting_time(self):
        self.workflow.accept(self.create, NOW)
        self.assertEqual("duplicate", self.workflow.accept(self.create, "2026-10-18T09:00:00Z"))
        self.assertEqual(2, len(self.workflow.deliveries()))

    def test_conflicting_event_id_is_rejected_without_state_change(self):
        self.workflow.accept(self.create, NOW)
        with self.assertRaisesRegex(WorkflowError, "different content"):
            self.workflow.accept({**self.create, "start_at": "2026-10-17T12:00:00Z"}, NOW)
        self.assertEqual("2026-10-17T10:00:00Z", self.workflow.meeting("meeting")["start_at"])

    def test_offset_conversion_and_due_boundary(self):
        self.workflow.accept(self.create, NOW)
        sink = SyntheticSink()
        self.workflow.flush("2026-10-17T12:44:59+03:00", sink)
        self.assertEqual(["calendar_upsert"], [r["kind"] for r in sink.received.values()])
        self.workflow.flush("2026-10-17T05:45:00-04:00", sink)
        self.assertEqual(["calendar_upsert", "reminder"], [r["kind"] for r in sink.received.values()])

    def test_naive_invalid_and_past_timestamps_have_no_side_effects(self):
        for value in ["2026-10-17T10:00:00", "not-a-date", NOW, "2026-10-16T10:00:00Z"]:
            with self.subTest(value=value), self.assertRaises(WorkflowError):
                self.workflow.accept({**self.create, "start_at": value}, NOW)
        self.assertIsNone(self.workflow.meeting("meeting"))
        self.assertEqual([], self.workflow.deliveries())

    def test_reschedule_invalidates_old_reminder_and_sends_new_one(self):
        self.workflow.accept(self.create, NOW)
        sink = SyntheticSink()
        self.workflow.flush(NOW, sink)
        self.workflow.accept(self.change("reschedule", start_at="2026-10-17T11:00:00Z"), NOW)
        self.workflow.flush("2026-10-17T09:45:00Z", sink)
        self.assertFalse(any(r["kind"] == "reminder" for r in sink.received.values()))
        self.workflow.flush("2026-10-17T10:45:00Z", sink)
        reminders = [r for r in sink.received.values() if r["kind"] == "reminder"]
        self.assertEqual([2], [r["payload"]["version"] for r in reminders])

    def test_cancel_invalidates_pending_failure_and_reminder(self):
        self.workflow.accept(self.create, NOW)
        sink = SyntheticSink(fail_once="calendar_upsert")
        self.workflow.flush(NOW, sink)
        cancel = self.change("cancel")
        self.workflow.accept(cancel, NOW)
        self.assertEqual("duplicate", self.workflow.accept(cancel, NOW))
        self.workflow.flush("2026-10-17T09:45:00Z", sink)
        self.assertEqual(["calendar_cancel"], [r["kind"] for r in sink.received.values()])
        self.assertEqual("cancelled", self.workflow.meeting("meeting")["status"])

    def test_stale_reschedule_rejected_and_cannot_revive_cancelled_meeting(self):
        self.workflow.accept(self.create, NOW)
        with self.assertRaisesRegex(WorkflowError, "stale"):
            self.workflow.accept(self.change("reschedule", 2, start_at="2026-10-17T11:00:00Z"), NOW)
        self.workflow.accept(self.change("cancel"), NOW)
        with self.assertRaisesRegex(WorkflowError, "not scheduled"):
            self.workflow.accept(self.change("reschedule", 2, start_at="2026-10-17T11:00:00Z"), NOW)

    def test_failure_retries_with_recorded_attempts(self):
        self.workflow.accept(self.create, NOW)
        sink = SyntheticSink(fail_once="calendar_upsert")
        self.assertEqual([(1, "retry_pending")], self.workflow.flush(NOW, sink))
        self.assertIsNotNone(self.workflow.deliveries()[0]["last_error"])
        self.assertEqual([(1, "delivered")], self.workflow.flush(NOW, sink))
        self.assertEqual(2, self.workflow.deliveries()[0]["attempts"])
        self.assertIsNone(self.workflow.deliveries()[0]["last_error"])

    def test_lost_acknowledgement_does_not_duplicate_downstream_effect(self):
        self.workflow.accept(self.create, NOW)
        sink = SyntheticSink(lose_ack_once="calendar_upsert")
        self.workflow.flush(NOW, sink)
        self.workflow.flush(NOW, sink)
        self.assertEqual(1, len(sink.received))
        self.assertEqual(2, self.workflow.deliveries()[0]["attempts"])

    def test_reminder_waits_for_successful_calendar_update(self):
        self.workflow.accept({**self.create, "start_at": "2026-10-17T09:10:00Z"}, NOW)
        sink = SyntheticSink(fail_once="calendar_upsert")
        self.workflow.flush(NOW, sink)
        self.assertEqual({}, sink.received)
        self.workflow.flush(NOW, sink)
        self.assertEqual(["calendar_upsert", "reminder"], [r["kind"] for r in sink.received.values()])

    def test_enqueue_failure_rolls_back_inbox_state_and_partial_outbox(self):
        enqueue = self.workflow._enqueue

        def fail_on_reminder(payload, kind, due):
            if kind == "reminder":
                raise RuntimeError("synthetic local transaction failure")
            enqueue(payload, kind, due)

        with patch.object(self.workflow, "_enqueue", side_effect=fail_on_reminder):
            with self.assertRaises(RuntimeError):
                self.workflow.accept(self.create, NOW)
        self.assertIsNone(self.workflow.meeting("meeting"))
        self.assertEqual([], self.workflow.deliveries())
        self.assertEqual("accepted", self.workflow.accept(self.create, NOW))

    def test_expired_reminder_is_not_sent_late(self):
        self.workflow.accept(self.create, NOW)
        sink = SyntheticSink()
        self.workflow.flush("2026-10-17T10:00:00Z", sink)
        self.assertEqual(["calendar_upsert"], [r["kind"] for r in sink.received.values()])
        self.assertEqual("invalidated", self.workflow.deliveries()[1]["state"])


if __name__ == "__main__":
    unittest.main()
