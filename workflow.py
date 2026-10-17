"""Synthetic, offline workflow simulator. No n8n or MirrorFly integration."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone


class WorkflowError(ValueError):
    pass


def instant(value):
    """Require an explicit UTC offset; never infer the computer's timezone."""
    if not isinstance(value, str):
        raise WorkflowError("timestamp must be an ISO 8601 string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WorkflowError("invalid ISO 8601 timestamp") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise WorkflowError("timestamp needs an explicit UTC offset")
    return result.astimezone(timezone.utc)


def stamp(value):
    return value.isoformat().replace("+00:00", "Z")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class Workflow:
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meetings (
                id TEXT PRIMARY KEY, version INTEGER NOT NULL,
                start_at TEXT NOT NULL, status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS inbox (
                id TEXT PRIMARY KEY, event TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, meeting_id TEXT NOT NULL,
                version INTEGER NOT NULL, kind TEXT NOT NULL,
                due_at TEXT NOT NULL, payload TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                UNIQUE(meeting_id, version, kind));
        """)

    def close(self):
        self.db.close()

    def meeting(self, meeting_id):
        row = self.db.execute("SELECT * FROM meetings WHERE id=?", (meeting_id,)).fetchone()
        return dict(row) if row else None

    def deliveries(self):
        return [dict(row) for row in self.db.execute("SELECT * FROM outbox ORDER BY id")]

    def accept(self, event, now):
        now = instant(now)
        if not isinstance(event, dict):
            raise WorkflowError("event must be an object")
        kind = event.get("type")
        if kind not in ("create", "reschedule", "cancel"):
            raise WorkflowError("unknown event type")
        required = {"event_id", "meeting_id", "type"}
        if kind != "cancel":
            required.add("start_at")
        if kind != "create":
            required.add("expected_version")
        if set(event) != required:
            raise WorkflowError("missing or unexpected event fields")
        for name in ("event_id", "meeting_id"):
            if not isinstance(event[name], str) or not event[name].strip():
                raise WorkflowError(name + " must be a nonempty string")
        if kind != "create" and (
            type(event["expected_version"]) is not int or event["expected_version"] < 1
        ):
            raise WorkflowError("expected_version must be a positive integer")
        raw = encode(event)
        # Inbox insertion, state change, and outbox writes form one transaction.
        with self.db:
            old = self.db.execute("SELECT event FROM inbox WHERE id=?", (event["event_id"],)).fetchone()
            if old:
                if old["event"] != raw:
                    raise WorkflowError("event_id reused with different content")
                return "duplicate"
            current = self.meeting(event["meeting_id"])
            if kind == "create":
                if current:
                    raise WorkflowError("meeting already exists")
                version = 1
            else:
                if not current or current["status"] != "scheduled":
                    raise WorkflowError("meeting is not scheduled")
                if current["version"] != event["expected_version"]:
                    raise WorkflowError("stale meeting version")
                version = current["version"] + 1
            start = instant(event["start_at"]) if kind != "cancel" else instant(current["start_at"])
            if kind != "cancel" and start <= now:
                raise WorkflowError("meeting must start in the future")
            status = "cancelled" if kind == "cancel" else "scheduled"
            self.db.execute("INSERT INTO inbox VALUES (?,?)", (event["event_id"], raw))
            self.db.execute(
                "INSERT OR REPLACE INTO meetings VALUES (?,?,?,?)",
                (event["meeting_id"], version, stamp(start), status),
            )
            self.db.execute(
                "UPDATE outbox SET state='invalidated' WHERE meeting_id=? AND state='pending'",
                (event["meeting_id"],),
            )
            payload = {"meeting_id": event["meeting_id"], "version": version,
                       "start_at": stamp(start), "status": status}
            self._enqueue(payload, "calendar_cancel" if kind == "cancel" else "calendar_upsert", now)
            if kind != "cancel":
                self._enqueue(payload, "reminder", max(now, start - timedelta(minutes=15)))
        return "accepted"

    def _enqueue(self, payload, kind, due):
        self.db.execute(
            "INSERT INTO outbox(meeting_id,version,kind,due_at,payload) VALUES (?,?,?,?,?)",
            (payload["meeting_id"], payload["version"], kind, stamp(due), encode(payload)),
        )

    def flush(self, now, sink):
        """Single-worker demo; at-least-once attempts with stable delivery keys."""
        now = instant(now)
        result = []
        rows = self.db.execute("SELECT * FROM outbox WHERE state='pending' ORDER BY id").fetchall()
        for row in rows:
            if instant(row["due_at"]) > now:
                continue
            current = self.meeting(row["meeting_id"])
            obsolete = current["version"] != row["version"]
            expired = row["kind"] == "reminder" and (
                current["status"] != "scheduled" or instant(current["start_at"]) <= now
            )
            if obsolete or expired:
                with self.db:
                    self.db.execute("UPDATE outbox SET state='invalidated' WHERE id=?", (row["id"],))
                result.append((row["id"], "invalidated"))
                continue
            if row["kind"] == "reminder":
                calendar = self.db.execute(
                    "SELECT state FROM outbox WHERE meeting_id=? AND version=? AND kind='calendar_upsert'",
                    (row["meeting_id"], row["version"]),
                ).fetchone()
                if not calendar or calendar["state"] != "delivered":
                    continue
            # JSON encoding avoids collisions when IDs contain punctuation.
            delivery_key = encode([row["meeting_id"], row["version"], row["kind"]])
            try:
                sink.deliver(delivery_key, row["kind"], json.loads(row["payload"]))
            except RuntimeError as exc:
                with self.db:
                    self.db.execute(
                        "UPDATE outbox SET attempts=attempts+1,last_error=? WHERE id=?",
                        (str(exc), row["id"]),
                    )
                result.append((row["id"], "retry_pending"))
            else:
                with self.db:
                    self.db.execute(
                        "UPDATE outbox SET attempts=attempts+1,last_error=NULL,state='delivered' WHERE id=?",
                        (row["id"],),
                    )
                result.append((row["id"], "delivered"))
        return result


class SyntheticSink:
    """In-memory fake downstream, deliberately supporting idempotency keys."""
    def __init__(self, fail_once=None, lose_ack_once=None):
        self.fail_once = fail_once
        self.lose_ack_once = lose_ack_once
        self.received = {}

    def deliver(self, key, kind, payload):
        if key in self.received:
            return
        if kind == self.fail_once:
            self.fail_once = None
            raise RuntimeError("synthetic temporary downstream failure")
        self.received[key] = {"kind": kind, "payload": payload}
        if kind == self.lose_ack_once:
            self.lose_ack_once = None
            raise RuntimeError("synthetic lost acknowledgement after acceptance")


def demo():
    workflow = Workflow()
    sink = SyntheticSink(fail_once="calendar_upsert")
    now = "2026-10-17T09:00:00Z"
    create = {"event_id": "e1", "meeting_id": "demo-1", "type": "create",
              "start_at": "2026-10-17T13:00:00+03:00"}
    print("SYNTHETIC OFFLINE DEMO: no external integration or messages")
    print("create:", workflow.accept(create, now))
    print("start UTC:", workflow.meeting("demo-1")["start_at"])
    print("duplicate:", workflow.accept(create, now))
    print("first flush:", workflow.flush(now, sink))
    print("retry flush:", workflow.flush(now, sink))
    print("reschedule:", workflow.accept({"event_id": "e2", "meeting_id": "demo-1",
          "type": "reschedule", "expected_version": 1, "start_at": "2026-10-17T11:00:00Z"}, now))
    print("old reminder time:", workflow.flush("2026-10-17T09:45:00Z", sink))
    print("cancel:", workflow.accept({"event_id": "e3", "meeting_id": "demo-1",
          "type": "cancel", "expected_version": 2}, "2026-10-17T10:00:00Z"))
    print("after cancellation:", workflow.flush("2026-10-17T10:45:00Z", sink))
    print("reminders delivered:", sum(item["kind"] == "reminder" for item in sink.received.values()))
    print("outbox:", [(r["id"], r["kind"], r["state"], r["attempts"]) for r in workflow.deliveries()])
    workflow.close()


if __name__ == "__main__":
    demo()
