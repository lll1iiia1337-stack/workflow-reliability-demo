# Offline meeting workflow demonstration

This is an AI-created synthetic Python standard-library example, prepared as a
technical work sample. It does not connect to n8n, MirrorFly, a calendar, a CRM,
email, or a payment platform. It has no credentials and makes no network calls.
It is not client work, an accepted order, or evidence of a production integration.

From this directory:

```sh
python3 workflow.py
python3 -m unittest -v
```

The deterministic scenario creates a meeting with an explicit timezone offset,
replays its event, recovers from a simulated calendar failure, reschedules it,
and cancels it before a reminder can be sent. `sample-output.txt` contains the
actual CLI output.

Validated events update a SQLite inbox, meeting record, and outbox in one
transaction. Event IDs reject conflicting reuse; meeting revisions reject stale
updates. Rescheduling and cancellation invalidate pending work from previous
revisions. Reminders are not sent after the meeting has started. A retry uses the
same downstream delivery key even when an acknowledgement was lost. Reminders
wait for the same revision's calendar update to be acknowledged successfully.

The fake downstream explicitly honors idempotency keys. A real downstream would
need that support or a reconciliation design: this example does not promise
exactly-once delivery to arbitrary services. Processing is single-worker, with
manual retry and an injected clock. Production scheduling, authentication,
authorization, webhook signatures, backoff, concurrent workers, remote-state
reconciliation, and actual video calls are outside this sample's scope.
