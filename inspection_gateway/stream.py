"""Kafka streaming stages with durable local queue state.

Screening, deep inspection, and assistant behavior are synthetic fixtures. The
streaming boundaries, deadlines, capacity, and idempotent mock actions are real.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

from .core import Event, assistant_fixture, authorize_action, parse_time, utc_now

INPUT = "inspection.input.v2"
SCREENED = "inspection.screened.v2"
REQUESTS = "inspection.requests.v2"
RESULTS = "inspection.results.v2"
DECISIONS = "inspection.decisions.v2"
ERRORS = "inspection.errors.v2"
TOPICS = (INPUT, SCREENED, REQUESTS, RESULTS, DECISIONS, ERRORS)
_INIT_LOCK = threading.Lock()


class StreamStore:
    def __init__(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with _INIT_LOCK:
            self.db = sqlite3.connect(path, timeout=10)
            self.db.execute("PRAGMA busy_timeout=10000")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("""CREATE TABLE IF NOT EXISTS screened (
                event_id TEXT PRIMARY KEY, payload TEXT NOT NULL
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS session_state (
                session_id TEXT PRIMARY KEY, max_sequence INTEGER NOT NULL
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS queue (
                event_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                state TEXT NOT NULL, enqueued_at TEXT NOT NULL
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS decisions (
                event_id TEXT PRIMARY KEY, payload TEXT NOT NULL
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS mock_outbox (
                event_id TEXT PRIMARY KEY, recipient TEXT NOT NULL, body TEXT NOT NULL
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS stage_events (
                event_id TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL,
                recorded_at TEXT NOT NULL, detail TEXT NOT NULL,
                PRIMARY KEY (event_id, stage, status)
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS worker_heartbeats (
                stage TEXT PRIMARY KEY, seen_at TEXT NOT NULL
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS runtime_controls (
                name TEXT PRIMARY KEY, value INTEGER NOT NULL, updated_at TEXT NOT NULL
            )""")
            self.db.commit()

    def close(self) -> None:
        self.db.close()

    def record_stage(self, event_id: str, stage: str, status: str, detail: str = "") -> None:
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO stage_events VALUES (?, ?, ?, ?, ?)",
                (event_id, stage, status, utc_now().isoformat(), detail),
            )

    def heartbeat(self, stage: str) -> None:
        with self.db:
            self.db.execute("""INSERT INTO worker_heartbeats VALUES (?, ?)
                ON CONFLICT(stage) DO UPDATE SET seen_at=excluded.seen_at""", (stage, utc_now().isoformat()))

    def worker_status(self) -> list[dict[str, Any]]:
        rows = dict(self.db.execute("SELECT stage, seen_at FROM worker_heartbeats").fetchall())
        result = []
        for stage in ("screen", "schedule", "deep", "finalize"):
            last = rows.get(stage)
            online = bool(last and (utc_now() - parse_time(last)).total_seconds() < 6)
            result.append({"stage": stage, "online": online, "last_seen": last})
        return result

    def deep_capacity(self, default: int) -> int:
        row = self.db.execute("SELECT value FROM runtime_controls WHERE name='deep_capacity'").fetchone()
        return row[0] if row else default

    def set_deep_capacity(self, value: int) -> None:
        if not 0 <= value <= 8:
            raise ValueError("deep capacity must be between 0 and 8")
        with self.db:
            self.db.execute("""INSERT INTO runtime_controls VALUES ('deep_capacity', ?, ?)
                ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (value, utc_now().isoformat()),
            )

    def queue_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "inflight": 0, "complete": 0, "overload": 0}
        for state, count in self.db.execute("SELECT state, count(*) FROM queue GROUP BY state"):
            counts[state] = count
        return counts

    def screen(self, raw: dict[str, Any]) -> dict[str, Any]:
        event = Event.from_dict(raw)
        row = self.db.execute("SELECT payload FROM screened WHERE event_id=?", (event.event_id,)).fetchone()
        if row:
            self.record_stage(event.event_id, "screen", "complete")
            return json.loads(row[0])
        previous = self.db.execute(
            "SELECT max_sequence FROM session_state WHERE session_id=?", (event.session_id,)
        ).fetchone()
        sequence_status = "out_of_order" if previous and event.sequence <= previous[0] else "in_order"
        needs_deep = "[[DEEP]]" in event.content or "[[DEEP_BLOCK]]" in event.content
        basic = "block" if "[[BLOCK]]" in event.content else "release"
        payload = {
            "event": raw,
            "screen": {
                "inspector": "fixture-light-v1",
                "disposition": basic,
                "requires_deep": needs_deep,
                "sequence_status": sequence_status,
                "screened_at": utc_now().isoformat(),
            },
        }
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO screened VALUES (?, ?)", (event.event_id, json.dumps(payload)))
            self.db.execute("""INSERT INTO session_state VALUES (?, ?)
                ON CONFLICT(session_id) DO UPDATE SET max_sequence=max(max_sequence, excluded.max_sequence)""",
                (event.session_id, event.sequence),
            )
        self.record_stage(event.event_id, "screen", "complete", sequence_status)
        return payload

    def enqueue(self, screened: dict[str, Any], limit: int) -> str:
        event = Event.from_dict(screened["event"])
        row = self.db.execute("SELECT state FROM queue WHERE event_id=?", (event.event_id,)).fetchone()
        if row:
            return "overload" if row[0] == "overload" else "duplicate"
        queued = self.db.execute("SELECT count(*) FROM queue WHERE state IN ('pending', 'inflight')").fetchone()[0]
        state = "overload" if queued >= limit else "pending"
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO queue VALUES (?, ?, ?, ?)",
                (event.event_id, json.dumps(screened), state, utc_now().isoformat()),
            )
        self.record_stage(event.event_id, "scheduler", state)
        return state

    def pending(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT payload, enqueued_at FROM queue WHERE state='pending' ORDER BY enqueued_at, event_id"
        ).fetchall()
        return [{"screened": json.loads(payload), "enqueued_at": enqueued_at} for payload, enqueued_at in rows]

    def inflight_count(self) -> int:
        return self.db.execute("SELECT count(*) FROM queue WHERE state='inflight'").fetchone()[0]

    def expired_inflight(self) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT payload, enqueued_at FROM queue WHERE state='inflight'").fetchall()
        return [
            {"screened": json.loads(payload), "enqueued_at": enqueued_at}
            for payload, enqueued_at in rows
            if utc_now() >= parse_time(json.loads(payload)["event"]["deadline"])
        ]

    def mark(self, event_id: str, state: str) -> None:
        with self.db:
            self.db.execute("UPDATE queue SET state=? WHERE event_id=?", (state, event_id))

    def finalize(self, result: dict[str, Any]) -> dict[str, Any]:
        event = Event.from_dict(result["event"])
        if result["route"] not in {"basic", "deep", "hold"} or result["disposition"] not in {"release", "block", "timeout"}:
            raise ValueError("invalid inspection result")
        row = self.db.execute("SELECT payload FROM decisions WHERE event_id=?", (event.event_id,)).fetchone()
        if row:
            return json.loads(row[0])
        now = utc_now()
        expired = now >= parse_time(event.deadline)
        disposition = "timeout" if expired else result["disposition"]
        assistant_output, proposal = assistant_fixture(event) if disposition == "release" else (None, None)
        gate_outcome = authorize_action(event, proposal)
        decision = {
            "event_id": event.event_id,
            "session_id": event.session_id,
            "sequence": event.sequence,
            "route": result["route"],
            "disposition": disposition,
            "reason_code": "deadline_expired" if expired else result["reason_code"],
            "queue_age_ms": result.get("queue_age_ms", 0),
            "proposed_action": proposal,
            "assistant_output": assistant_output,
            "gate_outcome": gate_outcome,
            "processed_at": now.isoformat(),
        }
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO decisions VALUES (?, ?)", (event.event_id, json.dumps(decision)))
            if gate_outcome == "allowed" and proposal:
                self.db.execute(
                    "INSERT OR IGNORE INTO mock_outbox VALUES (?, ?, ?)",
                    (event.event_id, proposal["recipient"], proposal["body"]),
                )
        return decision

    def outbox(self) -> list[dict[str, str]]:
        rows = self.db.execute("SELECT event_id, recipient, body FROM mock_outbox ORDER BY event_id").fetchall()
        return [dict(zip(("event_id", "recipient", "body"), row)) for row in rows]

    def queue_status(self) -> list[dict[str, str]]:
        rows = self.db.execute("SELECT event_id, state FROM queue ORDER BY event_id").fetchall()
        return [{"event_id": event_id, "state": state} for event_id, state in rows]

    def timeline(self, event_id: str) -> list[dict[str, str]]:
        rows = self.db.execute(
            "SELECT stage, status, recorded_at, detail FROM stage_events WHERE event_id=? ORDER BY recorded_at, rowid",
            (event_id,),
        ).fetchall()
        return [dict(zip(("stage", "status", "recorded_at", "detail"), row)) for row in rows]

    def recent_events(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.execute("""SELECT s.event_id, s.payload, q.state, d.payload
            FROM screened AS s
            LEFT JOIN queue AS q USING (event_id)
            LEFT JOIN decisions AS d USING (event_id)
            ORDER BY s.rowid DESC LIMIT ?""", (limit,)).fetchall()
        events = []
        for event_id, screened_json, queue_state, decision_json in rows:
            screened = json.loads(screened_json)
            event = screened["event"]
            decision = json.loads(decision_json) if decision_json else None
            events.append({
                "event_id": event_id,
                "session_id": event["session_id"],
                "sequence": event["sequence"],
                "task_type": event["task_type"],
                "task_prompt": event.get("task_prompt", ""),
                "allowed_recipients": event.get("allowed_recipients", []),
                "content": event["content"],
                "screen": screened["screen"],
                "queue_state": queue_state,
                "decision": decision,
            })
        return events

    def metrics(self, run_id: str | None = None) -> dict[str, Any]:
        rows = self.db.execute("""SELECT d.event_id, d.payload, s.payload
            FROM decisions AS d LEFT JOIN screened AS s USING (event_id)""").fetchall()
        decisions = []
        latencies = []
        for event_id, decision_json, screened_json in rows:
            if run_id and not event_id.startswith(run_id + "-"):
                continue
            decision = json.loads(decision_json)
            decisions.append(decision)
            if screened_json:
                arrival = json.loads(screened_json)["event"].get("replay_arrival_at")
                if arrival:
                    latency = (parse_time(decision["processed_at"]) - parse_time(arrival)).total_seconds() * 1000
                    latencies.append(max(0, latency))
        latencies.sort()
        percentile = lambda p: round(latencies[max(0, math.ceil(p * len(latencies)) - 1)], 1) if latencies else None
        return {
            "decisions": len(decisions),
            "routes": dict(Counter(item["route"] for item in decisions)),
            "dispositions": dict(Counter(item["disposition"] for item in decisions)),
            "gate_outcomes": dict(Counter(item["gate_outcome"] for item in decisions)),
            "end_to_end_p50_ms": percentile(0.5),
            "end_to_end_p95_ms": percentile(0.95),
        }


def result_payload(screened: dict[str, Any], route: str, disposition: str, reason: str, enqueued_at: str) -> dict[str, Any]:
    age = max(0, int((utc_now() - parse_time(enqueued_at)).total_seconds() * 1000))
    return {
        "event": screened["event"],
        "route": route,
        "disposition": disposition,
        "reason_code": reason,
        "queue_age_ms": age,
        "inspector": "fixture-deep-v1" if route == "deep" else "fixture-light-v1",
        "completed_at": utc_now().isoformat(),
    }


def schedule_once(store: StreamStore, publish, deep_capacity: int) -> None:
    """Dispatch all currently feasible work; held events stay durable in SQLite."""
    for item in store.expired_inflight():
        screened = item["screened"]
        event = Event.from_dict(screened["event"])
        result = result_payload(screened, "deep", "timeout", "deadline_expired", item["enqueued_at"])
        publish(RESULTS, event.session_id, result)
        store.mark(event.event_id, "complete")
        store.record_stage(event.event_id, "scheduler", "timeout")
    slots = max(0, deep_capacity - store.inflight_count())
    for item in store.pending():
        screened = item["screened"]
        event = Event.from_dict(screened["event"])
        if utc_now() >= parse_time(event.deadline):
            result = result_payload(screened, "hold", "timeout", "deadline_expired", item["enqueued_at"])
            publish(RESULTS, event.session_id, result)
            store.mark(event.event_id, "complete")
            store.record_stage(event.event_id, "scheduler", "timeout")
        elif not screened["screen"]["requires_deep"]:
            result = result_payload(screened, "basic", screened["screen"]["disposition"], "fixture_basic", item["enqueued_at"])
            publish(RESULTS, event.session_id, result)
            store.mark(event.event_id, "complete")
            store.record_stage(event.event_id, "scheduler", "basic")
        elif slots > 0:
            request = {"event": screened["event"], "enqueued_at": item["enqueued_at"], "route": "deep"}
            publish(REQUESTS, event.session_id, request)
            store.mark(event.event_id, "inflight")
            store.record_stage(event.event_id, "scheduler", "deep_request")
            slots -= 1
        else:
            store.record_stage(event.event_id, "scheduler", "held", "deep_capacity_exhausted")


def deep_inspect(request: dict[str, Any], delay_seconds: float = 0.25) -> dict[str, Any]:
    event = Event.from_dict(request["event"])
    time.sleep(delay_seconds)
    disposition = "block" if "[[DEEP_BLOCK]]" in event.content else "release"
    return result_payload(
        {"event": request["event"]}, "deep", disposition,
        "fixture_deep_block" if disposition == "block" else "fixture_deep_release",
        request["enqueued_at"],
    )
