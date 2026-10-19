"""Deterministic, locally testable gateway core.

The inspector and assistant here are fixtures. They are deliberately not presented
as security models; their purpose is to exercise the transport and permission gate.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SEND_MARKER = re.compile(r"\[\[SEND:([^\]]+)\]\]")
FIXTURE_MARKER = re.compile(r"\[\[(?:SEND:[^\]]+|BLOCK|DEEP|DEEP_BLOCK)\]\]")


def propose_action(event: "Event") -> dict[str, str] | None:
    """Fixture assistant; email markers intentionally test the fixed gate."""
    marker = SEND_MARKER.search(event.content)
    if marker:
        return {"action": "send", "recipient": marker.group(1).strip(), "body": "Synthetic mock message"}
    if event.task_type == "send" and event.allowed_recipients:
        return {"action": "send", "recipient": event.allowed_recipients[0], "body": "Synthetic mock message"}
    return None


def authorize_action(event: "Event", proposal: dict[str, str] | None) -> str:
    if proposal is None:
        return "not_applicable"
    return "allowed" if event.task_type == "send" and proposal["recipient"] in event.allowed_recipients else "denied"


def assistant_fixture(event: "Event") -> tuple[str, dict[str, str] | None]:
    """Deterministic task output for displaying the agent boundary in the POC."""
    proposal = propose_action(event)
    clean_content = FIXTURE_MARKER.sub("", event.content).strip()
    if event.task_type == "summarize":
        return (f"Summary preview: {clean_content[:180]}" if clean_content else "No readable email text to summarize."), proposal
    if event.task_type == "lookup":
        terms = {
            term.lower() for term in re.findall(r"[A-Za-z]{3,}", event.task_prompt)
            if term.lower() not in {"the", "and", "what", "when", "where", "who", "does", "this", "email", "about", "is", "are"}
        }
        sentences = [piece.strip() for piece in re.split(r"(?<=[.!?])\s+", clean_content) if piece.strip()]
        match = next((sentence for sentence in sentences if any(term in sentence.lower() for term in terms)), None)
        return (f"Found in email: {match}" if match else "No matching fact found in this email."), proposal
    recipient = proposal["recipient"] if proposal else "no recipient"
    return f"Prepared a mock send to {recipient}.", proposal


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("deadline must include a timezone")
    return parsed


@dataclass(frozen=True)
class Event:
    event_id: str
    session_id: str
    sequence: int
    content: str
    task_type: str
    allowed_recipients: tuple[str, ...]
    deadline: str
    task_prompt: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        required = ("event_id", "session_id", "sequence", "content", "task_type", "deadline")
        if any(key not in data for key in required):
            raise ValueError("missing required event field")
        if not all(isinstance(data[key], str) and data[key] for key in ("event_id", "session_id", "deadline")):
            raise ValueError("event_id, session_id, and deadline must be nonempty strings")
        if not isinstance(data["sequence"], int) or data["sequence"] < 0:
            raise ValueError("sequence must be a nonnegative integer")
        if not isinstance(data["content"], str) or len(data["content"]) > 100_000:
            raise ValueError("content must be a string of at most 100,000 characters")
        if data["task_type"] not in {"summarize", "lookup", "send"}:
            raise ValueError("task_type must be summarize, lookup, or send")
        task_prompt = data.get("task_prompt", "")
        if not isinstance(task_prompt, str):
            raise ValueError("task_prompt must be a string")
        recipients = data.get("allowed_recipients", [])
        if not isinstance(recipients, list) or not all(isinstance(item, str) for item in recipients):
            raise ValueError("allowed_recipients must be a list of strings")
        parse_time(data["deadline"])
        return cls(
            event_id=data["event_id"],
            session_id=data["session_id"],
            sequence=data["sequence"],
            content=data["content"],
            task_type=data["task_type"],
            allowed_recipients=tuple(recipients),
            deadline=data["deadline"],
            task_prompt=task_prompt,
        )


class GatewayStore:
    def __init__(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS decisions (
            event_id TEXT PRIMARY KEY, inspection_json TEXT NOT NULL,
            decision_json TEXT NOT NULL
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS mock_outbox (
            event_id TEXT PRIMARY KEY, recipient TEXT NOT NULL, body TEXT NOT NULL
        )""")
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def get(self, event_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        row = self.db.execute(
            "SELECT inspection_json, decision_json FROM decisions WHERE event_id=?", (event_id,)
        ).fetchone()
        return (json.loads(row[0]), json.loads(row[1])) if row else None

    def process(self, event: Event, now: datetime | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        existing = self.get(event.event_id)
        if existing:
            return existing
        now = now or utc_now()
        expired = now >= parse_time(event.deadline)
        # Stub inspector: [[BLOCK]] is a synthetic test signal, not a detector.
        disposition = "timeout" if expired else ("block" if "[[BLOCK]]" in event.content else "release")
        inspection = {
            "event_id": event.event_id,
            "session_id": event.session_id,
            "inspector": "fixture-v1",
            "disposition": disposition,
            "reason_code": "deadline_expired" if expired else ("fixture_block" if disposition == "block" else "fixture_release"),
        }
        proposal: dict[str, str] | None = None
        assistant_output: str | None = None
        gate_outcome = "not_applicable"
        if disposition == "release":
            assistant_output, proposal = assistant_fixture(event)
            gate_outcome = authorize_action(event, proposal)
        decision = {
            "event_id": event.event_id,
            "session_id": event.session_id,
            "sequence": event.sequence,
            "route": "fixture_basic",
            "disposition": disposition,
            "proposed_action": proposal,
            "assistant_output": assistant_output,
            "gate_outcome": gate_outcome,
            "processed_at": now.isoformat(),
        }
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO decisions VALUES (?, ?, ?)",
                (event.event_id, json.dumps(inspection), json.dumps(decision)),
            )
            if gate_outcome == "allowed" and proposal is not None:
                self.db.execute(
                    "INSERT OR IGNORE INTO mock_outbox VALUES (?, ?, ?)",
                    (event.event_id, proposal["recipient"], proposal["body"]),
                )
        return self.get(event.event_id)  # type: ignore[return-value]

    def outbox(self) -> list[dict[str, str]]:
        rows = self.db.execute("SELECT event_id, recipient, body FROM mock_outbox ORDER BY event_id").fetchall()
        return [dict(zip(("event_id", "recipient", "body"), row)) for row in rows]


def event_payload(event: Event) -> dict[str, Any]:
    result = asdict(event)
    result["allowed_recipients"] = list(event.allowed_recipients)
    return result
