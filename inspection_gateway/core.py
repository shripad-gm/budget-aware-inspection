"""Event schema, rule-based task output, and fixed action authorization."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .mail import classify_email, deep_inspection, requested_recipient

def propose_action(event: "Event") -> dict[str, str] | None:
    """Extract a candidate from email text, or draft for a trusted send task."""
    recipient = requested_recipient(event.content)
    if recipient is None and event.task_type == "send" and event.allowed_recipients:
        recipient = event.allowed_recipients[0]
    if recipient:
        return {
            "action": "send",
            "recipient": recipient,
            "body": f"Subject: Fwd: {event.subject or '(no subject)'}\n\n{event.content[:2000]}",
        }
    return None


def authorize_action(event: "Event", proposal: dict[str, str] | None) -> str:
    if proposal is None:
        return "not_applicable"
    return "allowed" if event.task_type == "send" and proposal["recipient"] in event.allowed_recipients else "denied"


def assistant_rules(event: "Event") -> tuple[str, dict[str, str] | None]:
    """Produce a simple local summary, lookup answer, or send draft."""
    proposal = propose_action(event)
    clean_content = event.content.strip()
    if event.task_type == "summarize":
        sentences = [piece.strip() for piece in re.split(r"(?<=[.!?])\s+|\n+", clean_content) if piece.strip()]
        preview = " ".join(sentences[:2])[:300]
        return (f"Summary: {preview}" if preview else "No readable email text to summarize."), proposal
    if event.task_type == "lookup":
        terms = {
            term.lower() for term in re.findall(r"[A-Za-z]{3,}", event.task_prompt)
            if term.lower() not in {"the", "and", "what", "when", "where", "who", "does", "this", "email", "about", "is", "are"}
        }
        sentences = [piece.strip() for piece in re.split(r"(?<=[.!?])\s+", clean_content) if piece.strip()]
        match = next((sentence for sentence in sentences if any(term in sentence.lower() for term in terms)), None)
        return (f"Found in email: {match}" if match else "No matching fact found in this email."), proposal
    recipient = proposal["recipient"] if proposal else "no recipient"
    return f"Draft ready for {recipient}; the local authorization gate will check it.", proposal


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
    sender: str = ""
    subject: str = ""
    message_date: str = ""
    message_id: str = ""
    source: str = ""

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
        optional = ("sender", "subject", "message_date", "message_id", "source")
        if any(not isinstance(data.get(name, ""), str) for name in optional):
            raise ValueError("email metadata must be strings")
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
            sender=data.get("sender", ""),
            subject=data.get("subject", ""),
            message_date=data.get("message_date", ""),
            message_id=data.get("message_id", ""),
            source=data.get("source", ""),
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
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mock_outbox'").fetchone():
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='draft_outbox'").fetchone():
                self.db.execute("INSERT OR IGNORE INTO draft_outbox SELECT * FROM mock_outbox")
                self.db.execute("DROP TABLE mock_outbox")
            else:
                self.db.execute("ALTER TABLE mock_outbox RENAME TO draft_outbox")
        self.db.execute("""CREATE TABLE IF NOT EXISTS draft_outbox (
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
        screen = classify_email(event.sender, event.subject, event.content)
        disposition = "timeout" if expired else screen["disposition"]
        if not expired and screen["requires_deep"]:
            disposition, reason = deep_inspection(event.subject, event.content)
        else:
            reason = screen["reason_code"]
        inspection = {
            "event_id": event.event_id,
            "session_id": event.session_id,
            "inspector": "rules-v1",
            "category": screen["category"],
            "signals": screen["signals"],
            "disposition": disposition,
            "reason_code": "deadline_expired" if expired else reason,
        }
        proposal: dict[str, str] | None = None
        assistant_output: str | None = None
        gate_outcome = "not_applicable"
        if disposition == "release":
            assistant_output, proposal = assistant_rules(event)
            gate_outcome = authorize_action(event, proposal)
        decision = {
            "event_id": event.event_id,
            "session_id": event.session_id,
            "sequence": event.sequence,
            "route": "deep" if screen["requires_deep"] else "basic",
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
                    "INSERT OR IGNORE INTO draft_outbox VALUES (?, ?, ?)",
                    (event.event_id, proposal["recipient"], proposal["body"]),
                )
        return self.get(event.event_id)  # type: ignore[return-value]

    def outbox(self) -> list[dict[str, str]]:
        rows = self.db.execute("SELECT event_id, recipient, body FROM draft_outbox ORDER BY event_id").fetchall()
        return [dict(zip(("event_id", "recipient", "body"), row)) for row in rows]


def event_payload(event: Event) -> dict[str, Any]:
    result = asdict(event)
    result["allowed_recipients"] = list(event.allowed_recipients)
    return result
