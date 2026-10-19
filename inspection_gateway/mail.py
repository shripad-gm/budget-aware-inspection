"""Parse uploaded RFC 5322 email and apply transparent, deterministic rules."""

from __future__ import annotations

import re
from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from email.utils import parseaddr
from html.parser import HTMLParser


MAX_EMAIL_BYTES = 2_000_000
ADDRESS = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
SEND_REQUEST = re.compile(r"\b(?:send|forward|email)\b.{0,100}?\bto\s+(" + ADDRESS.pattern + r")", re.I | re.S)
OVERRIDE = re.compile(r"\b(?:ignore|disregard|override)\s+(?:all\s+)?(?:previous|prior|system|developer)\s+(?:instructions|messages|rules)\b", re.I)
ASSISTANT_DIRECTIVE = re.compile(r"\b(?:ai\s+assistant|assistant|agent|system\s+prompt|developer\s+message)\b", re.I)
CREDENTIAL = re.compile(r"\b(?:password|credentials|api[ -]?key|secret|access[ -]?token)\b", re.I)
DISCLOSE = re.compile(r"\b(?:send|forward|share|reveal|disclose|export)\b", re.I)
VERIFY = re.compile(r"\b(?:verify your account|reset your password|confirm your login)\b", re.I)


@dataclass(frozen=True)
class ParsedEmail:
    sender: str
    subject: str
    body: str
    message_date: str
    message_id: str
    attachment_count: int


class _HTMLText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"script", "style"}:
            self.hidden += 1
        elif tag in {"p", "div", "br", "li", "tr", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self.hidden:
            self.hidden -= 1
        elif tag in {"p", "div", "li", "tr", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)


def _part_text(part) -> str:
    try:
        content = part.get_content()
    except (LookupError, UnicodeError):
        content = (part.get_payload(decode=True) or b"").decode("utf-8", errors="replace")
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    if part.get_content_type() == "text/html":
        parser = _HTMLText()
        parser.feed(content)
        content = "".join(parser.parts)
    return re.sub(r"[ \t]+", " ", content).strip()


def parse_eml(raw: bytes) -> ParsedEmail:
    if not raw or len(raw) > MAX_EMAIL_BYTES:
        raise ValueError("Email file must be between 1 byte and 2 MB")
    message = BytesParser(policy=policy.default).parsebytes(raw)
    sender = parseaddr(str(message.get("From", "")))[1]
    if not sender or "@" not in sender:
        raise ValueError("Email needs a valid From address")
    plain, html = [], []
    attachments = 0
    for part in message.walk():
        if part.is_multipart():
            continue
        if part.get_content_disposition() == "attachment":
            attachments += 1
            continue
        kind = part.get_content_type()
        if kind == "text/plain":
            plain.append(_part_text(part))
        elif kind == "text/html":
            html.append(_part_text(part))
    readable_plain = [piece for piece in plain if piece]
    body = "\n\n".join(readable_plain or [piece for piece in html if piece]).strip()
    if not body:
        raise ValueError("Email needs a readable plain-text or HTML body")
    if len(body) > 100_000:
        raise ValueError("Decoded email body exceeds 100,000 characters")
    return ParsedEmail(
        sender=sender,
        subject=str(message.get("Subject", "")) or "(no subject)",
        body=body,
        message_date=str(message.get("Date", "")),
        message_id=str(message.get("Message-ID", "")),
        attachment_count=attachments,
    )


def requested_recipient(body: str) -> str | None:
    match = SEND_REQUEST.search(body)
    return match.group(1) if match else None


def classify_email(sender: str, subject: str, body: str) -> dict:
    """Return auditable labels; these rules are illustrative, not a trained detector."""
    text = f"{subject}\n{body}".lower()
    if re.search(r"\b(invoice|receipt|payment|billing|purchase order)\b", text):
        category = "finance"
    elif re.search(r"\b(meeting|agenda|calendar|appointment|schedule)\b", text):
        category = "meeting"
    elif re.search(r"\b(newsletter|unsubscribe|weekly digest)\b", text):
        category = "newsletter"
    elif re.search(r"\b(ticket|support request|incident|bug report)\b", text):
        category = "support"
    else:
        category = "general"
    signals = []
    if OVERRIDE.search(body):
        signals.append("instruction_override")
    if ASSISTANT_DIRECTIVE.search(body) and (OVERRIDE.search(body) or requested_recipient(body) or DISCLOSE.search(body)):
        signals.append("assistant_directive")
    if requested_recipient(body):
        signals.append("requested_external_send")
    if CREDENTIAL.search(body) and DISCLOSE.search(body):
        signals.append("credential_disclosure_request")
    if VERIFY.search(body):
        signals.append("account_verification_language")
    disposition = "block" if "instruction_override" in signals and "assistant_directive" in signals else "release"
    requires_deep = disposition == "release" and bool(signals)
    return {
        "category": category,
        "signals": signals,
        "disposition": disposition,
        "requires_deep": requires_deep,
        "reason_code": "explicit_assistant_override" if disposition == "block" else ("rule_review" if requires_deep else "no_risk_rule_match"),
    }


def deep_inspection(subject: str, body: str) -> tuple[str, str]:
    """A second pass with stricter combinations of explicit risk signals."""
    if OVERRIDE.search(body) and ASSISTANT_DIRECTIVE.search(body):
        return "block", "explicit_assistant_override"
    if ASSISTANT_DIRECTIVE.search(body) and CREDENTIAL.search(body) and DISCLOSE.search(body):
        return "block", "assistant_credential_exfiltration"
    if VERIFY.search(body) and re.search(r"https?://\S+", body) and CREDENTIAL.search(body):
        return "block", "credential_phishing_pattern"
    return "release", "no_blocking_rule_match"
