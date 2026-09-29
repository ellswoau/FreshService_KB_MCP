"""Sanitize and redact ticket text before it is embedded or indexed.

Goals, in priority order:
1. Never let secrets (BitLocker recovery keys, passwords, tokens) into the index.
2. Remove boilerplate that adds noise to embeddings (signatures, quoted chains).
3. Redact PII (emails, phones) so the KB carries fixes, not people.
"""

from __future__ import annotations

import re
from typing import List, Tuple

from .models import _BITLOCKER_GUID, _PASSWORDISH

_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_PHONE = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}(?!\d)")

# Signature / boilerplate line markers.
_SIGNATURE_MARKERS = [
    re.compile(r"^\s*(?:thanks|thank you|regards|best|sincerely|cheers)[,!.\s]*$", re.IGNORECASE),
    re.compile(r"^\s*(?:sent from my|get outlook for)\b", re.IGNORECASE),
    re.compile(r"^\s*[-_]{2,}\s*$"),
    re.compile(r"^\s*(?:this email|the information contained|confidentiality notice|disclaimer)\b", re.IGNORECASE),
]
# Quoted-reply markers: keep everything before the first one.
_QUOTE_MARKERS = [
    re.compile(r"^\s*on .{0,80}\bwrote:\s*$", re.IGNORECASE),
    re.compile(r"^\s*-{2,}\s*original message\s*-{2,}\s*$", re.IGNORECASE),
    re.compile(r"^\s*from:\s.+$", re.IGNORECASE),
    re.compile(r"^\s*_{5,}\s*$"),
]
_BOILERPLATE_OPENERS = [
    re.compile(r"^\s*(?:hello|hi|hey|good morning|good afternoon)[ ,].{0,60}?(?:thank you for contacting|thanks for contacting).*$", re.IGNORECASE),
]

# HTML that FreshService bodies sometimes carry.
_TAG = re.compile(r"<[^>]+>")
_HTML_ENTITIES = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&#39;": "'", "&quot;": '"'}


def strip_html(text: str) -> str:
    if not text:
        return ""
    out = _TAG.sub(" ", text)
    for entity, char in _HTML_ENTITIES.items():
        out = out.replace(entity, char)
    return out


def _cut_at_first_quote(lines: List[str]) -> List[str]:
    for idx, line in enumerate(lines):
        if any(marker.match(line) for marker in _QUOTE_MARKERS):
            return lines[:idx]
    return lines


def _drop_signature_tail(lines: List[str]) -> List[str]:
    for idx, line in enumerate(lines):
        if any(marker.match(line) for marker in _SIGNATURE_MARKERS):
            return lines[:idx]
    return lines


def clean_text(text: str) -> str:
    """Remove HTML, quoted chains, signatures and boilerplate openers."""

    text = strip_html(text or "")
    lines = [ln.rstrip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln.strip()]
    lines = _cut_at_first_quote(lines)
    lines = _drop_signature_tail(lines)
    lines = [ln for ln in lines if not any(marker.match(ln) for marker in _BOILERPLATE_OPENERS)]
    cleaned = "\n".join(lines).strip()
    return re.sub(r"\n{3,}", "\n\n", cleaned)


def redact(text: str, redact_contacts: bool = True) -> str:
    """Replace secret/PII tokens with placeholders."""

    if not text:
        return ""
    # GUID first: otherwise the broader password pattern would swallow the key
    # value and label it a generic secret.
    text = _BITLOCKER_GUID.sub("<redacted-key>", text)
    text = _PASSWORDISH.sub("<redacted-secret>", text)
    if redact_contacts:
        text = _EMAIL.sub("<email>", text)
        text = _PHONE.sub("<phone>", text)
    return text


def contains_secret(text: str) -> bool:
    if not text:
        return False
    return bool(_PASSWORDISH.search(text) or _BITLOCKER_GUID.search(text))


def sanitize(text: str) -> str:
    """Full pipeline: clean then redact."""

    return redact(clean_text(text))


def is_secret_only(text: str) -> bool:
    """True when, after removing secret material, almost nothing remains.

    Used to drop records whose only 'resolution' was a recovery key/password.
    """

    if not text or not contains_secret(text):
        return False
    remainder = redact(text).replace("<redacted-secret>", "").replace("<redacted-key>", "")
    remainder = re.sub(r"[\s\W]+", "", remainder)
    return len(remainder) < 20
