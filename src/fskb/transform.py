"""Transform a raw FreshService ticket (plus conversations) into a KbDocument.

The split that matters:
  symptom    = ticket description + first requester reply(s)  -> embedded
  resolution = resolution custom field, else last private note,
               else last public reply                          -> stored

An optional IT Glue link map can populate the provenance fields.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .enrich import enrich
from .models import KbDocument, is_terminal_status, slugify_id
from .sanitize import clean_text, is_secret_only, sanitize


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _iso(value: Any) -> Optional[str]:
    dt = _parse_dt(value)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def _normalize_store(custom_fields: Dict[str, Any]) -> List[str]:
    raw = None
    if isinstance(custom_fields, dict):
        for key in ("msf_store", "store", "location"):
            if custom_fields.get(key) not in (None, "", []):
                raw = custom_fields[key]
                break
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def _conv_text(conv: Dict[str, Any]) -> str:
    for key in ("body_text", "body"):
        if conv.get(key):
            return str(conv[key])
    return ""


def _is_requester_visible(conv: Dict[str, Any]) -> bool:
    """Public reply vs private note.

    FreshService uses ``private: true`` for internal notes; ``incoming: true``
    means it came from the requester.
    """

    private = conv.get("private")
    return not bool(private)


def _is_incoming(conv: Dict[str, Any]) -> bool:
    return bool(conv.get("incoming"))


def build_symptom(ticket: Dict[str, Any], conversations: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    desc = ticket.get("description_text") or ticket.get("description") or ""
    if desc:
        parts.append(clean_text(str(desc)))
    # First couple of requester-visible incoming replies add symptom context.
    incoming_seen = 0
    for conv in conversations:
        if _is_incoming(conv) and incoming_seen < 2:
            text = clean_text(_conv_text(conv))
            if text:
                parts.append(text)
                incoming_seen += 1
    return "\n".join(p for p in parts if p).strip()


def build_resolution(ticket: Dict[str, Any], conversations: List[Dict[str, Any]]):
    """Return (resolution_text, source). Prefers the explicit resolution field,
    then the last private note, then the last public reply."""

    custom_fields = ticket.get("custom_fields") or {}
    if isinstance(custom_fields, dict):
        for key in ("resolution", "resolution_notes"):
            value = custom_fields.get(key)
            if value not in (None, "", []):
                text = clean_text(str(value))
                if text:
                    return text, "resolution_field"

    private_notes = [c for c in conversations if not _is_requester_visible(c) and _conv_text(c)]
    if private_notes:
        private_notes.sort(key=lambda c: _parse_dt(c.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
        text = clean_text(_conv_text(private_notes[-1]))
        if text:
            return text, "private_note"

    replies = [c for c in conversations if _is_requester_visible(c) and not _is_incoming(c) and _conv_text(c)]
    if replies:
        replies.sort(key=lambda c: _parse_dt(c.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
        text = clean_text(_conv_text(replies[-1]))
        if text:
            return text, "reply"

    return "", "unknown"


def qualifies(ticket: Dict[str, Any], symptom: str, resolution: str, min_symptom_chars: int = 15) -> bool:
    """Only resolved/closed tickets with a real symptom and a real fix."""

    if not is_terminal_status(ticket.get("status")):
        return False
    if len(symptom.strip()) < min_symptom_chars:
        return False
    if not resolution.strip():
        return False
    if is_secret_only(resolution):
        return False
    return True


def to_document(
    ticket: Dict[str, Any],
    conversations: Optional[List[Dict[str, Any]]] = None,
    itglue_links: Optional[Dict[str, Dict[str, str]]] = None,
    min_symptom_chars: int = 15,
) -> Optional[KbDocument]:
    """Build a KbDocument, or None when the ticket should not be indexed."""

    conversations = conversations or []
    ticket_id = ticket.get("id")
    if ticket_id is None:
        return None

    symptom_raw = build_symptom(ticket, conversations)
    resolution_raw, source = build_resolution(ticket, conversations)

    if not qualifies(ticket, symptom_raw, resolution_raw, min_symptom_chars):
        return None

    symptom = sanitize(symptom_raw)
    resolution = sanitize(resolution_raw)
    if not symptom or not resolution:
        return None

    linked_ids: List[str] = []
    linked_urls: List[str] = []
    links = (itglue_links or {}).get(str(ticket_id))
    if links:
        linked_ids = list(links.get("ids", []) or [])
        linked_urls = list(links.get("urls", []) or [])

    created = _parse_dt(ticket.get("created_at"))
    closed = _parse_dt(ticket.get("closed_at") or ticket.get("resolved_at"))
    age_days = None
    if created and closed:
        age_days = max(0, (closed - created).days)

    entities = enrich(symptom, resolution)

    return KbDocument(
        id=slugify_id(ticket_id),
        ticket_id=int(ticket_id),
        display_id=ticket.get("display_id"),
        subject=str(ticket.get("subject") or ""),
        symptom_text=symptom,
        resolution_text=resolution,
        resolution_source=source,
        has_resolution=True,
        category=ticket.get("category"),
        sub_category=ticket.get("sub_category"),
        item_category=ticket.get("item_category"),
        department=ticket.get("department_id") and str(ticket.get("department_id")) or ticket.get("department"),
        store=_normalize_store(ticket.get("custom_fields") or {}),
        group=ticket.get("group_id") and str(ticket.get("group_id")) or ticket.get("group"),
        ticket_type=ticket.get("type") or ticket.get("ticket_type"),
        tags=[str(t) for t in (ticket.get("tags") or [])],
        created_at=_iso(ticket.get("created_at")),
        closed_at=_iso(ticket.get("closed_at") or ticket.get("resolved_at")),
        updated_at=_iso(ticket.get("updated_at")),
        age_days=age_days,
        linked_itglue_doc_ids=linked_ids,
        linked_itglue_urls=linked_urls,
        **entities,
    )
