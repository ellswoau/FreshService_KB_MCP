"""Thin FreshService v2 API client.

Paginated reads, with retry/backoff on 429 and 5xx. Deliberately narrow:
list tickets (incremental by updated OR created time), read one ticket with
conversations, and a single narrow write -- a private note (used by the
correlated-ticket monitor). No other field is ever written.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import requests

from .config import Settings

_RETRYABLE = {429, 500, 502, 503, 504}


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _created_at_gte(created_at: Any, since: str) -> bool:
    """True when ``created_at`` >= ``since`` (unparseable values are kept)."""

    c = _parse_iso(created_at)
    s = _parse_iso(since)
    if c is None or s is None:
        return True
    return c >= s


class FreshServiceError(RuntimeError):
    pass


class FreshServiceClient:
    def __init__(self, settings: Settings, session: Optional[requests.Session] = None):
        settings.require_freshservice()
        self.settings = settings
        self.base = settings.fs_base_url
        self.session = session or requests.Session()
        self.session.auth = (settings.fs_api_key or "", "X")  # FS basic auth: key as username
        self.session.headers.update({"Accept": "application/json"})

    # --- low level -------------------------------------------------------
    def _get(self, path: str, params: Optional[dict] = None, max_retries: int = 5):
        url = f"{self.base}{path}"
        backoff = 1.0
        for attempt in range(max_retries + 1):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code in _RETRYABLE:
                if attempt == max_retries:
                    raise FreshServiceError(f"GET {path} failed: HTTP {resp.status_code}")
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                time.sleep(min(delay, 30.0))
                backoff = min(backoff * 2, 30.0)
                continue
            if resp.status_code >= 400:
                raise FreshServiceError(f"GET {path} failed: HTTP {resp.status_code} {resp.text[:300]}")
            return resp.json()
        raise FreshServiceError(f"GET {path}: retries exhausted")

    # --- tickets ---------------------------------------------------------
    def list_tickets(
        self,
        updated_since: Optional[str] = None,
        created_since: Optional[str] = None,
        include_stats: bool = False,
        order_by: Optional[str] = None,
        order_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Yield tickets, paging forward.

        ``updated_since`` is an ISO-8601 date(time) for incremental pulls by
        last-modification. ``created_since`` filters to tickets *created* at/after
        that time -- what the correlated-ticket monitor needs, since it cares
        about when a ticket appeared. FreshService has no server-side
        ``created_since`` (it 400s), so this is done as an ``updated_since``
        pull (a safe superset) plus a local ``created_at`` filter.
        ``order_by``/``order_type`` (e.g. ``created_at`` / ``desc``) make the
        window deterministic so "the last N tickets" means something.
        ``limit`` stops the pull early instead of fetching then slicing.

        FreshService caps offset paging at 9000 records (page * per_page); a
        larger backfill needs date-window slicing on ``created_at``.
        """

        page = 1
        per_page = max(1, min(self.settings.fs_per_page, 100))
        seen = 0
        # FreshService has NO server-side ``created_since`` filter -- it rejects
        # the field with HTTP 400 "Unexpected/invalid field in request". A
        # newly-created ticket is always also recently *updated*, so
        # ``updated_since`` is a safe superset; filter on ``created_at`` locally
        # to recover exact created-since semantics.
        api_updated_since = updated_since or created_since
        while True:
            params: Dict[str, Any] = {"page": page, "per_page": per_page}
            if api_updated_since:
                params["updated_since"] = api_updated_since
            if order_by:
                params["order_by"] = order_by
            if order_type:
                params["order_type"] = order_type
            if include_stats:
                params["include_stats"] = "true"
            payload = self._get("/api/v2/tickets", params=params)
            tickets: List[dict] = payload.get("tickets", []) if isinstance(payload, dict) else []
            if not tickets:
                return
            for t in tickets:
                if created_since and not _created_at_gte(t.get("created_at"), created_since):
                    continue
                yield t
                seen += 1
                if limit is not None and seen >= limit:
                    return
            if len(tickets) < per_page:
                return
            page += 1
            if page > 90:  # 90 * 100 = 9000, the FS offset-paging cap
                return

    # --- narrow write: private note -------------------------------------
    def _post(self, path: str, files: Optional[dict] = None, max_retries: int = 5):
        """POST with the same 429/5xx retry/backoff contract as ``_get``."""

        url = f"{self.base}{path}"
        backoff = 1.0
        for attempt in range(max_retries + 1):
            resp = self.session.post(url, files=files, timeout=30)
            if resp.status_code in _RETRYABLE:
                if attempt == max_retries:
                    raise FreshServiceError(f"POST {path} failed: HTTP {resp.status_code}")
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                time.sleep(min(delay, 30.0))
                backoff = min(backoff * 2, 30.0)
                continue
            if resp.status_code >= 400:
                raise FreshServiceError(f"POST {path} failed: HTTP {resp.status_code} {resp.text[:300]}")
            return resp.json() if resp.content else {}
        raise FreshServiceError(f"POST {path}: retries exhausted")

    def add_private_note(self, ticket_id: int, body: str, private: bool = True) -> Dict[str, Any]:
        """Add a note to a ticket. Defaults to a PRIVATE (internal) note.

        FreshService's note endpoint expects multipart form-data; passing
        ``files`` with ``(None, value)`` produces form fields without a
        filename, which is exactly what the API accepts.
        """

        files = {
            "body": (None, body),
            "private": (None, "true" if private else "false"),
        }
        payload = self._post(f"/api/v2/tickets/{int(ticket_id)}/notes", files=files)
        if isinstance(payload, dict) and "note" in payload:
            return payload["note"]
        return payload if isinstance(payload, dict) else {}

    def get_ticket(self, ticket_id: int, include_conversations: bool = True) -> Dict[str, Any]:
        params = {"include": "conversations"} if include_conversations else None
        payload = self._get(f"/api/v2/tickets/{ticket_id}", params=params)
        if isinstance(payload, dict) and "ticket" in payload:
            return payload["ticket"]
        return payload if isinstance(payload, dict) else {}

    def get_conversations(self, ticket_id: int) -> List[Dict[str, Any]]:
        payload = self._get(f"/api/v2/tickets/{ticket_id}/conversations", params={"per_page": 100})
        if isinstance(payload, dict):
            return payload.get("conversations", []) or []
        return payload or []
