"""Thin FreshService v2 API client.

Read-only, paginated, with retry/backoff on 429 and 5xx. Deliberately narrow:
list tickets (incremental), read one ticket with conversations.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Iterator, List, Optional

import requests

from .config import Settings

_RETRYABLE = {429, 500, 502, 503, 504}


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
        include_stats: bool = False,
        order_by: Optional[str] = None,
        order_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Yield tickets, paging forward.

        ``updated_since`` is an ISO-8601 date(time) for incremental pulls.
        ``order_by``/``order_type`` (e.g. ``created_at`` / ``desc``) make the
        window deterministic so "the last N tickets" means something.
        ``limit`` stops the pull early instead of fetching then slicing.

        FreshService caps offset paging at 9000 records (page * per_page); a
        larger backfill needs date-window slicing on ``created_at``.
        """

        page = 1
        per_page = max(1, min(self.settings.fs_per_page, 100))
        seen = 0
        while True:
            params: Dict[str, Any] = {"page": page, "per_page": per_page}
            if updated_since:
                params["updated_since"] = updated_since
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
                yield t
                seen += 1
                if limit is not None and seen >= limit:
                    return
            if len(tickets) < per_page:
                return
            page += 1
            if page > 90:  # 90 * 100 = 9000, the FS offset-paging cap
                return

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
