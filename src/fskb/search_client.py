"""Azure AI Search client: document push (mergeOrUpload) and hybrid query.

Query shape used by the consuming tool:
  - optional OData pre-filter (category/store/app),
  - BM25 over subject/symptom/resolution,
  - k-NN over the symptom vector,
  - fused with reciprocal rank fusion + semantic reranker,
  - a client-side recency/frequency-style boost applied by the caller.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import requests

from .config import Settings

_RETRYABLE = {429, 500, 502, 503, 504}


class SearchError(RuntimeError):
    pass


class SearchClient:
    def __init__(self, settings: Settings, session: Optional[requests.Session] = None):
        settings.require_search()
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.update(
            {"api-key": settings.search_api_key or "", "Content-Type": "application/json"}
        )
        self.api = settings.search_api_version
        self.base = settings.search_endpoint
        self.index = settings.search_index_name

    # --- indexing --------------------------------------------------------
    def upload_documents(self, docs: Iterable[dict], max_retries: int = 5) -> List[dict]:
        """Push documents with mergeOrUpload. Idempotent by key."""

        docs = list(docs)
        if not docs:
            return []
        url = f"{self.base}/indexes/{self.index}/docs/index?api-version={self.api}"
        results: List[dict] = []
        size = max(1, self.settings.batch_size)
        for start in range(0, len(docs), size):
            chunk = docs[start : start + size]
            payload = {"value": [{"@search.action": "mergeOrUpload", **doc} for doc in chunk]}
            import time

            backoff = 1.0
            for attempt in range(max_retries + 1):
                resp = self.session.post(url, json=payload, timeout=60)
                if resp.status_code in _RETRYABLE:
                    if attempt == max_retries:
                        raise SearchError(f"upload failed: HTTP {resp.status_code}")
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                    time.sleep(min(delay, 30.0))
                    backoff = min(backoff * 2, 30.0)
                    continue
                if resp.status_code >= 400:
                    raise SearchError(f"upload failed: HTTP {resp.status_code} {resp.text[:400]}")
                results.extend(resp.json().get("value", []))
                break
        return results

    def delete_documents(self, ids: Iterable[str]) -> List[dict]:
        ids = list(ids)
        if not ids:
            return []
        url = f"{self.base}/indexes/{self.index}/docs/index?api-version={self.api}"
        payload = {"value": [{"@search.action": "delete", "id": i} for i in ids]}
        resp = self.session.post(url, json=payload, timeout=60)
        if resp.status_code >= 400:
            raise SearchError(f"delete failed: HTTP {resp.status_code} {resp.text[:400]}")
        return resp.json().get("value", [])

    def list_ids(self, top: int = 1000) -> List[str]:
        """Page the id field, for reconcile runs."""

        ids: List[str] = []
        skip = 0
        while True:
            params = {
                "search": "*",
                "select": "id",
                "$top": min(top, 1000),
                "$skip": skip,
                "api-version": self.api,
            }
            resp = self.session.get(f"{self.base}/indexes/{self.index}/docs", params=params, timeout=30)
            if resp.status_code >= 400:
                raise SearchError(f"list_ids failed: HTTP {resp.status_code} {resp.text[:200]}")
            values = resp.json().get("value", [])
            ids.extend(v["id"] for v in values)
            if len(values) < min(top, 1000):
                return ids
            skip += len(values)

    def fetch_all(
        self,
        select: str = (
            "ticket_id,symptom_text,resolution_text,category,sub_category,store,age_days"
        ),
        max_docs: int = 100000,
    ) -> List[Dict[str, Any]]:
        """Page every document out of the index (for gold-set building).

        The index - not a stale local JSONL - is the source of truth once a
        backfill has run, so gold sampling reads from here by default.
        """

        docs: List[Dict[str, Any]] = []
        skip = 0
        page = 1000
        while len(docs) < max_docs:
            params = {
                "search": "*",
                "select": select,
                "$top": page,
                "$skip": skip,
                "$count": "false",
                "api-version": self.api,
            }
            resp = self.session.get(
                f"{self.base}/indexes/{self.index}/docs", params=params, timeout=60
            )
            if resp.status_code >= 400:
                raise SearchError(f"fetch_all failed: HTTP {resp.status_code} {resp.text[:200]}")
            values = resp.json().get("value", [])
            docs.extend(values)
            if len(values) < page:
                break
            skip += len(values)
        return docs[:max_docs]

    # --- querying --------------------------------------------------------
    def hybrid_search(
        self,
        query_text: str,
        query_vector: Optional[List[float]] = None,
        filter_expr: Optional[str] = None,
        top: int = 5,
        use_semantic: bool = True,
        exclude_ticket_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """BM25 + vector + (optional) semantic rerank, RRF-fused.

        ``exclude_ticket_id`` drops one ticket from the results - used by the
        leave-one-out evaluation so a held-out ticket cannot match itself.
        """

        payload: Dict[str, Any] = {
            "search": query_text or "*",
            "top": top,
            "select": (
                "id,ticket_id,display_id,subject,resolution_text,resolution_source,"
                "category,sub_category,store,apps,error_codes,created_at,age_days,"
                "linked_itglue_urls"
            ),
        }
        if exclude_ticket_id is not None:
            exclusion = f"ticket_id ne {int(exclude_ticket_id)}"
            filter_expr = f"({filter_expr}) and {exclusion}" if filter_expr else exclusion
        if query_vector:
            payload["vectorQueries"] = [
                {
                    "kind": "vector",
                    "vector": query_vector,
                    "k": max(top * 4, 20),
                    "fields": "symptom_vector",
                }
            ]
        if filter_expr:
            payload["filter"] = filter_expr
        if use_semantic:
            payload["queryType"] = "semantic"
            payload["semanticConfiguration"] = "default-semantic"

        resp = self.session.post(
            f"{self.base}/indexes/{self.index}/docs/search.post.search?api-version={self.api}",
            json=payload,
            timeout=30,
        )
        if resp.status_code >= 400:
            raise SearchError(f"search failed: HTTP {resp.status_code} {resp.text[:300]}")
        return resp.json().get("value", [])


def build_filter(
    category: Optional[str] = None,
    sub_category: Optional[str] = None,
    store: Optional[str] = None,
    app: Optional[str] = None,
    only_with_resolution: bool = True,
) -> Optional[str]:
    """Compose an OData filter from optional metadata constraints."""

    clauses: List[str] = []
    if only_with_resolution:
        clauses.append("has_resolution eq true")
    if category:
        clauses.append(f"category eq '{_escape(category)}'")
    if sub_category:
        clauses.append(f"sub_category eq '{_escape(sub_category)}'")
    if store:
        clauses.append(f"store/any(s: s eq '{_escape(store)}')")
    if app:
        clauses.append(f"apps/any(a: a eq '{_escape(app)}')")
    return " and ".join(clauses) if clauses else None


def _escape(value: str) -> str:
    return value.replace("'", "''")
