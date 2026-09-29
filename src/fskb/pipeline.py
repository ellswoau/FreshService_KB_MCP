"""Pipeline orchestration and step functions.

Every step has an offline mode (``--offline`` / ``dry_run``) so the whole flow
can be exercised and tested without credentials or network access.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

from .config import Settings
from .freshservice import FreshServiceClient
from .models import KbDocument
from .search_client import SearchClient
from .search_index import IndexManager
from .state import State
from .transform import to_document

Progress = Callable[[str], None]


def _noop(msg: str) -> None:  # pragma: no cover
    print(msg)


def resolve_watermark(
    updated_since: Optional[str], stored: Optional[str], no_watermark: bool
) -> Optional[str]:
    """Choose the watermark a run pulls with.

    A backfill (``no_watermark=True``) IGNORES the stored watermark entirely, so
    it pulls the whole corpus regardless of how far a prior incremental run
    advanced the state. Otherwise an explicit ``updated_since`` wins, then the
    stored watermark.
    """

    if no_watermark:
        return updated_since  # usually None -> pull everything
    return updated_since if updated_since is not None else stored


def should_advance_watermark(
    limit: Optional[int], no_watermark: bool, updated_since: Optional[str]
) -> bool:
    """Whether a run may move the incremental watermark.

    Only a full, uncapped, watermark-driven run may advance it. A capped run
    (``limit``) or a backfill (``no_watermark``) must leave it alone, or a
    sampled/backfill run silently poisons the scheduled incremental state.
    """

    return (limit is None) and (not no_watermark) and (updated_since is None)


@dataclass
class PipelineResult:
    tickets_seen: int = 0
    records_built: int = 0
    records_dropped: int = 0
    upload_ok: int = 0
    upload_failed: int = 0
    documents: List[KbDocument] = field(default_factory=list)
    newest_updated_at: Optional[str] = None


def extract_tickets(
    settings: Settings,
    updated_since: Optional[str],
    order_by: Optional[str] = None,
    order_type: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[dict]:
    client = FreshServiceClient(settings)
    return list(
        client.list_tickets(
            updated_since=updated_since,
            order_by=order_by,
            order_type=order_type,
            limit=limit,
        )
    )


def build_records(
    tickets: Iterable[dict],
    conversations_by_id: Optional[Dict[int, List[dict]]] = None,
    itglue_links: Optional[Dict[str, Dict[str, str]]] = None,
    min_symptom_chars: int = 15,
    hydrate: Optional[Callable[[int], List[dict]]] = None,
) -> PipelineResult:
    """Turn raw tickets into KbDocuments. Pure aside from optional ``hydrate``."""

    conversations_by_id = conversations_by_id or {}
    result = PipelineResult()
    for ticket in tickets:
        result.tickets_seen += 1
        tid = ticket.get("id")
        convs = conversations_by_id.get(tid)
        if convs is None and hydrate is not None and tid is not None:
            convs = hydrate(int(tid))
        doc = to_document(
            ticket,
            conversations=convs or [],
            itglue_links=itglue_links,
            min_symptom_chars=min_symptom_chars,
        )
        if doc is None:
            result.records_dropped += 1
            continue
        result.records_built += 1
        result.documents.append(doc)
        updated = doc.updated_at
        if updated and (result.newest_updated_at is None or updated > result.newest_updated_at):
            result.newest_updated_at = updated
    return result


def embed_documents(documents: List[KbDocument], embed_fn: Callable[[List[str]], List[List[float]]]) -> None:
    """Fill symptom_vector (and resolution_vector) in place."""

    if not documents:
        return
    symptoms = [d.symptom_text for d in documents]
    res = [d.resolution_text for d in documents]
    for doc, vec in zip(documents, embed_fn(symptoms)):
        doc.symptom_vector = vec
    for doc, vec in zip(documents, embed_fn(res)):
        doc.resolution_vector = vec


def run_full(
    settings: Settings,
    updated_since: Optional[str] = None,
    dry_run: bool = False,
    progress: Progress = _noop,
    limit: Optional[int] = None,
    order_by: Optional[str] = None,
    order_type: Optional[str] = None,
    no_watermark: bool = False,
) -> PipelineResult:
    """Extract -> build -> embed -> push.

    Incremental by default (honours the stored watermark). A *limited* run never
    advances the watermark - otherwise a sampled run would silently poison the
    incremental state. Pass ``no_watermark=True`` for a backfill so the run
    leaves ``last_updated_at`` alone entirely.
    """

    state = State.load(settings.state_dir)
    # A backfill must not READ the stored watermark either, or it stays incremental.
    watermark = resolve_watermark(updated_since, state.last_updated_at, no_watermark)
    # A capped or explicitly-watermark-free run must not MOVE the watermark.
    advance_watermark = should_advance_watermark(limit, no_watermark, updated_since)

    progress(
        f"[extract] FreshService tickets updated_since={watermark!r} "
        f"order_by={order_by!r} order_type={order_type!r} limit={limit!r} "
        f"advance_watermark={advance_watermark}"
    )
    tickets = extract_tickets(
        settings, watermark, order_by=order_by, order_type=order_type, limit=limit
    )
    progress(f"[extract] {len(tickets)} ticket(s)")

    fs_client = FreshServiceClient(settings)

    def hydrate(ticket_id: int) -> List[dict]:
        return fs_client.get_conversations(ticket_id)

    if dry_run:
        hydrate = None  # offline: rely on whatever conversations are inline

    progress("[transform] building records")
    result = build_records(
        tickets,
        conversations_by_id={},
        min_symptom_chars=settings.min_symptom_chars,
        hydrate=hydrate,
    )
    progress(f"[transform] built={result.records_built} dropped={result.records_dropped}")

    if dry_run:
        progress("[dry-run] stopping before embed/push")
        return result

    from .embed import EmbeddingClient

    embedder = EmbeddingClient(settings)
    progress("[embed] embedding symptom + resolution vectors")
    embed_documents(result.documents, embedder.embed_all)

    search = SearchClient(settings)
    progress("[push] uploading documents")
    upload_results = search.upload_documents(d.to_search_doc() for d in result.documents)
    for item in upload_results:
        if item.get("status", True) and item.get("error") is None:
            result.upload_ok += 1
        else:
            result.upload_failed += 1
    progress(f"[push] ok={result.upload_ok} failed={result.upload_failed}")

    # Advance the watermark only after a clean, uncapped push.
    if advance_watermark and result.newest_updated_at:
        state.last_updated_at = result.newest_updated_at
        progress(f"[state] watermark advanced -> {result.newest_updated_at}")
    elif not advance_watermark:
        progress("[state] watermark NOT advanced (capped or watermark-free run)")
    state.last_run_at = datetime.now(timezone.utc).isoformat()
    state.last_indexed_count = result.records_built
    state.save()
    return result


def reconcile(settings: Settings, progress: Progress = _noop) -> int:
    """Soft-delete index docs whose ticket is no longer resolved/closed.

    Full reconcile compares the index id set to the live terminal-ticket set.
    """

    search = SearchClient(settings)
    fs_client = FreshServiceClient(settings)
    indexed = set(search.list_ids())
    live = {
        str(t.get("id"))
        for t in fs_client.list_tickets()
        if str(t.get("status")) in {"4", "5"}
    }
    stale = sorted(indexed - live)
    progress(f"[reconcile] indexed={len(indexed)} live={len(live)} stale={len(stale)}")
    if stale:
        search.delete_documents(stale)
    return len(stale)


def write_jsonl(documents: List[KbDocument], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for doc in documents:
            fh.write(json.dumps(doc.model_dump(), ensure_ascii=False) + "\n")
