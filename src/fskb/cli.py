"""Command-line interface.

Commands:
  init-index   create (or recreate) the Azure AI Search index
  extract      pull tickets and write sanitized records to JSONL (no embed/push)
  index        full run: extract -> embed -> push (incremental by watermark)
  query        hybrid lookup against the KB
  reconcile    soft-delete index docs for tickets no longer resolved/closed
  monitor      correlated-ticket open-rate monitor (live cluster counter)
  status       print redacted config + index doc count
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import Settings


def _settings() -> Settings:
    return Settings.from_env()


def cmd_init_index(args: argparse.Namespace) -> int:
    from .search_index import IndexManager

    settings = _settings()
    mgr = IndexManager(settings)
    info = mgr.create(dimensions=settings.embed_dimensions, recreate=args.recreate)
    print(f"index '{settings.search_index_name}' ready (fields={len(info.get('fields', []))})")
    return 0


def cmd_extract(args: argparse.Namespace) -> int:
    from .pipeline import build_records, extract_tickets, write_jsonl

    settings = _settings()
    tickets = extract_tickets(
        settings,
        args.updated_since,
        order_by=args.order_by,
        order_type=args.order_type,
        limit=args.limit,
    )
    result = build_records(tickets, min_symptom_chars=settings.min_symptom_chars)
    out = Path(args.out)
    write_jsonl(result.documents, out)
    print(
        f"tickets={result.tickets_seen} built={result.records_built} "
        f"dropped={result.records_dropped} -> {out}"
    )
    return 0


def cmd_index(args: argparse.Namespace) -> int:
    from .pipeline import run_full

    settings = _settings()
    result = run_full(
        settings,
        updated_since=args.updated_since,
        dry_run=args.dry_run,
        progress=lambda m: print(m, file=sys.stderr),
        limit=args.limit,
        order_by=args.order_by,
        order_type=args.order_type,
    )
    print(
        f"built={result.records_built} dropped={result.records_dropped} "
        f"uploaded={result.upload_ok} failed={result.upload_failed}"
    )
    return 0 if result.upload_failed == 0 else 1


def cmd_backfill(args: argparse.Namespace) -> int:
    """Index the most recent N tickets regardless of the watermark.

    This is the answer to "index the last 5000 tickets now": fresh date-desc
    pull, no watermark read, and (critically) no watermark write - so it cannot
    poison the incremental state for the scheduled runs.
    """

    from .pipeline import run_full

    settings = _settings()
    limit = args.limit if args.limit and args.limit > 0 else None
    result = run_full(
        settings,
        updated_since=None,          # ignore the stored watermark
        dry_run=args.dry_run,
        progress=lambda m: print(m, file=sys.stderr),
        limit=limit,
        order_by="created_at",
        order_type="desc",
        no_watermark=True,           # do not move last_updated_at
    )
    print(
        f"backfill: built={result.records_built} dropped={result.records_dropped} "
        f"uploaded={result.upload_ok} failed={result.upload_failed} (watermark unchanged)"
    )
    return 0 if result.upload_failed == 0 else 1


def cmd_query(args: argparse.Namespace) -> int:
    from .embed import EmbeddingClient
    from .search_client import SearchClient, build_filter

    settings = _settings()
    vector = None
    if not args.no_vector:
        vector = EmbeddingClient(settings).embed_one(args.text)
    flt = build_filter(category=args.category, sub_category=args.sub_category, store=args.store, app=args.app)
    hits = SearchClient(settings).hybrid_search(args.text, query_vector=vector, filter_expr=flt, top=args.top)
    for i, hit in enumerate(hits, 1):
        print(f"{i}. [{hit.get('ticket_id')}] {hit.get('subject')}  (score={hit.get('@search.score')})")
        rt = (hit.get("resolution_text") or "").strip().replace("\n", " ")
        print(f"   {rt[:200]}")
        for url in hit.get("linked_itglue_urls") or []:
            print(f"   doc: {url}")
    print(f"-- {len(hits)} hit(s)")
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    from .pipeline import reconcile

    settings = _settings()
    n = reconcile(settings, progress=lambda m: print(m, file=sys.stderr))
    print(f"soft-deleted {n} stale document(s)")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    settings = _settings()
    print(json.dumps(settings.redacted(), indent=2))
    try:
        from .search_index import IndexManager

        if IndexManager(settings).exists():
            print(f"index doc count: {IndexManager(settings).count()}")
        else:
            print("index does not exist yet")
    except Exception as exc:  # keep status usable without full config
        print(f"index check skipped: {exc}")
    return 0


def cmd_gold(args: argparse.Namespace) -> int:
    """Build a gold set. Defaults to the INDEX (source of truth after a backfill),
    which is why a stale out/records.jsonl no longer caps the sample at 13."""

    from .evaluate import build_gold, save_gold

    settings = _settings()
    if args.from_jsonl:
        source = Path(args.from_jsonl)
        if not source.exists():
            print(f"source not found: {source}")
            return 2
        documents = [
            json.loads(line)
            for line in source.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        origin = f"jsonl:{source}"
    else:
        from .search_client import SearchClient

        documents = SearchClient(settings).fetch_all()
        origin = f"index:{settings.search_index_name}"

    if not documents:
        print(f"no documents found in {origin} - run 'fskb backfill' first")
        return 2

    items = build_gold(documents, sample_size=args.size, seed=args.seed)
    out = Path(args.out)
    save_gold(items, out)
    singles = sum(1 for i in items if len(i.expected_ticket_ids) == 1)
    print(
        f"gold set: {len(items)} item(s) from {origin} ({len(documents)} candidate docs) -> {out}\n"
        f"  {singles} single-fix, {len(items) - singles} with cluster mates; labeled=false"
    )
    print("Review the file and set \"labeled\": true on items you have confirmed.")
    return 0


def cmd_eval(args: argparse.Namespace) -> int:
    from .embed import EmbeddingClient
    from .evaluate import evaluate, format_report, load_gold
    from .search_client import SearchClient

    settings = _settings()
    items = load_gold(Path(args.gold))
    if not items:
        print("gold set is empty")
        return 2
    embedder = EmbeddingClient(settings)
    search = SearchClient(settings)

    def search_fn(query_text: str, exclude_ticket_id=None, top: int = 3):
        vector = None if args.no_vector else embedder.embed_one(query_text)
        return search.hybrid_search(
            query_text, query_vector=vector, top=top, exclude_ticket_id=exclude_ticket_id
        )

    report = evaluate(items, search_fn, k=args.k, exclude_self=not args.integrity)
    print(format_report(report))
    if args.json:
        Path(args.json).write_text(json.dumps(report.as_dict(), indent=2), encoding="utf-8")
        print(f"wrote {args.json}")
    # Integrity mode is a smoke test, not a quality gate; only fail on LOO.
    if not args.integrity and report.hit_at_k < args.min_hit:
        print(f"FAIL: hit@{args.k} {report.hit_at_k:.1%} < required {args.min_hit:.1%}")
        return 1
    return 0


def cmd_feedback(args: argparse.Namespace) -> int:
    from .feedback import FeedbackEvent, log_event, summarize, write_boost_table

    settings = _settings()
    log_path = Path(args.log)
    if args.suggested_ticket is None:
        print("error: --suggested-ticket is required", file=sys.stderr)
        return 2
    resolved = None
    if args.resolved is not None:
        resolved = args.resolved.lower() in {"1", "true", "yes", "resolved"}
    event = FeedbackEvent(
        ticket_id=args.ticket,
        suggested_ticket_id=args.suggested_ticket,
        rank=args.rank,
        accepted=args.accepted,
        resolved=resolved,
        agent=args.agent,
        note=args.note,
    )
    log_event(log_path, event)
    print(f"logged feedback: ticket={args.ticket} suggestion={args.suggested_ticket} accepted={args.accepted}")
    if args.boost_out:
        write_boost_table(log_path, Path(args.boost_out))
        print(f"wrote boost table -> {args.boost_out}")
    else:
        stats = summarize(log_path)
        hits = {str(k): v.accept_rate for k, v in sorted(stats.items())[:10]}
        print(json.dumps({"suggestions_tracked": len(stats), "accept_rate_top": hits}, indent=2))
    return 0


def _cmd_monitor(args: argparse.Namespace) -> int:
    # Imported lazily so the package stays import-safe without the monitor deps.
    from .monitor import cmd_monitor

    return cmd_monitor(args)


def _cmd_baseline(args: argparse.Namespace) -> int:
    from .monitor import cmd_baseline

    return cmd_baseline(args)


def _cmd_seq_baseline(args: argparse.Namespace) -> int:
    from .monitor import cmd_seq_baseline

    return cmd_seq_baseline(args)


def _cmd_monitor_feedback(args: argparse.Namespace) -> int:
    from .monitor import cmd_monitor_feedback

    return cmd_monitor_feedback(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fskb", description="FreshService -> Azure AI Search KB")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init-index", help="create the search index")
    p.add_argument("--recreate", action="store_true", help="delete then recreate")
    p.set_defaults(func=cmd_init_index)

    p = sub.add_parser("extract", help="extract sanitized records to JSONL")
    p.add_argument("--out", default="out/records.jsonl")
    p.add_argument("--updated-since", default=None)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--order-by", default=None, help="e.g. created_at")
    p.add_argument("--order-type", default=None, help="asc or desc")
    p.set_defaults(func=cmd_extract)

    p = sub.add_parser("index", help="incremental extract -> embed -> push (honours watermark)")
    p.add_argument("--updated-since", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=None, help="cap this run; does NOT advance the watermark")
    p.add_argument("--order-by", default=None)
    p.add_argument("--order-type", default=None)
    p.set_defaults(func=cmd_index)

    p = sub.add_parser("backfill", help="index the most recent N tickets (ignores + does not move watermark)")
    p.add_argument("--limit", type=int, default=5000, help="number of most-recent tickets (0 = no cap, 9000 max per pull)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_backfill)

    p = sub.add_parser("query", help="hybrid search")
    p.add_argument("text")
    p.add_argument("--top", type=int, default=5)
    p.add_argument("--category", default=None)
    p.add_argument("--sub-category", default=None)
    p.add_argument("--store", default=None)
    p.add_argument("--app", default=None)
    p.add_argument("--no-vector", action="store_true")
    p.set_defaults(func=cmd_query)

    p = sub.add_parser("reconcile", help="soft-delete stale docs")
    p.set_defaults(func=cmd_reconcile)

    p = sub.add_parser("gold", help="build a gold eval set (from the index by default)")
    p.add_argument("--from-jsonl", default=None, help="read a local JSONL instead of the index")
    p.add_argument("--out", default="gold/goldset.jsonl")
    p.add_argument("--size", type=int, default=50)
    p.add_argument("--seed", type=int, default=42)
    p.set_defaults(func=cmd_gold)

    p = sub.add_parser("eval", help="score retrieval against the gold set")
    p.add_argument("--gold", default="gold/goldset.jsonl")
    p.add_argument("--k", type=int, default=3)
    p.add_argument("--integrity", action="store_true", help="self-match smoke test (not a quality gate)")
    p.add_argument("--no-vector", action="store_true")
    p.add_argument("--min-hit", type=float, default=0.8, help="fail below this hit@k in LOO mode")
    p.add_argument("--json", default=None, help="write the report as JSON")
    p.set_defaults(func=cmd_eval)

    p = sub.add_parser("feedback", help="log an accept/reject for a suggestion")
    p.add_argument("--ticket", type=int, required=True)
    p.add_argument("--suggested-ticket", type=int, default=None)
    p.add_argument("--rank", type=int, default=None)
    p.add_argument("--accepted", action="store_true")
    p.add_argument("--resolved", default=None, help="true/false/unknown")
    p.add_argument("--agent", default=None)
    p.add_argument("--note", default=None)
    p.add_argument("--log", default="feedback/events.jsonl")
    p.add_argument("--boost-out", default=None, help="write the boost table here")
    p.set_defaults(func=cmd_feedback)

    p = sub.add_parser("monitor", help="correlated-ticket monitor: cluster newly-created tickets")
    p.add_argument("--once", action="store_true", help="single poll, then exit")
    p.add_argument("--loop", action="store_true", help="run the polling loop (default)")
    p.add_argument("--dry-run", action="store_true", help="compose alerts/notes but do not post")
    p.add_argument("--replay-jsonl", dest="replay_jsonl", default=None,
                   help="feed synthetic tickets from a JSONL file (offline verification)")
    p.add_argument("--now", default=None, help="ISO timestamp to treat as 'now' for replay")
    p.add_argument("--db", default=None, help="SQLite path (default MONITOR_DB_PATH)")
    p.add_argument("--window", type=int, default=None, help="cluster window minutes")
    p.add_argument("--min-count", dest="min_count", type=int, default=None,
                   help="tickets required to fire a cluster")
    p.add_argument("--cooldown", type=int, default=None, help="cooldown minutes per cluster key")
    p.add_argument("--interval", type=int, default=None, help="loop interval seconds")
    p.add_argument("--health-port", dest="health_port", type=int, default=None,
                   help="HTTP /health port (0 disables)")
    p.set_defaults(func=_cmd_monitor)

    p = sub.add_parser("baseline", help="compute the open-rate baseline (system key x dow x hour)")
    p.add_argument("--dry-run", action="store_true", help="compute but do not write")
    p.add_argument("--db", default=None, help="SQLite path (default MONITOR_DB_PATH)")
    p.set_defaults(func=_cmd_baseline)

    p = sub.add_parser("seq-baseline", help="backfill the Engage error-rate (Seq) baseline")
    p.add_argument("--db", default=None, help="SQLite path (default MONITOR_DB_PATH)")
    p.set_defaults(func=_cmd_seq_baseline)

    p = sub.add_parser("monitor-feedback", help="record a verdict on a monitor alert")
    p.add_argument("--alert-id", dest="alert_id", type=int, required=True)
    p.add_argument("--verdict", required=True, choices=["accept", "reject", "unsure"])
    p.add_argument("--agent", default=None)
    p.add_argument("--note", default=None)
    p.add_argument("--cluster-key", dest="cluster_key", default=None)
    p.add_argument("--close", action="store_true", help="also close the alert (accept/reject)")
    p.add_argument("--db", default=None, help="SQLite path (default MONITOR_DB_PATH)")
    p.add_argument("--log", default=None, help="feedback JSONL (default MONITOR_FEEDBACK_LOG)")
    p.set_defaults(func=_cmd_monitor_feedback)

    p = sub.add_parser("status", help="redacted config + index count")
    p.set_defaults(func=cmd_status)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
