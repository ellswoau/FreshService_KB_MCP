"""Retrieval-only MCP server for the FreshService knowledge base.

This is what Axle uses on a helpdesk ticket: "have we fixed this before?".
It embeds the query (via the configured embedding provider) and runs the same
hybrid BM25 + vector + semantic search the rest of the package uses, then
returns the prior fixes.

Run:
    python -m fskb.mcp_server                          # stdio (embedded client)
    python -m fskb.mcp_server --transport http --port 8100

Network transports are gated behind a bearer token (``KB_MCP_AUTH_TOKEN``);
``/health`` stays credential-free for monitors.
"""

from __future__ import annotations

import argparse
import hmac
import os
import sys
from typing import Any, Dict, List, Optional

try:
    from fastmcp import FastMCP
except Exception:  # pragma: no cover - import-safe without the extra
    FastMCP = None  # type: ignore

from .config import Settings
from .search_client import SearchClient, build_filter


def _settings() -> Settings:
    return Settings.from_env()


def _embed_query(settings: Settings, text: str) -> Optional[List[float]]:
    """Embed the query when an embedding provider is configured; else None so
    the search degrades to BM25-only rather than failing."""

    try:
        settings.require_embedding()
    except RuntimeError:
        return None
    from .embed import EmbeddingClient

    try:
        return EmbeddingClient(settings).embed_one(text)
    except Exception:
        return None


def _recency_boost(age_days: Optional[int]) -> float:
    """Gentle decay: a fix from last week outranks one from last year.

    Exposed so the caller sees why ordering differs from raw score.
    """

    if age_days is None:
        return 1.0
    if age_days <= 30:
        return 1.15
    if age_days <= 180:
        return 1.05
    if age_days <= 365:
        return 1.0
    return 0.9


def search_kb_impl(
    settings: Settings,
    query: str,
    top: int = 3,
    category: Optional[str] = None,
    sub_category: Optional[str] = None,
    store: Optional[str] = None,
    app: Optional[str] = None,
    boost_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Core retrieval, importable and testable without MCP."""

    vector = _embed_query(settings, query)
    flt = build_filter(category=category, sub_category=sub_category, store=store, app=app)
    hits = SearchClient(settings).hybrid_search(
        query, query_vector=vector, filter_expr=flt, top=max(top * 3, 10)
    )

    boosts: Dict[int, float] = {}
    if boost_path and os.path.exists(boost_path):
        try:
            import json

            with open(boost_path, "r", encoding="utf-8") as fh:
                boosts = {int(k): float(v) for k, v in json.load(fh).items()}
        except Exception:
            boosts = {}

    scored = []
    for h in hits:
        base = float(h.get("@search.rerankerScore") or h.get("@search.score") or 0.0)
        factor = _recency_boost(h.get("age_days")) * boosts.get(int(h.get("ticket_id") or 0), 1.0)
        scored.append((base * factor, h))
    scored.sort(key=lambda t: t[0], reverse=True)

    results = []
    for score, h in scored[:top]:
        results.append(
            {
                "ticket_id": h.get("ticket_id"),
                "display_id": h.get("display_id"),
                "subject": h.get("subject"),
                "symptom": (h.get("symptom_text") or "")[:400],
                "resolution": (h.get("resolution_text") or "")[:1200],
                "resolution_source": h.get("resolution_source"),
                "category": h.get("category"),
                "sub_category": h.get("sub_category"),
                "store": h.get("store"),
                "apps": h.get("apps"),
                "error_codes": h.get("error_codes"),
                "age_days": h.get("age_days"),
                "score": round(score, 4),
                "itglue_urls": h.get("linked_itglue_urls") or [],
            }
        )
    return {
        "query": query,
        "mode": "hybrid+semantic" if vector else "bm25-only",
        "count": len(results),
        "results": results,
        "note": "Prior fixes only; verify against the live system before applying.",
    }


def build_server() -> "FastMCP":
    if FastMCP is None:  # pragma: no cover
        raise RuntimeError("fastmcp is not installed. Install with: pip install 'freshservice-kb[mcp]'")

    mcp = FastMCP("freshservice-kb")

    @mcp.tool()
    def search_kb(
        query: str,
        top: int = 3,
        category: Optional[str] = None,
        sub_category: Optional[str] = None,
        store: Optional[str] = None,
        app: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Find previously resolved helpdesk tickets whose fix matches a new
        ticket's symptom.

        Pass the requester's own words as ``query`` (the symptom, not the fix).
        Optionally narrow with category / sub_category / store / app. Returns
        the top prior fixes with their resolutions, ages and any IT Glue links.
        """
        settings = _settings()
        return search_kb_impl(
            settings,
            query,
            top=top,
            category=category,
            sub_category=sub_category,
            store=store,
            app=app,
            boost_path=os.environ.get("KB_BOOST_PATH"),
        )

    @mcp.tool()
    def kb_stats() -> Dict[str, Any]:
        """Report index size and configuration (no secrets)."""

        settings = _settings()
        from .search_index import IndexManager

        try:
            count = IndexManager(settings).count()
        except Exception as exc:
            count = f"unavailable: {exc}"
        return {
            "index": settings.search_index_name,
            "documents": count,
            "embedding_provider": settings.embed_provider,
            "embedding_model": settings.embed_model
            if settings.embed_provider == "openai"
            else settings.aoai_embed_deployment,
        }

    return mcp


class _BearerAuthMiddleware:
    """Require ``Authorization: Bearer <token>`` on non-public HTTP paths."""

    PUBLIC = ("/health", "/healthz")

    def __init__(self, app, allowed_key: str = "", public_paths=PUBLIC):
        self.app = app
        self.allowed_key = allowed_key
        self.public_paths = public_paths

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            return await self.app(scope, receive, send)
        path = scope.get("path", "")
        if any(path == p or path.startswith(p + "/") for p in self.public_paths):
            return await self.app(scope, receive, send)
        auth = ""
        for name, value in scope.get("headers", []):
            if name == b"authorization":
                auth = value.decode("latin-1")
                break
        if not hmac.compare_digest(auth, "Bearer " + self.allowed_key):
            body = b'{"error":"unauthorized"}'
            await send({"type": "http.response.start", "status": 401,
                        "headers": [(b"content-type", b"application/json"),
                                    (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
            return
        return await self.app(scope, receive, send)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="fskb-mcp", description="FreshService KB retrieval MCP server")
    parser.add_argument("--transport", default="stdio", choices=["stdio", "http", "sse", "streamable-http"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8100)
    args = parser.parse_args(argv)

    mcp = build_server()
    if args.transport == "stdio":
        mcp.run()
        return 0

    token = os.environ.get("KB_MCP_AUTH_TOKEN", "")
    middlewares = []
    if token:
        middlewares.append((_BearerAuthMiddleware, {"allowed_key": token}, {}))
    else:  # pragma: no cover
        print("WARNING: KB_MCP_AUTH_TOKEN not set - network transport is unauthenticated", file=sys.stderr)
    mcp.run(transport=args.transport, host=args.host, port=args.port, middlewares=middlewares)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
