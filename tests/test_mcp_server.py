"""Offline tests for the retrieval MCP server core and scheduler safety."""

import json

import fskb.mcp_server as m
from fskb.config import Settings


# --- recency boosting -----------------------------------------------------
def test_recency_boost_monotonic():
    assert m._recency_boost(1) > m._recency_boost(90)
    assert m._recency_boost(90) > m._recency_boost(400)
    assert m._recency_boost(None) == 1.0


def test_recency_boost_bounds():
    for days in (0, 30, 180, 365, 5000):
        assert 0.8 <= m._recency_boost(days) <= 1.2


# --- search_kb_impl ordering ----------------------------------------------
class _FakeSearch:
    """Returns canned hits regardless of query."""

    def __init__(self, hits):
        self._hits = hits

    def hybrid_search(self, *a, **k):
        return list(self._hits)


def _settings_ok():
    # No embedding provider required for the bm25-only path in these tests.
    return Settings(embed_provider="openai", embed_base_url="http://x", embed_api_key="k", embed_model="m")


def test_search_kb_impl_orders_by_recency_weighted_score(monkeypatch):
    hits = [
        {"ticket_id": 1, "subject": "old", "resolution_text": "r1", "age_days": 900,
         "@search.score": 1.0, "symptom_text": "s"},
        {"ticket_id": 2, "subject": "new", "resolution_text": "r2", "age_days": 5,
         "@search.score": 1.0, "symptom_text": "s"},
    ]
    monkeypatch.setattr(m, "SearchClient", lambda s: _FakeSearch(hits))
    monkeypatch.setattr(m, "_embed_query", lambda s, q: None)

    out = m.search_kb_impl(_settings_ok(), "anything", top=2)
    assert out["count"] == 2
    # The recent ticket must outrank the stale one at equal base score.
    assert out["results"][0]["ticket_id"] == 2


def test_search_kb_impl_applies_boost_table(monkeypatch, tmp_path):
    hits = [
        {"ticket_id": 1, "subject": "a", "resolution_text": "r", "age_days": 400,
         "@search.score": 1.0, "symptom_text": "s"},
        {"ticket_id": 2, "subject": "b", "resolution_text": "r", "age_days": 400,
         "@search.score": 1.0, "symptom_text": "s"},
    ]
    boost = tmp_path / "boost.json"
    boost.write_text(json.dumps({"1": 1.5, "2": 1.0}))
    monkeypatch.setattr(m, "SearchClient", lambda s: _FakeSearch(hits))

    out = m.search_kb_impl(_settings_ok(), "q", top=2, boost_path=str(boost))
    assert out["results"][0]["ticket_id"] == 1


def test_search_kb_impl_reports_mode(monkeypatch):
    monkeypatch.setattr(m, "SearchClient", lambda s: _FakeSearch([]))
    monkeypatch.setattr(m, "_embed_query", lambda s, q: None)
    assert m.search_kb_impl(_settings_ok(), "q")["mode"] == "bm25-only"

    monkeypatch.setattr(m, "_embed_query", lambda s, q: [0.1, 0.2])
    assert m.search_kb_impl(_settings_ok(), "q")["mode"] == "hybrid+semantic"


def test_search_kb_impl_shape(monkeypatch):
    hits = [{"ticket_id": 9, "subject": "s", "resolution_text": "r", "age_days": 1,
             "@search.score": 1.0, "linked_itglue_urls": ["https://x"], "symptom_text": "sym"}]
    monkeypatch.setattr(m, "SearchClient", lambda s: _FakeSearch(hits))
    monkeypatch.setattr(m, "_embed_query", lambda s, q: None)
    r = m.search_kb_impl(_settings_ok(), "q")["results"][0]
    for key in ("ticket_id", "subject", "resolution", "score", "itglue_urls"):
        assert key in r


# --- embed_query graceful degradation -------------------------------------
def test_embed_query_returns_none_without_provider(monkeypatch):
    # No embedding config at all -> None (BM25 fallback), never raise.
    assert m._embed_query(Settings(), "q") is None


# --- auth middleware ------------------------------------------------------
def test_auth_middleware_allows_public_path(monkeypatch):
    import asyncio

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    mw = m._BearerAuthMiddleware(app, allowed_key="secret")
    sent = []

    async def send(msg):
        sent.append(msg)

    asyncio.get_event_loop().run_until_complete(
        mw({"type": "http", "path": "/health", "headers": []}, None, send)
    )
    assert sent[0]["status"] == 200


def test_auth_middleware_rejects_missing_token():
    import asyncio

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})

    mw = m._BearerAuthMiddleware(app, allowed_key="secret")
    sent = []

    async def send(msg):
        sent.append(msg)

    asyncio.get_event_loop().run_until_complete(
        mw({"type": "http", "path": "/mcp", "headers": []}, None, send)
    )
    assert sent[0]["status"] == 401


def test_auth_middleware_accepts_valid_token():
    import asyncio

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})

    mw = m._BearerAuthMiddleware(app, allowed_key="secret")
    sent = []

    async def send(msg):
        sent.append(msg)

    scope = {"type": "http", "path": "/mcp", "headers": [(b"authorization", b"Bearer secret")]}
    asyncio.get_event_loop().run_until_complete(mw(scope, None, send))
    assert sent[0]["status"] == 200
