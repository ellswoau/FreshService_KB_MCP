"""Offline sanity tests. No credentials, no network."""

import json

from fskb.config import Settings
from fskb.embed import EmbeddingClient
from fskb.enrich import extract_apps, extract_error_codes, extract_hostnames
from fskb.models import KbDocument, is_terminal_status
from fskb.pipeline import build_records, resolve_watermark, should_advance_watermark
from fskb.sanitize import clean_text, is_secret_only, redact, sanitize
from fskb.search_client import build_filter
from fskb.search_index import index_schema
from fskb.state import State
from fskb.transform import build_resolution, build_symptom, to_document


# --- models ---------------------------------------------------------------
def test_is_terminal_status():
    assert is_terminal_status(4)
    assert is_terminal_status("5")
    assert not is_terminal_status(2)
    assert not is_terminal_status(None)


def test_to_search_doc_omits_empty_vectors():
    doc = KbDocument(id="1", ticket_id=1, symptom_text="a", resolution_text="b")
    out = doc.to_search_doc()
    assert "symptom_vector" not in out
    assert out["id"] == "1"


# --- enrich ---------------------------------------------------------------
def test_extract_error_codes():
    codes = extract_error_codes("Failed 0x80070005 and error 1907, HTTP 500")
    assert "0x80070005" in codes
    assert "1907" in codes
    assert "500" in codes


def test_extract_hostnames():
    hosts = extract_hostnames("on bos1-vdi-143 and UPS-Phoenix2 and app.weller.corp")
    joined = " ".join(hosts).lower()
    assert "bos1-vdi-143" in joined
    assert "ups-phoenix2" in joined


def test_extract_apps():
    apps = extract_apps("Outlook keeps asking for credentials, not Teams")
    assert "outlook" in apps
    assert "teams" in apps


# --- sanitize -------------------------------------------------------------
def test_redact_password_and_key():
    text = "password: Hunter2! and recovery key: FF2DD770-8061-46AA-B5EC-4E10CA1F81BA"
    out = redact(text)
    assert "Hunter2" not in out
    assert "FF2DD770" not in out
    assert "<redacted-secret>" in out
    assert "<redacted-key>" in out


def test_redact_email_phone():
    out = redact("email me at joe@example.com or 555-123-4567")
    assert "joe@example.com" not in out
    assert "555-123-4567" not in out


def test_clean_trims_quoted_chain_and_signature():
    body = "Outlook will not open.\nThanks,\nBob\n\nOn Mon, Sep 1, Jim wrote:\n>> old text"
    cleaned = clean_text(body)
    assert "Outlook will not open." in cleaned
    assert "old text" not in cleaned
    assert "Thanks," not in cleaned


def test_is_secret_only():
    assert is_secret_only("recovery key: FF2DD770-8061-46AA-B5EC-4E10CA1F81BA")
    assert not is_secret_only(
        "reset the account in AD and cleared the cached credential, key was FF2DD770-8061-46AA-B5EC-4E10CA1F81BA"
    )


def test_sanitize_roundtrip():
    out = sanitize("<p>Hello</p> password: secret123")
    assert "<p>" not in out
    assert "secret123" not in out


# --- transform ------------------------------------------------------------
def _ticket(**over):
    base = {
        "id": 47211,
        "status": 4,
        "subject": "Cannot log in",
        "description_text": "Account locked after password change.",
        "custom_fields": {"resolution": "Unlocked account in AD; stale cached credential."},
        "created_at": "2026-09-01T10:00:00Z",
        "closed_at": "2026-09-01T11:00:00Z",
        "updated_at": "2026-09-01T11:00:00Z",
    }
    base.update(over)
    return base


def test_to_document_resolved_ticket():
    doc = to_document(_ticket())
    assert doc is not None
    assert doc.ticket_id == 47211
    assert doc.resolution_source == "resolution_field"
    assert doc.age_days == 0
    assert doc.has_resolution


def test_to_document_drops_unresolved():
    assert to_document(_ticket(status=2)) is None


def test_to_document_drops_short_symptom():
    assert to_document(_ticket(description_text="help")) is None


def test_resolution_prefers_field_then_note_then_reply():
    ticket = _ticket(custom_fields={})
    convs = [
        {"private": False, "incoming": False, "body_text": "All fixed.", "created_at": "2026-09-01T11:00:00Z"},
        {"private": True, "body_text": "Root cause: stale token. Re-registered.", "created_at": "2026-09-01T10:30:00Z"},
    ]
    text, source = build_resolution(ticket, convs)
    assert source == "private_note"
    assert "stale token" in text

    text2, source2 = build_resolution(_ticket(custom_fields={}), [
        {"private": False, "incoming": False, "body_text": "All fixed.", "created_at": "2026-09-01T11:00:00Z"},
    ])
    assert source2 == "reply"


def test_symptom_uses_description_and_incoming_replies():
    convs = [
        {"private": False, "incoming": True, "body_text": "It happens on my VDI too.", "created_at": "2026-09-01T10:10:00Z"},
        {"private": False, "incoming": False, "body_text": "We are looking into it.", "created_at": "2026-09-01T10:20:00Z"},
    ]
    symptom = build_symptom(_ticket(), convs)
    assert "Account locked" in symptom
    assert "VDI" in symptom
    assert "looking into it" not in symptom


# --- pipeline -------------------------------------------------------------
def test_build_records_filters_and_extracts():
    tickets = [_ticket(), _ticket(id=99, status=2), _ticket(id=100, description_text="x")]
    result = build_records(tickets, conversations_by_id={}, min_symptom_chars=15)
    assert result.tickets_seen == 3
    assert result.records_built == 1
    assert result.records_dropped == 2
    assert result.documents[0].id == "47211"


def test_build_records_hydrates_conversations():
    calls = []

    def hydrate(tid):
        calls.append(tid)
        return [{"private": True, "body_text": "Fixed by re-registering the device.", "created_at": "2026-09-01T10:30:00Z"}]

    result = build_records(
        [_ticket(custom_fields={})], hydrate=hydrate, min_symptom_chars=15
    )
    assert calls == [47211]
    assert result.records_built == 1
    assert "re-registering" in result.documents[0].resolution_text


# --- watermark safety (the --limit trap) ----------------------------------
def test_uncapped_incremental_run_advances_watermark():
    assert should_advance_watermark(limit=None, no_watermark=False, updated_since=None) is True


def test_capped_run_does_not_advance_watermark():
    # Regression: a sampled run must not poison the incremental state.
    assert should_advance_watermark(limit=50, no_watermark=False, updated_since=None) is False
    assert should_advance_watermark(limit=5000, no_watermark=False, updated_since=None) is False


def test_backfill_never_advances_watermark():
    assert should_advance_watermark(limit=None, no_watermark=True, updated_since=None) is False
    assert should_advance_watermark(limit=5000, no_watermark=True, updated_since=None) is False


def test_explicit_updated_since_does_not_advance_watermark():
    # An explicit window is caller-controlled; leave stored state alone.
    assert should_advance_watermark(limit=None, no_watermark=False, updated_since="2026-01-01") is False


# --- watermark READ side (the backfill bug) -------------------------------
def test_incremental_reads_stored_watermark():
    assert resolve_watermark(None, "2026-09-29T13:24:33Z", no_watermark=False) == "2026-09-29T13:24:33Z"


def test_backfill_ignores_stored_watermark():
    # Regression: backfill must NOT read the stored watermark, or it stays
    # incremental and only pulls tickets since the last run.
    assert resolve_watermark(None, "2026-09-29T13:24:33Z", no_watermark=True) is None


def test_backfill_honours_explicit_window_but_still_not_stored():
    assert resolve_watermark("2026-08-01", "2026-09-29T13:24:33Z", no_watermark=True) == "2026-08-01"


def test_explicit_window_overrides_stored():
    assert resolve_watermark("2026-08-01", "2026-09-29T13:24:33Z", no_watermark=False) == "2026-08-01"


# --- search client --------------------------------------------------------
def test_build_filter():
    flt = build_filter(category="User Account", store="8-Atlanta", app="outlook")
    assert "has_resolution eq true" in flt
    assert "category eq 'User Account'" in flt
    assert "store/any(s: s eq '8-Atlanta')" in flt
    assert "apps/any(a: a eq 'outlook')" in flt


def test_build_filter_escaping():
    flt = build_filter(category="O'Brien")
    assert "O''Brien" in flt


# --- index schema ---------------------------------------------------------
def test_index_schema_shape():
    schema = index_schema("kb", 3072)
    fields = {f["name"]: f for f in schema["fields"]}
    assert fields["id"]["key"] is True
    assert fields["symptom_vector"]["dimensions"] == 3072
    assert fields["store"]["type"] == "Collection(Edm.String)"
    assert schema["semantic"]["configurations"][0]["name"] == "default-semantic"


# --- state ----------------------------------------------------------------
def test_state_roundtrip(tmp_path):
    st = State.load(tmp_path)
    st.last_updated_at = "2026-09-01T00:00:00Z"
    st.last_indexed_count = 5
    st.save()
    again = State.load(tmp_path)
    assert again.last_updated_at == "2026-09-01T00:00:00Z"
    assert again.last_indexed_count == 5


def test_state_survives_corrupt_file(tmp_path):
    (tmp_path / "state.json").write_text("{ not json")
    st = State.load(tmp_path)
    assert st.last_updated_at is None


# --- embeddings provider switch -------------------------------------------
def _azure_settings():
    return Settings(
        embed_provider="azure",
        aoai_endpoint="https://res.openai.azure.com",
        aoai_api_key="azkey",
        aoai_embed_deployment="text-embedding-3-large",
    )


def _openai_settings():
    return Settings(
        embed_provider="openai",
        embed_base_url="http://litellm.internal:4000",
        embed_api_key="sk-abc",
        embed_model="text-embedding-3-large",
    )


def test_azure_provider_url_and_header():
    client = EmbeddingClient(_azure_settings())
    assert client._url == (
        "https://res.openai.azure.com/openai/deployments/text-embedding-3-large"
        "/embeddings?api-version=2024-02-01"
    )
    assert client.session.headers["api-key"] == "azkey"
    assert "Authorization" not in client.session.headers
    # Azure carries the model in the URL, not the body.
    assert "model" not in client._payload(["x"])


def test_openai_provider_url_and_header():
    client = EmbeddingClient(_openai_settings())
    assert client._url == "http://litellm.internal:4000/v1/embeddings"
    assert client.session.headers["Authorization"] == "Bearer sk-abc"
    assert "api-key" not in client.session.headers
    # OpenAI-compatible shape sends the model in the body.
    assert client._payload(["x"])["model"] == "text-embedding-3-large"


def test_openai_provider_requires_fields():
    import pytest

    with pytest.raises(RuntimeError):
        EmbeddingClient(Settings(embed_provider="openai"))


def test_azure_provider_alias_still_works():
    # require_azure_openai is kept as a back-compat alias of require_embedding.
    assert Settings.require_azure_openai is Settings.require_embedding
