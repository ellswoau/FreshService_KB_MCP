"""Offline tests for Phase 3 corroborators. No network."""

import base64
from datetime import datetime, timedelta, timezone

from fskb import corroborators as C

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)


def test_catalog_is_internally_consistent():
    cat = C.load_catalog()
    stream_ids = {k: v["id"] for k, v in cat["streams"].items()}
    assert stream_ids["horizon"] == "693064393dbd12f7069db218"
    for sig in cat["signals"]:
        assert sig["stream"] in cat["streams"], sig["key"]
        assert sig["role"] in ("cause", "effect")
        assert sig["any"], sig["key"]
        for phrase in sig["any"]:
            field = phrase.split(":", 1)[0]
            # every query field must be a declared field for that stream
            assert field in cat["fields"].get(sig["stream"], {}).values(), phrase
    # the catalog's explicit "never use" list must exist
    assert any(e["event"] == "AGENT_RECONFIGURED" for e in cat["excluded_high_volume"])


def test_catalog_matches_skill_field_exact():
    cat = C.load_catalog()
    cause = {s["key"]: s for s in cat["signals"]}
    assert "View@6876_EventType:VLSI_DESKTOP_PUSH_IMAGE_SCHEDULED" in cause["vdi.image_push"]["any"]
    assert (
        "View@6876_EventType:BROKER_PROVISIONING_ERROR_VM_CLONING"
        in cause["vdi.provisioning_error"]["any"]
    )


def test_graylog_client_uses_token_as_basic_user_and_password():
    cli = C.GraylogClient("https://graylog.example/", "S3CRET", verify_ssl=False)
    hdr = cli._headers()
    decoded = base64.b64decode(hdr["Authorization"].split(" ", 1)[1]).decode()
    assert decoded == "S3CRET:token"          # token as username, literal "token" as password
    assert hdr["X-Requested-By"] == "fskb-monitor"


class _FakeGraylog:
    def __init__(self, cause=0, effect=0, prior=0):
        self.cause, self.effect, self.prior = cause, effect, prior

    def search(self, stream_id, query, from_, to, limit=10):
        if "VLSI_DESKTOP_PUSH_IMAGE_SCHEDULED" in query:
            n = self.cause
        elif "BROKER_" in query:
            n = self.prior if limit == 1 else self.effect
        else:
            n = 0
        return {"total_results": n, "messages": []}


def _run(**kw):
    return C.corroborate(_FakeGraylog(**kw), C.load_catalog(), NOW - timedelta(minutes=60), NOW)


def test_confidence_cause_plus_elevated_effect_is_likely():
    res = _run(cause=2, effect=10, prior=3)  # 10 >= max(3, 2*3)
    assert res.confidence == C.CONF_LIKELY


def test_confidence_cause_only_is_consistent():
    res = _run(cause=2, effect=1, prior=0)  # effect count 1 -> not elevated
    assert res.confidence == C.CONF_CONSISTENT


def test_confidence_effect_only_elevated_is_consistent():
    res = _run(cause=0, effect=9, prior=2)
    assert res.confidence == C.CONF_CONSISTENT


def test_confidence_nothing_is_none():
    res = _run(cause=0, effect=0, prior=0)
    assert res.confidence == C.CONF_NONE


def test_effect_presence_alone_is_not_elevated():
    # A high-volume effect at its normal rate must NOT corroborate.
    res = _run(cause=0, effect=8, prior=8)
    eff = [r for r in res.results if r.role == "effect"][0]
    assert eff.elevated is False


def test_corroborate_queries_every_catalog_signal():
    res = _run(cause=1, effect=1, prior=0)
    assert len(res.results) == len(C.load_catalog()["signals"])
