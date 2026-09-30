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


# --- Horizon via MCP + pool alignment --------------------------------------
class _FakeMCP:
    def __init__(self, pools):
        self.pools = pools

    def call_tool(self, name, arguments=None):
        assert name == "desktop_pool_status"
        return {"pools": self.pools}


def test_horizon_pool_errors_is_a_presence_effect():
    mcp = _FakeMCP([
        {"name": "man2-vdi", "display_name": "GR Manufacturing Pool", "error_count": 4},
        {"name": "bos1-vdi", "display_name": "GR Office and Sales Pool", "error_count": 0},
    ])
    res = C.corroborate(_FakeGraylog(cause=0, effect=0), C.load_catalog(),
                        NOW - timedelta(minutes=60), NOW, mcp_client=mcp, pool_hint="man2")
    sig = [r for r in res.results if r.key == "vdi.pool_errors"][0]
    assert sig.count == 4 and sig.elevated is True  # presence mode: errors>0 matters
    assert sig.samples[0]["pool"] == "GR Manufacturing Pool"
    # cause none + effect present -> consistent with
    assert res.confidence == C.CONF_CONSISTENT
    assert any("aligned" in ln for ln in res.summary_lines())


def test_horizon_unavailable_is_not_fatal():
    class _Boom:
        def call_tool(self, name, arguments=None):
            raise RuntimeError("mcp down")

    res = C.corroborate(_FakeGraylog(cause=2, effect=9, prior=2), C.load_catalog(),
                        NOW - timedelta(minutes=60), NOW, mcp_client=_Boom())
    sig = [r for r in res.results if r.key == "vdi.pool_errors"][0]
    assert sig.count == 0  # degraded, but the alert still scores on Graylog
    assert res.confidence == C.CONF_LIKELY


def test_pool_errors_are_scoped_to_the_cluster_pool():
    # Errors on a DIFFERENT pool must NOT corroborate this cluster's pool.
    mcp = _FakeMCP([
        {"name": "man2-vdi", "display_name": "GR Manufacturing Pool", "error_count": 0},
        {"name": "bos1-vdi", "display_name": "GR Office and Sales Pool", "error_count": 13},
    ])
    res = C.corroborate(_FakeGraylog(cause=1, effect=0), C.load_catalog(),
                        NOW - timedelta(minutes=60), NOW, mcp_client=mcp, pool_hint="man2")
    sig = [r for r in res.results if r.key == "vdi.pool_errors"][0]
    assert sig.count == 0 and sig.elevated is False
    # cause present, no aligned effect -> only "consistent with"
    assert res.confidence == C.CONF_CONSISTENT


def test_horizon_queries_are_scoped_to_resolved_pool():
    seen = []

    class _G:
        def search(self, stream_id, query, frm, to, limit=10):
            seen.append(query)
            return {"total_results": 0, "messages": []}

    mcp = _FakeMCP([{"name": "man2-vdi", "display_name": "GR Manufacturing Pool", "error_count": 0}])
    C.corroborate(_G(), C.load_catalog(), NOW - timedelta(minutes=60), NOW,
                  mcp_client=mcp, pool_hint="man2")
    # every Horizon-stream query must carry the pool filter
    assert seen and all('View@6876_DesktopId:"man2-vdi"' in q for q in seen)


def test_no_pool_hint_leaves_queries_global():
    seen = []

    class _G:
        def search(self, stream_id, query, frm, to, limit=10):
            seen.append(query)
            return {"total_results": 0, "messages": []}

    C.corroborate(_G(), C.load_catalog(), NOW - timedelta(minutes=60), NOW)
    assert seen and not any("DesktopId" in q for q in seen)


# --- change feed (FreshService changes as a cause) --------------------------
class _FakeChanges:
    def __init__(self, changes):
        self.changes = changes

    def list_changes(self, updated_since=None):
        return iter(self.changes)


def _ch(cid, subject, start, end, impacted=None):
    return {"id": cid, "subject": subject, "planned_start_date": start,
            "planned_end_date": end, "impacted_services": impacted or []}


_IN = "2026-09-30T11:45:00Z"      # overlaps the [11:00,12:00] cluster window
_OUT = "2026-09-30T12:30:00Z"
_PAST_S, _PAST_E = "2026-09-29T04:00:00Z", "2026-09-29T04:30:00Z"


def test_change_requires_planned_window_overlap():
    inwin = _ch(1, "Change affecting Engage", _IN, _OUT, ["Engage"])
    past = _ch(2, "Change affecting Engage", _PAST_S, _PAST_E, ["Engage"])
    sig = C.corroborate_changes(_FakeChanges([inwin, past]), NOW - timedelta(minutes=60), NOW,
                                keywords=["engage"])
    assert sig.count == 1 and sig.samples[0]["id"] == 1


def test_change_prefers_impacted_services_with_subject_fallback():
    by_impact = _ch(1, "Routine maintenance", _IN, _OUT, ["Engage"])
    by_subject = _ch(2, "Updating Engage servers", _IN, _OUT, [])
    none = _ch(3, "Printer firmware", _IN, _OUT, [])
    sig = C.corroborate_changes(_FakeChanges([by_impact, by_subject, none]),
                                NOW - timedelta(minutes=60), NOW, keywords=["engage"])
    assert {s["id"] for s in sig.samples} == {1, 2}


def test_change_without_keywords_counts_all_in_window():
    changes = [_ch(1, "a", _IN, _OUT), _ch(2, "b", _IN, _OUT)]
    sig = C.corroborate_changes(_FakeChanges(changes), NOW - timedelta(minutes=60), NOW)
    assert sig.count == 2


def test_change_overlap_can_be_disabled():
    past = _ch(2, "Old change", _PAST_S, _PAST_E, ["Engage"])
    sig = C.corroborate_changes(_FakeChanges([past]), NOW - timedelta(minutes=60), NOW,
                                keywords=["engage"], require_overlap=False)
    assert sig.count == 1


def test_corroborate_includes_change_signal_when_client_present():
    changes = _FakeChanges([_ch(9, "Change affecting Engage", _IN, _OUT, ["Engage"])])
    res = C.corroborate(_FakeGraylog(cause=0, effect=0), C.load_catalog(),
                        NOW - timedelta(minutes=60), NOW, changes_client=changes,
                        keywords=["engage"])
    sig = [r for r in res.results if r.key == "change_feed"][0]
    assert sig.count == 1
    assert res.confidence == C.CONF_CONSISTENT  # cause present, no effect


# --- SaaS status scraping --------------------------------------------------
def test_parse_status_text_up_and_issues():
    up = C.parse_status_text("Microsoft 365 status is up Last checked 2026-09-30T13:30:00.000Z")
    assert up["status"] == "up" and up["checked"].startswith("2026-09-30T13:30")
    bad = C.parse_status_text("RingCentral status is down")
    assert bad["status"] == "issues"
    html = "<div>Microsoft 365 status is up</div>"
    assert C.parse_status_text(html)["status"] == "up"


def test_saas_status_counts_non_up_vendors(monkeypatch):
    cat = {"saas_status": [
        {"vendor": "Microsoft 365", "url": "http://x/m365"},
        {"vendor": "Cloudflare", "url": "http://x/cf"},
    ]}
    bodies = {"http://x/m365": "Microsoft 365 status is up", "http://x/cf": "Cloudflare status is down"}
    monkeypatch.setattr(C, "_http_get_text", lambda url, timeout=20: bodies[url])
    sig = C.corroborate_saas_status(cat)
    assert sig.count == 1
    assert {s["vendor"] for s in sig.samples if s["status"] != "up"} == {"Cloudflare"}
