"""Offline tests for the correlated-ticket monitor (Phase 1). No network."""

from datetime import datetime, timedelta, timezone

from fskb import monitor as M
from fskb.clusters import match_keys, primary_cluster_key, system_key
from fskb.monitor import ClusterStore, LiveWindow, MonitorConfig, baseline_gate, parse_ticket

UTC = timezone.utc
T0 = datetime(2026, 9, 30, 11, 0, 0, tzinfo=UTC)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# The real descriptions of the motivating 2026-09-30 Engage cluster.
REAL = {
    47450: ("Joseph Singleton Login", "Contact Love, Nate Computer MAN2-VDI-128 Joseph Singleton "
            "cannot log into Engage. It also brings up a message with an error about OneDrive.", "Software", "Microsoft"),
    47449: ("[Other][Blocking] LOGIN INTO ENGAGE", "LOGIN INTO ENGAGE Category Other Severity "
            "Blocking can't work Store 5 - Grandville Production Form Parts User ID mswitzer", "Engage", "Engage Desktop"),
    47447: ("Log in", "I am trying to log in to engage. It says one drive has to be reinstalled. "
            "I have shut my computer down and turned it back on already and still says the same thing.", "Software", "Microsoft"),
    47446: ("Nate Whitney's computer", "Nate Whitney cannot log onto engage, outlook, or one drive "
            "this morning.", "Software", "Microsoft"),
    47452: ("WH-MAN 3 AND WH-MAN 4 engage", "WH-MAN 3 AND WH-MAN 4 We cannot start up engage this "
            "maorning ist saying no signal", "Engage", "Engage Desktop"),
}


# --- matcher ---------------------------------------------------------------
def test_every_motivating_ticket_maps_to_engage():
    for tid, (subj, desc, cat, sub) in REAL.items():
        assert primary_cluster_key(f"{subj}\n{desc}") == "app:engage", tid


def test_not_every_ticket_mentions_onedrive_but_cluster_still_groups():
    # The whole point: 47449 never mentions OneDrive yet belongs to the cluster.
    subj, desc, _, _ = REAL[47449]
    keys = match_keys(f"{subj}\n{desc}")
    assert "app:engage" in keys
    assert "sys:onedrive" not in keys  # proves detection is not on the culprit word


def test_ticket_can_match_multiple_keys_with_weighted_primary():
    subj, desc, _, _ = REAL[47446]  # engage + outlook + one drive
    keys = match_keys(f"{subj}\n{desc}")
    assert keys[0] == "app:engage"           # weight 90 wins
    assert "app:outlook" in keys
    assert "sys:onedrive" in keys


def test_word_boundaries_avoid_false_positive():
    # "engagement" must not match "engage".
    assert "app:engage" not in match_keys("customer engagement survey")
    assert primary_cluster_key("nothing relevant here") is None


def test_system_key_uses_classification_only():
    assert system_key("Engage", "Engage Desktop") == "engage/engage desktop"
    assert system_key("Software", None) == "software"
    assert system_key(None, None) == "uncategorized"


# --- parse -----------------------------------------------------------------
def test_parse_ticket_requires_id_and_created():
    assert parse_ticket({"description_text": "x"}) is None
    assert parse_ticket({"id": 1}) is None
    t = parse_ticket({"id": 5, "created_at": _iso(T0), "description_text": "can't open engage"})
    assert t.id == 5 and t.primary_key == "app:engage"


# --- window ----------------------------------------------------------------
def test_window_counts_and_evicts():
    w = LiveWindow(window_minutes=60)
    for i in range(3):
        t = parse_ticket({"id": i + 1, "created_at": _iso(T0 + timedelta(minutes=i)),
                          "description_text": "cannot get into engage"})
        w.add(t)
    assert len(w.clusters(T0 + timedelta(minutes=5), 3)["app:engage"]) == 3
    # A 4th ticket added later but stamped outside the window must be evicted.
    w.add(parse_ticket({"id": 99, "created_at": _iso(T0 - timedelta(minutes=90)),
                        "description_text": "engage down"}))
    w.evict(T0 + timedelta(minutes=5))
    assert w.tickets("app:engage", T0 + timedelta(minutes=5))  # still the 3
    assert len(w.clusters(T0 + timedelta(minutes=5), 4)) == 0    # not 4 -> no cluster
    # Advance past the window: everything drains.
    w.evict(T0 + timedelta(minutes=200))
    assert w.keys() == []


# --- store / cooldown ------------------------------------------------------
def test_store_cooldown_lifecycle(tmp_path):
    s = ClusterStore(tmp_path / "m.sqlite")
    assert s.in_cooldown("app:engage", T0) is None
    aid = s.record_alert("app:engage", 47446, T0, 4, T0 + timedelta(minutes=120))
    assert aid > 0
    assert s.in_cooldown("app:engage", T0 + timedelta(minutes=30)) is not None
    assert s.in_cooldown("app:engage", T0 + timedelta(minutes=121)) is None
    s.record_feedback(aid, "accept", "Axle", "real VDI push")
    row = s.conn.execute("SELECT * FROM feedback WHERE alert_id=?", (aid,)).fetchone()
    assert row["verdict"] == "accept"
    s.close()


def test_heartbeat_and_meta(tmp_path):
    s = ClusterStore(tmp_path / "m.sqlite")
    s.heartbeat(T0)
    assert s.get_meta("last_poll_at") == _iso(T0)
    s.close()


def test_read_meta_readonly_from_another_thread(tmp_path):
    # The /health server thread reads meta; a main-thread connection would raise
    # sqlite3.ProgrammingError. The read-only helper must work cross-thread.
    import threading

    s = ClusterStore(tmp_path / "m.sqlite")
    s.heartbeat(T0)
    out = {}

    def run():
        out["v"] = s.read_meta_readonly("last_poll_at")

    t = threading.Thread(target=run)
    t.start()
    t.join()
    assert out["v"] == _iso(T0)
    assert s.read_meta_readonly("missing", "dflt") == "dflt"
    s.close()


# --- monitor replay / cooldown firing -------------------------------------
def _synthetic(n, minute_step=1, key_text="cannot get into engage"):
    return [
        {"id": 1000 + i, "created_at": _iso(T0 + timedelta(minutes=i * minute_step)),
         "subject": f"t{i}", "description_text": key_text, "category": "Engage", "sub_category": "Engage Desktop"}
        for i in range(n)
    ]


def test_replay_fires_one_alert_and_posts_no_note_in_dry_run(tmp_path):
    lines = []
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lines.append)
    fired = mon.replay(_synthetic(3))
    assert len(fired) == 1
    assert any("dry-run" in ln for ln in lines)
    assert any("#1000" in ln for ln in lines)  # oldest ticket named in the note


def test_second_evaluate_is_suppressed_by_cooldown(tmp_path):
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    mon.replay(_synthetic(3))
    again = mon.evaluate(T0 + timedelta(minutes=5))
    assert again == []  # cooldown holds


def test_below_threshold_does_not_fire(tmp_path):
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    assert mon.replay(_synthetic(2)) == []


def test_different_keys_fire_separately(tmp_path):
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    tickets = _synthetic(3, key_text="cannot get into engage")
    tickets += [{"id": 2000 + i, "created_at": _iso(T0 + timedelta(minutes=i)),
                 "description_text": "vpn will not connect", "subject": "vpn"} for i in range(3)]
    fired = mon.replay(tickets)
    assert len(fired) == 2


# --- note composition ------------------------------------------------------
def test_compose_note_contains_ids_and_disclaimer(tmp_path):
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"))
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    tickets = [parse_ticket(t) for t in _synthetic(3)]
    note = mon.compose_note("app:engage", tickets, T0)
    assert "#1000" in note and "#1002" in note
    assert "no tickets were merged" in note.lower()
    assert "engage/engage desktop" in note


# --- baseline gate (Phase 2 seam) -----------------------------------------
def test_baseline_gate():
    assert baseline_gate(3, None, None) is True
    assert baseline_gate(2, 1.0, 0.2) is False
    # median 1, mad 0.5 -> sigma ~0.74 -> threshold ~3.2 -> 5 fires, 3 does not
    assert baseline_gate(5, 1.0, 0.5) is True
    assert baseline_gate(3, 1.0, 0.5) is False


# --- client write path (multipart private note) ----------------------------
class _FakeResp:
    status_code = 200
    headers = {}
    content = b'{"note": {"id": 1}}'

    def json(self):
        return {"note": {"id": 1}}


class _FakeSession:
    def __init__(self):
        self.calls = []
        self.headers = {}
        self.auth = None

    def get(self, *a, **k):  # pragma: no cover
        return _FakeResp()

    def post(self, url, data=None, files=None, timeout=None):
        self.calls.append((url, files))
        return _FakeResp()


def _settings():
    from fskb.config import Settings

    return Settings(fs_base_url="https://x.freshservice.com", fs_api_key="k")


def test_add_private_note_posts_multipart():
    from fskb.freshservice import FreshServiceClient

    sess = _FakeSession()
    client = FreshServiceClient(_settings(), session=sess)
    client.add_private_note(47446, "hello")
    url, files = sess.calls[-1]
    assert url.endswith("/api/v2/tickets/47446/notes")
    assert files["body"][1] == "hello"
    assert files["private"][1] == "true"


def test_list_tickets_created_since_is_local_filter():
    # FreshService rejects created_since (400); we pull by updated_since (superset)
    # and filter created_at locally.
    from fskb.freshservice import FreshServiceClient

    captured = {}
    payload = {"tickets": [
        {"id": 1, "created_at": "2026-09-30T09:00:00Z", "updated_at": "2026-09-30T09:30:00Z"},
        {"id": 2, "created_at": "2026-09-30T10:30:00Z", "updated_at": "2026-09-30T10:31:00Z"},
    ]}

    class _Sess(_FakeSession):
        def get(self, url, params=None, timeout=None):
            captured.update(params or {})
            return type("R", (), {"status_code": 200, "headers": {},
                                  "json": lambda self: payload})()

    client = FreshServiceClient(_settings(), session=_Sess())
    got = list(client.list_tickets(created_since="2026-09-30T10:00:00Z"))
    assert captured.get("updated_since") == "2026-09-30T10:00:00Z"  # superset pull
    assert "created_since" not in captured                          # API would 400 on it
    assert [t["id"] for t in got] == [2]                           # local created_at filter


# --- Phase 2: baseline aggregation + gate ----------------------------------
from zoneinfo import ZoneInfo  # noqa: E402

import statistics  # noqa: E402

DETROIT = ZoneInfo("America/Detroit")


def _ticket_at(tid, dt, cat="Engage", sub="Engage Desktop"):
    return {"id": tid, "created_at": _iso(dt), "category": cat, "sub_category": sub}


def test_compute_baseline_rows_median_mad_and_zero_fill():
    now = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)  # a Wednesday
    weeks = 4
    week_keys = M._weeks_in_window(now, weeks, DETROIT)
    k = len(week_keys)
    # Key A: j+1 tickets in each week's Monday 09:00 -> deterministic series.
    now_local = now.astimezone(DETROIT)
    monday_local = (now_local - timedelta(days=now_local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    tickets = []
    tid = 0
    for j, _ in enumerate(week_keys):
        for _n in range(j + 1):
            d = monday_local - timedelta(weeks=j) + timedelta(hours=9, minutes=_n)
            tickets.append(_ticket_at(tid := tid + 1, d.astimezone(UTC)))
    # Key B: only 3 tickets, all in the most recent week -> other weeks zero-fill.
    for _n in range(3):
        d = monday_local + timedelta(hours=9, minutes=_n)
        tickets.append(_ticket_at(tid := tid + 1, d.astimezone(UTC), cat="Software", sub="Microsoft"))

    rows = {(r[0], r[1], r[2]): r for r in M.compute_baseline_rows(tickets, weeks, "America/Detroit", now)}
    a = rows[("engage/engage desktop", 0, 9)]
    assert a[3] == k  # n == number of weeks observed
    assert a[4] == statistics.median(range(1, k + 1))
    assert a[5] == statistics.median([abs(v - a[4]) for v in range(1, k + 1)])
    b = rows[("software/microsoft", 0, 9)]
    assert b[3] == k and b[4] == 0.0  # 3 real + zeros -> median 0


def test_baseline_threshold_none_without_rows(tmp_path):
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"))
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    tickets = [parse_ticket(t) for t in _synthetic(3)]
    assert mon.baseline_threshold(tickets, T0 + timedelta(minutes=5)) is None


def _monitor_with_baseline(tmp_path, median, mad):
    now = T0 + timedelta(minutes=5)
    local = now.astimezone(DETROIT)
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    mon.store.replace_baseline(
        [("engage/engage desktop", local.weekday(), local.hour, 8, median, mad)], M.iso(now)
    )
    return mon, now


def test_low_baseline_still_fires(tmp_path):
    mon, now = _monitor_with_baseline(tmp_path, median=0.0, mad=0.0)  # threshold max(3,0)=3
    assert len(mon.replay(_synthetic(3), now=now)) == 1


def test_high_baseline_suppresses_cluster(tmp_path):
    # Normal volume at this (dow,hour) is ~10, so a 3-ticket cluster is not news.
    mon, now = _monitor_with_baseline(tmp_path, median=10.0, mad=0.0)  # threshold 10
    lines = []
    mon.emit = lines.append
    assert mon.replay(_synthetic(3), now=now) == []
    assert any("[suppressed]" in ln for ln in lines)


def test_no_baseline_falls_back_to_phase1_rule(tmp_path):
    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=None, emit=lambda *_: None)
    mon.store.replace_baseline([], M.iso(T0))  # empty baseline
    assert len(mon.replay(_synthetic(3), now=T0 + timedelta(minutes=5))) == 1


def test_run_baseline_writes_rows_and_stamp(tmp_path):
    class _Client:
        def list_tickets(self, **kw):
            return [_ticket_at(i + 1, T0 - timedelta(weeks=i)) for i in range(3)]

    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), baseline_weeks=8)
    mon = M.CorrelatedMonitor(cfg, client=_Client(), emit=lambda *_: None)
    rows, tickets = mon.run_baseline(now=T0)
    assert rows >= 1 and tickets == 3
    assert mon.store.get_meta("baseline_at") == _iso(T0)
    assert len(mon.store.all_baseline()) >= 1


def test_run_baseline_writes_even_in_dry_run(tmp_path):
    class _Client:
        def list_tickets(self, **kw):
            return [_ticket_at(i + 1, T0 - timedelta(weeks=i)) for i in range(3)]

    cfg = MonitorConfig(db_path=str(tmp_path / "m.sqlite"), baseline_weeks=8, dry_run=True)
    mon = M.CorrelatedMonitor(cfg, client=_Client(), emit=lambda *_: None)
    mon.run_baseline(now=T0)
    assert mon.store.get_meta("baseline_at") == _iso(T0)
