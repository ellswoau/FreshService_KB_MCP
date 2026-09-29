"""Offline tests for the gold-set evaluation and the feedback loop."""

import json

from fskb.evaluate import (
    EvalReport,
    GoldItem,
    build_gold,
    cluster_by_resolution,
    evaluate,
    format_report,
    load_gold,
    resolution_signature,
    save_gold,
)
from fskb.feedback import (
    FeedbackEvent,
    boost_table,
    log_event,
    read_events,
    summarize,
    write_boost_table,
)


def _docs():
    return [
        {
            "ticket_id": 101,
            "symptom_text": "Outlook account locked after password change",
            "resolution_text": "Unlocked account in AD and cleared cached credential on the phone.",
            "category": "User Account",
            "sub_category": "Account Unlocking",
            "store": ["8-Atlanta"],
        },
        {
            "ticket_id": 102,
            "symptom_text": "Cannot sign in, account locked following password reset",
            "resolution_text": "Unlocked account in AD and cleared cached credential on the phone.",
            "category": "User Account",
            "sub_category": "Account Unlocking",
            "store": ["8-Atlanta"],
        },
        {
            "ticket_id": 103,
            "symptom_text": "WorldShip label printer offline",
            "resolution_text": "Restarted the print spooler and re-added the UPS printer.",
            "category": "Hardware",
            "store": ["5-Boston"],
        },
    ]


# --- gold construction ----------------------------------------------------
def test_resolution_signature_stable():
    a = resolution_signature("Unlocked account in AD.")
    b = resolution_signature("unlocked   account in ad")
    assert a == b


def test_cluster_by_resolution_groups_mates():
    clusters = cluster_by_resolution(_docs())
    sizes = sorted(len(v) for v in clusters.values())
    assert sizes == [1, 2]


def test_build_gold_samples_and_clusters():
    items = build_gold(_docs(), sample_size=10, seed=1)
    assert len(items) == 3
    by_id = {i.query_ticket_id: i for i in items}
    # 101 and 102 share a resolution -> each expects the other too
    assert 102 in by_id[101].expected_ticket_ids
    assert 101 in by_id[102].expected_ticket_ids
    # 103 has a unique fix
    assert by_id[103].expected_ticket_ids == [103]
    assert all(i.labeled is False for i in items)


def test_build_gold_without_clusters():
    items = build_gold(_docs(), sample_size=10, seed=1, include_cluster_mates=False)
    assert all(len(i.expected_ticket_ids) == 1 for i in items)


def test_gold_roundtrip(tmp_path):
    items = build_gold(_docs(), sample_size=10, seed=1)
    path = tmp_path / "gold.jsonl"
    save_gold(items, path)
    again = load_gold(path)
    assert len(again) == len(items)
    assert again[0].expected_ticket_ids == items[0].expected_ticket_ids


# --- evaluation -----------------------------------------------------------
def _perfect_search(_text, exclude_ticket_id=None, top=3):
    # Pretend retrieval always returns the right prior fix first.
    mapping = {
        "Outlook account locked after password change": [{"ticket_id": 102}, {"ticket_id": 999}],
        "Cannot sign in, account locked following password reset": [{"ticket_id": 101}, {"ticket_id": 998}],
        "WorldShip label printer offline": [{"ticket_id": 103}],
    }
    return mapping.get(_text, [])[:top]


def _empty_search(_text, exclude_ticket_id=None, top=3):
    return []


def test_evaluate_loo_perfect():
    items = build_gold(_docs(), sample_size=10, seed=1)
    report = evaluate(items, _perfect_search, k=3, exclude_self=True)
    # 103 has a unique fix, so in LOO mode its only expected id is itself and is
    # withheld -> ungradeable. Only the two clustered tickets (101/102) count.
    assert report.evaluated == 2
    assert report.hit_at_1 == 1.0
    assert report.hit_at_k == 1.0
    assert report.mrr == 1.0
    assert report.misses == []


def test_evaluate_integrity_grades_all_three():
    items = build_gold(_docs(), sample_size=10, seed=1)
    report = evaluate(items, _perfect_search, k=3, exclude_self=False)
    assert report.evaluated == 3
    assert report.hit_at_k == 1.0


def test_integrity_mode_does_not_exclude_self():
    # Regression: integrity mode must NOT pass exclude_ticket_id, or a
    # self-match can never be found.
    seen = {}

    def spy(text, exclude_ticket_id=None, top=3):
        seen["exclude"] = exclude_ticket_id
        return [{"ticket_id": 103}]

    items = [GoldItem(query_ticket_id=103, query_text="q", expected_ticket_ids=[103])]
    report = evaluate(items, spy, k=3, exclude_self=False)
    assert seen["exclude"] is None
    assert report.hit_at_1 == 1.0


def test_evaluate_loo_drops_self_expectation():
    # An item whose only expected id is itself is skipped in LOO mode.
    item = GoldItem(query_ticket_id=103, query_text="x", expected_ticket_ids=[103])
    report = evaluate([item], _perfect_search, k=3, exclude_self=True)
    assert report.evaluated == 0  # nothing gradeable, and not counted
    assert report.misses == []


def test_evaluate_integrity_counts_self():
    item = GoldItem(query_ticket_id=103, query_text="WorldShip label printer offline", expected_ticket_ids=[103])
    report = evaluate([item], _perfect_search, k=3, exclude_self=False)
    assert report.evaluated == 1
    assert report.hit_at_1 == 1.0


def test_evaluate_misses_recorded():
    items = [GoldItem(query_ticket_id=101, query_text="Outlook account locked after password change", expected_ticket_ids=[102])]
    report = evaluate(items, _empty_search, k=3, exclude_self=True)
    assert report.hit_at_k == 0.0
    assert len(report.misses) == 1


def test_evaluate_reports_empty_expected_as_miss():
    item = GoldItem(query_ticket_id=1, query_text="x", expected_ticket_ids=[])
    report = evaluate([item], _perfect_search, k=3, exclude_self=False)
    assert report.misses and report.misses[0]["reason"] == "no_expected_ids"


def test_format_report_smoke():
    report = EvalReport(items=1, evaluated=1, hit_at_1=1.0, hit_at_k=1.0, recall_at_k=1.0, mrr=1.0, k=3)
    text = format_report(report)
    assert "hit@1" in text and "leave-one-out" not in text  # integrity wording
    assert "integrity" in text


def test_evaluate_passes_exclude_to_search():
    seen = {}

    def spy(text, exclude_ticket_id=None, top=3):
        seen["exclude"] = exclude_ticket_id
        return [{"ticket_id": 102}]

    items = [GoldItem(query_ticket_id=101, query_text="q", expected_ticket_ids=[102])]
    evaluate(items, spy, k=3, exclude_self=True)
    assert seen["exclude"] == 101


# --- feedback -------------------------------------------------------------
def test_feedback_log_and_summarize(tmp_path):
    log = tmp_path / "events.jsonl"
    log_event(log, FeedbackEvent(ticket_id=1, suggested_ticket_id=101, rank=1, accepted=True, resolved=True))
    log_event(log, FeedbackEvent(ticket_id=2, suggested_ticket_id=101, rank=1, accepted=True, resolved=False))
    log_event(log, FeedbackEvent(ticket_id=3, suggested_ticket_id=101, rank=2, accepted=False))
    log_event(log, FeedbackEvent(ticket_id=4, suggested_ticket_id=202, accepted=True, resolved=True))
    events = read_events(log)
    assert len(events) == 4

    stats = summarize(log)
    s101 = stats[101]
    assert s101.shown == 3
    assert s101.accepted == 2
    assert s101.resolved_after_accept == 1
    assert s101.accept_rate == round(2 / 3, 4)
    assert s101.success_rate == 0.5


def test_feedback_dedupes_by_pair(tmp_path):
    log = tmp_path / "events.jsonl"
    log_event(log, FeedbackEvent(ticket_id=1, suggested_ticket_id=101, accepted=False))
    log_event(log, FeedbackEvent(ticket_id=1, suggested_ticket_id=101, accepted=True, resolved=True))
    stats = summarize(log)
    assert stats[101].shown == 1  # last event wins, not double counted
    assert stats[101].accepted == 1


def test_feedback_outcome_property():
    assert FeedbackEvent(1, 1, resolved=True).outcome == "resolved"
    assert FeedbackEvent(1, 1, resolved=False).outcome == "not_resolved"
    assert FeedbackEvent(1, 1, resolved=None).outcome == "unknown"


def test_boost_table_bounds(tmp_path):
    log = tmp_path / "events.jsonl"
    for i in range(20):
        log_event(log, FeedbackEvent(ticket_id=i, suggested_ticket_id=101, accepted=True, resolved=True))
    boost = boost_table(log)
    assert 1.0 <= boost[101] <= 1.5
    # A never-accepted suggestion gets no boost entry.
    log_event(log, FeedbackEvent(ticket_id=99, suggested_ticket_id=777, accepted=False))
    assert 777 not in boost_table(log)


def test_write_boost_table(tmp_path):
    log = tmp_path / "events.jsonl"
    log_event(log, FeedbackEvent(ticket_id=1, suggested_ticket_id=101, accepted=True, resolved=True))
    out = tmp_path / "boost.json"
    write_boost_table(log, out)
    data = json.loads(out.read_text())
    assert data == {"101": 1.5} or data["101"] >= 1.0
