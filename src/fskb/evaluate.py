"""Gold-set evaluation for the knowledge base.

Two question shapes, deliberately separated:

* **integrity ("self")** - query with a ticket's own symptom; does the index
  return that ticket? This only proves the pipeline works at all (embeddings
  land, fields filter). It is NOT a quality measure.
* **quality ("loo", leave-one-out)** - withhold the ticket from the results and
  check whether the top-k still surfaces an *equivalent fix* (another ticket in
  the same fix cluster). This is the number that matters: it measures whether a
  brand-new ticket would find the right prior fix.

Metrics: hit@1, hit@k, recall@k, MRR.

A gold set is JSONL; one item per query:

    {"query_ticket_id": 47211, "query_text": "<symptom>",
     "expected_ticket_ids": [47211, 47390], "category": "...", ...}

``expected_ticket_ids`` are the tickets whose resolution answers the query.
For a pure integrity item that is just the ticket itself; for a quality item it
is the ticket plus its cluster mates (or, once labeled, whichever tickets the
human says hold the right fix).
"""

from __future__ import annotations

import json
import random
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

SearchFn = Callable[..., List[dict]]


@dataclass
class GoldItem:
    query_ticket_id: int
    query_text: str
    expected_ticket_ids: List[int]
    category: Optional[str] = None
    sub_category: Optional[str] = None
    store: List[str] = field(default_factory=list)
    labeled: bool = False  # False = auto-generated, awaiting human review


@dataclass
class EvalReport:
    items: int = 0
    evaluated: int = 0
    hit_at_1: float = 0.0
    hit_at_k: float = 0.0
    recall_at_k: float = 0.0
    mrr: float = 0.0
    k: int = 3
    exclude_self: bool = False
    misses: List[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        data = asdict(self)
        data.pop("misses", None)
        data["miss_count"] = len(self.misses)
        return data


# --------------------------------------------------------------------------- #
# gold-set construction
# --------------------------------------------------------------------------- #
_RESOLUTION_NOISE = re.compile(r"[\s\W]+")


def resolution_signature(text: str) -> str:
    """A cheap equivalence key for a resolution: normalized lowercase text.

    Tickets that fix the same thing in the same words share a signature. Not
    perfect - a human label is still the gold standard - but it lets the tool
    pre-fill candidate expected ids so review is confirm-or-edit, not blank-page.
    """

    return _RESOLUTION_NOISE.sub("", (text or "").lower())[:400]


def cluster_by_resolution(documents: Iterable[dict]) -> Dict[str, List[int]]:
    clusters: Dict[str, List[int]] = {}
    for doc in documents:
        sig = resolution_signature(doc.get("resolution_text", ""))
        if len(sig) < 20:
            continue
        clusters.setdefault(sig, []).append(int(doc["ticket_id"]))
    return clusters


def build_gold(
    documents: Sequence[dict],
    sample_size: int = 50,
    seed: int = 42,
    include_cluster_mates: bool = True,
) -> List[GoldItem]:
    """Sample resolved tickets into a review-ready gold set.

    Each item starts with ``expected_ticket_ids`` = the ticket itself, plus
    (when clustering is on) any other ticket sharing its resolution signature.
    The operator then confirms or edits - the point is that the file is the
    durable, auditable ground truth, not a black box.
    """

    docs = [d for d in documents if d.get("symptom_text") and d.get("resolution_text")]
    rng = random.Random(seed)
    if sample_size and len(docs) > sample_size:
        docs = rng.sample(docs, sample_size)

    clusters = cluster_by_resolution(documents) if include_cluster_mates else {}
    sig_to_ids: Dict[str, List[int]] = clusters

    items: List[GoldItem] = []
    for doc in docs:
        tid = int(doc["ticket_id"])
        expected = [tid]
        if include_cluster_mates:
            sig = resolution_signature(doc.get("resolution_text", ""))
            mates = [i for i in sig_to_ids.get(sig, []) if i != tid]
            expected.extend(mates)
        items.append(
            GoldItem(
                query_ticket_id=tid,
                query_text=doc.get("symptom_text", ""),
                expected_ticket_ids=sorted(set(expected)),
                category=doc.get("category"),
                sub_category=doc.get("sub_category"),
                store=list(doc.get("store") or []),
                labeled=False,
            )
        )
    return items


def save_gold(items: Sequence[GoldItem], path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for item in items:
            fh.write(json.dumps(asdict(item), ensure_ascii=False) + "\n")


def load_gold(path: Path) -> List[GoldItem]:
    items: List[GoldItem] = []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            items.append(
                GoldItem(
                    query_ticket_id=int(raw["query_ticket_id"]),
                    query_text=raw.get("query_text", ""),
                    expected_ticket_ids=[int(x) for x in raw.get("expected_ticket_ids", [])],
                    category=raw.get("category"),
                    sub_category=raw.get("sub_category"),
                    store=list(raw.get("store") or []),
                    labeled=bool(raw.get("labeled", False)),
                )
            )
    return items


# --------------------------------------------------------------------------- #
# evaluation
# --------------------------------------------------------------------------- #
def evaluate(
    items: Sequence[GoldItem],
    search_fn: SearchFn,
    k: int = 3,
    exclude_self: bool = False,
) -> EvalReport:
    """Score a gold set against a search function.

    ``search_fn`` is called as ``search_fn(query_text, exclude_ticket_id=...,
    top=...)`` and must return a list of hit dicts (each with ``ticket_id``).
    Injecting it keeps this offline-testable.
    """

    report = EvalReport(items=len(items), k=k, exclude_self=exclude_self)
    hits1 = hitsk = 0
    rr_total = 0.0
    recall_total = 0.0

    for item in items:
        expected = set(item.expected_ticket_ids)
        if exclude_self:
            expected = {e for e in expected if e != item.query_ticket_id}
        # An LOO item with no remaining expected id cannot be graded; skip it.
        if not expected:
            if not exclude_self:
                # integrity item with an empty expectation is a data error
                report.misses.append({"ticket_id": item.query_ticket_id, "reason": "no_expected_ids"})
            continue

        report.evaluated += 1
        # Only withhold the ticket from its own results in LOO mode; integrity
        # mode deliberately expects a self-match.
        exclude_id = item.query_ticket_id if exclude_self else None
        results = search_fn(item.query_text, exclude_ticket_id=exclude_id, top=k)
        found = [int(r["ticket_id"]) for r in results]
        relevant_ranks = [idx for idx, tid in enumerate(found, start=1) if tid in expected]

        if relevant_ranks:
            hitsk += 1
            rr_total += 1.0 / relevant_ranks[0]
            if relevant_ranks[0] == 1:
                hits1 += 1
            recall_total += len(set(found) & expected) / len(expected)
        else:
            report.misses.append(
                {
                    "ticket_id": item.query_ticket_id,
                    "query": item.query_text[:160],
                    "expected": sorted(expected),
                    "got": found[:k],
                }
            )

    n = report.evaluated or 1
    report.hit_at_1 = round(hits1 / n, 4)
    report.hit_at_k = round(hitsk / n, 4)
    report.recall_at_k = round(recall_total / n, 4)
    report.mrr = round(rr_total / n, 4)
    return report


def format_report(report: EvalReport) -> str:
    mode = "leave-one-out" if report.exclude_self else "integrity (self)"
    lines = [
        f"mode: {mode}   k={report.k}",
        f"items: {report.items}   evaluated: {report.evaluated}   misses: {len(report.misses)}",
        f"hit@1:    {report.hit_at_1:.1%}",
        f"hit@{report.k}:    {report.hit_at_k:.1%}",
        f"recall@{report.k}: {report.recall_at_k:.1%}",
        f"MRR:      {report.mrr:.3f}",
    ]
    if report.misses:
        lines.append("")
        lines.append("sample misses:")
        for miss in report.misses[:5]:
            lines.append(
                f"  - [{miss['ticket_id']}] expected {miss.get('expected')} got {miss.get('got')}"
            )
    return "\n".join(lines)
