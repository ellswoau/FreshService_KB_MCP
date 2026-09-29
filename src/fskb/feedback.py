"""Feedback loop: log what agents actually do with retrieved suggestions.

The evaluation set measures retrieval offline. This measures it in production:
when an agent is shown top-k KB suggestions on a ticket and then acts, we log
whether a suggestion was accepted and whether the ticket resolved. That signal
is what lets the recency/frequency weights be tuned against reality rather than
vibes.

One JSONL line per (ticket, suggestion) judgement:

    {"at": "...", "ticket_id": 48001, "suggested_ticket_id": 47211,
     "rank": 1, "accepted": true, "resolved": true, "agent": "Axel W",
     "note": "same fix worked"}

``accepted``     - did the agent use/confirm this suggestion?
``resolved``     - did the ticket end up resolved (the outcome)? None = unknown.
``outcome``      - "resolved" | "not_resolved" | "unknown" when resolved is None.

Aggregation produces per-suggested-ticket accept rates and a recency-aware
boost table the caller can blend into ranking.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class FeedbackEvent:
    ticket_id: int
    suggested_ticket_id: int
    rank: Optional[int] = None
    accepted: bool = False
    resolved: Optional[bool] = None
    agent: Optional[str] = None
    note: Optional[str] = None
    at: Optional[str] = None

    def finalize(self) -> "FeedbackEvent":
        if self.at is None:
            self.at = datetime.now(timezone.utc).isoformat()
        return self

    @property
    def outcome(self) -> str:
        if self.resolved is True:
            return "resolved"
        if self.resolved is False:
            return "not_resolved"
        return "unknown"


def log_event(path: Path, event: FeedbackEvent) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(event.finalize()), ensure_ascii=False) + "\n")


def read_events(path: Path) -> List[FeedbackEvent]:
    path = Path(path)
    if not path.exists():
        return []
    events: List[FeedbackEvent] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            events.append(
                FeedbackEvent(
                    ticket_id=int(raw["ticket_id"]),
                    suggested_ticket_id=int(raw["suggested_ticket_id"]),
                    rank=raw.get("rank"),
                    accepted=bool(raw.get("accepted", False)),
                    resolved=raw.get("resolved"),
                    agent=raw.get("agent"),
                    note=raw.get("note"),
                    at=raw.get("at"),
                )
            )
    return events


@dataclass
class SuggestionStats:
    suggested_ticket_id: int
    shown: int = 0
    accepted: int = 0
    resolved_after_accept: int = 0

    @property
    def accept_rate(self) -> float:
        return round(self.accepted / self.shown, 4) if self.shown else 0.0

    @property
    def success_rate(self) -> float:
        """Of the times this suggestion was accepted, how often did it resolve?"""

        return round(self.resolved_after_accept / self.accepted, 4) if self.accepted else 0.0


def summarize(path: Path) -> Dict[int, SuggestionStats]:
    """Per-suggested-ticket stats. Deduplicated by (ticket, suggestion) keeping
    the last event, so a re-judged suggestion is not double-counted."""

    latest: Dict[tuple, FeedbackEvent] = {}
    for event in read_events(path):
        latest[(event.ticket_id, event.suggested_ticket_id)] = event

    stats: Dict[int, SuggestionStats] = {}
    for event in latest.values():
        s = stats.setdefault(event.suggested_ticket_id, SuggestionStats(event.suggested_ticket_id))
        s.shown += 1
        if event.accepted:
            s.accepted += 1
            if event.resolved is True:
                s.resolved_after_accept += 1
    return stats


def boost_table(path: Path) -> Dict[int, float]:
    """A simple multiplicative boost per suggested ticket id.

    Within [1.0, 1.5]: success rate drives most of it, with a small volume term
    so a suggestion used often but rarely failing still edges out a one-off.
    Persist this (or recompute at serve time) and multiply into the base score.
    """

    stats = summarize(path)
    boosts: Dict[int, float] = {}
    for tid, s in stats.items():
        if s.accepted == 0:
            continue
        volume = min(s.accepted / 10.0, 0.1)
        boosts[tid] = round(1.0 + 0.4 * s.success_rate + volume, 4)
    return boosts


def write_boost_table(path: Path, out: Path) -> None:
    table = boost_table(path)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump({str(k): v for k, v in table.items()}, fh, indent=2, sort_keys=True)
        os.replace(tmp, out)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
