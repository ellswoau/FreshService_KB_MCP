"""Cluster-key matcher: detect correlated ticket clusters from description text.

Detection runs on the requester's own words (``description_text``), **not** on
FreshService ``category`` / ``sub_category``. A cluster's tickets arrive
untagged, and one ticket may never name the shared culprit -- the 2026-09-30
Engage/OneDrive cluster (47450/47449/47447/47446) had tickets that never said
"OneDrive" at all. Category/sub_category is kept only as a *system key*
(:func:`system_key`) for grouping and baselining, never as the detection key.

Matching is a lightweight, dependency-free any-of over a curated rule table:
a rule fires when **any** of its phrases is present. A ticket may match several
rules; the highest ``weight`` wins as the primary cluster key.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

_NON_WORD = re.compile(r"[^a-z0-9]+")


def normalize(text: Optional[str]) -> str:
    """Lowercase, strip punctuation, collapse whitespace to single spaces.

    So "can't login" -> "can t login" and matching is punctuation-insensitive.
    """

    if not text:
        return ""
    return _NON_WORD.sub(" ", str(text).lower()).strip()


@dataclass(frozen=True)
class ClusterRule:
    """One detectable cluster. ``any`` is a tuple of normalized-ish phrases."""

    key: str  # canonical slug, e.g. "app:engage"
    label: str  # human label for notes/alerts
    any: Tuple[str, ...]  # any-of phrases; multi-word phrases match as substring
    weight: int = 50  # higher wins when one ticket matches several rules

    def matches(self, normalized: str) -> bool:
        if not normalized:
            return False
        padded = f" {normalized} "
        for phrase in self.any:
            p = normalize(phrase)
            if not p:
                continue
            if " " in p:
                if p in normalized:
                    return True
            elif f" {p} " in padded:
                return True
        return False


# The curated table. Order does not matter; weight breaks ties. Keep entries
# broad enough to catch wording variants but specific enough not to over-cluster.
CLUSTER_RULES: Tuple[ClusterRule, ...] = (
    ClusterRule("app:engage", "Engage", ("engage",), weight=90),
    ClusterRule("app:outlook", "Outlook", ("outlook", "outlook web", "owa"), weight=70),
    ClusterRule("app:teams", "Teams", ("microsoft teams", "ms teams", "teams"), weight=70),
    ClusterRule("sys:onedrive", "OneDrive", ("onedrive", "one drive"), weight=60),
    ClusterRule("vpn:connect", "VPN", ("vpn", "anyconnect", "globalprotect"), weight=80),
    ClusterRule("auth:login", "Login / lockout", (
        "cannot log in", "cannot login", "can t log in", "can t login", "can not log in",
        "can not login", "can t log on", "cannot log on", "locked out", "account locked",
        "password expired",
    ), weight=50),
    ClusterRule("net:connectivity", "Network / connectivity", (
        "no signal", "no internet", "wifi", "wi fi", "wireless", "network down",
    ), weight=40),
    ClusterRule("print:printer", "Printer", ("printer", "print queue", "printing"), weight=40),
)


def match_keys(text: Optional[str]) -> List[str]:
    """Cluster keys this text matches, ordered by descending weight (primary first)."""

    normalized = normalize(text)
    hits = [r for r in CLUSTER_RULES if r.matches(normalized)]
    hits.sort(key=lambda r: r.weight, reverse=True)
    return [r.key for r in hits]


def primary_cluster_key(text: Optional[str]) -> Optional[str]:
    """The single strongest cluster key, or None when nothing matches."""

    keys = match_keys(text)
    return keys[0] if keys else None


def rule_for(key: str, rules: Sequence[ClusterRule] = CLUSTER_RULES) -> Optional[ClusterRule]:
    for r in rules:
        if r.key == key:
            return r
    return None


def label_for(key: str) -> str:
    r = rule_for(key)
    return r.label if r else key


def system_key(category: Optional[str], sub_category: Optional[str]) -> str:
    """Grouping/baseline key from FreshService classification (never detection)."""

    cat = (category or "uncategorized").strip().lower()
    sub = (sub_category or "").strip().lower()
    return f"{cat}/{sub}" if sub else cat
