"""Entity extraction that makes hybrid (BM25 + vector) search work.

Vectors are bad at exact tokens: error codes, hostnames, SKUs. We pull those out
into filterable/keyword fields so a Tangier exact-string query still matches.
"""

from __future__ import annotations

import re
from typing import Iterable, List

# Windows/ODBC/HRESULT-style codes, plus hex and common error patterns.
_ERROR_CODE_PATTERNS = [
    re.compile(r"\b0x[0-9A-Fa-f]{4,8}\b"),                       # 0x80070005
    re.compile(r"\b0[xX][0-9A-Fa-f]{3,}\b"),                     # generic hex
    re.compile(r"\b(?:error|err|code|status)\s*[:#]?\s*(\d{3,6})\b", re.IGNORECASE),
    re.compile(r"\bHTTP\s*(\d{3})\b", re.IGNORECASE),
]

# Hostname conventions seen in this environment, matched conservatively.
_HOSTNAME_PATTERNS = [
    re.compile(r"\b[A-Z]{2,}\d-[A-Za-z]{2,}[A-Za-z0-9-]*\b"),    # bos1-vdi-143 -> partial
    re.compile(r"\b[a-z0-9]{2,}-[a-z]{2,3}-[a-z]?-?\d{2,4}\b"),  # bos1-vdi-143
    re.compile(r"\bUPS-[A-Za-z]+\d*\b"),                         # UPS-Boston, UPS-Phoenix2
    re.compile(r"\b[A-Za-z0-9-]+\.(?:weller\.corp|wellertruck\.com|local)\b", re.IGNORECASE),
]

# Small, high-signal application dictionary. Extend freely; misses are harmless.
_APP_DICTIONARY = [
    "outlook", "teams", "excel", "word", "onedrive", "sharepoint", "chrome",
    "edge", "firefox", "vpn", "anyconnect", "worldship", "horizon", "citrix",
    "ringcentral", "sentinelone", "sophos", "acrobat", "adobe", "java",
    "sage", "quickbooks", "engage", "epicor", "print", "printer", "scanner",
    "bitlocker", "onedrive", "exchange", "m365", "office", "windows",
]


def _dedupe_ci(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in values:
        key = v.strip().lower()
        if key and key not in seen:
            seen.add(key)
            out.append(v.strip())
    return out


def extract_error_codes(*texts: str) -> List[str]:
    found: List[str] = []
    for text in texts:
        if not text:
            continue
        for pattern in _ERROR_CODE_PATTERNS:
            for match in pattern.finditer(text):
                # use the captured group when the pattern has one, else the whole match
                token = match.group(1) if match.groups() else match.group(0)
                found.append(token)
    return _dedupe_ci(found)


def extract_hostnames(*texts: str) -> List[str]:
    found: List[str] = []
    for text in texts:
        if not text:
            continue
        for pattern in _HOSTNAME_PATTERNS:
            for match in pattern.finditer(text):
                found.append(match.group(0))
    return _dedupe_ci(found)


def extract_apps(*texts: str) -> List[str]:
    haystack = " ".join(t for t in texts if t).lower()
    if not haystack:
        return []
    return [app for app in _APP_DICTIONARY if app in haystack]


def enrich(*texts: str) -> dict:
    """Return the three extracted entity lists in one call."""

    return {
        "error_codes": extract_error_codes(*texts),
        "hostnames": extract_hostnames(*texts),
        "apps": _dedupe_ci(extract_apps(*texts)),
    }
