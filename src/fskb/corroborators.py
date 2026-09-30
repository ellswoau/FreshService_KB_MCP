"""Corroborators (Phase 3): independent-system evidence for a ticket cluster.

The field-exact query catalog lives in the ``corroborators`` skill
(``workshop-skills/corroborators/references/corroborators.md``) and is mirrored
as machine-readable data in ``fskb/data/corroborators.json`` -- the SINGLE source
of truth this module reads. The skill stays the agent's reasoning playbook; this
module only runs the deterministic queries and scores the evidence (cause vs
effect, elevation, confidence tier).

Nothing here is LLM-facing: it is plain HTTP against Graylog's REST API.
"""

from __future__ import annotations

import base64
import json
import ssl
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .mcp_client import MCPClient

UTC = timezone.utc

_DATA = Path(__file__).resolve().parent / "data" / "corroborators.json"

# Confidence tiers, aligned with the corroborators skill's table.
CONF_LIKELY = "likely"
CONF_CONSISTENT = "consistent with"
CONF_NONE = "none"


def load_catalog(path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the shared corroborator catalog (machine-readable)."""

    p = Path(path) if path else _DATA
    with p.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _gl_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class GraylogClient:
    """Minimal Graylog REST client.

    Graylog 6/7 API-token auth is HTTP Basic with the token as BOTH the username
    and the literal password ``token`` (``base64("<token>:token")``); the token
    alone / ``Bearer`` both 401. Verified against graylog.weller.corp (7.1.8).
    """

    def __init__(self, url: str, token: str, verify_ssl: bool = True, timeout: int = 30):
        self.url = url.rstrip("/")
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        cred = base64.b64encode(f"{self.token}:token".encode()).decode()
        return {
            "Authorization": f"Basic {cred}",
            "Accept": "application/json",
            "X-Requested-By": "fskb-monitor",
        }

    def _ssl_ctx(self):  # noqa: ANN202
        if self.verify_ssl:
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def search(self, stream_id: str, query: str, from_: datetime, to: datetime, limit: int = 10) -> Dict[str, Any]:
        """Run an absolute-window search restricted to one stream."""

        params = urllib.parse.urlencode(
            {
                "query": query,
                "from": _gl_ts(from_),
                "to": _gl_ts(to),
                "limit": limit,
                "filter": f"streams:{stream_id}",
            }
        )
        req = urllib.request.Request(
            f"{self.url}/api/search/universal/absolute?{params}", headers=self._headers()
        )
        with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_ctx()) as resp:
            return json.load(resp)


@dataclass
class SignalResult:
    key: str
    role: str  # cause | effect
    stream: str
    query: str
    count: int
    prior_count: int = 0
    samples: List[Dict[str, Any]] = field(default_factory=list)
    note: str = ""
    mode: str = "presence"  # "presence" (low base rate) | "elevation" (high volume)

    @property
    def elevated(self) -> bool:
        """Whether this signal actually corroborates.

        ``presence``: any hit matters (a low-base-rate cause, or a pool sitting
        in an ERROR state when it is normally 0). ``elevation``: a high-volume
        effect is noise on presence alone, so require a rise vs its own prior
        equal window.
        """

        if self.mode == "presence":
            return self.count > 0
        return self.count >= 3 and self.count >= max(3, 2 * self.prior_count)


@dataclass
class CorroborationResult:
    results: List[SignalResult] = field(default_factory=list)
    confidence: str = CONF_NONE
    pool_hint: Optional[str] = None

    def by_role(self, role: str) -> List[SignalResult]:
        return [r for r in self.results if r.role == role]

    def pools(self, top: int = 3) -> List[str]:
        counts: Dict[str, int] = {}
        for r in self.results:
            for s in r.samples:
                name = s.get("desktop") or s.get("pool")
                if name:
                    counts[name] = counts.get(name, 0) + 1
        return [k for k, _ in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:top]]

    def summary_lines(self) -> List[str]:
        lines = [f"Corroborators -- {self.confidence}:"]
        for r in self.results:
            unit = "event" if r.count == 1 else "events"
            extra = f" (prior window {r.prior_count})" if r.role == "effect" else ""
            flag = " *" if r.elevated and r.role == "effect" else ""
            lines.append(
                f"  - [{r.role}] {r.key} @ {r.stream}: {r.count} {unit}{extra}{flag}"
            )
        pools = self.pools()
        if pools:
            lines.append(f"  pools seen: {', '.join(pools)}")
        if self.pool_hint:
            hint = self.pool_hint.lower()
            aligned = [p for p in pools if hint in (p or "").lower()]
            lines.append(
                f"  cluster machine hint '{self.pool_hint}' -> \
"
                f"aligned pool(s): {', '.join(aligned) if aligned else 'none matched'}"
            )
        lines.append("  (independent systems; cause window widened, clocks aligned to UTC)")
        return lines


def _sample(entry: Dict[str, Any], signal: Dict[str, Any], catalog: Dict[str, Any]) -> Dict[str, Any]:
    msg = entry.get("message", entry) or {}
    fields = catalog.get("fields", {}).get(signal["stream"], {})
    out: Dict[str, Any] = {
        "timestamp": msg.get("timestamp") or entry.get("timestamp"),
        "source": msg.get(fields.get("source", "source")),
        "event": msg.get(signal.get("event_field", "event")),
    }
    for f in signal.get("display_fields", []):
        key = f.split("@")[-1].split("_")[-1].lower()
        out[key] = msg.get(f)
    # Convenience: expose the desktop pool under a stable name for scoring.
    desktop = msg.get(fields.get("desktop", "View@6876_DesktopDisplayName"))
    if desktop:
        out["desktop"] = desktop
    return out


def corroborate_horizon_pools(mcp_client: MCPClient, pool_hint: Optional[str] = None) -> SignalResult:
    """Pool cloning/ERROR counts from Horizon via ``horizon-mcp`` (effect).

    A pool in an ERROR state when it is normally 0 is meaningful on PRESENCE, so
    this signal uses ``mode='presence'`` (unlike the high-volume Graylog effect).
    """

    status = mcp_client.call_tool("desktop_pool_status", {})
    pools = status.get("pools", []) or []
    samples: List[Dict[str, Any]] = []
    total = 0
    for p in pools:
        ec = int(p.get("error_count") or 0)
        total += ec
        if ec:
            samples.append(
                {
                    "pool": p.get("display_name") or p.get("name"),
                    "name": p.get("name"),
                    "error_count": ec,
                }
            )
    return SignalResult(
        key="vdi.pool_errors",
        role="effect",
        stream="horizon-mcp",
        query="desktop_pool_status",
        count=total,
        samples=samples,
        mode="presence",
        note="Horizon pool cloning/ERROR state (current); normally 0.",
    )


def corroborate(
    client: GraylogClient,
    catalog: Dict[str, Any],
    window_start: datetime,
    window_end: datetime,
    cause_lookback_hours: int = 24,
    effect_lookback_hours: int = 1,
    limit: int = 8,
    mcp_client: Optional[MCPClient] = None,
    pool_hint: Optional[str] = None,
) -> CorroborationResult:
    """Run every catalog signal for a cluster window and score the evidence."""

    results: List[SignalResult] = []
    for sig in catalog.get("signals", []):
        stream_key = sig["stream"]
        stream = catalog["streams"][stream_key]
        role = sig["role"]
        lookback = cause_lookback_hours if role == "cause" else effect_lookback_hours
        frm = window_start - timedelta(hours=lookback)
        to = window_end
        query = " OR ".join(sig["any"])
        # Effect signals need a big enough sample to count pools; keep only
        # ``limit`` for the note.
        fetch = max(limit, 300) if role == "effect" else limit
        data = client.search(stream["id"], query, frm, to, limit=fetch)
        count = int(data.get("total_results", 0) or 0)
        samples = [_sample(m, sig, catalog) for m in data.get("messages", [])[:limit]]

        prior = 0
        if role == "effect":
            span = to - frm
            pdata = client.search(stream["id"], query, frm - span, frm, limit=1)
            prior = int(pdata.get("total_results", 0) or 0)

        results.append(
            SignalResult(
                key=sig["key"], role=role, stream=stream_key, query=query,
                count=count, prior_count=prior, samples=samples, note=sig.get("note", ""),
                mode="presence" if role == "cause" else "elevation",
            )
        )

    if mcp_client is not None:
        try:
            results.append(corroborate_horizon_pools(mcp_client, pool_hint))
        except Exception as exc:  # evidence is best-effort
            results.append(
                SignalResult(key="vdi.pool_errors", role="effect", stream="horizon-mcp",
                             query="desktop_pool_status", count=0, mode="presence",
                             note=f"unavailable: {exc}")
            )

    cause_hit = any(r.role == "cause" and r.elevated for r in results)
    effect_hit = any(r.role == "effect" and r.elevated for r in results)
    if cause_hit and effect_hit:
        confidence = CONF_LIKELY
    elif cause_hit or effect_hit:
        confidence = CONF_CONSISTENT
    else:
        confidence = CONF_NONE
    return CorroborationResult(results=results, confidence=confidence, pool_hint=pool_hint)
