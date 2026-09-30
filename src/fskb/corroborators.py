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
import re
import ssl
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .mcp_client import MCPClient
from .sanitize import strip_html

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
            aligned: List[str] = []
            for r in self.results:
                for s in r.samples:
                    for cand in (s.get("name"), s.get("pool")):
                        if cand and hint in str(cand).lower() and cand not in aligned:
                            aligned.append(cand)
            lines.append(
                f"  cluster machine hint '{self.pool_hint}' -> "
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

    When ``pool_hint`` is given (a ticket named a machine on some pool), the
    signal is SCOPED to that pool: an ERROR burst on a *different* pool must not
    corroborate this cluster. If no pool matches the hint, the count is 0.
    """

    status = mcp_client.call_tool("desktop_pool_status", {})
    pools = status.get("pools", []) or []
    note = "Horizon pool cloning/ERROR state (current); normally 0."
    if pool_hint:
        hint = pool_hint.lower()
        pools = [
            p for p in pools
            if hint in f"{p.get('name', '')} {p.get('display_name') or ''}".lower()
        ]
        note += f" Scoped to cluster pool hint '{pool_hint}'."
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
        note=note,
    )


def _resolve_pool(mcp_client: Optional[MCPClient], pool_hint: Optional[str]) -> Optional[Dict[str, Any]]:
    """Map a site-code hint (``man2``) to a Horizon pool record (name ``man2-vdi``)."""

    if not mcp_client or not pool_hint:
        return None
    try:
        status = mcp_client.call_tool("desktop_pool_status", {})
    except Exception:
        return None
    hint = pool_hint.lower()
    for p in status.get("pools", []) or []:
        if hint in f"{p.get('name', '')} {p.get('display_name') or ''}".lower():
            return p
    return None


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class MCPChangesClient:
    """Adapts a house MCP server's ``list_changes`` to the changes-client API.

    Prefer this when available: the FreshService MCP returns humanised labels
    (status "Closed", risk "Low") the raw ``/api/v2/changes`` response lacks.
    """

    def __init__(self, mcp_client: MCPClient, per_page: int = 50):
        self.mcp = mcp_client
        self.per_page = per_page

    def list_changes(self, updated_since: Optional[str] = None, limit: Optional[int] = None):
        out: List[Dict[str, Any]] = []
        page = 1
        while True:
            res = self.mcp.call_tool(
                "list_changes",
                {"updated_since": updated_since, "page": page, "per_page": self.per_page},
            )
            changes = res.get("changes", []) if isinstance(res, dict) else []
            if not changes:
                break
            out.extend(changes)
            if limit is not None and len(out) >= limit:
                return out[:limit]
            if len(changes) < self.per_page:
                break
            page += 1
            if page > 90:
                break
        return out


def corroborate_changes(
    changes_client: Any,
    window_start: datetime,
    window_end: datetime,
    lookback_hours: int = 72,
    keywords: Optional[List[str]] = None,
    limit: int = 6,
    require_overlap: bool = True,
) -> SignalResult:
    """Recent FreshService changes = a CAUSE source (what changed, when, systems).

    Matching rules (per review):
      (a) generic tokens are dropped upstream (:func:`monitor._cluster_keywords`);
      (b) the change's PLANNED window must overlap the cluster window (a change
          that ended hours before the cluster is not a credible cause here);
      (c) ``impacted_services`` is preferred, but subject/description text is an
          accepted fallback (impacted_services is often empty).
    """

    since = window_start - timedelta(hours=lookback_hours)
    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    kws = [k.lower() for k in (keywords or []) if k]
    matched: List[Dict[str, Any]] = []
    total = 0
    for ch in changes_client.list_changes(updated_since=since_str):
        total += 1
        start = _parse_dt(ch.get("planned_start_date"))
        end = _parse_dt(ch.get("planned_end_date")) or start
        if require_overlap:
            if start is None:
                continue
            if not (start <= window_end and end >= window_start):
                continue
        if kws:
            impacted = ch.get("impacted_services") or []
            impacted_text = (
                " ".join(str(x) for x in impacted).lower()
                if isinstance(impacted, list) else str(impacted).lower()
            )
            subject_text = " ".join(
                str(ch.get(f) or "") for f in ("subject", "description_text")
            ).lower()
            if not ((impacted_text and any(k in impacted_text for k in kws))
                    or any(k in subject_text for k in kws)):
                continue
        matched.append(ch)
    samples = [
        {
            "id": c.get("id"),
            "subject": c.get("subject"),
            "status": c.get("status"),
            "risk": c.get("risk"),
            "planned_start": c.get("planned_start_date"),
            "planned_end": c.get("planned_end_date"),
            "impacted": c.get("impacted_services"),
        }
        for c in matched[:limit]
    ]
    return SignalResult(
        key="change_feed",
        role="cause",
        stream="freshservice-changes",
        query=f"changes updated_since {since_str}" + (f" overlap=[{window_start:%H:%M},{window_end:%H:%M}]" if require_overlap else ""),
        count=len(matched),
        samples=samples,
        mode="presence",
        note=f"Changes whose planned window overlaps the cluster (of {total} since {since_str}).",
    )


# --- SaaS vendor status (incidenthub.cloud status pages) -------------------
# The status banner is an <h1>/<h2>: "<Vendor> status is up" / "<Vendor> is
# experiencing issues". Match the BANNER, never the FAQ/history prose (which
# contains phrases like "is experiencing an outage?" and "reported N outages").
_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
# The status banner is an <h1>/<h2>: "<Vendor> status is up" / "<Vendor> is
# experiencing issues". Match the BANNER, never the FAQ/history prose (which
# contains phrases like "is experiencing an outage?" and "reported N outages").
# NOTE: Next.js inserts HTML comments between text nodes ("Vendor<!-- --> status
# is up"), so comments are stripped before matching.
_BANNER_RE = re.compile(
    r"<h[12][^>]*>[^<]*?"
    r"(?:status is (up|down|degraded)|is experiencing "
    r"(issues|an outage|degraded|service degradation)(?!\?))"
    r"[^<]*?</h[12]>",
    re.IGNORECASE,
)
_STATUS_LINE_RE = re.compile(r"status is (up|down|degraded)", re.IGNORECASE)
_EXP_RE = re.compile(
    r"is experiencing (issues|an outage|degraded|service degradation)(?!\?)", re.IGNORECASE
)
# "Last checked" lives in <time id="lastChecked" dateTime="...">.
_CHECKED_RE = re.compile(r"<time[^>]*dateTime=\"([^\"]+)\"", re.IGNORECASE)
_PLAIN_CHECKED_RE = re.compile(r"Last checked[:\s]*([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z)")


def _status_from_segment(segment: str) -> str:
    m = _STATUS_LINE_RE.search(segment)
    if m:
        return "up" if m.group(1).lower() == "up" else "issues"
    if _EXP_RE.search(segment):
        return "issues"
    return "unknown"


def parse_status_text(text: str, vendor: Optional[str] = None) -> Dict[str, Any]:
    """Extract {status, checked} from an incidenthub status page.

    Anchored to the status banner: prefers the ``<h1>/<h2>`` status element, and
    only falls back to the first ~400 chars (the banner region) of the page.
    Returns ``unknown`` rather than guessing when no status phrase is present.
    """

    clean = _COMMENT_RE.sub("", text)
    status = "unknown"
    banner = _BANNER_RE.search(clean)
    if banner:
        status = _status_from_segment(strip_html(banner.group(0)))
    if status == "unknown":
        status = _status_from_segment(strip_html(clean)[:400])
    c = _CHECKED_RE.search(clean) or _PLAIN_CHECKED_RE.search(clean)
    return {"status": status, "checked": c.group(1) if c else None}


def _http_get_text(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "fskb-monitor/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def corroborate_saas_status(catalog: Dict[str, Any], timeout: int = 20) -> SignalResult:
    """Check SaaS vendor status pages; a vendor NOT 'up' is a cause signal.

    Scrapes the public status page text (no API key) -- per review.
    """

    pages = catalog.get("saas_status", []) or []
    samples: List[Dict[str, Any]] = []
    down = 0
    for page in pages:
        try:
            text = _http_get_text(page["url"], timeout=timeout)
            st = parse_status_text(text)
        except Exception as exc:
            samples.append({"vendor": page.get("vendor"), "status": "unreachable", "error": str(exc)[:80]})
            continue
        if st["status"] != "up":
            down += 1
        samples.append({"vendor": page.get("vendor"), "status": st["status"], "checked": st.get("checked")})
    return SignalResult(
        key="saas_status",
        role="cause",
        stream="incidenthub",
        query="status pages: " + ", ".join(p.get("vendor", "?") for p in pages),
        count=down,
        samples=samples,
        mode="presence",
        note="SaaS vendor status (incidenthub.cloud). Only non-up vendors count.",
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
    changes_client: Any = None,
    change_lookback_hours: int = 72,
    keywords: Optional[List[str]] = None,
    check_saas: bool = False,
    saas_timeout: int = 20,
) -> CorroborationResult:
    """Run every catalog signal for a cluster window and score the evidence.

    When a machine hint resolves to a Horizon pool, the Horizon-stream queries
    are SCOPED to that pool (``View@6876_DesktopId:"<pool>"``) so a change or an
    error burst on an unrelated pool cannot corroborate this cluster.
    """

    pool = _resolve_pool(mcp_client, pool_hint)
    scope = ""
    if pool:
        did = catalog.get("fields", {}).get("horizon", {}).get("desktop_id", "View@6876_DesktopId")
        scope = f' AND {did}:"{pool["name"]}"'

    results: List[SignalResult] = []
    for sig in catalog.get("signals", []):
        stream_key = sig["stream"]
        stream = catalog["streams"][stream_key]
        role = sig["role"]
        lookback = cause_lookback_hours if role == "cause" else effect_lookback_hours
        frm = window_start - timedelta(hours=lookback)
        to = window_end
        base_query = " OR ".join(sig["any"])
        query = f"({base_query}){scope}" if (scope and stream_key == "horizon") else base_query
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

    if changes_client is not None:
        try:
            results.append(corroborate_changes(
                changes_client, window_start, window_end,
                lookback_hours=change_lookback_hours, keywords=keywords,
            ))
        except Exception as exc:  # evidence is best-effort
            results.append(
                SignalResult(key="change_feed", role="cause", stream="freshservice-changes",
                             query="changes", count=0, mode="presence", note=f"unavailable: {exc}")
            )

    if check_saas:
        try:
            results.append(corroborate_saas_status(catalog, timeout=saas_timeout))
        except Exception as exc:  # evidence is best-effort
            results.append(
                SignalResult(key="saas_status", role="cause", stream="incidenthub",
                             query="status pages", count=0, mode="presence", note=f"unavailable: {exc}")
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
