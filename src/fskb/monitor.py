"""Correlated-ticket open-rate monitor (Phase 1: live cluster counter).

Watches tickets as they are *created* in FreshService. When ``>= min_count``
tickets share a cluster key (derived from ``description_text`` by
:mod:`fskb.clusters`) within a sliding window, it raises an alert: it leaves a
**private note on the first (earliest) ticket of the cluster** and calls a
pluggable report hook. It never merges tickets and never writes any other field.

Design notes
------------
* Detection is on description text, not FreshService category/sub_category.
* The live window is in-process memory and re-hydratable from FreshService, so a
  restart rebuilds it in one lookback poll. Only alerts, cooldowns, feedback and
  (Phase 2) the baseline aggregate persist -- in a small SQLite file.
* Phase 1 does not gate on a baseline; Phase 2 adds ``baseline_ok``. The hook is
  already here so Phase 3 corroborators slot in without restructuring.

State (SQLite, WAL, this process is the sole writer):

    meta(k, v)
    baseline(key, dow, hour, n, median, mad, updated_at, PK(key, dow, hour))
    alert(id, key, first_ticket_id, opened_at, count, status, cooldown_until)
    feedback(alert_id, verdict, agent, note, at)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import statistics
import sys
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

from .clusters import label_for, match_keys, primary_cluster_key, system_key
from .config import Settings
from .corroborators import CorroborationResult, GraylogClient, MCPChangesClient, corroborate, load_catalog
from .mcp_client import MCPClient

# A report hook receives (alert_id, cluster_key, tickets, note_text) and returns
# nothing. Default is a no-op: Phase 1 leaves the report channel pluggable and
# only writes the private note.
ReportHook = Callable[[int, str, List["Ticket"], str], None]

ISO = "%Y-%m-%dT%H:%M:%SZ"


# --------------------------------------------------------------------------- #
# time helpers
# --------------------------------------------------------------------------- #
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Any) -> Optional[datetime]:
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
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(ISO)


# --------------------------------------------------------------------------- #
# ticket view
# --------------------------------------------------------------------------- #
@dataclass
class Ticket:
    id: int
    created_at: datetime
    description_text: str = ""
    subject: str = ""
    category: Optional[str] = None
    sub_category: Optional[str] = None
    status: Optional[int] = None
    cluster_keys: List[str] = field(default_factory=list)

    @property
    def system_key(self) -> str:
        return system_key(self.category, self.sub_category)

    @property
    def primary_key(self) -> Optional[str]:
        return self.cluster_keys[0] if self.cluster_keys else None


def parse_ticket(raw: Dict[str, Any], rules_text: Optional[str] = None) -> Optional[Ticket]:
    """Normalize a raw FreshService ticket. Returns None when unusable.

    Detection text is ``description_text`` (falls back to the HTML
    ``description``), optionally concatenated with ``subject`` via
    ``rules_text`` -- callers pass ``f"{subject}\n{description_text}"``.
    """

    tid = raw.get("id")
    created = parse_dt(raw.get("created_at"))
    if tid is None or created is None:
        return None
    text = rules_text if rules_text is not None else (
        raw.get("description_text") or raw.get("description") or ""
    )
    return Ticket(
        id=int(tid),
        created_at=created,
        description_text=str(raw.get("description_text") or raw.get("description") or ""),
        subject=str(raw.get("subject") or ""),
        category=raw.get("category"),
        sub_category=raw.get("sub_category"),
        status=raw.get("status"),
        cluster_keys=match_keys(text),
    )


# --------------------------------------------------------------------------- #
# live window (in-process)
# --------------------------------------------------------------------------- #
class LiveWindow:
    """Sliding window of tickets per cluster key. Not persisted."""

    def __init__(self, window_minutes: int = 60):
        self.window = timedelta(minutes=window_minutes)
        self._by_key: Dict[str, Dict[int, Ticket]] = defaultdict(dict)

    def add(self, ticket: Ticket) -> None:
        for key in ticket.cluster_keys:
            self._by_key[key][ticket.id] = ticket

    def evict(self, now: datetime) -> None:
        cutoff = now - self.window
        for key in list(self._by_key):
            bucket = self._by_key[key]
            for tid in [t for t, tk in bucket.items() if tk.created_at < cutoff]:
                del bucket[tid]
            if not bucket:
                del self._by_key[key]

    def tickets(self, key: str, now: Optional[datetime] = None) -> List[Ticket]:
        bucket = self._by_key.get(key, {})
        items = list(bucket.values())
        if now is not None:
            cutoff = now - self.window
            items = [t for t in items if t.created_at >= cutoff]
        items.sort(key=lambda t: (t.created_at, t.id))
        return items

    def clusters(self, now: datetime, min_count: int) -> Dict[str, List[Ticket]]:
        out: Dict[str, List[Ticket]] = {}
        for key in list(self._by_key):
            tickets = self.tickets(key, now)
            if len(tickets) >= min_count:
                out[key] = tickets
        return out

    def keys(self) -> List[str]:
        return sorted(self._by_key)


# --------------------------------------------------------------------------- #
# SQLite store (sole writer)
# --------------------------------------------------------------------------- #
_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS baseline (
    key TEXT NOT NULL, dow INTEGER NOT NULL, hour INTEGER NOT NULL,
    n INTEGER NOT NULL DEFAULT 0, median REAL, mad REAL, updated_at TEXT,
    PRIMARY KEY (key, dow, hour)
);
CREATE TABLE IF NOT EXISTS alert (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    first_ticket_id INTEGER,
    opened_at TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    cooldown_until TEXT
);
CREATE INDEX IF NOT EXISTS idx_alert_key_opened ON alert(key, opened_at);
CREATE TABLE IF NOT EXISTS feedback (
    alert_id INTEGER, verdict TEXT, agent TEXT, note TEXT, at TEXT
);
"""


class ClusterStore:
    def __init__(self, path: "Path | str"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), timeout=15)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.executescript(_SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        """Additive migrations for pre-existing DB files (evidence column)."""

        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(alert)")}
        if "evidence" not in cols:
            self.conn.execute("ALTER TABLE alert ADD COLUMN evidence TEXT")

    # --- meta ---
    def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        return row["v"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        self.conn.commit()

    def heartbeat(self, at: datetime) -> None:
        self.set_meta("last_poll_at", iso(at))

    def read_meta_readonly(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Read one meta value on a short-lived READ-ONLY connection.

        The health endpoint runs on a different thread than the poll loop, and
        a sqlite3 connection may only be used from the thread that created it.
        A separate read-only connection keeps the health probe independent (and
        cannot block the writer, thanks to WAL).
        """

        try:
            con = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=5)
            try:
                row = con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
                return row[0] if row else default
            finally:
                con.close()
        except Exception:
            return default

    # --- baseline (Phase 2) ---
    def get_baseline(self, key: str, dow: int, hour: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM baseline WHERE key=? AND dow=? AND hour=?", (key, dow, hour)
        ).fetchone()

    def replace_baseline(self, rows: Sequence[tuple], updated_at: str) -> None:
        """Atomically swap the whole baseline table and stamp the run time."""

        with self.conn:
            self.conn.execute("DELETE FROM baseline")
            self.conn.executemany(
                "INSERT INTO baseline(key, dow, hour, n, median, mad, updated_at)"
                " VALUES(?,?,?,?,?,?,?)",
                [(k, d, h, n, med, mad, updated_at) for (k, d, h, n, med, mad) in rows],
            )

    def all_baseline(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM baseline ORDER BY key, dow, hour"
        ).fetchall()

    # --- alerts / cooldown ---
    def in_cooldown(self, key: str, now: datetime) -> Optional[sqlite3.Row]:
        row = self.conn.execute(
            "SELECT * FROM alert WHERE key=? AND cooldown_until IS NOT NULL "
            "AND cooldown_until > ? ORDER BY id DESC LIMIT 1",
            (key, iso(now)),
        ).fetchone()
        return row

    def latest_alert(self, key: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM alert WHERE key=? ORDER BY id DESC LIMIT 1", (key,)
        ).fetchone()

    def record_alert(
        self, key: str, first_ticket_id: int, opened_at: datetime, count: int,
        cooldown_until: datetime, evidence: Optional[str] = None,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO alert(key, first_ticket_id, opened_at, count, status, cooldown_until, evidence)"
            " VALUES(?,?,?,?, 'open', ?, ?)",
            (key, first_ticket_id, iso(opened_at), count, iso(cooldown_until), evidence),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def open_alerts(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM alert WHERE status='open' ORDER BY id DESC"
        ).fetchall()

    def set_status(self, alert_id: int, status: str) -> None:
        self.conn.execute("UPDATE alert SET status=? WHERE id=?", (status, alert_id))
        self.conn.commit()

    # --- feedback ---
    def record_feedback(self, alert_id: int, verdict: str, agent: Optional[str], note: Optional[str]) -> None:
        self.conn.execute(
            "INSERT INTO feedback(alert_id, verdict, agent, note, at) VALUES(?,?,?,?,?)",
            (alert_id, verdict, agent, note, iso(utcnow())),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


# --------------------------------------------------------------------------- #
# monitor
# --------------------------------------------------------------------------- #
@dataclass
class MonitorConfig:
    db_path: str = "monitor.sqlite"
    window_minutes: int = 60
    min_count: int = 3
    cooldown_minutes: int = 120
    interval_seconds: int = 300
    lookback_minutes: int = 180
    health_port: int = 8016
    dry_run: bool = False
    max_per_poll: int = 500
    # Phase 2 baseline gate
    baseline_enabled: bool = True
    baseline_weeks: int = 8
    baseline_tz: str = "America/Detroit"
    baseline_interval_hours: int = 24
    baseline_sigma: float = 3.0
    # Phase 3 corroborators
    corroborate_enabled: bool = True
    corroborate_cause_lookback_hours: int = 24
    corroborate_effect_lookback_hours: int = 1
    corroborate_limit: int = 8
    graylog_url: Optional[str] = None
    graylog_api_token: Optional[str] = None
    graylog_verify_ssl: bool = True
    horizon_enabled: bool = True
    horizon_mcp_url: Optional[str] = None
    horizon_mcp_auth_token: Optional[str] = None
    horizon_mcp_verify_ssl: bool = True
    change_enabled: bool = True
    change_lookback_hours: int = 72
    saas_enabled: bool = True
    fs_mcp_url: Optional[str] = None
    fs_mcp_auth_token: Optional[str] = None
    use_fs_mcp_changes: bool = True

    @classmethod
    def from_settings(cls, settings: Settings) -> "MonitorConfig":
        return cls(
            db_path=settings.monitor_db_path,
            window_minutes=settings.monitor_window_minutes,
            min_count=settings.monitor_min_count,
            cooldown_minutes=settings.monitor_cooldown_minutes,
            interval_seconds=settings.monitor_interval_seconds,
            lookback_minutes=settings.monitor_lookback_minutes,
            health_port=settings.monitor_health_port,
            dry_run=settings.monitor_dry_run,
            max_per_poll=settings.monitor_max_per_poll,
            baseline_enabled=settings.monitor_baseline_enabled,
            baseline_weeks=settings.monitor_baseline_weeks,
            baseline_tz=settings.monitor_baseline_tz,
            baseline_interval_hours=settings.monitor_baseline_interval_hours,
            baseline_sigma=settings.monitor_baseline_sigma,
            corroborate_enabled=settings.monitor_corroborate_enabled,
            corroborate_cause_lookback_hours=settings.monitor_corroborate_cause_lookback_hours,
            corroborate_effect_lookback_hours=settings.monitor_corroborate_effect_lookback_hours,
            corroborate_limit=settings.monitor_corroborate_limit,
            graylog_url=settings.graylog_url,
            graylog_api_token=settings.graylog_api_token,
            graylog_verify_ssl=settings.graylog_verify_ssl,
            horizon_enabled=settings.monitor_horizon_enabled,
            horizon_mcp_url=settings.horizon_mcp_url,
            horizon_mcp_auth_token=settings.horizon_mcp_auth_token,
            horizon_mcp_verify_ssl=settings.horizon_mcp_verify_ssl,
            change_enabled=settings.monitor_change_enabled,
            change_lookback_hours=settings.monitor_change_lookback_hours,
            saas_enabled=settings.monitor_saas_enabled,
            fs_mcp_url=settings.freshservice_mcp_url,
            fs_mcp_auth_token=settings.freshservice_mcp_auth_token,
            use_fs_mcp_changes=settings.monitor_use_fs_mcp_changes,
        )


class CorrelatedMonitor:
    def __init__(
        self,
        config: MonitorConfig,
        client: Any = None,
        store: Optional[ClusterStore] = None,
        report_hook: Optional[ReportHook] = None,
        emit: Callable[[str], None] = lambda m: print(m, file=sys.stderr),
    ):
        self.cfg = config
        self.client = client
        self.store = store or ClusterStore(config.db_path)
        self.window = LiveWindow(config.window_minutes)
        self.report_hook = report_hook or (lambda *a, **k: None)
        self.emit = emit
        self.catalog = load_catalog()
        self.graylog: Optional[GraylogClient] = None
        if config.corroborate_enabled and config.graylog_url and config.graylog_api_token:
            self.graylog = GraylogClient(
                config.graylog_url, config.graylog_api_token, config.graylog_verify_ssl
            )
        self.horizon_mcp: Optional[MCPClient] = None
        if config.horizon_enabled and config.horizon_mcp_url and config.horizon_mcp_auth_token:
            self.horizon_mcp = MCPClient(
                config.horizon_mcp_url, config.horizon_mcp_auth_token, config.horizon_mcp_verify_ssl
            )
        # Change feed: prefer the FreshService MCP (humanised labels); fall back
        # to the direct FreshService client (self.client) otherwise.
        self.changes_client: Any = None
        if config.use_fs_mcp_changes and config.fs_mcp_url and config.fs_mcp_auth_token:
            self.changes_client = MCPChangesClient(
                MCPClient(config.fs_mcp_url, config.fs_mcp_auth_token)
            )

    # --- ingestion ---
    def ingest(self, raw: Dict[str, Any]) -> Optional[Ticket]:
        text = f"{raw.get('subject') or ''}\n{raw.get('description_text') or raw.get('description') or ''}"
        ticket = parse_ticket(raw, rules_text=text)
        if ticket is not None:
            self.window.add(ticket)
        return ticket

    # --- one poll ---
    def poll_once(self, now: Optional[datetime] = None) -> List[int]:
        now = now or utcnow()
        since = self.store.get_meta("created_watermark") or iso(now - timedelta(minutes=self.cfg.lookback_minutes))
        raw_tickets: List[Dict[str, Any]] = []
        if self.client is not None:
            raw_tickets = list(
                self.client.list_tickets(
                    created_since=since, order_by="created_at", order_type="asc", limit=self.cfg.max_per_poll
                )
            )
        newest: Optional[datetime] = None
        for raw in raw_tickets:
            self.ingest(raw)
            created = parse_dt(raw.get("created_at"))
            if created and (newest is None or created > newest):
                newest = created
        self.window.evict(now)
        if newest is not None:
            # Re-pull with a small overlap so a boundary ticket cannot be missed;
            # the window and alert cooldown make reprocessing idempotent.
            self.store.set_meta("created_watermark", iso(newest - timedelta(minutes=2)))
        self.store.heartbeat(now)
        alert_ids = self.evaluate(now)
        self.emit(
            f"[poll] now={iso(now)} fetched={len(raw_tickets)} "
            f"keys={len(self.window.keys())} alerts={alert_ids}"
        )
        return alert_ids

    # --- evaluation ---
    def evaluate(self, now: Optional[datetime] = None) -> List[int]:
        now = now or utcnow()
        fired: List[int] = []
        for key, tickets in self.window.clusters(now, self.cfg.min_count).items():
            if self.store.in_cooldown(key, now):
                continue
            if self.cfg.baseline_enabled:
                threshold = self.baseline_threshold(tickets, now)
                if threshold is not None and len(tickets) < threshold:
                    self.emit(
                        f"[suppressed] key={key} observed={len(tickets)} < baseline "
                        f"threshold={threshold:.2f}"
                    )
                    continue
            alert_id = self.fire_alert(key, tickets, now)
            if alert_id is not None:
                fired.append(alert_id)
        return fired

    def fire_alert(self, key: str, tickets: Sequence[Ticket], now: Optional[datetime] = None) -> Optional[int]:
        now = now or utcnow()
        if self.store.in_cooldown(key, now):
            return None
        tickets = sorted(tickets, key=lambda t: (t.created_at, t.id))
        first = tickets[0]
        evidence = self._corroborate(now, tickets, key)
        note = self.compose_note(key, tickets, now, evidence=evidence)
        alert_id = self.store.record_alert(
            key, first.id, now, len(tickets), now + timedelta(minutes=self.cfg.cooldown_minutes),
            evidence=_evidence_json(evidence),
        )
        if self.cfg.dry_run:
            self.emit(f"[dry-run] alert #{alert_id} key={key} first={first.id} count={len(tickets)}")
            self.emit("[dry-run] private note that WOULD be posted:\n" + _indent(note))
        else:
            try:
                self.client.add_private_note(first.id, note)
                self.emit(f"[alert] #{alert_id} key={key} first={first.id} count={len(tickets)} note posted")
            except Exception as exc:  # never lose the alert because the note failed
                self.emit(f"[alert] #{alert_id} key={key} note post FAILED: {exc}")
        try:
            self.report_hook(alert_id, key, list(tickets), note)
        except Exception as exc:  # a hook must not break the loop
            self.emit(f"[alert] #{alert_id} report hook error: {exc}")
        return alert_id

    # --- baseline (Phase 2) ---
    def baseline_threshold(self, tickets: Sequence[Ticket], now: datetime) -> Optional[float]:
        """``max(3, median + K*sigma)`` for the cluster's system key(s).

        ``sigma = 1.4826*MAD`` (robust to outliers). Uses the MOST CONSERVATIVE
        (highest) threshold among the system keys the cluster touches, so a
        category whose normal volume already explains the count does not become
        news. Returns None when no baseline exists -> caller falls back to the
        Phase 1 rule (``>= min_count``).
        """

        tz = ZoneInfo(self.cfg.baseline_tz)
        local = now.astimezone(tz)
        dow, hour = local.weekday(), local.hour
        syskeys = [t.system_key for t in tickets]
        if not syskeys:
            return None
        dominant = max(set(syskeys), key=syskeys.count)
        best: Optional[float] = None
        for sk in {dominant, *syskeys}:
            row = self.store.get_baseline(sk, dow, hour)
            if not row:
                continue
            sigma = 1.4826 * float(row["mad"] or 0.0)
            thr = max(3.0, float(row["median"] or 0.0) + self.cfg.baseline_sigma * sigma)
            best = thr if best is None else max(best, thr)
        return best

    def run_baseline(self, now: Optional[datetime] = None) -> tuple:
        """Aggregate ~``baseline_weeks`` of created tickets into median+MAD rows."""

        now = now or utcnow()
        since = iso(now - timedelta(weeks=self.cfg.baseline_weeks))
        raw: List[Dict[str, Any]] = []
        if self.client is not None:
            raw = list(
                self.client.list_tickets(
                    created_since=since, order_by="created_at", order_type="desc"
                )
            )
        rows = compute_baseline_rows(raw, self.cfg.baseline_weeks, self.cfg.baseline_tz, now)
        # The baseline is derived INTERNAL state (never an outward action), so it
        # is written even in dry-run mode; dry-run only suppresses ticket notes.
        self.store.replace_baseline(rows, iso(now))
        self.store.set_meta("baseline_at", iso(now))
        self.emit(f"[baseline] {len(rows)} row(s) from {len(raw)} ticket(s) written")
        return len(rows), len(raw)

    def maybe_run_baseline(self, now: Optional[datetime] = None) -> None:
        """Run the baseline when it is missing or older than the interval."""

        now = now or utcnow()
        if not self.cfg.baseline_enabled:
            return
        last = parse_dt(self.store.get_meta("baseline_at"))
        if last is not None and (now - last) < timedelta(hours=self.cfg.baseline_interval_hours):
            return
        try:
            self.run_baseline(now)
        except Exception as exc:  # never break the loop over a baseline failure
            self.emit(f"[baseline] error: {exc}")

    # --- corroborators (Phase 3) ---
    def _corroborate(
        self, now: datetime, tickets: Optional[Sequence[Ticket]] = None, key: Optional[str] = None
    ) -> Optional[CorroborationResult]:
        if self.graylog is None and self.horizon_mcp is None and self.client is None:
            return None
        try:
            return corroborate(
                self.graylog,
                self.catalog,
                now - timedelta(minutes=self.cfg.window_minutes),
                now,
                cause_lookback_hours=self.cfg.corroborate_cause_lookback_hours,
                effect_lookback_hours=self.cfg.corroborate_effect_lookback_hours,
                limit=self.cfg.corroborate_limit,
                mcp_client=self.horizon_mcp,
                pool_hint=_pool_hint(tickets) if tickets else None,
                changes_client=(self.changes_client or self.client) if self.cfg.change_enabled else None,
                change_lookback_hours=self.cfg.change_lookback_hours,
                keywords=_cluster_keywords(key, tickets),
                check_saas=self.cfg.saas_enabled,
            )
        except Exception as exc:  # evidence is best-effort; never lose the alert
            self.emit(f"[corroborate] error: {exc}")
            return None

    # --- note text ---
    def compose_note(
        self, key: str, tickets: Sequence[Ticket], now: datetime,
        evidence: Optional[CorroborationResult] = None,
    ) -> str:
        label = label_for(key)
        syskeys = sorted({t.system_key for t in tickets})
        lines = [
            f"[correlated-ticket monitor] Possible incident cluster: {label} ({key})",
            f"{len(tickets)} ticket(s) created within {self.cfg.window_minutes} min "
            f"as of {iso(now)} (UTC).",
            "",
            "Tickets (oldest first):",
        ]
        for t in tickets:
            subj = (t.subject or "").strip().replace("\n", " ")[:90]
            lines.append(f"  - #{t.id}  {iso(t.created_at)}  {subj}")
        lines += [
            "",
            f"System key(s) seen: {', '.join(syskeys)}",
            "Detection: description-text cluster match (category/sub_category not used for detection).",
            "This is a heuristic cluster, not a confirmed incident. No tickets were merged;",
            "verify against the live systems before acting.",
        ]
        if evidence is not None:
            lines.append("")
            lines.extend(evidence.summary_lines())
        return "\n".join(lines)

    # --- synthetic replay (verification / tests) ---
    def replay(self, tickets: Iterable[Dict[str, Any]], now: Optional[datetime] = None) -> List[int]:
        tickets = list(tickets)
        for raw in tickets:
            self.ingest(raw)
        if now is None:
            # Default to just after the newest synthetic ticket so replay does
            # not depend on wall-clock time (a fixed fixture must stay in-window).
            stamps = [s for s in (parse_dt(t.get("created_at")) for t in tickets) if s]
            now = (max(stamps) + timedelta(minutes=1)) if stamps else utcnow()
        self.window.evict(now)
        return self.evaluate(now)

    # --- loop ---
    def run_forever(self) -> None:
        self.emit(
            f"[monitor] starting loop every {self.cfg.interval_seconds}s "
            f"window={self.cfg.window_minutes}min min_count={self.cfg.min_count} "
            f"cooldown={self.cfg.cooldown_minutes}min dry_run={self.cfg.dry_run}"
        )
        while True:
            try:
                self.maybe_run_baseline()
                self.poll_once()
            except Exception as exc:  # keep the daemon alive
                self.emit(f"[monitor] poll error: {exc}")
            threading.Event().wait(self.cfg.interval_seconds)


def _indent(text: str, prefix: str = "    ") -> str:
    return "\n".join(prefix + ln for ln in text.splitlines())


_POOL_HINT = re.compile(r"\b([A-Za-z]{2,}\d{1,2})-VDI-\d+", re.IGNORECASE)
_STOPWORDS = {
    "with", "from", "that", "this", "cannot", "issue", "issues", "other", "desktop",
    "account", "error", "errors", "change", "changes", "standard", "update", "updates",
    "service", "services", "store", "stores", "primary", "secondary", "emergency",
    "normal", "routine", "unknown", "cannot", "won't", "doesn't",
}


def _cluster_keywords(key: Optional[str], tickets: Optional[Sequence[Ticket]]) -> List[str]:
    """Words that identify the cluster's system, for matching against changes.

    From the cluster key + label (``app:engage`` -> engage) and the tickets'
    system key (category/sub_category). Words <4 chars and generic stopwords are
    dropped so a change is only matched on something meaningful.
    """

    sources: List[str] = []
    if key:
        sources.append(f"{key} {label_for(key)}")
    for t in tickets or []:
        sources.append(t.system_key or "")
    words = set()
    for src in sources:
        for part in re.split(r"[^a-z0-9]+", src.lower()):
            if len(part) >= 4 and part not in _STOPWORDS:
                words.add(part)
    return sorted(words)


def _pool_hint(tickets: Optional[Sequence[Ticket]]) -> Optional[str]:
    """Machine hostname in a ticket -> site code that a Horizon pool name starts with.

    e.g. ``MAN2-VDI-128`` -> ``man2``, which matches pool ``man2-vdi``. Used to
    check the corroborating effect is on the SAME population as the cluster.
    """

    for t in tickets or []:
        text = f"{t.subject or ''}\n{t.description_text or ''}"
        m = _POOL_HINT.search(text)
        if m:
            return m.group(1).lower()
    return None


def _evidence_json(evidence: Optional[CorroborationResult]) -> Optional[str]:
    if evidence is None:
        return None
    return json.dumps(
        {
            "confidence": evidence.confidence,
            "signals": [
                {"key": r.key, "role": r.role, "count": r.count,
                 "prior": r.prior_count, "elevated": r.elevated}
                for r in evidence.results
            ],
            "pools": evidence.pools(),
        }
    )


def _weeks_in_window(now: datetime, weeks: int, tzinfo) -> List[tuple]:
    """The ISO (year, week) keys covering the trailing ``weeks`` weeks."""

    local = now.astimezone(tzinfo)
    out = set()
    for i in range(weeks * 7 + 1):
        iso = (local - timedelta(days=i)).isocalendar()
        out.add((iso[0], iso[1]))
    return sorted(out)


def compute_baseline_rows(
    tickets: Iterable[Dict[str, Any]], weeks: int, tz_name: str, now: datetime
) -> List[tuple]:
    """Aggregate tickets into (key, dow, hour, n, median, mad) rows.

    For each system key and each (day-of-week, hour) slot in LOCAL time, the
    series is the per-week CREATE count; median and MAD are taken over that
    series (0-filled for weeks with no ticket in the slot, so the typical rate
    is not inflated).
    """

    tzinfo = ZoneInfo(tz_name)
    per_slot: Dict[tuple, Dict[tuple, int]] = defaultdict(lambda: defaultdict(int))
    for raw in tickets:
        created = parse_dt(raw.get("created_at"))
        if created is None:
            continue
        local = created.astimezone(tzinfo)
        key = system_key(raw.get("category"), raw.get("sub_category"))
        iso = local.isocalendar()
        per_slot[(key, local.weekday(), local.hour)][(iso[0], iso[1])] += 1
    weekset = _weeks_in_window(now, weeks, tzinfo)
    rows: List[tuple] = []
    for (key, dow, hour), counts in per_slot.items():
        values = [counts.get(w, 0) for w in weekset]
        med = float(statistics.median(values)) if values else 0.0
        mad = float(statistics.median([abs(v - med) for v in values])) if values else 0.0
        rows.append((key, dow, hour, len(values), med, mad))
    return rows


# --------------------------------------------------------------------------- #
# health endpoint (stdlib; credential-free, mirrors the house /health)
# --------------------------------------------------------------------------- #
def start_health_server(monitor: CorrelatedMonitor, host: str = "0.0.0.0", port: int = 8016):
    if port <= 0:
        return None
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path.split("?")[0] not in ("/health", "/healthz"):
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps(
                {
                    "status": "ok",
                    "service": "freshservice-kb-monitor",
                    "last_poll_at": monitor.store.read_meta_readonly("last_poll_at"),
                    "window_keys": monitor.window.keys(),
                    "dry_run": monitor.cfg.dry_run,
                    "corroborate": monitor.graylog is not None,
                    "horizon_mcp": monitor.horizon_mcp is not None,
                }
            ).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):  # silence access logs
            return

    server = ThreadingHTTPServer((host, port), _Handler)
    thread = threading.Thread(target=server.serve_forever, name="monitor-health", daemon=True)
    thread.start()
    return server


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _build_monitor(args: argparse.Namespace) -> CorrelatedMonitor:
    settings = Settings.from_env()
    cfg = MonitorConfig.from_settings(settings)
    if getattr(args, "db", None):
        cfg.db_path = args.db
    if getattr(args, "window", None):
        cfg.window_minutes = args.window
    if getattr(args, "min_count", None):
        cfg.min_count = args.min_count
    if getattr(args, "cooldown", None):
        cfg.cooldown_minutes = args.cooldown
    if getattr(args, "interval", None):
        cfg.interval_seconds = args.interval
    if getattr(args, "health_port", None) is not None:
        cfg.health_port = args.health_port
    if getattr(args, "dry_run", False):
        cfg.dry_run = True
    client = None
    try:
        settings.require_freshservice()
        from .freshservice import FreshServiceClient

        client = FreshServiceClient(settings)
    except Exception as exc:
        print(f"monitor: no FreshService client ({exc}); replay-only", file=sys.stderr)
    return CorrelatedMonitor(cfg, client=client)


def cmd_monitor(args: argparse.Namespace) -> int:
    monitor = _build_monitor(args)

    if args.replay_jsonl:
        path = Path(args.replay_jsonl)
        tickets = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        now = parse_dt(args.now) if args.now else utcnow()
        fired = monitor.replay(tickets, now=now)
        print(f"replay: {len(tickets)} ticket(s) -> {len(fired)} alert(s) {fired}")
        return 0

    if args.once:
        fired = monitor.poll_once()
        print(f"once: {len(fired)} alert(s) {fired}")
        return 0

    start_health_server(monitor, port=monitor.cfg.health_port)
    monitor.run_forever()
    return 0


def cmd_baseline(args: argparse.Namespace) -> int:
    """Compute (and store) the open-rate baseline once, then exit."""

    monitor = _build_monitor(args)
    rows, tickets = monitor.run_baseline()
    print(f"baseline: {rows} row(s) from {tickets} ticket(s)")
    return 0


def add_monitor_subparser(sub) -> None:  # type: ignore[no-untyped-def]
    p = sub.add_parser("monitor", help="correlated-ticket open-rate monitor (live cluster counter)")
    p.add_argument("--once", action="store_true", help="single poll, then exit")
    p.add_argument("--loop", action="store_true", help="run the polling loop (default when neither --once nor --replay-jsonl)")
    p.add_argument("--dry-run", action="store_true", help="compose alerts/notes but do not post")
    p.add_argument("--replay-jsonl", default=None, help="feed synthetic tickets from JSONL (offline verification)")
    p.add_argument("--now", default=None, help="ISO timestamp to treat as 'now' for replay")
    p.add_argument("--db", default=None, help="SQLite path (default MONITOR_DB_PATH)")
    p.add_argument("--window", type=int, default=None, help="window minutes")
    p.add_argument("--min-count", dest="min_count", type=int, default=None, help="tickets to fire a cluster")
    p.add_argument("--cooldown", type=int, default=None, help="cooldown minutes per cluster key")
    p.add_argument("--interval", type=int, default=None, help="loop interval seconds")
    p.add_argument("--health-port", dest="health_port", type=int, default=None, help="HTTP /health port (0 disables)")
    p.set_defaults(func=cmd_monitor)


def main(argv: Optional[List[str]] = None) -> int:  # pragma: no cover - thin
    from .cli import build_parser

    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


# --------------------------------------------------------------------------- #
# Phase 2/3 seams (documented, not yet active in Phase 1)
# --------------------------------------------------------------------------- #
def baseline_gate(observed: int, baseline_median: Optional[float], baseline_mad: Optional[float]) -> bool:
    """Phase 2 gate: observed >= max(3, median + 3*sigma), sigma = 1.4826*MAD.

    Exposed now so Phase 2 only has to fill the baseline table and call it.
    """

    if baseline_median is None or baseline_mad is None:
        return observed >= 3
    sigma = 1.4826 * baseline_mad
    threshold = max(3.0, baseline_median + 3.0 * sigma)
    return observed >= threshold
