#!/usr/bin/env python3
"""Autonomous scheduler: keep the Azure AI Search KB fresh.

Runs an incremental index every INTERVAL_HOURS (default 4), plus a reconcile
once a day. A plain loop is deliberate - no cron/systemd dependency, so it works
inside any container.

  docker run ... python scripts/scheduler.py
  INTERVAL_HOURS=4 RECONCILE_HOUR_UTC=3 python scripts/scheduler.py

Failures are logged and retried on the next tick rather than killing the loop,
so one bad run cannot take the updater permanently offline.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

from fskb.config import Settings
from fskb.pipeline import reconcile, run_full


def _log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat()} {msg}", flush=True)


def main() -> int:
    interval_hours = float(os.environ.get("INTERVAL_HOURS", "4"))
    reconcile_hour = int(os.environ.get("RECONCILE_HOUR_UTC", "3"))
    run_at_start = os.environ.get("RUN_AT_START", "true").lower() in {"1", "true", "yes"}
    interval_s = max(60.0, interval_hours * 3600.0)

    settings = Settings.from_env()
    _log(f"scheduler starting: interval={interval_hours}h reconcile_hour_utc={reconcile_hour} run_at_start={run_at_start}")

    last_reconcile_day = None
    if not run_at_start:
        time.sleep(interval_s)

    while True:
        tick = datetime.now(timezone.utc)
        try:
            _log("run: incremental index")
            result = run_full(settings, progress=_log)
            _log(
                f"run: done built={result.records_built} uploaded={result.upload_ok} "
                f"failed={result.upload_failed} dropped={result.records_dropped}"
            )
            if result.upload_failed:
                _log(f"run: WARNING {result.upload_failed} document(s) failed to upload")
        except Exception as exc:  # keep the loop alive
            _log(f"run: FAILED {type(exc).__name__}: {exc}")

        if reconcile_hour >= 0 and tick.hour >= reconcile_hour and last_reconcile_day != tick.date():
            try:
                _log("run: nightly reconcile")
                stale = reconcile(settings, progress=_log)
                _log(f"run: reconcile removed={stale}")
                last_reconcile_day = tick.date()
            except Exception as exc:
                _log(f"run: reconcile FAILED {type(exc).__name__}: {exc}")

        _log(f"sleep {interval_hours}h until next run")
        time.sleep(interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
