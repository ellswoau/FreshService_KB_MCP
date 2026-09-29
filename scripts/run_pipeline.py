#!/usr/bin/env python3
"""Automation entrypoint.

Intended for a scheduler (Azure Function timer, Container Apps job, cron, or an
OpenClaw automation):

  * every 6h  -> incremental index (default)
  * nightly   -> incremental index + reconcile

Reads all configuration from the environment (see .env.example). Exits non-zero
on failure so the scheduler can alert.
"""

from __future__ import annotations

import argparse
import sys

from fskb.config import Settings
from fskb.pipeline import reconcile, run_full


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the FreshService KB pipeline.")
    parser.add_argument(
        "--mode",
        choices=["incremental", "reconcile", "both"],
        default="incremental",
        help="incremental = index changed tickets; reconcile = drop stale docs.",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    try:
        if args.mode in {"incremental", "both"}:
            result = run_full(settings, progress=lambda m: print(m, file=sys.stderr))
            print(
                f"indexed built={result.records_built} uploaded={result.upload_ok} "
                f"failed={result.upload_failed}"
            )
            if result.upload_failed:
                return 1
        if args.mode in {"reconcile", "both"}:
            stale = reconcile(settings, progress=lambda m: print(m, file=sys.stderr))
            print(f"reconcile removed={stale}")
    except Exception as exc:
        print(f"pipeline failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
