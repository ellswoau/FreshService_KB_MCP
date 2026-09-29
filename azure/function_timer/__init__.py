"""Azure Function (timer trigger): incremental KB index every 6 hours.

Deploy the package under src/ alongside this function, or install it as a
dependency. All configuration comes from app settings / environment variables
(see ../../.env.example). Use managed identity or Key Vault references for
secrets in production.

To also reconcile nightly, add a second timer function that calls
``run(mode="reconcile")`` with schedule "0 0 3 * * *".
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Make the package importable when deployed without an install step.
_SRC = Path(__file__).resolve().parents[2] / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from fskb.config import Settings  # noqa: E402
from fskb.pipeline import reconcile, run_full  # noqa: E402


def run(mode: str = "incremental") -> None:
    settings = Settings.from_env()
    if mode in {"incremental", "both"}:
        result = run_full(settings, progress=lambda m: logging.info(m))
        logging.info(
            "indexed built=%s uploaded=%s failed=%s",
            result.records_built,
            result.upload_ok,
            result.upload_failed,
        )
        if result.upload_failed:
            raise RuntimeError(f"{result.upload_failed} document(s) failed to upload")
    if mode in {"reconcile", "both"}:
        stale = reconcile(settings, progress=lambda m: logging.info(m))
        logging.info("reconcile removed=%s", stale)


def main(myTimer) -> None:  # noqa: ANN001 - provided by the Functions runtime
    if not myTimer.past_due:
        logging.info("timer is not past due; running incremental index")
    run("incremental")
