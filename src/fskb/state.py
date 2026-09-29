"""Incremental watermark state, kept in a small JSON file.

The whole incremental contract is: remember the newest ``updated_at`` we have
successfully processed, and ask FreshService for tickets changed after it.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class State:
    path: Path
    last_updated_at: Optional[str] = None
    last_run_at: Optional[str] = None
    last_indexed_count: int = 0

    @classmethod
    def load(cls, state_dir: Path) -> "State":
        path = Path(state_dir) / "state.json"
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                raw = {}
            return cls(
                path=path,
                last_updated_at=raw.get("last_updated_at"),
                last_run_at=raw.get("last_run_at"),
                last_indexed_count=int(raw.get("last_indexed_count", 0)),
            )
        return cls(path=path)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_updated_at": self.last_updated_at,
            "last_run_at": self.last_run_at,
            "last_indexed_count": self.last_indexed_count,
        }
        # atomic write
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
