"""Environment-driven configuration.

Read once via :func:`Settings.from_env`. Everything is optional at import time so
the package (and its tests) can be imported without credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:  # dotenv is a declared dependency, but stay import-safe
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


def _load_dotenv_once() -> None:
    if load_dotenv is not None:
        load_dotenv(override=False)


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class Settings:
    """Resolved runtime configuration."""

    # FreshService
    fs_base_url: Optional[str] = None
    fs_api_key: Optional[str] = None
    fs_per_page: int = 100

    # Embedding provider: "azure" (Azure OpenAI native) or "openai" (LiteLLM /
    # any OpenAI-compatible /v1/embeddings endpoint).
    embed_provider: str = "azure"

    # OpenAI-compatible provider (LiteLLM proxy, OpenAI, etc.)
    embed_base_url: Optional[str] = None  # e.g. http://litellm:4000  (no /v1)
    embed_api_key: Optional[str] = None
    embed_model: str = "text-embedding-3-large"

    # Azure OpenAI embeddings (provider = "azure")
    aoai_endpoint: Optional[str] = None
    aoai_api_key: Optional[str] = None
    aoai_embed_deployment: str = "text-embedding-3-large"
    aoai_api_version: str = "2024-02-01"
    embed_dimensions: int = 3072

    # Azure AI Search
    search_endpoint: Optional[str] = None
    search_api_key: Optional[str] = None
    search_index_name: str = "freshservice-kb-v1"
    search_api_version: str = "2024-07-01"

    # Pipeline behaviour
    state_dir: Path = field(default_factory=lambda: Path(".state"))
    batch_size: int = 100
    embed_batch_size: int = 16
    drop_secret_only: bool = True
    min_symptom_chars: int = 15

    # Correlated-ticket monitor (Phase 1)
    monitor_db_path: str = "monitor.sqlite"
    monitor_window_minutes: int = 60
    monitor_min_count: int = 3
    monitor_cooldown_minutes: int = 120
    monitor_interval_seconds: int = 300
    monitor_lookback_minutes: int = 180
    monitor_health_port: int = 8016
    monitor_dry_run: bool = False
    monitor_max_per_poll: int = 500

    # Correlated-ticket monitor - Phase 2 baseline gate
    monitor_baseline_enabled: bool = True
    monitor_baseline_weeks: int = 8
    monitor_baseline_tz: str = "America/Detroit"
    monitor_baseline_interval_hours: int = 24
    monitor_baseline_sigma: float = 3.0

    # Corroborators (Phase 3) - independent-system evidence via Graylog REST
    graylog_url: Optional[str] = None
    graylog_api_token: Optional[str] = None
    graylog_verify_ssl: bool = True
    monitor_corroborate_enabled: bool = True
    monitor_corroborate_cause_lookback_hours: int = 24
    monitor_corroborate_effect_lookback_hours: int = 1
    monitor_corroborate_limit: int = 8

    @classmethod
    def from_env(cls) -> "Settings":
        _load_dotenv_once()
        return cls(
            fs_base_url=(_env("FS_BASE_URL") or "").rstrip("/") or None,
            fs_api_key=_env("FS_API_KEY"),
            fs_per_page=_env_int("FS_PER_PAGE", 100),
            embed_provider=(_env("EMBED_PROVIDER", "azure") or "azure").strip().lower(),
            embed_base_url=(_env("EMBED_BASE_URL") or "").rstrip("/") or None,
            embed_api_key=_env("EMBED_API_KEY"),
            embed_model=_env("EMBED_MODEL", "text-embedding-3-large"),
            aoai_endpoint=(_env("AOAI_ENDPOINT") or "").rstrip("/") or None,
            aoai_api_key=_env("AOAI_API_KEY"),
            aoai_embed_deployment=_env("AOAI_EMBED_DEPLOYMENT", "text-embedding-3-large"),
            aoai_api_version=_env("AOAI_API_VERSION", "2024-02-01"),
            embed_dimensions=_env_int("EMBED_DIMENSIONS", 3072),
            search_endpoint=(_env("SEARCH_ENDPOINT") or "").rstrip("/") or None,
            search_api_key=_env("SEARCH_API_KEY"),
            search_index_name=_env("SEARCH_INDEX_NAME", "freshservice-kb-v1"),
            search_api_version=_env("SEARCH_API_VERSION", "2024-07-01"),
            state_dir=Path(_env("STATE_DIR", ".state") or ".state"),
            batch_size=_env_int("BATCH_SIZE", 100),
            embed_batch_size=_env_int("EMBED_BATCH_SIZE", 16),
            drop_secret_only=_env_bool("DROP_SECRET_ONLY", True),
            min_symptom_chars=_env_int("MIN_SYMPTOM_CHARS", 15),
            monitor_db_path=_env("MONITOR_DB_PATH", "monitor.sqlite") or "monitor.sqlite",
            monitor_window_minutes=_env_int("MONITOR_WINDOW_MINUTES", 60),
            monitor_min_count=_env_int("MONITOR_MIN_COUNT", 3),
            monitor_cooldown_minutes=_env_int("MONITOR_COOLDOWN_MINUTES", 120),
            monitor_interval_seconds=_env_int("MONITOR_INTERVAL_SECONDS", 300),
            monitor_lookback_minutes=_env_int("MONITOR_LOOKBACK_MINUTES", 180),
            monitor_health_port=_env_int("MONITOR_HEALTH_PORT", 8016),
            monitor_dry_run=_env_bool("MONITOR_DRY_RUN", False),
            monitor_max_per_poll=_env_int("MONITOR_MAX_PER_POLL", 500),
            monitor_baseline_enabled=_env_bool("MONITOR_BASELINE_ENABLED", True),
            monitor_baseline_weeks=_env_int("MONITOR_BASELINE_WEEKS", 8),
            monitor_baseline_tz=_env("MONITOR_BASELINE_TZ", "America/Detroit") or "America/Detroit",
            monitor_baseline_interval_hours=_env_int("MONITOR_BASELINE_INTERVAL_HOURS", 24),
            monitor_baseline_sigma=_env_float("MONITOR_BASELINE_SIGMA", 3.0),
            graylog_url=(_env("GRAYLOG_URL") or "").rstrip("/") or None,
            graylog_api_token=_env("GRAYLOG_API_TOKEN"),
            graylog_verify_ssl=_env_bool("GRAYLOG_VERIFY_SSL", True),
            monitor_corroborate_enabled=_env_bool("MONITOR_CORROBORATE_ENABLED", True),
            monitor_corroborate_cause_lookback_hours=_env_int("MONITOR_CORROBORATE_CAUSE_LOOKBACK_HOURS", 24),
            monitor_corroborate_effect_lookback_hours=_env_int("MONITOR_CORROBORATE_EFFECT_LOOKBACK_HOURS", 1),
            monitor_corroborate_limit=_env_int("MONITOR_CORROBORATE_LIMIT", 8),
        )

    # --- capability checks used by the CLI to fail fast and clearly ---------
    def require_freshservice(self) -> None:
        missing = [n for n, v in (("FS_BASE_URL", self.fs_base_url), ("FS_API_KEY", self.fs_api_key)) if not v]
        if missing:
            raise RuntimeError(f"Missing FreshService configuration: {', '.join(missing)}")

    def require_embedding(self) -> None:
        """Validate the configured embedding provider's settings."""

        if self.embed_provider == "openai":
            missing = [
                n
                for n, v in (
                    ("EMBED_BASE_URL", self.embed_base_url),
                    ("EMBED_API_KEY", self.embed_api_key),
                    ("EMBED_MODEL", self.embed_model),
                )
                if not v
            ]
            if missing:
                raise RuntimeError(f"Missing embedding (OpenAI/LiteLLM) configuration: {', '.join(missing)}")
            return
        missing = [
            n
            for n, v in (
                ("AOAI_ENDPOINT", self.aoai_endpoint),
                ("AOAI_API_KEY", self.aoai_api_key),
                ("AOAI_EMBED_DEPLOYMENT", self.aoai_embed_deployment),
            )
            if not v
        ]
        if missing:
            raise RuntimeError(f"Missing Azure OpenAI configuration: {', '.join(missing)}")

    # Back-compat alias.
    require_azure_openai = require_embedding

    def require_search(self) -> None:
        missing = [
            n
            for n, v in (
                ("SEARCH_ENDPOINT", self.search_endpoint),
                ("SEARCH_API_KEY", self.search_api_key),
            )
            if not v
        ]
        if missing:
            raise RuntimeError(f"Missing Azure AI Search configuration: {', '.join(missing)}")

    def redacted(self) -> dict:
        """A secret-free view, safe to log."""

        def mask(value: Optional[str]) -> str:
            if not value:
                return "<unset>"
            return f"<set:…{value[-4:]}>" if len(value) > 4 else "<set>"

        return {
            "fs_base_url": self.fs_base_url or "<unset>",
            "fs_api_key": mask(self.fs_api_key),
            "embed_provider": self.embed_provider,
            "embed_base_url": self.embed_base_url or "<unset>",
            "embed_api_key": mask(self.embed_api_key),
            "embed_model": self.embed_model,
            "aoai_endpoint": self.aoai_endpoint or "<unset>",
            "aoai_api_key": mask(self.aoai_api_key),
            "aoai_embed_deployment": self.aoai_embed_deployment,
            "embed_dimensions": self.embed_dimensions,
            "search_endpoint": self.search_endpoint or "<unset>",
            "search_api_key": mask(self.search_api_key),
            "search_index_name": self.search_index_name,
        }
