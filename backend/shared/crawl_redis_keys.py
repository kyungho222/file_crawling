"""Shared Redis key namespace for crawl runtime state."""

from __future__ import annotations

from typing import Optional

try:
    from config.settings import APP_ENV
except Exception:  # pragma: no cover - import-safe fallback
    APP_ENV = "prod"


_DEV_ENVS = {"dev", "development", "test"}


def crawl_redis_namespace() -> str:
    """Keep production keys stable while isolating non-production runtimes."""
    return "crawl:dev" if str(APP_ENV or "prod").strip().lower() in _DEV_ENVS else "crawl"


def crawl_state_key(db_name: str, job_id: str) -> str:
    return f"{crawl_redis_namespace()}:{db_name}:{job_id}:state"


def crawl_progress_channel(db_name: str, job_id: str) -> str:
    return f"{crawl_redis_namespace()}:{db_name}:{job_id}:progress"


def crawl_client_heartbeat_key(db_name: str, job_id: str) -> str:
    return f"{crawl_redis_namespace()}:{db_name}:{job_id}:client_heartbeat"


def crawl_state_scan_pattern(job_id: str) -> str:
    return f"{crawl_redis_namespace()}:*:{job_id}:state"


def db_name_from_crawl_state_key(key: str) -> Optional[str]:
    parts = str(key or "").split(":")
    namespace_parts = crawl_redis_namespace().split(":")
    db_index = len(namespace_parts)
    if len(parts) <= db_index + 2 or parts[:db_index] != namespace_parts:
        return None
    return str(parts[db_index] or "").strip() or None