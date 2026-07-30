from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
except Exception:
    pass


_DEFAULT_CRAWL_CELERY_WORKER_CONCURRENCY = 1


def _clamp_worker_concurrency(value: int) -> int:
    return max(1, min(int(value), 64))


def resolve_crawl_celery_worker_concurrency(explicit: Optional[int] = None) -> int:
    """Return the effective Celery worker concurrency for crawl workflows.

    Priority:
    1. Explicit argument
    2. `CRAWL_CELERY_WORKER_CONCURRENCY`
    3. `CELERY_CONCURRENCY`
    4. Safe default: 1
    """

    if explicit is not None:
        try:
            return _clamp_worker_concurrency(int(explicit))
        except Exception:
            return _DEFAULT_CRAWL_CELERY_WORKER_CONCURRENCY

    for env_name in ("CRAWL_CELERY_WORKER_CONCURRENCY", "CELERY_CONCURRENCY"):
        raw = str(os.getenv(env_name, "") or "").strip()
        if not raw:
            continue
        try:
            return _clamp_worker_concurrency(int(raw))
        except Exception:
            continue

    return _DEFAULT_CRAWL_CELERY_WORKER_CONCURRENCY
