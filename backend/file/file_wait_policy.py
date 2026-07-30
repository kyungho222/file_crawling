"""Resolve per-chatbot file crawl request delay from crawling configuration."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Tuple


logger = logging.getLogger("backend.file.file_wait_policy")

FILE_FETCH_DELAY_PAYLOAD_KEY = "_file_crawl_fetch_delay_sec"
FILE_FETCH_DELAY_SOURCE_KEY = "_file_crawl_fetch_delay_source"


def backend_file_fetch_delay_sec() -> float:
    raw = (
        os.getenv("FILE_CRAWL_FETCH_ENQUEUE_DELAY_SEC")
        or os.getenv("FILE_CRAWL_FETCH_INTERVAL_SEC")
        or "3"
    )
    try:
        value = float(raw)
    except Exception:
        value = 3.0
    return max(0.0, min(value, 60.0))


def resolve_file_fetch_delay(
    values: Dict[str, Any],
    *,
    backend_default: float | None = None,
) -> Tuple[float, str]:
    default = backend_file_fetch_delay_sec() if backend_default is None else float(backend_default)
    default = max(0.0, min(default, 60.0))
    if "file_waiti" not in (values or {}):
        return default, "backend_default_missing_file_waiti"
    try:
        configured = float(str((values or {}).get("file_waiti") or "").strip())
    except Exception:
        return default, "backend_default_invalid_file_waiti"
    if configured < 0.0 or configured > 60.0:
        return default, "backend_default_invalid_file_waiti"
    return configured, "database_file_waiti"


async def apply_file_wait_config_to_payload(
    data: Dict[str, Any],
    *,
    db_name: str,
    chat_bot_id: str,
    job_id: str,
) -> float:
    values: Dict[str, Any] = {}
    try:
        from db.crawl_db_manager import get_config_values_by_keys

        values = await get_config_values_by_keys(
            chat_bot_id,
            ["file_waiti"],
            dbname=db_name,
            match_chat_bot_id=True,
            use_cache=False,
        )
    except Exception as exc:
        logger.warning(
            "[FileWaitPolicy] config lookup failed; backend default used | job_id=%s db=%s chat_bot_id=%s err=%s",
            job_id,
            db_name,
            chat_bot_id,
            exc,
        )

    delay_sec, source = resolve_file_fetch_delay(values)
    data[FILE_FETCH_DELAY_PAYLOAD_KEY] = delay_sec
    data[FILE_FETCH_DELAY_SOURCE_KEY] = source
    logger.info("[FileWaitPolicy] 크롤링 대기시간=%s초", int(delay_sec) if float(delay_sec).is_integer() else delay_sec)
    return delay_sec
