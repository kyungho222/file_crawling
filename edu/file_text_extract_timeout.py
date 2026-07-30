from __future__ import annotations

import asyncio
import os
from typing import Any


def file_text_extract_timeout_seconds(timeout_sec: float | None = None) -> float:
    if timeout_sec is not None:
        try:
            value = float(timeout_sec)
        except Exception:
            value = 0.0
    else:
        try:
            value = float(os.getenv("FILE_TEXT_EXTRACT_TIMEOUT_SEC", "1800") or "1800")
        except Exception:
            value = 1800.0
    return max(0.0, min(value, 24 * 3600.0))


async def await_file_text_extract(
    awaitable,
    *,
    path: str = "",
    stage: str = "",
    logger: Any = None,
    timeout_sec: float | None = None,
    raise_on_timeout: bool = True,
):
    timeout = file_text_extract_timeout_seconds(timeout_sec)
    try:
        if timeout > 0:
            return await asyncio.wait_for(awaitable, timeout=timeout)
        return await awaitable
    except asyncio.TimeoutError as exc:
        if logger is not None:
            logger.error(
                "[FileTextExtractTimeout] timeout=%ss stage=%s path=%s",
                int(timeout),
                stage,
                str(path or "")[:260],
            )
        if raise_on_timeout:
            raise TimeoutError(f"file_text_extract_timeout:{int(timeout)}s:{stage}") from exc
        return ""
