from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar


T = TypeVar("T")


def is_retryable_embedding_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    retryable_markers = (
        "connection error",
        "api connection",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "connection refused",
        "rate limit",
        "429",
        "500",
        "502",
        "503",
        "504",
    )
    return any(marker in text for marker in retryable_markers)


async def run_embedding_call_with_retries(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    base_delay_seconds: float,
    logger=None,
    label: str = "embedding",
) -> T:
    max_attempts = max(1, int(attempts or 1))
    base_delay = max(0.0, float(base_delay_seconds or 0.0))
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            last_exc = exc
            retryable = is_retryable_embedding_error(exc)
            if (not retryable) or attempt >= max_attempts:
                raise

            delay = base_delay * (2 ** (attempt - 1))
            if logger:
                logger.warning(
                    "[임베딩 재시도] %s failed (%s/%s): %s | retry_in=%.2fs",
                    label,
                    attempt,
                    max_attempts,
                    exc,
                    delay,
                )
            await asyncio.sleep(delay)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label}_retry_exhausted")
