"""Hard response-header deadlines for protected download endpoints."""
from __future__ import annotations

import asyncio
import os
from typing import Any, Awaitable


def common_http_header_deadline_sec() -> float:
    """Return the common direct-download header deadline before PW fallback."""
    try:
        value = float(os.getenv("DOWNLOAD_HTTP_HEADER_HARD_TIMEOUT_SEC", "20") or "20")
    except (TypeError, ValueError):
        value = 20.0
    return max(1.0, min(value, 120.0))


def _consume_cancelled_task(task: asyncio.Task[Any]) -> None:
    try:
        response = task.result()
        release = getattr(response, "release", None)
        if callable(release):
            release()
    except BaseException:
        pass


async def await_http_response_headers(
    request: Awaitable[Any],
    *,
    timeout_sec: float,
) -> Any:
    """Return headers or hand control back without waiting for slow cancellation."""
    deadline = max(0.001, float(timeout_sec))
    request_task = asyncio.ensure_future(request)
    done, _ = await asyncio.wait({request_task}, timeout=deadline)
    if request_task in done:
        return request_task.result()

    request_task.cancel()
    request_task.add_done_callback(_consume_cancelled_task)
    raise asyncio.TimeoutError(f"http_response_headers_deadline:{deadline:.3f}s")
