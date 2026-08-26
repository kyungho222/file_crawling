"""Hard response-header deadlines for protected download endpoints."""
from __future__ import annotations

import asyncio
from typing import Any, Awaitable


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
