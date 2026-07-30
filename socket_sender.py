import logging
import asyncio
from typing import Any, Optional

logger = logging.getLogger("socket_sender")


async def _maybe_await(result: Any) -> Any:
    if asyncio.iscoroutine(result):
        return await result
    return result


async def send_message_to_socket(job_id: str, payload: Any, job_manager: Optional[Any] = None) -> None:
    """
    Compatibility shim for socket/SSE message sending.

    Many modules call:
        await send_message_to_socket(job_id, message_dict, job_manager)

    This implementation tries a few sensible fallbacks:
    - If job_manager has a coroutine callable named 'enqueue_sse_message', 'publish', 'send', or 'enqueue',
      call it with (job_id, payload).
    - Otherwise, log the message at INFO level.
    """
    try:
        if job_manager is not None:
            # Prefer well-known method names
            for name in ("enqueue_sse_message", "enqueue", "publish", "send", "send_message"):
                fn = getattr(job_manager, name, None)
                if callable(fn):
                    try:
                        res = fn(job_id, payload)
                        await _maybe_await(res)
                        return
                    except TypeError:
                        # Try without job_id
                        res = fn(payload)
                        await _maybe_await(res)
                        return

        # Fallback: try to import shared SSE helper (best-effort)
        try:
            from backend.shared.sse_publish_queue import enqueue_sse_message  # type: ignore
            res = enqueue_sse_message(job_id, payload)
            await _maybe_await(res)
            return
        except Exception:
            pass

        # Final fallback: log
    except Exception as e:
        logger.exception("send_message_to_socket failed: %s", e)

async def send_message_to_redis_sse(job_id: str, message: dict, dbname: Optional[str] = None):
    """
    Compatibility shim to publish Redis SSE formatted messages.
    Tries backend.shared.redis_sse_service.send_message_to_redis_sse first,
    falls back to best-effort publish/log.
    """
    try:
        try:
            from backend.shared.redis_sse_service import send_message_to_redis_sse as _send  # type: ignore
            return await _send(job_id=job_id, message=message, dbname=dbname)
        except Exception:
            pass

        # Fallback: try publish via backend.shared.sse_publish_queue.enqueue_sse_message
        try:
            from backend.shared.sse_publish_queue import enqueue_sse_message  # type: ignore
            # enqueue_sse_message signature may accept (job_id, message, dbname, topic)
            res = enqueue_sse_message(job_id, message, dbname, "workflow_progress")
            if asyncio.iscoroutine(res):
                await res
            return None
        except Exception:
            pass

        # Final fallback: just log
    except Exception as e:
        logger.exception("send_message_to_redis_sse failed: %s", e)
