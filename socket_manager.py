"""
Compatibility shim for socket_manager.

Some environments may not provide a full socket_manager implementation.
This file provides a lightweight fallback object named `socket_manager`
with a few common methods used by the codebase. All methods are best-effort
and will not raise if underlying services are missing.
"""
from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any, Optional

logger = logging.getLogger("socket_manager")


class _SocketManager:
    async def send(self, job_id: str, payload: Any) -> None:
        try:
            # best-effort: delegate to socket_sender if available
            from socket_sender import send_message_to_socket  # type: ignore

            res = send_message_to_socket(job_id, payload, None)
            if asyncio.iscoroutine(res):
                await res
            return
        except Exception:
            pass
    def enqueue(self, *args, **kwargs) -> None:
        # legacy convenience: do nothing
        logger.debug("socket_manager.enqueue called - args=%s kwargs=%s", args, kwargs)

    def publish(self, *args, **kwargs) -> None:
        logger.debug("socket_manager.publish called - args=%s kwargs=%s", args, kwargs)

    def register(self, *args, **kwargs) -> None:
        logger.debug("socket_manager.register called - args=%s kwargs=%s", args, kwargs)

    def unregister(self, *args, **kwargs) -> None:
        logger.debug("socket_manager.unregister called - args=%s kwargs=%s", args, kwargs)


# exported singleton
socket_manager = SimpleNamespace()
_sm = _SocketManager()
for name in ("send", "enqueue", "publish", "register", "unregister"):
    setattr(socket_manager, name, getattr(_sm, name))

__all__ = ["socket_manager"]

