"""Small pipeline facade used during board workflow refactoring."""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("backend.board.board_pipeline")


class FilePipeline:
    """Minimal lifecycle wrapper for future WorkerManager/JobQueues extraction."""

    def __init__(self, use_global_pool: bool = False) -> None:
        self.use_global_pool = use_global_pool
        self._running = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return
            self._running = True
            logger.debug("FilePipeline started")

    async def stop(self) -> None:
        async with self._lock:
            if not self._running:
                return
            self._running = False
            logger.debug("FilePipeline stopped")

    def is_running(self) -> bool:
        return self._running
