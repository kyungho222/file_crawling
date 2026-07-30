"""Compatibility wrapper for gradual BoardContentWorkflow extraction."""

from __future__ import annotations

from typing import Any


class BoardContentWorkflowCore:
    """Proxy around the current workflow while responsibilities are extracted."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    async def start_workflow(self, *args: Any, **kwargs: Any) -> Any:
        return await self._inner.start_workflow(*args, **kwargs)

    def get_stats(self) -> Any:
        return self._inner.get_stats()

    def stop(self) -> Any:
        return self._inner.stop()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
