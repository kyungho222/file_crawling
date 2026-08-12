"""Execution policy helpers for file LEARN_LIST persistence."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar


T = TypeVar("T")


# LEARN_LIST ensure 작업을 공통 timeout 정책 아래에서 실행합니다.
async def run_learn_list_ensure(operation: Callable[[], Awaitable[T]], timeout_sec: float) -> T:
    return await asyncio.wait_for(operation(), timeout=timeout_sec)
