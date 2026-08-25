"""Process-local host admission shared by file detail fetches and downloads."""
from __future__ import annotations

import asyncio
import os
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


_TRANSPORT_FAILURES = {"connect_timeout", "body_timeout", "stream_stall_timeout"}
_GATES: Dict[Tuple[int, str], "_HostGate"] = {}


def _host_env_key(host: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(host or "").strip().upper()).strip("_")


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.getenv(name, default) or default).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _configured_limit(host: str, requested_limit: Optional[int]) -> int:
    host_key = _host_env_key(host)
    host_override = (
        _env_int(
            f"FILE_CRAWL_SHARED_HOST_MAX_CONCURRENT_{host_key}",
            0,
            minimum=0,
            maximum=16,
        )
        if host_key
        else 0
    )
    configured = host_override or _env_int(
        "FILE_CRAWL_SHARED_HOST_MAX_CONCURRENT",
        3,
        minimum=1,
        maximum=16,
    )
    if requested_limit is None:
        return configured
    try:
        requested = max(1, int(requested_limit))
    except (TypeError, ValueError):
        return configured
    return min(configured, requested)


@dataclass
class _HostGate:
    limit: int
    active: int = 0
    failures: int = 0
    last_failure_at: float = 0.0
    backpressure_until: float = 0.0
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)

    def effective_limit(self, now: float) -> int:
        if self.backpressure_until > now:
            return 1
        if self.backpressure_until:
            self.backpressure_until = 0.0
            self.failures = 0
        return self.limit


def _gate(host: str) -> _HostGate:
    normalized_host = str(host or "").strip().lower()
    if not normalized_host:
        raise ValueError("host is required")
    key = (id(asyncio.get_running_loop()), normalized_host)
    limit = _configured_limit(normalized_host, None)
    gate = _GATES.get(key)
    if gate is None:
        gate = _HostGate(limit=limit)
        _GATES[key] = gate
    return gate


async def acquire_file_crawl_host_slot(
    host: str,
    *,
    requested_limit: Optional[int] = None,
    timeout_sec: Optional[float] = None,
) -> Dict[str, float | int]:
    """Acquire one shared file-crawl host slot.

    The gate is per process/event loop and therefore spans detail-page fetch,
    response prewarm, direct download, and retry work for different jobs.
    """
    gate = _gate(host)
    caller_limit = _configured_limit(host, requested_limit)
    started_at = time.monotonic()

    async def _wait() -> None:
        async with gate.condition:
            while gate.active >= min(gate.effective_limit(time.monotonic()), caller_limit):
                await gate.condition.wait()
            gate.active += 1

    if timeout_sec is None:
        await _wait()
    else:
        await asyncio.wait_for(_wait(), timeout=max(0.001, float(timeout_sec)))
    now = time.monotonic()
    return {
        "limit": min(gate.effective_limit(now), caller_limit),
        "active": gate.active,
        "wait_sec": max(0.0, now - started_at),
        "backpressure": int(gate.backpressure_until > now),
    }


async def release_file_crawl_host_slot(host: str) -> None:
    gate = _gate(host)
    async with gate.condition:
        if gate.active > 0:
            gate.active -= 1
        gate.condition.notify_all()


async def record_file_crawl_host_transport_failure(host: str, reason: str) -> None:
    if str(reason or "") not in _TRANSPORT_FAILURES:
        return
    gate = _gate(host)
    now = time.monotonic()
    async with gate.condition:
        gate.failures = gate.failures + 1 if now - gate.last_failure_at <= 45.0 else 1
        gate.last_failure_at = now
        if gate.failures >= 2:
            gate.backpressure_until = now + 30.0
            gate.condition.notify_all()


async def record_file_crawl_host_transport_success(host: str) -> None:
    gate = _gate(host)
    async with gate.condition:
        gate.failures = 0
        gate.last_failure_at = 0.0
        gate.backpressure_until = 0.0
        gate.condition.notify_all()


def clear_file_crawl_host_gates_for_test() -> None:
    _GATES.clear()
