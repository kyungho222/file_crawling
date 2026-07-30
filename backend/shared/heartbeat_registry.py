import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger("backend.shared.heartbeat_registry")


@dataclass
class HeartbeatHandle:
    job_id: str
    db_name: str
    stop_event: asyncio.Event
    task: asyncio.Task


_lock = asyncio.Lock()
_by_job: Dict[str, HeartbeatHandle] = {}


async def register_heartbeat(job_id: str, db_name: str, stop_event: asyncio.Event, task: asyncio.Task) -> None:
    if not job_id or not task:
        return
    async with _lock:
        _by_job[job_id] = HeartbeatHandle(job_id=job_id, db_name=db_name or "", stop_event=stop_event, task=task)


async def unregister_heartbeat(job_id: str) -> None:
    if not job_id:
        return
    async with _lock:
        _by_job.pop(job_id, None)


async def stop_heartbeat_by_db(db_name: str, reason: Optional[str] = None) -> int:
    """Stop all heartbeat loops that match db_name."""
    name = (db_name or "").strip()
    if not name:
        return 0
    async with _lock:
        targets = [hb for hb in _by_job.values() if hb.db_name == name]
    if not targets:
        return 0
    stopped = 0
    for hb in targets:
        try:
            hb.stop_event.set()
            hb.task.cancel()
            try:
                await hb.task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            stopped += 1
        except Exception:
            pass
    async with _lock:
        for hb in targets:
            _by_job.pop(hb.job_id, None)
    if stopped:
        logger.info(
            "[HeartbeatRegistry] stopped=%s db=%s reason=%s",
            stopped,
            name,
            reason or "unknown",
        )
    return stopped

