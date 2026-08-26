"""In-memory DB diagnostics for active file crawl jobs.

This collector deliberately has no logger and no persistence path.  It is
consumed only by the file crawl DB dashboard while the current process lives.
"""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional


_MAX_JOBS = 200
_MAX_EVENTS_PER_JOB = 300
_jobs: Dict[str, Dict[str, Any]] = {}


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _job(job_id: Any, db_name: Any = "") -> Optional[Dict[str, Any]]:
    jid = _text(job_id)
    if not jid:
        return None
    item = _jobs.get(jid)
    if item is None:
        while len(_jobs) >= _MAX_JOBS:
            _jobs.pop(next(iter(_jobs)), None)
        item = {
            "job_id": jid,
            "db_name": _text(db_name),
            "sequence": 0,
            "events": deque(maxlen=_MAX_EVENTS_PER_JOB),
            "operations": {},
            "created_at": _now(),
        }
        _jobs[jid] = item
    elif _text(db_name):
        item["db_name"] = _text(db_name)
    return item


def record_file_crawl_db_event(
    *,
    job_id: Any,
    db_name: Any,
    category: int,
    event: str,
    elapsed_ms: Optional[float] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    item = _job(job_id, db_name)
    if item is None:
        return
    item["sequence"] = int(item.get("sequence", 0) or 0) + 1
    payload: Dict[str, Any] = {
        "sequence": item["sequence"],
        "at": _now(),
        "category": max(1, min(int(category or 1), 6)),
        "event": _text(event) or "unknown",
    }
    if elapsed_ms is not None:
        payload["elapsed_ms"] = round(max(0.0, float(elapsed_ms)), 1)
    if isinstance(detail, dict):
        payload["detail"] = {str(k): v for k, v in detail.items()}
    item["events"].append(payload)


def record_file_crawl_db_operation(*, job_id: Any, db_name: Any, operation: str, elapsed_ms: float) -> None:
    item = _job(job_id, db_name)
    if item is None:
        return
    op = _text(operation) or "unknown"
    bucket = item["operations"].setdefault(op, {"count": 0, "total_ms": 0.0, "max_ms": 0.0})
    bucket["count"] = int(bucket["count"] or 0) + 1
    bucket["total_ms"] = float(bucket["total_ms"] or 0.0) + max(0.0, float(elapsed_ms or 0.0))
    bucket["max_ms"] = max(float(bucket["max_ms"] or 0.0), max(0.0, float(elapsed_ms or 0.0)))
    category = 6 if ("duplicate" in op or "status" in op) else 1
    record_file_crawl_db_event(
        job_id=job_id,
        db_name=db_name,
        category=category,
        event=op,
        elapsed_ms=elapsed_ms,
    )


def record_file_crawl_start_url_summary(summary: Dict[str, Any]) -> None:
    if not isinstance(summary, dict):
        return
    record_file_crawl_db_event(
        job_id=summary.get("job_id"),
        db_name=summary.get("db_name"),
        category=5,
        event="exploration_start_urls_ready",
        detail={
            "sql_rows": int(summary.get("sql_rows", 0) or 0),
            "final_start_urls": int(summary.get("final_start_urls", 0) or 0),
            "deduped": int(summary.get("deduped", 0) or 0),
            "excluded": max(0, int(summary.get("sql_rows", 0) or 0) - int(summary.get("final_start_urls", 0) or 0)),
        },
    )


def snapshot_file_crawl_db_diagnostics(job_id: Any, *, after_sequence: int = 0) -> Dict[str, Any]:
    item = _jobs.get(_text(job_id))
    if item is None:
        return {"available": False, "job_id": _text(job_id), "events": [], "operations": {}}
    operations = {}
    for op, bucket in item.get("operations", {}).items():
        count = int(bucket.get("count", 0) or 0)
        total_ms = float(bucket.get("total_ms", 0.0) or 0.0)
        operations[op] = {
            "count": count,
            "avg_ms": round(total_ms / count, 1) if count else 0.0,
            "max_ms": round(float(bucket.get("max_ms", 0.0) or 0.0), 1),
            "total_ms": round(total_ms, 1),
        }
    marker = max(0, int(after_sequence or 0))
    return {
        "available": True,
        "job_id": item["job_id"],
        "db_name": item.get("db_name", ""),
        "sequence": int(item.get("sequence", 0) or 0),
        "operations": operations,
        "events": [event for event in item["events"] if int(event.get("sequence", 0) or 0) > marker],
    }
