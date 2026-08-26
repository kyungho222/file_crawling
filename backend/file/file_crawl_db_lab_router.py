"""Live, in-memory DB diagnostics for active file crawl jobs."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

from backend.shared.crawler_state import crawler_state
from backend.shared.db_write_queue import db_write_queue_status
from backend.shared.file_crawl_db_diagnostics import snapshot_file_crawl_db_diagnostics


router = APIRouter(prefix="/file-crawl-db-lab", tags=["file-crawl-db-lab"])
_HTML_PATH = Path(__file__).resolve().parents[2] / "dashboard" / "file_crawl_db_lab.html"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _pool_snapshot(db_name: str) -> str:
    try:
        from db.mariadb_pool import _current_pool_snapshot

        return str(_current_pool_snapshot(db_name))
    except Exception as exc:
        return f"unavailable: {type(exc).__name__}"


def _workflow_db_metrics(workflow: Any) -> Dict[str, Any]:
    try:
        getter = getattr(workflow, "_file_db_operation_metrics_snapshot", None)
        return getter() if callable(getter) else {}
    except Exception:
        return {}


def build_learn_list_insert_phase_metrics(operations: Dict[str, Any]) -> Dict[str, Any]:
    """Expose measured insert sub-steps while retaining the total metric."""
    prefix = "file_learn_list_insert_"
    return {
        operation[len(prefix):]: metric
        for operation, metric in (operations or {}).items()
        if str(operation).startswith(prefix)
    }


def _pipeline_snapshot(workflow: Any) -> Dict[str, Any]:
    if workflow is None:
        return {}
    try:
        queues = getattr(workflow, "_file_job_queues", None)
        queue_snapshot = queues.debug_snapshot() if queues is not None and hasattr(queues, "debug_snapshot") else {}
    except Exception as exc:
        queue_snapshot = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    try:
        health = getattr(workflow, "_file_pipeline_worker_health_snapshot", None)
        worker_snapshot = health() if callable(health) else {}
    except Exception as exc:
        worker_snapshot = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    return {"queues": queue_snapshot, "workers": worker_snapshot}


def _snapshot(job_id: str, after_sequence: int = 0) -> Dict[str, Any]:
    jid = _text(job_id)
    if not jid:
        raise HTTPException(status_code=400, detail="job_id is required")
    workflow = crawler_state.workflows.get(jid)
    diagnostics = snapshot_file_crawl_db_diagnostics(jid, after_sequence=after_sequence)
    db_name = _text(getattr(workflow, "db_name", "")) or _text(diagnostics.get("db_name"))
    workflow_metrics = _workflow_db_metrics(workflow) if workflow is not None else {}
    pool = _pool_snapshot(db_name) if db_name else "db_name unavailable"
    return {
        "job_id": jid,
        "active": workflow is not None,
        "workflow": {
            "db_name": db_name,
            "chat_bot_id": _text(getattr(workflow, "chat_bot_id", "")),
            "final_status": _text(getattr(workflow, "final_status", "")),
            "stop_requested": bool(getattr(workflow, "_stop_requested", False)),
        },
        "categories": {
            "1": {
                "title": "연결·쿼리 시간",
                "operations": diagnostics.get("operations", {}),
                "learn_list_insert_phases": build_learn_list_insert_phase_metrics(diagnostics.get("operations", {})),
                "workflow_operations": workflow_metrics,
                "pool": pool,
            },
            "2": {"title": "DB Write Queue", "queue": db_write_queue_status()},
            "3": {"title": "Timeout·장기 점유", "queue": db_write_queue_status()},
            "4": {
                "title": "설정 조회·캐시",
                "events": [e for e in diagnostics.get("events", []) if e.get("category") == 4],
                "note": "workflow DB 준비 진입 이후의 in-memory event",
            },
            "5": {"title": "Exploration·Start URL", "events": [e for e in diagnostics.get("events", []) if e.get("category") == 5]},
            "6": {"title": "중복·상태 UPDATE", "events": [e for e in diagnostics.get("events", []) if e.get("category") == 6]},
        },
        "pipeline": _pipeline_snapshot(workflow),
        "diagnostics": diagnostics,
    }


@router.get("")
async def file_crawl_db_lab_page() -> FileResponse:
    return FileResponse(_HTML_PATH, headers={"X-File-Crawl-DB-Lab-Build": "1"})


@router.get("/api/jobs/{job_id}")
async def file_crawl_db_lab_job(job_id: str, after_sequence: int = 0) -> Dict[str, Any]:
    return _snapshot(job_id, after_sequence=after_sequence)


@router.get("/events/{job_id}")
async def file_crawl_db_lab_events(job_id: str, after_sequence: int = 0) -> StreamingResponse:
    async def stream():
        marker = max(0, int(after_sequence or 0))
        while True:
            payload = _snapshot(job_id, after_sequence=marker)
            marker = max(marker, int(payload.get("diagnostics", {}).get("sequence", marker) or marker))
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            await asyncio.sleep(1.0)

    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
