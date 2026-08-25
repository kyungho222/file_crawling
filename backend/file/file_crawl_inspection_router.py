"""Read-only operational inspection endpoints for the file crawl pipeline."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response

from backend.shared.batch_embedding_scheduler import get_pending_embedding_callback_count
from backend.shared.crawler_state import crawler_state
from backend.shared.file_crawl_log import file_crawl_log_dir, file_crawl_log_path
from backend.shared.progress_contract import is_file_mode_workflow

logger = logging.getLogger("backend.file.file_crawl_inspection")

router = APIRouter(prefix="/file-crawl-dashboard", tags=["file-crawl-dashboard"])

_ROOT = Path(__file__).resolve().parents[2]
_HTML_PATH = _ROOT / "dashboard" / "file_crawl_dashboard.html"
_MAX_AUDIT_EVENTS = 80
_MAX_HISTORY_ITEMS = 200
_HISTORY_PATH = file_crawl_log_dir() / "file_crawl_dashboard_history.jsonl"
_HISTORY_LOCK = threading.Lock()


def _json_value(value: Any) -> Any:
    """Return dashboard-safe data without exposing arbitrary runtime objects."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    return str(value)


def _history_timestamp() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _append_dashboard_history(event: str, *, job_id: str, db_name: str, chat_bot_id: str, **extra: Any) -> None:
    """Keep dashboard-originated job ownership across process restarts.

    ASADAL_CRAWLING_LOG is externally created and does not consistently expose
    chat_bot_id.  This small append-only index only records dashboard control
    actions; runtime counters remain sourced from the real crawl log/workflow.
    """
    try:
        _HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": _history_timestamp(),
            "event": event,
            "job_id": job_id,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            **_json_value(extra),
        }
        with _HISTORY_LOCK:
            with _HISTORY_PATH.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.warning("[FileCrawlDashboard] history write failed | event=%s job_id=%s err=%s", event, job_id, exc)


def _dashboard_history(*, db_name: str = "", chat_bot_id: str = "", limit: int = 50) -> List[Dict[str, Any]]:
    """Return latest control history, filtered by the exact DB/chatbot pair."""
    try:
        if not _HISTORY_PATH.exists():
            return []
        records: Dict[str, Dict[str, Any]] = {}
        for raw_line in _HISTORY_PATH.read_text(encoding="utf-8", errors="replace").splitlines()[-_MAX_HISTORY_ITEMS:]:
            try:
                item = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            job_id = str(item.get("job_id") or "").strip()
            if not job_id:
                continue
            if db_name and str(item.get("db_name") or "") != db_name:
                continue
            if chat_bot_id and str(item.get("chat_bot_id") or "") != chat_bot_id:
                continue
            current = records.setdefault(job_id, {"job_id": job_id, "events": []})
            current.update({key: value for key, value in item.items() if key != "event"})
            current["events"].append(item)
        items = list(records.values())
        for item in items:
            item["events"] = item["events"][-10:]
            item["last_event"] = str(item["events"][-1].get("event") or "") if item["events"] else ""
        return sorted(items, key=lambda item: str(item.get("ts") or ""), reverse=True)[:max(1, min(limit, 100))]
    except Exception as exc:
        logger.warning("[FileCrawlDashboard] history read failed | db=%s chat_bot_id=%s err=%s", db_name, chat_bot_id, exc)
        return []


def _workflow_stats(workflow: Any) -> Dict[str, Any]:
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else getattr(workflow, "stats", {})
    except Exception as exc:
        return {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    return _json_value(dict(stats or {}))


def _workflow_queue_snapshot(workflow: Any) -> Dict[str, Any]:
    queues = getattr(workflow, "_file_job_queues", None)
    if queues is None:
        return {}
    try:
        return _json_value(queues.debug_snapshot())
    except Exception as exc:
        return {"snapshot_error": f"{type(exc).__name__}: {exc}"}


def _workflow_worker_snapshot(workflow: Any) -> Dict[str, Any]:
    snapshot = getattr(workflow, "_file_pipeline_worker_health_snapshot", None)
    if not callable(snapshot):
        return {}
    try:
        return _json_value(snapshot())
    except Exception as exc:
        return {"snapshot_error": f"{type(exc).__name__}: {exc}"}


def _workflow_db_metrics(workflow: Any) -> Dict[str, Any]:
    snapshot = getattr(workflow, "_file_db_operation_metrics_snapshot", None)
    if not callable(snapshot):
        return {}
    try:
        return _json_value(snapshot())
    except Exception as exc:
        return {"snapshot_error": f"{type(exc).__name__}: {exc}"}


def _audit_events(job_id: str) -> List[Dict[str, Any]]:
    """Read the bounded tail of the per-job JSONL audit file, best effort."""
    try:
        path = file_crawl_log_path(job_id)
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-_MAX_AUDIT_EVENTS:]
        result: List[Dict[str, Any]] = []
        for line in reversed(lines):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(record.get("job_id") or "") == job_id:
                result.append(_json_value(record))
        return result
    except Exception as exc:
        logger.debug("[FileCrawlInspection] audit read skipped | job_id=%s err=%s", job_id, exc)
        return []


def _is_file_job(job_id: str, workflow: Any = None) -> bool:
    if workflow is not None and is_file_mode_workflow(workflow):
        return True
    history = crawler_state.job_history.get(job_id) or {}
    detail = str(history.get("detail") or "").lower()
    return "file" in detail


def _job_summary(job_id: str, workflow: Any = None) -> Dict[str, Any]:
    history = dict(crawler_state.job_history.get(job_id) or {})
    task = crawler_state.workflow_tasks.get(job_id)
    return {
        "job_id": job_id,
        "active": workflow is not None,
        "task_done": bool(task.done()) if isinstance(task, asyncio.Task) else None,
        "history": _json_value(history),
        "stats": _workflow_stats(workflow) if workflow is not None else {},
    }


def _job_detail(job_id: str, workflow: Any) -> Dict[str, Any]:
    history = dict(crawler_state.job_history.get(job_id) or {})
    task = crawler_state.workflow_tasks.get(job_id)
    task_error = ""
    if isinstance(task, asyncio.Task) and task.done() and not task.cancelled():
        try:
            exc = task.exception()
            task_error = f"{type(exc).__name__}: {exc}" if exc else ""
        except Exception:
            task_error = "task_exception_unavailable"
    try:
        pending_callbacks = int(get_pending_embedding_callback_count(job_id) or 0)
    except Exception:
        pending_callbacks = 0
    return {
        "job_id": job_id,
        "active": True,
        "workflow": {
            "db_name": str(getattr(workflow, "db_name", "") or ""),
            "chat_bot_id": str(getattr(workflow, "chat_bot_id", "") or ""),
            "final_status": str(getattr(workflow, "final_status", "") or ""),
            "stop_requested": bool(getattr(workflow, "_stop_requested", False)),
            "hard_stop": bool(getattr(workflow, "_hard_stop", False)),
            "enable_db_save": getattr(workflow, "enable_db_save", None),
            "enable_learning": getattr(workflow, "enable_learning", None),
            "skip_learning": getattr(workflow, "file_pipeline_skip_learning", None),
        },
        "history": _json_value(history),
        "history_events": _json_value(list(crawler_state.job_history_events.get(job_id) or [])[-50:]),
        "stats": _workflow_stats(workflow),
        "queues": _workflow_queue_snapshot(workflow),
        "workers": _workflow_worker_snapshot(workflow),
        "db_operations": _workflow_db_metrics(workflow),
        "pending_embedding_callbacks": pending_callbacks,
        "task_error": task_error,
        "audit_events": _audit_events(job_id),
    }


async def _persistent_job_summary(item: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich a dashboard history row with the authoritative crawl-log counters."""
    job_id = str(item.get("job_id") or "").strip()
    db_name = str(item.get("db_name") or "").strip()
    summary: Dict[str, Any] = {}
    if job_id and db_name:
        try:
            from db.crawl_db_manager import get_crawling_log_summary

            summary = await get_crawling_log_summary(job_id, dbname=db_name)
        except Exception as exc:
            logger.debug("[FileCrawlDashboard] crawl log lookup skipped | job_id=%s db=%s err=%s", job_id, db_name, exc)
    return {
        **_json_value(item),
        "active": job_id in crawler_state.workflows,
        "crawl_log": _json_value(summary),
    }


@router.get("", response_class=FileResponse)
@router.get("/", response_class=FileResponse)
async def file_crawl_inspection_page() -> FileResponse:
    if not _HTML_PATH.exists():
        raise HTTPException(status_code=404, detail="file crawl inspection frontend missing")
    return FileResponse(
        _HTML_PATH,
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@router.get("/api/jobs")
async def list_file_crawl_jobs(
    db_name: str = "",
    chat_bot_id: str = "",
    history_limit: int = 50,
) -> Dict[str, Any]:
    db_filter = str(db_name or "").strip()
    chat_filter = str(chat_bot_id or "").strip()
    jobs: List[Dict[str, Any]] = []
    for job_id, workflow in list(crawler_state.workflows.items()):
        if _is_file_job(str(job_id), workflow):
            if db_filter and str(getattr(workflow, "db_name", "") or "") != db_filter:
                continue
            if chat_filter and str(getattr(workflow, "chat_bot_id", "") or "") != chat_filter:
                continue
            jobs.append(_job_summary(str(job_id), workflow))
    for job_id in reversed(list(crawler_state.job_history.keys())):
        jid = str(job_id)
        if any(item["job_id"] == jid for item in jobs) or not _is_file_job(jid):
            continue
        history = crawler_state.job_history.get(jid) or {}
        if db_filter and str(history.get("db_name") or "") != db_filter:
            continue
        if chat_filter and str(history.get("chat_bot_id") or "") != chat_filter:
            continue
        jobs.append(_job_summary(jid, None))
    persisted = await asyncio.gather(
        *[_persistent_job_summary(item) for item in _dashboard_history(
            db_name=db_filter,
            chat_bot_id=chat_filter,
            limit=history_limit,
        )],
        return_exceptions=True,
    )
    return {
        "jobs": jobs,
        "history": [item for item in persisted if isinstance(item, dict)],
        "workflow_slots": _json_value(crawler_state.get_workflow_debug_snapshot()),
    }


@router.post("/api/start")
async def start_file_crawl_from_dashboard(request: Request, background_tasks: BackgroundTasks) -> Response:
    """Invoke the normal file crawl start handler; the dashboard adds no parallel path."""
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON payload: {exc}") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON object payload required")

    requested_colle = str(body.get("colle") or "file").strip().lower()
    if requested_colle not in {"", "file"}:
        raise HTTPException(status_code=400, detail="dashboard only starts file crawling")
    job_id = str(body.get("job_id") or uuid.uuid4()).strip()
    db_name = str(body.get("db_name") or "").strip()
    chat_bot_id = str(body.get("chat_bot_id") or "").strip()
    if not db_name or not chat_bot_id:
        raise HTTPException(status_code=400, detail="db_name and chat_bot_id are required")

    body["job_id"] = job_id
    body["db_name"] = db_name
    body["chat_bot_id"] = chat_bot_id
    body["colle"] = "file"
    body["content_type"] = "file"
    _append_dashboard_history(
        "start_requested",
        job_id=job_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        start_url=str(body.get("contents_url") or body.get("access_url") or body.get("target_url") or ""),
        mode=str(body.get("dashboard_run_mode") or "full"),
    )
    logger.info(
        "[FileCrawlDashboard][start] job_id=%s db=%s chat_bot_id=%s mode=%s",
        job_id,
        db_name,
        chat_bot_id,
        body.get("dashboard_run_mode") or "full",
    )
    from backend.shared.crawl_start import crawl_start

    response = await crawl_start(request, background_tasks)
    if isinstance(response, JSONResponse) and response.status_code >= 400:
        _append_dashboard_history(
            "start_rejected",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            status_code=response.status_code,
        )
    return response


@router.post("/api/jobs/{job_id}/stop")
async def stop_file_crawl_from_dashboard(job_id: str, request: Request) -> Response:
    jid = str(job_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="job_id is required")
    workflow = crawler_state.workflows.get(jid)
    db_name = str(getattr(workflow, "db_name", "") or "").strip()
    chat_bot_id = str(getattr(workflow, "chat_bot_id", "") or "").strip()
    if not db_name or not chat_bot_id:
        for item in _dashboard_history(limit=_MAX_HISTORY_ITEMS):
            if item.get("job_id") == jid:
                db_name = db_name or str(item.get("db_name") or "")
                chat_bot_id = chat_bot_id or str(item.get("chat_bot_id") or "")
                break
    _append_dashboard_history(
        "stop_requested",
        job_id=jid,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        source=str(request.client.host if request.client else "internal"),
    )
    logger.info("[FileCrawlDashboard][stop] job_id=%s db=%s chat_bot_id=%s", jid, db_name, chat_bot_id)
    from backend.router import stop_crawl

    return await stop_crawl(jid)


@router.get("/api/jobs/{job_id}")
async def get_file_crawl_job(job_id: str) -> Dict[str, Any]:
    jid = str(job_id or "").strip()
    workflow = crawler_state.workflows.get(jid)
    if workflow is None:
        history = crawler_state.job_history.get(jid)
        if history is None:
            persisted = next((item for item in _dashboard_history(limit=_MAX_HISTORY_ITEMS) if item.get("job_id") == jid), None)
            if persisted is None:
                raise HTTPException(status_code=404, detail="file crawl job not found")
            detail = await _persistent_job_summary(persisted)
            crawl_log = detail.get("crawl_log") or {}
            return {
                "job_id": jid,
                "active": False,
                "workflow": {
                    "db_name": persisted.get("db_name") or "",
                    "chat_bot_id": persisted.get("chat_bot_id") or "",
                    "final_status": crawl_log.get("status") or persisted.get("last_event") or "",
                },
                "history": persisted,
                "history_events": persisted.get("events") or [],
                "stats": {
                    "scan_count": crawl_log.get("scan", 0),
                    "collection_count": crawl_log.get("collection", 0),
                    "save_count": crawl_log.get("save", 0),
                    "study_count": crawl_log.get("study", 0),
                },
                "queues": {},
                "workers": {},
                "db_operations": {},
                "pending_embedding_callbacks": 0,
                "task_error": "",
                "audit_events": _audit_events(jid),
                "message": "persistent dashboard history; live worker data are unavailable",
            }
        return {
            **_job_summary(jid, None),
            "history_events": _json_value(list(crawler_state.job_history_events.get(jid) or [])[-50:]),
            "audit_events": _audit_events(jid),
            "message": "workflow has already been cleaned up; live queue and worker data are unavailable",
        }
    if not _is_file_job(jid, workflow):
        raise HTTPException(status_code=404, detail="not a file crawl job")
    return _job_detail(jid, workflow)


@router.post("/api/jobs/{job_id}/sync-crawl-log")
async def sync_file_crawl_log(job_id: str) -> Dict[str, Any]:
    """Persist the current in-memory file workflow counters on explicit demand."""
    jid = str(job_id or "").strip()
    workflow = crawler_state.workflows.get(jid)
    if workflow is None or not _is_file_job(jid, workflow):
        raise HTTPException(status_code=404, detail="active file crawl job not found")

    db_name = str(getattr(workflow, "db_name", "") or "").strip()
    if not db_name:
        raise HTTPException(status_code=409, detail="workflow db_name is unavailable")
    stats = _workflow_stats(workflow)
    from db.crawl_db_manager import update_crawling_log_counters

    applied = await update_crawling_log_counters(
        jid,
        scan=int(stats.get("scan_count", 0) or 0),
        collection=int(stats.get("collection_count", 0) or 0),
        saved=int(stats.get("save_count", 0) or 0),
        study=int(stats.get("study_success_count", stats.get("study_count", 0)) or 0),
        dbname=db_name,
        log_id=getattr(workflow, "craw_id", None),
        colle="file",
        force=True,
    )
    if not applied:
        raise HTTPException(status_code=409, detail="crawling_log update was not applied")
    logger.info(
        "[FileCrawlDashboard][crawl_log_sync] job_id=%s db=%s scan=%s collection=%s save=%s study=%s",
        jid,
        db_name,
        stats.get("scan_count", 0),
        stats.get("collection_count", 0),
        stats.get("save_count", 0),
        stats.get("study_success_count", stats.get("study_count", 0)),
    )
    return {"job_id": jid, "db_name": db_name, "applied": True, "stats": stats}
