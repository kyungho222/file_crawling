"""Maintenance dashboard API for persisted file metadata and SimHash backfill."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse

from backend.file.file_crawl_maintenance_backfill import (
    _normalize_target_content_type,
    run_file_metadata_backfill,
    run_file_simhash_backfill,
)
from backend.file.file_crawl_maintenance_health import build_execution_health
from backend.shared.job_completion_summary import build_job_completion_summary


logger = logging.getLogger("backend.file.file_crawl_maintenance_lab")
router = APIRouter(prefix="/file-crawl-maintenance-lab", tags=["file-crawl-maintenance-lab"])

_ROOT = Path(__file__).resolve().parents[2]
_HTML_PATH = _ROOT / "dashboard" / "file_crawl_maintenance_lab.html"
_JOBS: Dict[str, Dict[str, Any]] = {}
_MAX_JOBS = 50
_MAX_EVENTS = 300


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _event(job: Dict[str, Any], event: str, **payload: Any) -> None:
    job["last_activity_at"] = _now()
    events = job.setdefault("events", [])
    events.append({"event": event, "at": _now(), **payload})
    del events[:-_MAX_EVENTS]


def _set_completion_summary(job: Dict[str, Any]) -> None:
    summary = dict(job.get("summary") or {})
    completion = build_job_completion_summary(
        summary,
        job_id=_text(job.get("job_id")) or "-",
        workflow_name="메타데이터 백필" if job.get("mode") == "metadata" else "해시 백필",
        status=_text(job.get("status")),
        processing_count=summary.get("updated"),
        followup_pending=False,
    )
    job["completion_summary"] = completion
    logger.info("[FileMaintenanceLab][completion] %s", completion["text"])


def _view(job: Dict[str, Any], *, events: bool = False) -> Dict[str, Any]:
    value = {key: item for key, item in job.items() if key not in {"task", "stop_event", "events", "metadata_plan", "simhash_plan", "simhash_plan_details"}}
    if job.get("status") == "awaiting_approval":
        value["approval_preview"] = list(job.get("simhash_plan_details") or job.get("preview_samples") or [])
    task = job.get("task")
    task_alive = isinstance(task, asyncio.Task) and not task.done()
    last_activity_at = _text(job.get("last_activity_at"))
    activity_age_seconds = 0.0
    if last_activity_at:
        try:
            activity_age_seconds = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(last_activity_at).astimezone(timezone.utc)).total_seconds())
        except ValueError:
            activity_age_seconds = 0.0
    value["execution_health"] = {
        **build_execution_health(
            _text(job.get("status")),
            task_alive=task_alive,
            activity_age_seconds=activity_age_seconds,
        ),
        "task_alive": task_alive,
        "last_activity_at": last_activity_at,
    }
    value["event_count"] = len(job.get("events") or [])
    if events:
        value["events"] = list(job.get("events") or [])
    return value


def _trim_finished() -> None:
    finished = [job for job in _JOBS.values() if job.get("status") in {"completed", "failed", "stopped"}]
    finished.sort(key=lambda item: str(item.get("completed_at") or item.get("created_at") or ""))
    for job in finished[:-_MAX_JOBS]:
        _JOBS.pop(_text(job.get("job_id")), None)


async def _run(job_id: str) -> None:
    job = _JOBS.get(job_id)
    if job is None:
        return
    started = time.perf_counter()
    job["status"] = "running"
    job["started_at"] = _now()
    _event(job, "backfill_started", mode=job["mode"])

    async def report(event_name: str, payload: Dict[str, Any]) -> None:
        if event_name in {"metadata_scan_started", "metadata_scan_progress"} or event_name.startswith("simhash_"):
            job["progress"] = dict(payload)
        _event(job, event_name, mode=job["mode"], **payload)

    try:
        if job["mode"] == "metadata":
            summary = await run_file_metadata_backfill(
                job_id=job_id,
                db_name=job["db_name"],
                chat_bot_id=job["chat_bot_id"],
                stop_event=job["stop_event"],
                event=report,
                pg_table=job["pg_table"],
                content_type=job["content_type"],
                target_domains=[job["target_domain"]],
            )
            job["metadata_plan"] = list(summary.pop("_prepared_plan", []) or [])
            job["preview_samples"] = list(summary.pop("preview_samples", []) or [])
            job["ambiguous_filename_candidates"] = list(summary.pop("_ambiguous_filename_candidates", []) or [])
            job["summary"] = summary
            if job["stop_event"].is_set():
                job["status"] = "stopped"
                _event(job, "backfill_stopped")
            elif not job["metadata_plan"]:
                job["status"] = "completed"
                _set_completion_summary(job)
                _event(
                    job,
                    "backfill_completed",
                    summary=summary,
                    ambiguous_filename_candidates=job["ambiguous_filename_candidates"],
                )
            else:
                job["status"] = "awaiting_approval"
                _event(
                    job,
                    "metadata_approval_required",
                    summary=summary,
                    preview=job["preview_samples"],
                    ambiguous_filename_candidates=job["ambiguous_filename_candidates"],
                )
            return
        else:
            summary = await run_file_simhash_backfill(
                job_id=job_id,
                db_name=job["db_name"],
                chat_bot_id=job["chat_bot_id"],
                stop_event=job["stop_event"],
                event=report,
                pg_table=job["pg_table"],
                content_type=job["content_type"],
            )
            job["simhash_plan"] = list(summary.pop("_prepared_learn_list_ids", []) or [])
            job["simhash_plan_details"] = list(summary.pop("_prepared_file_details_all", []) or [])
            job["summary"] = summary
            if job["stop_event"].is_set():
                job["status"] = "stopped"
                _event(job, "backfill_stopped")
            elif not job["simhash_plan"]:
                job["status"] = "completed"
                _set_completion_summary(job)
                _event(job, "backfill_completed", summary=summary)
            else:
                job["status"] = "awaiting_approval"
                _event(job, "simhash_approval_required", summary=summary)
            return
    except asyncio.CancelledError:
        job["status"] = "stopped"
        _event(job, "backfill_stopped")
        raise
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = f"{type(exc).__name__}: {exc}"
        _event(job, "backfill_failed", error=job["error"])
        logger.exception(
            "[FileMaintenanceLab][failed] job_id=%s mode=%s db=%s chat_bot_id=%s",
            job_id,
            job.get("mode"),
            job.get("db_name"),
            job.get("chat_bot_id"),
        )
    finally:
        if job.get("status") in {"completed", "failed", "stopped"}:
            job["completed_at"] = _now()
            if not job.get("completion_summary"):
                _set_completion_summary(job)
        job["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)


async def _apply_backfill_plan(job_id: str) -> None:
    job = _JOBS.get(job_id)
    if job is None:
        return
    started = time.perf_counter()
    job["status"] = "applying"
    _event(job, f"{job['mode']}_backfill_apply_started", mode=job["mode"])

    async def report(event_name: str, payload: Dict[str, Any]) -> None:
        _event(job, event_name, mode=job["mode"], **payload)

    try:
        if job["mode"] == "metadata":
            applied = await run_file_metadata_backfill(
                job_id=job_id, db_name=job["db_name"], chat_bot_id=job["chat_bot_id"],
                stop_event=job["stop_event"], event=report, pg_table=job["pg_table"],
                content_type=job["content_type"], dry_run=False,
                prepared_plan=job.get("metadata_plan") or [],
            )
        else:
            applied = await run_file_simhash_backfill(
                job_id=job_id, db_name=job["db_name"], chat_bot_id=job["chat_bot_id"],
                stop_event=job["stop_event"], event=report, pg_table=job["pg_table"],
                content_type=job["content_type"], dry_run=False,
                prepared_learn_list_ids=job.get("simhash_plan") or [],
            )
            applied.pop("_prepared_learn_list_ids", None)
        job["summary"] = {**(job.get("summary") or {}), **applied, "dry_run": False}
        job["status"] = "stopped" if job["stop_event"].is_set() else "completed"
        _set_completion_summary(job)
        _event(job, "backfill_completed", summary=job["summary"])
    except asyncio.CancelledError:
        job["status"] = "stopped"
        _event(job, "backfill_stopped")
        raise
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = f"{type(exc).__name__}: {exc}"
        _event(job, "backfill_failed", error=job["error"])
        logger.exception("[FileMaintenanceLab][apply_failed] job_id=%s", job_id)
    finally:
        job["completed_at"] = _now()
        if not job.get("completion_summary"):
            _set_completion_summary(job)
        job["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)


@router.get("", response_class=FileResponse)
@router.get("/", response_class=FileResponse)
async def page() -> FileResponse:
    if not _HTML_PATH.exists():
        raise HTTPException(status_code=404, detail="file crawl maintenance frontend missing")
    return FileResponse(
        _HTML_PATH,
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@router.post("/api/jobs")
async def start_job(request: Request) -> Dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON payload: {exc}") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON object payload required")
    db_name = _text(body.get("db_name"))
    chat_bot_id = _text(body.get("chat_bot_id"))
    pg_table = _text(body.get("pg_table"))
    target_domain = _text(body.get("target_domain"))
    try:
        content_type = _normalize_target_content_type(body.get("content_type"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    mode = _text(body.get("mode")).lower()
    if not db_name or not chat_bot_id:
        raise HTTPException(status_code=400, detail="db_name and chat_bot_id are required")
    if mode not in {"metadata", "simhash"}:
        raise HTTPException(status_code=400, detail="mode must be metadata or simhash")
    if mode == "metadata" and not target_domain:
        raise HTTPException(status_code=400, detail="target_domain_required")
    job_id = str(uuid.uuid4())
    job: Dict[str, Any] = {
        "job_id": job_id,
        "mode": mode,
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "pg_table": pg_table,
        "content_type": content_type,
        "target_domain": target_domain,
        "status": "queued",
        "created_at": _now(),
        "started_at": "",
        "completed_at": "",
        "elapsed_ms": None,
        "error": "",
        "summary": {},
        "progress": {},
        "stop_event": asyncio.Event(),
        "events": [],
    }
    _event(
        job,
        "backfill_queued",
        mode=mode,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        pg_table=pg_table,
        content_type=content_type,
        target_domain=target_domain,
    )
    _JOBS[job_id] = job
    job["task"] = asyncio.create_task(_run(job_id), name=f"file-crawl-maintenance:{mode}:{job_id}")
    _trim_finished()
    return _view(job)


@router.get("/api/jobs")
async def list_jobs() -> Dict[str, Any]:
    jobs = sorted((_view(job) for job in _JOBS.values()), key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return {"jobs": jobs}


@router.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> Dict[str, Any]:
    job = _JOBS.get(_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return _view(job, events=True)


@router.post("/api/jobs/{job_id}/approve")
async def approve_backfill(job_id: str) -> Dict[str, Any]:
    job = _JOBS.get(_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.get("status") != "awaiting_approval":
        raise HTTPException(status_code=409, detail="backfill dry-run approval is not pending")
    plan_key = "metadata_plan" if job.get("mode") == "metadata" else "simhash_plan"
    if not job.get(plan_key):
        raise HTTPException(status_code=409, detail="backfill dry-run produced no rows to apply")
    mode = _text(job.get("mode"))
    job["status"] = "applying"
    _event(
        job,
        f"{mode}_backfill_apply_queued",
        mode=mode,
        planned_rows=len(job.get(plan_key) or []),
    )
    job["task"] = asyncio.create_task(_apply_backfill_plan(_text(job_id)), name=f"file-crawl-maintenance:{mode}-apply:{job_id}")
    return _view(job)


@router.post("/api/jobs/{job_id}/stop")
async def stop_job(job_id: str) -> Dict[str, Any]:
    job = _JOBS.get(_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    job["stop_event"].set()
    task = job.get("task")
    if isinstance(task, asyncio.Task) and not task.done():
        task.cancel()
    if job.get("status") in {"queued", "awaiting_approval"}:
        job["status"] = "stopped"
        job["completed_at"] = _now()
    _event(job, "stop_requested")
    return _view(job)


@router.post("/api/modes/{mode}/stop")
async def stop_mode_jobs(mode: str) -> Dict[str, Any]:
    """Stop active maintenance jobs of one mode without touching crawl jobs."""
    normalized_mode = _text(mode).lower()
    if normalized_mode not in {"metadata", "simhash"}:
        raise HTTPException(status_code=400, detail="mode must be metadata or simhash")
    stopped_job_ids: List[str] = []
    for job in _JOBS.values():
        if job.get("mode") != normalized_mode or job.get("status") not in {"queued", "running", "awaiting_approval", "applying"}:
            continue
        job["stop_event"].set()
        task = job.get("task")
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
        if job.get("status") in {"queued", "awaiting_approval"}:
            job["status"] = "stopped"
            job["completed_at"] = _now()
        _event(job, "stop_requested", scope="mode")
        stopped_job_ids.append(_text(job.get("job_id")))
    return {"mode": normalized_mode, "stopped_job_ids": stopped_job_ids, "count": len(stopped_job_ids)}


@router.get("/api/jobs/{job_id}/events")
async def events(job_id: str) -> StreamingResponse:
    if _text(job_id) not in _JOBS:
        raise HTTPException(status_code=404, detail="job not found")

    async def stream():
        sent = 0
        last_heartbeat = 0.0
        while True:
            job = _JOBS.get(_text(job_id))
            if job is None:
                return
            entries: List[Dict[str, Any]] = list(job.get("events") or [])
            while sent < len(entries):
                yield f"event: maintenance\ndata: {json.dumps(entries[sent], ensure_ascii=False)}\n\n"
                sent += 1
            if job.get("status") in {"completed", "failed", "stopped"}:
                yield f"event: terminal\ndata: {json.dumps(_view(job), ensure_ascii=False)}\n\n"
                return
            now = time.monotonic()
            if now - last_heartbeat >= 5.0:
                heartbeat = {
                    "event": "execution_heartbeat",
                    "at": _now(),
                    "execution_health": _view(job).get("execution_health", {}),
                }
                yield f"event: maintenance\ndata: {json.dumps(heartbeat, ensure_ascii=False)}\n\n"
                last_heartbeat = now
            else:
                yield ": keepalive\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
