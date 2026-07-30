from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from db.mysql_db_config import mysql_execute_query
from utils.db_name import resolve_db_name

from backend.shared.title_only_mode import (
    _get_table_columns_lower,
    _is_weak_title,
    _learn_content_column,
    _load_partial_title_target_rows,
    _load_title_target_rows,
    _resolve_candidate_title,
    _row_needs_title_update,
    _safe_int,
)

logger = logging.getLogger("backend.shared.title_candidate_mode")

TITLE_CANDIDATE_JOBS: Dict[str, Dict[str, Any]] = {}
TITLE_CANDIDATE_TASKS: Dict[str, asyncio.Task] = {}
TITLE_CANDIDATE_STOP_REQUESTS: set[str] = set()


def _resolve_chat_bot_id(data: Dict[str, Any]) -> str:
    chat_bot_id = str((data or {}).get("chat_bot_id") or "").strip()
    if chat_bot_id:
        return chat_bot_id
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    return str(meta.get("chat_bot_id") or "").strip()


def _has_explicit_title_candidate_urls(data: Dict[str, Any]) -> bool:
    raw = (data or {}).get("start_urls_override")
    return isinstance(raw, list) and bool(raw)


def _is_blank_or_short_numeric_title(value: Any) -> bool:
    text = re.sub(r"\s+", "", str(value or "")).strip()
    return not text or bool(re.fullmatch(r"\d{1,2}", text))


def _is_title_candidate_repair_target(row: Dict[str, Any]) -> bool:
    subject = row.get("subject")
    web_title = row.get("web_title")
    return _is_blank_or_short_numeric_title(subject) or _is_blank_or_short_numeric_title(web_title)


async def _load_learn_list_row_for_title_apply(
    *,
    db_name: str,
    learn_table: str,
    columns: set[str],
    content_col: str,
    row_id: int,
) -> Dict[str, Any]:
    select_cols = ["id", f"`{content_col}` AS content_value"]
    for col in ("subject", "web_title", "content_type", "content_created_at", "created_at"):
        if col in columns:
            if col in {"content_created_at", "created_at"}:
                select_cols.append(f"CAST(`{col}` AS CHAR) AS `{col}`")
            else:
                select_cols.append(f"`{col}`")
    rows = await mysql_execute_query(
        f"SELECT {', '.join(select_cols)} FROM `{learn_table}` WHERE id = %s LIMIT 1",
        (row_id,),
        fetch=True,
        dbname=db_name,
    )
    row = rows[0] if rows else {}
    return dict(row) if isinstance(row, dict) else {}


def _schedule_title_candidate_relearn_workflow(
    *,
    db_name: str,
    chat_bot_id: str,
    job_id: str,
    targets: List[Dict[str, Any]],
) -> str:
    if not targets:
        return ""
    try:
        from backend.board.board_content_workflow import BoardContentWorkflow
        from backend.shared.crawler_state import crawler_state
        from backend.shared.workflow_runner import run_workflow_task

        relearn_job_id = f"{job_id or 'title-candidate'}-relearn-{uuid4().hex[:8]}"
        workflow = BoardContentWorkflow()
        workflow.job_id = relearn_job_id
        workflow.db_name = db_name
        workflow.chat_bot_id = chat_bot_id
        workflow.enable_learning = True
        workflow.enable_db_save = True
        workflow.colle = "board"
        workflow.start_urls_override_source = "partial_content_relearn"
        workflow.content_relearn_mode = True
        workflow._force_duplicate_repair_runtime = False
        start_urls = [dict(item, force_relearn=True, type="partial_content_relearn") for item in targets]
        crawler_state.workflows[relearn_job_id] = workflow
        task = asyncio.create_task(
            run_workflow_task(
                workflow,
                start_urls,
                None,
                None,
                relearn_job_id,
                "",
                db_name,
                chat_bot_id,
                False,
            ),
            name=f"title_candidate_relearn:{relearn_job_id}",
        )
        crawler_state.workflow_tasks[relearn_job_id] = task

        def _done(tt, jid=relearn_job_id):
            crawler_state.workflow_tasks.pop(jid, None)
            try:
                logger.info(
                    "[TitleCandidates][RelearnDebug][workflow_done] job_id=%s cancelled=%s exc=%s",
                    jid,
                    tt.cancelled(),
                    None if tt.cancelled() else tt.exception(),
                )
            except Exception:
                logger.debug("[TitleCandidates][RelearnDebug] workflow_done logging failed", exc_info=True)

        task.add_done_callback(_done)
        if job_id:
            TITLE_CANDIDATE_JOBS[job_id] = {
                **TITLE_CANDIDATE_JOBS.get(job_id, {}),
                "relearn_job_id": relearn_job_id,
                "updated_at": time.time(),
            }
        logger.info(
            "[TitleCandidates][RelearnDebug][workflow_scheduled] source_job_id=%s relearn_job_id=%s db=%s chat_bot_id=%s targets=%s",
            job_id,
            relearn_job_id,
            db_name,
            chat_bot_id,
            len(start_urls),
        )
        return relearn_job_id
    except Exception as exc:
        logger.exception(
            "[TitleCandidates][RelearnDebug][schedule_error] job_id=%s db=%s chat_bot_id=%s targets=%s err=%s",
            job_id,
            db_name,
            chat_bot_id,
            len(targets or []),
            exc,
        )
        return ""


async def _load_title_candidate_rows(
    *,
    db_name: str,
    learn_table: str,
    columns: set[str],
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    content_col = _learn_content_column(columns)
    if not content_col or "id" not in columns:
        logger.info(
            "[TitleCandidates][TargetDebug] skipped | job_id=%s db=%s table=%s reason=missing_id_or_content columns=%s",
            str((data or {}).get("job_id") or ""),
            db_name,
            learn_table,
            sorted(columns),
        )
        return []
    if _has_explicit_title_candidate_urls(data):
        rows = await _load_title_target_rows(
            db_name=db_name,
            learn_table=learn_table,
            columns=columns,
            data=data,
        )
        logger.info(
            "[TitleCandidates][TargetDebug] explicit_url_targets | job_id=%s db=%s table=%s rows=%s",
            str((data or {}).get("job_id") or ""),
            db_name,
            learn_table,
            len(rows),
        )
        return rows

    rows = await _load_partial_title_target_rows(
        db_name=db_name,
        learn_table=learn_table,
        columns=columns,
        content_col=content_col,
        data=data,
    )
    before_filter = len(rows)
    rows = [row for row in rows if _is_title_candidate_repair_target(row)]
    logger.info(
        "[TitleCandidates][TargetDebug] period_url_targets | job_id=%s db=%s table=%s rows=%s before_filter=%s target_date=%s mode=blank_or_1_2_digit_title",
        str((data or {}).get("job_id") or ""),
        db_name,
        learn_table,
        len(rows),
        before_filter,
        (data or {}).get("target_date") or (data or {}).get("start_urls_target_date"),
    )
    return rows


async def build_title_candidate_preview(
    data: Dict[str, Any],
    progress_cb: Optional[Any] = None,
) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user") or "dev_user"
    chat_bot_id = _resolve_chat_bot_id(data)
    if not chat_bot_id:
        raise RuntimeError("title candidate preview requires chat_bot_id")

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    columns = await _get_table_columns_lower(db_name, str(learn_table or "")) if learn_table else set()
    rows = await _load_title_candidate_rows(
        db_name=db_name,
        learn_table=str(learn_table or ""),
        columns=columns,
        data=data,
    )
    total = len(rows)
    try:
        timeout_raw = float(os.getenv("TITLE_ONLY_FETCH_TIMEOUT_SEC", "10") or "10")
    except Exception:
        timeout_raw = 10.0
    timeout_sec = max(3.0, min(timeout_raw, 30.0))
    concurrency = max(1, min(_safe_int(data.get("title_parse_concurrency"), _safe_int(os.getenv("TITLE_ONLY_CONCURRENCY"), 5)), 32))
    semaphore = asyncio.Semaphore(concurrency)
    items: List[Dict[str, Any]] = []
    parsed_count = 0
    would_update_count = 0

    logger.info(
        "[TitleCandidates][DryRunDebug][start] job_id=%s db=%s chat_bot_id=%s table=%s rows=%s target_date=%s concurrency=%s",
        str((data or {}).get("job_id") or ""),
        db_name,
        chat_bot_id,
        learn_table,
        total,
        (data or {}).get("target_date") or (data or {}).get("start_urls_target_date"),
        concurrency,
    )

    async def _preview_one(row: Dict[str, Any]) -> Dict[str, Any]:
        row_id = _safe_int(row.get("id"), 0)
        content_value = str(row.get("content_value") or "").strip()
        if row_id <= 0:
            return {
                "id": row_id,
                "content": content_value,
                "subject": str(row.get("subject") or ""),
                "web_title": str(row.get("web_title") or ""),
                "candidate_title": "",
                "would_update": False,
                "status": "skipped",
                "reason": str(row.get("_title_skip_reason") or "learn_list_missing"),
            }
        async with semaphore:
            candidate_title, source = await _resolve_candidate_title(
                row,
                fetch_missing=True,
                timeout_sec=timeout_sec,
            )
        would_update = bool(candidate_title and _row_needs_title_update(row, candidate_title))
        return {
            "id": row_id,
            "content": content_value,
            "subject": str(row.get("subject") or ""),
            "web_title": str(row.get("web_title") or ""),
            "candidate_title": candidate_title,
            "would_update": would_update,
            "status": "parsed" if candidate_title else "skipped",
            "reason": source if candidate_title else "candidate_title_empty",
        }

    for row in rows:
        if str((data or {}).get("job_id") or "") in TITLE_CANDIDATE_STOP_REQUESTS:
            logger.info(
                "[TitleCandidates][DryRunDebug][stopped] job_id=%s parsed=%s total=%s",
                str((data or {}).get("job_id") or ""),
                parsed_count,
                total,
            )
            return {
                "status": "cancelled",
                "job_id": str((data or {}).get("job_id") or ""),
                "db_name": db_name,
                "chat_bot_id": chat_bot_id,
                "learn_list_table": learn_table,
                "items": items,
                "total_count": total,
                "parsed_count": parsed_count,
                "would_update_count": would_update_count,
                "dry_run": True,
                "source": "title_candidates",
                "stop_requested": True,
            }
        try:
            item = await _preview_one(row)
        except Exception as exc:
            item = {
                "id": _safe_int(row.get("id"), 0),
                "content": str(row.get("content_value") or ""),
                "subject": str(row.get("subject") or ""),
                "web_title": str(row.get("web_title") or ""),
                "candidate_title": "",
                "would_update": False,
                "status": "error",
                "reason": str(exc),
            }
        items.append(item)
        parsed_count += 1
        if item.get("would_update"):
            would_update_count += 1
        if progress_cb:
            await progress_cb(
                {
                    "status": "running",
                    "items": list(items),
                    "total_count": total,
                    "parsed_count": parsed_count,
                    "would_update_count": would_update_count,
                    "learn_list_table": learn_table,
                }
            )

    logger.info(
        "[TitleCandidates][DryRunDebug][completed] job_id=%s db=%s table=%s total=%s parsed=%s would_update=%s",
        str((data or {}).get("job_id") or ""),
        db_name,
        learn_table,
        total,
        parsed_count,
        would_update_count,
    )
    return {
        "status": "completed",
        "job_id": str((data or {}).get("job_id") or ""),
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "learn_list_table": learn_table,
        "items": items,
        "total_count": total,
        "parsed_count": parsed_count,
        "would_update_count": would_update_count,
        "dry_run": True,
        "source": "title_candidates",
    }


async def apply_title_candidate_updates(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user") or "dev_user"
    chat_bot_id = _resolve_chat_bot_id(data)
    items = data.get("items") if isinstance(data.get("items"), list) else []
    if not chat_bot_id:
        raise RuntimeError("title candidate apply requires chat_bot_id")
    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    columns = await _get_table_columns_lower(db_name, str(learn_table or "")) if learn_table else set()
    if not learn_table or "id" not in columns:
        raise RuntimeError("learn_list table/id column not found")
    content_col = _learn_content_column(columns)
    if not content_col:
        raise RuntimeError("learn_list content column not found")

    applied_ids: List[int] = []
    relearn_targets: List[Dict[str, Any]] = []
    skipped_count = 0
    job_id = str((data or {}).get("job_id") or "")
    for item in items:
        if not isinstance(item, dict):
            skipped_count += 1
            continue
        row_id = _safe_int(item.get("id"), 0)
        candidate_title = re.sub(r"\s+", " ", str(item.get("candidate_title") or "").strip())
        if row_id <= 0 or _is_weak_title(candidate_title):
            skipped_count += 1
            continue
        row = await _load_learn_list_row_for_title_apply(
            db_name=db_name,
            learn_table=str(learn_table),
            columns=columns,
            content_col=content_col,
            row_id=row_id,
        )
        source_url = str(row.get("content_value") or item.get("content") or "").strip()
        if not source_url:
            skipped_count += 1
            logger.info(
                "[TitleCandidates][DryRunDebug][apply_skip] job_id=%s row_id=%s reason=source_url_missing",
                job_id,
                row_id,
            )
            continue
        if not _is_title_candidate_repair_target(row):
            skipped_count += 1
            logger.info(
                "[TitleCandidates][DryRunDebug][apply_skip] job_id=%s row_id=%s reason=current_title_not_target subject=%r web_title=%r candidate=%r",
                job_id,
                row_id,
                str(row.get("subject") or "")[:160],
                str(row.get("web_title") or "")[:160],
                candidate_title[:160],
            )
            continue
        update_sets: List[str] = []
        update_params: List[Any] = []
        if "subject" in columns:
            update_sets.append("`subject` = %s")
            update_params.append(candidate_title)
        if "web_title" in columns:
            update_sets.append("`web_title` = %s")
            update_params.append(candidate_title)
        if not update_sets:
            skipped_count += 1
            continue
        update_params.append(row_id)
        from backend.shared.db_write_queue import run_db_write

        await run_db_write(
            "postprocess.title_candidate_update",
            lambda: mysql_execute_query(
                f"UPDATE `{learn_table}` SET {', '.join(update_sets)} WHERE id = %s",
                tuple(update_params),
                dbname=db_name,
            ),
        )
        applied_ids.append(row_id)
        relearn_targets.append(
            {
                "url": source_url,
                "title": candidate_title,
                "subject": candidate_title,
                "learn_list_id": row_id,
                "force_relearn": True,
                "type": "partial_content_relearn",
            }
        )

    relearn_job_id = _schedule_title_candidate_relearn_workflow(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        job_id=job_id,
        targets=relearn_targets,
    )
    relearn_failed_count = 0 if relearn_job_id or not relearn_targets else len(relearn_targets)

    logger.info(
        "[TitleCandidates][DryRunDebug][apply] job_id=%s db=%s chat_bot_id=%s table=%s requested=%s applied=%s relearn_scheduled=%s relearn_job_id=%s relearn_failed=%s skipped=%s",
        job_id,
        db_name,
        chat_bot_id,
        learn_table,
        len(items),
        len(applied_ids),
        len(relearn_targets) if relearn_job_id else 0,
        relearn_job_id,
        relearn_failed_count,
        skipped_count,
    )
    return {
        "status": "success",
        "job_id": job_id,
        "learn_list_table": learn_table,
        "applied_count": len(applied_ids),
        "applied_ids": applied_ids,
        "skipped_count": skipped_count,
        "relearn_count": len(relearn_targets) if relearn_job_id else 0,
        "relearn_ids": applied_ids if relearn_job_id else [],
        "relearn_job_id": relearn_job_id,
        "relearn_failed_count": relearn_failed_count,
        "dry_run": False,
        "source": "title_candidates_apply",
    }


async def _run_title_candidate_preview_job(job_id: str, body: Dict[str, Any]) -> None:
    async def _progress(payload: Dict[str, Any]) -> None:
        TITLE_CANDIDATE_JOBS[job_id] = {
            **TITLE_CANDIDATE_JOBS.get(job_id, {}),
            **payload,
            "job_id": job_id,
            "parse_job_id": job_id,
            "updated_at": time.time(),
        }

    try:
        logger.info(
            "[TitleCandidates][FlowDebug][start] job_id=%s db=%s chat_bot_id=%s target_date=%s limit=%s",
            job_id,
            body.get("db_name"),
            body.get("chat_bot_id"),
            body.get("target_date"),
            body.get("limit"),
        )
        result = await build_title_candidate_preview(body, progress_cb=_progress)
        TITLE_CANDIDATE_JOBS[job_id] = {
            **result,
            "job_id": job_id,
            "parse_job_id": job_id,
            "updated_at": time.time(),
        }
    except asyncio.CancelledError:
        logger.info("[TitleCandidates][FlowDebug][cancelled] job_id=%s", job_id)
        TITLE_CANDIDATE_JOBS[job_id] = {
            **TITLE_CANDIDATE_JOBS.get(job_id, {}),
            "status": "cancelled",
            "job_id": job_id,
            "parse_job_id": job_id,
            "message": "제목후보정 작업이 중단되었습니다.",
            "stop_requested": True,
            "updated_at": time.time(),
        }
        raise
    except Exception as exc:
        logger.exception("[TitleCandidates][FlowDebug][error] job_id=%s err=%s", job_id, exc)
        TITLE_CANDIDATE_JOBS[job_id] = {
            **TITLE_CANDIDATE_JOBS.get(job_id, {}),
            "status": "error",
            "job_id": job_id,
            "parse_job_id": job_id,
            "message": str(exc),
            "updated_at": time.time(),
        }
    finally:
        TITLE_CANDIDATE_TASKS.pop(job_id, None)


def queue_title_candidate_preview(data: Dict[str, Any]) -> Dict[str, Any]:
    body = dict(data or {})
    db_name = resolve_db_name(body, default="dev_user") or "dev_user"
    chat_bot_id = _resolve_chat_bot_id(body)
    if not chat_bot_id:
        raise ValueError("chat_bot_id is required")
    job_id = str(body.get("job_id") or body.get("parse_job_id") or f"title_candidates_{uuid4()}").strip()
    body["db_name"] = db_name
    body["chat_bot_id"] = chat_bot_id
    body["job_id"] = job_id
    TITLE_CANDIDATE_STOP_REQUESTS.discard(job_id)
    raw_subject_filter = body.get("subject_filter") or body.get("title_subject_filter")
    subject_filter_value = ""
    if isinstance(raw_subject_filter, dict):
        subject_filter_value = str(raw_subject_filter.get("value") or raw_subject_filter.get("subject") or raw_subject_filter.get("text") or "").strip()
    else:
        subject_filter_value = str(raw_subject_filter or "").strip()
    if subject_filter_value and body.get("title_ignore_date_filter") is None:
        body["title_ignore_date_filter"] = True
    if body.get("limit") or body.get("title_limit"):
        body["title_limit"] = body.get("limit") or body.get("title_limit")
    else:
        body["title_limit_disabled"] = True
    TITLE_CANDIDATE_JOBS[job_id] = {
        "status": "queued",
        "job_id": job_id,
        "parse_job_id": job_id,
        "items": [],
        "total_count": 0,
        "parsed_count": 0,
        "would_update_count": 0,
        "dry_run": True,
        "updated_at": time.time(),
    }
    task = asyncio.create_task(_run_title_candidate_preview_job(job_id, body), name=f"title_candidates:{job_id}")
    TITLE_CANDIDATE_TASKS[job_id] = task
    return dict(TITLE_CANDIDATE_JOBS[job_id])


def get_title_candidate_preview_status(data: Dict[str, Any]) -> Dict[str, Any]:
    job_id = str((data or {}).get("job_id") or (data or {}).get("parse_job_id") or "").strip()
    if not job_id:
        raise ValueError("job_id is required")
    state = TITLE_CANDIDATE_JOBS.get(job_id)
    if not state:
        return {"status": "missing", "job_id": job_id, "items": []}
    return dict(state)


async def apply_title_candidate_preview(data: Dict[str, Any]) -> Dict[str, Any]:
    result = await apply_title_candidate_updates(data)
    job_id = str((data or {}).get("job_id") or (data or {}).get("parse_job_id") or "").strip()
    if job_id and job_id in TITLE_CANDIDATE_JOBS:
        TITLE_CANDIDATE_JOBS[job_id] = {
            **TITLE_CANDIDATE_JOBS[job_id],
            "status": "applied" if int(result.get("applied_count") or 0) > 0 else "skipped",
            "apply_result": result,
            "updated_at": time.time(),
        }
    return result


async def request_title_candidate_preview_stop(data: Dict[str, Any]) -> Dict[str, Any]:
    job_id = str((data or {}).get("job_id") or (data or {}).get("parse_job_id") or "").strip()
    stop_all = bool((data or {}).get("all") or (data or {}).get("stop_all") or (data or {}).get("force_all") or not job_id)
    if stop_all:
        target_job_ids = sorted(set(TITLE_CANDIDATE_TASKS.keys()) | set(TITLE_CANDIDATE_JOBS.keys()))
    else:
        target_job_ids = [job_id] if job_id else []
    cancelled_tasks = 0
    missing = []
    for target_job_id in target_job_ids:
        TITLE_CANDIDATE_STOP_REQUESTS.add(target_job_id)
        task = TITLE_CANDIDATE_TASKS.get(target_job_id)
        if task and not task.done():
            task.cancel()
            cancelled_tasks += 1
        elif target_job_id not in TITLE_CANDIDATE_JOBS:
            missing.append(target_job_id)
        TITLE_CANDIDATE_JOBS[target_job_id] = {
            **TITLE_CANDIDATE_JOBS.get(target_job_id, {}),
            "status": "cancelled",
            "job_id": target_job_id,
            "parse_job_id": target_job_id,
            "message": "제목후보정 작업이 중단되었습니다.",
            "stop_requested": True,
            "updated_at": time.time(),
        }

    relearn_stopped = 0
    try:
        from backend.shared.crawler_state import crawler_state

        known_relearn_ids = {
            str((state or {}).get("relearn_job_id") or "").strip()
            for state in TITLE_CANDIDATE_JOBS.values()
            if isinstance(state, dict)
        }
        for running_job_id in list(crawler_state.workflow_tasks.keys()):
            running_job_id_s = str(running_job_id or "")
            if (
                running_job_id_s in known_relearn_ids
                or "-relearn-" in running_job_id_s
                and running_job_id_s.startswith(tuple(f"{jid}-" for jid in target_job_ids if jid))
                or stop_all
                and "-relearn-" in running_job_id_s
                and running_job_id_s.startswith("title_candidates_")
            ):
                task = crawler_state.workflow_tasks.get(running_job_id)
                if task and not task.done():
                    task.cancel()
                    relearn_stopped += 1
                workflow = crawler_state.workflows.get(running_job_id)
                if workflow is not None and hasattr(workflow, "_force_hard_stop"):
                    try:
                        ret = workflow._force_hard_stop(reason="title_candidates_stop")
                        if asyncio.iscoroutine(ret):
                            await ret
                    except Exception:
                        logger.debug("[TitleCandidates][Stop] workflow hard stop failed", exc_info=True)
    except Exception:
        logger.debug("[TitleCandidates][Stop] relearn workflow stop skipped", exc_info=True)

    logger.info(
        "[TitleCandidates][Stop] requested | job_id=%s all=%s targets=%s cancelled_tasks=%s relearn_stopped=%s missing=%s",
        job_id,
        stop_all,
        len(target_job_ids),
        cancelled_tasks,
        relearn_stopped,
        missing[:10],
    )
    return {
        "status": "cancelled",
        "job_id": job_id,
        "all": stop_all,
        "target_job_ids": target_job_ids,
        "cancelled_tasks": cancelled_tasks,
        "relearn_stopped": relearn_stopped,
        "missing": missing,
        "source": "title_candidates_stop",
    }


__all__ = [
    "apply_title_candidate_preview",
    "apply_title_candidate_updates",
    "build_title_candidate_preview",
    "get_title_candidate_preview_status",
    "queue_title_candidate_preview",
    "request_title_candidate_preview_stop",
]
