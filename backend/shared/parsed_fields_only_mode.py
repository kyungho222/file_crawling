from __future__ import annotations

import logging
import json
from datetime import datetime
from typing import Any, Dict, Optional

from backend.shared.duplicate_category_only_mode import normalize_duplicate_repair_request_mode
from backend.shared.redis_sse_service import send_message_to_redis_sse, update_state_only
from backend.shared.summary_only_mode import normalize_duplicate_summary_request_mode
from backend.shared.title_only_mode import normalize_duplicate_title_request_mode
from db.crawl_db_manager import update_crawling_log_counters
from utils.db_name import resolve_db_name
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.shared.parsed_fields_only_mode")


PARSED_FIELDS_MODE_VALUES = {"on", "parsed_fields", "author", "date", "reg_date", "content_created_at"}
REPAIR_ONLY_CRAWL_MODES = {
    "duplicate_repair_only",
    "duplicate_parsed_fields_only",
    "parsed_fields_only",
    "author_date_only",
}


def normalize_duplicate_parsed_fields_request_mode(raw_value: Optional[Any]) -> str:
    value = str(raw_value or "").strip().lower()
    if value in {"on", "parsed_fields"}:
        return "parsed_fields"
    if value == "author":
        return "author"
    if value in {"date", "reg_date", "content_created_at"}:
        return "date"
    return "off"


def _bool_from_payload(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def _positive_int_from_payload(*values: Any) -> int:
    out = 0
    for value in values:
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed > out:
            out = parsed
    return out


def _resolve_repair_modes(data: Dict[str, Any]) -> tuple[str, str, str, str]:
    repair_mode = normalize_duplicate_repair_request_mode(
        data.get("duplicate_repair_mode")
        or data.get("duplicateRepairMode")
        or data.get("board_duplicate_repair")
        or data.get("duplicate_repair")
    )
    summary_mode = normalize_duplicate_summary_request_mode(
        data.get("duplicate_summary_mode")
        or data.get("duplicateSummaryMode")
        or data.get("board_duplicate_summary")
        or data.get("duplicate_summary")
    )
    title_mode = normalize_duplicate_title_request_mode(
        data.get("duplicate_title_mode")
        or data.get("duplicateTitleMode")
        or data.get("board_duplicate_title")
        or data.get("duplicate_title")
        or data.get("title_mode")
        or data.get("titleMode")
    )
    parsed_mode = normalize_duplicate_parsed_fields_request_mode(
        data.get("duplicate_parsed_fields_mode")
        or data.get("duplicateParsedFieldsMode")
        or data.get("board_duplicate_parsed_fields")
        or data.get("duplicate_parsed_fields")
    )
    if parsed_mode != "off":
        if repair_mode == "category":
            repair_mode = "category_parsed_fields"
        elif repair_mode == "off":
            repair_mode = "parsed_fields"
    return repair_mode, summary_mode, title_mode, parsed_mode


def is_duplicate_repair_only_request(data: Dict[str, Any]) -> bool:
    payload = data or {}
    mode = str(payload.get("crawl_mode") or "").strip().lower()
    if mode in REPAIR_ONLY_CRAWL_MODES:
        return True
    if _bool_from_payload(payload.get("duplicate_repair_only")):
        return True
    if str(payload.get("colle") or "board").strip().lower() != "board":
        return False
    repair_mode, summary_mode, title_mode, parsed_mode = _resolve_repair_modes(payload)
    if _bool_from_payload(payload.get("postprocess_only")) and any(
        mode_value != "off"
        for mode_value in (repair_mode, summary_mode, title_mode, parsed_mode)
    ):
        return True
    return any(
        mode_value != "off"
        for mode_value in (repair_mode, summary_mode, title_mode, parsed_mode)
    )


def is_parsed_fields_only_request(data: Dict[str, Any]) -> bool:
    return is_duplicate_repair_only_request(data)


def _first_content_url(data: Dict[str, Any]) -> str:
    contents = (data or {}).get("contents")
    value = ""
    if isinstance(contents, list) and contents:
        value = str(contents[0] or "").strip()
    elif isinstance(contents, str):
        value = contents.strip()
    if not value:
        value = str((data or {}).get("content") or (data or {}).get("url") or "").strip()
    return ensure_url_scheme(value) if value else ""


def _resolve_chat_bot_id(data: Dict[str, Any]) -> str:
    meta = (data or {}).get("metadata") if isinstance((data or {}).get("metadata"), dict) else {}
    return str((data or {}).get("chat_bot_id") or meta.get("chat_bot_id") or "").strip()


def _parse_target_date_bound(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _target_date_bounds(data: Dict[str, Any]) -> tuple[Optional[datetime], Optional[datetime]]:
    target_date = (data or {}).get("target_date")
    if isinstance(target_date, list) and len(target_date) >= 2:
        return _parse_target_date_bound(target_date[0]), _parse_target_date_bound(target_date[1])
    return None, None


async def _load_category_rule_object(
    data: Dict[str, Any],
    *,
    chat_bot_id: str,
    db_name: str,
    source_url: str,
) -> Optional[Dict[str, Any]]:
    if not (chat_bot_id and db_name):
        logger.info(
            "[DuplicateRepairOnlyDebug][category_rules_skip] reason=missing_context db=%s chat_bot_id=%s",
            db_name,
            bool(chat_bot_id),
        )
        return None
    try:
        from backend.shared.pre_explored_url import get_category_url_pattern_raw

        raw_rules = await get_category_url_pattern_raw(
            str(chat_bot_id),
            str((data or {}).get("method") or "period"),
            str(db_name),
            contents_url=source_url or None,
        )
        if not raw_rules:
            logger.warning(
                "[DuplicateRepairOnlyDebug][category_rules_empty] db=%s chat_bot_id=%s contents_url=%s",
                db_name,
                chat_bot_id,
                source_url[:220],
            )
            return None
        category_obj = json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
        if not isinstance(category_obj, dict):
            logger.warning(
                "[DuplicateRepairOnlyDebug][category_rules_invalid] db=%s chat_bot_id=%s type=%s",
                db_name,
                chat_bot_id,
                type(category_obj).__name__,
            )
            return None
        try:
            rule_count = len((category_obj or {}).get("rules") or [])
        except Exception:
            rule_count = 0
        logger.info(
            "[DuplicateRepairOnlyDebug][category_rules_loaded] db=%s chat_bot_id=%s rules=%s contents_url=%s",
            db_name,
            chat_bot_id,
            rule_count,
            source_url[:220],
        )
        return category_obj
    except Exception as exc:
        logger.warning(
            "[DuplicateRepairOnlyDebug][category_rules_failed] db=%s chat_bot_id=%s contents_url=%s err=%s",
            db_name,
            chat_bot_id,
            source_url[:220],
            exc,
        )
        return None


def _category_repair_requested(repair_mode: str) -> bool:
    return str(repair_mode or "").strip().lower() in {
        "category",
        "category_parsed_fields",
        "on",
    }


async def run_duplicate_repair_only(data: Dict[str, Any]) -> Dict[str, Any]:
    from backend.board.board_content_workflow import BoardContentWorkflow

    db_name = resolve_db_name(data, default="dev_user")
    job_id = str((data or {}).get("job_id") or "").strip()
    chat_bot_id = _resolve_chat_bot_id(data)
    source_url = _first_content_url(data)
    repair_mode, summary_mode, title_mode, parsed_mode = _resolve_repair_modes(data or {})
    created_at_start, created_at_end = _target_date_bounds(data)

    logger.info(
        "[DuplicateRepairOnlyDebug][route] job_id=%s db=%s chat_bot_id=%s repair_mode=%s summary_mode=%s title_mode=%s parsed_mode=%s created_at_start=%s created_at_end=%s url=%s",
        job_id,
        db_name,
        chat_bot_id,
        repair_mode,
        summary_mode,
        title_mode,
        parsed_mode,
        created_at_start,
        created_at_end,
        source_url[:220],
    )

    payload_start = {
        "status": "running",
        "event": "duplicate_repair_only_started",
        "job_id": job_id,
        "account_name": db_name,
        "source": "duplicate_repair_only",
        "h3": "후보정 처리 중",
        "scan_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
    }
    if job_id:
        await update_state_only(job_id=job_id, account_name=db_name, payload=payload_start)
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=payload_start)

    workflow = BoardContentWorkflow()
    workflow.job_id = job_id
    workflow.db_name = db_name
    workflow.chat_bot_id = chat_bot_id
    workflow.enable_db_save = True
    workflow.colle = "board"
    workflow.target_url = source_url
    workflow.duplicate_repair_mode = repair_mode
    workflow.duplicate_summary_mode = summary_mode
    workflow.duplicate_title_mode = title_mode
    workflow.duplicate_title_dry_run = _bool_from_payload(
        data.get("duplicate_title_dry_run")
        or data.get("duplicateTitleDryRun")
        or data.get("title_dry_run")
        or data.get("titleDryRun")
    )
    workflow.duplicate_parsed_fields_mode = parsed_mode
    repair_concurrency = _positive_int_from_payload(
        data.get("duplicate_repair_concurrency"),
        data.get("duplicateRepairConcurrency"),
        data.get("summary_only_concurrency"),
        data.get("summaryOnlyConcurrency"),
        data.get("title_only_concurrency"),
        data.get("titleOnlyConcurrency"),
    )
    if repair_concurrency > 0:
        workflow.duplicate_repair_concurrency = repair_concurrency
        workflow.summary_only_concurrency = repair_concurrency
    workflow._force_duplicate_repair_runtime = True
    workflow._force_duplicate_repair_sources = {"learn_list"}
    workflow.start_urls_override_source = "learn_list"
    workflow.duplicate_repair_created_at_start = created_at_start
    workflow.duplicate_repair_created_at_end = created_at_end
    if _category_repair_requested(repair_mode):
        workflow._category_rule_obj_cache = await _load_category_rule_object(
            data or {},
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            source_url=source_url,
        )
    else:
        workflow._category_rule_obj_cache = None
        logger.info(
            "[DuplicateRepairOnlyDebug][category_rules_skip] reason=category_repair_off repair_mode=%s parsed_mode=%s",
            repair_mode,
            parsed_mode,
        )
    try:
        workflow._configure_path_scope([source_url] if source_url else [], contents_url=source_url, start_urls=[])
    except Exception as exc:
        logger.debug(
            "[DuplicateRepairOnlyDebug][scope_skip] job_id=%s url=%s err=%s",
            job_id,
            source_url[:180],
            exc,
        )

    logger.info(
        "[DuplicateRepairOnlyDebug][start] job_id=%s source=learn_list features={repair:%s,summary:%s,title:%s,parsed:%s} title_dry_run=%s",
        job_id,
        repair_mode,
        summary_mode,
        title_mode,
        parsed_mode,
        workflow.duplicate_title_dry_run,
    )
    stats = await workflow._run_duplicate_learn_list_repair_source()
    await workflow._await_board_pipeline_tail_tasks()
    logger.info(
        "[DuplicateRepairOnlyDebug][done] job_id=%s stats=%s",
        job_id,
        stats,
    )

    scan_count = int(stats.get("scanned") or 0)
    save_count = int(
        stats.get("updated")
        or stats.get("category_updated")
        or stats.get("parsed_fields_updated")
        or stats.get("summary_updated")
        or stats.get("title_updated")
        or stats.get("scheduled")
        or 0
    )
    payload_done = {
        "status": "completed",
        "event": "workflow_completed",
        "job_id": job_id,
        "account_name": db_name,
        "source": "duplicate_repair_only",
        "h3": "후보정 처리 완료",
        "scan_count": scan_count,
        "collection_count": scan_count,
        "save_count": save_count,
        "study_count": 0,
        "stats": stats,
    }
    if job_id:
        try:
            await update_crawling_log_counters(
                job_id=job_id,
                scan=scan_count,
                collection=scan_count,
                saved=save_count,
                study=0,
                dbname=db_name,
                status="completed",
            )
        except Exception as exc:
            logger.warning("[DuplicateRepairOnlyDebug][crawling_log_update_failed] job_id=%s err=%s", job_id, exc)
        await update_state_only(job_id=job_id, account_name=db_name, payload=payload_done)
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=payload_done)
    return stats


async def run_parsed_fields_only(data: Dict[str, Any]) -> Dict[str, Any]:
    return await run_duplicate_repair_only(data)


__all__ = [
    "is_duplicate_repair_only_request",
    "is_parsed_fields_only_request",
    "normalize_duplicate_parsed_fields_request_mode",
    "run_duplicate_repair_only",
    "run_parsed_fields_only",
]
