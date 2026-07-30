"""
Workflow assembly after URL and date resolution.

This module is shared by the HTTP dispatcher and Celery worker so both paths
build board/file workflows with the same options.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from backend.board.board_crawl_module import create_board_crawl_workflow
from backend.file.file_crawl_module import create_file_crawl_workflow
from backend.file.integrated_workflow import IntegratedWorkflow
from backend.shared.basic_crawling_flow import (
    apply_basic_crawling_flow_to_payload,
    apply_basic_crawling_flow_to_workflow,
    build_basic_crawling_flow_config,
)
from backend.shared.crawl_request_config import CrawlRequestConfig
from backend.shared.crawl_shared import resolve_stream_matched_rules_only
from backend.shared.duplicate_category_only_mode import normalize_duplicate_repair_request_mode
from backend.shared.summary_only_mode import normalize_duplicate_summary_request_mode
from backend.shared.title_only_mode import normalize_duplicate_title_request_mode
from backend.shared.pre_explored_url import resolve_workflow_class_for_colle

logger = logging.getLogger("backend.shared.workflow_dispatch_assembly")

_HANGUL_RE = re.compile(r"[가-힣]")
_EMPTY_UI_TOKENS = {"", "undefined", "null", "none", "nan"}


def _clean_ui_text(value: Any) -> str:
    try:
        text = str(value if value is not None else "").strip()
    except Exception:
        return ""
    if text.lower() in _EMPTY_UI_TOKENS:
        return ""
    fixed = _maybe_fix_mojibake(text)
    return str(fixed or "").strip()


def _clean_optional_text(value: Any) -> Optional[str]:
    text = _clean_ui_text(value)
    return text or None


def _maybe_fix_mojibake(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        try:
            value = str(value)
        except Exception:
            return value
    if not value:
        return value
    if _HANGUL_RE.search(value):
        return value
    try:
        fixed = value.encode("latin1").decode("utf-8")
    except Exception:
        return value
    if _HANGUL_RE.search(fixed):
        return fixed
    return value


def _extract_cate2_from_web_title(web_title: Optional[str]) -> str:
    try:
        if not web_title:
            return ""
        part = str(web_title).split("<", 1)[0].strip()
        part = re.sub(r"\s*\([^)]*\)\s*", "", part).strip()
        return part
    except Exception:
        return ""


def _extract_first_payload_url(value: Any) -> Optional[str]:
    try:
        if isinstance(value, list) and value:
            candidate = str(value[0] or "").strip()
            return candidate or None
        if isinstance(value, str):
            candidate = value.strip()
            return candidate or None
    except Exception:
        return None
    return None


def _resolve_primary_target_url(data: Dict[str, Any], start_urls: List[Any]) -> Optional[str]:
    try:
        override_source = str(data.get("start_urls_override_source") or "").strip().lower()
    except Exception:
        override_source = ""

    if override_source == "contents_detail_direct" and start_urls:
        first = start_urls[0]
        try:
            if isinstance(first, dict):
                candidate = str(first.get("url") or "").strip()
            else:
                candidate = str(first).strip()
            if candidate:
                return candidate
        except Exception:
            pass

    for candidate in (
        _extract_first_payload_url(data.get("contents_url")),
        _extract_first_payload_url(data.get("target_url")),
        _extract_first_payload_url(data.get("contents")),
    ):
        if candidate:
            return candidate

    if start_urls:
        first = start_urls[0]
        try:
            if isinstance(first, dict):
                candidate = str(first.get("url") or "").strip()
            else:
                candidate = str(first).strip()
            return candidate or None
        except Exception:
            return None
    return None


def _apply_workflow_mode_boundary(workflow: Any, colle_mode: str) -> None:
    mode = str(colle_mode or "").strip().lower()
    try:
        workflow.colle_mode = mode
    except Exception:
        pass
    try:
        workflow.file_mode = mode == "file"
    except Exception:
        pass
    try:
        workflow.ui_colle = mode
    except Exception:
        pass
    try:
        if hasattr(workflow, "colle"):
            workflow.colle = mode or "board"
    except Exception:
        pass


def _create_workflow_for_mode(
    *,
    data: Dict[str, Any],
    start_urls: List[Any],
    job_id: str,
    colle_mode: str,
) -> Any:
    mode = str(colle_mode or "").strip().lower()
    primary_target_url = _resolve_primary_target_url(data, start_urls)
    if mode == "board":
        workflow_class = resolve_workflow_class_for_colle(mode)
        logger.debug("[DispatchAsm] %s -> %s | job_id=%s", mode, workflow_class.__name__, job_id)
        return create_board_crawl_workflow(
            workflow_class=workflow_class,
            data=data,
            start_urls=start_urls,
            primary_target_url=primary_target_url,
            job_id=job_id,
        )
    if mode == "file":
        workflow_class = resolve_workflow_class_for_colle(mode)
        logger.debug("[DispatchAsm] %s -> %s | job_id=%s", mode, workflow_class.__name__, job_id)
        return create_file_crawl_workflow(
            workflow_class=workflow_class,
            data=data,
            start_urls=start_urls,
            primary_target_url=primary_target_url,
            job_id=job_id,
        )

    logger.debug("[DispatchAsm] fallback -> IntegratedWorkflow | job_id=%s colle=%s", job_id, mode)
    return IntegratedWorkflow()


def assemble_workflow_after_url_resolve(
    data: Dict[str, Any],
    start_urls: List[Any],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    job_id: str,
    craw_id: str,
    db_name: str,
    chat_bot_id: str,
    use_query_links_only: bool,
    override_source: str,
    primary_content: Optional[Any] = None,
) -> Any:
    """
    Build and return the workflow instance for board/file modes.

    File crawl requests should resolve to FileDownloadWorkflow. The generic
    IntegratedWorkflow remains only as a fallback for legacy/unknown modes.
    """
    request_data_for_flow = dict(data or {})
    contents = data.get("contents") or []
    if primary_content is None and isinstance(contents, list) and contents:
        primary_content = contents[0]

    cate1 = data.get("cate1")
    cate2 = data.get("cate2")
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    if cate1 is None:
        cate1 = meta.get("cate1")
    if cate2 is None:
        cate2 = meta.get("cate2")
    category_patterns_enabled = resolve_stream_matched_rules_only(data)
    duplicate_repair_mode = normalize_duplicate_repair_request_mode(
        data.get("duplicate_repair_mode")
        or data.get("duplicateRepairMode")
        or data.get("board_duplicate_repair")
        or data.get("duplicate_repair")
    )
    duplicate_parsed_fields_mode = str(
        data.get("duplicate_parsed_fields_mode")
        or data.get("duplicateParsedFieldsMode")
        or data.get("board_duplicate_parsed_fields")
        or data.get("duplicate_parsed_fields")
        or ""
    ).strip().lower()
    if duplicate_parsed_fields_mode in {"on", "parsed_fields", "author", "date", "reg_date", "content_created_at"}:
        if duplicate_repair_mode == "category":
            duplicate_repair_mode = "category_parsed_fields"
        elif duplicate_repair_mode == "off":
            duplicate_repair_mode = "parsed_fields"
    data["duplicate_repair_mode"] = duplicate_repair_mode
    data["duplicate_parsed_fields_mode"] = (
        "parsed_fields"
        if duplicate_parsed_fields_mode in {"on", "parsed_fields"}
        else "author"
        if duplicate_parsed_fields_mode == "author"
        else "date"
        if duplicate_parsed_fields_mode in {"date", "reg_date", "content_created_at"}
        else "off"
    )
    duplicate_summary_mode = normalize_duplicate_summary_request_mode(
        data.get("duplicate_summary_mode")
        or data.get("duplicateSummaryMode")
        or data.get("board_duplicate_summary")
        or data.get("duplicate_summary")
    )
    data["duplicate_summary_mode"] = duplicate_summary_mode
    duplicate_title_mode = normalize_duplicate_title_request_mode(
        data.get("duplicate_title_mode")
        or data.get("duplicateTitleMode")
        or data.get("board_duplicate_title")
        or data.get("duplicate_title")
        or data.get("title_mode")
        or data.get("titleMode")
    )
    data["duplicate_title_mode"] = duplicate_title_mode
    request_config = CrawlRequestConfig.from_payload(data)
    crawl_mode = request_config.crawl_mode
    postprocess_only = str(
        data.get("postprocess_only")
        or data.get("duplicate_repair_only")
        or data.get("duplicate_parsed_fields_only")
        or ""
    ).strip().lower() in {"1", "true", "yes", "on", "y"}
    if (
        crawl_mode in {"", "crawling"}
        and not postprocess_only
        and duplicate_summary_mode == "off"
        and duplicate_title_mode == "off"
        and data.get("duplicate_parsed_fields_mode") == "off"
        and duplicate_repair_mode != "off"
    ):
        logger.debug(
            "[DispatchAsm][PostprocessGuard] normal crawling forces duplicate_repair_mode=off | job_id=%s requested=%s",
            job_id,
            duplicate_repair_mode,
        )
        duplicate_repair_mode = "off"
        data["duplicate_repair_mode"] = "off"
    try:
        logger.debug(
            "[DispatchAsm] categories resolved | job_id=%s colle=%s cate1=%r cate2=%r metadata_cate1=%r metadata_cate2=%r stream_matched_rules_only=%s duplicate_repair_mode=%s",
            job_id,
            str(data.get("colle") or "").strip().lower() or "board",
            cate1,
            cate2,
            meta.get("cate1"),
            meta.get("cate2"),
            category_patterns_enabled,
            duplicate_repair_mode,
        )
    except Exception:
        pass
    try:
        logger.info(
            "[CrawlBoard][StartMode] job_id=%s colle=%s duplicate_repair_mode=%s",
            job_id,
            str(data.get("colle") or "").strip().lower() or "board",
            duplicate_repair_mode,
        )
    except Exception:
        pass

    raw_colle_mode, colle_mode = request_config.raw_colle_mode, request_config.colle_mode
    if raw_colle_mode and raw_colle_mode != colle_mode:
        logger.debug(
            "[DispatchAsm] normalized colle | job_id=%s raw_colle=%s normalized=%s",
            job_id,
            raw_colle_mode,
            colle_mode,
        )
    content_type_mode = request_config.content_type
    if content_type_mode in {"file", "attach", "attachment"} and colle_mode != "file":
        logger.debug(
            "[DispatchAsm] content_type forces file workflow | job_id=%s before_colle=%s content_type=%s",
            job_id,
            colle_mode,
            content_type_mode,
        )
        colle_mode = "file"
        data["colle"] = "file"

    flow_config = build_basic_crawling_flow_config(
        data=request_data_for_flow,
        colle_mode=colle_mode,
        category_patterns_enabled=category_patterns_enabled,
        duplicate_repair_mode=duplicate_repair_mode,
        duplicate_summary_mode=duplicate_summary_mode,
        duplicate_title_mode=duplicate_title_mode,
    )
    apply_basic_crawling_flow_to_payload(data, flow_config)
    category_patterns_enabled = flow_config.category_patterns_enabled
    duplicate_repair_mode = flow_config.duplicate_repair_mode
    duplicate_summary_mode = flow_config.duplicate_summary_mode
    duplicate_title_mode = flow_config.duplicate_title_mode
    if flow_config.pure_crawling_mode:
        logger.debug("[DispatchAsm] pure board crawling defaults applied | job_id=%s", job_id)

    subject_value = None
    try:
        subjects_payload = data.get("subjects")
        if isinstance(subjects_payload, list) and subjects_payload:
            subject_value = _clean_optional_text(subjects_payload[0])
        elif isinstance(subjects_payload, str):
            subject_value = _clean_optional_text(subjects_payload)
    except Exception:
        subject_value = None
    if not subject_value:
        try:
            subject_value = _clean_optional_text(data.get("subject"))
        except Exception:
            subject_value = None
    if subject_value:
        subject_value = _clean_optional_text(subject_value)

    if colle_mode == "board" and category_patterns_enabled:
        try:
            extracted_cate2 = _extract_cate2_from_web_title(subject_value)
        except Exception:
            extracted_cate2 = ""
        if extracted_cate2:
            cate2 = extracted_cate2

    details_value = ""
    try:
        if start_date and end_date:
            details_value = f"{start_date.date().isoformat()} ~ {end_date.date().isoformat()}"
    except Exception:
        details_value = ""

    h3_value = ""
    try:
        raw_h3 = data.get("h3")
        if raw_h3 is not None:
            h3_value = _clean_ui_text(raw_h3)
    except Exception:
        h3_value = ""
    if h3_value:
        h3_value = _clean_ui_text(h3_value)
    if not h3_value:
        if colle_mode == "board":
            h3_value = "게시판"
        elif colle_mode == "file":
            h3_value = "게시판 파일"
        elif colle_mode == "date":
            h3_value = "뉴스"

    try:
        logger.debug(
            "[DispatchAsm] colle branch check | job_id=%s colle_mode=%s contents0=%s start_urls_count=%s start_urls_first=%s use_query_links_only=%s",
            job_id,
            colle_mode,
            primary_content,
            len(start_urls),
            start_urls[0] if start_urls else None,
            use_query_links_only,
        )
    except Exception:
        pass

    workflow: Any = _create_workflow_for_mode(
        data=data,
        start_urls=start_urls,
        job_id=job_id,
        colle_mode=colle_mode,
    )
    if colle_mode not in {"board", "file"}:
        try:
            logger.debug(
                "[DispatchAsm] IntegratedWorkflow context | job_id=%s colle_mode=%s contents0=%s start_urls_first=%s use_query_links_only=%s",
                job_id,
                colle_mode,
                primary_content,
                start_urls[0] if start_urls else None,
                use_query_links_only,
            )
        except Exception:
            pass

    try:
        logger.debug(
            "[DispatchAsm] workflow assembled | job_id=%s colle=%s workflow=%s",
            job_id,
            colle_mode,
            type(workflow).__name__,
        )
    except Exception:
        pass

    try:
        workflow.use_global_pool = True
    except Exception:
        pass
    _apply_workflow_mode_boundary(workflow, colle_mode)

    workflow.chat_bot_id = chat_bot_id
    workflow.job_id = job_id
    workflow.db_name = db_name
    try:
        workflow._crawl_method = str(data.get("method") or "period").strip() or "period"
    except Exception:
        try:
            workflow._crawl_method = "period"
        except Exception:
            pass
    workflow.server_domain = data.get("server_domain")
    try:
        workflow.board_list_urls = data.get("board_list_urls")
    except Exception:
        pass
    try:
        workflow.start_urls_override_source = data.get("start_urls_override_source") or override_source
    except Exception:
        pass
    try:
        workflow.pre_explored_start_urls_count = int(data.get("pre_explored_start_urls_count") or 0)
    except Exception:
        workflow.pre_explored_start_urls_count = 0
    try:
        workflow.exploration_post_total_count = int(data.get("exploration_post_total_count") or 0)
    except Exception:
        workflow.exploration_post_total_count = 0
    try:
        workflow.exploration_display_count_fixed = bool(data.get("exploration_display_count_fixed"))
    except Exception:
        workflow.exploration_display_count_fixed = False
    try:
        workflow.exploration_display_max_count = int(data.get("exploration_display_max_count") or 0)
    except Exception:
        workflow.exploration_display_max_count = 0
    try:
        workflow.learn_list_duplicate_exclude_result = data.get("learn_list_duplicate_exclude_result") or {}
    except Exception:
        workflow.learn_list_duplicate_exclude_result = {}
    try:
        workflow.learn_list_duplicate_exclude_selected_count = int(
            data.get("learn_list_duplicate_exclude_selected_count") or 0
        )
    except Exception:
        workflow.learn_list_duplicate_exclude_selected_count = 0
    try:
        workflow.file_crawl_stream_config = data.get("file_crawl_stream_config") or {}
    except Exception:
        workflow.file_crawl_stream_config = {}
    try:
        workflow.board_dashboard_detail_concurrency = int(data.get("board_dashboard_detail_concurrency") or 0)
    except Exception:
        workflow.board_dashboard_detail_concurrency = 0
    try:
        workflow.board_dashboard_db_throttle = bool(data.get("board_dashboard_db_throttle"))
    except Exception:
        workflow.board_dashboard_db_throttle = False
    try:
        workflow.board_dashboard_save_batch_size = int(data.get("board_dashboard_save_batch_size") or 0)
    except Exception:
        workflow.board_dashboard_save_batch_size = 0
    try:
        workflow.board_dashboard_save_batch_wait_ms = int(data.get("board_dashboard_save_batch_wait_ms") or 0)
    except Exception:
        workflow.board_dashboard_save_batch_wait_ms = 0
    try:
        apply_basic_crawling_flow_to_workflow(workflow, flow_config)
    except Exception:
        workflow.stream_matched_rules_only = False
        workflow.use_category_url_patterns = False
    try:
        raw_title_dry_run = (
            data.get("duplicate_title_dry_run")
            or data.get("duplicateTitleDryRun")
            or data.get("title_dry_run")
            or data.get("titleDryRun")
            or ""
        )
        workflow.duplicate_title_dry_run = str(raw_title_dry_run).strip().lower() in {"1", "true", "yes", "on", "y"}
    except Exception:
        workflow.duplicate_title_dry_run = False
    try:
        workflow.requested_scope_path_prefix = data.get("scope_path_prefix") or ""
    except Exception:
        workflow.requested_scope_path_prefix = ""
    try:
        workflow.sitemap_markdown = data.get("sitemap_markdown")
    except Exception:
        pass
    try:
        workflow.access_url = data.get("access_url")
    except Exception:
        pass
    try:
        workflow._category_sync_request_cookies = data.get("_category_sync_request_cookies") or {}
    except Exception:
        workflow._category_sync_request_cookies = {}
    try:
        workflow._edu_ingang_warm_url = data.get("_edu_ingang_warm_url") or data.get("edu_ingang_warm_url") or data.get("warm_url")
    except Exception:
        workflow._edu_ingang_warm_url = None
    try:
        partial_content_relearn_enabled = str(
            os.getenv("PARTIAL_CONTENT_RELEARN_ENABLED", "0") or "0"
        ).strip().lower() in {"1", "true", "yes", "on", "y"}
        workflow.content_relearn_mode = bool(data.get("content_relearn_mode")) and partial_content_relearn_enabled
    except Exception:
        pass
    try:
        workflow.suppress_terminal_sse = bool(data.get("_suppress_terminal_sse"))
    except Exception:
        pass
    try:
        workflow.ui_subject = subject_value
    except Exception:
        pass
    try:
        workflow.ui_h3 = h3_value
    except Exception:
        pass
    try:
        workflow.ui_details = details_value
    except Exception:
        pass

    try:
        workflow.cate1 = _clean_optional_text(cate1)
    except Exception:
        workflow.cate1 = None
    try:
        workflow.cate2 = _clean_optional_text(cate2)
    except Exception:
        workflow.cate2 = None

    try:
        memo_val = data.get("memo")
        if (memo_val is None or memo_val == "") and ("memo1" in data):
            memo_val = data.get("memo1")
        if isinstance(memo_val, list):
            memo_val = memo_val[0] if memo_val else ""
        memo_val = str(memo_val).strip() if memo_val is not None else ""
    except Exception:
        memo_val = ""
    try:
        if hasattr(workflow, "memo"):
            workflow.memo = memo_val
    except Exception:
        pass

    unique_id = data.get("unique_id")
    if unique_id:
        try:
            workflow.unique_id = unique_id
        except Exception:
            pass

    return workflow
