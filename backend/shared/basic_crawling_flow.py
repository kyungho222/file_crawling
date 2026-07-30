"""Default crawling flow policy.

This module keeps the baseline crawl behavior separate from optional
post-processing features. The default frontend flow is:

1. discover/list
2. select/detail
3. save to LEARN_LIST
4. learn/index chunks
5. report counters through workflow progress

New board crawl runs apply lightweight category classification to newly
collected URLs. Optional repairs/backfills must be explicitly requested.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


DEFAULT_FLOW_STAGES = ("discover", "select", "save", "learn", "report")

_FALSEISH_STRINGS = {"", "off", "false", "0", "no", "none", "null"}
BOARD_COLLE_ALIASES = {"web_de", "content", "auto_crawl", "crawl_auto"}
FILE_ROUTE_HINTS = {"file", "attach", "attachment"}
CANONICAL_COLLE_MODES = {"board", "file"}

POSTPROCESSING_OPT_IN_KEYS = {
    "duplicate_repair_mode",
    "duplicateRepairMode",
    "board_duplicate_repair",
    "duplicate_repair",
    "duplicate_summary_mode",
    "duplicateSummaryMode",
    "board_duplicate_summary",
    "duplicate_summary",
    "duplicate_title_mode",
    "duplicateTitleMode",
    "board_duplicate_title",
    "duplicate_title",
    "title_mode",
    "titleMode",
    "duplicate_parsed_fields_mode",
    "duplicateParsedFieldsMode",
    "board_duplicate_parsed_fields",
    "duplicate_parsed_fields",
    "use_category_url_patterns",
    "stream_matched_rules_only",
    "category_url_patterns",
    "enable_post_job_cate_update",
    "enable_selector_learning",
}


@dataclass(frozen=True)
class BasicCrawlingFlowConfig:
    colle_mode: str
    stages: tuple[str, ...]
    postprocessing_opt_in: bool
    pure_crawling_mode: bool
    category_patterns_enabled: bool
    duplicate_repair_mode: str
    duplicate_summary_mode: str
    duplicate_title_mode: str
    duplicate_parsed_fields_mode: str
    auto_category_enabled: bool
    enable_post_job_cate_update: bool
    enable_selector_learning: bool


def normalize_colle_mode(data: Dict[str, Any]) -> tuple[str, str]:
    try:
        colle_mode = str(data.get("colle") or "").strip().lower()
    except Exception:
        colle_mode = ""
    raw_colle_mode = colle_mode
    try:
        content_type_mode = str(data.get("content_type") or "").strip().lower()
    except Exception:
        content_type_mode = ""

    if content_type_mode in FILE_ROUTE_HINTS or colle_mode in FILE_ROUTE_HINTS:
        colle_mode = "file"
    elif colle_mode in BOARD_COLLE_ALIASES:
        colle_mode = "board"
    elif not colle_mode and str(data.get("crawl_mode") or "").strip().lower() == "crawling":
        colle_mode = "board"
    return raw_colle_mode, colle_mode


def _is_falseish(value: Any) -> bool:
    if value is None or value is False:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _FALSEISH_STRINGS
    return False


def request_opted_into_board_postprocessing(data: Dict[str, Any]) -> bool:
    for key in POSTPROCESSING_OPT_IN_KEYS:
        if key in data and not _is_falseish(data.get(key)):
            return True
    return False


def _request_enabled(data: Dict[str, Any], *keys: str) -> bool:
    for key in keys:
        if key in data and not _is_falseish(data.get(key)):
            return True
    return False


def build_basic_crawling_flow_config(
    *,
    data: Dict[str, Any],
    colle_mode: str,
    category_patterns_enabled: bool,
    duplicate_repair_mode: str,
    duplicate_summary_mode: str,
    duplicate_title_mode: str,
) -> BasicCrawlingFlowConfig:
    postprocessing_opt_in = (
        colle_mode == "board" and request_opted_into_board_postprocessing(data)
    )
    pure_crawling_mode = colle_mode == "board" and not postprocessing_opt_in

    if pure_crawling_mode:
        category_patterns_enabled = False
        duplicate_repair_mode = "off"
        duplicate_summary_mode = "off"
        duplicate_title_mode = "off"

    return BasicCrawlingFlowConfig(
        colle_mode=colle_mode,
        stages=DEFAULT_FLOW_STAGES,
        postprocessing_opt_in=postprocessing_opt_in,
        pure_crawling_mode=pure_crawling_mode,
        category_patterns_enabled=bool(category_patterns_enabled),
        duplicate_repair_mode=duplicate_repair_mode,
        duplicate_summary_mode=duplicate_summary_mode,
        duplicate_title_mode=duplicate_title_mode,
        duplicate_parsed_fields_mode=str(data.get("duplicate_parsed_fields_mode") or "off").strip().lower() or "off",
        auto_category_enabled=bool(colle_mode == "board"),
        enable_post_job_cate_update=bool(
            colle_mode == "board"
            and not pure_crawling_mode
            and _request_enabled(data, "enable_post_job_cate_update", "enablePostJobCateUpdate")
        ),
        enable_selector_learning=not pure_crawling_mode,
    )


def apply_basic_crawling_flow_to_payload(
    data: Dict[str, Any],
    config: BasicCrawlingFlowConfig,
) -> None:
    data["duplicate_repair_mode"] = config.duplicate_repair_mode
    data["duplicate_summary_mode"] = config.duplicate_summary_mode
    data["duplicate_title_mode"] = config.duplicate_title_mode
    data["duplicate_parsed_fields_mode"] = config.duplicate_parsed_fields_mode
    data["stream_matched_rules_only"] = config.category_patterns_enabled
    data["use_category_url_patterns"] = config.category_patterns_enabled


def apply_basic_crawling_flow_to_workflow(
    workflow: Any,
    config: BasicCrawlingFlowConfig,
) -> None:
    workflow.basic_crawling_flow = config
    workflow.basic_crawling_flow_stages = config.stages
    workflow.pure_crawling_mode = config.pure_crawling_mode
    workflow.stream_matched_rules_only = config.category_patterns_enabled
    workflow.use_category_url_patterns = config.category_patterns_enabled
    workflow.duplicate_repair_mode = config.duplicate_repair_mode
    workflow.duplicate_summary_mode = config.duplicate_summary_mode
    workflow.duplicate_title_mode = config.duplicate_title_mode
    workflow.duplicate_parsed_fields_mode = config.duplicate_parsed_fields_mode
    workflow.auto_category_enabled = config.auto_category_enabled
    workflow.enable_post_job_cate_update = config.enable_post_job_cate_update
    if config.pure_crawling_mode:
        workflow.enable_selector_learning = False
