"""Board crawl workflow assembly helpers."""

from __future__ import annotations

import logging
import re
from typing import Any, List, Optional

logger = logging.getLogger("backend.board.board_crawl_module")


def _extract_first_contents_url(contents: Any) -> str:
    try:
        if isinstance(contents, list) and contents:
            return str(contents[0] or "").strip()
        return str(contents or "").strip()
    except Exception:
        return ""


def _apply_target_board_id(workflow: Any, contents: Any, *, job_id: str) -> None:
    target_url = _extract_first_contents_url(contents)
    if not target_url:
        return
    try:
        m_t = re.search(r"/bbs/([a-zA-Z0-9_]+)", target_url, re.IGNORECASE)
        if m_t:
            workflow.target_board_id = m_t.group(1)
            logger.info("[BoardCrawlModule] Injected target_board_id=%s | job_id=%s", m_t.group(1), job_id)
    except Exception:
        logger.debug("[BoardCrawlModule] target_board_id injection skipped | job_id=%s", job_id, exc_info=True)


def _apply_target_domains(workflow: Any, raw_target_domains: Any) -> None:
    if raw_target_domains is None:
        return
    try:
        if isinstance(raw_target_domains, list):
            workflow.target_domains = [str(x).strip() for x in raw_target_domains if x]
        else:
            workflow.target_domains = [x.strip() for x in str(raw_target_domains).split(",") if x.strip()]
    except Exception:
        logger.debug("[BoardCrawlModule] target_domains injection skipped", exc_info=True)


def apply_board_workflow_boundary(workflow: Any, data: dict) -> None:
    """Keep board crawling mode explicit at the module boundary."""
    data["colle"] = "board"
    if not data.get("content_type"):
        data["content_type"] = "url"

    for attr, value in (
        ("colle", "board"),
        ("colle_mode", "board"),
        ("ui_colle", "board"),
        ("file_mode", False),
    ):
        try:
            setattr(workflow, attr, value)
        except Exception:
            pass


def create_board_crawl_workflow(
    *,
    workflow_class: type,
    data: dict,
    start_urls: List[Any],
    primary_target_url: Optional[str],
    job_id: str,
) -> Any:
    """Create and initialize the board crawling workflow surface."""
    workflow = workflow_class()
    contents = data.get("contents") or []

    apply_board_workflow_boundary(workflow, data)
    _apply_target_board_id(workflow, contents, job_id=job_id)

    if primary_target_url:
        workflow.target_url = str(primary_target_url)

    _apply_target_domains(workflow, data.get("target_domains"))
    return workflow
