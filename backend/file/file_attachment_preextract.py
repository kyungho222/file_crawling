"""파일 크롤링 시작 전 게시글 상세페이지의 첨부파일을 선추출한다."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict

from utils.db_name import resolve_db_name
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.file.file_attachment_preextract")


def _safe_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_title(title: Any) -> str:
    text = str(title or "").strip()
    if text and text.count(")") > text.count("("):
        close_index = text.find(")")
        if 0 <= close_index <= 24 and not text.startswith("("):
            return f"({text}"
    return text


async def extract_file_attachments_readonly(body: Dict[str, Any]) -> Dict[str, Any]:
    """기존 파일 첨부 추출기만 실행하고 DB 저장과 학습은 수행하지 않는다."""
    url = ensure_url_scheme(
        str(
            body.get("url")
            or body.get("detail_url")
            or body.get("contents_url")
            or body.get("target_url")
            or ""
        ).strip()
    )
    if not url:
        return {"status": "error", "message": "url is required"}

    db_name = resolve_db_name(body, default="dev_user")
    metadata = _safe_dict(body.get("metadata"))
    chat_bot_id = str(body.get("chat_bot_id") or metadata.get("chat_bot_id") or "").strip()
    try:
        fetch_timeout_sec = float(body.get("probe_fetch_timeout_sec") or body.get("fetch_timeout_sec") or 6.0)
    except Exception:
        fetch_timeout_sec = 6.0
    fetch_timeout_sec = max(1.0, min(fetch_timeout_sec, 20.0))
    try:
        playwright_timeout_sec = float(body.get("probe_playwright_timeout_sec") or 8.0)
    except Exception:
        playwright_timeout_sec = 8.0
    playwright_timeout_sec = max(1.0, min(playwright_timeout_sec, 20.0))
    disable_playwright = str(body.get("disable_playwright") or "").strip().lower() in {"1", "true", "yes", "on"}

    from backend.file.file_download_workflow import FileDownloadWorkflow, _extract_file_author_info
    from backend.file.fast_attachment_extractor import extract_fast_attachments

    workflow = FileDownloadWorkflow()
    workflow.db_name = db_name
    workflow.chat_bot_id = chat_bot_id
    workflow.job_id = str(body.get("job_id") or f"file-preextract-{int(time.time())}")
    workflow.enable_db_save = False
    workflow.enable_learning = False
    workflow.file_pipeline_skip_learning = True

    try:
        probe_min_delay_sec = float(body.get("probe_domain_min_delay_sec") or 0.0)
    except Exception:
        probe_min_delay_sec = 0.0
    if probe_min_delay_sec > 0:
        probe_min_delay_sec = min(probe_min_delay_sec, 5.0)
        try:
            probe_max_delay_sec = float(body.get("probe_domain_max_delay_sec") or probe_min_delay_sec)
        except Exception:
            probe_max_delay_sec = probe_min_delay_sec
        workflow._domain_fetch_min_delay_sec = probe_min_delay_sec
        workflow._domain_fetch_max_delay_sec = max(probe_min_delay_sec, min(probe_max_delay_sec, 10.0))

    html = ""
    fetch_method = "static"
    try:
        html = await workflow._fetch_html_static(url, timeout_sec=fetch_timeout_sec) or ""
        if not html and not disable_playwright:
            html = await asyncio.wait_for(workflow._fetch_html_playwright(url), timeout=playwright_timeout_sec) or ""
            fetch_method = "playwright_fallback"
        if not html:
            return {"status": "error", "message": "failed to fetch detail html", "url": url}

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")
            title = workflow._extract_board_title(soup, url=url, html=html)
        except Exception:
            soup = None
            title = ""
        selector_profile = await workflow._get_selector_profile_for_detail(url=url, board_url="")
        try:
            author_info = _extract_file_author_info(html, url=url, selector_profile=selector_profile)
        except Exception:
            author_info = {}
        try:
            reg_date_dt = workflow._extract_board_reg_date(soup, html=html, url=url) if soup is not None else None
            reg_date = reg_date_dt.strftime("%Y-%m-%d %H:%M:%S") if reg_date_dt else ""
        except Exception:
            reg_date = ""

        attachments = extract_fast_attachments(html, url)
        if not attachments:
            attachments = extract_fast_attachments(html, url, force_full_scan=True)
        try:
            ajax_attachments = await workflow._extract_kcohesion_filelist_attachments(html, base_url=url)
        except Exception:
            ajax_attachments = []
        seen = {str(item.get("href") or "").strip().lower() for item in attachments if isinstance(item, dict)}
        for item in ajax_attachments:
            href = str((item or {}).get("href") or "").strip()
            if href and href.lower() not in seen:
                attachments.append(item)
                seen.add(href.lower())

        return {
            "status": "ok",
            "url": url,
            "source_url": url,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "fetch_method": fetch_method,
            "html_length": len(html),
            "title": _normalize_title(title),
            "reg_date": reg_date,
            "author_info": _safe_dict(author_info),
            "metadata": {
                "source_url": url,
                "title": _normalize_title(title),
                "reg_date": reg_date,
                "created_at": reg_date,
                "updated_at": reg_date,
                "probe_generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "fetch_method": fetch_method,
                "html_length": len(html),
            },
            "attachment_summary": {"attachment_count": len(attachments), "attachments": attachments},
            "attachments": attachments,
            "counts": {"scan_count": 1, "attachment_count": len(attachments)},
        }
    finally:
        try:
            await workflow._close_http_session()
        except Exception:
            pass
        try:
            await workflow._close_playwright()
        except Exception:
            pass

_PREEXTRACT_FLAGS = (
    "file_dashboard_preextract_attachments",
    "file_crawl_preextract_attachments",
    "preextract_file_attachments",
    "use_extracted_attachments_as_start_urls",
)

_START_ITEM_FIELDS = (
    "type", "cate1", "cate2", "store_cate1", "store_cate2", "assigned_cate1", "assigned_cate2",
    "board_cate1", "board_cate2", "board_cate1_name", "board_cate2_name", "category1", "category2",
    "learn_list_id", "id", "title", "subject", "reg_date", "reg_date_str", "author_info",
)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def file_attachment_preextract_enabled(data: Dict[str, Any]) -> bool:
    return any(_truthy(data.get(key)) for key in _PREEXTRACT_FLAGS)


def apply_file_attachment_preextract_default(data: Dict[str, Any]) -> bool:
    """파일 크롤링 시작 요청에만 선추출 기본값을 적용하며 명시 값은 보존한다."""
    if not isinstance(data, dict):
        return False
    if any(key in data for key in _PREEXTRACT_FLAGS):
        return file_attachment_preextract_enabled(data)
    data["file_crawl_preextract_attachments"] = True
    return True


def start_urls_have_direct_attachments(start_urls: Any) -> bool:
    return any(
        isinstance(item, dict)
        and isinstance(item.get("attachments") or item.get("direct_attachments"), list)
        for item in (start_urls or [])
    )


def _item_url(item: Any) -> str:
    raw_url = item.get("url") if isinstance(item, dict) else item
    return ensure_url_scheme(str(raw_url or "").strip()) if raw_url else ""


def _normalize_attachment(item: Any, post_url: str) -> Dict[str, Any]:
    """첨부 추출기의 원본 파일명 메타를 바꾸지 않고 큐 문맥만 보강한다."""
    source = _safe_dict(item)
    href = ensure_url_scheme(str(source.get("href") or source.get("url") or "").strip())
    if not href:
        return {}
    normalized = dict(source)
    normalized.setdefault("href", href)
    normalized.setdefault("url", href)
    normalized["post_url"] = post_url
    normalized["source_page"] = post_url
    return normalized


def _build_direct_start_item(original: Any, result: Dict[str, Any], post_url: str) -> Dict[str, Any]:
    source = _safe_dict(original)
    item = {key: source[key] for key in _START_ITEM_FIELDS if key in source}
    attachments = []
    seen = set()
    for attachment in result.get("attachments") or []:
        normalized = _normalize_attachment(attachment, post_url)
        href = str(normalized.get("href") or "").lower()
        if href and href not in seen:
            attachments.append(normalized)
            seen.add(href)
    item["url"] = post_url
    item["type"] = str(item.get("type") or "post")
    item["source"] = "file_attachment_preextract"
    item["attachments"] = attachments
    item["direct_attachments"] = attachments
    if not str(item.get("title") or "").strip():
        item["title"] = str(result.get("title") or "").strip()
    if not str(item.get("subject") or "").strip():
        item["subject"] = str(result.get("title") or "").strip()
    if not str(item.get("reg_date") or "").strip():
        item["reg_date"] = str(result.get("reg_date") or "").strip()
    if not isinstance(item.get("author_info"), dict):
        item["author_info"] = _safe_dict(result.get("author_info"))
    return item


async def preextract_file_attachment_start_urls(
    *, data: Dict[str, Any], start_urls: Any, db_name: str, chat_bot_id: str, job_id: str,
) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    """게시글 URL 목록을 기존 첨부 추출기로 처리해 direct attachment start_urls로 바꾼다."""
    original_items = [item for item in (start_urls or []) if _item_url(item)]
    try:
        configured_concurrency = int(
            data.get("file_attachment_preextract_concurrency")
            or os.getenv("FILE_ATTACHMENT_PREEXTRACT_CONCURRENCY", "4")
        )
    except Exception:
        configured_concurrency = 4
    concurrency = max(1, min(configured_concurrency, 8))
    meta: Dict[str, Any] = {
        "enabled": True,
        "input_post_count": len(original_items),
        "attachment_post_count": 0,
        "attachment_count": 0,
        "empty_post_count": 0,
        "failed_post_count": 0,
        "concurrency": concurrency,
        "failed_samples": [],
    }
    if not original_items:
        return [], meta

    semaphore = asyncio.Semaphore(concurrency)

    async def extract_one(original: Any) -> tuple[Dict[str, Any], Dict[str, Any]]:
        post_url = _item_url(original)
        async with semaphore:
            try:
                result = await extract_file_attachments_readonly(
                    {
                        "url": post_url,
                        "db_name": db_name,
                        "chat_bot_id": chat_bot_id,
                        "job_id": job_id,
                        "disable_playwright": data.get("disable_playwright"),
                        "probe_fetch_timeout_sec": data.get("file_attachment_preextract_timeout_sec"),
                        "probe_playwright_timeout_sec": data.get("file_attachment_preextract_playwright_timeout_sec"),
                        "probe_domain_min_delay_sec": data.get("file_attachment_preextract_delay_sec"),
                    }
                )
            except Exception as exc:
                return {}, {"post_url": post_url, "reason": "attachment_preextract_exception", "error": repr(exc)}
        if result.get("status") != "ok":
            return {}, {
                "post_url": post_url,
                "reason": "attachment_preextract_failed",
                "error": str(result.get("message") or "unknown"),
            }
        return _build_direct_start_item(original, result, post_url), {}

    extracted = await asyncio.gather(*(extract_one(item) for item in original_items), return_exceptions=True)
    direct_items: list[Dict[str, Any]] = []
    for original, result in zip(original_items, extracted):
        post_url = _item_url(original)
        if isinstance(result, BaseException):
            meta["failed_post_count"] += 1
            if len(meta["failed_samples"]) < 5:
                meta["failed_samples"].append({"post_url": post_url, "reason": "attachment_preextract_exception", "error": repr(result)})
            continue
        item, failure = result
        if failure:
            meta["failed_post_count"] += 1
            if len(meta["failed_samples"]) < 5:
                meta["failed_samples"].append(failure)
            continue
        attachment_count = len(item.get("attachments") or [])
        if not attachment_count:
            meta["empty_post_count"] += 1
            continue
        meta["attachment_post_count"] += 1
        meta["attachment_count"] += attachment_count
        direct_items.append(item)
    return direct_items, meta
