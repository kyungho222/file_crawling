# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from backend.board.board_meta_extractor import extract_author_info_from_html
from backend.file.fast_attachment_extractor import (
    _is_noise_attachment_asset,
    extract_fast_attachments,
    infer_attachment_extension,
)
from backend.file.file_detail_category import (
    filter_unexposed_file_detail_cates,
    normalize_file_detail_cates,
    split_detail_cates,
)
from backend.file.file_crawl_stage3 import enqueue_file_crawl_stage3_candidates
from backend.file.html_encoding import decode_html_response_bytes
from backend.shared.date_utils import is_date_in_range, parse_date
from core.crawler.file_host_request_gate import (
    acquire_file_crawl_host_slot,
    release_file_crawl_host_slot,
)
from utils.file import strip_fallback_download_label
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.file.fast_attachment_producer")

_MENU_SHELL_PATH_RE = re.compile(r"(?:^|/)main\.do$", re.IGNORECASE)
_LIST_PAGE_PATH_RE = re.compile(r"(?:^|/)(?:list|index)\.(?:do|jsp|php|asp|aspx|html?)$|(?:^|/)list$", re.IGNORECASE)
_STATIC_CONTENTS_PATH_RE = re.compile(r"(?:^|/)main/contents\.do$", re.IGNORECASE)
_CONTENTS_UPDATE_LABELS = (
    "최종업데이트",
    "최종수정일",
    "최종수정",
    "수정일",
    "업데이트",
)


def _clip_log_value(value: Any, limit: int = 240) -> str:
    try:
        text = str(value or "").replace("\n", "\\n").replace("\r", "\\r").strip()
    except Exception:
        text = ""
    if len(text) > limit:
        return text[: max(0, limit - 3)] + "..."
    return text


def _log_file_url_status(
    *,
    stage: str,
    status: str,
    process_url: str = "",
    post_url: str = "",
    file_url: str = "",
    selected: str = "",
    saved: str = "",
    learn: str = "",
    reason: str = "",
    error: Any = "",
    name: str = "",
    count: Any = "",
    job_id: Any = "",
    db_name: Any = "",
) -> None:
    try:
        status_text = str(status or "").strip().lower()
        reason_text = str(reason or "").strip().lower()
        error_text = str(error or "").strip()
        error_lower = error_text.lower()
        normal_skip_reasons = {
            "non_doc_file",
            "non_doc_precheck",
            "non_doc_mime",
            "viewer_convert_url",
            "scan_filter_non_doc",
            "completed_cache",
            "db_duplicate",
            "duplicate_existing",
            "duplicate_reuse_learned",
            "file_pipeline_skip_learning",
            "list_page",
            "menu_shell",
            "list_page_no_attachment_extract",
            "no_attachments",
            "attachment_empty",
        }
        failure_reasons = {
            "exception",
            "learn_list_no_row",
            "file_text_extract_empty",
            "learning_pipeline_failed",
            "upload_copy_failed",
            "download_failed",
            "download_timeout",
            "download timeout",
            "timeout",
            "connectiontimeouterror",
            "connection_timeout",
            "connection timeout",
            "ocr_status_429",
            "ocr_api_failed",
        }
        failure_tokens = (
            "download_failed",
            "download timeout",
            "download_timeout",
            "connectiontimeouterror",
            "connection timeout",
            "timeout",
            "timed out",
            "ocr_status_429",
            "statuscode 429",
            "status code 429",
            "???? 429",
            "http 429",
            "429",
            "failed",
            "error",
            "exception",
        )
        is_normal_skip = reason_text in normal_skip_reasons
        is_error = (
            status_text in {"error", "failed"}
            or reason_text in failure_reasons
            or any(token in error_lower for token in failure_tokens)
        )
        if not is_error or is_normal_skip:
            return
        logger.error(
            "file crawl url error | stage=%s status=%s job_id=%s db=%s process_url=%s post_url=%s file_url=%s selected=%s saved=%s learn=%s count=%s name=%s reason=%s error=%s",
            _clip_log_value(stage, 80),
            _clip_log_value(status, 80),
            _clip_log_value(job_id, 80),
            _clip_log_value(db_name, 80),
            _clip_log_value(process_url, 500),
            _clip_log_value(post_url, 500),
            _clip_log_value(file_url, 500),
            _clip_log_value(selected, 40),
            _clip_log_value(saved, 40),
            _clip_log_value(learn, 40),
            _clip_log_value(count, 40),
            _clip_log_value(name, 260),
            _clip_log_value(reason, 300),
            _clip_log_value(error, 800),
        )
    except Exception:
        pass


def _file_fetch_enqueue_delay_sec(workflow: Any = None) -> float:
    configured = getattr(workflow, "_file_crawl_fetch_delay_sec", None) if workflow is not None else None
    raw = configured if configured is not None else (
        os.getenv("FILE_CRAWL_FETCH_ENQUEUE_DELAY_SEC")
        or os.getenv("FILE_CRAWL_FETCH_INTERVAL_SEC")
        or "3"
    )
    try:
        value = float(raw)
    except Exception:
        value = 3.0
    return max(0.0, min(value, 60.0))

def _is_static_contents_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return False
    return bool(_STATIC_CONTENTS_PATH_RE.search(str(parsed.path or "")))


def _detail_query_keys() -> set[str]:
    return {
        "ntt_id", "nttid", "bbs_id", "bbsid", "board_id", "boardid",
        "article_id", "articleid", "seq", "no", "idx", "sn", "id",
        "bdid", "bmid", "pst_id", "pstid", "post_id", "postid",
    }


def _is_menu_shell_url(url: str) -> bool:
    """Skip menu shell pages that rarely contain direct attachments."""
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return False
    if not _MENU_SHELL_PATH_RE.search(str(parsed.path or "")):
        return False
    query = parse_qs(parsed.query or "", keep_blank_values=True)
    lowered_keys = {str(key or "").lower() for key in query}
    if "menuid" not in lowered_keys:
        return False
    return not (lowered_keys & _detail_query_keys())


def _is_list_page_url(url: str) -> bool:
    """File downloads must come from detail pages so title/meta can follow."""
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return False
    path = str(parsed.path or "")
    if not _LIST_PAGE_PATH_RE.search(path):
        return False
    query = parse_qs(parsed.query or "", keep_blank_values=True)
    lowered_keys = {str(key or "").lower() for key in query}
    return not (lowered_keys & _detail_query_keys())


def _ko_fast_extract_status(value: Any) -> str:
    text = str(value or "").strip()
    mapping = {
        "found": "발견",
        "empty": "비어있음",
        "fetch_empty": "상세페이지_응답없음",
        "found_extra": "추가추출_발견",
        "rescued_full_scan": "전체스캔_복구",
        "rescued_refetch": "재요청_복구",
        "noise_filtered_empty": "노이즈제거후_비어있음",
        "failed": "실패",
    }
    if text.startswith("skip_"):
        return "스킵_" + _ko_fast_skip_reason(text[5:])
    if text.endswith("_noise_filtered"):
        base = text[: -len("_noise_filtered")]
        return mapping.get(base, base) + "_노이즈제거"
    return mapping.get(text, text or "알수없음")


def _ko_fast_skip_reason(value: Any) -> str:
    text = str(value or "").strip()
    return {
        "menu_shell": "메뉴껍데기",
        "list_page": "목록페이지",
    }.get(text, text or "알수없음")


@dataclass
class FastFilePostItem:
    url: str
    board_url: str = ""
    cate1: str = ""
    cate2: str = ""
    title: str = ""
    reg_date: str = ""
    author: str = ""
    department: str = ""


def normalize_fast_file_post_item(item: Any) -> Optional[FastFilePostItem]:
    if isinstance(item, str):
        url = item.strip()
        return FastFilePostItem(url=ensure_url_scheme(url)) if url else None
    if not isinstance(item, dict):
        return None
    url = str(
        item.get("url")
        or item.get("href")
        or item.get("content")
        or item.get("contents_url")
        or item.get("target_url")
        or ""
    ).strip()
    if not url:
        return None
    cate1 = str(
        item.get("cate1")
        or item.get("category1")
        or item.get("board_cate1")
        or item.get("board_cate1_name")
        or item.get("store_cate1")
        or item.get("assigned_cate1")
        or ""
    ).strip()
    cate2 = str(
        item.get("cate2")
        or item.get("category2")
        or item.get("board_cate2")
        or item.get("board_cate2_name")
        or item.get("store_cate2")
        or item.get("assigned_cate2")
        or ""
    ).strip()
    if not (cate1 or cate2):
        matched_cate1, matched_cate2 = split_detail_cates(item.get("cate_match"))
        cate1 = cate1 or matched_cate1
        cate2 = cate2 or matched_cate2
    if not (cate1 or cate2):
        type_cate1, type_cate2 = split_detail_cates(item.get("type"))
        cate1 = cate1 or type_cate1
        cate2 = cate2 or type_cate2
    cate1, cate2 = normalize_file_detail_cates(cate1, cate2)
    return FastFilePostItem(
        url=ensure_url_scheme(url),
        board_url=str(item.get("board_url") or item.get("list_url") or item.get("source_url") or "").strip(),
        cate1=cate1,
        cate2=cate2,
        title=str(item.get("title") or item.get("subject") or "").strip(),
        reg_date=str(item.get("reg_date") or item.get("published_at") or item.get("post_date") or "").strip(),
        author=str(item.get("author") or item.get("content_author") or "").strip(),
        department=str(item.get("department") or "").strip(),
    )


async def _fetch_with_workflow(
    workflow: Any,
    url: str,
    timeout_sec: float,
    *,
    playwright_fallback_on_fetch_failure: bool = False,
) -> str:
    async def _playwright_retry(reason: str) -> str:
        fallback = getattr(workflow, "_fetch_html_playwright_detail_fallback", None)
        if not callable(fallback):
            return ""
        logger.info(
            "[file-fast][detail_playwright_retry] job_id=%s reason=%s url=%s",
            getattr(workflow, "job_id", ""),
            reason,
            url,
        )
        try:
            return str((await fallback(url)) or "")
        except Exception as exc:
            logger.debug(
                "[file-fast][detail_playwright_retry_failed] job_id=%s reason=%s url=%s err=%s",
                getattr(workflow, "job_id", ""),
                reason,
                url,
                exc,
            )
            return ""

    fetcher = getattr(workflow, "_fetch_html_static", None)
    if callable(fetcher):
        try:
            html = await fetcher(url, timeout_sec=timeout_sec)
        except TypeError:
            html = await fetcher(url)
        except Exception:
            if playwright_fallback_on_fetch_failure:
                return await _playwright_retry("static_exception")
            raise
        if html or not playwright_fallback_on_fetch_failure:
            return str(html or "")
        return await _playwright_retry("static_empty")
    try:
        import aiohttp  # type: ignore[import-not-found]
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("aiohttp is required when workflow fetcher is unavailable") from exc
    timeout = aiohttp.ClientTimeout(total=max(float(timeout_sec or 20), 1.0))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            raw = await response.read()
            return decode_html_response_bytes(raw, response.headers.get("Content-Type", ""))


def _empty_refetch_min_chars() -> int:
    try:
        return max(0, int(os.getenv("FILE_FAST_ATTACHMENT_EMPTY_REFETCH_MIN_CHARS", "350000") or "350000"))
    except Exception:
        return 350000


def _light_attachment_scan_bytes() -> int:
    try:
        return max(0, int(os.getenv("FILE_FAST_ATTACHMENT_LIGHT_SCAN_BYTES", "120000") or "120000"))
    except Exception:
        return 120000



def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _valid_author_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        from backend.board.board_meta_extractor import is_valid_content_author_value
        if not is_valid_content_author_value(text):
            return ""
    except Exception:
        if len(text) > 80:
            return ""
    return text


def _format_board_date(value: Any) -> str:
    if value is None:
        return ""
    try:
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = parse_date(text)
    except Exception:
        parsed = None
    if parsed is not None:
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    return text[:32]


def _extract_date_with_workflow(workflow: Any, html: str, url: str) -> str:
    date_fn = getattr(workflow, "_extract_board_reg_date", None)
    if not callable(date_fn):
        return ""
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]

        soup = BeautifulSoup(html or "", "html.parser")
    except Exception:
        soup = None
    if soup is None:
        return ""
    try:
        return _format_board_date(date_fn(soup, html=html, url=url))
    except TypeError:
        try:
            return _format_board_date(date_fn(soup, html, url))
        except Exception:
            return ""
    except Exception:
        return ""


def _extract_static_contents_update_date(html: str) -> str:
    if not html:
        return ""
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]

        soup = BeautifulSoup(html or "", "html.parser")
    except Exception:
        soup = None

    labels_compact = {re.sub(r"\s+", "", label).lower() for label in _CONTENTS_UPDATE_LABELS}
    if soup is not None:
        try:
            for dl in soup.find_all("dl"):
                dts = dl.find_all("dt", recursive=False)
                dds = dl.find_all("dd", recursive=False)
                for dt, dd in zip(dts, dds):
                    label = re.sub(r"\s+", "", dt.get_text(" ", strip=True)).lower()
                    if any(key and key in label for key in labels_compact):
                        formatted = _format_board_date(dd.get_text(" ", strip=True))
                        if formatted:
                            return formatted
        except Exception:
            pass
        try:
            text = soup.get_text(" ", strip=True)
        except Exception:
            text = str(html or "")
    else:
        text = str(html or "")

    label_alt = "|".join(re.escape(label) for label in _CONTENTS_UPDATE_LABELS)
    match = re.search(
        rf"(?:{label_alt})\s*[:：]?\s*((?:19|20)\d{{2}}[-./년\s]+\d{{1,2}}[-./월\s]+\d{{1,2}}(?:일)?)",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        return _format_board_date(match.group(1))
    return ""


def _extract_date_from_text(html: str) -> str:
    text = re.sub(r"\s+", " ", str(html or " ")).strip()
    if not text:
        return ""
    label_pattern = (
        r"(?:작성일|등록일|게시일|공고일|날짜|일자|date)"
        r"\s*[:：]?\s*"
        r"((?:19|20)\d{2}[-./년\s]+\d{1,2}[-./월\s]+\d{1,2}(?:일)?(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)"
    )
    date_pattern = (
        r"(?:19|20)\d{2}[-./년\s]+\d{1,2}[-./월\s]+\d{1,2}(?:일)?(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?"
    )
    candidates = [m.group(1) for m in re.finditer(label_pattern, text, flags=re.IGNORECASE)]
    candidates.extend(m.group(0) for m in re.finditer(date_pattern, text))
    for candidate in candidates[:20]:
        formatted = _format_board_date(candidate)
        if formatted:
            return formatted
    return ""

def _compact_meta_value(value: Any, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" :-|\t\r\n")
    return text[:limit]


def _extract_labeled_meta_value(html: str, labels: tuple[str, ...]) -> str:
    if not html:
        return ""
    label_set = {re.sub(r"\s+", "", label).lower() for label in labels}
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]

        soup = BeautifulSoup(html or "", "html.parser")
    except Exception:
        soup = None

    if soup is not None:
        try:
            for row in soup.find_all("tr"):
                cells = row.find_all(["th", "td"], recursive=False)
                if len(cells) >= 2:
                    label = re.sub(r"\s+", "", cells[0].get_text(" ", strip=True)).lower()
                    if any(key and (key in label or label in key) for key in label_set):
                        value = _compact_meta_value(cells[1].get_text(" ", strip=True))
                        if value:
                            return value
        except Exception:
            pass
        try:
            for dl in soup.find_all("dl"):
                dts = dl.find_all("dt", recursive=False)
                dds = dl.find_all("dd", recursive=False)
                for dt, dd in zip(dts, dds):
                    label = re.sub(r"\s+", "", dt.get_text(" ", strip=True)).lower()
                    if any(key and (key in label or label in key) for key in label_set):
                        value = _compact_meta_value(dd.get_text(" ", strip=True))
                        if value:
                            return value
        except Exception:
            pass
        text = soup.get_text(" ", strip=True)
    else:
        text = str(html or "")

    label_alt = "|".join(re.escape(label) for label in labels)
    match = re.search(rf"(?:{label_alt})\s*[:：]?\s*([^|\n\r]{1,120})", text, flags=re.IGNORECASE)
    if match:
        return _compact_meta_value(match.group(1))
    return ""


def _extract_simple_author_meta(html: str) -> Dict[str, str]:
    author = _extract_labeled_meta_value(
        html,
        (
            "작성자",
            "담당자",
            "담당부서",
            "작성부서",
            "등록부서",
            "부서",
            "author",
            "writer",
        ),
    )
    department = _extract_labeled_meta_value(
        html,
        (
            "담당부서",
            "작성부서",
            "등록부서",
            "처리부서",
            "부서명",
            "department",
        ),
    )
    if not department and author and any(token in author for token in ("부서", "팀", "과", "실", "담당")):
        department = author
    return {"author": author, "department": department}


def _extract_minimal_meta(workflow: Any, html: str, item: FastFilePostItem) -> Dict[str, Any]:
    reg_date = _first_text(item.reg_date)
    is_static_contents = _is_static_contents_url(item.url)
    if not reg_date and is_static_contents:
        reg_date = _extract_static_contents_update_date(html)
    if not reg_date and not is_static_contents:
        reg_date = _extract_date_with_workflow(workflow, html, item.url)
    if not reg_date and not is_static_contents:
        reg_date = _extract_date_from_text(html)

    author_info: Dict[str, Any] = {}
    try:
        author_info = extract_author_info_from_html(html or "", url=item.url) or {}
    except Exception:
        author_info = {}

    simple_author = _extract_simple_author_meta(html or "")
    author = _first_text(
        _valid_author_text(item.author),
        _valid_author_text(author_info.get("author")),
        _valid_author_text(author_info.get("content_author")),
        _valid_author_text(simple_author.get("author")),
    )
    department = _first_text(
        _valid_author_text(item.department),
        _valid_author_text(author_info.get("department")),
        _valid_author_text(simple_author.get("department")),
    )
    author_kind = str(author_info.get("author_kind") or "").strip() or None
    author_raw = _first_text(_valid_author_text(author_info.get("author_raw")), author) or None
    department_raw = _first_text(_valid_author_text(author_info.get("department_raw")), department) or None
    return {
        "reg_date": _format_board_date(reg_date) if reg_date else "",
        "author": author,
        "department": department,
        "author_kind": author_kind,
        "author_raw": author_raw,
        "department_raw": department_raw,
    }


def _file_attachment_period_decision(workflow: Any, reg_date: Any) -> tuple[bool, str]:
    """Decide whether an extracted attachment may enter Stage 3 for this period."""
    start_date = getattr(workflow, "start_date", None)
    end_date = getattr(workflow, "end_date", None)
    if not start_date and not end_date:
        return True, "period_inactive"

    text = str(reg_date or "").strip()
    if not text:
        # Preserve the existing file-crawl policy: a missing date must not
        # silently discard an otherwise valid document.
        return True, "reg_date_missing"
    try:
        parsed = parse_date(text)
    except Exception:
        parsed = None
    if parsed is None:
        return True, "reg_date_unparseable"
    if is_date_in_range(parsed, start_date, end_date):
        return True, "in_range"
    return False, "out_of_range"


def _is_generic_attachment_name(value: Any) -> bool:
    norm = re.sub(r"\s+", " ", str(value or "").strip().lower())
    compact = re.sub(r"[\s:_\-\[\]\(\)\.]+", "", norm)
    return compact in {
        "",
        "attachment",
        "attachedfile",
        "file",
        "download",
        "filedownload",
        "view",
        "preview",
        "open",
        "save",
        "다운로드",
        "내려받기",
        "받기",
        "첨부",
        "첨부파일",
        "파일",
        "파일다운로드",
        "바로보기",
        "미리보기",
    }


def _attachment_name(attachment: Dict[str, Any], fallback_title: str = "") -> str:
    name = _first_text(
        attachment.get("name"),
        attachment.get("file_name"),
        attachment.get("filename"),
        attachment.get("title"),
        "attachment",
    )
    cleaned = strip_fallback_download_label(name) or name
    if fallback_title and _is_generic_attachment_name(cleaned):
        cleaned = str(fallback_title or "").strip() or cleaned
    return cleaned

def _attachment_dedup_key(attachment: Dict[str, Any]) -> str:
    href = _attachment_url(attachment)
    if not href:
        return ""
    try:
        from utils.attachment_url_normalize import canonicalize_attachment_url_for_learn_list
        from utils.url import canonicalize_url_for_dedup

        return canonicalize_attachment_url_for_learn_list(href) or canonicalize_url_for_dedup(href) or href.lower()
    except Exception:
        return href.lower()


def _merge_attachment_lists(primary: List[Dict[str, Any]], extra: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = [x for x in (primary or []) if isinstance(x, dict)]
    seen = {_attachment_dedup_key(x) for x in merged}
    for item in extra or []:
        if not isinstance(item, dict):
            continue
        key = _attachment_dedup_key(item)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        merged.append(item)
    return merged


async def _extract_workflow_extra_attachments(workflow: Any, html: str, url: str) -> List[Dict[str, Any]]:
    extractor = getattr(workflow, "_extract_kcohesion_filelist_attachments", None)
    if not callable(extractor):
        return []
    try:
        result = extractor(html or "", base_url=url)
        if hasattr(result, "__await__"):
            result = await result
    except Exception as exc:
        logger.debug("[file-fast][extra_attachment_extract_failed] post=%s error=%s", url, exc)
        return []
    return [x for x in (result or []) if isinstance(x, dict)]

def _attachment_url(attachment: Dict[str, Any]) -> str:
    return _first_text(
        attachment.get("href"),
        attachment.get("url"),
        attachment.get("download_url"),
        attachment.get("file_url"),
    )


def _filter_noise_attachments(attachments: List[Dict[str, Any]], fallback_title: str = "") -> tuple[List[Dict[str, Any]], int]:
    kept: List[Dict[str, Any]] = []
    dropped = 0
    for attachment in attachments or []:
        if not isinstance(attachment, dict):
            continue
        name = _attachment_name(attachment, fallback_title)
        url = _attachment_url(attachment)
        if _is_noise_attachment_asset(url, name):
            dropped += 1
            logger.debug(
                "[file-fast][attachment_noise_skip] file=%s url=%s",
                name or "attachment",
                url or "",
            )
            continue
        kept.append(attachment)
    return kept, dropped

async def run_fast_file_attachment_front(
    *,
    workflow: Any,
    post_items: List[Any],
    concurrency: int = 32,
    timeout_sec: float = 20.0,
    enqueue: bool = True,
    include_attachment_details: bool = False,
    include_breadcrumb_metadata: bool = False,
    playwright_fallback_on_fetch_failure: bool = False,
    result_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
    candidate_enqueue_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
) -> Dict[str, Any]:
    raw_items = [
        item
        for item in (normalize_fast_file_post_item(value) for value in (post_items or []))
        if item
    ]
    seen_post_urls: set[str] = set()
    items: List[FastFilePostItem] = []
    source_url_duplicate_count = 0
    for item in raw_items:
        try:
            from utils.url import canonicalize_url_for_dedup

            post_key = canonicalize_url_for_dedup(item.url) or str(item.url or "").strip().lower()
        except Exception:
            post_key = str(item.url or "").split("#", 1)[0].strip().lower()
        if post_key and post_key in seen_post_urls:
            source_url_duplicate_count += 1
            continue
        if post_key:
            seen_post_urls.add(post_key)
        items.append(item)
    limit = max(1, min(int(concurrency or 1), 64))
    fetch_delay_sec = _file_fetch_enqueue_delay_sec(workflow)
    last_fetch_started_at_by_worker: Dict[int, float] = {}
    sem = asyncio.Semaphore(limit)
    work_queue: asyncio.Queue[Optional[FastFilePostItem]] = asyncio.Queue(
        maxsize=max(2, limit * 2)
    )
    results: List[Dict[str, Any]] = []
    counters = {
        "post_count": len(raw_items),
        "post_unique_count": len(items),
        "post_duplicate_skipped_count": source_url_duplicate_count,
        "post_success_count": 0,
        "post_error_count": 0,
        "attachment_count": 0,
        "enqueued_count": 0,
    }

    async def _record_result(result: Dict[str, Any]) -> None:
        results.append(result)
        if result_callback is None:
            return
        try:
            callback_result = result_callback(dict(result))
            if inspect.isawaitable(callback_result):
                await callback_result
        except Exception as exc:
            logger.warning(
                "[file-fast][result_callback_failed] job_id=%s post_url=%s err=%s",
                getattr(workflow, "job_id", ""),
                result.get("url") or "",
                exc,
            )

    def _attachment_details(attachments: List[Dict[str, Any]], fallback_title: str) -> List[Dict[str, Any]]:
        if not include_attachment_details:
            return []
        return [
            {
                "name": _attachment_name(attachment, fallback_title),
                "url": _attachment_url(attachment),
                "extension": infer_attachment_extension(
                    _attachment_name(attachment, fallback_title),
                    _attachment_url(attachment),
                ) or "",
                # Keep the source-provided byte metadata with the stage result.
                # It lets a later download-only validation stage retain the
                # production large-lane decision without re-parsing the HTML.
                "declared_file_size_bytes": int(
                    attachment.get("declared_file_size_bytes")
                    or attachment.get("_declared_file_size_bytes")
                    or 0
                ),
                "exact_file_size_bytes": int(
                    attachment.get("exact_file_size_bytes")
                    or attachment.get("_exact_file_size_bytes")
                    or 0
                ),
            }
            for attachment in attachments
            if isinstance(attachment, dict)
        ]
    logger.info(
        "[file-fast][config] posts=%s unique_posts=%s duplicate_posts_skipped=%s concurrency=%s worker_queue_max=%s fetch_enqueue_delay_sec=%.3f fetch_delay_scope=per_worker enqueue=%s source_url_dedup=enabled",
        len(raw_items),
        len(items),
        source_url_duplicate_count,
        limit,
        work_queue.maxsize,
        fetch_delay_sec,
        bool(enqueue),
    )

    def _workflow_stop_requested() -> bool:
        if bool(getattr(workflow, "_hard_stop", False) or getattr(workflow, "_stop_requested", False)):
            return True
        stop_event = getattr(workflow, "stop_event", None)
        try:
            return bool(stop_event is not None and stop_event.is_set())
        except Exception:
            return False

    async def _wait_before_fetch(url: str, reason: str, worker_no: int) -> None:
        if fetch_delay_sec <= 0:
            return
        now = time.monotonic()
        previous_started_at = float(last_fetch_started_at_by_worker.get(worker_no, 0.0) or 0.0)
        wait_sec = 0.0
        if previous_started_at > 0:
            wait_sec = max(0.0, fetch_delay_sec - max(0.0, now - previous_started_at))
        if wait_sec > 0:
            logger.debug(
                "[file-fast][fetch_delay] worker=%s reason=%s wait_sec=%.3f post=%s",
                worker_no,
                reason,
                wait_sec,
                url,
            )
            await asyncio.sleep(wait_sec)
        last_fetch_started_at_by_worker[worker_no] = time.monotonic()

    async def process(item: FastFilePostItem, worker_no: int) -> None:
        async with sem:
            try:
                if _workflow_stop_requested():
                    logger.debug("[file-fast][stop_skip] stage=before_detail_fetch job_id=%s post_url=%s", getattr(workflow, "job_id", ""), item.url)
                    return
                _log_file_url_status(
                    stage="detail_visit",
                    status="start",
                    process_url=item.url,
                    post_url=item.url,
                    selected="pending",
                    saved="pending",
                    learn="pending",
                    job_id=getattr(workflow, "job_id", ""),
                    db_name=getattr(workflow, "db_name", ""),
                )
                if _is_menu_shell_url(item.url) or _is_list_page_url(item.url):
                    skip_reason = "menu_shell" if _is_menu_shell_url(item.url) else "list_page"
                    counters["post_success_count"] += 1
                    _log_file_url_status(
                        stage="detail_visit",
                        status="skipped",
                        process_url=item.url,
                        post_url=item.url,
                        selected="no",
                        saved="no",
                        learn="not_started",
                        reason=skip_reason,
                        count=0,
                        job_id=getattr(workflow, "job_id", ""),
                        db_name=getattr(workflow, "db_name", ""),
                    )
                    logger.debug(
                        "[파일빠른추출][상세] 게시물=%s\n첨부파일=없음 추출상태=스킵_%s 첨부수=0 큐등록=0",
                        item.url,
                        _ko_fast_skip_reason(skip_reason),
                    )
                    skipped_result = {
                        "url": item.url,
                        "attachment_count": 0,
                        "enqueued_count": 0,
                        "skip_reason": skip_reason,
                    }
                    if include_attachment_details:
                        skipped_result["attachments"] = []
                    await _record_result(skipped_result)
                    return
                await _wait_before_fetch(item.url, "detail", worker_no)
                if _workflow_stop_requested():
                    logger.debug("[file-fast][stop_skip] stage=before_http_fetch job_id=%s post_url=%s", getattr(workflow, "job_id", ""), item.url)
                    return
                host = (urlparse(str(item.url or "")).hostname or "").lower()
                host_slot_acquired = False
                try:
                    if host:
                        await acquire_file_crawl_host_slot(host, requested_limit=limit)
                        host_slot_acquired = True
                    html = await _fetch_with_workflow(
                        workflow,
                        item.url,
                        timeout_sec,
                        playwright_fallback_on_fetch_failure=playwright_fallback_on_fetch_failure,
                    )
                finally:
                    if host_slot_acquired:
                        await release_file_crawl_host_slot(host)
                if _workflow_stop_requested():
                    logger.debug("[file-fast][stop_skip] stage=after_http_fetch job_id=%s post_url=%s", getattr(workflow, "job_id", ""), item.url)
                    return
                breadcrumb_cate = ""
                breadcrumb_tokens: List[str] = []
                breadcrumb_trace: Dict[str, Any] = {}
                if html:
                    original_cates = (item.cate1, item.cate2)
                    breadcrumb_probe_needed = not str(item.cate2 or "").strip()
                    item.cate1, item.cate2 = filter_unexposed_file_detail_cates(
                        html,
                        item.cate1,
                        item.cate2,
                    )
                    if original_cates != (item.cate1, item.cate2):
                        logger.debug(
                            "[file-fast][category_detail_marker_removed] job_id=%s post_url=%s before=%s after=%s",
                            getattr(workflow, "job_id", ""),
                            item.url,
                            original_cates,
                            (item.cate1, item.cate2),
                        )
                    attachments = extract_fast_attachments(html or "", item.url)
                    extract_status = "found" if attachments else "empty"
                else:
                    attachments = []
                    extract_status = "fetch_empty"
                # Avoid extra resolver/API work on pages where the fast scan already found files.
                # When the fast scan is empty, keep the resolver path for accuracy.
                if not attachments:
                    extra_attachments = await _extract_workflow_extra_attachments(workflow, html or "", item.url)
                    if extra_attachments:
                        attachments = _merge_attachment_lists(attachments, extra_attachments)
                        if attachments:
                            extract_status = "found_extra"
                        logger.debug(
                            "[file-fast][attachment_extra_workflow] post=%s extra_count=%s merged_count=%s",
                            item.url,
                            len(extra_attachments or []),
                            len(attachments or []),
                        )
                # Small pages already use their full HTML for the fast scan.
                # Re-parsing that same HTML cannot discover another attachment.
                if not attachments and html and len(html) > _light_attachment_scan_bytes():
                    full_scan_attachments = extract_fast_attachments(html or "", item.url, force_full_scan=True)
                    if full_scan_attachments:
                        logger.debug(
                            "[file-fast][attachment_rescued_full_scan] post=%s html_len=%s count=%s",
                            item.url,
                            len(html or ""),
                            len(full_scan_attachments),
                        )
                        attachments = full_scan_attachments
                        extract_status = "rescued_full_scan"
                    elif len(html or "") >= _empty_refetch_min_chars():
                        # No attachment after both local scans: do not spend another request and wait cycle.
                        logger.debug(
                            "[file-fast][attachment_empty_fast_continue] post=%s html_len=%s",
                            item.url,
                            len(html or ""),
                        )
                attachments, noise_dropped = _filter_noise_attachments(attachments, item.title)
                if attachments and (include_breadcrumb_metadata or not str(item.cate2 or "").strip()):
                    try:
                        from backend.file.file_breadcrumb import (
                            extract_file_breadcrumb_tokens_from_html,
                            extract_file_category_breadcrumb_from_html,
                            inspect_file_breadcrumb_from_html,
                        )

                        breadcrumb_tokens = extract_file_breadcrumb_tokens_from_html(
                            html or "",
                            detail_url=item.url,
                        )
                        breadcrumb_cate = extract_file_category_breadcrumb_from_html(
                            html or "",
                            detail_url=item.url,
                        )
                        breadcrumb_trace = inspect_file_breadcrumb_from_html(
                            html or "",
                            detail_url=item.url,
                        )
                    except Exception:
                        breadcrumb_cate = ""
                        breadcrumb_tokens = []
                        breadcrumb_trace = {"source": "extract_error"}
                    if breadcrumb_cate and not str(item.cate2 or "").strip():
                        item.cate2 = breadcrumb_cate
                if html and attachments and breadcrumb_probe_needed:
                    logger.info(
                        "[FileBreadcrumbTrace][resolved] job_id=%s db=%s post_url=%s source=%s "
                        "selector_hits=%s menu_info_tokens=%s tokens=%s cate_before=%s cate_after=%s attachments=%s",
                        getattr(workflow, "job_id", ""),
                        getattr(workflow, "db_name", ""),
                        item.url,
                        _clip_log_value(breadcrumb_trace.get("source"), 80),
                        _clip_log_value(breadcrumb_trace.get("selector_hits"), 300),
                        _clip_log_value(breadcrumb_trace.get("menu_info_tokens"), 300),
                        _clip_log_value(breadcrumb_trace.get("tokens") or breadcrumb_tokens, 300),
                        _clip_log_value(original_cates, 160),
                        _clip_log_value((item.cate1, item.cate2), 160),
                        len(attachments or []),
                    )
                if noise_dropped and not attachments:
                    extract_status = "noise_filtered_empty"
                elif noise_dropped:
                    extract_status = f"{extract_status}_noise_filtered"
                if noise_dropped:
                    logger.debug(
                        "[file-fast][attachment_noise_filtered] post=%s dropped=%s remaining=%s",
                        item.url,
                        noise_dropped,
                        len(attachments or []),
                    )
                _log_file_url_status(
                    stage="attachment_extract",
                    status="found" if attachments else extract_status,
                    process_url=item.url,
                    post_url=item.url,
                    selected="pending" if attachments else "no",
                    saved="pending" if attachments else "no",
                    learn="pending" if attachments else "not_started",
                    count=len(attachments or []),
                    reason=extract_status if not attachments else "",
                    job_id=getattr(workflow, "job_id", ""),
                    db_name=getattr(workflow, "db_name", ""),
                )
                reg_date = None
                author = None
                department = None
                author_kind = None
                author_raw = None
                department_raw = None
                if attachments:
                    minimal_meta = _extract_minimal_meta(workflow, html or "", item)
                    reg_date = str(minimal_meta.get("reg_date") or "").strip() or None
                    author = str(minimal_meta.get("author") or "").strip() or None
                    department = str(minimal_meta.get("department") or "").strip() or None
                    author_kind = minimal_meta.get("author_kind")
                    author_raw = minimal_meta.get("author_raw")
                    department_raw = minimal_meta.get("department_raw")
                period_allowed, period_reason = _file_attachment_period_decision(workflow, reg_date)
                queue_attachments = attachments if period_allowed else []
                period_skipped = len(attachments or []) if not period_allowed else 0
                if period_skipped:
                    logger.info(
                        "[기간필터][file][attachment] 기간 외 큐 제외 | job_id=%s post_url=%s reg_date=%s start=%s end=%s attachments=%s",
                        getattr(workflow, "job_id", ""),
                        item.url,
                        reg_date or "",
                        getattr(workflow, "start_date", None),
                        getattr(workflow, "end_date", None),
                        period_skipped,
                    )
                for attachment in attachments:
                    if not isinstance(attachment, dict):
                        continue
                    attachment["source_post_url"] = item.url
                    attachment["reg_date"] = reg_date
                    attachment["author"] = author
                    attachment["department"] = department
                    file_name = _attachment_name(attachment, item.title)
                    file_url = _attachment_url(attachment)
                    ext = infer_attachment_extension(file_name, file_url) or "unknown"
                    logger.debug(
                        "[file-fast][attachment_found] post=%s reg_date=%s author=%s file=%s ext=%s url=%s",
                        item.url,
                        reg_date or "",
                        author or department or "",
                        file_name or "attachment",
                        ext,
                        file_url or "",
                    )
                if not attachments:
                    pass

                enqueued = 0
                attachment_count = len(attachments or [])
                duplicate_skipped = 0
                enqueue_dropped = 0
                enqueue_candidate_count = 0
                enqueue_candidate_skipped = 0
                enqueue_missing = 0
                total_dropped = 0
                enqueue_reason = ""
                if (candidate_enqueue_callback is not None or enqueue) and queue_attachments:
                    detail_cates = (item.cate1, item.cate2) if (item.cate1 or item.cate2) else None
                    payload = {
                        "post_url": item.url,
                        "board_url": item.board_url or item.url,
                        "attachments": [dict(attachment) for attachment in queue_attachments if isinstance(attachment, dict)],
                        "reg_date": reg_date,
                        "author": author or department,
                        "department": department,
                        "author_kind": author_kind,
                        "author_raw": author_raw,
                        "department_raw": department_raw,
                        "detail_cates": detail_cates,
                        "post_title": item.title,
                    }
                    try:
                        callback = candidate_enqueue_callback
                        if callback is None:
                            # ``enqueue=True`` used to call the legacy
                            # workflow._enqueue_file_downloads path. Keep the
                            # public switch, but route every caller through
                            # the shared Stage-3 ingress instead.
                            callback = lambda stage3_payload: enqueue_file_crawl_stage3_candidates(
                                workflow,
                                **stage3_payload,
                            )
                        callback_result = callback(payload)
                        if inspect.isawaitable(callback_result):
                            callback_result = await callback_result
                        if isinstance(callback_result, dict):
                            enqueued = int(callback_result.get("queued", 0) or 0)
                            duplicate_skipped = int(callback_result.get("duplicate", 0) or 0)
                            enqueue_candidate_skipped = int(callback_result.get("non_document", 0) or 0)
                            enqueue_dropped = int(callback_result.get("invalid", 0) or 0)
                        else:
                            enqueued = int(callback_result or 0)
                    except Exception as exc:
                        enqueue_reason = "stage3_enqueue_failed"
                        logger.exception(
                            "[file-fast][stage3_enqueue_failed] job_id=%s post_url=%s err=%s",
                            getattr(workflow, "job_id", ""),
                            item.url,
                            exc,
                        )
                    total_dropped = enqueue_dropped + enqueue_candidate_skipped
                    enqueue_missing = max(0, attachment_count - enqueued - duplicate_skipped - enqueue_candidate_skipped - enqueue_dropped - period_skipped)
                    if not enqueue_reason:
                        enqueue_reason = "공용Stage3큐등록" if enqueued else "공용Stage3미등록"
                elif period_skipped:
                    enqueue_reason = "기간외제외"
                    _log_file_url_status(
                        stage="download_enqueue",
                        status="skipped",
                        process_url=item.url,
                        post_url=item.url,
                        selected="no",
                        saved="no",
                        learn="not_started",
                        count=period_skipped,
                        reason="out_of_range",
                        job_id=getattr(workflow, "job_id", ""),
                        db_name=getattr(workflow, "db_name", ""),
                    )
                elif False:  # Legacy _enqueue_file_downloads branch is intentionally disabled.
                    if _workflow_stop_requested():
                        logger.debug("[file-fast][stop_skip] stage=before_enqueue job_id=%s post_url=%s", getattr(workflow, "job_id", ""), item.url)
                        return
                    stats_before = getattr(workflow, "stats", None)
                    try:
                        dup_before = int((stats_before or {}).get("file_attachment_enqueue_duplicate_drop_total", 0) or 0)
                    except Exception:
                        dup_before = 0
                    try:
                        dropped_before = int((stats_before or {}).get("file_attachment_enqueue_dropped_total", 0) or 0)
                    except Exception:
                        dropped_before = 0
                    try:
                        candidate_before = int((stats_before or {}).get("file_attachment_enqueue_candidate_total", 0) or 0)
                    except Exception:
                        candidate_before = 0
                    item.cate1, item.cate2 = filter_unexposed_file_detail_cates(
                        html,
                        item.cate1,
                        item.cate2,
                    )
                    detail_cates = (item.cate1, item.cate2) if (item.cate1 or item.cate2) else None
                    sample = [
                        {
                            "name": _attachment_name(att, item.title),
                            "url": _attachment_url(att),
                        }
                        for att in attachments[:10]
                        if isinstance(att, dict)
                    ]
                    logger.debug(
                        "[file-fast][enqueue_start] post=%s attachments=%s detail_cates=%s sample=%s",
                        item.url,
                        len(attachments or []),
                        detail_cates,
                        sample,
                    )
                    queued = await workflow._enqueue_file_downloads(
                        post_url=item.url,
                        board_url=item.board_url or item.url,
                        reg_date=reg_date,
                        attachments=attachments,
                        author=author or department,
                        department=department,
                        author_kind=author_kind,
                        author_raw=author_raw,
                        department_raw=department_raw,
                        contact_phone=None,
                        view_count=None,
                        sync_after_download=True,
                        detail_cates=detail_cates,
                    )
                    try:
                        enqueued = int(queued or 0)
                    except Exception:
                        enqueued = 0
                    stats_after = getattr(workflow, "stats", None)
                    try:
                        duplicate_skipped = max(0, int((stats_after or {}).get("file_attachment_enqueue_duplicate_drop_total", 0) or 0) - dup_before)
                    except Exception:
                        duplicate_skipped = 0
                    try:
                        total_dropped = max(0, int((stats_after or {}).get("file_attachment_enqueue_dropped_total", 0) or 0) - dropped_before)
                    except Exception:
                        total_dropped = 0
                    try:
                        enqueue_candidate_count = max(0, int((stats_after or {}).get("file_attachment_enqueue_candidate_total", 0) or 0) - candidate_before)
                    except Exception:
                        enqueue_candidate_count = 0
                    enqueue_candidate_skipped = max(0, int(attachment_count or 0) - int(enqueue_candidate_count or 0))
                    enqueue_missing = max(0, int(enqueue_candidate_count or 0) - int(enqueued or 0) - int(duplicate_skipped or 0))
                    enqueue_dropped = max(0, int(total_dropped or 0) - int(duplicate_skipped or 0))
                    if enqueued == attachment_count:
                        enqueue_reason = "전체큐등록"
                    elif duplicate_skipped == attachment_count:
                        enqueue_reason = "중복스킵"
                    elif enqueue_candidate_skipped > 0 and enqueue_missing == 0 and duplicate_skipped == 0:
                        enqueue_reason = "후보제외"
                    elif duplicate_skipped > 0 and enqueue_missing == 0:
                        enqueue_reason = "일부중복"
                    elif enqueue_missing > 0:
                        enqueue_reason = "큐누락"
                    elif attachment_count > 0 and enqueued == 0:
                        enqueue_reason = "큐등록0"
                    else:
                        enqueue_reason = "일부큐등록"
                    log_enqueue_result = logger.info
                    _log_file_url_status(
                        stage="download_enqueue",
                        status="enqueued" if enqueued > 0 else "not_enqueued",
                        process_url=item.url,
                        post_url=item.url,
                        selected="yes" if enqueued > 0 else "no",
                        saved="pending" if enqueued > 0 else "no",
                        learn="skipped" if bool(getattr(workflow, "file_pipeline_skip_learning", False)) else ("pending" if enqueued > 0 else "not_started"),
                        count=enqueued,
                        reason="enqueue_zero" if enqueued <= 0 else "",
                        job_id=getattr(workflow, "job_id", ""),
                        db_name=getattr(workflow, "db_name", ""),
                    )
                    log_enqueue_result(
                        "[파일크롤링추적][큐등록] 게시물URL=%s\n작업ID=%s DB=%s 첨부수=%s 큐등록=%s 상세분류=%s 파일=%s",
                        item.url,
                        getattr(workflow, "job_id", ""),
                        getattr(workflow, "db_name", ""),
                        len(attachments or []),
                        enqueued,
                        detail_cates,
                        sample,
                    )
                    try:
                        stats = getattr(workflow, "stats", None)
                        lock = getattr(workflow, "_stats_lock", None)
                        async def _update_fast_stats() -> None:
                            stats["fast_attachment_found_count"] = int(stats.get("fast_attachment_found_count", 0) or 0) + len(attachments or [])
                            stats["fast_attachment_enqueued_count"] = int(stats.get("fast_attachment_enqueued_count", 0) or 0) + int(enqueued or 0)
                            if len(attachments or []) > 0 and enqueued <= 0:
                                stats["fast_attachment_enqueue_zero_count"] = int(stats.get("fast_attachment_enqueue_zero_count", 0) or 0) + 1
                        if isinstance(stats, dict):
                            if lock is not None:
                                async with lock:
                                    await _update_fast_stats()
                            else:
                                await _update_fast_stats()
                    except Exception:
                        pass
                if attachments:
                    file_names = [
                        _attachment_name(att, item.title)
                        for att in attachments[:3]
                        if isinstance(att, dict)
                    ]
                    logger.info(
                        "[파일빠른추출][상세] 게시물=%s\n첨부파일=있음 추출상태=%s 첨부수=%s 후보=%s 큐등록=%s 중복스킵=%s 후보제외=%s 큐누락=%s 사유=%s 파일명=%s",
                        item.url,
                        _ko_fast_extract_status(extract_status),
                        attachment_count,
                        enqueue_candidate_count,
                        enqueued,
                        duplicate_skipped,
                        enqueue_candidate_skipped,
                        enqueue_missing,
                        enqueue_reason or "-",
                        file_names or "-",
                    )
                try:
                    stats_lock = getattr(workflow, "_stats_lock", None)
                    record_scan_page = getattr(workflow, "record_file_scan_attachment_page", None)
                    if callable(record_scan_page):
                        if stats_lock is not None:
                            async with stats_lock:
                                record_scan_page(item.url, attachment_count)
                        else:
                            record_scan_page(item.url, attachment_count)
                except Exception:
                    logger.debug(
                        "[file-fast][scan_count_record_failed] job_id=%s post_url=%s",
                        getattr(workflow, "job_id", ""),
                        item.url,
                        exc_info=True,
                    )
                counters["post_success_count"] += 1
                counters["attachment_count"] += len(attachments)
                counters["enqueued_count"] += enqueued
                result = {
                    "url": item.url,
                    "reg_date": reg_date or "",
                    "period_filter": period_reason,
                    "author": author or "",
                    "department": department or "",
                    "attachment_count": len(attachments),
                    "enqueued_count": enqueued,
                }
                if include_attachment_details:
                    result["attachments"] = _attachment_details(attachments, item.title)
                if include_breadcrumb_metadata:
                    result["breadcrumb"] = {
                        "source": str(breadcrumb_trace.get("source") or "").strip(),
                        "selector_hits": [str(value) for value in (breadcrumb_trace.get("selector_hits") or [])],
                        "menu_info_tokens": [str(value) for value in (breadcrumb_trace.get("menu_info_tokens") or [])],
                        "tokens": [str(value) for value in (breadcrumb_trace.get("tokens") or breadcrumb_tokens or [])],
                        "resolved_cate": str(breadcrumb_cate or "").strip(),
                        "cate1": str(item.cate1 or "").strip(),
                        "cate2": str(item.cate2 or "").strip(),
                    }
                await _record_result(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                counters["post_error_count"] += 1
                _log_file_url_status(
                    stage="detail_visit",
                    status="error",
                    process_url=item.url,
                    post_url=item.url,
                    selected="no",
                    saved="no",
                    learn="not_started",
                    error=repr(exc),
                    job_id=getattr(workflow, "job_id", ""),
                    db_name=getattr(workflow, "db_name", ""),
                )
                logger.warning(
                    "[파일빠른추출][상세] 게시물=%s\n첨부파일=확인불가 추출상태=실패 첨부수=0 큐등록=0 오류=%s",
                    item.url,
                    exc,
                    exc_info=True,
                )
                error_result = {"url": item.url, "error": str(exc)}
                if include_attachment_details:
                    error_result["attachments"] = []
                await _record_result(error_result)

    async def worker(worker_no: int) -> None:
        while True:
            item = await work_queue.get()
            try:
                if item is None:
                    return
                await process(item, worker_no)
            finally:
                work_queue.task_done()

    workers = [
        asyncio.create_task(
            worker(worker_no),
            name=f"file-fast-attachment-worker-{getattr(workflow, 'job_id', 'unknown')}-{worker_no}",
        )
        for worker_no in range(1, limit + 1)
    ]
    try:
        for item in items:
            if _workflow_stop_requested():
                break
            await work_queue.put(item)
        await work_queue.join()
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise
    else:
        for _ in workers:
            await work_queue.put(None)
        await asyncio.gather(*workers)
    return {**counters, "results": results}


