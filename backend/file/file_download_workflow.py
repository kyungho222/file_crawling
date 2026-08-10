"""
게시판 상세에서 첨부 링크만 추출·다운로드하는 전용 워크플로우.

- BoardContentFilePipelineMixin + FileCrawlBoardMixin(파일 전용 통계/SSE 별칭) + BoardContentWorkflow
- 기본: 저장 후 학습 파이프라인까지 수행(file_pipeline_skip_learning=False).
  다운로드만 하려면 workflow.file_pipeline_skip_learning=True 또는
  BOARD_FILE_DOWNLOAD_SKIP_LEARNING=1 / 요청 file_pipeline_skip_learning=true
- 인터페이스: BoardContentWorkflow와 동일 (start_workflow, get_stats, stop)
- 게시판과 분리: start_workflow 이후에만 _category 테이블 기반 파일 분류 매핑은 LEARN_LIST 저장 단계에서 수행한다.
  (파일 크롤은 url_pattern/cate_match를 사용하지 않는다.)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse

from config.settings import settings
from backend.board.board_content_workflow import (
    BoardContentWorkflow,
    WorkflowState,
    static_html_insufficient_for_detail_playwright,
)
from backend.board.anseong_file import (
    clean_anseong_attachment_name,
    is_anseong_file_url,
    resolve_anseong_yhlib_download_url,
)
from backend.board.gm_file import extract_gm_nftc_filelist_attachments
from backend.board.yongin_board import resolve_yongin_file_download_url
from backend.board.yongin_water_board import is_yongin_water_attachment_detail_url
from backend.file.file_crawl_board_mixin import FileCrawlBoardMixin, ensure_file_study_stat_keys
from backend.file.file_detail_category import (
    filter_unexposed_file_detail_cates,
    normalize_file_detail_cates,
)
from backend.file.file_detail_meta import extract_file_detail_meta_from_html
from backend.board.chuncheon_contract import (
    is_chuncheon_contract_detail_url as _is_chuncheon_contract_detail_url,
    is_chuncheon_contract_url as _is_chuncheon_contract_url,
)
from backend.board.file_content_workflow import BoardContentFilePipelineMixin
from backend.shared.file_crawl_post_urls import (
    _missing_required_file_detail_query_reason,
    _normalize_cate_codes_upper,
)
from backend.shared.selector_profile_store import get_profile, make_profile_key
from utils.file import strip_fallback_download_label, strip_trailing_file_size
from utils.url import canonicalize_url_for_dedup, extract_download_url_from_js, normalize_attachment_href

logger = logging.getLogger("backend.file.file_download_workflow")


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


def _compact_text(value: Any) -> str:
    """파일명 비교를 위한 공백·기호 제거 정규화값."""
    try:
        return re.sub(r"[^0-9a-z가-힣]+", "", str(value or "").lower())
    except Exception:
        return ""


def _file_crawl_fast_front_concurrency(kwargs: Dict[str, Any]) -> int:
    # File crawling uses one shared concurrency baseline for discovery and download.
    try:
        value = int(getattr(settings, "DOWNLOAD_WORKERS", 4) or 4)
    except Exception:
        value = 4
    return max(1, min(value, 16))

def _file_crawl_detail_fetch_timeout_sec(kwargs: Dict[str, Any]) -> float:
    raw = (
        kwargs.get("file_crawl_fetch_timeout_sec")
        or os.getenv("FILE_CRAWL_DETAIL_FETCH_TIMEOUT_SEC")
        or "30"
    )
    try:
        value = float(raw)
    except Exception:
        value = 30.0
    return max(1.0, min(value, 120.0))


_LIST_PAGE_PATH_RE = re.compile(r"(?:^|/)(?:list|index)\.(?:do|jsp|php|asp|aspx|html?)$|(?:^|/)list$", re.IGNORECASE)


def _file_detail_query_keys() -> set[str]:
    return {
        "ntt_id", "nttid", "bbs_id", "bbsid", "board_id", "boardid",
        "article_id", "articleid", "seq", "no", "idx", "sn", "id",
        "bdid", "bmid", "pst_id", "pstid", "post_id", "postid",
    }


def _is_file_crawl_list_page_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return False
    if not _LIST_PAGE_PATH_RE.search(str(parsed.path or "")):
        return False
    query = parse_qs(parsed.query or "", keep_blank_values=True)
    lowered_keys = {str(key or "").lower() for key in query}
    return not (lowered_keys & _file_detail_query_keys())


def _content_author_debug_enabled() -> bool:
    return str(
        os.getenv("FILE_CONTENT_AUTHOR_DEBUG", os.getenv("CONTENT_AUTHOR_DEBUG", "0"))
        or "0"
    ).strip().lower() in {"1", "true", "yes", "on"}


def _content_author_debug_value(value: Any, limit: int = 180) -> str:
    try:
        text = str(value or "").strip()
    except Exception:
        text = ""
    return text[:limit]


def _extract_file_author_info(
    html: str,
    *,
    url: str,
    selector_profile: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Extract author fields with file-crawl-only metadata logic."""
    file_info = extract_file_detail_meta_from_html(html, url=url)
    merged: Dict[str, Any] = {
        "author": file_info.get("author") or "",
        "department": file_info.get("department") or "",
    }
    merged["content_author"] = merged.get("author") or merged.get("department") or ""
    if merged.get("content_author"):
        merged["author_raw"] = merged["content_author"]
        merged["author_kind"] = "file_detail_meta"

    if _content_author_debug_enabled():
        logger.warning(
            "[ContentAuthorDebug][file_detail.meta_extract] url=%s result=%r author=%r department=%r kind=%r",
            (url or "")[:220],
            _content_author_debug_value(merged.get("content_author")),
            _content_author_debug_value(merged.get("author")),
            _content_author_debug_value(merged.get("department")),
            _content_author_debug_value(merged.get("author_kind")),
        )
    if merged.get("author") or merged.get("department") or merged.get("content_author"):
        return merged

    if _content_author_debug_enabled():
        logger.debug(
            "[ContentAuthorDebug][file_detail.selector_fallback_empty] url=%s author=%r department=%r kind=%r",
            (url or "")[:220],
            _content_author_debug_value(merged.get("author")),
            _content_author_debug_value(merged.get("department")),
            _content_author_debug_value(merged.get("author_kind")),
        )
    return merged


def _valid_contents_url(value: Any) -> Optional[str]:
    try:
        text = str(value or "").strip()
    except Exception:
        return None
    if not text or text.lower() in {"http", "https", "http://", "https://"}:
        return None
    try:
        parsed = urlparse(text if "://" in text else f"https://{text}")
        if not parsed.netloc or "." not in parsed.netloc:
            return None
    except Exception:
        return None
    return text


def _file_crawl_is_preview_only_href(url: str) -> bool:
    """성동구 등 previewBbs.do 계열은 뷰어용이므로 파일크롤 대상에서 제외."""
    u = (url or "").strip().lower()
    return "previewbbs" in u


def _file_crawl_is_preview_link_label(*, link_text: str = "", title: str = "", name: str = "") -> bool:
    """앵커가 '미리보기'인 링크는 실제 다운로드가 아니면 제외하기 위한 라벨 검사."""
    for s in (link_text, title, name):
        if s and "미리보기" in s:
            return True
    return False


def _split_js_call_args(arg_text: str) -> list[str]:
    args: list[str] = []
    cur: list[str] = []
    quote = ""
    escape = False
    for ch in str(arg_text or ""):
        if escape:
            cur.append(ch)
            escape = False
            continue
        if ch == "\\":
            cur.append(ch)
            escape = True
            continue
        if quote:
            cur.append(ch)
            if ch == quote:
                quote = ""
            continue
        if ch in ("'", '"'):
            cur.append(ch)
            quote = ch
            continue
        if ch == ",":
            args.append("".join(cur).strip())
            cur = []
            continue
        cur.append(ch)
    if cur or arg_text:
        args.append("".join(cur).strip())
    out: list[str] = []
    for arg in args:
        a = arg.strip()
        if len(a) >= 2 and a[0] in ("'", '"') and a[-1] == a[0]:
            a = a[1:-1]
        out.append(a)
    return out


def _parse_js_function_call(js_text: str) -> Optional[Tuple[str, list[str]]]:
    text = str(js_text or "").strip()
    if text.lower().startswith("javascript:"):
        text = text.split(":", 1)[1].strip()
    m = re.search(r"([A-Za-z_$][\w$]*)\s*\((.*?)\)", text, re.DOTALL)
    if not m:
        return None
    return m.group(1), _split_js_call_args(m.group(2) or "")


def _find_js_function_body(html: str, function_name: str) -> Optional[Tuple[list[str], str]]:
    if not html or not function_name:
        return None
    pattern = re.compile(
        r"function\s+" + re.escape(function_name) + r"\s*\((?P<params>[^)]*)\)\s*\{",
        re.IGNORECASE,
    )
    m = pattern.search(html)
    if not m:
        return None
    start = m.end()
    depth = 1
    quote = ""
    escape = False
    for idx in range(start, len(html)):
        ch = html[idx]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if quote:
            if ch == quote:
                quote = ""
            continue
        if ch in ("'", '"'):
            quote = ch
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth <= 0:
                params = [p.strip() for p in (m.group("params") or "").split(",") if p.strip()]
                return params, html[start:idx]
    return None


def _evaluate_simple_js_url_expression(expr: str, arg_map: Dict[str, str]) -> str:
    text = str(expr or "").strip()
    if not text:
        return ""
    parts: list[str] = []
    for token in re.finditer(
        r"""(?P<quote>['"])(?P<literal>.*?)(?P=quote)|encodeURIComponent\s*\(\s*(?P<enc>[A-Za-z_$][\w$]*)\s*\)|(?P<ident>[A-Za-z_$][\w$]*)""",
        text,
        re.DOTALL,
    ):
        if token.group("literal") is not None:
            parts.append(token.group("literal") or "")
        elif token.group("enc"):
            parts.append(str(arg_map.get(token.group("enc"), "")))
        elif token.group("ident"):
            ident = token.group("ident")
            if ident in arg_map:
                parts.append(str(arg_map.get(ident, "")))
    return "".join(parts).strip()


def _resolve_onclick_via_script_function(onclick: str, html: str, base_url: str) -> str:
    call = _parse_js_function_call(onclick)
    if not call:
        return ""
    function_name, args = call
    found = _find_js_function_body(html, function_name)
    if not found:
        return ""
    params, body = found
    arg_map = {name: args[idx] for idx, name in enumerate(params) if idx < len(args)}
    expr = ""
    m = re.search(
        r"(?:location\.href|window\.location\.href|document\.location\.href)\s*=\s*(?P<expr>[^;]+)",
        body,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        expr = m.group("expr") or ""
    else:
        m = re.search(r"window\.open\s*\(\s*(?P<expr>[^,\)]+)", body, re.IGNORECASE | re.DOTALL)
        if m:
            expr = m.group("expr") or ""
    resolved = _evaluate_simple_js_url_expression(expr, arg_map)
    if not resolved:
        return ""
    try:
        return urljoin(base_url, resolved)
    except Exception:
        return resolved

_STATIC_CONTENTS_PATH_RE = re.compile(r"(?:^|/)main/contents\.do$", re.IGNORECASE)
_CONTENTS_UPDATE_LABELS = (
    "\ucd5c\uc885\uc5c5\ub370\uc774\ud2b8",
    "\ucd5c\uc885\uc218\uc815\uc77c",
    "\ucd5c\uc885\uc218\uc815",
    "\uc218\uc815\uc77c",
    "\uc5c5\ub370\uc774\ud2b8",
)


def _is_static_contents_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return False
    return bool(_STATIC_CONTENTS_PATH_RE.search(str(parsed.path or "")))


def _extract_static_contents_update_datetime(soup: Any, html: str = "") -> Any:
    labels_compact = {re.sub(r"\s+", "", label).lower() for label in _CONTENTS_UPDATE_LABELS}
    text = ""
    if soup is not None:
        try:
            for dl in soup.find_all("dl"):
                dts = dl.find_all("dt", recursive=False)
                dds = dl.find_all("dd", recursive=False)
                for dt, dd in zip(dts, dds):
                    label = re.sub(r"\s+", "", dt.get_text(" ", strip=True)).lower()
                    if any(key and key in label for key in labels_compact):
                        value = dd.get_text(" ", strip=True)
                        if value:
                            from backend.shared.date_utils import parse_date

                            return parse_date(value)
        except Exception:
            pass
        try:
            text = soup.get_text(" ", strip=True)
        except Exception:
            text = ""
    if not text:
        text = str(html or "")
    label_alt = "|".join(re.escape(label) for label in _CONTENTS_UPDATE_LABELS)
    match = re.search(
        rf"(?:{label_alt})\s*[:\uff1a]?\s*((?:19|20)\d{{2}}[-./\ub144\s]+\d{{1,2}}[-./\uc6d4\s]+\d{{1,2}}(?:\uc77c)?)",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        try:
            from backend.shared.date_utils import parse_date

            return parse_date(match.group(1))
        except Exception:
            return None
    return None



def _is_chorogusan_results_reporting_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    return "chorogusan.or.kr" in host and path.endswith("/contents/resultsreportingview.do")


def _is_file_generic_report_label(value: Any) -> bool:
    text = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    compact = re.sub(r"[\s:_\-\[\]\(\)\.▶▷>]+", "", text)
    return compact in {
        "",
        "pdf",
        "pdf로보기",
        "pdf보기",
        "pdf다운로드",
        "파일다운로드",
        "다운로드",
        "보기",
        "바로보기",
        "새창열림",
        "새창열기",
    }

class FileDownloadWorkflow(BoardContentFilePipelineMixin, FileCrawlBoardMixin, BoardContentWorkflow):
    """첨부 수집 전용. collection_count는 LEARN_LIST 저장 성공 시 save_count와 동기화(선별=저장 눈금)."""

    def __init__(self) -> None:
        super().__init__()
        ensure_file_study_stat_keys(self)
        self.colle = "file"
        self.is_attachment_file_crawl_workflow = True
        self.sync_after_download = True
        try:
            file_learn_conc = int(getattr(settings, "FILE_CRAWL_LEARN_CONCURRENCY", 2) or 2)
        except Exception:
            file_learn_conc = 2
        file_learn_conc = max(1, min(file_learn_conc, 32))
        self._file_learn_concurrency = file_learn_conc
        self._learn_sem = asyncio.Semaphore(file_learn_conc)
        self._preexplored_start_urls_lock_scan_total = False
        self._sync_scan_count_before_finalize_snapshot = self._file_crawl_finalize_scan_count
        self.file_pipeline_skip_learning = False
        self._seen_file_urls: Set[str] = set()
        self._file_job_queues: Any = None
        self._file_worker_manager: Any = None
        self._file_worker_task: Optional[asyncio.Task] = None
        self._file_progress_task: Optional[asyncio.Task] = None
        self._file_pipeline_lock = asyncio.Lock()
        self._file_enqueue_lock = asyncio.Lock()
        self._file_parallel_learn_tasks: Set[asyncio.Task] = set()
        self._file_saved_learn_list_ids: Set[int] = set()
        self._url_to_cate_map: Dict[str, str] = {}
        self._file_job_started_monotonic = time.monotonic()
        self._file_job_summary_logged = False

    def _file_summary_int(self, stats: Dict[str, Any], *keys: str) -> int:
        for key in keys:
            try:
                value = int(stats.get(key, 0) or 0)
            except Exception:
                value = 0
            if value:
                return value
        return 0

    def _log_file_job_summary_once(self) -> None:
        if getattr(self, "_file_job_summary_logged", False):
            return
        self._file_job_summary_logged = True
        try:
            stats = self.get_stats() if hasattr(self, "get_stats") else dict(getattr(self, "stats", {}) or {})
        except Exception:
            stats = dict(getattr(self, "stats", {}) or {})
        total = self._file_summary_int(stats, "total_count", "scan_count")
        save_success = self._file_summary_int(stats, "save_success_count", "save_count")
        save_failed = self._file_summary_int(stats, "save_failed_count")
        learn_success = self._file_summary_int(stats, "file_study_success_count", "study_success_count", "study_count")
        learn_failed = self._file_summary_int(stats, "file_study_failed_count", "study_failed_count")
        learn_done = self._file_summary_int(stats, "file_study_done_count", "study_done_count")
        download_skipped = self._file_summary_int(stats, "file_download_skipped_count")
        selected = self._file_summary_int(stats, "file_attachment_found_total_count", "file_attachment_found_count")
        detail_success = self._file_summary_int(stats, "file_detail_fetch_success_count")
        detail_empty = self._file_summary_int(stats, "file_detail_no_html_count", "file_attachment_extract_empty_count")
        try:
            elapsed = max(0.0, time.monotonic() - float(getattr(self, "_file_job_started_monotonic", time.monotonic())))
        except Exception:
            elapsed = 0.0
        failed_total = max(save_failed, learn_failed, detail_empty)
        logger.info(
            "[Job Summary] job_id=%s db=%s total=%s success=%s failed=%s elapsed=%.1fs | detail=%s/%s download=%s/%s save=%s/%s learn=%s/%s skipped=%s",
            getattr(self, "job_id", ""),
            getattr(self, "db_name", ""),
            total,
            save_success,
            failed_total,
            elapsed,
            detail_success,
            total,
            save_success,
            max(selected, save_success + download_skipped),
            save_success,
            max(save_success + save_failed, save_success),
            learn_success,
            max(learn_done, learn_success + learn_failed),
            download_skipped,
        )

    def _extract_board_reg_date(self, soup: Any, html: str = "", url: str = "") -> Any:
        if _is_static_contents_url(url):
            return _extract_static_contents_update_datetime(soup, html=html)
        return super()._extract_board_reg_date(soup, html=html, url=url)

    def _extract_chorogusan_results_title(self, soup: Any, html: str = "") -> str:
        selectors = ("strong#title", "#title", ".title_top strong", ".title_area strong")
        for selector in selectors:
            try:
                node = soup.select_one(selector) if soup is not None else None
                text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip() if node else ""
            except Exception:
                text = ""
            if text and not _is_file_generic_report_label(text):
                return text
        try:
            meta = soup.select_one("meta[property=\"og:title\"]") if soup is not None else None
            text = re.sub(r"\s+", " ", str(meta.get("content") or "")).strip() if meta else ""
            if text and not _is_file_generic_report_label(text):
                return text
        except Exception:
            pass
        return ""

    def _extract_board_title(self, soup: Any, url: str = "", html: str = "") -> str:
        if _is_chorogusan_results_reporting_url(url):
            title = self._extract_chorogusan_results_title(soup, html=html)
            if title:
                return title
        return super()._extract_board_title(soup, url=url, html=html)


    async def _get_selector_profile_for_detail(
        self,
        *,
        url: str,
        board_url: str = "",
    ) -> Optional[Dict[str, Any]]:
        try:
            domain = (urlparse(url or "").netloc or "").strip().lower()
            profile_key = make_profile_key(domain, board_url or "")
            if not profile_key:
                return None
            cached = self._selector_profile_cache.get(profile_key)
            if isinstance(cached, dict):
                return cached
            profile = await get_profile(profile_key)
            if isinstance(profile, dict):
                self._selector_profile_cache[profile_key] = profile
                return profile
        except Exception:
            return None
        return None

    def _extract_attachment_actions_generic(self, html: str, *, base_url: str, soup: Any) -> list[Dict[str, Any]]:
        actions: list[Dict[str, Any]] = []
        if not html or soup is None:
            return actions

        def _name_from_node(node: Any) -> str:
            try:
                title_attr = (node.get("title") or node.get("aria-label") or "").strip()
                value_attr = (node.get("value") or node.get("alt") or "").strip()
                link_text = (node.get_text(" ", strip=True) or value_attr or "").strip()
                return title_attr or link_text or value_attr or "attachment"
            except Exception:
                return "attachment"

        def _form_target_from_submit(onclick: str) -> str:
            text = str(onclick or "")
            patterns = (
                r"document\.([A-Za-z_$][\w$]*)\.submit\s*\(",
                r"document\.forms\s*\[\s*['\"]([^'\"]+)['\"]\s*\]\.submit\s*\(",
                r"getElementById\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\.submit\s*\(",
                r"\$\s*\(\s*['\"]#([^'\"]+)['\"]\s*\)\.submit\s*\(",
            )
            for pat in patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    return (m.group(1) or "").strip()
            return ""

        def _find_form(form_name: str, trigger: Any) -> Any:
            if form_name:
                try:
                    found = soup.find("form", attrs={"name": form_name}) or soup.find("form", attrs={"id": form_name})
                    if found is not None:
                        return found
                except Exception:
                    pass
            try:
                return trigger.find_parent("form")
            except Exception:
                return None

        def _collect_form_params(form: Any) -> Dict[str, str]:
            params: Dict[str, str] = {}
            try:
                fields = form.find_all(["input", "select", "textarea"])
            except Exception:
                fields = []
            for field in fields:
                try:
                    name = (field.get("name") or "").strip()
                    if not name:
                        continue
                    value = (field.get("value") or "").strip()
                    params[name] = value
                except Exception:
                    continue
            return params


        for node in soup.find_all(["a", "button", "input"]):
            try:
                onclick = (node.get("onclick") or "").strip()
            except Exception:
                onclick = ""
            if not onclick:
                continue
            name = _name_from_node(node)

            resolved = (
                extract_download_url_from_js(onclick, base_url)
                or resolve_anseong_yhlib_download_url(onclick, base_url)
                or _resolve_onclick_via_script_function(onclick, html, base_url)
            )
            if resolved:
                actions.append(
                    {
                        "kind": "onclick",
                        "name": name,
                        "url": resolved,
                        "href": resolved,
                        "method": "GET",
                        "params": {},
                        "raw": onclick,
                        "needs_response_validation": True,
                    }
                )
                continue

            if "submit" not in onclick.lower():
                continue
            form_name = _form_target_from_submit(onclick)
            form = _find_form(form_name, node)
            if form is None:
                continue
            try:
                action = (form.get("action") or "").strip()
                method = (form.get("method") or "GET").strip().upper()
            except Exception:
                action = ""
                method = "GET"
            if not action:
                continue
            params = _collect_form_params(form)
            full_action = urljoin(base_url, action)
            href = full_action
            if method == "GET" and params:
                sep = "&" if "?" in href else "?"
                href = f"{href}{sep}{urlencode(params)}"
            actions.append(
                {
                    "kind": "form",
                    "name": name,
                    "url": full_action,
                    "href": href,
                    "method": method,
                    "params": params,
                    "raw": onclick,
                    "needs_response_validation": True,
                }
            )

        return actions

    def _extract_attachment_links_generic(self, html: str, *, base_url: str) -> list[Dict[str, Any]]:
        """
        파일크롤 전용 첨부 후보 수집 보강.
        - board 공통 추출 결과를 유지하면서, file 모드에서만 download 핸들러/onclick 패턴을 추가 수집
        - 확장자 표기가 없는 downloadBbsFile 계열도 첨부 후보로 허용

        Recall-first boundary: this method should collect broad attachment-like
        candidates and avoid strong filtering. Final file validation belongs to
        the download/response-validation stage.
        """
        base_items = super()._extract_attachment_links_generic(html, base_url=base_url)
        out: list[Dict[str, Any]] = []
        seen: set[str] = set()
        seen_index: dict[str, int] = {}
        soup_for_title = None
        page_title_for_file = ""
        if _is_chorogusan_results_reporting_url(base_url):
            try:
                from bs4 import BeautifulSoup

                soup_for_title = BeautifulSoup(html or "", "html.parser")
                page_title_for_file = self._extract_chorogusan_results_title(soup_for_title, html=html)
            except Exception:
                soup_for_title = None
                page_title_for_file = ""

        def _is_generic_download_name(value: str) -> bool:
            norm = re.sub(r"\s+", " ", str(value or "").strip().lower())
            compact = re.sub(r"[\s:_\-\[\]\(\)\.]+", "", norm)
            return norm in {
                "download",
                "file download",
                "attachment",
                "attached file",
                "file",
                "view",
                "preview",
                "open",
                "save",
                "\uc0c8\ucc3d\uc5f4\ub9bc",
                "\uc0c8\ucc3d\uc5f4\uae30",
                "\uc0c8\ucc3d",
                "\uc0c8 \ucc3d \uc5f4\ub9bc",
                "\uc0c8 \ucc3d \uc5f4\uae30",
                "\uc0c8\ucc3d\uc73c\ub85c\uc5f4\ub9bc",
                "\uc0c8\ucc3d\uc73c\ub85c\uc5f4\uae30",
                "?ㅼ슫濡쒕뱶",
                "?대젮諛쏄린",
                "諛쏄린",
                "\ub2e4\uc6b4\ub85c\ub4dc",
                "\ub0b4\ub824\ubc1b\uae30",
                "\ubc1b\uae30",
                "\ucca8\ubd80",
                "\ucca8\ubd80\ud30c\uc77c",
                "\ud30c\uc77c",
                "\ud30c\uc77c\ub2e4\uc6b4\ub85c\ub4dc",
                "\ubc14\ub85c\ubcf4\uae30",
                "\ubbf8\ub9ac\ubcf4\uae30",
            } or compact in {
                "download",
                "filedownload",
                "attachment",
                "attachedfile",
                "file",
                "view",
                "preview",
                "open",
                "save",
                "\ub2e4\uc6b4\ub85c\ub4dc",
                "\ub0b4\ub824\ubc1b\uae30",
                "\ubc1b\uae30",
                "\ucca8\ubd80",
                "\ucca8\ubd80\ud30c\uc77c",
                "\ud30c\uc77c",
                "\ud30c\uc77c\ub2e4\uc6b4\ub85c\ub4dc",
                "\ubc14\ub85c\ubcf4\uae30",
                "\ubbf8\ub9ac\ubcf4\uae30",
            }
        def _clean_attachment_name(value: str) -> str:
            cleaned = re.sub(r"\s+", " ", (value or "").strip())
            if is_anseong_file_url(base_url):
                cleaned = clean_anseong_attachment_name(cleaned)
            cleaned = re.sub(r"\s*(?:다운로드|내려받기|받기)\s*$", "", cleaned, flags=re.IGNORECASE)
            cleaned = strip_trailing_file_size(cleaned)
            cleaned = strip_fallback_download_label(cleaned) or cleaned
            return cleaned.strip()

        def _is_generic_file_icon_name(value: str) -> bool:
            cleaned = _clean_attachment_name(value)
            stem, ext = os.path.splitext(cleaned)
            return bool(ext and _is_generic_download_name(stem))

        def _is_better_file_name(candidate: str, current: str) -> bool:
            c = _clean_attachment_name(candidate)
            cur = _clean_attachment_name(current)
            if not c:
                return False
            if (_is_generic_download_name(cur) or _is_generic_file_icon_name(cur)) and not _is_generic_download_name(c):
                return True
            if "." in c and "." not in cur:
                return True
            return False
        def _pick_file_name(title_attr: str, link_text: str) -> str:
            title_s = _clean_attachment_name(title_attr)
            text_s = _clean_attachment_name(link_text)
            if _is_generic_download_name(title_s):
                return "" if _is_generic_download_name(text_s) else text_s
            if text_s and "." in text_s and title_s.startswith(text_s):
                return text_s
            return title_s or text_s

        def _is_generic_image_view_label(value: str) -> bool:
            label = _clean_attachment_name(value)
            if not label:
                return False
            norm = re.sub(r"\s+", " ", label.strip().lower())
            compact = re.sub(r"[\s:_\-\[\]\(\)\.]+", "", norm)
            return (
                _is_generic_download_name(label)
                or norm in {"원본 이미지 보기", "원본이미지 보기", "이미지 보기", "사진 보기", "큰 이미지 보기"}
                or compact in {"원본이미지보기", "이미지보기", "사진보기", "큰이미지보기", "viewimage", "openimage"}
            )

        def _append_url_extension_if_missing(name: str, url: str) -> str:
            clean_name = _clean_attachment_name(name)
            if not clean_name or "." in clean_name:
                return clean_name
            try:
                path = urlparse(str(url or "")).path.lower()
            except Exception:
                path = str(url or "").lower()
            ext = os.path.splitext(path)[1]
            if ext in file_exts:
                return f"{clean_name}{ext}"
            return clean_name

        def _file_name_from_url(url: str) -> str:
            try:
                path = urlparse(str(url or "")).path
                name = unquote(os.path.basename(path or ""))
            except Exception:
                name = ""
            name = _clean_attachment_name(name)
            if name and not _is_generic_download_name(name) and _has_any_file_ext(name):
                return name
            return ""

        def _page_title_file_name(url: str) -> str:
            title = _clean_attachment_name(page_title_for_file)
            if not title:
                return ""
            try:
                ext = os.path.splitext(urlparse(str(url or "")).path.lower())[1]
            except Exception:
                ext = ""
            if ext not in file_exts:
                return title
            return title if title.lower().endswith(ext) else f"{title}{ext}"

        def _image_alt_name(node: Any, url: str = "") -> str:
            names: list[str] = []
            try:
                images = node.find_all("img") if hasattr(node, "find_all") else []
            except Exception:
                images = []
            for img in images:
                try:
                    for attr in ("alt", "title", "aria-label", "data-alt"):
                        val = _clean_attachment_name(img.get(attr) or "")
                        if val and not _is_generic_image_view_label(val):
                            names.append(_append_url_extension_if_missing(val, url))
                except Exception:
                    continue
            for name in names:
                if name:
                    return name
            return ""

        def _pick_anchor_file_name(node: Any, title_attr: str, link_text: str, url: str = "") -> str:
            title_s = _clean_attachment_name(title_attr)
            text_s = _clean_attachment_name(link_text)
            img_s = _image_alt_name(node, url)
            picked = _pick_file_name(title_s, text_s)
            if img_s and (
                not picked
                or _is_generic_image_view_label(picked)
                or ("." in img_s and "." not in picked)
            ):
                return img_s
            return picked or img_s

        def _is_share_or_social_href(value: str) -> bool:
            low = str(value or "").lower()
            if not low:
                return False
            share_hosts = (
                "share.naver.com",
                "facebook.com/sharer",
                "twitter.com/share",
                "x.com/share",
                "story.kakao.com/share",
                "serviceapi.nmv.naver.com",
            )
            share_paths = (
                "/share",
                "/sns/",
                "/social/",
            )
            if any(host in low for host in share_hosts):
                return True
            return any(path in low for path in share_paths) and not any(hint in low for hint in ("file", "download", "attach"))

        def _is_noise_attachment_container(node: Any) -> bool:
            cur = node
            for _ in range(8):
                if cur is None:
                    return False
                try:
                    if str(getattr(cur, "name", "") or "").lower() in {"nav", "header", "footer", "aside"}:
                        return True
                    attrs: list[str] = []
                    for attr in ("id", "class", "role", "aria-label"):
                        val = cur.get(attr) if hasattr(cur, "get") else None
                        if isinstance(val, list):
                            attrs.extend(str(x) for x in val if x)
                        elif val:
                            attrs.append(str(val))
                    blob = " ".join(attrs).lower()
                    if any(token in blob for token in (
                        "gnb", "lnb", "snb", "menu", "nav", "breadcrumb",
                        "footer", "header", "quick", "sns", "share", "depth", "side",
                        "family-site", "family_site", "related-site", "related_site",
                        "major-site", "major_site", "shortcut", "site-link", "site_link",
                    )):
                        return True
                except Exception:
                    pass
                cur = getattr(cur, "parent", None)
            return False

        def _is_noise_attachment_asset(href: Any, name: Any = "") -> bool:
            low_href = str(href or "").strip().lower()
            low_name = str(name or "").strip().lower()
            compact_name = _compact_text(low_name)
            if not low_href and not low_name:
                return False
            if compact_name == "rfc2350" and "atchfileid=file_000000000070941" in low_href:
                return True
            if "img_wa" in low_href or "webwatch" in low_href:
                return True
            if "wa 품질인증" in low_name or "웹와치" in low_name or "webwatch" in low_name:
                return True
            return False

        def _candidate_score_reason(
            *,
            has_ext: bool = False,
            is_download_handler: bool = False,
            has_attachment_context: bool = False,
            source: str = "",
        ) -> tuple[float, str]:
            source_key = str(source or "").strip().lower()
            if source_key in {"base", "gm", "ne_direct_file", "onclick", "form", "script_url", "script_id", "egov_pair", "nd_file_pair"}:
                source_scores = {
                    "base": 0.86,
                    "gm": 0.95,
                    "ne_direct_file": 0.92,
                    "onclick": 0.82,
                    "form": 0.78,
                    "script_url": 0.72,
                    "script_id": 0.56,
                    "egov_pair": 0.88,
                    "nd_file_pair": 0.90,
                }
                return source_scores[source_key], source_key
            if has_ext and is_download_handler:
                return 0.9, "extension+download_handler"
            if has_ext:
                return 0.76, "extension"
            if is_download_handler:
                return 0.68, "download_handler"
            if has_attachment_context:
                return 0.42, "attachment_context"
            return 0.2, "weak_candidate"

        def _is_broad_attachment_candidate(*, full: str, context_text: str) -> bool:
            return bool(str(full or "").strip()) and _has_attachment_context(context_text)

        def _push(u: str, n: str, extra: Optional[Dict[str, Any]] = None) -> None:
            uu = str(u or "").strip()
            if not uu:
                return
            low_uu = uu.lower()
            if "webtrans.llsollu.com" in low_uu and ("/ezweb" in low_uu or "/ezweb/translate" in low_uu):
                logger.debug(
                    "[FileUrlTrace][attachment_extract.skip_translation_proxy] base_url=%s href=%s name=%s",
                    str(base_url or "")[:240],
                    uu[:240],
                    str(n or "")[:120],
                )
                return
            if _is_share_or_social_href(uu):
                logger.debug(
                    "[FileUrlTrace][attachment_extract.skip_share_link] base_url=%s href=%s name=%s",
                    str(base_url or "")[:240],
                    uu[:240],
                    str(n or "")[:120],
                )
                return
            nn = _clean_attachment_name(str(n or "")) or "attachment"
            if _is_noise_attachment_asset(uu, nn):
                logger.debug(
                    "[FileUrlTrace][attachment_extract.skip_noise_asset] base_url=%s href=%s name=%s",
                    str(base_url or "")[:240],
                    uu[:240],
                    nn[:120],
                )
                return
            meta = dict(extra or {})
            original_name = str(meta.get("name") or meta.get("display_name") or n or "").strip()
            if _is_generic_download_name(original_name) and not _is_generic_download_name(nn):
                original_name = nn
                for key_name in ("display_name", "attachment_name", "original_name"):
                    if _is_generic_download_name(str(meta.get(key_name) or "")):
                        meta[key_name] = nn
            method = str(meta.get("method") or "GET").strip().upper()
            params = meta.get("params") or {}
            try:
                params_key = urlencode(sorted((str(k), str(v)) for k, v in dict(params).items()))
            except Exception:
                params_key = str(params or "")
            url_key = canonicalize_url_for_dedup(uu) or uu
            key = f"{url_key}|{method}|{params_key}"
            if key in seen:
                idx = seen_index.get(key)
                if idx is not None and 0 <= idx < len(out) and _is_better_file_name(nn, out[idx].get("name", "")):
                    out[idx]["name"] = nn
                    out[idx]["display_name"] = nn
                    out[idx]["attachment_name"] = nn
                    out[idx]["original_name"] = nn
                if idx is not None and 0 <= idx < len(out) and meta.get("needs_response_validation"):
                    out[idx]["needs_response_validation"] = True
                if idx is not None and 0 <= idx < len(out):
                    for mk, mv in meta.items():
                        if mk not in {"href", "url", "name"} and mv is not None and out[idx].get(mk) in (None, ""):
                            out[idx][mk] = mv
                return
            seen.add(key)
            seen_index[key] = len(out)
            item = meta
            source_kind = str(item.get("kind") or item.get("source") or "").strip()
            if "candidate_score" not in item or "candidate_reason" not in item:
                score, reason = _candidate_score_reason(source=source_kind)
                item.setdefault("candidate_score", score)
                item.setdefault("candidate_reason", reason)
            item.setdefault("url", uu)
            item.setdefault("display_name", original_name or nn)
            item.setdefault("attachment_name", original_name or nn)
            if original_name:
                item.setdefault("original_name", original_name)
            item.update({"href": uu, "name": nn})
            out.append(item)

        gm_items = extract_gm_nftc_filelist_attachments(html, base_url)
        for item in gm_items:
            try:
                meta = dict(item or {})
                meta.setdefault("kind", "gm")
                score, reason = _candidate_score_reason(source="gm")
                meta.setdefault("candidate_score", score)
                meta.setdefault("candidate_reason", reason)
                _push(item.get("href") or "", item.get("name") or "", meta)
            except Exception:
                continue
        if gm_items:
            return out

        for item in base_items or []:
            try:
                h = normalize_attachment_href(item.get("href") or "")
                n = (item.get("name") or "").strip()
                if _file_crawl_is_preview_only_href(h):
                    continue
                if h.lower() in {"javascript:;", "javascript:void(0)", "javascript:void(0);"}:
                    continue
                meta = dict(item or {})
                meta.setdefault("kind", "base")
                score, reason = _candidate_score_reason(source="base")
                meta.setdefault("candidate_score", score)
                meta.setdefault("candidate_reason", reason)
                _push(h, n, meta)
            except Exception:
                continue

        try:
            from bs4 import BeautifulSoup
        except Exception:
            return out
        if not html:
            return out
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return out

        for action in self._extract_attachment_actions_generic(html, base_url=base_url, soup=soup):
            try:
                _push(action.get("href") or "", action.get("name") or "", action)
            except Exception:
                continue

        file_exts = (
            ".hwpx", ".hwp", ".xlsx", ".xls", ".pptx", ".ppt", ".docx", ".doc", ".pdf", ".csv", ".zip",
            ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tif", ".tiff",
        )
        url_file_hints = (
            "downloadbbsfile",
            "atchmnflno",
            "filedown",
            "filedownload",
            "download",
            "atchfile",
            "atchfileid",
            "filesn",
            "fileid",
            "file_id",
            "file_sn",
            "filedown.do",
            "download.do",
            "nd_filedownload.do",
        )
        context_hints = (
            "attach",
            "attachment",
            "file",
            "filedown",
            "download",
            "첨부",
            "파일",
            "다운로드",
            "내려받기",
        )

        def _node_context_text(node: Any, depth: int = 3) -> str:
            parts: list[str] = []
            cur = node
            for _ in range(depth):
                if cur is None:
                    break
                try:
                    parts.append(cur.get_text(" ", strip=True) or "")
                except Exception:
                    pass
                try:
                    attrs = []
                    for attr in ("id", "class", "title", "aria-label", "name"):
                        val = cur.get(attr)
                        if isinstance(val, list):
                            attrs.extend(str(v) for v in val)
                        elif val:
                            attrs.append(str(val))
                    if attrs:
                        parts.append(" ".join(attrs))
                except Exception:
                    pass
                cur = getattr(cur, "parent", None)
            return re.sub(r"\s+", " ", " ".join(p for p in parts if p)).strip()

        def _has_any_file_ext(*values: str) -> bool:
            text = " ".join(str(v or "") for v in values).lower()
            return any(ext in text for ext in file_exts)

        def _has_attachment_context(text: str) -> bool:
            low = str(text or "").lower()
            return any(h in low for h in context_hints)

        def _looks_like_board_detail_link(value: str) -> bool:
            low = str(value or "").lower()
            if not low:
                return False
            return any(token in low for token in ("/view.do", "view.do?", "/detail.do", "detail.do?", "/read.do", "read.do?"))
        def _contextual_title_name(node: Any, *remove_values: str) -> str:
            remove_tokens = [str(v or "").strip() for v in remove_values if str(v or "").strip()]
            selectors = (
                "a[href*='view.do']",
                "a[href*='detail.do']",
                "a[href*='read.do']",
                "a.img",
                ".subject a",
                ".title a",
                "[class*='subject'] a",
                "[class*='title'] a",
                ".subject",
                ".title",
                "[class*='subject']",
                "[class*='title']",
            )
            try:
                containers = list(getattr(node, "parents", []) or [])[:8]
            except Exception:
                containers = []
            for container in containers:
                if str(getattr(container, "name", "") or "").lower() not in {"li", "div", "tr", "td", "dl", "p"}:
                    continue
                for selector in selectors:
                    try:
                        candidates = container.select(selector)
                    except Exception:
                        candidates = []
                    for candidate in candidates:
                        try:
                            text = re.sub(r"\s+", " ", candidate.get_text(" ", strip=True) or "").strip()
                        except Exception:
                            text = ""
                        for token in remove_tokens:
                            text = text.replace(token, " ")
                        text = _clean_attachment_name(text)
                        if text and not _is_generic_download_name(text) and 2 <= len(text) <= 180:
                            return text
            return ""

        def _script_context_name(source: str, pos: int = 0, fallback: str = "") -> str:
            source_text = str(source or "")
            raw_pos = int(pos or 0)
            object_start = source_text.rfind("{", 0, raw_pos + 1)
            object_end = source_text.find("}", raw_pos)
            if object_start >= 0 and object_end > object_start and object_end - object_start <= 1800:
                start = object_start
                end = object_end + 1
            else:
                start = max(0, raw_pos - 900)
                end = min(len(source_text), raw_pos + 900)
            ctx = source_text[start:end]
            name_key_re = re.compile(
                r"['\"]?(?:orignlFileNm|originFileNm|orgFileNm|orginlFileNm|fileNm|fileName|filename|realFileNm|userFileNm|downFileNm)['\"]?\s*[:=]\s*['\"]([^'\"]{1,220})['\"]",
                re.IGNORECASE,
            )
            local_pos = max(0, int(pos or 0) - start)
            named: list[tuple[int, str]] = []
            for match in name_key_re.finditer(ctx):
                name = _clean_attachment_name(match.group(1) or "")
                if name and not _is_generic_download_name(name):
                    named.append((abs(match.start() - local_pos), name))
            if named:
                named.sort(key=lambda item: item[0])
                return named[0][1]
            file_names: list[tuple[int, str]] = []
            for m in re.finditer(
                r"([^\s\"'<>]{1,220}\.(?:hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|zip|jpg|jpeg|png|gif|bmp|webp|tif|tiff))",
                ctx,
                re.IGNORECASE,
            ):
                name = _clean_attachment_name(m.group(1) or "")
                if name:
                    file_names.append((abs(m.start() - local_pos), name))
            if file_names:
                file_names.sort(key=lambda item: item[0])
                return file_names[0][1]
            return _clean_attachment_name(fallback)

        def _script_value_near(source: str, pos: int, key_names: tuple[str, ...]) -> str:
            source_text = str(source or "")
            raw_pos = int(pos or 0)
            object_start = source_text.rfind("{", 0, raw_pos + 1)
            object_end = source_text.find("}", raw_pos)
            if object_start >= 0 and object_end > object_start and object_end - object_start <= 1800:
                start = object_start
                end = object_end + 1
            else:
                start = max(0, raw_pos - 700)
                end = min(len(source_text), raw_pos + 700)
            ctx = source_text[start:end]
            local_pos = max(0, raw_pos - start)
            key_pat = "|".join(re.escape(k) for k in key_names)
            patterns = (
                rf"(?:{key_pat})\s*[:=]\s*['\"]([^'\"]{{1,160}})['\"]",
                rf"['\"](?:{key_pat})['\"]\s*:\s*['\"]([^'\"]{{1,160}})['\"]",
            )
            found: list[tuple[int, str]] = []
            for pat in patterns:
                for m in re.finditer(pat, ctx, flags=re.IGNORECASE):
                    value = (m.group(1) or "").strip()
                    if value:
                        found.append((abs(m.start() - local_pos), value))
            if found:
                found.sort(key=lambda item: item[0])
                return found[0][1]
            return ""

        def _collect_script_json_candidates() -> None:
            source = str(html or "")
            if not source:
                return
            pushed = 0
            max_candidates = 60

            def _push_limited(url: str, name: str, kind: str, reason: str, raw: str = "") -> None:
                nonlocal pushed
                if pushed >= max_candidates:
                    return
                score, score_reason = _candidate_score_reason(source=kind)
                _push(
                    url,
                    name or "attachment",
                    {
                        "kind": kind,
                        "method": "GET",
                        "params": {},
                        "raw": raw[:500],
                        "needs_response_validation": True,
                        "candidate_score": score,
                        "candidate_reason": reason or score_reason,
                    },
                )
                pushed += 1

            for match in re.finditer(r"['\"]?atchFileId['\"]?\s*[:=]\s*['\"]([^'\"]{6,160})['\"]", source, flags=re.IGNORECASE):
                atch_id = (match.group(1) or "").strip()
                file_sn = _script_value_near(source, match.start(), ("fileSn", "fileSN", "file_sn", "atchFileSn"))
                if not atch_id or not file_sn:
                    continue
                name = _script_context_name(source, match.start())
                query = urlencode({"atchFileId": atch_id, "fileSn": file_sn})
                _push_limited(urljoin(base_url, f"/cmm/fms/FileDown.do?{query}"), name, "egov_pair", "script_egov_atchFileId_fileSn", source[match.start():match.end()])

            for match in re.finditer(r"['\"]?q_fileSn['\"]?\s*[:=]\s*['\"]([^'\"]{1,80})['\"]", source, flags=re.IGNORECASE):
                file_sn = (match.group(1) or "").strip()
                file_id = _script_value_near(source, match.start(), ("q_fileId", "fileId", "file_id"))
                if not file_sn or not file_id:
                    continue
                name = _script_context_name(source, match.start())
                query = urlencode({"q_fileSn": file_sn, "q_fileId": file_id})
                _push_limited(urljoin(base_url, f"/component/file/ND_fileDownload.do?{query}"), name, "nd_file_pair", "script_nd_fileSn_fileId", source[match.start():match.end()])

            quoted_re = re.compile(r"['\"]([^'\"]{1,700})['\"]")
            for match in quoted_re.finditer(source):
                raw = (match.group(1) or "").strip()
                if not raw or raw.lower().startswith(("#", "mailto:", "tel:", "javascript:")):
                    continue
                low = raw.lower()
                looks_like_path = raw.startswith(("http://", "https://", "/", "./", "../"))
                has_hint = any(h in low for h in url_file_hints)
                has_ext = _has_any_file_ext(raw)
                if not looks_like_path:
                    continue
                try:
                    full = urljoin(base_url, normalize_attachment_href(raw))
                except Exception:
                    full = raw
                try:
                    path_low = urlparse(full).path.lower()
                except Exception:
                    path_low = str(full or "").lower()
                if any(token in path_low for token in ("/static/", "/resources/", "/assets/", "/images/layout", "/img/layout", "/common/img/", "/common/image/")):
                    continue
                is_file_storage_path = "/files/" in path_low or "/file/" in path_low
                if not (has_hint or (has_ext and is_file_storage_path)):
                    continue
                if _is_share_or_social_href(full) or _file_crawl_is_preview_only_href(full):
                    continue
                name = _script_context_name(source, match.start(), os.path.basename(urlparse(full).path))
                _push_limited(full, name, "script_url", "script_quoted_file_url", raw)

        _collect_script_json_candidates()
        if (
            "k-cohesion.go.kr" in str(base_url or "").lower()
            and "/pcnc/contents/" in str(base_url or "").lower()
        ):
            title = ""
            try:
                title_el = soup.select_one(".board_detail_wrap .detail_tit, .detail_tit")
                title = re.sub(r"\s+", " ", title_el.get_text(" ", strip=True)).strip() if title_el else ""
            except Exception:
                title = ""
            photo_idx = 0
            for inp in soup.select('input[id^="photoMask"][value]'):
                try:
                    mask = str(inp.get("value") or "").strip()
                except Exception:
                    mask = ""
                if not mask:
                    continue
                photo_idx += 1
                photo_name = f"{title} 사진 {photo_idx}".strip() if title else f"photo {photo_idx}"
                _push(urljoin(base_url, f"/comm/download.do?f={mask}"), photo_name)

        if "yongin.go.kr" in str(base_url or "").lower():
            for a in soup.find_all("a"):
                try:
                    raw_href = (a.get("href") or a.get("data-href") or a.get("data-url") or "").strip()
                    onclick = (a.get("onclick") or "").strip()
                    title_attr = (a.get("title") or "").strip()
                    link_text = (a.get_text(" ", strip=True) or "").strip()
                except Exception:
                    continue
                resolved = (
                    resolve_yongin_file_download_url(raw_href, base_url)
                    or resolve_yongin_file_download_url(onclick, base_url)
                )
                if not resolved:
                    continue
                name = _pick_anchor_file_name(a, title_attr, link_text, resolved)
                if not name:
                    try:
                        ctx = a.parent.get_text(" ", strip=True) if a.parent is not None else ""
                    except Exception:
                        ctx = ""
                    m = re.search(r"([^\s\"'<>]{1,180}\.(?:hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|zip))", ctx, re.IGNORECASE)
                    name = (m.group(1) or "").strip() if m else ""
                _push(resolved, name)

        if "ne.go.kr" in str(base_url or "").lower():
            ne_file_re = re.compile(
                r"([^\s\"'<>|:]{1,180}\.(?:hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|zip|jpg|jpeg|png|gif|bmp|webp|tif|tiff))",
                re.IGNORECASE,
            )

            def _ne_name_from_context(node: Any, fallback: str = "") -> str:
                for container_name in ("tr", "li", "dl", "div", "p"):
                    try:
                        container = node.find_parent(container_name)
                    except Exception:
                        container = None
                    if container is None:
                        continue
                    try:
                        ctx = re.sub(r"\s+", " ", container.get_text(" ", strip=True)).strip()
                    except Exception:
                        ctx = ""
                    match = ne_file_re.search(ctx)
                    if match:
                        return _clean_attachment_name(match.group(1))
                try:
                    ctx = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
                except Exception:
                    ctx = ""
                match = ne_file_re.search(ctx)
                if match:
                    return _clean_attachment_name(match.group(1))
                return _clean_attachment_name(fallback)

            for node in soup.find_all(["a", "button", "input"]):
                try:
                    raw_href = (
                        node.get("href")
                        or node.get("data-href")
                        or node.get("data-url")
                        or node.get("data-download-url")
                        or node.get("formaction")
                        or ""
                    ).strip()
                    onclick = (node.get("onclick") or "").strip()
                    title_attr = (node.get("title") or node.get("aria-label") or "").strip()
                    value_attr = (node.get("value") or node.get("alt") or "").strip()
                    link_text = (node.get_text(" ", strip=True) or value_attr or "").strip()
                except Exception:
                    continue
                candidates = [raw_href, onclick]
                resolved = ""
                for raw_candidate in candidates:
                    raw_candidate = str(raw_candidate or "").strip()
                    if not raw_candidate:
                        continue
                    if "ND_fileDownload.do" in raw_candidate or "q_fileSn" in raw_candidate or "q_fileId" in raw_candidate:
                        resolved = extract_download_url_from_js(raw_candidate, base_url) or raw_candidate
                        break
                if not resolved:
                    continue
                try:
                    resolved = urljoin(base_url, normalize_attachment_href(resolved))
                except Exception:
                    resolved = normalize_attachment_href(resolved)
                low_resolved = resolved.lower()
                if "nd_filedownload.do" not in low_resolved or "q_filesn=" not in low_resolved:
                    continue
                name = _pick_anchor_file_name(node, title_attr, link_text, resolved)
                if not name or name == "attachment" or _is_generic_download_name(name):
                    name = _ne_name_from_context(node, value_attr or title_attr or link_text)
                score, reason = _candidate_score_reason(source="ne_direct_file")
                extra = {
                    "kind": "ne_direct_file",
                    "method": "GET",
                    "params": {},
                    "needs_response_validation": True,
                    "candidate_score": score,
                    "candidate_reason": reason,
                }
                _push(resolved, name or "attachment", extra)

        for a in soup.find_all(["a", "input", "button"]):
            if _is_noise_attachment_container(a):
                continue
            try:
                href = normalize_attachment_href(
                    (
                        a.get("href")
                        or a.get("data-href")
                        or a.get("data-url")
                        or a.get("data-download-url")
                        or a.get("formaction")
                        or ""
                    ).strip()
                )
            except Exception:
                href = ""
            onclick = ""
            try:
                onclick = (a.get("onclick") or "").strip()
            except Exception:
                onclick = ""

            title_attr = (a.get("title") or a.get("aria-label") or "").strip()
            value_attr = (a.get("value") or a.get("alt") or "").strip()
            link_text = (a.get_text(" ", strip=True) or value_attr or "").strip()
            context_text = _node_context_text(a)
            name = _pick_anchor_file_name(a, title_attr, link_text, href)
            lname = name.lower()
            if onclick and (
                not href
                or href.startswith("#")
                or href.lower() in {"javascript:;", "javascript:void(0)", "javascript:void(0);"}
            ):
                resolved = extract_download_url_from_js(onclick, base_url) or ""
                if not resolved:
                    resolved = resolve_anseong_yhlib_download_url(onclick, base_url) or ""
                if resolved:
                    href = normalize_attachment_href(resolved)

            # fragment-only href(#, #right_area 등)는 실제 다운로드 URL이 아니므로 onclick 해석을 우선 시도한다.
            if not href or href.startswith("#"):
                if onclick:
                    cand = extract_download_url_from_js(onclick, base_url) or ""
                    if not cand:
                        cand = resolve_anseong_yhlib_download_url(onclick, base_url) or ""
                    href = cand.strip() if cand else ""
                if not href:
                    continue

            lhref = href.lower()
            is_download_handler = any(h in lhref for h in url_file_hints)

            # file 전용: '바로보기'라도 실제 다운로드 핸들러면 첨부로 인정
            if (("바로보기" in link_text) or ("바로듣기" in link_text) or ("바로보기" in title_attr) or ("바로듣기" in title_attr)) and not is_download_handler:
                continue

            is_js = lhref.startswith("javascript:")
            if is_js:
                resolved = extract_download_url_from_js(onclick, base_url) if onclick else ""
                if not resolved:
                    resolved = extract_download_url_from_js(href, base_url) or ""
                if not resolved:
                    resolved = resolve_anseong_yhlib_download_url(onclick or href, base_url) or ""
                if resolved:
                    href = normalize_attachment_href(resolved)
                    lhref = href.lower()
                    is_js = lhref.startswith("javascript:")
                    is_download_handler = any(h in lhref for h in url_file_hints)
                if is_js and not (is_download_handler or onclick):
                    continue

            try:
                full = href if is_js else urljoin(base_url, href)
            except Exception:
                full = href
            if not full:
                continue

            lfull = full.lower()
            if _is_noise_attachment_asset(full, name):
                continue
            if _is_share_or_social_href(full):
                continue
            combined_hint_text = " ".join(
                str(v or "")
                for v in (full, name, title_attr, link_text, value_attr, onclick, context_text)
            )
            combined_hint_low = combined_hint_text.lower()
            is_download_handler = any(h in combined_hint_low for h in url_file_hints)
            has_ext = _has_any_file_ext(full, name, title_attr, link_text, value_attr, onclick, context_text)
            has_attachment_context = _has_attachment_context(context_text)
            broad_context_candidate = _is_broad_attachment_candidate(full=full, context_text=context_text)
            if _file_crawl_is_preview_only_href(full) and not (has_ext or is_download_handler):
                continue
            if _looks_like_board_detail_link(full) and not (has_ext or is_download_handler):
                continue
            if (
                _file_crawl_is_preview_link_label(link_text=link_text, title=title_attr, name=name)
                and not (has_ext or is_download_handler)
            ):
                continue
            # file 전용 완화: 다운로드 핸들러면 확장자 표기 없어도 수집
            if not (has_ext or is_download_handler or broad_context_candidate):
                continue

            if (not name or name == "attachment" or _is_generic_download_name(name)) and link_text != value_attr:
                name = _pick_anchor_file_name(a, title_attr, value_attr, full) or name
            if not name or name == "attachment" or _is_generic_download_name(name):
                contextual_name = _contextual_title_name(a, title_attr, link_text, value_attr)
                if contextual_name:
                    name = contextual_name
            if (not name or name == "attachment" or _is_generic_download_name(name)) and a.parent is not None:
                node = a.parent
                for _ in range(4):
                    if node is None:
                        break
                    try:
                        ctx = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
                    except Exception:
                        ctx = ""
                    for token in (title_attr, link_text, value_attr):
                        if token:
                            ctx = ctx.replace(token, " ")
                    m = re.search(
                        r"([^\s\"'<>]{1,180}\.(?:hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|zip))",
                        ctx,
                        re.IGNORECASE,
                    )
                    if m:
                        name = _clean_attachment_name(m.group(1))
                        break
                    node = getattr(node, "parent", None)

            if not name or name == "attachment" or _is_generic_download_name(name):
                name = _file_name_from_url(full) or name

            if _is_chorogusan_results_reporting_url(base_url) and _is_file_generic_report_label(name):
                name = _page_title_file_name(full) or name

            extra: Dict[str, Any] = {}
            score, reason = _candidate_score_reason(
                has_ext=has_ext,
                is_download_handler=is_download_handler,
                has_attachment_context=broad_context_candidate,
            )
            extra["candidate_score"] = score
            extra["candidate_reason"] = reason
            if broad_context_candidate and not (has_ext or is_download_handler):
                extra["kind"] = "context"
                extra["method"] = "GET"
                extra["params"] = {}
                extra["raw"] = context_text[:500]
                extra["needs_response_validation"] = True
            _push(full, name, extra)

        for img in soup.find_all("img"):
            if _is_noise_attachment_container(img):
                continue
            try:
                src = normalize_attachment_href(
                    (
                        img.get("src")
                        or img.get("data-src")
                        or img.get("data-original")
                        or img.get("data-url")
                        or ""
                    ).strip()
                )
            except Exception:
                src = ""
            if not src:
                continue
            try:
                full = urljoin(base_url, src)
            except Exception:
                full = src
            try:
                path_low = urlparse(full).path.lower()
            except Exception:
                path_low = str(full or "").lower()
            if _is_noise_attachment_asset(full):
                continue
            if any(token in path_low for token in ("/static/", "/resources/", "/common/", "/assets/", "/images/layout", "/img/layout")):
                continue
            has_ext = _has_any_file_ext(full)
            is_files_path = "/files/" in path_low or "/file/" in path_low
            if not (has_ext and is_files_path):
                continue
            try:
                parent_link = img.find_parent("a")
            except Exception:
                parent_link = None
            if parent_link is not None:
                try:
                    parent_href = parent_link.get("href") or parent_link.get("data-href") or ""
                    if parent_href and urljoin(base_url, normalize_attachment_href(parent_href)) == full:
                        continue
                except Exception:
                    pass
            alt_name = _image_alt_name(img, full)
            if not alt_name:
                try:
                    alt_name = _append_url_extension_if_missing(img.get("alt") or img.get("title") or "", full)
                except Exception:
                    alt_name = ""
            if not alt_name:
                try:
                    alt_name = os.path.basename(urlparse(full).path) or "attachment"
                except Exception:
                    alt_name = "attachment"
            score, _reason = _candidate_score_reason(has_ext=True, source="img_src")
            _push(
                full,
                alt_name,
                {
                    "kind": "img_src",
                    "candidate_score": max(score, 0.74),
                    "candidate_reason": "image_src_files_path",
                },
            )

        return out

    async def _extract_kcohesion_filelist_attachments(self, html: str, *, base_url: str) -> list[Dict[str, str]]:
        """
        k-cohesion detail pages render ordinary attachments through an AJAX
        `/afile/fileList.do` call, so static HTML only contains the fileId.
        """
        if not html or "k-cohesion.go.kr" not in str(base_url or "").lower():
            return []
        if "/pcnc/contents/" not in str(base_url or "").lower():
            return []

        file_ids: list[str] = []
        seen_ids: set[str] = set()
        patterns = (
            r"""data\s*:\s*["']fileId=([^"']+)["']""",
            r"""fileId\s*[:=]\s*["']([A-Za-z0-9_-]{6,})["']""",
        )
        for pattern in patterns:
            for match in re.finditer(pattern, html, flags=re.IGNORECASE):
                file_id = (match.group(1) or "").strip()
                if not file_id or file_id.lower() == "fileid" or file_id in seen_ids:
                    continue
                seen_ids.add(file_id)
                file_ids.append(file_id)

        if not file_ids:
            return []

        session = await self._get_http_session(timeout_sec=15.0)
        if session is None:
            return []

        out: list[Dict[str, str]] = []
        seen_urls: set[str] = set()
        for file_id in file_ids[:10]:
            api_url = urljoin(base_url, f"/afile/fileList.do?fileId={file_id}")
            try:
                import aiohttp

                async with self._guarded_session_get(
                    session,
                    api_url,
                    channel="file",
                    timeout=aiohttp.ClientTimeout(total=15.0),
                    headers={
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "Referer": base_url,
                        "X-Requested-With": "XMLHttpRequest",
                    },
                ) as resp:
                    if resp.status != 200:
                        continue
                    rows = json.loads((await resp.read()).decode("utf-8-sig"))
            except Exception as exc:
                logger.debug(
                    "[FileProbeDebug][kcohesion.filelist] request failed | url=%s api=%s err=%s",
                    (base_url or "")[:160],
                    api_url,
                    exc,
                )
                continue

            if isinstance(rows, dict):
                rows = rows.get("data") or rows.get("list") or rows.get("result") or []
            if not isinstance(rows, list):
                continue

            for row in rows:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("fileName") or row.get("name") or row.get("orgFileNm") or "").strip()
                is_pdf = str(row.get("ext") or "").strip().lower() == "pdf"
                href = str(
                    (row.get("openPath") if is_pdf else row.get("downloadPath"))
                    or row.get("downloadPath")
                    or row.get("openPath")
                    or ""
                ).strip()
                if not href:
                    continue
                full = urljoin(base_url, href)
                key = canonicalize_url_for_dedup(full) or full.lower()
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                original_name = name or "attachment"
                out.append(
                    {
                        "href": full,
                        "name": original_name,
                        "attachment_name": original_name,
                        "original_name": original_name,
                    }
                )

        return out

    async def _request_summarize_keywords(self, url: str) -> Optional[Dict[str, Any]]:
        """
        게시판 부모의 URL 기반 summarize_keywords 는 파일 크롤에 쓰지 않는다.
        파일은 학습 파이프라인 이후 LEARN_LIST status=Y 일 때 `_file_crawl_post_summarize_keywords` 만 호출한다.
        """
        return None

    def _reset_run_state(self) -> None:
        try:
            s = getattr(self, "_seen_file_urls", None)
            if isinstance(s, set):
                s.clear()
        except Exception:
            pass
        try:
            ts = getattr(self, "_file_parallel_learn_tasks", None)
            if isinstance(ts, set):
                for _t in list(ts):
                    if isinstance(_t, asyncio.Task) and not _t.done():
                        _t.cancel()
                ts.clear()
        except Exception:
            pass
        try:
            ssum = getattr(self, "_file_summarize_dispatched_keys", None)
            if isinstance(ssum, set):
                ssum.clear()
        except Exception:
            pass
        super()._reset_run_state()

    def _resolve_contents_url_for_learn_list_filters(
        self, start_urls: Any, kwargs: Dict[str, Any]
    ) -> Optional[str]:
        """LEARN_LIST 행 선택(domain/method)용 contents_url — board start_workflow 와 동일 후보."""
        _cu = _valid_contents_url(kwargs.get("contents_url"))
        if not _cu:
            _tu = getattr(self, "target_url", None)
            _cu = _valid_contents_url(_tu)
        if not _cu and isinstance(start_urls, (list, tuple)) and start_urls:
            _first = start_urls[0]
            if isinstance(_first, dict):
                _cu = _valid_contents_url(_first.get("url"))
            else:
                _cu = _valid_contents_url(_first)
        if not _cu and isinstance(start_urls, str):
            _cu = _valid_contents_url(start_urls)
        return _cu.strip() if isinstance(_cu, str) else None

    async def _prepare_learned_file_source_url_dedup(self, start_urls: Any) -> None:
        """Load learned file source URLs once for the start-URL prequeue check."""
        self._learned_file_source_url_keys: Set[str] = set()
        if not isinstance(start_urls, (list, tuple)) or not start_urls:
            return
        if not (getattr(self, "chat_bot_id", None) and getattr(self, "db_name", None)):
            return

        try:
            from db.mariadb_save_update import (
                ensure_learn_list_standard_columns,
                get_account_identifier_from_chatbot_setup,
                get_learn_list_table_name,
            )
            from db.mysql_db_config import mysql_execute_query

            account_identifier = await get_account_identifier_from_chatbot_setup(
                self.chat_bot_id,
                self.db_name,
            )
            learn_table = get_learn_list_table_name(account_identifier)
            columns = await ensure_learn_list_standard_columns(self.db_name, learn_table)
            if not columns or "source_url" not in columns or "status" not in columns:
                logger.info(
                    "[FileStartDedup] skipped | job_id=%s db=%s reason=missing_source_or_status_column",
                    getattr(self, "job_id", ""),
                    self.db_name,
                )
                return

            if "content_type" in columns:
                type_sql = "`content_type` = %s"
                params: tuple[Any, ...] = ("Y", "file")
            elif "type" in columns:
                type_sql = "`type` = %s"
                params = ("Y", "file")
            else:
                logger.info(
                    "[FileStartDedup] skipped | job_id=%s db=%s reason=missing_file_type_column",
                    getattr(self, "job_id", ""),
                    self.db_name,
                )
                return

            rows = await mysql_execute_query(
                f"SELECT DISTINCT `source_url` FROM `{learn_table}` "
                f"WHERE `status` = %s AND {type_sql} "
                "AND `source_url` IS NOT NULL AND `source_url` <> ''",
                params,
                fetch=True,
                dbname=self.db_name,
                op_name=f"file_start_learned_source_url_lookup:job={getattr(self, 'job_id', '') or '-'}",
            )
            learned_source_keys = {
                canonicalize_url_for_dedup(str(row.get("source_url") or "")) or ""
                for row in (rows or [])
                if isinstance(row, dict) and str(row.get("source_url") or "").strip()
            }
            learned_source_keys.discard("")
            self._learned_file_source_url_keys = learned_source_keys

            logger.info(
                "[FileStartDedup] ready | job_id=%s db=%s start_urls=%s learned_sources=%s",
                getattr(self, "job_id", ""),
                self.db_name,
                len(start_urls),
                len(learned_source_keys),
            )
        except Exception as exc:
            logger.warning(
                "[FileStartDedup] failed open | job_id=%s db=%s err=%s",
                getattr(self, "job_id", ""),
                getattr(self, "db_name", ""),
                exc,
            )

    def _is_learned_file_source_url(self, item: Any) -> bool:
        learned_source_keys = getattr(self, "_learned_file_source_url_keys", set()) or set()
        if not learned_source_keys:
            return False
        try:
            post_url = str(getattr(item, "url", item) or "")
            post_key = canonicalize_url_for_dedup(post_url) or post_url.strip()
            return bool(post_key and post_key in learned_source_keys)
        except Exception:
            return False

    async def start_workflow(
        self,
        start_urls: Any = None,
        progress_callback=None,
        start_date=None,
        end_date=None,
        target_domains: Any = None,
        filtered_memory_storage: Any = None,
        **kwargs: Any,
    ) -> None:

        from backend.file.fast_attachment_producer import run_fast_file_attachment_front

        self.state = WorkflowState.RUNNING
        self.is_running = True
        self.progress_callback = progress_callback
        self.start_date, self.end_date = start_date, end_date
        self.stop_event.clear()
        self.colle = "file"
        self.colle_mode = "file"
        self.file_mode = True
        self._pre_filtered_memory = filtered_memory_storage or []
        self._reset_run_state()
        await self._prepare_learned_file_source_url_dedup(start_urls)
        if start_urls:
            try:
                self.init_file_scan_count_base_from_start_urls(start_urls)
            except Exception as scan_base_exc:
                logger.warning(
                    "[파일크롤링][분리앞단] scan_count 초기화 실패 | job_id=%s err=%s",
                    getattr(self, "job_id", ""),
                    scan_base_exc,
                )
        else:
            try:
                self.set_file_scan_count_base_from_count(
                    int(getattr(self, "pre_explored_start_urls_count", 0) or 0)
                )
            except Exception:
                pass

        if self.progress_callback:
            try:
                self.progress_callback(self.get_stats())
            except Exception:
                pass

        normal_completed = False
        try:
            concurrency = _file_crawl_fast_front_concurrency(kwargs)
            timeout_sec = _file_crawl_detail_fetch_timeout_sec(kwargs)
            logger.info(
                "[파일크롤링][동시성] fast_front_concurrency=%s timeout_sec=%s learn_concurrency=%s post_url_count=%s",
                concurrency,
                timeout_sec,
                getattr(self, "_file_learn_concurrency", None),
                len(start_urls or []),
            )
            self.fast_file_front_result = await run_fast_file_attachment_front(
                workflow=self,
                post_items=start_urls or [],
                concurrency=concurrency,
                timeout_sec=timeout_sec,
                enqueue=True,
                prequeue_skip_check=self._is_learned_file_source_url,
            )
            try:
                fast_result = self.fast_file_front_result if isinstance(self.fast_file_front_result, dict) else {}
                fast_counters = fast_result.get("counters") if isinstance(fast_result.get("counters"), dict) else fast_result
                fast_results = fast_result.get("results") if isinstance(fast_result.get("results"), list) else []
                fast_post_count = int(fast_counters.get("post_count", 0) or 0)
                fast_unique_post_count = int(fast_counters.get("post_unique_count", fast_post_count) or 0)
                fast_prequeue_duplicate_skipped = int(
                    fast_counters.get("post_prequeue_duplicate_skipped_count", 0) or 0
                )
                fast_success_count = int(fast_counters.get("post_success_count", 0) or 0)
                fast_attachment_count = int(fast_counters.get("attachment_count", 0) or 0)
                fast_enqueued_count = int(fast_counters.get("enqueued_count", 0) or 0)
                fast_found_posts = sum(
                    1 for row in fast_results
                    if isinstance(row, dict) and int(row.get("attachment_count", 0) or 0) > 0
                )
                async with self._stats_lock:
                    self.stats["file_fast_front_post_count"] = fast_post_count
                    self.stats["file_fast_front_success_count"] = fast_success_count
                    self.stats["file_fast_front_attachment_count"] = fast_attachment_count
                    self.stats["file_fast_front_enqueued_count"] = fast_enqueued_count
                    self.stats["file_fast_front_found_post_count"] = fast_found_posts
                    self.stats["file_detail_fetch_success_count"] = max(
                        int(self.stats.get("file_detail_fetch_success_count", 0) or 0),
                        fast_success_count,
                    )
                    self.stats["file_attachment_extract_attempt_count"] = max(
                        int(self.stats.get("file_attachment_extract_attempt_count", 0) or 0),
                        fast_success_count,
                    )
                    self.stats["file_attachment_found_post_count"] = max(
                        int(self.stats.get("file_attachment_found_post_count", 0) or 0),
                        fast_found_posts,
                    )
                    self.stats["file_attachment_found_total_count"] = max(
                        int(self.stats.get("file_attachment_found_total_count", 0) or 0),
                        fast_attachment_count,
                    )
                logger.info(
                    "[FileStartDedup] completed | job_id=%s input=%s unique=%s skipped=%s collection_targets=%s",
                    getattr(self, "job_id", ""),
                    fast_post_count,
                    fast_unique_post_count,
                    fast_prequeue_duplicate_skipped,
                    max(0, fast_unique_post_count - fast_prequeue_duplicate_skipped),
                )
            except Exception as fast_stats_exc:
                logger.debug(
                    "[file-fast][stats_merge_failed] job_id=%s err=%s",
                    getattr(self, "job_id", ""),
                    fast_stats_exc,
                )
            await self._complete_file_exploration_phase()
            pipeline_completed = await self._finalize_stats()
            normal_completed = bool(pipeline_completed)
        except asyncio.CancelledError:
            self.final_status = "cancelled"
            raise
        except Exception as exc:
            self.final_status = "error"
            logger.error(
                "[파일크롤링][분리앞단] 실패 | job_id=%s err=%s",
                getattr(self, "job_id", ""),
                exc,
                exc_info=True,
            )
            try:
                await self._finalize_stats()
            except Exception:
                pass
            raise
        finally:
            current_final_status = str(getattr(self, "final_status", "") or "").strip().lower()
            if normal_completed and current_final_status in {"", "running"}:
                self.final_status = "completed"
            self.is_running = False
            try:
                if self.state == WorkflowState.RUNNING:
                    self.state = WorkflowState.COMPLETED if normal_completed else WorkflowState.ERROR
            except Exception:
                pass
            if self.progress_callback:
                try:
                    self.progress_callback(self.get_stats())
                except Exception:
                    pass
            logger.debug(
                "[파일크롤링][분리앞단] 종료 | job_id=%s completed=%s stats=%s",
                getattr(self, "job_id", ""),
                normal_completed,
                self.get_stats(),
            )

    async def stop(self) -> None:
        await super().stop()
        try:
            await self._shutdown_file_pipeline(graceful=False)
        except Exception:
            pass
        try:
            await self._cleanup_stop_resources()
        except Exception:
            pass

    async def _complete_file_exploration_phase(self) -> None:
        """
        Producer(탐색) 종료: 상세 워커가 모두 끝난 직후 호출.
        남은 선별 배치를 다운로드 큐로 넘기고 scan_count/total_count 를 잠근다.
        """
        logger.debug(
            "[Phase][file] 1. 탐색(Producer) 종료 — 큐 잔량 flush 후 scan_count 고정 | job_id=%s",
            getattr(self, "job_id", ""),
        )
        qs = getattr(self, "_file_job_queues", None)
        if qs is not None:
            try:
                await qs.scan_batch_queue.flush()
            except Exception as ex:
                logger.debug("[Phase][file] scan flush | %s", ex)
            try:
                await qs.collection_batch_queue.flush()
                await qs.large_collection_batch_queue.flush()
            except Exception as ex:
                logger.debug("[Phase][file] collection flush | %s", ex)
        async with self._stats_lock:
            total = self.lock_file_exploration_scan_total()
        if self.progress_callback:
            try:
                self.progress_callback(self.get_stats())
            except Exception:
                pass
        logger.debug(
            "[Phase][file] scan_count=%s 고정 → 2. 소비(다운로드·저장·학습) 진행 | job_id=%s",
            total,
            getattr(self, "job_id", ""),
        )

    async def _finalize_stats(self) -> None:
        if getattr(self, "_hard_stop", False):
            try:
                logger.debug(
                    "[Phase][file] 2a. scan_batch_queue.join (선별 워커 소진) | job_id=%s",
                    self.job_id,
                )
                logger.debug(
                    "[Phase][file] hard-stop finalize shortcut | job_id=%s hard_stop=%s",
                    getattr(self, "job_id", ""),
                    getattr(self, "_hard_stop", False),
                )
                await self._shutdown_file_pipeline(graceful=False)
            except Exception:
                pass
            try:
                await self._cleanup_stop_resources()
            except Exception:
                pass
            await super()._finalize_stats()
            self._log_file_job_summary_once()
            return

        if getattr(self, "_file_job_queues", None):
            try:
                logger.debug(
                    "[Phase][file] 2. 소비(Consumer) 종료 대기 시작 | job_id=%s",
                    self.job_id,
                )
                queues = self._file_job_queues
                try:
                    await queues.scan_batch_queue.flush()
                    await queues.collection_batch_queue.flush()
                    await queues.large_collection_batch_queue.flush()
                    await queues.save_batch_queue.flush()
                    await queues.study_batch_queue.flush()
                except Exception:
                    pass
                try:
                    # Playwright 폴백(예: 180s)·재시도·배치를 고려해 기본값을 넉넉히 둔다.
                    timeout_sec = float(os.getenv("BOARD_CONTENT_FILE_FINALIZE_TIMEOUT_SEC", "1800") or "1800")
                except Exception:
                    timeout_sec = 720.0
                timeout_sec = max(30.0, min(timeout_sec, 1800.0))
                raw_final = str(getattr(self, "final_status", "") or "").strip().lower()
                if self.stop_event.is_set():
                    try:
                        stop_timeout_sec = float(
                            os.getenv("BOARD_CONTENT_FILE_FINALIZE_TIMEOUT_ON_STOP_SEC", "180") or "180"
                        )
                    except Exception:
                        stop_timeout_sec = 180.0
                    timeout_sec = max(10.0, min(timeout_sec, stop_timeout_sec))
                if raw_final in {"error", "failed", "fail", "exception"}:
                    try:
                        error_timeout_sec = float(
                            os.getenv("BOARD_CONTENT_FILE_FINALIZE_TIMEOUT_ON_ERROR_SEC", "90") or "90"
                        )
                    except Exception:
                        error_timeout_sec = 90.0
                    timeout_sec = max(10.0, min(timeout_sec, error_timeout_sec))
                try:
                    wait_log_sec = float(os.getenv("BOARD_CONTENT_FILE_FINALIZE_WAIT_LOG_SEC", "60") or "60")
                except Exception:
                    wait_log_sec = 60.0
                wait_log_sec = max(10.0, min(wait_log_sec, 300.0))
                terminal_wait = bool(self.stop_event.is_set() or raw_final in {"error", "failed", "fail", "exception"})

                async def _join_until_drained(queue, stage: str) -> None:
                    while True:
                        try:
                            await asyncio.wait_for(queue.join(), timeout=timeout_sec if terminal_wait else wait_log_sec)
                            return
                        except asyncio.TimeoutError:
                            try:
                                snapshot = queues.snapshot() if hasattr(queues, "snapshot") else {}
                            except Exception:
                                snapshot = {}
                            if terminal_wait:
                                raise
                            logger.warning(
                                "[Workflow][file] 큐 소진 대기 계속 | job_id=%s stage=%s queues=%s",
                                self.job_id,
                                stage,
                                snapshot,
                            )

                logger.debug(
                    "[Phase][file] 2a. collection_batch_queue.join (다운로드 워커 소진) | job_id=%s",
                    self.job_id,
                )
                logger.debug(
                    "[Phase][file] 2a. scan_batch_queue.join (selection workers drained) | job_id=%s",
                    self.job_id,
                )
                await _join_until_drained(queues.scan_batch_queue, "scan_batch")
                await _join_until_drained(queues.collection_batch_queue, "download_normal")
                await _join_until_drained(queues.large_collection_batch_queue, "download_large")
                logger.debug(
                    "[Phase][file] 2b. progress_queue.join (download event dispatch) | job_id=%s",
                    self.job_id,
                )
                await _join_until_drained(queues.progress_queue, "progress_dispatch")
                logger.debug(
                    "[Phase][file] 2b. local_finalize_queue.join | job_id=%s",
                    self.job_id,
                )
                await self._wait_for_file_local_finalize_drain()
                await _join_until_drained(queues.progress_queue, "progress_after_finalize")
                logger.debug(
                    "[Phase][file] 2b. file_save_queue.join | job_id=%s",
                    self.job_id,
                )
                await self._wait_for_file_save_drain()
                logger.debug(
                    "[Phase][file] 2c. save_batch_queue.join | job_id=%s",
                    self.job_id,
                )
                await _join_until_drained(queues.save_batch_queue, "save")
                logger.debug(
                    "[Phase][file] 2d. study_batch_queue.join | job_id=%s",
                    self.job_id,
                )
                await _join_until_drained(queues.study_batch_queue, "study")
                await asyncio.sleep(0.5)
                try:
                    await self._shutdown_file_pipeline(graceful=True)
                except Exception:
                    pass
                logger.debug(
                    "[Phase][file] 3. 소비 파이프라인 대기 완료 | job_id=%s",
                    self.job_id,
                )
            except asyncio.TimeoutError:
                try:
                    queue_snapshot = queues.snapshot() if hasattr(queues, "snapshot") else {}
                except Exception:
                    queue_snapshot = {}
                async with self._stats_lock:
                    self.stats["file_pipeline_finalize_timeout"] = True
                    self.stats["file_pipeline_finalize_queue_snapshot"] = queue_snapshot
                self.final_status = "incomplete"
                logger.warning(
                    "[Workflow][file] 파일 대기 타임아웃 | job_id=%s timeout_sec=%s queues=%s",
                    self.job_id,
                    timeout_sec,
                    queue_snapshot,
                )
                await super()._finalize_stats()
                self._log_file_job_summary_once()
                return False
            except Exception as e:
                logger.error("[Workflow][file] 파일 대기 중 오류: %s", e)

        await super()._finalize_stats()
        self._log_file_job_summary_once()
        return True

    async def _process_one_detail(self, it, depth: int = 0) -> None:
        if self.stop_event.is_set():
            return
        raw_url = it.url
        if not raw_url:
            return

        url = (it.url or "").split("#")[0].strip() or raw_url
        url_key = canonicalize_url_for_dedup(url) or url
        it.url = url
        _log_file_url_status(
            stage="detail_visit",
            status="start",
            process_url=url,
            post_url=url,
            selected="pending",
            saved="pending",
            learn="pending",
            job_id=getattr(self, "job_id", ""),
            db_name=getattr(self, "db_name", ""),
        )
        if getattr(self, "is_attachment_file_crawl_workflow", False) and _is_file_crawl_list_page_url(url):
            _log_file_url_status(
                stage="detail_visit",
                status="skipped",
                process_url=url,
                post_url=url,
                selected="no",
                saved="no",
                learn="not_started",
                reason="list_page_no_attachment_extract",
                job_id=getattr(self, "job_id", ""),
                db_name=getattr(self, "db_name", ""),
            )
            logger.debug(
                "[file_crawl][detail] skip_list_page_attachment_extract | job_id=%s url=%s",
                getattr(self, "job_id", None),
                (url or "")[:220],
            )
            try:
                if hasattr(self, "_record_job_result_stage"):
                    self._record_job_result_stage(
                        url=url_key,
                        stage="detail_html_fetch",
                        status="skipped",
                        reason="list_page_no_attachment_extract",
                        source_url=url,
                    )
            except Exception:
                pass
            return
        if getattr(self, "is_attachment_file_crawl_workflow", False):
            try:
                detail_started_keys = getattr(self, "_file_detail_started_keys", None)
                if not isinstance(detail_started_keys, set):
                    detail_started_keys = set()
                    setattr(self, "_file_detail_started_keys", detail_started_keys)
                if url_key:
                    detail_started_keys.add(url_key)
                self.stats["file_detail_started_count"] = len(detail_started_keys)
            except Exception:
                pass

        # File crawling no longer uses CATEGORY url_pattern/cate_match legacy resolution.
        # Direct board category values are carried forward and mapped to the File root during LEARN_LIST persistence.
        direct_cate1, direct_cate2 = normalize_file_detail_cates(
            getattr(it, "cate1", ""),
            getattr(it, "cate2", ""),
        )
        store_cate1, store_cate2 = _normalize_cate_codes_upper(direct_cate1, direct_cate2)
        if store_cate1 or store_cate2:
            logger.debug(
                "[Cate][file] direct board category applied before file-root mapping | cate1=%s cate2=%s | url=%s",
                store_cate1,
                store_cate2,
                (url or "")[:100],
            )

        async with self._stats_lock:
            self._seen_scan.add(url_key)
            if getattr(self, "is_attachment_file_crawl_workflow", False):
                self._sync_file_mode_scan_count()
            elif not getattr(self, "_lock_scan_count_from_pre_explored", False):
                self.stats["scan_count"] = len(self._seen_scan)
            if self.progress_callback:
                self.progress_callback(self.get_stats())

        direct_attachments = getattr(it, "direct_attachments", None)
        if direct_attachments:
            try:
                direct_reg_date_str = str(getattr(it, "reg_date_str", "") or "").strip()
                if (self.start_date or self.end_date) and direct_reg_date_str:
                    try:
                        from backend.shared.date_utils import is_date_in_range, parse_date

                        direct_reg_dt = parse_date(direct_reg_date_str)
                    except Exception:
                        direct_reg_dt = None
                    if direct_reg_dt is not None and not is_date_in_range(direct_reg_dt, self.start_date, self.end_date):
                        logger.debug(
                            "[기간필터][file][direct_attachments] 기간 밖 제외 | url=%s reg_date=%s",
                            (url or "")[:120],
                            direct_reg_date_str,
                        )
                        return
                direct_author_info = getattr(it, "author_info", None)
                if not isinstance(direct_author_info, dict):
                    direct_author_info = {}
                direct_author = (
                    direct_author_info.get("content_author")
                    or direct_author_info.get("author")
                    or direct_author_info.get("department")
                )
                direct_department = direct_author_info.get("department")
                if _content_author_debug_enabled():
                    logger.warning(
                        "[ContentAuthorDebug][file_dashboard.direct_enqueue_author] job_id=%s post=%s count=%s result=%r author=%r content_author=%r department=%r keys=%s",
                        getattr(self, "job_id", None),
                        (url or "")[:220],
                        len(direct_attachments or []),
                        _content_author_debug_value(direct_author),
                        _content_author_debug_value(direct_author_info.get("author")),
                        _content_author_debug_value(direct_author_info.get("content_author")),
                        _content_author_debug_value(direct_author_info.get("department")),
                        sorted(str(k) for k in direct_author_info.keys()),
                    )
                _log_file_url_status(
                    stage="attachment_extract",
                    status="found_direct",
                    process_url=url,
                    post_url=url,
                    selected="pending",
                    saved="pending",
                    learn="pending",
                    count=len(direct_attachments or []),
                    job_id=getattr(self, "job_id", ""),
                    db_name=getattr(self, "db_name", ""),
                )
                logger.debug(
                    "[FileDashboard][direct_attachments] enqueue without detail refetch | job_id=%s post=%s count=%s",
                    getattr(self, "job_id", None),
                    (url or "")[:220],
                    len(direct_attachments or []),
                )
                try:
                    _trace_key = canonicalize_url_for_dedup(url) or str(url or "").strip()
                    async with self._stats_lock:
                        direct_keys = getattr(self, "_file_attachment_direct_post_keys", None)
                        if not isinstance(direct_keys, set):
                            direct_keys = set()
                            setattr(self, "_file_attachment_direct_post_keys", direct_keys)
                        found_keys = getattr(self, "_file_attachment_found_post_keys", None)
                        if not isinstance(found_keys, set):
                            found_keys = set()
                            setattr(self, "_file_attachment_found_post_keys", found_keys)
                        if _trace_key:
                            direct_keys.add(_trace_key)
                            found_keys.add(_trace_key)
                        self.stats["file_attachment_direct_post_count"] = len(direct_keys)
                        self.stats["file_attachment_found_post_count"] = len(found_keys)
                        self.stats["file_attachment_found_total_count"] = int(
                            self.stats.get("file_attachment_found_total_count", 0) or 0
                        ) + len(direct_attachments or [])
                except Exception:
                    pass
                await self._enqueue_file_downloads(
                    post_url=url,
                    board_url=getattr(it, "board_url", "") or "",
                    reg_date=direct_reg_date_str or None,
                    attachments=list(direct_attachments or []),
                    author=direct_author,
                    department=direct_department,
                    author_kind=direct_author_info.get("author_kind"),
                    author_raw=direct_author_info.get("author_raw"),
                    department_raw=direct_author_info.get("department_raw"),
                    contact_phone=None,
                    view_count=None,
                    sync_after_download=True,
                    detail_cates=(store_cate1, store_cate2),
                    post_title=(getattr(it, "post_title", None) or getattr(it, "title", None) or ""),
                )
            finally:
                async with self._stats_lock:
                    if url_key not in self._seen_filtered_detail:
                        self._seen_filtered_detail.add(url_key)
                    self._sync_file_mode_scan_count()
                if self.progress_callback:
                    self.progress_callback(self.get_stats())
            return

        if self.enable_db_save and self.chat_bot_id and self.db_name:
            logger.debug("[중복선별][file] 게시글 단위 중복 체크 생략 | url=%s", (url or "")[:100])

        html, fetch_meta = await self._fetch_detail_html_for_selection(
            url,
            board_url=getattr(it, "board_url", "") or "",
            disable_playwright=bool(getattr(it, "disable_playwright", False)),
            slow_background_mode=bool(getattr(it, "slow_background_mode", False)),
            purpose="file_attachment_detail",
        )
        file_fetch_timeout = (fetch_meta or {}).get("fetch_timeout_sec")
        if not html:
            try:
                if getattr(self, "is_attachment_file_crawl_workflow", False):
                    static_fetch_outcome = {}
                    try:
                        if hasattr(self, "_static_fetch_outcome_for_url"):
                            static_fetch_outcome = self._static_fetch_outcome_for_url(url) or {}
                    except Exception:
                        static_fetch_outcome = {}
                    if not static_fetch_outcome and isinstance(fetch_meta, dict):
                        static_fetch_outcome = dict(fetch_meta.get("static_fetch_outcome") or {})
                    invalid_fetch = {}
                    try:
                        if hasattr(self, "_get_invalid_detail_fetch_reason"):
                            invalid_fetch = self._get_invalid_detail_fetch_reason(url) or {}
                    except Exception:
                        invalid_fetch = {}
                    no_html_reason = (
                        str((invalid_fetch or {}).get("reason") or "").strip()
                        or str((static_fetch_outcome or {}).get("outcome") or "").strip()
                        or "unknown"
                    )
                    no_html_keys = getattr(self, "_file_detail_no_html_keys", None)
                    if not isinstance(no_html_keys, set):
                        no_html_keys = set()
                        setattr(self, "_file_detail_no_html_keys", no_html_keys)
                    if url_key:
                        no_html_keys.add(url_key)
                    self.stats["file_detail_no_html_count"] = len(no_html_keys)
                    reason_counter = getattr(self, "_file_detail_no_html_reason_counts", None)
                    if not isinstance(reason_counter, dict):
                        reason_counter = {}
                        setattr(self, "_file_detail_no_html_reason_counts", reason_counter)
                    reason_counter[no_html_reason] = int(reason_counter.get(no_html_reason, 0) or 0) + 1
                    self.stats["file_detail_no_html_reason_counts"] = dict(reason_counter)
                    safe_reason = re.sub(r"[^A-Za-z0-9_]+", "_", no_html_reason).strip("_").lower()[:80] or "unknown"
                    stat_key = f"file_detail_no_html_by_{safe_reason}_count"
                    self.stats[stat_key] = int(self.stats.get(stat_key, 0) or 0) + 1
                    samples = getattr(self, "_file_detail_no_html_samples", None)
                    if not isinstance(samples, list):
                        samples = []
                        setattr(self, "_file_detail_no_html_samples", samples)
                    if len(samples) < 30:
                        samples.append({
                            "url": (url or "")[:240],
                            "reason": no_html_reason,
                            "status": (static_fetch_outcome or {}).get("status"),
                            "final_url": str((static_fetch_outcome or {}).get("final_url") or "")[:240],
                            "error": str((static_fetch_outcome or {}).get("error") or "")[:240],
                        })
                    self.stats["file_detail_no_html_samples"] = list(samples[-30:])
                    if hasattr(self, "_write_detail_failure_log"):
                        self._write_detail_failure_log(
                            "file_detail_fetch_empty",
                            url=url,
                            board_url=str(getattr(it, "board_url", "") or ""),
                            fetch_timeout_sec=file_fetch_timeout,
                            shared_fetch_meta=fetch_meta,
                            no_html_reason=no_html_reason,
                            static_fetch_outcome=static_fetch_outcome,
                            invalid_fetch=invalid_fetch,
                        )
                    _log_file_url_status(
                        stage="detail_fetch",
                        status="error",
                        process_url=url,
                        post_url=url,
                        selected="no",
                        saved="no",
                        learn="not_started",
                        reason=no_html_reason,
                        error=str((static_fetch_outcome or {}).get("error") or ""),
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                    logger.warning(
                        "[파일크롤링][상세fetch실패] html 없음 | job_id=%s reason=%s status=%s url=%s final=%s error=%s",
                        getattr(self, "job_id", ""),
                        no_html_reason,
                        (static_fetch_outcome or {}).get("status"),
                        (url or "")[:180],
                        str((static_fetch_outcome or {}).get("final_url") or "")[:180],
                        str((static_fetch_outcome or {}).get("error") or "")[:180],
                    )
            except Exception:
                pass
            return

        try:
            from bs4 import BeautifulSoup
        except Exception:
            return
        soup = BeautifulSoup(html, "html.parser")

        original_cates = (store_cate1, store_cate2)
        store_cate1, store_cate2 = filter_unexposed_file_detail_cates(
            html,
            store_cate1,
            store_cate2,
        )
        if original_cates != (store_cate1, store_cate2):
            logger.debug(
                "[Cate][file] unexposed detail category blocked | job_id=%s post_url=%s before=%s after=%s",
                getattr(self, "job_id", ""),
                (url or "")[:220],
                original_cates,
                (store_cate1, store_cate2),
            )

        reg_date_dt = self._extract_board_reg_date(soup, html=html, url=url)

        if self.start_date or self.end_date:
            if reg_date_dt is None:
                logger.debug("[기간필터][file] reg_date 미확인으로 포함 | url=%s", url[:120] if url else "")
            else:
                from backend.shared.date_utils import is_date_in_range

                if not is_date_in_range(reg_date_dt, self.start_date, self.end_date):
                    logger.debug("[기간필터] 기간 외 제외 | url=%s reg_date=%s", url[:120] if url else "", reg_date_dt)
                    return

        current_title = self._extract_board_title(soup, url=url, html=html)
        reg_date_val = reg_date_dt.strftime("%Y-%m-%d %H:%M:%S") if reg_date_dt else (getattr(it, "reg_date_str", "") or "")

        # Defer author/department selector work until an attachment is actually found.
        selector_profile = None
        author_info = {}
        file_author = None
        file_dept = None
        try:
            if hasattr(self, "_write_detail_failure_log"):
                self._write_detail_failure_log(
                    "file_attachment_extract_attempt",
                    url=url,
                    board_url=str(getattr(it, "board_url", "") or ""),
                    html_len=len(html or ""),
                    yhlib_count=(html or "").count("yhLib.file.download"),
                )
            _trace_key = canonicalize_url_for_dedup(url) or str(url or "").strip()
            async with self._stats_lock:
                fetch_keys = getattr(self, "_file_attachment_fetch_success_keys", None)
                if not isinstance(fetch_keys, set):
                    fetch_keys = set()
                    setattr(self, "_file_attachment_fetch_success_keys", fetch_keys)
                extract_keys = getattr(self, "_file_attachment_extract_attempt_keys", None)
                if not isinstance(extract_keys, set):
                    extract_keys = set()
                    setattr(self, "_file_attachment_extract_attempt_keys", extract_keys)
                if _trace_key:
                    fetch_keys.add(_trace_key)
                    extract_keys.add(_trace_key)
                self.stats["file_detail_fetch_success_count"] = len(fetch_keys)
                self.stats["file_attachment_extract_attempt_count"] = len(extract_keys)
        except Exception:
            pass
        attachments = self._extract_attachment_links_generic(html, base_url=url)
        kcohesion_ajax_attachments = []
        if "k-cohesion.go.kr" in str(url or "").lower():
            try:
                kcohesion_ajax_attachments = await self._extract_kcohesion_filelist_attachments(html, base_url=url)
            except Exception:
                kcohesion_ajax_attachments = []
        if kcohesion_ajax_attachments:
            def _kcohesion_file_token(value: Any) -> str:
                try:
                    path = urlparse(str(value or "")).path or ""
                    match = re.search(r"(?i)/afile/(?:fileopen/(?:pdf|hwp|hwpx)/|filedownload/)([A-Za-z0-9_-]+)$", path)
                    return str(match.group(1) or "").lower() if match else ""
                except Exception:
                    return ""

            token_to_index = {
                _kcohesion_file_token((attachment or {}).get("href")): index
                for index, attachment in enumerate(attachments or [])
                if isinstance(attachment, dict) and _kcohesion_file_token(attachment.get("href"))
            }
            seen_attachment_keys = {
                canonicalize_url_for_dedup(str(a.get("href") or "")) or str(a.get("href") or "").strip().lower()
                for a in (attachments or [])
                if isinstance(a, dict)
            }
            for attach in kcohesion_ajax_attachments:
                href = str((attach or {}).get("href") or "").strip()
                key = canonicalize_url_for_dedup(href) or href.lower()
                token = _kcohesion_file_token(href)
                existing_index = token_to_index.get(token)
                if existing_index is not None:
                    # The AJAX payload is authoritative for the original filename
                    # and direct download route; generic HTML exposes only a token.
                    attachments[existing_index] = dict(attach)
                    seen_attachment_keys.add(key)
                elif key and key not in seen_attachment_keys:
                    attachments.append(attach)
                    seen_attachment_keys.add(key)
        try:
            if hasattr(self, "_write_detail_failure_log"):
                self._write_detail_failure_log(
                    "file_attachment_extract_result",
                    url=url,
                    board_url=str(getattr(it, "board_url", "") or ""),
                    html_len=len(html or ""),
                    attachment_count=len(attachments or []),
                    kcohesion_ajax_count=len(kcohesion_ajax_attachments or []),
                    yhlib_count=(html or "").count("yhLib.file.download"),
                    sample=[
                        {
                            "name": (a.get("name") or a.get("title") or a.get("text") or "")[:120],
                            "href": (a.get("href") or "")[:220],
                        }
                        for a in (attachments or [])[:5]
                        if isinstance(a, dict)
                    ],
                )
            _log_file_url_status(
                stage="attachment_extract",
                status="found" if attachments else "empty",
                process_url=url,
                post_url=url,
                selected="pending" if attachments else "no",
                saved="pending" if attachments else "no",
                learn="pending" if attachments else "not_started",
                count=len(attachments or []),
                reason="" if attachments else "attachment_empty",
                job_id=getattr(self, "job_id", ""),
                db_name=getattr(self, "db_name", ""),
            )
            logger.debug(
                "[FileProbeDebug][attachments.extract] job_id=%s url=%s html_len=%s yhlib_count=%s count=%s sample=%s",
                getattr(self, "job_id", None),
                (url or "")[:220],
                len(html or ""),
                (html or "").count("yhLib.file.download"),
                len(attachments or []),
                [
                    {
                        "name": (a.get("name") or a.get("title") or a.get("text") or "")[:120],
                        "href": (a.get("href") or "")[:220],
                    }
                    for a in (attachments or [])[:5]
                    if isinstance(a, dict)
                ],
            )
        except Exception:
            pass
        if attachments:
            try:
                sample = [
                    {
                        "name": (a.get("name") or a.get("title") or a.get("text") or "")[:180],
                        "url": (a.get("href") or "")[:500],
                        "source_page": url,
                    }
                    for a in (attachments or [])[:20]
                    if isinstance(a, dict)
                ]
                async with self._stats_lock:
                    self.stats["event"] = "attachments_found"
                    self.stats["message"] = f"attachments found: {len(attachments or [])}"
                    self.stats["attachment_count"] = len(attachments or [])
                    self.stats["file_attachment_found_count"] = max(
                        int(self.stats.get("file_attachment_found_count", 0) or 0),
                        len(attachments or []),
                    )
                    self.stats["file_attachment_found_samples"] = sample
                    self.stats["attachments"] = sample
                    self.stats["recent_files"] = sample
                if self.progress_callback:
                    self.progress_callback(self.get_stats())
            except Exception:
                pass
        if not attachments and is_yongin_water_attachment_detail_url(url):
            logger.info(
                "[yongin][water][file] static attachment miss -> Playwright retry | url=%s",
                (url or "")[:160],
            )
            retry_html = await self._fetch_html_playwright_detail_fallback(url)
            if retry_html:
                html = retry_html
                soup = BeautifulSoup(html, "html.parser")
                attachments = self._extract_attachment_links_generic(html, base_url=url)
                try:
                    logger.debug(
                        "[FileProbeDebug][attachments.retry_extract] job_id=%s url=%s html_len=%s count=%s sample=%s",
                        getattr(self, "job_id", None),
                        (url or "")[:220],
                        len(html or ""),
                        len(attachments or []),
                        [
                            {
                                "name": (a.get("name") or a.get("title") or a.get("text") or "")[:120],
                                "href": (a.get("href") or "")[:220],
                            }
                            for a in (attachments or [])[:5]
                            if isinstance(a, dict)
                        ],
                    )
                except Exception:
                    pass
        if not attachments:
            try:
                if hasattr(self, "_write_detail_failure_log"):
                    self._write_detail_failure_log(
                        "file_attachment_extract_empty",
                        url=url,
                        board_url=str(getattr(it, "board_url", "") or ""),
                        html_len=len(html or ""),
                        yhlib_count=(html or "").count("yhLib.file.download"),
                        title=(current_title or "")[:160],
                    )
                _trace_key = canonicalize_url_for_dedup(url) or str(url or "").strip()
                async with self._stats_lock:
                    empty_keys = getattr(self, "_file_attachment_extract_empty_keys", None)
                    if not isinstance(empty_keys, set):
                        empty_keys = set()
                        setattr(self, "_file_attachment_extract_empty_keys", empty_keys)
                    if _trace_key:
                        empty_keys.add(_trace_key)
                    self.stats["file_attachment_extract_empty_count"] = len(empty_keys)
                logger.debug(
                    "[FileProbeDebug][attachments.none] job_id=%s url=%s html_len=%s yhlib_count=%s title=%s",
                    getattr(self, "job_id", None),
                    (url or "")[:220],
                    len(html or ""),
                    (html or "").count("yhLib.file.download"),
                    (current_title or "")[:160],
                )
            except Exception:
                pass
        if attachments:
            try:
                _trace_key = canonicalize_url_for_dedup(url) or str(url or "").strip()
                async with self._stats_lock:
                    found_keys = getattr(self, "_file_attachment_found_post_keys", None)
                    if not isinstance(found_keys, set):
                        found_keys = set()
                        setattr(self, "_file_attachment_found_post_keys", found_keys)
                    if _trace_key:
                        found_keys.add(_trace_key)
                    self.stats["file_attachment_found_post_count"] = len(found_keys)
                    self.stats["file_attachment_found_total_count"] = int(
                        self.stats.get("file_attachment_found_total_count", 0) or 0
                    ) + len(attachments or [])
                selector_profile = await self._get_selector_profile_for_detail(
                    url=url,
                    board_url=getattr(it, "board_url", "") or "",
                )
                author_info = _extract_file_author_info(html, url=url, selector_profile=selector_profile)
                file_author = (
                    author_info.get("content_author")
                    or author_info.get("author")
                    or author_info.get("department")
                )
                file_dept = author_info.get("department")
                if _content_author_debug_enabled():
                    logger.warning(
                        "[ContentAuthorDebug][file_detail.extract_after_attachment] job_id=%s url=%s result=%r author=%r content_author=%r department=%r kind=%r raw=%r selector_profile=%s title=%r",
                        getattr(self, "job_id", None),
                        (url or "")[:220],
                        _content_author_debug_value(file_author),
                        _content_author_debug_value(author_info.get("author")),
                        _content_author_debug_value(author_info.get("content_author")),
                        _content_author_debug_value(file_dept),
                        _content_author_debug_value(author_info.get("author_kind")),
                        _content_author_debug_value(author_info.get("author_raw")),
                        bool(selector_profile),
                        _content_author_debug_value(current_title),
                    )
                original_file_author_for_debug = file_author
                try:
                    ct = (current_title or "").strip()
                    fa = (str(file_author).strip() if file_author is not None else "")
                    if ct and fa and ct == fa:
                        file_author = None
                except Exception:
                    pass
                if _content_author_debug_enabled() and original_file_author_for_debug != file_author:
                    logger.debug(
                        "[ContentAuthorDebug][file_detail.author_rejected] job_id=%s url=%s reason=equals_title before=%r after=%r title=%r",
                        getattr(self, "job_id", None),
                        (url or "")[:220],
                        _content_author_debug_value(original_file_author_for_debug),
                        _content_author_debug_value(file_author),
                        _content_author_debug_value(current_title),
                    )
                _log_file_url_status(
                    stage="download_enqueue",
                    status="start",
                    process_url=url,
                    post_url=url,
                    selected="pending",
                    saved="pending",
                    learn="pending",
                    count=len(attachments or []),
                    job_id=getattr(self, "job_id", ""),
                    db_name=getattr(self, "db_name", ""),
                )
                await self._enqueue_file_downloads(
                    post_url=url,
                    board_url=getattr(it, "board_url", ""),
                    reg_date=reg_date_val,
                    attachments=attachments,
                    author=file_author,
                    department=file_dept,
                    author_kind=author_info.get("author_kind"),
                    author_raw=author_info.get("author_raw"),
                    department_raw=author_info.get("department_raw"),
                    contact_phone=None,
                    view_count=None,
                    sync_after_download=True,
                    detail_cates=(store_cate1, store_cate2),
                    post_title=current_title or "",
                )
                try:
                    if hasattr(self, "_write_detail_failure_log"):
                        self._write_detail_failure_log(
                            "file_attachment_enqueue_called",
                            url=url,
                            board_url=str(getattr(it, "board_url", "") or ""),
                            attachment_count=len(attachments or []),
                        )
                except Exception:
                    pass
                if _content_author_debug_enabled():
                    logger.debug(
                        "[ContentAuthorDebug][file_detail.enqueue_called] job_id=%s url=%s attachment_count=%s result=%r author=%r content_author=%r department=%r kind=%r raw=%r",
                        getattr(self, "job_id", None),
                        (url or "")[:220],
                        len(attachments or []),
                        _content_author_debug_value(file_author),
                        _content_author_debug_value(author_info.get("author")),
                        _content_author_debug_value(author_info.get("content_author")),
                        _content_author_debug_value(file_dept),
                        _content_author_debug_value(author_info.get("author_kind")),
                        _content_author_debug_value(author_info.get("author_raw")),
                    )
            except Exception as e:
                try:
                    if hasattr(self, "_write_detail_failure_log"):
                        self._write_detail_failure_log(
                            "file_attachment_enqueue_failed",
                            url=url,
                            board_url=str(getattr(it, "board_url", "") or ""),
                            attachment_count=len(attachments or []),
                            error=repr(e),
                        )
                except Exception:
                    pass
                _log_file_url_status(
                    stage="download_enqueue",
                    status="error",
                    process_url=url,
                    post_url=url,
                    selected="no",
                    saved="no",
                    learn="not_started",
                    count=len(attachments or []),
                    error=repr(e),
                    job_id=getattr(self, "job_id", ""),
                    db_name=getattr(self, "db_name", ""),
                )
                logger.error("[file_workflow] _enqueue_file_downloads failed: %s", e)
        async with self._stats_lock:
            if url_key not in self._seen_filtered_detail:
                self._seen_filtered_detail.add(url_key)
            self._sync_file_mode_scan_count()

        if self.progress_callback:
            self.progress_callback(self.get_stats())


























