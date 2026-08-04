# core/crawler/workers/scan.py
import sys
import os

# 프로젝트 루트를 sys.path에 추가 (core/crawler/workers -> ../../../)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# (debug) NDJSON instrumentation removed
"""
게시판 스캔 워커
"""
import asyncio
import logging
import re
import hashlib
from urllib.parse import urljoin, urlparse, quote_plus, parse_qsl, urlencode, urlunparse
from typing import Optional, Dict, Any, List, Callable, Awaitable
from playwright.async_api import Browser, Error, BrowserContext, TimeoutError as PlaywrightTimeoutError
import json
import time
from config.constants import BOARD_PATTERNS, SKIP_DEPTH_PATTERNS, ALLOWED_EXTENSIONS
from core.crawler.batch_queue import BatchQueue
from core.crawler.dedup import CollectionDeduplicator
from utils.runtime_flags import is_no_limits_mode
from utils.url import ensure_songpa_www_path

try:
    import requests  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]

try:
    import httpx  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)
# 로거 핸들러 및 레벨 설정 (핸들러가 없을 때만)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(name)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)  # 운영 기본: INFO (기본 DEBUG 강제 설정 제거)

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
# 페이지 이동 후 추가 데이터(JS 등) 로딩을 기다리는 고정 시간 (5000ms -> 2000ms로 단축)
MANUAL_POST_GOTO_WAIT_MS = 2000

def _ensure_url_scheme(url: str) -> str:
    if not re.match(r"^[a-zA-Z]+://", url):
        return "http://" + url
    return url


def _normalize_url(candidate: str, base: str) -> str:
    """
    [강화된 URL 정규화]
    1. 상대 경로 복원 및 도메인 유실 방지
    2. jsessionid, rnd 등 가비지 파라미터 제거
    3. 파라미터 알파벳 정렬을 통한 중복 방문 방지
    """
    if not candidate:
        return ""
        
    if candidate.startswith(("http://", "https://")):
        next_url = candidate
    else:
        if candidate.startswith("//"):
            candidate = "https:" + candidate
        next_url = urljoin(base, candidate)

    parsed = urlparse(next_url)
    
    # 도메인 강제 복구 (urljoin 실패 대비)
    if not parsed.netloc:
        base_parsed = urlparse(base)
        next_url = urlunparse((
            base_parsed.scheme or "https",
            base_parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        parsed = urlparse(next_url)

    # 파라미터 정제 및 정렬
    query_params = parse_qsl(parsed.query, keep_blank_values=False)
    cleaned_query = []
    seen_keys = set()
    
    for k, v in query_params:
        if k.lower() in ["jsessionid", "rnd", "timestamp", "_", "menu_cd"]:
            continue
        if k not in seen_keys:
            seen_keys.add(k)
            cleaned_query.append((k, v))
            
    cleaned_query.sort(key=lambda x: x[0])
    new_query_string = urlencode(cleaned_query)

    return urlunparse((
        parsed.scheme, parsed.netloc, parsed.path,
        parsed.params, new_query_string, ""  # 앵커 # 제거
    ))
    
async def scan_worker(
    in_queue: asyncio.Queue,
    scan_batch_queue: BatchQueue,
    collection_batch_queue: BatchQueue,
    progress_queue: asyncio.Queue,
    browser: Browser,
    max_depth: int = 1,
    context_options: Optional[Dict[str, Any]] = None,
    browser_relauncher: Optional[Callable[[], Awaitable[Browser]]] = None,
    max_concurrent_pages: int = 2,  # 동시에 열 수 있는 페이지 수 제한
    heartbeat_guard: Optional[Callable[[], bool]] = None,
    start_date=None,
    end_date=None,
    chat_bot_id: Optional[str] = None,
    db_name: Optional[str] = None,
    file_deduplicator: Optional[CollectionDeduplicator] = None,
):
    """
    Scan Worker:
    - in_queue에서 URL을 가져와 스캔
    - 발견된 URL은 scan_batch_queue로 전달
    - 진행 상황은 progress_queue를 통해 알림
    - max_concurrent_pages: 동시에 열 수 있는 페이지 수 제한 (기본값: 2)
    - start_date, end_date: 날짜 필터링 범위 (None이면 필터링 안 함)
    - chat_bot_id, db_name: 파일 다운로드 경로 생성을 위한 메타데이터
    """
    import sys
    import os
    import contextvars
    import html as _html_unescape
    # 프로젝트 루트를 sys.path에 추가하여 backend.shared.date_utils import 가능하도록
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from backend.shared.date_utils import extract_post_date, is_date_in_range, extract_date_from_text
    from backend.shared.detail_page_utils import (
        is_detail_page_url,
        is_post_detail_page_from_html,
        find_detail_page_url_in_parent,
    )
    from backend.board.board_meta_extractor import (
        extract_author_from_html,
        extract_author_from_text,
        extract_author_info_from_html,
    )
    from datetime import datetime, timedelta
    # ===== job_id 격리 =====
    # 전역 워커풀(멀티 job_id)에서 scan_worker를 공유할 수 있도록,
    # visited/dedupe/필터 컨텍스트를 job_id 단위로 분리한다.
    _default_start_date = start_date
    _default_end_date = end_date
    _default_chat_bot_id = chat_bot_id
    _default_db_name = db_name

    visited_by_job: Dict[str, set] = {}
    visited_urls = visited_by_job.setdefault("default", set())

    file_dedup_by_job: Optional[Dict[str, CollectionDeduplicator]] = None
    if file_deduplicator is None:
        file_dedup_by_job = {}

    # 전역 워커풀에서 background task(create_task)들이 job 컨텍스트를 잃지 않도록 ContextVar 사용
    _scan_ctx_var: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
        "scan_ctx",
        default={
            "job_id": "default",
            "chat_bot_id": _default_chat_bot_id,
            "db_name": _default_db_name,
            "start_date": _default_start_date,
            "end_date": _default_end_date,
        },
    )

    def _is_scan_disabled(job_id: str) -> bool:
        """
        전역 워커풀(GlobalWorkerPool) stop semantics:
        - stop 요청 시 pool.disable_scan(job_id)가 호출되어 ctx.scan_enabled=False로 전환된다.
        - scan_worker가 이미 처리 중인 페이지에서도, 더 이상 collection/file 후보를 enqueue하지 않게 하여
          '중단 후 탐색이 계속되는 것처럼 보이는' 체감을 줄인다.
        """
        try:
            from core.crawler.global_pool import get_global_worker_pool
            ctx = get_global_worker_pool().get_job_context(str(job_id))
            if ctx is None:
                return False
            return not bool(getattr(ctx, "scan_enabled", True))
        except Exception:
            return False

    def _is_list_page_url(u: str) -> bool:
        try:
            lu = (u or "").lower()
        except Exception:
            lu = str(u).lower()

        # 1. 상세 페이지 식별자(파라미터/액션)가 포함되면 리스트가 아님
        if any(s in lu for s in (
            'nttid=', 'num=', 'seq=', 'artid=', 'article_no=', 'board_no=',
            'view.do', 'detail.do', 'read.do',
            'view.asp', 'detail.asp', 'read.asp',
            'view.jsp', 'detail.jsp', 'read.jsp',
            'view.php', 'detail.php', 'read.php',
            'boardview', 'board_view', 'articleview'
        )):
            return False

        # 2. 파일 다운로드/핸들러 패턴 제외
        if any(f in lu for f in ('download', 'filedown', 'atchfileid')):
            return False

        # 3. 명시적 리스트 패턴 (list 키워드 포함 시 강력 추정)
        if 'list' in lu:
            return True

        # 4. 그 외 게시판 관련 키워드가 있으면 리스트로 "추정"
        # (이미 상세/파일 패턴은 위에서 걸러졌으므로, board/notice 등이 포함되면 목록일 확률이 높음)
        if any(p in lu for p in BOARD_PATTERNS):
            return True

        return False

    def _canonicalize_file_url(candidate: str, base: str) -> str:
        """
        fileDown/file handler URL은 같은 파일이 다양한 형태로(상대/절대, www 유무, query 순서, redirect) 등장한다.
        스캔 단계에서 워커 간 dedupe를 안정적으로 하기 위해 canonical 형태로 정규화한다.
        """
        if not candidate:
            return ""
        try:
            u = candidate.strip()
        except Exception:
            u = str(candidate)
        if not u:
            return ""
        # javascript: 래퍼는 파일 URL이 아니므로 그대로 두지 않는다.
        if u.lower().startswith("javascript:"):
            # JS에서 직접 URL을 뽑는 로직(_extract_direct_download_url)이 따로 있으므로 여기서는 빈 값 처리
            return ""
        # absolute url로 변환
        try:
            abs_u = u if u.startswith(("http://", "https://")) else urljoin(base, u)
        except Exception:
            abs_u = u
        try:
            p = urlparse(abs_u)
            scheme = (p.scheme or "https").lower()
            netloc = (p.netloc or "").lower()
            # 흔한 중복 케이스: www 유무
            if netloc.startswith("www."):
                netloc = netloc[4:]
            path = p.path or ""
            # query 정렬(순서가 달라 생기는 중복 제거)
            q = ""
            try:
                pairs = parse_qsl(p.query or "", keep_blank_values=True)
                pairs.sort()
                q = urlencode(pairs, doseq=True)
            except Exception:
                q = p.query or ""
            # fragment 제거
            return urlunparse((scheme, netloc, path, "", q, ""))
        except Exception:
            return abs_u

    # list -> view 확장 후, view를 "즉시" 확인할지 여부(기본: 일부만 즉시 처리)
    # - 너무 많이 즉시 처리하면 워커가 list 처리에 오래 붙잡힐 수 있으므로 상한을 둔다.
    try:
        INLINE_VIEW_PROCESSING = os.getenv("SCAN_INLINE_VIEW_PROCESSING", "1") == "1"
    except Exception:
        INLINE_VIEW_PROCESSING = True
    try:
        INLINE_VIEW_PROCESSING_MAX = int(os.getenv("SCAN_INLINE_VIEW_PROCESSING_MAX", "3") or "3")
    except Exception:
        INLINE_VIEW_PROCESSING_MAX = 3
    # 동일 source_page(view.do 등)에서 파일이 여러 개 나올 수 있으므로 메타를 캐시한다.
    source_author_cache: Dict[str, Optional[str]] = {}
    source_department_cache: Dict[str, Optional[str]] = {}
    source_author_kind_cache: Dict[str, Optional[str]] = {}
    source_author_raw_cache: Dict[str, Optional[str]] = {}
    source_department_raw_cache: Dict[str, Optional[str]] = {}
    # list.do → view.do 추출 캐시 (job_id별로 분리: 크롤링 재실행 시 list를 다시 스캔하도록)
    _list_view_cache_by_job: Dict[str, Dict[str, List[str]]] = {}
    # list 페이지네이션(자동 pageIndex 증가) 중복 방지용 시그니처 캐시
    _list_page_signature_by_job: Dict[str, Dict[str, set[str]]] = {}
    # 기간 초과 반복 감지(리스트 페이지) 캐시
    _list_page_date_gate_by_job: Dict[str, Dict[str, int]] = {}
    _list_page_date_stop_by_job: Dict[str, set[str]] = {}
    context: Optional[BrowserContext] = None
    
    # 동시 페이지 수 제한을 위한 Semaphore
    page_semaphore = asyncio.Semaphore(max_concurrent_pages)

    DOWNLOAD_SELECTOR = (
        'a[href*="download(" i], a[href*="fileDown" i], '
        'a[href^="javascript:" i], '  # javascript: 링크 전체 포함 (preListen 등)
        'a[onclick*="download" i], a[onclick*="fileDown" i], '
        'button[onclick*="download" i], button[onclick*="fileDown" i], '
        'span[onclick*="download" i], span[onclick*="fileDown" i]'
    )
    CALL_PATTERN = re.compile(r"(?:javascript:)?(?P<func>[A-Za-z_][\w]*)\s*\((?P<args>[^)]*)\)")
    DEFAULT_DOWNLOAD_TEMPLATES = {
        "download": "/common/board/fileDown.do?atchFileId={atchFileId}&fileSn={fileSn}",
        "fn_download": "/cmm/fms/FileDown.do?atchFileId={atchFileId}&fileSn={fileSn}",
        "filedown": "/cmm/fms/FileDown.do?atchFileId={atchFileId}&fileSn={fileSn}",
        "filedownload": "/cmm/fms/FileDown.do?atchFileId={atchFileId}&fileSn={fileSn}",
    }

    STATIC_HEADERS = {
        "User-Agent": BROWSER_USER_AGENT
    }

    STATIC_ASSET_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.gif', '.ico', '.svg', '.webp']
    DOWNLOAD_HANDLER_KEYWORDS = [
        "download.asp",
        "download.do",
        "/include/download",
        "filedown",
        "filedownload",
        "file_down",
        "downfile",
        "getfile",
        "atchfileid=",
    ]

    # URL 필터링 로직은 Collection 단계로 이동됨

    async def reset_context():
        nonlocal context
        if context:
            try:
                await context.close()
            except Exception:
                pass
        context = None

    async def ensure_context() -> BrowserContext:
        nonlocal context, browser
        if context is None:
            context_kwargs = dict(context_options or {})
            context_kwargs.setdefault("user_agent", BROWSER_USER_AGENT)
            context_kwargs.setdefault("ignore_https_errors", True)
            context_kwargs.setdefault("bypass_csp", False)
            max_attempts = 5
            for attempt in range(1, max_attempts + 1):
                try:
                    context = await browser.new_context(**context_kwargs)
                    logger.debug("[Scan] Created new browser context for worker.")
                    return context
                except Exception as exc:
                    # TargetClosed 류는 재런치 후 재시도 가능
                    if is_target_closed_error(exc):
                        logger.warning(
                            "[Scan] Browser/context appears closed. Attempting relaunch... (attempt %s/%s)",
                            attempt,
                            max_attempts,
                        )
                        if browser_relauncher:
                            context = None
                            try:
                                browser = await browser_relauncher()
                                await asyncio.sleep(1)  # 부하 분산 및 안정화 대기
                                continue
                            except Exception as re_err:
                                logger.error("[Scan] Browser relaunch failed: %s", re_err)
                        # relauncher가 없거나 relaunch 실패면 즉시 상위로 전달
                    raise

            # 여기까지 왔다면 계속 TargetClosed로 실패했지만 relaunch는 되었거나(continue) 컨텍스트를 못 만들었다는 뜻
            context = None
            raise RuntimeError("[Scan] Failed to create browser context after retries (context is None)")

        # context가 이미 있는 경우(재사용)
        if context is None:
            # 이 케이스가 발생하면 내부 상태가 꼬인 것이므로 빠르게 복구 시도하도록 예외로 올린다.
            raise RuntimeError("[Scan] Browser context is None (unexpected)")
        return context

    async def safe_goto(target_url: str, page_profile: Optional[Dict[str, Any]] = None):
        # safe_goto only opens a page. Caller (scan_worker) does goto/close.
        nonlocal browser
        last_exc = None
        for attempt in range(1, 4):
            ctx = await ensure_context()
            if ctx is None:
                # 방어적 처리: ensure_context는 원칙적으로 None을 반환하면 안 된다.
                last_exc = RuntimeError("[SafeGoto] ensure_context returned None")
                await reset_context()
                if browser_relauncher:
                    try:
                        browser = await browser_relauncher()
                    except Exception:
                        pass
                continue
            try:
                return await ctx.new_page()
            except Error as exc:
                last_exc = exc
                if is_target_closed_error(exc):
                    logger.warning("[SafeGoto] Context/browser closed when creating page; resetting context (attempt %s)", attempt)
                    await reset_context()
                    if browser_relauncher:
                        try:
                            browser = await browser_relauncher()
                        except Exception:
                            pass
                    continue
                raise
        raise last_exc

    def is_target_closed_error(exc: Exception) -> bool:
        """Playwright의 TargetClosedError 여부 확인"""
        message = str(exc).lower()
        patterns = [
            "target closed", 
            "browser has been closed", 
            "context or browser has been closed",
            "target page, context or browser has been closed",
            "connection closed"
        ]
        return any(p in message for p in patterns)

    def _looks_like_file(url: str) -> bool:
        if not url:
            return False
        lowered = url.lower().split("?")[0]
        return any(lowered.endswith(ext) for ext in ALLOWED_EXTENSIONS)

    def _looks_like_download_handler(url: str) -> bool:
        if not url:
            return False
        lowered = url.lower()
        return any(keyword in lowered for keyword in DOWNLOAD_HANDLER_KEYWORDS)

    def _extract_filename_candidate(*candidates: Optional[str]) -> str:
        for candidate in candidates:
            if not candidate:
                continue
            cleaned = candidate.strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            for ext in ALLOWED_EXTENSIONS:
                if not ext:
                    continue
                idx = lowered.find(ext)
                if idx != -1:
                    return cleaned[: idx + len(ext)].strip()
        return ""

    def _parse_env_list(name: str, default_list: List[str]) -> List[str]:
        raw = os.getenv(name, "")
        if not raw:
            return list(default_list)
        items = []
        for part in raw.split(","):
            v = part.strip()
            if v:
                items.append(v)
        return items if items else list(default_list)

    _PRIVATE_DETECT_ENABLED = str(os.getenv("PRIVATE_DETECT_ENABLED", "1")).strip().lower() not in ("0", "false", "no", "off")
    _PRIVATE_TEXT_KEYWORDS = tuple(
        _parse_env_list(
            "PRIVATE_KEYWORDS",
            [
                "비공개",
                "비공개글",
                "비공개 글",
                "비공개 게시물",
                "비밀글",
                "비밀 글",
                "비밀 게시물",
                "잠금",
                "잠김",
            ],
        )
    )
    _PRIVATE_TEXT_KEYWORDS_LOWER = tuple(
        _parse_env_list("PRIVATE_KEYWORDS_EN", ["private", "secret", "locked"])
    )
    _PRIVATE_REGEX_RAW = os.getenv("PRIVATE_KEYWORD_REGEX", "").strip()
    try:
        _PRIVATE_REGEX = re.compile(_PRIVATE_REGEX_RAW, re.IGNORECASE) if _PRIVATE_REGEX_RAW else None
    except Exception:
        _PRIVATE_REGEX = None

    def _contains_private_marker(text: str) -> bool:
        if not _PRIVATE_DETECT_ENABLED:
            return False
        if not text:
            return False
        try:
            lowered = text.lower()
        except Exception:
            lowered = ""
        for kw in _PRIVATE_TEXT_KEYWORDS:
            if kw and kw in text:
                return True
        for kw in _PRIVATE_TEXT_KEYWORDS_LOWER:
            if kw and kw in lowered:
                return True
        if _PRIVATE_REGEX:
            try:
                if _PRIVATE_REGEX.search(text):
                    return True
            except Exception:
                pass
        # 비밀번호 입력 화면 감지(강한 신호)
        if ("비밀번호" in text) or ("password" in lowered):
            if ("입력" in text) or ("확인" in text) or re.search(r"type=[\"']password[\"']", lowered):
                return True
        return False

    def _is_private_url(target_url: str) -> bool:
        if not _PRIVATE_DETECT_ENABLED:
            return False
        try:
            q = parse_qsl(urlparse(target_url).query or "", keep_blank_values=True)
        except Exception:
            return False
        for k, v in q:
            key = (k or "").strip().lower()
            val = (str(v) if v is not None else "").strip().lower()
            if key in tuple(_parse_env_list("PRIVATE_URL_PARAMS", [
                "secret",
                "secret_yn",
                "secretyn",
                "is_secret",
                "issecret",
                "private",
                "private_yn",
                "lock",
                "locked",
                "is_private",
                "isprivate",
            ])):
                if val in ("y", "yes", "1", "true", "on"):
                    return True
            if key in tuple(_parse_env_list("PRIVATE_URL_PUBLIC_PARAMS", [
                "open",
                "open_yn",
                "openyn",
                "public",
                "public_yn",
                "publicyn",
            ])):
                if val in ("n", "no", "0", "false", "off"):
                    return True
        return False

    def _is_private_list_item(a_tag) -> bool:
        if not _PRIVATE_DETECT_ENABLED:
            return False
        try:
            parts: List[str] = []
            if a_tag:
                try:
                    parts.append(a_tag.get_text(" ", strip=True))
                except Exception:
                    pass
                for attr in ("title", "aria-label"):
                    try:
                        v = a_tag.get(attr)
                        if v:
                            parts.append(str(v))
                    except Exception:
                        pass
                try:
                    cls = a_tag.get("class") or []
                    if cls:
                        parts.append(" ".join(cls))
                except Exception:
                    pass
                row = a_tag.find_parent(["tr", "li", "div"])
                if row is not None:
                    try:
                        parts.append(row.get_text(" ", strip=True))
                    except Exception:
                        pass
                    try:
                        for img in row.find_all("img"):
                            alt = img.get("alt") or ""
                            if alt:
                                parts.append(str(alt))
                    except Exception:
                        pass
            text = " ".join([p for p in parts if p])
            return _contains_private_marker(text)
        except Exception:
            return False

    def _private_reason(html_text: str) -> Dict[str, Any]:
        matched: List[str] = []
        if not html_text:
            return {"matched": matched, "has_password_input": False, "has_password_text": False}
        try:
            lowered = html_text.lower()
        except Exception:
            lowered = ""
        for kw in _PRIVATE_TEXT_KEYWORDS:
            if kw and kw in html_text:
                matched.append(kw)
        for kw in _PRIVATE_TEXT_KEYWORDS_LOWER:
            if kw and kw in lowered:
                matched.append(kw)
        has_password_input = bool(re.search(r"type=[\"']password[\"']", lowered))
        has_password_text = ("비밀번호" in html_text) or ("password" in lowered)
        if has_password_text and ("입력" in html_text or "확인" in html_text or has_password_input):
            matched.append("password_prompt")
        return {
            "matched": matched[:3],
            "has_password_input": has_password_input,
            "has_password_text": has_password_text,
        }

    def _is_private_html(html_text: str) -> bool:
        if not _PRIVATE_DETECT_ENABLED:
            return False
        if not html_text:
            return False
        try:
            lowered = html_text.lower()
            # password input이 있는 경우는 강한 신호
            if re.search(r"type=[\"']password[\"']", lowered):
                return True
            # 비밀번호 문구만으로는 오탐이 많아 기본은 무시한다.
            # 필요 시 env로 켜서 "입력/확인" 문구 기반 감지를 복구할 수 있다.
            try:
                prompt_enabled = str(os.getenv("SCAN_PRIVATE_PASSWORD_PROMPT", "0") or "0").strip().lower()
            except Exception:
                prompt_enabled = "0"
            if prompt_enabled not in ("0", "false", "no", "off"):
                if ("비밀번호" in html_text) or ("password" in lowered):
                    if ("입력" in html_text) or ("확인" in html_text):
                        return True
            # 텍스트 키워드 기반 판정은 strict 모드에서만 사용 (기본 off)
            try:
                strict_text = str(os.getenv("SCAN_PRIVATE_TEXT_STRICT", "0") or "0").strip().lower()
            except Exception:
                strict_text = "0"
            if strict_text not in ("0", "false", "no", "off"):
                if _contains_private_marker(html_text):
                    return True
        except Exception:
            return False
        return False

    _INVALID_PAGE_TEXTS = (
        "없는 페이지",
        "페이지가 존재하지",
        "요청하신 페이지를 찾을 수 없습니다",
        "페이지를 찾을 수 없습니다",
    )

    def _is_invalid_page_html(html_text: str) -> bool:
        if not html_text:
            return False
        for phrase in _INVALID_PAGE_TEXTS:
            if phrase in html_text:
                return True
        return False

    def _is_file_mode(profile: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(profile, dict):
            return False
        if profile.get("file_mode"):
            return True
        try:
            return str(profile.get("colle") or "").strip().lower() == "file"
        except Exception:
            return False

    def _row_has_attachment(tag) -> bool:
        if not tag:
            return False
        try:
            row = tag.find_parent(["tr", "li"])
        except Exception:
            row = None
        if row is None:
            try:
                row = tag.find_parent()
            except Exception:
                row = None
        if row is None:
            return False
        try:
            text = row.get_text(" ", strip=True)
        except Exception:
            text = ""
        if text and any(k in text for k in ("첨부", "파일", "download", "file")):
            return True
        try:
            for a in row.find_all("a", href=True):
                href = (a.get("href") or "").lower()
                if any(k in href for k in ("filedown", "filedown", "atchfileid", "filesn", "filedownload", "download")):
                    return True
        except Exception:
            pass
        try:
            class_str = " ".join(row.get("class") or []).lower()
        except Exception:
            class_str = ""
        if class_str and any(k in class_str for k in ("file", "attach", "attachment", "down")):
            return True
        try:
            for img in row.find_all("img"):
                alt = (img.get("alt") or "").lower()
                title = (img.get("title") or "").lower()
                if any(k in alt for k in ("첨부", "file", "download", "attach")):
                    return True
                if any(k in title for k in ("첨부", "file", "download", "attach")):
                    return True
        except Exception:
            pass
        return False

    # 정적 fetch는 "탐색 체감"에 큰 영향을 주므로 타임아웃을 ENV로 제어한다.
    # - SCAN_STATIC_FETCH_TIMEOUT_SEC: 일반 정적 fetch 타임아웃(기본 12초)
    # - SCAN_STATIC_ONLY_FETCH_TIMEOUT_SEC: static_only phase(정적 우선)에서의 더 짧은 타임아웃(기본 10.0초)
    try:
        _STATIC_FETCH_TIMEOUT_SEC = float(os.getenv("SCAN_STATIC_FETCH_TIMEOUT_SEC", "12") or "12")
    except Exception:
        _STATIC_FETCH_TIMEOUT_SEC = 12.0
    try:
        _STATIC_ONLY_FETCH_TIMEOUT_SEC = float(os.getenv("SCAN_STATIC_ONLY_FETCH_TIMEOUT_SEC", "10.0") or "10.0")
    except Exception:
        _STATIC_ONLY_FETCH_TIMEOUT_SEC = 10.0
    _STATIC_FETCH_TIMEOUT_SEC = max(0.5, min(_STATIC_FETCH_TIMEOUT_SEC, 60.0))
    _STATIC_ONLY_FETCH_TIMEOUT_SEC = max(0.5, min(_STATIC_ONLY_FETCH_TIMEOUT_SEC, 60.0))

    # 정적 fetch 세부 제어(옵션):
    # - requests timeout 튜플(connect, read)을 지원하기 위해 connect/read를 분리한다.
    # - SCAN_STATIC_FETCH_CONNECT_TIMEOUT_SEC / SCAN_STATIC_FETCH_READ_TIMEOUT_SEC
    # - SCAN_STATIC_ONLY_FETCH_CONNECT_TIMEOUT_SEC / SCAN_STATIC_ONLY_FETCH_READ_TIMEOUT_SEC
    # - SCAN_STATIC_FETCH_MAX_ATTEMPTS: 일반 정적 fetch 최대 시도 횟수(기본 2 = 1회 재시도)
    # - SCAN_STATIC_ONLY_FETCH_MAX_ATTEMPTS: static_only 최대 시도 횟수(기본 2 = 1회 재시도)
    # - SCAN_STATIC_FETCH_RETRY_BACKOFF_SEC / SCAN_STATIC_FETCH_RETRY_BACKOFF_MAX_SEC: 재시도 백오프(기본 0.35s, max 1.5s)
    def _env_float(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)) or str(default))
        except Exception:
            return float(default)
    def _env_int(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)) or str(default))
        except Exception:
            return int(default)
    
    _STATIC_FETCH_CONNECT_TIMEOUT_SEC = max(0.2, min(_env_float("SCAN_STATIC_FETCH_CONNECT_TIMEOUT_SEC", _STATIC_FETCH_TIMEOUT_SEC), 120.0))
    _STATIC_FETCH_READ_TIMEOUT_SEC = max(0.2, min(_env_float("SCAN_STATIC_FETCH_READ_TIMEOUT_SEC", _STATIC_FETCH_TIMEOUT_SEC), 120.0))
    _STATIC_ONLY_FETCH_CONNECT_TIMEOUT_SEC = max(0.2, min(_env_float("SCAN_STATIC_ONLY_FETCH_CONNECT_TIMEOUT_SEC", _STATIC_ONLY_FETCH_TIMEOUT_SEC), 120.0))
    _STATIC_ONLY_FETCH_READ_TIMEOUT_SEC = max(0.2, min(_env_float("SCAN_STATIC_ONLY_FETCH_READ_TIMEOUT_SEC", _STATIC_ONLY_FETCH_TIMEOUT_SEC), 120.0))
    
    _STATIC_FETCH_MAX_ATTEMPTS = max(1, min(_env_int("SCAN_STATIC_FETCH_MAX_ATTEMPTS", 2), 5))
    _STATIC_ONLY_FETCH_MAX_ATTEMPTS = max(1, min(_env_int("SCAN_STATIC_ONLY_FETCH_MAX_ATTEMPTS", 2), 3))
    _STATIC_FETCH_RETRY_BACKOFF_SEC = max(0.0, min(_env_float("SCAN_STATIC_FETCH_RETRY_BACKOFF_SEC", 0.35), 10.0))
    _STATIC_FETCH_RETRY_BACKOFF_MAX_SEC = max(_STATIC_FETCH_RETRY_BACKOFF_SEC, min(_env_float("SCAN_STATIC_FETCH_RETRY_BACKOFF_MAX_SEC", 1.5), 30.0))

    # ==================== Inline views bulk: background scheduling ====================
    # list→views 확장 후 view들을 즉시 처리할 때, await로 기다리면 scan 워커가 멈추며 "파도"가 심해진다.
    # 따라서 bulk inline 처리는 백그라운드 태스크로 돌리고 scan 루프는 계속 URL을 소비하도록 한다.
    try:
        _bulk_task_conc = int(os.getenv("SCAN_INLINE_BULK_TASK_CONCURRENCY", "2") or "2")
    except Exception:
        _bulk_task_conc = 2
    _bulk_task_conc = max(1, min(_bulk_task_conc, 10))
    _inline_bulk_sem = asyncio.Semaphore(_bulk_task_conc)
    _inline_bulk_tasks: set[asyncio.Task] = set()

    # 정적 fetch는 같은 호스트로 과도한 동시 요청이 몰리면 ConnectTimeout이 급증한다.
    # 호스트별 동시성 제한을 둬서 과부하를 완화한다.
    try:
        _STATIC_FETCH_HOST_CONCURRENCY = int(os.getenv("STATIC_FETCH_HOST_CONCURRENCY", "4") or "4")
    except Exception:
        _STATIC_FETCH_HOST_CONCURRENCY = 4
    _STATIC_FETCH_HOST_CONCURRENCY = max(1, min(_STATIC_FETCH_HOST_CONCURRENCY, 16))
    _static_host_semaphores: Dict[str, asyncio.Semaphore] = {}

    httpx_client: Optional["httpx.AsyncClient"] = None
    httpx_client_lock = asyncio.Lock()

    async def _get_httpx_client() -> Optional["httpx.AsyncClient"]:
        nonlocal httpx_client
        if not httpx:
            return None
        async with httpx_client_lock:
            if httpx_client and not httpx_client.is_closed:
                return httpx_client
            httpx_client = httpx.AsyncClient(
                headers={"User-Agent": BROWSER_USER_AGENT},
                follow_redirects=True,
            )
            # region agent log
            try:
                _log_path = os.getenv(
                    "AGENT_DEBUG_LOG_PATH",
                    os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
                )
                try:
                    os.makedirs(os.path.dirname(_log_path), exist_ok=True)
                except Exception:
                    pass
                with open(_log_path, "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                        "hypothesisId": "H_httpx_client",
                        "location": "core/crawler/workers/scan.py:_get_httpx_client",
                        "message": "httpx_client_created",
                        "data": {},
                        "timestamp": int(time.time() * 1000),
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # endregion
            return httpx_client

    def _track_task(t: asyncio.Task) -> None:
        _inline_bulk_tasks.add(t)
        def _done(_t: asyncio.Task) -> None:
            _inline_bulk_tasks.discard(_t)
        t.add_done_callback(_done)

    async def _schedule_inline_bulk(view_urls: List[str], depth: int, page_profile: Optional[Dict[str, Any]], source_list_url: str) -> None:
        async with _inline_bulk_sem:
            await _process_views_inline_bulk(view_urls, depth, page_profile, source_list_url=source_list_url)

    async def _fetch_static_html(
        target_url: str,
        *,
        timeout_sec: Optional[float] = None,
        static_only: bool = False,
        referer: Optional[str] = None,
    ) -> str:
        if not requests and not httpx:
            raise RuntimeError("requests/httpx package unavailable")

        # NOTE:
        # - requests(timeout=...)가 일부 환경/상황(DNS/SSL 등)에서 예상보다 오래 블록되는 런타임 증거가 있어
        #   asyncio 레벨에서 총 타임아웃을 한 번 더 강제한다.
        t_default = _STATIC_ONLY_FETCH_TIMEOUT_SEC if static_only else _STATIC_FETCH_TIMEOUT_SEC
        t = float(timeout_sec if timeout_sec is not None else t_default)
        t = max(0.2, min(t, 120.0))

        # connect/read timeout 분리 (requests는 (connect, read) 튜플 지원)
        base_connect = _STATIC_ONLY_FETCH_CONNECT_TIMEOUT_SEC if static_only else _STATIC_FETCH_CONNECT_TIMEOUT_SEC
        base_read = _STATIC_ONLY_FETCH_READ_TIMEOUT_SEC if static_only else _STATIC_FETCH_READ_TIMEOUT_SEC
        connect_t = max(0.2, min(float(base_connect), t))
        read_t = max(0.2, min(float(base_read), t))
        req_timeout = (connect_t, read_t)

        use_httpx = False
        try:
            env_use = str(os.getenv("SCAN_STATIC_USE_HTTPX", "1") or "1").strip().lower()
            use_httpx = (httpx is not None) and env_use not in ("0", "false", "no", "off")
        except Exception:
            use_httpx = httpx is not None
        try:
            ua = str(os.getenv("SCAN_STATIC_UA", "Mozilla/5.0") or "Mozilla/5.0").strip()
        except Exception:
            ua = "Mozilla/5.0"

        max_attempts = _STATIC_ONLY_FETCH_MAX_ATTEMPTS if static_only else _STATIC_FETCH_MAX_ATTEMPTS
        max_attempts = max(1, int(max_attempts))

        loop = asyncio.get_running_loop()

        # 재시도 대상 예외(네트워크 일시 장애/느린 TLS 핸드셰이크 등)
        _retry_exc = ()
        if requests:
            try:
                _retry_exc = (
                    requests.exceptions.ConnectTimeout,  # type: ignore[attr-defined]
                    requests.exceptions.ReadTimeout,     # type: ignore[attr-defined]
                    requests.exceptions.ConnectionError, # type: ignore[attr-defined]
                )
            except Exception:
                _retry_exc = (Exception,)

        # region agent log
        try:
            _log_path = os.getenv(
                "AGENT_DEBUG_LOG_PATH",
                os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
            )
            try:
                os.makedirs(os.path.dirname(_log_path), exist_ok=True)
            except Exception:
                pass
            # pageIndex 추출(성능 병목 확인용)
            try:
                _page_index = dict(parse_qsl(urlparse(target_url).query)).get("pageIndex")
            except Exception:
                _page_index = None
            with open(_log_path, "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                    "hypothesisId": "H1_static_fetch_slow",
                    "location": "core/crawler/workers/scan.py:_fetch_static_html",
                    "message": "static_fetch_start",
                    "data": {
                        "url": str(target_url)[:220],
                        "static_only": static_only,
                        "timeout_sec": t,
                        "connect_t": connect_t,
                        "read_t": read_t,
                        "max_attempts": max_attempts,
                        "pageIndex": _page_index,
                        "use_httpx": use_httpx,
                        "referer": str(referer)[:220] if referer else None,
                    },
                    "timestamp": int(time.time() * 1000),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # endregion

        last_exc: Optional[BaseException] = None
        # 호스트별 동시성 제한
        try:
            _host = urlparse(target_url).netloc or "unknown"
        except Exception:
            _host = "unknown"
        _sem = _static_host_semaphores.get(_host)
        if _sem is None:
            _sem = asyncio.Semaphore(_STATIC_FETCH_HOST_CONCURRENCY)
            _static_host_semaphores[_host] = _sem

        async with _sem:
            for attempt in range(1, max_attempts + 1):
                try:
                    _attempt_t0 = time.perf_counter()
                    if use_httpx:
                        client = await _get_httpx_client()
                        if client is None:
                            raise RuntimeError("httpx client unavailable")
                        headers = {"User-Agent": ua}
                        if referer:
                            headers["Referer"] = referer
                        timeout_cfg = httpx.Timeout(
                            t,
                            connect=connect_t,
                            read=read_t,
                            write=read_t,
                            pool=t,
                        )
                        resp = await client.get(target_url, headers=headers, timeout=timeout_cfg)
                        resp.raise_for_status()
                        html_text = resp.text
                    else:
                        def _request_once() -> str:
                            headers = {"User-Agent": ua}
                            if referer:
                                headers["Referer"] = referer
                            resp = requests.get(
                                target_url,
                                headers=headers,
                                timeout=req_timeout,
                                allow_redirects=True,
                            )
                            resp.raise_for_status()
                            return resp.text

                        # 약간의 여유(스케줄링/컨텍스트 전환)를 더한 총 타임아웃
                        per_attempt_timeout = float(max(connect_t, read_t)) + 0.5
                        html_text = await asyncio.wait_for(
                            loop.run_in_executor(None, _request_once),
                            timeout=per_attempt_timeout,
                        )
                except asyncio.TimeoutError as e:
                    last_exc = e
                except _retry_exc as e:
                    last_exc = e
                except Exception as e:
                    if use_httpx and httpx:
                        if isinstance(e, httpx.TimeoutException) or isinstance(e, httpx.RequestError):
                            last_exc = e
                            continue
                    # 4xx/5xx 등은 재시도로 해결되지 않는 경우가 많아 즉시 상위로 전파
                    raise
                else:
                    # region agent log
                    try:
                        _log_path = os.getenv(
                            "AGENT_DEBUG_LOG_PATH",
                            os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
                        )
                        try:
                            os.makedirs(os.path.dirname(_log_path), exist_ok=True)
                        except Exception:
                            pass
                        with open(_log_path, "a", encoding="utf-8") as _f:
                            _f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                                "hypothesisId": "H1_static_fetch_slow",
                                "location": "core/crawler/workers/scan.py:_fetch_static_html",
                                "message": "static_fetch_ok",
                                "data": {
                                    "url": str(target_url)[:220],
                                    "attempt": attempt,
                                    "elapsed_ms": int((time.perf_counter() - _attempt_t0) * 1000),
                                    "html_size": len(html_text) if html_text is not None else None,
                                },
                                "timestamp": int(time.time() * 1000),
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # endregion
                    return html_text

                if attempt >= max_attempts:
                    break

                # backoff (static_only는 빠른 실패가 목적이므로 attempts 자체를 1로 두는 것을 권장)
                try:
                    backoff = _STATIC_FETCH_RETRY_BACKOFF_SEC * (2 ** (attempt - 1))
                    backoff = min(backoff, _STATIC_FETCH_RETRY_BACKOFF_MAX_SEC)
                except Exception:
                    backoff = 0.35
                if backoff > 0:
                    logger.debug(
                        "[Scan] Static fetch retrying | attempt=%s/%s backoff=%.2fs url=%s err=%s",
                        attempt + 1,
                        max_attempts,
                        backoff,
                        str(target_url)[:180],
                        last_exc,
                    )
                    await asyncio.sleep(backoff)

        if last_exc is None:
            raise RuntimeError("static fetch failed (unknown)")
        # 기존 로그 패턴을 유지하되, 원인을 더 명확히 한다.
        # region agent log
        try:
            _log_path = os.getenv(
                "AGENT_DEBUG_LOG_PATH",
                os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
            )
            try:
                os.makedirs(os.path.dirname(_log_path), exist_ok=True)
            except Exception:
                pass
            with open(_log_path, "a", encoding="utf-8") as _f:
                _f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                    "hypothesisId": "H1_static_fetch_slow",
                    "location": "core/crawler/workers/scan.py:_fetch_static_html",
                    "message": "static_fetch_failed",
                    "data": {
                        "url": str(target_url)[:220],
                        "attempts": max_attempts,
                        "error": str(last_exc)[:200],
                    },
                    "timestamp": int(time.time() * 1000),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # endregion
        raise RuntimeError(f"static fetch failed after {max_attempts} attempt(s): {last_exc}") from last_exc

    async def _process_view_inline(
        view_url: str,
        depth: int,
        page_profile: Optional[Dict[str, Any]],
        *,
        referer: Optional[str] = None,
    ) -> bool:
        """
        list에서 view를 발견한 즉시, 가능하면 정적으로(view URL을 requests로) 처리하여
        등록일/첨부파일을 바로 확인한다.
        - True: 즉시 처리 완료(또는 처리 시도 후 더 이상 enqueue 불필요)
        - False: 즉시 처리 실패/불가 → 기존처럼 in_queue에 enqueue 필요
        """
        if not view_url:
            return True
        # ContextVar로 job_id를 복구(전역 워커풀에서 background task가 job을 섞지 않도록)
        try:
            _ctx = _scan_ctx_var.get()
        except Exception:
            _ctx = {}
        try:
            _jid = str((_ctx or {}).get("job_id") or "default")
        except Exception:
            _jid = "default"
        _visited = visited_by_job.setdefault(_jid, set())

        if view_url in _visited:
            return True
        # outer loop와 동일하게 "방문 처리"로 마킹하여 중복 처리를 막는다.
        _visited.add(view_url)
        try:
            # 정적 처리 우선(빠르고 안정적). 실패 시 bulk 로직이 즉시 동적 fallback(또는 enqueue)을 수행한다.
            # ✅ 중요: list 페이지의 page_profile(static_only 등)을 view inline에도 전달해야
            # - static_only 타임아웃(짧게) 적용
            # - 실패 시 즉시 동적 fallback 정책이 일관되게 동작한다.
            try:
                pp = dict(page_profile) if isinstance(page_profile, dict) else {}
            except Exception:
                pp = {}
            pp["is_dynamic"] = False
            handled = await _process_static_page(view_url, depth, pp, referer=referer)
            return bool(handled)
        except Exception:
            return False

    async def _process_views_inline_bulk(
        view_urls: List[str],
        depth: int,
        page_profile: Optional[Dict[str, Any]],
        *,
        source_list_url: str,
    ) -> Dict[str, int]:
        """
        view URL들을 즉시(정적) 처리하여 파일 링크 수집을 앞당긴다.
        - 정적 처리 성공: 큐에 넣지 않음
        - 정적 처리 실패: 기존처럼 in_queue에 enqueue (static_only면 이후 static_failed→동적 phase로 넘어감)
        """
        if not view_urls:
            return {"total": 0, "processed_inline": 0, "enqueued": 0, "skipped": 0}
        # ContextVar 기반 job 컨텍스트 복구 (background task 안전)
        try:
            _ctx = _scan_ctx_var.get()
        except Exception:
            _ctx = {}
        try:
            _jid = str((_ctx or {}).get("job_id") or "default")
        except Exception:
            _jid = "default"
        _visited = visited_by_job.setdefault(_jid, set())
        _chat_bot_id = (_ctx or {}).get("chat_bot_id")
        _db_name = (_ctx or {}).get("db_name")
        _start_date = (_ctx or {}).get("start_date")
        _end_date = (_ctx or {}).get("end_date")

        # 동시성 제한 (너무 높이면 requests/CPU/대상서버에 부담)
        try:
            conc = int(os.getenv("SCAN_INLINE_VIEW_PROCESSING_CONCURRENCY", "6") or "6")
        except Exception:
            conc = 6
        conc = max(1, min(conc, 30))
        sem = asyncio.Semaphore(conc)

        # "views 발견 시 즉시 처리" 옵션
        # 너무 큰 리스트는 즉시 처리 상한을 둔다.
        max_n = len(view_urls)
        if INLINE_VIEW_PROCESSING_MAX > 0:
            max_n = min(max_n, INLINE_VIEW_PROCESSING_MAX)

        processed_inline = 0
        enqueued = 0
        skipped = 0
        static_only = isinstance(page_profile, dict) and page_profile.get("static_only")

        async def _one(vurl: str) -> bool:
            async with sem:
                return await _process_view_inline(vurl, depth, page_profile, referer=source_list_url)

        # 상위 max_n개는 즉시 처리 시도 (chunking으로 과도한 gather 부담 완화)
        to_process = view_urls[:max_n]
        try:
            chunk_size = int(os.getenv("SCAN_INLINE_VIEW_CHUNK_SIZE", "50") or "50")
        except Exception:
            chunk_size = 50
        chunk_size = max(1, min(chunk_size, 500))

        for start in range(0, len(to_process), chunk_size):
            chunk = to_process[start:start + chunk_size]
            results = await asyncio.gather(*[_one(v) for v in chunk], return_exceptions=True)

            # 처리 결과 반영 + 실패분 enqueue
            for idx_v, vurl in enumerate(chunk):
                if vurl in _visited:
                    # _process_view_inline이 visited로 마킹함. 여기선 중복 enqueue 방지용 통계만.
                    pass
                ok = results[idx_v]
                if isinstance(ok, Exception):
                    ok = False
                if ok:
                    processed_inline += 1
                    continue
                if static_only:
                    # ✅ 요청 반영: 상세(view) 페이지는 정적 실패 시 "다음 phase"로 미루지 않고 즉시 동적으로 fallback
                    # - _process_view_inline에서 visited_urls에 먼저 add하므로, 동적 enqueue 전에 discard 필요
                    try:
                        _visited.discard(vurl)
                    except Exception:
                        pass
                    try:
                        progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                    except Exception:
                        pass
                    await in_queue.put({'url': vurl, 'depth': depth, 'page_profile': {'is_dynamic': True}, 'job_id': _jid, 'chat_bot_id': _chat_bot_id, 'db_name': _db_name, 'start_date': _start_date, 'end_date': _end_date, 'referer': source_list_url})
                    enqueued += 1
                    continue
                if vurl in _visited:
                    # 이미 처리/시도된 URL은 중복 enqueue하지 않음
                    skipped += 1
                    continue
                try:
                    progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                except Exception:
                    pass
                await in_queue.put({'url': vurl, 'depth': depth, 'page_profile': page_profile, 'job_id': _jid, 'chat_bot_id': _chat_bot_id, 'db_name': _db_name, 'start_date': _start_date, 'end_date': _end_date, 'referer': source_list_url})
                enqueued += 1

        # 나머지는 기존처럼 enqueue (즉시 처리 상한을 둔 경우)
        for vurl in view_urls[max_n:]:
            if vurl in _visited:
                skipped += 1
                continue
            if static_only:
                # ✅ 요청 반영: 즉시 동적으로 fallback
                try:
                    progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                except Exception:
                    pass
                await in_queue.put({'url': vurl, 'depth': depth, 'page_profile': {'is_dynamic': True}, 'job_id': _jid, 'chat_bot_id': _chat_bot_id, 'db_name': _db_name, 'start_date': _start_date, 'end_date': _end_date, 'referer': source_list_url})
                enqueued += 1
                continue
            try:
                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
            except Exception:
                pass
            await in_queue.put({'url': vurl, 'depth': depth, 'page_profile': page_profile, 'job_id': _jid, 'chat_bot_id': _chat_bot_id, 'db_name': _db_name, 'start_date': _start_date, 'end_date': _end_date, 'referer': source_list_url})
            enqueued += 1

        logger.info(
            "[Scan] Inline view processing finished | list=%s total=%s inline_ok=%s enqueued=%s skipped=%s",
            str(source_list_url)[:120],
            len(view_urls),
            processed_inline,
            enqueued,
            skipped,
        )
        return {
            "total": len(view_urls),
            "processed_inline": processed_inline,
            "enqueued": enqueued,
            "skipped": skipped,
        }

    async def _expand_list_to_views(
        list_url: str,
        base_url: str,
        *,
        depth: Optional[int] = None,
        page_profile: Optional[Dict[str, Any]] = None,
        source_list_url: Optional[str] = None,
    ) -> List[str]:
        """
        메뉴/컨텐츠 페이지(source)에서 게시판 목록(list.do) 링크를 발견했을 때:
        - list 자체는 기간(reg_date)을 못 뽑는 경우가 많다.
        - 대신 list 페이지 HTML을 1회 조회하여 view.do(상세글) 링크를 추출하고,
          상세(view)에서 등록일/작성자/첨부파일을 수집한다.
        """
        if not list_url:
            return []
        try:
            abs_list = urljoin(base_url, list_url)
        except Exception:
            abs_list = list_url
        # pagination을 따라갈 때도 같은 게시판(list)의 캐시를 재사용할 수 있도록
        # pageIndex/pageNo/page 등의 페이징 파라미터는 캐시 키에서 제거한다.
        def _normalize_list_cache_key(u: str) -> str:
            try:
                p = urlparse(u)
                pairs = parse_qsl(p.query or "", keep_blank_values=True)
                filtered = [(k, v) for (k, v) in pairs if k.lower() not in ("pageindex", "pageno", "page", "curpage", "page_no", "page_index")]
                filtered.sort()
                q = urlencode(filtered, doseq=True)
                scheme = (p.scheme or "https").lower()
                netloc = (p.netloc or "").lower()
                if netloc.startswith("www."):
                    netloc = netloc[4:]
                return urlunparse((scheme, netloc, p.path or "", "", q, ""))
            except Exception:
                return u

        cache_key = _normalize_list_cache_key(abs_list)
        # cache (job_id별 분리)
        try:
            _ctx = _scan_ctx_var.get()
            _jid = str((_ctx or {}).get("job_id") or "default")
        except Exception:
            _jid = "default"
        job_cache = _list_view_cache_by_job.setdefault(_jid, {})
        cached = job_cache.get(cache_key)
        if cached is not None:
            return cached

        # BeautifulSoup 없으면 list를 직접 follow하는 방식으로 fallback(여기서는 view 추출 불가)
        if not BeautifulSoup:
            job_cache[cache_key] = []
            return []

        # NOTE:
        # 사용자 요청에 따라 list 페이지네이션의 "최대 페이지/최대 view 수" 상한을 제거한다.
        # 종료 조건은 다음으로 충분하다:
        # - pages_seen(중복 방지) + pages_to_visit(더 이상 신규 페이지가 없으면 종료)

        def _canonicalize_list_page_url(u: str) -> str:
            # list 페이지 방문 중복 방지를 위한 canonical 형태(쿼리 정렬/fragment 제거)
            try:
                p = urlparse(u)
                pairs = parse_qsl(p.query or "", keep_blank_values=True)
                pairs.sort()
                q = urlencode(pairs, doseq=True)
                scheme = (p.scheme or "https").lower()
                netloc = (p.netloc or "").lower()
                if netloc.startswith("www."):
                    netloc = netloc[4:]
                return urlunparse((scheme, netloc, p.path or "", "", q, ""))
            except Exception:
                return u

        def _build_page_url(base_page_url: str, page_no: int, param_name: str) -> str:
            try:
                p = urlparse(base_page_url)
                pairs = parse_qsl(p.query or "", keep_blank_values=True)
                qd: Dict[str, str] = {}
                for k, v in pairs:
                    qd[str(k)] = str(v)
                qd[param_name] = str(page_no)
                # 정렬하여 canonical하게 구성
                new_pairs = sorted(qd.items(), key=lambda x: x[0])
                q = urlencode(new_pairs, doseq=True)
                return urlunparse((p.scheme or "https", p.netloc, p.path, "", q, ""))
            except Exception:
                return base_page_url

        def _extract_page_no(page_url: str) -> Optional[int]:
            try:
                pairs = parse_qsl(urlparse(page_url).query or "", keep_blank_values=True)
            except Exception:
                return None
            for k, v in pairs:
                if str(k).lower() in ("pageindex", "pageno", "page", "curpage", "page_no", "page_index"):
                    try:
                        return int(str(v).strip())
                    except Exception:
                        return None
            return None

        def _hash_view_links(links: List[str]) -> str:
            if not links:
                return ""
            try:
                payload = "\n".join(sorted(set(links)))
            except Exception:
                payload = "\n".join(links)
            return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()

        # 어떤 페이징 파라미터를 쓸지(가능하면 기존 URL/페이지 히든 input 기반으로 추정)
        def _guess_page_param(page_url: str, soup_obj) -> str:
            try:
                qkeys = {k.lower() for (k, _v) in parse_qsl(urlparse(page_url).query or "", keep_blank_values=True)}
            except Exception:
                qkeys = set()
            for cand in ("pageindex", "pageno", "page", "curpage"):
                if cand in qkeys:
                    # 원래 대소문자를 유지할 필요는 없으므로 대표값으로 매핑
                    return "pageIndex" if cand == "pageindex" else ("pageNo" if cand == "pageno" else ("page" if cand == "page" else "curPage"))
            # hidden input에 pageIndex/pageNo가 있는지 확인
            try:
                for nm in ("pageIndex", "pageNo", "page", "curPage"):
                    tag = soup_obj.find("input", attrs={"name": nm})
                    if tag is not None:
                        return nm
            except Exception:
                pass
            # default: egov 프레임워크 계열에서 흔한 pageIndex
            return "pageIndex"

        views: List[str] = []
        view_seen: set[str] = set()
        pages_seen: set[str] = set()
        page_signatures: set[str] = set()
        pages_lock = asyncio.Lock()
        view_lock = asyncio.Lock()
        page_queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        await page_queue.put(abs_list)

        try:
            list_conc = int(os.getenv("SCAN_LIST_PAGINATION_CONCURRENCY", "3") or "3")
        except Exception:
            list_conc = 3
        list_conc = max(1, min(list_conc, 10))
        try:
            max_pages = int(os.getenv("SCAN_LIST_MAX_PAGES", "200") or "200")
        except Exception:
            max_pages = 200
        max_pages = max(0, min(max_pages, 5000))
        stop_pagination = False

        async def _list_worker() -> None:
            nonlocal stop_pagination
            while True:
                page_url = await page_queue.get()
                try:
                    if page_url is None:
                        return
                    if stop_pagination:
                        continue
                    canon_page = _canonicalize_list_page_url(page_url)
                    async with pages_lock:
                        if canon_page in pages_seen:
                            continue
                        pages_seen.add(canon_page)
                        if max_pages > 0 and len(pages_seen) > max_pages:
                            stop_pagination = True
                            logger.info(
                                "[Scan] Stop pagination due to max pages | max=%s list=%s",
                                max_pages,
                                str(abs_list)[:160],
                            )
                            continue

                    try:
                        html = await _fetch_static_html(page_url)
                    except Exception:
                        continue
                    if _is_invalid_page_html(html):
                        logger.info(
                            "[Scan] Skip invalid page content | url=%s",
                            str(page_url)[:160],
                        )
                        continue

                    try:
                        soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
                    except Exception:
                        continue

                    # 1) view 링크 추출
                    new_views: List[str] = []
                    page_view_links: List[str] = []
                    page_view_seen: set[str] = set()
                    file_mode = _is_file_mode(page_profile)
                    try:
                        file_filter_on = str(os.getenv("SCAN_FILE_LIST_ATTACH_FILTER", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
                    except Exception:
                        file_filter_on = True
                    use_attach_filter = file_mode and file_filter_on
                    candidate_all: List[str] = []
                    candidate_with_attach: List[str] = []
                    candidate_seen: set[str] = set()
                    try:
                        for a in soup.find_all("a", href=True):
                            href = (a.get("href") or "").strip()
                            if not href or href.startswith("#"):
                                continue
                            lh = href.lower()
                            # javascript:는 아래 JS 페이징에서 처리
                            if lh.startswith("javascript:"):
                                continue
                            # 광진구청 계열: view.do + nttId
                            if ("view.do" in lh or "detail.do" in lh or "read.do" in lh) and ("nttid=" in lh or "num=" in lh):
                                full = ensure_songpa_www_path(urljoin(page_url, href))
                                if full not in page_view_seen:
                                    page_view_seen.add(full)
                                    page_view_links.append(full)
                                # 비공개 글로 추정되는 경우는 접근하지 않음
                                if _is_private_url(full) or _is_private_list_item(a):
                                    logger.debug(
                                        "[Scan] Skip private view link (list) | list=%s view=%s",
                                        str(page_url)[:120],
                                        str(full)[:180],
                                    )
                                    continue
                                if use_attach_filter:
                                    if full in candidate_seen:
                                        continue
                                    candidate_seen.add(full)
                                    candidate_all.append(full)
                                    try:
                                        if _row_has_attachment(a):
                                            candidate_with_attach.append(full)
                                    except Exception:
                                        pass
                                else:
                                    async with view_lock:
                                        if full in view_seen:
                                            continue
                                        view_seen.add(full)
                                        views.append(full)
                                    new_views.append(full)
                    except Exception:
                        pass
                    if use_attach_filter and candidate_all:
                        selected = candidate_with_attach if candidate_with_attach else candidate_all
                        async with view_lock:
                            for full in selected:
                                if full in view_seen:
                                    continue
                                view_seen.add(full)
                                views.append(full)
                                new_views.append(full)

                    # 발견되는 대로 상세페이지를 병행 처리(백그라운드)
                    if new_views and INLINE_VIEW_PROCESSING and depth is not None:
                        try:
                            t = asyncio.create_task(
                                _schedule_inline_bulk(new_views, depth, page_profile, source_list_url or list_url)
                            )
                            _track_task(t)
                        except Exception:
                            pass

                    # 2) pagination 링크 추출 (href로 직접 페이지 이동하는 케이스)
                    found_pagination_link = False
                    try:
                        for a in soup.find_all("a", href=True):
                            href = (a.get("href") or "").strip()
                            if not href or href.startswith("#"):
                                continue
                            lh = href.lower()
                            if lh.startswith("javascript:"):
                                continue
                            # page param 포함 + 같은 보드(캐시 키 동일)일 때만 follow
                            if any(k in lh for k in ("pageindex=", "pageno=", "page=", "curpage=", "page_no=", "page_index=")):
                                full = ensure_songpa_www_path(urljoin(page_url, href))
                                if _normalize_list_cache_key(full) != cache_key:
                                    continue
                                found_pagination_link = True
                                if stop_pagination:
                                    continue
                                await page_queue.put(full)
                    except Exception:
                        pass

                    # 3) JS 기반 페이징(egov 등): javascript:fn_egov_link_page('2') 또는 onclick="fn_egov_link_page('2')"
                    js_pages: set[int] = set()
                    try:
                        for tag in soup.find_all(["a", "button", "span"]):
                            for attr in ("href", "onclick"):
                                v = (tag.get(attr) or "").strip()
                                if not v:
                                    continue
                                m = re.search(
                                    r"(?:fn_egov_link_page|link_page|goPage|go_page|movePage|fncPage|fn_go_page|fn_goPage)\s*"
                                    r"\(\s*['\"]?(\d+)['\"]?\s*\)",
                                    v,
                                    re.IGNORECASE,
                                )
                                if m:
                                    try:
                                        js_pages.add(int(m.group(1)))
                                    except Exception:
                                        pass
                                # JS/링크 문자열에 직접 page 파라미터가 있는 경우도 수집
                                m2 = re.search(r"(?:pageindex|pageno|page|curpage|page_no|page_index)\s*=\s*['\"]?(\d+)['\"]?", v, re.IGNORECASE)
                                if m2:
                                    try:
                                        js_pages.add(int(m2.group(1)))
                                    except Exception:
                                        pass
                    except Exception:
                        pass

                    if js_pages:
                        try:
                            page_param = _guess_page_param(page_url, soup)
                        except Exception:
                            page_param = "pageIndex"
                        for pn in sorted(js_pages):
                            if pn <= 0:
                                continue
                            full = _build_page_url(page_url, pn, page_param)
                            if _normalize_list_cache_key(full) != cache_key:
                                continue
                            found_pagination_link = True
                            if stop_pagination:
                                continue
                            await page_queue.put(full)
                        logger.warning(
                            "[Scan][Pagination] JS pages queued | list=%s count=%s param=%s pages=%s",
                            str(page_url)[:160],
                            len(js_pages),
                            page_param,
                            ",".join(str(p) for p in sorted(js_pages))[:200],
                        )

                    # 4) pagination 링크가 없을 때, pageIndex 계열 파라미터를 1부터 증가시키며 시도
                    if not found_pagination_link and not js_pages and not stop_pagination:
                        page_signature = _hash_view_links(page_view_links)
                        if page_signature:
                            if page_signature in page_signatures:
                                # 같은 페이지 내용이 반복되면 종료
                                stop_pagination = True
                            else:
                                page_signatures.add(page_signature)
                        # 내용이 있는 페이지에서만 다음 페이지를 시도
                        if not stop_pagination and page_view_links:
                            try:
                                page_param = _guess_page_param(page_url, soup)
                            except Exception:
                                page_param = "pageIndex"
                            cur_no = _extract_page_no(page_url) or 1
                            next_no = cur_no + 1
                            full = _build_page_url(page_url, next_no, page_param)
                            if _normalize_list_cache_key(full) == cache_key:
                                await page_queue.put(full)
                                logger.warning(
                                    "[Scan][Pagination] Auto next queued | list=%s cur=%s next=%s param=%s views=%s",
                                    str(page_url)[:160],
                                    cur_no,
                                    next_no,
                                    page_param,
                                    len(page_view_links),
                                )
                            else:
                                logger.warning(
                                    "[Scan][Pagination] Auto next skipped (cache key mismatch) | list=%s next=%s",
                                    str(page_url)[:160],
                                    str(full)[:160],
                                )
                        elif stop_pagination:
                            logger.warning(
                                "[Scan][Pagination] Auto pagination stopped (duplicate page signature) | list=%s",
                                str(page_url)[:160],
                            )
                        elif not page_view_links:
                            logger.warning(
                                "[Scan][Pagination] Auto pagination skipped (no views) | list=%s",
                                str(page_url)[:160],
                            )
                finally:
                    page_queue.task_done()

        workers = [asyncio.create_task(_list_worker()) for _ in range(list_conc)]
        await page_queue.join()
        for _ in workers:
            await page_queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)

        # dedupe (순서 유지)
        uniq: List[str] = []
        seen: set[str] = set()
        for v in views:
            if v in seen:
                continue
            seen.add(v)
            uniq.append(v)
        job_cache[cache_key] = uniq
        return uniq

    async def _preflight_request(target_url: str) -> str:
        if not target_url or not requests:
            return target_url

        # preflight timeout은 환경변수로 조정 가능 (기본 6초)
        try:
            _preflight_timeout = float(os.getenv("SCAN_PREFLIGHT_TIMEOUT_SEC", "6") or "6")
        except Exception:
            _preflight_timeout = 6.0
        _preflight_timeout = max(1.0, min(_preflight_timeout, 30.0))

        def _request():
            resp = requests.get(
                target_url,
                headers=STATIC_HEADERS,
                timeout=_preflight_timeout,
                allow_redirects=True,
            )
            resp.raise_for_status()
            return resp.url

        loop = asyncio.get_running_loop()
        try:
            final_url = await loop.run_in_executor(None, _request)
            if final_url and final_url != target_url:
                logger.debug("[Scan] Preflight redirect detected: %s -> %s", target_url, final_url)
            return final_url or target_url
        except Exception as exc:
            logger.debug("[Scan] Preflight request failed for %s: %s", target_url, exc)
            return target_url
    async def _enqueue_file_candidate(
        file_url: str,
        source_url: str,
        job_id: Optional[str] = None,
        display_name: Optional[str] = None,
        reg_date: Optional[str] = None,
        author: Optional[str] = None,
        department: Optional[str] = None,
        author_kind: Optional[str] = None,
        author_raw: Optional[str] = None,
        department_raw: Optional[str] = None,
    ):
        """
        파일 URL 후보를 선별(Collection) 단계로 전달한다.

        정책(요구사항):
        - 게시판 내용 수집 크롤링에서는 선별을 건너뛰지 않는다.
        - 중복 체크는 선별(Collection) 단계에서만 수행한다.
        """
        if not file_url:
            return
        # job 컨텍스트를 안정적으로 결정 (전역 워커풀 + background task 대응)
        eff_job_id = job_id
        try:
            if not eff_job_id:
                _ctx = _scan_ctx_var.get()
                eff_job_id = str((_ctx or {}).get("job_id") or "default")
        except Exception:
            eff_job_id = eff_job_id or "default"
        eff_job_id = str(eff_job_id or "default")

        # 컨텍스트 기반 메타(날짜필터/저장경로 메타) 보정
        eff_start_date = start_date
        eff_end_date = end_date
        eff_chat_bot_id = chat_bot_id
        eff_db_name = db_name
        try:
            _ctx = _scan_ctx_var.get()
            if _ctx:
                eff_start_date = _ctx.get("start_date", eff_start_date)
                eff_end_date = _ctx.get("end_date", eff_end_date)
                eff_chat_bot_id = _ctx.get("chat_bot_id", eff_chat_bot_id)
                eff_db_name = _ctx.get("db_name", eff_db_name)
        except Exception:
            pass

        # 0. 즉석 필터링 (용님 요청: 제외 패턴 즉시 적용)
        lowered_url = file_url.lower()
        from config.constants import EXCLUDE_URL_PATTERNS, SKIP_DEPTH_PATTERNS

        # 제외 패턴(로그인 등) 또는 스킵 패턴(contents 등) 포함 시 즉시 중단
        if any(p in lowered_url for p in EXCLUDE_URL_PATTERNS) or any(p in lowered_url for p in SKIP_DEPTH_PATTERNS):
            # logger.debug("[Scan] File candidate rejected (exclude/skip pattern) | url=%s source=%s", file_url, source_url)
            return

        # ✅ 기간 필터(게시판과 동일): 게시글 등록일(reg_date) 기준으로 탐색 단계에서 필터링
        # - 기간 필터가 켜져 있고 reg_date가 없으면 스킵 (scan 카운트도 올리지 않음)
        # - reg_date가 있으면 파싱 후 범위 통과 시에만 scan 이벤트 발행
        if eff_start_date or eff_end_date:
            if not reg_date:
                logger.info(
                    "[Scan] 📅 File candidate skipped (missing post reg_date under date filter) | url=%s source=%s",
                    file_url,
                    str(source_url)[:200] if source_url else "",
                )
                return
            try:
                reg_dt = None
                if isinstance(reg_date, datetime):
                    reg_dt = reg_date
                else:
                    try:
                        reg_dt = extract_post_date(str(reg_date))
                    except Exception:
                        reg_dt = None
                if reg_dt is None:
                    try:
                        reg_dt = extract_date_from_text(str(reg_date))
                    except Exception:
                        reg_dt = None
                if not is_date_in_range(reg_dt, eff_start_date, eff_end_date):
                    logger.info(
                        "[Scan] 📅 File candidate skipped (out of range) | url=%s reg_date=%s source=%s",
                        file_url,
                        str(reg_date)[:64],
                        str(source_url)[:200] if source_url else "",
                    )
                    return
            except Exception:
                # 파싱 실패는 기간 필터 활성 시 스킵 (안전 우선)
                logger.info(
                    "[Scan] 📅 File candidate skipped (reg_date parse failed) | url=%s reg_date=%s source=%s",
                    file_url,
                    str(reg_date)[:64],
                    str(source_url)[:200] if source_url else "",
                )
                return

        # ✅ 선별 단일화:
        # - Scan 단계에서는 파일 URL 후보를 그대로 선별 큐(scan_batch_queue)로 넘긴다.
        # - canonicalize / in-memory dedupe / visited(file_url) 차단은 하지 않는다.
        # - 중복 여부는 Collection 단계에서 MariaDB(_LEARN_LIST.url) 기준으로만 판정한다.
        # - 탐색(scan) 카운트는 "기간 통과(=reg_date 존재)" 후보에서만 증가한다.
        try:
            await progress_queue.put({"type": "scan", "count": 1, "items": [file_url], "job_id": eff_job_id})
        except Exception:
            pass
        payload: Dict[str, Any] = {
            "url": file_url,
            "source_page": source_url,
            "type": "file",
            "reg_date": reg_date,
            "name": display_name,
            "chat_bot_id": eff_chat_bot_id,
            "db_name": eff_db_name,
            "job_id": eff_job_id,
            "start_date": eff_start_date,
            "end_date": eff_end_date,
        }
        if author:
            payload["author"] = author
            logger.debug(f"[Scan] _enqueue_file_candidate: Author 포함 | file_url={file_url[:100]} author={author!r} source_url={source_url[:100]}")
        if department:
            payload["department"] = department
        if author_kind:
            payload["author_kind"] = author_kind
        if author_raw:
            payload["author_raw"] = author_raw
        if department_raw:
            payload["department_raw"] = department_raw

        # ✅ 중요: reg_date가 있어도 author/department는 source_page(view.do)에만 있는 케이스가 많다.
        # author가 없으면 source_url이 상세페이지로 보일 때 HTML을 1회만 조회해 author/department를 보강한다.
        if (not payload.get("author")) and source_url:
            try:
                s = str(source_url).strip()
            except Exception:
                s = ""
            if s and is_detail_page_url(s):
                try:
                    cached_author = source_author_cache.get(s, "__MISS__")
                    cached_dept = source_department_cache.get(s, "__MISS__")
                    cached_kind = source_author_kind_cache.get(s, "__MISS__")
                    cached_author_raw = source_author_raw_cache.get(s, "__MISS__")
                    cached_dept_raw = source_department_raw_cache.get(s, "__MISS__")
                    if (
                        cached_author != "__MISS__"
                        or cached_dept != "__MISS__"
                        or cached_kind != "__MISS__"
                        or cached_author_raw != "__MISS__"
                        or cached_dept_raw != "__MISS__"
                    ):
                        if cached_author:
                            payload["author"] = cached_author
                        if cached_dept:
                            payload["department"] = cached_dept
                        if cached_kind:
                            payload["author_kind"] = cached_kind
                        if cached_author_raw:
                            payload["author_raw"] = cached_author_raw
                        if cached_dept_raw:
                            payload["department_raw"] = cached_dept_raw
                    else:
                        html = await _fetch_static_html(s)
                        try:
                            info = extract_author_info_from_html(html, url=s)
                        except Exception:
                            info = {"author": None, "department": None, "author_kind": None}
                        author_val = info.get("author")
                        dept_val = info.get("department")
                        kind_val = info.get("author_kind")
                        author_raw_val = info.get("author_raw")
                        dept_raw_val = info.get("department_raw")
                        source_author_cache[s] = author_val
                        source_department_cache[s] = dept_val
                        source_author_kind_cache[s] = kind_val
                        source_author_raw_cache[s] = author_raw_val
                        source_department_raw_cache[s] = dept_raw_val
                        if author_val:
                            payload["author"] = author_val
                        if dept_val:
                            payload["department"] = dept_val
                        if kind_val:
                            payload["author_kind"] = kind_val
                        if author_raw_val:
                            payload["author_raw"] = author_raw_val
                        if dept_raw_val:
                            payload["department_raw"] = dept_raw_val
                        if author_val or dept_val:
                            logger.info(
                                "[Scan] Author/Dept 보강 성공 (source_page 캐시) | source=%s author=%r dept=%r kind=%r",
                                s[:120],
                                author_val,
                                dept_val,
                                kind_val,
                            )
                        # original_meta에 HTML 및 브레드크럼(web_title)을 보관하여
                        # 이후 파일 저장/후처리 단계에서 재사용할 수 있도록 한다.
                        try:
                            if html:
                                om = payload.get("original_meta") if isinstance(payload.get("original_meta"), dict) else {}
                                # HTML 원문 저장(key 호환성: html / response_text / page_html)
                                om.setdefault("html", html)
                                om.setdefault("response_text", html)
                                om.setdefault("page_html", html)
                                # breadcrumb 최하단을 web_title로 추출하여 저장 (파일 전용 로직)
                                try:
                                    from backend.file.file_breadcrumb import extract_file_web_title_from_html

                                    bc = extract_file_web_title_from_html(html or "")
                                    print(f"=================== [test] bc ===================")
                                    print(f"bc: {bc}")
                                except Exception:
                                    bc = ""
                                if bc:
                                    om.setdefault("web_title", bc)
                                payload["original_meta"] = om
                        except Exception:
                            pass
                except Exception as e:
                    logger.debug("[Scan] Author 보강 실패 (source_page) | source=%s err=%s", str(source_url)[:120], e)

        # ✅ 선별(Collection) 큐로 전달 (scan_batch_queue -> collection_worker)
        try:
            progress_queue.put_nowait({"type": "in_flight", "stage": "collection", "delta": 1})
        except Exception:
            pass
        # 파일/수집 후보를 큐에 넣기 전에 발견 URL 정보를 파일로 기록 (best-effort)
        try:
            from backend.shared.crawl_shared import write_scan_log
            write_scan_log(
                job_id,
                {
                    "job_id": job_id,
                    "url": payload.get("url"),
                    "url_key": payload.get("url_key"),
                    "note": "queued_payload",
                },
            )
        except Exception:
            pass
        await scan_batch_queue.put(payload)
        # BatchQueue 지연 완화(안전장치)
        try:
            await scan_batch_queue.flush()
        except Exception:
            pass
        logger.info(
            "[Scan] File candidate queued → collection | url=%s reg_date=%s source=%s",
            file_url,
            reg_date,
            source_url,
        )

    async def _process_static_page(
        url: str,
        depth: int,
        page_profile: Optional[Dict[str, Any]],
        *,
        referer: Optional[str] = None,
    ):
        # 전역 워커풀에서 background task가 실행되는 경우에도 job 컨텍스트를 보존하기 위해
        # ContextVar 값을 우선으로 사용한다.
        try:
            # fragment(#...)는 서버 요청에 영향이 없고, 중복/지연만 유발하므로 제거
            if url:
                _p = urlparse(url)
                if _p.fragment:
                    url = urlunparse(_p._replace(fragment=""))
            _ctx = _scan_ctx_var.get()
        except Exception:
            _ctx = {}
        try:
            _jid = str((_ctx or {}).get("job_id") or "default")
        except Exception:
            _jid = "default"
        # 아래 로직은 함수 지역 변수(start_date/end_date/chat_bot_id/db_name/visited_urls)를 참조하므로,
        # 여기서 지역 변수로 덮어써서 "현재 처리 중인 job" 기준으로 동작하게 만든다.
        # ⚠️ 주의: start_date = (..., start_date) 형태는 파이썬 스코프 규칙상 UnboundLocalError를 유발할 수 있으므로
        # 기본값은 outer start_date가 아니라 _default_*를 사용한다.
        start_date = (_ctx or {}).get("start_date") or _default_start_date
        end_date = (_ctx or {}).get("end_date") or _default_end_date
        chat_bot_id = (_ctx or {}).get("chat_bot_id") or _default_chat_bot_id
        db_name = (_ctx or {}).get("db_name") or _default_db_name
        visited_urls = visited_by_job.setdefault(_jid, set())

        if _is_private_url(url):
            logger.info("[Scan] 🔒 Static skip (private url) | url=%s", url[:200])
            return True

        if not BeautifulSoup:
            logger.warning("[Scan] BeautifulSoup not available; fallback to dynamic handling for %s", url)
            return False
        try:
            # static_only(정적 우선 phase)는 느린 URL을 오래 붙잡으면 전체가 느려진다.
            # 따라서 더 짧은 타임아웃으로 빠르게 실패시키고, 동적 phase로 넘긴다.
            is_static_only = bool(isinstance(page_profile, dict) and page_profile.get("static_only"))
            timeout = _STATIC_ONLY_FETCH_TIMEOUT_SEC if is_static_only else None
            html = await _fetch_static_html(url, timeout_sec=timeout, static_only=is_static_only, referer=referer)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Scan] Static fetch failed for %s: %s", url, exc)
            return False

        # === 기간 게이트(정적 상세페이지) ===
        # 요구사항:
        # - 기간 기준은 "게시물 상세페이지의 등록일(작성일/게시일)"이다.
        # - 등록일이 기간 밖이면: 파일 링크(첨부) 수집을 하지 않는다. (동적 폴백도 타지 않게 '처리 완료'로 종료)
        extracted_post_date_static = None
        extracted_post_date_str_static: Optional[str] = None
        # 예외로 try 블록이 중간에 깨져도 아래에서 참조될 수 있으므로 기본값을 먼저 둔다.
        is_post_detail_page_static = False

        # 페이지(게시물) 단위 메타(작성자/부서)는 파일 링크마다 반복 추출하지 않고 1회만 시도한다.
        # ✅ 중요: 작성자/부서는 "상세(게시글) 페이지"에서만 추출한다.
        # - menu/contents 페이지의 하단(문의/전화) 영역을 author로 오탐할 수 있으므로,
        #   상세 페이지 판별 후에만 meta 추출을 수행한다.
        page_author: Optional[str] = None
        page_department: Optional[str] = None
        page_author_kind: Optional[str] = None
        page_author_raw: Optional[str] = None
        page_department_raw: Optional[str] = None
        try:
            # 상세 판별(정적): URL 기반 + HTML 점수
            is_list_page_static = _is_list_page_url(url)
            if not is_list_page_static:
                try:
                    if is_detail_page_url(url):
                        is_post_detail_page_static = True
                    else:
                        lowered_url = str(url).lower()
                        # ✅ 과탐 방지:
                        # contents.do(메뉴/컨텐츠) 류는 '?'가 붙는 경우가 많아 HTML 점수 기반 상세 판별이 과하게 True가 될 수 있다.
                        # 이 경우 "상세로 오판 → H 로그 폭발 → F로 동적 폴백"이 발생해 속도가 크게 저하된다.
                        is_contents_like = ("contents.do" in lowered_url) or ("/main/contents" in lowered_url)
                        has_board_hint = (
                            any(p in lowered_url for p in BOARD_PATTERNS)
                            or any(h in lowered_url for h in ["view.do", "detail.do", "read.do", "nttid=", "brdview.do", "brddetail.do"])
                        )
                        if is_contents_like and not has_board_hint:
                            is_post_detail_page_static = False
                        elif ("?" in lowered_url) or any(p in lowered_url for p in BOARD_PATTERNS):
                            is_post_detail_page_static = is_post_detail_page_from_html(html, url)
                except Exception:
                    is_post_detail_page_static = any(h in str(url).lower() for h in ["view.do", "detail.do", "read.do", "nttid=", "num="])

            if is_post_detail_page_static:
                # 비공개 글은 접근/추출하지 않음
                if _is_private_html(html):
                    # region agent log
                    try:
                        _log_path = os.getenv(
                            "AGENT_DEBUG_LOG_PATH",
                            os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
                        )
                        try:
                            os.makedirs(os.path.dirname(_log_path), exist_ok=True)
                        except Exception:
                            pass
                        with open(_log_path, "a", encoding="utf-8") as _f:
                            _f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                                "hypothesisId": "H_private_detected",
                                "location": "core/crawler/workers/scan.py:static_detail",
                                "message": "private_detected",
                                "data": {
                                    "url": str(url)[:220],
                                    "reason": _private_reason(html),
                                },
                                "timestamp": int(time.time() * 1000),
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # endregion
                    logger.info("[Scan] 🔒 Static detail skipped (private post) | url=%s", url[:200])
                    return True
                # 1) 게시물 등록일 추출 (기간 필터용 기준값)
                try:
                    extracted_post_date_static = extract_post_date(html, url, raw_response_text=html)
                    extracted_post_date_str_static = (
                        extracted_post_date_static.strftime('%Y-%m-%d') if extracted_post_date_static else None
                    )
                except Exception as e:
                    extracted_post_date_static = None
                    extracted_post_date_str_static = None
                    logger.debug("[Scan] Static detail post_date extract failed (ignore) | url=%s err=%s", url[:200], e)

                # ✅ 일시적으로 필터링 조건 비활성화 (파일 크롤링 테스트용)
                # 2) 기간 필터가 켜져 있으면: 등록일 기준으로 게이트
                # if start_date or end_date:
                #     # 등록일을 확정 못 하면(기간 필터 켜진 상태) 첨부 수집을 하지 않는다.
                #     if not extracted_post_date_static:
                #         logger.info(
                #             "[Scan] 📅 Static detail skipped (missing post_date under date filter) | url=%s",
                #             url[:200],
                #         )
                #         return True
                #     if not is_date_in_range(extracted_post_date_static, start_date, end_date):
                #         logger.info(
                #             "[Scan] 📅 Static detail skipped (out of range) | url=%s post_date=%s",
                #             url[:200],
                #             extracted_post_date_str_static,
                #         )
                #         return True

                info = extract_author_info_from_html(html, url=url)
                page_author = info.get("author")
                page_department = info.get("department")
                page_author_kind = info.get("author_kind")
                page_author_raw = info.get("author_raw")
                page_department_raw = info.get("department_raw")
                if page_author or page_department:
                    logger.debug(
                        "[Scan] Page-level author/department extracted | page=%s author=%r dept=%r kind=%r",
                        url[:120],
                        page_author,
                        page_department,
                        page_author_kind,
                    )
        except Exception:
            page_author = None
            page_department = None
            page_author_kind = None
            page_author_raw = None
            page_department_raw = None

        soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]

        if is_list_page_static:
            logger.warning(
                "[Scan][Pagination] Static list page detected | url=%s",
                url[:200],
            )
        
        # ✅ 헤더, 푸터, 사이드메뉴 등 네비게이션 요소 제거
        # - 이러한 요소들의 링크가 수집되면 URL이 꼬이는 문제가 발생
        # - 본문 영역만 추출하여 정확한 게시판 링크만 수집
        exclude_selectors = [
            'header', '#header', '.header', '.educat-header',  # 헤더
            'footer', '#footer', '.footer', '.foot2025', '#footer2025',  # 푸터
            'nav.lnb', '#lnb', '.lnb', '.side', '#sidebar', '.left_menu',  # 사이드메뉴
            '.hgroup', '.breadcrumb', '.location', '.path', '.sub_top_nav', '.sub-top-nav',  # 경로(breadcrumbs)
            '.utilSet', '.layoutSnsWrap', '.sns-share',  # SNS 공유 버튼
            '.admSet', '.comment', '.satisfaction',  # 만족도/댓글
            'nav#gnb', '.gnb', '.gnbOpen',  # 전체 메뉴
        ]
        
        for selector in exclude_selectors:
            try:
                for elem in soup.select(selector):
                    elem.decompose()  # 요소를 완전히 제거
            except Exception:
                pass

        def _guess_page_param_for_js(page_url: str, soup_obj) -> str:
            try:
                qkeys = {k.lower() for (k, _v) in parse_qsl(urlparse(page_url).query or "", keep_blank_values=True)}
            except Exception:
                qkeys = set()
            for cand in ("pageindex", "pageno", "page", "curpage"):
                if cand in qkeys:
                    return "pageIndex" if cand == "pageindex" else ("pageNo" if cand == "pageno" else ("page" if cand == "page" else "curPage"))
            try:
                for nm in ("pageIndex", "pageNo", "page", "curPage"):
                    tag = soup_obj.find("input", attrs={"name": nm})
                    if tag is not None:
                        return nm
            except Exception:
                pass
            return "pageIndex"

        def _build_page_url_for_js(base_page_url: str, page_no: int, param_name: str) -> str:
            try:
                p = urlparse(base_page_url)
                pairs = parse_qsl(p.query or "", keep_blank_values=True)
                qd: Dict[str, str] = {}
                for k, v in pairs:
                    qd[str(k)] = str(v)
                qd[param_name] = str(page_no)
                new_pairs = sorted(qd.items(), key=lambda x: x[0])
                q = urlencode(new_pairs, doseq=True)
                return urlunparse((p.scheme or "https", p.netloc, p.path, "", q, ""))
            except Exception:
                return base_page_url

        def _build_pagination_url_from_js(expr: str, page_url: str, soup_obj) -> Optional[str]:
            try:
                m = re.search(
                    r"(?:fn_egov_link_page|link_page|goPage|go_page|movePage|fncPage|fn_go_page|fn_goPage)\s*"
                    r"\(\s*['\"]?(\d+)['\"]?\s*\)",
                    expr,
                    re.IGNORECASE,
                )
                if m:
                    pn = int(m.group(1))
                    if pn > 0:
                        param_name = _guess_page_param_for_js(page_url, soup_obj)
                        return _build_page_url_for_js(page_url, pn, param_name)
            except Exception:
                return None
            m2 = re.search(r"(?:pageindex|pageno|page|curpage|page_no|page_index)\s*=\s*['\"]?(\d+)['\"]?", expr, re.IGNORECASE)
            if m2:
                try:
                    pn = int(m2.group(1))
                except Exception:
                    return None
                if pn > 0:
                    param_name = _guess_page_param_for_js(page_url, soup_obj)
                    return _build_page_url_for_js(page_url, pn, param_name)
            return None

        def _normalize_list_cache_key(u: str) -> str:
            try:
                p = urlparse(u)
                pairs = parse_qsl(p.query or "", keep_blank_values=True)
                filtered = [(k, v) for (k, v) in pairs if k.lower() not in ("pageindex", "pageno", "page", "curpage", "page_no", "page_index")]
                filtered.sort()
                q = urlencode(filtered, doseq=True)
                scheme = (p.scheme or "https").lower()
                netloc = (p.netloc or "").lower()
                if netloc.startswith("www."):
                    netloc = netloc[4:]
                return urlunparse((scheme, netloc, p.path or "", "", q, ""))
            except Exception:
                return u

        def _extract_page_no(page_url: str) -> Optional[int]:
            try:
                pairs = parse_qsl(urlparse(page_url).query or "", keep_blank_values=True)
            except Exception:
                return None
            for k, v in pairs:
                if str(k).lower() in ("pageindex", "pageno", "page", "curpage", "page_no", "page_index"):
                    try:
                        return int(str(v).strip())
                    except Exception:
                        return None
            return None

        def _hash_view_links(links: List[str]) -> str:
            if not links:
                return ""
            try:
                payload = "\n".join(sorted(set(links)))
            except Exception:
                payload = "\n".join(links)
            return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()
        
        anchors = soup.find_all("a")
        # ✅ 첨부 링크가 <a>가 아니라 button/div 등 onclick으로 노출되는 사이트가 존재한다.
        # 따라서 onclick을 가진 모든 태그도 함께 스캔한다.
        try:
            onclick_tags = soup.find_all(onclick=True)  # type: ignore[misc]
        except Exception:
            onclick_tags = []
        if not anchors and not onclick_tags:
            return True

        # ==================== Static Fast Track (back01 style) ====================
        # 정적 phase(static_only)에서는 "파일 후보만" 빠르게 처리하고,
        # 일반 링크를 collection_worker(HEAD 검증)로 보내는 비용을 줄여 체감 속도를 올린다.
        # - 파일 후보: fileDown/download handler/확장자 기반 → _enqueue_file_candidate로 직행
        # - 정적 실패/불확실(첨부 0개 상세 등)은 기존 로직대로 동적 phase로 넘긴다.
        try:
            env_fast = str(os.getenv("SCAN_STATIC_FAST_TRACK", "1")).strip().lower() not in ("0", "false", "no", "off")
        except Exception:
            env_fast = True
        static_fast_track = bool(env_fast) and isinstance(page_profile, dict) and (page_profile.get("static_only") or (not page_profile.get("is_dynamic", True)))

        # 목록 페이지 전체에서 대표 날짜를 추출하지 않음 (항목별로 다르기 때문)
        # list_post_date = extract_post_date(html, url)
        # list_post_date_str = list_post_date.strftime('%Y-%m-%d') if list_post_date else None

        parsed_url = urlparse(url)
        allowed_domain = parsed_url.netloc
        collection_items: List[Dict[str, Any]] = [] # Renamed from batch_to_collect for clarity
        seen_targets = set()
        seen_files = set()
        templates = _discover_download_templates(html)
        file_items: List[Dict[str, Any]] = []

        def _infer_detail_source_page_from_tag(tag, base_url: str) -> Optional[str]:
            """
            목록(list.do)에서 파일다운로드 링크(fileDown.do 등)가 노출되는 경우,
            같은 "게시글 row"에 있는 상세(view.do?nttId=...) 링크를 찾아 source_page로 보강한다.
            - 사이트마다 파일 링크와 제목(view 링크)이 같은 div에 있지 않을 수 있어,
              1단계(parent.find_parent 1회)로는 실패하는 케이스가 많다.
            - 따라서 상위 컨테이너를 여러 단계 올라가며 detail 링크를 탐색한다.
            """
            if not tag:
                return None
            try:
                # 1) 가까운 상위 컨테이너부터 점진적으로 확장 탐색
                cur = tag
                for _ in range(8):
                    parent = getattr(cur, "parent", None)
                    if not parent:
                        break
                    cur = parent
                    hrefs: List[str] = []
                    try:
                        for a2 in cur.find_all("a"):
                            h2 = a2.get("href")
                            if h2:
                                hrefs.append(str(h2).strip())
                            # 일부 사이트는 onclick에 view.do를 포함
                            oc = a2.get("onclick")
                            if oc and "view.do" in str(oc):
                                hrefs.append(str(oc).strip())
                    except Exception:
                        hrefs = []
                    detail = find_detail_page_url_in_parent(hrefs, base_url) if hrefs else ""
                    if detail:
                        return detail
                return None
            except Exception:
                return None

        def _register_file(
            file_url: Optional[str],
            display_name: Optional[str] = None,
            reg_date: Optional[str] = None,
            author: Optional[str] = None,
            source_page: Optional[str] = None,
        ):
            if not file_url:
                return
            if file_url in seen_files or file_url in visited_urls:
                return
            seen_files.add(file_url)
            visited_urls.add(file_url)
            file_items.append({
                'url': file_url, 
                'name': (display_name or '').strip(),
                'reg_date': reg_date,
                # 목록 페이지에서 파일 링크를 발견한 경우,
                # 같은 row/부모 요소에 있는 "상세페이지(view.do?nttId=...)" 링크를 source_page로 보존해야
                # 작성자/부서 폴백 추출이 가능해진다.
                'source_page': source_page or url,
                # row_text에서 추출한 author가 없으면, 페이지 단위 author(부서 폴백)를 사용
                'author': author or page_author,
                'department': page_department,
                'author_kind': page_author_kind,
                'author_raw': page_author_raw,
                'department_raw': page_department_raw,
            })

        # anchors + onclick_tags(중복 제거) 순회
        _seen_tag_ids = set()
        tags_to_scan = []
        for t in anchors:
            tid = id(t)
            if tid in _seen_tag_ids:
                continue
            _seen_tag_ids.add(tid)
            tags_to_scan.append(t)
        for t in onclick_tags:
            tid = id(t)
            if tid in _seen_tag_ids:
                continue
            _seen_tag_ids.add(tid)
            tags_to_scan.append(t)

        list_cache_key = _normalize_list_cache_key(url) if is_list_page_static else ""
        list_page_view_links: List[str] = []
        list_page_view_seen: set[str] = set()
        list_page_item_dates: List[datetime] = []
        found_pagination_link = False
        detail_inline_attempts = 0

        if is_list_page_static and list_cache_key:
            try:
                stop_set = _list_page_date_stop_by_job.setdefault(_jid, set())
                if list_cache_key in stop_set:
                    logger.warning(
                        "[Scan][Pagination] Static list skipped (date gate stop) | list=%s",
                        str(url)[:160],
                    )
                    return True
            except Exception:
                pass


        # 🚨 [강화] 탐색 범위를 본문으로 한정 (없으면 전체 soup 사용)
    # 기존 soup.find_all 대신 optimized_scope.find_all 사용
    optimized_scope = soup.find("article") or soup.find(id="contents") or soup.find(id="content") or soup
    tags_to_scan = optimized_scope.find_all(["a", "link"], href=True)

    for tag in tags_to_scan:
        href = tag.get("href", "").strip()
        # 이전에 강화한 _normalize_url 함수 호출
        target_url = _normalize_url(href, url) 
        
        # [강화 필터] 메뉴성 콘텐츠 페이지(?key=... 포함 contents.do)는 차단
        if "contents.do" in target_url.lower():
            continue
        
        for tag in tags_to_scan:
            # ✅ 안전장치: 어떤 분기에서도 target_url이 참조되기 전에 항상 초기화되도록 한다.
            # (드물게 href 처리 분기에서 예외/조기 continue가 발생하면 UnboundLocalError가 날 수 있음)
            target_url = None
            href = tag.get("href")
            onclick = tag.get("onclick")
            text_content = tag.get_text(strip=True) if tag.text else ""
            title_attr = tag.get("title") or ""
            download_attr = tag.get("download") or ""
            class_attr = tag.get("class") or []
            class_str = " ".join(class_attr) if isinstance(class_attr, list) else class_attr or ""
            filename_hint = _extract_filename_candidate(text_content, title_attr, download_attr)
            candidate_name = (filename_hint or text_content or title_attr).strip()
            has_file_class = 'file_name' in class_str.lower() or 'attach' in class_str.lower()

            candidate_js = []
            if href:
                try:
                    href = str(href).strip()
                except Exception:
                    href = ""
                if href.lower().startswith("javascript:"):
                    candidate_js.append(href[len("javascript:"):])
                elif href and href not in ("#", ""):
                    target_url = urljoin(url, href)
                else:
                    target_url = None
            else:
                target_url = None

            if onclick:
                candidate_js.append(onclick)

            handled_js = False
            pagination_url = None
            for expr in candidate_js:
                if _is_list_page_url(url):
                    pagination_url = _build_pagination_url_from_js(expr, url, soup)
                    if pagination_url:
                        target_url = pagination_url
                        found_pagination_link = True
                        break
                direct_url = _extract_direct_download_url(expr, url)
                if direct_url:
                    # [목록 행 스캔] 태그 유연성 추가: 부모 요소(row)의 텍스트에서 날짜 추출 시도
                    row_text = ""
                    try:
                        parent = tag.find_parent(["li", "tr", "div"])
                        if parent:
                            row_text = parent.get_text(separator=" ", strip=True)
                    except Exception:
                        pass

                    combined_text = f"{candidate_name} {row_text}"
                    item_date = extract_date_from_text(combined_text)
                    item_date_str = item_date.strftime('%Y-%m-%d') if item_date else None
                    item_author = None
                    try:
                        item_author = extract_author_from_text(combined_text)
                    except Exception:
                        item_author = None
                    inferred_source_page = _infer_detail_source_page_from_tag(tag, url)

                    _register_file(
                        direct_url,
                        candidate_name,
                        reg_date=item_date_str,
                        author=item_author,
                        source_page=inferred_source_page,
                    )
                    handled_js = True
                    continue
                parsed = _parse_call_expression(expr)
                if not parsed:
                    continue
                func, args = parsed
                file_url = _build_download_url(func, args, url, templates)
                if file_url:
                    # [목록 행 스캔] 태그 유연성 추가: 부모 요소(row)의 텍스트에서 날짜 추출 시도
                    row_text = ""
                    try:
                        # <a> 태그의 상위 부모(li, tr 등)를 찾아 전체 텍스트를 가져옴
                        parent = tag.find_parent(["li", "tr", "div"])
                        if parent:
                            row_text = parent.get_text(separator=" ", strip=True)
                    except Exception:
                        pass
                    
                    # 링크 텍스트 + 부모 행 텍스트 통합하여 날짜 추출
                    combined_text = f"{candidate_name} {row_text}"
                    item_date = extract_date_from_text(combined_text)
                    item_date_str = item_date.strftime('%Y-%m-%d') if item_date else None
                    item_author = None
                    try:
                        item_author = extract_author_from_text(combined_text)
                    except Exception:
                        item_author = None
                    inferred_source_page = _infer_detail_source_page_from_tag(tag, url)
                    
                    _register_file(
                        file_url,
                        candidate_name,
                        reg_date=item_date_str,
                        author=item_author,
                        source_page=inferred_source_page,
                    )
                    handled_js = True
            if handled_js:
                continue

            if not target_url:
                continue

            if is_list_page_static:
                try:
                    if is_detail_page_url(target_url) or any(p in target_url.lower() for p in ("view.do", "detail.do", "read.do", "nttid=", "num=")):
                        if target_url not in list_page_view_seen:
                            list_page_view_seen.add(target_url)
                            list_page_view_links.append(target_url)
                except Exception:
                    pass
                try:
                    if any(k in target_url.lower() for k in ("pageindex=", "pageno=", "page=", "curpage=", "page_no=", "page_index=")):
                        if _normalize_list_cache_key(target_url) == list_cache_key:
                            found_pagination_link = True
                except Exception:
                    pass

            if target_url in seen_targets:
                continue
            seen_targets.add(target_url)

            parsed_target = urlparse(target_url)
            if parsed_target.netloc and allowed_domain and parsed_target.netloc != allowed_domain:
                continue

            lowered_target = target_url.lower()
            # [최우선 필터링] yong님 요청: contents 등 제외 패턴 즉시 스킵
            if any(skip in lowered_target for skip in SKIP_DEPTH_PATTERNS):
                continue

            if any(lowered_target.endswith(ext) for ext in STATIC_ASSET_EXTENSIONS):
                continue

            if _looks_like_file(target_url) or _looks_like_download_handler(target_url) or filename_hint or has_file_class:
                # [목록 행 스캔] 부모 요소(row)의 텍스트에서 날짜 추출 시도
                row_text = ""
                try:
                    parent = tag.find_parent(["li", "tr", "div"])
                    if parent:
                        row_text = parent.get_text(separator=" ", strip=True)
                except Exception:
                    pass
                # source_page 보강: 같은 row/부모 요소 안에서 상세페이지 링크를 찾아두면
                # DB 저장 단계에서 author/department 복구가 가능해진다.
                inferred_source_page = _infer_detail_source_page_from_tag(tag, url)
                
                combined_text = f"{candidate_name} {row_text}"
                item_date = extract_date_from_text(combined_text)
                item_date_str = item_date.strftime('%Y-%m-%d') if item_date else None
                item_author = None
                try:
                    item_author = extract_author_from_text(combined_text)
                except Exception:
                    item_author = None

                _register_file(
                    target_url,
                    candidate_name,
                    reg_date=item_date_str,
                    author=item_author,
                    source_page=inferred_source_page,
                )
                continue

            # yong님 요청: 일반 링크 발견 시에는 더이상 탐색(Scan) 카운트를 올리지 않음

            is_detail_link = any(pattern in target_url.lower() for pattern in [
                'view.do', 'detail.do', 'read.do', 'num=', 'nttid=', 'brddetail.do', 'brdview.do'
            ]) or is_detail_page_url(target_url)
            # 상세페이지는 우선 정적 처리로 먼저 진입(요청: 상세 접근 먼저 실행)
            if INLINE_VIEW_PROCESSING and is_detail_link:
                try:
                    if INLINE_VIEW_PROCESSING_MAX <= 0 or detail_inline_attempts < INLINE_VIEW_PROCESSING_MAX:
                        detail_inline_attempts += 1
                        handled = await _process_view_inline(target_url, depth, page_profile, referer=url)
                        if handled:
                            continue
                        # 정적 처리 실패 시 동적 fallback을 허용하기 위해 방문 마킹을 제거
                        try:
                            visited_urls.discard(target_url)
                        except Exception:
                            pass
                except Exception:
                    try:
                        visited_urls.discard(target_url)
                    except Exception:
                        pass

            if depth < max_depth:
                # 다음 탐색(scan) 인플라이트 증가
                progress_queue.put_nowait({
                    'type': 'in_flight',
                    'stage': 'scan',
                    'delta': 1
                })
                await in_queue.put({'url': target_url, 'depth': depth + 1, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date, 'referer': url})
            # [목록 행 스캔] 상세 링크 발견 시에도 주변 텍스트에서 날짜 추출
            row_text = ""
            try:
                parent = tag.find_parent(["li", "tr", "div"])
                if parent:
                    row_text = parent.get_text(separator=" ", strip=True)
            except Exception:
                pass
            
            combined_text = f"{candidate_name} {row_text}"
            item_date = extract_date_from_text(combined_text)
            item_date_str = item_date.strftime('%Y-%m-%d') if item_date else None

            # region agent log
            try:
                import json as _json, time as _time
                payload = {
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "H_SCAN1",
                    "location": "core/crawler/workers/scan.py:scan_worker:item_date_extracted",
                    "message": "item_date extracted",
                    "data": {
                        "job_id": job_id,
                        "target_url": target_url,
                        "item_date_str": item_date_str,
                        "start_date": str(start_date),
                        "end_date": str(end_date),
                        "is_detail_link": bool(is_detail_link),
                    },
                    "timestamp": int(_time.time() * 1000),
                }
                logging.getLogger("core.crawler.workers.scan").debug(_json.dumps(payload, ensure_ascii=False))
            except Exception:
                pass
            # endregion

            if item_date:
                try:
                    lowered_target = target_url.lower() if target_url else ""
                except Exception:
                    lowered_target = ""
                is_boardish_link = (
                    is_detail_link
                    or any(p in lowered_target for p in BOARD_PATTERNS)
                    or _is_list_page_url(target_url)
                )
                if is_boardish_link:
                    list_page_item_dates.append(item_date)

            # [추가] yong님 요청: 탐색 단계 필터링 (기간 필터링)
            if item_date and (start_date or end_date):
                target_date = item_date.date() if hasattr(item_date, 'date') else item_date
                if not is_date_in_range(target_date, start_date, end_date):
                    # region agent log
                    try:
                        import json as _json, time as _time
                        payload = {
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "H_SCAN2",
                            "location": "core/crawler/workers/scan.py:scan_worker:excluded_by_date",
                            "message": "excluded by date range",
                            "data": {
                                "job_id": job_id,
                                "target_url": target_url,
                                "item_date": item_date_str,
                                "start_date": str(start_date),
                                "end_date": str(end_date),
                            },
                            "timestamp": int(_time.time() * 1000),
                        }
                        logging.getLogger("core.crawler.workers.scan").debug(_json.dumps(payload, ensure_ascii=False))
                    except Exception:
                        pass
                    # endregion
                    logger.info(f"[Scan] 📅 탐색/수집 제외: 기간 필터링 ({item_date_str}) | {target_url}")
                    continue
            if (start_date or end_date) and not item_date:
                # ✅ 예외 처리(권장 적용):
                # - target_url이 list면: 등록일이 없어도 list→view 확장 후 view들을 enqueue (view에서 등록일 확정)
                # - target_url이 view(상세)면: 등록일이 없어도 먼저 진입해서 상세 HTML에서 등록일을 재추출
                try:
                    lowered_target = str(target_url).lower()
                except Exception:
                    lowered_target = ""
                detail_hints = ('view.do', 'detail.do', 'read.do', 'num=', 'nttid=', 'brddetail.do', 'brdview.do')
                is_boardish_link = any(p in lowered_target for p in BOARD_PATTERNS) or any(h in lowered_target for h in detail_hints) or is_detail_page_url(target_url)

                if is_boardish_link:
                    # 상세(view) 링크면 같은 depth로 강제 follow (query_links_only/max_depth=0에서도 동작)
                    if is_detail_page_url(target_url):
                        if target_url not in visited_urls:
                            try:
                                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                            except Exception:
                                pass
                            # region agent log
                            try:
                                import json as _json, time as _time
                                payload = {
                                    "sessionId": "debug-session",
                                    "runId": "run1",
                                    "hypothesisId": "H_SCAN3",
                                    "location": "core/crawler/workers/scan.py:scan_worker:enqueue_detail_follow",
                                    "message": "enqueueing detail follow for date resolution",
                                    "data": {
                                        "job_id": job_id,
                                        "target_url": target_url,
                                        "depth": depth,
                                        "start_date": str(start_date),
                                        "end_date": str(end_date),
                                    },
                                    "timestamp": int(_time.time() * 1000),
                                }
                                logging.getLogger("core.crawler.workers.scan").debug(_json.dumps(payload, ensure_ascii=False))
                            except Exception:
                                pass
                            # endregion
                            await in_queue.put({'url': target_url, 'depth': depth, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date, 'referer': url})
                        continue

                    # 목록(list) 링크면 view들을 추출하여 같은 depth로 enqueue
                    if _is_list_page_url(target_url):
                        # ✅ 중요: source가 "상세(view)" 페이지인 경우, 본문/첨부와 무관한 메뉴(list) 링크가 매우 많다.
                        # 이 분기(기간 필터 + item_date 없음)에서 list→views 확장을 해버리면
                        # 'source 첨부를 안 뽑는다'는 오해를 만들고, 불필요한 view 폭발로 성능을 갉아먹는다.
                        # 따라서 상세 페이지에서는 list→views 확장을 하지 않는다(해당 list는 이미 일반 스캔 큐로는 들어감).
                        if is_post_detail_page_static:
                            continue
                        try:
                            view_urls = await _expand_list_to_views(
                                target_url,
                                url,
                                depth=depth,
                                page_profile=page_profile,
                                source_list_url=target_url,
                            )
                        except Exception:
                            view_urls = []
                        if not view_urls:
                            logger.debug(
                                "[Scan] List link has no view links; skip (static) | source=%s list=%s",
                                url[:120],
                                target_url[:120],
                            )
                            # ✅ list는 JS로 view 링크가 렌더되는 경우가 많다.
                            # 정적 확장에서 0개면 동적 phase로 넘겨서 list→view 추출을 재시도한다.
                            if isinstance(page_profile, dict) and page_profile.get("static_only", False):
                                try:
                                    await progress_queue.put({'type': 'static_failed', 'count': 1, 'items': [target_url]})
                                except Exception:
                                    pass
                            else:
                                # static_only가 아니면 바로 동적 스캔 대상으로 enqueue(같은 depth)
                                try:
                                    next_profile = dict(page_profile) if isinstance(page_profile, dict) else {}
                                    next_profile.pop("static_only", None)
                                    next_profile["is_dynamic"] = True
                                except Exception:
                                    next_profile = {"is_dynamic": True}
                                try:
                                    progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                                except Exception:
                                    pass
                                await in_queue.put({'url': target_url, 'depth': depth, 'page_profile': next_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date, 'referer': url})
                            continue
                        logger.info(
                            "[Scan] List link expanded to views (static) | source=%s list=%s views=%s",
                            url[:120],
                            target_url[:120],
                            len(view_urls),
                        )
                        # ✅ 요구사항: view가 있으면 즉시 파일 링크 수집까지 진행
                        # 단, scan 워커가 멈추지 않도록 background로 스케줄링한다.
                        try:
                            t = asyncio.create_task(_schedule_inline_bulk(view_urls, depth, page_profile, target_url))
                            _track_task(t)
                        except Exception:
                            # fallback: 최악의 경우 동기 처리
                            await _process_views_inline_bulk(view_urls, depth, page_profile, source_list_url=target_url)
                        continue

                # 기간 필터가 켜진 상태에서 등록일을 확정할 수 없고, boardish도 아니면 Collection으로 넘기지 않는다.
                logger.info(f"[Scan] 🛑 탐색/수집 제외: 등록일 정보 확인 불가 | {target_url}")
                continue

            # back01 방식(정적 fast-track)에서는 일반 링크 후보를 collection_worker로 보내지 않는다.
            # - collection은 HEAD 검증/DB 중복 체크로 느릴 수 있어 "카운팅이 안 오르는" 체감을 유발한다.
            # - 파일 후보는 위에서 _register_file/_enqueue_file_candidate로 이미 처리됨.
            if not static_fast_track:
                collect_item = {
                    'url': target_url, 
                    'source_page': target_url if is_detail_link else url,
                    'reg_date': item_date_str,
                    'chat_bot_id': chat_bot_id,
                    'db_name': db_name,
                    'job_id': job_id,
                    'start_date': start_date,
                    'end_date': end_date,
                }
                if filename_hint:
                    collect_item['name'] = filename_hint
                collection_items.append(collect_item)

        # [용님 요청] 실시간성 향상: 배치를 만들지 않고 발견 즉시 개별 전송
        # 수집 대상(collection_worker)으로 개별 아이템 즉시 전달
        if not static_fast_track:
            # ✅ stop(중단) 요청 시: 더 이상 collection 후보를 enqueue하지 않는다.
            # - 이미 선별된(scan_batch_queue에 들어간 것들)은 이후 단계에서 저장/학습까지 마무리됨
            # - stop 이후에도 list/상세 페이지 처리가 이어지며 후보를 계속 밀어 넣는 것을 방지(UX)
            if _is_scan_disabled(job_id):
                logger.info(
                    "[Scan] stop requested -> skip enqueue collection candidates | job_id=%s url=%s candidates=%s",
                    job_id,
                    str(url)[:200],
                    len(collection_items),
                )
                collection_items = []
            for collect_item in collection_items:
                # 개별 아이템마다 인플라이트 증가
                progress_queue.put_nowait({
                    'type': 'in_flight',
                    'stage': 'collection',
                    'delta': 1
                })
                # 개별 아이템 전송 (BatchQueue가 내부적으로 버퍼링 및 쪼개서 전송 수행)
                # 발견된 개별 URL 정보를 파일로 기록 (best-effort)
                try:
                    from backend.shared.crawl_shared import write_scan_log
                    write_scan_log(
                        job_id,
                        {
                            "job_id": job_id,
                            "url": collect_item.get("url"),
                            "url_key": collect_item.get("url_key"),
                            "note": "queued_collect_item",
                        },
                    )
                except Exception:
                    pass
                await scan_batch_queue.put(collect_item)

        if file_items:
            # ✅ stop(중단) 요청 시: 파일 후보도 추가 enqueue하지 않는다(이미 큐에 들어간 것만 처리).
            if _is_scan_disabled(job_id):
                logger.info(
                    "[Scan] stop requested -> skip enqueue file candidates | job_id=%s url=%s files=%s",
                    job_id,
                    str(url)[:200],
                    len(file_items),
                )
                file_items = []
            for file_meta in file_items:
                file_url = file_meta['url']
                # ✅ 기간 필터 단일화:
                # - 기간 필터가 켜져 있고 현재 페이지가 상세가 아니라면,
                #   목록/컨텐츠에서 보이는 파일 링크를 바로 수집하지 않고,
                #   source_page(상세페이지)를 큐에 넣어 "게시물 등록일" 기준으로 판단하게 한다.
                if (start_date or end_date) and (not is_post_detail_page_static):
                    sp = file_meta.get('source_page') or ""
                    try:
                        sp = str(sp).strip()
                    except Exception:
                        sp = ""
                    if sp and is_detail_page_url(sp) and (sp not in visited_urls):
                        try:
                            progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                        except Exception:
                            pass
                        # stop(중단) 요청 시에는 추가 scan enqueue를 하지 않는다.
                        if not _is_scan_disabled(job_id):
                            await in_queue.put({'url': sp, 'depth': depth, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date})
                    continue

                # 정적 페이지에서도 즉시 파일을 처리하도록 Fast Track 호출
                await _enqueue_file_candidate(
                    file_url,
                    file_meta.get('source_page') or url,
                    job_id=job_id,
                    display_name=file_meta.get('name'),
                    # 상세페이지라면 게시물 등록일(추출값)을 reg_date로 사용한다.
                    # (기간 필터 OFF일 때는 기존처럼 row-text 기반 reg_date도 허용)
                    reg_date=(
                        extracted_post_date_str_static
                        if is_post_detail_page_static
                        else file_meta.get('reg_date')
                    ),
                    author=file_meta.get('author') or page_author,
                    department=file_meta.get('department') or page_department,
                    author_kind=file_meta.get('author_kind') or page_author_kind,
                    author_raw=file_meta.get('author_raw') or page_author_raw,
                    department_raw=file_meta.get('department_raw') or page_department_raw,
                )
        else:
            # 정적 경로에서 첨부가 0개인 경우도 A/B 구분을 위해 로그를 남긴다.
            pass

        if is_post_detail_page_static and not file_items:
            # 상세 페이지로 보이는데 정적으로 파일을 하나도 못 찾았다면?
            # -> 내용이 동적(JS)으로 로딩될 가능성이 높으므로, True를 반환해 "처리 완료"하지 말고
            #    False를 반환하여 메인 루프의 Playwright(동적 분석)가 다시 확인하게 한다.
            logger.debug(
                "[Scan] Static detail page with 0 files -> Fallback to dynamic scan (safety check) | url=%s",
                url[:120]
            )
            return False

        logger.debug(
            "[Scan] Static parsed | url=%s depth=%s anchors=%s collection_candidates=%s file_candidates=%s",
            url,
            depth,
            len(anchors),
            len(collection_items),
            len(file_items),
        )

        # 목록 페이지는 현재 페이지 처리 후 다음 pageIndex로 자동 진행 (페이징 링크가 없을 때)
        if is_list_page_static:
            logger.warning(
                "[Scan][Pagination] Static list summary | url=%s views=%s found_pagination_link=%s",
                url[:200],
                len(list_page_view_links),
                bool(found_pagination_link),
            )

        if is_list_page_static and list_cache_key and (start_date or end_date) and list_page_item_dates:
            try:
                date_gate_enabled = os.getenv("SCAN_LIST_STOP_ON_DATE_EXCEEDED", "1") == "1"
            except Exception:
                date_gate_enabled = True
            if date_gate_enabled:
                try:
                    threshold = int(os.getenv("SCAN_LIST_DATE_STOP_THRESHOLD", "2") or "2")
                except Exception:
                    threshold = 2
                threshold = max(1, min(threshold, 10))
                try:
                    start_cmp = start_date.date() if isinstance(start_date, datetime) else start_date
                except Exception:
                    start_cmp = start_date
                # 확장(그레이스) 설정: 페이지네이션 중단 기준을 start_date에서 extra_days만큼 확장하여 여유 탐색
                try:
                    extra_days = int(os.getenv("SCAN_DATE_GATE_EXTRA_DAYS", "7") or "7")
                except Exception:
                    extra_days = 7
                try:
                    include_extra_gate = str(os.getenv("SCAN_DATE_GATE_INCLUDE_EXTRA", "1")).strip().lower() in ("1", "true", "yes", "on")
                except Exception:
                    include_extra_gate = True
                if start_cmp and include_extra_gate:
                    try:
                        start_cmp_extended = start_cmp - timedelta(days=extra_days)
                    except Exception:
                        start_cmp_extended = start_cmp
                else:
                    start_cmp_extended = start_cmp
                if start_cmp:
                    try:
                        page_dates = [
                            (d.date() if isinstance(d, datetime) else d)
                            for d in list_page_item_dates
                            if d
                        ]
                        if page_dates:
                            max_dt = max(page_dates)
                            date_gate_cache = _list_page_date_gate_by_job.setdefault(_jid, {})
                            stop_set = _list_page_date_stop_by_job.setdefault(_jid, set())
                            # 비교는 확장된 기준(start_cmp_extended)을 사용
                            if max_dt < start_cmp_extended:
                                count = date_gate_cache.get(list_cache_key, 0) + 1
                                date_gate_cache[list_cache_key] = count
                                if count >= threshold:
                                    stop_set.add(list_cache_key)
                                    logger.warning(
                                        "[Scan][Pagination] Static list date-gate stop (out-of-range repeated) | list=%s count=%s max_dt=%s start_date=%s",
                                        str(url)[:160],
                                        count,
                                        max_dt.strftime("%Y-%m-%d") if hasattr(max_dt, "strftime") else str(max_dt),
                                        start_cmp_extended.strftime("%Y-%m-%d") if hasattr(start_cmp_extended, "strftime") else str(start_cmp_extended),
                                    )
                                    try:
                                        # 명시적 요구: 기간 초과로 더 이상 크롤링할 항목이 없음을 stdout에 표시
                                        print("finish", flush=True)
                                    except Exception:
                                        pass
                            else:
                                if date_gate_cache.get(list_cache_key):
                                    date_gate_cache[list_cache_key] = 0
                    except Exception:
                        pass

        if is_list_page_static and list_page_view_links and not found_pagination_link:
            try:
                _ctx = _scan_ctx_var.get()
                _jid = str((_ctx or {}).get("job_id") or "default")
            except Exception:
                _jid = "default"
            try:
                stop_set = _list_page_date_stop_by_job.setdefault(_jid, set())
                if list_cache_key and list_cache_key in stop_set:
                    logger.warning(
                        "[Scan][Pagination] Static auto stopped (date gate) | list=%s",
                        str(url)[:160],
                    )
                    return True
            except Exception:
                pass
            list_sig_cache = _list_page_signature_by_job.setdefault(_jid, {})
            sig_set = list_sig_cache.setdefault(list_cache_key or url, set())
            signature = _hash_view_links(list_page_view_links)
            if signature and signature in sig_set:
                logger.warning(
                    "[Scan][Pagination] Static auto stopped (duplicate signature) | list=%s",
                    str(url)[:160],
                )
            else:
                if signature:
                    sig_set.add(signature)
                try:
                    max_pages = int(os.getenv("SCAN_LIST_MAX_PAGES", "200") or "200")
                except Exception:
                    max_pages = 200
                max_pages = max(0, min(max_pages, 5000))
                if max_pages == 0 or len(sig_set) < max_pages:
                    try:
                        page_param = _guess_page_param_for_js(url, soup)
                    except Exception:
                        page_param = "pageIndex"
                    cur_no = _extract_page_no(url) or 1
                    next_no = cur_no + 1
                    next_url = _build_page_url_for_js(url, next_no, page_param)
                    if _normalize_list_cache_key(next_url) == (list_cache_key or _normalize_list_cache_key(url)):
                        if next_url not in visited_urls and not _is_scan_disabled(job_id):
                            try:
                                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                            except Exception:
                                pass
                            await in_queue.put({'url': next_url, 'depth': depth, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date, 'referer': url})
                            logger.warning(
                                "[Scan][Pagination] Static auto next queued | list=%s cur=%s next=%s param=%s views=%s",
                                str(url)[:160],
                                cur_no,
                                next_no,
                                page_param,
                                len(list_page_view_links),
                            )
                        else:
                            logger.warning(
                                "[Scan][Pagination] Static auto next skipped (visited/stop) | list=%s next=%s",
                                str(url)[:160],
                                str(next_url)[:160],
                            )
                    else:
                        logger.warning(
                            "[Scan][Pagination] Static auto next skipped (cache key mismatch) | list=%s next=%s",
                            str(url)[:160],
                            str(next_url)[:160],
                        )
                else:
                    logger.warning(
                        "[Scan][Pagination] Static auto stopped (max pages) | list=%s max=%s",
                        str(url)[:160],
                        max_pages,
                    )
        return True

    def _normalize_url(candidate: str, base: str) -> str:
        """
        [로직 강화] 
        1. 상대 경로를 절대 경로로 복원 (Fallback 포함)
        2. 파라미터 정렬 및 정제 (무한 루프 방지 핵심)
        """
        if not candidate:
            return ""
            
        # 1. 기초 정규화: 프로토콜 및 상대 경로 결합
        if candidate.startswith(("http://", "https://")):
            next_url = candidate
        else:
            if candidate.startswith("//"):
                candidate = "https:" + candidate
            next_url = urljoin(base, candidate)

        # 2. 도메인 누락 복구 (Fallback)
        # urljoin 결과물에 도메인이 없는 기형적 주소일 경우 현재 도메인을 강제 주입합니다.
        parsed = urlparse(next_url)
        if not parsed.netloc:
            base_parsed = urlparse(base)
            next_url = urlunparse((
                base_parsed.scheme or "https",
                base_parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            parsed = urlparse(next_url)

        # 3. 무한 루프 방지: 파라미터 정제 및 알파벳 순 정렬
        # keep_blank_values=False를 통해 URL 끝에 붙는 불필요한 '&'를 제거합니다.
        query_params = parse_qsl(parsed.query, keep_blank_values=False)
        cleaned_query = []
        seen_keys = set()
        
        for k, v in query_params:
            # jsessionid, rnd 등 무의미한 난수 파라미터 제외
            if k.lower() in ["jsessionid", "rnd", "timestamp", "_", "menu_cd"]:
                continue
            if k not in seen_keys:
                seen_keys.add(k)
                cleaned_query.append((k, v))
                
        # 🚨 파라미터 정렬: 순서가 바뀌어 생성되는 중복 URL을 하나로 통일합니다.
        cleaned_query.sort(key=lambda x: x[0])
        new_query_string = urlencode(cleaned_query)

        # 4. 최종 URL 재조합 (페이지 내 이동 앵커 # 제거)
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query_string,
            ""  
        ))

    def _extract_direct_download_url(expr: str, base: str) -> Optional[str]:
        """
        onclick/href 문자열 안에 다운로드 URL이 직접 포함된 경우를 추출한다.
        - 예: location.href='/portal/cmmn/file/fileDown.do?...'
        - 예: fetch('/portal/cmmn/file/fileDown.do?...')
        """
        if not expr:
            return None
        direct_url_pattern = re.compile(
            r"(?P<url>"
            r"(?:https?:)?//[^'\"]+?filedown\.do\?[^'\"]+"
            r"|/[^'\"]+?filedown\.do\?[^'\"]+"
            r")",
            re.IGNORECASE,
        )
        m = direct_url_pattern.search(expr)
        if not m:
            return None
        u = (m.group("url") or "").strip()
        if not u:
            return None
        # HTML 속성(href)에서 &amp; 같은 엔티티가 그대로 들어오는 케이스가 많다.
        # 이를 그대로 쓰면 query param이 깨져 다운로드/식별 실패가 발생한다.
        try:
            u = _html_unescape.unescape(u)
        except Exception:
            # 최소한 &amp;만이라도 치환
            u = u.replace("&amp;", "&")
        if u.startswith("//"):
            try:
                p = urlparse(base)
                scheme = p.scheme or "https"
            except Exception:
                scheme = "https"
            u = f"{scheme}:{u}"
        return _normalize_url(u, base)

    def _discover_download_templates(page_html: str) -> Dict[str, str]:
        templates: Dict[str, str] = {}
        if not page_html:
            return templates
        fn_pattern = re.compile(r"function\s+([A-Za-z_][\w]*)\s*\(([^)]*)\)\s*{(.*?)}", re.IGNORECASE | re.DOTALL)
        # fileDown URL을 atchFileId/fileSn로 문자열 결합하는 패턴을 폭넓게 감지한다.
        # - location.href = '...fileDown.do?...atchFileId=' + atchFileId + '&fileSn=' + fileSn
        # - fetch('...fileDown.do?...atchFileId=' + atchFileId + '&fileSn=' + fileSn, ...)
        # - window.open('...fileDown.do?...atchFileId=' + atchFileId + '&fileSn=' + fileSn)
        assign_patterns = [
            re.compile(
                r"location\.href\s*=\s*['\"](?P<prefix>[^'\"]*?fileDown[^'\"]*?atchFileId=)['\"]\s*\+\s*[^;]*?atchFileId[^;]*?"
                r"\+\s*['\"](?P<mid>[^'\"]*?fileSn=)['\"]\s*\+\s*[^;]*?fileSn",
                re.IGNORECASE | re.DOTALL,
            ),
            re.compile(
                r"fetch\s*\(\s*['\"](?P<prefix>[^'\"]*?fileDown[^'\"]*?atchFileId=)['\"]\s*\+\s*[^,;]*?atchFileId[^,;]*?"
                r"\+\s*['\"](?P<mid>[^'\"]*?fileSn=)['\"]\s*\+\s*[^,;]*?fileSn",
                re.IGNORECASE | re.DOTALL,
            ),
            re.compile(
                r"window\.open\s*\(\s*['\"](?P<prefix>[^'\"]*?fileDown[^'\"]*?atchFileId=)['\"]\s*\+\s*[^,;]*?atchFileId[^,;]*?"
                r"\+\s*['\"](?P<mid>[^'\"]*?fileSn=)['\"]\s*\+\s*[^,;]*?fileSn",
                re.IGNORECASE | re.DOTALL,
            ),
            re.compile(
                r"document\.location(?:\.href)?\s*=\s*['\"](?P<prefix>[^'\"]*?fileDown[^'\"]*?atchFileId=)['\"]\s*\+\s*[^;]*?atchFileId[^;]*?"
                r"\+\s*['\"](?P<mid>[^'\"]*?fileSn=)['\"]\s*\+\s*[^;]*?fileSn",
                re.IGNORECASE | re.DOTALL,
            ),
        ]
        for match in fn_pattern.finditer(page_html):
            func_name = match.group(1).strip().lower()
            body = match.group(3)
            if "filedown" not in body.lower() and "download" not in body.lower():
                continue
            found = None
            for ptn in assign_patterns:
                found = ptn.search(body)
                if found:
                    break
            if not found:
                continue
            prefix = found.group("prefix")
            mid = found.group("mid")
            template = f"{prefix}" + "{atchFileId}" + f"{mid}" + "{fileSn}"
            templates[func_name] = template
        return templates

    def _parse_call_expression(expr: str):
        if not expr:
            return None
        match = CALL_PATTERN.search(expr)
        if not match:
            return None
        func = match.group("func").strip().lower()
        args_str = match.group("args")
        args = []
        for part in re.finditer(r"(?:'([^']*)'|\"([^\"]*)\"|([^,]+))", args_str or ""):
            value = next((g for g in part.groups() if g is not None), "").strip()
            if value.endswith(";"):
                value = value[:-1].strip()
            args.append(value)
        return func, args

    def _build_download_url(func: str, args, current_url: str, templates: Dict[str, str]) -> Optional[str]:
        if not func or not args:
            return None
        func_name = func.lower()
        template = templates.get(func_name)
        if not template:
            if func_name in DEFAULT_DOWNLOAD_TEMPLATES:
                template = DEFAULT_DOWNLOAD_TEMPLATES[func_name]
            elif "download" in func_name:
                template = DEFAULT_DOWNLOAD_TEMPLATES["download"]
            elif "filedown" in func_name or "downfile" in func_name:
                template = DEFAULT_DOWNLOAD_TEMPLATES["filedown"]
        if not template:
            return None
        atch_file_id = args[0]
        file_sn = args[1] if len(args) > 1 and args[1] else "0"
        if not atch_file_id:
            return None
        formatted = template.format(
            atchFileId=quote_plus(atch_file_id),
            fileSn=quote_plus(file_sn),
        )
        try:
            formatted = _html_unescape.unescape(formatted)
        except Exception:
            formatted = formatted.replace("&amp;", "&")
        return _normalize_url(formatted, current_url)

    async def _extract_js_attachment_urls(page, current_url: str) -> List[str]:
        attachment_urls = set()

        # HTML 추출
        try:
            page_html = await page.content()
        except Exception as e:
            # HTML을 가져올 수 없는 경우 굳이 템플릿 분석할 필요 없음
            page_html = None

        templates = _discover_download_templates(page_html) if page_html else {}

        # ✅ 헤더, 푸터, 사이드메뉴 등 네비게이션 요소 제거 (JavaScript로 DOM 조작)
        # - 정적 페이지와 동일한 방식으로 본문 영역만 추출
        try:
            await page.evaluate("""
                () => {
                    const excludeSelectors = [
                        'header', '#header', '.header', '.educat-header',
                        'footer', '#footer', '.footer', '.foot2025', '#footer2025',
                        'nav.lnb', '#lnb', '.lnb', '.side', '#sidebar', '.left_menu',
                        '.hgroup', '.breadcrumb', '.location', '.path', '.sub_top_nav', '.sub-top-nav',
                        '.utilSet', '.layoutSnsWrap', '.sns-share',
                        '.admSet', '.comment', '.satisfaction',
                        'nav#gnb', '.gnb', '.gnbOpen'
                    ];
                    
                    excludeSelectors.forEach(selector => {
                        try {
                            document.querySelectorAll(selector).forEach(el => el.remove());
                        } catch (e) {
                            // 선택자 오류 무시
                        }
                    });
                }
            """)
        except Exception as e:
            logger.debug("[Scan] Failed to remove navigation elements in dynamic page: %s", e)

        # 다운로드 대상 DOM 요소 추출
        try:
            elements = await page.query_selector_all(DOWNLOAD_SELECTOR)
        except Exception as e:
            elements = []
        for el in elements:
            try:
                attrs = [
                    await el.get_attribute("href"),
                    await el.get_attribute("onclick"),
                ]
            except Exception:
                continue

            for attr in (a for a in attrs if a):
                direct = _extract_direct_download_url(attr, current_url)
                if direct:
                    attachment_urls.add(direct)
                    continue
                parsed = _parse_call_expression(attr)
                if not parsed:
                    continue

                func, args = parsed
                download_url = _build_download_url(func, args, current_url, templates)

                if download_url:
                    attachment_urls.add(download_url)

        # 순서를 고정시키면 테스트 안정성 증가
        return sorted(attachment_urls)

    # AttachDebug 제거됨

    heartbeat_interval = 30  # 30초마다 하트비트 로그
    last_heartbeat = asyncio.get_event_loop().time()
    
    _last_pause_log: Dict[str, float] = {}
    while True:
        url_item = None
        try:
            # 1. 큐에서 아이템 가져오기 (타임아웃 추가: 5초마다 하트비트 확인)
            try:
                url_item = await asyncio.wait_for(in_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                # 큐가 비어있어도 하트비트 로그 출력 (워커가 살아있음을 확인)
                current_time = asyncio.get_event_loop().time()
                if current_time - last_heartbeat >= heartbeat_interval:
                    should_log = True
                    if heartbeat_guard is not None:
                        try:
                            should_log = bool(heartbeat_guard())
                        except Exception:
                            should_log = True
                    if should_log:
                        logger.info("[Scan] Worker alive, waiting for URL...")
                        last_heartbeat = current_time
                continue
            
            # ===== job_id 기반 컨텍스트 스위칭 =====
            job_id = "default"
            if isinstance(url_item, dict):
                try:
                    job_id = str(url_item.get("job_id") or "default")
                except Exception:
                    job_id = "default"

                # item-level overrides (job별 상이한 설정 지원)
                start_date = url_item.get("start_date", _default_start_date)
                end_date = url_item.get("end_date", _default_end_date)
                chat_bot_id = url_item.get("chat_bot_id", _default_chat_bot_id)
                db_name = url_item.get("db_name", _default_db_name)

                # job별 visited/dedupe로 교체
                visited_urls = visited_by_job.setdefault(job_id, set())
                if file_dedup_by_job is not None:
                    file_deduplicator = file_dedup_by_job.setdefault(job_id, CollectionDeduplicator())

                url = url_item.get('url')
                depth = url_item.get('depth', 0)
                page_profile = url_item.get('page_profile')
                referer = url_item.get('referer')
            else:
                url = url_item
                depth = 0
                page_profile = None
                referer = None
                # dict가 아니면(레거시) default 컨텍스트
                job_id = "default"
                start_date = _default_start_date
                end_date = _default_end_date
                chat_bot_id = _default_chat_bot_id
                db_name = _default_db_name
                visited_urls = visited_by_job.setdefault(job_id, set())
                if file_dedup_by_job is not None:
                    file_deduplicator = file_dedup_by_job.setdefault(job_id, CollectionDeduplicator())

            # backpressure: scan pause
            # pause control: prefer awaiting an event instead of busy polling
            try:
                from core.crawler.queues import get_job_pause_event, get_job_pause_flags
                pause_event = get_job_pause_event(job_id)
                paused = bool(get_job_pause_flags(job_id).get("scan", False))
            except Exception:
                pause_event = None
                paused = False

            if paused:
                # Log pause entry once
                last = _last_pause_log.get(job_id, 0.0)
                now = asyncio.get_event_loop().time()
                if now - last >= 5.0:
                    _last_pause_log[job_id] = now
                    try:
                        _log_path = os.getenv(
                            "AGENT_DEBUG_LOG_PATH",
                            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".cursor", "debug.log")),
                        )
                        try:
                            os.makedirs(os.path.dirname(_log_path), exist_ok=True)
                        except Exception:
                            pass
                        with open(_log_path, "a", encoding="utf-8") as _f:
                            _f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                                "hypothesisId": "H_scan_paused",
                                "location": "core/crawler/workers/scan.py:scan_worker",
                                "message": "scan_paused",
                                "data": {"job_id": job_id, "url": str(url)[:160]},
                                "timestamp": int(time.time() * 1000),
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                # Wait on the pause_event if available, otherwise fallback to short sleep
                try:
                    if pause_event is not None:
                        await pause_event.wait()
                    else:
                        await asyncio.sleep(0.2)
                except asyncio.CancelledError:
                    raise
                # re-check queue item after resume
                continue

            # background task(create_task)들이 현재 job 컨텍스트를 안전하게 상속받도록 ContextVar를 세팅
            try:
                _scan_ctx_var.set(
                    {
                        "job_id": job_id,
                        "chat_bot_id": chat_bot_id,
                        "db_name": db_name,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
            except Exception:
                pass

            if not url or url in visited_urls or depth > max_depth:
                # 큐에서 꺼낸 아이템은 어떤 경우에도 task_done 처리해야 join()이 멈추지 않는다.
                try:
                    progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': -1})
                except Exception:
                    pass
                try:
                    in_queue.task_done()
                except Exception:
                    pass
                continue

            # yong님 요청: 페이지 방문은 탐색(Scan) 수로 카운트하지 않음

            # 1차 필터링 로직이 Collection 단계로 이동됨

            if _looks_like_file(url) or _looks_like_download_handler(url):
                logger.debug("[Scan] Detected direct download URL; enqueueing without page.goto: %s", url)
                # 📊 Progress: URL 처리 시도 (직접 파일 URL)
                # 직접 파일 URL은 날짜 필터링을 적용할 수 없으므로 통과 (목록 페이지에서 발견된 경우는 페이지를 열어서 필터링)
                await _enqueue_file_candidate(url, url, job_id=job_id)
                try:
                    progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': -1})
                except Exception:
                    pass
                try:
                    in_queue.task_done()
                except Exception:
                    pass
                continue
            
            visited_urls.add(url)
            
            # 📊 Progress: URL 처리 시도 시작
            await progress_queue.put({'type': 'scan_attempt', 'count': 1})

            # 2. 페이지 열기 및 링크 추출
            # 2. 페이지 열기 및 링크 추출 (전체 처리 리트라이 루프 추가)
            is_static_profile = bool(page_profile) and not page_profile.get('is_dynamic', True)
            if is_static_profile:
                handled = await _process_static_page(url, depth, page_profile, referer=referer)
                if handled:
                    try:
                        progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': -1})
                    except Exception:
                        pass
                    in_queue.task_done()
                    continue
                # ✅ 요청 반영:
                # 상세(view) 페이지는 정적으로 체크 후 실패하면 "다음 phase"로 미루지 말고
                # 즉시 Playwright(동적)로 fallback한다.
                if isinstance(page_profile, dict) and page_profile.get("static_only") and is_detail_page_url(url):
                    try:
                        # 동적 처리로 다시 들어가야 하므로 visited 마킹을 해제
                        visited_urls.discard(url)
                    except Exception:
                        pass
                    try:
                        page_profile = dict(page_profile)
                        page_profile.pop("static_only", None)
                        page_profile["is_dynamic"] = True
                    except Exception:
                        page_profile = {"is_dynamic": True}
                    logger.debug("[Scan] Static failed on detail in static_only -> immediate dynamic fallback | url=%s", url)
                elif isinstance(page_profile, dict) and page_profile.get("static_only"):
                    # list/기타 페이지는 기존처럼 workflow가 모아서 동적 phase에서 처리(비용 절감)
                    try:
                        # 동일 워커에서 재처리(동적 phase)될 수 있으므로 visited를 풀어준다.
                        visited_urls.discard(url)
                    except Exception:
                        pass
                    try:
                        await progress_queue.put({'type': 'static_failed', 'count': 1, 'items': [url]})
                    except Exception:
                        pass
                    try:
                        progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': -1})
                    except Exception:
                        pass
                    try:
                        in_queue.task_done()
                    except Exception:
                        pass
                    continue

                logger.debug("[Scan] Static handling failed for %s; falling back to Playwright.", url)

            # [전체 리트라이 루프] 브라우저/페이지 닫힘 오류(TargetClosedError) 대응
            max_processing_attempts = 2
            process_success = False
            
            for p_attempt in range(1, max_processing_attempts + 1):
                page = None
                try:
                    logger.debug(f"[Scan] Scanning url={url} depth={depth} (attempt {p_attempt}/{max_processing_attempts})")

                    # URL 전처리 및 강화
                    if url:
                        url = url.strip().replace('\\', '/')
                        
                    if not url or not isinstance(url, str):
                        process_success = True # 스킵 처리
                        break
                        
                    if not url.startswith('http'):
                        url = f"https:{url}" if url.startswith('//') else f"https://{url}"

                    # fragment(#...)는 서버 요청에 영향이 없고, 일부 사이트에서 로딩/스크립트 대기로 타임아웃을 유발하므로 제거한다.
                    try:
                        _p = urlparse(url)
                        if _p.fragment:
                            url = urlunparse(_p._replace(fragment=""))
                    except Exception:
                        pass

                    if _is_private_url(url):
                        logger.info("[Scan] 🔒 Dynamic skip (private url) | url=%s", url[:200])
                        process_success = True
                        break

                    url = await _preflight_request(url)

                    # 동시 페이지 수 제한 (Semaphore 사용)
                    async with page_semaphore:
                        page = await safe_goto(url, page_profile)

                        # 1. 페이지 이동 (기본 리트라이 포함)
                        goto_success = False
                        goto_attempts = [
                            {'wait_until': 'domcontentloaded', 'timeout': 30_000},
                            {'wait_until': 'domcontentloaded', 'timeout': 60_000},
                        ]
                        
                        for attempt_idx, opt in enumerate(goto_attempts, 1):
                            try:
                                await page.goto(url, wait_until=opt['wait_until'], timeout=opt['timeout'])
                                await page.wait_for_timeout(MANUAL_POST_GOTO_WAIT_MS)
                                goto_success = True
                                break
                            except PlaywrightTimeoutError:
                                # 마지막 시도에서도 타임아웃이면 해당 URL만 스킵하고 워커는 계속 진행한다.
                                if attempt_idx == len(goto_attempts):
                                    logger.warning("[Scan] page.goto timeout; skip url=%s", url)
                                    goto_success = False
                                    break
                            except Error as e:
                                if is_target_closed_error(e):
                                    await reset_context()
                                    try: 
                                        if page and not page.is_closed(): await page.close()
                                    except: pass
                                    page = await safe_goto(url, page_profile)
                                    continue
                                raise
                        
                        if not goto_success:
                            continue # 다음 p_attempt 시도

                        # 2. 데이터 추출 및 분석 (body 대기, JS 분석, 링크 추출 등)
                        try:
                            await page.wait_for_selector('body', timeout=5000)
                        except Exception: pass

                        # 상세(게시글) 페이지 판별:
                        # 1) URL 기반 빠른 판별 → True면 그대로 사용
                        # 2) 그 외 '?', BOARD_PATTERNS 조건을 만족하는 경우에만 HTML 점수 판별로 보강
                        lowered_url = str(url).lower()
                        # ✅ 추천 적용:
                        # 1) list 페이지는 어떤 경우에도 "상세"로 판정하지 않는다.
                        #    - list HTML에는 날짜/제목 등이 있어 HTML score 기반 로직이 오탐할 수 있음
                        # 2) list 페이지는 즉시 view를 추출/수집하는 경로로 보낸다.
                        is_list_page = _is_list_page_url(url)
                        # ✅ 핵심: 메뉴/컨텐츠(contents.do)는 게시글 상세(view)로 오탐이 매우 잦다.
                        # - 이런 페이지는 첨부가 없고, footer 날짜가 extract_post_date에 잡혀 should_collect_files=True로 남을 수 있음
                        # - 결과적으로 "detail page인데 attachments=0" 로그만 양산하며 진짜 view 처리를 지연시킨다.
                        is_contents_page = ("contents.do" in lowered_url) or ("main/contents.do" in lowered_url)
                        is_post_detail_page = False
                        if (not is_list_page) and (not is_contents_page):
                            try:
                                if is_detail_page_url(url):
                                    is_post_detail_page = True
                                elif ("?" in lowered_url) or any(p in lowered_url for p in BOARD_PATTERNS):
                                    page_content_for_check = await page.content()
                                    is_post_detail_page = is_post_detail_page_from_html(page_content_for_check, url)
                            except Exception:
                                # 판별 실패 시 기존 휴리스틱 fallback
                                is_post_detail_page = any(pattern in lowered_url for pattern in [
                                    'view.do', 'detail.do', 'read.do', 'num=', 'nttid=', 'brddetail.do', 'brdview.do'
                                ])

                        # ✅ list 페이지라면: list HTML에서 view 링크를 추출하여 큐에 넣고,
                        # 상세(view)에서 등록일/작성자/첨부파일을 수집한다.
                        # - query_links_only(max_depth=0)에서도 동작하도록 view는 동일 depth로 enqueue
                        # - list 자체에서 author 추출/첨부 수집을 시도하지 않는다(노이즈/오탐 방지)
                        if is_list_page:
                            try:
                                view_urls = await _expand_list_to_views(
                                    url,
                                    url,
                                    depth=depth,
                                    page_profile=page_profile,
                                    source_list_url=url,
                                )
                            except Exception:
                                view_urls = []
                            if view_urls:
                                logger.info(
                                    "[Scan] Current page is list; enqueue views | list=%s views=%s",
                                    url[:120],
                                    len(view_urls),
                                )
                                # ✅ 요구사항: view가 있으면 즉시 파일 링크 수집까지 진행
                                # 단, scan 워커가 멈추지 않도록 background로 스케줄링한다.
                                try:
                                    t = asyncio.create_task(_schedule_inline_bulk(view_urls, depth, page_profile, url))
                                    _track_task(t)
                                except Exception:
                                    await _process_views_inline_bulk(view_urls, depth, page_profile, source_list_url=url)
                                # 만약 정적 확장에 성공했다면 여기서 루프 종료 (최적화)
                                process_success = True
                                break
                            else:
                                # 정적 확장이 실패(0개)했다면, Playwright DOM 기반 탐색으로 폴백한다 (break 제거)
                                logger.debug(
                                    "[Scan] List link expanded but no views found (static); fallback to dynamic scan | list=%s",
                                    url[:120],
                                )
                                # process_success = True  <-- 제거
                                # break                   <-- 제거
                        
                        extracted_post_date = None
                        extracted_post_date_str = None
                        extracted_author = None
                        extracted_department = None
                        extracted_author_kind = None
                        extracted_author_raw = None
                        extracted_department_raw = None
                        should_collect_files = True
                        attachment_urls: List[str] = []
                        
                        if is_post_detail_page:
                            try:
                                page_content = await page.content()
                                if _is_private_html(page_content):
                                    logger.info("[Scan] 🔒 Dynamic detail skipped (private post) | url=%s", url[:200])
                                    process_success = True
                                    break
                                extracted_post_date = extract_post_date(page_content, url, raw_response_text=page_content)
                                try:
                                    info = extract_author_info_from_html(page_content, url=url)
                                    extracted_author = info.get("author")
                                    extracted_department = info.get("department")
                                    extracted_author_kind = info.get("author_kind")
                                    extracted_author_raw = info.get("author_raw")
                                    extracted_department_raw = info.get("department_raw")
                                    if extracted_author or extracted_department:
                                        msg = f"[Scan] Author/Dept 추출 성공 (Playwright 페이지) | url={url[:100]} author={extracted_author!r} dept={extracted_department!r} kind={extracted_author_kind!r}"
                                        logger.info(msg)
                                        print(msg, flush=True)  # 즉시 확인을 위한 print
                                    else:
                                        logger.debug(f"[Scan] Author/Dept 추출 실패 (Playwright 페이지) | url={url[:100]}")
                                except Exception as e:
                                    logger.debug(f"[Scan] Author/Dept 추출 중 예외 (Playwright 페이지) | url={url[:100]} error={e}")
                                    extracted_author = None
                                    extracted_department = None
                                    extracted_author_kind = None
                                    extracted_author_raw = None
                                    extracted_department_raw = None
                                # 날짜 필터링 복원 + 추가 그레이스(확장) 옵션:
                                # - SCAN_DATE_GATE_EXTRA_DAYS: 페이징 중단 기준을 확장할 추가 일수 (기본 7)
                                # - SCAN_DATE_GATE_INCLUDE_IN_ENQUEUE: 확장 기간을 파일 enqueue(선별)에도 포함할지 여부 (기본 True)
                                should_collect_files = True
                                try:
                                    try:
                                        extra_days = int(os.getenv("SCAN_DATE_GATE_EXTRA_DAYS", "7") or "7")
                                    except Exception:
                                        extra_days = 7
                                    include_in_enqueue = str(os.getenv("SCAN_DATE_GATE_INCLUDE_IN_ENQUEUE", "1")).strip().lower() in (
                                        "1",
                                        "true",
                                        "yes",
                                        "on",
                                    )
                                except Exception:
                                    extra_days = 7
                                    include_in_enqueue = True

                                try:
                                    if extracted_post_date and (start_date or end_date):
                                        effective_start = start_date
                                        if include_in_enqueue and start_date:
                                            try:
                                                effective_start = (start_date - timedelta(days=extra_days)) if isinstance(start_date, datetime) else (start_date - timedelta(days=extra_days))
                                            except Exception:
                                                effective_start = start_date
                                        if not is_date_in_range(extracted_post_date, effective_start, end_date):
                                            should_collect_files = False
                                            logger.info(
                                                f"[Scan] 📅 기간 필터링 제외: {url} ({extracted_post_date.strftime('%Y-%m-%d')}) effective_start={getattr(effective_start, 'isoformat', lambda: str(effective_start))()}"
                                            )
                                except Exception as e:
                                    logger.debug(f"[Scan] 날짜 범위 검사 중 예외, 필터링 보류: {e}")
                                # scan_count는 "기간 통과한 상세 게시글의 첨부파일 URL"에서만 증가하도록 변경됨.
                            except Exception as date_exc:
                                logger.error(f"[Scan] ❌ 날짜 추출 중 오류 (통과): {date_exc}")
                        else:
                            # ✅ 요구사항: author/department 추출은 상세페이지로 판정된 경우에만 수행한다.
                            extracted_author = None
                            extracted_department = None
                            extracted_author_kind = None
                            extracted_author_raw = None
                            extracted_department_raw = None

                        extracted_post_date_str = extracted_post_date.strftime('%Y-%m-%d') if extracted_post_date else None
                        
                        # JS 첨부파일 추출
                        # ✅ 첨부 추출/선별은 "상세(게시글) 페이지"에서만 수행한다.
                        # - contents.do 같은 메뉴/컨텐츠 페이지는 첨부가 없고, 불필요한 추출 시도만 발생한다.
                        # ✅ scan_count 재정의에 맞춰: 기간 통과(또는 필터 비활성) 시에만 첨부파일 후보를 enqueue한다.
                        logger.debug(
                            f"[Scan] 첨부파일 추출 조건 체크 | should_collect_files={should_collect_files} is_post_detail_page={is_post_detail_page} url={url[:100]}"
                        )
                        if should_collect_files and is_post_detail_page:
                            attachment_urls = await _extract_js_attachment_urls(page, url)
                            
                            # ✅ 첨부 URL JSON 파일로 저장
                            if attachment_urls:
                                try:
                                    # job_id 확인 및 로깅
                                    current_job_id = job_id or "default"
                                    logger.info(
                                        f"[Scan] 📝 첨부 URL JSON 저장 시도 | job_id={current_job_id} url={url[:100]} count={len(attachment_urls)}"
                                    )
                                    
                                    # JSON 출력 디렉토리 생성
                                    output_dir = os.path.join(project_root, "output", "attachment_urls")
                                    os.makedirs(output_dir, exist_ok=True)
                                    logger.debug(f"[Scan] JSON 출력 디렉토리: {output_dir}")
                                    
                                    # job_id 기반 파일명 생성
                                    json_filename = f"attachment_urls_{current_job_id}.json"
                                    json_filepath = os.path.join(output_dir, json_filename)
                                    logger.debug(f"[Scan] JSON 파일 경로: {json_filepath}")
                                    
                                    # 기존 데이터 로드 (있는 경우)
                                    attachment_data = []
                                    if os.path.exists(json_filepath):
                                        try:
                                            with open(json_filepath, "r", encoding="utf-8") as f:
                                                attachment_data = json.load(f)
                                            logger.debug(f"[Scan] 기존 JSON 데이터 로드 완료 | 기존 항목 수: {len(attachment_data)}")
                                        except Exception as load_err:
                                            logger.warning(f"[Scan] 기존 JSON 로드 실패: {load_err}")
                                            attachment_data = []
                                    
                                    # 새 첨부 URL 데이터 추가
                                    new_count = 0
                                    for file_url in attachment_urls:
                                        attachment_entry = {
                                            "file_url": file_url,
                                            "source_page": url,
                                            "reg_date": extracted_post_date_str,
                                            "author": extracted_author,
                                            "department": extracted_department,
                                            "author_kind": extracted_author_kind,
                                            "author_raw": extracted_author_raw,
                                            "department_raw": extracted_department_raw,
                                            "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S")
                                        }
                                        # 중복 체크 (file_url 기준)
                                        if not any(entry.get("file_url") == file_url for entry in attachment_data):
                                            attachment_data.append(attachment_entry)
                                            new_count += 1
                                    
                                    logger.debug(f"[Scan] 새로 추가된 항목: {new_count}개, 총 항목: {len(attachment_data)}개")
                                    
                                    # JSON 파일로 저장
                                    with open(json_filepath, "w", encoding="utf-8") as f:
                                        json.dump(attachment_data, f, ensure_ascii=False, indent=2)
                                    
                                    logger.info(
                                        f"[Scan] ✅ 첨부 URL JSON 저장 완료 | job_id={current_job_id} 새 항목={new_count} 총 항목={len(attachment_data)} file={json_filepath}"
                                    )
                                    print(
                                        f"[Scan] ✅ 첨부 URL JSON 저장 완료 | job_id={current_job_id} file={json_filepath}",
                                        flush=True
                                    )
                                except Exception as json_err:
                                    import traceback
                                    error_detail = traceback.format_exc()
                                    logger.error(
                                        f"[Scan] ⚠️ 첨부 URL JSON 저장 실패 | job_id={job_id} error={json_err}\n{error_detail}"
                                    )
                                    print(
                                        f"[Scan] ⚠️ 첨부 URL JSON 저장 실패: {json_err}",
                                        flush=True
                                    )
                            else:
                                logger.debug(f"[Scan] 첨부 URL 없음 | url={url[:100]}")
                            
                            for file_url in attachment_urls:
                                await _enqueue_file_candidate(
                                    file_url,
                                    url,
                                    job_id=job_id,
                                    reg_date=extracted_post_date_str,
                                    author=extracted_author,
                                    department=extracted_department,
                                    author_kind=extracted_author_kind,
                                    author_raw=extracted_author_raw,
                                    department_raw=extracted_department_raw,
                                )
                        else:
                            pass

                        # 링크 추출 및 처리
                        links = await page.query_selector_all("a")
                        # 동적(list) 경로용 날짜 수집 (static 경로의 list_page_item_dates와 유사하게 동작)
                        list_page_item_dates: List[datetime] = []
                        allowed_domain = urlparse(url).netloc
                        
                        # ✅ 첨부 링크 follow/선별도 "상세(게시글) 페이지"에서만 수행한다.
                        if should_collect_files and is_post_detail_page:
                            for a in links:
                                try:
                                    href = await a.get_attribute("href")
                                    if not href or href.startswith("#") or href.startswith("javascript:"): continue
                                    
                                    target_url = urljoin(url, href)
                                    if urlparse(target_url).netloc != allowed_domain: continue
                                    
                                    lowered_target = target_url.lower()
                                    if any(skip in lowered_target for skip in SKIP_DEPTH_PATTERNS): continue
                                    if any(lowered_target.endswith(ext) for ext in STATIC_ASSET_EXTENSIONS): continue

                                    # 제목/날짜 추출 (Evaluate 사용 - 더 안전함)
                                    info = await a.evaluate("""el => {
                                        let h = el.innerText || el.getAttribute('title') || '';
                                        let p = el.closest('tr, li, .b-list-item, div[class*="item"]');
                                        // 목록에서 파일다운로드 링크(fileDown 등)가 별도 앵커로 존재하는 경우가 많아
                                        // 같은 row/container 안의 상세(view.do/detail.do/read.do) 링크를 찾아 source_page로 보강한다.
                                        let detailHref = '';
                                        try {
                                            if (p) {
                                                const cand = p.querySelector('a[href*=\"view.do\"],a[href*=\"detail.do\"],a[href*=\"read.do\"],a[href*=\"brdview\"],a[href*=\"brddetail\"]');
                                                if (cand) detailHref = cand.getAttribute('href') || '';
                                            }
                                        } catch (e) {}
                                        return { name: h.trim(), row_text: p ? p.innerText : '', detail_href: (detailHref || '').trim() };
                                    }""")
                                    
                                    combined_text = f"{info['name']} {info['row_text']}"
                                    item_date = extract_date_from_text(combined_text)
                                    if item_date:
                                        try:
                                            list_page_item_dates.append(item_date)
                                        except Exception:
                                            pass
                                    item_date_str = item_date.strftime('%Y-%m-%d') if item_date else None
                                    row_author = None
                                    try:
                                        row_author = extract_author_from_text(combined_text)
                                        if row_author:
                                            logger.debug(f"[Scan] Author 추출 성공 (목록 row_text) | target_url={target_url[:100]} author={row_author!r} combined_text={combined_text[:150]!r}")
                                    except Exception as e:
                                        logger.debug(f"[Scan] Author 추출 중 예외 (목록 row_text) | target_url={target_url[:100]} error={e}")
                                        row_author = None

                                    # 기간 필터는 Scan 단계에서 처리한다.
                                    # 단, 모든 링크에 적용하면(main.do 같은 네비게이션) 불필요한 탈락/로그가 발생하므로
                                    # "게시글/게시판성 링크"로 보이는 경우에만 적용한다.
                                    detail_hints = (
                                        'view.do', 'detail.do', 'read.do', 'num=', 'nttid=', 'brddetail.do', 'brdview.do'
                                    )
                                    is_boardish_link = (
                                        any(p in lowered_target for p in BOARD_PATTERNS)
                                        or any(h in lowered_target for h in detail_hints)
                                        or is_detail_page_url(target_url)
                                    )
                                    if (start_date or end_date) and is_boardish_link:
                                        if not item_date:
                                            # 메뉴/컨텐츠 페이지에서 '상세(view.do)' 링크를 발견한 경우,
                                            # 목록에서 날짜를 못 뽑아도 상세 페이지로 들어가서 파일/날짜를 추출해야 한다.
                                            if is_detail_page_url(target_url):
                                                logger.debug(
                                                    "[Scan] Boardish link missing reg_date but is detail page -> will follow | source=%s target=%s",
                                                    url,
                                                    target_url,
                                                )
                                            # 목록(list.do) 링크를 발견했는데 날짜를 못 뽑는 경우:
                                            # list 페이지에서 view 링크를 추출해 상세로 진입 후(=게시물로 판단) 파일 수집을 진행한다.
                                            elif _is_list_page_url(target_url):
                                                try:
                                                    view_urls = await _expand_list_to_views(
                                                        target_url,
                                                        url,
                                                        depth=depth,
                                                        page_profile=page_profile,
                                                        source_list_url=target_url,
                                                    )
                                                except Exception:
                                                    view_urls = []
                                                if not view_urls:
                                                    logger.debug(
                                                        "[Scan] List link has no view links; skip | source=%s list=%s",
                                                        url,
                                                        target_url,
                                                    )
                                                    continue
                                                logger.info(
                                                    "[Scan] List link expanded to views | source=%s list=%s views=%s",
                                                    url[:120],
                                                    target_url[:120],
                                                    len(view_urls),
                                                )
                                                # view들을 같은 depth로 강제 follow (query_links_only/max_depth=0에서도 동작)
                                                for idx_v, vurl in enumerate(view_urls):
                                                    if vurl in visited_urls:
                                                        continue
                                                    # ✅ 즉시 확인 모드: 상위 N개 view는 바로 처리 시도
                                                    if INLINE_VIEW_PROCESSING and idx_v < INLINE_VIEW_PROCESSING_MAX:
                                                        ok = await _process_view_inline(vurl, depth, page_profile)
                                                        if ok:
                                                            continue
                                                    try:
                                                        progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                                                    except Exception:
                                                        pass
                                                    await in_queue.put({'url': vurl, 'depth': depth, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date})
                                                continue
                                            else:
                                                logger.debug(
                                                    "[Scan] Skip boardish link (missing reg_date) | source=%s target=%s",
                                                    url,
                                                    target_url,
                                                )
                                                continue
                                        target_dt = item_date.date() if hasattr(item_date, "date") else item_date
                                        if not is_date_in_range(target_dt, start_date, end_date):
                                            logger.info(
                                                "[Scan] 📅 Skip boardish link (out of range) | reg_date=%s target=%s",
                                                item_date_str,
                                                target_url,
                                            )
                                            continue

                                    # ✅ 등록일을 못 뽑아도 상세페이지(view.do 등)라면 먼저 진입해서
                                    # 상세 HTML에서 등록일을 다시 추출해야 한다.
                                    # - 특히 query_links_only 모드(max_depth=0)에서는 일반 링크 확장을 막지만,
                                    #   상세 페이지는 같은 depth로 "강제 follow"하여 첨부파일 수집을 가능하게 한다.
                                    if (start_date or end_date) and (not item_date) and is_detail_page_url(target_url):
                                        if target_url not in visited_urls:
                                            try:
                                                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                                            except Exception:
                                                pass
                                            # depth를 증가시키지 않는다(확장 억제). visited_urls가 중복 진입은 막는다.
                                            await in_queue.put({'url': target_url, 'depth': depth, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date, 'referer': url})
                                        continue

                                    # ✅ 날짜 필터가 없거나(혹은 통과했거나), 목록(list.do) 링크라면
                                    #    "뷰 확장"을 통해 게시글을 수집해야 한다.
                                    #    특히 main.do -> list.do 탐색 시 depth 문제로 스킵되는 것을 방지.
                                    if _is_list_page_url(target_url) and target_url not in visited_urls:
                                        try:
                                            view_urls = await _expand_list_to_views(
                                                target_url,
                                                url,
                                                depth=depth,
                                                page_profile=page_profile,
                                                source_list_url=target_url,
                                            )
                                        except Exception:
                                            view_urls = []
                                        
                                        if view_urls:
                                            logger.info(
                                                "[Scan] List link expanded to views (Auto) | source=%s list=%s views=%s",
                                                url[:120],
                                                target_url[:120],
                                                len(view_urls),
                                            )
                                            # ✅ 요구사항: view가 있으면 즉시 파일 링크 수집까지 진행
                                            # 단, scan 워커가 멈추지 않도록 background로 스케줄링한다.
                                            try:
                                                t = asyncio.create_task(_schedule_inline_bulk(view_urls, depth, page_profile, target_url))
                                                _track_task(t)
                                            except Exception:
                                                await _process_views_inline_bulk(view_urls, depth, page_profile, source_list_url=target_url)
                                            continue
                                        else:
                                            # 뷰가 없으면 그냥 일반 링크 처리(depth check 등)로 넘어감
                                            logger.debug(
                                                "[Scan] List link has no views (Auto); Fallback to normal traverse | list=%s",
                                                target_url[:120],
                                            )

                                    if _looks_like_file(target_url) or _looks_like_download_handler(target_url) or _extract_filename_candidate(info['name']):
                                        # 작성자(author)는 날짜 추출 성공/실패와 무관하게, 확보된 값이 있으면 전달한다.
                                        # - 상세 페이지에서 author만 잡히고 date가 실패하는 케이스가 실제로 존재함
                                        author_for_item = extracted_author or row_author
                                        if author_for_item:
                                            msg = f"[Scan] Author 최종 설정 (파일 후보) | target_url={target_url[:100]} extracted_author={extracted_author!r} row_author={row_author!r} final={author_for_item!r}"
                                            logger.info(msg)
                                            print(msg, flush=True)  # 즉시 확인을 위한 print
                                        # ✅ source_page 보강:
                                        # 목록 페이지에서 파일 링크를 발견한 경우, 같은 row의 view.do 링크를 source_page로 넣어야
                                        # DB 저장 단계에서 author/department 복구(fallback)가 가능해진다.
                                        source_for_item = url
                                        try:
                                            dh = (info.get("detail_href") or "").strip()
                                            if dh:
                                                source_for_item = urljoin(url, dh)
                                        except Exception:
                                            source_for_item = url
                                        await _enqueue_file_candidate(
                                            target_url,
                                            source_for_item,
                                            job_id=job_id,
                                            display_name=info['name'] or None,
                                            reg_date=extracted_post_date_str or item_date_str,
                                            author=author_for_item,
                                            department=extracted_department,
                                            author_kind=extracted_author_kind,
                                            author_raw=extracted_author_raw,
                                            department_raw=extracted_department_raw,
                                        )
                                        continue

                                    if depth < max_depth and target_url not in visited_urls:
                                        progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': 1})
                                        await in_queue.put({'url': target_url, 'depth': depth + 1, 'page_profile': page_profile, 'job_id': job_id, 'chat_bot_id': chat_bot_id, 'db_name': db_name, 'start_date': start_date, 'end_date': end_date, 'referer': url})
                                except Exception: continue # 개별 링크 처리 실패는 무시

                        # 동적 처리 후: list 페이지에서 추출한 날짜들로 date-gate 적용 (static과 동등한 동작)
                        try:
                            if (not is_list_page_static) and (start_date or end_date) and list_page_item_dates:
                                try:
                                    date_gate_enabled = os.getenv("SCAN_LIST_STOP_ON_DATE_EXCEEDED", "1") == "1"
                                except Exception:
                                    date_gate_enabled = True
                                if date_gate_enabled:
                                    try:
                                        threshold = int(os.getenv("SCAN_LIST_DATE_STOP_THRESHOLD", "2") or "2")
                                    except Exception:
                                        threshold = 2
                                    threshold = max(1, min(threshold, 10))
                                    try:
                                        start_cmp = start_date.date() if isinstance(start_date, datetime) else start_date
                                    except Exception:
                                        start_cmp = start_date
                                    # 확장(그레이스) 설정: 페이지네이션 중단 기준을 start_date에서 extra_days만큼 확장하여 여유 탐색
                                    try:
                                        extra_days = int(os.getenv("SCAN_DATE_GATE_EXTRA_DAYS", "7") or "7")
                                    except Exception:
                                        extra_days = 7
                                    try:
                                        include_extra_gate = str(os.getenv("SCAN_DATE_GATE_INCLUDE_EXTRA", "1")).strip().lower() in ("1", "true", "yes", "on")
                                    except Exception:
                                        include_extra_gate = True
                                    if start_cmp and include_extra_gate:
                                        try:
                                            start_cmp_extended = start_cmp - timedelta(days=extra_days)
                                        except Exception:
                                            start_cmp_extended = start_cmp
                                    else:
                                        start_cmp_extended = start_cmp
                                    if start_cmp:
                                        try:
                                            page_dates = [
                                                (d.date() if isinstance(d, datetime) else d)
                                                for d in list_page_item_dates
                                                if d
                                            ]
                                            if page_dates:
                                                max_dt = max(page_dates)
                                                _ctx = _scan_ctx_var.get()
                                                _jid = str((_ctx or {}).get("job_id") or "default")
                                                date_gate_cache = _list_page_date_gate_by_job.setdefault(_jid, {})
                                                # list cache key normalize: remove paging params
                                                try:
                                                    p = urlparse(url)
                                                    pairs = parse_qsl(p.query or "", keep_blank_values=True)
                                                    filtered = [(k, v) for (k, v) in pairs if k.lower() not in ("pageindex", "pageno", "page", "curpage", "page_no", "page_index")]
                                                    filtered.sort()
                                                    q = urlencode(filtered, doseq=True)
                                                    scheme = (p.scheme or "https").lower()
                                                    netloc = (p.netloc or "").lower()
                                                    if netloc.startswith("www."):
                                                        netloc = netloc[4:]
                                                    list_cache_key_dyn = urlunparse((scheme, netloc, p.path or "", "", q, ""))
                                                except Exception:
                                                    list_cache_key_dyn = str(url)
                                                stop_set = _list_page_date_stop_by_job.setdefault(_jid, set())
                                                # 비교는 확장된 기준(start_cmp_extended)을 사용
                                                if max_dt < start_cmp_extended:
                                                    count = date_gate_cache.get(list_cache_key_dyn, 0) + 1
                                                    date_gate_cache[list_cache_key_dyn] = count
                                                    if count >= threshold:
                                                        stop_set.add(list_cache_key_dyn)
                                                        logger.warning(
                                                            "[Scan][Pagination] Dynamic list date-gate stop (out-of-range repeated) | list=%s count=%s max_dt=%s start_date=%s",
                                                            str(url)[:160],
                                                            count,
                                                            max_dt.strftime("%Y-%m-%d") if hasattr(max_dt, "strftime") else str(max_dt),
                                                            start_cmp_extended.strftime("%Y-%m-%d") if hasattr(start_cmp_extended, "strftime") else str(start_cmp_extended),
                                                        )
                                                        try:
                                                            print("finish", flush=True)
                                                        except Exception:
                                                            pass
                                                else:
                                                    if date_gate_cache.get(list_cache_key_dyn):
                                                        date_gate_cache[list_cache_key_dyn] = 0
                                        except Exception:
                                            pass
                        except Exception:
                            pass
                        logger.debug(
                            "[Scan] Dynamic parsed | url=%s depth=%s attachments=%s anchors=%s should_collect_files=%s extracted_post_date=%s",
                            url,
                            depth,
                            len(attachment_urls),
                            len(links),
                            should_collect_files,
                            extracted_post_date_str,
                        )

                        process_success = True
                        break # 성공 시 p_attempt 루프 탈출

                except Error as e:
                    if is_target_closed_error(e) and p_attempt < max_processing_attempts:
                        logger.warning(f"[Scan] Target closed during processing (attempt {p_attempt}); retrying: {url}")
                        await reset_context()
                        continue
                    raise
                except Exception as e:
                    # Playwright 컨텍스트 생성 실패(브라우저 크래시/연결 끊김) 케이스는
                    # 해당 URL만 정적으로 처리로 폴백하여 크롤링 전체가 "멈춘 것처럼" 보이는 현상을 줄인다.
                    msg = str(e)
                    if "Failed to create browser context after retries" in msg or "Browser context is None" in msg:
                        logger.warning("[Scan] Playwright context unavailable; fallback to static parse | url=%s err=%s", url, msg)
                        try:
                            await reset_context()
                        except Exception:
                            pass
                        try:
                            handled = await _process_static_page(url, depth, {"is_dynamic": False}, referer=referer)
                            if handled:
                                process_success = True
                                break
                        except Exception as static_exc:
                            logger.debug("[Scan] Static fallback failed | url=%s err=%s", url, static_exc)
                    logger.exception(f"[Scan] Error processing {url}: {e}")
                    break
                finally:
                    if page:
                        try: await page.close()
                        except: pass
            
            # 📊 Progress: URL 처리 시도 종료
            if url_item is not None:
                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': -1})
                in_queue.task_done()
        
        except asyncio.CancelledError:
            logger.debug("[Scan] Worker cancelled")
            # region agent log
            try:
                _log_path = os.getenv(
                    "AGENT_DEBUG_LOG_PATH",
                    os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
                )
                try:
                    os.makedirs(os.path.dirname(_log_path), exist_ok=True)
                except Exception:
                    pass
                with open(_log_path, "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                        "hypothesisId": "H_scan_worker_cancelled",
                        "location": "core/crawler/workers/scan.py:scan_worker",
                        "message": "scan_worker_cancelled",
                        "data": {},
                        "timestamp": int(time.time() * 1000),
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # endregion
            break
        
        except Exception as e:
            logger.exception("[Scan] Critical error in worker loop: %s", e)
            # region agent log
            try:
                _log_path = os.getenv(
                    "AGENT_DEBUG_LOG_PATH",
                    os.path.abspath(os.path.join(project_root, ".cursor", "debug.log")),
                )
                try:
                    os.makedirs(os.path.dirname(_log_path), exist_ok=True)
                except Exception:
                    pass
                with open(_log_path, "a", encoding="utf-8") as _f:
                    _f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                        "hypothesisId": "H_scan_worker_exception",
                        "location": "core/crawler/workers/scan.py:scan_worker",
                        "message": "scan_worker_exception",
                        "data": {"err": str(e)[:200]},
                        "timestamp": int(time.time() * 1000),
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # endregion
    
    await reset_context()
