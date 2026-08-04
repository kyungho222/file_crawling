# backend/router.py
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
import asyncio
import json
import inspect
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from pydantic import ValidationError
import time
import os

logger = logging.getLogger("backend.router")

def _agent_debug_log(hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    return

from backend.file.integrated_workflow import IntegratedWorkflow, WorkflowState
from backend.board.board_content_workflow import BoardContentWorkflow
from backend.shared.stop_service import stop_active_crawl
from config.settings import settings
from db.db_redis import get_redis, describe_redis_connection
from db.crawl_db_manager import update_crawling_log_counters
from backend.shared.redis_sse_service import send_message_to_redis_sse, get_last_publish_meta
from backend.shared.sse_utils import format_sse
from backend.shared.board_header import CrawlRequest as HeaderCrawlRequest, CrawlResponse, crawl_header as header_crawl_handler
from utils.url import ensure_url_scheme
from config.settings import get_storage_domain_for_db_name
from utils.runtime_flags import is_no_limits_mode
import re
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from backend.shared.crawler_state import crawler_state
from backend.board.board_endpoints import router as board_router
from backend.file.file_endpoints import router as file_router
from backend.shared.crawl_start import router as session_router
from backend.shared.db_bridge import router as db_bridge_router
from backend.shared.sse_publish_queue import (
    ensure_worker_started as ensure_shared_sse_worker_started,
    enqueue_sse_message as enqueue_shared_sse_message,
    debug_sse_publish_queue as debug_shared_sse_publish_queue,
)
from core.crawler.queues import dispose_job_queues
from core.crawler.global_pool import get_global_worker_pool
from core.crawler.workers.download import cancel_download_worker_activity

try:
    import requests  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

_NAV_CONTAINER_TAGS = ("nav", "header", "footer", "aside")
_NAV_CONTAINER_HINTS = (
    "menu",
    "gnb",
    "lnb",
    "snb",
    "sidebar",
    "side",
    "sidemenu",
    "leftmenu",
    "rightmenu",
    "nav",
    "header",
    "footer",
    "topmenu",
    "quick",
)


def _is_nav_or_sidebar_anchor(a) -> bool:
    try:
        for parent in a.parents:
            name = (getattr(parent, "name", "") or "").lower()
            if name in _NAV_CONTAINER_TAGS:
                return True
            try:
                pid = parent.get("id") or ""
            except Exception:
                pid = ""
            try:
                classes = parent.get("class") or []
            except Exception:
                classes = []
            if isinstance(classes, str):
                classes = [classes]
            for token in [pid, *classes]:
                if not token:
                    continue
                lt = str(token).lower()
                if any(h in lt for h in _NAV_CONTAINER_HINTS):
                    return True
    except Exception:
        return False
    return False

try:
    from config import Config  # type: ignore
except Exception:  # pragma: no cover - Config 누락 대비
    Config = None  # type: ignore


router = APIRouter()
router.include_router(board_router)
router.include_router(session_router)
router.include_router(file_router)
router.include_router(db_bridge_router)

# f1_dev 브리지는 이 router가 /Ai_Pro_filecrawler prefix로 등록된 뒤 사용된다.
# Debug: router module loaded (no file writes)
try:
    logging.getLogger("backend.router").debug(
        json.dumps(
            {"sessionId": "debug-session", "runId": "run1", "hypothesisId": "H_ROUTER_LOADED", "location": "backend/router.py:loaded", "message": "router_loaded", "data": {}, "timestamp": int(time.time() * 1000)},
            ensure_ascii=False,
        )
    )
except Exception:
    pass


def _env_bool(name: str, default: str = "1") -> bool:
    try:
        return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"


async def _force_terminate_job_after_finish(*, workflow: Any, job_id: str, db_name: str) -> None:
    """
    작업 종료 로그 직후, job 관련 리소스를 가능한 한 즉시 해제/중단한다.
    (router.py 내 run_workflow_task 경로)
    """
    try:
        ret = workflow.stop() if hasattr(workflow, "stop") else None
        if inspect.isawaitable(ret):
            try:
                await asyncio.wait_for(ret, timeout=1.5)
            except Exception:
                pass
    except Exception:
        pass

    try:
        wm = getattr(workflow, "worker_manager", None)
        if wm is not None and hasattr(wm, "stop"):
            ret = wm.stop(graceful=False)  # type: ignore[call-arg]
            if inspect.isawaitable(ret):
                try:
                    await asyncio.wait_for(ret, timeout=2.0)
                except Exception:
                    pass
    except Exception:
        pass

    try:
        for attr in ("_post_download_tasks", "_trigger_tasks"):
            s = getattr(workflow, attr, None)
            if isinstance(s, set) and s:
                for t in list(s):
                    try:
                        if isinstance(t, asyncio.Task) and not t.done():
                            t.cancel()
                    except Exception:
                        pass
    except Exception:
        pass

    for attr in ("_worker_manager_start_task", "_stop_grace_enforcer_task"):
        try:
            t = getattr(workflow, attr, None)
            if isinstance(t, asyncio.Task) and not t.done():
                t.cancel()
        except Exception:
            pass

    try:
        if bool(getattr(workflow, "use_global_pool", False)):
            get_global_worker_pool().unregister_job(job_id)
    except Exception:
        pass

    try:
        key = getattr(workflow, "_job_queue_key", None) or getattr(workflow, "job_id", None) or job_id
        await dispose_job_queues(str(key))
    except Exception:
        pass

    try:
        crawler_state.workflows.pop(job_id, None)
        prev_status = crawler_state.job_history.get(job_id, {}).get("status")
        if prev_status not in {"failed_to_start", "creation_failed"}:
            crawler_state.record_history(job_id, "cleaned", "force_cleanup_after_finish", db_name, chat_bot_id=getattr(workflow, "chat_bot_id", None))
    except Exception:
        pass

# ==================== start_urls pre-expand (list -> view) ====================
def _is_list_page_url(u: str) -> bool:
    try:
        lu = (u or "").lower()
    except Exception:
        lu = str(u).lower()
    if "list.do" in lu or "list.asp" in lu or "list.jsp" in lu:
        return True
    try:
        path = (urlparse(u).path or "").lower()
        return path.endswith(("list.do", "list.asp", "list.jsp"))
    except Exception:
        return False


def _normalize_list_cache_key(u: str) -> str:
    """
    같은 게시판 list URL 캐시 키 정규화 (pageIndex/pageNo/page 등 페이징 파라미터 제거).

    주의: 이 함수는 캐시 키 정규화를 위해 페이징 파라미터를 제거합니다.
    실제 페이징 처리는 _expand_list_to_views_router 등의 다른 로직에서 수행됩니다.
    페이징 파라미터가 포함된 URL도 같은 게시판으로 인식하여 중복 방지 및 그룹화에 사용됩니다.
    """
    try:
        p = urlparse(u)
        pairs = parse_qsl(p.query or "", keep_blank_values=True)
        # 페이지네이션 파라미터 제거 (캐시 키 정규화용)
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


def _canonicalize_url(u: str) -> str:
    """방문 중복 방지를 위한 canonical URL (query 정렬/fragment 제거)."""
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
        new_pairs = sorted(qd.items(), key=lambda x: x[0])
        q = urlencode(new_pairs, doseq=True)
        return urlunparse((p.scheme or "https", p.netloc, p.path, "", q, ""))
    except Exception:
        return base_page_url


def _guess_page_param(page_url: str, soup_obj) -> str:
    try:
        qkeys = {k.lower() for (k, _v) in parse_qsl(urlparse(page_url).query or "", keep_blank_values=True)}
    except Exception:
        qkeys = set()
    for cand in ("pageindex", "pageno", "page", "curpage"):
        if cand in qkeys:
            return "pageIndex" if cand == "pageindex" else ("pageNo" if cand == "pageno" else ("page" if cand == "page" else "curPage"))
    # hidden input 힌트
    try:
        for nm in ("pageIndex", "pageNo", "page", "curPage"):
            tag = soup_obj.find("input", attrs={"name": nm})
            if tag is not None:
                return nm
    except Exception:
        pass
    return "pageIndex"


async def _fetch_static_html(url: str) -> Optional[str]:
    if not requests:
        return None

    try:
        # pre-expand는 "시작 지연"을 만들면 안 되므로 기본 타임아웃을 짧게 둔다.
        timeout_sec = float(os.getenv("START_URLS_PREEXPAND_REQUEST_TIMEOUT_SEC", "3") or "3")
    except Exception:
        timeout_sec = 3.0

    def _req() -> str:
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout_sec,
            allow_redirects=True,
        )
        r.raise_for_status()
        return r.text

    try:
        return await asyncio.to_thread(_req)
    except Exception:
        return None


async def _expand_list_to_views_router(list_url: str) -> List[str]:
    """
    start_urls 단계에서 list 페이지를 view URL들로 '미리' 확장한다.
    - query_links_only(max_depth=0) 모드에서도 view 중심으로 시작하도록 안정성/정확도 향상
    - 실패 시 빈 리스트 반환(기존 로직 fallback)
    """
    if not list_url or not _is_list_page_url(list_url):
        return []
    if not BeautifulSoup:
        return []

    # NOTE:
    # 사용자 요청에 따라 start_urls pre-expand 단계의 "최대 페이지/최대 view 수" 상한을 제거한다.
    # - 종료 조건: pages_seen + pages_to_visit(더 이상 신규 페이지가 없으면 종료)

    cache_key = _normalize_list_cache_key(list_url)
    # 간단 캐시(함수 스코프 단일 호출 내만): 상위에서 중복 호출을 막아준다.

    pages_seen: set[str] = set()
    pages_to_visit: List[str] = [list_url]
    view_seen: set[str] = set()
    views: List[str] = []

    while pages_to_visit:
        page_url = pages_to_visit.pop(0)
        canon_page = _canonicalize_url(page_url)
        if canon_page in pages_seen:
            continue
        # 같은 게시판(list) 내 페이지만 follow
        if _normalize_list_cache_key(page_url) != cache_key:
            continue
        pages_seen.add(canon_page)

        html = await _fetch_static_html(page_url)
        if not html:
            continue

        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            continue

        # 1) view 링크 추출 (view.do/detail.do/read.do + nttId/num)
        try:
            # 검색 루트: 공용 헬퍼로 결정
            try:
                from backend.shared.content_scope import select_search_root  # local import to avoid cycle
                search_root = select_search_root(soup, base_url=page_url)
            except Exception:
                search_root = soup

            for a in search_root.find_all("a", href=True):
                if _is_nav_or_sidebar_anchor(a):
                    continue
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                lh = href.lower()
                if lh.startswith("javascript:"):
                    continue
                if ("view.do" in lh or "detail.do" in lh or "read.do" in lh) and ("nttid=" in lh or "num=" in lh):
                    full = urljoin(page_url, href)
                    if full in view_seen:
                        continue
                    view_seen.add(full)
                    views.append(full)
        except Exception:
            pass

        # 2) href 기반 pagination 링크(list + page param)
        try:
            # 동일한 search_root 사용 (본문 내부로 제한)
            for a in search_root.find_all("a", href=True):
                if _is_nav_or_sidebar_anchor(a):
                    continue
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                lh = href.lower()
                if lh.startswith("javascript:"):
                    continue
                if ("list.do" in lh or "list.asp" in lh or "list.jsp" in lh) and any(k in lh for k in ("pageindex=", "pageno=", "page=", "curpage=")):
                    full = urljoin(page_url, href)
                    if _normalize_list_cache_key(full) != cache_key:
                        continue
                    cfull = _canonicalize_url(full)
                    if cfull not in pages_seen and cfull not in map(_canonicalize_url, pages_to_visit):
                        pages_to_visit.append(full)
        except Exception:
            pass

        # 3) JS 기반 pagination (egov)
        js_pages: set[int] = set()
        try:
            for tag in soup.find_all(["a", "button", "span"]):
                for attr in ("href", "onclick"):
                    v = (tag.get(attr) or "").strip()
                    if not v:
                        continue
                    if tag.name == "a" and _is_nav_or_sidebar_anchor(tag):
                        continue
                    m = re.search(r"fn_egov_link_page\s*\(\s*['\"]?(\d+)['\"]?\s*\)", v, re.IGNORECASE)
                    if not m:
                        m = re.search(r"link_page\s*\(\s*['\"]?(\d+)['\"]?\s*\)", v, re.IGNORECASE)
                    if m:
                        try:
                            js_pages.add(int(m.group(1)))
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
                cfull = _canonicalize_url(full)
                if cfull not in pages_seen and cfull not in map(_canonicalize_url, pages_to_visit):
                    pages_to_visit.append(full)

    # dedupe (순서 유지)
    uniq: List[str] = []
    seen: set[str] = set()
    for v in views:
        if v in seen:
            continue
        seen.add(v)
        uniq.append(v)
    return uniq


async def _expand_query_links_to_start_urls(query_urls: List[str]) -> List[str]:
    """
    query_links를 start_urls로 사용할 때, list URL들을 view URL로 미리 확장한다.
    - 확장에 성공하면 view 중심 start_urls를 반환
    - 실패/결과 없음이면 원본 query_urls를 그대로 반환
    """
    if not query_urls:
        return []
    # requests/bs4 없으면 그대로
    if not requests or not BeautifulSoup:
        return query_urls

    # 중복 제거(순서 유지)
    ordered: List[str] = []
    seen0: set[str] = set()
    for u in query_urls:
        if not u:
            continue
        if u in seen0:
            continue
        seen0.add(u)
        ordered.append(u)

    list_urls = [u for u in ordered if _is_list_page_url(u)]
    non_list_urls = [u for u in ordered if not _is_list_page_url(u)]

    # list가 거의 없으면 굳이 확장하지 않는다.
    if not list_urls:
        return ordered

    expanded_views: List[str] = []
    # list별 중복 확장 방지
    list_cache: Dict[str, List[str]] = {}
    t0 = time.time()
    # ✅ 핵심: pre-expand는 "best effort"여야 한다.
    # - 너무 오래 걸리면 크롤링 자체가 시작도 못하므로 시간 예산으로 중단하고 fallback한다.
    # NOTE:
    # 사용자 요청에 따라 query_links -> start_urls 확장의 "시간 예산/대표 list 개수" 제한을 제거한다.
    try:
        per_list_timeout_sec = float(os.getenv("START_URLS_PREEXPAND_PER_LIST_TIMEOUT_SEC", "2.5") or "2.5")
    except Exception:
        per_list_timeout_sec = 2.5

    # unique list key 기준으로 상한 적용 (같은 board list 중복 확장 방지)
    list_keys_in_order: List[str] = []
    key_to_url: Dict[str, str] = {}
    for lu0 in list_urls:
        k0 = _normalize_list_cache_key(lu0)
        if k0 in key_to_url:
            continue
        key_to_url[k0] = lu0
        list_keys_in_order.append(k0)

    limited_keys = list_keys_in_order

    for idx, key in enumerate(limited_keys, 1):

        lu = key_to_url.get(key) or ""
        key = _normalize_list_cache_key(lu)
        if key in list_cache:
            expanded_views.extend(list_cache[key])
            continue
        try:
            views = await asyncio.wait_for(_expand_list_to_views_router(lu), timeout=per_list_timeout_sec)
        except asyncio.TimeoutError:
            views = []
        list_cache[key] = views
        expanded_views.extend(views)

    # view 결과가 있으면 non_list_urls + expanded_views로 시작(순서/중복 제거)
    if expanded_views:
        final: List[str] = []
        seenf: set[str] = set()
        # 안전성/정확도: 확장되지 않은 list도 그대로 포함하여 scan 단계의 list→view fallback이 동작하게 한다.
        for u in (non_list_urls + expanded_views + list_urls):
            if not u or u in seenf:
                continue
            seenf.add(u)
            final.append(u)
        return final

    return ordered

# ==================== SSE publish 우선순위 큐(고우선/코얼레싱) ====================
#
# 목적:
# - 진행률(SSE/Redis PubSub) 발행은 "실시간성"이 핵심이므로, 서버의 다른 작업보다
#   스케줄링 상 앞에 오도록 전용 워커로 처리한다.
# - create_task 난사로 이벤트루프가 바빠지는 현상을 줄이기 위해 job_id별로 최신 메시지만 유지(coalesce).
#
# 동작:
# - enqueue 시: job_id별 최신 payload를 저장하고, 큐에는 job_id 토큰만 1개 넣는다.
# - worker가 token을 꺼내면: 최신 payload 1회 발행(그 사이 갱신된 값도 포함).
#
_sse_publish_queue: "asyncio.PriorityQueue[tuple[int, float, str]]" = asyncio.PriorityQueue()
_sse_latest_by_job: Dict[str, Dict[str, Any]] = {}
_sse_latest_db_by_job: Dict[str, str] = {}
_sse_latest_source_by_job: Dict[str, str] = {}
_sse_job_enqueued: set[str] = set()
_sse_worker_task: Optional[asyncio.Task] = None
_sse_worker_lock = asyncio.Lock()
_sse_worker_heartbeat_ts: float = 0.0
_sse_last_published_ts_by_job: Dict[str, float] = {}


async def _ensure_sse_worker_started() -> None:
    """라우터 startup 시점에 SSE publish 워커를 1회만 기동."""
    global _sse_worker_task
    async with _sse_worker_lock:
        if _sse_worker_task and not _sse_worker_task.done():
            return
        _sse_worker_task = asyncio.create_task(_sse_publish_worker(), name="sse-publish-worker")


def enqueue_sse_message(job_id: str, payload: Dict[str, Any], db_name: str, source: str, priority: int = 0) -> None:
    """
    SSE 발행 요청을 큐에 넣는다(고우선/코얼레싱).
    priority: 숫자가 낮을수록 우선(0이 최우선).
    """
    if not job_id:
        return
    # 워커가 죽었거나 아직 startup 전에 enqueue가 들어오는 케이스 대비(비동기 fire-and-forget)
    try:
        asyncio.get_event_loop().create_task(_ensure_sse_worker_started())
    except Exception:
        pass
    # 최신 payload 저장 (job 단위로 1개만 유지)
    _sse_latest_by_job[job_id] = payload
    _sse_latest_db_by_job[job_id] = db_name
    _sse_latest_source_by_job[job_id] = source
    if job_id in _sse_job_enqueued:
        return
    _sse_job_enqueued.add(job_id)
    _sse_publish_queue.put_nowait((int(priority), time.time(), job_id))


async def _sse_publish_worker() -> None:
    """SSE 발행 전용 워커. 가능한 빨리(=다른 작업보다) publish를 처리한다."""
    while True:
        try:
            global _sse_worker_heartbeat_ts
            _sse_worker_heartbeat_ts = time.time()
            priority, ts, job_id = await _sse_publish_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.01)
            continue

        try:
            payload = _sse_latest_by_job.pop(job_id, None)
            db_name = _sse_latest_db_by_job.pop(job_id, None)
            source = _sse_latest_source_by_job.pop(job_id, None) or "queued"
            _sse_job_enqueued.discard(job_id)
            if payload and db_name:
                await _send_sse_message(job_id, payload, db_name, source)
                _sse_last_published_ts_by_job[job_id] = time.time()
        except Exception as exc:
            pass
        finally:
            try:
                _sse_publish_queue.task_done()
            except Exception:
                pass


@router.on_event("startup")
async def _router_startup() -> None:
    # SSE publish 워커를 미리 올려서 초기 요청부터 지연이 없게 한다.
    await _ensure_sse_worker_started()
    # 새 모듈(backend/sse_publish_queue.py) 기반 워커도 같이 기동 (progress/runner 경로에서 사용)
    await ensure_shared_sse_worker_started()

# NOTE: crawler_state는 backend/crawler_state.py에서 단일 인스턴스로 관리한다.


def _swallow_task_exception(task: asyncio.Task, *, label: str) -> None:
    """
    fire-and-forget 태스크의 예외를 회수하여 'Future exception was never retrieved' 경고를 방지.
    - Playwright TargetClosedError 등 종료 레이스에서 흔한 예외는 DEBUG로만 무시한다.
    """
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except Exception as e:
        return
    if not exc:
        return
    msg = str(exc)
    if "TargetClosedError" in msg or "Target page, context or browser has been closed" in msg:
        return

# ==================== 요청 분석 유틸 ====================

def _bool_from_payload(value: Any) -> bool:
    """다양한 표현식을 불리언으로 변환"""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "y", "yes", "on"}
    return False


def _detect_board_crawl(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    sample 모듈의 게시판 탐지 로직을 단순화하여 적용.
    - url_filter == 'Q'
    - URL 내 '?' 포함
    - board_mode/crawl_mode 값
    """
    reasons: List[str] = []
    content_type = (payload.get("content_type") or "url").strip().lower()
    if content_type != "url":
        return {"mode": "file", "is_board": False, "reasons": ["content_type!=url"]}

    url_filter = (payload.get("url_filter") or "").strip().upper()
    if url_filter == "Q":
        reasons.append("url_filter=Q")

    if _bool_from_payload(payload.get("board_mode")):
        reasons.append("board_mode flag")

    crawl_mode = (payload.get("crawl_mode") or "").strip().lower()
    if crawl_mode == "crawling":
        reasons.append("crawl_mode=crawling")

    contents = payload.get("contents") or []
    if isinstance(contents, list):
        for idx, item in enumerate(contents):
            if isinstance(item, str) and "?" in item:
                reasons.append(f"contents[{idx}] contains '?'")
                break

    is_board = len(reasons) > 0
    mode = "board" if is_board else "file"
    if not reasons:
        reasons.append("default:file")

    return {
        "mode": mode,
        "is_board": is_board,
        "reasons": reasons,
    }

# ==================== 유틸리티 ====================

async def _resolve_db_name(job_id: str, provided: Optional[str] = None) -> Optional[str]:
    """요청에 account_name이 없을 때 Redis 메타데이터/상태 키를 통해 DB명을 추론한다."""
    try:
        redis = await get_redis()
    except Exception as exc:
        return provided

    meta_key = f"job_meta:{job_id}"
    try:
        meta = await redis.hgetall(meta_key)
        if meta:
            raw_db = meta.get("dbname") or meta.get(b"dbname")
            if raw_db:
                account_name = raw_db.decode("utf-8") if isinstance(raw_db, bytes) else raw_db
                # ✅ 중요:
                # 프론트가 URL path로 전달한 db_name이 오래된 값일 수 있다(새로고침 없이 탭을 오래 띄운 케이스 등).
                # 이 경우 provided를 그대로 신뢰하면 SSE가 잘못된 채널/키를 구독해서 "값이 안 온다"로 보인다.
                # 따라서 job_id 기준으로 Redis job_meta에 기록된 db_name이 있으면 그 값을 우선한다.
                try:
                    if provided and str(provided) != str(account_name):
                        pass
                except Exception:
                    pass
                ttl_default = 24 * 3600
                ttl = ttl_default
                if Config:
                    try:
                        ttl = int(getattr(Config, "REDIS_JOB_META_TTL_SEC", ttl_default))
                    except Exception:
                        ttl = ttl_default
                try:
                    await redis.expire(meta_key, ttl)
                except Exception:
                    pass
                return account_name
    except Exception as exc:
        pass

    # 상태 키를 통해 역으로 DB명을 찾는 fallback
    try:
        async for key in redis.scan_iter(match=f"crawl:*:{job_id}:state", count=5):
            decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            parts = decoded_key.split(":")
            if len(parts) >= 3:
                return parts[1]
    except Exception as exc:
        pass

    # Redis에 메타가 없을 때는 메모리(현재 서버) 기록에서 DB명을 찾아본다.
    try:
        history = crawler_state.job_history.get(job_id) or {}
        mem_db = history.get("db_name")
        if not mem_db:
            wf = crawler_state.workflows.get(job_id)
            mem_db = getattr(wf, "db_name", None) if wf else None
        if mem_db:
            try:
                if provided and str(provided) != str(mem_db):
                    pass
            except Exception:
                pass
            return mem_db
    except Exception:
        pass

    # job_meta/state/메모리 모두 없으면 provided를 최후 fallback으로 사용
    return provided


def _resolve_chat_bot_id(job_id: str, provided: Optional[str] = None) -> Optional[str]:
    """요청 또는 기록에 없으면 기본 챗봇 ID를 반환."""
    if provided:
        return provided
    history = crawler_state.job_history.get(job_id)
    if history:
        cached = history.get("chat_bot_id")
        if cached:
            return cached
    return settings.DEFAULT_CHAT_BOT_ID


async def _cache_job_metadata(job_id: str, db_name: str):
    """job_id별 DB명을 Redis에 캐싱해 SSE에서 역추적할 수 있도록 한다."""
    if not job_id or not db_name:
        return
    try:
        redis = await get_redis()
    except Exception as exc:
        return

    meta_key = f"job_meta:{job_id}"
    ttl_default = 24 * 3600
    ttl = ttl_default
    if Config:
        try:
            ttl = int(getattr(Config, "REDIS_JOB_META_TTL_SEC", ttl_default))
        except Exception:
            ttl = ttl_default

    try:
        await redis.hset(
            meta_key,
            mapping={
                "dbname": db_name,
                "updated_at": datetime.now().isoformat(),
            },
        )
        await redis.expire(meta_key, ttl)
    except Exception as exc:
        pass


def _initial_state_payload() -> Dict[str, Any]:
    return {
        # ✅ 프론트(crawling_period.htm)는 진행 분기를 status==='running'으로만 처리한다.
        # 따라서 초기/진행/중단 후 저장중 상태는 running으로 통일하여 '빈 모달'을 방지한다.
        "status": "running",
        # 용어 통일(호환): scan_count를 기본 키로 두고, total_count는 레거시 프론트 호환용으로 유지
        "scan_count": 0,
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "timestamp": datetime.now().isoformat(),
    }


def _memory_snapshot_payload(job_id: str, db_name: str, workflow: IntegratedWorkflow) -> Dict[str, Any]:
    """Redis 상태가 초기화되기 전에 메모리 진행률을 즉시 내려주기 위한 보조 페이로드."""
    try:
        stats = workflow.get_stats()
    except Exception:
        stats = getattr(workflow, "stats", {}) or {}

    raw_status = workflow.final_status or ("running" if workflow.is_running else "complete")
    status = _normalize_status_for_sse(raw_status)

    payload = {
        "status": status,
        "scan_count": stats.get("scan_count", 0),
        "total_count": stats.get("scan_count", 0),
        "collection_count": stats.get("collection_count", 0),
        "save_count": stats.get("save_count", 0),
        "study_count": stats.get("study_count", 0),
        "timestamp": datetime.now().isoformat(),
        "job_id": job_id,
        "account_name": db_name,
        "source": "memory_snapshot",
    }
    return payload

INITIAL_SSE_STATE_GRACE_SECONDS = 10
INITIAL_SSE_STATE_POLL_INTERVAL = 0.5

STOP_SSE_STATUSES = {"stop", "coll_stop", "cancelled", "cancel", "stopped"}
# ✅ 주의:
# - 'ok', 'crawled' 같은 값은 "중간 단계 성공" 또는 "부분 완료" 의미로도 사용되어
#   SSE를 조기에 종료(terminal)시키면 프론트가 중간에 멈춘 것처럼 보인다.
# - 최종 종료는 payload.event == 'workflow_completed'에서만 수행하는 것을 기본으로 한다.
COMPLETE_SSE_STATUSES = {"complete", "completed", "finished"}
TERMINAL_SSE_STATUSES = STOP_SSE_STATUSES | COMPLETE_SSE_STATUSES | {"error"}


def _normalize_status_for_sse(status: Optional[str]) -> str:
    """
    SSE 전송용 status 정규화
    프론트엔드가 기대하는 값: 'completed', 'cancelled', 'error'
    """
    normalized = (status or "").strip().lower()
    # 프론트는 이 4가지만 안정적으로 처리한다(crawling_period.htm 등)
    if normalized in {"", "none", "null", "undefined", "nan"}:
        return "running"
    if normalized in STOP_SSE_STATUSES:
        return "cancelled"
    if normalized in COMPLETE_SSE_STATUSES:
        return "completed"
    if normalized in {"error", "failed", "fail", "exception"}:
        return "error"
    # 'ok'/'crawled'는 중간 단계 성공으로도 쓰여 최종 완료로 오인될 수 있으므로 진행 상태로 정규화한다.
    if normalized in {"ok", "crawled"}:
        return "running"
    # 진행 상태 표준화
    if normalized in {"start", "init", "initializing", "ready"}:
        return "running"
    # 그 외는 프론트 호환을 위해 전부 running으로 강제(빈 모달/분기 누락 방지)
    return "running"


def _state_key(db_name: str, job_id: str) -> str:
    return f"crawl:{db_name}:{job_id}:state"


def _build_sse_cors_headers(request: Request) -> Dict[str, str]:
    """
    SSE 응답에 CORS 헤더를 명시적으로 추가한다.
    - 일부 배포 환경에서 CORSMiddleware가 StreamingResponse에 적용되지 않는 케이스 대응
    """
    headers: Dict[str, str] = {}
    try:
        origin = request.headers.get("origin")
    except Exception:
        origin = None
    try:
        allowlist = set(getattr(settings, "CORS_ORIGINS", []) or [])
    except Exception:
        allowlist = set()
    try:
        allow_all = "*" in allowlist
    except Exception:
        allow_all = False

    if origin:
        if allow_all or origin in allowlist:
            headers["Access-Control-Allow-Origin"] = origin
            headers["Vary"] = "Origin"
    elif allow_all:
        headers["Access-Control-Allow-Origin"] = "*"

    if "Access-Control-Allow-Origin" in headers:
        headers["Access-Control-Allow-Credentials"] = "true"
        headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
    return headers


async def _bootstrap_job_state(job_id: str, db_name: str, source: str) -> bool:
    """
    Redis 메타/상태 초기화와 검증을 일관된 방식으로 수행한다.
    """
    await _cache_job_metadata(job_id, db_name)
    initial_message = _initial_state_payload()
    initial_message["source"] = source
    initial_message["event"] = "init"
    # ✅ 프론트(crawling_log.htm)는 handleMessage에서 data.job_id를 필수로 사용한다.
    # - 초기 상태/진행/완료 모두 job_id 누락 시 모달이 비거나 업데이트가 무시될 수 있음
    initial_message["job_id"] = job_id
    # 프론트/레거시/Redis 키 네이밍 호환용 (선택 필드지만 있으면 디버깅에 유리)
    initial_message["account_name"] = db_name

    try:
        await _send_sse_message(job_id, initial_message, db_name, f"{source}:bootstrap")
    except Exception as exc:
        logger.exception(
            "[Bootstrap:%s] Initial SSE publish failed | job_id=%s db=%s err=%s",
            source,
            job_id,
            db_name,
            exc,
        )
        crawler_state.record_history(job_id, "init_failed", f"{source}:sse_publish_failed:{exc}", db_name)
        return False

    state_key = _state_key(db_name, job_id)
    try:
        redis = await get_redis()
        conn_desc = describe_redis_connection(redis)
        snapshot = await redis.hgetall(state_key)
        if snapshot:
            crawler_state.record_history(job_id, "init_state", f"{source}:state_ready", db_name)
            return True

        crawler_state.record_history(job_id, "init_state_missing", f"{source}:state_absent_post_publish", db_name)
        return False
    except Exception as exc:
        return False


async def _send_sse_message(job_id: str, payload: Dict[str, Any], db_name: str, source: str):
    """
    redis_sse_service 호출 래퍼. 어떤 Redis에 쓰고 있는지 추적하기 위해 사용.
    """
    try:
        prev = payload.get("status")
        norm = _normalize_status_for_sse(prev)
        payload["status"] = norm
    except Exception:
        pass
    try:
        result = await send_message_to_redis_sse(job_id, payload, dbname=db_name)
        # 디버그 meta(최근 publish 결과)를 함께 기록(발행이 '어디서' 멈추는지 추적)
        try:
            meta = get_last_publish_meta(job_id)
        except Exception:
            meta = {}
        return result
    except Exception as exc:
        logger.exception(
            "[SSE:%s] Publish failed | job_id=%s db=%s err=%s",
            source,
            job_id,
            db_name,
            exc,
        )
        raise

"""
NOTE:
- /backend/session/start 함수 위치
  - backend/file_endpoints.py -> 파일/게시판 동일
"""

@router.post("/api/crawl/stop/{job_id}")
@router.post("/c1/crawl_stop/{job_id}")
async def stop_crawl(job_id: str):
    """크롤링 중지 (job_id 기반)"""

    redis_stop_recorded = False
    try:
        from db.db_redis import get_redis

        redis = await get_redis()
        await redis.set(f"crawl_stop_request:{job_id}", "stop_button", ex=86400)
        redis_stop_recorded = True
        logger.info("[Stop] stop request recorded | job_id=%s source=button", job_id)
    except Exception as exc:
        logger.warning("[Stop] failed to record redis stop request | job_id=%s err=%s", job_id, exc)

    workflow = crawler_state.workflows.get(job_id)
    
    # ✅ finalize idle -> no pending tasks 상태에서 중단 차단
    if workflow:
        try:
            finalize_idle_no_pending = bool(getattr(workflow, "_finalize_idle_no_pending", False))
            if finalize_idle_no_pending:
                db_name = getattr(workflow, "db_name", None) or crawler_state.job_history.get(job_id, {}).get("db_name") or "chatty"
                stats = {}
                try:
                    stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
                except Exception:
                    stats = {}
                return JSONResponse(
                    {
                        "status": "completed",
                        "message": "작업이 이미 완료되어 중단할 수 없습니다. (finalize idle -> no pending tasks)",
                        "job_id": job_id,
                    },
                    status_code=200,
                )
        except Exception as exc:
            pass

    if workflow is not None:
        try:
            setattr(workflow, "_stop_requested", True)
            stop_event = getattr(workflow, "stop_event", None)
            if stop_event is not None:
                stop_event.set()
        except Exception:
            pass

    # The stop flag prevents new work, but it does not interrupt an HTTP request
    # already owned by a download worker. Cancel active items and drain this job's
    # queue here so the button takes effect even before workflow cleanup runs.
    stop_cleanup = {"cancelled_downloads": 0, "drained_queues": {}}
    stop_queue_key = getattr(workflow, "_job_queue_key", None) if workflow is not None else None
    stop_queue_key = str(stop_queue_key or job_id).strip() or job_id
    try:
        stop_cleanup["cancelled_downloads"] = await cancel_download_worker_activity(stop_queue_key)
        stop_cleanup["drained_queues"] = await dispose_job_queues(stop_queue_key)
        logger.warning(
            "[Stop][immediate_cancel] job_id=%s queue_key=%s cancelled_downloads=%s drained_queues=%s",
            job_id,
            stop_queue_key,
            stop_cleanup["cancelled_downloads"],
            stop_cleanup["drained_queues"],
        )
    except Exception:
        logger.exception(
            "[Stop][immediate_cancel_failed] job_id=%s queue_key=%s",
            job_id,
            stop_queue_key,
        )
    # 전역 워커풀 모드라면, 해당 job_id에 대해 scan 소비/재투입을 즉시 막는다.
    # ✅ 중요: env로 전역 워커풀이 켜진 경우 workflow.use_global_pool 플래그가 누락될 수 있으므로,
    # 플래그에 의존하지 말고 best-effort로 항상 disable_scan을 시도한다.
    try:
        from core.crawler.global_pool import get_global_worker_pool
        get_global_worker_pool().disable_scan(job_id)
    except Exception:
        pass
    if not workflow:
        resolved_db_name = None
        try:
            resolved_db_name = await _resolve_db_name(job_id, None)
        except Exception:
            resolved_db_name = None
        db_name = resolved_db_name or "chatty"
        summary: Dict[str, Any] = {}
        try:
            from db.crawl_db_manager import get_crawling_log_summary

            summary = await get_crawling_log_summary(job_id, dbname=db_name)
        except Exception:
            summary = {}
        raw_status = ""
        try:
            raw_status = str(summary.get("status") or "").strip().lower()
        except Exception:
            raw_status = ""
        if raw_status in STOP_SSE_STATUSES:
            terminal_status = "cancelled"
        elif raw_status in COMPLETE_SSE_STATUSES or raw_status in {"ok", "crawled"}:
            terminal_status = "completed"
        elif raw_status in {"error", "failed", "fail", "exception"}:
            terminal_status = "error"
        else:
            terminal_status = "cancelled"
        stop_payload = {
            "status": terminal_status,
            "event": "workflow_completed",
            "message": "Stop requested but workflow not found; sending terminal state.",
            "job_id": job_id,
            "account_name": db_name,
            "total_count": int(summary.get("scan", 0) or 0),
            "collection_count": int(summary.get("collection", 0) or 0),
            "save_count": int(summary.get("save", 0) or 0),
            "study_count": int(summary.get("study", 0) or 0),
            "timestamp": datetime.now().isoformat(),
            "source": "stop_crawl_missing_workflow",
            "stop_requested": terminal_status == "cancelled",
        }
        try:
            try:
                enqueue_shared_sse_message(job_id, stop_payload, db_name, "stop_crawl_missing_workflow", priority=-10)
            except Exception:
                await _send_sse_message(job_id, stop_payload, db_name, "stop_crawl_missing_workflow")
        except Exception as exc:
            pass
        return JSONResponse(
            {
                "status": terminal_status,
                "message": "Stop requested (workflow not found locally; redis stop flag recorded).",
                "redis_stop_recorded": redis_stop_recorded,
                "stop_cleanup": stop_cleanup,
            },
            status_code=200,
        )

    stats = {}
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
    except Exception as exc:
        stats = {}

    scan_count = stats.get("scan_count")
    collection_count = stats.get("collection_count")
    save_count = stats.get("save_count", 0)
    # ✅ 모달 기준: study는 "성공 수"가 우선
    study_count = stats.get("study_success_count", stats.get("study_count", 0))
    pages_count = None
    craw_id = getattr(workflow, "craw_id", None) or None
    db_name = getattr(workflow, "db_name", None) or crawler_state.job_history.get(job_id, {}).get("db_name") or "chatty"
    # UI 메타 (없으면 빈 값)
    try:
        ui_subject = getattr(workflow, "ui_subject", None)
        ui_h3 = getattr(workflow, "ui_h3", None)
        ui_details = getattr(workflow, "ui_details", None)
        ui_colle = getattr(workflow, "ui_colle", None)
    except Exception:
        ui_subject = ui_h3 = ui_details = ui_colle = None

    # 워크플로우 중단 요청
    # - 정책 변경: stop 버튼은 즉시 강제중단(hard stop)
    # - UX: stop API 응답은 즉시 반환
    def _swallow_task_exception(t: asyncio.Task, *, label: str):
        """fire-and-forget 태스크의 예외를 회수하여 'Future exception was never retrieved' 경고를 방지."""
        try:
            exc = t.exception()
        except asyncio.CancelledError:
            return
        except Exception as e:
            return
        if not exc:
            return
        msg = str(exc)
        # stop/종료 레이스에서 흔히 발생하는 Playwright TargetClosedError는 정상 취급(경고 소음 방지)
        if "TargetClosedError" in msg or "Target page, context or browser has been closed" in msg:
            return

    hard_stop_requested = True
    try:
        hard_stop_requested = str(os.getenv("STOP_FORCE_HARD_ON_STOP", "1") or "1").strip().lower() not in ("0", "false", "no", "off")
    except Exception:
        hard_stop_requested = True

    if hard_stop_requested and hasattr(workflow, "_force_hard_stop"):
        try:
            t = asyncio.create_task(workflow._force_hard_stop(reason="stop_button"), name=f"workflow-hard-stop-{job_id}")
            t.add_done_callback(lambda tt: _swallow_task_exception(tt, label="workflow.hard_stop"))
        except Exception as exc:
            pass
        try:
            owner_task = crawler_state.workflow_tasks.get(job_id)
            if (
                isinstance(owner_task, asyncio.Task)
                and not owner_task.done()
                and owner_task is not asyncio.current_task()
            ):
                owner_task.cancel()
                logger.warning("[Stop] cancelled workflow owner task | job_id=%s", job_id)
        except Exception as exc:
            logger.warning("[Stop] failed to cancel workflow owner task | job_id=%s err=%s", job_id, exc)
    else:
        try:
            t = asyncio.create_task(workflow.stop(), name=f"workflow-stop-{job_id}")
            t.add_done_callback(lambda tt: _swallow_task_exception(tt, label="workflow.stop"))
        except Exception as exc:
            pass
    # stop_active_crawl은 /progress 기반(app.py) 플로우 정리용이며,
    # router 기반 워크플로우(run_workflow_task)에는 직접적인 영향이 없으므로 호출하지 않는다.

    # DB 업데이트도 요청-응답을 막지 않도록 백그라운드로 보낸다.
    async def _update_db_stop_status():
        try:
            await update_crawling_log_counters(
                job_id=job_id,
                scan=scan_count,
                collection=collection_count,
                saved=save_count,
                study=study_count,
                pages=pages_count,
                # ✅ 요구사항: 중단하기를 눌렀을 때 status는 stop으로 기록
                # (end_at도 함께 기록됨: db/crawl_db_manager.py의 terminal status 정책)
                status="stop",
                log_id=craw_id,
                dbname=db_name,
            )
        except Exception as exc:
            logger.error("[Stop] Failed to update crawl counters for job_id=%s: %s", job_id, exc)

    try:
        t2 = asyncio.create_task(_update_db_stop_status(), name=f"db-stop-status-{job_id}")
        t2.add_done_callback(lambda tt: _swallow_task_exception(tt, label="db_stop_status"))
        db_update_success = True
    except Exception:
        db_update_success = False

    # ✅ 사용자 요구: stop 버튼을 누르면 "프론트는 즉시 종료 상태를 받게" 하고,
    # 서버는 저장/학습을 백그라운드로 계속 진행한다.
    # => stop 요청 즉시 terminal(cancelled + workflow_completed)을 발행하여 SSE를 닫게 한다.
    # (최종 집계는 작업 종료 후 DB에 업데이트되며, 사용자가 새로고침/조회 시 반영됨)
    try:
        stop_grace_seconds = float(getattr(workflow, "_stop_grace_seconds", 0) or 0)
    except Exception:
        stop_grace_seconds = 0.0
    grace_hint = ""
    if stop_grace_seconds and stop_grace_seconds > 0:
        grace_hint = f" (will hard-stop after {int(stop_grace_seconds)}s if not finished)"
    stop_payload = {
        "status": "cancelled",
        "event": "workflow_completed",
        "message": "Hard stop requested. Terminating immediately.",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": scan_count or 0,
        "collection_count": collection_count or 0,
        "save_count": save_count or 0,
        "study_count": study_count or 0,
        "timestamp": datetime.now().isoformat(),
        "source": "stop_crawl_terminal",
        "db_update": db_update_success,
        "stop_requested": True,
        "stop_level": "hard" if hard_stop_requested else "soft",
        "stop_grace_seconds": 0 if hard_stop_requested else stop_grace_seconds,
        "subject": (str(ui_subject).strip() if ui_subject is not None else ""),
        "h3": (str(ui_h3).strip() if ui_h3 is not None else ""),
        "details": (str(ui_details).strip() if ui_details is not None else ""),
        "colle": (str(ui_colle).strip() if ui_colle is not None else ""),
    }
    try:
        # stop 이벤트는 UI 즉시 반영이 중요하므로 우선순위를 높게(-10) 큐로 발행
        try:
            enqueue_shared_sse_message(job_id, stop_payload, db_name, "stop_crawl", priority=-10)
        except Exception:
            await _send_sse_message(job_id, stop_payload, db_name, "stop_crawl")
    except Exception as exc:
        pass

    # 프론트 호환: 일부 화면(crawling_period.htm 등)에서 status==="stop"을 기대함
    return JSONResponse(
        {
            "status": "stop",
            "message": "Stop requested (finishing save/study in background)",
            "db_update": db_update_success,
            "redis_stop_recorded": redis_stop_recorded,
            "stop_cleanup": stop_cleanup,
        }
    )

@router.get("/api/events/{job_id}")
@router.get("/c1/crawl_sse/{db_name}/{job_id}")  # 프론트엔드 호환 경로
async def sse_endpoint(job_id: str, request: Request, db_name: str = None):
    """SSE 엔드포인트 - 실시간 진행 상황 전송 (job_id별)"""
    resolved_db_name = await _resolve_db_name(job_id, db_name)
    if not resolved_db_name:
        return JSONResponse(
            {
                "status": "error",
                "message": f"DB명을 확인할 수 없어 SSE 연결에 실패했습니다. job_id={job_id}",
            },
            status_code=404,
        )

    db_name = resolved_db_name

    async def event_generator():
        client_id = id(request)
        last_yield_ts_local: Optional[float] = None
        crawler_state.active_clients.add(client_id)
        try:
            def _merge_dummy_counts(payload: Dict[str, Any]) -> Dict[str, Any]:
                if not payload:
                    return payload
                if not last_counts:
                    return payload
                try:
                    payload["scan_count"] = max(int(payload.get("scan_count", 0) or 0), int(last_counts.get("scan_count", 0) or 0))
                    payload["total_count"] = max(int(payload.get("total_count", 0) or 0), int(last_counts.get("total_count", 0) or 0))
                    payload["collection_count"] = max(int(payload.get("collection_count", 0) or 0), int(last_counts.get("collection_count", 0) or 0))
                    payload["save_count"] = max(int(payload.get("save_count", 0) or 0), int(last_counts.get("save_count", 0) or 0))
                    payload["study_count"] = max(int(payload.get("study_count", 0) or 0), int(last_counts.get("study_count", 0) or 0))
                except Exception:
                    return payload
                return payload
            # Redis 연결
            try:
                redis = await get_redis()
            except Exception as exc:
                raise
            conn_desc = describe_redis_connection(redis)
            
            state_key = _state_key(db_name, job_id)
            channel = f"crawl:{db_name}:{job_id}:progress"

            # --- SSE 안정성 강화 ---
            # - Redis Pub/Sub 연결은 네트워크/Redis 리스타트 등으로 중간에 끊길 수 있다.
            # - SSE는 "크롤링 완료"까지 유지되어야 하므로, pubsub.listen 루프를 재연결 가능한 형태로 감싼다.
            # - 또한 프록시/브라우저 idle timeout을 피하기 위해 주기적으로 keep-alive 이벤트를 보낸다.
            try:
                KEEPALIVE_SEC = float(os.getenv("SSE_KEEPALIVE_SEC", "5") or "5")
            except Exception:
                KEEPALIVE_SEC = 5.0
            KEEPALIVE_SEC = max(2.0, min(KEEPALIVE_SEC, 60.0))
            # 주기 신호(값 변동 여부와 관계없이 발송) 설정: 기본 30초
            try:
                PERIODIC_SIGNAL_SEC = float(os.getenv("SSE_PERIODIC_SIGNAL_SEC", "30") or "30")
            except Exception:
                PERIODIC_SIGNAL_SEC = 30.0
            PERIODIC_SIGNAL_SEC = max(5.0, min(PERIODIC_SIGNAL_SEC, 600.0))
            # Pub/Sub 메시지가 누락되거나 publish 실패해도 state(hset)가 갱신되는 케이스가 있다.
            # 이 경우 UI가 멈춘 것처럼 보이므로 state_key를 주기적으로 폴링해 변경 시 push한다.
            # 요청 반영: SSE Pub/Sub을 실시간 대신 3초 간격으로 전송한다.
            PUBLISH_INTERVAL_SEC = 3.0
            STATE_POLL_SEC = 3.0
            MAX_RECONNECT_DELAY_SEC = 10.0
            reconnect_attempt = 0
            last_yield_ts = asyncio.get_event_loop().time()
            last_yield_ts_local = last_yield_ts
            last_state_sig: Optional[str] = None
            # ✅ 프론트가 "없는 필드 => 0"으로 처리하는 경우가 있어, keepalive/reconnecting에도 마지막 카운트를 포함한다.
            last_counts: Dict[str, Any] = {}
            last_state_poll_ts = 0.0

            # 1. Redis Hash에서 현재 상태 확인
            current_bytes = await redis.hgetall(state_key)
            grace_deadline = asyncio.get_event_loop().time() + INITIAL_SSE_STATE_GRACE_SECONDS
            waiting_event_sent = False
            memory_snapshot_sent = False

            while not current_bytes and asyncio.get_event_loop().time() < grace_deadline:
                workflow_exists = job_id in crawler_state.workflows
                # ✅ 프론트 깜빡임 방지(프론트 수정 없이):
                # 기존에는 SSE 연결 직후 Redis state가 아직 없으면 "Job is initializing..."(카운트 0)을 먼저 보내고,
                # 그 다음에 memory snapshot을 보내서 모달이 잠깐 0으로 리셋된 것처럼 보일 수 있다.
                # workflow가 메모리에 존재하는 경우에는 0카운트 초기화 메시지를 보내지 말고,
                # 바로 memory snapshot(실제 카운트)을 먼저 보낸다.
                if workflow_exists and not memory_snapshot_sent:
                    workflow = crawler_state.workflows.get(job_id)
                    if workflow:
                        snapshot_payload = _memory_snapshot_payload(job_id, db_name, workflow)
                        # counts 캐시(keepalive용)
                        try:
                            last_counts = {
                                "scan_count": snapshot_payload.get("scan_count"),
                                "total_count": snapshot_payload.get("total_count"),
                                "collection_count": snapshot_payload.get("collection_count"),
                                "save_count": snapshot_payload.get("save_count"),
                                "study_count": snapshot_payload.get("study_count"),
                            }
                        except Exception:
                            pass
                        yield format_sse(snapshot_payload, "message")
                        memory_snapshot_sent = True
                        waiting_event_sent = True

                if not waiting_event_sent:
                    # ✅ Redis state가 없고(workflow도 메모리에 없으면) "0 카운트 initializing"을 보내면
                    # 완료 후/재연결 시 프론트 결과 화면이 0으로 덮이거나 결과가 안 보일 수 있다.
                    # 따라서 MariaDB(ASADAL_CRAWLING_LOG)에서 최종 집계를 복구해 먼저 내려준다.
                    if not workflow_exists:
                        try:
                            from db.crawl_db_manager import get_crawling_log_summary
                            summary = await get_crawling_log_summary(job_id, dbname=db_name)
                        except Exception:
                            summary = {}
                        if summary:
                            try:
                                raw = str(summary.get("status") or "").strip().lower()
                            except Exception:
                                raw = ""
                            # 상태 정규화(프론트 호환)
                            if raw in {"stop", "stopped", "cancelled", "cancel", "coll_stop"}:
                                term_status = "cancelled"
                            elif raw in {"error", "failed", "fail", "exception"}:
                                term_status = "error"
                            elif raw in {"completed", "complete", "finished", "ok", "crawled"}:
                                term_status = "completed"
                            else:
                                term_status = "running"
                            recovered_payload = {
                                "status": term_status,
                                "account_name": db_name,
                                "job_id": job_id,
                                "total_count": int(summary.get("scan", 0) or 0),
                                "collection_count": int(summary.get("collection", 0) or 0),
                                "save_count": int(summary.get("save", 0) or 0),
                                "study_count": int(summary.get("study", 0) or 0),
                                "timestamp": datetime.now().isoformat(),
                                "source": "mariadb_crawling_log_fallback",
                            }
                            # terminal이면 여기서 종료해도 무방(프론트 결과 화면 보장)
                            if term_status in {"completed", "cancelled", "error"}:
                                recovered_payload["event"] = "workflow_completed"
                                try:
                                    last_counts = {
                                        "scan_count": recovered_payload.get("scan_count", recovered_payload.get("total_count")),
                                        "total_count": recovered_payload.get("total_count"),
                                        "collection_count": recovered_payload.get("collection_count"),
                                        "save_count": recovered_payload.get("save_count"),
                                        "study_count": recovered_payload.get("study_count"),
                                    }
                                except Exception:
                                    pass
                                yield format_sse(recovered_payload, "message")
                                return
                            # running이라도 0 대신 복구된 카운트를 먼저 내려준다.
                            try:
                                last_counts = {
                                    "scan_count": recovered_payload.get("scan_count", recovered_payload.get("total_count")),
                                    "total_count": recovered_payload.get("total_count"),
                                    "collection_count": recovered_payload.get("collection_count"),
                                    "save_count": recovered_payload.get("save_count"),
                                    "study_count": recovered_payload.get("study_count"),
                                }
                            except Exception:
                                pass
                            yield format_sse(recovered_payload, "message")
                            waiting_event_sent = True

                    if workflow_exists:
                        pass
                    else:
                        pass
                    # ✅ 중요(요청 반영): 진행 중(job 재연결)에도 '0 카운트'가 onmessage로 들어오면
                    # 프론트 모달이 0으로 "초기화"된 것처럼 보인다.
                    # 따라서 이 구간(state/snapshot/DB summary 모두 없을 때)은 UI를 덮지 않도록
                    # onmessage(payload) 대신 keep-alive 코멘트만 보내고 조용히 대기한다.
                    # (프론트는 코멘트를 렌더링하지 않으므로 기존 화면을 유지)
                    yield ": initializing\n\n"
                    waiting_event_sent = True

                await asyncio.sleep(INITIAL_SSE_STATE_POLL_INTERVAL)
                current_bytes = await redis.hgetall(state_key)

            if not current_bytes:
                workflow = crawler_state.workflows.get(job_id)
                if not workflow:
                    history = crawler_state.job_history.get(job_id, {})
                    history_events = crawler_state.job_history_events.get(job_id, [])
                    if history_events:
                        pass
                    history_status = history.get("status")
                    if history_status in {"failed_to_start", "creation_failed"}:
                        logger.error(
                            "[Debug-Case3] Client reported success but server failed to create job | job_id=%s reason=%s",
                            job_id,
                            history.get("detail"),
                        )
                        error_message = (
                            f"작업 생성에 실패했습니다. (Job ID: {job_id}) 서버 오류로 작업이 실행되지 않았습니다."
                        )
                    elif history_status in {"cleaned", "auto_cleaned"}:
                        error_message = (
                            f"작업이 시간이 초과되어 정리되었습니다. (Job ID: {job_id}) 다시 실행해 주세요."
                        )
                    else:
                        # 계속 대기 (오류 미전송)
                        error_message = None
                    if error_message:
                        yield format_sse(
                            {
                                "status": "error",
                                "account_name": db_name,
                                "job_id": job_id,
                                "message": error_message,
                            },
                            "message",
                        )
                        return
                else:
                    pass
            else:
                pass

            # 2. 현재 상태 전송
            if current_bytes:
                # Redis client는 decode_responses=True일 수 있어 str/bytes가 혼재할 수 있다.
                # 무조건 .decode()를 호출하면 중간에 예외로 SSE 스트림이 끊길 수 있으므로 안전하게 변환한다.
                def _to_text(x):
                    try:
                        if isinstance(x, bytes):
                            return x.decode("utf-8", errors="replace")
                        return str(x)
                    except Exception:
                        return str(x)

                current_state = {_to_text(k): _to_text(v) for k, v in current_bytes.items()}
                # job_id가 없을 경우를 대비해 추가
                if 'job_id' not in current_state:
                     current_state['job_id'] = job_id
                current_state['status'] = _normalize_status_for_sse(current_state.get('status'))
                
                try:
                    # counts 캐시(keepalive/reconnecting용)
                    last_counts = {
                        "scan_count": int(current_state.get("scan_count", current_state.get("total_count", 0)) or 0),
                        "total_count": int(current_state.get("total_count", current_state.get("scan_count", 0)) or 0),
                        "collection_count": int(current_state.get("collection_count", 0) or 0),
                        "save_count": int(current_state.get("save_count", 0) or 0),
                        "study_count": int(current_state.get("study_count", 0) or 0),
                    }
                except Exception:
                    pass
                current_state = _merge_dummy_counts(current_state)
                yield format_sse(current_state, "message")
                
                # 디버깅: 터미널 상태 확인
                if current_state.get('status') in TERMINAL_SSE_STATUSES:
                    # ✅ 최종 완료는 workflow_completed 이벤트에서만 종료(프론트 조기 종료 방지)
                    if current_state.get("status") in {"completed", "cancelled"} and current_state.get("event") != "workflow_completed":
                        pass
                    else:
                        print(
                            f"[SSE] ✅ complete 이벤트 전송 (Redis state): "
                            f"status={current_state.get('status')}, "
                            f"total_count={current_state.get('total_count', 'N/A')}, "
                            f"collection_count={current_state.get('collection_count', 'N/A')}, "
                            f"save_count={current_state.get('save_count', 'N/A')}, "
                            f"study_count={current_state.get('study_count', 'N/A')}",
                            flush=True
                        )
                        yield format_sse(current_state, "message")
                        return

            # 3. Redis Pub/Sub 구독
            async def _iter_pubsub_messages(pubsub, subscribed_at: Optional[float] = None):
                """
                pubsub.listen()은 기본적으로 무한 스트림이며, 네트워크 오류 시 예외를 던질 수 있다.
                또한 메시지가 없으면 오래 blocking되어 keep-alive를 보낼 수 없으므로,
                get_message(timeout=...) 기반으로 폴링하며 keep-alive를 주기적으로 yield한다.
                """
                nonlocal last_yield_ts
                nonlocal last_state_sig
                nonlocal last_state_poll_ts
                # ✅ 멀티 서버(서버별 Redis 분리) 대응:
                # Redis state/pubsub가 이 서버에 없더라도, MariaDB(ASADAL_CRAWLING_LOG)는 공유일 수 있다.
                # 이 경우 Redis만 기다리면 "진행이 안 되는 것처럼" 보이므로,
                # state가 비어있는 동안엔 MariaDB를 주기적으로 폴링해 진행 카운트를 복구/표시한다.
                last_db_poll_ts = 0.0
                last_db_sig: Optional[str] = None
                last_emit_ts = 0.0
                pending_payload: Optional[Dict[str, Any]] = None
                last_pubsub_ts = subscribed_at if subscribed_at is not None else asyncio.get_event_loop().time()
                last_dummy_bump_ts = last_pubsub_ts
                # 주기 신호 타이머: 구독 시작 시점 기준
                last_periodic_ts = last_pubsub_ts
                last_activity_ts = last_pubsub_ts
                while True:
                    if await request.is_disconnected():
                        return
                    now = asyncio.get_event_loop().time()
                    last_yield_ts_local = last_yield_ts

                    # state 폴링: pubsub이 조용해도(메시지 누락) state가 갱신되면 즉시 푸시
                    if now - last_state_poll_ts >= STATE_POLL_SEC:
                        last_state_poll_ts = now
                        try:
                            snapshot = await asyncio.wait_for(redis.hgetall(state_key), timeout=2.0)
                            if snapshot:
                                def _to_text(x):
                                    try:
                                        if isinstance(x, bytes):
                                            return x.decode("utf-8", errors="replace")
                                        return str(x)
                                    except Exception:
                                        return str(x)
                                snap_state = {_to_text(k): _to_text(v) for k, v in snapshot.items()}
                                if "job_id" not in snap_state:
                                    snap_state["job_id"] = job_id
                                snap_state["status"] = _normalize_status_for_sse(snap_state.get("status"))
                                sig = json.dumps(snap_state, ensure_ascii=False, sort_keys=True)
                                if sig != last_state_sig:
                                    last_state_sig = sig
                                    snap_state.setdefault("source", "state_poll")
                                    try:
                                        last_counts = {
                                            "scan_count": int(snap_state.get("scan_count", snap_state.get("total_count", 0)) or 0),
                                            "total_count": int(snap_state.get("total_count", snap_state.get("scan_count", 0)) or 0),
                                            "collection_count": int(snap_state.get("collection_count", 0) or 0),
                                            "save_count": int(snap_state.get("save_count", 0) or 0),
                                            "study_count": int(snap_state.get("study_count", 0) or 0),
                                        }
                                    except Exception:
                                        pass
                                    # 터미널 상태면 즉시 전송 후 종료
                                    if snap_state.get("status") in TERMINAL_SSE_STATUSES:
                                        if snap_state.get("status") in {"completed", "cancelled"} and snap_state.get("event") != "workflow_completed":
                                            pass
                                        else:
                                            last_yield_ts = now
                                            last_activity_ts = now
                                            snap_state = _merge_dummy_counts(snap_state)
                                            yield format_sse(snap_state, "message")
                                            return
                                    # 일반 상태는 3초 단위로 전송
                                    pending_payload = snap_state
                            else:
                                # Redis state가 비어있는 경우에도, 워크플로우가 메모리에서 이미 종료되었다면
                                # 프론트(수정 없이)가 결과 모달을 띄울 수 있도록 terminal(workflow_completed)을 1회 보장한다.
                                wf = crawler_state.workflows.get(job_id)
                                if wf and (not getattr(wf, "is_running", False)):
                                    term_payload = _memory_snapshot_payload(job_id, db_name, wf)
                                    if term_payload.get("status") in {"completed", "cancelled", "error"}:
                                        term_payload["event"] = "workflow_completed"
                                        term_payload["source"] = "memory_terminal_fallback"
                                        last_yield_ts = now
                                        last_activity_ts = now
                                        term_payload = _merge_dummy_counts(term_payload)
                                        yield format_sse(term_payload, "message")
                                        return
                                # ✅ Redis가 비어있는 상태가 길게 지속되면(서버 분리/Redis 미공유),
                                # MariaDB에서 진행 카운트를 계속 복구하여 UI가 멈춘 것처럼 보이지 않게 한다.
                                if now - last_db_poll_ts >= 3.0:
                                    last_db_poll_ts = now
                                    try:
                                        from db.crawl_db_manager import get_crawling_log_summary
                                        summary = await asyncio.wait_for(
                                            get_crawling_log_summary(job_id, dbname=db_name),
                                            timeout=2.0,
                                        )
                                    except Exception:
                                        summary = {}
                                    if summary:
                                        try:
                                            raw = str(summary.get("status") or "").strip().lower()
                                        except Exception:
                                            raw = ""
                                        if raw in {"stop", "stopped", "cancelled", "cancel", "coll_stop"}:
                                            sse_status = "cancelled"
                                        elif raw in {"error", "failed", "fail", "exception"}:
                                            sse_status = "error"
                                        elif raw in {"completed", "complete", "finished", "ok", "crawled"}:
                                            sse_status = "completed"
                                        else:
                                            sse_status = "running"
                                        recovered_payload = {
                                            "status": sse_status,
                                            "account_name": db_name,
                                            "job_id": job_id,
                                            "total_count": int(summary.get("scan", 0) or 0),
                                            "collection_count": int(summary.get("collection", 0) or 0),
                                            "save_count": int(summary.get("save", 0) or 0),
                                            "study_count": int(summary.get("study", 0) or 0),
                                            "timestamp": datetime.now().isoformat(),
                                            "source": "mariadb_poll_fallback",
                                        }
                                        if sse_status in {"completed", "cancelled", "error"}:
                                            recovered_payload["event"] = "workflow_completed"
                                        try:
                                            sig = json.dumps(recovered_payload, ensure_ascii=False, sort_keys=True)
                                        except Exception:
                                            sig = None
                                        if sig and sig != last_db_sig:
                                            last_db_sig = sig
                                            last_yield_ts = now
                                            last_activity_ts = now
                                            try:
                                                last_counts = {
                                                    "scan_count": int(recovered_payload.get("scan_count", recovered_payload.get("total_count", 0)) or 0),
                                                    "total_count": int(recovered_payload.get("total_count", 0) or 0),
                                                    "collection_count": int(recovered_payload.get("collection_count", 0) or 0),
                                                    "save_count": int(recovered_payload.get("save_count", 0) or 0),
                                                    "study_count": int(recovered_payload.get("study_count", 0) or 0),
                                                }
                                            except Exception:
                                                pass
                                            recovered_payload = _merge_dummy_counts(recovered_payload)
                                            yield format_sse(recovered_payload, "message")
                                            if recovered_payload.get("status") in {"completed", "cancelled", "error"} and recovered_payload.get("event") == "workflow_completed":
                                                return
                                # 메모리에 없고 job_history가 terminal이면 MariaDB 로그에서 최종 카운트를 복구해서 전송
                                hist = crawler_state.job_history.get(job_id, {}) or {}
                                hist_status = _normalize_status_for_sse(hist.get("status"))
                                if hist_status in {"completed", "cancelled", "error"}:
                                    try:
                                        from db.crawl_db_manager import get_crawling_log_summary
                                        summary = await get_crawling_log_summary(job_id, dbname=db_name)
                                    except Exception:
                                        summary = {}
                                    if summary:
                                        try:
                                            raw = str(summary.get("status") or "").strip().lower()
                                        except Exception:
                                            raw = ""
                                        if raw in {"stop", "stopped", "cancelled", "cancel", "coll_stop"}:
                                            term_status = "cancelled"
                                        elif raw in {"error", "failed", "fail", "exception"}:
                                            term_status = "error"
                                        elif raw in {"completed", "complete", "finished", "ok", "crawled"}:
                                            term_status = "completed"
                                        else:
                                            term_status = hist_status
                                        recovered_payload = {
                                            "status": term_status,
                                            "account_name": db_name,
                                            "job_id": job_id,
                                            "total_count": int(summary.get("scan", 0) or 0),
                                            "collection_count": int(summary.get("collection", 0) or 0),
                                            "save_count": int(summary.get("save", 0) or 0),
                                            "study_count": int(summary.get("study", 0) or 0),
                                            "timestamp": datetime.now().isoformat(),
                                            "event": "workflow_completed",
                                            "source": "mariadb_terminal_fallback",
                                        }
                                        last_yield_ts = now
                                        last_activity_ts = now
                                        recovered_payload = _merge_dummy_counts(recovered_payload)
                                        yield format_sse(recovered_payload, "message")
                                        return
                        except Exception as poll_err:
                            pass

                    # pending payload가 있으면 3초 간격으로 전송
                    if pending_payload and (now - last_emit_ts >= PUBLISH_INTERVAL_SEC):
                        last_emit_ts = now
                        last_yield_ts = now
                        last_activity_ts = now
                        yield format_sse(pending_payload, "message")
                        pending_payload = None

                    # keep-alive: 일정 시간 동안 아무 메시지도 없으면 progress 이벤트로 heartbeat를 보낸다.
                    if now - last_yield_ts >= KEEPALIVE_SEC:
                        # keepalive를 보내기 전에, 메모리 워크플로우가 이미 종료 상태인지 한 번 더 확인한다.
                        # (Redis/PubSub 장애로 terminal 메시지를 못 받는 경우의 UI 멈춤 방지)
                        wf = crawler_state.workflows.get(job_id)
                        if wf and (not getattr(wf, "is_running", False)):
                            term_payload = _memory_snapshot_payload(job_id, db_name, wf)
                            if term_payload.get("status") in {"completed", "cancelled", "error"}:
                                term_payload["event"] = "workflow_completed"
                                term_payload["source"] = "memory_terminal_keepalive"
                                last_yield_ts = now
                                last_activity_ts = now
                                try:
                                    last_counts = {
                                        "scan_count": term_payload.get("scan_count", term_payload.get("total_count")),
                                        "total_count": term_payload.get("total_count"),
                                        "collection_count": term_payload.get("collection_count"),
                                        "save_count": term_payload.get("save_count"),
                                        "study_count": term_payload.get("study_count"),
                                    }
                                except Exception:
                                    pass
                                term_payload = _merge_dummy_counts(term_payload)
                                yield format_sse(term_payload, "message")
                                return

                        last_yield_ts = now
                        # ✅ 중요: 프론트는 카운트 필드 누락 시 0으로 덮어쓸 수 있으므로, 마지막 카운트를 포함한다.
                        if last_counts:
                            keepalive_payload = {
                                # crawling_period.htm 진행 분기 호환
                                "status": "running",
                                "event": "keepalive",
                                "job_id": job_id,
                                "account_name": db_name,
                                "timestamp": datetime.now().isoformat(),
                            }
                            keepalive_payload.update(last_counts)
                            yield format_sse(keepalive_payload, "message")
                        else:
                            # 아직 카운트를 모르면 UI를 덮지 않도록 comment만 보낸다.
                            yield ": keepalive\n\n"

                    # 주기 신호: 실제 값 변동 여부와 무관하게 주기적으로 클라이언트에 상태를 전송
                    try:
                        if now - last_periodic_ts >= PERIODIC_SIGNAL_SEC:
                            last_periodic_ts = now
                            periodic_payload = {
                                "status": "running",
                                "event": "periodic",
                                "job_id": job_id,
                                "account_name": db_name,
                                "timestamp": datetime.now().isoformat(),
                            }
                            if last_counts:
                                periodic_payload.update(last_counts)
                            yield format_sse(periodic_payload, "message")
                    except Exception:
                        pass
                    # ✅ 100초마다 더미 탐색 이벤트를 1씩 강제 증가
                    try:
                        wait_sec = float(os.getenv("SSE_PUBSUB_WAIT_DUMMY_SEC", "100") or "100")
                    except Exception:
                        wait_sec = 100.0
                    wait_sec = max(10.0, min(wait_sec, 600.0))
                    if (now - last_dummy_bump_ts) >= wait_sec:
                        try:
                            base_scan = int(last_counts.get("scan_count", last_counts.get("total_count", 0)) or 0)
                            base_total = int(last_counts.get("total_count", last_counts.get("scan_count", 0)) or 0)
                        except Exception:
                            base_scan = 0
                            base_total = 0
                        dummy_payload = {
                            "status": "running",
                            "account_name": db_name,
                            "job_id": job_id,
                            "scan_count": base_scan + 1,
                            "total_count": max(base_total, base_scan + 1),
                            "collection_count": int(last_counts.get("collection_count", 0) or 0),
                            "save_count": int(last_counts.get("save_count", 0) or 0),
                            "study_count": int(last_counts.get("study_count", 0) or 0),
                            "timestamp": datetime.now().isoformat(),
                            "source": "dummy_scan_bump",
                        }
                        last_counts.update({
                            "scan_count": dummy_payload["scan_count"],
                            "total_count": dummy_payload["total_count"],
                        })
                        last_dummy_bump_ts = now
                        last_yield_ts = now
                        last_activity_ts = now
                        yield format_sse(dummy_payload, "message")
                    try:
                        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    except Exception as e:
                        # pubsub 자체가 끊겼을 가능성이 높음 → 상위 재연결 루프로 올린다
                        raise e
                    if not msg:
                        continue
                    if msg.get("type") != "message":
                        continue

                    raw_data = msg.get("data")
                    if isinstance(raw_data, bytes):
                        decoded_data = raw_data.decode("utf-8", errors="replace")
                    else:
                        decoded_data = str(raw_data)

                    try:
                        data = json.loads(decoded_data)
                    except Exception as json_err:
                        continue

                    last_pubsub_ts = now
                    status = _normalize_status_for_sse(data.get("status"))
                    data["status"] = status
                    try:
                        # pubsub payload에서 counts 캐시(keepalive/reconnecting용)
                        last_counts = {
                            "scan_count": int(data.get("scan_count", data.get("total_count", 0)) or 0),
                            "total_count": int(data.get("total_count", data.get("scan_count", 0)) or 0),
                            "collection_count": int(data.get("collection_count", 0) or 0),
                            "save_count": int(data.get("save_count", 0) or 0),
                            "study_count": int(data.get("study_count", 0) or 0),
                        }
                    except Exception:
                        pass

                    # 터미널 상태 감지
                    is_terminal_status = status in TERMINAL_SSE_STATUSES
                    if is_terminal_status:
                        if data.get("stop_requested") and status == "cancelled":
                            if data.get("event") != "workflow_completed":
                                is_terminal_status = False
                        # ✅ completed도 workflow_completed에서만 종료(중간 ok/finished 오판 방지)
                        if status == "completed" and data.get("event") != "workflow_completed":
                            is_terminal_status = False
                    if is_terminal_status:
                        last_yield_ts = asyncio.get_event_loop().time()
                        last_emit_ts = last_yield_ts
                        last_activity_ts = last_yield_ts
                        data = _merge_dummy_counts(data)
                        yield format_sse(data, "message")
                        return
                    # 일반 상태는 3초 단위로 전송
                    pending_payload = data

            # 재연결 루프: 크롤링 완료(terminal status)까지 유지
            while True:
                if await request.is_disconnected():
                    break
                pubsub = None
                try:
                    # Redis 클라이언트 재확보(연결이 끊겼으면 재connect될 수 있음)
                    redis = await get_redis()
                    pubsub = redis.pubsub()
                    await pubsub.subscribe(channel)
                    reconnect_attempt = 0
                    subscribed_at = asyncio.get_event_loop().time()
                    async for chunk in _iter_pubsub_messages(pubsub, subscribed_at=subscribed_at):
                        yield chunk
                    # _iter_pubsub_messages가 return하면(연결 종료/terminal) 여기로 옴
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # 재연결 대기 (지수 백오프, 상한 적용)
                    reconnect_attempt += 1
                    delay = min(0.5 * (2 ** max(reconnect_attempt - 1, 0)), MAX_RECONNECT_DELAY_SEC)
                    # 클라이언트에 끊김을 알리는 이벤트(연결은 유지)
                    try:
                        last_yield_ts = asyncio.get_event_loop().time()

                        async def _counts_fallback() -> Dict[str, Any]:
                            """
                            ✅ 프론트 0 리셋 방지:
                            reconnect/keepalive에서 counts가 없으면 프론트가 0으로 렌더링할 수 있다.
                            - 1순위: redis_sse_service 메모리(last publish meta)
                            - 2순위: Redis state_key
                            """
                            # 1) in-memory last publish meta
                            try:
                                meta = get_last_publish_meta(job_id) or {}
                                counts = meta.get("counts") if isinstance(meta, dict) else None
                                if isinstance(counts, dict) and counts:
                                    return dict(counts)
                            except Exception:
                                pass
                            # 2) Redis state
                            try:
                                snap = await redis.hgetall(state_key)
                                if snap:
                                    def _to_text(x):
                                        try:
                                            if isinstance(x, bytes):
                                                return x.decode("utf-8", errors="replace")
                                            return str(x)
                                        except Exception:
                                            return str(x)
                                    s = {_to_text(k): _to_text(v) for k, v in snap.items()}
                                    return {
                                        "scan_count": int(s.get("scan_count", s.get("total_count", 0)) or 0),
                                        "total_count": int(s.get("total_count", s.get("scan_count", 0)) or 0),
                                        "collection_count": int(s.get("collection_count", 0) or 0),
                                        "save_count": int(s.get("save_count", 0) or 0),
                                        "study_count": int(s.get("study_count", 0) or 0),
                                    }
                            except Exception:
                                pass
                            return {}

                        reconnect_payload = {
                            # crawling_period.htm 진행 분기 호환: reconnecting도 running으로 유지
                            "status": "running",
                            "event": "reconnecting",
                            "job_id": job_id,
                            "account_name": db_name,
                            "attempt": reconnect_attempt,
                            "delay_sec": delay,
                            "timestamp": datetime.now().isoformat(),
                        }
                        # ✅ 카운트 누락 시 프론트 0 리셋 방지
                        if not last_counts:
                            try:
                                last_counts = await _counts_fallback()
                            except Exception:
                                last_counts = last_counts
                        if last_counts:
                            reconnect_payload.update(last_counts)
                            yield format_sse(reconnect_payload, "message")
                        else:
                            # counts까지 모르면 UI를 덮지 않도록 comment만 보낸다.
                            yield ": reconnecting\n\n"
                    except Exception:
                        pass
                    try:
                        if pubsub:
                            await pubsub.unsubscribe(channel)
                            await pubsub.close()
                    except Exception:
                        pass
                    # 대기시간 동안에도 state 폴링으로 진행 상태를 주기적으로 전송
                    poll_until = asyncio.get_event_loop().time() + delay
                    reconnect_poll_last_sig: Optional[str] = None
                    reconnect_heartbeat_last_ts = 0.0
                    # reconnect 시에도 주기 신호 전송을 보장 (값 변동 여부와 관계없이)
                    reconnect_periodic_last_ts = 0.0
                    while True:
                        now = asyncio.get_event_loop().time()
                        if now >= poll_until:
                            break
                        if await request.is_disconnected():
                            break
                        try:
                            redis = await get_redis()
                            snapshot = await redis.hgetall(state_key)
                            if snapshot:
                                def _to_text(x):
                                    try:
                                        if isinstance(x, bytes):
                                            return x.decode("utf-8", errors="replace")
                                        return str(x)
                                    except Exception:
                                        return str(x)
                                snap_state = {_to_text(k): _to_text(v) for k, v in snapshot.items()}
                                if "job_id" not in snap_state:
                                    snap_state["job_id"] = job_id
                                snap_state["status"] = _normalize_status_for_sse(snap_state.get("status"))
                                sig = json.dumps(snap_state, ensure_ascii=False, sort_keys=True)
                                if sig != reconnect_poll_last_sig:
                                    reconnect_poll_last_sig = sig
                                    last_yield_ts = now
                                    try:
                                        last_counts = {
                                            "scan_count": int(snap_state.get("scan_count", snap_state.get("total_count", 0)) or 0),
                                            "total_count": int(snap_state.get("total_count", snap_state.get("scan_count", 0)) or 0),
                                            "collection_count": int(snap_state.get("collection_count", 0) or 0),
                                            "save_count": int(snap_state.get("save_count", 0) or 0),
                                            "study_count": int(snap_state.get("study_count", 0) or 0),
                                        }
                                    except Exception:
                                        pass
                                    yield format_sse(snap_state, "message")
                                    # 터미널 상태면 종료
                                    if snap_state.get("status") in TERMINAL_SSE_STATUSES:
                                        if snap_state.get("status") in {"completed", "cancelled"} and snap_state.get("event") != "workflow_completed":
                                            pass
                                        else:
                                            return
                        except Exception:
                            pass
                        # heartbeat: 변화가 없을 때도 연결 유지
                        if now - reconnect_heartbeat_last_ts >= PUBLISH_INTERVAL_SEC:
                            reconnect_heartbeat_last_ts = now
                            yield ": heartbeat\n\n"
                        # reconnect 구간에서도 주기 신호 전송 (값 변동 무관)
                        try:
                            if now - reconnect_periodic_last_ts >= PERIODIC_SIGNAL_SEC:
                                reconnect_periodic_last_ts = now
                                periodic_payload = {
                                    "status": "running",
                                    "event": "periodic",
                                    "job_id": job_id,
                                    "account_name": db_name,
                                    "timestamp": datetime.now().isoformat(),
                                }
                                if last_counts:
                                    periodic_payload.update(last_counts)
                                yield format_sse(periodic_payload, "message")
                        except Exception:
                            pass
                        await asyncio.sleep(min(PUBLISH_INTERVAL_SEC, max(0.1, poll_until - now)))
                    continue
                finally:
                    try:
                        if pubsub:
                            await pubsub.unsubscribe(channel)
                            await pubsub.close()
                    except Exception:
                        pass

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("[SSE] Error while streaming job_id=%s: %s", job_id, e)
            yield format_sse({"status": "error", "message": f"System Error: {str(e)}"}, "message")
        finally:
            crawler_state.active_clients.discard(client_id)

    # CORS 및 Charset 헤더 설정
    headers = {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    try:
        headers.update(_build_sse_cors_headers(request))
    except Exception:
        pass

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=headers)

@router.get("/debug/redis_state/{db_name}/{job_id}")
async def debug_redis_state(db_name: str, job_id: str):
    """
    주어진 db/job 조합의 Redis 상태와 메타 정보를 조회하는 임시 진단용 엔드포인트.
    """
    redis = await get_redis()
    conn_desc = describe_redis_connection(redis)
    state_key = _state_key(db_name, job_id)
    job_meta_key = f"job_meta:{job_id}"

    state = await redis.hgetall(state_key)
    job_meta = await redis.hgetall(job_meta_key)
    ttl = await redis.ttl(state_key)
    channel = f"crawl:{db_name}:{job_id}:progress"

    return {
        "redis_connection": conn_desc,
        "state_key": state_key,
        "state": state,
        "ttl": ttl,
        "channel": channel,
        "job_meta_key": job_meta_key,
        "job_meta": job_meta,
    }


@router.post("/debug/force_workflow_completed/{db_name}/{job_id}")
async def debug_force_workflow_completed(db_name: str, job_id: str, status: str = "completed"):
    """
    디버그 전용:
    - 긴 크롤링을 끝까지 기다리기 어려울 때, 완료(workflow_completed) 이벤트만 강제로 발행해서
      프론트 모달이 '완료 순간'에 비는지 여부를 빠르게 재현/검증한다.
    - HTML 수정 없이 Python만으로 완료 분기 테스트 가능.
    """
    normalized = _normalize_status_for_sse(status)
    payload = {
        "status": normalized,
        "event": "workflow_completed",
        "job_id": job_id,
        "account_name": db_name,
        "timestamp": datetime.now().isoformat(),
        # 최소 필드(프론트는 카운트/문구를 렌더링하므로 0 기본값 제공)
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "source": "debug_force_workflow_completed",
    }
    await _send_sse_message(job_id, payload, db_name, "workflow_completed")
    return {"ok": True, "job_id": job_id, "db_name": db_name, "status": normalized}


@router.get("/debug/sse_publish_queue")
async def debug_sse_publish_queue():
    # 신버전(모듈 분리) SSE publish 큐 상태
    return await debug_shared_sse_publish_queue()


@router.post("/debug/breadcrumb_option_bridge")
async def debug_breadcrumb_option_bridge(payload: Dict[str, Any]):
    """Return breadcrumb selector context for the learning-service bridge."""
    db_name = str((payload or {}).get("db_name") or "").strip()
    if not db_name:
        return JSONResponse(status_code=422, content={"detail": "db_name is required"})

    raw_learn_list_id = (payload or {}).get("learn_list_id")
    try:
        learn_list_id = int(raw_learn_list_id) if raw_learn_list_id not in (None, "") else None
    except (TypeError, ValueError):
        return JSONResponse(status_code=422, content={"detail": "learn_list_id must be an integer"})

    try:
        limit = max(1, min(int((payload or {}).get("limit") or 10), 50))
    except (TypeError, ValueError):
        limit = 10

    from backend.shared.breadcrumb_option_bridge import fetch_breadcrumb_option_bridge_payload

    return await fetch_breadcrumb_option_bridge_payload(
        db_name=db_name,
        learn_list_id=learn_list_id,
        contents_url=str((payload or {}).get("contents_url") or "").strip(),
        limit=limit,
    )

async def run_workflow_task(
    workflow: IntegratedWorkflow,
    start_urls: List[str],  # 탐색 시작 URL 목록 (query_links 또는 contents[0])
    start_date,
    end_date,
    job_id: str,
    craw_id: str,
    db_name: str = "default",
    chat_bot_id: str | None = None,
    use_query_links_only: bool = False,
):
    """워크플로우 실행 래퍼"""
    workflow.craw_id = craw_id # Store craw_id directly in the workflow object
    workflow.db_name = db_name
    workflow.job_id = job_id
    workflow.chat_bot_id = chat_bot_id
    
    # unique_id가 아직 설정되지 않았으면 조회하여 설정
    if not workflow.unique_id and chat_bot_id and db_name:
        # 백그라운드 태스크에서도 최대 3회 재시도 수행
        for attempt in range(3):
            try:
                from db.mariadb_save_update import get_account_identifier_from_chatbot_setup
                unique_id = await asyncio.wait_for(
                    get_account_identifier_from_chatbot_setup(
                        chat_bot_id=chat_bot_id,
                        db_name=db_name
                    ),
                    timeout=5.0
                )
                if unique_id:
                    unique_id = str(unique_id).upper().strip()
                    workflow.unique_id = unique_id
                    break
                else:
                    if attempt < 2:
                        await asyncio.sleep(1)
                        continue
            except (asyncio.TimeoutError, Exception) as exc:
                if attempt < 2:
                    await asyncio.sleep(1.5)
                else:
                    if chat_bot_id:
                        from db.mariadb_save_update import _extract_identifier_from_chat_bot_id
                        workflow.unique_id = _extract_identifier_from_chat_bot_id(chat_bot_id)

    # DB_name 체크포인트 testchatbot1???
    print(f"start_urls: {len(start_urls)}개, start_date: {start_date}, end_date: {end_date}, job_id: {job_id}, craw_id: {craw_id}, db_name: {db_name}", flush=True)
    crawler_state.record_history(job_id, "starting", "workflow_task_started", db_name, chat_bot_id=workflow.chat_bot_id)
    # ✅ 어떤 예외 경로를 타더라도, 마지막에는 workflow_completed(terminal) SSE를 1회 보장한다.
    # - 프론트는 completed/cancelled/error + event=workflow_completed 조합을 받아야 "종료"로 처리한다.
    terminal_sse_sent = False
    try:
        # 콜백 함수 정의
        def progress_callback(stats: Dict[str, Any]): # Add 'stats' parameter
            # 1. workflow.stats는 자동 업데이트됨 (IntegratedWorkflow 내부에서)
            
            # 2. Redis로 진행 상황 전송 (비동기)
            try:
                # [변경] 일관성을 위해 redis_sse_service 사용

                # 상태 결정: 중단 후 저장 중이면 상태를 "start"으로 유지하여 프론트엔드가 실시간 업데이트를 받을 수 있도록 함
                save_count = stats.get('save_count', 0)
                collection_count = stats.get('collection_count', 0)
                is_stopping_mode = workflow.final_status == "stopped" or workflow.state == WorkflowState.STOPPING
                is_saving_in_progress = is_stopping_mode and (save_count < collection_count)
                
                if is_saving_in_progress:
                    # 중단 버튼은 눌렸지만 저장/학습은 계속 진행 중.
                    # ✅ crawling_period.htm은 진행 분기를 status==='running'만 처리하므로 running 유지.
                    # 대신 stop_requested/status_hint로 '중단 후 저장중'을 표현한다.
                    current_status = "running"
                elif workflow.final_status:
                    current_status = _normalize_status_for_sse(workflow.final_status)
                elif not workflow.is_running:
                    # ✅ 중요: 프론트(crawling_period.htm)는 completed/cancelled/error/running만 처리한다.
                    # 'complete'를 그대로 보내면 분기 누락으로 progress_header가 빈 채로 남을 수 있다.
                    current_status = _normalize_status_for_sse("complete")
                else:
                    current_status = "running"

                # 메시지 구성
                message = {
                    "status": current_status,
                    # 용어 통일(호환): scan_count를 기본 키로 두고, total_count는 레거시 프론트 호환용으로 유지
                    "scan_count": stats.get('scan_count', 0),
                    "total_count": stats.get('scan_count', 0),
                    "collection_count": stats.get('collection_count', 0),
                    "save_count": stats.get('save_count', 0),
                    # ✅ study_count(표시/DB): "학습 성공 수"(chunks>0) 우선
                    "study_count": stats.get('study_success_count', stats.get('study_count', 0)),
                    "timestamp": datetime.now().isoformat(),
                    # ✅ 프론트(crawling_log.htm)는 data.job_id를 기준으로 화면 갱신한다.
                    "job_id": job_id,
                    "account_name": db_name,
                    # 실시간성(표시) 강화: 완료 카운트가 안 오르는 구간에서도
                    # 현재 진행 중/대기 중 상태를 프론트가 표기할 수 있게 제공
                    "pending_collection_count": stats.get("pending_collection_count", 0),
                    "pending_save_count": stats.get("pending_save_count", 0),
                    "in_flight": stats.get("in_flight", {}),
                }

                
                # 중단 후 저장 모드 표시 (프론트엔드에서 구분 가능하도록)
                if is_saving_in_progress:
                    message["stop_requested"] = True
                    message["status_hint"] = "stopped"
                    message["event"] = "stopping_save"
                    message["message"] = f"중단 후 저장 진행 중... (수집: {collection_count}, 저장: {save_count})"
                
                # 디버깅: 발행 전 메시지 확인 (중단 후 저장 시 상세 로그는 DEBUG로만 남김)
                if is_stopping_mode:
                    pass
                else:
                    pass
                # print(
                #     f"[SSE_DEBUG] 발행 메시지: job_id={job_id}, "
                #     f"total_count={message['total_count']}, "
                #     f"collection_count={message['collection_count']}, "
                #     f"save_count={message['save_count']}, "
                #     f"study_count={message['study_count']}",
                #     flush=True
                # )
                
                # 실시간성 우선: SSE 발행은 전용 워커(고우선/코얼레싱)로 전달
                enqueue_sse_message(job_id, message, db_name, "workflow_progress", priority=0)
                # print(f"[SSE_DEBUG] Message published to Redis task: job_id={job_id}, status={current_status}", flush=True)
            except Exception as e:
                pass
            pass

        await workflow.start_workflow(
            start_urls,
            progress_callback=progress_callback,
            start_date=start_date,
            end_date=end_date,
            use_query_links_only=use_query_links_only,
        )
        # NOTE: final_status는 내부 워크플로우 구현에 의해 "start/running"으로 남는 케이스가 있다.
        # 최종 메시지는 workflow_completed에서 반드시 terminal이 되어야 하므로,
        # 여기서는 raw만 보존하고 실제 terminal 매핑은 아래에서 수행한다.
        final_status = workflow.final_status or ("running" if workflow.is_running else "completed")
        
        crawler_state.record_history(job_id, final_status, "workflow_completed", db_name, chat_bot_id=workflow.chat_bot_id)
        
        # 저장 작업 완료 후 최신 통계 가져오기
        final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
        if not final_stats:
            final_stats = getattr(workflow, 'stats', {}) or {}

        # Safety: 내부의 비동기 후처리/트리거 태스크가 남아있다면 DB 업데이트 전에 대기 (best-effort)
        try:
            try:
                wait_sec = float(os.getenv("WORKFLOW_DB_UPDATE_WAIT_SEC", "30") or "30")
            except Exception:
                wait_sec = 30.0
            wait_sec = max(0.0, min(wait_sec, 600.0))
            pending_tasks = []
            for attr in ("_post_download_tasks", "_trigger_tasks", "_learn_tasks"):
                s = getattr(workflow, attr, None)
                if isinstance(s, (set, list)) and s:
                    for t in list(s):
                        try:
                            if isinstance(t, asyncio.Task) and not t.done():
                                pending_tasks.append(t)
                        except Exception:
                            pass
            if pending_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*pending_tasks, return_exceptions=True), timeout=wait_sec)
                except Exception:
                    pass
        except Exception:
            pass
        
        # DB에 최종 상태 업데이트 (완료/중지 시)
        # ✅ 요구사항: 중단하기를 눌렀을 때 status는 stop으로 기록한다.
        # 따라서 최종 종료 시점에도 stopped/cancelled는 stop으로 유지한다.
        db_status = None
        if final_status in ["completed", "stopped", "cancelled"]:
            db_status = "completed" if final_status == "completed" else "stop"
        
        if db_status:
            try:
                craw_id = getattr(workflow, "craw_id", None) or None
                await update_crawling_log_counters(
                    job_id=job_id,
                    scan=final_stats.get('scan_count'),
                    collection=final_stats.get('collection_count'),
                    saved=final_stats.get('save_count'),
                    study=final_stats.get('study_success_count', final_stats.get('study_count')),
                    status=db_status,
                    log_id=craw_id,
                    dbname=db_name,
                )
            except Exception as db_err:
                logger.error(
                    "[RunWorkflowTask] DB 상태 업데이트 실패 | job_id=%s status=%s err=%s",
                    job_id,
                    db_status,
                    db_err
                )
        
        # 워크플로우 완료 시 최종 집계 현황을 포함한 완료 메시지 전송
        try:
            # ✅ terminal status 강제 매핑 (프론트 종료 보장)
            raw_final = (workflow.final_status or final_status or "").strip().lower()
            if raw_final in STOP_SSE_STATUSES:
                terminal_status = "cancelled"
            elif raw_final in COMPLETE_SSE_STATUSES:
                terminal_status = "completed"
            elif raw_final in {"error", "failed", "fail", "exception"}:
                terminal_status = "error"
            else:
                # 남는 케이스(start/running/unknown)는 완료로 간주(종료 보장)
                terminal_status = "completed"

            final_message = {
                "status": terminal_status,
                "total_count": final_stats.get('scan_count', 0),
                "collection_count": final_stats.get('collection_count', 0),
                "save_count": final_stats.get('save_count', 0),
                "study_count": final_stats.get('study_success_count', final_stats.get('study_count', 0)),
                "timestamp": datetime.now().isoformat(),
                "event": "workflow_completed",
                # ✅ 프론트는 종료 이벤트에서도 job_id가 있어야 후속 DB 갱신 요청을 정상 수행한다.
                "job_id": job_id,
                "account_name": db_name,
            }
            print(
                f"[RunWorkflowTask] ✅ 워크플로우 완료 - 최종 집계: "
                f"total_count={final_message['total_count']}, "
                f"collection_count={final_message['collection_count']}, "
                f"save_count={final_message['save_count']}, "
                f"study_count={final_message['study_count']}",
                flush=True
            )
            await _send_sse_message(job_id, final_message, db_name, "workflow_completed")
            terminal_sse_sent = True
            print(
                f"[RunWorkflowTask] ✅ 완료 메시지 전송 확인: "
                f"status={final_message['status']}, "
                f"total_count={final_message['total_count']}, "
                f"collection_count={final_message['collection_count']}, "
                f"save_count={final_message['save_count']}, "
                f"study_count={final_message['study_count']}",
                flush=True
            )
        except Exception as exc:
            pass
    except Exception as e:
        logger.exception("[RunWorkflowTask] Workflow error for job_id=%s: %s", job_id, e)
        crawler_state.record_history(job_id, "failed_to_start", str(e), db_name, chat_bot_id=workflow.chat_bot_id)
        
        # DB에 status='error'로 업데이트
        try:
            final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
            if not final_stats:
                final_stats = getattr(workflow, 'stats', {}) or {}
            await update_crawling_log_counters(
                job_id=job_id,
                scan=final_stats.get('scan_count'),
                collection=final_stats.get('collection_count'),
                saved=final_stats.get('save_count'),
                study=final_stats.get('study_count'),
                status="error",
                dbname=db_name,
            )
        except Exception as db_err:
            logger.error("[RunWorkflowTask] Failed to update DB status to 'error' for job_id=%s: %s", job_id, db_err)
    finally:
        print(f"\n" + "="*50)
        print(f"🏁 작업 종료_router (Job ID: {job_id})")
        print(f"="*50 + "\n", flush=True)
        # ✅ 최종 SSE 보장(예외/조기 리턴/전송 실패 대비)
        if not terminal_sse_sent:
            try:
                # terminal status 강제 매핑 (최소: completed/cancelled/error 중 하나)
                raw_final = (getattr(workflow, "final_status", None) or "").strip().lower()
                if raw_final in STOP_SSE_STATUSES:
                    terminal_status = "cancelled"
                elif raw_final in COMPLETE_SSE_STATUSES:
                    terminal_status = "completed"
                elif raw_final in {"error", "failed", "fail", "exception"}:
                    terminal_status = "error"
                else:
                    # 작업 종료 로그가 찍혔는데도 상태가 비정상이면 completed로 종료 처리
                    terminal_status = "completed"

                try:
                    final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
                except Exception:
                    final_stats = {}
                if not final_stats:
                    final_stats = getattr(workflow, "stats", {}) or {}

                final_message = {
                    "status": terminal_status,
                    "total_count": final_stats.get("scan_count", 0),
                    "collection_count": final_stats.get("collection_count", 0),
                    "save_count": final_stats.get("save_count", 0),
                    "study_count": final_stats.get("study_count", 0),
                    "timestamp": datetime.now().isoformat(),
                    "event": "workflow_completed",
                    "job_id": job_id,
                    "account_name": db_name,
                }
                await _send_sse_message(job_id, final_message, db_name, "workflow_completed:finally")
                terminal_sse_sent = True
            except Exception as exc:
                pass
        # ✅ 요구사항: 작업 종료 로그 이후 즉시 강제 종료(자원 해제) 모드
        # - 기본 ON (원치 않으면 WORKFLOW_FORCE_TERMINATE_AFTER_FINISH=0)
        if _env_bool("WORKFLOW_FORCE_TERMINATE_AFTER_FINISH", "1"):
            try:
                await _force_terminate_job_after_finish(workflow=workflow, job_id=job_id, db_name=db_name)
            except Exception:
                pass
            return

        # ✅ 완료 후 정리 작업은 "분리(detach)"하여 run_workflow_task가 즉시 종료되게 한다.
        # - 기존에는 await sleep(60) 때문에 job 완료 후에도 task가 60초 동안 살아있어
        #   운영 로그/모니터링에서 "서버가 안 죽는 것처럼" 보일 수 있었다.
        # - 클라이언트가 결과를 받을 시간 확보는 동일하게 유지하되, cleanup은 백그라운드 task로 처리.
        try:
            delay_sec_default = 60
            try:
                delay_sec = int(os.getenv("WORKFLOW_CLEANUP_DELAY_SEC", str(delay_sec_default)) or delay_sec_default)
            except Exception:
                delay_sec = delay_sec_default
            delay_sec = max(0, min(int(delay_sec), 3600))
        except Exception:
            delay_sec = 60

        async def _cleanup_after_delay():
            try:
                if delay_sec > 0:
                    await asyncio.sleep(delay_sec)
                crawler_state.workflows.pop(job_id, None)
                prev_status = crawler_state.job_history.get(job_id, {}).get("status")
                if prev_status not in {"failed_to_start", "creation_failed"}:
                    cleanup_detail = f"auto_cleanup_after_{prev_status or 'unknown'}"
                    crawler_state.record_history(job_id, "cleaned", cleanup_detail, db_name, chat_bot_id=workflow.chat_bot_id)
            except Exception as exc:
                pass

        try:
            asyncio.create_task(_cleanup_after_delay(), name=f"workflow-cleanup:{job_id}")
        except Exception:
            # create_task가 불가능하면 기존 방식으로 fallback(최악의 경우)
            if delay_sec > 0:
                await asyncio.sleep(delay_sec)
            crawler_state.workflows.pop(job_id, None)