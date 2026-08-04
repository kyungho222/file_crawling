# core/crawler/workers/collection.py
import asyncio
import logging
import aiohttp
import time
import os
from email.utils import parsedate_to_datetime
from typing import List, Dict, Callable, Optional, Awaitable, Tuple
from core.crawler.batch_queue import BatchQueue
from db.repository import DBRepository
from config.constants import COLLECTION_EXTENSIONS, ALLOWED_EXTENSIONS, IMG_EXTENSIONS, BOARD_PATTERNS, EXCLUDE_URL_PATTERNS
from core.crawler.dedup import CollectionDeduplicator
from db.mysql_db_config import mysql_execute_query
from utils.url import canonicalize_url_for_dedup
import sys
import os
# 프로젝트 루트를 sys.path에 추가하여 backend.shared.date_utils import 가능하도록
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from backend.shared.date_utils import is_date_in_range
from datetime import datetime
import json

logger = logging.getLogger(__name__)
FLOW_DEBUG = os.getenv("CRAWL_DEBUG_FLOW", "0") == "1"
if FLOW_DEBUG:
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass

# ✅ 선별 단계 중복 체크 정책:
# - 중복 체크는 "선별(Collection) 단계에서만" 수행한다.
# - 기준은 MariaDB의 ASADAL_{token}_LEARN_LIST 테이블의 `url` 컬럼 값이다.
# - (호환) 테이블에 `url`이 없으면 `content`로 fallback 한다.
_learn_list_url_col_cache: Dict[Tuple[str, str], str] = {}

async def _get_learn_list_url_column(db_name: str, table_name: str) -> str:
    """
    LEARN_LIST 테이블에서 URL을 저장하는 컬럼명을 결정한다.
    우선순위: url -> content
    """
    if not db_name or not table_name:
        return "url"
    key = (db_name, table_name)
    cached = _learn_list_url_col_cache.get(key)
    if cached:
        return cached
    try:
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
        """
        rows = await mysql_execute_query(sql, (db_name, table_name), fetch=True, dbname=db_name)
        cols = set()
        for r in rows or []:
            if isinstance(r, dict) and r.get("column_name"):
                cols.add(str(r["column_name"]).lower())
        if "url" in cols:
            _learn_list_url_col_cache[key] = "url"
            return "url"
        # 레거시(프로젝트 일부): content 컬럼에 URL을 저장
        if "content" in cols:
            _learn_list_url_col_cache[key] = "content"
            return "content"
    except Exception:
        pass
    # 기본은 url
    _learn_list_url_col_cache[key] = "url"
    return "url"

# 선별(HEAD) 타임아웃: 느린 서버가 섞이면 collection_count가 '안 오르는 것처럼' 보일 수 있다.
# 운영에서는 ENV로 조절한다.
try:
    COLLECTION_HEAD_TIMEOUT_SEC = float(os.getenv("COLLECTION_HEAD_TIMEOUT_SEC", "5") or "5")
except Exception:
    COLLECTION_HEAD_TIMEOUT_SEC = 5.0
COLLECTION_HEAD_TIMEOUT_SEC = max(1.0, min(COLLECTION_HEAD_TIMEOUT_SEC, 30.0))

# ==================== FORCE COLLECTION (HEAD 우회) ====================
# 일부 서버는 HEAD를 막거나(405), Referer/Origin이 없으면 HTML/403을 반환한다.
# 이 경우 back01처럼 "선별 카운트는 올리고 다운로드 단계(GET)에서 최종 판정"을 하도록 우회 옵션을 둔다.
def _env_bool(key: str, default: str = "0") -> bool:
    try:
        return str(os.getenv(key, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"

# 기본값은 ON.
# 관공서/포털의 fileDown/download handler는 HEAD를 막는 경우가 매우 흔해서
# 선별 카운트가 "안 오르는 것처럼" 보이는 문제를 유발한다.
# 필요 시 ENV(COLLECTION_FORCE_ENABLE=0)로 끌 수 있다.
COLLECTION_FORCE_ENABLE = _env_bool("COLLECTION_FORCE_ENABLE", "1")
COLLECTION_FORCE_ON_TIMEOUT = _env_bool("COLLECTION_FORCE_ON_TIMEOUT", "1")
COLLECTION_FORCE_ON_403_401 = _env_bool("COLLECTION_FORCE_ON_403_401", "1")
COLLECTION_FORCE_ON_405 = _env_bool("COLLECTION_FORCE_ON_405", "1")
COLLECTION_FORCE_ON_HTML = _env_bool("COLLECTION_FORCE_ON_HTML", "1")

try:
    COLLECTION_FORCE_GET_PROBE = _env_bool("COLLECTION_FORCE_GET_PROBE", "1")
except Exception:
    COLLECTION_FORCE_GET_PROBE = True
try:
    COLLECTION_FORCE_GET_TIMEOUT_SEC = float(os.getenv("COLLECTION_FORCE_GET_TIMEOUT_SEC", "5") or "5")
except Exception:
    COLLECTION_FORCE_GET_TIMEOUT_SEC = 5.0
COLLECTION_FORCE_GET_TIMEOUT_SEC = max(1.0, min(COLLECTION_FORCE_GET_TIMEOUT_SEC, 30.0))

def _is_likely_file_url(url: str) -> bool:
    if not url:
        return False
    lu = url.lower()
    path = lu.split("?")[0]
    if any(path.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return True
    # handler/다운로드 패턴(확장자 없이도 파일을 내려주는 URL)
    file_hints = (
        "filedown", "download", "file.do", "filedownload", "cmm/fms/filedown",
        "atchfile", "atchfileid", "filesn", "fileid", "fileSeq", "file_no",
    )
    return any(h in lu for h in file_hints)

# 허용 MIME 타입 (확장자 없는 URL 처리용)
ALLOWED_MIME_TYPES = [
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "image/jpeg",
    "image/png",
    "image/gif",
]

def _extract_filename_ext_from_content_disposition(cd: str) -> str:
    """
    Content-Disposition에서 filename 확장자를 best-effort로 추출한다.
    예: attachment; filename="file.pdf"
    """
    if not cd:
        return ""
    try:
        s = str(cd)
    except Exception:
        return ""
    # RFC 5987 filename*=UTF-8''... 는 단순 처리(따옴표/세미콜론 제거 후 마지막 점 기준)
    try:
        import re as _re
        m = _re.search(r'filename\*?=(?:UTF-8\'\')?("?)([^";\r\n]+)\1', s, flags=_re.IGNORECASE)
        if not m:
            return ""
        fn = (m.group(2) or "").strip()
        # URL-encoding 된 경우 복호화(best-effort)
        try:
            from urllib.parse import unquote
            fn = unquote(fn)
        except Exception:
            pass
        fn = fn.strip().strip('"').strip("'")
        if "." not in fn:
            return ""
        ext = "." + fn.rsplit(".", 1)[-1].lower()
        return ext
    except Exception:
        return ""

async def _validate_item(
    item: Dict,
    session: aiohttp.ClientSession,
    deduplicator: Optional[CollectionDeduplicator],
    chat_bot_id: Optional[str],
    db_name: Optional[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime]
) -> Tuple[Optional[Dict], str]:
    """한 개의 아이템을 검증하는 헬퍼 함수"""
    url = item.get('url')
    if not url:
        return None, "missing_url"

    try:
        # ===== yong님 요청: 최종 선별을 위한 전용 필터링 (Primary Filter) =====
        lowered_url = url.lower()
        
        # 1. 제외 패턴 필터링 (로그인, 회원가입, 단순 안내 페이지 등)
        if any(p in lowered_url for p in EXCLUDE_URL_PATTERNS):
            logger.debug(f"[Collection] 🛑 선별 탈락: 제외 패턴 포함 | {url}")
            return None, "excluded_pattern"
            
        # 2. 게시판 패턴 및 파일 성격 확인
        # - 직접 파일 주소인 경우(ALLOWED_EXTENSIONS 포함)는 게시판 키워드 상관없이 통과
        # - 확장자 없는 fileDown/download handler도 파일로 강하게 추정되면 통과해야 한다.
        is_direct_file = any(lowered_url.split('?')[0].endswith(ext) for ext in ALLOWED_EXTENSIONS)
        is_board = None
        try:
            likely_file = _is_likely_file_url(url) or (str(item.get("type") or "").lower() == "file")
        except Exception:
            likely_file = False
        if not is_direct_file:
            is_board = any(p in lowered_url for p in BOARD_PATTERNS)
        if not is_direct_file:
            if not is_board and not likely_file:
                logger.debug(f"[Collection] 🛑 선별 탈락: 게시판 패턴 아님 | {url}")
                return None, "not_board_or_file"

        # 3. 날짜 추출 및 기간 선제 필터링
        reg_date_str = item.get('reg_date')
        item_name = item.get('name', '')
        
        # 제목이나 라벨에서 날짜 재추출 시도 (날짜 정보가 누락되어 넘어온 경우)
        if not reg_date_str and item_name:
            try:
                from backend.shared.date_utils import extract_date_from_text
                found_date = extract_date_from_text(item_name)
                if found_date:
                    reg_date_str = found_date.strftime('%Y-%m-%d')
            except Exception:
                pass

        # 기간 필터는 Scan 단계에서 처리한다.
        # Collection은 Scan이 전달한 후보에 대해 중복/유효성(HEAD) 검증만 수행한다.

        # 4. 수집 중복 체크 (DB) - 선별 단계에서만 수행
        if chat_bot_id and db_name:
            try:
                # 테이블명 결정 로직은 저장 단계(db.mariadb_save_update)와 동일하게 맞춘다.
                from db.mariadb_save_update import get_account_identifier_from_chatbot_setup, get_learn_list_table_name
                account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
                table = get_learn_list_table_name(account_identifier)
                url_col = await _get_learn_list_url_column(db_name, table)

                # Use canonicalization + host-variant candidates to match insert_into_learn_list behavior
                canon_url = canonicalize_url_for_dedup(url) or None
                url_candidates = []
                try:
                    if isinstance(url, str) and url:
                        url_candidates.append(url)
                except Exception:
                    pass
                try:
                    if isinstance(canon_url, str) and canon_url and canon_url not in url_candidates:
                        url_candidates.append(canon_url)
                except Exception:
                    pass

                # add www / non-www host variants for both raw and canonical if possible
                try:
                    from urllib.parse import urlparse, urlunparse
                    for candidate in list(url_candidates):
                        try:
                            p = urlparse(candidate)
                            host = p.netloc or ""
                            if host:
                                if host.startswith("www."):
                                    alt = host[len("www."):]
                                else:
                                    alt = "www." + host
                                alt_url = urlunparse(p._replace(netloc=alt))
                                if alt_url not in url_candidates:
                                    url_candidates.append(alt_url)
                        except Exception:
                            pass
                except Exception:
                    pass

                # perform one IN-query to check any candidate presence
                if url_candidates:
                    placeholders = ",".join(["%s"] * len(url_candidates))
                    sql = f"SELECT id FROM `{table}` WHERE `{url_col}` IN ({placeholders}) LIMIT 1"
                    rows = await mysql_execute_query(sql, tuple(url_candidates), fetch=True, dbname=db_name)
                    if rows:
                        logger.debug(f"[Collection] 🔄 선별 탈락: 이미 DB에 존재함({url_col}) | {url} candidates={len(url_candidates)}")
                        return None, "duplicate_db"
            except Exception as e:
                logger.warning(f"[Collection] DB 중복 체크 실패(통과): {url}, {e}")
        
        # 5. HEAD 요청을 통한 파일 유효성 최종 검증
        headers = {'Referer': item.get('source_page', '')}
        likely_file = _is_likely_file_url(url) or (str(item.get("type") or "").lower() == "file")

        async def _accept_forced(reason: str) -> Optional[Dict]:
            if not (COLLECTION_FORCE_ENABLE and likely_file):
                return None
            # 날짜는 scan에서 통과한 후보일 때만 들어오므로, 없더라도 강제 통과는 허용(기존 요구사항과 충돌 시 ENV로 끄면 됨)
            item['reg_date'] = reg_date_str
            item['forced'] = True
            item['force_reason'] = reason
            logger.warning("[Collection] ⚠️ FORCE accept | reason=%s url=%s", reason, url)
            return item

        async def _probe_get() -> Optional[Dict]:
            """HEAD가 막힌 서버를 위해 Range GET로 최소 판정(가능하면 강제 통과보다 우선)."""
            # GET probe는 비용이 크지 않도록 Range(2KB)로 제한하며,
            # fileDown/download handler류(=likely_file)에서는 FORCE가 꺼져 있어도 판정용으로 수행한다.
            if not (COLLECTION_FORCE_GET_PROBE and likely_file):
                return None
            try:
                req_headers = dict(headers)
                # Range 지원 시 2KB만 받아서 HTML 여부를 빠르게 판정
                req_headers["Range"] = "bytes=0-2047"
                async with session.get(
                    url,
                    headers=req_headers,
                    timeout=COLLECTION_FORCE_GET_TIMEOUT_SEC,
                    allow_redirects=True,
                ) as r:
                    # 일부 서버는 Range를 무시하고 200으로 내려줄 수 있음
                    if r.status not in (200, 206):
                        return None
                    ct = (r.headers.get("content-type") or "").lower()
                    # HTML이면 파일이 아닐 확률이 매우 높다(차단/로그인/안내 페이지).
                    # 따라서 "통과"가 아니라 "거절"로 처리한다.
                    if "text/html" in ct:
                        return None
                    # 확장자/허용 MIME 체크
                    url_path = url.lower().split('?')[0]
                    is_allowed_ext = any(url_path.endswith(ext) for ext in ALLOWED_EXTENSIONS)
                    is_allowed_mime = any(mime in ct for mime in ALLOWED_MIME_TYPES)
                    if not is_allowed_ext and not is_allowed_mime:
                        return None
                    item['reg_date'] = reg_date_str
                    return item
            except Exception:
                return None

        try:
            async with session.head(url, headers=headers, timeout=COLLECTION_HEAD_TIMEOUT_SEC, allow_redirects=True) as response:
                if response.status != 200:
                    # HEAD 차단/인증/리다이렉트 정책이 강하면 GET은 성공하는 케이스가 있다.
                    if response.status in (405, 401, 403):
                        probed = await _probe_get()
                        if probed:
                            return probed, "probe_get_ok"
                        # 마지막 수단(옵션): 강제 통과
                        if response.status == 405 and COLLECTION_FORCE_ON_405:
                            forced = await _accept_forced("head_405")
                            if forced:
                                return forced, "forced:head_405"
                        if response.status in (401, 403) and COLLECTION_FORCE_ON_403_401:
                            forced = await _accept_forced(f"head_{response.status}")
                            if forced:
                                return forced, f"forced:head_{response.status}"
                    logger.debug(f"[Collection] 🛑 선별 탈락: HTTP 상태 {response.status} | {url}")
                    return None, f"head_status_{response.status}"

                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' in content_type:
                    # HEAD가 HTML을 주더라도 GET은 파일을 주는 서버가 있다(Referer/Origin 필요 등).
                    probed = await _probe_get()
                    if probed:
                        return probed, "probe_get_ok"
                    if COLLECTION_FORCE_ON_HTML:
                        forced = await _accept_forced("head_html")
                        if forced:
                            return forced, "forced:head_html"
                    logger.debug(f"[Collection] 🛑 선별 탈락: HTML 페이지임 | {url}")
                    return None, "content_type_html"

                url_path = url.lower().split('?')[0]
                is_allowed_ext = any(url_path.endswith(ext) for ext in ALLOWED_EXTENSIONS)
                is_allowed_mime = any(mime in content_type for mime in ALLOWED_MIME_TYPES)

                if not is_allowed_ext and not is_allowed_mime:
                    # fileDown/download handler는 MIME이 application/octet-stream으로 떨어지는 케이스가 많다.
                    # - 이 경우 Content-Disposition의 filename 확장자 또는 octet-stream 자체를 허용(단, likely_file일 때만)
                    try:
                        cd = response.headers.get("content-disposition", "") or ""
                    except Exception:
                        cd = ""
                    ext_from_cd = _extract_filename_ext_from_content_disposition(cd)
                    if likely_file:
                        if ext_from_cd and any(ext_from_cd == ext for ext in ALLOWED_EXTENSIONS):
                            item["head_content_disposition"] = cd
                            item["head_filename_ext"] = ext_from_cd
                            item["head_content_type"] = content_type
                            return item, "ok_cd_ext"
                        if "application/octet-stream" in content_type or "binary/octet-stream" in content_type:
                            item["head_content_disposition"] = cd
                            item["head_filename_ext"] = ext_from_cd
                            item["head_content_type"] = content_type
                            return item, "ok_octet_stream"
                    logger.debug(f"[Collection] 🛑 선별 탈락: 허용되지 않는 문서 형식 | {url} ct={content_type} cd={cd[:80]}")
                    return None, "disallowed_mime"
                
                if response.headers.get('content-length') == '0':
                    return None, "content_length_0"

                item['reg_date'] = reg_date_str
                return item, "ok"

        except asyncio.TimeoutError:
            if COLLECTION_FORCE_ON_TIMEOUT:
                forced = await _accept_forced("head_timeout")
                if forced:
                    return forced, "forced:head_timeout"
            return None, "head_timeout"

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        # 네트워크 계열 오류에서도 파일 URL이면 강제 통과 옵션 적용 가능
        if COLLECTION_FORCE_ON_TIMEOUT and isinstance(e, asyncio.TimeoutError):
            try:
                if COLLECTION_FORCE_ENABLE and _is_likely_file_url(url):
                    item['reg_date'] = item.get('reg_date')
                    item['forced'] = True
                    item['force_reason'] = "head_timeout_exc"
                    logger.warning("[Collection] ⚠️ FORCE accept (timeout exc) | url=%s", url)
                    return item, "forced:head_timeout_exc"
            except Exception:
                pass
        logger.debug(f"[Validation] 네트워크 오류로 건너뜀: {url} ({type(e).__name__})")
        return None, f"network_error_{type(e).__name__}"
    except Exception as e:
        logger.error(f"[Validation] 알 수 없는 오류: {url}, {e}", exc_info=True)
        return None, f"unexpected_{type(e).__name__}"

"""
수집(Collection) 워커 - 실시간 처리
- Scan 단계에서 넘어온 링크들을 즉시 처리
- HEAD 요청으로 파일 유효성 사전 검증 (HTTP 상태, Content-Type, Content-Length)
- 중복 검사 (DB)
- 확장자 필터링
- 유효한 파일만 Download 단계로 전달
"""
async def collection_worker(
    in_queue: BatchQueue,
    out_queue: Optional[BatchQueue],
    progress_queue: asyncio.Queue,
    db_repo: DBRepository,
    deduplicator: CollectionDeduplicator | None = None,
    on_valid_batch: Optional[Callable[[List[Dict]], Awaitable[None]]] = None,
    forward_to_queue: bool = True,
    chat_bot_id: Optional[str] = None,
    db_name: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
):
    """
    Collection Worker (Batch):
    - in_queue (ScanBatchQueue)에서 배치(List)를 가져옴
    - HEAD 요청으로 파일 유효성 검증
    - 중복 검사 및 필터링 수행
    - out_queue (CollectionBatchQueue)로 전달
    - 진행 상황은 progress_queue를 통해 알림
    """
    current_task = asyncio.current_task()
    worker_label = current_task.get_name() if current_task and current_task.get_name() else f"id={id(current_task)}"
    logger.debug("[Collection][%s] Worker started, waiting for items...", worker_label)
    wait_logged = False
    wait_log_interval = 30.0
    last_wait_log = 0.0
    # 배치 요약 로그 rate-limit (스팸 방지)
    _last_batch_print_at_by_job: Dict[str, float] = {}
    _batch_print_interval_sec = 10.0
    
    # User-Agent 헤더 추가
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # ===== job_id 격리 =====
    # 전역 워커풀(멀티 job_id)에서 collection_worker를 공유할 수 있도록,
    # deduplicator/컨텍스트를 job_id 단위로 분리한다.
    _dedup_by_job: Dict[str, CollectionDeduplicator] = {}

    # DB 중복 캐시(job 단위): 게시판 워크플로우와 동일하게
    _db_dup_checked_by_job: Dict[str, Set[str]] = {}
    _db_dup_urls_by_job: Dict[str, Set[str]] = {}
    # 작은 helper: 즉시 반환하는 duplicate 코루틴
    async def _dup_result():
        return (None, "duplicate_db")

    _last_pause_log: Dict[str, float] = {}
    async with aiohttp.ClientSession(headers=headers) as session:
        heartbeat_interval = 30  # 30초마다 하트비트 로그
        last_heartbeat = time.monotonic()
        
        while True:
            batch_items = None
            try:
                # 1. 배치 가져오기 (타임아웃 추가: 3초마다 하트비트 확인)
                now = time.monotonic()
                try:
                    # 1. 아이템 가져오기 (실시간 처리)
                    batch_items: List[Dict] = await asyncio.wait_for(in_queue.get(), timeout=3.0)
                    wait_logged = False
                    # 아이템을 받았으면 하트비트 시간 업데이트
                    last_heartbeat = now
                except asyncio.TimeoutError:
                    # 큐가 비어있어도 하트비트 로그 출력 (워커가 살아있음을 확인)
                    if now - last_heartbeat >= heartbeat_interval:
                        # 버퍼 상태도 확인하여 로그 출력
                        buffer_size = 0
                        queue_size = 0
                        try:
                            if hasattr(in_queue, "buffer"):
                                buffer_size = len(getattr(in_queue, "buffer") or [])
                        except Exception:
                            buffer_size = 0
                        # MultiplexBatchQueue는 .queue가 없을 수 있으므로 qsize() 우선 사용
                        try:
                            if hasattr(in_queue, "qsize"):
                                queue_size = int(in_queue.qsize() or 0)
                            else:
                                q_obj = getattr(in_queue, "queue", None)
                                if q_obj is not None and hasattr(q_obj, "qsize"):
                                    queue_size = int(q_obj.qsize() or 0)
                        except Exception:
                            queue_size = 0
                        # STOP 이후에도 저장/학습 마무리를 위해 워커가 잠시 유지될 수 있다.
                        # 운영 로그 소음을 줄이기 위해 heartbeat는 DEBUG로만 남긴다.
                        logger.debug(f"[Collection] Worker alive, waiting for item... (queue={queue_size})")
                        last_heartbeat = now
                    
                    # 기존 wait 로그도 유지
                    if not wait_logged or (now - last_wait_log) >= wait_log_interval:
                        wait_logged = True
                        last_wait_log = now
                    continue
                
                if not batch_items:
                    continue

                # 배치 job_id 추정(대부분 단일 job_id)
                try:
                    job_ids = {str((it or {}).get("job_id") or "default") for it in (batch_items or [])}
                except Exception:
                    job_ids = {"default"}
                batch_job_id = next(iter(job_ids)) if len(job_ids) == 1 else "multi"

                # backpressure: collection pause
                try:
                    from core.crawler.queues import get_job_pause_flags
                    paused = bool(get_job_pause_flags(batch_job_id).get("collection", False))
                except Exception:
                    paused = False
                if paused:
                    now = time.monotonic()
                    last = _last_pause_log.get(batch_job_id, 0.0)
                    if now - last >= 5.0:
                        _last_pause_log[batch_job_id] = now
                        # 디버그 파일 쓰기를 제거하고 로거로 대체
                        try:
                            payload = {
                                "sessionId": "debug-session",
                                "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
                                "hypothesisId": "H_collection_paused",
                                "location": "core/crawler/workers/collection.py:collection_worker",
                                "message": "collection_paused",
                                "data": {"job_id": batch_job_id, "batch_size": len(batch_items)},
                                "timestamp": int(time.time() * 1000),
                            }
                            logger.debug("AGENT_DEBUG %s", json.dumps(payload, ensure_ascii=False))
                        except Exception:
                            pass
                        # #endregion
                    await asyncio.sleep(0.2)
                    continue

                # 2. 각 항목을 병렬로 검증 (job_id별 컨텍스트 적용)
                validation_tasks = []
                for item in batch_items:
                    try:
                        jid = str((item or {}).get("job_id") or "default")
                    except Exception:
                        jid = "default"

                    eff_dedup = deduplicator
                    if eff_dedup is None:
                        eff_dedup = _dedup_by_job.setdefault(jid, CollectionDeduplicator())

                    eff_chat_bot_id = (item or {}).get("chat_bot_id") or chat_bot_id
                    eff_db_name = (item or {}).get("db_name") or db_name
                    eff_start_date = (item or {}).get("start_date") or start_date
                    eff_end_date = (item or {}).get("end_date") or end_date
                    # prepare DB-dup caches for this job
                    _db_dup_checked_by_job.setdefault(jid, set())
                    _db_dup_urls_by_job.setdefault(jid, set())

                    # canonicalize url key for caching/lookup
                    try:
                        raw_url = (item or {}).get("url") or ""
                        url_key = canonicalize_url_for_dedup(str(raw_url)) or str(raw_url)
                    except Exception:
                        url_key = (item or {}).get("url") or ""

                    # If we've already seen this URL as DB-duplicate for this job, skip validation and emit duplicate result
                    if url_key and url_key in _db_dup_urls_by_job.get(jid, set()):
                        validation_tasks.append(_dup_result())
                    else:
                        validation_tasks.append(
                            _validate_item(item, session, eff_dedup, eff_chat_bot_id, eff_db_name, eff_start_date, eff_end_date)
                        )
                results = await asyncio.gather(*validation_tasks, return_exceptions=True)
                
                
                # [용님 요청] 실시간성 향상: 검증 완료된 아이템 즉시 개별 전송
                valid_items = []
                skipped_total = 0
                reject_reasons: Dict[str, int] = {}
                reject_samples: List[Tuple[str, str]] = []
                forced_count = 0
                force_reasons: Dict[str, int] = {}
                # iterate with index to map back to original item for rejected cases
                for idx, result in enumerate(results):
                    item = batch_items[idx] if batch_items and idx < len(batch_items) else {}
                    if isinstance(result, Exception):
                        skipped_total += 1
                        rsn = f"task_exception_{type(result).__name__}"
                        reject_reasons[rsn] = int(reject_reasons.get(rsn, 0) or 0) + 1
                        continue
                    try:
                        validated, rsn = result  # type: ignore[misc]
                    except Exception:
                        skipped_total += 1
                        rsn = "invalid_validate_return"
                        reject_reasons[rsn] = int(reject_reasons.get(rsn, 0) or 0) + 1
                        continue

                    if isinstance(validated, dict):
                        valid_items.append(validated)
                        try:
                            if bool(validated.get("forced")):
                                forced_count += 1
                                fr = str(validated.get("force_reason") or "unknown")
                                force_reasons[fr] = int(force_reasons.get(fr, 0) or 0) + 1
                        except Exception:
                            pass
                        
                        # 개별 아이템 즉시 전송 (실시간 처리)
                        if on_valid_batch:
                            try:
                                await on_valid_batch([validated])  # 단일 아이템을 리스트로 감싸서 전송
                            except Exception as callback_err:
                                logger.warning(f"[Collection] on_valid_batch callback failed: {callback_err}")
                        
                        if forward_to_queue and out_queue:
                            await out_queue.put(validated)
                        
                        # 수집 카운트 개별 보고
                        try:
                            eff_job_id = str(validated.get("job_id") or "default")
                        except Exception:
                            eff_job_id = "default"
                        await progress_queue.put({
                            'type': 'collection',
                            'count': 1,
                            'items': [validated.get('url')],
                            'names': [validated.get('name') or validated.get('title') or ''],
                            # ✅ GlobalWorkerPool(MultiplexProgressQueue) 라우팅 안정성: job_id를 명시한다.
                            'job_id': eff_job_id,
                        })
                        # 즉시 JSON 파일 생성(실시간 확인용) - integrated_workflow의 save_worker_log_json 호출
                        try:
                            # import locally to avoid potential circular imports at module load time
                            from backend.file.integrated_workflow import save_worker_log_json
                            try:
                                save_worker_log_json(
                                    job_id=eff_job_id,
                                    normed_urls=[validated.get('url')],
                                    names=[validated.get('name') or validated.get('title') or ''],
                                    collection_count=int((validated.get('collection_count') or 0) or 0)
                                )
                                # 디버깅 출력: 생성 시그널
                                try:
                                    print(f"[test040] collection immediate json created for job_id={eff_job_id} url={validated.get('url')}", flush=True)
                                except Exception:
                                    pass
                            except Exception:
                                # 실패해도 워커 흐름에 영향주지 않음
                                pass
                        except Exception:
                            # import 실패 무시
                            pass
                        if FLOW_DEBUG:
                            try:
                                logger.info(
                                    "[Flow] file_found | job_id=%s url=%s source_page=%s",
                                    eff_job_id,
                                    str(validated.get("url") or "")[:220],
                                    str(validated.get("source_page") or "")[:220],
                                )
                            except Exception:
                                pass
                        
                    else:
                        skipped_total += 1
                        rsn = str(rsn or "rejected_unknown")
                        reject_reasons[rsn] = int(reject_reasons.get(rsn, 0) or 0) + 1
                        # 게시판 동작과 동일하게 DB 중복인 경우에는 scan 카운트를 올려줌
                        if rsn in ("duplicate_db", "duplicate_db_cached"):
                            try:
                                u = str((item or {}).get("url") or "")
                            except Exception:
                                u = ""
                            if u:
                                try:
                                    # emit scan progress so workflow can bump scan_count but not collection_count
                                    await progress_queue.put({'type': 'scan', 'count': 1, 'items': [u], 'job_id': str((item or {}).get('job_id') or 'default')})
                                except Exception:
                                    pass
                            # cache this url_key to avoid repeated DB checks
                            try:
                                jid = str((item or {}).get("job_id") or "default")
                                url_key = canonicalize_url_for_dedup(u) or u
                                if url_key:
                                    _db_dup_urls_by_job.setdefault(jid, set()).add(url_key)
                            except Exception:
                                pass
                        # 샘플은 최대 3개만(스팸 방지)
                        if len(reject_samples) < 3:
                            try:
                                u = str((item or {}).get("url") or "")
                            except Exception:
                                u = ""
                            if u:
                                reject_samples.append((rsn, u))

                # 배치 처리 요약 출력은 디버깅 성격이 강하므로 제거(필요 시 logger.debug로 확인)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Collection] Error: {e}")
            finally:
                if batch_items is not None:
                    progress_queue.put_nowait({
                        'type': 'in_flight',
                        'stage': 'collection',
                        'delta': -len(batch_items)
                    })
                    in_queue.task_done()
