# core/crawler/workers/collection.py
import asyncio
import logging
import aiohttp
import time
import os
import sys
import json # JSON 파일 생성용 모듈 추가
from datetime import datetime # 저장 시각 기록용 모듈 추가
from email.utils import parsedate_to_datetime
from typing import List, Dict, Callable, Optional, Awaitable, Tuple
from core.crawler.batch_queue import BatchQueue
from db.repository import DBRepository
from config.constants import COLLECTION_EXTENSIONS, ALLOWED_EXTENSIONS, IMG_EXTENSIONS, BOARD_PATTERNS, EXCLUDE_URL_PATTERNS
from core.crawler.dedup import CollectionDeduplicator
from db.mysql_db_config import mysql_execute_query
from utils.url import canonicalize_url_for_dedup

# 프로젝트 루트를 sys.path에 추가하여 backend.shared.date_utils import 가능하도록 설정
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from backend.shared.date_utils import is_date_in_range

logger = logging.getLogger(__name__)
FLOW_DEBUG = os.getenv("CRAWL_DEBUG_FLOW", "0") == "1"
if FLOW_DEBUG:
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass

# =================================================================
# [추가] 통합 워크플로우에 의존하지 않는 독립적인 선별 통과 JSON 생성 함수
# =================================================================
def force_save_collection_log_json(job_id: str, urls: list, names: list):
    """선별(Collection) 단계를 최종 통과한 URL만 모아서 별도의 JSON 파일로 강제 기록합니다."""
    if not job_id or not urls: return
    try:
        # 파일이 저장될 워커 전용 폴더 경로를 생성합니다.
        target_dir = os.path.join(project_root, "core", "crawler", "workers")
        os.makedirs(target_dir, exist_ok=True)
        # scan 단계와 구분하기 위해 파일명을 collection_passed_... 로 지정합니다.
        filepath = os.path.join(target_dir, f"collection_passed_{job_id}.json")
        
        data = []
        # 기존 파일이 있다면 데이터를 불러와서 이어쓰기를 준비합니다.
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                
        added_count = 0
        # 중복되지 않는 새로운 URL 정보만 목록에 추가합니다.
        for i, u in enumerate(urls):
            if u and not any(e.get("url") == u for e in data):
                data.append({
                    "url": u,
                    "filename": names[i] if i < len(names) else u.split("/")[-1],
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "collection_count": len(data) + 1 # 실제 선별 통과 순서(카운트)를 명시합니다.
                })
                added_count += 1
                
        # 1건이라도 추가된 데이터가 있다면 JSON 파일로 디스크에 저장합니다.
        if added_count > 0:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"[Collection] 🎯 선별 통과 URL 파일 기록 완료 | job_id={job_id} +{added_count}건 (총 {len(data)}건) -> {filepath}", flush=True)
    except Exception as e:
        print(f"[Collection] ❌ 선별 통과 JSON 기록 실패: {e}", flush=True)


# ✅ 선별 단계 중복 체크 정책:
# - 중복 체크는 "선별(Collection) 단계에서만"수행한다.
# - 기준은 MariaDB의 ASADAL_{token}_LEARN_LIST 테이블의 `url` 컬럼 값이다.
# - (호환) 테이블에 `url`이 없으면 `content`로 fallback 한다.
_learn_list_url_col_cache: Dict[Tuple[str, str], str] = {}

async def _get_learn_list_url_column(db_name: str, table_name: str) -> str:
    """LEARN_LIST 테이블에서 URL을 저장하는 컬럼명을 결정한다."""
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
        if "url"in cols:
            _learn_list_url_col_cache[key] = "url"
            return "url"
        if "content"in cols:
            _learn_list_url_col_cache[key] = "content"
            return "content"
    except Exception:
        pass
    _learn_list_url_col_cache[key] = "url"
    return "url"

try:
    COLLECTION_HEAD_TIMEOUT_SEC = float(os.getenv("COLLECTION_HEAD_TIMEOUT_SEC", "5") or "5")
except Exception:
    COLLECTION_HEAD_TIMEOUT_SEC = 5.0
COLLECTION_HEAD_TIMEOUT_SEC = max(1.0, min(COLLECTION_HEAD_TIMEOUT_SEC, 30.0))

def _env_bool(key: str, default: str = "0") -> bool:
    try:
        return str(os.getenv(key, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"

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
    file_hints = (
        "filedown", "download", "file.do", "filedownload", "cmm/fms/filedown",
        "atchfile", "atchfileid", "filesn", "fileid", "fileSeq", "file_no",
    )
    return any(h in lu for h in file_hints)

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
    if not cd:
        return ""
    try:
        s = str(cd)
    except Exception:
        return ""
    try:
        import re as _re
        m = _re.search(r'filename\*?=(?:UTF-8\'\')?("?)([^";\r\n]+)\1', s, flags=_re.IGNORECASE)
        if not m:
            return ""
        fn = (m.group(2) or "").strip()
        try:
            from urllib.parse import unquote
            fn = unquote(fn)
        except Exception:
            pass
        fn = fn.strip().strip('"').strip("'")
        if "."not in fn:
            return ""
        ext = "."+ fn.rsplit(".", 1)[-1].lower()
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
    """한 개의 아이템(URL)이 수집 및 다운로드 대상인지 검증합니다."""
    url = item.get('url')
    if not url:
        return None, "missing_url"

    try:
        lowered_url = url.lower()
        
        # 1. 제외 패턴 필터링
        if any(p in lowered_url for p in EXCLUDE_URL_PATTERNS):
            logger.debug(f"[Collection] 🛑 선별 탈락: 제외 패턴 포함 | {url}")
            return None, "excluded_pattern"
            
        # 2. 게시판 패턴 및 파일 성격 확인
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
        if not reg_date_str and item_name:
            try:
                from backend.shared.date_utils import extract_date_from_text
                found_date = extract_date_from_text(item_name)
                if found_date:
                    reg_date_str = found_date.strftime('%Y-%m-%d')
            except Exception:
                pass

        # 4. 수집 중복 체크 (DB)
        if chat_bot_id and db_name:
            try:
                from db.mariadb_save_update import get_account_identifier_from_chatbot_setup, get_learn_list_table_name
                account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
                table = get_learn_list_table_name(account_identifier)
                url_col = await _get_learn_list_url_column(db_name, table)

                canon_url = canonicalize_url_for_dedup(url) or None
                url_candidates = []
                try:
                    if isinstance(url, str) and url: url_candidates.append(url)
                except Exception: pass
                try:
                    if isinstance(canon_url, str) and canon_url and canon_url not in url_candidates:
                        url_candidates.append(canon_url)
                except Exception: pass

                try:
                    from urllib.parse import urlparse, urlunparse
                    for candidate in list(url_candidates):
                        try:
                            p = urlparse(candidate)
                            host = p.netloc or ""
                            if host:
                                if host.startswith("www."): alt = host[len("www."):]
                                else: alt = "www."+ host
                                alt_url = urlunparse(p._replace(netloc=alt))
                                if alt_url not in url_candidates: url_candidates.append(alt_url)
                        except Exception: pass
                except Exception: pass

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
            if not (COLLECTION_FORCE_ENABLE and likely_file): return None
            item['reg_date'] = reg_date_str
            item['forced'] = True
            item['force_reason'] = reason
            logger.warning("[Collection] ⚠️ FORCE accept | reason=%s url=%s", reason, url)
            return item

        async def _probe_get() -> Optional[Dict]:
            if not (COLLECTION_FORCE_GET_PROBE and likely_file): return None
            try:
                req_headers = dict(headers)
                req_headers["Range"] = "bytes=0-2047"
                async with session.get(url, headers=req_headers, timeout=COLLECTION_FORCE_GET_TIMEOUT_SEC, allow_redirects=True) as r:
                    if r.status not in (200, 206): return None
                    ct = (r.headers.get("content-type") or "").lower()
                    if "text/html"in ct: return None
                    url_path = url.lower().split('?')[0]
                    is_allowed_ext = any(url_path.endswith(ext) for ext in ALLOWED_EXTENSIONS)
                    is_allowed_mime = any(mime in ct for mime in ALLOWED_MIME_TYPES)
                    if not is_allowed_ext and not is_allowed_mime: return None
                    item['reg_date'] = reg_date_str
                    return item
            except Exception:
                return None

        try:
            async with session.head(url, headers=headers, timeout=COLLECTION_HEAD_TIMEOUT_SEC, allow_redirects=True) as response:
                if response.status != 200:
                    if response.status in (405, 401, 403):
                        probed = await _probe_get()
                        if probed: return probed, "probe_get_ok"
                        if response.status == 405 and COLLECTION_FORCE_ON_405:
                            forced = await _accept_forced("head_405")
                            if forced: return forced, "forced:head_405"
                        if response.status in (401, 403) and COLLECTION_FORCE_ON_403_401:
                            forced = await _accept_forced(f"head_{response.status}")
                            if forced: return forced, f"forced:head_{response.status}"
                    logger.debug(f"[Collection] 🛑 선별 탈락: HTTP 상태 {response.status} | {url}")
                    return None, f"head_status_{response.status}"

                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' in content_type:
                    probed = await _probe_get()
                    if probed: return probed, "probe_get_ok"
                    if COLLECTION_FORCE_ON_HTML:
                        forced = await _accept_forced("head_html")
                        if forced: return forced, "forced:head_html"
                    logger.debug(f"[Collection] 🛑 선별 탈락: HTML 페이지임 | {url}")
                    return None, "content_type_html"

                url_path = url.lower().split('?')[0]
                is_allowed_ext = any(url_path.endswith(ext) for ext in ALLOWED_EXTENSIONS)
                is_allowed_mime = any(mime in content_type for mime in ALLOWED_MIME_TYPES)

                if not is_allowed_ext and not is_allowed_mime:
                    try: cd = response.headers.get("content-disposition", "") or ""
                    except Exception: cd = ""
                    ext_from_cd = _extract_filename_ext_from_content_disposition(cd)
                    if likely_file:
                        if ext_from_cd and any(ext_from_cd == ext for ext in ALLOWED_EXTENSIONS):
                            item["head_content_disposition"] = cd
                            item["head_filename_ext"] = ext_from_cd
                            item["head_content_type"] = content_type
                            return item, "ok_cd_ext"
                        if "application/octet-stream"in content_type or "binary/octet-stream"in content_type:
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
                if forced: return forced, "forced:head_timeout"
            return None, "head_timeout"

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        if COLLECTION_FORCE_ON_TIMEOUT and isinstance(e, asyncio.TimeoutError):
            try:
                if COLLECTION_FORCE_ENABLE and _is_likely_file_url(url):
                    item['reg_date'] = item.get('reg_date')
                    item['forced'] = True
                    item['force_reason'] = "head_timeout_exc"
                    logger.warning("[Collection] ⚠️ FORCE accept (timeout exc) | url=%s", url)
                    return item, "forced:head_timeout_exc"
            except Exception: pass
        logger.debug(f"[Validation] 네트워크 오류로 건너뜀: {url} ({type(e).__name__})")
        return None, f"network_error_{type(e).__name__}"
    except Exception as e:
        logger.error(f"[Validation] 알 수 없는 오류: {url}, {e}", exc_info=True)
        return None, f"unexpected_{type(e).__name__}"

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
    - Scan 큐에서 넘어온 배치를 가져와 유효성 검사 수행
    - 검사를 통과한 항목만 다운로드 큐로 전달 및 JSON 강제 기록
    """
    current_task = asyncio.current_task()
    worker_label = current_task.get_name() if current_task and current_task.get_name() else f"id={id(current_task)}"
    logger.debug("[Collection][%s] Worker started, waiting for items...", worker_label)
    wait_logged = False
    wait_log_interval = 30.0
    last_wait_log = 0.0
    _last_batch_print_at_by_job: Dict[str, float] = {}
    _batch_print_interval_sec = 10.0
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    _dedup_by_job: Dict[str, CollectionDeduplicator] = {}
    _db_dup_checked_by_job: Dict[str, Set[str]] = {}
    _db_dup_urls_by_job: Dict[str, Set[str]] = {}

    async def _dup_result():
        return (None, "duplicate_db")

    _last_pause_log: Dict[str, float] = {}
    async with aiohttp.ClientSession(headers=headers) as session:
        heartbeat_interval = 30 
        last_heartbeat = time.monotonic()
        
        while True:
            batch_items = None
            try:
                now = time.monotonic()
                try:
                    batch_items: List[Dict] = await asyncio.wait_for(in_queue.get(), timeout=3.0)
                    wait_logged = False
                    last_heartbeat = now
                except asyncio.TimeoutError:
                    if now - last_heartbeat >= heartbeat_interval:
                        queue_size = 0
                        try:
                            if hasattr(in_queue, "qsize"):
                                queue_size = int(in_queue.qsize() or 0)
                            else:
                                q_obj = getattr(in_queue, "queue", None)
                                if q_obj is not None and hasattr(q_obj, "qsize"):
                                    queue_size = int(q_obj.qsize() or 0)
                        except Exception:
                            pass
                        logger.debug(f"[Collection] Worker alive, waiting for item... (queue={queue_size})")
                        last_heartbeat = now
                    
                    if not wait_logged or (now - last_wait_log) >= wait_log_interval:
                        wait_logged = True
                        last_wait_log = now
                    continue
                
                if not batch_items:
                    continue

                try:
                    job_ids = {str((it or {}).get("job_id") or "default") for it in (batch_items or [])}
                except Exception:
                    job_ids = {"default"}
                batch_job_id = next(iter(job_ids)) if len(job_ids) == 1 else "multi"

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
                    await asyncio.sleep(0.2)
                    continue

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
                    
                    _db_dup_checked_by_job.setdefault(jid, set())
                    _db_dup_urls_by_job.setdefault(jid, set())

                    try:
                        raw_url = (item or {}).get("url") or ""
                        url_key = canonicalize_url_for_dedup(str(raw_url)) or str(raw_url)
                    except Exception:
                        url_key = (item or {}).get("url") or ""

                    if url_key and url_key in _db_dup_urls_by_job.get(jid, set()):
                        validation_tasks.append(_dup_result())
                    else:
                        validation_tasks.append(
                            _validate_item(item, session, eff_dedup, eff_chat_bot_id, eff_db_name, eff_start_date, eff_end_date)
                        )
                results = await asyncio.gather(*validation_tasks, return_exceptions=True)
                
                valid_items = []
                skipped_total = 0
                
                for idx, result in enumerate(results):
                    item = batch_items[idx] if batch_items and idx < len(batch_items) else {}
                    if isinstance(result, Exception):
                        skipped_total += 1
                        continue
                    try:
                        validated, rsn = result  
                    except Exception:
                        skipped_total += 1
                        continue

                    if isinstance(validated, dict):
                        valid_items.append(validated)
                        
                        if on_valid_batch:
                            try:
                                await on_valid_batch([validated])
                            except Exception as callback_err:
                                logger.warning(f"[Collection] on_valid_batch callback failed: {callback_err}")
                        
                        if forward_to_queue and out_queue:
                            await out_queue.put(validated)
                        
                        try:
                            eff_job_id = str(validated.get("job_id") or "default")
                        except Exception:
                            eff_job_id = "default"
                            
                        await progress_queue.put({
                            'type': 'collection',
                            'count': 1,
                            'items': [validated.get('url')],
                            'job_id': eff_job_id,
                        })
                    else:
                        skipped_total += 1
                        rsn = str(rsn or "rejected_unknown")
                        if rsn in ("duplicate_db", "duplicate_db_cached"):
                            try:
                                u = str((item or {}).get("url") or "")
                            except Exception:
                                u = ""
                            if u:
                                try:
                                    await progress_queue.put({'type': 'scan', 'count': 1, 'items': [u], 'job_id': str((item or {}).get('job_id') or 'default')})
                                except Exception:
                                    pass
                            try:
                                jid = str((item or {}).get("job_id") or "default")
                                url_key = canonicalize_url_for_dedup(u) or u
                                if url_key:
                                    _db_dup_urls_by_job.setdefault(jid, set()).add(url_key)
                            except Exception:
                                pass

                # =================================================================
                # [추가] 선별 검증을 통과한 아이템이 존재할 경우, 곧바로 JSON 기록 수행
                # =================================================================
                if valid_items:
                    urls_to_log = []
                    names_to_log = []
                    print(f"collection_log makes json valid_items={valid_items}")
                    for vi in valid_items:
                        u = vi.get("url")
                        print(f"collection_log makes json u={u}")
                        if u:
                            urls_to_log.append(u)
                            names_to_log.append(vi.get("name") or "unknown")
                            print(f"urls_to_log={urls_to_log} names_to_log={names_to_log}")

                    if urls_to_log:
                        print(f"collection_log makes json")
                        # 독립 함수를 호출하여 collection_passed JSON 파일 생성
                        force_save_collection_log_json(batch_job_id, urls_to_log, names_to_log)

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