import asyncio
import logging
import aiohttp
import time
import os
import sys
import json
import re
from datetime import datetime 
from email.utils import parsedate_to_datetime
from typing import List, Dict, Callable, Optional, Awaitable, Tuple, Set
from urllib.parse import urlparse, unquote
from core.crawler.batch_queue import BatchQueue
from db.repository import DBRepository
from config.constants import COLLECTION_EXTENSIONS, ALLOWED_EXTENSIONS, IMG_EXTENSIONS, BOARD_PATTERNS, EXCLUDE_URL_PATTERNS
from core.crawler.dedup import (
    CollectionDeduplicator,
    try_acquire_cross_job_claim,
    release_cross_job_claim,
)
from db.mysql_db_config import mysql_execute_query
from utils.url import canonicalize_url_for_dedup

# 프로젝트 루트 경로를 설정하여 내부 모듈을 참조할 수 있게 합니다.
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from backend.shared.date_utils import is_date_in_range

logger = logging.getLogger(__name__)

# 단계별 URL 리포트(다운로드 폴더 JSON 누적)
try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        try:
            import sys as _sys
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            if project_root not in _sys.path:
                _sys.path.insert(0, project_root)
            from backend.shared.stage_url_report import append_stage_urls as _impl  # type: ignore
            return _impl(stage=stage, urls=urls, job_id=job_id, db_name=db_name, output_dir=output_dir, extra_meta=extra_meta, entry_extra=entry_extra)
        except Exception:
            return None

# 환경 변수에 따라 디버그 모드 로그 레벨을 조정합니다.
FLOW_DEBUG = os.getenv("CRAWL_DEBUG_FLOW", "0") == "1"
if FLOW_DEBUG:
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass


def _flow_debug_print(*args, **kwargs) -> None:
    if not FLOW_DEBUG:
        return
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)

# [함수] 선별 단계를 통과한 URL 정보를 별도의 JSON 파일로 강제 기록합니다.
def force_save_collection_log_json(job_id: str, urls: list, names: list):
    _flow_debug_print(f" ================================ force_save_collection_log_json")
    if not job_id or not urls:
        _flow_debug_print(f"  early exit: missing job_id or urls | job_id={job_id} urls_len={len(urls) if urls is not None else 0}")
        return
    try:
        # 파일이 저장될 워커 전용 폴더 경로를 생성합니다.
        _flow_debug_print(f"  setting up target directory, project_root={project_root}")
        target_dir = os.path.join(project_root, "core", "crawler", "workers")
        os.makedirs(target_dir, exist_ok=True)
        _flow_debug_print(f"  ensured target_dir={target_dir}")
        filepath = os.path.join(target_dir, f"collection_passed_{job_id}.json")
        _flow_debug_print(f"  target filepath={filepath}")
        
        data = []
        # 기존 파일이 있다면 데이터를 불러와서 이어쓰기를 준비합니다.
        if os.path.exists(filepath):
            _flow_debug_print(f"  existing file detected, loading JSON: {filepath}")
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            _flow_debug_print(f"  loaded existing entries count={len(data)}")
                
        added_count = 0
        # 중복되지 않는 새로운 URL 정보만 목록에 추가합니다.
        for i, u in enumerate(urls):
            _flow_debug_print(f"  checking url index={i} url={u}")
            if u and not any(e.get("url") == u for e in data):
                filename = names[i] if i < len(names) else u.split("/")[-1]
                _flow_debug_print(f"  adding url index={i} url={u} filename={filename}")
                data.append({
                    "url": u,
                    "filename": filename,
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "collection_count": len(data) + 1 
                })
                added_count += 1
            else:
                _flow_debug_print(f"  skipped (duplicate or empty) url index={i} url={u}")
                
        # 1건이라도 추가된 데이터가 있다면 JSON 파일로 디스크에 저장합니다.
        if added_count > 0:
            _flow_debug_print(f"  writing {added_count} new entries to {filepath}")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            _flow_debug_print(f"[Collection] 🎯 선별 통과 URL 파일 기록 완료 | job_id={job_id} +{added_count}건 (총 {len(data)}건) -> {filepath}")
            _flow_debug_print(f"  write complete total_entries={len(data)}")
    except Exception as e:
        _flow_debug_print(f"[Collection] ❌ 선별 통과 JSON 기록 실패: {e}")
        _flow_debug_print(f"  exception in force_save_collection_log_json: {e}")

# [함수] DB 테이블 내에서 URL 정보를 저장하는 컬럼명을 동적으로 파악합니다.
# 버전을 올리면 기존 캐시를 무시(컬럼 우선순위 변경 시 배포 후 즉시 반영).
_LEARN_LIST_URL_COL_CACHE_VER = 3
_learn_list_url_col_cache: Dict[Tuple[str, str, int], str] = {}
async def _get_learn_list_url_column(db_name: str, table_name: str) -> str:
    if not db_name or not table_name: return ""
    key = (db_name, table_name, _LEARN_LIST_URL_COL_CACHE_VER)
    if key in _learn_list_url_col_cache: return _learn_list_url_col_cache[key]
    try:
        sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s"
        rows = await mysql_execute_query(sql, (db_name, table_name), fetch=True, dbname=db_name)
        cols = {str(r["column_name"]).lower() for r in rows if isinstance(r, dict) and r.get("column_name")}
        _learn_list_url_col_cache[key] = "content" if "content" in cols else ""
        return _learn_list_url_col_cache[key]
    except Exception:
        return ""

# 환경 변수로부터 네트워크 타임아웃 및 강제 수집 설정을 로드합니다.
try:
    COLLECTION_HEAD_TIMEOUT_SEC = max(1.0, min(float(os.getenv("COLLECTION_HEAD_TIMEOUT_SEC", "5") or "5"), 30.0))
except Exception:
    COLLECTION_HEAD_TIMEOUT_SEC = 5.0
try:
    COLLECTION_HANDLER_HEAD_TIMEOUT_SEC = max(0.2, min(float(os.getenv("COLLECTION_HANDLER_HEAD_TIMEOUT_SEC", "1.0") or "1.0"), 10.0))
except Exception:
    COLLECTION_HANDLER_HEAD_TIMEOUT_SEC = 1.0
try:
    COLLECTION_HANDLER_GET_TIMEOUT_SEC = max(0.2, min(float(os.getenv("COLLECTION_HANDLER_GET_TIMEOUT_SEC", "1.5") or "1.5"), 10.0))
except Exception:
    COLLECTION_HANDLER_GET_TIMEOUT_SEC = 1.5

def _env_bool(key: str, default: str = "0") -> bool:
    return str(os.getenv(key, default)).strip().lower() in ("1", "true", "yes", "on")

COLLECTION_TRUST_DOWNLOAD_HANDLER = _env_bool("COLLECTION_TRUST_DOWNLOAD_HANDLER", "0")
COLLECTION_FORCE_ENABLE = _env_bool("COLLECTION_FORCE_ENABLE", "1")
COLLECTION_FORCE_ON_TIMEOUT = _env_bool("COLLECTION_FORCE_ON_TIMEOUT", "1")
COLLECTION_FORCE_ON_403_401 = _env_bool("COLLECTION_FORCE_ON_403_401", "1")
COLLECTION_FORCE_ON_405 = _env_bool("COLLECTION_FORCE_ON_405", "1")
COLLECTION_FORCE_ON_HTML = _env_bool("COLLECTION_FORCE_ON_HTML", "1")
COLLECTION_FORCE_GET_PROBE = _env_bool("COLLECTION_FORCE_GET_PROBE", "1")
COLLECTION_FORCE_GET_TIMEOUT_SEC = 5.0

# [함수] URL의 확장자나 힌트를 분석하여 파일 다운로드 링크인지 추측합니다.
def _is_likely_file_url(url: str) -> bool:
    if not url: return False
    lu = url.lower()
    if any(lu.split("?")[0].endswith(ext) for ext in ALLOWED_EXTENSIONS): return True
    file_hints = ("filedown", "download", "file.do", "filedownload", "atchfile", "file_no")
    return any(h in lu for h in file_hints)


def _is_trusted_download_handler_url(url: str) -> bool:
    if not url:
        return False
    lu = str(url).strip().lower()
    path = lu.split("?", 1)[0]
    trusted_hints = (
        "filedown",
        "filedownnew.jsp",
        "filedownload",
        "downloadbbsfile",
        "downloadbbs",
        "downloadfile",
        "download.do",
        "download.jsp",
        "atchfile",
        "atchfileid",
        "atchmnflno",
        "atchmnfl",
        "file_no=",
        "fileno=",
        "filesn=",
        "file_sn=",
        "sys_file_nm=",
    )
    if any(h in lu for h in trusted_hints):
        return True
    return any(path.endswith(ext) for ext in ALLOWED_EXTENSIONS)


def _matched_exclude_pattern(url: str, *, likely_file: bool) -> Optional[str]:
    lowered_url = (url or "").lower()
    for pattern in EXCLUDE_URL_PATTERNS:
        p = str(pattern or "").lower()
        if not p or p not in lowered_url:
            continue
        if likely_file and p == "session" and "jsessionid" in lowered_url:
            continue
        return p
    return None


# 허용되는 문서 및 이미지 파일의 MIME 타입을 정의합니다.
ALLOWED_MIME_TYPES = [
    "application/pdf", "application/msword", 
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "image/jpeg", "image/png", "image/gif"
]

# [함수] 응답 헤더의 Content-Disposition에서 실제 파일 확장자를 추출합니다.
def _extract_filename_ext_from_content_disposition(cd: str) -> str:
    if not cd: return ""
    try:
        import re as _re
        m = _re.search(r'filename\*?=(?:UTF-8\'\')?("?)([^";\r\n]+)\1', str(cd), flags=_re.IGNORECASE)
        if not m: return ""
        from urllib.parse import unquote
        fn = unquote((m.group(2) or "").strip().strip('"').strip("'"))
        return "." + fn.rsplit(".", 1)[-1].lower() if "." in fn else ""
    except Exception:
        return ""

def _extract_filename_only(value: str) -> str:
    """URL/경로/파일명 문자열에서 '파일명'만 추출한다."""
    if not value:
        return ""
    s = str(value).strip()
    # URL이면 path 기준으로 basename 추출
    try:
        p = urlparse(s)
        if p.scheme and p.netloc:
            s = p.path or ""
    except Exception:
        pass

    s = s.replace("\\", "/")
    s = s.split("?", 1)[0].split("#", 1)[0]
    s = s.rsplit("/", 1)[-1]
    try:
        s = unquote(s)
    except Exception:
        pass
    return s.strip().strip('"').strip("'")

def _normalize_filename_for_compare(filename: str) -> str:
    if not filename:
        return ""
    # 대소문자/공백 차이 정도만 흡수 (과도한 정규화는 오탐 가능)
    return " ".join(str(filename).strip().lower().split())

# [함수] 단일 아이템(URL)이 수집 및 다운로드 대상인지 최종 검증합니다.
async def _validate_item(
    item: Dict, session: aiohttp.ClientSession, deduplicator: Optional[CollectionDeduplicator],
    chat_bot_id: Optional[str], db_name: Optional[str], start_date: Optional[datetime], end_date: Optional[datetime]
) -> Tuple[Optional[Dict], str]:
    url = item.get('url')
    # JS 클릭 다운로드 형태는 download 워커가 직접 처리할 수 없으므로, 가능한 경우 실제 URL로 정규화한다.
    try:
        if isinstance(url, str) and url.lower().startswith("javascript:"):
            from utils.url import extract_download_url_from_js
            base = (item.get("source_page") or item.get("referer") or "") if isinstance(item, dict) else ""
            normalized = extract_download_url_from_js(url, base or None)
            if normalized:
                item["url"] = normalized
                url = normalized
            else:
                return None, "js_unresolved"
    except Exception:
        # best-effort: 정규화 실패 시 기존 로직으로 진행(단, url이 javascript면 결국 네트워크 단계에서 실패할 확률이 큼)
        pass
    _flow_debug_print(f"===============005=============== _validate_item")
    if not url: return None, "missing_url"
    _flow_debug_print(f"===============006=============== _validate_item")
    try:
        try:
            _flow_debug_print(f"[test010][validate_item] entry url={url} name={item.get('name')} type={item.get('type')}")
        except Exception:
            pass
    
        # 2. 게시판 패턴 및 파일 성격 기본 확인
        lowered_url = url.lower()
        is_direct_file = any(lowered_url.split('?')[0].endswith(ext) for ext in ALLOWED_EXTENSIONS)
        likely_file = _is_likely_file_url(url) or (str(item.get("type") or "").lower() == "file")
        trusted_download_handler = likely_file and _is_trusted_download_handler_url(url)
        try:
            _flow_debug_print(f"[test010][validate_item] file_check is_direct_file={is_direct_file} likely_file={likely_file}")
        except Exception:
            pass
        # 1. 스팸 혹은 제외 패턴 필터링
        # download.do;jsessionid=... 형태의 실제 첨부 URL은 session 제외어 예외로 둔다.
        exclude_pattern = _matched_exclude_pattern(url, likely_file=likely_file)
        if exclude_pattern:
            try:
                _flow_debug_print(f"[test010][validate_item] excluded_pattern match={exclude_pattern} | url={url}")
            except Exception:
                pass
            return None, "excluded_pattern"

        if not is_direct_file and not any(p in lowered_url for p in BOARD_PATTERNS) and not likely_file:
            try:
                _flow_debug_print(f"[test010][validate_item] not_board_or_file | url={url}")
            except Exception:
                pass
            return None, "not_board_or_file"

        # 3. 날짜 정보 보정 (기록용으로만 사용하며, 기간 필터링은 하지 않음)
        reg_date_str = item.get('reg_date')
        if not reg_date_str and item.get('name'):
            try:
                from backend.shared.date_utils import extract_date_from_text
                found_date = extract_date_from_text(item['name'])
                if found_date: reg_date_str = found_date.strftime('%Y-%m-%d')
            except Exception:
                try:
                    _flow_debug_print(f"[test010][validate_item] date_extract_failed name={item.get('name')}")
                except Exception:
                    pass

        try:
            _flow_debug_print(f"[test010][validate_item] reg_date={reg_date_str}")
        except Exception:
            pass

        # 4. DB 수집 중복 체크 (이미 저장된 파일인지 확인)
        # - 요청사항: learn_list의 content 컬럼에 저장된 값에서 "파일명"만 추출하여 비교한다.
        #   (content에는 보통 uploaded_files URL이 저장되며, 끝 basename이 파일명으로 사용됨)
        # 4. DB 수집 중복 체크 (한 줄 주석: 고유 URL과 파일명을 이중으로 대조하여 데이터 적체를 방지)
        # 4. DB 수집 중복 체크 (판단 로직 및 로그 강화)
        if chat_bot_id and db_name:
            try:
                from db.mariadb_save_update import get_account_identifier_from_chatbot_setup, get_learn_list_table_name
                # 챗봇 ID를 사용하여 실제 데이터가 담긴 테이블 이름을 동적으로 가져옵니다.
                account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
                table = get_learn_list_table_name(account_identifier)

                # 중복 비교를 위해 현재 URL과 정규화된 파일명을 준비합니다.
                target_url = url
                url_col = await _get_learn_list_url_column(db_name, table)
                candidate_name = _extract_filename_only(item.get("name") or item.get("filename") or target_url)
                candidate_key = _normalize_filename_for_compare(candidate_name)

                if target_url and url_col:
                    if url_col == "content":
                        try:
                            from utils.attachment_url_normalize import (
                                canonicalize_attachment_url_for_learn_list,
                            )
                            from utils.url import canonicalize_url_for_dedup

                            canon = (
                                canonicalize_attachment_url_for_learn_list(target_url)
                                or canonicalize_url_for_dedup(target_url)
                                or target_url
                            )
                        except Exception:
                            canon = target_url
                        sql = (
                            f"SELECT `{url_col}` as content FROM `{table}` "
                            f"WHERE `{url_col}` = %s LIMIT 5"
                        )
                        rows = await mysql_execute_query(
                            sql, (canon,), fetch=True, dbname=db_name
                        )
                        if rows:
                            logger.info(
                                "[중복선별] 결과: LEARN_LIST content 중복 (Skip) | key=%s",
                                (canon[:200] + "…") if len(canon) > 200 else canon,
                            )
                            return None, "duplicate_db_content"
                        try:
                            from utils.attachment_url_normalize import (
                                extract_sys_file_nm_from_attachment_url,
                                sql_like_contains_pattern,
                            )

                            _sys_nm = extract_sys_file_nm_from_attachment_url(target_url)
                            if _sys_nm:
                                _pat = sql_like_contains_pattern(_sys_nm)
                                sql_sf = (
                                    f"SELECT `{url_col}` as content FROM `{table}` "
                                    f"WHERE `{url_col}` LIKE %s ESCAPE '!' LIMIT 5"
                                )
                                rows_sf = await mysql_execute_query(
                                    sql_sf, (_pat,), fetch=True, dbname=db_name
                                )
                                if rows_sf:
                                    logger.info(
                                        "[중복선별] 결과: LEARN_LIST %s sys_file_nm LIKE 중복 (Skip) | nm=%s",
                                        url_col,
                                        (_sys_nm[:120] + "…") if len(_sys_nm) > 120 else _sys_nm,
                                    )
                                    return None, "duplicate_db_content_sys_file_nm"
                        except Exception:
                            pass
                    else:
                        # DB에서 동일한 URL이나 유사한 파일명이 있는지 최대 200건까지 조회합니다.
                        sql = f"SELECT `{url_col}` as content FROM `{table}` WHERE `{url_col}` = %s OR `{url_col}` LIKE %s LIMIT 200"
                        rows = await mysql_execute_query(sql, (target_url, f"%{candidate_name}"), fetch=True, dbname=db_name)

                        if rows:
                            for row in rows:
                                db_content = row.get("content") if isinstance(row, dict) else (row[0] if row else None)
                                if not db_content:
                                    continue

                                # 1순위 판단: URL 주소가 완벽히 일치하는 경우 중복으로 처리합니다.
                                if db_content == target_url:
                                    logger.info(f"[중복선별] 결과: URL 중복 발견 (Skip) | url={target_url}")
                                    return None, "duplicate_db_url"

                                # 2순위 판단: 주소는 달라도 파일명이 같으면 중복 데이터로 간주합니다.
                                db_name_only = _extract_filename_only(db_content)
                                if candidate_key and _normalize_filename_for_compare(db_name_only) == candidate_key:
                                    logger.info(f"[중복선별] 결과: 파일명 중복 발견 (Skip) | name={candidate_name}")
                                    return None, "duplicate_db_filename"

                    # 최종 판단: 중복 리스트를 모두 확인했으나 일치하는 항목이 없으면 신규로 판단합니다.
                    logger.info(f"[중복선별] 결과: 신규 데이터 확인 (Pass) | url={target_url}")
                                        
            except Exception as e:
                # 중복 체크 중 예상치 못한 에러가 발생하면 로그를 남깁니다.
                logger.error(f"[중복선별] 판단 중 오류 발생 | url={target_url} | error={e}")        

        # 5. HEAD 요청을 통한 실시간 파일 유효성 검증
        headers = {'Referer': item.get('source_page', '')}
        
        async def _accept_forced(reason: str):
            if not (COLLECTION_FORCE_ENABLE and likely_file): return None
            item.update({'reg_date': reg_date_str, 'forced': True, 'force_reason': reason})
            return item

        if trusted_download_handler and COLLECTION_TRUST_DOWNLOAD_HANDLER:
            forced = await _accept_forced("trusted_download_handler")
            if forced:
                logger.debug("[Collection] trusted download handler accepted without HEAD/GET | url=%s", url)
                return forced, "forced:trusted_download_handler"

        async def _get_probe_validate() -> Tuple[Optional[Dict], str]:
            if not (COLLECTION_FORCE_ENABLE and COLLECTION_FORCE_GET_PROBE and likely_file):
                return None, "get_probe_disabled"
            try:
                probe_headers = dict(headers)
                probe_headers.setdefault("Range", "bytes=0-0")
                probe_timeout = COLLECTION_HANDLER_GET_TIMEOUT_SEC if trusted_download_handler else COLLECTION_FORCE_GET_TIMEOUT_SEC
                async with session.get(url, headers=probe_headers, timeout=probe_timeout, allow_redirects=True) as r:
                    status = r.status
                    if status < 200 or status >= 300:
                        return None, f"get_probe_status_{status}"

                    content_type = (r.headers.get("content-type", "") or "").lower()
                    if "text/html" in content_type and not COLLECTION_FORCE_ON_HTML:
                        return None, "get_probe_content_type_html"

                    if r.headers.get("content-length") == "0":
                        return None, "get_probe_content_length_0"

                    item["reg_date"] = reg_date_str
                    return item, "ok:get_probe"
            except asyncio.TimeoutError:
                return None, "get_probe_timeout"
            except Exception as e:
                return None, f"get_probe_unexpected_{type(e).__name__}"

        try:
            head_timeout = COLLECTION_HANDLER_HEAD_TIMEOUT_SEC if trusted_download_handler else COLLECTION_HEAD_TIMEOUT_SEC
            async with session.head(url, headers=headers, timeout=head_timeout, allow_redirects=True) as response:
                try:
                    _flow_debug_print(f"[test010][validate_item] HEAD request sent | url={url} timeout={head_timeout}")
                except Exception:
                    pass
                status = response.status
                if status < 200 or status >= 300:
                    probed_item, probed_reason = await _get_probe_validate()
                    if probed_item:
                        try:
                            _flow_debug_print(f"[test010][validate_item] GET probe accepted | url={url} reason={probed_reason}")
                        except Exception:
                            pass
                        return probed_item, probed_reason

                    forced_reason = None
                    if COLLECTION_FORCE_ENABLE and likely_file:
                        if status in (401, 403) and COLLECTION_FORCE_ON_403_401:
                            forced_reason = f"head_{status}"
                        elif status == 405 and COLLECTION_FORCE_ON_405:
                            forced_reason = f"head_{status}"

                    # HEAD 401/403/405라도 실제 GET probe가 404면 죽은 첨부이므로
                    # 선별 단계에서 제외해 다운로드 경고까지 내려가지 않게 한다.
                    if forced_reason and probed_reason not in ("get_probe_status_404",):
                        forced = await _accept_forced(forced_reason)
                        try:
                            _flow_debug_print(f"[test010][validate_item] HEAD forced accepted reason={forced_reason} | url={url}")
                        except Exception:
                            pass
                        if forced:
                            return forced, f"forced:{forced_reason}"

                    if probed_reason not in ("get_probe_disabled",):
                        return None, probed_reason

                    return None, f"head_status_{status}"

                content_type = response.headers.get('content-type', '').lower()
                try:
                    _flow_debug_print(f"[test010][validate_item] HEAD response status={response.status} content_type={content_type}")
                except Exception:
                    pass
                if 'text/html' in content_type and not COLLECTION_FORCE_ON_HTML: return None, "content_type_html"
                
                # 용량이 0인 파일은 건너뜁니다.
                if response.headers.get('content-length') == '0': return None, "content_length_0"

                item['reg_date'] = reg_date_str
                try:
                    _flow_debug_print(f"[test010][validate_item] validation ok | url={url} reg_date={reg_date_str}")
                except Exception:
                    pass
                return item, "ok"
        except asyncio.TimeoutError:
            probed_item, probed_reason = await _get_probe_validate()
            if probed_item:
                try:
                    _flow_debug_print(f"[test010][validate_item] GET probe accepted after HEAD timeout | url={url} reason={probed_reason}")
                except Exception:
                    pass
                return probed_item, probed_reason
            if COLLECTION_FORCE_ON_TIMEOUT:
                forced = await _accept_forced("head_timeout")
                if forced: return forced, "forced:head_timeout"
            return None, "head_timeout"

    except Exception as e:
        if COLLECTION_FORCE_ENABLE and likely_file and isinstance(e, (aiohttp.ClientError, OSError)):
            forced = await _accept_forced(f"head_error_{type(e).__name__}")
            if forced:
                logger.info("[Validation] HEAD network error forced accepted | url=%s err=%s", url, type(e).__name__)
                return forced, f"forced:head_error_{type(e).__name__}"
        logger.error(f"[Validation] 오류: {url}, {e}")
        return None, f"unexpected_{type(e).__name__}"

# [함수] Scan 큐로부터 작업을 가져와 검증하고 JSON 로그를 남기는 메인 워커입니다.
def _should_keep_collection_claim_recent(validated: Optional[Dict], reason: Optional[str]) -> bool:
    if validated:
        return True
    low = str(reason or "").strip().lower()
    if not low:
        return False
    non_recent_prefixes = (
        "head_timeout",
        "get_probe_timeout",
        "unexpected_",
        "get_probe_unexpected",
    )
    return not any(low.startswith(prefix) for prefix in non_recent_prefixes)


async def _validate_item_with_cross_job_claim(
    item: Dict,
    session: aiohttp.ClientSession,
    deduplicator: Optional[CollectionDeduplicator],
    chat_bot_id: Optional[str],
    db_name: Optional[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
) -> Tuple[Optional[Dict], str]:
    claim_url = str((item or {}).get("url") or "").strip()
    claim_job_id = str((item or {}).get("job_id") or "default").strip() or "default"
    claim_db_name = str((item or {}).get("db_name") or db_name or "").strip()
    claim_acquired = False
    keep_recent = False

    if claim_url and claim_db_name:
        claim_acquired = await try_acquire_cross_job_claim(
            "collection",
            claim_db_name,
            claim_url,
            claim_job_id,
        )
        if not claim_acquired:
            return None, "duplicate_other_job_claim"

    try:
        validated, reason = await _validate_item(
            item, session, deduplicator, chat_bot_id, db_name, start_date, end_date
        )
        keep_recent = _should_keep_collection_claim_recent(validated, reason)
        return validated, reason
    except Exception:
        keep_recent = False
        raise
    finally:
        if claim_acquired:
            await release_cross_job_claim(
                "collection",
                claim_db_name,
                claim_url,
                claim_job_id,
                keep_recent=keep_recent,
            )


async def collection_worker(
    in_queue: BatchQueue, out_queue: Optional[BatchQueue], progress_queue: asyncio.Queue,
    db_repo: DBRepository, deduplicator: CollectionDeduplicator | None = None,
    on_valid_batch: Optional[Callable[[List[Dict]], Awaitable[None]]] = None,
    forward_to_queue: bool = True, chat_bot_id: Optional[str] = None,
    db_name: Optional[str] = None, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None,
):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    _db_dup_urls_by_job: Dict[str, Set[str]] = {}

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            batch_items = None
            try:
                # 큐에서 작업 배치를 가져옵니다 (3초 타임아웃).
                try:
                    batch_items = await asyncio.wait_for(in_queue.get(), timeout=3.0)
                except asyncio.TimeoutError:
                    continue
                
                if not batch_items: continue
                batch_job_id = str(batch_items[0].get("job_id", "default"))
                seen = _db_dup_urls_by_job.setdefault(batch_job_id, set())
                # 배치의 모든 아이템에 대해 유효성 검사를 비동기로 병렬 실행합니다.
                validation_tasks = [
                    _validate_item_with_cross_job_claim(
                        item, session, deduplicator, chat_bot_id, db_name, start_date, end_date
                    )
                    for item in batch_items
                ]
                results = await asyncio.gather(*validation_tasks, return_exceptions=True)
                
                valid_items = []
                for idx, result in enumerate(results):
                    if isinstance(result, tuple) and result[0]:
                        validated = result[0]
                        url = validated.get("url")

                        # 메모리 dedupe
                        if url in seen:
                            continue

                        seen.add(url)

                        valid_items.append(validated)

                        if forward_to_queue and out_queue:
                            await out_queue.put(validated)
                        
                        # UI 혹은 로그용으로 선별 통과 사실을 알립니다.
                        await progress_queue.put({
                            'type': 'collection', 'count': 1, 
                            'items': [validated.get('url')], 'job_id': batch_job_id
                        })
                    else:
                        # 탈락한 데이터에 대한 스캔 처리 카운트를 업데이트합니다.
                        item = batch_items[idx]
                        reason = None
                        if isinstance(result, tuple):
                            reason = result[1] if len(result) > 1 else None
                        elif isinstance(result, BaseException):
                            reason = f"exception_{type(result).__name__}"
                        await progress_queue.put({
                            'type': 'scan', 'count': 1, 
                            'items': [item.get('url')], 'job_id': batch_job_id,
                            'reason': reason,
                        })
                        try:
                            append_stage_urls(
                                stage="collection_reject",
                                urls=[{
                                    "url": item.get("url"),
                                    "name": item.get("name"),
                                    "reason": reason,
                                }],
                                job_id=batch_job_id,
                                db_name=db_name,
                            )
                        except Exception:
                            pass

                # 선별 검증을 최종 통과한 아이템이 있다면 즉시 JSON 파일에 기록합니다.
                if valid_items:
                    urls_to_log = [vi.get("url") for vi in valid_items if vi.get("url")]
                    names_to_log = [vi.get("name") or "unknown" for vi in valid_items]
                    if urls_to_log:
                        force_save_collection_log_json(batch_job_id, urls_to_log, names_to_log)
                        # ✅ stage/trace JSON 누적(다운로드 폴더)
                        try:
                            append_stage_urls(
                                stage="collection",
                                urls=[
                                    {"url": u, "name": (names_to_log[i] if i < len(names_to_log) else None)}
                                    for i, u in enumerate(urls_to_log or [])
                                    if u
                                ],
                                job_id=batch_job_id,
                                db_name=db_name,
                            )
                        except Exception:
                            pass

            except asyncio.CancelledError: break
            except Exception as e: logger.error(f"[Collection] 워커 오류: {e}")
            finally:
                if batch_items:
                    # 처리 완료된 배치의 상태를 큐에 알립니다.
                    progress_queue.put_nowait({'type': 'in_flight', 'stage': 'collection', 'delta': -len(batch_items)})
                    in_queue.task_done()

def get_clean_url(file_url):
    # javascript 형태인 경우 실제 경로만 추출하는 정규식
    js_pattern = r"javascript:downloadFile\(['\"](.*?)['\"]" 
    
    match = re.search(js_pattern, file_url)
    if match:
        return match.group(1) # 추출된 '/attach/cms/webzine/...' 반환
    return file_url # 일반 URL인 경우 그대로 반환

def check_db_duplicate(file_url, table_name, db=None):
    # DB 연결 객체가 없으면 에러를 발생시킵니다.
    if db is None: raise ValueError("db client is required")
    
    # URL에서 파라미터를 제거한 깨끗한 경로를 추출합니다.
    target_url = get_clean_url(file_url)
    
    # 해당 URL이 DB에 몇 개 존재하는지 카운트하는 쿼리를 작성합니다.
    query = f"SELECT count(*) as cnt FROM {table_name} WHERE content = '{target_url}'"
    
    # 쿼리를 실행하여 결과를 가져옵니다.
    result = db.execute(query)
    count = result[0]['cnt'] if result else 0
    
    # 데이터가 1개 이상이면 중복(True), 0개면 신규(False)로 판단하여 반환합니다.
    is_dup = count > 0
    result_msg = "중복 발견" if is_dup else "신규 확인"
    logger.info(f"[중복선별] 최종 판단: {result_msg} | url={target_url}")
    
    return is_dup
