import os
import logging
import requests
import random
import time
import re
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
from urllib.parse import urlparse, urljoin, parse_qs
from db.db_operations import insert_data, delete_data, execute_query
# Milvus helper import: prefer local shim (db.db_milvus_operations) but fall back to services.milvus_service
try:
    from db.db_milvus_operations import (
        MilvusSyncContext,
        activate_milvus_sync_context,
        reset_milvus_sync_context,
        sync_rows_to_milvus,
    )
except Exception:
    try:
        from services.milvus_service import (
            MilvusSyncContext,
            activate_milvus_sync_context,
            reset_milvus_sync_context,
            sync_rows_to_milvus,
        )
    except Exception:
        # Define no-op fallbacks to avoid import-time failures
        MilvusSyncContext = None  # type: ignore
        def activate_milvus_sync_context(ctx):  # type: ignore
            return None
        def reset_milvus_sync_context(token):  # type: ignore
            return None
        async def sync_rows_to_milvus(rows, context):  # type: ignore
            return None
from db.maria_db_config import DatabasePool as MARIADB_DatabasePool
from db.maria_operations import maria_execute_query
# from utils.get_mysql import insert_url_learn_list
from config import Config
from langchain.text_splitter import RecursiveCharacterTextSplitter
from utils.logging_util import LoggerSingleton
from edu.classes import TLSAdapter, CrawlStopSignal, SmartCrawlQueue
from db.db_job_managers import AsyncJobManager, AsyncJobProgress
from socket_sender import send_message_to_socket, send_message_to_redis_sse
from socket_manager import socket_manager
# extract_ 관련 함수들은 extract_html.py로 분리
from edu.extract_html import *
# Playwright 관련 함수들은 extract_playwright.py로 분리
from edu.extract_playwright import (
    cleanup_playwright_processes,
    get_or_create_browser,
    fetch_page_with_timeout,
    fetch_page_with_playwright,
    get_government_user_agent
)
from urllib3.util import ssl_
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import ssl
import asyncio
import aiohttp
import json
from db.db_redis import get_redis
import multiprocessing
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import xml.etree.ElementTree as ET
from utils.hash_policy import sha256_hex_utf8
from datetime import datetime, timedelta
import traceback
from edu.classes import CrawlingContext
from backend.shared.url_scope import extract_precise_scope_path_prefix

# ✅ 전역 SSL 검증 완전 비활성화
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', message='.*SSL.*')
warnings.filterwarnings('ignore', message='.*ssl.*')

# SSL 관련 모든 경고 무시
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# aiohttp 기본 SSL 설정 비활성화
aiohttp.ClientSession._build_ssl_context = lambda self, *args, **kwargs: False

# 전역 SSL 컨텍스트 설정
try:
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

# 환경변수로 SSL 검증 비활성화
os.environ['PYTHONHTTPSVERIFY'] = '0'
os.environ['SSL_VERIFY'] = 'False'

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.url", level=logging.INFO)


def db_save_trace_enabled() -> bool:
    v = (os.getenv("DEBUG_DB_SAVE_TRACE") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def db_save_trace_log(msg: str, *args: Any, level: int = logging.INFO, exc_info: bool = False) -> None:
    if db_save_trace_enabled():
        logger.log(level, "[DB-SAVE-TRACE] " + msg, *args, exc_info=exc_info)


# ✅ 배치 임베딩 및 벌크 삽입을 위한 전역 변수 (Config 설정 적용)
EMBEDDING_BATCH_SIZE = Config.URL_SINGLE_EMBEDDING_BATCH_SIZE  # 단일 URL 처리 시 배치 크기
DB_BULK_SIZE = Config.URL_SINGLE_DB_BULK_SIZE  # 단일 URL 처리 시 벌크 삽입 크기
CRAWLING_EMBEDDING_BATCH_SIZE = 50  # 크롤링 모드 최적화: 5 → 20으로 증가 (4배 성능 개선)

# ✅ 하이브리드 스트리밍 구조를 위한 배치 크기 설정
CHANGE_DETECTION_BATCH_SIZE = 50  # 변경 감지 배치 크기


def _learn_list_existing_subject(rec) -> str:
    try:
        if not rec:
            return ""
        r0 = rec[0]
        if isinstance(r0, dict):
            return str(r0.get("subject") or "").strip()
        return str(r0 or "").strip()
    except Exception:
        return ""


def _subject_is_url_or_content_placeholder(subj: str, content_url: str) -> bool:
    """subject 컬럼에 URL만 들어간 경우 재수집 시 실제 제목으로 보정할 수 있다."""
    s = (subj or "").strip()
    u = (content_url or "").strip()
    if not s or not u:
        return False
    if s == u:
        return True
    s_low, u_low = s.lower().rstrip("/"), u.lower().rstrip("/")
    if s_low == u_low:
        return True
    if not s_low.startswith("http"):
        return False
    try:
        from urllib.parse import urlparse, unquote

        def _nh(netloc: str) -> str:
            return (netloc or "").lower().replace("www.", "", 1)

        ps, pu = urlparse(s), urlparse(u)
        if _nh(ps.netloc) != _nh(pu.netloc):
            return False
        pth = unquote((ps.path or "").rstrip("/").lower())
        puth = unquote((pu.path or "").rstrip("/").lower())
        return pth == puth
    except Exception:
        return False


def _crawl_title_is_weak(value: str) -> bool:
    text = str(value or "").strip()
    if not text or text == "제목 없음":
        return True
    if re.match(r"^https?://", text, flags=re.IGNORECASE):
        return True
    compact = re.sub(r"\s+", "", text)
    return compact.lower() in {
        "content",
        "contents",
        "attachment",
        "attachments",
        "파일",
        "첨부파일",
    }


def _subject_is_trivial_placeholder(subj: str) -> bool:
    t = (subj or "").strip().lower()
    if t in {
        "이미지",
        "image",
        "제목 없음",
        "untitled",
        "null",
        "none",
    }:
        return True
    compact = re.sub(r"\s+", "", subj or "")
    if not compact:
        return True
    placeholder_chars = set("?-_./|:;()[]{}\\")
    return all(ch in placeholder_chars for ch in compact)


def _subject_differs_from_new_title(subj: str, new_title: str) -> bool:
    old_norm = re.sub(r"\s+", " ", str(subj or "").strip()).lower()
    new_norm = re.sub(r"\s+", " ", str(new_title or "").strip()).lower()
    if not old_norm or not new_norm:
        return False
    return old_norm != new_norm


async def _maybe_skip_existing_url_learning(
    *,
    source_url: str,
    title: str,
    result: Dict[str, Any],
    context: CrawlingContext,
    dbname: str,
    chat_bot_id: str,
    learn_list_content_type: str,
    learn_list_type: str,
    total_chunks: int,
    content_hash: str,
    favicon_url: str,
    content_bytes: int,
    start_time: float,
    url_index: int,
) -> Optional[Dict[str, Any]]:
    """
    기존 LEARN_LIST 행이 있으면 제목/분류 같은 메타만 보정하고
    청킹·임베딩·PG UPSERT는 건너뛴다.
    """
    if not chat_bot_id or learn_list_content_type == "file":
        return None
    if bool(getattr(context, "force_relearn", False) or getattr(context, "content_relearn_mode", False)):
        logger.info(
            "[RelearnDeleteDebug][duplicate_skip_bypass] job_id=%s url=%s",
            getattr(context, "job_id", ""),
            source_url[:180],
        )
        return None

    try:
        _name_lc = (dbname or "").strip().lower()
        if _name_lc in ("chatty", "naraone"):
            from db.mysql_db_config import mysql_execute_query as _exec_query
            _db_tag = "MySQL"
        else:
            from db.maria_operations import maria_execute_query as _exec_query
            _db_tag = "MariaDB"

        from db.mariadb_save_update import (
            ensure_learn_list_standard_columns,
            _coalesce_author_fields,
            _pick_first_existing_column,
            ensure_learn_list_type_not_blank,
        )

        table_name_db = f"ASADAL_{chat_bot_id[-12:]}_LEARN_LIST"
        cols_m = await ensure_learn_list_standard_columns(dbname, table_name_db)
        author_col = _pick_first_existing_column(
            cols_m, ("content_author", "content_au", "content_auth", "author")
        )
        author_val = _coalesce_author_fields(
            {
                "author": result.get("author"),
                "content_author": result.get("content_author"),
                "writer": result.get("writer"),
                "department": result.get("department"),
            }
        )
        use_author = bool(author_col and author_val and str(author_val).strip())
        use_type_col = bool("type" in cols_m)

        existing_any_record = await _exec_query(
            f"""
                SELECT id, subject, status FROM {table_name_db}
                WHERE content = %s
                ORDER BY id DESC
                LIMIT 1
            """,
            [source_url],
            fetch=True,
            dbname=dbname,
        )
        if not existing_any_record:
            return None

        sub_change_mode_on = False
        try:
            _cfg_map = await _fetch_crawling_config_map(dbname, chat_bot_id)
            sub_change_mode_on = str(_cfg_map.get("sub_change", "off")).lower() == "on"
        except Exception as _cfg_ex:
            logger.warning(f"[sub_change 조회 실패] 기본 off 적용: {_cfg_ex}")

        existing_sub = _learn_list_existing_subject(existing_any_record)
        title_stripped = (title or "").strip()
        update_subject_col = bool(sub_change_mode_on) or (
            bool(title_stripped)
            and (
                _subject_is_url_or_content_placeholder(existing_sub, source_url)
                or _subject_is_trivial_placeholder(existing_sub)
                or _subject_differs_from_new_title(existing_sub, title_stripped)
            )
        )

        existing_row = existing_any_record[0] if existing_any_record else None
        if isinstance(existing_row, dict):
            learn_list_row_id = existing_row.get("id")
            existing_status = str(existing_row.get("status") or "").strip().upper()
        else:
            learn_list_row_id = None
            existing_status = ""
        if existing_status != "Y":
            return None

        allow_existing_duplicate_update = str(
            os.getenv("BOARD_CRAWL_ALLOW_EXISTING_DUPLICATE_ROW_UPDATE", "0") or "0"
        ).strip().lower() in ("1", "true", "yes", "on")
        if not allow_existing_duplicate_update:
            logger.info(
                "[BoardDuplicate] existing LEARN_LIST row detected before embedding; learning skipped without modifying existing row | url=%s learn_list_id=%s status=%s",
                source_url,
                learn_list_row_id,
                existing_status,
            )
            return {
                "url": source_url,
                "title": title,
                "chunks": 0,
                "order": url_index,
                "processing_time": round(time.time() - start_time, 2),
                "favicon_url": favicon_url,
                "source_size": [content_bytes],
                "change_status": "duplicate_skipped",
                "chunk_hash": content_hash,
                "learn_list_inserted": False,
                "learn_list_action": "skip_existing",
                "learn_list_id": learn_list_row_id,
                "skipped_learning": True,
                "detected_chunks": total_chunks,
                "duplicate_existing_subject": existing_sub,
                "db_tag": _db_tag,
            }

        set_parts = ["content_type = %s"]
        params: list[Any] = [learn_list_content_type]
        if update_subject_col:
            set_parts.append("subject = %s")
            params.append(title)
        if getattr(context, "cate1", None):
            set_parts.append("cate1 = %s")
            params.append(context.cate1)
        if getattr(context, "cate2", None):
            set_parts.append("cate2 = %s")
            params.append(context.cate2)
        if use_type_col:
            set_parts.append("`type` = %s")
            params.append(learn_list_type)
        if use_author:
            set_parts.append(f"`{author_col}` = %s")
            params.append(author_val)

        params.append(source_url)
        await _exec_query(
            f"UPDATE {table_name_db} SET {', '.join(set_parts)} WHERE content = %s",
            params,
            fetch=False,
            dbname=dbname,
        )
        try:
            await ensure_learn_list_type_not_blank(
                db_name=str(dbname),
                learn_list_table=str(table_name_db),
                default_type=learn_list_type,
                content=source_url,
            )
        except Exception as _type_fix_ex:
            logger.warning(
                "[URL 중복 스킵] LEARN_LIST type 보정 실패 | url=%s err=%s",
                source_url,
                _type_fix_ex,
            )

        logger.info(
            "[BoardDuplicate] existing LEARN_LIST row detected before embedding; metadata-only update and learning skipped | url=%s learn_list_id=%s update_subject_col=%s",
            source_url,
            learn_list_row_id,
            update_subject_col,
        )
        return {
            "url": source_url,
            "title": title,
            "chunks": 0,
            "order": url_index,
            "processing_time": round(time.time() - start_time, 2),
            "favicon_url": favicon_url,
            "source_size": [content_bytes],
            "change_status": "duplicate_skipped",
            "chunk_hash": content_hash,
            "learn_list_inserted": False,
            "learn_list_action": "update_existing",
            "learn_list_id": learn_list_row_id,
            "skipped_learning": True,
            "detected_chunks": total_chunks,
            "duplicate_existing_subject": existing_sub,
            "db_tag": _db_tag,
        }
    except Exception as exc:
        logger.warning(
            "[BoardDuplicate] existing-row precheck failed, continue normal learning | url=%s err=%s",
            source_url,
            exc,
        )
        return None
CHANGE_DETECTION_TIMEOUT = 0.5     # 변경 감지 타임아웃 (초) - 0.5초로 단축하여 반응성 향상
DB_SAVE_BATCH_SIZE = 50            # DB 저장 배치 크기
DB_SAVE_TIMEOUT = 0.3             # DB 저장 타임아웃 (초) - 0.3초로 단축하여 반응성 향상

# ✅ 웹소켓 메시지 전송 전 값 정리 헬퍼 함수
def prepare_crawl_count_message(
    total_discovered: int = 0,
    count: int = 0,
    success_url: int = 0,
    current_url: str = "",
    filter_type: str = "",
    **kwargs
) -> dict:
    """
    crawl_count 타입 웹소켓 메시지를 전송하기 전에 값을 정리하는 헬퍼 함수
    
    Args:
        total_discovered: 전체 탐색한 URL 수
        count: 변경 감지 후 실제 DB 저장 대상 개수
        success_url: 실제 DB 저장 한 개수
        current_url: 현재 처리 중인 URL
        filter_type: URL 필터 타입
        **kwargs: 추가 필드 (processed_url, total_chunks_processed, chunk_hash 등)
    
    Returns:
        정리된 웹소켓 메시지 딕셔너리
    """
    message = {
        "status": "completed",
        "type": "crawl_count",
        "total_count": total_discovered,
        "collection_count": count,
        "save_count": success_url,
        "study_count": success_url, 
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    # 추가 필드가 있으면 추가
    if kwargs:
        message.update(kwargs)
    return message

# ✅ 웹페이지 변경감지 시스템 (3단계 접근법)
async def check_url_changes(url: str, table_name: str, dbname: str) -> dict:
    """
    URL 변경 감지 호환 함수.

    게시판/파일 크롤링 공용 경로에서는 content_hash 기반 변경감지를 사용하지 않는다.
    """
    logger.debug("[변경감지 비활성] URL 해시 비교 스킵: %s", url)
    return {'status': 'NEW_URL', 'reason': 'CONTENT_HASH_DISABLED'}
    try:
        stored_metadata = await get_url_metadata(url, table_name, dbname)
        
        if not stored_metadata:
            return {'status': 'NEW_URL', 'reason': '기존 데이터 없음'}
        
        content_result = await check_content_hash(url, stored_metadata)
        
        return content_result
        
    except Exception as e:
        logger.error(f"[변경감지 오류] {url}: {e}")
        return {'status': 'ERROR', 'message': str(e)}

async def check_content_hash(url: str, stored_metadata: dict) -> dict:
    """
    메인 콘텐츠 HASH 비교 호환 함수.
    """
    logger.debug("[변경감지 비활성] 콘텐츠 해시 확인 스킵: %s", url)
    return {'status': 'CHANGED', 'reason': 'CONTENT_HASH_DISABLED'}
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        try:
            ssl_context.options |= ssl.OP_LEGACY_SERVER_CONNECT
        except AttributeError:
            pass

        connector = aiohttp.TCPConnector(ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(url) as resp:
                html_content = await resp.text()
        
        main_content = extract_main_content(html_content)
        current_hash = sha256_hex_utf8(main_content)
        stored_hash = stored_metadata.get('content_hash')
        
        if stored_hash and current_hash == stored_hash:
            return {'status': 'NO_CHANGE', 'reason': 'CONTENT_HASH_SAME'}
        
        return {
            'status': 'CHANGED',
            'reason': 'CONTENT_HASH_CHANGED',
            'current_hash': current_hash,
            'html_content': html_content
        }
        
    except Exception as e:
        logger.error(f"[변경감지 해시 오류] {url}: {e}")
        return {'status': 'ERROR', 'reason': f'콘텐츠 해시 확인 오류: {str(e)}'}

async def get_url_metadata(url: str, table_name: str, dbname: str) -> dict:
    """
    URL의 기존 메타데이터 조회 호환 함수.

    content_hash 변경감지를 쓰지 않으므로 DB 조회를 수행하지 않는다.
    """
    logger.debug("[메타데이터 조회 스킵] content_hash 변경감지 비활성: %s", url)
    return None
    try:
        from db.db_config import connect_db, return_connection
        conn = await connect_db(dbname)
        
        # content_metadata JSONB 필드에서 메타데이터 조회 (우선)
        query_jsonb = """
            SELECT content_metadata
            FROM {table_name}
            WHERE content = $1
            AND content_metadata IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        """.format(table_name=table_name)
        
        result_jsonb = await conn.fetchrow(query_jsonb, url)
        
        if result_jsonb and result_jsonb['content_metadata']:
            # JSONB 필드에서 메타데이터 추출
            content_metadata_raw = result_jsonb['content_metadata']
            
            # content_metadata가 문자열인 경우 JSON으로 파싱
            if isinstance(content_metadata_raw, str):
                try:
                    import json
                    content_metadata = json.loads(content_metadata_raw)
                    logger.debug(f"[메타데이터 조회] JSONB 문자열을 딕셔너리로 파싱: {url}")
                except json.JSONDecodeError as e:
                    logger.warning(f"[메타데이터 조회] JSONB 파싱 실패: {url}, 오류: {e}")
                    content_metadata = {}
            else:
                # 이미 딕셔너리인 경우
                content_metadata = content_metadata_raw
            
            # 메타데이터 추출 (content_hash만 사용)
            metadata = {
                'content_hash': content_metadata.get('content_hash') if isinstance(content_metadata, dict) else None,
                'update_frequency': content_metadata.get('update_frequency', '1_day') if isinstance(content_metadata, dict) else '1_day'
            }
            logger.debug(f"[메타데이터 조회] JSONB 필드에서 발견: {url}, 해시: {metadata.get('content_hash', 'None')}")
            return metadata
        
        # content_metadata가 없는 경우 None 반환
        logger.debug(f"[메타데이터 조회] content_metadata 없음: {url}")
        return None
            
    except Exception as e:
        logger.error(f"[메타데이터 조회 오류] {url}: {e}")
        return None
    finally:
        if 'conn' in locals():
            await return_connection(conn, dbname)


async def is_url_in_db(url: str, table_name: str, dbname: str) -> bool:
    """
    URL이 MariaDB learn_list 테이블에 이미 존재하는지 조회 (중복 판정용).
    - 비교 대상: MariaDB의 _learn_list 테이블 (content).
    - 들어오는 URL과 DB에서 불러온 URL 값을 동일한 정규화 함수
      (normalize_url_protocol_agnostic)로 정규화한 뒤 비교하여,
      동일 조건에서 중복 여부를 판단한다.
    메모리(visited) 비교 없이 실제 DB 기준으로만 판단할 때 사용.
    """
    try:
        if not table_name or not dbname:
            return False
        norm_url = normalize_url_protocol_agnostic(url)
        from db.maria_operations import maria_execute_query
        # MariaDB learn_list: content URL 저장 컬럼 조회 후 정규화해 비교
        query = """
            SELECT content
            FROM `{table_name}`
            WHERE content IS NOT NULL AND content != ''
        """.format(table_name=table_name)
        rows = await maria_execute_query(query, None, fetch=True, dbname=dbname)
        if not rows:
            return False
        for row in rows:
            try:
                val = row.get("content") if isinstance(row, dict) else None
            except (KeyError, IndexError, TypeError):
                continue
            if not val:
                continue
            try:
                norm_db = normalize_url_protocol_agnostic(str(val))
            except Exception:
                continue
            if norm_db == norm_url:
                return True
        return False
    except Exception as e:
        logger.warning(f"[DB 중복 조회 오류] url={url[:80]}..., 오류: {e}")
        return False


async def get_url_by_content_hash(content_hash: str, table_name: str, dbname: str, exclude_url: str = None) -> str:
    """
    content_hash 기반 URL 조회 호환 함수.
    
    Args:
        content_hash: 콘텐츠 해시값
        table_name: 테이블명
        dbname: 데이터베이스명
        exclude_url: 제외할 URL (현재 검사 중인 URL, None이면 제외하지 않음)
    
    Returns:
        같은 해시를 가진 URL (없으면 None)
    """
    logger.debug("[해시 중복 조회 스킵] content_hash 변경감지 비활성")
    return None
    try:
        if not table_name or not dbname:
            logger.debug(f"[2단계 중복 검사] 테이블명 또는 DB명 없음 - 스킵 (table: {table_name}, db: {dbname})")
            return None
            
        from db.db_config import connect_db, return_connection
        conn = await connect_db(dbname)
        
        # content_metadata JSONB 필드에서 해시로 URL 조회
        # exclude_url이 있으면 현재 URL을 제외하고 조회 (다른 URL의 중복만 검사)
        if exclude_url:
            logger.debug(f"[2단계 중복 검사 시작] 해시: {content_hash[:16]}..., 현재 URL 제외: {exclude_url}")
            query = """
                SELECT content
                FROM {table_name}
                WHERE content_metadata IS NOT NULL
                AND content_metadata->>'content_hash' = $1
                AND content != $2
                ORDER BY created_at DESC
                LIMIT 1
            """.format(table_name=table_name)
            result = await conn.fetchrow(query, content_hash, exclude_url)
        else:
            logger.debug(f"[2단계 중복 검사 시작] 해시: {content_hash[:16]}..., 제외 URL 없음")
            query = """
                SELECT content
                FROM {table_name}
                WHERE content_metadata IS NOT NULL
                AND content_metadata->>'content_hash' = $1
                ORDER BY created_at DESC
                LIMIT 1
            """.format(table_name=table_name)
            result = await conn.fetchrow(query, content_hash)
        
        if result and result['content']:
            url = result['content']
            logger.info(f"[2단계 중복 검사 결과] ✅ 중복 발견 - 해시: {content_hash[:16]}...")
            logger.info(f"   📌 현재 URL: {exclude_url or '(제외 없음)'}")
            logger.info(f"   🔗 기존 URL: {url}")
            return url
        
        logger.debug(f"[2단계 중복 검사 결과] ❌ 중복 없음 - 해시: {content_hash[:16]}...")
        return None
            
    except Exception as e:
        logger.error(f"[2단계 중복 검사 오류] 해시: {content_hash[:16]}..., 오류: {e}")
        import traceback
        logger.error(f"[2단계 중복 검사 오류 상세] {traceback.format_exc()}")
        return None
    finally:
        if 'conn' in locals():
            await return_connection(conn, dbname)


async def batch_get_urls_by_content_hash(
    content_hashes: List[str],
    table_name: str,
    dbname: str,
) -> Dict[str, List[str]]:
    """content_hash 기반 배치 URL 조회 호환 함수."""
    logger.debug("[배치 해시 중복 조회 스킵] content_hash 변경감지 비활성")
    return {}
    if not content_hashes or not table_name or not dbname:
        return {}

    unique_hashes = []
    seen = set()
    for h in content_hashes:
        h_text = str(h or "").strip()
        if not h_text or h_text in seen:
            continue
        seen.add(h_text)
        unique_hashes.append(h_text)
    if not unique_hashes:
        return {}

    conn = None
    try:
        from db.db_config import connect_db, return_connection

        conn = await connect_db(dbname)
        query = """
            SELECT content, content_metadata->>'content_hash' AS content_hash
            FROM {table_name}
            WHERE content_metadata IS NOT NULL
            AND content_metadata->>'content_hash' = ANY($1)
            AND content IS NOT NULL
            AND content != ''
            ORDER BY content_metadata->>'content_hash', created_at DESC
        """.format(table_name=table_name)
        rows = await conn.fetch(query, unique_hashes)

        def _row_value(row: Any, key: str) -> Any:
            try:
                if isinstance(row, dict):
                    return row.get(key)
                return row[key]
            except Exception:
                return None

        by_hash: Dict[str, List[str]] = {}
        for row in rows or []:
            h = str(_row_value(row, "content_hash") or "").strip()
            content = str(_row_value(row, "content") or "").strip()
            if not h or not content:
                continue
            by_hash.setdefault(h, []).append(content)
        logger.info(
            "[배치 해시 중복 조회] hashes=%s matched_hashes=%s matched_urls=%s table=%s",
            len(unique_hashes),
            len(by_hash),
            sum(len(v) for v in by_hash.values()),
            table_name,
        )
        return by_hash
    except Exception as e:
        logger.error("[배치 해시 중복 조회 오류] hashes=%s 오류=%s", len(unique_hashes), e)
        logger.debug("[배치 해시 중복 조회 오류 상세] %s", traceback.format_exc())
        return {}
    finally:
        if conn is not None:
            try:
                from db.db_config import return_connection
                await return_connection(conn, dbname)
            except Exception:
                pass

async def get_urls_by_content_hash(content_hash: str, table_name: str, dbname: str) -> list:
    """
    content_hash 기반 URL 목록 조회 호환 함수.
    
    Args:
        content_hash: 콘텐츠 해시값
        table_name: 테이블명
        dbname: 데이터베이스명
    
    Returns:
        같은 해시를 가진 URL 리스트
    """
    logger.debug("[해시 기반 URL 목록 조회 스킵] content_hash 변경감지 비활성")
    return []
    try:
        if not table_name or not dbname:
            return []
            
        from db.db_config import connect_db, return_connection
        conn = await connect_db(dbname)
        
        # content_metadata JSONB 필드에서 해시로 모든 URL 조회
        query = """
            SELECT DISTINCT content
            FROM {table_name}
            WHERE content_metadata IS NOT NULL
            AND content_metadata->>'content_hash' = $1
            ORDER BY created_at DESC
        """.format(table_name=table_name)
        
        results = await conn.fetch(query, content_hash)
        
        urls = [row['content'] for row in results if row['content']]
        
        if urls:
            logger.debug(f"[3차 매핑 추적] 해시로 {len(urls)}개 URL 발견: {content_hash[:16]}...")
            logger.debug(f"   - URL 목록: {urls[:5]}{'...' if len(urls) > 5 else ''}")
        
        return urls
            
    except Exception as e:
        logger.error(f"[해시 기반 URL 목록 조회 오류] 해시: {content_hash[:16]}..., 오류: {e}")
        return []
    finally:
        if 'conn' in locals():
            await return_connection(conn, dbname)

async def batch_get_url_metadata(urls: list, table_name: str, dbname: str, batch_size: int = 50) -> dict:
    """
    여러 URL의 메타데이터 조회 호환 함수.
    
    Args:
        urls: 조회할 URL 리스트
        table_name: 테이블명
        dbname: 데이터베이스명
        batch_size: 한 번에 조회할 URL 개수 (기본값: 20)
    
    Returns:
        URL을 키로 하는 메타데이터 딕셔너리
    """
    logger.debug("[배치 메타데이터 조회 스킵] content_hash 변경감지 비활성 urls=%s", len(urls or []))
    return {}
    try:
        if not urls:
            return {}
        
        logger.info(f"📦 [배치 메타데이터 조회 시작] 총 {len(urls)}개 URL, 배치 크기: {batch_size}개")
        
        from db.db_config import connect_db, return_connection
        
        metadata_dict = {}
        total_batches = (len(urls) + batch_size - 1) // batch_size
        conn = await connect_db(dbname)
        
        # ✅ URL을 배치 크기로 나누어서 조회 (DB 부하 방지)
        try:
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(urls))
                url_batch = urls[start_idx:end_idx]
                
                logger.info(f"📦 [배치 {batch_idx + 1}/{total_batches}] {len(url_batch)}개 URL 조회 중... ({start_idx + 1}~{end_idx}/{len(urls)})")
                
                # IN 절을 사용하여 배치 조회
                query = """
                    SELECT DISTINCT ON (content) content, content_metadata
                    FROM {table_name}
                    WHERE content = ANY($1)
                    AND content_metadata IS NOT NULL
                    ORDER BY content, created_at DESC
                """.format(table_name=table_name)

                results = await conn.fetch(query, url_batch)

                # 결과를 URL: metadata 딕셔너리로 변환
                for row in results:
                    url = row['content']
                    content_metadata_raw = row['content_metadata']
                    
                    # content_metadata 파싱
                    if isinstance(content_metadata_raw, str):
                        try:
                            import json
                            content_metadata = json.loads(content_metadata_raw)
                        except json.JSONDecodeError:
                            content_metadata = {}
                    else:
                        content_metadata = content_metadata_raw
                    
                    metadata_dict[url] = {
                        'content_hash': content_metadata.get('content_hash') if isinstance(content_metadata, dict) else None,
                        'update_frequency': content_metadata.get('update_frequency', '1_day') if isinstance(content_metadata, dict) else '1_day'
                    }

                logger.info(f"✅ [배치 {batch_idx + 1}/{total_batches}] {len(results)}개 기존 데이터 발견")
        finally:
            if conn:
                await return_connection(conn, dbname)
        
        logger.info(f"📦 [배치 메타데이터 조회 완료] 총 {len(urls)}개 URL 중 {len(metadata_dict)}개 기존 데이터 발견")
        return metadata_dict
            
    except Exception as e:
        logger.error(f"❌ [배치 메타데이터 조회 오류] {e}")
        import traceback
        logger.error(f"❌ [배치 메타데이터 조회 오류 상세] {traceback.format_exc()}")
        return {}


async def batch_check_url_changes(crawl_results: list, table_name: str, dbname: str) -> tuple:
    """
    크롤링 결과 전체를 저장 대상으로 통과시킨다.

    게시판/파일 크롤링에서는 content_hash 기반 변경감지를 적용하지 않는다.
    이 함수는 기존 호출부 호환을 위해 유지하되, 배치마다 메타데이터 조회와
    해시 비교를 수행하지 않도록 단순 통과 처리한다.
    
    Args:
        crawl_results: 크롤링 결과 리스트 (각 항목은 {'source': url, 'content': text, ...} 형태)
        table_name: 테이블명
        dbname: 데이터베이스명
    
    Returns:
        (변경된_항목_리스트, 변경없는_항목_리스트) 튜플
    """
    try:
        if not crawl_results:
            logger.info("[배치 변경감지 스킵] 크롤링 결과가 비어있음")
            return [], []

        start_time = time.time()
        changed_items = [result for result in crawl_results if result and result.get("source")]
        elapsed_time = time.time() - start_time

        logger.info(
            "[배치 변경감지 스킵] elapsed=%.3fs total=%s passthrough=%s table=%s db=%s",
            elapsed_time,
            len(crawl_results),
            len(changed_items),
            table_name,
            dbname,
        )

        return changed_items, []
        
    except Exception as e:
        logger.error(f"❌ [배치 변경감지 오류] {e}")
        import traceback
        logger.error(f"❌ [배치 변경감지 오류 상세] {traceback.format_exc()}")
        # 오류 발생 시 모든 항목을 변경된 것으로 처리
        return crawl_results, []

async def should_check_url_changes(url: str, table_name: str, dbname: str, force_check: bool = False) -> bool:
    """
    URL 변경감지 수행 여부 판단
    
    Args:
        url: 확인할 URL
        table_name: 테이블명
        dbname: 데이터베이스명
        force_check: 강제 확인 여부 (테스트용)
    
    Note:
        게시판/파일 크롤링 공용 경로에서는 content_hash 변경감지를 수행하지 않습니다.
    """
    logger.debug("[변경감지 비활성] URL 변경감지 수행 안 함: %s", url)
    return False

# ✅ 배치 임베딩 및 품질 평가 시스템
async def batch_embeddings(texts: List[str]) -> List[List[float]]:
    """Legacy direct embedding path disabled.

    Runtime learning now submits chunks through backend.shared.batch_embedding_scheduler
    and stores vectors only after the embedding batch callback returns.
    """
    raise RuntimeError(
        "Direct embedding is disabled. Use the batch embedding scheduler instead."
    )


async def bulk_insert_data(
    table_name: str,
    data_list: List[Dict],
    dbname: str,
    job_manager: Optional[AsyncJobManager] = None,
    job_id: str = ""
) -> List[Dict]:
    """대량 청크 데이터를 배치로 삽입한다.

    데이터베이스 벌크 삽입 과정 중 `use_crawl_stop` 상태가 감지되면 즉시 중단하고
    이미 저장이 완료된 레코드만 반환한다.

    Args:
        table_name: 삽입 대상 테이블명.
        data_list: 저장할 레코드 딕셔너리 목록.
        dbname: 연결할 데이터베이스 이름.
        job_manager: 작업 상태 조회용 매니저. 미전달 시 상태 확인을 생략한다.
        job_id: 상태 조회에 사용할 작업 ID. `job_manager`가 있을 때만 의미가 있다.

    Returns:
        실제로 DB에 저장된 레코드 목록. 중단되면 부분 목록만 반환한다.

    Raises:
        Exception: 벌크 삽입 도중 예외가 발생하면 그대로 전달한다.
    """

    if not data_list:
        db_save_trace_log(
            "pg.batch_upsert.skip no_data db=%s table=%s job_id=%s",
            dbname,
            table_name,
            job_id,
            level=logging.WARNING,
        )
        return []

    inserted_records: List[Dict] = []

    try:
        logger.info(f"[벌크 삽입 시작] {len(data_list)}개 레코드, 테이블: {table_name}")

        # 데이터 배치별로 나누어 삽입
        for i in range(0, len(data_list), DB_BULK_SIZE):
            # ✅ 중단 신호 체크 제거: 이미 수집된 데이터는 모두 저장 보장

            batch = data_list[i:i + DB_BULK_SIZE]

            # 벌크 삽입 실행
            await _execute_bulk_insert(table_name, batch, dbname)
            inserted_records.extend(batch)

            logger.info(
                f"[벌크 삽입 진행] {min(i + DB_BULK_SIZE, len(data_list))}/{len(data_list)} 완료"
            )

        logger.info(f"[벌크 삽입 종료] 총 {len(inserted_records)}개 레코드 저장")
        return inserted_records

    except Exception as e:
        logger.error(f"[벌크 삽입 실패] {e}")
        raise

async def _is_use_crawl_stop_active(
    job_manager: Optional[AsyncJobManager],
    job_id: str
) -> bool:
    """작업 상태를 조회해 `use_crawl_stop` 여부를 확인한다.

    Args:
        job_manager: 작업 상태를 조회할 `AsyncJobManager` 인스턴스.
        job_id: 상태 조회에 사용할 작업 ID.

    Returns:
        중단 신호가 활성화되어 있으면 True, 아니면 False.
    """

    if not job_manager or not job_id:
        return False

    try:
        status = await job_manager.get_job_status(job_id)
        return status == "use_crawl_stop"
    except Exception as status_error:
        logger.warning(f"[use_crawl_stop 확인 실패] job_id={job_id}, 오류: {status_error}")
        return False

async def save_url_metadata_immediately(url: str, metadata: dict, table_name: str, dbname: str):
    """
    URL의 메타데이터 즉시 저장 (변경감지용 필드) - 모든 청크에 저장
    """
    try:
        logger.info(f"[메타데이터 즉시 저장] URL: {url}, 메타데이터: {metadata}")
        
        from db.db_config import connect_db, return_connection
        conn = await connect_db(dbname)
        
        # 기존 데이터가 있는지 먼저 확인 (모든 청크)
        check_query = """
            SELECT COUNT(*) as count FROM {table_name}
            WHERE content = $1
        """.format(table_name=table_name)
        
        count_result = await conn.fetchrow(check_query, url)
        existing_count = count_result['count'] if count_result else 0
        
        logger.info(f"[메타데이터 즉시 저장] 기존 데이터 확인: {url}, 청크 개수: {existing_count}")
        
        if existing_count > 0:
            # 웹사이트 중복 크롤링 방지를 위한 변경감지 메타데이터 업데이트
            update_query = """
                UPDATE {table_name}
                SET content_metadata = (
                    COALESCE(content_metadata, '{{}}'::jsonb)
                    - 'source'
                    - 'file_url'
                    - 'download_url'
                    - 'page_url'
                    - 'post_url'
                    - 'source_page'
                    - 'sourcePage'
                    - 'origin_url'
                    - 'originUrl'
                    - 'original_url'
                    - 'originalUrl'
                    - 'reg_date'
                    - 'chunk_num'
                    - 'chunk_number'
                    - 'file_name'
                    - 'file_path'
                    - 'local_path'
                    - 'file_size'
                    - 'content_type'
                    - 'title'
                    - 'learn_list_id'
                    - 'author'
                    - 'content_author'
                    - 'department'
                    - 'author_kind'
                    - 'job_id'
                ) || jsonb_build_object(
                    'content_hash', $1,
                    'update_frequency', $2
                )
                WHERE content = $3
            """.format(table_name=table_name)
            
            result = await conn.execute(
                update_query,
                metadata.get('content_hash'),
                metadata.get('update_frequency', '1_day'),
                url
            )
            
            logger.info(f"[메타데이터 즉시 저장 성공] 중복 크롤링 방지용 변경감지 메타데이터 업데이트 완료: {url}, 업데이트된 행 수: {result}")
            return True
        else:
            logger.warning(f"[메타데이터 즉시 저장] 기존 데이터 없음: {url} (청크 데이터가 아직 저장되지 않음)")
            return False
        
    except Exception as e:
        logger.error(f"[메타데이터 즉시 저장 오류] URL: {url}, 오류: {e}")
        import traceback
        logger.error(f"[메타데이터 즉시 저장 오류 상세] {traceback.format_exc()}")
        return False
    finally:
        if 'conn' in locals():
            await return_connection(conn, dbname)

async def upsert_data_by_content_and_chunk(table_name: str, data: Dict, dbname: str):
    """
    content와 chunk_num을 기준으로 데이터를 upsert하는 함수 (db_operations.py 방식 사용)
    """
    try:
        from db.db_operations import (
            execute_query,
            insert_data_with_metadata,
            update_data_with_metadata,
            _normalize_learning_payload,
            _prepare_learning_row_common,
        )

        # insert_data 와 동일하게 정규화 후 content 는 subject 와 일치(원본 URL은 content_metadata)
        working = _normalize_learning_payload(dict(data))
        _prepare_learning_row_common(working)

        content_value = working.get("content")
        chunk_num_value = working.get("chunk_num")
        db_save_trace_log(
            "pg.upsert.start db=%s table=%s content=%s chunk_num=%s keys=%s",
            dbname,
            table_name,
            str(content_value or "")[:220],
            chunk_num_value,
            ",".join(sorted(str(k) for k in working.keys())),
        )

        if not content_value or chunk_num_value is None:
            db_save_trace_log(
                "pg.upsert.blocked missing_required db=%s table=%s content_present=%s chunk_num=%r keys=%s",
                dbname,
                table_name,
                bool(content_value),
                chunk_num_value,
                ",".join(sorted(str(k) for k in working.keys())),
                level=logging.WARNING,
            )
            raise ValueError("content와 chunk_num은 필수 필드입니다.")
        
        # chunk_num을 문자열로 정규화 (DB 컬럼이 VARCHAR 타입)
        chunk_num_str = str(chunk_num_value) if chunk_num_value is not None else "0"
        
        # 기존 데이터 확인 (content와 chunk_num 기준)
        check_query = """
            SELECT COUNT(*) as count FROM {table_name}
            WHERE content = $1 AND chunk_num = $2
        """.format(table_name=table_name)
        
        count_result = await execute_query(check_query, (content_value, chunk_num_str), fetch=True, dbname=dbname)
        existing_count = count_result[0]['count'] if count_result else 0
        db_save_trace_log(
            "pg.upsert.lookup db=%s table=%s content=%s chunk_num=%s existing_count=%s",
            dbname,
            table_name,
            str(content_value or "")[:220],
            chunk_num_str,
            existing_count,
        )
        
        logger.debug(f"[UPSERT] 기존 데이터 확인: content={content_value[:50]}..., chunk_num={chunk_num_value}, 개수: {existing_count}")
        
        if existing_count > 0:
            # UPDATE 실행 (기존 데이터가 있는 경우)
            logger.debug(f"[UPSERT] 기존 데이터 발견, UPDATE 수행: content={content_value[:50]}..., chunk_num={chunk_num_value}")
            
            # UPDATE용 데이터 준비 (content, chunk_num 제외)
            update_data_dict = {k: v for k, v in working.items() if k not in ['content', 'chunk_num']}
            
            # UPDATE 실행 (JSONB 필드 자동 처리) - chunk_num 문자열 타입 사용
            conditions = {'content': content_value, 'chunk_num': chunk_num_str}
            returning_id = await update_data_with_metadata(table_name, update_data_dict, conditions, dbname)
            db_save_trace_log(
                "pg.upsert.done db=%s table=%s action=update returning_id=%s content=%s chunk_num=%s",
                dbname,
                table_name,
                returning_id,
                str(content_value or "")[:220],
                chunk_num_str,
            )
            
            logger.debug(f"[UPSERT] UPDATE 성공: content={content_value[:50]}..., chunk_num={chunk_num_value}, returning_id={returning_id}")
            return {'returning_id': returning_id, 'is_insert': False}
        else:
            # INSERT 실행 (기존 데이터가 없는 경우)
            logger.debug(f"[UPSERT] 기존 데이터 없음, INSERT 수행: content={content_value[:50]}..., chunk_num={chunk_num_value}")
            
            # INSERT용 데이터 준비 - chunk_num 타입 정규화 (정규화된 working 기준)
            insert_data_dict = dict(working)
            insert_data_dict['chunk_num'] = chunk_num_str  # 문자열 타입으로 통일
            
            # INSERT 실행 (JSONB 필드 자동 처리)
            returning_id = await insert_data_with_metadata(table_name, insert_data_dict, dbname)
            db_save_trace_log(
                "pg.upsert.done db=%s table=%s action=insert returning_id=%s content=%s chunk_num=%s",
                dbname,
                table_name,
                returning_id,
                str(content_value or "")[:220],
                chunk_num_str,
            )
            
            logger.debug(f"[UPSERT] INSERT 성공: content={content_value[:50]}..., chunk_num={chunk_num_value}, returning_id={returning_id}")
            return {'returning_id': returning_id, 'is_insert': True}

    except Exception as e:
        db_save_trace_log(
            "pg.upsert.error db=%s table=%s err=%s raw_keys=%s raw_content=%s",
            dbname,
            table_name,
            e,
            ",".join(sorted(str(k) for k in (data or {}).keys())) if isinstance(data, dict) else "-",
            str((data or {}).get("content") if isinstance(data, dict) else "")[:220],
            level=logging.ERROR,
            exc_info=True,
        )
        logger.error(f"[UPSERT] {e}")
        raise

async def _execute_bulk_insert(table_name: str, batch_data: List[Dict], dbname: str):
    """실제 벌크 삽입 실행 (insert_data 와 동일한 content/subject 전처리 적용)"""
    if not batch_data:
        return

    from db.db_operations import _normalize_learning_payload, _prepare_learning_row_common

    prepared: List[Dict] = []
    for raw in batch_data:
        w = _normalize_learning_payload(dict(raw))
        _prepare_learning_row_common(w)
        cm = w.get("content_metadata")
        if isinstance(cm, dict):
            try:
                w["content_metadata"] = json.dumps(cm, ensure_ascii=False)
            except Exception:
                pass
        prepared.append(w)
    batch_data = prepared

    # 동적 쿼리 생성 (created_at 필드 추가)
    columns = list(batch_data[0].keys())
    columns.append('created_at')

    placeholders = ', '.join([f'${i+1}' for i in range(len(columns) - 1)]) + ', NOW()'
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
    """

    # 벌크 삽입 실행
    conn = None
    try:
        from db.db_config import connect_db, return_connection
        conn = await connect_db(dbname)

        row_values_list = [
            tuple(item.get(col) for col in columns[:-1])
            for item in batch_data
        ]
        async with conn.transaction():
                # created_at 제외한 값들만 전달 (NOW()는 쿼리에서 처리)
            await conn.executemany(insert_query, row_values_list)

        logger.debug(f"[벌크 삽입 성공] {len(batch_data)}개 레코드 삽입 완료")

    except Exception as e:
        logger.error(f"[벌크 삽입 쿼리 실패] {e}")
        raise
    finally:
        if conn:
            await return_connection(conn, dbname)

async def extract_content_from_url(
    url: str,
    table_name: str = None,
    dbname: str = None,
    enable_change_detection: bool = False,
    crawl_mode: str = None,
    stop_signal: CrawlStopSignal = None,
    chat_bot_id: str = None
) -> Dict[str, str]:
    """URL에서 콘텐츠를 추출하는 스마트 하이브리드 함수."""
    logger.debug(f"[콘텐츠 추출 시작] URL: {url}")

    try:
        # URL 정규화
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        # ✅ 변경감지 로직 (table_name과 dbname이 제공된 경우)
        if enable_change_detection and table_name and dbname:
            # 환경변수로 강제 실행 옵션 확인 (테스트용)
            force_check = os.getenv('FORCE_CHANGE_DETECTION', 'false').lower() == 'true'
            
            # 업데이트 주기 확인
            should_check = await should_check_url_changes(url, table_name, dbname, force_check=force_check)
            if should_check:
                # 3단계 변경감지 수행
                change_result = await check_url_changes(url, table_name, dbname)
                
                if change_result['status'] == 'NO_CHANGE':
                    logger.info(f"[변경감지] 콘텐츠 변경 없음, 기존 데이터 유지: {url}")
                    return {'status': 'NO_CHANGE', 'reason': change_result['reason']}
                elif change_result['status'] == 'LIKELY_NO_CHANGE':
                    logger.info(f"[변경감지] 콘텐츠 변경 없을 가능성 높음, 기존 데이터 유지: {url}")
                    return {'status': 'NO_CHANGE', 'reason': change_result['reason']}
                elif change_result['status'] == 'CHANGED':
                    logger.info(f"[변경감지] 콘텐츠 변경 감지, 새로 학습 진행: {url}")
                    # 콘텐츠 해시에서 이미 HTML을 가져온 경우 재사용
                    if change_result.get('html_content'):
                        # ✅ DB에서 block 태그 조회
                        block_tag = None
                        if chat_bot_id and crawl_mode == "crawling":
                            try:
                                parsed_url = urlparse(url)
                                domain = parsed_url.netloc or parsed_url.path.split('/')[0] if parsed_url.path else ''
                                block_tag = await fetch_subject_block_from_db(domain, chat_bot_id, dbname)
                            except Exception as e:
                                logger.warning(f"[block 태그 조회 실패] URL: {url}, 오류: {e}")
                        
                        # 크롤링 모드에 따른 HTML 파싱 함수 선택
                        if crawl_mode == "crawling":
                            result = parse_html_content_for_crawling_mode(change_result['html_content'], url, block_tag=block_tag)
                            logger.info(f"[크롤링모드 변경감지 파싱] URL: {url}, block_tag: {block_tag}")
                        else:
                            result = parse_html_content(change_result['html_content'], url)
                        if result:
                            # 메타데이터 즉시 저장 (content_hash만)
                            metadata = {'content_hash': change_result.get('current_hash')}
                            await save_url_metadata_immediately(url, metadata, table_name, dbname)
                            return result
                elif change_result['status'] == 'NEW_URL':
                    logger.info(f"[변경감지] 신규 URL, 새로 학습 진행: {url}")
                    # 신규 URL은 아래 일반 처리 로직에서 메타데이터 수집
                else:
                    logger.warning(f"[변경감지] 오류 발생, 일반 처리로 진행: {url} - {change_result.get('message', '')}")
            else:
                logger.debug(f"[변경감지] 업데이트 주기 미도달, 기존 데이터 유지: {url}")
                return {'status': 'NO_CHANGE', 'reason': '업데이트 주기 미도달'}

        # ✅ 정부 사이트 감지 - endswith로 정확한 도메인 매칭
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        is_government_site = any(domain.endswith(gov_domain) for gov_domain in [
            '.go.kr', '.gov.kr', 'mois.go.kr', 'sd.go.kr', 'korea.kr', 'childfund.or.kr',
            'nowon.kr'
        ])

        # ✅ 메타데이터 수집을 위해 변경감지 활성화 여부 전달
        collect_metadata = enable_change_detection and table_name and dbname
        
        # ✅ DB에서 block 태그 조회
        block_tag = None
        if chat_bot_id and crawl_mode == "crawling":
            try:
                block_tag = await fetch_subject_block_from_db(domain, chat_bot_id, dbname)
            except Exception as e:
                logger.warning(f"[block 태그 조회 실패] URL: {url}, 오류: {e}")
        
        if is_government_site:
            # 정부 사이트는 Playwright 우선 사용
            logger.debug(f"[정부 사이트 감지] Playwright 사용: {url}")
            result = await extract_with_playwright(
                url,
                collect_metadata=collect_metadata,
                crawl_mode=crawl_mode,
                stop_signal=stop_signal,
                block_tag=block_tag
            )
        else:
            # 일반 사이트는 HTTP 우선, 실패시 Playwright
            try:
                result = await extract_with_http(url, collect_metadata=collect_metadata, crawl_mode=crawl_mode, block_tag=block_tag)
                if result and result.get("content") and len(result["content"]) > 100:
                    logger.debug(f"[HTTP 추출 성공] URL: {url}")
                else:
                    logger.debug(f"[HTTP 추출 부족] Playwright로 전환: {url}")
                    result = await extract_with_playwright(
                        url,
                        collect_metadata=collect_metadata,
                        crawl_mode=crawl_mode,
                        stop_signal=stop_signal,
                        block_tag=block_tag
                    )
            except Exception as e:
                logger.debug(f"[HTTP 추출 실패] Playwright로 전환: {url}: {e}")
                result = await extract_with_playwright(
                    url,
                    collect_metadata=collect_metadata,
                    crawl_mode=crawl_mode,
                    stop_signal=stop_signal,
                    block_tag=block_tag
                )

        # ✅ 메타데이터는 청크 저장 시 포함되므로 별도 저장 불필요
        if enable_change_detection and table_name and dbname and result and result.get('_metadata'):
            try:
                logger.info(f"[메타데이터 준비 완료] URL: {url}, 청크 저장 시 포함됩니다")
                
                # 결과에서 메타데이터 제거하지 않음 (청크 처리에서 사용)
                # del result['_metadata']  # 주석 처리
                
            except Exception as e:
                logger.warning(f"[메타데이터 처리 실패] URL: {url}, 오류: {e}")

        return result

    except Exception as e:
        logger.error(f"[콘텐츠 추출 전체 실패] URL: {url}: {e}")
        return None




def is_downloadable_file(url: str) -> bool:
    """URL이 다운로드 가능한 파일(미디어, 문서, 실행파일 등)인지 확인"""
    
    # 다운로드 가능한 파일 확장자 패턴
    downloadable_extensions = [
        # 이미지 파일
        r'\.(jpg|jpeg|png|gif|bmp|tiff|tif|webp|svg|ico|raw|cr2|nef|arw)$',
        # 비디오 파일
        r'\.(mp4|avi|mov|wmv|flv|webm|mkv|m4v|3gp|ogv|mts|m2ts|ts|vob|asf|rm|rmvb)$',
        # 오디오 파일
        r'\.(mp3|wav|flac|aac|ogg|wma|m4a|opus|aiff|au|mid|midi)$',
        # 문서 파일
        r'\.(pdf|hwp|hwpx|doc|docx|ppt|pptx|xls|xlsx|txt|rtf|csv|odt|ods|odp)$',
        # 디자인/그래픽 파일
        r'\.(psd|ai|eps|indd|sketch|fig|xd|cdr|dwg|dxf|blend|max|3ds|obj|fbx|dae)$',
        # 압축 파일
        r'\.(zip|rar|7z|tar|gz|bz2|lzma|xz|ace|arj|cab|iso|dmg|pkg)$',
        # 실행 파일
        r'\.(exe|msi|dmg|pkg|deb|rpm|apk|app|bat|cmd|com|scr|vbs|js|jar|war)$',
        # 기타 미디어/플러그인
        r'\.(swf|fla|f4v|f4p|f4a|f4b|shockwave|quicktime|realmedia)$',
        # 데이터베이스 파일
        r'\.(db|sqlite|mdb|accdb|dbf|odb|sql)$',
        # 백업 파일
        r'\.(bak|backup|old|tmp|temp|log|cache)$',
        # 설정 파일
        r'\.(ini|cfg|conf|config|xml|json|yaml|yml|toml)$',
        # 소스 코드 파일 (웹페이지가 아닌 개발 파일만)
        r'\.(c|cpp|h|java|py|js|scss|less|sql|pl|rb|go|rs|swift|kt|ts|jsx|tsx|vue|svelte)$'
    ]
    
    url_lower = url.lower()
    
    # 확장자 패턴 확인
    for pattern in downloadable_extensions:
        if re.search(pattern, url_lower):
            return True
    
    # 다운로드 관련 경로 패턴 확인 (강화)
    download_paths = [
        '/download/', '/downloads/', '/file/download/', '/files/', '/uploads/', '/attachments/',
        '/media/', '/assets/', '/static/'
    ]
    
    # URL 경로에 download 단어가 포함된 경우
    download_keywords = ['download', 'attachment', 'file']
    
    for path in download_paths:
        if path in url_lower:
            return True
            
    # URL에 다운로드 관련 키워드가 포함된 경우 
    for keyword in download_keywords:
        if keyword in url_lower:
            return True
    
    return False

def normalize_url(url: str) -> str:
    """URL 정규화: 스킴 보정, 호스트 소문자, 프래그먼트 제거, 불필요/중복 쿼리 제거.
    - buffer 등 변동성 높은 파라미터 제거
    - 동일 키가 중복되면 첫 번째만 유지
    """
    try:
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
        p = urlparse(url)
        scheme = p.scheme or "https"
        netloc = (p.netloc or "").lower()
        path = p.path or "/"
        # 프래그먼트 제거
        fragment = ""
        # 쿼리 정규화
        removed_keys = {"buffer"}
        seen_keys = set()
        canon_pairs = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            if k in removed_keys:
                continue
            if k in seen_keys:
                continue
            seen_keys.add(k)
            canon_pairs.append((k, v))
        query = urlencode(canon_pairs, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, fragment))
    except Exception:
        return url

def normalize_url_protocol_agnostic(url: str) -> str:
    """프로토콜을 제외한 URL 정규화 (중복 검사용)
    - HTTP/HTTPS 프로토콜 차이 무시
    - 호스트 소문자, 프래그먼트 제거, 불필요/중복 쿼리 제거
    - 트레일링 슬래시 정규화 (예: /page와 /page/를 동일하게 처리)
    - 중복 검사 시 동일한 페이지로 인식
    """
    try:
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
        p = urlparse(url)
        # 프로토콜 제외 - 중복 검사용
        scheme = ""  # 프로토콜 무시
        netloc = (p.netloc or "").lower()
        path = p.path or "/"
        
        # ✅ 트레일링 슬래시 정규화: 루트(/)가 아니면 트레일링 슬래시 제거
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        
        # 프래그먼트 제거
        fragment = ""
        # 쿼리 정규화
        removed_keys = {"buffer"}
        seen_keys = set()
        canon_pairs = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            if k in removed_keys:
                continue
            if k in seen_keys:
                continue
            seen_keys.add(k)
            canon_pairs.append((k, v))
        query = urlencode(canon_pairs, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, fragment))
    except Exception:
        return url

from urllib.parse import urlparse

def normalize_domain(domain: str) -> str:
    """도메인 정규화: www 서브도메인 제거하여 비교
    
    Args:
        domain: 정규화할 도메인 (예: "www.yna.co.kr" 또는 "yna.co.kr")
    
    Returns:
        정규화된 도메인 (예: "yna.co.kr")
    
    Examples:
        normalize_domain("www.yna.co.kr") -> "yna.co.kr"
        normalize_domain("yna.co.kr") -> "yna.co.kr"
        normalize_domain("subdomain.example.com") -> "subdomain.example.com"
    """
    if not domain:
        return domain
    
    domain = domain.lower().strip()
    
    # www.로 시작하는 경우 제거
    if domain.startswith("www."):
        return domain[4:]
    
    return domain

def should_include_url_by_filter(url: str, url_filter: str) -> bool:
    """
    URL 필터 옵션에 따라 URL을 포함할지 결정
    
    Args:
        url: 확인할 URL (str 또는 dict)
        url_filter: Q(쿼리스트링만), P(패스만), B(둘다)
    
    Returns:
        bool: URL을 포함할지 여부
    """
    # dict로 들어오는 경우 url 값만 뽑기
    if isinstance(url, dict):
        url = url.get("url", "")

    # url_filter가 빈 문자열이면 기본 허용
    if not url_filter:
        return True
        
    if url_filter == "B":  # Both - 둘 다 수집
        return True
    
    parsed = urlparse(url)
    has_query = bool(parsed.query)
    if url_filter == "Q":  # Query - 쿼리스트링만 수집
        return has_query
    elif url_filter == "P":  # Path - 패스 기반만 수집
        return not has_query
    else:
        # 알 수 없는 필터 옵션은 기본값으로 처리
        return True
 
def should_exclude_url(url: str) -> bool:
    """
    Backwards-compatible shim for exclusion checks.
    기존 코드(예: edu.classes)가 `should_exclude_url`을 import하여 사용하므로,
    하위 호환을 위해 간단한 래퍼를 제공합니다.
    실제 제외 판정은 is_wiki_excluded_page의 결과를 사용합니다.
    """
    try:
        return is_wiki_excluded_page(url)[0]
    except Exception:
        return False


# TLSAdapter 클래스는 edu.classes에서 import

def _metadata_has_value(value) -> bool:
    return value not in (None, "")


def _put_metadata_if_present(metadata: dict, key: str, value) -> None:
    if not _metadata_has_value(value) or metadata.get(key) not in (None, ""):
        return
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return
    metadata[key] = str(value) if key in {"created_at", "updated_at", "content_created_at", "content_updated_at"} else value


_COMMON_CONTENT_METADATA_KEYS = {
    "source_url",
    "chunk_index",
    "content_length",
    "update_frequency",
    "content_hash",
    "created_at",
    "updated_at",
    "content_created_at",
    "content_updated_at",
    "date_rerank_target",
    "source_category",
    "content_author",
}


def _build_chunk_content_metadata(
    *,
    source_url: str,
    title: str,
    subject: str,
    learn_list_content_type: str,
    learn_list_type: str,
    chunk_index: int,
    content_length: int,
    url_metadata: dict = None,
) -> dict:
    source_category = learn_list_type or ("file" if learn_list_content_type == "file" else "post")
    base = dict(url_metadata or {})
    metadata = {}
    _put_metadata_if_present(metadata, "source_url", source_url)

    metadata["chunk_index"] = chunk_index
    metadata["content_length"] = content_length
    metadata["update_frequency"] = base.get("update_frequency") or "1_day"
    _put_metadata_if_present(metadata, "content_hash", base.get("content_hash"))
    for key in ("content_created_at", "content_updated_at", "created_at", "updated_at"):
        _put_metadata_if_present(metadata, key, base.get(key))
    if learn_list_content_type == "file" and metadata.get("content_created_at") in (None, ""):
        now_value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata["content_created_at"] = now_value
        metadata["created_at"] = now_value
        if metadata.get("content_updated_at") in (None, ""):
            metadata["content_updated_at"] = now_value
        if metadata.get("updated_at") in (None, ""):
            metadata["updated_at"] = now_value
    if metadata.get("content_created_at") not in (None, ""):
        _put_metadata_if_present(metadata, "created_at", metadata.get("content_created_at"))
    if metadata.get("content_updated_at") not in (None, ""):
        _put_metadata_if_present(metadata, "updated_at", metadata.get("content_updated_at"))
    metadata["date_rerank_target"] = True
    metadata["source_category"] = source_category
    for key, value in base.items():
        if learn_list_content_type == "file" and key not in _COMMON_CONTENT_METADATA_KEYS:
            continue
        if key not in metadata and isinstance(value, (str, int, float, bool)):
            _put_metadata_if_present(metadata, key, value)
    return metadata


async def process_chunks_parallel(
    chunks: List[str],
    source_url: str,
    title: str,
    subject: str,
    table_name: str,
    dbname: str,
    job_id: str,
    job_manager: AsyncJobManager,
    memo: str,
    page_snippet: str,
    favicon_url: str,
    job_progress_manager: AsyncJobProgress = None,
    chunk_progress: float = 0.0,
    batch_size: int = 5,
    is_crawling_mode: bool = False,
    url_metadata: dict = None,
    web_title: str = "",
    chat_bot_id: str = None,
    stored_content_type: str = "url",
):
    import time  # 성능 측정을 위한 import
    """청크를 임베딩하고 DB에 저장한다.

    배치 임베딩과 DB 삽입을 수행하며 `use_crawl_stop` 상태를 감지하면 삽입을 중단하고
    이미 저장된 레코드 목록을 반환한다.

    Args:
        chunks: 분할된 텍스트 청크 목록.
        source_url: 원본 URL.
        title: 페이지 제목.
        subject: 학습 주제.
        table_name: 저장 대상 테이블명.
        dbname: 사용할 데이터베이스 이름.
        job_id: 작업 식별자.
        job_manager: 작업 상태 조회 매니저.
        memo: 사용자 메모.
        page_snippet: 요약 스니펫.
        favicon_url: 파비콘 URL.
        job_progress_manager: 진행률 관리 객체.
        chunk_progress: 청크당 진행률 가중치.
        batch_size: 임베딩 배치 크기.
        is_crawling_mode: 크롤링 모드 여부.
        url_metadata: URL 기반 메타데이터.
        web_title: 웹 타이틀 문자열.
        chat_bot_id: 챗봇 ID (Milvus 전용).
        stored_content_type: PG/Milvus 청크 및 연동 시 content_type 컬럼 값 (파일 학습은 file).
    Returns:
        데이터베이스에 저장된 레코드 목록. 중단 시 부분 목록만 포함한다.

    Raises:
        Exception: 임베딩 또는 DB 삽입 과정에서 발생한 예외.
    """

    db_save_trace_log(
        "process_chunks.start db=%s table=%s url=%s chunks=%s crawling_mode=%s stored_content_type=%s job_id=%s",
        dbname,
        table_name,
        str(source_url or "")[:220],
        len(chunks or []),
        bool(is_crawling_mode),
        stored_content_type,
        job_id,
    )

    if not chunks:
        db_save_trace_log(
            "process_chunks.skip no_chunks db=%s table=%s url=%s job_id=%s",
            dbname,
            table_name,
            str(source_url or "")[:220],
            job_id,
            level=logging.WARNING,
        )
        return []
    
    # ✅ 크롤링 모드에서는 기존 subject가 비어있을 때만 추출 제목으로 보정한다.
    # 이미 상위 워크플로우에서 상세 제목(subject)을 넘긴 경우에는 카테고리/브레드크럼 값으로 덮어쓰지 않는다.
    title_guard_locked = bool((url_metadata or {}).get("title_guard_locked"))
    if is_crawling_mode and title and title.strip():
        current_subject = str(subject or "").strip()
        title_s = str(title or "").strip()
        web_title_s = str(web_title or "").strip()
        noise_titles = {"교육·행사", "교육행사", "새소식", "공지사항", "시정소식"}
        title_is_noise = title_s in noise_titles
        subject_changed = False
        if _crawl_title_is_weak(current_subject) and not title_is_noise:
            subject = title_s
            subject_changed = True
        if _crawl_title_is_weak(web_title_s):
            web_title_to_save = subject if not _crawl_title_is_weak(subject) else title_s
        else:
            web_title_to_save = web_title_s  # 크롤링 모드에서는 실제 title 태그를 web_title에 저장
        if title_guard_locked and not title_is_noise:
            subject = title_s
            web_title_to_save = web_title_s or title_s
            subject_changed = current_subject != subject
        logger.info(
            "[CrawlTitleDebug] url=%s before_subject=%r title=%r web_title=%r title_is_noise=%s subject_changed=%s after_subject=%r",
            source_url,
            current_subject,
            title_s,
            web_title_s,
            title_is_noise,
            subject_changed,
            subject,
        )
        logger.info(
            "[TitleDecisionTrace] stage=url_edu_subject_decision url=%s locked=%s before_subject=%r title=%r web_title=%r title_is_noise=%s subject_changed=%s after_subject=%r web_title_to_save=%r",
            source_url,
            title_guard_locked,
            current_subject[:220],
            title_s[:220],
            web_title_s[:220],
            title_is_noise,
            subject_changed,
            str(subject or "")[:220],
            str(web_title_to_save or "")[:220],
        )
    else:
        # 일반 모드: 기존 로직 유지
        web_title_to_save = title  # 일반 모드에서는 추출된 제목을 web_title에 저장

    logger.info(f"[배치 청크 처리 시작] URL: {source_url}, 총 청크: {len(chunks)}")

    # 1단계: 모든 청크 처리 (품질 평가 비활성화)
    quality_filtered_chunks = []
    for i, chunk in enumerate(chunks):
        quality_filtered_chunks.append({
            'chunk': chunk,
            'chunk_idx': i + 1,
            'quality_score': 1.0  # 모든 청크에 최고 품질 점수 부여
        })

    # ✅ 중단 신호 체크 제거: 이미 수집된 URL의 청크 임베딩 및 저장 보장

    # 2단계: 배치 임베딩 처리 (크롤링 모드 최적화)
    current_batch_size = CRAWLING_EMBEDDING_BATCH_SIZE if is_crawling_mode else EMBEDDING_BATCH_SIZE
    logger.info(f"[배치 크기 최적화] 크롤링 모드: {is_crawling_mode}, 배치 크기: {current_batch_size}")
    
    embedding_batches = []
    for i in range(0, len(quality_filtered_chunks), current_batch_size):
        batch = quality_filtered_chunks[i:i + current_batch_size]
        embedding_batches.append(batch)

    all_db_data = []
    # ✅ stop_requested 변수 제거: 수집된 데이터는 끝까지 처리

    for batch_idx, chunk_batch in enumerate(embedding_batches):
        # ✅ 중단 신호 체크 제거: 이미 수집된 URL의 임베딩 배치 처리 완료 보장

        batch_start_time = time.time()

        try:
            # 배치 임베딩 생성
            text_prep_start = time.time()
            batch_texts = []
            for item in chunk_batch:
                # 메타데이터 포맷 통일 (# url_edu.py 기준)
                chunk_with_metadata = (
                    f"[Source: {source_url}]\n"
                    f"[Chunk_number: {item['chunk_idx']}]\n"
                    f"[User_memo: {memo}]\n"
                    f"[Title: {title}]\n{item['chunk']}"
                )
                batch_texts.append(chunk_with_metadata)
            
            text_prep_time = time.time() - text_prep_start

            # 배치 임베딩 실행 (시간 측정)
            embedding_start_time = time.time()
            embeddings = await batch_embeddings(batch_texts)
            embedding_time = time.time() - embedding_start_time

            # DB 저장용 데이터 준비
            for i, (item, embedding) in enumerate(zip(chunk_batch, embeddings)):
                if embedding is not None:
                    # 메타데이터 포맷 통일 (# url_edu.py 기준) - 직접 재생성하여 할당
                    chunk_with_metadata = (
                        f"[Source: {source_url}]\n"
                        f"[Chunk_number: {item['chunk_idx']}]\n"
                        f"[User_memo: {memo}]\n"
                        f"[Title: {title}]\n{item['chunk']}"
                    )

                    chunk_data = {
                        "content": source_url,
                        "chunk_num": str(item['chunk_idx']),
                        "memo": memo,
                        "content_type": stored_content_type,
                        "subject": subject,
                        "text_data": chunk_with_metadata,  # chunk_with_metadata를 직접 할당
                        "embedding": f"[{','.join(map(str, embedding))}]",
                        "web_title": web_title_to_save
                        # "quality_score": item['quality_score']
                    }
                    content_author_value = (url_metadata or {}).get("content_author")
                    if content_author_value not in (None, ""):
                        chunk_data["content_author"] = str(content_author_value)
                    
                    # ✅ 원본 페이지 해시를 모든 청크에 통일 적용 (변경감지 목적)
                    try:
                        if url_metadata:
                            # 원본 페이지의 해시를 모든 청크가 공유 (변경감지용)
                            chunk_text = batch_texts[i]
                            chunk_content_length = len(chunk_text.encode('utf-8'))
                            # original_page_hash = url_metadata.get('content_hash')  # 원본 페이지 해시 사용
                            
                            content_metadata = {
                                "content_length": chunk_content_length,  # 청크별 실제 길이
                                # "content_hash": original_page_hash,      # 원본 페이지 해시 (모든 청크 동일)
                                "update_frequency": url_metadata.get('update_frequency', '1_day'),
                                "chunk_index": item['chunk_idx']  # 청크 인덱스 추가
                            }
                            content_metadata["source_url"] = source_url

                            # JSONB 변환 (안전성 강화)
                            try:
                                import json
                                content_metadata = {
                                    "chunk_index": item['chunk_idx'],
                                    "content_length": chunk_content_length,
                                    "update_frequency": url_metadata.get('update_frequency', '1_day'),
                                }
                                content_metadata["source_url"] = source_url
                                for metadata_key in ("content_created_at", "content_updated_at"):
                                    metadata_value = url_metadata.get(metadata_key)
                                    if metadata_value not in (None, ""):
                                        content_metadata[metadata_key] = str(metadata_value)
                                if content_metadata.get("content_created_at") not in (None, ""):
                                    content_metadata["created_at"] = str(content_metadata["content_created_at"])
                                if content_metadata.get("content_updated_at") not in (None, ""):
                                    content_metadata["updated_at"] = str(content_metadata["content_updated_at"])
                                ordered_content_metadata = {
                                    "chunk_index": content_metadata.get("chunk_index"),
                                    "content_length": content_metadata.get("content_length"),
                                    "update_frequency": content_metadata.get("update_frequency"),
                                }
                                ordered_content_metadata = {
                                    "source_url": content_metadata.get("source_url"),
                                    **ordered_content_metadata,
                                }
                                for metadata_key in ("created_at", "updated_at", "content_created_at", "content_updated_at"):
                                    metadata_value = content_metadata.get(metadata_key)
                                    if metadata_value not in (None, ""):
                                        ordered_content_metadata[metadata_key] = metadata_value
                                ordered_content_metadata["date_rerank_target"] = True
                                ordered_content_metadata["source_category"] = learn_list_type
                                for metadata_key, metadata_value in (url_metadata or {}).items():
                                    if (
                                        metadata_key not in ordered_content_metadata
                                        and (
                                            learn_list_content_type != "file"
                                            or metadata_key in _COMMON_CONTENT_METADATA_KEYS
                                        )
                                        and metadata_value not in (None, "")
                                        and isinstance(metadata_value, (str, int, float, bool))
                                    ):
                                        ordered_content_metadata[metadata_key] = metadata_value
                                content_metadata = ordered_content_metadata
                                chunk_data["content_metadata"] = json.dumps(content_metadata, ensure_ascii=False)
                            except Exception as json_error:
                                logger.warning(f"[JSONB 변환 실패] 청크 {item['chunk_idx']} JSON 변환 오류: {json_error}")
                                chunk_data["content_metadata"] = None
                        else:
                            fallback_metadata = {
                                "chunk_index": item["chunk_idx"],
                                "content_length": len(batch_texts[i].encode("utf-8")),
                                "update_frequency": "1_day",
                                "date_rerank_target": True,
                                "source_category": learn_list_type,
                            }
                            fallback_metadata = {"source_url": source_url, **fallback_metadata}
                            chunk_data["content_metadata"] = json.dumps(
                                fallback_metadata,
                                ensure_ascii=False,
                            )
                    
                    except Exception as metadata_error:
                        logger.error(f"[메타데이터 처리 오류] 청크 {item['chunk_idx']} 메타데이터 처리 실패: {metadata_error}")
                        chunk_data["content_metadata"] = None

                    all_db_data.append(chunk_data)

            # 진행률 업데이트
            if job_progress_manager:
                current_progress = await job_progress_manager.get_job_progress(job_id)
                batch_progress = chunk_progress * len(chunk_batch)
                new_progress = round(min(current_progress + batch_progress, 99.99), 2)
                await job_progress_manager.set_job_progress(job_id, new_progress)

            batch_total_time = time.time() - batch_start_time
            metadata_processing_time = batch_total_time - embedding_time - text_prep_time
            
            # ✅ 배치별 상세 성능 로그
            successful_embeddings = len([e for e in embeddings if e is not None])
            logger.info(f"🚀 [임베딩 배치 {batch_idx + 1}/{len(embedding_batches)}] 완료:")
            logger.info(f"  - 성공: {successful_embeddings}/{len(embeddings)}")
            logger.info(f"  - 텍스트 준비: {text_prep_time:.3f}초")
            logger.info(f"  - 임베딩 API: {embedding_time:.3f}초 ({current_batch_size}개)")
            logger.info(f"  - 메타데이터 처리: {metadata_processing_time:.3f}초")
            logger.info(f"  - 배치 총 시간: {batch_total_time:.3f}초")
            logger.info(f"  - 청크당 평균: {batch_total_time/len(chunk_batch):.3f}초")
            
            # ✅ 중단 신호 체크 제거: 임베딩 배치 완료까지 보장

        except Exception as e:
            if "Direct embedding is disabled" in str(e):
                logger.error(
                    "[EmbeddingSchedulerRequired] direct embedding path invoked while disabled | url=%s job_id=%s",
                    source_url,
                    job_id,
                )
                raise
            logger.error(f"배치 임베딩 처리 중 오류 (배치 {batch_idx + 1}): {e}")
            continue

    # ✅ 수집된 모든 청크 데이터 유지 (중단 시에도 저장)

    inserted_records: List[Dict] = []

    # ✅ Milvus를 사용할 DB 이름 목록
    MILVUS_DB_NAME = ["chatty", "testchatbot1"]
    MILVUS_TARGET_DB = set(MILVUS_DB_NAME)
    MILVUS_ACCOUNT_NAME = dbname  # Milvus 계정명

    # 3단계: DB 삽입
    if all_db_data:  # ✅ stop_requested 체크 제거: 모든 데이터 저장 보장
        # ✅ Milvus Context 활성화 (MILVUS_TARGET_DB에 해당하는 경우)
        milvus_token = None
        if dbname and dbname.strip().lower() in MILVUS_TARGET_DB and chat_bot_id:
            milvus_ctx = MilvusSyncContext(
                enabled=True,
                dbname=dbname,
                chat_bot_id=chat_bot_id,
                account_name=MILVUS_ACCOUNT_NAME,
            )
            milvus_token = activate_milvus_sync_context(milvus_ctx)
            logger.info(f"[Milvus] [Milvus Context 활성화] dbname={dbname}, chat_bot_id={chat_bot_id}, account={MILVUS_ACCOUNT_NAME}")
        
        # ✅ 1단계: PostgreSQL에 먼저 저장 (returning_id 획득을 위해)
        try:
            if is_crawling_mode:
                # ✅ 크롤링 모드: all_db_data를 한 번에 UPSERT
                logger.info(f"[PostgreSQL 크롤링 모드 UPSERT 시작] URL: {source_url}, 데이터 수: {len(all_db_data)}")
                inserted_records_list = await fallback_individual_upsert(
                    table_name,
                    all_db_data,
                    dbname,
                    job_manager=job_manager,
                    job_id=job_id,
                )
                inserted_count = len(inserted_records_list)
                if inserted_count == 0:
                    logger.warning(f"[PostgreSQL 저장 실패] 크롤링 모드 UPSERT 결과가 0개입니다: {source_url}")
                    inserted_records = []
                else:
                    # returning_id 목록 추출
                    returning_ids = [record.get('returning_id') for record in inserted_records_list if record.get('returning_id') is not None]
                    logger.info(f"[PostgreSQL 크롤링 모드 UPSERT 완료] URL: {source_url}, 처리된 청크: {inserted_count}, RETURNING_ID: {returning_ids}")
                    inserted_records = inserted_records_list
            else:
                # 일반 모드에서는 기존 벌크 삽입 사용
                logger.info(f"[PostgreSQL 일반 모드 벌크 삽입] URL: {source_url}, 데이터 수: {len(all_db_data)}")
                inserted_records_list = await bulk_insert_data(
                    table_name,
                    all_db_data,
                    dbname,
                    job_manager=job_manager,
                    job_id=job_id,
                )
                inserted_count = len(inserted_records_list)
                if inserted_count == 0:
                    logger.warning(f"[PostgreSQL 저장 실패] 일반 모드 벌크 삽입 결과가 0개입니다: {source_url}")
                    inserted_records = []
                else:
                    logger.info(f"[PostgreSQL 일반 모드 벌크 삽입 완료] URL: {source_url}, 성공적으로 저장된 청크: {inserted_count}")
                    inserted_records = inserted_records_list
        
        except Exception as e:
            logger.error(f"[PostgreSQL 삽입 실패] {e}")
            # 폴백: 개별 삽입/upsert
            if is_crawling_mode:
                inserted_records = await fallback_individual_upsert(
                    table_name,
                    all_db_data,
                    dbname,
                    job_manager=job_manager,
                    job_id=job_id,
                )
            else:
                inserted_records = await fallback_individual_insert(
                    table_name,
                    all_db_data,
                    dbname,
                    job_manager=job_manager,
                    job_id=job_id,
                )
        
        # ✅ 2단계: Milvus 동기화 (Context가 활성화된 경우 자동 처리)
        if milvus_token and inserted_records:
            try:
                # ✅ Milvus 전용 데이터 변환: text_data 제외, 간소화된 metadata만 포함
                milvus_rows = []
                for record in inserted_records:
                    # ✅ returning_id, embedding, 간소화된 metadata만 포함
                    milvus_record = {
                        "returning_id": record.get("returning_id"),
                        "id": record.get("returning_id"),  # 호환성을 위해 id도 포함
                        "embedding": record.get("embedding"),
                        "embedded_data": record.get("embedding"),  # 호환성을 위해 embedded_data도 포함
                        "content": record.get("content"),  # source_url (content_name으로 사용됨)
                        "content_type": record.get("content_type", stored_content_type),
                    }
                    
                    # ✅ 간소화된 metadata 구성 (url, chunk, size 등 기본 정보만)
                    simplified_metadata = {}
                    if record.get("content"):
                        simplified_metadata["url"] = record.get("content")  # source_url
                    if record.get("chunk_num"):
                        simplified_metadata["chunk"] = record.get("chunk_num")
                    if record.get("text_data"):
                        # text_data의 크기만 포함 (본문 내용은 제외)
                        text_data_str = str(record.get("text_data"))
                        simplified_metadata["size"] = len(text_data_str.encode('utf-8'))  # bytes
                    
                    # ✅ content_metadata가 있으면 일부 필드만 추출
                    if record.get("content_metadata"):
                        try:
                            if isinstance(record.get("content_metadata"), str):
                                content_meta = json.loads(record.get("content_metadata"))
                            else:
                                content_meta = record.get("content_metadata")
                            
                            # 필요한 기본 필드만 추출
                            if isinstance(content_meta, dict):
                                if "content_length" in content_meta:
                                    simplified_metadata["content_length"] = content_meta["content_length"]
                                if "chunk_index" in content_meta:
                                    simplified_metadata["chunk_index"] = content_meta["chunk_index"]
                        except Exception as meta_error:
                            logger.debug(f"[Milvus] [메타데이터 파싱 실패] {meta_error}")
                    
                    # ✅ 간소화된 metadata를 content_metadata로 저장
                    milvus_record["content_metadata"] = json.dumps(simplified_metadata, ensure_ascii=False) if simplified_metadata else None
                    
                    milvus_rows.append(milvus_record)
                
                logger.info(
                    f"[Milvus] [Milvus 전용 데이터 변환 완료] URL: {source_url}, "
                    f"원본 레코드: {len(inserted_records)}개, Milvus 레코드: {len(milvus_rows)}개, "
                    f"text_data 제외됨"
                )
                
                milvus_ctx = MilvusSyncContext(
                    enabled=True,
                    dbname=dbname,
                    chat_bot_id=chat_bot_id,
                    account_name=MILVUS_ACCOUNT_NAME,
                )
                logger.info(f"[Milvus] [Milvus 동기화 시작] URL: {source_url}, PostgreSQL 저장 완료 후 Milvus 동기화")
                await sync_rows_to_milvus(rows=milvus_rows, context=milvus_ctx)
                # ✅ sync_rows_to_milvus 내부에서 실패 시 경고 로깅하므로 여기서는 성공으로 간주
                logger.info(f"[Milvus] [Milvus 동기화 처리 완료] URL: {source_url}, 청크 수: {len(milvus_rows)} (실제 삽입 여부는 위 로그 확인)")
            except Exception as milvus_error:
                logger.error(f"[Milvus] [Milvus 동기화 오류] URL: {source_url}, 오류: {milvus_error} - PostgreSQL에는 저장됨")
            finally:
                # Context 정리
                reset_milvus_sync_context(milvus_token)
                logger.debug(f"[Milvus] [Milvus Context 해제] dbname={dbname}")
    else:
        logger.warning(f"[DB 삽입 스킵] URL: {source_url} - 저장할 데이터 없음")
        inserted_records = []

    return inserted_records

async def fallback_individual_upsert(
    table_name: str,
    data_list: List[Dict],
    dbname: str,
    job_manager: Optional[AsyncJobManager] = None,
    job_id: str = ""
) -> List[Dict]:
    """UPSERT 폴백 경로에서 레코드를 배치로 저장한다.

    작업 상태가 `use_crawl_stop`이면 즉시 중단하고 이미 저장된 레코드만 반환한다.

    Args:
        table_name: 대상 테이블명.
        data_list: UPSERT 대상 데이터 목록.
        dbname: 사용할 데이터베이스 이름.
        job_manager: 작업 상태를 조회할 매니저. 없으면 상태 확인을 생략한다.
        job_id: 상태 조회에 사용할 작업 ID.

    Returns:
        UPSERT에 성공한 레코드 목록 (각 딕셔너리에 'returning_id' 포함).

    Raises:
        Exception: UPSERT 수행 중 발생한 예외.
    """

    import time

    total_start_time = time.time()
    db_save_trace_log(
        "pg.batch_upsert.start db=%s table=%s rows=%s job_id=%s first_content=%s",
        dbname,
        table_name,
        len(data_list or []),
        job_id,
        str((data_list[0] or {}).get("content") if data_list else "")[:220],
    )
    # logger.info(f"[배치 UPSERT 시작] {len(data_list)}개 레코드 배치 처리 시작")

    if not data_list:
        logger.warning("[배치 UPSERT] 처리할 데이터가 없습니다.")
        return []

    inserted_records: List[Dict] = []

    success_count = 0
    batch_size = DB_SAVE_BATCH_SIZE  # 하이브리드 스트리밍 배치 크기 사용
    total_batches = (len(data_list) + batch_size - 1) // batch_size

    # logger.info(f"[배치 UPSERT 전략] {len(data_list)}개를 {batch_size}개씩 {total_batches}개 배치로 처리")

    db_semaphore = asyncio.Semaphore(15)

    async def upsert_with_semaphore(data: Dict) -> Any:
        async with db_semaphore:
            return await upsert_data_by_content_and_chunk(
                table_name=table_name,
                data=data,
                dbname=dbname,
            )

    for batch_idx in range(total_batches):
        # ✅ 중단 신호 체크 제거: 이미 수집된 데이터는 모두 저장 보장
        
        batch_start_time = time.time()
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(data_list))
        current_batch = data_list[start_idx:end_idx]

        # logger.info(
        #     f"[배치 {batch_idx + 1}/{total_batches}] {len(current_batch)}개 레코드 병렬 처리 중..."
        # )

        batch_tasks = [upsert_with_semaphore(data) for data in current_batch]

        gather_start_time = time.time()
        try:
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            gather_time = time.time() - gather_start_time

            batch_success = 0
            batch_insert_count = 0  # 실제 INSERT된 레코드 수
            for index, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"[배치 {batch_idx + 1}] 레코드 {index + 1} 실패: {result}"
                    )
                else:
                    batch_success += 1
                    # result가 dict인 경우 (is_insert 포함) 또는 기존 형식 (returning_id만)
                    if isinstance(result, dict):
                        is_insert = result.get('is_insert', False)
                        returning_id = result.get('returning_id')
                    else:
                        # 하위 호환성: 기존 형식 (returning_id만 반환)
                        is_insert = True  # 기존 코드는 INSERT만 했으므로 True로 가정
                        returning_id = result
                    
                    # INSERT 또는 UPDATE 모두 카운트 (변경된 데이터는 모두 카운트)
                    success_count += 1
                    
                    # 실제 INSERT된 경우에만 batch_insert_count 증가
                    if is_insert:
                        batch_insert_count += 1
                    
                    # returning_id를 포함한 레코드 저장
                    record_with_id = current_batch[index].copy()
                    record_with_id['returning_id'] = returning_id
                    record_with_id['is_insert'] = is_insert
                    inserted_records.append(record_with_id)
                    
                    if not is_insert:
                        logger.debug(f"[배치 {batch_idx + 1}] 레코드 {index + 1} UPDATE (카운트 포함)")

            batch_total_time = time.time() - batch_start_time
            avg_per_record = batch_total_time / len(current_batch) if current_batch else 0

            batch_update_count = batch_success - batch_insert_count
            # logger.info(f"[배치 {batch_idx + 1}] ✅ 성공: {batch_success}/{len(current_batch)} (INSERT: {batch_insert_count}개, UPDATE: {batch_update_count}개, 모두 카운트 포함)")
            # logger.info(f"  - 병렬 실행: {gather_time:.3f}초")
            # logger.info(f"  - 배치 총 시간: {batch_total_time:.3f}초")
            # logger.info(f"  - 레코드당 평균: {avg_per_record:.3f}초")

        except Exception as batch_error:
            logger.error(f"[배치 {batch_idx + 1}] 배치 처리 실패: {batch_error}")
            fallback_start_time = time.time()
            for index, data in enumerate(current_batch):
                if job_manager and job_id:
                    try:
                        status = await job_manager.get_job_status(job_id)
                        if status == "use_crawl_stop":
                            logger.info(
                                "[배치 UPSERT 폴백 중단] use_crawl_stop 감지로 추가 UPSERT를 중단합니다."
                            )
                            break
                    except Exception as status_error:
                        logger.warning(f"[폴백 상태 확인 실패] {status_error}")

                try:
                    result = await upsert_with_semaphore(data)
                    # result가 dict인 경우 (is_insert 포함) 또는 기존 형식 (returning_id만)
                    if isinstance(result, dict):
                        is_insert = result.get('is_insert', False)
                        returning_id = result.get('returning_id')
                    else:
                        # 하위 호환성: 기존 형식
                        is_insert = True
                        returning_id = result
                    
                    # INSERT 또는 UPDATE 모두 카운트 (변경된 데이터는 모두 카운트)
                    success_count += 1
                    # returning_id를 포함한 레코드 저장
                    record_with_id = data.copy()
                    record_with_id['returning_id'] = returning_id
                    record_with_id['is_insert'] = is_insert
                    inserted_records.append(record_with_id)
                    
                    if not is_insert:
                        logger.debug(f"[배치 {batch_idx + 1}] 개별 레코드 {index + 1} UPDATE (카운트 포함)")
                except Exception as individual_error:
                    logger.error(
                        f"[배치 {batch_idx + 1}] 개별 레코드 {index + 1} 실패: {individual_error}"
                    )
                    continue

            fallback_time = time.time() - fallback_start_time
            logger.info(f"[배치 {batch_idx + 1}] 폴백 처리 시간: {fallback_time:.3f}초")

        if job_manager and job_id:
            try:
                status = await job_manager.get_job_status(job_id)
                if status == "use_crawl_stop":
                    logger.info(
                        "[배치 UPSERT 종료] use_crawl_stop 감지로 루프를 종료합니다."
                    )
                    break
            except Exception as status_error:
                logger.warning(f"[배치 종료 상태 확인 실패] {status_error}")

    total_elapsed_time = time.time() - total_start_time
    avg_per_record_total = total_elapsed_time / len(inserted_records) if inserted_records else 0
    records_per_second = (
        len(inserted_records) / total_elapsed_time if total_elapsed_time > 0 else 0
    )

    # INSERT와 UPDATE 모두 카운트에 포함됨
    insert_count = sum(1 for r in inserted_records if r.get('is_insert', True))
    update_count = len(inserted_records) - insert_count
    db_save_trace_log(
        "pg.batch_upsert.done db=%s table=%s job_id=%s processed=%s/%s insert=%s update=%s",
        dbname,
        table_name,
        job_id,
        len(inserted_records),
        len(data_list or []),
        insert_count,
        update_count,
    )
    # logger.info(f"[배치 UPSERT 종료] 총 처리: {len(inserted_records)}/{len(data_list)} (INSERT: {insert_count}개, UPDATE: {update_count}개, 모두 카운트 포함)")
    # logger.info(f"  - 총 처리 시간: {total_elapsed_time:.3f}초")
    # logger.info(f"  - INSERT 레코드당 평균: {avg_per_record_total:.3f}초")
    # logger.info(f"  - 초당 처리 레코드: {records_per_second:.3f}건")

    return inserted_records

async def fallback_individual_insert(
    table_name: str,
    data_list: List[Dict],
    dbname: str,
    job_manager: Optional[AsyncJobManager] = None,
    job_id: str = ""
) -> List[Dict]:
    """벌크 실패 시 레코드를 순차 삽입한다.

    `use_crawl_stop` 상태가 확인되면 남은 데이터는 건너뛰고 현재까지 저장된
    레코드만 반환한다.

    Args:
        table_name: 삽입 대상 테이블명.
        data_list: 저장할 레코드 목록.
        dbname: 사용할 데이터베이스 이름.
        job_manager: 작업 상태 조회 매니저. 없으면 상태 확인을 하지 않는다.
        job_id: 상태 확인에 사용할 작업 ID.

    Returns:
        정상적으로 삽입된 레코드 목록.

    Raises:
        Exception: 삽입 과정 중 발생한 예외를 그대로 전파한다.
    """

    logger.info(f"[폴백 개별 삽입] {len(data_list)}개 레코드 개별 저장 시작")

    inserted_records: List[Dict] = []

    for i, data in enumerate(data_list):
        # ✅ 중단 신호 체크 제거: 이미 수집된 데이터는 모두 저장 보장

        try:
            from db.db_operations import insert_data
            await insert_data(table=table_name, data=data, dbname=dbname)
            inserted_records.append(data)
        except Exception as e:
            logger.error(f"[개별 삽입 실패] {i+1}/{len(data_list)}: {e}")
            continue

    logger.info(f"[폴백 개별 삽입 종료] 총 {len(inserted_records)}/{len(data_list)}개 저장")
    return inserted_records

_pending_contents_cache_by_job_id: Dict[str, set] = {}
_pending_contents_cache_lock_by_job_id: Dict[str, asyncio.Lock] = {}

async def update_redis_job_data(redis_client, job_id: str, source_url: str):
    """
    Redis job_data를 비동기로 업데이트.
    - 매 URL마다 job_data를 GET+JSON 파싱하면 Redis/CPU 부하가 커질 수 있어,
      job_id 단위로 pending_contents 목록을 캐시해 GET/파싱 횟수를 줄입니다.
    """
    if not job_id or not source_url:
        return

    # job_id별 캐시 락(동시에 여러 URL 처리 시 중복 GET 방지)
    lock = _pending_contents_cache_lock_by_job_id.get(job_id)
    if lock is None:
        lock = asyncio.Lock()
        _pending_contents_cache_lock_by_job_id[job_id] = lock

    async with lock:
        cached = _pending_contents_cache_by_job_id.get(job_id)
        if cached is None:
            cached = set()
            try:
                job_data_json = await redis_client.get(f"job_data:{job_id}")
                if job_data_json:
                    job_data = json.loads(job_data_json)
                    pending = job_data.get("pending_contents")
                    if isinstance(pending, list):
                        cached = {str(x) for x in pending}
            except Exception:
                # Redis 장애가 있어도 크롤링/완료 흐름을 끊지 않음
                cached = set()
            _pending_contents_cache_by_job_id[job_id] = cached

        # 이미 포함되어 있으면 Redis write를 하지 않습니다.
        if str(source_url) in cached:
            return

        # pending_contents에 새 URL이 들어가야 하는 경우에만 Redis에서 job_data를 재로딩 후 갱신
        try:
            job_data_json = await redis_client.get(f"job_data:{job_id}")
            if not job_data_json:
                cached.add(str(source_url))
                return
            job_data = json.loads(job_data_json)

            pending = job_data.get("pending_contents")
            if not isinstance(pending, list):
                pending = []

            s_url = str(source_url)
            if s_url not in pending:
                pending.append(s_url)
                job_data["pending_contents"] = pending
                await redis_client.set(f"job_data:{job_id}", json.dumps(job_data))
                logger.debug(f"[Redis 업데이트] job_id: {job_id}, URL 추가: {source_url}")

            cached.add(s_url)
            _pending_contents_cache_by_job_id[job_id] = cached
        except Exception as e:
            logger.error(f"Redis 업데이트 실패: {e}")

async def process_single_crawled_url(
    semaphore: asyncio.Semaphore,
    result: Dict[str, Any],
    subject: str,
    context: CrawlingContext,
    memo: str,
    redis_client,
    start_time: float,
    url_index: int,
    total_urls: int,
    page_progress: float,
    stop_signal: CrawlStopSignal = None,
) -> Dict[str, Any]:
    """개별 크롤된 URL을 병렬로 처리"""
    # Context에서 값 추출
    table_name = context.table_name
    dbname = context.dbname
    job_id = context.job_id
    job_manager = context.job_manager
    job_progress_manager = context.job_progress
    chat_bot_id = context.chat_bot_id

    async with semaphore:  # 동시 실행 개수 제한
        try:
            source_url = str(result.get("source_url") or result.get("source") or "").strip()
            if not source_url:
                raise ValueError("source_url is required")
            file_info = result.get("file_info") if isinstance(result.get("file_info"), dict) else {}
            original_meta = file_info.get("original_meta") if isinstance(file_info.get("original_meta"), dict) else {}
            title = result.get("title") or result.get("subject", "") or ""
            web_title = result.get("web_title", "")
            title_guard_locked = bool(result.get("title_guard_locked"))
            content = result["content"]
            page_snippet = result.get("snippet", "")
            favicon_url = result.get("favicon_url", "")
            headers = result.get("headers", {})
            # 파일 첨부·파일 크롤 학습은 PG/MariaDB LEARN_LIST 모두 content_type=file (페이지 URL 학습은 url)
            learn_list_content_type = (
                "file"
                if str(result.get("content_type") or "").strip().lower() == "file"
                else "url"
            )
            learn_list_type = "file" if learn_list_content_type == "file" else "post"

            logger.info(f"[URL 처리 시작] {url_index}/{total_urls} - {source_url}")
            
            # ✅ 중단 신호 체크 제거: 이미 수집된 URL은 끝까지 저장 보장

            # 🔍 배치 변경감지가 이미 완료되어 이 함수에 도달한 경우는 변경/신규 URL만 처리
            # (개별 변경감지 로직 제거 - 배치 처리로 대체됨)
            logger.info(f"✅ [배치 변경감지 통과] {source_url} - 변경/신규 URL로 처리 진행")

            # ✅ 사이트맵에서 온 URL의 경우 실제 콘텐츠 추출 (변경감지 비활성화)
            if not content or content.strip() == "":
                logger.info(f"[콘텐츠 추출] 사이트맵 URL의 실제 콘텐츠 추출: {source_url}")

                try:
                    # 스마트 하이브리드 방식으로 콘텐츠 추출 (변경감지 비활성화 - 이미 배치에서 처리됨)
                    content_result = await extract_content_from_url(
                        source_url,
                        table_name,
                        dbname,
                        enable_change_detection=False,
                        crawl_mode="crawling",
                        stop_signal=stop_signal,
                        chat_bot_id=chat_bot_id
                    )
                    
                    if content_result:
                        # 정상적인 콘텐츠 추출 결과 처리
                        title = content_result.get("title") or content_result.get("subject") or title
                        content = content_result.get("content", "")
                        page_snippet = content_result.get("snippet", "")
                        favicon_url = content_result.get("favicon_url", "")
                        chunk_hash = content_result.get("chunk_hash", [])

                        logger.info(f"[콘텐츠 추출 성공] URL: {source_url}")
                    else:
                        logger.warning(f"[콘텐츠 추출 실패] URL: {source_url} - 스킵")
                        return None

                except Exception as e:
                    logger.error(f"[콘텐츠 추출 오류] URL: {source_url}: {e}")
                    return None

            # ✅ 중단 신호 체크 제거: 이미 수집된 URL의 청크 처리 보장

            # ✅ 청킹 전 메타데이터 사전 계산 (변경감지용)
            url_metadata = {}
            main_content = content  # 기본값: 원본 content
            
            try:
                # HTTP 헤더에서 메타데이터 추출
                # content_length가 없는 경우 실제 콘텐츠 길이 계산
                content_length = headers.get('content_length')
                if not content_length:
                    try:
                        content_length = len(content.encode('utf-8'))
                        logger.debug(f"[메타데이터] Content-Length 헤더 없음, 실제 길이 계산: {content_length}")
                    except Exception as e:
                        logger.warning(f"[메타데이터] Content-Length 계산 실패: {e}")
                        content_length = None
                
                url_metadata = {
                    'content_length': content_length,
                    'update_frequency': '1_day'  # 기본값
                }
                content_author = (
                    result.get('content_author')
                    or result.get('author')
                    or result.get('department')
                    or file_info.get('content_author')
                    or file_info.get('author')
                    or original_meta.get('content_author')
                    or original_meta.get('author')
                    or file_info.get('department')
                    or original_meta.get('department')
                )
                if content_author not in (None, ""):
                    url_metadata['content_author'] = str(content_author)

                content_created_at = (
                    result.get('content_created_at')
                    or result.get('reg_date')
                    or result.get('created_at')
                )
                content_updated_at = (
                    result.get('content_updated_at')
                    or result.get('updated_at')
                    or content_created_at
                )
                if content_created_at not in (None, ""):
                    url_metadata['content_created_at'] = str(content_created_at)
                if content_updated_at not in (None, ""):
                    url_metadata['content_updated_at'] = str(content_updated_at)
                if title_guard_locked:
                    url_metadata['title_guard_locked'] = True
                    if isinstance(result.get("title_decision"), dict):
                        url_metadata['title_decision'] = result.get("title_decision")

                main_content = re.sub(r'\s+', ' ', content).strip()
                content_hash = ""
                
                logger.info(f"🔧 [메타데이터 사전 계산] URL: {source_url}")
                logger.info(f"   - Content-Length: {url_metadata.get('content_length', 'None')}")
                
            except Exception as e:
                logger.warning(f"[메타데이터 사전 계산 실패] URL: {source_url}, 오류: {e}")
                main_content = re.sub(r'\s+', ' ', content).strip()
                content_hash = ""

            # 1. 텍스트 분할
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=Config.BASIC_CHUNK_SIZE,
                chunk_overlap=Config.BASIC_CHUNK_OVERLAP,
            )

            all_text = f"Title: {title}\n\nContent: {main_content}"
            try:
                from edu.semantic_chunking import split_text_semantically_if_markdown

                chunks = split_text_semantically_if_markdown(
                    all_text,
                    chunk_size=Config.BASIC_CHUNK_SIZE,
                    chunk_overlap=Config.BASIC_CHUNK_OVERLAP,
                )
            except Exception:
                chunks = text_splitter.split_text(all_text)
            
            total_chunks = len(chunks)
            content_bytes = len(content.encode("utf-8"))
            main_content_bytes = len(main_content.encode("utf-8"))
            all_text_bytes = len(all_text.encode("utf-8"))
            logger.info(f"[청크 생성] URL: {source_url}, 청크 수: {total_chunks}")

            duplicate_skip_result = await _maybe_skip_existing_url_learning(
                source_url=source_url,
                title=title,
                result=result,
                context=context,
                dbname=dbname,
                chat_bot_id=chat_bot_id,
                learn_list_content_type=learn_list_content_type,
                learn_list_type=learn_list_type,
                total_chunks=total_chunks,
                content_hash=content_hash,
                favicon_url=favicon_url,
                content_bytes=content_bytes,
                start_time=start_time,
                url_index=url_index,
            )
            if duplicate_skip_result:
                return duplicate_skip_result

            # 2. Redis 업데이트 (비동기)
            try:
                await update_redis_job_data(redis_client, job_id, source_url)
            except Exception as e:
                logger.error(f"[Redis 업데이트 실패] job_id: {job_id}, 오류: {str(e)}")

            # 3. ✅ 청크별 진행률 계산 및 병렬 처리
            chunk_progress = round(page_progress / total_chunks, 4) if total_chunks > 0 else 0

            inserted_records = await process_chunks_parallel(
                chunks=chunks,
                source_url=source_url,
                title=title,
                subject=subject,
                table_name=table_name,
                dbname=dbname,
                job_id=job_id,
                job_manager=job_manager,
                memo=memo,
                page_snippet=page_snippet,
                favicon_url=favicon_url,
                job_progress_manager=job_progress_manager,
                chunk_progress=chunk_progress,
                batch_size=EMBEDDING_BATCH_SIZE,
                is_crawling_mode=True,
                url_metadata=url_metadata,  # 메타데이터 전달
                web_title=web_title,  # 크롤링 모드에서 추출된 실제 title 태그
                chat_bot_id=chat_bot_id,  # Milvus 전용
                stored_content_type=learn_list_content_type,
            )
            chunks = []
            all_text = ""
            content = ""
            main_content = ""

            # ✅ 중단 신호 체크 제거: 이미 저장된 데이터 유지 (삭제 방지)

            # ✅ inserted_records가 있을 때만 후속 처리 (MariaDB 저장, 소켓 업데이트 등)
            learn_list_inserted = None
            learn_list_action = ""
            learn_list_row_id = None
            if inserted_records:
                logger.info(
                    f"[DB 삽입 결과] URL: {source_url}, 저장된 청크 수: {len(inserted_records)}"
                )

                # ✅ MariaDB에 URL당 1개의 레코드 저장
                sub_change_mode_on = False
                try:
                    if chat_bot_id:
                        _cfg_map = await _fetch_crawling_config_map(dbname, chat_bot_id)
                        sub_change_mode_on = str(_cfg_map.get("sub_change", "off")).lower() == "on"
                except Exception as _e:
                    logger.warning(f"[sub_change 조회 실패] 기본 off 적용: {_e}")
            
            # 파일·첨부 학습: LEARN_LIST는 insert_into_learn_list에서 content=첨부 URL로
            # 이미 1행 존재. 여기서 content=source_url(원본 다운로드 URL)로 UPDATE/INSERT 하면 동일 파일에 2행이 생김 → 생략.
            if inserted_records and chat_bot_id and learn_list_content_type != "file":
                try:
                    # DB 선택 (MySQL 또는 MariaDB)
                    _name_lc = (dbname or '').strip().lower()
                    if _name_lc in ('chatty', 'naraone'):
                        from db.mysql_db_config import mysql_execute_query as _exec_query
                        _db_tag = 'MySQL'
                    else:
                        from db.maria_operations import maria_execute_query as _exec_query
                        _db_tag = 'MariaDB'

                    table_name_db = f"ASADAL_{chat_bot_id[-12:]}_LEARN_LIST"

                    # 게시판 크롤링 등: result의 author/department → LEARN_LIST 작성자 컬럼
                    cols_m = set()
                    author_col = None
                    author_val = None
                    try:
                        from db.mariadb_save_update import (
                            ensure_learn_list_standard_columns,
                            _coalesce_author_fields,
                            _pick_first_existing_column,
                        )

                        cols_m = await ensure_learn_list_standard_columns(dbname, table_name_db)
                        author_col = _pick_first_existing_column(
                            cols_m, ("content_author", "content_au", "content_auth", "author")
                        )
                        author_val = _coalesce_author_fields(
                            {
                                "author": result.get("author"),
                                "content_author": result.get("content_author"),
                                "writer": result.get("writer"),
                                "department": result.get("department"),
                            }
                        )
                    except Exception:
                        pass
                    use_author = bool(
                        author_col and author_val and str(author_val).strip()
                    )
                    use_type_col = bool("type" in cols_m)

                    # 1) 해당 URL의 subject 존재 여부 확인
                    existing_any_query = f"""
                        SELECT id, subject, status FROM {table_name_db}
                        WHERE content = %s
                        ORDER BY id DESC
                        LIMIT 1
                    """
                    existing_any_record = await _exec_query(
                        existing_any_query,
                        [source_url],
                        fetch=True,
                        dbname=dbname,
                    )
                    existing_any_row = existing_any_record[0] if existing_any_record else None
                    existing_any_status = ""
                    existing_any_id = None
                    if isinstance(existing_any_row, dict):
                        existing_any_status = str(existing_any_row.get("status") or "").strip().upper()
                        existing_any_id = existing_any_row.get("id")
                    if existing_any_status == "N" and existing_any_id is not None:
                        await _exec_query(
                            f"DELETE FROM {table_name_db} WHERE id = %s",
                            [existing_any_id],
                            fetch=False,
                            dbname=dbname,
                        )
                        logger.info(
                            f"[{_db_tag}] pending duplicate LEARN_LIST row removed before fresh insert | id={existing_any_id} url={source_url}"
                        )
                        existing_any_record = []
                    had_existing_learn_list_row = bool(existing_any_record and len(existing_any_record) > 0)
                    learn_list_inserted = not had_existing_learn_list_row
                    learn_list_action = "insert" if learn_list_inserted else "update_existing"

                    check_query = f"""
                        SELECT subject FROM {table_name_db}
                        WHERE content = %s AND subject IS NOT NULL AND subject != ''
                        LIMIT 1
                    """
                    existing_record = await _exec_query(check_query, [source_url], fetch=True, dbname=dbname)
                    logger.info(f"[{_db_tag} subject 확인] URL: {source_url}, existing_record: {existing_record}, type: {type(existing_record)}")

                    def _learn_list_existing_subject(rec) -> str:
                        try:
                            if not rec:
                                return ""
                            r0 = rec[0]
                            if isinstance(r0, dict):
                                return str(r0.get("subject") or "").strip()
                            return str(r0 or "").strip()
                        except Exception:
                            return ""

                    def _subject_is_url_or_content_placeholder(subj: str, content_url: str) -> bool:
                        """subject 컬럼에 URL만 넣였을 때(제목 미추출) 재수집 시 갱신해야 함."""
                        s = (subj or "").strip()
                        u = (content_url or "").strip()
                        if not s or not u:
                            return False
                        if s == u:
                            return True
                        s_low, u_low = s.lower().rstrip("/"), u.lower().rstrip("/")
                        if s_low == u_low:
                            return True
                        if not s_low.startswith("http"):
                            return False
                        try:
                            from urllib.parse import urlparse, unquote

                            def _nh(netloc: str) -> str:
                                return (netloc or "").lower().replace("www.", "", 1)

                            ps, pu = urlparse(s), urlparse(u)
                            if _nh(ps.netloc) != _nh(pu.netloc):
                                return False
                            pth = unquote((ps.path or "").rstrip("/").lower())
                            puth = unquote((pu.path or "").rstrip("/").lower())
                            return pth == puth
                        except Exception:
                            return False

                    def _subject_is_trivial_placeholder(subj: str) -> bool:
                        t = (subj or "").strip().lower()
                        if t in {
                            "이미지",
                            "image",
                            "제목 없음",
                            "untitled",
                            "null",
                            "none",
                        }:
                            return True
                        # 초성/자모만 남은 값(예: 'ㅅㄷㄴㅅ')은 정상 제목이 아니라 placeholder로 본다.
                        compact = re.sub(r"\s+", "", subj or "")
                        if not compact:
                            return True
                        placeholder_chars = set("?-_./|:;()[]{}\\")
                        return all(ch in placeholder_chars for ch in compact)

                    def _subject_differs_from_new_title(subj: str, new_title: str) -> bool:
                        old_norm = re.sub(r"\s+", " ", str(subj or "").strip()).lower()
                        new_norm = re.sub(r"\s+", " ", str(new_title or "").strip()).lower()
                        if not old_norm or not new_norm:
                            return False
                        return old_norm != new_norm

                    existing_sub = _learn_list_existing_subject(existing_record)
                    title_stripped = (title or "").strip()
                    existing_is_url_placeholder = _subject_is_url_or_content_placeholder(existing_sub, source_url)
                    existing_is_trivial_placeholder = _subject_is_trivial_placeholder(existing_sub)
                    existing_differs_from_new_title = _subject_differs_from_new_title(existing_sub, title_stripped)
                    # sub_change=off여도 기존 subject가 URL/무의미하거나 새 제목과 다르면 교체한다.
                    update_subject_col = bool(sub_change_mode_on) or (
                        bool(title_stripped)
                        and (
                            existing_is_url_placeholder
                            or existing_is_trivial_placeholder
                            or existing_differs_from_new_title
                        )
                    )
                    logger.info(
                        f"[{_db_tag}] subject 판정 | "
                        f"existing_sub={existing_sub!r} new_title={title_stripped!r} "
                        f"sub_change_on={sub_change_mode_on} "
                        f"url_placeholder={existing_is_url_placeholder} "
                        f"trivial_placeholder={existing_is_trivial_placeholder} "
                        f"title_diff={existing_differs_from_new_title} "
                        f"update_subject_col={update_subject_col} "
                        f"url={source_url}"
                    )

                    # 공통 준비 값들 (size: 용량은 항상 all_text_bytes = 청킹에 사용된 전체 텍스트 바이트)
                    total_size = all_text_bytes
                    # LEARN_LIST.hash: 저장하지 않음 → UPDATE/INSERT SQL에서 컬럼 생략(NULL, 스키마 nullable 가정)

                    # 2) 기존 subject 있음 + subject 갱신 안 함(sub_change=off & 플레이스홀더 아님) → subject 제외 UPDATE
                    if existing_record and len(existing_record) > 0 and not update_subject_col:
                        logger.info(f"[{_db_tag}] 기존 subject 유지 UPDATE (sub_change=off·플레이스홀더 아님): {source_url}")
                        _auth_set = (
                            f", `{author_col}` = %s" if use_author else ""
                        )
                        _type_set = ", `type` = %s" if use_type_col else ""
                        db_query = f"""
                            UPDATE {table_name_db}
                            SET content_type = %s, status = %s, size = %s, chunk = %s, created_at = %s, cate1 = %s, cate2 = %s{_type_set}{_auth_set}
                            WHERE content = %s
                        """
                        db_params = [
                            learn_list_content_type,
                            'N',
                            total_size,
                            total_chunks,
                            __import__('datetime').datetime.now().isoformat(),
                            context.cate1,
                            context.cate2,
                        ]
                        if use_type_col:
                            db_params.append(learn_list_type)
                        if use_author:
                            db_params.append(author_val)
                        db_params.append(source_url)
                    # 3) 기존 subject 있음 + subject 갱신(sub_change=on 또는 URL/플레이스홀더 보정)
                    elif existing_record and len(existing_record) > 0 and update_subject_col:
                        logger.info(
                            f"[{_db_tag}] subject 갱신 UPDATE | sub_change={sub_change_mode_on} "
                            f"placeholder_fix={not sub_change_mode_on}: {source_url}"
                        )
                        _type_set2 = ", `type` = %s" if use_type_col else ""
                        _auth_set2 = f", `{author_col}` = %s" if use_author else ""
                        db_query = f"""
                            UPDATE {table_name_db}
                            SET subject = %s, content_type = %s, status = %s, size = %s, chunk = %s, created_at = %s, cate1 = %s, cate2 = %s{_type_set2}{_auth_set2}
                            WHERE content = %s
                        """
                        db_params = [
                            title,
                            learn_list_content_type,
                            'N',
                            total_size,
                            total_chunks,
                            __import__('datetime').datetime.now().isoformat(),
                            context.cate1,
                            context.cate2,
                        ]
                        if use_type_col:
                            db_params.append(learn_list_type)
                        if use_author:
                            db_params.append(author_val)
                        db_params.append(source_url)
                    else:
                        # 4) subject 없음 → UPSERT (중복 방지)
                        logger.info(f"[{_db_tag}] subject 없음 → UPSERT: {source_url}")
                        if use_author and use_type_col:
                            db_query = f"""
                            INSERT INTO {table_name_db} (content, subject, content_type, `type`, status, size, chunk, created_at, cate1, cate2, `{author_col}`)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                subject = VALUES(subject),
                                content_type = VALUES(content_type),
                                `type` = VALUES(`type`),
                                status = VALUES(status),
                                size = VALUES(size),
                                chunk = VALUES(chunk),
                                created_at = VALUES(created_at),
                                cate1 = VALUES(cate1),
                                cate2 = VALUES(cate2),
                                `{author_col}` = VALUES(`{author_col}`)
                        """
                            db_params = [
                                source_url,
                                title,
                                learn_list_content_type,
                                learn_list_type,
                                'N',
                                total_size,
                                total_chunks,
                                __import__('datetime').datetime.now().isoformat(),
                                context.cate1,
                                context.cate2,
                                author_val,
                            ]
                        elif use_author:
                            db_query = f"""
                            INSERT INTO {table_name_db} (content, subject, content_type, status, size, chunk, created_at, cate1, cate2, `{author_col}`)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                subject = VALUES(subject),
                                content_type = VALUES(content_type),
                                status = VALUES(status),
                                size = VALUES(size),
                                chunk = VALUES(chunk),
                                created_at = VALUES(created_at),
                                cate1 = VALUES(cate1),
                                cate2 = VALUES(cate2),
                                `{author_col}` = VALUES(`{author_col}`)
                        """
                            db_params = [
                                source_url,
                                title,
                                learn_list_content_type,
                                'N',
                                total_size,
                                total_chunks,
                                __import__('datetime').datetime.now().isoformat(),
                                context.cate1,
                                context.cate2,
                                author_val,
                            ]
                        elif use_type_col:
                            db_query = f"""
                            INSERT INTO {table_name_db} (content, subject, content_type, `type`, status, size, chunk, created_at, cate1, cate2)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                subject = VALUES(subject),
                                content_type = VALUES(content_type),
                                `type` = VALUES(`type`),
                                status = VALUES(status),
                                size = VALUES(size),
                                chunk = VALUES(chunk),
                                created_at = VALUES(created_at),
                                cate1 = VALUES(cate1),
                                cate2 = VALUES(cate2)
                        """
                            db_params = [
                                source_url,
                                title,
                                learn_list_content_type,
                                learn_list_type,
                                'N',
                                total_size,
                                total_chunks,
                                __import__('datetime').datetime.now().isoformat(),
                                context.cate1,
                                context.cate2,
                            ]
                        else:
                            db_query = f"""
                            INSERT INTO {table_name_db} (content, subject, content_type, status, size, chunk, created_at, cate1, cate2)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                subject = VALUES(subject),
                                content_type = VALUES(content_type),
                                status = VALUES(status),
                                size = VALUES(size),
                                chunk = VALUES(chunk),
                                created_at = VALUES(created_at),
                                cate1 = VALUES(cate1),
                                cate2 = VALUES(cate2)
                        """
                            db_params = [
                                source_url,
                                title,
                                learn_list_content_type,
                                'N',
                                total_size,
                                total_chunks,
                                __import__('datetime').datetime.now().isoformat(),
                                context.cate1,
                                context.cate2,
                            ]

                    if db_query:
                        try:
                            if "dongjak.go.kr/yeyak/progrm/master/online/" in str(source_url or ""):
                                logger.warning(
                                    "[DongjakTypeDebug][url_edu_before] table=%s use_type_col=%s learn_list_type=%s content_type=%s url=%s sql=%s",
                                    table_name_db,
                                    use_type_col,
                                    learn_list_type,
                                    learn_list_content_type,
                                    (source_url or "")[:220],
                                    " ".join(str(db_query or "").split())[:260],
                                )
                        except Exception:
                            pass
                        await _exec_query(db_query, db_params, fetch=False, dbname=dbname)
                        try:
                            from db.mariadb_save_update import ensure_learn_list_type_not_blank

                            await ensure_learn_list_type_not_blank(
                                db_name=str(dbname),
                                learn_list_table=str(table_name_db),
                                default_type=learn_list_type,
                                content=source_url,
                            )
                        except Exception as _type_fix_ex:
                            logger.warning(
                                "[URL 학습] LEARN_LIST type 보정 실패 | url=%s err=%s",
                                source_url,
                                _type_fix_ex,
                            )
                        try:
                            if "dongjak.go.kr/yeyak/progrm/master/online/" in str(source_url or ""):
                                from backend.shared.learn_list_url_row_cache import find_learn_list_row_in_url_cache

                                _cached_type_row = await find_learn_list_row_in_url_cache(
                                    db_name=str(dbname),
                                    table_name=str(table_name_db),
                                    columns=("id", "content", "content_type", "type"),
                                    candidate_url=source_url,
                                )
                                _type_row = [_cached_type_row] if _cached_type_row else await _exec_query(
                                    f"SELECT id, content_type, type FROM {table_name_db} WHERE content = %s ORDER BY id DESC LIMIT 1",
                                    [source_url],
                                    fetch=True,
                                    dbname=dbname,
                                )
                                _type_r0 = _type_row[0] if _type_row else None
                                if isinstance(_type_r0, dict):
                                    logger.warning(
                                        "[DongjakTypeDebug][url_edu_after] table=%s id=%s content_type=%s type=%s url=%s",
                                        table_name_db,
                                        _type_r0.get("id"),
                                        _type_r0.get("content_type"),
                                        _type_r0.get("type"),
                                        (source_url or "")[:220],
                                    )
                        except Exception:
                            pass
                        logger.info(
                            f"[{_db_tag} 저장 완료] URL: {source_url}, sub_change_on={sub_change_mode_on}, 총 사이즈: {total_size} bytes, 청크 개수: {total_chunks}"
                        )
                        try:
                            from db.mariadb_save_update import update_learn_list_status_board
                            from backend.shared.learn_list_url_row_cache import find_learn_list_row_in_url_cache

                            _cached_id_row = await find_learn_list_row_in_url_cache(
                                db_name=str(dbname),
                                table_name=str(table_name_db),
                                columns=("id", "content"),
                                candidate_url=source_url,
                            )
                            _idr = [_cached_id_row] if _cached_id_row else await _exec_query(
                                f"SELECT id FROM {table_name_db} WHERE content = %s ORDER BY id DESC LIMIT 1",
                                [source_url],
                                fetch=True,
                                dbname=dbname,
                            )
                            _r0 = _idr[0] if _idr else None
                            if isinstance(_r0, dict):
                                _rid = _r0.get("id")
                            else:
                                _rid = _r0
                            learn_list_row_id = _rid
                            if _rid is None:
                                if had_existing_learn_list_row:
                                    learn_list_inserted = False
                                    learn_list_action = "update_missing"
                                else:
                                    learn_list_inserted = False
                                    learn_list_action = "insert_failed"
                            if _rid is not None:
                                await update_learn_list_status_board(
                                    str(dbname),
                                    str(chat_bot_id),
                                    str(_rid),
                                    int(total_chunks or 0),
                                    cate1=context.cate1,
                                    cate2=context.cate2,
                                )
                        except Exception as _st_ex:
                            logger.warning(
                                "[URL 학습] LEARN_LIST 학습완료(status=Y) 반영 실패 | url=%s err=%s",
                                source_url,
                                _st_ex,
                            )
                except Exception as _db_error:
                    logger.error(f"[URL 저장 실패] DB: {dbname}, URL: {source_url}, 오류: {_db_error}")
            #         # MariaDB 실패해도 PostgreSQL은 이미 성공했으므로 계속 진행

            # 4. 처리 완료된 URL 정보 반환 (inserted_records가 있을 때만)
            if not inserted_records:
                logger.error(f"[URL 처리 실패] 데이터 저장 실패로 인한 전체 처리 실패: {source_url}")
                return None
            
            url_result = {
                "url": source_url,
                "title": title,  # web_title을 위해 주석 해제
                "chunks": total_chunks,
                "order": url_index,
                "processing_time": round(time.time() - start_time, 2),
                # "snippet": page_snippet,  # 주석 처리
                "favicon_url": favicon_url,
                "source_size": [len(content.encode('utf-8'))],  # bytes 단위로 변경
                "change_status": "new_or_changed",  # 신규 또는 변경된 URL
                "chunk_hash": content_hash,
                "learn_list_inserted": learn_list_inserted,
                "learn_list_action": learn_list_action,
                "learn_list_id": learn_list_row_id,
            }
            if inserted_records:
                url_result["inserted_records"] = inserted_records
            
            logger.info(f"[변경감지 결과] {source_url} - change_status: {url_result['change_status']}")
            logger.info(f"[URL 처리 완료] {url_index}/{total_urls}: {source_url}")
            return url_result

        except Exception as e:
            logger.error(f"개별 URL 처리 실패: {source_url}, 오류: {e}")
            return None
        except asyncio.CancelledError:
            logger.info(f"[중단 취소] URL 처리 태스크 취소됨: {result.get('source')}")
            raise

# ✅ 메인 process_url 함수
async def process_url(
    content: str,
    subject: str,
    context: CrawlingContext,
    each_progress: float,
    memo: str = "",
    max_depth: int = 10,
):

    """
    URL을 처리하여 텍스트를 추출하고 벡터화한 후 데이터베이스에 저장합니다.
    """
    try:
        # Context에서 값 추출
        table_name = context.table_name
        dbname = context.dbname
        job_id = context.job_id
        job_manager = context.job_manager
        job_progress_manager = context.job_progress
        
        logger.info(f"[Process URL 시작] URL: {content}, crawl_mode: {context.crawl_mode}, url_filter: {context.url_filter}")
        
        # ✅ 403 폴백 처리가 포함된 병렬 크롤링 모드
        logger.info(f"[403 폴백 크롤링 모드 감지] URL: {content}")
        processed_urls = await crawl_and_process_url_parallel_with_403_fallback(
            start_url=content,
            subject=subject,
            context=context,
            each_progress=each_progress,
            memo=memo,
            max_depth=max_depth,
        )
        
        total_discovered_count = processed_urls.get('total_discovered_urls', 0) # ✅ 탐색된 전체 URL 개수
        target_urls_count = processed_urls.get('total_new_or_changed_urls', 0)  # ✅ 변경감지 후 최종 DB insert 대상 개수
        new_or_changed_count = processed_urls.get('new_or_changed_count', 0)  # ✅ 실제 성공한 URL 개수

        logger.info(f"[403 폴백 크롤링 완료] 탐색 URLs : {total_discovered_count}개, 학습 결과: {processed_urls.get('total_processed_urls', 0)}개 URL, chunk_count: {len(processed_urls.get('chunk_count', []))}")
        logger.info(f"\n■■■■■■■■■■■■■■■■■■■■■■■■■■■ \n전체 탐색 URL {total_discovered_count}개\n학습 대상 URL (최종 DB insert 대상) {target_urls_count}개\n실제 성공 {new_or_changed_count}개 \n■■■■■■■■■■■■■■■■■■■■■■■■■■■")

        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # ✅ DB 업데이트 전에 job_status 확인 (use_crawl_stop 상태가 그대로 남아있음)
        current_job_status = None
        if job_manager and job_id:
            try:
                current_job_status = await job_manager.get_job_status(job_id)
                logger.info(f"[DB 업데이트 전 상태 확인] job_id={job_id}, current_status={current_job_status}")
            except Exception as e:
                logger.warning(f"[상태 확인 실패] job_id={job_id}, 오류: {e}")
        
        # ✅ status 결정 (use_crawl_stop 상태를 직접 사용)
        is_user_stop = (current_job_status == "use_crawl_stop")
        
        if is_user_stop:
            # 사용자가 의도적으로 중단 버튼을 클릭한 경우
            if target_urls_count > 0 and new_or_changed_count == 0:
                status = 'error'
            else:
                status = 'stop'
            logger.info(f"[사용자 중단 감지] job_id={job_id}, status='{status}'으로 설정 (use_crawl_stop)")
        elif target_urls_count > 0 and new_or_changed_count == 0:
            status = 'error'
        elif target_urls_count == new_or_changed_count:
            status = 'ok'            
        elif target_urls_count > 0 and new_or_changed_count > 0:
            status = 'stop'
        else:
            status = 'ok'

        query = """
        UPDATE ASADAL_CRAWLING_LOG
        SET scan = %s, collection = %s, save = %s, study = %s, status = %s, end_at = %s
        WHERE job_id = %s
        LIMIT 1;
        """

        # ✅ dbname에 따라 MySQL/MariaDB 분기
        _name_lc = (dbname or '').strip().lower()
        if _name_lc in ('chatty', 'naraone'):
            from db.mysql_db_config import mysql_execute_query
            await mysql_execute_query(query, [total_discovered_count, target_urls_count, new_or_changed_count, new_or_changed_count, status, end_time, job_id], fetch=False, dbname=dbname)
        else:
            await maria_execute_query(query, [total_discovered_count, target_urls_count, new_or_changed_count, new_or_changed_count, status, end_time, job_id], fetch=False, dbname=dbname)
        if status == 'stop':
            sse_status = 'cancelled'
        elif status == 'error':
            sse_status = 'error'
        else:
            sse_status = 'completed'
        # ✅ Redis SSE 최종 완료 메시지 전송
        sse_message = {
            "status": sse_status,
            "total_count": total_discovered_count,
            "collection_count": target_urls_count,
            "save_count": new_or_changed_count,
            "study_count": new_or_changed_count,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        await send_message_to_redis_sse(job_id, sse_message, dbname)
        logger.info(f"[Redis SSE 최종 완료 메시지 전송] job_id={job_id}, status=completed, 탐색={total_discovered_count}, 수집={target_urls_count}, 저장={new_or_changed_count}")
        
        # ✅ DB 업데이트 완료 후 use_crawl_stop 상태를 초기화 (다음 크롤링을 위해)
        if is_user_stop and job_manager and job_id:
            try:
                await job_manager.update_job_status(job_id, "completed")
                logger.info(f"[상태 초기화] DB 업데이트 완료 후 use_crawl_stop → completed 변경: job_id={job_id}")
            except Exception as reset_error:
                logger.warning(f"[상태 초기화 실패] job_id={job_id}, 오류: {reset_error}")
        
        message = {
            "type": "completed",
            "total_count": total_discovered_count,
            "collection_count": target_urls_count,
            "save_count": new_or_changed_count,
            "study_count": new_or_changed_count
        }
        await send_message_to_socket(job_id, message, job_manager)
        logger.info(f"[ASADAL_CRAWLING_LOG 업데이트 완료] JOB_ID: {job_id}, status: {status}, scan: {total_discovered_count}, collection: {target_urls_count}, save: {new_or_changed_count}")
        return processed_urls
    except Exception as e:
        logger.error(f"URL 처리 중 오류: {e}", exc_info=True)
        status = 'error'
        try:
            end_time
        except NameError:
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
        UPDATE ASADAL_CRAWLING_LOG
        SET status = %s, end_at = %s
        WHERE job_id = %s
        LIMIT 1;
        """
        
        # ✅ dbname에 따라 MySQL/MariaDB 분기
        _name_lc = (dbname or '').strip().lower()
        if _name_lc in ('chatty', 'naraone'):
            from db.mysql_db_config import mysql_execute_query
            await mysql_execute_query(query, [status, end_time, job_id], fetch=False, dbname=dbname)
        else:
            await maria_execute_query(query, [status, end_time, job_id], fetch=False, dbname=dbname)
        
        # ✅ Redis SSE 오류 메시지 전송
        error_sse_message = {
            "status": "error",  # ✅ TTL 1시간 적용
            "total_count": total_discovered_count if 'total_discovered_count' in locals() else 0,
            "collection_count": target_urls_count if 'target_urls_count' in locals() else 0,
            "save_count": new_or_changed_count if 'new_or_changed_count' in locals() else 0,
            "study_count": new_or_changed_count if 'new_or_changed_count' in locals() else 0,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "error_message": str(e)
        }
        await send_message_to_redis_sse(job_id, error_sse_message, dbname)
        logger.info(f"[Redis SSE 오류 메시지 전송] job_id={job_id}, status=error, 오류={str(e)[:100]}")
        
        # WebSocket으로도 오류 메시지 전송 (하위 호환성)
        await send_message_to_socket(
            job_id, {"status": "error", "message": str(e)}, job_manager
        )
        message = {
            "type": "completed",
            "total_count": total_discovered_count if 'total_discovered_count' in locals() else 0,
            "collection_count": target_urls_count if 'target_urls_count' in locals() else 0,
            "save_count": new_or_changed_count if 'new_or_changed_count' in locals() else 0,
            "study_count": new_or_changed_count if 'new_or_changed_count' in locals() else 0
        }
        await send_message_to_socket(job_id, message, job_manager)
        raise RuntimeError(f"URL 처리 중 오류 발생: {e}")

# ✅ fetch_page_with_playwright 함수는 extract_playwright.py로 이관되었습니다.

# ✅ 크롤링 환경설정 조회 및 시간대에 따른 max_crawl_urls 결정 유틸리티
from datetime import datetime
from typing import Any, Dict
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # Python<3.9 등 환경 호환용

def _get_kst_now() -> datetime:
    """KST 기준 현재 시간을 반환한다. ZoneInfo 미지원 시 시스템 로컬 시간을 사용한다."""
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Seoul"))
    return datetime.now()

def _is_weekday_daytime(now_kst: datetime) -> bool:
    """평일 낮 시간(월~금, 09:00~18:00 미만) 여부."""
    return now_kst.weekday() < 5 and 9 <= now_kst.hour < 18

def _parse_config_int(raw_value: Any, default_value: int) -> int:
    """콤마가 포함된 숫자 문자열 등을 안전하게 int로 변환한다."""
    try:
        if raw_value is None:
            return default_value
        return int(str(raw_value).replace(",", "").strip())
    except Exception:
        return default_value

async def _fetch_crawling_config_map(dbname: str, chat_bot_id: str) -> Dict[str, str]:
    """ASADAL_CRAWLING_CONFIG에서 4개 키를 모두 조회하여 dict로 반환한다."""
    DEFAULT_CRAWL_CONFIG: Dict[str, str] = {
        "week_count": "100",
        "page_count": "3000",
        "stop_count": "10",
        "conc_count": "3",
        "sub_change": "off",
    }
    sql_query = (
        "SELECT `key`, value "
        "FROM ASADAL_CRAWLING_CONFIG "
        "WHERE `key` IN ('week_count','page_count','stop_count','conc_count','sub_change')"
    )
    params = {}
    # dbname 기준으로 조회 대상 DB 선택
    db_kind = "MARIADB"
    try:
        name_lc = (dbname or "").strip().lower()
        if name_lc in ("chatty", "naraone"):
            from db.mysql_db_config import mysql_execute_query
            data = await mysql_execute_query(sql_query, params, fetch=True, dbname=dbname)
            db_kind = "MYSQL"
        else:
            from db.maria_operations import maria_execute_query
            data = await maria_execute_query(sql_query, params, fetch=True, dbname=dbname)
            db_kind = "MARIADB"
    except Exception as e:
        logger.error(f"설정 조회 중 DB 선택/실행 오류: {e}")
        raise
    config_map: Dict[str, str] = {}
    if data:
        for row in data:
            k = row.get("key")
            v = row.get("value")
            if k is not None:
                config_map[str(k)] = v
    # 기본값과 병합 (DB 값이 우선)
    merged_map: Dict[str, str] = {**DEFAULT_CRAWL_CONFIG, **config_map}
    missing_keys = [k for k in DEFAULT_CRAWL_CONFIG.keys() if k not in config_map]
    if missing_keys:
        logger.info(f"[ℹ️ 기본값 적용] 누락 키: {missing_keys} → { {k: DEFAULT_CRAWL_CONFIG[k] for k in missing_keys} }")
    return merged_map

def _decide_max_crawl_urls_by_time(config_map: Dict[str, str], default_value: int) -> int:
    """평일 낮/야간·주말에 따라 week_count/page_count 중 하나를 선택한다."""
    now_kst = _get_kst_now()
    use_key = "week_count" if _is_weekday_daytime(now_kst) else "page_count"
    chosen_value = _parse_config_int(config_map.get(use_key), default_value)
    crawl_settings = {
        "max_crawl_urls": chosen_value,
        "conc_count": config_map.get('conc_count'),
        "sub_change": config_map.get('sub_change'),
        "stop_count": config_map.get('stop_count'),
    }
    when_text = "평일 낮" if use_key == "week_count" else "야간/주말"
    logger.info(
        f"[⏰ 시간대 판정] {when_text}({now_kst.strftime('%Y-%m-%d %H:%M:%S %Z')}) → {use_key} 적용: {chosen_value}"
    )
    # if "stop_count" in config_map:
    #     logger.info(f"[✅ 설정 적용] stop_count: {config_map.get('stop_count')}")
    # if "conc_count" in config_map:
    #     logger.info(f"[✅ 설정 적용] conc_count: {config_map.get('conc_count')}")
    # if "sub_change" in config_map:
    #     logger.info(f"[✅ 설정 적용] sub_change: {config_map.get('sub_change')}")
    return crawl_settings

async def _subject_exists(dbname: str, url: str, chat_bot_id: str = None) -> bool:
    """해당 URL의 subject가 이미 존재하는지 확인한다 (MySQL/MariaDB 분기).
    
    Args:
        dbname: 데이터베이스명.
        url: 확인할 URL.
        chat_bot_id: 챗봇 ID (ASADAL_{chat_bot_id[-12:]}_LEARN_LIST 테이블 조회용).
    
    Returns:
        subject가 존재하면 True, 아니면 False.
    """
    try:
        # 사용할 DB 실행 함수 선택
        _name_lc = (dbname or '').strip().lower()
        if _name_lc in ('chatty', 'naraone'):
            from db.mysql_db_config import mysql_execute_query as _exec_query
            _db_tag = 'MySQL'
        else:
            from db.maria_operations import maria_execute_query as _exec_query
            _db_tag = 'MariaDB'

        # chat_bot_id가 제공되면 ASADAL_{chat_bot_id[-12:]}_LEARN_LIST 테이블에서 조회
        if chat_bot_id:
            logger.info(f"chat_bot_id: {chat_bot_id}")
            table_suffix = chat_bot_id[-12:]
            table_name = f"ASADAL_{table_suffix}_LEARN_LIST"
            query = (
                f"SELECT 1 FROM {table_name} "
                "WHERE content = %s AND subject IS NOT NULL AND subject != '' LIMIT 1"
            )
            logger.debug(f"[{_db_tag} subject 존재 확인] 테이블: {table_name}, URL: {url}")
        else:
            logger.info(f"chat_bot_id: {chat_bot_id}")
            query = (
                "SELECT 1 FROM ASADAL_CRAWLING_DATA "
                "WHERE content = %s AND subject IS NOT NULL AND subject != '' LIMIT 1"
            )
            logger.debug(f"[{_db_tag} subject 존재 확인] 테이블: ASADAL_CRAWLING_DATA, URL: {url}")

        rows = await _exec_query(query, [url], fetch=True, dbname=dbname)
        exists = bool(rows)
        
        if exists:
            logger.info(f"[{_db_tag} subject 존재 확인] URL: {url}, subject 존재함")
        else:
            logger.info(f"[{_db_tag} subject 존재 확인] URL: {url}, subject 없음")
            
        return exists
    except Exception as e:
        logger.warning(f"[subject 존재 확인 실패] url={url}, 오류={e}")
        return False

# ✅ 하이브리드 스트리밍 구조: 변경 감지 워커
async def change_detection_worker(
    collection_queue: asyncio.Queue,
    change_detection_queue: asyncio.Queue,
    table_name: str,
    dbname: str,
    job_id: str,
    job_manager: AsyncJobManager,
    stop_signal: CrawlStopSignal = None,
    total_new_or_changed: list = None,
    total_discovered_urls_final: list = None,
    success_count: list = None,
    chat_bot_id: str = None
):
    """변경 감지 워커: 수집된 URL을 배치로 변경 감지"""
    batch = []
    logger.info(f"[변경 감지 워커 시작] 배치 크기: {CHANGE_DETECTION_BATCH_SIZE}, 타임아웃: {CHANGE_DETECTION_TIMEOUT}초")
    
    while True:
        try:
            # 중단 신호 확인
            if stop_signal and stop_signal.is_stopped():
                # 남은 배치 처리
                if batch:
                    await _process_change_detection_batch(
                        batch,
                        change_detection_queue,
                        table_name,
                        dbname,
                        job_id,
                        job_manager,
                        total_new_or_changed,
                        total_discovered_urls_final,
                        success_count,
                        stop_signal=stop_signal,
                        chat_bot_id=chat_bot_id
                    )
                logger.info("[변경 감지 워커 종료] 중단 신호 감지")
                break
            
            # 큐에서 URL 가져오기 (타임아웃 설정)
            try:
                result = await asyncio.wait_for(
                    collection_queue.get(), 
                    timeout=CHANGE_DETECTION_TIMEOUT
                )
                
                # 종료 신호 확인
                if result is None:
                    collection_queue.task_done()
                    # 남은 배치 처리
                    if batch:
                        await _process_change_detection_batch(
                            batch,
                            change_detection_queue,
                            table_name,
                            dbname,
                            job_id,
                            job_manager,
                            total_new_or_changed,
                            total_discovered_urls_final,
                            success_count,
                            stop_signal=stop_signal
                        )
                    logger.info("[변경 감지 워커 종료] 종료 신호 수신")
                    break
                
                # ✅ 배치에 추가하기 전에 중복 체크
                url = result.get("source", "")
                is_duplicate_in_batch = any(item.get("source") == url for item in batch)
                if is_duplicate_in_batch:
                    logger.error(f"[🚨 변경 감지 워커 배치 중복!] {url} - 같은 배치에 이미 존재!")
                    collection_queue.task_done()
                    continue
                
                batch.append(result)
                logger.debug(f"[변경 감지 워커 배치 추가] {url} (배치 크기: {len(batch)}/{CHANGE_DETECTION_BATCH_SIZE})")
                collection_queue.task_done()
            except asyncio.TimeoutError:
                # 타임아웃: 배치 크기에 도달하지 않아도 처리
                if batch:
                    await _process_change_detection_batch(
                        batch,
                        change_detection_queue,
                        table_name,
                        dbname,
                        job_id,
                        job_manager,
                        total_new_or_changed,
                        total_discovered_urls_final,
                        success_count,
                        stop_signal=stop_signal,
                        chat_bot_id=chat_bot_id
                    )
                    batch = []
                continue
            
            # 배치 크기 도달 시 처리
            if len(batch) >= CHANGE_DETECTION_BATCH_SIZE:
                await _process_change_detection_batch(
                    batch,
                    change_detection_queue,
                    table_name,
                    dbname,
                    job_id,
                    job_manager,
                    total_new_or_changed,
                    total_discovered_urls_final,
                    success_count,
                    stop_signal=stop_signal
                )
                batch = []
                
        except Exception as e:
            logger.error(f"[변경 감지 워커 오류] {e}", exc_info=True)
            if batch:
                batch = []  # 오류 발생 시 배치 초기화

async def _process_change_detection_batch(
    batch: List[Dict],
    change_detection_queue: asyncio.Queue,
    table_name: str,
    dbname: str,
    job_id: str,
    job_manager: AsyncJobManager,
    total_new_or_changed: list = None,
    total_discovered_urls_final: list = None,
    success_count: list = None,
    stop_signal: CrawlStopSignal = None,
    chat_bot_id: str = None
):
    """배치 변경 감지 처리"""
    try:
        logger.info(f"[배치 변경 감지] {len(batch)}개 URL 처리 시작")
        
        async def _emit_collection_progress(changed_increment: int) -> None:
            if changed_increment <= 0:
                return
            if total_new_or_changed is not None:
                total_new_or_changed[0] += changed_increment
            
            total_discovered_val = total_discovered_urls_final[0] if total_discovered_urls_final else 0
            count_val = total_new_or_changed[0] if total_new_or_changed else changed_increment
            
            if count_val > total_discovered_val:
                logger.error(
                    f"[강제 검증 - 수집단계] count({count_val}) > total_discovered({total_discovered_val}) 논리적 오류! count를 total_discovered로 강제 제한."
                )
                count_val = total_discovered_val
                if total_new_or_changed is not None:
                    total_new_or_changed[0] = total_discovered_val
            
            success_url_val = success_count[0] if success_count else 0
            if total_discovered_val == 0:
                logger.warning("[수집단계 경고] total_discovered가 0입니다. 탐색 워커가 아직 시작되지 않았을 수 있습니다.")
            
            logger.info(
                f"[수집단계 메시지 전송] total_discovered: {total_discovered_val}, count: {count_val} (증가 {changed_increment}개), success_url: {success_url_val}"
            )
            
            message = {
                "type": "crawl_count",
                "status": "running",
                "total_count": total_discovered_val,
                "collection_count": count_val,
                "save_count": success_url_val,
                "study_count": success_url_val,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            
            try:
                await update_crawling_log_db(
                    job_id,
                    dbname,
                    total_discovered_val,
                    count_val,
                    success_url_val,
                    success_url_val,
                )
                logger.info(
                    f"[변경감지 단계 DB 업데이트] job_id={job_id}, scan={total_discovered_val}, collection={count_val}, save={success_url_val}"
                )
            except Exception as db_error:
                logger.error(f"[변경감지 단계 DB 업데이트 실패] job_id={job_id}, 오류={db_error}")
            
            await send_message_to_redis_sse(job_id, message, dbname=dbname)
            logger.info(
                f"[변경감지 단계] Redis SSE 메시지 전송 완료: 탐색={total_discovered_val}, 수집={count_val}, 저장={success_url_val}"
            )
        
        # 게시판/파일 크롤링은 해시 비교를 적용하지 않으므로 선 파싱 없이 배치 단위로 통과시킨다.
        logger.info(
            "[배치 변경감지 우회 시작] 대상=%s, table=%s, db=%s",
            len(batch),
            table_name,
            dbname,
        )
        changed_items, unchanged_items = await batch_check_url_changes(batch, table_name, dbname)
        logger.info(
            "[배치 변경감지 우회 완료] 저장대상=%s, 스킵=%s",
            len(changed_items),
            len(unchanged_items),
        )
        
        # 변경/신규 URL만 다음 큐에 전달 (비동기로 즉시 전달하여 저장 워커가 바로 처리할 수 있도록)
        # ✅ total_new_or_changed 업데이트를 먼저 수행
        await _emit_collection_progress(len(changed_items))
        
        # ✅ 수집 단계: 변경 감지 완료 후 웹소켓 메시지 전송 (값을 변수로 정리 후 전송)
        logger.info(f"[배치 변경 감지 완료] 처리 대상: {len(changed_items)}개 (변경/신규), 스킵: {len(unchanged_items)}개 (변경없음)")
        
        # collection_count 갱신은 _emit_collection_progress에서 처리됨
        
        # ✅ 큐에 아이템 전달 (웹소켓 메시지 전송 후 즉시 처리)
        for item in changed_items:
            url = item.get("source", "")
            await change_detection_queue.put(item)
            logger.debug(f"[변경 감지 → 저장 큐 전달] {url}")
    except Exception as e:
        logger.error(f"[배치 변경 감지 오류] {e}", exc_info=True)

# ✅ 하이브리드 스트리밍 구조: 저장 워커
async def save_worker(
    change_detection_queue: asyncio.Queue,
    subject: str,
    context: CrawlingContext,
    memo: str,
    each_progress: float,
    stop_signal: CrawlStopSignal = None,
    processed_urls: dict = None,
    total_chunks_processed: list = None,
    completed_count: list = None,
    success_count: list = None,
    total_new_or_changed: list = None,
    start_time: float = None,
    total_discovered_urls_ref: list = None,  # ✅ 탐색 완료 시점의 값을 참조하기 위한 파라미터
    processed_urls_lock: asyncio.Lock = None,  # ✅ 중복 체크를 위한 Lock
):
    """저장 워커: 변경 감지된 URL을 배치로 저장"""
    # Context에서 값 추출
    table_name = context.table_name
    dbname = context.dbname
    job_id = context.job_id
    job_manager = context.job_manager
    job_progress_manager = context.job_progress
    chat_bot_id = context.chat_bot_id
    
    batch = []
    redis_client = await get_redis()
    if start_time is None:
        start_time = time.time()
    max_concurrent_urls = min(multiprocessing.cpu_count(), 12)
    semaphore = asyncio.Semaphore(max_concurrent_urls)
    
    logger.info(f"[저장 워커 시작] 배치 크기: {DB_SAVE_BATCH_SIZE}, 타임아웃: {DB_SAVE_TIMEOUT}초")
    
    while True:
        try:
            # 중단 신호 확인
            if stop_signal and stop_signal.is_stopped():
                # 남은 배치 처리
                if batch:
                    await _process_save_batch(
                        batch, subject, context,
                        memo, each_progress,
                        semaphore, redis_client, start_time, processed_urls,
                        total_chunks_processed, completed_count, success_count, total_new_or_changed,
                        total_discovered_urls_ref, processed_urls_lock, stop_signal
                    )
                logger.info("[저장 워커 종료] 중단 신호 감지")
                break
            
            # 큐에서 URL 가져오기 (타임아웃 설정)
            try:
                result = await asyncio.wait_for(
                    change_detection_queue.get(),
                    timeout=DB_SAVE_TIMEOUT
                )
                
                # 종료 신호 확인
                if result is None:
                    change_detection_queue.task_done()
                    # 남은 배치 처리
                    if batch:
                        await _process_save_batch(
                            batch, subject, context,
                            memo, each_progress,
                            semaphore, redis_client, start_time, processed_urls,
                            total_chunks_processed, completed_count, success_count, total_new_or_changed,
                            total_discovered_urls_ref, processed_urls_lock, stop_signal
                        )
                    logger.info("[저장 워커 종료] 종료 신호 수신")
                    break
                
                # ✅ 배치에 추가하기 전에 중복 체크
                url = result.get("source", "")
                is_duplicate_in_batch = any(item.get("source") == url for item in batch)
                if is_duplicate_in_batch:
                    logger.error(f"[🚨 저장 워커 배치 중복!] {url} - 같은 배치에 이미 존재!")
                    change_detection_queue.task_done()
                    continue
                
                batch.append(result)
                logger.debug(f"[저장 워커 배치 추가] {url} (배치 크기: {len(batch)}/{DB_SAVE_BATCH_SIZE})")
                change_detection_queue.task_done()
            except asyncio.TimeoutError:
                # 타임아웃: 배치 크기에 도달하지 않아도 처리
                if batch:
                    await _process_save_batch(
                        batch, subject, context,
                        memo, each_progress,
                        semaphore, redis_client, start_time, processed_urls,
                        total_chunks_processed, completed_count, success_count, total_new_or_changed,
                        total_discovered_urls_ref, processed_urls_lock, stop_signal
                    )
                    batch = []
                continue
            
            # 배치 크기 도달 시 처리
            if len(batch) >= DB_SAVE_BATCH_SIZE:
                    await _process_save_batch(
                        batch, subject, context,
                        memo, each_progress,
                        semaphore, redis_client, start_time, processed_urls,
                        total_chunks_processed, completed_count, success_count, total_new_or_changed,
                        total_discovered_urls_ref, processed_urls_lock
                    )
                    batch = []
                
        except Exception as e:
            logger.error(f"[저장 워커 오류] {e}", exc_info=True)
            if batch:
                batch = []  # 오류 발생 시 배치 초기화

async def _process_save_batch(
    batch: List[Dict],
    subject: str,
    context: CrawlingContext,
    memo: str,
    each_progress: float,
    semaphore: asyncio.Semaphore,
    redis_client,
    start_time: float,
    processed_urls: dict,
    total_chunks_processed: list,
    completed_count: list,
    success_count: list,
    total_new_or_changed: list,
    total_discovered_urls_ref: list = None,  # ✅ 탐색 완료 시점의 값을 참조하기 위한 파라미터
    processed_urls_lock: asyncio.Lock = None,  # ✅ 중복 체크를 위한 Lock
    stop_signal: CrawlStopSignal = None,
):
    """배치 저장 처리"""
    try:
        # Context에서 값 추출
        table_name = context.table_name
        dbname = context.dbname
        job_id = context.job_id
        job_manager = context.job_manager
        job_progress_manager = context.job_progress
        chat_bot_id = context.chat_bot_id
        
        logger.info(f"[배치 저장 시작] {len(batch)}개 URL 처리")
        
        # ✅ 1단계: 배치의 모든 URL을 먼저 processed_urls에 예약 (중복 방지)
        reserved_urls = []
        duplicate_count = 0
        if processed_urls_lock:
            async with processed_urls_lock:
                for result in batch:
                    source_url = result.get("source")
                    if source_url:
                        if source_url in processed_urls:
                            duplicate_count += 1
                            reserved_status = processed_urls[source_url]
                            logger.error(f"[🚨 배치 저장 중복!] {source_url} - 이미 processed_urls에 존재, 스킵 (상태: {reserved_status})")
                        else:
                            # 예약: placeholder로 먼저 등록
                            processed_urls[source_url] = {"reserved": True}
                            reserved_urls.append(source_url)
                            logger.debug(f"[배치 저장 예약] {source_url}")
        else:
            for result in batch:
                source_url = result.get("source")
                if source_url and source_url not in processed_urls:
                    processed_urls[source_url] = {"reserved": True}
                    reserved_urls.append(source_url)
        
        logger.info(f"[배치 저장 예약 완료] {len(reserved_urls)}개 URL 예약 (중복 제외: {len(batch) - len(reserved_urls)}개)")

        # ✅ 2단계: 예약된 URL만 처리
        tasks = []
        for idx, result in enumerate(batch):
            source_url = result.get("source")
            # 예약된 URL만 처리
            if source_url in reserved_urls:
                task = asyncio.create_task(process_single_crawled_url(
                    semaphore=semaphore,
                    result=result,
                    subject=subject,
                    context=context,
                    memo=memo,
                    redis_client=redis_client,
                    start_time=start_time,
                    url_index=completed_count[0] + idx + 1,
                    total_urls=total_new_or_changed[0] if total_new_or_changed else len(batch),
                    page_progress=each_progress / (total_new_or_changed[0] if total_new_or_changed else len(batch)),
                    stop_signal=stop_signal,
                ))
                tasks.append(task)
            else:
                logger.info(f"[배치 저장 스킵] {source_url} - 중복 URL")
        
        # ✅ 3단계: 결과 수집 및 업데이트
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for url_result in results:
            if isinstance(url_result, Exception):
                logger.error(f"[배치 저장 오류] {url_result}")
                continue
            
            if url_result:
                source_url = url_result.get("url")
                if source_url:
                    # ✅ 예약된 URL의 결과로 업데이트 (이미 Lock으로 예약했으므로 추가 Lock 불필요)
                    if processed_urls_lock:
                        async with processed_urls_lock:
                            # placeholder를 실제 결과로 교체
                            processed_urls[source_url] = url_result
                            logger.debug(f"[배치 저장 결과 업데이트] {source_url}")
                    else:
                        processed_urls[source_url] = url_result
                    
                    total_chunks_processed[0] += url_result.get("chunks", 0)
                    completed_count[0] += 1
                    
                    change_status = url_result.get("change_status", "new_or_changed")
                    logger.info(f"[저장단계 처리] URL: {source_url}, change_status: {change_status}")
                    
                    # ✅ new_or_changed인 경우만 success_count 증가
                    if change_status == "new_or_changed":
                        success_count[0] += 1
                        logger.info(f"[저장단계 success_count 증가] 현재 success_count: {success_count[0]}")
                    else:
                        logger.info(f"[저장단계 스킵] URL: {source_url}, change_status: {change_status} (new_or_changed가 아님)")
                else:
                    logger.warning(f"[저장단계 경고] url_result에 URL이 없음: {url_result}")
            else:
                logger.warning(f"[저장단계 경고] url_result가 None 또는 비어있음")
        
        # ✅ 배치 저장 완료 후 웹소켓 메시지 전송 (배치 단위로 한 번만)
        total_discovered_val = total_discovered_urls_ref[0] if total_discovered_urls_ref else 0
        count_val = total_new_or_changed[0] if total_new_or_changed else 0
        success_url_val = success_count[0]
        
        # ✅ total_discovered_val이 0이면 경고 로그 출력
        if total_discovered_val == 0:
            logger.error(f"[저장단계 심각한 오류] total_discovered가 0입니다! total_discovered_urls_ref가 제대로 전달되지 않았습니다.")
            logger.error(f"[저장단계 현재 값] count: {count_val}, success_url: {success_url_val}")
            
        # ✅ 강제 값 검증: 논리적 제약 확인 (total_discovered_val > 0인 경우만)
        if total_discovered_val > 0:
            if count_val > total_discovered_val:
                logger.error(
                    f"[강제 검증 - 저장단계] count({count_val}) > total_discovered({total_discovered_val}) 논리적 오류! count를 total_discovered로 강제 제한."
                )
                count_val = total_discovered_val
                if total_new_or_changed is not None:
                    total_new_or_changed[0] = total_discovered_val
            
            if success_url_val > count_val:
                logger.error(
                    f"[강제 검증 - 저장단계] success_url({success_url_val}) > count({count_val}) 논리적 오류! success_url을 count로 강제 제한."
                )
                success_url_val = count_val
                success_count[0] = count_val
            
            if success_url_val > total_discovered_val:
                logger.error(
                    f"[강제 검증 - 저장단계] success_url({success_url_val}) > total_discovered({total_discovered_val}) 논리적 오류! success_url을 total_discovered로 강제 제한."
                )
                success_url_val = total_discovered_val
                success_count[0] = total_discovered_val
        
        # ✅ 저장 단계: 배치 처리 완료 후 DB 업데이트 및 메시지 전송
        logger.info(f"[저장단계 메시지 전송] {len(batch)}개 URL 처리 완료, total_discovered: {total_discovered_val}, count: {count_val}, success_url: {success_url_val}")
        
        # 메시지 생성
        message = {
            "type": "crawl_count",
            "status": "running",
            "total_count": total_discovered_val,
            "collection_count": count_val,
            "save_count": success_url_val,
            "study_count": success_url_val,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # ✅ ASADAL_CRAWLING_LOG 즉시 업데이트 (배치 완료 시마다, 공통 함수 재사용)
        try:
            await update_crawling_log_db(job_id, dbname, total_discovered_val, count_val, success_url_val, success_url_val)
            logger.info(f"[저장 단계 DB 업데이트] job_id={job_id}, scan={total_discovered_val}, collection={count_val}, save={success_url_val}")
        except Exception as db_error:
            logger.error(f"[저장 단계 DB 업데이트 실패] job_id={job_id}, 오류={db_error}")
        
        # Redis SSE Pub/Sub 방식으로 진행 상황 전송
        await send_message_to_redis_sse(job_id, message, dbname=dbname)
        logger.info(f"[저장 단계] Redis SSE 메시지 전송 완료: 탐색={total_discovered_val}, 수집={count_val}, 저장={success_url_val}")
        
    except Exception as e:
        logger.error(f"[배치 저장 오류] {e}", exc_info=True)

# ✅ SSE 방식을 위한 주기적 DB 업데이트 함수
# ✅ 공통 DB 업데이트 함수 (재사용)
async def update_crawling_log_db(
    job_id: str,
    dbname: str,
    scan: int,
    collection: int,
    save: int,
    study: int
):
    """ASADAL_CRAWLING_LOG 테이블 업데이트 (공통 함수).
    
    Args:
        job_id: 작업 ID
        dbname: 계정명(데이터베이스 이름)
        scan: 탐색된 URL 수
        collection: 수집된 URL 수
        save: 저장된 URL 수
        study: 학습된 URL 수
    
    Raises:
        ValueError: 영향받은 행이 0개인 경우
    """
    query = """
    UPDATE ASADAL_CRAWLING_LOG 
    SET scan = %s, collection = %s, save = %s, study = %s
    WHERE job_id = %s
    LIMIT 1;
    """
    
    # ✅ dbname에 따라 MySQL/MariaDB 분기
    _name_lc = (dbname or '').strip().lower()
    if _name_lc in ('chatty', 'naraone'):
        from db.mysql_db_config import mysql_execute_query
        await mysql_execute_query(query, [scan, collection, save, study, job_id], fetch=False, dbname=dbname)
    else:
        await maria_execute_query(query, [scan, collection, save, study, job_id], fetch=False, dbname=dbname)
    
    logger.debug(f"[CRAWLING_LOG 업데이트] job_id={job_id}, scan={scan}, collection={collection}, save={save}, study={study}")


async def periodic_db_updater(
    job_id: str,
    dbname: str,
    interval: int,  # 업데이트 주기 (초)
    total_discovered_urls_final: list,
    total_new_or_changed: list,
    success_count: list,
    stop_event: asyncio.Event
):
    """주기적으로 ASADAL_CRAWLING_LOG 테이블 업데이트 및 SSE 메시지 발행 (SSE 방식 대응)"""
    logger.info(f"[주기적 DB 업데이트 시작] job_id={job_id}, 주기={interval}초")
    consecutive_errors = 0  # 연속 오류 카운터
    max_consecutive_errors = 3  # 최대 연속 오류 허용 횟수
    
    while not stop_event.is_set():
        try:
            await asyncio.sleep(interval)
            
            # 종료 이벤트 재확인 (sleep 후)
            if stop_event.is_set():
                break
            
            # ✅ 방안 1: Redis 완료 신호 확인 (가장 빠른 확인 방법)
            try:
                from db.db_redis import get_redis
                redis_client = await get_redis()
                stop_signal = await redis_client.get(f"crawl:{dbname}:{job_id}:stop_signal")
                if stop_signal:
                    logger.info(f"[주기적 DB 업데이트 종료] Redis 완료(중단) 신호 감지: job_id={job_id}")
                    # 최종 완료 메시지 발행
                    scan = total_discovered_urls_final[0]
                    collection = total_new_or_changed[0]
                    save = success_count[0]
                    final_message = {
                        "status": "cancelled",
                        "total_count": scan,
                        "collection_count": collection,
                        "save_count": save,
                        "study_count": save,
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    await send_message_to_redis_sse(job_id, final_message, dbname=dbname)
                    break
            except Exception as redis_check_error:
                logger.debug(f"[Redis 완료 신호 확인 실패] {redis_check_error}")
            
            # ✅ 방안 1: 작업 완료 상태 확인 (DB 조회)
            try:
                # ✅ dbname에 따라 MySQL/MariaDB 분기 처리 추가
                _name_lc = (dbname or '').strip().lower()
                check_query = """
                    SELECT status FROM ASADAL_CRAWLING_LOG 
                    WHERE job_id = %s LIMIT 1
                """
                if _name_lc in ('chatty', 'naraone'):
                    from db.mysql_db_config import mysql_execute_query
                    result = await mysql_execute_query(check_query, [job_id], fetch=True, dbname=dbname)
                else:
                    from db.maria_operations import maria_execute_query
                    result = await maria_execute_query(check_query, [job_id], fetch=True, dbname=dbname)
                
                if result and len(result) > 0:
                    db_status = result[0].get('status')
                    if db_status in ['ok', 'error', 'stop']:
                        logger.info(f"[주기적 DB 업데이트 종료] 작업 완료 감지: job_id={job_id}, status={db_status}")
                        # 최종 완료 메시지 발행
                        scan = total_discovered_urls_final[0]
                        collection = total_new_or_changed[0]
                        save = success_count[0]
                        final_status = "completed" if db_status == "ok" else ("cancelled" if db_status == "stop" else "error")
                        final_message = {
                            "status": final_status,
                            "total_count": scan,
                            "collection_count": collection,
                            "save_count": save,
                            "study_count": save,
                            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        await send_message_to_redis_sse(job_id, final_message, dbname=dbname)
                        break
            except Exception as db_check_error:
                # ✅ 에러를 WARNING으로 변경하여 문제를 더 명확히 표시
                logger.warning(f"[작업 상태 확인 실패] job_id={job_id}, dbname={dbname}, 오류={db_check_error}")
                # 에러가 발생해도 계속 진행 (다음 update_crawling_log_db는 시도)
            
            # 현재 진행 상황 가져오기
            scan = total_discovered_urls_final[0]
            collection = total_new_or_changed[0]
            save = success_count[0]
            
            # ✅ 공통 함수 호출
            await update_crawling_log_db(job_id, dbname, scan, collection, save, save)
            logger.info(f"[주기적 DB 업데이트] job_id={job_id}, scan={scan}, collection={collection}, save={save}, study={save}")
            
            # ✅ 탐색 단계 SSE 메시지 발행 (탐색만 진행 중일 때도 진행 상황 전송)
            try:
                sse_message = {
                    "status": "running",
                    "total_count": scan,
                    "collection_count": collection,
                    "save_count": save,
                    "study_count": save,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                await send_message_to_redis_sse(job_id, sse_message, dbname=dbname)
                logger.debug(f"[탐색 단계 SSE 발행] job_id={job_id}, scan={scan}, collection={collection}, save={save}")
            except Exception as sse_error:
                logger.warning(f"[탐색 단계 SSE 발행 실패] job_id={job_id}, 오류={sse_error}")
            
            consecutive_errors = 0  # 성공 시 오류 카운터 리셋
            
        except asyncio.CancelledError:
            logger.info(f"[주기적 DB 업데이트 취소] job_id={job_id}")
            break
        except ValueError as e:
            error_msg = str(e)
            # ✅ 영향받은 행이 0개인 경우 (크롤링 완료로 인한 최종 UPDATE 후 재시도) → 정상 종료
            if "영향받은 행이 0개" in error_msg:
                logger.info(f"[주기적 DB 업데이트 종료] job_id={job_id} 크롤링 완료로 인한 최종 업데이트 완료, 주기적 업데이트 중지")
                break
            # ✅ job_id가 DB에 없음 (레코드 삭제됨) → 즉시 종료
            logger.warning(f"[주기적 DB 업데이트 종료] job_id={job_id}에 대한 레코드가 없음, 업데이트 중지")
            break
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"[주기적 DB 업데이트 오류] job_id={job_id}, 오류={e}, 연속 오류 횟수={consecutive_errors}/{max_consecutive_errors}")
            
            # ✅ 연속 오류가 임계값 초과 시 루프 종료
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"[주기적 DB 업데이트 중단] job_id={job_id}, 연속 {consecutive_errors}회 오류 발생으로 종료")
                break
    
    logger.info(f"[주기적 DB 업데이트 완전 종료] job_id={job_id}")


# ✅ 모든 도메인 공통: DB에서 탐색된 URL 목록 조회
async def fetch_exploration_urls_from_db(
    chat_bot_id: str,
    target_dbname: str
) -> List[Dict]:
    """
    ASADAL_CRAWLING_EXPLORATION 테이블에서 URL 목록 조회
    - type='page' 조건으로 조회 (is_active 0과 1 모두 포함)
    - 개발 서버: 한 테이블에 여러 도메인 데이터가 섞여있어서 chat_bot_id로 필터링 필요
    - 일반 모드: 각 도메인별 DB에서 조회
    
    Args:
        chat_bot_id: 챗봇 ID (해당 챗봇의 URL만 조회)
        target_dbname: 저장 대상 DB 이름 (각 도메인별 DB)
    
    Returns:
        List[Dict]: URL 정보 목록 [{"url": "...", "learn_list_id": ...}, ...]
    """
    try:
        # ✅ DB 선택
        # 개발 서버: dev_user DB에서 항상 조회
        # 운영 서버: 각 관리자 페이지별 DB 사용 (예: anseong DB)
        if target_dbname and target_dbname.strip().lower() == 'dev_user':
            exploration_dbname = 'dev_user'
            logger.info(f"[개발 서버 모드] dev_user DB에서 ASADAL_CRAWLING_EXPLORATION 테이블 조회")
        else:
            # 운영 서버: 각 도메인별 DB 사용 (target_dbname 그대로 사용, 예: anseong)
            exploration_dbname = target_dbname
            logger.info(f"[운영 서버 모드] {exploration_dbname} DB에서 ASADAL_CRAWLING_EXPLORATION 테이블 조회")
        
        logger.info(f"[DB 탐색] ASADAL_CRAWLING_EXPLORATION 테이블에서 URL 조회 시작")
        logger.info(f"[DB 연결] dbname: {exploration_dbname}, chat_bot_id: {chat_bot_id}")
        
        # DB 종류 확인 (MySQL 또는 MariaDB)
        name_lc = (exploration_dbname or '').strip().lower()
        if name_lc in ('chatty', 'naraone'):
            from db.mysql_db_config import MYSQL_DatabasePool, mysql_execute_query
            await MYSQL_DatabasePool.get_pool(exploration_dbname)
            execute_query = mysql_execute_query
            db_tag = 'MySQL'
        else:
            from db.maria_db_config import DatabasePool as MARIADB_DatabasePool; from db.maria_operations import maria_execute_query
            await MARIADB_DatabasePool.get_pool(exploration_dbname)
            execute_query = maria_execute_query
            db_tag = 'MariaDB'
        
        # URL 목록 조회: chat_bot_id + type='page' 조건
        # 개발 서버: 한 테이블에 여러 도메인 데이터가 섞여있어서 chat_bot_id로 필터링 필요
        query = """
            SELECT url, learn_list_id, created_at
            FROM ASADAL_CRAWLING_EXPLORATION
            WHERE chat_bot_id = %s
              AND type = 'page'
            ORDER BY id ASC
        """
        
        rows = await execute_query(query, [chat_bot_id], fetch=True, dbname=exploration_dbname)
        
        if not rows:
            logger.warning(f"[DB 탐색] {db_tag} DB({exploration_dbname})에서 URL을 찾지 못함. chat_bot_id: {chat_bot_id}")
            return []
        
        url_list = []
        for row in rows:
            url_list.append({
                "url": row.get("url", ""),
                "learn_list_id": row.get("learn_list_id"),
                "created_at": row.get("created_at"),
            })

        logger.info(f"[DB 탐색] {db_tag} DB({exploration_dbname})에서 {len(url_list)}개 URL 조회 완료")
        return url_list
        
    except Exception as e:
        logger.error(f"[DB 탐색] DB 조회 오류: {type(e).__name__}: {str(e)}")
        logger.error(f"[스택 트레이스]\n{traceback.format_exc()}")
        return []
    

# ✅ ASADAL_CRAWLING_WEBSUB 테이블에서 도메인별 block 태그 조회 (도메인별 DB 지원)
async def fetch_subject_block_from_db(domain: str, chat_bot_id: str, target_dbname: str = None) -> Optional[str]:
    """
    ASADAL_CRAWLING_WEBSUB 테이블에서 도메인별 block(제목 추출 태그) 조회
    - 개발 서버: dev_user DB에서 항상 조회
    - 운영 서버: 각 도메인별 DB에서 조회 (target_dbname 사용)
    
    Args:
        domain: 도메인 (예: "seocho.go.kr")
        chat_bot_id: AI봇 ID
        target_dbname: 저장 대상 DB 이름 (각 도메인별 DB, None이면 dev_user 사용)
        
    Returns:
        block 값 (태그 정보) 또는 None
    """
    try:
        # ✅ DB 선택
        # 개발 서버: dev_user DB에서 항상 조회
        # 운영 서버: 각 도메인별 DB 사용 (예: anseong DB)
        if target_dbname and target_dbname.strip().lower() == 'dev_user':
            block_dbname = 'dev_user'
            logger.info(f"[개발 서버 모드] dev_user DB에서 ASADAL_CRAWLING_WEBSUB 테이블 조회")
        else:
            # 운영 서버: 각 도메인별 DB 사용 (target_dbname 그대로 사용, 예: anseong)
            # target_dbname이 None이면 dev_user 사용 (기본값)
            block_dbname = target_dbname if target_dbname else 'dev_user'
            logger.info(f"[운영 서버 모드] {block_dbname} DB에서 ASADAL_CRAWLING_WEBSUB 테이블 조회")
        
        logger.info(f"[제목 추출 태그 조회] domain={domain}, chat_bot_id={chat_bot_id}, dbname={block_dbname}")
        
        # DB 종류 확인 (MySQL 또는 MariaDB)
        name_lc = (block_dbname or '').strip().lower()
        if name_lc in ('chatty', 'naraone'):
            from db.mysql_db_config import MYSQL_DatabasePool, mysql_execute_query
            await MYSQL_DatabasePool.get_pool(block_dbname)
            execute_query = mysql_execute_query
            db_tag = 'MySQL'
        else:
            from db.maria_db_config import DatabasePool as MARIADB_DatabasePool; from db.maria_operations import maria_execute_query
            await MARIADB_DatabasePool.get_pool(block_dbname)
            execute_query = maria_execute_query
            db_tag = 'MariaDB'
        
        # 도메인 정규화 (www 제거)
        normalized_domain = normalize_domain(domain)
        
        # ASADAL_CRAWLING_WEBSUB 테이블에서 조회
        query = """
            SELECT block
            FROM ASADAL_CRAWLING_WEBSUB
            WHERE domain = %s
              AND chat_bot_id = %s
            LIMIT 1
        """
        
        rows = await execute_query(query, [normalized_domain, chat_bot_id], fetch=True, dbname=block_dbname)
        
        if rows and len(rows) > 0:
            block = rows[0].get('block', '')
            if block:
                logger.info(f"[제목 추출 태그 조회 성공] domain={normalized_domain}, block={block}, dbname={block_dbname}")
                return block
            else:
                logger.info(f"[제목 추출 태그 조회] domain={normalized_domain}, block이 비어있음, dbname={block_dbname}")
                return None
        else:
            logger.info(f"[제목 추출 태그 조회] domain={normalized_domain}, chat_bot_id={chat_bot_id}에 해당하는 레코드 없음, dbname={block_dbname}")
            return None
            
    except Exception as e:
        logger.error(f"[제목 추출 태그 조회 오류] domain={domain}, chat_bot_id={chat_bot_id}, dbname={target_dbname}, 오류: {type(e).__name__}: {str(e)}")
        logger.error(f"[스택 트레이스]\n{traceback.format_exc()}")
        return None


# ✅ 병렬 크롤링 및 처리 함수 (하이브리드 스트리밍 구조)
async def crawl_and_process_url_parallel(
    start_url: str,
    subject: str,
    context: CrawlingContext,
    each_progress: float,
    memo: str = "",
    max_depth: int = 10,
    max_tasks: int = 4,  # ✅ 20 → 4로 변경 (탐색 4개 + 변경 감지 4개 + 저장 4개 = 총 12개)
    max_crawl_urls: int = 10000,  # 크롤링 테스트용 URL 개수 제한
) -> dict:
    try:
        # Context에서 값 추출
        table_name = context.table_name
        dbname = context.dbname
        job_id = context.job_id
        job_manager = context.job_manager
        job_progress_manager = context.job_progress
        chat_bot_id = context.chat_bot_id
        
        logger.info(f"[✅ 하이브리드 스트리밍 크롤링 시작] URL: {start_url}")
        logger.info(f"[성능 향상 포인트] 스트리밍 파이프라인, 배치 크기: 변경감지={CHANGE_DETECTION_BATCH_SIZE}, 저장={DB_SAVE_BATCH_SIZE}, 임베딩={CRAWLING_EMBEDDING_BATCH_SIZE}")

        # ✅ DB별 사용자 설정 max_crawl_urls 조회 (MySQL/MariaDB 분기)
        _name_lc = (dbname or '').strip().lower()
        _db_tag = 'MYSQL' if _name_lc in ('chatty', 'naraone') else 'MARIADB'
        logger.info(f"[🔍 |{_db_tag}|DB 접속 시도] dbname: '{dbname}', chat_bot_id: '{chat_bot_id}'")

        # ✅ stop_count 기본값 설정 (10초)
        stop_count = 10
        
        # dbname 검증
        if not dbname:
            logger.error(f"[❌ |{_db_tag}|DB 접속 실패] dbname이 None 또는 빈값입니다. dbname: {dbname}")
            logger.info(f"[기본값 사용] max_crawl_urls: {max_crawl_urls}개, stop_count: {stop_count}초")
        elif not chat_bot_id:
            logger.error(f"[❌ |{_db_tag}|DB 접속 실패] chat_bot_id가 None 또는 빈값입니다. chat_bot_id: {chat_bot_id}")
            logger.info(f"[기본값 사용] max_crawl_urls: {max_crawl_urls}개, stop_count: {stop_count}초")
        else:
            try:
                # 풀 생성 분기
                if _db_tag == 'MYSQL':
                    from db.mysql_db_config import MYSQL_DatabasePool as _Pool
                else:
                    from db.maria_db_config import MARIADB_DatabasePool as _Pool

                logger.info(f"[🔌 |{_db_tag}|DB 연결풀 생성] get_pool('{dbname}') 시도")
                await _Pool.get_pool(dbname)
                await _Pool.release_unused_pools()
                logger.info(f"[✅ |{_db_tag}|DB 연결풀 성공] dbname: '{dbname}'")

                # 4개 키 모두 조회 후, 시간대에 따라 week_count/page_count 선택
                config_map = await _fetch_crawling_config_map(dbname, chat_bot_id)
                if config_map:
                    crawl_settings = _decide_max_crawl_urls_by_time(config_map, max_crawl_urls)
                    max_crawl_urls = crawl_settings.get('max_crawl_urls', 100)
                    stop_count = int(crawl_settings.get('stop_count', 10))  # ✅ stop_count 설정 적용
                    max_jobs = int(crawl_settings.get('conc_count', 1))
                    sub_change_mode_on = str(crawl_settings.get('sub_change', 'off'))
                    logger.info(f"[✅ |{_db_tag}| 설정 적용] max_crawl_urls: {max_crawl_urls}개, stop_count: {stop_count}초, max_jobs: {max_jobs}개, sub_change_mode_on: {sub_change_mode_on}")
                else:
                    logger.warning(f"[⚠️ |{_db_tag}|DB 데이터 없음] ASADAL_CRAWLING_CONFIG에서 chat_bot_id='{chat_bot_id}' 설정 미발견")
                    logger.info(f"[|{_db_tag}| 기본값 설정 적용] max_crawl_urls: {max_crawl_urls}개, stop_count: {stop_count}초, max_jobs: {max_jobs}개")

            except Exception as e:
                logger.error(f"[❌ |{_db_tag}|DB 설정 오류] 상세 오류: {type(e).__name__}: {str(e)}")
                logger.error(f"[|{_db_tag}|오류 위치] dbname='{dbname}', chat_bot_id='{chat_bot_id}'")
                logger.info(f"[|{_db_tag}| 기본값 설정 적용] max_crawl_urls: {max_crawl_urls}개, stop_count: {stop_count}초, max_jobs: {max_jobs}개")
                logger.error(f"[|{_db_tag}|스택 트레이스]\n{traceback.format_exc()}")

        # ✅ 하이브리드 스트리밍 구조: 큐 및 워커 초기화
        collection_queue = asyncio.Queue()  # 수집 큐
        change_detection_queue = asyncio.Queue()  # 변경 감지 큐
        stop_signal = CrawlStopSignal()  # 글로벌 중단 신호
        start_time = time.time()  # 처리 시작 시간

        # ✅ use_crawl_stop 모니터링 태스크 (DB 우회 경로 포함)
        async def _monitor_use_crawl_stop() -> None:
            if not job_id or not job_manager:
                return
            while True:
                try:
                    status = await job_manager.get_job_status(job_id)
                    if status == "use_crawl_stop":
                        stop_signal.set_stop("use_crawl_stop")
                        return
                    await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.warning(f"[use_crawl_stop 모니터링 실패] job_id={job_id}, 오류={e}")
                    await asyncio.sleep(1)

        stop_monitor_task = asyncio.create_task(_monitor_use_crawl_stop())
        
        # 상태 추적 변수 (리스트로 참조 전달)
        processed_urls = {}
        processed_urls_lock = asyncio.Lock()  # ✅ 중복 체크를 위한 Lock 추가
        total_chunks_processed = [0]
        completed_count = [0]
        success_count = [0]
        total_new_or_changed = [0]  # 변경 감지 완료 후 업데이트됨
        total_discovered_urls_final = [0]  # ✅ 탐색 완료 시점의 최종 값 저장
        total_discovered_urls = 0  # ✅ 탐색 완료 후 설정됨 (crawl_website 결과)
        
        # ✅ SSE 방식을 위한 주기적 DB 업데이트 태스크 시작
        stop_event = asyncio.Event()
        update_interval = 5  # 5초마다 업데이트 (운영 환경)
        
        periodic_update_task = asyncio.create_task(
            periodic_db_updater(
                job_id, dbname, update_interval,
                total_discovered_urls_final, total_new_or_changed, success_count,
                stop_event
            )
        )
        logger.info(f"[주기적 DB 업데이트 태스크 시작] job_id={job_id}, 주기={update_interval}초")
        
        # ✅ 변경 감지 워커 4개 시작 (병렬 처리 향상)
        change_detection_tasks = []
        chat_bot_id = context.chat_bot_id
        for i in range(4):
            task = asyncio.create_task(
                change_detection_worker(
                    collection_queue, change_detection_queue, table_name, dbname,
                    job_id, job_manager, stop_signal, total_new_or_changed, total_discovered_urls_final, success_count,
                    chat_bot_id=chat_bot_id
                )
            )
            change_detection_tasks.append(task)
        
        # ✅ 저장 워커 4개 시작 (병렬 처리 향상)
        save_tasks = []
        for i in range(4):
            task = asyncio.create_task(
                save_worker(
                    change_detection_queue, subject, context,
                    memo, each_progress,
                    stop_signal, processed_urls, total_chunks_processed, completed_count,
                    success_count, total_new_or_changed, start_time,
                    total_discovered_urls_ref=total_discovered_urls_final,  # ✅ 탐색 완료 시점의 값을 전달하기 위한 참조
                    processed_urls_lock=processed_urls_lock,  # ✅ Lock 전달
                )
            )
            save_tasks.append(task)
        
        logger.info("[하이브리드 스트리밍] 변경 감지 워커 4개, 저장 워커 4개 시작 완료")

        # ✅ 크롤링 시작 시간 기록 (stop_count 시간 제한 체크용)
        crawl_start_time = time.time()
        logger.info(f"[⏱️ 크롤링 시간 제한] 시작 시간: {crawl_start_time}, 최대 허용 시간: {stop_count}초")
        
        # ✅ 모든 URL에 대해 DB 조회 시도
        url_filter = context.url_filter or ""
        
        # 개발 서버: dev_user DB에서 탐색된 URL 목록 조회
        # 개발 서버에서는 DB에서 URL을 가져와서 사용 (직접 탐색하지 않음)
        logger.info(f"[🔍 DB 탐색 시도] start_url: {start_url}, chat_bot_id={chat_bot_id}, dbname={dbname}")
        
        # ✅ URL 도메인 기반으로 실제 chat_bot_id 찾기 (개발 서버 + 운영 서버 모두 적용)
        # 개발 서버: 한 테이블에 여러 도메인 데이터가 섞여있어서 필요
        # 운영 서버: 요청의 chat_bot_id가 잘못될 수 있어서 URL 도메인 기반으로 찾는 것이 더 안전
        actual_chat_bot_id = chat_bot_id
        try:
            from urllib.parse import urlparse
            parsed_url = urlparse(start_url)
            domain = parsed_url.netloc or parsed_url.path.split('/')[0] if parsed_url.path else ''
            # www. 제거
            domain = domain.replace('www.', '')
            
            logger.info(f"[URL 도메인 기반 chat_bot_id 찾기] domain={domain}, dbname={dbname}")
            
            # ✅ fetch_exploration_urls_from_db와 동일한 DB 선택 로직 적용
            # 개발 서버: dev_user DB에서 항상 조회
            # 운영 서버: 각 관리자 페이지별 DB 사용 (예: gangnamingang DB)
            if dbname and dbname.strip().lower() == 'dev_user':
                find_dbname = 'dev_user'
                logger.info(f"[개발 서버 모드] dev_user DB에서 chat_bot_id 조회")
            else:
                # 운영 서버: 각 도메인별 DB 사용 (dbname 그대로 사용, 예: gangnamingang)
                find_dbname = dbname
                logger.info(f"[운영 서버 모드] {find_dbname} DB에서 chat_bot_id 조회")
            
            # DB 종류 확인
            name_lc = (find_dbname or '').strip().lower()
            if name_lc in ('chatty', 'naraone'):
                from db.mysql_db_config import mysql_execute_query as find_exec_query
            else:
                from db.maria_operations import maria_execute_query as find_exec_query
            
            # start_url의 도메인과 매칭되는 chat_bot_id 찾기
            # 운영 서버: 각 DB에 해당 도메인 데이터만 있으므로 해당 DB에서 조회
            # 개발 서버: dev_user DB에서 조회
            find_chatbot_query = """
                SELECT DISTINCT chat_bot_id
                FROM ASADAL_CRAWLING_EXPLORATION
                WHERE url LIKE %s
                  AND type = 'page'
                LIMIT 1
            """
            domain_pattern = f"%{domain}%"
            find_result = await find_exec_query(find_chatbot_query, [domain_pattern], fetch=True, dbname=find_dbname)
            
            if find_result and len(find_result) > 0:
                actual_chat_bot_id = find_result[0].get('chat_bot_id')
                logger.info(f"[URL 도메인 기반 chat_bot_id 찾기 성공] {chat_bot_id} → {actual_chat_bot_id} (dbname={find_dbname})")
            else:
                logger.warning(f"[URL 도메인 기반 chat_bot_id를 찾지 못함] 원본 chat_bot_id 사용: {chat_bot_id} (dbname={find_dbname})")
        except Exception as e:
            logger.warning(f"[URL 도메인 기반 chat_bot_id 찾기 실패] {e}, 원본 chat_bot_id 사용: {chat_bot_id}")
        
        exploration_urls = await fetch_exploration_urls_from_db(actual_chat_bot_id, dbname)
        
        if exploration_urls:
            # ✅ 2번 방식: DB 조회 결과 사용 (탐색 단계 건너뛰기)
            logger.info(f"[🚀 DB 탐색 성공] DB에서 {len(exploration_urls)}개 URL 조회 완료")
            
            # 조회된 URL을 collection_queue에 직접 투입
            logger.info("[DB 탐색] collection_queue 투입 시작")
            for url_info in exploration_urls:
                url = url_info.get("url", "")
                if url:
                    # 기존 crawl_website가 전달하는 형식과 동일하게 맞춤
                    result = {
                        "source": url,
                        "content": "",  # 변경감지 워커에서 실제 콘텐츠를 가져옴
                        "title": "",
                        "snippet": "",
                        "page_snippet": "",
                        "favicon_url": "",
                        "url_metadata": {},
                    }
                    await collection_queue.put(result)
            
            total_discovered_urls = len(exploration_urls)
            total_discovered_urls_final[0] = total_discovered_urls
            crawl_data = {"total_discovered_urls": total_discovered_urls}
            
            logger.info(f"[DB 탐색] total_count 갱신 완료: {total_discovered_urls}")
            logger.info(f"[DB 탐색] collection_queue 투입 완료: {total_discovered_urls}개 URL")
        else:
            # ✅ DB에서 탐색 목록이 비어있으면 크롤링하지 않고 0개로 종료
            logger.warning(f"[❌ DB 탐색 실패] {dbname} DB에서 URL을 찾지 못했습니다. 크롤링을 시작하지 않습니다.")
            logger.warning(f"[❌ DB 탐색 실패] ASADAL_CRAWLING_EXPLORATION 테이블에 데이터가 있는지 확인하세요. chat_bot_id: {chat_bot_id}")
            total_discovered_urls = 0
            total_discovered_urls_final[0] = 0
            crawl_data = {"total_discovered_urls": 0}
        
        # ✅ 크롤링 완료 대기 (탐색 워커가 collection_queue에 모든 항목을 추가할 때까지)
        logger.info("[하이브리드 스트리밍] 탐색 완료, collection_queue 처리 대기 중...")
        
        # ✅ collection_queue가 완전히 비워질 때까지 대기 (변경 감지 워커가 모두 처리할 때까지)
        max_wait_time = 60  # 최대 60초 대기
        wait_interval = 0.5  # 0.5초마다 확인
        elapsed = 0
        while elapsed < max_wait_time:
            queue_size = collection_queue.qsize()
            if queue_size == 0:
                logger.info("[하이브리드 스트리밍] collection_queue 비었음, 변경 감지 워커 종료 신호 전송")
                break
            logger.debug(f"[하이브리드 스트리밍] collection_queue 대기 중: {queue_size}개")
            await asyncio.sleep(wait_interval)
            elapsed += wait_interval
        
        if elapsed >= max_wait_time:
            logger.warning(f"[하이브리드 스트리밍] collection_queue 타임아웃 (대기 시간: {elapsed}초), 강제 종료 진행")
        
        # ✅ 크롤링 완료 신호: collection_queue에 None 전달하여 변경 감지 워커 4개 종료
        for i in range(4):
            await collection_queue.put(None)  # 종료 신호 4개
        
        # ✅ 변경 감지 워커 4개 완료 대기
        logger.info("[하이브리드 스트리밍] 변경 감지 워커 4개 완료 대기")
        await asyncio.gather(*change_detection_tasks)
        
        # ✅ change_detection_queue가 완전히 비워질 때까지 대기 (저장 워커가 모두 처리할 때까지)
        logger.info("[하이브리드 스트리밍] 변경 감지 완료, change_detection_queue 처리 대기 중...")
        max_wait_time = 60  # 최대 60초 대기
        elapsed = 0
        while elapsed < max_wait_time:
            queue_size = change_detection_queue.qsize()
            if queue_size == 0:
                logger.info("[하이브리드 스트리밍] change_detection_queue 비었음, 저장 워커 종료 신호 전송")
                break
            logger.debug(f"[하이브리드 스트리밍] change_detection_queue 대기 중: {queue_size}개")
            await asyncio.sleep(wait_interval)
            elapsed += wait_interval
        
        if elapsed >= max_wait_time:
            logger.warning(f"[하이브리드 스트리밍] change_detection_queue 타임아웃 (대기 시간: {elapsed}초), 강제 종료 진행")
        
        # ✅ 변경 감지 완료 신호: change_detection_queue에 None 전달하여 저장 워커 4개 종료
        for i in range(4):
            await change_detection_queue.put(None)  # 종료 신호 4개
        
        # ✅ 저장 워커 4개 완료 대기
        logger.info("[하이브리드 스트리밍] 저장 워커 4개 완료 대기")
        await asyncio.gather(*save_tasks)

        # ✅ 중단 모니터 태스크 종료
        if stop_monitor_task:
            stop_monitor_task.cancel()
            await asyncio.gather(stop_monitor_task, return_exceptions=True)
        
        logger.info(f"[하이브리드 스트리밍 완료] 처리된 URL: {completed_count[0]}개, 성공: {success_count[0]}개, 청크: {total_chunks_processed[0]}개")
        
        # ✅ 빈 결과 처리
        if completed_count[0] == 0:
            logger.info(f"[처리 건너뛰기] 수집된 URL이 없어 처리 단계를 건너뜁니다: {start_url}")
            return {
                "total_discovered_urls": total_discovered_urls_final[0],  # ✅ 탐색 값은 반드시 포함
                "total_crawled_urls": total_discovered_urls_final[0],
                "total_processed_urls": 0,
                "total_new_or_changed_urls": 0,
                "new_or_changed_count": 0,
                "no_change_count": 0,
                "success_url": 0,
                "total_chunks": 0,
                "chunk_count": [],
                "use_source": [],
                "source_size": [],
                "web_title": [],
                "processing_time": 0.0,
                "chunk_hash": []
            }
            
        # ✅ 결과 수집 및 반환
        processing_time = round(time.time() - start_time, 2)
        
        
        # processed_urls에서 결과 추출
        use_source_list = []
        chunk_count_list = []
        source_size_list = []
        web_title_list = []
        chunk_hash_list = []
        
        for url, result in processed_urls.items():
            if result:
                use_source_list.append(url)
                chunk_count_list.append(result.get("chunks", 0))
                source_size_list.append(result.get("source_size", [0]))
                web_title_list.append(result.get("title", ""))
                chunk_hash_list.append(result.get("chunk_hash", ""))
        
        # ✅ total_discovered_urls_final 값으로 동기화 (정확한 탐색 카운트)
        total_discovered_urls = total_discovered_urls_final[0]
        
        # ✅ 최종 통계 로그 출력
        logger.info("=" * 80)
        logger.info(f"📊 [크롤링 최종 통계]")
        logger.info(f"   - 탐색 완료 (total_discovered): {total_discovered_urls} 개")
        logger.info(f"   - 변경 감지 완료 (count): {total_new_or_changed[0]} 개")
        logger.info(f"   - DB 저장 완료 (success_url): {success_count[0]} 개")
        logger.info(f"   - processed_urls 딕셔너리 크기: {len(processed_urls)} 개")
        logger.info(f"   - 처리 시간: {processing_time:.2f} 초")
        logger.info("=" * 80)
        
        # ✅ 중복 분석: total_discovered와 processed_urls 크기가 다르면 경고
        if len(processed_urls) != total_discovered_urls:
            logger.warning(f"⚠️ [중복 의심!] 탐색 URL({total_discovered_urls})과 processed_urls({len(processed_urls)})가 다릅니다!")
            logger.warning(f"   - 차이: {total_discovered_urls - len(processed_urls)} 개")
        
        # ✅ 중복 분석: success_url과 processed_urls 크기가 다르면 경고
        if success_count[0] != len(processed_urls):
            logger.warning(f"⚠️ [DB 저장 불일치!] success_url({success_count[0]})과 processed_urls({len(processed_urls)})가 다릅니다!")
            logger.warning(f"   - 차이: {success_count[0] - len(processed_urls)} 개")
        
        # ✅ 최종 진행률 전송 (progress 타입 주석 처리)
        await send_message_to_socket(
            job_id,
            {
                "type": "progress",
                "status": "info",
                "message": "completed url list",
                "progress": 95,
                "completed_urls": use_source_list,
                "total_new_or_changed_urls": total_new_or_changed[0],
                "chunk_count": chunk_count_list,
                "source_size": source_size_list,
                "total_processed_urls": len(use_source_list),
                "total_chunks": total_chunks_processed[0],
                "processing_time": processing_time,
                "no_change_count": completed_count[0] - success_count[0],
                "total_crawled_urls": total_discovered_urls,
                "chunk_hash": chunk_hash_list,
                "timestamp": time.time(),
            },
            job_manager,
        )

        # ✅ 주기적 DB 업데이트 태스크 중지
        logger.info(f"[주기적 DB 업데이트 태스크 중지 시작] job_id={job_id}")
        stop_event.set()
        try:
            await asyncio.wait_for(periodic_update_task, timeout=3.0)
            logger.info(f"[주기적 DB 업데이트 태스크 중지 완료] job_id={job_id}")
        except asyncio.TimeoutError:
            logger.warning(f"[주기적 DB 업데이트 태스크 타임아웃] job_id={job_id}, 강제 취소")
            periodic_update_task.cancel()

        return {
            "total_processed_urls": len(use_source_list),
            "total_chunks": total_chunks_processed[0],
            "chunk_count": chunk_count_list,
            "total_new_or_changed_urls": total_new_or_changed[0],
            "use_source": use_source_list,
            "source_size": source_size_list,
            "web_title": web_title_list,
            "chunk_hash": chunk_hash_list,
            "processing_time": processing_time,
            "no_change_count": completed_count[0] - success_count[0],
            "new_or_changed_count": success_count[0],
            "total_discovered_urls": total_discovered_urls,
            "total_crawled_urls": total_discovered_urls,
            "success_url": success_count[0]
        }

    except Exception as e:
        error_message = f"병렬 크롤링 중 오류 발생: {str(e)}"
        logger.error(f"[병렬 크롤링 오류] {error_message}", exc_info=True)
        
        await send_message_to_socket(
            job_id,
            {
                "type": "error",
                "status": "error",
                "message": error_message,
                "timestamp": time.time(),
            },
            job_manager,
        )
        raise
    
    finally:
        # ✅ 예외 발생 여부와 관계없이 모든 태스크 정리 (GeneratorExit 방지)
        logger.info(f"[태스크 정리 시작] job_id={job_id}")
        
        # 주기적 DB 업데이트 태스크 정리
        try:
            if 'stop_event' in locals():
                stop_event.set()
            if 'periodic_update_task' in locals() and not periodic_update_task.done():
                periodic_update_task.cancel()
                try:
                    await asyncio.wait_for(periodic_update_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        except Exception as e:
            logger.warning(f"[주기적 DB 업데이트 태스크 정리 오류] {e}")
        
        # 변경 감지 워커 정리
        try:
            if 'change_detection_tasks' in locals():
                for task in change_detection_tasks:
                    if not task.done():
                        task.cancel()
                if change_detection_tasks:
                    await asyncio.gather(*change_detection_tasks, return_exceptions=True)
        except Exception as e:
            logger.warning(f"[변경 감지 워커 정리 오류] {e}")
        
        # 저장 워커 정리
        try:
            if 'save_tasks' in locals():
                for task in save_tasks:
                    if not task.done():
                        task.cancel()
                if save_tasks:
                    await asyncio.gather(*save_tasks, return_exceptions=True)
        except Exception as e:
            logger.warning(f"[저장 워커 정리 오류] {e}")
        
        # 중단 모니터 태스크 정리
        try:
            if 'stop_monitor_task' in locals() and not stop_monitor_task.done():
                stop_monitor_task.cancel()
                try:
                    await asyncio.wait_for(stop_monitor_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        except Exception as e:
            logger.warning(f"[중단 모니터 태스크 정리 오류] {e}")
        
        logger.info(f"[태스크 정리 완료] job_id={job_id}")

# ✅ 기존 순차 처리 함수는 백업으로 유지 (필요시 사용)
# ✅ 개선된 타임아웃 및 에러 처리가 적용된 URL 콘텐츠 가져오기 함수
async def fetch_url_content(session, url, smart_queue=None, stop_signal: CrawlStopSignal = None):
    """개선된 타임아웃 및 에러 처리가 적용된 URL 콘텐츠 가져오기 함수 (메타데이터 포함)
    - HTTPS 우선 시도, 실패 시 HTTP 시도
    """
    # URL 스키마가 없는 경우 'https://' 추가
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    
    # HTTPS 우선 시도, 실패 시 HTTP 시도
    urls_to_try = []
    if url.startswith("https://"):
        urls_to_try = [url, url.replace("https://", "http://")]
    elif url.startswith("http://"):
        urls_to_try = [url.replace("http://", "https://"), url]
    else:
        urls_to_try = [url]
    
    last_error = None
    for attempt_url in urls_to_try:
        try:
            # 각 URL에 대해 기존 로직 실행
            result = await _fetch_single_url(session, attempt_url, smart_queue, stop_signal)
            if result:
                return result
        except Exception as e:
            last_error = e
            logger.debug(f"[URL 시도 실패] {attempt_url}: {e}")
            continue
    
    # 모든 시도 실패
    if last_error:
        logger.warning(f"[URL 모든 시도 실패] {url}: {last_error}")
    return None

async def _fetch_single_url(session, url, smart_queue=None, stop_signal: CrawlStopSignal = None):
    """단일 URL에 대한 콘텐츠 가져오기 (내부 함수)"""
    
    # URL 확장자로 XML/JSON 필터링
    url_lower = url.lower()
    if any(url_lower.endswith(ext) for ext in ['.xml', '.json', '.rss', '.atom']):
        logger.info(f"[크롤링 제외] XML/JSON 확장자 스킵: {url}")
        return None

    # ✅ 정부 사이트 감지 (크롤링용) - endswith로 정확한 도메인 매칭
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.lower()
    is_government_site = any(domain.endswith(gov_domain) for gov_domain in [
        '.go.kr', '.gov.kr', '.seoul.kr', 'mois.go.kr', 'sd.go.kr', 'korea.kr', 'childfund.or.kr',
        'kdi.re.kr', 'kostat.go.kr', 'nia.or.kr', 'nipa.kr', 'nowon.kr'
    ]) or any(gov_keyword in domain for gov_keyword in ['government.', 'public.'])
    
    if is_government_site:
        # ✅ Playwright 시작 전 중단 신호 확인
        if stop_signal and stop_signal.is_stopped():
            logger.info(f"[🛑⚡ Playwright 시작 전 중단 신호 감지] 처리 중단 - URL: {url}")
            return None
        
        # 정부 사이트는 Playwright 사용 (3회 재시도)
        logger.info(f"[크롤링 정부 사이트 감지] Playwright 사용: {url}")
        
        # ✅ 플레이라이트 작업 시작 추적
        if smart_queue:
            await smart_queue.start_playwright_task()
        
        try:
            # Playwright 재시도 로직 (브라우저 크래시 대비)
            for playwright_attempt in range(3):
                try:
                    html_content = await fetch_page_with_timeout(url, 0, timeout=30, stop_signal=stop_signal)  # ✅ stop_signal 전달
                    
                    if html_content:
                        return {
                            'html': html_content,
                            'headers': {}
                        }
                    else:
                        logger.warning(f"[크롤링 Playwright 실패] URL: {url} - HTML 콘텐츠가 None 반환됨 (시도 {playwright_attempt + 1})")
                        if playwright_attempt < 2:
                            await asyncio.sleep(1)  # 1초 대기 후 재시도
                            continue
                except Exception as e:
                    logger.error(f"[크롤링 Playwright 오류] URL: {url}, 시도 {playwright_attempt + 1}, 오류: {e}", exc_info=True)
                    if playwright_attempt < 2:
                        await asyncio.sleep(2)  # 2초 대기 후 재시도
                        continue
            
        finally:
            # ✅ 플레이라이트 작업 완료 추적 (성공/실패 무관하게)
            if smart_queue:
                await smart_queue.end_playwright_task()
        
        # 모든 Playwright 시도 실패 시 HTTP 폴백
        logger.info(f"[크롤링 HTTP 폴백] 모든 Playwright 시도 실패로 HTTP 방식 시도: {url}")

    max_retries = 2  # 크롤링에서는 2번만 재시도 (성능 고려)

    for retry_count in range(max_retries):
        try:

            # ✅ 개선된 헤더 (정부 사이트 호환성 향상)
            headers = {
                "User-Agent": get_random_user_agent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate",  # brotli 제외
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }

            # 재시도 시 약간의 딜레이
            if retry_count > 0:
                await asyncio.sleep(retry_count * 0.5)  # 0.5초, 1초 딜레이

            logger.debug(f"Fetching URL with improved headers: {url} (시도 {retry_count + 1}/{max_retries})")

            # ✅ DH_KEY_TOO_SMALL 해결을 위한 SSL 컨텍스트 생성
            ssl_context = ssl.create_default_context()
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")  # DH 키 크기 제한 해제
            ssl_context.check_hostname = False  # 호스트명 검증 비활성화
            ssl_context.verify_mode = ssl.CERT_NONE  # 인증서 검증 비활성화
            try:
                ssl_context.options |= ssl.OP_LEGACY_SERVER_CONNECT  # 레거시 서버 연결 허용
            except AttributeError:
                pass

            # ✅ 개선된 타임아웃 설정 (크롤링용 - 더 짧게)
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=45, connect=8, sock_read=25),  # 45초 총 타임아웃
                ssl=ssl_context
            ) as response:
                # ✅ HTTP 요청 완료 후 중단 신호 확인 (네트워크 대기 후 즉시 확인)
                if stop_signal and stop_signal.is_stopped():
                    logger.info(f"[🛑⚡ HTTP 요청 완료 후 중단 신호 감지] 응답 처리 중단 - URL: {url}")
                    return None
                
                if response.status == 200:
                    # Content-Type 확인하여 XML/JSON 페이지 필터링
                    content_type = response.headers.get('Content-Type', '').lower()
                    if any(excluded_type in content_type for excluded_type in ['application/json', 'application/xml', 'text/xml', 'application/rss+xml', 'application/atom+xml']):
                        logger.info(f"[크롤링 제외] XML/JSON 페이지 스킵: {url}, Content-Type: {content_type}")
                        return None
                    
                    # ✅ HTTP 헤더 정보 수집 (변경감지용)
                    response_headers = {
                        'content_length': response.headers.get('Content-Length'),
                    }
                    
                    # ✅ Content-Type과 Content-Disposition으로 다운로드 파일 감지
                    content_type = response.headers.get('content-type', '').lower()
                    content_disposition = response.headers.get('content-disposition', '').lower()
                    
                    # 다운로드 파일 타입 확인
                    download_types = [
                        'application/pdf', 'application/zip', 'application/octet-stream',
                        'application/msword', 'application/vnd.ms-excel', 'application/vnd.ms-powerpoint',
                        'application/vnd.openxmlformats', 'application/x-hwp', 'application/haansofthwp',
                        'image/', 'video/', 'audio/', 'application/x-'
                    ]
                    
                    # Content-Disposition에 attachment가 있으면 다운로드 파일
                    if 'attachment' in content_disposition:
                        logger.info(f"[다운로드 파일 감지] Content-Disposition: {url}")
                        return None
                        
                    # Content-Type이 다운로드 파일 타입이면 제외
                    for download_type in download_types:
                        if download_type in content_type:
                            logger.info(f"[다운로드 파일 감지] Content-Type {content_type}: {url}")
                            return None
                    
                    logger.debug(f"Successfully fetched URL: {url}")

                    # ✅ 인코딩 오류 처리가 포함된 텍스트 추출
                    try:
                        html_content = await response.text()
                        # HTML과 헤더 정보를 함께 반환
                        return {
                            'html': html_content,
                            'headers': response_headers
                        }
                    except UnicodeDecodeError:
                        # UTF-8 디코딩 실패 시 오류를 무시하고 강제 디코딩
                        logger.warning(f"[크롤링 인코딩 오류 무시] URL: {url}, 일부 문자가 손실될 수 있음")
                        html_content = await response.text(errors='ignore')
                        return {
                            'html': html_content,
                            'headers': response_headers
                        }
                else:
                    # ✅ 영구적 오류(4xx 클라이언트 에러)는 재시도하지 않음
                    if response.status in [400, 401, 403, 404, 405, 410, 451]:
                        # ✅ 404 에러는 DEBUG 레벨로 변경하여 로그 과다 출력 방지
                        if response.status == 404:
                            logger.debug(f"HTTP Error: {url} returned status 404 - 재시도 없이 건너뜀 (영구적 오류)")
                        else:
                            logger.warning(f"HTTP Error: {url} returned status {response.status} - 재시도 없이 건너뜀 (영구적 오류)")
                        return None
                    
                    # 그 외 에러(5xx 서버 에러 등)는 재시도
                    logger.warning(f"HTTP Error: {url} returned status {response.status} (시도 {retry_count + 1}/{max_retries})")
                    if retry_count < max_retries - 1:
                        continue  # 다음 재시도로
                    else:
                        return None

        except asyncio.TimeoutError as e:
            logger.warning(f"Timeout error for {url} (시도 {retry_count + 1}/{max_retries}): {e}")
            if retry_count < max_retries - 1:
                continue  # 다음 재시도로
            else:
                logger.error(f"All retry attempts failed due to timeout: {url}")
                return None

        except aiohttp.ClientError as e:
            logger.warning(f"Network error for {url} (시도 {retry_count + 1}/{max_retries}): {e}")
            if retry_count < max_retries - 1:
                continue  # 다음 재시도로
            else:
                logger.error(f"All retry attempts failed due to network error: {url}")
                return None

        except Exception as e:
            logger.warning(f"Exception for {url} (시도 {retry_count + 1}/{max_retries}): {e}")
            if retry_count < max_retries - 1:
                continue  # 다음 재시도로
            else:
                logger.error(f"All retry attempts failed due to exception: {url}")
                return None

    return None

async def crawl_website(start_url, max_depth: int, max_tasks=4, job_id=None, job_manager=None, max_crawl_urls=100, url_filter: str = None, dbname: str = None, chat_bot_id: str = None, table_name: str = None, collection_queue: asyncio.Queue = None, total_new_or_changed: list = None, success_count: list = None, total_discovered_urls_final: list = None, crawl_start_time: float = None, stop_count: int = 10):
    """크롤링 중 실시간 카운트만 표시하고 완료 메시지 없음 (개선된 연결 설정)
    
    Args:
        crawl_start_time: 크롤링 시작 시간 (time.time() 값), 시간 제한 체크용
        stop_count: 크롤링 최대 허용 시간 (초 단위), 이 시간 초과 시 탐색 중단
    """
    # ✅ 워커 수만큼 여유를 두어 정확도 향상 (각 워커가 큐에서 가져온 작업까지 처리 가능)
    # 예: max_crawl_urls=100, max_tasks=4 → 실제 제한: 100 + (4 * 2) = 108개
    # 워커당 최대 2개씩 여유를 두어 큐에서 가져온 작업까지 완료 가능하도록 함
    actual_max_crawl_urls = max_crawl_urls + (max_tasks * 2)
    logger.info(
        f"[크롤링 시작] URL: {start_url}, max_depth: {max_depth}, max_tasks: {max_tasks}, url_filter: {url_filter}"
    )
    logger.info(f"[크롤링 제한 설정] 요청값: {max_crawl_urls}개, 실제 제한: {actual_max_crawl_urls}개 (워커 여유: {max_tasks * 2}개)")
    # 시작 URL 정규화
    start_url = normalize_url(start_url)
    parsed_url = urlparse(start_url)
    # 스킴이 없는 경우 기본 https 사용
    scheme = parsed_url.scheme or "https"
    domain = parsed_url.netloc
    # ✅ 도메인 정규화: www 서브도메인 제거하여 비교 (www.yna.co.kr == yna.co.kr)
    normalized_domain = normalize_domain(domain)
    base_path = parsed_url.path
    precise_scope_path = extract_precise_scope_path_prefix(start_url) or "/"

    # ✅ 하위 경로 제한: 입력된 URL의 하위 경로만 크롤링
    logger.info(f"[하위 경로 제한] 원본 도메인: {domain}, 정규화 도메인: {normalized_domain}, 기본 경로: {base_path}")

    visited = set()
    visited_lock = asyncio.Lock()  # ✅ visited 중복 체크를 위한 Lock
    enqueued = set()
    enqueued_lock = asyncio.Lock()  # ✅ 탐색 단계 중복 체크를 위한 Lock
    failures = {}
    results = []
    queue = asyncio.Queue()
    stop_signal = CrawlStopSignal()  # ✅ 글로벌 중단 신호 생성
    total_discovered_urls = [0]  # ✅ 전체 탐색한 URL 수 카운트 (리스트로 참조 전달)

    # ✅ 시드 확장: 루트와 상위 경로까지 초기 시드로 투입
    def _dir_path(path: str) -> str:
        # 파일로 보이면 상위 디렉토리로 조정
        if path and not path.endswith('/'):
            if '/' in path:
                return path.rsplit('/', 1)[0] + '/'
            return '/'
        return path or '/'

    dir_path = _dir_path(base_path)
    parts = [p for p in dir_path.strip('/').split('/') if p]
    prefixes = ['/']
    acc = ''
    for p in parts:
        acc += '/' + p
        if not acc.endswith('/'):
            acc += '/'
        prefixes.append(acc)
    prefixes = [precise_scope_path]

    # 탐색 범위와 수집 범위를 분리
    # 탐색 범위: 최상위 하위 디렉토리 (예: /surakhyu/) - 링크 발견을 위해 넓게
    # 수집 범위: 입력 URL 경로 (예: /surakhyu/web/main/main/) - 실제 결과에 포함할 범위
    crawl_allowed_path = precise_scope_path
    collect_base_path = precise_scope_path
    logger.info(f"[경로 설정] 탐색 범위: {crawl_allowed_path}, 수집 범위: {collect_base_path}")

    seed_set = set()
    # 루트→상위 경로→기준 경로→사용자 입력 URL 순으로 enqueue
    logger.info(f"[시드 URL 추가 시작] 총 {len(prefixes) + 1}개 시드 예상")
    for pref in prefixes:
        seed_url = f"{scheme}://{domain}{pref}"
        if seed_url not in seed_set:
            await queue.put((seed_url, 0))
            seed_set.add(seed_url)
            # ✅ enqueued에도 추가하여 나중에 중복 발견 방지
            protocol_agnostic_seed = normalize_url_protocol_agnostic(seed_url)
            enqueued.add(protocol_agnostic_seed)
            logger.info(f"[시드 추가] {seed_url}")
    if not start_url.startswith(('http://', 'https://')):
        start_seed = f"https://{start_url}"
    else:
        start_seed = start_url
    if start_seed not in seed_set:
        await queue.put((start_seed, 0))
        seed_set.add(start_seed)
        # ✅ enqueued에도 추가
        protocol_agnostic_seed = normalize_url_protocol_agnostic(start_seed)
        enqueued.add(protocol_agnostic_seed)
        logger.info(f"[시드 추가] {start_seed}")
    logger.info(f"[시드 URL 추가 완료] 총 {len(seed_set)}개 시드가 큐에 추가됨")

    # ✅ 크롤링 시작 메시지 (한 번만)
    if job_id and job_manager:
        await send_message_to_socket(
            job_id,
            {
                "type": "crawl_status",
                "status": "crawling",
                "message": "페이지 수집을 시작합니다",
                "timestamp": time.time(),
            },
            job_manager,
        )

    # ✅ DH_KEY_TOO_SMALL 해결을 위한 SSL 컨텍스트 생성 (크롤링 호환성 극대화)
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")  # DH 키 크기 제한 해제
    ssl_context.check_hostname = False  # 호스트명 검증 비활성화
    ssl_context.verify_mode = ssl.CERT_NONE  # 인증서 검증 비활성화
    try:
        ssl_context.options |= ssl.OP_LEGACY_SERVER_CONNECT  # 레거시 서버 연결 허용
    except AttributeError:
        pass

    # ✅ 강력한 SSL 검증 비활성화된 커넥터 설정 (크롤링용)
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,  # SSL 컨텍스트 사용 (ssl=False 대신)
        force_close=True,  # 연결 재사용 방지 (SSL 문제 회피)
        limit=Config.URL_HTTP_CONNECTION_POOL_SIZE,  # 크롤링용 더 많은 연결 허용
        limit_per_host=Config.URL_HTTP_CONNECTION_PER_HOST,  # 호스트당 연결 수 증가
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True,
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=45, connect=8, sock_read=25)  # 크롤링용 타임아웃
    ) as session:
        tasks = []
        # 시드로 인해 실제 깊이가 1~2단계 증가할 수 있으므로 내부 max_depth 소폭 보정
        internal_max_depth = max_depth
        # ✅ worker heartbeat 추적용 딕셔너리 (worker_id → timestamp)
        worker_last_seen = {}
        
        for i in range(max_tasks):
            worker_id = i  # enumerate index 사용
            task = asyncio.create_task(
                crawl_page_worker_with_count(
                    queue, session, domain, internal_max_depth, visited, results, 
                    crawl_allowed_path, collect_base_path, enqueued, failures, 
                    job_id, job_manager, actual_max_crawl_urls, url_filter, dbname, chat_bot_id, 
                    stop_signal,  # ✅ 글로벌 중단 신호 전달
                    total_discovered_urls,  # ✅ 전체 탐색한 URL 수 카운트 전달
                    visited_lock,  # ✅ visited 중복 체크 Lock 전달
                    enqueued_lock,  # ✅ enqueued 중복 체크 Lock 전달
                    table_name,  # ✅ 변경 감지를 위한 테이블명 전달
                    collection_queue,  # ✅ 하이브리드 스트리밍: 수집 큐 전달
                    total_new_or_changed,  # ✅ 수집 완료 누적 개수 전달
                    success_count,  # ✅ 저장 완료 누적 개수 전달
                    total_discovered_urls_final,  # ✅ 탐색 완료 시점의 값을 실시간으로 업데이트하기 위한 참조
                    crawl_start_time,  # ✅ 크롤링 시작 시간 전달
                    stop_count,  # ✅ 시간 제한 (초) 전달
                    worker_id=worker_id,  # ✅ worker ID 전달
                    worker_last_seen=worker_last_seen  # ✅ heartbeat 추적 딕셔너리 전달
                )
            )
            tasks.append(task)

        # ✅ 소켓 상태를 확인하면서 큐 작업 완료 대기 (소켓 끊김 시 조기 종료)
        socket_disconnected = False
        
        # ✅ watchdog 상태 추적 변수 (3요소 체크용)
        last_unfinished = None
        last_queue_size = None
        last_progress_ts = time.time()
        STALL_SECONDS = 30  # 정체 판단 시간 (정부 사이트 + Playwright 고려)
        MAX_STALL_TIMEOUT = 40  # 최대 정체 타임아웃 횟수
        stall_timeout_count = 0
        WORKER_HEARTBEAT_TTL = 30  # worker 생존 판단 시간 (초)
        while True:  # 큐와 워커 모두 확인하도록 수정
            # ✅ 최우선: 글로벌 중단 신호 확인 (더 빠른 반응)
            if stop_signal and stop_signal.is_stopped():
                socket_disconnected = True
                logger.info(f"[🛑⚡ 글로벌 중단 신호 감지] 크롤링 메인 루프 즉시 종료 - 이유: {stop_signal.get_reason()}")
                logger.info(f"현재 수집된 URL: {len(results)}개, 큐 대기: {queue.qsize()}개")
                break
            
            # use_crawl_stop 확인 (세션 끊김 감지 제거)
            if job_id and job_manager:
                try:
                    status = await job_manager.get_job_status(job_id)
                    if status == "use_crawl_stop":
                        socket_disconnected = True  # 크롤링만 중단, 웹소켓은 유지됨
                        stop_signal.set_stop("use_crawl_stop")  # ✅ 글로벌 중단 신호 설정
                        logger.info("[🛑 사용자 중단 신호] 크롤링 중단, 웹소켓 유지하며 후처리 진행")
                        logger.info(f"현재 수집된 URL: {len(results)}개, 큐 대기: {queue.qsize()}개")
                        break
                except Exception:
                    pass
                               
            # ✅ 큐 완료 대기 (3초마다 중단 신호 확인)
            try:
                await asyncio.wait_for(queue.join(), timeout=3.0)
                
                # ✅ 탐색 큐가 비었으면, collection_queue도 비었는지 확인 (하이브리드 스트리밍)
                if collection_queue is not None:
                    collection_queue_size = collection_queue.qsize()
                    if collection_queue_size > 0:
                        logger.info(f"[탐색 큐 완료] 탐색 큐는 비었지만, collection_queue에 {collection_queue_size}개 대기 중, 계속 대기...")
                        await asyncio.sleep(1.0)  # 1초 대기 후 다시 확인
                        continue
                    else:
                        logger.info("[탐색 큐 완료] 탐색 큐 및 collection_queue 모두 비었음")
                
                # 큐가 비워졌음 → 워커들에게 종료 신호 전송
                logger.info("[크롤링 완료] 모든 큐 비었음, 워커들에게 종료 신호 전송")
                
                # ✅ 센티널 값(None)을 각 워커에게 전송하여 queue.get()에서 깨어나도록 함
                for _ in range(max_tasks):
                    await queue.put((None, 0))  # None = 종료 신호
                
                logger.info(f"[워커 종료 신호] {max_tasks}개 워커에게 종료 신호 전송 완료")
                break
            except asyncio.TimeoutError:
                # 타임아웃 → 큐 작업 계속 진행 중
                stall_timeout_count += 1
                
                queue_size = queue.qsize()
                # ✅ _unfinished_tasks 보호 접근 (런타임별 차이 대비)
                unfinished = getattr(queue, "_unfinished_tasks", None)
                if not isinstance(unfinished, int):
                    continue  # 내부 필드 접근 실패 시 스킵
                
                # ✅ 1️⃣ 진행 상황 변화 확인 (unfinished_tasks 변화 + queue_size 변화)
                progress_changed = (
                    unfinished != last_unfinished
                    or queue_size != last_queue_size
                )
                
                if progress_changed:
                    # 진행 중이면 상태 업데이트 및 카운터 리셋
                    last_progress_ts = time.time()
                    last_unfinished = unfinished
                    last_queue_size = queue_size
                    stall_timeout_count = 0
                # progress_changed가 False면 정체 상태 유지 (카운터는 이미 증가됨)
                
                now = time.time()
                stall_time = now - last_progress_ts
                
                # ✅ 2️⃣ worker 생존 확인 (heartbeat 체크)
                alive_worker_exists = any(
                    now - ts < WORKER_HEARTBEAT_TTL
                    for ts in worker_last_seen.values()
                )
                
                # ✅ watchdog 디버그 로그 (운영 장애 분석용)
                logger.warning(
                    f"[watchdog] no progress "
                    f"{stall_time:.1f}s | q={queue_size} | unfin={unfinished} "
                    f"| alive={alive_worker_exists} | stall_timeout={stall_timeout_count}"
                )
                
                # ✅ 3️⃣ 최종 강제 종료 조건 (3요소 모두 만족 시)
                should_force_stop = (
                    queue_size == 0
                    and unfinished > 0
                    and stall_time > STALL_SECONDS
                    and not alive_worker_exists
                    and stall_timeout_count >= MAX_STALL_TIMEOUT
                )
                
                if should_force_stop:
                    logger.error(
                        f"[워커 강제 종료] 정체 감지 "
                        f"(stall={stall_time:.1f}s, unfinished={unfinished}, "
                        f"queue={queue_size}, alive={alive_worker_exists}, "
                        f"stall_timeout={stall_timeout_count})"
                    )
                    # 글로벌 중단 신호 설정
                    if stop_signal:
                        stop_signal.set_stop(f"워커 정체 감지로 인한 강제 종료 (stall={stall_time:.1f}s, unfinished={unfinished}, queue={queue_size})")
                    # 센티널 값 전송하여 워커 종료
                    for _ in range(max_tasks):
                        await queue.put((None, 0))
                    # ✅ unfinished_tasks를 강제로 0으로 만들기 위해 task_done() 호출
                    # (워커가 멈춰서 task_done()을 호출하지 못한 경우 대비)
                    if unfinished > 0:
                        logger.warning(f"[큐 정리] unfinished_tasks={unfinished}개를 강제로 정리합니다.")
                        for _ in range(unfinished):
                            try:
                                queue.task_done()
                            except ValueError:
                                # 이미 0이면 무시
                                pass
                    break
                
                # 워커 상태 확인을 위해 추가 대기
                if queue_size == 0 and unfinished >= 1:
                    await asyncio.sleep(1.0)
                continue  # 3초마다 중단 신호 확인하며 계속 대기
        
        if socket_disconnected:
            logger.info("소켓 끊김으로 인한 크롤링 조기 종료")
            # 큐에 남은 작업들을 task_done() 처리하여 워커들이 정상 종료되도록 함
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            # ✅ 중단 시에도 센티널 값 전송하여 queue.get()에서 대기 중인 워커 종료
            logger.info(f"[중단 신호] {max_tasks}개 워커에게 종료 신호 전송")
            for _ in range(max_tasks):
                await queue.put((None, 0))
        else:
            logger.info("모든 크롤링 태스크 완료")

        # 워커 정리 (센티널 값 전송 완료, 워커들이 자연스럽게 종료됨)
        active_workers = len([t for t in tasks if not t.done()])
        logger.info(f"[워커 정리 시작] 활성 워커: {active_workers}개 / 총 {len(tasks)}개")
        logger.info("[워커 정리] 센티널 값 전송 완료, 워커 종료 대기 (최대 15초)")
        
        try:
            # 센티널 값을 받은 워커들이 종료될 때까지 대기 (15초 타임아웃)
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), 
                timeout=15.0
            )
            logger.info("[워커 정리 완료] 모든 워커 정상 종료")
        except asyncio.TimeoutError:
            # 15초 후에도 남아있으면 강제 취소 (Playwright 작업이 걸린 경우)
            remaining = len([t for t in tasks if not t.done()])
            logger.warning(f"[워커 정리 타임아웃] {remaining}개 워커 강제 취소 (Playwright 작업 지연 가능성)")
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("[워커 정리 완료] 강제 취소 후 종료")

    # ✅ 완료 메시지 제거 - 바로 처리 단계로 넘어감
    # 로그에만 기록
    logger.info(f"크롤링 완료: 총 {len(results)}개 페이지 수집")
    
    # ✅ total_discovered_urls 값과 함께 반환
    return {
        "results": results,
        "total_discovered_urls": total_discovered_urls[0] if total_discovered_urls else 0
    }

# ✅ 하이브리드 스트리밍 구조: 크롤링 워커 (수집 즉시 큐에 전달)
async def crawl_page_worker_with_count(
    queue, session, domain, max_depth, visited, results, 
    crawl_allowed_path, collect_base_path, enqueued, failures, 
    job_id=None, job_manager=None, max_crawl_urls=100, url_filter: str = None, dbname: str = None, chat_bot_id: str = None,
    stop_signal: CrawlStopSignal = None,  # ✅ 글로벌 중단 신호 추가
    total_discovered_urls: list = None,  # ✅ 전체 탐색한 URL 수 카운트를 위한 리스트 참조
    visited_lock: asyncio.Lock = None,  # ✅ visited 중복 체크를 위한 Lock
    enqueued_lock: asyncio.Lock = None,  # ✅ enqueued 중복 체크를 위한 Lock
    table_name: str = None,  # ✅ 변경 감지를 위한 테이블명 추가
    collection_queue: asyncio.Queue = None,  # ✅ 하이브리드 스트리밍: 수집 큐
    total_new_or_changed: list = None,  # ✅ 수집 완료 누적 개수 (배치 처리 후에도 유지)
    success_count: list = None,  # ✅ 저장 완료 누적 개수
    total_discovered_urls_final: list = None,  # ✅ 탐색 완료 시점의 값을 실시간으로 업데이트하기 위한 참조
    crawl_start_time: float = None,  # ✅ 크롤링 시작 시간 (시간 제한 체크용)
    stop_count: int = 10,  # ✅ 크롤링 최대 허용 시간 (초 단위)
    worker_id: int = None,  # ✅ worker ID (heartbeat 추적용)
    worker_last_seen: dict = None  # ✅ heartbeat 추적 딕셔너리 (worker_id → timestamp)
):
    logger.info(f"[워커 시작] worker_id={worker_id}, url_filter: {url_filter}, stop_count: {stop_count}초")
    socket_disconnected = False  # 소켓 끊김 상태 플래그
    while True:
        # ✅ heartbeat 업데이트 (루프 맨 위 - 1회)
        if worker_id is not None and worker_last_seen is not None:
            worker_last_seen[worker_id] = time.time()
        # ✅ 최우선 1: 시간 제한 체크 (stop_count 초과 시 중단)
        if crawl_start_time is not None and stop_count > 0:
            elapsed_time = time.time() - crawl_start_time
            if elapsed_time >= stop_count:
                logger.warning(f"[⏱️ 시간 제한 도달] 크롤링 시간이 {stop_count}초를 초과했습니다 (경과: {elapsed_time:.2f}초)")
                logger.info(f"   - 현재까지 수집: {len(results)}개")
                # ✅ 글로벌 중단 신호 설정하여 모든 워커 종료
                if stop_signal:
                    stop_signal.set_stop("크롤링 최대 시간 도달 (시간 제한)")
                break
        
        # ✅ 최우선 2: 글로벌 중단 신호 확인 (즉시 반응)
        if stop_signal and stop_signal.is_stopped():
            logger.info(f"[🛑⚡ 글로벌 중단 신호 감지] 기존 워커 즉시 종료 - 이유: {stop_signal.get_reason()}")
            logger.info(f"   - 현재까지 수집: {len(results)}개")
            break
        
        # ✅ use_crawl_stop 확인 (세션 끊김 감지 제거)
        if job_id and job_manager and not socket_disconnected:
            try:
                status = await job_manager.get_job_status(job_id)
            except Exception:
                status = None
            if status == "use_crawl_stop":
                socket_disconnected = True  # 크롤링만 중단, 웹소켓은 유지됨
                if stop_signal:
                    stop_signal.set_stop("use_crawl_stop")  # ✅ 다른 워커들에게도 신호 전파
                logger.info("[🛑 기존 워커에서 중단 신호 감지] URL 탐색 중단, 웹소켓 유지하며 후처리 진행")
                logger.info(f"현재까지 수집: {len(results)}개, 방문: {len(visited)}개, 대기: {queue.qsize()}개")
        
        # ✅ 사용자 중단 신호 시 URL 탐색 중단
        if socket_disconnected:
                break
        
        # ✅ 큐에서 작업 하나 가져온 뒤, 어떤 종료 경로에서도 task_done이 정확히 1번 호출되도록 보장
        try:
            current_url, depth = await queue.get()
        except asyncio.CancelledError:
            logger.info("[워커 취소] queue.get() 취소됨 (CancelledError)")
            raise

        try:
            # ✅ 센티널 값(None) 확인 - 종료 신호
            if current_url is None:
                logger.info("[워커 종료] 센티널 값 수신, 워커 정상 종료")
                break
            
            # ✅ 작업 처리 전 중단 신호 재확인 (더 빠른 반응)
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑⚡ 작업 처리 전 중단 신호 감지] 기존 워커 작업 중단 - URL: {current_url}")
                break
            
            current_url = normalize_url(current_url)
            
            # ✅ 큐에서 작업을 가져온 후 소켓 상태 재확인
            if socket_disconnected:
                break
            
            # URL 검증 로직 수정
            parsed_current = urlparse(current_url)
            if parsed_current.netloc != domain:
                continue
            
            protocol_agnostic_url = normalize_url_protocol_agnostic(current_url)
            
            # ✅ URL 수신 직후 DB 중복 검사 (메모리 비교 제거, 실제 DB로만 중복 판정)
            if table_name and dbname:
                try:
                    if await is_url_in_db(current_url, table_name, dbname):
                        if visited_lock:
                            async with visited_lock:
                                visited.add(protocol_agnostic_url)
                        else:
                            visited.add(protocol_agnostic_url)
                        logger.info(f"[DB 중복] 스킵 (URL 수신 직후): {current_url}")
                        continue
                except Exception as db_check_err:
                    logger.warning(f"[DB 중복 검사 예외] 스킵하고 진행: {current_url}, 오류: {db_check_err}")
            
            # ✅ 이번 런에서 이미 처리했거나 스킵한 URL인지만 확인 (재큐 방지용, 중복 판정은 DB만 사용)
            is_already_seen = False
            should_stop_due_to_limit = False
            if visited_lock:
                async with visited_lock:
                    if protocol_agnostic_url in visited:
                        is_already_seen = True
                    else:
                        visited.add(protocol_agnostic_url)
                        logger.info(f"[🎯 visited 등록] {current_url}")
                        logger.debug(f"   - 정규화 URL: {protocol_agnostic_url}")
                        logger.debug(f"   - 현재 visited 크기: {len(visited)}")
                        current_discovered_count = len(visited)
                        if current_discovered_count >= max_crawl_urls:
                            should_stop_due_to_limit = True
                            logger.info(f"[크롤링 제한] 최대 URL 개수({max_crawl_urls})에 도달하여 워커 종료 (현재 탐색: {current_discovered_count}개)")
            else:
                if protocol_agnostic_url in visited:
                    is_already_seen = True
                else:
                    visited.add(protocol_agnostic_url)
                    current_discovered_count = len(visited)
                    if current_discovered_count >= max_crawl_urls:
                        should_stop_due_to_limit = True
                        logger.info(f"[크롤링 제한] 최대 URL 개수({max_crawl_urls})에 도달하여 워커 종료 (현재 탐색: {current_discovered_count}개)")
            
            if is_already_seen:
                logger.debug(f"[이번 런 이미 처리됨] 스킵: {current_url}")
                continue
            
            # ✅ max_crawl_urls 제한 도달 시 즉시 종료 (Lock 내부에서 체크한 결과 사용)
            if should_stop_due_to_limit:
                # ✅ 글로벌 중단 신호 설정하여 다른 워커들도 함께 종료
                if stop_signal:
                    stop_signal.set_stop("max_urls_reached")
                break  # ✅ 워커 종료
            
            # 깊이 체크
            if depth > max_depth:
                continue
            
            # ✅ URL 처리 시작 전 중단 신호 재확인 (즉시 중단)
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑⚡ URL 처리 시작 전 중단 신호 감지] 즉시 중단 - URL: {current_url}")
                break
            
            logger.info(f"[{depth}] 크롤링 중: {current_url}")
            
            # ✅ heartbeat 업데이트 (fetch 직전 - Playwright/HTTP 대기 전)
            if worker_id is not None and worker_last_seen is not None:
                worker_last_seen[worker_id] = time.time()
            
            try:
                url_result = await fetch_url_content(session, current_url, stop_signal=stop_signal)  # ✅ 중단 신호 전달
                
                # ✅ heartbeat 업데이트 (fetch 직후 - 성공/실패 무관하게)
                if worker_id is not None and worker_last_seen is not None:
                    worker_last_seen[worker_id] = time.time()
            except asyncio.CancelledError:
                logger.info(f"[워커 취소] fetch_url_content 취소됨: {current_url} (CancelledError)")
                # Playwright 작업이 진행 중일 수 있으므로 잠시 대기 후 정리
                await asyncio.sleep(1.0)  # 1초 대기하여 Playwright 작업 완료 대기
                break  # 워커 종료
            except Exception as fetch_error:
                logger.error(f"[워커 오류] fetch_url_content 실패: {current_url}, 오류: {fetch_error}")
                # url_result = None 과 동일한 효과로 다음 분기에서 처리
                url_result = None
                await asyncio.sleep(0.5)
                continue  # 다음 작업으로 진행
                
            # ✅ fetch 완료 후 중단 신호 확인 (네트워크 대기 후 즉시 확인)
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑⚡ fetch 완료 후 중단 신호 감지] 파싱 생략 - URL: {current_url}")
                break
                
            if not url_result:
                # 1회 재시도: 동일 깊이에서 다시 큐에 투입
                failure_key = (current_url, depth)
                cnt = failures.get(failure_key, 0)
                if cnt < 1:
                    failures[failure_key] = cnt + 1
                    await queue.put((current_url, depth))
                continue
            
            html = url_result['html']
            headers = url_result['headers']
            
            # ✅ BeautifulSoup 파싱 전 중단 신호 확인
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑⚡ BeautifulSoup 파싱 전 중단 신호 감지] 파싱 중단 - URL: {current_url}")
                break
                
            soup = BeautifulSoup(html, "lxml")
            
            # 수집 여부 판단: 수집 범위에 해당하는 URL만 결과에 추가
            parsed_current = urlparse(current_url)
            should_collect = parsed_current.path.startswith(collect_base_path)
            
            # ✅ 결과 처리 전 중단 신호 확인
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑⚡ 결과 처리 전 중단 신호 감지] 결과 저장 중단 - URL: {current_url}")
                break
            
            # ✅ 전체 탐색 URL 카운트 증가 (수집 여부와 무관하게)
            if total_discovered_urls is not None:
                total_discovered_urls[0] += 1
                # ✅ total_discovered_urls_final을 최대값으로 유지 (워커 간 동기화 문제 해결)
                if total_discovered_urls_final is not None:
                    # 현재 값보다 클 때만 업데이트 (덮어쓰기 방지)
                    if total_discovered_urls[0] > total_discovered_urls_final[0]:
                        total_discovered_urls_final[0] = total_discovered_urls[0]
                        
                        # ✅ 탐색 대상 URL이 정해질 때 SSE 발행
                        if job_id and dbname:
                            try:
                                current_collection = total_new_or_changed[0] if total_new_or_changed else 0
                                current_save = success_count[0] if success_count else 0
                                sse_message = {
                                    "status": "running",
                                    "total_count": total_discovered_urls_final[0],
                                    "collection_count": current_collection,
                                    "save_count": current_save,
                                    "study_count": current_save,
                                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                await send_message_to_redis_sse(job_id, sse_message, dbname=dbname)
                                logger.debug(f"[탐색 단계 SSE 발행] job_id={job_id}, scan={total_discovered_urls_final[0]}, collection={current_collection}, save={current_save}")
                            except Exception as sse_error:
                                logger.warning(f"[탐색 단계 SSE 발행 실패] job_id={job_id}, 오류={sse_error}")
                    else:
                        # 이미 더 큰 값이 있으면 로컬 값을 최신으로 동기화
                        total_discovered_urls[0] = total_discovered_urls_final[0]
            
            # ✅ URL 필터링 적용 (수집 단계에서) - url_filter가 있으면 항상 적용
            if should_collect:
                if url_filter and url_filter != "B":
                    filter_passed = should_include_url_by_filter(current_url, url_filter)
                    if not filter_passed:
                        should_collect = False
                        logger.info(f"[수집 제외 - URL 필터] {current_url} (필터: {url_filter})")
                else:
                    logger.info(f"[필터 적용 안함] {current_url} (필터: {url_filter or 'None'})")
            
            if should_collect:
                # ✅ DB에서 block 태그 조회
                block_tag = None
                if chat_bot_id:
                    try:
                        parsed_current = urlparse(current_url)
                        domain = parsed_current.netloc or parsed_current.path.split('/')[0] if parsed_current.path else ''
                        block_tag = await fetch_subject_block_from_db(domain, chat_bot_id, dbname)
                    except Exception as e:
                        logger.warning(f"[block 태그 조회 실패] URL: {current_url}, 오류: {e}")
                
                # 공통 파서 사용
                structured = build_structured_content(soup, current_url, html, block_tag=block_tag)
                
                if structured.get("content"):
                    content = structured.get("content", "")
                    title = structured.get("title", "") or structured.get("web_title", "")
                    
                    # ✅ 에러 페이지 필터링 추가 (404 등)
                    title_stripped = title.strip()
                    content_lower = content.lower()
                    title_lower = title.lower()
                    
                    # 에러 키워드 체크
                    error_keywords = [
                        '페이지를 찾을 수 없습니다',
                        '페이지를 찾을수 없습니다',
                        '요청하신 페이지를 찾을 수 없습니다',
                        '요청하신 페이지를 찾을수 없습니다',
                        '페이지가 존재하지 않습니다',
                        '존재하지 않는 페이지',
                        '페이지 오류',
                        '페이지오류',
                        '서비스 준비중',
                        '점검 중',
                        '시스템 점검',
                        '임시로 이용할 수 없습니다',
                        '서버 오류',
                        '요청하신 페이지를 표시할 수 없습니다',
                        '권한이 없습니다',
                        '로그인이 필요합니다',
                        '접속 권한 없음',
                        '접근 제한',
                        '차단되었습니다',
                        '해당 글은 비공개',
                        '삭제된 게시물',
                        '비정상적인 접근',
                        '자동화 접근',
                        '로봇 차단',
                        '캡차',
                        '인증이 필요합니다',
                        'page not found',
                        'service unavailable',
                        'maintenance',
                        'temporarily unavailable',
                        'forbidden',
                        'access restricted',
                        'permission denied',
                        'login required',
                        'sign in',
                        'session expired',
                        'captcha',
                        'robot',
                        'bot detected',
                        'page cannot be displayed',
                        'error occurred',
                        '404 error',
                        '404 not found',
                        '접근할 수 없습니다',
                        '잘못된 경로',
                        '잘못된 페이지',
                        'not found',
                        'access denied',
                        '접근 거부',
                        '삭제된 페이지',
                        '삭제되었습니다'
                    ]
                    
                    # 1. 에러 키워드가 제목이나 내용에 포함되어 있는지 확인
                    is_error_page = any(keyword in content_lower or keyword in title_lower for keyword in error_keywords)
                    
                    # 2. 제목이 비어있거나 너무 짧은 경우 (10자 이하)
                    is_title_empty_or_short = len(title_stripped) == 0 or len(title_stripped) <= 10
                    
                    # 3. 제목이 숫자로만 구성된 경우 (200, 404, 500 등)
                    is_numeric_only = title_stripped.isdigit()
                    
                    # 4. HTTP 상태 코드 패턴 체크 (200, 400, 403, 404, 500, 502, 503 등)
                    http_status_codes = ['200', '400', '401', '403', '404', '500', '502', '503', '504']
                    is_http_status = title_stripped in http_status_codes
                    
                    # 에러 페이지 판정 및 제외
                    if is_error_page:
                        logger.warning(f"[크롤링 제외] 에러 페이지 감지: {current_url}")
                        logger.warning(f"   제목: {title[:50]}...")
                        logger.warning(f"   내용: {content[:100]}...")
                        continue  # 다음 URL로 진행
                    
                    # 제목이 짧고 숫자로만 구성된 경우 (404, 500 등)
                    if is_title_empty_or_short and is_numeric_only:
                        logger.warning(f"[크롤링 제외] 제목이 숫자만 포함 ({title_stripped}): {current_url}")
                        logger.warning(f"   제목: {title[:50]}...")
                        logger.warning(f"   내용: {content[:100]}...")
                        continue  # 다음 URL로 진행
                    
                    # HTTP 상태 코드 제목 감지
                    if is_http_status:
                        logger.warning(f"[크롤링 제외] HTTP 상태 코드 제목 감지 ({title_stripped}): {current_url}")
                        logger.warning(f"   제목: {title[:50]}...")
                        logger.warning(f"   내용: {content[:100]}...")
                        continue  # 다음 URL로 진행
                    
                    # 제목이 비어있으면 제목 추출 실패로 간주하여 제외
                    if len(title_stripped) == 0:
                        logger.warning(f"[크롤링 제외] 제목 추출 실패 (빈 제목): {current_url}")
                        logger.warning(f"   내용: {content[:100]}...")
                        continue  # 다음 URL로 진행
                    
                    # ✅ 2. 수집 제외 조건: '이 문서가 현재 존재하지 않습니다.' 포함 페이지 제외   - 위키용
                    if '이 문서가 현재 존재하지 않습니다.' in content:
                        logger.info(f"[❌ 컨텐츠 제외] 존재하지 않는 문서: {current_url}")
                        continue  # 다음 URL로 진행
                    
                    result_item = {
                        "source": current_url,
                        "title": structured.get("title", ""),
                        "web_title": structured.get("web_title", ""),
                        "content": content,
                        "snippet": structured.get("snippet", ""),
                        "favicon_url": structured.get("favicon_url", ""),
                        "source_size": [len(content)],
                        "headers": headers,
                        "change_status": "passthrough",
                    }

                    if collection_queue is not None:
                        await collection_queue.put(result_item)
                        logger.info(f"[collection_queue 추가] {current_url} (필터: {url_filter or 'ALL'})")
                        logger.debug(f"   - visited에 등록된 URL: {protocol_agnostic_url}")
                        logger.debug(f"   - collection_queue 크기: {collection_queue.qsize()}")
                    else:
                        results.append(result_item)
                        logger.info(f"[수집됨] {current_url} (필터: {url_filter or 'ALL'}, 총 수집: {len(results)}개)")
                else:
                    # ✅ 컨텐츠가 비어있는 경우 로그 출력
                    logger.warning(f"[❌ 컨텐츠 없음] 파싱 후 컨텐츠가 비어있음: {current_url}")
                    logger.debug(f"   - title: {structured.get('title', 'N/A')}")
                    logger.debug(f"   - structured keys: {list(structured.keys())}")
            else:
                if parsed_current.path.startswith(collect_base_path):
                    logger.info(f"[❌ 필터링됨] {current_url} (필터: {url_filter} 조건 불일치)")
                else:
                    logger.info(f"[탐색만] {current_url} (수집 범위 외부, 링크 탐색용)")
            
            # ✅ 탐색 단계: URL 발견 시마다 웹소켓 메시지 전송
            if job_id and job_manager and not socket_disconnected:
                # ✅ 메시지 전송 직전에 최신 값으로 재동기화 (다른 워커가 업데이트했을 수 있음)
                if total_discovered_urls is not None and total_discovered_urls_final is not None:
                    if total_discovered_urls[0] < total_discovered_urls_final[0]:
                        # 다른 워커가 더 큰 값으로 업데이트했으므로 로컬 값 동기화
                        old_local = total_discovered_urls[0]
                        total_discovered_urls[0] = total_discovered_urls_final[0]
                        logger.debug(f"[탐색단계 재동기화] 로컬 값({old_local}) < 최신 값({total_discovered_urls_final[0]}), URL: {current_url}")
                    elif total_discovered_urls[0] > total_discovered_urls_final[0]:
                        # 현재 워커가 가장 큰 값을 가지고 있으므로 final 값 업데이트
                        old_final = total_discovered_urls_final[0]
                        total_discovered_urls_final[0] = total_discovered_urls[0]
                        logger.debug(f"[탐색단계 재동기화] 최신 값({old_final}) < 로컬 값({total_discovered_urls[0]}), URL: {current_url}")
                        
                        # ✅ 탐색 대상 URL이 정해질 때 SSE 발행
                        if job_id and dbname:
                            try:
                                current_collection = total_new_or_changed[0] if total_new_or_changed else 0
                                current_save = success_count[0] if success_count else 0
                                sse_message = {
                                    "status": "running",
                                    "total_count": total_discovered_urls_final[0],
                                    "collection_count": current_collection,
                                    "save_count": current_save,
                                    "study_count": current_save,
                                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                await send_message_to_redis_sse(job_id, sse_message, dbname=dbname)
                                logger.debug(f"[탐색 단계 SSE 발행] job_id={job_id}, scan={total_discovered_urls_final[0]}, collection={current_collection}, save={current_save}")
                            except Exception as sse_error:
                                logger.warning(f"[탐색 단계 SSE 발행 실패] job_id={job_id}, 오류={sse_error}")
                
                # 탐색 단계: total_discovered만 증가, count와 success_url은 현재 값 유지
                # total_discovered_urls_final 우선 (이미 최신 값으로 동기화됨)
                if total_discovered_urls_final and total_discovered_urls_final[0] > 0:
                    total_discovered_val = total_discovered_urls_final[0]
                elif total_discovered_urls and total_discovered_urls[0] > 0:
                    total_discovered_val = total_discovered_urls[0]
                else:
                    total_discovered_val = 0
                
                # total_discovered_val이 0이면 메시지 전송하지 않음 (아직 초기화 안됨)
                if total_discovered_val > 0:
                    count_val = total_new_or_changed[0] if total_new_or_changed else 0
                    success_url_val = success_count[0] if success_count else 0

                    message = {
                        "type": "crawl_count",
                        "total_count": total_discovered_val,
                        "collection_count": count_val,
                        "save_count": success_url_val,
                        "study_count": success_url_val,
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    await send_message_to_socket(job_id, message, job_manager)
                    # 여러 워커가 동시에 실행되므로 값이 순차적으로 증가하지 않을 수 있음
                    logger.info(f"[탐색단계 메시지 전송] total_discovered: {total_discovered_val}, count: {count_val}, success_url: {success_url_val}, URL: {current_url}")
                else:
                    logger.warning(f"[탐색단계 메시지 스킵] total_discovered가 0이므로 메시지 전송하지 않음, URL: {current_url}")
            
            # ✅ 링크 추출 전 중단 신호 확인
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑⚡ 링크 추출 전 중단 신호 감지] 링크 추출 중단 - URL: {current_url}")
                break
            
            # 링크 추출 로직 (enqueued 중복 방지)
            for link in soup.find_all(["a", "link"], href=True):
                href = link.get("href", "").strip()
                if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue
            
                next_url = normalize_url(urljoin(current_url, href))
                parsed_next = urlparse(next_url)
            
                if (
                    next_url.endswith(".css")
                    or "/css/" in next_url
                    or "?ver=" in next_url
                ):
                    continue
            
                # 다운로드 가능한 파일 제외
                if is_downloadable_file(next_url):
                    logger.info(f"\n==========\n[다운로드 가능한 파일 제외] {next_url}\n==========\n")
                    continue
                # ✅ 위키 제외 페이지 링크도 큐에 추가하지 않음
                is_excluded, _ = is_wiki_excluded_page(next_url)
                if is_excluded:
                    logger.info(f"\n==========\n[위키 편집 페이지 제외] {next_url}\n==========\n")
                    continue
            
                # 탐색 범위 내의 링크만 큐에 추가 (넓은 범위로 탐색)
                # ✅ 도메인 정규화 비교: www 서브도메인 무시 (www.yna.co.kr == yna.co.kr)
                normalized_next_domain = normalize_domain(parsed_next.netloc)
                normalized_base_domain = normalize_domain(domain)
                if (
                    normalized_next_domain == normalized_base_domain
                    and parsed_next.path.startswith(crawl_allowed_path)
                ):
                    # ✅ Lock을 사용하여 중복 검사와 큐 추가를 원자적으로 수행 (race condition 방지)
                    protocol_agnostic_next = normalize_url_protocol_agnostic(next_url)
                    if enqueued_lock:
                        async with enqueued_lock:
                            if (protocol_agnostic_next not in visited) and (protocol_agnostic_next not in enqueued):
                                enqueued.add(protocol_agnostic_next)
                                await queue.put((next_url, depth + 1))
                    else:
                        # Lock이 없는 경우 (하위 호환성)
                        if (protocol_agnostic_next not in visited) and (protocol_agnostic_next not in enqueued):
                            enqueued.add(protocol_agnostic_next)
                            await queue.put((next_url, depth + 1))
            
            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            logger.info("[워커 취소] 워커 취소됨 (CancelledError)")
            raise
        except Exception as e:
            logger.error(f"워커 오류: {e}", exc_info=True)  # 스택 트레이스 포함
            # ✅ 예외 발생 후에도 워커는 계속 실행 (다음 작업 처리)
            await asyncio.sleep(0.5)
            continue
        finally:
            # ✅ 여기서만 task_done을 한 번 호출 → get 1회당 task_done 1회 보장
            try:
                queue.task_done()
            except ValueError:
                # 이미 0이거나 잘못된 상태일 때는 조용히 무시
                pass

def is_wiki_excluded_page(url: str, soup=None) -> tuple[bool, str]:
    """
    위키에서 제외해야 할 페이지인지 확인하는 통합 함수
    Returns: (is_excluded: bool, reason: str)
    """
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # 1. 위키 편집 페이지 체크 (URL 파라미터)
        action_values = query_params.get('action', [])
        if action_values:
            wiki_edit_actions = ['edit', 'history', 'watch', 'unwatch', 'submit']
            for action in action_values:
                if action.lower() in wiki_edit_actions:
                    return True, f"wiki_edit_action_{action}"
        
        # 2. MediaWiki 리소스 로더 체크
        if parsed_url.path.endswith('/load.php') or parsed_url.path == '/load.php':
            # only=styles/scripts 파라미터가 있으면 확실히 리소스 로더
            only_values = query_params.get('only', [])
            if any(val in ['styles', 'scripts'] for val in only_values):
                return True, "resource_loader_only"
                
            # modules 파라미터에 mediawiki./skins. 모듈이 있으면 리소스 로더
            modules_values = query_params.get('modules', [])
            for module in modules_values:
                if 'mediawiki.' in module or 'skins.' in module:
                    return True, "resource_loader_modules"
                    
            # 리소스 로더 특징적 파라미터 조합 체크
            resource_params = ['modules', 'only', 'skin', 'version']
            matching_params = sum(1 for param in resource_params if param in query_params)
            if matching_params >= 2:
                return True, "resource_loader_params"
        
        # 3. HTML 기반 편집 페이지 체크 (soup이 제공된 경우)
        if soup:
            # MediaWiki 편집 폼 확인
            if soup.find('form', {'id': 'editform'}):
                return True, "edit_form_html"
                
            # 편집 텍스트박스 확인
            if soup.find('textarea', {'name': 'wpTextbox1'}):
                return True, "edit_textarea_html"
                
            # 편집 관련 hidden input 확인
            edit_inputs = soup.find_all('input', {'name': ['wpStarttime', 'wpEdittime', 'wpAutoSummary']})
            if len(edit_inputs) >= 2:
                return True, "edit_inputs_html"
        
        return False, "normal_page"
        
    except Exception as e:
        logger.warning(f"위키 제외 페이지 체크 중 오류: {url}, {e}")
        return False, "check_error"

# 글로벌 세션 및 배치 관리 (Config 설정 적용)
GLOBAL_URL_BATCH_SIZE = Config.URL_GLOBAL_EMBEDDING_BATCH_SIZE  # 여러 URL의 청크들을 합쳐서 대형 배치로 처리
URL_CONCURRENT_LIMIT = Config.URL_GLOBAL_CONCURRENT_LIMIT   # URL 동시 처리 수

async def collect_files_from_parent_directory(target_url: str, session) -> List[str]:
    """
    403 오류 시 상위 디렉토리에서 대상 URL 경로의 파일들을 재귀적으로 수집하는 함수
    
    Args:
        target_url: 대상 URL (예: https://example.kr/study/)
        session: aiohttp 세션
    
    Returns:
        발견된 파일 URL 리스트
    """
    discovered_files = set()  # 중복 제거를 위해 set 사용
    
    try:
        logger.info(f"[상위 디렉토리 탐색] 대상 URL: {target_url}")
        
        # URL 파싱
        parsed_url = urlparse(target_url)
        path_parts = [part for part in parsed_url.path.strip('/').split('/') if part]
        
        if len(path_parts) == 0:
            logger.warning(f"[상위 디렉토리 탐색] 루트 디렉토리이므로 상위 디렉토리가 없음")
            return []
        
        # 상위 디렉토리 URL 생성
        if len(path_parts) > 1:
            parent_path = '/'.join(path_parts[:-1])
            target_dir_name = path_parts[-1]  # 대상 디렉토리명
        else:
            parent_path = ""
            target_dir_name = path_parts[0]  # 대상 디렉토리명
        
        parent_url = f"{parsed_url.scheme}://{parsed_url.netloc}/{parent_path}/"
        
        logger.info(f"[상위 디렉토리 탐색] 상위 디렉토리: {parent_url}")
        logger.info(f"[상위 디렉토리 탐색] 대상 디렉토리명: {target_dir_name}")
        
        # 상위 디렉토리에서 HTML 가져오기
        parent_html = await fetch_url_content(session, parent_url, )
        if not parent_html:
            logger.warning(f"[상위 디렉토리 탐색] 상위 디렉토리 접근 실패: {parent_url}")
            return []
        
        # HTML 파싱하여 링크 추출
        soup = BeautifulSoup(parent_html, "lxml")
        
        # 1단계: 상위 디렉토리에서 직접 링크 수집
        initial_links = []
        for link in soup.find_all("a", href=True):
            href = link.get("href", "").strip()
            if href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
                full_url = urljoin(parent_url, href)
                
                # 대상 디렉토리로의 링크 또는 하위 파일 링크 필터링
                if (full_url.startswith(target_url) or 
                    target_dir_name in full_url):
                    norm = normalize_url(full_url)
                    initial_links.append(norm)
                    discovered_files.add(norm)
                    logger.info(f"[상위 디렉토리 탐색] 1단계 발견: {full_url}")
        
        logger.info(f"[상위 디렉토리 탐색] 1단계 완료: {len(initial_links)}개 파일 발견")
        
        # 2단계: 발견된 URL들에서 재귀적으로 추가 파일 수집
        for initial_url in initial_links:
            try:
                logger.info(f"[상위 디렉토리 탐색] 2단계 재귀 탐색: {initial_url}")
                
                # 발견된 URL에서 HTML 가져오기
                url_html = await fetch_url_content(session, initial_url)
                if not url_html:
                    continue
                
                # HTML 파싱하여 추가 링크 수집
                url_soup = BeautifulSoup(url_html, "lxml")
                
                for link in url_soup.find_all("a", href=True):
                    href = link.get("href", "").strip()
                    if href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
                        full_url = urljoin(initial_url, href)
                        
                        # 대상 디렉토리 경로 내의 파일들만 필터링
                        if (
                            full_url.startswith(target_url)
                            and full_url != target_url  # 자기 자신 제외
                        ):
                            norm_full = normalize_url(full_url)
                            if norm_full not in discovered_files:
                                discovered_files.add(norm_full)
                                logger.info(f"[상위 디렉토리 탐색] 2단계 재귀 발견: {full_url}")
                
            except Exception as e:
                logger.warning(f"[상위 디렉토리 탐색] 2단계 재귀 탐색 실패: {initial_url}, 오류: {e}")
                continue
        
        result_files = list(discovered_files)
        logger.info(f"[상위 디렉토리 탐색] 최종 완료: {len(result_files)}개 파일 발견 (1단계: {len(initial_links)}개, 2단계: {len(result_files) - len(initial_links)}개)")
        return result_files
        
    except Exception as e:
        logger.error(f"[상위 디렉토리 탐색] 오류: {target_url}, 오류: {e}")
        return []

# ✅ 403 폴백 처리가 포함된 병렬 크롤링 및 처리 함수
async def crawl_and_process_url_parallel_with_403_fallback(
    start_url: str,
    subject: str,
    context: CrawlingContext,
    each_progress: float,
    memo: str = "",
    max_depth: int = 10,
    max_tasks: int = 4,  # ✅ 20 → 4로 변경 (탐색 4개 + 변경 감지 4개 + 저장 4개 = 총 12개)
) -> dict:
    """
    403 폴백 처리가 포함된 병렬 크롤링 및 처리 함수
    
    1. 먼저 원본 URL에서 403 오류 확인
    2. 403이면 상위 디렉토리에서 파일 수집
    3. 403이 아니면 기존 크롤링 로직 실행
    """
    try:
        # Context에서 값 추출
        table_name = context.table_name
        dbname = context.dbname
        job_id = context.job_id
        job_manager = context.job_manager
        job_progress_manager = context.job_progress
        chat_bot_id = context.chat_bot_id
        url_filter = context.url_filter or "B"
        if not url_filter:
            url_filter = "B"
            
        logger.info(f"[✅ 403 폴백 크롤링 시작] URL: {start_url}, url_filter: {url_filter}")
        
        # URL 스키마가 없는 경우 'https://' 추가
        if not start_url.startswith(("http://", "https://")):
            start_url = "https://" + start_url
        
        # ✅ 1. 먼저 일반 크롤링으로 전체 URL 수집 (변경감지 포함)
        logger.info(f"[403 폴백 크롤링] 1단계: 일반 크롤링으로 전체 URL 수집")
        initial_crawl_result = await crawl_and_process_url_parallel(
            start_url=start_url,
            subject=subject,
            context=context,
            each_progress=each_progress,
            memo=memo,
            max_depth=max_depth,
            max_tasks=max_tasks,
        )
        
        # ✅ 전체 수집 URL 정보 보존
        total_discovered_urls = initial_crawl_result.get("total_discovered_urls", 0)
        total_crawled_urls = initial_crawl_result.get("total_crawled_urls", 0)
        original_count = initial_crawl_result.get("original_count", 0)
        no_change_count = initial_crawl_result.get("no_change_count", 0)
        new_or_changed_count = initial_crawl_result.get("new_or_changed_count", 0)
        
        logger.info(f"[403 폴백 크롤링] 1단계 완료: 탐색 {total_discovered_urls}개, 전체 {total_crawled_urls}개 URL 수집, 변경 없음 {no_change_count}개, 신규 학습 {new_or_changed_count}개")
        
        # ✅ 일반 크롤링 완료: total_discovered_urls가 있으면 항상 결과 반환 (변경 없음이어도)
        if total_discovered_urls > 0:
            logger.info(f"[403 폴백 크롤링] 1단계 완료: 탐색 {total_discovered_urls}개, 처리 {initial_crawl_result.get('total_processed_urls', 0)}개 → 결과 반환")
            logger.info(f"[403 폴백 크롤링 결과 반환] total_discovered_urls={total_discovered_urls}, total_new_or_changed_urls={initial_crawl_result.get('total_new_or_changed_urls', 0)}, new_or_changed_count={initial_crawl_result.get('new_or_changed_count', 0)}")
            return initial_crawl_result
        
        # ✅ 개발 서버: DB 탐색 실패 시 403 폴백 실행하지 않고 바로 0개 반환
        if dbname and dbname.strip().lower() == 'dev_user':
            logger.info(f"[개발 서버 모드] DB 탐색 실패로 403 폴백을 실행하지 않고 0개로 종료합니다.")
            return initial_crawl_result
        
        # ✅ 2. total_discovered_urls도 0인 경우에만 403 폴백 처리 진행 (크롤링 실패)
        logger.info(f"[403 폴백 크롤링] 2단계: 탐색 실패 (total_discovered_urls=0), 403 폴백 처리 시작")
        
        # aiohttp 세션 생성
        connector = aiohttp.TCPConnector(limit=Config.URL_HTTP_CONNECTION_POOL_SIZE, limit_per_host=Config.URL_HTTP_CONNECTION_PER_HOST)
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": get_random_user_agent()}
        ) as session:
            
            # 1. 먼저 원본 URL에서 403 오류 확인
            logger.info(f"[403 폴백 크롤링] 원본 URL 확인: {start_url}")
            
            try:
                async with session.get(
                    start_url,
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True
                ) as response:
                    if response.status == 403:
                        logger.info(f"[403 폴백 크롤링] 403 오류 확인됨: {start_url}")
                        
                        # ✅ 403 오류 감지 시 웹소켓 메시지 전송
                        await send_message_to_socket(
                            job_id,
                            {
                                "type": "crawl_status",
                                "current_url": start_url,
                                "timestamp": time.time(),
                            },
                            job_manager,
                        )
                        
                        # 2. 상위 디렉토리에서 파일 수집
                        discovered_files = await collect_files_from_parent_directory(start_url, session)
                        
                        if not discovered_files:
                            logger.warning(f"[403 폴백 크롤링] 상위 디렉토리에서 파일을 찾을 수 없음: {start_url}")
                            
                            # ✅ 파일을 찾지 못했을 때 웹소켓 메시지 전송
                            await send_message_to_socket(
                                job_id,
                                {
                                    "type": "crawl_status",
                                    "status": "warning",
                                    "message": f"상위 디렉토리에서 파일을 찾을 수 없습니다: {start_url}",
                                    "current_url": start_url,
                                    "timestamp": time.time(),
                                },
                                job_manager,
                            )

                            logger.info(f"1 # # # # # #최종 return 되기 전 전체 URLs: {total_crawled_urls} # # # # # #")
                            
                            return {
                                "status": "no_files_found",
                                "message": f"상위 디렉토리에서 파일을 찾을 수 없습니다: {start_url}",
                                "total_processed_urls": 0,
                                "total_chunks": 0,
                                "chunk_count": [],
                                "chunk_hash": [],
                                "use_source": [],
                                "source_size": [],
                                "web_title": [],
                                "processing_time": 0.0,
                                # ✅ 전체 수집 URL 정보 추가
                                "total_crawled_urls": total_crawled_urls,
                                "no_change_count": no_change_count,
                                "new_or_changed_count": 0,
                                "chunk_hash": [],
                            }
                        
                        logger.info(f"[403 폴백 크롤링] {len(discovered_files)}개 파일 발견: {start_url}")
                        
                        # ✅ 파일 발견 시 웹소켓 메시지 전송
                        await send_message_to_socket(
                            job_id,
                            {
                                "type": "crawl_count",
                                "count": len(discovered_files),
                                "current_url": start_url,
                                "timestamp": time.time(),
                            },
                            job_manager,
                        )
                        
                        # 3. 발견된 파일들을 크롤링 결과 형태로 변환
                        crawl_results = []
                        for file_url in discovered_files:
                            crawl_results.append({
                                "source": normalize_url(file_url),
                                "title": f"Discovered URL: {file_url}",
                                "content": "",  # 나중에 실제 콘텐츠 추출
                                "snippet": "",
                                "favicon_url": "",
                                "chunk_hash": [],
                            })
                        
                        # 4. 403 폴백 처리 결과에 전체 URL 정보 추가
                        fallback_result = await process_crawl_results_with_403_fallback(
                            crawl_results=crawl_results,
                            subject=subject,
                            context=context,
                            each_progress=each_progress,
                            memo=memo,
                            original_url=start_url,
                        )
                        
                        # ✅ 전체 수집 URL 정보 추가
                        fallback_result.update({
                            "total_crawled_urls": total_crawled_urls,
                            "no_change_count": no_change_count,
                            "new_or_changed_count": fallback_result.get("total_processed_urls", 0),
                            "chunk_hash": fallback_result.get("chunk_hash", [])
                        })
                        
                        logger.info(f"[403 폴백 크롤링] 3단계 완료: 전체 {total_crawled_urls}개 URL 수집, 변경 없음 {no_change_count}개, 403 폴백으로 신규 학습 {fallback_result.get('total_processed_urls', 0)}개")
                        
                        return fallback_result
                        
                    else:
                        # 403이 아닌 경우 기존 크롤링 결과 반환
                        logger.info(f"[403 폴백 크롤링] 2단계: 403이 아님 (상태: {response.status}), 기존 크롤링 결과 반환")
                        return initial_crawl_result
                        
            except Exception as e:
                logger.error(f"[403 폴백 크롤링] 2단계 원본 URL 확인 중 오류: {start_url}, 오류: {e}")
                # 오류 발생 시 기존 크롤링 결과 반환
                logger.info(f"[403 폴백 크롤링] 2단계 오류로 인해 기존 크롤링 결과 반환")
                return initial_crawl_result
                
    except Exception as e:
        error_message = f"403 폴백 크롤링 중 오류 발생: {str(e)}"
        logger.error(f"[403 폴백 크롤링 오류] {error_message}", exc_info=True)
        await send_message_to_socket(
            job_id,
            {
                "type": "error",
                "status": "error",
                "message": error_message,
                "timestamp": time.time(),
            },
            job_manager,
        )
        raise

async def process_crawl_results_with_403_fallback(
    crawl_results: List[Dict],
    subject: str,
    context: CrawlingContext,
    each_progress: float,
    memo: str = "",
    original_url: str = "",
) -> dict:
    """
    403 폴백으로 발견된 파일들을 처리하는 함수
    """
    try:
        # Context에서 값 추출
        table_name = context.table_name
        dbname = context.dbname
        job_id = context.job_id
        job_manager = context.job_manager
        job_progress_manager = context.job_progress
        chat_bot_id = context.chat_bot_id
        
        logger.info(f"[403 폴백 결과 처리] {len(crawl_results)}개 파일 처리 시작")
        
        if not crawl_results:
            error_message = f"처리할 파일이 없음: {original_url}"
            logger.warning(f"[403 폴백 결과 처리 경고] {error_message} - 빈 결과로 처리 계속")
            await send_message_to_socket(
                job_id,
                {
                    "type": "403_crawl_warning",
                    "status": "warning",
                    "message": f"{error_message} - 빈 결과로 처리 계속",
                    "timestamp": time.time(),
                },
                job_manager,
            )
            # ValueError 대신 빈 결과로 계속 진행
            crawl_results = []

        # ✅ 개선된 처리 단계 시작 메시지
        await send_message_to_socket(
            job_id,
            {
                "type": "crawl_status",
                "status": "in_progress",
                "message": f"✅ 403 폴백으로 발견된 {len(crawl_results)}개 파일 처리 시작",
                "current_url": original_url,
                "count": len(crawl_results),
                "timestamp": time.time(),
            },
            job_manager,
        )

        # 병렬 처리 설정
        total_pages = len(crawl_results)
        
        # ✅ 빈 결과 처리: 0으로 나누기 방지
        if total_pages == 0:
            logger.info(f"[403 폴백 처리 건너뛰기] 처리할 파일이 없어 건너뜁니다: {original_url}")
            return {
                "total_processed_urls": 0,
                "total_chunks": 0,
                "chunk_count": [],
                "chunk_hash": [],
                "use_source": [],
                "source_size": [],
                "web_title": [],
                "processing_time": 0.0,
                "chunk_hash": [],
            }
            
        page_progress = each_progress / total_pages
        max_concurrent_urls = min(multiprocessing.cpu_count(), 12)
        semaphore = asyncio.Semaphore(max_concurrent_urls)

        # 시작 시간 및 상태 초기화
        start_time = time.time()
        processed_urls = {}
        total_chunks_processed = 0
        completed_count = 0
        success_count = 0  # 신규 학습된 URL 카운트
        redis_client = await get_redis()

        # 병렬 URL 처리 실행
        tasks = []
        for idx, result in enumerate(crawl_results):
            task = process_single_crawled_url(
                semaphore=semaphore,
                result=result,
                subject=subject,
                context=context,
                memo=memo,
                redis_client=redis_client,
                start_time=start_time,
                url_index=idx + 1,
                total_urls=total_pages,
                page_progress=page_progress,
            )
            tasks.append(task)

        # 병렬 실행 및 실시간 진행률 업데이트 - 순서 보장을 위한 수정
        url_task_results = [None] * len(tasks)  # 원래 순서 유지를 위한 배열
        task_to_index = {id(task): idx for idx, task in enumerate(tasks)}  # task와 인덱스 매핑
        
        for completed_task in asyncio.as_completed(tasks):
            try:
                # use_crawl_stop 확인 (세션 끊김 감지 제거)
                status = await job_manager.get_job_status(job_id)
                if status == "use_crawl_stop":
                    logger.info(f"[403 폴백 처리 중단] use_crawl_stop 감지: job_id: {job_id}")

                    for task in tasks:
                        if not task.done():
                            task.cancel()

                    await send_message_to_socket(
                        job_id,
                        {
                            "type": "crawl_status",
                            "status": "cancelled",
                            "message": "403 폴백 처리가 취소되었습니다",
                            "current_url": original_url,
                            "timestamp": time.time(),
                        },
                        job_manager,
                    )
                    return processed_urls

                # 개별 URL 처리 결과 받기
                url_result = await completed_task
                if url_result:
                    # 원래 인덱스 찾기
                    task_index = task_to_index.get(id(completed_task))
                    if task_index is not None:
                        # 올바른 order 설정 (1부터 시작)
                        url_result["order"] = task_index + 1
                        url_task_results[task_index] = url_result
                    
                    source_url = url_result["url"]
                    processed_urls[source_url] = url_result
                    total_chunks_processed += url_result["chunks"]
                    completed_count += 1

                    # 실시간 진행률 업데이트
                    progress = min(int((completed_count / total_pages) * 95), 95)

                    # ✅ 변경감지: 신규 학습된 URL 카운트
                    change_status = url_result.get("change_status", "new_or_changed")
                    if change_status == "new_or_changed":
                        success_count += 1

                    # ✅ crawl_count 메시지로 개별 파일 처리 완료 알림
                    await send_message_to_socket(
                        job_id,
                        {
                            "type": "crawl_count",
                            "count": completed_count,
                            "current_url": url_result["url"],
                            "success_url": success_count,  # 신규 학습된 URL 개수
                            "timestamp": time.time(),
                        },
                        job_manager,
                    )

            except Exception as e:
                logger.error(f"403 폴백 개별 URL 처리 중 오류: {e}")

        # 완료 처리
        total_processing_time = round(time.time() - start_time, 2)

        # 결과 정리
        chunk_count_list = []
        use_source_list = []
        source_size_list = []
        web_title_list = []
        chunk_hash_list = []
        use_source = {}
        sub_change_mode_on = False

        # URL을 순서대로 정렬 (order 기준)
        sorted_urls = sorted(processed_urls.items(), key=lambda x: x[1].get("order", 0))

        for source_url, url_result in sorted_urls:
            chunk_count_list.append(url_result.get("chunks", 0))
            use_source_list.append(source_url)
            source_size = url_result.get("source_size", [0])
            if isinstance(source_size, list) and len(source_size) > 0:
                source_size_list.append(source_size[0])
            else:
                source_size_list.append(0)
            try:
                if not sub_change_mode_on and await _subject_exists(dbname, source_url, chat_bot_id):
                    web_title_list.append("")
                else:
                    web_title_list.append(url_result.get("title", ""))
            except Exception:
                web_title_list.append(url_result.get("title", ""))
            chunk_hash_list.append(url_result.get("chunk_hash", ""))
            use_source[source_url] = {
                "chunks": url_result.get("chunks", 0),
                "favicon_url": url_result.get("favicon_url", ""),
                "processing_time": url_result.get("processing_time", 0),
                "order": url_result.get("order", 0)
            }

        # ✅ 403 폴백 처리에서는 변경감지가 적용되지 않으므로 모든 URL을 신규 학습으로 처리
        logger.info(f"📊 [최종 결과] 수집 URL : {len(processed_urls)}개, 변경 사항 없음 : 0개, 변경 감지 - 신규 학습: {len(processed_urls)}개")
        
        logger.info(
            f"[403 폴백 크롤링 작업 완료] 총 처리된 URL 수: {len(processed_urls)}, "
            f"총 청크: {total_chunks_processed}, 처리 시간: {total_processing_time}초"
        )

        return {
            "total_processed_urls": len(processed_urls),
            "total_chunks": total_chunks_processed,
            "chunk_count": chunk_count_list,
            "use_source": use_source_list,
            "source_size": source_size_list,
            "web_title": web_title_list,
            "processing_time": total_processing_time,
            "chunk_hash": chunk_hash_list,
            "success_url": success_count,  # 신규 학습된 URL 최종 개수
        }

    except Exception as e:
        error_message = f"403 폴백 결과 처리 중 오류 발생: {str(e)}"
        logger.error(f"[403 폴백 결과 처리 오류] {error_message}", exc_info=True)
        await send_message_to_socket(
            job_id,
            {
                "type": "error",
                "status": "error",
                "message": error_message,
                "current_url": original_url,
                "timestamp": time.time(),
                "chunk_hash": [],
            },
            job_manager,
        )
        raise
