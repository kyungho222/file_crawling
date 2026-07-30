import logging
import json
import re
from config.settings import connect_db, return_connection
from utils.logging_util import LoggerSingleton
from utils.text_sanitize import remove_emoji
from backend.shared.config import Config
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
import asyncio
import time
from datetime import date, datetime
from models.long_term_table_models import ConversationVector
from utils.hash_policy import hash_generation_disabled, sha1_hex_utf8

logger = LoggerSingleton.get_logger(logger_name="db.db_operations", level=logging.INFO)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CONTENT_METADATA_COMMON_KEYS = (
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
)

# -----------------------------------------------------------------------------
# Schema compatibility helpers
# - 운영/레거시 환경에서 td_*_training_data의 컬럼 구성이 다를 수 있음.
# - information_schema를 조회해 실제 존재하는 컬럼만 INSERT/UPDATE에 포함한다.
# -----------------------------------------------------------------------------
_TABLE_COLUMNS_CACHE = {}
_TABLE_COLUMNS_TTL_SEC = 300.0
_PG_LEARN_WRITE_SEMAPHORE: asyncio.Semaphore | None = None
_PG_LEARN_WRITE_SEMAPHORE_LIMIT: int | None = None


def _get_pg_statement_timeout_ms() -> int:
    try:
        value = int(getattr(Config, "POSTGRES_STATEMENT_TIMEOUT_MS", 180000) or 180000)
    except Exception:
        value = 180000
    return max(10000, min(900000, value))


def _get_pg_retry_count() -> int:
    try:
        value = int(getattr(Config, "POSTGRES_QUERY_RETRY_COUNT", 2) or 2)
    except Exception:
        value = 2
    return max(0, min(5, value))


def _get_pg_retry_delay_sec() -> float:
    try:
        value = float(getattr(Config, "POSTGRES_QUERY_RETRY_DELAY_SEC", 0.5) or 0.5)
    except Exception:
        value = 0.5
    return max(0.05, min(30.0, value))


def _is_pg_retryable_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    retry_hints = (
        "statement timeout",
        "canceling statement due to statement timeout",
        "deadlock detected",
        "could not serialize access",
        "lock timeout",
        "too many connections",
        "connection was closed",
        "connection reset",
        "terminating connection",
    )
    return any(hint in msg for hint in retry_hints)


def _get_pg_learn_write_semaphore() -> asyncio.Semaphore:
    global _PG_LEARN_WRITE_SEMAPHORE, _PG_LEARN_WRITE_SEMAPHORE_LIMIT
    try:
        limit = int(getattr(Config, "POSTGRES_LEARN_WRITE_MAX_CONCURRENCY", 2) or 2)
    except Exception:
        limit = 2
    limit = max(1, min(16, limit))
    if _PG_LEARN_WRITE_SEMAPHORE is None or _PG_LEARN_WRITE_SEMAPHORE_LIMIT != limit:
        _PG_LEARN_WRITE_SEMAPHORE = asyncio.Semaphore(limit)
        _PG_LEARN_WRITE_SEMAPHORE_LIMIT = limit
    return _PG_LEARN_WRITE_SEMAPHORE


def _normalize_learning_payload(data):
    """
    학습/크롤링 데이터 페이로드 키를 레거시/신규 컬럼명과 호환되게 보강한다.
    - chunk_num <-> chunk_number

    NOTE: 실제 INSERT/UPDATE에 포함할지는 테이블 스키마 필터링에서 결정한다.
    """
    working = dict(data or {})

    if "chunk_num" in working and "chunk_number" not in working:
        working["chunk_number"] = working.get("chunk_num")
    elif "chunk_number" in working and "chunk_num" not in working:
        working["chunk_num"] = working.get("chunk_number")

    return working


def _metadata_to_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return dict(parsed)
        except Exception:
            return {}
    return {}


def _format_content_metadata_timestamp(value) -> str:
    """Normalize JSONB metadata timestamps to `YYYY-MM-DD HH:MM:SS`."""
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S")
    raw = str(value).strip()
    if not raw:
        return ""
    normalized = raw.replace("T", " ").replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    match = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?", raw)
    if match:
        year, month, day, hour, minute, second = match.groups()
        try:
            parsed = datetime(
                int(year),
                int(month),
                int(day),
                int(hour or 0),
                int(minute or 0),
                int(second or 0),
            )
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return raw
    return raw


def _normalize_content_metadata_timestamps(value):
    meta = _metadata_to_dict(value)
    if not meta:
        return value
    changed = False
    for key in ("created_at", "updated_at"):
        if key in meta and meta.get(key) not in (None, ""):
            formatted = _format_content_metadata_timestamp(meta.get(key))
            if formatted and formatted != meta.get(key):
                meta[key] = formatted
                changed = True
    return meta if changed else value


def _first_nonempty_string(*values) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _metadata_put_if_present(meta: dict, key: str, *values) -> None:
    if meta.get(key) not in (None, ""):
        return
    for value in values:
        if value in (None, ""):
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        meta[key] = value
        return


def _metadata_first_value(*values):
    for value in values:
        if value in (None, ""):
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        return value
    return None


def _file_content_metadata_source_only(working: dict, previous_content=None) -> dict:
    meta = _metadata_to_dict(working.get("content_metadata"))
    file_info = _metadata_to_dict(working.get("file_info"))
    original_meta = _metadata_to_dict(file_info.get("original_meta"))
    clean_meta = dict(original_meta or meta)
    source = _first_nonempty_string(
        working.get("source_page"),
        working.get("sourcePage"),
        working.get("post_url"),
        working.get("postUrl"),
        meta.get("source_page"),
        meta.get("post_url"),
        file_info.get("source_page"),
        file_info.get("post_url"),
        original_meta.get("source_page"),
        original_meta.get("post_url"),
        working.get("source_url"),
        working.get("sourceUrl"),
        meta.get("source_url"),
        file_info.get("source_url"),
        original_meta.get("source_url"),
        working.get("file_url"),
        working.get("fileUrl"),
        working.get("download_url"),
        working.get("downloadUrl"),
        meta.get("file_url"),
        meta.get("download_url"),
        file_info.get("file_url"),
        file_info.get("download_url"),
        working.get("url"),
        working.get("source"),
        previous_content,
        meta.get("source"),
    )
    _metadata_put_if_present(clean_meta, "source_url", source)
    _metadata_put_if_present(
        clean_meta,
        "chunk_index",
        meta.get("chunk_index"),
        meta.get("chunk_num"),
        meta.get("chunk_number"),
        working.get("chunk_index"),
        working.get("chunk_num"),
        working.get("chunk_number"),
    )
    for key in ("content_length", "content_hash", "update_frequency"):
        _metadata_put_if_present(clean_meta, key, meta.get(key), working.get(key), file_info.get(key), original_meta.get(key))
    _metadata_put_if_present(clean_meta, "update_frequency", "1_day")
    created_value = _metadata_first_value(
        meta.get("content_created_at"),
        meta.get("created_at"),
        working.get("content_created_at"),
        working.get("created_at"),
        file_info.get("content_created_at"),
        file_info.get("created_at"),
        original_meta.get("content_created_at"),
        original_meta.get("created_at"),
        working.get("reg_date"),
        file_info.get("reg_date"),
        original_meta.get("reg_date"),
    )
    updated_value = _metadata_first_value(
        meta.get("content_updated_at"),
        meta.get("updated_at"),
        working.get("content_updated_at"),
        working.get("updated_at"),
        file_info.get("content_updated_at"),
        file_info.get("updated_at"),
        original_meta.get("content_updated_at"),
        original_meta.get("updated_at"),
        created_value,
    )
    _metadata_put_if_present(clean_meta, "content_created_at", created_value)
    _metadata_put_if_present(clean_meta, "created_at", created_value)
    _metadata_put_if_present(clean_meta, "content_updated_at", updated_value)
    _metadata_put_if_present(clean_meta, "updated_at", updated_value)
    _metadata_put_if_present(clean_meta, "date_rerank_target", True)
    _metadata_put_if_present(clean_meta, "source_category", "file")
    _metadata_put_if_present(
        clean_meta,
        "content_author",
        meta.get("content_author"),
        working.get("content_author"),
        working.get("author"),
        file_info.get("content_author"),
        file_info.get("author"),
        original_meta.get("content_author"),
        original_meta.get("author"),
        file_info.get("department"),
        original_meta.get("department"),
    )
    return {key: value for key, value in clean_meta.items() if value not in (None, "")}


def _apply_alternate_urls_to_content(working: dict) -> None:
    """
    호출부에서 url / file_url 등으로 전달된 값을 content 컬럼에 반영한다.
    (기존 insert_data 내 url_source 블록과 동일)
    """
    url_source = (
        working.get("url")
        or working.get("file_url")
        or working.get("fileUrl")
        or working.get("download_url")
        or working.get("downloadUrl")
        or working.get("source_url")
        or working.get("sourceUrl")
    )
    if isinstance(url_source, str) and url_source.strip():
        working["content"] = url_source.strip()


def _apply_file_content_equals_subject(working: dict, *, incoming_content_type) -> None:
    """
    호출부에서 넘긴 원래 content_type 이 \"file\" 인 학습 행만
    content 컬럼을 subject 와 동일하게 맞춘다.
    (url_edu 페이지 URL 학습은 content_type \"url\" 등으로 들어오므로 content=페이지 URL 유지)

    덮어쓰기 전 content(다운로드 URL·로컬 경로 등)는 content_metadata 에 보관한다.
    """
    if str(incoming_content_type or "").strip().lower() != "file":
        return
    subj = working.get("subject")
    if not isinstance(subj, str) or not subj.strip():
        return
    subj_stripped = subj.strip()
    prev = working.get("content")
    working["content_metadata"] = _file_content_metadata_source_only(working, previous_content=prev)
    working["content"] = subj_stripped


def _ensure_file_metadata_source_and_chunk(working: dict, *, incoming_content_type) -> None:
    if str(incoming_content_type or "").strip().lower() != "file":
        return
    working["content_metadata"] = _file_content_metadata_source_only(working)


def _payload_is_file_learning_row(working: dict, incoming_content_type=None) -> bool:
    if str(incoming_content_type or working.get("content_type") or "").strip().lower() == "file":
        return True
    meta = _metadata_to_dict(working.get("content_metadata"))
    if str(meta.get("source_category") or "").strip().lower() == "file":
        return True
    file_only_keys = {
        "file_url",
        "fileUrl",
        "download_url",
        "downloadUrl",
        "file_name",
        "filename",
        "file_path",
        "local_path",
        "file_size",
        "filesize",
        "file_info",
    }
    return any(key in working for key in file_only_keys) or any(key in meta for key in file_only_keys)


def _normalize_learning_content_metadata_for_write(working: dict, *, incoming_content_type=None) -> None:
    if "content_metadata" not in working and not _payload_is_file_learning_row(working, incoming_content_type):
        return
    if _payload_is_file_learning_row(working, incoming_content_type):
        working["content_metadata"] = _file_content_metadata_source_only(working)
    elif "content_metadata" in working:
        working["content_metadata"] = _normalize_content_metadata_timestamps(working.get("content_metadata"))


def _prepare_learning_row_common(working: dict) -> None:
    """
    insert_data / insert_data_with_metadata 공통: subject 보강, content_type=file 정책,
    url 키 반영, (원래 content_type==file 인 경우에만) content=subject 정렬.
    """
    try:
        if not working.get("subject"):
            cont = working.get("content")
            if isinstance(cont, str) and cont.strip():
                try:
                    import os as _os

                    working["subject"] = _os.path.basename(cont) or cont
                except Exception:
                    working["subject"] = cont
    except Exception:
        pass

    try:
        # 덮어쓰기 전 원래 의도(url vs file 학습 등) — 아래에서 PG 저장용으로 \"file\" 로 통일하기 전에 보관
        incoming_ctype = working.get("content_type")
        _apply_alternate_urls_to_content(working)
        _apply_file_content_equals_subject(working, incoming_content_type=incoming_ctype)
        _normalize_learning_content_metadata_for_write(working, incoming_content_type=incoming_ctype)
    except Exception:
        pass


def _split_schema_table(table_ident: str):
    if not table_ident:
        return None, ""
    if "." in table_ident:
        schema, name = table_ident.split(".", 1)
        return schema, name
    return None, table_ident


async def _get_existing_columns(table_ident: str, *, dbname=None):
    """
    information_schema를 조회해 테이블의 실제 컬럼 목록을 반환한다.
    - schema가 명시된 경우에만 조회한다(public.td_* 등).
    - 조회 실패 시 None 반환(기존 로직 유지).
    """
    schema, table_name = _split_schema_table(table_ident)
    if not schema or not table_name:
        return None

    # Postgres는 "미인용 식별자"를 소문자로 폴딩한다.
    # INSERT/UPDATE에서 td_BX... 처럼 대문자가 섞여와도 실제 테이블은 보통 td_bx... 로 생성/조회된다.
    # information_schema에는 폴딩된(table_name이 소문자) 값이 저장되므로, 캐시/조회는 소문자 키를 기본으로 사용한다.
    normalized_table_name = table_name.lower()
    cache_key = (str(dbname or Config.DB_NAME or ""), f"{schema}.{normalized_table_name}")
    now = time.time()
    cached = _TABLE_COLUMNS_CACHE.get(cache_key)
    if cached:
        ts, cols = cached
        if now - ts <= _TABLE_COLUMNS_TTL_SEC:
            return cols

    try:
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"

        # 1) 우선 소문자 폴딩된 테이블명으로 조회
        rows = await execute_query(query, [schema, normalized_table_name], fetch=True, dbname=dbname)

        # 2) 만약 비어있고 원래 테이블명이 달랐다면(=대문자 포함 가능성), 원본으로도 한 번 더 조회
        #    (혹시라도 과거에 따옴표로 대문자 테이블을 만든 특이 케이스를 대비)
        if not rows and table_name != normalized_table_name:
            rows = await execute_query(query, [schema, table_name], fetch=True, dbname=dbname)

        cols = set()
        for r in rows or []:
            col = None
            try:
                col = r.get("column_name") if hasattr(r, "get") else r["column_name"]
            except Exception:
                try:
                    col = r["column_name"]
                except Exception:
                    col = None
            if col:
                cols.add(str(col))
        _TABLE_COLUMNS_CACHE[cache_key] = (now, cols)
        return cols
    except Exception as e:
        # 컬럼 조회 실패는 치명적 오류가 아니므로, 기존 방식대로 진행(쿼리 실패 시 상위에서 로그/예외 처리)
        try:
            logger.debug(
                "[LEARN-POSTGRES] [SCHEMA] table column lookup failed | db=%s table=%s err=%s",
                dbname or Config.DB_NAME,
                table_ident,
                str(e)[:300],
            )
        except Exception:
            pass
        return None


async def _get_varchar_max_lengths(table_ident: str, *, dbname=None) -> dict:
    """
    information_schema에서 character varying(n) 컬럼의 최대 길이를 조회한다.
    - 조회 실패 시 빈 dict 반환
    """
    schema, table_name = _split_schema_table(table_ident)
    if not schema or not table_name:
        return {}

    normalized_table_name = table_name.lower()
    cache_key = (str(dbname or Config.DB_NAME or ""), f"{schema}.{normalized_table_name}", "varchar_max")
    now = time.time()
    cached = _TABLE_COLUMNS_CACHE.get(cache_key)
    if cached:
        ts, meta = cached
        if now - ts <= _TABLE_COLUMNS_TTL_SEC:
            return meta

    try:
        query = """
            SELECT column_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
              AND data_type = 'character varying'
              AND character_maximum_length IS NOT NULL
        """
        rows = await execute_query(query, [schema, normalized_table_name], fetch=True, dbname=dbname)
        if not rows and table_name != normalized_table_name:
            rows = await execute_query(query, [schema, table_name], fetch=True, dbname=dbname)

        meta = {}
        for r in rows or []:
            try:
                col = r.get("column_name") if hasattr(r, "get") else r["column_name"]
                mx = r.get("character_maximum_length") if hasattr(r, "get") else r["character_maximum_length"]
            except Exception:
                continue
            if col and mx:
                try:
                    meta[str(col)] = int(mx)
                except Exception:
                    pass

        _TABLE_COLUMNS_CACHE[cache_key] = (now, meta)
        return meta
    except Exception:
        return {}


def _shorten_for_varchar(value: str, max_len: int) -> str:
    """
    varchar(max_len) 컬럼에 넣기 위해 문자열을 안정적으로 축약한다.
    - 원본 앞부분 + "~" + sha1(원문) 일부를 붙여 충돌 가능성을 낮춘다.
    """
    if not isinstance(value, str) or not isinstance(max_len, int) or max_len <= 0:
        return value
    if len(value) <= max_len:
        return value

    if hash_generation_disabled():
        digest = str(len(value))
    else:
        digest = (sha1_hex_utf8(value) or str(len(value)))
    suffix = "~" + digest[:12]
    prefix_len = max_len - len(suffix)
    if prefix_len <= 0:
        return digest[:max_len]
    return value[:prefix_len] + suffix


def _quote_ident(ident: str) -> str:
    """
    안전한 SQL 식별자 quoting.
    - 컬럼명/테이블명은 알파벳/숫자/underscore만 허용 (SQL injection 및 컬럼 밀림 방지)
    """
    if not ident or not isinstance(ident, str):
        raise ValueError("Invalid identifier (empty)")
    if not _IDENT_RE.fullmatch(ident):
        raise ValueError(f"Invalid identifier: {ident!r}")
    return f"\"{ident}\""

def _validate_ident(ident: str) -> str:
    """식별자 유효성만 검사하고 원문을 반환(테이블명은 미인용으로 써서 PG의 소문자 폴딩 규칙을 따른다)."""
    if not ident or not isinstance(ident, str):
        raise ValueError("Invalid identifier (empty)")
    if not _IDENT_RE.fullmatch(ident):
        raise ValueError(f"Invalid identifier: {ident!r}")
    return ident


def _resolve_table_ident(table: str) -> str:
    """
    테이블 식별자 결정.
    - 기본적으로는 검증된 테이블명(예: td_xxx_training_data)을 그대로 사용한다.
    - td_{chatid}_training_data 형태의 테이블은 명시적으로 public 스키마를 사용하도록 'public.' 접두어를 붙인다.
    """
    if not table or not isinstance(table, str):
        raise ValueError("Invalid table identifier")
    # training table 패턴일 경우 public 스키마로 명시
    import re

    if re.fullmatch(r"td_[A-Za-z0-9]+_training_data", table):
        # table 부분은 검증된 형태이므로 안전하게 연결
        # PG 미인용 식별자는 소문자로 폴딩되므로, 조회/캐시 일관성을 위해 소문자로 정규화한다.
        return f"public.{table.lower()}"
    # 기본: 테이블명은 그대로 사용 (검증)
    return _validate_ident(table)


async def execute_query(query, params=None, fetch=False, dbname=None):
    """
    SQL 쿼리를 실행하는 범용 함수.

    Args:
        query (str): 실행할 SQL 쿼리
        params (tuple, optional): 쿼리에 전달할 매개변수. 기본값은 None.
        fetch (bool, optional): 데이터를 반환할지 여부. 기본값은 False.
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.

    Returns:
        list or None: fetch=True일 경우, 쿼리 결과를 반환.
    """
    safe_params = params
    try:
        if isinstance(params, (list, tuple)) and len(params) > 0:
            safe_params = list(params)
            for i, v in enumerate(safe_params):
                if isinstance(v, str) and len(v) > 500:
                    safe_params[i] = v[:500] + " ...(truncated)"
    except Exception:
        safe_params = params

    max_attempts = max(1, _get_pg_retry_count() + 1)
    for attempt in range(1, max_attempts + 1):
        conn = None
        started = time.perf_counter()
        try:
            conn = await connect_db(dbname)
            try:
                await conn.execute(f"SET statement_timeout = {_get_pg_statement_timeout_ms()}")
            except Exception:
                pass

            if fetch:
                result = await conn.fetch(query, *(params or ()))
            else:
                result = await conn.execute(query, *(params or ()))

            if attempt > 1:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                logger.info(
                    "[LEARN-POSTGRES] [SQL-RECOVERED] db=%s host=%s attempt=%s/%s elapsed_ms=%s query=%s",
                    dbname or Config.DB_NAME,
                    Config.DB_HOST,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    query,
                )
            return result
        except Exception as e:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            retryable = _is_pg_retryable_error(e)
            if retryable and attempt < max_attempts:
                wait_sec = _get_pg_retry_delay_sec() * attempt
                logger.warning(
                    "[LEARN-POSTGRES] [SQL-RETRY] db=%s host=%s attempt=%s/%s elapsed_ms=%s wait=%.2fs err=%s query=%s params=%s",
                    dbname or Config.DB_NAME,
                    Config.DB_HOST,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    wait_sec,
                    e,
                    query,
                    safe_params,
                )
                await asyncio.sleep(wait_sec)
                continue
            logger.error(
                f"[LEARN-POSTGRES] [SQL-FAIL] db={dbname or Config.DB_NAME} host={Config.DB_HOST} elapsed_ms={elapsed_ms} err={e} query={query} params={safe_params}",
                exc_info=True,
            )
            raise
        finally:
            if conn:
                await return_connection(conn, dbname)


async def insert_data(table, data, dbname=None):
    # 1. 실제 DB 컬럼명(보내주신 목록)과 1:1로 매칭되는 화이트리스트 구성
    allowed_fields = [
        "content_author",
        "content",          # 실제 컬럼명: content
        "content_type",     # 실제 컬럼명: content_type
        "subject",          # 실제 컬럼명: subject (또는 title)
        "text_data",        # 실제 컬럼명: text_data (필수!)
        "chunk_num",        # 실제 컬럼명: chunk_num
        "page_num",         # 실제 컬럼명: page_num
        "memo",             # 실제 컬럼명: memo
        "web_title",        # 실제 컬럼명: web_title
        "embedding",        # 실제 컬럼명: embedding (필수!)
        "content_metadata", # 실제 컬럼명: content_metadata
        "memo_embedding"    # 실제 컬럼명: memo_embedding (추가됨)
    ]

    # 2. 테이블 식별자 해석
    table_ident = _resolve_table_ident(str(table))
    
    # 3. 데이터 정규화 (전달된 data의 키를 표준 allowed_fields 키로 변환)
    working = _normalize_learning_payload(data)
    # subject 보강, content_type=file, url 키 반영, content=subject (원본 URL/경로는 content_metadata)
    _prepare_learning_row_common(working)

    # 크롤링 후 DB 저장 전: 문자열 필드에서 이모지 제거
    for k in list(working.keys()):
        v = working.get(k)
        if isinstance(v, str):
            working[k] = remove_emoji(v)
    
    # 4. 허용 목록에 있고 실제 데이터가 존재하는 컬럼 추출
    columns = [k for k in allowed_fields if k in working]

    # 5. 실제 스키마(DB)에 존재하는 컬럼인지 2차 검증 (안전장치)
    existing_cols = await _get_existing_columns(table_ident, dbname=dbname)
    if existing_cols is not None:
        columns = [c for c in columns if c in existing_cols]

    # 6. 만약 결과가 없다면 로깅 후 중단
    if not columns:
        logger.warning(
            f"[LEARN-POSTGRES] [DB-INSERT-SKIP] 테이블: {table} | "
            f"입력값과 DB 컬럼명이 일치하지 않음. 입력키: {list((data or {}).keys())}"
        )
        return

    # 로그에는 민감하거나 대용량인 필드를 제외하고 허용된 페이로드만 출력
    # 임베딩 벡터는 매우 크므로 로그 출력에서 제외
    debug_payload = {k: working.get(k) for k in columns if k != "embedding"}
    logger.info(f"[LEARN-POSTGRES] [DB-INSERT] 테이블: {table}")

    # varchar 길이 메타 정보(있는 경우) 조회
    varchar_max = await _get_varchar_max_lengths(table_ident, dbname=dbname)

    # 컬럼/값 리스트 생성
    values = []
    for c in columns:
        v = working.get(c)
        # DB가 varchar(n)로 제한한 컬럼에 대해 안전하게 축약
        if isinstance(v, str) and c in varchar_max and len(v) > varchar_max[c]:
            shortened = _shorten_for_varchar(v, varchar_max[c])
            try:
                logger.warning(
                    f"[LEARN-POSTGRES] [DB-INSERT-TRUNCATE] 테이블: {table} | 컬럼: {c} | "
                    f"len={len(v)} -> {len(shortened)} (max={varchar_max[c]})"
                )
            except Exception:
                pass
            v = shortened
        if c == "content_metadata" and v is not None:
            # dict면 JSON으로 변환하여 jsonb 캐스팅과 호환
            if isinstance(v, dict):
                try:
                    v = json.dumps(v, ensure_ascii=False)
                except Exception:
                    v = None
        values.append(v)

    keys = ", ".join(_quote_ident(k) for k in columns)
    placeholders = []
    for i, key in enumerate(columns):
        if key == "content_metadata":
            placeholders.append(f"${i+1}::jsonb")
        elif key == "embedding":
            placeholders.append(f"${i+1}::vector(1536)")
        else:
            placeholders.append(f"${i+1}")

    placeholders_str = ", ".join(placeholders)
    # ✅ 중요: Postgres는 미인용 식별자를 소문자로 폴딩한다.
    # chat_id에 대문자가 포함되면("972SG629") 기존 테이블이 td_972sg629_training_data 로 생성되어 있을 수 있으므로,
    # 테이블명은 따옴표로 감싸지 않고(=폴딩 규칙을 따르고), 안전성은 정규식으로만 보장한다.
    include_created_at = True
    if existing_cols is not None and "created_at" not in existing_cols:
        include_created_at = False

    if include_created_at:
        query = f"INSERT INTO {table_ident} ({keys}, created_at) VALUES ({placeholders_str}, NOW())"
    else:
        query = f"INSERT INTO {table_ident} ({keys}) VALUES ({placeholders_str})"

    try:
        async with _get_pg_learn_write_semaphore():
            exec_result = await execute_query(query, values, dbname=dbname)
        logger.info(
            f"[LEARN-POSTGRES] [DB-INSERT-OK] 테이블: {table} | db={dbname or Config.DB_NAME} host={Config.DB_HOST} result={exec_result}"
        )
    except Exception as e:
        logger.error(
            f"[LEARN-POSTGRES] [DB-INSERT-FAIL] 테이블: {table} | db={dbname or Config.DB_NAME} host={Config.DB_HOST} err={e}",
            exc_info=True,
        )
        raise


async def insert_data_with_metadata(table, data, dbname=None):
    """
    content_metadata JSONB 필드를 포함한 데이터 삽입 함수.

    Args:
        table (str): 테이블 이름.
        data (dict): 삽입할 데이터 (컬럼명: 값).
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.
    """
    table_ident = _resolve_table_ident(str(table))
    working = _normalize_learning_payload(data)
    _prepare_learning_row_common(working)

    # JSONB 필드 타입 변환
    processed_data = {}
    for key, value in working.items():
        if key == 'content_metadata' and value is not None:
            # content_metadata가 딕셔너리인 경우 JSON 문자열로 변환
            if isinstance(value, dict):
                try:
                    import json
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"JSONB 변환 실패: {key}={value}, 오류: {e}")
                    processed_data[key] = None
            elif isinstance(value, str):
                # 이미 문자열인 경우 그대로 사용
                processed_data[key] = value
            else:
                logger.warning(f"JSONB 예상치 못한 타입: {key}={type(value)}")
                processed_data[key] = None
        else:
            processed_data[key] = value

    # 크롤링 후 DB 저장 전: 문자열 필드에서 이모지 제거
    for k in list(processed_data.keys()):
        v = processed_data.get(k)
        if isinstance(v, str):
            processed_data[k] = remove_emoji(v)

    # 실제 스키마에 존재하는 컬럼만 삽입하도록 필터링(레거시 호환)
    existing_cols = await _get_existing_columns(table_ident, dbname=dbname)
    if existing_cols is not None:
        processed_data = {k: v for k, v in processed_data.items() if k in existing_cols}

    columns = list(processed_data.keys())
    if not columns:
        logger.warning(
            f"[LEARN-POSTGRES] [DB-INSERT-SKIP] 테이블: {table} | 전달된 데이터에 존재하는 컬럼이 없음. 입력키: {list((data or {}).keys())}"
        )
        return
    values = [processed_data[c] for c in columns]
    keys = ", ".join(_quote_ident(k) for k in columns)
    
    # JSONB 필드 및 임베딩 필드에 대한 명시적 캐스팅 추가
    placeholders = []
    for i, key in enumerate(columns):
        if key == 'content_metadata':
            placeholders.append(f"${i+1}::jsonb")  # JSONB 캐스팅
        elif key == 'embedding':
            placeholders.append(f"${i+1}::vector(1536)")  # Vector 차원 명시적 캐스팅
        else:
            placeholders.append(f"${i+1}")
    
    placeholders_str = ", ".join(placeholders)

    include_created_at = True
    if existing_cols is not None and "created_at" not in existing_cols:
        include_created_at = False

    if include_created_at:
        query = f"INSERT INTO {table_ident} ({keys}, created_at) VALUES ({placeholders_str}, NOW())"
    else:
        query = f"INSERT INTO {table_ident} ({keys}) VALUES ({placeholders_str})"
    async with _get_pg_learn_write_semaphore():
        await execute_query(query, values, dbname=dbname)


async def update_data(table, data, conditions, dbname=None):
    """
    데이터 업데이트 함수.
    """
    table_ident = _resolve_table_ident(str(table))
    working = _normalize_learning_payload(data)
    _normalize_learning_content_metadata_for_write(working, incoming_content_type=working.get("content_type"))
    existing_cols = await _get_existing_columns(table_ident, dbname=dbname)
    if existing_cols is not None:
        working = {k: v for k, v in working.items() if k in existing_cols}

    # 크롤링 후 DB 저장 전: 문자열 필드에서 이모지 제거
    for k in list(working.keys()):
        v = working.get(k)
        if isinstance(v, str):
            working[k] = remove_emoji(v)

    set_clauses = []
    for i, key in enumerate(working.keys(), 1):
        if key == 'embedding':
             set_clauses.append(f"{key} = ${i}::vector(1536)")
        else:
             set_clauses.append(f"{key} = ${i}")
    
    set_clause = ", ".join(set_clauses)
    
    where_clause = " AND ".join(
        f"{key} = ${i}" for i, key in enumerate(conditions.keys(), len(working) + 1)
    )

    include_created_at = True
    if existing_cols is not None and "created_at" not in existing_cols:
        include_created_at = False

    if include_created_at:
        query = f"UPDATE {table_ident} SET {set_clause}, created_at = NOW() WHERE {where_clause}"
    else:
        query = f"UPDATE {table_ident} SET {set_clause} WHERE {where_clause}"

    params = list(working.values()) + list(conditions.values())
    await execute_query(query, params, dbname=dbname)


async def update_data_with_metadata(table, data, conditions, dbname=None):
    """
    content_metadata JSONB 필드를 포함한 데이터 업데이트 함수.

    Args:
        table (str): 테이블 이름.
        data (dict): 업데이트할 데이터 (컬럼명: 값).
        conditions (dict): 조건 필드와 값.
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.
    """
    table_ident = _resolve_table_ident(str(table))
    working = _normalize_learning_payload(data)
    _normalize_learning_content_metadata_for_write(working, incoming_content_type=working.get("content_type"))
    existing_cols = await _get_existing_columns(table_ident, dbname=dbname)

    # JSONB 필드 타입 변환
    processed_data = {}
    for key, value in working.items():
        if key == 'content_metadata' and value is not None:
            # content_metadata가 딕셔너리인 경우 JSON 문자열로 변환
            if isinstance(value, dict):
                try:
                    import json
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"JSONB 변환 실패: {key}={value}, 오류: {e}")
                    processed_data[key] = None
            elif isinstance(value, str):
                # 이미 문자열인 경우 그대로 사용
                processed_data[key] = value
            else:
                logger.warning(f"JSONB 예상치 못한 타입: {key}={type(value)}")
                processed_data[key] = None
        else:
            processed_data[key] = value

    if existing_cols is not None:
        processed_data = {k: v for k, v in processed_data.items() if k in existing_cols}

    # 크롤링 후 DB 저장 전: 문자열 필드에서 이모지 제거
    for k in list(processed_data.keys()):
        v = processed_data.get(k)
        if isinstance(v, str):
            processed_data[k] = remove_emoji(v)
    
    # SET 절에 JSONB 및 Vector 캐스팅 추가
    set_clauses = []
    for i, key in enumerate(processed_data.keys(), 1):
        if key == 'content_metadata':
            set_clauses.append(f"{key} = ${i}::jsonb")  # JSONB 캐스팅
        elif key == 'embedding':
            set_clauses.append(f"{key} = ${i}::vector(1536)")  # Vector 차원 명시적 캐스팅
        else:
            set_clauses.append(f"{key} = ${i}")
    
    set_clause = ", ".join(set_clauses)
    where_clause = " AND ".join(
        f"{key} = ${i}" for i, key in enumerate(conditions.keys(), len(processed_data) + 1)
    )

    include_created_at = True
    if existing_cols is not None and "created_at" not in existing_cols:
        include_created_at = False

    if include_created_at:
        query = f"UPDATE {table_ident} SET {set_clause}, created_at = NOW() WHERE {where_clause}"
    else:
        query = f"UPDATE {table_ident} SET {set_clause} WHERE {where_clause}"
    params = list(processed_data.values()) + list(conditions.values())
    await execute_query(query, params, dbname=dbname)


async def delete_data(table, conditions, dbname=None):
    """
    테이블에서 데이터를 삭제하는 함수.
    - 학습 중복 제거 등에서 사용.
    - asyncpg execute 결과("DELETE N")를 파싱해 bool을 반환한다.
    """
    if not conditions or not isinstance(conditions, dict):
        raise ValueError("delete_data requires non-empty conditions dict")

    keys = list(conditions.keys())
    where_clause = " AND ".join(f"{_quote_ident(k)} = ${i+1}" for i, k in enumerate(keys))
    params = [conditions[k] for k in keys]

    table_ident = _resolve_table_ident(str(table))
    query = f"DELETE FROM {table_ident} WHERE {where_clause}"

    # 로그용 (민감/대용량 방지)
    try:
        target_hint = conditions.get("content") or conditions.get("url") or conditions.get("subject") or ""
    except Exception:
        target_hint = ""
    if isinstance(target_hint, str) and len(target_hint) > 200:
        target_hint = target_hint[:200] + " ...(truncated)"

    logger.info(
        "[LEARN-POSTGRES] [DB-DELETE] table=%s db=%s target=%s where_keys=%s",
        table_ident,
        dbname or Config.DB_NAME,
        target_hint,
        keys,
    )

    conn = None
    try:
        conn = await connect_db(dbname)
        async with conn.transaction():
            result = await conn.execute(query, *params)
            deleted_count = 0
            try:
                deleted_count = int(str(result).split()[-1])
            except Exception:
                deleted_count = 0
            logger.info(
                "[LEARN-POSTGRES] [DB-DELETE-OK] table=%s db=%s deleted=%s",
                table_ident,
                dbname or Config.DB_NAME,
                deleted_count,
            )
            return deleted_count > 0
    except Exception as e:
        logger.error(
            "[LEARN-POSTGRES] [DB-DELETE-FAIL] table=%s db=%s err=%s",
            table_ident,
            dbname or Config.DB_NAME,
            e,
            exc_info=True,
        )
        return False
    finally:
        if conn:
            await return_connection(conn, dbname)
    # """
    # 테이블에서 데이터를 삭제하기 전 파일 URL(content) 등을 기준으로 존재 여부를 확인하고 삭제하는 함수.
    # """
    # where_clause = " AND ".join(
    #     f"{key} = ${i+1}" for i, key in enumerate(conditions.keys())
    # )
    # params = list(conditions.values())
    
    # # URL 정보 추출 (로그용)
    # target_url = conditions.get('content') or conditions.get('url') or "Unknown URL"

    # try:
    #     conn = await connect_db(dbname)
    #     async with conn.transaction():
    #         # 1. URL 기반 존재 여부 및 건수 확인
    #         check_query = f"SELECT count(*) FROM {table} WHERE {where_clause}"
    #         existing_count = await conn.fetchval(check_query, *params)
            
    #         if existing_count == 0:
    #             logger.info(f"[Delete] 기존 데이터 없음(중복 아님): Table={table} | URL={target_url}")
    #             return False

    #         # 2. 삭제 수행 (중복 데이터 제거)
    #         query = f"DELETE FROM {table} WHERE {where_clause}"
    #         logger.info(f"[Delete] 중복 데이터 삭제 시도: URL={target_url} | Expected={existing_count} rows")
            
    #         result = await conn.execute(query, *params)
    #         # 삭제된 행 수 확인
    #         deleted_count = int(result.split(" ")[-1])  # "DELETE 1"에서 1 추출
    #         logger.info(f"[Delete] 삭제 완료: URL={target_url} | Deleted={deleted_count} rows")
            
    #         return deleted_count > 0
    # except Exception as e:
    #     logger.error(f"DELETE 쿼리 실패: {e}")
    #     return False
    # finally:
    #     if conn:
    #         await return_connection(conn, dbname)


# SqlAlchemy 로 데이터베이스 사용하기


async def db_insert(session: AsyncSession, model):
    """
    데이터베이스에 데이터를 삽입하는 비동기 함수 (최적화된 버전)
    """
    try:
        session.add(model)  # 모델 추가
        await session.commit()  # 커밋
        await session.refresh(model)  # PK 갱신
        return model
    except Exception as e:
        await session.rollback()  # 예외 발생 시 롤백
        print(f"❌ DB 삽입 오류 발생: {e}")
        raise


async def insert_summary_memory(
    session: AsyncSession,
    summary_model: ConversationVector,
    hypothetical_question_model: ConversationVector,
    count: int = 30,
):
    """
    롱텀 메모리 저장시 30개 쌍을 유지하면서 저장하는 sql
    """
    delete_sql = f"""
    DELETE FROM conversation_vector
    WHERE message_id IN (
        SELECT message_id FROM conversation_vector
        WHERE chat_id = :chat_id AND user_id = :user_id
        GROUP BY message_id
        ORDER BY MIN(timestamp) desc
        OFFSET {Config.LONG_TERM_COUNT-1}
    )
    """
    try:
        await session.execute(
            text(delete_sql),
            {"chat_id": summary_model.chat_id, "user_id": summary_model.user_id},
        )

        refresh_model = await db_insert(session=session, model=summary_model)

        hypothetical_question_model.reference_id = refresh_model.id

        await db_insert(session=session, model=hypothetical_question_model)

        return refresh_model
    except Exception as e:
        await session.rollback()
        logger.error(f"insert_summary_memory 오류 발생: {e}")
        raise


async def chunk_db_insert(session: AsyncSession, model, response_chunks):
    """
    데이터베이스에 데이터를 삽입하는 비동기 함수 (최적화된 버전)
    """
    try:
        while len(response_chunks) == 0:
            await asyncio.sleep(0.1)

        await asyncio.sleep(0.5)

        # 전체 응답
        full_response = "".join(response_chunks)

        model.message = full_response

        await db_insert(session=session, model=model)

    except Exception as e:
        logger.error(f"chunk_db_insert 오류 발생: {e}")


async def get_message_by_id(session: AsyncSession, message_id: int, model):
    """
    데이터베이스에서 메시지를 조회하는 비동기 함수 (최적화된 버전)
    """
    try:
        result = await session.execute(
            select(model).where(model.message_id == message_id)
        )

        messages = result.scalar_one_or_none()
        if messages.is_selected:
            raise ValueError(f"message_id : {message_id} already selected")

        if messages:
            messages.is_selected = True
            await session.commit()
            await session.refresh(messages)
            return messages.message, messages.timestamp
        else:
            raise ValueError(f"message_id : {message_id} no message found")

    except Exception as e:
        await session.rollback()  # 예외 발생 시 롤백
        print(f"❌ DB 조회 오류 발생: {e}")
        raise


async def delete_data_alchemy(session: AsyncSession, table_name: str, elements: dict):
    """
    데이터베이스에서 데이터를 삭제하는 비동기 함수
    """
    try:
        message_ids = elements["message_ids"]
        # message_id 키를 제거
        elements_without_message_id = {k: v for k, v in elements.items() if k != "message_ids"}
        
        # 다른 조건들에 대한 where 절 구성
        where_conditions = " AND ".join([f"{key} = :{key}" for key in elements_without_message_id.keys()])
        
        # message_id IN (...) 조건 추가 - 수정된 부분
        placeholders = ", ".join([f":message_id_{i}" for i in range(len(message_ids))])
        delete_sql = f"DELETE FROM {table_name} WHERE {where_conditions} AND message_id IN ({placeholders})"
        
        # 파라미터에 개별 message_id 값 추가 - 수정된 부분
        params = {**elements_without_message_id}
        for i, msg_id in enumerate(message_ids):
            params[f"message_id_{i}"] = msg_id
        
        await session.execute(text(delete_sql), params)
        await session.commit()

    except Exception as e:
        await session.rollback()
        logger.error(f"delete_data_alchemy 오류 발생: {e}")
        raise
