from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"
DEFAULT_CHUNK_SIZE = 100
DEFAULT_RDBMS_RETRY_COUNT = 5
DEFAULT_RDBMS_RETRY_DELAY_SECONDS = 2.0
LOGGER = logging.getLogger("sync_post_content_type_from_exploration")
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False
_MARIA = "maria"
_MYSQL = "mysql"
_MYSQL_DB_NAMES = {"naraone"}
_STANDALONE_ENV_LOADED = False
_STANDALONE_MARIA_POOLS: dict[tuple[str, str], Any] = {}
_STANDALONE_PG_POOLS: dict[str, Any] = {}

# ──────────────────────────────────────────────
# Optional direct-run defaults
#
# CLI 인자를 매번 치기 번거로우면 아래 값만 채우고 실행해도 됩니다.
# - TARGET_DBNAME: 필수 DB명
# - TARGET_CHAT_BOT_ID: 선택 chat_bot_id 필터
# - APPLY_CHANGES=False: 기본 dry-run
# - ALLOW_MULTIPLE_BOTS=False: 다중 bot apply 기본 차단
# - TARGET_CHUNK_SIZE: LEARN_LIST.id 기준 UPDATE chunk 크기
# - RDBMS_DEADLOCK_RETRY_COUNT=5: MariaDB/MySQL deadlock/lock wait 재시도 횟수
# - RDBMS_DEADLOCK_RETRY_DELAY_SECONDS=2.0: 재시도 대기 초. attempt 배수로 증가
# - SYNC_PG_EXISTING_POST=False: 이미 LEARN_LIST가 post인 row까지 PG 보정할지 여부
# - APPLY 시 PG td_{chat_id}_training_data.content_type 도 같은 기준으로 post 반영
# - TARGET_LOG_LEVEL="INFO": 진행 상황 로그 레벨
#
# CLI 예:
#   --chat-bot-id / --chat_bot_id
# 오타 방지 별칭:
#   --chat-bpt-id / --chat_bpt_id
# ──────────────────────────────────────────────
TARGET_DBNAME = "yongin"  # 대상 DB/schema 이름. CLI --dbname 값을 주면 이 값보다 우선합니다.
TARGET_CHAT_BOT_ID = ""  # 특정 chat_bot_id만 처리하려면 입력합니다. 빈 값이면 post 대상 bot 전체를 조회합니다.
TARGET_DB_TYPE = "maria"  # LEARN_LIST가 있는 RDBMS 실행 방식입니다. "maria" 또는 "mysql"이며 PG 실행 여부와는 무관합니다.
APPLY_CHANGES = False  # False면 dry-run만 수행하고, True면 실제 UPDATE를 실행합니다. CLI --apply로도 켤 수 있습니다.
ALLOW_MULTIPLE_BOTS = False  # chat_bot_id 미지정 상태에서 여러 bot을 실제 UPDATE할지 여부입니다. 기본은 안전하게 차단합니다.
TARGET_CHUNK_SIZE = DEFAULT_CHUNK_SIZE  # LEARN_LIST.id 기준 한 번에 조회/UPDATE할 row 수입니다.
RDBMS_DEADLOCK_RETRY_COUNT = DEFAULT_RDBMS_RETRY_COUNT  # MariaDB/MySQL deadlock 또는 lock wait 발생 시 chunk UPDATE 재시도 횟수입니다.
RDBMS_DEADLOCK_RETRY_DELAY_SECONDS = DEFAULT_RDBMS_RETRY_DELAY_SECONDS  # 재시도 전 기본 대기 초입니다. 실제 대기는 attempt 배수로 증가합니다.
SYNC_PG_EXISTING_POST = False  # True면 이미 LEARN_LIST가 post인 content도 PG가 url이면 post로 보정합니다. 추가 스캔이 있어 기본은 False입니다.
JSON_OUTPUT = False  # True면 최종 요약을 사람이 읽는 텍스트 대신 JSON으로 출력합니다.
TARGET_LOG_LEVEL = "INFO"  # 진행 상황 로그 레벨입니다. DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택합니다.

# DB 접속값 직접 설정. config.py 기준으로 채웠고, 빈 문자열이면 .env/환경변수 값을 사용합니다.
MARIADB_HOST = "110.45.147.58"  # MariaDB host. config.py Config.MARIA_DB_HOST 기준입니다.
MARIADB_PORT = 3306  # MariaDB port. 일반적으로 3306입니다.
MARIADB_USER = "chatty_master"  # MariaDB user. config.py Config.MARIA_DB_USER 기준입니다.
MARIADB_PASSWORD = "dktkekf0215@#"  # MariaDB password. config.py Config.MARIA_DB_PASSWORD 기준입니다.
MARIADB_POOL_MIN = 1  # MariaDB 연결 pool 최소 개수입니다. 기존 maria_db_config.py는 Config.DB_POOL_MIN을 사용합니다.
MARIADB_POOL_MAX = 35  # MariaDB 연결 pool 최대 개수입니다. 기존 maria_db_config.py는 Config.DB_POOL_MAX를 사용합니다.

POSTGRES_HOST = "milvus.chatbaram.com"  # PostgreSQL host. config.py Config.DB_HOST 기준입니다.
POSTGRES_CHATTY_HOST = "new-milvus.chatbaram.com"  # dbname=chatty 전용 PostgreSQL host. config.py Config.CHATTY_PG_DB_HOST 기준입니다.
POSTGRES_PORT = 5432  # PostgreSQL port. 일반적으로 5432입니다.
POSTGRES_USER = "postgres"  # PostgreSQL user. config.py Config.DB_USER 기준입니다.
POSTGRES_PASSWORD = "dktkekf0215@#"  # PostgreSQL password. config.py Config.DB_PASSWORD 기준입니다.
POSTGRES_POOL_MIN = 1  # PostgreSQL 연결 pool 최소 개수입니다.
POSTGRES_POOL_MAX = 35  # PostgreSQL 연결 pool 최대 개수입니다.

MYSQL_HOST = ""  # TARGET_DB_TYPE="mysql"일 때 MySQL host. 비워두면 MYSQL_HOST 값을 사용합니다.
MYSQL_PORT = 3306  # TARGET_DB_TYPE="mysql"일 때 MySQL port입니다.
MYSQL_USER = ""  # TARGET_DB_TYPE="mysql"일 때 MySQL user. 비워두면 MYSQL_USER 값을 사용합니다.
MYSQL_PASSWORD = ""  # TARGET_DB_TYPE="mysql"일 때 MySQL password. 비워두면 MYSQL_PASS 또는 MYSQL_PASSWORD 값을 사용합니다.
MYSQL_DATABASE = ""  # TARGET_DB_TYPE="mysql"일 때 기본 database. 비워두면 실행 dbname 또는 MYSQL_DB 값을 사용합니다.

ExecuteQuery = Callable[..., Awaitable[Any]]
PgExecuteQuery = Callable[..., Awaitable[Any]]
TableNameBuilder = Callable[[str], str]
DbTypeResolver = Callable[[str, Optional[str]], str]
ChatIdResolver = Callable[[str, str], Awaitable[str | None]]


@dataclass
class ChangedLearnRowPayload:
    id: Any
    content: str = ""
    url: str = ""
    original_cate1: str = ""
    original_cate2: str = ""


@dataclass
class BotSyncResult:
    chat_bot_id: str
    learn_table: str
    pg_training_table: str = ""
    post_url_count: int = 0
    update_candidates: int = 0
    updates_applied_estimate: int = 0
    chunks_applied: int = 0
    pg_update_candidates: int = 0
    pg_updates_applied_estimate: int = 0
    pg_chunks_applied: int = 0
    match_mode: str = ""
    skipped_reason: str | None = None
    pg_skipped_reason: str | None = None
    changed_rows: list[ChangedLearnRowPayload] = field(default_factory=list)


@dataclass
class SyncSummary:
    dbname: str
    db_type: str
    apply: bool
    allow_multiple_bots: bool = False
    sync_pg_existing_post: bool = False
    chunk_size: int = DEFAULT_CHUNK_SIZE
    chat_bot_id: str | None = None
    apply_blocked_reason: str | None = None
    skipped_reason: str | None = None
    bots_found: int = 0
    bots_processed: int = 0
    updates_planned: int = 0
    updates_applied_estimate: int = 0
    pg_updates_planned: int = 0
    pg_updates_applied_estimate: int = 0
    missing_learn_tables: list[str] = field(default_factory=list)
    missing_pg_training_tables: list[str] = field(default_factory=list)
    results: list[BotSyncResult] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    changed_rows_by_bot: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(identifier or ""):
        raise ValueError(f"unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def quote_pg_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(identifier or ""):
        raise ValueError(f"unsafe PostgreSQL identifier: {identifier!r}")
    return f'"{identifier}"'


def build_pg_training_table_name(chat_id: str) -> str:
    if not str(chat_id or "").strip():
        raise ValueError("chat_id가 필요합니다.")
    return f"td_{str(chat_id).strip().lower()}_training_data"


def _optional_config_value(value: Any) -> Any | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "all":
        return None
    return value


def _count_from_row(row: Any, *keys: str) -> int:
    if row is None:
        return 0
    if isinstance(row, Mapping):
        for key in keys or ("count",):
            if key in row:
                return int(row[key] or 0)
        return 0
    if isinstance(row, (tuple, list)) and row:
        return int(row[0] or 0)
    for key in keys or ("count",):
        try:
            return int(row[key] or 0)
        except Exception:
            continue
    try:
        return int(row[0] or 0)
    except Exception:
        pass
    return int(row or 0)


def _first_value_from_row(row: Any, *keys: str) -> Any:
    if row is None:
        return None
    if isinstance(row, Mapping):
        for key in keys:
            if key in row:
                return row[key]
        return next(iter(row.values()), None)
    if isinstance(row, (tuple, list)) and row:
        return row[0]
    try:
        if keys:
            return row[keys[0]]
        return row[0]
    except Exception:
        return row


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, Mapping):
        if key in row:
            return row[key]
        lower_key = key.lower()
        for row_key, value in row.items():
            if str(row_key).lower() == lower_key:
                return value
        return default
    try:
        return row[key]
    except Exception:
        return default



def _mysql_error_code(exc: Exception) -> int | None:
    args = getattr(exc, "args", None) or ()
    if args:
        try:
            return int(args[0])
        except Exception:
            pass
    return None


def is_retryable_rdbms_lock_error(exc: Exception) -> bool:
    """MariaDB/MySQL deadlock/lock wait timeout이면 True."""
    error_code = _mysql_error_code(exc)
    if error_code in {1205, 1213}:
        return True
    text = str(exc).lower()
    return "deadlock found" in text or "lock wait timeout" in text


def _candidate_rows_from_rows(rows: Any) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows or []:
        if isinstance(row, Mapping):
            row_id = row.get("id")
            content = row.get("content")
            url = row.get("url") or content
            cate1 = row.get("original_cate1", row.get("cate1", ""))
            cate2 = row.get("original_cate2", row.get("cate2", ""))
        elif isinstance(row, (tuple, list)):
            row_id = row[0] if len(row) > 0 else None
            content = row[1] if len(row) > 1 else None
            url = row[2] if len(row) > 2 else content
            cate1 = row[3] if len(row) > 3 else ""
            cate2 = row[4] if len(row) > 4 else ""
        else:
            try:
                row_id = row["id"]
                content = row["content"]
                url = row["url"] if "url" in row else content
                cate1 = row["original_cate1"] if "original_cate1" in row else row.get("cate1", "")
                cate2 = row["original_cate2"] if "original_cate2" in row else row.get("cate2", "")
            except Exception:
                continue
        if row_id is not None and content:
            candidates.append({
                "id": row_id,
                "content": str(content),
                "url": str(url or content),
                "original_cate1": str(cate1 or ""),
                "original_cate2": str(cate2 or ""),
            })
    return candidates


def _payloads_from_candidate_rows(rows: list[dict[str, Any]]) -> list[ChangedLearnRowPayload]:
    return [
        ChangedLearnRowPayload(
            id=row.get("id"),
            content=str(row.get("content") or ""),
            url=str(row.get("url") or row.get("content") or ""),
            original_cate1=str(row.get("original_cate1") or ""),
            original_cate2=str(row.get("original_cate2") or ""),
        )
        for row in rows
        if row.get("id") is not None
    ]


def configure_logging(log_level: str | None) -> None:
    level_name = str(log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    LOGGER.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(level)


def _strip_env_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_standalone_env() -> None:
    """프로젝트 DB 헬퍼를 import하지 않기 위한 최소 .env 로더."""
    global _STANDALONE_ENV_LOADED
    if _STANDALONE_ENV_LOADED:
        return

    for env_path in (PROJECT_ROOT / ".env", Path.cwd() / ".env"):
        if not env_path.exists():
            continue
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                os.environ[key] = _strip_env_value(value)
        except Exception as exc:
            LOGGER.warning("[standalone env 로드 실패] path=%s error=%s", env_path, exc)
    _STANDALONE_ENV_LOADED = True


def _env_value(name: str, *, default: Any = None, required: bool = False) -> Any:
    _load_standalone_env()
    value = os.getenv(name)
    if value is None or value == "":
        if required and default is None:
            raise RuntimeError(f"{name} 환경변수 또는 .env 값이 필요합니다.")
        return default
    return value


def _env_int(name: str, *, default: int, required: bool = False) -> int:
    value = _env_value(name, default=None if required else default, required=required)
    try:
        return int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} 값은 정수여야 합니다: {value!r}") from exc


def _env_first(names: tuple[str, ...], *, default: Any = None, required: bool = False) -> Any:
    _load_standalone_env()
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    if required and default is None:
        joined = " 또는 ".join(names)
        raise RuntimeError(f"{joined} 환경변수 또는 .env 값이 필요합니다.")
    return default


def _env_int_first(names: tuple[str, ...], *, default: int, required: bool = False) -> int:
    value = _env_first(names, default=None if required else default, required=required)
    try:
        return int(value)
    except Exception as exc:
        joined = " 또는 ".join(names)
        raise RuntimeError(f"{joined} 값은 정수여야 합니다: {value!r}") from exc


def _direct_setting_value(value: Any) -> Any | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "all":
            return None
        return text
    return value


def _setting_value(
    direct_value: Any,
    env_names: tuple[str, ...],
    *,
    default: Any = None,
    required: bool = False,
) -> Any:
    direct = _direct_setting_value(direct_value)
    if direct is not None:
        return direct
    return _env_first(env_names, default=default, required=required)


def _setting_int(
    direct_value: Any,
    env_names: tuple[str, ...],
    *,
    default: int,
    required: bool = False,
) -> int:
    value = _setting_value(direct_value, env_names, default=None if required else default, required=required)
    try:
        return int(value)
    except Exception as exc:
        joined = " 또는 ".join(env_names)
        raise RuntimeError(f"{joined} 값은 정수여야 합니다: {value!r}") from exc


def _standalone_resolve_maria_db_type(db_name: str, db_type: str | None = None) -> str:
    if db_type is not None:
        return db_type
    return _MYSQL if str(db_name or "").strip().lower() in _MYSQL_DB_NAMES else _MARIA


def _standalone_build_url_learn_list_table_name(chat_bot_id: str, content_type: str = "url") -> str:
    del content_type
    if not chat_bot_id:
        raise ValueError("chat_bot_id가 필요합니다.")
    tail = str(chat_bot_id).replace("-", "")[-12:]
    if not tail:
        raise ValueError("유효한 chat_bot_id가 아닙니다.")
    return f"ASADAL_{tail}_LEARN_LIST"


async def _get_standalone_maria_pool(dbname: str):
    try:
        import aiomysql
    except ImportError as exc:
        raise RuntimeError("standalone MariaDB 실행에는 aiomysql 패키지가 필요합니다.") from exc

    key = (str(dbname), _MARIA)
    pool = _STANDALONE_MARIA_POOLS.get(key)
    if pool is not None:
        return pool

    minsize = max(1, _setting_int(MARIADB_POOL_MIN, ("MARIA_DB_POOL_MIN", "DB_POOL_MIN"), default=1))
    maxsize = max(minsize, _setting_int(MARIADB_POOL_MAX, ("MARIA_DB_POOL_MAX", "DB_POOL_MAX"), default=5))
    pool = await aiomysql.create_pool(
        db=dbname,
        user=_setting_value(MARIADB_USER, ("MARIA_DB_USER",), required=True),
        password=_setting_value(MARIADB_PASSWORD, ("MARIA_DB_PASSWORD",), required=True),
        host=_setting_value(MARIADB_HOST, ("MARIA_DB_HOST",), required=True),
        port=_setting_int(MARIADB_PORT, ("MARIA_DB_PORT",), default=3306),
        charset="utf8mb4",
        autocommit=True,
        minsize=minsize,
        maxsize=maxsize,
    )
    _STANDALONE_MARIA_POOLS[key] = pool
    return pool


async def _standalone_execute_mysql_query(
    sql: str,
    params: tuple[Any, ...] | list[Any] | None = None,
    *,
    fetch: str | bool | None = None,
    as_dict: bool = False,
    dbname: str | None = None,
) -> Any:
    """utils.get_mysql 없이 MYSQL_* 환경변수만으로 실행하는 MySQL 경로."""
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("standalone MySQL 실행에는 pymysql 패키지가 필요합니다.") from exc

    def _run_sync() -> Any:
        conn = pymysql.connect(
            host=_setting_value(MYSQL_HOST, ("MYSQL_HOST",), required=True),
            user=_setting_value(MYSQL_USER, ("MYSQL_USER",), required=True),
            password=_setting_value(MYSQL_PASSWORD, ("MYSQL_PASS", "MYSQL_PASSWORD"), required=True),
            database=dbname or _setting_value(MYSQL_DATABASE, ("MYSQL_DB",), default=None),
            port=_setting_int(MYSQL_PORT, ("MYSQL_PORT",), default=3306),
            charset="utf8mb4",
            autocommit=True,
            ssl=None,
            use_unicode=True,
            read_timeout=30,
            write_timeout=30,
        )
        cursor = None
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor) if as_dict else conn.cursor()
            cursor.execute(sql, tuple(params or ()))
            if fetch == "one":
                return cursor.fetchone()
            if fetch == "all" or fetch is True:
                return cursor.fetchall()
            return None
        finally:
            if cursor is not None:
                cursor.close()
            conn.close()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _run_sync)


async def _standalone_execute_rdbms_query(
    sql: str,
    params: tuple[Any, ...] | list[Any] | None = None,
    *,
    fetch: str | bool | None = None,
    as_dict: bool = False,
    dbname: str | None = None,
    db_type: str | None = None,
) -> Any:
    resolved_dbname = str(dbname or "").strip()
    if not resolved_dbname:
        raise ValueError("dbname이 필요합니다.")

    resolved_db_type = _standalone_resolve_maria_db_type(resolved_dbname, db_type)
    if resolved_db_type == _MYSQL:
        return await _standalone_execute_mysql_query(
            sql,
            params,
            fetch=fetch,
            as_dict=as_dict,
            dbname=resolved_dbname,
        )

    try:
        import aiomysql
    except ImportError as exc:
        raise RuntimeError("standalone MariaDB 실행에는 aiomysql 패키지가 필요합니다.") from exc

    pool = await _get_standalone_maria_pool(resolved_dbname)
    async with pool.acquire() as conn:
        cursor_context = conn.cursor(aiomysql.DictCursor) if as_dict else conn.cursor()
        async with cursor_context as cur:
            await cur.execute(sql, tuple(params or ()))
            if fetch == "one":
                return await cur.fetchone()
            if fetch == "all" or fetch is True:
                return await cur.fetchall()
    return None


async def _get_standalone_pg_pool(dbname: str):
    try:
        import asyncpg
    except ImportError as exc:
        raise RuntimeError("standalone PostgreSQL 실행에는 asyncpg 패키지가 필요합니다.") from exc

    key = str(dbname)
    pool = _STANDALONE_PG_POOLS.get(key)
    if pool is not None:
        return pool

    host = _setting_value(POSTGRES_HOST, ("DB_HOST",), required=True)
    if str(dbname or "").strip().lower() == "chatty":
        host = _setting_value(POSTGRES_CHATTY_HOST, ("CHATTY_PG_DB_HOST",), default=host)

    minsize = max(1, _setting_int(POSTGRES_POOL_MIN, ("PG_DB_POOL_MIN", "DB_POOL_MIN"), default=1))
    maxsize = max(minsize, _setting_int(POSTGRES_POOL_MAX, ("PG_DB_POOL_MAX", "DB_POOL_MAX"), default=5))
    pool = await asyncpg.create_pool(
        database=dbname,
        user=_setting_value(POSTGRES_USER, ("DB_USER",), required=True),
        password=_setting_value(POSTGRES_PASSWORD, ("DB_PASSWORD",), required=True),
        host=host,
        port=_setting_int(POSTGRES_PORT, ("DB_PORT",), default=5432),
        min_size=minsize,
        max_size=maxsize,
    )
    _STANDALONE_PG_POOLS[key] = pool
    return pool


async def _standalone_execute_pg_query(
    query: str,
    params: tuple[Any, ...] | list[Any] | None = None,
    *,
    fetch: bool = False,
    dbname: str | None = None,
) -> Any:
    resolved_dbname = str(dbname or "").strip()
    if not resolved_dbname:
        raise ValueError("dbname이 필요합니다.")
    pool = await _get_standalone_pg_pool(resolved_dbname)
    async with pool.acquire() as conn:
        if fetch:
            return await conn.fetch(query, *(params or ()))
        await conn.execute(query, *(params or ()))
    return None


async def _standalone_get_chat_id_from_db(db_name: str, chat_bot_id: str) -> str | None:
    try:
        rows = await _standalone_execute_pg_query(
            """
            SELECT chat_id
            FROM chatbot_setup
            WHERE chat_bot_id = $1
            """,
            params=(chat_bot_id,),
            fetch=True,
            dbname=db_name,
        )
        row = rows[0] if rows else None
        value = _first_value_from_row(row, "chat_id")
        return str(value) if value else None
    except Exception as exc:
        LOGGER.error("[chat_id 조회 실패] [DB:%s] chat_bot_id=%s error=%s", db_name, chat_bot_id, exc)
        return None


async def _close_standalone_db_pools() -> None:
    maria_pools = list(_STANDALONE_MARIA_POOLS.values())
    pg_pools = list(_STANDALONE_PG_POOLS.values())
    _STANDALONE_MARIA_POOLS.clear()
    _STANDALONE_PG_POOLS.clear()

    for pool in maria_pools:
        pool.close()
        await pool.wait_closed()
    for pool in pg_pools:
        await pool.close()


def _load_db_helpers() -> tuple[ExecuteQuery, PgExecuteQuery, TableNameBuilder, DbTypeResolver, ChatIdResolver]:
    return (
        _standalone_execute_rdbms_query,
        _standalone_execute_pg_query,
        _standalone_build_url_learn_list_table_name,
        _standalone_resolve_maria_db_type,
        _standalone_get_chat_id_from_db,
    )


def _resolve_helpers(
    execute_query: ExecuteQuery | None,
    pg_execute_query: PgExecuteQuery | None,
    table_name_builder: TableNameBuilder | None,
    db_type_resolver: DbTypeResolver | None,
    chat_id_resolver: ChatIdResolver | None,
) -> tuple[ExecuteQuery, PgExecuteQuery, TableNameBuilder, DbTypeResolver, ChatIdResolver]:
    if (
        execute_query is not None
        and pg_execute_query is not None
        and table_name_builder is not None
        and db_type_resolver is not None
        and chat_id_resolver is not None
    ):
        return execute_query, pg_execute_query, table_name_builder, db_type_resolver, chat_id_resolver

    loaded_query, loaded_pg_query, loaded_builder, loaded_resolver, loaded_chat_id_resolver = _load_db_helpers()
    return (
        execute_query or loaded_query,
        pg_execute_query or loaded_pg_query,
        table_name_builder or loaded_builder,
        db_type_resolver or loaded_resolver,
        chat_id_resolver or loaded_chat_id_resolver,
    )


async def learn_table_exists(
    *,
    dbname: str,
    table_name: str,
    execute_query: ExecuteQuery,
    db_type: str,
) -> bool:
    row = await execute_query(
        """
        SELECT COUNT(*) AS count
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (dbname, table_name),
        dbname=dbname,
        db_type=db_type,
        fetch="one",
        as_dict=True,
    )
    return _count_from_row(row, "count") > 0


async def pg_training_table_exists(
    *,
    dbname: str,
    table_name: str,
    pg_execute_query: PgExecuteQuery,
) -> bool:
    rows = await pg_execute_query(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = $1
        ) AS exists
        """,
        params=(table_name,),
        fetch=True,
        dbname=dbname,
    )
    row = rows[0] if rows else None
    return bool(_first_value_from_row(row, "exists"))



async def fetch_post_chatbots(
    *,
    dbname: str,
    execute_query: ExecuteQuery,
    db_type: str,
    chat_bot_id: str | None = None,
) -> list[dict[str, Any]]:
    where = [
        "LOWER(type) = 'post'",
        "chat_bot_id IS NOT NULL",
        "chat_bot_id <> ''",
        "url IS NOT NULL",
        "url <> ''",
    ]
    params: list[Any] = []
    if chat_bot_id:
        where.append("chat_bot_id = %s")
        params.append(chat_bot_id)

    rows = await execute_query(
        f"""
        SELECT chat_bot_id, COUNT(DISTINCT url) AS post_url_count
        FROM {_EXPLORATION_TABLE}
        WHERE {' AND '.join(where)}
        GROUP BY chat_bot_id
        ORDER BY chat_bot_id
        """,
        tuple(params),
        dbname=dbname,
        db_type=db_type,
        fetch="all",
        as_dict=True,
    )
    return list(rows or [])


def _is_missing_exploration_table_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "ASADAL_CRAWLING_EXPLORATION" in message
        and (
            "doesn't exist" in message
            or "does not exist" in message
            or "1146" in message
        )
    )


async def fetch_post_urls_chunk(
    *,
    dbname: str,
    chat_bot_id: str,
    execute_query: ExecuteQuery,
    db_type: str,
    chunk_size: int,
    after_url: str | None = None,
) -> list[str]:
    where = [
        "chat_bot_id = %s",
        "LOWER(type) = 'post'",
        "url IS NOT NULL",
        "url <> ''",
    ]
    params: list[Any] = [chat_bot_id]
    if after_url is not None:
        where.append("url > %s")
        params.append(after_url)
    params.append(chunk_size)
    rows = await execute_query(
        f"""
        SELECT DISTINCT url
        FROM {_EXPLORATION_TABLE}
        WHERE {' AND '.join(where)}
        ORDER BY url
        LIMIT %s
        """,
        tuple(params),
        dbname=dbname,
        db_type=db_type,
        fetch="all",
        as_dict=True,
    )
    urls: list[str] = []
    for row in rows or []:
        value = _row_get(row, "url")
        if value:
            urls.append(str(value))
    return urls


async def fetch_update_candidate_rows_for_urls(
    *,
    dbname: str,
    learn_table: str,
    urls: list[str],
    execute_query: ExecuteQuery,
    db_type: str,
) -> list[dict[str, Any]]:
    if not urls:
        return []
    quoted_table = quote_identifier(learn_table)
    placeholders = ", ".join(["%s"] * len(urls))
    rows = await execute_query(
        f"""
        SELECT ll.id AS id, ll.content AS content,
            ll.content AS url,
            ll.cate1 AS original_cate1,
            ll.cate2 AS original_cate2
        FROM {quoted_table} ll
        WHERE ll.content_type = 'url'
          AND ll.content IN ({placeholders})
        ORDER BY ll.id
        """,
        tuple(urls),
        dbname=dbname,
        db_type=db_type,
        fetch="all",
        as_dict=True,
    )
    return _candidate_rows_from_rows(rows)



async def fetch_pg_mirror_source_rows(
    *,
    dbname: str,
    learn_table: str,
    execute_query: ExecuteQuery,
    db_type: str,
    chunk_size: int | None = None,
    after_id: Any | None = None,
) -> list[dict[str, Any]]:
    quoted_table = quote_identifier(learn_table)
    cursor_clause = "AND ll.id > %s" if after_id is not None else ""
    limit_clause = "LIMIT %s" if chunk_size is not None else ""
    params: list[Any] = []
    if after_id is not None:
        params.append(after_id)
    if chunk_size is not None:
        params.append(chunk_size)
    rows = await execute_query(
        f"""
        SELECT ll.id AS id, ll.content AS content
        FROM {quoted_table} ll
        WHERE ll.content_type = 'post'
          AND ll.content IS NOT NULL
          AND ll.content <> ''
          {cursor_clause}
        ORDER BY ll.id
        {limit_clause}
        """,
        tuple(params),
        dbname=dbname,
        db_type=db_type,
        fetch="all",
        as_dict=True,
    )
    return _candidate_rows_from_rows(rows)


async def fetch_update_candidate_rows_in_chunks(
    *,
    dbname: str,
    learn_table: str,
    chat_bot_id: str,
    execute_query: ExecuteQuery,
    db_type: str,
    chunk_size: int,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    after_url: str | None = None
    chunk_no = 1
    while True:
        url_started_at = time.perf_counter()
        LOGGER.info(
            "[점검 chunk 시작] [DB:%s] chat_bot_id=%s 학습테이블=%s chunk=%s size=%s",
            dbname,
            chat_bot_id,
            learn_table,
            chunk_no,
            chunk_size,
        )
        urls = await fetch_post_urls_chunk(
            dbname=dbname,
            chat_bot_id=chat_bot_id,
            execute_query=execute_query,
            db_type=db_type,
            chunk_size=chunk_size,
            after_url=after_url,
        )
        url_elapsed_seconds = time.perf_counter() - url_started_at
        if not urls:
            LOGGER.info(
                "[점검 탐색 URL chunk 종료] [DB:%s] chat_bot_id=%s 학습테이블=%s chunk=%s 추가 URL 없음 소요=%.2f초",
                dbname,
                chat_bot_id,
                learn_table,
                chunk_no,
                url_elapsed_seconds,
            )
            break
        match_started_at = time.perf_counter()
        rows = await fetch_update_candidate_rows_for_urls(
            dbname=dbname,
            learn_table=learn_table,
            urls=urls,
            execute_query=execute_query,
            db_type=db_type,
        )
        match_elapsed_seconds = time.perf_counter() - match_started_at
        all_rows.extend(rows)
        after_url = urls[-1]
        LOGGER.info(
            "[점검 chunk 결과] [DB:%s] chat_bot_id=%s 학습테이블=%s chunk=%s 탐색URL=%s LEARN_LIST매칭=%s 누적매칭=%s URL조회=%.2f초 매칭조회=%.2f초",
            dbname,
            chat_bot_id,
            learn_table,
            chunk_no,
            len(urls),
            len(rows),
            len(all_rows),
            url_elapsed_seconds,
            match_elapsed_seconds,
        )
        if len(urls) < chunk_size:
            break
        chunk_no += 1
    return all_rows


async def apply_update_chunk(
    *,
    dbname: str,
    learn_table: str,
    ids: list[Any],
    execute_query: ExecuteQuery,
    db_type: str,
    retry_count: int = DEFAULT_RDBMS_RETRY_COUNT,
    retry_delay_seconds: float = DEFAULT_RDBMS_RETRY_DELAY_SECONDS,
    chat_bot_id: str = "",
    chunk_no: int = 0,
) -> None:
    if not ids:
        return

    quoted_table = quote_identifier(learn_table)
    placeholders = ", ".join(["%s"] * len(ids))
    sql = f"""
    UPDATE {quoted_table}
    SET content_type = 'post'
    WHERE id IN ({placeholders})
      AND content_type = 'url'
    """
    params = tuple(ids)
    max_attempts = max(1, int(retry_count or 0) + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            await execute_query(
                sql,
                params,
                dbname=dbname,
                db_type=db_type,
            )
            return
        except Exception as exc:
            if not is_retryable_rdbms_lock_error(exc) or attempt >= max_attempts:
                raise
            wait_seconds = max(0.0, float(retry_delay_seconds or 0.0) * attempt)
            LOGGER.warning(
                "[chunk RDBMS UPDATE 재시도] [DB:%s] chat_bot_id=%s learn_table=%s chunk=%s rows=%s "
                "attempt=%s/%s error_code=%s wait=%.2fs error=%s",
                dbname,
                chat_bot_id or "-",
                learn_table,
                chunk_no or "-",
                len(ids),
                attempt,
                max_attempts,
                _mysql_error_code(exc) or "-",
                wait_seconds,
                exc,
            )
            if wait_seconds:
                await asyncio.sleep(wait_seconds)


async def count_pg_update_candidates(
    *,
    dbname: str,
    pg_training_table: str,
    contents: list[str],
    pg_execute_query: PgExecuteQuery,
) -> int:
    if not contents:
        return 0
    quoted_table = quote_pg_identifier(pg_training_table)
    rows = await pg_execute_query(
        f"""
        SELECT COUNT(*) AS target_count
        FROM {quoted_table}
        WHERE content_type = $1
          AND content = ANY($2::text[])
        """,
        params=("url", list(dict.fromkeys(contents))),
        fetch=True,
        dbname=dbname,
    )
    row = rows[0] if rows else None
    return _count_from_row(row, "target_count", "count")


async def apply_pg_update_chunk(
    *,
    dbname: str,
    pg_training_table: str,
    contents: list[str],
    pg_execute_query: PgExecuteQuery,
) -> int:
    if not contents:
        return 0
    quoted_table = quote_pg_identifier(pg_training_table)
    rows = await pg_execute_query(
        f"""
        UPDATE {quoted_table}
        SET content_type = $1
        WHERE content_type = $2
          AND content = ANY($3::text[])
        RETURNING id
        """,
        params=("post", "url", list(dict.fromkeys(contents))),
        fetch=True,
        dbname=dbname,
    )
    return len(rows or [])


async def apply_pg_mirror_in_chunks(
    *,
    dbname: str,
    learn_table: str,
    chat_bot_id: str,
    execute_query: ExecuteQuery,
    pg_execute_query: PgExecuteQuery,
    db_type: str,
    pg_training_table: str,
    chunk_size: int,
) -> tuple[int, int]:
    pg_applied_estimate = 0
    pg_chunks_applied = 0
    after_id: Any | None = None
    mirror_chunk_no = 0

    while True:
        mirror_chunk_no += 1
        LOGGER.info(
            "[PG mirror 대상 조회] [DB:%s] chat_bot_id=%s learn_table=%s pg_table=%s chunk=%s size=%s after_id=%s",
            dbname,
            chat_bot_id,
            learn_table,
            pg_training_table,
            mirror_chunk_no,
            chunk_size,
            after_id or "-",
        )
        candidate_rows = await fetch_pg_mirror_source_rows(
            dbname=dbname,
            learn_table=learn_table,
            execute_query=execute_query,
            db_type=db_type,
            chunk_size=chunk_size,
            after_id=after_id,
        )
        if not candidate_rows:
            LOGGER.info(
                "[PG mirror 종료] [DB:%s] chat_bot_id=%s learn_table=%s pg_table=%s chunk=%s 대상 없음",
                dbname,
                chat_bot_id,
                learn_table,
                pg_training_table,
                mirror_chunk_no,
            )
            break

        after_id = candidate_rows[-1]["id"]
        contents = [row["content"] for row in candidate_rows]
        LOGGER.info(
            "[PG mirror UPDATE 시작] [DB:%s] chat_bot_id=%s pg_table=%s chunk=%s rows=%s first_id=%s last_id=%s",
            dbname,
            chat_bot_id,
            pg_training_table,
            mirror_chunk_no,
            len(contents),
            candidate_rows[0]["id"],
            after_id,
        )
        pg_updated = await apply_pg_update_chunk(
            dbname=dbname,
            pg_training_table=pg_training_table,
            contents=contents,
            pg_execute_query=pg_execute_query,
        )
        pg_applied_estimate += pg_updated
        if pg_updated > 0:
            pg_chunks_applied += 1
        LOGGER.info(
            "[PG mirror UPDATE 완료] [DB:%s] chat_bot_id=%s pg_table=%s chunk=%s rows=%s pg_applied_estimate=%s",
            dbname,
            chat_bot_id,
            pg_training_table,
            mirror_chunk_no,
            pg_updated,
            pg_applied_estimate,
        )

        if len(candidate_rows) < chunk_size:
            break

    return pg_applied_estimate, pg_chunks_applied


async def apply_update_in_chunks(
    *,
    dbname: str,
    learn_table: str,
    chat_bot_id: str,
    execute_query: ExecuteQuery,
    pg_execute_query: PgExecuteQuery,
    db_type: str,
    pg_training_table: str,
    chunk_size: int,
    rdbms_retry_count: int = DEFAULT_RDBMS_RETRY_COUNT,
    rdbms_retry_delay_seconds: float = DEFAULT_RDBMS_RETRY_DELAY_SECONDS,
    sync_pg_existing_post: bool = False,
) -> tuple[int, int, int, int, list[ChangedLearnRowPayload]]:
    applied_estimate = 0
    chunks_applied = 0
    pg_applied_estimate = 0
    pg_chunks_applied = 0
    changed_rows: list[ChangedLearnRowPayload] = []
    after_url: str | None = None
    url_chunk_no = 1

    while True:
        url_select_started_at = time.perf_counter()
        LOGGER.info(
            "[chunk 시작] [DB:%s] chat_bot_id=%s learn_table=%s chunk=%s size=%s",
            dbname,
            chat_bot_id,
            learn_table,
            url_chunk_no,
            chunk_size,
        )
        urls = await fetch_post_urls_chunk(
            dbname=dbname,
            chat_bot_id=chat_bot_id,
            execute_query=execute_query,
            db_type=db_type,
            chunk_size=chunk_size,
            after_url=after_url,
        )
        url_select_elapsed = time.perf_counter() - url_select_started_at
        if not urls:
            LOGGER.info(
                "[탐색 URL chunk 종료] [DB:%s] chat_bot_id=%s learn_table=%s chunk=%s 추가 URL 없음 소요=%.2f초",
                dbname,
                chat_bot_id,
                learn_table,
                url_chunk_no,
                url_select_elapsed,
            )
            break
        match_started_at = time.perf_counter()
        candidate_rows = await fetch_update_candidate_rows_for_urls(
            dbname=dbname,
            learn_table=learn_table,
            urls=urls,
            execute_query=execute_query,
            db_type=db_type,
        )
        match_elapsed = time.perf_counter() - match_started_at
        after_url = urls[-1]
        if not candidate_rows:
            LOGGER.info(
                "[chunk 결과] [DB:%s] chat_bot_id=%s learn_table=%s chunk=%s 탐색URL=%s LEARN_LIST매칭=0 누적반영=%s URL조회=%.2f초 매칭조회=%.2f초",
                dbname,
                chat_bot_id,
                learn_table,
                url_chunk_no,
                len(urls),
                applied_estimate,
                url_select_elapsed,
                match_elapsed,
            )
            if len(urls) < chunk_size:
                break
            url_chunk_no += 1
            continue

        ids = [row["id"] for row in candidate_rows]
        contents = [row["content"] for row in candidate_rows]
        changed_rows.extend(_payloads_from_candidate_rows(candidate_rows))
        LOGGER.info(
            "[chunk 결과/RDBMS UPDATE 시작] [DB:%s] chat_bot_id=%s learn_table=%s chunk=%s 탐색URL=%s LEARN_LIST매칭=%s URL조회=%.2f초 매칭조회=%.2f초",
            dbname,
            chat_bot_id,
            learn_table,
            url_chunk_no,
            len(urls),
            len(ids),
            url_select_elapsed,
            match_elapsed,
        )
        await apply_update_chunk(
            dbname=dbname,
            learn_table=learn_table,
            ids=ids,
            execute_query=execute_query,
            db_type=db_type,
            retry_count=rdbms_retry_count,
            retry_delay_seconds=rdbms_retry_delay_seconds,
            chat_bot_id=chat_bot_id,
            chunk_no=url_chunk_no,
        )
        applied_estimate += len(ids)
        chunks_applied += 1
        LOGGER.info(
            "[chunk RDBMS UPDATE 완료] [DB:%s] chat_bot_id=%s learn_table=%s chunk=%s rows=%s applied_estimate=%s",
            dbname,
            chat_bot_id,
            learn_table,
            url_chunk_no,
            len(ids),
            applied_estimate,
        )
        LOGGER.info(
            "[chunk PG UPDATE 시작] [DB:%s] chat_bot_id=%s pg_table=%s chunk=%s rows=%s",
            dbname,
            chat_bot_id,
            pg_training_table,
            url_chunk_no,
            len(contents),
        )
        pg_updated = await apply_pg_update_chunk(
            dbname=dbname,
            pg_training_table=pg_training_table,
            contents=contents,
            pg_execute_query=pg_execute_query,
        )
        pg_applied_estimate += pg_updated
        if pg_updated > 0:
            pg_chunks_applied += 1
        LOGGER.info(
            "[chunk PG UPDATE 완료] [DB:%s] chat_bot_id=%s pg_table=%s chunk=%s rows=%s pg_applied_estimate=%s",
            dbname,
            chat_bot_id,
            pg_training_table,
            url_chunk_no,
            pg_updated,
            pg_applied_estimate,
        )

        if len(urls) < chunk_size:
            break
        url_chunk_no += 1

    if sync_pg_existing_post:
        pg_mirror_updated, pg_mirror_chunks = await apply_pg_mirror_in_chunks(
            dbname=dbname,
            learn_table=learn_table,
            chat_bot_id=chat_bot_id,
            execute_query=execute_query,
            pg_execute_query=pg_execute_query,
            db_type=db_type,
            pg_training_table=pg_training_table,
            chunk_size=chunk_size,
        )
        pg_applied_estimate += pg_mirror_updated
        pg_chunks_applied += pg_mirror_chunks

    return applied_estimate, chunks_applied, pg_applied_estimate, pg_chunks_applied, changed_rows


async def sync_post_content_type(
    *,
    dbname: str,
    apply: bool = False,
    allow_multiple_bots: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chat_bot_id: str | None = None,
    db_type: str | None = None,
    rdbms_retry_count: int = DEFAULT_RDBMS_RETRY_COUNT,
    rdbms_retry_delay_seconds: float = DEFAULT_RDBMS_RETRY_DELAY_SECONDS,
    sync_pg_existing_post: bool = False,
    execute_query: ExecuteQuery | None = None,
    pg_execute_query: PgExecuteQuery | None = None,
    table_name_builder: TableNameBuilder | None = None,
    db_type_resolver: DbTypeResolver | None = None,
    chat_id_resolver: ChatIdResolver | None = None,
) -> SyncSummary:
    if not str(dbname or "").strip():
        raise ValueError("--dbname is required")
    if chunk_size <= 0:
        raise ValueError("--chunk-size must be greater than 0")
    if rdbms_retry_count < 0:
        raise ValueError("--rdbms-retry-count must be greater than or equal to 0")
    if rdbms_retry_delay_seconds < 0:
        raise ValueError("--rdbms-retry-delay-seconds must be greater than or equal to 0")

    LOGGER.info(
        "[content_type=post 점검 시작] [DB:%s] 실행모드=%s 대상 bot=%s 배치크기=%s 여러 bot 허용=%s "
        "기존 post PG보정=%s 재시도=%s 대기=%.2f초",
        dbname,
        "실제반영" if apply else "점검",
        chat_bot_id or "전체",
        chunk_size,
        allow_multiple_bots,
        sync_pg_existing_post,
        rdbms_retry_count,
        rdbms_retry_delay_seconds,
    )
    execute_query, pg_execute_query, table_name_builder, db_type_resolver, chat_id_resolver = _resolve_helpers(
        execute_query,
        pg_execute_query,
        table_name_builder,
        db_type_resolver,
        chat_id_resolver,
    )
    resolved_db_type = db_type_resolver(dbname, db_type)
    LOGGER.info("[DB 타입 확인] [DB:%s] 타입=%s", dbname, resolved_db_type)
    summary = SyncSummary(
        dbname=dbname,
        db_type=resolved_db_type,
        apply=apply,
        allow_multiple_bots=allow_multiple_bots,
        sync_pg_existing_post=sync_pg_existing_post,
        chunk_size=chunk_size,
        chat_bot_id=chat_bot_id,
    )
    LOGGER.info("[탐색목록 post URL 보유 bot 조회 시작] [DB:%s] 대상 bot=%s", dbname, chat_bot_id or "전체")
    try:
        post_chatbots = await fetch_post_chatbots(
            dbname=dbname,
            execute_query=execute_query,
            db_type=resolved_db_type,
            chat_bot_id=chat_bot_id,
        )
    except Exception as exc:
        if not _is_missing_exploration_table_error(exc):
            raise
        summary.skipped_reason = "exploration_table_missing"
        LOGGER.warning(
            "[탐색목록 테이블 없음] [DB:%s] table=%s | 이 DB는 건너뜁니다.",
            dbname,
            _EXPLORATION_TABLE,
        )
        return summary
    summary.bots_found = len(post_chatbots)
    LOGGER.info("[탐색목록 post URL 보유 bot 조회 완료] [DB:%s] 대상 bot 수=%s", dbname, summary.bots_found)
    if apply and len(post_chatbots) > 1 and not (chat_bot_id or allow_multiple_bots):
        summary.apply_blocked_reason = (
            "multiple chat_bot_id targets found; rerun with --chat-bot-id for a single-table apply "
            "or pass --allow-multiple-bots to explicitly allow multi-table partial-apply risk"
        )
        LOGGER.warning("[APPLY 차단] [DB:%s] %s", dbname, summary.apply_blocked_reason)

    for index, row in enumerate(post_chatbots, start=1):
        bot_id = str(row.get("chat_bot_id") or "").strip()
        post_url_count = int(row.get("post_url_count") or 0)
        if not bot_id:
            continue

        try:
            learn_table = table_name_builder(bot_id)
            quote_identifier(learn_table)
        except Exception as exc:
            summary.errors.append({"chat_bot_id": bot_id, "error": str(exc)})
            LOGGER.exception("[bot 처리 실패] [DB:%s] chat_bot_id=%s error=%s", dbname, bot_id, exc)
            continue

        LOGGER.info(
            "[bot 처리 시작] [DB:%s] %s/%s chat_bot_id=%s 학습테이블=%s 탐색 post URL=%s",
            dbname,
            index,
            summary.bots_found,
            bot_id,
            learn_table,
            post_url_count,
        )
        result = BotSyncResult(
            chat_bot_id=bot_id,
            learn_table=learn_table,
            post_url_count=post_url_count,
            match_mode="url_content",
        )

        if not await learn_table_exists(
            dbname=dbname,
            table_name=learn_table,
            execute_query=execute_query,
            db_type=resolved_db_type,
        ):
            result.skipped_reason = "learn_table_missing"
            summary.missing_learn_tables.append(learn_table)
            summary.results.append(result)
            LOGGER.warning(
                "[bot 건너뜀] [DB:%s] chat_bot_id=%s 학습테이블=%s 이유=학습 테이블 없음(%s)",
                dbname,
                bot_id,
                learn_table,
                result.skipped_reason,
            )
            continue

        chat_id = await chat_id_resolver(dbname, bot_id)
        if not chat_id:
            result.pg_skipped_reason = "chat_id_missing"
            LOGGER.warning("[PG 변경 건너뜀] [DB:%s] chat_bot_id=%s 이유=chat_id 없음", dbname, bot_id)
        else:
            try:
                pg_training_table = build_pg_training_table_name(chat_id)
                quote_pg_identifier(pg_training_table)
                result.pg_training_table = pg_training_table
            except Exception as exc:
                result.pg_skipped_reason = str(exc)
                LOGGER.exception("[PG 테이블명 생성 실패] [DB:%s] chat_bot_id=%s chat_id=%s error=%s", dbname, bot_id, chat_id, exc)

        if result.pg_training_table and not await pg_training_table_exists(
            dbname=dbname,
            table_name=result.pg_training_table,
            pg_execute_query=pg_execute_query,
        ):
            result.pg_skipped_reason = "pg_training_table_missing"
            summary.missing_pg_training_tables.append(result.pg_training_table)
            LOGGER.warning(
                "[PG 변경 건너뜀] [DB:%s] chat_bot_id=%s PG테이블=%s 이유=PG training 테이블 없음(%s)",
                dbname,
                bot_id,
                result.pg_training_table,
                result.pg_skipped_reason,
            )



        pg_status = (
            f"SKIP({result.pg_skipped_reason})"
            if result.pg_skipped_reason
            else ("READY" if result.pg_training_table else "N/A")
        )

        can_apply_stream = (
            apply
            and not summary.apply_blocked_reason
            and result.pg_training_table
            and not result.pg_skipped_reason
        )
        if can_apply_stream:
            LOGGER.info(
                "[content_type=post 실제 반영 시작] [DB:%s] chat_bot_id=%s 학습테이블=%s PG테이블=%s 배치크기=%s",
                dbname,
                bot_id,
                learn_table,
                result.pg_training_table,
                chunk_size,
            )
            applied_estimate, chunks_applied, pg_applied_estimate, pg_chunks_applied, changed_rows = await apply_update_in_chunks(
                dbname=dbname,
                learn_table=learn_table,
                chat_bot_id=bot_id,
                execute_query=execute_query,
                pg_execute_query=pg_execute_query,
                db_type=resolved_db_type,
                pg_training_table=result.pg_training_table,
                chunk_size=chunk_size,
                rdbms_retry_count=rdbms_retry_count,
                rdbms_retry_delay_seconds=rdbms_retry_delay_seconds,
                sync_pg_existing_post=sync_pg_existing_post,
            )
            result.update_candidates = applied_estimate
            result.updates_applied_estimate = applied_estimate
            result.chunks_applied = chunks_applied
            result.pg_update_candidates = pg_applied_estimate
            result.pg_updates_applied_estimate = pg_applied_estimate
            result.pg_chunks_applied = pg_chunks_applied
            result.changed_rows = changed_rows
            summary.changed_rows_by_bot[bot_id] = [asdict(row) for row in changed_rows]
            summary.updates_planned += result.update_candidates
            summary.updates_applied_estimate += result.updates_applied_estimate
            summary.pg_updates_planned += result.pg_update_candidates
            summary.pg_updates_applied_estimate += result.pg_updates_applied_estimate
            LOGGER.info(
                "[content_type=post 실제 반영 완료] [DB:%s] chat_bot_id=%s 학습테이블=%s PG테이블=%s | LEARN_LIST 반영=%s PG 반영=%s | 배치=%s PG배치=%s",
                dbname,
                bot_id,
                learn_table,
                result.pg_training_table,
                result.updates_applied_estimate,
                result.pg_updates_applied_estimate,
                result.chunks_applied,
                result.pg_chunks_applied,
            )
        else:
            LOGGER.info("[post 변경 대상 row 배치 조회 시작] [DB:%s] chat_bot_id=%s 학습테이블=%s 배치크기=%s", dbname, bot_id, learn_table, chunk_size)
            candidate_rows = await fetch_update_candidate_rows_in_chunks(
                dbname=dbname,
                learn_table=learn_table,
                chat_bot_id=bot_id,
                execute_query=execute_query,
                db_type=resolved_db_type,
                chunk_size=chunk_size,
            )
            result.update_candidates = len(candidate_rows)
            summary.updates_planned += result.update_candidates
            result.changed_rows = _payloads_from_candidate_rows(candidate_rows)
            summary.changed_rows_by_bot[bot_id] = [asdict(row) for row in result.changed_rows]
            if result.pg_training_table and not result.pg_skipped_reason:
                pg_candidate_contents: list[str] = []
                pg_candidate_contents.extend(row["content"] for row in candidate_rows)
                if sync_pg_existing_post:
                    mirror_rows = await fetch_pg_mirror_source_rows(
                        dbname=dbname,
                        learn_table=learn_table,
                        execute_query=execute_query,
                        db_type=resolved_db_type,
                    )
                    pg_candidate_contents.extend(row["content"] for row in mirror_rows)
                if pg_candidate_contents:
                    result.pg_update_candidates = await count_pg_update_candidates(
                        dbname=dbname,
                        pg_training_table=result.pg_training_table,
                        contents=pg_candidate_contents,
                        pg_execute_query=pg_execute_query,
                    )
                    summary.pg_updates_planned += result.pg_update_candidates
            LOGGER.info(
                "[post 변경 예정] [DB:%s] chat_bot_id=%s | LEARN_LIST=%s 대상=%s건 content_type url→post | "
                "PG=%s 대상=%s건 content_type url→post 상태=%s",
                dbname,
                bot_id,
                learn_table,
                result.update_candidates,
                result.pg_training_table or "-",
                result.pg_update_candidates,
                pg_status,
            )

        if not can_apply_stream and not apply:
            LOGGER.info(
                "[점검 모드] 실제 변경 안 함 [DB:%s] chat_bot_id=%s | LEARN_LIST=%s 변경예정=%s건 | "
                "PG=%s 변경예정=%s건 상태=%s",
                dbname,
                bot_id,
                learn_table,
                result.update_candidates,
                result.pg_training_table or "-",
                result.pg_update_candidates,
                pg_status,
            )
        elif not can_apply_stream and result.pg_skipped_reason:
            LOGGER.warning(
                "[변경 건너뜀] PG 동기화가 불가능해서 LEARN_LIST도 변경하지 않음 [DB:%s] chat_bot_id=%s | "
                "LEARN_LIST=%s 대상=%s건 | PG=%s 상태=%s",
                dbname,
                bot_id,
                learn_table,
                result.update_candidates,
                result.pg_training_table or "-",
                pg_status,
            )
        elif not can_apply_stream and summary.apply_blocked_reason:
            LOGGER.info(
                "[변경 건너뜀] [DB:%s] chat_bot_id=%s 학습테이블=%s 이유=안전 차단 대상=%s건",
                dbname,
                bot_id,
                learn_table,
                result.update_candidates,
            )
        elif (
            can_apply_stream
            and result.updates_applied_estimate == 0
            and result.pg_updates_applied_estimate == 0
        ):
            LOGGER.info(
                "[post 변경 대상 없음] [DB:%s] chat_bot_id=%s 학습테이블=%s",
                dbname,
                bot_id,
                learn_table,
            )

        summary.bots_processed += 1
        summary.results.append(result)

    LOGGER.info(
        "[content_type=post 처리 완료] [DB:%s] 처리 bot=%s | LEARN_LIST 대상=%s 반영=%s | "
        "PG 대상=%s 반영=%s | 누락 학습테이블=%s 누락 PG테이블=%s 오류=%s",
        dbname,
        summary.bots_processed,
        summary.updates_planned,
        summary.updates_applied_estimate,
        summary.pg_updates_planned,
        summary.pg_updates_applied_estimate,
        len(summary.missing_learn_tables),
        len(summary.missing_pg_training_tables),
        len(summary.errors),
    )
    return summary


def _format_pg_status(item: BotSyncResult) -> str:
    if item.pg_skipped_reason:
        return f"SKIP({item.pg_skipped_reason})"
    if item.pg_training_table:
        return "READY"
    return "N/A"


def render_text_summary(summary: SyncSummary) -> str:
    lines = [
        "[탐색목록 post URL → 학습/PG content_type 보정]",
        f"DB: {summary.dbname} ({summary.db_type})",
        f"실행모드: {'실제반영(APPLY)' if summary.apply else '점검(DRY-RUN)'}",
        f"여러 bot 동시 반영 허용: {summary.allow_multiple_bots}",
        f"이미 post인 LEARN_LIST의 PG 보정 포함: {summary.sync_pg_existing_post}",
        f"배치크기: {summary.chunk_size}",
        f"대상 bot: {summary.chat_bot_id or '전체'}",
        "",
        "수정 기준:",
        "  - 탐색 테이블: ASADAL_CRAWLING_EXPLORATION.type = 'post'",
        "  - 매칭: LEARN_LIST.content = EXPLORATION.url",
        "  - RDBMS 대상: 현재 LEARN_LIST.content_type = 'url' 인 row만 content_type = 'post' 로 변경",
        "  - PG 기존 post 보정: --sync-pg-existing-post 사용 시에만 실행",
        "",
        "전체 변경 예정/결과:",
        f"  - LEARN_LIST content_type 변경: 대상={summary.updates_planned} 반영={summary.updates_applied_estimate}",
        f"  - PG training_data content_type 변경: 대상={summary.pg_updates_planned} 반영={summary.pg_updates_applied_estimate}",
        "",
        f"탐색목록 post URL 보유 bot 수: {summary.bots_found}",
        f"처리한 bot 수: {summary.bots_processed}",
    ]
    if summary.missing_learn_tables:
        lines.append("누락된 학습 테이블:")
        lines.extend(f"  - {table}" for table in summary.missing_learn_tables)
    if summary.missing_pg_training_tables:
        lines.append("누락된 PG training 테이블:")
        lines.extend(f"  - {table}" for table in summary.missing_pg_training_tables)
    if summary.errors:
        lines.append("오류:")
        lines.extend(f"  - {item}" for item in summary.errors)
    if summary.apply_blocked_reason:
        lines.append(f"실제반영 차단: {summary.apply_blocked_reason}")
    if summary.results:
        lines.append("bot별 상세:")
        for item in summary.results:
            lines.append(f"  - chat_bot_id={item.chat_bot_id}")
            lines.append(f"    탐색목록 post URL 수: {item.post_url_count}")
            lines.append(
                f"    LEARN_LIST: table={item.learn_table} content_type url→post 대상={item.update_candidates} "
                f"반영={item.updates_applied_estimate} 배치={item.chunks_applied}"
            )
            lines.append(
                f"    PG: table={item.pg_training_table or '-'} content_type url→post 대상={item.pg_update_candidates} "
                f"반영={item.pg_updates_applied_estimate} 배치={item.pg_chunks_applied} "
                f"상태={_format_pg_status(item)}"
            )
            if item.skipped_reason:
                lines.append(f"    건너뜀: {item.skipped_reason}")
            lines.append(f"    매칭방식: {item.match_mode}")
    if not summary.apply:
        lines.append("")
        lines.append("점검 모드입니다: 실제 UPDATE는 하지 않았습니다. 적용하려면 --apply를 붙여 실행하세요.")
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    default_dbname = _optional_config_value(TARGET_DBNAME)
    default_chat_bot_id = _optional_config_value(TARGET_CHAT_BOT_ID)
    default_db_type = _optional_config_value(TARGET_DB_TYPE)
    parser = argparse.ArgumentParser(
        description=(
            "Sync ASADAL_CRAWLING_EXPLORATION type='post' URLs into matching "
            "chatbot LEARN_LIST.content_type='post' rows and mirror the same "
            "content_type into PostgreSQL td_{chat_id}_training_data rows."
        )
    )
    parser.add_argument(
        "--dbname",
        default=default_dbname,
        required=default_dbname is None,
        help="Target database/schema name. Can also be set with TARGET_DBNAME in this file.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=APPLY_CHANGES,
        help="Actually run UPDATE statements. Default is dry-run.",
    )
    parser.add_argument(
        "--allow-multiple-bots",
        action="store_true",
        default=ALLOW_MULTIPLE_BOTS,
        help=(
            "Allow --apply to update multiple chatbot LEARN_LIST tables in one run. "
            "Default blocks multi-bot apply to avoid partial cross-table updates."
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=TARGET_CHUNK_SIZE,
        help=(
            "Number of LEARN_LIST.id rows to update per chunk when --apply is used. "
            f"Default is {DEFAULT_CHUNK_SIZE}."
        ),
    )
    parser.add_argument(
        "--rdbms-retry-count",
        type=int,
        default=RDBMS_DEADLOCK_RETRY_COUNT,
        help=(
            "MariaDB/MySQL deadlock or lock-wait retry count for each LEARN_LIST UPDATE chunk. "
            f"Default is {DEFAULT_RDBMS_RETRY_COUNT}."
        ),
    )
    parser.add_argument(
        "--rdbms-retry-delay-seconds",
        type=float,
        default=RDBMS_DEADLOCK_RETRY_DELAY_SECONDS,
        help=(
            "Base delay seconds before retrying a MariaDB/MySQL deadlock or lock wait. "
            "Actual wait is base_delay * attempt."
        ),
    )
    parser.add_argument(
        "--sync-pg-existing-post",
        action="store_true",
        default=SYNC_PG_EXISTING_POST,
        help=(
            "Also scan LEARN_LIST.content_type='post' rows and update matching PG content_type='url' "
            "rows to 'post'. Off by default because it adds an extra MariaDB scan."
        ),
    )
    parser.add_argument(
        "--chat-bot-id",
        "--chat_bot_id",
        "--chat-bpt-id",
        "--chat_bpt_id",
        dest="chat_bot_id",
        default=default_chat_bot_id,
        help=(
            "Optional single chat_bot_id filter. Also accepts --chat_bpt_id/--chat-bpt-id "
            "as typo-compatible aliases. Can also be set with TARGET_CHAT_BOT_ID in this file."
        ),
    )
    parser.add_argument(
        "--db-type",
        choices=("maria", "mysql"),
        default=default_db_type,
        help="Override DB type; defaults to existing resolver.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=JSON_OUTPUT,
        help="Print machine-readable JSON summary.",
    )
    parser.add_argument(
        "--log-level",
        default=TARGET_LOG_LEVEL,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Progress log level. Logs are written to stderr, so --json stdout stays parseable.",
    )
    return parser


async def async_main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    try:
        summary = await sync_post_content_type(
            dbname=args.dbname,
            apply=args.apply,
            allow_multiple_bots=args.allow_multiple_bots,
            chunk_size=args.chunk_size,
            chat_bot_id=args.chat_bot_id,
            db_type=args.db_type,
            rdbms_retry_count=args.rdbms_retry_count,
            rdbms_retry_delay_seconds=args.rdbms_retry_delay_seconds,
            sync_pg_existing_post=args.sync_pg_existing_post,
        )
    finally:
        await _close_standalone_db_pools()
    if args.json:
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(render_text_summary(summary))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
