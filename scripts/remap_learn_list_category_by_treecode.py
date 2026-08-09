from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
DEFAULT_CHUNK_SIZE = 500
LOGGER = logging.getLogger("remap_learn_list_category_by_treecode")
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False
_MARIA = "maria"
_MYSQL = "mysql"
_MYSQL_DB_NAMES = {"naraone"}
_STANDALONE_ENV_LOADED = False
_STANDALONE_MARIA_POOLS: dict[tuple[str, str], Any] = {}

# ──────────────────────────────────────────────
# Optional direct-run defaults
#
# CLI 인자를 매번 치기 번거로우면 아래 값만 채우고 실행해도 됩니다.
# - TARGET_DBNAME: 필수 DB명
# - TARGET_CHAT_BOT_ID: 필수 chat_bot_id
# - OLD_CATE_TREECODE: 수정 전 기준 cate_treecode
# - NEW_CATE_TREECODE: 수정 후 기준 cate_treecode
# - APPLY_CHANGES=False: 기본 dry-run
# - TARGET_CHUNK_SIZE=500: LEARN_LIST.id 기준 UPDATE chunk 크기
# - TARGET_LOG_LEVEL="INFO": 진행 상황 로그 레벨
# ──────────────────────────────────────────────
TARGET_DBNAME = ""
TARGET_CHAT_BOT_ID = "b61ca52f-a3e1-4be5-a484-8fe87ff1dfaf"
OLD_CATE_TREECODE = "c00020002"
NEW_CATE_TREECODE = "c00030002"
TARGET_DB_TYPE = "maria"
APPLY_CHANGES = False
TARGET_CHUNK_SIZE = DEFAULT_CHUNK_SIZE
JSON_OUTPUT = False
TARGET_LOG_LEVEL = "INFO"

# DB 접속값 직접 설정. config.py 기준으로 채웠고, 빈 문자열이면 .env/환경변수 값을 사용합니다.
MARIADB_HOST = "110.45.147.58"  # MariaDB host. config.py Config.MARIA_DB_HOST 기준입니다.
MARIADB_PORT = 3306  # MariaDB port. 일반적으로 3306입니다.
MARIADB_USER = "chatty_master"  # MariaDB user. config.py Config.MARIA_DB_USER 기준입니다.
MARIADB_PASSWORD = "dktkekf0215@#"  # MariaDB password. config.py Config.MARIA_DB_PASSWORD 기준입니다.
MARIADB_POOL_MIN = 1  # MariaDB 연결 pool 최소 개수입니다. 기존 maria_db_config.py는 Config.DB_POOL_MIN을 사용합니다.
MARIADB_POOL_MAX = 35  # MariaDB 연결 pool 최대 개수입니다. 기존 maria_db_config.py는 Config.DB_POOL_MAX를 사용합니다.

MYSQL_HOST = ""  # TARGET_DB_TYPE="mysql"일 때 MySQL host. 비워두면 MYSQL_HOST 값을 사용합니다.
MYSQL_PORT = 3306  # TARGET_DB_TYPE="mysql"일 때 MySQL port입니다.
MYSQL_USER = ""  # TARGET_DB_TYPE="mysql"일 때 MySQL user. 비워두면 MYSQL_USER 값을 사용합니다.
MYSQL_PASSWORD = ""  # TARGET_DB_TYPE="mysql"일 때 MySQL password. 비워두면 MYSQL_PASS 또는 MYSQL_PASSWORD 값을 사용합니다.
MYSQL_DATABASE = ""  # TARGET_DB_TYPE="mysql"일 때 기본 database. 비워두면 실행 dbname 또는 MYSQL_DB 값을 사용합니다.

ExecuteQuery = Callable[..., Awaitable[Any]]
TableNameBuilder = Callable[[str], str]
CategoryTableNameBuilder = Callable[[str, str], str]
DbTypeResolver = Callable[[str, Optional[str]], str]


@dataclass(frozen=True)
class CategoryRow:
    cate_code: str
    cate_treecode: str
    cate_name: str
    url: str = ""


@dataclass(frozen=True)
class ChildCategoryMapping:
    cate_name: str
    old_cate_code: str
    old_cate_treecode: str
    new_cate_code: str
    new_cate_treecode: str


@dataclass
class RemapSummary:
    dbname: str
    db_type: str
    apply: bool
    chat_bot_id: str
    old_cate_treecode: str
    new_cate_treecode: str
    chunk_size: int = DEFAULT_CHUNK_SIZE
    learn_table: str = ""
    category_table: str = ""
    old_parent_cate_code: str = ""
    new_parent_cate_code: str = ""
    old_child_count: int = 0
    new_child_count: int = 0
    matched_child_count: int = 0
    updates_planned: int = 0
    updates_applied_estimate: int = 0
    chunks_applied: int = 0
    selected_row_count: int = 0
    created_child_count: int = 0
    created_categories: list[dict[str, Any]] = field(default_factory=list)
    category_change_stats: list[dict[str, Any]] = field(default_factory=list)
    zero_match_reason: str | None = None
    missing_name_matches: list[dict[str, str]] = field(default_factory=list)
    ambiguous_name_matches: list[dict[str, Any]] = field(default_factory=list)
    missing_tables: list[str] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(identifier or ""):
        raise ValueError(f"unsafe SQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _normalize_config_value(value: Any) -> Any | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return value


def _normalize_treecode(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_name(value: Any) -> str:
    return str(value or "").strip()


def _row_value(row: Any, key: str, index: int, default: Any = "") -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    if isinstance(row, (tuple, list)):
        return row[index] if len(row) > index else default
    try:
        return row[key]
    except Exception:
        try:
            return row[index]
        except Exception:
            return default


def _count_from_row(row: Any, *keys: str) -> int:
    if row is None:
        return 0
    missing = object()
    for key in keys or ("count",):
        if isinstance(row, Mapping):
            if key not in row:
                continue
            value = row.get(key)
        else:
            try:
                value = row[key]
            except Exception:
                value = missing
        if value is missing:
            continue
        try:
            return int(value or 0)
        except Exception:
            continue
    if isinstance(row, (tuple, list)) and row:
        try:
            return int(row[0] or 0)
        except Exception:
            return 0
    try:
        return int(row or 0)
    except Exception:
        return 0


def _category_row_from_row(row: Any) -> CategoryRow:
    return CategoryRow(
        cate_code=str(_row_value(row, "cate_code", 0, "") or "").strip(),
        cate_treecode=str(_row_value(row, "cate_treecode", 1, "") or "").strip(),
        cate_name=str(_row_value(row, "cate_name", 2, "") or "").strip(),
        url=str(_row_value(row, "url", 3, "") or "").strip(),
    )


def _candidate_rows_from_rows(rows: Any) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows or []:
        row_id = _row_value(row, "id", 0, None)
        cate2 = str(_row_value(row, "cate2", 1, "") or "").strip()
        if row_id is not None and cate2:
            candidates.append({"id": row_id, "cate2": cate2})
    return candidates


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


def _standalone_resolve_category_table_name(chat_bot_id: str, dbname: str) -> str:
    if str(dbname or "").strip().lower() == "chatty":
        return "ASADAL_CHATTY_CATEGORY"
    tail = str(chat_bot_id or "").replace("-", "")[-12:]
    if not tail:
        raise ValueError("chat_bot_id가 필요합니다.")
    return f"ASADAL_{tail}_CATEGORY"


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


async def _close_standalone_db_pools() -> None:
    maria_pools = list(_STANDALONE_MARIA_POOLS.values())
    _STANDALONE_MARIA_POOLS.clear()
    for pool in maria_pools:
        pool.close()
        await pool.wait_closed()


def _load_db_helpers() -> tuple[ExecuteQuery, TableNameBuilder, CategoryTableNameBuilder, DbTypeResolver]:
    return (
        _standalone_execute_rdbms_query,
        _standalone_build_url_learn_list_table_name,
        _standalone_resolve_category_table_name,
        _standalone_resolve_maria_db_type,
    )


def _resolve_helpers(
    execute_query: ExecuteQuery | None,
    table_name_builder: TableNameBuilder | None,
    category_table_name_builder: CategoryTableNameBuilder | None,
    db_type_resolver: DbTypeResolver | None,
) -> tuple[ExecuteQuery, TableNameBuilder, CategoryTableNameBuilder, DbTypeResolver]:
    if (
        execute_query is not None
        and table_name_builder is not None
        and category_table_name_builder is not None
        and db_type_resolver is not None
    ):
        return execute_query, table_name_builder, category_table_name_builder, db_type_resolver

    loaded_query, loaded_table_builder, loaded_category_builder, loaded_db_type_resolver = _load_db_helpers()
    return (
        execute_query or loaded_query,
        table_name_builder or loaded_table_builder,
        category_table_name_builder or loaded_category_builder,
        db_type_resolver or loaded_db_type_resolver,
    )


async def table_exists(
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
        fetch="one",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return _count_from_row(row, "count") > 0


async def fetch_category_roots(
    *,
    dbname: str,
    category_table: str,
    old_cate_treecode: str,
    new_cate_treecode: str,
    execute_query: ExecuteQuery,
    db_type: str,
) -> dict[str, CategoryRow]:
    rows = await execute_query(
        f"""
        SELECT cate_code, cate_treecode, cate_name, url
        FROM {quote_identifier(category_table)}
        WHERE LOWER(COALESCE(cate_treecode, '')) IN (%s, %s)
        """,
        (_normalize_treecode(old_cate_treecode), _normalize_treecode(new_cate_treecode)),
        fetch="all",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    roots: dict[str, CategoryRow] = {}
    for row in rows or []:
        item = _category_row_from_row(row)
        if item.cate_treecode:
            roots[_normalize_treecode(item.cate_treecode)] = item
    return roots


async def fetch_category_children(
    *,
    dbname: str,
    category_table: str,
    parent_cate_treecode: str,
    execute_query: ExecuteQuery,
    db_type: str,
    direct_only: bool = False,
) -> list[CategoryRow]:
    parent_tree = _normalize_treecode(parent_cate_treecode)
    direct_clause = "AND CHAR_LENGTH(LOWER(COALESCE(cate_treecode, ''))) = %s" if direct_only else ""
    params: tuple[Any, ...] = (
        f"{parent_tree}%",
        parent_tree,
        len(parent_tree) + 4,
    ) if direct_only else (f"{parent_tree}%", parent_tree)
    rows = await execute_query(
        f"""
        SELECT cate_code, cate_treecode, cate_name, url
        FROM {quote_identifier(category_table)}
        WHERE LOWER(COALESCE(cate_treecode, '')) LIKE %s
          AND LOWER(COALESCE(cate_treecode, '')) <> %s
          {direct_clause}
        ORDER BY cate_treecode, cate_code
        """,
        params,
        fetch="all",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return [_category_row_from_row(row) for row in rows or []]


def build_child_mapping_by_name(
    old_children: list[CategoryRow],
    new_children: list[CategoryRow],
) -> tuple[dict[str, ChildCategoryMapping], list[dict[str, str]], list[dict[str, Any]]]:
    new_by_name: dict[str, list[CategoryRow]] = {}
    for child in new_children:
        name = _normalize_name(child.cate_name)
        if not name:
            continue
        new_by_name.setdefault(name, []).append(child)

    mappings: dict[str, ChildCategoryMapping] = {}
    missing: list[dict[str, str]] = []
    ambiguous: list[dict[str, Any]] = []

    for old_child in old_children:
        name = _normalize_name(old_child.cate_name)
        if not name:
            missing.append(
                {
                    "old_cate_code": old_child.cate_code,
                    "old_cate_treecode": old_child.cate_treecode,
                    "cate_name": "",
                    "reason": "empty_old_cate_name",
                }
            )
            continue

        matches = new_by_name.get(name, [])
        if not matches:
            missing.append(
                {
                    "old_cate_code": old_child.cate_code,
                    "old_cate_treecode": old_child.cate_treecode,
                    "cate_name": name,
                    "reason": "missing_new_cate_name",
                }
            )
            continue
        if len(matches) > 1:
            ambiguous.append(
                {
                    "cate_name": name,
                    "old_cate_code": old_child.cate_code,
                    "new_candidates": [
                        {
                            "cate_code": match.cate_code,
                            "cate_treecode": match.cate_treecode,
                        }
                        for match in matches
                    ],
                }
            )
            continue

        new_child = matches[0]
        mappings[old_child.cate_code] = ChildCategoryMapping(
            cate_name=name,
            old_cate_code=old_child.cate_code,
            old_cate_treecode=old_child.cate_treecode,
            new_cate_code=new_child.cate_code,
            new_cate_treecode=new_child.cate_treecode,
        )

    return mappings, missing, ambiguous



def _selected_payload_row(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return {
            "id": row.get("id"),
            "content": row.get("content") or row.get("url") or "",
            "url": row.get("url") or row.get("content") or "",
            "original_cate1": row.get("original_cate1") or row.get("cate1") or "",
            "original_cate2": row.get("original_cate2") or row.get("cate2") or "",
        }
    return {
        "id": getattr(row, "id", None),
        "content": getattr(row, "content", getattr(row, "url", "")),
        "url": getattr(row, "url", getattr(row, "content", "")),
        "original_cate1": getattr(row, "original_cate1", getattr(row, "cate1", "")),
        "original_cate2": getattr(row, "original_cate2", getattr(row, "cate2", "")),
    }


def _normalize_selected_payload(rows: list[Any] | None) -> list[dict[str, Any]]:
    return [row for row in (_selected_payload_row(item) for item in rows or []) if row.get("id") is not None]


def build_category_change_stats(
    selected_rows: list[dict[str, Any]],
    child_mappings: dict[str, ChildCategoryMapping],
) -> list[dict[str, Any]]:
    stats_by_old_cate2: dict[str, dict[str, Any]] = {}
    for payload in selected_rows:
        old_cate2 = str(payload.get("original_cate2") or "").strip()
        mapping = child_mappings.get(old_cate2)
        if not mapping or not mapping_changes_cate2(mapping):
            continue
        stat = stats_by_old_cate2.setdefault(
            old_cate2,
            {
                "old_cate2": mapping.old_cate_code,
                "old_cate2_treecode": mapping.old_cate_treecode,
                "new_cate2": mapping.new_cate_code,
                "new_cate2_treecode": mapping.new_cate_treecode,
                "cate_name": mapping.cate_name,
                "row_count": 0,
            },
        )
        stat["row_count"] += 1
    return sorted(stats_by_old_cate2.values(), key=lambda item: (str(item["cate_name"]), str(item["old_cate2"])))


def mapping_changes_cate2(mapping: ChildCategoryMapping) -> bool:
    return str(mapping.old_cate_code or "").strip() != str(mapping.new_cate_code or "").strip()


def filter_noop_child_mappings(
    child_mappings: dict[str, ChildCategoryMapping],
) -> tuple[dict[str, ChildCategoryMapping], int]:
    filtered = {
        old_cate_code: mapping
        for old_cate_code, mapping in child_mappings.items()
        if mapping_changes_cate2(mapping)
    }
    return filtered, len(child_mappings) - len(filtered)


async def fetch_category_by_code(
    *,
    dbname: str,
    category_table: str,
    cate_code: str,
    execute_query: ExecuteQuery,
    db_type: str,
) -> CategoryRow | None:
    row = await execute_query(
        f"""
        SELECT cate_code, cate_treecode, cate_name, url
        FROM {quote_identifier(category_table)}
        WHERE cate_code = %s
        """,
        (cate_code,),
        fetch="one",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return _category_row_from_row(row) if row else None


def _child_parent_treecode(parent: CategoryRow) -> str:
    return _normalize_treecode(parent.cate_treecode)


def _next_child_suffix(existing_children: list[CategoryRow]) -> str:
    max_suffix = 0
    for child in existing_children:
        for value in (child.cate_code, child.cate_treecode):
            text = str(value or "")
            if len(text) >= 4 and text[-4:].isdigit():
                max_suffix = max(max_suffix, int(text[-4:]))
    return f"{max_suffix + 1:04d}"


async def generated_category_key_exists(
    *,
    dbname: str,
    category_table: str,
    cate_code: str,
    cate_treecode: str,
    execute_query: ExecuteQuery,
    db_type: str,
) -> bool:
    row = await execute_query(
        f"""
        SELECT COUNT(*) AS count
        FROM {quote_identifier(category_table)}
        WHERE cate_code = %s
           OR cate_treecode = %s
        """,
        (cate_code, cate_treecode),
        fetch="one",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return _count_from_row(row, "count") > 0


async def next_unique_generated_category_key(
    *,
    dbname: str,
    category_table: str,
    parent: CategoryRow,
    existing_children: list[CategoryRow],
    execute_query: ExecuteQuery,
    db_type: str,
) -> tuple[str, str]:
    suffix_number = int(_next_child_suffix(existing_children))
    parent_treecode = _child_parent_treecode(parent)
    while True:
        suffix = f"{suffix_number:04d}"
        cate_code = f"{parent.cate_code}{suffix}"
        cate_treecode = f"{parent_treecode}{suffix}"
        if not await generated_category_key_exists(
            dbname=dbname,
            category_table=category_table,
            cate_code=cate_code,
            cate_treecode=cate_treecode,
            execute_query=execute_query,
            db_type=db_type,
        ):
            return cate_code, cate_treecode
        suffix_number += 1


async def ensure_board_child_category(
    *,
    dbname: str,
    category_table: str,
    board_parent: CategoryRow,
    source_category: CategoryRow,
    apply: bool,
    execute_query: ExecuteQuery,
    db_type: str,
) -> tuple[ChildCategoryMapping | None, bool, dict[str, Any] | None]:
    name = _normalize_name(source_category.cate_name)
    if not name:
        return None, False, {"old_cate_code": source_category.cate_code, "reason": "empty_source_cate_name"}
    board_children = await fetch_category_children(
        dbname=dbname,
        category_table=category_table,
        parent_cate_treecode=board_parent.cate_treecode,
        execute_query=execute_query,
        db_type=db_type,
        direct_only=True,
    )
    matches = [child for child in board_children if _normalize_name(child.cate_name) == name]
    if len(matches) > 1:
        return None, False, {
            "cate_name": name,
            "old_cate_code": source_category.cate_code,
            "reason": "ambiguous_board_child",
            "new_candidates": [{"cate_code": child.cate_code, "cate_treecode": child.cate_treecode} for child in matches],
        }
    if len(matches) == 1:
        target = matches[0]
        return ChildCategoryMapping(name, source_category.cate_code, source_category.cate_treecode, target.cate_code, target.cate_treecode), False, None

    new_code, new_treecode = await next_unique_generated_category_key(
        dbname=dbname,
        category_table=category_table,
        parent=board_parent,
        existing_children=board_children,
        execute_query=execute_query,
        db_type=db_type,
    )
    if apply:
        await execute_query(
            f"""
            INSERT INTO {quote_identifier(category_table)}
                (cate_code, cate_treecode, cate_name, url, cate_use, cate_use_part)
            VALUES (%s, %s, %s, %s, 'y', 'p')
            """,
            (new_code, new_treecode, name, source_category.url),
            dbname=dbname,
            db_type=db_type,
        )
    return ChildCategoryMapping(name, source_category.cate_code, source_category.cate_treecode, new_code, new_treecode), True, None


async def ensure_board_parent_category(
    *,
    dbname: str,
    category_table: str,
    new_root: CategoryRow,
    new_root_children: list[CategoryRow],
    apply: bool,
    execute_query: ExecuteQuery,
    db_type: str,
) -> tuple[CategoryRow | None, bool, dict[str, Any] | None]:
    matches = [child for child in new_root_children if _normalize_name(child.cate_name) == "게시판"]
    if len(matches) > 1:
        return None, False, {"cate_name": "게시판", "reason": "ambiguous_board_parent", "matches": len(matches)}
    if len(matches) == 1:
        return matches[0], False, None

    board_code, board_treecode = await next_unique_generated_category_key(
        dbname=dbname,
        category_table=category_table,
        parent=new_root,
        existing_children=new_root_children,
        execute_query=execute_query,
        db_type=db_type,
    )
    if apply:
        await execute_query(
            f"""
            INSERT INTO {quote_identifier(category_table)}
                (cate_code, cate_treecode, cate_name, url, cate_use, cate_use_part)
            VALUES (%s, %s, %s, %s, 'y', 'p')
            """,
            (board_code, board_treecode, "게시판", new_root.url),
            dbname=dbname,
            db_type=db_type,
        )
    return CategoryRow(
        cate_code=board_code,
        cate_treecode=board_treecode,
        cate_name="게시판",
        url=new_root.url,
    ), True, None


async def build_selected_payload_mappings(
    *,
    dbname: str,
    category_table: str,
    new_root: CategoryRow,
    selected_rows: list[dict[str, Any]],
    apply: bool,
    execute_query: ExecuteQuery,
    db_type: str,
) -> tuple[dict[str, ChildCategoryMapping], CategoryRow | None, int, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    new_children = await fetch_category_children(
        dbname=dbname,
        category_table=category_table,
        parent_cate_treecode=new_root.cate_treecode,
        execute_query=execute_query,
        db_type=db_type,
        direct_only=True,
    )
    board_parent, board_created, board_problem = await ensure_board_parent_category(
        dbname=dbname,
        category_table=category_table,
        new_root=new_root,
        new_root_children=new_children,
        apply=apply,
        execute_query=execute_query,
        db_type=db_type,
    )
    if board_problem:
        return {}, None, 0, [], [board_problem], []
    if board_parent is None:
        return {}, None, 0, [], [{"cate_name": "게시판", "reason": "board_parent_unavailable"}], []
    mappings: dict[str, ChildCategoryMapping] = {}
    created_count = int(board_created)
    created_categories: list[dict[str, Any]] = []
    if board_created:
        created_categories.append(
            {
                "cate_code": board_parent.cate_code,
                "cate_treecode": board_parent.cate_treecode,
                "cate_name": board_parent.cate_name,
                "url": board_parent.url,
                "parent_cate_code": new_root.cate_code,
                "parent_cate_treecode": new_root.cate_treecode,
                "category_role": "board_parent",
            }
        )
    missing: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    for payload in selected_rows:
        source_code = str(payload.get("original_cate2") or payload.get("original_cate1") or "").strip()
        if not source_code or source_code in mappings:
            continue
        source = await fetch_category_by_code(
            dbname=dbname,
            category_table=category_table,
            cate_code=source_code,
            execute_query=execute_query,
            db_type=db_type,
        )
        if not source:
            missing.append({"old_cate_code": source_code, "reason": "source_category_not_found"})
            continue
        mapping, created, problem = await ensure_board_child_category(
            dbname=dbname,
            category_table=category_table,
            board_parent=board_parent,
            source_category=source,
            apply=apply,
            execute_query=execute_query,
            db_type=db_type,
        )
        if problem:
            if problem.get("reason") == "ambiguous_board_child":
                ambiguous.append(problem)
            else:
                missing.append(problem)
            continue
        if mapping:
            mappings[source_code] = mapping
            created_count += int(created)
            if created:
                created_categories.append(
                    {
                        "cate_code": mapping.new_cate_code,
                        "cate_treecode": mapping.new_cate_treecode,
                        "cate_name": mapping.cate_name,
                        "url": source.url,
                        "parent_cate_code": board_parent.cate_code,
                        "parent_cate_treecode": board_parent.cate_treecode,
                        "source_cate_code": source.cate_code,
                        "source_cate_treecode": source.cate_treecode,
                        "category_role": "board_child",
                    }
                )
    return mappings, board_parent, created_count, missing, ambiguous, created_categories


async def count_selected_update_candidates(
    *,
    learn_table: str,
    selected_rows: list[dict[str, Any]],
    child_mappings: dict[str, ChildCategoryMapping],
    execute_query: ExecuteQuery,
    dbname: str,
    db_type: str,
) -> int:
    ids = []
    for row in selected_rows:
        mapping = child_mappings.get(str(row.get("original_cate2") or "").strip())
        if mapping and mapping_changes_cate2(mapping):
            ids.append(row["id"])
    if not ids:
        return 0
    placeholders = ", ".join(["%s"] * len(ids))
    row = await execute_query(
        f"""
        SELECT COUNT(*) AS target_count
        FROM {quote_identifier(learn_table)}
        WHERE id IN ({placeholders})
        """,
        tuple(ids),
        fetch="one",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return _count_from_row(row, "target_count", "count")


def _iter_chunks(values: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]


async def apply_selected_updates(
    *,
    dbname: str,
    learn_table: str,
    board_parent_cate_code: str,
    selected_rows: list[dict[str, Any]],
    child_mappings: dict[str, ChildCategoryMapping],
    execute_query: ExecuteQuery,
    db_type: str,
    chunk_size: int,
) -> tuple[int, int]:
    grouped_ids: dict[str, list[Any]] = {}
    for payload in selected_rows:
        source_code = str(payload.get("original_cate2") or "").strip()
        mapping = child_mappings.get(source_code)
        row_id = payload.get("id")
        if not mapping or not mapping_changes_cate2(mapping) or row_id is None:
            continue
        grouped_ids.setdefault(mapping.new_cate_code, []).append(row_id)

    total_targets = sum(len(ids) for ids in grouped_ids.values())
    LOGGER.info(
        "[선택 row 카테고리 UPDATE 시작] [DB:%s] 학습테이블=%s 대상=%s 그룹=%s 배치크기=%s",
        dbname,
        learn_table,
        total_targets,
        len(grouped_ids),
        chunk_size,
    )
    if not grouped_ids:
        return 0, 0

    applied = 0
    chunks_applied = 0
    for new_child_cate_code, ids in sorted(grouped_ids.items()):
        for chunk_ids in _iter_chunks(ids, chunk_size):
            placeholders = ", ".join(["%s"] * len(chunk_ids))
            await execute_query(
                f"""
                UPDATE {quote_identifier(learn_table)}
                SET cate1 = %s, cate2 = %s
                WHERE id IN ({placeholders})
                """,
                tuple([board_parent_cate_code, new_child_cate_code, *chunk_ids]),
                dbname=dbname,
                db_type=db_type,
            )
            applied += len(chunk_ids)
            chunks_applied += 1
            LOGGER.info(
                "[선택 row 카테고리 UPDATE 진행] [DB:%s] 학습테이블=%s chunk=%s 반영누적=%s/%s 현재그룹=%s 현재chunk=%s",
                dbname,
                learn_table,
                chunks_applied,
                applied,
                total_targets,
                new_child_cate_code,
                len(chunk_ids),
            )

    LOGGER.info(
        "[선택 row 카테고리 UPDATE 완료] [DB:%s] 학습테이블=%s 반영=%s 배치=%s",
        dbname,
        learn_table,
        applied,
        chunks_applied,
    )
    return applied, chunks_applied

async def count_update_candidates(
    *,
    dbname: str,
    learn_table: str,
    old_parent_cate_code: str,
    old_child_cate_codes: list[str],
    execute_query: ExecuteQuery,
    db_type: str,
) -> int:
    if not old_child_cate_codes:
        return 0
    placeholders = ", ".join(["%s"] * len(old_child_cate_codes))
    row = await execute_query(
        f"""
        SELECT COUNT(*) AS target_count
        FROM {quote_identifier(learn_table)}
        WHERE LOWER(COALESCE(content_type, '')) IN ('url', 'post')
          AND cate1 = %s
          AND cate2 IN ({placeholders})
        """,
        tuple([old_parent_cate_code, *old_child_cate_codes]),
        fetch="one",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return _count_from_row(row, "target_count", "count")


async def fetch_update_candidate_rows(
    *,
    dbname: str,
    learn_table: str,
    old_parent_cate_code: str,
    old_child_cate_codes: list[str],
    execute_query: ExecuteQuery,
    db_type: str,
    chunk_size: int,
) -> list[dict[str, Any]]:
    if not old_child_cate_codes:
        return []
    placeholders = ", ".join(["%s"] * len(old_child_cate_codes))
    rows = await execute_query(
        f"""
        SELECT id, cate2
        FROM {quote_identifier(learn_table)}
        WHERE LOWER(COALESCE(content_type, '')) IN ('url', 'post')
          AND cate1 = %s
          AND cate2 IN ({placeholders})
        ORDER BY id
        LIMIT %s
        """,
        tuple([old_parent_cate_code, *old_child_cate_codes, chunk_size]),
        fetch="all",
        as_dict=True,
        dbname=dbname,
        db_type=db_type,
    )
    return _candidate_rows_from_rows(rows)


def _group_candidate_ids_by_cate2(candidate_rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = {}
    for row in candidate_rows:
        cate2 = str(row.get("cate2") or "").strip()
        row_id = row.get("id")
        if cate2 and row_id is not None:
            grouped.setdefault(cate2, []).append(row_id)
    return grouped


async def apply_update_group(
    *,
    dbname: str,
    learn_table: str,
    ids: list[Any],
    old_parent_cate_code: str,
    old_child_cate_code: str,
    new_parent_cate_code: str,
    new_child_cate_code: str,
    execute_query: ExecuteQuery,
    db_type: str,
) -> None:
    if not ids:
        return
    placeholders = ", ".join(["%s"] * len(ids))
    await execute_query(
        f"""
        UPDATE {quote_identifier(learn_table)}
        SET cate1 = %s,
            cate2 = %s
        WHERE id IN ({placeholders})
          AND cate1 = %s
          AND cate2 = %s
        """,
        tuple([new_parent_cate_code, new_child_cate_code, *ids, old_parent_cate_code, old_child_cate_code]),
        dbname=dbname,
        db_type=db_type,
    )


async def apply_updates_in_chunks(
    *,
    dbname: str,
    learn_table: str,
    old_parent_cate_code: str,
    new_parent_cate_code: str,
    child_mappings: dict[str, ChildCategoryMapping],
    execute_query: ExecuteQuery,
    db_type: str,
    chunk_size: int,
) -> tuple[int, int]:
    applied_estimate = 0
    chunks_applied = 0
    old_child_codes = list(child_mappings.keys())

    while True:
        next_chunk_no = chunks_applied + 1
        LOGGER.info("[chunk 대상 조회] [DB:%s] learn_table=%s chunk=%s size=%s", dbname, learn_table, next_chunk_no, chunk_size)
        candidate_rows = await fetch_update_candidate_rows(
            dbname=dbname,
            learn_table=learn_table,
            old_parent_cate_code=old_parent_cate_code,
            old_child_cate_codes=old_child_codes,
            execute_query=execute_query,
            db_type=db_type,
            chunk_size=chunk_size,
        )
        if not candidate_rows:
            LOGGER.info("[chunk 종료] [DB:%s] learn_table=%s chunk=%s 대상 없음", dbname, learn_table, next_chunk_no)
            break

        grouped_ids = _group_candidate_ids_by_cate2(candidate_rows)
        LOGGER.info(
            "[chunk UPDATE 시작] [DB:%s] learn_table=%s chunk=%s rows=%s groups=%s",
            dbname,
            learn_table,
            next_chunk_no,
            len(candidate_rows),
            len(grouped_ids),
        )
        for old_child_cate_code, ids in grouped_ids.items():
            mapping = child_mappings.get(old_child_cate_code)
            if not mapping or not mapping_changes_cate2(mapping):
                continue
            await apply_update_group(
                dbname=dbname,
                learn_table=learn_table,
                ids=ids,
                old_parent_cate_code=old_parent_cate_code,
                old_child_cate_code=old_child_cate_code,
                new_parent_cate_code=new_parent_cate_code,
                new_child_cate_code=mapping.new_cate_code,
                execute_query=execute_query,
                db_type=db_type,
            )
            applied_estimate += len(ids)

        chunks_applied += 1
        LOGGER.info(
            "[chunk UPDATE 완료] [DB:%s] learn_table=%s chunk=%s applied_estimate=%s",
            dbname,
            learn_table,
            next_chunk_no,
            applied_estimate,
        )
        if len(candidate_rows) < chunk_size:
            break

    return applied_estimate, chunks_applied


async def remap_learn_list_category_by_treecode(
    *,
    dbname: str,
    chat_bot_id: str,
    old_cate_treecode: str,
    new_cate_treecode: str,
    apply: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    db_type: str | None = None,
    execute_query: ExecuteQuery | None = None,
    table_name_builder: TableNameBuilder | None = None,
    category_table_name_builder: CategoryTableNameBuilder | None = None,
    db_type_resolver: DbTypeResolver | None = None,
    selected_rows: list[Any] | None = None,
) -> RemapSummary:
    dbname = str(dbname or "").strip()
    chat_bot_id = str(chat_bot_id or "").strip()
    old_cate_treecode = str(old_cate_treecode or "").strip()
    new_cate_treecode = str(new_cate_treecode or "").strip()
    if not dbname:
        raise ValueError("--dbname is required")
    if not chat_bot_id:
        raise ValueError("--chat-bot-id is required")
    if not old_cate_treecode:
        raise ValueError("--old-cate-treecode is required")
    if not new_cate_treecode:
        raise ValueError("--new-cate-treecode is required")
    if chunk_size <= 0:
        raise ValueError("--chunk-size must be greater than 0")

    execute_query, table_name_builder, category_table_name_builder, db_type_resolver = _resolve_helpers(
        execute_query,
        table_name_builder,
        category_table_name_builder,
        db_type_resolver,
    )
    resolved_db_type = db_type_resolver(dbname, db_type)
    learn_table = table_name_builder(chat_bot_id)
    category_table = category_table_name_builder(chat_bot_id, dbname)
    quote_identifier(learn_table)
    quote_identifier(category_table)

    summary = RemapSummary(
        dbname=dbname,
        db_type=resolved_db_type,
        apply=apply,
        chat_bot_id=chat_bot_id,
        old_cate_treecode=old_cate_treecode,
        new_cate_treecode=new_cate_treecode,
        chunk_size=chunk_size,
        learn_table=learn_table,
        category_table=category_table,
    )

    LOGGER.info(
        "[카테고리 보정 시작] [DB:%s] DB타입=%s chat_bot_id=%s 기존트리=%s 새트리=%s 실행모드=%s 배치크기=%s",
        dbname,
        resolved_db_type,
        chat_bot_id,
        old_cate_treecode,
        new_cate_treecode,
        "실제반영" if apply else "점검",
        chunk_size,
    )

    for table_name in (category_table, learn_table):
        if not await table_exists(
            dbname=dbname,
            table_name=table_name,
            execute_query=execute_query,
            db_type=resolved_db_type,
        ):
            summary.missing_tables.append(table_name)
    if summary.missing_tables:
        LOGGER.warning("[카테고리 보정 중단] [DB:%s] chat_bot_id=%s 이유=필수 테이블 없음 tables=%s", dbname, chat_bot_id, ", ".join(summary.missing_tables))
        return summary

    roots = await fetch_category_roots(
        dbname=dbname,
        category_table=category_table,
        old_cate_treecode=old_cate_treecode,
        new_cate_treecode=new_cate_treecode,
        execute_query=execute_query,
        db_type=resolved_db_type,
    )
    old_root = roots.get(_normalize_treecode(old_cate_treecode))
    new_root = roots.get(_normalize_treecode(new_cate_treecode))
    if not old_root:
        summary.errors.append({"old_cate_treecode": old_cate_treecode, "error": "old root category not found"})
    if not new_root:
        summary.errors.append({"new_cate_treecode": new_cate_treecode, "error": "new root category not found"})
    if summary.errors:
        LOGGER.warning("[카테고리 보정 중단] [DB:%s] chat_bot_id=%s 이유=기존/새 기준 카테고리 조회 실패 errors=%s", dbname, chat_bot_id, summary.errors)
        return summary

    summary.old_parent_cate_code = old_root.cate_code
    summary.new_parent_cate_code = new_root.cate_code
    LOGGER.info(
        "[기준 카테고리 확인] [DB:%s] chat_bot_id=%s 기존=%s(cate_code=%s) 새기준=%s(cate_code=%s)",
        dbname,
        chat_bot_id,
        old_root.cate_treecode,
        old_root.cate_code,
        new_root.cate_treecode,
        new_root.cate_code,
    )

    selected_payload = _normalize_selected_payload(selected_rows)
    summary.selected_row_count = len(selected_payload)

    old_children = await fetch_category_children(
        dbname=dbname,
        category_table=category_table,
        parent_cate_treecode=old_cate_treecode,
        execute_query=execute_query,
        db_type=resolved_db_type,
    )
    board_parent: CategoryRow | None = None
    if selected_payload:
        child_mappings, board_parent, created_count, missing_matches, ambiguous_matches, created_categories = await build_selected_payload_mappings(
            dbname=dbname,
            category_table=category_table,
            new_root=new_root,
            selected_rows=selected_payload,
            apply=apply,
            execute_query=execute_query,
            db_type=resolved_db_type,
        )
        new_children = await fetch_category_children(
            dbname=dbname,
            category_table=category_table,
            parent_cate_treecode=new_cate_treecode,
            execute_query=execute_query,
            db_type=resolved_db_type,
        )
        summary.created_child_count = created_count
        summary.created_categories = created_categories
    else:
        new_children = await fetch_category_children(
            dbname=dbname,
            category_table=category_table,
            parent_cate_treecode=new_cate_treecode,
            execute_query=execute_query,
            db_type=resolved_db_type,
        )
        child_mappings, missing_matches, ambiguous_matches = build_child_mapping_by_name(old_children, new_children)
    summary.matched_child_count = len(child_mappings)
    summary.old_child_count = len(old_children)
    summary.new_child_count = len(new_children)
    summary.missing_name_matches = missing_matches
    summary.ambiguous_name_matches = ambiguous_matches
    child_mappings, noop_mapping_count = filter_noop_child_mappings(child_mappings)
    if noop_mapping_count:
        summary.matched_child_count = len(child_mappings)
        LOGGER.info(
            "[카테고리 변경 제외] [DB:%s] chat_bot_id=%s 이유=기존 cate2와 새 cate2가 동일한 매핑 %s건 제외",
            dbname,
            chat_bot_id,
            noop_mapping_count,
        )
    if selected_payload:
        summary.category_change_stats = build_category_change_stats(selected_payload, child_mappings)
    LOGGER.info(
        "[게시판 하위 카테고리 매핑 결과] [DB:%s] chat_bot_id=%s 기존하위=%s 새기준하위=%s 매핑성공=%s 매핑누락=%s 중복이름=%s 생성예정/생성=%s",
        dbname,
        chat_bot_id,
        summary.old_child_count,
        summary.new_child_count,
        summary.matched_child_count,
        len(summary.missing_name_matches),
        len(summary.ambiguous_name_matches),
        summary.created_child_count,
    )

    if not child_mappings:
        LOGGER.warning(
            "[카테고리 변경 대상 없음] [DB:%s] chat_bot_id=%s 이유=post row의 기존 cate2가 비어있거나 CATEGORY에서 같은 cate_name을 찾지 못함",
            dbname,
            chat_bot_id,
        )
        return summary

    old_child_codes = list(child_mappings.keys())
    if selected_payload:
        if apply:
            if board_parent is None:
                summary.errors.append({"cate_name": "게시판", "error": "board parent category not found"})
                LOGGER.warning("[카테고리 변경 대상 없음] [DB:%s] chat_bot_id=%s 이유=새 기준 하위 '게시판' 카테고리 사용 불가", dbname, chat_bot_id)
                return summary
            applied_estimate, chunks_applied = await apply_selected_updates(
                dbname=dbname,
                learn_table=learn_table,
                board_parent_cate_code=board_parent.cate_code,
                selected_rows=selected_payload,
                child_mappings=child_mappings,
                execute_query=execute_query,
                db_type=resolved_db_type,
                chunk_size=chunk_size,
            )
            summary.updates_planned = applied_estimate
            summary.updates_applied_estimate = applied_estimate
            summary.chunks_applied = chunks_applied
        else:
            summary.updates_planned = await count_selected_update_candidates(
                learn_table=learn_table,
                selected_rows=selected_payload,
                child_mappings=child_mappings,
                execute_query=execute_query,
                dbname=dbname,
                db_type=resolved_db_type,
            )
    elif apply:
        applied_estimate, chunks_applied = await apply_updates_in_chunks(
            dbname=dbname,
            learn_table=learn_table,
            old_parent_cate_code=old_root.cate_code,
            new_parent_cate_code=new_root.cate_code,
            child_mappings=child_mappings,
            execute_query=execute_query,
            db_type=resolved_db_type,
            chunk_size=chunk_size,
        )
        summary.updates_planned = applied_estimate
        summary.updates_applied_estimate = applied_estimate
        summary.chunks_applied = chunks_applied
    else:
        summary.updates_planned = await count_update_candidates(
            dbname=dbname,
            learn_table=learn_table,
            old_parent_cate_code=old_root.cate_code,
            old_child_cate_codes=old_child_codes,
            execute_query=execute_query,
            db_type=resolved_db_type,
        )
        LOGGER.info(
            "[점검 모드] 실제 cate1/cate2 변경 안 함 [DB:%s] 학습테이블=%s 변경예정=%s건",
            dbname,
            learn_table,
            summary.updates_planned,
        )

    if selected_payload and summary.updates_planned == 0:
        summary.zero_match_reason = "no_learn_matches"

    LOGGER.info(
        "[카테고리 보정 완료] [DB:%s] 학습테이블=%s cate1=%s→%s 변경대상=%s 반영=%s 배치=%s 생성카테고리=%s",
        dbname,
        learn_table,
        old_root.cate_code,
        new_root.cate_code,
        summary.updates_planned,
        summary.updates_applied_estimate,
        summary.chunks_applied,
        summary.created_child_count,
    )
    return summary


def render_text_summary(summary: RemapSummary) -> str:
    lines = [
        "[post URL 카테고리 보정]",
        f"DB: {summary.dbname} ({summary.db_type})",
        f"실행모드: {'실제반영(APPLY)' if summary.apply else '점검(DRY-RUN)'}",
        f"chat_bot_id: {summary.chat_bot_id}",
        f"학습테이블: {summary.learn_table or '-'}",
        f"카테고리테이블: {summary.category_table or '-'}",
        f"배치크기: {summary.chunk_size}",
        "",
        "기준 카테고리:",
        f"  - 기존: treecode={summary.old_cate_treecode} cate_code={summary.old_parent_cate_code or '-'}",
        f"  - 새 기준: treecode={summary.new_cate_treecode} cate_code={summary.new_parent_cate_code or '-'}",
        "",
        "게시판 하위 카테고리 매핑:",
        f"  - 기존 하위 카테고리 수: {summary.old_child_count}",
        f"  - 새 기준 하위 카테고리 수: {summary.new_child_count}",
        f"  - 매핑 성공: {summary.matched_child_count}",
        f"  - 매핑 누락: {len(summary.missing_name_matches)}",
        f"  - 같은 이름 중복: {len(summary.ambiguous_name_matches)}",
        f"  - 생성 카테고리 수: {summary.created_child_count}",
        "",
        "LEARN_LIST cate1/cate2 변경:",
        f"  - cate1: 기존값 -> 게시판 cate_code",
        "  - cate2: 기존 cate2의 cate_name과 같은 게시판 하위 cate_code",
        f"  - 변경 대상: {summary.updates_planned}",
        f"  - 반영 건수: {summary.updates_applied_estimate}",
        f"  - 배치 수: {summary.chunks_applied}",
    ]
    if summary.missing_tables:
        lines.append("")
        lines.append("누락 테이블:")
        lines.extend(f"  - {table}" for table in summary.missing_tables)
    if summary.missing_name_matches:
        lines.append("")
        lines.append("매핑 누락 카테고리:")
        lines.extend(
            f"  - cate_name={item.get('cate_name') or '-'} 기존 cate_code={item.get('old_cate_code')}"
            for item in summary.missing_name_matches[:20]
        )
        if len(summary.missing_name_matches) > 20:
            lines.append(f"  ... 외 {len(summary.missing_name_matches) - 20}건")
    if summary.ambiguous_name_matches:
        lines.append("")
        lines.append("같은 이름이 중복된 카테고리:")
        lines.extend(
            f"  - cate_name={item.get('cate_name')} 기존 cate_code={item.get('old_cate_code')} "
            f"새 후보 수={len(item.get('new_candidates') or [])}"
            for item in summary.ambiguous_name_matches[:20]
        )
        if len(summary.ambiguous_name_matches) > 20:
            lines.append(f"  ... 외 {len(summary.ambiguous_name_matches) - 20}건")
    if summary.errors:
        lines.append("")
        lines.append("오류:")
        lines.extend(f"  - {item}" for item in summary.errors)
    if not summary.apply:
        lines.append("")
        lines.append("점검 모드입니다: 실제 INSERT/UPDATE는 하지 않았습니다. 적용하려면 --apply를 붙여 실행하세요.")
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    default_dbname = _normalize_config_value(TARGET_DBNAME)
    default_chat_bot_id = _normalize_config_value(TARGET_CHAT_BOT_ID)
    default_old_tree = _normalize_config_value(OLD_CATE_TREECODE)
    default_new_tree = _normalize_config_value(NEW_CATE_TREECODE)
    default_db_type = _normalize_config_value(TARGET_DB_TYPE)
    parser = argparse.ArgumentParser(
        description=(
            "Remap URL/post LEARN_LIST cate1/cate2 from one CATEGORY treecode subtree "
            "to another by matching child cate_name values."
        )
    )
    parser.add_argument(
        "--dbname",
        default=default_dbname,
        required=default_dbname is None,
        help="Target database/schema name. Can also be set with TARGET_DBNAME in this file.",
    )
    parser.add_argument(
        "--chat-bot-id",
        "--chat_bot_id",
        "--chat-bpt-id",
        "--chat_bpt_id",
        dest="chat_bot_id",
        default=default_chat_bot_id,
        required=default_chat_bot_id is None,
        help="Target chat_bot_id. Also accepts --chat_bpt_id/--chat-bpt-id aliases.",
    )
    parser.add_argument(
        "--old-cate-treecode",
        "--old_cate_treecode",
        default=default_old_tree,
        required=default_old_tree is None,
        help="Current parent cate_treecode to move from.",
    )
    parser.add_argument(
        "--new-cate-treecode",
        "--new_cate_treecode",
        default=default_new_tree,
        required=default_new_tree is None,
        help="New parent cate_treecode to move to.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=APPLY_CHANGES,
        help="Actually run UPDATE statements. Default is dry-run.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=TARGET_CHUNK_SIZE,
        help=f"Number of LEARN_LIST.id rows to update per chunk. Default is {DEFAULT_CHUNK_SIZE}.",
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
        summary = await remap_learn_list_category_by_treecode(
            dbname=args.dbname,
            chat_bot_id=args.chat_bot_id,
            old_cate_treecode=args.old_cate_treecode,
            new_cate_treecode=args.new_cate_treecode,
            apply=args.apply,
            chunk_size=args.chunk_size,
            db_type=args.db_type,
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
