from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set

from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from db.mysql_db_config import mysql_execute_query
from backend.shared.url_pattern_identity import canonical_url_key

logger = logging.getLogger("backend.shared.learn_list_duplicate_groups")

_SAFE_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _max_rows() -> int:
    return max(1000, _safe_int(os.getenv("LEARN_LIST_DUPLICATE_GROUP_MAX_ROWS", "500000"), 500000))


def _normalize_limit(value: Any) -> int:
    default = _safe_int(os.getenv("LEARN_LIST_DUPLICATE_GROUP_DEFAULT_LIMIT", "200000"), 200000)
    limit = _safe_int(value, default)
    return max(1, min(limit, _max_rows()))


def _normalize_created_at_bound(value: Any, *, is_end: bool = False) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("T", " ")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return f"{text} {'23:59:59' if is_end else '00:00:00'}"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    raise ValueError("created_at must be YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]")


def _json_safe_datetime(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def learn_list_url_group_key(raw_url: Any) -> str:
    return canonical_url_key(raw_url)


async def _get_table_columns_lower(db_name: str, table_name: str) -> Set[str]:
    rows = await mysql_execute_query(
        """
        SELECT LOWER(column_name) AS column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND LOWER(table_name) = LOWER(%s)
        """,
        (db_name, table_name),
        fetch=True,
        dbname=db_name,
    )
    columns: Set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("column_name") or "").strip().lower()
        if name:
            columns.add(name)
    return columns


def group_learn_list_url_duplicates(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    groups_by_key: Dict[str, List[Dict[str, Any]]] = {}
    scanned = 0
    skipped = 0

    for row in rows or []:
        if not isinstance(row, dict):
            skipped += 1
            continue
        content_type = str(row.get("content_type") or "").strip().lower()
        if content_type and content_type != "url":
            skipped += 1
            continue
        content = str(row.get("content") or "").strip()
        key = learn_list_url_group_key(content)
        if not key:
            skipped += 1
            continue
        scanned += 1
        compact_row = {
            "id": row.get("id"),
            "content": content,
            "subject": row.get("subject"),
            "web_title": row.get("web_title"),
            "status": row.get("status"),
            "type": row.get("type"),
            "cate1": row.get("cate1"),
            "cate2": row.get("cate2"),
            "content_at": _json_safe_datetime(row.get("content_at")),
            "content_created_at": _json_safe_datetime(row.get("content_created_at")),
            "created_at": _json_safe_datetime(row.get("created_at")),
        }
        groups_by_key.setdefault(key, []).append(compact_row)

    duplicate_groups = []
    for key, items in groups_by_key.items():
        if len(items) <= 1:
            continue
        sorted_items = sorted(items, key=lambda item: _safe_int(item.get("id"), 0))
        keep_row = sorted_items[0]
        duplicate_rows = sorted_items[1:]
        duplicate_groups.append(
            {
                "key": key,
                "count": len(sorted_items),
                "duplicate_count": len(duplicate_rows),
                "keep_row": keep_row,
                "rows": sorted_items,
                "duplicate_rows": duplicate_rows,
            }
        )
    duplicate_groups.sort(key=lambda group: (-_safe_int(group.get("count"), 0), str(group.get("key") or "")))

    duplicate_row_count = sum(_safe_int(group.get("count"), 0) for group in duplicate_groups)
    return {
        "scanned_count": scanned,
        "skipped_count": skipped,
        "unique_key_count": len(groups_by_key),
        "duplicate_group_count": len(duplicate_groups),
        "duplicate_row_count": duplicate_row_count,
        "extra_duplicate_count": max(0, duplicate_row_count - len(duplicate_groups)),
        "groups": duplicate_groups,
    }


async def load_learn_list_url_duplicate_groups(
    *,
    db_name: str,
    chat_bot_id: str,
    limit: Optional[Any] = None,
    created_at_start: Optional[Any] = None,
    created_at_end: Optional[Any] = None,
) -> Dict[str, Any]:
    db_name = str(db_name or "").strip()
    chat_bot_id = str(chat_bot_id or "").strip()
    if not db_name:
        raise ValueError("db_name is required")
    if not chat_bot_id:
        raise ValueError("chat_bot_id is required")

    table_name = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    table_name = str(table_name or "").strip()
    if not table_name or not _SAFE_TABLE_RE.fullmatch(table_name):
        raise ValueError("learn_list table could not be resolved")

    columns = await _get_table_columns_lower(db_name, table_name)
    if "content" not in columns:
        raise ValueError(f"{table_name} has no content column")
    if "content_type" not in columns:
        raise ValueError(f"{table_name} has no content_type column")

    created_at_start_norm = _normalize_created_at_bound(created_at_start, is_end=False)
    created_at_end_norm = _normalize_created_at_bound(created_at_end, is_end=True)
    if (created_at_start_norm or created_at_end_norm) and "created_at" not in columns:
        raise ValueError(f"{table_name} has no created_at column")

    wanted = [
        "id",
        "content",
        "created_at",
    ]
    select_cols = [f"`{col}`" for col in wanted if col in columns]
    order_cols = ["`content` ASC"]
    if "id" in columns:
        order_cols.append("`id` ASC")
    row_limit = _normalize_limit(limit)
    where_parts = [
        "LOWER(COALESCE(`content_type`, '')) = 'url'",
        "COALESCE(`content`, '') <> ''",
    ]
    params: List[Any] = []
    if created_at_start_norm:
        where_parts.append("`created_at` >= %s")
        params.append(created_at_start_norm)
    if created_at_end_norm:
        where_parts.append("`created_at` <= %s")
        params.append(created_at_end_norm)
    params.append(row_limit)
    rows = await mysql_execute_query(
        f"""
        SELECT {', '.join(select_cols)}
        FROM `{table_name}`
        WHERE {' AND '.join(where_parts)}
        ORDER BY {', '.join(order_cols)}
        LIMIT %s
        """,
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    result = group_learn_list_url_duplicates(rows or [])
    result.update(
        {
            "status": "success",
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "table": table_name,
            "limit": row_limit,
            "loaded_count": len(rows or []),
            "truncated": len(rows or []) >= row_limit,
            "created_at_filter": {
                "enabled": bool(created_at_start_norm or created_at_end_norm),
                "start": created_at_start_norm,
                "end": created_at_end_norm,
            },
        }
    )
    logger.info(
        "[LearnListDuplicateGroups] grouped | db=%s table=%s loaded=%s groups=%s duplicate_rows=%s",
        db_name,
        table_name,
        result["loaded_count"],
        result["duplicate_group_count"],
        result["duplicate_row_count"],
    )
    return result
