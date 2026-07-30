from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from db.maria_operations import maria_execute_query
from tools.doc_format_dashboard.paths import dashboard_html_path


router = APIRouter(prefix="/doc-format-dashboard", tags=["doc format dashboard"])
api_router = APIRouter(prefix="/doc-format-dashboard", tags=["doc format dashboard api"])
logger = logging.getLogger("tools.doc_format_dashboard")

IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
SOURCE_COLUMN_CANDIDATES = (
    "id",
    "subject",
    "title",
    "content",
    "file_path",
    "path",
    "size",
    "file_size",
    "bytes",
    "content_type",
    "status",
    "cate1",
    "cate2",
    "memo",
    "memo1",
    "memo2",
    "created_at",
    "updated_at",
    "modify_at",
    "regdate",
    "wdate",
    "site",
    "site_name",
)


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def doc_format_dashboard_page() -> HTMLResponse:
    html_path = dashboard_html_path()
    return HTMLResponse(
        html_path.read_text(encoding="utf-8"),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


async def _json_object(request: Request) -> Dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_int(value: Any, default: int, *, minimum: int = 0, maximum: int = 50000) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def _chatbot_suffix(chat_bot_id: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]", "", str(chat_bot_id or ""))
    return compact[-12:] if len(compact) >= 12 else compact


def _quote_ident(value: str) -> str:
    if not IDENT_RE.match(value or ""):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f"`{value}`"


def _default_source_table(chat_bot_id: str) -> str:
    suffix = _chatbot_suffix(chat_bot_id)
    return f"ASADAL_{suffix}_LEARN_LIST" if suffix else "ASADAL_CRAWLING_LEARN_LIST"


def _basename(value: Any) -> str:
    raw = str(value or "").replace("\\", "/").rstrip("/")
    if not raw:
        return ""
    return raw.split("/")[-1].split("?")[0]


async def _table_columns(db_name: str, table_name: str) -> List[str]:
    rows = await maria_execute_query(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (db_name, table_name),
        fetch=True,
        dbname=db_name,
    )
    out: List[str] = []
    for row in rows or []:
        if isinstance(row, dict):
            name = row.get("COLUMN_NAME") or row.get("column_name")
            if name:
                out.append(str(name))
    return out


def _first(columns: set[str], candidates: List[str] | Tuple[str, ...]) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


async def _require_table(db_name: str, table_name: str) -> set[str]:
    columns = set(await _table_columns(db_name, table_name))
    if not columns:
        raise ValueError(f"table not found or has no readable columns: {table_name}")
    return columns


def _select_columns(columns: set[str]) -> List[str]:
    selected = [name for name in SOURCE_COLUMN_CANDIDATES if name in columns]
    if "id" not in selected and columns:
        selected.insert(0, sorted(columns)[0])
    return selected


async def _candidate_tables(db_name: str, chat_bot_id: str) -> List[Dict[str, Any]]:
    suffix = _chatbot_suffix(chat_bot_id)
    patterns = [
        f"ASADAL\\_{suffix}\\_%" if suffix else "ASADAL\\_%\\_LEARN_LIST",
        "%LEARN_LIST%",
        "%DOC%",
        "%FORM%",
        "%FORMAT%",
    ]
    where = " OR ".join(["TABLE_NAME LIKE %s"] * len(patterns))
    rows = await maria_execute_query(
        f"""
        SELECT TABLE_NAME, TABLE_ROWS
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND ({where})
        ORDER BY TABLE_NAME
        LIMIT 300
        """,
        (db_name, *patterns),
        fetch=True,
        dbname=db_name,
    )
    return list(rows or [])


async def _distinct_categories(db_name: str, source_table: str, columns: set[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {"cate1": [], "cate2": []}
    table_sql = _quote_ident(source_table)
    for col in ("cate1", "cate2"):
        if col not in columns:
            continue
        rows = await maria_execute_query(
            f"""
            SELECT DISTINCT {_quote_ident(col)} AS value
            FROM {table_sql}
            WHERE {_quote_ident(col)} IS NOT NULL AND {_quote_ident(col)} <> ''
            ORDER BY {_quote_ident(col)}
            LIMIT 300
            """,
            fetch=True,
            dbname=db_name,
        )
        out[col] = [str(row.get("value")) for row in rows or [] if isinstance(row, dict) and row.get("value")]
    return out


def _source_filters(payload: Dict[str, Any], columns: set[str]) -> Tuple[List[str], List[Any]]:
    where: List[str] = []
    params: List[Any] = []
    if "content_type" in columns:
        where.append("`content_type` = %s")
        params.append("file")
    for col in ("cate1", "cate2"):
        value = str(payload.get(col) or "").strip()
        if value and col in columns:
            where.append(f"{_quote_ident(col)} = %s")
            params.append(value)
    status = str(payload.get("status") or "all").strip()
    if status.lower() != "all" and "status" in columns:
        where.append("`status` = %s")
        params.append(status.upper())
    keyword = str(payload.get("keyword") or "").strip()
    if keyword:
        subject_col = _first(columns, ("subject", "title"))
        content_col = _first(columns, ("content", "file_path", "path"))
        likes: List[str] = []
        for col in (subject_col, content_col):
            if col:
                likes.append(f"{_quote_ident(col)} LIKE %s")
                params.append(f"%{keyword}%")
        if likes:
            where.append("(" + " OR ".join(likes) + ")")
    return where, params


@api_router.post("/api/bootstrap")
async def bootstrap(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    db_name = str(payload.get("db_name") or payload.get("db") or "").strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    if not db_name:
        return JSONResponse({"ok": False, "message": "db_name is required"}, status_code=400)

    source_table = str(payload.get("source_table") or _default_source_table(chat_bot_id)).strip()
    columns = set(await _table_columns(db_name, source_table)) if source_table else set()
    tables = await _candidate_tables(db_name, chat_bot_id)
    categories = await _distinct_categories(db_name, source_table, columns) if columns else {"cate1": [], "cate2": []}
    return JSONResponse(
        {
            "ok": True,
            "source_table": source_table,
            "source_columns": sorted(columns),
            "tables": tables,
            "categories": categories,
        }
    )


@api_router.post("/api/search")
async def search_rows(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    db_name = str(payload.get("db_name") or payload.get("db") or "").strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    source_table = str(payload.get("source_table") or _default_source_table(chat_bot_id)).strip()
    if not db_name or not source_table:
        return JSONResponse({"ok": False, "message": "db_name and source_table are required"}, status_code=400)

    columns = await _require_table(db_name, source_table)
    selected = _select_columns(columns)
    where, params = _source_filters(payload, columns)
    limit = _safe_int(payload.get("limit"), 100, minimum=1, maximum=1000)
    offset = _safe_int(payload.get("offset"), 0, minimum=0, maximum=500000)
    id_col = _first(columns, ("id", "idx", "no"))
    order_col = _first(columns, ("created_at", "modify_at", "id"))
    where_sql = " WHERE " + " AND ".join(where) if where else ""
    order_sql = f" ORDER BY {_quote_ident(order_col)} DESC" if order_col else ""
    count_rows = await maria_execute_query(
        f"SELECT COUNT(*) AS cnt FROM {_quote_ident(source_table)}{where_sql}",
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    total = int((count_rows or [{}])[0].get("cnt") or 0)
    rows = await maria_execute_query(
        f"""
        SELECT {", ".join(_quote_ident(col) for col in selected)}
        FROM {_quote_ident(source_table)}
        {where_sql}
        {order_sql}
        LIMIT %s OFFSET %s
        """,
        tuple([*params, limit, offset]),
        fetch=True,
        dbname=db_name,
    )
    return JSONResponse(
        {
            "ok": True,
            "rows": rows or [],
            "total": total,
            "limit": limit,
            "offset": offset,
            "id_column": id_col,
            "columns": selected,
        }
    )


def _map_doc_row(source: Dict[str, Any], target_columns: set[str], payload: Dict[str, Any]) -> Dict[str, Any]:
    content = source.get("content") or source.get("file_path") or source.get("path") or ""
    subject = source.get("subject") or source.get("title") or _basename(content)
    filename = _basename(content) or _basename(subject) or str(subject or "")
    site = str(payload.get("site") or "").strip() or source.get("site") or source.get("site_name") or "file"
    status = str(payload.get("target_status") or "").strip() or "N"
    candidates: Dict[str, Any] = {
        "chat_bot_id": str(payload.get("chat_bot_id") or "").strip(),
        "site": site,
        "subject": subject,
        "file_path": content,
        "full_name": filename,
        "uname": filename,
        "size": source.get("size") or source.get("file_size") or source.get("bytes") or 0,
        "status": status,
        "created_at": source.get("created_at") or source.get("regdate") or source.get("wdate"),
        "modify_at": source.get("modify_at") or source.get("updated_at") or source.get("created_at"),
        "memo1": source.get("memo1") or source.get("memo") or "",
        "memo2": source.get("memo2") or "",
        "cate1": source.get("cate1") or "",
        "cate2": source.get("cate2") or "",
        "content": content,
        "source_learn_list_id": source.get("id"),
    }
    mapped = {key: value for key, value in candidates.items() if key in target_columns}
    if "subject" not in mapped:
        raise ValueError("target table requires a subject column")
    if not ({"file_path", "content"} & set(mapped)):
        raise ValueError("target table requires file_path or content column")
    return mapped


async def _fetch_source_by_ids(
    db_name: str,
    source_table: str,
    source_columns: set[str],
    ids: List[Any],
) -> List[Dict[str, Any]]:
    id_col = _first(source_columns, ("id", "idx", "no"))
    if not id_col:
        raise ValueError("source table has no id-like column")
    selected = _select_columns(source_columns)
    placeholders = ", ".join(["%s"] * len(ids))
    rows = await maria_execute_query(
        f"""
        SELECT {", ".join(_quote_ident(col) for col in selected)}
        FROM {_quote_ident(source_table)}
        WHERE {_quote_ident(id_col)} IN ({placeholders})
        """,
        tuple(ids),
        fetch=True,
        dbname=db_name,
    )
    return list(rows or [])


async def _target_exists(db_name: str, target_table: str, target_columns: set[str], mapped: Dict[str, Any]) -> bool:
    clauses: List[str] = []
    params: List[Any] = []
    if "source_learn_list_id" in target_columns and mapped.get("source_learn_list_id") is not None:
        clauses.append("`source_learn_list_id` = %s")
        params.append(mapped.get("source_learn_list_id"))
    else:
        if "subject" in target_columns:
            clauses.append("`subject` = %s")
            params.append(mapped.get("subject"))
        path_col = "file_path" if "file_path" in target_columns else "content" if "content" in target_columns else None
        if path_col:
            clauses.append(f"{_quote_ident(path_col)} = %s")
            params.append(mapped.get(path_col))
    if not clauses:
        return False
    rows = await maria_execute_query(
        f"SELECT 1 FROM {_quote_ident(target_table)} WHERE {' AND '.join(clauses)} LIMIT 1",
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    return bool(rows)


async def _insert_target(db_name: str, target_table: str, mapped: Dict[str, Any]) -> int:
    cols = list(mapped.keys())
    return int(
        await maria_execute_query(
            f"""
            INSERT INTO {_quote_ident(target_table)}
            ({", ".join(_quote_ident(col) for col in cols)})
            VALUES ({", ".join(["%s"] * len(cols))})
            """,
            tuple(mapped[col] for col in cols),
            fetch=False,
            dbname=db_name,
        )
        or 0
    )


@api_router.post("/api/register")
async def register_rows(request: Request) -> JSONResponse:
    payload = await _json_object(request)
    db_name = str(payload.get("db_name") or payload.get("db") or "").strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    source_table = str(payload.get("source_table") or _default_source_table(chat_bot_id)).strip()
    target_table = str(payload.get("target_table") or "").strip()
    ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
    dry_run = bool(payload.get("dry_run", True))
    allow_duplicates = bool(payload.get("allow_duplicates", False))

    if not db_name or not source_table or not target_table:
        return JSONResponse({"ok": False, "message": "db_name, source_table, target_table are required"}, status_code=400)
    if not ids:
        return JSONResponse({"ok": False, "message": "ids are required"}, status_code=400)

    source_columns = await _require_table(db_name, source_table)
    target_columns = await _require_table(db_name, target_table)
    rows = await _fetch_source_by_ids(db_name, source_table, source_columns, ids)

    preview: List[Dict[str, Any]] = []
    inserted = 0
    skipped = 0
    errors: List[Dict[str, Any]] = []
    for row in rows:
        try:
            mapped = _map_doc_row(row, target_columns, payload)
            exists = False if allow_duplicates else await _target_exists(db_name, target_table, target_columns, mapped)
            preview.append(
                {
                    "source_id": row.get("id"),
                    "subject": mapped.get("subject"),
                    "file_path": mapped.get("file_path") or mapped.get("content"),
                    "site": mapped.get("site"),
                    "status": mapped.get("status"),
                    "duplicate": exists,
                }
            )
            if exists:
                skipped += 1
                continue
            if not dry_run:
                await _insert_target(db_name, target_table, mapped)
                inserted += 1
        except Exception as exc:
            errors.append({"source_id": row.get("id"), "message": str(exc)})

    return JSONResponse(
        {
            "ok": not errors,
            "dry_run": dry_run,
            "requested": len(ids),
            "matched": len(rows),
            "inserted": inserted,
            "skipped_duplicates": skipped,
            "errors": errors,
            "preview": preview,
        }
    )
