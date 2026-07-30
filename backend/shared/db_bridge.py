from __future__ import annotations

import asyncio
import hmac
import logging
import os
import re
from typing import Any

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from db.db_operations import execute_query as pg_execute_query
from db.maria_operations import maria_execute_query


logger = logging.getLogger("backend.shared.db_bridge")
router = APIRouter()

_READ_START_RE = re.compile(r"^(?:SELECT|WITH|EXPLAIN)\b", re.IGNORECASE)
_WRITE_TOKEN_RE = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|REPLACE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|"
    r"COPY|CALL|DO|VACUUM|ANALYZE|SET|USE|LOCK|UNLOCK|LOAD|INTO)\b",
    re.IGNORECASE,
)
_ENGINES = frozenset({"mariadb", "postgres"})


def _normalized_read_query(value: Any) -> str:
    query = str(value or "").strip()
    if query.endswith(";"):
        query = query[:-1].rstrip()
    if not query or ";" in query:
        raise ValueError("단일 SQL 문만 허용됩니다")
    if not _READ_START_RE.match(query):
        raise ValueError("SELECT, WITH, EXPLAIN 조회만 허용됩니다")
    if _WRITE_TOKEN_RE.search(query):
        raise ValueError("쓰기 또는 DDL SQL 키워드는 허용되지 않습니다")
    return query


def _bridge_token_is_valid(request: Request) -> bool:
    expected = str(os.getenv("F1_DEV_DB_BRIDGE_API_TOKEN") or "").strip()
    supplied = str(request.headers.get("X-F1-Dev-DB-Bridge-Token") or "").strip()
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


@router.post("/backend/db-bridge/query")
async def execute_read_only_db_query(request: Request) -> JSONResponse:
    """f1_dev에서만 실행되는 공용 읽기 전용 DB 브리지다."""
    try:
        if not _bridge_token_is_valid(request):
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = str(body.get("db_name") or "").strip()
        engine = str(body.get("engine") or "").strip().lower()
        query = _normalized_read_query(body.get("query"))
        params = body.get("params") or []
        if not db_name:
            return JSONResponse({"ok": False, "error": "db_name is required"}, status_code=400)
        if engine not in _ENGINES:
            return JSONResponse({"ok": False, "error": "engine must be mariadb or postgres"}, status_code=400)
        if not isinstance(params, list):
            return JSONResponse({"ok": False, "error": "params must be a JSON list"}, status_code=400)

        executor = maria_execute_query if engine == "mariadb" else pg_execute_query
        rows = await asyncio.wait_for(
            executor(query, tuple(params), fetch=True, dbname=db_name),
            timeout=30.0,
        )
        result_rows = [dict(row) for row in (rows or [])]
        return JSONResponse(jsonable_encoder({
            "ok": True,
            "db_name": db_name,
            "engine": engine,
            "returned": len(result_rows),
            "rows": result_rows,
        }))
    except asyncio.TimeoutError:
        logger.error("[DBBridge] 조회 시간초과 | db_name=%s engine=%s", locals().get("db_name", ""), locals().get("engine", ""))
        return JSONResponse({"ok": False, "error": "query_timeout"}, status_code=504)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[DBBridge] 조회 실패 | db_name=%s engine=%s error=%s", locals().get("db_name", ""), locals().get("engine", ""), exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)