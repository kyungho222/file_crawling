from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from backend.pattern_builder_core import (
    DEFAULT_EXPLORATION_TABLE_NAME,
    DEFAULT_FILTER_CONDITION,
    InvalidUrlError,
    PatternBuilderCore,
    UnsafeFilterConditionError,
    get_default_source_dbname,
)
from db.mysql_db_config import mysql_execute_query


logger = logging.getLogger("backend.local_pattern_builder")
router = APIRouter(prefix="/local-pattern-builder", tags=["local-pattern-builder"])
_DEFAULT_SOURCE_DBNAME = get_default_source_dbname()
_PATTERN_BUILDER_DEBUG_VERSION = "pattern-builder-debug-v3"

core = PatternBuilderCore(
    execute_query=mysql_execute_query,
    default_source_dbname=_DEFAULT_SOURCE_DBNAME,
    exploration_table_name=DEFAULT_EXPLORATION_TABLE_NAME,
    logger=logger,
)


class CollectRequest(BaseModel):
    db_name: str = _DEFAULT_SOURCE_DBNAME
    seed_url: str
    top_domain: Optional[str] = None
    chat_bot_id: Optional[str] = None
    filter_condition: str = DEFAULT_FILTER_CONDITION
    scope_hosts: List[str] = Field(default_factory=list)
    path_prefix: Optional[str] = None
    include_db_urls: bool = True
    include_live_urls: bool = True
    max_db_urls: Optional[int] = Field(default=None, ge=50)
    max_live_pages: int = Field(default=6, ge=1, le=20)
    known_rules: List[Dict[str, Any]] = Field(default_factory=list)


class AnalyzeRequest(BaseModel):
    urls: List[Any] = Field(default_factory=list)
    min_group_size: int = Field(default=2, ge=1, le=50)
    known_rules: List[Dict[str, Any]] = Field(default_factory=list)


def _static_file_path() -> Path:
    return Path(__file__).resolve().parent / "static" / "pattern_builder.html"


def _raise_http_for_core_error(exc: Exception) -> None:
    if isinstance(exc, UnsafeFilterConditionError):
        raise HTTPException(status_code=400, detail="unsafe_filter_condition") from exc
    if isinstance(exc, InvalidUrlError):
        raise HTTPException(status_code=400, detail="invalid_url") from exc
    raise exc


@router.get("", response_class=HTMLResponse)
async def pattern_builder_page() -> HTMLResponse:
    page_path = _static_file_path()
    if not page_path.exists():
        raise HTTPException(status_code=404, detail="pattern_builder_html_missing")
    return HTMLResponse(page_path.read_text(encoding="utf-8"))


@router.get("/api/seeds")
async def pattern_builder_seeds(
    db_name: str = Query(default=_DEFAULT_SOURCE_DBNAME),
    filter_condition: str = Query(default=DEFAULT_FILTER_CONDITION),
    search: str = Query(default=""),
    limit: int = Query(default=200, ge=10, le=1000),
) -> JSONResponse:
    try:
        source_db_name = core.normalize_db_name(db_name)
        validated_filter = core.validate_filter_condition(filter_condition)
    except Exception as exc:
        _raise_http_for_core_error(exc)

    rows: List[Dict[str, Any]] = []
    try:
        rows = await core.load_seed_rows_from_exploration(source_db_name, max(limit * 2, 200), validated_filter)
    except Exception as exc:
        logger.warning(
            "[PatternBuilder] exploration seed load failed | source_db=%s filter=%s err=%s",
            source_db_name,
            validated_filter,
            exc,
        )

    unique_rows: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        seed_url = str(row.get("seed_url") or "")
        if seed_url:
            unique_rows.setdefault(seed_url, row)

    filtered = core.filter_seed_rows(list(unique_rows.values()), search=search, limit=limit)
    return JSONResponse(
        {
            "ok": True,
            "db_name": source_db_name,
            "source_db_name": source_db_name,
            "table_name": DEFAULT_EXPLORATION_TABLE_NAME,
            "filter_condition": validated_filter,
            "count": len(filtered),
            "sources": ["exploration_pattern"],
            "items": filtered,
        }
    )


@router.post("/api/collect")
async def pattern_builder_collect(payload: CollectRequest) -> JSONResponse:
    seed_url = core.clean_url(payload.seed_url)
    if not seed_url:
        raise HTTPException(status_code=400, detail="seed_url_required")

    try:
        source_db_name = core.normalize_db_name(payload.db_name)
        validated_filter = core.validate_filter_condition(payload.filter_condition)
    except Exception as exc:
        _raise_http_for_core_error(exc)

    collected_items: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {
        "seed_url": seed_url,
        "db_name": source_db_name,
        "table_name": DEFAULT_EXPLORATION_TABLE_NAME,
        "filter_condition": validated_filter,
        "source_db_name": source_db_name,
        "debug_version": _PATTERN_BUILDER_DEBUG_VERSION,
        "module_file": str(Path(__file__).resolve()),
    }
    logger.warning(
        "[PatternBuilder][collect_entry] version=%s file=%s db=%s top_domain=%s include_db=%s include_live=%s max_db_urls=%s max_live_pages=%s",
        _PATTERN_BUILDER_DEBUG_VERSION,
        str(Path(__file__).resolve()),
        source_db_name,
        str(payload.top_domain or ""),
        bool(payload.include_db_urls),
        bool(payload.include_live_urls),
        str(payload.max_db_urls),
        str(payload.max_live_pages),
    )

    if payload.include_db_urls:
        db_items, db_debug = await core.query_exploration_urls(
            source_db_name=source_db_name,
            seed_url=seed_url,
            chat_bot_id=payload.chat_bot_id,
            limit=payload.max_db_urls,
            filter_condition=validated_filter,
            scope_hosts=payload.scope_hosts,
            path_prefix=payload.path_prefix,
            top_domain=payload.top_domain,
        )
        collected_items.extend(db_items)
        meta["db_url_count"] = len(db_items)
        meta["db_collect_debug"] = db_debug
        logger.warning(
            "[PatternBuilder][collect_db_result] db=%s top_domain=%s raw=%s unique=%s invalid=%s dup_rows=%s limit=%s mode=%s",
            source_db_name,
            str(payload.top_domain or ""),
            db_debug.get("raw_row_count"),
            db_debug.get("unique_url_count"),
            db_debug.get("invalid_url_count"),
            db_debug.get("duplicate_row_count"),
            db_debug.get("limit"),
            db_debug.get("used_mode"),
        )
        if payload.top_domain:
            meta["top_domain"] = payload.top_domain

    if payload.include_live_urls:
        live_items, live_meta = await core.collect_live_urls(seed_url, payload.max_live_pages)
        collected_items.extend(live_items)
        meta.update(
            {
                "live_url_count": len(live_items),
                "live_fetched_pages": live_meta.get("fetched_pages", 0),
            }
        )

    merged, duplicate_urls = core.merge_url_items(
        seed_url=seed_url,
        url_items=collected_items,
        known_rules=payload.known_rules,
    )
    source_counts: Counter[str] = Counter()
    for item in merged:
        for source in item.get("sources", []):
            source_counts[source] += 1

    return JSONResponse(
        {
            "ok": True,
            "db_name": source_db_name,
            "source_db_name": source_db_name,
            "table_name": DEFAULT_EXPLORATION_TABLE_NAME,
            "filter_condition": validated_filter,
            "seed_url": seed_url,
            "chat_bot_id": payload.chat_bot_id,
            "url_count": len(merged),
            "duplicate_url_count": len(duplicate_urls),
            "source_counts": dict(source_counts),
            "meta": meta,
            "urls": merged,
            "duplicate_urls": duplicate_urls,
        }
    )


@router.post("/api/analyze")
async def pattern_builder_analyze(payload: AnalyzeRequest) -> JSONResponse:
    result = core.analyze_urls(payload.urls, payload.min_group_size, payload.known_rules)
    return JSONResponse({"ok": True, **result})


@router.get("/api/meta")
async def pattern_builder_meta(url: str = Query(...)) -> JSONResponse:
    try:
        meta = await core.fetch_meta(url)
    except Exception as exc:
        _raise_http_for_core_error(exc)
    return JSONResponse({"ok": True, **meta})
