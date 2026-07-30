"""Duplicate lookup helpers for attachment file crawling."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set
from utils.url import canonicalize_url_for_dedup

logger = logging.getLogger("backend.file.file_duplicate")


async def has_pg_file_duplicate_by_source_url_and_subject(
    *,
    db_name: str,
    table_name: str,
    source_url: str,
    subject: str,
    job_id: Any = None,
) -> bool:
    """Return whether PG training data already has this file.

    Duplicate key: content_metadata->>'source_url' + subject + content_type=file.
    Uses SELECT 1/LIMIT 1 and does not touch LEARN_LIST.
    """
    source_url = str(source_url or "").strip()
    subject = str(subject or "").strip()
    if not (db_name and table_name and source_url and subject):
        return False
    if not re.fullmatch(r"td_[a-z0-9_]+_training_data", table_name):
        return False

    source_candidates: List[str] = []
    for candidate in (source_url, canonicalize_url_for_dedup(source_url) or ""):
        value = str(candidate or "").strip()
        if value and value not in source_candidates:
            source_candidates.append(value)
    if not source_candidates:
        return False

    try:
        from db.db_operations import execute_query as pg_execute_query

        rows = await pg_execute_query(
            f"""
            SELECT 1 AS found
            FROM public.{table_name}
            WHERE (content_metadata::jsonb ->> 'source_url') = ANY($1::text[])
              AND subject = $2
              AND LOWER(COALESCE(content_type, '')) = $3
            LIMIT 1
            """,
            (source_candidates, subject, "file"),
            fetch=True,
            dbname=str(db_name),
        )
        return bool(rows)
    except Exception as exc:
        logger.debug(
            "[Duplicate][file] PG source_url+subject exists lookup failed open | job_id=%s source=%s subject=%s err=%s",
            job_id,
            source_url[:180],
            subject[:120],
            exc,
        )
        return False


async def find_pg_file_source_url_processed_row(
    *,
    db_name: str,
    table_name: str,
    source_url: str,
    cache: Optional[Dict[str, Optional[Dict[str, Any]]]] = None,
    job_id: Any = None,
) -> Optional[Dict[str, Any]]:
    """Find a PG learned file row by board detail source_url.

    Matches are restricted to content_type=file so board/content rows cannot
    block file crawling. This helper is intentionally narrow and uses LIMIT 1.
    """
    source_url = str(source_url or "").strip()
    if not (db_name and table_name and source_url):
        return None
    if not re.fullmatch(r"td_[a-z0-9_]+_training_data", table_name):
        return None

    cache_key = canonicalize_url_for_dedup(source_url) or source_url
    if cache is not None and cache_key in cache:
        cached = cache.get(cache_key)
        return dict(cached) if isinstance(cached, dict) and cached else None

    try:
        from db.db_operations import execute_query as pg_execute_query

        col_rows = await pg_execute_query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
            """,
            (table_name,),
            fetch=True,
            dbname=str(db_name),
        )
        cols = {
            str(dict(row).get("column_name") or "").strip()
            for row in col_rows or []
            if row and dict(row).get("column_name")
        }
        if "content_metadata" not in cols or "content_type" not in cols:
            if cache is not None:
                cache[cache_key] = None
            return None

        candidates: List[str] = []
        for value in (source_url, canonicalize_url_for_dedup(source_url) or ""):
            text = str(value or "").strip()
            if text and text not in candidates:
                candidates.append(text)
        if not candidates:
            if cache is not None:
                cache[cache_key] = None
            return None

        select_cols = ["id"] if "id" in cols else []
        for col in ("content", "subject", "content_type", "content_metadata"):
            if col in cols:
                select_cols.append(col)
        if not select_cols:
            select_cols = ["content_metadata"]

        order_sql = "ORDER BY id" if "id" in cols else ""
        rows = await pg_execute_query(
            f"""
            SELECT {', '.join(select_cols)}
            FROM public.{table_name}
            WHERE (content_metadata::jsonb ->> 'source_url') = ANY($1::text[])
              AND LOWER(COALESCE(content_type, '')) = $2
            {order_sql}
            LIMIT 1
            """,
            (candidates, "file"),
            fetch=True,
            dbname=str(db_name),
        )
        row = dict(rows[0]) if rows else None
        if cache is not None:
            cache[cache_key] = row or None
        if row:
            logger.debug(
                "[FileSourceDup] PG file source_url hit | job_id=%s table=%s row_id=%s post=%s",
                job_id,
                table_name,
                row.get("id"),
                source_url[:220],
            )
        return row
    except Exception as exc:
        logger.debug(
            "[FileSourceDup] PG file source_url lookup failed open | job_id=%s db=%s post=%s err=%s",
            job_id,
            db_name,
            source_url[:220],
            exc,
        )
        if cache is not None:
            cache[cache_key] = None
        return None
