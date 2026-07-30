from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from db.db_operations import execute_query
from db.mysql_db_config import mysql_execute_query
from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from utils.url import canonicalize_url_for_dedup
from utils.whoami import get_chat_id_from_db

logger = logging.getLogger("backend.shared.duplicate_learning_metadata_postprocess")

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"
_STOP_REQUESTS: Set[str] = set()
_EXISTING_KEY_ALIASES = {
    "created_at": ("created_at", "create_at"),
    "updated_at": ("updated_at", "update_at"),
    "content_created_at": ("content_created_at", "content_create_at"),
    "content_updated_at": ("content_updated_at", "content_update_at"),
}


def _duplicate_learning_metadata_postprocess_debug_enabled() -> bool:
    return str(os.getenv("DUPLICATE_LEARNING_METADATA_POSTPROCESS_DEBUG", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def request_duplicate_learning_metadata_postprocess_stop(job_id: str) -> bool:
    key = str(job_id or "").strip()
    if not key:
        return False
    _STOP_REQUESTS.add(key)
    return True


def _is_stop_requested(job_id: str) -> bool:
    return bool(job_id and job_id in _STOP_REQUESTS)


def _clear_stop_request(job_id: str) -> None:
    if job_id:
        _STOP_REQUESTS.discard(job_id)


def _safe_table_name(value: str) -> str:
    table = str(value or "").strip()
    if not table or not _TABLE_NAME_RE.match(table):
        raise ValueError(f"invalid table name: {value!r}")
    return table


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _date_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return ""
    return text[:10] if len(text) >= 10 else text


def _parse_target_date_range(data: Dict[str, Any]) -> Tuple[str, str]:
    raw = data.get("target_date") or data.get("start_urls_target_date")
    start = ""
    end = ""
    if isinstance(raw, list) and raw:
        start = str(raw[0] or "").strip()[:10]
        end = str((raw[1] if len(raw) > 1 else raw[0]) or "").strip()[:10]
    else:
        start = str(data.get("target_date_start") or data.get("created_at_start") or "").strip()[:10]
        end = str(data.get("target_date_end") or data.get("created_at_end") or "").strip()[:10]
    if start and not end:
        end = start
    if end and not start:
        start = end
    return start, end


def _parse_metadata(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, str) and parsed.strip():
                parsed = json.loads(parsed.strip())
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _url_keys(value: Any) -> Set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    keys = {raw}
    try:
        canonical = canonicalize_url_for_dedup(raw)
        if canonical:
            keys.add(str(canonical).strip())
    except Exception:
        pass
    return {key for key in keys if key}


def _chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _metadata_patch_for_learn_row(row: Dict[str, Any]) -> Dict[str, Any]:
    content_created_at = _date_text(row.get("content_created_at"))
    content_updated_at = _date_text(row.get("content_updated_at"))

    patch: Dict[str, Any] = {}
    if content_created_at:
        patch["created_at"] = content_created_at
    if content_updated_at:
        patch["updated_at"] = content_updated_at
    patch["date_rerank_target"] = True
    patch["source_category"] = "post"
    return patch


def _metadata_recovery_patch_for_pg_row(row: Dict[str, Any]) -> Dict[str, Any]:
    patch: Dict[str, Any] = {}
    content = str(row.get("content") or "").strip()
    if content:
        patch["source_url"] = content

    chunk_num = row.get("chunk_num")
    chunk_text = str(row.get("text_data") or "")
    try:
        patch["chunk_index"] = int(str(chunk_num).strip())
    except Exception:
        if not _is_blank(chunk_num):
            patch["chunk_index"] = str(chunk_num).strip()
    if chunk_text:
        patch["content_length"] = len(chunk_text.encode("utf-8"))
    patch["update_frequency"] = "1_day"
    return patch


def _regenerate_content_metadata(pg_row: Dict[str, Any], learn_patch: Dict[str, Any]) -> Dict[str, Any]:
    regenerated: Dict[str, Any] = {}

    source_url = str(pg_row.get("content") or "").strip()
    if source_url:
        regenerated["source_url"] = source_url

    chunk_num = pg_row.get("chunk_num")
    try:
        regenerated["chunk_index"] = int(str(chunk_num).strip())
    except Exception:
        if not _is_blank(chunk_num):
            regenerated["chunk_index"] = str(chunk_num).strip()

    chunk_text = str(pg_row.get("text_data") or "")
    regenerated["content_length"] = len(chunk_text.encode("utf-8")) if chunk_text else 0
    regenerated["update_frequency"] = "1_day"

    for key in ("created_at", "updated_at"):
        value = learn_patch.get(key)
        if not _is_blank(value):
            regenerated[key] = value

    regenerated["date_rerank_target"] = True
    regenerated["source_category"] = "post"
    return regenerated


async def _load_exploration_post_url_set(
    *,
    db_name: str,
    chat_bot_id: str,
    page_size: int,
    job_id: str = "",
) -> Set[str]:
    offset = 0
    post_urls: Set[str] = set()
    while True:
        if _is_stop_requested(job_id):
            break
        rows = await mysql_execute_query(
            f"""
            SELECT DISTINCT url
            FROM `{_EXPLORATION_TABLE}`
            WHERE LOWER(COALESCE(type, '')) = 'post'
              AND url IS NOT NULL
              AND TRIM(CAST(url AS CHAR)) <> ''
            ORDER BY url ASC
            LIMIT %s OFFSET %s
            """,
            (page_size, offset),
            fetch=True,
            dbname=db_name,
        )

        if not rows:
            break
        for row in rows:
            url = str((row or {}).get("url") or "").strip()
            post_urls.update(_url_keys(url))
        offset += len(rows)
        if len(rows) < page_size:
            break
    return post_urls


async def _load_exploration_post_url_set_for_contents(
    *,
    db_name: str,
    contents: Iterable[str],
    chunk_size: int = 800,
) -> Set[str]:
    lookup_values: List[str] = []
    seen: Set[str] = set()
    for content in contents:
        for key in _url_keys(content):
            if key and key not in seen:
                seen.add(key)
                lookup_values.append(key)

    post_urls: Set[str] = set()
    for batch in _chunks(lookup_values, max(1, chunk_size)):
        placeholders = ", ".join(["%s"] * len(batch))
        rows = await mysql_execute_query(
            f"""
            SELECT url
            FROM `{_EXPLORATION_TABLE}`
            WHERE LOWER(COALESCE(type, '')) = 'post'
              AND url IN ({placeholders})
            """,
            tuple(batch),
            fetch=True,
            dbname=db_name,
        )
        for row in rows or []:
            post_urls.update(_url_keys((row or {}).get("url")))
    return post_urls


def _missing_patch(metadata: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    missing: Dict[str, Any] = {}
    for key, value in patch.items():
        aliases = _EXISTING_KEY_ALIASES.get(key, (key,))
        has_value = any((alias in metadata and not _is_blank(metadata.get(alias))) for alias in aliases)
        if not has_value:
            missing[key] = value
    return missing


async def _resolve_pg_table(db_name: str, chat_bot_id: str) -> str:
    chat_id = await get_chat_id_from_db(db_name, chat_bot_id)
    if not chat_id:
        raise RuntimeError("PostgreSQL 학습 테이블명을 찾을 수 없습니다(chat_id 없음).")
    return _safe_table_name(f"td_{chat_id}_training_data".lower())


async def _load_learn_metadata_map(
    *,
    db_name: str,
    learn_table: str,
    limit: int,
    page_size: int,
    target_date_start: str = "",
    target_date_end: str = "",
    exploration_post_urls: Optional[Set[str]] = None,
    validate_exploration_posts: bool = False,
    job_id: str = "",
) -> Tuple[Dict[str, Dict[str, Any]], int, Dict[str, int]]:
    offset = 0
    scanned = 0
    learn_by_content: Dict[str, Dict[str, Any]] = {}
    diagnostics = {
        "learn_duplicate_content": 0,
        "learn_empty_content": 0,
        "learn_not_exploration_post": 0,
        "learn_empty_patch": 0,
        "learn_selected": 0,
    }
    learn_table = _safe_table_name(learn_table)

    while scanned < limit:
        if _is_stop_requested(job_id):
            break
        current_limit = min(page_size, limit - scanned)
        where_sql = """
            content IS NOT NULL
              AND TRIM(CAST(content AS CHAR)) <> ''
              AND LOWER(COALESCE(content_type, '')) = 'url'
        """
        params: List[Any] = []
        if target_date_start and target_date_end:
            where_sql += """
              AND DATE(created_at) BETWEEN %s AND %s
            """
            params.extend([target_date_start, target_date_end])
        rows = await mysql_execute_query(
            f"""
            SELECT id, content, created_at, content_created_at, content_updated_at
            FROM `{learn_table}`
            WHERE {where_sql}
            ORDER BY id ASC
            LIMIT %s OFFSET %s
            """,
            (*params, current_limit, offset),
            fetch=True,
            dbname=db_name,
        )
        if not rows:
            break

        batch_post_urls: Optional[Set[str]] = exploration_post_urls
        if validate_exploration_posts and exploration_post_urls is None:
            batch_post_urls = await _load_exploration_post_url_set_for_contents(
                db_name=db_name,
                contents=[str((row or {}).get("content") or "").strip() for row in rows or []],
            )

        for row in rows:
            scanned += 1
            content = str((row or {}).get("content") or "").strip()
            if not content:
                diagnostics["learn_empty_content"] += 1
                continue
            if content in learn_by_content:
                diagnostics["learn_duplicate_content"] += 1
                continue
            if batch_post_urls is not None and not (_url_keys(content) & batch_post_urls):
                diagnostics["learn_not_exploration_post"] += 1
                continue
            patch = _metadata_patch_for_learn_row(dict(row or {}))
            if patch:
                learn_by_content[content] = patch
                diagnostics["learn_selected"] += 1
            else:
                diagnostics["learn_empty_patch"] += 1

        offset += len(rows)
        if len(rows) < current_limit:
            break

    return learn_by_content, scanned, diagnostics


async def _fetch_pg_rows_for_contents(
    *,
    db_name: str,
    pg_table: str,
    contents: List[str],
) -> List[Dict[str, Any]]:
    if not contents:
        return []
    placeholders = ", ".join(f"${idx}" for idx in range(1, len(contents) + 1))
    rows = await execute_query(
        f"""
        SELECT id, content, chunk_num, text_data, content_metadata
        FROM {pg_table}
        WHERE content IN ({placeholders})
        """,
        tuple(contents),
        fetch=True,
        dbname=db_name,
    )
    return [dict(row) for row in (rows or [])]


async def run_duplicate_learning_metadata_postprocess(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = str(data.get("db_name") or data.get("dbname") or "").strip()
    chat_bot_id = str(data.get("chat_bot_id") or "").strip()
    job_id = str(data.get("job_id") or data.get("jobId") or "").strip()
    if not db_name or not chat_bot_id:
        raise ValueError("db_name, chat_bot_id가 필요합니다.")

    limit = max(1, min(int(data.get("limit") or 200000), 1000000))
    page_size = max(100, min(int(data.get("page_size") or 2000), 10000))
    pg_batch_size = max(50, min(int(data.get("pg_batch_size") or 500), 2000))
    dry_run = str(data.get("dry_run") or "").strip().lower() in {"1", "true", "yes", "on"}
    target_date_start, target_date_end = _parse_target_date_range(data)
    if not (target_date_start and target_date_end):
        raise ValueError("target_date가 필요합니다. 특정 날짜 또는 날짜 범위를 지정해주세요.")

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    if not learn_table:
        raise RuntimeError("MariaDB LEARN_LIST 테이블명을 찾을 수 없습니다.")
    learn_table = _safe_table_name(learn_table)
    pg_table = await _resolve_pg_table(db_name, chat_bot_id)
    exploration_post_urls = None
    stopped = _is_stop_requested(job_id)
    if False and not exploration_post_urls and not stopped:
        raise RuntimeError("ASADAL_CRAWLING_EXPLORATION에서 type=post URL을 찾지 못했습니다.")

    learn_by_content, learn_scanned, diagnostics = await _load_learn_metadata_map(
        db_name=db_name,
        learn_table=learn_table,
        limit=limit,
        page_size=page_size,
        target_date_start=target_date_start,
        target_date_end=target_date_end,
        exploration_post_urls=exploration_post_urls,
        validate_exploration_posts=True,
        job_id=job_id,
    )
    if _is_stop_requested(job_id):
        stopped = True

    pg_scanned = 0
    update_targets = 0
    updated = 0
    skipped = 0
    pg_matched_contents: Set[str] = set()
    missing_counts: Dict[str, int] = {}
    samples: List[Dict[str, Any]] = []

    contents = list(learn_by_content.keys())
    for content_batch in _chunks(contents, pg_batch_size):
        if _is_stop_requested(job_id):
            stopped = True
            break
        pg_rows = await _fetch_pg_rows_for_contents(
            db_name=db_name,
            pg_table=pg_table,
            contents=content_batch,
        )
        pg_scanned += len(pg_rows)
        for pg_row in pg_rows:
            if _is_stop_requested(job_id):
                stopped = True
                break
            content = str(pg_row.get("content") or "").strip()
            if content:
                pg_matched_contents.add(content)
            patch = learn_by_content.get(content)
            if not patch:
                diagnostics["pg_row_without_learn_patch"] = diagnostics.get("pg_row_without_learn_patch", 0) + 1
                skipped += 1
                continue
            metadata = _parse_metadata(pg_row.get("content_metadata"))
            regenerated = _regenerate_content_metadata(pg_row, patch)
            if not regenerated:
                diagnostics["pg_empty_regenerated_metadata"] = diagnostics.get("pg_empty_regenerated_metadata", 0) + 1
                skipped += 1
                continue
            if metadata == regenerated:
                diagnostics["pg_already_same_metadata"] = diagnostics.get("pg_already_same_metadata", 0) + 1
                skipped += 1
                continue

            update_targets += 1
            changed_keys = sorted(set(metadata.keys()) | set(regenerated.keys()))
            for key in changed_keys:
                if metadata.get(key) != regenerated.get(key):
                    missing_counts[key] = missing_counts.get(key, 0) + 1
            if dry_run or len(samples) < 20:
                sample = {
                    "id": pg_row.get("id"),
                    "content": content[:240],
                    "learn_patch": patch,
                    "regenerated_keys": sorted(regenerated.keys()),
                    "changed_keys": [key for key in changed_keys if metadata.get(key) != regenerated.get(key)],
                }
                if dry_run:
                    sample["regenerated_metadata"] = regenerated
                    sample["current_metadata"] = metadata
                samples.append(sample)

            if dry_run:
                continue

            await execute_query(
                f"""
                UPDATE {pg_table}
                SET content_metadata = $1::jsonb
                WHERE id = $2
                """,
                (json.dumps(regenerated, ensure_ascii=False), pg_row.get("id")),
                fetch=False,
                dbname=db_name,
            )
            updated += 1
        if stopped:
            break

    diagnostics["pg_unmatched_learn_content"] = max(0, len(learn_by_content) - len(pg_matched_contents))

    result = {
        "status": "stopped" if stopped else "success",
        "source": "duplicate_learning_metadata_postprocess",
        "mode": "postprocess_only",
        "no_crawling": True,
        "message": (
            f"중복학습메타데이터 후보정 완료: "
            f"{updated if not dry_run else update_targets}건 "
            f"{'업데이트 대상 확인' if dry_run else '업데이트'}"
        ),
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "job_id": job_id,
        "learn_list_table": learn_table,
        "pg_table": pg_table,
        "exploration_table": _EXPLORATION_TABLE,
        "exploration_post_url_count": None,
        "learn_scanned": learn_scanned,
        "learn_content_count": len(learn_by_content),
        "pg_scanned": pg_scanned,
        "update_targets": update_targets,
        "updated": updated,
        "skipped": skipped,
        "missing_counts": missing_counts,
        "diagnostics": diagnostics,
        "samples": samples,
        "dry_run": dry_run,
        "target_date_start": target_date_start,
        "target_date_end": target_date_end,
        "stopped": stopped,
    }
    if stopped:
        result["message"] = f"중복학습메타데이터 후보정이 중단되었습니다. 업데이트 {updated}건"
    _clear_stop_request(job_id)
    logger.info("[DuplicateLearningMetadataPostprocess] %s", result)
    return result


__all__ = ["run_duplicate_learning_metadata_postprocess", "request_duplicate_learning_metadata_postprocess_stop"]
