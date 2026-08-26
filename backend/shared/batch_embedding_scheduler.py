from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import zlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp

from config.settings import Config
from backend.shared.stage_url_report import append_stage_urls
from db.db_redis import get_redis
from langchain_text_splitters import RecursiveCharacterTextSplitter
from utils.hash_policy import sha256_hex_utf8

logger = logging.getLogger("backend.shared.batch_embedding_scheduler")

_BATCH_CONTEXT_PREFIX = "embedding_batch_ctx:"
_BATCH_DONE_PREFIX = "embedding_batch_done:"
_BATCH_CANCELLED_PREFIX = "embedding_batch_cancelled:"
_BATCH_SCHEDULER_RETRY_STATUSES = {429, 500, 502, 503, 504}
_MEMORY_BATCH_CONTEXTS: Dict[str, Tuple[float, str]] = {}
_MEMORY_BATCH_DONE: Dict[str, float] = {}
_MEMORY_BATCH_CANCELLED: Dict[str, float] = {}
_PENDING_EMBEDDING_CALLBACKS_BY_JOB: Dict[str, int] = {}
_PENDING_EMBEDDING_BATCH_TO_JOB: Dict[str, str] = {}


def batch_embedding_flow_debug_enabled() -> bool:
    try:
        return bool(getattr(Config, "BATCH_EMBEDDING_FLOW_DEBUG", False))
    except Exception:
        return False


def _board_mariadb_minimal_enabled() -> bool:
    return str(os.getenv("BOARD_CRAWL_MARIADB_MINIMAL", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _flow_debug(event: str, **fields: Any) -> None:
    if not batch_embedding_flow_debug_enabled():
        return
    parts: List[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).replace("\n", "\\n")
        if len(text) > 240:
            text = text[:237] + "..."
        parts.append(f"{key}={text}")
    suffix = " | " + " ".join(parts) if parts else ""
    logger.info("[BatchEmbedding][Flow] %s%s", event, suffix)




def _file_study_debug_enabled() -> bool:
    return str(os.getenv("FILE_STUDY_DEBUG", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


_FILE_STUDY_INFO_EVENTS = {
    "batch_callback_received",
    "batch_callback_pg_upsert_done",
    "callback_progress_active_log_synced",
    "callback_progress_late_study_increment",
    "callback_progress_late_sse_skipped",
}


def _file_study_debug(event: str, **fields: Any) -> None:
    if not _file_study_debug_enabled():
        return
    if event not in _FILE_STUDY_INFO_EVENTS and not batch_embedding_flow_debug_enabled():
        return
    parts: List[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).replace("\n", "\\n")
        if len(text) > 240:
            text = text[:237] + "..."
        parts.append(f"{key}={text}")
    suffix = " " + " ".join(parts) if parts else ""
    logger.info("[FileStudyDebug][%s]%s", event, suffix)



def _increment_pending_embedding_callback(job_id: str, batch_id: str) -> int:
    jid = str(job_id or "").strip()
    bid = str(batch_id or "").strip()
    if not jid or not bid:
        return 0
    _PENDING_EMBEDDING_BATCH_TO_JOB[bid] = jid
    next_count = int(_PENDING_EMBEDDING_CALLBACKS_BY_JOB.get(jid, 0) or 0) + 1
    _PENDING_EMBEDDING_CALLBACKS_BY_JOB[jid] = next_count
    _file_study_debug(
        "pending_embedding_increment",
        job_id=jid,
        batch_id=bid,
        pending=next_count,
    )
    return next_count


def _decrement_pending_embedding_callback(job_id: str = "", batch_id: str = "", reason: str = "") -> int:
    bid = str(batch_id or "").strip()
    jid = str(job_id or "").strip() or (str(_PENDING_EMBEDDING_BATCH_TO_JOB.get(bid) or "").strip() if bid else "")
    if not jid:
        return 0
    current = int(_PENDING_EMBEDDING_CALLBACKS_BY_JOB.get(jid, 0) or 0)
    next_count = max(0, current - 1)
    if next_count > 0:
        _PENDING_EMBEDDING_CALLBACKS_BY_JOB[jid] = next_count
    else:
        _PENDING_EMBEDDING_CALLBACKS_BY_JOB.pop(jid, None)
    if bid:
        _PENDING_EMBEDDING_BATCH_TO_JOB.pop(bid, None)
    _file_study_debug(
        "pending_embedding_decrement",
        job_id=jid,
        batch_id=bid,
        previous=current,
        pending=next_count,
        reason=reason or "callback_done",
    )
    return next_count


def get_pending_embedding_callback_count(job_id: str) -> int:
    jid = str(job_id or "").strip()
    if not jid:
        return 0
    return max(0, int(_PENDING_EMBEDDING_CALLBACKS_BY_JOB.get(jid, 0) or 0))


def has_pending_embedding_callbacks(job_id: str) -> bool:
    return get_pending_embedding_callback_count(job_id) > 0

def mark_pending_embedding_callback_done(job_id: str = "", batch_id: str = "", reason: str = "external_done") -> int:
    return _decrement_pending_embedding_callback(job_id=job_id, batch_id=batch_id, reason=reason)

def _batch_scheduler_submit_retry_attempts() -> int:
    try:
        return max(1, int(getattr(Config, "BATCH_SCHEDULER_SUBMIT_RETRY_ATTEMPTS", 3) or 3))
    except Exception:
        return 3


def _batch_scheduler_submit_retry_delay_sec() -> float:
    try:
        return max(0.0, float(getattr(Config, "BATCH_SCHEDULER_SUBMIT_RETRY_DELAY_SEC", 1.0) or 0.0))
    except Exception:
        return 1.0


def _normalize_batch_content_type(content_type: Any) -> str:
    return "file" if str(content_type or "").strip().lower() == "file" else "board"


def resolve_batch_embedding_service_name(content_type: Any = None) -> str:
    normalized = _normalize_batch_content_type(content_type)
    if normalized == "file":
        return str(getattr(Config, "BATCH_FILE_EMBEDDING_SERVICE_NAME", "") or "").strip()
    return str(getattr(Config, "BATCH_BOARD_EMBEDDING_SERVICE_NAME", "") or "").strip()


def _is_batch_embedding_scheduler_flag_enabled(content_type: Any = None) -> bool:
    normalized = _normalize_batch_content_type(content_type)
    if normalized == "file":
        return bool(getattr(Config, "USE_FILE_BATCH_EMBEDDING_SCHEDULER", False))
    return bool(getattr(Config, "USE_BOARD_BATCH_EMBEDDING_SCHEDULER", False))


def is_batch_embedding_scheduler_enabled(content_type: Any = None) -> bool:
    try:
        enabled = _is_batch_embedding_scheduler_flag_enabled(content_type)
        base_url = str(getattr(Config, "BATCH_SCHEDULER_BASE_URL", "") or "").strip()
        service_name = resolve_batch_embedding_service_name(content_type)
        active = enabled and bool(base_url) and bool(service_name)
        _flow_debug(
            "scheduler.enabled_check",
            content_type=_normalize_batch_content_type(content_type),
            flag_enabled=enabled,
            has_base_url=bool(base_url),
            service_name=service_name,
            active=active,
        )
        return active
    except Exception:
        return False


def batch_callback_requires_auth() -> bool:
    try:
        return bool(str(getattr(Config, "BATCH_CALLBACK_TOKEN", "") or "").strip())
    except Exception:
        return False


def callback_token_matches(token: str) -> bool:
    expected = str(getattr(Config, "BATCH_CALLBACK_TOKEN", "") or "").strip()
    provided = str(token or "").strip()
    if not expected:
        return True
    return bool(provided) and provided == expected


def _resolve_batch_callback_url() -> str:
    direct = (
        os.getenv("BATCH_CALLBACK_URL")
        or os.getenv("BATCH_EMBEDDING_CALLBACK_URL")
        or os.getenv("FILECRAWLER_BATCH_CALLBACK_URL")
        or ""
    )
    direct = str(direct or "").strip()
    if direct:
        return direct
    base = (
        os.getenv("BATCH_CALLBACK_BASE_URL")
        or os.getenv("FILECRAWLER_PUBLIC_BASE_URL")
        or os.getenv("BACKEND_PUBLIC_BASE_URL")
        or ""
    )
    base = str(base or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/Ai_Pro_filecrawler/backend/filecrawler/embedding-batch/callback"


def _batch_context_ttl_sec() -> int:
    try:
        ttl = int(getattr(Config, "BATCH_CALLBACK_TTL_SEC", 7 * 24 * 3600) or (7 * 24 * 3600))
    except Exception:
        ttl = 7 * 24 * 3600
    return max(3600, min(ttl, 30 * 24 * 3600))


def _memory_batch_expire_at() -> float:
    return time.monotonic() + float(_batch_context_ttl_sec())


def _memory_batch_prune() -> None:
    now = time.monotonic()
    for store in (_MEMORY_BATCH_CONTEXTS, _MEMORY_BATCH_DONE, _MEMORY_BATCH_CANCELLED):
        try:
            for key, value in list(store.items()):
                expires_at = float(value[0] if isinstance(value, tuple) else value)
                if expires_at <= now:
                    store.pop(key, None)
        except Exception:
            pass


def _memory_store_batch_context(batch_id: str, context: Dict[str, Any]) -> None:
    try:
        _memory_batch_prune()
        _MEMORY_BATCH_CONTEXTS[str(batch_id)] = (_memory_batch_expire_at(), _serialize_context(context))
    except Exception:
        pass


def _memory_load_batch_context(batch_id: str) -> Optional[Dict[str, Any]]:
    try:
        _memory_batch_prune()
        item = _MEMORY_BATCH_CONTEXTS.get(str(batch_id))
        if not item:
            return None
        _expires_at, raw = item
        return _deserialize_context(raw)
    except Exception:
        return None


def _memory_mark_flag(store: Dict[str, float], batch_id: str) -> None:
    try:
        _memory_batch_prune()
        store[str(batch_id)] = _memory_batch_expire_at()
    except Exception:
        pass


def _memory_has_flag(store: Dict[str, float], batch_id: str) -> bool:
    try:
        _memory_batch_prune()
        return str(batch_id) in store
    except Exception:
        return False


def _as_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on", "y")


def _serialize_context(data: Dict[str, Any]) -> str:
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(raw, level=6)
    return base64.b64encode(compressed).decode("ascii")


def _deserialize_context(encoded: str) -> Dict[str, Any]:
    compressed = base64.b64decode((encoded or "").encode("ascii"))
    raw = zlib.decompress(compressed)
    loaded = json.loads(raw.decode("utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("Decoded batch context is not a dict")
    return loaded


def _extract_content_type(result: Dict[str, Any]) -> str:
    return "file" if str(result.get("content_type") or "").strip().lower() == "file" else "url"


def _build_text_chunks(*, title: str, content: str) -> List[str]:
    all_text = f"Title: {title}\n\nContent: {content}"
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=Config.BASIC_CHUNK_SIZE,
        chunk_overlap=Config.BASIC_CHUNK_OVERLAP,
    )
    try:
        from edu.semantic_chunking import split_text_semantically_if_markdown

        chunks = split_text_semantically_if_markdown(
            all_text,
            chunk_size=Config.BASIC_CHUNK_SIZE,
            chunk_overlap=Config.BASIC_CHUNK_OVERLAP,
        )
    except Exception:
        chunks = splitter.split_text(all_text)
    return [str(chunk or "") for chunk in chunks if str(chunk or "").strip()]


def _build_row_skeletons(
    *,
    result: Dict[str, Any],
    subject: str,
    memo: str,
    table_name: str,
    post_reg_date: Optional[Any] = None,
) -> Tuple[List[List[Dict[str, str]]], List[Dict[str, Any]]]:
    source_url = str(result.get("source_url") or result.get("source") or "").strip()
    title = str(result.get("title") or result.get("subject") or "").strip()
    subject_value = str(subject or "").strip() or title or source_url
    web_title = str(result.get("web_title") or "").strip()
    content = str(result.get("content") or "").strip()
    content_type = _extract_content_type(result)
    file_size_value = _resolve_result_size_bytes(result) if content_type == "file" else 0
    file_info = result.get("file_info") if isinstance(result.get("file_info"), dict) else {}
    original_meta = file_info.get("original_meta") if isinstance(file_info.get("original_meta"), dict) else {}
    content_author = (
        result.get("content_author")
        or result.get("author")
        or result.get("department")
        or file_info.get("content_author")
        or file_info.get("author")
        or original_meta.get("content_author")
        or original_meta.get("author")
        or file_info.get("department")
        or original_meta.get("department")
    )
    content_created_at = (
        result.get("content_created_at")
        or result.get("reg_date")
        or result.get("created_at")
        or post_reg_date
    )
    content_updated_at = (
        result.get("content_updated_at")
        or result.get("updated_at")
        or content_created_at
    )
    if content_type == "file" and content_updated_at in (None, ""):
        content_updated_at = content_created_at

    content_hash = sha256_hex_utf8(" ".join(content.split())) if content else None
    chunks = _build_text_chunks(title=title, content=content)
    messages: List[List[Dict[str, str]]] = []
    rows: List[Dict[str, Any]] = []

    for idx, chunk in enumerate(chunks, start=1):
        text_data = (
            f"[Source: {source_url}]\n"
            f"[Chunk_number: {idx}]\n"
            f"[User_memo: {memo}]\n"
            f"[Title: {title}]\n{chunk}"
        )
        if content_type == "file":
            chunk_metadata = dict(original_meta or {})
            detail_source_url = str(
                chunk_metadata.get("source_url")
                or chunk_metadata.get("source_page")
                or chunk_metadata.get("post_url")
                or chunk_metadata.get("board_url")
                or source_url
                or ""
            ).strip()
            if detail_source_url:
                chunk_metadata["source_url"] = detail_source_url
                chunk_metadata.setdefault("source_page", detail_source_url)
            chunk_metadata["chunk_index"] = idx
            chunk_metadata["content_length"] = len(text_data.encode("utf-8"))
            chunk_metadata.setdefault("update_frequency", "1_day")
            chunk_metadata.setdefault("date_rerank_target", True)
            chunk_metadata.setdefault("source_category", "file")
            if file_size_value > 0:
                chunk_metadata.setdefault("file_size", file_size_value)
                chunk_metadata.setdefault("content_size_bytes", file_size_value)
            if content_hash:
                chunk_metadata.setdefault("content_hash", content_hash)
            simhash_decimal = str(
                result.get("simhash_decimal")
                or (original_meta or {}).get("simhash_decimal")
                or ""
            ).strip()
            simhash_normalized_length = str(
                result.get("simhash_normalized_length")
                or (original_meta or {}).get("simhash_normalized_length")
                or ""
            ).strip()
            if simhash_decimal and simhash_normalized_length:
                chunk_metadata.setdefault("simhash_decimal", simhash_decimal)
                chunk_metadata.setdefault("simhash_normalized_length", simhash_normalized_length)
            if content_created_at not in (None, ""):
                chunk_metadata.setdefault("content_created_at", str(content_created_at))
                chunk_metadata.setdefault("created_at", str(content_created_at))
            if content_updated_at not in (None, ""):
                chunk_metadata.setdefault("content_updated_at", str(content_updated_at))
                chunk_metadata.setdefault("updated_at", str(content_updated_at))
            if content_author not in (None, ""):
                chunk_metadata.setdefault("content_author", str(content_author))
        else:
            chunk_metadata = {
                "source_url": source_url,
                "chunk_index": idx,
                "content_length": len(text_data.encode("utf-8")),
                "update_frequency": "1_day",
            }
            if content_hash:
                chunk_metadata["content_hash"] = content_hash
            if content_created_at not in (None, ""):
                chunk_metadata["content_created_at"] = str(content_created_at)
                chunk_metadata["created_at"] = str(content_created_at)
            if content_updated_at not in (None, ""):
                chunk_metadata["content_updated_at"] = str(content_updated_at)
                chunk_metadata["updated_at"] = str(content_updated_at)
            ordered_chunk_metadata = {
                "source_url": chunk_metadata.get("source_url"),
                "chunk_index": chunk_metadata.get("chunk_index"),
                "content_length": chunk_metadata.get("content_length"),
                "update_frequency": chunk_metadata.get("update_frequency"),
            }
            if content_hash:
                ordered_chunk_metadata["content_hash"] = content_hash
            for metadata_key in ("created_at", "updated_at", "content_created_at", "content_updated_at"):
                metadata_value = chunk_metadata.get(metadata_key)
                if metadata_value not in (None, ""):
                    ordered_chunk_metadata[metadata_key] = metadata_value
            ordered_chunk_metadata["date_rerank_target"] = True
            ordered_chunk_metadata["source_category"] = "post"
            chunk_metadata = ordered_chunk_metadata
        row = {
            "content": source_url,
            "chunk_num": str(idx),
            "memo": memo,
            "content_type": content_type,
            "subject": subject_value,
            "text_data": text_data,
            "web_title": web_title or title,
            "content_metadata": json.dumps(chunk_metadata, ensure_ascii=False),
            "_table_name": table_name,
        }
        if content_author not in (None, ""):
            row["content_author"] = str(content_author)
        rows.append(row)
        messages.append([{"input": text_data}])
    return messages, rows


def _resolve_result_size_bytes(result: Dict[str, Any]) -> int:
    for key in ("size", "content_size_bytes", "file_size", "filesize"):
        try:
            value = result.get(key)
        except Exception:
            value = None
        if value in (None, ""):
            continue
        try:
            return max(0, int(value or 0))
        except Exception:
            try:
                return max(0, int(float(str(value).strip() or "0")))
            except Exception:
                continue
    try:
        return len(str(result.get("content") or "").encode("utf-8"))
    except Exception:
        return len(str(result.get("content") or ""))


def _extract_batch_id_from_submit_response(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        for key in ("batch_id", "id"):
            value = payload.get(key)
            if value:
                return str(value)
        data = payload.get("data")
        if isinstance(data, dict):
            nested = _extract_batch_id_from_submit_response(data)
            if nested:
                return nested
    return None


def _extract_callback_batch_id(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        for key in ("batch_id", "id"):
            value = payload.get(key)
            if value:
                return str(value)
        data = payload.get("data")
        if isinstance(data, dict):
            nested = _extract_callback_batch_id(data)
            if nested:
                return nested
        meta = payload.get("metadata")
        if isinstance(meta, dict):
            value = meta.get("batch_id") or meta.get("id")
            if value:
                return str(value)
    return None


def _extract_results_from_callback_payload(payload: Any) -> List[Any]:
    if isinstance(payload, dict):
        for key in ("results",):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        for key in ("data", "result", "output"):
            value = payload.get(key)
            if isinstance(value, dict):
                nested = _extract_results_from_callback_payload(value)
                if nested:
                    return nested
    return []


def _coerce_embedding_vector(value: Any) -> Optional[List[float]]:
    if isinstance(value, list):
        try:
            return [float(v) for v in value]
        except Exception:
            return None
    if isinstance(value, str):
        text = str(value or "").strip()
        if not text:
            return None
        try:
            decoded = json.loads(text)
        except Exception:
            return None
        if isinstance(decoded, list):
            try:
                return [float(v) for v in decoded]
            except Exception:
                return None
    return None


def _extract_embedding_vector(item: Any) -> Optional[List[float]]:
    direct = _coerce_embedding_vector(item)
    if direct:
        return direct
    if not isinstance(item, dict):
        return None

    for key in ("content", "embedding", "vector"):
        value = item.get(key)
        parsed = _coerce_embedding_vector(value)
        if parsed:
            return parsed

    response = item.get("response")
    if isinstance(response, dict):
        body = response.get("body")
        if isinstance(body, dict):
            data = body.get("data")
            if isinstance(data, list) and data:
                embedding = (data[0] or {}).get("embedding")
                parsed = _coerce_embedding_vector(embedding)
                if parsed:
                    return parsed
    return None


def _build_presave_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        next_row = dict(row)
        next_row.pop("_table_name", None)
        next_row.pop("embedding", None)
        prepared.append(next_row)
    return prepared


def _batch_presave_chunk_size() -> int:
    try:
        value = int((os.getenv("BATCH_EMBEDDING_PRESAVE_CHUNK_SIZE") or "50").strip())
    except Exception:
        value = 50
    return max(1, min(value, 500))


def _batch_presave_timeout_sec() -> float:
    try:
        value = float((os.getenv("BATCH_EMBEDDING_PRESAVE_TIMEOUT_SEC") or "300").strip())
    except Exception:
        value = 300.0
    return max(1.0, value)


async def _presave_batch_rows(
    *,
    table_name: str,
    db_name: str,
    job_id: str,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not table_name or not db_name or not rows:
        return []

    from edu.url_edu import fallback_individual_upsert
    from backend.shared.db_write_queue import run_db_write

    presave_rows = _build_presave_rows(rows)
    chunk_size = _batch_presave_chunk_size()
    timeout_sec = _batch_presave_timeout_sec()
    saved_records: List[Dict[str, Any]] = []
    total_chunks = (len(presave_rows) + chunk_size - 1) // chunk_size

    for chunk_index in range(total_chunks):
        start = chunk_index * chunk_size
        chunk_rows = presave_rows[start:start + chunk_size]
        label = f"batch_embedding.presave_rows.{chunk_index + 1}/{total_chunks}"
        chunk_records = await run_db_write(
            label,
            lambda chunk_rows=chunk_rows: fallback_individual_upsert(
                table_name,
                chunk_rows,
                db_name,
                job_manager=None,
                job_id=job_id,
            ),
            timeout_sec=timeout_sec,
        )
        saved_records.extend(chunk_records or [])
        _flow_debug(
            "submit.pg_presave_chunk_done",
            job_id=job_id,
            db_name=db_name,
            table_name=table_name,
            chunk=f"{chunk_index + 1}/{total_chunks}",
            saved_count=len(chunk_records or []),
            row_count=len(chunk_rows),
        )

    _flow_debug(
        "submit.pg_presave_done",
        job_id=job_id,
        db_name=db_name,
        table_name=table_name,
        saved_count=len(saved_records),
        row_count=len(presave_rows),
        chunk_size=chunk_size,
        chunks=total_chunks,
    )
    return saved_records

async def _store_batch_context(batch_id: str, context: Dict[str, Any]) -> None:
    _memory_store_batch_context(batch_id, context)
    try:
        redis = await get_redis()
        key = f"{_BATCH_CONTEXT_PREFIX}{batch_id}"
        await redis.set(key, _serialize_context(context), ex=_batch_context_ttl_sec())
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis context store failed; memory fallback active | batch_id=%s err=%s", batch_id, exc)
    _flow_debug(
        "context.stored",
        batch_id=batch_id,
        job_id=context.get("job_id"),
        db_name=context.get("db_name"),
        content_type=context.get("content_type"),
        row_count=len(list(context.get("rows") or [])),
        learn_list_id=context.get("learn_list_id"),
    )


async def _load_batch_context(batch_id: str) -> Optional[Dict[str, Any]]:
    raw = None
    try:
        redis = await get_redis()
        raw = await redis.get(f"{_BATCH_CONTEXT_PREFIX}{batch_id}")
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis context load failed; trying memory fallback | batch_id=%s err=%s", batch_id, exc)
        context = _memory_load_batch_context(batch_id)
        if context:
            _flow_debug(
                "context.loaded_memory_fallback",
                batch_id=batch_id,
                job_id=context.get("job_id"),
                db_name=context.get("db_name"),
                content_type=context.get("content_type"),
                row_count=len(list(context.get("rows") or [])),
            )
            return context
    if not raw:
        context = _memory_load_batch_context(batch_id)
        if context:
            _flow_debug(
                "context.loaded_memory_fallback",
                batch_id=batch_id,
                job_id=context.get("job_id"),
                db_name=context.get("db_name"),
                content_type=context.get("content_type"),
                row_count=len(list(context.get("rows") or [])),
            )
            return context
        _flow_debug("context.missing", batch_id=batch_id)
        return None
    try:
        context = _deserialize_context(raw)
        _flow_debug(
            "context.loaded",
            batch_id=batch_id,
            job_id=context.get("job_id"),
            db_name=context.get("db_name"),
            content_type=context.get("content_type"),
            row_count=len(list(context.get("rows") or [])),
        )
        return context
    except Exception as exc:
        logger.error("[BatchEmbedding] failed to decode batch context | batch_id=%s err=%s", batch_id, exc)
        return None


async def _mark_batch_done(batch_id: str) -> None:
    _memory_mark_flag(_MEMORY_BATCH_DONE, batch_id)
    try:
        redis = await get_redis()
        await redis.set(f"{_BATCH_DONE_PREFIX}{batch_id}", "1", ex=_batch_context_ttl_sec())
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis mark done failed; memory flag active | batch_id=%s err=%s", batch_id, exc)
    _flow_debug("context.mark_done", batch_id=batch_id)


async def _is_batch_done(batch_id: str) -> bool:
    if _memory_has_flag(_MEMORY_BATCH_DONE, batch_id):
        return True
    try:
        redis = await get_redis()
        return bool(await redis.get(f"{_BATCH_DONE_PREFIX}{batch_id}"))
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis done check failed; continuing callback | batch_id=%s err=%s", batch_id, exc)
        return False


async def _mark_batch_cancelled(batch_id: str) -> None:
    _memory_mark_flag(_MEMORY_BATCH_CANCELLED, batch_id)
    try:
        redis = await get_redis()
        await redis.set(f"{_BATCH_CANCELLED_PREFIX}{batch_id}", "1", ex=_batch_context_ttl_sec())
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis mark cancelled failed; memory flag active | batch_id=%s err=%s", batch_id, exc)
    _flow_debug("context.mark_cancelled", batch_id=batch_id)


async def _is_batch_cancelled(batch_id: str) -> bool:
    if _memory_has_flag(_MEMORY_BATCH_CANCELLED, batch_id):
        return True
    try:
        redis = await get_redis()
        return bool(await redis.get(f"{_BATCH_CANCELLED_PREFIX}{batch_id}"))
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis cancelled check failed; continuing callback | batch_id=%s err=%s", batch_id, exc)
        return False


async def _delete_batch_context(batch_id: str) -> None:
    _MEMORY_BATCH_CONTEXTS.pop(str(batch_id), None)
    try:
        redis = await get_redis()
        await redis.delete(f"{_BATCH_CONTEXT_PREFIX}{batch_id}")
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis context delete failed | batch_id=%s err=%s", batch_id, exc)
    _flow_debug("context.deleted", batch_id=batch_id)


async def _delete_batch_tracking(batch_id: str) -> Dict[str, bool]:
    memory_context_deleted = str(batch_id) in _MEMORY_BATCH_CONTEXTS
    memory_done_deleted = str(batch_id) in _MEMORY_BATCH_DONE
    _MEMORY_BATCH_CONTEXTS.pop(str(batch_id), None)
    _MEMORY_BATCH_DONE.pop(str(batch_id), None)
    _MEMORY_BATCH_CANCELLED.pop(str(batch_id), None)
    context_key = f"{_BATCH_CONTEXT_PREFIX}{batch_id}"
    done_key = f"{_BATCH_DONE_PREFIX}{batch_id}"
    context_deleted = memory_context_deleted
    done_deleted = memory_done_deleted
    try:
        redis = await get_redis()
        context_deleted = bool(await redis.delete(context_key)) or context_deleted
        done_deleted = bool(await redis.delete(done_key)) or done_deleted
    except Exception as exc:
        logger.warning("[BatchEmbedding] Redis tracking delete failed; memory tracking deleted | batch_id=%s err=%s", batch_id, exc)
    _flow_debug(
        "context.tracking_deleted",
        batch_id=batch_id,
        context_deleted=context_deleted,
        done_deleted=done_deleted,
    )
    return {
        "context_deleted": context_deleted,
        "done_deleted": done_deleted,
    }


async def cancel_embedding_batch(batch_id: str) -> Dict[str, Any]:
    resolved_batch_id = str(batch_id or "").strip()
    if not resolved_batch_id:
        raise ValueError("batch_id is required")

    pending_context = await _load_batch_context(resolved_batch_id)
    already_done = await _is_batch_done(resolved_batch_id)
    had_local_state = bool(pending_context) or already_done

    if already_done:
        _flow_debug(
            "cancel.skipped_completed",
            batch_id=resolved_batch_id,
            had_local_state=had_local_state,
        )
        return {
            "status": "already_completed",
            "batch_id": resolved_batch_id,
            "scheduler_status": None,
            "had_local_state": had_local_state,
            "context_deleted": False,
            "done_deleted": False,
        }

    if not pending_context:
        _flow_debug(
            "cancel.skipped_not_pending",
            batch_id=resolved_batch_id,
            had_local_state=had_local_state,
        )
        return {
            "status": "not_found",
            "batch_id": resolved_batch_id,
            "scheduler_status": None,
            "had_local_state": had_local_state,
            "context_deleted": False,
            "done_deleted": False,
        }

    scheduler_base = str(getattr(Config, "BATCH_SCHEDULER_BASE_URL", "") or "").strip().rstrip("/")
    if not scheduler_base:
        raise ValueError("Batch scheduler configuration is incomplete")

    token = str(getattr(Config, "BATCH_SCHEDULER_API_TOKEN", "") or "").strip()
    headers: Dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response_status: Optional[int] = None
    response_body = ""
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.delete(
            f"{scheduler_base}/batches/{quote(resolved_batch_id, safe='')}",
            headers=headers,
        ) as response:
            response_status = int(response.status)
            response_body = (await response.text()).strip()
            if response.status >= 400 and response.status != 404:
                logger.error(
                    "[BatchEmbedding] cancel failed | batch_id=%s status=%s body=%s",
                    resolved_batch_id,
                    response.status,
                    response_body[:2000],
                )
                response.raise_for_status()

    await _mark_batch_cancelled(resolved_batch_id)
    deleted_flags = await _delete_batch_tracking(resolved_batch_id)

    status = "deleted"

    _flow_debug(
        "cancel.completed",
        batch_id=resolved_batch_id,
        status=status,
        scheduler_status=response_status,
        had_local_state=had_local_state,
        context_deleted=deleted_flags.get("context_deleted"),
        done_deleted=deleted_flags.get("done_deleted"),
    )
    return {
        "status": status,
        "batch_id": resolved_batch_id,
        "scheduler_status": response_status,
        "had_local_state": had_local_state,
        **deleted_flags,
    }


async def _sync_workflow_progress_after_callback(
    *,
    job_id: str,
    db_name: str,
    source_url: str,
    learn_list_id: Optional[int],
    content_type: str,
    learning_ok: bool,
    source_event: str = "batch_embedding_callback",
) -> None:
    if not job_id or not db_name:
        _file_study_debug(
            "callback_progress_sync_skip",
            reason="missing_job_or_db",
            job_id=job_id,
            db=db_name,
            source_url=source_url,
            learn_list_id=learn_list_id,
            learning_ok=learning_ok,
        )
        return

    sync_started = time.perf_counter()
    _file_study_debug(
        "callback_progress_sync_start",
        job_id=job_id,
        db=db_name,
        source_url=source_url,
        learn_list_id=learn_list_id,
        content_type=content_type,
        learning_ok=learning_ok,
        source_event=source_event,
    )

    payload: Optional[Dict[str, Any]] = None
    workflow = None
    registry_size: Optional[int] = None
    registry_keys: List[str] = []
    history_status = ""
    history_detail = ""
    history_timestamp = ""
    workflow_tasks_active: List[str] = []
    workflow_tasks_done: List[str] = []
    active_worker_tasks: List[str] = []
    admitted_jobs: List[str] = []
    waiting_jobs: List[str] = []
    try:
        from backend.shared.crawler_state import crawler_state

        workflow = crawler_state.workflows.get(job_id)
        registry_keys = sorted(str(k) for k in list(crawler_state.workflows.keys()))
        registry_size = len(registry_keys)
        history = crawler_state.job_history.get(job_id) or {}
        history_status = str(history.get("status") or "")
        history_detail = str(history.get("detail") or "")
        history_timestamp = str(history.get("timestamp") or "")
        try:
            snapshot = crawler_state.get_workflow_debug_snapshot()
        except Exception:
            snapshot = {}
        workflow_tasks_active = [str(k) for k in (snapshot.get("workflow_tasks_active") or [])]
        workflow_tasks_done = [str(k) for k in (snapshot.get("workflow_tasks_done") or [])]
        active_worker_tasks = [str(k) for k in (snapshot.get("active_worker_tasks") or [])]
        admitted_jobs = [str(k) for k in (snapshot.get("admitted") or [])]
        waiting_jobs = [str(k) for k in (snapshot.get("waiting") or [])]
    except Exception as exc:
        _file_study_debug(
            "callback_progress_workflow_lookup_error",
            job_id=job_id,
            db=db_name,
            error=str(exc)[:240],
        )
        workflow = None

    terminal_history_statuses = {
        "completed",
        "ok",
        "error",
        "failed",
        "stop",
        "stopped",
        "cancelled",
        "interrupted",
        "download_stop",
        "coll_stop",
    }
    workflow_is_active = bool(workflow) and history_status.strip().lower() not in terminal_history_statuses

    _file_study_debug(
        "callback_progress_workflow_lookup",
        job_id=job_id,
        db=db_name,
        lookup_job_id=job_id,
        workflow_found=bool(workflow),
        workflow_active=workflow_is_active,
        has_mark_study_done=bool(workflow and hasattr(workflow, "_mark_study_done")),
        registry_size=registry_size,
        registry_keys=",".join(registry_keys[:20]),
        registry_keys_more=max(0, len(registry_keys) - 20),
        history_status=history_status,
        history_detail=history_detail,
        history_timestamp=history_timestamp,
        workflow_tasks_active=",".join(workflow_tasks_active[:20]),
        workflow_tasks_done=",".join(workflow_tasks_done[:20]),
        active_worker_tasks=",".join(active_worker_tasks[:20]),
        admitted_jobs=",".join(admitted_jobs[:20]),
        waiting_jobs=",".join(waiting_jobs[:20]),
        learn_list_id=learn_list_id,
        source_url=source_url,
    )

    if workflow_is_active and workflow and hasattr(workflow, "_mark_study_done"):
        try:
            count_key = None
            if hasattr(workflow, "_build_stats_counter_key"):
                try:
                    count_key = workflow._build_stats_counter_key(  # type: ignore[attr-defined]
                        url=source_url,
                        learn_list_id=learn_list_id,
                    )
                except Exception:
                    count_key = None
            mark_started = time.perf_counter()
            await workflow._mark_study_done(  # type: ignore[attr-defined]
                url=source_url,
                outcome="success" if learning_ok else "failed",
                counter_key=count_key,
            )
            _file_study_debug(
                "callback_progress_mark_study_done",
                job_id=job_id,
                db=db_name,
                source_url=source_url,
                learn_list_id=learn_list_id,
                outcome="success" if learning_ok else "failed",
                counter_key=count_key,
                elapsed_ms=int((time.perf_counter() - mark_started) * 1000),
            )
            _flow_debug(
                f"{source_event}.workflow_stats_synced",
                job_id=job_id,
                db_name=db_name,
                content_type=content_type,
                learn_list_id=learn_list_id,
                learning_ok=learning_ok,
            )
        except Exception as exc:
            logger.warning(
                "[BatchEmbedding] workflow stat sync failed | job_id=%s db=%s source=%s err=%s",
                job_id,
                db_name,
                source_url,
                exc,
            )

        try:
            if hasattr(workflow, "get_stats"):
                payload = workflow.get_stats()
        except Exception:
            payload = None
        try:
            if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                workflow.progress_callback(workflow.get_stats())
                _file_study_debug(
                    "callback_progress_callback_called",
                    job_id=job_id,
                    db=db_name,
                    payload_study=payload.get("study_count") if isinstance(payload, dict) else None,
                    payload_study_done=payload.get("study_done_count") if isinstance(payload, dict) else None,
                    payload_study_success=payload.get("study_success_count") if isinstance(payload, dict) else None,
                )
        except Exception as exc:
            logger.debug(
                "[BatchEmbedding] progress_callback failed | job_id=%s db=%s source=%s err=%s",
                job_id,
                db_name,
                source_url,
                exc,
            )

    if not workflow_is_active and learning_ok:
        # The workflow may already have sent its terminal event before an external
        # embedding callback returns. Count this callback directly in the durable
        # crawl log instead of relying on in-memory workflow stats.
        try:
            from db.crawl_db_manager import increment_crawling_log_study

            increment_started = time.perf_counter()
            incremented = await increment_crawling_log_study(job_id, dbname=db_name)
            _file_study_debug(
                "callback_progress_late_study_increment",
                job_id=job_id,
                db=db_name,
                learn_list_id=learn_list_id,
                incremented=incremented,
                elapsed_ms=int((time.perf_counter() - increment_started) * 1000),
            )
        except Exception as exc:
            logger.warning(
                "[BatchEmbedding] late callback study counter update failed | job_id=%s db=%s learn_list_id=%s err=%s",
                job_id,
                db_name,
                learn_list_id,
                exc,
            )

    if payload is None:
        payload = {}
        try:
            from db.crawl_db_manager import get_crawling_log_summary

            summary = await get_crawling_log_summary(job_id, dbname=db_name)
            payload = {
                "scan_count": summary.get("scan", 0),
                "total_count": summary.get("scan", 0),
                "collection_count": summary.get("collection", 0),
                "save_count": summary.get("save", 0),
                "study_count": summary.get("study", 0),
                "study_done_count": summary.get("study", 0),
                "study_success_count": summary.get("study", 0),
            }
            _file_study_debug(
                "callback_progress_db_summary_payload",
                job_id=job_id,
                db=db_name,
                payload_study=payload.get("study_count"),
                payload_study_done=payload.get("study_done_count"),
                payload_study_success=payload.get("study_success_count"),
            )
        except Exception as exc:
            payload = {}
            _file_study_debug(
                "callback_progress_db_summary_failed",
                job_id=job_id,
                db=db_name,
                error=str(exc)[:240],
            )

    if workflow_is_active and payload:
        # Keep the DB counter current even when the queued SSE write is delayed.
        # status=None deliberately preserves any terminal state already recorded.
        try:
            from db.crawl_db_manager import update_crawling_log_counters

            await update_crawling_log_counters(
                job_id,
                scan=payload.get("scan_count"),
                collection=payload.get("collection_count"),
                saved=payload.get("save_count"),
                study=payload.get("study_count"),
                pages=payload.get("pages"),
                colle=payload.get("colle"),
                dbname=db_name,
                force=True,
            )
            _file_study_debug(
                "callback_progress_active_log_synced",
                job_id=job_id,
                db=db_name,
                payload_study=payload.get("study_count"),
            )
        except Exception as exc:
            logger.warning(
                "[BatchEmbedding] active callback crawl log update failed | job_id=%s db=%s err=%s",
                job_id,
                db_name,
                exc,
            )

    if workflow_is_active and payload:
        try:
            from backend.shared.sse_publish_queue import enqueue_sse_message

            enqueue_sse_message(job_id, payload, db_name, source=source_event, priority=0)
            _file_study_debug(
                "callback_progress_sse_enqueued",
                job_id=job_id,
                db=db_name,
                source_event=source_event,
                payload_study=payload.get("study_count"),
                payload_study_done=payload.get("study_done_count"),
                payload_study_success=payload.get("study_success_count"),
                elapsed_ms=int((time.perf_counter() - sync_started) * 1000),
            )
            _flow_debug(
                f"{source_event}.sse_enqueued",
                job_id=job_id,
                db_name=db_name,
                payload_study=payload.get("study_count"),
                payload_study_done=payload.get("study_done_count"),
                payload_study_success=payload.get("study_success_count"),
            )
        except Exception as exc:
            logger.debug(
                "[BatchEmbedding] SSE publish failed | job_id=%s db=%s source=%s err=%s",
                job_id,
                db_name,
                source_url,
                exc,
            )
    elif payload:
        # A late callback must not enqueue a status-less SSE event: the queue turns
        # that into `start` and can overwrite a completed/stopped crawl state.
        _file_study_debug(
            "callback_progress_late_sse_skipped",
            job_id=job_id,
            db=db_name,
            history_status=history_status or "missing",
            payload_study=payload.get("study_count"),
        )


async def submit_crawled_url_embedding_batch(
    *,
    result: Dict[str, Any],
    subject: str,
    memo: str,
    context: Any,
    learn_list_id: Optional[int],
    display_name: str,
    post_reg_date: Optional[Any] = None,
    preserve_created_at: bool = False,
    mark_status_y_on_submit: bool = False,
    presave_rows: bool = True,
) -> Dict[str, Any]:
    normalized_content_type = _normalize_batch_content_type(result.get("content_type"))
    table_name = str(getattr(context, "table_name", "") or "").strip()
    db_name = str(getattr(context, "dbname", "") or "").strip()
    job_id = str(getattr(context, "job_id", "") or "").strip()
    workflow_job_id = str(getattr(context, "workflow_job_id", "") or job_id).strip()
    craw_id = str(getattr(context, "craw_id", "") or "").strip()
    chat_bot_id = str(getattr(context, "chat_bot_id", "") or "").strip()
    source_url = str(result.get("source_url") or result.get("source") or "").strip()
    parsed_text_for_summary = str(result.get("content") or "").strip()
    scheduler_base = str(getattr(Config, "BATCH_SCHEDULER_BASE_URL", "") or "").strip().rstrip("/")
    service_name = resolve_batch_embedding_service_name(normalized_content_type)
    token = str(getattr(Config, "BATCH_SCHEDULER_API_TOKEN", "") or "").strip()
    callback_url = _resolve_batch_callback_url()

    if not scheduler_base or not service_name:
        raise ValueError("Batch scheduler configuration is incomplete")
    if not chat_bot_id:
        raise ValueError("Batch scheduler requires chat_bot_id")

    submit_started = time.perf_counter()
    _file_study_debug(
        "batch_submit_start",
        job_id=job_id,
        db=db_name,
        content_type=normalized_content_type,
        learn_list_id=learn_list_id,
        source_url=source_url,
        presave_rows=presave_rows,
        mark_status_y_on_submit=mark_status_y_on_submit,
        text_chars=len(parsed_text_for_summary),
        service=service_name,
    )

    messages, rows = _build_row_skeletons(
        result=result,
        subject=subject,
        memo=memo,
        table_name=table_name,
        post_reg_date=post_reg_date,
    )
    if not rows:
        _file_study_debug(
            "batch_submit_no_chunks",
            job_id=job_id,
            db=db_name,
            learn_list_id=learn_list_id,
            source_url=source_url,
            elapsed_ms=int((time.perf_counter() - submit_started) * 1000),
        )
        return {"status": "skipped", "reason": "no_chunks", "chunks": 0}

    _file_study_debug(
        "batch_submit_rows_built",
        job_id=job_id,
        db=db_name,
        learn_list_id=learn_list_id,
        source_url=source_url,
        chunks=len(rows),
        elapsed_ms=int((time.perf_counter() - submit_started) * 1000),
    )

    presaved_records: List[Dict[str, Any]] = []
    if presave_rows:
        presave_started = time.perf_counter()
        _file_study_debug(
            "batch_submit_presave_start",
            job_id=job_id,
            db=db_name,
            table=table_name,
            chunks=len(rows),
            learn_list_id=learn_list_id,
        )
        presaved_records = await _presave_batch_rows(
            table_name=table_name,
            db_name=db_name,
            job_id=job_id,
            rows=rows,
        )
        _file_study_debug(
            "batch_submit_presave_done",
            job_id=job_id,
            db=db_name,
            table=table_name,
            presaved_count=len(presaved_records),
            elapsed_ms=int((time.perf_counter() - presave_started) * 1000),
        )
        if not presaved_records:
            raise ValueError("Batch scheduler pre-save failed")
    else:
        _file_study_debug(
            "batch_submit_presave_skipped",
            job_id=job_id,
            db=db_name,
            table=table_name,
            chunks=len(rows),
            learn_list_id=learn_list_id,
        )
        _flow_debug(
            "submit.pg_presave_skipped",
            job_id=job_id,
            db_name=db_name,
            table_name=table_name,
            row_count=len(rows),
        )

    if learn_list_id is not None:
        try:
            from db.mariadb_save_update import update_learn_list_pre_embedding_metrics

            from backend.shared.db_write_queue import run_db_write

            pre_metrics_ok = await run_db_write(
                "batch_embedding.pre_metrics_update",
                lambda: update_learn_list_pre_embedding_metrics(
                    db_name=db_name,
                    chat_bot_id=chat_bot_id,
                    db_id=str(int(learn_list_id)),
                    chunks=len(rows),
                    size_bytes=_resolve_result_size_bytes(result),
                ),
            )
            _flow_debug(
                "submit.learn_list_pre_metrics",
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                learn_list_id=learn_list_id,
                chunks=len(rows),
                size=_resolve_result_size_bytes(result),
                ok=pre_metrics_ok,
            )
        except Exception as exc:
            logger.warning(
                "[BatchEmbedding] pre-scheduler metrics update failed | db=%s chat_bot_id=%s learn_list_id=%s chunks=%s err=%s",
                db_name,
                chat_bot_id,
                learn_list_id,
                len(rows),
                exc,
            )
    submit_payload = {
        "type": "embedding",
        "service_name": service_name,
        "chat_bot_id": chat_bot_id,
        "messages": messages,
        "metadata": {
            "job_id": job_id,
            "craw_id": craw_id,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "table_name": table_name,
            "content_type": normalized_content_type,
            "source_url": source_url,
            "learn_list_id": learn_list_id,
            "chunks": len(rows),
            "callback_url": callback_url or None,
        },
    }
    if callback_url:
        submit_payload["callback_url"] = callback_url
        submit_payload["callbackUrl"] = callback_url
        submit_payload["webhook_url"] = callback_url

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    _flow_debug(
        "submit.prepare",
        job_id=job_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        content_type=normalized_content_type,
        service_name=service_name,
        scheduler_base=scheduler_base,
        chunk_count=len(rows),
        source_url=source_url,
        has_token=bool(token),
        callback_url=callback_url or "-",
    )
    if callback_url:
        logger.debug(
            "[BatchEmbedding] callback url attached | job_id=%s db=%s type=%s callback_url=%s",
            job_id,
            db_name,
            normalized_content_type,
            callback_url,
        )
    else:
        logger.warning(
            "[BatchEmbedding] callback url missing; relying on scheduler service default | job_id=%s db=%s type=%s service=%s",
            job_id,
            db_name,
            normalized_content_type,
            service_name,
        )

    retry_attempts = _batch_scheduler_submit_retry_attempts()
    retry_delay_sec = _batch_scheduler_submit_retry_delay_sec()
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(1, retry_attempts + 1):
            try:
                _file_study_debug(
                    "batch_submit_http_start",
                    job_id=job_id,
                    db=db_name,
                    learn_list_id=learn_list_id,
                    attempt=attempt,
                    retry_attempts=retry_attempts,
                    chunks=len(rows),
                    source_url=source_url,
                )
                http_started = time.perf_counter()
                async with session.post(f"{scheduler_base}/batches", json=submit_payload, headers=headers) as response:
                    if response.status >= 400:
                        error_body = await response.text()
                        retryable = response.status in _BATCH_SCHEDULER_RETRY_STATUSES
                        logger.error(
                            "[BatchEmbedding] submit failed | status=%s service=%s type=%s chat_bot_id=%s attempt=%s/%s retryable=%s body=%s",
                            response.status,
                            service_name,
                            normalized_content_type,
                            chat_bot_id,
                            attempt,
                            retry_attempts,
                            retryable,
                            error_body[:2000],
                        )
                        if retryable and attempt < retry_attempts:
                            await asyncio.sleep(retry_delay_sec * attempt)
                            continue
                        response.raise_for_status()
                    response_payload = await response.json(content_type=None)
                    _file_study_debug(
                        "batch_submit_http_done",
                        job_id=job_id,
                        db=db_name,
                        learn_list_id=learn_list_id,
                        status=response.status,
                        attempt=attempt,
                        batch_id=_extract_batch_id_from_submit_response(response_payload),
                        elapsed_ms=int((time.perf_counter() - http_started) * 1000),
                    )
                    _flow_debug(
                        "submit.response",
                        status=response.status,
                        job_id=job_id,
                        service_name=service_name,
                        response_batch_id=_extract_batch_id_from_submit_response(response_payload),
                        attempt=attempt,
                    )
                    break
            except aiohttp.ClientError:
                if attempt >= retry_attempts:
                    raise
                logger.warning(
                    "[BatchEmbedding] submit client error; retrying | service=%s type=%s chat_bot_id=%s attempt=%s/%s",
                    service_name,
                    normalized_content_type,
                    chat_bot_id,
                    attempt,
                    retry_attempts,
                    exc_info=True,
                )
                await asyncio.sleep(retry_delay_sec * attempt)

    batch_id = _extract_batch_id_from_submit_response(response_payload)
    if not batch_id:
        raise ValueError(f"Batch scheduler response missing batch_id: {response_payload}")

    resolved_learn_list_id = learn_list_id
    if resolved_learn_list_id is None and not _board_mariadb_minimal_enabled():
        resolved_learn_list_id = await _resolve_learn_list_id_from_source(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            source_url=source_url,
        )

    if resolved_learn_list_id is None:
        logger.warning(
            "[BatchEmbedding] submit could not resolve learn_list_id; callback finalize will retry lookup | db=%s chat_bot_id=%s source=%s",
            db_name,
            chat_bot_id,
            source_url,
        )

    status_y_on_submit = False
    if mark_status_y_on_submit:
        status_y_on_submit = await _mark_learn_list_status_y_on_submit(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            learn_list_id=resolved_learn_list_id,
            chunks=len(rows),
            post_reg_date=post_reg_date,
            preserve_created_at=bool(preserve_created_at),
            source_url=source_url,
        )
    else:
        _flow_debug(
            "submit.status_y_deferred",
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            learn_list_id=resolved_learn_list_id,
            chunks=len(rows),
            source_url=source_url,
        )
    summary_dispatched_on_submit = False
    if status_y_on_submit:
        logger.debug(
            "[BatchEmbedding] summarize_keywords deferred until learning completion | db=%s learn_list_id=%s content_type=%s source=%s",
            db_name,
            resolved_learn_list_id,
            normalized_content_type,
            source_url[:180],
        )

    context_store_started = time.perf_counter()
    await _store_batch_context(
        batch_id,
        {
            "batch_id": batch_id,
            "job_id": job_id,
            "workflow_job_id": workflow_job_id,
            "craw_id": craw_id,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "table_name": table_name,
            "content_type": normalized_content_type,
            "source_url": source_url,
            "rows": rows,
            "learn_list_id": resolved_learn_list_id,
            "display_name": display_name,
            "post_reg_date": post_reg_date,
            "preserve_created_at": bool(preserve_created_at),
            "status_y_on_submit": status_y_on_submit,
            "submitted_at_epoch": time.time(),
        },
    )

    _file_study_debug(
        "batch_submit_context_stored",
        job_id=job_id,
        workflow_job_id=workflow_job_id,
        db=db_name,
        learn_list_id=resolved_learn_list_id,
        batch_id=batch_id,
        chunks=len(rows),
        elapsed_ms=int((time.perf_counter() - context_store_started) * 1000),
    )
    pending_count = _increment_pending_embedding_callback(workflow_job_id or job_id, batch_id)
    _file_study_debug(
        "batch_submit_pending_registered",
        job_id=job_id,
        workflow_job_id=workflow_job_id,
        db=db_name,
        learn_list_id=resolved_learn_list_id,
        batch_id=batch_id,
        pending=pending_count,
    )
    _file_study_debug(
        "batch_submit_done",
        job_id=job_id,
        db=db_name,
        learn_list_id=resolved_learn_list_id,
        batch_id=batch_id,
        chunks=len(rows),
        presaved_count=len(presaved_records),
        status_y_on_submit=status_y_on_submit,
        total_elapsed_ms=int((time.perf_counter() - submit_started) * 1000),
    )
    logger.info(
        "[BatchEmbedding] submitted | batch_id=%s job_id=%s db=%s type=%s service=%s chunks=%s callback_url=%s source=%s",
        batch_id,
        job_id,
        db_name,
        normalized_content_type,
        service_name,
        len(rows),
        callback_url or "-",
        source_url,
    )
    submit_elapsed_sec = time.perf_counter() - submit_started
    if submit_elapsed_sec >= 3.0:
        logger.warning(
            "[FileLearnTrace][batch_submit_slow] job_id=%s batch_id=%s db=%s elapsed_sec=%.3f chunks=%s service=%s source=%s",
            job_id,
            batch_id,
            db_name,
            submit_elapsed_sec,
            len(rows),
            service_name,
            source_url[:220],
        )
    return {
        "status": "submitted",
        "batch_id": batch_id,
        "chunks": len(rows),
        "presaved_count": len(presaved_records),
        "learn_list_id": resolved_learn_list_id,
        "status_y_on_submit": status_y_on_submit,
        "summary_dispatched_on_submit": summary_dispatched_on_submit,
        "raw_response": response_payload,
    }


async def _resolve_learn_list_id_from_source(*, db_name: str, chat_bot_id: str, source_url: str) -> Optional[int]:
    if not db_name or not chat_bot_id or not source_url:
        return None
    try:
        from backend.shared.learn_list_url_row_cache import find_learn_list_row_in_url_cache
        from db.mysql_db_config import mysql_execute_query

        learn_table = f"ASADAL_{str(chat_bot_id).strip()[-12:]}_LEARN_LIST"
        cached_row = await find_learn_list_row_in_url_cache(
            db_name=str(db_name),
            table_name=learn_table,
            columns=("id", "content"),
            candidate_url=source_url,
        )
        rows = [cached_row] if cached_row else await mysql_execute_query(
            f"SELECT id FROM `{learn_table}` WHERE content_type = %s AND content = %s LIMIT 1",
            ("url", source_url),
            fetch=True,
            dbname=db_name,
        )
        if rows:
            value = (rows[0] or {}).get("id")
            if value is not None:
                return int(value)
    except Exception as exc:
        logger.warning(
            "[BatchEmbedding] learn_list_id lookup failed | db=%s chat_bot_id=%s source=%s err=%s",
            db_name,
            chat_bot_id,
            source_url,
            exc,
        )
    return None


async def _mark_learn_list_status_y_on_submit(
    *,
    db_name: str,
    chat_bot_id: str,
    learn_list_id: Optional[int],
    chunks: int,
    post_reg_date: Optional[Any],
    preserve_created_at: bool,
    source_url: str,
) -> bool:
    if learn_list_id is None or not db_name or not chat_bot_id:
        _flow_debug(
            "submit.status_y_skipped",
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            learn_list_id=learn_list_id,
            reason="missing_required_context",
        )
        return False
    try:
        from db.mariadb_save_update import update_learn_list_status_board

        from backend.shared.db_write_queue import run_db_write

        ok = await run_db_write(
            "batch_embedding.status_y_update",
            lambda: update_learn_list_status_board(
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                db_id=str(int(learn_list_id)),
                chunks=chunks,
                raw_filters_str=None,
                content_created_at=post_reg_date,
                preserve_created_at=preserve_created_at,
            ),
        )
        _flow_debug(
            "submit.status_y_update",
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            learn_list_id=learn_list_id,
            chunks=chunks,
            ok=ok,
        )
        if ok:
            logger.debug(
                "[BatchEmbedding] status=Y updated on scheduler submit | db=%s chat_bot_id=%s learn_list_id=%s chunks=%s source=%s",
                db_name,
                chat_bot_id,
                learn_list_id,
                chunks,
                source_url,
            )
        return bool(ok)
    except Exception as exc:
        logger.warning(
            "[BatchEmbedding] status=Y update on submit failed | db=%s chat_bot_id=%s learn_list_id=%s chunks=%s source=%s err=%s",
            db_name,
            chat_bot_id,
            learn_list_id,
            chunks,
            source_url,
            exc,
        )
        _flow_debug(
            "submit.status_y_update_failed",
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            learn_list_id=learn_list_id,
            chunks=chunks,
            err=exc,
        )
        return False


async def _dispatch_summarize_keywords_after_status_y_on_submit(
    *,
    chat_bot_id: str,
    db_name: str,
    learn_list_id: Any,
    source_url: str,
    content_type: str,
    batch_id: str,
) -> None:
    if not chat_bot_id or not db_name or learn_list_id is None:
        _flow_debug(
            "callback.summarize_skipped",
            batch_id=batch_id,
            db_name=db_name,
            learn_list_id=learn_list_id,
            reason="missing_context",
        )
        return
    try:
        from backend.shared.learning_service import LearningService

        _flow_debug(
            "callback.summarize_start",
            batch_id=batch_id,
            db_name=db_name,
            learn_list_id=learn_list_id,
            content_type=content_type,
            source=source_url[:180],
        )
        ls = LearningService(chat_bot_id=chat_bot_id, db_name=db_name, progress_callback=None)
        await ls._await_summarize_keywords_after_learn_steps(
            content=source_url,
            pg_content=source_url,
            content_type=content_type,
            learn_list_id=learn_list_id,
            normalized_text=None,
        )
        _flow_debug(
            "callback.summarize_done",
            batch_id=batch_id,
            db_name=db_name,
            learn_list_id=learn_list_id,
        )
    except Exception as exc:
        logger.warning(
            "[BatchEmbedding] summarize_keywords dispatch failed after status_y_on_submit | batch_id=%s db=%s learn_list_id=%s source=%s err=%s",
            batch_id,
            db_name,
            learn_list_id,
            source_url[:180],
            exc,
        )
        _flow_debug(
            "callback.summarize_failed",
            batch_id=batch_id,
            db_name=db_name,
            learn_list_id=learn_list_id,
            error=str(exc)[:300],
        )


async def process_embedding_callback(payload: Dict[str, Any]) -> Dict[str, Any]:
    batch_id = _extract_callback_batch_id(payload)
    if not batch_id:
        raise ValueError("Callback payload missing batch_id")

    callback_started = time.perf_counter()
    status = str(payload.get("status") or ((payload.get("data") or {}) if isinstance(payload.get("data"), dict) else {}).get("status") or "").strip().lower()
    _file_study_debug(
        "batch_callback_received",
        batch_id=batch_id,
        status=status or "unknown",
        top_level_keys=",".join(sorted(str(k) for k in payload.keys())),
    )
    _flow_debug(
        "callback.received",
        batch_id=batch_id,
        status=status or "unknown",
        top_level_keys=",".join(sorted(str(k) for k in payload.keys())),
    )
    if status and status != "completed":
        logger.warning("[BatchEmbedding] callback non-completed | batch_id=%s status=%s", batch_id, status)
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="non_completed")
        return {"status": status or "ignored", "batch_id": batch_id}

    if await _is_batch_done(batch_id):
        _flow_debug("callback.duplicate", batch_id=batch_id)
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="duplicate_callback")
        return {"status": "duplicate", "batch_id": batch_id}
    if await _is_batch_cancelled(batch_id):
        _flow_debug("callback.cancelled", batch_id=batch_id)
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="cancelled_callback")
        return {"status": "cancelled", "batch_id": batch_id}

    context = await _load_batch_context(batch_id)
    if not context:
        logger.warning("[BatchEmbedding] callback missing context | batch_id=%s", batch_id)
        _flow_debug("callback.missing_context", batch_id=batch_id)
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="missing_context")
        return {"status": "missing_context", "batch_id": batch_id}

    try:
        submitted_at_epoch = float(context.get("submitted_at_epoch") or 0.0)
    except (TypeError, ValueError):
        submitted_at_epoch = 0.0
    callback_lag_sec = max(0.0, time.time() - submitted_at_epoch) if submitted_at_epoch else 0.0
    if callback_lag_sec >= 10.0:
        logger.warning(
            "[FileLearnTrace][batch_callback_slow] job_id=%s batch_id=%s db=%s lag_sec=%.3f learn_list_id=%s chunks=%s",
            context.get("workflow_job_id") or context.get("job_id"),
            batch_id,
            context.get("db_name"),
            callback_lag_sec,
            context.get("learn_list_id"),
            len(context.get("rows") or []),
        )

    rows = list(context.get("rows") or [])
    results = _extract_results_from_callback_payload(payload)
    if not results:
        logger.warning("[BatchEmbedding] callback missing results | batch_id=%s", batch_id)
        _flow_debug("callback.missing_results", batch_id=batch_id)
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="missing_results")
        return {"status": "missing_results", "batch_id": batch_id}
    if len(results) < len(rows):
        logger.warning(
            "[BatchEmbedding] callback result count mismatch | batch_id=%s result_count=%s pending_rows=%s",
            batch_id,
            len(results),
            len(rows),
        )
        _flow_debug(
            "callback.result_count_mismatch",
            batch_id=batch_id,
            result_count=len(results),
            pending_rows=len(rows),
        )
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="result_count_mismatch")
        return {
            "status": "result_count_mismatch",
            "batch_id": batch_id,
            "result_count": len(results),
            "pending_rows": len(rows),
        }

    _flow_debug(
        "callback.results_loaded",
        batch_id=batch_id,
        pending_rows=len(rows),
        result_count=len(results),
    )

    merged_rows: List[Dict[str, Any]] = []
    skipped_rows = 0
    for idx, row in enumerate(rows):
        if idx >= len(results):
            break
        vector = _extract_embedding_vector(results[idx])
        if not vector:
            skipped_rows += 1
            continue
        next_row = dict(row)
        next_row.pop("_table_name", None)
        next_row["embedding"] = f"[{','.join(map(str, vector))}]"
        merged_rows.append(next_row)

    if not merged_rows:
        logger.warning("[BatchEmbedding] callback yielded no embeddings | batch_id=%s skipped_rows=%s", batch_id, skipped_rows)
        _flow_debug(
            "callback.no_embeddings",
            batch_id=batch_id,
            skipped_rows=skipped_rows,
            pending_rows=len(rows),
        )
        _decrement_pending_embedding_callback(batch_id=batch_id, reason="no_embeddings")
        return {
            "status": "no_embeddings",
            "batch_id": batch_id,
            "skipped_rows": skipped_rows,
            "pending_rows": len(rows),
        }

    _flow_debug(
        "callback.results_merged",
        batch_id=batch_id,
        merged_rows=len(merged_rows),
        skipped_rows=skipped_rows,
    )

    table_name = str(context.get("table_name") or "").strip()
    db_name = str(context.get("db_name") or "").strip()
    job_id = str(context.get("job_id") or "").strip()
    workflow_job_id = str(context.get("workflow_job_id") or job_id).strip()
    craw_id = str(context.get("craw_id") or context.get("crawling_log_id") or "").strip()
    source_url = str(context.get("source_url") or "").strip()
    chat_bot_id = str(context.get("chat_bot_id") or "").strip()
    content_type = _normalize_batch_content_type(context.get("content_type"))

    from edu.url_edu import fallback_individual_upsert

    pg_started = time.perf_counter()
    _file_study_debug(
        "batch_callback_pg_upsert_start",
        batch_id=batch_id,
        job_id=job_id,
        db=db_name,
        table=table_name,
        merged_rows=len(merged_rows),
        skipped_rows=skipped_rows,
        learn_list_id=context.get("learn_list_id"),
        source_url=source_url,
    )
    inserted_records = await fallback_individual_upsert(
        table_name,
        merged_rows,
        db_name,
        job_manager=None,
        job_id=job_id,
    )
    _file_study_debug(
        "batch_callback_pg_upsert_done",
        batch_id=batch_id,
        job_id=job_id,
        db=db_name,
        inserted_count=len(inserted_records),
        elapsed_ms=int((time.perf_counter() - pg_started) * 1000),
    )
    _flow_debug(
        "callback.pg_upsert_done",
        batch_id=batch_id,
        db_name=db_name,
        table_name=table_name,
        inserted_count=len(inserted_records),
    )
    pg_upsert_succeeded = bool(inserted_records)
    if not pg_upsert_succeeded:
        # A callback result alone is not a learning success.  Do not mark the
        # LEARN_LIST row Y or advance workflow/SSE learning counters unless at
        # least one PG chunk was actually inserted or updated.
        logger.error(
            "[BatchEmbedding][pg_upsert_empty] batch_id=%s db=%s table=%s learn_list_id=%s merged_rows=%s source_url=%s",
            batch_id,
            db_name,
            table_name,
            context.get("learn_list_id"),
            len(merged_rows),
            source_url[:220],
        )
        _file_study_debug(
            "batch_callback_pg_upsert_empty",
            batch_id=batch_id,
            job_id=job_id,
            db=db_name,
            table=table_name,
            merged_rows=len(merged_rows),
            learn_list_id=context.get("learn_list_id"),
            source_url=source_url,
        )

    learn_list_id = context.get("learn_list_id")
    if learn_list_id is None and not _board_mariadb_minimal_enabled():
        learn_list_id = await _resolve_learn_list_id_from_source(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            source_url=source_url,
        )

    status_y_on_submit = _as_bool(context.get("status_y_on_submit"))
    learning_ok = False
    if pg_upsert_succeeded and learn_list_id is not None and chat_bot_id and db_name:
        if status_y_on_submit:
            # Older contexts may say status=Y was applied at submission time.
            # Re-apply and verify it after the callback PG write so Redis cannot
            # report completion while LEARN_LIST remains N.
            from backend.shared.db_write_queue import run_db_write
            from db.mariadb_save_update import update_learn_list_status_board

            learning_ok = bool(
                await run_db_write(
                    "batch_embedding.callback_status_y_verify",
                    lambda: update_learn_list_status_board(
                        db_name=db_name,
                        chat_bot_id=chat_bot_id,
                        db_id=str(learn_list_id),
                        chunks=len(inserted_records) or len(merged_rows),
                        raw_filters_str=None,
                        content_created_at=context.get("post_reg_date"),
                        preserve_created_at=_as_bool(context.get("preserve_created_at")),
                    ),
                )
            )
            _flow_debug(
                "callback.status_y_reverified",
                batch_id=batch_id,
                db_name=db_name,
                learn_list_id=learn_list_id,
                learning_ok=learning_ok,
            )
            if learning_ok:
                await _dispatch_summarize_keywords_after_status_y_on_submit(
                    chat_bot_id=chat_bot_id,
                    db_name=db_name,
                    learn_list_id=learn_list_id,
                    source_url=source_url,
                    content_type=content_type,
                    batch_id=batch_id,
                )
        else:
            from backend.shared.learning_finalize import finalize_learning_to_mariadb

            _flow_debug(
                "callback.learning_finalize_start",
                batch_id=batch_id,
                db_name=db_name,
                learn_list_id=learn_list_id,
                actual_chunks=len(inserted_records) or len(merged_rows),
            )
            finalize_started = time.perf_counter()
            _file_study_debug(
                "batch_callback_finalize_start",
                batch_id=batch_id,
                job_id=job_id,
                db=db_name,
                learn_list_id=learn_list_id,
                actual_chunks=len(inserted_records) or len(merged_rows),
                source_url=source_url,
            )
            learning_ok = await finalize_learning_to_mariadb(
                chat_bot_id=chat_bot_id,
                db_name=db_name,
                learn_list_id=str(learn_list_id),
                display_name=str(context.get("display_name") or source_url),
                actual_chunks=len(inserted_records) or len(merged_rows),
                pg_content_value=source_url,
                learning_service=None,
                pg_wait_timeout_seconds=None,
                post_reg_date=context.get("post_reg_date"),
                preserve_created_at=_as_bool(context.get("preserve_created_at")),
                job_id_for_count=workflow_job_id or job_id,
                crawling_log_id=int(craw_id) if craw_id.isdigit() else None,
                # Batch callbacks sync workflow stats below; the SSE/Redis payload is
                # the single source used to update ASADAL_CRAWLING_LOG counters.
                increment_study_count_on_success=False,
            )
            _file_study_debug(
                "batch_callback_finalize_done",
                batch_id=batch_id,
                job_id=job_id,
                db=db_name,
                learn_list_id=learn_list_id,
                learning_ok=learning_ok,
                elapsed_ms=int((time.perf_counter() - finalize_started) * 1000),
            )
            _flow_debug(
                "callback.learning_finalize_done",
                batch_id=batch_id,
                db_name=db_name,
                learn_list_id=learn_list_id,
                learning_ok=learning_ok,
            )
        if learning_ok:
            try:
                append_stage_urls(
                    stage="study",
                    urls=[
                        {
                            "url": source_url,
                            "db_id": str(learn_list_id),
                        }
                    ],
                    job_id=job_id,
                    db_name=db_name,
                )
            except Exception as exc:
                logger.warning(
                    "[BatchEmbedding] stage study append failed | batch_id=%s db=%s source=%s err=%s",
                    batch_id,
                    db_name,
                    source_url,
                    exc,
                )
    else:
        _flow_debug(
            "callback.learning_finalize_skipped",
            batch_id=batch_id,
            db_name=db_name,
            learn_list_id=learn_list_id,
            has_chat_bot_id=bool(chat_bot_id),
            pg_upsert_succeeded=pg_upsert_succeeded,
        )

    _file_study_debug(
        "batch_callback_progress_sync_before",
        batch_id=batch_id,
        job_id=job_id,
        workflow_job_id=workflow_job_id,
        db=db_name,
        source_url=source_url,
        learn_list_id=learn_list_id,
        content_type=content_type,
        learning_ok=learning_ok,
    )
    progress_sync_started = time.perf_counter()
    await _sync_workflow_progress_after_callback(
        job_id=workflow_job_id or job_id,
        db_name=db_name,
        source_url=source_url,
        learn_list_id=int(learn_list_id) if learn_list_id is not None else None,
        content_type=content_type,
        learning_ok=learning_ok,
    )
    _file_study_debug(
        "batch_callback_progress_sync_after",
        batch_id=batch_id,
        job_id=job_id,
        workflow_job_id=workflow_job_id,
        db=db_name,
        learn_list_id=learn_list_id,
        learning_ok=learning_ok,
        elapsed_ms=int((time.perf_counter() - progress_sync_started) * 1000),
    )

    done_mark_started = time.perf_counter()
    await _mark_batch_done(batch_id)
    await _delete_batch_context(batch_id)
    _file_study_debug(
        "batch_callback_tracking_done",
        batch_id=batch_id,
        job_id=job_id,
        db=db_name,
        elapsed_ms=int((time.perf_counter() - done_mark_started) * 1000),
        total_elapsed_ms=int((time.perf_counter() - callback_started) * 1000),
    )

    _decrement_pending_embedding_callback(job_id=workflow_job_id or job_id, batch_id=batch_id, reason="callback_applied")
    logger.info(
        "[BatchEmbedding] callback applied | batch_id=%s db=%s type=%s inserted=%s learn_list_id=%s learning_ok=%s",
        batch_id,
        db_name,
        content_type,
        len(inserted_records),
        learn_list_id,
        learning_ok,
    )
    return {
        "status": "completed",
        "batch_id": batch_id,
        "inserted_count": len(inserted_records),
        "learn_list_id": learn_list_id,
        "learning_ok": learning_ok,
    }


__all__ = [
    "batch_embedding_flow_debug_enabled",
    "batch_callback_requires_auth",
    "cancel_embedding_batch",
    "callback_token_matches",
    "get_pending_embedding_callback_count",
    "has_pending_embedding_callbacks",
    "mark_pending_embedding_callback_done",
    "is_batch_embedding_scheduler_enabled",
    "process_embedding_callback",
    "resolve_batch_embedding_service_name",
    "submit_crawled_url_embedding_batch",
    "_extract_batch_id_from_submit_response",
    "_extract_callback_batch_id",
    "_extract_embedding_vector",
    "_extract_results_from_callback_payload",
    "_resolve_result_size_bytes",
]
