"""Isolated maintenance helpers for file-crawl metadata and SimHash backfill.

These jobs intentionally do not enter the production download, MariaDB save,
PG learning, or embedding pipeline.  They only reconcile already persisted
file rows with PG training rows and use the existing attachment extractor when
the detail-page source URL has to be recovered.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import unicodedata
import uuid
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text

from backend.file.fast_attachment_producer import run_fast_file_attachment_front
from backend.file.file_download_workflow import FileDownloadWorkflow, _file_crawl_detail_fetch_timeout_sec
from backend.shared.file_crawl_post_urls import load_file_crawl_post_url_strings
from backend.shared.file_simhash_generation import FileSimhashGenerationResult, generate_file_simhash_result
from db.db_postgres import get_session_factory
from db.maria_operations import maria_execute_query
from db.mariadb_save_update import (
    get_account_identifier_from_chatbot_setup,
    resolve_learn_list_table_name_for_chatbot,
)


logger = logging.getLogger("backend.file.file_crawl_maintenance_backfill")

_PG_TABLE_PATTERN = re.compile(r"^td_[a-z0-9_]+_training_data$")
_MARIADB_TABLE_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_BRACKET_METADATA_LINE = re.compile(
    r"^\[(?:source|source_url|url|page|chunk(?:_number)?|title|user_memo|date|created_at|reg_date)\s*:\s*.*\]\s*$",
    re.IGNORECASE,
)
_URL_ONLY_LINE = re.compile(r"^(?:https?://|www\.)", re.IGNORECASE)
_DATE_ONLY_LINE = re.compile(r"^(?:\d{4}[-./년]\s*\d{1,2}[-./월]\s*\d{1,2}(?:일)?|\d{8})$")
_TARGET_CONTENT_TYPES = frozenset({"file", "post"})


EventCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]


def _metadata_scan_progress_payload(
    *,
    exploration_urls: int,
    visited_details: int,
    attachment_lists_extracted_details: int,
    attachments_found: int,
    attachments_exact_size_known: int,
    attachment_size_missing: int,
    attachment_matches: int,
    first_detail_matches: int,
    author_extracted_pages: int,
    reg_date_extracted_pages: int,
    last_source_url: str,
) -> Dict[str, Any]:
    """Build the compact, client-facing progress contract for metadata scans."""
    return {
        "exploration_urls": exploration_urls,
        "visited_details": visited_details,
        "attachment_lists_extracted_details": attachment_lists_extracted_details,
        "remaining_details": max(exploration_urls - visited_details, 0),
        "attachments_found": attachments_found,
        "attachments_exact_size_known": attachments_exact_size_known,
        "attachment_size_missing": attachment_size_missing,
        "attachment_matches": attachment_matches,
        "first_detail_matches": first_detail_matches,
        "author_extracted_pages": author_extracted_pages,
        "reg_date_extracted_pages": reg_date_extracted_pages,
        "last_source_url": last_source_url,
    }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_target_content_type(value: Any) -> str:
    content_type = _text(value).lower() or "file"
    if content_type not in _TARGET_CONTENT_TYPES:
        raise ValueError(f"unsupported_content_type:{content_type}")
    return content_type


def _filename_match_key(name: Any) -> Optional[str]:
    """Normalize a usable attachment filename for conservative filename matching."""
    raw_name = unicodedata.normalize("NFC", _text(name))
    base_name, separator, extension = raw_name.rpartition(".")
    if not separator or not base_name or not extension:
        return None
    return raw_name.casefold()


def _nas_file_candidates_for_hash_backfill(
    row: Dict[str, Any], *, db_name: str, chat_bot_id: str
) -> List[str]:
    """Build persistent FileUpload and legacy-download candidates in priority order."""
    subject = _text(row.get("subject"))
    content_address = _text(row.get("content_address"))
    candidates: List[str] = []
    if db_name and chat_bot_id:
        try:
            from config.settings import get_fileupload_root, get_storage_domain_for_db_name

            storage_domain = _text(get_storage_domain_for_db_name(db_name))
            uuid_tail = _text(chat_bot_id).rsplit("-", 1)[-1]
            if storage_domain and uuid_tail:
                persistent_dir = os.path.join(get_fileupload_root(), storage_domain, uuid_tail)
                # content_address is a transient download path, but its basename is
                # the storage filename recorded at crawl time.  Preserve it when
                # re-building the official FileUpload location.
                storage_filename = os.path.basename(content_address) if content_address else ""
                if storage_filename:
                    candidates.append(os.path.join(persistent_dir, storage_filename))
                if subject:
                    candidates.append(os.path.join(persistent_dir, subject))
        except Exception:
            logger.debug("[FileMaintenance][simhash_nas_candidate_config_failed] db=%s chat_bot_id=%s", db_name, chat_bot_id, exc_info=True)
    if content_address:
        candidates.append(content_address)
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def _resolve_nas_file_for_hash_backfill(
    row: Dict[str, Any], *, db_name: str = "", chat_bot_id: str = ""
) -> Tuple[str, str]:
    """Return the persistent NAS file only when it still matches the LEARN_LIST subject."""
    subject = _text(row.get("subject"))
    content_address = _text(row.get("content_address"))
    if not subject:
        return "", "subject_missing"
    candidates = _nas_file_candidates_for_hash_backfill(row, db_name=db_name, chat_bot_id=chat_bot_id)
    if not candidates:
        return "", "content_address_missing"
    allowed_filenames = {subject.casefold()}
    storage_filename = os.path.basename(content_address).strip()
    found_non_file = False
    found_filename_mismatch = False
    for candidate in candidates:
        if not os.path.exists(candidate):
            continue
        if not os.path.isfile(candidate):
            found_non_file = True
            continue
        is_rebuilt_storage_candidate = bool(storage_filename) and os.path.normpath(candidate) != os.path.normpath(content_address)
        candidate_filename = os.path.basename(candidate).casefold()
        if candidate_filename not in allowed_filenames and not (
            is_rebuilt_storage_candidate and candidate_filename == storage_filename.casefold()
        ):
            found_filename_mismatch = True
            continue
        return candidate, ""
    if found_filename_mismatch:
        return "", "filename_mismatch"
    if found_non_file:
        return "", "nas_path_not_file"
    return "", "nas_file_missing"


async def _extract_nas_file_text_for_hash_backfill(path: str) -> str:
    """Recreate the crawler's pre-persist SimHash text input from a NAS file."""
    try:
        from edu.learn_file_plain_text import LEARN_PLAIN_TEXT_EXTS, extract_plain_text_like_learn_modules
        from edu.document_markdown_fallback import extract_structured_markdown, should_use_structured_markdown_fallback
        from backend.file.file_learning_text_mask import mask_file_learning_text
        from utils.rrn_pattern_guard import learning_blocked_by_rrn_pattern, mask_rrn_like_patterns

        ext = os.path.splitext(path)[1].lower()
        if ext not in LEARN_PLAIN_TEXT_EXTS:
            return ""
        extracted = str(
            await extract_plain_text_like_learn_modules(
                path,
                personal_info_filter="N",
                timeout_sec=1800.0,
            )
            or ""
        ).strip()
        if should_use_structured_markdown_fallback(path, extracted):
            markdown = str(await extract_structured_markdown(path) or "").strip()
            if len(markdown) > len(extracted):
                extracted = markdown
        if learning_blocked_by_rrn_pattern(extracted):
            if str(os.getenv("FILE_CRAWL_RRN_MASK_INSTEAD_OF_BLOCK", "1") or "1").strip().lower() not in {"1", "true", "yes", "on"}:
                return ""
            extracted = mask_rrn_like_patterns(extracted)
        return mask_file_learning_text(extracted)[0].strip()
    except Exception:
        logger.exception("[FileMaintenance][simhash_parse_failed] file=%s", path)
        return ""


def _index_maria_rows_by_filename(maria_rows: Iterable[Dict[str, Any]]) -> Tuple[Dict[str, List[int]], int]:
    """Index persisted files by normalized filename before scanning detail pages."""
    indexed: Dict[str, List[int]] = defaultdict(list)
    skipped = 0
    for row in maria_rows:
        row_id = int(row.get("id") or 0)
        key = _filename_match_key(row.get("subject"))
        if row_id <= 0 or key is None:
            skipped += 1
            continue
        indexed[key].append(row_id)
    return dict(indexed), skipped


def _resolve_filename_match_candidates(
    candidates: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[int, Dict[str, Any]], List[Dict[str, Any]]]:
    """Keep only one-row/one-page filename matches; retain the rest for review."""
    resolved: Dict[int, Dict[str, Any]] = {}
    ambiguous: List[Dict[str, Any]] = []
    for key in sorted(candidates):
        candidate = candidates[key]
        row_ids = sorted({int(row_id) for row_id in candidate.get("learn_list_ids", set()) if int(row_id or 0) > 0})
        details_by_source_url = candidate.get("details_by_source_url") or {}
        source_urls = sorted(url for url in details_by_source_url if _text(url))
        if len(row_ids) == 1 and len(source_urls) == 1:
            resolved[row_ids[0]] = dict(details_by_source_url[source_urls[0]])
            continue
        reasons: List[str] = []
        if len(row_ids) != 1:
            reasons.append("learn_list_rows")
        if len(source_urls) != 1:
            reasons.append("detail_pages")
        ambiguous.append(
            {
                "file": _text(candidate.get("file_name")) or key,
                "learn_list_ids": row_ids,
                "source_urls": source_urls,
                "reason": ",".join(reasons) or "ambiguous",
            }
        )
    return resolved, ambiguous


def _safe_pg_table(value: Any) -> str:
    table = _text(value).lower()
    return table if _PG_TABLE_PATTERN.fullmatch(table) else ""


def _safe_maria_table(value: Any) -> str:
    table = _text(value)
    return table if _MARIADB_TABLE_PATTERN.fullmatch(table) else ""


def _pg_training_table_candidates(
    chat_bot_id: str,
    account_identifier: str = "",
    requested_table: str = "",
) -> List[str]:
    """Return explicit and compatibility PG training-table candidates in order."""
    candidates: List[str] = []
    for raw in (
        requested_table,
        f"td_{_text(account_identifier).lower()}_training_data",
        f"td_{_text(chat_bot_id).replace('-', '').lower()}_training_data",
    ):
        candidate = _safe_pg_table(raw)
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    raw = _text(value)
    for _ in range(2):
        if not raw:
            break
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            break
        if isinstance(parsed, dict):
            return dict(parsed)
        if isinstance(parsed, str):
            raw = parsed.strip()
            continue
        break
    return {}


def _value_from_mapping(mapping: Dict[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, (dict, list, tuple)):
            continue
        value = _text(value)
        if value:
            return value
    return ""


def extract_pg_text_source(text_data: Any) -> str:
    """Read the attachment URL from JSON and legacy bracketed PG text data."""
    parsed = _parse_json_dict(text_data)
    source = _value_from_mapping(
        parsed,
        ("source", "source_url", "file_url", "attachment_url", "download_url", "url"),
    )
    if source:
        return source
    raw = _text(text_data)
    for line in raw.splitlines():
        match = re.match(r"^\[(?:source|source_url|file_url|url)\s*:\s*(.*?)\]\s*$", line.strip(), re.I)
        if match and _text(match.group(1)):
            return _text(match.group(1))
    return ""


def extract_pg_plain_body(text_data: Any) -> str:
    """Prefer JSON ``content`` and otherwise remove recognisable PG metadata.

    Text-data formats have changed over time.  Keeping this parser independent
    from the backfill workflow makes new format guards small and testable.
    """
    parsed = _parse_json_dict(text_data)
    content = _value_from_mapping(parsed, ("content", "body", "text", "plain_text", "text_data"))
    if content:
        return content
    raw = _text(text_data)
    lines: List[str] = []
    for line in raw.splitlines():
        clean = line.strip()
        if not clean or _BRACKET_METADATA_LINE.match(clean) or _URL_ONLY_LINE.match(clean) or _DATE_ONLY_LINE.match(clean):
            continue
        lines.append(clean)
    return "\n".join(lines).strip()


def _merge_blank_metadata(current: Any, additions: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    metadata = _parse_json_dict(current)
    changed = False
    for key, value in additions.items():
        value = _text(value)
        if value and not _text(metadata.get(key)):
            metadata[key] = value
            changed = True
    return metadata, changed


async def _resolve_pg_training_table(
    db_name: str, chat_bot_id: str, requested_table: str = ""
) -> str:
    account_identifier = ""
    try:
        account_identifier = _text(await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name))
    except Exception:
        logger.debug("[FileMaintenance][pg_account_lookup_failed] db=%s chat_bot_id=%s", db_name, chat_bot_id, exc_info=True)
    candidates = _pg_training_table_candidates(
        chat_bot_id,
        account_identifier,
        requested_table,
    )
    if not candidates:
        return ""
    session_factory = get_session_factory(db_name)
    async with session_factory() as session:
        rows = await session.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = ANY(:names)"
            ),
            {"names": candidates},
        )
        existing = {str(row[0]).lower() for row in rows.fetchall() if row and row[0]}
    return next((candidate for candidate in candidates if candidate in existing), "")


async def _pg_columns(db_name: str, table_name: str) -> set[str]:
    session_factory = get_session_factory(db_name)
    async with session_factory() as session:
        rows = await session.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :table_name"
            ),
            {"table_name": table_name},
        )
        return {str(row[0]) for row in rows.fetchall() if row and row[0]}


async def _load_maria_rows(
    *, db_name: str, learn_table: str, hash_blank_only: bool, content_type: str
) -> List[Dict[str, Any]]:
    content_type = _normalize_target_content_type(content_type)
    where = "`content_type` = %s"
    params: List[Any] = [content_type]
    if hash_blank_only:
        where += " AND (`hash` IS NULL OR TRIM(`hash`) = '')"
    rows = await maria_execute_query(
        f"SELECT `id`, `subject`, `content`, `content_address`, `size`, `hash`, `status` FROM `{learn_table}` WHERE {where} ORDER BY `id` ASC",
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    return [dict(row) for row in (rows or []) if isinstance(row, dict)]


async def _load_pg_rows_by_subjects(
    *, db_name: str, table_name: str, subjects: Iterable[str], content_type: str
) -> List[Dict[str, Any]]:
    values = sorted({_text(value) for value in subjects if _text(value)})
    if not values:
        return []
    content_type = _normalize_target_content_type(content_type)
    columns = await _pg_columns(db_name, table_name)
    required = {"id", "subject", "text_data", "content_metadata", "content_type"}
    missing = required - columns
    if missing:
        raise RuntimeError(f"pg_columns_missing:{','.join(sorted(missing))}")
    safe_table = _safe_pg_table(table_name)
    if not safe_table:
        raise ValueError("invalid_pg_training_table")
    session_factory = get_session_factory(db_name)
    loaded: List[Dict[str, Any]] = []
    async with session_factory() as session:
        for offset in range(0, len(values), 300):
            result = await session.execute(
                text(
                    f"SELECT id, subject, text_data, content_metadata FROM public.\"{safe_table}\" "
                    "WHERE subject = ANY(:subjects) AND content_type = :content_type ORDER BY id"
                ),
                {"subjects": values[offset : offset + 300], "content_type": content_type},
            )
            loaded.extend(dict(row) for row in result.mappings().all())
    return loaded


def _confirmed_matches(
    maria_rows: Iterable[Dict[str, Any]], pg_rows: Iterable[Dict[str, Any]]
) -> Tuple[Dict[int, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    by_subject: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in pg_rows:
        subject = _text(row.get("subject"))
        if subject:
            by_subject[subject].append(row)
    confirmed: Dict[int, List[Dict[str, Any]]] = {}
    unresolved: List[Dict[str, Any]] = []
    for maria in maria_rows:
        row_id = int(maria.get("id") or 0)
        subject = _text(maria.get("subject"))
        file_url = _text(maria.get("content"))
        candidates = by_subject.get(subject, [])
        exact = [row for row in candidates if extract_pg_text_source(row.get("text_data")) == file_url]
        if row_id > 0 and exact:
            confirmed[row_id] = exact
        else:
            unresolved.append({**maria, "candidate_count": len(candidates)})
    return confirmed, unresolved


def _metadata_source_url(pg_rows: Iterable[Dict[str, Any]]) -> str:
    for row in pg_rows:
        metadata = _parse_json_dict(row.get("content_metadata"))
        source = _value_from_mapping(metadata, ("source_url", "source_page", "post_url", "board_url"))
        if source:
            return source
    return ""


def _metadata_needs_backfill(pg_rows: Iterable[Dict[str, Any]]) -> bool:
    desired_keys = ("source_url", "author", "department", "reg_date", "cate1", "cate2")
    metadata_rows = [_parse_json_dict(row.get("content_metadata")) for row in pg_rows]
    return any(not any(_text(metadata.get(key)) for metadata in metadata_rows) for key in desired_keys)


def _metadata_known_values(pg_rows: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for row in pg_rows:
        metadata = _parse_json_dict(row.get("content_metadata"))
        for key in ("source_url", "author", "department", "reg_date", "cate1", "cate2"):
            if not values.get(key) and _text(metadata.get(key)):
                values[key] = _text(metadata.get(key))
    return values


async def _update_pg_metadata_rows(
    *, db_name: str, table_name: str, pg_rows: Iterable[Dict[str, Any]], additions: Dict[str, Any]
) -> int:
    safe_table = _safe_pg_table(table_name)
    if not safe_table:
        raise ValueError("invalid_pg_training_table")
    updates: List[Tuple[int, str]] = []
    for row in pg_rows:
        row_id = int(row.get("id") or 0)
        metadata, changed = _merge_blank_metadata(row.get("content_metadata"), additions)
        if row_id > 0 and changed:
            updates.append((row_id, json.dumps(metadata, ensure_ascii=False)))
    if not updates:
        return 0
    session_factory = get_session_factory(db_name)
    async with session_factory() as session:
        async with session.begin():
            for row_id, metadata in updates:
                await session.execute(
                    text(f"UPDATE public.\"{safe_table}\" SET content_metadata = CAST(:metadata AS jsonb) WHERE id = :id"),
                    {"id": row_id, "metadata": metadata},
                )
    return len(updates)


async def _close_workflow(workflow: Optional[FileDownloadWorkflow]) -> None:
    if workflow is None:
        return
    await asyncio.gather(
        *[
            action()
            for action in (getattr(workflow, "_close_http_session", None), getattr(workflow, "_close_playwright", None))
            if callable(action)
        ],
        return_exceptions=True,
    )


async def _recover_source_urls(
    *,
    job_id: str,
    db_name: str,
    chat_bot_id: str,
    unresolved: List[Dict[str, Any]],
    stop_event: asyncio.Event,
    event: EventCallback,
) -> Dict[int, Dict[str, str]]:
    """Recover missing detail URLs by scanning each exploration post URL once."""
    targets_by_url = {_text(row.get("content")): row for row in unresolved if _text(row.get("content"))}
    targets_by_subject: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in unresolved:
        subject = _text(row.get("subject"))
        if subject:
            targets_by_subject[subject].append(row)
    if not targets_by_url and not targets_by_subject:
        return {}
    post_items = await load_file_crawl_post_url_strings(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        contents_url=None,
        target_domains=None,
        use_category_rules=False,
        dedupe_urls=True,
        limit=None,
    )
    await event("source_recovery_started", {"post_count": len(post_items), "target_count": len(unresolved)})
    workflow: Optional[FileDownloadWorkflow] = None
    found: Dict[int, Dict[str, str]] = {}
    ambiguous: set[int] = set()
    try:
        workflow = FileDownloadWorkflow()
        workflow.job_id = f"file-maintenance:{job_id}"
        workflow.db_name = db_name
        workflow.chat_bot_id = chat_bot_id
        workflow.enable_db_save = False
        workflow.enable_learning = False
        workflow.file_pipeline_skip_learning = True
        workflow.stop_event = stop_event

        async def on_result(result: Dict[str, Any]) -> None:
            if stop_event.is_set():
                return
            post_url = _text(result.get("url"))
            attachments = result.get("attachments") if isinstance(result.get("attachments"), list) else []
            matches_by_row: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                file_url = _text(attachment.get("url"))
                name = _text(attachment.get("name"))
                exact_target = targets_by_url.get(file_url)
                if exact_target:
                    matches_by_row[int(exact_target["id"])].append(attachment)
                    continue
                subject_targets = targets_by_subject.get(name, [])
                if len(subject_targets) == 1:
                    matches_by_row[int(subject_targets[0]["id"])].append(attachment)
            for row_id, matched in matches_by_row.items():
                if len(matched) != 1 or row_id in found:
                    ambiguous.add(row_id)
                    found.pop(row_id, None)
                    continue
                attachment = matched[0]
                breadcrumb = result.get("breadcrumb") if isinstance(result.get("breadcrumb"), dict) else {}
                found[row_id] = {
                    "source_url": post_url,
                    "reg_date": _text(result.get("reg_date")),
                    "author": _text(result.get("author")),
                    "department": _text(result.get("department")),
                    "cate1": _text(breadcrumb.get("cate1")),
                    "cate2": _text(breadcrumb.get("cate2")),
                }
                await event("source_recovered", {"learn_list_id": row_id, "source_url": post_url})

        await run_fast_file_attachment_front(
            workflow=workflow,
            post_items=post_items,
            concurrency=2,
            timeout_sec=_file_crawl_detail_fetch_timeout_sec({}),
            enqueue=False,
            include_attachment_details=True,
            include_breadcrumb_metadata=True,
            playwright_fallback_on_fetch_failure=True,
            result_callback=on_result,
        )
    finally:
        await _close_workflow(workflow)
    for row_id in ambiguous:
        found.pop(row_id, None)
    await event("source_recovery_completed", {"recovered_count": len(found), "ambiguous_count": len(ambiguous)})
    return found


async def _apply_metadata_backfill_plan(
    *,
    db_name: str,
    stop_event: asyncio.Event,
    event: EventCallback,
    prepared_plan: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    """Apply a previously reviewed in-memory metadata plan without rescanning."""
    applied = 0
    planned = 0
    for item in prepared_plan:
        if not isinstance(item, dict):
            continue
        planned += 1
        if stop_event.is_set():
            break
        changed = await _update_pg_metadata_rows(
            db_name=db_name,
            table_name=_text(item.get("pg_table")),
            pg_rows=item.get("pg_rows") or [],
            additions=item.get("additions") or {},
        )
        applied += changed
    summary = {"planned_learn_rows": planned, "updated": applied}
    await event("metadata_backfill_applied", summary)
    return summary


async def run_file_metadata_backfill(
    *,
    job_id: str,
    db_name: str,
    chat_bot_id: str,
    stop_event: asyncio.Event,
    event: EventCallback,
    pg_table: str = "",
    content_type: str = "file",
    target_domains: Optional[List[str]] = None,
    dry_run: bool = True,
    prepared_plan: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Backfill blank PG metadata from the first matching exploration detail page.

    Matching is deliberately local and conservative: filename, extension, and
    an explicit exact byte size must all agree before a persisted file can be
    associated with an attachment.  The operation never downloads files or
    enters the production save/learning pipeline.
    """
    if not dry_run:
        if prepared_plan is None:
            raise ValueError("metadata_backfill_plan_required")
        return await _apply_metadata_backfill_plan(
            db_name=db_name,
            stop_event=stop_event,
            event=event,
            prepared_plan=prepared_plan,
        )
    target_domains = [domain for domain in (target_domains or []) if _text(domain)]
    if not target_domains:
        raise ValueError("target_domain_required")
    learn_table = _safe_maria_table(await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name))
    if not learn_table:
        raise RuntimeError("learn_list_table_not_found")
    content_type = _normalize_target_content_type(content_type)
    maria_rows = await _load_maria_rows(
        db_name=db_name,
        learn_table=learn_table,
        hash_blank_only=False,
        content_type=content_type,
    )
    maria_by_id = {int(row.get("id") or 0): row for row in maria_rows if int(row.get("id") or 0) > 0}
    maria_index, maria_filename_missing = _index_maria_rows_by_filename(maria_rows)
    requested_pg_table = _text(pg_table)
    pg_table = await _resolve_pg_training_table(db_name, chat_bot_id, requested_pg_table)
    if not pg_table:
        raise RuntimeError(f"pg_training_table_not_found:{requested_pg_table or 'auto'}")

    post_items = await load_file_crawl_post_url_strings(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        contents_url=None,
        target_domains=target_domains,
        use_category_rules=False,
        dedupe_urls=True,
        limit=None,
    )
    await event(
        "metadata_scan_started",
        {
            "maria_rows": len(maria_rows),
            "content_type": content_type,
            "maria_matchable": sum(len(row_ids) for row_ids in maria_index.values()),
            "maria_filename_missing": maria_filename_missing,
            "exploration_urls": len(post_items),
            "target_domains": target_domains,
        },
    )

    workflow: Optional[FileDownloadWorkflow] = None
    first_detail_by_row_id: Dict[int, Dict[str, Any]] = {}
    filename_candidates: Dict[str, Dict[str, Any]] = {}
    attachment_size_missing = 0
    attachment_matches = 0
    scanned_details = 0
    attachments_found = 0
    attachments_exact_size_known = 0
    attachment_lists_extracted_details = 0
    author_extracted_pages = 0
    reg_date_extracted_pages = 0
    last_progress_emit_at = 0.0
    last_progress_emit_visited = 0
    post_order: Dict[str, int] = {}
    for index, item in enumerate(post_items):
        post_url = _text(getattr(item, "url", "") or (item.get("url") if isinstance(item, dict) else ""))
        if post_url and post_url not in post_order:
            post_order[post_url] = index

    try:
        workflow = FileDownloadWorkflow()
        workflow.job_id = f"file-maintenance:{job_id}"
        workflow.db_name = db_name
        workflow.chat_bot_id = chat_bot_id
        workflow.enable_db_save = False
        workflow.enable_learning = False
        workflow.file_pipeline_skip_learning = True
        workflow.stop_event = stop_event

        async def emit_progress(*, last_source_url: str = "", force: bool = False) -> None:
            nonlocal last_progress_emit_at, last_progress_emit_visited
            now = time.monotonic()
            if not force and scanned_details != 1:
                progressed = scanned_details - last_progress_emit_visited
                if progressed < 10 and now - last_progress_emit_at < 1.0:
                    return
            await event(
                "metadata_scan_progress",
                _metadata_scan_progress_payload(
                    exploration_urls=len(post_items),
                    visited_details=scanned_details,
                    attachment_lists_extracted_details=attachment_lists_extracted_details,
                    attachments_found=attachments_found,
                    attachments_exact_size_known=attachments_exact_size_known,
                    attachment_size_missing=attachment_size_missing,
                    attachment_matches=attachment_matches,
                    first_detail_matches=len(first_detail_by_row_id),
                    author_extracted_pages=author_extracted_pages,
                    reg_date_extracted_pages=reg_date_extracted_pages,
                    last_source_url=last_source_url,
                ),
            )
            last_progress_emit_at = now
            last_progress_emit_visited = scanned_details

        async def on_result(result: Dict[str, Any]) -> None:
            nonlocal attachment_matches, attachment_size_missing, attachments_found
            nonlocal attachments_exact_size_known, author_extracted_pages, reg_date_extracted_pages
            nonlocal scanned_details, attachment_lists_extracted_details
            if stop_event.is_set():
                return
            scanned_details += 1
            if "attachment_count" in result:
                attachment_lists_extracted_details += 1
            source_url = _text(result.get("url"))
            order = post_order.get(source_url, len(post_order) + scanned_details)
            attachments = result.get("attachments") if isinstance(result.get("attachments"), list) else []
            attachments_found += len(attachments)
            author = _text(result.get("author"))
            reg_date = _text(result.get("reg_date"))
            if author:
                author_extracted_pages += 1
            if reg_date:
                reg_date_extracted_pages += 1
            for attachment in attachments:
                if not isinstance(attachment, dict):
                    continue
                exact_size = attachment.get("exact_file_size_bytes")
                if int(exact_size or 0) > 0:
                    attachments_exact_size_known += 1
                key = _filename_match_key(attachment.get("name"))
                if key is None:
                    if _text(attachment.get("name")):
                        attachment_size_missing += 1
                    continue
                matched_row_ids = maria_index.get(key, [])
                if not matched_row_ids:
                    continue
                attachment_matches += len(matched_row_ids)
                detail = {
                    "order": order,
                    "source_url": source_url,
                    "author": author,
                    "reg_date": reg_date,
                }
                candidate = filename_candidates.setdefault(
                    key,
                    {
                        "file_name": _text(attachment.get("name")),
                        "learn_list_ids": set(),
                        "details_by_source_url": {},
                    },
                )
                candidate["learn_list_ids"].update(matched_row_ids)
                previous = candidate["details_by_source_url"].get(source_url)
                if previous is None or order < int(previous.get("order") or order):
                    candidate["details_by_source_url"][source_url] = detail
            await emit_progress(last_source_url=source_url)

        await run_fast_file_attachment_front(
            workflow=workflow,
            post_items=post_items,
            concurrency=2,
            timeout_sec=_file_crawl_detail_fetch_timeout_sec({}),
            enqueue=False,
            include_attachment_details=True,
            include_breadcrumb_metadata=False,
            playwright_fallback_on_fetch_failure=True,
            result_callback=on_result,
        )
        await emit_progress(force=True)
    finally:
        await _close_workflow(workflow)

    first_detail_by_row_id, ambiguous_filename_candidates = _resolve_filename_match_candidates(filename_candidates)
    selected_maria_rows = [maria_by_id[row_id] for row_id in first_detail_by_row_id if row_id in maria_by_id]
    pg_rows = await _load_pg_rows_by_subjects(
        db_name=db_name,
        table_name=pg_table,
        subjects=(row.get("subject") for row in selected_maria_rows),
        content_type=content_type,
    )
    pg_by_subject: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in pg_rows:
        subject = _text(row.get("subject"))
        if subject:
            pg_by_subject[subject].append(row)

    pg_subject_missing = 0
    source_already_present = 0
    prepared: List[Dict[str, Any]] = []
    preview_samples: List[Dict[str, Any]] = []
    planned_pg_rows = 0
    for row_id, detail in first_detail_by_row_id.items():
        if stop_event.is_set():
            break
        maria = maria_by_id.get(row_id) or {}
        rows = pg_by_subject.get(_text(maria.get("subject")), [])
        if not rows:
            pg_subject_missing += 1
            continue
        if _metadata_source_url(rows):
            source_already_present += 1
        additions = {
            "source_url": _text(detail.get("source_url")),
            "author": _text(detail.get("author")),
            "reg_date": _text(detail.get("reg_date")),
        }
        changed_rows = [row for row in rows if _merge_blank_metadata(row.get("content_metadata"), additions)[1]]
        if not changed_rows:
            continue
        prepared.append(
            {
                "learn_list_id": row_id,
                "pg_table": pg_table,
                "pg_rows": rows,
                "additions": additions,
            }
        )
        planned_pg_rows += len(changed_rows)
        if len(preview_samples) < 20:
            preview_samples.append(
                {
                    "learn_list_id": row_id,
                    "file": _text(maria.get("subject")),
                    "source_url": _text(detail.get("source_url")),
                    "pg_rows": len(changed_rows),
                }
            )
    summary = {
        "maria_rows": len(maria_rows),
        "content_type": content_type,
        "maria_matchable": sum(len(row_ids) for row_ids in maria_index.values()),
        "maria_filename_missing": maria_filename_missing,
        "exploration_urls": len(post_items),
        "target_domains": target_domains,
        "detail_scanned": scanned_details,
        "visited_details": scanned_details,
        "remaining_details": max(len(post_items) - scanned_details, 0),
        "attachments_found": attachments_found,
        "attachment_lists_extracted_details": attachment_lists_extracted_details,
        "attachments_exact_size_known": attachments_exact_size_known,
        "attachment_size_missing": attachment_size_missing,
        "attachment_matches": attachment_matches,
        "first_detail_matches": len(first_detail_by_row_id),
        "ambiguous_filename_candidate_count": len(ambiguous_filename_candidates),
        "author_extracted_pages": author_extracted_pages,
        "reg_date_extracted_pages": reg_date_extracted_pages,
        "pg_rows": len(pg_rows),
        "pg_subject_missing": pg_subject_missing,
        "source_already_present": source_already_present,
        "dry_run": True,
        "planned_learn_rows": len(prepared),
        "planned_pg_rows": planned_pg_rows,
        "updated": 0,
    }
    await event(
        "metadata_dry_run_ready",
        {**summary, "preview": preview_samples, "ambiguous_filename_candidates": ambiguous_filename_candidates[:20]},
    )
    return {
        **summary,
        "_prepared_plan": prepared,
        "preview_samples": preview_samples,
        "_ambiguous_filename_candidates": ambiguous_filename_candidates[:20],
    }


async def run_file_simhash_backfill(
    *,
    job_id: str,
    db_name: str,
    chat_bot_id: str,
    stop_event: asyncio.Event,
    event: EventCallback,
    pg_table: str = "",
    content_type: str = "file",
    dry_run: bool = True,
    prepared_learn_list_ids: Optional[Iterable[int]] = None,
) -> Dict[str, Any]:
    """Validate file rows first, then dispatch only after explicit approval."""
    learn_table = _safe_maria_table(await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name))
    if not learn_table:
        raise RuntimeError("learn_list_table_not_found")
    content_type = _normalize_target_content_type(content_type)
    maria_rows = await _load_maria_rows(
        db_name=db_name,
        learn_table=learn_table,
        hash_blank_only=True,
        content_type=content_type,
    )
    approved_ids: Optional[set[int]] = None
    if prepared_learn_list_ids is not None:
        approved_ids = {int(value) for value in prepared_learn_list_ids if int(value or 0) > 0}
        maria_rows = [row for row in maria_rows if int(row.get("id") or 0) in approved_ids]
    prepared = 0
    nas_missing = 0
    filename_mismatch = 0
    parse_failed = 0
    dispatched = 0
    dispatch_failed = 0
    update_error_details: List[Dict[str, Any]] = []
    nas_unavailable_details: List[Dict[str, Any]] = []
    prepared_learn_list_ids_result: List[int] = []
    prepared_file_details: List[Dict[str, Any]] = []
    prepared_file_details_all: List[Dict[str, Any]] = []
    total_rows = len(maria_rows)

    async def emit_simhash_progress(event_name: str, payload: Dict[str, Any], row_index: int) -> None:
        await event(
            event_name,
            {
                **payload,
                "processed_rows": row_index,
                "total_rows": total_rows,
                "dry_run": dry_run,
            },
        )

    for row_index, maria in enumerate(maria_rows, start=1):
        if stop_event.is_set():
            break
        row_id = int(maria.get("id") or 0)
        subject = _text(maria.get("subject"))
        content_address = _text(maria.get("content_address"))
        nas_candidates = _nas_file_candidates_for_hash_backfill(
            maria,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
        )
        logger.debug(
            "[FileMaintenance][simhash_nas_lookup] job_id=%s db=%s chat_bot_id=%s learn_list_id=%s subject=%s content_address=%s candidates=%s candidate_states=%s",
            job_id,
            db_name,
            chat_bot_id,
            row_id,
            subject,
            content_address,
            nas_candidates,
            [
                {"path": candidate, "exists": os.path.exists(candidate), "is_file": os.path.isfile(candidate)}
                for candidate in nas_candidates
            ],
        )
        path, reason = _resolve_nas_file_for_hash_backfill(
            maria,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
        )
        if not path:
            candidate_states = [
                {"path": candidate, "exists": os.path.exists(candidate), "is_file": os.path.isfile(candidate)}
                for candidate in nas_candidates
            ]
            unavailable_detail = {
                "learn_list_id": row_id,
                "file": subject,
                "reason": reason,
                "content_address": content_address,
                "candidates": candidate_states,
            }
            if len(nas_unavailable_details) < 20:
                nas_unavailable_details.append(unavailable_detail)
            if reason == "filename_mismatch":
                filename_mismatch += 1
            else:
                nas_missing += 1
            logger.info(
                "[FileMaintenance][simhash_nas_unavailable] job_id=%s db=%s chat_bot_id=%s learn_list_id=%s subject=%s content_address=%s candidates=%s reason=%s",
                job_id,
                db_name,
                chat_bot_id,
                row_id,
                subject,
                content_address,
                nas_candidates,
                reason,
            )
            await emit_simhash_progress("simhash_skipped", unavailable_detail, row_index)
            continue
        prepared += 1
        try:
            file_size = os.path.getsize(path)
        except OSError:
            file_size = -1
        logger.debug(
            "[FileMaintenance][simhash_nas_prepared] job_id=%s db=%s chat_bot_id=%s learn_list_id=%s subject=%s resolved_path=%s file_size=%s",
            job_id,
            db_name,
            chat_bot_id,
            row_id,
            subject,
            path,
            file_size,
        )
        await emit_simhash_progress(
            "simhash_nas_prepared",
            {"learn_list_id": row_id, "file": _text(maria.get("subject"))},
            row_index,
        )
        body = await _extract_nas_file_text_for_hash_backfill(path)
        if not body:
            parse_failed += 1
            await emit_simhash_progress(
                "simhash_skipped",
                {"learn_list_id": row_id, "reason": "nas_file_text_empty"},
                row_index,
            )
            continue
        if dry_run:
            prepared_learn_list_ids_result.append(row_id)
            prepared_detail = {
                "learn_list_id": row_id,
                "file": subject,
                "nas_path": path,
                "content_chars": len(body),
            }
            prepared_file_details_all.append(prepared_detail)
            if len(prepared_file_details) < 20:
                prepared_file_details.append(prepared_detail)
            await emit_simhash_progress(
                "simhash_dry_run_prepared",
                prepared_detail,
                row_index,
            )
            continue
        failure_context: Dict[str, str] = {}
        generated = await generate_file_simhash_result(
            job_id=job_id,
            learn_list_row_id=row_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            file_url=_text(maria.get("content")),
            source_url="",
            title=_text(maria.get("subject")),
            content=body,
            consume_result=True,
            failure_context=failure_context,
        )
        if generated is None or not _text(getattr(generated, "value", "")) or not bool(getattr(generated, "updated", False)):
            dispatch_failed += 1
            reason = failure_context.get("reason") or "hash_api_update_not_confirmed"
            error = failure_context.get("error") or (
                "hash_api_no_result" if generated is None else "hash_api_response_missing_updated"
            )
            if len(update_error_details) < 20:
                update_error_details.append(
                    {
                        "learn_list_id": row_id,
                        "file": subject,
                        "reason": reason,
                        "error": error,
                    }
                )
            logger.warning(
                "[FileMaintenance][simhash_update_failed] job_id=%s db=%s chat_bot_id=%s learn_list_id=%s subject=%s reason=%s error=%s",
                job_id,
                db_name,
                chat_bot_id,
                row_id,
                subject,
                reason,
                error,
            )
            await emit_simhash_progress(
                "simhash_skipped",
                {"learn_list_id": row_id, "file": subject, "reason": reason, "error": error},
                row_index,
            )
            continue
        dispatched += 1
        await emit_simhash_progress(
            "simhash_dispatched",
            {
                "learn_list_id": row_id,
                "subject": _text(maria.get("subject")),
                "content_chars": len(body),
                "normalized_length": int(getattr(generated, "normalized_length", 0) or 0),
            },
            row_index,
        )
    return {
        "content_type": content_type,
        "maria_rows": len(maria_rows),
        "nas_prepared": prepared,
        "nas_missing": nas_missing,
        "nas_unavailable_details": nas_unavailable_details,
        "nas_unavailable_detail_count": len(nas_unavailable_details),
        "filename_mismatch": filename_mismatch,
        "parse_failed": parse_failed,
        "dry_run": dry_run,
        "planned_learn_rows": len(prepared_learn_list_ids_result) if dry_run else len(approved_ids or ()),
        "updated": dispatched,
        "update_failed": dispatch_failed,
        "requests_dispatched": dispatched,
        "request_failed": dispatch_failed,
        "update_error_details": update_error_details,
        "update_error_detail_count": len(update_error_details),
        "prepared_file_details": prepared_file_details,
        "_prepared_file_details_all": prepared_file_details_all,
        "_prepared_learn_list_ids": prepared_learn_list_ids_result,
    }
