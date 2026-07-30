"""
Resolve post/detail URLs for file crawling (colle=file).

File crawl uses existing ASADAL_CRAWLING_EXPLORATION post rows as the
source of detail pages. Category mapping is handled later from direct board category values and `_category` file-root mapping.

Rules:
- Load DB rows with type='post' as start_urls for FileDownloadWorkflow.
- Do not use legacy url_pattern/cate_match category routing for file crawling.
- Preserve direct board category values only when they are explicitly present on the source row/item.
- Select exploration post rows scoped by contents/target domains and service path.
- Do not use `url_pattern` rows or `cate_match` metadata for file crawling.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from typing import Any, AsyncIterable, Dict, List, Optional, Tuple, Union
from urllib.parse import parse_qsl, urljoin, urlparse, urlunparse

from backend.shared.url_scope import (
    extract_service_scope_path_prefix,
    extract_scope_host,
    extract_scope_identities,
    extract_scope_path_prefix,
    normalize_scope_path_prefix,
    url_matches_scope_identities,
)
from backend.shared.url_pattern_identity import (
    group_urls_by_structure_pattern,
    url_structure_pattern_has_variable,
    url_structure_pattern_key,
)
from backend.shared.pre_explored_url import (
    _build_exploration_date_range_condition,
    _coerce_bool_flag,
    mark_exploration_url_as_post_for_temporary_category_match,
    _parse_target_date_range,
    _resolve_exploration_date_column,
)
from backend.shared.exploration_query import (
    EXPLORATION_TABLE,
    ExplorationQuerySpec,
    build_exploration_conditions,
    sql_single_quoted_literal,
)
from db.maria_operations import maria_execute_query, maria_select_data
from utils.attachment_url_normalize import canonicalize_attachment_url_for_learn_list
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme, normalize_attachment_href

logger = logging.getLogger("backend.shared.file_crawl_post_urls")

_EXPLORATION_COLUMN_CACHE: Dict[Tuple[str, str], set[str]] = {}
_EXPLORATION_ATTACHMENT_META_COLUMNS: Tuple[str, ...] = ()


def _missing_required_file_detail_query_reason(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    query = parsed.query or ""
    if "ne.go.kr" in host and path.endswith("/platform/user/pblccomm/bd_pblcdiscussionview.do") and "discussionId=" not in query and "discussionid=" not in query.lower():
        return "missing_required_detail_query:discussionid"
    return ""


_EXPLORATION_DIRECT_META_COLUMNS = (
    "title",
    "subject",
    "reg_date",
    "published_at",
    "post_date",
    "content_author",
    "author",
    "department",
)

_EXPLORATION_CATEGORY_META_COLUMNS = (
    "cate1",
    "cate2",
    "category1",
    "category2",
    "board_cate1",
    "board_cate2",
    "board_cate1_name",
    "board_cate2_name",
    "store_cate1",
    "store_cate2",
    "assigned_cate1",
    "assigned_cate2",
)
_LEARN_LIST_TABLE = "ASADAL_CRAWLING_LEARN_LIST"


def _category_debug_log_enabled() -> bool:
    return str(os.getenv("FILE_CRAWL_CATEGORY_DEBUG_LOG", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _safe_positive_int_env(name: str, default: int, *, minimum: int = 1, maximum: int = 5000) -> int:
    try:
        value = int(os.getenv(name, str(default)) or str(default))
    except Exception:
        value = default
    return max(minimum, min(value, maximum))


def _direct_attachment_fast_path_enabled() -> bool:
    # File crawling now starts from exploration type=post detail URLs and extracts
    # attachments from the fetched detail HTML. Do not use stored attachment JSON
    # or metadata columns as the front-stage source.
    value = str(os.getenv("FILE_CRAWL_DIRECT_ATTACHMENTS_FAST_PATH", "0") or "0").strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


async def _load_exploration_columns(db_name: str, table_name: str) -> set[str]:
    cache_key = (str(db_name or "").strip(), str(table_name or "").strip().lower())
    if cache_key in _EXPLORATION_COLUMN_CACHE:
        return _EXPLORATION_COLUMN_CACHE[cache_key]
    if not cache_key[0] or not cache_key[1]:
        return set()
    try:
        rows = await maria_execute_query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)",
            (cache_key[0], table_name),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        logger.debug(
            "[FileCrawlPosts][direct_attachments] exploration column scan failed | db=%s table=%s err=%s",
            db_name,
            table_name,
            exc,
        )
        _EXPLORATION_COLUMN_CACHE[cache_key] = set()
        return set()
    cols = {
        str((row or {}).get("column_name") or (row or {}).get("COLUMN_NAME") or "").strip().lower()
        for row in (rows or [])
        if isinstance(row, dict)
    }
    cols.discard("")
    _EXPLORATION_COLUMN_CACHE[cache_key] = cols
    return cols


def _build_exploration_select_columns(existing_cols: set[str], *, include_id: bool = False) -> str:
    base = ["id"] if include_id else []
    base.extend(["url", "type"])
    extra: List[str] = []
    wanted = list(_EXPLORATION_CATEGORY_META_COLUMNS)
    if _direct_attachment_fast_path_enabled():
        wanted.extend(list(_EXPLORATION_ATTACHMENT_META_COLUMNS))
        wanted.extend(list(_EXPLORATION_DIRECT_META_COLUMNS))
    for col in wanted:
        if col in existing_cols and col not in base and col not in extra:
            extra.append(col)
    return ", ".join([f"`{c}`" if c not in {"id", "url", "type"} else c for c in base + extra])


def _positive_int_or_none(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _extract_markdown_link_urls(value: Any) -> List[str]:
    text = str(value or "").strip()
    if not text:
        return []
    urls: List[str] = []
    for match in re.finditer(r"\[[^\]]*\]\((https?://[^\s)]+)\)", text, flags=re.IGNORECASE):
        urls.append(str(match.group(1) or "").strip())
    for match in re.finditer(r"https?://[^\s\])]+", text, flags=re.IGNORECASE):
        candidate = str(match.group(0) or "").strip()
        if candidate:
            urls.append(candidate)
    return urls


def _reference_url_value(raw: Any) -> str:
    if isinstance(raw, dict):
        raw = raw.get("url") or raw.get("content") or raw.get("contents_url")
    text = str(raw or "").strip()
    if not text:
        return ""
    markdown_urls = _extract_markdown_link_urls(text)
    if markdown_urls:
        # Prefer the markdown href. For "[display](href)", the href appears first
        # in this extractor and is the URL users intend for list-page scoping.
        return markdown_urls[0]
    return text

def _iter_contents_url_candidates(contents_url: Optional[Union[str, List[str]]]) -> List[str]:
    raw_values: List[Any]
    if isinstance(contents_url, list):
        raw_values = list(contents_url)
    else:
        raw_values = [contents_url]

    candidates: List[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        text = _reference_url_value(raw)
        raw_text = str(raw or "").strip()
        values = [text]
        values.extend(_extract_markdown_link_urls(raw_text))
        if raw_text and raw_text not in values:
            values.append(raw_text)
        for value in values:
            if not value:
                continue
            for candidate in (value, ensure_url_scheme(value)):
                candidate = str(candidate or "").strip()
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    candidates.append(candidate)
    return candidates


def _normalize_url_scope_base(raw_url: Any) -> str:
    text = _reference_url_value(raw_url)
    if not text:
        return ""
    try:
        text = ensure_url_scheme(text)
    except Exception:
        text = str(text or "").strip()
    text = str(text or "").strip()
    if not text:
        return ""
    if "#" in text:
        text = text.split("#", 1)[0].strip()
    while text.endswith("/") and not re.match(r"^https?://[^/]+/$", text, flags=re.IGNORECASE):
        text = text[:-1]
    return text


def _iter_url_scope_host_variants(scope: str) -> List[str]:
    text = str(scope or "").strip()
    if not text:
        return []
    variants: List[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        value = str(value or "").strip()
        if value and value not in seen:
            seen.add(value)
            variants.append(value)

    add(text)
    try:
        parsed = urlparse(text)
    except Exception:
        return variants
    host = str(parsed.netloc or "").strip()
    if not host:
        return variants
    if host.lower().startswith("www."):
        alt_host = host[4:]
    else:
        alt_host = f"www.{host}"
    if alt_host:
        add(urlunparse(parsed._replace(netloc=alt_host)))
    return variants


def _iter_post_detail_scope_variants(scope: str) -> List[str]:
    text = str(scope or "").strip()
    if not text:
        return []
    out: List[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        value = str(value or "").strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)

    try:
        parsed = urlparse(text)
    except Exception:
        return []
    path = str(parsed.path or "")
    replacements = (
        ("selectBbsNttList.do", "selectBbsNttView.do"),
        ("selectBbsNttList", "selectBbsNttView"),
        ("BD_selectBbsList.do", "BD_selectBbs.do"),
        ("BD_selectBbsList", "BD_selectBbs"),
    )
    for old, new in replacements:
        if old.lower() in path.lower():
            match = re.search(re.escape(old), path, flags=re.IGNORECASE)
            if not match:
                continue
            detail_path = path[: match.start()] + new + path[match.end():]
            add(urlunparse(parsed._replace(path=detail_path, query="")))
            break
    return out


def _scope_query_pair_sql_conditions(scope: str) -> List[str]:
    try:
        pairs = parse_qsl(urlparse(scope).query or "", keep_blank_values=True)
    except Exception:
        return []
    ignored = {"page", "pageindex", "searchkeyword", "keyword", "searchkrwd", "searchcnd", "startdate", "enddate"}
    out: List[str] = []
    seen: set[str] = set()
    for key, value in pairs:
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if not key_text or key_text.lower() in ignored:
            continue
        token = (key_text.lower(), value_text)
        if token in seen:
            continue
        seen.add(token)
        regex = r"[?&]" + re.escape(key_text) + r"=" + re.escape(value_text) + r"(&|$)"
        out.append(f"`url` REGEXP '{sql_single_quoted_literal(regex)}'")
    return out


def _append_prefix_scope_condition(parts: List[str], scope: str) -> None:
    escaped = sql_single_quoted_literal(scope)
    suffix_parts = [
        f"`url` = '{escaped}'",
        f"`url` LIKE '{escaped}/%%'",
        f"`url` LIKE '{escaped}?%%'",
    ]
    if "?" in scope:
        suffix_parts.append(f"`url` LIKE '{escaped}&%%'")
    parts.append("(" + " OR ".join(suffix_parts) + ")")


def _append_detail_scope_condition(parts: List[str], detail_scope: str, reference_scope: str) -> None:
    for scoped_detail in _iter_url_scope_host_variants(detail_scope):
        escaped = sql_single_quoted_literal(scoped_detail)
        clause_parts = [f"`url` LIKE '{escaped}?%%'"]
        clause_parts.extend(_scope_query_pair_sql_conditions(reference_scope))
        parts.append("(" + " AND ".join(clause_parts) + ")")


def _build_url_scope_fallback_condition(
    contents_url: Optional[Union[str, List[str]]],
) -> tuple[str, str]:
    candidates = _iter_contents_url_candidates(contents_url)
    parts: List[str] = []
    seen: set[str] = set()
    first_scope = ""
    for candidate in candidates:
        base_scope = _normalize_url_scope_base(candidate)
        if not base_scope:
            continue
        if not first_scope:
            first_scope = base_scope
        for scope in _iter_url_scope_host_variants(base_scope):
            if not scope or scope in seen:
                continue
            seen.add(scope)
            _append_prefix_scope_condition(parts, scope)
        for detail_scope in _iter_post_detail_scope_variants(base_scope):
            key = f"detail:{detail_scope}"
            if key in seen:
                continue
            seen.add(key)
            _append_detail_scope_condition(parts, detail_scope, base_scope)
    if not parts:
        return "", ""
    return "(" + " OR ".join(parts) + ")", first_scope


async def resolve_learn_list_id_for_contents(
    *,
    db_name: Optional[str],
    chat_bot_id: Optional[str],
    contents_url: Optional[Union[str, List[str]]],
) -> Optional[int]:
    """Find LEARN_LIST row matching frontend contents and return its id."""

    if not (db_name and str(db_name).strip() and chat_bot_id and str(chat_bot_id).strip()):
        return None
    candidates = _iter_contents_url_candidates(contents_url)
    if not candidates:
        return None

    placeholders = ", ".join(["%s"] * len(candidates))
    query = (
        f"SELECT id, `content` AS matched_content FROM `{_LEARN_LIST_TABLE}` "
        f"WHERE `chat_bot_id` = %s AND `content_type` = %s AND `content` IN ({placeholders}) "
        f"ORDER BY id DESC LIMIT 1"
    )
    try:
        rows = await maria_execute_query(
            query,
            (str(chat_bot_id).strip(), "url", *candidates),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        logger.debug(
            "[FileCrawlPosts][learn_list_scope] learn_list lookup failed | db=%s table=%s column=%s chat_bot_id=%s contents=%s err=%s",
            db_name,
            _LEARN_LIST_TABLE,
            "content",
            chat_bot_id,
            str(candidates[0] if candidates else "")[:200],
            exc,
        )
        return None

    if not rows:
        logger.debug(
            "[FileCrawlPosts][learn_list_scope] no matching learn_list row | db=%s table=%s column=%s chat_bot_id=%s candidates=%s",
            db_name,
            _LEARN_LIST_TABLE,
            "content",
            chat_bot_id,
            candidates[:3],
        )
        return None
    learn_list_id = _positive_int_or_none((rows[0] or {}).get("id"))
    if learn_list_id:
        logger.debug(
            "[FileCrawlPosts][learn_list_scope] resolved from learn_list | db=%s table=%s column=%s chat_bot_id=%s learn_list_id=%s content=%s",
            db_name,
            _LEARN_LIST_TABLE,
            "content",
            chat_bot_id,
            learn_list_id,
            str((rows[0] or {}).get("matched_content") or "")[:200],
        )
        logger.debug(
            "[START_URLS] learn_list_id matched | chat_bot_id=%s contents_url=%s learn_list_id=%s",
            chat_bot_id,
            str(candidates[0] if candidates else "")[:300],
            learn_list_id,
        )
    return learn_list_id


async def _resolve_learn_list_scope_condition(
    *,
    db_name: Optional[str],
    chat_bot_id: Optional[str],
    contents_url: Optional[Union[str, List[str]]],
    learn_list_id_scope: Optional[Union[int, str]],
    scope_by_contents_learn_list_id: bool,
) -> tuple[str, Optional[int], str]:
    learn_list_id = _positive_int_or_none(learn_list_id_scope)
    if learn_list_id is not None:
        logger.debug(
            "[FileCrawlPosts][learn_list_scope] using explicit learn_list_id | db=%s chat_bot_id=%s learn_list_id=%s",
            db_name,
            chat_bot_id,
            learn_list_id,
        )
    elif scope_by_contents_learn_list_id:
        learn_list_id = await resolve_learn_list_id_for_contents(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            contents_url=contents_url,
        )
    if learn_list_id is None:
        if scope_by_contents_learn_list_id:
            logger.debug(
                "[START_URLS] learn_list_id not found | exact contents scope returns no rows contents_url=%s",
                str(contents_url or "")[:300],
            )
            return "1 = 0", None, ""
        fallback_condition, fallback_scope = _build_url_scope_fallback_condition(contents_url)
        if fallback_condition:
            logger.debug(
                "[START_URLS] learn_list_id not found | fallback=url_scope contents_url=%s",
                fallback_scope[:300],
            )
            return fallback_condition, None, fallback_scope
        return "", None, ""
    return f"learn_list_id = {int(learn_list_id)}", learn_list_id, ""


def _row_value(row: Any, key: str) -> Any:
    if not isinstance(row, dict):
        return None
    if key in row:
        return row.get(key)
    lower = {str(k).lower(): v for k, v in row.items()}
    return lower.get(str(key or "").lower())


def _jsonish(raw: Any) -> Any:
    value = raw
    for _ in range(3):
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        if not isinstance(value, str):
            return value
        text = value.strip()
        if not text:
            return None
        if not (text.startswith("{") or text.startswith("[")):
            return text
        try:
            value = json.loads(text)
        except Exception:
            return text
    return value


def _dict_looks_like_attachment(value: Dict[str, Any]) -> bool:
    if any(k in value for k in ("href", "download_url", "downloadUrl", "file_url", "fileUrl", "downloadPath", "openPath")):
        return True
    raw_url = value.get("url") or value.get("link") or value.get("path")
    if raw_url is None:
        return False
    has_name = any(
        value.get(k)
        for k in (
            "name",
            "filename",
            "file_name",
            "fileName",
            "original_name",
            "org_file_nm",
            "user_file_nm",
        )
    )
    if has_name:
        return True
    try:
        u = str(raw_url or "").strip().lower()
    except Exception:
        u = ""
    return any(token in u for token in ("download", "filedown", "filedownload", "attach", "atchfile", "/file/"))


def _iter_attachment_payloads(value: Any, *, depth: int = 0) -> List[Any]:
    if value is None or depth > 4:
        return []
    value = _jsonish(value)
    if isinstance(value, str):
        text = value.strip()
        low = text.lower()
        if text and (low.startswith(("http://", "https://", "/")) or any(t in low for t in ("download", "filedown", "filedownload", "attach", "atchfile"))):
            return [{"href": text}]
        return []
    if isinstance(value, list):
        out: List[Any] = []
        for item in value:
            out.extend(_iter_attachment_payloads(item, depth=depth + 1))
        return out
    if not isinstance(value, dict):
        return []
    if _dict_looks_like_attachment(value):
        return [value]
    out = []
    for key in (
        "attachments",
        "direct_attachments",
        "attachment_list",
        "attachmentList",
        "files",
        "file_list",
        "fileList",
        "items",
        "data",
        "result",
        "results",
    ):
        if key in value:
            out.extend(_iter_attachment_payloads(value.get(key), depth=depth + 1))
    return out


def _normalize_direct_attachment_payload(payload: Any, *, base_url: str) -> Optional[Dict[str, str]]:
    payload = _jsonish(payload)
    if not isinstance(payload, dict):
        return None
    raw_href = (
        payload.get("href")
        or payload.get("url")
        or payload.get("download_url")
        or payload.get("downloadUrl")
        or payload.get("file_url")
        or payload.get("fileUrl")
        or payload.get("downloadPath")
        or payload.get("openPath")
        or payload.get("link")
        or payload.get("path")
    )
    href = normalize_attachment_href(str(raw_href or "").strip())
    if not href:
        return None
    href_l = href.lower()
    if href_l.startswith(("#", "mailto:", "tel:")):
        return None
    if href_l.startswith("javascript:"):
        return None
    full = urljoin(base_url, href)
    name = (
        payload.get("name")
        or payload.get("filename")
        or payload.get("file_name")
        or payload.get("fileName")
        or payload.get("original_name")
        or payload.get("org_file_nm")
        or payload.get("user_file_nm")
        or payload.get("title")
        or payload.get("text")
        or payload.get("label")
        or ""
    )
    out = {"href": full, "name": str(name or "").strip() or "attachment"}
    return out


def _extract_direct_attachments_from_row(row: Any, *, base_url: str) -> List[Dict[str, str]]:
    if not _direct_attachment_fast_path_enabled() or not isinstance(row, dict):
        return []
    seen: set[str] = set()
    out: List[Dict[str, str]] = []
    for col in _EXPLORATION_ATTACHMENT_META_COLUMNS:
        raw = _row_value(row, col)
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            continue
        for payload in _iter_attachment_payloads(raw):
            item = _normalize_direct_attachment_payload(payload, base_url=base_url)
            if not item:
                continue
            key = (
                canonicalize_attachment_url_for_learn_list(item.get("href") or "", base_url=base_url)
                or canonicalize_url_for_dedup(item.get("href") or "", base_url)
                or (item.get("href") or "").strip().lower()
            )
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(item)
    return out


def _extract_direct_author_info_from_row(row: Any) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    out: Dict[str, Any] = {}
    for col, dst in (
        ("content_author", "content_author"),
        ("author", "author"),
        ("department", "department"),
    ):
        val = _row_value(row, col)
        if val is not None and str(val).strip():
            out[dst] = str(val).strip()
    for col in _EXPLORATION_ATTACHMENT_META_COLUMNS:
        meta = _jsonish(_row_value(row, col))
        if not isinstance(meta, dict):
            continue
        nested = meta.get("author_info") if isinstance(meta.get("author_info"), dict) else meta
        if not isinstance(nested, dict):
            continue
        for src, dst in (
            ("content_author", "content_author"),
            ("author", "author"),
            ("department", "department"),
            ("author_kind", "author_kind"),
            ("author_raw", "author_raw"),
            ("department_raw", "department_raw"),
        ):
            val = nested.get(src)
            if val is not None and str(val).strip() and dst not in out:
                out[dst] = str(val).strip()
    return out


def _apply_direct_fast_path_fields(item: Dict[str, Any], row: Any, *, base_url: str) -> None:
    for src, dst in (
        ("cate1", "cate1"),
        ("cate2", "cate2"),
        ("category1", "cate1"),
        ("category2", "cate2"),
        ("board_cate1", "cate1"),
        ("board_cate2", "cate2"),
        ("board_cate1_name", "board_cate1_name"),
        ("board_cate2_name", "board_cate2_name"),
        ("store_cate1", "store_cate1"),
        ("store_cate2", "store_cate2"),
        ("assigned_cate1", "assigned_cate1"),
        ("assigned_cate2", "assigned_cate2"),
    ):
        val = _row_value(row, src)
        if val is not None and str(val).strip() and dst not in item:
            item[dst] = str(val).strip()

    attachments = _extract_direct_attachments_from_row(row, base_url=base_url)
    if attachments:
        item["attachments"] = attachments
        author_info = _extract_direct_author_info_from_row(row)
        if author_info:
            item["author_info"] = author_info
        for src, dst in (
            ("title", "title"),
            ("subject", "subject"),
            ("reg_date", "reg_date"),
            ("published_at", "reg_date"),
            ("post_date", "reg_date"),
        ):
            val = _row_value(row, src)
            if val is not None and str(val).strip() and dst not in item:
                item[dst] = str(val).strip()


def _should_fallback_to_legacy_exploration_condition(exc: Exception) -> bool:
    try:
        msg = str(exc or "").strip().lower()
    except Exception:
        msg = ""
    if not msg:
        return False
    if "merge_status" not in msg and "is_active" not in msg:
        return False
    return any(
        token in msg
        for token in (
            "unknown column",
            "no such column",
            "doesn't exist",
            "does not exist",
            "invalid column",
            "1054",
        )
    )


def file_crawl_learn_list_relax_content_domain_match() -> bool:
    """
    Return True when file crawl may match LEARN_LIST url_pattern rows by
    chat_bot_id/domain even when the row content does not exactly match the
    requested contents URL.
    """
    v = (os.getenv("FILE_CRAWL_LEARN_LIST_RELAX_CONTENT_MATCH") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _normalize_cate_codes_upper(c1: str, c2: str) -> Tuple[str, str]:
    """Normalize category codes used by LEARN_LIST/category mapping."""
    s1 = str(c1 or "").strip()
    s2 = str(c2 or "").strip()
    return (s1.upper(), s2.upper())


def _extract_base_domain(url_input: Union[str, List[str], None]) -> str:
    return extract_scope_host(_first_contents_url_value(url_input) or url_input)


def _url_host_matches_scope_domains(url: str, domains: List[str], *, path_prefix: str = "") -> bool:
    return url_matches_scope_identities(url, domains, path_prefix=path_prefix)


def _reference_structure_pattern_key(contents_url: Optional[Union[str, List[str]]]) -> str:
    target = contents_url[0] if isinstance(contents_url, list) and contents_url else contents_url
    if not isinstance(target, str) or not target.strip():
        return ""
    if not url_structure_pattern_has_variable(target):
        return ""
    return url_structure_pattern_key(target)


def _url_matches_reference_structure_pattern(url: str, reference_pattern_key: str) -> bool:
    if not reference_pattern_key:
        return True
    return url_structure_pattern_key(url) == reference_pattern_key
def _first_contents_url_value(contents_url: Optional[Union[str, List[str]]]) -> str:
    if isinstance(contents_url, list):
        for item in contents_url:
            text = _reference_url_value(item)
            if text:
                return text
        return ""
    return _reference_url_value(contents_url)


_REFERENCE_QUERY_IGNORED_KEYS = {
    "page",
    "pageindex",
    "pageno",
    "page_no",
    "currentpage",
    "searchkeyword",
    "keyword",
    "searchword",
    "searchtext",
    "searchcondition",
    "sort",
    "order",
}


def _reference_query_pairs(contents_url: Optional[Union[str, List[str]]]) -> List[Tuple[str, str]]:
    target = _first_contents_url_value(contents_url)
    if not target:
        return []
    try:
        parsed = urlparse(ensure_url_scheme(target))
    except Exception:
        return []
    pairs: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for key, value in parse_qsl(parsed.query or "", keep_blank_values=True):
        key_text = str(key or "").strip().lower()
        if not key_text or key_text in _REFERENCE_QUERY_IGNORED_KEYS:
            continue
        pair = (key_text, str(value or ""))
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def _url_matches_reference_query_pairs(url: str, reference_query_pairs: List[Tuple[str, str]]) -> bool:
    if not reference_query_pairs:
        return True
    try:
        parsed = urlparse(ensure_url_scheme(str(url or "").strip()))
    except Exception:
        return False
    row_values: Dict[str, set[str]] = {}
    for key, value in parse_qsl(parsed.query or "", keep_blank_values=True):
        key_text = str(key or "").strip().lower()
        if not key_text:
            continue
        row_values.setdefault(key_text, set()).add(str(value or ""))
    for key, value in reference_query_pairs:
        if value not in row_values.get(key, set()):
            return False
    return True


def _learn_list_col(row: Any, col_name: str) -> Any:
    if not isinstance(row, dict):
        return None
    if col_name in row:
        v = row.get(col_name)
        if v is not None and (not isinstance(v, str) or v.strip()):
            return v
    lower = {str(k).lower(): v for k, v in row.items()}
    v = lower.get(col_name.lower())
    if v is not None and (not isinstance(v, str) or v.strip()):
        return v
    return None


def _normalize_rule_sequence(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, (str, bytes)):
        s = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        s = (s or "").strip()
        return [s] if s else []
    if isinstance(raw, dict):
        return [raw]
    try:
        return list(raw) if isinstance(raw, (tuple, set)) else [raw]
    except Exception:
        return []
def _get_rule_entries(filters_obj: Optional[Dict[str, Any]]) -> List[Any]:
    if not isinstance(filters_obj, dict):
        return []
    return _normalize_rule_sequence(filters_obj.get("rules"))


def _summarize_rule_entries(filters_obj: Optional[Dict[str, Any]], *, limit: int = 5) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for item in _get_rule_entries(filters_obj)[: max(0, int(limit))]:
        try:
            if isinstance(item, dict):
                summary.append(
                    {
                        "url": str(item.get("url") or "").strip(),
                        "query_keys": list(item.get("query_keys") or item.get("query") or []),
                        "cate1": str(item.get("cate1") or "").strip(),
                        "cate2": str(item.get("cate2") or "").strip(),
                    }
                )
            else:
                text = str(item or "").strip()
                if text:
                    summary.append({"url": text, "query_keys": [], "cate1": "", "cate2": ""})
        except Exception:
            continue
    return summary


def _pattern_object_has_nonempty_rules(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    if (data.get("mode") or "").strip().lower() == "exclude":
        return False
    for item in _get_rule_entries(data):
        if isinstance(item, dict):
            u = item.get("url")
            if u is not None and str(u).strip():
                return True
        elif isinstance(item, str) and item.strip():
            return True
        elif item is not None and str(item).strip():
            return True
    return False


def _parse_filter_cell(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    try:
        if isinstance(raw, dict):
            return raw if _pattern_object_has_nonempty_rules(raw) else None
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return None
            data = json.loads(s)
            return data if isinstance(data, dict) and _pattern_object_has_nonempty_rules(data) else None
    except Exception:
        return None
    return None


def _learn_list_row_id(row: Any) -> int:
    if not isinstance(row, dict):
        return 0
    for k in ("id", "ID"):
        if k in row and row[k] is not None:
            try:
                return int(row[k])
            except (TypeError, ValueError):
                pass
    lower = {str(k).lower(): v for k, v in row.items()}
    v = lower.get("id")
    if v is not None:
        try:
            return int(v)
        except (TypeError, ValueError):
            pass
    return 0


def _normalize_contents_url_for_match(raw_url: Any) -> str:
    try:
        txt = ensure_url_scheme(str(raw_url or "").strip())
    except Exception:
        txt = str(raw_url or "").strip()
    if not txt:
        return ""
    try:
        return canonicalize_url_for_dedup(txt) or txt
    except Exception:
        return txt


def _valid_file_crawl_contents_url(raw_url: Any) -> Optional[str]:
    try:
        text = str(raw_url or "").strip()
    except Exception:
        return None
    if not text or text.lower() in {"http", "https", "http://", "https://"}:
        return None
    try:
        parsed = urlparse(ensure_url_scheme(text))
        if not parsed.netloc or "." not in parsed.netloc:
            return None
    except Exception:
        return None
    return text


def _first_path_segment_from_pattern(pattern: Any) -> Optional[str]:
    try:
        raw = str(pattern or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return None
    if "://" in raw or raw.startswith("//"):
        try:
            path = urlparse(ensure_url_scheme(raw)).path or ""
        except Exception:
            path = ""
    else:
        if "/" not in raw and "." not in raw:
            return None
        path = raw.split("?", 1)[0].strip()
    parts = [part for part in str(path or "").split("/") if part]
    if not parts:
        return None
    return str(parts[0] or "").strip().lower() or None


def _extract_rule_urls(filters_obj: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(filters_obj, dict):
        return []
    out: List[str] = []
    for item in _get_rule_entries(filters_obj):
        try:
            if isinstance(item, dict):
                value = str(item.get("url") or "").strip()
            else:
                value = str(item or "").strip()
        except Exception:
            value = ""
        if value:
            out.append(value)
    return out


def _resolve_scope_path_prefix_from_patterns(
    contents_url: Optional[Union[str, List[str]]],
    pattern_keywords: Optional[List[str]],
) -> str:
    reference_url = _first_contents_url_value(contents_url)
    base_prefix = extract_service_scope_path_prefix(reference_url) or extract_scope_path_prefix(reference_url)
    if not base_prefix:
        return ""
    base_parts = [part for part in str(base_prefix or "").split("/") if part]
    if not base_parts:
        return ""
    base_first = str(base_parts[0] or "").strip().lower()
    keywords = [str(k or "").strip() for k in (pattern_keywords or []) if str(k or "").strip()]
    if not keywords:
        return base_prefix
    for keyword in keywords:
        seg = _first_path_segment_from_pattern(keyword)
        if not seg:
            continue
        if seg != base_first:
            return ""
    return base_prefix


def _resolve_file_crawl_scope(
    *,
    target_domains: Optional[List[str]],
    contents_url: Optional[Union[str, List[str]]],
    use_rule_scope: bool,
    rule_patterns: Optional[List[str]] = None,
    explicit_path_prefix: Optional[str] = None,
) -> Tuple[List[str], str]:
    """
    Resolve the domain and path scope used when selecting file crawl start URLs.

    If category include rules are present, prefer their URL scope. Otherwise,
    use explicit target domains or derive the service scope from contents_url.
    """
    final_domains: List[str] = extract_scope_identities(target_domains)
    explicit_scope_path_prefix = normalize_scope_path_prefix(explicit_path_prefix)
    scope_path_prefix = explicit_scope_path_prefix
    if explicit_scope_path_prefix:
        if not final_domains and contents_url:
            base_domain = _extract_base_domain(contents_url)
            if base_domain:
                final_domains = [base_domain]
    elif use_rule_scope:
        if not final_domains and contents_url:
            base_domain = _extract_base_domain(contents_url)
            if base_domain:
                final_domains = [base_domain]
        scope_path_prefix = _resolve_scope_path_prefix_from_patterns(contents_url, rule_patterns)
    elif contents_url:
        base_domain = _extract_base_domain(contents_url)
        if base_domain and not final_domains:
            final_domains = [base_domain]
        scope_path_prefix = extract_service_scope_path_prefix(_first_contents_url_value(contents_url))
    return final_domains, scope_path_prefix


async def fetch_category_rule_object_for_file_crawl(
    *,
    db_name: str,
    chat_bot_id: str,
    contents_url: Optional[Union[str, List[str]]] = None,
    method: str = "period",
) -> Optional[Dict[str, Any]]:
    """Legacy compatibility shim. File crawling no longer reads url_pattern rules."""
    return None


async def fetch_learn_list_url_pattern_object_for_file_crawl(
    *,
    db_name: str,
    chat_bot_id: str,
    contents_url: Optional[Union[str, List[str]]] = None,
    method: str = "period",
) -> Optional[Dict[str, Any]]:
    """Legacy compatibility shim. File crawling no longer uses url_pattern rules."""
    return None


async def stream_post_urls_for_file_crawl(
    db_name: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    contents_url: Optional[Union[str, List[str]]] = None,
    chat_bot_id: Optional[str] = None,
    batch_size: int = 200,
    method: str = "period",
    target_date: Optional[List[str]] = None,
    exploration_date_filter_enabled: bool = False,
    scope_path_prefix: Optional[str] = None,
    start_urls_order: Optional[str] = None,
    use_category_rules: bool = True,
    dedupe_urls: bool = True,
    learn_list_id_scope: Optional[Union[int, str]] = None,
    scope_by_contents_learn_list_id: bool = False,
) -> AsyncIterable[List[Dict[str, Any]]]:
    """
    Stream exploration post URLs for file crawling.

    Category include rules narrow the set when available. Without include rules,
    the function falls back to scoped exploration post rows. Each item is a dict
    with at least {"url", "type"}; type usually remains "post" unless category
    matching metadata is attached.
    """
    has_db_name = bool(db_name and str(db_name).strip())
    has_chat_bot_id = bool(chat_bot_id and str(chat_bot_id).strip())
    has_learn_list_scope = learn_list_id_scope is not None or bool(scope_by_contents_learn_list_id)
    if not has_db_name or (not has_chat_bot_id and not has_learn_list_scope):
        logger.warning(
            "[FileCrawlPosts] db_name ?????chat_bot_id/learn_list scope ?????????대첉?????????????start_urls ???耀붾굝???????| db=%s bot=%s learn_scope=%s",
            db_name,
            chat_bot_id,
            has_learn_list_scope,
        )
        return
    use_category_rules = False
    exploration_date_filter_enabled = _coerce_bool_flag(
        exploration_date_filter_enabled,
        default=False,
    )


    category_rule_obj: Optional[Dict[str, Any]] = None

    use_url_rule_scope = False
    if not use_url_rule_scope:
        logger.debug(
            "[FileCrawlPosts] no CATEGORY url/query rule found; falling back to scoped exploration posts | db=%s chat_bot_id=%s",
            db_name,
            chat_bot_id,
        )
    final_domains, scope_path_prefix = _resolve_file_crawl_scope(
        target_domains=target_domains,
        contents_url=contents_url,
        use_rule_scope=use_url_rule_scope,
        rule_patterns=_extract_rule_urls(category_rule_obj),
        explicit_path_prefix=scope_path_prefix,
    )
    # Seed candidates stay inside the same service scope, not only the same host.
    effective_path_prefix = normalize_scope_path_prefix(scope_path_prefix)
    requested_scope_path_prefix = normalize_scope_path_prefix(scope_path_prefix)
    allow_broad_scope_fallback = False
    logger.debug(
        "[START_URLS_RULE_TRACE][file][paged] init | db=%s chat_bot_id=%s contents_url=%s rule_count=%s rule_scope=%s final_domains=%s path_prefix=%s requested_path_prefix=%s rule_sample=%s",
        db_name,
        chat_bot_id,
        (str(contents_url[0] if isinstance(contents_url, list) and contents_url else contents_url or "")[:180]),
        len(_get_rule_entries(category_rule_obj)) if isinstance(category_rule_obj, dict) else 0,
        use_url_rule_scope,
        final_domains,
        effective_path_prefix,
        requested_scope_path_prefix,
        _summarize_rule_entries(category_rule_obj),
    )
    logger.debug(
        "[START_URLS_RULE_TRACE][file] init | db=%s chat_bot_id=%s contents_url=%s rule_count=%s rule_scope=%s final_domains=%s path_prefix=%s requested_path_prefix=%s rule_sample=%s",
        db_name,
        chat_bot_id,
        (str(contents_url[0] if isinstance(contents_url, list) and contents_url else contents_url or "")[:180]),
        len(_get_rule_entries(category_rule_obj)) if isinstance(category_rule_obj, dict) else 0,
        use_url_rule_scope,
        final_domains,
        effective_path_prefix,
        requested_scope_path_prefix,
        _summarize_rule_entries(category_rule_obj),
    )
    if False and scope_path_prefix:
        logger.debug(
            "[FileCrawlPosts][paged] include rule missing; host fallback suppressed path_prefix | db=%s chat_bot_id=%s host_scope=%s original_path_prefix=%s",
            db_name,
            chat_bot_id,
            final_domains,
            scope_path_prefix,
        )
    if False and scope_path_prefix and not effective_path_prefix:
        logger.debug(
            "[FileCrawlPosts] include rule missing; host fallback suppressed path_prefix | db=%s chat_bot_id=%s host_scope=%s original_path_prefix=%s",
            db_name,
            chat_bot_id,
            final_domains,
            scope_path_prefix,
        )

    if scope_path_prefix:
        logger.debug(
            "[FileCrawlPosts] start_urls url_rule applied | rule_scope=%s db=%s chat_bot_id=%s host_scope=%s original_path_prefix=%s",
            use_url_rule_scope,
            db_name,
            chat_bot_id,
            final_domains,
            scope_path_prefix,
        )

    table_name = EXPLORATION_TABLE
    exploration_date_condition = ""
    start_date_iso, end_date_iso = _parse_target_date_range(target_date)
    if not exploration_date_filter_enabled:
        logger.debug(
            "[START_URLS_DATE_FILTER][file] off | db=%s chat_bot_id=%s target_date=%s",
            db_name,
            chat_bot_id,
            target_date,
        )
    elif not (start_date_iso and end_date_iso):
        logger.warning(
            "[START_URLS_DATE_FILTER][file] on_but_invalid_target_date | db=%s chat_bot_id=%s target_date=%s",
            db_name,
            chat_bot_id,
            target_date,
        )
    elif exploration_date_filter_enabled and start_date_iso and end_date_iso:
        exploration_date_column = await _resolve_exploration_date_column(db_name, table_name)
        exploration_date_condition = _build_exploration_date_range_condition(
            str(exploration_date_column or "").strip(),
            start_date_iso=start_date_iso,
            end_date_iso=end_date_iso,
        )
        if exploration_date_condition:
            logger.debug(
                "[START_URLS_DATE_FILTER][file] applied | db=%s chat_bot_id=%s column=%s start=%s end=%s",
                db_name,
                chat_bot_id,
                exploration_date_column,
                start_date_iso,
                end_date_iso,
            )
        else:
            logger.warning(
                "[START_URLS_DATE_FILTER][file] skipped | db=%s chat_bot_id=%s start=%s end=%s column=%s",
                db_name,
                chat_bot_id,
                start_date_iso,
                end_date_iso,
                exploration_date_column,
            )
    learn_list_extra_condition, resolved_learn_list_id, fallback_url_scope = await _resolve_learn_list_scope_condition(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        contents_url=contents_url,
        learn_list_id_scope=learn_list_id_scope,
        scope_by_contents_learn_list_id=scope_by_contents_learn_list_id,
    )
    if resolved_learn_list_id:
        logger.debug(
            "[FileCrawlPosts][learn_list_scope] applying exact exploration filter | db=%s chat_bot_id=%s learn_list_id=%s",
            db_name,
            chat_bot_id,
            resolved_learn_list_id,
        )
    elif has_learn_list_scope and not fallback_url_scope:
        logger.debug(
            "[FileCrawlPosts][learn_list_scope] no resolved learn_list_id; exact contents scope returns no rows | db=%s chat_bot_id=%s contents=%s explicit_scope=%s",
            db_name,
            chat_bot_id,
            str(contents_url or "")[:200],
            learn_list_id_scope,
        )
    fallback_scope_active = bool(fallback_url_scope and learn_list_extra_condition)
    query_conditions = build_exploration_conditions(
        ExplorationQuerySpec(
            chat_bot_id=chat_bot_id,
            target_domains=[] if fallback_scope_active else list(final_domains or []),
            path_prefix="" if fallback_scope_active else effective_path_prefix,
            include_empty_type=use_category_rules,
            dedupe_urls=dedupe_urls,
            date_condition=exploration_date_condition,
            extra_condition=learn_list_extra_condition,
        )
    )
    if fallback_scope_active:
        logger.debug(
            "[START_URLS] fallback url_scope overrides domain/path scope | db=%s chat_bot_id=%s ignored_domains=%s ignored_path_prefix=%s scope=%s",
            db_name,
            chat_bot_id,
            final_domains,
            effective_path_prefix,
            fallback_url_scope[:300],
        )
    condition = query_conditions.condition
    legacy_condition = query_conditions.legacy_condition
    base_condition = query_conditions.base_condition
    legacy_base_condition = query_conditions.legacy_base_condition
    scope_condition = query_conditions.scope_condition
    exploration_cols = await _load_exploration_columns(str(db_name), table_name)
    select_columns = _build_exploration_select_columns(exploration_cols, include_id=False)

    try:
        rows = await maria_select_data(table_name, columns=select_columns, condition=condition, dbname=db_name)
    except Exception as e:
        if _should_fallback_to_legacy_exploration_condition(e):
            logger.warning(
                "[FileCrawlPosts] exploration filter columns unavailable -> fallback to legacy condition | db=%s chat_bot_id=%s err=%s",
                db_name,
                chat_bot_id,
                e,
            )
            try:
                rows = await maria_select_data(
                    table_name,
                    columns=select_columns,
                    condition=legacy_condition,
                    dbname=db_name,
                )
                condition = legacy_condition
                base_condition = legacy_base_condition
            except Exception as legacy_exc:
                logger.error("[FileCrawlPosts] legacy fallback DB query failed | dbname=%s err=%s", db_name, legacy_exc)
                return
        else:
            logger.error("[FileCrawlPosts] DB query failed | dbname=%s err=%s", db_name, e)
            return

    if fallback_url_scope:
        logger.debug(
            "[START_URLS] fallback scope result | matched_rows=%s scope=%s",
            len(rows or []),
            fallback_url_scope[:300],
        )

    if not rows and scope_condition and allow_broad_scope_fallback:
        logger.warning(
            "[FileCrawlPosts] strict scope SQL returned 0 rows -> broad query fallback | db=%s chat_bot_id=%s domains=%s path_prefix=%s",
            db_name,
            chat_bot_id,
            final_domains,
            effective_path_prefix,
        )
        try:
            rows = await maria_select_data(table_name, columns=select_columns, condition=base_condition, dbname=db_name)
        except Exception as e:
            logger.error("[FileCrawlPosts] broad fallback DB query failed | dbname=%s err=%s", db_name, e)
            return

    if not rows:
        logger.warning(
            "[FileCrawlPosts] no post rows | dbname=%s chat_bot_id=%s domains=%s",
            db_name,
            chat_bot_id,
            final_domains,
        )
        return

    try:
        n_exploration = len(rows)
    except Exception:
        n_exploration = -1
    logger.debug(
        "[FileCrawlPosts] ASADAL_CRAWLING_EXPLORATION post rows selected=%s | db=%s chat_bot_id=%s domains=%s "
        "(category include rules may narrow start_urls)",
        n_exploration,
        db_name,
        chat_bot_id,
        final_domains,
    )

    batch: List[Dict[str, Any]] = []
    seen: set[str] = set()
    scanned_count = 0
    deduped_count = 0
    domain_skipped_count = 0
    missing_query_skipped_count = 0
    unmatched_count = 0
    matched_count = 0
    domain_skipped_samples: List[str] = []
    missing_query_skipped_samples: List[str] = []
    unmatched_samples: List[str] = []
    matched_samples: List[Dict[str, Any]] = []
    reference_pattern_key = _reference_structure_pattern_key(contents_url)
    reference_query_pairs = _reference_query_pairs(contents_url)
    if reference_query_pairs and not resolved_learn_list_id:
        logger.info(
            "[FileCrawlPosts][query_scope] applying contents query pair filter | db=%s chat_bot_id=%s pairs=%s",
            db_name,
            chat_bot_id,
            reference_query_pairs,
        )
    if reference_pattern_key and not resolved_learn_list_id:
        pattern_index = group_urls_by_structure_pattern(rows if isinstance(rows, list) else [])
        candidate_rows = list(pattern_index.get(reference_pattern_key) or [])
        logger.debug(
            "[FileCrawlPosts][pattern] memory grouping applied | db=%s chat_bot_id=%s candidates=%s groups=%s matched=%s pattern=%s",
            db_name,
            chat_bot_id,
            len(rows) if isinstance(rows, list) else -1,
            len(pattern_index),
            len(candidate_rows),
            reference_pattern_key,
        )
        rows = candidate_rows
    for r in rows:
        u = r.get("url") if isinstance(r, dict) else None
        row_type = str(r.get("type") or "").strip().lower() if isinstance(r, dict) else ""
        if not u:
            continue
        try:
            url = ensure_url_scheme(str(u).strip())
        except Exception:
            continue
        if not url:
            continue
        scanned_count += 1
        # Use request_url for crawling; use normalized_key only for comparison/dedupe.
        request_url = url
        normalized_key = (canonicalize_url_for_dedup(request_url) or request_url.strip() or "").strip()
        missing_query_reason = _missing_required_file_detail_query_reason(request_url)
        if missing_query_reason:
            missing_query_skipped_count += 1
            if len(missing_query_skipped_samples) < 5:
                missing_query_skipped_samples.append(request_url[:200])
            logger.warning(
                "[FileUrlTrace][file_crawl_posts.detail_query_missing] db=%s chat_bot_id=%s row_type=%s reason=%s action=skip raw_url=%s request_url=%s normalized_key=%s",
                db_name,
                chat_bot_id,
                row_type or "-",
                missing_query_reason,
                str(u or "").strip()[:300],
                request_url[:300],
                normalized_key[:300],
            )
            continue
        dedupe_key = ""
        if dedupe_urls:
            dedupe_key = normalized_key
            if not dedupe_key or dedupe_key in seen:
                deduped_count += 1
                continue
        if (not resolved_learn_list_id) and final_domains and not _url_host_matches_scope_domains(url, final_domains, path_prefix=effective_path_prefix):
            domain_skipped_count += 1
            if len(domain_skipped_samples) < 5:
                domain_skipped_samples.append(url[:200])
            logger.debug(
                "[FileCrawlPosts] skipped URL outside resolved file crawl scope | url=%s scope=%s",
                url[:200],
                final_domains,
            )
            continue
        if (not resolved_learn_list_id) and not _url_matches_reference_query_pairs(url, reference_query_pairs):
            unmatched_count += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(url[:200])
            continue
        if (not resolved_learn_list_id) and not _url_matches_reference_structure_pattern(url, reference_pattern_key):
            unmatched_count += 1
            if len(unmatched_samples) < 5:
                unmatched_samples.append(url[:200])
            continue
        item_type = row_type or "post"
        temporary_post_match = False
        if dedupe_urls:
            seen.add(dedupe_key)
        item = {"url": url, "type": item_type}
        if missing_query_reason:
            item["selection_warning"] = missing_query_reason
        _apply_direct_fast_path_fields(item, r, base_url=url)
        if temporary_post_match:
            item["force_relearn"] = True
            item["temporary_post_match"] = True
            item["disable_playwright"] = True
        batch.append(item)
        matched_count += 1
        if len(matched_samples) < 5:
            matched_samples.append({"url": url[:200], "type": item_type})
        if len(batch) >= max(1, int(batch_size or 200)):
            yield batch
            batch = []
    logger.debug(
        "[START_URLS_RULE_TRACE][file] emit summary | db=%s chat_bot_id=%s scanned=%s deduped=%s domain_skipped=%s missing_query_skipped=%s unmatched=%s matched=%s emitted=%s sample_matched=%s sample_unmatched=%s sample_domain_skipped=%s sample_missing_query=%s",
        db_name,
        chat_bot_id,
        scanned_count,
        deduped_count,
        domain_skipped_count,
        missing_query_skipped_count,
        unmatched_count,
        matched_count,
        matched_count,
        matched_samples,
        unmatched_samples,
        domain_skipped_samples,
        missing_query_skipped_samples,
    )
    if batch:
        yield batch


async def load_file_crawl_post_url_strings(
    *,
    db_name: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    contents_url: Optional[Union[str, List[str]]] = None,
    chat_bot_id: Optional[str] = None,
    method: str = "period",
    target_date: Optional[List[str]] = None,
    exploration_date_filter_enabled: bool = False,
    scope_path_prefix: Optional[str] = None,
    start_urls_order: Optional[str] = None,
    use_category_rules: bool = True,
    dedupe_urls: bool = True,
    limit: Optional[int] = None,
    learn_list_id_scope: Optional[Union[int, str]] = None,
    scope_by_contents_learn_list_id: bool = False,
) -> List[Dict[str, Any]]:
    """Load file crawl start URLs as dicts with url/type metadata."""
    out: List[Dict[str, Any]] = []
    try:
        max_items = int(limit or 0)
    except Exception:
        max_items = 0
    async for chunk in stream_post_urls_for_file_crawl(
        db_name=db_name,
        target_domains=target_domains,
        contents_url=contents_url,
        chat_bot_id=chat_bot_id,
        batch_size=200,
        method=method,
        target_date=target_date,
        exploration_date_filter_enabled=exploration_date_filter_enabled,
        scope_path_prefix=scope_path_prefix,
        start_urls_order=start_urls_order,
        use_category_rules=use_category_rules,
        dedupe_urls=dedupe_urls,
        learn_list_id_scope=learn_list_id_scope,
        scope_by_contents_learn_list_id=scope_by_contents_learn_list_id,
    ):
        if max_items > 0:
            remaining = max_items - len(out)
            if remaining <= 0:
                break
            out.extend(list(chunk or [])[:remaining])
            if len(out) >= max_items:
                break
        else:
            out.extend(chunk)
    return out


async def count_file_crawl_post_urls(
    *,
    db_name: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    contents_url: Optional[Union[str, List[str]]] = None,
    chat_bot_id: Optional[str] = None,
    method: str = "period",
    target_date: Optional[List[str]] = None,
    exploration_date_filter_enabled: bool = False,
    scope_path_prefix: Optional[str] = None,
    start_urls_order: Optional[str] = None,
    use_category_rules: bool = True,
    dedupe_urls: bool = True,
    learn_list_id_scope: Optional[Union[int, str]] = None,
    scope_by_contents_learn_list_id: bool = False,
) -> int:
    """Count post URLs that would be streamed for file crawling."""
    return await count_file_crawl_post_urls_paged(
        db_name=db_name,
        target_domains=target_domains,
        contents_url=contents_url,
        chat_bot_id=chat_bot_id,
        method=method,
        target_date=target_date,
        exploration_date_filter_enabled=exploration_date_filter_enabled,
        scope_path_prefix=scope_path_prefix,
        start_urls_order=start_urls_order,
        use_category_rules=use_category_rules,
        dedupe_urls=dedupe_urls,
        learn_list_id_scope=learn_list_id_scope,
        scope_by_contents_learn_list_id=scope_by_contents_learn_list_id,
    )


async def stream_post_urls_for_file_crawl_paged(
    db_name: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    contents_url: Optional[Union[str, List[str]]] = None,
    chat_bot_id: Optional[str] = None,
    batch_size: int = 200,
    method: str = "period",
    target_date: Optional[List[str]] = None,
    exploration_date_filter_enabled: bool = False,
    scope_path_prefix: Optional[str] = None,
    start_urls_order: Optional[str] = None,
    use_category_rules: bool = True,
    dedupe_urls: bool = True,
    learn_list_id_scope: Optional[Union[int, str]] = None,
    scope_by_contents_learn_list_id: bool = False,
) -> AsyncIterable[List[Dict[str, Any]]]:
    """
    Stream exploration post URLs in pages for large file crawl jobs.

    Paging avoids loading all matching rows into memory and keeps the count path
    aligned with the stream path.
    """
    has_db_name = bool(db_name and str(db_name).strip())
    has_chat_bot_id = bool(chat_bot_id and str(chat_bot_id).strip())
    has_learn_list_scope = learn_list_id_scope is not None or bool(scope_by_contents_learn_list_id)
    if not has_db_name or (not has_chat_bot_id and not has_learn_list_scope):
        logger.warning(
            "[FileCrawlPosts][paged] db_name or chat_bot_id/learn_list scope missing; skip stream | db=%s bot=%s learn_scope=%s",
            db_name,
            chat_bot_id,
            has_learn_list_scope,
        )
        return
    use_category_rules = False
    exploration_date_filter_enabled = _coerce_bool_flag(
        exploration_date_filter_enabled,
        default=False,
    )


    category_rule_obj: Optional[Dict[str, Any]] = None
    if False and use_category_rules:
        try:
            category_rule_obj = await fetch_category_rule_object_for_file_crawl(
                db_name=str(db_name),
                chat_bot_id=str(chat_bot_id).strip(),
                contents_url=contents_url,
                method=method,
            )
        except Exception as ex:
            logger.warning("[FileCrawlPosts][paged][cate] CATEGORY rule load failed | %s", ex)
    else:
        logger.debug(
            "[FileCrawlPosts][paged][cate] CATEGORY rule load skipped by request mode | db=%s chat_bot_id=%s",
            db_name,
            chat_bot_id,
        )

    use_url_rule_scope = False
    final_domains, scope_path_prefix = _resolve_file_crawl_scope(
        target_domains=target_domains,
        contents_url=contents_url,
        use_rule_scope=use_url_rule_scope,
        rule_patterns=_extract_rule_urls(category_rule_obj),
        explicit_path_prefix=scope_path_prefix,
    )
    # Seed candidates stay inside the same service scope, not only the same host.
    effective_path_prefix = normalize_scope_path_prefix(scope_path_prefix)
    requested_scope_path_prefix = normalize_scope_path_prefix(scope_path_prefix)
    allow_broad_scope_fallback = False

    table_name = EXPLORATION_TABLE
    exploration_date_condition = ""
    start_date_iso, end_date_iso = _parse_target_date_range(target_date)
    if not exploration_date_filter_enabled:
        logger.debug(
            "[START_URLS_DATE_FILTER][file][paged] off | db=%s chat_bot_id=%s target_date=%s",
            db_name,
            chat_bot_id,
            target_date,
        )
    elif not (start_date_iso and end_date_iso):
        logger.warning(
            "[START_URLS_DATE_FILTER][file][paged] on_but_invalid_target_date | db=%s chat_bot_id=%s target_date=%s",
            db_name,
            chat_bot_id,
            target_date,
        )
    elif exploration_date_filter_enabled and start_date_iso and end_date_iso:
        exploration_date_column = await _resolve_exploration_date_column(db_name, table_name)
        exploration_date_condition = _build_exploration_date_range_condition(
            str(exploration_date_column or "").strip(),
            start_date_iso=start_date_iso,
            end_date_iso=end_date_iso,
        )
        if exploration_date_condition:
            logger.debug(
                "[START_URLS_DATE_FILTER][file][paged] applied | db=%s chat_bot_id=%s column=%s start=%s end=%s",
                db_name,
                chat_bot_id,
                exploration_date_column,
                start_date_iso,
                end_date_iso,
            )
        else:
            logger.warning(
                "[START_URLS_DATE_FILTER][file][paged] skipped | db=%s chat_bot_id=%s start=%s end=%s column=%s",
                db_name,
                chat_bot_id,
                start_date_iso,
                end_date_iso,
                exploration_date_column,
            )
    learn_list_extra_condition, resolved_learn_list_id, fallback_url_scope = await _resolve_learn_list_scope_condition(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        contents_url=contents_url,
        learn_list_id_scope=learn_list_id_scope,
        scope_by_contents_learn_list_id=scope_by_contents_learn_list_id,
    )
    if resolved_learn_list_id:
        logger.info(
            "[FileCrawlPosts][paged][learn_list_scope] applying exact exploration filter | db=%s chat_bot_id=%s learn_list_id=%s",
            db_name,
            chat_bot_id,
            resolved_learn_list_id,
        )
    elif has_learn_list_scope and not fallback_url_scope:
        logger.debug(
            "[FileCrawlPosts][paged][learn_list_scope] no resolved learn_list_id; exact contents scope returns no rows | db=%s chat_bot_id=%s contents=%s explicit_scope=%s",
            db_name,
            chat_bot_id,
            str(contents_url or "")[:200],
            learn_list_id_scope,
        )
    fallback_scope_active = bool(fallback_url_scope and learn_list_extra_condition)
    query_conditions = build_exploration_conditions(
        ExplorationQuerySpec(
            chat_bot_id=chat_bot_id,
            target_domains=[] if fallback_scope_active else list(final_domains or []),
            path_prefix="" if fallback_scope_active else effective_path_prefix,
            include_empty_type=use_category_rules,
            dedupe_urls=dedupe_urls,
            require_active=True,
            date_condition=exploration_date_condition,
            extra_condition=learn_list_extra_condition,
        )
    )
    if fallback_scope_active:
        logger.debug(
            "[START_URLS] fallback url_scope overrides domain/path scope | db=%s chat_bot_id=%s ignored_domains=%s ignored_path_prefix=%s scope=%s",
            db_name,
            chat_bot_id,
            final_domains,
            effective_path_prefix,
            fallback_url_scope[:300],
        )
    condition = query_conditions.condition
    legacy_condition = query_conditions.legacy_condition
    base_condition = query_conditions.base_condition
    legacy_base_condition = query_conditions.legacy_base_condition
    scope_condition = query_conditions.scope_condition
    page_size = _safe_positive_int_env(
        "FILE_CRAWL_EXPLORATION_STREAM_PAGE_SIZE",
        max(int(batch_size or 200), 200),
        minimum=50,
        maximum=5000,
    )
    order_raw = str(start_urls_order or "").strip().lower()
    reverse_order = order_raw in {"reverse", "desc", "backward", "backwards", "from_back", "back"}
    shuffle_order = order_raw in {"shuffle", "random", "rand", "randomize", "mixed"}
    order_by = "id DESC" if reverse_order else "id ASC"
    page_comparator = "id < %s" if reverse_order else "id > %s"
    initial_reverse_cursor = 9223372036854775807
    page_cursor = initial_reverse_cursor if reverse_order else 0
    logger.debug(
        "[FileCrawlPosts][paged] start | db=%s chat_bot_id=%s domains=%s path_prefix=%s effective_path_prefix=%s requested_path_prefix=%s page_size=%s batch_size=%s rule_scope=%s order=%s",
        db_name,
        chat_bot_id,
        final_domains,
        scope_path_prefix,
        effective_path_prefix,
        requested_scope_path_prefix,
        page_size,
        batch_size,
        use_url_rule_scope,
        "shuffle" if shuffle_order else ("reverse" if reverse_order else "forward"),
    )

    batch: List[Dict[str, Any]] = []
    seen: set[str] = set()
    seen_any_row = False
    scanned_count = 0
    deduped_count = 0
    domain_skipped_count = 0
    missing_query_skipped_count = 0
    unmatched_count = 0
    matched_count = 0
    page_row_count = 0
    domain_skipped_samples: List[str] = []
    missing_query_skipped_samples: List[str] = []
    unmatched_samples: List[str] = []
    matched_samples: List[Dict[str, Any]] = []
    reference_pattern_key = _reference_structure_pattern_key(contents_url)
    reference_query_pairs = _reference_query_pairs(contents_url)
    if reference_query_pairs and not resolved_learn_list_id:
        logger.info(
            "[FileCrawlPosts][paged][query_scope] applying contents query pair filter | db=%s chat_bot_id=%s pairs=%s",
            db_name,
            chat_bot_id,
            reference_query_pairs,
        )
    if reference_pattern_key and not resolved_learn_list_id:
        logger.debug(
            "[FileCrawlPosts][paged][pattern] reference structure filter enabled | db=%s chat_bot_id=%s pattern=%s",
            db_name,
            chat_bot_id,
            reference_pattern_key,
        )

    strict_sql_fallback_used = False
    using_legacy_condition = False
    exploration_cols = await _load_exploration_columns(str(db_name), table_name)
    select_columns = _build_exploration_select_columns(exploration_cols, include_id=True)

    while True:
        query = (
            f"SELECT {select_columns} FROM {table_name} "
            f"WHERE ({condition}) AND {page_comparator} "
            f"ORDER BY {order_by} LIMIT {page_size}"
        )
        try:
            rows = await maria_execute_query(query, (page_cursor,), fetch=True, dbname=db_name)
        except Exception as exc:
            if (not using_legacy_condition) and _should_fallback_to_legacy_exploration_condition(exc):
                logger.warning(
                    "[FileCrawlPosts][paged] exploration filter columns unavailable -> fallback to legacy condition | db=%s chat_bot_id=%s err=%s",
                    db_name,
                    chat_bot_id,
                    exc,
                )
                condition = legacy_condition
                base_condition = legacy_base_condition
                using_legacy_condition = True
                strict_sql_fallback_used = False
                continue
            logger.error("[FileCrawlPosts][paged] DB query failed | db=%s err=%s", db_name, exc)
            return

        if (
            not rows
            and ((reverse_order and page_cursor == initial_reverse_cursor) or ((not reverse_order) and page_cursor == 0))
            and scope_condition
            and not strict_sql_fallback_used
            and allow_broad_scope_fallback
        ):
            logger.warning(
                "[FileCrawlPosts][paged] strict scope SQL returned 0 rows -> broad query fallback | db=%s chat_bot_id=%s domains=%s path_prefix=%s",
                db_name,
                chat_bot_id,
                final_domains,
                effective_path_prefix,
            )
            condition = base_condition
            strict_sql_fallback_used = True
            continue

        if not rows:
            break

        seen_any_row = True
        page_row_count += len(rows)
        next_page_cursor = page_cursor
        for row in rows:
            try:
                row_id = int((row or {}).get("id") or 0)
            except Exception:
                row_id = 0
            if reverse_order:
                if row_id > 0 and row_id < next_page_cursor:
                    next_page_cursor = row_id
            elif row_id > next_page_cursor:
                next_page_cursor = row_id

            raw_url = row.get("url") if isinstance(row, dict) else None
            row_type = str(row.get("type") or "").strip().lower() if isinstance(row, dict) else ""
            if not raw_url:
                continue
            try:
                url = ensure_url_scheme(str(raw_url).strip())
            except Exception:
                continue
            if not url:
                continue
            scanned_count += 1
            # Use request_url for crawling; use normalized_key only for comparison/dedupe.
            request_url = url
            normalized_key = (canonicalize_url_for_dedup(request_url) or request_url.strip() or "").strip()
            missing_query_reason = _missing_required_file_detail_query_reason(request_url)
            if missing_query_reason:
                missing_query_skipped_count += 1
                if len(missing_query_skipped_samples) < 5:
                    missing_query_skipped_samples.append(request_url[:200])
                logger.warning(
                    "[FileUrlTrace][file_crawl_posts.detail_query_missing] db=%s chat_bot_id=%s row_type=%s reason=%s action=skip raw_url=%s request_url=%s normalized_key=%s",
                    db_name,
                    chat_bot_id,
                    row_type or "-",
                    missing_query_reason,
                    str(raw_url or "").strip()[:300],
                    request_url[:300],
                    normalized_key[:300],
                )
                continue

            dedupe_key = ""
            if dedupe_urls:
                dedupe_key = normalized_key
                if not dedupe_key or dedupe_key in seen:
                    deduped_count += 1
                    continue
            if (not resolved_learn_list_id) and final_domains and not _url_host_matches_scope_domains(url, final_domains, path_prefix=effective_path_prefix):
                domain_skipped_count += 1
                if len(domain_skipped_samples) < 5:
                    domain_skipped_samples.append(url[:200])
                continue
            if (not resolved_learn_list_id) and not _url_matches_reference_query_pairs(url, reference_query_pairs):
                unmatched_count += 1
                if len(unmatched_samples) < 5:
                    unmatched_samples.append(url[:200])
                continue
            if (not resolved_learn_list_id) and not _url_matches_reference_structure_pattern(url, reference_pattern_key):
                unmatched_count += 1
                if len(unmatched_samples) < 5:
                    unmatched_samples.append(url[:200])
                continue
            item_type = row_type or "post"
            temporary_post_match = False
            if dedupe_urls:
                seen.add(dedupe_key)
            item = {"url": url, "type": item_type}
            _apply_direct_fast_path_fields(item, row, base_url=url)
            if temporary_post_match:
                item["force_relearn"] = True
                item["temporary_post_match"] = True
                item["disable_playwright"] = True
            batch.append(item)
            matched_count += 1
            if len(matched_samples) < 5:
                matched_samples.append({"url": url[:200], "type": item_type})
            if len(batch) >= max(1, int(batch_size or 200)):
                if shuffle_order:
                    random.shuffle(batch)
                yield batch
                batch = []

        if next_page_cursor == page_cursor:
            break
        page_cursor = next_page_cursor

    if fallback_url_scope:
        logger.debug(
            "[START_URLS] fallback scope result | matched_rows=%s scope=%s",
            page_row_count,
            fallback_url_scope[:300],
        )

    if not seen_any_row:
        logger.warning(
            "[FileCrawlPosts][paged] no post rows | dbname=%s chat_bot_id=%s domains=%s",
            db_name,
            chat_bot_id,
            final_domains,
        )
        return

    logger.debug(
        "[START_URLS_RULE_TRACE][file][paged] emit summary | db=%s chat_bot_id=%s fetched_rows=%s scanned=%s deduped=%s domain_skipped=%s missing_query_skipped=%s unmatched=%s matched=%s emitted=%s sample_matched=%s sample_unmatched=%s sample_domain_skipped=%s sample_missing_query=%s",
        db_name,
        chat_bot_id,
        page_row_count,
        scanned_count,
        deduped_count,
        domain_skipped_count,
        missing_query_skipped_count,
        unmatched_count,
        matched_count,
        matched_count,
        matched_samples,
        unmatched_samples,
        domain_skipped_samples,
        missing_query_skipped_samples,
    )
    if batch:
        if shuffle_order:
            random.shuffle(batch)
        yield batch


async def count_file_crawl_post_urls_paged(
    *,
    db_name: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    contents_url: Optional[Union[str, List[str]]] = None,
    chat_bot_id: Optional[str] = None,
    method: str = "period",
    target_date: Optional[List[str]] = None,
    exploration_date_filter_enabled: bool = False,
    scope_path_prefix: Optional[str] = None,
    start_urls_order: Optional[str] = None,
    use_category_rules: bool = True,
    dedupe_urls: bool = True,
    learn_list_id_scope: Optional[Union[int, str]] = None,
    scope_by_contents_learn_list_id: bool = False,
) -> int:
    total = 0
    async for chunk in stream_post_urls_for_file_crawl_paged(
        db_name=db_name,
        target_domains=target_domains,
        contents_url=contents_url,
        chat_bot_id=chat_bot_id,
        method=method,
        target_date=target_date,
        exploration_date_filter_enabled=exploration_date_filter_enabled,
        scope_path_prefix=scope_path_prefix,
        start_urls_order=start_urls_order,
        use_category_rules=use_category_rules,
        dedupe_urls=dedupe_urls,
        learn_list_id_scope=learn_list_id_scope,
        scope_by_contents_learn_list_id=scope_by_contents_learn_list_id,
    ):
        total += len(chunk)
    return total

