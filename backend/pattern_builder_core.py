from __future__ import annotations

import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse, urlunparse

import aiohttp

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


DEFAULT_EXPLORATION_TABLE_NAME = "ASADAL_CRAWLING_EXPLORATION"
DEFAULT_FILTER_CONDITION = "type = 'post'"
DEFAULT_PATTERN_BUILDER_SOURCE_DB = "dev_user"

QueryExecutor = Callable[..., Awaitable[Any]]

_TIMEOUT = aiohttp.ClientTimeout(total=12, connect=4, sock_read=8)
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_DYNAMIC_SEGMENT_RE = re.compile(r"^(?:\d+|[0-9a-f]{8,}|[0-9a-f-]{16,})$", re.IGNORECASE)
_BOARD_HINTS = (
    "board",
    "bbs",
    "notice",
    "ntt",
    "list",
    "view",
    "article",
    "portal",
    "news",
    "announcement",
    "gosi",
    "gonggo",
)
_STABLE_QUERY_KEYS = {
    "bbsno",
    "bbsid",
    "boardid",
    "board_id",
    "bo_table",
    "key",
    "menuno",
    "menu_no",
    "menuid",
    "ctgrycd",
    "ctgry_cd",
    "categoryid",
    "category_id",
    "categorycd",
    "category_cd",
    "siteid",
    "site_id",
}
_DETAIL_QUERY_KEYS = {
    "nttno",
    "nttid",
    "wr_id",
    "idx",
    "seq",
    "no",
    "articleno",
    "article_no",
    "boardseq",
    "board_seq",
}
_PAGING_QUERY_KEYS = {
    "page",
    "pageno",
    "pageindex",
    "pageunit",
    "page_size",
    "currentpage",
}
_MULTI_LABEL_PUBLIC_SUFFIXES = {
    "co.kr",
    "go.kr",
    "or.kr",
    "ac.kr",
    "re.kr",
    "pe.kr",
    "ne.kr",
    "mil.kr",
    "hs.kr",
    "ms.kr",
    "es.kr",
    "sc.kr",
    "com.au",
    "net.au",
    "org.au",
    "co.jp",
}


class PatternBuilderError(Exception):
    pass


class UnsafeFilterConditionError(PatternBuilderError):
    pass


class InvalidUrlError(PatternBuilderError):
    pass


def get_default_source_dbname() -> str:
    return str(os.getenv("PATTERN_BUILDER_SOURCE_DB") or DEFAULT_PATTERN_BUILDER_SOURCE_DB).strip() or DEFAULT_PATTERN_BUILDER_SOURCE_DB


def _ensure_url_scheme(url_or_domain: Any, default_scheme: str = "https") -> str:
    if not url_or_domain:
        return ""
    if isinstance(url_or_domain, dict):
        url_or_domain = url_or_domain.get("url", "")
    if not url_or_domain:
        return ""
    value = str(url_or_domain).strip()
    if value.startswith(("http://", "https://")):
        return value
    if value.startswith("//"):
        return f"{default_scheme}:{value}"
    return f"{default_scheme}://{value}"


def _scope_include_scheme() -> bool:
    try:
        value = str(os.getenv("CRAWL_SCOPE_INCLUDE_SCHEME", "0") or "0").strip().lower()
    except Exception:
        value = "0"
    return value in ("1", "true", "yes", "on")


def _coerce_scope_source(raw: Any) -> Any:
    if isinstance(raw, (list, tuple, set)):
        for item in raw:
            text = str(item or "").strip()
            if text:
                return item
        return ""
    return raw


def _as_parseable_url(raw: Any) -> str:
    raw = _coerce_scope_source(raw)
    text = str(raw or "").strip()
    if not text:
        return ""
    if "://" not in text:
        if text.startswith("//"):
            return f"https:{text}"
        return f"https://{text}"
    return text


def _canonicalize_scope_host(host: Any) -> str:
    text = str(host or "").strip().lower()
    if text.startswith("www."):
        return text[4:]
    return text


def extract_scope_host(raw: Any) -> str:
    try:
        parsed = urlparse(_as_parseable_url(raw))
        return _canonicalize_scope_host(parsed.hostname)
    except Exception:
        return ""


def normalize_scope_path_prefix(path_prefix: Any) -> str:
    try:
        raw = str(path_prefix or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return ""
    try:
        if "://" in raw or raw.startswith("//"):
            raw = str(urlparse(_as_parseable_url(raw)).path or "").strip() or "/"
    except Exception:
        pass
    while "//" in raw:
        raw = raw.replace("//", "/")
    if not raw.startswith("/"):
        raw = "/" + raw
    if raw == "/":
        return "/"
    return raw.rstrip("/") + "/"


def extract_scope_path_prefix(raw: Any) -> str:
    try:
        parsed = urlparse(_as_parseable_url(raw))
        path = str(parsed.path or "").strip()
    except Exception:
        path = ""
    if not path or path == "/":
        return ""
    while "//" in path:
        path = path.replace("//", "/")
    parts = [part for part in path.split("/") if part]
    if not parts:
        return ""
    first = str(parts[0] or "").strip()
    if not first:
        return ""
    if len(parts) == 1 and "." in first:
        return "/"
    return f"/{first}/"


def _sql_escape_literal(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _host_scope_aliases(host: str) -> List[str]:
    text = _canonicalize_scope_host(host)
    if not text:
        return []
    aliases = [text]
    if not text.startswith("www."):
        aliases.append(f"www.{text}")
    return list(dict.fromkeys(aliases))


def build_sql_scope_condition(
    column: str,
    identities: List[str],
    *,
    include_scheme: Optional[bool] = None,
    path_prefix: Optional[str] = None,
) -> str:
    use_scheme = _scope_include_scheme() if include_scheme is None else bool(include_scheme)
    if not identities:
        return ""
    normalized_path_prefix = normalize_scope_path_prefix(path_prefix)

    clauses: List[str] = []
    for ident in identities:
        if not ident:
            continue
        if use_scheme:
            parsed = urlparse(_as_parseable_url(ident))
            scheme = str(parsed.scheme or "https").strip().lower()
            for host_alias in _host_scope_aliases(parsed.hostname):
                escaped = _sql_escape_literal(f"{scheme}://{host_alias}")
                if normalized_path_prefix:
                    escaped_path = _sql_escape_literal(normalized_path_prefix)
                    base = f"{escaped}{escaped_path}"
                    if normalized_path_prefix == "/":
                        clauses.append(f"{column} = '{escaped}'")
                        clauses.append(f"{column} LIKE '{escaped}/%%'")
                    else:
                        clauses.append(f"{column} LIKE '{base}%%'")
                else:
                    clauses.append(f"{column} = '{escaped}'")
                    clauses.append(f"{column} LIKE '{escaped}/%%'")
            continue

        for host_alias in _host_scope_aliases(ident):
            host = _sql_escape_literal(host_alias)
            for scheme in ("https", "http"):
                origin = f"{scheme}://{host}"
                if normalized_path_prefix:
                    escaped_path = _sql_escape_literal(normalized_path_prefix)
                    if normalized_path_prefix == "/":
                        clauses.append(f"{column} = '{origin}'")
                        clauses.append(f"{column} LIKE '{origin}/%%'")
                    else:
                        clauses.append(f"{column} LIKE '{origin}{escaped_path}%%'")
                else:
                    clauses.append(f"{column} = '{origin}'")
                    clauses.append(f"{column} LIKE '{origin}/%%'")

    deduped = list(dict.fromkeys(clauses))
    if not deduped:
        return ""
    return "(" + " OR ".join(deduped) + ")"


def build_sql_top_domain_condition(column: str, top_domain: str) -> str:
    normalized = _top_domain_from_host(top_domain) or _canonicalize_scope_host(top_domain)
    if not normalized:
        return ""
    escaped = _sql_escape_literal(normalized)
    clauses: List[str] = []
    for scheme in ("https", "http"):
        origin = f"{scheme}://{escaped}"
        clauses.append(f"{column} = '{origin}'")
        clauses.append(f"{column} LIKE '{origin}/%%'")
        clauses.append(f"{column} LIKE '{scheme}://%%.{escaped}'")
        clauses.append(f"{column} LIKE '{scheme}://%%.{escaped}/%%'")
    deduped = list(dict.fromkeys(clauses))
    return "(" + " OR ".join(deduped) + ")" if deduped else ""


def _clean_url(raw: Any) -> str:
    try:
        value = _ensure_url_scheme(str(raw or "").strip())
    except Exception:
        value = str(raw or "").strip()
    if not value or not value.startswith(("http://", "https://")):
        return ""
    try:
        parsed = urlparse(value)
        value = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))
    except Exception:
        return ""
    return value


def _extract_count_value(rows: Any) -> int:
    first_row = (rows or [{}])[0] if isinstance(rows, list) else {}
    if not isinstance(first_row, dict):
        return 0
    for key in ("cnt", "count", "total", "COUNT(*)"):
        if key in first_row:
            try:
                return int(first_row.get(key) or 0)
            except Exception:
                return 0
    for value in first_row.values():
        try:
            return int(value or 0)
        except Exception:
            continue
    return 0


def _normalize_filter_condition(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        text = DEFAULT_FILTER_CONDITION
    return re.sub(r"^\s*where\s+", "", text, flags=re.IGNORECASE)


def _validate_filter_condition(raw: Any) -> str:
    condition = _normalize_filter_condition(raw)
    lowered = f" {condition.lower()} "
    if any(token in condition for token in (";", "--", "/*", "*/")):
        raise UnsafeFilterConditionError("unsafe_filter_condition")
    if any(
        token in lowered
        for token in (
            " insert ",
            " update ",
            " delete ",
            " drop ",
            " alter ",
            " create ",
            " truncate ",
            " replace ",
            " union ",
            " into outfile ",
            " load_file(",
            " sleep(",
            " benchmark(",
        )
    ):
        raise UnsafeFilterConditionError("unsafe_filter_condition")
    return condition


def _url_without_scheme(url: str) -> str:
    parsed = urlparse(url)
    text = parsed.netloc + parsed.path
    if parsed.query:
        text += "?" + parsed.query
    return text.lower()


def _url_host(url: str) -> str:
    try:
        return str(urlparse(url).netloc or "").strip().lower()
    except Exception:
        return ""


def _path_segments(path: str) -> List[str]:
    return [segment for segment in str(path or "").split("/") if segment]


def _looks_like_filename(segment: str) -> bool:
    text = str(segment or "").lower()
    return "." in text and not text.endswith(".kr")


def _segment_template(segment: str) -> str:
    text = str(segment or "").strip().lower()
    if not text:
        return ""
    if _DYNAMIC_SEGMENT_RE.fullmatch(text):
        return ":var"
    return text


def _stable_query_pairs(url: str, *, preserve_case: bool = False) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for key, value in parse_qsl(urlparse(url).query, keep_blank_values=False):
        raw_key = str(key or "").strip()
        normalized_key = raw_key.lower()
        normalized_value = str(value or "").strip()
        if not normalized_key or not normalized_value:
            continue
        if normalized_key in _DETAIL_QUERY_KEYS or normalized_key in _PAGING_QUERY_KEYS:
            continue
        if normalized_key in _STABLE_QUERY_KEYS or len(normalized_value) <= 40:
            pairs.append((raw_key if preserve_case else normalized_key, normalized_value))
    return sorted(set(pairs))


def _query_signature(url: str) -> str:
    pairs = _stable_query_pairs(url)
    if not pairs:
        return ""
    return "&".join(f"{key}={value}" for key, value in pairs)


def _pattern_signature(url: str) -> str:
    parsed = urlparse(url)
    templated_segments = [_segment_template(segment) for segment in _path_segments(parsed.path)]
    path_signature = "/" + "/".join(templated_segments)
    return f"{parsed.netloc.lower()}|{path_signature}|{_query_signature(url)}"


def _top_domain_from_host(host: str) -> str:
    normalized = str(host or "").strip().lower()
    if not normalized:
        return ""
    labels = [label for label in normalized.split(".") if label]
    if len(labels) <= 2:
        return normalized
    suffix = ".".join(labels[-2:])
    if suffix in _MULTI_LABEL_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _path_template(url: str) -> str:
    parsed = urlparse(url)
    templated_segments = [_segment_template(segment) for segment in _path_segments(parsed.path)]
    return "/" + "/".join(templated_segments)


def _pattern_group_key(url: str) -> Tuple[str, str, str]:
    host = _url_host(url)
    return (_top_domain_from_host(host), _path_template(url), _query_signature(url))


def _score_boardish_url(url: str, anchor_text: str = "") -> int:
    lower = (url + " " + anchor_text).lower()
    score = 0
    for hint in _BOARD_HINTS:
        if hint in lower:
            score += 2
    if any(key in lower for key in ("list.do", "list.jsp", "list.asp", "board")):
        score += 3
    if any(key in lower for key in ("view.do", "nttno=", "nttid=", "wr_id=")):
        score += 1
    return score


def _common_prefix_segments(urls: Sequence[str], *, strip_script: bool) -> List[str]:
    segment_lists: List[List[str]] = []
    for url in urls:
        parsed = urlparse(url)
        segments = _path_segments(parsed.path)
        if strip_script and segments and _looks_like_filename(segments[-1]):
            segments = segments[:-1]
        if segments:
            segment_lists.append(segments)
    if not segment_lists:
        return []
    prefix = list(segment_lists[0])
    for segments in segment_lists[1:]:
        next_prefix: List[str] = []
        for left, right in zip(prefix, segments):
            if left != right:
                break
            next_prefix.append(left)
        prefix = next_prefix
        if not prefix:
            break
    return prefix


def _suggest_match_rule(urls: Sequence[str]) -> Dict[str, List[str]]:
    first_url = urls[0]
    parsed = urlparse(first_url)
    host = parsed.netloc.lower()
    script = _path_segments(parsed.path)[-1] if _path_segments(parsed.path) else ""
    common_dir = _common_prefix_segments(urls, strip_script=True)

    url_suggestion = f"{parsed.scheme or 'https'}://{host}"
    if common_dir:
        url_suggestion += "/" + "/".join(common_dir)
    if _looks_like_filename(script):
        if not url_suggestion.endswith("/"):
            url_suggestion += "/"
        url_suggestion += script

    query_parts: List[str] = []
    pair_counts: Counter[str] = Counter()
    display_tokens: Dict[str, str] = {}
    for url in urls:
        normalized_pairs = _stable_query_pairs(url)
        display_pairs = {
            f"{key.lower()}={value}": f"{key}={value}"
            for key, value in _stable_query_pairs(url, preserve_case=True)
        }
        for key, value in normalized_pairs:
            token = f"{key}={value}"
            pair_counts[token] += 1
            display_tokens.setdefault(token, display_pairs.get(token, token))
    threshold = max(1, len(urls) // 2)
    stable_url_tokens: List[str] = []
    for token, count in pair_counts.most_common():
        if count >= threshold:
            stable_url_tokens.append(display_tokens.get(token, token))
            query_parts.append(display_tokens.get(token, token))

    if stable_url_tokens:
        url_suggestion += "?" + "&".join(stable_url_tokens)

    unique_query_parts = list(dict.fromkeys(part for part in query_parts if part))
    return {
        "url": [url_suggestion] if url_suggestion else [],
        "query": unique_query_parts,
    }


def _frequent_tokens(urls: Sequence[str]) -> List[str]:
    counts: Counter[str] = Counter()
    for url in urls:
        parsed = urlparse(url)
        script = _path_segments(parsed.path)[-1] if _path_segments(parsed.path) else ""
        if _looks_like_filename(script):
            counts[script] += 1
        for key, value in _stable_query_pairs(url, preserve_case=True):
            counts[f"{key}={value}"] += 1
    return [token for token, count in counts.most_common(8) if count >= 2]


def _normalize_known_rule(rule: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    match_rule = rule.get("match_rule") if isinstance(rule.get("match_rule"), dict) else rule
    url_parts = match_rule.get("url") if isinstance(match_rule, dict) else []
    query_parts = match_rule.get("query") if isinstance(match_rule, dict) else []

    normalized_urls = []
    for item in (url_parts or []):
        text = str(item or "").strip()
        if not text:
            continue
        cleaned = _clean_url(text)
        if cleaned:
            normalized_urls.append(_url_without_scheme(cleaned))
        else:
            normalized_urls.append(text.lower())
    normalized_queries = [str(item or "").strip().lower() for item in (query_parts or []) if str(item or "").strip()]
    return normalized_urls, normalized_queries


def _url_matches_known_rules(url: str, known_rules: Sequence[Dict[str, Any]]) -> bool:
    normalized = _url_without_scheme(url)
    for rule in known_rules:
        url_parts, query_parts = _normalize_known_rule(rule)
        if url_parts and not all(part in normalized for part in url_parts):
            continue
        if query_parts and not all(part in normalized for part in query_parts):
            continue
        if url_parts or query_parts:
            return True
    return False


def _build_group_payload(signature: str, urls: Sequence[str]) -> Dict[str, Any]:
    parsed = urlparse(urls[0])
    sample_urls = list(urls[:8])
    rule = _suggest_match_rule(urls)
    return {
        "id": signature,
        "group_size": len(urls),
        "host": parsed.netloc.lower(),
        "path_template": "/" + "/".join(_segment_template(segment) for segment in _path_segments(parsed.path)),
        "suggested_match_rule": rule,
        "tokens": _frequent_tokens(urls),
        "sample_urls": sample_urls,
    }


def _representative_seed_row(rows: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None

    def _sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
        url = str(row.get("url") or "")
        score = _score_boardish_url(url)
        depth = len(_path_segments(urlparse(url).path))
        try:
            row_id = int(row.get("id") or 0)
        except Exception:
            row_id = 0
        return (score, -depth, -len(url), row_id)

    return max(rows, key=_sort_key)


def _filter_seed_rows(rows: Sequence[Dict[str, Any]], search: str, limit: int) -> List[Dict[str, Any]]:
    if not search:
        return list(rows[:limit])
    needle = str(search or "").strip().lower()
    filtered = []
    for row in rows:
        haystack = " ".join(
            [
                str(row.get("label") or ""),
                str(row.get("seed_url") or ""),
                str(row.get("chat_bot_id") or ""),
                str(row.get("host") or ""),
                str(row.get("path_template") or ""),
                str(row.get("query_signature") or ""),
                " ".join(str(item or "") for item in (row.get("scope_hosts") or [])),
            ]
        ).lower()
        if needle in haystack:
            filtered.append(row)
        if len(filtered) >= limit:
            break
    return filtered


def _normalize_input_urls(items: Sequence[Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        url = _clean_url(item.get("url") if isinstance(item, dict) else item)
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _merge_url_items(
    *,
    seed_url: str,
    url_items: Iterable[Dict[str, Any]],
    known_rules: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    merged: Dict[str, Dict[str, Any]] = {}
    duplicate_map: Dict[str, Dict[str, Any]] = {}
    seed_clean = _clean_url(seed_url)
    for item in url_items:
        url = _clean_url(item.get("url"))
        if not url:
            continue
        if _url_matches_known_rules(url, known_rules):
            continue
        source = str(item.get("source") or "unknown")
        existing = merged.get(url)
        if existing is None:
            merged[url] = {"url": url, "sources": [source]}
            continue
        if source not in existing["sources"]:
            existing["sources"].append(source)
        duplicate_entry = duplicate_map.setdefault(
            url,
            {
                "url": url,
                "count": 1,
                "sources": list(existing["sources"]),
            },
        )
        duplicate_entry["count"] += 1
        if source not in duplicate_entry["sources"]:
            duplicate_entry["sources"].append(source)

    if seed_clean and seed_clean not in merged and not _url_matches_known_rules(seed_clean, known_rules):
        merged[seed_clean] = {"url": seed_clean, "sources": ["seed"]}

    duplicates = sorted(
        duplicate_map.values(),
        key=lambda item: (-int(item.get("count") or 0), str(item.get("url") or "")),
    )
    return list(merged.values()), duplicates


def _extract_anchor_urls(html: str, base_url: str) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    if not html:
        return items

    if BeautifulSoup is None:
        for href in re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE):
            absolute = _clean_url(urljoin(base_url, href))
            if absolute:
                items.append((absolute, ""))
        return items

    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        absolute = _clean_url(urljoin(base_url, href))
        if not absolute:
            continue
        text = " ".join(anchor.stripped_strings)
        items.append((absolute, text))
    return items


@dataclass(slots=True)
class PatternBuilderCore:
    execute_query: QueryExecutor
    default_source_dbname: str = field(default_factory=get_default_source_dbname)
    exploration_table_name: str = DEFAULT_EXPLORATION_TABLE_NAME
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("pattern_builder_core"))

    def clean_url(self, raw: Any) -> str:
        return _clean_url(raw)

    def normalize_db_name(self, raw: Any) -> str:
        value = str(raw or "").strip()
        return value or self.default_source_dbname

    def validate_filter_condition(self, raw: Any) -> str:
        return _validate_filter_condition(raw)

    def filter_seed_rows(self, rows: Sequence[Dict[str, Any]], search: str, limit: int) -> List[Dict[str, Any]]:
        return _filter_seed_rows(rows, search, limit)

    def normalize_input_urls(self, items: Sequence[Any]) -> List[str]:
        return _normalize_input_urls(items)

    def merge_url_items(
        self,
        *,
        seed_url: str,
        url_items: Iterable[Dict[str, Any]],
        known_rules: Sequence[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        return _merge_url_items(seed_url=seed_url, url_items=url_items, known_rules=known_rules)

    def analyze_urls(self, items: Sequence[Any], min_group_size: int, known_rules: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        urls = [url for url in _normalize_input_urls(items) if not _url_matches_known_rules(url, known_rules)]
        if not urls:
            return {"url_count": 0, "group_count": 0, "leftover_group_count": 0, "groups": []}

        grouped: Dict[str, List[str]] = defaultdict(list)
        for url in urls:
            grouped[_pattern_signature(url)].append(url)

        groups = []
        for signature, group_urls in grouped.items():
            sorted_urls = sorted(group_urls)
            if len(sorted_urls) < min_group_size:
                continue
            groups.append(_build_group_payload(signature, sorted_urls))

        groups.sort(key=lambda item: (-int(item.get("group_size") or 0), str(item.get("host") or ""), str(item.get("id") or "")))
        leftovers = sum(1 for group_urls in grouped.values() if len(group_urls) < min_group_size)
        return {
            "url_count": len(urls),
            "group_count": len(groups),
            "leftover_group_count": leftovers,
            "groups": groups,
        }

    async def fetch_html(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
        headers = {"User-Agent": _USER_AGENT}
        async with session.get(url, headers=headers, allow_redirects=True, ssl=False) as response:
            content_type = str(response.headers.get("content-type") or "").lower()
            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                return str(response.url), ""
            text = await response.text(errors="ignore")
            return str(response.url), text

    async def fetch_meta(self, url: str) -> Dict[str, Any]:
        normalized_url = _clean_url(url)
        if not normalized_url:
            raise InvalidUrlError("invalid_url")

        connector = aiohttp.TCPConnector(limit=2, ssl=False)
        async with aiohttp.ClientSession(timeout=_TIMEOUT, connector=connector) as session:
            final_url, html = await self.fetch_html(session, normalized_url)

        title = ""
        headings: List[str] = []
        if BeautifulSoup is not None and html:
            soup = BeautifulSoup(html, "html.parser")
            if soup.title and soup.title.string:
                title = str(soup.title.string).strip()
            for tag_name in ("h1", "h2", "strong"):
                for tag in soup.find_all(tag_name):
                    text = " ".join(tag.stripped_strings)
                    if text and text not in headings:
                        headings.append(text)
                    if len(headings) >= 6:
                        break
                if len(headings) >= 6:
                    break

        return {"url": final_url, "title": title, "headings": headings}

    def should_keep_live_candidate(self, url: str, seed_url: str) -> bool:
        candidate_host = extract_scope_host(url)
        seed_host = extract_scope_host(seed_url)
        if not candidate_host or candidate_host != seed_host:
            return False

        seed_prefix = normalize_scope_path_prefix(extract_scope_path_prefix(seed_url))
        candidate_path = str(urlparse(url).path or "")
        if seed_prefix and seed_prefix not in ("", "/") and candidate_path.startswith(seed_prefix):
            return True

        lower = url.lower()
        return any(token in lower for token in _BOARD_HINTS)

    async def collect_live_urls(self, seed_url: str, max_pages: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        seen_urls: Dict[str, Dict[str, Any]] = {}
        queue: List[Tuple[int, str]] = [(100, seed_url)]
        visited_pages: set[str] = set()
        fetched_pages = 0

        connector = aiohttp.TCPConnector(limit=6, ssl=False)
        async with aiohttp.ClientSession(timeout=_TIMEOUT, connector=connector) as session:
            while queue and fetched_pages < max_pages:
                queue.sort(key=lambda item: (-item[0], len(item[1])))
                _, page_url = queue.pop(0)
                if page_url in visited_pages:
                    continue
                visited_pages.add(page_url)

                try:
                    final_url, html = await self.fetch_html(session, page_url)
                except Exception as exc:
                    self.logger.info("[PatternBuilder] live fetch skipped | url=%s err=%s", page_url, exc)
                    continue

                fetched_pages += 1
                if final_url not in seen_urls:
                    seen_urls[final_url] = {"url": final_url, "source": "live", "source_page": page_url}

                candidates = _extract_anchor_urls(html, final_url)
                follow_candidates: List[Tuple[int, str]] = []
                for candidate_url, anchor_text in candidates:
                    if not self.should_keep_live_candidate(candidate_url, seed_url):
                        continue
                    if candidate_url not in seen_urls:
                        seen_urls[candidate_url] = {
                            "url": candidate_url,
                            "source": "live",
                            "source_page": final_url,
                        }
                    score = _score_boardish_url(candidate_url, anchor_text)
                    if candidate_url not in visited_pages and score > 0:
                        follow_candidates.append((score, candidate_url))

                for item in follow_candidates[: max_pages * 4]:
                    if item[1] not in {queued_url for _, queued_url in queue}:
                        queue.append(item)

        return list(seen_urls.values()), {"fetched_pages": fetched_pages, "follow_queue_final": len(queue)}

    async def query_exploration_urls(
        self,
        *,
        source_db_name: str,
        seed_url: str,
        chat_bot_id: Optional[str],
        limit: Optional[int],
        filter_condition: str,
        scope_hosts: Optional[Sequence[str]] = None,
        path_prefix: Optional[str] = None,
        top_domain: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        identities = [extract_scope_host(seed_url)] if extract_scope_host(seed_url) else []
        if scope_hosts:
            identities = [extract_scope_host(item) for item in scope_hosts if extract_scope_host(item)]
        identities = list(dict.fromkeys(identity for identity in identities if identity))
        effective_path_prefix = path_prefix or extract_scope_path_prefix(seed_url)

        conditions = [_validate_filter_condition(filter_condition)]
        if chat_bot_id:
            safe_bot_id = str(chat_bot_id).replace("\\", "\\\\").replace("'", "''")
            conditions.append(f"chat_bot_id = '{safe_bot_id}'")
        scope_sql = ""
        if top_domain:
            scope_sql = build_sql_top_domain_condition("url", str(top_domain))
        if not scope_sql:
            scope_sql = build_sql_scope_condition("url", identities, path_prefix=effective_path_prefix)
        if scope_sql:
            conditions.append(scope_sql)

        strict_where = " AND ".join(conditions + ["COALESCE(LOWER(merge_status), '') <> 'duplicate'", "COALESCE(is_active, 0) = 1"])
        fallback_where = " AND ".join(conditions)
        limit_sql = f" LIMIT {int(limit)}" if limit and int(limit) > 0 else ""
        strict_count_query = f"SELECT COUNT(*) AS cnt FROM {self.exploration_table_name} WHERE {strict_where}"
        fallback_count_query = f"SELECT COUNT(*) AS cnt FROM {self.exploration_table_name} WHERE {fallback_where}"
        query = (
            f"SELECT url, type FROM {self.exploration_table_name} "
            f"WHERE {strict_where} ORDER BY id DESC{limit_sql}"
        )
        fallback_query = (
            f"SELECT url, type FROM {self.exploration_table_name} "
            f"WHERE {fallback_where} ORDER BY id DESC{limit_sql}"
        )
        self.logger.warning(
            "[PatternBuilder][collect_sql] db=%s top_domain=%s limit=%s limit_sql=%s strict=%s fallback=%s",
            source_db_name,
            top_domain or "",
            str(limit),
            limit_sql or "(none)",
            query,
            fallback_query,
        )

        used_query = query
        used_mode = "strict"
        count_query = strict_count_query
        count_mode = "strict"
        filtered_total_count = 0
        try:
            count_rows = await self.execute_query(strict_count_query, fetch=True, dbname=source_db_name)
            filtered_total_count = _extract_count_value(count_rows)
        except Exception:
            count_query = fallback_count_query
            count_mode = "fallback"
            count_rows = await self.execute_query(fallback_count_query, fetch=True, dbname=source_db_name)
            filtered_total_count = _extract_count_value(count_rows)
        try:
            rows = await self.execute_query(query, fetch=True, dbname=source_db_name)
        except Exception:
            used_query = fallback_query
            used_mode = "fallback"
            rows = await self.execute_query(fallback_query, fetch=True, dbname=source_db_name)

        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        invalid_url_count = 0
        duplicate_row_count = 0
        for row in rows or []:
            url = _clean_url((row or {}).get("url"))
            if not url:
                invalid_url_count += 1
                continue
            if url in seen:
                duplicate_row_count += 1
                continue
            seen.add(url)
            out.append({"url": url, "source": "exploration", "type": (row or {}).get("type") or "post"})
        debug_info = {
            "db_name": source_db_name,
            "top_domain": top_domain or "",
            "seed_url": seed_url,
            "filter_condition": filter_condition,
            "chat_bot_id": chat_bot_id or "",
            "scope_hosts": identities,
            "path_prefix": effective_path_prefix or "",
            "limit": limit,
            "limit_sql": limit_sql.strip(),
            "strict_where": strict_where,
            "fallback_where": fallback_where,
            "count_mode": count_mode,
            "count_query": count_query,
            "filtered_total_count": filtered_total_count,
            "used_mode": used_mode,
            "used_query": used_query,
            "raw_row_count": len(rows or []),
            "invalid_url_count": invalid_url_count,
            "duplicate_row_count": duplicate_row_count,
            "unique_url_count": len(out),
        }
        self.logger.warning(
            "[PatternBuilder][collect_debug] db=%s top_domain=%s mode=%s limit=%s raw=%s invalid=%s dup_rows=%s unique=%s",
            source_db_name,
            top_domain or "",
            used_mode,
            str(limit),
            len(rows or []),
            invalid_url_count,
            duplicate_row_count,
            len(out),
        )
        return out, debug_info

    async def load_seed_rows_from_exploration(self, source_db_name: str, limit: int, filter_condition: str) -> List[Dict[str, Any]]:
        validated_filter = _validate_filter_condition(filter_condition)
        query = (
            "SELECT id, chat_bot_id, url "
            f"FROM {self.exploration_table_name} "
            f"WHERE {validated_filter} "
            f"ORDER BY id DESC LIMIT {int(limit * 10)}"
        )
        rows = await self.execute_query(query, fetch=True, dbname=source_db_name)
        grouped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for row in rows or []:
            url = _clean_url((row or {}).get("url"))
            if not url:
                continue
            top_domain, path_template, query_signature = _pattern_group_key(url)
            if not top_domain:
                continue
            group = grouped.setdefault(
                (top_domain, path_template, query_signature),
                {
                    "top_domain": top_domain,
                    "path_template": path_template,
                    "query_signature": query_signature,
                    "rows": [],
                    "hosts": set(),
                    "chat_bot_ids": set(),
                },
            )
            group["rows"].append(
                {
                    "id": (row or {}).get("id"),
                    "url": url,
                    "chat_bot_id": str((row or {}).get("chat_bot_id") or "").strip() or None,
                }
            )
            group["hosts"].add(_url_host(url))
            if (row or {}).get("chat_bot_id"):
                group["chat_bot_ids"].add(str((row or {}).get("chat_bot_id") or "").strip())

        out: List[Dict[str, Any]] = []
        sorted_groups = sorted(
            grouped.values(),
            key=lambda item: (
                len(item["rows"]),
                len(item["hosts"]),
                _score_boardish_url(str((_representative_seed_row(item["rows"]) or {}).get("url") or "")),
                item["top_domain"],
                item["path_template"],
            ),
            reverse=True,
        )

        for group in sorted_groups[:limit]:
            representative = _representative_seed_row(group["rows"])
            if not representative:
                continue
            representative_url = str(representative.get("url") or "")
            common_dir = _common_prefix_segments([str(row.get("url") or "") for row in group["rows"]], strip_script=True)
            if common_dir:
                path_prefix = normalize_scope_path_prefix("/" + "/".join(common_dir))
            else:
                path_prefix = normalize_scope_path_prefix(extract_scope_path_prefix(representative_url))

            chat_bot_ids = sorted(bot_id for bot_id in group["chat_bot_ids"] if bot_id)
            out.append(
                {
                    "id": f"exploration:{group['top_domain']}:{group['path_template']}:{group['query_signature'] or '-'}",
                    "source": "exploration_pattern",
                    "seed_url": representative_url,
                    "chat_bot_id": chat_bot_ids[0] if len(chat_bot_ids) == 1 else None,
                    "label": group["top_domain"],
                    "host": group["top_domain"],
                    "path_prefix": path_prefix,
                    "path_template": group["path_template"],
                    "query_signature": group["query_signature"],
                    "pattern_size": len(group["rows"]),
                    "scope_hosts": sorted(host for host in group["hosts"] if host),
                    "scope_host_count": len(group["hosts"]),
                    "chat_bot_ids": chat_bot_ids,
                    "chat_bot_count": len(chat_bot_ids),
                }
            )
        return out
