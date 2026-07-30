#!/usr/bin/env python3
"""Count ASADAL_CRAWLING_EXPLORATION URL matches through the f1_dev bridge.

No direct DB connection is used. The script calls the remote file-dashboard
exploration-posts endpoint, then applies fallback-style URL scope, path, and
query key/value filtering locally. Multiple URLs are supported.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlparse

import requests

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.shared.file_crawl_post_urls import (  # noqa: E402
    _reference_structure_pattern_key,
    _resolve_file_crawl_scope,
    _url_matches_reference_structure_pattern,
)
from backend.shared.url_scope import url_matches_scope_identities  # noqa: E402

DEFAULT_BRIDGE_BASE = "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler"
DEFAULT_BRIDGE_PATH = "/backend/file-dashboard/exploration-posts"


def _bridge_endpoint(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")

def _extract_reference_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"\[[^\]]*\]\((https?://[^\s)]+)\)", text, flags=re.IGNORECASE)
    if match:
        return str(match.group(1) or "").strip()
    return text


def _csv(value: str | None) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if not isinstance(row, dict):
        return default
    if key in row:
        return row.get(key, default)
    lower = {str(k).lower(): v for k, v in row.items()}
    return lower.get(key.lower(), default)


def _post_bridge_json(endpoint: str, payload: dict[str, Any], timeout: float, retries: int) -> dict[str, Any]:
    session = requests.Session()
    session.trust_env = False
    last_exc: Exception | None = None
    for attempt in range(max(1, retries + 1)):
        try:
            response = session.post(endpoint, json=payload, timeout=timeout)
            if response.status_code in {502, 503, 504} and attempt < retries:
                time.sleep(min(2.0, 0.4 * (attempt + 1)))
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected bridge response type: {type(data).__name__}")
            if data.get("ok") is False:
                raise RuntimeError(str(data.get("error") or data.get("message") or data))
            return data
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(2.0, 0.4 * (attempt + 1)))
                continue
            raise
    raise RuntimeError(str(last_exc or "bridge request failed"))


def _extract_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = data.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _remote_total(data: dict[str, Any]) -> int:
    try:
        return int(data.get("total") or 0)
    except Exception:
        return 0


def _exploration_type_for_url(raw_url: str, requested: str) -> str:
    value = str(requested or "auto").strip().lower()
    if value and value != "auto":
        return value
    try:
        path = (urlparse(raw_url).path or "").lower()
    except Exception:
        path = ""
    if path.endswith("/contents.do") or path.endswith("contents.do"):
        return "all"
    return "post"


def _iter_bridge_rows(
    *,
    endpoint: str,
    db_name: str,
    chat_bot_id: str,
    contents_url: str,
    active_only: bool,
    include_duplicates: bool,
    page_size: int,
    max_rows: int,
    timeout: float,
    retries: int,
    exploration_type: str,
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    offset = 0
    fetched = 0
    while True:
        limit = min(page_size, max_rows - fetched) if max_rows > 0 else page_size
        if limit <= 0:
            break
        payload: dict[str, Any] = {
            "db_name": db_name,
            "contents_url": contents_url,
            "contents": [contents_url],
            "limit": limit,
            "offset": offset,
            "active_only": active_only,
            "include_duplicates": include_duplicates,
            "method": "all",
            "count_only": False,
            "scope_by_contents_learn_list_id": False,
            "exploration_type": exploration_type,
        }
        if chat_bot_id:
            payload["chat_bot_id"] = chat_bot_id
        data = _post_bridge_json(endpoint, payload, timeout, retries)
        rows = _extract_rows(data)
        for row in rows:
            yield row, data
        fetched += len(rows)
        total = _remote_total(data)
        offset += len(rows)
        if not rows or len(rows) < limit or (total and offset >= total):
            break


def _query_pairs(url: str, *, ignored_keys: set[str]) -> list[tuple[str, str]]:
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return []
    out: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query or "", keep_blank_values=True):
        key_text = str(key or "").strip()
        if not key_text or key_text.lower() in ignored_keys:
            continue
        out.append((key_text.lower(), str(value or "")))
    return out


def _query_values_by_key(url: str) -> dict[str, set[str]]:
    try:
        parsed = urlparse(str(url or ""))
    except Exception:
        return {}
    out: dict[str, set[str]] = {}
    for key, value in parse_qsl(parsed.query or "", keep_blank_values=True):
        key_text = str(key or "").strip().lower()
        if not key_text:
            continue
        out.setdefault(key_text, set()).add(str(value or ""))
    return out


def _matches_query_pairs(row_url: str, query_pairs: list[tuple[str, str]]) -> bool:
    if not query_pairs:
        return True
    row_values = _query_values_by_key(row_url)
    for key, value in query_pairs:
        if value not in row_values.get(key, set()):
            return False
    return True


def _matches_fallback_scope(
    row_url: str,
    domains: list[str],
    path_prefix: str,
    pattern_key: str,
    query_pairs: list[tuple[str, str]],
) -> bool:
    if domains and not url_matches_scope_identities(row_url, domains, path_prefix=path_prefix):
        return False
    if not _matches_query_pairs(row_url, query_pairs):
        return False
    if pattern_key and not _url_matches_reference_structure_pattern(row_url, pattern_key):
        return False
    return True


def _count_one(args: argparse.Namespace, url: str) -> dict[str, Any]:
    endpoint = _bridge_endpoint(args.bridge_base, args.bridge_path)
    chat_bot_id = "" if args.no_chat_bot_filter else str(args.chat_bot_id or "").strip()
    target_domains = _csv(args.target_domains)
    reference_url = _extract_reference_url(url)
    domains, path_prefix = _resolve_file_crawl_scope(
        target_domains=target_domains or None,
        contents_url=reference_url,
        use_rule_scope=False,
        rule_patterns=[],
        explicit_path_prefix=args.scope_path_prefix,
    )
    domains = list(domains or [])
    pattern_key = _reference_structure_pattern_key(reference_url)
    ignored_query_keys = {key.lower() for key in _csv(args.ignore_query_keys)}
    query_pairs = [] if args.ignore_query_values else _query_pairs(reference_url, ignored_keys=ignored_query_keys)
    exploration_type = _exploration_type_for_url(url, args.exploration_type)

    remote_total = None
    scanned = 0
    matched = 0
    samples: list[dict[str, Any]] = []
    last_scope: dict[str, Any] = {}
    last_bridge_type = ""

    for row, data in _iter_bridge_rows(
        endpoint=endpoint,
        db_name=args.db,
        chat_bot_id=chat_bot_id,
        contents_url=reference_url,
        active_only=not args.include_inactive,
        include_duplicates=args.include_duplicates,
        page_size=max(1, min(int(args.page_size or 5000), 50000)),
        max_rows=max(0, int(args.max_rows or 0)),
        timeout=float(args.timeout),
        retries=max(0, int(args.retries or 0)),
        exploration_type=exploration_type,
    ):
        if remote_total is None:
            remote_total = _remote_total(data)
        if isinstance(data.get("url_scope"), dict):
            last_scope = data.get("url_scope") or {}
        last_bridge_type = str(data.get("exploration_type") or "")
        row_url = str(_row_get(row, "url", "") or "").strip()
        if not row_url:
            continue
        scanned += 1
        if not _matches_fallback_scope(row_url, domains, path_prefix, pattern_key, query_pairs):
            continue
        matched += 1
        if len(samples) < args.sample:
            samples.append({"id": _row_get(row, "id"), "type": _row_get(row, "type"), "url": row_url})

    return {
        "db_name": args.db,
        "url": url,
        "chat_bot_id": chat_bot_id or None,
        "bridge_endpoint": endpoint,
        "exploration_type_requested": exploration_type,
        "exploration_type_bridge": last_bridge_type or None,
        "bridge_remote_total": remote_total or 0,
        "bridge_rows_scanned": scanned,
        "domains": domains,
        "path_prefix": path_prefix or "",
        "structure_pattern": pattern_key or "",
        "query_pairs": query_pairs,
        "matched_count": matched,
        "samples": samples,
        "bridge_url_scope": last_scope,
        "note": "learn_list_id is not used; f1_dev bridge rows are client-filtered by fallback URL scope/pattern.",
    }


def _print_text(result: dict[str, Any], *, index: int, total: int) -> None:
    if total > 1:
        print(f"[{index}/{total}]")
    print(f"db_name: {result['db_name']}")
    print(f"url: {result['url']}")
    if result.get("reference_url") and result.get("reference_url") != result.get("url"):
        print(f"reference_url: {result['reference_url']}")
    print(f"chat_bot_id: {result.get('chat_bot_id') or '-'}")
    print(f"bridge_endpoint: {result['bridge_endpoint']}")
    print(f"exploration_type_requested: {result['exploration_type_requested']}")
    print(f"exploration_type_bridge: {result.get('exploration_type_bridge') or '-'}")
    print(f"bridge_remote_total: {result['bridge_remote_total']}")
    print(f"bridge_rows_scanned: {result['bridge_rows_scanned']}")
    print(f"domains: {result['domains']}")
    print(f"path_prefix: {result['path_prefix'] or '-'}")
    print(f"structure_pattern: {result['structure_pattern'] or '-'}")
    print(f"query_pairs: {result['query_pairs'] or '-'}")
    print(f"matched_count: {result['matched_count']}")
    samples = result.get("samples") or []
    if samples:
        print(f"samples (max {len(samples)}):")
        for item in samples:
            row_type = item.get("type") or "-"
            print(f"  {item.get('id')}: type={row_type} {item.get('url')}")


def _read_urls(args: argparse.Namespace) -> list[str]:
    urls = [str(item or "").strip() for item in (args.urls or []) if str(item or "").strip()]
    if not urls and not sys.stdin.isatty():
        raw = sys.stdin.read()
        urls = [line.strip() for line in raw.splitlines() if line.strip()]
    return urls


def _run(args: argparse.Namespace) -> int:
    args.db = str(args.db or "").strip()
    if not args.db:
        print("--db is required", file=sys.stderr)
        return 2
    urls = _read_urls(args)
    if not urls:
        print("url is required", file=sys.stderr)
        return 2
    results = [_count_one(args, url) for url in urls]
    if args.format == "json":
        print(json.dumps(results if len(results) > 1 else results[0], ensure_ascii=False, indent=2, default=str))
    else:
        for idx, result in enumerate(results, start=1):
            if idx > 1:
                print("")
            _print_text(result, index=idx, total=len(results))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Count exploration URL matches via f1_dev bridge, without direct DB access.",
    )
    parser.add_argument("urls", nargs="*", help="contents URL(s) or pattern-like detail URL(s)")
    parser.add_argument("--db", required=True, help="MariaDB schema name on f1_dev")
    parser.add_argument("--chat-bot-id", default="", help="Optional chat_bot_id filter")
    parser.add_argument("--no-chat-bot-filter", action="store_true", help="Do not filter by chat_bot_id")
    parser.add_argument("--target-domains", default="", help="Comma-separated host scope override")
    parser.add_argument("--scope-path-prefix", default="", help="Explicit path prefix scope")
    parser.add_argument("--exploration-type", default="auto", help="post, all, empty_or_post, or auto(contents.do -> all)")
    parser.add_argument("--include-duplicates", action="store_true", help="Include rows marked merge_status=duplicate")
    parser.add_argument("--include-inactive", action="store_true", help="Include rows where is_active is not 1")
    parser.add_argument("--ignore-query-values", action="store_true", help="Do not require input query key/value pairs such as key=5806")
    parser.add_argument("--ignore-query-keys", default="page,pageindex,searchKeyword,keyword", help="Comma-separated query keys to ignore while matching values")
    parser.add_argument("--page-size", type=int, default=5000, help="Bridge page size, max 50000")
    parser.add_argument("--max-rows", type=int, default=0, help="Stop after scanning N bridge rows; 0 means all returned pages")
    parser.add_argument("--sample", type=int, default=10, help="Number of sample rows to print")
    parser.add_argument("--timeout", type=float, default=90.0, help="Bridge request timeout seconds")
    parser.add_argument("--retries", type=int, default=2, help="Retry count for transient bridge 502/503/504 errors")
    parser.add_argument("--bridge-base", default=DEFAULT_BRIDGE_BASE, help="f1_dev bridge API base URL")
    parser.add_argument("--bridge-path", default=DEFAULT_BRIDGE_PATH, help="Bridge API path")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format")
    return parser


def main() -> None:
    raise SystemExit(_run(_build_parser().parse_args()))


if __name__ == "__main__":
    main()
