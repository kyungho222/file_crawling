#!/usr/bin/env python3
"""Count seed URLs through the f1_dev bridge.

The bridge resolves contents -> learn_list id -> exploration post rows on the
remote backend. This script prints only the matching post count.

Usage:
  python scripts/load_seed_urls_by_contents.py "https://example.go.kr/board/list.do"
  python scripts/load_seed_urls_by_contents.py --db dev_user --format json "https://..."
  echo https://example.go.kr/board/list.do | python scripts/load_seed_urls_by_contents.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import requests

LEARN_LIST_TABLE = "ASADAL_CRAWLING_LEARN_LIST"
EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"
DEFAULT_BRIDGE_BASE = "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler"
DEFAULT_BRIDGE_PATH = "/backend/file-dashboard/exploration-posts"


def _log(message: str, *, quiet: bool = False) -> None:
    if not quiet:
        print(message, file=sys.stderr)


def _bridge_endpoint(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def _post_bridge_json(endpoint: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    session = requests.Session()
    session.trust_env = False
    response = session.post(endpoint, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected bridge response type: {type(data).__name__}")
    if data.get("ok") is False:
        raise RuntimeError(str(data.get("error") or data.get("message") or data))
    return data


def _count_seed_urls_via_bridge(
    *,
    db_name: str,
    contents: str,
    chat_bot_id: str | None,
    active_only: bool,
    include_duplicates: bool,
    first: bool,
    bridge_base: str,
    bridge_path: str,
    timeout: float,
) -> dict[str, Any]:
    endpoint = _bridge_endpoint(bridge_base, bridge_path)
    payload: dict[str, Any] = {
        "db_name": db_name,
        "contents_url": contents,
        "contents": [contents],
        "scope_by_contents_learn_list_id": True,
        "first_learn_list_match": bool(first),
        "limit": 1,
        "offset": 0,
        "active_only": active_only,
        "include_duplicates": include_duplicates,
        "method": "all",
    }
    if chat_bot_id:
        payload["chat_bot_id"] = chat_bot_id

    data = _post_bridge_json(endpoint, payload, timeout)
    try:
        count = int(data.get("total") or 0)
    except Exception:
        count = 0
    return {
        "count": count,
        "db_name": db_name,
        "contents": contents,
        "bridge_base": bridge_base,
        "bridge_path": bridge_path,
        "learn_list_ids": data.get("learn_list_ids") or [],
        "learn_list_scope": data.get("learn_list_scope") or {},
    }


def _run(args: argparse.Namespace) -> int:
    contents = str(args.contents or "").strip()
    if not contents and not sys.stdin.isatty():
        contents = sys.stdin.read().strip()
    if not contents:
        try:
            contents = input("contents: ").strip()
        except EOFError:
            contents = ""
    if not contents:
        print("contents value is required.", file=sys.stderr)
        return 2

    db_name = str(args.db or os.getenv("DB_NAME") or "dev_user").strip()
    chat_bot_id = str(args.chat_bot_id or "").strip() or None

    _log(f"[1/3] contents received: {contents}", quiet=args.quiet)
    _log(f"[2/3] f1_dev bridge resolves {LEARN_LIST_TABLE}.content -> id", quiet=args.quiet)
    _log(f"[3/3] bridge counts {EXPLORATION_TABLE} rows where learn_list_id matches and type=post", quiet=args.quiet)
    result = _count_seed_urls_via_bridge(
        db_name=db_name,
        contents=contents,
        chat_bot_id=chat_bot_id,
        active_only=not args.include_inactive,
        include_duplicates=args.include_duplicates,
        first=bool(args.first),
        bridge_base=args.bridge_base,
        bridge_path=args.bridge_path,
        timeout=args.timeout,
    )
    _log(f"[done] seed_urls count: {result['count']}", quiet=args.quiet)

    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(result["count"])
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Count matching exploration post seed URLs via f1_dev bridge."
    )
    parser.add_argument("contents", nargs="?", default="", help="contents value, usually the input/list URL")
    parser.add_argument("--db", default=os.getenv("DB_NAME", "dev_user"), help="MariaDB schema name")
    parser.add_argument("--chat-bot-id", default=None, help="Optional chat_bot_id filter when the bridge supports the column")
    parser.add_argument("--first", action="store_true", help="Use only the first matching learn_list row")
    parser.add_argument("--include-inactive", action="store_true", help="Include exploration rows where is_active is not 1")
    parser.add_argument("--include-duplicates", action="store_true", help="Include rows marked merge_status=duplicate")
    parser.add_argument("--timeout", type=float, default=90.0, help="Bridge request timeout seconds")
    parser.add_argument("--bridge-base", default=os.getenv("F1_DEV_BRIDGE_BASE", DEFAULT_BRIDGE_BASE), help="f1_dev bridge API base URL")
    parser.add_argument("--bridge-path", default=os.getenv("F1_DEV_BRIDGE_PATH", DEFAULT_BRIDGE_PATH), help="Bridge API path")
    parser.add_argument("--quiet", action="store_true", help="Hide step logs on stderr")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. text prints only the count.",
    )
    raise SystemExit(_run(parser.parse_args()))


if __name__ == "__main__":
    main()

