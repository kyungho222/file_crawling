#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
POST https://dev.chatbaram.com:9001/summarize_keywords 최소 호출.

요청 본문: chat_bot_id, db_name, contents, content_type, concurrency.

  python scripts/post_summarize_keywords.py ^
    --chat-bot-id <UUID> --db-name dev_user ^
    --contents "https://example.com/doc.pdf"

  여러 건:
  python scripts/post_summarize_keywords.py -c BOT -d DB --contents URL1 --contents URL2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "https://dev.chatbaram.com:9001/summarize_keywords"


def main() -> int:
    p = argparse.ArgumentParser(description="POST /summarize_keywords (간단 호출)")
    p.add_argument(
        "--url",
        default=(os.getenv("SUMMARIZE_KEYWORDS_URL") or "").strip() or DEFAULT_URL,
        help=f"엔드포인트 (기본: {DEFAULT_URL})",
    )
    p.add_argument("--chat-bot-id", "-c", default=os.getenv("CHAT_BOT_ID") or os.getenv("DEFAULT_CHAT_BOT_ID") or "")
    p.add_argument("--db-name", "-d", default=os.getenv("DB_NAME") or "")
    p.add_argument(
        "--contents",
        action="append",
        dest="contents",
        metavar="STR",
        help="요약 대상 URL/문자열 (여러 번 지정 가능)",
    )
    p.add_argument(
        "--content-type",
        default=(os.getenv("SUMMARIZE_KEYWORDS_CONTENT_TYPE") or "url").strip() or "url",
    )
    p.add_argument("--job-id", "-j", default=os.getenv("SUMMARIZE_JOB_ID") or "")
    p.add_argument("--concurrency", type=int, default=3)
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument("--print-request", action="store_true", help="보내는 JSON 출력")
    args = p.parse_args()

    contents = [s.strip() for s in (args.contents or []) if s and str(s).strip()]
    if not args.chat_bot_id or not args.db_name or not contents:
        p.error("--chat-bot-id, --db-name, --contents 가 필요합니다. (환경변수 CHAT_BOT_ID, DB_NAME 도 가능)")

    payload: dict = {
        "chat_bot_id": str(args.chat_bot_id),
        "db_name": str(args.db_name),
        "contents": contents,
        "content_type": str(args.content_type),
        "concurrency": int(args.concurrency),
    }

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if args.print_request:
        print(body.decode("utf-8"), file=sys.stderr)

    req = urllib.request.Request(
        args.url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            print(raw)
            return 0 if int(getattr(resp, "status", 200) or 200) == 200 else 1
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = str(e)
        print(f"HTTP {e.code}\n{err_body}", file=sys.stderr)
        return 1
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
