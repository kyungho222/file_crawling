#!/usr/bin/env python3
"""
파일 크롤링(colle=file) 시 API가 쓰는 것과 동일한 규칙으로
start_urls 규모를 조회한다. chat_bot_id·LEARN_LIST url_pattern(유효 include)이 있어야 하며,
exploration post 전체를 url_pattern 없이 쓰는 폴백은 없다.

대상 URL(크롤링 시 contents[0]에 넣는 값)을 주면, 그 URL에서 뽑은 도메인으로
LEARN_LIST 조회·url LIKE 에 쓴다(API의 file_crawl_post_urls 와 동일).

Usage:
  python scripts/count_file_crawl_start_urls.py "https://example.go.kr/bbs/list.do?mId=1"
  python scripts/count_file_crawl_start_urls.py --db dev_user --chat-bot-id mybot "https://..."
  python scripts/count_file_crawl_start_urls.py --target-domains "a.go.kr,b.go.kr" --db dev_user

환경변수: DB_NAME, DEFAULT_CHAT_BOT_ID (옵션 기본값)
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.shared.file_crawl_post_urls import (  # noqa: E402
    _extract_base_domain,
    stream_post_urls_for_file_crawl,
)


def _parse_domains(s: str | None) -> list[str] | None:
    if not s or not str(s).strip():
        return None
    return [x.strip() for x in str(s).split(",") if x.strip()]


async def _run(args: argparse.Namespace) -> int:
    url = (args.url or "").strip()
    if not url and not sys.stdin.isatty():
        url = sys.stdin.read().strip()

    if not url and args.target_domains:
        contents_url = None
        target_domains = _parse_domains(args.target_domains)
    elif url:
        contents_url = url
        target_domains = _parse_domains(args.target_domains)
    else:
        print("대상 URL을 인자로 주거나, --target-domains 로 도메인을 지정하세요.", file=sys.stderr)
        return 2

    db_name = args.db or os.getenv("DB_NAME", "dev_user")
    if args.no_chat_bot_filter:
        chat_bot_id = None
    else:
        chat_bot_id = (args.chat_bot_id or "").strip() or None

    derived = _extract_base_domain(contents_url) if contents_url else ""

    n = 0
    sample_list: list[str] = []
    need_sample = args.sample > 0
    async for chunk in stream_post_urls_for_file_crawl(
        db_name=db_name,
        target_domains=target_domains,
        contents_url=contents_url,
        chat_bot_id=chat_bot_id,
    ):
        n += len(chunk)
        if need_sample and len(sample_list) < args.sample:
            for u in chunk:
                if len(sample_list) >= args.sample:
                    break
                sample_list.append(u.get("url") if isinstance(u, dict) else u)

    print(f"start_urls_count: {n}")
    print(f"db_name: {db_name}")
    print(f"chat_bot_id: {chat_bot_id!r}")
    if target_domains:
        print(f"target_domains: {target_domains}")
    elif derived:
        print(f"domain_from_url: {derived}")
    else:
        print(
            "domain_from_url: (없음 — chat_bot_id·url_pattern 필수, exploration 은 봇·도메인 조건만 적용)"
        )

    if need_sample and sample_list:
        print(f"sample (max {args.sample}):")
        for u in sample_list:
            print(f"  {u}")

    return 0


def main() -> None:
    p = argparse.ArgumentParser(description="파일 크롤링용 start_urls( post URL ) 개수 조회")
    p.add_argument(
        "url",
        nargs="?",
        default="",
        help="크롤 요청의 contents[0]에 해당하는 URL(도메인 추출용). 생략 시 --target-domains 필수",
    )
    p.add_argument("--db", default=os.getenv("DB_NAME", "dev_user"), help="MariaDB 스키마/계정명")
    p.add_argument(
        "--chat-bot-id",
        default=None,
        metavar="ID",
        help="봇 ID(기본: 환경변수 DEFAULT_CHAT_BOT_ID)",
    )
    p.add_argument(
        "--no-chat-bot-filter",
        action="store_true",
        help="chat_bot_id 를 비움(현재 파일 크롤 규칙상 start_urls 는 항상 0)",
    )
    p.add_argument(
        "--target-domains",
        help="쉼표 구분 도메인(url LIKE). 지정 시 URL에서 도메인 추출을 덮어씀",
    )
    p.add_argument(
        "--sample",
        type=int,
        default=0,
        metavar="N",
        help="개수 외에 상위 N개 URL 출력",
    )
    args = p.parse_args()
    if args.no_chat_bot_filter:
        args.chat_bot_id = None
    elif args.chat_bot_id is None:
        args.chat_bot_id = os.getenv("DEFAULT_CHAT_BOT_ID")

    if not args.url.strip() and not args.target_domains:
        try:
            line = input("크롤링 대상 URL(contents[0]): ").strip()
        except EOFError:
            line = ""
        args.url = line

    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
