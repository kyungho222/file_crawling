#!/usr/bin/env python3
"""
파일 크롤링으로 적재된 LEARN_LIST 행에서, 메모에 쓰기 좋은 필드만 조회·출력한다.

- 대분류 / 소분류: cate1, cate2
- 작성일: content_created_at
- 글쓴이: content_author

(실제 DB 컬럼 memo1/memo와 별개로, 위 네 값이 어떻게 보이는지 확인용)

Usage:
  python scripts/preview_file_crawl_memo_fields.py --db dev_user --chat-bot-id <uuid>
  python scripts/preview_file_crawl_memo_fields.py --db dev_user --limit 20
  python scripts/preview_file_crawl_memo_fields.py --db dev_user --id 12345
  python scripts/preview_file_crawl_memo_fields.py --db dev_user --json --limit 50

환경: db/mysql_db_config 가 로드하는 .env (DB 접속)
기본 chat_bot_id: chatbot_setup 최신 행 또는 환경변수 DEFAULT_CHAT_BOT_ID
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _format_memo_block(
    cate1: Any,
    cate2: Any,
    content_created_at: Any,
    content_author: Any,
) -> str:
    c1 = (str(cate1).strip() if cate1 is not None else "") or "-"
    c2 = (str(cate2).strip() if cate2 is not None else "") or "-"
    dt = (str(content_created_at).strip() if content_created_at is not None else "") or "-"
    au = (str(content_author).strip() if content_author is not None else "") or "-"
    lines = [
        f"대분류: {c1}",
        f"소분류: {c2}",
        f"작성일: {dt}",
        f"글쓴이: {au}",
    ]
    return "\n".join(lines)


def _file_row_filter_sql(cols: set) -> tuple[str, tuple]:
    """content_type / crawl_type 컬럼 존재 여부에 맞춰 WHERE 절 구성."""
    has_ct = "content_type" in cols
    has_crawl = "crawl_type" in cols
    if has_ct and has_crawl:
        return "(content_type = %s OR crawl_type = %s)", ("file", "file")
    if has_ct:
        return "content_type = %s", ("file",)
    if has_crawl:
        return "crawl_type = %s", ("file",)
    return "1=1", tuple()


async def _run(args: argparse.Namespace) -> int:
    from db.mysql_db_config import mysql_execute_query
    from db.mariadb_save_update import (
        ensure_learn_list_standard_columns,
        get_account_identifier_from_chatbot_setup,
        get_latest_chat_bot_id_from_chatbot_setup,
        get_learn_list_table_name,
    )

    db_name = (args.db or os.getenv("DB_NAME") or "").strip()
    if not db_name:
        print("--db 또는 환경변수 DB_NAME 이 필요합니다.", file=sys.stderr)
        return 2

    chat_bot_id = (args.chat_bot_id or "").strip() or None
    if not chat_bot_id:
        chat_bot_id = await get_latest_chat_bot_id_from_chatbot_setup(db_name)
    if not chat_bot_id:
        print("chat_bot_id 를 --chat-bot-id 로 주거나 chatbot_setup 에 행이 있어야 합니다.", file=sys.stderr)
        return 2

    account_id = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    table = get_learn_list_table_name(account_id)
    cols = await ensure_learn_list_standard_columns(db_name, table)
    if not cols:
        print(f"테이블 컬럼을 읽지 못했습니다: {db_name}.{table}", file=sys.stderr)
        return 1

    need = ["id", "cate1", "cate2", "content_created_at", "content_author"]
    select_cols = [c for c in need if c in cols]
    if len(select_cols) < len(need):
        missing = [c for c in need if c not in cols]
        print(f"경고: 다음 컬럼이 없어 제외됩니다: {missing}", file=sys.stderr)

    extra = []
    for opt in ("subject", "memo1", "memo", "content_type", "crawl_type"):
        if opt in cols and opt not in select_cols:
            extra.append(opt)
    select_cols.extend(extra)

    where_extra = ""
    params: List[Any] = []
    if args.id is not None:
        where_extra = " AND id = %s"
        params.append(int(args.id))
    else:
        cond, cond_params = _file_row_filter_sql(cols)
        where_extra = f" AND ({cond})"
        params.extend(list(cond_params))

    limit = max(1, min(int(args.limit or 50), 5000))
    if args.id is not None:
        sql = f"SELECT {', '.join('`' + c + '`' for c in select_cols)} FROM `{table}` WHERE 1=1{where_extra} LIMIT 1"
    else:
        sql = (
            f"SELECT {', '.join('`' + c + '`' for c in select_cols)} FROM `{table}` "
            f"WHERE 1=1{where_extra} ORDER BY id DESC LIMIT {limit}"
        )

    try:
        rows = await mysql_execute_query(sql, tuple(params), fetch=True, dbname=db_name)
    except Exception as e:
        print(f"쿼리 실패: {e}", file=sys.stderr)
        return 1

    if not rows:
        print("(조건에 맞는 행 없음)")
        return 0

    if args.json:
        out_rows: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            rid = row.get("id")
            memo_block = _format_memo_block(
                row.get("cate1"),
                row.get("cate2"),
                row.get("content_created_at"),
                row.get("content_author"),
            )
            out_rows.append(
                {
                    "id": rid,
                    "cate1": row.get("cate1"),
                    "cate2": row.get("cate2"),
                    "content_created_at": row.get("content_created_at"),
                    "content_author": row.get("content_author"),
                    "memo_preview": memo_block,
                }
            )
        print(json.dumps(out_rows, ensure_ascii=False, indent=2))
        return 0

    print(f"db={db_name} table={table} rows={len(rows)}")
    _cond_only, _ = _file_row_filter_sql(cols)
    if args.id is None and _cond_only == "1=1":
        print(
            "(경고: content_type·crawl_type 컬럼이 없어 파일 행 필터를 쓰지 못했습니다. id 역순 상위 행만 표시합니다.)",
            file=sys.stderr,
        )

    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        rid = row.get("id")
        print(f"\n{'=' * 60}\nid={rid}")
        print(_format_memo_block(
            row.get("cate1"),
            row.get("cate2"),
            row.get("content_created_at"),
            row.get("content_author"),
        ))
        for k in extra:
            if k in row and row.get(k) is not None:
                v = row.get(k)
                s = str(v)
                if len(s) > 200:
                    s = s[:200] + "…"
                print(f"[{k}] {s}")

    return 0


def main() -> None:
    p = argparse.ArgumentParser(description="파일 크롤 LEARN_LIST — 메모용 필드(cate1/2, 작성일, 글쓴이) 미리보기")
    p.add_argument("--db", default=os.getenv("DB_NAME", ""), help="MariaDB 스키마( DB_NAME )")
    p.add_argument("--chat-bot-id", default=os.getenv("DEFAULT_CHAT_BOT_ID", ""), help="LEARN_LIST 테이블 UUID 결정")
    p.add_argument("--limit", type=int, default=20, help="최근 id 기준 최대 행 수 (--id 사용 시 무시)")
    p.add_argument("--id", type=int, default=None, help="특정 learn_list id 한 건만")
    p.add_argument("--json", action="store_true", help="JSON 배열로 출력")
    args = p.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
