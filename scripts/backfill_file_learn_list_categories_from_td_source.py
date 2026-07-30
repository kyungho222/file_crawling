"""Backfill missing file LEARN_LIST categories from TD training metadata.

Dry-run by default. Intended to run on a server that can access both MariaDB and
PostgreSQL directly. It does not create category/schema/index objects.

Flow:
1. Keyset-scan LEARN_LIST by id only.
2. Pick learned file rows whose cate1/cate2 is missing.
3. Match file rows to td_*_training_data rows by content/subject.
4. Read detail page source_url and board category hints from TD content_metadata.
5. If metadata lacks category codes, optionally keyset-scan LEARN_LIST post rows
   to map source_url -> board cate1/cate2.
6. Map board category to an existing same-name child under the "??" category.
7. Optionally update file LEARN_LIST cate1/cate2 by id.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_project_env() -> None:
    for env_path in (PROJECT_ROOT / ".env", PROJECT_ROOT / "backend" / ".env"):
        if env_path.exists():
            load_dotenv(env_path, override=False)


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def report_path_default() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "reports" / f"file_category_backfill_from_td_source_{stamp}.json"


def quote_maria_ident(value: str) -> str:
    text = str(value or "").strip()
    if not _IDENT_RE.fullmatch(text):
        raise SystemExit(f"unsafe MariaDB identifier: {text!r}")
    return f"`{text}`"


def quote_pg_table(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit("empty PostgreSQL table name")
    parts = text.split(".") if "." in text else ["public", text]
    if len(parts) != 2 or not all(_IDENT_RE.fullmatch(p or "") for p in parts):
        raise SystemExit(f"unsafe PostgreSQL table name: {text!r}")
    return ".".join(f'"{p}"' for p in parts)


def normalize_row(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def is_missing_category(row: Dict[str, Any]) -> bool:
    return is_blank(row.get("cate1")) or is_blank(row.get("cate2"))

def row_status_matches(row: Dict[str, Any], desired_status: str) -> bool:
    status = str(desired_status or "").strip().upper()
    if not status:
        return True
    return str(row.get("status") or "").strip().upper() == status



def parse_metadata(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return dict(parsed)
        except Exception:
            return {}
    return {}


def first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def metadata_category_pair(meta: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(meta, dict):
        return "", ""
    cate1 = first_text(
        meta.get("cate1"),
        meta.get("store_cate1"),
        meta.get("assigned_cate1"),
        meta.get("ref_cate1"),
        meta.get("board_cate1"),
        meta.get("board_cate1_name"),
        meta.get("cate1_name"),
    )
    cate2 = first_text(
        meta.get("cate2"),
        meta.get("store_cate2"),
        meta.get("assigned_cate2"),
        meta.get("ref_cate2"),
        meta.get("board_cate2"),
        meta.get("board_cate2_name"),
        meta.get("cate2_name"),
        meta.get("category_hint"),
    )
    return cate1, cate2


def row_match_values(row: Dict[str, Any]) -> List[str]:
    values: List[str] = []
    for key in ("content", "subject", "memo1", "content_address"):
        text = str(row.get(key) or "").strip()
        if text and text not in values:
            values.append(text)
    return values


async def resolve_learn_table(args: argparse.Namespace) -> str:
    if args.learn_table:
        return str(args.learn_table).strip()
    from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot

    table = await resolve_learn_list_table_name_for_chatbot(args.chat_bot_id, args.db)
    if not table:
        raise SystemExit("learn_list table could not be resolved; pass --learn-table")
    return table


async def fetch_learn_batch(*, db_name: str, learn_table: str, after_id: int, batch_size: int) -> List[Dict[str, Any]]:
    from db.mysql_db_config import mysql_execute_query

    cols = [
        "id", "content", "subject", "content_type", "status", "chunk",
        "cate1", "cate2", "memo1", "content_address", "created_at",
    ]
    sql = f"""
        SELECT {', '.join(quote_maria_ident(c) for c in cols)}
        FROM {quote_maria_ident(learn_table)}
        WHERE `id` > %s
        ORDER BY `id` ASC
        LIMIT %s
    """
    rows = await mysql_execute_query(sql, (after_id, batch_size), fetch=True, dbname=db_name)
    return [normalize_row(row) for row in rows or [] if normalize_row(row)]


async def fetch_td_matches(*, db_name: str, pg_table: str, values: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    from db.db_operations import execute_query

    unique_values = [v for v in dict.fromkeys(str(x or "").strip() for x in values) if v]
    if not unique_values:
        return {}
    table_sql = quote_pg_table(pg_table)
    sql = f"""
        SELECT
            content,
            subject,
            content_metadata,
            content_metadata->>'source_url' AS source_url,
            COUNT(*)::int AS chunk_count
        FROM {table_sql}
        WHERE content = ANY($1::text[])
           OR subject = ANY($1::text[])
        GROUP BY content, subject, content_metadata, content_metadata->>'source_url'
    """
    rows = await execute_query(sql, (unique_values,), fetch=True, dbname=db_name)
    by_value: Dict[str, List[Dict[str, Any]]] = {}
    for raw in rows or []:
        row = normalize_row(raw)
        meta = parse_metadata(row.get("content_metadata"))
        item = {
            "content": str(row.get("content") or "").strip(),
            "subject": str(row.get("subject") or "").strip(),
            "source_url": str(row.get("source_url") or meta.get("source_url") or "").strip(),
            "metadata_cate1": metadata_category_pair(meta)[0],
            "metadata_cate2": metadata_category_pair(meta)[1],
            "chunk_count": int(row.get("chunk_count") or 0),
        }
        for key in (item["content"], item["subject"]):
            if key:
                by_value.setdefault(key, []).append(item)
    return by_value


async def scan_post_categories_by_source_url(
    *,
    db_name: str,
    learn_table: str,
    source_urls: Sequence[str],
    scan_batch_size: int,
    max_scan_rows: int,
) -> Dict[str, Tuple[str, str]]:
    from db.mysql_db_config import mysql_execute_query

    wanted = {str(url or "").strip() for url in source_urls if str(url or "").strip()}
    if not wanted:
        return {}
    found: Dict[str, Tuple[str, str]] = {}
    after_id = 0
    scanned = 0
    while wanted - set(found.keys()):
        if max_scan_rows and scanned >= max_scan_rows:
            break
        rows = await mysql_execute_query(
            f"""
            SELECT `id`, `content`, `content_type`, `cate1`, `cate2`
            FROM {quote_maria_ident(learn_table)}
            WHERE `id` > %s
            ORDER BY `id` ASC
            LIMIT %s
            """,
            (after_id, scan_batch_size),
            fetch=True,
            dbname=db_name,
        )
        rows_list = [normalize_row(row) for row in rows or [] if normalize_row(row)]
        if not rows_list:
            break
        scanned += len(rows_list)
        after_id = max(int(row.get("id") or after_id) for row in rows_list)
        for row in rows_list:
            content = str(row.get("content") or "").strip()
            if content not in wanted or content in found:
                continue
            ctype = str(row.get("content_type") or "").strip().lower()
            if ctype == "file":
                continue
            cate1 = str(row.get("cate1") or "").strip()
            cate2 = str(row.get("cate2") or "").strip()
            if cate1 or cate2:
                found[content] = (cate1, cate2)
    return found


async def update_file_category(*, db_name: str, learn_table: str, row_id: int, cate1: str, cate2: str) -> bool:
    from db.mysql_db_config import mysql_execute_query

    set_parts = ["`cate1` = %s", "`cate2` = %s"]
    params: List[Any] = [cate1, cate2, row_id]
    await mysql_execute_query(
        f"""
        UPDATE {quote_maria_ident(learn_table)}
        SET {', '.join(set_parts)}
        WHERE `id` = %s
          AND LOWER(COALESCE(`content_type`, '')) = 'file'
          AND (COALESCE(NULLIF(`cate1`, ''), '') = '' OR COALESCE(NULLIF(`cate2`, ''), '') = '')
        """,
        tuple(params),
        fetch=False,
        dbname=db_name,
    )
    return True


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    from db.mariadb_save_update import _ensure_file_learning_category_mapping

    learn_table = await resolve_learn_table(args)
    started = time.perf_counter()
    after_id = max(0, int(args.after_id or 0))
    processed = 0
    candidates = 0
    td_matched = 0
    category_resolved = 0
    updated = 0
    skipped: Dict[str, int] = {}
    samples: Dict[str, List[Dict[str, Any]]] = {
        "updated": [],
        "resolved": [],
        "skipped": [],
        "ambiguous": [],
    }
    category_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}

    def add_skip(reason: str, item: Dict[str, Any]) -> None:
        skipped[reason] = skipped.get(reason, 0) + 1
        if len(samples["skipped"]) < args.report_sample_limit:
            samples["skipped"].append({"reason": reason, **item})

    while True:
        if args.max_rows and processed >= args.max_rows:
            break
        rows = await fetch_learn_batch(
            db_name=args.db,
            learn_table=learn_table,
            after_id=after_id,
            batch_size=args.batch_size,
        )
        if not rows:
            break
        processed += len(rows)
        after_id = max(int(row.get("id") or after_id) for row in rows)
        file_rows = [
            row for row in rows
            if (
                str(row.get("content_type") or "").strip().lower() == "file"
                and row_status_matches(row, args.status)
                and is_missing_category(row)
            )
        ]
        if not file_rows:
            continue
        candidates += len(file_rows)
        lookup_values: List[str] = []
        for row in file_rows:
            for value in row_match_values(row):
                if value not in lookup_values:
                    lookup_values.append(value)
        td_by_value = await fetch_td_matches(db_name=args.pg_db or args.db, pg_table=args.pg_table, values=lookup_values)

        row_td: Dict[int, Dict[str, Any]] = {}
        source_urls: List[str] = []
        for row in file_rows:
            rid = int(row.get("id") or 0)
            matches: List[Dict[str, Any]] = []
            seen_sources: set[str] = set()
            for value in row_match_values(row):
                for item in td_by_value.get(value, []):
                    source = str(item.get("source_url") or "").strip()
                    dedupe_key = source or f"{item.get('content')}|{item.get('subject')}"
                    if dedupe_key in seen_sources:
                        continue
                    seen_sources.add(dedupe_key)
                    matches.append(item)
            if not matches:
                add_skip("td_no_match", {"id": rid, "subject": row.get("subject"), "content": row.get("content")})
                continue
            td_matched += 1
            source_set = {str(item.get("source_url") or "").strip() for item in matches if str(item.get("source_url") or "").strip()}
            if len(source_set) > 1:
                if len(samples["ambiguous"]) < args.report_sample_limit:
                    samples["ambiguous"].append({"id": rid, "subject": row.get("subject"), "sources": sorted(source_set)[:10]})
                add_skip("td_ambiguous_source_url", {"id": rid, "subject": row.get("subject")})
                continue
            selected = matches[0]
            row_td[rid] = selected
            source = str(selected.get("source_url") or "").strip()
            if source and source not in source_urls:
                source_urls.append(source)

        post_cates = {}
        if args.scan_post_learn_list and source_urls:
            post_cates = await scan_post_categories_by_source_url(
                db_name=args.db,
                learn_table=learn_table,
                source_urls=source_urls,
                scan_batch_size=args.post_scan_batch_size,
                max_scan_rows=args.post_scan_max_rows,
            )

        for row in file_rows:
            rid = int(row.get("id") or 0)
            td = row_td.get(rid)
            if not td:
                continue
            source_url = str(td.get("source_url") or "").strip()
            source_cate1 = str(td.get("metadata_cate1") or "").strip()
            source_cate2 = str(td.get("metadata_cate2") or "").strip()
            if (not source_cate1 and not source_cate2) and source_url in post_cates:
                source_cate1, source_cate2 = post_cates[source_url]
            if not (source_cate1 or source_cate2):
                add_skip("source_category_missing", {"id": rid, "subject": row.get("subject"), "source_url": source_url})
                continue
            cache_key = (source_cate1, source_cate2)
            if cache_key not in category_cache:
                category_cache[cache_key] = await _ensure_file_learning_category_mapping(
                    chat_bot_id=args.chat_bot_id,
                    db_name=args.db,
                    source_cate1=source_cate1,
                    source_cate2=source_cate2,
                    create_missing=False,
                )
            mapped_cate1, mapped_cate2 = category_cache[cache_key]
            if not (mapped_cate1 or mapped_cate2):
                add_skip("file_category_mapping_missing", {"id": rid, "source_cate1": source_cate1, "source_cate2": source_cate2})
                continue
            category_resolved += 1
            item = {
                "id": rid,
                "subject": row.get("subject"),
                "source_url": source_url,
                "source_cate1": source_cate1,
                "source_cate2": source_cate2,
                "mapped_cate1": mapped_cate1,
                "mapped_cate2": mapped_cate2,
            }
            if len(samples["resolved"]) < args.report_sample_limit:
                samples["resolved"].append(item)
            if args.apply:
                await update_file_category(
                    db_name=args.db,
                    learn_table=learn_table,
                    row_id=rid,
                    cate1=mapped_cate1,
                    cate2=mapped_cate2,
                )
                updated += 1
                if len(samples["updated"]) < args.report_sample_limit:
                    samples["updated"].append(item)
                if args.update_sleep_sec > 0:
                    await asyncio.sleep(args.update_sleep_sec)

    return {
        "status": "success",
        "dry_run": not args.apply,
        "db": args.db,
        "pg_db": args.pg_db or args.db,
        "pg_table": args.pg_table,
        "chat_bot_id": args.chat_bot_id,
        "learn_table": learn_table,
        "processed": processed,
        "candidate_missing_category": candidates,
        "td_matched": td_matched,
        "category_resolved": category_resolved,
        "updated": updated,
        "last_id": after_id,
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "skipped": skipped,
        "samples": samples,
    }


async def cleanup() -> None:
    try:
        from db.rdbms_router import rdbms_cleanup_on_shutdown
        await rdbms_cleanup_on_shutdown()
    except Exception:
        pass
    try:
        from config.settings import DatabasePool
        await DatabasePool.close_all_pools()
    except Exception:
        pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill missing file learn_list categories from TD source_url metadata.")
    parser.add_argument("--db", required=True, help="MariaDB/PostgreSQL logical DB name, e.g. jongno.")
    parser.add_argument("--pg-db", default="", help="PostgreSQL DB name. Defaults to --db.")
    parser.add_argument("--chat-bot-id", required=True, help="Chatbot UUID used to resolve LEARN_LIST and CATEGORY tables.")
    parser.add_argument("--learn-table", default="", help="Override LEARN_LIST table name.")
    parser.add_argument("--pg-table", required=True, help="PostgreSQL td_*_training_data table.")
    parser.add_argument("--batch-size", type=int, default=300, help="LEARN_LIST id-keyset scan batch size.")
    parser.add_argument("--status", default="Y", help="Only backfill rows with this LEARN_LIST status. Empty string means all statuses.")
    parser.add_argument("--max-rows", type=int, default=0, help="Stop after scanning N LEARN_LIST rows. 0 means no limit.")
    parser.add_argument("--after-id", type=int, default=0, help="Resume after this LEARN_LIST id.")
    parser.add_argument("--scan-post-learn-list", action="store_true", help="If TD metadata lacks category, scan post rows by source_url using id keyset.")
    parser.add_argument("--post-scan-batch-size", type=int, default=1000, help="Post row id-keyset scan batch size.")
    parser.add_argument("--post-scan-max-rows", type=int, default=0, help="Max post rows to scan for source_url category fallback. 0 means no limit.")
    parser.add_argument("--apply", action="store_true", help="Apply cate1/cate2 updates. Default is dry-run.")
    parser.add_argument("--update-sleep-sec", type=float, default=0.02, help="Small delay after each update when --apply is used.")
    parser.add_argument("--report", default="", help="JSON report path. Defaults to reports/file_category_backfill_from_td_source_*.json")
    parser.add_argument("--report-sample-limit", type=int, default=1000, help="Max sample rows per report bucket.")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    load_project_env()
    parser = build_parser()
    args = parser.parse_args(argv)
    args.batch_size = max(1, min(int(args.batch_size or 300), 2000))
    args.status = str(args.status or "").strip().upper()
    args.post_scan_batch_size = max(1, min(int(args.post_scan_batch_size or 1000), 5000))
    args.report_sample_limit = max(1, int(args.report_sample_limit or 1000))
    if not args.pg_db:
        args.pg_db = args.db
    report_path = Path(args.report) if args.report else report_path_default()
    try:
        report = asyncio.run(run(args))
    finally:
        try:
            asyncio.run(cleanup())
        except Exception:
            pass
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    summary = {k: report.get(k) for k in ("status", "dry_run", "processed", "candidate_missing_category", "td_matched", "category_resolved", "updated", "last_id", "elapsed_sec")}
    summary["skipped"] = report.get("skipped", {})
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
