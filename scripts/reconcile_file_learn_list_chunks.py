"""Call the f1_dev bridge endpoint to reconcile file learn_list chunks.

Local machines cannot connect to the 10.20.* DB network directly. This script
therefore posts a dry-run/apply request to the f1_dev HTTP bridge API. The
server-side endpoint performs the MariaDB/PostgreSQL reads and optional
MariaDB chunk updates.

Default behavior is dry-run/report only:
- scan MariaDB learn_list rows where content_type='file'
- match PostgreSQL td_*_training_data rows by content/subject/metadata source URL
- report rows that can receive a chunk update
- report rows whose PG chunks are missing and should be reviewed/relearned
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRIDGE_BASE = "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler"
DEFAULT_BRIDGE_PATH = "/backend/learn-list/file-chunk-reconcile"
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    return PROJECT_ROOT / "reports" / f"file_learn_list_chunk_reconcile_{stamp}.json"


def build_pg_table(args: argparse.Namespace) -> str:
    if args.pg_table:
        return args.pg_table.strip()
    if args.chat_id:
        chat_id = str(args.chat_id).strip().lower()
        if not _IDENT_RE.fullmatch(chat_id):
            raise SystemExit(f"unsafe chat id for pg table: {chat_id!r}")
        return f"td_{chat_id}_training_data"
    raise SystemExit("provide --pg-table or --chat-id")


def bridge_url(base: str, path: str) -> str:
    base = str(base or DEFAULT_BRIDGE_BASE).rstrip("/") + "/"
    return urljoin(base, str(path or DEFAULT_BRIDGE_PATH).lstrip("/"))


def post_json_with_retry(url: str, payload: Dict[str, Any], *, timeout: float, retries: int) -> Dict[str, Any]:
    try:
        import requests
    except ImportError as exc:
        raise SystemExit("requests package is required for f1_dev bridge calls") from exc

    session = requests.Session()
    session.trust_env = False
    last_error: Optional[BaseException] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = session.post(url, json=payload, timeout=timeout)
            if response.status_code in {502, 503, 504} and attempt < retries:
                time.sleep(min(2.0 * attempt, 8.0))
                continue
            if response.status_code >= 400:
                body = response.text[:2000]
                raise SystemExit(f"bridge request failed: HTTP {response.status_code} {body}")
            data = response.json()
            if not isinstance(data, dict):
                raise SystemExit("bridge returned non-object JSON")
            return data
        except SystemExit:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2.0 * attempt, 8.0))
                continue
            break
    raise SystemExit(f"bridge request failed: {last_error}")


def print_counts(report: Dict[str, Any]) -> None:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    counts = {
        "status": report.get("status") or report.get("ok"),
        "dry_run": report.get("dry_run"),
        "db": report.get("db") or report.get("db_name"),
        "pg_table": report.get("pg_table"),
        "learn_table": report.get("learn_table"),
        "processed": report.get("processed", 0),
        "batches": report.get("batches", 0),
        "last_id": report.get("last_id", 0),
        **summary,
    }
    print(json.dumps(counts, ensure_ascii=False, indent=2, default=json_default))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile file learn_list.chunk values through the f1_dev bridge endpoint.")
    parser.add_argument("--db", required=True, help="Target customer DB/schema, e.g. jongno.")
    parser.add_argument("--bridge-db", default="f1_dev", help="Compatibility label for the f1_dev bridge. Not used as a direct DB connection.")
    parser.add_argument("--bridge-base", default=os.getenv("F1_DEV_BRIDGE_BASE", DEFAULT_BRIDGE_BASE), help="f1_dev bridge API base URL.")
    parser.add_argument("--bridge-path", default=DEFAULT_BRIDGE_PATH, help="Bridge endpoint path.")
    parser.add_argument("--learn-table", default="ASADAL_CRAWLING_LEARN_LIST", help="MariaDB learn_list table name.")
    parser.add_argument("--pg-db", default="", help="PostgreSQL DB name. Defaults to --db.")
    parser.add_argument("--pg-table", default="", help="PostgreSQL training table, e.g. td_xxx_training_data.")
    parser.add_argument("--chat-id", default="", help="Build pg table as td_{chat_id}_training_data when --pg-table is omitted.")
    parser.add_argument("--chat-bot-id", default="", help="Optional chatbot id for server-side learn_list table resolution.")
    parser.add_argument("--batch-size", type=int, default=100, help="Rows per server DB batch.")
    parser.add_argument("--max-rows", type=int, default=0, help="Stop after N MariaDB rows. 0 means no server-side limit.")
    parser.add_argument("--max-id", type=int, default=0, help="Optional upper id bound for MariaDB keyset scan.")
    parser.add_argument("--after-id", type=int, default=0, help="Resume after this MariaDB id.")
    parser.add_argument("--include-existing-chunk", action="store_true", help="Also inspect rows whose MariaDB chunk is already nonblank.")
    parser.add_argument("--match-mode", choices=("broad", "content_only"), default="broad", help="PG match strategy. broad checks td.content, td.subject, and td.content_metadata.source_url.")
    parser.add_argument("--apply-chunk-update", action="store_true", help="Actually update MariaDB chunk for rows with matching PG chunks.")
    parser.add_argument("--report", default="", help="JSON report path. Defaults to reports/file_learn_list_chunk_reconcile_*.json")
    parser.add_argument("--report-sample-limit", type=int, default=5000, help="Max rows per report section returned by the server.")
    parser.add_argument("--timeout", type=float, default=300.0, help="HTTP request timeout seconds.")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry count for transient bridge failures.")
    parser.add_argument("--counts-only", action="store_true", help="Print only summary counts after writing the JSON report.")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    pg_table = build_pg_table(args)
    if not args.pg_db:
        args.pg_db = args.db
    payload: Dict[str, Any] = {
        "db_name": args.db,
        "bridge_db": args.bridge_db,
        "learn_table": args.learn_table,
        "pg_db": args.pg_db,
        "pg_table": pg_table,
        "chat_bot_id": args.chat_bot_id,
        "batch_size": max(1, min(int(args.batch_size or 100), 500)),
        "max_rows": max(0, int(args.max_rows or 0)),
        "max_id": max(0, int(args.max_id or 0)),
        "after_id": max(0, int(args.after_id or 0)),
        "include_existing_chunk": bool(args.include_existing_chunk),
        "match_mode": args.match_mode,
        "apply_chunk_update": bool(args.apply_chunk_update),
        "report_sample_limit": max(1, int(args.report_sample_limit or 5000)),
    }
    url = bridge_url(args.bridge_base, args.bridge_path)
    report = post_json_with_retry(url, payload, timeout=max(1.0, float(args.timeout or 300.0)), retries=max(1, int(args.retries or 3)))
    out_path = Path(args.report) if args.report else report_path_default()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    if not args.counts_only:
        print(f"[reconcile] bridge={url}")
        print(f"[reconcile] report={out_path}")
    print_counts(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
