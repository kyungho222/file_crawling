"""Migrate rows from one logical DB to another through an HTTP endpoint.

The script reads rows from a source DB using the project's RDBMS router, then
POSTs each batch to an endpoint that performs the target DB write.

Example:
    python scripts/endpoint_db_bridge_migrate.py ^
      --source-db dev_user ^
      --target-db prod_user ^
      --endpoint-url http://127.0.0.1:8000/Ai_Pro_filecrawler/backend/learn-list/board-gap/save-posts ^
      --table ASADAL_BOARD_EXPLORATION ^
      --columns url,title,published_at,source_page,learn_list_id ^
      --where "type = 'post'" ^
      --rows-field posts ^
      --extra-json "{\"chat_bot_id\":\"user-bot-...\"}" ^
      --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))



def load_project_env() -> None:
    for env_path in (
        PROJECT_ROOT / ".env",
        PROJECT_ROOT / "backend" / ".env",
    ):
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


def normalize_row(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        source = row
    else:
        source = dict(row)
    return {
        str(key): json.loads(json.dumps(value, default=json_default, ensure_ascii=False))
        for key, value in source.items()
    }


def parse_extra_json(raw: str) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--extra-json is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SystemExit("--extra-json must be a JSON object")
    return parsed


def load_sql(args: argparse.Namespace) -> Optional[str]:
    if args.query_file:
        return Path(args.query_file).read_text(encoding="utf-8").strip()
    if args.query:
        return args.query.strip()
    return None


def quote_identifier(name: str) -> str:
    safe = str(name or "").strip()
    if not safe:
        raise SystemExit("empty SQL identifier")
    if "`" in safe or "\x00" in safe:
        raise SystemExit(f"unsafe SQL identifier: {safe!r}")
    return f"`{safe}`"


def build_table_query(args: argparse.Namespace, after_pk: Optional[Any]) -> Tuple[str, Tuple[Any, ...]]:
    columns = [
        col.strip()
        for col in str(args.columns or "*").split(",")
        if col.strip()
    ]
    if not columns:
        raise SystemExit("--columns produced no columns")
    select_sql = "*" if columns == ["*"] else ", ".join(quote_identifier(col) for col in columns)
    where_parts: List[str] = []
    params: List[Any] = []
    if args.where:
        where_parts.append(f"({args.where})")
    if args.pk and after_pk is not None:
        where_parts.append(f"{quote_identifier(args.pk)} > %s")
        params.append(after_pk)
    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    order_sql = f" ORDER BY {quote_identifier(args.pk)} ASC" if args.pk else ""
    sql = (
        f"SELECT {select_sql} FROM {quote_identifier(args.table)}"
        f"{where_sql}{order_sql} LIMIT %s"
    )
    params.append(args.batch_size)
    return sql, tuple(params)


async def fetch_batch(
    args: argparse.Namespace,
    *,
    offset: int,
    after_pk: Optional[Any],
    base_query: Optional[str],
) -> List[Dict[str, Any]]:
    from db.rdbms_router import rdbms_execute_query

    if base_query:
        paged_query = f"SELECT * FROM ({base_query}) AS bridge_src LIMIT %s OFFSET %s"
        params: Tuple[Any, ...] = (args.batch_size, offset)
    else:
        paged_query, params = build_table_query(args, after_pk)
    rows = await rdbms_execute_query(
        paged_query,
        params=params,
        fetch=True,
        dbname=args.source_db,
    )
    return [normalize_row(row) for row in rows or []]


def build_payload(args: argparse.Namespace, rows: List[Dict[str, Any]], batch_no: int) -> Dict[str, Any]:
    payload = parse_extra_json(args.extra_json)
    payload[args.target_db_field] = args.target_db
    payload[args.rows_field] = rows
    payload.setdefault("source_db", args.source_db)
    payload.setdefault("migration_batch_no", batch_no)
    if args.job_id:
        payload.setdefault("job_id", args.job_id)
    if args.chat_bot_id:
        payload.setdefault("chat_bot_id", args.chat_bot_id)
    if args.dry_run:
        payload[args.dry_run_field] = True
    return payload


def response_ok(data: Dict[str, Any], status_code: int) -> bool:
    if status_code >= 400:
        return False
    for key in ("ok", "success"):
        if key in data:
            return bool(data.get(key))
    status = str(data.get("status") or "").lower()
    return status not in {"error", "failed", "fail"}


async def post_batch(
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    rows: List[Dict[str, Any]],
    batch_no: int,
) -> Dict[str, Any]:
    payload = build_payload(args, rows, batch_no)
    last_error: Optional[BaseException] = None
    for attempt in range(1, args.retries + 2):
        try:
            response = await client.post(args.endpoint_url, json=payload)
            try:
                data = response.json()
            except Exception:
                data = {"raw_response": response.text[:1000]}
            if response_ok(data, response.status_code):
                return {
                    "ok": True,
                    "status_code": response.status_code,
                    "response": data,
                }
            last_error = RuntimeError(
                f"endpoint returned HTTP {response.status_code}: "
                f"{json.dumps(data, ensure_ascii=False)[:1000]}"
            )
        except Exception as exc:
            last_error = exc
        if attempt <= args.retries:
            await asyncio.sleep(args.retry_delay * attempt)
    raise RuntimeError(f"batch {batch_no} failed: {last_error}") from last_error


def append_jsonl(path: Optional[str], item: Dict[str, Any]) -> None:
    if not path:
        return
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False, default=json_default) + "\n")


async def migrate(args: argparse.Namespace) -> int:
    base_query = load_sql(args)
    if not base_query and not args.table:
        raise SystemExit("provide --query/--query-file or --table")
    if base_query and args.pk:
        print("[bridge] --pk is ignored when --query/--query-file is used; LIMIT/OFFSET pagination is used.")

    headers = parse_extra_json(args.headers_json)
    timeout = httpx.Timeout(args.timeout)
    processed = 0
    batch_no = 0
    offset = args.offset
    after_pk: Optional[Any] = args.after_pk
    started = time.perf_counter()

    async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
        while True:
            rows = await fetch_batch(args, offset=offset, after_pk=after_pk, base_query=base_query)
            if not rows:
                break
            batch_no += 1
            result = await post_batch(client, args, rows, batch_no)
            processed += len(rows)
            if args.pk and not base_query:
                after_pk = rows[-1].get(args.pk)
            else:
                offset += len(rows)
            append_jsonl(
                args.audit_jsonl,
                {
                    "batch_no": batch_no,
                    "rows": len(rows),
                    "processed": processed,
                    "last_pk": after_pk,
                    "status_code": result.get("status_code"),
                    "response": result.get("response"),
                },
            )
            print(
                f"[bridge] batch={batch_no} rows={len(rows)} "
                f"processed={processed} last_pk={after_pk}"
            )
            if args.max_rows and processed >= args.max_rows:
                break

    elapsed = time.perf_counter() - started
    print(f"[bridge] done processed={processed} batches={batch_no} elapsed_sec={elapsed:.2f}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Move source DB rows to a target DB through a bridge HTTP endpoint."
    )
    parser.add_argument("--source-db", required=True, help="Logical source DB name used by db.rdbms_router.")
    parser.add_argument("--target-db", required=True, help="Logical target DB name passed to the endpoint.")
    parser.add_argument("--endpoint-url", required=True, help="Endpoint that writes the batch into the target DB.")
    parser.add_argument("--table", help="Source table name. Alternative to --query/--query-file.")
    parser.add_argument("--columns", default="*", help="Comma-separated source columns when --table is used.")
    parser.add_argument("--where", default="", help="Optional SQL WHERE fragment when --table is used.")
    parser.add_argument("--query", help="Source SELECT query. It will be wrapped for LIMIT/OFFSET pagination.")
    parser.add_argument("--query-file", help="File containing the source SELECT query.")
    parser.add_argument("--pk", default="id", help="Keyset pagination column for --table mode. Use empty string for offset.")
    parser.add_argument("--after-pk", default=None, help="Resume --table mode after this primary-key value.")
    parser.add_argument("--offset", type=int, default=0, help="Initial offset for --query mode or --table without --pk.")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per endpoint request.")
    parser.add_argument("--max-rows", type=int, default=0, help="Stop after this many rows. 0 means no limit.")
    parser.add_argument("--rows-field", default="rows", help="Payload field containing row list, e.g. posts/targets/rows.")
    parser.add_argument("--target-db-field", default="db_name", help="Payload field used for target DB name.")
    parser.add_argument("--extra-json", default="", help="Extra JSON object merged into every endpoint payload.")
    parser.add_argument("--headers-json", default="", help="JSON object for request headers.")
    parser.add_argument("--chat-bot-id", default="", help="Convenience field added as chat_bot_id.")
    parser.add_argument("--job-id", default="", help="Convenience field added as job_id.")
    parser.add_argument("--dry-run", action="store_true", help="Set the endpoint dry-run flag in every payload.")
    parser.add_argument("--dry-run-field", default="dry_run", help="Payload field name for --dry-run.")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout seconds.")
    parser.add_argument("--retries", type=int, default=2, help="Retries per failed batch.")
    parser.add_argument("--retry-delay", type=float, default=1.0, help="Base retry delay seconds.")
    parser.add_argument("--audit-jsonl", default="", help="Optional JSONL path for batch audit records.")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    load_project_env()
    parser = build_parser()
    args = parser.parse_args(argv)
    args.pk = str(args.pk or "").strip()
    try:
        return asyncio.run(migrate(args))
    finally:
        try:
            from db.rdbms_router import rdbms_cleanup_on_shutdown

            asyncio.run(rdbms_cleanup_on_shutdown())
        except RuntimeError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())


