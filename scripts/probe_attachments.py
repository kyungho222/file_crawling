import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    import requests
except Exception:
    requests = None  # type: ignore[assignment]

from backend.file.file_download_workflow import FileDownloadWorkflow


DEFAULT_BRIDGE_BASE = "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler"
DEFAULT_POST_ROWS_PATH = "/backend/file-dashboard/exploration-posts"


def _compact_attachment(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kind": item.get("kind") or "url",
        "name": item.get("name") or item.get("title") or item.get("text") or "",
        "href": item.get("href") or "",
        "url": item.get("url") or item.get("href") or "",
        "method": item.get("method") or "GET",
        "params": item.get("params") or {},
        "raw": item.get("raw") or "",
    }


async def probe_url(url: str, *, use_playwright: bool = False, timeout_sec: float = 30.0) -> Dict[str, Any]:
    workflow = FileDownloadWorkflow()
    try:
        html = await workflow._fetch_html_static(url, timeout_sec=timeout_sec)
        fetch_method = "static"
        if (not html) and use_playwright:
            html = await workflow._fetch_html_playwright(url)
            fetch_method = "playwright"
        if not html:
            return {
                "url": url,
                "ok": False,
                "error": "html_fetch_failed",
                "attachment_count": 0,
                "attachments": [],
            }
        attachments = workflow._extract_attachment_links_generic(html, base_url=url)
        compact = [_compact_attachment(a) for a in attachments if isinstance(a, dict)]
        return {
            "url": url,
            "ok": True,
            "fetch_method": fetch_method,
            "html_len": len(html or ""),
            "attachment_count": len(compact),
            "attachments": compact,
        }
    except Exception as exc:
        return {
            "url": url,
            "ok": False,
            "error": repr(exc),
            "attachment_count": 0,
            "attachments": [],
        }
    finally:
        try:
            await workflow._close_playwright()
        except Exception:
            pass


def _extract_urls_from_bridge_response(data: Any) -> List[str]:
    urls: List[str] = []
    seen: set[str] = set()

    def push(value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            urls.append(text)

    if isinstance(data, dict):
        for key in ("urls", "post_urls", "contents", "content_urls"):
            value = data.get(key)
            if isinstance(value, list):
                for item in value:
                    push(item)
        rows = data.get("rows") or data.get("data") or data.get("items")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    push(row.get("url") or row.get("content") or row.get("contents_url") or row.get("source_page"))
                else:
                    push(row)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                push(item.get("url") or item.get("content") or item.get("contents_url") or item.get("source_page"))
            else:
                push(item)
    return urls


def load_urls_from_bridge(
    *,
    bridge_base: str,
    bridge_path: str,
    db_name: str,
    chat_bot_id: str,
    limit: int,
    status: str = "",
) -> List[str]:
    if requests is None:
        raise RuntimeError("requests is required for bridge mode")
    endpoint = bridge_base.rstrip("/") + "/" + bridge_path.lstrip("/")
    payload: Dict[str, Any] = {
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "limit": limit,
    }
    if status:
        payload["status"] = status
    session = requests.Session()
    session.trust_env = False
    response = session.post(endpoint, json=payload, timeout=60)
    response.raise_for_status()
    return _extract_urls_from_bridge_response(response.json())


async def load_urls_from_local_db(
    *,
    db_name: str,
    chat_bot_id: str,
    limit: int,
    status: str = "",
) -> List[str]:
    from db.mysql_db_config import mysql_execute_query
    from db.mariadb_save_update import (
        ensure_learn_list_standard_columns,
        get_account_identifier_from_chatbot_setup,
        get_learn_list_table_name,
        resolve_learn_list_table_name_for_chatbot,
    )

    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    if not table:
        table = get_learn_list_table_name(account_identifier)
    cols = await ensure_learn_list_standard_columns(db_name, table)
    if not cols or "content" not in cols:
        return []

    where = ["`content` IS NOT NULL", "TRIM(CAST(`content` AS CHAR)) <> ''"]
    params: List[Any] = []
    if "type" in cols:
        where.append("LOWER(COALESCE(`type`, '')) = 'post'")
    elif "content_type" in cols:
        where.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
    if status and "status" in cols:
        where.append("UPPER(COALESCE(`status`, '')) = %s")
        params.append(str(status).strip().upper())
    rows = await mysql_execute_query(
        f"""
        SELECT `content` AS url
        FROM `{table}`
        WHERE {' AND '.join(where)}
        ORDER BY `id` DESC
        LIMIT %s
        """,
        tuple(params + [max(1, min(int(limit or 100), 5000))]),
        fetch=True,
        dbname=db_name,
    )
    return _extract_urls_from_bridge_response({"rows": list(rows or [])})


def load_urls_from_file(path: str) -> List[str]:
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if text and not text.startswith("#"):
                urls.append(text)
    return urls


def _print_text_result(results: List[Dict[str, Any]]) -> None:
    total = len(results)
    found = sum(1 for row in results if int(row.get("attachment_count") or 0) > 0)
    print(f"URLs: {total}, with attachments: {found}")
    for row in results:
        print("")
        print(f"[{row.get('attachment_count', 0)}] {row.get('url')}")
        if not row.get("ok"):
            print(f"  ERROR: {row.get('error')}")
            continue
        for idx, attach in enumerate(row.get("attachments") or [], 1):
            print(f"  {idx}. ({attach.get('kind')}) {attach.get('name') or 'attachment'}")
            print(f"     {attach.get('href') or attach.get('url')}")
            if attach.get("method") and attach.get("method") != "GET":
                print(f"     method={attach.get('method')} params={attach.get('params')}")


async def run(args: argparse.Namespace) -> int:
    urls: List[str] = []
    if args.url:
        urls.extend(args.url)
    if args.file:
        urls.extend(load_urls_from_file(args.file))
    if args.bridge:
        if not args.db_name or not args.chat_bot_id:
            raise SystemExit("--bridge requires --db-name and --chat-bot-id")
        urls.extend(
            load_urls_from_bridge(
                bridge_base=args.bridge_base,
                bridge_path=args.bridge_path,
                db_name=args.db_name,
                chat_bot_id=args.chat_bot_id,
                limit=args.limit,
                status=args.status,
            )
        )

    deduped: List[str] = []
    seen: set[str] = set()
    for url in urls:
        text = str(url or "").strip()
        if text and text not in seen:
            seen.add(text)
            deduped.append(text)
    if args.max_urls:
        deduped = deduped[: args.max_urls]
    if not deduped:
        raise SystemExit("No URLs provided")

    sem = asyncio.Semaphore(max(1, args.concurrency))

    async def one(target: str) -> Dict[str, Any]:
        async with sem:
            return await probe_url(target, use_playwright=args.playwright, timeout_sec=args.timeout)

    results = await asyncio.gather(*(one(url) for url in deduped))
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        _print_text_result(results)
    return 0


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe attachment URLs using the current file crawler extraction logic.")
    parser.add_argument("url", nargs="*", help="Detail page URL(s) to probe.")
    parser.add_argument("--file", help="Text file containing one URL per line.")
    parser.add_argument("--bridge", action="store_true", help="Load type=post URLs through the f1_dev bridge endpoint.")
    parser.add_argument("--bridge-base", default=DEFAULT_BRIDGE_BASE, help="Bridge API base URL.")
    parser.add_argument("--bridge-path", default=DEFAULT_POST_ROWS_PATH, help="Bridge API path.")
    parser.add_argument("--db-name", "--db", dest="db_name", default="", help="DB name for bridge mode.")
    parser.add_argument("--chat-bot-id", default="", help="chat_bot_id for bridge mode.")
    parser.add_argument("--status", default="", help="Optional learn_list status filter for bridge mode, e.g. Y.")
    parser.add_argument("--limit", type=int, default=100, help="Bridge row limit.")
    parser.add_argument("--max-urls", type=int, default=0, help="Maximum URLs to probe after loading.")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent probe count.")
    parser.add_argument("--timeout", type=float, default=30.0, help="Static fetch timeout seconds.")
    parser.add_argument("--playwright", action="store_true", help="Retry with Playwright when static fetch fails.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text summary.")
    parser.add_argument("--output", help="Write JSON results to this file.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    if sys.platform == "win32":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        except Exception:
            pass
    raise SystemExit(asyncio.run(run(parse_args())))
