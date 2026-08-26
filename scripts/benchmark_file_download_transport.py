"""Run one attachment through HTTP-only or Playwright-only production download code.

This is an operational comparison tool.  It does not write MariaDB, PG, Redis,
websync, or learning records.  Downloaded files are removed unless --keep-file
is supplied.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare one file with HTTP-only or PW-only download.")
    parser.add_argument("--post-url", required=True, help="Board detail page URL")
    parser.add_argument("--file-name", required=True, help="Exact attachment display filename")
    parser.add_argument("--transport", choices=("http", "pw"), required=True)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--output-dir", default=str(ROOT / "tmp" / "file_download_transport"))
    parser.add_argument("--browser-executable", default="", help="Optional local Chrome/Chromium executable for PW-only verification.")
    parser.add_argument("--keep-file", action="store_true")
    return parser.parse_args()


async def _extract_attachment(post_url: str, filename: str) -> dict[str, Any]:
    from backend.file.fast_attachment_extractor import extract_fast_attachments

    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9"}
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, trust_env=False) as session:
        async with session.get(post_url) as response:
            response.raise_for_status()
            html = await response.text(errors="ignore")
    attachments = extract_fast_attachments(html, post_url)
    expected = str(filename or "").strip()
    for item in attachments:
        if str(item.get("name") or "").strip() == expected:
            return dict(item)
    discovered = [str(item.get("name") or "") for item in attachments]
    raise RuntimeError(f"attachment_not_found expected={expected!r} discovered={discovered!r}")


async def _drain(queue: asyncio.Queue) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            return rows
        try:
            if isinstance(item, dict):
                rows.append(item)
        finally:
            queue.task_done()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from playwright.async_api import async_playwright

    from config.settings import settings
    from core.crawler.batch_queue import BatchQueue
    from core.crawler.browser_launch import filter_launch_args, get_default_launch_args
    from core.crawler.download_browser_pool import DownloadBrowserPool
    from core.crawler.workers.download import download_worker

    attachment = await _extract_attachment(args.post_url, args.file_name)
    job_id = f"transport-{args.transport}-{uuid.uuid4().hex[:10]}"
    output_dir = (Path(args.output_dir) / job_id).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "job_id": job_id,
        "url": attachment.get("url"),
        "name": attachment.get("name"),
        "subject": attachment.get("name"),
        "attachment_name": attachment.get("name"),
        "source_page": args.post_url,
        "source_url": args.post_url,
        "sync_after_download": False,
        "skip_study_worker": True,
        "defer_save_batch_until_learn_list": False,
    }

    previous_download_path = settings.DOWNLOAD_PATH
    previous_pw_attempts = os.environ.get("DOWNLOAD_PLAYWRIGHT_MAX_ATTEMPTS_PER_URL")
    settings.DOWNLOAD_PATH = output_dir
    os.environ["DOWNLOAD_PLAYWRIGHT_MAX_ATTEMPTS_PER_URL"] = "1"

    input_queue = BatchQueue(batch_size=1)
    output_queue = BatchQueue(batch_size=1)
    progress_queue: asyncio.Queue = asyncio.Queue()
    playwright = None
    browser_pool = None
    worker: asyncio.Task | None = None
    events: list[dict[str, Any]] = []

    async def _launch_download_browser() -> Any:
        assert playwright is not None
        launch_kwargs: dict[str, Any] = {
            "headless": True,
            "args": filter_launch_args(get_default_launch_args()),
        }
        if args.browser_executable:
            launch_kwargs["executable_path"] = args.browser_executable
        return await playwright.chromium.launch(
            **launch_kwargs,
        )

    try:
        if args.transport == "pw":
            playwright = await async_playwright().start()
            browser_pool = DownloadBrowserPool(
                _launch_download_browser,
                max_browsers=1,
                label="transport-benchmark",
            )
        worker = asyncio.create_task(
            download_worker(
                input_queue,
                output_queue,
                progress_queue,
                max_concurrent=1,
                worker_id=1,
                direct_http_enabled=(args.transport == "http"),
                download_browser_getter=(browser_pool.acquire if browser_pool else None),
                download_browser_releaser=(browser_pool.release if browser_pool else None),
            ),
            name=f"transport-benchmark-{args.transport}",
        )
        started = time.perf_counter()
        await input_queue.put(meta)
        await asyncio.wait_for(input_queue.join(), timeout=max(5.0, args.timeout))
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
        events = await _drain(progress_queue)
    finally:
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        if browser_pool is not None:
            await browser_pool.close()
        if playwright is not None:
            await playwright.stop()
        settings.DOWNLOAD_PATH = previous_download_path
        if previous_pw_attempts is None:
            os.environ.pop("DOWNLOAD_PLAYWRIGHT_MAX_ATTEMPTS_PER_URL", None)
        else:
            os.environ["DOWNLOAD_PLAYWRIGHT_MAX_ATTEMPTS_PER_URL"] = previous_pw_attempts

    saved = next((event for event in events if event.get("type") in {"file_saved", "download_local_saved"}), None)
    skipped = next((event for event in events if event.get("type") == "download_skipped"), None)
    file_info = (saved or {}).get("file_info") or {}
    saved_path = str(file_info.get("file_path") or "")
    cleanup = "not_created"
    if not args.keep_file:
        await asyncio.to_thread(shutil.rmtree, output_dir, True)
        cleanup = "removed" if saved_path else "removed_empty_dir"
    elif saved_path:
        cleanup = "kept"
    return {
        "job_id": job_id,
        "transport": args.transport,
        "post_url": args.post_url,
        "attachment": {"name": meta["name"], "url": meta["url"]},
        "elapsed_ms": elapsed_ms,
        "saved": bool(saved),
        "saved_size": file_info.get("size"),
        "skip_reason": (skipped or {}).get("detail") or (skipped or {}).get("reason"),
        "cleanup": cleanup,
        "safety": {"db": False, "learning": False, "websync": False},
    }


def main() -> int:
    args = _parse_args()
    try:
        report = asyncio.run(_run(args))
    except Exception as exc:
        report = {"transport": args.transport, "error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["saved"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
