from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aiohttp import web


LOGGER = logging.getLogger("scripts.verify_file_crawl_runtime")
POST_URL_KEYS = {"post_url", "post_urls", "detail_url", "detail_urls", "url_list", "start_urls", "target_urls"}
ATTACHMENT_KEYS = {"attachments", "files", "file_urls"}


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _unique(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def _read_json(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("payload root must be a JSON object")
    return value


def _attachment_url(item: dict[str, Any], source_page: str) -> str:
    raw = str(
        item.get("href")
        or item.get("url")
        or item.get("download_url")
        or item.get("file_url")
        or ""
    ).strip()
    if not raw:
        return ""
    return urljoin(source_page, raw)


def _attachment_name(item: dict[str, Any]) -> str:
    return str(
        item.get("name")
        or item.get("attachment_name")
        or item.get("filename")
        or item.get("file_name")
        or item.get("title")
        or "attachment"
    ).strip()


def _collect_payload_inputs(payload: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]], str]:
    post_urls: list[str] = []
    attachments: list[dict[str, Any]] = []
    job_id = str(payload.get("job_id") or payload.get("task_id") or "").strip()

    def walk(value: Any, parent_post_url: str = "") -> None:
        if isinstance(value, dict):
            local_post = str(value.get("post_url") or value.get("detail_url") or parent_post_url or "").strip()
            for key, child in value.items():
                normalized = str(key).strip().lower()
                if normalized in POST_URL_KEYS:
                    if isinstance(child, str):
                        post_urls.append(child)
                    elif isinstance(child, list):
                        post_urls.extend(str(row) for row in child if isinstance(row, str))
                if normalized in ATTACHMENT_KEYS:
                    rows = child if isinstance(child, list) else [child]
                    for row in rows:
                        if isinstance(row, str):
                            attachments.append({"url": row, "source_page": local_post})
                        elif isinstance(row, dict):
                            copied = dict(row)
                            copied.setdefault("source_page", local_post)
                            attachments.append(copied)
                walk(child, local_post)
        elif isinstance(value, list):
            for child in value:
                walk(child, parent_post_url)

    walk(payload)
    return _unique(post_urls), attachments, job_id


def _merge_attachments(rows: Iterable[dict[str, Any]], source_page: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        url = _attachment_url(row, source_page)
        if not url or url in seen:
            continue
        seen.add(url)
        copied = dict(row)
        copied["url"] = url
        copied.setdefault("source_page", source_page)
        result.append(copied)
    return result


def _queue_meta(item: dict[str, Any], *, source_page: str, job_id: str) -> dict[str, Any]:
    url = _attachment_url(item, source_page)
    name = _attachment_name(item)
    return {
        "job_id": job_id,
        "url": url,
        "name": name,
        "subject": name,
        "attachment_name": name,
        "source_page": source_page,
        "source_url": source_page,
        "request_method": str(item.get("method") or item.get("request_method") or "GET").upper(),
        "request_params": item.get("params") or item.get("request_params") or {},
        "needs_response_validation": bool(item.get("needs_response_validation")),
        "sync_after_download": False,
        "skip_study_worker": True,
        "defer_save_batch_until_learn_list": False,
        "harness_mode": True,
        "original_meta": dict(item),
    }


async def _close_workflow(workflow: Any) -> None:
    for method_name in ("_close_http_session", "_close_playwright"):
        try:
            method = getattr(workflow, method_name, None)
            if method:
                await method()
        except Exception:
            LOGGER.debug("workflow cleanup failed: %s", method_name, exc_info=True)


async def _extract_post(
    workflow: Any,
    post_url: str,
    *,
    timeout_sec: float,
    html_override: str = "",
) -> dict[str, Any]:
    from backend.file.fast_attachment_extractor import extract_fast_attachments

    started = _now_ms()
    try:
        html = html_override or await workflow._fetch_html_static(post_url, timeout_sec=timeout_sec) or ""
        fetch_ms = round(_now_ms() - started, 2)
        if not html:
            return {"post_url": post_url, "ok": False, "reason": "empty_html", "fetch_ms": fetch_ms, "attachments": []}

        extract_started = _now_ms()
        fast_rows = extract_fast_attachments(html, post_url)
        generic_rows = workflow._extract_attachment_links_generic(html, base_url=post_url)
        attachments = _merge_attachments([*(fast_rows or []), *(generic_rows or [])], post_url)
        return {
            "post_url": post_url,
            "ok": True,
            "html_bytes": len(html.encode("utf-8", errors="ignore")),
            "fetch_ms": fetch_ms,
            "extract_ms": round(_now_ms() - extract_started, 2),
            "fast_count": len(fast_rows or []),
            "generic_count": len(generic_rows or []),
            "attachment_count": len(attachments),
            "attachments": attachments,
        }
    except Exception as exc:
        return {
            "post_url": post_url,
            "ok": False,
            "reason": type(exc).__name__,
            "error": str(exc)[:500],
            "fetch_ms": round(_now_ms() - started, 2),
            "attachments": [],
        }


async def _drain_progress(queue: asyncio.Queue) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    while True:
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            if isinstance(item, dict):
                rows.append(item)
        finally:
            queue.task_done()
    return rows


async def _drain_batch_queue(queue: Any) -> list[dict[str, Any]]:
    await queue.flush()
    rows: list[dict[str, Any]] = []
    while True:
        try:
            batch = queue.queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            rows.extend(item for item in (batch or []) if isinstance(item, dict))
        finally:
            queue.task_done()
    return rows


async def _run_download_stage(
    metas: list[dict[str, Any]],
    *,
    args: argparse.Namespace,
    job_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    from config.settings import settings
    from core.crawler.queues import create_job_queues, dispose_job_queues
    from core.crawler.workers.download import download_worker

    os.environ["CRAWLER_COLLECTION_QUEUE_MAXSIZE"] = str(args.queue_maxsize)
    os.environ["DOWNLOAD_FAILED_RETRY_RAM_QUEUE"] = "0"
    settings.DOWNLOAD_PATH = output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    queues = create_job_queues(job_id, collection_batch_size=args.batch_size)
    collection = queues.collection_batch_queue
    workers = [
        asyncio.create_task(
            download_worker(
                collection,
                queues.save_batch_queue,
                queues.progress_queue,
                max_concurrent=args.download_concurrency,
                browser=None,
                worker_id=index + 1,
            ),
            name=f"harness-download-{index + 1}",
        )
        for index in range(args.download_workers)
    ]

    metrics = {
        "queue_maxsize": collection.queue.maxsize,
        "batch_size": collection.batch_size,
        "max_queued_batches": 0,
        "backpressure_wait_count": 0,
        "backpressure_wait_ms": 0.0,
        "timed_out": False,
        "run_timeout_sec": args.run_timeout,
    }
    started = _now_ms()

    async def monitor() -> None:
        while True:
            metrics["max_queued_batches"] = max(metrics["max_queued_batches"], collection.queue.qsize())
            await asyncio.sleep(0.005)

    monitor_task = asyncio.create_task(monitor(), name="harness-queue-monitor")
    try:
        for meta in metas:
            put_started = _now_ms()
            await collection.put(meta)
            elapsed = _now_ms() - put_started
            if elapsed >= args.backpressure_threshold_ms:
                metrics["backpressure_wait_count"] += 1
                metrics["backpressure_wait_ms"] += elapsed
        await collection.flush()
        await asyncio.wait_for(collection.join(), timeout=args.run_timeout)
    except asyncio.TimeoutError:
        metrics["timed_out"] = True
    finally:
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    progress = await _drain_progress(queues.progress_queue)
    saved_rows = await _drain_batch_queue(queues.save_batch_queue)
    await dispose_job_queues(job_id)

    saved_events = [row for row in progress if row.get("type") == "file_saved"]
    skipped_events = [row for row in progress if row.get("type") == "download_skipped"]
    metrics["backpressure_wait_ms"] = round(float(metrics["backpressure_wait_ms"]), 2)
    metrics["elapsed_ms"] = round(_now_ms() - started, 2)
    return {
        "metrics": metrics,
        "saved_count": len(saved_events),
        "save_queue_count": len(saved_rows),
        "skipped_count": len(skipped_events),
        "saved": [
            {
                "url": row.get("url"),
                "file_path": (row.get("file_info") or {}).get("file_path"),
                "name": (row.get("file_info") or {}).get("name"),
                "size": (row.get("file_info") or {}).get("size"),
            }
            for row in saved_events
        ],
        "skipped": [
            {
                "url": row.get("url"),
                "post_url": row.get("source_page") or row.get("source_url"),
                "reason": row.get("reason"),
                "detail": row.get("detail"),
            }
            for row in skipped_events
        ],
    }


async def _start_fixture_server(args: argparse.Namespace) -> tuple[web.AppRunner, str]:
    pdf = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<<>>\n%%EOF\n"

    async def detail(_request: web.Request) -> web.Response:
        links = "\n".join(
            f'<a class="file" href="/files/{index}.pdf" download>fixture-{index}.pdf</a>'
            for index in range(args.fixture_count)
        )
        return web.Response(text=f"<html><body><div class='file-list'>{links}</div></body></html>", content_type="text/html")

    async def attachment(request: web.Request) -> web.Response:
        index = int(request.match_info["index"])
        delay = (
            args.fixture_timeout_delay
            if index == args.fixture_timeout_index
            else args.fixture_delay
        )
        await asyncio.sleep(delay)
        if index == args.fixture_fail_index:
            return web.Response(status=500, text="injected failure")
        return web.Response(
            body=pdf,
            content_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="fixture-{index}.pdf"'},
        )

    app = web.Application()
    app.router.add_get("/detail", detail)
    app.router.add_get("/files/{index}.pdf", attachment)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = list(getattr(site, "_server").sockets)
    port = int(sockets[0].getsockname()[1])
    return runner, f"http://127.0.0.1:{port}/detail"


def _test_case(*, name: str, stage: str, expected: str, actual: str, passed: bool) -> dict[str, Any]:
    return {
        "name": name,
        "stage": stage,
        "expected": expected,
        "actual": actual,
        "passed": bool(passed),
    }


def _build_observations(
    *,
    detail_rows: list[dict[str, Any]],
    filtered: list[dict[str, Any]],
    download_result: dict[str, Any] | None,
    self_test: bool,
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for row in detail_rows:
        if not row.get("ok"):
            observations.append(
                {
                    "level": "error",
                    "stage": "detail_fetch",
                    "expected": False,
                    "post_url": row.get("post_url"),
                    "reason": row.get("reason"),
                    "detail": row.get("error") or "empty_html",
                    "message": "상세페이지 응답 또는 HTML 추출에 실패했습니다.",
                }
            )
        elif not int(row.get("attachment_count") or 0):
            observations.append(
                {
                    "level": "info",
                    "stage": "attachment_extract",
                    "expected": True,
                    "post_url": row.get("post_url"),
                    "reason": "no_attachment",
                    "detail": "첨부파일 후보가 없습니다.",
                    "message": "첨부파일이 없는 게시물입니다.",
                }
            )

    if filtered:
        observations.append(
            {
                "level": "info",
                "stage": "candidate_filter",
                "expected": True,
                "reason": "filtered_non_document",
                "count": len(filtered),
                "detail": filtered[:5],
                "message": "문서가 아닌 첨부 후보를 다운로드 전에 제외했습니다.",
            }
        )

    for row in (download_result or {}).get("skipped") or []:
        detail = str(row.get("detail") or "")
        is_http_500 = "http_status_500" in detail
        is_timeout = "timeout" in detail.lower()
        controlled = self_test and (is_http_500 or is_timeout)
        observations.append(
            {
                "level": "expected_failure" if controlled else "warning",
                "stage": "download",
                "expected": controlled,
                "post_url": row.get("post_url"),
                "file_url": row.get("url"),
                "reason": row.get("reason") or "download_skipped",
                "detail": detail,
                "message": (
                    "하네스가 의도적으로 주입한 HTTP 500을 정상적으로 skip 처리했습니다."
                    if is_http_500 and controlled
                    else (
                        "하네스가 의도적으로 지연시킨 응답이 HTTP timeout으로 종료됐고, "
                        "이후 큐 항목 처리가 계속됐습니다."
                        if is_timeout and controlled
                        else "첨부파일 다운로드가 완료되지 않았습니다."
                    )
                ),
            }
        )

    metrics = (download_result or {}).get("metrics") or {}
    wait_count = int(metrics.get("backpressure_wait_count") or 0)
    if wait_count:
        observations.append(
            {
                "level": "info",
                "stage": "collection_queue",
                "expected": self_test,
                "reason": "backpressure",
                "count": wait_count,
                "detail": {
                    "queue_maxsize": metrics.get("queue_maxsize"),
                    "max_queued_batches": metrics.get("max_queued_batches"),
                    "wait_ms": metrics.get("backpressure_wait_ms"),
                },
                "message": "다운로드 큐 포화로 탐색 단계가 대기했다가 워커 소비 후 재개됐습니다.",
            }
        )
    if metrics.get("timed_out"):
        observations.append(
            {
                "level": "error",
                "stage": "collection_queue",
                "expected": False,
                "reason": "run_timeout",
                "detail": {"run_timeout_sec": metrics.get("run_timeout_sec")},
                "message": "다운로드 큐가 제한 시간 안에 비워지지 않았습니다.",
            }
        )
    return observations


def _print_human_summary(report: dict[str, Any]) -> None:
    result = report.get("result") or "unknown"
    print()
    print(f"=== Harness Result: {result.upper()} ===")
    print("[Tested]")
    for row in report.get("tests") or []:
        state = "PASS" if row.get("passed") else "FAIL"
        print(f"- {state} | {row.get('stage')} | {row.get('name')}")
        print(f"  expected={row.get('expected')} actual={row.get('actual')}")

    observations = report.get("observations") or []
    print("[Observations]")
    if not observations:
        print("- none")
    for row in observations:
        level = str(row.get("level") or "info").upper()
        expected = "expected" if row.get("expected") else "unexpected"
        print(
            f"- {level} ({expected}) | {row.get('stage')} | "
            f"reason={row.get('reason')} | {row.get('message')}"
        )


async def run(args: argparse.Namespace) -> int:
    from backend.file.file_download_workflow import FileDownloadWorkflow
    from utils.download_doc_filter import should_skip_attachment_at_scan

    payload: dict[str, Any] = _read_json(args.payload) if args.payload else {}
    payload_posts, payload_attachments, payload_job_id = _collect_payload_inputs(payload)
    job_id = args.job_id or payload_job_id or f"harness-{uuid.uuid4().hex[:12]}"
    output_dir = Path(args.download_dir or (ROOT / "tmp" / "file_crawl_harness" / job_id)).resolve()
    report_path = Path(args.output).resolve()

    fixture_runner: web.AppRunner | None = None
    post_urls = _unique([*(args.post_url or []), *payload_posts])
    direct_attachments = list(payload_attachments)
    if args.file_url:
        source_page = post_urls[0] if post_urls else str(args.base_url or "")
        direct_attachments.extend(
            {"url": url, "name": args.file_name or Path(url).name or "attachment", "source_page": source_page}
            for url in args.file_url
        )

    html_override = ""
    if args.html_file:
        html_override = Path(args.html_file).read_text(encoding="utf-8", errors="replace")
        base = str(args.base_url or (post_urls[0] if post_urls else "https://fixture.invalid/detail"))
        post_urls = [base]

    if args.self_test:
        os.environ["DOWNLOAD_HTTP_RETRIES"] = "1"
        os.environ["DOWNLOAD_HTTP_TIMEOUT_SEC"] = "5"
        fixture_runner, fixture_url = await _start_fixture_server(args)
        post_urls = [fixture_url]
        args.download = True

    if not post_urls and not direct_attachments:
        raise ValueError("provide --post-url, --file-url, --payload, --html-file, or --self-test")

    workflow = FileDownloadWorkflow()
    detail_rows: list[dict[str, Any]] = []
    try:
        sem = asyncio.Semaphore(max(1, args.detail_concurrency))

        async def extract_one(url: str) -> dict[str, Any]:
            async with sem:
                override = html_override if len(post_urls) == 1 else ""
                return await _extract_post(workflow, url, timeout_sec=args.fetch_timeout, html_override=override)

        detail_rows = await asyncio.gather(*(extract_one(url) for url in post_urls))
    finally:
        await _close_workflow(workflow)

    merged: list[dict[str, Any]] = []
    for detail in detail_rows:
        for attachment in detail.get("attachments") or []:
            copied = dict(attachment)
            copied.setdefault("source_page", detail.get("post_url") or "")
            merged.append(copied)
    merged.extend(direct_attachments)

    candidates: list[dict[str, Any]] = []
    filtered: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in merged:
        if not isinstance(item, dict):
            continue
        source_page = str(item.get("source_page") or item.get("post_url") or (post_urls[0] if post_urls else ""))
        meta = _queue_meta(item, source_page=source_page, job_id=job_id)
        url = str(meta.get("url") or "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        if should_skip_attachment_at_scan(url, str(meta.get("name") or "")) and not meta.get("needs_response_validation"):
            filtered.append({"url": url, "name": meta.get("name"), "reason": "non_doc_precheck"})
            continue
        candidates.append(meta)

    if args.max_attachments > 0:
        candidates = candidates[: args.max_attachments]

    download_result: dict[str, Any] | None = None
    if args.download and candidates:
        download_result = await _run_download_stage(candidates, args=args, job_id=job_id, output_dir=output_dir)

    report: dict[str, Any] = {
        "job_id": job_id,
        "mode": "self_test" if args.self_test else ("download" if args.download else "extract_only"),
        "safety": {
            "db_write": False,
            "redis_write": False,
            "learning": False,
            "web_storage_sync": False,
            "local_download_dir": str(output_dir),
        },
        "runtime": {
            "detail_concurrency": args.detail_concurrency,
            "download_workers": args.download_workers,
            "download_concurrency_per_worker": args.download_concurrency,
            "collection_batch_size": args.batch_size,
            "collection_queue_maxsize": args.queue_maxsize,
            "fetch_timeout_sec": args.fetch_timeout,
            "run_timeout_sec": args.run_timeout,
        },
        "summary": {
            "post_count": len(post_urls),
            "post_success_count": sum(1 for row in detail_rows if row.get("ok")),
            "attachment_discovered_count": len(merged),
            "candidate_count": len(candidates),
            "filtered_count": len(filtered),
            "download_saved_count": int((download_result or {}).get("saved_count") or 0),
            "download_skipped_count": int((download_result or {}).get("skipped_count") or 0),
        },
        "details": [
            {key: value for key, value in row.items() if key != "attachments"}
            for row in detail_rows
        ],
        "filtered": filtered,
        "download": download_result,
    }

    metrics = (download_result or {}).get("metrics") or {}
    detail_success_count = sum(1 for row in detail_rows if row.get("ok"))
    tests = [
        _test_case(
            name="상세페이지 응답 및 HTML 추출",
            stage="detail_fetch",
            expected=f"{len(post_urls)}개 게시물 응답 성공",
            actual=f"{detail_success_count}/{len(post_urls)}개 성공",
            passed=detail_success_count == len(post_urls),
        ),
        _test_case(
            name="첨부 후보 수집",
            stage="attachment_extract",
            expected="응답 HTML에서 첨부 후보를 추출",
            actual=f"발견={len(merged)}개, 문서후보={len(candidates)}개, 제외={len(filtered)}개",
            passed=detail_success_count == len(post_urls),
        ),
    ]
    if args.download:
        handled_count = int((download_result or {}).get("saved_count") or 0) + int(
            (download_result or {}).get("skipped_count") or 0
        )
        tests.extend(
            [
                _test_case(
                    name="다운로드 워커 처리 완료",
                    stage="download",
                    expected=f"후보 {len(candidates)}개가 저장 또는 skip으로 종료",
                    actual=(
                        f"저장={int((download_result or {}).get('saved_count') or 0)}개, "
                        f"skip={int((download_result or {}).get('skipped_count') or 0)}개"
                    ),
                    passed=handled_count == len(candidates) and not bool(metrics.get("timed_out")),
                ),
                _test_case(
                    name="큐 drain 종료",
                    stage="collection_queue",
                    expected=f"{args.run_timeout:.1f}초 안에 다운로드 큐 종료",
                    actual=(
                        f"최대적재={metrics.get('max_queued_batches', 0)}/"
                        f"{metrics.get('queue_maxsize', args.queue_maxsize)}배치, "
                        f"대기={metrics.get('backpressure_wait_count', 0)}회, "
                        f"timeout={bool(metrics.get('timed_out'))}"
                    ),
                    passed=not bool(metrics.get("timed_out")),
                ),
            ]
        )

    exit_code = 0
    self_checks: dict[str, bool] = {}
    if args.self_test:
        failure_indexes = {
            index
            for index in (args.fixture_fail_index, args.fixture_timeout_index)
            if 0 <= index < args.fixture_count
        }
        expected_saved = args.fixture_count - len(failure_indexes)
        skipped_rows = (download_result or {}).get("skipped") or []

        def _skip_matches(index: int, token: str) -> bool:
            return any(
                f"/files/{index}.pdf" in str(row.get("url") or "")
                and token.lower() in str(row.get("detail") or "").lower()
                for row in skipped_rows
            )

        handled_count = int((download_result or {}).get("saved_count") or 0) + int(
            (download_result or {}).get("skipped_count") or 0
        )
        http_500_observed = (
            args.fixture_fail_index < 0
            or _skip_matches(args.fixture_fail_index, "http_status_500")
        )
        timeout_observed = (
            args.fixture_timeout_index < 0
            or _skip_matches(args.fixture_timeout_index, "timeout")
        )
        self_checks = {
            "all_attachments_discovered": len(candidates) == args.fixture_count,
            "all_candidates_reach_terminal_state": handled_count == args.fixture_count,
            "expected_downloads_saved": int((download_result or {}).get("saved_count") or 0) == expected_saved,
            "injected_http_500_observed": http_500_observed,
            "injected_timeout_released_queue": timeout_observed and not bool(metrics.get("timed_out")),
            "backpressure_observed": (
                args.fixture_count <= args.queue_maxsize
                or int(metrics.get("backpressure_wait_count") or 0) >= 1
            ),
            "queue_not_timed_out": not bool(metrics.get("timed_out")),
        }
        tests.extend(
            [
                _test_case(
                    name="Injected HTTP 500 isolation",
                    stage="download",
                    expected=f"fixture-{args.fixture_fail_index}.pdf is skipped",
                    actual=f"http_500_observed={http_500_observed}",
                    passed=http_500_observed,
                ),
                _test_case(
                    name="Timeout item releases queue",
                    stage="download",
                    expected=(
                        f"fixture-{args.fixture_timeout_index}.pdf times out, "
                        "then remaining queue items finish"
                    ),
                    actual=(
                        f"timeout_observed={timeout_observed}, "
                        f"terminal={handled_count}/{args.fixture_count}, "
                        f"queue_timeout={bool(metrics.get('timed_out'))}"
                    ),
                    passed=self_checks["injected_timeout_released_queue"]
                    and self_checks["all_candidates_reach_terminal_state"],
                ),
                _test_case(
                    name="탐색 단계 backpressure",
                    stage="collection_queue",
                    expected=f"큐 {args.queue_maxsize}배치 포화 시 producer 대기",
                    actual=(
                        f"최대적재={metrics.get('max_queued_batches', 0)}, "
                        f"대기={metrics.get('backpressure_wait_count', 0)}회/"
                        f"{metrics.get('backpressure_wait_ms', 0)}ms"
                    ),
                    passed=self_checks["backpressure_observed"],
                ),
            ]
        )
        report["self_test"] = {
            "fixture": {
                "attachment_count": args.fixture_count,
                "download_delay_sec": args.fixture_delay,
                "http_500_file_index": args.fixture_fail_index,
                "timeout_file_index": args.fixture_timeout_index,
                "timeout_delay_sec": args.fixture_timeout_delay,
                "download_http_timeout_sec": 5,
            },
            "expected_saved": expected_saved,
            "checks": self_checks,
            "passed": all(self_checks.values()),
        }
        exit_code = 0 if report["self_test"]["passed"] else 2
    observations = _build_observations(
        detail_rows=detail_rows,
        filtered=filtered,
        download_result=download_result,
        self_test=args.self_test,
    )
    unexpected_observations = [
        row
        for row in observations
        if not row.get("expected") and row.get("level") in {"warning", "error"}
    ]
    if args.self_test:
        result = "passed" if report["self_test"]["passed"] else "failed"
    elif any(not row.get("passed") for row in tests):
        result = "failed"
        exit_code = 1
    elif unexpected_observations:
        result = "needs_review"
    else:
        result = "passed"

    report["tests"] = tests
    report["observations"] = observations
    report["result"] = result
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _print_human_summary(report)
    print(f"report={report_path}")
    print(f"download_dir={output_dir}")

    if fixture_runner is not None:
        await fixture_runner.cleanup()
    return exit_code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay file-crawl detail extraction and production download workers without DB/Redis writes.")
    parser.add_argument("--post-url", action="append", default=[], help="Board detail URL; repeatable.")
    parser.add_argument("--file-url", action="append", default=[], help="Direct attachment URL; repeatable.")
    parser.add_argument("--file-name", default="", help="Display name for --file-url.")
    parser.add_argument("--payload", help="Captured operation payload JSON.")
    parser.add_argument("--html-file", help="Local detail HTML fixture.")
    parser.add_argument("--base-url", default="", help="Base URL for --html-file or --file-url.")
    parser.add_argument("--download", action="store_true", help="Run production download_worker into an isolated local directory.")
    parser.add_argument("--job-id", default="")
    parser.add_argument("--download-dir", default="")
    parser.add_argument("--output", default=str(ROOT / "tmp" / "file_crawl_runtime_report.json"))
    parser.add_argument("--fetch-timeout", type=float, default=30.0)
    parser.add_argument("--run-timeout", type=float, default=180.0)
    parser.add_argument("--detail-concurrency", type=int, default=3)
    parser.add_argument("--download-workers", type=int, default=2)
    parser.add_argument("--download-concurrency", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--queue-maxsize", type=int, default=30)
    parser.add_argument("--max-attachments", type=int, default=50, help="0 means unlimited.")
    parser.add_argument("--backpressure-threshold-ms", type=float, default=10.0)
    parser.add_argument("--self-test", action="store_true", help="Run an isolated local fixture with one injected HTTP 500.")
    parser.add_argument("--fixture-count", type=int, default=35)
    parser.add_argument("--fixture-delay", type=float, default=0.05)
    parser.add_argument("--fixture-fail-index", type=int, default=7)
    parser.add_argument("--fixture-timeout-index", type=int, default=8)
    parser.add_argument("--fixture-timeout-delay", type=float, default=6.0)
    args = parser.parse_args(argv)
    args.detail_concurrency = max(1, min(args.detail_concurrency, 16))
    args.download_workers = max(1, min(args.download_workers, 8))
    args.download_concurrency = max(1, min(args.download_concurrency, 8))
    args.batch_size = max(1, min(args.batch_size, 100))
    args.queue_maxsize = max(1, min(args.queue_maxsize, 5000))
    args.fixture_count = max(1, min(args.fixture_count, 500))
    args.fixture_timeout_delay = max(5.5, min(args.fixture_timeout_delay, 60.0))
    if args.fixture_fail_index == args.fixture_timeout_index and args.fixture_fail_index >= 0:
        parser.error("--fixture-fail-index and --fixture-timeout-index must differ")
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s:%(message)s")
    if sys.platform == "win32" and sys.version_info < (3, 14):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        except Exception:
            pass
    raise SystemExit(asyncio.run(run(parse_args())))
