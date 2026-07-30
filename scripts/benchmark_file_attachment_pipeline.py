from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import aiohttp

from backend.file.file_download_workflow import FileDownloadWorkflow
from backend.shared.llm_link_filter import filter_attachment_candidates_with_llm
from core.crawler.batch_queue import BatchQueue
from utils.download_doc_filter import should_skip_attachment_at_scan


DEFAULT_TARGETS: List[Tuple[str, str]] = [
    (
        "guro",
        "https://www.guro.go.kr/www/selectBbsNttView.do?bbsNo=663&nttNo=29167&pageUnit=10&key=1791&pageIndex=1&",
    ),
    (
        "gwangjin",
        "https://www.gwangjin.go.kr/portal/bbs/B0000001/list.do?menuNo=200190",
    ),
    (
        "dongjak",
        "https://dongjak.go.kr/yeyak/progrm/master/online/view.do?tmplatSeCd=14&menuNo=1600014&pageIndex=1&prgSn=9676&useAt=Y",
    ),
    (
        "songpa",
        "https://www.songpa.go.kr/www/selectBbsNttList.do?bbsNo=92&key=2779",
    ),
    (
        "gangnam",
        "https://www.gangnam.go.kr/board/B_000001/list.do?mid=ID05_040101",
    ),
]


DOC_EXTS = (
    ".pdf",
    ".hwp",
    ".hwpx",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".csv",
    ".zip",
)


@dataclass
class Timer:
    start: float

    @classmethod
    def begin(cls) -> "Timer":
        return cls(time.perf_counter())

    def ms(self) -> float:
        return round((time.perf_counter() - self.start) * 1000.0, 2)


def _dedupe_urls(urls: Iterable[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for label, url in urls:
        clean = str(url or "").strip()
        if clean and clean not in seen:
            seen.add(clean)
            out.append((str(label or urlparse(clean).netloc or "target"), clean))
    return out


def _load_targets_from_file(path: str) -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, 1):
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            if "," in text:
                label, url = text.split(",", 1)
                rows.append((label.strip() or f"url{idx}", url.strip()))
            else:
                rows.append((f"url{idx}", text))
    return rows


def _compact_attachment(item: Any) -> Dict[str, Any]:
    if not isinstance(item, dict):
        return {"type": type(item).__name__}
    return {
        "kind": item.get("kind") or item.get("source") or "url",
        "name": item.get("name") or item.get("title") or item.get("text") or "",
        "href": item.get("href") or item.get("url") or "",
        "method": item.get("method") or "GET",
        "params": item.get("params") or {},
        "needs_response_validation": bool(item.get("needs_response_validation")),
        "candidate_score": item.get("candidate_score"),
        "candidate_reason": item.get("candidate_reason"),
    }


def _attachment_url(attach: Dict[str, Any], source_page: str) -> str:
    raw = str(attach.get("href") or attach.get("url") or "").strip()
    if not raw:
        return ""
    if raw.lower().startswith(("javascript:", "mailto:", "tel:")):
        return raw
    return urljoin(source_page, raw)


def _attachment_name(attach: Dict[str, Any]) -> str:
    return str(attach.get("name") or attach.get("title") or attach.get("text") or "attachment").strip()


def _build_queue_meta(
    *,
    label: str,
    source_page: str,
    attach: Dict[str, Any],
    file_url: str,
    job_id: str,
) -> Dict[str, Any]:
    return {
        "job_id": job_id,
        "url": file_url,
        "name": _attachment_name(attach),
        "source_page": source_page,
        "source_url": source_page,
        "label": label,
        "type": "file",
        "file_crawl": True,
        "request_method": str(attach.get("method") or "GET").strip().upper(),
        "request_params": attach.get("params") or {},
        "needs_response_validation": bool(attach.get("needs_response_validation")),
        "original_meta": dict(attach),
    }


async def _fetch_html(workflow: FileDownloadWorkflow, url: str, *, timeout_sec: float) -> Tuple[str, str]:
    html = await workflow._fetch_html_static(url, timeout_sec=timeout_sec)
    return html or "", "static"


async def _probe_download(
    session: aiohttp.ClientSession,
    file_meta: Dict[str, Any],
    *,
    timeout_sec: float,
    max_bytes: int,
) -> Dict[str, Any]:
    url = str(file_meta.get("url") or "").strip()
    source_page = str(file_meta.get("source_page") or "").strip()
    method = str(file_meta.get("request_method") or "GET").strip().upper()
    params = file_meta.get("request_params") or {}
    needs_validation = bool(file_meta.get("needs_response_validation"))
    name = str(file_meta.get("name") or "").strip()
    timer = Timer.begin()
    if not url:
        return {"ok": False, "reason": "empty_url", "elapsed_ms": timer.ms()}
    if should_skip_attachment_at_scan(url, name) and not needs_validation:
        return {"ok": False, "reason": "non_doc_precheck", "elapsed_ms": timer.ms()}
    if url.lower().startswith("javascript:"):
        return {"ok": False, "reason": "javascript_url", "elapsed_ms": timer.ms()}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
        "Accept": "*/*",
    }
    if source_page:
        headers["Referer"] = source_page
    if max_bytes > 0:
        headers["Range"] = f"bytes=0-{max(0, max_bytes - 1)}"

    safe_url = quote(url, safe=":/?=&%")
    try:
        if method == "POST":
            req = session.post(safe_url, data=params, headers=headers, timeout=timeout_sec, allow_redirects=True)
        else:
            req = session.get(safe_url, params=params if method == "GET" else None, headers=headers, timeout=timeout_sec, allow_redirects=True)
        async with req as resp:
            content_type = (resp.headers.get("content-type") or "").lower()
            content_disposition = resp.headers.get("content-disposition") or ""
            size = 0
            async for chunk in resp.content.iter_chunked(8192):
                size += len(chunk)
                if max_bytes > 0 and size >= max_bytes:
                    break
            is_html = "text/html" in content_type or "application/xhtml" in content_type
            looks_doc = any(ext in url.lower() or ext in name.lower() for ext in DOC_EXTS)
            looks_doc = looks_doc or bool(content_disposition)
            looks_doc = looks_doc or any(token in content_type for token in ("pdf", "hwp", "excel", "word", "powerpoint", "octet-stream", "zip"))
            ok = 200 <= resp.status < 400 and looks_doc and not is_html
            return {
                "ok": ok,
                "status": resp.status,
                "content_type": content_type,
                "content_disposition": bool(content_disposition),
                "bytes_read": size,
                "reason": "" if ok else ("html_response" if is_html else "not_document_like"),
                "elapsed_ms": timer.ms(),
            }
    except Exception as exc:
        return {
            "ok": False,
            "reason": type(exc).__name__,
            "error": str(exc)[:300],
            "elapsed_ms": timer.ms(),
        }


async def _queue_and_probe_downloads(
    metas: List[Dict[str, Any]],
    *,
    batch_size: int,
    concurrency: int,
    timeout_sec: float,
    max_bytes: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    queue: BatchQueue[Dict[str, Any]] = BatchQueue(batch_size=max(1, batch_size))
    put_timer = Timer.begin()
    for meta in metas:
        await queue.put(meta)
    await queue.flush()
    queue_put_ms = put_timer.ms()

    results: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(max(1, concurrency))
    timeout = aiohttp.ClientTimeout(total=max(1.0, timeout_sec + 2.0))
    headers = {"User-Agent": "Mozilla/5.0"}
    consume_timer = Timer.begin()

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        while not queue.empty():
            batch = await queue.get()

            async def one(item: Dict[str, Any]) -> Dict[str, Any]:
                async with sem:
                    probe = await _probe_download(session, item, timeout_sec=timeout_sec, max_bytes=max_bytes)
                    return {
                        "label": item.get("label"),
                        "source_page": item.get("source_page"),
                        "url": item.get("url"),
                        "name": item.get("name"),
                        "method": item.get("request_method"),
                        "needs_response_validation": item.get("needs_response_validation"),
                        "probe": probe,
                    }

            results.extend(await asyncio.gather(*(one(item) for item in batch)))
            queue.task_done()

    return results, {"queue_put_ms": queue_put_ms, "queue_consume_probe_ms": consume_timer.ms()}


async def benchmark_one(
    label: str,
    url: str,
    *,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    workflow = FileDownloadWorkflow()
    total_timer = Timer.begin()
    try:
        fetch_timer = Timer.begin()
        html, fetch_method = await _fetch_html(workflow, url, timeout_sec=args.fetch_timeout)
        fetch_ms = fetch_timer.ms()
        if not html:
            return {
                "label": label,
                "url": url,
                "ok": False,
                "error": "html_fetch_failed",
                "fetch_ms": fetch_ms,
                "total_ms": total_timer.ms(),
            }

        extract_timer = Timer.begin()
        raw_attachments = workflow._extract_attachment_links_generic(html, base_url=url)
        extract_ms = extract_timer.ms()

        candidates = []
        scan_skipped = 0
        for attach in raw_attachments:
            if not isinstance(attach, dict):
                continue
            file_url = _attachment_url(attach, url)
            file_name = _attachment_name(attach)
            if not file_url:
                scan_skipped += 1
                continue
            if should_skip_attachment_at_scan(file_url, file_name) and not bool(attach.get("needs_response_validation")):
                scan_skipped += 1
                continue
            candidates.append((attach, file_url, file_name, file_url, []))

        llm_ms = 0.0
        llm_before = len(candidates)
        if args.llm_filter:
            llm_timer = Timer.begin()
            candidates = await filter_attachment_candidates_with_llm(candidates, post_url=url)
            llm_ms = llm_timer.ms()

        metas = [
            _build_queue_meta(
                label=label,
                source_page=url,
                attach=attach,
                file_url=file_url,
                job_id=args.job_id,
            )
            for attach, file_url, _file_name, _file_url_key, _dedup_keys in candidates
        ]

        probe_results: List[Dict[str, Any]] = []
        queue_metrics = {"queue_put_ms": 0.0, "queue_consume_probe_ms": 0.0}
        if args.download_mode != "none" and metas:
            if args.max_downloads_per_site > 0:
                metas = metas[: args.max_downloads_per_site]
            probe_results, queue_metrics = await _queue_and_probe_downloads(
                metas,
                batch_size=args.batch_size,
                concurrency=args.download_concurrency,
                timeout_sec=args.download_timeout,
                max_bytes=args.max_bytes,
            )

        ok_downloads = sum(1 for row in probe_results if (row.get("probe") or {}).get("ok"))
        return {
            "label": label,
            "url": url,
            "ok": True,
            "fetch_method": fetch_method,
            "html_len": len(html),
            "raw_attachment_count": len(raw_attachments or []),
            "scan_candidate_count": llm_before,
            "scan_skipped_count": scan_skipped,
            "queued_count": len(metas),
            "download_probe_count": len(probe_results),
            "download_probe_ok_count": ok_downloads,
            "timing_ms": {
                "fetch": fetch_ms,
                "extract": extract_ms,
                "llm_filter": llm_ms,
                **queue_metrics,
                "total": total_timer.ms(),
            },
            "attachments_sample": [_compact_attachment(a) for a in (raw_attachments or [])[: args.sample_size]],
            "download_probe_sample": probe_results[: args.sample_size],
        }
    except Exception as exc:
        return {
            "label": label,
            "url": url,
            "ok": False,
            "error": f"{type(exc).__name__}: {str(exc)[:500]}",
            "total_ms": total_timer.ms(),
        }
    finally:
        try:
            await workflow._close_http_session()
        except Exception:
            pass
        try:
            await workflow._close_playwright()
        except Exception:
            pass


def _print_summary(results: List[Dict[str, Any]]) -> None:
    print("\n=== File Attachment Pipeline Benchmark ===")
    total_ms = sum(float((row.get("timing_ms") or {}).get("total") or row.get("total_ms") or 0) for row in results)
    print(f"sites={len(results)} total_ms={round(total_ms, 2)}")
    print("")
    for row in results:
        timing = row.get("timing_ms") or {}
        print(
            "{label:10s} ok={ok!s:5s} raw={raw:3d} cand={cand:3d} queued={queued:3d} "
            "dl_ok={dl_ok:3d}/{dl_cnt:3d} fetch={fetch:8.2f} extract={extract:8.2f} "
            "llm={llm:8.2f} queue+probe={probe:8.2f} total={total:8.2f} url={url}".format(
                label=str(row.get("label") or "")[:10],
                ok=bool(row.get("ok")),
                raw=int(row.get("raw_attachment_count") or 0),
                cand=int(row.get("scan_candidate_count") or 0),
                queued=int(row.get("queued_count") or 0),
                dl_ok=int(row.get("download_probe_ok_count") or 0),
                dl_cnt=int(row.get("download_probe_count") or 0),
                fetch=float(timing.get("fetch") or 0),
                extract=float(timing.get("extract") or 0),
                llm=float(timing.get("llm_filter") or 0),
                probe=float(timing.get("queue_consume_probe_ms") or 0),
                total=float(timing.get("total") or row.get("total_ms") or 0),
                url=row.get("url") or "",
            )
        )
        if not row.get("ok"):
            print(f"  error={row.get('error')}")


async def run(args: argparse.Namespace) -> int:
    targets: List[Tuple[str, str]] = []
    if args.file:
        targets.extend(_load_targets_from_file(args.file))
    for idx, url in enumerate(args.url or [], 1):
        targets.append((f"url{idx}", url))
    if not targets:
        targets = list(DEFAULT_TARGETS)
    targets = _dedupe_urls(targets)[: max(1, args.limit)]

    sem = asyncio.Semaphore(max(1, args.site_concurrency))

    async def one(item: Tuple[str, str]) -> Dict[str, Any]:
        async with sem:
            return await benchmark_one(item[0], item[1], args=args)

    results = await asyncio.gather(*(one(item) for item in targets))
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        _print_summary(results)
        if args.output:
            print(f"\njson={args.output}")
    return 0


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark detail HTML fetch, attachment extraction, queue handoff, and download probe for file crawling."
    )
    parser.add_argument("url", nargs="*", help="Detail/list page URL(s). If omitted, uses five built-in gu-office samples.")
    parser.add_argument("--file", help="Text file with 'label,url' or URL per line.")
    parser.add_argument("--limit", type=int, default=5, help="Maximum site count.")
    parser.add_argument("--site-concurrency", type=int, default=2, help="Concurrent sites.")
    parser.add_argument("--fetch-timeout", type=float, default=20.0, help="HTML fetch timeout seconds.")
    parser.add_argument("--download-timeout", type=float, default=15.0, help="Download probe timeout seconds.")
    parser.add_argument("--download-concurrency", type=int, default=4, help="Concurrent download probes per site.")
    parser.add_argument("--batch-size", type=int, default=3, help="Collection queue batch size.")
    parser.add_argument("--download-mode", choices=("none", "probe"), default="probe", help="none skips queue consumer download probe.")
    parser.add_argument("--max-downloads-per-site", type=int, default=5, help="Limit download probes per site; 0 means all.")
    parser.add_argument("--max-bytes", type=int, default=65536, help="Bytes to read per attachment in probe mode.")
    parser.add_argument("--llm-filter", action="store_true", help="Include the current LLM attachment filter in timings.")
    parser.add_argument("--job-id", default="attachment-benchmark", help="Synthetic job id used in queue metadata.")
    parser.add_argument("--sample-size", type=int, default=3, help="Attachment/result sample size in JSON.")
    parser.add_argument("--json", action="store_true", help="Print full JSON.")
    parser.add_argument("--output", default=os.path.join("tmp", "file_attachment_pipeline_benchmark.json"), help="Write JSON result path.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    if sys.platform == "win32" and sys.version_info < (3, 14):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        except Exception:
            pass
    raise SystemExit(asyncio.run(run(parse_args())))
