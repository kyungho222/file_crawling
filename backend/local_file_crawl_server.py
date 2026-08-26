from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse
from urllib.request import Request as UrlRequest, urlopen
from uuid import uuid4
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from backend.file.fast_attachment_extractor import extract_fast_attachments, infer_attachment_extension
from backend.shared.crawl_shared import cache_job_metadata
from backend.shared.crawl_redis_keys import crawl_state_key
from backend.shared.crawl_start import _crawl_file_worker
from backend.shared.crawler_state import crawler_state
from backend.shared.redis_sse_service import get_redis, update_state_only
from db.maria_operations import maria_execute_query
from utils.db_name import resolve_db_name
from utils.file import sanitize_filename, truncate_filename_to_max_bytes
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.local_file_crawl_server")

router = APIRouter()

EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"
DEFAULT_LIMIT = 200
MAX_LIMIT = 5000
DEFAULT_BRIDGE_DB = "f1_dev"
LOCAL_JOBS: Dict[str, Dict[str, Any]] = {}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _limit(value: Any, default: int = DEFAULT_LIMIT) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(1, min(parsed, MAX_LIMIT))


def _like_pattern(raw: str) -> str:
    text = _clean_text(raw)
    if not text:
        return ""
    text = text.replace("*", "%")
    if "%" not in text and "_" not in text:
        text = f"%{text}%"
    return text


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _row_url(row: Dict[str, Any]) -> str:
    return _clean_text(row.get("url") or row.get("URL"))


def _job_id(prefix: str = "local-file-crawl") -> str:
    return f"{prefix}-{int(time.time())}-{uuid4().hex[:8]}"



def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def _snapshot_workflow_stats(job_id: str) -> Dict[str, Any]:
    workflow = getattr(crawler_state, "workflows", {}).get(job_id)
    if workflow is None:
        return {}
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else getattr(workflow, "stats", {})
    except Exception:
        stats = getattr(workflow, "stats", {}) or {}
    return dict(stats or {})


def _status_from_task(job_id: str) -> str:
    task = getattr(crawler_state, "active_worker_tasks", {}).get(job_id)
    workflow_task = getattr(crawler_state, "workflow_tasks", {}).get(job_id)
    workflow = getattr(crawler_state, "workflows", {}).get(job_id)
    if task is not None and not task.done():
        return "running"
    if workflow_task is not None and not workflow_task.done():
        return "running"
    if workflow is not None and getattr(workflow, "is_running", False):
        return "running"
    saved = LOCAL_JOBS.get(job_id) or {}
    return str(saved.get("status") or "unknown")


def _local_job_payload(job_id: str) -> Dict[str, Any]:
    saved = dict(LOCAL_JOBS.get(job_id) or {})
    stats = _snapshot_workflow_stats(job_id) or dict(saved.get("stats") or {})
    status = _status_from_task(job_id)
    if status == "unknown" and saved:
        status = str(saved.get("status") or "accepted")
    return {
        "status": status,
        "job_id": job_id,
        "db_name": saved.get("db_name"),
        "bridge_db_name": saved.get("bridge_db_name"),
        "mode": saved.get("mode"),
        "url": saved.get("url"),
        "started_at": saved.get("started_at"),
        "finished_at": saved.get("finished_at"),
        "stats": stats,
        "counts": {
            "scan": _safe_int(stats.get("scan_count") or stats.get("total_count")),
            "total": _safe_int(stats.get("total_count") or stats.get("scan_count")),
            "collection": _safe_int(stats.get("collection_count")),
            "save": _safe_int(stats.get("save_count")),
            "study": _safe_int(stats.get("study_count") or stats.get("file_study_success_count")),
            "attachments": _safe_int(stats.get("file_attachment_found_total_count") or stats.get("file_attachment_found_count") or stats.get("attachment_count")),
            "failed": _safe_int(stats.get("save_failed_count")) + _safe_int(stats.get("study_failed_count")) + _safe_int(stats.get("file_study_failed_count")),
        },
        "event": stats.get("event"),
        "message": stats.get("message") or saved.get("message"),
        "recent_files": stats.get("recent_files") or stats.get("file_attachment_found_samples") or stats.get("attachments") or [],
    }



DIRECT_SAVE_ROOT = Path(__file__).resolve().parents[1] / "downloads" / "local-file-crawl"


def _job_urls(payload: Dict[str, Any]) -> List[str]:
    values: List[Any] = []
    values.extend(payload.get("start_urls_override") or [])
    values.extend(payload.get("contents") or [])
    values.append(payload.get("target_url") or payload.get("contents_url"))
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        if isinstance(value, dict):
            raw = value.get("url") or value.get("href") or value.get("content")
        else:
            raw = value
        url = _clean_text(raw)
        if not url:
            continue
        try:
            url = ensure_url_scheme(url)
        except Exception:
            pass
        if url and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _request_bytes(url: str, timeout: float = 30.0) -> tuple[bytes, Dict[str, str], str]:
    req = UrlRequest(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf,application/octet-stream,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    with urlopen(req, timeout=timeout) as resp:  # nosec - local operator supplied crawl target
        data = resp.read()
        headers = {str(k): str(v) for k, v in resp.headers.items()}
        final_url = str(getattr(resp, "url", url) or url)
    return data, headers, final_url


def _decode_html(data: bytes, headers: Dict[str, str]) -> str:
    content_type = headers.get("Content-Type") or headers.get("content-type") or ""
    match = re.search(r"charset=([^;\s]+)", content_type, flags=re.IGNORECASE)
    encodings = [match.group(1)] if match else []
    encodings.extend(["utf-8", "cp949", "euc-kr"])
    for encoding in encodings:
        try:
            return data.decode(encoding, errors="replace")
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def _content_disposition_filename(headers: Dict[str, str]) -> str:
    cd = headers.get("Content-Disposition") or headers.get("content-disposition") or ""
    match = re.search(r"filename\*=\s*([^']*)''([^;]+)", cd, flags=re.IGNORECASE)
    if match:
        return unquote(match.group(2)).strip().strip('"')
    match = re.search(r"filename\s*=\s*\"?([^\";]+)\"?", cd, flags=re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        try:
            return raw.encode("latin1", errors="ignore").decode("utf-8", errors="ignore") or raw
        except Exception:
            return raw
    return ""


def _extension_from_content(data: bytes, headers: Dict[str, str], name: str, url: str) -> str:
    ext = infer_attachment_extension(name, url)
    if ext:
        return ext
    head = data[:8]
    if head.startswith(b"%PDF"):
        return ".pdf"
    if head.startswith(b"\xd0\xcf\x11\xe0"):
        return ".hwp"
    if head.startswith(b"PK\x03\x04"):
        return ".zip"
    content_type = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
    if "pdf" in content_type:
        return ".pdf"
    return ".bin"


def _unique_path(directory: Path, filename: str) -> Path:
    path = directory / filename
    if not path.exists():
        return path
    stem = path.stem or "file"
    suffix = path.suffix
    for idx in range(2, 10000):
        candidate = directory / f"{stem}_{idx}{suffix}"
        if not candidate.exists():
            return candidate
    return directory / f"{stem}_{uuid4().hex[:8]}{suffix}"


def _download_attachment_sync(attachment: Dict[str, Any], directory: Path, index: int) -> Dict[str, Any]:
    url = _clean_text(attachment.get("url") or attachment.get("href"))
    data, headers, final_url = _request_bytes(url, timeout=60.0)
    header_name = _content_disposition_filename(headers)
    raw_name = _clean_text(attachment.get("name") or header_name or f"file_{index:03d}")
    ext = _extension_from_content(data, headers, raw_name, final_url)
    filename = sanitize_filename(raw_name) or f"file_{index:03d}"
    if not infer_attachment_extension(filename, filename) and ext:
        filename = f"{filename}{ext}"
    filename = truncate_filename_to_max_bytes(filename, 180)
    path = _unique_path(directory, filename)
    path.write_bytes(data)
    return {
        "name": filename,
        "url": final_url,
        "path": str(path),
        "size": len(data),
        "content_type": headers.get("Content-Type") or headers.get("content-type") or "",
    }


async def _run_direct_save_worker(payload: Dict[str, Any]) -> None:
    job_id = _clean_text(payload.get("job_id"))
    urls = _job_urls(payload)
    out_dir = DIRECT_SAVE_ROOT / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    saved_files: List[Dict[str, Any]] = []
    stats: Dict[str, Any] = {
        "scan_count": len(urls),
        "total_count": len(urls),
        "attachment_count": 0,
        "file_attachment_found_count": 0,
        "save_count": 0,
        "save_failed_count": 0,
        "recent_files": saved_files,
        "output_dir": str(out_dir),
    }
    LOCAL_JOBS.setdefault(job_id, {}).update({"status": "running", "message": "fetching pages", "stats": stats})
    all_attachments: List[Dict[str, Any]] = []
    for page_index, page_url in enumerate(urls, start=1):
        LOCAL_JOBS[job_id].update({"message": f"fetching page {page_index}/{len(urls)}: {page_url}", "stats": stats})
        data, headers, _ = await asyncio.to_thread(_request_bytes, page_url, 30.0)
        html = _decode_html(data, headers)
        attachments = extract_fast_attachments(html, page_url, force_full_scan=True)
        for attachment in attachments:
            if isinstance(attachment, dict):
                attachment["source_page"] = page_url
                all_attachments.append(attachment)
        stats["attachment_count"] = len(all_attachments)
        stats["file_attachment_found_count"] = len(all_attachments)
        stats["file_attachment_found_samples"] = all_attachments[:20]
    if not all_attachments:
        LOCAL_JOBS[job_id].update({"status": "completed", "finished_at": time.time(), "message": "no attachments found", "stats": stats})
        return
    for index, attachment in enumerate(all_attachments, start=1):
        try:
            LOCAL_JOBS[job_id].update({"message": f"downloading {index}/{len(all_attachments)}", "stats": stats})
            saved = await asyncio.to_thread(_download_attachment_sync, attachment, out_dir, index)
            saved_files.insert(0, saved)
            del saved_files[20:]
            stats["save_count"] = _safe_int(stats.get("save_count")) + 1
            stats["recent_files"] = saved_files
        except Exception as exc:
            logger.exception("[LocalFileCrawl] direct download failed | job_id=%s url=%s err=%s", job_id, attachment.get("url") or attachment.get("href"), exc)
            stats["save_failed_count"] = _safe_int(stats.get("save_failed_count")) + 1
            stats["message"] = str(exc)
    status = "completed" if stats.get("save_count") else "error"
    message = f"saved {stats.get('save_count', 0)}/{len(all_attachments)} files to {out_dir}"
    LOCAL_JOBS[job_id].update({"status": status, "finished_at": time.time(), "message": message, "stats": stats, "output_dir": str(out_dir)})

async def _run_local_worker(payload: Dict[str, Any], background_tasks: BackgroundTasks) -> None:
    job_id = _clean_text(payload.get("job_id"))
    try:
        LOCAL_JOBS.setdefault(job_id, {}).update({"status": "running", "message": "running"})
        if _bool_value(payload.get("local_direct_save"), False):
            await _run_direct_save_worker(payload)
            return
        await _crawl_file_worker(payload, background_tasks)
        LOCAL_JOBS.setdefault(job_id, {}).update(
            {
                "status": "completed",
                "finished_at": time.time(),
                "message": "completed",
                "stats": _snapshot_workflow_stats(job_id),
            }
        )
    except Exception as exc:
        logger.exception("[LocalFileCrawl] worker failed | job_id=%s err=%s", job_id, exc)
        LOCAL_JOBS.setdefault(job_id, {}).update(
            {
                "status": "error",
                "finished_at": time.time(),
                "message": str(exc),
                "stats": _snapshot_workflow_stats(job_id),
            }
        )
        raise
def _bridge_db_name(body: Dict[str, Any], logical_db_name: str) -> str:
    return _clean_text(
        body.get("bridge_db_name")
        or body.get("bridge_db")
        or body.get("db_bridge_name")
        or DEFAULT_BRIDGE_DB
        or logical_db_name
    )


def _sql_identifier(name: str) -> str:
    text = _clean_text(name)
    if not text.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(status_code=400, detail=f"invalid identifier: {text}")
    return "`" + text.replace("`", "``") + "`"


def _f1_dev_api_base() -> str:
    return str(
        os.getenv("F1_DEV_API_BASE")
        or "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler"
    ).rstrip("/")


def _post_f1_dev_json(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    request = urllib.request.Request(
        f"{_f1_dev_api_base()}/{path.lstrip('/')}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise HTTPException(status_code=exc.code, detail=f"f1_dev bridge failed: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"f1_dev bridge unavailable: {exc}") from exc
    if not isinstance(data, dict) or data.get("ok") is False:
        raise HTTPException(status_code=502, detail=str((data or {}).get("error") or "f1_dev bridge invalid response"))
    return data


async def _query_pattern_rows(
    *,
    db_name: str,
    bridge_db_name: str,
    chat_bot_id: str,
    pattern: str,
    limit: int,
    post_only: bool = True,
) -> List[Dict[str, Any]]:
    db_name = _clean_text(db_name)
    chat_bot_id = _clean_text(chat_bot_id)
    if not db_name:
        raise HTTPException(status_code=400, detail="db_name is required")
    if not chat_bot_id:
        raise HTTPException(status_code=400, detail="chat_bot_id is required")

    data = await asyncio.to_thread(
        _post_f1_dev_json,
        "/backend/file-dashboard/exploration-posts",
        {
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "exploration_type": "post" if post_only else "all",
            "active_only": True,
            "include_duplicates": False,
            "limit": 5000,
            "offset": 0,
            "method": "all",
        },
    )
    like = _like_pattern(pattern)
    rows = [row for row in (data.get("rows") or []) if isinstance(row, dict) and _row_url(row)]
    if like:
        needle = like.replace("%", "").lower()
        rows = [row for row in rows if needle in _row_url(row).lower()]
    return rows[:limit]

def _rows_to_start_urls(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        url = _row_url(row)
        if not url:
            continue
        try:
            url = ensure_url_scheme(url)
        except Exception:
            pass
        if not url or url in seen:
            continue
        seen.add(url)
        item = {"url": url, "type": _clean_text(row.get("type")) or "post"}
        row_id = row.get("id")
        if row_id is not None:
            item["exploration_id"] = row_id
        out.append(item)
    return out


async def _schedule_payload(payload: Dict[str, Any], background_tasks: BackgroundTasks) -> None:
    job_id = _clean_text(payload.get("job_id"))
    db_name = resolve_db_name(payload, default="dev_user") or "dev_user"
    if not _bool_value(payload.get("local_file_crawl_no_redis"), True):
        await cache_job_metadata(job_id, db_name)
        try:
            redis = await get_redis()
            await redis.delete(crawl_state_key(db_name, job_id))
        except Exception:
            logger.debug("[LocalFileCrawl] redis state reset skipped", exc_info=True)
        await update_state_only(
            job_id=job_id,
            account_name=db_name,
            payload={
                "status": "start",
                "event": "local_file_crawl_start",
                "scan_count": int(payload.get("pre_explored_start_urls_count") or 0),
                "total_count": int(payload.get("pre_explored_start_urls_count") or 0),
                "h3": "crawl status",
                "message": "local file crawl accepted",
            },
        )

    LOCAL_JOBS[job_id] = {
        "status": "accepted",
        "job_id": job_id,
        "db_name": payload.get("logical_db_name") or db_name,
        "bridge_db_name": payload.get("bridge_db_name") or db_name,
        "mode": payload.get("local_file_crawl_mode"),
        "url": (payload.get("target_url") or payload.get("contents_url") or ""),
        "started_at": time.time(),
        "message": "accepted",
        "stats": {"scan_count": int(payload.get("pre_explored_start_urls_count") or 0), "total_count": int(payload.get("pre_explored_start_urls_count") or 0)},
    }
    task = asyncio.create_task(_run_local_worker(payload, background_tasks), name=f"local-worker:{job_id}")
    if not hasattr(crawler_state, "active_worker_tasks"):
        crawler_state.active_worker_tasks = {}
    crawler_state.active_worker_tasks[job_id] = task


def _base_payload(*, db_name: str, chat_bot_id: str, job_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "job_id": _clean_text(job_id) or _job_id(),
        "db_name": db_name,
        "account_name": db_name,
        "chat_bot_id": chat_bot_id,
        "colle": "file",
        "content_type": "file",
        "method": "period",
        "crawl_mode": "crawling",
        "stream_matched_rules_only": False,
        "file_pipeline_skip_learning": False,
        "enable_db_save": True,
    }


@router.get("/local-file-crawl", response_class=HTMLResponse)
async def local_file_crawl_page() -> HTMLResponse:
    return HTMLResponse(_PAGE_HTML)


@router.get("/local-file-crawl/api/status/{job_id}")
async def local_file_crawl_status(job_id: str) -> JSONResponse:
    payload = _local_job_payload(job_id)
    if payload.get("status") == "unknown":
        raise HTTPException(status_code=404, detail="job not found")
    return JSONResponse(payload)


@router.post("/local-file-crawl/api/preview-pattern")
async def preview_pattern(request: Request) -> JSONResponse:
    body = await request.json()
    db_name = resolve_db_name(body, default="dev_user") or "dev_user"
    bridge_db_name = _bridge_db_name(body, db_name)
    rows = await _query_pattern_rows(
        db_name=db_name,
        bridge_db_name=bridge_db_name,
        chat_bot_id=_clean_text(body.get("chat_bot_id")),
        pattern=_clean_text(body.get("pattern")),
        limit=_limit(body.get("limit")),
        post_only=_bool_value(body.get("post_only"), True),
    )
    return JSONResponse({"status": "ok", "db_name": db_name, "bridge_db_name": bridge_db_name, "count": len(rows), "rows": rows[:100]})


@router.post("/local-file-crawl/api/start-pattern")
async def start_pattern(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    body = await request.json()
    db_name = resolve_db_name(body, default="dev_user") or "dev_user"
    bridge_db_name = _bridge_db_name(body, db_name)
    chat_bot_id = _clean_text(body.get("chat_bot_id"))
    rows = await _query_pattern_rows(
        db_name=db_name,
        bridge_db_name=bridge_db_name,
        chat_bot_id=chat_bot_id,
        pattern=_clean_text(body.get("pattern")),
        limit=_limit(body.get("limit")),
        post_only=_bool_value(body.get("post_only"), True),
    )
    start_urls = _rows_to_start_urls(rows)
    if not start_urls:
        raise HTTPException(status_code=404, detail="no matching rows")

    payload = _base_payload(db_name=bridge_db_name, chat_bot_id=chat_bot_id, job_id=body.get("job_id"))
    payload.update(
        {
            "contents": [start_urls[0]["url"]],
            "contents_url": start_urls[0]["url"],
            "target_domains": [],
            "start_urls_override": start_urls,
            "start_urls_override_source": "file_crawl_post_db",
            "pre_explored_start_urls_count": len(start_urls),
            "selected_start_urls_count": len(start_urls),
            "actual_start_urls_count": len(start_urls),
            "file_dashboard": True,
            "local_file_crawl": True,
            "local_file_crawl_no_redis": True,
            "local_direct_save": True,
            "enable_db_save": False,
            "file_pipeline_skip_learning": True,
            "logical_db_name": db_name,
            "bridge_db_name": bridge_db_name,
            "local_file_crawl_mode": "pattern",
            "local_file_crawl_pattern": _clean_text(body.get("pattern")),
        }
    )
    await _schedule_payload(payload, background_tasks)
    return JSONResponse(
        {
            "status": "accepted",
            "job_id": payload["job_id"],
            "db_name": db_name,
            "bridge_db_name": bridge_db_name,
            "count": len(start_urls),
            "sample": start_urls[:5],
        }
    )


@router.post("/local-file-crawl/api/start-url")
async def start_url(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    body = await request.json()
    db_name = resolve_db_name(body, default="dev_user") or "dev_user"
    bridge_db_name = _bridge_db_name(body, db_name)
    chat_bot_id = _clean_text(body.get("chat_bot_id"))
    url = _clean_text(body.get("url"))
    if not chat_bot_id:
        raise HTTPException(status_code=400, detail="chat_bot_id is required")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    try:
        url = ensure_url_scheme(url)
    except Exception:
        pass

    payload = _base_payload(db_name=bridge_db_name, chat_bot_id=chat_bot_id, job_id=body.get("job_id"))
    payload.update(
        {
            "contents": [url],
            "contents_url": url,
            "target_url": url,
            "target_domains": [],
            "force_direct_detail": True,
            "single_detail_mode": True,
            "pre_explored_start_urls_count": 1,
            "selected_start_urls_count": 1,
            "actual_start_urls_count": 1,
            "local_file_crawl": True,
            "local_file_crawl_no_redis": True,
            "local_direct_save": True,
            "enable_db_save": False,
            "file_pipeline_skip_learning": True,
            "logical_db_name": db_name,
            "bridge_db_name": bridge_db_name,
            "local_file_crawl_mode": "single_url",
        }
    )
    await _schedule_payload(payload, background_tasks)
    return JSONResponse({"status": "accepted", "job_id": payload["job_id"], "db_name": db_name, "bridge_db_name": bridge_db_name, "url": url})


_PAGE_HTML = r"""
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Local File Crawl</title>
  <style>
    :root { color-scheme: light; font-family: Arial, sans-serif; }
    body { margin: 0; background: #f7f8fa; color: #1d2430; }
    main { max-width: 1120px; margin: 0 auto; padding: 24px; }
    h1 { font-size: 24px; margin: 0 0 18px; }
    h2 { font-size: 17px; margin: 0 0 12px; }
    section { background: #fff; border: 1px solid #d9dee7; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
    label { display: block; font-size: 12px; font-weight: 700; margin: 10px 0 5px; color: #415066; }
    input { width: 100%; box-sizing: border-box; height: 36px; border: 1px solid #c6ceda; border-radius: 6px; padding: 0 10px; font-size: 14px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .actions { display: flex; gap: 8px; align-items: center; margin-top: 14px; flex-wrap: wrap; }
    button { height: 36px; border: 1px solid #1f6feb; background: #1f6feb; color: #fff; border-radius: 6px; padding: 0 12px; font-weight: 700; cursor: pointer; }
    button.secondary { background: #fff; color: #1f6feb; }
    .check { display: inline-flex; gap: 6px; align-items: center; font-size: 13px; }
    .check input { width: auto; height: auto; }
    pre { white-space: pre-wrap; overflow: auto; max-height: 280px; margin: 0; background: #0f172a; color: #dbeafe; border-radius: 8px; padding: 12px; font-size: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #e6eaf0; vertical-align: top; }
    th { color: #536174; background: #f9fafc; }
    .url { word-break: break-all; }
    @media (max-width: 760px) { main { padding: 14px; } .row { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<main>
  <h1>Local File Crawl</h1>
  <section>
    <h2>Pattern rows from DB</h2>
    <div class="row"><div><label>DB name</label><input id="p-db" value="sb" /></div><div><label>Bridge DB</label><input id="p-bridge" value="f1_dev" /></div></div><label>Chatbot ID</label><input id="p-bot" />
    <label>URL pattern</label><input id="p-pattern" placeholder="/board/view.do?*" />
    <div class="row"><div><label>Limit</label><input id="p-limit" type="number" min="1" max="5000" value="200" /></div><div><label>&nbsp;</label><span class="check"><input id="p-post" type="checkbox" checked /> post rows only</span></div></div>
    <div class="actions"><button class="secondary" id="preview">Preview rows</button><button id="start-pattern">Start crawl</button></div>
  </section>
  <section>
    <h2>Single detail URL attachments</h2>
    <div class="row"><div><label>DB name</label><input id="u-db" value="sb" /></div><div><label>Bridge DB</label><input id="u-bridge" value="f1_dev" /></div></div><label>Chatbot ID</label><input id="u-bot" />
    <label>Detail URL</label><input id="u-url" placeholder="https://example.go.kr/.../view.do?nttNo=123" />
    <div class="actions"><button id="start-url">Start attachment crawl</button></div>
  </section>
  <section><h2>Result</h2><pre id="result">Ready.</pre></section>
  <section><h2>Progress</h2><pre id="progress">No active job.</pre></section>
  <section><h2>Preview</h2><div id="rows"></div></section>
</main>
<script>
const $ = (id) => document.getElementById(id);
const result = $("result");
const progress = $("progress");
const rows = $("rows");
let progressTimer = null;
function payload(prefix) { return { db_name: $(prefix + "-db").value.trim(), bridge_db_name: $(prefix + "-bridge").value.trim(), chat_bot_id: $(prefix + "-bot").value.trim() }; }
async function postJson(url, data) {
  result.textContent = "Working...";
  const res = await fetch(url, { method: "POST", headers: {"content-type": "application/json"}, body: JSON.stringify(data) });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.detail || json.message || res.statusText);
  result.textContent = JSON.stringify(json, null, 2);
  return json;
}
function escapeHtml(value) { return String(value).replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c])); }
function drawRows(items) {
  if (!items || !items.length) { rows.innerHTML = "<p>No rows.</p>"; return; }
  const body = items.map((r) => `<tr><td>${escapeHtml(r.id || "")}</td><td>${escapeHtml(r.type || "")}</td><td class="url">${escapeHtml(r.url || "")}</td><td>${escapeHtml(r.title || "")}</td></tr>`).join("");
  rows.innerHTML = `<table><thead><tr><th>ID</th><th>Type</th><th>URL</th><th>Title</th></tr></thead><tbody>${body}</tbody></table>`;
}
function compactProgress(json) {
  const c = json.counts || {};
  const files = (json.recent_files || []).slice(0, 5).map((f) => typeof f === "string" ? f : (f.name || f.url || JSON.stringify(f))).join("\n");
  return [
    `status: ${json.status || "-"}`,
    `job_id: ${json.job_id || "-"}`,
    `db: ${json.db_name || "-"} / bridge: ${json.bridge_db_name || "-"}`,
    `scan: ${c.scan || 0}/${c.total || 0}`,
    `attachments: ${c.attachments || 0}`,
    `collection: ${c.collection || 0}`,
    `save: ${c.save || 0}`,
    `study: ${c.study || 0}`,
    `failed: ${c.failed || 0}`,
    `event: ${json.event || "-"}`,
    `message: ${json.message || "-"}`,
    files ? `recent:\n${files}` : "recent: -"
  ].join("\n");
}
async function pollProgress(jobId) {
  const res = await fetch(`/local-file-crawl/api/status/${encodeURIComponent(jobId)}`);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.detail || res.statusText);
  progress.textContent = compactProgress(json);
  if (["completed", "error", "cancelled", "duplicate"].includes(String(json.status || "").toLowerCase())) {
    clearInterval(progressTimer);
    progressTimer = null;
  }
}
function trackJob(jobId) {
  if (!jobId) return;
  if (progressTimer) clearInterval(progressTimer);
  progress.textContent = `tracking ${jobId}...`;
  pollProgress(jobId).catch((err) => { progress.textContent = String(err.message || err); });
  progressTimer = setInterval(() => {
    pollProgress(jobId).catch((err) => { progress.textContent = String(err.message || err); });
  }, 1000);
}
$("preview").onclick = async () => {
  try { const data = payload("p"); data.pattern = $("p-pattern").value.trim(); data.limit = Number($("p-limit").value || 200); data.post_only = $("p-post").checked; drawRows((await postJson("/local-file-crawl/api/preview-pattern", data)).rows); }
  catch (err) { result.textContent = String(err.message || err); }
};
$("start-pattern").onclick = async () => {
  try { const data = payload("p"); data.pattern = $("p-pattern").value.trim(); data.limit = Number($("p-limit").value || 200); data.post_only = $("p-post").checked; trackJob((await postJson("/local-file-crawl/api/start-pattern", data)).job_id); }
  catch (err) { result.textContent = String(err.message || err); }
};
$("start-url").onclick = async () => {
  try { const data = payload("u"); data.url = $("u-url").value.trim(); trackJob((await postJson("/local-file-crawl/api/start-url", data)).job_id); }
  catch (err) { result.textContent = String(err.message || err); }
};
</script>
</body>
</html>
"""














