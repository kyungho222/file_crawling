from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, AsyncIterator, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_TXT_PATH = PROJECT_ROOT / "env.txt"


def _parse_env_txt_line(line: str) -> tuple[str, str] | None:
    raw = str(line or "").strip()
    if not raw or raw.startswith("#") or "=" not in raw:
        return None
    key, value = raw.split("=", 1)
    key = key.strip()
    value = value.strip().strip("'\"")
    if not key:
        return None
    return key, value


def _load_local_env_txt() -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not ENV_TXT_PATH.exists():
        return loaded
    lines: list[str] | None = None
    for encoding in ("utf-8", "utf-8-sig", "cp949", "euc-kr"):
        try:
            lines = ENV_TXT_PATH.read_text(encoding=encoding).splitlines()
            break
        except UnicodeDecodeError:
            continue
        except Exception:
            return loaded
    if lines is None:
        return loaded
    for line in lines:
        parsed = _parse_env_txt_line(line)
        if not parsed:
            continue
        key, value = parsed
        os.environ.setdefault(key, value)
        loaded[key] = value
    alias_pairs = (
        ("MARIA_DB_PASSWORD", "DB_PASSWORD"),
        ("MARIA_DB_PASSWORD", "MYSQL_PASS"),
        ("MYSQL_MYSQL_PASSWORD", "MYSQL_PASS"),
        ("POSTGRES_DB_PASSWORD", "POSTGRES_DB_PASSWORD"),
    )
    for source, target in alias_pairs:
        value = os.environ.get(source)
        if value:
            os.environ.setdefault(target, value)
    return loaded


_LOCAL_ENV_TXT_KEYS = _load_local_env_txt()

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from backend.local_file_crawl_server import (
    _base_payload,
    _bool_value,
    _clean_text,
    _job_id,
    _job_urls,
    _local_job_payload,
    _schedule_payload,
    router as local_file_crawl_router,
)
from backend.shared.crawler_state import crawler_state
from tools.file_crawl_dashboard.integration import include_public_routes
from utils.db_name import resolve_db_name
from utils.url import ensure_url_scheme

logger = logging.getLogger("tools.file_crawl_dashboard.local_server")

app = FastAPI(title="Local File Crawl Dashboard", version="1.0.0-local")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

include_public_routes(app)
app.include_router(local_file_crawl_router)
app.include_router(local_file_crawl_router, prefix="/Ai_Pro_filecrawler")


def _json_sse(payload: Dict[str, Any]) -> str:
    return "data: " + json.dumps(payload, ensure_ascii=False, default=str) + "\n\n"


def _flatten_status(job_id: str) -> Dict[str, Any]:
    payload = _local_job_payload(job_id)
    stats = dict(payload.get("stats") or {})
    counts = dict(payload.get("counts") or {})
    out: Dict[str, Any] = {
        "status": payload.get("status") or "unknown",
        "event": payload.get("event") or "local_file_crawl_status",
        "job_id": job_id,
        "db_name": payload.get("db_name"),
        "message": payload.get("message"),
        "file_pipeline_skip_learning": True,
        "enable_db_save": stats.get("enable_db_save", True),
        "scan_count": counts.get("scan") or stats.get("scan_count") or stats.get("total_count") or 0,
        "total_count": counts.get("total") or stats.get("total_count") or stats.get("scan_count") or 0,
        "collection_count": counts.get("collection") or stats.get("collection_count") or 0,
        "save_count": counts.get("save") or stats.get("save_count") or stats.get("save_done_count") or 0,
        "save_failed_count": stats.get("save_failed_count") or 0,
        "file_attachment_found_count": counts.get("attachments") or stats.get("file_attachment_found_count") or stats.get("attachment_count") or 0,
        "attachment_count": counts.get("attachments") or stats.get("attachment_count") or 0,
        "recent_files": payload.get("recent_files") or [],
        "local_status": payload,
    }
    if out["status"] == "unknown":
        out["status"] = "accepted"
    return out


def _first_url_from_dashboard_payload(body: Dict[str, Any]) -> str:
    urls = _job_urls(body)
    if urls:
        return urls[0]
    raw = body.get("contents_url") or body.get("target_url") or ""
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    contents = body.get("contents")
    if isinstance(contents, list) and contents:
        first = contents[0]
        if isinstance(first, dict):
            return _clean_text(first.get("url") or first.get("href") or first.get("content"))
        return _clean_text(first)
    return ""


def _build_local_prelearn_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(body, default=_local_dashboard_config().get("db_name") or "dev_user") or (_local_dashboard_config().get("db_name") or "dev_user")
    chat_bot_id = _clean_text(body.get("chat_bot_id"))
    if not chat_bot_id:
        raise HTTPException(status_code=400, detail="chat_bot_id is required")
    url = _first_url_from_dashboard_payload(body)
    if not url:
        raise HTTPException(status_code=400, detail="contents_url is required")
    try:
        url = ensure_url_scheme(url)
    except Exception:
        pass

    job_id = _clean_text(body.get("job_id")) or _job_id("local-file-crawl")
    payload = _base_payload(db_name=db_name, chat_bot_id=chat_bot_id, job_id=job_id)
    payload.update(body)
    payload.update(
        {
            "job_id": job_id,
            "db_name": db_name,
            "dbname": db_name,
            "account_name": db_name,
            "chat_bot_id": chat_bot_id,
            "colle": "file",
            "ui_colle": "file",
            "content_type": "file",
            "crawl_mode": "crawling",
            "method": body.get("method") or "period",
            "contents": [url],
            "contents_url": url,
            "target_url": url,
            "target_domains": body.get("target_domains") or [],
            "file_dashboard": True,
            "file_crawl_dashboard": True,
            "file_crawl_prelearn_dashboard": True,
            "local_file_crawl": True,
            "local_file_crawl_no_redis": True,
            "local_file_crawl_mode": "dashboard_prelearn",
            "enable_db_save": _bool_value(body.get("enable_db_save"), True),
            "file_pipeline_enable_db_save": _bool_value(body.get("file_pipeline_enable_db_save"), True),
            "enable_learning": False,
            "file_pipeline_skip_learning": True,
            "pre_explored_start_urls_count": int(body.get("pre_explored_start_urls_count") or 1),
            "selected_start_urls_count": int(body.get("selected_start_urls_count") or 1),
            "actual_start_urls_count": int(body.get("actual_start_urls_count") or 1),
        }
    )
    if body.get("start_urls_override"):
        payload["start_urls_override"] = body.get("start_urls_override")
        payload["start_urls_override_source"] = body.get("start_urls_override_source") or "file_crawl_dashboard_local"
        try:
            count = len(body.get("start_urls_override") or [])
            payload["pre_explored_start_urls_count"] = count
            payload["selected_start_urls_count"] = count
            payload["actual_start_urls_count"] = count
        except Exception:
            pass
    else:
        payload["force_direct_detail"] = bool(body.get("probe_direct_detail") or body.get("file_probe_direct_detail"))
        payload["single_detail_mode"] = bool(payload["force_direct_detail"])
    return payload


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = str(os.environ.get(name, "") or "").strip()
        if value:
            return value
    return default


def _masked(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= 6:
        return "***"
    return f"{text[:2]}***{text[-2:]}"


def _local_dashboard_config() -> Dict[str, Any]:
    return {
        "api_base": _env_first("LOCAL_FILE_CRAWL_API_BASE", default="/Ai_Pro_filecrawler"),
        "db_name": _env_first("LOCAL_FILE_CRAWL_DB_NAME", "FILE_CRAWL_DEFAULT_DB_NAME", "DEFAULT_DB_NAME", default="dev_user"),
        "bridge_db_name": _env_first("LOCAL_FILE_CRAWL_BRIDGE_DB_NAME", "FILE_CRAWL_BRIDGE_DB_NAME", default="f1_dev"),
        "chat_bot_id": _env_first("LOCAL_FILE_CRAWL_CHAT_BOT_ID", "DEFAULT_CHAT_BOT_ID", "CHAT_BOT_ID", default=""),
        "default_url": _env_first("LOCAL_FILE_CRAWL_DEFAULT_URL", "FILE_CRAWL_DEFAULT_URL", default=""),
        "method": _env_first("LOCAL_FILE_CRAWL_METHOD", default="period"),
        "start_urls_order": _env_first("LOCAL_FILE_CRAWL_START_URLS_ORDER", default="forward"),
        "env_txt_loaded": bool(_LOCAL_ENV_TXT_KEYS),
        "env_txt_path": str(ENV_TXT_PATH),
        "env_txt_keys": sorted(k for k in _LOCAL_ENV_TXT_KEYS if "KEY" not in k and "PASSWORD" not in k and "TOKEN" not in k and "SECRET" not in k),
        "db_connection": {
            "mysql_host": _env_first("MYSQL_HOST", "DB_HOST", default=""),
            "mysql_user": _env_first("MYSQL_USER", "DB_USER", default=""),
            "mysql_port": _env_first("MYSQL_PORT", "DB_PORT", default="3306"),
            "mysql_password": _masked(_env_first("MYSQL_PASS", "DB_PASSWORD", default="")),
            "redis_url": _env_first("REDIS_URL", default=""),
        },
    }


def _f1_dev_api_base() -> str:
    return str(os.getenv("F1_DEV_API_BASE") or "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler").rstrip("/")


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
        raise HTTPException(status_code=exc.code, detail=exc.read().decode("utf-8", errors="replace")[:500]) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"f1_dev bridge unavailable: {exc}") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="invalid f1_dev response")
    return data


@app.post("/backend/file-dashboard/exploration-posts")
@app.post("/Ai_Pro_filecrawler/backend/file-dashboard/exploration-posts")
async def proxy_exploration_posts(request: Request) -> JSONResponse:
    body = await request.json()
    if not isinstance(body, dict):
        body = {}
    data = await asyncio.to_thread(_post_f1_dev_json, "/backend/file-dashboard/exploration-posts", body)
    return JSONResponse(data)

@app.get("/local-file-crawl/api/config")
@app.get("/Ai_Pro_filecrawler/local-file-crawl/api/config")
async def local_dashboard_config() -> JSONResponse:
    return JSONResponse(_local_dashboard_config())

@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/file-crawl-dashboard")


@app.post("/backend/session/start")
@app.post("/Ai_Pro_filecrawler/backend/session/start")
async def local_dashboard_start(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="json object body is required")
    data = await asyncio.to_thread(_post_f1_dev_json, "/backend/session/start", body)
    return JSONResponse(data)

@app.get("/c1/crawl_sse/{db_name}/{job_id}")
@app.get("/Ai_Pro_filecrawler/c1/crawl_sse/{db_name}/{job_id}")
async def local_dashboard_sse(db_name: str, job_id: str) -> StreamingResponse:
    async def stream() -> AsyncIterator[str]:
        last = ""
        while True:
            payload = _flatten_status(job_id)
            payload["db_name"] = payload.get("db_name") or db_name
            encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
            if encoded != last:
                last = encoded
                yield _json_sse(payload)
            status = str(payload.get("status") or "").lower()
            if status in {"completed", "error", "cancelled", "canceled", "stop", "stopped"}:
                break
            await asyncio.sleep(1.0)
    return StreamingResponse(stream(), media_type="text/event-stream")


@app.post("/c1/crawl_stop/{job_id}")
@app.post("/Ai_Pro_filecrawler/c1/crawl_stop/{job_id}")
async def local_dashboard_stop(job_id: str) -> JSONResponse:
    task = getattr(crawler_state, "active_worker_tasks", {}).get(job_id)
    cancelled = False
    if task is not None and not task.done():
        task.cancel()
        cancelled = True
    try:
        from backend.local_file_crawl_server import LOCAL_JOBS
        saved = LOCAL_JOBS.setdefault(job_id, {})
        saved.update({"status": "cancelled", "finished_at": time.time(), "message": "local stop requested"})
    except Exception:
        logger.debug("[LocalFileCrawlDashboard] local job state stop update failed", exc_info=True)
    return JSONResponse({"status": "cancelled", "job_id": job_id, "cancelled_task": cancelled})


@app.post("/backend/file/preview-homepage-categories")
@app.post("/Ai_Pro_filecrawler/backend/file/preview-homepage-categories")
async def local_category_preview(request: Request) -> JSONResponse:
    body = await request.json()
    return JSONResponse({"status": "ok", "local_only": True, "message": "category preview is not run by the local test server", "request": body})


@app.post("/backend/file/sync-homepage-categories")
@app.post("/Ai_Pro_filecrawler/backend/file/sync-homepage-categories")
async def local_category_sync(request: Request) -> JSONResponse:
    body = await request.json()
    return JSONResponse({"status": "ok", "local_only": True, "message": "category sync is not run by the local test server", "request": body})
