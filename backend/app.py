# backend/app.py
import asyncio
import inspect
import sys
import signal
import logging
import json
import time
import os
import mimetypes
import unicodedata
from typing import Dict, Any
from urllib.parse import unquote

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.shared.runtime_loop import (
    ensure_safe_runtime_loop,
    initialize_runtime_hardening,
    log_runtime_loop_snapshot,
    resolve_uvicorn_loop_mode,
)
from backend.shared.file_name_debug import (
    emit_file_name_debug,
    file_name_debug_log_path,
)

# CRITICAL: configure the Windows event loop policy during module import.
# The policy must be set before an event loop is created.
# Prefer Proactor for Playwright/asyncio subprocess compatibility.
logger = logging.getLogger("backend.app")

mimetypes.add_type("application/x-hwp", ".hwp")
mimetypes.add_type("application/vnd.hancom.hwpx", ".hwpx")


class _DropBreadcrumbOptionBridgeAccessLog(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            message = str(getattr(record, "msg", "") or "")
        return "/debug/breadcrumb_option_bridge" not in message


logging.getLogger("uvicorn.access").addFilter(_DropBreadcrumbOptionBridgeAccessLog())


def _filename_debug_log(location: str, **data: Any) -> None:
    emit_file_name_debug(component="app", location=location, data=data, logger=logger)


initialize_runtime_hardening(logger, component="backend.app.import")
_filename_debug_log(
    "startup",
    env_FILE_NAME_DEBUG=os.getenv("FILE_NAME_DEBUG"),
    env_CRAWL_DEBUG_FLOW=os.getenv("CRAWL_DEBUG_FLOW"),
    file_path=file_name_debug_log_path(),
)

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        # Compatibility fallback for older Python versions.
    except AttributeError:
        # Compatibility fallback for older Python versions.
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
        logger.info("[Main] DefaultEventLoopPolicy set at module load")

logger = logging.getLogger("backend.app")

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse, Response
from fastapi.templating import Jinja2Templates
import aiohttp
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
# Add project root to sys.path.

# Add project root to sys.path.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# New Modular Imports
from core.crawler.engine import run_crawler
from core.crawler.progress import Progress
# Backend Router (frontend integration API)

# Backend Router (frontend integration API)
from backend.router import router as backend_router
from backend.shared.crawler_state import crawler_state
from backend.shared.sse_utils import format_sse
from backend import app_state
from backend.shared.stop_service import stop_active_crawl
from backend.shared.shutdown_status import resolve_shutdown_crawl_status
from backend.shared.sse_publish_queue import ensure_worker_started
from config import settings
from config.settings import FILEUPLOAD_URL_PREFIX, get_fileupload_root

def _resolve_fastapi_root_path() -> str:
    raw = str(os.getenv("FASTAPI_ROOT_PATH", os.getenv("API_ROOT_PATH", "")) or "").strip()
    if not raw or raw == "/":
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw.rstrip("/")


app = FastAPI(root_path=_resolve_fastapi_root_path())

# Debug helper (NDJSON)
def _debug_log(*, location: str, message: str, data: Dict[str, Any], hypothesis_id: str) -> None:
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": "run1",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        logger.debug(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


# Debug middleware: log every incoming request path
@app.middleware("http")
async def _debug_request_mw(request: Request, call_next):
    # region agent log
    _debug_log(
        location="backend/app.py:middleware",
        message="request_in",
        data={
            "method": request.method,
            "path": request.url.path,
            "full_url": str(request.url),
            "client": getattr(request.client, "host", None),
            "headers_host": request.headers.get("host"),
        },
        hypothesis_id="H_PATH",
    )
    # endregion
    response = await call_next(request)
    return response

# CORS origins are managed by config/settings.py through CORS_ORIGINS.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,  # Loaded dynamically from environment/config.
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers
    allow_origin_regex=settings.CORS_ORIGIN_REGEX_PATTERN,
)

# Include Backend Router with /Ai_Pro_filecrawler prefix to match frontend requests
app.include_router(backend_router, prefix="/Ai_Pro_filecrawler")

# Board dashboard routes removed: this project is file-crawling only.

try:
    from tools.file_dashboard.integration import include_public_routes as include_file_dashboard_routes

    include_file_dashboard_routes(app)
except Exception as exc:
    logger.warning("[FileDashboard] router include skipped: %s", exc)

try:
    from tools.file_crawl_dashboard.integration import include_public_routes as include_file_crawl_dashboard_routes

    include_file_crawl_dashboard_routes(app)
except Exception as exc:
    logger.warning("[FileCrawlDashboard] router include skipped: %s", exc)

try:
    from backend.local_file_crawl_server import router as local_file_crawl_router

    app.include_router(local_file_crawl_router)
except Exception as exc:
    logger.warning("[LocalFileCrawl] router include skipped: %s", exc)

try:
    from backend.filecrawler_batch_endpoints import router as filecrawler_batch_router

    app.include_router(filecrawler_batch_router)
    app.include_router(filecrawler_batch_router, prefix="/Ai_Pro_filecrawler")
    app.include_router(filecrawler_batch_router, prefix="/api/Ai_Pro_filecrawler")
    app.include_router(filecrawler_batch_router, prefix="/api-aipro/f1_dev/api/Ai_Pro_filecrawler")
except Exception as exc:
    logger.warning("[FilecrawlerBatch] router include skipped: %s", exc)


def _log_callback_routes() -> None:
    try:
        paths = sorted(
            {
                str(getattr(route, "path", "") or "")
                for route in app.routes
                if "embedding/callback" in str(getattr(route, "path", "") or "")
                or "embedding-batch/callback" in str(getattr(route, "path", "") or "")
                or str(getattr(route, "path", "") or "") == "/batches/{batch_id}"
            }
        )
        logger.info(
            "[RouteCheck] root_path=%s callback_routes=%s",
            app.root_path or "",
            paths,
        )
    except Exception as exc:
        logger.debug("[RouteCheck] route log failed: %s", exc)

def _setup_signal_handlers():
    """Register SIGTERM/SIGINT signal handlers."""
    def signal_handler(signum, frame):
        try:
            signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        except (ValueError, AttributeError):
            signal_name = str(signum)
        
        logger.warning(f"[Signal Handler] {signal_name} signal received - exiting immediately")
        
        # Exit immediately without graceful shutdown.
        import os
        os._exit(0)
    
    # Unix/Linux: SIGTERM and SIGINT; Windows: SIGINT only.
    
    # Unix/Linux: SIGTERM and SIGINT; Windows: SIGINT only.
    if sys.platform != 'win32':
        try:
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
            logger.info("[Signal Handler] SIGTERM, SIGINT handlers registered")
        except (ValueError, OSError) as e:
            logger.warning(f"[Signal Handler] failed to register signal handlers: {e}")
    else:
        # Windows: SIGINT only
        try:
            signal.signal(signal.SIGINT, signal_handler)
            logger.info("[Signal Handler] SIGINT handler registered on Windows")
        except (ValueError, OSError) as e:
            logger.warning(f"[Signal Handler] failed to register SIGINT handler: {e}")

def _shutdown_timeout_seconds(env_name: str, default: float) -> float:
    try:
        value = float(os.getenv(env_name, str(default)) or str(default))
    except Exception:
        value = float(default)
    return max(0.5, min(value, 120.0))


async def _run_shutdown_action(*, label: str, action, timeout: float) -> None:
    try:
        result = action() if callable(action) else action
        if inspect.isawaitable(result):
            await asyncio.wait_for(result, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("[Shutdown] timeout | step=%s timeout=%.1fs", label, timeout)
    except Exception as exc:
        logger.warning("[Shutdown] failed | step=%s err=%s", label, exc)


@app.on_event("startup")
async def startup_handler():
    """
    Run application startup tasks.
    - Avoid DB work in the startup event.
    - Register signal handlers.
    """
    logger.info("[Startup] application startup")
    log_runtime_loop_snapshot(logger, component="backend.app.startup")
    ensure_safe_runtime_loop(logger, component="backend.app.startup")
    # Keep backend workflow logs visible in deployments where INFO logs are filtered.
    try:
        logging.getLogger("backend.file.integrated_workflow").setLevel(logging.INFO)
        logging.getLogger("backend.shared.workflow_runner").setLevel(logging.INFO)
        # httpx 요청 로그 과다 노출 방지 (INFO:HTTP Request ...)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        from backend.shared.log_compact_filter import install_board_shared_core_log_filter

        install_board_shared_core_log_filter()
    except Exception:
        pass
    _log_callback_routes()
    # Register signal handlers.
    
    # Register signal handlers.
    _setup_signal_handlers()
    try:
        loop = asyncio.get_running_loop()
        def _loop_exception_handler(loop, context):
            try:
                err = context.get("exception")
                msg = str(err) if err else str(context.get("message"))
            except Exception:
                msg = "unknown"
        loop.set_exception_handler(_loop_exception_handler)
    except Exception:
        pass

@app.on_event("shutdown")
async def shutdown_handler():
    """Update active workflow states during application shutdown."""
    import logging
    logger = logging.getLogger("backend.app")
    
    logger.info("[Shutdown] application shutdown started - updating active workflow states")
    
    from db.crawl_db_manager import update_crawling_log_counters
    workflow_stop_timeout = _shutdown_timeout_seconds("APP_SHUTDOWN_WORKFLOW_STOP_TIMEOUT_SEC", 10.0)
    db_update_timeout = _shutdown_timeout_seconds("APP_SHUTDOWN_DB_UPDATE_TIMEOUT_SEC", 5.0)
    resource_timeout = _shutdown_timeout_seconds("APP_SHUTDOWN_RESOURCE_TIMEOUT_SEC", 8.0)
    
    # Process active workflows.
    active_workflows = list(crawler_state.workflows.items())
    
    if not active_workflows:
        logger.info("[Shutdown] no active workflows")
        active_workflows = []
    
    logger.info(f"[Shutdown] active workflows found: {len(active_workflows)}")
    
    stop_tasks = []
    for job_id, workflow in active_workflows:
        try:
            # Load db_name from job_history.
            job_info = crawler_state.job_history.get(job_id, {})
            db_name = job_info.get("db_name") or "dev_user"  # default
            # Load final stats from the workflow.
            final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
            
            shutdown_status = resolve_shutdown_crawl_status(final_stats)

            # Update shutdown status in DB.
            await _run_shutdown_action(
                label=f"db_update:{job_id}",
                action=lambda job_id=job_id, final_stats=final_stats, shutdown_status=shutdown_status, db_name=db_name: update_crawling_log_counters(
                    job_id=job_id,
                    scan=final_stats.get('scan_count'),
                    collection=final_stats.get('collection_count'),
                    saved=final_stats.get('save_count'),
                    study=final_stats.get('study_count'),
                    status=shutdown_status,
                    dbname=db_name,
                    log_id=getattr(workflow, "craw_id", None),
                ),
                timeout=db_update_timeout,
            )

            logger.info(
                f"[Shutdown] workflow state updated to '{shutdown_status}' | "
                f"job_id={job_id} db={db_name} scan={final_stats.get('scan_count')}"
            )
            
            # Try to stop the workflow.
            stop_fn = getattr(workflow, "stop", None)
            if callable(stop_fn):
                stop_tasks.append(
                    _run_shutdown_action(
                        label=f"workflow_stop:{job_id}",
                        action=stop_fn,
                        timeout=workflow_stop_timeout,
                    )
                )
        except Exception as e:
            logger.error(f"[Shutdown] workflow state update failed | job_id={job_id} err={e}", exc_info=True)
    
    if stop_tasks:
        await asyncio.gather(*stop_tasks, return_exceptions=True)

    logger.info("[Shutdown] all workflow states updated")
    # 1) Stop global Playwright worker pool if running
    try:
        from core.crawler.global_pool import get_global_worker_pool

        pool = get_global_worker_pool()
        if pool and getattr(pool, "_started", False):
            await _run_shutdown_action(
                label="global_worker_pool.stop",
                action=pool.stop,
                timeout=resource_timeout,
            )
    except Exception:
        pass

    try:
        from backend.board.playwright_renderer import shutdown_playwright_renderer

        await _run_shutdown_action(
            label="playwright_renderer.shutdown",
            action=shutdown_playwright_renderer,
            timeout=resource_timeout,
        )
    except Exception:
        pass

    # 2) Close shared aiohttp session if initialized
    try:
        from utils.http_client import close_global_aiohttp

        await _run_shutdown_action(
            label="close_global_aiohttp",
            action=close_global_aiohttp,
            timeout=resource_timeout,
        )
    except Exception:
        pass

    # 3) Shutdown Redis connections (if any)
    try:
        from db.db_redis import shutdown_redis

        await _run_shutdown_action(
            label="shutdown_redis",
            action=shutdown_redis,
            timeout=resource_timeout,
        )
    except Exception:
        pass

# Setup templates
templates_dir = os.path.join(PROJECT_ROOT, "frontend", "templates")
templates = Jinja2Templates(directory=templates_dir)

# Mount Static Files
# NOTE: Mount static files only when the directory already exists.
# This avoids creating user-owned frontend/static implicitly.
static_dir = os.path.join(PROJECT_ROOT, "frontend", "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
else:
    # Fall back to backend/static when frontend/static does not exist.
    backend_static_dir = os.path.join(PROJECT_ROOT, "backend", "static")
    if os.path.isdir(backend_static_dir):
        try:
            app.mount("/static", StaticFiles(directory=backend_static_dir), name="static")
            logger.info(f"[StaticFiles] mounted backend/static as /static: {backend_static_dir}")
        except Exception as e:
            logger.warning(f"[StaticFiles] failed to mount backend/static as /static: {backend_static_dir} err={e}")
    else:
        logger.info(f"[StaticFiles] static mount skipped: {static_dir}")

# Mount FileUpload (전달 경로 일원화: config.get_fileupload_root, config.FILEUPLOAD_URL_PREFIX)
fileupload_root = get_fileupload_root()
try:
    os.makedirs(fileupload_root, exist_ok=True)
except Exception as e:
    logger.warning(f"[Main] WARNING: failed to ensure FILEUPLOAD_ROOT dir: {fileupload_root} err={e}")
try:
    app.mount(FILEUPLOAD_URL_PREFIX, StaticFiles(directory=fileupload_root), name="fileupload")
    logger.info(f"[Main] FileUpload mounted: url_prefix={FILEUPLOAD_URL_PREFIX} dir={fileupload_root}")
except Exception as e:
    logger.warning(f"[Main] WARNING: failed to mount {FILEUPLOAD_URL_PREFIX}: dir={fileupload_root} err={e}")


def _uploaded_match_key(value: str) -> str:
    text = str(value or "").replace("\\", "/").strip()
    for _ in range(2):
        try:
            decoded = unquote(text)
        except Exception:
            break
        if decoded == text:
            break
        text = decoded
    text = unicodedata.normalize("NFC", text)
    return " ".join(text.split()).casefold()


def _iter_uploaded_file_candidates(rel_file: str) -> list[str]:
    raw = str(rel_file or "").replace("\\", "/").lstrip("/")
    candidates: list[str] = []
    for value in (raw, unquote(raw)):
        value = unicodedata.normalize("NFC", str(value or "").replace("\\", "/").lstrip("/"))
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def _resolve_uploaded_file_path(uuid_tail: str, file_path: str, host: str | None = None) -> str | None:
    base = os.path.abspath(fileupload_root)
    tail = str(uuid_tail or "").strip().strip("/\\")
    rel_file = str(file_path or "").replace("\\", "/").lstrip("/")
    rel_candidates = _iter_uploaded_file_candidates(rel_file)
    if not tail or not rel_candidates:
        return None
    for rel in rel_candidates:
        if ".." in rel.split("/"):
            return None

    domains: list[str] = []
    if host:
        domains.append(str(host).split(":", 1)[0].strip())
    try:
        domains.extend(sorted(os.listdir(base)))
    except Exception:
        pass

    seen: set[str] = set()
    for domain in domains:
        domain = str(domain or "").strip()
        if not domain or domain in seen:
            continue
        seen.add(domain)
        tail_dir = os.path.abspath(os.path.join(base, domain, tail))
        try:
            if os.path.commonpath([base, tail_dir]) != base:
                continue
        except Exception:
            continue

        for rel in rel_candidates:
            candidate = os.path.abspath(os.path.join(tail_dir, *rel.split("/")))
            try:
                if os.path.commonpath([base, candidate]) != base:
                    continue
            except Exception:
                continue
            if os.path.isfile(candidate):
                return candidate

        requested_name = rel_candidates[-1].split("/")[-1]
        requested_key = _uploaded_match_key(requested_name)
        try:
            for entry in os.listdir(tail_dir):
                candidate = os.path.abspath(os.path.join(tail_dir, entry))
                if os.path.isfile(candidate) and _uploaded_match_key(entry) == requested_key:
                    return candidate
        except Exception:
            pass
    return None


def _uploaded_file_response(request: Request, uuid_tail: str, file_path: str, *, head_only: bool = False):
    resolved = _resolve_uploaded_file_path(uuid_tail, file_path, request.url.hostname)
    if not resolved:
        return JSONResponse({"detail": "uploaded file not found"}, status_code=404)
    media_type = mimetypes.guess_type(resolved)[0] or "application/octet-stream"
    headers = {"X-Content-Type-Options": "nosniff"}
    try:
        headers["Content-Length"] = str(os.path.getsize(resolved))
    except Exception:
        pass
    if head_only:
        return Response(status_code=200, media_type=media_type, headers=headers)
    return FileResponse(
        resolved,
        media_type=media_type,
        filename=os.path.basename(resolved),
        headers=headers,
    )


@app.get("/chat/uploaded_files/{uuid_tail}/{file_path:path}")
async def serve_uploaded_file_compat(request: Request, uuid_tail: str, file_path: str):
    return _uploaded_file_response(request, uuid_tail, file_path, head_only=False)


@app.head("/chat/uploaded_files/{uuid_tail}/{file_path:path}")
async def head_uploaded_file_compat(request: Request, uuid_tail: str, file_path: str):
    return _uploaded_file_response(request, uuid_tail, file_path, head_only=True)



@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    index_path = os.path.join(templates_dir, "index.html")
    if not os.path.isfile(index_path):
        logger.error("[Frontend] root template missing | path=%s", index_path)
        return HTMLResponse("Frontend template is not deployed.", status_code=503)
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/monitor", response_class=HTMLResponse)
async def stage_monitor(request: Request):
    """Stage monitor page for crawling progress."""
    return templates.TemplateResponse("stage_monitor.html", {"request": request})

# Legacy /crawl endpoint is disabled; run_background_crawl is kept for internal callers.

async def run_background_crawl(url: str, start_date=None, end_date=None):
    progress = app_state.crawl_progress
    crawl_progress = progress
    current_workflow = app_state.current_workflow
    
    # Initialize Progress Tracker
    progress_tracker = Progress()
    
    # Callback to update global state
    async def update_global_state(data):
        progress.update(data)
        
    progress_tracker.add_callback(update_global_state)
    
    try:
        # Use IntegratedWorkflow when a date range is provided; otherwise use the legacy engine.
        if start_date or end_date:
            from backend.file.integrated_workflow import IntegratedWorkflow
            
            def progress_callback(*args, **kwargs):
                """Progress callback."""
                # Support both IntegratedWorkflow and crawler progress payloads.
                updated = False
                
                # Positional arguments are ignored; keyword counters drive UI updates.
                if args:
                    # print(f"[Debug] progress_callback received args: {args}", flush=True)
                    pass
                
                # IntegratedWorkflow payloads (scan_count, collection_count, etc.)
                if 'scan_count' in kwargs:
                    crawl_progress['scan_count'] = kwargs['scan_count']
                    crawl_progress['stage'] = 'scan'
                    crawl_progress['message'] = f"탐색 중... ({kwargs['scan_count']}개 발견)"
                    crawl_progress['status'] = 'running'
                    updated = True
                if 'collection_count' in kwargs:
                    crawl_progress['collection_count'] = kwargs['collection_count']
                    crawl_progress['stage'] = 'collection'
                    crawl_progress['message'] = f"수집 중... ({kwargs['collection_count']}개 수집)"
                    crawl_progress['status'] = 'running'
                    updated = True
                if 'save_count' in kwargs:
                    crawl_progress['save_count'] = kwargs['save_count']
                    crawl_progress['stage'] = 'save'
                    crawl_progress['message'] = f"저장 중... ({kwargs['save_count']}개 저장)"
                    crawl_progress['status'] = 'running'
                    updated = True
                if 'study_count' in kwargs:
                    crawl_progress['study_count'] = kwargs['study_count']
                    crawl_progress['stage'] = 'study'
                    crawl_progress['message'] = f"학습 중... ({kwargs['study_count']}개 학습)"
                    crawl_progress['status'] = 'running'
                    updated = True
                
                # Handle crawler-style progress payloads (board_count, post_count, new_file).
                if 'board_count' in kwargs:
                    # board_count is another scan-stage counter, so reflect it in the scan stage.
                    if 'scan_count' not in kwargs:
                        crawl_progress['stage'] = 'scan'
                        crawl_progress['status'] = 'running'
                        crawl_progress['message'] = f"게시글 탐색 중... ({kwargs['board_count']}개 발견)"
                        updated = True
                
                if 'post_count' in kwargs:
                    # post_count represents discovered file-link candidates in the scan stage.
                    if 'scan_count' not in kwargs:
                        crawl_progress['scan_count'] = kwargs['post_count']
                        crawl_progress['stage'] = 'scan'
                        crawl_progress['status'] = 'running'
                        crawl_progress['message'] = f"파일 탐색 중... ({kwargs['post_count']}개 발견)"
                        updated = True
                
                # Update recent_files when a new file is discovered.
                if 'new_file' in kwargs and kwargs['new_file']:
                    if 'recent_files' not in crawl_progress:
                        crawl_progress['recent_files'] = []
                    
                    # Normalize new_file payload.
                    file_info = kwargs['new_file']
                    if isinstance(file_info, dict):
                        # Convert crawler file payload for the frontend.
                        recent_file = {
                            'name': file_info.get('filename', file_info.get('name', 'Unknown')),
                            'url': file_info.get('url', ''),
                            'source': file_info.get('source_page', file_info.get('source', ''))
                        }
                        _filename_debug_log(
                            "progress_recent_file",
                            incoming_name=file_info.get("name"),
                            incoming_filename=file_info.get("filename"),
                            recent_name=recent_file.get("name"),
                            url=recent_file.get("url"),
                            source=recent_file.get("source"),
                        )
                    else:
                        recent_file = file_info
                    
                    crawl_progress['recent_files'].insert(0, recent_file)
                    # Keep the latest 20 files only.
                    if len(crawl_progress['recent_files']) > 20:
                        crawl_progress['recent_files'] = crawl_progress['recent_files'][:20]
                    
                    # Increment scan_count only when scan_count was not provided explicitly.
                    if 'scan_count' not in kwargs:
                        current_scan_count = crawl_progress.get('scan_count', 0)
                        crawl_progress['scan_count'] = current_scan_count + 1
                        crawl_progress['stage'] = 'scan'
                        crawl_progress['status'] = 'running'
                        crawl_progress['message'] = f"탐색 중... ({crawl_progress['scan_count']}개 발견)"
                    
                    updated = True
                
                # Log progress only when one of the primary counters changed.
                if updated and any(key in kwargs for key in ['scan_count', 'collection_count', 'save_count', 'study_count', 'new_file']):
                    logger.info(f"[Main] Progress: stage={crawl_progress.get('stage')}, scan={crawl_progress.get('scan_count')}, collection={crawl_progress.get('collection_count')}, save={crawl_progress.get('save_count')}, study={crawl_progress.get('study_count')}")
            
            # Initialize progress message.
            crawl_progress["message"] = "크롤링 시작 중..."
            crawl_progress["status"] = "running"
            crawl_progress["stage"] = "start"
            
            app_state.current_workflow = IntegratedWorkflow()
            current_workflow = app_state.current_workflow
            
            # Callback that sends crawler-discovered files into the workflow.
            def crawler_file_callback(file_info):
                """Send a crawler-discovered file into the collection stage."""
                if not (current_workflow and file_info):
                    return

                # Convert file metadata into the scan result format.
                import datetime
                import asyncio
                from backend.shared.duplicate_utils import generate_unique_key

                url = file_info.get('url', '')
                filename = file_info.get('name', file_info.get('filename', ''))
                if not filename:
                    # Extract a filename from the URL.
                    filename = url.split('/')[-1].split('?')[0]
                _filename_debug_log(
                    "crawler_file_callback",
                    incoming_name=file_info.get("name"),
                    incoming_filename=file_info.get("filename"),
                    selected_filename=filename,
                    url=url,
                    source_page=file_info.get("source_page"),
                )

                filesize = 1024 * 1024  # Mock size
                ext = filename.split('.')[-1] if '.' in filename else ''
                scan_result = {
                    'url': url,
                    'filename': filename,
                    'filesize': filesize,
                    'ext': ext,
                    'type': file_info.get('type', 'file'),
                    'source_page': file_info.get('source_page', ''),
                    'created_at': datetime.datetime.now().isoformat(),
                    'unique_key': generate_unique_key(url, filename, filesize),
                    'post_date': file_info.get('post_date'),
                }

                current_workflow.crawler_file_buffer.append(scan_result)

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running() and current_workflow.is_running:
                        asyncio.create_task(current_workflow.process_scan_batch([scan_result]))
                        logger.info(
                            f"[Main] Crawler file queued for collection: "
                            f"{scan_result.get('filename', 'unknown')[:50]}"
                        )
                except Exception as e:
                    logger.error(f"[Main] Error queuing crawler file: {e}", exc_info=True)
                    import traceback
                    traceback.print_exc()

            # Merge the crawler file callback into progress_callback.
            original_progress_callback = progress_callback
            def enhanced_progress_callback(*args, **kwargs):
                # Update the UI through the original progress callback first.
                original_progress_callback(*args, **kwargs)

                # If new_file is present, pass it to the crawler file callback.
                if 'new_file' in kwargs and kwargs['new_file']:
                    crawler_file_callback(kwargs['new_file'])
            
            await current_workflow.start_workflow(
                start_url=url,
                progress_callback=enhanced_progress_callback,
                stop_check=None,  # The workflow uses its internal stop_event.
                start_date=start_date,
                end_date=end_date
            )
            current_workflow = None  # Cleanup after completion
            app_state.current_workflow = None
        else:
            await run_crawler(url, progress_tracker)
    except asyncio.CancelledError:
        logger.info("[Main] Crawl task cancelled.")
        crawl_progress["status"] = "cancelled"
        crawl_progress["message"] = "사용자에 의해 중단되었습니다."
        crawl_progress["error"] = None
        # Clean up the active workflow as well.
        if current_workflow:
            ret = current_workflow.stop()
            import inspect
            if inspect.isawaitable(ret):
                await ret
            current_workflow = None
            app_state.current_workflow = None
    except NotImplementedError as e:
        logger.critical("[Main] CRITICAL ERROR: NotImplementedError occurred.")
        logger.critical("[Main] This is a Windows subprocess issue with Playwright.")
        logger.critical("Error details: %s", e, exc_info=True)
        crawl_progress["status"] = "error"
        crawl_progress["error"] = f"Windows subprocess error: {str(e)}"
        crawl_progress["message"] = "크롤링 중 Windows subprocess 오류가 발생했습니다. 이벤트 루프 정책을 확인하세요."
    except Exception as e:
        logger.error(f"[Main] Unexpected Error: {e}", exc_info=True)
        crawl_progress["status"] = "error"
        crawl_progress["error"] = str(e)
        crawl_progress["message"] = f"오류 발생: {e}"

@app.get("/progress")
async def get_progress():
    """Get current crawling progress"""
    return JSONResponse(app_state.crawl_progress)

# @app.post("/stop")
# async def stop_crawl():
#     print("================以묐떒?붿껌 ?몄텧app.py==================")
#     """Stop current crawling"""
    # Verify the event loop policy configured during module import.
#     print("================done==================")
#     return JSONResponse(result)


EXTERNAL_SSE_HOST = os.getenv("EXTERNAL_SSE_HOST", "https://dev.chatbaram.com:7000")

if __name__ == "__main__":
    import uvicorn
    
    # Verify the event loop policy configured during module import.
    if sys.platform == 'win32':
        policy = asyncio.get_event_loop_policy()
        if isinstance(policy, asyncio.WindowsProactorEventLoopPolicy):
            logger.info("[Main] WindowsProactorEventLoopPolicy confirmed")
        else:
            logger.warning(f"[Main] WARNING: Event loop policy is {type(policy).__name__}, not WindowsProactorEventLoopPolicy!")

    _loop = resolve_uvicorn_loop_mode()
    logger.info("[Main] Starting uvicorn with loop=%s", _loop)

    uvicorn.run(
        "backend.app:app",  # backend/app.py의 app 객체 참조
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        loop=_loop,
    )

