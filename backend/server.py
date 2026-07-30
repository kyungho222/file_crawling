from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import sys
import mimetypes
import unicodedata
import uvicorn
from urllib.parse import unquote

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from .router import router as api_router
except ImportError:
    from backend.router import router as api_router

from config.settings import FILEUPLOAD_URL_PREFIX, get_fileupload_root
from backend.shared.runtime_loop import (
    ensure_safe_runtime_loop,
    initialize_runtime_hardening,
    log_runtime_loop_snapshot,
    resolve_uvicorn_loop_mode,
)


def _resolve_fastapi_root_path() -> str:
    raw = str(os.getenv("FASTAPI_ROOT_PATH", os.getenv("API_ROOT_PATH", "")) or "").strip()
    if not raw or raw == "/":
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw.rstrip("/")


app = FastAPI(root_path=_resolve_fastapi_root_path())
logger = logging.getLogger("backend.server")
mimetypes.add_type("application/x-hwp", ".hwp")
mimetypes.add_type("application/vnd.hancom.hwpx", ".hwpx")
initialize_runtime_hardening(logger, component="backend.server.import")

# Debug startup marker (no file writes)
try:
    import json, time
    logger.debug(
        json.dumps(
            {
                "sessionId": "debug-session",
                "runId": "run1",
                "hypothesisId": "H_STARTUP",
                "location": "backend/server.py:startup",
                "message": "server_module_imported",
                "data": {"pid": None},
                "timestamp": int(time.time() * 1000),
            },
            ensure_ascii=False,
        )
    )
except Exception:
    pass
# Configure CORS middleware
# - 운영은 backend/app.py를 주로 사용하지만, server.py로 띄우는 환경도 있어 동일 정책을 적용한다.
try:
    from config.settings import settings  # type: ignore
    allow_origins = getattr(settings, "CORS_ORIGINS", ["*"])
    allow_origin_regex = getattr(settings, "CORS_ORIGIN_REGEX_PATTERN", None)
except Exception:
    allow_origins = ["*"]
    allow_origin_regex = []
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_origin_regex=allow_origin_regex,
)

# Setup templates directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
if not os.path.exists(TEMPLATES_DIR):
    os.makedirs(TEMPLATES_DIR)

templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Include Router with /Ai_Pro_filecrawler prefix
app.include_router(api_router, prefix="/Ai_Pro_filecrawler")

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

# Optional: serve downloaded files (전달 경로 일원화: config)
fileupload_root = get_fileupload_root()
try:
    os.makedirs(fileupload_root, exist_ok=True)
except Exception:
    pass
try:
    app.mount(FILEUPLOAD_URL_PREFIX, StaticFiles(directory=fileupload_root), name="fileupload")
except Exception:
    pass


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
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.on_event("startup")
async def _runtime_startup_log():
    log_runtime_loop_snapshot(logger, component="backend.server.startup")
    ensure_safe_runtime_loop(logger, component="backend.server.startup")
    _log_callback_routes()

if __name__ == "__main__":
    loop = resolve_uvicorn_loop_mode()
    logger.info("[Server] Starting uvicorn with loop=%s", loop)
    uvicorn.run(app, host="0.0.0.0", port=8000, loop=loop)
