import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse

from config.settings import (
    FILEUPLOAD_URL_PREFIX,
    get_file_download_path,
    get_fileupload_root,
)
from backend.file.file_category_apply import (
    preview_file_category_apply_plan,
    sync_existing_file_categories_from_homepage_learning,
)

logger = logging.getLogger("backend.file.file_endpoints")

router = APIRouter()


@router.post("/backend/upload")
async def upload_file(file: UploadFile = File(...), domain: Optional[str] = Form(None), chat_bot_id: Optional[str] = Form(None)):
    """
    단일 파일 업로드 엔드포인트.
    - form-data로 파일을 전송 (key: file)
    - domain, chat_bot_id를 함께 보내면 `/FileUpload/{domain}/{uuid_tail12}/{filename}` 형태로 저장
    - 저장 결과와 웹 접근 경로를 JSON으로 반환
    """
    try:
        root = get_fileupload_root()
        try:
            if domain and chat_bot_id:
                rel_dir = get_file_download_path(domain, chat_bot_id)
                if rel_dir.startswith(FILEUPLOAD_URL_PREFIX):
                    rel_sub = rel_dir[len(FILEUPLOAD_URL_PREFIX) :].lstrip("/\\")
                    local_dir = os.path.join(root, rel_sub)
                else:
                    local_dir = os.path.join(root, os.path.basename(rel_dir))
            else:
                local_dir = os.path.join(root, "unknown")
        except Exception:
            local_dir = os.path.join(root, "unknown")

        os.makedirs(local_dir, exist_ok=True)

        filename = getattr(file, "filename", None) or f"upload_{int(time.time())}"
        target_path = os.path.join(local_dir, filename)
        content = await file.read()
        with open(target_path, "wb") as fh:
            fh.write(content)

        rel_path = os.path.relpath(local_dir, root).replace(os.sep, "/")
        saved_web_path = f"{FILEUPLOAD_URL_PREFIX}/{rel_path}/{filename}"
        filesize = len(content)

        logger.info(
            "[Upload] saved | path=%s size=%d filename=%s domain=%s chat_bot_id=%s",
            target_path,
            filesize,
            filename,
            domain,
            chat_bot_id,
        )

        try:
            log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, "upload.log")
            with open(log_path, "a", encoding="utf-8") as lf:
                entry = json.dumps(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "saved_path": target_path,
                        "web_path": saved_web_path,
                        "filename": filename,
                        "filesize": filesize,
                        "domain": domain,
                        "chat_bot_id": chat_bot_id,
                    },
                    ensure_ascii=False,
                )
                lf.write(entry + "\n")
        except Exception as e:
            logger.warning("[Upload] failed to append upload.log: %s", e)

        return JSONResponse({"status": "ok", "saved_path": target_path, "web_path": saved_web_path, "filesize": filesize})
    except Exception as exc:
        logger.error("[Upload] failed | err=%s", exc, exc_info=True)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/file/sync-homepage-categories")
async def sync_homepage_categories_for_file_learning(request: Request):
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}

        chat_bot_id = str(body.get("chat_bot_id") or "").strip()
        db_name = str(body.get("db_name") or body.get("account_name") or body.get("dbname") or "").strip()
        access_url = str(body.get("access_url") or "").strip() or None

        if not chat_bot_id or not db_name:
            return JSONResponse(
                {"status": "error", "message": "chat_bot_id 와 db_name 이 필요합니다."},
                status_code=400,
            )

        result = await sync_existing_file_categories_from_homepage_learning(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            access_url=access_url,
            request_cookies=dict(request.cookies or {}),
        )
        return JSONResponse({"status": "ok", **result}, status_code=200)
    except Exception as exc:
        logger.error("[CategorySync][file] failed | err=%s", exc, exc_info=True)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/file/preview-homepage-categories")
async def preview_homepage_categories_for_file_learning(request: Request):
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}

        chat_bot_id = str(body.get("chat_bot_id") or "").strip()
        db_name = str(body.get("db_name") or body.get("account_name") or body.get("dbname") or "").strip()

        if not chat_bot_id or not db_name:
            return JSONResponse(
                {"status": "error", "message": "chat_bot_id 와 db_name 이 필요합니다."},
                status_code=400,
            )

        result = await preview_file_category_apply_plan(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
        )
        return JSONResponse({"status": "ok", **result}, status_code=200)
    except Exception as exc:
        logger.error("[CategoryPreview][file] failed | err=%s", exc, exc_info=True)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)
