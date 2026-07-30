import asyncio
import logging
import mimetypes
import os
from typing import Dict, Optional

import requests

from config.settings import settings

# 대용량 파일 업로드·서버 처리 시간 반영 (기본 5분, 환경변수 LEARN_FILE_UPLOAD_TIMEOUT_SEC 로 조절)
LEARN_FILE_UPLOAD_TIMEOUT_SEC = int(
    os.getenv("LEARN_FILE_UPLOAD_TIMEOUT_SEC", "300") or "300"
)
LEARN_FILE_UPLOAD_TIMEOUT_SEC = max(60, min(LEARN_FILE_UPLOAD_TIMEOUT_SEC, 1800))

logger = logging.getLogger(__name__)


def _upload_file_sync(
    url: str,
    file_path: str,
    chat_bot_id: str,
    extra_fields: Optional[Dict[str, str]] = None,
) -> bool:
    if not os.path.exists(file_path):
        logger.warning("[LearnFile] 파일이 존재하지 않습니다: %s", file_path)
        print(f"[LearnFile] ❌ 파일을 찾을 수 없습니다: {file_path}", flush=True)
        return False

    file_size = os.path.getsize(file_path)
    file_mtime = os.path.getmtime(file_path)

    data = {
        "chat_bot_id": chat_bot_id,
        "content_type": "file",
        "status": "N",
    }
    if extra_fields:
        data.update({k: v for k, v in extra_fields.items() if v is not None})

    mime_type, _ = mimetypes.guess_type(file_path)
    mime_type = mime_type or "application/octet-stream"

    print(f"[LearnFile] ▶ POST {url}", flush=True)
    print(f"[LearnFile]    - chat_bot_id: {chat_bot_id}", flush=True)
    print(f"[LearnFile]    - file_path: {file_path}", flush=True)
    print(f"[LearnFile]    - file_size: {file_size} bytes", flush=True)
    print(f"[LearnFile]    - modified_at: {file_mtime}", flush=True)
    print(f"[LearnFile]    - data fields: {list(data.keys())}", flush=True)
    with open(file_path, "rb") as fh:
        files = {
            "files[]": (os.path.basename(file_path), fh, mime_type),
        }
        try:
            response = requests.post(
                url, data=data, files=files, timeout=LEARN_FILE_UPLOAD_TIMEOUT_SEC
            )
            print(f"[LearnFile] Response status code: {response.status_code}", flush=True)
            print(f"[LearnFile] Response headers: {dict(response.headers)}", flush=True)
            
            response_text = response.text
            print(f"[LearnFile] Raw response text: {response_text[:500]}", flush=True)
            
            response.raise_for_status()
            
            # 응답이 비어있는지 확인
            if not response_text or not response_text.strip():
                logger.warning("[LearnFile] Empty response from server")
                print(f"[LearnFile] ❌ Empty response", flush=True)
                return False
            
            # JSON 파싱 시도
            try:
                payload = response.json()
                print(f"[LearnFile] ✅ Parsed JSON: {payload}", flush=True)
                
                if payload.get("status") == "success":
                    print(f"[LearnFile] ✅ Response: {payload}", flush=True)
                    return True
                else:
                    logger.warning(
                        "[LearnFile] Service returned non-success response: %s", payload
                    )
                    print(f"[LearnFile] ❌ Non-success: {payload}", flush=True)
                    return False
            except ValueError as json_err:
                # JSON 파싱 실패
                logger.warning(
                    "[LearnFile] Failed to parse JSON response: %s. Response text: %s",
                    json_err,
                    response_text[:200]
                )
                print(f"[LearnFile] ❌ JSON parse error: {json_err}", flush=True)
                print(f"[LearnFile] Response text: {response_text[:500]}", flush=True)
                return False
                
        except requests.exceptions.RequestException as req_exc:
            logger.warning("[LearnFile] Request failed: %s", req_exc)
            print(f"[LearnFile] ❌ Request Exception: {req_exc}", flush=True)
            return False
        except Exception as exc:  # noqa: BLE001 - broad to log issues
            logger.warning("[LearnFile] Upload failed: %s", exc, exc_info=True)
            print(f"[LearnFile] ❌ Exception: {exc}", flush=True)
            return False


async def upload_saved_file_to_db(
    file_path: str,
    chat_bot_id: Optional[str],
    extra_fields: Optional[Dict[str, str]] = None,
) -> bool:
    """
    saved 파일을 learn_file_add.php 서비스에 업로드하여 DB에 기록.
    """
    service_url = settings.LEARN_FILE_ADD_URL
    if not service_url:
        logger.debug(
            "[LearnFile] LEARN_FILE_ADD_URL is not configured. Skipping upload."
        )
        return False

    if not chat_bot_id:
        logger.debug("[LearnFile] chat_bot_id is missing; skipping upload.")
        return False

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _upload_file_sync, service_url, file_path, chat_bot_id, extra_fields or {}
    )

