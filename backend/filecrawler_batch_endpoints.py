import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from backend.shared.batch_embedding_scheduler import (
    batch_embedding_flow_debug_enabled,
    batch_callback_requires_auth,
    cancel_embedding_batch,
    callback_token_matches,
    mark_pending_embedding_callback_done,
    _extract_callback_batch_id,
    process_embedding_callback,
)

logger = logging.getLogger("backend.filecrawler_batch_endpoints")

router = APIRouter()


def _extract_callback_token(request: Request) -> str:
    auth_header = str(request.headers.get("authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    for key in ("x-api-token", "api-token", "x-callback-token"):
        value = str(request.headers.get(key) or "").strip()
        if value:
            return value
    return ""



@router.post("/embedding-batch/callback")
@router.post("/backend/filecrawler/embedding-batch/callback")
async def filecrawler_embedding_batch_callback(request: Request):
    if batch_callback_requires_auth():
        token = _extract_callback_token(request)
        if not callback_token_matches(token):
            raise HTTPException(status_code=401, detail="invalid callback token")

    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("[FilecrawlerBatchCallback] invalid json: %s", exc)
        return JSONResponse(
            {"status": "invalid_json", "message": str(exc)},
            status_code=400,
        )
    try:
        result = await process_embedding_callback(payload)
    except Exception as exc:
        try:
            batch_id = _extract_callback_batch_id(payload if isinstance(payload, dict) else {})
            mark_pending_embedding_callback_done(batch_id=batch_id, reason="callback_endpoint_exception")
        except Exception:
            pass
        logger.exception("[FilecrawlerBatchCallback] failed: %s", exc)
        return JSONResponse(
            {"status": "error", "message": str(exc)},
            status_code=500,
        )
    return JSONResponse(result, status_code=200)


@router.delete("/batches/{batch_id}")
@router.delete("/batches/{batch_id}/")
async def delete_batch(batch_id: str, request: Request):
    if batch_callback_requires_auth():
        token = _extract_callback_token(request)
        if not callback_token_matches(token):
            raise HTTPException(status_code=401, detail="invalid callback token")

    try:
        result = await cancel_embedding_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("[FilecrawlerBatchDelete] failed: %s", exc)
        return JSONResponse(
            {"status": "error", "message": str(exc), "batch_id": str(batch_id or "").strip()},
            status_code=500,
        )

    status_code = 404 if result.get("status") == "not_found" else 200
    return JSONResponse(result, status_code=status_code)
