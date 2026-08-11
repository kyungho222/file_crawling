"""Local API server for the detail attachment extractor dashboard."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote, urlparse

import aiohttp
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from extract_detail_attachments import extract_from_url


BASE_DIR = Path(__file__).resolve().parent
app = FastAPI(title="Detail Attachment Extractor")


class ExtractRequest(BaseModel):
    url: str = Field(min_length=1, max_length=4096)
    timeout: float = Field(default=20.0, ge=1.0, le=60.0)


@app.get("/")
async def dashboard() -> FileResponse:
    return FileResponse(BASE_DIR / "dashboard.html")


@app.post("/api/extract")
async def extract_attachments(payload: ExtractRequest) -> dict[str, object]:
    try:
        return await extract_from_url(payload.url, payload.timeout)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"detail fetch failed: {exc}") from exc


def _download_file_name(value: str) -> str:
    return str(value or "attachment").replace("\r", "").replace("\n", "").strip() or "attachment"


@app.get("/api/download")
async def download_attachment(
    url: str = Query(min_length=1, max_length=8192),
    file_name: str = Query(default="attachment", max_length=512),
) -> StreamingResponse:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=422, detail="url must be an absolute http(s) URL")

    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=60),
        trust_env=False,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DetailAttachmentExtractor/1.0)"},
    )
    try:
        upstream = await session.get(url, allow_redirects=True)
    except Exception as exc:
        await session.close()
        raise HTTPException(status_code=502, detail=f"attachment download failed: {exc}") from exc

    if upstream.status >= 400:
        upstream.release()
        await session.close()
        raise HTTPException(status_code=502, detail=f"attachment download failed: HTTP {upstream.status}")

    resolved_name = _download_file_name(file_name)
    headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{quote(resolved_name, safe='')}"}
    if content_length := upstream.headers.get("Content-Length"):
        headers["Content-Length"] = content_length

    async def stream_attachment():
        try:
            async for chunk in upstream.content.iter_chunked(64 * 1024):
                yield chunk
        finally:
            upstream.release()
            await session.close()

    return StreamingResponse(
        stream_attachment(),
        media_type=upstream.headers.get("Content-Type", "application/octet-stream"),
        headers=headers,
    )
