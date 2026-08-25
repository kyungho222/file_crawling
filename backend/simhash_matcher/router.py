"""Standalone HTTP API for the SimHash matcher.

This router deliberately does not live below the crawler URL prefix.  It can
therefore be deployed behind a small public API gateway without starting a
crawl workflow.
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import re
import socket
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.simhash_matcher.public_simhash import (
    _clean,
    format_simhash,
    make_simhash,
    make_simhash_from_text,
    public_simhash,
)
from db.maria_operations import maria_execute_query
from utils.db_name import resolve_db_name


logger = logging.getLogger("backend.simhash_matcher.router")

router = APIRouter(prefix="/backend/public-simhash", tags=["public-simhash"])
file_simhash_router = APIRouter(prefix="/simhash", tags=["file-simhash"])


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


_URL_REQUEST_CONCURRENCY = _env_int(
    "SIMHASH_URL_REQUEST_CONCURRENCY", 2, minimum=1, maximum=8
)
_URL_REQUEST_TIMEOUT_SEC = _env_int(
    "SIMHASH_URL_REQUEST_TIMEOUT_SEC", 90, minimum=10, maximum=300
)
_url_request_semaphore = asyncio.Semaphore(_URL_REQUEST_CONCURRENCY)


class SimhashTextRequest(BaseModel):
    subject: str | None = Field(default=None, max_length=100_000)
    content: str | None = Field(default=None, max_length=2_000_000)
    text: str | None = Field(default=None, max_length=2_000_000)


class SimhashCheckRequest(BaseModel):
    db_name: str = Field(min_length=1, max_length=64)
    chat_bot_id: str = Field(min_length=1, max_length=128)
    subject: str | None = Field(default=None, max_length=100_000)
    content: str | None = Field(default=None, max_length=2_000_000)
    text: str | None = Field(default=None, max_length=2_000_000)


class SimhashUrlRequest(BaseModel):
    url: str = Field(min_length=1, max_length=4_096)


class FileSimhashGenerateRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=256)
    job_id: str = Field(min_length=1, max_length=256)
    id: int = Field(gt=0)
    db_name: str = Field(min_length=1, max_length=64)
    chat_bot_id: str = Field(min_length=1, max_length=128)
    content_type: str = Field(default="file", max_length=32)
    file_url: str = Field(default="", max_length=4_096)
    source_url: str = Field(default="", max_length=4_096)
    title: str = Field(default="", max_length=100_000)
    content: str = Field(default="")


def _hash_from_payload(payload: SimhashTextRequest) -> int:
    text = str(payload.text or "").strip()
    if text:
        value = make_simhash_from_text(text)
    else:
        subject = str(payload.subject or "").strip()
        content = str(payload.content or "").strip()
        if not subject or not content:
            raise HTTPException(
                status_code=422,
                detail="provide text, or provide both subject and content",
            )
        value = make_simhash(subject, content)
    if value is None:
        raise HTTPException(status_code=422, detail="unable to create simhash from empty text")
    return value


@file_simhash_router.post("/generate")
async def generate_file_simhash(payload: FileSimhashGenerateRequest) -> dict[str, Any]:
    """Generate only the agreed decimal file SimHash; never read a database."""
    if str(payload.content_type or "").strip().lower() != "file":
        return {
            "schema": "simhash.generate.v1",
            "ok": False,
            "status": "failed",
            "request_id": payload.request_id,
            "hash": None,
            "normalized_length": 0,
        }
    content = str(payload.content or "").strip()
    title = str(payload.title or "").strip()
    normalized_length = len(_clean(content))
    if not content or not title:
        return {
            "schema": "simhash.generate.v1",
            "ok": False,
            "status": "failed",
            "request_id": payload.request_id,
            "hash": None,
            "normalized_length": normalized_length,
        }
    value = make_simhash(title, content)
    if value is None:
        return {
            "schema": "simhash.generate.v1",
            "ok": False,
            "status": "failed",
            "request_id": payload.request_id,
            "hash": None,
            "normalized_length": normalized_length,
        }
    return {
        "schema": "simhash.generate.v1",
        "ok": True,
        "status": "completed",
        "request_id": payload.request_id,
        "hash": str(value),
        "normalized_length": normalized_length,
    }


async def _validate_public_http_url(raw_url: str) -> str:
    value = str(raw_url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HTTPException(status_code=422, detail="url must be an absolute http or https URL")

    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost"):
        raise HTTPException(status_code=422, detail="local addresses are not allowed")

    try:
        addresses = await asyncio.to_thread(socket.getaddrinfo, host, None, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise HTTPException(status_code=422, detail=f"url host could not be resolved: {host}") from exc

    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        if not ip.is_global:
            raise HTTPException(status_code=422, detail="non-public network addresses are not allowed")
    return value


def _learn_list_table(chat_bot_id: str) -> str:
    tail = str(chat_bot_id or "").rsplit("-", 1)[-1].lower()
    if not re.fullmatch(r"[a-z0-9]{12}", tail):
        raise HTTPException(status_code=422, detail="chat_bot_id must end with a 12-character alphanumeric identifier")
    return f"ASADAL_{tail}_LEARN_LIST"


@router.get("/health")
async def simhash_health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "simhash_matcher",
        "url_request_concurrency": _URL_REQUEST_CONCURRENCY,
        "url_request_timeout_sec": _URL_REQUEST_TIMEOUT_SEC,
    }


@router.post("/generate")
async def generate_simhash(payload: SimhashTextRequest) -> dict[str, str]:
    return {"hash": format_simhash(_hash_from_payload(payload))}


@router.post("/check")
async def check_simhash(payload: SimhashCheckRequest) -> dict[str, Any]:
    payload_data = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    value = _hash_from_payload(SimhashTextRequest(**payload_data))
    db_name = resolve_db_name({"db_name": payload.db_name}, default=payload.db_name)
    table = _learn_list_table(payload.chat_bot_id)
    hash_text = format_simhash(value)
    try:
        rows = await maria_execute_query(
            f"SELECT 1 FROM `{table}` WHERE `hash`=%s LIMIT 1",
            (hash_text,),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        if "unknown column" in str(exc).lower() and "hash" in str(exc).lower():
            logger.warning("[SimHash] hash column unavailable | db=%s table=%s", db_name, table)
            return {"duplicate": False, "save": True, "hash": hash_text, "match_available": False}
        logger.exception("[SimHash] duplicate check failed | db=%s table=%s", db_name, table)
        raise HTTPException(status_code=502, detail="simhash duplicate lookup failed") from exc
    duplicate = bool(rows)
    return {"duplicate": duplicate, "save": not duplicate, "hash": hash_text, "match_available": True}


@router.post("")
async def generate_url_simhash(payload: SimhashUrlRequest) -> dict[str, Any]:
    url = await _validate_public_http_url(payload.url)
    try:
        async with _url_request_semaphore:
            return await asyncio.wait_for(public_simhash(url), timeout=_URL_REQUEST_TIMEOUT_SEC)
    except TimeoutError as exc:
        logger.warning("[SimHash] URL render timed out | url=%s timeout_sec=%s", url, _URL_REQUEST_TIMEOUT_SEC)
        raise HTTPException(status_code=504, detail="simhash URL rendering timed out") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[SimHash] URL render failed | url=%s", url)
        raise HTTPException(status_code=502, detail="simhash URL rendering failed") from exc
