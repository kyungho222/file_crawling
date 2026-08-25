"""File-crawl client for the hash-only external SimHash API."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

import aiohttp


logger = logging.getLogger("backend.shared.file_simhash_generation")

FILE_SIMHASH_GENERATE_URL = "http://110.45.147.63:3030/simhash/generate"
_MAX_SIMHASH_VALUE = (1 << 128) - 1
_DECIMAL_SIMHASH_PATTERN = re.compile(r"^\d{1,39}$")


def normalize_decimal_simhash(value: Any) -> Optional[str]:
    """Accept only the agreed 128-bit decimal SimHash representation."""
    raw = str(value or "").strip()
    if not _DECIMAL_SIMHASH_PATTERN.fullmatch(raw):
        return None
    try:
        parsed = int(raw, 10)
    except ValueError:
        return None
    if parsed < 0 or parsed > _MAX_SIMHASH_VALUE:
        return None
    return str(parsed)


def _pure_file_body(content: str) -> str:
    """Exclude extractor provenance lines; the API receives document body only."""
    return "\n".join(
        line
        for line in str(content or "").splitlines()
        if not re.match(r"^\[(?:Source|Page):", line.strip(), flags=re.IGNORECASE)
    ).strip()


def build_file_simhash_payload(
    *,
    job_id: str,
    learn_list_row_id: int,
    db_name: str,
    chat_bot_id: str,
    file_url: str,
    source_url: str,
    title: str,
    content: str,
) -> Dict[str, str]:
    """Build the row-addressable contract for the external file SimHash service."""
    row_id = int(learn_list_row_id or 0)
    if row_id <= 0:
        raise ValueError("LEARN_LIST.id is required for a file SimHash request")
    normalized_job_id = str(job_id or "").strip()
    body = _pure_file_body(content)
    return {
        "request_id": f"{normalized_job_id}:{row_id}",
        "job_id": normalized_job_id,
        "id": str(row_id),
        "db_name": str(db_name or "").strip(),
        "chat_bot_id": str(chat_bot_id or "").strip(),
        "content_type": "file",
        "file_url": str(file_url or "").strip(),
        "source_url": str(source_url or "").strip(),
        "title": str(title or "").strip(),
        "content": body,
    }


def _response_error_message(response_body: Any, default: str = "-") -> str:
    if not isinstance(response_body, dict):
        return default
    value = str(response_body.get("error_message") or "").strip()
    return value[:500] or default


async def generate_file_simhash(
    *,
    job_id: str,
    learn_list_row_id: int,
    db_name: str,
    chat_bot_id: str,
    file_url: str,
    source_url: str,
    title: str,
    content: str,
    consume_result: bool = True,
) -> Optional[str]:
    """Request a decimal SimHash without performing duplicate checks."""
    payload = build_file_simhash_payload(
        job_id=job_id,
        learn_list_row_id=learn_list_row_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        file_url=file_url,
        source_url=source_url,
        title=title,
        content=content,
    )
    body = payload["content"]
    if not body:
        logger.info(
            "[FileSimHash][request_skipped] request_id=%s source_url=%s reason=empty_body",
            payload["request_id"],
            payload["source_url"][:220],
        )
        return None

    try:
        from backend.shared.file_simhash_request_trace import record_file_simhash_request

        record_file_simhash_request(payload)
    except Exception as exc:
        logger.warning(
            "[FileSimHash][request_trace_failed] request_id=%s err_type=%s",
            payload["request_id"],
            type(exc).__name__,
        )

    logger.info(
        "[FileSimHash][request_start] request_id=%s job_id=%s id=%s db=%s chat_bot_id=%s file_url=%s source_url=%s title=%s content_chars=%s",
        payload["request_id"],
        payload["job_id"],
        payload["id"],
        payload["db_name"],
        payload["chat_bot_id"],
        payload["file_url"][:220],
        payload["source_url"][:220],
        payload["title"][:160],
        len(body),
    )
    timeout = aiohttp.ClientTimeout(total=10.0, connect=3.0, sock_read=7.0)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(FILE_SIMHASH_GENERATE_URL, json=payload) as response:
                if not consume_result:
                    if response.status != 200:
                        logger.warning(
                            "[FileSimHash][request_failed] request_id=%s status=%s source_url=%s error_message=result_discarded",
                            payload["request_id"],
                            response.status,
                            payload["source_url"][:220],
                        )
                    return None
                response_body = await response.json(content_type=None)
                if response.status != 200 or not isinstance(response_body, dict):
                    logger.warning(
                        "[FileSimHash][request_failed] request_id=%s status=%s source_url=%s error_message=%s",
                        payload["request_id"],
                        response.status,
                        payload["source_url"][:220],
                        _response_error_message(response_body, "invalid_response"),
                    )
                    return None
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.warning(
            "[FileSimHash][request_failed] request_id=%s source_url=%s err_type=%s error_message=%s",
            payload["request_id"],
            payload["source_url"][:220],
            type(exc).__name__,
            "client_request_failed",
        )
        return None

    if (
        response_body.get("schema") != "simhash.generate.v1"
        or response_body.get("ok") is not True
        or response_body.get("status") != "completed"
        or str(response_body.get("request_id") or "") != payload["request_id"]
    ):
        logger.warning(
            "[FileSimHash][response_not_completed] request_id=%s source_url=%s status=%s error_message=%s",
            payload["request_id"],
            payload["source_url"][:220],
            response_body.get("status"),
            _response_error_message(response_body),
        )
        return None
    value = normalize_decimal_simhash(response_body.get("hash"))
    if value is None:
        logger.warning(
            "[FileSimHash][invalid_hash] request_id=%s source_url=%s error_message=%s",
            payload["request_id"],
            payload["source_url"][:220],
            _response_error_message(response_body, "invalid_decimal_hash"),
        )
    else:
        logger.info(
            "[FileSimHash][response_completed] request_id=%s source_url=%s normalized_length=%s hash_length=%s",
            payload["request_id"],
            payload["source_url"][:220],
            response_body.get("normalized_length"),
            len(value),
        )
    return value
