"""In-memory inspection feed for outbound file SimHash requests."""

from __future__ import annotations

import hashlib
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List


_MAX_FILE_SIMHASH_REQUEST_HISTORY = 50
_REQUEST_HISTORY: Deque[Dict[str, Any]] = deque(maxlen=_MAX_FILE_SIMHASH_REQUEST_HISTORY)
_NEXT_SEQUENCE = 0


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _copy_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    copied = dict(entry)
    copied["payload"] = dict(entry.get("payload") or {})
    return copied


def record_file_simhash_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Store the exact outbound payload for short-lived operator inspection."""
    global _NEXT_SEQUENCE

    copied_payload = {str(key): value for key, value in dict(payload or {}).items()}
    content = str(copied_payload.get("content") or "")
    _NEXT_SEQUENCE += 1
    entry = {
        "sequence": _NEXT_SEQUENCE,
        "recorded_at": _now(),
        "job_id": str(copied_payload.get("job_id") or "").strip(),
        "request_id": str(copied_payload.get("request_id") or "").strip(),
        "learn_list_row_id": str(copied_payload.get("id") or "").strip(),
        "title": str(copied_payload.get("title") or "").strip(),
        "content_chars": len(content),
        "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "payload": copied_payload,
    }
    _REQUEST_HISTORY.append(entry)
    return _copy_entry(entry)


def list_file_simhash_request_trace(
    *,
    after_sequence: int = 0,
    job_id: str = "",
) -> List[Dict[str, Any]]:
    """Return recent requests in chronological order, optionally filtered by job."""
    after = max(0, int(after_sequence or 0))
    requested_job_id = str(job_id or "").strip()
    return [
        _copy_entry(entry)
        for entry in _REQUEST_HISTORY
        if int(entry.get("sequence") or 0) > after
        and (not requested_job_id or entry.get("job_id") == requested_job_id)
    ]


def clear_file_simhash_request_trace() -> None:
    """Clear the process-local feed; used by deterministic checks and shutdown tools."""
    global _NEXT_SEQUENCE
    _REQUEST_HISTORY.clear()
    _NEXT_SEQUENCE = 0
