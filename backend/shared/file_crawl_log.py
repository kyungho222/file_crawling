"""Append-only JSONL logs for attachment file crawling.

The log is intentionally independent from Python logging handlers so each crawl
leaves a durable audit trail under the project-level ``download/`` directory.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

_WRITE_LOCK = threading.Lock()
_MAX_TEXT = 2000


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def file_crawl_log_dir() -> Path:
    raw = str(os.getenv("FILE_CRAWL_LOG_DIR", "") or "").strip()
    return Path(raw).expanduser().resolve() if raw else (_project_root() / "download")


def file_crawl_log_path(job_id: Optional[Any] = None) -> Path:
    date_part = datetime.now().strftime("%Y%m%d")
    jid = str(job_id or "").strip()
    if jid:
        safe_job = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in jid)[:80]
        return file_crawl_log_dir() / f"file_crawl_{date_part}_{safe_job}.jsonl"
    return file_crawl_log_dir() / f"file_crawl_{date_part}.jsonl"


def _short(value: Any, limit: int = _MAX_TEXT) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


def _sanitize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize(v) for v in value]
    return _short(value)


def append_file_crawl_log(
    event: str,
    *,
    job_id: Optional[Any] = None,
    url: Optional[Any] = None,
    filename: Optional[Any] = None,
    file_info: Optional[Mapping[str, Any]] = None,
    progress: Optional[Mapping[str, Any]] = None,
    bottleneck: Optional[Mapping[str, Any]] = None,
    error: Optional[Any] = None,
    result: Optional[Any] = None,
    **extra: Any,
) -> Optional[str]:
    """Append one JSON line and return the written path, best-effort."""
    try:
        path = file_crawl_log_path(job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds"),
            "event": str(event or "unknown"),
            "job_id": _short(job_id, 200),
            "url": _short(url, 2000),
            "filename": _short(filename, 500),
            "file_info": _sanitize(file_info or {}),
            "progress": _sanitize(progress or {}),
            "bottleneck": _sanitize(bottleneck or {}),
            "error_message": _short(error, 2000),
            "result": _sanitize(result),
        }
        for key, value in extra.items():
            record[str(key)] = _sanitize(value)
        line = json.dumps(record, ensure_ascii=False, default=str)
        with _WRITE_LOCK:
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        return str(path)
    except Exception:
        return None
