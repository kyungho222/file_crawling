"""Durable Stage-2 attachment results for operational file-crawl inspection.

The production workflow may be created before an operator knows its ``job_id``.
This small append-only snapshot lets the stage lab read already-completed detail
page extraction results without re-fetching the pages or relying on workflow
memory that is removed at job finalization.
"""

from __future__ import annotations

import copy
import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from backend.shared.file_crawl_log import file_crawl_log_dir

_WRITE_LOCK = threading.Lock()
_READ_LOCK = threading.Lock()
_READ_CACHE: Dict[str, Dict[str, Any]] = {}


def _safe_job_id(job_id: Any) -> str:
    value = str(job_id or "").strip()
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)[:80]


def file_crawl_attachment_snapshot_path(
    job_id: Any,
    *,
    directory: Optional[Path] = None,
) -> Path:
    """Return a stable per-job path, independent of the calendar date."""
    safe_job_id = _safe_job_id(job_id)
    if not safe_job_id:
        raise ValueError("job_id is required")
    root = Path(directory) if directory is not None else file_crawl_log_dir()
    return root / f"file_crawl_attachment_snapshot_{safe_job_id}.jsonl"


def _attachment_view(value: Any) -> Optional[Dict[str, str]]:
    if not isinstance(value, Mapping):
        return None
    name = str(value.get("name") or "").strip()
    url = str(value.get("url") or "").strip()
    if not name and not url:
        return None
    return {"name": name, "url": url}


def _result_view(value: Mapping[str, Any]) -> Dict[str, Any]:
    attachments: List[Dict[str, str]] = []
    for attachment in value.get("attachments") or []:
        normalized = _attachment_view(attachment)
        if normalized is not None:
            attachments.append(normalized)
    attachment_count = value.get("attachment_count")
    try:
        attachment_count = int(attachment_count)
    except (TypeError, ValueError):
        attachment_count = len(attachments)
    return {
        "url": str(value.get("url") or "").strip(),
        "attachment_count": max(0, attachment_count),
        "attachments": attachments,
    }


def _append(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(dict(record), ensure_ascii=False, separators=(",", ":"), default=str)
    with _WRITE_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def initialize_file_crawl_attachment_snapshot(
    job_id: Any,
    *,
    db_name: Any,
    chat_bot_id: Any,
    loaded_url_count: Any,
    directory: Optional[Path] = None,
) -> str:
    """Record the Stage-1 URL count before attachment extraction begins."""
    try:
        count = max(0, int(loaded_url_count or 0))
    except (TypeError, ValueError):
        count = 0
    path = file_crawl_attachment_snapshot_path(job_id, directory=directory)
    _append(path, {
        "event": "start",
        "job_id": str(job_id or "").strip(),
        "db_name": str(db_name or "").strip(),
        "chat_bot_id": str(chat_bot_id or "").strip(),
        "loaded_url_count": count,
    })
    return str(path)


def append_file_crawl_attachment_snapshot(
    job_id: Any,
    result: Mapping[str, Any],
    *,
    directory: Optional[Path] = None,
) -> str:
    """Persist one completed detail-page extraction result."""
    path = file_crawl_attachment_snapshot_path(job_id, directory=directory)
    _append(path, {
        "event": "detail_result",
        "job_id": str(job_id or "").strip(),
        "result": _result_view(result),
    })
    return str(path)


def _empty_snapshot() -> Dict[str, Any]:
    return {
        "available": False,
        "db_name": "",
        "chat_bot_id": "",
        "loaded_url_count": 0,
        "detail_results": [],
        "detail_visited_count": 0,
        "attachment_found_count": 0,
    }


def _apply_record(snapshot: Dict[str, Any], record: Mapping[str, Any]) -> None:
    event = str(record.get("event") or "").strip()
    if event == "start":
        snapshot["available"] = True
        snapshot["db_name"] = str(record.get("db_name") or snapshot["db_name"]).strip()
        snapshot["chat_bot_id"] = str(record.get("chat_bot_id") or snapshot["chat_bot_id"]).strip()
        try:
            snapshot["loaded_url_count"] = max(snapshot["loaded_url_count"], int(record.get("loaded_url_count") or 0))
        except (TypeError, ValueError):
            pass
        return
    if event != "detail_result" or not isinstance(record.get("result"), Mapping):
        return
    result = _result_view(record["result"])
    url = result["url"]
    if not url:
        return
    results_by_url = snapshot.setdefault("_results_by_url", {})
    order = snapshot.setdefault("_result_order", [])
    if url not in results_by_url:
        order.append(url)
    results_by_url[url] = result
    snapshot["available"] = True


def _finalize_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    order = snapshot.get("_result_order") or []
    results_by_url = snapshot.get("_results_by_url") or {}
    results = [results_by_url[url] for url in order if url in results_by_url]
    snapshot["detail_results"] = results
    snapshot["detail_visited_count"] = len(results)
    snapshot["attachment_found_count"] = sum(
        int(item.get("attachment_count", 0) or 0) for item in results
    )
    snapshot.pop("_results_by_url", None)
    snapshot.pop("_result_order", None)
    return snapshot


def read_file_crawl_attachment_snapshot(
    job_id: Any,
    *,
    directory: Optional[Path] = None,
) -> Dict[str, Any]:
    """Read a per-job snapshot incrementally, avoiding repeated full file scans."""
    try:
        path = file_crawl_attachment_snapshot_path(job_id, directory=directory)
    except ValueError:
        return _empty_snapshot()
    if not path.exists():
        return _empty_snapshot()

    cache_key = str(path.resolve())
    with _READ_LOCK:
        cache = _READ_CACHE.get(cache_key)
        current_size = path.stat().st_size
        if not isinstance(cache, dict) or current_size < int(cache.get("offset", 0) or 0):
            cache = {"offset": 0, "snapshot": _empty_snapshot()}
            _READ_CACHE[cache_key] = cache
        snapshot = cache["snapshot"]
        with path.open("rb") as handle:
            handle.seek(int(cache.get("offset", 0) or 0))
            while True:
                raw_line = handle.readline()
                if not raw_line:
                    break
                if not raw_line.endswith(b"\n"):
                    break
                next_offset = handle.tell()
                try:
                    record = json.loads(raw_line.decode("utf-8", errors="replace"))
                except (TypeError, ValueError):
                    cache["offset"] = next_offset
                    continue
                if isinstance(record, Mapping):
                    _apply_record(snapshot, record)
                cache["offset"] = next_offset
        rendered = _finalize_snapshot(copy.deepcopy(snapshot))
        return rendered
