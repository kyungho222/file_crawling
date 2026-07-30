from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


DEFAULT_STORAGE_DIR = Path("tools/local_file_crawler/storage/json")
DEFAULT_INPUT = DEFAULT_STORAGE_DIR / "gm_attachment_result.json"
DEFAULT_OUTPUT = DEFAULT_STORAGE_DIR / "gm_delete_candidate_groups.json"


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _read_attachment_result(path: Path) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    detail_rows = payload.get("details") or payload.get("new_details") or []
    rows: List[Dict[str, Any]] = []
    for detail_index, detail in enumerate(detail_rows):
        if not isinstance(detail, dict):
            continue
        detail_url = detail.get("detail_url") or detail.get("final_url") or ""
        files = detail.get("files") or []
        for file_index, file_item in enumerate(files):
            if not isinstance(file_item, dict):
                continue
            row = dict(file_item)
            row.setdefault("detail_url", detail_url)
            row.setdefault("title", detail.get("title") or "")
            row.setdefault("gm_index", detail.get("gm_index", detail_index))
            row.setdefault("detail_file_index", file_index)
            rows.append(row)
    return rows


def _read_input_rows(path: Path) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        return _read_jsonl(path)
    rows = _read_attachment_result(path)
    if rows:
        return rows
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _normalize_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query = sorted((key.lower(), val) for key, val in query)
    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.lower(),
            "",
            urlencode(query, doseq=True),
            "",
        )
    )


def _normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[()\[\]{}<>\"'`~!@#$%^&*_+=|\\:;,.?/ -]+", "", text)
    return text


def _extract_detail_timestamp(row: Dict[str, Any]) -> str:
    for key in ("detail_url", "source_page", "url"):
        text = str(row.get(key) or "")
        match = re.search(r"(20\d{12,15})", text)
        if match:
            return match.group(1)
    return ""


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    digits = re.sub(r"\D+", "", text)
    for size, fmt in ((20, "%Y%m%d%H%M%S%f"), (17, "%Y%m%d%H%M%S%f"), (14, "%Y%m%d%H%M%S")):
        if len(digits) >= size:
            try:
                return datetime.strptime(digits[:size], fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _recent_sort_key(row: Dict[str, Any]) -> tuple:
    detail_ts = _extract_detail_timestamp(row)
    parsed_detail = _parse_datetime(detail_ts)
    queued_at = _parse_datetime(row.get("queued_at"))
    gm_index = row.get("gm_index")
    try:
        gm_index_int = int(gm_index)
    except Exception:
        gm_index_int = -1
    return (
        parsed_detail.timestamp() if parsed_detail else 0,
        queued_at.timestamp() if queued_at else 0,
        gm_index_int,
        str(row.get("detail_url") or ""),
        str(row.get("url") or ""),
    )


def _group_key(row: Dict[str, Any], mode: str) -> str:
    url_key = _normalize_url(row.get("url"))
    name_key = _normalize_name(row.get("name"))
    extension = str(row.get("extension") or "").strip().lower()
    if mode == "url":
        return f"url:{url_key}"
    if mode == "name":
        return f"name:{name_key}|ext:{extension}"
    return f"url:{url_key}|name:{name_key}|ext:{extension}"


def build_delete_candidate_groups(rows: Iterable[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = _group_key(row, mode)
        if not key or key in {"url:", "name:|ext:", "url:|name:|ext:"}:
            continue
        grouped.setdefault(key, []).append(row)

    groups: List[Dict[str, Any]] = []
    delete_candidate_count = 0
    for key, items in grouped.items():
        if len(items) < 2:
            continue
        ordered = sorted(items, key=_recent_sort_key, reverse=True)
        keep = ordered[0]
        delete_candidates = ordered[1:]
        delete_candidate_count += len(delete_candidates)
        groups.append(
            {
                "group_key": key,
                "duplicate_count": len(items),
                "keep_latest": keep,
                "delete_candidates": delete_candidates,
            }
        )

    groups.sort(key=lambda group: (group["duplicate_count"], len(group["delete_candidates"])), reverse=True)
    return {
        "ok": True,
        "stage": "delete_candidate_groups",
        "grouping_mode": mode,
        "source_count": len(list(rows)) if not isinstance(rows, list) else len(rows),
        "duplicate_group_count": len(groups),
        "delete_candidate_count": delete_candidate_count,
        "keep_count": len(groups),
        "groups": groups,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--mode", choices=("url", "name", "url_name"), default="url")
    args = parser.parse_args()

    rows = _read_input_rows(args.input)
    result = build_delete_candidate_groups(rows, args.mode)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": result["ok"],
                "source_count": result["source_count"],
                "duplicate_group_count": result["duplicate_group_count"],
                "delete_candidate_count": result["delete_candidate_count"],
                "output": str(args.output),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
