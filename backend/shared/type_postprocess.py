from __future__ import annotations

import logging
from typing import Any, Dict, Set

logger = logging.getLogger("backend.shared.type_postprocess")


def _partial_update_fields(data: Dict[str, Any]) -> Set[str]:
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list):
        return set()
    return {str(item or "").strip().lower() for item in fields if str(item or "").strip()}


def is_type_postprocess_request(data: Dict[str, Any]) -> bool:
    mode = str((data or {}).get("crawl_mode") or "").strip().lower()
    if mode == "type_postprocess":
        return True
    raw_enabled = (data or {}).get("type_postprocess_enabled")
    if isinstance(raw_enabled, bool):
        return raw_enabled
    if str(raw_enabled or "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    fields = _partial_update_fields(data or {})
    return (
        str((data or {}).get("colle") or "").strip().lower() == "content"
        and "type" in fields
        and not bool(fields & {"title", "content", "cate", "symmary", "summary"})
    )


async def run_type_postprocess(data: Dict[str, Any]) -> Dict[str, Any]:
    """Retained API contract after disabling exploration-table mutations."""
    payload = data or {}
    result = {
        "status": "skipped",
        "event": "type_postprocess_skipped",
        "job_id": str(payload.get("job_id") or "").strip(),
        "account_name": str(payload.get("db_name") or payload.get("db") or "").strip(),
        "matched_count": 0,
        "updated_count": 0,
        "inserted_count": 0,
        "reason": "exploration_writes_disabled",
        "source": "type_postprocess",
        "message": "exploration 테이블 쓰기가 비활성화되어 type 후보정을 건너뜁니다.",
    }
    logger.info(
        "[TypePostprocess] skipped | job_id=%s db=%s reason=%s",
        result["job_id"],
        result["account_name"],
        result["reason"],
    )
    return result