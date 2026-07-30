"""Shared progress/count normalization for crawl status payloads.

The crawler still has several producers of progress dictionaries. This module
keeps the frontend-facing count contract explicit while larger workflow files
are refactored in smaller steps.
"""

from __future__ import annotations

from typing import Any, Dict


TERMINAL_SSE_STATUSES = {"completed", "cancelled", "error"}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except Exception:
        return default


def is_file_mode_workflow(workflow: Any) -> bool:
    try:
        if bool(getattr(workflow, "is_attachment_file_crawl_workflow", False)):
            return True
    except Exception:
        pass
    for attr in ("file_mode", "ui_colle", "colle", "colle_mode"):
        try:
            value = getattr(workflow, attr, None)
            if isinstance(value, bool):
                if attr == "file_mode" and value:
                    return True
                continue
            if str(value or "").strip().lower() == "file":
                return True
        except Exception:
            continue
    return False


def resolve_effective_study_count(
    stats: Dict[str, Any],
    *,
    is_file_mode: bool = False,
    clamp_to_save: bool = True,
    zero_save_allows_study: bool = True,
) -> int:
    """Return the representative study count for UI and crawl-log payloads.

    Current board and file modes both prefer successful study count when it is
    present, then fall back to the generic study count. The optional clamp keeps
    displayed study progress from exceeding saved rows.
    """
    save = max(0, safe_int((stats or {}).get("save_count"), 0))
    study = max(
        0,
        safe_int(
            (stats or {}).get("study_success_count", (stats or {}).get("study_count", 0)),
            0,
        ),
    )
    if clamp_to_save and save > 0:
        return min(study, save)
    if clamp_to_save and save <= 0 and not zero_save_allows_study:
        return 0
    return study


def resolve_effective_study_count_for_workflow(
    stats: Dict[str, Any],
    *,
    workflow: Any = None,
    clamp_to_save: bool = True,
    zero_save_allows_study: bool = True,
) -> int:
    return resolve_effective_study_count(
        stats or {},
        is_file_mode=is_file_mode_workflow(workflow) if workflow is not None else False,
        clamp_to_save=clamp_to_save,
        zero_save_allows_study=zero_save_allows_study,
    )


def count_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload or {}
    scan = safe_int(payload.get("scan_count", payload.get("total_count", 0)), 0)
    total = safe_int(payload.get("total_count", payload.get("scan_count", scan)), scan)
    snapshot: Dict[str, Any] = {
        "scan_count": scan,
        "total_count": total,
        "collection_count": safe_int(payload.get("collection_count"), 0),
        "save_count": safe_int(payload.get("save_count"), 0),
        "study_count": safe_int(payload.get("study_count"), 0),
    }
    source = str(payload.get("source") or "").strip()
    if source:
        snapshot["source"] = source
    return snapshot


def dashboard_stats_from_payload(payload: Dict[str, Any]) -> Dict[str, int]:
    payload = payload or {}
    return {
        "scan": safe_int(payload.get("scan_count", payload.get("total_count", 0)), 0),
        "collection": safe_int(payload.get("collection_count"), 0),
        "save": safe_int(payload.get("save_count", payload.get("save_done_count", 0)), 0),
        "study": safe_int(payload.get("study_success_count", payload.get("study_count", 0)), 0),
        "pages": safe_int(payload.get("pages", payload.get("page_count", 0)), 0),
    }

