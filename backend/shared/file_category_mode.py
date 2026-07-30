from typing import Any, Dict


def _bool_from_payload(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "y", "yes", "on"}
    return False


def is_file_crawl_request(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    try:
        colle = str(data.get("colle") or "").strip().lower()
    except Exception:
        colle = ""
    try:
        content_type = str(data.get("content_type") or "").strip().lower()
    except Exception:
        content_type = ""
    return colle == "file" or content_type in {"file", "attach", "attachment"}


def file_category_mode(data: Dict[str, Any]) -> str:
    if not isinstance(data, dict):
        return "crawl"
    raw = (
        data.get("file_category_mode")
        or data.get("fileCategoryMode")
        or data.get("file_category_update_mode")
        or data.get("fileCategoryUpdateMode")
        or ""
    )
    mode = str(raw or "").strip().lower()
    if mode in {"category_only", "category", "sync_only", "update_only", "category_update_only"}:
        return "category_only"
    return "crawl"


def is_file_category_update_only_request(data: Dict[str, Any]) -> bool:
    if not is_file_crawl_request(data):
        return False
    if file_category_mode(data) == "category_only":
        return True
    for key in (
        "file_category_update_only",
        "fileCategoryUpdateOnly",
        "file_category_sync_only",
        "fileCategorySyncOnly",
    ):
        if _bool_from_payload(data.get(key)):
            return True
    return False

