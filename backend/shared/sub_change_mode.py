from __future__ import annotations

from typing import Any, Dict


SUB_CHANGE_ON = "on"
SUB_CHANGE_OFF = "off"


def normalize_sub_change_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    return SUB_CHANGE_ON if mode == SUB_CHANGE_ON else SUB_CHANGE_OFF


def is_sub_change_enabled(value: Any) -> bool:
    return normalize_sub_change_mode(value) == SUB_CHANGE_ON


def _partial_fields(data: Dict[str, Any]) -> set[str]:
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list):
        return set()
    return {str(item or "").strip().lower() for item in fields if str(item or "").strip()}


def partial_update_fields_without_title(data: Dict[str, Any]) -> list[str]:
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list):
        return []
    out: list[str] = []
    for item in fields:
        value = str(item or "").strip().lower()
        if value and value != "title" and value not in out:
            out.append(value)
    return out


def is_partial_title_only_request(data: Dict[str, Any]) -> bool:
    if not is_partial_title_change_request(data):
        return False
    fields = _partial_fields(data)
    return fields == {"title"}


def is_partial_title_change_request(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    return (
        str(data.get("colle") or "").strip().lower() == "content"
        and "title" in _partial_fields(data)
        and data.get("partial_target_filter") is not None
    )


async def get_sub_change_mode_from_config(
    chat_bot_id: str | None,
    dbname: str = "chatty",
) -> str:
    from db.crawl_db_manager import get_config_values_by_keys

    values = await get_config_values_by_keys(
        chat_bot_id,
        ["sub_change"],
        dbname=dbname,
        match_chat_bot_id=True,
    )
    return normalize_sub_change_mode(values.get("sub_change"))


async def partial_title_change_enabled(data: Dict[str, Any], *, dbname: str) -> bool:
    if not is_partial_title_change_request(data):
        return False
    chat_bot_id = str((data or {}).get("chat_bot_id") or "").strip()
    if not chat_bot_id:
        return False
    return is_sub_change_enabled(await get_sub_change_mode_from_config(chat_bot_id, dbname=dbname))
