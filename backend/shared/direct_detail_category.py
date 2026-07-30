from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple, Union

from backend.shared.crawl_shared import resolve_stream_matched_rules_only
from backend.shared.pre_explored_url import (
    _load_category_url_pattern_object,
    _merge_runtime_rule_from_input_url,
    resolve_cate_for_detail_url,
)
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.shared.direct_detail_category")


DirectDetailStartUrl = Union[str, Dict[str, str]]


def _clean_category_value(value: Any) -> str:
    try:
        text = str(value or "").strip()
    except Exception:
        return ""
    if text.lower() in {"undefined", "null", "none", "array"}:
        return ""
    return text


def _request_categories(data: Dict[str, Any]) -> Tuple[str, str]:
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    cate1 = data.get("cate1")
    cate2 = data.get("cate2")
    if cate1 is None:
        cate1 = meta.get("cate1")
    if cate2 is None:
        cate2 = meta.get("cate2")
    return _clean_category_value(cate1), _clean_category_value(cate2)


async def build_direct_detail_start_url_item(
    data: Dict[str, Any],
    direct_detail_url: str,
    *,
    db_name: Optional[str] = None,
    chat_bot_id: Optional[str] = None,
) -> DirectDetailStartUrl:
    """
    Direct-detail bypass still needs the same category payload shape as DB-streamed URLs.

    When CATEGORY url/query pattern mode is enabled, attach `cate_match|cate1|cate2`
    to the single start URL so downstream logs and duplicate/category repair see
    the classification before detail processing begins.
    """
    url = ensure_url_scheme(str(direct_detail_url or "").strip()) if direct_detail_url else ""
    if not url:
        return ""

    if not resolve_stream_matched_rules_only(data):
        return url

    req_cate1, req_cate2 = _request_categories(data)
    filters_obj = None
    if chat_bot_id and db_name:
        try:
            filters_obj = await _load_category_url_pattern_object(
                str(chat_bot_id),
                str(db_name),
                contents_url=url,
                require_nonempty_rules=False,
            )
        except Exception as exc:
            logger.debug(
                "[DirectDetailCategory] category rule load skipped | url=%s err=%s",
                url[:180],
                exc,
                exc_info=True,
            )

    try:
        filters_obj = _merge_runtime_rule_from_input_url(
            filters_obj,
            contents_url=url,
            cate1=req_cate1,
            cate2=req_cate2,
        )
        resolved = resolve_cate_for_detail_url(url, filters_obj) if filters_obj else None
    except Exception as exc:
        logger.debug(
            "[DirectDetailCategory] category rule resolve skipped | url=%s err=%s",
            url[:180],
            exc,
            exc_info=True,
        )
        resolved = None

    if not resolved:
        return url

    cate1, cate2 = _clean_category_value(resolved[0]), _clean_category_value(resolved[1])
    if not (cate1 or cate2):
        return url

    return {
        "url": url,
        "type": "post",
        "cate_match": f"cate_match|{cate1}|{cate2}",
    }
