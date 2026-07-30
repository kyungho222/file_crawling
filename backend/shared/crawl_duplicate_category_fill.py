from __future__ import annotations

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


_IMPL_PATH = Path(__file__).with_name(
    "학습완료데이터 빈분류 자동등록모듈_crawl_duplicate_category_fill.py"
)
_SPEC = spec_from_file_location("backend.shared._crawl_duplicate_category_fill_impl", _IMPL_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Failed to load duplicate category fill module: {_IMPL_PATH}")

_MODULE = module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)

DuplicateCategoryFillDecision = _MODULE.DuplicateCategoryFillDecision
DuplicateParsedFieldFillDecision = _MODULE.DuplicateParsedFieldFillDecision
coalesce_duplicate_categories = _MODULE.coalesce_duplicate_categories
duplicate_row_categories_empty = _MODULE.duplicate_row_categories_empty
decide_duplicate_category_fill = _MODULE.decide_duplicate_category_fill
apply_duplicate_category_fill = _MODULE.apply_duplicate_category_fill
decide_duplicate_parsed_field_fill = _MODULE.decide_duplicate_parsed_field_fill


async def apply_duplicate_parsed_field_fill(
    *,
    db_name,
    table_name,
    existing_row,
    incoming_values=None,
    incoming_meta=None,
    columns,
    execute_query,
):
    """Apply parsed field repair updates; the decision function only fills blanks."""
    return await _MODULE.apply_duplicate_parsed_field_fill(
        db_name=db_name,
        table_name=table_name,
        existing_row=existing_row,
        incoming_values=incoming_values,
        incoming_meta=incoming_meta,
        columns=columns,
        execute_query=execute_query,
    )

__all__ = [
    "DuplicateCategoryFillDecision",
    "DuplicateParsedFieldFillDecision",
    "coalesce_duplicate_categories",
    "duplicate_row_categories_empty",
    "decide_duplicate_category_fill",
    "apply_duplicate_category_fill",
    "decide_duplicate_parsed_field_fill",
    "apply_duplicate_parsed_field_fill",
]
