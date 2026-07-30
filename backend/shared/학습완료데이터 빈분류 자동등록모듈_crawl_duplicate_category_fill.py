from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
import re
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional, Tuple

from backend.shared.sub_cate_mode import is_sub_cate_overwrite, merge_category_pair


logger = logging.getLogger("backend.shared.crawl_duplicate_category_fill")


def _first_non_empty_str(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _is_future_date_string(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", text)
    if not m:
        digits = re.sub(r"[^0-9]", "", text)
        if len(digits) >= 8:
            m = re.match(r"(\d{4})(\d{2})(\d{2})", digits)
    if not m:
        return False
    try:
        dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return False
    return dt.date() > datetime.now().date()


def coalesce_duplicate_categories(info: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    """
    중복 행 보정에 쓸 분류값을 한곳에서 모은다.
    - 상위 dict의 cate1/cate2
    - original_meta 내부의 cate1/cate2, store_*, assigned_*

    이 파일은 다른 크롤링 기능에 그대로 이식하기 위한 독립 모듈이다.
    """
    if not isinstance(info, dict):
        return ("", "")

    original_meta = info.get("original_meta")
    original_meta = original_meta if isinstance(original_meta, dict) else {}

    cate1 = _first_non_empty_str(
        info.get("cate1"),
        original_meta.get("cate1"),
        original_meta.get("store_cate1"),
        original_meta.get("assigned_cate1"),
    )
    cate2 = _first_non_empty_str(
        info.get("cate2"),
        original_meta.get("cate2"),
        original_meta.get("store_cate2"),
        original_meta.get("assigned_cate2"),
    )
    return (cate1, cate2)


def duplicate_row_categories_empty(
    existing_cate1: Any,
    existing_cate2: Any,
    has_cate2_column: bool,
) -> bool:
    """
    기존 행에 자동 보정이 필요한 빈 분류 칸이 있는지 판정한다.
    - cate2 컬럼이 있으면 cate1 또는 cate2 중 하나라도 비어 있으면 True
    - cate2 컬럼이 없으면 cate1만 비어 있으면 True
    """
    cate1 = str(existing_cate1 or "").strip()
    cate2 = str(existing_cate2 or "").strip()
    if has_cate2_column:
        return (not cate1) or (not cate2)
    return not cate1


@dataclass(frozen=True)
class DuplicateCategoryFillDecision:
    should_update: bool
    row_id: Any = None
    cate1: str = ""
    cate2: str = ""
    reason: str = ""


@dataclass(frozen=True)
class DuplicateParsedFieldFillDecision:
    should_update: bool
    row_id: Any = None
    values: Dict[str, str] | None = None
    reason: str = ""


def decide_duplicate_category_fill(
    *,
    existing_row: Optional[Mapping[str, Any]],
    incoming_meta: Optional[Dict[str, Any]],
    columns: Iterable[str],
    sub_cate_mode: str = "emp",
) -> DuplicateCategoryFillDecision:
    """
    크롤링 > 중복발견 > 분류값 여부 확인 > 비어있으면 자동 등록 / 비어있지 않으면 스킵

    기존 기능에 붙이기 전에, 이 순수 함수만 호출해서 보정 여부를 먼저 결정할 수 있다.
    """
    cols = set(columns or [])
    if not existing_row:
        return DuplicateCategoryFillDecision(False, reason="missing_row")
    if "cate1" not in cols:
        return DuplicateCategoryFillDecision(False, reason="missing_cate1_column")

    row_id = existing_row.get("id")
    if row_id is None:
        return DuplicateCategoryFillDecision(False, reason="missing_row_id")

    has_cate2 = "cate2" in cols
    overwrite_mode = is_sub_cate_overwrite(sub_cate_mode)
    existing_cate1 = str(existing_row.get("cate1") or "").strip()
    existing_cate2 = str(existing_row.get("cate2") or "").strip() if has_cate2 else ""
    new_cate1, new_cate2 = coalesce_duplicate_categories(incoming_meta)
    if not (new_cate1 or new_cate2):
        logger.info(
            "[AutoCategoryDebug][Decision] incoming categories empty | row_id=%s existing=(%r,%r) columns=%s",
            row_id,
            existing_cate1,
            existing_cate2,
            sorted(cols),
        )
        return DuplicateCategoryFillDecision(
            False,
            row_id=row_id,
            reason="incoming_categories_empty",
        )

    replace_primary_only_category = (
        overwrite_mode
        and
        has_cate2
        and bool(existing_cate1)
        and not bool(existing_cate2)
        and bool(new_cate1)
        and existing_cate1 != new_cate1
    )

    target_cate1, target_cate2 = merge_category_pair(
        sub_cate_mode,
        existing_cate1,
        existing_cate2,
        new_cate1,
        new_cate2,
        has_cate2=has_cate2,
    )

    if not duplicate_row_categories_empty(existing_cate1, existing_cate2, has_cate2):
        if target_cate1 != existing_cate1 or target_cate2 != existing_cate2:
            logger.info(
                "[AutoCategoryDebug][Decision] existing categories differ; replace with incoming | row_id=%s existing=(%r,%r) incoming=(%r,%r) target=(%r,%r) has_cate2=%s",
                row_id,
                existing_cate1,
                existing_cate2,
                new_cate1,
                new_cate2,
                target_cate1,
                target_cate2,
                has_cate2,
            )
            return DuplicateCategoryFillDecision(
                True,
                row_id=row_id,
                cate1=target_cate1,
                cate2=target_cate2,
                reason="replace_different_categories" if overwrite_mode else "fill_empty_only",
            )
        logger.info(
            "[AutoCategoryDebug][Decision] existing categories already block update | row_id=%s existing=(%r,%r) incoming=(%r,%r) has_cate2=%s",
            row_id,
            existing_cate1,
            existing_cate2,
            new_cate1,
            new_cate2,
            has_cate2,
        )
        return DuplicateCategoryFillDecision(
            False,
            row_id=row_id,
            cate1=target_cate1,
            cate2=target_cate2,
            reason="existing_categories_present",
        )

    if target_cate1 == existing_cate1 and target_cate2 == existing_cate2:
        return DuplicateCategoryFillDecision(
            False,
            row_id=row_id,
            cate1=target_cate1,
            cate2=target_cate2,
            reason="incoming_categories_missing_for_empty_fields",
        )

    return DuplicateCategoryFillDecision(
        True,
        row_id=row_id,
        cate1=target_cate1,
        cate2=target_cate2,
        reason=(
            "replace_primary_only_category"
            if replace_primary_only_category
            else "apply_missing_categories"
        ),
    )


def decide_duplicate_parsed_field_fill(
    *,
    existing_row: Optional[Mapping[str, Any]],
    incoming_values: Optional[Mapping[str, Any]] = None,
    incoming_meta: Optional[Dict[str, Any]] = None,
    columns: Iterable[str],
) -> DuplicateParsedFieldFillDecision:
    """
    중복 행의 파싱 기준값(content_author 등)을 신규 파싱값으로 보강하거나 교체한다.

    - existing_row: 현재 DB에 있는 중복 행
    - incoming_values: 이미 정규화된 신규 파싱값 딕셔너리
    - incoming_meta: 하위 호환용 별칭. incoming_values가 없을 때 사용
    - columns: 실제 테이블 컬럼 목록
    """
    cols = set(columns or [])
    values_src = incoming_values if isinstance(incoming_values, Mapping) else incoming_meta
    if not existing_row:
        return DuplicateParsedFieldFillDecision(False, reason="missing_row")

    row_id = existing_row.get("id")
    if row_id is None:
        return DuplicateParsedFieldFillDecision(False, reason="missing_row_id")

    if not isinstance(values_src, Mapping):
        return DuplicateParsedFieldFillDecision(
            False,
            row_id=row_id,
            reason="incoming_values_missing",
        )

    normalized_values: Dict[str, str] = {}
    alias_groups = {
        "content_author": ("content_author", "author", "writer"),
        "content_created_at": ("content_created_at", "file_created_at", "reg_date"),
        "content_updated_at": ("content_updated_at", "modified_at", "updated_at"),
    }
    for target_column, aliases in alias_groups.items():
        for alias in aliases:
            if target_column == "content_updated_at" and alias in values_src:
                normalized_values[target_column] = str(values_src.get(alias) or "").strip()
                break
            raw_value = values_src.get(alias)
            value = str(raw_value or "").strip()
            if value:
                normalized_values[target_column] = value
                break

    update_values: Dict[str, str] = {}
    had_non_empty_existing = False
    had_missing_target_column = False
    had_incoming_value = False
    for column, raw_value in normalized_values.items():
        if column not in cols:
            had_missing_target_column = True
            continue
        value = str(raw_value or "").strip()
        if not value and column != "content_updated_at":
            continue
        if column == "content_created_at" and _is_future_date_string(value):
            logger.info(
                "[DuplicateRepair][ParsedFields] future content_created_at skipped | row_id=%s value=%r",
                row_id,
                value,
            )
            continue
        if column == "content_updated_at" and value and _is_future_date_string(value):
            logger.info(
                "[DuplicateRepair][ParsedFields] future content_updated_at skipped | row_id=%s value=%r",
                row_id,
                value,
            )
            continue
        had_incoming_value = True
        existing_value = str(existing_row.get(column) or "").strip()
        if column == "content_updated_at":
            if existing_value != value:
                update_values[column] = value
            continue
        if existing_value:
            had_non_empty_existing = True
            continue
        update_values[column] = value

    if update_values:
        return DuplicateParsedFieldFillDecision(
            True,
            row_id=row_id,
            values=update_values,
            reason="fill_empty_parsed_fields",
        )

    return DuplicateParsedFieldFillDecision(
        False,
        row_id=row_id,
        values={},
        reason=(
            "existing_parsed_fields_present"
            if had_non_empty_existing
            else "target_columns_missing"
            if had_missing_target_column
            else "incoming_parsed_fields_empty"
            if not had_incoming_value
            else "incoming_parsed_fields_empty"
        ),
    )


async def apply_duplicate_category_fill(
    *,
    db_name: str,
    table_name: str,
    existing_row: Optional[Mapping[str, Any]],
    incoming_meta: Optional[Dict[str, Any]],
    columns: Iterable[str],
    execute_query: Callable[..., Awaitable[Any]],
    sub_cate_mode: str = "emp",
) -> DuplicateCategoryFillDecision:
    """
    다른 크롤링 기능에서 그대로 가져다 쓸 수 있는 DB 반영용 래퍼.
    execute_query 에는 mysql_execute_query 같은 비동기 실행 함수를 넘긴다.
    """
    cols = set(columns or [])
    row_id = existing_row.get("id") if isinstance(existing_row, Mapping) else None
    existing_cate1 = str((existing_row or {}).get("cate1") or "").strip() if isinstance(existing_row, Mapping) else ""
    existing_cate2 = str((existing_row or {}).get("cate2") or "").strip() if isinstance(existing_row, Mapping) else ""
    incoming_cate1 = str((incoming_meta or {}).get("cate1") or "").strip()
    incoming_cate2 = str((incoming_meta or {}).get("cate2") or "").strip()
    logger.info(
        "[DuplicateRepair][Category] apply request | db=%s table=%s row_id=%s existing=(%r,%r) incoming=(%r,%r) sub_cate_mode=%s has_cate2=%s columns=%s",
        db_name,
        table_name,
        row_id,
        existing_cate1,
        existing_cate2,
        incoming_cate1,
        incoming_cate2,
        sub_cate_mode,
        "cate2" in cols,
        sorted(cols),
    )

    decision = decide_duplicate_category_fill(
        existing_row=existing_row,
        incoming_meta=incoming_meta,
        columns=columns,
        sub_cate_mode=sub_cate_mode,
    )
    if not decision.should_update:
        logger.info(
            "[DuplicateRepair][Category] UPDATE skip | db=%s table=%s row_id=%s reason=%s existing=(%r,%r) incoming=(%r,%r)",
            db_name,
            table_name,
            decision.row_id,
            decision.reason,
            existing_cate1,
            existing_cate2,
            incoming_cate1,
            incoming_cate2,
        )
        return decision

    try:
        if "cate2" in cols:
            logger.info(
                "[DuplicateRepair][Category] UPDATE attempt | db=%s table=%s row_id=%s cate1=%r cate2=%r",
                db_name,
                table_name,
                decision.row_id,
                decision.cate1,
                decision.cate2,
            )
            await execute_query(
                f"UPDATE `{table_name}` SET cate1 = %s, cate2 = %s WHERE id = %s",
                (decision.cate1, decision.cate2, decision.row_id),
                dbname=db_name,
            )
        else:
            logger.info(
                "[DuplicateRepair][Category] UPDATE attempt | db=%s table=%s row_id=%s cate1=%r",
                db_name,
                table_name,
                decision.row_id,
                decision.cate1,
            )
            await execute_query(
                f"UPDATE `{table_name}` SET cate1 = %s WHERE id = %s",
                (decision.cate1, decision.row_id),
                dbname=db_name,
            )
    except Exception as exc:
        logger.warning(
            "[DuplicateRepair][Category] UPDATE failed | db=%s table=%s row_id=%s reason=%s err=%s",
            db_name,
            table_name,
            decision.row_id,
            decision.reason,
            exc,
        )
        raise

    logger.info(
        "[DuplicateRepair][Category] UPDATE done | db=%s table=%s row_id=%s cate1=%r cate2=%r",
        db_name,
        table_name,
        decision.row_id,
        decision.cate1,
        decision.cate2,
    )
    return decision


async def apply_duplicate_parsed_field_fill(
    *,
    db_name: str,
    table_name: str,
    existing_row: Optional[Mapping[str, Any]],
    incoming_values: Optional[Mapping[str, Any]] = None,
    incoming_meta: Optional[Dict[str, Any]] = None,
    columns: Iterable[str],
    execute_query: Callable[..., Awaitable[Any]],
) -> DuplicateParsedFieldFillDecision:
    """
    다른 모듈에서도 재사용할 수 있는 중복 행 파싱 기준값 보강용 DB 반영 헬퍼.
    """
    decision = decide_duplicate_parsed_field_fill(
        existing_row=existing_row,
        incoming_values=incoming_values,
        incoming_meta=incoming_meta,
        columns=columns,
    )
    if not decision.should_update or not decision.values:
        return decision

    set_parts = [f"`{column}` = %s" for column in decision.values]
    params = [decision.values[column] for column in decision.values]
    params.append(decision.row_id)
    await execute_query(
        f"UPDATE `{table_name}` SET {', '.join(set_parts)} WHERE id = %s",
        tuple(params),
        dbname=db_name,
    )
    return decision
