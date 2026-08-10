"""Shared SQL condition builder for exploration URL reads.

This module centralizes the stable parts of ASADAL_CRAWLING_EXPLORATION
filtering so count/select/stream paths can converge without changing query
execution behavior all at once.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from backend.shared.url_scope import build_sql_scope_condition, normalize_scope_path_prefix


EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"


def sql_single_quoted_literal(value: str) -> str:
    """Escape a value for existing string-composed SQL conditions."""

    return str(value).replace("\\", "\\\\").replace("'", "''")


@dataclass(frozen=True)
class ExplorationQuerySpec:
    chat_bot_id: Optional[str] = None
    target_domains: List[str] = field(default_factory=list)
    path_prefix: str = ""
    include_empty_type: bool = False
    dedupe_urls: bool = True
    require_active: bool = True
    date_condition: str = ""
    extra_condition: str = ""


@dataclass(frozen=True)
class ExplorationQueryConditions:
    table_name: str
    condition: str
    legacy_condition: str
    base_condition: str
    legacy_base_condition: str
    scope_condition: str
    path_prefix: str


def build_exploration_conditions(spec: ExplorationQuerySpec) -> ExplorationQueryConditions:
    """Build current and legacy WHERE conditions for exploration URL queries."""

    base_type_condition = (
        "(`type` = 'post' OR `type` IS NULL OR `type` = '')"
        if spec.include_empty_type
        else "`type` = 'post'"
    )
    legacy_base_condition = (
        base_type_condition
        + " AND (`study_status` IS NULL OR `study_status` <> 'delete')"
    )

    chat_bot_id = str(spec.chat_bot_id or "").strip()
    if chat_bot_id:
        legacy_base_condition += (
            f" AND chat_bot_id = '{sql_single_quoted_literal(chat_bot_id)}'"
        )

    base_condition = legacy_base_condition
    if spec.dedupe_urls:
        base_condition += " AND (`merge_status` IS NULL OR `merge_status` <> 'duplicate')"
    if spec.require_active:
        # NULL is excluded by the old COALESCE expression as well, so this
        # direct predicate keeps the same result while remaining index-friendly.
        base_condition += " AND is_active = 1"

    date_condition = str(spec.date_condition or "").strip()
    if date_condition:
        base_condition += f" AND ({date_condition})"
        legacy_base_condition += f" AND ({date_condition})"

    normalized_path_prefix = normalize_scope_path_prefix(spec.path_prefix)
    scope_condition = build_sql_scope_condition(
        "url",
        list(spec.target_domains or []),
        path_prefix=normalized_path_prefix,
    )

    condition = base_condition
    legacy_condition = legacy_base_condition
    if scope_condition:
        condition += f" AND {scope_condition}"
        legacy_condition += f" AND {scope_condition}"

    extra_condition = str(spec.extra_condition or "").strip()
    if extra_condition:
        condition += f" AND {extra_condition}"
        legacy_condition += f" AND {extra_condition}"

    return ExplorationQueryConditions(
        table_name=EXPLORATION_TABLE,
        condition=condition,
        legacy_condition=legacy_condition,
        base_condition=base_condition,
        legacy_base_condition=legacy_base_condition,
        scope_condition=scope_condition,
        path_prefix=normalized_path_prefix,
    )
