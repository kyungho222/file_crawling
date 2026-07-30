from __future__ import annotations

from typing import Iterable, FrozenSet


DUPLICATE_REPAIR_FEATURE_CATEGORY = "category"
DUPLICATE_REPAIR_FEATURE_TITLE = "title"
DUPLICATE_REPAIR_FEATURE_EXPLORATION = "exploration"
DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS = "parsed_fields"
DUPLICATE_REPAIR_FEATURE_SUMMARY = "summary"

ALL_DUPLICATE_REPAIR_FEATURES = (
    DUPLICATE_REPAIR_FEATURE_CATEGORY,
    DUPLICATE_REPAIR_FEATURE_TITLE,
    DUPLICATE_REPAIR_FEATURE_EXPLORATION,
    DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    DUPLICATE_REPAIR_FEATURE_SUMMARY,
)

DEFAULT_DUPLICATE_REPAIR_FEATURES = ()

DEFAULT_IMMEDIATE_DUPLICATE_REPAIR_FEATURES = ()

_DUPLICATE_REPAIR_FEATURE_ALIASES = {
    "all": "__all__",
    "default": "__all__",
    "off": "__none__",
    "none": "__none__",
    "no": "__none__",
    "false": "__none__",
    "disabled": "__none__",
    "disable": "__none__",
    "cate": DUPLICATE_REPAIR_FEATURE_CATEGORY,
    "category": DUPLICATE_REPAIR_FEATURE_CATEGORY,
    "categories": DUPLICATE_REPAIR_FEATURE_CATEGORY,
    "title": DUPLICATE_REPAIR_FEATURE_TITLE,
    "subject": DUPLICATE_REPAIR_FEATURE_TITLE,
    "web_title": DUPLICATE_REPAIR_FEATURE_TITLE,
    "exploration": DUPLICATE_REPAIR_FEATURE_EXPLORATION,
    "explore": DUPLICATE_REPAIR_FEATURE_EXPLORATION,
    "parsed_fields": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "parsed_field": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "author": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "content_author": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "date": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "reg_date": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "content_created_at": DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS,
    "summary": DUPLICATE_REPAIR_FEATURE_SUMMARY,
    "memo1": DUPLICATE_REPAIR_FEATURE_SUMMARY,
}


def normalize_duplicate_repair_features(
    features: Iterable[str] | None,
) -> FrozenSet[str]:
    """
    중복 보정 기능 선택값을 정규화한다.

    - `None`: 기본값으로 전체 기능 사용
    - `"all"`, `"default"` 포함: 전체 기능 사용
    - alias 허용: `author -> parsed_fields`, `memo1 -> summary`
    - 알 수 없는 값은 무시
    """
    if features is None:
        return frozenset(DEFAULT_DUPLICATE_REPAIR_FEATURES)

    normalized = set()
    for raw_feature in features:
        key = str(raw_feature or "").strip().lower()
        if not key:
            continue
        mapped = _DUPLICATE_REPAIR_FEATURE_ALIASES.get(key)
        if mapped == "__all__":
            return frozenset(ALL_DUPLICATE_REPAIR_FEATURES)
        if mapped == "__none__":
            return frozenset()
        if mapped:
            normalized.add(mapped)
    return frozenset(normalized)


def is_duplicate_repair_feature_enabled(
    enabled_features: Iterable[str] | None,
    feature: str,
) -> bool:
    return str(feature or "").strip() in normalize_duplicate_repair_features(enabled_features)


def normalize_immediate_duplicate_repair_features(
    features: Iterable[str] | None,
) -> FrozenSet[str]:
    if features is None:
        return frozenset(DEFAULT_IMMEDIATE_DUPLICATE_REPAIR_FEATURES)
    return normalize_duplicate_repair_features(features)


__all__ = [
    "ALL_DUPLICATE_REPAIR_FEATURES",
    "DEFAULT_DUPLICATE_REPAIR_FEATURES",
    "DEFAULT_IMMEDIATE_DUPLICATE_REPAIR_FEATURES",
    "DUPLICATE_REPAIR_FEATURE_CATEGORY",
    "DUPLICATE_REPAIR_FEATURE_TITLE",
    "DUPLICATE_REPAIR_FEATURE_EXPLORATION",
    "DUPLICATE_REPAIR_FEATURE_PARSED_FIELDS",
    "DUPLICATE_REPAIR_FEATURE_SUMMARY",
    "normalize_duplicate_repair_features",
    "normalize_immediate_duplicate_repair_features",
    "is_duplicate_repair_feature_enabled",
]
