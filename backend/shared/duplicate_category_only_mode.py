from __future__ import annotations

import os
from typing import Mapping, Optional


TRUE_VALUES = {"1", "true", "yes", "on", "y", "enable", "enabled"}
FALSE_VALUES = {"0", "false", "no", "off", "n", "disable", "disabled", "none"}
REQUEST_MODE_DEFAULT = "off"
REQUEST_MODE_ALIASES = {
    "": REQUEST_MODE_DEFAULT,
    "default": REQUEST_MODE_DEFAULT,
    "auto": REQUEST_MODE_DEFAULT,
    "0": "off",
    "false": "off",
    "no": "off",
    "none": "off",
    "disable": "off",
    "disabled": "off",
    "off": "off",
    "1": "on",
    "true": "on",
    "yes": "on",
    "enable": "on",
    "enabled": "on",
    "on": "on",
    "category": "category",
    "cate": "category",
    "classification": "category",
    "auto_category": "category",
    "parsed_fields": "parsed_fields",
    "parsed_field": "parsed_fields",
    "author": "parsed_fields",
    "metadata": "parsed_fields",
    "content_metadata": "parsed_fields",
    "category_parsed_fields": "category_parsed_fields",
    "category+parsed_fields": "category_parsed_fields",
    "category,parsed_fields": "category_parsed_fields",
    "cate_parsed_fields": "category_parsed_fields",
    "category_metadata": "category_parsed_fields",
}


PURE_CRAWLING_VALUES = {
    "BOARD_AUTO_CATEGORY": "1",
    "CATEGORY_RULE_DEBUG": "0",
    "BOARD_SELECTOR_LEARNING": "1",
    "BOARD_CONTENT_ENABLE_POST_JOB_CATE_UPDATE": "1",
    "BOARD_DUPLICATE_REPAIR_FEATURES": "off",
    "BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES": "off",
    "BOARD_DUPLICATE_REPAIR_SOURCES": "exploration",
    "BOARD_DUPLICATE_REPAIR_ENABLE_TITLE": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_AUTHOR": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_PARSED_FIELDS": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_IGNORE_PERIOD": "0",
}

DUPLICATE_CATEGORY_ONLY_VALUES = {
    "BOARD_AUTO_CATEGORY": "1",
    "BOARD_SELECTOR_LEARNING": "0",
    "BOARD_CONTENT_ENABLE_POST_JOB_CATE_UPDATE": "0",
    "BOARD_DUPLICATE_REPAIR_FEATURES": "category",
    "BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES": "category",
    "BOARD_DUPLICATE_REPAIR_SOURCES": "learn_list",
    "BOARD_DUPLICATE_REPAIR_ENABLE_TITLE": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_AUTHOR": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_PARSED_FIELDS": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES": "1",
    "BOARD_DUPLICATE_CATEGORY_ONLY_IGNORE_PERIOD": "1",
}

BROAD_DUPLICATE_REPAIR_VALUES = {
    "BOARD_AUTO_CATEGORY": "1",
    "BOARD_SELECTOR_LEARNING": "1",
    "BOARD_CONTENT_ENABLE_POST_JOB_CATE_UPDATE": "1",
    "BOARD_DUPLICATE_REPAIR_FEATURES": "exploration,category,parsed_fields,title",
    "BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES": "exploration,category,parsed_fields,title",
    "BOARD_DUPLICATE_REPAIR_SOURCES": "exploration,learn_list",
    "BOARD_DUPLICATE_REPAIR_ENABLE_TITLE": "1",
    "BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE": "1",
    "BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_AUTHOR": "1",
    "BOARD_DUPLICATE_REPAIR_ENABLE_PARSED_FIELDS": "1",
    "BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_IGNORE_PERIOD": "0",
}

BROAD_DUPLICATE_REPAIR_SWITCH_VALUES = {
    "BOARD_DUPLICATE_REPAIR_FEATURES": "category,parsed_fields,title",
    "BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES": "category,parsed_fields,title",
    "BOARD_DUPLICATE_REPAIR_SOURCES": "exploration,learn_list",
    "BOARD_DUPLICATE_REPAIR_ENABLE_TITLE": "1",
    "BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE": "1",
    "BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_AUTHOR": "1",
    "BOARD_DUPLICATE_REPAIR_ENABLE_PARSED_FIELDS": "1",
    "BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_IGNORE_PERIOD": "0",
}

DUPLICATE_REPAIR_OFF_VALUES = {
    "BOARD_DUPLICATE_REPAIR_FEATURES": "off",
    "BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES": "off",
    "BOARD_DUPLICATE_REPAIR_SOURCES": "exploration",
    "BOARD_DUPLICATE_REPAIR_ENABLE_TITLE": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_AUTHOR": "0",
    "BOARD_DUPLICATE_REPAIR_ENABLE_PARSED_FIELDS": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES": "0",
    "BOARD_DUPLICATE_CATEGORY_ONLY_IGNORE_PERIOD": "0",
}


def env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default) or default).strip().lower() in TRUE_VALUES


def apply_env_values(values: Mapping[str, str]) -> None:
    for key, value in values.items():
        os.environ[key] = value


def board_feature_preset_values(raw_preset: Optional[str]) -> Optional[Mapping[str, str]]:
    preset = str(raw_preset or "").strip().lower()
    if preset in {"pure_crawling", "crawl_only", "pure"}:
        return PURE_CRAWLING_VALUES
    if preset in {"auto_classification_only", "auto_category_only"}:
        return DUPLICATE_CATEGORY_ONLY_VALUES
    if preset == "duplicate_repair_on":
        return BROAD_DUPLICATE_REPAIR_VALUES
    return None


def duplicate_repair_switch_values(raw_value: Optional[str]) -> tuple[Optional[str], Optional[Mapping[str, str]]]:
    value = str(raw_value or "").strip().lower()
    if value in TRUE_VALUES:
        return "on", BROAD_DUPLICATE_REPAIR_SWITCH_VALUES
    if value in {"category", "cate", "classification", "auto_category"}:
        return "category", DUPLICATE_CATEGORY_ONLY_VALUES
    if value in FALSE_VALUES:
        return "off", DUPLICATE_REPAIR_OFF_VALUES
    return None, None


def duplicate_category_only_enabled() -> bool:
    features = str(os.getenv("BOARD_DUPLICATE_REPAIR_FEATURES", "") or "").strip().lower()
    immediate = str(os.getenv("BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES", "") or "").strip().lower()
    sources = str(os.getenv("BOARD_DUPLICATE_REPAIR_SOURCES", "") or "").strip().lower()
    return (
        features == "category"
        and immediate == "category"
        and sources == "learn_list"
        and env_flag("BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES")
    )


def normalize_duplicate_repair_request_mode(raw_value: Optional[str], default: str = REQUEST_MODE_DEFAULT) -> str:
    value = str(raw_value if raw_value is not None else default or "").strip().lower()
    if value in REQUEST_MODE_ALIASES:
        return REQUEST_MODE_ALIASES[value]
    return REQUEST_MODE_ALIASES.get(str(default or "").strip().lower(), REQUEST_MODE_DEFAULT)


def skip_non_duplicate_crawling_enabled() -> bool:
    return env_flag("BOARD_DUPLICATE_CATEGORY_ONLY_SKIP_NON_DUPLICATES")


def ignore_period_enabled() -> bool:
    return env_flag("BOARD_DUPLICATE_CATEGORY_ONLY_IGNORE_PERIOD")
