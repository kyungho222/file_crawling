"""Normalized crawl request configuration.

This is a small adapter around the current dict-based payloads. It gives the
large dispatcher/workflow modules a stable place to read common request modes
while the legacy payload keys continue to work.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from backend.shared.basic_crawling_flow import normalize_colle_mode


TRUE_STRINGS = {"1", "true", "yes", "y", "on"}
FALSE_STRINGS = {"", "0", "false", "no", "n", "off", "none", "null"}


def parse_bool(value: Any, default: Optional[bool] = False) -> Optional[bool]:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    try:
        text = str(value).strip().lower()
    except Exception:
        return default
    if text in TRUE_STRINGS:
        return True
    if text in FALSE_STRINGS:
        return False
    return default


def first_present(payload: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return default


def normalize_string(value: Any, default: str = "") -> str:
    try:
        text = str(value if value is not None else "").strip()
    except Exception:
        return default
    return text or default


def normalize_target_date(value: Any) -> Any:
    if isinstance(value, list) and len(value) >= 2:
        return [normalize_string(value[0]), normalize_string(value[1])]
    if isinstance(value, str) and value.strip():
        return value.strip()
    return value


@dataclass(frozen=True)
class CrawlRequestConfig:
    raw_colle_mode: str
    colle_mode: str
    content_type: str
    crawl_mode: str
    start_urls_override_source: str
    scope_path_prefix_enabled: bool
    start_urls_prefix_enabled: bool
    apply_scope_path_prefix: bool
    learn_list_duplicate_exclude_enabled: bool
    learn_list_duplicate_exclude_full_scan: bool
    enable_learning: bool
    board_gap_dashboard: bool
    board_gap_exact_targets_only: bool
    target_date: Any = None

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "CrawlRequestConfig":
        data = payload or {}
        raw_colle_mode, colle_mode = normalize_colle_mode(data)
        content_type = normalize_string(data.get("content_type"), "url").lower()
        crawl_mode = normalize_string(data.get("crawl_mode"), "crawling").lower()
        scope_enabled = bool(
            parse_bool(
                first_present(data, "scope_path_prefix_enabled", "start_urls_prefix_enabled"),
                True,
            )
        )
        start_urls_prefix_enabled = bool(
            parse_bool(data.get("start_urls_prefix_enabled"), scope_enabled)
        )
        apply_scope_path_prefix = bool(
            parse_bool(data.get("apply_scope_path_prefix"), scope_enabled)
        )
        duplicate_exclude_enabled = bool(
            parse_bool(
                first_present(
                    data,
                    "learn_list_duplicate_exclude_enabled",
                    "learnListDuplicateExcludeEnabled",
                ),
                True,
            )
        )
        duplicate_exclude_full_scan = bool(
            parse_bool(data.get("learn_list_duplicate_exclude_full_scan"), True)
        )
        return cls(
            raw_colle_mode=raw_colle_mode,
            colle_mode=colle_mode,
            content_type=content_type or "url",
            crawl_mode=crawl_mode or "crawling",
            start_urls_override_source=normalize_string(data.get("start_urls_override_source")),
            scope_path_prefix_enabled=scope_enabled,
            start_urls_prefix_enabled=start_urls_prefix_enabled,
            apply_scope_path_prefix=apply_scope_path_prefix,
            learn_list_duplicate_exclude_enabled=duplicate_exclude_enabled,
            learn_list_duplicate_exclude_full_scan=duplicate_exclude_full_scan,
            enable_learning=bool(parse_bool(data.get("enable_learning"), True)),
            board_gap_dashboard=bool(parse_bool(data.get("board_gap_dashboard"), False)),
            board_gap_exact_targets_only=bool(parse_bool(data.get("board_gap_exact_targets_only"), False)),
            target_date=normalize_target_date(data.get("target_date")),
        )

    def apply_mode_keys(self, payload: Dict[str, Any]) -> None:
        payload["colle"] = self.colle_mode
        payload["content_type"] = self.content_type
        payload["crawl_mode"] = self.crawl_mode

    def apply_scope_keys(self, payload: Dict[str, Any]) -> None:
        payload["scope_path_prefix_enabled"] = self.scope_path_prefix_enabled
        payload["start_urls_prefix_enabled"] = self.start_urls_prefix_enabled
        payload["apply_scope_path_prefix"] = self.apply_scope_path_prefix

    def apply_duplicate_keys(self, payload: Dict[str, Any]) -> None:
        payload["learn_list_duplicate_exclude_enabled"] = self.learn_list_duplicate_exclude_enabled
        payload["learnListDuplicateExcludeEnabled"] = self.learn_list_duplicate_exclude_enabled
        payload["learn_list_duplicate_exclude_full_scan"] = self.learn_list_duplicate_exclude_full_scan

    def apply_dashboard_keys(self, payload: Dict[str, Any]) -> None:
        payload["enable_learning"] = self.enable_learning
        payload["board_gap_dashboard"] = self.board_gap_dashboard
        payload["board_gap_exact_targets_only"] = self.board_gap_exact_targets_only

    def apply_to_payload(self, payload: Dict[str, Any]) -> None:
        self.apply_mode_keys(payload)
        self.apply_scope_keys(payload)
        self.apply_duplicate_keys(payload)
        self.apply_dashboard_keys(payload)
        if self.start_urls_override_source:
            payload["start_urls_override_source"] = self.start_urls_override_source
        if self.target_date is not None:
            payload["target_date"] = self.target_date


def target_domains_from_url_strings(urls: List[str]) -> List[str]:
    from urllib.parse import urlparse

    from utils.url import ensure_url_scheme

    domains: List[str] = []
    seen: set[str] = set()
    for raw_url in urls or []:
        try:
            parsed = urlparse(ensure_url_scheme(raw_url))
            host = (parsed.netloc or "").strip().lower()
        except Exception:
            host = ""
        if host and host not in seen:
            seen.add(host)
            domains.append(host)
    return domains
