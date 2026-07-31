from __future__ import annotations

import os
from urllib.parse import urlparse

from backend.board.gm_board import (
    is_gm_contract_detail_url,
    is_gm_general_bbs_url,
    is_gm_group_info_url,
    is_gm_lobas_tcm_detail_url,
    is_gm_static_info_url,
)
from backend.board.yongin_board import is_yongin_general_bbs_url


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or str(default))
    except Exception:
        return default


def clamp_timeout(value: float, *, minimum: float = 1.0, maximum: float = 30.0) -> float:
    return max(minimum, min(float(value or 0.0), maximum))


def is_gm_fast_static_fetch_url(url: str | None) -> bool:
    return bool(
        is_gm_contract_detail_url(url)
        or is_gm_lobas_tcm_detail_url(url)
        or is_gm_general_bbs_url(url)
        or is_gm_group_info_url(url)
        or is_gm_static_info_url(url)
        or is_gm_partinfo_static_noquery_url(url)
    )


def is_gm_partinfo_static_noquery_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = (parsed.query or "").strip()
    except Exception:
        return False
    return bool(
        host in {"gm.go.kr", "www.gm.go.kr"}
        and path.startswith("/pt/partinfo/")
        and not query
        and (path.endswith(".jsp") or path.endswith(".do"))
    )


def gm_fast_static_fetch_timeout_sec() -> float:
    raw = os.getenv("BOARD_GM_STATIC_FETCH_TIMEOUT_SEC")
    if raw in (None, ""):
        raw = os.getenv("BOARD_GM_CONTRACT_STATIC_FETCH_TIMEOUT_SEC", "2.5")
    return clamp_timeout(_env_float("BOARD_GM_STATIC_FETCH_TIMEOUT_SEC", float(raw or "2.5")), maximum=10.0)


def gm_general_bbs_static_fetch_timeout_sec() -> float:
    return clamp_timeout(
        _env_float("BOARD_GM_GENERAL_BBS_STATIC_FETCH_TIMEOUT_SEC", 10.0),
        maximum=15.0,
    )


def is_yongin_fast_static_fetch_url(url: str | None) -> bool:
    return bool(is_yongin_general_bbs_url(str(url or "")))


def yongin_prefetch_static_fetch_timeout_sec() -> float:
    return clamp_timeout(
        _env_float("BOARD_YONGIN_PREFETCH_STATIC_FETCH_TIMEOUT_SEC", 2.5),
        maximum=10.0,
    )


def is_dobong_receipt_detail_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()
    except Exception:
        return False
    return bool(
        host in {"dobong.go.kr", "www.dobong.go.kr"}
        and path == "/wdb_dev/receipt/receiptview.asp"
        and "receipt_mst_num=" in query
    )


def is_sd_go_fast_static_fetch_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    return bool(
        host in {"sd.go.kr", "www.sd.go.kr"}
        and (
            path.endswith("/selectbbsnttlist.do")
            or path.endswith("/sitemap.do")
        )
    )


def dobong_receipt_prefetch_static_fetch_timeout_sec() -> float:
    return clamp_timeout(
        _env_float("BOARD_DOBONG_RECEIPT_PREFETCH_STATIC_FETCH_TIMEOUT_SEC", 2.5),
        maximum=8.0,
    )


def sd_go_prefetch_static_fetch_timeout_sec() -> float:
    return clamp_timeout(
        _env_float("BOARD_SD_GO_PREFETCH_STATIC_FETCH_TIMEOUT_SEC", 8.0),
        maximum=15.0,
    )


def is_suwon_slow_detail_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    return bool(
        host in {"suwon.go.kr", "www.suwon.go.kr"}
        and (path.startswith("/culture/") or path.startswith("/web/reserv/"))
        and path.endswith(".do")
        and not path.endswith("/index.do")
    )


def suwon_slow_detail_static_fetch_timeout_sec() -> float:
    return 10.0


def suwon_slow_detail_fetch_guard_timeout_sec() -> float:
    return 12.0


def static_fetch_connect_timeout_sec(
    url: str | None,
    *,
    request_timeout_sec: float,
    configured_timeout_sec: float,
) -> float:
    """Resolve the TCP-connect budget without letting a short default preempt a scoped request."""
    request_timeout = max(0.5, float(request_timeout_sec or 0.0))
    configured_timeout = max(0.5, float(configured_timeout_sec or 0.0))
    if is_suwon_slow_detail_url(url):
        configured_timeout = max(configured_timeout, suwon_slow_detail_static_fetch_timeout_sec())
    return min(configured_timeout, request_timeout)


def prefetch_static_fetch_timeout_sec(
    url: str | None,
    *,
    accelerated_parse_only: bool = False,
    accelerated_fetch_timeout_sec: float = 6.0,
) -> float:
    if accelerated_parse_only:
        base_timeout = float(accelerated_fetch_timeout_sec or 6.0)
    else:
        base_timeout = _env_float(
            "BOARD_DETAIL_PREFETCH_STATIC_FETCH_TIMEOUT_SEC",
            _env_float("BOARD_DETAIL_STATIC_FETCH_TIMEOUT_SEC", 20.0),
        )

    if is_gm_general_bbs_url(url):
        base_timeout = gm_general_bbs_static_fetch_timeout_sec()
    elif is_gm_fast_static_fetch_url(url):
        base_timeout = gm_fast_static_fetch_timeout_sec()
    elif is_yongin_fast_static_fetch_url(url):
        base_timeout = yongin_prefetch_static_fetch_timeout_sec()
    elif is_dobong_receipt_detail_url(url):
        base_timeout = dobong_receipt_prefetch_static_fetch_timeout_sec()
    elif is_sd_go_fast_static_fetch_url(url):
        base_timeout = sd_go_prefetch_static_fetch_timeout_sec()
    elif is_suwon_slow_detail_url(url):
        base_timeout = max(base_timeout, suwon_slow_detail_static_fetch_timeout_sec())

    return clamp_timeout(
        base_timeout,
        maximum=clamp_timeout(
            _env_float("BOARD_DETAIL_PREFETCH_STATIC_FETCH_MAX_SEC", 30.0),
            minimum=1.0,
            maximum=60.0,
        ),
    )


def static_fetch_effective_timeout_sec(url: str | None, requested_timeout_sec: float) -> float:
    requested = float(requested_timeout_sec or 30.0)
    if is_gm_general_bbs_url(url):
        return clamp_timeout(min(requested, gm_general_bbs_static_fetch_timeout_sec()), maximum=15.0)
    if is_gm_fast_static_fetch_url(url):
        return clamp_timeout(min(requested, gm_fast_static_fetch_timeout_sec()), maximum=10.0)
    if is_yongin_fast_static_fetch_url(url) and requested <= 4.0:
        return clamp_timeout(min(requested, yongin_prefetch_static_fetch_timeout_sec()), maximum=10.0)
    if is_dobong_receipt_detail_url(url) and requested <= 4.0:
        return clamp_timeout(min(requested, dobong_receipt_prefetch_static_fetch_timeout_sec()), maximum=8.0)
    if is_sd_go_fast_static_fetch_url(url):
        return clamp_timeout(min(requested, sd_go_prefetch_static_fetch_timeout_sec()), maximum=15.0)
    if is_suwon_slow_detail_url(url):
        return clamp_timeout(max(requested, suwon_slow_detail_static_fetch_timeout_sec()), maximum=30.0)
    return max(5.0, requested)


def should_cap_prefetch_guard_timeout(url: str | None) -> bool:
    return bool(
        is_gm_fast_static_fetch_url(url)
        or is_yongin_fast_static_fetch_url(url)
        or is_dobong_receipt_detail_url(url)
        or is_sd_go_fast_static_fetch_url(url)
    )
