"""
페이지 유형(정적/동적) 자동 판별 유틸리티.
크롤링 시작 전에 간단한 HTTP 요청만으로 대상 게시판의 특성을 파악해
파이프라인이 보다 적절한 전략을 선택하도록 돕는다.
"""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Any

try:  # pragma: no cover - 안전장치
    import httpx  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# 동적 페이지에서 자주 등장하는 JS 프레임워크 및 비동기 호출 단서
DYNAMIC_KEYWORDS = [
    "axios", "fetch(", "xmlhttprequest", "$.ajax", "egovframework",
    "vue", "react", "nuxt", "next", "svelte", "angular", "ng-app",
    "data-v-", "data-ng-", "kendo", "dojo", "Ext.onReady".lower(),
    "websocket", "sockjs", "stomp"
]

# 정적 첨부 링크를 빠르게 감지하기 위한 패턴
STATIC_FILE_PATTERN = re.compile(
    r'href\s*=\s*["\']([^"\']+\.(?:hwp|hwpx|pdf|docx?|pptx?|xlsx?|xls|zip|7z|rar))',
    re.IGNORECASE,
)

JS_DOWNLOAD_PATTERN = re.compile(
    r'(?:fileDown|downloadFile|goDownload|fn_filedownload|atchFileId\s*=|FileDown\.do)',
    re.IGNORECASE,
)

HIDDEN_ATCH_PATTERN = re.compile(
    r'(?:name|id)\s*=\s*["\'](?:atchFileId|fileId)["\']',
    re.IGNORECASE,
)

@dataclass
class PageProfile:
    url: str
    page_type: str
    is_dynamic: bool
    score: int
    script_count: int
    status_code: int
    content_length: int
    fetch_ms: int
    signals: List[str]
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["signals"] = list(self.signals)  # ensure list copy
        return data


async def detect_page_profile(url: str, timeout: float = 8.0) -> Dict[str, Any]:
    if httpx is None:
        raise RuntimeError("httpx is required for page profiling but is not installed.")
    """
    대상 URL의 HTML만으로 동적/정적 특성을 추정한다.
    반환값은 직렬화 가능한 dict이며 scan 워커 큐에 그대로 첨부할 수 있다.
    """
    start_ts = time.monotonic()
    try:
        # 타임아웃을 명확하게 설정: connect, read, write, pool 모두 동일한 타임아웃 적용
        # 총 타임아웃 = timeout 초 (기본값 8초, 비동기 처리 시 3초로 단축)
        timeout_config = httpx.Timeout(
            timeout,  # connect timeout
            timeout,  # read timeout
            timeout,  # write timeout
            timeout   # pool timeout
        )
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout_config,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            resp = await client.get(url, timeout=timeout_config)
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - start_ts) * 1000)
        profile = PageProfile(
            url=url,
            page_type="unknown",
            is_dynamic=True,  # 안전을 위해 기본값은 동적 처리
            score=0,
            script_count=0,
            status_code=0,
            content_length=0,
            fetch_ms=elapsed_ms,
            signals=[],
            note=f"fetch_error:{type(exc).__name__}",
        )
        return profile.to_dict()

    html = resp.text or ""
    lowered = html.lower()
    elapsed_ms = int((time.monotonic() - start_ts) * 1000)
    script_count = len(re.findall(r"<script\b", lowered))

    dynamic_score = 0
    signals: List[str] = []

    def add_signal(label: str):
        nonlocal signals
        if label not in signals:
            signals.append(label)

    if script_count >= 25:
        dynamic_score += 1
        add_signal("script>=25")
    if script_count >= 60:
        dynamic_score += 1
        add_signal("script>=60")

    if re.search(r"\bon(click|load|change|submit|keyup)\s*=", lowered):
        dynamic_score += 1
        add_signal("inline-handlers")

    if re.search(r"fetch\s*\(", lowered) or "window.fetch" in lowered:
        dynamic_score += 1
        add_signal("fetch")
    if "xmlhttprequest" in lowered or "$.ajax" in lowered:
        dynamic_score += 1
        add_signal("ajax")

    if "<iframe" in lowered:
        add_signal("iframe")

    for keyword in DYNAMIC_KEYWORDS:
        if keyword in lowered:
            dynamic_score += 1
            add_signal(f"kw:{keyword}")

    has_static_files = bool(STATIC_FILE_PATTERN.search(lowered))
    if has_static_files:
        add_signal("direct-file-link")

    has_js_download = bool(JS_DOWNLOAD_PATTERN.search(lowered))
    if has_js_download:
        add_signal("js-download")

    has_hidden_atch = bool(HIDDEN_ATCH_PATTERN.search(lowered))
    if has_hidden_atch:
        add_signal("hidden-atch-id")

    if dynamic_score >= 2:
        page_type = "dynamic"
        is_dynamic = True
    elif not dynamic_score and (has_static_files or has_js_download):
        page_type = "static"
        is_dynamic = False
    elif dynamic_score == 1:
        page_type = "hybrid"
        is_dynamic = True
    else:
        page_type = "unknown"
        is_dynamic = True

    profile = PageProfile(
        url=url,
        page_type=page_type,
        is_dynamic=is_dynamic,
        score=dynamic_score,
        script_count=script_count,
        status_code=resp.status_code,
        content_length=len(resp.content),
        fetch_ms=elapsed_ms,
        signals=signals,
        note="static_files" if has_static_files else "",
    )
    return profile.to_dict()


def detect_page_profile_sync(url: str, timeout: float = 8.0) -> Dict[str, Any]:
    """
    동기 코드에서도 사용할 수 있도록 sync wrapper 제공.
    """
    return asyncio.run(detect_page_profile(url, timeout=timeout))

