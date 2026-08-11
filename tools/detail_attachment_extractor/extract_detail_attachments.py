"""Fetch one detail page and return its document attachment URLs.

Usage:
    python tools/detail_attachment_extractor/extract_detail_attachments.py \
        "https://example.go.kr/board/view.do?seq=1"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from dataclasses import asdict, dataclass, replace
from html import unescape
from typing import Iterable
from urllib.parse import parse_qsl, unquote, urljoin, urlparse, urlunparse

import aiohttp
from bs4 import BeautifulSoup


DOCUMENT_EXTENSIONS = {
    ".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".txt", ".csv", ".zip", ".7z",
}
NOISE_SUFFIXES = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".css", ".js", ".woff", ".woff2", ".mp3", ".mp4",
}
DOWNLOAD_MARKERS = (
    "filedown", "download", "attachment", "attach", "/fms/", "/file/",
)


@dataclass(frozen=True)
class Attachment:
    title: str
    file_name: str
    url: str
    source: str


# HTML 및 공백이 섞인 값을 정리된 한 줄 텍스트로 변환합니다.
def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


# URL 또는 파일명에서 확장자를 추출합니다.
def _extension(value: str) -> str:
    path = unquote(urlparse(value).path or "").lower()
    match = re.search(r"(\.[a-z0-9]{1,8})$", path)
    return match.group(1) if match else ""


# URL과 이름을 기준으로 문서 첨부파일 후보인지 판별합니다.
def _looks_like_document(url: str, name: str) -> bool:
    url_lower = url.lower()
    ext = _extension(name) or _extension(url)
    if ext in NOISE_SUFFIXES:
        return False
    return ext in DOCUMENT_EXTENSIONS or any(marker in url_lower for marker in DOWNLOAD_MARKERS)


# 중복 비교용으로 URL의 호스트·경로·주요 쿼리를 정규화합니다.
def _canonical_key(url: str) -> str:
    parsed = urlparse(url)
    query = sorted(
        (key.lower(), value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if key and value and key.lower() not in {"page", "pageno", "menuno", "utm_source", "utm_medium"}
    )
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, "", "&".join(f"{k}={v}" for k, v in query), ""))


# HTML 노드의 속성·표시 텍스트에서 첨부 표시명을 선택합니다.
def _name_from_node(node: object, url: str) -> str:
    text = _clean_text(getattr(node, "get_text", lambda *args, **kwargs: "")(" ", strip=True))
    for value in (
        getattr(node, "get", lambda key, default=None: default)("download"),
        getattr(node, "get", lambda key, default=None: default)("title"),
        getattr(node, "get", lambda key, default=None: default)("data-filename"),
        text,
        unquote(urlparse(url).path.rsplit("/", 1)[-1]),
    ):
        value = _clean_text(value)
        if value:
            return value
    return "attachment"


# 표시명과 URL에서 실제 저장에 쓸 파일명을 추정합니다.
def _file_name_from_value(value: str, url: str) -> str:
    """Prefer an actual document filename over a generic display label."""
    text = _clean_text(value)
    extension_pattern = "|".join(re.escape(ext[1:]) for ext in sorted(DOCUMENT_EXTENSIONS))
    match = re.search(rf"([^\\/:*?\"<>|]+\.({extension_pattern}))", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    query_name = _file_name_from_url_query(url)
    if query_name:
        return query_name
    path_name = _clean_text(unquote(urlparse(url).path.rsplit("/", 1)[-1]))
    if _extension(path_name) in DOCUMENT_EXTENSIONS:
        return path_name
    return text or "attachment"


# 다운로드 URL 쿼리의 원본 파일명 파라미터를 읽습니다.
def _file_name_from_url_query(url: str) -> str:
    parsed = urlparse(url)
    file_name_keys = {
        "user_file_nm", "filename", "file_name", "original_name", "original_filename",
        "atch_file_nm", "atchmnflnm", "download_name",
    }
    for key, query_value in parse_qsl(parsed.query, keep_blank_values=False):
        if key.lower() in file_name_keys:
            query_name = _clean_text(query_value)
            if _extension(query_name) in DOCUMENT_EXTENSIONS:
                return query_name
    return ""


# Content-Disposition 헤더에서 URL 디코딩된 파일명을 추출합니다.
def _file_name_from_content_disposition(value: str) -> str:
    text = str(value or "")
    encoded_match = re.search(r"filename\*\s*=\s*(?:UTF-8'')?([^;]+)", text, flags=re.IGNORECASE)
    if encoded_match:
        return _clean_text(unquote(encoded_match.group(1).strip().strip('"')))
    plain_match = re.search(r"filename\s*=\s*(?:\"([^\"]+)\"|([^;]+))", text, flags=re.IGNORECASE)
    if plain_match:
        return _clean_text(unquote(plain_match.group(1) or plain_match.group(2)))
    return ""


# 응답 헤더 파일명이 문서명으로 신뢰 가능한지 확인합니다.
def _is_usable_header_file_name(value: str) -> bool:
    """Reject mojibake headers when a URL already supplied the original name."""
    text = _clean_text(value)
    return bool(text and "?" not in text and _extension(text) in DOCUMENT_EXTENSIONS)


# HTML 속성 또는 onclick 코드에서 첨부 URL과 발견 경로를 찾습니다.
def _resolve_candidate(raw_url: str, onclick: str, base_url: str) -> tuple[str, str]:
    raw_url = _clean_text(raw_url)
    if raw_url and not raw_url.lower().startswith("javascript:"):
        return urljoin(base_url, raw_url), "attribute"

    script = f"{raw_url} {onclick}"
    match = re.search(r"['\"](?P<url>(?:https?://|/)[^'\"\s]+)['\"]", script)
    if match:
        return urljoin(base_url, unescape(match.group("url"))), "onclick"
    return "", ""


# 상세페이지 HTML에서 중복 없는 문서 첨부파일 후보를 수집합니다.
def extract_attachments(html: str, detail_url: str) -> list[Attachment]:
    soup = BeautifulSoup(html, "html.parser")
    attachments: list[Attachment] = []
    seen: set[str] = set()
    attributes = ("href", "data-href", "data-url", "data-download-url", "formaction")

    for node in soup.find_all(["a", "button", "input", "form"]):
        raw_url = next((node.get(attribute) for attribute in attributes if node.get(attribute)), "")
        url, source = _resolve_candidate(raw_url, node.get("onclick", ""), detail_url)
        if not url:
            continue
        name = _name_from_node(node, url)
        if not _looks_like_document(url, name):
            continue
        key = _canonical_key(url)
        if not key or key in seen:
            continue
        seen.add(key)
        attachments.append(
            Attachment(
                title=name,
                file_name=_file_name_from_value(name, url),
                url=url,
                source=source,
            )
        )

    return attachments


# 상세페이지 HTML을 지정한 시간 안에 가져옵니다.
async def fetch_detail_html(url: str, timeout_sec: float) -> str:
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, trust_env=False) as session:
        async with session.get(url, allow_redirects=True) as response:
            response.raise_for_status()
            return await response.text(errors="replace")


# HEAD 또는 범위 GET 응답 헤더로 후보 파일명을 보완합니다.
async def _resolve_attachment_file_names(
    attachments: list[Attachment],
    detail_url: str,
    timeout_sec: float,
) -> list[Attachment]:
    """Fill file_name from Content-Disposition without downloading file bodies."""
    if not attachments:
        return attachments
    timeout = aiohttp.ClientTimeout(total=min(max(timeout_sec, 1.0), 10.0))
    headers = {"User-Agent": "Mozilla/5.0", "Referer": detail_url, "Accept": "*/*"}
    semaphore = asyncio.Semaphore(4)

    # 첨부 한 건의 응답 헤더를 확인해 파일명을 갱신합니다.
    async def resolve(attachment: Attachment, session: aiohttp.ClientSession) -> Attachment:
        async with semaphore:
            # A site-supplied original filename query parameter is authoritative.
            if _file_name_from_url_query(attachment.url):
                return attachment
            try:
                async with session.head(attachment.url, allow_redirects=True) as response:
                    header_name = _file_name_from_content_disposition(
                        response.headers.get("Content-Disposition", "")
                    )
                    if _is_usable_header_file_name(header_name):
                        return replace(attachment, file_name=header_name)
            except Exception:
                pass
            try:
                async with session.get(
                    attachment.url,
                    headers={"Range": "bytes=0-0"},
                    allow_redirects=True,
                ) as response:
                    header_name = _file_name_from_content_disposition(
                        response.headers.get("Content-Disposition", "")
                    )
                    await response.content.read(1)
                    if _is_usable_header_file_name(header_name):
                        return replace(attachment, file_name=header_name)
            except Exception:
                pass
        return attachment

    async with aiohttp.ClientSession(timeout=timeout, headers=headers, trust_env=False) as session:
        return list(await asyncio.gather(*(resolve(item, session) for item in attachments)))


# URL 입력부터 첨부 추출과 파일명 보완까지 전체 흐름을 실행합니다.
async def extract_from_url(url: str, timeout_sec: float = 20.0) -> dict[str, object]:
    detail_url = str(url or "").strip()
    parsed = urlparse(detail_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("url must be an absolute http(s) URL")

    html = await fetch_detail_html(detail_url, timeout_sec)
    attachments = extract_attachments(html, detail_url)
    attachments = await _resolve_attachment_file_names(attachments, detail_url, timeout_sec)
    return {
        "detail_url": detail_url,
        "attachment_count": len(attachments),
        "attachments": [asdict(item) for item in attachments],
    }


# 명령행 인자를 받아 추출 결과를 UTF-8 JSON으로 출력합니다.
def main(argv: Iterable[str] | None = None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass
    parser = argparse.ArgumentParser(description="Extract document attachment URLs from one detail page.")
    parser.add_argument("url", help="detail page URL")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    args = parser.parse_args(argv)
    result = asyncio.run(extract_from_url(args.url, args.timeout))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
