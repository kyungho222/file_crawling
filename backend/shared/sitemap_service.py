import html
import hashlib

from utils.hash_policy import hash_generation_disabled
import json
import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Callable, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
import asyncio
from utils.url import canonicalize_url_for_dedup as _canonicalize_url_for_dedup
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from backend.shared.redis_sse_service import publish_wake_event

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
DEFAULT_BOARD_PATTERNS = ["board", "bbs", "forum", "thread", "list", "article"]
BUILD_SITEMAP_CONCURRENCY = 20
SITEMAP_MAX_RESPONSE_BYTES = int(os.getenv("SITEMAP_MAX_RESPONSE_BYTES", str(1024 * 1024)))
SITEMAP_REQUEST_TOTAL_TIMEOUT = float(os.getenv("SITEMAP_REQUEST_TOTAL_TIMEOUT", "20"))
SITEMAP_SLOW_RESPONSE_THRESHOLD = float(os.getenv("SITEMAP_SLOW_RESPONSE_THRESHOLD", "2.0"))


def _env_flag(name: str, default: str = "1") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")


SITEMAP_SKIP_NON_HTML = _env_flag("SITEMAP_SKIP_NON_HTML", "1")

CANONICAL_QUERY_BLACKLIST = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}


@dataclass
class CrawlResult:
    entries: List[dict]
    errors: List[str]
    fallback_used: bool = False


def ensure_scheme(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"
    return url


def normalize_url(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None
    normalized = parsed._replace(netloc=parsed.netloc.lower(), fragment="")
    return urlunparse(normalized)


def canonical_host(host: str) -> str:
    if not host:
        return ""
    cleaned = host.lower()
    if cleaned.startswith("www."):
        cleaned = cleaned[4:]
    return cleaned


def same_domain(candidate: str, reference: str) -> bool:
    candidate_host = canonical_host(urlparse(candidate).netloc)
    reference_host = canonical_host(reference)
    if not candidate_host or not reference_host:
        return False
    return (
        candidate_host == reference_host
        or candidate_host.endswith(f".{reference_host}")
        or reference_host.endswith(f".{candidate_host}")
    )


def _canonical_query(query: str) -> str:
    if not query:
        return ""
    try:
        params = [
            (k, v)
            for k, v in parse_qsl(query, keep_blank_values=True)
            if k not in CANONICAL_QUERY_BLACKLIST
        ]
        params.sort()
        return urlencode(params, doseq=True)
    except Exception:
        return query


def canonicalize_url_for_dedup(url: str) -> str | None:
    """utils.url과 동일한 정규화 함수 사용 (중복 판정/저장 일관성)."""
    s = _canonicalize_url_for_dedup(url)
    return s if s else None


def canonical_hash(url: str) -> str:
    if hash_generation_disabled():
        return url or ""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _is_html_content_type(content_type: str) -> bool:
    if not content_type:
        return True
    ctype = content_type.split(";", 1)[0].strip().lower()
    return ctype in ("text/html", "application/xhtml+xml")


def _should_skip_response(resp: aiohttp.ClientResponse) -> tuple[bool, str]:
    if SITEMAP_SKIP_NON_HTML:
        if not _is_html_content_type(resp.headers.get("Content-Type", "")):
            return True, "non-html"
    if SITEMAP_MAX_RESPONSE_BYTES and SITEMAP_MAX_RESPONSE_BYTES > 0:
        length = resp.headers.get("Content-Length")
        if length:
            try:
                if int(length) > SITEMAP_MAX_RESPONSE_BYTES:
                    return True, "too-large"
            except ValueError:
                pass
    return False, ""


async def _read_limited_text(
    resp: aiohttp.ClientResponse, max_bytes: int
) -> tuple[Optional[str], Optional[str]]:
    if max_bytes and max_bytes > 0:
        raw = await resp.content.read(max_bytes + 1)
        if len(raw) > max_bytes:
            return None, "too-large"
    else:
        raw = await resp.read()
    encoding = resp.charset or resp.get_encoding() or "utf-8"
    try:
        return raw.decode(encoding, errors="ignore"), None
    except Exception:
        return raw.decode("utf-8", errors="ignore"), None


def is_board_url(url: str, patterns: Sequence[str]) -> bool:
    lower = url.lower()
    for pattern in patterns:
        if pattern and pattern in lower:
            return True
    return False


def extract_title(soup: BeautifulSoup, url: str = "", html: str = "") -> str:
    if url and "youthcenter.go.kr" in url.lower():
        try:
            from backend.board.youthcenter_board import (
                extract_youthcenter_board_title,
                extract_youthcenter_openapi_title,
                extract_youthcenter_policy_title,
                is_youthcenter_bbs_view_url,
                is_youthcenter_openapi_doc_url,
                is_youthcenter_policy_detail_url,
            )

            if is_youthcenter_bbs_view_url(url):
                title = extract_youthcenter_board_title(soup, url=url, html=html or str(soup))
                if title:
                    return title
            if is_youthcenter_policy_detail_url(url):
                title = extract_youthcenter_policy_title(soup, url=url, html=html or str(soup))
                if title:
                    return title
            if is_youthcenter_openapi_doc_url(url):
                title = extract_youthcenter_openapi_title(soup)
                if title:
                    return title
        except Exception:
            pass
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    header = soup.find(attrs={"class": re.compile(r"(title|headline)", re.I)})
    if header and header.get_text(strip=True):
        return header.get_text(strip=True)
    return ""


def detect_board_in_soup(soup: BeautifulSoup, url: str, patterns: Sequence[str]) -> bool:
    """
    HTML 기반 히ュー리스틱으로 해당 페이지가 '게시판'인지 판별.
    점수화 방식: 여러 신호를 더해 threshold 이상이면 게시판으로 판단.
    """
    score = 0
    text = (soup.get_text(" ", strip=True) or "").lower()
    url_lower = (url or "").lower()
    patterns = [p.lower() for p in patterns if p]

    # 1) URL 패턴 매칭 (강한 신호)
    for p in patterns:
        if p and p in url_lower:
            score += 3
            break

    # 2) 키워드 매칭 (작성자/작성일/조회수 등)
    kw_list = ["게시판", "글 목록", "목록", "작성자", "작성일", "등록일", "조회수", "댓글", "등록자", "글번호"]
    for kw in kw_list:
        if kw in text:
            score += 1
    # cap incremental score from keywords
    if score > 6:
        score = 6

    # 3) 리스트/테이블 구조 탐지: 많은 게시글 링크와 날짜 패턴 존재 여부
    date_re = re.compile(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}")
    anchors = soup.find_all("a", href=True)
    post_like = 0
    for a in anchors:
        # anchor text 짧고 날짜 인접(형태상 게시물 목록에 흔함)
        txt = (a.get_text(" ", strip=True) or "")
        if len(txt) < 120 and txt and any(ch.isalpha() or ch.isdigit() for ch in txt):
            # 근처에 날짜가 있는지 확인
            sib_text = ""
            parent = a.parent
            if parent:
                sib_text = parent.get_text(" ", strip=True)
            if date_re.search(sib_text) or date_re.search(txt):
                post_like += 1
            # class name 힌트
            cls = " ".join(a.get("class") or [])
            if re.search(r"(title|subject|post|article|board|item|link)", cls, re.I):
                post_like += 1
    if post_like >= 5:
        score += 3
    elif post_like >= 2:
        score += 1

    # 4) 테이블 기반 게시글 목록 검사
    tables = soup.find_all("table")
    table_rows = 0
    for t in tables:
        rows = t.find_all("tr")
        if len(rows) >= 3:
            # 첫 몇 행에서 날짜 패턴이 보이면 게시글 테이블 가능
            sample_text = " ".join([" ".join([c.get_text(" ", strip=True) for c in r.find_all("td")])
                                    for r in rows[:5]])
            if date_re.search(sample_text):
                table_rows += len(rows)
    if table_rows >= 5:
        score += 2

    # 최종 판단: threshold 4
    return score >= 4


def crawl_static(
    root_url: str,
    board_patterns: Iterable[str],
    max_depth: int = 5,
    max_pages: int = 400,
    timeout: int = 10,
    stream_handler: Optional[Callable[[dict], None]] = None,
) -> CrawlResult:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; Sitemap/1.0)"})
    root_url = ensure_scheme(root_url)
    parsed_root = urlparse(root_url)
    base_host = parsed_root.netloc
    queue = deque([(root_url, 0)])
    visited: set[str] = set()
    entries: list[dict] = []
    errors: list[str] = []
    patterns = [p.lower() for p in board_patterns if p]
    seen_hashes: set[str] = set()

    while queue and len(entries) < max_pages:
        current_url, depth = queue.popleft()
        normalized = normalize_url(current_url)
        if not normalized or normalized in visited:
            continue
        visited.add(normalized)
        canonical = canonicalize_url_for_dedup(normalized)
        if not canonical:
            continue
        canonical_key = canonical_hash(canonical)
        if canonical_key in seen_hashes:
            continue
        seen_hashes.add(canonical_key)
        try:
            resp = session.get(normalized, timeout=timeout)
        except requests.RequestException as exc:
            errors.append(f"{normalized} 요청 실패: {exc}")
            continue
        if resp.status_code >= 400:
            errors.append(f"{normalized} 상태 코드 {resp.status_code}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        title = extract_title(soup, normalized, resp.text or "") or normalized
        # HTML 기반 판별기를 우선 사용하고, URL 패턴 기반 판별을 보조로 사용
        try:
            is_board_flag = detect_board_in_soup(soup, normalized, patterns) or is_board_url(normalized, patterns)
        except Exception:
            is_board_flag = is_board_url(normalized, patterns)
        entry = {
            "url": normalized,
            "title": title,
            "is_board": is_board_flag,
            "canonical_url": canonical,
            "canonical_hash": canonical_key,
        }
        entries.append(entry)
        if stream_handler:
            try:
                stream_handler(entry)
            except Exception:
                logger.debug("stream_handler failed for %s", normalized, exc_info=True)

        if depth >= max_depth:
            continue

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            candidate = normalize_url(urljoin(normalized, href))
            if not candidate:
                continue
            if candidate in visited:
                continue
            if not same_domain(candidate, base_host):
                continue
            queue.append((candidate, depth + 1))
            if len(entries) + len(queue) >= max_pages:
                break

    return CrawlResult(entries=entries, errors=errors)


def crawl_dynamic(
    root_url: str,
    board_patterns: Iterable[str],
    max_links: int = 250,
    timeout: int = 30_000,
    stream_handler: Optional[Callable[[dict], None]] = None,
) -> CrawlResult:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        msg = "Playwright 미설치: `pip install playwright` 이 후 `playwright install` 필요"
        logger.warning(msg)
        return CrawlResult(entries=[], errors=[msg], fallback_used=True)

    root_url = ensure_scheme(root_url)
    base_host = urlparse(root_url).netloc
    patterns = [p.lower() for p in board_patterns if p]
    collected: dict[str, dict] = {}
    errors: list[str] = []
    seen_hashes: set[str] = set()

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                ignore_https_errors=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="ko-KR",
            )
            page = context.new_page()
            page.goto(root_url, wait_until="networkidle", timeout=timeout)
            page.wait_for_timeout(2000)
            html_content = page.content()
            browser.close()
    except Exception as exc:
        errors.append(f"Playwright 처리 실패: {exc}")
        return CrawlResult(entries=[], errors=errors, fallback_used=True)

    soup = BeautifulSoup(html_content, "html.parser")
    normalized_root = normalize_url(root_url)
    if normalized_root:
        canonical_root = canonicalize_url_for_dedup(normalized_root)
        if canonical_root:
            canonical_key = canonical_hash(canonical_root)
            if canonical_key not in seen_hashes:
                seen_hashes.add(canonical_key)
                try:
                    root_is_board = detect_board_in_soup(soup, normalized_root, patterns) or is_board_url(
                        normalized_root, patterns
                    )
                except Exception:
                    root_is_board = is_board_url(normalized_root, patterns)
                entry = {
                    "url": normalized_root,
                    "title": extract_title(soup, normalized_root, text or "") or normalized_root,
                    "is_board": root_is_board,
                    "canonical_url": canonical_root,
                    "canonical_hash": canonical_key,
                }
                collected[normalized_root] = entry
                if stream_handler:
                    try:
                        stream_handler(entry)
                    except Exception:
                        logger.debug("stream_handler failed for %s", normalized_root, exc_info=True)

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        candidate = normalize_url(urljoin(root_url, href))
        canonical_candidate = canonicalize_url_for_dedup(candidate)
        if not candidate or not canonical_candidate:
            continue
        canonical_key = canonical_hash(canonical_candidate)
        if canonical_key in seen_hashes:
            continue
        if not same_domain(candidate, base_host):
            continue
        text = anchor.get_text(strip=True) or candidate
        # anchor 주변 텍스트/클래스 검사로 게시판 힌트 획득
        anchor_text_lower = (anchor.get_text(" ", strip=True) or "").lower()
        parent_text = ""
        if anchor.parent:
            parent_text = anchor.parent.get_text(" ", strip=True).lower()
        is_board_anchor = is_board_url(candidate, patterns) or any(
            kw in anchor_text_lower for kw in ("게시판", "목록", "글", "게시물")
        ) or any(kw in parent_text for kw in ("게시판", "목록", "작성일", "작성자", "조회수"))
        entry = {
            "url": candidate,
            "title": text,
            "is_board": is_board_anchor,
            "canonical_url": canonical_candidate,
            "canonical_hash": canonical_key,
        }
        seen_hashes.add(canonical_key)
        collected[candidate] = entry
        if stream_handler:
            try:
                stream_handler(entry)
            except Exception:
                logger.debug("stream_handler failed for %s", candidate, exc_info=True)
        if len(collected) >= max_links:
            break

    return CrawlResult(entries=list(collected.values()), errors=errors, fallback_used=True)


async def async_crawl_static(
    root_url: str,
    board_patterns: Iterable[str],
    max_depth: int = 5,
    max_pages: int = 400,
    timeout_connect: int = 5,
    timeout_read: int = 10,
    concurrency: int = BUILD_SITEMAP_CONCURRENCY,
    stream_handler: Optional[Callable[[dict], None]] = None,
) -> CrawlResult:
    """
    Async version of crawl_static using aiohttp for concurrent fetches.
    """
    root_url = ensure_scheme(root_url)
    parsed_root = urlparse(root_url)
    base_host = parsed_root.netloc
    queue: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
    await queue.put((root_url, 0))
    visited: set[str] = set()
    entries: list[dict] = []
    errors: list[str] = []
    patterns = [p.lower() for p in board_patterns if p]
    seen_hashes: set[str] = set()

    timeout = ClientTimeout(connect=timeout_connect, sock_read=timeout_read, total=SITEMAP_REQUEST_TOTAL_TIMEOUT)
    connector = TCPConnector(limit=concurrency, limit_per_host=max(2, concurrency // 4))
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Sitemap/1.0)"}

    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:

        async def worker() -> None:
            nonlocal entries, errors
            while not queue.empty() and len(entries) < max_pages:
                try:
                    current_url, depth = await queue.get()
                except asyncio.CancelledError:
                    return
                normalized = normalize_url(current_url)
                if not normalized or normalized in visited:
                    queue.task_done()
                    continue
                visited.add(normalized)
                canonical = canonicalize_url_for_dedup(normalized)
                if not canonical:
                    queue.task_done()
                    continue
                canonical_key = canonical_hash(canonical)
                if canonical_key in seen_hashes:
                    queue.task_done()
                    continue
                seen_hashes.add(canonical_key)

                async with sem:
                    try:
                        req_start = monotonic()
                        async with session.get(normalized) as resp:
                            status = resp.status
                            skip, reason = _should_skip_response(resp)
                            if skip:
                                errors.append(f"{normalized} 응답 스킵: {reason}")
                                await resp.release()
                                queue.task_done()
                                continue
                            text, read_err = await _read_limited_text(resp, SITEMAP_MAX_RESPONSE_BYTES)
                            if read_err:
                                errors.append(f"{normalized} 응답 스킵: {read_err}")
                                queue.task_done()
                                continue
                        req_dur = monotonic() - req_start
                        if req_dur > SITEMAP_SLOW_RESPONSE_THRESHOLD:
                            logger.info("느린 응답 감지: url=%s duration=%.2fs", normalized, req_dur)
                    except asyncio.TimeoutError as exc:
                        errors.append(f"{normalized} 타임아웃: {exc}")
                        queue.task_done()
                        continue
                    except Exception as exc:
                        errors.append(f"{normalized} 요청 실패: {exc}")
                        logger.debug("요청 실패 상세: %s", normalized, exc_info=True)
                        queue.task_done()
                        continue

                if status >= 400:
                    errors.append(f"{normalized} 상태 코드 {status}")
                    queue.task_done()
                    continue

                soup = BeautifulSoup(text or "", "html.parser")
                title = extract_title(soup, normalized, text or "") or normalized
                try:
                    is_board_flag = detect_board_in_soup(soup, normalized, patterns) or is_board_url(normalized, patterns)
                except Exception:
                    is_board_flag = is_board_url(normalized, patterns)
                entry = {
                    "url": normalized,
                    "title": title,
                    "is_board": is_board_flag,
                    "canonical_url": canonical,
                    "canonical_hash": canonical_key,
                }
                entries.append(entry)
                if stream_handler:
                    try:
                        stream_handler(entry)
                    except Exception:
                        logger.debug("stream_handler failed for %s", normalized, exc_info=True)

                if depth < max_depth:
                    for anchor in soup.find_all("a", href=True):
                        href = anchor["href"]
                        candidate = normalize_url(urljoin(normalized, href))
                        if not candidate:
                            continue
                        if candidate in visited:
                            continue
                        if not same_domain(candidate, base_host):
                            continue
                        await queue.put((candidate, depth + 1))
                        if len(entries) + queue.qsize() >= max_pages:
                            break

                queue.task_done()

        # start workers
        workers = [asyncio.create_task(worker(), name=f"sitemap-worker-{i}") for i in range(max(2, concurrency // 4))]
        try:
            await queue.join()
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    return CrawlResult(entries=entries, errors=errors)


async def async_crawl_dynamic(
    root_url: str,
    board_patterns: Iterable[str],
    max_links: int = 250,
    timeout_ms: int = 30_000,
    stream_handler: Optional[Callable[[dict], None]] = None,
) -> CrawlResult:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        msg = "Playwright 미설치(비동기): `pip install playwright` 이 후 `playwright install` 필요"
        logger.warning(msg)
        return CrawlResult(entries=[], errors=[msg], fallback_used=True)

    root_url = ensure_scheme(root_url)
    base_host = urlparse(root_url).netloc
    patterns = [p.lower() for p in board_patterns if p]
    collected: dict[str, dict] = {}
    errors: list[str] = []
    seen_hashes: set[str] = set()

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                ignore_https_errors=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="ko-KR",
            )
            page = await context.new_page()
            await page.goto(root_url, wait_until="networkidle", timeout=timeout_ms)
            await page.wait_for_timeout(2000)
            html_content = await page.content()
            await context.close()
            await browser.close()
    except Exception as exc:
        errors.append(f"Playwright 처리 실패: {exc}")
        return CrawlResult(entries=[], errors=errors, fallback_used=True)

    soup = BeautifulSoup(html_content, "html.parser")
    normalized_root = normalize_url(root_url)
    if normalized_root:
        canonical_root = canonicalize_url_for_dedup(normalized_root)
        if canonical_root:
            canonical_key = canonical_hash(canonical_root)
            if canonical_key not in seen_hashes:
                seen_hashes.add(canonical_key)
                try:
                    root_is_board = detect_board_in_soup(soup, normalized_root, patterns) or is_board_url(
                        normalized_root, patterns
                    )
                except Exception:
                    root_is_board = is_board_url(normalized_root, patterns)
                entry = {
                    "url": normalized_root,
                    "title": extract_title(soup, normalized_root, text or "") or normalized_root,
                    "is_board": root_is_board,
                    "canonical_url": canonical_root,
                    "canonical_hash": canonical_key,
                }
                collected[normalized_root] = entry
                if stream_handler:
                    try:
                        stream_handler(entry)
                    except Exception:
                        logger.debug("stream_handler failed for %s", normalized_root, exc_info=True)

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        candidate = normalize_url(urljoin(root_url, href))
        canonical_candidate = canonicalize_url_for_dedup(candidate)
        if not candidate or not canonical_candidate:
            continue
        canonical_key = canonical_hash(canonical_candidate)
        if canonical_key in seen_hashes:
            continue
        if not same_domain(candidate, base_host):
            continue
        text = anchor.get_text(strip=True) or candidate
        anchor_text_lower = (anchor.get_text(" ", strip=True) or "").lower()
        parent_text = ""
        if anchor.parent:
            parent_text = anchor.parent.get_text(" ", strip=True).lower()
        is_board_anchor = is_board_url(candidate, patterns) or any(
            kw in anchor_text_lower for kw in ("게시판", "목록", "글", "게시물")
        ) or any(kw in parent_text for kw in ("게시판", "목록", "작성일", "작성자", "조회수"))
        entry = {
            "url": candidate,
            "title": text,
            "is_board": is_board_anchor,
            "canonical_url": canonical_candidate,
            "canonical_hash": canonical_key,
        }
        seen_hashes.add(canonical_key)
        collected[candidate] = entry
        if stream_handler:
            try:
                stream_handler(entry)
            except Exception:
                logger.debug("stream_handler failed for %s", candidate, exc_info=True)
        if len(collected) >= max_links:
            break

    return CrawlResult(entries=list(collected.values()), errors=errors, fallback_used=True)


async def async_build_sitemap_payload(
    root_url: str,
    board_patterns: List[str],
    sitemap_dir: Path,
    stream_handler: Optional[Callable[[dict], None]] = None,
) -> dict:
    logger.info("async 사이트맵 생성 시작: root=%s", root_url)
    # wake 이벤트(프론트 깨우기): 비차단 방식으로 발행
    try:
        asyncio.create_task(publish_wake_event(root_url))
    except Exception:
        logger.debug("sitemap wake publish failed", exc_info=True)
    start_ts = monotonic()
    stream_publish_count = 0
    stream_last_url: str | None = None
    effective_stream_handler = stream_handler
    if stream_handler:
        original_stream_handler = stream_handler

        def _tracked_stream(entry: dict) -> None:
            nonlocal stream_publish_count, stream_last_url
            stream_publish_count += 1
            stream_last_url = entry.get("url")
            if stream_publish_count % 50 == 0:
                logger.debug(
                    "sitemap stream progress - entries=%d last=%s",
                    stream_publish_count,
                    stream_last_url,
                )
            original_stream_handler(entry)

        effective_stream_handler = _tracked_stream
    # 기존 sitemap 파일이 존재하면 새로 크롤링하지 않고 기존 파일을 사용하도록 함
    try:
        root_domain = urlparse(root_url).netloc or root_url
        label = slugify(root_domain)
        sitemap_dir.mkdir(parents=True, exist_ok=True)
        pattern = f"{label}_*.json"
        candidates = list(sitemap_dir.glob(pattern))
        if candidates:
            latest_json = max(candidates, key=lambda p: p.stat().st_mtime)
            try:
                entries = json.loads(latest_json.read_text(encoding="utf-8"))
                errors: list[str] = []
                fallback = False
                unique_urls = {entry["url"] for entry in entries if entry.get("url")}
                md_path = latest_json.with_suffix(".md")
                html_path = latest_json.with_suffix(".html")
                md_text = md_path.read_text(encoding="utf-8") if md_path.exists() else build_markdown(entries, f"{root_domain} 사이트맵")
                html_text = html_path.read_text(encoding="utf-8") if html_path.exists() else build_html(entries, f"{root_domain} 사이트맵")
                output_files = {
                    "json": latest_json,
                    "md": md_path,
                    "html": html_path,
                    "md_text": md_text,
                    "html_text": html_text,
                }
                total_duration = monotonic() - start_ts
                logger.info("기존 사이트맵 사용 - file=%s entries=%d", latest_json.name, len(entries))
                if stream_handler:
                    logger.info(
                        "sitemap stream publish summary - 전송된 항목=%d last=%s",
                        stream_publish_count,
                        stream_last_url or "n/a",
                    )
                return {
                    "entries": entries,
                    "errors": errors,
                    "files": output_files,
                    "fallback_used": fallback,
                    "debug_info": {
                        "timings": {
                            "write": 0.0,
                            "total": round(total_duration, 3),
                        },
                        "counts": {
                            "after_dedup": len(entries),
                            "unique_urls": len(unique_urls),
                            "errors": len(errors),
                            "streamed_entries": stream_publish_count,
                        },
                        "stream_last_url": stream_last_url,
                        "dedup_removed": 0,
                    },
                    "shortcut_links": {
                        "json": output_files["json"].as_posix(),
                        "md": output_files["md"].as_posix(),
                        "html": output_files["html"].as_posix(),
                    },
                    "md_preview": output_files["md_text"],
                    "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                }
            except Exception:
                logger.debug("existing sitemap 읽기 실패 - 계속 크롤링 수행", exc_info=True)

    except Exception:
        logger.debug("existing sitemap 검사 중 예외 발생 - 계속 크롤링 수행", exc_info=True)

    static_result = await async_crawl_static(root_url, board_patterns, stream_handler=effective_stream_handler)
    static_entries_count = len(static_result.entries)
    entries = static_result.entries
    errors = list(static_result.errors)
    fallback = False
    unique_urls = {entry["url"] for entry in entries if entry.get("url")}

    static_duration = 0.0
    if len(unique_urls) <= 5:
        logger.info("정적 결과 부족 -> async Playwright 동적 폴백 실행")
        dynamic_result = await async_crawl_dynamic(root_url, board_patterns, stream_handler=effective_stream_handler)
        dynamic_entries_count = len(dynamic_result.entries)
        errors.extend(dynamic_result.errors)
        if dynamic_result.entries:
            entries = dynamic_result.entries
            fallback = True
            logger.info("동적 크롤링으로 보완된 항목 수: %d", len(entries))
        else:
            logger.warning("동적 폴백 시도했으나 결과 없음")

    entries_before_dedup = len(entries)
    entries = deduplicate(entries)
    entries_after_dedup = len(entries)

    write_start = monotonic()
    output_files = await asyncio.to_thread(write_sitemap_files, entries, urlparse(root_url).netloc or root_url, sitemap_dir)
    write_duration = monotonic() - write_start

    total_duration = monotonic() - start_ts
    logger.info(
        "async 사이트맵 생성 완료 - entries=%d fallback=%s errors=%d duration=%.2fs",
        len(entries),
        fallback,
        len(errors),
        total_duration,
    )
    if stream_handler:
        logger.info(
            "sitemap stream publish summary - 전송된 항목=%d last=%s",
            stream_publish_count,
            stream_last_url or "n/a",
        )

    return {
        "entries": entries,
        "errors": errors,
        "files": output_files,
        "fallback_used": fallback,
        "debug_info": {
            "timings": {
                "write": round(write_duration, 3),
                "total": round(total_duration, 3),
            },
            "counts": {
                "after_dedup": len(entries),
                "unique_urls": len(unique_urls),
                "errors": len(errors),
                "streamed_entries": stream_publish_count,
            },
            "stream_last_url": stream_last_url,
            "dedup_removed": max(entries_before_dedup - entries_after_dedup, 0),
        },
        "shortcut_links": {
            "json": output_files["json"].as_posix(),
            "md": output_files["md"].as_posix(),
            "html": output_files["html"].as_posix(),
        },
        "md_preview": output_files["md_text"],
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def slugify(value: str) -> str:
    lowered = value.lower()
    slug = re.sub(r"https?://", "", lowered)
    slug = re.sub(r"[^a-z0-9\-_\.]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug.strip("-")


def build_markdown(entries: list[dict], title: str) -> str:
    def add_to_tree(tree: dict, entry: dict) -> None:
        parsed = urlparse(entry["url"])
        path = parsed.path or "/"
        segments = [segment for segment in path.split("/") if segment]
        node = tree
        for segment in segments:
            node = node.setdefault(segment, {"children": {}, "entries": []})["children"]
        node.setdefault("entries", []).append(entry)

    def render_tree(node: dict, prefix: str = "") -> list[str]:
        lines: list[str] = []
        entry_lines = []
        for entry in node.get("entries", []):
            title_text = html.escape(entry["title"] or entry["url"])
            board_text = "예" if entry["is_board"] else "아니오"
            entry_lines.append(f"{prefix}├── {title_text} (게시판: {board_text})")
        lines.extend(entry_lines)
        for segment in sorted(k for k in node.keys() if k != "entries"):
            sub = node[segment]
            lines.append(f"{prefix}├── `{segment}/`")
            lines.extend(render_tree(sub["children"], prefix + "│   "))
        return lines

    tree: dict = {}
    for entry in entries:
        add_to_tree(tree, entry)

    rendered = [f"# {title}", "", "홈페이지 구조 트리", ""]
    rendered.extend(render_tree({"root": {"children": tree, "entries": []}}))
    if not entries:
        rendered.append("_(사이트맵 항목이 없습니다.)_")
    return "\n".join(rendered)


def build_html(entries: list[dict], title: str) -> str:
    safe_title = html.escape(title)
    rows = [
        "<!doctype html>",
        '<html lang="ko">',
        "<head>",
        "  <meta charset=\"utf-8\"/>",
        f"  <title>{safe_title}</title>",
        "  <style>",
        "    body{font-family:Segoe UI,system-ui;-webkit-font-smoothing:antialiased;margin:0;padding:1rem;background:#f4f4f7;color:#1f1f1f;}",
        "    .entry{padding:1rem;border-bottom:1px solid rgba(0,0,0,.1);}",
        "    .title{font-weight:600;}",
        "    .url{color:#2563eb;word-break:break-word;}",
        "  </style>",
        "</head>",
        "<body>",
        f"  <h1>{safe_title}</h1>",
        "  <ol>",
    ]
    for entry in entries:
        board_flag = "예" if entry["is_board"] else "아니오"
        rows.extend(
            [
                "    <li class=\"entry\">",
                f"      <div class=\"title\">{html.escape(entry['title'] or entry['url'])}</div>",
                f"      <div class=\"url\"><a href=\"{entry['url']}\" target=\"_blank\">{entry['url']}</a></div>",
                f"      <div>게시판: {board_flag}</div>",
                "    </li>",
            ]
        )
    rows.extend(["  </ol>", "</body>", "</html>"])
    return "\n".join(rows)


def write_sitemap_files(entries: list[dict], root_domain: str, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    label = slugify(root_domain or "sitemap")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{label}_{timestamp}"
    json_path = output_dir / f"{base}.json"
    md_path = output_dir / f"{base}.md"
    html_path = output_dir / f"{base}.html"
    json_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    md_text = build_markdown(entries, f"{root_domain} 사이트맵")
    html_text = build_html(entries, f"{root_domain} 사이트맵")
    md_path.write_text(md_text, encoding="utf-8")
    html_path.write_text(html_text, encoding="utf-8")
    logger.info("사이트맵 파일 쓰기 완료: %s", output_dir.as_posix())
    logger.info(" - JSON: %s", json_path.name)
    logger.info(" - MD:   %s", md_path.name)
    logger.info(" - HTML: %s", html_path.name)

    return {
        "json": json_path,
        "md": md_path,
        "html": html_path,
        "md_text": md_text,
        "html_text": html_text,
    }


def deduplicate(entries: List[dict]) -> List[dict]:
    seen: set[str] = set()
    deduped: List[dict] = []
    for entry in entries:
        canonical = entry.get("canonical_url")
        if not canonical:
            canonical = canonicalize_url_for_dedup(entry.get("url") or "")
        if not canonical:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        deduped.append(entry)
    return deduped


async def start_stream_processor(
    queue: "asyncio.Queue[str]",
    worker_count: int = 10,
    per_host_limit: int = 3,
    process_callback: Optional[Callable[[str, int, str, float], object]] = None,
    session: Optional[aiohttp.ClientSession] = None,
) -> tuple[list[asyncio.Task], bool, aiohttp.ClientSession]:
    """
    Start a pool of worker tasks that consume URLs from `queue` and fetch them concurrently.

    Returns (tasks, session_created_flag, session).
    - `process_callback` may be an async function or sync callable with signature
      (url, status, text, duration_seconds).
    - If `session` is not provided, a ClientSession is created and the returned
      session_created_flag will be True (caller should close it via stop_stream_processor).
    """
    print(f"===================test5===================")
    session_created = False
    if session is None:
        connector = TCPConnector(limit=max(2, worker_count), limit_per_host=max(2, per_host_limit))
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=ClientTimeout(total=SITEMAP_REQUEST_TOTAL_TIMEOUT),
            headers={"User-Agent": "Mozilla/5.0 (compatible; Sitemap/1.0)"},
        )
        session_created = True

    per_host_semaphores: dict[str, asyncio.Semaphore] = {}

    async def _process_url(url: str) -> None:
        normalized = normalize_url(url)
        if not normalized:
            logger.debug("stream processor - invalid url: %s", url)
            return
        host = urlparse(normalized).netloc
        sem = per_host_semaphores.setdefault(host, asyncio.Semaphore(per_host_limit))
        async with sem:
            req_start = monotonic()
            try:
                async with session.get(normalized) as resp:
                    status = resp.status
                    skip, reason = _should_skip_response(resp)
                    if skip:
                        logger.debug("stream skip: %s reason=%s", normalized, reason)
                        await resp.release()
                        return
                    text, read_err = await _read_limited_text(resp, SITEMAP_MAX_RESPONSE_BYTES)
                    if read_err:
                        logger.debug("stream skip: %s reason=%s", normalized, read_err)
                        return
                req_dur = monotonic() - req_start
                # 기존 async_crawl_static에서 사용한 임계값과 동일한 기본값(2.0s)을 사용
                if req_dur > SITEMAP_SLOW_RESPONSE_THRESHOLD:
                    logger.info("느린 응답 감지(stream): url=%s duration=%.2fs", normalized, req_dur)
                if process_callback:
                    try:
                        result = process_callback(normalized, status, text, req_dur)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        # callback 실패는 전체 흐름을 방해하지 않도록 로그만 남김
                        logger.debug("stream processor callback failed for %s", normalized, exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("stream processor fetch failed: %s err=%s", normalized, exc, exc_info=True)

    async def worker() -> None:
        while True:
            try:
                url = await queue.get()
            except asyncio.CancelledError:
                break
            try:
                await _process_url(url)
            finally:
                try:
                    queue.task_done()
                except Exception:
                    pass

    tasks = [asyncio.create_task(worker(), name=f"sitemap-stream-worker-{i}") for i in range(worker_count)]
    return tasks, session_created, session


async def stop_stream_processor(tasks: list[asyncio.Task], session_created: bool, session: Optional[aiohttp.ClientSession]) -> None:
    """
    Stop the worker tasks and close the session if it was created by start_stream_processor.
    """
    for t in tasks:
        try:
            t.cancel()
        except Exception:
            pass
    await asyncio.gather(*tasks, return_exceptions=True)
    if session_created and session:
        try:
            await session.close()
        except Exception:
            pass


# Removed Flask-based HTML UI to allow running as a standalone CLI/library module.


def parse_board_input(raw: str) -> List[str]:
    if not raw:
        return []
    segments = [part.strip() for part in raw.replace("\n", ",").split(",")]
    return [segment for segment in segments if segment]


def ensure_root_url(raw: str) -> str:
    cleaned = raw.strip()
    if not cleaned:
        raise ValueError("루트 URL을 입력해야 합니다.")
    parsed = urlparse(cleaned)
    if not parsed.scheme:
        cleaned = f"https://{cleaned}"
    return cleaned


def build_sitemap_payload(
    root_url: str,
    board_patterns: List[str],
    sitemap_dir: Path,
    stream_handler: Optional[Callable[[dict], None]] = None,
) -> dict:
    logger.info("사이트맵 생성 시작: root=%s", root_url)
    logger.info("======= test01 =======")
    logger.info("게시판 패턴: %s", ", ".join(board_patterns[:20]))
    logger.info(
        "build_sitemap_payload 입력 - root_url=%s, sitemap_dir=%s, num_patterns=%d",
        root_url,
        str(sitemap_dir),
        len(board_patterns),
    )
    try:
        parsed_root = urlparse(root_url)
        logger.info(
            "parsed root - scheme=%s, netloc=%s, path=%s",
            parsed_root.scheme,
            parsed_root.netloc,
            parsed_root.path,
        )
    except Exception as exc:
        logger.info("root_url 파싱 실패: %s", exc)
    start_ts = monotonic()
    stream_publish_count = 0
    stream_last_url: str | None = None
    effective_stream_handler = stream_handler
    if stream_handler:
        original_stream_handler = stream_handler

        def _tracked_stream(entry: dict) -> None:
            nonlocal stream_publish_count, stream_last_url
            stream_publish_count += 1
            stream_last_url = entry.get("url")
            if stream_publish_count % 50 == 0:
                logger.debug(
                    "sitemap stream progress - entries=%d last=%s",
                    stream_publish_count,
                    stream_last_url,
                )
            original_stream_handler(entry)

        effective_stream_handler = _tracked_stream
    static_start = monotonic()
    static_result = crawl_static(root_url, board_patterns, stream_handler=effective_stream_handler)
    static_duration = monotonic() - static_start
    static_entries_count = len(static_result.entries)
    entries = static_result.entries
    errors = list(static_result.errors)
    fallback = False
    unique_urls = {entry["url"] for entry in entries if entry.get("url")}

    logger.info(
        "정적 크롤링 결과: 발견=%d, 오류=%d, duration=%.2fs",
        len(unique_urls),
        len(errors),
        static_duration,
    )

    if len(unique_urls) <= 5:
        logger.info("정적 결과 부족 -> Playwright 동적 폴백 실행")
        dynamic_start = monotonic()
        dynamic_result = crawl_dynamic(root_url, board_patterns, stream_handler=effective_stream_handler)
        dynamic_duration = monotonic() - dynamic_start
        dynamic_entries_count = len(dynamic_result.entries)
        errors.extend(dynamic_result.errors)
        if dynamic_result.entries:
            entries = dynamic_result.entries
            fallback = True
            logger.info(
                "동적 크롤링으로 보완된 항목 수: %d, duration=%.2fs",
                len(entries),
                dynamic_duration,
            )
        else:
            logger.warning("동적 폴백 시도했으나 결과 없음")
    else:
        dynamic_duration = 0.0
        dynamic_entries_count = 0

    entries_before_dedup = len(entries)
    entries = deduplicate(entries)
    entries_after_dedup = len(entries)
    logger.info(
        "dedup 처리 - before=%d after=%d removed=%d",
        entries_before_dedup,
        entries_after_dedup,
        max(entries_before_dedup - entries_after_dedup, 0),
    )
    write_start = monotonic()
    output_files = write_sitemap_files(entries, urlparse(root_url).netloc or root_url, sitemap_dir)
    write_duration = monotonic() - write_start

    logger.info("사이트맵 파일 생성 위치: %s", output_files["json"].parent.as_posix())
    logger.info("생성된 파일: %s, %s, %s", output_files["json"].name, output_files["md"].name, output_files["html"].name)
    logger.info("다음 작업: HTML 파일을 열어 미리보기 확인하거나 JSON을 파이프라인에 전달하세요.")
    total_duration = monotonic() - start_ts
    logger.info(
        "사이트맵 생성 완료 - entries=%d fallback=%s errors=%d duration=%.2fs",
        len(entries),
        fallback,
        len(errors),
        total_duration,
    )
    if stream_handler:
        logger.info(
            "sitemap stream publish summary - 전송된 항목=%d last=%s",
            stream_publish_count,
            stream_last_url or "n/a",
        )

    return {
        "entries": entries,
        "errors": errors,
        "files": output_files,
        "fallback_used": fallback,
        "debug_info": {
            "timings": {
                "static": round(static_duration, 3),
                "dynamic": round(dynamic_duration, 3),
                "write": round(write_duration, 3),
                "total": round(total_duration, 3),
            },
            "counts": {
                "static_entries": static_entries_count,
                "dynamic_entries": dynamic_entries_count,
                "after_dedup": len(entries),
                "unique_urls": len(unique_urls),
                "errors": len(errors),
                "streamed_entries": stream_publish_count,
            },
            "stream_last_url": stream_last_url,
            "dedup_removed": max(entries_before_dedup - entries_after_dedup, 0),
        },
        "shortcut_links": {
            "json": output_files["json"].as_posix(),
            "md": output_files["md"].as_posix(),
            "html": output_files["html"].as_posix(),
        },
        "md_preview": output_files["md_text"],
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


async def crawl_sitemap_entries(
    root_url: str,
    board_patterns: Iterable[str],
    stream_queue: asyncio.Queue[dict] | None = None,
) -> Tuple[List[dict], List[str], bool]:
    """
    Async wrapper to generate sitemap entries using the synchronous build_sitemap_payload.
    Returns (entries, errors, fallback_used).
    """
    sitemap_dir = _default_sitemap_dir()
    # run blocking work in threadpool
    stream_handler: Optional[Callable[[dict], None]] = None
    if stream_queue is not None:
        loop = asyncio.get_running_loop()

        def _thread_stream(entry: dict) -> None:
            try:
                asyncio.run_coroutine_threadsafe(stream_queue.put(entry), loop)
            except Exception:
                logger.debug("sitemap stream enqueue failed", exc_info=True)

        stream_handler = _thread_stream
    # Prefer the async implementation when available to avoid blocking the event loop.
    try:
        result = await async_build_sitemap_payload(root_url, list(board_patterns), sitemap_dir, stream_handler)
    except Exception:
        # Fallback to synchronous version if async implementation fails for any reason.
        result = await asyncio.to_thread(
            build_sitemap_payload,
            root_url,
            list(board_patterns),
            sitemap_dir,
            stream_handler,
        )
    return result.get("entries", []), result.get("errors", []), bool(result.get("fallback_used", False))


def build_context(result: dict | None, form_values: dict | None) -> dict:
    values = form_values or {"root_domain": "", "board_patterns": ", ".join(DEFAULT_BOARD_PATTERNS)}
    return {
        "form_values": values,
        "board_defaults": ", ".join(DEFAULT_BOARD_PATTERNS),
        "result": result,
        "error_message": None,
    }


def _default_sitemap_dir() -> Path:
    d = Path("static") / "sitemaps"
    d.mkdir(parents=True, exist_ok=True)
    return d


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Generate sitemap (static + optional Playwright fallback).")
    parser.add_argument("root_url", help="Root URL to crawl (e.g. https://example.com)")
    parser.add_argument("-b", "--board-patterns", default=", ".join(DEFAULT_BOARD_PATTERNS), help="Comma-separated additional board patterns")
    parser.add_argument("-o", "--output-dir", default=_default_sitemap_dir().as_posix(), help="Output directory for generated sitemap files")
    args = parser.parse_args()

    try:
        root_url = ensure_root_url(args.root_url)
    except ValueError as exc:
        logger.error("루트 URL 오류: %s", exc)
        raise

    board_patterns = DEFAULT_BOARD_PATTERNS + parse_board_input(args.board_patterns)
    sitemap_dir = Path(args.output_dir)
    sitemap_dir.mkdir(parents=True, exist_ok=True)

    result = build_sitemap_payload(root_url, board_patterns, sitemap_dir)
    logger.info("생성 완료: 총=%d, 파일=%s", len(result["entries"]), result["files"]["json"].as_posix())
    if result["errors"]:
        logger.warning("오류 로그(%d): %s", len(result["errors"]), "; ".join(result["errors"][:5]))
    print("JSON:", result["files"]["json"].as_posix())
    print("MD:  ", result["files"]["md"].as_posix())
    print("HTML:", result["files"]["html"].as_posix())


if __name__ == "__main__":
    main()
