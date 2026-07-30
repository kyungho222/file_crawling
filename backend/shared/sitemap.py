import asyncio
import html
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Sequence
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_BOARD_PATTERNS = ["board", "bbs", "forum", "thread", "list", "article"]


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
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    path = path.rstrip("/") if path != "/" else path
    normalized = parsed._replace(netloc=netloc, path=path, fragment="")
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


def crawl_static(
    root_url: str,
    board_patterns: Iterable[str],
    max_depth: int = 5,
    max_pages: int = 500,
    timeout: int = 10,
    max_concurrency: int = 12,
    per_host_limit: int = 5,
) -> CrawlResult:
    return _run_coroutine(
        _crawl_static_async(
            ensure_scheme(root_url),
            board_patterns,
            max_depth,
            max_pages,
            timeout,
            max_concurrency,
            per_host_limit,
        )
    )


def _run_coroutine(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


async def _crawl_static_async(
    root_url: str,
    board_patterns: Iterable[str],
    max_depth: int,
    max_pages: int,
    timeout: int,
    max_concurrency: int,
    per_host_limit: int,
) -> CrawlResult:
    root_url = ensure_scheme(root_url)
    normalized_root = normalize_url(root_url)
    if not normalized_root:
        return CrawlResult(entries=[], errors=[f"루트 URL 정규화 실패: {root_url}"])

    sorted_patterns = [p.lower() for p in board_patterns if p]
    reference_host = urlparse(normalized_root).netloc
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SitemapCrawler/1.0; +https://example.com/)",
    }
    connector = TCPConnector(limit=max_concurrency, limit_per_host=per_host_limit)
    timeout_cfg = ClientTimeout(total=timeout)

    queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
    entries: list[dict] = []
    errors: list[str] = []
    visited: set[str] = set()
    lock = asyncio.Lock()

    async def try_enqueue(candidate: str, depth: int) -> None:
        if depth > max_depth:
            return
        async with lock:
            if len(entries) >= max_pages or candidate in visited:
                return
            visited.add(candidate)
        await queue.put((candidate, depth))

    async def worker() -> None:
        while True:
            try:
                current_url, depth = await queue.get()
            except asyncio.CancelledError:
                break

            try:
                async with lock:
                    if len(entries) >= max_pages:
                        continue

                try:
                    async with session.get(current_url) as resp:
                        resp.raise_for_status()
                        html_text = await resp.text()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    errors.append(f"{current_url} 요청 실패: {exc}")
                    continue

                soup = BeautifulSoup(html_text, "html.parser")
                title = extract_title(soup, current_url, html_text) or current_url
                entry = {
                    "url": current_url,
                    "title": title,
                    "is_board": is_board_url(current_url, sorted_patterns),
                }
                async with lock:
                    if len(entries) < max_pages:
                        entries.append(entry)

                if depth < max_depth:
                    for anchor in soup.find_all("a", href=True):
                        href = anchor["href"]
                        absolute = urljoin(current_url, href)
                        candidate = normalize_url(absolute)
                        if not candidate:
                            continue
                        if not same_domain(candidate, reference_host):
                            continue
                        await try_enqueue(candidate, depth + 1)
            finally:
                queue.task_done()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg, headers=headers) as session:
        await try_enqueue(normalized_root, 0)
        tasks = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
        try:
            await queue.join()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return CrawlResult(entries=entries, errors=errors)


def crawl_dynamic(
    root_url: str,
    board_patterns: Iterable[str],
    max_links: int = 200,
    timeout: int = 30_000,
) -> CrawlResult:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        msg = "Playwright 미설치: `pip install playwright` 후 `playwright install` 필요"
        logger.warning(msg)
        return CrawlResult(entries=[], errors=[msg], fallback_used=True)

    root_url = ensure_scheme(root_url)
    parsed_root = urlparse(root_url)
    reference_host = parsed_root.netloc
    sorted_patterns = [p.lower() for p in board_patterns if p]
    collected: dict[str, dict] = {}
    errors: list[str] = []

    try:
        with sync_playwright() as pw:
            with pw.chromium.launch(headless=True) as browser:
                page = browser.new_page()
                try:
                    page.goto(root_url, wait_until="domcontentloaded", timeout=timeout)
                    html_content = page.content()
                finally:
                    page.close()
                soup = BeautifulSoup(html_content, "html.parser")

                root_title = extract_title(soup, root_url, html_content) or root_url
                normalized_root = normalize_url(root_url)
                if normalized_root:
                    collected[normalized_root] = {
                        "url": normalized_root,
                        "title": root_title,
                        "is_board": is_board_url(normalized_root, sorted_patterns),
                    }

                for anchor in soup.find_all("a", href=True):
                    href = anchor["href"]
                    absolute = urljoin(root_url, href)
                    candidate = normalize_url(absolute)
                    if not candidate or candidate in collected:
                        continue
                    if not same_domain(candidate, reference_host):
                        continue
                    title_text = anchor.get_text(strip=True) or candidate
                    collected[candidate] = {
                        "url": candidate,
                        "title": title_text,
                        "is_board": is_board_url(candidate, sorted_patterns),
                    }
                    if len(collected) >= max_links:
                        break
    except Exception as exc:  # pragma: no cover
        errors.append(f"Playwright 처리 실패: {exc}")

    entries = list(collected.values())
    return CrawlResult(entries=entries, errors=errors, fallback_used=True)


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"https?://", "", value)
    value = re.sub(r"[^a-z0-9\-_\.]", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def build_markdown(entries: list[dict], title: str) -> str:
    lines = [f"# {title}", ""]
    for entry in entries:
        entry_title = entry["title"] or entry["url"]
        lines.append(f"## {html.escape(entry_title)}")
        lines.append("")
        lines.append(f"- URL: {entry['url']}")
        lines.append(f"- 게시판: {'예' if entry['is_board'] else '아니오'}")
        lines.append("")
    return "\n".join(lines)


def build_html(entries: list[dict], title: str) -> str:
    safe_title = html.escape(title)
    lines = [
        "<!doctype html>",
        "<html lang=\"ko\">",
        "<head>",
        "  <meta charset=\"utf-8\"/>",
        f"  <title>{safe_title}</title>",
        "  <style>",
        "    body{font-family:Segoe UI,system-ui;-webkit-font-smoothing:antialiased;margin:0;padding:1rem;background:#f4f4f7;color:#1f1f1f;}",
        "    .wrapper{max-width:960px;margin:0 auto;}",
        "    .entry{padding:1rem;border-bottom:1px solid rgba(0,0,0,.1);}",
        "    .entry:last-child{border-bottom:none;}",
        "    .entry-title{font-size:1.1rem;font-weight:600;margin:0 0 .35rem;}",
        "    .entry-url a{color:#0066cc;word-break:break-word;}",
        "    .entry-meta{font-size:.9rem;color:#555;}",
        "    ol{counter-reset:item;list-style:none;margin:0;padding:0;}",
        "    ol li::before{content:counter(item) \".\";counter-increment:item;font-weight:700;margin-right:.75rem;color:#1a73e8;}",
        "  </style>",
        "</head>",
        "<body>",
        "  <div class=\"wrapper\">",
        f"    <h1>{safe_title}</h1>",
        "    <p>자동 생성 사이트맵</p>",
        "    <div>",
        "      <ol>",
    ]
    for entry in entries:
        entry_title = html.escape(entry["title"] or entry["url"])
        entry_url = html.escape(entry["url"])
        board_text = "예" if entry["is_board"] else "아니오"
        lines.extend(
            [
                "        <li class=\"entry\">",
                f"          <p class=\"entry-title\">{entry_title}</p>",
                f"          <p class=\"entry-url\"><a href=\"{entry_url}\" target=\"_blank\">{entry_url}</a></p>",
                f"          <p class=\"entry-meta\">게시판: {board_text}</p>",
                "        </li>",
            ]
        )
    lines.extend(
        [
            "      </ol>",
            "    </div>",
            "  </div>",
            "</body>",
            "</html>",
        ]
    )
    return "\n".join(lines)


def write_sitemap_files(
    entries: list[dict],
    root_domain: str,
    output_dir: Path,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    label = slugify(root_domain or "")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename_base = f"{label or 'sitemap'}_{timestamp}"
    json_path = output_dir / f"{filename_base}.json"
    md_path = output_dir / f"{filename_base}.md"
    html_path = output_dir / f"{filename_base}.html"

    json_text = json.dumps(entries, ensure_ascii=False, indent=2)
    json_path.write_text(json_text, encoding="utf-8")

    md_content = build_markdown(entries, f"{root_domain} 사이트맵")
    md_path.write_text(md_content, encoding="utf-8")

    html_content = build_html(entries, f"{root_domain} 사이트맵")
    html_path.write_text(html_content, encoding="utf-8")

    return {
        "json": json_path,
        "md": md_path,
        "html": html_path,
        "md_text": md_content,
        "html_text": html_content,
    }
