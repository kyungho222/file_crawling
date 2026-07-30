#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
단일 게시글 URL → HWP 첨부 처리.

【기본: 실제 크롤러와 동일한 파일 파이프라인】
  FileDownloadWorkflow(첨부 전용, 기본 학습 ON):
  - HTML: _fetch_html_static → (필요 시) _fetch_html_playwright
  - 첨부: _extract_attachment_links_generic → HWP만 선별
  - 투입: _enqueue_file_downloads (collection 큐 → download_worker → save → study_worker)
  - 종료 대기: _finalize_stats (큐 join + _shutdown_file_pipeline)

  전역 워커 풀(GLOBAL_WORKER_POOL)과 섞이지 않도록 use_global_pool=False 고정.

【--download-only】
  파이프라인 없이 aiohttp로만 HWP 저장 (DB/브라우저 워커 미사용). 네트워크만 검증할 때.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("sync_hwp_learning")


def get_workflow():
    from backend.file.file_download_workflow import FileDownloadWorkflow

    return FileDownloadWorkflow()


def clean_filename(filename: str) -> str:
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "_")
    return filename.strip() or "unnamed_file"


def _filter_hwp_attachments(attachments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """_extract_attachment_links_generic 결과 중 HWP만 (href/name 유지 → _enqueue_file_downloads 호환)."""
    out: List[Dict[str, Any]] = []
    seen: set = set()
    handlers = (
        "filedown.do",
        "filedownload",
        "atchfileid",
        "atchfile",
        "download.do",
        "filedown",
    )
    for a in attachments or []:
        href = (a.get("href") or "").strip()
        name = (a.get("name") or "").strip()
        if not href:
            continue
        lh, ln = href.lower(), name.lower()
        path_only = lh.split("?", 1)[0]
        url_has_ext = path_only.endswith((".hwp", ".hwpx"))
        name_hwp = ln.endswith((".hwp", ".hwpx")) or ".hwp" in ln or ".hwpx" in ln
        handler = any(h in lh for h in handlers)
        if url_has_ext or (handler and name_hwp) or name_hwp:
            key = (href, name)
            if key not in seen:
                seen.add(key)
                out.append({"href": href, "name": name})
    return out


def _resolve_download_href(href: str, post_url: str) -> str:
    """--download-only 전용. 파이프라인 모드는 워커가 enqueue 시점과 동일하게 JS 처리."""
    from utils.url import extract_download_url_from_js

    h = (href or "").strip()
    if not h:
        return ""
    if h.lower().startswith("javascript:"):
        resolved = (extract_download_url_from_js(h, post_url) or "").strip()
        if resolved:
            return resolved
        m = re.search(r"https?://[^\s'\"<>)]+", h)
        return m.group(0).rstrip(");,") if m else h
    return h


def _download_headers(post_url: str) -> Dict[str, str]:
    from urllib.parse import urlparse

    h = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }
    if post_url:
        h["Referer"] = post_url
        try:
            p = urlparse(post_url)
            if p.scheme and p.netloc:
                h["Origin"] = f"{p.scheme}://{p.netloc}"
        except Exception:
            pass
    return h


async def _download_to_path(
    session: aiohttp.ClientSession,
    file_url: str,
    save_path: str,
    *,
    post_url: str,
) -> bool:
    try:
        async with session.get(
            file_url,
            headers=_download_headers(post_url),
            timeout=aiohttp.ClientTimeout(total=120),
            allow_redirects=True,
        ) as response:
            if response.status != 200:
                logger.error("Download failed | status=%s url=%s", response.status, file_url[:200])
                return False
            os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(await response.read())
            return True
    except Exception as e:
        logger.error("Download error | url=%s err=%s", file_url[:200], e)
        return False


async def _fetch_detail_html(workflow: Any, url: str) -> Optional[str]:
    html = await workflow._fetch_html_static(url, timeout_sec=30.0)
    if not html:
        logger.info("Static HTML empty; trying Playwright...")
        try:
            html = await workflow._fetch_html_playwright(url)
        except Exception as e:
            logger.warning("Playwright fetch failed: %s", e)
            html = None
    return html


async def _run_download_only(
    workflow: Any,
    url: str,
    title: str,
    hwp_list: List[Dict[str, Any]],
) -> None:
    save_base = os.path.join(project_root, "downloads", "hwp_sync", clean_filename(title))
    os.makedirs(save_base, exist_ok=True)
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for idx, attach in enumerate(hwp_list):
            raw_href = attach.get("href") or ""
            file_url = _resolve_download_href(raw_href, url)
            if not file_url or file_url.lower().startswith("javascript:"):
                logger.error("[%s] unresolved href=%s", idx + 1, raw_href[:120])
                continue
            raw_name = attach.get("name") or f"attachment_{idx}.hwp"
            file_name = clean_filename(raw_name)
            if not file_name.lower().endswith((".hwp", ".hwpx")):
                file_name += ".hwp"
            local_path = os.path.join(save_base, file_name)
            logger.info("[%s/%s] download-only → %s", idx + 1, len(hwp_list), local_path)
            if await _download_to_path(session, file_url, local_path, post_url=url):
                logger.info("Saved: %s", local_path)


async def _run_production_pipeline(
    workflow: Any,
    url: str,
    title: str,
    reg_date_val: str,
    hwp_list: List[Dict[str, Any]],
    *,
    sync_after_download: bool,
) -> None:
    """
    FileDownloadWorkflow: _enqueue_file_downloads → WorkerManager(download/study) → _finalize_stats
    """
    enqueued = await workflow._enqueue_file_downloads(
        post_url=url,
        board_url="",
        reg_date=reg_date_val or None,
        attachments=hwp_list,
        author=title,
        department=None,
        author_kind=None,
        author_raw=None,
        department_raw=None,
        contact_phone=None,
        view_count=None,
        sync_after_download=sync_after_download,
    )
    logger.info(
        "[pipeline] _enqueue_file_downloads finished | enqueued=%s job_id=%s",
        enqueued,
        getattr(workflow, "job_id", ""),
    )
    await workflow._finalize_stats()
    try:
        logger.info("[pipeline] final stats=%s", workflow.get_stats())
    except Exception:
        pass


async def main(
    url: str,
    *,
    chat_bot_id: str,
    db_name: str,
    download_only: bool,
    sync_after_download: bool,
    access_url: Optional[str],
) -> None:
    logger.info(
        "sync_hwp_learning | url=%s download_only=%s sync=%s",
        url,
        download_only,
        sync_after_download,
    )

    workflow = get_workflow()
    # 실제 단일 작업과 격리: 전역 풀에 붙지 않음 (board_content_workflow._ensure_file_pipeline 과 정합)
    workflow.use_global_pool = False
    workflow.job_id = str(uuid.uuid4())
    workflow.chat_bot_id = chat_bot_id
    workflow.db_name = db_name
    workflow.sync_after_download = sync_after_download

    if access_url:
        workflow.access_url = access_url.strip()
    else:
        try:
            p = urlparse(url)
            if p.scheme and p.netloc:
                workflow.access_url = f"{p.scheme}://{p.netloc}"
        except Exception:
            workflow.access_url = None
    try:
        workflow.server_domain = (urlparse(url).netloc or "").split(":")[0] or None
    except Exception:
        workflow.server_domain = None

    try:
        html = await _fetch_detail_html(workflow, url)
        if not html:
            logger.error("Failed to fetch HTML.")
            return

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        title = workflow._extract_board_title(soup, url=url, html=html)
        logger.info("Title: %s", title)

        reg_date_dt = workflow._extract_board_reg_date(soup, html=html, url=url)
        reg_date_val = (
            reg_date_dt.strftime("%Y-%m-%d %H:%M:%S") if reg_date_dt else ""
        )

        attachments = workflow._extract_attachment_links_generic(html, base_url=url)
        logger.info("Attachments (generic): %s", len(attachments))

        if not attachments:
            logger.info("No attachments; retry Playwright...")
            try:
                html2 = await workflow._fetch_html_playwright(url)
            except Exception as e:
                logger.warning("Playwright retry failed: %s", e)
                html2 = None
            if html2:
                soup = BeautifulSoup(html2, "html.parser")
                title = workflow._extract_board_title(soup, url=url, html=html2)
                reg_date_dt = workflow._extract_board_reg_date(soup, html=html2, url=url)
                reg_date_val = (
                    reg_date_dt.strftime("%Y-%m-%d %H:%M:%S") if reg_date_dt else ""
                )
                attachments = workflow._extract_attachment_links_generic(html2, base_url=url)
                logger.info("After Playwright: attachments=%s", len(attachments))

        hwp_list = _filter_hwp_attachments(attachments)
        if not hwp_list:
            if attachments:
                logger.warning(
                    "No HWP. Sample: %s",
                    [(a.get("name"), (a.get("href") or "")[:100]) for a in attachments[:5]],
                )
            else:
                logger.warning("No attachments.")
            return

        logger.info("HWP candidates: %s", len(hwp_list))

        if download_only:
            await _run_download_only(workflow, url, title, hwp_list)
        else:
            await _run_production_pipeline(
                workflow,
                url,
                title,
                reg_date_val,
                hwp_list,
                sync_after_download=sync_after_download,
            )

    except Exception:
        logger.exception("sync_hwp_learning failed")
    finally:
        # 파이프라인 모드에서 _finalize_stats 가 _shutdown_file_pipeline 호출함.
        # 워크플로 자체 HTML/Playwright 세션만 정리.
        try:
            await workflow._close_http_session()
        except Exception:
            pass
        try:
            await workflow._close_playwright()
        except Exception:
            pass


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="단일 URL HWP — 기본은 실제 파일 파이프라인(collection→download→study)",
    )
    p.add_argument("url", help="게시글 상세 URL")
    p.add_argument(
        "--chat-bot-id",
        default=os.getenv("SYNC_HWP_CHAT_BOT_ID", "manual_sync"),
        dest="chat_bot_id",
        help="워크플로 chat_bot_id (파일 메타·DB 중복 검사 등)",
    )
    p.add_argument(
        "--db-name",
        default=os.getenv("SYNC_HWP_DB_NAME", "asadal_crawling"),
        dest="db_name",
        help="워크플로 db_name",
    )
    p.add_argument(
        "--download-only",
        action="store_true",
        help="파이프라인 없이 로컬 다운로드만",
    )
    p.add_argument(
        "--sync",
        action="store_true",
        dest="sync_after_download",
        help="다운로드 후 웹서버 동기화(sync_file_to_webserver), 운영과 동일",
    )
    p.add_argument(
        "--access-url",
        default=None,
        dest="access_url",
        help="access_url 미지정 시 게시글 URL에서 scheme://host 로 추론",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(
        main(
            args.url,
            chat_bot_id=args.chat_bot_id,
            db_name=args.db_name,
            download_only=args.download_only,
            sync_after_download=args.sync_after_download,
            access_url=args.access_url,
        )
    )
