import logging
import asyncio
import time
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from backend.board.board_content_workflow import BoardContentWorkflow
from backend.board.chuncheon_contract import is_chuncheon_contract_detail_url
from backend.board.hscity_board import is_hscity_photo_url
from backend.board.playwright_renderer import shutdown_playwright_renderer
from backend.shared.pre_explored_url import _load_category_url_pattern_object, resolve_cate_for_detail_url
from db.mariadb_save_update import build_board_post_learn_list_input_preview
from utils.db_name import resolve_db_name
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.board.board_probe_endpoints")

router = APIRouter()


def _normalize_debug_title(title: Any) -> str:
    text = str(title or "").strip()
    if text and text.count(")") > text.count("("):
        close_idx = text.find(")")
        if 0 <= close_idx <= 24 and not text.startswith("("):
            text = f"({text}"
    return text


def _content_preview(text: Any, limit: int) -> str:
    source = str(text or "")
    if limit <= 0 or len(source) <= limit:
        return source
    return f"{source[:limit]}..."


def _safe_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


async def run_board_crawl_probe_readonly(body: Dict[str, Any]) -> Dict[str, Any]:
    """Run board detail parsing and final DB payload assembly without writes."""
    request_kind = str(body.get("colle") or body.get("content_type") or "").strip().lower()
    if request_kind == "file":
        return await run_file_crawl_probe_readonly(body)

    url = ensure_url_scheme(
        str(
            body.get("url")
            or body.get("detail_url")
            or body.get("contents_url")
            or body.get("target_url")
            or ""
        ).strip()
    )
    if not url:
        return {"status": "error", "message": "url is required"}

    db_name = resolve_db_name(body, default="dev_user")
    metadata = _safe_dict(body.get("metadata"))
    chat_bot_id = str(body.get("chat_bot_id") or metadata.get("chat_bot_id") or "").strip()
    try:
        max_content_chars = int(body.get("max_content_chars") or 2000)
    except Exception:
        max_content_chars = 2000
    max_content_chars = max(200, min(max_content_chars, 20000))

    workflow = BoardContentWorkflow()
    workflow.db_name = db_name
    workflow.chat_bot_id = chat_bot_id
    workflow.job_id = str(body.get("job_id") or f"board-probe-{int(time.time())}")
    workflow.enable_db_save = False
    workflow.enable_learning = False

    html = ""
    fetch_method = "static"
    try:
        if is_hscity_photo_url(url):
            html = await workflow._fetch_hscity_photo_html(url) or ""
            fetch_method = "static_hscity_photo"
            if not html:
                html = await workflow._fetch_html_static(url) or ""
                fetch_method = "static_hscity_fallback"
            if not html:
                html = await workflow._fetch_html_playwright(url) or ""
                fetch_method = "playwright_hscity_fallback"
        elif is_chuncheon_contract_detail_url(url):
            html = await workflow._fetch_html_playwright(url) or ""
            fetch_method = "playwright_chuncheon_contract"
            if not html:
                html = await workflow._fetch_html_static(url) or ""
                fetch_method = "static_chuncheon_fallback"
        else:
            html = await workflow._fetch_html_static(url) or ""
            if not html:
                html = await workflow._fetch_html_playwright(url) or ""
                fetch_method = "playwright_fallback"

        if not html:
            return {
                "status": "error",
                "message": "failed to fetch detail html",
                "url": url,
                "dry_run": True,
                "writes_disabled": True,
            }

        finalized = await workflow._build_final_runtime_parse(
            url=url,
            html=html,
            record_title=False,
        )
        if not finalized:
            return {
                "status": "error",
                "message": "failed to parse final board fields",
                "url": url,
                "dry_run": True,
                "writes_disabled": True,
            }

        category_rules = None
        category_error = ""
        finalized = _safe_dict(finalized)
        runtime_output = _safe_dict(finalized.get("runtime_output"))
        display = _safe_dict(runtime_output.get("display"))
        post_info = _safe_dict(runtime_output.get("post_info"))
        learning_result = _safe_dict(runtime_output.get("learning_result"))
        category = {
            "cate1": display.get("cate1") or post_info.get("cate1") or "",
            "cate2": display.get("cate2") or post_info.get("cate2") or "",
            "matched_type": "",
            "rules_loaded": False,
            "error": "",
        }
        if db_name and chat_bot_id:
            try:
                category_rules = await _load_category_url_pattern_object(
                    str(chat_bot_id),
                    str(db_name),
                    contents_url=url,
                    require_nonempty_rules=False,
                )
                resolved = resolve_cate_for_detail_url(url, category_rules) if category_rules else None
                if resolved:
                    cate1 = str(resolved[0] or "").strip()
                    cate2 = str(resolved[1] or "").strip()
                    category.update(
                        {
                            "cate1": cate1,
                            "cate2": cate2,
                            "matched_type": f"cate_match|{cate1}|{cate2}" if cate1 or cate2 else "",
                        }
                    )
            except Exception as exc:
                category_error = str(exc)
        category["rules_loaded"] = bool(category_rules)
        category["error"] = category_error

        final_content = str(finalized.get("clean_content") or "")
        attach_info = _safe_dict(finalized.get("attach_info"))
        learn_list_payload = build_board_post_learn_list_input_preview(post_info)
        if isinstance(learn_list_payload, dict):
            payload_subject = str(learn_list_payload.get("subject") or "").strip()
            final_title = _normalize_debug_title(finalized.get("clean_title"))
            if final_title and (not payload_subject or payload_subject == url):
                learn_list_payload["subject"] = final_title
                if not str(learn_list_payload.get("web_title") or "").strip():
                    learn_list_payload["web_title"] = final_title
            learn_list_payload["parsed_content"] = final_content

        return {
            "status": "ok",
            "dry_run": True,
            "writes_disabled": True,
            "write_guards": {
                "enable_db_save": False,
                "enable_learning": False,
                "insert": False,
                "update": False,
            },
            "url": url,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "fetch_method": fetch_method,
            "html_length": len(html),
            "title": _normalize_debug_title(finalized.get("clean_title")),
            "web_title": _normalize_debug_title(finalized.get("web_title")),
            "content": final_content,
            "parsed_content": final_content,
            "content_preview": _content_preview(final_content, max_content_chars),
            "content_length": len(final_content),
            "reg_date": str(finalized.get("reg_date_val") or ""),
            "author_info": _safe_dict(finalized.get("author_info")),
            "contact_info": _safe_dict(finalized.get("contact_info")),
            "attachment_summary": attach_info,
            "display": display,
            "post_info": post_info,
            "learning_result": learning_result,
            "learn_list_payload": learn_list_payload,
            "category": category,
            "counts": {
                "scan_count": 1,
                "content_length": len(final_content),
                "attachment_count": int(attach_info.get("attachment_count") or 0),
            },
        }
    finally:
        try:
            await workflow._close_http_session()
        except Exception:
            pass
        try:
            await workflow._close_playwright()
        except Exception:
            pass
        try:
            await shutdown_playwright_renderer()
        except Exception:
            pass


async def run_file_crawl_probe_readonly(body: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch one detail page and run the file attachment extractor without DB writes."""
    url = ensure_url_scheme(
        str(
            body.get("url")
            or body.get("detail_url")
            or body.get("contents_url")
            or body.get("target_url")
            or ""
        ).strip()
    )
    if not url:
        return {"status": "error", "message": "url is required"}

    db_name = resolve_db_name(body, default="dev_user")
    metadata = _safe_dict(body.get("metadata"))
    chat_bot_id = str(body.get("chat_bot_id") or metadata.get("chat_bot_id") or "").strip()
    try:
        fetch_timeout_sec = float(body.get("probe_fetch_timeout_sec") or body.get("fetch_timeout_sec") or 6.0)
    except Exception:
        fetch_timeout_sec = 6.0
    fetch_timeout_sec = max(1.0, min(fetch_timeout_sec, 20.0))
    try:
        playwright_timeout_sec = float(body.get("probe_playwright_timeout_sec") or 8.0)
    except Exception:
        playwright_timeout_sec = 8.0
    playwright_timeout_sec = max(1.0, min(playwright_timeout_sec, 20.0))
    disable_playwright = str(body.get("disable_playwright") or "").strip().lower() in {"1", "true", "yes", "on"}

    from backend.file.file_download_workflow import FileDownloadWorkflow

    workflow = FileDownloadWorkflow()
    workflow.db_name = db_name
    workflow.chat_bot_id = chat_bot_id
    workflow.job_id = str(body.get("job_id") or f"file-probe-{int(time.time())}")
    workflow.enable_db_save = False
    workflow.enable_learning = False
    workflow.file_pipeline_skip_learning = True

    html = ""
    fetch_method = "static"
    try:
        html = await workflow._fetch_html_static(url, timeout_sec=fetch_timeout_sec) or ""
        if not html and not disable_playwright:
            html = await asyncio.wait_for(
                workflow._fetch_html_playwright(url),
                timeout=playwright_timeout_sec,
            ) or ""
            fetch_method = "playwright_fallback"
        if not html:
            return {
                "status": "error",
                "message": "failed to fetch detail html",
                "url": url,
                "dry_run": True,
                "writes_disabled": True,
            }

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")
            title = workflow._extract_board_title(soup, url=url, html=html)
        except Exception:
            soup = None
            title = ""

        selector_profile = await workflow._get_selector_profile_for_detail(url=url, board_url="")
        try:
            from backend.file.file_download_workflow import _extract_file_author_info

            author_info = _extract_file_author_info(
                html,
                url=url,
                selector_profile=selector_profile,
            )
        except Exception:
            author_info = {}
        try:
            reg_date_dt = workflow._extract_board_reg_date(soup, html=html, url=url) if soup is not None else None
            reg_date = reg_date_dt.strftime("%Y-%m-%d %H:%M:%S") if reg_date_dt else ""
        except Exception:
            reg_date = ""
        probe_generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata_date = reg_date

        attachments = workflow._extract_attachment_links_generic(html, base_url=url)
        try:
            ajax_attachments = await workflow._extract_kcohesion_filelist_attachments(html, base_url=url)
        except Exception:
            ajax_attachments = []
        seen = {str(a.get("href") or "").strip().lower() for a in attachments if isinstance(a, dict)}
        for item in ajax_attachments:
            href = str((item or {}).get("href") or "").strip()
            key = href.lower()
            if href and key not in seen:
                attachments.append(item)
                seen.add(key)

        return {
            "status": "ok",
            "dry_run": True,
            "writes_disabled": True,
            "url": url,
            "source_url": url,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "fetch_method": fetch_method,
            "html_length": len(html),
            "title": _normalize_debug_title(title),
            "title_extraction": _safe_dict(getattr(workflow, "_last_title_extraction_debug", {}) or {}),
            "reg_date": reg_date,
            "author_info": _safe_dict(author_info),
            "metadata": {
                "source_url": url,
                "title": _normalize_debug_title(title),
                "reg_date": reg_date,
                "created_at": metadata_date,
                "updated_at": metadata_date,
                "content_created_at": metadata_date,
                "content_updated_at": metadata_date,
                "probe_generated_at": probe_generated_at,
                "author": (author_info or {}).get("author") or "",
                "content_author": (author_info or {}).get("author") or (author_info or {}).get("department") or "",
                "department": (author_info or {}).get("department") or "",
                "author_kind": (author_info or {}).get("author_kind") or "",
                "author_raw": (author_info or {}).get("author_raw") or "",
                "department_raw": (author_info or {}).get("department_raw") or "",
                "fetch_method": fetch_method,
                "html_length": len(html),
            },
            "attachment_summary": {
                "attachment_count": len(attachments or []),
                "attachments": attachments,
            },
            "attachments": attachments,
            "counts": {
                "scan_count": 1,
                "attachment_count": len(attachments or []),
                "file_attachment_found_count": len(attachments or []),
            },
        }
    finally:
        try:
            await workflow._close_http_session()
        except Exception:
            pass
        try:
            await workflow._close_playwright()
        except Exception:
            pass
        try:
            await shutdown_playwright_renderer()
        except Exception:
            pass


@router.post("/backend/board/crawl-probe")
async def board_crawl_probe_readonly(request: Request):
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        result = await run_board_crawl_probe_readonly(body)
        status_code = 200 if result.get("status") == "ok" else 400
        return JSONResponse(jsonable_encoder(result), status_code=status_code)
    except Exception as exc:
        logger.error("[BoardCrawlProbe] failed | err=%s", exc, exc_info=True)
        return JSONResponse(
            {
                "status": "error",
                "message": str(exc),
                "dry_run": True,
                "writes_disabled": True,
            },
            status_code=500,
        )
