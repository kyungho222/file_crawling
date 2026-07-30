import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from backend.shared.board_header import (
    CrawlRequest as HeaderCrawlRequest,
    CrawlResponse,
    build_sitemap_markdown,
    crawl_header as header_crawl_handler,
    get_base_origin,
    get_sitemap_cache_paths,
    is_top_domain_url,
    store_sitemap_cache,
)
from config.settings import get_storage_domain_for_db_name
from backend.shared.crawl_dispatcher import dispatch_and_schedule_workflow
from backend.shared.crawl_shared import bool_from_payload, resolve_chat_bot_id, resolve_stream_matched_rules_only
from backend.shared.detail_page_utils import is_detail_page_url
from backend.shared.direct_detail_category import build_direct_detail_start_url_item
from backend.shared.duplicate_category_only_mode import ignore_period_enabled, normalize_duplicate_repair_request_mode
from backend.shared.summary_only_mode import normalize_duplicate_summary_request_mode
from backend.shared.title_only_mode import normalize_duplicate_title_request_mode
from backend.shared.sub_change_mode import is_partial_title_change_request, partial_title_change_enabled
from backend.shared.file_crawl_post_urls import load_file_crawl_post_url_strings
from backend.shared.partial_category_postprocess import (
    is_partial_category_postprocess_request,
    run_partial_category_postprocess,
)
from backend.shared.redis_sse_service import update_state_only
from backend.shared.sse_publish_queue import enqueue_sse_message
from backend.shared.url_scope import (
    extract_scope_host,
    extract_scope_path_prefix,
    normalize_scope_path_prefix,
    scope_path_prefix_enabled,
    url_matches_scope_identities,
)
from backend.file.file_category_apply import sync_existing_file_categories_from_homepage_learning
from utils.url import ensure_url_scheme
from utils.db_name import resolve_db_name
from utils.timezone_utils import get_local_now

router = APIRouter()

_START_URLS_DEFAULT_START_DATE_ISO = "2026-01-01"
_TODAY_DATE_ALIASES = frozenset({"today", "오늘", "금일", "now"})


async def _run_partial_category_postprocess_background(data: dict) -> None:
    db_name = resolve_db_name(data, default="dev_user")
    job_id = str(data.get("job_id") or "").strip()
    try:
        if is_partial_title_change_request(data) and await partial_title_change_enabled(data, dbname=db_name):
            from backend.shared.title_only_mode import run_title_only

            await run_title_only(data)
        await run_partial_category_postprocess(data)
    except Exception as exc:
        logging.getLogger("backend.board.board_endpoints").exception(
            "[PartialCategory] background failed | job_id=%s err=%s",
            job_id,
            exc,
        )
        payload = {
            "status": "error",
            "event": "workflow_error",
            "job_id": job_id,
            "account_name": db_name,
            "message": f"분류 후보정 실패: {exc}",
            "source": "partial_category_postprocess",
        }
        if job_id:
            try:
                await update_state_only(job_id=job_id, account_name=db_name, payload=payload)
                enqueue_sse_message(
                    job_id,
                    payload,
                    db_name,
                    "partial_category_postprocess_error",
                    priority=-10,
                )
            except Exception:
                pass


async def _run_duplicate_repair_only_background(data: dict) -> None:
    db_name = resolve_db_name(data, default="dev_user")
    job_id = str(data.get("job_id") or "").strip()
    try:
        from backend.shared.parsed_fields_only_mode import run_duplicate_repair_only

        await run_duplicate_repair_only(data)
    except Exception as exc:
        logging.getLogger("backend.board.board_endpoints").exception(
            "[DuplicateRepairOnlyDebug] background failed | job_id=%s err=%s",
            job_id,
            exc,
        )
        payload = {
            "status": "error",
            "event": "workflow_error",
            "job_id": job_id,
            "account_name": db_name,
            "message": f"글쓴이/등록일 후보정 실패: {exc}",
            "source": "duplicate_repair_only",
        }
        if job_id:
            try:
                await update_state_only(job_id=job_id, account_name=db_name, payload=payload)
                enqueue_sse_message(
                    job_id,
                    payload,
                    db_name,
                    "duplicate_repair_only_error",
                    priority=-10,
                )
            except Exception:
                pass


def _first_contents_url(contents: object) -> Optional[str]:
    try:
        if isinstance(contents, list) and contents:
            first = contents[0]
            if isinstance(first, dict):
                first = first.get("url") or first.get("content") or first.get("contents_url") or first.get("target_url")
            value = str(first or "").strip()
            return value or None
        if isinstance(contents, dict):
            value = str(contents.get("url") or contents.get("content") or contents.get("contents_url") or contents.get("target_url") or "").strip()
            return value or None
        if isinstance(contents, str):
            value = contents.strip()
            return value or None
    except Exception:
        return None
    return None


def _resolve_primary_contents_url(data: dict) -> Optional[str]:
    try:
        for candidate in (
            _first_contents_url(data.get("contents")),
            str(data.get("contents_url") or "").strip(),
            str(data.get("target_url") or "").strip(),
        ):
            if candidate:
                return candidate
    except Exception:
        return None
    return None


def _resolve_learn_list_id_scope(data: dict):
    for key in ("learn_list_id", "learnListId", "learn_id", "learnId", "db_id", "dbId"):
        value = data.get(key)
        if value not in (None, ""):
            return value
    for source_key in ("contents", "contents_url", "target_url"):
        value = data.get(source_key)
        items = value if isinstance(value, list) else [value]
        for item in items:
            if not isinstance(item, dict):
                continue
            for key in ("learn_list_id", "learnListId", "learn_id", "learnId", "db_id", "dbId", "id"):
                item_value = item.get(key)
                if item_value not in (None, ""):
                    return item_value
    return None


def _resolve_requested_scope_path_prefix(data: dict) -> str:
    if not scope_path_prefix_enabled(data):
        data["scope_path_prefix"] = ""
        return ""

    for key in ("scope_path_prefix", "path_prefix", "start_urls_path_prefix"):
        if key not in data:
            continue
        normalized = normalize_scope_path_prefix(data.get(key))
        data["scope_path_prefix"] = normalized
        return normalized
    normalized = normalize_scope_path_prefix(data.get("scope_path_prefix"))
    if not normalized:
        normalized = extract_scope_path_prefix(_resolve_primary_contents_url(data))
    if "scope_path_prefix" in data or normalized:
        data["scope_path_prefix"] = normalized
    return normalized


def _resolve_start_urls_order(data: dict) -> str:
    raw = str(
        data.get("start_urls_order")
        or data.get("crawl_direction")
        or data.get("start_url_direction")
        or ""
    ).strip().lower()
    if raw in {"reverse", "desc", "backward", "backwards", "from_back", "back"}:
        return "reverse"
    if raw in {"shuffle", "random", "rand", "randomize", "mixed"}:
        return "shuffle"
    if data.get("reverse_start_urls") is True:
        return "reverse"
    if data.get("shuffle_start_urls") is True:
        return "shuffle"
    return "forward"


def _direct_url_matches_requested_scope(url: str, scope_path_prefix: str) -> bool:
    normalized_url = ensure_url_scheme(str(url or "").strip())
    if not normalized_url:
        return False
    normalized_prefix = normalize_scope_path_prefix(scope_path_prefix)
    if not normalized_prefix:
        return True
    scope_host = extract_scope_host(normalized_url)
    if not scope_host:
        return False
    return url_matches_scope_identities(
        normalized_url,
        [scope_host],
        path_prefix=normalized_prefix,
    )


def _resolve_start_urls_date_filter_enabled(data: dict) -> bool:
    for key in (
        "start_urls_date_filter_enabled",
        "exploration_date_filter_enabled",
        "start_urls_date_filter",
    ):
        if key in data:
            return bool_from_payload(data.get(key))
    return False


def _normalize_start_urls_target_date(data: dict, *, enabled: bool) -> Optional[list[str]]:
    if not enabled:
        return None

    try:
        today_iso = get_local_now().date().isoformat()
    except Exception:
        today_iso = datetime.now().date().isoformat()

    raw = data.get("start_urls_target_date")
    if isinstance(raw, list):
        start_raw = str(raw[0] or "").strip() if len(raw) >= 1 else ""
        end_raw = str(raw[1] or "").strip() if len(raw) >= 2 else ""
    else:
        start_raw = ""
        end_raw = ""

    changed = False
    if not start_raw:
        start_raw = _START_URLS_DEFAULT_START_DATE_ISO
        changed = True
    if not end_raw or end_raw.lower() in _TODAY_DATE_ALIASES:
        end_raw = today_iso
        changed = True

    normalized = [start_raw, end_raw]
    if changed or raw != normalized:
        data["start_urls_target_date"] = normalized
    return normalized


def _parse_cookie_header(raw: object) -> dict[str, str]:
    cookies: dict[str, str] = {}
    text = str(raw or "").strip()
    if not text:
        return cookies
    for part in text.split(";"):
        chunk = part.strip()
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        key = key.strip()
        if key:
            cookies[key] = value.strip()
    return cookies


def _collect_request_cookies(data: dict, request: Request) -> dict[str, str]:
    merged: dict[str, str] = {}
    try:
        merged.update({str(k): str(v) for k, v in dict(request.cookies or {}).items() if k and v is not None})
    except Exception:
        pass
    try:
        for key in ("x-session-cookie", "x-request-cookie", "x-edu-ingang-cookie"):
            header_val = request.headers.get(key)
            if header_val:
                merged.update(_parse_cookie_header(header_val))
    except Exception:
        pass

    explicit_map = data.get("request_cookies")
    if isinstance(explicit_map, dict):
        for k, v in explicit_map.items():
            if k and v is not None:
                merged[str(k)] = str(v)

    for key in ("cookie_header", "request_cookie_header", "edu_ingang_cookie_header"):
        try:
            merged.update(_parse_cookie_header(data.get(key)))
        except Exception:
            pass

    return merged

# URL이 게시판 목록 페이지(list.do/list.asp/list.jsp) 패턴인지 여부를 판단
def _is_list_page_url(url: str) -> bool:
    try:
        lu = (url or "").lower()
    except Exception:
        lu = str(url).lower()
    return ("list.do" in lu) or ("list.asp" in lu) or ("list.jsp" in lu)


@router.post("/c1/crawling")
# 게시판 크롤링 API 엔드포인트(요청을 파싱하여 백그라운드 작업으로 디스패치)
async def crawl_board(request: Request, background_tasks: BackgroundTasks):
    """
    게시판 크롤링 (프론트엔드 호환)
    요청 형식:
    {
        "contents": ["url"],
        "subjects": ["사이트명"],
        "job_id": "...",
        "target_date": ["2024-01-01", "2024-12-31"],
        "url_filter": "Q",
        ...
    }
    use_category_url_patterns/category_pattern_url_enabled (선택, 기본 false): true면 CATEGORY url/query
    규칙 매칭 URL만 start_urls로 쓰고, false/생략이면 분류를 무시하고 탐색 DB의 post 전체를 start_urls로 쓴다.
    stream_matched_rules_only도 하위 호환으로 동일하게 지원한다.
    colle=file: 헤더/사이트맵 없이 backend.shared.file_crawl_post_urls 로 탐색 DB type=post URL만 start_urls로 사용.
    """
    print(f"============================ [BOARD] crawling start ============================")
    data = await request.json()
    data["stream_matched_rules_only"] = resolve_stream_matched_rules_only(data)
    data["duplicate_repair_mode"] = normalize_duplicate_repair_request_mode(
        data.get("duplicate_repair_mode")
        or data.get("duplicateRepairMode")
        or data.get("board_duplicate_repair")
        or data.get("duplicate_repair")
    )
    duplicate_parsed_fields_mode = str(
        data.get("duplicate_parsed_fields_mode")
        or data.get("duplicateParsedFieldsMode")
        or data.get("board_duplicate_parsed_fields")
        or data.get("duplicate_parsed_fields")
        or ""
    ).strip().lower()
    if duplicate_parsed_fields_mode in {"on", "parsed_fields", "author", "date", "reg_date", "content_created_at"}:
        if data["duplicate_repair_mode"] == "category":
            data["duplicate_repair_mode"] = "category_parsed_fields"
        elif data["duplicate_repair_mode"] == "off":
            data["duplicate_repair_mode"] = "parsed_fields"
    data["duplicate_parsed_fields_mode"] = (
        "parsed_fields"
        if duplicate_parsed_fields_mode in {"on", "parsed_fields"}
        else "author"
        if duplicate_parsed_fields_mode == "author"
        else "date"
        if duplicate_parsed_fields_mode in {"date", "reg_date", "content_created_at"}
        else "off"
    )
    data["duplicate_summary_mode"] = normalize_duplicate_summary_request_mode(
        data.get("duplicate_summary_mode")
        or data.get("duplicateSummaryMode")
        or data.get("board_duplicate_summary")
        or data.get("duplicate_summary")
    )
    data["duplicate_title_mode"] = normalize_duplicate_title_request_mode(
        data.get("duplicate_title_mode")
        or data.get("duplicateTitleMode")
        or data.get("board_duplicate_title")
        or data.get("duplicate_title")
        or data.get("title_mode")
        or data.get("titleMode")
    )
    crawl_mode = str(data.get("crawl_mode") or "").strip().lower()
    postprocess_only = bool_from_payload(
        data.get("postprocess_only")
        or data.get("duplicate_repair_only")
        or data.get("duplicate_parsed_fields_only")
    )
    if (
        crawl_mode in {"", "crawling"}
        and not postprocess_only
        and data.get("duplicate_summary_mode") == "off"
        and data.get("duplicate_title_mode") == "off"
        and data.get("duplicate_parsed_fields_mode") == "off"
        and data.get("duplicate_repair_mode") != "off"
    ):
        logging.getLogger("backend.board.board_endpoints").info(
            "[CrawlBoard][PostprocessGuard] normal crawling forces duplicate_repair_mode=off | job_id=%s requested=%s",
            data.get("job_id"),
            data.get("duplicate_repair_mode"),
        )
        data["duplicate_repair_mode"] = "off"
    # debug log (no file writes)
    try:
        print(f"============================ [BOARD] crawling start02 ============================")
        logger = logging.getLogger("backend.board.board_endpoints")
        logger.debug(
            json.dumps(
                {
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "H_ENTRY",
                    "location": "backend/board_endpoints.py:crawl_board",
                    "message": "entry",
                    "data": {"contents0": (data.get("contents")[0] if isinstance(data.get("contents"), list) and data.get("contents") else None)},
                    "timestamp": int(time.time() * 1000),
                },
                ensure_ascii=False,
            )
        )
    except Exception:
        pass
    # region agent log
    try:
        logger = logging.getLogger("backend.board.board_endpoints")
        logger.warning(
            "[DBG][BOARD] entry contents0=%s duplicate_repair_mode=%s",
            (data.get("contents")[0] if isinstance(data.get("contents"), list) and data.get("contents") else None),
            data.get("duplicate_repair_mode"),
        )
    except Exception:
        pass
    # endregion
    try:
        if not data.get("colle"):
            data["colle"] = "board"
    except Exception:
        pass
    requested_scope_path_prefix = _resolve_requested_scope_path_prefix(data)

    db_name = resolve_db_name(data, default="dev_user")
    data["server_domain"] = get_storage_domain_for_db_name(db_name)
    data["_category_sync_request_cookies"] = _collect_request_cookies(data, request)
    try:
        explicit_warm = str(
            data.get("edu_ingang_warm_url")
            or data.get("warm_url")
            or request.headers.get("x-edu-ingang-warm-url")
            or request.headers.get("x-session-warm-url")
            or ""
        ).strip()
    except Exception:
        explicit_warm = ""
    try:
        access_url = str(data.get("access_url") or "").strip()
    except Exception:
        access_url = ""
    if explicit_warm:
        data["_edu_ingang_warm_url"] = explicit_warm
    elif "edu.ingang.go.kr" in access_url.lower():
        data["_edu_ingang_warm_url"] = access_url

    job_id = data.get("job_id", "") or ""

    if is_partial_category_postprocess_request(data):
        logging.getLogger("backend.board.board_endpoints").info(
            "[PartialCategory] accepted | job_id=%s db=%s colle=%s fields=%s filter=%s target_date=%s",
            job_id,
            db_name,
            data.get("colle"),
            data.get("partial_update_fields"),
            data.get("partial_target_filter"),
            data.get("target_date"),
        )
        background_tasks.add_task(_run_partial_category_postprocess_background, dict(data))
        return JSONResponse(
            {
                "status": "accepted",
                "job_id": job_id,
                "source": "partial_category_postprocess",
            },
            status_code=200,
        )

    # colle=file: 게시판 헤더·사이트맵과 무관하게 탐색 DB post URL만 사용하고 CATEGORY 규칙으로만 분류
    try:
        from backend.shared.parsed_fields_only_mode import is_duplicate_repair_only_request

        if is_duplicate_repair_only_request(data):
            logging.getLogger("backend.board.board_endpoints").info(
                "[DuplicateRepairOnlyDebug][accepted] job_id=%s db=%s duplicate_repair_mode=%s duplicate_summary_mode=%s duplicate_title_mode=%s duplicate_parsed_fields_mode=%s target_date=%s",
                job_id,
                db_name,
                data.get("duplicate_repair_mode"),
                data.get("duplicate_summary_mode"),
                data.get("duplicate_title_mode"),
                data.get("duplicate_parsed_fields_mode"),
                data.get("target_date"),
            )
            background_tasks.add_task(_run_duplicate_repair_only_background, dict(data))
            return JSONResponse(
                {
                    "status": "accepted",
                    "job_id": job_id,
                    "source": "duplicate_repair_only",
                },
                status_code=200,
            )
    except Exception as exc:
        logging.getLogger("backend.board.board_endpoints").warning(
            "[DuplicateRepairOnlyDebug][route_check_failed] job_id=%s err=%s",
            job_id,
            exc,
        )

    try:
        _colle = str(data.get("colle") or "").strip().lower()
    except Exception:
        _colle = "board"
    try:
        mode = str(data.get("duplicate_repair_mode") or "category").strip().lower()
        print(
            f"============================ [CrawlBoard][StartMode] duplicate_repair_mode={mode} "
            f"job_id={job_id} colle={_colle} ============================",
            flush=True,
        )
        logging.getLogger("backend.board.board_endpoints").warning(
            "[CrawlBoard][StartMode] job_id=%s colle=%s duplicate_repair_mode=%s",
            job_id,
            _colle,
            mode,
        )
    except Exception:
        pass
    start_urls_date_filter_enabled = _resolve_start_urls_date_filter_enabled(data)
    if data.get("duplicate_repair_mode") in {"category", "parsed_fields", "category_parsed_fields"}:
        start_urls_date_filter_enabled = False
        data["start_urls_target_date"] = None
    normalized_start_urls_target_date = _normalize_start_urls_target_date(
        data,
        enabled=start_urls_date_filter_enabled,
    )
    if _colle == "file":
        logging.getLogger("backend.board.board_endpoints").info(
            "[START_URLS_DATE_FILTER] request | job_id=%s colle=file enabled=%s start_urls_target_date=%s crawl_target_date=%s",
            job_id,
            start_urls_date_filter_enabled,
            normalized_start_urls_target_date or data.get("start_urls_target_date"),
            data.get("target_date"),
        )
        raw_chat_bot_id = data.get("chat_bot_id") or (data.get("metadata") or {}).get("chat_bot_id")
        chat_bot_id = resolve_chat_bot_id(job_id, raw_chat_bot_id)
        if chat_bot_id:
            data["chat_bot_id"] = chat_bot_id
        try:
            auto_sync_file_categories = (
                bool_from_payload(data.get("sync_file_categories"))
                if "sync_file_categories" in data
                else True
            )
        except Exception:
            auto_sync_file_categories = True
        if auto_sync_file_categories:
            if not chat_bot_id:
                return JSONResponse(
                    {"status": "error", "message": "file category sync requires chat_bot_id"},
                    status_code=400,
                )
            try:
                sync_result = await sync_existing_file_categories_from_homepage_learning(
                    chat_bot_id=chat_bot_id,
                    db_name=db_name,
                    access_url=str(data.get("access_url") or "").strip() or None,
                    request_cookies=dict(request.cookies or {}),
                )
                data["file_category_sync"] = {
                    "ok": bool(sync_result.get("ok")),
                    "preview_summary": dict(sync_result.get("preview_summary") or {}),
                    "stats": dict(sync_result.get("stats") or {}),
                }
                logging.getLogger("backend.board.board_endpoints").info(
                    "[CrawlBoard][file] category sync applied | job_id=%s chat_bot_id=%s stats=%s",
                    job_id,
                    chat_bot_id,
                    data["file_category_sync"],
                )
            except Exception as exc:
                logger = logging.getLogger("backend.board.board_endpoints")
                logger.exception("[CrawlBoard][file] category sync failed: %s", exc)
                return JSONResponse(
                    {
                        "status": "error",
                        "message": f"failed to sync file categories before crawl: {exc}",
                    },
                    status_code=500,
                )
        contents_url_for_scope = _resolve_primary_contents_url(data)
        direct_detail_url = ensure_url_scheme(contents_url_for_scope or "") if contents_url_for_scope else ""
        direct_detail_is_detail = bool(direct_detail_url and is_detail_page_url(direct_detail_url))
        if direct_detail_is_detail:
            direct_detail_in_scope = _direct_url_matches_requested_scope(
                direct_detail_url,
                requested_scope_path_prefix,
            )
            direct_detail_item = (
                await build_direct_detail_start_url_item(
                    data,
                    direct_detail_url,
                    db_name=db_name,
                    chat_bot_id=chat_bot_id,
                )
                if direct_detail_in_scope
                else None
            )
            data["start_urls_override"] = [direct_detail_item] if direct_detail_item else []
            data["pre_explored_start_urls_count"] = 1 if direct_detail_in_scope else 0
            data["start_urls_override_source"] = "contents_detail_direct"
            data["file_crawl_stream_config"] = {}
            logging.getLogger("backend.board.board_endpoints").warning(
                "[CrawlBoard][file] direct detail URL detected -> bypass DB stream | job_id=%s url=%s scope_path_prefix=%s in_scope=%s item=%s",
                job_id,
                direct_detail_url,
                requested_scope_path_prefix,
                direct_detail_in_scope,
                direct_detail_item,
            )
            return await dispatch_and_schedule_workflow(
                data,
                background_tasks,
                header_response=None,
            )
        target_domains = None
        try:
            td = data.get("target_domains")
            if isinstance(td, list):
                target_domains = [str(x).strip() for x in td if x]
            elif isinstance(td, str) and td.strip():
                target_domains = [x.strip() for x in td.split(",") if x.strip()]
        except Exception:
            target_domains = None
        stream_method = str(data.get("method") or "period")
        start_urls_order = _resolve_start_urls_order(data)
        learn_list_id_scope = _resolve_learn_list_id_scope(data)
        start_urls = await load_file_crawl_post_url_strings(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            target_domains=target_domains,
            contents_url=contents_url_for_scope,
            method=stream_method,
            target_date=data.get("start_urls_target_date"),
            exploration_date_filter_enabled=start_urls_date_filter_enabled,
            scope_path_prefix=data.get("scope_path_prefix"),
            start_urls_order=start_urls_order,
            use_category_rules=False,
            dedupe_urls=True,
            learn_list_id_scope=learn_list_id_scope,
            scope_by_contents_learn_list_id=True,
        )
        data["_file_start_urls_seed_source"] = "learn_list_id" if start_urls else "empty"
        if not start_urls:
            logger.warning(
                "[CrawlBoard][file] learn_list_id seed returned empty | job_id=%s learn_list_id_scope=%s contents_url=%s scope_path_prefix=%s",
                job_id,
                learn_list_id_scope,
                str(contents_url_for_scope or "")[:180],
                data.get("scope_path_prefix"),
            )
        try:
            sample_urls = []
            for item in (start_urls or [])[:5]:
                if isinstance(item, dict):
                    sample_urls.append({"url": str(item.get("url") or "")[:180], "type": item.get("type")})
                else:
                    sample_urls.append(str(item or "")[:180])
            logger.warning(
                "[CrawlBoard][StartUrls] job_id=%s colle=%s duplicate_repair_mode=%s count=%s seed_source=%s target_domains=%s scope_path_prefix=%s date_filter=%s target_date=%s sample=%s",
                job_id,
                _colle,
                data.get("duplicate_repair_mode"),
                len(start_urls or []),
                data.get("_file_start_urls_seed_source"),
                target_domains,
                data.get("scope_path_prefix"),
                start_urls_date_filter_enabled,
                data.get("start_urls_target_date"),
                sample_urls,
            )
        except Exception:
            pass
        data["start_urls_override"] = start_urls
        data["pre_explored_start_urls_count"] = len(start_urls)
        data["start_urls_override_source"] = "file_crawl_post_db"
        data["file_crawl_stream_config"] = {}
        data["_file_start_urls_db_branch_applied"] = True
        if not start_urls:
            data["_file_start_urls_db_branch_failure_reason"] = "file_seed_urls_empty_after_learn_list_and_legacy_scope"
            data["_file_start_urls_db_branch_failure_message"] = (
                "learn_list_id seed URL lookup and legacy subfolder scope fallback both returned empty."
            )
            data["_file_start_urls_db_branch_failure_contents_url"] = contents_url_for_scope
        return await dispatch_and_schedule_workflow(
            data,
            background_tasks,
            header_response=None,
        )

    header_source_url: Optional[str] = None
    header_response: Optional[CrawlResponse] = None
    contents_payload = data.get("contents")
    if isinstance(contents_payload, list) and contents_payload:
        first_entry = contents_payload[0]
        if isinstance(first_entry, str):
            candidate = first_entry.strip()
            if candidate:
                header_source_url = ensure_url_scheme(candidate)

    # list.do URL은 이미 범위가 충분히 구체적이므로 start_urls를 잠금 처리
    list_url_lock = False
    try:
        has_override = isinstance(data.get("start_urls_override"), list) and bool(data.get("start_urls_override"))
        if header_source_url and _is_list_page_url(header_source_url) and not has_override:
            data["start_urls_override"] = [ensure_url_scheme(header_source_url)]
            data["start_urls_override_source"] = "contents_list_lock"
            list_url_lock = True
    except Exception:
        list_url_lock = False

    if header_source_url:
        header_debug_flag = bool_from_payload(data.get("header_debug") if "header_debug" in data else data.get("debug"))
        try:
            header_request = HeaderCrawlRequest(url=header_source_url, debug=header_debug_flag)
        except ValidationError as exc:
            logger = logging.getLogger("backend.board.board_endpoints")
            logger.warning(
                "[CrawlBoard] Header crawl skipped due to invalid URL | job_id=%s url=%s err=%s",
                job_id,
                header_source_url,
                exc,
            )
        else:
            for attempt in range(2):
                try:
                    header_response = await header_crawl_handler(header_request)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger = logging.getLogger("backend.board.board_endpoints")
                    if attempt < 1:
                        logger.warning(
                            "[CrawlBoard] Header crawl failed, retrying (%d/2): %s",
                            attempt + 1,
                            exc,
                        )
                        await asyncio.sleep(1.5)
                    else:
                        logger.warning(
                            "[CrawlBoard] Header crawl failed after retries; continue without header_response | job_id=%s url=%s err=%s",
                            job_id,
                            header_source_url,
                            exc,
                        )
                        header_response = None
                        break

    if header_response and getattr(header_response, "groups", None):
        try:
            base_url = str(getattr(header_response, "source_url", "") or header_source_url or "")
            sitemap_md = build_sitemap_markdown(header_response.groups, base_url)
            data["sitemap_markdown"] = sitemap_md
            try:
                base_origin = get_base_origin(base_url)
                store_sitemap_cache(base_origin, header_response.groups)
                _, md_path = get_sitemap_cache_paths(base_origin)
                data["sitemap_markdown_path"] = md_path
            except Exception as exc:
                logger = logging.getLogger("backend.board.board_endpoints")
                logger.warning(
                    "[CrawlBoard] sitemap markdown file write failed | job_id=%s url=%s err=%s",
                    job_id,
                    base_url,
                    exc,
                )
        except Exception as exc:
            logger = logging.getLogger("backend.board.board_endpoints")
            logger.warning(
                "[CrawlBoard] sitemap markdown build failed | job_id=%s url=%s err=%s",
                job_id,
                header_source_url,
                exc,
            )

    if header_source_url and not list_url_lock:
        try:
            is_top_domain = is_top_domain_url(header_source_url)
            if is_top_domain and header_response:
                board_list_urls = list(getattr(header_response, "board_list_urls", []) or [])
                board_list_links = list(getattr(header_response, "board_list_links", []) or [])
                if board_list_urls:
                    label_by_url = {}
                    for link in board_list_links:
                        normalized_link_url = ensure_url_scheme(getattr(link, "url", "") or "")
                        if not normalized_link_url:
                            continue
                        label = str(getattr(link, "label", "") or "").strip()
                        if label:
                            label_by_url[normalized_link_url] = label
                    start_urls_override = []
                    for u in board_list_urls:
                        normalized_url = ensure_url_scheme(u)
                        if not normalized_url:
                            continue
                        title_hint = label_by_url.get(normalized_url, "")
                        if title_hint:
                            start_urls_override.append({"url": normalized_url, "title": title_hint, "subject": title_hint})
                        else:
                            start_urls_override.append(normalized_url)
                    data["start_urls_override"] = start_urls_override
                    data["start_urls_override_source"] = "sitemap_board_list"
                else:
                    # 최고 도메인이지만 board list를 못 찾으면 입력 URL로 fallback
                    data["start_urls_override"] = [ensure_url_scheme(header_source_url)]
                    data["start_urls_override_source"] = "direct_match"
            else:
                # 최고 도메인이 아니면 입력 URL만 사용
                data["start_urls_override"] = [ensure_url_scheme(header_source_url)]
                data["start_urls_override_source"] = "direct_match"
        except Exception as exc:
            logger = logging.getLogger("backend.board.board_endpoints")
            logger.warning(
                "[CrawlBoard] start_urls override setup failed | job_id=%s url=%s err=%s",
                job_id,
                header_source_url,
                exc,
            )

    # 백그라운드로 디스패처 실행하고 즉시 응답 반환
    try:
        asyncio.create_task(
            dispatch_and_schedule_workflow(data, background_tasks, header_response=header_response),
            name=f"dispatch_and_schedule_workflow:{job_id}",
        )
    except Exception as exc:
        logger = logging.getLogger("backend.board.board_endpoints")
        logger.exception("[CrawlBoard] Failed to schedule dispatch: %s", exc)
        return JSONResponse({"status": "error", "message": "failed to schedule background task"}, status_code=500)

    return JSONResponse({"status": "accepted", "job_id": job_id}, status_code=200)
