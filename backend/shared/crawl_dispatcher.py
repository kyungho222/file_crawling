import asyncio
import json
import logging
import os
import random
import time
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse

from backend.shared.board_header import CrawlResponse
from backend.shared.crawler_state import crawler_state
from backend.shared.crawl_shared import (
    bootstrap_job_state,
    initial_state_payload,
    publish_job_terminal_completed,
    publish_job_terminal_error,
    resolve_chat_bot_id,
    send_sse_message,
    swallow_task_exception,
)
from backend.shared.learn_list_start_url_dedupe import apply_learn_list_start_url_dedupe
from backend.file.integrated_workflow import IntegratedWorkflow
from backend.shared.crawl_monitor import monitor_auto_stop
from backend.board.board_content_workflow import BoardContentWorkflow
from backend.shared.seed_urls import resolve_seed_urls
from backend.shared.start_urls_generation import normalize_known_start_url_alias
from backend.shared.workflow_dispatch_assembly import assemble_workflow_after_url_resolve
from backend.shared.workflow_runner import run_workflow_task
from utils.timezone_utils import get_local_now
from utils.url import ensure_url_scheme
from backend.shared.sse_publish_queue import enqueue_sse_message
from backend.shared.url_scope import (
    extract_service_scope_path_prefix,
    extract_scope_host,
    extract_scope_identities,
    fallback_scope_path_prefixes,
    normalize_scope_path_prefix,
    scope_path_prefix_enabled,
    url_matches_scope_identities,
)

logger = logging.getLogger("backend.shared.crawl_dispatcher")
CONCURRENT_CRAWL_LOG_PREFIX = "[ConcurrentCrawlStartDebug]"
SONGPA_TITLE_TRACE_PREFIX = "[SongpaTitleTrace]"


def _songpa_title_trace(stage: str, *, url: str = "", **fields: Any) -> None:
    if "songpa.go.kr" not in str(url or "").lower():
        return
    try:
        compact = {
            str(k): (str(v or "")[:240] if v is not None else "")
            for k, v in (fields or {}).items()
        }
        logger.warning("%s stage=%s url=%s fields=%s", SONGPA_TITLE_TRACE_PREFIX, stage, str(url or "")[:240], compact)
        try:
            os.makedirs("logs", exist_ok=True)
            with open(os.path.join("logs", "songpa_title_trace.log"), "a", encoding="utf-8") as fp:
                fp.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {SONGPA_TITLE_TRACE_PREFIX} module=crawl_dispatcher stage={stage} url={str(url or '')[:240]} fields={compact}\n")
        except Exception:
            pass
    except Exception:
        pass


def _crawl_workflow_via_celery() -> bool:
    """True硫??щ· ?뚰겕?뚮줈瑜?Celery ?뚯빱?먯꽌 ?ㅽ뻾 (uvicorn? ?먯뿉留??ｌ쓬)."""
    try:
        return str(os.getenv("CRAWL_WORKFLOW_USE_CELERY", "") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        return False


def _force_direct_detail_enabled(data: Dict[str, Any]) -> bool:
    return str(
        data.get("probe_direct_detail")
        or data.get("file_probe_direct_detail")
        or data.get("direct_detail_url")
        or ""
    ).strip().lower() in {"1", "true", "y", "yes", "on"}


async def _crawl_wf_redis_job_active(job_id: str) -> bool:
    """Celery ?щ·??吏꾪뻾 以묒씠硫?payload ?ㅺ? 議댁옱?쒕떎."""
    try:
        from db.db_redis import get_redis

        r = await get_redis()
        return bool(await r.exists(f"crawl_wf_payload:{job_id}"))
    except Exception:
        return False


def _crawling_log_wait_timeout_seconds() -> float:
    try:
        value = float(os.getenv("CRAWLING_LOG_WAIT_TIMEOUT_SEC", "3") or "3")
    except Exception:
        value = 3.0
    return max(0.0, min(value, 30.0))


async def _wait_for_php_crawling_log(job_id: str, db_name: str, craw_id: Any) -> int | None:
    """
    The PHP side creates ASADAL_CRAWLING_LOG before FastAPI starts the crawl.
    If craw_id was not forwarded, wait briefly until the PHP row is visible by job_id.
    """
    if not job_id or not db_name:
        return None
    try:
        existing = int(str(craw_id).strip()) if craw_id else 0
    except Exception:
        existing = 0
    if existing > 0:
        return existing

    timeout = _crawling_log_wait_timeout_seconds()
    if timeout <= 0:
        return None

    deadline = time.monotonic() + timeout
    delay = 0.15
    while True:
        try:
            from db.crawl_db_manager import resolve_crawling_log_id

            resolved_id = await resolve_crawling_log_id(job_id, dbname=db_name)
            if resolved_id:
                return int(resolved_id)
        except Exception as exc:
            logger.debug(
                "[Dispatch] PHP crawling log row wait check failed | job_id=%s db=%s err=%s",
                job_id,
                db_name,
                exc,
            )

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "[Dispatch] PHP crawling log row not visible before workflow start | job_id=%s db=%s waited_ms=%s",
                job_id,
                db_name,
                int(timeout * 1000),
            )
            return None
        await asyncio.sleep(min(delay, remaining))
        delay = min(delay * 1.5, 1.0)


# Debug: no file writes; use logger.debug instead
def _debug_log(*, location: str, message: str, data: Dict[str, Any], hypothesis_id: str) -> None:
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": "run1",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        logging.getLogger("backend.shared.crawl_dispatcher").debug(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


_HANGUL_RE = re.compile(r"[가-힣]")


def _maybe_fix_mojibake(value: Optional[str]) -> Optional[str]:
    """
    UTF-8 諛붿씠?멸? latin1?쇰줈 ?섎せ ?붿퐫?⑸맂 臾몄옄?댁쓣 蹂듦뎄?쒕떎.
    ?? "챗쨈?샖?㎮왗ぢ돠??꼲? -> "愿묒쭊援ъ껌"
    """
    if value is None:
        return None
    if not isinstance(value, str):
        try:
            value = str(value)
        except Exception:
            return value
    if not value:
        return value
    if _HANGUL_RE.search(value):
        return value
    try:
        fixed = value.encode("latin1").decode("utf-8")
    except Exception:
        return value
    if _HANGUL_RE.search(fixed):
        return fixed
    return value


def _extract_cate2_from_web_title(web_title: Optional[str]) -> str:
    try:
        if not web_title:
            return ""
        part = str(web_title).split("<", 1)[0].strip()
        part = re.sub(r"\s*\([^)]*\)\s*", "", part).strip()
        return part
    except Exception:
        return ""


def _extract_first_payload_url(value: Any) -> Optional[str]:
    try:
        if isinstance(value, list) and value:
            first = value[0]
            if isinstance(first, dict):
                first = first.get("url") or first.get("content") or first.get("contents_url") or first.get("target_url")
            candidate = str(first or "").strip()
            return candidate or None
        if isinstance(value, dict):
            candidate = str(value.get("url") or value.get("content") or value.get("contents_url") or value.get("target_url") or "").strip()
            return candidate or None
        if isinstance(value, str):
            candidate = value.strip()
            return candidate or None
    except Exception:
        return None
    return None


def _resolve_learn_list_id_scope_from_payload(data: Dict[str, Any]) -> Any:
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


def _resolve_requested_scope_path_prefix(data: Dict[str, Any]) -> str:
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
        normalized = extract_service_scope_path_prefix(
            _extract_first_payload_url(data.get("contents"))
            or _extract_first_payload_url(data.get("contents_url"))
            or _extract_first_payload_url(data.get("target_url"))
        )
    if "scope_path_prefix" in data or normalized:
        data["scope_path_prefix"] = normalized
        source_url = (
            _extract_first_payload_url(data.get("contents"))
            or _extract_first_payload_url(data.get("contents_url"))
            or _extract_first_payload_url(data.get("target_url"))
        )
        fallbacks = fallback_scope_path_prefixes(source_url, normalized)
        if fallbacks:
            data["scope_path_prefix_fallbacks"] = fallbacks
    return normalized


def _resolve_scope_source_for_start_urls(data: Dict[str, Any], start_urls: List[Any]) -> str:
    for raw in (data.get("contents_url"), data.get("target_url"), data.get("contents")):
        candidate = _extract_first_payload_url(raw)
        if candidate:
            return candidate
    for item in start_urls or []:
        try:
            candidate = item.get("url") if isinstance(item, dict) else item
        except Exception:
            candidate = item
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _url_path_matches_prefix(url: str, path_prefix: str) -> bool:
    normalized_prefix = normalize_scope_path_prefix(path_prefix)
    if not normalized_prefix:
        return True
    try:
        parsed = urlparse(ensure_url_scheme(str(url or "").strip()))
        path = str(parsed.path or "").strip() or "/"
    except Exception:
        return False
    normalized_path = normalize_scope_path_prefix(path)
    if normalized_prefix == "/":
        return True
    return normalized_path.startswith(normalized_prefix)


def _filter_start_urls_by_requested_scope(data: Dict[str, Any], start_urls: List[Any]) -> tuple[List[Any], Dict[str, Any]]:
    requested_prefix = _resolve_requested_scope_path_prefix(data)
    if not requested_prefix or not start_urls:
        return start_urls, {
            "requested_path_prefix": requested_prefix,
            "identities": [],
            "before": len(start_urls or []),
            "after": len(start_urls or []),
            "skipped": 0,
            "samples": [],
        }

    identities = extract_scope_identities(data.get("target_domains"))
    if not identities:
        scope_source = _resolve_scope_source_for_start_urls(data, start_urls)
        scope_host = extract_scope_host(scope_source)
        if scope_host:
            identities = [scope_host]

    def _apply_scope(path_prefix: str) -> tuple[List[Any], int, List[str]]:
        filtered_items: List[Any] = []
        skipped_count = 0
        skipped_sample_items: List[str] = []
        for item in start_urls or []:
            try:
                raw_url = item.get("url") if isinstance(item, dict) else item
            except Exception:
                raw_url = item
            normalized_url = ensure_url_scheme(str(raw_url or "").strip()) if raw_url else ""
            if not normalized_url:
                continue
            if identities:
                try:
                    in_scope = url_matches_scope_identities(
                        normalized_url,
                        identities,
                        path_prefix=path_prefix,
                    )
                except Exception:
                    in_scope = False
            else:
                in_scope = _url_path_matches_prefix(normalized_url, path_prefix)
            if not in_scope:
                skipped_count += 1
                if len(skipped_sample_items) < 5:
                    skipped_sample_items.append(normalized_url[:200])
                continue
            if isinstance(item, dict):
                item_copy = dict(item)
                item_copy["url"] = normalized_url
                filtered_items.append(item_copy)
            else:
                filtered_items.append(normalized_url)
        return filtered_items, skipped_count, skipped_sample_items

    filtered, skipped, skipped_samples = _apply_scope(requested_prefix)
    fallback_prefix_used = ""
    if not filtered:
        fallback_prefixes = data.get("scope_path_prefix_fallbacks")
        if not isinstance(fallback_prefixes, list):
            fallback_prefixes = []
        for fallback_prefix in fallback_prefixes:
            normalized_fallback = normalize_scope_path_prefix(fallback_prefix)
            if not normalized_fallback or normalized_fallback == requested_prefix:
                continue
            fallback_filtered, fallback_skipped, fallback_samples = _apply_scope(normalized_fallback)
            if fallback_filtered:
                
                filtered, skipped, skipped_samples = fallback_filtered, fallback_skipped, fallback_samples
                fallback_prefix_used = normalized_fallback
                data["scope_path_prefix"] = normalized_fallback
                break

    return filtered, {
        "requested_path_prefix": requested_prefix,
        "fallback_path_prefix": fallback_prefix_used,
        "identities": identities,
        "before": len(start_urls or []),
        "after": len(filtered),
        "skipped": skipped,
        "samples": skipped_samples,
    }


def _resolve_start_urls_order(data: Dict[str, Any]) -> str:
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


def _apply_start_urls_order(data: Dict[str, Any], start_urls: List[Any]) -> List[Any]:
    if not start_urls:
        return start_urls
    order = _resolve_start_urls_order(data)
    if order == "shuffle":
        shuffled = list(start_urls)
        seed_raw = str(data.get("start_urls_shuffle_seed") or "").strip()
        if seed_raw:
            random.Random(seed_raw).shuffle(shuffled)
        else:
            random.shuffle(shuffled)
        return shuffled
    return start_urls


def _is_all_start_urls_duplicate_excluded(meta: Any) -> bool:
    if not isinstance(meta, dict) or not meta.get("enabled"):
        return False
    try:
        before = int(meta.get("before") or 0)
    except Exception:
        before = 0
    try:
        after = int(meta.get("after") or 0)
    except Exception:
        after = 0
    try:
        duplicates = int(meta.get("duplicates") or 0)
    except Exception:
        duplicates = 0
    return before > 0 and after == 0 and duplicates >= before


def _payload_is_file_crawl_intent(data: Dict[str, Any], override_source: str = "") -> bool:
    try:
        colle_mode = str(data.get("colle") or "").strip().lower()
    except Exception:
        colle_mode = ""
    try:
        content_type = str(data.get("content_type") or "").strip().lower()
    except Exception:
        content_type = ""
    override_source = str(override_source or data.get("start_urls_override_source") or "").strip()
    if colle_mode == "file":
        return True
    if content_type in {"file", "attach", "attachment"}:
        return True
    if bool(data.get("_file_crawl_mode")):
        return True
    if bool(data.get("file_dashboard")):
        return True
    if override_source in {"file_crawl_post_db", "file_crawl_post_db_stream"}:
        return True
    return False


def _is_file_start_urls_db_branch_enabled(data: Dict[str, Any], override_source: str) -> bool:
    if not _payload_is_file_crawl_intent(data, override_source):
        return False

    if _force_direct_detail_enabled(data):
        return False

    try:
        if bool(data.get("file_category_update_only")):
            return None
    except Exception:
        pass

    # These sources are already exact targets or special maintenance flows.
    exact_sources = {
        "contents_detail_direct",
        "partial_content_relearn",
        "board_gap_dashboard",
        "file_crawl_post_db_stream",
    }
    if str(override_source or "").strip() in exact_sources:
        return False
    return True


async def _resolve_file_start_urls_from_exploration_posts(
    data: Dict[str, Any],
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    job_id: str,
) -> Optional[List[Any]]:
    if not chat_bot_id:
        logger.warning(
            "[Dispatch][file_start_urls] chat_bot_id missing; cannot load exploration post URLs | job_id=%s db=%s",
            job_id,
            db_name,
        )
        return None
    try:
        from backend.shared.file_crawl_post_urls import load_file_crawl_post_url_strings
        from backend.shared.pre_explored_url import count_exploration_post_urls

        contents_url = (
            _extract_first_payload_url(data.get("contents"))
            or _extract_first_payload_url(data.get("contents_url"))
            or _extract_first_payload_url(data.get("target_url"))
        )
        target_domains = data.get("target_domains")
        if isinstance(target_domains, str):
            target_domains = [x.strip() for x in target_domains.split(",") if x.strip()]
        elif not isinstance(target_domains, list):
            target_domains = None
        scope_path_prefix = _resolve_requested_scope_path_prefix(data)
        start_urls = await load_file_crawl_post_url_strings(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            target_domains=target_domains,
            contents_url=contents_url,
            method=str(data.get("method") or "period"),
            target_date=None,
            exploration_date_filter_enabled=False,
            scope_path_prefix=scope_path_prefix,
            start_urls_order=_resolve_start_urls_order(data),
            use_category_rules=False,
            dedupe_urls=True,
            learn_list_id_scope=None,
            scope_by_contents_learn_list_id=True,
        )
        data["_file_start_urls_seed_source"] = "learn_list_id" if start_urls else "empty"
        if start_urls:
            logger.debug(
                "[Dispatch][file_start_urls] learn_list ID scope loaded | job_id=%s db=%s chat_bot_id=%s count=%s",
                job_id,
                db_name,
                chat_bot_id,
                len(start_urls or []),
            )
        else:
            logger.warning(
                "[Dispatch][file_start_urls] learn_list ID scope returned empty | job_id=%s db=%s chat_bot_id=%s contents_url=%s",
                job_id,
                db_name,
                chat_bot_id,
                str(contents_url or "")[:180],
            )
            data["_file_start_urls_db_branch_failure_reason"] = "file_start_urls_empty"
            data["_file_start_urls_db_branch_failure_message"] = (
                "No exploration rows matched the LEARN_LIST id resolved from contents."
            )
            data["_file_start_urls_db_branch_failure_contents_url"] = contents_url
        if start_urls:
            post_total = len(start_urls or [])
        else:
            post_total = 0
    except Exception as exc:
        logger.warning(
            "[Dispatch][file_start_urls] exploration post URL load failed | job_id=%s db=%s chat_bot_id=%s err=%s",
            job_id,
            db_name,
            chat_bot_id,
            exc,
            exc_info=True,
        )
        return None

    try:
        total = int(post_total or 0)
    except Exception:
        total = 0
    data["_file_start_urls_db_branch_applied"] = True
    data["start_urls_override"] = start_urls
    data["start_urls_override_source"] = "file_crawl_post_db"
    data["pre_explored_start_urls_count"] = total or len(start_urls or [])
    data["exploration_post_total_count"] = total or len(start_urls or [])
    data["selected_start_urls_count"] = len(start_urls or [])
    data["actual_start_urls_count"] = len(start_urls or [])
    data["file_crawl_stream_config"] = {}
    return start_urls

async def _resolve_file_start_urls_from_board_static_discovery(
    seed_url: str,
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    job_id: str,
) -> List[str]:
    if not seed_url:
        return []
    try:
        normalized_seed = normalize_known_start_url_alias(ensure_url_scheme(str(seed_url).strip()))
    except Exception:
        normalized_seed = str(seed_url or "").strip()
    if not normalized_seed:
        return []
    detail_urls: List[str] = []
    try:
        from backend.shared.board_list_discovery_pipeline import (
            _fetch_html as _fetch_board_list_html,
            extract_post_urls_from_list_html_fast,
        )

        html = await _fetch_board_list_html(normalized_seed, timeout_sec=20.0)
        detail_urls = extract_post_urls_from_list_html_fast(html or "", page_url=normalized_seed)
        try:
            seed_qs = parse_qs(urlparse(normalized_seed).query or "")
            seed_bbs_code = (seed_qs.get("q_bbsCode") or seed_qs.get("q_bbscode") or [""])[0]
        except Exception:
            seed_bbs_code = ""
        if seed_bbs_code:
            filtered_detail_urls: List[str] = []
            for candidate_url in detail_urls or []:
                try:
                    cand_qs = parse_qs(urlparse(candidate_url).query or "")
                    cand_bbs_code = (cand_qs.get("q_bbsCode") or cand_qs.get("q_bbscode") or [""])[0]
                except Exception:
                    cand_bbs_code = ""
                if cand_bbs_code == seed_bbs_code:
                    filtered_detail_urls.append(candidate_url)
            detail_urls = filtered_detail_urls
        try:
            seed_bbs_no = (seed_qs.get("bbsNo") or seed_qs.get("bbsno") or [""])[0]
        except Exception:
            seed_bbs_no = ""
        if seed_bbs_no:
            filtered_detail_urls = []
            for candidate_url in detail_urls or []:
                try:
                    cand_qs = parse_qs(urlparse(candidate_url).query or "")
                    cand_bbs_no = (cand_qs.get("bbsNo") or cand_qs.get("bbsno") or [""])[0]
                except Exception:
                    cand_bbs_no = ""
                if cand_bbs_no == seed_bbs_no:
                    filtered_detail_urls.append(candidate_url)
            detail_urls = filtered_detail_urls
    except Exception as exc:
        logger.warning(
            "[Dispatch][file_start_urls] board list fast extraction fallback failed | job_id=%s seed=%s err=%s",
            job_id,
            normalized_seed,
            exc,
            exc_info=True,
        )
        detail_urls = []
    if not detail_urls:
        try:
            workflow = BoardContentWorkflow()
            workflow.job_id = job_id
            workflow.chat_bot_id = chat_bot_id
            workflow.db_name = db_name
            detail_urls = await workflow.discover_detail_urls_only(
                [normalized_seed],
                start_date=None,
                end_date=None,
                use_query_links_only=False,
            )
        except Exception as exc:
            logger.warning(
                "[Dispatch][file_start_urls] board static discovery fallback failed | job_id=%s seed=%s err=%s",
                job_id,
                normalized_seed,
                exc,
                exc_info=True,
            )
            return []
    seen: set[str] = set()
    out: List[str] = []
    for raw in detail_urls or []:
        try:
            url = ensure_url_scheme(str(raw or "").strip())
        except Exception:
            url = str(raw or "").strip()
        if not url:
            continue
        key = url.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        out.append(url)
    logger.warning(
        "[Dispatch][file_start_urls] board static discovery fallback resolved | job_id=%s seed=%s count=%s sample=%s",
        job_id,
        normalized_seed,
        len(out),
        out[:3],
    )
    return out

async def dispatch_and_schedule_workflow(
    data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    header_response: Optional[CrawlResponse] = None,
) -> JSONResponse:
    """
    怨듭슜 ?붿뒪?⑥쿂:
    - board/file ?붾뱶?ъ씤?몄뿉???몄텧
    - start_urls 援ъ꽦(query_links ??; ?섏쭛 ???0嫄댁씠硫?contents(硫붿씤) ?대갚 ?놁씠 HTTP 422쨌status=error 濡??ㅽ뙣
    - workflow ?좏깮(colle) 諛?task ?ㅼ?以꾨쭅
    """

    dispatch_t0 = time.perf_counter()
    contents = data.get("contents", [])
    job_id = data.get("job_id", "") or ""
    if not job_id:
        return JSONResponse({"status": "error", "message": "job_id is required"}, status_code=400)
    try:
        from utils.db_name import resolve_db_name
        initial_db_name = resolve_db_name(data, default="dev_user")
    except Exception:
        initial_db_name = data.get("db_name") or data.get("dbname") or data.get("account_name") or "dev_user"
    initial_payload = initial_state_payload()
    initial_payload.update(
        {
            "status": "start",
            "event": "dispatch_received",
            "job_id": job_id,
            "account_name": initial_db_name,
            "source": "dispatch_initial",
        }
    )
    try:
        await send_sse_message(job_id, initial_payload, str(initial_db_name), "dispatch_initial")
    except Exception:
        pass
    primary_content = None
    if isinstance(contents, list) and contents:
        primary_content = contents[0]
    _songpa_title_trace(
        "dispatch_entry",
        url=str(data.get("contents_url") or data.get("access_url") or primary_content or ""),
        job_id=job_id,
        colle=data.get("colle"),
        content_type=data.get("content_type"),
        contents0=primary_content,
        has_override=isinstance(data.get("start_urls_override"), list) and bool(data.get("start_urls_override")),
    )
    # Debug: entry snapshot
    # region agent log
    _debug_log(
        location="backend/crawl_dispatcher.py:dispatch_and_schedule_workflow:entry",
        message="dispatch entry",
        data={
            "job_id": job_id,
            "colle": str(data.get("colle") or ""),
            "contents0": primary_content,
            "has_override": isinstance(data.get("start_urls_override"), list) and bool(data.get("start_urls_override")),
        },
        hypothesis_id="H1",
    )
    try:
        logger.debug(
            "[Dispatch] entry payload | job_id=%s override_count=%s override_source=%s contents0=%s",
            job_id,
            len(data.get("start_urls_override") or []) if isinstance(data.get("start_urls_override"), list) else 0,
            data.get("start_urls_override_source"),
            primary_content,
        )
    except Exception:
        pass
    # endregion

    # ?숈씪 job_id媛 ?대? ?ㅽ뻾 以묒씠硫??덈줈 ??뼱?곗? ?딄퀬 洹몃?濡??묐떟
    celery_active = False
    if _crawl_workflow_via_celery():
        celery_active = await _crawl_wf_redis_job_active(job_id)
    existing_task = crawler_state.workflow_tasks.get(job_id)
    try:
        logger.debug(
            "%s[dispatch_entry] job_id=%s celery_active=%s existing_task_active=%s active=%s env_max=%s use_celery=%s",
            CONCURRENT_CRAWL_LOG_PREFIX,
            job_id,
            celery_active,
            bool(existing_task and not existing_task.done()),
            crawler_state.get_workflow_debug_snapshot(),
            os.getenv("CRAWL_MAX_ACTIVE_WORKFLOWS"),
            _crawl_workflow_via_celery(),
        )
    except Exception:
        logger.debug("%s[dispatch_entry] snapshot failed", CONCURRENT_CRAWL_LOG_PREFIX, exc_info=True)
    if (existing_task and not existing_task.done()) or celery_active:
        try:
            req_colle = str(data.get("colle") or "").strip().lower()
        except Exception:
            req_colle = ""
        try:
            existing_wf = crawler_state.workflows.get(job_id)
        except Exception:
            existing_wf = None
        try:
            existing_colle = str(getattr(existing_wf, "ui_colle", "") or "").strip().lower() if existing_wf else ""
        except Exception:
            existing_colle = ""

        existing_mode = existing_colle
        try:
            if existing_wf is not None and not existing_mode:
                if isinstance(existing_wf, BoardContentWorkflow):
                    _cm = str(getattr(existing_wf, "colle", "board") or "board").strip().lower()
                    existing_mode = _cm if _cm in ("board", "file") else "board"
                elif isinstance(existing_wf, IntegratedWorkflow):
                    existing_mode = "file"
        except Exception:
            existing_mode = existing_colle or ""

        if req_colle and existing_mode and req_colle != existing_mode:
            try:
                from utils.db_name import resolve_db_name
                existing_db = (
                    resolve_db_name(data)
                    or crawler_state.job_history.get(job_id, {}).get("db_name")
                    or "dev_user"
                )
            except Exception:
                existing_db = (
                    data.get("db_name")
                    or data.get("dbname")
                    or data.get("account_name")
                    or crawler_state.job_history.get(job_id, {}).get("db_name")
                    or "dev_user"
                )
            logger.warning(
                "[Dispatch] job_id collision across modes; rejecting | job_id=%s existing_colle=%s req_colle=%s",
                job_id,
                existing_mode,
                req_colle,
            )
            crawler_state.record_history(
                job_id,
                "creation_failed",
                f"job_id_collision:existing={existing_mode}:requested={req_colle}",
                existing_db,
                chat_bot_id=(getattr(existing_wf, "chat_bot_id", None) if existing_wf else None),
            )
            return JSONResponse(
                {
                    "status": "error",
                    "message": "job_id is already running with a different mode (colle). Please generate a new job_id.",
                    "job_id": job_id,
                    "existing_colle": existing_mode,
                    "requested_colle": req_colle,
                },
                status_code=409,
            )
        try:
            from utils.db_name import resolve_db_name
            existing_db = (
                resolve_db_name(data)
                or crawler_state.job_history.get(job_id, {}).get("db_name")
                or "dev_user"
            )
        except Exception:
            existing_db = (
                data.get("db_name")
                or data.get("dbname")
                or data.get("account_name")
                or crawler_state.job_history.get(job_id, {}).get("db_name")
                or "dev_user"
            )
        return JSONResponse({"status": "start", "job_id": job_id, "db_name": existing_db})

    craw_id = (
        data.get("id")
        or data.get("craw_id")
        or data.get("crawling_log_id")
        or data.get("log_id")
        or ""
    )
    target_date = data.get("target_date", [])

    start_urls_resolution = await resolve_seed_urls(
        data,
        contents=contents,
        job_id=job_id,
        header_response=header_response,
    )
    start_urls = start_urls_resolution.start_urls
    use_query_links_only = start_urls_resolution.use_query_links_only
    override_source = start_urls_resolution.override_source

    # ?뚯씪 ?щ·留? ?대? 寃뚯떆臾?detail) URL?대?濡?紐⑸줉 ?뺤옣쨌源딆씠 ?먯깋 ?놁씠 ?곸꽭留?泥섎━
    try:
        _ov_src = str(data.get("start_urls_override_source") or override_source or "").strip()
    except Exception:
        _ov_src = ""
    try:
        _dispatch_file_flow = _payload_is_file_crawl_intent(data, _ov_src)
    except Exception:
        _dispatch_file_flow = False
    _dispatch_flow_log = logger.debug if _dispatch_file_flow else logger.info
    if _ov_src in ("file_crawl_post_db", "file_crawl_post_db_stream") and (
        start_urls or int(data.get("pre_explored_start_urls_count") or 0) > 0
    ):
        use_query_links_only = True

    # db_name ???쇱슜 ??? db_name / dbname / account_name ??(?먭린 ?묐떟쨌?댄썑 怨듯넻)
    try:
        from utils.db_name import resolve_db_name
        db_name = resolve_db_name(data, default="dev_user")
    except Exception:
        db_name = data.get("db_name") or data.get("dbname") or data.get("account_name") or "dev_user"
    raw_chat_bot_id = data.get("chat_bot_id") or (data.get("metadata") or {}).get("chat_bot_id")
    chat_bot_id = resolve_chat_bot_id(job_id, raw_chat_bot_id)
    if chat_bot_id:
        data["chat_bot_id"] = chat_bot_id
    try:
        _colle_mode = str(data.get("colle") or "").strip().lower()
    except Exception:
        _colle_mode = ""

    try:
        _file_pipeline_mode = _ov_src in ("file_crawl_post_db", "file_crawl_post_db_stream")
    except Exception:
        _file_pipeline_mode = False

    if _colle_mode == "file" or _dispatch_file_flow:
        from backend.file.file_wait_policy import apply_file_wait_config_to_payload

        await apply_file_wait_config_to_payload(
            data,
            db_name=str(db_name),
            chat_bot_id=str(chat_bot_id or ""),
            job_id=str(job_id),
        )

    if _force_direct_detail_enabled(data) and not craw_id:
        _dispatch_flow_log(
            "[Dispatch] direct detail probe skips PHP crawling log wait | job_id=%s db=%s",
            job_id,
            db_name,
        )
    else:
        wait_log_t0 = time.perf_counter()
        _dispatch_flow_log(
            "[BottleneckTrace][php_log_wait_start] job_id=%s db=%s craw_id=%s elapsed_ms=%s",
            job_id,
            db_name,
            craw_id,
            int((wait_log_t0 - dispatch_t0) * 1000),
        )
        resolved_craw_id = await _wait_for_php_crawling_log(job_id, db_name, craw_id)
        if resolved_craw_id and not craw_id:
            craw_id = str(resolved_craw_id)
            data["craw_id"] = craw_id
        _dispatch_flow_log(
            "[BottleneckTrace][php_log_wait_done] job_id=%s wait_ms=%s elapsed_ms=%s",
            job_id,
            int((time.perf_counter() - wait_log_t0) * 1000),
            int((time.perf_counter() - dispatch_t0) * 1000),
        )

    if _is_file_start_urls_db_branch_enabled(data, _ov_src):
        file_start_urls = await _resolve_file_start_urls_from_exploration_posts(
            data,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
        )
        if file_start_urls is not None:
            start_urls = file_start_urls
            use_query_links_only = True
            override_source = "file_crawl_post_db"
            _ov_src = "file_crawl_post_db"
    try:
        _file_pipeline_mode = _ov_src in ("file_crawl_post_db", "file_crawl_post_db_stream")
    except Exception:
        _file_pipeline_mode = False

    if not start_urls:
        try:
            _file_pipeline_mode = _ov_src in ("file_crawl_post_db", "file_crawl_post_db_stream")
        except Exception:
            _file_pipeline_mode = False
        try:
            _pre_count_hint = int(data.get("pre_explored_start_urls_count") or 0)
        except Exception:
            _pre_count_hint = 0
        if isinstance(contents, list):
            _contents0 = contents[0] if contents else None
        else:
            _contents0 = contents
        fallback_contents_url = ""
        if (
            (_payload_is_file_crawl_intent(data, _ov_src) or _file_pipeline_mode)
            and _contents0
        ):
            try:
                fallback_contents_url = normalize_known_start_url_alias(ensure_url_scheme(str(_contents0).strip()))
            except Exception:
                fallback_contents_url = ""
        if data.get("_file_start_urls_db_branch_applied"):
            logger.warning(
                "[Dispatch] file exploration type=post branch returned empty start_urls; skip broad board discovery | job_id=%s override_source=%s reason=%s seed=%s",
                job_id,
                _ov_src,
                data.get("_file_start_urls_db_branch_failure_reason") or "file_start_urls_empty",
                fallback_contents_url,
            )
            # File crawling must consume exploration content_type=post rows only.
            # Falling back to a site home page can expand into unrelated site-wide URLs.
            fallback_contents_url = ""
        elif _file_pipeline_mode and _pre_count_hint > 0:
            logger.debug(
                "[Dispatch] keep empty start_urls for file stream mode | job_id=%s pre_count=%s override_source=%s",
                job_id,
                _pre_count_hint,
                _ov_src,
            )
        if fallback_contents_url and not (_ov_src == "file_crawl_post_db_stream" and _pre_count_hint > 0):
            discovered_start_urls = await _resolve_file_start_urls_from_board_static_discovery(
                fallback_contents_url,
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                job_id=job_id,
            )
            if discovered_start_urls:
                start_urls = list(discovered_start_urls)
                use_query_links_only = True
                override_source = "file_board_static_discovery"
                _ov_src = "file_board_static_discovery"
                try:
                    data["start_urls_override"] = list(start_urls)
                    data["start_urls_override_source"] = "file_board_static_discovery"
                    data["pre_explored_start_urls_count"] = len(start_urls)
                    data["selected_start_urls_count"] = len(start_urls)
                    data["actual_start_urls_count"] = len(start_urls)
                    data["exploration_post_total_count"] = 0
                    data["_file_start_urls_static_discovery_applied"] = True
                    data["_file_start_urls_db_branch_applied"] = False
                    data.pop("_file_start_urls_db_branch_failure_reason", None)
                    data.pop("_file_start_urls_db_branch_failure_message", None)
                except Exception:
                    pass
            if data.get("_file_start_urls_db_branch_applied"):
                logger.warning(
                    "[Dispatch] file post start_urls empty after exploration and board discovery; contents_url will not be enqueued directly | job_id=%s seed=%s",
                    job_id,
                    fallback_contents_url,
                )

    try:
        if (
            start_urls
            and not _force_direct_detail_enabled(data)
            and _ov_src != "partial_content_relearn"
            and not (_payload_is_file_crawl_intent(data, _ov_src) or _file_pipeline_mode)
            and not (
                _ov_src == "contents_url_fallback"
                and (_payload_is_file_crawl_intent(data, _ov_src) or _file_pipeline_mode)
            )
        ):
            dispatch_dedupe_t0 = time.perf_counter()
            _dispatch_flow_log(
                "[BottleneckTrace][dispatch_dedupe_start] job_id=%s start_urls=%s elapsed_ms=%s",
                job_id,
                len(start_urls or []),
                int((dispatch_dedupe_t0 - dispatch_t0) * 1000),
            )
            metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
            request_chat_bot_id = data.get("chat_bot_id") or metadata.get("chat_bot_id")
            start_urls, learn_list_dedupe_meta = await apply_learn_list_start_url_dedupe(
                data=data,
                start_urls=start_urls,
                db_name=db_name,
                chat_bot_id=resolve_chat_bot_id(job_id, request_chat_bot_id),
            )
            if learn_list_dedupe_meta.get("enabled"):
                data["learn_list_duplicate_exclude_result"] = learn_list_dedupe_meta
                try:
                    before_count = int(learn_list_dedupe_meta.get("before") or 0)
                    after_count = int(learn_list_dedupe_meta.get("after") or len(start_urls or []))
                    data["learn_list_duplicate_exclude_scan_count"] = before_count
                    data["learn_list_duplicate_exclude_selected_count"] = after_count
                    data["selected_start_urls_count"] = after_count
                    data["actual_start_urls_count"] = after_count
                    data["pre_explored_start_urls_count"] = max(
                        int(data.get("pre_explored_start_urls_count") or 0),
                        before_count,
                    )
                except Exception:
                    data["selected_start_urls_count"] = len(start_urls or [])
                    data["actual_start_urls_count"] = len(start_urls or [])
                    data["pre_explored_start_urls_count"] = max(
                        int(data.get("pre_explored_start_urls_count") or 0),
                        len(start_urls or []),
                    )
                _dispatch_flow_log(
                    "[LargeModeTargetUrls] dispatch selected remaining start_urls | job_id=%s db=%s chat_bot_id=%s before=%s selected=%s duplicates=%s table=%s",
                    job_id,
                    db_name,
                    resolve_chat_bot_id(job_id, request_chat_bot_id),
                    learn_list_dedupe_meta.get("before"),
                    learn_list_dedupe_meta.get("after"),
                    learn_list_dedupe_meta.get("duplicates"),
                    learn_list_dedupe_meta.get("table"),
                )
            _dispatch_flow_log(
                "[BottleneckTrace][dispatch_dedupe_done] job_id=%s dedupe_ms=%s elapsed_ms=%s start_urls=%s enabled=%s",
                job_id,
                int((time.perf_counter() - dispatch_dedupe_t0) * 1000),
                int((time.perf_counter() - dispatch_t0) * 1000),
                len(start_urls or []),
                learn_list_dedupe_meta.get("enabled") if isinstance(learn_list_dedupe_meta, dict) else None,
            )
        elif start_urls and _force_direct_detail_enabled(data):
            _dispatch_flow_log(
                "[LearnListStartUrlDedupe] skipped for direct detail probe | job_id=%s count=%s",
                job_id,
                len(start_urls or []),
            )
        elif start_urls and _ov_src == "partial_content_relearn":
            _dispatch_flow_log(
                "[LearnListStartUrlDedupe] skipped for partial content relearn | job_id=%s count=%s",
                job_id,
                len(start_urls or []),
            )
        elif start_urls and _ov_src == "contents_url_fallback":
            _dispatch_flow_log(
                "[LearnListStartUrlDedupe] skipped for contents_url fallback | job_id=%s count=%s colle=%s",
                job_id,
                len(start_urls or []),
                _colle_mode,
            )
        elif start_urls and (_payload_is_file_crawl_intent(data, _ov_src) or _file_pipeline_mode):
            _dispatch_flow_log(
                "[FileUrlTrace][learn_list_dedupe.skip_file_crawl] job_id=%s count=%s override_source=%s reason=file_crawl_post_urls_must_not_match_board_url_duplicates",
                job_id,
                len(start_urls or []),
                _ov_src,
            )
    except Exception as exc:
        logger.warning(
            "[LearnListStartUrlDedupe] failed open | job_id=%s db=%s err=%s",
            job_id,
            db_name,
            exc,
            exc_info=True,
        )

    stream_mode_keeps_empty_start_urls = (
        _ov_src == "file_crawl_post_db_stream"
        and int(data.get("pre_explored_start_urls_count") or 0) > 0
        and not start_urls
    )

    if data.get("_file_start_urls_db_branch_applied"):
        scope_filter_meta = {
            "requested_path_prefix": "",
            "identities": [],
            "before": len(start_urls or []),
            "after": len(start_urls or []),
            "skipped": 0,
            "samples": [],
            "skipped_reason": "file_exploration_type_post_all",
        }
        try:
            data["scope_path_prefix"] = ""
        except Exception:
            pass
        _dispatch_flow_log(
            "[START_URLS_SCOPE] dispatch filter skipped | job_id=%s override_source=%s reason=file_exploration_type_post_all count=%s",
            job_id,
            _ov_src,
            len(start_urls or []),
        )
    else:
        start_urls, scope_filter_meta = _filter_start_urls_by_requested_scope(data, start_urls)
    if start_urls:
        try:
            if data.get("learn_list_duplicate_exclude_result"):
                data["learn_list_duplicate_exclude_selected_count"] = len(start_urls)
                data["selected_start_urls_count"] = len(start_urls)
                data["actual_start_urls_count"] = len(start_urls)
            elif not int(data.get("exploration_post_total_count") or 0):
                data["pre_explored_start_urls_count"] = len(start_urls)
        except Exception:
            pass
    try:
        exploration_post_total = int(data.get("exploration_post_total_count") or 0)
    except Exception:
        exploration_post_total = 0
    duplicate_exclude_selected = bool(data.get("learn_list_duplicate_exclude_result"))
    if exploration_post_total > 0:
        data["exploration_display_count_fixed"] = True
        data["exploration_display_max_count"] = exploration_post_total
        data["actual_start_urls_count"] = len(start_urls or [])
        if data.get("learn_list_duplicate_exclude_result"):
            data["pre_explored_start_urls_count"] = exploration_post_total
            data["learn_list_duplicate_exclude_selected_count"] = len(start_urls or [])
            data["selected_start_urls_count"] = len(start_urls or [])
        try:
            if not int(data.get("pre_explored_start_urls_count") or 0):
                data["pre_explored_start_urls_count"] = len(start_urls or [])
        except Exception:
            data["pre_explored_start_urls_count"] = len(start_urls or [])
        _dispatch_flow_log(
            "[Dispatch] start_urls display total fixed | job_id=%s exploration_total=%s actual_start_urls=%s target_count=%s duplicate_exclude=%s",
            job_id,
            exploration_post_total,
            len(start_urls or []),
            data.get("pre_explored_start_urls_count"),
            duplicate_exclude_selected,
        )
    elif (not start_urls) and scope_filter_meta.get("requested_path_prefix") and not stream_mode_keeps_empty_start_urls:
        try:
            data["pre_explored_start_urls_count"] = 0
        except Exception:
            pass
    logger.debug(
        "[START_URLS_SCOPE] dispatch filter | job_id=%s requested_path_prefix=%s identities=%s before=%s after=%s skipped=%s skipped_samples=%s",
        job_id,
        scope_filter_meta.get("requested_path_prefix") or "",
        scope_filter_meta.get("identities") or [],
        scope_filter_meta.get("before"),
        scope_filter_meta.get("after"),
        scope_filter_meta.get("skipped"),
        scope_filter_meta.get("samples") or [],
    )

    if start_urls:
        ordered_start_urls = _apply_start_urls_order(data, start_urls)
        if ordered_start_urls is not start_urls:
            start_urls = ordered_start_urls
            try:
                _dispatch_flow_log(
                    "[START_URLS_ORDER] applied | job_id=%s order=%s count=%s sample=%s",
                    job_id,
                    _resolve_start_urls_order(data),
                    len(start_urls),
                    start_urls[:3],
                )
            except Exception:
                pass

    if not start_urls and not (
        _ov_src == "file_crawl_post_db_stream" and int(data.get("pre_explored_start_urls_count") or 0) > 0
    ):
        dedupe_meta = data.get("learn_list_duplicate_exclude_result")
        if _is_all_start_urls_duplicate_excluded(dedupe_meta):
            try:
                before_count = int(dedupe_meta.get("before") or 0)
            except Exception:
                before_count = int(data.get("pre_explored_start_urls_count") or 0)
            try:
                duplicate_count = int(dedupe_meta.get("duplicates") or before_count)
            except Exception:
                duplicate_count = before_count
            crawler_state.record_history(
                job_id,
                "completed",
                "all_start_urls_duplicate_excluded",
                db_name,
                chat_bot_id=chat_bot_id,
            )
            complete_msg = (
                f"?섏쭛 ???{before_count}嫄댁씠 紐⑤몢 湲곗〈 ?숈뒿 ?곗씠?곗? 以묐났?섏뼱 "
                "?덈줈 ?섏쭛??URL???놁뒿?덈떎."
            )
            try:
                await publish_job_terminal_completed(
                    job_id,
                    db_name,
                    reason="all_start_urls_duplicate_excluded",
                    message=complete_msg,
                    source="dispatch_all_duplicates_completed",
                    scan_count=before_count,
                    duplicate_count=duplicate_count,
                )
            except Exception:
                logger.debug("[Dispatch] publish_job_terminal_completed failed (ignore)", exc_info=True)
            return JSONResponse(
                {
                    "status": "completed",
                    "job_id": job_id,
                    "db_name": db_name,
                    "reason": "all_start_urls_duplicate_excluded",
                    "message": complete_msg,
                    "scan_count": before_count,
                    "duplicate_count": duplicate_count,
                    "selected_start_urls_count": 0,
                },
                status_code=200,
            )
        file_branch_failure_reason = str(data.get("_file_start_urls_db_branch_failure_reason") or "").strip()
        if not file_branch_failure_reason and (not contents or not contents[0]):
            return JSONResponse({"status": "error", "message": "URL is required"}, status_code=400)
        crawler_state.record_history(
            job_id,
            "failed",
            file_branch_failure_reason or "no_start_urls",
            db_name,
            chat_bot_id=chat_bot_id,
        )
        err_reason = file_branch_failure_reason or "no_start_urls"
        err_msg = (
            str(data.get("_file_start_urls_db_branch_failure_message") or "").strip()
            or "크롤링 대상 게시글 URL을 찾지 못했습니다."
        )
        try:
            await publish_job_terminal_error(
                job_id,
                db_name,
                reason=err_reason,
                message=err_msg,
                source="dispatch_file_learn_list_scope_failed" if file_branch_failure_reason else "dispatch_no_start_urls",
            )
        except Exception:
            logger.debug("[Dispatch] publish_job_terminal_error failed (ignore)", exc_info=True)
        logger.error(
            "[Dispatch] Job failed: start_urls 0건(contents_url 범위 및 LEARN_LIST fallback 결과 없음) | job_id=%s reason=%s",
            job_id,
            err_reason,
        )
        return JSONResponse(
            {
                "status": "error",
                "job_id": job_id,
                "db_name": db_name,
                "reason": err_reason,
                "message": err_msg,
            },
            status_code=422,
        )

    bootstrap_t0 = time.perf_counter()
    try:
        bootstrap_timeout_sec = float(os.getenv("DISPATCH_BOOTSTRAP_TIMEOUT_SEC", "8") or "8")
    except Exception:
        bootstrap_timeout_sec = 8.0
    bootstrap_timeout_sec = max(1.0, min(bootstrap_timeout_sec, 30.0))
    try:
        logger.debug(
            "[BottleneckTrace][dispatch_bootstrap_start] job_id=%s db=%s start_urls=%s selected=%s timeout_sec=%.1f",
            job_id,
            db_name,
            len(start_urls or []),
            int(data.get("selected_start_urls_count") or len(start_urls or [])),
            bootstrap_timeout_sec,
        )
        init_success = await asyncio.wait_for(
            bootstrap_job_state(job_id, db_name, "dispatch"),
            timeout=bootstrap_timeout_sec,
        )
        logger.debug(
            "[BottleneckTrace][dispatch_bootstrap_done] job_id=%s db=%s ok=%s elapsed_ms=%s",
            job_id,
            db_name,
            bool(init_success),
            int((time.perf_counter() - bootstrap_t0) * 1000),
        )
    except asyncio.TimeoutError:
        init_success = False
        logger.debug(
            "[BottleneckTrace][dispatch_bootstrap_timeout] job_id=%s db=%s timeout_sec=%.1f elapsed_ms=%s action=continue_schedule",
            job_id,
            db_name,
            bootstrap_timeout_sec,
            int((time.perf_counter() - bootstrap_t0) * 1000),
        )
    except Exception as exc:
        init_success = False
        logger.debug(
            "[BottleneckTrace][dispatch_bootstrap_failed] job_id=%s db=%s elapsed_ms=%s err=%s action=continue_schedule",
            job_id,
            db_name,
            int((time.perf_counter() - bootstrap_t0) * 1000),
            exc,
            exc_info=True,
        )
    if not init_success:
        logger.debug("[Dispatch] Bootstrap reported failure | job_id=%s db=%s", job_id, db_name)

    try:
        _dispatch_flow_log(
            "[Dispatch] start_urls resolved | job_id=%s count=%s selected=%s display_total=%s override_source=%s use_query_links_only=%s sample=%s",
            job_id,
            len(start_urls or []),
            int(data.get("selected_start_urls_count") or len(start_urls or [])),
            int(data.get("pre_explored_start_urls_count") or len(start_urls or [])),
            (data.get("start_urls_override_source") or override_source),
            bool(use_query_links_only),
            (start_urls or [])[:5],
        )
    except Exception:
        pass

    try:
        payload = {
            "event": "start_urls_determined",
            "status": "dispatch_start_urls",
            "start_urls_count": len(start_urls or []),
            "scan_count": int(data.get("pre_explored_start_urls_count") or len(start_urls or [])),
            "total_count": int(data.get("pre_explored_start_urls_count") or len(start_urls or [])),
            "actual_start_urls_count": len(start_urls or []),
            "selected_start_urls_count": int(data.get("selected_start_urls_count") or len(start_urls or [])),
            "pre_explored_start_urls_count": int(data.get("pre_explored_start_urls_count") or len(start_urls or [])),
            "exploration_post_total_count": int(data.get("exploration_post_total_count") or 0),
            "exploration_display_max_count": int(data.get("exploration_display_max_count") or 0),
            "start_urls_sample": (start_urls or [])[:10],
            "start_urls_override_source": data.get("start_urls_override_source") or override_source,
            "use_query_links_only": bool(use_query_links_only),
            "learn_list_duplicate_exclude_result": data.get("learn_list_duplicate_exclude_result"),
        }
        enqueue_sse_message(job_id, payload, db_name, "dispatch_start_urls", priority=0)
    except Exception:
        logger.debug("[Dispatch] failed to enqueue start_urls debug SSE", exc_info=True)

    start_date = None
    end_date = None
    if target_date and isinstance(target_date, list) and len(target_date) >= 2:
        start_raw = str(target_date[0] or "").strip()
        end_raw = str(target_date[1] or "").strip()
        parsed = False
        for fmt in ("%Y-%m-%d", "%y-%m-%d"):
            try:
                start_date = datetime.strptime(start_raw, fmt)
                end_date = datetime.strptime(end_raw, fmt)
                parsed = True
                break
            except Exception:
                start_date = None
                end_date = None
        if not parsed:
            start_date = None
            end_date = None

    try:
        import json as _json, time as _time
        payload = {
            "sessionId": "debug-session",
            "runId": "run1",
            "hypothesisId": "H_PARSE",
            "location": "backend/shared/crawl_dispatcher.py:dispatch_and_schedule_workflow:date_parse",
            "message": "parsed target_date",
            "data": {
                "job_id": job_id,
                "target_date_raw": target_date,
                "start_raw": start_raw,
                "end_raw": end_raw,
                "start_date": getattr(start_date, "isoformat", lambda: str(start_date))(),
                "end_date": getattr(end_date, "isoformat", lambda: str(end_date))(),
                "parsed": bool(parsed),
            },
            "timestamp": int(_time.time() * 1000),
        }
        logging.getLogger("backend.shared.crawl_dispatcher").debug(_json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass

    # --- Celery ?뚯빱?먯꽌 ?뚰겕?뚮줈 ?ㅽ뻾 (uvicorn 遺??遺꾨━) ---
    if _crawl_workflow_via_celery():
        _songpa_title_trace(
            "dispatch_celery_payload_store",
            url=str(data.get("contents_url") or data.get("access_url") or primary_content or ""),
            job_id=job_id,
            colle=data.get("colle"),
            start_urls_count=len(start_urls or []),
            start_urls_first=(start_urls[0] if start_urls else ""),
        )
        try:
            from db.db_redis import get_redis

            payload = {
                "data": data,
                "start_urls": start_urls,
                "start_date_iso": start_date.isoformat() if start_date else None,
                "end_date_iso": end_date.isoformat() if end_date else None,
                "job_id": job_id,
                "craw_id": craw_id,
                "db_name": db_name,
                "chat_bot_id": chat_bot_id,
                "use_query_links_only": bool(use_query_links_only),
                "override_source": (data.get("start_urls_override_source") or override_source or ""),
                "primary_content": primary_content,
            }
            raw = json.dumps(payload, ensure_ascii=False, default=str)
            r = await get_redis()
            await r.set(f"crawl_wf_payload:{job_id}", raw, ex=172800)
        except Exception as exc:
            logger.exception("%s[celery_payload_store_failed] job_id=%s err=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id, exc)
            crawler_state.record_history(job_id, "creation_failed", f"celery_payload_failed:{exc}", db_name, chat_bot_id=chat_bot_id)
            return JSONResponse(
                {"status": "error", "message": "Failed to queue crawl job (redis payload)"},
                status_code=500,
            )

        celery_task_id = None
        try:
            try:
                from backend.src.tasks.task_sender import send_crawl_workflow_dispatch_job
            except Exception:
                from src.tasks.task_sender import send_crawl_workflow_dispatch_job
            celery_task_id = send_crawl_workflow_dispatch_job(job_id)
        except Exception as exc:
            logger.debug("%s[celery_enqueue_failed] job_id=%s err=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id, exc)
            celery_task_id = None

        if not celery_task_id:
            try:
                from db.db_redis import get_redis

                r = await get_redis()
                await r.delete(f"crawl_wf_payload:{job_id}")
            except Exception:
                pass
            crawler_state.record_history(job_id, "creation_failed", "celery_enqueue_failed", db_name, chat_bot_id=chat_bot_id)
            return JSONResponse(
                {"status": "error", "message": "Failed to queue crawl job (Celery). Check worker and broker."},
                status_code=503,
            )

        try:
            from db.db_redis import get_redis

            r = await get_redis()
            await r.set(f"crawl_wf_active:{job_id}", celery_task_id, ex=172800)
        except Exception:
            pass

        crawler_state.record_history(job_id, "created", "workflow_queued_celery", db_name, chat_bot_id=chat_bot_id)
        logger.debug(
            "%s[celery_queued] job_id=%s celery_task_id=%s db=%s start_urls=%s",
            CONCURRENT_CRAWL_LOG_PREFIX,
            job_id,
            celery_task_id,
            db_name,
            len(start_urls or []),
        )
        return JSONResponse({"status": "start", "job_id": job_id, "db_name": db_name, "runner": "celery", "celery_task_id": celery_task_id})

    workflow = assemble_workflow_after_url_resolve(
        data=data,
        start_urls=start_urls,
        start_date=start_date,
        end_date=end_date,
        job_id=job_id,
        craw_id=craw_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        use_query_links_only=use_query_links_only,
        override_source=override_source,
        primary_content=primary_content,
    )
    try:
        logger.debug(
            "[Dispatch] workflow assembled | job_id=%s type=%s",
            job_id,
            type(workflow).__name__,
        )
    except Exception:
        pass
    _songpa_title_trace(
        "dispatch_workflow_assembled",
        url=str(data.get("contents_url") or data.get("access_url") or primary_content or ""),
        job_id=job_id,
        workflow_type=type(workflow).__name__,
        workflow_colle=getattr(workflow, "colle", ""),
        workflow_ui_colle=getattr(workflow, "ui_colle", ""),
        start_urls_count=len(start_urls or []),
        start_urls_first=(start_urls[0] if start_urls else ""),
    )

    crawler_state.workflows[job_id] = workflow
    crawler_state.record_history(job_id, "created", "workflow_registered", db_name, chat_bot_id=chat_bot_id)
    try:
        logger.debug(
            "%s[workflow_registered] job_id=%s workflow=%s active=%s",
            CONCURRENT_CRAWL_LOG_PREFIX,
            job_id,
            type(workflow).__name__,
            crawler_state.get_workflow_debug_snapshot(),
        )
    except Exception:
        logger.debug("%s[workflow_registered] snapshot failed", CONCURRENT_CRAWL_LOG_PREFIX, exc_info=True)

    dispatch_monitor_enabled = str(
        os.getenv("WORKFLOW_AUTO_STOP_DISPATCH_MONITOR", "0") or "0"
    ).strip().lower() in ("1", "true", "yes", "on")
    if dispatch_monitor_enabled:
        _debug_log(
            location="backend/crawl_dispatcher.py:dispatch_and_schedule_workflow:auto_monitor",
            message="auto_monitor_start",
            data={
                "job_id": job_id,
                "db_name": db_name,
                "chat_bot_id": chat_bot_id,
                "workflow": type(workflow).__name__,
            },
            hypothesis_id="H_MON",
        )
        try:
            start_local_time = get_local_now()
        except Exception:
            start_local_time = None
        try:
            asyncio.create_task(
                monitor_auto_stop(
                    workflow=workflow,
                    job_id=job_id,
                    db_name=db_name,
                    chat_bot_id=chat_bot_id,
                    stop_signal=getattr(workflow, "stop_event", None) or asyncio.Event(),
                    start_time=start_local_time,
                    source="dispatch",
                ),
                name=f"auto_stop_dispatch:{job_id}",
            )
        except Exception as exc:
            logger.warning("[Dispatch] auto-monitor start failed | job_id=%s err=%s", job_id, exc)
    else:
        logger.debug("[Dispatch] AutoMonitor skipped; workflow_runner owns monitor | job_id=%s", job_id)

    try:
        t = asyncio.create_task(
            run_workflow_task(workflow, start_urls, start_date, end_date, job_id, craw_id, db_name, chat_bot_id, use_query_links_only),
            name=f"run_workflow_task:{job_id}",
        )
        try:
            import json as _json, time as _time
            payload = {
                "sessionId": "debug-session",
                "runId": "run1",
                "hypothesisId": "H_PASS",
                "location": "backend/shared/crawl_dispatcher.py:dispatch_and_schedule_workflow:before_run_workflow_task",
                "message": "scheduling run_workflow_task",
                "data": {
                    "job_id": job_id,
                    "workflow": type(workflow).__name__,
                    "start_date": getattr(start_date, "isoformat", lambda: str(start_date))(),
                    "end_date": getattr(end_date, "isoformat", lambda: str(end_date))(),
                    "start_urls_count": len(start_urls or []),
                },
                "timestamp": int(_time.time() * 1000),
            }
            logging.getLogger("backend.shared.crawl_dispatcher").debug(_json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass
        crawler_state.workflow_tasks[job_id] = t

        def _on_workflow_task_done(tt, jid=job_id):
            crawler_state.workflow_tasks.pop(jid, None)
            try:
                history = dict(crawler_state.job_history.get(jid) or {})
                workflow_obj = crawler_state.workflows.get(jid)
            except Exception:
                logger.debug("%s[workflow_task_done] snapshot failed", CONCURRENT_CRAWL_LOG_PREFIX, exc_info=True)

        t.add_done_callback(_on_workflow_task_done)
        t.add_done_callback(lambda tt: swallow_task_exception(tt, label="run_workflow_task"))
        
        logger.debug(
            "[BottleneckTrace][dispatch_task_created] job_id=%s dispatch_total_ms=%s start_urls=%s",
            job_id,
            int((time.perf_counter() - dispatch_t0) * 1000),
            len(start_urls or []),
        )
    except Exception as exc:
        logger.exception("[Dispatch] Failed to schedule workflow task | job_id=%s db=%s err=%s", job_id, db_name, exc)
        crawler_state.record_history(job_id, "creation_failed", f"schedule_failed:{exc}", db_name, chat_bot_id=chat_bot_id)
        return JSONResponse({"status": "error", "message": "Failed to start workflow"}, status_code=500)

    return JSONResponse({"status": "start", "job_id": job_id, "db_name": db_name})



