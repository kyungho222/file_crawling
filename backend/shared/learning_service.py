"""
?숈뒿 ?쒕퉬??紐⑤뱢
- ?뚯씪 ????꾨즺 ??媛쒕퀎 ?숈뒿 泥섎━
- LEARN_LIST.status = 'Y' ?낅뜲?댄듃
- TRAINING_PROCESS 湲곕줉 異붽?
- status=Y 諛섏쁺 ?? web_title 蹂닿컯쨌progress_callback ?ㅼ쓬 POST /summarize_keywords
  (?섍꼍蹂??SUMMARIZE_KEYWORDS_FIRE_AND_FORGET=1 ?대㈃ ?숈씪 ?몄옄濡?鍮꾨룞湲??쒖뒪?щ쭔 ?ㅼ?以?
"""
import os
import time
import asyncio
import logging
import inspect
import json
from datetime import datetime
from typing import Optional, Callable, Any, Dict, Iterable
from db.maria_operations import maria_execute_query
from db.db_operations import execute_query as pg_execute_query
from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from utils.whoami import get_chat_id_from_db
from backend.shared.pre_explored_url import get_url_pattern_raw

logger = logging.getLogger(__name__)

# ??⑸웾 ?뚯씪 ?숈뒿 ??PG 泥?겕 諛섏쁺 ?湲??쒓컙 (湲곕낯 5遺? LEARN_PG_WAIT_TIMEOUT_SEC 濡?議곗젅)
_DEFAULT_PG_WAIT_SEC = float(os.getenv("LEARN_PG_WAIT_TIMEOUT_SEC", "300") or "300")
_DEFAULT_PG_WAIT_SEC = max(30.0, min(_DEFAULT_PG_WAIT_SEC, 1800.0))
_DEFAULT_PG_WAIT_URL_SEC = float(os.getenv("LEARN_PG_WAIT_TIMEOUT_URL_SEC", "45") or "45")
_DEFAULT_PG_WAIT_URL_SEC = max(10.0, min(_DEFAULT_PG_WAIT_URL_SEC, 600.0))

_GENERIC_DYNAMIC_BASENAMES = frozenset(
    {
        "view.do",
        "list.do",
        "detail.do",
        "board.do",
        "index.do",
        "selectbbsnttview.do",
        "selectbbslist.do",
        "selectbbsnttlist.do",
    }
)


def _compact_log_body(value: Optional[str], *, limit: int = 240) -> str:
    if not value:
        return ""
    try:
        compact = " ".join(str(value).split())
    except Exception:
        compact = str(value)
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def _http_candidate_preview(value: Optional[str], *, limit: int = 160) -> str:
    if not isinstance(value, str):
        return ""
    return _compact_log_body(value, limit=limit)


def _is_http_candidate(value: Optional[str]) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().startswith(("http://", "https://"))


def _build_non_empty_cate_update_kwargs(row: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    理쒖쥌 status=Y 諛섏쁺 ??鍮?cate 媛믪쑝濡?湲곗〈 遺꾨쪟瑜???뼱?곗? ?딅룄濡?    鍮꾩뼱 ?덉? ?딆? cate留?UPDATE kwargs 濡??섍릿??

    learning ?④퀎? 遺꾨쪟 蹂댁젙 ?④퀎媛 ?뉕컝由????덉뼱?? 鍮?臾몄옄??None ?
    "?꾩옱 row 媛믪쓣 鍮꾩슦寃좊떎"媛 ?꾨땲??"嫄대뱶由ъ? ?딄쿋??濡?痍④툒?섎뒗 ?몄씠 ?덉쟾?섎떎.
    """
    if not isinstance(row, dict):
        return {}

    out: Dict[str, str] = {}
    for key in ("cate1", "cate2"):
        try:
            value = str(row.get(key) or "").strip()
        except Exception:
            value = ""
        if value:
            out[key] = value
    return out


def _safe_content_created_at_select_expr(column_name: str = "content_created_at") -> str:
    """
    MariaDB zero-date(?? 0000-00-00 00:00:00)媛 ?덉뼱??asyncmy媛 datetime/date濡?    蹂?섑븯吏 ?딅룄濡?臾몄옄?대줈 ?쎄퀬, ?좏슚 踰붿쐞 諛?媛믪? NULL濡??뺢퇋?뷀븳??
    """
    quoted = f"`{column_name}`"
    char_expr = f"CAST({quoted} AS CHAR)"
    return (
        "CASE "
        f"WHEN {quoted} IS NULL THEN NULL "
        f"WHEN {char_expr} < '0001-01-01' THEN NULL "
        f"ELSE {char_expr} "
        f"END AS `{column_name}`"
    )


async def _resolve_maybe_awaitable(value: Any) -> Any:
    """
    ?쇰? DB/?ы띁 ?섑띁媛 ?덉쇅?곸쑝濡?寃곌낵瑜???踰???awaitable/Future 濡?媛먯떥
    諛섑솚?섎뒗 寃쎌슦媛 ?덉뼱, ?몃뜳???꾩뿉 ?ㅼ젣 媛믪쓣 爰쇰궦??
    """
    try:
        if inspect.isawaitable(value):
            return await value
    except Exception:
        pass
    return value


def _build_training_process_insert_parts(
    *,
    cols: set[str],
    row: Optional[Dict[str, Any]],
    subject: Optional[str],
    mb_id: str,
    mb_name: str,
) -> tuple[list[str], list[str], list[Any]]:
    """
    TRAINING_PROCESS INSERT??而щ읆/媛??뚮씪誘명꽣瑜?援ъ꽦?쒕떎.

    ?댁쁺 DB蹂?而щ읆 李⑥씠瑜??덉슜?섎릺, 湲곕낯媛??녿뒗 NOT NULL 而щ읆?
    紐낆떆媛믪쓣 ?ｌ뼱 asyncmy 寃쎄퀬瑜??쇳븳??
    """
    insert_cols: list[str] = []
    insert_vals: list[str] = []
    params: list[Any] = []

    contact_value = ""
    raw_file_size: Any = 0
    if isinstance(row, dict):
        try:
            contact_value = str(
                row.get("contact")
                or row.get("contact_phone")
                or ""
            ).strip()
        except Exception:
            contact_value = ""
        raw_file_size = row.get("file_size", row.get("size", 0))

    try:
        file_size_value = int(raw_file_size or 0)
    except Exception:
        try:
            file_size_value = int(float(str(raw_file_size).strip() or "0"))
        except Exception:
            file_size_value = 0
    file_size_value = max(file_size_value, 0)

    if "mb_id" in cols:
        insert_cols.append("mb_id")
        insert_vals.append("%s")
        params.append(mb_id)
    if "mb_name" in cols:
        insert_cols.append("mb_name")
        insert_vals.append("%s")
        params.append(mb_name)
    if "subject" in cols and subject is not None:
        insert_cols.append("subject")
        insert_vals.append("%s")
        params.append(subject)
    if "contact" in cols:
        insert_cols.append("contact")
        insert_vals.append("%s")
        params.append(contact_value)
    if "contact_phone" in cols:
        insert_cols.append("contact_phone")
        insert_vals.append("%s")
        params.append(contact_value)
    if "site_explain" in cols:
        insert_cols.append("site_explain")
        insert_vals.append("%s")
        params.append("")
    if "file_size" in cols:
        insert_cols.append("file_size")
        insert_vals.append("%s")
        params.append(file_size_value)
    if "process" in cols:
        insert_cols.append("process")
        insert_vals.append("%s")
        params.append("")
    if "type" in cols:
        insert_cols.append("type")
        insert_vals.append("%s")
        params.append("c")
    if "state" in cols:
        insert_cols.append("state")
        insert_vals.append("%s")
        params.append("f")
    if "created_at" in cols:
        insert_cols.append("created_at")
        insert_vals.append("NOW()")

    return insert_cols, insert_vals, params

class LearningService:
    """Learning processing service for individual files."""
    
    def __init__(self, chat_bot_id: str, db_name: str, progress_callback: Optional[Callable] = None, domain: Optional[str] = None):
        self.chat_bot_id = chat_bot_id
        self.db_name = db_name
        self.progress_callback = progress_callback
        self.domain = domain
        self.study_count = 0
        self._chat_id: Optional[str] = None
        self._table_columns_cache: dict[str, set[str]] = {}
        
        # 濡쒓렇 ?ㅼ젙
        self.log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        self.log_file = os.path.join(self.log_dir, "learning_trigger.log")
    
    def _write_log(self, msg: str):
        """濡쒓렇 ?뚯씪??湲곕줉"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {msg}\n")
        except Exception:
            pass

    @staticmethod
    def _first_http_url_for_summarize(*candidates: Optional[str]) -> Optional[str]:
        for c in candidates:
            if not isinstance(c, str):
                continue
            s = c.strip()
            if s.startswith(("http://", "https://")):
                return s
        return None

    @staticmethod
    def _resolve_summarize_dispatch_target(
        *,
        content: Optional[str],
        pg_content: Optional[str],
        content_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        inspected = {
            "content": {
                "is_http": _is_http_candidate(content),
                "preview": _http_candidate_preview(content),
            },
            "pg_content": {
                "is_http": _is_http_candidate(pg_content),
                "preview": _http_candidate_preview(pg_content),
            },
        }
        content_type_s = str(content_type or "").strip().lower()
        if content_type_s in {"file", "attach", "attachment"}:
            candidates = (("pg_content", pg_content), ("content", content))
        else:
            candidates = (("content", content), ("pg_content", pg_content))
        for source_name, raw in candidates:
            if not isinstance(raw, str):
                continue
            selected = raw.strip()
            if selected.startswith(("http://", "https://")):
                return {
                    "selected_url": selected,
                    "selected_from": source_name,
                    "reason": "selected",
                    "inspected": inspected,
                }
        return {
            "selected_url": None,
            "selected_from": None,
            "reason": "no_http_candidate",
            "inspected": inspected,
        }

    async def _post_summarize_keywords_after_status_y(
        self,
        *,
        content: Optional[str],
        pg_content: Optional[str],
        content_type: Optional[str] = None,
        learn_list_id: Optional[Any] = None,
        normalized_text: Optional[str] = None,
    ) -> None:
        """Dispatch summarize request after learning status is updated."""
        dispatch_target = self._resolve_summarize_dispatch_target(
            content=content,
            pg_content=pg_content,
            content_type=content_type,
        )
        content_u = dispatch_target.get("selected_url")
        inspected = dispatch_target.get("inspected") or {}
        if not content_u:
            logger.info(
                "[Learning][SummarizeDebug] skip_before_dispatch reason=%s content=%s pg_content=%s",
                dispatch_target.get("reason"),
                ((inspected.get("content") or {}).get("preview") or "-"),
                ((inspected.get("pg_content") or {}).get("preview") or "-"),
            )
            self._write_log(
                "[SummarizeDebug] skip_before_dispatch "
                f"reason={dispatch_target.get('reason')} "
                f"content={((inspected.get('content') or {}).get('preview') or '-')!r} "
                f"pg_content={((inspected.get('pg_content') or {}).get('preview') or '-')!r}"
            )
            return
        from backend.shared.summarize_keywords_client import (
            COOLDOWN_ACTIVE_STATUS,
            enqueue_summarize_keywords,
            post_summarize_keywords,
            summarize_keywords_endpoint,
            summarize_keywords_payload_concurrency,
            summarize_keywords_timeout_sec,
            summarize_keywords_use_queue,
        )
        endpoint = summarize_keywords_endpoint()
        logger.info(
            "[Learning][SummarizeDebug] dispatch_start source=%s endpoint=%s target=%s content=%s pg_content=%s",
            dispatch_target.get("selected_from"),
            endpoint,
            _compact_log_body(content_u, limit=180),
            ((inspected.get("content") or {}).get("preview") or "-"),
            ((inspected.get("pg_content") or {}).get("preview") or "-"),
        )
        self._write_log(
            "[SummarizeDebug] dispatch_start "
            f"source={dispatch_target.get('selected_from')} "
            f"endpoint={endpoint} "
            f"target={_compact_log_body(content_u, limit=180)!r}"
        )

        content_type_s = str(content_type or "").strip().lower()
        summarize_content_type = "file" if content_type_s in {"file", "attach", "attachment"} else "url"
        payload: Dict[str, Any] = {
            "chat_bot_id": str(self.chat_bot_id),
            "db_name": str(self.db_name),
            "target_db": str(self.db_name),
            "target": "learn_list",
            "contents": [content_u],
            "content_type": summarize_content_type,
            "concurrency": summarize_keywords_payload_concurrency(),
        }
        try:
            learn_table_for_summary = await resolve_learn_list_table_name_for_chatbot(
                self.chat_bot_id,
                self.db_name,
            )
        except Exception:
            learn_table_for_summary = ""
        if learn_table_for_summary:
            payload["learn_table"] = str(learn_table_for_summary)
            payload["target_table"] = str(learn_table_for_summary)
        try:
            if learn_list_id is not None and str(learn_list_id).strip():
                payload["learn_list_id"] = int(learn_list_id)
        except Exception:
            payload["learn_list_id"] = str(learn_list_id)
        normalized_text_s = str(normalized_text or "").strip()
        if normalized_text_s:
            payload["normalized_text"] = normalized_text_s
            payload["normalized_contents"] = [normalized_text_s]
            payload["source_url"] = content_u
        try:
            if summarize_keywords_use_queue():
                await enqueue_summarize_keywords(
                    endpoint,
                    payload,
                    timeout_sec=summarize_keywords_timeout_sec(),
                )
                logger.info(
                    "[Learning][SummarizeDebug] dispatch_queued source=%s target=%s endpoint=%s",
                    dispatch_target.get("selected_from"),
                    _compact_log_body(content_u, limit=180),
                    endpoint,
                )
                self._write_log(
                    "[SummarizeDebug] dispatch_queued "
                    f"source={dispatch_target.get('selected_from')} "
                    f"target={_compact_log_body(content_u, limit=180)!r}"
                )
                return
            status, body = await post_summarize_keywords(
                endpoint,
                payload,
                timeout_sec=summarize_keywords_timeout_sec(),
            )
            if status == COOLDOWN_ACTIVE_STATUS:
                logger.info(
                    "[Learning] summarize_keywords skipped detail=%s | source=%s target=%s endpoint=%s",
                    _compact_log_body(body),
                    dispatch_target.get("selected_from"),
                    _compact_log_body(content_u, limit=180),
                    endpoint,
                )
                self._write_log(
                    "[SummarizeDebug] dispatch_skipped "
                    f"reason=cooldown_active source={dispatch_target.get('selected_from')} "
                    f"target={_compact_log_body(content_u, limit=180)!r} "
                    f"detail={_compact_log_body(body)!r}"
                )
                return
            if status != 200:
                excerpt = _compact_log_body(body)
                if status <= 0:
                    logger.warning(
                        "[Learning] summarize_keywords request_failed detail=%s | source=%s target=%s endpoint=%s",
                        excerpt,
                        dispatch_target.get("selected_from"),
                        _compact_log_body(content_u, limit=180),
                        endpoint,
                    )
                else:
                    logger.warning(
                        "[Learning] summarize_keywords http=%s detail=%s | source=%s target=%s endpoint=%s",
                        status,
                        excerpt,
                        dispatch_target.get("selected_from"),
                        _compact_log_body(content_u, limit=180),
                        endpoint,
                    )
                self._write_log(
                    "[SummarizeDebug] dispatch_failed "
                    f"status={status} source={dispatch_target.get('selected_from')} "
                    f"target={_compact_log_body(content_u, limit=180)!r} "
                    f"detail={excerpt!r}"
                )
                return
            logger.info(
                "[Learning][SummarizeDebug] dispatch_success source=%s target=%s endpoint=%s",
                dispatch_target.get("selected_from"),
                _compact_log_body(content_u, limit=180),
                endpoint,
            )
            self._write_log(
                "[SummarizeDebug] dispatch_success "
                f"source={dispatch_target.get('selected_from')} "
                f"target={_compact_log_body(content_u, limit=180)!r}"
            )
        except Exception as exc:
            logger.warning(
                "[Learning] summarize_keywords error: %s | source=%s target=%s endpoint=%s",
                exc,
                dispatch_target.get("selected_from"),
                _compact_log_body(content_u, limit=180),
                endpoint,
            )
            self._write_log(
                "[SummarizeDebug] dispatch_error "
                f"source={dispatch_target.get('selected_from')} "
                f"target={_compact_log_body(content_u, limit=180)!r} "
                f"err={_compact_log_body(str(exc))!r}"
            )

    @staticmethod
    def _breadcrumb_crawl_enabled() -> bool:
        raw = str(os.getenv("BREADCRUMB_CRAWL_ENABLED", "1") or "1").strip().lower()
        return raw in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _breadcrumb_crawl_endpoint() -> str:
        return (
            os.getenv("BREADCRUMB_CRAWL_URL")
            or "https://api-aipro.chatbaram.com/ai-video-dev/AI_video/breadcrumb-crawl/single-url"
        ).strip()

    @staticmethod
    def _breadcrumb_db_bridge_endpoint() -> str:
        return (
            os.getenv("BREADCRUMB_DB_BRIDGE_URL")
            or "https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler/debug/breadcrumb_option_bridge"
        ).strip()

    @staticmethod
    def _breadcrumb_crawl_timeout_sec() -> float:
        try:
            value = float((os.getenv("BREADCRUMB_CRAWL_TIMEOUT_SEC") or "30").strip() or "30")
        except ValueError:
            value = 30.0
        return max(3.0, min(value, 180.0))

    @staticmethod
    def _breadcrumb_crawl_fetch_timeout_sec() -> float:
        try:
            value = float((os.getenv("BREADCRUMB_CRAWL_FETCH_TIMEOUT_SEC") or "15").strip() or "15")
        except ValueError:
            value = 15.0
        return max(3.0, min(value, 60.0))

    @staticmethod
    def _breadcrumb_fragment_max_chars() -> int:
        try:
            value = int((os.getenv("BREADCRUMB_CRAWL_FRAGMENT_MAX_CHARS") or "20000").strip() or "20000")
        except ValueError:
            value = 20000
        return max(500, min(value, 200000))

    @staticmethod
    def _breadcrumb_crawl_require_fragment() -> bool:
        raw = str(os.getenv("BREADCRUMB_CRAWL_REQUIRE_FRAGMENT", "0") or "0").strip().lower()
        return raw in {"1", "true", "yes", "y", "on"}

    async def _fetch_breadcrumb_bridge_context(self, learn_list_id: int, *, contents_url: str = "") -> Dict[str, str]:
        endpoint = self._breadcrumb_db_bridge_endpoint()
        if not endpoint:
            return {}

        try:
            payload = {
                "db_name": str(self.db_name),
                "learn_list_id": int(learn_list_id),
                "contents_url": str(contents_url or ""),
                "limit": 1,
            }
            if endpoint.startswith(("http://", "https://")):
                import aiohttp

                timeout = aiohttp.ClientTimeout(total=self._breadcrumb_crawl_fetch_timeout_sec())
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(endpoint, json=payload) as resp:
                        text = await resp.text()
                        if resp.status >= 400:
                            raise RuntimeError(f"bridge_http_status={resp.status} body={_compact_log_body(text)}")
                        data = json.loads(text) if text else {}
            else:
                from backend.shared.breadcrumb_option_bridge import fetch_breadcrumb_option_bridge_payload

                data = await fetch_breadcrumb_option_bridge_payload(**payload)
        except Exception as exc:
            logger.info(
                "[Learning][BreadcrumbCrawl] bridge_lookup_failed endpoint=%s learn_list_id=%s db=%s contents_url=%s detail=%s",
                endpoint,
                learn_list_id,
                self.db_name,
                _compact_log_body(contents_url, limit=180),
                _compact_log_body(str(exc)),
            )
            self._write_log(
                "[BreadcrumbCrawl] bridge_lookup_failed "
                f"learn_list_id={learn_list_id} detail={_compact_log_body(str(exc))!r}"
            )
            return {}

        if not isinstance(data, dict) or not data.get("ok", False):
            logger.info(
                "[Learning][BreadcrumbCrawl] bridge_response_not_ok learn_list_id=%s body=%s",
                learn_list_id,
                _compact_log_body(json.dumps(data, ensure_ascii=False, default=str)),
            )
            return {}

        selector = ""
        url = ""
        for row in data.get("exploration_joined_samples") or []:
            if not isinstance(row, dict):
                continue
            selector = selector or str(row.get("breadcrumb_selector") or "").strip()
            url = (
                url
                or str(row.get("exploration_url") or "").strip()
                or str(row.get("learn_content") or "").strip()
            )
            if selector and url:
                break
        if not selector or not url:
            for row in data.get("joined_samples") or []:
                if not isinstance(row, dict):
                    continue
                selector = selector or str(row.get("breadcrumb_selector") or "").strip()
                url = url or str(row.get("content") or "").strip()
                if selector and url:
                    break
        if not url:
            for row in data.get("exploration_exact_rows_for_learn_list_id") or []:
                if isinstance(row, dict):
                    url = str(row.get("url") or "").strip()
                    if url:
                        break
        if not selector:
            for row in data.get("samples") or []:
                if isinstance(row, dict):
                    selector = str(row.get("breadcrumb_selector") or "").strip()
                    if selector:
                        break

        logger.info(
            "[Learning][BreadcrumbCrawl] bridge_lookup_ok learn_list_id=%s contents_url=%s selector=%s url=%s",
            learn_list_id,
            _compact_log_body(contents_url, limit=180),
            _compact_log_body(selector, limit=180),
            _compact_log_body(url, limit=180),
        )
        return {"selector": selector, "url": url}

    @staticmethod
    def _extract_breadcrumb_html_fragment(html: str, selector: str) -> str:
        if not html or not selector:
            return ""
        try:
            from bs4 import BeautifulSoup  # type: ignore[import-not-found]
        except Exception:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return ""

        selectors = [selector]
        split_selectors = [
            part.strip()
            for part in str(selector).replace("\r", "\n").split("\n")
            if part and part.strip()
        ]
        if len(split_selectors) > 1:
            selectors.extend(split_selectors)

        for sel in selectors:
            try:
                node = soup.select_one(sel)
            except Exception:
                node = None
            if node is not None:
                fragment = str(node).strip()
                if fragment:
                    return fragment
        return ""

    async def _fetch_breadcrumb_html_fragment(self, *, url: str, selector: str) -> str:
        def _fetch_sync() -> str:
            try:
                from utils.http_client import get_requests_session

                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    )
                }
                with get_requests_session() as session:
                    resp = session.get(
                        url,
                        timeout=self._breadcrumb_crawl_fetch_timeout_sec(),
                        allow_redirects=True,
                        headers=headers,
                    )
                    if int(getattr(resp, "status_code", 0) or 0) >= 400:
                        return ""
                    return resp.text or ""
            except Exception:
                return ""

        html = await asyncio.to_thread(_fetch_sync)
        fragment = self._extract_breadcrumb_html_fragment(html, selector)
        max_chars = self._breadcrumb_fragment_max_chars()
        if len(fragment) > max_chars:
            fragment = fragment[:max_chars]
        return fragment

    async def _post_breadcrumb_crawl_after_status_y(
        self,
        *,
        content: Optional[str],
        pg_content: Optional[str],
        learn_list_id: Optional[Any] = None,
    ) -> None:
        logger.info(
            "[Learning][BreadcrumbCrawl] post_start learn_list_id=%s content=%s pg_content=%s",
            learn_list_id,
            _http_candidate_preview(content),
            _http_candidate_preview(pg_content),
        )
        if not self._breadcrumb_crawl_enabled():
            logger.info("[Learning][BreadcrumbCrawl] skip reason=disabled learn_list_id=%s", learn_list_id)
            return
        endpoint = self._breadcrumb_crawl_endpoint()
        if not endpoint:
            logger.info("[Learning][BreadcrumbCrawl] skip reason=no_endpoint learn_list_id=%s", learn_list_id)
            return
        try:
            learn_id = int(learn_list_id or 0)
        except Exception:
            learn_id = 0
        if learn_id <= 0:
            logger.info("[Learning][BreadcrumbCrawl] skip reason=missing_learn_list_id")
            return

        dispatch_target = self._resolve_summarize_dispatch_target(
            content=content,
            pg_content=pg_content,
        )
        target_url = str(dispatch_target.get("selected_url") or "").strip()

        contents_url = str(getattr(self, "breadcrumb_contents_url", "") or "").strip()
        bridge_lookup_url = contents_url or target_url
        logger.info(
            "[Learning][BreadcrumbCrawl] bridge_lookup_start learn_list_id=%s target_url=%s contents_url=%s reason=%s",
            learn_id,
            _compact_log_body(target_url, limit=180),
            _compact_log_body(contents_url, limit=180),
            dispatch_target.get("reason"),
        )
        bridge_ctx = await self._fetch_breadcrumb_bridge_context(learn_id, contents_url=bridge_lookup_url)
        selector = str((bridge_ctx or {}).get("selector") or "").strip()
        bridged_url = str((bridge_ctx or {}).get("url") or "").strip()
        logger.info(
            "[Learning][BreadcrumbCrawl] bridge_ctx_resolved learn_list_id=%s has_selector=%s has_bridged_url=%s target_url=%s",
            learn_id,
            bool(selector),
            bool(bridged_url),
            _compact_log_body(target_url, limit=180),
        )
        if bridged_url.startswith(("http://", "https://")):
            target_url = bridged_url
        if not target_url:
            logger.info(
                "[Learning][BreadcrumbCrawl] skip reason=no_url learn_list_id=%s detail=%s",
                learn_id,
                dispatch_target.get("reason"),
            )
            return
        if not selector:
            logger.info(
                "[Learning][BreadcrumbCrawl] skip reason=no_selector learn_list_id=%s url=%s",
                learn_id,
                _compact_log_body(target_url, limit=180),
            )
            return

        fragment = await self._fetch_breadcrumb_html_fragment(url=target_url, selector=selector)
        if not fragment:
            if self._breadcrumb_crawl_require_fragment():
                logger.info(
                    "[Learning][BreadcrumbCrawl] skip reason=no_fragment learn_list_id=%s selector=%s url=%s",
                    learn_id,
                    _compact_log_body(selector, limit=180),
                    _compact_log_body(target_url, limit=180),
                )
                return
            logger.info(
                "[Learning][BreadcrumbCrawl] fragment_missing_dispatch_anyway learn_list_id=%s selector=%s url=%s",
                learn_id,
                _compact_log_body(selector, limit=180),
                _compact_log_body(target_url, limit=180),
            )

        payload: Dict[str, Any] = {
            "chat_bot_id": str(self.chat_bot_id),
            "learn_list_id": learn_id,
            "db_name": str(self.db_name),
            "url": target_url,
            "breadcrumb_selector": selector,
            "selector": selector,
            "html_fragment": fragment,
        }
        if contents_url:
            payload["contents_url"] = contents_url
            payload["contents"] = [contents_url]

        def _post_sync() -> tuple[int, str]:
            try:
                from utils.http_client import get_requests_session

                with get_requests_session() as session:
                    resp = session.post(
                        endpoint,
                        json=payload,
                        timeout=self._breadcrumb_crawl_timeout_sec(),
                        headers={"Content-Type": "application/json; charset=utf-8"},
                    )
                    return int(resp.status_code or 0), resp.text or ""
            except Exception as exc:
                return 0, str(exc)

        logger.info(
            "[Learning][BreadcrumbCrawl] dispatch_start endpoint=%s learn_list_id=%s selector=%s url=%s fragment_len=%s",
            endpoint,
            learn_id,
            _compact_log_body(selector, limit=180),
            _compact_log_body(target_url, limit=180),
            len(fragment),
        )
        status, body = await asyncio.to_thread(_post_sync)
        if status != 200:
            logger.warning(
                "[Learning][BreadcrumbCrawl] dispatch_failed status=%s learn_list_id=%s detail=%s url=%s",
                status,
                learn_id,
                _compact_log_body(body),
                _compact_log_body(target_url, limit=180),
            )
            self._write_log(
                f"[BreadcrumbCrawl] dispatch_failed status={status} learn_list_id={learn_id} "
                f"detail={_compact_log_body(body)!r} url={_compact_log_body(target_url, limit=180)!r}"
            )
            return
        logger.info(
            "[Learning][BreadcrumbCrawl] dispatch_success learn_list_id=%s url=%s",
            learn_id,
            _compact_log_body(target_url, limit=180),
        )
        self._write_log(
            f"[BreadcrumbCrawl] dispatch_success learn_list_id={learn_id} "
            f"url={_compact_log_body(target_url, limit=180)!r}"
        )

    def _schedule_breadcrumb_crawl_fire_and_forget(
        self,
        *,
        content: Optional[str],
        pg_content: Optional[str],
        learn_list_id: Optional[Any] = None,
    ) -> None:
        async def _runner() -> None:
            logger.info(
                "[Learning][BreadcrumbCrawl] runner_start learn_list_id=%s",
                learn_list_id,
            )
            try:
                await self._post_breadcrumb_crawl_after_status_y(
                    content=content,
                    pg_content=pg_content,
                    learn_list_id=learn_list_id,
                )
                logger.info(
                    "[Learning][BreadcrumbCrawl] runner_done learn_list_id=%s",
                    learn_list_id,
                )
            except Exception as exc:
                logger.warning("[Learning][BreadcrumbCrawl] fire_and_forget_failed: %s", exc)
                self._write_log(f"[BreadcrumbCrawl] fire_and_forget_failed err={_compact_log_body(str(exc))!r}")

        try:
            asyncio.get_running_loop().create_task(_runner())
            logger.info(
                "[Learning][BreadcrumbCrawl] runner_scheduled learn_list_id=%s",
                learn_list_id,
            )
        except RuntimeError:
            logger.info("[Learning][BreadcrumbCrawl] skip reason=no_running_loop")

    async def _schedule_summarize_keywords_fire_and_forget(
        self,
        *,
        content: Optional[str],
        pg_content: Optional[str],
        content_type: Optional[str] = None,
        learn_list_id: Optional[Any] = None,
        normalized_text: Optional[str] = None,
    ) -> None:
        try:
            asyncio.get_running_loop().create_task(
                self._post_summarize_keywords_after_status_y(
                    content=content,
                    pg_content=pg_content,
                    content_type=content_type,
                    learn_list_id=learn_list_id,
                    normalized_text=normalized_text,
                )
            )
        except RuntimeError:
            await self._post_summarize_keywords_after_status_y(
                content=content,
                pg_content=pg_content,
                content_type=content_type,
                learn_list_id=learn_list_id,
                normalized_text=normalized_text,
            )

    async def _await_summarize_keywords_after_learn_steps(
        self,
        *,
        content: Optional[str],
        pg_content: Optional[str],
        content_type: Optional[str] = None,
        learn_list_id: Optional[Any] = None,
        normalized_text: Optional[str] = None,
    ) -> None:
        """
        trigger_learning ?꾨떒?먯꽌 ?몄텧.
        - 湲곕낯: ?붿빟 API源뚯? await.
        - SUMMARIZE_KEYWORDS_FIRE_AND_FORGET=1: create_task留??섍퀬 利됱떆 諛섑솚(?뚯빱??癒쇱? 吏꾪뻾).
        - 湲곕낯媛믪쓣 await 履쎌쑝濡??먯뼱, ?숈뒿 吏곹썑 ?먮룞 ?몄텧 ?먯껜媛 ?꾨씫?섎뒗 ?덉씠?ㅻ? 以꾩씤??
        """
        ff = (os.getenv("SUMMARIZE_KEYWORDS_FIRE_AND_FORGET", "1") or "1").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        dispatch_target = self._resolve_summarize_dispatch_target(
            content=content,
            pg_content=pg_content,
            content_type=content_type,
        )
        inspected = dispatch_target.get("inspected") or {}
        logger.info(
            "[Learning][SummarizeDebug] schedule mode=%s selected_from=%s reason=%s content=%s pg_content=%s",
            "fire_and_forget" if ff else "await",
            dispatch_target.get("selected_from") or "-",
            dispatch_target.get("reason"),
            ((inspected.get("content") or {}).get("preview") or "-"),
            ((inspected.get("pg_content") or {}).get("preview") or "-"),
        )
        self._write_log(
            "[SummarizeDebug] schedule "
            f"mode={'fire_and_forget' if ff else 'await'} "
            f"selected_from={dispatch_target.get('selected_from') or '-'} "
            f"reason={dispatch_target.get('reason')}"
        )
        self._schedule_breadcrumb_crawl_fire_and_forget(
            content=content,
            pg_content=pg_content,
            learn_list_id=learn_list_id,
        )
        if ff:
            await self._schedule_summarize_keywords_fire_and_forget(
                content=content,
                pg_content=pg_content,
                content_type=content_type,
                learn_list_id=learn_list_id,
                normalized_text=normalized_text,
            )
            return
        await self._post_summarize_keywords_after_status_y(
            content=content,
            pg_content=pg_content,
            content_type=content_type,
            learn_list_id=learn_list_id,
            normalized_text=normalized_text,
        )

    async def _ensure_chat_id(self) -> Optional[str]:
        """chat_id 罹먯떆"""
        if self._chat_id:
            return self._chat_id
        try:
            self._chat_id = await get_chat_id_from_db(self.db_name, self.chat_bot_id)
        except Exception as exc:
            logger.warning("[Learning] chat_id 議고쉶 ?ㅽ뙣: %s", exc)
            self._chat_id = None
        return self._chat_id

    async def _get_pg_chunk_count(self, content_value: Optional[str]) -> int:
        """PostgreSQL?먯꽌 ?대떦 content??泥?겕 ?섎? 議고쉶"""
        if not content_value:
            return 0
        chat_id = await self._ensure_chat_id()
        if not chat_id:
            return 0
        # normalize table name to lowercase to match Postgres identifier folding
        table_name = f"td_{chat_id}_training_data".lower()
        sql = f"SELECT COUNT(*) FROM {table_name} WHERE content = $1"
        try:
            rows = await pg_execute_query(sql, params=(content_value,), fetch=True, dbname=self.db_name)
            rows = await _resolve_maybe_awaitable(rows)
            if rows:
                first = rows[0]
                first = await _resolve_maybe_awaitable(first)
                try:
                    return int(first[0])
                except (TypeError, ValueError):
                    return 0
        except Exception as exc:
            logger.warning("[Learning] PG chunk count 議고쉶 ?ㅽ뙣: %s", exc)
        return 0

    async def _get_pg_chunk_count_by_subject(self, subject_value: Optional[str]) -> int:
        if not subject_value:
            return 0
        chat_id = await self._ensure_chat_id()
        if not chat_id:
            return 0
        table_name = f"td_{chat_id}_training_data".lower()
        sql = f"SELECT COUNT(*) FROM {table_name} WHERE subject = $1"
        try:
            rows = await pg_execute_query(sql, params=(subject_value,), fetch=True, dbname=self.db_name)
            rows = await _resolve_maybe_awaitable(rows)
            if rows:
                first = rows[0]
                first = await _resolve_maybe_awaitable(first)
                try:
                    return int(first[0])
                except (TypeError, ValueError):
                    return 0
        except Exception as exc:
            logger.warning("[Learning] PG subject chunk count 議고쉶 ?ㅽ뙣: %s", exc)
        return 0

    @staticmethod
    def _normalize_pg_lookup_candidate(value: Optional[str]) -> Optional[str]:
        if not isinstance(value, str):
            return None
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.startswith(("http://", "https://")):
            try:
                from utils.url import canonicalize_url_for_dedup

                candidate = canonicalize_url_for_dedup(candidate) or candidate
            except Exception:
                pass
        return candidate

    @classmethod
    def _build_pg_lookup_candidates(
        cls,
        *,
        filename: Optional[str],
        subject_value: Optional[str],
        pg_content_value: Optional[str],
        content_value: Optional[str],
    ) -> list[str]:
        candidates: list[str] = []
        seen: set[str] = set()

        def _push(raw: Optional[str]) -> None:
            normalized = cls._normalize_pg_lookup_candidate(raw)
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            candidates.append(normalized)

            try:
                basename = os.path.basename(normalized.split("?", 1)[0].rstrip("/\\"))
            except Exception:
                basename = ""
            basename = basename.strip()
            basename_lower = basename.lower()
            if basename and basename_lower not in _GENERIC_DYNAMIC_BASENAMES and basename not in seen:
                seen.add(basename)
                candidates.append(basename)

        _push(pg_content_value)
        _push(content_value)
        _push(subject_value)
        _push(filename)
        return candidates

    async def _get_pg_chunk_count_for_candidates(
        self, candidates: Iterable[str]
    ) -> tuple[int, Optional[str]]:
        best_count = 0
        best_candidate: Optional[str] = None
        for candidate in candidates:
            chunk_count = await self._get_pg_chunk_count(candidate)
            looks_urlish = isinstance(candidate, str) and (
                candidate.startswith(("http://", "https://"))
                or "/" in candidate
                or "?" in candidate
                or "=" in candidate
            )
            if not looks_urlish and chunk_count <= 0:
                subject_chunk_count = await self._get_pg_chunk_count_by_subject(candidate)
                if subject_chunk_count > chunk_count:
                    chunk_count = subject_chunk_count
            if chunk_count > best_count:
                best_count = chunk_count
                best_candidate = candidate
        return best_count, best_candidate

    @staticmethod
    def _derive_pg_wait_timeout_seconds(
        *,
        requested_timeout_seconds: Optional[float],
        expected_min: int,
        primary_candidate: Optional[str],
    ) -> float:
        try:
            timeout_sec = float(requested_timeout_seconds or _DEFAULT_PG_WAIT_SEC)
        except Exception:
            timeout_sec = _DEFAULT_PG_WAIT_SEC
        timeout_sec = max(10.0, min(timeout_sec, 1800.0))

        candidate = str(primary_candidate or "").strip().lower()
        looks_urlish = candidate.startswith(("http://", "https://")) or ".do?" in candidate
        if looks_urlish and expected_min <= 3:
            timeout_sec = min(timeout_sec, _DEFAULT_PG_WAIT_URL_SEC)
        return timeout_sec

    async def _get_table_columns(self, table_name: str) -> set[str]:
        """Return cached table columns from information_schema."""
        if not table_name:
            return set()
        if table_name in self._table_columns_cache:
            return self._table_columns_cache[table_name]

        cols: set[str] = set()
        try:
            sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
            """
            rows = await maria_execute_query(sql, (self.db_name, table_name), fetch=True, dbname=self.db_name)
            rows = await _resolve_maybe_awaitable(rows)
            for r in rows or []:
                col = r.get("column_name") if isinstance(r, dict) else None
                if col:
                    cols.add(str(col))
        except Exception as exc:
            logger.debug("[Learning] Failed to introspect columns for %s: %s", table_name, exc)
            cols = set()

        self._table_columns_cache[table_name] = cols
        return cols

    async def _table_exists(self, table_name: str) -> bool:
        """MariaDB ?뚯씠釉?議댁옱 ?щ? ?뺤씤."""
        if not table_name:
            return False
        try:
            sql = """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
                LIMIT 1
            """
            rows = await maria_execute_query(sql, (self.db_name, table_name), fetch=True, dbname=self.db_name)
            rows = await _resolve_maybe_awaitable(rows)
            return bool(rows)
        except Exception:
            return False

    async def _resolve_table_name_case(self, table_name: str) -> Optional[str]:
        """?뚯씠釉붾챸 ??뚮Ц??遺덉씪移????ㅼ젣 ?대쫫??議고쉶??蹂댁젙?쒕떎."""
        if not table_name:
            return None
        try:
            sql = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND LOWER(table_name) = LOWER(%s)
                LIMIT 1
            """
            rows = await maria_execute_query(sql, (self.db_name, table_name), fetch=True, dbname=self.db_name)
            rows = await _resolve_maybe_awaitable(rows)
            if rows:
                row = rows[0]
                row = await _resolve_maybe_awaitable(row)
                actual = row.get("table_name") if isinstance(row, dict) else None
                if actual:
                    return str(actual)
        except Exception:
            return None
        return None
    
    async def trigger_learning(
        self,
        db_id: str,
        filename: str,
        stats: Optional[dict] = None,
        actual_chunks: int = 0,
        *,
        pg_content_value: Optional[str] = None,
        pg_wait_timeout_seconds: Optional[float] = None,
        pg_wait_interval_seconds: float = 2.0,
        post_reg_date: Optional[Any] = None,
        preserve_created_at: bool = False,
        summarize_after_status_y: bool = True,
        summarize_normalized_text: Optional[str] = None,
    ) -> bool:
        """
        媛쒕퀎 ?뚯씪 ?숈뒿 ?꾨즺 泥섎━ 諛?MariaDB ?곹깭 ?낅뜲?댄듃

        post_reg_date: ?щ· ?④퀎?먯꽌 異붿텧???깅줉???묒꽦??臾몄옄????. LEARN_LIST content_created_at 諛섏쁺???ъ슜.
        """
        if pg_wait_timeout_seconds is None:
            pg_wait_timeout_seconds = _DEFAULT_PG_WAIT_SEC
        start_msg = f"?렞 媛쒕퀎 ?숈뒿 ?꾨즺 泥섎━ ?쒖옉 | ID={db_id} | ?뚯씪={filename} | Chunks={actual_chunks}"
        logger.debug(f"[Learning] {start_msg}")
        self._write_log(start_msg)
        self.last_missing_learn_list_record = False

        try:
            # 異붽? ?붾쾭源?濡쒓렇: ?몄텧 ?쒖젏???낅젰媛?湲곕줉
            try:
                dbg_call = {
                    "db_id": str(db_id),
                    "filename": str(filename),
                    "actual_chunks": int(actual_chunks or 0),
                    "pg_content_value": pg_content_value,
                }
                logger.debug(f"[Learning][DBG] trigger_learning call: {dbg_call}")
                self._write_log(f"[DBG] trigger_learning call: {dbg_call}")
            except Exception:
                pass
            # ????④퀎? ?숈씪??resolver濡?LEARN_LIST ?뚯씠釉붿쓣 李얜뒗??
            learn_list_table = await resolve_learn_list_table_name_for_chatbot(
                self.chat_bot_id,
                self.db_name,
            )
            if not learn_list_table:
                raise RuntimeError(
                    f"[Learning] learn_list table resolve failed (db={self.db_name}, chat_bot_id={self.chat_bot_id})"
                )
            training_process_table = (
                learn_list_table.replace("_LEARN_LIST", "_TRAINING_PROCESS")
                if learn_list_table.endswith("_LEARN_LIST")
                else ""
            )
            if not await self._table_exists(learn_list_table):
                resolved_name = await self._resolve_table_name_case(learn_list_table)
                if resolved_name:
                    learn_list_table = resolved_name
                else:
                    raise RuntimeError(
                        f"[Learning] table missing: {self.db_name}.{learn_list_table} (chat_bot_id={self.chat_bot_id})"
                    )
            if training_process_table and not await self._table_exists(training_process_table):
                resolved_tp = await self._resolve_table_name_case(training_process_table)
                training_process_table = resolved_tp
            logger.debug(
                "[LearningTrace][table_resolved] db=%s chat_bot_id=%s learn_list_table=%s training_process_table=%s db_id=%s",
                self.db_name,
                self.chat_bot_id,
                learn_list_table,
                training_process_table or "-",
                db_id,
            )
            
            # 1. LEARN_LIST ?뚯씠釉붿쓽 status瑜?'N'?쇰줈 珥덇린??(?묒뾽 ?쒖옉 ?쒖떆)
            try:
                cols_ll = await self._get_table_columns(learn_list_table)
            except Exception:
                cols_ll = set()

            init_sets = ["status = 'N'"]
            if not preserve_created_at:
                init_sets.append("created_at = NOW()")
            init_params = []
            try:
                pre_rows = await maria_execute_query(
                    f"SELECT status FROM `{learn_list_table}` WHERE id = %s",
                    (db_id,),
                    fetch=True,
                    dbname=self.db_name,
                )
                pre_rows = await _resolve_maybe_awaitable(pre_rows)
                pre_status = pre_rows[0].get("status") if pre_rows else None
            except Exception as exc:
                pre_status = f"__err__:{str(exc)[:120]}"
            # ?대? status='Y'硫??섎룎由ъ? ?딅뒗???꾨즺媛?蹂댄샇)
            if str(pre_status).upper() != "Y":
                init_sql = f"UPDATE `{learn_list_table}` SET " + ", ".join(init_sets) + " WHERE id = %s"
                init_params.append(db_id)
                from backend.shared.db_write_queue import run_db_write

                await run_db_write(
                    "learning.learn_list_status_init",
                    lambda: maria_execute_query(init_sql, tuple(init_params), fetch=False, dbname=self.db_name, op_name="learn_status_update:init"),
                )
            
            # 2. ?낅뜲?댄듃???덉퐫??議고쉶
            try:
                select_cols = ["`id`", "`subject`", "`size`", "`content`"]
                if cols_ll and "content_type" in cols_ll:
                    select_cols.append("`content_type`")
                if cols_ll and "content_created_at" in cols_ll:
                    select_cols.append(_safe_content_created_at_select_expr("content_created_at"))
                # ?숈뒿 ?꾨즺 ???됰퀎 cate ?좎?(?⑦꽩 留ㅼ묶 ?? ???놁쑝硫?CATEGORY url/query 洹쒖튃?쇰줈 蹂닿컯
                if cols_ll and "cate1" in cols_ll:
                    select_cols.append("`cate1`")
                if cols_ll and "cate2" in cols_ll:
                    select_cols.append("`cate2`")
                select_sql = f"SELECT {', '.join(select_cols)} FROM `{learn_list_table}` WHERE id = %s"
            except Exception:
                select_sql = f"SELECT id, subject, size, content FROM `{learn_list_table}` WHERE id = %s"
            result = await maria_execute_query(select_sql, (db_id,), fetch=True, dbname=self.db_name)
            result = await _resolve_maybe_awaitable(result)
            
            if not result:
                self.last_missing_learn_list_record = True
                logger.warning(
                    "[Learning] learn_list row missing; skip finalize | db=%s chat_bot_id=%s table=%s id=%s file=%s",
                    self.db_name,
                    self.chat_bot_id,
                    learn_list_table,
                    db_id,
                    filename,
                )
                self._write_log(
                    f"[WARN] learn_list row missing; skip finalize | db={self.db_name} table={learn_list_table} id={db_id} file={filename}"
                )
                return False
            
            row = result[0]
            row = await _resolve_maybe_awaitable(row)
            logger.info(
                "[Learning][cate-update-debug] selected learn_list row | id=%s file=%s content_type=%s row_cate=(%r,%r) content=%s",
                db_id,
                filename,
                row.get("content_type") if isinstance(row, dict) else None,
                row.get("cate1") if isinstance(row, dict) else None,
                row.get("cate2") if isinstance(row, dict) else None,
                (str(row.get("content") or "")[:180] if isinstance(row, dict) else ""),
            )
            # 議고쉶??MariaDB row ?붾쾭洹?濡쒓퉭 (誘쇨컧?뺣낫 ?쒖쇅)
            try:
                dbg_row = {
                    "id": row.get("id"),
                    "subject": row.get("subject"),
                    "size": row.get("size"),
                    "content_preview": (row.get("content") or "")[:300] if isinstance(row.get("content"), str) else None,
                }
                logger.debug(f"[Learning][DBG] mariadb row: {dbg_row}")
                self._write_log(f"[DBG] mariadb row: {dbg_row}")
            except Exception:
                pass
            # subject??LEARN_LIST????λ맂 媛믪쓣 洹몃?濡??ъ슜?쒕떎 (fallback 留ㅽ븨/?泥?而щ읆 ?ъ슜 湲덉?)
            subject = row.get('subject')
            content_value = row.get('content')
            # 3. ?숈뒿 ?꾨즺 ?먯젙: "泥?겕媛?議댁옱 = ?숈뒿 ?꾨즺"
            # - PG?먮뒗 content(濡쒖뺄 ?뚯씪 寃쎈줈)媛 ??λ릺怨? MariaDB LEARN_LIST.content??URL?????덈떎.
            #   ?곕씪??PG 議고쉶??content 媛?pg_content_value)??蹂꾨룄濡?諛쏆쓣 ???덇쾶 ?쒕떎.
            expected_min = int(actual_chunks or 0)
            if expected_min <= 0:
                expected_min = 1  # 理쒖냼 1媛?泥?겕媛 ?앷꺼???꾨즺濡?媛꾩＜

            pg_lookup_candidates = self._build_pg_lookup_candidates(
                filename=filename,
                subject_value=subject,
                pg_content_value=pg_content_value,
                content_value=content_value,
            )
            pg_lookup_value = pg_lookup_candidates[0] if pg_lookup_candidates else None
            effective_pg_wait_timeout_seconds = self._derive_pg_wait_timeout_seconds(
                requested_timeout_seconds=pg_wait_timeout_seconds,
                expected_min=expected_min,
                primary_candidate=pg_lookup_value,
            )
            if pg_content_value and pg_content_value != content_value:
                logger.debug(
                    "[Learning] Using pg lookup candidates | id=%s maria.content=%s pg.content=%s candidates=%s",
                    db_id,
                    content_value,
                    pg_content_value,
                    pg_lookup_candidates,
                )
            # PG ?湲????곹깭 濡쒓퉭
            try:
                logger.debug(
                    "[Learning][DBG] pg_lookup_candidates=%s expected_min=%s pg_wait_timeout_seconds=%s pg_wait_interval_seconds=%s",
                    pg_lookup_candidates,
                    expected_min,
                    effective_pg_wait_timeout_seconds,
                    pg_wait_interval_seconds,
                )
                self._write_log(
                    f"[DBG] pg_lookup_candidates={pg_lookup_candidates} expected_min={expected_min} pg_wait_timeout_seconds={effective_pg_wait_timeout_seconds}"
                )
            except Exception:
                pass

            pg_chunk_count = 0
            matched_pg_lookup_value = None
            waited_seconds = 0.0
            if pg_lookup_candidates:
                try:
                    deadline = asyncio.get_event_loop().time() + float(effective_pg_wait_timeout_seconds or 0)
                except Exception:
                    deadline = None
                wait_started = time.perf_counter()
                while True:
                    pg_chunk_count, matched_pg_lookup_value = await self._get_pg_chunk_count_for_candidates(
                        pg_lookup_candidates
                    )
                    if pg_chunk_count >= expected_min:
                        break
                    if deadline is not None and asyncio.get_event_loop().time() >= deadline:
                        break
                    await asyncio.sleep(float(pg_wait_interval_seconds or 1.0))
                waited_seconds = round(time.perf_counter() - wait_started, 1)
                # PG 議고쉶 猷⑦봽 醫낅즺 ???곹깭 濡쒓퉭
                try:
                    logger.debug(
                        "[Learning][DBG] pg wait finished | id=%s pg_chunk_count=%s expected_min=%s matched_candidate=%s waited=%ss",
                        db_id,
                        pg_chunk_count,
                        expected_min,
                        matched_pg_lookup_value,
                        waited_seconds,
                    )
                    self._write_log(
                        f"[DBG] pg_chunk_count={pg_chunk_count} expected_min={expected_min} matched_candidate={matched_pg_lookup_value} waited={waited_seconds}s"
                    )
                except Exception:
                    pass
            if pg_chunk_count < expected_min:
                logger.warning(
                    "[Learning] chunk not ready; skip status=Y update | id=%s file=%s maria.content=%s pg.content=%s waited=%ss pg_chunk_count=%s expected_min=%s matched=%s pg.candidates=%s",
                    db_id,
                    filename,
                    content_value,
                    pg_content_value,
                    waited_seconds or effective_pg_wait_timeout_seconds,
                    pg_chunk_count,
                    expected_min,
                    matched_pg_lookup_value,
                    pg_lookup_candidates,
                )
                logger.warning(
                    "[Learning][cate-update-debug] status update not called: chunk not ready | id=%s file=%s row_cate=(%s,%s) pg_chunk_count=%s expected_min=%s",
                    db_id,
                    filename,
                    (row or {}).get("cate1") if isinstance(row, dict) else None,
                    (row or {}).get("cate2") if isinstance(row, dict) else None,
                    pg_chunk_count,
                    expected_min,
                )
                try:
                    self._write_log(
                        f"[WARN] chunk not ready | id={db_id} file={filename} pg_chunk_count={pg_chunk_count} expected_min={expected_min} matched={matched_pg_lookup_value} pg_lookup_candidates={pg_lookup_candidates}"
                    )
                except Exception:
                    pass
                return False

            final_chunks = pg_chunk_count

            # 4. TRAINING_PROCESS ?뚯씠釉붿뿉 ?숈뒿 ?대젰 湲곕줉 (?숈뒿 ?꾨즺濡??먯젙???ㅼ뿉留?
            # - ?붿껌?ы빆: fallback(?泥?而щ읆 留ㅽ븨) ?놁씠, "議댁옱?섎뒗 而щ읆留? INSERT
            mb_id, mb_name = "crawler", "?먮룞?섏쭛"
            try:
                cols = await self._get_table_columns(training_process_table)
                insert_cols, insert_vals, params = _build_training_process_insert_parts(
                    cols=cols,
                    row=row,
                    subject=subject,
                    mb_id=mb_id,
                    mb_name=mb_name,
                )

                if insert_cols:
                    quoted_insert_cols = ", ".join(f"`{col}`" for col in insert_cols)
                    process_sql = (
                        f"INSERT INTO `{training_process_table}` ({quoted_insert_cols}) "
                        f"VALUES ({', '.join(insert_vals)})"
                    )
                    from backend.shared.db_write_queue import run_db_write

                    await run_db_write(
                        "learning.training_process_insert",
                        lambda: maria_execute_query(process_sql, tuple(params), fetch=False, dbname=self.db_name),
                    )
                else:
                    logger.debug("[Learning] TRAINING_PROCESS insert skipped (no compatible columns) | table=%s", training_process_table)
            except Exception as proc_exc:
                logger.warning(f"[Learning] TRAINING_PROCESS 湲곕줉 ?ㅽ뙣: {proc_exc}")
            
            try:
                # 以묒븰?붾맂 LEARN_LIST ?곹깭 ?낅뜲?댄듃 ?⑥닔 ?ъ슜
                from db.mariadb_save_update import update_learn_list_status_board

                # LEARN_LIST???대? ??λ맂 cate(?됰퀎 ?⑦꽩 留ㅼ묶 ?? ??update?먯꽌 CATEGORY 洹쒖튃 蹂닿컯蹂대떎 ?곗꽑
                cate_update_kwargs = _build_non_empty_cate_update_kwargs(row)
                logger.info(
                    "[Learning][cate-update-debug] status update call | id=%s file=%s row_cate=(%s,%s) kwargs=%s final_chunks=%s",
                    db_id,
                    filename,
                    (row or {}).get("cate1") if isinstance(row, dict) else None,
                    (row or {}).get("cate2") if isinstance(row, dict) else None,
                    cate_update_kwargs,
                    final_chunks,
                )
                try:
                    raw_filters = await get_url_pattern_raw(self.chat_bot_id, "period", self.db_name)
                    logger.debug(f"[Learning][DBG] category rule raw fetched len={len(raw_filters) if raw_filters else 0}")
                except Exception as e:
                    raw_filters = None
                    logger.debug(f"[Learning][DBG] get_url_pattern_raw failed: {e}")

                effective_post_date = post_reg_date
                if effective_post_date is None and isinstance(row, dict):
                    effective_post_date = row.get("content_created_at")

                from backend.shared.db_write_queue import run_db_write

                ok = await run_db_write(
                    "learning.learn_list_status_update",
                    lambda: update_learn_list_status_board(
                        db_name=self.db_name,
                        chat_bot_id=self.chat_bot_id,
                        db_id=str(db_id),
                        chunks=int(final_chunks or 0),
                        raw_filters_str=raw_filters,
                        content_created_at=effective_post_date,
                        preserve_created_at=preserve_created_at,
                        **cate_update_kwargs,
                    ),
                )
                if ok:
                    logger.info(f"[Learning] 최종 상태 업데이트 완료 via update_learn_list_status_board: ID={db_id}, status=Y, chunk={final_chunks}")
                else:
                    logger.warning(f"[Learning] 理쒖쥌 ?곹깭 ?낅뜲?댄듃 ?ㅽ뙣 (update_learn_list_status_board returned False): ID={db_id}")
                    return False
            except Exception as final_upd_exc:
                logger.warning(f"[Learning] 理쒖쥌 ?곹깭 ?낅뜲?댄듃 ?ㅽ뙣: {final_upd_exc}")
                return False
            # File learning titles must come from the stored subject/file name, not breadcrumb labels.
            web_title = ""

            display_subject = subject or filename
            success_msg = f"파일 학습 완료 | ID={db_id} | {display_subject} | status=Y | chunk={final_chunks}"
            logger.info(f"[Learning] {success_msg}")
            self._write_log(success_msg)
            
            if self.progress_callback and stats is not None:
                try:
                    self.progress_callback(stats)
                except Exception as callback_exc:
                    logger.warning(f"[Learning] progress_callback ?몄텧 ?ㅽ뙣: {callback_exc}")

            if summarize_after_status_y:
                await self._await_summarize_keywords_after_learn_steps(
                    content=content_value,
                    pg_content=pg_content_value,
                    content_type=(row or {}).get("content_type") if isinstance(row, dict) else None,
                    learn_list_id=db_id,
                    normalized_text=summarize_normalized_text,
                )
            else:
                logger.info(
                    "[Learning][SummarizeDebug] skip generic dispatch by caller | id=%s file=%s",
                    db_id,
                    filename,
                )

            return True
                
        except Exception as exc:
            # ?곸꽭 ?덉쇅 濡쒓렇 (?ㅽ깮 ?몃젅?댁뒪 ?ы븿)
            logger.exception("[Learning] ???숈뒿 ?ㅻ쪟 諛쒖깮 | ID=%s file=%s err=%s", db_id, filename, exc)
            try:
                extra_dbg = {
                    "db_id": db_id,
                    "filename": filename,
                    "subject": subject if 'subject' in locals() else None,
                    "content_value_preview": (content_value or "")[:300] if 'content_value' in locals() and isinstance(content_value, str) else None,
                    "pg_lookup_value": matched_pg_lookup_value if 'matched_pg_lookup_value' in locals() and matched_pg_lookup_value else (pg_lookup_value if 'pg_lookup_value' in locals() else None),
                    "expected_min": expected_min if 'expected_min' in locals() else None,
                }
                self._write_log(f"[ERROR] ?숈뒿 ?ㅻ쪟 | ID={db_id} file={filename} exc={exc} context={extra_dbg}")
            except Exception:
                pass
            return False


