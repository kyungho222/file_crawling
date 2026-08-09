import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.settings import settings
from db.db_redis import get_redis
from backend.shared.crawl_redis_keys import (
    crawl_client_heartbeat_key,
    crawl_state_key,
    crawl_state_scan_pattern,
    db_name_from_crawl_state_key,
)

from backend.shared.redis_sse_service import send_message_to_redis_sse, get_last_publish_meta
from backend.shared.crawler_state import crawler_state

logger = logging.getLogger("backend.shared.crawl_shared")

try:
    from config import Config  # type: ignore
except Exception:  # pragma: no cover - Config 누락 대비
    Config = None  # type: ignore


def swallow_task_exception(task: asyncio.Task, *, label: str) -> None:
    """
    fire-and-forget 태스크의 예외를 회수하여 'Future exception was never retrieved' 경고를 방지.
    - Playwright TargetClosedError 등 종료 레이스에서 흔한 예외는 DEBUG로만 무시한다.
    """
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.debug("[%s] task.exception() failed (ignore): %s", label, e)
        return
    if not exc:
        return
    msg = str(exc)
    if "TargetClosedError" in msg or "Target page, context or browser has been closed" in msg:
        logger.debug("[%s] ignored TargetClosedError: %s", label, msg)
        return
    logger.warning("[%s] task failed: %s", label, exc, exc_info=True)


def bool_from_payload(value: Any) -> bool:
    """다양한 표현식을 불리언으로 변환"""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "y", "yes", "on"}
    return False


def resolve_stream_matched_rules_only(payload: Dict[str, Any], *, default: bool = False) -> bool:
    """
    CATEGORY url/query 패턴으로 start_urls를 제한할지 결정한다.

    - True: CATEGORY url/query 규칙에 매칭되는 post만 start_urls로 사용.
    - False: 기본 동작. 분류 규칙을 무시하고 탐색 DB의 post URL 전체를 start_urls로 사용.
    """
    if not isinstance(payload, dict):
        return default

    mode_keys = (
        "category_start_urls_mode",
        "category_pattern_mode",
        "start_urls_category_mode",
    )
    for key in mode_keys:
        raw = payload.get(key)
        if raw is None:
            continue
        mode = str(raw).strip().lower()
        if mode in {"all", "all_post", "all_posts", "post_all", "ignore", "ignore_category", "off", "disabled"}:
            return False
        if mode in {"category", "category_patterns", "pattern", "patterns", "matched", "matched_rules", "on", "enabled"}:
            return True

    negative_keys = (
        "ignore_category_patterns",
        "ignore_category_url_patterns",
        "disable_category_patterns",
        "disable_category_url_patterns",
        "fetch_all_post_urls",
        "all_post_start_urls",
    )
    for key in negative_keys:
        if key in payload:
            return not bool_from_payload(payload.get(key))

    positive_keys = (
        "stream_matched_rules_only",
        "use_category_url_patterns",
        "use_category_patterns",
        "category_pattern_url_enabled",
        "category_pattern_filter_enabled",
        "category_url_pattern_filter_enabled",
        "category_start_urls_enabled",
    )
    for key in positive_keys:
        if key in payload:
            return bool_from_payload(payload.get(key))

    return default


def detect_board_crawl(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    게시판 탐지 로직(단순화).
    - url_filter == 'Q'
    - URL 내 '?' 포함
    - board_mode/crawl_mode 값
    """
    print(f"============================ [test3] detect_board_crawl ============================")
    reasons: List[str] = []
    content_type = (payload.get("content_type") or "url").strip().lower()
    if content_type != "url":
        return {"mode": "file", "is_board": False, "reasons": ["content_type!=url"]}

    # crawl_start._prepare_crawl 전용: DB 선별 URL은 상세에 /bbs/ 등이 있어도 첨부(file) 파이프라인이다.
    # colle 미지정 시 board로 오인하면 BoardContentWorkflow로 가버림 → FileDownloadWorkflow로 가야 함.
    try:
        _ovs = str(payload.get("start_urls_override_source") or "").strip().lower()
    except Exception:
        _ovs = ""
    if _ovs in ("pre_explored_db", "file_crawl_post_db", "file_crawl_post_db_stream"):
        return {
            "mode": "file",
            "is_board": False,
            "reasons": [f"start_urls_override_source={_ovs}(file_pipeline)"],
        }

    url_filter = (payload.get("url_filter") or "").strip().upper()
    if url_filter == "Q":
        reasons.append("url_filter=Q")

    # start_urls_override가 있으면 게시판 모드로 간주(사전 탐색/사이트맵 기반)
    try:
        override_payload = payload.get("start_urls_override")
        print(f"============================ [test4] override_payload ============================")
        print(f"override_payload: {override_payload}")
    except Exception:
        override_payload = None
    if isinstance(override_payload, list) and override_payload:
        try:
            override_source = str(payload.get("start_urls_override_source") or "").strip().lower()
        except Exception:
            override_source = ""
        if override_source in {"sitemap_board_list", "sitemap_board", "pre_explored_asadal", "pre_explored", "sitemap"}:
            reasons.append(f"start_urls_override_source={override_source}")
        else:
            # 소스가 명확하지 않은 경우에도 보드 패턴이 있으면 보드로 판단
            try:
                for u in override_payload:
                    lu = str(u).lower()
                    if any(t in lu for t in ("/bbs/", "list.do", "view.do", "bbsno=", "nttno=", "bbsid=", "boardid=", "board")):
                        reasons.append("start_urls_override:board_pattern")
                        break
            except Exception:
                pass

    if bool_from_payload(payload.get("board_mode")):
        reasons.append("board_mode flag")

    crawl_mode = (payload.get("crawl_mode") or "").strip().lower()
    if crawl_mode == "crawling":
        reasons.append("crawl_mode=crawling")

    contents = payload.get("contents") or []
    if isinstance(contents, list):
        for idx, item in enumerate(contents):
            if isinstance(item, str) and "?" in item:
                reasons.append(f"contents[{idx}] contains '?'")
                break

    is_board = len(reasons) > 0
    mode = "board" if is_board else "file"
    if not reasons:
        reasons.append("default:file")

    return {
        "mode": mode,
        "is_board": is_board,
        "reasons": reasons,
    }


# SSE 종료 판정용 (crawl_shared에 있어야 workflow_runner/dispatcher가 공유 가능)
STOP_SSE_STATUSES = {"stop", "coll_stop", "cancelled", "cancel", "stopped"}
COMPLETE_SSE_STATUSES = {"complete", "completed", "finished"}
TERMINAL_SSE_STATUSES = STOP_SSE_STATUSES | COMPLETE_SSE_STATUSES | {"error"}


def normalize_status_for_sse(status: Optional[str]) -> str:
    """
    SSE 전송용 status 정규화
    프론트엔드가 기대하는 값: 'completed', 'cancelled', 'error', 'running'
    """
    normalized = (status or "").strip().lower()
    if normalized in {"", "none", "null", "undefined", "nan"}:
        return "running"
    if normalized in STOP_SSE_STATUSES:
        return "cancelled"
    if normalized in COMPLETE_SSE_STATUSES:
        return "completed"
    if normalized in {"error", "failed", "fail", "exception"}:
        return "error"
    # 'ok'/'crawled'는 중간 단계 성공으로도 쓰여 최종 완료로 오인될 수 있으므로 진행 상태로 정규화한다.
    if normalized in {"ok", "crawled"}:
        return "running"
    if normalized in {"start", "init", "initializing", "ready"}:
        return "running"
    return "running"


def state_key(db_name: str, job_id: str) -> str:
    return crawl_state_key(db_name, job_id)


async def resolve_db_name(job_id: str, provided: Optional[str] = None) -> Optional[str]:
    """요청에 account_name이 없을 때 Redis 메타데이터/상태 키를 통해 DB명을 추론한다."""
    try:
        redis = await get_redis()
    except Exception as exc:
        logger.warning("[SSE] Redis 연결 실패로 DB명을 가져올 수 없습니다: job_id=%s err=%s", job_id, exc)
        return provided

    meta_key = f"job_meta:{job_id}"
    try:
        meta = await redis.hgetall(meta_key)
        if meta:
            raw_db = meta.get("dbname") or meta.get(b"dbname")
            if raw_db:
                account_name = raw_db.decode("utf-8") if isinstance(raw_db, bytes) else raw_db
                # job_meta TTL 갱신(best-effort)
                ttl_default = 24 * 3600
                ttl = ttl_default
                if Config:
                    try:
                        ttl = int(getattr(Config, "REDIS_JOB_META_TTL_SEC", ttl_default))
                    except Exception:
                        ttl = ttl_default
                try:
                    await redis.expire(meta_key, ttl)
                except Exception:
                    pass
                logger.debug("[SSE] job_meta로 DB명을 확인했습니다: job_id=%s db=%s", job_id, account_name)
                return account_name
    except Exception as exc:
        logger.debug("[SSE] job_meta 조회 실패: job_id=%s err=%s", job_id, exc)

    # 상태 키를 통해 역으로 DB명을 찾는 fallback
    try:
        async for key in redis.scan_iter(match=crawl_state_scan_pattern(job_id), count=5):
            decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            parts = decoded_key.split(":")
            if len(parts) >= 3:
                logger.debug("[SSE] 상태 키로 DB명을 확인했습니다: job_id=%s key=%s", job_id, decoded_key)
                return parts[1]
    except Exception as exc:
        logger.debug("[SSE] 상태 키 SCAN 실패: job_id=%s err=%s", job_id, exc)

    # Redis에 메타가 없을 때는 메모리(현재 서버) 기록에서 DB명을 찾아본다.
    try:
        history = crawler_state.job_history.get(job_id) or {}
        mem_db = history.get("db_name")
        if not mem_db:
            wf = crawler_state.workflows.get(job_id)
            mem_db = getattr(wf, "db_name", None) if wf else None
        if mem_db:
            try:
                if provided and str(provided) != str(mem_db):
                    logger.info(
                        "[SSE] Overriding provided db_name with in-memory db_name | job_id=%s provided=%s resolved=%s",
                        job_id,
                        provided,
                        mem_db,
                    )
            except Exception:
                pass
            return mem_db
    except Exception:
        pass

    return provided


def resolve_chat_bot_id(job_id: str, provided: Optional[str] = None) -> Optional[str]:
    """요청 또는 기록에 없으면 기본 챗봇 ID를 반환."""
    if provided:
        return provided
    history = crawler_state.job_history.get(job_id)
    if history:
        cached = history.get("chat_bot_id")
        if cached:
            return cached
    return settings.DEFAULT_CHAT_BOT_ID


async def cache_job_metadata(job_id: str, db_name: str) -> None:
    """job_id별 DB명을 Redis에 캐싱해 SSE에서 역추적할 수 있도록 한다."""
    if not job_id or not db_name:
        return
    try:
        redis = await get_redis()
    except Exception as exc:
        logger.debug("[SSE] job_meta 캐시 실패(연결): job_id=%s err=%s", job_id, exc)
        return

    meta_key = f"job_meta:{job_id}"
    ttl_default = 24 * 3600
    ttl = ttl_default
    if Config:
        try:
            ttl = int(getattr(Config, "REDIS_JOB_META_TTL_SEC", ttl_default))
        except Exception:
            ttl = ttl_default

    try:
        await redis.hset(
            meta_key,
            mapping={
                "dbname": db_name,
                "updated_at": datetime.now().isoformat(),
            },
        )
        await redis.expire(meta_key, ttl)
        logger.debug("[SSE] job_meta 캐싱 완료: job_id=%s db=%s ttl=%s", job_id, db_name, ttl)
    except Exception as exc:
        logger.debug("[SSE] job_meta 캐싱 중 오류: job_id=%s db=%s err=%s", job_id, db_name, exc)


async def publish_client_redis_heartbeat(job_id: str, db_name: str) -> Dict[str, Any]:
    """
    브라우저가 주기적으로 호출해 Redis PING 및 client 생존 키를 갱신한다.
    - 긴 Playwright/다운로드 구간에서 연결·메타 TTL 유지에 도움.
    """
    out: Dict[str, Any] = {"ok": False, "job_id": job_id, "db_name": db_name}
    if not job_id or not db_name:
        out["error"] = "missing_job_id_or_db_name"
        return out
    try:
        redis = await get_redis()
    except Exception as exc:
        out["error"] = f"redis_connect:{exc}"
        logger.warning("[ClientHB] Redis 연결 실패 | job_id=%s err=%s", job_id, exc)
        return out
    out["pong"] = True
    try:
        ttl = int(os.getenv("CRAWL_CLIENT_HEARTBEAT_TTL_SEC", "600") or "600")
    except Exception:
        ttl = 600
    ttl = max(120, min(ttl, 86400))
    hb_key = crawl_client_heartbeat_key(db_name, job_id)
    payload = json.dumps(
        {"ts": datetime.utcnow().isoformat() + "Z", "job_id": job_id, "db_name": db_name},
        ensure_ascii=False,
    )
    try:
        meta_key = f"job_meta:{job_id}"
        meta_ttl_default = 24 * 3600
        meta_ttl = meta_ttl_default
        if Config:
            try:
                meta_ttl = int(getattr(Config, "REDIS_JOB_META_TTL_SEC", meta_ttl_default))
            except Exception:
                meta_ttl = meta_ttl_default
        pipe = redis.pipeline(transaction=False)
        pipe.set(hb_key, payload, ex=ttl)
        pipe.hset(
            meta_key,
            mapping={
                "dbname": db_name,
                "updated_at": datetime.now().isoformat(),
            },
        )
        pipe.expire(meta_key, meta_ttl)
        await pipe.execute()
        out["ok"] = True
        out["heartbeat_key"] = hb_key
    except Exception as exc:
        out["error"] = f"set:{exc}"
        logger.warning("[ClientHB] 키 저장 실패 | job_id=%s err=%s", job_id, exc)
        return out
    logger.debug("[ClientHB] ok | job_id=%s db=%s", job_id, db_name)
    return out


def initial_state_payload() -> Dict[str, Any]:
    return {
        "status": "running",
        "scan_count": 0,
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "timestamp": datetime.now().isoformat(),
    }


async def publish_job_terminal_error(
    job_id: str,
    db_name: str,
    *,
    reason: str,
    message: str = "",
    source: str = "dispatch_terminal_error",
) -> None:
    """
    워크플로 시작 전 실패(탐색 start_urls 0건 등) 시 Redis `crawl:{db}:{job_id}:state` 에
    status=error 를 기록한다. bootstrap_job_state 가 호출되지 않는 경로용.
    """
    if not job_id or not db_name:
        return
    try:
        await cache_job_metadata(job_id, db_name)
    except Exception:
        pass
    msg = (message or reason or "").strip()[:2000]
    payload: Dict[str, Any] = {
        "status": "error",
        "scan_count": 0,
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "reason": (reason or "error")[:500],
        "message": msg,
        "timestamp": datetime.now().isoformat(),
        "job_id": job_id,
        "account_name": db_name,
        "source": source,
    }
    try:
        await send_sse_message(job_id, payload, db_name, source)
    except Exception as exc:
        logger.warning(
            "[SSE] publish_job_terminal_error failed | job_id=%s db=%s reason=%s err=%s",
            job_id,
            db_name,
            reason,
            exc,
            exc_info=True,
        )


async def publish_job_terminal_completed(
    job_id: str,
    db_name: str,
    *,
    reason: str,
    message: str = "",
    source: str = "dispatch_terminal_completed",
    scan_count: int = 0,
    duplicate_count: int = 0,
) -> None:
    """
    워크플로를 띄우기 전에 정상적으로 더 처리할 URL이 없다고 확정된 경우
    Redis/SSE 상태를 completed로 기록한다.
    """
    if not job_id or not db_name:
        return
    try:
        await cache_job_metadata(job_id, db_name)
    except Exception:
        pass
    try:
        scan = max(0, int(scan_count or 0))
    except Exception:
        scan = 0
    try:
        duplicates = max(0, int(duplicate_count or 0))
    except Exception:
        duplicates = 0
    msg = (message or reason or "").strip()[:2000]
    payload: Dict[str, Any] = {
        "status": "completed",
        "event": "workflow_completed",
        "scan_count": scan,
        "total_count": scan,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "duplicate_count": duplicates,
        "reason": (reason or "completed")[:500],
        "message": msg,
        "timestamp": datetime.now().isoformat(),
        "job_id": job_id,
        "account_name": db_name,
        "source": source,
    }
    try:
        await send_sse_message(job_id, payload, db_name, source)
    except Exception as exc:
        logger.warning(
            "[SSE] publish_job_terminal_completed failed | job_id=%s db=%s reason=%s err=%s",
            job_id,
            db_name,
            reason,
            exc,
            exc_info=True,
        )


def memory_snapshot_payload(job_id: str, db_name: str, workflow: Any) -> Dict[str, Any]:
    """Redis 상태가 초기화되기 전에 메모리 진행률을 즉시 내려주기 위한 보조 페이로드."""
    try:
        stats = workflow.get_stats()
    except Exception:
        stats = getattr(workflow, "stats", {}) or {}

    raw_status = getattr(workflow, "final_status", None) or ("running" if getattr(workflow, "is_running", False) else "complete")
    status = normalize_status_for_sse(raw_status)

    return {
        "status": status,
        "scan_count": stats.get("scan_count", 0),
        "total_count": stats.get("scan_count", 0),
        "collection_count": stats.get("collection_count", 0),
        "save_count": stats.get("save_count", 0),
        "study_count": stats.get("study_count", 0),
        "timestamp": datetime.now().isoformat(),
        "job_id": job_id,
        "account_name": db_name,
        "source": "memory_snapshot",
    }


INITIAL_SSE_STATE_GRACE_SECONDS = 10
INITIAL_SSE_STATE_POLL_INTERVAL = 0.5


async def send_sse_message(job_id: str, payload: Dict[str, Any], db_name: str, source: str):
    """
    Redis SSE 발행 래퍼. 내부에서 send_message_to_redis_sse(job_id, payload, dbname) 호출.

    발행 시점(호출처):
      - 최초 발행: crawl_dispatcher.dispatch_and_schedule_workflow()
        -> bootstrap_job_state(job_id, db_name, "dispatch")
        -> send_sse_message(..., "bootstrap:dispatch") 또는 "bootstrap:retry:N"
      - 진행/완료: workflow_runner progress_callback -> enqueue_sse_message (큐)
        -> sse_publish_queue 워커 -> send_sse_message(..., "workflow_progress")
      - 완료 터미널: workflow_runner -> send_sse_message(..., "workflow_completed")
      - 파일 모드 감지: crawl_start -> send_sse_message(..., "crawl_file_mode")
    """
    try:
        prev = payload.get("status")
        norm = normalize_status_for_sse(prev)
        payload["status"] = norm
    except Exception:
        pass
    try:
        result = await send_message_to_redis_sse(job_id, payload, dbname=db_name)
        try:
            meta = get_last_publish_meta(job_id)
        except Exception:
            meta = {}
        logger.debug(
            "[SSE:%s] Publish success | job_id=%s db=%s status=%s totals=%s meta=%s",
            source,
            job_id,
            db_name,
            payload.get("status"),
            {
                "total": payload.get("total_count"),
                "collection": payload.get("collection_count"),
                "save": payload.get("save_count"),
                "study": payload.get("study_count"),
            },
            meta,
        )
        return result
    except Exception as exc:
        logger.exception(
            "[SSE:%s] Publish failed | job_id=%s db=%s err=%s",
            source,
            job_id,
            db_name,
            exc,
        )
        raise


async def bootstrap_job_state(job_id: str, db_name: str, source: str) -> bool:
    """
    Redis 메타/상태 초기화와 검증을 일관된 방식으로 수행한다.
    """
    await cache_job_metadata(job_id, db_name)

    # 우선: 클라이언트에서 confirm_subscribe POST로 구독 완료를 통지하면
    # 최대 대기시간 동안 기다려 초기 발행이 구독자에게 도달하도록 시도한다.
    try:
        confirm_wait_sec = float(os.getenv("SSE_CONFIRM_WAIT_SEC", "3") or "3")
    except Exception:
        confirm_wait_sec = 3.0
    confirm_wait_sec = max(0.0, min(confirm_wait_sec, 10.0))
    start_wait = asyncio.get_event_loop().time()
    confirmed = False
    try:
        while (asyncio.get_event_loop().time() - start_wait) < confirm_wait_sec:
            try:
                if job_id in getattr(crawler_state, "confirmed_subscriptions", set()):
                    confirmed = True
                    logger.debug("[Bootstrap:%s] subscriber confirmed for job_id=%s", source, job_id)
                    break
            except Exception:
                pass
            await asyncio.sleep(0.05)
    except Exception:
        pass

    initial_message = initial_state_payload()
    # Do not reset already-published progress while waiting for the SSE subscriber.
    # A dispatch bootstrap can run after URL discovery has emitted real counts.
    try:
        previous_message = dict((get_last_publish_meta(job_id) or {}).get("message") or {})
    except Exception:
        previous_message = {}
    if previous_message:
        initial_message.update(previous_message)
    initial_message["status"] = normalize_status_for_sse(initial_message.get("status"))
    initial_message["source"] = source
    # 첫 발행 시도 및 짧은 재시도(retry) 로직:
    # - published_raw(=published) 여부를 확인해 구독자가 없어서 발행이 무시된 경우
    #   짧게 재시도하여 구독 연결 타이밍 레이스를 완화한다.
    published = False
    try:
        res = await send_sse_message(job_id, initial_message, db_name, f"bootstrap:{source}")
        try:
            published = bool(getattr(res, "published", False))
        except Exception:
            published = False
    except Exception as exc:
        logger.warning(
            "[Bootstrap:%s] initial SSE publish failed | job_id=%s db=%s err=%s",
            source,
            job_id,
            db_name,
            exc,
        )

    # 최대 재시도: 점진적 백오프 (총 대기 최대 ~1.9s)
    retry = 0
    max_retries = 5
    backoff = 0.1
    while not published and retry < max_retries:
        try:
            await asyncio.sleep(backoff)
            res = await send_sse_message(job_id, initial_message, db_name, f"bootstrap:retry:{retry}")
            try:
                if getattr(res, "published", False):
                    published = True
                    break
            except Exception:
                pass
        except Exception as exc:
            logger.debug(
                "[Bootstrap:%s] retry publish failed (ignore) | job_id=%s db=%s retry=%s err=%s",
                source,
                job_id,
                db_name,
                retry,
                exc,
            )
        retry += 1
        backoff = min(1.0, backoff * 2)

    # Redis에 state key가 생성되는지 확인(best-effort)
    try:
        redis = await get_redis()
        key = state_key(db_name, job_id)
        start = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start) < INITIAL_SSE_STATE_GRACE_SECONDS:
            try:
                exists = await redis.exists(key)
                if exists:
                    return True
            except Exception:
                pass
            await asyncio.sleep(INITIAL_SSE_STATE_POLL_INTERVAL)
        logger.debug("[Bootstrap:%s] Redis state key not observed within grace | job_id=%s db=%s key=%s", source, job_id, db_name, key)
        return False
    except Exception as exc:
        logger.warning(
            "[Bootstrap:%s] Redis state verification failed | job_id=%s db=%s err=%s",
            source,
            job_id,
            db_name,
            exc,
        )
        return False
