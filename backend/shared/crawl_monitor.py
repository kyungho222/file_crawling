import asyncio
import logging
import time
import os
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Tuple

logger = logging.getLogger("backend.shared.crawl_monitor")

# config 미조회/실패 시 사용하는 방어 기본값 (DB ASADAL_CRAWLING_CONFIG 없을 때)
DEFAULT_WEEK_COUNT = 3000
DEFAULT_PAGE_COUNT = 3000
DEFAULT_STOP_COUNT = 10800

# BoardContentWorkflow 등: 생성 시 final_status="running", is_running=False → 조기 종료 방지
_WORKFLOW_ACTIVE_STATUSES = frozenset({"running", "unknown", ""})
_WORKFLOW_TERMINAL_STATUSES = frozenset({"completed", "stopped", "failed", "done", "error", "fail", "exception"})
_WORKFLOW_ACTIVE_STATES = frozenset({"running", "stopping", "init"})
_AUTO_STOP_CONFIG_CACHE_TTL_SEC = 300.0
_AUTO_STOP_CONFIG_FETCH_TIMEOUT_SEC = 3.0
_AUTO_STOP_CONFIG_CACHE: Dict[tuple, Tuple[float, Dict[str, Any], str]] = {}


def _workflow_state_norm(workflow: Any) -> str:
    state = getattr(workflow, "state", None)
    if state is None:
        return ""
    return str(getattr(state, "value", state) or "").strip().lower()


def _workflow_still_active(workflow: Any, *, final_status_norm: str, is_running: bool) -> bool:
    """is_running=False 이어도 finalize/시작 대기 중이면 True."""
    if is_running:
        return True
    if final_status_norm in _WORKFLOW_ACTIVE_STATUSES:
        if _workflow_state_norm(workflow) in _WORKFLOW_ACTIVE_STATES:
            return True
        q = getattr(workflow, "_post_save_queue", None)
        if isinstance(q, asyncio.Queue) and q.qsize() > 0:
            return True
    return False


def _parse_int_or_none(value: Any) -> int | None:
    """헬퍼 함수: 설정값을 정수로 변환한다."""
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except (ValueError, TypeError):
        return None

def _normalize_candidate_id(value: Any) -> str | None:
    raw = str(value).strip() if value is not None else ""
    return raw or None


def _extract_chat_bot_suffix(chat_bot_id: str | None) -> str | None:
    normalized = _normalize_candidate_id(chat_bot_id)
    if not normalized:
        return None
    parts = normalized.split("-")
    suffix = parts[-1].strip() if parts else ""
    return suffix or None


def _config_has_any_value(conf: dict[str, Any], keys: list[str]) -> bool:
    if not conf:
        return False
    return any(conf.get(key) is not None for key in keys)


def _build_auto_stop_config_candidates(*, chat_bot_id: str | None, workflow: Any) -> list[tuple[str | None, bool, str]]:
    candidates: list[tuple[str | None, bool, str]] = []
    seen: set[tuple[str, bool]] = set()

    def _add(candidate_id: str | None, *, match_chat_bot_id: bool, source: str) -> None:
        normalized = _normalize_candidate_id(candidate_id)
        dedupe_key = (normalized or "", match_chat_bot_id)
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        candidates.append((normalized, match_chat_bot_id, source))

    _add(chat_bot_id, match_chat_bot_id=True, source="chat_bot_id")
    _add(getattr(workflow, "unique_id", None), match_chat_bot_id=True, source="workflow.unique_id")

    suffix = _extract_chat_bot_suffix(chat_bot_id)
    if suffix:
        _add(suffix, match_chat_bot_id=True, source="chat_bot_suffix")
        _add(suffix.upper(), match_chat_bot_id=True, source="chat_bot_suffix_upper")

    _add(None, match_chat_bot_id=False, source="shared")
    return candidates


async def _load_auto_stop_config(
    *,
    fetcher: Callable[..., Awaitable[list[Any]]],
    workflow: Any,
    chat_bot_id: str | None,
    db_name: str,
    job_id: str,
    keys: list[str],
    fetch_timeout_sec: float = _AUTO_STOP_CONFIG_FETCH_TIMEOUT_SEC,
) -> tuple[dict[str, Any], str]:
    candidates = _build_auto_stop_config_candidates(chat_bot_id=chat_bot_id, workflow=workflow)
    candidate_sources = ",".join(source for _, _, source in candidates)
    primary_id = _normalize_candidate_id(chat_bot_id) or _normalize_candidate_id(getattr(workflow, "unique_id", None))
    normalized_keys = tuple(sorted(str(key).strip() for key in keys if str(key or "").strip()))
    candidate_ids = [candidate_id for candidate_id, _, _ in candidates]
    resolved_cache_key = ("resolved", db_name, tuple(candidate_ids), normalized_keys)
    defaults_cache_key = ("defaults", db_name, primary_id or "", normalized_keys)
    now = time.monotonic()
    for cache_key in (resolved_cache_key, defaults_cache_key):
        cached = _AUTO_STOP_CONFIG_CACHE.get(cache_key)
        if cached and now - cached[0] <= _AUTO_STOP_CONFIG_CACHE_TTL_SEC:
            return dict(cached[1]), cached[2]

    try:
        rows = await asyncio.wait_for(
            fetcher(candidate_ids=candidate_ids, keys=keys, dbname=db_name),
            timeout=max(0.1, float(fetch_timeout_sec)),
        )
    except asyncio.TimeoutError:
        logger.warning(
            "[AutoStop] Config fetch timeout; using defaults | job_id=%s db=%s bot_id=%s timeout_sec=%.1f",
            job_id,
            db_name,
            primary_id,
            max(0.1, float(fetch_timeout_sec)),
        )
        return {}, "defaults"
    except Exception as exc:
        logger.warning(
            "[AutoStop] Config fetch error; using defaults | job_id=%s db=%s bot_id=%s err=%s",
            job_id,
            db_name,
            primary_id,
            exc,
        )
        return {}, "defaults"

    def _row_value(row: Any, key: str, index: int) -> Any:
        if isinstance(row, dict):
            if key in row:
                return row.get(key)
            for row_key, value in row.items():
                if str(row_key).strip().strip("`").lower() == key:
                    return value
        try:
            return row[index]
        except (IndexError, KeyError, TypeError):
            return None

    common_values: Dict[str, Any] = {}
    candidate_values: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        key = str(_row_value(row, "key", 1) or "").strip()
        if not key:
            continue
        candidate_id = _normalize_candidate_id(_row_value(row, "chat_bot_id", 0))
        value = _row_value(row, "value", 2)
        if not candidate_id or candidate_id.lower() == "default":
            common_values.setdefault(key, value)
        else:
            candidate_values.setdefault(candidate_id, {})[key] = value

    for candidate_id, _, source in candidates:
        conf = dict(common_values)
        if candidate_id:
            conf.update(candidate_values.get(candidate_id, {}))
        if _config_has_any_value(conf, keys):
            _AUTO_STOP_CONFIG_CACHE[resolved_cache_key] = (time.monotonic(), dict(conf), source)
            if source != "chat_bot_id":
                logger.warning(
                    "[AutoStop] Config resolved via fallback | source=%s job_id=%s db=%s bot_id=%s",
                    source,
                    job_id,
                    db_name,
                    primary_id,
                )
            return conf, source

    logger.warning(
        "[AutoStop] Config not found, using defaults | job_id=%s db=%s bot_id=%s candidates=%s",
        job_id,
        db_name,
        primary_id,
        candidate_sources,
    )
    _AUTO_STOP_CONFIG_CACHE[defaults_cache_key] = (time.monotonic(), {}, "defaults")
    return {}, "defaults"


async def monitor_auto_stop(
    *,
    workflow: Any,
    job_id: str,
    db_name: str,
    chat_bot_id: str | None,
    stop_signal: asyncio.Event,
    start_time: datetime | None = None,
    interval_sec: float = 3.0,
    refresh_config_sec: float = 30.0,
    source: str = "unknown",
) -> None:
    """크롤링 진행 중 자동 중단 조건을 감시한다."""
    # 중복 실행 방지 (최대한 빨리 플래그 설정)
    if getattr(workflow, "_auto_stop_monitor_started", False):
        logger.warning("[AutoStop] AlreadyStarted | job_id=%s source=%s", job_id, source)
        return
    setattr(workflow, "_auto_stop_monitor_started", True)

    end_reason = "unknown"
    try:
        print(f"***** monitoring start@crawl_monitor.py ***** job_id={job_id}")

        # 1. 초기 지연 (DB 반영 및 워크플로우 세팅 시간을 벌어줌)
        await asyncio.sleep(2.0)

        # 파라미터 유효성 검사
        target_id = str(chat_bot_id).strip() if chat_bot_id else None
        target_id = _normalize_candidate_id(chat_bot_id)
        workflow_unique_id = _normalize_candidate_id(getattr(workflow, "unique_id", None))
        if not (db_name and (target_id or workflow_unique_id)):
            logger.warning(
                "[AutoStop] Disabled | job_id=%s db=%s bot_id=%s",
                job_id,
                db_name,
                target_id or workflow_unique_id,
            )
            return

        try:
            # DB 설정값, 시간, 중단 기능을 가져옵니다.
            from db.crawl_db_manager import get_config_rows_by_candidate_ids
            from utils.timezone_utils import get_local_now
            from backend.router import stop_crawl
        except Exception as exc:
            logger.error("[AutoStop] Import failed: %s", exc)
            return

        if start_time is None:
            start_time = get_local_now()

        # 업무 시간(평일 9-18시) 여부 확인
        is_weekday = start_time.weekday() < 5
        use_week_limit = bool(is_weekday and 9 <= start_time.hour < 18)

        monitor_start_log_level = (
            logging.INFO
            if str(os.getenv("AUTO_STOP_MONITOR_START_INFO", "0") or "0").strip().lower()
            in {"1", "true", "yes", "on"}
            else logging.DEBUG
        )
        logger.log(
            monitor_start_log_level,
            "[AutoStop] MonitorStart | job_id=%s source=%s start_time=%s use_week=%s",
            job_id,
            source,
            start_time.isoformat(),
            use_week_limit,
        )

        # 2. 초기 설정값 로딩 (최대 10회 재시도)
        conf, config_source = await _load_auto_stop_config(
            fetcher=get_config_rows_by_candidate_ids,
            workflow=workflow,
            chat_bot_id=target_id or workflow_unique_id,
            db_name=db_name,
            job_id=job_id,
            keys=["week_count", "page_count", "stop_count"],
            fetch_timeout_sec=_AUTO_STOP_CONFIG_FETCH_TIMEOUT_SEC,
        )

        if not conf:
            logger.warning(
                "[AutoStop] Config empty after retries, using defaults | job_id=%s db=%s bot_id=%s",
                job_id,
                db_name,
                target_id or workflow_unique_id,
            )

        # 초기 설정 파싱 + 방어: None이면 기본값 적용
        week_limit = _parse_int_or_none(conf.get("week_count"))
        night_limit = _parse_int_or_none(conf.get("page_count"))
        stop_limit = _parse_int_or_none(conf.get("stop_count"))
        if week_limit is None:
            week_limit = DEFAULT_WEEK_COUNT
            logger.debug("[AutoStop] Config fallback | week_count=%s (default)", week_limit)
        if night_limit is None:
            night_limit = DEFAULT_PAGE_COUNT
            logger.debug("[AutoStop] Config fallback | page_count=%s (default)", night_limit)
        if stop_limit is None:
            stop_limit = DEFAULT_STOP_COUNT
            logger.debug("[AutoStop] Config fallback | stop_count=%s (default)", stop_limit)
        time_limit = week_limit if use_week_limit else night_limit
        last_config_ts = time.time()

        # 3. 메인 감시 루프
        loop_count = 0
        last_heartbeat_ts = time.time()
        try:
            heartbeat_sec = float(os.getenv("AUTO_STOP_HEARTBEAT_SEC", "30") or "30")
        except Exception:
            heartbeat_sec = 30.0
        heartbeat_sec = max(5.0, min(heartbeat_sec, 300.0))
        try:
            start_wait_sec = float(os.getenv("AUTO_STOP_WAIT_START_SEC", "180") or "180")
        except Exception:
            start_wait_sec = 180.0
        start_wait_sec = max(5.0, min(start_wait_sec, 3600.0))
        start_wait_deadline = time.time() + start_wait_sec
        started = False

        while not stop_signal.is_set():
            loop_count += 1
            now_ts = time.time()

            # 워크플로우 상태 체크
            wf_final_status = getattr(workflow, "final_status", None)
            wf_final_status_norm = str(wf_final_status).lower() if wf_final_status is not None else "unknown"
            wf_is_running = bool(getattr(workflow, "is_running", False))
            is_finished_status = wf_final_status_norm in _WORKFLOW_TERMINAL_STATUSES
            wf_still_active = _workflow_still_active(
                workflow,
                final_status_norm=wf_final_status_norm,
                is_running=wf_is_running,
            )

            # 워크플로우가 실제로 시작될 때까지 대기 (prestart 단계 보호)
            if not started:
                # final_status만 "running"이면 started로 보지 않음 (초기 is_running=False 오판 방지)
                if wf_is_running or wf_final_status_norm in _WORKFLOW_TERMINAL_STATUSES:
                    started = True
                elif now_ts < start_wait_deadline:
                    if loop_count % 10 == 0:
                        try:
                            from backend.shared.crawler_state import crawler_state

                            slot_snapshot = crawler_state.get_workflow_slot_snapshot()
                            task = crawler_state.workflow_tasks.get(job_id)
                            task_state = (
                                "missing"
                                if task is None
                                else ("done" if task.done() else "running")
                            )
                            active_slot = crawler_state.has_active_workflow_slot(job_id)
                        except Exception:
                            slot_snapshot = {}
                            task_state = "unknown"
                            active_slot = False
                    await asyncio.sleep(interval_sec)
                    continue
                else:
                    end_reason = "workflow_not_started"
                    break

            # 초기 기동 후 일정 시간 지났을 때 종료 여부 판단
            if loop_count > 5:
                if is_finished_status:
                    end_reason = f"workflow_finished ({wf_final_status})"
                    break
                if not wf_is_running and not wf_still_active:
                    end_reason = "workflow_not_running"
                    break

            # 외부 중단 이벤트 확인
            stop_event = getattr(workflow, "stop_event", None)
            if stop_event and stop_event.is_set():
                end_reason = "workflow_stop_event"
                break

            # 주기적인 설정 새로고침
            if now_ts - last_config_ts >= refresh_config_sec:
                try:
                    conf, _ = await _load_auto_stop_config(
                        fetcher=get_config_rows_by_candidate_ids,
                        workflow=workflow,
                        chat_bot_id=target_id or workflow_unique_id,
                        db_name=db_name,
                        job_id=job_id,
                        keys=["week_count", "page_count", "stop_count"],
                        fetch_timeout_sec=_AUTO_STOP_CONFIG_FETCH_TIMEOUT_SEC,
                    )
                    conf = conf or {}
                    week_limit = _parse_int_or_none(conf.get("week_count"))
                    night_limit = _parse_int_or_none(conf.get("page_count"))
                    stop_limit = _parse_int_or_none(conf.get("stop_count"))
                    if week_limit is None:
                        week_limit = DEFAULT_WEEK_COUNT
                    if night_limit is None:
                        night_limit = DEFAULT_PAGE_COUNT
                    if stop_limit is None:
                        stop_limit = DEFAULT_STOP_COUNT
                    time_limit = week_limit if use_week_limit else night_limit
                    last_config_ts = now_ts
                except Exception as e:
                    logger.warning("[AutoStop] Refresh failed, keeping previous limits: %s", e)

            # 수집 통계 조회
            stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
            collection_count = int(stats.get("collection_count", 0) or 0)

            # --- [자동 중단 검사 로직] ---

            # 1) 날짜 기반 하드 브레이크 (수집 데이터가 시작일보다 과거면 중단)
            use_date_hard_break = str(os.getenv("WORKFLOW_AUTO_STOP_DATE_HARD_BREAK", "1")).lower() in ("1", "true", "yes", "on")
            if use_date_hard_break:
                start_dt = getattr(workflow, "start_date", None)
                last_summary = getattr(workflow, "_last_page_summary", None)
                if start_dt and last_summary and isinstance(last_summary, dict):
                    dr = last_summary.get("date_range")
                    if dr and "~" in dr:
                        try:
                            right_date_str = dr.split("~")[-1].strip()
                            parsed_date = None
                            for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d"):
                                try:
                                    parsed_date = datetime.strptime(right_date_str, fmt)
                                    break
                                except: continue
                            
                            if parsed_date:
                                # 날짜 비교를 위해 datetime 객체로 통일
                                sd_dt = start_dt if isinstance(start_dt, datetime) else datetime.combine(start_dt, datetime.min.time())
                                if parsed_date < sd_dt:
                                    await stop_crawl(job_id)
                                    end_reason = "date_hard_break"
                                    return
                        except Exception: pass

            # 2) 연속 범위 초과(Out of Range) 페이지 검사
            use_streak_stop = str(os.getenv("WORKFLOW_AUTO_STOP_OUT_OF_RANGE_STREAK", "1")).lower() in ("1", "true", "yes", "on")
            if use_streak_stop:
                streak = int(getattr(workflow, "_out_of_range_page_streak", 0) or 0)
                streak_limit = int(getattr(workflow, "_out_of_range_page_streak_limit", 0) or 0)
                if streak_limit > 0 and streak >= streak_limit:
                    await stop_crawl(job_id)
                    end_reason = "out_of_range_streak"
                    return

            # 3) 수집 개수 제한 도달 여부 확인
            reached_time_limit = time_limit is not None and collection_count >= time_limit
            reached_stop_limit = stop_limit is not None and collection_count >= stop_limit
            
            if reached_time_limit or reached_stop_limit:
                await stop_crawl(job_id)
                end_reason = "count_limit_reached"
                return

            # 주기적 하트비트 로그
            if now_ts - last_heartbeat_ts >= heartbeat_sec:
                last_heartbeat_ts = now_ts
                
                # Redis 연결 끊김 방지를 위해 생존 신호를 전송합니다.
                try:
                    # Redis 기능을 이 시점에만 독립적으로 불러옵니다.
                    from db.db_redis import get_redis

                    # 현재 활성화된 Redis 클라이언트 객체를 가져옵니다.
                    redis_client = await get_redis()

                    # 클라이언트가 정상적으로 존재하면 연결 유지 신호(Ping)를 보냅니다.
                    if redis_client:
                        try:
                            pong = await redis_client.ping()
                        except Exception as ping_err:
                            logger.warning(
                                "[AutoStop] Redis ping failed (during ping): %s | job_id=%s",
                                ping_err,
                                job_id,
                            )
                    else:
                        logger.warning(
                            "[AutoStop] Redis client not available | job_id=%s", job_id
                        )

                except ImportError as imp_err:
                    # 파일 경로나 이름이 달라서 모듈을 찾지 못할 경우 기록합니다.
                    logger.warning("[AutoStop] Redis import error (Check file path): %s", imp_err)
                except Exception as redis_err:
                    # 통신 에러 등 다른 이유로 실패할 경우 원인을 기록합니다.
                    logger.warning("[AutoStop] Redis ping failed (Connection issue): %s", redis_err)

            await asyncio.sleep(interval_sec)

    except Exception as e:
        logger.error("[AutoStop] Loop Error: %s", e)
        end_reason = f"error: {str(e)}"
    finally:
        setattr(workflow, "_auto_stop_monitor_started", False)
