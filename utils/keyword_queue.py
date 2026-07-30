"""
Redis Queue 기반 키워드/요약 백그라운드 추출 모듈

학습 파이프라인에서 키워드/요약 추출을 분리하여 Redis Queue로 비동기 처리.
학습은 즉시 완료되고, 키워드/요약은 백그라운드 워커가 순차 처리한다.

흐름:
1. 학습 파이프라인에서 enqueue_keyword_job()으로 작업을 Redis에 PUSH
2. 백그라운드 워커(keyword_worker)가 BRPOP으로 꺼내 처리
3. extract_keywords_llm() → update_keywords()

삭제 연동:
- learn_del 호출 시 cancel_keyword_jobs()로 취소 마킹
- 워커가 job을 꺼낼 때 Cancel 키 확인 → 매칭 시 스킵

Redis Keys:
  KEYWORD_QUEUE          (List, FIFO via LPUSH/BRPOP)
  KW_CANCEL:{bot}::{val} (String, TTL 1h, 삭제 마킹)
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

from db.db_redis import get_redis
from logs.logging_util import LoggerSingleton
from utils.learn_list_keyword import (
    ensure_keyword_columns,
    extract_keywords_llm,
    find_latest_learn_item_id,
    update_keywords,
    _normalize_text,
    _has_body,
    fetch_chunks_text,
    _resolve_training_table,
    LEARN_LIST_KEYWORD_COLUMNS_ENABLED,
)

logger = LoggerSingleton.get_logger(logger_name="utils.keyword_queue", level=logging.INFO)

QUEUE_KEY = "KEYWORD_QUEUE"
CANCEL_PREFIX = "KW_CANCEL"
CANCEL_TTL = 3600
MAX_RETRIES = 2
RETRY_DELAY_BASE = 2
WORKER_CONCURRENCY = 3

_worker_task: Optional[asyncio.Task] = None
_stop_event = asyncio.Event()


async def enqueue_keyword_job(
    *,
    chat_bot_id: str,
    maria_db_name: str,
    content_type: str,
    subject: str = "",
    content: str = "",
    text_for_llm: Optional[str] = None,
    normalized_text: Optional[str] = None,
    pg_db_name: Optional[str] = None,
    db_type: Optional[str] = None,
    learn_table_override: Optional[str] = None,
) -> bool:
    """
    키워드/요약 추출 작업을 Redis Queue에 PUSH.

    Args:
        chat_bot_id: 챗봇 ID
        maria_db_name: MariaDB/MySQL 스키마명
        content_type: 콘텐츠 유형 (url, image, video, sound, file 등)
        subject: LEARN_LIST 조회용 subject
        content: LEARN_LIST 조회용 content (또는 LLM 입력 텍스트)
        text_for_llm: LLM에 직접 전달할 텍스트 (있으면 이걸 우선 사용)
        normalized_text: 정규화된 텍스트 (PG 조회 대신 직접 사용)
        pg_db_name: PostgreSQL DB명 (text_for_llm이 없을 때 PG 폴백용)
        db_type: "maria" / "mysql" / None(자동판별)
        learn_table_override: 테이블명 직접 지정 (예: SOUND_LEARN_LIST)

    Returns:
        True: 큐에 성공적으로 추가됨
    """
    if not LEARN_LIST_KEYWORD_COLUMNS_ENABLED:
        logger.info(
            "[enqueue] skipped: LEARN_LIST keyword/memo1 columns disabled | content_type=%s subject=%s",
            content_type,
            subject[:60],
        )
        return False

    job = {
        "chat_bot_id": chat_bot_id,
        "maria_db_name": maria_db_name,
        "content_type": content_type,
        "subject": subject,
        "content": content,
        "text_for_llm": text_for_llm,
        "normalized_text": normalized_text,
        "pg_db_name": pg_db_name,
        "db_type": db_type,
        "learn_table_override": learn_table_override,
        "enqueued_at": time.time(),
        "retry_count": 0,
    }

    try:
        r = await get_redis()

        # 재학습(delete→re-insert) 시 이전 cancel 마킹이 남아있으면 제거
        cancel_keys = []
        for val in (content, subject):
            val = (val or "").strip()
            if val:
                cancel_keys.append(_cancel_key(chat_bot_id, val))
        if cancel_keys:
            await r.delete(*cancel_keys)

        await r.lpush(QUEUE_KEY, json.dumps(job, ensure_ascii=False))
        queue_len = await r.llen(QUEUE_KEY)
        logger.info(
            f"[enqueue] content_type={content_type}, subject={subject[:60]}, "
            f"queue_len={queue_len}"
        )
        return True
    except Exception as e:
        logger.error(f"[enqueue] Redis PUSH 실패: {e}")
        return False


def _cancel_key(chat_bot_id: str, value: str) -> str:
    return f"{CANCEL_PREFIX}:{chat_bot_id}::{value}"


async def cancel_keyword_jobs(
    chat_bot_id: str,
    contents: List[str],
) -> int:
    """
    learn_del 시 호출. 삭제 대상 content를 Cancel 키로 마킹하여
    워커가 해당 작업을 스킵하도록 한다.

    Args:
        chat_bot_id: 챗봇 ID
        contents: 삭제 대상 content 값 리스트 (URL, 파일명 등)

    Returns:
        마킹된 건수
    """
    if not contents:
        return 0

    try:
        r = await get_redis()
        pipe = r.pipeline()
        count = 0
        for val in contents:
            val = (val or "").strip()
            if not val:
                continue
            key = _cancel_key(chat_bot_id, val)
            pipe.set(key, "1", ex=CANCEL_TTL)
            count += 1
        if count:
            await pipe.execute()
        logger.info(
            f"[cancel] chat_bot_id={chat_bot_id}, "
            f"마킹 {count}건 (TTL={CANCEL_TTL}s)"
        )
        return count
    except Exception as e:
        logger.error(f"[cancel] Redis 마킹 실패: {e}")
        return 0


async def _is_cancelled(r, job: dict) -> bool:
    """job의 content/subject가 Cancel 마킹되어 있는지 확인."""
    chat_bot_id = job.get("chat_bot_id", "")
    candidates = set()
    for field in ("content", "subject"):
        v = (job.get(field) or "").strip()
        if v:
            candidates.add(v)
    if not candidates:
        return False

    for val in candidates:
        key = _cancel_key(chat_bot_id, val)
        if await r.exists(key):
            await r.delete(key)
            return True
    return False


async def _process_job(job: dict) -> dict:
    """
    단건 키워드/요약 추출 + DB UPDATE 처리.

    Returns: {"status": "success"|"skip"|"error", ...}
    """
    if not LEARN_LIST_KEYWORD_COLUMNS_ENABLED:
        return {"status": "skip", "reason": "learn_list_keyword_columns_disabled"}

    chat_bot_id = job["chat_bot_id"]
    maria_db_name = job["maria_db_name"]
    content_type = job["content_type"]
    subject = job.get("subject", "")
    content = job.get("content", "")
    text_for_llm = job.get("text_for_llm")
    normalized_text = job.get("normalized_text")
    pg_db_name = job.get("pg_db_name")
    db_type = job.get("db_type")
    learn_table_override = job.get("learn_table_override")

    if learn_table_override:
        learn_table = learn_table_override
    else:
        tail = chat_bot_id.replace("-", "")[-12:]
        learn_table = f"ASADAL_{tail}_LEARN_LIST"

    await ensure_keyword_columns(learn_table, maria_db_name, db_type=db_type)

    item_id = await find_latest_learn_item_id(
        learn_table=learn_table,
        maria_db_name=maria_db_name,
        content_type=content_type,
        subject=subject,
        content=content,
        db_type=db_type,
    )
    if not item_id:
        return {"status": "skip", "reason": "LEARN_LIST row 미존재"}

    llm_input = None
    if text_for_llm and _has_body(text_for_llm):
        llm_input = text_for_llm
    elif normalized_text and _has_body(normalized_text):
        llm_input = normalized_text
    elif pg_db_name:
        training_table = await _resolve_training_table(chat_bot_id, pg_db_name)
        if training_table:
            raw_text = await fetch_chunks_text(
                training_table, pg_db_name, content_type, subject, content,
            )
            llm_input = _normalize_text(raw_text)

    if not llm_input or not _has_body(llm_input):
        return {"status": "skip", "item_id": item_id, "reason": "본문 부족"}

    result = await extract_keywords_llm(llm_input)
    keywords = result["keywords"]
    summary = result["summary"]

    if not keywords:
        return {"status": "skip", "item_id": item_id, "reason": "LLM 키워드 미추출"}

    await update_keywords(
        learn_table, maria_db_name, item_id, keywords, summary, db_type=db_type,
    )
    return {
        "status": "success",
        "item_id": item_id,
        "keywords_count": len(keywords),
        "summary_len": len(summary or ""),
    }


async def _consume_one(r) -> bool:
    """
    Redis에서 작업 1건을 BRPOP하여 처리.
    Returns: True(처리함), False(큐 비어있음/타임아웃)
    """
    result = await r.brpop(QUEUE_KEY, timeout=5)
    if result is None:
        return False

    _, raw = result
    job = json.loads(raw)
    job_desc = f"content_type={job.get('content_type')}, subject={job.get('subject', '')[:60]}"
    retry_count = job.get("retry_count", 0)

    if await _is_cancelled(r, job):
        logger.info(f"[worker] 삭제 대상 스킵 (learn_del): {job_desc}")
        return True

    try:
        res = await asyncio.wait_for(_process_job(job), timeout=30)
        logger.info(f"[worker] 처리 완료: {job_desc} → {res.get('status')}")
    except asyncio.TimeoutError:
        logger.warning(f"[worker] 처리 타임아웃: {job_desc}")
        await _retry_or_discard(r, job, retry_count, "timeout")
    except Exception as e:
        logger.error(f"[worker] 처리 실패: {job_desc}, error={e}")
        await _retry_or_discard(r, job, retry_count, str(e))

    return True


async def _retry_or_discard(r, job: dict, retry_count: int, reason: str):
    """실패한 작업을 재시도하거나 폐기."""
    if retry_count < MAX_RETRIES:
        job["retry_count"] = retry_count + 1
        delay = RETRY_DELAY_BASE ** (retry_count + 1)
        await asyncio.sleep(delay)
        await r.lpush(QUEUE_KEY, json.dumps(job, ensure_ascii=False))
        logger.info(
            f"[retry] 재시도 {job['retry_count']}/{MAX_RETRIES}, "
            f"reason={reason}, subject={job.get('subject', '')[:60]}"
        )
    else:
        logger.warning(
            f"[discard] 최대 재시도 초과, 폐기: "
            f"content_type={job.get('content_type')}, "
            f"subject={job.get('subject', '')[:60]}, last_error={reason}"
        )


async def keyword_worker():
    """
    Redis Queue에서 키워드/요약 추출 작업을 소비하는 백그라운드 워커.
    WORKER_CONCURRENCY만큼 동시 처리한다.
    """
    logger.info(f"[keyword_worker] 시작 (concurrency={WORKER_CONCURRENCY})")
    sem = asyncio.Semaphore(WORKER_CONCURRENCY)

    async def _worker_loop():
        r = await get_redis()
        while not _stop_event.is_set():
            try:
                async with sem:
                    processed = await _consume_one(r)
                    if not processed:
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[keyword_worker] 루프 오류: {e}")
                await asyncio.sleep(3)

    workers = [asyncio.create_task(_worker_loop()) for _ in range(WORKER_CONCURRENCY)]

    try:
        await asyncio.gather(*workers, return_exceptions=True)
    except asyncio.CancelledError:
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    logger.info("[keyword_worker] 종료")


def start_keyword_worker() -> asyncio.Task:
    """백그라운드 키워드 워커를 시작하고 Task를 반환."""
    global _worker_task
    _stop_event.clear()
    _worker_task = asyncio.create_task(keyword_worker())
    logger.info("[start_keyword_worker] 백그라운드 키워드 워커 시작됨")
    return _worker_task


async def stop_keyword_worker():
    """백그라운드 키워드 워커를 정상 종료."""
    global _worker_task
    _stop_event.set()
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await asyncio.wait_for(_worker_task, timeout=10)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    _worker_task = None
    logger.info("[stop_keyword_worker] 백그라운드 키워드 워커 종료됨")


async def get_queue_status() -> dict:
    """현재 큐 상태 조회 (모니터링용)."""
    try:
        r = await get_redis()
        queue_len = await r.llen(QUEUE_KEY)
        return {"queue_key": QUEUE_KEY, "pending_jobs": queue_len}
    except Exception as e:
        return {"queue_key": QUEUE_KEY, "error": str(e)}
