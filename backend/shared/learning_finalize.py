from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional, Any

from backend.shared.learning_service import LearningService

logger = logging.getLogger(__name__)


def _resolve_pg_wait_timeout(actual_chunks: int, explicit: Optional[float]) -> float:
    """
    PG 청크 카운트 반영이 늦을 때 status=Y가 빠지는 것을 줄이기 위해 대기 상한을 확장한다.
    - explicit 이 있으면 그 값을 상한(LEARN_PG_WAIT_MAX_SEC) 내에서만 사용.
    - 없으면 LEARN_PG_WAIT_TIMEOUT_SEC + 청크당 보너스(상한 LEARN_PG_WAIT_CHUNK_BONUS_MAX_SEC).
    """
    try:
        cap = float(os.getenv("LEARN_PG_WAIT_MAX_SEC", "7200") or "7200")
    except Exception:
        cap = 7200.0
    cap = max(60.0, min(cap, 14_400.0))

    if explicit is not None:
        try:
            t = float(explicit)
        except Exception:
            t = 300.0
        return max(30.0, min(t, cap))

    try:
        base = float(os.getenv("LEARN_PG_WAIT_TIMEOUT_SEC", "300") or "300")
    except Exception:
        base = 300.0
    try:
        per_ch = float(os.getenv("LEARN_PG_WAIT_PER_CHUNK_SEC", "3") or "3")
    except Exception:
        per_ch = 3.0
    try:
        bonus_cap = float(os.getenv("LEARN_PG_WAIT_CHUNK_BONUS_MAX_SEC", "3600") or "3600")
    except Exception:
        bonus_cap = 3600.0
    bonus_cap = max(0.0, min(bonus_cap, cap))
    ch = max(0, int(actual_chunks or 0))
    bonus = min(per_ch * float(ch), bonus_cap)
    out = base + bonus
    return max(30.0, min(out, cap))


async def finalize_learning_to_mariadb(
    *,
    chat_bot_id: str,
    db_name: str,
    learn_list_id: str,
    display_name: str,
    actual_chunks: int,
    pg_content_value: Optional[str],
    learning_service: Optional[LearningService] = None,
    pg_wait_timeout_seconds: Optional[float] = None,
    post_reg_date: Optional[Any] = None,
    preserve_created_at: bool = False,
    job_id_for_count: Optional[str] = None,
    crawling_log_id: Optional[int] = None,
    increment_study_count_on_success: bool = True,
    summarize_after_status_y: bool = True,
    summarize_normalized_text: Optional[str] = None,
) -> bool:
    """
    게시판/파일 공용: 학습 완료 후 MariaDB 반영 단계를 일원화한다.

    - PG에 청크 저장(edu/learn)이 끝난 뒤 호출한다.
    - 내부에서 PG 청크 존재 여부를 확인하고,
      TRAINING_PROCESS 기록 + LEARN_LIST status='Y' 업데이트까지 수행한다.
    - PG 반영이 지연되면 trigger_learning 이 한 번에 실패할 수 있어,
      actual_chunks>0 인 경우 LEARN_FINALIZE_MAX_ATTEMPTS 만큼 재시도한다.
    """
    ls = learning_service or LearningService(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        progress_callback=None,
    )

    chunks_i = int(actual_chunks or 0)
    wait_sec = _resolve_pg_wait_timeout(chunks_i, pg_wait_timeout_seconds)

    try:
        max_att = int(os.getenv("LEARN_FINALIZE_MAX_ATTEMPTS", "4") or "4")
    except Exception:
        max_att = 4
    max_att = max(1, min(max_att, 30))

    try:
        pause = float(os.getenv("LEARN_FINALIZE_RETRY_SLEEP_SEC", "5") or "5")
    except Exception:
        pause = 5.0
    pause = max(0.5, min(pause, 120.0))

    last = False
    for attempt in range(max_att):
        attempt_started = time.perf_counter()
        logger.debug(
            "[LearningTrace][finalize.start] db=%s chat_bot_id=%s learn_list_id=%s attempt=%s/%s chunks=%s wait_pg=%s display_name=%s",
            db_name,
            chat_bot_id,
            learn_list_id,
            attempt + 1,
            max_att,
            chunks_i,
            int(wait_sec),
            str(display_name or "")[:120],
        )
        try:
            last = bool(
                await ls.trigger_learning(
                    db_id=str(learn_list_id),
                    filename=str(display_name or ""),
                    stats=None,
                    actual_chunks=chunks_i,
                    pg_content_value=pg_content_value,
                    pg_wait_timeout_seconds=wait_sec,
                    post_reg_date=post_reg_date,
                    preserve_created_at=preserve_created_at,
                    summarize_after_status_y=summarize_after_status_y,
                    summarize_normalized_text=summarize_normalized_text,
                )
            )
        except Exception as exc:
            logger.error(
                "[LearningError][finalize.error] db=%s chat_bot_id=%s learn_list_id=%s attempt=%s/%s elapsed_ms=%s err=%s",
                db_name,
                chat_bot_id,
                learn_list_id,
                attempt + 1,
                max_att,
                int((time.perf_counter() - attempt_started) * 1000),
                exc,
                exc_info=True,
            )
            raise
        logger.debug(
            "[LearningTrace][finalize.done] db=%s chat_bot_id=%s learn_list_id=%s attempt=%s/%s ok=%s elapsed_ms=%s",
            db_name,
            chat_bot_id,
            learn_list_id,
            attempt + 1,
            max_att,
            last,
            int((time.perf_counter() - attempt_started) * 1000),
        )
        if last:
            if job_id_for_count and increment_study_count_on_success:
                try:
                    from db.crawl_db_manager import increment_crawling_log_study

                    incremented = await increment_crawling_log_study(
                        job_id_for_count,
                        dbname=db_name,
                        log_id=crawling_log_id,
                        amount=1,
                    )
                    if incremented:
                        logger.debug(
                            "[LearningTrace][finalize.study_count_incremented] db=%s job_id=%s learn_list_id=%s log_id=%s",
                            db_name,
                            job_id_for_count,
                            learn_list_id,
                            crawling_log_id,
                        )
                    else:
                        logger.debug(
                            "[LearningTrace][finalize.study_count_increment_skipped] db=%s job_id=%s learn_list_id=%s log_id=%s reason=no_crawling_log_row",
                            db_name,
                            job_id_for_count,
                            learn_list_id,
                            crawling_log_id,
                        )
                except Exception as exc:
                    logger.error(
                        "[LearningError][finalize.study_count_increment_failed] db=%s job_id=%s learn_list_id=%s log_id=%s err=%s",
                        db_name,
                        job_id_for_count,
                        learn_list_id,
                        crawling_log_id,
                        exc,
                    )
            return True
        if getattr(ls, "last_missing_learn_list_record", False):
            logger.error(
                "[LearningError][finalize.skip_retry] db=%s chat_bot_id=%s learn_list_id=%s reason=learn_list_row_missing",
                db_name,
                chat_bot_id,
                learn_list_id,
            )
            return False
        if chunks_i <= 0:
            logger.error(
                "[LearningError][finalize.skip_retry] db=%s chat_bot_id=%s learn_list_id=%s reason=no_chunks",
                db_name,
                chat_bot_id,
                learn_list_id,
            )
            return False
        if attempt + 1 < max_att:
            logger.debug(
                "[LearningTrace][finalize.retry_wait] db=%s chat_bot_id=%s learn_list_id=%s next_attempt=%s/%s sleep_sec=%s",
                db_name,
                chat_bot_id,
                learn_list_id,
                attempt + 2,
                max_att,
                pause,
            )
            await asyncio.sleep(pause)
    return last
