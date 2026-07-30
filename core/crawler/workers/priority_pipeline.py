"""
다운로드(collection_batch)와 학습(save_batch) 간 우선순위 대기.

- collection 쪽에 버퍼/큐에 작업이 있으면 항상 다운로드를 먼저 처리한다.
- 비어 있고 save_batch에만 쌓이면 유휴 다운로드 워커가 학습 배치를 가져간다
  (CRAWL_UNIVERSAL_PIPELINE_WORKERS=1 일 때 download_worker와 연동).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Literal, Tuple

from core.crawler.batch_queue import BatchQueue

logger = logging.getLogger(__name__)

Kind = Literal["download", "study"]


async def await_next_download_or_study_batch(
    collection_bq: BatchQueue,
    study_pull_bq: BatchQueue,
) -> Tuple[Kind, List[Dict[str, Any]]]:
    """
    다음 배치 1건을 반환한다. 다운로드(선별→저장) 작업이 있으면 항상 우선.
    둘 다 비어 있으면 두 큐 중 먼저 채워지는 쪽을 선택한다(동시 완료 시 다운로드 우선).
    """
    try:
        if not collection_bq.empty():
            return "download", await collection_bq.get()
        if not study_pull_bq.empty():
            return "study", await study_pull_bq.get()
    except Exception as ex:
        logger.debug("[priority_pipeline] empty() fast-path failed: %s", ex)

    t_dl = asyncio.create_task(collection_bq.get())
    t_st = asyncio.create_task(study_pull_bq.get())
    done, pending = await asyncio.wait(
        {t_dl, t_st},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for p in pending:
        p.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    if t_dl.done() and not t_dl.cancelled():
        try:
            return "download", t_dl.result()
        except Exception as ex:
            logger.warning("[priority_pipeline] download get failed: %s", ex)
    if t_st.done() and not t_st.cancelled():
        try:
            return "study", t_st.result()
        except Exception as ex:
            logger.warning("[priority_pipeline] study get failed: %s", ex)
    logger.error("[priority_pipeline] unexpected state; falling back to collection get()")
    return "download", await collection_bq.get()
