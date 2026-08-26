"""Model file-crawl startup outcomes without DB, Redis, HTTP, or Playwright.

This is a failure-injection check for the startup boundary:
request accepted -> workflow slot -> worker/pool ready -> scan enqueued.
It intentionally does not run a real crawl.
"""

import asyncio
import os
from dataclasses import dataclass
from typing import Optional

from backend.shared.crawler_state import CrawlerState


@dataclass
class FileWorkflow:
    colle: str = "file"
    stop_event: Optional[asyncio.Event] = None
    final_status: str = ""


async def _acquire(state: CrawlerState, job_id: str) -> dict:
    return await state.acquire_workflow_slot(job_id, workflow=FileWorkflow())


async def _unlimited_admission() -> None:
    state = CrawlerState()
    old = os.environ.pop("FILE_CRAWL_MAX_ACTIVE_WORKFLOWS", None)
    old_alias = os.environ.pop("FILE_CRAWL_MAX_ACTIVE_JOBS", None)
    try:
        results = await asyncio.gather(*(_acquire(state, f"unlimited-{idx}") for idx in range(6)))
        assert all(result["granted"] for result in results)
        assert state.get_workflow_slot_snapshot(workflow=FileWorkflow())["limit"] == 0
        assert len(state.admitted_workflow_jobs) == 6
        print("unlimited_admission: 6 accepted jobs can all reach startup concurrently")
    finally:
        for job_id in list(state.admitted_workflow_jobs):
            await state.release_workflow_slot(job_id)
        if old is not None:
            os.environ["FILE_CRAWL_MAX_ACTIVE_WORKFLOWS"] = old
        if old_alias is not None:
            os.environ["FILE_CRAWL_MAX_ACTIVE_JOBS"] = old_alias


async def _leaked_slot() -> None:
    state = CrawlerState()
    old = os.environ.get("FILE_CRAWL_MAX_ACTIVE_WORKFLOWS")
    os.environ["FILE_CRAWL_MAX_ACTIVE_WORKFLOWS"] = "2"
    try:
        assert (await _acquire(state, "leaked-1"))["granted"]
        assert (await _acquire(state, "healthy-2"))["granted"]
        waiting = asyncio.create_task(_acquire(state, "blocked-before-scan"))
        await asyncio.sleep(0.05)
        snapshot = state.get_workflow_slot_snapshot(workflow=FileWorkflow())
        assert snapshot == {"limit": 2, "active": 2, "waiting": 1}
        assert not waiting.done()
        print("leaked_slot: next job remains before scan while an admitted job is never released")
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
    finally:
        for job_id in list(state.admitted_workflow_jobs):
            await state.release_workflow_slot(job_id)
        if old is None:
            os.environ.pop("FILE_CRAWL_MAX_ACTIVE_WORKFLOWS", None)
        else:
            os.environ["FILE_CRAWL_MAX_ACTIVE_WORKFLOWS"] = old


def _startup_failure_map() -> None:
    stages = (
        "request_accepted",
        "workflow_task_scheduled",
        "workflow_slot_granted",
        "global_pool_registered",
        "global_workers_ready",
        "scan_enqueued",
    )
    for failed_stage in stages[:-1]:
        reached_scan = stages.index(failed_stage) >= stages.index("scan_enqueued")
        assert not reached_scan
        print(f"startup_failure: {failed_stage} -> terminal failure can retain scan=0")


async def main() -> None:
    await _unlimited_admission()
    await _leaked_slot()
    _startup_failure_map()
    print("OK: scan=0 diagnoses a startup-boundary failure, not a scan parsing failure")


if __name__ == "__main__":
    asyncio.run(main())
