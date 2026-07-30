from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi import BackgroundTasks  # noqa: E402

from backend.shared import crawl_start as crawl_start_module  # noqa: E402
from backend.shared.workflow_dispatch_assembly import _create_workflow_for_mode  # noqa: E402


class FakeRequest:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload
        self.cookies: Dict[str, str] = {}

    async def json(self) -> Dict[str, Any]:
        return dict(self._payload)


class FakeRedis:
    async def delete(self, *_args: Any, **_kwargs: Any) -> int:
        return 1


async def main() -> int:
    os.environ["CRAWL_START_BURST_DEDUPE"] = "0"
    captured_worker_payload: Dict[str, Any] = {}
    worker_seen = asyncio.Event()

    async def fake_get_redis() -> FakeRedis:
        return FakeRedis()

    async def fake_cache_job_metadata(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def fake_update_state_only(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def fake_send_message_to_redis_sse(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def fake_crawl_file_worker(data: Dict[str, Any], _background_tasks: BackgroundTasks) -> None:
        captured_worker_payload.update(dict(data))
        worker_seen.set()

    async def fake_load_file_crawl_post_url_strings(**_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "url": "https://www.guro.go.kr/www/selectBbsNttView.do?bbsNo=846&nttNo=209819&pageIndex=1",
                "type": "cate_match|NOTICE|BUILDING",
                "title": "샘플 상세 게시글",
                "reg_date": "2024-10-22",
                "author": "건축과",
            }
        ]

    original = {
        "get_redis": crawl_start_module.get_redis,
        "cache_job_metadata": crawl_start_module.cache_job_metadata,
        "update_state_only": crawl_start_module.update_state_only,
        "send_message_to_redis_sse": crawl_start_module.send_message_to_redis_sse,
        "_crawl_file_worker": crawl_start_module._crawl_file_worker,
        "load_file_crawl_post_url_strings": crawl_start_module.load_file_crawl_post_url_strings,
    }
    crawl_start_module.get_redis = fake_get_redis
    crawl_start_module.cache_job_metadata = fake_cache_job_metadata
    crawl_start_module.update_state_only = fake_update_state_only
    crawl_start_module.send_message_to_redis_sse = fake_send_message_to_redis_sse
    crawl_start_module._crawl_file_worker = fake_crawl_file_worker
    crawl_start_module.load_file_crawl_post_url_strings = fake_load_file_crawl_post_url_strings

    try:
        payload = {
            "job_id": "sample-session-file-branch",
            "db_name": "dev_user",
            "chat_bot_id": "sample-chat-bot",
            "colle": "file",
            "content_type": "url",
            "crawl_mode": "crawling",
            "method": "period",
            "contents": ["https://www.guro.go.kr/www/selectBbsNttList.do?bbsNo=846&key=1871"],
            "target_domains": ["guro.go.kr"],
            "target_date": ["2024-01-01", "2024-12-31"],
        }

        response = await crawl_start_module.crawl_start(FakeRequest(payload), BackgroundTasks())
        await asyncio.wait_for(worker_seen.wait(), timeout=2.0)

        prepared_payload = dict(payload)
        await crawl_start_module._prepare_crawl(prepared_payload)

        workflow = _create_workflow_for_mode(
            data=prepared_payload,
            start_urls=prepared_payload.get("start_urls_override") or [],
            job_id=str(prepared_payload.get("job_id") or ""),
            colle_mode=str(prepared_payload.get("colle") or ""),
        )

        result = {
            "endpoint_status_code": response.status_code,
            "endpoint_response": json.loads(response.body.decode("utf-8")),
            "worker_colle": captured_worker_payload.get("colle"),
            "worker_ui_colle": captured_worker_payload.get("ui_colle"),
            "worker_colle_mode": captured_worker_payload.get("colle_mode"),
            "worker_file_crawl_mode": captured_worker_payload.get("_file_crawl_mode"),
            "worker_content_type": captured_worker_payload.get("content_type"),
            "prepare_start_urls_source": prepared_payload.get("start_urls_override_source"),
            "prepare_start_urls_count": len(prepared_payload.get("start_urls_override") or []),
            "prepare_start_urls_sample": prepared_payload.get("start_urls_override")[:1],
            "workflow_class": type(workflow).__name__,
            "workflow_colle": getattr(workflow, "colle", None),
            "workflow_colle_mode": getattr(workflow, "colle_mode", None),
            "workflow_file_mode": getattr(workflow, "file_mode", None),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

        ok = (
            response.status_code == 200
            and captured_worker_payload.get("colle") == "file"
            and captured_worker_payload.get("ui_colle") == "file"
            and captured_worker_payload.get("colle_mode") == "file"
            and captured_worker_payload.get("_file_crawl_mode") is True
            and prepared_payload.get("start_urls_override_source") == "file_crawl_post_db"
            and len(prepared_payload.get("start_urls_override") or []) == 1
            and type(workflow).__name__ == "FileDownloadWorkflow"
            and getattr(workflow, "file_mode", None) is True
        )
        return 0 if ok else 1
    finally:
        for name, value in original.items():
            setattr(crawl_start_module, name, value)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
