import asyncio
import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.shared.crawl_monitor import _AUTO_STOP_CONFIG_CACHE, _load_auto_stop_config


class Workflow:
    unique_id = "workflow-unique-id"


class NextWorkflow:
    unique_id = "next-workflow-unique-id"


async def main() -> None:
    _AUTO_STOP_CONFIG_CACHE.clear()
    calls = []

    async def fetcher(*, candidate_ids, keys, dbname):
        calls.append((candidate_ids, keys, dbname))
        return [
            {"chat_bot_id": "bot-1", "key": "page_count", "value": "100"},
            {"chat_bot_id": "default", "key": "week_count", "value": "200"},
            {"chat_bot_id": "default", "key": "stop_count", "value": "300"},
        ]

    keys = ["week_count", "page_count", "stop_count"]
    conf, source = await _load_auto_stop_config(
        fetcher=fetcher,
        workflow=Workflow(),
        chat_bot_id="bot-1",
        db_name="example",
        job_id="job-1",
        keys=keys,
        fetch_timeout_sec=0.05,
    )
    assert source == "chat_bot_id"
    assert conf == {"week_count": "200", "page_count": "100", "stop_count": "300"}
    assert len(calls) == 1
    assert calls[0][0] == ["bot-1", "workflow-unique-id", "1", None]

    cached_conf, cached_source = await _load_auto_stop_config(
        fetcher=fetcher,
        workflow=Workflow(),
        chat_bot_id="bot-1",
        db_name="example",
        job_id="job-2",
        keys=keys,
        fetch_timeout_sec=0.05,
    )
    assert cached_conf == conf
    assert cached_source == source
    assert len(calls) == 1

    _AUTO_STOP_CONFIG_CACHE.clear()
    empty_calls = []

    async def empty_fetcher(**_kwargs):
        empty_calls.append(1)
        return []

    defaults_conf, defaults_source = await _load_auto_stop_config(
        fetcher=empty_fetcher,
        workflow=Workflow(),
        chat_bot_id="bot-defaults",
        db_name="example",
        job_id="job-defaults-1",
        keys=keys,
        fetch_timeout_sec=0.05,
    )
    assert defaults_conf == {}
    assert defaults_source == "defaults"
    cached_defaults_conf, cached_defaults_source = await _load_auto_stop_config(
        fetcher=empty_fetcher,
        workflow=NextWorkflow(),
        chat_bot_id="bot-defaults",
        db_name="example",
        job_id="job-defaults-2",
        keys=keys,
        fetch_timeout_sec=0.05,
    )
    assert cached_defaults_conf == {}
    assert cached_defaults_source == "defaults"
    assert len(empty_calls) == 1

    _AUTO_STOP_CONFIG_CACHE.clear()
    timeout_cancelled = False

    async def slow_fetcher(**_kwargs):
        nonlocal timeout_cancelled
        try:
            await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            timeout_cancelled = True
            raise
        return []

    started = asyncio.get_running_loop().time()
    timeout_conf, timeout_source = await _load_auto_stop_config(
        fetcher=slow_fetcher,
        workflow=Workflow(),
        chat_bot_id="bot-timeout",
        db_name="example",
        job_id="job-timeout",
        keys=keys,
        fetch_timeout_sec=0.1,
    )
    elapsed = asyncio.get_running_loop().time() - started
    assert timeout_conf == {}
    assert timeout_source == "defaults"
    assert timeout_cancelled
    assert elapsed < 0.2


if __name__ == "__main__":
    asyncio.run(main())
    print("auto-stop config resolution policy ok")
