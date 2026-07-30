import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.board.board_content_workflow import BoardContentWorkflow, _DetailItem  # noqa: E402


async def main() -> None:
    # 실제 OpenAI 호출 없이(비활성) 디버그 로그 흐름만 확인하는 스크립트
    w = BoardContentWorkflow()
    w.job_id = "debug-local"
    w.enable_selector_learning = False
    await w._maybe_learn_selector_profiles([_DetailItem(url="https://example.com/a", board_url="")])


if __name__ == "__main__":
    asyncio.run(main())


