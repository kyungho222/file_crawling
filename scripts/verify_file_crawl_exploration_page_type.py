import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.shared.exploration_query import ExplorationQuerySpec, build_exploration_conditions
from backend.shared.file_crawl_post_urls import _FILE_CRAWL_EXPLORATION_RECORD_TYPE


def main() -> None:
    assert _FILE_CRAWL_EXPLORATION_RECORD_TYPE == "post"
    post_conditions = build_exploration_conditions(
        ExplorationQuerySpec(
            chat_bot_id="test-bot",
            record_type=_FILE_CRAWL_EXPLORATION_RECORD_TYPE,
        )
    )
    assert "`type` = 'post'" in post_conditions.condition
    assert "`type` = 'page'" not in post_conditions.condition

    default_conditions = build_exploration_conditions(ExplorationQuerySpec())
    assert "`type` = 'post'" in default_conditions.condition


if __name__ == "__main__":
    main()
    print("file crawl exploration post type policy ok")
