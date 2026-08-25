import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.shared.exploration_query import ExplorationQuerySpec, build_exploration_conditions


def main() -> None:
    page_conditions = build_exploration_conditions(
        ExplorationQuerySpec(chat_bot_id="test-bot", record_type="page")
    )
    assert "`type` = 'page'" in page_conditions.condition
    assert "`type` = 'post'" not in page_conditions.condition

    default_conditions = build_exploration_conditions(ExplorationQuerySpec())
    assert "`type` = 'post'" in default_conditions.condition


if __name__ == "__main__":
    main()
    print("file crawl exploration page type policy ok")
