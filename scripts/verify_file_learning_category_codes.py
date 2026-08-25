"""Guard LEARN_LIST file category columns against display-name writes."""

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db.mariadb_save_update import sanitize_file_learning_category_codes


def main() -> None:
    assert sanitize_file_learning_category_codes("AS1787203006", "AS1729062355") == (
        "AS1787203006",
        "AS1729062355",
    )
    assert sanitize_file_learning_category_codes("", "구정소식") == ("", "")
    assert sanitize_file_learning_category_codes("파일", "고시공고") == ("", "")
    assert sanitize_file_learning_category_codes("", "AS1729062355") == ("", "")
    persistence_source = (PROJECT_ROOT / "db" / "mariadb_save_update.py").read_text(encoding="utf-8")
    assert "_cc1, _cc2 = sanitize_file_learning_category_codes(" in persistence_source
    print("file LEARN_LIST category code-only guard: ok")


if __name__ == "__main__":
    main()
