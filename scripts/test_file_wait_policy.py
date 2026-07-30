from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.file.file_wait_policy import resolve_file_fetch_delay


def main() -> None:
    assert resolve_file_fetch_delay({}, backend_default=3.0) == (
        3.0,
        "backend_default_missing_file_waiti",
    )
    assert resolve_file_fetch_delay({"file_waiti": "5"}, backend_default=3.0) == (
        5.0,
        "database_file_waiti",
    )
    assert resolve_file_fetch_delay({"file_waiti": "bad"}, backend_default=3.0) == (
        3.0,
        "backend_default_invalid_file_waiti",
    )
    assert resolve_file_fetch_delay({"file_waiti": "61"}, backend_default=3.0) == (
        3.0,
        "backend_default_invalid_file_waiti",
    )
    assert resolve_file_fetch_delay({"file_radio": "N", "file_waiti": "2"}, backend_default=3.0) == (
        2.0,
        "database_file_waiti",
    )
    print("file wait policy ok")


if __name__ == "__main__":
    main()
