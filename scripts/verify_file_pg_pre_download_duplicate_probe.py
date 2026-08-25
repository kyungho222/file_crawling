"""Static guard for the PG duplicate pre-download metadata probe."""

from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "backend" / "board" / "file_content_workflow.py"


def main() -> None:
    source = SOURCE.read_text(encoding="utf-8")
    required = (
        "self._file_pg_duplicate_name_exists(file_name)",
        "await self._probe_attachment_response_metadata_before_download(",
        '"pg_learned_match_pre_download_probe"',
        "phase=pre_download_probe",
    )
    missing = [text for text in required if text not in source]
    assert not missing, f"missing pre-download PG duplicate probe wiring: {missing}"
    print("file PG pre-download duplicate probe: ok")


if __name__ == "__main__":
    main()
