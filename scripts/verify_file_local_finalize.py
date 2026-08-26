"""Contract checks for file-crawl local finalization."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "core" / "crawler" / "workers" / "download_storage.py"
sys.path.insert(0, str(ROOT))
SPEC = importlib.util.spec_from_file_location("download_storage_contract", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
finalize_download_temp_file = MODULE.finalize_download_temp_file


async def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        temp_path = os.path.join(directory, "sample.pdf.part-test")
        final_path = os.path.join(directory, "sample.pdf")
        with open(temp_path, "wb") as handle:
            handle.write(b"%PDF-1.4\nminimal fixture\n")

        size = await finalize_download_temp_file(
            temp_path=temp_path,
            final_path=final_path,
            filename="sample.pdf",
            url="https://example.test/sample.pdf",
            content_type="application/pdf",
            expected_size=os.path.getsize(temp_path),
        )
        assert size > 0
        assert not os.path.exists(temp_path)
        assert os.path.exists(final_path)

        bad_temp_path = os.path.join(directory, "error.pdf.part-test")
        bad_final_path = os.path.join(directory, "error.pdf")
        with open(bad_temp_path, "wb") as handle:
            handle.write(b"<html>error</html>")

        try:
            await finalize_download_temp_file(
                temp_path=bad_temp_path,
                final_path=bad_final_path,
                filename="error.pdf",
                url="https://example.test/error.pdf",
                content_type="text/html",
            )
        except RuntimeError as exc:
            assert "HTML" in str(exc)
        else:
            raise AssertionError("HTML payload must fail finalization")
        assert not os.path.exists(bad_temp_path)
        assert not os.path.exists(bad_final_path)


if __name__ == "__main__":
    asyncio.run(main())
    print("file local finalize contract: ok")
