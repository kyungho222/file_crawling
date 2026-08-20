"""Regression checks for file-crawl progress counter boundaries.

These checks intentionally inspect the orchestration source.  The workflow is
too infrastructure-heavy to construct in a standalone test, while the order
of these calls is the public progress contract.
"""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FILE_WORKFLOW = ROOT / "backend" / "board" / "file_content_workflow.py"
BATCH_CALLBACK = ROOT / "backend" / "shared" / "batch_embedding_scheduler.py"
WORKFLOW_RUNNER = ROOT / "backend" / "shared" / "workflow_runner.py"


def _assert_before(source: str, earlier: str, later: str) -> None:
    earlier_index = source.index(earlier)
    later_index = source.index(later)
    assert earlier_index < later_index, f"expected {earlier!r} before {later!r}"


def main() -> None:
    workflow = FILE_WORKFLOW.read_text(encoding="utf-8")
    callback = BATCH_CALLBACK.read_text(encoding="utf-8")
    workflow_runner = WORKFLOW_RUNNER.read_text(encoding="utf-8")

    # A document becomes selected only once its download work has been queued.
    _assert_before(
        workflow,
        "await _put_collection_queue_with_trace(file_meta, file_url=file_url, file_name=file_name)\n                    await self._confirm_file_selection_for_queue(",
        "stage=\"download_enqueue\",\n                        status=\"queued\"",
    )

    # Save is the verified storage boundary, not the later LEARN_LIST write.
    _assert_before(
        workflow,
        "[FilePersist][storage_saved_counted]",
        "[FilePersist][persist_begin]",
    )
    assert "await self._mark_save_done(url=save_key, ok=True)\n                        logger.info(\n                            \"[FilePersist][persist_result]" not in workflow
    assert "self.stats[\"save_count\"] = max(0, int(self.stats.get(\"save_count\", 0) or 0) - 1)" not in workflow

    # Final state reconciliation must not redefine selection as storage count.
    assert "adjusted[\"collection_count\"] = actual_saved" not in workflow_runner

    # A storage save and a database outcome are separate, traceable events.
    assert "async def _record_file_persist_outcome(" in workflow
    assert "[FilePersist][db_outcome]" in workflow
    assert "persistence_action=persistence_action" in workflow

    # Study progress is applied only after the callback's status-Y finalization.
    _assert_before(
        callback,
        "learning_ok = await finalize_learning_to_mariadb(",
        "await _sync_workflow_progress_after_callback(",
    )


if __name__ == "__main__":
    main()
    print("file crawl counter boundaries ok")
