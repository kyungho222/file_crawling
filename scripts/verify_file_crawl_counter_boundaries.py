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
STAGE_LAB_ROUTER = ROOT / "backend" / "file" / "file_crawl_stage_lab_router.py"
STAGE_LAB_DASHBOARD = ROOT / "dashboard" / "file_crawl_stage_lab.html"


def _assert_before(source: str, earlier: str, later: str) -> None:
    earlier_index = source.index(earlier)
    later_index = source.index(later)
    assert earlier_index < later_index, f"expected {earlier!r} before {later!r}"


def main() -> None:
    workflow = FILE_WORKFLOW.read_text(encoding="utf-8")
    callback = BATCH_CALLBACK.read_text(encoding="utf-8")
    workflow_runner = WORKFLOW_RUNNER.read_text(encoding="utf-8")
    stage_lab_router = STAGE_LAB_ROUTER.read_text(encoding="utf-8")
    stage_lab_dashboard = STAGE_LAB_DASHBOARD.read_text(encoding="utf-8")

    # Queue admission is not selection. A document becomes selected only when
    # the same MariaDB INSERT boundary that increments save_count succeeds.
    assert "_confirm_file_selection_for_queue" not in workflow
    assert "[FileCounterTrace][selection_queued]" not in workflow
    insert_count_marker = (
        "if persistence_action == \"insert\":\n"
        "                            await self._confirm_file_selection_for_persist("
    )
    assert insert_count_marker in workflow

    # Save is counted only after a new MariaDB LEARN_LIST row is inserted.
    assert "[FilePersist][storage_saved_counted]" not in workflow
    insert_count_index = workflow.index(insert_count_marker)
    save_count_index = workflow.index(
        "await self._mark_save_done(url=save_key, ok=True)",
        insert_count_index,
    )
    assert insert_count_index < save_count_index
    assert save_count_index < workflow.index("[FilePersist][persist_result]", insert_count_index)
    assert "self.stats[\"save_count\"] = max(0, int(self.stats.get(\"save_count\", 0) or 0) - 1)" not in workflow

    # LEARN_LIST insert status=N is an earlier, distinct boundary from both
    # storage completion and the eventual learning callback.
    insert_n_marker = 'self.stats["file_learn_list_status_n_insert_count"]'
    assert insert_n_marker in workflow
    assert workflow.index(insert_n_marker) < workflow.index("storage_sync_after_persist")
    assert '"learn_list_inserted_count": stat_count("file_learn_list_status_n_insert_count")' in stage_lab_router
    for label in ("DB log update", "DB INSERT 완료(status=N)", "학습 완료"):
        assert label in stage_lab_dashboard

    # Final state reconciliation must not redefine selection as storage count.
    assert "adjusted[\"collection_count\"] = actual_saved" not in workflow_runner

    # A storage save and a database outcome are separate, traceable events.
    assert "async def _record_file_persist_outcome(" in workflow
    assert "[FilePersist][db_outcome]" in workflow
    assert "persistence_action=persistence_action" in workflow

    # The legacy all-fields queue-stall warning was too noisy for operations.
    assert "[파일크롤링추적][큐정체]" not in workflow

    # Pipeline transitions are operational logs: UTF-8 Korean labels and one
    # field per line make a stalled URL readable in journalctl.
    for label in (
        "[FilePipelineTrace][전환]",
        "작업ID=%s",
        "이전단계=%s",
        "현재단계=%s",
        "파일URL=%s",
        "게시물URL=%s",
        "파일명=%s",
        "상세=%s",
    ):
        assert label in workflow

    # Queue admission shows whether the attachment size was supplied by the
    # page metadata; an unknown size must not be displayed as a zero-byte file.
    assert "메타데이터 파일용량 미확인" in workflow
    assert 'f"메타데이터 파일용량={declared_size_bytes} bytes"' in workflow

    # Study progress is applied only after the callback's status-Y finalization.
    _assert_before(
        callback,
        "learning_ok = await finalize_learning_to_mariadb(",
        "await _sync_workflow_progress_after_callback(",
    )


if __name__ == "__main__":
    main()
    print("file crawl counter boundaries ok")
