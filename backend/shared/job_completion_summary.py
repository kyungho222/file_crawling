"""Stable, human-readable terminal summaries for crawl and dashboard jobs."""

from __future__ import annotations

from typing import Any, Dict, Iterable


_STATUS_LABELS = {
    "completed": "완료",
    "complete": "완료",
    "success": "완료",
    "cancelled": "중지",
    "stopped": "중지",
    "stop": "중지",
    "failed": "실패",
    "fail": "실패",
    "error": "오류",
}


def _count(stats: Dict[str, Any], keys: Iterable[str]) -> int:
    for key in keys:
        value = stats.get(key)
        if value is None or value == "":
            continue
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            continue
    return 0


def _has_any(stats: Dict[str, Any], keys: Iterable[str]) -> bool:
    return any(key in stats and stats.get(key) is not None for key in keys)


def build_job_completion_summary(
    stats: Dict[str, Any] | None,
    *,
    job_id: str,
    workflow_name: str,
    status: str,
    processing_count: int | None = None,
    pg_saved_count: int | None = None,
    pg_total_count: int | None = None,
    original_learn_list_id: int | str | None = None,
    followup_pending: bool = False,
) -> Dict[str, Any]:
    """Return one ordered terminal summary without changing existing payload fields."""
    values = dict(stats or {})
    lines = []

    url_duplicate_keys = ("url_duplicate_skipped_count", "duplicate_runtime_excluded_count")
    if _has_any(values, url_duplicate_keys):
        lines.append(f"URL 중복 제외: {_count(values, url_duplicate_keys)}건")

    simhash_duplicate_keys = ("simhash_duplicate_skip_count", "simhash_duplicate_skipped_count")
    if _has_any(values, simhash_duplicate_keys):
        lines.append(f"SimHash 중복 제외: {_count(values, simhash_duplicate_keys)}건")

    simhash_backfill_keys = ("simhash_backfill_count",)
    if _has_any(values, simhash_backfill_keys):
        lines.append(f"SimHash 백필: {_count(values, simhash_backfill_keys)}건")

    collection_success_keys = ("collection_success_count", "collection_count")
    collection_failed_keys = ("collection_failed_count", "request_failed")
    if _has_any(values, collection_success_keys + collection_failed_keys):
        lines.append(
            f"수집 성공 / 실패: {_count(values, collection_success_keys)}건 / {_count(values, collection_failed_keys)}건"
        )
    elif _has_any(values, ("updated", "update_failed")):
        lines.append(f"처리 성공 / 실패: {_count(values, ('updated',))}건 / {_count(values, ('update_failed',))}건")

    parse_success_keys = ("parse_success_count",)
    parse_failed_keys = ("parse_failed_count", "parse_failed")
    if _has_any(values, parse_success_keys + parse_failed_keys):
        lines.append(
            f"파싱 성공 / 실패: {_count(values, parse_success_keys)}건 / {_count(values, parse_failed_keys)}건"
        )

    save_success_keys = ("save_success_count", "save_count")
    save_failed_keys = ("save_failed_count",)
    if _has_any(values, save_success_keys + save_failed_keys):
        lines.append(
            f"저장 성공 / 실패: {_count(values, save_success_keys)}건 / {_count(values, save_failed_keys)}건"
        )

    study_keys = ("study_success_count", "study_count")
    if _has_any(values, study_keys):
        lines.append(f"학습 완료: {_count(values, study_keys)}건")

    terminal = (
        f"이벤트=워커 작업 완료 job_id={job_id} 워크플로우={workflow_name} "
        f"상태={_STATUS_LABELS.get(str(status or '').strip().lower(), status or '-')}"
    )
    if processing_count is not None:
        terminal += f" 처리={max(0, int(processing_count))}건"
    if pg_saved_count is not None or pg_total_count is not None:
        terminal += f" PG저장={max(0, int(pg_saved_count or 0))}/{max(0, int(pg_total_count or 0))}건"
    if original_learn_list_id not in (None, ""):
        terminal += f" 원본learn_list_id={original_learn_list_id}"
    if followup_pending:
        terminal += " 후속처리=임베딩callback/요약큐는 API컨테이너에서 계속진행"
    lines.append(terminal)

    return {"lines": lines, "text": "\n".join(lines)}
