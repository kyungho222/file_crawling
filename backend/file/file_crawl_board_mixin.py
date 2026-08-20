"""
게시판 상세 URL을 통해 첨부만 수집·학습할 때 BoardContentWorkflow와 조합하는 믹스인.

- Phase 1 (Producer / 탐색): 상세 URL 순회 + 고유 첨부 URL 선별·collection 큐 적재까지.
  종료 시 lock_file_exploration_scan_total() 로 scan_count 를 고정(이후 변경 없음).
- Phase 2 (Consumer / 소비): 다운로드·NAS·DB·학습은 큐 drain 이후 _finalize_stats 에서 대기.
- 학습 실패/성공 집계는 file_study_* (게시판 study_*와 분리)
- scan_count(잠금 전): 고유 상세(시작 시점 start_urls 기준 1회 카운트) + 고유 첨부 URL 수
  (= Post base + File 증분, 상세·첨부 이중 합산 없음)
- collection_count: MariaDB LEARN_LIST 신규 row INSERT가 성공한 문서 수
- save_count: MariaDB LEARN_LIST 신규 row INSERT 성공 시에만 증가
- study_count: 외부 임베딩 콜백에서 LEARN_LIST status=Y 반영이 성공한 수
- get_stats()는 SSE/UI 호환을 위해 study_*에 file_study_* 별칭을 실어 준다.

FileDownloadWorkflow: (BoardContentFilePipelineMixin, FileCrawlBoardMixin, BoardContentWorkflow)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("backend.file.file_crawl_board_mixin")

_StartUrls = Optional[List[Any]]


def unique_detail_url_count_from_start_urls(start_urls: _StartUrls) -> int:
    """수집 대상 고유 상세(URL) 개수 — 첨부·상세 이중 카운트 방지용 base."""
    try:
        from utils.url import canonicalize_url_for_dedup, ensure_url_scheme
    except Exception:
        return 0
    seen: set[str] = set()
    for item in start_urls or []:
        try:
            if isinstance(item, dict):
                raw = (item.get("url") or "").strip()
            else:
                raw = str(item or "").strip()
            if not raw:
                continue
            u = ensure_url_scheme(raw)
            k = (canonicalize_url_for_dedup(u) or u.strip() or "").strip()
            if k:
                seen.add(k)
        except Exception:
            continue
    return len(seen)

_FILE_STUDY_STAT_DEFAULTS: Dict[str, int] = {
    "file_study_count": 0,
    "file_study_done_count": 0,
    "file_study_success_count": 0,
    "file_study_failed_count": 0,
    "file_study_skipped_count": 0,
}


def ensure_file_study_stat_keys(workflow: Any) -> None:
    """BoardContentWorkflow.__init__ 이후 인스턴스에 file_study_* 키를 보장한다."""
    st = getattr(workflow, "stats", None)
    if not isinstance(st, dict):
        return
    for k, v in _FILE_STUDY_STAT_DEFAULTS.items():
        st.setdefault(k, v)


class FileCrawlBoardMixin:
    """BoardContentWorkflow 서브클래스에만 붙인다 (단독 사용 금지)."""

    def _reset_file_exploration_scan_lock(self) -> None:
        self._file_exploration_scan_locked = False
        self._file_exploration_scan_total_at_lock = 0
        self._file_scan_count_base_details = 0
        try:
            st = getattr(self, "stats", None)
            if isinstance(st, dict):
                st.pop("file_exploration_complete", None)
                st.pop("scan_count_locked", None)
                st.pop("scan_detail_leg", None)
                st.pop("scan_attachment_leg", None)
        except Exception:
            pass

    def init_file_scan_count_base_from_start_urls(self, start_urls: _StartUrls) -> None:
        """
        크롤 시작 시점: scan_count = 고유 상세(Post) 개수만 반영.
        이후 상세에서 발견한 고유 첨부(File)만 _sync_file_mode_scan_count 에서 가산.
        """
        base = unique_detail_url_count_from_start_urls(start_urls)
        self.set_file_scan_count_base_from_count(base)
        return

    def set_file_scan_count_base_from_count(self, base_count: int) -> None:
        base = max(0, int(base_count or 0))
        self._file_scan_count_base_details = int(base)
        if getattr(self, "_file_exploration_scan_locked", False):
            return
        self.stats["scan_count"] = base
        self.stats["total_count"] = base
        self.stats["scan_detail_leg"] = base
        self.stats["scan_attachment_leg"] = 0

    def _sync_file_mode_scan_count(self) -> None:
        """scan_count = 고유 상세(base, 시작 시점) + 고유 첨부(f). 탐색 잠금 후에는 고정값만 유지."""
        if getattr(self, "_file_exploration_scan_locked", False):
            try:
                self.stats["scan_count"] = int(
                    getattr(self, "_file_exploration_scan_total_at_lock", 0) or 0
                )
            except Exception:
                pass
            return
        try:
            base = int(getattr(self, "_file_scan_count_base_details", 0) or 0)
        except Exception:
            base = 0
        f = len(getattr(self, "_seen_file_urls", None) or set())
        total = base + f
        self.stats["scan_count"] = total
        try:
            self.stats["scan_detail_leg"] = base
            self.stats["scan_attachment_leg"] = f
        except Exception:
            pass

    def lock_file_exploration_scan_total(self) -> int:
        """탐색(Producer) 종료 시점: 전체 업무량(scan_count) 확정·잠금. Consumer 단계에서는 증가하지 않음."""
        try:
            base = int(getattr(self, "_file_scan_count_base_details", 0) or 0)
        except Exception:
            base = 0
        f = len(getattr(self, "_seen_file_urls", None) or set())
        total = base + f
        self._file_exploration_scan_total_at_lock = total
        self._file_exploration_scan_locked = True
        self.stats["scan_count"] = total
        self.stats["total_count"] = total
        self.stats["file_exploration_complete"] = True
        self.stats["scan_count_locked"] = True
        self.stats["scan_detail_leg"] = base
        self.stats["scan_attachment_leg"] = f
        logger.info(
            "[Phase][file] 탐색 완료·scan_count 잠금 | job_id=%s total=%s (Post base=%s + 고유 File=%s)",
            getattr(self, "job_id", ""),
            total,
            base,
            f,
        )
        return total

    def _file_crawl_finalize_scan_count(self) -> None:
        self._sync_file_mode_scan_count()

    def _sync_file_pipeline_counters_into_stats_dict(self) -> None:
        """Finalize/로그용: raw self.stats에 collection·study_*를 file 파이프라인 눈금에 맞춘다.
        호출부에서 _stats_lock을 잡은 상태에서 호출하는 것을 권장한다."""
        st = getattr(self, "stats", None)
        if not isinstance(st, dict):
            return
        # UI 선별은 MariaDB LEARN_LIST 신규 행 생성에 성공한 문서다.
        # 다운로드 큐 등록만으로는 카운트하지 않아 저장 카운트와 일치한다.
        try:
            selected = int(st.get("file_attachment_selection_confirmed_total", 0) or 0)
        except Exception:
            selected = 0
        st["collection_count"] = max(0, selected)
        for src, dst in (
            # 파일 모드 대표 학습 수는 "실제 성공 수"만 반영한다.
            ("file_study_success_count", "study_count"),
            ("file_study_done_count", "study_done_count"),
            ("file_study_success_count", "study_success_count"),
            ("file_study_failed_count", "study_failed_count"),
            ("file_study_skipped_count", "study_skipped_count"),
        ):
            try:
                st[dst] = int(st.get(src, 0) or 0)
            except Exception:
                st[dst] = 0

    def get_stats(self) -> Dict[str, Any]:
        # board.get_stats()는 board 모드의 상관관계를 적용할 수 있으므로, 파일 모드는
        # 선별과 저장은 같은 MariaDB 신규 row INSERT 경계에서 확정한다.
        try:
            confirmed_selection = max(
                0,
                int((self.stats or {}).get("file_attachment_selection_confirmed_total", 0) or 0),
            )
            self.stats["collection_count"] = confirmed_selection
        except Exception:
            confirmed_selection = 0
        out = super().get_stats()
        raw_save = int((self.stats or {}).get("save_count", 0) or 0)
        try:
            raw_save_succ = int((self.stats or {}).get("save_success_count", 0) or 0)
            out["collection_count"] = confirmed_selection
            out["save_count"] = raw_save
            out["save_success_count"] = min(raw_save_succ, raw_save)
        except Exception:
            pass

        # 파일 모드 UI/DB 메인 학습 수는 실제 성공 수만 사용한다.
        out["study_count"] = int(out.get("file_study_success_count", 0) or 0)
        out["study_done_count"] = int(out.get("file_study_done_count", 0) or 0)
        out["study_success_count"] = int(out.get("file_study_success_count", 0) or 0)
        out["study_failed_count"] = int(out.get("file_study_failed_count", 0) or 0)
        out["study_skipped_count"] = int(out.get("file_study_skipped_count", 0) or 0)
        fr = str(out.get("file_study_fail_reason") or "").strip()
        fu = str(out.get("file_study_fail_url") or "").strip()
        fd = str(out.get("file_study_fail_detail") or "").strip()
        if fr:
            out["study_fail_reason"] = fr
        if fu:
            out["study_fail_url"] = fu[:300]
        if fd:
            out["study_fail_detail"] = fd[:500]
        sr = str(out.get("file_study_skip_reason") or "").strip()
        su = str(out.get("file_study_skip_url") or "").strip()
        sd = str(out.get("file_study_skip_detail") or "").strip()
        if sr:
            out["study_skip_reason"] = sr
        if su:
            out["study_skip_url"] = su[:300]
        if sd:
            out["study_skip_detail"] = sd[:500]
        if "study_skip_samples" in self.stats:
            out["study_skip_samples"] = self.stats.get("study_skip_samples") or []
        try:
            out["study_count"] = min(int(out.get("study_count", 0) or 0), raw_save)
            out["study_done_count"] = min(int(out.get("study_done_count", 0) or 0), raw_save)
        except Exception:
            pass
        try:
            out["study_success_count"] = min(
                int(out.get("study_success_count", 0) or 0), int(out.get("study_count", 0) or 0)
            )
        except Exception:
            pass
        if getattr(self, "_file_exploration_scan_locked", False):
            out["file_exploration_complete"] = True
            out["scan_count_locked"] = True
            try:
                out["total_count"] = int(out.get("scan_count", 0) or 0)
            except Exception:
                pass
        for _k in ("scan_detail_leg", "scan_attachment_leg"):
            try:
                if _k in (self.stats or {}):
                    out[_k] = int((self.stats or {}).get(_k) or 0)
            except Exception:
                pass
        return out

    async def _record_study_skip(
        self,
        *,
        reason: str,
        **fields: Any,
    ) -> None:
        reason_safe = str(reason or "").strip() or "unknown"

        try:
            async with self._stats_lock:
                self.stats["event"] = "file_study_skipped"
                self.stats["message"] = reason_safe
                self.stats["file_study_skip_reason"] = reason_safe

                if reason_safe in {
                    "duplicate_reuse_learned",
                    "duplicate_learn_pipeline_skip",
                }:
                    self.stats["file_duplicate_reuse_learned_count"] = int(
                        self.stats.get(
                            "file_duplicate_reuse_learned_count",
                            0,
                        )
                        or 0
                    ) + 1

                if "url" in fields:
                    url_val = str(fields.get("url") or "")
                    self.stats["file_study_skip_url"] = (
                        url_val[:300] if url_val else ""
                    )

                if "detail" in fields:
                    detail_val = str(fields.get("detail") or "").strip()
                    if detail_val:
                        self.stats["file_study_skip_detail"] = detail_val[:500]

                try:
                    samples = list(
                        self.stats.get("study_skip_samples") or []
                    )

                    samples.append(
                        {
                            "reason": reason_safe[:80],
                            "url": str(fields.get("url") or "")[:200],
                            "learn_list_id": fields.get("learn_list_id"),
                            "status": str(
                                fields.get("status") or ""
                            )[:40],
                            "detail": str(
                                fields.get("detail") or ""
                            )[:200],
                        }
                    )

                    self.stats["study_skip_samples"] = samples[-20:]

                except Exception:
                    pass

        except Exception:
            pass

        try:
            safe: Dict[str, Any] = {}

            for k, v in (fields or {}).items():
                if isinstance(v, str) and len(v) > 300:
                    safe[k] = v[:300] + " ...(생략됨)"
                else:
                    safe[k] = v

            reason_text_map = {
                "duplicate_reuse_learned": "LEARN_LIST 중복",
                "duplicate_learn_pipeline_skip": "LEARN_LIST 중복",
                "unknown": "알 수 없음",
            }

            detail_text_map = {
                (
                    "existing LEARN_LIST row matched; "
                    "normal crawl does not modify or relearn "
                    "existing duplicate rows"
                ): "LEARN_LIST 중복",
            }

            reason_text = reason_text_map.get(
                reason_safe,
                reason_safe,
            )

            detail_value = str(safe.get("detail") or "").strip()
            detail_text = detail_text_map.get(
                detail_value,
                detail_value,
            )
            post_url = str(
                safe.get("post_url")
                or safe.get("source_page")
                or safe.get("source_url")
                or safe.get("board_url")
                or safe.get("page_url")
                or safe.get("url")
                or ""
            )

            logger.debug(
                "[파일 학습 건너뜀 상세] "
                "작업ID=%s | "
                "사유=%s | "
                "URL=%s | "
                "학습목록ID=%s | "
                "상태=%s | "
                "상세=%s",
                getattr(self, "job_id", ""),
                reason_text,
                post_url,
                safe.get("learn_list_id", ""),
                safe.get("status", ""),
                detail_text,
            )

        except Exception:
            pass

    async def _record_study_fail(
        self,
        *,
        reason: str,
        emit_fail_detail_log: bool = True,
        **fields: Any,
    ) -> None:
        reason_safe = str(reason or "").strip() or "unknown"
        try:
            async with self._stats_lock:
                self.stats["event"] = "file_study_failed"
                self.stats["message"] = reason_safe
                self.stats["file_study_fail_reason"] = reason_safe
                if "url" in fields:
                    url_val = str(fields.get("url") or "")
                    self.stats["file_study_fail_url"] = url_val[:300] if url_val else ""
                if "detail" in fields:
                    dv = str(fields.get("detail") or "").strip()
                    if dv:
                        self.stats["file_study_fail_detail"] = dv[:500]
                try:
                    samples = list(self.stats.get("study_issue_samples") or [])
                    samples.append(
                        {
                            "reason": reason_safe[:80],
                            "url": str(fields.get("url") or "")[:200],
                            "path": str(fields.get("path") or "")[:160],
                            "detail": str(fields.get("detail") or fields.get("err") or "")[:200],
                        }
                    )
                    self.stats["study_issue_samples"] = samples[-10:]
                except Exception:
                    pass
        except Exception:
            pass
        if emit_fail_detail_log:
            try:
                safe: Dict[str, Any] = {}
                for k, v in (fields or {}).items():
                    if isinstance(v, str) and len(v) > 300:
                        safe[k] = v[:300] + " ...(truncated)"
                    else:
                        safe[k] = v
                logger.warning(
                    "[FILE-STUDY-FAIL-DETAIL] job_id=%s reason=%s fields=%s",
                    getattr(self, "job_id", ""),
                    reason_safe,
                    safe,
                )
            except Exception:
                pass

    async def _bump_study_success_after_learning(self, url_key: str) -> None:
        try:
            async with self._stats_lock:
                if url_key not in self._counted_study_keys:
                    self._counted_study_keys.add(url_key)
                    self.stats["file_study_count"] = int(self.stats.get("file_study_count", 0)) + 1
                    self.stats["file_study_success_count"] = int(
                        self.stats.get("file_study_success_count", 0)
                    ) + 1
                    self.stats["file_study_done_count"] = int(self.stats.get("file_study_done_count", 0)) + 1
        except Exception:
            pass

    def _apply_study_outcome_counts(self, outcome_norm: str) -> None:
        # 파일 크롤은 학습 카운트를 "성공" 기준으로만 올린다.
        # (done/failed/skipped는 보조 지표로 유지)
        if outcome_norm == "success":
            self.stats["file_study_done_count"] = int(self.stats.get("file_study_done_count", 0) or 0) + 1
            self.stats["file_study_count"] = int(self.stats.get("file_study_count", 0) or 0) + 1
            self.stats["file_study_success_count"] = int(
                self.stats.get("file_study_success_count", 0) or 0
            ) + 1
        elif outcome_norm == "skipped":
            self.stats["file_study_done_count"] = int(self.stats.get("file_study_done_count", 0) or 0) + 1
            self.stats["file_study_skipped_count"] = int(
                self.stats.get("file_study_skipped_count", 0) or 0
            ) + 1
        else:
            self.stats["file_study_done_count"] = int(self.stats.get("file_study_done_count", 0) or 0) + 1
            self.stats["file_study_failed_count"] = int(
                self.stats.get("file_study_failed_count", 0) or 0
            ) + 1

    def _upgrade_study_outcome_counts(self, previous: str, new: str) -> None:
        if new != "success":
            return
        if previous == "failed":
            self.stats["file_study_failed_count"] = max(
                0,
                int(self.stats.get("file_study_failed_count", 0) or 0) - 1,
            )
        elif previous == "skipped":
            self.stats["file_study_skipped_count"] = max(
                0,
                int(self.stats.get("file_study_skipped_count", 0) or 0) - 1,
            )
        self.stats["file_study_count"] = int(self.stats.get("file_study_count", 0) or 0) + 1
        self.stats["file_study_success_count"] = int(self.stats.get("file_study_success_count", 0) or 0) + 1
        self._sync_file_pipeline_counters_into_stats_dict()

    def _reset_run_state(self) -> None:
        self._reset_file_exploration_scan_lock()
        try:
            self.stats.update(dict(_FILE_STUDY_STAT_DEFAULTS))
            for _k in (
                "study_issue_samples",
                "study_skip_samples",
                "file_study_fail_reason",
                "file_study_fail_url",
                "file_study_fail_detail",
                "file_study_skip_reason",
                "file_study_skip_url",
                "file_study_skip_detail",
            ):
                try:
                    self.stats.pop(_k, None)
                except Exception:
                    pass
            try:
                saved_ids = getattr(self, "_file_saved_learn_list_ids", None)
                if isinstance(saved_ids, set):
                    saved_ids.clear()
                else:
                    self._file_saved_learn_list_ids = set()
            except Exception:
                pass
            try:
                outcomes = getattr(self, "_counted_study_outcomes", None)
                if isinstance(outcomes, dict):
                    outcomes.clear()
                else:
                    self._counted_study_outcomes = {}
            except Exception:
                pass
            try:
                retry_inflight = getattr(self, "_file_learn_retry_inflight_keys", None)
                if isinstance(retry_inflight, set):
                    retry_inflight.clear()
                else:
                    self._file_learn_retry_inflight_keys = set()
            except Exception:
                pass
        except Exception:
            pass
        super()._reset_run_state()
