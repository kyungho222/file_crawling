"""Stage contract for the attachment file crawl pipeline.

This module is intentionally declarative.  It names the current pipeline
boundaries before the large workflow methods are split into smaller services.
Runtime code can import these names for logging, trace records, and tests
without changing crawler behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple


class FileCrawlStage(str, Enum):
    DETAIL_URL_PREPARE = "detail_url_prepare"
    CATEGORY_RESOLVE = "category_resolve"
    DETAIL_HTML_FETCH = "detail_html_fetch"
    POST_META_EXTRACT = "post_meta_extract"
    ATTACHMENT_CANDIDATE_COLLECT = "attachment_candidate_collect"
    DOWNLOAD_CANDIDATE_SELECT = "download_candidate_select"
    FILE_PAYLOAD_BUILD = "file_payload_build"
    DOWNLOAD_SAVE = "download_save"
    SAVE_EVENT_HANDLE = "save_event_handle"
    LEARN_LIST_ROW_ENSURE = "learn_list_row_ensure"
    LEARN_LIST_PERSIST = "learn_list_persist"


@dataclass(frozen=True)
class FileCrawlStageBoundary:
    stage: FileCrawlStage
    owner: str
    function_refs: Tuple[str, ...]
    inputs: Tuple[str, ...]
    outputs: Tuple[str, ...]
    preserved_fields: Tuple[str, ...]


FILE_CRAWL_STAGE_BOUNDARIES: Tuple[FileCrawlStageBoundary, ...] = (
    FileCrawlStageBoundary(
        stage=FileCrawlStage.DETAIL_URL_PREPARE,
        owner="backend.file.file_download_workflow.FileDownloadWorkflow",
        function_refs=("_process_one_detail",),
        inputs=("raw detail item",),
        outputs=("detail_url",),
        preserved_fields=("post_url",),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.CATEGORY_RESOLVE,
        owner="backend.file.file_download_workflow.FileDownloadWorkflow",
        function_refs=("_process_one_detail direct category block", "_ensure_file_learning_category_mapping"),
        inputs=("detail_url", "direct cate1/cate2 from board page or source item"),
        outputs=("board-derived cate1/cate2 for file-root mapping"),
        preserved_fields=("cate1", "cate2"),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.DETAIL_HTML_FETCH,
        owner="backend.file.file_download_workflow.FileDownloadWorkflow",
        function_refs=("_fetch_detail_html_for_selection",),
        inputs=("detail_url",),
        outputs=("html", "fetch_meta"),
        preserved_fields=("requested_url", "final_url"),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.POST_META_EXTRACT,
        owner="backend.file.file_download_workflow.FileDownloadWorkflow",
        function_refs=(
            "_extract_board_title",
            "_extract_board_reg_date",
            "_extract_file_author_info",
        ),
        inputs=("html", "detail_url"),
        outputs=("post_title", "reg_date", "author", "department"),
        preserved_fields=("post_title", "reg_date", "author"),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.ATTACHMENT_CANDIDATE_COLLECT,
        owner="backend.file.file_download_workflow.FileDownloadWorkflow",
        function_refs=("_extract_attachment_links_generic",),
        inputs=("html", "detail_url"),
        outputs=("attachment_candidates",),
        preserved_fields=(
            "file_url",
            "attachment_name",
            "display_name",
            "candidate_score",
            "candidate_reason",
        ),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.DOWNLOAD_CANDIDATE_SELECT,
        owner="backend.board.file_content_workflow.BoardContentFilePipelineMixin",
        function_refs=("_enqueue_file_downloads",),
        inputs=("post_url", "attachments", "detail_cates", "post_meta"),
        outputs=("selected attachment candidates",),
        preserved_fields=("post_url", "attachment_name", "cate1", "cate2", "post_title"),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.FILE_PAYLOAD_BUILD,
        owner="backend.board.file_content_workflow.BoardContentFilePipelineMixin",
        function_refs=("_build_file_meta",),
        inputs=("attachment", "post_url", "cate1", "cate2", "post_meta"),
        outputs=("file_meta",),
        preserved_fields=(
            "original_meta.attachment_name",
            "original_meta.store_cate1",
            "original_meta.store_cate2",
            "post_url",
            "file_url",
        ),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.DOWNLOAD_SAVE,
        owner="core.crawler.workers.download",
        function_refs=("download_worker",),
        inputs=("file_meta",),
        outputs=("file_saved event",),
        preserved_fields=(
            "attachment_name",
            "saved_filename",
            "storage_filename",
            "original_meta",
        ),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.SAVE_EVENT_HANDLE,
        owner="backend.board.file_content_workflow.BoardContentFilePipelineMixin",
        function_refs=("_run_file_progress_loop", "_file_run_saved_file_learn_after_save"),
        inputs=("file_saved event",),
        outputs=("learn_list file_info",),
        preserved_fields=("original_meta", "attachment_name", "cate1", "cate2"),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.LEARN_LIST_ROW_ENSURE,
        owner="backend.board.file_content_workflow.BoardContentFilePipelineMixin",
        function_refs=("_ensure_learn_list_row_for_file_save",),
        inputs=("saved file_info",),
        outputs=("insert_into_learn_list call",),
        preserved_fields=("subject candidates", "category candidates", "content URL"),
    ),
    FileCrawlStageBoundary(
        stage=FileCrawlStage.LEARN_LIST_PERSIST,
        owner="db.mariadb_save_update",
        function_refs=("insert_into_learn_list",),
        inputs=("file_info",),
        outputs=("LEARN_LIST insert/update",),
        preserved_fields=(
            "subject from _resolve_file_learning_subject",
            "cate from coalesce_learn_list_cates",
        ),
    ),
)


def file_crawl_stage_order() -> Tuple[FileCrawlStage, ...]:
    return tuple(boundary.stage for boundary in FILE_CRAWL_STAGE_BOUNDARIES)


def get_file_crawl_stage_boundary(stage: FileCrawlStage | str) -> FileCrawlStageBoundary:
    normalized = FileCrawlStage(stage)
    for boundary in FILE_CRAWL_STAGE_BOUNDARIES:
        if boundary.stage == normalized:
            return boundary
    raise KeyError(str(stage))


__all__ = [
    "FILE_CRAWL_STAGE_BOUNDARIES",
    "FileCrawlStage",
    "FileCrawlStageBoundary",
    "file_crawl_stage_order",
    "get_file_crawl_stage_boundary",
]
