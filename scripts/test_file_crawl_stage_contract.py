import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.file.file_crawl_stage_contract import (
    FILE_CRAWL_STAGE_BOUNDARIES,
    FileCrawlStage,
    file_crawl_stage_order,
    get_file_crawl_stage_boundary,
)


def main() -> None:
    expected = (
        FileCrawlStage.DETAIL_URL_PREPARE,
        FileCrawlStage.CATEGORY_RESOLVE,
        FileCrawlStage.DETAIL_HTML_FETCH,
        FileCrawlStage.POST_META_EXTRACT,
        FileCrawlStage.ATTACHMENT_CANDIDATE_COLLECT,
        FileCrawlStage.DOWNLOAD_CANDIDATE_SELECT,
        FileCrawlStage.FILE_PAYLOAD_BUILD,
        FileCrawlStage.DOWNLOAD_SAVE,
        FileCrawlStage.SAVE_EVENT_HANDLE,
        FileCrawlStage.LEARN_LIST_ROW_ENSURE,
        FileCrawlStage.LEARN_LIST_PERSIST,
    )
    assert file_crawl_stage_order() == expected
    assert len(FILE_CRAWL_STAGE_BOUNDARIES) == len(expected)

    for stage in expected:
        boundary = get_file_crawl_stage_boundary(stage)
        assert boundary.stage == stage
        assert boundary.owner
        assert boundary.function_refs
        assert boundary.inputs
        assert boundary.outputs

    payload = get_file_crawl_stage_boundary(FileCrawlStage.FILE_PAYLOAD_BUILD)
    assert "original_meta.attachment_name" in payload.preserved_fields
    assert "original_meta.store_cate1" in payload.preserved_fields
    assert "original_meta.store_cate2" in payload.preserved_fields

    download = get_file_crawl_stage_boundary("download_save")
    assert "saved_filename" in download.preserved_fields
    assert "attachment_name" in download.preserved_fields

    candidate = get_file_crawl_stage_boundary(FileCrawlStage.ATTACHMENT_CANDIDATE_COLLECT)
    assert candidate.outputs == ("attachment_candidates",)
    assert "candidate_score" in candidate.preserved_fields


if __name__ == "__main__":
    main()
    print("file crawl stage contract ok")
