# File Crawl Stage Boundaries

Updated: 2026-08-01

This document names the current attachment file crawl boundaries before moving
code out of the large workflow methods. The source of truth for code-facing
stage names is `backend/file/file_crawl_stage_contract.py`.

```text
1. detail_url_prepare
   owner: FileDownloadWorkflow._process_one_detail
   input: raw detail item
   output: detail_url
   preserve: post_url

2. category_resolve
   owner: _process_one_detail category block
   input: detail_url, cate_match, url_pattern cache
   output: store_cate1, store_cate2
   preserve: cate1/cate2

3. detail_html_fetch
   owner: _fetch_detail_html_for_selection(..., purpose="file_attachment_detail")
   input: detail_url
   output: html, fetch_meta
   preserve: requested_url, final_url

4. post_meta_extract
   owner: _extract_board_title, _extract_board_reg_date, _extract_file_author_info
   input: html, detail_url
   output: post_title, reg_date, author, department
   preserve: post_title, reg_date, author

5. attachment_candidate_collect
   owner: _extract_attachment_links_generic(html, base_url=detail_url)
   input: html, detail_url
   output: attachment_candidates[]
   preserve: file_url, attachment_name/display_name, candidate_score/candidate_reason
   rule: recall first. Collect broad file-like candidates, exclude only obvious
         SNS/share links here, and leave final file validation to download_save.

6. download_candidate_select
   owner: _enqueue_file_downloads
   input: post_url, attachments, detail_cates, post_meta
   output: selected attachment candidates
   preserve: post_url, attachment_name, cate1/cate2, post_title

7. file_payload_build
   owner: _build_file_meta
   input: attachment, post_url, cate1/cate2, post_meta
   output: file_meta
   preserve: original_meta.attachment_name, original_meta.store_cate1/store_cate2,
             post_url, file_url

8. download_save
   owner: core.crawler.workers.download.download_worker
   input: file_meta
   output: download_local_saved event
   preserve: attachment_name, saved_filename, storage_filename, original_meta
   rule: remote work ends after response validation and atomic local file move

9. local_file_finalize
   owner: BoardContentFilePipelineMixin local finalize workers
   input: download_local_saved event
   output: file_saved event
   work: local file readiness, document metadata, storage sync
   rule: this bounded local queue must not occupy a remote download worker

10. save_event_handle
   owner: _run_file_progress_loop / _file_run_saved_file_learn_after_save
   input: file_saved event
   output: learn_list file_info
   preserve: original_meta, attachment_name, cate1/cate2

11. learn_list_row_ensure
   owner: _ensure_learn_list_row_for_file_save
   input: saved file_info
   output: insert_into_learn_list call
   preserve: subject candidates, category candidates, content URL

12. learn_list_persist
   owner: db.mariadb_save_update.insert_into_learn_list
   input: file_info
   output: LEARN_LIST insert/update
    decision:
      subject = _resolve_file_learning_subject(file_info)
      cate = coalesce_learn_list_cates(file_info)
```

## Boundary Rules

- `post_url` is the source post/detail URL and must survive through
  `original_meta.source_url` or `original_meta.post_url`.
- `attachment_name` is the source page display name and must survive even when
  the downloaded file is saved under a generated safe filename.
- `saved_filename` or `storage_filename` is the physical/storage name. It must
  not replace the source attachment display name.
- Category values should be carried as both top-level `cate1/cate2` where
  needed and `original_meta.store_cate1/store_cate2` for recovery.
- `_extract_attachment_links_generic()` is a candidate collector, not a final
  file verifier. Strong filtering belongs in the download/response validation
  stage.
- `post_title` should be added to `original_meta` during the next behavior patch;
  the current extraction flow computes it but does not consistently pass it to
  the download payload.

## Local Postprocess Settings

- FILE_CRAWL_DEFER_LOCAL_POSTPROCESS=1 enables the split stage for file
  crawls that use defer_save_batch_until_learn_list.
- FILE_CRAWL_LOCAL_FINALIZE_WORKERS defaults to 2.
- FILE_CRAWL_LOCAL_FINALIZE_QUEUE_MAXSIZE defaults to 100.
- FILE_PIPELINE_COLLECTION_QUEUE_MAXSIZE defaults to 500 for file crawls only;
  the shared crawler collection queue keeps its existing default.
- The workflow waits for this queue before final save/learning completion and
  cancels it during a forced stop.

## Download Safety Settings

- DOWNLOAD_ITEM_HARD_TIMEOUT_SEC defaults to 240 seconds and bounds one complete
  attachment attempt, including domain-lock waits and browser fallbacks.
- DOWNLOAD_ITEM_LARGE_HARD_TIMEOUT_SEC defaults to 600 seconds for files at or
  above DOWNLOAD_LARGE_FILE_THRESHOLD_MB.
- A hard timeout releases the worker slot and uses the existing delayed retry queue.