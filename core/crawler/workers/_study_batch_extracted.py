async def study_process_batch_items(
    batch: List[Dict[str, Any]],
    redis_client,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
) -> None:
    """Process one study batch (shared with universal download worker)."""
    if not batch:
        return
    try:
        batch_conc = int(os.getenv("STUDY_BATCH_CONCURRENCY", "2") or "2")
    except Exception:
        batch_conc = 2
    batch_conc = max(1, min(batch_conc, 10))
    sem = asyncio.Semaphore(batch_conc)

    def _resolve_learning_title(item: Dict[str, Any], file_path: str = "") -> str:
        if not isinstance(item, dict):
            return os.path.basename(file_path or "")
        original_meta = item.get("original_meta")
        candidates = [
            item.get("display_name"),
            item.get("attachment_name"),
            original_meta.get("attachment_name") if isinstance(original_meta, dict) else None,
            item.get("title"),
            original_meta.get("title") if isinstance(original_meta, dict) else None,
            item.get("subject"),
            item.get("name"),
            original_meta.get("name") if isinstance(original_meta, dict) else None,
        ]
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text:
                return text
        return os.path.basename(file_path or "")

    async def _process_item(item: Dict[str, Any]) -> None:
        async with sem:
            try:
                file_path = item.get('file_path') or item.get('local_path')
                if not file_path:
                    return

                # IntegratedWorkflow는 다운로드 완료 후 learn()을 직접 호출한다.
                # 해당 경로에서는 중복 학습을 방지하기 위해 skip 플래그를 사용한다.
                if item.get("skip_study_worker"):
                    logger.info(
                        f"[StudyWorker] ⏭️ Skipping learning for {os.path.basename(file_path)} (skip_study_worker=True)"
                    )
                    return

                chat_bot_id = item.get("chat_bot_id")
                db_name = resolve_db_name(item)
                name = _resolve_learning_title(item, file_path)
                if not (chat_bot_id and db_name):
                    logger.warning(
                        "[StudyWorker] missing chat_bot_id/db_name; cannot learn | file=%s",
                        os.path.basename(file_path),
                    )
                    return

                # learn()이 허용하는 타입으로 매핑
                # IntegratedWorkflow는 file_info에 type 키를 주로 사용한다.
                c_type = (item.get("content_type") or item.get("type") or "text")
                allowed_types = ["text", "url", "image", "video", "sound"]
                target_type = "text"
                try:
                    if c_type in allowed_types:
                        target_type = c_type
                    elif isinstance(c_type, str) and c_type.startswith("image/"):
                        target_type = "image"
                    elif isinstance(c_type, str) and c_type.startswith("video/"):
                        target_type = "video"
                    elif isinstance(c_type, str) and c_type.startswith("audio/"):
                        target_type = "sound"
                    else:
                        # 문서 파일 등은 text로 보내고, process_and_store가 확장자로 재매핑 가능
                        target_type = "text"
                except Exception:
                    target_type = "text"

                # 개별 서브 job_id 생성
                sub_job_id = f"study_{int(time.time())}_{os.getpid()}_{os.path.basename(file_path)[:20]}"

                # 1) MariaDB LEARN_LIST 등록 (학습 트리거용 ID 확보)
                learn_list_id = None
                try:
                    from db.mariadb_save_update import (
                        coalesce_learn_list_cates,
                        insert_into_learn_list,
                    )
                    # 상위 워크플로우(예: IntegratedWorkflow)가 이미 LEARN_LIST에 저장하고 db_id를 전달한 경우 재사용
                    provided_db_id = item.get("db_id") or item.get("learn_list_id") or item.get("learn_list")
                    if provided_db_id:
                        learn_list_id = provided_db_id
                        logger.info(
                            "[StudyWorker] Using provided LEARN_LIST id | id=%s file=%s url=%s",
                            learn_list_id,
                            os.path.basename(file_path),
                            item.get("url"),
                        )
                    else:
                        file_info = {
                            "url": item.get("url"),
                            "name": name,
                            "type": item.get("content_type") or target_type,
                            "size": item.get("size"),
                            "file_path": file_path,
                            "local_path": item.get("local_path") or file_path,
                            "author": item.get("author"),
                            "department": item.get("department"),
                            "author_kind": item.get("author_kind"),
                            "author_raw": item.get("author_raw"),
                            "department_raw": item.get("department_raw"),
                            "source_page": item.get("source_page"),
                            "reg_date": item.get("reg_date"),
                            "original_meta": item.get("original_meta"),
                        }
                        try:
                            _sc1, _sc2 = coalesce_learn_list_cates(item)
                            file_info["cate1"] = _sc1
                            file_info["cate2"] = _sc2
                        except Exception:
                            pass
                        if file_info.get("url"):
                            learn_list_id = await insert_into_learn_list(
                                chat_bot_id=chat_bot_id,
                                db_name=db_name,
                                file_info=file_info,
                            )
                            if learn_list_id:
                                logger.info(
                                    "[StudyWorker] LEARN_LIST registered | id=%s file=%s url=%s disk=%s",
                                    learn_list_id,
                                    os.path.basename(file_path),
                                    item.get("url"),
                                    os.path.isfile(file_path),
                                )
                except Exception as db_exc:
                    logger.warning(
                        "[StudyWorker] LEARN_LIST insert failed | file=%s err=%s",
                        os.path.basename(file_path),
                        db_exc,
                    )

                memo_val = item.get("memo")
                if isinstance(memo_val, list):
                    memo_val = memo_val[0] if memo_val else ""
                memo_text = str(memo_val).strip() if memo_val is not None else ""
                if not memo_text:
                    memo_text = f"Auto-learned from crawl queue: {item.get('url', '')}"
                # no-op placeholders removed; memo_text already prepared

                req = EduRequest(
                    job_id=sub_job_id,
                    chat_bot_id=chat_bot_id,
                    db_name=db_name,
                    content_type=target_type,
                    # contents는 "PG 저장/중복 처리 식별자"로 사용한다.
                    # 파일 학습은 식별자를 URL(정규화)로 통일하고, 실제 로컬 경로는 file_paths로 별도 전달한다.
                    contents=[canonicalize_url_for_dedup(item.get("url") or "") or (item.get("url") or file_path)],
                    file_paths=[file_path],
                    subjects=[name],
                    crawl_mode="Y",
                    memo=[memo_text],
                )

                # ✅ 중복이면 삭제 후 재학습 (learn() 내부 + process_and_store 내부 정책으로 보장)
                result = await learn(
                    req,
                    job_manager=job_manager,
                    job_progress_manager=job_progress_manager,
                    redis_client=redis_client,
                )

                # 2) MariaDB 학습 상태/이력 반영 (청크가 실제로 저장된 경우에만)
                try:
                    if learn_list_id and isinstance(result, dict):
                        chunks_val = 0
                        chunk_list = result.get("chunk_count")
                        if isinstance(chunk_list, list) and chunk_list:
                            try:
                                chunks_val = int(chunk_list[0] or 0)
                            except Exception:
                                chunks_val = 0
                        else:
                            try:
                                chunks_val = int(result.get("chunks", 0) or 0)
                            except Exception:
                                chunks_val = 0

                        if chunks_val > 0:
                            # ✅ 게시판/파일 공용 학습 완료 반영(상태 업데이트 + TRAINING_PROCESS 기록)
                            from backend.shared.learning_finalize import finalize_learning_to_mariadb

                            # PG 저장 식별자는 URL(정규화)로 통일한다.
                            pg_content_value = canonicalize_url_for_dedup(item.get("url") or "") or (item.get("url") or "").strip()
                            if not pg_content_value:
                                pg_content_value = item.get("url") or file_path

                            trigger_ok = await finalize_learning_to_mariadb(
                                chat_bot_id=chat_bot_id,
                                db_name=db_name,
                                learn_list_id=str(learn_list_id),
                                display_name=(name or os.path.basename(file_path)),
                                actual_chunks=chunks_val,
                                pg_content_value=pg_content_value,
                                learning_service=None,
                                pg_wait_timeout_seconds=None,
                                job_id_for_count=item.get("job_id") or item.get("jobId"),
                                crawling_log_id=item.get("craw_id") if isinstance(item, dict) else None,
                            )
                            # ✅ status=Y 업데이트 완료 시점에 study 카운트 증가 (DB 반영)
                            if trigger_ok:
                                try:
                                    job_id_for_log = item.get("job_id") or item.get("jobId")
                                except Exception:
                                    job_id_for_log = None
                                if job_id_for_log:
                                    # ✅ 단계별 결과 URL 저장 (study 성공)
                                    try:
                                        url_to_log = (item.get("url") or "").strip()
                                        if not url_to_log:
                                            # URL이 없으면 count_key/db_id로라도 추적 가능하게 남김
                                            url_to_log = (item.get("_count_key") or "").strip() or f"db:{learn_list_id}"
                                        append_stage_urls(
                                            stage="study",
                                            urls=[{"url": url_to_log, "db_id": str(learn_list_id) if learn_list_id else None}],
                                            job_id=job_id_for_log,
                                            db_name=db_name,
                                        )
                                    except Exception:
                                        pass
                                    try:
                                        await asyncio.sleep(0)
                                    except Exception:
                                        pass
                                    # 학습 카운트 증가 시 Redis SSE로 전체 카운트도 함께 발행하여
                                    # scan/collection/save 값이 UI에 반영되도록 보장
                                    try:
                                        # 가능한 경우 workflow의 get_stats()를 사용해 현재 통계 취득
                                        from backend.shared.crawler_state import crawler_state
                                        try:
                                            workflow = crawler_state.workflows.get(job_id_for_log)
                                        except Exception:
                                            workflow = None

                                        payload = None
                                        if workflow and hasattr(workflow, "get_stats"):
                                            try:
                                                payload = workflow.get_stats()
                                            except Exception:
                                                payload = None

                                        # workflow 통계를 못 얻었으면 DB에서 최종 집계 조회
                                        if payload is None:
                                            try:
                                                from db.crawl_db_manager import get_crawling_log_summary
                                                summary = await get_crawling_log_summary(job_id_for_log, dbname=db_name)
                                                payload = {
                                                    "scan_count": summary.get("scan", 0),
                                                    "total_count": summary.get("scan", 0),
                                                    "collection_count": summary.get("collection", 0),
                                                    "save_count": summary.get("save", 0),
                                                    "study_count": summary.get("study", 0),
                                                    "study_success_count": summary.get("study", 0),
                                                }
                                            except Exception:
                                                payload = {}

                                        # 발행 시도
                                        try:
                                            if payload:
                                                from backend.shared.crawl_shared import send_sse_message
                                                await send_sse_message(job_id_for_log, payload, db_name, source="study_worker")
                                        except Exception as pub_exc:
                                            logger.debug("[StudyWorker] SSE publish failed: %s", pub_exc)
                                    except Exception:
                                        pass
                                
                                # ✅ workflow의 _pending_study_success_keys에 count_key 추가
                                try:
                                    count_key = item.get("_count_key")
                                    if not count_key:
                                        try:
                                            fallback_db_id = item.get("db_id")
                                        except Exception:
                                            fallback_db_id = None
                                        count_key = (
                                            f"db:{fallback_db_id}" if fallback_db_id else None
                                        ) or (item.get("url") or "").strip() or file_path or name
                                        if not count_key:
                                            count_key = f"study_fallback_{int(time.time() * 1000)}_{id(item)}"
                                        # 보정된 키를 다시 주입 (후속 디버깅/추적 용)
                                        try:
                                            item["_count_key"] = count_key
                                        except Exception:
                                            pass
                                        logger.info(
                                            "[StudyWorker] ⚠️ _count_key missing; fallback generated | job_id=%s count_key=%s",
                                            job_id_for_log,
                                            count_key,
                                        )

                                    if count_key and job_id_for_log:
                                        from backend.shared.crawler_state import crawler_state
                                        workflow = crawler_state.workflows.get(job_id_for_log)
                                        if workflow and hasattr(workflow, "_pending_study_success_keys"):
                                            stats_lock = getattr(workflow, "_stats_lock", None)
                                            if stats_lock:
                                                async with stats_lock:
                                                    if hasattr(workflow, "_pending_study_success_keys"):
                                                        workflow._pending_study_success_keys.add(count_key)
                                                        logger.info(
                                                            "[StudyWorker] ✅ Added to _pending_study_success_keys | job_id=%s count_key=%s pending_success_count=%s",
                                                            job_id_for_log,
                                                            count_key,
                                                            len(workflow._pending_study_success_keys),
                                                        )
                                                        # ✅ progress_callback 호출하여 UI 즉시 갱신
                                                        if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                                                            try:
                                                                workflow.progress_callback(workflow.get_stats())
                                                            except Exception:
                                                                pass
                                            else:
                                                # lock이 없으면 직접 추가 (동시성 문제 가능하지만 예외 처리)
                                                if hasattr(workflow, "_pending_study_success_keys"):
                                                    workflow._pending_study_success_keys.add(count_key)
                                                    logger.info(
                                                        "[StudyWorker] ✅ Added to _pending_study_success_keys (no lock) | job_id=%s count_key=%s pending_success_count=%s",
                                                        job_id_for_log,
                                                        count_key,
                                                        len(workflow._pending_study_success_keys),
                                                    )
                                                    # ✅ progress_callback 호출하여 UI 즉시 갱신
                                                    if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                                                        try:
                                                            workflow.progress_callback(workflow.get_stats())
                                                        except Exception:
                                                            pass
                                        elif workflow and hasattr(workflow, "_counted_study_keys") and hasattr(workflow, "stats"):
                                            # BoardContentWorkflow 등 pending 키가 없는 워크플로우도
                                            # 카운트 증가는 workflow._mark_study_done로 일원화하여
                                            # 중복 가산을 방지한다.
                                            try:
                                                # Use workflow's _mark_study_done to ensure single place에서 증가
                                                outcome_norm = "success"
                                                try:
                                                    # call central marker which already guards with _counted_study_keys
                                                    await workflow._mark_study_done(url=count_key, outcome=outcome_norm)
                                                except Exception:
                                                    # As fallback, fall back to guarded increment (best-effort)
                                                    stats_lock = getattr(workflow, "_stats_lock", None)
                                                    if stats_lock:
                                                        async with stats_lock:
                                                            if count_key not in workflow._counted_study_keys:
                                                                workflow._counted_study_keys.add(count_key)
                                                                _file_ns = bool(
                                                                    getattr(
                                                                        workflow,
                                                                        "is_attachment_file_crawl_workflow",
                                                                        False,
                                                                    )
                                                                )
                                                                if _file_ns:
                                                                    workflow.stats["file_study_count"] = int(
                                                                        workflow.stats.get("file_study_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["file_study_success_count"] = int(
                                                                        workflow.stats.get("file_study_success_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["file_study_done_count"] = int(
                                                                        workflow.stats.get("file_study_done_count", 0) or 0
                                                                    ) + 1
                                                                else:
                                                                    workflow.stats["study_count"] = int(
                                                                        workflow.stats.get("study_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["study_success_count"] = int(
                                                                        workflow.stats.get("study_success_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["study_done_count"] = int(
                                                                        workflow.stats.get("study_done_count", 0) or 0
                                                                    ) + 1
                                                # Ensure UI update via progress_callback
                                                if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                                                    try:
                                                        workflow.progress_callback(workflow.get_stats())
                                                    except Exception:
                                                        pass
                                            except Exception as exc:
                                                logger.debug("[StudyWorker] fallback mark_study_done failed | err=%s", exc)
                                        elif not workflow:
                                            logger.warning(
                                                "[StudyWorker] ⚠️ Workflow not found in crawler_state | job_id=%s count_key=%s",
                                                job_id_for_log,
                                                count_key,
                                            )
                                        elif not hasattr(workflow, "_pending_study_success_keys"):
                                            logger.warning(
                                                "[StudyWorker] ⚠️ Workflow has no _pending_study_success_keys | job_id=%s workflow_type=%s",
                                                job_id_for_log,
                                                type(workflow).__name__,
                                            )
                                    elif not count_key:
                                        logger.warning(
                                            "[StudyWorker] ⚠️ _count_key not found in item | job_id=%s item_keys=%s",
                                            job_id_for_log,
                                            list(item.keys()) if isinstance(item, dict) else "not_dict",
                                        )
                                except Exception as pending_exc:
                                    logger.error(
                                        "[StudyWorker] ❌ Failed to add to _pending_study_success_keys | job_id=%s err=%s",
                                        job_id_for_log,
                                        pending_exc,
                                        exc_info=True,
                                    )
                except Exception as trig_exc:
                    logger.warning(
                        "[StudyWorker] trigger_learning failed | id=%s file=%s err=%s",
                        learn_list_id,
                        os.path.basename(file_path),
                        trig_exc,
                    )

                detail_url = (item.get("source_page") or item.get("post_url") or item.get("referer") or "").strip()
                download_url = (item.get("url") or "").strip()
                logger.info(
                    "[StudyWorker][test090] learn finished | status=%s file=%s detail_url=%s download_url=%s",
                    (result or {}).get("status"),
                    os.path.basename(file_path),
                    detail_url,
                    download_url,
                )
                # 자동 원본 삭제 (학습 성공 후)
                try:
                    delete_enabled = str(os.getenv("WEB_SYNC_DELETE_AFTER_STUDY", "1") or "1").strip().lower() in (
                        "1",
                        "true",
                        "yes",
                        "on",
                    )
                    chunks_val_calc = 0
                    try:
                        chunks_val_calc = int((result or {}).get("chunks", 0) or 0)
                    except Exception:
                        chunks_val_calc = 0
                    learn_status = (result or {}).get("status")
                    # 조건: 삭제 기능 활성화, 청크가 존재하고 학습 상태가 성공 계열일 때 삭제
                    if delete_enabled and chunks_val_calc > 0 and learn_status in ("success", "ok", "completed"):
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info("[StudyWorker] original file removed after study | file=%s", file_path)
                        except Exception as del_err:
                            logger.warning("[StudyWorker] failed to remove original file after study | file=%s err=%s", file_path, del_err)
                except Exception:
                    pass
            except Exception as item_err:
                logger.error(f"[StudyWorker] Error processing item {item}: {item_err}")

    tasks = [asyncio.create_task(_process_item(item)) for item in batch]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
