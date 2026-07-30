import os
import asyncio
import logging
import backend.edu_url_patch  # noqa: F401
from db.db_operations import insert_data, execute_query, delete_data
from config.settings import connect_db, return_connection, Config
from edu.doc_edu import process_doc
from edu.txt_edu import process_txt
from edu.pdf_edu import process_pdf
from edu.hwp_edu import process_hwp, process_hwpx
from edu.xls_edu import process_xls
from edu.pptx_edu import process_ppt
from edu.url_edu import process_url
from edu.img_edu import process_img
from edu.text_edu import process_text
from edu.image_edu import process_image
from utils.logging_util import LoggerSingleton
from db.db_job_managers import AsyncJobManager, AsyncJobProgress

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.learnmodules", level=logging.INFO)

FILE_TEXT_EXTRACT_TIMEOUT_TYPES = {"pdf", "txt", "doc", "hwp", "hwpx", "xls", "ppt", "img", "file"}


def _file_text_extract_timeout_seconds() -> float:
    try:
        value = float(os.getenv("FILE_TEXT_EXTRACT_TIMEOUT_SEC", "1800") or "1800")
    except Exception:
        value = 1800.0
    return max(0.0, min(value, 24 * 3600.0))


async def _await_file_processor_with_timeout(coro, *, content_type: str, content: str, file_path: str, subject: str, job_id: str):
    timeout_sec = _file_text_extract_timeout_seconds()
    if timeout_sec <= 0 or str(content_type or "").lower() not in FILE_TEXT_EXTRACT_TIMEOUT_TYPES:
        return await coro
    try:
        return await asyncio.wait_for(coro, timeout=timeout_sec)
    except asyncio.TimeoutError:
        logger.error(
            "[FileTextExtractTimeout] timeout=%ss job_id=%s content_type=%s content=%s file_path=%s subject=%s",
            int(timeout_sec),
            job_id,
            content_type,
            str(content or "")[:240],
            str(file_path or "")[:260],
            str(subject or "")[:180],
        )
        return {
            "status": "error",
            "message": f"file_text_extract_timeout:{int(timeout_sec)}s",
            "chunks": 0,
            "chunk_count": [0],
            "use_source": [content or subject or file_path],
        }

async def check_faiss_index_exists(dbname: str, chat_id: str) -> bool:
    """
    FAISS 인덱스 파일이 존재하는지 확인합니다.
    """
    try:
        query = """SELECT faiss_index_name FROM chatbot_setup WHERE chat_id = $1"""
        result = await execute_query(
            query, params=(chat_id,), fetch=True, dbname=dbname
        )

        if result and result[0][0]:
            index_path = os.path.join(Config.FAISS_INDEX_DIR, result[0][0])
            return os.path.exists(index_path)
        return False
    except Exception as e:
        logger.error(f"FAISS 인덱스 확인 중 오류 발생: {e}")
        return False


async def check_duplicate_content(content: str, table_name: str, dbname: str) -> bool:
    """
    content가 이미 학습되어 있는지 확인합니다.

    Args:
        content: 파일명 또는 URL
        table_name: 테이블 이름
        dbname: 데이터베이스 이름

    Returns:
        bool: 중복 여부 (True: 중복, False: 중복 아님)
    """
    try:
        query = f"""
            SELECT COUNT(*) 
            FROM {table_name} 
            WHERE content = $1
        """
        result = await execute_query(
            query, params=(content,), fetch=True, dbname=dbname
        )
        is_dup = result[0][0] > 0
        return is_dup
    except Exception as e:
        logger.error(f"중복 검사 중 오류 발생: {e}")
        return False


async def process_and_store(
    content: str,
    file_path: str,
    content_type: str,
    table_name: str,
    dbname: str,
    job_id: str,
    subject: str = None,
    each_progress: float = 0.0,
    job_manager: AsyncJobManager = None,
    job_progress_manager: AsyncJobProgress = None,
    memo: str = None,
    crawl_mode: str = None,
    sitemap: str = "N",  # 사이트맵 탐색 옵션 추가
    chat_bot_id: str = None,  # chat_bot_id 매개변수 추가
    url_filter: str = "B",  # URL 필터링 옵션 추가
    chatbot_config: dict = None,
):
    try:
        def _normalize_file_content_value(raw_content: str | None, file_path_value: str | None) -> str | None:
            """
            파일 학습용 content 값 정규화.
            - content가 URL이면 URL(정규화)을 유지한다.  (파일/게시판 공통: URL을 식별자로 사용)
            - URL이 아니면 레거시 호환을 위해 '파일명.확장자'(basename)로 축약한다.
            """
            try:
                from utils.url import canonicalize_url_for_dedup

                if isinstance(raw_content, str) and raw_content.startswith(("http://", "https://")):
                    return canonicalize_url_for_dedup(raw_content) or raw_content
            except Exception:
                pass

            candidate = file_path_value or raw_content
            if not candidate or not isinstance(candidate, str):
                return raw_content
            try:
                return os.path.basename(candidate) or raw_content
            except Exception:
                return raw_content

        # -------------------------------------------------
        # 크롤링 워크플로우는 request.content_type을 "text"로 고정해서 보내는 경우가 있다.
        # 하지만 content가 "로컬 파일 경로"라면 확장자에 따라 실제 파일 타입 프로세서를 사용해야 한다.
        # (PDF/HWP 등을 text_edu로 읽으면 한글 깨짐/바이너리 깨짐이 발생)
        # -------------------------------------------------
        try:
            # content가 URL로 들어오는 경우에도 file_path가 실제 파일이면 확장자 기반 라우팅을 수행한다.
            candidate_path = file_path or (content if isinstance(content, str) else None)
            if content_type == "text" and isinstance(candidate_path, str) and os.path.exists(candidate_path) and os.path.isfile(candidate_path):
                _, ext = os.path.splitext(candidate_path)
                ext = ext.lower().lstrip(".")
                ext_map = {
                    "pdf": "pdf",
                    "doc": "doc",
                    "docx": "doc",
                    "hwp": "hwp",
                    "hwpx": "hwpx",
                    "xls": "xls",
                    "xlsx": "xls",
                    "csv": "xls",
                    "ppt": "ppt",
                    "pptx": "ppt",
                    "txt": "text",  # 순수 텍스트는 기존 text_edu 로직을 사용
                    "jpg": "img",
                    "jpeg": "img",
                    "png": "img",
                    "gif": "img",
                    "bmp": "img",
                }
                inferred = ext_map.get(ext)
                if inferred and inferred != "text":
                    logger.info(
                        "[LEARN-POSTGRES] [ROUTE] override content_type=text -> %s (file extension=%s, path=%s)",
                        inferred,
                        ext,
                        candidate_path,
                    )
                    # file 프로세서 경로로 태우기 위해 content_type을 덮어쓴다.
                    content_type = inferred
                    file_path = candidate_path
        except Exception as route_exc:
            logger.debug("[LEARN-POSTGRES] [ROUTE] override check failed: %s", route_exc)

        # chatbot_config 설정 불러오기
        chatbot_config = chatbot_config
        personal_info_filter = chatbot_config.get("personal_info_filter","N")
        logger.info(f"chat_bot_id={chat_bot_id} personal_info_filter 설정 불러오기: {personal_info_filter}")
        logger.info(f"[DEBUG] chatbot_config 전체: {chatbot_config}")
        logger.info(f"[DEBUG] personal_info_filter 값: '{personal_info_filter}' (type: {type(personal_info_filter)})")
        if content_type == "text":
            # 텍스트 타입일 경우 subject(제목)를 원본 그대로 사용
            logger.info(f"[TEXT] 중복 체크 시작: Subject={subject} | Table={table_name} | db_Name={dbname}")
            is_duplicate = await check_duplicate_content(
                subject, table_name, dbname
            )
            logger.info(f"[TEXT] 중복 체크 결과: is_duplicate={is_duplicate} | Subject={subject}")

            if is_duplicate:
                # ✅ 정책 변경: 중복이면 스킵이 아니라 삭제 후 재학습
                logger.warning(f"[TEXT] 이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {subject}")
                try:
                    await delete_data(table_name, {"content": subject}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"[TEXT] 중복 삭제 실패(진행): {del_exc}")

            processor = process_text

        elif content_type == "url":
            # URL 타입일 경우 URL을 원본 그대로 사용
            logger.info(f"[URL] 중복 체크 시작: Content={content} | Table={table_name} | db_Name={dbname}")
            is_duplicate = await check_duplicate_content(
                content, table_name, dbname
            )
            logger.info(f"[URL] 중복 체크 결과: is_duplicate={is_duplicate} | Content={content}")
            
            if is_duplicate:
                # ✅ 정책 변경: 중복이면 스킵이 아니라 삭제 후 재학습
                logger.warning(f"[URL] 이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {content}")
                try:
                    await delete_data(table_name, {"content": content}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"[URL] 중복 삭제 실패(진행): {del_exc}")
            
            file_path = (
                subject  # url은 file_path는 필요없어서 이부분에 제목을 받아서 넣는다
            )
            processor = process_url
        # 이미지 설명 text 로직으로 처리 어린이재단 처리 때문에 급하게
        elif content_type == "image":
            # 이미지 설명 타입일 경우 subject(이미지 파일명)를 사용
            logger.info(f"[IMAGE] 중복 체크 시작: Subject={subject} | Table={table_name} | db_Name={dbname}")
            is_duplicate = await check_duplicate_content(
                subject, table_name, dbname
            )
            logger.info(f"[IMAGE] 중복 체크 결과: is_duplicate={is_duplicate} | Subject={subject}")
            
            if is_duplicate:
                # ✅ 정책 변경: 중복이면 스킵이 아니라 삭제 후 재학습
                logger.warning(f"[IMAGE] 이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {subject}")
                try:
                    await delete_data(table_name, {"content": subject}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"[IMAGE] 중복 삭제 실패(진행): {del_exc}")

            processor = process_image  # 이미지 설명은 텍스트로 처리

        else:
            # 파일 타입일 경우: URL이면 URL(정규화)을 유지하고, 그렇지 않으면 basename으로 축약(레거시 호환)
            normalized_content = _normalize_file_content_value(content, file_path)
            if normalized_content and normalized_content != content:
                logger.info("[LEARN-POSTGRES] [NORMALIZE] content for file -> %s", normalized_content)
                content = normalized_content

            # 파일 타입일 경우 파일명을 원본 그대로 사용
            logger.info(f"중복 체크 시작: Content={content} | Table={table_name}")
            is_duplicate = await check_duplicate_content(
                content, table_name, dbname
            )
            logger.info(f"중복 체크 결과: is_duplicate={is_duplicate} | Content={content}")

            if is_duplicate:
                # ✅ 정책 변경: 중복이면 스킵이 아니라 삭제 후 재학습
                logger.warning(f"이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {content}")
                try:
                    await delete_data(table_name, {"content": content}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"중복 삭제 실패(진행): {del_exc}")
            
            # 파일 확장자 추출 및 정규화
            _, ext = os.path.splitext(file_path)
            ext = ext.lower().replace(".", "")

            # 파일 타입 매핑
            content_type_mapping = {
                "pdf": "pdf",
                "doc": "doc",
                "docx": "doc",
                "hwp": "hwp",
                "hwpx": "hwpx",
                "xls": "xls",
                "xlsx": "xls",
                "csv": "xls",
                "ppt": "ppt",
                "pptx": "ppt",
                "txt": "txt",
                "jpg": "img",
                "jpeg": "img",
                "png": "img",
                "gif": "img",
                "bmp": "img",
            }

            content_type = content_type_mapping.get(ext)
            if not content_type:
                raise ValueError(f"지원하지 않는 파일 확장자: {ext}")

        # 프로세서 매핑
        processors = {
            "pdf": process_pdf,
            "txt": process_txt,
            "doc": process_doc,
            "hwp": process_hwp,
            "hwpx": process_hwpx,
            "xls": process_xls,
            "ppt": process_ppt,
            "url": process_url,
            "img": process_img,
            "text": process_text,
            "image": process_image,  # 이미지 설명은 텍스트로 처리
        }

        processor = processors.get(content_type)
        if not processor:
            logger.error(f"[LEARN-POSTGRES] [FAIL] 지원하지 않는 파일 유형: {content_type} | Content={content[:50]}")
            raise ValueError(f"지원하지 않는 파일 유형: {content_type}")
        
        # ✅ 처리 결과를 저장할 변수
        result = None
        
        logger.info(f"[LEARN-POSTGRES] [STEP 2] 프로세서 호출 시작: Processor={processor.__name__} | Type={content_type} | Table={table_name}")
        
        # 파일/텍스트 처리 및 학습 데이터 저장
        if content_type == "text":
            logger.info(f"[LEARN-POSTGRES] [DEBUG] process_text(text) 호출 직전 | Processor ID={id(processor)}")
            result = await processor(
                content,
                subject,
                table_name,
                dbname,
                job_id,
                each_progress,
                job_manager,
                job_progress_manager,
                memo,
                personal_info_filter=personal_info_filter,  # 개인정보 필터링 옵션 전달
            )
        elif content_type == "image":
            # 이미지 설명은 텍스트로 처리하지만 content로 subject(이미지 파일명) 사용
            logger.info(f"[LEARN_MODULES DEBUG] process_image 호출 전 - content: '{content[:50]}...', subject: '{subject}', memo: '{memo}'")
            result = await processor(
                content,  # 이미지 설명 텍스트
                subject,  # 이미지 파일명
                table_name,
                dbname,
                job_id,
                each_progress,
                job_manager,
                job_progress_manager,
                memo,
                personal_info_filter=personal_info_filter, # 개인정보 필터링 옵션 전달
            )
        elif content_type == "url":  # URL 처리 시 crawl_mode와 sitemap 전달
            result = await processor(
                content,
                file_path,
                table_name,
                dbname,
                job_id,
                each_progress,
                job_manager,
                job_progress_manager,
                memo,
                crawl_mode,  # crawl_mode 파라미터 전달
                sitemap,  # 사이트맵 옵션 전달
                chat_bot_id=chat_bot_id,
                url_filter=url_filter,  # URL 필터링 옵션 전달
                personal_info_filter=personal_info_filter, # 개인정보 필터링 옵션 전달
            )
        else:
            logger.info(f"[DEBUG] processor 호출 전 personal_info_filter: '{personal_info_filter}'")
            processor_coro = processor(
                content,
                file_path,
                content_type,
                table_name,
                dbname,
                job_id,
                each_progress,
                job_manager,
                job_progress_manager,
                memo,
                personal_info_filter=personal_info_filter, # ???? ??? ?? ??
            )
            result = await _await_file_processor_with_timeout(
                processor_coro,
                content_type=content_type,
                content=content,
                file_path=file_path,
                subject=subject,
                job_id=job_id,
            )

        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"작업이 취소되었습니다: job_id={job_id}")
            return {"chunks": 0, "status": "cancelled"}

        if content_type == "text":
            logger.info(f"{content_type} 처리 완료: {subject}")
        elif content_type == "image":
            logger.info(f"{content_type} 처리 완료: {subject}")
        else:
            logger.info(f"{content_type} 처리 완료: {content}")
        
        # ✅ 결과 반환 (모든 타입에 대해 청크 수 포함)
        if result and isinstance(result, dict):
            logger.info(f"[LEARN-POSTGRES] [STEP 4] 학습 처리 완료: Chunks={result.get('chunks', 0)} | Status={result.get('status')} | Table={table_name}")
            # chunk_count와 use_source가 없는 경우 기본값으로 채워줌
            if "chunk_count" not in result:
                result["chunk_count"] = [result.get("chunks", 0)]
            if "use_source" not in result:
                result["use_source"] = [content if content_type not in ["text", "image"] else subject]
            
            # ✅ full_filtered_text 정보가 있으면 그대로 전달
            if "full_filtered_text" in result:
                logger.info(f"[LEARN_MODULES] full_filtered_text 정보 전달: {content_type}, 길이: {len(result.get('full_filtered_text', ''))}")
            
            return result
        else:
            # 결과가 없는 경우 기본 결과 반환
            return {
                "chunks": 0, 
                "status": "success",
                "chunk_count": [0],
                "use_source": [content if content_type not in ["text", "image"] else subject]
            }

    except Exception as e:
        # 개별 파일 처리 실패가 전체 작업(uvicorn) 예외로 터지지 않도록
        # 여기서는 error 결과 dict로 반환한다. (상위 study_worker가 status/chunks로 판단)
        error_msg = f"처리 중 오류 발생: {e}"
        logger.error(error_msg, exc_info=True)
        try:
            use_src = [content if content_type not in ["text", "image"] else (subject or content)]
        except Exception:
            use_src = [content]
        return {
            "status": "error",
            "message": error_msg,
            "chunks": 0,
            "chunk_count": [0],
            "use_source": use_src,
        }
