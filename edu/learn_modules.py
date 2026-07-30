import os
import logging
import asyncio
from typing import Optional, List, Dict
from db.db_operations import insert_data, execute_query, delete_data
from db.db_config import connect_db, return_connection
from config import Config
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
from edu.video_edu import process_video
from utils.keyword_queue import enqueue_keyword_job
from logs.logging_util import LoggerSingleton
# from faiss_process import refresh_index, create_and_save_index

# from db.db_redis import job_manager
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

FILE_LIKE_CONTENT_TYPES = {"pdf", "txt", "doc", "hwp", "hwpx", "xls", "ppt", "img", "file"}


# process_and_store() 함수용 컨텍스트
class PTContext:
    """process_and_store() 함수용 컨텍스트 클래스.
    
    process_and_store() 함수에서 필요한 모든 파라미터를 하나의 객체로 관리하여
    함수 시그니처를 단순화하고 유지보수성을 향상시킵니다.
    """
    def __init__(
        self,
        content: str,
        file_path: str,
        content_type: str,
        table_name: str,
        dbname: str,
        job_id: str,
        job_manager: AsyncJobManager,
        job_progress_manager: AsyncJobProgress,
        subject: Optional[str] = None,
        each_progress: float = 0.0,
        memo: Optional[str] = None,
        crawl_mode: Optional[str] = None,
        sitemap: str = "N",
        chat_bot_id: Optional[str] = None,
        url_filter: str = "B",
        chatbot_config: Optional[dict] = None,
        video_file_name: Optional[str] = None,
        image_file_name: Optional[str] = None,
        sound_file_name: Optional[str] = None,
        content_created_at: Optional[str] = None,
        content_updated_at: Optional[str] = None,
        video_segments: Optional[List[Dict[str, str]]] = None,
    ):
        self.content = content
        self.file_path = file_path
        self.content_type = content_type
        self.table_name = table_name
        self.dbname = dbname
        self.job_id = job_id
        self.job_manager = job_manager
        self.job_progress_manager = job_progress_manager
        self.subject = subject
        self.each_progress = each_progress
        self.memo = memo
        self.crawl_mode = crawl_mode
        self.sitemap = sitemap
        self.chat_bot_id = chat_bot_id
        self.url_filter = url_filter
        self.chatbot_config = chatbot_config
        self.video_file_name = video_file_name
        self.image_file_name = image_file_name
        self.sound_file_name = sound_file_name
        self.content_created_at = content_created_at
        self.content_updated_at = content_updated_at
        self.video_segments = video_segments


# ❌ 더 이상 사용하지 않음 - 사용자 입력 데이터는 원본 그대로 보존
# def normalize_content(content):
#     """
#     content 값을 정규화하여 공백, 줄바꿈 등을 제거합니다.
#     주로 URL이나 텍스트 제목에 사용됩니다.
# 
#     Args:
#         content (str): 정규화할 문자열.
# 
#     Returns:
#         str: 정규화된 문자열.
#     """
#     # 앞뒤 공백, 줄바꿈 제거
#     normalized = content.strip()
#     # 여러 개의 연속된 공백을 하나로 축소
#     normalized = " ".join(normalized.split())
#     
#     return normalized


# async def check_faiss_index_exists(dbname: str, chat_id: str) -> bool:
#     """
#     FAISS 인덱스 파일이 존재하는지 확인합니다.
#     """
#     try:
#         query = """SELECT faiss_index_name FROM chatbot_setup WHERE chat_id = $1"""
#         result = await execute_query(
#             query, params=(chat_id,), fetch=True, dbname=dbname
#         )

#         if result and result[0][0]:
#             index_path = os.path.join(Config.FAISS_INDEX_DIR, result[0][0])
#             return os.path.exists(index_path)
#         return False
#     except Exception as e:
#         logger.error(f"FAISS 인덱스 확인 중 오류 발생: {e}")
#         return False


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
        return result[0][0] > 0
    except Exception as e:
        logger.error(f"중복 검사 중 오류 발생: {e}")
        return False


async def process_and_store(pt_ctx: PTContext):

    logger.info(f"[LEARN-EDU] process_and_store 진입 | Type={pt_ctx.content_type} | Subject={pt_ctx.subject}")
    try:
        try:
            print(f"[test050] process_and_store received memo={pt_ctx.memo}", flush=True)
        except Exception:
            pass
        try:
            print(f"[test060] process_and_store received subject={pt_ctx.subject}", flush=True)
        except Exception:
            pass
        # 내부 편의 변수
        content = pt_ctx.content
        file_path = pt_ctx.file_path
        content_type = pt_ctx.content_type
        table_name = pt_ctx.table_name
        dbname = pt_ctx.dbname
        job_id = pt_ctx.job_id
        job_manager = pt_ctx.job_manager
        job_progress_manager = pt_ctx.job_progress_manager
        subject = pt_ctx.subject

        def _normalize_file_content_value(raw_content: str | None, file_path_value: str | None) -> str | None:
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

        # chatbot_config 설정 불러오기
        chatbot_config = pt_ctx.chatbot_config
        personal_info_filter = chatbot_config.get("personal_info_filter", "N") if chatbot_config else "N"
        logger.info(f"chat_bot_id={pt_ctx.chat_bot_id} personal_info_filter 설정 불러오기: {personal_info_filter}")

        # 게시판(url/text/image) 경로는 그대로 두고, 메타/큐에서만 오는 리터럴 "file"만 text로 맞춘 뒤
        # 아래 블록에서 로컬 파일이면 확장자 기반으로 pdf/hwp 등으로 재라우팅한다.
        if content_type == "file":
            content_type = "text"

        # content_type이 text이지만 실제 file_path가 로컬 파일이면 확장자 기반으로 라우팅
        try:
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
                    "txt": "text",
                    "jpg": "img",
                    "jpeg": "img",
                    "png": "img",
                    "gif": "img",
                    "bmp": "img",
                }
                inferred = ext_map.get(ext)
                if inferred and inferred != "text":
                    logger.info(f"[LEARN-EDU] override content_type=text -> {inferred} (file extension={ext}, path={candidate_path})")
                    content_type = inferred
                    file_path = candidate_path
        except Exception as route_exc:
            logger.debug("[LEARN-EDU] override check failed: %s", route_exc)

        # 중복 검사 및 삭제(중복시 삭제 후 재학습 정책)
        if content_type == "text":
            if not subject or subject.strip() == "":
                raise ValueError("텍스트 제목(subject)이 비어있습니다.")
            logger.info(f"[TEXT] 중복 체크 시작: Subject={subject} | Table={table_name} | db_Name={dbname}")
            is_dup = await check_duplicate_content(subject, table_name, dbname)
            logger.info(f"[TEXT] 중복 체크 결과: {is_dup} | Subject={subject}")
            if is_dup:
                logger.warning(f"[TEXT] 이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {subject}")
                try:
                    await delete_data(table_name, {"content": subject}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"[TEXT] 중복 삭제 실패(진행): {del_exc}")
            processor = process_text

        elif content_type == "url":
            logger.info(f"[URL] 중복 체크 시작: Content={content} | Table={table_name} | db_Name={dbname}")
            is_dup = await check_duplicate_content(content, table_name, dbname)
            logger.info(f"[URL] 중복 체크 결과: {is_dup} | Content={content}")
            if is_dup:
                logger.warning(f"[URL] 이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {content}")
                try:
                    await delete_data(table_name, {"content": content}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"[URL] 중복 삭제 실패(진행): {del_exc}")
            file_path = subject
            processor = process_url

        elif content_type == "image":
            logger.info(f"[IMAGE] 중복 체크 시작: Subject={subject} | Table={table_name} | db_Name={dbname}")
            is_dup = await check_duplicate_content(subject, table_name, dbname)
            logger.info(f"[IMAGE] 중복 체크 결과: {is_dup} | Subject={subject}")
            if is_dup:
                logger.warning(f"[IMAGE] 이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {subject}")
                try:
                    await delete_data(table_name, {"content": subject}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"[IMAGE] 중복 삭제 실패(진행): {del_exc}")
            processor = process_image

        else:
            # 파일 타입일 경우 content 정규화 및 중복 검사
            normalized_content = _normalize_file_content_value(content, file_path)
            if normalized_content and normalized_content != content:
                logger.info("[LEARN-EDU] [NORMALIZE] content for file -> %s", normalized_content)
                content = normalized_content
            logger.info(f"중복 체크 시작: Content={content} | Table={table_name}")
            is_dup = await check_duplicate_content(content, table_name, dbname)
            logger.info(f"중복 체크 결과: {is_dup} | Content={content}")
            if is_dup:
                logger.warning(f"이미 학습된 콘텐츠 발견 → 삭제 후 재학습: {content}")
                try:
                    await delete_data(table_name, {"content": content}, dbname=dbname)
                except Exception as del_exc:
                    logger.warning(f"중복 삭제 실패(진행): {del_exc}")

            # 파일 확장자 추출 및 정규화
            _, ext = os.path.splitext(file_path or "")
            ext = ext.lower().replace(".", "")
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
            content_type = content_type_mapping.get(ext) or content_type
            if not content_type or content_type == "file":
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
            "image": process_image,
            "video": process_video,
        }

        processor = processors.get(content_type)
        if not processor:
            logger.error(f"[LEARN-EDU] [FAIL] 지원하지 않는 파일 유형: {content_type} | Content={str(content)[:50]}")
            raise ValueError(f"지원하지 않는 파일 유형: {content_type}")

        result = None

        # 처리 호출
        if content_type == "text":
            result = await processor(
                content,
                subject,
                table_name,
                dbname,
                job_id,
                pt_ctx.each_progress,
                job_manager,
                job_progress_manager,
                pt_ctx.memo,
                personal_info_filter=personal_info_filter,
            )
        elif content_type == "image":
            content_preview = (content or "")[:50].replace('\n', ' ').replace('\r', '')
            logger.info(f"[LEARN_MODULES DEBUG] process_image 호출 전 - content: '{content_preview}...', subject: '{subject}', memo: '{pt_ctx.memo}', image_file_name: '{pt_ctx.image_file_name}'")
            result = await processor(
                content,
                subject,
                table_name,
                dbname,
                job_id,
                pt_ctx.each_progress,
                job_manager,
                job_progress_manager,
                pt_ctx.memo,
                personal_info_filter=personal_info_filter,
                image_file_name=pt_ctx.image_file_name if pt_ctx.image_file_name else subject,
                content_created_at=pt_ctx.content_created_at,
                content_updated_at=pt_ctx.content_updated_at,
                chat_bot_id=pt_ctx.chat_bot_id,
            )
        elif content_type == "video":
            logger.info(f"[LEARN_MODULES DEBUG] process_video 호출 전 - content: '{(content or '')[:50]}...', subject: '{subject}', memo: '{pt_ctx.memo}', video_file_name: '{pt_ctx.video_file_name}'")
            result = await processor(
                content,
                subject,
                table_name,
                dbname,
                job_id,
                pt_ctx.each_progress,
                job_manager,
                job_progress_manager,
                pt_ctx.memo,
                personal_info_filter=personal_info_filter,
                video_file_name=pt_ctx.video_file_name,
                content_created_at=pt_ctx.content_created_at,
                content_updated_at=pt_ctx.content_updated_at,
                video_segments=pt_ctx.video_segments,
                chat_bot_id=pt_ctx.chat_bot_id,
            )
        elif content_type == "sound":
            result = await processor(
                content,
                subject,
                table_name,
                dbname,
                job_id,
                pt_ctx.each_progress,
                job_manager,
                job_progress_manager,
                pt_ctx.memo,
                personal_info_filter=personal_info_filter,
                sound_file_name=pt_ctx.sound_file_name,
                content_created_at=pt_ctx.content_created_at,
                content_updated_at=pt_ctx.content_updated_at,
                chat_bot_id=pt_ctx.chat_bot_id,
            )
        elif content_type == "url":
            result = await processor(
                content,
                file_path,
                table_name,
                dbname,
                job_id,
                pt_ctx.each_progress,
                job_manager,
                job_progress_manager,
                pt_ctx.memo,
                pt_ctx.crawl_mode,
                pt_ctx.sitemap,
                chat_bot_id=pt_ctx.chat_bot_id,
                url_filter=pt_ctx.url_filter,
                personal_info_filter=personal_info_filter,
            )
        else:
            logger.info(f"[DEBUG] processor 호출 전 personal_info_filter: '{personal_info_filter}'")
            processor_coro = processor(
                content=content,                             # ??? ??
                file_path=file_path,                         # ?? ??
                content_type=content_type,                           # ?? ?? (file/civil_form)
                table_name=table_name,                       # ??? ?
                dbname=dbname,                               # DB ?
                job_id=job_id,                               # ?? ID
                each_progress=pt_ctx.each_progress,          # ?? ???
                job_manager=job_manager,                     # ?? ???
                job_progress_manager=job_progress_manager,   # ?? ???
                subject=subject,                              # ? ?? ???(???)
                memo=pt_ctx.memo,                            # ??
                personal_info_filter=personal_info_filter,    # ???? ?? ?? (?? ??)
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

        # 파일 계열에 대해 키워드 큐 등록
        if content_type in FILE_LIKE_CONTENT_TYPES and pt_ctx.chat_bot_id and dbname:
            try:
                lookup_subject = (content or subject or "").strip()
                full_text = result.get("full_filtered_text", "") if isinstance(result, dict) else ""
                await enqueue_keyword_job(
                    chat_bot_id=pt_ctx.chat_bot_id,
                    maria_db_name=dbname,
                    content_type="file",
                    subject=lookup_subject,
                    content=lookup_subject,
                    text_for_llm=full_text or None,
                    pg_db_name=dbname,
                )
                logger.info(f"[파일 키워드 큐 등록] content_type={content_type}, subject={lookup_subject[:80]}")
            except Exception as kw_err:
                logger.warning(f"[파일 키워드 큐 등록 실패] content_type={content_type}, error={kw_err}")

        # 완료 로깅
        if content_type in {"text", "image", "video", "sound"}:
            logger.info(f"{content_type} 처리 완료: {subject}")
        else:
            logger.info(f"{content_type} 처리 완료: {content}")

        # 결과 정리 및 반환
        if result and isinstance(result, dict):
            if "chunk_count" not in result:
                result["chunk_count"] = [result.get("chunks", 0)]
            if "use_source" not in result:
                result["use_source"] = [content if content_type not in ["text", "image", "video", "sound"] else subject]
            if "chunk_hash" not in result:
                result["chunk_hash"] = []
            elif not isinstance(result["chunk_hash"], list):
                result["chunk_hash"] = [result["chunk_hash"]]
            if "full_filtered_text" in result:
                logger.info(f"[LEARN_MODULES] full_filtered_text 정보 전달: {content_type}, 길이: {len(result.get('full_filtered_text', ''))}")
            return result
        else:
            return {
                "chunks": 0,
                "status": "success",
                "chunk_count": [0],
                "use_source": [content if content_type not in ["text", "image", "video", "sound"] else subject],
                "chunk_hash": [],
            }
    except Exception as e:
        error_msg = f"처리 중 오류 발생: {e}"
        logger.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg)
