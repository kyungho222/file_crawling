from typing import List, Dict, Optional

from db.db_job_managers import AsyncJobManager, AsyncJobProgress
from db.db_operations import insert_data_with_metadata
from config import Config
from langchain.text_splitter import RecursiveCharacterTextSplitter
from utils.embedding_config import get_embedding_model
from utils.keyword_queue import enqueue_keyword_job
from logs.logging_util import LoggerSingleton
from socket_sender import send_message_to_socket
import logging
import asyncio

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.video", level=logging.INFO)

embedding_model = get_embedding_model()

DEFAULT_VIDEO_CHUNK_SIZE = 1000
MIN_SEGMENTS_PER_CHUNK = 2


def build_segment_chunks(
    segments: List[Dict[str, str]], chunk_size: int = DEFAULT_VIDEO_CHUNK_SIZE
) -> List[List[Dict[str, str]]]:
    """세그먼트를 기준으로 청크를 생성합니다."""
    normalized_segments: List[Dict[str, str]] = []
    for segment in segments:
        if not segment:
            continue
        text = str(segment.get("text", "")).strip()
        if not text:
            continue
        normalized_segments.append(
            {
                "start": str(segment.get("start", "")),
                "text": text,
            }
        )

    if not normalized_segments:
        return []

    min_segments = MIN_SEGMENTS_PER_CHUNK if len(normalized_segments) > 1 else 1

    chunks: List[List[Dict[str, str]]] = []
    current_chunk: List[Dict[str, str]] = []
    current_length = 0

    for segment in normalized_segments:
        segment_length = len(segment["text"])

        should_flush = (
            current_chunk
            and (current_length + segment_length) > chunk_size
            and len(current_chunk) >= min_segments
        )

        if should_flush:
            chunks.append(current_chunk)
            current_chunk = []
            current_length = 0

        current_chunk.append(segment)
        current_length += segment_length

    if current_chunk:
        if len(current_chunk) < min_segments and chunks:
            chunks[-1].extend(current_chunk)
        else:
            chunks.append(current_chunk)

    if not chunks:
        chunks.append(normalized_segments)

    return chunks

async def process_video(
    content: str,
    subject: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    memo: str = "",
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
    video_file_name: str = None,  # 비디오 파일 이름 추가
    content_created_at: str = None,
    content_updated_at: str = None,
    video_segments: Optional[List[Dict[str, str]]] = None,
    chat_bot_id: str = None,
):
    """
    비디오 데이터를 처리하여 벡터화한 후 데이터베이스에 저장합니다.

    Args:
        content: 학습할 비디오 내용
        subject: 학습 제목
        table_name: 저장할 테이블 이름
        dbname: 데이터베이스 이름
    """
    try:
        # 텍스트를 청크로 나누기
        chunk_payloads = []
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=DEFAULT_VIDEO_CHUNK_SIZE, chunk_overlap=50
        )

        if video_segments:
            segment_chunks = build_segment_chunks(video_segments)
            for chunk in segment_chunks:
                chunk_text = " ".join(segment["text"] for segment in chunk).strip()
                if chunk_text:
                    chunk_payloads.append({"text": chunk_text, "segments": chunk})
        else:
            text_chunks = text_splitter.split_text(content)
            chunk_payloads = [
                {"text": chunk, "segments": None} for chunk in text_chunks if chunk.strip()
            ]

        if not chunk_payloads and content:
            chunk_payloads = [{"text": content.strip(), "segments": None}]

        total_chunks = len(chunk_payloads)
        if total_chunks == 0:
            logger.warning(f"[VIDEO] 청크가 생성되지 않았습니다: subject={subject}")
            return {"chunks": 0, "status": "success"}

        chunk_progress = round(each_progress / total_chunks, 2) if total_chunks else each_progress

        # 개인정보 필터링 시 필터링된 청크들을 저장할 리스트
        filtered_chunks = []
        has_sensitive_data = False  # 개인정보가 감지된 청크가 있는지 확인

        metadata_payload: Dict[str, str] = {}
        if content_created_at:
            metadata_payload["created_at"] = content_created_at
        if content_updated_at:
            metadata_payload["updated_at"] = content_updated_at

        segment_debug_info = []

        logger.info(
            f"[conent: {subject}, total_chunk: {total_chunks}, 학습 시작\n"
            f" job_id:{job_id} table_name: {table_name}, video_file_name: {video_file_name}]"
        )

        # ✅ Redis Queue로 키워드/요약 백그라운드 추출 위임
        if chat_bot_id and dbname:
            try:
                lookup_subject = (subject or video_file_name or "").strip()
                await enqueue_keyword_job(
                    chat_bot_id=chat_bot_id,
                    maria_db_name=dbname,
                    content_type="video",
                    subject=lookup_subject,
                    content=subject,
                    text_for_llm=content,
                )
                logger.info(f"[비디오 키워드 큐 등록] subject: {lookup_subject}")
            except Exception as e:
                logger.warning(f"[비디오 키워드 큐 등록 실패] subject: {subject}, 오류: {e}")

        # 청크를 데이터베이스에 저장
        for idx, chunk_info in enumerate(chunk_payloads):
            chunk = chunk_info["text"]
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"작업이 취소되었습니다: job_id={job_id}")
                return {"chunks": 0, "status": "cancelled"}
            if personal_info_filter == "Y":
                logger.info(f"TEXT 개별 청크 처리 중 개인정보 필터링 적용: job_id={job_id}, chunk={idx+1}")
                from utils.dlp_api import check_pii_content

                original_chunk = chunk  # 원본 청크 저장
                pii_result = check_pii_content(chunk)

                # 마스킹된 텍스트를 chunk 변수에 할당
                if pii_result["success"]:
                    chunk = pii_result["masked_text"]
                    masked_parts_text = pii_result.get("masked_parts_text", "")

                    # ✅ 개인정보가 감지된 경우에만 웹소켓으로 전송
                    if pii_result["is_sensitive"]:
                        has_sensitive_data = True  # 개인정보 감지 플래그 설정
                        from socket_sender import send_pii_filter_result

                        pii_message = {
                            "type": "pii_filter_result",
                            "status": "pii_detected",
                            "file_name": subject,
                            "chunk_number": idx + 1,
                            "original_text": original_chunk,
                            "masked_text": chunk,
                            "masked_parts_text": masked_parts_text,
                            "total_entities": pii_result.get("total_entities", 0),
                            "message": f"개인정보가 감지되어 마스킹 처리되었습니다. (청크 {idx + 1})",
                        }
                        await send_pii_filter_result(job_id, pii_message, job_manager)

                    # 개인정보 필터링이 적용된 모든 청크를 리스트에 저장
                    filtered_chunks.append(chunk)
                else:
                    # PII 검사 실패 시 에러 로깅
                    logger.error(f"PII 검사 실패: {pii_result.get('error', 'Unknown error')}")
                    # 실패 시 원본 텍스트 유지
            chunk_num = f"{idx + 1}"
            chunk_with_metadata = (
                f"[Source: {subject}]\n[Title: {video_file_name or subject}]"
                f"\n[Chunk_number: {idx + 1}]\n{chunk}"
            )
            embedding = embedding_model.embed_query(chunk_with_metadata)

            # PostgreSQL vector 형식으로 변환
            embedding_array = f"[{','.join(map(str, embedding))}]"

            # ✅ DB 저장 직전 취소 상태 확인
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"TEXT DB 저장 전 작업 취소됨: job_id={job_id}, chunk={idx+1}")
                return {"chunks": idx, "status": "cancelled"}

            row_data = {
                "content": subject,
                "chunk_num": chunk_num,
                "memo": memo,
                "content_type": "video",
                "text_data": chunk_with_metadata,
                "embedding": embedding_array,
                "subject": video_file_name,
            }
            chunk_metadata = metadata_payload.copy()
            chunk_segments = chunk_info.get("segments") or []
            if chunk_segments:
                chunk_metadata["segments"] = chunk_segments
            if chunk_metadata:
                row_data["content_metadata"] = chunk_metadata

            await insert_data_with_metadata(
                table=table_name,
                data=row_data,
                dbname=dbname,
            )
            # 진행률 업데이트
            current_progress = await job_progress_manager.get_job_progress(job_id)
            new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
            await job_progress_manager.set_job_progress(job_id, new_progress)
            await send_message_to_socket(
                job_id, {"status": "in_progress", "progress": new_progress}, job_manager
            )

            segment_debug_info.append(
                {
                    "chunk_num": idx + 1,
                    "segment_count": len(chunk_segments),
                    "segment_starts": [seg.get("start", "") for seg in chunk_segments],
                    "char_length": len(chunk),
                    "text_preview": chunk[:80],
                }
            )

            logger.info(
                f"[VIDEO_SEGMENTS] chunk={idx+1}, segments={len(chunk_segments)}, "
                f"chars={len(chunk)}, starts={[seg.get('start', '') for seg in chunk_segments]}"
            )

        logger.info(
            f"[conent: {subject}, total_chunk: {total_chunks}, 학습 완료 job_id:{job_id}, table_name: {table_name}]"
        )

        # ✅ 처리 완료 후 청크 수 반환
        result = {"chunks": total_chunks, "status": "success"}
        if segment_debug_info:
            result["segment_debug"] = segment_debug_info

        # ✅ 개인정보 필터링이 적용되었고 개인정보가 감지된 청크가 있는 경우 마스킹된 전체 텍스트 정보 포함
        if personal_info_filter == "Y" and has_sensitive_data and filtered_chunks:
            # 마스킹된 청크들만으로 구성된 전체 텍스트 (개인정보가 마스킹된 상태)
            result["full_filtered_text"] = "\n".join(filtered_chunks)
            logger.info(
                f"[TEXT_EDU] 마스킹된 전체 텍스트 생성: {len(filtered_chunks)}개 청크, "
                f"총 길이: {len(result['full_filtered_text'])}"
            )

        return result

    except Exception as e:
        logger.error(f"텍스트 처리 중 오류 발생: {str(e)}")
        await send_message_to_socket(
            job_id, {"status": "error", "message": str(e)}, job_manager
        )
        raise RuntimeError(f"텍스트 처리 중 오류 발생: {e}")