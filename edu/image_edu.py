from db.db_operations import insert_data, delete_data
from backend.config import Config
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from utils.logging_util import LoggerSingleton
import logging
from db.db_job_managers import AsyncJobManager, AsyncJobProgress


# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.text", level=logging.INFO)

embedding_model = OpenAIEmbeddings(openai_api_key=Config.OPENAI_API_KEY)

# 이미지 설명 텍스트로 저장하는 로직
async def process_image(
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
):
    """
    텍스트 데이터를 처리하여 벡터화한 후 데이터베이스에 저장합니다.

    Args:
        content: 학습할 텍스트 내용
        subject: 학습 제목
        cate1: 카테고리1
        cate2: 카테고리2
        table_name: 저장할 테이블 이름
        dbname: 데이터베이스 이름
        
    Returns:
        dict: 처리 결과 정보 (청크 수 포함)
    """
    try:
        logger.info(f"[PROCESS_IMAGE DEBUG] 받은 파라미터 - content: '{content[:50]}...', subject: '{subject}', memo: '{memo}'")
        # 텍스트를 청크로 나누기
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=50)
        chunks = text_splitter.split_text(content)
        total_chunks = len(chunks)
        chunk_progress = round(each_progress / total_chunks, 2)
        
        # 개인정보 필터링 시 필터링된 청크들을 저장할 리스트
        filtered_chunks = []
        has_sensitive_data = False  # 개인정보가 감지된 청크가 있는지 확인
        
        logger.info(
            f"[conent: {subject}, total_chunk: {total_chunks}, 학습 시작\n job_id:{job_id} table_name: {table_name}]"
        )
        # 청크를 데이터베이스에 저장
        for idx, chunk in enumerate(chunks):
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"작업이 취소되었습니다: job_id={job_id}")
                return {"chunks": 0, "status": "cancelled"}
            
            if personal_info_filter == "Y":
                logger.info(f"IMAGE 개별 청크 처리 중 개인정보 필터링 적용: job_id={job_id}, chunk={idx+1}")
                from utils.dlp_api import check_pii_content
                original_chunk = chunk  # 원본 청크 저장
                pii_result = check_pii_content(chunk)
                
                # 마스킹된 텍스트를 chunk 변수에 할당
                if pii_result["success"]:
                    chunk = pii_result["masked_text"]
                    if pii_result["is_sensitive"]:
                        has_sensitive_data = True  # 개인정보 감지 플래그 설정
                    
                    # 개인정보 필터링이 적용된 모든 청크를 리스트에 저장
                    filtered_chunks.append(chunk)
                else:
                    # PII 검사 실패 시 에러 로깅
                    logger.error(f"PII 검사 실패: {pii_result.get('error', 'Unknown error')}")
                    # 실패 시 원본 텍스트 유지
            
            chunk_num = f"{idx + 1}"
            chunk_with_metadata = chunk
            embedding = embedding_model.embed_query(chunk_with_metadata)

            # PostgreSQL vector 형식으로 변환
            embedding_array = f"[{','.join(map(str, embedding))}]"

            # ✅ DB 저장 직전 취소 상태 확인
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"TEXT DB 저장 전 작업 취소됨: job_id={job_id}, chunk={idx+1}")
                return {"chunks": idx, "status": "cancelled"}

            logger.info(f"[PROCESS_IMAGE DEBUG] DB 저장 직전 - content: '{subject}', memo: '{memo}', chunk_num: {chunk_num}")
            await insert_data(
                table=table_name,
                data={
                    "content": subject,  # subject를 content로 저장
                    "chunk_num": chunk_num,
                    "memo": memo,
                    # ✅ 파일(이미지) 학습은 content_type을 "file"로 통일
                    "content_type": "file",
                    "text_data": chunk_with_metadata,
                    "embedding": embedding_array,
                },
                dbname=dbname,
            )
            # 진행률 업데이트
            current_progress = await job_progress_manager.get_job_progress(job_id)
            new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
            await job_progress_manager.set_job_progress(job_id, new_progress)


            # logger.info(f"[Chunk_number: {idx + 1}]")

        logger.info(
            f"[conent: {subject}, total_chunk: {total_chunks}, 학습 완료 job_id:{job_id}, table_name: {table_name}]"
        )
        

        
        # ✅ 처리 완료 후 청크 수 반환
        return {"chunks": total_chunks, "status": "success"}

    except Exception as e:
        logger.error(f"텍스트 처리 중 오류 발생: {str(e)}")

        raise RuntimeError(f"텍스트 처리 중 오류 발생: {e}")
