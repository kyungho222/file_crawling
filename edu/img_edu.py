import os
import logging
import requests
from db.db_operations import insert_data, delete_data
from backend.config import Config
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from utils.logging_util import LoggerSingleton
from typing import Dict, Any
from backend.config import Config

# from db.db_redis import job_manager, job_progress_manager
from db.db_job_managers import AsyncJobManager, AsyncJobProgress



# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.img", level=logging.INFO)

embedding_model = OpenAIEmbeddings(openai_api_key=Config.OPENAI_API_KEY)


async def extract_text_from_image(file_path: str) -> str:
    """
    이미지 파일에서 OCR을 사용하여 텍스트를 추출합니다.

    Args:
        file_path: 이미지 파일 경로

    Returns:
        str: 추출된 텍스트
    """
    api_key = Config.UPSTAGE_API_KEY
    api_url = Config.UPSTAGE_API_URL
    if not api_key or not api_url:
        raise ValueError(
            "OCR API settings are missing. Check the environment variables."
        )

    try:
        headers = {"Authorization": f"Bearer {api_key}"}

        with open(file_path, "rb") as f:
            files = {"document": f}
            response = requests.post(api_url, headers=headers, files=files)

        response.raise_for_status()

        ocr_data = response.json()
        pages = ocr_data.get("pages", [])

        if not pages or not pages[0].get("text"):
            raise ValueError("No text was found in the ocr result.")

        extracted_text = pages[0].get("text", "")
        logger.info(f"이미지 {file_path}에서 텍스트 추출 완료")

        return extracted_text

    except requests.RequestException as e:
        logger.error(f"Error calling OCR API: {e}", exc_info=True)
        raise RuntimeError(f"Error calling OCR API: {e}")


async def process_img(
    content: str,
    file_path: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    subject: str = "",
    memo: str = "",
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
) -> Dict[str, Any]:
    """
    이미지 파일을 처리하여 텍스트를 추출하고 벡터화한 후 데이터베이스에 저장합니다.

    Args:
        content: 이미지 파일 경로
        cate1: 카테고리1
        cate2: 카테고리2
        table_name: 저장할 테이블 이름
        dbname: 데이터베이스 이름

    Returns:
        Dict[str, Any]: 처리 결과 상태 및 메시지
    """
    try:
        # 이미지에서 텍스트 추출
        extracted_text = await extract_text_from_image(file_path)

        if not extracted_text.strip():
            logger.warning(f"이미지 {content}에서 텍스트를 추출할 수 없습니다.")
            return {"status": "warning", "message": "Unable to extract text."}

        # 텍스트 분할
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(extracted_text)
        total_chunks = len(chunks)
        
        # ✅ main.py에서 이미 계산된 청크별 진행률을 그대로 사용 (txt_edu.py와 동일)
        chunk_progress = each_progress
        
        logger.info(
            f"[IMG 청크 생성] 파일: {content}, 총 청크 수: {total_chunks}"
        )
        logger.info(
            f"[IMG 진행률 계산] 파일당 진행률: {each_progress}%, 청크별 진행률: {chunk_progress}%, 총 청크: {total_chunks}"
        )
        
        # 청크 처리 및 저장
        for idx, chunk in enumerate(chunks):
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"작업이 취소되었습니다: job_id={job_id}")
                return
            if personal_info_filter == "Y":
                logger.info(f"IMAGE 개별 청크 처리 중 개인정보 필터링 적용: job_id={job_id}, chunk={idx+1}")
                from utils.dlp_api import check_pii_content
                original_chunk = chunk  # 원본 청크 저장
                pii_result = check_pii_content(chunk)
                
                # 마스킹된 텍스트를 chunk 변수에 할당
                if pii_result["success"]:
                    chunk = pii_result["masked_text"]
                    

                else:
                    # PII 검사 실패 시 에러 로깅
                    logger.error(f"PII 검사 실패: {pii_result.get('error', 'Unknown error')}")
                    # 실패 시 원본 텍스트 유지
                
            chunk_num = f"{idx + 1}"
            chunk_with_metadata = chunk
            embedding = embedding_model.embed_query(chunk_with_metadata)

            # PostgreSQL vector 형식으로 변환 (대괄호[] 사용)
            embedding_array = f"[{','.join(map(str, embedding))}]"

            await insert_data(
                table=table_name,
                data={
                    "content": content,
                    "subject": subject or os.path.basename(file_path or "") or content,
                    "chunk_num": chunk_num,
                    "memo": memo,
                    "content_type": "file",
                    "text_data": chunk_with_metadata,
                    "embedding": embedding_array,
                },
                dbname=dbname,
            )
            
            # ✅ 진행률 업데이트 - txt_edu.py와 동일한 방식으로 chunk_progress 사용
            current_progress = await job_progress_manager.get_job_progress(job_id)
            new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
            
            await job_progress_manager.set_job_progress(job_id, new_progress)


        logger.info(
            f"[content: {content}, total_chunks: {total_chunks}, 학습 완료 job_id:{job_id}, table_name: {table_name}]"
        )
        return {"status": "success", "message": f"{content} 처리 완료", "chunks": total_chunks}

    except Exception as e:
        logger.error(f"Error processing image: {e}", exc_info=True)

        raise RuntimeError(f"Error processing image: {e}")


async def calculate_img_chunks(file_path: str) -> int:
    """
    이미지 파일의 청크 수를 계산합니다.
    OCR을 통해 텍스트를 추출한 후 청크 수를 계산합니다.
    """
    try:
        # OCR로 텍스트 추출
        extracted_text = await extract_text_from_image(file_path)
        
        if not extracted_text.strip():
            logger.warning(f"이미지 {file_path}에서 텍스트를 추출할 수 없습니다.")
            return 1  # 텍스트가 없어도 최소 1개 청크로 처리
        
        # 텍스트 분할하여 청크 수 계산
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, 
            chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(extracted_text)
        chunk_count = len(chunks)
        
        logger.info(f"이미지 청크 수 계산 완료: {file_path}, 청크 수: {chunk_count}")
        return chunk_count
        
    except Exception as e:
        logger.error(f"이미지 청크 수 계산 실패: {file_path}, 오류: {e}")
        return 1  # 오류 시 기본값 1개
