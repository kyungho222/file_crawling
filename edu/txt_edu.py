from db.db_operations import insert_data, delete_data
from backend.config import Config
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from utils.logging_util import LoggerSingleton
import logging
from db.db_job_managers import AsyncJobManager, AsyncJobProgress

import asyncio
import aiofiles
from typing import List
import time
import multiprocessing
import os

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.txt", level=logging.INFO)

embedding_model = OpenAIEmbeddings(openai_api_key=Config.OPENAI_API_KEY)


# ✅ 고정 12코어 병렬성 계산 함수
def calculate_optimal_txt_processing(total_chunks: int, file_size_mb: float, active_jobs: int = 1) -> dict:
    """고정 12코어 기준 TXT 처리 설정 계산"""
    cpu_cores = multiprocessing.cpu_count()
    
    # 🔧 고정 12코어 사용
    base_concurrent = 12
    base_batch_size = 8
    
    # 📁 파일 크기별 최적화 (12코어 기준)
    if file_size_mb < 0.5:  # 512KB 미만 (매우 작은 파일)
        # I/O bound 특성, 더 많은 동시 처리 가능
        max_concurrent = min(base_concurrent * 2, 24)
        batch_size = min(base_batch_size * 2, 16)
        processing_type = "I/O_LIGHT"
    elif file_size_mb < 2:  # 2MB 미만 (작은 파일)
        # 표준 처리
        max_concurrent = base_concurrent
        batch_size = base_batch_size
        processing_type = "STANDARD"
    elif file_size_mb < 10:  # 10MB 미만 (중간 파일)
        # CPU/메모리 사용량 증가, 병렬성 감소
        max_concurrent = max(base_concurrent // 2, 6)
        batch_size = max(base_batch_size // 2, 4)
        processing_type = "CPU_MEDIUM"
    else:  # 큰 파일
        # 메모리 집약적, 최소 병렬성
        max_concurrent = max(base_concurrent // 3, 4)
        batch_size = max(base_batch_size // 3, 3)
        processing_type = "MEMORY_HEAVY"
    
    return {
        "max_concurrent": max_concurrent,
        "batch_size": batch_size,
        "processing_type": processing_type,
        "cpu_cores": cpu_cores,
        "active_jobs": active_jobs,
        "file_size_mb": file_size_mb,
        "total_chunks": total_chunks
    }


# ✅ 개별 청크를 비동기로 처리하는 함수
async def process_single_chunk_txt(
    chunk: str,
    chunk_idx: int,
    content: str,
    subject: str,
    table_name: str,
    dbname: str,
    memo: str,
):
    """개별 청크를 비동기로 처리 (임베딩 + 저장)"""
    try:
        chunk_num = str(chunk_idx)
        chunk_with_metadata = chunk

        # ✅ 비동기 임베딩 생성
        embedding = await embedding_model.aembed_query(chunk_with_metadata)
        embedding_array = f"[{','.join(map(str, embedding))}]"

        # ✅ 비동기 DB 저장
        logger.info(f"[TXT DB 저장] content 값: '{content}', chunk_idx: {chunk_idx}")
        
        await insert_data(
            table=table_name,
            data={
                "content": content,
                "subject": subject,
                "chunk_num": chunk_num,
                "memo": memo,
                "content_type": "file",
                "text_data": chunk_with_metadata,
                "embedding": embedding_array,
            },
            dbname=dbname,
        )

    except Exception as e:
        logger.error(f"TXT 청크 처리 오류 ({content}, chunk {chunk_idx}): {e}")
        # 개별 청크 실패는 전체를 중단하지 않음


# ✅ 개선된 청크들을 동적 병렬 처리하는 함수 (multiprocessing 기반)
async def process_chunks_parallel_txt(
    chunks: List[str],
    content: str,
    subject: str,
    table_name: str,
    dbname: str,
    job_id: str,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    chunk_progress: float,
    memo: str,
    file_size_mb: float = 0,
    active_jobs: int = 1,
    batch_size: int = None,  # None이면 동적 계산
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
):
    """청크들을 CPU 코어 수 기반 동적 병렬 처리"""
    
    total_chunks = len(chunks)
    
    # ✅ 동적 병렬 처리 설정 계산
    if batch_size is None:
        processing_config = calculate_optimal_txt_processing(
            total_chunks=total_chunks,
            file_size_mb=file_size_mb,
            active_jobs=active_jobs
        )
        batch_size = processing_config["batch_size"]
        max_concurrent = processing_config["max_concurrent"]
        processing_type = processing_config["processing_type"]
        
        logger.info(f"[TXT 동적 병렬 설정] 파일: {content}, "
                   f"크기: {file_size_mb:.2f}MB, CPU 코어: {processing_config['cpu_cores']}, "
                   f"활성 작업: {active_jobs}, 처리 타입: {processing_type}, "
                   f"최대 동시: {max_concurrent}, 배치 크기: {batch_size}")
    else:
        max_concurrent = min(multiprocessing.cpu_count(), 16)
        processing_type = "MANUAL"
        logger.info(f"[TXT 수동 병렬 설정] 파일: {content}, 배치 크기: {batch_size}")
    
    # ✅ 세마포어로 동시 실행 개수 제한
    semaphore = asyncio.Semaphore(max_concurrent)
    completed_chunks = 0
    
    # ✅ 배치별로 청크 처리
    for i in range(0, len(chunks), batch_size):
        # 취소 확인
        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"TXT 청크 처리 중 작업 취소됨: job_id={job_id}")
            return

        batch_chunks = chunks[i:i + batch_size]
        batch_tasks = []

        # ✅ 배치 내 병렬 처리 (세마포어 적용)
        for chunk_idx, chunk in enumerate(batch_chunks, start=i + 1):
            if personal_info_filter == "Y":
                logger.info(f"TXT 개별 청크 처리 중 개인정보 필터링 적용: job_id={job_id}, chunk={chunk_idx}")
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
            task = process_single_chunk_txt_with_progress_and_semaphore(
                semaphore=semaphore,
                chunk=chunk,
                chunk_idx=chunk_idx,
                content=content,
                subject=subject,
                table_name=table_name,
                dbname=dbname,
                memo=memo,
                job_id=job_id,
                job_manager=job_manager,
                job_progress_manager=job_progress_manager,
                chunk_progress=chunk_progress
            )
            batch_tasks.append(task)

        # ✅ 배치 실행 및 진행률 업데이트
        try:
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 배치에서 성공한 청크 수만큼 진행률 업데이트
            successful_chunks = 0
            for result in results:
                if not isinstance(result, Exception) and isinstance(result, dict) and result.get("status") == "success":
                    successful_chunks += 1
            
            if successful_chunks > 0:
                completed_chunks += successful_chunks
                
                # 배치 완료 시 한 번에 진행률 업데이트
                current_progress = await job_progress_manager.get_job_progress(job_id)
                progress_increment = chunk_progress * successful_chunks
                new_progress = round(min(current_progress + progress_increment, 99.99), 2)
                
                logger.info(f"[TXT 진행률 업데이트] 파일: {content}, "
                           f"성공 청크: {successful_chunks}/{len(batch_chunks)}, "
                           f"청크별 진행률: {chunk_progress:.6f}%, "
                           f"증가량: {progress_increment:.6f}%, "
                           f"현재 진행률: {current_progress}% → {new_progress}%, "
                           f"완료된 총 청크: {completed_chunks}/{total_chunks}")
                
                await job_progress_manager.set_job_progress(job_id, new_progress)
                

            
            logger.debug(f"TXT 배치 처리 완료: {content}, 배치 {i//batch_size + 1}, "
                        f"성공 청크: {successful_chunks}/{len(batch_chunks)}")
            
        except Exception as e:
            logger.error(f"TXT 배치 처리 중 오류: {e}")

    logger.info(f"[TXT 파일 청크 처리 완료] {content}: {completed_chunks}/{total_chunks} 청크 성공")


# ✅ 세마포어와 함께 개별 청크를 처리하는 함수
async def process_single_chunk_txt_with_progress_and_semaphore(
    semaphore: asyncio.Semaphore,
    chunk: str,
    chunk_idx: int,
    content: str,
    subject: str,
    table_name: str,
    dbname: str,
    memo: str,
    job_id: str,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    chunk_progress: float
):
    """세마포어로 제어되는 개별 청크 비동기 처리"""
    async with semaphore:  # 동시 실행 개수 제한
        try:
            # 청크 처리 전 취소 상태 확인
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"TXT 개별 청크 처리 중 작업 취소됨: job_id={job_id}, chunk={chunk_idx}")
                return {"status": "cancelled", "chunk_idx": chunk_idx}
            
            chunk_num = str(chunk_idx)
            chunk_with_metadata = chunk

            # 비동기 임베딩 생성
            embedding = await embedding_model.aembed_query(chunk_with_metadata)
            embedding_array = f"[{','.join(map(str, embedding))}]"

            # DB 저장 전 한 번 더 취소 상태 확인
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"TXT DB 저장 전 작업 취소됨: job_id={job_id}, chunk={chunk_idx}")
                return {"status": "cancelled", "chunk_idx": chunk_idx}

            # 비동기 DB 저장
            logger.debug(f"[TXT DB 저장] content 값: '{content}', chunk_idx: {chunk_idx}")
            
            await insert_data(
                table=table_name,
                data={
                    "content": content,
                    "subject": subject,
                    "chunk_num": chunk_num,
                    "memo": memo,
                    "content_type": "file",
                    "text_data": chunk_with_metadata,
                    "embedding": embedding_array,
                },
                dbname=dbname,
            )

            return {"status": "success", "chunk_idx": chunk_idx}

        except Exception as e:
            logger.error(f"TXT 청크 처리 오류 ({content}, chunk {chunk_idx}): {e}")
            return {"status": "error", "chunk_idx": chunk_idx, "error": str(e)}


# ✅ 기존 함수는 호환성을 위해 유지 (deprecated)
async def process_single_chunk_txt_with_progress(
    chunk: str,
    chunk_idx: int,
    content: str,
    subject: str,
    table_name: str,
    dbname: str,
    memo: str,
    job_id: str,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    chunk_progress: float
):
    """개별 청크를 비동기로 처리 (호환성을 위해 유지)"""
    # 세마포어 없는 버전으로 기존 로직 유지
    try:
        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"TXT 개별 청크 처리 중 작업 취소됨: job_id={job_id}, chunk={chunk_idx}")
            return {"status": "cancelled", "chunk_idx": chunk_idx}
        
        chunk_num = str(chunk_idx)
        chunk_with_metadata = chunk

        embedding = await embedding_model.aembed_query(chunk_with_metadata)
        embedding_array = f"[{','.join(map(str, embedding))}]"

        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"TXT DB 저장 전 작업 취소됨: job_id={job_id}, chunk={chunk_idx}")
            return {"status": "cancelled", "chunk_idx": chunk_idx}

        await insert_data(
            table=table_name,
            data={
                "content": content,
                "subject": subject,
                "chunk_num": chunk_num,
                "memo": memo,
                "content_type": "file",
                "text_data": chunk_with_metadata,
                "embedding": embedding_array,
            },
            dbname=dbname,
        )

        return {"status": "success", "chunk_idx": chunk_idx}

    except Exception as e:
        logger.error(f"TXT 청크 처리 오류 ({content}, chunk {chunk_idx}): {e}")
        return {"status": "error", "chunk_idx": chunk_idx, "error": str(e)}


# ✅ 개선된 TXT 처리 함수 (진행률 계산 수정)
async def process_txt(
    content: str,
    file_path: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,  # 파일당 진행률
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    subject: str = "",
    memo: str = "",
    active_jobs: int = 1,
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
):
    """
    TXT 파일을 multiprocessing 기반 병렬 처리하여 텍스트를 추출하고 벡터화한 후 데이터베이스에 저장합니다.
    """
    try:
        start_time = time.time()
        logger.info(f"[TXT 병렬 처리 시작] 파일: {content}")

        # ✅ 파일 크기 계산
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        logger.info(f"[TXT 파일 분석] 파일: {content}, 크기: {file_size_mb:.2f}MB ({file_size_bytes:,} bytes)")

        # ✅ 비동기로 파일 읽기
        async with aiofiles.open(file_path, "r", encoding="utf-8") as file:
            all_text = await file.read()

        # 텍스트를 청크로 나누기
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, 
            chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(all_text)
        total_chunks = len(chunks)
        
        # ✅ main.py에서 이미 계산된 청크별 진행률을 그대로 사용
        chunk_progress = each_progress
        
        logger.info(f"[TXT 청크 생성] 파일: {content}, 총 청크 수: {total_chunks}")
        logger.info(f"[TXT 진행률 계산] 파일당 진행률: {each_progress}%, "
                   f"청크별 진행률: {chunk_progress}%, 총 청크: {total_chunks}")

        # ✅ multiprocessing 기반 청크들을 동적 병렬 처리
        await process_chunks_parallel_txt(
            chunks=chunks,
            content=content,
            subject=subject or os.path.basename(file_path or "") or content,
            table_name=table_name,
            dbname=dbname,
            job_id=job_id,
            job_manager=job_manager,
            job_progress_manager=job_progress_manager,
            chunk_progress=chunk_progress,  # ✅ 수정된 청크별 진행률 전달
            memo=memo,
            file_size_mb=file_size_mb,
            active_jobs=active_jobs,
            batch_size=None,
            personal_info_filter=personal_info_filter, # 개인정보 필터링 옵션 전달
        )

        processing_time = round(time.time() - start_time, 2)
        logger.info(f"[TXT 병렬 처리 완료] 파일: {content}, 청크 수: {total_chunks}, "
                   f"파일 크기: {file_size_mb:.2f}MB, 처리 시간: {processing_time}초")

        return {
            "status": "success",
            "message": f"TXT 파일 처리 완료: {content}",
            "chunks": total_chunks,
            "chunk_count": [total_chunks],
            "use_source": [content],
            "processing_time": processing_time,
            "file_size_mb": file_size_mb
        }

    except Exception as e:
        error_msg = f"TXT 처리 중 오류 발생: {content} - {str(e)}"
        logger.error(error_msg, exc_info=True)

        raise RuntimeError(error_msg)


async def calculate_txt_chunks(file_path: str) -> int:
    """TXT 파일의 청크 수를 계산합니다."""
    try:
        import aiofiles
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        from backend.config import Config
        
        async with aiofiles.open(file_path, "r", encoding="utf-8") as file:
            content = await file.read()
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, 
            chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(content)
        return len(chunks)
    except Exception as e:
        logger.error(f"TXT 청크 수 계산 실패: {file_path}, 오류: {e}")
        return 5  # 기본값
