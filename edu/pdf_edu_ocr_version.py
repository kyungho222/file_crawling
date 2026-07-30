import os
import pdfplumber
import camelot
import pandas as pd
from db.db_operations import insert_data, delete_data
from config import Config
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OpenAIEmbeddings
import logging
from logs.logging_util import LoggerSingleton
import logging

# pdf_edu.py 상단에 추가
import pytesseract
from PIL import Image


# from db.db_redis import job_manager, job_progress_manager
from db.db_job_managers import AsyncJobManager, AsyncJobProgress
import json
from socket_sender import send_message_to_socket

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.pdf", level=logging.INFO)


embedding_model = OpenAIEmbeddings(openai_api_key=Config.OPENAI_API_KEY)


async def ocr_extract_text_from_page(page) -> str:
    # pdfplumber의 page.to_image() → PIL Image 객체
    pil_image = page.to_image(resolution=300).original
    # OCR 실행
    ocr_text = pytesseract.image_to_string(
        pil_image, lang="kor"
    )  # lang은 필요한 언어에 맞추어 설정
    return ocr_text.strip()


async def extract_text_with_tables(pdf_file):
    """
    PDF 파일에서 페이지별 텍스트와 표 데이터를 함께 추출합니다.
    """
    pages_text = []

    with pdfplumber.open(pdf_file) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            # 페이지 텍스트 추출
            page_text = page.extract_text() or ""
            # 텍스트가 비어있을 경우 (이미지 기반 추정) -> OCR 시도
            if not page_text.strip():
                logger.info(f"페이지 {page_num}에서 텍스트를 찾지 못해 OCR 시도")
                ocr_result = await ocr_extract_text_from_page(page)
                if ocr_result:
                    page_text = ocr_result

            # 표 데이터 추출
            table_chunks = []
            tables = page.extract_tables()
            for table_idx, table in enumerate(tables):
                if table:
                    # 표 데이터를 텍스트 형태로 변환
                    table_text = "\n".join(
                        [
                            "\t".join(
                                [cell if cell is not None else "" for cell in row]
                            )
                            for row in table
                        ]
                    )
                    table_chunks.append(
                        {
                            "page_num": page_num,
                            "table_idx": table_idx + 1,
                            "table_text": f"### Table {table_idx + 1} on Page {page_num} ###\n{table_text}",
                        }
                    )

            pages_text.append(
                {
                    "page_num": page_num,
                    "text": page_text.strip(),
                    "tables": table_chunks,
                }
            )

    return pages_text


async def process_pdf(
    content: str,
    file_path: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    memo: str = "",
):
    """
    PDF 파일을 처리하여 페이지별 텍스트와 표 데이터를 청킹하고, 각 청크에 페이지 번호를 포함해 저장합니다.
    """
    try:
        # 페이지별 텍스트와 표 데이터 추출
        pages_text = await extract_text_with_tables(file_path)
        total_pages = len(pages_text)
        # logger.info(f"total_pages:{total_pages}, each_progress:{each_progress}")
        # PDF 전체가 텍스트가 없는 경우 기본 메시지 추가
        if total_pages == 0:
            logger.warning(f"PDF에서 추출된 텍스트가 없습니다: {content}")
            pages_text = [
                {
                    "page_num": 0,
                    "text": "[이 문서는 텍스트를 포함하지 않습니다.]",
                    "tables": [],
                }
            ]
            total_pages = 1
        each_progress = round(each_progress / total_pages, 2)
        # logger.info(f"each_progress:{each_progress}")
        for page in pages_text:
            page_num = page["page_num"]
            page_text = page["text"]
            table_chunks = page["tables"]

            # 페이지 텍스트를 청킹
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=Config.BASIC_CHUNK_SIZE,
                chunk_overlap=Config.BASIC_CHUNK_OVERLAP,
            )
            text_chunks = text_splitter.split_text(page_text)
            total_chunks = len(text_chunks) + len(table_chunks)
            if total_chunks == 0:
                text_chunks = [""]
                total_chunks = 1
            chunk_progress = round(each_progress / total_chunks, 2)
            logger.info(
                f"[conent: {content}, total_chunk: {total_chunks}, page_num: {page_num}, 학습 시작 job_id:{job_id}, table_name: {table_name}]"
            )
            # 텍스트 청크 처리 및 저장
            for idx, chunk in enumerate(text_chunks):
                status = await job_manager.get_job_status(job_id)
                if status == "cancel":
                    logger.info(f"작업이 취소되었습니다: job_id={job_id}")
                    return
                chunk_num = f"{idx + 1}"
                chunk_with_metadata = f"[Source: {content}]\n[Page: {page_num}\nChunk: {idx + 1}]\n{chunk}"
                embedding = embedding_model.embed_query(chunk_with_metadata)

                # PostgreSQL vector 형식으로 변환
                embedding_array = f"[{','.join(map(str, embedding))}]"

                await insert_data(
                    table=table_name,
                    data={
                        "content": content,
                        "chunk_num": chunk_num,
                        "page_num": str(page_num),
                        "memo": memo,
                        "content_type": "file",
                        "text_data": chunk_with_metadata,
                        "embedding": embedding_array,
                    },
                    dbname=dbname,
                )
                # logger.info(f"[PDF_TEXT_Chunk_number: {idx + 1}]")
                current_progress = await job_progress_manager.get_job_progress(job_id)
                new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
                # logger.info(f"new_progress_text:{new_progress}")
                await job_progress_manager.set_job_progress(job_id, new_progress)
                await send_message_to_socket(
                    job_id,
                    {"status": "in_progress", "progress": new_progress},
                    job_manager,
                )
            # 테이블 데이터를 단일 청크로 처리
            for table_chunk in table_chunks:
                status = await job_manager.get_job_status(job_id)
                if status == "cancel":
                    logger.info(f"작업이 취소되었습니다: job_id={job_id}")
                    return
                chunk_num = f"{idx + 1}"
                page_num = table_chunk["page_num"]
                table_metadata = f"[Source: {content}]\n[Page: {table_chunk['page_num']}\nTable: {table_chunk['table_idx']}]\n"
                table_with_metadata = table_metadata + table_chunk["table_text"]
                embedding = embedding_model.embed_query(table_with_metadata)

                # PostgreSQL vector 형식으로 변환
                embedding_array = f"[{','.join(map(str, embedding))}]"

                await insert_data(
                    table=table_name,
                    data={
                        "content": content,
                        "chunk_num": chunk_num,
                        "page_num": str(page_num),
                        "memo": memo,
                        "content_type": "file",
                        "text_data": table_with_metadata,
                        "embedding": embedding_array,
                    },
                    dbname=dbname,
                )
                # logger.info(f"[PDF_TABLE_Chunk_number: {idx + 1}]")
                current_progress = await job_progress_manager.get_job_progress(job_id)
                new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
                # logger.info(f"new_progress_table:{new_progress}")
                await job_progress_manager.set_job_progress(job_id, new_progress)
                await send_message_to_socket(
                    job_id,
                    {"status": "in_progress", "progress": new_progress},
                    job_manager,
                )
        logger.info(
            f"[conent: {content}, total_chunk: {total_chunks}, 학습 완료 job_id:{job_id}, table_name: {table_name}]"
        )
        return {"status": "success", "message": f"{content} 처리 완료"}

    except Exception as e:
        logger.error(f"PDF 처리 중 오류 발생: {e}", exc_info=True)
        await send_message_to_socket(
            job_id, {"status": "error", "message": str(e)}, job_manager
        )
        raise RuntimeError(f"PDF 처리 중 오류 발생: {e}")
