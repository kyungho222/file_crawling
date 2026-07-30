from db.db_operations import insert_data, delete_data
from backend.config import Config
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from utils.logging_util import LoggerSingleton
import logging
import os
from db.db_job_managers import AsyncJobManager, AsyncJobProgress


# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.text", level=logging.INFO)

embedding_model = OpenAIEmbeddings(openai_api_key=Config.OPENAI_API_KEY)

def _decode_bytes_best_effort(raw_bytes: bytes) -> str:
    """
    바이트를 사람이 읽을 수 있는 텍스트로 최대한 복원한다.
    - UTF-8/UTF-8-SIG/UTF-16(LE/BE)/CP949/EUC-KR 등을 시도
    - '�'(replacement char) 개수가 가장 적은 디코딩을 선택
    """
    if raw_bytes is None:
        return ""

    # BOM 기반 우선 후보
    candidates = [
        "utf-8-sig",
        "utf-16",    # BOM 있으면 자동 처리
        "utf-16le",
        "utf-16be",
        "utf-8",
        "cp949",
        "euc-kr",
        "latin1",    # 최후의 수단(항상 성공하므로 마지막)
    ]

    best_text = None
    best_score = None

    for enc in candidates:
        try:
            txt = raw_bytes.decode(enc)
        except Exception:
            continue

        # 점수: replacement char 개수 + 비정상 제어문자 비율(가벼운 패널티)
        rep = txt.count("\ufffd")
        ctrl = sum(1 for ch in txt if ord(ch) < 9 or (13 < ord(ch) < 32))
        # Postgres는 NUL(\x00)을 허용하지 않으므로 강하게 패널티
        nul = txt.count("\x00")
        score = rep * 10 + ctrl + (nul * 50)
        if best_score is None or score < best_score:
            best_score = score
            best_text = txt
            # 완벽에 가까우면 조기 종료
            if score == 0:
                break

    if best_text is None:
        best_text = raw_bytes.decode("utf-8", errors="replace")

    # 최종 안전장치: NUL 제거 (PostgreSQL insert 방지)
    if "\x00" in best_text:
        best_text = best_text.replace("\x00", "")
    return best_text


async def process_text(
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
    logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [ENTRY] 함수 진입 성공 | Subject={subject}")
    try:
        logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [START] Subject={subject} | Table={table_name} | DB={dbname}")
        # content는 호출부에서 "파일 경로"로 넘어오는 경우가 있다(크롤링 워크플로우).
        # - PG의 content 컬럼에는 원본 식별자(content 인자)를 그대로 저장
        # - 청킹/임베딩/저장(text_data)은 실제 파일 본문을 사용
        text_body = content or ""
        try:
            if isinstance(content, str) and os.path.exists(content) and os.path.isfile(content):
                # 텍스트 파일 본문 읽기 (인코딩 폴백)
                raw_bytes = None
                with open(content, "rb") as f:
                    raw_bytes = f.read()
                text_body = _decode_bytes_best_effort(raw_bytes)
                logger.info(
                    "[LEARN-POSTGRES] [EDU-TEXT] [LOAD] file body loaded | path=%s bytes=%s",
                    content,
                    len(raw_bytes),
                )
        except Exception as read_exc:
            logger.warning(f"[LEARN-POSTGRES] [EDU-TEXT] [LOAD] failed to read file body: {read_exc}")

        # Postgres는 NUL 문자를 허용하지 않는다. (asyncpg CharacterNotInRepertoireError 방지)
        if "\x00" in text_body:
            text_body = text_body.replace("\x00", "")

        # 텍스트를 청크로 나누기
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=50)
        chunks = text_splitter.split_text(text_body)
        total_chunks = len(chunks)
        logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [CHUNKING] Total Chunks={total_chunks}")
        chunk_progress = round(each_progress / total_chunks, 2)
        
        # 개인정보 필터링 시 필터링된 청크들을 저장할 리스트
        filtered_chunks = []
        has_sensitive_data = False  # 개인정보가 감지된 청크가 있는지 확인
        
        logger.info(
            f"[conent: {subject}, total_chunk: {total_chunks}, 학습 시작\n job_id:{job_id} table_name: {table_name}]"
        )
        # 청크를 데이터베이스에 저장
        for idx, chunk in enumerate(chunks):
            logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [PROCESSING] Chunk {idx+1}/{total_chunks} 시작")
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [CANCEL] 작업 취소됨: job_id={job_id}")
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
                    
                    if pii_result["is_sensitive"]:
                        has_sensitive_data = True  # 개인정보 감지 플래그 설정
                    
                    # 개인정보 필터링이 적용된 모든 청크를 리스트에 저장
                    filtered_chunks.append(chunk)
                else:
                    # PII 검사 실패 시 에러 로깅
                    logger.error(f"PII 검사 실패: {pii_result.get('error', 'Unknown error')}")
                    # 실패 시 원본 텍스트 유지
            chunk_num = f"{idx + 1}"
            # 요청사항: text_data에는 실제 본문 내용(청크)을 그대로 저장
            # DB/임베딩 안전장치: NUL 제거
            if "\x00" in chunk:
                chunk = chunk.replace("\x00", "")
            chunk_with_metadata = chunk
            
            logger.debug(f"[LEARN-POSTGRES] [EDU-TEXT] [EMBEDDING] Chunk {idx+1} 생성 중")
            embedding = embedding_model.embed_query(chunk_with_metadata)

            # PostgreSQL vector 형식으로 변환
            embedding_array = f"[{','.join(map(str, embedding))}]"

            # ✅ DB 저장 데이터 구성 및 디버깅
            insert_payload = {
                "content": content,           # 파일 URL 기준으로 저장 (중복 체크 기준)
                "subject": subject,           # 파일명 표시용
                "chunk_num": chunk_num,
                "memo": memo,
                # ✅ 파일 학습은 content_type을 "file"로 통일
                "content_type": "file",
                "text_data": chunk_with_metadata,
                "embedding": embedding_array,
            }

            # 임베딩을 제외한 실제 저장 값 로그 출력 (print와 logger 병행)
            debug_payload = {k: v for k, v in insert_payload.items() if k != 'embedding'}
            print(f"[LEARN-POSTGRES] [EDU-TEXT] [DB-INSERT-DATA] Table={table_name} | Data={debug_payload}", flush=True)
            logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [DB-INSERT-DATA] Table={table_name} | Data={debug_payload}")

            # ✅ DB 저장 직전 취소 상태 확인
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [CANCEL] DB 저장 전 취소됨: job_id={job_id}, chunk={idx+1}")
                return {"chunks": idx, "status": "cancelled"}

            logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [DB-SAVE] Chunk {idx+1} 저장 시도")
            await insert_data(
                table=table_name,
                data=insert_payload,
                dbname=dbname,
            )
            logger.info(f"[LEARN-POSTGRES] [EDU-TEXT] [DB-SAVE-OK] Chunk {idx+1} 저장 완료")
            # 진행률 업데이트
            current_progress = await job_progress_manager.get_job_progress(job_id)
            new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
            await job_progress_manager.set_job_progress(job_id, new_progress)


            # logger.info(f"[Chunk_number: {idx + 1}]")

        logger.info(
            f"[conent: {subject}, total_chunk: {total_chunks}, 학습 완료 job_id:{job_id}, table_name: {table_name}]"
        )
        
        # ✅ 처리 완료 후 청크 수 반환
        result = {"chunks": total_chunks, "status": "success"}
        
        # ✅ 개인정보 필터링이 적용되었고 개인정보가 감지된 청크가 있는 경우 마스킹된 전체 텍스트 정보 포함
        if personal_info_filter == "Y" and has_sensitive_data and filtered_chunks:
            # 마스킹된 청크들만으로 구성된 전체 텍스트 (개인정보가 마스킹된 상태)
            result["full_filtered_text"] = "\n".join(filtered_chunks)
            logger.info(f"[TEXT_EDU] 마스킹된 전체 텍스트 생성: {len(filtered_chunks)}개 청크, 총 길이: {len(result['full_filtered_text'])}")
        
        return result

    except Exception as e:
        logger.error(f"텍스트 처리 중 오류 발생: {str(e)}")

        raise RuntimeError(f"텍스트 처리 중 오류 발생: {e}")
