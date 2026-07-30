import logging
from config.settings import connect_db, return_connection
from utils.logging_util import LoggerSingleton
from backend.shared.config import Config
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
import asyncio
from models.long_term_table_models import ConversationVector

logger = LoggerSingleton.get_logger(logger_name="db.db_operations", level=logging.INFO)


async def execute_query(query, params=None, fetch=False, dbname=None):
    """
    SQL 쿼리를 실행하는 범용 함수.

    Args:
        query (str): 실행할 SQL 쿼리
        params (tuple, optional): 쿼리에 전달할 매개변수. 기본값은 None.
        fetch (bool, optional): 데이터를 반환할지 여부. 기본값은 False.
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.

    Returns:
        list or None: fetch=True일 경우, 쿼리 결과를 반환.
    """
    conn = None
    try:
        conn = await connect_db(dbname)

        if fetch:
            result = await conn.fetch(query, *(params or ()))
        else:
            await conn.execute(query, *(params or ()))
            result = None

        return result
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            await return_connection(conn, dbname)


async def insert_data(table, data, dbname=None):
    """
    데이터 삽입 함수.

    Args:
        table (str): 테이블 이름.
        data (dict): 삽입할 데이터 (컬럼명: 값).
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.
    """
    keys = ", ".join(data.keys())
    placeholders = []
    for i, key in enumerate(data.keys()):
        if key == 'embedding':
            placeholders.append(f"${i+1}::vector(1536)")
        else:
            placeholders.append(f"${i+1}")
    
    placeholders_str = ", ".join(placeholders)
    # query = f"INSERT INTO {table} ({keys}, created_at) VALUES ({placeholders}, NOW()) returning id"
    query = f"INSERT INTO {table} ({keys}, created_at) VALUES ({placeholders_str}, NOW())"
    await execute_query(query, list(data.values()), dbname=dbname)


async def insert_data_with_metadata(table, data, dbname=None):
    """
    content_metadata JSONB 필드를 포함한 데이터 삽입 함수.

    Args:
        table (str): 테이블 이름.
        data (dict): 삽입할 데이터 (컬럼명: 값).
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.
    """
    # JSONB 필드 타입 변환
    processed_data = {}
    for key, value in data.items():
        if key == 'content_metadata' and value is not None:
            # content_metadata가 딕셔너리인 경우 JSON 문자열로 변환
            if isinstance(value, dict):
                try:
                    import json
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"JSONB 변환 실패: {key}={value}, 오류: {e}")
                    processed_data[key] = None
            elif isinstance(value, str):
                # 이미 문자열인 경우 그대로 사용
                processed_data[key] = value
            else:
                logger.warning(f"JSONB 예상치 못한 타입: {key}={type(value)}")
                processed_data[key] = None
        else:
            processed_data[key] = value
    
    keys = ", ".join(processed_data.keys())
    
    # JSONB 필드 및 임베딩 필드에 대한 명시적 캐스팅 추가
    placeholders = []
    for i, key in enumerate(processed_data.keys()):
        if key == 'content_metadata':
            placeholders.append(f"${i+1}::jsonb")  # JSONB 캐스팅
        elif key == 'embedding':
            placeholders.append(f"${i+1}::vector(1536)")  # Vector 차원 명시적 캐스팅
        else:
            placeholders.append(f"${i+1}")
    
    placeholders_str = ", ".join(placeholders)
    query = f"INSERT INTO {table} ({keys}, created_at) VALUES ({placeholders_str}, NOW())"
    await execute_query(query, list(processed_data.values()), dbname=dbname)


async def update_data(table, data, conditions, dbname=None):
    """
    데이터 업데이트 함수.
    """
    set_clauses = []
    for i, key in enumerate(data.keys(), 1):
        if key == 'embedding':
             set_clauses.append(f"{key} = ${i}::vector(1536)")
        else:
             set_clauses.append(f"{key} = ${i}")
    
    set_clause = ", ".join(set_clauses)
    
    where_clause = " AND ".join(
        f"{key} = ${i}" for i, key in enumerate(conditions.keys(), len(data) + 1)
    )
    query = f"UPDATE {table} SET {set_clause}, created_at = NOW() WHERE {where_clause}"
    params = list(data.values()) + list(conditions.values())
    await execute_query(query, params, dbname=dbname)


async def update_data_with_metadata(table, data, conditions, dbname=None):
    """
    content_metadata JSONB 필드를 포함한 데이터 업데이트 함수.

    Args:
        table (str): 테이블 이름.
        data (dict): 업데이트할 데이터 (컬럼명: 값).
        conditions (dict): 조건 필드와 값.
        dbname (str, optional): 연결할 데이터베이스 이름. 기본값은 None.
    """
    # JSONB 필드 타입 변환
    processed_data = {}
    for key, value in data.items():
        if key == 'content_metadata' and value is not None:
            # content_metadata가 딕셔너리인 경우 JSON 문자열로 변환
            if isinstance(value, dict):
                try:
                    import json
                    processed_data[key] = json.dumps(value, ensure_ascii=False)
                except Exception as e:
                    logger.warning(f"JSONB 변환 실패: {key}={value}, 오류: {e}")
                    processed_data[key] = None
            elif isinstance(value, str):
                # 이미 문자열인 경우 그대로 사용
                processed_data[key] = value
            else:
                logger.warning(f"JSONB 예상치 못한 타입: {key}={type(value)}")
                processed_data[key] = None
        else:
            processed_data[key] = value
    
    # SET 절에 JSONB 및 Vector 캐스팅 추가
    set_clauses = []
    for i, key in enumerate(processed_data.keys(), 1):
        if key == 'content_metadata':
            set_clauses.append(f"{key} = ${i}::jsonb")  # JSONB 캐스팅
        elif key == 'embedding':
            set_clauses.append(f"{key} = ${i}::vector(1536)")  # Vector 차원 명시적 캐스팅
        else:
            set_clauses.append(f"{key} = ${i}")
    
    set_clause = ", ".join(set_clauses)
    where_clause = " AND ".join(
        f"{key} = ${i}" for i, key in enumerate(conditions.keys(), len(processed_data) + 1)
    )
    query = f"UPDATE {table} SET {set_clause}, created_at = NOW() WHERE {where_clause}"
    params = list(processed_data.values()) + list(conditions.values())
    await execute_query(query, params, dbname=dbname)


async def delete_data(table, conditions, dbname=None):
    pass
    # """
    # 테이블에서 데이터를 삭제하기 전 파일 URL(content) 등을 기준으로 존재 여부를 확인하고 삭제하는 함수.
    # """
    # where_clause = " AND ".join(
    #     f"{key} = ${i+1}" for i, key in enumerate(conditions.keys())
    # )
    # params = list(conditions.values())
    
    # # URL 정보 추출 (로그용)
    # target_url = conditions.get('content') or conditions.get('url') or "Unknown URL"

    # try:
    #     conn = await connect_db(dbname)
    #     async with conn.transaction():
    #         # 1. URL 기반 존재 여부 및 건수 확인
    #         check_query = f"SELECT count(*) FROM {table} WHERE {where_clause}"
    #         existing_count = await conn.fetchval(check_query, *params)
            
    #         if existing_count == 0:
    #             logger.info(f"[Delete] 기존 데이터 없음(중복 아님): Table={table} | URL={target_url}")
    #             return False

    #         # 2. 삭제 수행 (중복 데이터 제거)
    #         query = f"DELETE FROM {table} WHERE {where_clause}"
    #         logger.info(f"[Delete] 중복 데이터 삭제 시도: URL={target_url} | Expected={existing_count} rows")
            
    #         result = await conn.execute(query, *params)
    #         # 삭제된 행 수 확인
    #         deleted_count = int(result.split(" ")[-1])  # "DELETE 1"에서 1 추출
    #         logger.info(f"[Delete] 삭제 완료: URL={target_url} | Deleted={deleted_count} rows")
            
    #         return deleted_count > 0
    # except Exception as e:
    #     logger.error(f"DELETE 쿼리 실패: {e}")
    #     return False
    # finally:
    #     if conn:
    #         await return_connection(conn, dbname)


# SqlAlchemy 로 데이터베이스 사용하기


async def db_insert(session: AsyncSession, model):
    """
    데이터베이스에 데이터를 삽입하는 비동기 함수 (최적화된 버전)
    """
    try:
        session.add(model)  # 모델 추가
        await session.commit()  # 커밋
        await session.refresh(model)  # PK 갱신
        return model
    except Exception as e:
        await session.rollback()  # 예외 발생 시 롤백
        print(f"❌ DB 삽입 오류 발생: {e}")
        raise


async def insert_summary_memory(
    session: AsyncSession,
    summary_model: ConversationVector,
    hypothetical_question_model: ConversationVector,
    count: int = 30,
):
    """
    롱텀 메모리 저장시 30개 쌍을 유지하면서 저장하는 sql
    """
    delete_sql = f"""
    DELETE FROM conversation_vector
    WHERE message_id IN (
        SELECT message_id FROM conversation_vector
        WHERE chat_id = :chat_id AND user_id = :user_id
        GROUP BY message_id
        ORDER BY MIN(timestamp) desc
        OFFSET {Config.LONG_TERM_COUNT-1}
    )
    """
    try:
        await session.execute(
            text(delete_sql),
            {"chat_id": summary_model.chat_id, "user_id": summary_model.user_id},
        )

        refresh_model = await db_insert(session=session, model=summary_model)

        hypothetical_question_model.reference_id = refresh_model.id

        await db_insert(session=session, model=hypothetical_question_model)

        return refresh_model
    except Exception as e:
        await session.rollback()
        logger.error(f"insert_summary_memory 오류 발생: {e}")
        raise


async def chunk_db_insert(session: AsyncSession, model, response_chunks):
    """
    데이터베이스에 데이터를 삽입하는 비동기 함수 (최적화된 버전)
    """
    try:
        while len(response_chunks) == 0:
            await asyncio.sleep(0.1)

        await asyncio.sleep(0.5)

        # 전체 응답
        full_response = "".join(response_chunks)

        model.message = full_response

        await db_insert(session=session, model=model)

    except Exception as e:
        logger.error(f"chunk_db_insert 오류 발생: {e}")


async def get_message_by_id(session: AsyncSession, message_id: int, model):
    """
    데이터베이스에서 메시지를 조회하는 비동기 함수 (최적화된 버전)
    """
    try:
        result = await session.execute(
            select(model).where(model.message_id == message_id)
        )

        messages = result.scalar_one_or_none()
        if messages.is_selected:
            raise ValueError(f"message_id : {message_id} already selected")

        if messages:
            messages.is_selected = True
            await session.commit()
            await session.refresh(messages)
            return messages.message, messages.timestamp
        else:
            raise ValueError(f"message_id : {message_id} no message found")

    except Exception as e:
        await session.rollback()  # 예외 발생 시 롤백
        print(f"❌ DB 조회 오류 발생: {e}")
        raise


async def delete_data_alchemy(session: AsyncSession, table_name: str, elements: dict):
    """
    데이터베이스에서 데이터를 삭제하는 비동기 함수
    """
    try:
        message_ids = elements["message_ids"]
        # message_id 키를 제거
        elements_without_message_id = {k: v for k, v in elements.items() if k != "message_ids"}
        
        # 다른 조건들에 대한 where 절 구성
        where_conditions = " AND ".join([f"{key} = :{key}" for key in elements_without_message_id.keys()])
        
        # message_id IN (...) 조건 추가 - 수정된 부분
        placeholders = ", ".join([f":message_id_{i}" for i in range(len(message_ids))])
        delete_sql = f"DELETE FROM {table_name} WHERE {where_conditions} AND message_id IN ({placeholders})"
        
        # 파라미터에 개별 message_id 값 추가 - 수정된 부분
        params = {**elements_without_message_id}
        for i, msg_id in enumerate(message_ids):
            params[f"message_id_{i}"] = msg_id
        
        await session.execute(text(delete_sql), params)
        await session.commit()

    except Exception as e:
        await session.rollback()
        logger.error(f"delete_data_alchemy 오류 발생: {e}")
        raise
