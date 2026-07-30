import logging
import asyncio
from logs.logging_util import LoggerSingleton
from db.maria_db_config import connect_db, return_connection

# 로거 설정
logger = LoggerSingleton.get_logger(
    logger_name="utils.get_maria", level=logging.INFO
)

async def get_history_maria(chat_bot_id: str, db_name: str, user_id: str, turn: int = 3):
    """
    챗봇 ID, 데이터베이스 이름, 사용자 ID를 입력받아 해당 챗봇의 사용자 대화 내역을 반환합니다.
    
    Args:
        chat_bot_id (str): 챗봇 ID 
        db_name (str): 데이터베이스 이름
        user_id (str): 사용자 ID
        turn (int): 가져올 대화 쌍의 수 (기본값: 3)
        
    Returns:
        list: 대화 내역 리스트 [{'content': '내용', 'type': '타입', 'created_at': '생성시간', 'text_data': '텍스트데이터'}, ...]
    """
    try:
        # 챗봇 ID에서 마지막 문자열 추출
        bot_suffix = chat_bot_id.split('-')[-1]
        # 테이블명 생성
        table_name = f"ASADAL_{bot_suffix}_CHATING_PROCESS"
        
        logger.info(f"MariaDB 조회 시작 - 테이블: {table_name}, 사용자: {user_id}, turn: {turn}")
        
        conn = None
        try:
            conn = await connect_db(db_name)
            async with conn.cursor() as cursor:
                # 최근 대화 내역을 turn 수만큼 가져오는 쿼리
                query = f"""
                    SELECT content, type, created_at, text_data
                    FROM {table_name}
                    WHERE mb_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """
                
                await cursor.execute(query, (user_id, turn * 2))  # 질문-답변 쌍이므로 turn * 2
                results = await cursor.fetchall()
                
                logger.info(f"MariaDB 원본 결과: {len(results)}개 행 조회됨")
                
                # 결과를 딕셔너리 형태로 변환
                formatted_results = []
                for i, row in enumerate(results):
                    formatted_row = {
                        'content': row[0],
                        'type': row[1],
                        'created_at': row[2],
                        'text_data': row[3]
                    }
                    formatted_results.append(formatted_row)
                    
                    # 디버깅: 각 행의 데이터 구조 확인
                    logger.debug(f"행 {i+1}: content='{str(row[0])[:50]}...', type='{row[1]}', created_at='{row[2]}', text_data='{str(row[3])[:50] if row[3] else None}...'")
                
                logger.info(f"챗봇 ID {chat_bot_id}의 대화 내역 조회 성공: {len(formatted_results)}개 결과")
                logger.info(f"데이터 가공 완료 - 첫 번째 결과: {formatted_results[0] if formatted_results else 'None'}")
                
                return formatted_results
                
        finally:
            if conn:
                await return_connection(conn, db_name)
    
    except Exception as e:
        logger.error(f"챗봇 ID {chat_bot_id}의 대화 내역 조회 중 오류 발생: {e}")
        raise
