
import os
import pymysql
import logging
import asyncio
from logs.logging_util import LoggerSingleton
from dotenv import load_dotenv

# 로거 설정
logger = LoggerSingleton.get_logger(
    logger_name="utils.get_mysql", level=logging.INFO
)

load_dotenv()

def get_mysql_connection_crawl(database: str | None = None):
    """
    MySQL 데이터베이스 연결을 생성하는 함수
    
    Returns:
        pymysql.Connection: MySQL 데이터베이스 연결 객체
    """
    try:
        host = os.getenv("MYSQL_HOST")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASS")
        port_str = os.getenv("MYSQL_PORT")
        port = int(port_str)

        database = database
        # logger.info(f"MYSQL_HOST: {host}\nMYSQL_USER: {user}\nMYSQL_PASS: {password}\nMYSQL_PORT: {port}\nMYSQL_DB: {database}")

        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset="utf8",
            autocommit=True,
            ssl=None,  # PyMySQL 0.9.3에서 SSL 비활성화
            use_unicode=True,
            read_timeout=30,
            write_timeout=30,
        )
        logger.info("MySQL 데이터베이스 연결 성공")
        return conn
    except Exception as e:
        logger.error(f"MySQL 연결 중 오류 발생: {str(e)}")
        raise

def get_mysql_connection():
    """
    MySQL 데이터베이스 연결을 생성하는 함수

    Returns:
        pymysql.Connection: MySQL 데이터베이스 연결 객체
    """
    try:
        conn = pymysql.connect(
            host="110.45.146.45",
            user="Asadal_Gwi",
            password="aodnsrhkddjchqkq@",
            database="Asadal_Chatbot",
            port=3306,
            charset="utf8",
        )
        logger.info("MySQL 데이터베이스 연결 성공")
        return conn
    except Exception as e:
        logger.error(f"MySQL 연결 중 오류 발생: {str(e)}")
        raise





async def get_history_msyql(chat_bot_id: str, db_name: str, user_id: str, turn: int = 3):
    """
    챗봇 ID, 데이터베이스 이름, 사용자 ID를 입력받아 해당 챗봇의 사용자 대화 내역을 반환합니다.
    
    Args:
        chat_bot_id (str): 챗봇 ID 
        db_name (str): 데이터베이스 이름
        user_id (str): 사용자 ID
        turn (int): 가져올 대화 쌍의 수 (기본값: 3)
        
    Returns:
        list: 대화 내역 리스트 [(질문1, 답변1), (질문2, 답변2), ...]
    """
    try:
        # 챗봇 ID에서 마지막 문자열 추출
        bot_suffix = chat_bot_id.split('-')[-1]
        # 테이블명 생성
        table_name = f"ASADAL_{bot_suffix}_CHATING_PROCESS"
        
        loop = asyncio.get_event_loop()
        
        def fetch_history_ask():
            try:
                conn = get_mysql_connection()
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                
                # 최근 대화 내역을 turn 수만큼 가져오는 쿼리
                query = f"""
                    SELECT content, type, created_at, text_data
                    FROM {table_name}
                    WHERE mb_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """
                
                cursor.execute(query, (user_id, turn * 2))  # 질문-답변 쌍이므로 turn * 2
                results = cursor.fetchall()
                
                return results
                
            except Exception as e:
                logger.error(f"대화 내역 조회 중 오류 발생: {e}")
                raise
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
        
        return await loop.run_in_executor(None, fetch_history_ask)
    
    except Exception as e:
        logger.error(f"챗봇 ID {chat_bot_id}의 대화 내역 조회 중 오류 발생: {e}")
        raise