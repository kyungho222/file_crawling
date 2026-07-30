from config import Config
import asyncio
import asyncmy
from asyncmy.cursors import DictCursor
import logging
from src.utils.logging_util import LoggerSingleton
import time
from typing import Dict, Tuple, Optional

logger = LoggerSingleton.get_logger(logger_name="db.maria_db_config", level=logging.INFO)


async def maria_execute_query(query, params=None, fetch=False, dbname=None):
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
        # ✅ connect_db 사용해서 연결 가져오기
        conn = await maria_connect_db(dbname)
        # logger.info(f"[Maria DB] Executing query: {query} | Params: {params}")

        if fetch:
            # SELECT 쿼리 실행
            async with conn.cursor(DictCursor) as cursor:
                await cursor.execute(query, params or ())
                result = await cursor.fetchall()
        else:
            # INSERT/UPDATE/DELETE 쿼리 실행
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())
                result = None

        return result
    except Exception as e:
        logger.error(f"[Maria DB] Database operation failed: {e}")
        raise
    finally:
        if conn:
            # ✅ maria_return_connection 사용해서 연결 반환
            await maria_return_connection(conn, dbname)

async def maria_release_connection(conn, dbname=None):
    """
    커넥션을 풀에 반환하는 함수.
    """
    await MARIADB_DatabasePool.maria_release_connection(conn, dbname)

class MARIADB_DatabasePool:
    _pools: Dict[str, Tuple[object, float]] = {}
    _lock = asyncio.Lock()
    _cleanup_task: Optional[asyncio.Task] = None

    @classmethod
    async def get_pool(cls, dbname=None):
        """
        요청이 들어오면 해당 dbname에 대한 커넥션 풀을 가져오거나 생성한다.
        """
        if dbname is None:
            logger.error("Database name cannot be None")
            raise ValueError("Database name cannot be None")
        
        db_name = dbname
        
        async with cls._lock:
            # 풀이 없으면 생성
            if db_name not in cls._pools:
                try:
                    pool = await asyncmy.create_pool(
                        db=db_name,
                        user=Config.MARIA_DB_USER,
                        password=Config.MARIA_DB_PASSWORD,
                        host=Config.MARIA_DB_HOST,
                        port=Config.MARIA_DB_PORT,
                        minsize=Config.DB_POOL_MIN,
                        maxsize=Config.DB_POOL_MAX,
                        charset='utf8mb4',
                        autocommit=True,
                        connect_timeout=10,  # ✅ 연결 타임아웃 10초
                    )
                    cls._pools[db_name] = (pool, time.time())
                    logger.info(f"✅ 데이터베이스 풀 생성 완료 {db_name}")
                    
                    # 자동 정리 태스크 시작 (한 번만)
                    if cls._cleanup_task is None or cls._cleanup_task.done():
                        cls._cleanup_task = asyncio.create_task(cls._auto_cleanup())
                        
                except Exception as e:
                    logger.error(f"❌ 데이터베이스 풀 생성 실패 {db_name}: {e}")
                    raise
            else:
                # 마지막 사용 시간만 업데이트
                pool, _ = cls._pools[db_name]
                cls._pools[db_name] = (pool, time.time())
        
        return cls._pools[db_name][0]

    @classmethod
    async def _auto_cleanup(cls):
        """
        백그라운드에서 주기적으로 사용하지 않는 풀을 정리
        """
        while True:
            try:
                await asyncio.sleep(Config.DB_POOL_CHCK)  # 10분마다 체크
                await cls.release_unused_pools()
            except Exception as e:
                logger.error(f"자동 정리 중 오류: {e}")
            except asyncio.CancelledError:
                logger.info("자동 정리 태스크 종료")
                break

    @classmethod
    async def release_unused_pools(cls, timeout=None):
        if timeout is None:
            timeout = Config.DB_POOL_CHCK
        async with cls._lock:
            current_time = asyncio.get_event_loop().time()
            to_remove = []
            for dbname, (pool, last_used) in cls._pools.items():
                if current_time - last_used > timeout:
                    try:
                        pool.close()
                        await pool.wait_closed()
                        to_remove.append(dbname)
                        logger.info(f"데이터베이스 풀 해제 완료 {dbname}")
                    except Exception as e:
                        logger.error(f"데이터베이스 풀 해제 실패 {dbname}: {e}")
            for dbname in to_remove:
                del cls._pools[dbname]

    @classmethod
    def get_pool_status(cls):
        """
        현재 풀 상태 정보 반환.
        """
        current_time = asyncio.get_event_loop().time()
        status = {}
        
        for dbname, (pool, last_used) in cls._pools.items():
            age = current_time - last_used
            status[dbname] = {
                'pool_size': pool.size,
                'free_size': pool.freesize,
                'last_used_seconds_ago': age,
                'is_idle': age > Config.DB_POOL_CHCK
            }
        
        return status


async def maria_connect_db(dbname=None):
    """
    데이터베이스 연결 함수.
    """
    try:
        pool = await MARIADB_DatabasePool.get_pool(dbname)
        return await pool.acquire()
    except Exception as e:
        logger.error(f"데이터베이스 연결 실패: {e}")
        raise


async def maria_return_connection(conn, dbname=None):
    """
    커넥션을 풀에 반환하는 함수.
    """
    try:
        if conn:
            db_name = dbname
            pool = await MARIADB_DatabasePool.get_pool(db_name)
            pool.release(conn)
    except Exception as e:
        logger.error(f"커넥션 반환 실패: {e}")


# 애플리케이션 종료 시 풀 정리를 위한 이벤트 핸들러
async def maria_cleanup_on_shutdown():
    """
    애플리케이션 종료 시 호출할 정리 함수
    """
    await MARIADB_DatabasePool.close_all_pools()


async def test_maria_connection(dbname: str = None) -> bool:
    """
    MariaDB 연결 테스트 (비동기)
    
    Args:
        dbname: 연결 테스트할 데이터베이스명 (None이면 Config.MARIA_DB_HOST 사용)
    
    Returns:
        bool: 연결 성공 시 True, 실패 시 False
    """
    if dbname is None:
        dbname = "dev_user"  # 기본값
    
    try:
        logger.info("")
        logger.info("=" * 60)
        logger.info("🔍 [MariaDB] 연결 테스트 시작")
        logger.info("=" * 60)
        logger.info(f"   연결 정보:")
        logger.info(f"      • Host: {Config.MARIA_DB_HOST}")
        logger.info(f"      • Port: {Config.MARIA_DB_PORT}")
        logger.info(f"      • User: {Config.MARIA_DB_USER}")
        logger.info(f"      • Database: {dbname}")
        logger.info(f"   시도 URL: mysql://{Config.MARIA_DB_USER}:***@{Config.MARIA_DB_HOST}:{Config.MARIA_DB_PORT}/{dbname}")
        logger.info("=" * 60)
        
        # 간단한 연결 테스트 (풀 생성 전)
        test_conn = None
        try:
            test_conn = await asyncmy.connect(
                db=dbname,
                user=Config.MARIA_DB_USER,
                password=Config.MARIA_DB_PASSWORD,
                host=Config.MARIA_DB_HOST,
                port=Config.MARIA_DB_PORT,
                connect_timeout=10,  # ✅ 연결 타임아웃 10초
            )
            async with test_conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                result = await cursor.fetchone()
                if result:
                    logger.info("✅ MariaDB 연결 테스트 성공")
                    logger.info("=" * 60)
                    return True
                else:
                    logger.error("❌ MariaDB 연결 테스트 실패: 쿼리 결과 없음")
                    return False
        finally:
            if test_conn:
                test_conn.close()
                await test_conn.ensure_closed()
                
    except asyncio.TimeoutError as timeout_error:
        logger.error("")
        logger.error("=" * 60)
        logger.error("❌ MariaDB 연결 테스트 실패: 타임아웃")
        logger.error("=" * 60)
        logger.error(f"   오류 타입: {type(timeout_error).__name__}")
        logger.error(f"   오류 메시지: {str(timeout_error)}")
        logger.error("")
        logger.error(f"   연결 정보:")
        logger.error(f"      • Host: {Config.MARIA_DB_HOST}")
        logger.error(f"      • Port: {Config.MARIA_DB_PORT}")
        logger.error(f"      • User: {Config.MARIA_DB_USER}")
        logger.error(f"      • Database: {dbname}")
        logger.error(f"   실제 시도 URL: mysql://{Config.MARIA_DB_USER}:***@{Config.MARIA_DB_HOST}:{Config.MARIA_DB_PORT}/{dbname}")
        logger.error("")
        logger.error("   💡 타임아웃 원인:")
        logger.error("      1. MariaDB 서버가 실행 중이지만 응답하지 않음")
        logger.error("      2. 방화벽이 연결을 차단하고 있음")
        logger.error("      3. 네트워크 경로 문제")
        logger.error("      4. MariaDB 서버의 max_connections 초과")
        logger.error("")
        logger.error("   🔍 확인 방법:")
        logger.error(f"      • 네트워크 연결 테스트: telnet {Config.MARIA_DB_HOST} {Config.MARIA_DB_PORT}")
        logger.error(f"      • 또는: nc -zv {Config.MARIA_DB_HOST} {Config.MARIA_DB_PORT}")
        logger.error("      • MariaDB 서버 실행 상태: sudo systemctl status mariadb (또는 mysql)")
        logger.error("=" * 60)
        return False
    except Exception as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error("❌ MariaDB 연결 테스트 실패")
        logger.error("=" * 60)
        logger.error(f"   오류 타입: {type(e).__name__}")
        logger.error(f"   오류 메시지: {str(e)}")
        logger.error(f"   오류 상세: {repr(e)}")
        logger.error("")
        logger.error(f"   연결 정보:")
        logger.error(f"      • Host: {Config.MARIA_DB_HOST}")
        logger.error(f"      • Port: {Config.MARIA_DB_PORT}")
        logger.error(f"      • User: {Config.MARIA_DB_USER}")
        logger.error(f"      • Database: {dbname}")
        logger.error(f"   실제 시도 URL: mysql://{Config.MARIA_DB_USER}:***@{Config.MARIA_DB_HOST}:{Config.MARIA_DB_PORT}/{dbname}")
        logger.error("=" * 60)
        import traceback
        logger.error("   스택 트레이스:")
        logger.error(f"{traceback.format_exc()}")
        logger.error("=" * 60)
        return False