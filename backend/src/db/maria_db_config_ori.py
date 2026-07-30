from config import Config
import asyncio
import aiomysql
import logging
from logs.logging_util import LoggerSingleton
import time
from typing import Dict, Tuple, Optional

logger = LoggerSingleton.get_logger(logger_name="db.maria_db_config", level=logging.INFO)


class DatabasePool:
    _pools: Dict[str, Tuple[aiomysql.Pool, float]] = {}
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
                    pool = await aiomysql.create_pool(
                        db=db_name,
                        user=Config.MARIA_DB_USER,
                        password=Config.MARIA_DB_PASSWORD,
                        host=Config.MARIA_DB_HOST,
                        port=Config.MARIA_DB_PORT,
                        minsize=Config.DB_POOL_MIN,
                        maxsize=Config.DB_POOL_MAX,
                        charset='utf8mb4',
                        autocommit=True
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
        """
        일정 시간 동안 사용되지 않은 풀을 해제한다.
        """
        if timeout is None:
            timeout = Config.DB_POOL_CHCK
            
        async with cls._lock:
            current_time = time.time()
            to_remove = []

            for dbname, (pool, last_used) in cls._pools.items():
                if current_time - last_used > timeout:
                    try:
                        pool.close()
                        await pool.wait_closed()
                        to_remove.append(dbname)
                        logger.info(f"풀 해제 완료 {dbname}")
                    except Exception as e:
                        logger.error(f"풀 해제 실패 {dbname}: {e}")

            for dbname in to_remove:
                del cls._pools[dbname]

    @classmethod
    async def close_all_pools(cls):
        """
        모든 풀을 정리 (애플리케이션 종료 시 호출)
        """
        logger.info(f"총 {len(cls._pools)}개의 커넥션 풀을 정리합니다.")
        
        # 자동 정리 태스크 중지
        if cls._cleanup_task and not cls._cleanup_task.done():
            cls._cleanup_task.cancel()
            try:
                await cls._cleanup_task
            except asyncio.CancelledError:
                pass

        async with cls._lock:
            for dbname, (pool, _) in cls._pools.items():
                logger.info(f"풀 닫기 시작: {dbname}")
                try:
                    pool.close()
                    await pool.wait_closed()
                    logger.info(f"✅ 풀 닫기 완료 {dbname}")
                except Exception as e:
                    logger.error(f"❌ 풀 닫기 실패: {dbname} - {e}")

            cls._pools.clear()
            logger.info("모든 커넥션 풀 정리 완료")

    @classmethod
    def get_pool_status(cls):
        """
        현재 풀 상태 정보 반환
        """
        current_time = time.time()
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


async def connect_db(dbname=None):
    """
    데이터베이스 연결 함수.
    """
    try:
        pool = await DatabasePool.get_pool(dbname)
        return await pool.acquire()
    except Exception as e:
        logger.error(f"데이터베이스 연결 실패: {e}")
        raise


async def return_connection(conn, dbname=None):
    """
    커넥션을 풀에 반환하는 함수.
    """
    try:
        if conn:
            db_name = dbname
            pool = await DatabasePool.get_pool(db_name)
            pool.release(conn)
    except Exception as e:
        logger.error(f"커넥션 반환 실패: {e}")


# 애플리케이션 종료 시 풀 정리를 위한 이벤트 핸들러
async def cleanup_on_shutdown():
    """
    애플리케이션 종료 시 호출할 정리 함수
    """
    await DatabasePool.close_all_pools()
