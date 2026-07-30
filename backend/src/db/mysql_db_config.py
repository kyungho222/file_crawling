import asyncio
import logging
from typing import Dict, Tuple, Optional, Any

import asyncmy
from asyncmy.cursors import DictCursor
from config import Config
from src.utils.logging_util import LoggerSingleton


logger = LoggerSingleton.get_logger(logger_name="db.mysql_db_config", level=logging.INFO)


async def mysql_execute_query(query, params=None, fetch=False, dbname=None):
    """
    SQL 쿼리를 실행하는 범용 함수 (MySQL/MariaDB 자동 라우팅).

    Args:
        query (str): 실행할 SQL 쿼리
        params (tuple | list | dict, optional): 바인딩 파라미터
        fetch (bool, optional): SELECT 여부. True면 결과 반환
        dbname (str, optional): 연결할 데이터베이스명 (필수)

    Returns:
        list | None: fetch=True일 경우 쿼리 결과(list[dict]) 반환, 아니면 None
    """
    if dbname is None:
        logger.error("Database name cannot be None")
        raise ValueError("Database name cannot be None")
    name = (str(dbname).strip().lower())
    if name != "chatty":
        from src.db.maria_db_config import maria_execute_query
        return await maria_execute_query(query, params=params, fetch=fetch, dbname=dbname)
    # chatty → asyncmy 풀 사용
    conn = None
    try:
        conn = await mysql_connect_db(dbname)
        if fetch:
            async with conn.cursor(DictCursor) as cursor:
                await cursor.execute(query, params or ())
                result = await cursor.fetchall()
                return [_normalize_row_unicode(row) for row in (result or [])]
        else:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())
            return None
    except Exception as e:
        logger.error(f"[MySQL] Database operation failed (dbname={dbname}): {e}")
        raise
    finally:
        if conn:
            await mysql_return_connection(conn, dbname)


def _decode_bytes_safely(value: bytes) -> str:
    """바이트 값을 안전하게 문자열로 디코딩한다. UTF-8 실패 시 한국어 환경 폴백 적용."""
    for enc in ("utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return value.decode(enc)
        except Exception:
            continue
    # 최종 폴백: 손실 허용
    return value.decode("utf-8", errors="replace")


def _normalize_row_unicode(row: Any) -> Any:
    """DictCursor 결과의 바이트 필드를 문자열로 변환한다."""
    if row is None:
        return row
    if isinstance(row, dict):
        return {k: (_decode_bytes_safely(v) if isinstance(v, (bytes, bytearray)) else v) for k, v in row.items()}
    if isinstance(row, (list, tuple)):
        converted: list[Any] = []
        for v in row:
            if isinstance(v, (bytes, bytearray)):
                converted.append(_decode_bytes_safely(v))
            else:
                converted.append(v)
        return type(row)(converted)  # 유지 가능한 경우 동일 타입으로 반환
    if isinstance(row, (bytes, bytearray)):
        return _decode_bytes_safely(row)
    return row


async def mysql_release_connection(conn, dbname=None):
    """커넥션을 풀에 반환하는 함수 (외부 직접 사용시)."""
    await MYSQL_DatabasePool.mysql_release_connection(conn, dbname)


class MYSQL_DatabasePool:
    _pools: Dict[str, Tuple[asyncmy.Pool, float]] = {}
    _lock = asyncio.Lock()
    _cleanup_task: Optional[asyncio.Task] = None

    @staticmethod
    def _get_mysql_credentials(dbname: Optional[str]):
        """dbname 값에 따라 MySQL 접속 정보를 선택한다.

        규칙:
        - naraone → GWI_* 크레덴셜 사용
        - chatty  → CHATTY_* 크레덴셜 사용
        - 그 외   → 지원하지 않음 (명시적 오류)
        환경변수도 동일 키로 폴백 지원.
        """
        name = (dbname or "").strip().lower()

        def pick(prefix: str):
            user = getattr(Config, f"{prefix}_MYSQL_USER", None)
            password = getattr(Config, f"{prefix}_MYSQL_PASSWORD", None)
            host = getattr(Config, f"{prefix}_MYSQL_HOST", None)
            port_val = getattr(Config, f"{prefix}_MYSQL_PORT", None)
            try:
                port = int(port_val)
            except Exception:
                port = 3306
            return user, password, host, port

        if name == "naraone":
            return pick("GWI")
        elif name == "chatty":
            return pick("CHATTY")
        else:
            # 지원하지 않는 db_name
            return None, None, None, 3306

    @staticmethod
    def _resolve_mysql_dbname(dbname: Optional[str]) -> str:
        """프론트 로지컬 이름을 실제 MySQL 스키마명으로 매핑한다.

        - naraone → Asadal_Chatbot (또는 Config.GWI_MYSQL_DBNAME, 없으면 'Asadal_Chatbot')
        - chatty  → chatty (또는 Config.CHATTY_MYSQL_DBNAME, 없으면 'chatty')
        - 그 외   → 오류
        """
        name = (dbname or "").strip().lower()
        if name == "naraone":
            return getattr(Config, 'GWI_MYSQL_DBNAME', 'Asadal_Chatbot')
        if name == "chatty":
            return getattr(Config, 'CHATTY_MYSQL_DBNAME', 'chatty')
        raise ValueError(f"지원되지 않는 db_name: {dbname}")

    @classmethod
    async def get_pool(cls, dbname=None):
        """요청된 dbname의 커넥션 풀 반환 또는 생성."""
        if dbname is None:
            logger.error("Database name cannot be None")
            raise ValueError("Database name cannot be None")

        db_name = dbname

        async with cls._lock:
            if db_name not in cls._pools:
                try:
                    actual_db = cls._resolve_mysql_dbname(db_name)
                    user, password, host, port = cls._get_mysql_credentials(db_name)

                    if not all([user, password, host]):
                        raise ValueError(f"지원되지 않는 db_name 또는 크레덴셜 누락: db_name={db_name}")

                    # 연결 문자셋/콜레이션 설정 (MySQL 5.0.x → utf8 권장)
                    charset = getattr(Config, 'MYSQL_CHARSET', None) or 'utf8'
                    collation = getattr(Config, 'MYSQL_COLLATION', None) or 'utf8_general_ci'

                    pool = await asyncmy.create_pool(
                        db=actual_db,
                        user=user,
                        password=password,
                        host=host,
                        port=port,
                        minsize=Config.DB_POOL_MIN,
                        maxsize=Config.DB_POOL_MAX,
                        charset=charset,
                        init_command=f"SET NAMES {charset} COLLATE {collation}",
                        autocommit=True,
                    )
                    cls._pools[db_name] = (pool, asyncio.get_event_loop().time())
                    logger.info(f"✅ MySQL 풀 생성 완료 logical='{db_name}' → actual='{actual_db}' ({host}:{port})")

                    if cls._cleanup_task is None or cls._cleanup_task.done():
                        cls._cleanup_task = asyncio.create_task(cls._auto_cleanup())

                except Exception as e:
                    logger.error(f"❌ MySQL 풀 생성 실패 {db_name} ({host}:{port}): {e}")
                    raise
            else:
                pool, _ = cls._pools[db_name]
                cls._pools[db_name] = (pool, asyncio.get_event_loop().time())

        return cls._pools[db_name][0]

    @classmethod
    async def _auto_cleanup(cls):
        """주기적으로 사용하지 않는 풀 정리."""
        while True:
            try:
                await asyncio.sleep(Config.DB_POOL_CHCK)
                await cls.release_unused_pools()
            except Exception as e:
                logger.error(f"자동 정리 중 오류: {e}")
            except asyncio.CancelledError:
                logger.info("자동 정리 태스크 종료")
                break

    @classmethod
    async def release_unused_pools(cls, timeout=None):
        """일정 시간 사용되지 않은 풀 해제."""
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
                        logger.info(f"MySQL 풀 해제 완료 {dbname}")
                    except Exception as e:
                        logger.error(f"MySQL 풀 해제 실패 {dbname}: {e}")

            for dbname in to_remove:
                del cls._pools[dbname]

    @classmethod
    async def close_all_pools(cls):
        """모든 풀 정리 (애플리케이션 종료 시 호출)."""
        logger.info(f"총 {len(cls._pools)}개의 MySQL 커넥션 풀을 정리합니다.")

        if cls._cleanup_task and not cls._cleanup_task.done():
            cls._cleanup_task.cancel()
            try:
                await cls._cleanup_task
            except asyncio.CancelledError:
                pass

        async with cls._lock:
            for dbname, (pool, _) in cls._pools.items():
                logger.info(f"MySQL 풀 닫기 시작: {dbname}")
                try:
                    pool.close()
                    await pool.wait_closed()
                    logger.info(f"✅ MySQL 풀 닫기 완료 {dbname}")
                except Exception as e:
                    logger.error(f"❌ MySQL 풀 닫기 실패: {dbname} - {e}")

            cls._pools.clear()
            logger.info("모든 MySQL 커넥션 풀 정리 완료")

    @classmethod
    def get_pool_status(cls):
        """현재 풀 상태 정보 반환."""
        current_time = asyncio.get_event_loop().time()
        status = {}

        for dbname, (pool, last_used) in cls._pools.items():
            age = current_time - last_used
            status[dbname] = {
                'pool_size': pool.size,
                'free_size': pool.freesize,
                'last_used_seconds_ago': age,
                'is_idle': age > Config.DB_POOL_CHCK,
            }

        return status

    @classmethod
    async def mysql_release_connection(cls, conn, dbname=None):
        """클래스 메서드: 커넥션을 풀에 반환 (호환용)."""
        try:
            if conn:
                pool = await cls.get_pool(dbname)
                pool.release(conn)
        except Exception as e:
            logger.error(f"MySQL 커넥션 반환 실패: {e}")


async def mysql_connect_db(dbname=None):
    """데이터베이스 연결 획득. dbname이 chatty가 아니면 MariaDB로 라우팅."""
    try:
        name = (str(dbname).strip().lower()) if dbname is not None else None
        if name and name != "chatty":
            from src.db.maria_db_config import maria_connect_db
            return await maria_connect_db(dbname)
        pool = await MYSQL_DatabasePool.get_pool(dbname)
        return await pool.acquire()
    except Exception as e:
        logger.error(f"MySQL 연결 실패: {e}")
        raise


async def mysql_return_connection(conn, dbname=None):
    """커넥션을 풀에 반환. dbname이 chatty가 아니면 MariaDB로 라우팅."""
    try:
        if conn:
            name = (str(dbname).strip().lower()) if dbname is not None else None
            if name and name != "chatty":
                from src.db.maria_db_config import maria_return_connection
                await maria_return_connection(conn, dbname)
                return
            db_name = dbname
            pool = await MYSQL_DatabasePool.get_pool(db_name)
            pool.release(conn)
    except Exception as e:
        logger.error(f"MySQL 커넥션 반환 실패: {e}")


async def mysql_executemany(query: str, params_list, dbname: Optional[str] = None) -> int:
    """배치 실행 헬퍼(executemany). 비-SELECT 전용."""
    if not dbname:
        raise ValueError("Database name cannot be None")
    name = (str(dbname).strip().lower())
    if name != "chatty":
        from src.db.maria_db_config import maria_connect_db, maria_return_connection
        conn = None
        try:
            conn = await maria_connect_db(dbname)
            async with conn.cursor() as cursor:
                await cursor.executemany(query, params_list or [])
            try:
                await conn.commit()
            except Exception:
                pass
            return len(params_list or [])
        finally:
            if conn:
                await maria_return_connection(conn, dbname)
    # chatty → asyncmy 풀 사용
    conn = None
    try:
        conn = await mysql_connect_db(dbname)
        # 트랜잭션 범위에서 실행
        try:
            await conn.autocommit(False)  # type: ignore[attr-defined]
        except Exception:
            pass
        async with conn.cursor() as cursor:
            await cursor.executemany(query, params_list or [])
        try:
            await conn.commit()
        except Exception:
            pass
        return len(params_list or [])
    except Exception as e:
        try:
            if conn:
                await conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if conn:
            try:
                await conn.autocommit(True)  # type: ignore[attr-defined]
            except Exception:
                pass
            await mysql_return_connection(conn, dbname)


async def mysql_cleanup_on_shutdown():
    """애플리케이션 종료 시 MySQL 풀 정리."""
    await MYSQL_DatabasePool.close_all_pools()
