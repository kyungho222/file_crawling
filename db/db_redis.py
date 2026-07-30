# db/db_redis.py
import asyncio
import redis.asyncio as redis
import logging
import json
import os
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)
CONCURRENT_CRAWL_LOG_PREFIX = "[ConcurrentCrawlStartDebug]"

def _agent_debug_log_redis(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # 파일 쓰기를 제거하고 로거로 대체합니다(운영에서 .cursor 파일 생성 방지).
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        logger.debug("AGENT_DEBUG %s", json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass

class RedisManager:
    """
    Redis 연결을 관리하는 싱글톤 클래스.
    `redis.asyncio`를 사용하여 비동기 연결을 처리합니다.
    Redis 클라이언트 인스턴스도 싱글톤으로 재사용합니다.
    """
    _instance = None
    _pool = None
    _loop = None
    _client = None  # 싱글톤 클라이언트 인스턴스

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(RedisManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance
    def _use_local_redis(self) -> bool:
        # USE_LOCAL_REDIS 또는 USE_LOCAL_DB 환경변수로 로컬 모드 판단
        return str(os.getenv("USE_LOCAL_REDIS", os.getenv("USE_LOCAL_DB", "0"))).strip().lower() in ("1", "true", "yes", "on")

    class _DummyRedisClient:
        """간단한 더미 Redis 클라이언트: 필요한 메서드는 최소한으로 구현"""
        async def ping(self) -> bool:
            return True
        async def aclose(self) -> None:
            return None
        async def publish(self, *args, **kwargs) -> int:
            return 0
        def __getattr__(self, name: str) -> Callable[..., Any]:
            async def _noop(*args, **kwargs):
                return None
            return _noop
    async def connect(self, host: str = None, port: int = None, db: int = 0):
        """
        Redis 연결 풀을 생성하고 연결 테스트를 진행합니다.
        환경 변수 REDIS_URL (예: redis://localhost:6379/0)이 설정되어 있으면 우선적으로 사용합니다.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except Exception:
            current_loop = None

        if self._client and self._loop is current_loop and (self._pool or self._use_local_redis()):
            return
        if self._client and self._loop is not current_loop:
            logger.warning(
                "%s[redis_loop_mismatch] resetting Redis client for current asyncio loop | old_loop=%s current_loop=%s",
                CONCURRENT_CRAWL_LOG_PREFIX,
                id(self._loop) if self._loop is not None else None,
                id(current_loop) if current_loop is not None else None,
            )
            self._pool = None
            self._client = None
            self._loop = None

        try:
            if self._use_local_redis():
                logger.info("Local Redis mode enabled - using dummy Redis client.")
                self._pool = None
                self._client = RedisManager._DummyRedisClient()
                self._loop = current_loop
                return

            # 환경 변수에서 REDIS_URL 로드
            env_redis_url = os.getenv("REDIS_URL")
            if env_redis_url:
                redis_url = env_redis_url
                logger.debug("Using Redis URL from environment: %s", redis_url)
            else:
                # 인자가 없으면 기존 하드코딩된 기본값 사용 (호환성 유지)
                h = host or "192.168.1.14"
                p = port or 6379
                redis_url = f"redis://{h}:{p}/{db}"
                logger.debug("Using Redis URL from arguments/default: %s", redis_url)

            # region agent log
            _agent_debug_log_redis(
                "H_REDIS_CONNECT_START",
                "db/db_redis.py:connect",
                "connect_start",
                {"url": redis_url},
            )
            # endregion

            try:
                max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "100") or "100")
            except Exception:
                max_connections = 100
            max_connections = max(20, min(max_connections, 500))
            try:
                socket_timeout = float(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", "2.0") or "2.0")
            except Exception:
                socket_timeout = 2.0
            socket_timeout = max(0.2, min(socket_timeout, 30.0))
            try:
                socket_connect_timeout = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT_SEC", "2.0") or "2.0")
            except Exception:
                socket_connect_timeout = 2.0
            socket_connect_timeout = max(0.2, min(socket_connect_timeout, 30.0))

            self._pool = redis.ConnectionPool.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=max_connections,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                health_check_interval=30,
            )
            
            # 싱글톤 클라이언트 인스턴스 생성
            self._client = redis.Redis(connection_pool=self._pool)
            self._loop = current_loop
            
            # 연결 테스트
            await self._client.ping()
            logger.debug(
                "%s[redis_connected] singleton client ready | loop=%s local_mode=%s",
                CONCURRENT_CRAWL_LOG_PREFIX,
                id(current_loop) if current_loop is not None else None,
                self._use_local_redis(),
            )
            
        except Exception as e:
            # region agent log
            _agent_debug_log_redis(
                "H_REDIS_CONNECT_FAIL",
                "db/db_redis.py:connect",
                "connect_failed",
                {"error": str(e)},
            )
            # endregion
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            self._pool = None
            self._client = None
            self._loop = None
            # 연결 실패 시, 어플리케이션 시작을 막기 위해 예외를 다시 발생시킴
            raise ConnectionError(f"Could not connect to Redis: {e}") from e

    async def disconnect(self):
        """
        Redis 연결 풀을 닫습니다.
        """
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None
            
        if self._pool:
            logger.debug("Closing Redis connection pool.")
            try:
                await self._pool.disconnect()
            except Exception:
                pass
            self._pool = None
        self._loop = None

    async def get_client(self) -> "redis.Redis":
        """
        싱글톤 Redis 클라이언트 인스턴스를 반환합니다.
        풀이 초기화되지 않은 경우 예외를 발생시킵니다.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except Exception:
            current_loop = None
        if self._client and self._loop is not current_loop:
            logger.warning(
                "%s[redis_loop_mismatch_get_client] reconnecting | old_loop=%s current_loop=%s",
                CONCURRENT_CRAWL_LOG_PREFIX,
                id(self._loop) if self._loop is not None else None,
                id(current_loop) if current_loop is not None else None,
            )
            self._pool = None
            self._client = None
            self._loop = None

        if not self._client or (not self._pool and not self._use_local_redis()):
            # 일반적으로 connect가 먼저 호출되어야 함
            # 만약 풀이 없다면, 다시 연결 시도 (Robustness)
            logger.debug("Redis pool not initialized. Attempting to connect now.")
            await self.connect()
        
        return self._client

    async def is_connected(self) -> bool:
        """
        Redis 서버에 연결되어 있는지 확인합니다.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except Exception:
            current_loop = None
        if not self._client or self._loop is not current_loop or (not self._pool and not self._use_local_redis()):
            return False
        try:
            await self._client.ping()
            return True
        except Exception:
            return False

# 전역 인스턴스
redis_manager = RedisManager()

# FastAPI 등에서 의존성 주입으로 사용될 함수
async def get_redis() -> "redis.Redis":
    """
    전역 RedisManager 인스턴스에서 Redis 클라이언트를 가져옵니다.
    """
    return await redis_manager.get_client()

# FastAPI의 startup/shutdown 이벤트에 연결할 함수
async def startup_redis():
    await redis_manager.connect()

async def shutdown_redis():
    await redis_manager.disconnect()


def describe_redis_connection(client) -> str:
    """
    Redis 클라이언트가 어느 호스트/DB에 연결되어 있는지 문자열로 반환한다.
    디버깅용 보조 함수.
    """
    try:
        pool = getattr(client, "connection_pool", None)
        if not pool:
            return "redis:unknown-pool"
        kwargs = getattr(pool, "connection_kwargs", {}) or {}
        host = kwargs.get("host", "?")
        port = kwargs.get("port", "?")
        db = kwargs.get("db", "?")
        ssl = kwargs.get("ssl", False)
        return f"redis://{host}:{port}/{db}?ssl={ssl}"
    except Exception as exc:
        return f"redis:describe-error:{exc}"
