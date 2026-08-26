from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator, Optional, Dict
from urllib.parse import quote_plus
from backend.shared.config import Config
import logging

# 濡쒓굅 ?ㅼ젙
logger = logging.getLogger("db.db_postgres")
if not logger.handlers:
    logger.setLevel(logging.INFO)

# 踰좎씠???대옒???앹꽦
"""
?ш린???앹꽦??Base ?대옒?ㅻ? ?곸냽諛쏆븘 ?앹꽦???뚯씠釉붿? ?숈씪??DB ?먯꽌 ?앹꽦?쒓쾬?쇰줈 媛꾩＜?섍쾶 ?⑸땲??
sqlalchemy 瑜??ъ슜?좊븣 ?숈씪???몄뒪?댁뒪??Base 瑜??ъ슜?댁빞吏留?table 媛?愿怨꾨? 留븐쓣 ???덇퀬
?④퍡 議고쉶媛 媛?ν빀?덈떎.
"""

# 鍮꾨룞湲??곗씠?곕쿋?댁뒪 URL (asyncpg ?쒕씪?대쾭 ?ъ슜)

DEFAULT_DB_NAME = getattr(Config, "POSTGRES_DB_NAME", getattr(Config, "DB_NAME", None))


def _build_database_url_async(db_name: Optional[str] = None, host: Optional[str] = None) -> str:
    target_db = db_name or DEFAULT_DB_NAME
    target_host = host or Config.POSTGRES_DB_HOST
    if not target_db:
        raise ValueError("PostgreSQL DB ?대쫫???ㅼ젙?섏? ?딆븯?듬땲?? Config.POSTGRES_DB_NAME ?먮뒗 db_name???뺤씤?섏꽭??")
    return (
        f"postgresql+asyncpg://{Config.POSTGRES_DB_USER}:{quote_plus(Config.POSTGRES_DB_PASSWORD)}@"
        f"{target_host}:{Config.POSTGRES_DB_PORT}/{target_db}"
    )


# 1. ?곗씠?곕쿋?댁뒪媛 議댁옱?섏? ?딆쓣 寃쎌슦 ?앹꽦?섎뒗 ?숆린 ?⑥닔

# def create_database_if_not_exists(db_name: Optional[str] = None):
#     target_db = db_name
#     default_database = "postgres"
#     default_url = (
#         f"postgresql://{Config.POSTGRES_DB_USER}:{quote_plus(Config.POSTGRES_DB_PASSWORD)}@"
#         f"{Config.POSTGRES_DB_HOST}:{Config.POSTGRES_DB_PORT}/{default_database}"
#     )

#     engine = create_engine(default_url, isolation_level="AUTOCOMMIT")

#     with engine.connect() as conn:
#         result = conn.execute(
#             text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
#             {"dbname": target_db},
#         )

#         exists = result.scalar() is not None

#         if not exists:
#             conn.execute(text(f"SCHEMA MUTATION {target_db}"))
#             logger.debug(f"??DATABASE '{target_db}' created")
#         else:
#             logger.debug(f"??DATABASE '{target_db}' already exists")


# 鍮꾨룞湲??붿쭊/?몄뀡 罹먯떆 諛??⑺넗由?
_engine_cache: Dict[str, any] = {}
_session_factory_cache: Dict[str, sessionmaker] = {}


def get_async_engine(db_name: Optional[str] = None, host: Optional[str] = None):
    target_db = db_name or DEFAULT_DB_NAME
    target_host = host or Config.POSTGRES_DB_HOST
    if not target_db:
        raise ValueError("PostgreSQL DB ?대쫫???ㅼ젙?섏? ?딆븯?듬땲?? Config.POSTGRES_DB_NAME ?먮뒗 db_name???뺤씤?섏꽭??")
    
    cache_key = f"{target_host}/{target_db}"
    if cache_key in _engine_cache:
        return _engine_cache[cache_key]
    
    # 而ㅻ꽖??? ?ㅼ젙 紐낆떆??吏??諛??쒗븳
    # pool_size? max_overflow瑜??쒗븳?섏뿬 怨쇰룄???곌껐 諛⑹?
    pool_size = min(getattr(Config, 'DB_POOL_MIN', 5), 10)  # 湲곕낯 ? ?ш린: 理쒕? 10?쇰줈 ?쒗븳
    max_overflow = min(getattr(Config, 'DB_POOL_MAX', 35), 20)  # 理쒕? ?ㅻ쾭?뚮줈?? 理쒕? 20?쇰줈 ?쒗븳
    
    logger.info(f"[PostgreSQL Engine] 엔진 생성 | Host={target_host} | DB={target_db} | pool_size={pool_size} | max_overflow={max_overflow}")
    
    pg_connect_timeout = float(getattr(Config, "POSTGRES_CONNECT_TIMEOUT_SEC", 5.0) or 5.0)
    pg_pool_timeout = float(getattr(Config, "POSTGRES_POOL_TIMEOUT_SEC", 5.0) or 5.0)

    engine = create_async_engine(
        _build_database_url_async(target_db, host=target_host),
        echo=False,
        pool_size=pool_size,  # 紐낆떆??? ?ш린 ?ㅼ젙 (?쒗븳??
        max_overflow=max_overflow,  # 紐낆떆???ㅻ쾭?뚮줈???ㅼ젙 (?쒗븳??
        pool_recycle=1800,  # 30遺꾨쭏???곌껐 ?ъ깮??
        pool_pre_ping=True,  # ?곌껐 ?좏슚???ъ쟾 ?뺤씤
        pool_timeout=pg_pool_timeout,
        connect_args={
            "server_settings": {
                "client_encoding": "utf8",
            },
            "command_timeout": 30,  # 紐낅졊 ?ㅽ뻾 ??꾩븘??(30珥? asyncpg 吏??
            "timeout": pg_connect_timeout,
        },
    )
    _engine_cache[cache_key] = engine
    return engine


def get_session_factory(db_name: Optional[str] = None, host: Optional[str] = None) -> sessionmaker:
    target_db = db_name or DEFAULT_DB_NAME
    target_host = host or Config.POSTGRES_DB_HOST
    if not target_db:
        raise ValueError("PostgreSQL DB ?대쫫???ㅼ젙?섏? ?딆븯?듬땲?? Config.POSTGRES_DB_NAME ?먮뒗 db_name???뺤씤?섏꽭??")
    
    cache_key = f"{target_host}/{target_db}"
    if cache_key in _session_factory_cache:
        return _session_factory_cache[cache_key]
    factory = sessionmaker(get_async_engine(target_db, host=target_host), expire_on_commit=False, class_=AsyncSession)
    _session_factory_cache[cache_key] = factory
    return factory


# 湲곕낯 ?붿쭊/?몄뀡 (?섏쐞 ?명솚 - property濡?吏??濡쒕뵫)
# 紐⑤뱢 ?덈꺼?먯꽌 利됱떆 ?앹꽦?섏? ?딄퀬, ?묎렐 ?쒖뿉留??앹꽦
# ?대젃寃??섎㈃ 遺덊븘?뷀븳 ?붿쭊 ?앹꽦??諛⑹??????덉쓬

class _LazyEngine:
    """吏??濡쒕뵫 ?붿쭊 (?섏쐞 ?명솚??"""
    def __init__(self):
        self._engine = None
    
    def __getattr__(self, name):
        if self._engine is None:
            self._engine = get_async_engine()
        return getattr(self._engine, name)
    
    def __call__(self, *args, **kwargs):
        if self._engine is None:
            self._engine = get_async_engine()
        return self._engine(*args, **kwargs)

class _LazySessionFactory:
    """吏??濡쒕뵫 ?몄뀡 ?⑺넗由?(?섏쐞 ?명솚??"""
    def __init__(self):
        self._factory = None
    
    def __call__(self, *args, **kwargs):
        if self._factory is None:
            self._factory = get_session_factory()
        return self._factory(*args, **kwargs)
    
    def __getattr__(self, name):
        if self._factory is None:
            self._factory = get_session_factory()
        return getattr(self._factory, name)

# ?섏쐞 ?명솚?깆쓣 ?꾪븳 ?꾨줈?쇳떚 (?묎렐 ?쒖뿉留??앹꽦)
engine_async = _LazyEngine()
async_session = _LazySessionFactory()


# 鍮꾨룞湲??몄뀡 ?섏〈???⑥닔 (?숈쟻 db_name 吏??
async def get_db(db_name: Optional[str] = None) -> AsyncGenerator[AsyncSession, None]:
    session_factory = get_session_factory(db_name)
    async with session_factory() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise e


# async with 援щЦ?쇰줈 ?명빐 ?몄뀡? ?먮룞?쇰줈 醫낅즺??

