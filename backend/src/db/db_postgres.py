from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator, Optional, Dict
from urllib.parse import quote_plus
from backend.config import Config

from ..utils.logging_util import LoggerSingleton
import logging

# 濡쒓굅 ?ㅼ젙
logger = LoggerSingleton.get_logger(logger_name="db.db_postgres", level=logging.INFO)

# 踰좎씠???대옒???앹꽦
"""
?ш린???앹꽦??Base ?대옒?ㅻ? ?곸냽諛쏆븘 ?앹꽦???뚯씠釉붿? ?숈씪??DB ?먯꽌 ?앹꽦?쒓쾬?쇰줈 媛꾩＜?섍쾶 ?⑸땲??
sqlalchemy 瑜??ъ슜?좊븣 ?숈씪???몄뒪?댁뒪??Base 瑜??ъ슜?댁빞吏留?table 媛?愿怨꾨? 留븐쓣 ???덇퀬
?④퍡 議고쉶媛 媛?ν빀?덈떎.
"""

# 鍮꾨룞湲??곗씠?곕쿋?댁뒪 URL (asyncpg ?쒕씪?대쾭 ?ъ슜)

DEFAULT_DB_NAME = getattr(Config, "POSTGRES_DB_NAME", getattr(Config, "DB_NAME", None))


def _build_database_url_async(db_name: Optional[str] = None) -> str:
    target_db = db_name or DEFAULT_DB_NAME
    if not target_db:
        raise ValueError("PostgreSQL DB ?대쫫???ㅼ젙?섏? ?딆븯?듬땲?? Config.POSTGRES_DB_NAME ?먮뒗 db_name???뺤씤?섏꽭??")
    return (
        f"postgresql+asyncpg://{Config.POSTGRES_DB_USER}:{quote_plus(Config.POSTGRES_DB_PASSWORD)}@"
        f"{Config.POSTGRES_DB_HOST}:{Config.POSTGRES_DB_PORT}/{target_db}"
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
#             logger.info(f"??DATABASE '{target_db}' created")
#         else:
#             logger.info(f"??DATABASE '{target_db}' already exists")


# 鍮꾨룞湲??붿쭊/?몄뀡 罹먯떆 諛??⑺넗由?
_engine_cache: Dict[str, any] = {}
_session_factory_cache: Dict[str, sessionmaker] = {}


def get_async_engine(db_name: Optional[str] = None):
    target_db = db_name or DEFAULT_DB_NAME
    if not target_db:
        raise ValueError("PostgreSQL DB ?대쫫???ㅼ젙?섏? ?딆븯?듬땲?? Config.POSTGRES_DB_NAME ?먮뒗 db_name???뺤씤?섏꽭??")
    if target_db in _engine_cache:
        return _engine_cache[target_db]
    engine = create_async_engine(
        _build_database_url_async(target_db),
        echo=False,
        pool_size=Config.DB_POOL_MIN,
        max_overflow=Config.DB_POOL_MAX,
        pool_recycle=1800,
        pool_pre_ping=True,
        connect_args={
            "server_settings": {
                "client_encoding": "utf8",
                "password_encryption": "md5",
            }
        },
    )
    _engine_cache[target_db] = engine
    return engine


def get_session_factory(db_name: Optional[str] = None) -> sessionmaker:
    target_db = db_name or DEFAULT_DB_NAME
    if not target_db:
        raise ValueError("PostgreSQL DB ?대쫫???ㅼ젙?섏? ?딆븯?듬땲?? Config.POSTGRES_DB_NAME ?먮뒗 db_name???뺤씤?섏꽭??")
    if target_db in _session_factory_cache:
        return _session_factory_cache[target_db]
    factory = sessionmaker(get_async_engine(target_db), expire_on_commit=False, class_=AsyncSession)
    _session_factory_cache[target_db] = factory
    return factory


# 湲곕낯 ?붿쭊/?몄뀡 (?섏쐞 ?명솚)
engine_async = get_async_engine()
async_session = get_session_factory()


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

