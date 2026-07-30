from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from typing import AsyncGenerator
from urllib.parse import quote_plus
from config import Config

from src.utils.logging_util import LoggerSingleton
import logging

# 濡쒓굅 ?ㅼ젙
logger = LoggerSingleton.get_logger(logger_name="db.db_postgres", level=logging.INFO)

# 踰좎씠???대옒???앹꽦
"""
?ш린???앹꽦??Base ?대옒?ㅻ? ?곸냽諛쏆븘 ?앹꽦???뚯씠釉붿? ?숈씪??DB ?먯꽌 ?앹꽦?쒓쾬?쇰줈 媛꾩＜?섍쾶 ?⑸땲??
sqlalchemy 瑜??ъ슜?좊븣 ?숈씪???몄뒪?댁뒪??Base 瑜??ъ슜?댁빞吏留?table 媛?愿怨꾨? 留븐쓣 ???덇퀬
?④퍡 議고쉶媛 媛?ν빀?덈떎.
"""
Base = declarative_base()

# 鍮꾨룞湲??곗씠?곕쿋?댁뒪 URL (asyncpg ?쒕씪?대쾭 ?ъ슜)
DATABASE_URL_ASYNC = f"postgresql+asyncpg://{Config.DB_USER}:{quote_plus(Config.DB_PASSWORD)}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"

# ?숆린 ?곗씠?곕쿋?댁뒪 URL (湲곗〈 crud.py ?명솚??
DATABASE_URL_SYNC = f"postgresql://{Config.DB_USER}:{quote_plus(Config.DB_PASSWORD)}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"


# 1. ?곗씠?곕쿋?댁뒪媛 議댁옱?섏? ?딆쓣 寃쎌슦 ?앹꽦?섎뒗 ?숆린 ?⑥닔
def create_database_if_not_exists():
    default_database = "postgres"
    default_url = f"postgresql://{Config.DB_USER}:{quote_plus(Config.DB_PASSWORD)}@{Config.DB_HOST}:{Config.DB_PORT}/{default_database}"

    # AUTOCOMMIT 紐⑤뱶濡??붿쭊 ?앹꽦 (SCHEMA MUTATION 紐낅졊? ?몃옖??뀡 ?몃??먯꽌 ?ㅽ뻾?섏뼱????
    engine = create_engine(default_url, isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
            {"dbname": Config.DB_NAME},
        )

        exists = result.scalar() is not None

        if not exists:
            logger.warning("[Postgres] runtime database creation is disabled | db=%s", Config.DB_NAME)
            logger.info(f"??DATABASE '{Config.DB_NAME}' created")
        else:
            logger.info(f"??DATABASE '{Config.DB_NAME}' already exists")


# 鍮꾨룞湲??붿쭊 ?앹꽦
engine_async = create_async_engine(
    DATABASE_URL_ASYNC,
    echo=True,  # SQL 濡쒓렇 異쒕젰 (媛쒕컻/?붾쾭源????좎슜)
    pool_size=Config.DB_POOL_MIN,  # 湲곕낯 ????좎????곌껐????
    max_overflow=Config.DB_POOL_MAX,  # 湲곕낯 ???珥덇낵?섏뿬 ?앹꽦?????덈뒗 理쒕? ?곌껐 ??
    # pool_timeout=30,        # ?ъ슜 媛?ν븳 ?곌껐???놁쓣 寃쎌슦 ?湲고븷 理쒕? ?쒓컙(珥?
    pool_recycle=1800,  # ?쇱젙 ?쒓컙 ?댄썑 ?곌껐???ъ깮?깊븯??stale connection 諛⑹?
    pool_pre_ping=True,  # ?곌껐 ?좏슚??寃?щ줈 鍮꾩젙???곌껐 ?쒓굅
    connect_args={
        "server_settings": {
            "client_encoding": "utf8",
            "password_encryption": "md5",
        }
    },
)

# 鍮꾨룞湲??몄뀡 ?⑺넗由??앹꽦
async_session = sessionmaker(engine_async, expire_on_commit=False, class_=AsyncSession)

# ?숆린 ?붿쭊 諛??몄뀡 (湲곗〈 湲곕뒫 ?곌껐??
engine_sync = create_engine(
    DATABASE_URL_SYNC,
    echo=True,
    pool_size=Config.DB_POOL_MIN,
    max_overflow=Config.DB_POOL_MAX,
    pool_recycle=1800,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine_sync,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)


# 鍮꾨룞湲??몄뀡 ?섏〈???⑥닔
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise e

# async with 援щЦ?쇰줈 ?명빐 ?몄뀡? ?먮룞?쇰줈 醫낅즺??



