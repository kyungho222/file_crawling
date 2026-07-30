"""
?곗씠?곕쿋?댁뒪 ?곌껐 ?ㅼ젙 (梨쀫큸蹂?DB 遺꾨━ 吏??
"""
import os
import logging
from typing import Optional, Dict
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Base ?대옒??
Base = declarative_base()

# 梨쀫큸蹂??붿쭊 諛??몄뀡 罹먯떆
_engines: Dict[str, any] = {}
_session_makers: Dict[str, sessionmaker] = {}


def get_database_url(chat_bot_id: Optional[str] = None) -> str:
    """
    梨쀫큸 ID???곕씪 ?곸젅???곗씠?곕쿋?댁뒪 URL 諛섑솚
    
    Args:
        chat_bot_id: 梨쀫큸 ID (?놁쑝硫?湲곕낯 DB ?ъ슜)
        
    Returns:
        ?곗씠?곕쿋?댁뒪 ?곌껐 URL
    """
    if chat_bot_id:
        # 梨쀫큸蹂??곗씠?곕쿋?댁뒪 URL 媛?몄삤湲?(?섍꼍 蹂???먮뒗 ?ㅼ젙?먯꽌)
        custom_url = os.getenv(f"DATABASE_URL_{chat_bot_id}")
        if custom_url:
            logger.info(f"梨쀫큸 ?꾩슜 DB ?ъ슜: {chat_bot_id}")
            return custom_url
        
        # 梨쀫큸蹂??ㅽ궎留??ъ슜 (PostgreSQL??寃쎌슦)
        base_url = os.getenv("DATABASE_URL")
        if base_url and "postgresql" in base_url:
            # PostgreSQL: ?ㅽ궎留덈줈 遺꾨━
            logger.info(f"梨쀫큸蹂??ㅽ궎留??ъ슜: chatbot_{chat_bot_id}")
            return base_url  # ?ㅽ궎留덈뒗 荑쇰━ ??吏??
        
        # SQLite: ?뚯씪蹂?遺꾨━
        if not base_url or "sqlite" in base_url:
            db_path = f"./data/chatbot_{chat_bot_id}.db"
            os.makedirs("./data", exist_ok=True)
            logger.info(f"梨쀫큸 ?꾩슜 SQLite DB ?앹꽦: {db_path}")
            return f"sqlite:///{db_path}"
    
    # 湲곕낯 ?곗씠?곕쿋?댁뒪
    DB_PATH = os.getenv("DB_PATH", "./crawl_data.db")
    DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")
    return DATABASE_URL


def get_engine(chat_bot_id: Optional[str] = None):
    """
    梨쀫큸 ID???곕씪 ?곸젅??DB ?붿쭊 諛섑솚 (罹먯떛)
    
    Args:
        chat_bot_id: 梨쀫큸 ID
        
    Returns:
        SQLAlchemy Engine
    """
    cache_key = chat_bot_id or "default"
    
    if cache_key not in _engines:
        database_url = get_database_url(chat_bot_id)
        
        engine = create_engine(
            database_url,
            connect_args={"check_same_thread": False} if "sqlite" in database_url else {},
            echo=False,  # True濡??ㅼ젙 ??SQL 荑쇰━ 濡쒓렇 異쒕젰
            pool_pre_ping=True,  # ?곌껐 ?곹깭 ?뺤씤
            pool_recycle=3600  # 1?쒓컙留덈떎 ?곌껐 ?ъ깮??
        )
        
        _engines[cache_key] = engine
        logger.info(f"??DB ?붿쭊 ?앹꽦: {cache_key}")
    
    return _engines[cache_key]


def get_session_maker(chat_bot_id: Optional[str] = None) -> sessionmaker:
    """
    梨쀫큸 ID???곕씪 ?곸젅???몄뀡 硫붿씠而?諛섑솚 (罹먯떛)
    
    Args:
        chat_bot_id: 梨쀫큸 ID
        
    Returns:
        SQLAlchemy SessionMaker
    """
    cache_key = chat_bot_id or "default"
    
    if cache_key not in _session_makers:
        engine = get_engine(chat_bot_id)
        session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        _session_makers[cache_key] = session_maker
        logger.info(f"???몄뀡 硫붿씠而??앹꽦: {cache_key}")
    
    return _session_makers[cache_key]


def get_db(chat_bot_id: Optional[str] = None):
    """
    FastAPI ?섏〈??二쇱엯??DB ?몄뀡 ?쒕꼫?덉씠??
    
    Args:
        chat_bot_id: 梨쀫큸 ID (URL ?뚮씪誘명꽣???ㅻ뜑?먯꽌 ?꾨떖)
    
    ?ъ슜 ??
        @app.get("/files")
        def get_files(
            chat_bot_id: str = Query(None),
            db: Session = Depends(lambda: get_db(chat_bot_id))
        ):
            return db.query(CrawledFile).all()
    """
    SessionMaker = get_session_maker(chat_bot_id)
    db = SessionMaker()
    try:
        yield db
    finally:
        db.close()


def init_db(chat_bot_id: Optional[str] = None):
    """
    ?곗씠?곕쿋?댁뒪 珥덇린??(?뚯씠釉??앹꽦)
    
    Args:
        chat_bot_id: 梨쀫큸 ID (?놁쑝硫?湲곕낯 DB 珥덇린??
    
    ?ъ슜 ??
        from backend.src.db.database import init_db
        init_db()  # 湲곕낯 DB
        init_db("481238e0-e568-44fa-9521-014e800239bf")  # 梨쀫큸蹂?DB
    """
    from . import models  # ?쒗솚 李몄“ 諛⑹?
    
    engine = get_engine(chat_bot_id)
    logger.warning("[Database] runtime schema mutation is disabled")
    
    db_name = f"chatbot_{chat_bot_id}" if chat_bot_id else "湲곕낯"
    logger.info(f"???곗씠?곕쿋?댁뒪 珥덇린???꾨즺: {db_name}")


# ?섏쐞 ?명솚?깆쓣 ?꾪븳 湲곕낯 ?붿쭊 諛??몄뀡
engine = get_engine()
SessionLocal = get_session_maker()


