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
            logger.debug(f"梨쀫큸 ?꾩슜 DB ?ъ슜: {chat_bot_id}")
            return custom_url
        
        # 梨쀫큸蹂??ㅽ궎留??ъ슜 (PostgreSQL??寃쎌슦)
        base_url = os.getenv("DATABASE_URL")
        if base_url and "postgresql" in base_url:
            # PostgreSQL: ?ㅽ궎留덈줈 遺꾨━
            logger.debug(f"梨쀫큸蹂??ㅽ궎留??ъ슜: chatbot_{chat_bot_id}")
            return base_url  # ?ㅽ궎留덈뒗 荑쇰━ ??吏??
        
        # SQLite: ?뚯씪蹂?遺꾨━
        if not base_url or "sqlite" in base_url:
            db_path = f"./data/chatbot_{chat_bot_id}.db"
            os.makedirs("./data", exist_ok=True)
            logger.debug(f"梨쀫큸 ?꾩슜 SQLite DB ?앹꽦: {db_path}")
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
        logger.debug(f"??DB ?붿쭊 ?앹꽦: {cache_key}")
    
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
        logger.debug(f"???몄뀡 硫붿씠而??앹꽦: {cache_key}")
    
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
    ?곗씠?곕쿋?댁뒪 珥덇린??(?뚯씠釉??앹꽦 諛?留덉씠洹몃젅?댁뀡)
    
    Args:
        chat_bot_id: 梨쀫큸 ID (?놁쑝硫?湲곕낯 DB 珥덇린??
    
    ?ъ슜 ??
        from backend.src.db.database import init_db
        init_db()  # 湲곕낯 DB
        init_db("481238e0-e568-44fa-9521-014e800239bf")  # 梨쀫큸蹂?DB
    """
    from . import models  # ?쒗솚 李몄“ 諛⑹?
    
    engine = get_engine(chat_bot_id)
    
    # 1. ?뚯씠釉??앹꽦 (?놁쑝硫??앹꽦)
    logger.warning("[Database] runtime schema mutation is disabled")
    
    # 2. SQLite??寃쎌슦 ?꾨씫??而щ읆 異붽? (留덉씠洹몃젅?댁뀡)
    database_url = get_database_url(chat_bot_id)
    if "sqlite" in database_url:
        logger.warning("[Database] runtime SQLite schema migration is disabled")
    
    db_name = f"chatbot_{chat_bot_id}" if chat_bot_id else "湲곕낯"
    logger.debug(f"???곗씠?곕쿋?댁뒪 珥덇린???꾨즺: {db_name}")


def _migrate_sqlite_schema(engine, database_url: str):
    """
    SQLite ?ㅽ궎留?留덉씠洹몃젅?댁뀡 (?꾨씫??而щ읆 異붽?)
    """
    import sqlite3
    
    try:
        # SQLite ?뚯씪 寃쎈줈 異붿텧
        if database_url.startswith("sqlite:///"):
            db_path = database_url.replace("sqlite:///", "")
        else:
            return  # SQLite媛 ?꾨땲硫??ㅽ궢
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # ?꾩옱 ?뚯씠釉?援ъ“ ?뺤씤 (而щ읆紐? ??? not_null ?뺣낫)
        cursor.execute("PRAGMA table_info(crawl_sessions)")
        columns_info = cursor.fetchall()
        existing_columns = {col[1]: col[2] for col in columns_info}
        # col[3] = not_null (1?대㈃ NOT NULL, 0?대㈃ NULL ?덉슜)
        not_null_columns = {col[1]: col[3] for col in columns_info if col[3] == 1}
        
        # ??湲곗〈 DB???덉?留?紐⑤뜽?먮뒗 ?녿뒗 而щ읆 (start_url, max_depth, max_pages, notes ??
        # ??而щ읆?ㅼ? 湲곗〈 ?곗씠?곕? ?꾪빐 ?④꺼?먮릺, NULL ?덉슜?쇰줈 泥섎━
        legacy_columns = ['start_url', 'max_depth', 'max_pages', 'notes', 'created_at', 'started_at', 'completed_at', 'session_id']
        
        # ?꾩슂??而щ읆 ?뺤쓽 (紐⑤뜽???덉?留??뚯씠釉붿뿉 ?녿뒗 而щ읆)
        required_columns = {
            'job_id': 'VARCHAR(100)',
            'scan': 'INTEGER DEFAULT 0',
            'collection': 'INTEGER DEFAULT 0',
            'save': 'INTEGER DEFAULT 0',
            'pages': 'INTEGER DEFAULT 0',
            'colle': 'TEXT',
            'content_type': "VARCHAR(50) DEFAULT 'file'",
            'memo': 'TEXT',
            'details': 'TEXT',
            'chat_bot_id': 'VARCHAR(200)',
            'mb_id': 'VARCHAR(100)',
            'mb_name': 'VARCHAR(200)',
            'subject': 'VARCHAR(500)',
            'domain': 'VARCHAR(200)',
            'start_at': 'DATETIME',
            'end_at': 'DATETIME'
        }
        
        # ???덇굅??而щ읆??NOT NULL??寃쎌슦, 湲곕낯媛믪쓣 ?ㅼ젙?섏뿬 NOT NULL ?쒖빟議곌굔 ?꾪솕
        # SQLite??吏곸젒?곸쑝濡?NOT NULL???쒓굅?????놁쑝誘濡? 
        # ?덇굅??而щ읆???놁쑝硫?異붽??섍퀬 (NULL ?덉슜), ?덉쑝硫?湲곕낯媛??낅뜲?댄듃
        # for legacy_col in legacy_columns:
        #     if legacy_col not in existing_columns:
        #         # ?덇굅??而щ읆???놁쑝硫?NULL ?덉슜?쇰줈 異붽? (紐⑤뜽?먯꽌 ?ъ슜?섏? ?딆쓬)
        #         try:
        #             cursor.execute(sql)
        #             logger.debug(f"   ???덇굅??而щ읆 異붽?: {legacy_col} (NULL ?덉슜)")
        #         except sqlite3.OperationalError as e:
        #             logger.warning(f"   ?좑툘  ?덇굅??而щ읆 異붽? ?ㅽ뙣: {legacy_col} - {e}")
        #     elif legacy_col in not_null_columns:
        #         # ?덇굅??而щ읆??NOT NULL??寃쎌슦, 湲곗〈 NULL ?됱뿉 湲곕낯媛??ㅼ젙
        #         try:
        #             # 湲곗〈 NULL 媛믪뿉 湲곕낯媛??ㅼ젙 (??踰덈쭔 ?ㅽ뻾)
        #             if legacy_col == 'start_url':
        #                 cursor.execute("UPDATE crawl_sessions SET start_url = '' WHERE start_url IS NULL")
        #             elif legacy_col in ['max_depth', 'max_pages']:
        #                 cursor.execute(f"UPDATE crawl_sessions SET {legacy_col} = 0 WHERE {legacy_col} IS NULL")
        #             elif legacy_col in ['notes', 'session_id']:
        #                 cursor.execute(f"UPDATE crawl_sessions SET {legacy_col} = '' WHERE {legacy_col} IS NULL")
        #             logger.debug(f"   ?뵩 ?덇굅??而щ읆 湲곕낯媛??ㅼ젙: {legacy_col}")
        #         except Exception as e:
        #             logger.debug(f"   ??툘  ?덇굅??而щ읆 湲곕낯媛??ㅼ젙 ?ㅽ궢: {legacy_col} - {e}")
        
        conn.commit()  # ?덇굅??而щ읆 泥섎━ ??癒쇱? 而ㅻ컠
        
        # ?꾨씫??而щ읆 異붽?
        added_count = 0
        for col_name, col_type in required_columns.items():
            if col_name not in existing_columns:
                try:
                    continue
                    logger.debug(f"   ??而щ읆 異붽?: {col_name} ({col_type})")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e).lower():
                        logger.debug(f"   ??툘  而щ읆 ?대? 議댁옱: {col_name}")
                    else:
                        logger.warning(f"   ?좑툘  而щ읆 異붽? ?ㅽ뙣: {col_name} - {e}")
        
        if added_count > 0:
            conn.commit()
            logger.debug("crawl_sessions schema migration skipped: %s columns", added_count)
        
        # ??crawled_files ?뚯씠釉?留덉씠洹몃젅?댁뀡
        try:
            cursor.execute("PRAGMA table_info(crawled_files)")
            files_columns_info = cursor.fetchall()
            files_existing_columns = {col[1]: col[2] for col in files_columns_info}
            
            # crawled_files ?뚯씠釉붿뿉 ?꾩슂??而щ읆 ?뺤쓽
            files_required_columns = {
                'postgres_file_id': 'INTEGER',
                'domain': 'VARCHAR(200)',
                'formatted_size': 'VARCHAR(50)',
                'source_page': 'VARCHAR(1000)',
                'last_modified': 'VARCHAR(100)',
                'content_type': 'VARCHAR(200)',
                'download_status': "VARCHAR(20) DEFAULT 'pending'",
                'download_error': 'TEXT',
                'downloaded_at': 'DATETIME'
            }
            
            # ?꾨씫??而щ읆 異붽?
            files_added_count = 0
            for col_name, col_type in files_required_columns.items():
                if col_name not in files_existing_columns:
                    try:
                        continue
                        logger.debug(f"   ??crawled_files 而щ읆 異붽?: {col_name} ({col_type})")
                    except sqlite3.OperationalError as e:
                        if "duplicate column name" in str(e).lower():
                            logger.debug(f"   ??툘  crawled_files 而щ읆 ?대? 議댁옱: {col_name}")
                        else:
                            logger.warning(f"   ?좑툘  crawled_files 而щ읆 異붽? ?ㅽ뙣: {col_name} - {e}")
            
            if files_added_count > 0:
                conn.commit()
                logger.debug("crawled_files schema migration skipped: %s columns", files_added_count)
        except Exception as e:
            logger.warning(f"?좑툘 crawled_files 留덉씠洹몃젅?댁뀡 ?ㅽ뙣 (臾댁떆): {e}")
        
        conn.close()
        
    except Exception as e:
        logger.warning(f"?좑툘 SQLite 留덉씠洹몃젅?댁뀡 ?ㅽ뙣 (臾댁떆): {e}")
        # 留덉씠洹몃젅?댁뀡 ?ㅽ뙣?대룄 怨꾩냽 吏꾪뻾


# ?섏쐞 ?명솚?깆쓣 ?꾪븳 湲곕낯 ?붿쭊 諛??몄뀡
engine = get_engine()
SessionLocal = get_session_maker()


