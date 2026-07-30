"""
PostgreSQL 연결 테스트 서비스 (API 엔드포인트용)
"""
import asyncio
import logging
from typing import Dict, Any, Optional
from sqlalchemy import text
from db.db_postgres import get_async_engine, _build_database_url_async
from config.settings import Config, get_postgres_db_name

logger = logging.getLogger(__name__)


async def test_basic_connection(db_name: Optional[str] = None) -> Dict[str, Any]:
    """기본 PostgreSQL 연결 테스트"""
    target_db = db_name or Config.POSTGRES_DB_NAME
    
    result = {
        "success": False,
        "db_name": target_db,
        "host": Config.POSTGRES_DB_HOST,
        "port": Config.POSTGRES_DB_PORT,
        "user": Config.POSTGRES_DB_USER,
        "connection_url": None,
        "error": None,
        "postgresql_version": None,
        "current_database": None,
    }
    
    try:
        # 연결 URL 생성 (비밀번호 마스킹)
        db_url = _build_database_url_async(target_db)
        if '@' in db_url:
            parts = db_url.split('@')
            user_pass_part = parts[0]
            if ':' in user_pass_part:
                user = user_pass_part.split('//')[1].split(':')[0]
                masked_url = f"postgresql+asyncpg://{user}:***@{parts[1]}"
            else:
                masked_url = db_url
        else:
            masked_url = db_url
        result["connection_url"] = masked_url
        
        # 엔진 생성
        engine = get_async_engine(target_db)
        
        # 연결 테스트
        async with engine.begin() as conn:
            # 기본 연결 테스트
            test_result = await conn.execute(text("SELECT 1 as test"))
            if test_result.scalar() != 1:
                result["error"] = "연결 테스트 결과가 예상과 다릅니다."
                return result
            
            # PostgreSQL 버전 확인
            try:
                version_result = await conn.execute(text("SELECT version()"))
                result["postgresql_version"] = version_result.scalar()
            except Exception as e:
                logger.warning(f"PostgreSQL 버전 확인 실패: {e}")
            
            # 현재 데이터베이스 확인
            try:
                db_result = await conn.execute(text("SELECT current_database()"))
                result["current_database"] = db_result.scalar()
            except Exception as e:
                logger.warning(f"현재 데이터베이스 확인 실패: {e}")
        
        result["success"] = True
        return result
        
    except Exception as e:
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        logger.error(f"PostgreSQL 연결 테스트 실패: {e}", exc_info=True)
        return result


async def test_table_exists(db_name: str, table_name: str = "crawledfile") -> Dict[str, Any]:
    """테이블 존재 여부 확인"""
    result = {
        "success": False,
        "db_name": db_name,
        "table_name": table_name,
        "exists": False,
        "columns": [],
        "error": None,
    }
    
    try:
        engine = get_async_engine(db_name)
        async with engine.begin() as conn:
            # 테이블 존재 여부 확인
            check_sql = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                )
            """)
            check_result = await conn.execute(check_sql, {"table_name": table_name})
            exists = check_result.scalar()
            result["exists"] = exists
            
            if exists:
                # 테이블 컬럼 정보 조회
                table_info_sql = text("""
                    SELECT 
                        column_name, 
                        data_type,
                        character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                    ORDER BY ordinal_position
                """)
                table_info = await conn.execute(table_info_sql, {"table_name": table_name})
                rows = table_info.fetchall()
                
                columns = []
                for row in rows:
                    col_name, data_type, max_len = row
                    col_info = {
                        "name": col_name,
                        "type": data_type,
                    }
                    if max_len:
                        col_info["max_length"] = max_len
                    columns.append(col_info)
                
                result["columns"] = columns
            
            result["success"] = True
            return result
            
    except Exception as e:
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        logger.error(f"테이블 존재 확인 실패: {e}", exc_info=True)
        return result


async def test_multiple_databases(db_names: Optional[list] = None) -> Dict[str, Any]:
    """여러 데이터베이스 연결 테스트"""
    if db_names is None:
        db_names = [
            Config.POSTGRES_DB_NAME,
            "dev_user",
            "testchatbot1",
        ]
        # 중복 제거
        db_names = list(set([db for db in db_names if db]))
    
    results = {
        "success": True,
        "databases": {},
    }
    
    for db_name in db_names:
        try:
            engine = get_async_engine(db_name)
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT 1"))
                if result.scalar() == 1:
                    results["databases"][db_name] = {
                        "success": True,
                        "error": None,
                    }
                else:
                    results["databases"][db_name] = {
                        "success": False,
                        "error": "연결 테스트 결과가 예상과 다릅니다.",
                    }
        except Exception as e:
            results["databases"][db_name] = {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
            }
    
    return results


async def test_domain_db_mapping() -> Dict[str, Any]:
    """도메인별 DB 매핑 테스트"""
    test_domains = [
        "dev.han.kr",
        "test.han.kr",
        "aniestkh.han.kr",
        "example.han.kr",
    ]
    
    mapping = {}
    for domain in test_domains:
        try:
            mapping[domain] = "N/A (db_name must be provided by client)"
        except Exception as e:
            mapping[domain] = f"ERROR: {str(e)}"
    
    return {
        "success": True,
        "mapping": mapping,
    }

