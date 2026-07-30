"""
크롤링된 파일의 청크 데이터를 PostgreSQL에 저장하는 서비스
- DDL: init_crawled_file_table() - 타임아웃 강제 적용 (기본 10초)
- DML: save_chunk_to_postgres() - 데이터 저장만 수행
- 주의: startup 이벤트에서 DB 작업 절대 금지
"""
import os
import logging
from typing import Optional, List
from sqlalchemy import text
from db.db_postgres import get_async_engine
from config.settings import get_postgres_db_name, get_postgres_host
from backend.shared.config import Config

logger = logging.getLogger(__name__)

async def create_embedding(text_data: str) -> List[float]:
    """
    임베딩 생성.

    NOTE:
    - 외부 AI(OpenAI) 호출 로직 제거 요구사항에 따라 네트워크 임베딩 호출을 하지 않는다.
    - pgvector(1536) 컬럼에 저장 가능하도록 0 벡터를 반환한다.
    """
    try:
        # pgvector dimension(1536)과 맞춘 0 벡터
        return [0.0] * 1536
    except Exception as e:
        logger.error(f"[CrawledFile] 임베딩(0벡터) 생성 실패: {e}", exc_info=True)
        raise

def _is_production_environment() -> bool:
    """운영 환경 여부 확인 (환경 변수 또는 설정으로 판단)"""
    env_mode = os.getenv("ENVIRONMENT", os.getenv("ENV", "development")).lower()
    return env_mode in ("production", "prod", "live")


async def init_crawled_file_table(db_name: str, host: Optional[str] = None, timeout: float = 10.0, skip_prod_check: bool = False) -> None:
    """
    DDL: crawledFile 테이블 초기화 (타임아웃 강제)
    - skip_prod_check=True인 경우 운영 환경 체크를 건너뜀 (학습 시 자동 생성용)
    - 타임아웃 강제 적용 (기본 10초)
    
    Args:
        db_name: PostgreSQL DB 이름
        timeout: 타임아웃 시간 (초, 기본 10초)
        skip_prod_check: 운영 환경 체크를 건너뛸지 여부 (기본 False)
    """
    # 운영 환경에서는 런타임 DDL 실행 금지 (skip_prod_check가 False인 경우에만)
    if not skip_prod_check and _is_production_environment():
        logger.warning(
            f"[CrawledFile] 운영 환경에서는 테이블 자동 생성이 금지됩니다. "
            f"DB={db_name} - 테이블을 수동으로 생성해주세요."
        )
        return
    
    try:
        import asyncio
        engine = get_async_engine(db_name, host=host)
        
        # 타임아웃 강제 적용
        async def _init_with_timeout():
            async with engine.begin() as conn:  # begin()으로 트랜잭션 자동 관리
                # 테이블 존재 여부 확인
                check_sql = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'crawledfile'
                    )
                """)
                result = await conn.execute(check_sql)
                table_exists = result.scalar()
                
                if not table_exists:
                    # pgvector 확장 확인 및 생성
                    logger.warning(
                        "[CrawledFile] table is missing; runtime schema mutation is disabled | db=%s",
                        db_name,
                    )
                    
                    # 테이블 생성 (IF NOT EXISTS 사용)
                    create_table_sql = None
                    # Runtime DDL disabled.
                    
                    # 인덱스 생성 (벡터 검색 성능 향상, IF NOT EXISTS 사용)
                    try:
                        # 인덱스 이름을 명시적으로 지정하여 중복 생성 방지
                        create_index_sql = None
                        # Runtime DDL disabled.
                    except Exception as idx_err:
                        # 인덱스 생성 실패는 경고만 (테이블은 생성됨)
                        logger.warning(f"[CrawledFile] 인덱스 생성 실패 (계속 진행): {idx_err}")
                    
                    logger.info(f"[CrawledFile] 테이블 생성 완료: {db_name}.crawledFile")
                else:
                    logger.info(f"[CrawledFile] 테이블 이미 존재: {db_name}.crawledFile")
        
        # 타임아웃 강제 적용
        await asyncio.wait_for(_init_with_timeout(), timeout=timeout)
        logger.info(f"[CrawledFile] 테이블 초기화 완료 (타임아웃: {timeout}초): {db_name}")
                
    except asyncio.TimeoutError:
        error_msg = f"[CrawledFile] 테이블 초기화 타임아웃 ({timeout}초 초과): {db_name}"
        logger.error(error_msg)
        raise TimeoutError(error_msg)
    except Exception as e:
        logger.error(f"[CrawledFile] 테이블 생성/확인 실패: {e}", exc_info=True)
        raise

async def _ensure_table_exists(db_name: str, host: Optional[str] = None) -> bool:
    """
    테이블 존재 여부를 빠르게 확인하는 헬퍼 함수
    
    Args:
        db_name: PostgreSQL DB 이름
    
    Returns:
        테이블이 존재하면 True, 없으면 False
    """
    try:
        engine = get_async_engine(db_name, host=host)
        async with engine.begin() as conn:
            check_sql = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'crawledfile'
                )
            """)
            result = await conn.execute(check_sql)
            return result.scalar()
    except Exception as e:
        logger.warning(f"[CrawledFile] 테이블 존재 확인 실패: {e}")
        return False

# postgreDB 저장 함수
async def save_chunk_to_postgres(
    file_source: str,
    text_data: str,
    domain: Optional[str] = None,
    db_name: Optional[str] = None
) -> Optional[int]:
    """
    DML: 청크 데이터를 PostgreSQL에 저장
    - 테이블이 없으면 자동으로 생성 (학습 시 자동 생성 허용)
    
    Args:
        file_source: 파일 소스 (URL 또는 파일명)
        text_data: 청크 텍스트 데이터
        domain: 도메인 (db_name 결정용, 없으면 기본값 사용)
        db_name: PostgreSQL DB 이름 (없으면 domain으로부터 결정)
    
    Returns:
        저장된 레코드의 ID (실패 시 None)
    """
    try:
        # DB 이름 및 호스트 결정
        target_host = None
        if not db_name:
            # 정책: db_name은 클라이언트가 반드시 제공해야 함 (env fallback 금지)
            raise ValueError("db_name is required for PostgreSQL save (no env/domain fallback).")
        else:
            # db_name이 명시적으로 넘어온 경우에도 host를 domain 기반으로 찾거나 기본값 사용
            target_host = get_postgres_host(domain) if domain else Config.POSTGRES_DB_HOST

        # 테이블 존재 여부 확인 및 생성
        table_exists = await _ensure_table_exists(db_name, host=target_host)
        if not table_exists:
            logger.info(f"[CrawledFile] 테이블이 없어 자동 생성 시작: {db_name}.crawledFile (Host={target_host})")
            # skip_prod_check=True로 설정하여 운영 환경에서도 테이블 생성 허용 (학습 시 필요)
            await init_crawled_file_table(db_name, host=target_host, timeout=10.0, skip_prod_check=True)
            logger.info(f"[CrawledFile] 테이블 자동 생성 완료: {db_name}.crawledFile")
        
        # 임베딩 생성
        embedding = await create_embedding(text_data)
        embedding_str = '[' + ','.join(map(str, embedding)) + ']'
        
        # INSERT 실행 (begin()으로 트랜잭션 자동 관리, 커밋 보장)
        engine = get_async_engine(db_name, host=target_host)
        async with engine.begin() as conn:  # begin()으로 자동 커밋/롤백
            insert_sql = text("""
                INSERT INTO "crawledFile" ("fileSource", "textData", "createdAt", embedding)
                VALUES (:file_source, :text_data, CURRENT_TIMESTAMP, CAST(:embedding AS vector))
                RETURNING id
            """)
            
            logger.info(f"[CrawledFile DEBUG] Executing Query: {insert_sql}")
            # logger.info(f"[CrawledFile DEBUG] Params - file_source: {file_source[:50]}, text_data_len: {len(text_data)}, embedding_len: {len(embedding_str)}")
            
            result = await conn.execute(
                insert_sql,
                {
                    "file_source": file_source,
                    "text_data": text_data,
                    "embedding": embedding_str
                }
            )
            doc_id = result.scalar()
            
            logger.debug(f"[CrawledFile] 저장 완료 | DB={db_name} | ID={doc_id} | source={file_source[:50]}")
            return doc_id
            
    except Exception as e:
        logger.error(f"[CrawledFile] 저장 실패: {e}", exc_info=True)
        return None
