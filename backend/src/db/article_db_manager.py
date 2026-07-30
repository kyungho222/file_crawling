#!/usr/bin/env python3
"""
기사 데이터 DB 매니저 (CRUD 중심, PressDBManager 스타일)
"""

from typing import List, Set, Dict, Any, Optional
from ..utils.logging_util import LoggerSingleton
from .db_operations import execute_query
from sqlalchemy import text, bindparam
from .db_postgres import get_session_factory

logger = LoggerSingleton.get_logger(logger_name="ai_news.db.article_db")


CRAWLER_DB_NAME = ""
CRAWLER_TABLE_NAME = ""
CRAWLER_CHAT_ID = ""








class ArticleDBManager:
    """기사 데이터에 대한 CRUD(Create, Read, Update, Delete) 작업을 관리합니다."""
    
    def __init__(self, db_name: Optional[str] = None, chat_id: Optional[str] = None):
        # 요청 단위로 전달된 값을 우선 사용하고, 없으면 전역(호환) 값을 사용
        from ..utils.common import make_training_table_name
        self.db_name: str = (db_name or "")
        self.table_name: str = (
            make_training_table_name(chat_id) if chat_id else (CRAWLER_TABLE_NAME or "")
        )
        # 임시 테스트용 오버라이드: Config.TEST_POSTGRES_DB_NAME / TEST_POSTGRES_TABLE_NAME

    
    def _assert_ready(self) -> None:
        if not self.db_name or not self.table_name:
            raise RuntimeError("crawler target 미설정: ArticleDBManager(db_name, chat_id)로 생성하거나 set_crawler_target 호출 필요")
    
    # --- Read (조회) --- #
    
    

    async def get_existing_urls(self, urls: List[str]) -> Set[str]:
        """content 컬럼(URL)에 대한 기존 레코드 존재 여부를 조회합니다.

        Args:
            urls: 검사할 URL 목록

        Returns:
            Set[str]: DB에 존재하는 URL 값들
        """
        try:
            if not urls:
                return set()
            # 준비 상태 확인: 테이블 미지정 시 잘못된 SQL 생성 방지
            if not self.db_name or not self.table_name:
                logger.error("❌ URL 중복 검사 실패: crawler target 미설정 (db_name 또는 table_name 누락)")
                return set()

            stmt = text(
                f"""
                SELECT DISTINCT content AS url_value
                FROM {self.table_name}
                WHERE content IN :urls
                """
            ).bindparams(bindparam("urls", expanding=True))

            async with get_session_factory(self.db_name)() as session:
                result = await session.execute(stmt, {"urls": list(urls)})
                rows = result.mappings().all()
            existing_urls = {row['url_value'] for row in rows} if rows else set()
            return existing_urls
        except Exception as e:
            logger.error(f"❌ URL 중복 검사 실패: {e}")
            return set()

    async def get_existing_collected_urls(self, urls: List[str]) -> Set[str]:
        """content_metadata.collected_url 기준으로 기존 레코드 존재 여부를 조회합니다.

        Args:
            urls: 검사할 수집 URL 목록

        Returns:
            Set[str]: DB에 존재하는 collected_url 값들
        """
        try:
            if not urls:
                return set()
            # 준비 상태 확인: 테이블 미지정 시 잘못된 SQL 생성 방지
            if not self.db_name or not self.table_name:
                logger.error("❌ collected_url 중복 검사 실패: crawler target 미설정 (db_name 또는 table_name 누락)")
                return set()

            stmt = text(
                f"""
                SELECT DISTINCT content_metadata::json->>'collected_url' AS url_value
                FROM {self.table_name}
                WHERE content_metadata IS NOT NULL
                AND content_metadata::json->>'collected_url' IN :urls
                """
            ).bindparams(bindparam("urls", expanding=True))

            async with get_session_factory(self.db_name)() as session:
                result = await session.execute(stmt, {"urls": list(urls)})
                rows = result.mappings().all()

            existing_urls = {row['url_value'] for row in rows} if rows else set()
            return existing_urls
        except Exception as e:
            logger.error(f"❌ collected_url 중복 검사 실패: {e}")
            return set()
    
    async def get_total_count(self) -> int:
        """전체 기사 수를 반환합니다."""
        try:
            self._assert_ready()
            query = f"SELECT COUNT(*) as total FROM {self.table_name}"
            result = await execute_query(query, fetch=True, dbname=self.db_name)
            
            if result and len(result) > 0:
                return result[0]['total']
            return 0
            
        except Exception as e:
            logger.error(f"전체 기사 수 조회 실패: {e}")
            return 0
    
    async def get_sample_data(self, limit: int = 5) -> List[Dict[str, Any]]:
        """샘플 기사 데이터를 반환합니다 (디버깅용)."""
        try:
            query = f"""
                SELECT chunk_num, content, web_title
                FROM {self.table_name} 
                WHERE chunk_num IS NOT NULL 
                ORDER BY id DESC
                LIMIT $1
            """
            
            result = await execute_query(query, (limit,), fetch=True, dbname=self.db_name)
            return result if result else []
            
        except Exception as e:
            logger.error(f"샘플 데이터 조회 실패: {e}")
            return []
    
    async def get_by_hash(self, hash_value: str) -> List[Dict[str, Any]]:
        """특정 해시값으로 기사들을 조회합니다."""
        try:
            query = f"""
                SELECT chunk_num, content, web_title, text_data
                FROM {self.table_name}
                WHERE content_metadata::json->>'content_hash' = $1
                ORDER BY chunk_num
            """
            
            result = await execute_query(query, (hash_value,), fetch=True, dbname=self.db_name)
            return result if result else []
            
        except Exception as e:
            logger.error(f"해시별 기사 조회 실패 (해시: {hash_value}): {e}")
            return []
    
    async def get_rows_by_original_urls(self, original_urls: List[str]) -> List[Dict[str, Any]]:
        """original_url 목록으로 id, embedding, content_metadata를 조회합니다."""
        try:
            if not original_urls:
                return []
            self._assert_ready()
            placeholders = ",".join([f"${i+1}" for i in range(len(original_urls))])
            query = f"""
                SELECT id, embedding, content_metadata
                FROM {self.table_name}
                WHERE content_metadata IS NOT NULL
                  AND content_metadata::json->>'original_url' IN ({placeholders})
            """
            rows = await execute_query(query, tuple(original_urls), fetch=True, dbname=self.db_name)
            return rows if rows else []
        except Exception as e:
            logger.error(f"original_url 조회 실패: {e}")
            return []
    
    # --- Create (생성) --- #
    
    async def save_article_chunk(self, chunk_data: Dict[str, Any]) -> bool:
        """
        기사 청크 데이터를 DB에 저장합니다.
        
        Args:
            chunk_data: 저장할 청크 데이터
            
        Returns:
            bool: 저장 성공 여부
        """
        try:
            self._assert_ready()
            insert_query = f"""
                INSERT INTO {self.table_name} (
                    content, chunk_num, memo, content_type, subject, 
                    text_data, embedding, web_title, content_metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """
            
            params = (
                chunk_data.get('content', ''),
                chunk_data.get('chunk_num', ''),
                chunk_data.get('memo', ''),
                chunk_data.get('content_type', 'url'),
                chunk_data.get('subject', 'news'),
                chunk_data.get('text_data', ''),
                chunk_data.get('embedding', ''),
                chunk_data.get('web_title', ''),
                chunk_data.get('content_metadata', '{}')
            )
            
            await execute_query(insert_query, params, dbname=self.db_name)
            logger.debug(f"기사 청크 저장 완료: {chunk_data.get('chunk_num', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"기사 청크 저장 실패: {e}")
            return False
    
    # --- Delete (삭제) --- #
    
    async def delete_by_hash(self, hash_value: str) -> int:
        """특정 해시값의 모든 청크를 삭제합니다."""
        try:
            self._assert_ready()
            delete_query = f"""
                DELETE FROM {self.table_name}
                WHERE content_metadata::json->>'content_hash' = $1
            """
            
            result = await execute_query(delete_query, (hash_value,), dbname=self.db_name)
            deleted_count = result if isinstance(result, int) else 0
            
            logger.info(f"해시별 기사 삭제 완료: {hash_value} ({deleted_count}개 청크)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"해시별 기사 삭제 실패 (해시: {hash_value}): {e}")
            return 0
    
    # --- Utility --- #
    
    async def get_stats(self) -> Dict[str, Any]:
        """기사 DB 통계 정보를 반환합니다."""
        try:
            self._assert_ready()
            total_count = await self.get_total_count()
            sample_data = await self.get_sample_data(3)
            
            # 고유 해시 수 계산
            hash_count_query = f"""
                SELECT COUNT(DISTINCT split_part(chunk_num, '-', 1)) as unique_hashes
                FROM {self.table_name}
                WHERE chunk_num IS NOT NULL
            """
            hash_result = await execute_query(hash_count_query, fetch=True, dbname=self.db_name)
            unique_hashes = hash_result[0]['unique_hashes'] if hash_result else 0
            
            return {
                "total_articles": total_count,
                "unique_hashes": unique_hashes,
                "average_chunks_per_article": round(total_count / unique_hashes, 2) if unique_hashes > 0 else 0,
                "sample_data": [
                    {
                        "chunk_num": item.get("chunk_num", ""),
                        "title": item.get("web_title", "")[:50] + "..." if item.get("web_title") else "",
                        "content_preview": item.get("content", "")[:50] + "..." if item.get("content") else ""
                    }
                    for item in sample_data
                ]
            }
            
        except Exception as e:
            logger.error(f"기사 DB 통계 조회 실패: {e}")
            return {
                "total_articles": 0,
                "unique_hashes": 0,
                "average_chunks_per_article": 0,
                "sample_data": []
            }
