import os
import datetime
import logging
from typing import Set, Optional, List, Dict

logger = logging.getLogger(__name__)
_fallback_notice_emitted = False

class DBRepository:
    """
    데이터베이스 접근을 위한 Repository 클래스
    실제 DB 연결 시 이 클래스를 확장하여 사용
    """
    
    def __init__(self):
        # DB_HOST 환경 변수가 설정되어 있지 않으면 Fallback 모드(In-Memory) 사용
        global _fallback_notice_emitted
        if not os.getenv("DB_HOST"):
            self.fallback_mode = True
            self.mock_db: Dict[str, Dict] = {}  # In-memory DB for fallback
            if not _fallback_notice_emitted:
                logger.debug("[DBRepository] Initialized in Fallback Mode (In-Memory)")
                _fallback_notice_emitted = True
        else:
            self.fallback_mode = False
            # 실제 DB 연결 로직은 여기에 추가
            # 예: from db.connection import engine; self.engine = engine
            if not _fallback_notice_emitted:
                logger.debug("[DBRepository] Initialized in Real DB Mode (Host: %s)", os.getenv("DB_HOST"))
                _fallback_notice_emitted = True

    def get_all_unique_keys(self, domain: str) -> Set[str]:
        """
        해당 도메인에서 이미 처리된(또는 처리 중인) 모든 파일의 unique_key를 조회
        
        Args:
            domain: 도메인 이름
            
        Returns:
            unique_key 집합
        """
        if self.fallback_mode:
            return {k for k, v in self.mock_db.items() if v.get('domain') == domain}
        
        # SQL 예시: SELECT unique_key FROM files WHERE domain = :domain
        # return set(row['unique_key'] for row in results)
        return set()

    def insert_file_status(
        self, 
        unique_key: str, 
        status: str, 
        domain: str, 
        local_path: Optional[str] = None
    ):
        """
        파일 상태를 DB에 삽입 (INSERT)
        unique_key는 UNIQUE 인덱스가 걸려 있어야 함
        
        Args:
            unique_key: 파일 고유 키
            status: 파일 상태
            domain: 도메인 이름
            local_path: 로컬 저장 경로 (선택)
        """
        if self.fallback_mode:
            self.mock_db[unique_key] = {
                "unique_key": unique_key,
                "status": status,
                "domain": domain,
                "local_path": local_path,
                "created_at": datetime.datetime.now().isoformat()
            }
            print(f"[DB-Mock] Insert: {unique_key} -> {status}")
            return

        # SQL 예시: 
        # INSERT INTO files (unique_key, status, domain, local_path, created_at)
        # VALUES (:unique_key, :status, :domain, :local_path, NOW())
        # ON DUPLICATE KEY UPDATE status = :status
        print(f"[DB] Insert: {unique_key} -> {status}")

    def update_file_status(
        self, 
        unique_key: str, 
        status: str, 
        local_path: Optional[str] = None,
        chunk_count: int = 0, 
        vector_count: int = 0
    ):
        """
        파일 상태 업데이트 (UPDATE)
        
        Args:
            unique_key: 파일 고유 키
            status: 새로운 상태
            local_path: 로컬 저장 경로 (선택)
            chunk_count: 청크 개수 (선택)
            vector_count: 벡터 개수 (선택)
        """
        if self.fallback_mode:
            if unique_key in self.mock_db:
                self.mock_db[unique_key]['status'] = status
                if local_path:
                    self.mock_db[unique_key]['local_path'] = local_path
                if chunk_count > 0:
                    self.mock_db[unique_key]['chunk_count'] = chunk_count
                if vector_count > 0:
                    self.mock_db[unique_key]['vector_count'] = vector_count
                self.mock_db[unique_key]['updated_at'] = datetime.datetime.now().isoformat()
                print(f"[DB-Mock] Update: {unique_key} -> {status}")
            else:
                print(f"[DB-Mock] Warning: unique_key {unique_key} not found for update.")
            return

        # SQL 예시:
        # UPDATE files SET status = :status, local_path = :local_path, ...
        # WHERE unique_key = :unique_key
        print(f"[DB] Update: {unique_key} -> {status}")

    def get_file_by_key(self, unique_key: str) -> Optional[Dict]:
        """
        unique_key로 파일 정보 조회
        
        Args:
            unique_key: 파일 고유 키
            
        Returns:
            파일 정보 딕셔너리 또는 None
        """
        if self.fallback_mode:
            return self.mock_db.get(unique_key)
        
        # SQL 예시: SELECT * FROM files WHERE unique_key = :unique_key
        return None

    def get_files_by_domain(self, domain: str) -> List[Dict]:
        """
        도메인별 파일 목록 조회
        
        Args:
            domain: 도메인 이름
            
        Returns:
            파일 정보 리스트
        """
        if self.fallback_mode:
            return [v for v in self.mock_db.values() if v.get('domain') == domain]
        
        # SQL 예시: SELECT * FROM files WHERE domain = :domain
        return []
