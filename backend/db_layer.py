from typing import Set, Optional, List, Dict
import datetime
import logging

logger = logging.getLogger(__name__)
_fallback_notice_emitted = False

# 실제 DB 연결 라이브러리 (예: sqlalchemy, pymysql 등)에 맞게 수정 필요
# 현재는 로직 구현을 위한 Mock 클래스로 작성됨

class DBLayer:
    def __init__(self):
        # 실제 구현에서는 DB 커넥션 풀 등을 초기화
        self.fallback_mode = True # 기본적으로 Fallback 모드 (실제 DB 연결 시 False로 변경)
        self.mock_db: Dict[str, Dict] = {} # In-memory DB for fallback
        global _fallback_notice_emitted
        if not _fallback_notice_emitted:
            logger.debug("[DBLayer] Initialized in Fallback Mode (In-Memory)")
            _fallback_notice_emitted = True

    def get_all_unique_keys(self, domain: str) -> Set[str]:
        """
        해당 도메인에서 이미 처리된(또는 처리 중인) 모든 파일의 unique_key를 조회
        """
        if self.fallback_mode:
            return {k for k, v in self.mock_db.items() if v.get('domain') == domain}
        
        # SQL 예시: SELECT unique_key FROM files WHERE domain = :domain
        # return set(row['unique_key'] for row in results)
        return set() 

    def insert_file_status(self, unique_key: str, status: str, domain: str, local_path: Optional[str] = None):
        """
        파일 상태를 DB에 삽입 (INSERT)
        unique_key는 UNIQUE 인덱스가 걸려 있어야 함
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

    def update_file_status(self, unique_key: str, status: str, local_path: Optional[str] = None, 
                           chunk_count: int = 0, vector_count: int = 0):
        """
        파일 상태 업데이트 (UPDATE)
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
                print(f"[DB-Mock] Update: {unique_key} -> {status}")
            else:
                print(f"[DB-Mock] Warning: unique_key {unique_key} not found for update.")
            return

        # SQL 예시:
        # UPDATE files SET status = :status, local_path = :local_path, ...
        # WHERE unique_key = :unique_key
        print(f"[DB] Update: {unique_key} -> {status}")

db = DBLayer()
