# db/models.py
"""
데이터베이스 모델 정의
"""
from typing import Optional
from datetime import datetime
from pydantic import BaseModel

class FileMetadata(BaseModel):
    """파일 메타데이터 모델"""
    unique_key: str
    url: str
    filename: str
    filesize: int
    ext: str
    status: str
    domain: str
    local_path: Optional[str] = None
    chunk_count: int = 0
    vector_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class CrawlSession(BaseModel):
    """크롤링 세션 모델"""
    session_id: str
    start_url: str
    status: str
    stage: str
    scan_count: int = 0
    collection_count: int = 0
    save_count: int = 0
    study_count: int = 0
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
