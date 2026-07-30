"""
SQLAlchemy 데이터베이스 모델
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


class CrawlSession(Base):
    """
    크롤링 세션 (한 번의 크롤링 작업)
    서버 DB 스키마에 맞춤
    """
    __tablename__ = "crawl_sessions"

    # 기본 정보
    id = Column(Integer, primary_key=True, index=True, autoincrement=True, comment="ID")
    job_id = Column(String(100), unique=True, index=True, nullable=True, comment="작업 고유 ID")
    
    # 챗봇 및 사용자 정보
    chat_bot_id = Column(String(36), nullable=True, index=True, comment="챗봇 ID (UUID)")
    mb_id = Column(String(100), nullable=True, index=True, comment="사용자 계정 ID")
    mb_name = Column(String(200), nullable=True, comment="사용자 계정 이름")
    
    # 크롤링 대상 정보
    subject = Column(String(500), nullable=True, comment="크롤링 사이트명")
    domain = Column(String(200), nullable=True, index=True, comment="크롤링 도메인")
    # start_url = Column(String(500), nullable=False, comment="시작 URL")  # 서버 DB에 없음
    
    # 크롤링 설정
    # max_depth = Column(Integer, default=3, comment="크롤링 깊이")  # 서버 DB에 없음
    # max_pages = Column(Integer, default=150, comment="최대 페이지 수")  # 서버 DB에 없음
    
    # 상태 및 결과
    status = Column(String(20), default="start", comment="start, ok, error")
    content_type = Column(String(50), default="file", comment="콘텐츠 타입")
    
    # 파일 수집 통계
    scan = Column(Integer, default=0, comment="수집 예정 파일 개수")
    collection = Column(Integer, default=0, comment="수집된 파일 개수")
    save = Column(Integer, default=0, comment="실제 다운로드된 파일 개수")
    pages = Column(Integer, default=0, comment="크롤링한 페이지 수")
    
    # 시간
    # created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="생성 시각")  # 서버 DB에 없음
    start_at = Column(DateTime(timezone=True), nullable=True, comment="크롤링 시작 시각")
    end_at = Column(DateTime(timezone=True), nullable=True, comment="크롤링 종료 시각")
    
    # 추가 정보
    details = Column(Text, nullable=True, comment="상세 설명")
    memo = Column(Text, nullable=True, comment="메모")
    # user_agent = Column(String(500), nullable=True, comment="사용된 User-Agent")  # 서버 DB에 없음
    
    # 관계
    files = relationship("CrawledFile", back_populates="session", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<CrawlSession {self.job_id} - {self.status}>"


class CrawledFile(Base):
    """
    크롤링된 파일 정보
    """
    __tablename__ = "crawled_files"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    
    # 세션 연결
    session_id = Column(Integer, ForeignKey("crawl_sessions.id"), nullable=False, index=True)
    
    # 도메인 (사이트별 구분) - DB 저장소용 핵심 필드
    domain = Column(String(200), index=True, nullable=True, comment="사이트 도메인 (gwangjin.go.kr 등)")
    
    # 파일 정보
    filename = Column(String(500), nullable=False, comment="파일명")
    url = Column(String(1000), nullable=False, unique=True, index=True, comment="파일 다운로드 URL (중복 방지)")
    url_hash = Column(String(64), index=True, nullable=True, comment="URL 해시 (빠른 중복 체크)")
    filesize = Column(BigInteger, default=0, comment="파일 크기 (bytes)")
    filetype = Column(String(50), nullable=True, comment="파일 확장자 (pdf, hwp, ...)")
    formatted_size = Column(String(50), nullable=True, comment="읽기 쉬운 크기 (1.5 MB)")
    
    # 출처 정보
    source_page = Column(String(1000), nullable=True, comment="파일을 발견한 페이지 URL")
    last_modified = Column(String(100), nullable=True, comment="파일 최종 수정일")
    content_type = Column(String(200), nullable=True, comment="Content-Type")
    
    # 다운로드 정보
    is_downloaded = Column(Boolean, default=False, comment="다운로드 여부")
    local_path = Column(String(500), nullable=True, comment="로컬 저장 경로")
    download_status = Column(String(20), default="pending", comment="pending, downloading, completed, failed")
    download_error = Column(Text, nullable=True, comment="다운로드 오류 메시지")
    downloaded_at = Column(DateTime(timezone=True), nullable=True, comment="다운로드 완료 시각")
    
    # 시간
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="발견 시각")
    
    # 관계
    session = relationship("CrawlSession", back_populates="files")

    def __repr__(self):
        return f"<CrawledFile {self.filename} ({self.filetype})>"

    def to_dict(self):
        """딕셔너리로 변환 (API 응답용 + JSON 호환)"""
        return {
            "id": self.id,
            "session_id": self.session_id,
            "domain": self.domain,
            "filename": self.filename,
            "url": self.url,
            "url_hash": self.url_hash,
            "filesize": self.filesize,
            "filetype": self.filetype,
            "formatted_size": self.formatted_size,
            "source_page": self.source_page,
            "last_modified": self.last_modified,
            "content_type": self.content_type,
            "is_downloaded": self.is_downloaded,
            "local_path": self.local_path,
            "download_status": self.download_status,
            "download_error": self.download_error,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "downloaded_at": self.downloaded_at.isoformat() if self.downloaded_at else None,
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        """딕셔너리에서 객체 생성 (JSON → DB 마이그레이션용)"""
        return cls(
            domain=data.get('domain'),
            filename=data.get('filename'),
            url=data.get('url'),
            url_hash=data.get('url_hash'),
            filesize=data.get('filesize', 0),
            filetype=data.get('filetype'),
            formatted_size=data.get('formatted_size'),
            source_page=data.get('source_page'),
            last_modified=data.get('last_modified'),
            content_type=data.get('content_type'),
            is_downloaded=bool(data.get('saved_path') or data.get('local_path')),
            local_path=data.get('saved_path') or data.get('local_path'),
            download_status='completed' if data.get('saved_path') else 'pending'
        )

