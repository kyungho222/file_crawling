"""
SQLAlchemy database models used by the crawler.
"""

from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .database import Base


class CrawlSession(Base):
    """Metadata for a crawl session."""

    __tablename__ = "crawl_sessions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True, comment="ID")
    job_id = Column(String(100), unique=False, index=True, nullable=True, comment="Job ID")

    chat_bot_id = Column(String(200), nullable=True, index=True, comment="Chatbot ID")
    mb_id = Column(String(100), nullable=True, index=True, comment="Member ID")
    mb_name = Column(String(200), nullable=True, comment="Member name")

    subject = Column(String(500), nullable=True, comment="Crawl target name")
    domain = Column(String(200), nullable=True, index=True, comment="Crawl domain")
    colle = Column(Text, nullable=True, comment="Collection info")

    start_url = Column(Text, nullable=True, comment="Legacy start URL")
    max_depth = Column(Integer, nullable=True, comment="Legacy max depth")
    max_pages = Column(Integer, nullable=True, comment="Legacy max pages")
    notes = Column(Text, nullable=True, comment="Legacy notes")
    session_id = Column(String(100), nullable=True, comment="Legacy session ID")
    created_at = Column(DateTime(timezone=True), nullable=True, comment="Legacy created time")
    started_at = Column(DateTime(timezone=True), nullable=True, comment="Legacy started time")
    completed_at = Column(DateTime(timezone=True), nullable=True, comment="Legacy completed time")

    status = Column(String(20), default="start", comment="start, ok, error")
    content_type = Column(String(50), default="file", comment="Content type")

    scan = Column(Integer, default=0, comment="Scanned file count")
    collection = Column(Integer, default=0, comment="Collected file count")
    save = Column(Integer, default=0, comment="Saved file count")
    pages = Column(Integer, default=0, comment="Visited page count")

    start_at = Column(DateTime(timezone=True), nullable=True, comment="Crawl start time")
    end_at = Column(DateTime(timezone=True), nullable=True, comment="Crawl end time")

    details = Column(Text, nullable=True, comment="Details")
    memo = Column(Text, nullable=True, comment="Memo")

    files = relationship("CrawledFile", back_populates="session", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<CrawlSession {self.job_id} - {self.status}>"


class CrawledFile(Base):
    """Metadata for a crawled file."""

    __tablename__ = "crawled_files"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey("crawl_sessions.id"), nullable=False, index=True)
    domain = Column(String(200), index=True, nullable=True, comment="Site domain")

    filename = Column(String(500), nullable=False, comment="Filename")
    url = Column(String(1000), nullable=False, unique=True, index=True, comment="Download URL")
    postgres_file_id = Column(Integer, nullable=True, index=True, comment="Linked PostgreSQL file_contents.id")
    filesize = Column(BigInteger, default=0, comment="File size in bytes")
    filetype = Column(String(50), nullable=True, comment="File extension")
    formatted_size = Column(String(50), nullable=True, comment="Human readable file size")

    source_page = Column(String(1000), nullable=True, comment="Source page URL")
    last_modified = Column(String(100), nullable=True, comment="Last modified string")
    content_type = Column(String(200), nullable=True, comment="Content-Type header")

    is_downloaded = Column(Boolean, default=False, comment="Downloaded flag")
    local_path = Column(String(500), nullable=True, comment="Local saved path")
    download_status = Column(String(20), default="pending", comment="pending, downloading, completed, failed")
    download_error = Column(Text, nullable=True, comment="Download error")
    downloaded_at = Column(DateTime(timezone=True), nullable=True, comment="Download completion time")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="Discovery time")

    session = relationship("CrawlSession", back_populates="files")

    def __repr__(self) -> str:
        return f"<CrawledFile {self.filename} ({self.filetype})>"

    def to_dict(self) -> dict:
        """Convert to a serializable dictionary."""
        return {
            "id": self.id,
            "session_id": self.session_id,
            "domain": self.domain,
            "filename": self.filename,
            "url": self.url,
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
    def from_dict(cls, data: dict) -> "CrawledFile":
        """Create an instance from a dictionary."""
        return cls(
            domain=data.get("domain"),
            filename=data.get("filename"),
            url=data.get("url"),
            filesize=data.get("filesize", 0),
            filetype=data.get("filetype"),
            formatted_size=data.get("formatted_size"),
            source_page=data.get("source_page"),
            last_modified=data.get("last_modified"),
            content_type=data.get("content_type"),
            is_downloaded=bool(data.get("saved_path") or data.get("local_path")),
            local_path=data.get("saved_path") or data.get("local_path"),
            download_status="completed" if data.get("saved_path") else "pending",
        )
