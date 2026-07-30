from typing import List, Optional
from pydantic import BaseModel, Field

class VideoSegment(BaseModel):
    start: str
    text: str

class EduRequest(BaseModel):
    chat_bot_id: str
    db_name: str
    content_type: str = None  # "text", "url", or "file"
    contents: List[str]  # text values, URLs, or file identifiers
    # For file learning, contents carries the logical identifier (usually URL),
    # and file_paths may carry the actual local file paths.
    file_paths: Optional[List[Optional[str]]] = None
    subjects: Optional[List[str]] = None  # learning titles
    job_id: str = None
    # memo: Optional[str] = None
    # Default should be None to avoid shared mutable default list across instances.
    memo: Optional[List[str]] = None
    content_created_at: Optional[List[Optional[str]]] = None
    content_updated_at: Optional[List[Optional[str]]] = None
    crawl_mode: Optional[str] = None
    sitemap: Optional[str] = "N"  # sitemap discovery option (Y/N)
    url_filter: Optional[str] = ""  # URL filter option (Q=query string only, P=path only, B=both)
    video_file_name: Optional[List[str]] = None  # uploaded video file names
    image_file_name: Optional[List[str]] = None  # uploaded image file names
    sound_file_name: Optional[List[str]] = Field(None, alias="name")  # uploaded audio file names
    video_segments: Optional[List[List[VideoSegment]]] = None  # video segment metadata
