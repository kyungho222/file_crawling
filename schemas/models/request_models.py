from pydantic import BaseModel
from typing import List
from typing import Optional


class SaveLongTermMemory(BaseModel):
    chat_id: str
    user_id: str
    db_name: str
    message_id: int
    is_ok: bool


class DeleteLongMemoryRequest(BaseModel):
    chat_id: str
    user_id: str
    message_ids: List[int]
    db_name: str


class RefreshMemory(BaseModel):
    chat_bot_id: str
    user_id: str
    db_name: str

class ChatbotConfigRequest(BaseModel):
    chat_bot_id: str
    db_name: str
    rag_source: Optional[str] = None
    web_source: Optional[str] = None
    use_recent: Optional[str] = None
    llm_source: Optional[str] = None
    use_websearch: Optional[str] = None
    vector_k: Optional[int] = None
    vector_threshold: Optional[float] = None
    model_name: Optional[str] = None
    source_num: Optional[int] = None
    use_rag: Optional[str] = None
    use_suggested_questions: Optional[str] = None
    use_suggested_count: Optional[int] = None
    temperature: Optional[float] = None
    max_turns: Optional[int] = None
    personal_info_filter: Optional[str] = None
    harmful_content_filter: Optional[str] = None
    bm25_ratio: Optional[float] = None
    harmful_content_keyword: Optional[str] = None
    memory_expire_time: Optional[int] = None
    use_suggested_question_type: Optional[str] = None
