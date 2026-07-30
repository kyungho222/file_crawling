from backend.shared.config import Config

from utils.logging_util import LoggerSingleton
import logging

logger = LoggerSingleton.get_logger(
    logger_name="models.long_term_table_models", level=logging.INFO
)

from sqlalchemy import (
    Column,
    Integer,
    String,
    text,
    TIMESTAMP,
    ForeignKey,
    Text,
    BigInteger,
    Boolean,
    Enum,
)
from pgvector.sqlalchemy import Vector

from models import Base

"""
사용자의 질문과 답변을 저장하는 테이블을 생성하는 파이선 파일입니다.
BASE 를 상속받는 클래스를 생성하면, 해당 클래스의 테이블이 자동으로 생성됩니다.

class example_table(Base):
    __tablename__ = "example_table"

    id = Column(Integer, primary_key=True)
    question = Column(Text, nullable=False)
    answer = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, default=text("CURRENT_TIMESTAMP"))

"""


class ConversationList(Base):
    __tablename__ = "conversation_list"

    message_id = Column(BigInteger, primary_key=True, autoincrement=True)
    chat_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    timestamp = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    message = Column(Text, nullable=True)
    sender = Column(Enum("user", "bot", name="sender_enum"), nullable=False)
    is_selected = Column(Boolean, default=False, nullable=False)


class ConversationVector(Base):
    __tablename__ = "conversation_vector"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    chat_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    embedding = Column(Vector(1536), nullable=False)
    timestamp = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    content = Column(Text, nullable=False)
    reference = Column(Enum("summary", "hypothetical_question", name="reference_enum"), nullable=False)
    reference_id = Column(BigInteger, nullable=True)
