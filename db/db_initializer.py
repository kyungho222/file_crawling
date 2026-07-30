from backend.shared.config import Config

from utils.logging_util import LoggerSingleton
import logging

logger = LoggerSingleton.get_logger(logger_name="db.db_initializer", level=logging.INFO)

from db.db_postgres import engine_async
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base

from models import Base


async def init_db():
    async with engine_async.begin() as conn:
        # ????곗씠?곕쿋?댁뒪?먯꽌 pgvector ?뺤옣???앹꽦 (?대? 議댁옱?섎㈃ ?꾨Т ?곹뼢 ?놁씠 ?섏뼱媛?
        logger.warning("[DBInit] runtime schema mutation is disabled")

        # ?뚯씠釉??앹꽦
        logger.warning("[DBInit] runtime schema mutation is disabled")

    logger.info("[DBInit] runtime schema initialization skipped")

