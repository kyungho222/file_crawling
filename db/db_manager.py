import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from urllib.parse import quote_plus
import logging
from utils.logging_util import LoggerSingleton
from backend.shared.config import Config

logger = LoggerSingleton.get_logger(logger_name="db.db_manager", level=logging.INFO)

Base = declarative_base()


class DatabaseManager:
    def __init__(self):
        self.base_config = {
            "user": "postgres",
            "password": Config.POSTGRES_DB_PASSWORD,
            "host": "localhost",
            "port": "5432",
        }

    async def create_database(self, db_name):
        """?덈줈???곗씠?곕쿋?댁뒪 ?앹꽦"""
        try:
            # system DB(postgres)???곌껐
            conn = await asyncpg.connect(
                database="postgres",
                user=self.base_config["user"],
                password=self.base_config["password"],
                host=self.base_config["host"],
                port=self.base_config["port"],
            )

            try:
                # ?곗씠?곕쿋?댁뒪媛 ?대? 議댁옱?섎뒗吏 ?뺤씤
                db_exists = await conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1", db_name
                )
                if db_exists:
                    logger.warning(f"?곗씠?곕쿋?댁뒪 {db_name} ?대? 議댁옱?⑸땲??")
                    return {"success": False, "message": "Database already exists."}

                # ?덈줈 ?앹꽦
                logger.warning("[DatabaseManager] runtime schema mutation is disabled | db=%s", db_name)
                return {"success": False, "message": "Runtime schema creation is disabled."}
            except Exception as e:
                logger.error(f"?곗씠?곕쿋?댁뒪 ?앹꽦 以??ㅻ쪟 諛쒖깮: {str(e)}")
                return {"success": False, "error": str(e)}
            finally:
                await conn.close()

        except Exception as e:
            logger.error(f"?곗씠?곕쿋?댁뒪 ?곌껐 ?ㅽ뙣: {str(e)}")
            return {"success": False, "error": str(e)}

