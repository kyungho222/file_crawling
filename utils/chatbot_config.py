import sqlite3
import os
from config import Config
import logging
from logs.logging_util import LoggerSingleton
from db.db_operations import execute_query
from utils.whoami import get_upload_dir_from_db

logger = LoggerSingleton.get_logger(
    logger_name="utils.chatbot_config", level=logging.INFO
)

# 而щ읆 ?뺤쓽
CHATBOT_CONFIG_COLUMNS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "chat_bot_id": "VARCHAR(36) DEFAULT 'default'",
    "db_name": "VARCHAR(100) DEFAULT 'default'",
    "rag_source": "TEXT DEFAULT 'Y'",
    "web_source": "TEXT DEFAULT 'Y'",
    "llm_source": "TEXT DEFAULT 'Y'",
    "source_num": "INTEGER DEFAULT 0",
    "use_recent": "TEXT DEFAULT 'Y'",
    "use_websearch": "TEXT DEFAULT 'Y'",
    "use_rag": "TEXT DEFAULT 'Y'",
    "vector_k": "INTEGER DEFAULT 30",
    "vector_threshold": "FLOAT DEFAULT 0.35",
    "model_name": "VARCHAR(100) DEFAULT 'gpt'",
    "bm25_ratio": "FLOAT DEFAULT 0.3",
    "use_suggested_questions": "TEXT DEFAULT 'Y'",  # 異붿쿇 吏덈Ц ?ъ슜 ?щ?
    "use_suggested_count": "INTEGER DEFAULT 3",  # 異붿쿇 吏덈Ц 媛쒖닔
    "use_suggested_question_type": "TEXT DEFAULT 'Long'",  # 異붿쿇 吏덈Ц ???Long(臾몄옣),Short(?⑥뼱)
    "temperature": "FLOAT DEFAULT 0",  # ?듬? ?⑤룄 (李쎌쓽??議곗젅)
    "max_turns": "INTEGER DEFAULT 3",  # 硫?고꽩 媛?닔 (?댁쟾 ???湲곗뼲 ????
    "personal_info_filter": "TEXT DEFAULT 'N'",  # 媛쒖씤?뺣낫 ?꾪꽣留?    "harmful_content_filter": "TEXT DEFAULT 'Y'",  # ?좏빐?뺣낫 ?꾪꽣留?    "harmful_content_keyword": "TEXT DEFAULT ''",  # ?좏빐?뺣낫 ?ㅼ썙??    "memory_expire_time": "INTEGER DEFAULT 86400",  # 硫붾え由?留뚮즺 ?쒓컙(珥? - 湲곕낯媛? 1??    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    # ?덈줈??而щ읆 異붽? ???ш린??異붽?
    # 'new_column': 'DATA_TYPE DEFAULT VALUE'
}


def get_create_table_sql():
    """
    Runtime schema initialization SQL is not generated.
    """
    columns_sql = ",\n        ".join(
        f"{col} {type_}" for col, type_ in CHATBOT_CONFIG_COLUMNS.items()
    )
    raise RuntimeError("runtime schema creation is disabled")


async def init_chatbot_config_db(db_name: str, chat_bot_id: str):
    """
    chatbot_config.db瑜??앹꽦?섍퀬 ?꾩슂???뚯씠釉붿쓣 珥덇린?뷀븯???⑥닔

    Args:
        db_name (str): ?곗씠?곕쿋?댁뒪 ?대쫫
        chat_bot_id (str): 梨쀫큸 ID
    """
    # ?낅줈???붾젆?좊━ 寃쎈줈 媛?몄삤湲?
    upload_dir = await get_upload_dir_from_db(db_name, chat_bot_id)
    if not upload_dir:
        raise ValueError("?낅줈???붾젆?좊━ ?뺣낫瑜?李얠쓣 ???놁뒿?덈떎.")

    # Config.ACCOUNT_DIR ?섏쐞???곗씠?곕쿋?댁뒪 ?앹꽦
    db_dir = os.path.join(Config.ACCOUNT_DIR, upload_dir)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    db_path = os.path.join(db_dir, "chatbot_config.db")

    # ?곗씠?곕쿋?댁뒪 ?곌껐
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # chatbot_config ?뚯씠釉??앹꽦
    logger.warning("[ChatbotConfig] runtime schema mutation is disabled; skipping table initialization")

    # 珥덇린 ?곗씠???쎌엯
    cursor.execute(
        """
        INSERT INTO chatbot_config 
        (chat_bot_id, db_name, rag_source, web_source, llm_source, use_recent, use_websearch, use_rag, vector_k, vector_threshold, model_name, source_num, bm25_ratio, use_suggested_questions, use_suggested_count, use_suggested_question_type, temperature, max_turns, personal_info_filter, harmful_content_filter, harmful_content_keyword, memory_expire_time)
        VALUES (?, ?, 'Y', 'Y','Y', 'Y', 'Y', 'Y', 30, 0.35, 'gpt', 0, 0.3, 'Y', 3, 'Long', 0, 3, 'N', 'N', '', 86400)
        """,
        (chat_bot_id, db_name),
    )

    # 蹂寃쎌궗?????
    conn.commit()
    conn.close()

    logger.info(f"Database initialized at: {db_path}")


async def check_and_add_missing_columns(db_path: str):
    """
    ?곗씠?곕쿋?댁뒪???꾨씫??而щ읆???뺤씤?섍퀬 異붽??섎뒗 ?⑥닔

    Args:
        db_path (str): ?곗씠?곕쿋?댁뒪 ?뚯씪 寃쎈줈
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # ?꾩옱 ?뚯씠釉붿쓽 而щ읆 ?뺣낫 議고쉶
        cursor.execute("PRAGMA table_info(chatbot_config)")
        existing_columns = [row[1] for row in cursor.fetchall()]

        # CHATBOT_CONFIG_COLUMNS???뺤쓽??紐⑤뱺 而щ읆 ?뺤씤
        for column_name, column_type in CHATBOT_CONFIG_COLUMNS.items():
            if column_name not in existing_columns:
                logger.info(f"?꾨씫??而щ읆 異붽?: {column_name}")
                continue

        logger.info("而щ읆 ?뺤씤 諛?異붽? ?꾨즺")
    except Exception as e:
        logger.error(f"而щ읆 ?뺤씤 諛?異붽? 以??ㅻ쪟: {e}")
    finally:
        if "conn" in locals():
            conn.close()


async def get_chatbot_config(chat_bot_id: str, db_name: str):
    """
    SQLite chatbot_config.db?먯꽌 梨쀫큸 ?ㅼ젙??議고쉶?⑸땲??
    DB ?뚯씪???놁쑝硫??먮룞?쇰줈 ?앹꽦?⑸땲??

    Args:
        chat_bot_id (str): 梨쀫큸 ID
        db_name (str): ?곗씠?곕쿋?댁뒪 ?대쫫

    Returns:
        dict: ?ㅼ젙 ?곗씠???먮뒗 None
    """
    try:
        # 1. DB?먯꽌 ?붾젆?좊━ ?뺣낫 議고쉶
        upload_dir = await get_upload_dir_from_db(db_name, chat_bot_id)
        if not upload_dir:
            logger.error("?낅줈???붾젆?좊━ ?뺣낫瑜?李얠쓣 ???놁뒿?덈떎.")
            return None

        # 2. SQLite DB ?뚯씪 寃쎈줈 ?앹꽦
        db_path = os.path.join(Config.ACCOUNT_DIR, upload_dir, "chatbot_config.db")

        # DB ?뚯씪???놁쑝硫??앹꽦
        if not os.path.exists(db_path):
            logger.info(f"?ㅼ젙 DB ?뚯씪???놁뼱 ?덈줈 ?앹꽦?⑸땲?? {db_path}")
            await init_chatbot_config_db(db_name, chat_bot_id)
        else:
            # DB ?뚯씪???덉쑝硫??꾨씫??而щ읆 ?뺤씤 諛?異붽?
            await check_and_add_missing_columns(db_path)

        # 3. SQLite ?곌껐 諛?議고쉶
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT rag_source, web_source, llm_source, use_recent, use_websearch, use_rag, model_name, source_num, bm25_ratio, vector_k, vector_threshold, use_suggested_questions, use_suggested_count, use_suggested_question_type, temperature, max_turns, personal_info_filter, harmful_content_filter, memory_expire_time
            FROM chatbot_config 
            WHERE chat_bot_id = ?
        """,
            (chat_bot_id,),
        )

        row = cursor.fetchone()

        if row:
            columns = [
                "rag_source",
                "web_source",
                "llm_source",
                "use_recent",
                "use_websearch",
                "use_rag",
                "model_name",
                "source_num",
                "bm25_ratio",
                "vector_k",
                "vector_threshold",
                "use_suggested_questions",
                "use_suggested_count",
                "use_suggested_question_type",
                "temperature",
                "max_turns",
                "personal_info_filter",
                "harmful_content_filter",
                "memory_expire_time",
            ]
            return dict(zip(columns, row))

        # ?ㅼ젙???놁쑝硫?湲곕낯 ?ㅼ젙 ?앹꽦 (??遺遺꾩? init_chatbot_config_db?먯꽌 泥섎━?섎?濡??쒓굅)
        logger.info(f"梨쀫큸 ?ㅼ젙???놁뒿?덈떎: {chat_bot_id}")
        return None

    except Exception as e:
        logger.error(f"chatbot_config 議고쉶 以??ㅻ쪟: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


async def get_harmful_content_keyword(chat_bot_id: str, db_name: str):
    """
    SQLite chatbot_config.db?먯꽌 ?좏빐 肄섑뀗痢??ㅼ썙?쒕쭔 議고쉶?⑸땲??
    DB ?뚯씪???놁쑝硫??먮룞?쇰줈 ?앹꽦?⑸땲??

    Args:
        chat_bot_id (str): 梨쀫큸 ID
        db_name (str): ?곗씠?곕쿋?댁뒪 ?대쫫

    Returns:
        str: ?좏빐 肄섑뀗痢??ㅼ썙???먮뒗 鍮?臾몄옄??    """
    try:
        # 1. DB?먯꽌 ?붾젆?좊━ ?뺣낫 議고쉶
        upload_dir = await get_upload_dir_from_db(db_name, chat_bot_id)
        if not upload_dir:
            logger.error("?낅줈???붾젆?좊━ ?뺣낫瑜?李얠쓣 ???놁뒿?덈떎.")
            return ""

        # 2. SQLite DB ?뚯씪 寃쎈줈 ?앹꽦
        db_path = os.path.join(Config.ACCOUNT_DIR, upload_dir, "chatbot_config.db")

        # DB ?뚯씪???놁쑝硫??앹꽦
        if not os.path.exists(db_path):
            logger.info(f"?ㅼ젙 DB ?뚯씪???놁뼱 ?덈줈 ?앹꽦?⑸땲?? {db_path}")
            await init_chatbot_config_db(db_name, chat_bot_id)
        else:
            # DB ?뚯씪???덉쑝硫??꾨씫??而щ읆 ?뺤씤 諛?異붽?
            await check_and_add_missing_columns(db_path)

        # 3. SQLite ?곌껐 諛?議고쉶
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT harmful_content_keyword
            FROM chatbot_config 
            WHERE chat_bot_id = ?
        """,
            (chat_bot_id,),
        )

        row = cursor.fetchone()

        if row:
            return row[0] if row[0] else ""

        logger.info(f"?좏빐 肄섑뀗痢??ㅼ썙?쒓? ?놁뒿?덈떎: {chat_bot_id}")
        return ""

    except Exception as e:
        logger.error(f"?좏빐 肄섑뀗痢??ㅼ썙??議고쉶 以??ㅻ쪟: {e}")
        return ""
    finally:
        if "conn" in locals():
            conn.close()


async def save_chatbot_config(chat_bot_id: str, db_name: str, config_data: dict):
    """
    chatbot_config.db??梨쀫큸 ?ㅼ젙????ν빀?덈떎.
    """
    conn = None
    try:
        # 1. chatbot_config.db ?뚯씪 寃쎈줈 ?뺤씤
        upload_dir = await get_upload_dir_from_db(db_name, chat_bot_id)
        if not upload_dir:
            logger.error("?낅줈???붾젆?좊━ ?뺣낫瑜?李얠쓣 ???놁뒿?덈떎.")
            return {
                "status": "error",
                "message": "?낅줈???붾젆?좊━ ?뺣낫瑜?李얠쓣 ???놁뒿?덈떎.",
            }

        # 2. chatbot_config.db ?뚯씪 寃쎈줈 ?앹꽦
        db_path = os.path.join(Config.ACCOUNT_DIR, upload_dir, "chatbot_config.db")
        logger.info(f"?ㅼ젙 DB ?뚯씪 寃쎈줈: {db_path}")

        # 3. DB ?뚯씪???놁쑝硫??앹꽦
        if not os.path.exists(db_path):
            logger.info(f"?ㅼ젙 DB ?뚯씪???놁뼱 ?덈줈 ?앹꽦?⑸땲?? {db_path}")
            await init_chatbot_config_db(db_name, chat_bot_id)
        else:
            # DB ?뚯씪???덉쑝硫??꾨씫??而щ읆 ?뺤씤 諛?異붽?
            await check_and_add_missing_columns(db_path)

        # 4. SQLite ?곌껐
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 5. ?꾩옱 ?ㅼ젙 ?뺤씤
        cursor.execute(
            """
            SELECT 1 FROM chatbot_config 
            WHERE chat_bot_id = ?
        """,
            (chat_bot_id,),
        )

        exists = cursor.fetchone() is not None
        logger.info(f"湲곗〈 ?ㅼ젙 議댁옱 ?щ?: {exists}")

        # 6. ?붾쾭源낆쓣 ?꾪븳 濡쒓렇 異붽?
        logger.info(f"諛쏆? config_data: {config_data}")
        
        # 7. ?낅뜲?댄듃???꾨뱶 異붿텧 (chat_bot_id, db_name ?쒖쇅)
        update_fields = {}
        for k, v in config_data.items():
            if k not in ["chat_bot_id", "db_name"] and v is not None:
                update_fields[k] = v
                logger.info(f"?낅뜲?댄듃???꾨뱶 異붽?: {k} = {v}")
        
        logger.info(f"?낅뜲?댄듃???꾨뱶: {update_fields}")

        # 8. 鍮??꾨뱶??寃쎌슦 湲곕낯媛??ㅼ젙
        if not update_fields:
            logger.info("?낅뜲?댄듃???꾨뱶媛 ?놁뼱 湲곕낯媛믪쓣 ?ㅼ젙?⑸땲??")
            # 湲곕낯媛??ㅼ젙
            default_fields = {
                "rag_source": "Y",
                "web_source": "Y", 
                "llm_source": "Y",
                "use_recent": "Y",
                "use_websearch": "Y",
                "use_rag": "Y",
                "vector_k": 30,
                "vector_threshold": 0.35,
                "model_name": "gpt",
                "source_num": 0,
                "bm25_ratio": 0.3,
                "use_suggested_questions": "Y",
                "use_suggested_count": 3,
                "use_suggested_question_type": "Long",
                "temperature": 0.0,
                "max_turns": 3,
                "personal_info_filter": "N",
                "harmful_content_filter": "N",
                "harmful_content_keyword": "",
                "memory_expire_time": 86400
            }
            update_fields = default_fields
            logger.info(f"湲곕낯媛믪쑝濡??ㅼ젙???꾨뱶: {update_fields}")

        if exists:
            # 7. ?낅뜲?댄듃 荑쇰━ ?앹꽦
            set_clause = ", ".join([f"{k} = ?" for k in update_fields.keys()])
            update_query = f"""
                UPDATE chatbot_config 
                SET {set_clause}
                WHERE chat_bot_id = ?
            """

            # ?뚮씪誘명꽣 ?앹꽦
            params = list(update_fields.values()) + [chat_bot_id]

            # ?낅뜲?댄듃 ?ㅽ뻾
            cursor.execute(update_query, params)
            conn.commit()
            logger.info(
                f"?ㅼ젙 ?낅뜲?댄듃 ?꾨즺 - chat_bot_id: {chat_bot_id}, ?낅뜲?댄듃???꾨뱶: {update_fields}"
            )

            return {"status": "success", "message": "?낅뜲?댄듃 ?꾨즺"}
        else:
            # 8. ?쎌엯 荑쇰━ ?앹꽦
            # 湲곕낯 ?꾨뱶 異붽?
            insert_fields = {
                "chat_bot_id": chat_bot_id,
                "db_name": db_name,
                **update_fields,
            }

            columns = ", ".join(insert_fields.keys())
            placeholders = ", ".join(["?" for _ in insert_fields])

            insert_query = f"""
                INSERT INTO chatbot_config ({columns})
                VALUES ({placeholders})
            """

            # ?쎌엯 ?ㅽ뻾
            cursor.execute(insert_query, list(insert_fields.values()))
            conn.commit()
            logger.info(
                f"???ㅼ젙 ?쎌엯 ?꾨즺 - chat_bot_id: {chat_bot_id}, ?쎌엯???꾨뱶: {insert_fields}"
            )

            return {"status": "success", "message": "?쎌엯 ?꾨즺"}

    except Exception as e:
        logger.error(f"梨쀫큸 ?ㅼ젙 ???以??ㅻ쪟: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if conn:
            conn.close()

