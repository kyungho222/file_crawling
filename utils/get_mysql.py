import os
import pymysql
import logging
import asyncio
from typing import Optional, Iterable
from logs.logging_util import LoggerSingleton
from dotenv import load_dotenv

# 濡쒓굅 ?ㅼ젙
logger = LoggerSingleton.get_logger(
    logger_name="utils.get_mysql", level=logging.INFO
)

load_dotenv()

def get_mysql_connection_crawl(database: str | None = None):
    """
    MySQL ?곗씠?곕쿋?댁뒪 ?곌껐???앹꽦?섎뒗 ?⑥닔
    
    Returns:
        pymysql.Connection: MySQL ?곗씠?곕쿋?댁뒪 ?곌껐 媛앹껜
    """
    try:
        host = os.getenv("MYSQL_HOST")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASS")
        port_str = os.getenv("MYSQL_PORT")
        port = int(port_str)

        database = database
        # logger.info(f"MYSQL_HOST: {host}\nMYSQL_USER: {user}\nMYSQL_PASS: {password}\nMYSQL_PORT: {port}\nMYSQL_DB: {database}")

        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset="utf8",
            autocommit=True,
            ssl=None,  # PyMySQL 0.9.3?먯꽌 SSL 鍮꾪솢?깊솕
            use_unicode=True,
            read_timeout=30,
            write_timeout=30,
        )
        logger.info("MySQL ?곗씠?곕쿋?댁뒪 ?곌껐 ?깃났")
        return conn
    except Exception as e:
        logger.error(f"MySQL ?곌껐 以??ㅻ쪟 諛쒖깮: {str(e)}")
        raise

def get_mysql_connection():
    """
    MySQL ?곗씠?곕쿋?댁뒪 ?곌껐???앹꽦?섎뒗 ?⑥닔

    Returns:
        pymysql.Connection: MySQL ?곗씠?곕쿋?댁뒪 ?곌껐 媛앹껜
    """
    try:
        conn = pymysql.connect(
            host=os.getenv("CHATTY_MYSQL_HOST", "chatty.kr"),
            user=os.getenv("CHATTY_MYSQL_USER", "chatty_mig"),
            password=os.getenv("CHATTY_MYSQL_PASSWORD", "") or "",
            database="chatty",
            port=3306,
            charset="utf8",
        )
        logger.info("MySQL ?곗씠?곕쿋?댁뒪 ?곌껐 ?깃났")
        return conn
    except Exception as e:
        logger.error(f"MySQL ?곌껐 以??ㅻ쪟 諛쒖깮: {str(e)}")
        raise

def get_mysql_connection_naraone():
    """
    MySQL ?곗씠?곕쿋?댁뒪 ?곌껐???앹꽦?섎뒗 ?⑥닔

    Returns:
        pymysql.Connection: MySQL ?곗씠?곕쿋?댁뒪 ?곌껐 媛앹껜
    """
    try:
        conn = pymysql.connect(
            host=os.getenv("GWI_MYSQL_HOST", "dbm.asadal.com"),  # phpMyAdmin ?붾㈃ 湲곗? TCP/IP ?몄뒪??
            user=os.getenv("GWI_MYSQL_USER", "aisearch_user"),
            password=os.getenv("GWI_MYSQL_PASSWORD", "") or "",
            database="Asadal_Chatbot",
            port=3306,
            charset="utf8",
        )
        logger.info("MySQL ?곗씠?곕쿋?댁뒪 ?곌껐 ?깃났")
        return conn
    except Exception as e:
        logger.error(f"MySQL ?곌껐 以??ㅻ쪟 諛쒖깮: {str(e)}")
        raise

async def get_guideline_value_by_id(guideline_id: int):
    """
    ID 媛믩쭔 ?낅젰?섎㈃ ?대떦?섎뒗 value瑜?諛섑솚?섎뒗 媛꾨떒???⑥닔
    
    Args:
        guideline_id (int): 議고쉶??guideline??ID
        
    Returns:
        str: ?대떦 ID??value 媛??먮뒗 None
    """
    try:
        # 鍮꾨룞湲곕줈 ?곗씠?곕쿋?댁뒪 ?곌껐 諛?荑쇰━ ?ㅽ뻾
        loop = asyncio.get_event_loop()
        
        def fetch_guideline_value():
            conn = get_mysql_connection()
            
            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                
                query = """
                    SELECT value 
                    FROM chatbot_guideline 
                    WHERE id = %s
                """
                
                cursor.execute(query, (guideline_id,))
                result = cursor.fetchone()
                
                if result:
                    logger.info(f"Guideline ID {guideline_id}??value 議고쉶 ?깃났")
                    return result['value']
                else:
                    logger.warning(f"Guideline ID {guideline_id}???대떦?섎뒗 ?곗씠?곌? ?놁뒿?덈떎.")
                    return None
                    
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
        
        # 鍮꾨룞湲??ㅽ뻾
        return await loop.run_in_executor(None, fetch_guideline_value)
        
    except Exception as e:
        logger.error(f"MySQL 議고쉶 以??ㅻ쪟 諛쒖깮: {str(e)}")
        raise


async def insert_url_learn_list(data: dict, dbname: str) -> None:
    """
    URL ?숈뒿 ?꾨즺 ?곗씠??MySQL??異붽??섎뒗 ?⑥닔
    """
    try:
        loop = asyncio.get_event_loop()

        def _insert_sync():
            conn = None
            cursor = None
            try:
                chat_bot_id = data.get("chat_bot_id")
                
                conn = get_mysql_connection_crawl(database=dbname)
                logger.info(f"\n = = = = = = = = = = = = = = chat_bot_id: {chat_bot_id} = = = = = = = = = = = = = = \n")
                cursor = conn.cursor()
                
                # ?숈쟻 ?뚯씠釉붾챸 洹쒖튃: ASADAL_{chat_bot_id 留덉?留?12?먮━}_LEARN_LIST
                derived_table = None
                if chat_bot_id:
                    tail = chat_bot_id.replace("-", "")[-12:]
                    derived_table = f"ASADAL_{tail}_LEARN_LIST"
                    logger.info(f"\n\n\n\nDerived table: {derived_table}\n\n\n\n")

                if not derived_table:
                    raise ValueError("MySQL 濡쒓렇 ????뚯씠釉붾챸???놁뒿?덈떎.")

                # ???쎌엯???곗씠??以鍮?(?숈쟻 荑쇰━ ?앹꽦??
                mysql_insert_data = {
                    "subject": data.get("subject", ""),
                    "content": data.get("content", ""),
                    "chunk": int(data.get("chunk_count", 0)),
                    "size": int(data.get("size_bytes", 0)),
                    "content_type": data.get("content_type", "url"),
                    "status": data.get("status", "Y")
                }
                
                # ???숈쟻 荑쇰━ ?앹꽦 (db_operations.py? ?숈씪???⑦꽩)
                columns = ", ".join(f"`{key}`" for key in mysql_insert_data.keys())
                placeholders = ", ".join(["%s"] * len(mysql_insert_data))
                values = list(mysql_insert_data.values())
                
                primary_sql = f"INSERT INTO `{derived_table}` ({columns}, `created_at`) VALUES ({placeholders}, NOW())"
                
                cursor.execute(primary_sql, values)

                conn.commit()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

        await loop.run_in_executor(None, _insert_sync)
        logger.info(f"MySQL URL 濡쒓렇 湲곕줉 ?꾨즺: subject='{data.get('subject', '')[:50]}', url='{data.get('content', '')}', chunk={data.get('chunk_count', 0)}, size={data.get('size_bytes', 0)}")
    except Exception as e:
        # 蹂?湲곕뒫? 蹂댁“ 濡쒓퉭?대?濡??ㅽ뙣?대룄 二쇱슂 ?뚮줈?곕? 諛⑺빐?섏? ?딆쓬
        logger.warning(f"MySQL URL 濡쒓렇 湲곕줉 ?ㅽ뙣: {e}")

async def get_limit_by_chatbot_id(chatbot_id: str):
    """
    梨쀫큸 ID瑜??낅젰諛쏆븘 ?대떦 梨쀫큸???숈쁺???앹꽦 ?쒕룄瑜?諛섑솚?⑸땲??
    """
    try:
        loop = asyncio.get_event_loop()

        def fetch_limit():
            conn = get_mysql_connection()

            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                
                query = """
                    SELECT video_user_count, video_acco_count
                    FROM chatbot_setup_admin 
                    WHERE chat_bot_id = %s
                """

                cursor.execute(query, (chatbot_id,))
                result = cursor.fetchone()
                
                if result:
                    logger.info(f"梨쀫큸 ID {chatbot_id}???숈쁺???앹꽦 ?쒕룄 議고쉶 ?깃났")
                    return result['video_user_count'], result['video_acco_count']
                else:
                    logger.warning(f"梨쀫큸 ID {chatbot_id}???대떦?섎뒗 ?곗씠?곌? ?놁뒿?덈떎.")
                    return None, None
                
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
        
        # 鍮꾨룞湲??ㅽ뻾
        return await loop.run_in_executor(None, fetch_limit)
    
    except Exception as e:
        logger.error(f"梨쀫큸 ID {chatbot_id}???숈쁺???앹꽦 ?쒕룄 議고쉶 以??ㅻ쪟 諛쒖깮: {e}")
        raise

async def get_adult_category_urls():
    """
    chatbot_aisearch_site ?뚯씠釉붿뿉??asadal_chatty_category??'?깆씤' 移댄뀒怨좊━???대떦?섎뒗 URL 由ъ뒪?몃? 諛섑솚?⑸땲??
    """
    try:
        loop = asyncio.get_event_loop()

        def fetch_adult_urls():
            conn = get_mysql_connection()

            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                
                query = """
                    SELECT url 
                    FROM chatbot_aisearch_site 
                    WHERE cate3 = 'AS1729062628'
                """

                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    adult_urls = [result['url'] for result in results]
                    logger.info(f"?깆씤 移댄뀒怨좊━ URL {len(adult_urls)}媛?議고쉶 ?깃났")
                    return adult_urls
                else:
                    logger.info("?깆씤 移댄뀒怨좊━ URL???놁뒿?덈떎.")
                    return []
                
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
        
        # 鍮꾨룞湲??ㅽ뻾
        return await loop.run_in_executor(None, fetch_adult_urls)
    
    except Exception as e:
        logger.error(f"?깆씤 移댄뀒怨좊━ URL 議고쉶 以??ㅻ쪟 諛쒖깮: {e}")
        return []


async def get_content_created_at_by_content(chat_bot_id: str, content: str, database: Optional[str] = None) -> Optional[str]:
    """
    chat_bot_id??留덉?留?12?먮━濡??뚯깮??ASADAL_*_LEARN_LIST ?뚯씠釉붿뿉??
    ?숈씪??content 媛믪쓣 媛吏???됱쓽 content_created_at??諛섑솚?⑸땲??

    Returns ISO-like string (DB raw string) or None.
    """
    try:
        loop = asyncio.get_event_loop()

        def _fetch():
            # ?뚯깮 ?뚯씠釉붾챸 援ъ꽦
            tail = chat_bot_id.replace("-", "")[-12:]
            table_name = f"ASADAL_{tail}_LEARN_LIST"

            conn = get_mysql_connection_crawl(database=database) if database else get_mysql_connection()
            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                # content ?몃뜳?ㅺ? ?놁쓣 ???덉쑝誘濡??뺥솗 留ㅼ묶留??섑뻾
                sql = f"SELECT content_created_at FROM `{table_name}` WHERE content_type = %s AND content = %s ORDER BY id DESC LIMIT 1"
                cursor.execute(sql, ("url", content))
                row = cursor.fetchone()
                return row and row.get("content_created_at")
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

        return await loop.run_in_executor(None, _fetch)
    except Exception as e:
        logger.warning(f"content_created_at ?④굔 議고쉶 ?ㅽ뙣: {e}")
        return None


async def get_content_created_at_map(chat_bot_id: str, contents: Iterable[str], database: Optional[str] = None) -> dict[str, Optional[str]]:
    """
    ?щ윭 content瑜??쒕쾲??議고쉶?섏뿬 {content: content_created_at} 留ㅽ븨??諛섑솚?⑸땲??
    議댁옱?섏? ?딅뒗 寃쎌슦 媛믪? None.
    """
    try:
        contents_list = list(dict.fromkeys([c for c in contents if c]))  # 以묐났 ?쒓굅, 鍮덇컪 ?쒓굅
        if not contents_list:
            return {}

        loop = asyncio.get_event_loop()

        def _fetch_many():
            tail = chat_bot_id.replace("-", "")[-12:]
            table_name = f"ASADAL_{tail}_LEARN_LIST"
            conn = get_mysql_connection()
            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                # IN ?덉쓣 ?ъ슜???쇨큵 議고쉶 ???뚯씠?ъ뿉??留ㅽ븨
                placeholders = ", ".join(["%s"] * len(contents_list))
                sql = f"SELECT content, created_at, content_created_at, content_updated_at FROM `{table_name}` WHERE content_type = %s AND content IN ({placeholders}) AND status = 'Y'"
                cursor.execute(sql, ("url", *contents_list))
                rows = cursor.fetchall() or []
                result_map = {}
                for row in rows:
                    content = row.get("content")
                    if content is not None:
                        # datetime ??鍮꾨Ц????낆쓣 臾몄옄?대줈 ?뺢퇋??
                        raw_val = row.get("content_updated_at") or row.get("content_created_at") or ""
                        val = str(raw_val) if raw_val is not None else ""
                        result_map[content] = val
                # ?먮옒 ?낅젰 ?쒖꽌瑜?蹂댁〈?섏뿬 None 梨꾩썙?ｊ린
                return {c: result_map.get(c) for c in contents_list}
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

        return await loop.run_in_executor(None, _fetch_many)
    except Exception as e:
        logger.warning(f"content_created_at 諛곗튂 議고쉶 ?ㅽ뙣: {e}")
        return {c: None for c in contents}



async def load_news_defaults_from_mysql(host: str, news_class: str, chat_bot_id: Optional[str] = None):
    """
    host 鍮꾧탳 湲곗??쇰줈 is_use='T' ???됱뿉??(category_name, intro, description)留?諛섑솚.
    - ?곗꽑 host_url ?꾨찓?멸낵 ?쇱튂?섎뒗 ?됱쓣 ?섏쭛
    - ?댁뼱??admin.chatty.kr ?됱쓣 異붽??섎릺, category_name 湲곗??쇰줈 以묐났? 蹂댁〈 ?뺤콉:
      ?대? host_url?먯꽌 媛?몄삩 category_name???덉쑝硫??좎?(?ъ씠???곗꽑), ?놁쑝硫?admin ??ぉ 異붽?
    """
    try:
        loop = asyncio.get_event_loop()
        table_name = "news_subscribe"

        def _fetch():
            conn = get_mysql_connection()
            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                # ?숈쟻?쇰줈 ????뚯씠釉??먯깋: host, category_name, intro, description, is_use 而щ읆??紐⑤몢 媛吏??뚯씠釉?
                # 二쇱쓽: ?뚯씠釉붾챸? ?뚮씪誘명꽣 諛붿씤?⑺븷 ???놁쑝誘濡??붿씠?몃━?ㅽ듃/怨좎젙媛믩쭔 吏곸젒 ?쎌엯
                sql = (
                    f"SELECT category_name, intro, description, host, is_use, created_at, updated_at "
                    f"FROM `{table_name}` "
                    f"WHERE is_use = 'T' AND is_deleted = 'F' AND host = %s AND category_name = %s AND chat_bot_id = %s"
                )
                logger.info(f"[load_news_defaults_from_mysql] ?ㅽ뻾 以鍮?table_name={table_name}, host={host}, news_class={news_class}, chat_bot_id={chat_bot_id}")
                cursor.execute(sql, (host, news_class, chat_bot_id))
                rows = cursor.fetchall() or []
                return rows
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

        return await loop.run_in_executor(None, _fetch)
    except Exception as e:
        logger.warning(f"[load_news_defaults_from_mysql] 湲곕낯 ?ㅼ젙 MySQL 議고쉶 ?ㅽ뙣(臾댁떆): {e}")
        return []

async def schedule_get_news_defaults_from_mysql():
    """
    host 鍮꾧탳 湲곗??쇰줈 is_use='T' ???됱뿉??(category_name, intro, description)留?諛섑솚.
    """
    try:
        loop = asyncio.get_event_loop()
        table_name = "news_subscribe"

        def _fetch():
            conn = get_mysql_connection()
            try:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                hosts = ["admin.chatty.kr", "gwi.asadal.com"]
                placeholders = ", ".join(["%s"] * len(hosts))

                sql = (
                    f"SELECT category_name, intro, description, sites, host, is_use, created_at, updated_at, chat_bot_id "
                    f"FROM `{table_name}` WHERE is_use = 'T' AND is_deleted = 'F' AND host IN ({placeholders})"
                )
                logger.info(f"[schedule_get_news_defaults_from_mysql] ?ㅽ뻾 以鍮?table_name={table_name}, host={hosts}")
                cursor.execute(sql, tuple(hosts))
                results = cursor.fetchall() or []
                return results
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

        return await loop.run_in_executor(None, _fetch)
    except Exception as e:
        logger.warning(f"[schedule_get_news_defaults_from_mysql] 湲곕낯 ?ㅼ젙 MySQL 議고쉶 ?ㅽ뙣(臾댁떆): {e}")
        return []
