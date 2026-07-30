import os
from pathlib import Path
from urllib.parse import urlparse
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

_PROJECT_ENV = Path(__file__).resolve().parents[2] / ".env"
_BACKEND_ENV = Path(__file__).resolve().parents[1] / ".env"

def _safe_load_dotenv(path: Path | None = None, *, override: bool = True) -> bool:
    if load_dotenv is None:
        return False
    target = str(path) if path is not None else ".env"
    last_decode_error = None
    for encoding in ("utf-8", "utf-8-sig", "cp949", "euc-kr", "latin-1"):
        try:
            if path is None:
                loaded = bool(load_dotenv(override=override, encoding=encoding))
            else:
                loaded = bool(load_dotenv(dotenv_path=str(path), override=override, encoding=encoding))
            if loaded and encoding != "utf-8":
                print(f"[Config] WARNING: dotenv loaded with fallback encoding={encoding} path={target}", flush=True)
            return loaded
        except UnicodeDecodeError as exc:
            last_decode_error = exc
            continue
        except Exception as exc:
            print(f"[Config] WARNING: dotenv load skipped path={target} err={exc}", flush=True)
            return False
    print(f"[Config] WARNING: dotenv decode skipped path={target} err={last_decode_error}", flush=True)
    return False


if load_dotenv is not None:
    if _PROJECT_ENV.exists():
        _safe_load_dotenv(_PROJECT_ENV, override=True)
    if _BACKEND_ENV.exists():
        _safe_load_dotenv(_BACKEND_ENV, override=True)
    if not _PROJECT_ENV.exists() and not _BACKEND_ENV.exists():
        _safe_load_dotenv(None, override=True)

class Config:
    """
    ?좏뵆由ъ??댁뀡 ?ㅼ젙. ?섍꼍蹂??誘몄꽕???쒖뿉??湲곕낯媛믪쓣 ?ъ슜??Config 遺??誘몄꽕?????숈옉?섎룄濡???
    """
    CHATBOT_HOME = os.path.dirname(os.path.abspath(__file__))
    DATA_HOME = os.getenv("DATA_HOME", "/home/data_dev")
    WEB_CONTENT_DIR = os.getenv("WEB_CONTENT_DIR", "/home/web_content")
    FAISS_INDEX_DIR = os.path.join(DATA_HOME, "FAISS_INDEX")
    PROMPT_DIR = os.path.join(DATA_HOME, "prompts")
    APP_LOG_FILE_PATH = os.path.join(CHATBOT_HOME, "logs", "app_log.txt")
    ACCOUNT_DIR = os.path.join(DATA_HOME, "account_settings")
    MAIN_LOG_FILE_PATH = os.path.join(CHATBOT_HOME, "logs", "main_log.txt")
    UPLOAD_FOLDER = os.path.join(DATA_HOME, "uploads")
    VIEWER_PDF_DIR = os.path.join(DATA_HOME, "viewer_pdf")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "") or ""
    OPENAI_ASADAL_API_KEY = os.getenv("OPENAI_ASADAL_API_KEY", "") or ""
    OPENAI_SECOND_API_KEY = os.getenv("OPENAI_SECOND_API_KEY", "") or ""
    DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "") or ""
    DEEPSEEK_TITLE_API_KEY = os.getenv("DEEPSEEK_TITLE_API_KEY", "") or ""

    # -----------------------------------------------------
    # Selector ?숈뒿 湲곕뒫 ?쒖뼱(?먮룞遺꾨쪟)
    # -----------------------------------------------------
    # Selector ?숈뒿 愿???ㅼ젙(?댁쟾?먮뒗 BOARD_SELECTOR_LEARNING ?섍꼍蹂?섎줈 ?쒖뼱?섏뿀?쇰굹, ?대떦 ?뚮옒洹몃뒗 ?쒓굅??
    UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY", "") or ""
    UPSTAGE_API_URL = os.getenv("UPSTAGE_API_URL", "") or ""
    DASHSCOPE_API_KEY = os.getenv("ALIBABA_API_KEY", "") or ""   # Qwen API ??(Alibaba Cloud)
    MOONSHOT_API_KEY = os.getenv("MOONSHOT_API_KEY", "") or ""   # Kimi (Moonshot AI) API ??    BASIC_CHUNK_SIZE = 1000
    BASIC_CHUNK_OVERLAP = 50
    EDU_PORT = 9001 # cancel_learn?몄텧???꾩슂
    # WEB_DB = "dev_user"  # ?ㅼ젣 ?ъ슜 以묒씤 ?곗씠?곕쿋?댁뒪 ?대쫫
    web_db = os.getenv("MARIA_DB_NAME", "testchatbot1")

    # =====================================================
    # DB Connection Settings (怨좎젙媛?
    # =====================================================
    # PostgreSQL (怨좎젙)
    POSTGRES_DB_HOST = "10.20.20.12"
    POSTGRES_DB_USER = "postgres"
    POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD", "") or ""
    POSTGRES_DB_PORT = 5432
    POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "dev_user")
    
    DB_NAME = POSTGRES_DB_NAME

    # MariaDB: ?댁쁺 湲곕낯媛믪쓣 ?좎??섎릺 濡쒖뺄 SSH ?곕꼸 ?깆? ?섍꼍蹂?섎줈 ??뼱?대떎.
    MARIA_DB_USER = os.getenv("MARIA_DB_USER", "chatty_python")
    MARIA_DB_PASSWORD = os.getenv("MARIA_DB_PASSWORD", "") or ""
    MARIA_DB_HOST = os.getenv("MARIA_DB_HOST", "10.20.20.10")
    MARIA_DB_PORT = int(os.getenv("MARIA_DB_PORT", "3306") or "3306")
    DB_PORT = 5432
    
    # MySQL/MariaDB ?ㅼ젙 (chatty DB?? - 怨좎젙
    CHATTY_MYSQL_USER = "chatty_python"
    CHATTY_MYSQL_PASSWORD = os.getenv("CHATTY_MYSQL_PASSWORD", "") or ""
    CHATTY_MYSQL_HOST = "10.20.20.10"
    CHATTY_MYSQL_PORT = 3306
    CHATTY_MYSQL_DBNAME = "chatty"
    
    # GWI MySQL ?ㅼ젙 (naraone DB??
    # GWI_MYSQL_USER = os.getenv("GWI_MYSQL_USER", None)
    # GWI_MYSQL_PASSWORD = os.getenv("GWI_MYSQL_PASSWORD", None)
    # GWI_MYSQL_HOST = os.getenv("GWI_MYSQL_HOST", None)
    # GWI_MYSQL_PORT = int(os.getenv("GWI_MYSQL_PORT", "3306")) if os.getenv("GWI_MYSQL_PORT") else 3306
    # GWI_MYSQL_DBNAME = os.getenv("GWI_MYSQL_DBNAME", "Asadal_Chatbot")
    
    GWI_MYSQL_USER = os.getenv("GWI_MYSQL_USER", "") or ""
    GWI_MYSQL_PASSWORD = os.getenv("GWI_MYSQL_PASSWORD", "") or ""
    GWI_MYSQL_HOST = os.getenv("GWI_MYSQL_HOST", "10.20.20.10") or "10.20.20.10"
    _gwi_port = os.getenv("GWI_MYSQL_PORT")
    GWI_MYSQL_PORT = int(_gwi_port) if _gwi_port else 3306
    GWI_MYSQL_DBNAME = os.getenv("GWI_MYSQL_DBNAME", "Asadal_Chatbot") or "Asadal_Chatbot"

    DB_USER = POSTGRES_DB_USER
    DB_PASSWORD = POSTGRES_DB_PASSWORD
    DB_HOST = POSTGRES_DB_HOST
    DB_PORT = POSTGRES_DB_PORT
    DB_NAME = POSTGRES_DB_NAME
    try:
        DB_POOL_MIN = max(1, min(int(os.getenv("DB_POOL_MIN", "1") or 1), 64))
    except Exception:
        DB_POOL_MIN = 1
    try:
        DB_POOL_MAX = max(DB_POOL_MIN, min(int(os.getenv("DB_POOL_MAX", "16") or 16), 256))
    except Exception:
        DB_POOL_MAX = 16
    try:
        MARIADB_POOL_MIN = max(1, min(int(os.getenv("MARIADB_POOL_MIN", str(DB_POOL_MIN)) or DB_POOL_MIN), 64))
    except Exception:
        MARIADB_POOL_MIN = DB_POOL_MIN
    try:
        MARIADB_POOL_MAX = max(MARIADB_POOL_MIN, min(int(os.getenv("MARIADB_POOL_MAX", str(DB_POOL_MAX)) or DB_POOL_MAX), 256))
    except Exception:
        MARIADB_POOL_MAX = DB_POOL_MAX
    DB_POOL_CHCK = 300
    # 而ㅻ꽖??acquire 吏곹썑 SELECT 1濡??좎젣 ?좏슚??寃??二쎌? ?뚯폆 議곌린 援먯껜)
    DB_POOL_PRE_PING = str(os.getenv("DB_POOL_PRE_PING", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    # MariaDB(aiomysql): ?좏쑕 ?뚯폆??諛⑺솕踰?LB/server wait_timeout 蹂대떎 ?ㅻ옒 ?댁? ?딄쾶 二쇨린?곸쑝濡?援먯껜
    # (湲곕낯 300s ???섍꼍???곕씪 MARIA_POOL_RECYCLE=300~1800 議곗젙)
    try:
        _pr = int(os.getenv("MARIA_POOL_RECYCLE", os.getenv("DB_POOL_RECYCLE", "240")) or 240)
    except Exception:
        _pr = 240
    DB_POOL_RECYCLE = max(60, min(86400, _pr))
    try:
        _ct = int(os.getenv("MARIA_CONNECT_TIMEOUT", "8") or 8)
    except Exception:
        _ct = 8
    MARIA_CONNECT_TIMEOUT = max(3, min(120, _ct))
    try:
        _mat = float(os.getenv("MARIADB_POOL_ACQUIRE_TIMEOUT_SEC", "12") or 12)
    except Exception:
        _mat = 12.0
    MARIADB_POOL_ACQUIRE_TIMEOUT_SEC = max(1.0, min(120.0, _mat))
    try:
        _mysql_acquire_t = float(os.getenv("MYSQL_POOL_ACQUIRE_TIMEOUT_SEC", "5") or 5)
    except Exception:
        _mysql_acquire_t = 5.0
    MYSQL_POOL_ACQUIRE_TIMEOUT_SEC = max(1.0, min(120.0, _mysql_acquire_t))
    MARIADB_DYNAMIC_JOB_SHARE = str(
        os.getenv("MARIADB_DYNAMIC_JOB_SHARE", "1")
    ).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    try:
        _ppt = float(os.getenv("MARIADB_PRE_PING_TIMEOUT_SEC", "8") or 8)
    except Exception:
        _ppt = 8.0
    MARIADB_PRE_PING_TIMEOUT_SEC = max(0.2, min(30.0, _ppt))
    try:
        _mqt = float(os.getenv("MARIADB_QUERY_TIMEOUT_SEC", "20") or 20)
    except Exception:
        _mqt = 20.0
    MARIADB_QUERY_TIMEOUT_SEC = max(1.0, min(300.0, _mqt))
    try:
        _maria_op_slow = float(os.getenv("MARIADB_OPERATION_SLOW_LOG_MS", "1000") or 1000)
    except Exception:
        _maria_op_slow = 1000.0
    MARIADB_OPERATION_SLOW_LOG_MS = max(0.0, min(600000.0, _maria_op_slow))
    MARIADB_TRANSIENT_WARNING_LOG_LEVEL = os.getenv(
        "MARIADB_TRANSIENT_WARNING_LOG_LEVEL",
        "DEBUG",
    )
    try:
        _mysql_qt = float(os.getenv("MYSQL_QUERY_TIMEOUT_SEC", "20") or 20)
    except Exception:
        _mysql_qt = 20.0
    MYSQL_QUERY_TIMEOUT_SEC = max(1.0, min(300.0, _mysql_qt))
    try:
        _db_retry_attempts = int(os.getenv("DB_RETRY_ATTEMPTS", "3") or 3)
    except Exception:
        _db_retry_attempts = 3
    DB_RETRY_ATTEMPTS = max(1, min(10, _db_retry_attempts))
    try:
        _rids = float(os.getenv("DB_RETRY_INITIAL_DELAY_SEC", "0.5") or 0.5)
    except Exception:
        _rids = 0.5
    DB_RETRY_INITIAL_DELAY_SEC = max(0.05, min(30.0, _rids))
    try:
        _rbm = float(os.getenv("DB_RETRY_BACKOFF_MULTIPLIER", "2.0") or 2.0)
    except Exception:
        _rbm = 2.0
    DB_RETRY_BACKOFF_MULTIPLIER = max(1.0, min(5.0, _rbm))
    try:
        _pg_stmt = int(os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS", "180000") or 180000)
    except Exception:
        _pg_stmt = 180000
    POSTGRES_STATEMENT_TIMEOUT_MS = max(10000, min(900000, _pg_stmt))
    try:
        _pg_connect_timeout = float(os.getenv("POSTGRES_CONNECT_TIMEOUT_SEC", "5") or 5)
    except Exception:
        _pg_connect_timeout = 5.0
    POSTGRES_CONNECT_TIMEOUT_SEC = max(1.0, min(120.0, _pg_connect_timeout))
    try:
        _pg_pool_timeout = float(os.getenv("POSTGRES_POOL_TIMEOUT_SEC", "5") or 5)
    except Exception:
        _pg_pool_timeout = 5.0
    POSTGRES_POOL_TIMEOUT_SEC = max(1.0, min(120.0, _pg_pool_timeout))
    try:
        _pg_retry = int(os.getenv("POSTGRES_QUERY_RETRY_COUNT", "2") or 2)
    except Exception:
        _pg_retry = 2
    POSTGRES_QUERY_RETRY_COUNT = max(0, min(5, _pg_retry))
    try:
        _pg_retry_delay = float(os.getenv("POSTGRES_QUERY_RETRY_DELAY_SEC", "0.5") or 0.5)
    except Exception:
        _pg_retry_delay = 0.5
    POSTGRES_QUERY_RETRY_DELAY_SEC = max(0.05, min(30.0, _pg_retry_delay))
    try:
        _pg_learn_write = int(os.getenv("POSTGRES_LEARN_WRITE_MAX_CONCURRENCY", "2") or 2)
    except Exception:
        _pg_learn_write = 2
    POSTGRES_LEARN_WRITE_MAX_CONCURRENCY = max(1, min(16, _pg_learn_write))
    LONG_TERM_COUNT = 30
    IMAGE_API_URL = "https://dev.chatbaram.com" # ?대?吏 ?앹꽦 ?쒕쾭 二쇱냼
    GPU_SERVER_URL = os.getenv("GPU_SERVER_URL", "http://218.146.11.93:5700") # GPU ?쒕쾭 二쇱냼
    CALLBACK_BASE_URL = os.getenv("PROD_CALLBACK_BASE_URL", os.getenv("DEV_CALLBACK_BASE_URL", "https://test.han.kr"))
    CALLBACK_URL = os.getenv("PROD_CALLBACK_URL", os.getenv("DEV_CALLBACK_URL", "https://test.han.kr/chat/doc_summary_homepage_callback.php"))
    
    # ?뙋 ?뱀궗?댄듃 踰덉뿭 ?ㅼ젙
    WEBSITE_TRANSLATION_CHUNK_SIZE = 5000        # HTML 泥?겕 遺꾪븷 ?ш린
    WEBSITE_TRANSLATION_MAX_CONCURRENT = 200

    # ?? ?섎뱶?⑥뼱 理쒖쟻???ㅼ젙 (Xeon E5-2670 + 128GB RAM)
    HARDWARE_PHYSICAL_CORES = 16     # 臾쇰━ 肄붿뼱 ??    HARDWARE_LOGICAL_CORES = 32      # ?쇰━ 肄붿뼱 ??(?섏씠?쇱뒪?덈뵫)
    HARDWARE_TOTAL_RAM_GB = 256      # 珥?RAM ?⑸웾 (GB)
    
    # ?뙋 硫?곗쑀? ?섍꼍 由ъ냼??愿由?(20紐?湲곗? ?ㅺ퀎 - 256GB RAM ?쒖슜)
    GLOBAL_MAX_CONCURRENT_JOBS = 20          # ?숈떆 ?숈뒿 ?묒뾽 ?? 20紐?    GLOBAL_MAX_CONCURRENT_FILES = 60         # ?꾩껜 ?쒖뒪???뚯씪 ?숈떆 泥섎━ ?쒗븳 (20紐?횞 3媛?
    GLOBAL_MAX_CONCURRENT_URLS = 80          # ?꾩껜 ?쒖뒪??URL ?숈떆 泥섎━ ?쒗븳 (20紐?횞 4媛?
    GLOBAL_MAX_CONCURRENT_CHUNKS = 100       # ?꾩껜 ?쒖뒪??泥?겕 ?숈떆 泥섎━ ?쒗븳 (20紐?횞 5媛?
    # ?ъ슜?먮퀎 由ъ냼???좊떦 (20紐?遺꾩궛)
    USER_MAX_FILES_PER_JOB = 4              # ?ъ슜?먮퀎 理쒕? ?뚯씪 ?숈떆 泥섎━ (2?? 利앷?)
    USER_MAX_CHUNKS_PER_FILE = 8            # ?뚯씪??理쒕? 泥?겕 諛곗튂 ?ш린 (4?? 利앷?)
    # 硫?고봽濡쒖꽭??理쒖쟻???ㅼ젙 (32?ㅻ젅???쒖슜)
    MAX_CONCURRENT_URL_CRAWLING = 80     # URL ?щ·留?理쒕? ?숈떆 泥섎━ (40??0)
    MAX_CONCURRENT_TEXT_PROCESSING = 24  # ?띿뒪??泥섎━ 理쒕? ?숈떆 泥섎━ (10??4)
    MAX_CONCURRENT_FILE_PROCESSING = 32  # ?뚯씪 泥섎━ 理쒕? ?숈떆 泥섎━ (16??2)
    # 諛곗튂 泥섎━ 理쒖쟻???ㅼ젙 (256GB RAM ?쒖슜)
    TXT_BATCH_SIZE_SMALL = 6    # ?묒? TXT ?뚯씪 諛곗튂 ?ш린 (3??)
    TXT_BATCH_SIZE_MEDIUM = 8   # 以묎컙 TXT ?뚯씪 諛곗튂 ?ш린 (4??)
    TXT_BATCH_SIZE_LARGE = 10   # ??TXT ?뚯씪 諛곗튂 ?ш린 (5??0)
    URL_BATCH_SIZE_SMALL = 4    # ?묒? URL ?섏씠吏 諛곗튂 ?ш린 (2??)
    URL_BATCH_SIZE_MEDIUM = 6   # 以묎컙 URL ?섏씠吏 諛곗튂 ?ш린 (3??)
    URL_BATCH_SIZE_LARGE = 8    # ??URL ?섏씠吏 諛곗튂 ?ш린 (4??)


    # ?? URL 諛곗튂 泥섎━ 理쒖쟻???ㅼ젙 (???媛쒖꽑)
    # URL ?щ윭 媛?泥섎━ ???ъ슜?섎뒗 怨좎꽦??諛곗튂 泥섎━ ?ㅼ젙
    
        # 諛곗튂 泥섎━ ?꾧퀎媛?    URL_BATCH_OPTIMIZATION_THRESHOLD = 1    # 3媛??댁긽 URL????諛곗튂 理쒖쟻???곸슜

    # ?꾨쿋??諛곗튂 ?ш린 (200??00?쇰줈 利앷?)
    URL_GLOBAL_EMBEDDING_BATCH_SIZE = 400   # ?щ윭 URL 泥?겕瑜??듯빀??????꾨쿋??諛곗튂
    URL_SINGLE_EMBEDDING_BATCH_SIZE = 400   # ?⑥씪 URL 泥섎━ ???꾨쿋??諛곗튂

    # DB 踰뚰겕 ?쎌엯 ?ш린 (500??000?쇰줈 利앷?)
    URL_GLOBAL_DB_BULK_SIZE = 1000          # ?щ윭 URL 泥?겕瑜??듯빀?????DB 踰뚰겕 ?쎌엯
    URL_SINGLE_DB_BULK_SIZE = 1000          # ?⑥씪 URL 泥섎━ ??DB 踰뚰겕 ?쎌엯

    # ?숈떆 泥섎━ ??(32?ㅻ젅???쒖슜)
    URL_GLOBAL_CONCURRENT_LIMIT = 100       # ?щ윭 URL 諛곗튂 泥섎━ ???숈떆 URL ??(50??00)
    URL_SINGLE_CONCURRENT_LIMIT = 80        # ?⑥씪 URL ?먮뒗 ?뚯닔 URL 泥섎━ ???숈떆 ??(40??0)

    # HTTP ?곌껐 理쒖쟻??    URL_HTTP_CONNECTION_POOL_SIZE = 200     # HTTP ?곌껐 ? ?ш린 (100??00)
    URL_HTTP_CONNECTION_PER_HOST = 20       # ?몄뒪?몃떦 理쒕? ?곌껐 ??(10??0)
    URL_HTTP_DNS_CACHE_TTL = 600            # DNS 罹먯떆 TTL (珥? - ?곌껐 ?ъ궗??洹밸???    
    # ?덉쭏 ?꾪꽣留??ㅼ젙
    URL_CONTENT_QUALITY_THRESHOLD = 0.3     # 肄섑뀗痢??덉쭏 ?꾧퀎媛?(0.3 ?댁긽留????
    URL_MIN_CONTENT_LENGTH = 100            # 理쒖냼 肄섑뀗痢?湲몄씠 (100???댁긽)
    URL_MIN_CHUNK_COUNT = 2                 # 理쒖냼 泥?겕 ??(2媛??댁긽)
    
    # ?ъ떆??諛???꾩븘???ㅼ젙
    URL_MAX_RETRIES = 3                     # HTTP ?붿껌 理쒕? ?ъ떆???잛닔
    URL_TIMEOUT_TOTAL = 45                  # HTTP 珥???꾩븘??(珥?
    URL_TIMEOUT_CONNECT = 8                 # HTTP ?곌껐 ??꾩븘??(珥?
    URL_TIMEOUT_READ = 25                   # HTTP ?쎄린 ??꾩븘??(珥?
    
    # Playwright ?ㅼ젙 (?숈쟻 ?섏씠吏 泥섎━?? - 256GB RAM ?쒖슜
    PLAYWRIGHT_MAX_CONCURRENT = 16           # ?숈떆 ?ㅽ뻾 Playwright 釉뚮씪?곗? ??(16??2, 32?ㅻ젅???쒖슜)
    PLAYWRIGHT_TIMEOUT = 60                  # Playwright ?꾩껜 ??꾩븘??(珥?
    PLAYWRIGHT_PAGE_TIMEOUT = 50             # Playwright ?섏씠吏 濡쒕뱶 ??꾩븘??(珥?
    PLAYWRIGHT_MAX_RETRIES = 3               # Playwright 理쒕? ?ъ떆???잛닔
    PLAYWRIGHT_HEADLESS = True               # False硫?GUI ?꾩슂(Worker/?쒕쾭?먯꽌??True 沅뚯옣)

    # 濡쒓퉭 諛?紐⑤땲?곕쭅
    URL_BATCH_LOGGING_LEVEL = "INFO"        # 諛곗튂 泥섎━ 濡쒓퉭 ?덈꺼
    URL_ENABLE_PERFORMANCE_MONITORING = True # ?깅뒫 紐⑤땲?곕쭅 ?쒖꽦??    URL_PERFORMANCE_LOG_INTERVAL = 10       # ?깅뒫 濡쒓렇 異쒕젰 媛꾧꺽 (媛쒖닔)
