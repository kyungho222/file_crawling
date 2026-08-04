import os
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

class Config:
    """
    애플리케이션 설정. 환경변수 미설정 시에도 기본값을 사용해 Config 부재/미설정 시 동작하도록 함.
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

    # -----------------------------------------------------
    # Selector 학습 기능 제어(자동분류)
    # -----------------------------------------------------
    # Selector 학습 관련 설정(이전에는 BOARD_SELECTOR_LEARNING 환경변수로 제어되었으나, 해당 플래그는 제거됨)
    UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY", "") or ""
    UPSTAGE_API_URL = os.getenv("UPSTAGE_API_URL", "") or ""
    DASHSCOPE_API_KEY = os.getenv("ALIBABA_API_KEY", "") or ""   # Qwen API 키 (Alibaba Cloud)
    MOONSHOT_API_KEY = os.getenv("MOONSHOT_API_KEY", "") or ""   # Kimi (Moonshot AI) API 키
    BASIC_CHUNK_SIZE = 1000
    BASIC_CHUNK_OVERLAP = 50
    EDU_PORT = 9001 # cancel_learn호출실 필요
    # WEB_DB = "dev_user"  # 실제 사용 중인 데이터베이스 이름
    web_db = os.getenv("MARIA_DB_NAME", "testchatbot1")

    # =====================================================
    # DB Connection Settings (고정값)
    # =====================================================
    # PostgreSQL (고정)
    # POSTGRES_DB_HOST = "10.20.20.12"
    POSTGRES_DB_HOST = "milvus.chatbaram.com"
    POSTGRES_DB_USER = "postgres"
    POSTGRES_DB_PASSWORD = "dktkekf0215@#"
    POSTGRES_DB_PORT = 5432
    POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "dev_user")
    
    DB_NAME = POSTGRES_DB_NAME

    # 로컬 host   
    # PG_HOST = "milvus.chatbaram.com"
    # MILVUS_HOST = "110.45.147.71"
    # MARIA__USER = "chatty_master"
    # MARIA__HOST = "110.45.147.58"


    # MariaDB (고정)
    # MARIA_DB_USER = "chatty_python"
    MARIA_DB_PASSWORD = "dktkekf0215@#"
    # MARIA_DB_HOST = "10.20.20.10"
    MARIA_DB_PORT = 3306
    MARIA_DB_USER = "chatty_master"
    MARIA_DB_HOST = "110.45.147.58"
    DB_PORT = 5432
    
    # MySQL/MariaDB 설정 (chatty DB용) - 고정
    CHATTY_MYSQL_USER = "chatty_python"
    CHATTY_MYSQL_PASSWORD = "dktkekf0215@#"
    CHATTY_MYSQL_HOST = "10.20.20.10"
    CHATTY_MYSQL_PORT = 3306
    CHATTY_MYSQL_DBNAME = "chatty"
    
    # GWI MySQL 설정 (naraone DB용)
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
    DB_POOL_MIN = 10
    DB_POOL_MAX = 60
    DB_POOL_CHCK = 600
    LONG_TERM_COUNT = 30
    IMAGE_API_URL = "https://dev.chatbaram.com" # 이미지 생성 서버 주소
    GPU_SERVER_URL = os.getenv("GPU_SERVER_URL", "http://218.146.11.93:5700") # GPU 서버 주소
    CALLBACK_BASE_URL = os.getenv("PROD_CALLBACK_BASE_URL", os.getenv("DEV_CALLBACK_BASE_URL", "https://test.han.kr"))
    CALLBACK_URL = os.getenv("PROD_CALLBACK_URL", os.getenv("DEV_CALLBACK_URL", "https://test.han.kr/chat/doc_summary_homepage_callback.php"))
    
    # 🌐 웹사이트 번역 설정
    WEBSITE_TRANSLATION_CHUNK_SIZE = 5000        # HTML 청크 분할 크기
    WEBSITE_TRANSLATION_MAX_CONCURRENT = 200

    # 🚀 하드웨어 최적화 설정 (Xeon E5-2670 + 128GB RAM)
    HARDWARE_PHYSICAL_CORES = 16     # 물리 코어 수
    HARDWARE_LOGICAL_CORES = 32      # 논리 코어 수 (하이퍼스레딩)
    HARDWARE_TOTAL_RAM_GB = 256      # 총 RAM 용량 (GB)
    
    # 🌐 멀티유저 환경 리소스 관리 (20명 기준 설계 - 256GB RAM 활용)
    GLOBAL_MAX_CONCURRENT_JOBS = 20          # 동시 학습 작업 수: 20명
    GLOBAL_MAX_CONCURRENT_FILES = 60         # 전체 시스템 파일 동시 처리 제한 (20명 × 3개)
    GLOBAL_MAX_CONCURRENT_URLS = 80          # 전체 시스템 URL 동시 처리 제한 (20명 × 4개)
    GLOBAL_MAX_CONCURRENT_CHUNKS = 100       # 전체 시스템 청크 동시 처리 제한 (20명 × 5개)
    # 사용자별 리소스 할당 (20명 분산)
    USER_MAX_FILES_PER_JOB = 4              # 사용자별 최대 파일 동시 처리 (2→4 증가)
    USER_MAX_CHUNKS_PER_FILE = 8            # 파일당 최대 청크 배치 크기 (4→8 증가)
    # 멀티프로세싱 최적화 설정 (32스레드 활용)
    MAX_CONCURRENT_URL_CRAWLING = 80     # URL 크롤링 최대 동시 처리 (40→80)
    MAX_CONCURRENT_TEXT_PROCESSING = 24  # 텍스트 처리 최대 동시 처리 (10→24)
    MAX_CONCURRENT_FILE_PROCESSING = 32  # 파일 처리 최대 동시 처리 (16→32)
    # 배치 처리 최적화 설정 (256GB RAM 활용)
    TXT_BATCH_SIZE_SMALL = 6    # 작은 TXT 파일 배치 크기 (3→6)
    TXT_BATCH_SIZE_MEDIUM = 8   # 중간 TXT 파일 배치 크기 (4→8)
    TXT_BATCH_SIZE_LARGE = 10   # 큰 TXT 파일 배치 크기 (5→10)
    URL_BATCH_SIZE_SMALL = 4    # 작은 URL 페이지 배치 크기 (2→4)
    URL_BATCH_SIZE_MEDIUM = 6   # 중간 URL 페이지 배치 크기 (3→6)
    URL_BATCH_SIZE_LARGE = 8    # 큰 URL 페이지 배치 크기 (4→8)


    # 🚀 URL 배치 처리 최적화 설정 (대폭 개선)
    # URL 여러 개 처리 시 사용되는 고성능 배치 처리 설정
    
        # 배치 처리 임계값
    URL_BATCH_OPTIMIZATION_THRESHOLD = 1    # 3개 이상 URL일 때 배치 최적화 적용

    # 임베딩 배치 크기 (200→400으로 증가)
    URL_GLOBAL_EMBEDDING_BATCH_SIZE = 400   # 여러 URL 청크를 통합한 대형 임베딩 배치
    URL_SINGLE_EMBEDDING_BATCH_SIZE = 400   # 단일 URL 처리 시 임베딩 배치

    # DB 벌크 삽입 크기 (500→1000으로 증가)
    URL_GLOBAL_DB_BULK_SIZE = 1000          # 여러 URL 청크를 통합한 대형 DB 벌크 삽입
    URL_SINGLE_DB_BULK_SIZE = 1000          # 단일 URL 처리 시 DB 벌크 삽입

    # 동시 처리 수 (32스레드 활용)
    URL_GLOBAL_CONCURRENT_LIMIT = 100       # 여러 URL 배치 처리 시 동시 URL 수 (50→100)
    URL_SINGLE_CONCURRENT_LIMIT = 80        # 단일 URL 또는 소수 URL 처리 시 동시 수 (40→80)

    # HTTP 연결 최적화
    URL_HTTP_CONNECTION_POOL_SIZE = 200     # HTTP 연결 풀 크기 (100→200)
    URL_HTTP_CONNECTION_PER_HOST = 20       # 호스트당 최대 연결 수 (10→20)
    URL_HTTP_DNS_CACHE_TTL = 600            # DNS 캐시 TTL (초) - 연결 재사용 극대화
    
    # 품질 필터링 설정
    URL_CONTENT_QUALITY_THRESHOLD = 0.3     # 콘텐츠 품질 임계값 (0.3 이상만 저장)
    URL_MIN_CONTENT_LENGTH = 100            # 최소 콘텐츠 길이 (100자 이상)
    URL_MIN_CHUNK_COUNT = 2                 # 최소 청크 수 (2개 이상)
    
    # 재시도 및 타임아웃 설정
    URL_MAX_RETRIES = 3                     # HTTP 요청 최대 재시도 횟수
    URL_TIMEOUT_TOTAL = 45                  # HTTP 총 타임아웃 (초)
    URL_TIMEOUT_CONNECT = 8                 # HTTP 연결 타임아웃 (초)
    URL_TIMEOUT_READ = 25                   # HTTP 읽기 타임아웃 (초)
    
    # Playwright 설정 (동적 페이지 처리용) - 256GB RAM 활용
    PLAYWRIGHT_MAX_CONCURRENT = 16           # 동시 실행 Playwright 브라우저 수 (16→32, 32스레드 활용)
    PLAYWRIGHT_TIMEOUT = 60                  # Playwright 전체 타임아웃 (초)
    PLAYWRIGHT_PAGE_TIMEOUT = 50             # Playwright 페이지 로드 타임아웃 (초)
    PLAYWRIGHT_MAX_RETRIES = 3               # Playwright 최대 재시도 횟수
    PLAYWRIGHT_HEADLESS = True               # False면 GUI 필요(Worker/서버에서는 True 권장)

    # 로깅 및 모니터링
    URL_BATCH_LOGGING_LEVEL = "INFO"        # 배치 처리 로깅 레벨
    URL_ENABLE_PERFORMANCE_MONITORING = True # 성능 모니터링 활성화
    URL_PERFORMANCE_LOG_INTERVAL = 10       # 성능 로그 출력 간격 (개수)

