# config/settings.py
import os
from pathlib import Path
from typing import List, Optional
import sys
from urllib.parse import quote, unquote, urlparse
import json
import logging
import time

try:
    from backend.shared.config import Config as SharedDBConfig
except Exception:  # pragma: no cover - settings bootstrap fallback
    SharedDBConfig = None

from backend.shared.duplicate_category_only_mode import (
    apply_env_values,
    board_feature_preset_values,
    duplicate_repair_switch_values,
)

# Prevent creation of local .cursor/debug.log by default:
# If AGENT_DEBUG_LOG_PATH is not set, point it to the platform null device.
os.environ.setdefault("AGENT_DEBUG_LOG_PATH", os.devnull)

# .env 파일 로드 (여러 경로 시도)
try:
    from dotenv import load_dotenv

    def _safe_load_dotenv(path: Optional[Path] = None, *, override: bool = False) -> bool:
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
                    print(f"[SETTINGS] WARNING: .env loaded with fallback encoding={encoding}: {target}", flush=True)
                return loaded
            except UnicodeDecodeError as exc:
                last_decode_error = exc
                continue
            except Exception as exc:
                print(f"[SETTINGS] WARNING: .env load skipped: path={target} err={exc}", flush=True)
                return False
        print(f"[SETTINGS] WARNING: .env decode skipped: path={target} err={last_decode_error}", flush=True)
        return False
        # 여러 가능한 경로에서 .env 파일 찾기
    possible_paths = []
    
    # 1. 환경 변수로 지정된 경로 (최우선)
    env_file_path = os.getenv("ENV_FILE_PATH")
    if env_file_path:
        possible_paths.append(Path(env_file_path))
    
    # 2. 프로젝트 루트 (config/settings.py 기준)
    BASE_DIR = Path(__file__).parent.parent
    possible_paths.append(BASE_DIR / ".env")
    possible_paths.append(BASE_DIR / "backend" / ".env")

    # 3. 현재 작업 디렉토리
    possible_paths.append(Path.cwd() / ".env")
    
    # 4. 상위 디렉토리들도 시도 (최대 3단계)
    current = Path(__file__).parent
    for _ in range(3):
        current = current.parent
        possible_paths.append(current / ".env")
    
    # 5. 절대 경로 시도 (/home/chat_bot/Ai_Pro_filecrawler/.env)
    possible_paths.append(Path("/home/chat_bot/Ai_Pro_filecrawler/.env"))
    
    # 중복 제거 (순서 유지)
    seen = set()
    unique_paths = []
    for path in possible_paths:
        if path not in seen:
            seen.add(path)
            unique_paths.append(path)
    
    # 첫 번째로 찾은 .env 파일 로드
    env_loaded = False
    for env_path in unique_paths:
        if env_path.exists() and env_path.is_file():
            env_loaded = _safe_load_dotenv(env_path, override=True)
            if env_loaded:
                print(f"[SETTINGS] .env 파일 로드 완료: {env_path}", flush=True)
            else:
                print(f"[SETTINGS] WARNING: .env 파일 로드 생략: {env_path}", flush=True)
            break
    
    if not env_loaded:
        print("[SETTINGS] WARNING: .env 파일을 찾을 수 없습니다. 시도한 경로:", flush=True)
        for path in unique_paths[:5]:  # 처음 5개만 출력
            print(f"  - {path}", flush=True)
        print("[SETTINGS] TIP: 환경 변수 ENV_FILE_PATH로 .env 파일 경로를 지정할 수 있습니다.", flush=True)
        
except ImportError:
    load_dotenv = None
    print("[SETTINGS] WARNING: python-dotenv가 설치되지 않았습니다. .env 파일을 로드할 수 없습니다.", flush=True)
except Exception as e:
    load_dotenv = None
    print(f"[SETTINGS] WARNING: .env 파일 로드 중 오류 발생: {e}", flush=True)

def _apply_board_feature_preset() -> None:
    """
    Apply grouped board feature switches after .env loading.

    BOARD_FEATURE_PRESET values:
    - pure_crawling / crawl_only: duplicate repair off,
      automatic classification application on.
    - auto_classification_only / auto_category_only: crawl/exploration repair off,
      duplicate category repair and automatic classification on.
    - duplicate_repair_on: legacy broad duplicate repair on.
    - manual / custom / off / none: leave individual variables unchanged.
    """
    preset = str(os.getenv("BOARD_FEATURE_PRESET", "") or "").strip().lower()
    if not preset or preset in {"manual", "custom", "off", "none"}:
        return

    values = board_feature_preset_values(preset)
    if values is None:
        print(f"[SETTINGS] WARNING: unknown BOARD_FEATURE_PRESET={preset!r}; individual flags unchanged.", flush=True)
        return
    apply_env_values(values)
    print(f"[SETTINGS] BOARD_FEATURE_PRESET applied: {preset}", flush=True)


_apply_board_feature_preset()


def _apply_board_duplicate_repair_switch() -> None:
    """
    Apply the simple duplicate repair switch after broader board presets.

    BOARD_DUPLICATE_REPAIR=on enables duplicate repair metadata backfill.
    BOARD_DUPLICATE_REPAIR=category enables category-only duplicate repair.
    BOARD_DUPLICATE_REPAIR=off disables duplicate repair while leaving automatic
    classification flags untouched.
    """
    value = str(os.getenv("BOARD_DUPLICATE_REPAIR", "") or "").strip().lower()
    if not value:
        return
    applied, values = duplicate_repair_switch_values(value)
    if values is None or applied is None:
        print(f"[SETTINGS] WARNING: unknown BOARD_DUPLICATE_REPAIR={value!r}; duplicate repair flags unchanged.", flush=True)
        return
    apply_env_values(values)
    print(f"[SETTINGS] BOARD_DUPLICATE_REPAIR applied: {applied}", flush=True)


_apply_board_duplicate_repair_switch()


def _shared_db_pool_max_default() -> int:
    try:
        value = int(getattr(SharedDBConfig, "DB_POOL_MAX", 10) or 10)
    except Exception:
        value = 10
    return max(4, min(value, 64))


def _default_scan_workers(pool_max: int) -> int:
    return 4 if pool_max >= 8 else 2


def _default_collection_workers(pool_max: int) -> int:
    return 4 if pool_max >= 8 else 2


def _default_post_attach_workers(pool_max: int) -> int:
    return max(8, min(12, max(4, pool_max - 4) * 2))


def _default_study_workers(pool_max: int) -> int:
    return max(6, min(10, max(3, pool_max // 3) * 2))


def _default_download_max_concurrent(pool_max: int) -> int:
    return max(4, min(8, max(2, pool_max // 2) * 2))


def _default_file_pipeline_concurrency(pool_max: int) -> int:
    return 2


def _default_file_pipeline_collection_batch_size(pool_max: int) -> int:
    return 3


def _default_file_embedding_batch_size(pool_max: int) -> int:
    return max(4, min(8, pool_max))


def _default_file_table_embedding_batch_size(pool_max: int) -> int:
    return max(2, min(4, pool_max // 2))


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 128) -> int:
    try:
        value = int(os.getenv(name, str(default)) or default)
    except Exception:
        value = default
    return max(min_value, min(value, max_value))


def _feature_workers(
    name: str,
    legacy_name: str,
    default: int,
    *,
    min_value: int = 1,
    max_value: int = 128,
) -> int:
    try:
        value = int(default)
    except Exception:
        value = default
    return max(min_value, min(value, max_value))


def _derived_workers(
    feature_name: str,
    legacy_name: str,
    feature_value: int,
    *,
    min_value: int = 1,
    max_value: int = 128,
) -> int:
    try:
        value = int(feature_value)
    except Exception:
        value = min_value
    return max(min_value, min(value, max_value))


class Settings:
    """
    전역 설정 관리 클래스
    환경 변수를 통해 설정을 오버라이드할 수 있습니다.
    """
    
    # 프로젝트 루트 경로
    BASE_DIR: Path = Path(__file__).parent.parent
    
    # 다운로드 설정
    DOWNLOAD_DIR: str = os.getenv("DOWNLOAD_DIR", "downloads")
    DOWNLOAD_PATH: Path = BASE_DIR / DOWNLOAD_DIR
    
    # 서버 설정
    HOST: str = os.getenv("HOST", "127.0.0.1")
    PORT: int = int(os.getenv("PORT", "8000"))
    RELOAD: bool = os.getenv("RELOAD", "False").lower() == "true"
    
    # 크롤링 설정
    # 기본 탐색 깊이.
    # - 운영 안정성을 위해 기본값은 2로 제한한다.
    # - 별도 요청/기능에서 더 깊게 필요하면, 코드에서 명시적으로 예외 처리(또는 상한 변경)할 것.
    MAX_DEPTH: int = int(os.getenv("MAX_DEPTH", "2"))
    MAX_PAGES_PER_BOARD: int = int(os.getenv("MAX_PAGES_PER_BOARD", "0"))
    
    # 워커 설정
    # - 탐색/선별은 상대적으로 가볍고 빠르게 증가(카운트가 빨리 오름)하는 경향이 있다.
    # - 저장/학습(다운로드/DB/벡터화)은 무겁고 병목이 생기기 쉬워, 기본값을 "소폭" 보정해 둔다.
    # (환경변수로 언제든 조절 가능)
    #
    # 기본 밸런스(요청 반영):
    # - 탐색/선별: -1씩 (4 → 3)
    # - 저장/학습: +1씩 (download 5 → 6, study 4 → 5)
    _DB_POOL_MAX_FOR_WORKERS: int = _shared_db_pool_max_default()
    DISCOVERY_WORKERS: int = _feature_workers(
        "DISCOVERY_WORKERS",
        "SCAN_WORKERS",
        _default_scan_workers(_DB_POOL_MAX_FOR_WORKERS),
    )
    SELECTION_WORKERS: int = _feature_workers(
        "SELECTION_WORKERS",
        "COLLECTION_WORKERS",
        _default_collection_workers(_DB_POOL_MAX_FOR_WORKERS),
    )
    SAVE_WORKERS: int = _feature_workers(
        "SAVE_WORKERS",
        "POST_WORKERS",
        _default_post_attach_workers(_DB_POOL_MAX_FOR_WORKERS),
    )
    LEARNING_WORKERS: int = _feature_workers(
        "LEARNING_WORKERS",
        "STUDY_WORKERS",
        _default_study_workers(_DB_POOL_MAX_FOR_WORKERS),
    )
    SCAN_WORKERS: int = _derived_workers("DISCOVERY_WORKERS", "SCAN_WORKERS", DISCOVERY_WORKERS)
    COLLECTION_WORKERS: int = _derived_workers("SELECTION_WORKERS", "COLLECTION_WORKERS", SELECTION_WORKERS)
    POST_WORKERS: int = _derived_workers("SAVE_WORKERS", "POST_WORKERS", SAVE_WORKERS)
    ATTACH_WORKERS: int = _derived_workers("SAVE_WORKERS", "ATTACH_WORKERS", SAVE_WORKERS)
    # 저장(download)과 학습(study) 처리량 밸런스:
    # - 저장이 학습보다 너무 크면 save_count만 빠르게 증가하고 study가 밀리는 체감이 발생한다.
    # - 기본값은 "저장이 학습보다 살짝 높게" (download = study + 1)로 둔다.
    # save_batch_queue는 기본적으로 1건씩 학습 워커에 전달되므로,
    # 학습 병목 완화의 직접적인 기본 튜닝 포인트는 STUDY_WORKERS다.
    DOWNLOAD_WORKERS: int = _env_int("DOWNLOAD_WORKERS", _env_int("FILE_CRAWL_DOWNLOAD_WORKERS", 2, max_value=16), max_value=16)
    STUDY_WORKERS: int = _derived_workers("LEARNING_WORKERS", "STUDY_WORKERS", LEARNING_WORKERS)
    DOWNLOAD_MAX_CONCURRENT: int = _env_int("DOWNLOAD_MAX_CONCURRENT", _env_int("FILE_CRAWL_DOWNLOAD_MAX_CONCURRENT", 2, max_value=16), max_value=16)
    FILE_CRAWL_PIPELINE_CONCURRENCY: int = _env_int(
        "FILE_CRAWL_PIPELINE_CONCURRENCY",
        _default_file_pipeline_concurrency(_DB_POOL_MAX_FOR_WORKERS),
        max_value=64,
    )
    FILE_CRAWL_LEARN_CONCURRENCY: int = _env_int(
        "FILE_CRAWL_LEARN_CONCURRENCY",
        min(LEARNING_WORKERS, 2),
        max_value=32,
    )
    BOARD_CONTENT_DISCOVER_CONCURRENCY: int = _derived_workers(
        "DISCOVERY_WORKERS",
        "BOARD_CONTENT_DISCOVER_CONCURRENCY",
        DISCOVERY_WORKERS,
    )
    BOARD_CONTENT_LIST_PAGE_CONCURRENCY: int = _derived_workers(
        "DISCOVERY_WORKERS",
        "BOARD_CONTENT_LIST_PAGE_CONCURRENCY",
        DISCOVERY_WORKERS,
    )
    BOARD_CONTENT_DETAIL_CONCURRENCY: int = 3
    BOARD_CONTENT_PIPELINE_DETAIL_CONCURRENCY: int = 3
    BOARD_SELECTOR_LEARNING_CONCURRENCY: int = _derived_workers("SELECTION_WORKERS", "BOARD_SELECTOR_LEARNING_CONCURRENCY", SELECTION_WORKERS)
    BOARD_CONTENT_LEARN_CONCURRENCY: int = 3
    BOARD_LEARN_LIST_SAVE_WORKERS: int = 3
    BOARD_LEARN_LIST_SAVE_DB_CONCURRENCY: int = 3
    FILE_PIPELINE_COLLECTION_BATCH_SIZE: int = int(
        os.getenv(
            "FILE_PIPELINE_COLLECTION_BATCH_SIZE",
            str(_default_file_pipeline_collection_batch_size(_DB_POOL_MAX_FOR_WORKERS)),
        )
    )
    FILE_EMBEDDING_BATCH_SIZE: int = int(
        os.getenv(
            "FILE_EMBEDDING_BATCH_SIZE",
            str(_default_file_embedding_batch_size(_DB_POOL_MAX_FOR_WORKERS)),
        )
    )
    FILE_TABLE_EMBEDDING_BATCH_SIZE: int = int(
        os.getenv(
            "FILE_TABLE_EMBEDDING_BATCH_SIZE",
            str(_default_file_table_embedding_batch_size(_DB_POOL_MAX_FOR_WORKERS)),
        )
    )
    
    # Playwright 설정
    HEADLESS: bool = os.getenv("HEADLESS", "True").lower() == "true"
    BROWSER_TIMEOUT: int = int(os.getenv("BROWSER_TIMEOUT", "20000"))
    # Playwright 동시성 (기본값: 4)
    PLAYWRIGHT_MAX_CONCURRENT: int = 2
    PLAYWRIGHT_TIMEOUT: int = int(os.getenv("PLAYWRIGHT_TIMEOUT", "60"))
    PLAYWRIGHT_PAGE_TIMEOUT: int = 10
    PLAYWRIGHT_MAX_RETRIES: int = int(os.getenv("PLAYWRIGHT_MAX_RETRIES", "3"))
    PLAYWRIGHT_HEADLESS: bool = os.getenv("PLAYWRIGHT_HEADLESS", str(HEADLESS)).lower() == "true"
    
    # OpenAI 설정
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY", "")
    # 추가 OpenAI 키 (백엔드/shared/config.py와 호환되도록)
    OPENAI_ASADAL_API_KEY: Optional[str] = os.getenv("OPENAI_ASADAL_API_KEY", "")
    OPENAI_SECOND_API_KEY: Optional[str] = os.getenv("OPENAI_SECOND_API_KEY", "")
    USE_BATCH_EMBEDDING_SCHEDULER: bool = os.getenv("USE_BATCH_EMBEDDING_SCHEDULER", "False").lower() == "true"
    USE_BOARD_BATCH_EMBEDDING_SCHEDULER: bool = os.getenv(
        "USE_BOARD_BATCH_EMBEDDING_SCHEDULER",
        str(USE_BATCH_EMBEDDING_SCHEDULER),
    ).lower() == "true"
    USE_FILE_BATCH_EMBEDDING_SCHEDULER: bool = os.getenv(
        "USE_FILE_BATCH_EMBEDDING_SCHEDULER",
        str(USE_BATCH_EMBEDDING_SCHEDULER),
    ).lower() == "true"
    BATCH_SCHEDULER_BASE_URL: str = os.getenv("BATCH_SCHEDULER_BASE_URL", "").rstrip("/")
    BATCH_SCHEDULER_API_TOKEN: Optional[str] = os.getenv("BATCH_SCHEDULER_API_TOKEN", "")
    BATCH_EMBEDDING_SERVICE_NAME: str = os.getenv(
        "BATCH_EMBEDDING_SERVICE_NAME",
        "ai_pro_filecrawler_embedding",
    )
    BATCH_BOARD_EMBEDDING_SERVICE_NAME: str = os.getenv(
        "BATCH_BOARD_EMBEDDING_SERVICE_NAME",
        os.getenv("BATCH_EMBEDDING_SERVICE_NAME", "ai_pro_filecrawler_embedding"),
    )
    BATCH_FILE_EMBEDDING_SERVICE_NAME: str = os.getenv(
        "BATCH_FILE_EMBEDDING_SERVICE_NAME",
        "ai_pro_filecrawler_embedding",
    )
    BATCH_EMBEDDING_FLOW_DEBUG: bool = os.getenv(
        "BATCH_EMBEDDING_FLOW_DEBUG",
        "False",
    ).lower() == "true"
    BATCH_SCHEDULER_SUBMIT_RETRY_ATTEMPTS: int = int(os.getenv("BATCH_SCHEDULER_SUBMIT_RETRY_ATTEMPTS", "3"))
    BATCH_SCHEDULER_SUBMIT_RETRY_DELAY_SEC: float = float(os.getenv("BATCH_SCHEDULER_SUBMIT_RETRY_DELAY_SEC", "1.0"))
    BATCH_CALLBACK_TOKEN: Optional[str] = os.getenv("BATCH_CALLBACK_TOKEN", "")
    BATCH_CALLBACK_TTL_SEC: int = int(os.getenv("BATCH_CALLBACK_TTL_SEC", str(7 * 24 * 3600)))

    
    # DB 설정 (향후 확장용)
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "3306"))
    DB_NAME: str = os.getenv("DB_NAME", "crawler_db")
    DB_USER: str = os.getenv("DB_USER", "root")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # 외부 서비스 URL (.156 서버인 test.han.kr로 기본 설정)
    _LEARN_FILE_BASE_HOST: str = os.getenv("LEARN_FILE_ADD_HOST", "http://110.45.147.63")
    LEARN_FILE_ADD_URL: str = os.getenv(
        "LEARN_FILE_ADD_URL",
        f"{_LEARN_FILE_BASE_HOST.rstrip('/')}/Ai_Pro_filecrawler/services/learn_file_add.php",
    )
    
    # 추가: downloads/config.py에 있던 설정들을 통합
    WEB_DB: str = os.getenv("WEB_DB", "web_trans")
    CHATTY_PG_DB_HOST: str = os.getenv("CHATTY_PG_DB_HOST", "10.20.20.22")
    MILVUS_INSERT_API_BASE_URL: str = os.getenv("MILVUS_INSERT_API_BASE_URL", "10.20.20.22")
    
    # 서버 구분 설정
    # 크롤링 서버: 110.45.147.56
    # 학습 서버: 110.45.146.156 (test.han.kr)
    # 크롤링 서버에서는 로컬 임시 저장 후 학습 서버로 전송
    IS_CRAWLING_SERVER: bool = os.getenv("IS_CRAWLING_SERVER", "false").lower() == "true"
    
    # 기본 챗봇 ID (요청에서 누락될 경우 대비)
    DEFAULT_CHAT_BOT_ID: Optional[str] = os.getenv("DEFAULT_CHAT_BOT_ID")

    # CORS 설정
    # 환경 변수 CORS_ORIGINS가 있으면 사용, 없으면 기본값 사용
    # 형식: 쉼표로 구분된 origin 목록 (예: "https://dev.han.kr,https://test.han.kr,http://localhost:8000")
    _CORS_ORIGINS_ENV: Optional[str] = os.getenv("CORS_ORIGINS")
    _CORS_ORIGINS_DEFAULT: List[str] = [
        "https://dev.han.kr",
        "https://test.han.kr",
        "https://gwangjin.han.kr",
        "http://gwangjin.go.kr",
        "http://www.gwangjin.go.kr",
        "https://gwangjin.go.kr",
        "https://www.gwangjin.go.kr",
        "https://api-aipro.chatbaram.com",
        "https://dev.chatbaram.com",
        "https://admin.chatty.kr",
        # 로컬 개발/테스트용
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "null",
        # file:// 로 열어 테스트하는 경우 Origin이 "null"로 전송됨
        "null",
    ]
    _CORS_ORIGINS_ENV_LIST: List[str] = (
        [origin.strip() for origin in _CORS_ORIGINS_ENV.split(",") if origin.strip()]
        if _CORS_ORIGINS_ENV
        else []
    )
    # env가 있더라도 기본값을 완전히 덮어쓰지 않고, 누락된 필수 origin(gwangjin 등)은 항상 포함한다.
    CORS_ORIGINS: List[str] = list(dict.fromkeys(_CORS_ORIGINS_ENV_LIST + _CORS_ORIGINS_DEFAULT))

    _CORS_ORIGIN_REGEX_ENV: Optional[str] = os.getenv("CORS_ORIGIN_REGEX")
    # 서브도메인에 밑줄이 있는 호스트(예: songpa_health.han.kr)도 허용
    _CORS_ORIGIN_REGEX_DEFAULT: List[str] = [
        r"^https?://([a-zA-Z0-9_-]+\.)?han\.kr$",
        r"^https?://([a-zA-Z0-9_-]+\.)?gwangjin\.go\.kr$",
        r"^https?://([a-zA-Z0-9_-]+\.)?chatbaram\.com$",
        r"^https?://([a-zA-Z0-9_-]+\.)?chatty\.kr$",
    ]
    _CORS_ORIGIN_REGEX_ENV_LIST: List[str] = (
        [pattern.strip() for pattern in _CORS_ORIGIN_REGEX_ENV.split(",") if pattern.strip()]
        if _CORS_ORIGIN_REGEX_ENV
        else []
    )
    CORS_ORIGIN_REGEX: List[str] = list(
        dict.fromkeys(_CORS_ORIGIN_REGEX_ENV_LIST + _CORS_ORIGIN_REGEX_DEFAULT)
    )

    # CORS origin regex 리스트를 다루기 위한 문자열 OR 패턴
    # Starlette/FASTAPI는 allow_origin_regex에 단일 문자열만 허용하므로,
    # 여러 패턴이 구성되어 있다면 '|' 구분자로 이어붙여 전달합니다.
    # 클래스 변수 초기화 순서 문제를 피하기 위해 __init__에서 설정
    CORS_ORIGIN_REGEX_PATTERN: Optional[str] = None
    
    # 임베딩/DB 배치 크기 기본값 (URL 처리용)
    URL_SINGLE_EMBEDDING_BATCH_SIZE: int = int(os.getenv("URL_SINGLE_EMBEDDING_BATCH_SIZE", "50"))
    URL_SINGLE_DB_BULK_SIZE: int = int(os.getenv("URL_SINGLE_DB_BULK_SIZE", "500"))
    # 글로벌 임베딩 배치 크기 (예: 크롤링 전체에서 사용하는 전역 배치)
    URL_GLOBAL_EMBEDDING_BATCH_SIZE: int = int(os.getenv("URL_GLOBAL_EMBEDDING_BATCH_SIZE", "100"))
    # 글로벌 동시 실행 제한 (크롤링 전체에서 동시에 처리할 최대 URL 수)
    URL_GLOBAL_CONCURRENT_LIMIT: int = int(os.getenv("URL_GLOBAL_CONCURRENT_LIMIT", "12"))
    # 텍스트 청크 분할 기본값 (edu 모듈과의 호환성 유지)
    BASIC_CHUNK_SIZE: int = int(os.getenv("BASIC_CHUNK_SIZE", "1000"))
    BASIC_CHUNK_OVERLAP: int = int(os.getenv("BASIC_CHUNK_OVERLAP", "50"))
    
    def __init__(self):
        # 다운로드 디렉토리 생성 (기본 경로)
        # 임포트 시 파일시스템 문제로 모듈 초기화 실패를 방지하기 위해 예외를 방어적으로 처리합니다.
        try:
            self.DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            # 로그 시스템이 아직 초기화되지 않았을 수 있으므로 print로 경고 출력
            print(f"[SETTINGS] WARNING: DOWNLOAD_PATH 생성 실패: {self.DOWNLOAD_PATH} - {e}", flush=True)
        
        # CORS_ORIGIN_REGEX_PATTERN 초기화
        patterns = [
            pattern.strip()
            for pattern in (self.CORS_ORIGIN_REGEX or [])
            if pattern and pattern.strip()
        ]
        self.CORS_ORIGIN_REGEX_PATTERN = "|".join(patterns) if patterns else None
    
    def _extract_domain_folder_name(self, domain: str) -> str:
        """
        도메인에서 폴더명으로 사용할 부분만 추출합니다.
        
        예:
            gwangjin.go.kr -> gwangjin
            www.example.com -> example
            example.com -> example
            subdomain.example.co.kr -> example
        
        Args:
            domain: 전체 도메인명
        
        Returns:
            폴더명으로 사용할 도메인 부분
        """
        if not domain or domain == "unknown":
            return "unknown"
        
        # 포트 번호 제거
        domain = domain.split(':')[0].lower()
        
        # www. 제거
        if domain.startswith("www."):
            domain = domain[4:]
            
        parts = domain.split('.')
        if not parts:
            return "unknown"
        
        # 첫 번째 도메인 조각 추출 (예: gwangjin.go.kr -> gwangjin, test.han.kr -> test)
        folder_name = parts[0]
        
        return folder_name.lower()
    
    def get_web_url_prefix(self, chat_bot_id: str, domain: Optional[str] = None) -> str:
        """
        웹에서 접근 가능한 파일 경로 접두사를 생성합니다 (PHP 백엔드와 호환).
        예: https://test.han.kr/chat/uploaded_files/{UUID마지막12}/
        """
        uuid_fragment = chat_bot_id.split("-")[-1] if chat_bot_id and "-" in chat_bot_id else "unknown"
        uuid_tail12 = uuid_fragment[-12:] if len(uuid_fragment) >= 12 else uuid_fragment
        
        # 도메인 및 경로 추출
        from urllib.parse import urlparse
        target_url = self.LEARN_FILE_ADD_URL or self._LEARN_FILE_BASE_HOST
        parsed = urlparse(target_url)
        
        scheme = parsed.scheme or "https"
        # 환경 변수에서 추출된 호스트 도메인 사용 (하드코딩 제거)
        host_domain = parsed.netloc or "test.han.kr"
        
        # [복구] yong님 검토 사항: /chat 경로 다시 포함
        web_dir = os.path.dirname(parsed.path)
        if not web_dir or web_dir == '/' or 'chat' not in web_dir.lower():
            web_dir = '/chat'

        # domain_folder = self._extract_domain_folder_name(domain or "unknown")
        # 최종 URL: .../chat/uploaded_files/UUID/ (도메인 폴더 제외)
        web_prefix = f"{scheme}://{host_domain}{web_dir.rstrip('/')}/uploaded_files/{uuid_tail12}"
        
        return f"{web_prefix}/"

    def get_web_accessible_url(self, chat_bot_id: str, filename: str, domain: Optional[str] = None) -> str:
        """
        파일의 웹 접근 URL을 생성합니다.
        """
        prefix = self.get_web_url_prefix(chat_bot_id, domain)
        from urllib.parse import quote
        # 파일명 인코딩 (브라우저 접근용)
        return f"{prefix}{quote(filename)}"

    def get_download_path(self, chat_bot_id: str, domain: Optional[str] = None) -> Path:
        """
        다운로드 경로를 동적으로 생성합니다.
        
        크롤링 서버(110.45.147.56)인 경우:
            - 로컬 임시 디렉토리에 저장 (DOWNLOAD_PATH/{uuid_fragment})
            - 이후 학습 서버로 HTTP POST로 전송
        
        학습 서버(110.45.146.156)인 경우:
            - 직접 학습 서버 경로에 저장 (/home/test.han.kr/www/chat/uploaded_files/{uuid_tail12})
        
        Args:
            chat_bot_id: 챗봇 ID (예: cb-123-176edb6ceee1)
            domain: 수집된 사이트 도메인 (폴더 구분용)
        
        Returns:
            생성된 경로 Path 객체
        """
        # UUID 조각 추출 (마지막 부분) -> 마지막 12자리로 축약
        uuid_fragment = chat_bot_id.split("-")[-1] if chat_bot_id and "-" in chat_bot_id else "unknown"
        uuid_tail12 = uuid_fragment[-12:] if len(uuid_fragment) >= 12 else uuid_fragment
        
        # 크롤링 서버인 경우 로컬 임시 경로 사용
        if self.IS_CRAWLING_SERVER:
            # 크롤링 서버에서는 로컬 임시 디렉토리 사용
            full_path = self.DOWNLOAD_PATH / uuid_tail12
            
            # 경로 생성
            try:
                full_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"[SETTINGS] ERROR: 경로 생성 실패: {full_path}, 오류: {e}", flush=True)
            
            print(f"================================*download_path*================================", flush=True)
            print(f"[크롤링 서버 모드] 로컬 임시 저장 경로: {full_path}")
            print(f"다운로드 후 학습 서버({self._LEARN_FILE_BASE_HOST})로 HTTP POST 전송 예정")
            print(f"=================================================================================", flush=True)
            
            return full_path
        
        # .156 서버 경로 구조로 사용하되 도메인은 동적으로 처리
        target_url = self.LEARN_FILE_ADD_URL or self._LEARN_FILE_BASE_HOST
        parsed = urlparse(target_url)
        host_domain = parsed.netloc or "test.han.kr"
        
        # 최종 경로: /home/DOMAIN/www/chat/uploaded_files/UUID/
        full_path = Path("/home") / host_domain / "www" / "chat" / "uploaded_files" / uuid_tail12
        
        # 경로 생성
        try:
            full_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"[SETTINGS] ERROR: 경로 생성 실패: {full_path}, 오류: {e}", flush=True)
        
        print(f"================================*download_path*================================", flush=True)
        print(f"[학습 서버 모드] 최종 다운로드 물리 경로: {full_path}")
        print(f"매핑된 호스트 도메인: {host_domain}")
        print(f"=================================================================================", flush=True)

        return full_path
    
    @property
    def worker_config(self) -> dict:
        """워커 설정을 딕셔너리로 반환"""
        return {
            "discovery_workers": self.DISCOVERY_WORKERS,
            "selection_workers": self.SELECTION_WORKERS,
            "save_workers": self.SAVE_WORKERS,
            "learning_workers": self.LEARNING_WORKERS,
            "scan_workers": self.SCAN_WORKERS,
            "collection_workers": self.COLLECTION_WORKERS,
            "post_workers": self.POST_WORKERS,
            "attach_workers": self.ATTACH_WORKERS,
            "download_workers": self.DOWNLOAD_WORKERS,
            "study_workers": self.STUDY_WORKERS,
            "download_max_concurrent": self.DOWNLOAD_MAX_CONCURRENT,
            "file_crawl_pipeline_concurrency": self.FILE_CRAWL_PIPELINE_CONCURRENCY,
            "file_crawl_learn_concurrency": self.FILE_CRAWL_LEARN_CONCURRENCY,
            "file_pipeline_collection_batch_size": self.FILE_PIPELINE_COLLECTION_BATCH_SIZE,
            "board_content_discover_concurrency": self.BOARD_CONTENT_DISCOVER_CONCURRENCY,
            "board_content_list_page_concurrency": self.BOARD_CONTENT_LIST_PAGE_CONCURRENCY,
            "board_content_detail_concurrency": self.BOARD_CONTENT_DETAIL_CONCURRENCY,
            "board_content_pipeline_detail_concurrency": self.BOARD_CONTENT_PIPELINE_DETAIL_CONCURRENCY,
            "board_selector_learning_concurrency": self.BOARD_SELECTOR_LEARNING_CONCURRENCY,
            "board_content_learn_concurrency": self.BOARD_CONTENT_LEARN_CONCURRENCY,
            "board_learn_list_save_workers": self.BOARD_LEARN_LIST_SAVE_WORKERS,
            "board_learn_list_save_db_concurrency": self.BOARD_LEARN_LIST_SAVE_DB_CONCURRENCY,
        }
    
    @property
    def crawl_settings(self) -> dict:
        """크롤링 설정을 딕셔너리로 반환"""
        from .constants import ALLOWED_EXTENSIONS
        return {
            "max_depth": self.MAX_DEPTH,
            "max_pages_per_board": self.MAX_PAGES_PER_BOARD,
            "allowed_extensions": ALLOWED_EXTENSIONS
        }

# 싱글톤 인스턴스
settings = Settings()
# Backwards-compatible alias: some code imports `Config` from config.settings
# Provide `Config` name pointing to the singleton `settings` instance.
Config = settings

# 환경변수 기반 on/off 플래그 유틸리티
def _env_flag(name: str, default: str = "1") -> bool:
    """
    환경변수 기반 on/off 플래그.
    - truthy: 1, true, yes, on
    - falsy: 0, false, no, off, (empty)
    """
    try:
        raw = os.getenv(name)
    except Exception:
        raw = None
    val = (raw if raw is not None else default)
    return str(val).strip().lower() in ("1", "true", "yes", "on")

# =====================================================
# JSON 출력 제어 (파일 생성) 1 or 0
# =====================================================
ENABLE_JSON_OUTPUTS = _env_flag("ENABLE_JSON_OUTPUTS", "0")
BOARD_CONTENT_JSON_OUTPUT = _env_flag("BOARD_CONTENT_JSON_OUTPUT", "0")
MENU_KEYWORDS_JSON_OUTPUT = _env_flag("MENU_KEYWORDS_JSON_OUTPUT", "0")

# finalize 단계 idle 대기 시간 (초) - 기본 0 (미설정 시)
BOARD_CONTENT_FINALIZE_IDLE_WAIT_SEC = int(os.getenv("BOARD_CONTENT_FINALIZE_IDLE_WAIT_SEC", "0") or "0")
# SSE 완료 안정화 대기 시간 (초) — Config 부재 시 기본 300
BOARD_CONTENT_SSE_STABLE_SEC = float(os.getenv("BOARD_CONTENT_SSE_STABLE_SEC", "300") or "300")
# Redis job 메타 TTL (초) — Config 부재 시 24시간
REDIS_JOB_META_TTL_SEC = int(os.getenv("REDIS_JOB_META_TTL_SEC", str(24 * 3600)) or str(24 * 3600))


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # Debug trace is opt-in to avoid noise in production.
    try:
        enabled = str(os.getenv("CONFIG_DEBUG_TRACE", "0") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        enabled = False
    if not enabled:
        return
    try:
        log_path = os.getenv(
            "AGENT_DEBUG_LOG_PATH",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".cursor", "debug.log")),
        )
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
        except Exception:
            pass
        payload = {
            "sessionId": "debug-session",
            "runId": os.getenv("AGENT_DEBUG_RUN_ID", "run1"),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(log_path, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _load_dotenv_auto() -> Optional[str]:
    if load_dotenv is None:
        return None

    candidates = []
    env_file = os.getenv("ENV_FILE_PATH")
    if env_file:
        candidates.append(Path(env_file))

    base_dir = Path(__file__).resolve().parent.parent
    candidates.extend(
        [
            base_dir / ".env",
            Path(__file__).resolve().parent / ".env",
            Path.cwd() / ".env",
        ]
    )


    for path in candidates:
        try:
            if path and path.exists():
                _safe_load_dotenv(path, override=False)
                return str(path)
        except Exception:
            continue

    _safe_load_dotenv(None, override=False)
    return None


_load_dotenv_auto()

# =====================================================
# Application Environment (단일 정의)
# =====================================================
APP_ENV = os.getenv("APP_ENV", "prod").lower()
APP_LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()

def _configure_global_logging():
    level = getattr(logging, APP_LOG_LEVEL, logging.INFO)
    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(level)
    else:
        logging.basicConfig(level=level)
    logger = logging.getLogger("backend.shared.config")
    logger.setLevel(level)
    logger.debug(f"[Config] Global logging level set to {logging.getLevelName(level)}")

    # pdfminer.six (pdfplumber/camelot 내부 의존성)에서 깨진 PDF로 인해 WARNING 로그가 과도하게 발생할 수 있어
    # 운영 로그 노이즈를 줄이기 위해 pdfminer 로거는 ERROR 이상만 출력한다.
    # 예: "Cannot set gray non-stroke color because ... is an invalid float value"
    try:
        logging.getLogger("pdfminer").setLevel(logging.ERROR)
        logging.getLogger("pdfminer.pdfinterp").setLevel(logging.ERROR)
    except Exception:
        pass

    try:
        from backend.shared.log_compact_filter import install_board_shared_core_log_filter

        install_board_shared_core_log_filter()
    except Exception:
        pass

    return logger

logger = _configure_global_logging()


# =====================================================
# 전달 경로 (FileUpload) 일원화
# - 모든 /FileUpload/... 경로·루트·URL은 이 블록과 아래 함수들로만 정의/사용한다.
# - 경로 이슈 확인 문서: backend/docs/FILE_STORAGE_FLOW.md
# =====================================================
FILEUPLOAD_URL_PREFIX = "/FileUpload"
DEFAULT_FILEUPLOAD_ROOT = "/FileUpload"


def get_fileupload_root() -> str:
    """
    FileUpload 로컬 최상위 디렉터리.
    - ENV: FILEUPLOAD_ROOT (예: C:/FileUpload)
    - 미설정: 절대경로 /FileUpload (Windows에서는 드라이브 루트)
    """
    return os.getenv("FILEUPLOAD_ROOT") or os.path.abspath(DEFAULT_FILEUPLOAD_ROOT)


def fileupload_web_path_to_absolute(web_path: str) -> str:
    """
    웹 경로(/FileUpload/domain/uuid 등)를 로컬 절대 경로로 변환.
    - web_path: /FileUpload/... 또는 domain/uuid 형태
    """
    raw = (web_path or "").replace("\\", "/").strip()
    prefix = FILEUPLOAD_URL_PREFIX
    if raw.startswith(prefix):
        raw = raw[len(prefix) :].lstrip("/")
    return os.path.join(get_fileupload_root(), *raw.split("/")) if raw else get_fileupload_root()


def get_postgres_host(domain: Optional[str] = None) -> str:
    """
    PostgreSQL 호스트를 반환.

    정책: 서버 환경/도메인 분기 없이 고정 호스트를 사용한다.
    """
    return "10.20.20.12"


def is_production_server(hostname: Optional[str] = None) -> bool:
    """
    운영 서버 여부 판단 (도메인 프리픽스 기반)
    
    - dev.* → False (개발 서버, dev_user DB)
    - test.* → False (테스트 서버, testchatbot1 DB)
    - 그 외 도메인 → True (운영 서버, 도메인 프리픽스를 DB명으로 사용)
    
    Args:
        hostname: 호스트명 (예: dev.han.kr, test.han.kr, aniestkh.han.kr)
    
    Returns:
        운영 서버이면 True, 아니면 False
    """
    if hostname:
        hostname = hostname.lower().strip()
        # 포트 번호 제거 (예: dev.han.kr:8080 -> dev.han.kr)
        if ":" in hostname:
            hostname = hostname.split(":")[0]
        
        # 도메인 프리픽스 추출 (예: dev.han.kr -> dev)
        domain_parts = hostname.split(".")
        if len(domain_parts) > 0:
            prefix = domain_parts[0]
            # dev, test는 개발/테스트 서버
            if prefix in ("dev", "test"):
                return False
        
        # 그 외는 운영 서버
        return True
    else:
        # hostname이 없으면 APP_ENV로 판단
        return APP_ENV not in ("dev", "test", "development")

def get_postgres_db_name(db_name: str) -> str:
    """
    PostgreSQL DB_NAME 반환

    정책: 클라이언트가 전달한 db_name만 사용한다.
    - POSTGRES_DB_NAME(환경변수)로의 fallback은 사용하지 않는다.
    """
    if db_name is None:
        raise ValueError("db_name is required (client must provide db_name; no env fallback).")
    value = str(db_name).strip()
    if not value:
        raise ValueError("db_name is required (empty string).")
    return value


def get_storage_domain_for_db_name(db_name: Optional[str]) -> str:
    """
    db_name(프론트 전달값) 기준으로 파일 저장 경로(/FileUpload/{domain}/...)에 사용할 도메인을 결정한다.
    """
    raw = (str(db_name).strip() if db_name is not None else "")
    if not raw:
        return "unknown.han.kr"
    if raw == "dev_user":
        return "dev.han.kr"
    if raw == "testchatbot1":
        return "test.han.kr"
    if raw == "sungdong":  # 성동구청: FileUpload/로컬 저장 경로용 도메인
        return "sungdong.han.kr"
    return f"{raw}.han.kr" # 그 외에는 db_name.han.kr 형태 사용

def _uuid_tail12(chat_bot_id: str) -> str:
    """
    chat_bot_id에서 UUID(마지막 토큰)를 추출하고 마지막 12자리로 축약한다.
    예: "111-222-014e800239bf" -> "014e800239bf"
    """
    raw_uuid = None
    try:
        if chat_bot_id:
            parts = str(chat_bot_id).strip().split("-")
            raw_uuid = parts[-1] if parts else None
    except Exception:
        raw_uuid = None
    raw_uuid = (raw_uuid or "unknown").strip()
    return raw_uuid[-12:] if len(raw_uuid) >= 12 else raw_uuid

def normalize_access_url(access_url: Optional[str], db_name: Optional[str]) -> str:
    """
    '접속url'을 정규화하여 base URL(scheme://host) 형태로 반환한다.

    정책(요청사항):
    - 프론트에서 access_url이 오면 그 값을 최우선 사용
      - https://dev.han.kr/chat/... 처럼 경로가 포함돼도 scheme+netloc만 사용
      - dev.han.kr 처럼 host만 오면 https://를 가정
    - access_url이 없으면 db_name 기준 fallback
      - dev_user     -> https://dev.han.kr
      - testchatbot1 -> https://test.han.kr
      - 그 외        -> https://{db_name}.han.kr
    - db_name이 sungdong이고 access_url 호스트에 sd.go.kr이 포함되면
      https://sungdong.han.kr 로 정규화(로컬 저장·웹 동기화 베이스 통일)
    """
    key = (str(db_name).strip() if db_name is not None else "")

    raw = (str(access_url).strip() if access_url is not None else "")
    if raw:
        try:
            if "://" not in raw:
                raw = "https://" + raw.lstrip("/")
            p = urlparse(raw)
            if p.scheme and p.netloc:
                netloc = (p.netloc or "").split(":")[0].lower()
                # 성동구청: 크롤 대상이 www.sd.go.kr 등이어도 파일 저장·동기화 베이스는 sungdong.han.kr
                if key == "sungdong" and "sd.go.kr" in netloc:
                    return "https://sungdong.han.kr".rstrip("/")
                return f"{p.scheme}://{p.netloc}".rstrip("/")
        except Exception:
            pass

    if not key:
        host = "unknown.han.kr"
    elif key == "dev_user":
        host = "dev.han.kr"
    elif key == "testchatbot1":
        host = "test.han.kr"
    else:
        host = f"{key}.han.kr"
    return f"https://{host}".rstrip("/")

def get_uploaded_files_web_url(*, access_base_url: str, chat_bot_id: str, filename: str) -> str:
    """
    파일 접근 URL(요청사항)을 생성한다.
    Format: {접속url}/chat/uploaded_files/{UUID_마지막12}/{파일명}
    경로 이슈 확인 문서: backend/docs/FILE_STORAGE_FLOW.md
    """
    base = (access_base_url or "").rstrip("/")
    tail = _uuid_tail12(chat_bot_id or "")
    raw_name = str(filename or "").replace("\\", "/").lstrip("/")
    # Encode each path segment exactly once. HWP/HWPX viewer paths are sensitive
    # to raw Hangul/spaces and to double-encoded %25EC-style names.
    name_parts = [quote(unquote(part), safe="") for part in raw_name.split("/") if part]
    name = "/".join(name_parts)
    url = f"{base}/chat/uploaded_files/{tail}/{name}" if name else f"{base}/chat/uploaded_files/{tail}"
    _debug_log(
        "H_path_web_url",
        "backend/shared/config.py:get_uploaded_files_web_url",
        "web_url_computed",
        {
            "access_base_url": access_base_url,
            "chat_bot_id": chat_bot_id,
            "filename": filename,
            "base": base,
            "tail12": tail,
            "url": url,
        },
    )
    return url

def _normalize_domain_for_path(domain_or_url: str) -> str:
    """경로에 쓸 host만 추출 (프로토콜·포트·경로·www 제거)."""
    s = (domain_or_url or "").strip()
    if not s:
        return ""
    if "://" in s:
        s = s.split("://", 1)[1]
    if "/" in s:
        s = s.split("/")[0]
    if ":" in s:
        s = s.split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    return s


def get_uploaded_files_local_dir(
    *,
    access_base_url: str,
    chat_bot_id: str,
    storage_domain: Optional[str] = None,
) -> str:
    """
    실제 파일 저장 디렉토리를 반환한다. 기본은 프로젝트의 downloads 폴더를 사용한다.

    - 저장 경로: {downloads_root}/{host}/{UUID_마지막12}
    - downloads_root: env CRAWL_FILES_ROOT 미설정 시 프로젝트 루트의 downloads/ 사용
    - host: storage_domain 또는 FILEUPLOAD_STORAGE_DOMAIN 또는 access_base_url에서 추출 (크롤링 접속 도메인)
    """
    tail = _uuid_tail12(chat_bot_id or "")
    host = _normalize_domain_for_path(storage_domain) if storage_domain else ""
    if not host:
        host = _normalize_domain_for_path(os.getenv("FILEUPLOAD_STORAGE_DOMAIN", "") or "")
    if not host:
        host = _normalize_domain_for_path(access_base_url or "")
    host = host or "unknown.han.kr"

    # 기본: 기존 downloads 폴더 (프로젝트 루트/downloads). CRAWL_FILES_ROOT로만 override 가능
    downloads_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "downloads"))
    if os.getenv("CRAWL_FILES_ROOT"):
        downloads_root = os.path.abspath(os.getenv("CRAWL_FILES_ROOT", "").strip()) or downloads_root
    path = os.path.join(downloads_root, host, tail)
    if os.getenv("CRAWL_DEBUG_FLOW", "0") == "1":
        logger.info(
            "[Flow] path_local | access_base=%s host=%s tail=%s root=%s path=%s",
            (access_base_url or "")[:220],
            host,
            tail,
            downloads_root,
            path,
        )
    _debug_log(
        "H_path_local_dir",
        "backend/shared/config.py:get_uploaded_files_local_dir",
        "local_dir_downloads",
        {
            "access_base_url": access_base_url,
            "chat_bot_id": chat_bot_id,
            "host": host,
            "tail12": tail,
            "root": str(downloads_root),
            "path": path,
        },
    )
    return path


def get_webserver_uploaded_files_dir(*, access_base_url: str, chat_bot_id: str, db_name: Optional[str] = None) -> str:
    """
    웹서버에 파일이 최종적으로 놓일 디렉토리(절대경로)를 반환한다.
    경로 이슈 확인 문서: backend/docs/FILE_STORAGE_FLOW.md
    """
    # 챗봇 ID의 UUID 토큰에서 마지막 12자리를 추출하여 폴더명으로 사용
    tail = _uuid_tail12(chat_bot_id or "")
    
    # db_name이 전달된 경우 해당 값을 기준으로 저장용 호스트(host) 결정
    if db_name:
        host = get_storage_domain_for_db_name(db_name)
    else:
        # db_name이 없는 경우를 대비한 기존 URL 파싱 백업 로직
        base = (access_base_url or "").strip()
        host = ""
        try:
            p = urlparse(base if "://" in base else ("https://" + base))
            host = (p.netloc or "").split(":")[0]
            if host.startswith("www."):
                host = host[len("www."):]
        except Exception:
            host = ""
        host = host or "unknown.han.kr"

    # 설정된 PREFIX와 호스트, UUID를 조합하여 최종 경로 생성
    path = f"{FILEUPLOAD_URL_PREFIX}/{host}/{tail}"
    
    # 디버그 로그 기록 (기존 로직 유지)
    _debug_log(
        "H_path_remote_dir",
        "backend/shared.config.py:get_webserver_uploaded_files_dir",
        "remote_dir_computed",
        {
            "access_base_url": access_base_url,
            "chat_bot_id": chat_bot_id,
            "host": host,
            "tail12": tail,
            "path": path,
        },
    )
    if os.getenv("CRAWL_DEBUG_FLOW", "0") == "1":
        logger.info(
            "[Flow] path_remote | access_base=%s host=%s tail=%s path=%s",
            access_base_url[:220],
            host,
            tail,
            path,
        )
    return path
    

def get_file_download_path(domain: str, chat_bot_id: str, db_name: Optional[str] = None) -> str:
    """
    파일 다운로드 저장 경로를 반환 (일괄 관리)
    경로 이슈 확인 문서: backend/docs/FILE_STORAGE_FLOW.md
    
    Format: /FileUpload/{접속도메인}/{UUID_마지막12자리}
    Example: /FileUpload/dev.han.kr/014e800239bf
    
    Args:
        domain: 접속 도메인 (예: dev.han.kr - 크롤링 작업 주체)
        chat_bot_id: 챗봇 ID (예: 111-222-014e800239bf) -> 마지막 12자리만 추출하여 사용
    """
    # 1. 도메인 정제 (프로토콜, 포트, 경로 제거)
    if db_name:
        # db_name에 매핑된 도메인(dev.han.kr, sungdong.han.kr 등)을 가져옵니다.
        final_domain = get_storage_domain_for_db_name(db_name)
    else:
        # db_name이 없을 경우 기존의 도메인 정제 로직을 수행합니다.
        final_domain = domain or "unknown"
        if "://" in final_domain: final_domain = final_domain.split("://")[1]
        if ":" in final_domain: final_domain = final_domain.split(":")[0]
        if "/" in final_domain: final_domain = final_domain.split("/")[0]
        
    # 2) UUID 추출
    # - 정책: chat_bot_id의 "가장 마지막 토큰(하이픈 뒤)"를 uuid로 사용
    # - 저장 폴더는 길이를 제한해 안정적으로 운영 (기본: 마지막 12자리)
    #   예: "111-222-014e800239bf" -> "014e800239bf"
    raw_uuid = None
    try:
        if chat_bot_id:
            parts = str(chat_bot_id).strip().split("-")
            raw_uuid = parts[-1] if parts else None
    except Exception:
        raw_uuid = None
    raw_uuid = (raw_uuid or "unknown").strip()
    uuid_tail12 = raw_uuid[-12:] if len(raw_uuid) >= 12 else raw_uuid

    # 3) 최종 경로 (전달 경로 일원화: FILEUPLOAD_URL_PREFIX)
    return f"{FILEUPLOAD_URL_PREFIX}/{final_domain}/{uuid_tail12}"


def get_file_upload_content_url(
    access_base_url: str,
    domain: str,
    chat_bot_id: str,
    filename: str = "",
) -> str:
    """
    Return the public URL saved into LEARN_LIST content.
    Path reference: backend/docs/FILE_STORAGE_FLOW.md

    Physical storage remains /FileUpload/{domain}/{uuid_tail12}/{filename}, but
    the browser/viewer URL is /chat/uploaded_files/{uuid_tail12}/{filename}.
    Example: https://dev.han.kr/chat/uploaded_files/014e800239bf/report.pdf
    """
    public_base = ""
    try:
        public_base = normalize_access_url(domain, None) if domain else ""
    except Exception:
        public_base = ""
    if not public_base:
        public_base = (access_base_url or "").strip()
    if not public_base:
        public_base = "https://unknown.han.kr"
    if "://" not in public_base:
        public_base = "https://" + public_base
    try:
        p = urlparse(public_base)
        origin = f"{p.scheme or 'https'}://{p.netloc or ''}".rstrip("/")
    except Exception:
        origin = public_base.rstrip("/")
    return get_uploaded_files_web_url(
        access_base_url=origin,
        chat_bot_id=chat_bot_id,
        filename=filename,
    )




# =====================================================
# PostgreSQL Connection Pool Management
# (db/db_config.py에서 통합) - 풀 생성 시 backend.shared.config 사용
# =====================================================
import asyncio
try:
    import asyncpg  # optional dependency
except Exception:
    asyncpg = None
import logging
import time
import re

try:
    from backend.shared.config import Config as _PoolConfig
except Exception:
    _PoolConfig = None

logger = logging.getLogger("backend.shared.config")

_PG_CONN_HOLD_START: dict[int, tuple[float, str, str]] = {}


def _pg_conn_hold_warn_ms() -> float:
    try:
        value = float(os.getenv("POSTGRES_CONN_HOLD_WARN_MS", "5000") or "5000")
    except Exception:
        value = 5000.0
    return max(0.0, min(value, 600000.0))


PAGE_VALIDATION_CONFIG = {

    "content_selectors": [
        ".bod_view",
        ".bod_title",
        ".board_view",
        "#contents",
        ".view_cont",
        ".view_content",
        ".article-content",
        ".board_view_title",
        ".bod_view_title",
        ".view_title",
        ".subject",
        ".teacher_cnt",
        ".teacher_cnt .area_title",
    ],

    "error_keywords": [
        "게시글이 존재하지 않습니다",
        "게시물이 존재하지 않습니다",
        "삭제된 게시글입니다",
        "데이터가 없습니다",
        "조회된 데이터가 없습니다",
        "게시물이 없습니다",
        "시스템 오류가 발생하였습니다",
        "잘못된 경로로 접근",
    ],

    "login_indicators": [
        "로그인이 필요합니다",
        "아이디를 입력하세요",
        "비밀번호를 입력하세요",
        "member/login",
        "login_form",
    ]
}

OPENAI_ASADAL_API_KEY = os.getenv("OPENAI_CHATTY_API_KEY")
OPENAI_SECOND_API_KEY = os.getenv("OPENAI_SECOND_API_KEY")
    
class DatabasePool:
    """PostgreSQL 연결 풀 관리 클래스"""
    # Use string annotation for asyncpg.Pool so module import absence doesn't raise at runtime
    _pools: dict[str, tuple["asyncpg.Pool", float]] = {}
    _lock = asyncio.Lock()

    @staticmethod
    async def _close_pool_safely(pool, dbname: str = "", timeout: float = 5.0) -> None:
        try:
            await asyncio.wait_for(pool.close(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "PostgreSQL pool close timeout; terminating pool db=%s timeout=%ss",
                dbname or "-",
                timeout,
            )
            try:
                pool.terminate()
            except Exception as exc:
                logger.warning("PostgreSQL pool terminate failed db=%s err=%s", dbname or "-", exc)
        except Exception as exc:
            logger.warning("PostgreSQL pool close failed db=%s err=%s", dbname or "-", exc)
            try:
                pool.terminate()
            except Exception:
                pass

    @staticmethod
    def _get_connection_owner_pool(conn):
        try:
            holder = getattr(conn, "_holder", None)
            if holder is not None:
                pool = getattr(holder, "_pool", None)
                if pool is not None:
                    return pool
        except Exception:
            pass
        return None

    @classmethod
    async def get_pool(cls, dbname=None):
        """
        요청이 들어오면 해당 dbname에 대한 커넥션 풀을 가져오거나 생성한다.
        일정 시간 요청이 없으면 풀을 자동 해제한다.
        """
        if asyncpg is None:
            logger.error("asyncpg is not installed; PostgreSQL support is disabled. Install asyncpg to enable DB pools.")
            raise ModuleNotFoundError("asyncpg is required for PostgreSQL support. Install with: pip install asyncpg")
        if dbname is None:
            logger.error("Database name cannot be None")
            raise ValueError("Database name cannot be None")
        db_name = str(dbname).strip()
        # 기본적인 DB 이름 유효성 검사: 파일명/경로/확장자 등이 들어오는 실수를 방지
        if not re.fullmatch(r"^[A-Za-z_][A-Za-z0-9_]*$", db_name):
            logger.error(f"Invalid database name requested: {db_name!r}")
            raise ValueError(f"Invalid database name: {db_name!r}")
        
        async with cls._lock:
            existing = cls._pools.get(db_name)
            if existing:
                pool, _ = existing
                is_closed = False
                try:
                    is_closed = bool(pool.is_closed())
                except Exception:
                    is_closed = False
                if is_closed:
                    logger.warning("PostgreSQL pool is closed; recreating db=%s", db_name)
                    cls._pools.pop(db_name, None)

            if db_name not in cls._pools:
                try:
                    # asyncpg.create_pool에 필요한 정보는 backend.shared.config에서 가져옴
                    cfg = _PoolConfig if _PoolConfig is not None else Config
                    pool = await asyncpg.create_pool(
                        database=db_name,
                        user=cfg.DB_USER,
                        password=cfg.DB_PASSWORD,
                        host=cfg.DB_HOST,
                        port=cfg.DB_PORT,
                        min_size=cfg.DB_POOL_MIN,
                        max_size=cfg.DB_POOL_MAX,
                        command_timeout=max(
                            30,
                            int(getattr(cfg, "POSTGRES_STATEMENT_TIMEOUT_MS", 180000) / 1000),
                        ),
                        server_settings={
                            "statement_timeout": str(
                                int(getattr(cfg, "POSTGRES_STATEMENT_TIMEOUT_MS", 180000))
                            )
                        },
                    )
                    cls._pools[db_name] = (pool, time.time())  # (풀, 마지막 사용 시간)
                    logger.info(f"Database pool created for {db_name}")
                except Exception as e:
                    logger.error(f"Failed to create database pool: {e}")
                    raise
            else:
                # 마지막 사용 시간 갱신
                cls._pools[db_name] = (cls._pools[db_name][0], time.time())
        return cls._pools[db_name][0]

    @classmethod
    async def release_unused_pools(cls, timeout=None):
        """
        일정 시간 (timeout) 동안 사용되지 않은 풀을 해제한다.
        (기본 600초 = 10분)
        """
        if timeout is None:
            cfg = _PoolConfig if _PoolConfig is not None else Config
            timeout = cfg.DB_POOL_CHCK

        async with cls._lock:
            current_time = time.time()
            to_remove = []

            for dbname, (pool, last_used) in cls._pools.items():
                if current_time - last_used > timeout:
                    await cls._close_pool_safely(pool, dbname=dbname)
                    to_remove.append(dbname)
                    logger.info(f"🔄 Released unused pool for {dbname}")

            for dbname in to_remove:
                del cls._pools[dbname]

    @classmethod
    async def close_all_pools(cls):
        """FastAPI 종료 시 모든 DB 풀을 정리"""
        logger.info(f"총 {len(cls._pools)}개의 커넥션 풀을 정리합니다.")
        close_tasks = []

        for dbname, (pool, _) in cls._pools.items():
            logger.info(f"풀 닫기 시작: {dbname}")
            try:
                # 강제 종료 시도 (기존 커넥션들 대기 안 함)
                task = asyncio.create_task(cls._close_pool_safely(pool, dbname=dbname, timeout=5.0))
                close_tasks.append(task)
            except Exception as e:
                logger.error(f"풀 닫기 실패: {dbname} - {e}")

        try:
            await asyncio.wait_for(asyncio.gather(*close_tasks), timeout=7)
            logger.info("모든 커넥션 풀 닫기 완료")
        except asyncio.TimeoutError:
            logger.warning("⚠️ 일부 커넥션 풀 닫기 시간 초과")
        except Exception as e:
            logger.error(f"커넥션 풀 닫기 중 오류: {e}")

        cls._pools.clear()

    @classmethod
    async def close_pool(cls, dbname: str) -> None:
        """
        특정 dbname의 풀만 닫는다 (job 종료 시점 정리용).
        다른 dbname의 풀/다른 job에는 영향이 없다.
        """
        if not dbname:
            return
        name = str(dbname).strip()
        if not name:
            return
        async with cls._lock:
            entry = cls._pools.get(name)
            if not entry:
                return
            pool, _ = entry
            try:
                await cls._close_pool_safely(pool, dbname=name)
            except Exception:
                # best-effort
                pass
            try:
                cls._pools.pop(name, None)
            except Exception:
                pass


async def connect_db(dbname=None):
    """Acquire one asyncpg connection from the cached per-database pool."""
    try:
        pool = await DatabasePool.get_pool(dbname)
        try:
            conn = await pool.acquire()
            try:
                task = asyncio.current_task()
                _PG_CONN_HOLD_START[id(conn)] = (
                    time.perf_counter(),
                    str(dbname or ""),
                    task.get_name() if task else "",
                )
            except Exception:
                pass
            return conn
        except Exception as acquire_exc:
            msg = str(acquire_exc).lower()
            if "pool is closed" not in msg:
                raise
            logger.warning("PostgreSQL pool acquire hit closed pool; recreating db=%s", dbname or "-")
            await DatabasePool.close_pool(str(dbname or ""))
            pool = await DatabasePool.get_pool(dbname)
            conn = await pool.acquire()
            try:
                task = asyncio.current_task()
                _PG_CONN_HOLD_START[id(conn)] = (
                    time.perf_counter(),
                    str(dbname or ""),
                    task.get_name() if task else "",
                )
            except Exception:
                pass
            return conn
    except Exception as e:
        logger.error("Database connection failed: %s", e)
        raise


async def return_connection(conn, dbname=None):
    """Return an asyncpg connection to its owner pool."""
    try:
        if conn:
            if getattr(conn, "_con", None) is None:
                logger.debug("Connection already released; skip return db=%s", dbname or "-")
                return
            hold_info = _PG_CONN_HOLD_START.pop(id(conn), None)
            if hold_info:
                started_perf, tracked_dbname, task_name = hold_info
                hold_ms = (time.perf_counter() - started_perf) * 1000.0
                warn_ms = _pg_conn_hold_warn_ms()
                if hold_ms >= warn_ms and warn_ms >= 0:
                    logger.warning(
                        "[Postgres][conn_hold_slow] db=%s hold_ms=%.1f warn_ms=%.1f task=%s pool_count=%s",
                        tracked_dbname or dbname or "-",
                        hold_ms,
                        warn_ms,
                        task_name or "-",
                        len(getattr(DatabasePool, "_pools", {}) or {}),
                    )
            pool = DatabasePool._get_connection_owner_pool(conn)
            if pool is None:
                pool = await DatabasePool.get_pool(dbname)
            await pool.release(conn)
    except Exception as e:
        msg = str(e)
        if "already released" in msg or "not a member of this pool" in msg or "released" in msg:
            logger.debug("Connection release skipped db=%s reason=%s", dbname or "-", msg)
            return
        logger.error("Failed to return connection: %s", e)
