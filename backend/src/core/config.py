"""
백엔드 설정 관리
"""

import os
from typing import Optional
try:
    from pydantic_settings import BaseSettings
except ImportError:
    from pydantic import BaseSettings
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()


class Settings(BaseSettings):
    """애플리케이션 설정"""
    
    # 환경 설정
    environment: str = os.getenv("ENVIRONMENT", "development")  # "development", "production"
    
    # 서버 설정
    host: str = "0.0.0.0"
    port: int = int(os.getenv("PORT", "23001"))  # 기본값 23001 (file crawler 포트)
    debug: bool = True
    reload: bool = True
    
    # API 키 설정
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    google_api_key: Optional[str] = None
    
    # 데이터베이스 설정
    database_url: str = "sqlite:///./file_crawler.db"
    
    # 파일 업로드 설정
    upload_dir: str = "./uploads"
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    
    
    # 크롤링 설정
    crawl_timeout: int = 30
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
    # 메타데이터 저장 방식 설정
    use_metadata_db: bool = False  # True: DB 저장, False: JSON 저장
    metadata_duplicate_threshold: float = 90.0  # 중복 판정 임계값 (%)
    
    # Celery 설정 (백그라운드 작업용)
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"
    celery_task_track_started: bool = True
    celery_task_time_limit: int = 3600  # 1시간
    
    # CORS 설정
    # ⚠️ 주의: allow_credentials=True일 때 "*" origin은 사용 불가
    # 따라서 "*"는 제거하고 특정 origin만 허용
    cors_origins: list = [
        "http://localhost:3000", 
        "http://localhost:8080", 
        "http://127.0.0.1:3000",
        "https://dev.han.kr",  # ✅ 프론트엔드 도메인
        "http://dev.han.kr",
        "https://dev.chatbaram.com",
        "https://dev.chatbaram.com:7000",  # ✅ 7000 포트 명시
        "http://dev.chatbaram.com",
        "http://dev.chatbaram.com:23001",
        "null"
        # "*" 제거: credentials=True와 함께 사용 불가
    ] if environment == "production" else [
        "http://localhost:3000", 
        "http://localhost:8080", 
        "http://127.0.0.1:3000",
        "https://dev.han.kr",
        "http://dev.han.kr",
        "https://dev.chatbaram.com",
        "https://dev.chatbaram.com:7000",
        "http://dev.chatbaram.com",
        "http://dev.chatbaram.com:23001",
        "null"
        # "*" 제거: credentials=True와 함께 사용 불가
    ]
    cors_allow_credentials: bool = True
    cors_allow_methods: list = ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    cors_allow_headers: list = [
        "Content-Type",  # ✅ 가장 중요: JSON 요청에 필수
        "Authorization",
        "X-Requested-With",
        "Accept",
        "Origin",
        "Cookie",  # ✅ 쿠키 전달을 위해 필수
        "Access-Control-Request-Method",
        "Access-Control-Request-Headers"
        # "*" 제거: credentials=True와 함께 사용 불가
    ]
    
    # 로깅 설정
    log_level: str = "DEBUG"  # DEBUG로 변경하여 메타데이터 비교 상세 로그 출력
    log_file: Optional[str] = "./logs/backend.log"
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # ✅ 정의되지 않은 환경 변수 무시 (php_backend_url 등)


# 전역 설정 인스턴스
settings = Settings()
