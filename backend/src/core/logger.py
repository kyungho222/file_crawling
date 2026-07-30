"""
백엔드 로깅 설정
"""

import logging
import sys
from typing import Optional
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logger(
    name: str = "file_crawler_backend",
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    format_string: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    로거 설정
    
    Args:
        name: 로거 이름
        level: 로그 레벨
        log_file: 로그 파일 경로 (선택사항)
        format_string: 로그 포맷 문자열 (선택사항)
        max_bytes: 로그 파일 최대 크기 (기본: 10MB)
        backup_count: 백업 파일 개수 (기본: 5개)
    
    Returns:
        설정된 로거
    """
    
    # 기본 포맷 문자열
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    
    # 로거 생성
    logger = logging.getLogger(name)
    
    # 이미 핸들러가 있으면 재설정하지 않음 (중복 방지)
    if logger.handlers:
        # 이미 설정된 로거 반환
        logger.propagate = False  # 부모 로거로 전파 방지
        return logger
    
    logger.setLevel(level)
    logger.propagate = False  # 부모 로거로 전파 방지 (중복 로그 제거)
    
    # 포맷터 생성
    formatter = logging.Formatter(format_string)
    
    # 콘솔 핸들러 (간단한 포맷, UTF-8 인코딩 설정)
    console_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    console_formatter = logging.Formatter(console_format)
    
    # UTF-8 인코딩 강제 (Windows 환경 대응)
    import io
    import platform
    
    if platform.system() == 'Windows':
        # Windows에서만 UTF-8 래퍼 사용 (기존 stdout은 유지)
        try:
            # reconfigure로 인코딩만 변경 (Python 3.7+)
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
                console_handler = logging.StreamHandler(sys.stdout)
            else:
                # fallback: 새 래퍼 생성하되 buffer 복사
                utf8_stdout = io.TextIOWrapper(
                    sys.stdout.buffer, 
                    encoding='utf-8', 
                    errors='replace',
                    line_buffering=True
                )
                console_handler = logging.StreamHandler(utf8_stdout)
        except Exception as e:
            # 실패 시 기본 stdout 사용
            console_handler = logging.StreamHandler(sys.stdout)
    else:
        # Unix/Linux는 기본 stdout 사용
        console_handler = logging.StreamHandler(sys.stdout)
    
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 파일 핸들러 (로테이션 기능, 상세한 포맷)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # RotatingFileHandler 사용 (로그 로테이션)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"📝 로그 파일 설정: {log_file} (최대 {max_bytes // (1024*1024)}MB, 백업 {backup_count}개)")
    
    return logger


# 기본 로거 인스턴스
logger = setup_logger()
