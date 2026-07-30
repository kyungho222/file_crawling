# utils/duplicate.py
"""
중복 체크 및 파일 확장자 검증 유틸리티
"""
import hashlib
from config.constants import ALLOWED_EXTENSIONS
from utils.hash_policy import hash_generation_disabled

def generate_unique_key(url: str, filename: str = "", filesize: int = 0) -> str:
    """
    파일의 고유 키를 생성합니다.
    조합: url + filename + filesize
    """
    # None 체크 및 문자열 변환
    url = str(url) if url else ""
    filename = str(filename) if filename else ""
    filesize = str(filesize) if filesize is not None else "0"
    
    raw_string = url + filename + filesize
    if hash_generation_disabled():
        return raw_string[:2048] if len(raw_string) > 2048 else raw_string
    return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

def is_allowed_extension(ext: str) -> bool:
    """
    허용된 파일 확장자인지 확인합니다.
    """
    if not ext:
        return False
    # 점(.) 제거 및 소문자 변환
    clean_ext = ext.lower().replace('.', '')
    # ALLOWED_EXTENSIONS는 점이 포함된 형태이므로 비교를 위해 점 제거
    allowed_set = {e.lower().replace('.', '') for e in ALLOWED_EXTENSIONS}
    return clean_ext in allowed_set
