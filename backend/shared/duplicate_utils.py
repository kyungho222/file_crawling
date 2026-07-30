import hashlib

from utils.hash_policy import hash_generation_disabled

def generate_unique_key(url: str, filename: str, filesize: int) -> str:
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

ALLOWED_EXTENSIONS = {
    'pdf', 'hwp', 'hwpx', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx'
    # 압축파일(zip, rar, 7z 등)은 제외됨
}

def is_allowed_extension(ext: str) -> bool:
    if not ext:
        return False
    return ext.lower().replace('.', '') in ALLOWED_EXTENSIONS
