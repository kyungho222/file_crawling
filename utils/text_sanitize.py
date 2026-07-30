import re
from typing import Optional

NUL_RE = re.compile(r'\x00+')
CTRL_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]+')

_EMOJI_RE = re.compile(
    "[\U00010000-\U0010FFFF]",
    flags=re.UNICODE
)


def remove_emoji(text: str) -> str:
    """
    문자열에서 이모지를 제거
    """
    if not text:
        return text
    return _EMOJI_RE.sub("", text)

def remove_nuls_from_str(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    if '\x00' not in s:
        return s
    return NUL_RE.sub('', s)

def sanitize_text_input(value) -> Optional[str]:
    """
    안전한 텍스트 정리:
    - bytes: NUL 바이트 제거 후 UTF-8로 디코딩(문제 있는 바이트는 'replace'로 대체)
    - str: NUL 및 비표시 제어문자 제거
    - None: None 반환
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        cleaned = value.replace(b'\x00', b'')
        try:
            return cleaned.decode('utf-8', 'replace')
        except Exception:
            return cleaned.decode('latin1', 'replace')
    if isinstance(value, str):
        # 우선 NUL 제거
        cleaned = remove_nuls_from_str(value)
        # 제어문자 제거(탭/newline 제외)
        cleaned = CTRL_RE.sub('', cleaned)
        return cleaned
    return str(value)


