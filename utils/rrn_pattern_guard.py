"""
학습 전 단계에서 주민등록번호 '형태'(숫자 6자리 + 하이픈 + 숫자 7자리)를 정규식으로 탐지한다.
실제 유효성(체크섬)은 검사하지 않으며, 패턴 일치 시 학습을 건너뛴다.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

# ASCII 하이픈 및 자주 쓰이는 유니코드 하이픈/마이너스
_RRN_SEPARATOR = r"[-－‐‑‒–—]"

# 더 긴 숫자열 일부로 오인하지 않도록 경계 사용
KOREAN_RRN_LIKE_PATTERN = re.compile(
    rf"(?<!\d)\d{{6}}\s*{_RRN_SEPARATOR}\s*\d{{7}}(?!\d)",
)


def learning_blocked_by_rrn_pattern(text: Optional[str]) -> bool:
    """
    주민번호 유사 패턴이 있으면 True → 학습 건너뜀.
    없으면 False → 학습 진행.
    """
    if not text or not isinstance(text, str):
        return False
    return KOREAN_RRN_LIKE_PATTERN.search(text) is not None



def find_rrn_like_patterns(text: Optional[str]) -> list[str]:
    """Return resident-registration-number-like values found in text."""
    if not text or not isinstance(text, str):
        return []
    return [m.group(0) for m in KOREAN_RRN_LIKE_PATTERN.finditer(text)]


def mask_rrn_like_patterns(text: Optional[str], *, replacement: str = "[RRN_MASKED]") -> str:
    """Mask resident-registration-number-like values while preserving the rest of text."""
    if not text or not isinstance(text, str):
        return "" if text is None else str(text)
    return KOREAN_RRN_LIKE_PATTERN.sub(replacement, text)

def rrn_blocked_learning_payload(content_key: str) -> Dict[str, Any]:
    """학습 스킵 시 process_* / learn_modules 에서 공통으로 쓰는 결과 딕셔너리."""
    return {
        "status": "blocked_rrn",
        "chunks": 0,
        "chunk_count": [0],
        "use_source": [content_key],
        "message": "주민등록번호 유사 패턴(6자리-7자리)이 검출되어 학습을 건너뜁니다.",
    }

