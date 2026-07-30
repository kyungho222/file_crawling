"""
애플리케이션 레벨 암호화 해시(SHA-256/SHA-1 등) 생성 스위치.
기본: 생성하지 않음. 재활성화: 환경변수 DISABLE_HASH_GENERATION=0
"""
from __future__ import annotations

import hashlib
import os
from typing import Optional, Union

Raw = Union[str, bytes, None]


def hash_generation_disabled() -> bool:
    v = (os.getenv("DISABLE_HASH_GENERATION") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def sha256_hex_utf8(text: Optional[str]) -> Optional[str]:
    if hash_generation_disabled() or text is None:
        return None
    raw = text.encode("utf-8")
    if not raw:
        return None
    return hashlib.sha256(raw).hexdigest()


def sha256_hex_bytes(data: Optional[bytes]) -> Optional[str]:
    if hash_generation_disabled() or not data:
        return None
    return hashlib.sha256(data).hexdigest()


def sha1_hex_utf8(text: str, *, errors: str = "ignore") -> Optional[str]:
    if hash_generation_disabled():
        return None
    return hashlib.sha1(text.encode("utf-8", errors=errors)).hexdigest()
