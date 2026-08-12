"""Local file write and readiness helpers for download workers."""

from __future__ import annotations

import os
from uuid import uuid4


# 다운로드 본문을 원자적 교체 전 임시 파일에 기록합니다.
def write_download_bytes(filepath: str, data: bytes) -> None:
    directory = os.path.dirname(filepath)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(filepath, "wb") as file_handle:
        file_handle.write(data)


# 동시 다운로드 간 충돌하지 않는 임시 파일 경로를 생성합니다.
def make_download_temp_path(filepath: str) -> str:
    return f"{filepath}.part-{uuid4().hex}"


# 임시 파일의 안정화 확인 대기 시간을 반환합니다.
def download_temp_ready_timeout_sec() -> float:
    try:
        value = float(os.getenv("DOWNLOAD_TEMP_READY_TIMEOUT_SEC", "60") or "60")
    except Exception:
        value = 60.0
    return max(10.0, min(value, 300.0))


# 최종 파일의 안정화 확인 대기 시간을 반환합니다.
def download_final_ready_timeout_sec() -> float:
    try:
        value = float(os.getenv("DOWNLOAD_FINAL_READY_TIMEOUT_SEC", "60") or "60")
    except Exception:
        value = 60.0
    return max(10.0, min(value, 300.0))
