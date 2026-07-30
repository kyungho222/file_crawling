"""열람 가능한 암호화 HWPX를 한컴 렌더링과 OCR로 추출한다."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger("edu.encrypted_hwpx_ocr")


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _render_pages_with_hancom(
    hwpx_path: str,
    output_dir: str,
    *,
    timeout_sec: float,
) -> list[str]:
    if os.name != "nt":
        return []

    powershell = shutil.which("powershell.exe") or shutil.which("powershell")
    if not powershell:
        return []

    script = r"""
$ErrorActionPreference = 'Stop'
$SourcePath = $env:CODEX_HWPX_SOURCE_PATH
$OutputDir = $env:CODEX_HWPX_OUTPUT_DIR
if (-not $SourcePath -or -not $OutputDir) {
    throw 'HWPX 렌더링 입력 경로가 없습니다.'
}
$hwp = $null
try {
    $hwp = New-Object -ComObject HWPFrame.HwpObject
    $opened = $hwp.Open($SourcePath, 'HWPX', 'forceopen:true')
    if (-not $opened) {
        throw '한컴에서 HWPX 파일을 열지 못했습니다.'
    }

    Start-Sleep -Milliseconds 500
    $pages = [int]$hwp.PageCount
    $files = @()
    for ($page = 0; $page -lt $pages; $page++) {
        $requested = Join-Path $OutputDir ('page_{0:D4}.jpg' -f ($page + 1))
        $created = $hwp.CreatePageImage($requested, $page, 300, 24, 'jpg')
        $actual = [System.IO.Path]::ChangeExtension($requested, '.bmp')
        if (-not $created -or -not (Test-Path -LiteralPath $actual)) {
            throw ('페이지 이미지 생성 실패: page={0}' -f ($page + 1))
        }
        $files += $actual
    }

    [PSCustomObject]@{
        opened = $opened
        pages = $pages
        files = $files
    } | ConvertTo-Json -Compress
}
finally {
    if ($null -ne $hwp) {
        try { $hwp.Quit() } catch {}
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($hwp) | Out-Null
    }
}
"""

    process_env = os.environ.copy()
    process_env["CODEX_HWPX_SOURCE_PATH"] = os.path.abspath(hwpx_path)
    process_env["CODEX_HWPX_OUTPUT_DIR"] = os.path.abspath(output_dir)
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(10.0, float(timeout_sec or 120.0)),
        check=False,
        env=process_env,
    )
    if completed.returncode != 0:
        logger.error(
            "암호화 HWPX 한컴 렌더링 실패 | file=%s returncode=%s error=%s",
            hwpx_path,
            completed.returncode,
            (completed.stderr or completed.stdout or "").strip()[:1000],
        )
        return []

    payload = None
    for line in reversed((completed.stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
            break
        except json.JSONDecodeError:
            continue
    if not isinstance(payload, dict):
        logger.error(
            "암호화 HWPX 한컴 렌더링 결과 파싱 실패 | file=%s output=%s",
            hwpx_path,
            (completed.stdout or "").strip()[:1000],
        )
        return []

    rendered: list[str] = []
    for raw_path in payload.get("files") or []:
        path = os.path.abspath(str(raw_path or ""))
        if path and os.path.isfile(path):
            rendered.append(path)
    return rendered


def _ocr_page_image(image_path: str, *, timeout_sec: float) -> str:
    import requests
    from PIL import Image

    api_key = str(os.getenv("UPSTAGE_API_KEY", "") or "").strip()
    if not api_key:
        logger.error("암호화 HWPX OCR API 키가 없습니다 | file=%s", image_path)
        return ""

    png_path = str(Path(image_path).with_suffix(".png"))
    with Image.open(image_path) as image:
        image.save(png_path, "PNG")

    with open(png_path, "rb") as image_file:
        response = requests.post(
            "https://api.upstage.ai/v1/document-ai/document-parse",
            headers={"Authorization": f"Bearer {api_key}"},
            data={
                "ocr": True,
                "coordinates": True,
                "output_formats": str(["markdown"]),
            },
            files={
                "document": (
                    os.path.basename(png_path),
                    image_file,
                    "image/png",
                )
            },
            timeout=max(10.0, float(timeout_sec or 60.0)),
        )

    if response.status_code != 200:
        logger.error(
            "암호화 HWPX OCR 실패 | file=%s status=%s response=%s",
            image_path,
            response.status_code,
            (response.text or "")[:500],
        )
        return ""
    try:
        return str(response.json().get("content", {}).get("markdown", "") or "").strip()
    except Exception as exc:
        logger.error(
            "암호화 HWPX OCR 응답 파싱 실패 | file=%s error=%s",
            image_path,
            exc,
        )
        return ""


def extract_encrypted_hwpx_with_hancom_ocr(
    hwpx_path: str,
    *,
    render_timeout_sec: float = 120.0,
    page_timeout_sec: float = 60.0,
) -> str:
    """한컴에서 정상 열람되는 암호화 HWPX를 페이지 OCR 텍스트로 반환한다."""
    if not _env_bool("HWPX_ENCRYPTED_OCR_FALLBACK", True):
        return ""
    if os.name != "nt":
        logger.warning(
            "암호화 HWPX OCR 대체 경로를 사용할 수 없습니다 | reason=hancom_windows_required file=%s",
            hwpx_path,
        )
        return ""

    try:
        with tempfile.TemporaryDirectory(prefix="encrypted_hwpx_ocr_") as temp_dir:
            page_images = _render_pages_with_hancom(
                hwpx_path,
                temp_dir,
                timeout_sec=render_timeout_sec,
            )
            if not page_images:
                return ""

            page_texts: list[str] = []
            for page_number, image_path in enumerate(page_images, start=1):
                text = _ocr_page_image(image_path, timeout_sec=page_timeout_sec)
                if not text:
                    logger.error(
                        "암호화 HWPX OCR 페이지 텍스트가 비었습니다 | page=%s file=%s",
                        page_number,
                        hwpx_path,
                    )
                    return ""
                page_texts.append(f"[Page: {page_number}]\n{text}")

            extracted = "\n\n".join(page_texts).strip()
            logger.warning(
                "암호화 HWPX를 한컴 렌더링 OCR로 추출했습니다 | pages=%s chars=%s file=%s",
                len(page_texts),
                len(extracted),
                hwpx_path,
            )
            return extracted
    except subprocess.TimeoutExpired:
        logger.error(
            "암호화 HWPX 한컴 렌더링 시간 초과 | timeout=%s file=%s",
            render_timeout_sec,
            hwpx_path,
        )
    except Exception as exc:
        logger.exception(
            "암호화 HWPX OCR 대체 처리 실패 | file=%s error=%s",
            hwpx_path,
            exc,
        )
    return ""
