"""
HWP → PDF 변환 모듈.
LibreOffice headless를 사용하여 HWP 파일을 PDF로 변환합니다.
process_hwp에서 텍스트 추출 실패 시 fallback으로 사용됩니다.
"""
import os
import subprocess
import sys
import tempfile
import shutil
import logging

from utils.logging_util import LoggerSingleton

logger = LoggerSingleton.get_logger(logger_name="edu.hwp_to_pdf", level=logging.INFO)


def _get_soffice_path() -> str | None:
    """LibreOffice soffice 실행 경로를 반환합니다. 없으면 None."""
    if sys.platform == "win32":
        candidates = [
            os.environ.get("LIBREOFFICE_PATH"),
            r"C:\Program Files\LibreOffice\program\soffice.exe",
            r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
        ]
        for p in candidates:
            if p and os.path.isfile(p):
                return p
        # PATH에 있을 수 있음
        return shutil.which("soffice") or shutil.which("libreoffice")
    return shutil.which("soffice") or shutil.which("libreoffice")


def convert_hwp_to_pdf(hwp_file_path: str) -> str | None:
    """
    HWP 파일을 PDF로 변환합니다.
    LibreOffice headless(soffice)를 사용합니다.

    Args:
        hwp_file_path: HWP 파일 절대 경로.

    Returns:
        생성된 PDF 파일의 절대 경로. 실패 시 None.
        호출자는 사용 후 해당 파일(및 필요 시 임시 디렉터리) 삭제를 권장합니다.
    """
    if not os.path.isfile(hwp_file_path):
        logger.warning(f"[HWP→PDF] 파일 없음: {hwp_file_path}")
        return None

    soffice = _get_soffice_path()
    if not soffice:
        logger.warning("[HWP→PDF] LibreOffice(soffice)를 찾을 수 없습니다. PATH 또는 LIBREOFFICE_PATH를 확인하세요.")
        return None

    out_dir = tempfile.mkdtemp(prefix="hwp2pdf_")
    try:
        cmd = [
            soffice,
            "--headless",
            "--convert-to", "pdf",
            "--outdir", out_dir,
            os.path.abspath(hwp_file_path),
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=os.path.dirname(os.path.abspath(hwp_file_path)) or ".",
        )
        if result.returncode != 0:
            logger.warning(
                f"[HWP→PDF] 변환 실패 exit={result.returncode} stderr={result.stderr!r} file={hwp_file_path}"
            )
            return None

        base = os.path.splitext(os.path.basename(hwp_file_path))[0]
        pdf_path = os.path.join(out_dir, base + ".pdf")
        if not os.path.isfile(pdf_path):
            logger.warning(f"[HWP→PDF] 출력 PDF가 생성되지 않음: {pdf_path}")
            return None

        logger.info(f"[HWP→PDF] 변환 성공: {hwp_file_path} -> {pdf_path}")
        return pdf_path
    except subprocess.TimeoutExpired:
        logger.warning(f"[HWP→PDF] 변환 타임아웃(120s): {hwp_file_path}")
        return None
    except Exception as e:
        logger.warning(f"[HWP→PDF] 변환 중 오류: {e} file={hwp_file_path}", exc_info=True)
        return None
    finally:
        # out_dir은 호출자가 PDF 사용 후 삭제할 수 있도록 반환하지 않고,
        # PDF 경로만 반환. 삭제는 process_hwp 쪽에서 pdf_path 기준으로 처리.
        # (out_dir 내에 pdf만 있으므로, pdf 삭제 후 rmdir 가능)
        pass


def cleanup_converted_pdf(pdf_path: str) -> None:
    """
    변환된 임시 PDF와 그 부모 임시 디렉터리를 삭제합니다.
    convert_hwp_to_pdf()로 얻은 경로에 대해 사용 후 호출하세요.
    """
    if not pdf_path or not os.path.isfile(pdf_path):
        return
    try:
        os.unlink(pdf_path)
        parent = os.path.dirname(pdf_path)
        if parent and "hwp2pdf_" in parent and os.path.isdir(parent):
            try:
                os.rmdir(parent)
            except OSError:
                pass
    except OSError as e:
        logger.debug(f"[HWP→PDF] 정리 중 무시: {pdf_path} {e}")
