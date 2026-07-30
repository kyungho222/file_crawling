"""
learn_modules.process_and_store 가 확장자별로 쓰는 추출기와 동일한 규칙으로
로컬 파일에서 평문만 뽑는다(청킹·임베딩·DB 저장 없음).

게시판 file_content_workflow._extract_text_from_saved_file_for_learning 과
scripts/preview_learn_extract.py 가 이 모듈을 공유하면 미리보기 = 운영 본문과 맞춘다.
"""

from __future__ import annotations

import asyncio
import logging
import os

logger = logging.getLogger("edu.learn_file_plain_text")

# process_and_store 의 ext_map 과 맞춤 (sound/video/url/text 제외 — 로컬 파일만)
LEARN_PLAIN_TEXT_EXTS: frozenset[str] = frozenset(
    {
        ".txt",
        ".hwp",
        ".hwpx",
        ".pdf",
        ".xls",
        ".xlsx",
        ".csv",
        ".doc",
        ".docx",
        ".ppt",
        ".pptx",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".bmp",
    }
)

async def _await_extract_with_timeout(awaitable, *, timeout_sec: float | None, path: str, stage: str) -> str:
    try:
        timeout = float(timeout_sec or 0)
    except Exception:
        timeout = 0.0
    timeout = max(0.0, min(timeout, 24 * 3600.0))
    try:
        if timeout > 0:
            return await asyncio.wait_for(awaitable, timeout=timeout)
        return await awaitable
    except asyncio.TimeoutError:
        logger.error(
            "[FileTextExtractTimeout] timeout=%ss stage=%s path=%s",
            int(timeout),
            stage,
            str(path or "")[:260],
        )
        return ""



async def extract_plain_text_like_learn_modules(
    path: str,
    *,
    personal_info_filter: str = "N",
    timeout_sec: float | None = None,
) -> str:
    path = os.path.abspath(os.path.expanduser(path or ""))
    ext = os.path.splitext(path)[1].lower()
    if not path or not os.path.isfile(path):
        return ""

    content_label = os.path.basename(path)

    if ext == ".txt":

        def _read_txt() -> str:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                return f.read()

        return (await _await_extract_with_timeout(asyncio.to_thread(_read_txt), timeout_sec=timeout_sec, path=path, stage="txt") or "").strip()

    if ext == ".hwp":
        from edu.hwp_edu import hwp_to_text

        return (await _await_extract_with_timeout(asyncio.to_thread(hwp_to_text, path), timeout_sec=timeout_sec, path=path, stage="hwp") or "").strip()

    if ext == ".hwpx":
        from edu.hwp_edu import extract_hwpx_data, is_encrypted_hwpx

        if is_encrypted_hwpx(path):
            from edu.encrypted_hwpx_ocr import (
                extract_encrypted_hwpx_with_hancom_ocr,
            )

            return (
                await _await_extract_with_timeout(
                    asyncio.to_thread(
                        extract_encrypted_hwpx_with_hancom_ocr,
                        path,
                    ),
                    timeout_sec=timeout_sec,
                    path=path,
                    stage="encrypted_hwpx_ocr",
                )
                or ""
            ).strip()

        def _hwpx_text() -> str:
            data = extract_hwpx_data(path)
            lines: list[str] = []
            chunks = []
            tables = []
            if isinstance(data, dict):
                chunks = data.get("content") or []
                tables = data.get("tables") or []
            elif isinstance(data, tuple) and len(data) >= 2:
                chunks = data[0] or []
                tables = data[1] or []
            elif isinstance(data, list):
                chunks = data

            if isinstance(chunks, list):
                for c in chunks:
                    if isinstance(c, str) and c.strip():
                        lines.append(c.strip())

            for tbl in tables or []:
                if not isinstance(tbl, dict):
                    continue
                header = tbl.get("header")
                if isinstance(header, str) and header.strip():
                    lines.append(header.strip())
                rows = tbl.get("rows")
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if isinstance(row, list):
                        cell_line = " | ".join(str(x or "").strip() for x in row)
                        if cell_line.strip():
                            lines.append(cell_line.strip())
            return "\n".join(lines)

        return (await _await_extract_with_timeout(asyncio.to_thread(_hwpx_text), timeout_sec=timeout_sec, path=path, stage="hwpx") or "").strip()

    if ext == ".pdf":
        from edu.pdf_edu import extract_pdf_plain_text_like_process_pdf_async

        result = (
            await extract_pdf_plain_text_like_process_pdf_async(
                path,
                content_label,
                personal_info_filter,
                timeout_sec=timeout_sec,
                fail_on_timeout=True,
            )
            or ""
        ).strip()
        return result

    if ext in (".xls", ".xlsx", ".csv"):
        from edu.xls_plain_text_fallback import extract_excel_plain_text_safe

        return (await _await_extract_with_timeout(asyncio.to_thread(extract_excel_plain_text_safe, path), timeout_sec=timeout_sec, path=path, stage="excel") or "").strip()

    if ext in (".doc", ".docx"):
        from edu.doc_edu import extract_doc_plain_text_sync

        return (await _await_extract_with_timeout(asyncio.to_thread(extract_doc_plain_text_sync, path), timeout_sec=timeout_sec, path=path, stage="doc") or "").strip()

    if ext in (".ppt", ".pptx"):

        def _ppt() -> str:
            from edu.pptx_edu import convert_ppt_to_pptx, extract_text_from_ppt

            fp = path
            if fp.lower().endswith(".ppt"):
                fp = convert_ppt_to_pptx(fp)
            return extract_text_from_ppt(fp)

        return (await _await_extract_with_timeout(asyncio.to_thread(_ppt), timeout_sec=timeout_sec, path=path, stage="ppt") or "").strip()

    if ext in (".jpg", ".jpeg", ".png", ".gif", ".bmp"):
        from edu.img_edu import extract_text_from_image

        return (await _await_extract_with_timeout(extract_text_from_image(path), timeout_sec=timeout_sec, path=path, stage="image") or "").strip()

    return ""
