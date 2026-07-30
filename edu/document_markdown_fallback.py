from __future__ import annotations

import asyncio
import logging
import os
import zipfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger("edu.document_markdown_fallback")


STRUCTURED_MARKDOWN_EXTS = {
    ".pdf",
    ".ppt",
    ".pptx",
    ".doc",
    ".docx",
}


def structured_document_fallback_enabled() -> bool:
    return str(os.getenv("FILE_CRAWL_STRUCTURED_DOC_FALLBACK", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def min_plain_text_chars_for_fallback() -> int:
    try:
        value = int(os.getenv("FILE_CRAWL_STRUCTURED_FALLBACK_MIN_CHARS", "300") or "300")
    except Exception:
        value = 300
    return max(0, min(value, 100_000))


def _max_table_probe_pages() -> int:
    try:
        value = int(os.getenv("FILE_CRAWL_TABLE_PROBE_MAX_PAGES", "5") or "5")
    except Exception:
        value = 5
    return max(1, min(value, 50))


def document_likely_has_tables(path: str) -> bool:
    ext = Path(path or "").suffix.lower()
    if ext == ".pptx":
        try:
            with zipfile.ZipFile(path) as zf:
                return any(
                    name.startswith("ppt/slides/") and b"<a:tbl" in zf.read(name)
                    for name in zf.namelist()
                    if name.endswith(".xml")
                )
        except Exception:
            return False

    if ext == ".pdf":
        try:
            import pdfplumber  # type: ignore
        except Exception:
            return False
        try:
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages[:_max_table_probe_pages()]:
                    try:
                        if page.find_tables():
                            return True
                    except Exception:
                        try:
                            tables = page.extract_tables() or []
                        except Exception:
                            tables = []
                        if tables:
                            return True
        except Exception:
            return False

    return False


def should_use_structured_markdown_fallback(path: str, extracted_text: str) -> bool:
    if not structured_document_fallback_enabled():
        return False
    ext = Path(path or "").suffix.lower()
    if ext not in STRUCTURED_MARKDOWN_EXTS:
        return False
    if len((extracted_text or "").strip()) < min_plain_text_chars_for_fallback():
        return True
    if ext in {".pdf", ".ppt", ".pptx"} and document_likely_has_tables(path):
        return True
    return False


def _convert_with_docling(path: str) -> Optional[str]:
    try:
        from docling.document_converter import DocumentConverter  # type: ignore
    except Exception:
        return None
    try:
        result = DocumentConverter().convert(path)
        document = getattr(result, "document", None)
        if document is None:
            return None
        markdown = document.export_to_markdown()
        return markdown if isinstance(markdown, str) and markdown.strip() else None
    except Exception as exc:
        logger.info("[DocFallback] docling conversion failed | path=%s err=%s", path, exc)
        return None


def _convert_with_unstructured(path: str) -> Optional[str]:
    try:
        from unstructured.partition.auto import partition  # type: ignore
    except Exception:
        return None
    try:
        elements = partition(filename=path)
    except Exception as exc:
        logger.info("[DocFallback] unstructured conversion failed | path=%s err=%s", path, exc)
        return None

    parts: list[str] = []
    for element in elements or []:
        text = str(element or "").strip()
        if not text:
            continue
        category = str(getattr(element, "category", "") or "").strip().lower()
        if "title" in category:
            parts.append(f"## {text}")
        elif "table" in category:
            html = getattr(getattr(element, "metadata", None), "text_as_html", None)
            parts.append(str(html).strip() if html else text)
        else:
            parts.append(text)
    markdown = "\n\n".join(parts).strip()
    return markdown or None


async def extract_structured_markdown(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ""

    def _run() -> str:
        markdown = _convert_with_docling(path)
        if markdown:
            return markdown
        markdown = _convert_with_unstructured(path)
        return markdown or ""

    try:
        return (await asyncio.to_thread(_run)).strip()
    except Exception as exc:
        logger.info("[DocFallback] structured markdown fallback failed | path=%s err=%s", path, exc)
        return ""
