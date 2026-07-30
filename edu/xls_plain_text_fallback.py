from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

import pandas as pd


logger = logging.getLogger("edu.xls.plaintext")


def _normalize_row(row: List[Any]) -> List[str]:
    out: List[str] = []
    for cell in row or []:
        try:
            text = str(cell).strip()
        except Exception:
            text = ""
        if text and text.lower() != "nan":
            out.append(text)
    return out


def _sheets_dict_to_plain_text(sheets: Dict[str, List[List[Any]]]) -> str:
    if not isinstance(sheets, dict) or not sheets:
        return ""
    parts: List[str] = []
    for sheet_name, sheet_rows in sheets.items():
        parts.append(f"## {sheet_name}")
        if not isinstance(sheet_rows, list):
            continue
        for row in sheet_rows:
            normalized = _normalize_row(list(row or [])) if isinstance(row, list) else [str(row).strip()]
            if normalized:
                parts.append(" | ".join(normalized))
        parts.append("")
    return "\n".join(parts).strip()


def _extract_via_pandas(file_path: str) -> str:
    ext = os.path.splitext(file_path or "")[1].lower()
    if ext in (".xls", ".xlsx"):
        raw_sheets = pd.read_excel(file_path, sheet_name=None, header=None, dtype=str)
        normalized: Dict[str, List[List[str]]] = {}
        for sheet_name, df in (raw_sheets or {}).items():
            if df is None or df.empty:
                continue
            rows: List[List[str]] = []
            for _, row in df.fillna("").iterrows():
                row_values = _normalize_row(list(row.tolist()))
                if row_values:
                    rows.append(row_values)
            if rows:
                normalized[str(sheet_name)] = rows
        return _sheets_dict_to_plain_text(normalized)

    if ext == ".csv":
        df = pd.read_csv(
            file_path,
            header=None,
            dtype=str,
            encoding_errors="ignore",
            engine="python",
            on_bad_lines="skip",
            sep=None,
        )
        rows: List[List[str]] = []
        for _, row in df.fillna("").iterrows():
            row_values = _normalize_row(list(row.tolist()))
            if row_values:
                rows.append(row_values)
        return _sheets_dict_to_plain_text({"csv": rows})

    return ""


def extract_excel_plain_text_safe(file_path: str) -> str:
    """Best-effort xls/xlsx/csv plain-text extraction for file crawling learn preview."""
    try:
        from edu.xls_edu import extract_excel_to_plain_text as _primary

        text = (_primary(file_path) or "").strip()
        if text:
            return text
        logger.warning("excel primary extract empty; try pandas fallback | path=%s", file_path)
    except Exception as exc:
        logger.warning(
            "excel primary extract failed; try pandas fallback | path=%s err=%s",
            file_path,
            exc,
            exc_info=True,
        )

    try:
        text = (_extract_via_pandas(file_path) or "").strip()
        if text:
            logger.info("excel pandas fallback extract success | path=%s chars=%s", file_path, len(text))
        else:
            logger.warning("excel pandas fallback extract empty | path=%s", file_path)
        return text
    except Exception as exc:
        logger.warning(
            "excel pandas fallback extract failed | path=%s err=%s",
            file_path,
            exc,
            exc_info=True,
        )
        return ""
