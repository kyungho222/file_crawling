from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse


def is_sungdong_contract_detail_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()
    except Exception:
        u = str(url or "").lower()
        return "sd.go.kr" in u and "newselectcontractwebview.do" in u
    return (
        (host == "sd.go.kr" or host.endswith(".sd.go.kr"))
        and path.endswith("/newselectcontractwebview.do")
        and "ctrtacctbookmngno=" in query
    )


def is_sungdong_cffdn_format_detail_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()
    except Exception:
        u = str(url or "").lower()
        return "sd.go.kr" in u and "viewtncffdnformatu.do" in u
    return (
        (host == "sd.go.kr" or host.endswith(".sd.go.kr"))
        and path.endswith("/viewtncffdnformatu.do")
        and "cffdnno=" in query
    )


def sungdong_cffdn_invalid_notice_reason(*, html: str, url: str = "") -> str:
    if not is_sungdong_cffdn_format_detail_url(url):
        return ""
    h_low = str(html or "").lower()
    if not h_low:
        return ""
    if (
        "decodeuricomponent" in h_low
        and "window.history.back" in h_low
        and "alert(msg)" in h_low
        and "%eb%af%bc%ec%9b%90%ec%84%9c%ec%8b%9d%ec%9d%b4%20%ec%a1%b4%ec%9e%ac%ed%95%98%ec%a7%80%20%ec%95%8a%ec%8a%b5%eb%8b%88%eb%8b%a4" in h_low
    ):
        return "sungdong_cffdn_missing_format_notice"
    if (
        "decodeuricomponent" in h_low
        and "window.history.back" in h_low
        and "alert(msg)" in h_low
        and len(h_low) < 3000
    ):
        return "sungdong_cffdn_script_notice_page"
    return ""


def _norm_label(text: str) -> str:
    return re.sub(r"[\s:|]+", "", str(text or "")).strip()


def _clean_text(node: Any) -> str:
    try:
        return " ".join((node.get_text(" ", strip=True) or "").split()).strip()
    except Exception:
        return ""


def _value_after_label(table: Any, *labels: str) -> str:
    wanted = {_norm_label(label) for label in labels if label}
    if table is None or not wanted:
        return ""

    try:
        rows = table.select("tr")
    except Exception:
        rows = []
    for tr in rows:
        try:
            cells = tr.find_all(["th", "td"], recursive=False)
        except Exception:
            cells = []
        for idx, cell in enumerate(cells[:-1]):
            if getattr(cell, "name", "").lower() != "th":
                continue
            if _norm_label(cell.get_text(" ", strip=True)) not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                if getattr(nxt, "name", "").lower() != "td":
                    continue
                value = _clean_text(nxt)
                if value:
                    return value
    return ""


def extract_sungdong_contract_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_sungdong_contract_detail_url(url)):
        return ""

    for selector in (
        "div.contract_title",
        ".contract_program_view div.contract_title",
        "#contents div.contract_title",
    ):
        try:
            title = _clean_text(soup.select_one(selector))
        except Exception:
            title = ""
        if title:
            return title

    try:
        tables = list(soup.select("table.p-table, table[data-table], table"))
    except Exception:
        tables = []
    for table in tables:
        title = _value_after_label(table, "계약명")
        if title:
            return title
    return ""
