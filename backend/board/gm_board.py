from __future__ import annotations

import copy
import re
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from backend.board.url_param_date_extractor import extract_req_ymd_date


def is_gm_general_bbs_url(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return "gm.go.kr" in u and "/user/bbs/bd_selectbbs.do" in u


def is_gm_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        host = urlparse(str(url or "")).netloc.lower()
    except Exception:
        host = ""
    return host == "gm.go.kr" or host.endswith(".gm.go.kr") or "gm.go.kr" in str(url or "").lower()


def is_gm_nftc_bbs_url(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return "gm.go.kr" in u and "/user/nftcbbs/bd_selectnftcbbsdetail.do" in u


def is_gm_contract_detail_url(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return (
        "gm.go.kr" in u
        and "/disclosure/bidcontractinfo/contractinfo/" in u
        and (
            "contractview.do" in u
            or "accountview.do" in u
        )
    )


def is_gm_festival_detail_url(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return "gm.go.kr" in u and (
        "/pt/ns/gmfestival/view.do" in u
        or "/tour/festival/detail.do" in u
    )


def is_gm_festival_notice_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = re.sub(r"/+", "/", parsed.path or "").lower()
    except Exception:
        u = str(url or "").lower().split("?", 1)[0]
        return "gm.go.kr" in u and u.rstrip("/").endswith("/tour/festival")
    return (host == "gm.go.kr" or host.endswith(".gm.go.kr")) and path.rstrip("/") == "/tour/festival"


def is_gm_group_info_url(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return "gm.go.kr" in u and "/pt/gi/cityhallinfo/groupinfo/view.do" in u


def is_gm_lobas_tcm_detail_url(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return "gm.go.kr" in u and "/pt/user/lobastcm/bd_selectlobastcmbbsdetail.do" in u


def is_gm_static_info_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        u = str(url or "").lower()
        host = ""
        path = u.split("?", 1)[0]
    return (
        (host == "gm.go.kr" or host == "www.gm.go.kr" or (not host and "gm.go.kr" in str(url or "").lower()))
        and path.endswith(".jsp")
    )


def _gm_static_info_title_from_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(str(url or ""))
        path = re.sub(r"/+", "/", parsed.path or "").lower().rstrip("/")
    except Exception:
        path = str(url or "").lower().split("?", 1)[0].rstrip("/")
    title_by_suffix = {
        "/pt/partinfo/tb/localtax/ptmn670.jsp": "지방세 > 마을 세무사",
        "/pt/partinfo/tb/localtax/localtaxsummary/ptmn681.jsp": "지방세개요 > 지역자원시설세",
        "/pt/partinfo/tb/localtax/ptmn692.jsp": "지방세 > 납세자권리헌장",
        "/pt/complaint/cp/refrom/refrom1.jsp": "행정규제 개혁 > 규제개혁",
        "/pt/complaint/application/ptmn027.jsp": "민원처리공개",
    }
    for suffix, title in title_by_suffix.items():
        if path.endswith(suffix):
            return str(title or "").replace(" > ", " - ")
    return ""


def is_gm_epeople_iframe_page(url: str | None) -> bool:
    if not url:
        return False
    u = str(url or "").lower()
    return (
        "gm.go.kr" in u
        and u.split("?", 1)[0].endswith(".jsp")
        and "/pt/complaint/" in u
    )


def extract_gm_contract_reg_date(url: str | None) -> datetime | None:
    if not url or not is_gm_contract_detail_url(url):
        return None
    return extract_req_ymd_date(url)


def extract_gm_contract_reg_date_from_soup(soup: Any, url: str | None = None) -> datetime | None:
    if soup is None or (url and not is_gm_contract_detail_url(url)):
        return None
    table = _select_gm_contract_table(soup)
    for label in ("계약일자", "계약일", "계약체결일", "첫계약일"):
        value, _ = _value_after_label(table, label)
        dt = _parse_ymd_date(value)
        if dt:
            return dt
    text = _clean_text(table) or _clean_text(soup)
    return _date_after_text_label(text, "계약일자", "계약일", "계약체결일", "첫계약일")


def _norm_label(text: str) -> str:
    return re.sub(r"[\s:|]+", "", str(text or "")).strip()


def _clean_text(node: Any) -> str:
    try:
        return " ".join((node.get_text(" ", strip=True) or "").split()).strip()
    except Exception:
        return ""


def _value_after_label(table: Any, *labels: str) -> tuple[str, Any | None]:
    wanted = {_norm_label(label) for label in labels if label}
    if table is None or not wanted:
        return "", None

    for tr in table.select("tr"):
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
                    return value, nxt
                return "", nxt
    return "", None


def _parse_ymd_date(text: str) -> datetime | None:
    m = re.search(
        r"(\d{4})\s*[-./]\s*(\d{1,2})\s*[-./]\s*(\d{1,2})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?",
        str(text or ""),
    )
    if not m:
        return None
    try:
        hour = int(m.group(4) or 0)
        minute = int(m.group(5) or 0)
        second = int(m.group(6) or 0)
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), hour, minute, second)
    except Exception:
        return None


def _date_after_text_label(text: str, *labels: str) -> datetime | None:
    source = re.sub(r"\s+", " ", str(text or "")).strip()
    if not source:
        return None
    for label in labels:
        if not label:
            continue
        pattern = (
            re.escape(label)
            + r"\s*[:：]?\s*"
            + r"(\d{4}\s*[-./]\s*\d{1,2}\s*[-./]\s*\d{1,2}(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?)"
        )
        m = re.search(pattern, source)
        if not m:
            continue
        dt = _parse_ymd_date(m.group(1))
        if dt:
            return dt
    return None


def _select_gm_contract_table(soup: Any) -> Any | None:
    if soup is None:
        return None
    try:
        tables = list(soup.select("table.table_style2")) or list(soup.select("table"))
    except Exception:
        tables = []
    if not tables:
        return None

    for table in tables:
        title, _ = _value_after_label(table, "계약명")
        if title:
            return table
    return tables[0]


def _gm_general_title_from_table(table: Any) -> tuple[str, str]:
    for label in (
        "제목",
        "Subject",
        "subject",
        "SUBJECT",
        "민원사무명",
        "사무명",
        "업소명",
        "명칭",
        "상호",
        "시설명",
        "처분명",
    ):
        title, _ = _value_after_label(table, label)
        if title:
            return title, label
    return "", ""


def _is_gm_general_bbs_table_candidate(table: Any) -> bool:
    if table is None:
        return False
    title, _ = _gm_general_title_from_table(table)
    if title:
        return True

    try:
        summary = _norm_label(str(table.get("summary") or ""))
    except Exception:
        summary = ""
    try:
        caption_node = table.find("caption")
        caption = _norm_label(_clean_text(caption_node)) if caption_node is not None else ""
    except Exception:
        caption = ""
    try:
        labels = {_norm_label(th.get_text(" ", strip=True)) for th in table.select("th")}
    except Exception:
        labels = set()

    title_labels = {
        "제목",
        "Subject",
        "subject",
        "SUBJECT",
        "민원사무명",
        "사무명",
        "업소명",
        "명칭",
        "상호",
        "시설명",
        "처분명",
    }
    return bool(
        title_labels.intersection(labels)
        or "제목" in summary
        or "제목" in caption
    )


def _select_gm_general_bbs_table(soup: Any) -> Any | None:
    if soup is None:
        return None
    try:
        tables = list(soup.select("table.bbsView"))
    except Exception:
        tables = []
    if not tables:
        try:
            tables = [
                table
                for table in soup.select("table.type2, table.table-view, table.table_view, table")
                if _is_gm_general_bbs_table_candidate(table)
            ]
        except Exception:
            tables = []
    if not tables:
        return None
    for table in tables:
        title, _ = _gm_general_title_from_table(table)
        if title:
            return table
    return tables[0]


def _select_gm_bbs_viewbox(soup: Any) -> Any | None:
    if soup is None:
        return None
    for sel in (
        ".bbs_viewbox",
        ".bbs_view.bbs_new_skin",
        ".bbs_view",
        ".board_view",
    ):
        try:
            node = soup.select_one(sel)
        except Exception:
            node = None
        if node is not None and _clean_text(node):
            return node
    return None


def _gm_bbs_box_title(soup: Any) -> str:
    box = _select_gm_bbs_viewbox(soup)
    root = box or soup
    for sel in (
        "h5.view_title",
        ".board_view > h5.view_title",
        ".board_view .view_title",
        ".subjectbox",
        ".subject_box",
        ".board_subject",
        ".view_subject",
        ".titlebox",
    ):
        try:
            node = root.select_one(sel)
        except Exception:
            node = None
        title = _clean_text(node)
        if title:
            return title
    return ""


def is_gm_invalid_request_page(soup: Any = None, html: str | None = None, url: str | None = None) -> bool:
    if url and not is_gm_url(url):
        return False
    source = str(html or "")
    if not source and soup is not None:
        try:
            source = str(soup)
        except Exception:
            source = ""
    low = source.lower()
    if "history.back" in low and "alert(" in low:
        try:
            text = _clean_text(soup) if soup is not None else ""
        except Exception:
            text = ""
        if not text or len(text) < 200:
            return True
        try:
            if soup is not None and not (
                soup.select_one(".subjectbox")
                or soup.select_one(".bbs_viewbox")
                or soup.select_one(".viewcontentbox")
                or soup.select_one("table.table-view")
                or soup.select_one("table.bbsView")
                or soup.select_one("table.type2")
            ):
                return True
        except Exception:
            return True
    return False


def extract_gm_general_title(soup: Any) -> str:
    if soup is None:
        return ""
    table = _select_gm_general_bbs_table(soup)
    title, _ = _gm_general_title_from_table(table)
    return title or _gm_bbs_box_title(soup)


def extract_gm_nftc_title(soup: Any) -> str:
    return extract_gm_general_title(soup)


def extract_gm_static_info_title(soup: Any, url: str | None = None) -> str:
    if soup is None:
        return _gm_static_info_title_from_url(url)

    parts: list[str] = []
    for selector_group in (
        ("#depth2-t", ".depth2-t", "#dept2-t", ".dept2-t"),
        ("#depth3-t", ".depth3-t", "#dept3-t", ".dept3-t"),
    ):
        for sel in selector_group:
            try:
                node = soup.select_one(sel)
            except Exception:
                node = None
            text = _clean_text(node)
            if text:
                if text not in parts:
                    parts.append(text)
                break
    if parts:
        return " - ".join(parts)

    for sel in (".page-title h3", ".page-title h4"):
        try:
            node = soup.select_one(sel)
        except Exception:
            node = None
        text = _clean_text(node)
        if text and text not in parts:
            parts.append(text)
    if parts:
        return " - ".join(parts)

    for container_sel in (
        "#contents",
        "#content",
        ".contents",
        ".content",
        ".substance",
        ".pri_box",
        "main",
        "body",
    ):
        try:
            root = soup.select_one(container_sel)
        except Exception:
            root = None
        if root is None:
            continue
        heading_parts: list[str] = []
        try:
            headings = root.select("h3, h4")
        except Exception:
            headings = []
        for node in headings:
            text = _clean_text(node)
            if text and text not in heading_parts:
                heading_parts.append(text)
            if len(heading_parts) >= 2:
                break
        if heading_parts:
            return " - ".join(heading_parts)

    try:
        node = soup.select_one("title")
    except Exception:
        node = None
    title = _clean_text(node)
    if title:
        return re.split(r"\s*>\s*", title, maxsplit=1)[0].strip()
    return _gm_static_info_title_from_url(url)


def try_extract_gm_static_info_post(soup: Any, url: str | None):
    if soup is None or not is_gm_static_info_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    title = (extract_gm_static_info_title(soup, url=url) or "").strip()
    if not title:
        return None

    candidates: list[Any] = []
    for sel in (
        ".sub_content_cont_rt_cont",
        "#c-contents .sub_content_cont_rt_cont",
        "#c-contents",
        ".sub_contents",
    ):
        try:
            candidates.extend(list(soup.select(sel)))
        except Exception:
            continue
    if not candidates:
        return None

    unique: list[Any] = []
    seen: set[int] = set()
    for node in candidates:
        ident = id(node)
        if ident in seen:
            continue
        seen.add(ident)
        unique.append(node)

    leaf_nodes: list[Any] = []
    for node in unique:
        try:
            descendants = list(node.descendants)
        except Exception:
            descendants = []
        if not any(other is not node and other in descendants for other in unique):
            leaf_nodes.append(node)
    candidates = leaf_nodes or unique
    candidates.sort(key=lambda node: len(_clean_text(node)), reverse=True)
    root = candidates[0]

    try:
        frag = BeautifulSoup(str(copy.copy(root)), "html.parser")
    except Exception:
        return None

    for sel in (
        "script",
        "style",
        "noscript",
        "iframe",
        "form",
        "button",
        "input",
        "select",
        "textarea",
        ".page-title",
        ".pri_box",
        ".snb",
        ".lnb",
        ".left_menu",
        ".location",
        ".breadcrumb",
        ".sub_path",
        ".tab",
        ".tab_menu",
        ".sub_tab",
        ".btn",
        ".button",
        ".print",
        ".sns",
        ".share",
    ):
        for el in list(frag.select(sel)):
            try:
                el.decompose()
            except Exception:
                pass

    node = frag.find(True)
    if node is None:
        return None

    content_text = _extract_content_text(node)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = (content_text or "").strip()
    if not content_text:
        content_text = _clean_text(node)
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(node).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def extract_gm_contract_title(soup: Any) -> str:
    if soup is None:
        return ""
    table = _select_gm_contract_table(soup)
    title, _ = _value_after_label(table, "계약명")
    return title


def _select_gm_lobas_tcm_table(soup: Any) -> Any | None:
    if soup is None:
        return None
    try:
        tables = list(soup.select("table.table_style2")) or list(soup.select("table"))
    except Exception:
        tables = []
    for table in tables:
        title, _ = _value_after_label(table, "계약명")
        contract_date, _ = _value_after_label(table, "계약일자")
        target, _ = _value_after_label(table, "계약대상자")
        if title and (contract_date or target):
            return table
    return None


def extract_gm_lobas_tcm_title(soup: Any) -> str:
    table = _select_gm_lobas_tcm_table(soup)
    title, _ = _value_after_label(table, "계약명")
    return title


def extract_gm_lobas_tcm_reg_date(soup: Any, url: str | None = None) -> datetime | None:
    if soup is None or (url and not is_gm_lobas_tcm_detail_url(url)):
        return None
    table = _select_gm_lobas_tcm_table(soup)
    labels = (
        "계약일자",
        "계약일",
        "계약체결일",
        "첫계약일",
        "최초계약일",
        "등록일",
        "작성일",
        "게시일",
    )
    for label in labels:
        value, _ = _value_after_label(table, label)
        dt = _parse_ymd_date(value)
        if dt:
            return dt
    text = _clean_text(table) or _clean_text(soup)
    return _date_after_text_label(text, *labels)


def _select_gm_festival_table(soup: Any) -> Any | None:
    if soup is None:
        return None
    try:
        tables = list(soup.select("table.table_style2")) or list(soup.select("table"))
    except Exception:
        tables = []
    for table in tables:
        title, _ = _value_after_label(table, "행사명")
        if title:
            return table
    for table in tables:
        program, _ = _value_after_label(table, "프로그램")
        event_date, _ = _value_after_label(table, "행사일시", "행사기간")
        place, _ = _value_after_label(table, "장소")
        if program and (event_date or place):
            return table
    return None


def _extract_gm_festival_labeled_title(line: str) -> str:
    text = re.sub(r"\s+", " ", str(line or "")).strip()
    if not text:
        return ""
    match = re.match(r"^[oㅇ○\-ㆍ·]?\s*([^:：]{1,20})\s*[:：]\s*(.+)$", text)
    if not match:
        return ""
    label = re.sub(r"\s+", "", match.group(1) or "").strip()
    value = re.sub(r"\s+", " ", match.group(2) or "").strip()
    if not value:
        return ""
    title_labels = {
        "대회명",
        "공연명",
        "행사명",
        "축제명",
        "프로그램명",
        "프로그램",
        "전시명",
        "강연명",
        "교육명",
        "체험명",
        "명칭",
    }
    if label in title_labels:
        return value
    return ""


def extract_gm_festival_title(soup: Any) -> str:
    table = _select_gm_festival_table(soup)
    title, _ = _value_after_label(table, "행사명")
    if title:
        return title
    title, _ = _value_after_label(table, "프로그램")
    if title:
        return title
    try:
        detail = soup.select_one(".view-box2, .pre-view, .view_box2")
    except Exception:
        detail = None
    try:
        detail_text = detail.get_text("\n", strip=True) if detail is not None else ""
    except Exception:
        detail_text = ""
    detail_lines = []
    for raw_line in str(detail_text or "").splitlines():
        line = re.sub(r"\s+", " ", raw_line or "").strip()
        if not line:
            continue
        detail_lines.append(line)
        labeled_title = _extract_gm_festival_labeled_title(line)
        if labeled_title:
            return labeled_title
    for line in detail_lines:
        if re.match(r"^[oㅇ○]\s*(일시|장소|초청|주제|기간|문의|시간)\s*[:：]", line):
            continue
        return line
    text = _clean_text(detail)
    if text:
        first = re.split(r"\s+[oㅇ○]\s*(?:일시|장소|초청|주제|기간|문의|시간)\s*[:：]", text, maxsplit=1)[0].strip()
        if first:
            return first
    return ""


def extract_gm_festival_reg_date(soup: Any, url: str | None = None) -> datetime | None:
    if soup is None or (url and not is_gm_festival_detail_url(url)):
        return None
    table = _select_gm_festival_table(soup)
    for label in ("등록일", "작성일", "게시일", "등록일자", "작성일자"):
        value, _ = _value_after_label(table, label)
        if not value:
            continue
        m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", value)
        if not m:
            continue
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            continue
        if dt.date() > datetime.now().date():
            return None
        return dt
    # Do not use event/festival period as content_created_at.
    return None


def _extract_gm_table_label_date(table: Any, labels: tuple[str, ...]) -> datetime | None:
    for label in labels:
        value, _ = _value_after_label(table, label)
        dt = _parse_ymd_date(value)
        if dt:
            return dt
    text = _clean_text(table)
    return _date_after_text_label(text, *labels)


def extract_gm_reg_date(soup: Any, url: str | None = None) -> datetime | None:
    if soup is None or (url and not is_gm_url(url)):
        return None
    if is_gm_invalid_request_page(soup=soup, url=url):
        return None

    if is_gm_festival_detail_url(url):
        dt = extract_gm_festival_reg_date(soup, url=url)
        if dt:
            return dt
    if is_gm_lobas_tcm_detail_url(url):
        dt = extract_gm_lobas_tcm_reg_date(soup, url=url)
        if dt:
            return dt
    if is_gm_contract_detail_url(url):
        dt = extract_gm_contract_reg_date_from_soup(soup, url=url)
        if dt:
            return dt

    labels = (
        "등록일",
        "작성일",
        "게시일",
        "등록일자",
        "작성일자",
        "게시일자",
    )
    if is_gm_general_bbs_url(url) or is_gm_nftc_bbs_url(url):
        table = _select_gm_general_bbs_table(soup)
        dt = _extract_gm_table_label_date(table, labels)
        if dt:
            return dt
        box = _select_gm_bbs_viewbox(soup)
        dt = _date_after_text_label(_clean_text(box), *labels)
        if dt:
            return dt

    try:
        tables = list(soup.select("table"))
    except Exception:
        tables = []
    for table in tables:
        dt = _extract_gm_table_label_date(table, labels)
        if dt:
            return dt

    if url and "q_bbscttSn=" in str(url):
        try:
            qs = parse_qs(urlparse(str(url)).query)
            raw_sn = str((qs.get("q_bbscttSn") or [""])[0] or "").strip()
            if re.fullmatch(r"\d{14,}", raw_sn):
                return datetime.strptime(raw_sn[:14], "%Y%m%d%H%M%S")
            if re.fullmatch(r"\d{8,}", raw_sn):
                return datetime.strptime(raw_sn[:8], "%Y%m%d")
        except Exception:
            return None
    return None


def _select_gm_group_info_table(soup: Any) -> Any | None:
    if soup is None:
        return None
    try:
        tables = list(soup.select("table.table_style2")) or list(soup.select("table"))
    except Exception:
        tables = []
    for table in tables:
        summary = str(table.get("summary") or "")
        caption = ""
        try:
            cap = table.find("caption")
            caption = _clean_text(cap) if cap else ""
        except Exception:
            caption = ""
        headers = [_norm_label(th.get_text(" ", strip=True)) for th in table.select("th")]
        if (
            "조직안내" in summary
            or caption == "부서명"
            or {"부서명", "팀명", "직위", "전화번호", "담당업무"}.issubset(set(headers))
        ):
            return table
    return None


def extract_gm_group_info_title(soup: Any) -> str:
    if soup is None:
        return ""
    try:
        content = soup.select_one("#c-contents") or soup.select_one(".sub_contents") or soup
    except Exception:
        content = soup
    for h in content.find_all(["h1", "h2", "h3", "h4"]):
        text = _clean_text(h)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if text.startswith("부서명"):
            suffix = text.split(":", 1)[1].strip() if ":" in text else ""
            return suffix or "조직안내 및 전화번호"
        if text not in {"시청안내", "조직안내 및 전화번호"}:
            return text
    return "조직안내 및 전화번호"


def _strip_gm_contract_noise(root: Any) -> None:
    for sel in (
        "script",
        "style",
        "noscript",
        "iframe",
        "form",
        "button",
        "input",
        "select",
        "textarea",
        "img",
        ".btn",
        ".button",
        ".btn_area",
        ".btn_box",
    ):
        try:
            nodes = list(root.select(sel))
        except Exception:
            nodes = []
        for el in nodes:
            try:
                el.decompose()
            except Exception:
                pass


def _gm_contract_table_lines(table: Any) -> list[str]:
    lines: list[str] = []
    if table is None:
        return lines
    try:
        rows = list(table.select("tr"))
    except Exception:
        rows = []
    for tr in rows:
        try:
            cells = tr.find_all(["th", "td"], recursive=False)
        except Exception:
            cells = []
        pending_label = ""
        row_values: list[str] = []
        for cell in cells:
            name = str(getattr(cell, "name", "") or "").lower()
            text = _clean_text(cell)
            if not text:
                continue
            if name == "th":
                pending_label = text
                continue
            if pending_label:
                row_values.append(f"{pending_label}: {text}")
                pending_label = ""
            else:
                row_values.append(text)
        if row_values:
            lines.extend(row_values)
    return lines


def _table_label_value_lines(table: Any) -> list[str]:
    lines: list[str] = []
    if table is None:
        return lines
    try:
        rows = list(table.select("tr"))
    except Exception:
        rows = []
    for tr in rows:
        try:
            cells = tr.find_all(["th", "td"], recursive=False)
        except Exception:
            cells = []
        label = ""
        values: list[str] = []
        for cell in cells:
            name = str(getattr(cell, "name", "") or "").lower()
            text = _clean_text(cell)
            if name == "th":
                label = text
                continue
            if name == "td" and text:
                values.append(text)
        value = " ".join(values).strip()
        if label and value:
            lines.append(f"{label}: {value}")
        elif label:
            if _norm_label(label) == "행사내용":
                lines.append(f"{label}: 내용 없음")
            else:
                lines.append(f"{label}:")
        elif value:
            lines.append(value)
    return lines


def _gm_group_info_table_lines(table: Any, title: str) -> list[str]:
    lines: list[str] = [f"부서명: {title}"]
    if table is None:
        return lines
    try:
        headers = [_clean_text(th) for th in table.select("thead th")]
    except Exception:
        headers = []
    headers = [h for h in headers if h]
    if headers:
        lines.append("항목: " + ", ".join(headers))

    data_rows: list[str] = []
    try:
        rows = list(table.select("tbody tr"))
    except Exception:
        rows = []
    for tr in rows:
        try:
            values = [_clean_text(td) for td in tr.find_all("td", recursive=False)]
        except Exception:
            values = []
        values = [v for v in values if v]
        if not values:
            continue
        row_parts: list[str] = []
        for idx, value in enumerate(values):
            label = headers[idx] if idx < len(headers) else ""
            row_parts.append(f"{label}: {value}" if label else value)
        data_rows.append(" / ".join(row_parts))

    if data_rows:
        lines.extend(data_rows)
    else:
        lines.append("직원 목록: 데이터 없음")
    return lines


def _gm_page_title(soup: Any, fallback: str = "") -> str:
    if soup is None:
        return fallback
    for selector in ("h1", "h2", "h3", "title", "meta[name='description']", "meta[name='keywords']"):
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if node is None:
            continue
        if getattr(node, "name", "").lower() == "meta":
            text = str(node.get("content") or "").strip()
        else:
            text = _clean_text(node)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return text
    return fallback


def try_extract_gm_epeople_iframe_post(soup: Any, url: str | None):
    if soup is None or not is_gm_epeople_iframe_page(url):
        return None

    try:
        iframe = soup.select_one("iframe[src*='epeople.go.kr/frm/pttn/openPttnList.npaid']")
    except Exception:
        iframe = None
    if iframe is None:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
    )

    src = str(iframe.get("src") or "").strip()
    if not src:
        return None
    src = urljoin(str(url or ""), src)
    iframe_title = _clean_text(iframe) or str(iframe.get("title") or "").strip()
    title = _gm_page_title(soup, "공개민원")

    content_lines = [
        title,
        "외부 연계 페이지: 국민신문고 공개민원 목록",
    ]
    if iframe_title:
        content_lines.append(f"iframe 제목: {iframe_title}")
    content_lines.append(f"URL: {src}")
    content_text = "\n".join(line for line in content_lines if line).strip()
    content_html = (
        '<div class="gm-epeople-iframe">'
        f"<p>{title}</p>"
        f"<p>외부 연계 페이지: 국민신문고 공개민원 목록</p>"
        f'<p><a href="{src}">{src}</a></p>'
        "</div>"
    )
    return BoardPostExtract(
        url=str(url or ""),
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def try_extract_gm_group_info_post(soup: Any, url: str | None):
    if soup is None or not is_gm_group_info_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _sanitize_html_fragment,
    )

    table = _select_gm_group_info_table(soup)
    if table is None:
        return None

    title = extract_gm_group_info_title(soup)
    try:
        frag = BeautifulSoup(str(copy.copy(table)), "html.parser")
    except Exception:
        return None
    _strip_gm_contract_noise(frag)
    clean_table = frag.find("table") or frag.find(True)
    if clean_table is None:
        return None

    lines = _gm_group_info_table_lines(clean_table, title)
    content_text = "\n".join(line for line in lines if line).strip()
    content_html = _sanitize_html_fragment(clean_table).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_gm_lobas_tcm_post(soup: Any, url: str | None):
    if soup is None or not is_gm_lobas_tcm_detail_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _sanitize_html_fragment,
    )

    table = _select_gm_lobas_tcm_table(soup)
    if table is None:
        return None

    title = extract_gm_lobas_tcm_title(soup)
    if not title:
        return None

    try:
        frag = BeautifulSoup(str(copy.copy(table)), "html.parser")
    except Exception:
        return None
    _strip_gm_contract_noise(frag)
    clean_table = frag.find("table") or frag.find(True)
    if clean_table is None:
        return None

    lines = _gm_contract_table_lines(clean_table)
    content_text = "\n".join(line for line in lines if line).strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(clean_table).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_gm_festival_post(soup: Any, url: str | None):
    if soup is None or not is_gm_festival_detail_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _sanitize_html_fragment,
    )

    table = _select_gm_festival_table(soup)
    if table is None:
        return None

    title = extract_gm_festival_title(soup)
    if not title:
        return None

    try:
        frag = BeautifulSoup(str(copy.copy(table)), "html.parser")
    except Exception:
        return None
    _strip_gm_contract_noise(frag)
    clean_table = frag.find("table") or frag.find(True)
    if clean_table is None:
        return None

    lines = _table_label_value_lines(clean_table)
    content_text = "\n".join(line for line in lines if line).strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(clean_table).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_gm_contract_post(soup: Any, url: str | None):
    if soup is None or not is_gm_contract_detail_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _sanitize_html_fragment,
    )

    table = _select_gm_contract_table(soup)
    if table is None:
        return None

    title = extract_gm_contract_title(soup)
    if not title:
        return None

    try:
        frag = BeautifulSoup(str(copy.copy(table)), "html.parser")
    except Exception:
        return None
    _strip_gm_contract_noise(frag)
    clean_table = frag.find("table") or frag.find(True)
    if clean_table is None:
        return None

    lines = _gm_contract_table_lines(clean_table)
    content_text = "\n".join(line for line in lines if line).strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(clean_table).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_gm_general_post(soup: Any, url: str | None):
    if soup is None or not is_gm_general_bbs_url(url):
        return None
    return _try_extract_gm_bbs_box_post(soup, url) or _try_extract_gm_bbs_table_post(soup, url)


def try_extract_gm_nftc_post(soup: Any, url: str | None):
    if soup is None or not is_gm_nftc_bbs_url(url):
        return None
    return _try_extract_gm_bbs_box_post(soup, url) or _try_extract_gm_bbs_table_post(soup, url)


_GM_GENERAL_VALUE_POST_SKIP_LABELS = {
    "제목",
    "Subject",
    "subject",
    "SUBJECT",
    "조회수",
    "파일",
    "첨부파일",
    "우편번호",
    "상세주소",
}


def _extract_gm_bbs_label_value_lines(table: Any, title_label: str) -> list[str]:
    if table is None:
        return []

    lines: list[str] = []
    seen: set[str] = set()
    for tr in table.select("tr"):
        try:
            cells = tr.find_all(["th", "td"], recursive=False)
        except Exception:
            cells = []
        idx = 0
        while idx < len(cells):
            cell = cells[idx]
            idx += 1
            if getattr(cell, "name", "").lower() != "th":
                continue
            label = _clean_text(cell)
            if not label:
                continue
            if label in _GM_GENERAL_VALUE_POST_SKIP_LABELS:
                continue

            value_parts: list[str] = []
            while idx < len(cells) and getattr(cells[idx], "name", "").lower() != "th":
                value = _clean_text(cells[idx])
                if value:
                    value_parts.append(value)
                idx += 1
            value = " ".join(value_parts).strip()
            if not value:
                continue
            if label == title_label:
                value = value.strip()
            line = f"{label}: {value}" if label != "내용" else value
            key = re.sub(r"\s+", " ", line).strip().lower()
            if key in seen:
                continue
            seen.add(key)
            lines.append(line)
    return lines


def _try_extract_gm_bbs_box_post(soup: Any, url: str | None):
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    box = _select_gm_bbs_viewbox(soup)
    if box is None:
        return None
    title = _gm_bbs_box_title(soup)
    if not title:
        return None

    body = None
    for sel in (
        ".viewcontentbox",
        ".viewcontent",
        ".contenttext",
        ".content_text",
        ".bbs_content",
        "table.table-view",
        "table.table_view",
    ):
        try:
            body = box.select_one(sel)
        except Exception:
            body = None
        if body is not None and _clean_text(body):
            break
    if body is None:
        return None

    try:
        frag = BeautifulSoup(str(copy.copy(body)), "html.parser")
    except Exception:
        return None

    for sel in (
        "script",
        "style",
        "noscript",
        "button",
        "input",
        ".btn-group",
        ".btn-group2",
        ".filelistbox",
        ".file_list",
    ):
        for el in list(frag.select(sel)):
            try:
                el.decompose()
            except Exception:
                pass

    node = frag.find(True)
    if node is None:
        return None

    content_text = ""
    if getattr(node, "name", "").lower() == "table":
        content_text = "\n".join(_extract_gm_bbs_label_value_lines(node, "")).strip()
    if not content_text:
        content_text = _extract_content_text(node)
        content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
        content_text = (content_text or "").strip()
    if not content_text:
        content_text = _clean_text(node)
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(node).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_gm_bbs_table_post(soup: Any, url: str | None):
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    table = _select_gm_general_bbs_table(soup)
    if table is None:
        return None

    title, title_label = _gm_general_title_from_table(table)
    if not title:
        return None

    _, body_cell = _value_after_label(table, "내용", "본문")
    node = None
    content_text = ""

    if body_cell is not None:
        try:
            frag = BeautifulSoup(str(copy.copy(body_cell)), "html.parser")
        except Exception:
            return None

        for sel in (
            "script",
            "style",
            "noscript",
            ".tbl-imgbox:empty",
            ".btn-baro",
            "button",
            "input",
        ):
            for el in list(frag.select(sel)):
                try:
                    el.decompose()
                except Exception:
                    pass

        node = frag.find(True)
        if node is not None:
            content_text = _extract_content_text(node)
            content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
            content_text = (content_text or "").strip()

    if title_label and title_label != "제목":
        structured_lines = _extract_gm_bbs_label_value_lines(table, title_label)
        if structured_lines:
            content_text = "\n".join(structured_lines).strip()

    if not content_text:
        structured_lines = _extract_gm_bbs_label_value_lines(table, title_label or "")
        if structured_lines:
            content_text = "\n".join(structured_lines).strip()
            node = table

    if not content_text:
        return None

    content_html_root = node if node is not None else table
    content_html = _sanitize_html_fragment(content_html_root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=str(url or ""),
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )
