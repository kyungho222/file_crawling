"""
용인시청 일반 시민게시판(citizen/user/bbs/BD_selectBbs.do) 전용 파서.

- 제목: `.platform-board__detail` 내부 `제목` 라벨 값
- 본문: `제안내용`/`내용`/`상세내용` 등 실제 내용 라벨 값
- 통합예약(resve.yongin.go.kr): `p.main-title`, `.article-area .txt`
"""

from __future__ import annotations

import copy
import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse


def is_yongin_citizen_bbs_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "yongin.go.kr" in u and "/citizen/user/bbs/bd_selectbbs.do" in u


def is_yongin_resve_bbs_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "resve.yongin.go.kr" in u and "/user/bbs/bd_selectbbs.do" in u


def is_yongin_general_bbs_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "yongin.go.kr" in u and "/user/bbs/bd_selectbbs.do" in u and "/citizen/user/" not in u


def is_yongin_bbs_url(url: str) -> bool:
    if not url:
        return False
    return is_yongin_citizen_bbs_url(url) or is_yongin_general_bbs_url(url) or is_yongin_resve_bbs_url(url)


def resolve_yongin_file_download_url(raw: Any, base_url: str | None = None) -> str:
    """
    용인시청 첨부 URL 보강.

    대표 게시판은 직접 다운로드 링크가
    /component/file/ND_fileDownload.do?q_fileSn=...&q_fileId=...
    형태지만, 일부 마크업에는 preview.jsp?sn=...&id=... 만 노출된다.
    preview 파라미터에서 실제 다운로드 URL을 복원한다.
    """
    try:
        text = str(raw or "").strip()
    except Exception:
        text = ""
    if not text:
        return ""

    candidates = [text]
    try:
        candidates.extend(re.findall(r"['\"]([^'\"]{1,800})['\"]", text))
    except Exception:
        pass

    for cand in candidates:
        value = str(cand or "").strip()
        if not value:
            continue
        try:
            full = urljoin(base_url or "", value)
        except Exception:
            full = value
        low = full.lower()
        if "yongin.go.kr" not in low and "preview.jsp" not in low and "nd_filedownload.do" not in low:
            continue
        parsed = urlparse(full)
        path = (parsed.path or "").lower()
        qs = parse_qs(parsed.query or "")
        if "nd_filedownload.do" in path:
            file_sn = (qs.get("q_fileSn") or qs.get("q_filesn") or [""])[0]
            file_id = (qs.get("q_fileId") or qs.get("q_fileid") or [""])[0]
        elif "preview.jsp" in path:
            file_sn = (qs.get("sn") or qs.get("q_fileSn") or qs.get("q_filesn") or [""])[0]
            file_id = (qs.get("id") or qs.get("q_fileId") or qs.get("q_fileid") or [""])[0]
        else:
            continue
        file_sn = str(file_sn or "").strip()
        file_id = str(file_id or "").strip()
        if not file_sn or not file_id:
            continue
        query = urlencode({"q_fileSn": file_sn, "q_fileId": file_id})
        return urljoin(base_url or full, f"/component/file/ND_fileDownload.do?{query}")
    return ""


def is_yongin_empmntestinfo_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "yongin.go.kr" in u and "/empmntestinfo/bd_selectempmntestinfo.do" in u


def is_yongin_qestnar_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "yongin.go.kr" in u and "/citizen/qestnar/bd_selectqestnar.do" in u


def _norm_label(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"\s+", "", t)
    return t


def _extract_platform_board_pairs(root: Any) -> list[tuple[str, str, Any]]:
    pairs: list[tuple[str, str, Any]] = []
    if root is None:
        return pairs
    for li in root.select("li"):
        try:
            label_el = li.select_one(".platform-board__list-tit")
            value_el = li.select_one(".platform-board__list-txt")
        except Exception:
            label_el = None
            value_el = None
        if not label_el or not value_el:
            continue
        label = _norm_label(label_el.get_text(" ", strip=True))
        value = " ".join((value_el.get_text(" ", strip=True) or "").split()).strip()
        pairs.append((label, value, li))
    return pairs


def _extract_yongin_general_table_value(soup: Any, *labels: str) -> str:
    if soup is None or not labels:
        return ""

    wanted = {_norm_label(label) for label in labels if label}
    if not wanted:
        return ""

    root = soup.select_one("#contents") or soup

    for tr in root.select("tr"):
        try:
            cells = tr.find_all(["th", "td", "dt", "dd"])
        except Exception:
            cells = []
        if len(cells) < 2:
            continue
        for idx, cell in enumerate(cells[:-1]):
            try:
                label_norm = _norm_label(cell.get_text(" ", strip=True))
            except Exception:
                label_norm = ""
            if label_norm not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                try:
                    value = " ".join((nxt.get_text(" ", strip=True) or "").split()).strip()
                except Exception:
                    value = ""
                if value:
                    return value

    inline_labels = tuple(label for label in labels if label)
    for el in root.select("li, p, div, span, td, dd"):
        try:
            txt = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            txt = ""
        if not txt:
            continue
        for label in inline_labels:
            m = re.search(rf"{re.escape(label)}\s*[:|]?\s*(.+)$", txt)
            if not m:
                continue
            cand = " ".join((m.group(1) or "").split()).strip()
            if cand and cand != label:
                return cand

    return ""


def _extract_yongin_contents_table_title(soup: Any) -> str:
    if soup is None:
        return ""
    try:
        table = soup.select_one("#contentsTable")
    except Exception:
        table = None
    if table is None:
        return ""

    def _text(selector: str) -> str:
        try:
            el = table.select_one(selector)
        except Exception:
            el = None
        if el is None:
            return ""
        return " ".join((el.get_text(" ", strip=True) or "").split()).strip()

    caption = _text("caption")
    detail_title = _text("td.title")
    return detail_title or caption


def _extract_yongin_view_bbs_top_title(soup: Any) -> str:
    if soup is None:
        return ""
    try:
        top = soup.select_one(".view_bbs_top")
    except Exception:
        top = None
    if top is None:
        return ""

    def _text(selector: str) -> str:
        try:
            el = top.select_one(selector)
        except Exception:
            el = None
        if el is None:
            return ""
        try:
            return " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            return ""

    for sel in (
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        ".title",
        ".tit",
        ".subject",
        ".bbs_title",
        ".bbs-title",
        "[class*='title']",
        "[class*='subject']",
    ):
        txt = _text(sel)
        if txt:
            return txt
    return ""


def _extract_yongin_apartment_info_title(soup: Any) -> str:
    if soup is None:
        return ""
    for sel in (
        "#contents ul.gy_area li.gy_txt > strong",
        "#contents ul.gv_area li.gv_txt > strong",
        "#contents .gy_area .gy_txt strong",
        "#contents .gv_area .gv_txt strong",
        ".gy_area .gy_txt strong",
        ".gv_area .gv_txt strong",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        text = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        if text:
            return text
    return ""


def _extract_yongin_care_info_lines(root: Any) -> list[str]:
    if root is None:
        return []
    lines: list[str] = []

    def _clean_text(node: Any) -> str:
        try:
            return " ".join((node.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            return ""

    info = None
    for sel in (".gv_area .gv_txt", ".gy_area .gy_txt"):
        try:
            info = root.select_one(sel)
        except Exception:
            info = None
        if info is not None:
            break

    if info is not None:
        title_el = info.select_one("strong")
        title = _clean_text(title_el)
        if title:
            lines.append(title)

        for item in info.select(".txt_list > li"):
            try:
                label_el = item.select_one("b")
                label = _clean_text(label_el)
                if label_el is not None:
                    label_el.extract()

                for noisy in item.select("a.btn, a.button, a.homepage, button, input, img"):
                    try:
                        noisy.extract()
                    except Exception:
                        pass

                value = _clean_text(item)
                if label and value:
                    lines.append(f"{label}: {value}")
                elif value:
                    lines.append(value)
            except Exception:
                continue

    body = None
    try:
        body = root.select_one(".s_content_p")
    except Exception:
        body = None
    if body is not None:
        for noisy in body.select("script, style, noscript, #hwpEditorBoardContent, .btn, .button, button, input, img"):
            try:
                noisy.extract()
            except Exception:
                pass
        for block in body.find_all(["p", "li", "div"]):
            text = _clean_text(block)
            if text and text != "\xa0":
                lines.append(text)

    out: list[str] = []
    seen: set[str] = set()
    for line in lines:
        clean = re.sub(r"\s+", " ", str(line or "")).strip()
        if not clean or clean in seen:
            continue
        if clean in {"바로가기", "목록", "수정", "삭제", "프린트하기", "메뉴열기", "메뉴 닫기"}:
            continue
        seen.add(clean)
        out.append(clean)
    return out


_YONGIN_PHOTO_COPYRIGHT_NOTICE_TOKENS = (
    "저작권이 있는 사진",
    "저작권이있는사진",
    "캡처나 무단사용",
    "이사진은 저작권이 있는 사진이므로 무단사용을 금합니다",
    "행정과 기록물관리팀",
)


def _is_yongin_photo_copyright_notice(text: str) -> bool:
    compact = re.sub(r"\s+", "", str(text or ""))
    if not compact:
        return False
    if any(re.sub(r"\s+", "", token) in compact for token in _YONGIN_PHOTO_COPYRIGHT_NOTICE_TOKENS):
        return True
    return "행정과" in compact and re.search(r"(?:031|0\d{1,2})-?\d{3,4}-?\d{4}", compact) is not None


def _remove_yongin_photo_copyright_notice_text(text: str) -> str:
    if not text:
        return ""
    kept: list[str] = []
    for line in str(text).splitlines():
        if _is_yongin_photo_copyright_notice(line):
            continue
        kept.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(kept)).strip()


def _strip_yongin_photo_copyright_notice(root: Any) -> None:
    if root is None:
        return
    try:
        text_nodes = list(root.find_all(string=True))
    except Exception:
        return

    for text_node in text_nodes:
        raw = str(text_node or "")
        if not _is_yongin_photo_copyright_notice(raw):
            continue
        parent = getattr(text_node, "parent", None)
        if parent is not None:
            try:
                parent_text = parent.get_text(" ", strip=True)
            except Exception:
                parent_text = ""
            try:
                parent_name = str(getattr(parent, "name", "") or "").lower()
                if parent_name not in {"html", "body"} and parent_text and len(parent_text) <= 300:
                    parent.decompose()
                    continue
            except Exception:
                pass
        try:
            text_node.replace_with(_remove_yongin_photo_copyright_notice_text(raw))
        except Exception:
            pass


def _normalize_yongin_malformed_bracket_markers(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\[\s*;\s*([^\]\n\r]{1,40})\]\s*;", r"[\1]", str(text))


def _normalize_yongin_body_text(text: str) -> str:
    if not text:
        return ""
    text = str(text).replace("\xa0", " ")
    text = _normalize_yongin_malformed_bracket_markers(text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_yongin_body_html_markers(root: Any) -> None:
    if root is None:
        return
    try:
        text_nodes = list(root.find_all(string=True))
    except Exception:
        return
    for text_node in text_nodes:
        raw = str(text_node or "")
        normalized = _normalize_yongin_malformed_bracket_markers(raw)
        if normalized == raw:
            continue
        try:
            text_node.replace_with(normalized)
        except Exception:
            pass


def _extract_yongin_legacy_bbs_body_node(soup: Any) -> Any:
    if soup is None:
        return None
    try:
        has_meta_table = soup.select_one("#contentsTable") is not None
    except Exception:
        has_meta_table = False
    if not has_meta_table:
        return None

    for sel in (
        "#contents .t_view table td.tview_desc",
        "#contents td.tview_desc",
        ".t_view table td.tview_desc",
        "td.tview_desc",
    ):
        try:
            body = soup.select_one(sel)
        except Exception:
            body = None
        if body is None:
            continue
        try:
            if " ".join((body.get_text(" ", strip=True) or "").split()).strip():
                return body
        except Exception:
            return body
    return None


def _extract_yongin_contents_table_lines(soup: Any) -> list[str]:
    if soup is None:
        return []
    try:
        table = soup.select_one("#contentsTable")
    except Exception:
        table = None
    if table is None:
        return []

    lines: list[str] = []
    seen: set[str] = set()

    def _clean(node: Any) -> str:
        try:
            return " ".join((node.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            return ""

    try:
        caption = _clean(table.select_one("caption"))
    except Exception:
        caption = ""
    if caption:
        title = _extract_yongin_contents_table_title(soup)
        line = f"분류: {caption}"
        if caption != title and line not in seen:
            seen.add(line)
            lines.append(line)

    for tr in table.select("tr"):
        try:
            tr_style = str(tr.get("style") or "").lower()
        except Exception:
            tr_style = ""
        if "display:none" in tr_style.replace(" ", ""):
            continue

        try:
            cells = tr.find_all(["th", "td"], recursive=False)
        except Exception:
            cells = []
        if len(cells) < 2:
            continue

        idx = 0
        while idx < len(cells) - 1:
            th = cells[idx]
            td = cells[idx + 1]
            try:
                th_name = str(getattr(th, "name", "") or "").lower()
            except Exception:
                th_name = ""
            if th_name != "th":
                idx += 1
                continue

            label = _clean(th)
            value = _clean(td)
            if label and value:
                line = f"{label}: {value}"
                if line not in seen:
                    seen.add(line)
                    lines.append(line)
            idx += 2

    return lines


def extract_yongin_citizen_title(soup: Any) -> str:
    if soup is None:
        return ""
    root = soup.select_one(".platform-board__detail")
    if not root:
        return ""
    for label, value, _li in _extract_platform_board_pairs(root):
        if label == "제목" and value:
            return value
    return ""


def extract_yongin_resve_title(soup: Any) -> str:
    if soup is None:
        return ""
    for sel in (
        "#container .article-header .main-title",
        ".article-header .main-title",
        "p.main-title",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        t = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        if t:
            return t
    return ""


def extract_yongin_general_title(soup: Any) -> str:
    if soup is None:
        return ""
    contents_table_title = _extract_yongin_contents_table_title(soup)
    if contents_table_title:
        return contents_table_title
    view_bbs_top_title = _extract_yongin_view_bbs_top_title(soup)
    if view_bbs_top_title:
        return view_bbs_top_title
    for sel in (
        ".view_bbs_top h4",
        "#container article#content .article-header h1.article-subject",
        "#container article#content .article-header h1.article_subject",
        "#container article#content .article-header h4.article-subject",
        "#container article#content .article-header h4.article_subject",
        "article#content .article-header h1.article-subject",
        "article#content .article-header h1.article_subject",
        "article#content .article-header h4.article-subject",
        "article#content .article-header h4.article_subject",
        ".article-view .article-header h1.article-subject",
        ".article-view .article-header h1.article_subject",
        ".article-view .article-header h4.article-subject",
        ".article-view .article-header h4.article_subject",
        ".article-header h1.article-subject",
        ".article-header h1.article_subject",
        ".article-header h4.article-subject",
        ".article-header h4.article_subject",
        "h1.article-subject",
        "h1.article_subject",
        "h4.article-subject",
        "h4.article_subject",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        t = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        if t:
            return t

    title_from_meta = _extract_yongin_general_table_value(soup, "제목", "게시물명", "게시글명")
    if title_from_meta:
        return title_from_meta

    apartment_info_title = _extract_yongin_apartment_info_title(soup)
    if apartment_info_title:
        return apartment_info_title

    for sel in (
        ".article-header .main-title",
        ".article-header h1",
        ".article-header h2",
        ".article-header h3",
        "#contents .h3_box h3",
        "#contents .h3-box h3",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        t = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        if not t:
            continue
        if t in {"교육·행사", "교육행사", "새소식", "공지사항", "시정소식"}:
            continue
        if sel.startswith("#contents .h3") and len(t) < 8 and not re.search(r"[\d\[\]\(\)<>]", t):
            continue
        if t:
            return t

    return ""


def extract_yongin_empmntestinfo_title(soup: Any) -> str:
    if soup is None:
        return ""

    title_from_meta = _extract_yongin_general_table_value(soup, "제목", "게시물명", "게시글명")
    if title_from_meta:
        return title_from_meta

    for sel in (
        ".article-header h1.article-subject",
        ".article-header h1.article_subject",
        ".article-header h4.article-subject",
        ".article-header h4.article_subject",
        ".article-header h1",
        ".article-header h2",
        ".board-view h1",
        ".board-view h2",
        "#contents h1",
        "#contents h2",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        try:
            txt = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            txt = ""
        if txt:
            return txt

    return ""


def extract_yongin_qestnar_title(soup: Any) -> str:
    if soup is None:
        return ""
    return _extract_yongin_general_table_value(soup, "제목")


def debug_yongin_general_title_candidates(soup: Any) -> dict[str, str]:
    if soup is None:
        return {}

    def _txt(sel: str) -> str:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            return ""
        try:
            return " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            return ""

    def _input_value(sel: str) -> str:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            return ""
        try:
            return " ".join(((el.get("value") or "")).split()).strip()
        except Exception:
            return ""

    def _contents_table_txt(sel: str) -> str:
        try:
            table = soup.select_one("#contentsTable")
        except Exception:
            table = None
        if table is None:
            return ""
        try:
            el = table.select_one(sel)
        except Exception:
            el = None
        if el is None:
            return ""
        try:
            return " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            return ""

    def _contents_table_title_label_td() -> str:
        try:
            table = soup.select_one("#contentsTable")
        except Exception:
            table = None
        if table is None:
            return ""
        try:
            headers = table.select("th")
        except Exception:
            headers = []
        for th in headers:
            try:
                label = " ".join((th.get_text(" ", strip=True) or "").split()).strip()
            except Exception:
                label = ""
            if label != "제목":
                continue
            try:
                td = th.find_next_sibling("td")
            except Exception:
                td = None
            if td is None:
                return ""
            try:
                return " ".join((td.get_text(" ", strip=True) or "").split()).strip()
            except Exception:
                return ""
        return ""

    try:
        html_title = ""
        if getattr(soup, "title", None) and getattr(soup.title, "string", None):
            html_title = " ".join((soup.title.string or "").split()).strip()
    except Exception:
        html_title = ""

    try:
        og_title = ""
        og = soup.select_one("meta[property='og:title']")
        if og is not None:
            og_title = " ".join(((og.get("content") or "")).split()).strip()
    except Exception:
        og_title = ""

    try:
        has_contents_table = "1" if soup.select_one("#contentsTable") is not None else "0"
    except Exception:
        has_contents_table = "0"

    return {
        "selected": extract_yongin_general_title(soup) or "",
        "q_bbs_code": _input_value("#q_bbsCode, input[name='q_bbsCode']"),
        "q_bbsctt_sn": _input_value("#q_bbscttSn, input[name='q_bbscttSn']"),
        "has_contents_table": has_contents_table,
        "contents_table_caption": _contents_table_txt("caption"),
        "contents_table_td_title": _contents_table_txt("td.title"),
        "contents_table_label_title_td": _contents_table_title_label_td(),
        "meta_title": _extract_yongin_general_table_value(soup, "제목", "게시물명", "게시글명") or "",
        "view_bbs_top": _extract_yongin_view_bbs_top_title(soup) or "",
        "view_bbs_top_h4": _txt(".view_bbs_top h4"),
        "article_header_h1": _txt(".article-header h1.article-subject") or _txt(".article-header h1.article_subject"),
        "article_header_h4": _txt(".article-header h4.article-subject") or _txt(".article-header h4.article_subject"),
        "contents_table": _extract_yongin_contents_table_title(soup) or "",
        "main_title": _txt(".article-header .main-title") or _txt(".main-title"),
        "h3_box": _txt("#contents .h3_box h3") or _txt("#contents .h3-box h3"),
        "html_title": html_title,
        "og_title": og_title,
    }


def extract_yongin_general_category_hint(soup: Any) -> str:
    if soup is None:
        return ""

    def _txt(sel: str) -> str:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            return ""
        try:
            return " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        except Exception:
            return ""

    for sel in (
        "#contents .h3_box h3",
        "#contents .h3-box h3",
        ".sub-visual h2",
        ".location strong",
        ".location .current",
    ):
        txt = _txt(sel)
        if not txt:
            continue
        if len(txt) > 24:
            continue
        if re.search(r"\d", txt):
            continue
        return txt
    return ""


def extract_yongin_general_department(soup: Any) -> str:
    if soup is None:
        return ""
    for sel in (
        ".article-header .article-info li",
        ".article-header ul.article-info li",
        ".article-header .info-area .article-info li",
    ):
        try:
            items = soup.select(sel)
        except Exception:
            items = []
        for idx, el in enumerate(items):
            if idx != 0:
                continue
            txt = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
            if txt:
                return txt
    return _extract_yongin_general_table_value(soup, "부서명", "담당부서", "주관부서", "부서")


def extract_yongin_general_date_text(soup: Any) -> str:
    if soup is None:
        return ""
    for sel in (
        ".view_bbs_top .view_bbs_date",
        ".view_bbs_date",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        txt = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
        if txt:
            return txt
    for sel in (
        ".article-header .article-info li",
        ".article-header ul.article-info li",
        ".article-header .info-area .article-info li",
    ):
        try:
            items = soup.select(sel)
        except Exception:
            items = []
        for idx, el in enumerate(items):
            if idx != 1:
                continue
            txt = " ".join((el.get_text(" ", strip=True) or "").split()).strip()
            if txt:
                return txt
    return _extract_yongin_general_table_value(soup, "등록일자", "등록일시", "등록일")


def try_extract_yongin_citizen_post(soup: Any, url: str):
    if soup is None or not is_yongin_citizen_bbs_url(url):
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

    root = soup.select_one(".platform-board__detail")
    if not root:
        return None

    pairs = _extract_platform_board_pairs(root)
    if not pairs:
        return None

    title = extract_yongin_citizen_title(soup) or "제목 없음"

    content_label_prefixes = (
        "제안내용",
        "내용",
        "상세내용",
        "건의내용",
        "본문",
    )
    optional_label_prefixes = (
        "제안이유",
        "건의이유",
        "추진배경",
        "현황",
    )

    text_parts: list[str] = []
    html_wrap = BeautifulSoup('<div class="yongin-citizen-bbs-extract"></div>', "html.parser")
    wrap = html_wrap.find("div")

    for label, value, li in pairs:
        if not value:
            continue
        if any(label.startswith(prefix) for prefix in content_label_prefixes):
            text_parts.append(f"{label}\n{value}")
            if wrap is not None:
                wrap.append(copy.copy(li))
        elif any(label.startswith(prefix) for prefix in optional_label_prefixes):
            text_parts.append(f"{label}\n{value}")
            if wrap is not None:
                wrap.append(copy.copy(li))

    if not text_parts:
        return None

    content_text = "\n\n".join(part for part in text_parts if part.strip()).strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_yongin_resve_post(soup: Any, url: str):
    if soup is None or not is_yongin_resve_bbs_url(url):
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    article = soup.select_one("#container") or soup
    title = extract_yongin_resve_title(soup) or "제목 없음"
    body = (
        article.select_one(".article-area .txt")
        or article.select_one(".article-content .txt")
        or article.select_one(".article-area")
    )
    if body is None:
        return None

    content_text = _extract_content_text(copy.copy(body))
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = (content_text or "").strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(body).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_yongin_qestnar_post(soup: Any, url: str):
    if soup is None or not is_yongin_qestnar_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    body = soup.select_one(".cont_box .t_write") or soup.select_one(".t_write")
    if body is None:
        return None

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if root is None:
        return None

    title = extract_yongin_qestnar_title(soup) or "제목 없음"
    content_text = _extract_content_text(root)
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = (content_text or "").strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_yongin_general_post(soup: Any, url: str):
    if soup is None or not is_yongin_general_bbs_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    root = soup.select_one("#contents") or soup
    title = extract_yongin_general_title(soup) or "제목 없음"

    care_info_lines = _extract_yongin_care_info_lines(root)
    if care_info_lines:
        html_doc = BeautifulSoup('<div class="yongin-care-info-extract"></div>', "html.parser")
        wrap = html_doc.find("div")
        if wrap is not None:
            for line in care_info_lines:
                p = html_doc.new_tag("p")
                p.string = line
                wrap.append(p)
        content_text = "\n".join(care_info_lines).strip()
        content_text = _normalize_yongin_body_text(content_text)
        content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
        snippet = _collapse_ws(content_text)[:200]
        return BoardPostExtract(
            url=url,
            title=title.strip(),
            content_text=content_text,
            content_html=content_html,
            snippet=snippet,
        )

    legacy_body = _extract_yongin_legacy_bbs_body_node(soup)
    if legacy_body is not None:
        try:
            frag = BeautifulSoup(str(legacy_body), "html.parser")
        except Exception:
            frag = None
        node = frag.find(True) if frag is not None else None
        if node is not None:
            for sel in ("script", "style", "noscript"):
                for el in list(node.select(sel)):
                    try:
                        el.decompose()
                    except Exception:
                        pass
            _strip_yongin_photo_copyright_notice(node)
            _normalize_yongin_body_html_markers(node)
            content_text = _extract_content_text(node)
            content_text = _format_numbered_list_lines(content_text)
            content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
            content_text = _remove_yongin_photo_copyright_notice_text(content_text)
            content_text = _normalize_yongin_body_text(content_text)
            if title and content_text.startswith(title):
                content_text = content_text[len(title) :].lstrip()
            content_text = (content_text or "").strip()
            if content_text:
                content_html = _sanitize_html_fragment(node).strip()
                snippet = _collapse_ws(content_text)[:200]
                return BoardPostExtract(
                    url=url,
                    title=title.strip(),
                    content_text=content_text,
                    content_html=content_html,
                    snippet=snippet,
                )

    table_lines = _extract_yongin_contents_table_lines(soup)
    if table_lines:
        html_doc = BeautifulSoup('<div class="yongin-contents-table-extract"></div>', "html.parser")
        wrap = html_doc.find("div")
        if wrap is not None:
            for line in table_lines:
                p = html_doc.new_tag("p")
                p.string = line
                wrap.append(p)
        content_text = _normalize_yongin_body_text("\n".join(table_lines))
        content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
        snippet = _collapse_ws(content_text)[:200]
        return BoardPostExtract(
            url=url,
            title=title.strip(),
            content_text=content_text,
            content_html=content_html,
            snippet=snippet,
        )

    body = None
    for sel in (
        ".view_bbs_detail",
        "#contents .tview_desc",
        "#contents .t_view .tview_desc",
        "#contents .cont_box",
        "#contents .cont-box",
        "#contents .board-view-contents",
        "#contents .article-body",
        "#contents .article-area .txt",
        "#contents .txt",
        ".tview_desc",
        ".t_view .tview_desc",
        ".view_bbs_detail",
        ".board-view-contents",
        ".article-body",
        ".article-area .txt",
    ):
        try:
            candidate = root.select_one(sel) if root is not soup else soup.select_one(sel)
        except Exception:
            candidate = None
        if not candidate:
            continue
        body = candidate
        break

    if body is None:
        body = root

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None

    for sel in (
        "script",
        "style",
        "noscript",
        ".article-header",
        ".article-info",
        ".bbs_file",
        ".board_file",
        ".file",
        ".btn_wrap",
        ".btn-wrap",
        ".prev_next",
        ".prev-next",
        ".t_prenext",
        ".board-prev-next",
        ".board_bottom",
        ".board-bottom",
        ".overflow.marB50",
    ):
        for el in list(frag.select(sel)):
            try:
                el.decompose()
            except Exception:
                pass

    _strip_yongin_photo_copyright_notice(frag)
    _normalize_yongin_body_html_markers(frag)

    node = frag.find(True)
    if not node:
        return None

    content_text = _extract_content_text(node)
    marker = "게시판의 내용 전달"
    pos = content_text.find(marker)
    if pos >= 0:
        content_text = content_text[pos + len(marker) :].strip()
    content_text = re.sub(r"^\s*게시판의\s*내용\s*전달\s*", "", content_text).strip()
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = _remove_yongin_photo_copyright_notice_text(content_text)
    content_text = _normalize_yongin_body_text(content_text)
    content_text = (content_text or "").strip()
    if not content_text:
        return None

    if title and content_text.startswith(title):
        content_text = content_text[len(title) :].lstrip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(node).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )
