"""
Dongjak-gu office (dongjak.go.kr) board parsing helpers.

The archive BODY view uses a custom layout where the category, title, intro,
policy sections, and footer controls live in separate wrappers. Keeping these
rules here prevents the generic extractor from mistaking category/navigation
text for the post title or body.
"""

from __future__ import annotations

import copy
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


DONGJAK_ARCHIVE_TITLE_SELECTORS = (
    ".acview-tbox .btit",
    ".acview-top .acview-tbox .btit",
    "#acv-contents .btit",
)

DONGJAK_ARCHIVE_BODY_SELECTORS = (
    ".acview-conts .cview-content",
    ".cview-content",
)

DONGJAK_ARCHIVE_INTRO_SELECTORS = (
    ".acview-tbox .f-txt",
    ".acview-top .acview-tbox .f-txt",
)

DONGJAK_PORTAL_BBS_TABLE_TITLE_SELECTORS = (
    "#contentDiv .view .table p.heading",
    "#contentDiv .view p.heading",
    ".nw-content-data .view .table p.heading",
    ".view .table p.heading",
)

DONGJAK_NOISE_TITLE_TEXTS = {
    "동작구청",
    "동작구청 포털사이트",
    "동작소식",
    "분야별정보",
    "분야별 정보",
    "상세보기",
    "게시글 상세",
    "목록",
    "목록보기",
    "본문",
    "복지",
}


def _collapse_ws(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("\xa0", " ")).strip()


def _url_parts(url: str) -> tuple[str, str, str]:
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host, (parsed.path or "").lower(), (parsed.query or "").lower()
    except Exception:
        low = (url or "").lower()
        return ("dongjak.go.kr" if "dongjak.go.kr" in low else "", low, low)


def is_dongjak_url(url: str) -> bool:
    host, _path, _query = _url_parts(url or "")
    return host == "dongjak.go.kr" or host.endswith(".dongjak.go.kr")


def is_dongjak_archive_body_view_url(url: str) -> bool:
    host, path, query = _url_parts(url or "")
    if host != "dongjak.go.kr" and not host.endswith(".dongjak.go.kr"):
        return False
    return (
        path.startswith("/portal/bbs/b00014")
        and path.endswith("/view.do")
        and "viewtype=body" in query
    )


def is_dongjak_figure_view_url(url: str) -> bool:
    host, path, _query = _url_parts(url or "")
    if host != "dongjak.go.kr" and not host.endswith(".dongjak.go.kr"):
        return False
    return path == "/portal/bbs/b0000621/view.do"


def is_dongjak_portal_bbs_view_url(url: str) -> bool:
    host, path, _query = _url_parts(url or "")
    if host != "dongjak.go.kr" and not host.endswith(".dongjak.go.kr"):
        return False
    return "/portal/bbs/" in path and path.endswith("/view.do")


def is_dongjak_prvstl_check_form_url(url: str) -> bool:
    host, path, _query = _url_parts(url or "")
    if host != "dongjak.go.kr" and not host.endswith(".dongjak.go.kr"):
        return False
    return path in {
        "/portal/singl/prvstl/checkappncmpnyform.do",
        "/healthcare/singl/prvstl/checkappncmpnyform.do",
    }


def should_skip_dongjak_collection_url(url: str) -> bool:
    if not is_dongjak_prvstl_check_form_url(url or ""):
        return False
    blocked_menu_nos = {"200183", "300173"}
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        query = parse_qs(parsed.query or "", keep_blank_values=True)
    except Exception:
        low = (url or "").lower()
        return any(f"menuno={menu_no}" in low for menu_no in blocked_menu_nos)

    for key, values in query.items():
        if (key or "").lower() != "menuno":
            continue
        if any(str(value or "").strip() in blocked_menu_nos for value in values):
            return True
    return False


def _clean_title_text(value: Any) -> str:
    text = _collapse_ws(value)
    text = text.strip(" |:-")
    if not text:
        return ""
    if re.search(r"<\s*/?\s*[a-zA-Z!][^>]{0,80}>", text):
        return ""
    if text in DONGJAK_NOISE_TITLE_TEXTS:
        return ""
    if text.replace(" ", "") in {x.replace(" ", "") for x in DONGJAK_NOISE_TITLE_TEXTS}:
        return ""
    if len(text) <= 2 and not re.search(r"[가-힣A-Za-z0-9]{3,}", text):
        return ""
    return text


def _extract_dongjak_prvstl_title(soup: Any) -> str:
    if soup is None:
        return ""

    for selector in (
        'meta[name="description"]',
        'meta[property="og:description"]',
        'meta[property="og:title"]',
    ):
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if not node:
            continue
        title = _clean_title_text(node.get("content") or "")
        if title and title not in {"페이지 설명"}:
            return title.split("<", 1)[0].strip()

    try:
        raw_title = soup.title.get_text(" ", strip=True) if soup.title else ""
    except Exception:
        raw_title = ""
    for part in re.split(r"\s*[<|]\s*", raw_title):
        title = _clean_title_text(part)
        if title and title not in {"포털사이트", "행정정보", "계약입찰"}:
            return title

    return ""


def extract_dongjak_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_dongjak_url(url)):
        return ""

    if is_dongjak_prvstl_check_form_url(url or ""):
        return _extract_dongjak_prvstl_title(soup) or "수의계약시담"

    for selector in DONGJAK_PORTAL_BBS_TABLE_TITLE_SELECTORS:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if not node:
            continue
        title = _clean_title_text(node.get_text(" ", strip=True))
        if title:
            return title

    try:
        node = soup.select_one('script#fileName[data-name]')
    except Exception:
        node = None
    if node:
        title = _clean_title_text(node.get("data-name") or "")
        if title:
            return title

    try:
        node = soup.select_one("#contentDiv .view.boxGray h2") or soup.select_one(".view.boxGray h2")
    except Exception:
        node = None
    if node:
        title = _clean_title_text(node.get_text(" ", strip=True))
        if title:
            return title

    for selector in DONGJAK_ARCHIVE_TITLE_SELECTORS:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if not node:
            continue
        title = _clean_title_text(node.get_text(" ", strip=True))
        if title:
            return title
    return ""


def _strip_noise_nodes(node: Any) -> None:
    if node is None:
        return
    for selector in (
        "script",
        "style",
        "noscript",
        "iframe",
        "svg",
        ".dment-conts",
        ".glist-btn",
        ".btnSet",
        "#acv-footer",
        ".top-go",
        "a.b-list",
    ):
        try:
            for tag in node.select(selector):
                tag.decompose()
        except Exception:
            continue


def _content_text(node: Any) -> str:
    if node is None:
        return ""
    try:
        if BeautifulSoup is None:
            return _collapse_ws(node.get_text(" ", strip=True))
        frag = BeautifulSoup(str(node), "html.parser")
        root = frag.find(True) or frag
        _strip_noise_nodes(root)
        for tr in root.find_all("tr"):
            tr.insert_after("\n")
        for cell in root.find_all(["th", "td"]):
            cell.append(" ")
        for tag in root.find_all(["p", "div", "li", "section", "article", "br"]):
            tag.insert_after("\n")
        lines = [_collapse_ws(line) for line in root.get_text("\n", strip=True).splitlines()]
        return "\n".join(line for line in lines if line).strip()
    except Exception:
        try:
            return _collapse_ws(node.get_text(" ", strip=True))
        except Exception:
            return ""


def _strip_leading_title(text: str, title: str) -> str:
    value = (text or "").strip()
    title_norm = _collapse_ws(title)
    if not value or not title_norm:
        return value
    lines = value.splitlines()
    while lines and _collapse_ws(lines[0]) == title_norm:
        lines.pop(0)
    return "\n".join(lines).lstrip()


def _make_content_html(soup: Any, nodes: list[Any]) -> str:
    try:
        doc = soup.__class__('<div class="dongjak-archive-extract-root"></div>', "html.parser")
        wrap = doc.find("div")
        for node in nodes:
            if node is not None:
                wrap.append(copy.copy(node))
        _strip_noise_nodes(wrap)
        return str(wrap or "").strip()
    except Exception:
        return "\n".join(str(node or "") for node in nodes if node is not None).strip()


def _extract_dongjak_prvstl_manager_text(soup: Any) -> str:
    if soup is None:
        return ""
    try:
        page_inf = soup.select_one(".page-inf .manager")
    except Exception:
        page_inf = None
    text = _content_text(page_inf)
    text = _collapse_ws(text)
    if not text:
        return ""
    text = re.sub(r"^자료관리담당\s*", "", text).strip()
    return text


def _extract_dongjak_prvstl_check_form_post(soup: Any, url: str):
    if soup is None or not is_dongjak_prvstl_check_form_url(url):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    title = extract_dongjak_title(soup, url=url) or "수의계약시담"
    lines = ["지정회사명 및 사업자등록번호 확인이 필요한 수의계약 시담 페이지입니다."]
    manager = _extract_dongjak_prvstl_manager_text(soup)
    if manager:
        lines.append(f"자료관리담당: {manager}")
    content_text = "\n".join(lines).strip()

    try:
        form_node = soup.select_one("form#board") or soup.select_one("#contentDiv")
        content_html = str(form_node or "")
    except Exception:
        content_html = ""

    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def _extract_dongjak_figure_post(soup: Any, url: str):
    if soup is None or not is_dongjak_figure_view_url(url):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    try:
        view = soup.select_one("#contentDiv .view.boxGray") or soup.select_one(".view.boxGray")
    except Exception:
        view = None
    if not view:
        return None

    title = ""
    try:
        h2 = view.select_one("h2")
        title = _clean_title_text(h2.get_text(" ", strip=True) if h2 else "")
    except Exception:
        title = ""
    if not title:
        title = extract_dongjak_title(soup, url=url) or "제목 없음"

    parts: list[str] = []
    try:
        for dt in view.select(".desc dl dt"):
            dd = dt.find_next_sibling("dd")
            label = _collapse_ws(dt.get_text(" ", strip=True))
            value = _collapse_ws(dd.get_text(" ", strip=True) if dd else "")
            if label and value:
                parts.append(f"{label}: {value}")
    except Exception:
        pass

    body_node = None
    try:
        content_div = soup.select_one("#contentDiv")
        if content_div:
            candidates = content_div.find_all("p", recursive=False)
            for candidate in candidates:
                text = _content_text(candidate)
                if len(_collapse_ws(text)) >= 20:
                    body_node = candidate
                    break
        if body_node is None:
            sibling = view.find_next_sibling()
            while sibling is not None:
                name = getattr(sibling, "name", None)
                classes = set(getattr(sibling, "get", lambda *_: [])("class") or [])
                if name == "p" and len(_collapse_ws(_content_text(sibling))) >= 20:
                    body_node = sibling
                    break
                if name == "div" and "page-inf" in classes:
                    break
                sibling = getattr(sibling, "next_sibling", None)
    except Exception:
        body_node = None

    body_text = _content_text(body_node)
    if body_text:
        parts.append(body_text)

    content_text = "\n".join(part for part in parts if _collapse_ws(part)).strip()
    if not content_text:
        return None

    html_nodes = [view]
    if body_node is not None:
        html_nodes.append(body_node)

    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=_make_content_html(soup, html_nodes),
        snippet=_collapse_ws(content_text)[:200],
    )


def _extract_dongjak_portal_bbs_boxgray_post(soup: Any, url: str):
    if soup is None or not is_dongjak_portal_bbs_view_url(url):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    try:
        view = soup.select_one("#contentDiv .view.boxGray") or soup.select_one(".view.boxGray")
    except Exception:
        view = None
    if not view:
        return None

    title = extract_dongjak_title(soup, url=url)
    if not title:
        try:
            h2 = view.select_one("h2")
            title = _clean_title_text(h2.get_text(" ", strip=True) if h2 else "")
        except Exception:
            title = ""
    if not title:
        return None

    parts: list[str] = []
    try:
        for dt in view.select(".desc dl dt"):
            dd = dt.find_next_sibling("dd")
            label = _collapse_ws(dt.get_text(" ", strip=True))
            value = _collapse_ws(dd.get_text(" ", strip=True) if dd else "")
            label = re.sub(r"^\s*[^\w가-힣]+", "", label).strip()
            if label and value:
                parts.append(f"{label}: {value}")
    except Exception:
        pass

    try:
        desc = view.select_one(".desc")
    except Exception:
        desc = None
    desc_text = _content_text(desc)
    desc_text = _strip_leading_title(desc_text, title)
    if desc_text:
        existing = "\n".join(parts)
        for line in desc_text.splitlines():
            line_norm = _collapse_ws(line)
            if line_norm and line_norm not in existing:
                parts.append(line_norm)

    try:
        tab1 = soup.select_one("#contentDiv .tab-interface #tab1 .tabCts")
    except Exception:
        tab1 = None
    tab_text = _content_text(tab1)
    if tab_text:
        parts.append(tab_text)

    try:
        for img in view.select("img[alt], img[src]")[:3]:
            alt = _collapse_ws(img.get("alt") or "")
            src = _collapse_ws(img.get("src") or "")
            image_label = alt or src.rsplit("/", 1)[-1]
            if image_label:
                parts.append(f"이미지: {image_label}")
    except Exception:
        pass

    content_text = "\n".join(part for part in parts if _collapse_ws(part)).strip()
    content_text = _strip_leading_title(content_text, title)
    if not content_text:
        return None

    html_nodes = [view]
    if tab1 is not None:
        html_nodes.append(tab1)

    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=_make_content_html(soup, html_nodes),
        snippet=_collapse_ws(content_text)[:200],
    )


def _extract_dongjak_portal_bbs_table_post(soup: Any, url: str):
    if soup is None or not is_dongjak_portal_bbs_view_url(url):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    title = extract_dongjak_title(soup, url=url)
    if not title:
        return None

    try:
        view = (
            soup.select_one("#contentDiv .view")
            or soup.select_one(".nw-content-data .view")
            or soup.select_one(".view")
        )
    except Exception:
        view = None
    if not view:
        return None

    try:
        table_wrap = view.select_one(".table")
        table = table_wrap.select_one("table") if table_wrap else view.select_one("table")
    except Exception:
        table_wrap = None
        table = None
    if not table:
        return None

    try:
        table_copy = copy.copy(table)
        for caption in table_copy.select("caption"):
            caption.decompose()
        table_text = _content_text(table_copy)
    except Exception:
        table_text = _content_text(table)
    table_text = _strip_leading_title(table_text, title)

    parts: list[str] = []
    if table_text:
        parts.append(table_text)

    try:
        sibling = table_wrap.find_next_sibling() if table_wrap else table.find_next_sibling()
        active_heading = ""
        while sibling is not None:
            name = getattr(sibling, "name", None)
            classes = set(getattr(sibling, "get", lambda *_: [])("class") or [])
            if name == "div" and "btnSet" in classes:
                break
            if name and str(name).lower() in {"h2", "h3", "h4"}:
                active_heading = _collapse_ws(sibling.get_text(" ", strip=True))
                if active_heading:
                    parts.append(active_heading)
            elif name:
                text = _content_text(sibling)
                if text:
                    parts.append(f"{active_heading}\n{text}" if active_heading else text)
                try:
                    for img in sibling.select("img[alt], img[src]"):
                        alt = _collapse_ws(img.get("alt") or "")
                        src = _collapse_ws(img.get("src") or "")
                        image_label = alt or src.rsplit("/", 1)[-1]
                        if image_label:
                            prefix = f"{active_heading}: " if active_heading else "이미지: "
                            parts.append(f"{prefix}{image_label}")
                except Exception:
                    pass
            sibling = getattr(sibling, "next_sibling", None)
    except Exception:
        pass

    content_text = "\n".join(part for part in parts if _collapse_ws(part)).strip()
    content_text = _strip_leading_title(content_text, title)
    if not content_text:
        return None

    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=_make_content_html(soup, [view]),
        snippet=_collapse_ws(content_text)[:200],
    )


def try_extract_dongjak_post(soup: Any, url: str):
    prvstl_post = _extract_dongjak_prvstl_check_form_post(soup, url)
    if prvstl_post:
        return prvstl_post

    figure_post = _extract_dongjak_figure_post(soup, url)
    if figure_post:
        return figure_post

    boxgray_post = _extract_dongjak_portal_bbs_boxgray_post(soup, url)
    if boxgray_post:
        return boxgray_post

    table_post = _extract_dongjak_portal_bbs_table_post(soup, url)
    if table_post:
        return table_post

    if soup is None or not is_dongjak_archive_body_view_url(url):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    title = extract_dongjak_title(soup, url=url) or "제목 없음"
    parts: list[str] = []
    html_nodes: list[Any] = []

    for selector in DONGJAK_ARCHIVE_INTRO_SELECTORS:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        text = _content_text(node)
        if text:
            parts.append(text)
            html_nodes.append(node)
            break

    body_node = None
    for selector in DONGJAK_ARCHIVE_BODY_SELECTORS:
        try:
            body_node = soup.select_one(selector)
        except Exception:
            body_node = None
        if body_node:
            break

    body_text = _content_text(body_node)
    if body_text:
        parts.append(body_text)
        html_nodes.append(body_node)

    content_text = "\n\n".join(part for part in parts if part.strip()).strip()
    content_text = _strip_leading_title(content_text, title)
    if not content_text:
        return None

    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=_make_content_html(soup, html_nodes),
        snippet=_collapse_ws(content_text)[:200],
    )
