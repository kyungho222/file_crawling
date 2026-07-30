from __future__ import annotations

import re
from typing import Any, Iterable, Optional
from urllib.parse import urljoin, urlparse


ASIMC_TITLE_LABELS = (
    "공고명",
    "제목",
    "공고제목",
    "채용명",
    "채용제목",
)

ASIMC_BODY_LABELS = (
    "내용",
    "상세내용",
    "공고내용",
    "채용내용",
    "모집내용",
    "공지내용",
)

ASIMC_CONTENT_SELECTORS = (
    "#conts .bod_view .view_cont",
    "#conts .view_cont",
    "#conts .tbl-box .bd_detail",
    "#conts .tbl-box",
    "#conts .bd_detail",
    "#conts",
    ".tbl-box .bd_detail",
    "table.bd_detail",
)


def _collapse_ws(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _norm_label(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def _host(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return (url or "").lower()


def is_asimc_url(url: str) -> bool:
    return _host(url) in {"asimc.han.kr", "asimc.or.kr", "www.asimc.or.kr"}


def is_asimc_list_url(url: str) -> bool:
    raw = str(url or "").lower()
    if not raw or not is_asimc_url(raw):
        return False
    return "list.do" in raw and "view.do" not in raw


def _find_scope(soup: Any):
    if soup is None:
        return None
    return soup.select_one("#conts") or soup.select_one("#contents") or soup


def _clean_menu_text(text: Any) -> str:
    value = _collapse_ws(text)
    if not value:
        return ""
    parts = [p.strip() for p in re.split(r"\s*[>›»/|]\s*", value) if p and p.strip()]
    if parts:
        value = parts[-1]
    lowered = value.lower()
    if lowered in {"home", "homepage"}:
        return ""
    if value in {"대표홈", "홈"}:
        return ""
    return value


def extract_asimc_menu_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_asimc_url(url)):
        return ""

    for selector in (
        "#titWrap .spotWrap li:last-child",
        "#titWrap .spotWrap a:last-child",
        "#titWrap .spotWrap span:last-child",
        "section#content > header#titWrap .spotWrap li:last-child",
        "section#content > header#titWrap .spotWrap a:last-child",
        "#titWrap h3",
        "#titWrap h2",
        "section#content > header#titWrap h3",
        "section#content > header#titWrap h2",
    ):
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if node is None:
            continue
        text = _clean_menu_text(node.get_text(" ", strip=True))
        if text:
            return text
    return ""


def _find_label_value(root: Any, labels: Iterable[str]) -> str:
    if root is None:
        return ""
    wanted = {_norm_label(label) for label in labels if label}
    if not wanted:
        return ""

    for row in root.select("tr, dl, li"):
        try:
            cells = row.find_all(["th", "td", "dt", "dd", "strong", "span"], recursive=False)
        except Exception:
            cells = []
        if len(cells) < 2:
            continue
        for idx, cell in enumerate(cells[:-1]):
            label = _norm_label(cell.get_text(" ", strip=True))
            if label not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                text = _collapse_ws(nxt.get_text(" ", strip=True))
                if text:
                    return text
    return ""


def extract_asimc_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_asimc_url(url)):
        return ""

    if is_asimc_list_url(url):
        menu_title = extract_asimc_menu_title(soup, url=url)
        if menu_title:
            return menu_title

    scope = _find_scope(soup)
    for selector in (
        "#conts .bod_view > .subject",
        "#conts .subject",
        ".bod_view > .subject",
        ".subject",
    ):
        try:
            node = scope.select_one(selector) if scope is not None else soup.select_one(selector)
        except Exception:
            node = None
        if node is None:
            continue
        text = _collapse_ws(node.get_text(" ", strip=True))
        if text:
            return text

    title = _find_label_value(scope, ASIMC_TITLE_LABELS)
    if title:
        return title

    for selector in (
        "#conts table.bd_detail tr th[scope='row'] + td",
        "#conts .bd_detail tr td.tal",
        "#conts .tbl-box td.tal",
        "table.bd_detail tr th + td",
    ):
        try:
            nodes = scope.select(selector) if scope is not None else soup.select(selector)
        except Exception:
            nodes = []
        for node in nodes:
            text = _collapse_ws(node.get_text(" ", strip=True))
            if text:
                return text

    menu_title = extract_asimc_menu_title(soup, url=url)
    if menu_title:
        return menu_title

    return ""


def extract_asimc_body_node(soup: Any, *, url: str = ""):
    if soup is None or (url and not is_asimc_url(url)):
        return None

    scope = _find_scope(soup)
    if scope is None:
        return None

    wanted = {_norm_label(label) for label in ASIMC_BODY_LABELS}
    for row in scope.select("tr"):
        try:
            headers = row.find_all("th", recursive=False)
            cells = row.find_all("td", recursive=False)
        except Exception:
            headers = []
            cells = []
        if not headers or not cells:
            continue
        for th in headers:
            if _norm_label(th.get_text(" ", strip=True)) in wanted:
                for td in cells:
                    if _collapse_ws(td.get_text(" ", strip=True)):
                        return td

    for selector in ASIMC_CONTENT_SELECTORS:
        try:
            node = scope.select_one(selector) if scope is not None else soup.select_one(selector)
        except Exception:
            node = None
        if node is not None:
            return node
    return scope


def asimc_content_selector_hint(url: str) -> str:
    if not is_asimc_url(url):
        return ""
    return ", ".join(ASIMC_CONTENT_SELECTORS)


def extract_asimc_image_lines(root: Any, *, url: str = "", limit: int = 20) -> list[str]:
    if root is None:
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        images = list(root.select("img[src], img[data-attach-file]"))
    except Exception:
        images = []
    for img in images:
        try:
            candidates: list[str] = []
            src = str(img.get("src") or "").strip()
            if src:
                candidates.append(src)
            data_attach = str(img.get("data-attach-file") or "").strip()
            if data_attach:
                candidates.extend(re.findall(r"""['"]([^'"]+\.(?:jpg|jpeg|png|gif|bmp|webp))['"]""", data_attach, flags=re.IGNORECASE))
            full_src = ""
            for cand in candidates:
                cand = cand.strip()
                if not cand:
                    continue
                full_src = urljoin(url or "https://www.asimc.or.kr/", cand)
                break
            if not full_src or full_src in seen:
                continue
            seen.add(full_src)
            alt = _collapse_ws(img.get("alt") or img.get("title") or "")
            out.append(f"본문 이미지: {alt} ({full_src})" if alt else f"본문 이미지: {full_src}")
            if len(out) >= limit:
                break
        except Exception:
            pass
    return out


def strip_asimc_noise(soup: Any, *, url: str = "") -> None:
    if soup is None or (url and not is_asimc_url(url)):
        return
    selectors = (
        "#conts .board_btn",
        "#conts .btn_wrap",
        "#conts .btn-wrap",
        "#conts .pagination",
        "#conts .page",
        "#conts .search",
    )
    for selector in selectors:
        try:
            for tag in soup.select(selector):
                try:
                    tag.decompose()
                except Exception:
                    pass
        except Exception:
            pass


def try_extract_asimc_post(soup: Any, url: str):
    if soup is None or not is_asimc_url(url):
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws as _shared_collapse_ws,
        _extract_content_text,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    strip_asimc_noise(soup, url=url)
    title = (extract_asimc_title(soup, url=url) or "").strip()
    body = extract_asimc_body_node(soup, url=url)
    if body is None:
        return None

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if root is None:
        return None

    content_text = _trim_leading_skip_and_breadcrumb_text(_extract_content_text(root))
    content_text = (content_text or "").strip()
    image_lines = extract_asimc_image_lines(root, url=url)
    if not content_text:
        content_text = "\n".join(image_lines).strip()
    elif image_lines:
        existing = {_shared_collapse_ws(line) for line in content_text.splitlines()}
        for line in image_lines:
            if _shared_collapse_ws(line) not in existing:
                content_text = f"{content_text}\n{line}".strip()
                existing.add(_shared_collapse_ws(line))

    if not content_text:
        return None

    if title:
        title_norm = re.escape(_shared_collapse_ws(title))
        content_text = re.sub(rf"^{title_norm}\s*", "", content_text, count=1, flags=re.IGNORECASE).strip()

    content_html = _sanitize_html_fragment(root).strip()
    snippet = _shared_collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title or "제목 없음",
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )
