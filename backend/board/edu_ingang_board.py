"""
edu.ingang.go.kr lecture detail extractor.

Targets `highView` / `middleView` lecture pages and builds a structured body
from the actual lecture sections only:
- teacher summary
- course metadata
- book metadata
- intro/description
- curriculum rows

Noise such as login prompts, quiz modals, floating buttons, player checks,
and hidden utility forms is intentionally excluded.
"""

from __future__ import annotations

import re
from html import escape as _html_escape
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


_WS_RE = re.compile(r"\s+")
_LINE_SPLIT_RE = re.compile(r"[\r\n]+")


def is_edu_ingang_lecture_detail_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        path = (parsed.path or "").lower()
        qs = parse_qs(parsed.query or "", keep_blank_values=True)
        if host != "edu.ingang.go.kr":
            return False
        if not any(path.endswith(suffix) for suffix in ("/highview", "/middleview")):
            return False
        return bool(qs.get("lectureCd"))
    except Exception:
        low = (url or "").lower()
        return (
            "edu.ingang.go.kr" in low
            and any(part in low for part in ("/highview", "/middleview"))
            and "lecturecd=" in low
        )


def _collapse_ws(text: str) -> str:
    return _WS_RE.sub(" ", str(text or "")).strip()


def _norm_store_name(text: str) -> str:
    value = _collapse_ws(text)
    if not value:
        return ""
    low = value.lower()
    if "kyobo" in low or "교보문고" in value:
        return "교보문고"
    if "yes24" in low or "yes 24" in low:
        return "YES24"
    if "알라딘" in value:
        return "알라딘"
    value = value.replace("KYOBO", "교보문고")
    value = value.replace("YES 24.COM", "YES24")
    value = value.replace("YES 24", "YES24")
    return value


def _extract_text(node: Any, *, sep: str = " ", drop_selectors: tuple[str, ...] = ()) -> str:
    if node is None:
        return ""
    try:
        frag = BeautifulSoup(str(node), "html.parser")
    except Exception:
        try:
            return _collapse_ws(node.get_text(sep, strip=True))
        except Exception:
            return ""
    for sel in ("script", "style", "noscript", *drop_selectors):
        for tag in list(frag.select(sel)):
            try:
                tag.decompose()
            except Exception:
                pass
    root = frag.find(True) or frag
    try:
        return _collapse_ws(root.get_text(sep, strip=True))
    except Exception:
        return ""


def _extract_multilines(node: Any, *, drop_selectors: tuple[str, ...] = ()) -> list[str]:
    if node is None:
        return []
    try:
        frag = BeautifulSoup(str(node), "html.parser")
    except Exception:
        return []
    for sel in ("script", "style", "noscript", *drop_selectors):
        for tag in list(frag.select(sel)):
            try:
                tag.decompose()
            except Exception:
                pass
    root = frag.find(True) or frag
    try:
        raw_text = root.get_text("\n", strip=True)
    except Exception:
        return []

    out: list[str] = []
    for raw in _LINE_SPLIT_RE.split(raw_text.replace("\xa0", " ")):
        line = _collapse_ws(raw)
        if line:
            out.append(line)
    return out


def _extract_teacher_section(inner: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"status": [], "instructor": "", "title": ""}
    teacher_cnt = inner.select_one(".teacher_intro .teacher_cnt") if inner is not None else None
    if teacher_cnt is None:
        return result

    statuses = []
    for tag in teacher_cnt.select(".label_tag span"):
        txt = _collapse_ws(tag.get_text(" ", strip=True))
        if txt:
            statuses.append(txt)
    result["status"] = statuses

    title = _extract_text(teacher_cnt.select_one(".area_title"), drop_selectors=("button",))
    result["title"] = title

    subject_txt = ""
    teacher_txt = ""
    area_name = teacher_cnt.select_one(".area_name")
    if area_name is not None:
        try:
            for node in getattr(area_name, "contents", []) or []:
                if getattr(node, "name", None) == "strong":
                    teacher_txt = _collapse_ws(node.get_text(" ", strip=True))
                elif getattr(node, "name", None) not in {"a"}:
                    cand = _collapse_ws(str(getattr(node, "strip", lambda: node)()))
                    if cand:
                        subject_txt = cand
        except Exception:
            pass
    instructor = " ".join(part for part in (subject_txt, teacher_txt) if part).strip()
    if not instructor:
        instructor = _extract_text(area_name, drop_selectors=("a",))
    result["instructor"] = instructor
    return result


def _extract_course_items(inner: Any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if inner is None:
        return out
    course_info = inner.select_one(".course_info")
    if course_info is None:
        return out
    for item in course_info.select(".course .dl_item"):
        dt = item.select_one("dt")
        dd = item.select_one("dd")
        label = _collapse_ws(dt.get_text(" ", strip=True)) if dt else ""
        value = _collapse_ws(dd.get_text(" ", strip=True)) if dd else ""
        if label and value:
            out.append((label, value))
    return out


def _extract_book_items(inner: Any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if inner is None:
        return out
    course_info = inner.select_one(".course_info")
    if course_info is None:
        return out

    file_down = course_info.select_one(".info .file_down")
    if file_down is None:
        return out

    title_el = file_down.select_one("p")
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""
    if title:
        out.append(("교재명", title))

    date_el = file_down.select_one(".book_date")
    date_text = _extract_text(date_el) if date_el else ""
    date_text = re.sub(r"^출간일\s*:?\s*", "", date_text).strip()
    if date_text:
        out.append(("출간일", date_text))

    stores: list[str] = []
    for link in file_down.select(".pay_drop a"):
        name = ""
        img = link.select_one("img")
        if img is not None:
            name = _norm_store_name(img.get("alt") or "")
        if not name:
            name = _norm_store_name(link.get_text(" ", strip=True))
        if name and name not in stores:
            stores.append(name)
    if stores:
        out.append(("구매처", ", ".join(stores)))
    return out


def _extract_alert_lines(inner: Any) -> list[str]:
    alert = inner.select_one(".alert_basic") if inner is not None else None
    if alert is None:
        return []
    text = _extract_text(alert, drop_selectors=("button",))
    return [text] if text else []


def _extract_intro_items(inner: Any) -> list[tuple[str, list[str]]]:
    out: list[tuple[str, list[str]]] = []
    intro = inner.select_one(".course_intro") if inner is not None else None
    if intro is None:
        return out
    for dl in intro.select(".dl_list dl"):
        dt = dl.select_one("dt")
        dd = dl.select_one("dd")
        label = _collapse_ws(dt.get_text(" ", strip=True)) if dt else ""
        if not label or dd is None:
            continue
        lines = _extract_multilines(dd)
        if lines:
            out.append((label, lines))
    return out


def _extract_curriculum_lines(inner: Any) -> list[str]:
    out: list[str] = []
    if inner is None:
        return out
    table_root = inner.select_one(".respon_table")
    if table_root is None:
        return out
    rows = table_root.select("tbody tr")
    for row in rows:
        no = _extract_text(row.select_one("td.no"))
        title = _extract_text(row.select_one("td.tit"))
        cells = row.find_all("td")
        page = _collapse_ws(cells[2].get_text(" ", strip=True)) if len(cells) >= 3 else ""
        duration = _collapse_ws(cells[3].get_text(" ", strip=True)) if len(cells) >= 4 else ""
        previews: list[str] = []
        for span in row.select(".video_tag span"):
            txt = _collapse_ws(span.get_text(" ", strip=True))
            if txt and txt not in previews:
                previews.append(txt)
        if not any((no, title, page, duration, previews)):
            continue
        parts: list[str] = []
        if title:
            parts.append(f"{no}. {title}" if no else title)
        elif no:
            parts.append(no)
        if page:
            parts.append(f"페이지: {page}")
        if duration:
            parts.append(f"강의시간: {duration}")
        if previews:
            parts.append(f"맛보기: {', '.join(previews)}")
        line = " | ".join(part for part in parts if part)
        if line:
            out.append(line)
    return out


def _build_content_text(inner: Any) -> str:
    teacher = _extract_teacher_section(inner)
    course_items = _extract_course_items(inner)
    book_items = _extract_book_items(inner)
    alert_lines = _extract_alert_lines(inner)
    intro_items = _extract_intro_items(inner)
    curriculum_lines = _extract_curriculum_lines(inner)

    parts: list[str] = []

    status_list = teacher.get("status") or []
    if status_list:
        parts.append(f"상태: {', '.join(status_list)}")
    instructor = str(teacher.get("instructor") or "").strip()
    if instructor:
        parts.append(instructor)

    if course_items:
        parts.append("강좌정보")
        parts.extend(f"{label}: {value}" for label, value in course_items)

    if book_items:
        parts.append("")
        parts.append("교재정보")
        parts.extend(f"{label}: {value}" for label, value in book_items)

    if alert_lines:
        parts.append("")
        parts.append("안내")
        parts.extend(alert_lines)

    if intro_items:
        parts.append("")
        parts.append("강좌소개")
        for label, lines in intro_items:
            if len(lines) == 1:
                parts.append(f"{label}: {lines[0]}")
                continue
            parts.append(f"{label}:")
            parts.extend(lines)

    if curriculum_lines:
        parts.append("")
        parts.append("학습목차")
        parts.extend(curriculum_lines)

    return "\n".join(parts).strip()


def _append_kv_html(parts: list[str], heading: str, items: list[tuple[str, str]]) -> None:
    if not items:
        return
    parts.append(f'<section class="edu-ingang-section"><h2>{_html_escape(heading)}</h2><ul>')
    for label, value in items:
        parts.append(
            "<li>"
            f"<strong>{_html_escape(label)}:</strong> {_html_escape(value)}"
            "</li>"
        )
    parts.append("</ul></section>")


def _append_lines_html(parts: list[str], heading: str, lines: list[str]) -> None:
    if not lines:
        return
    parts.append(f'<section class="edu-ingang-section"><h2>{_html_escape(heading)}</h2>')
    for line in lines:
        parts.append(f"<p>{_html_escape(line)}</p>")
    parts.append("</section>")


def _append_intro_html(parts: list[str], items: list[tuple[str, list[str]]]) -> None:
    if not items:
        return
    parts.append('<section class="edu-ingang-section"><h2>강좌소개</h2>')
    for label, lines in items:
        parts.append(f"<h3>{_html_escape(label)}</h3>")
        if len(lines) == 1:
            parts.append(f"<p>{_html_escape(lines[0])}</p>")
            continue
        parts.append("<ul>")
        for line in lines:
            parts.append(f"<li>{_html_escape(line)}</li>")
        parts.append("</ul>")
    parts.append("</section>")


def _build_content_html(inner: Any, teacher_title: str) -> str:
    teacher = _extract_teacher_section(inner)
    course_items = _extract_course_items(inner)
    book_items = _extract_book_items(inner)
    alert_lines = _extract_alert_lines(inner)
    intro_items = _extract_intro_items(inner)
    curriculum_lines = _extract_curriculum_lines(inner)

    parts = ['<div class="edu-ingang-lecture-extract">']
    if teacher_title:
        parts.append(f"<h1>{_html_escape(teacher_title)}</h1>")
    status_list = teacher.get("status") or []
    if status_list:
        parts.append(f'<p><strong>상태:</strong> {_html_escape(", ".join(status_list))}</p>')
    instructor = str(teacher.get("instructor") or "").strip()
    if instructor:
        parts.append(f"<p>{_html_escape(instructor)}</p>")
    _append_kv_html(parts, "강좌정보", course_items)
    _append_kv_html(parts, "교재정보", book_items)
    _append_lines_html(parts, "안내", alert_lines)
    _append_intro_html(parts, intro_items)
    if curriculum_lines:
        parts.append('<section class="edu-ingang-section"><h2>학습목차</h2><ol>')
        for line in curriculum_lines:
            parts.append(f"<li>{_html_escape(line)}</li>")
        parts.append("</ol></section>")
    parts.append("</div>")
    return "".join(parts)


def try_extract_edu_ingang_lecture_post(soup: Any, url: str):
    if soup is None or not is_edu_ingang_lecture_detail_url(url):
        return None
    if BeautifulSoup is None:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _sanitize_html_fragment,
    )

    inner = soup.select_one(".content .inner") or soup.select_one("#content .inner")
    if inner is None:
        return None

    teacher = _extract_teacher_section(inner)
    title = str(teacher.get("title") or "").strip()
    if not title:
        return None

    content_text = _build_content_text(inner)
    if not content_text:
        return None

    try:
        frag = BeautifulSoup(_build_content_html(inner, title), "html.parser")
        root = frag.find(True)
        content_html = _sanitize_html_fragment(root).strip() if root else ""
    except Exception:
        content_html = _build_content_html(inner, title)

    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )
