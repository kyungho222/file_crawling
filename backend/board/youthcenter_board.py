"""온통청년 전용 추출기.

온통청년 `/cmnFooter/openapiIntro/oaiDoc`는 주요 표 데이터가 AJAX로 채워진다.
정적 HTML만 받은 경우에는 표 본문이 비어 있으므로, 같은 세션의 CSRF를 사용해
공개 JSON 엔드포인트를 보강 조회한다.
"""

from __future__ import annotations

import json
import os
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from html import escape, unescape
from typing import Any, Optional
from urllib.parse import urlparse

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


_WS_RE = re.compile(r"\s+")
_DOC_PATH = "/cmnfooter/openapiintro/oaidoc"
_BBS_VIEW_RE = re.compile(r"/bbs\d+view/([^/?#]+)/([^/?#]+)", re.I)
_POLICY_DETAIL_RE = re.compile(r"/youthpolicy/ythplcytotalsearch/ythplcydetail/([^/?#]+)", re.I)
_EVENT_DETAIL_RE = re.compile(r"/youthjoin/ythjoinevent/ythjedetail/([^/?#]+)", re.I)
_DEFAULT_API_SN = "86"
_REQUEST_ARTICLE_CODE = "0053001"
_PAYLOAD_CACHE_MAX = 256
_payload_cache: "OrderedDict[tuple[str, str], Optional[dict[str, Any]]]" = OrderedDict()
_CACHE_MISS = object()


@dataclass
class YouthcenterOpenApiExtract:
    title: str
    content_text: str
    content_html: str
    snippet: str


def _get_cached_payload(kind: str, key: str) -> object:
    if not key:
        return _CACHE_MISS
    cache_key = (kind, key)
    if cache_key not in _payload_cache:
        return _CACHE_MISS
    value = _payload_cache.pop(cache_key)
    _payload_cache[cache_key] = value
    return value


def _set_cached_payload(kind: str, key: str, value: Optional[dict[str, Any]]) -> None:
    if not key:
        return
    cache_key = (kind, key)
    if cache_key in _payload_cache:
        _payload_cache.pop(cache_key)
    _payload_cache[cache_key] = value
    while len(_payload_cache) > _PAYLOAD_CACHE_MAX:
        _payload_cache.popitem(last=False)


def _collapse_ws(value: str) -> str:
    return _WS_RE.sub(" ", str(value or "")).strip()


def _hangul_char_count(value: str) -> int:
    return sum(1 for ch in str(value or "") if "가" <= ch <= "힣")


def _repair_utf8_mojibake(value: Any) -> Any:
    if not isinstance(value, str) or not value:
        return value
    try:
        fixed = value.encode("latin1").decode("utf-8")
    except Exception:
        return value
    if _hangul_char_count(fixed) > _hangul_char_count(value):
        return fixed
    return value


def _repair_json_strings(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _repair_json_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_repair_json_strings(item) for item in value]
    return _repair_utf8_mojibake(value)


def _load_json_response(resp: Any) -> Optional[dict[str, Any]]:
    raw = getattr(resp, "content", b"") or b""
    encodings: list[str] = []
    for encoding in ("utf-8", "utf-8-sig", getattr(resp, "encoding", None), "cp949", "euc-kr"):
        if encoding and encoding not in encodings:
            encodings.append(encoding)

    for encoding in encodings:
        try:
            decoded = raw.decode(encoding)
            parsed = json.loads(decoded)
        except Exception:
            continue
        repaired = _repair_json_strings(parsed)
        return repaired if isinstance(repaired, dict) else None

    try:
        parsed = resp.json()
    except Exception:
        return None
    repaired = _repair_json_strings(parsed)
    return repaired if isinstance(repaired, dict) else None


def is_youthcenter_openapi_doc_url(url: str) -> bool:
    u = (url or "").lower()
    return "youthcenter.go.kr" in u and _DOC_PATH in u


def is_youthcenter_bbs_view_url(url: str) -> bool:
    u = (url or "").lower()
    return "youthcenter.go.kr" in u and _BBS_VIEW_RE.search(u) is not None


def is_youthcenter_policy_detail_url(url: str) -> bool:
    u = (url or "").lower()
    return "youthcenter.go.kr" in u and _POLICY_DETAIL_RE.search(u) is not None


def is_youthcenter_event_detail_url(url: str) -> bool:
    u = (url or "").lower()
    return "youthcenter.go.kr" in u and _EVENT_DETAIL_RE.search(u) is not None


def try_extract_youthcenter_post(soup, url: str, *, html: str = "") -> Optional[YouthcenterOpenApiExtract]:
    bbs_post = try_extract_youthcenter_bbs_view(soup, url, html=html)
    if bbs_post:
        return bbs_post
    policy_post = try_extract_youthcenter_policy_detail(soup, url, html=html)
    if policy_post:
        return policy_post
    event_post = try_extract_youthcenter_event_detail(soup, url, html=html)
    if event_post:
        return event_post
    return try_extract_youthcenter_openapi_doc(soup, url, html=html)


def extract_youthcenter_board_title(soup, *, url: str = "", html: str = "") -> str:
    if not is_youthcenter_bbs_view_url(url):
        return ""
    payload = _fetch_bbs_payload(url=url, html=html or str(soup))
    if payload:
        title = _collapse_ws(payload.get("pstTtl") or "")
        if title:
            return title

    static_title = ""
    try:
        el = soup.select_one("#content-detail[class*='bbs0'][class*='View'] #pstTtl, [class*='bbs0'][class*='View'] #pstTtl, h2.view-title#pstTtl")
        static_title = _collapse_ws(el.get_text(" ", strip=True)) if el else ""
    except Exception:
        static_title = ""
    if static_title:
        try:
            detail = (
                soup.select_one("main#content #content-detail[class*='bbs0'][class*='View']")
                or soup.select_one("#content-detail[class*='bbs0'][class*='View']")
            )
            body = detail.select_one("#pstCn") or detail.select_one(".conts-wrap:not(.download-list)") if detail else None
            content_text = _clean_bbs_body_text(_html_to_text(str(body))) if body else ""
            if not _looks_like_youthcenter_bbs_shell(
                title=static_title,
                section_title=_extract_section_title(soup),
                content_text=content_text,
            ):
                return static_title
        except Exception:
            return static_title
    return ""


def extract_youthcenter_board_reg_date(soup, *, url: str = "", html: str = "") -> Optional[datetime]:
    if not is_youthcenter_bbs_view_url(url):
        return None

    payload = _fetch_bbs_payload(url=url, html=html or str(soup))
    if payload:
        parsed = _parse_youthcenter_datetime(payload.get("frstRegDt") or "")
        if parsed:
            return parsed

    try:
        el = soup.select_one("#content-detail[class*='bbs0'][class*='View'] #frstRegDt span, [class*='bbs0'][class*='View'] #frstRegDt span")
        static_text = _collapse_ws(el.get_text(" ", strip=True)) if el else ""
        parsed = _parse_youthcenter_datetime(static_text)
        if parsed:
            return parsed
    except Exception:
        pass
    return None


def extract_youthcenter_policy_title(soup, *, url: str = "", html: str = "") -> str:
    if not is_youthcenter_policy_detail_url(url):
        return ""
    payload = _fetch_policy_payload(url=url, html=html or str(soup))
    if payload:
        title = _collapse_ws(payload.get("plcyNm") or "")
        if title:
            return title

    try:
        el = soup.select_one("#_plcyNm")
        title = _collapse_ws(el.get_text(" ", strip=True)) if el else ""
        if title:
            return title
    except Exception:
        pass
    return ""


def extract_youthcenter_policy_reg_date(soup, *, url: str = "", html: str = "") -> Optional[datetime]:
    if not is_youthcenter_policy_detail_url(url):
        return None
    payload = _fetch_policy_payload(url=url, html=html or str(soup))
    if not payload:
        return None
    # 온통청년 정책 상세는 기존 항목을 수정 갱신하는 경우가 있어
    # 크롤링 기준 날짜는 최초 등록일보다 최종 수정일을 우선 사용한다.
    return _parse_youthcenter_datetime(
        payload.get("lastMdfcnDt") or payload.get("frstRegDt") or ""
    )


def extract_youthcenter_event_title(soup, *, url: str = "", html: str = "") -> str:
    if not is_youthcenter_event_detail_url(url):
        return ""
    payload = _fetch_event_payload(url=url, html=html or str(soup))
    if payload:
        title = _collapse_ws(payload.get("evntNm") or "")
        if title:
            return title
    try:
        el = soup.select_one("#evntNm, #evntNmTop")
        title = _collapse_ws(el.get_text(" ", strip=True)) if el else ""
        if title:
            return title
    except Exception:
        pass
    return ""


def extract_youthcenter_event_reg_date(soup, *, url: str = "", html: str = "") -> Optional[datetime]:
    if not is_youthcenter_event_detail_url(url):
        return None
    payload = _fetch_event_payload(url=url, html=html or str(soup))
    if not payload:
        return None
    return _parse_youthcenter_datetime(payload.get("frstRegDt") or "")


def extract_youthcenter_openapi_title(soup) -> str:
    if soup is None:
        return ""
    for sel in ("main#content .title-wrap h1.stitle", "h1.stitle", "meta[property='og:title']", "title"):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        raw = el.get("content") if getattr(el, "name", "") == "meta" else el.get_text(" ", strip=True)
        title = _collapse_ws(raw or "")
        if not title:
            continue
        title = re.split(r"\s*(?:<|\||｜|-)\s*", title, maxsplit=1)[0].strip()
        if title:
            return title
    return ""


def try_extract_youthcenter_event_detail(soup, url: str, *, html: str = "") -> Optional[YouthcenterOpenApiExtract]:
    if not soup or not is_youthcenter_event_detail_url(url):
        return None

    payload = _fetch_event_payload(url=url, html=html or str(soup))
    if payload:
        return _extract_event_from_payload(payload)

    title = extract_youthcenter_event_title(soup, url=url, html=html) or _extract_section_title(soup)
    return _extract_event_from_rendered_dom(soup, title=title or "이벤트 상세")


def try_extract_youthcenter_policy_detail(soup, url: str, *, html: str = "") -> Optional[YouthcenterOpenApiExtract]:
    if not soup or not is_youthcenter_policy_detail_url(url):
        return None

    payload = _fetch_policy_payload(url=url, html=html or str(soup))
    if payload:
        return _extract_policy_from_payload(payload)

    title = extract_youthcenter_policy_title(soup, url=url, html=html) or extract_youthcenter_openapi_title(soup)
    return _extract_policy_from_rendered_dom(soup, title=title or "청년정책 상세")


def try_extract_youthcenter_bbs_view(soup, url: str, *, html: str = "") -> Optional[YouthcenterOpenApiExtract]:
    if not soup or not is_youthcenter_bbs_view_url(url):
        return None

    payload = _fetch_bbs_payload(url=url, html=html or str(soup))
    if payload:
        ajax = _extract_bbs_from_payload(payload)
        if ajax:
            return ajax

    rendered = _extract_bbs_from_rendered_dom(soup)
    if rendered and rendered.title and rendered.title != _extract_section_title(soup):
        return rendered

    return rendered


def _extract_policy_from_rendered_dom(soup, *, title: str) -> Optional[YouthcenterOpenApiExtract]:
    detail = soup.select_one("main#content #content-detail") or soup.select_one("#content-detail")
    if not detail:
        return None
    text = _clean_bbs_body_text(_html_to_text(str(detail)))
    if not text:
        return None
    return YouthcenterOpenApiExtract(
        title=title.strip() or "청년정책 상세",
        content_text=text,
        content_html=str(detail).strip(),
        snippet=_collapse_ws(text)[:200],
    )


def _extract_event_from_rendered_dom(soup, *, title: str) -> Optional[YouthcenterOpenApiExtract]:
    detail = soup.select_one("main#content #content-detail") or soup.select_one("#content-detail")
    if not detail:
        return None
    text = _clean_bbs_body_text(_html_to_text(str(detail)))
    if not text:
        return None
    return YouthcenterOpenApiExtract(
        title=title.strip() or "이벤트 상세",
        content_text=text,
        content_html=str(detail).strip(),
        snippet=_collapse_ws(text)[:200],
    )


def _extract_event_from_payload(evnt: dict[str, Any]) -> Optional[YouthcenterOpenApiExtract]:
    title = _collapse_ws(evnt.get("evntNm") or "")
    if not title:
        return None

    lines: list[str] = []
    expln = _clean_policy_value(evnt.get("evntExpln"))
    if expln and expln != title:
        lines.append(expln)
    body = _clean_policy_value(evnt.get("evntCn"))
    if body:
        lines.append(body)
    period = _event_period(evnt)
    if period:
        lines.append(f"이벤트 기간: {period}")
    prsntn = _format_compact_ymd(evnt.get("evntPrsntnDt"))
    if prsntn:
        lines.append(f"발표일: {prsntn}")
    status = _collapse_ws(evnt.get("evntPrgrsSttsCn") or "")
    if status:
        lines.append(f"진행상태: {status}")

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None
    content_html = _build_event_content_html(title=title, lines=lines)
    return YouthcenterOpenApiExtract(
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def _extract_policy_from_payload(plcy: dict[str, Any]) -> Optional[YouthcenterOpenApiExtract]:
    title = _collapse_ws(plcy.get("plcyNm") or "")
    if not title:
        return None

    lines: list[str] = []

    def add(label: str, value: Any) -> None:
        text = _clean_policy_value(value)
        if text:
            lines.append(f"{label}: {text}")

    summary = _clean_policy_value(plcy.get("plcyExplnCn"))
    if summary:
        lines.append(summary)

    lines.append("한 눈에 보는 정책 요약")
    add("정책번호", plcy.get("plcyNo"))
    add("정책분야", _policy_field_name(plcy))
    add("지원내용", plcy.get("plcySprtCn"))
    add("사업 운영 기간", _policy_biz_period(plcy))
    add("사업 신청기간", _policy_apply_period(plcy))
    add("지원 규모(명)", _policy_support_scale(plcy))

    lines.append("신청자격")
    add("연령", _policy_age(plcy))
    add("거주지역", _policy_regions(plcy))
    add("소득", _policy_income(plcy))
    add("학력", _join_list_names(plcy.get("qlfcAcbgList"), "qlfcAcbgCdNm"))
    add("전공", _join_list_names(plcy.get("mjrCndList"), "mjrCndCdNm"))
    add("취업상태", _join_list_names(plcy.get("empmSttsList"), "empmSttsCdNm"))
    add("특화분야", _join_list_names(plcy.get("spclFldList"), "spclFldCdNm"))
    add("추가사항", plcy.get("addAplyQlfcCndCn"))
    add("참여제한 대상", plcy.get("ptcpPrpTrgtCn"))

    lines.append("신청방법")
    add("신청절차", plcy.get("plcyAplyMthdCn"))
    add("심사 및 발표", plcy.get("srngMthdCn"))
    add("제출 서류", plcy.get("sbmsnDcmntCn"))

    lines.append("기타")
    add("기타", plcy.get("etcMttrCn"))
    add("주관 기관", plcy.get("sprvsnInstCdNm"))
    add("운영 기관", plcy.get("operInstCdNm"))

    lines.append("정보 변경 내역")
    add("최종 수정일", _format_youthcenter_date(plcy.get("lastMdfcnDt") or ""))
    add("최초 등록일", _format_youthcenter_date(plcy.get("frstRegDt") or ""))

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None
    content_html = _build_policy_content_html(title=title, lines=lines)
    return YouthcenterOpenApiExtract(
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def try_extract_youthcenter_openapi_doc(soup, url: str, *, html: str = "") -> Optional[YouthcenterOpenApiExtract]:
    if not soup or not is_youthcenter_openapi_doc_url(url):
        return None

    title = extract_youthcenter_openapi_title(soup) or "오픈(OPEN) API 제공목록"
    ajax = _extract_from_ajax(url=url, html=html or str(soup), title=title)
    if ajax:
        return ajax

    rendered = _extract_from_rendered_dom(soup, title=title)
    if rendered and _has_meaningful_parameter_rows(rendered.content_text):
        return rendered

    return rendered


def _extract_bbs_from_rendered_dom(soup) -> Optional[YouthcenterOpenApiExtract]:
    detail = soup.select_one("main#content #content-detail[class*='bbs0'][class*='View']") or soup.select_one("#content-detail[class*='bbs0'][class*='View']")
    if not detail or BeautifulSoup is None:
        return None

    title_el = detail.select_one("#pstTtl, .view-title")
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""
    body = detail.select_one("#pstCn") or detail.select_one(".conts-wrap:not(.download-list)")
    content_text = _clean_bbs_body_text(_html_to_text(str(body))) if body else ""
    if not title and not content_text:
        return None
    section_title = _extract_section_title(soup)
    if _looks_like_youthcenter_bbs_shell(title=title, section_title=section_title, content_text=content_text):
        return None

    content_text = content_text.strip()
    content_html = str(detail).strip()
    return YouthcenterOpenApiExtract(
        title=title or section_title or "제목 없음",
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def _extract_section_title(soup) -> str:
    try:
        el = soup.select_one("main#content .title-wrap h1.stitle, h1.stitle")
        return _collapse_ws(el.get_text(" ", strip=True)) if el else ""
    except Exception:
        return ""


def _extract_bbs_meta_lines(detail) -> list[str]:
    lines: list[str] = []
    for li in detail.select(".view-top ul.info li"):
        label = _collapse_ws(li.find("strong").get_text(" ", strip=True)) if li.find("strong") else ""
        value_el = li.find("span")
        value = _collapse_ws(value_el.get_text(" ", strip=True)) if value_el else ""
        if label and value:
            lines.append(f"{label}: {value}")
    return lines


def _build_bbs_meta_lines(item: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    date_text = _format_youthcenter_date(item.get("frstRegDt") or "")
    if date_text:
        lines.append(f"작성일: {date_text}")
    views = _collapse_ws(item.get("pstInqCnt") or "")
    if views:
        lines.append(f"조회수: {views}")
    return lines


def _extract_bbs_from_payload(item: dict[str, Any]) -> Optional[YouthcenterOpenApiExtract]:
    title = _collapse_ws(item.get("pstTtl") or "")
    raw_content = str(item.get("pstWholCn") or "")
    content_text = _clean_bbs_body_text(_html_to_text(raw_content))
    meta_lines = _build_bbs_meta_lines(item)
    content_text = content_text.strip()
    if not title or not content_text:
        return None

    content_html = _build_bbs_content_html(title=title, content_html=raw_content, meta_lines=meta_lines)
    return YouthcenterOpenApiExtract(
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    if BeautifulSoup is None:
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        return _clean_text_lines(unescape(text))
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
        _strip_bbs_shortcut_link_blocks(soup)
        for br in soup.find_all("br"):
            br.replace_with("\n")
        for tag in soup.find_all(["p", "div", "li", "tr", "h1", "h2", "h3", "h4"]):
            tag.insert_after("\n")
        return _clean_text_lines(soup.get_text(" ", strip=False))
    except Exception:
        return _clean_text_lines(unescape(re.sub(r"<[^>]+>", " ", html)))


def _strip_bbs_shortcut_link_blocks(soup) -> None:
    for link in list(soup.find_all(["a", "button"])):
        try:
            block = link.find_parent(["p", "li", "div"])
            if block is not None and _is_shortcut_only_block(block):
                block.decompose()
            elif getattr(link, "name", "") == "a":
                preserved = _extract_preservable_bbs_link_text(link)
                if preserved:
                    link.replace_with(preserved)
                else:
                    link.decompose()
            else:
                link.decompose()
        except Exception:
            continue


def _extract_preservable_bbs_link_text(link) -> str:
    text = _collapse_ws(link.get_text(" ", strip=True))
    href = _collapse_ws(link.get("href") or "")
    if _looks_like_raw_url_text(text):
        return text
    if not text and _looks_like_raw_url_text(href):
        return href
    return ""


def _looks_like_raw_url_text(value: str) -> bool:
    text = _collapse_ws(value)
    return bool(re.match(r"^https?://[^\s]+$", text, flags=re.I))


def _is_shortcut_only_block(block) -> bool:
    text = _collapse_ws(block.get_text(" ", strip=True))
    if not text:
        return True
    link_text = " ".join(_collapse_ws(a.get_text(" ", strip=True)) for a in block.find_all(["a", "button"]))
    remainder = text
    for part in [p for p in link_text.split(" ") if p]:
        remainder = remainder.replace(part, " ")
    remainder = re.sub(r"[\s\u200b\u200c\u200d\ufeff\xa0✔️✅👉🏻👉ㆍ·•\-\|\[\]\(\)]+", "", remainder)
    return not remainder


def _clean_text_lines(text: str) -> str:
    text = unescape(str(text or "")).replace("\xa0", " ")
    lines = [_collapse_ws(line) for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _clean_bbs_body_text(text: str) -> str:
    lines = []
    for line in (text or "").splitlines():
        clean = _collapse_ws(line)
        if not clean:
            continue
        lines.append(clean)
    return "\n".join(lines).strip()


def _looks_like_youthcenter_bbs_shell(*, title: str, section_title: str, content_text: str) -> bool:
    normalized_title = _collapse_ws(title)
    normalized_section = _collapse_ws(section_title)
    normalized_content = _collapse_ws(content_text)
    if not normalized_content:
        return not normalized_title or normalized_title == normalized_section

    allowed_labels = {"작성일", "카테고리", "조회수", "첨부파일", "작성자"}
    tokens = [tok.strip(":") for tok in re.split(r"\s+", normalized_content) if tok.strip(":")]
    if tokens and all(tok in allowed_labels for tok in tokens):
        return not normalized_title or normalized_title == normalized_section
    return False


def _format_youthcenter_date(value: str) -> str:
    raw = _collapse_ws(value)
    if not raw:
        return ""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if not m:
        return raw
    return ".".join(m.groups())


def _parse_youthcenter_datetime(value: str) -> Optional[datetime]:
    raw = _collapse_ws(value)
    if not raw:
        return None
    m_date = re.match(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})(.*)$", raw)
    if m_date:
        normalized = (
            f"{m_date.group(1)}-{m_date.group(2).zfill(2)}-{m_date.group(3).zfill(2)}"
            f"{m_date.group(4) or ''}"
        )
    else:
        normalized = raw
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        pass
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", normalized)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None


def _build_bbs_content_html(*, title: str, content_html: str, meta_lines: list[str]) -> str:
    meta = "".join(f"<li>{escape(line)}</li>" for line in meta_lines)
    return f"""
<article class="youthcenter-bbs-extract-wrap">
  <h2>{escape(title)}</h2>
  <ul>{meta}</ul>
  <div class="pstCn">{content_html}</div>
</article>
""".strip()


def _clean_policy_value(value: Any) -> str:
    if value is None:
        return ""
    return _clean_bbs_body_text(_html_to_text(str(value)))


def _policy_field_name(plcy: dict[str, Any]) -> str:
    names = []
    for item in plcy.get("userRegMclsfList") or []:
        name = _collapse_ws(item.get("userLclsfNm") or "")
        if name and name not in names:
            names.append(name)
    return ", ".join(names) or _collapse_ws(plcy.get("bscPlanPlcyWayNoNm") or "")


def _policy_biz_period(plcy: dict[str, Any]) -> str:
    code = _collapse_ws(plcy.get("bizPrdSeCd") or "")
    if code == "0056001":
        start = _format_compact_ymd(plcy.get("bizPrdBgngYmd"))
        end = _format_compact_ymd(plcy.get("bizPrdEndYmd"))
        return f"{start} ~ {end}".strip()
    if code == "0056002":
        return _clean_policy_value(plcy.get("bizPrdEtcCn"))
    return ""


def _policy_apply_period(plcy: dict[str, Any]) -> str:
    code = _collapse_ws(plcy.get("aplyPrdSeCd") or "")
    if code == "0057002":
        return "상시"
    if code == "0057003":
        return "마감"
    if code == "0057001":
        ranges = []
        for item in plcy.get("plcySchdlMngList") or []:
            if _collapse_ws(item.get("useYn") or "Y") != "Y":
                continue
            start = _format_compact_ymd(item.get("aplyPrdBgngYmd"))
            end = _format_compact_ymd(item.get("aplyPrdEndYmd"))
            if start or end:
                ranges.append(f"{start} ~ {end}".strip())
        if ranges:
            return "\n".join(ranges)
        start = _format_compact_ymd(plcy.get("aplyPrdBgngYmd"))
        end = _format_compact_ymd(plcy.get("aplyPrdEndYmd"))
        return f"{start} ~ {end}".strip()
    return ""


def _policy_support_scale(plcy: dict[str, Any]) -> str:
    if _collapse_ws(plcy.get("sprtSclLmtYn") or "") == "Y":
        return "제한없음"
    count = _collapse_ws(plcy.get("sprtSclCnt") or "")
    if not count:
        return ""
    suffix = " (선착순)" if _collapse_ws(plcy.get("sprtArvlSeqYn") or "") == "Y" else ""
    return f"{count}명{suffix}"


def _policy_age(plcy: dict[str, Any]) -> str:
    if _collapse_ws(plcy.get("sprtTrgtAgeLmtYn") or "") == "Y":
        return "제한없음"
    min_age = _collapse_ws(plcy.get("sprtTrgtMinAge") or "")
    max_age = _collapse_ws(plcy.get("sprtTrgtMaxAge") or "")
    if min_age or max_age:
        return f"만 {min_age}세 ~ 만 {max_age}세".strip()
    return ""


def _policy_regions(plcy: dict[str, Any]) -> str:
    regions = []
    top_regions = []
    for item in plcy.get("habRgnList") or []:
        top_name = _collapse_ws(item.get("stdgCtpvCdNm") or "")
        if top_name and top_name not in top_regions:
            top_regions.append(top_name)
        name = _collapse_ws(f"{item.get('stdgCtpvCdNm') or ''} {item.get('stdgSggCdNm') or ''}")
        if name and name not in regions:
            regions.append(name)

    if any(_collapse_ws(v) == "전국" for v in top_regions + regions):
        return "전국"

    # 온통청년 프론트는 전국형 정책을 '전국'으로 축약한다.
    # 백엔드는 session 기반 전체 시군구 수를 알 수 없으므로,
    # payload 상 전국 단위로 펼쳐진 경우(광역 시도 17개 이상 또는 시군구가 매우 많은 경우)를
    # 나열 대신 '전국'으로 정리한다.
    if len(top_regions) >= 17 or len(regions) >= 200:
        return "전국"

    return ", ".join(regions)


def _policy_income(plcy: dict[str, Any]) -> str:
    code = _collapse_ws(plcy.get("earnCndSeCd") or "")
    if code == "0043001":
        return "무관"
    if code == "0043002":
        return f"연소득 {plcy.get('earnMinAmt')}만원 이상 ~ {plcy.get('earnMaxAmt')}만원 이하"
    return _clean_policy_value(plcy.get("earnEtcCn"))


def _join_list_names(items: Any, key: str) -> str:
    names = []
    for item in items or []:
        name = _collapse_ws(item.get(key) or "")
        if name and name not in names:
            names.append(name)
    return ", ".join(names)


def _format_compact_ymd(value: Any) -> str:
    raw = re.sub(r"\D", "", str(value or ""))
    if len(raw) != 8:
        return _collapse_ws(value or "")
    return f"{raw[:4]}.{raw[4:6]}.{raw[6:8]}"


def _event_period(evnt: dict[str, Any]) -> str:
    start = _format_compact_ymd(evnt.get("evntBgngDt"))
    end = _format_compact_ymd(evnt.get("evntEndDt"))
    if start and end:
        return f"{start} ~ {end}"
    return start or end


def _build_policy_content_html(*, title: str, lines: list[str]) -> str:
    body = "\n".join(f"<p>{escape(line)}</p>" for line in lines if _collapse_ws(line))
    return f"""
<article class="youthcenter-policy-extract-wrap">
  <h2>{escape(title)}</h2>
  {body}
</article>
""".strip()


def _build_event_content_html(*, title: str, lines: list[str]) -> str:
    body = "\n".join(f"<p>{escape(line)}</p>" for line in lines if _collapse_ws(line))
    return f"""
<article class="youthcenter-event-extract-wrap">
  <h2>{escape(title)}</h2>
  {body}
</article>
""".strip()


def _extract_from_rendered_dom(soup, *, title: str) -> Optional[YouthcenterOpenApiExtract]:
    detail = soup.select_one("main#content #content-detail") or soup.select_one("#content-detail")
    if not detail or BeautifulSoup is None:
        return None

    try:
        frag = BeautifulSoup(str(detail), "html.parser")  # type: ignore[operator]
    except Exception:
        return None
    root = frag.find(id="content-detail") or frag.find(True)
    if not root:
        return None

    for sel in (
        "nav.tab-area",
        "script",
        "style",
        "button",
        ".btn-type04",
        ".btn-prev",
        ".btn-next",
    ):
        try:
            for tag in root.select(sel):
                tag.decompose()
        except Exception:
            pass

    try:
        for pane in list(root.select(".tab-content")):
            if pane.select_one("h3, table, .api-url, #apiUrlAddr, #reqParamList, #resultList"):
                continue
            pane.decompose()
    except Exception:
        pass

    lines = _dom_to_doc_lines(root)
    if not lines:
        return None

    content_text = "\n".join(line for line in lines if line).strip()
    content_html = str(root).strip()
    return YouthcenterOpenApiExtract(
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def _dom_to_doc_lines(root) -> list[str]:
    lines: list[str] = []

    def add(text: str) -> None:
        value = _collapse_ws(text)
        if value:
            lines.append(value)

    first_pane = None
    try:
        panes = [p for p in root.select(".tab-content") if p.select_one("h3, table, .api-url")]
        first_pane = panes[0] if panes else root
    except Exception:
        first_pane = root

    h2 = first_pane.select_one("h2") if first_pane else None
    add(h2.get_text(" ", strip=True) if h2 else "")

    for h3 in first_pane.find_all("h3") if first_pane else []:
        add(h3.get_text(" ", strip=True))
        nxt = h3.find_next_sibling()
        while nxt is not None and getattr(nxt, "name", "") != "h3":
            name = getattr(nxt, "name", "")
            if name == "table":
                lines.extend(_table_to_lines(nxt))
            elif name == "div":
                if _has_class(nxt, "table-wrap"):
                    table = nxt.find("table")
                    if table:
                        lines.extend(_table_to_lines(table))
                else:
                    txt = _collapse_ws(nxt.get_text(" ", strip=True))
                    if txt:
                        add(txt)
            elif name == "p":
                add(nxt.get_text(" ", strip=True))
            nxt = nxt.find_next_sibling()
    return _dedupe_adjacent(lines)


def _has_class(tag, class_name: str) -> bool:
    try:
        return class_name in (tag.get("class") or [])
    except Exception:
        return False


def _table_to_lines(table) -> list[str]:
    lines: list[str] = []
    caption = table.find("caption")
    if caption:
        cap = _collapse_ws(caption.get_text(" ", strip=True))
        if cap:
            lines.append(cap)

    for tr in table.find_all("tr"):
        cells = [_collapse_ws(td.get_text(" ", strip=True)) for td in tr.find_all(["th", "td"])]
        cells = [c for c in cells if c]
        if cells:
            lines.append(" | ".join(cells))
    return lines


def _dedupe_adjacent(lines: list[str]) -> list[str]:
    out: list[str] = []
    for line in lines:
        if out and out[-1] == line:
            continue
        out.append(line)
    return out


def _has_meaningful_parameter_rows(text: str) -> bool:
    compact = text or ""
    return "apiKeyNm" in compact


def _extract_from_ajax(*, url: str, html: str, title: str) -> Optional[YouthcenterOpenApiExtract]:
    if str(os.getenv("YOUTHCENTER_OPENAPI_ENABLE_AJAX", "1")).strip().lower() in ("0", "false", "no", "off"):
        return None

    payload = _fetch_openapi_payload(url=url, html=html)
    if not payload:
        return None

    api_name = payload.get("api_name") or "청년정책API"
    api_url = payload.get("api_url") or ""
    api_sn = payload.get("api_sn") or _DEFAULT_API_SN
    request_rows = payload.get("request_rows") or []
    result_rows = payload.get("result_rows") or []
    request_example = _extract_request_example_from_script(html, api_sn=api_sn, api_url=api_url)

    lines: list[str] = [
        _collapse_ws(str(api_name).replace("API", "")) or "청년정책",
        "1. 요청 URL",
        "URL",
    ]
    if api_url:
        lines.append(api_url)

    lines.extend(["2. 요청 Parameter", "항목 | 타입 | 필수여부 | 설명"])
    lines.extend(
        " | ".join(
            _collapse_ws(str(part or ""))
            for part in (
                row.get("apiArtclNm"),
                row.get("apiArtclTypeNm"),
                row.get("apiArtclEsntlYn"),
                row.get("apiArtclExpln"),
            )
        ).rstrip(" |")
        for row in request_rows
    )

    lines.extend(["3. 출력결과", "항목 | 타입 | 설명 | 비고"])
    lines.extend(
        " | ".join(
            _collapse_ws(str(part or ""))
            for part in (
                _wrap_xml_name(row.get("apiArtclNm")),
                row.get("apiArtclTypeNm"),
                row.get("apiArtclExpln"),
                _wrap_xml_close(row.get("apiArtclNm")),
            )
        ).rstrip(" |")
        for row in result_rows
    )

    lines.append("4. 요청 예시")
    if request_example:
        lines.append(request_example)

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None

    content_html = _build_ajax_content_html(
        api_name=str(api_name),
        api_url=api_url,
        request_rows=request_rows,
        result_rows=result_rows,
        request_example=request_example,
    )
    return YouthcenterOpenApiExtract(
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=_collapse_ws(content_text)[:200],
    )


def _wrap_xml_name(value: Any) -> str:
    name = _collapse_ws(str(value or ""))
    return f"<{name}>" if name else ""


def _wrap_xml_close(value: Any) -> str:
    name = _collapse_ws(str(value or ""))
    return f"</{name}>" if name else ""


def _fetch_openapi_payload(*, url: str, html: str) -> Optional[dict[str, Any]]:
    try:
        import requests  # type: ignore[import-not-found]
    except Exception:
        return None

    try:
        timeout = float(os.getenv("YOUTHCENTER_OPENAPI_AJAX_TIMEOUT", "10") or "10")
    except Exception:
        timeout = 10.0

    try:
        session = requests.Session()
        page_url = "https://www.youthcenter.go.kr/cmnFooter/openapiIntro/oaiDoc"
        page = session.get(page_url, timeout=timeout)
        page.raise_for_status()
        page_html = page.text or html or ""
        csrf = _extract_csrf(page_html) or _extract_csrf(html)
        headers = {
            "Accept": "application/json",
            "Referer": page_url,
            "X-Requested-With": "XMLHttpRequest",
        }
        if csrf:
            headers["X-CSRF-TOKEN"] = csrf

        api_list_resp = session.get(
            "https://www.youthcenter.go.kr/sur/link/openInfoChcApi",
            headers=headers,
            timeout=timeout,
        )
        api_list_resp.raise_for_status()
        api_list_payload = _load_json_response(api_list_resp) or {}
        api_list = api_list_payload.get("result", {}).get("data") or []
        selected = _select_api_info(api_list, html=page_html or html)
        api_sn = str((selected or {}).get("apiSn") or _DEFAULT_API_SN)
        api_name = str((selected or {}).get("apiNm") or "청년정책API")

        intro_resp = session.get(
            f"https://www.youthcenter.go.kr/sur/link/openApiIntro/{api_sn}",
            headers=headers,
            timeout=timeout,
        )
        intro_resp.raise_for_status()
        intro_payload = _load_json_response(intro_resp) or {}
        rows = intro_payload.get("result", {}).get("apiIntro") or []
        rows = [row for row in rows if str(row.get("useYn") or "Y").upper() == "Y"]
        rows.sort(key=_article_sort_key)
        request_rows = [row for row in rows if row.get("apiArtclSeCd") == _REQUEST_ARTICLE_CODE]
        result_rows = [row for row in rows if row.get("apiArtclSeCd") != _REQUEST_ARTICLE_CODE]
        api_url = ""
        for row in rows:
            api_url = _collapse_ws(row.get("apiUrlAddr") or "")
            if api_url:
                break
        return {
            "api_sn": api_sn,
            "api_name": api_name,
            "api_url": api_url,
            "request_rows": request_rows,
            "result_rows": result_rows,
        }
    except Exception:
        return None


def _fetch_bbs_payload(*, url: str, html: str) -> Optional[dict[str, Any]]:
    try:
        import requests  # type: ignore[import-not-found]
    except Exception:
        return None

    ids = _extract_bbs_ids(url=url, html=html)
    if not ids:
        return None
    bbs_sn, pst_sn = ids
    cache_key = f"{bbs_sn}:{pst_sn}"
    cached = _get_cached_payload("bbs", cache_key)
    if cached is not _CACHE_MISS:
        return cached

    try:
        timeout = float(os.getenv("YOUTHCENTER_BBS_AJAX_TIMEOUT", "10") or "10")
    except Exception:
        timeout = 10.0

    page_url = url or f"https://www.youthcenter.go.kr/bbs02View/{bbs_sn}/{pst_sn}"
    try:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )
        page = session.get(page_url, timeout=timeout)
        page.raise_for_status()
        page_html = page.text or html or ""
        csrf = _extract_csrf(page_html) or _extract_csrf(html)
        headers = {
            "Accept": "application/json",
            "Referer": page_url,
            "X-Requested-With": "XMLHttpRequest",
        }
        if csrf:
            headers["X-CSRF-TOKEN"] = csrf
        endpoint = f"https://www.youthcenter.go.kr/sur/cmu/bbs/pst/{bbs_sn}/{pst_sn}"
        for params in ({"pstExpsrYn": "Y"}, None):
            resp = session.get(
                endpoint,
                headers=headers,
                params=params,
                timeout=timeout,
            )
            resp.raise_for_status()
            payload = _load_json_response(resp) or {}
            item = payload.get("result", {}).get("bbs") or {}
            if isinstance(item, dict) and item:
                _set_cached_payload("bbs", cache_key, item)
                return item
        _set_cached_payload("bbs", cache_key, None)
        return None
    except Exception:
        return None


def _fetch_policy_payload(*, url: str, html: str) -> Optional[dict[str, Any]]:
    try:
        import requests  # type: ignore[import-not-found]
    except Exception:
        return None

    plcy_no = _extract_policy_no(url=url, html=html)
    if not plcy_no:
        return None
    cached = _get_cached_payload("policy", plcy_no)
    if cached is not _CACHE_MISS:
        return cached

    try:
        timeout = float(os.getenv("YOUTHCENTER_POLICY_AJAX_TIMEOUT", "10") or "10")
    except Exception:
        timeout = 10.0

    page_url = url or f"https://www.youthcenter.go.kr/youthPolicy/ythPlcyTotalSearch/ythPlcyDetail/{plcy_no}?isNew=N"
    try:
        session = requests.Session()
        page = session.get(page_url, timeout=timeout)
        page.raise_for_status()
        page_html = page.text or html or ""
        csrf = _extract_csrf(page_html) or _extract_csrf(html)
        headers = {
            "Accept": "application/json",
            "Referer": page_url,
            "X-Requested-With": "XMLHttpRequest",
        }
        if csrf:
            headers["X-CSRF-TOKEN"] = csrf
        resp = session.get(
            f"https://www.youthcenter.go.kr/wrk/yrm/plcyInfo/plcy/{plcy_no}?user=true",
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = _load_json_response(resp) or {}
        item = payload.get("result", {}).get("plcy") or {}
        if isinstance(item, dict) and item:
            _set_cached_payload("policy", plcy_no, item)
            return item
        _set_cached_payload("policy", plcy_no, None)
        return None
    except Exception:
        return None


def _fetch_event_payload(*, url: str, html: str) -> Optional[dict[str, Any]]:
    try:
        import requests  # type: ignore[import-not-found]
    except Exception:
        return None

    evnt_sn = _extract_event_id(url=url, html=html)
    if not evnt_sn:
        return None
    cached = _get_cached_payload("event", evnt_sn)
    if cached is not _CACHE_MISS:
        return cached

    try:
        timeout = float(os.getenv("YOUTHCENTER_EVENT_AJAX_TIMEOUT", "10") or "10")
    except Exception:
        timeout = 10.0

    page_url = url or f"https://www.youthcenter.go.kr/youthJoin/ythJoinEvent/ythjeDetail/{evnt_sn}"
    try:
        session = requests.Session()
        page = session.get(page_url, timeout=timeout)
        page.raise_for_status()
        page_html = page.text or html or ""
        csrf = _extract_csrf(page_html) or _extract_csrf(html)
        headers = {
            "Accept": "application/json",
            "Referer": page_url,
            "X-Requested-With": "XMLHttpRequest",
        }
        if csrf:
            headers["X-CSRF-TOKEN"] = csrf
        resp = session.get(
            f"https://www.youthcenter.go.kr/sur/cmu/evnt/evnt/{evnt_sn}?user=true",
            headers=headers,
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = _load_json_response(resp) or {}
        item = payload.get("result", {}).get("evnt") or {}
        if isinstance(item, dict) and item:
            _set_cached_payload("event", evnt_sn, item)
            return item
        _set_cached_payload("event", evnt_sn, None)
        return None
    except Exception:
        return None


def _extract_bbs_ids(*, url: str, html: str) -> Optional[tuple[str, str]]:
    m = _BBS_VIEW_RE.search(url or "")
    if m:
        return m.group(1), m.group(2)
    bbs_m = re.search(r"const\s+bbsSn\s*=\s*['\"]([^'\"]+)['\"]", html or "")
    pst_m = re.search(r"const\s+pstSn\s*=\s*['\"]([^'\"]+)['\"]", html or "")
    if bbs_m and pst_m:
        return bbs_m.group(1), pst_m.group(1)
    return None


def _extract_event_id(*, url: str, html: str) -> str:
    m = _EVENT_DETAIL_RE.search(url or "")
    if m:
        return m.group(1).strip()
    m = re.search(r"const\s+evntMngSn\s*=\s*['\"]([^'\"]+)['\"]", html or "")
    return (m.group(1) or "").strip() if m else ""


def _extract_policy_no(*, url: str, html: str) -> str:
    m = _POLICY_DETAIL_RE.search(url or "")
    if m:
        return m.group(1).strip()
    m = re.search(r"let\s+thisplcyNo\s*=\s*['\"]([^'\"]+)['\"]", html or "")
    return (m.group(1) or "").strip() if m else ""


def _extract_csrf(html: str) -> str:
    m = re.search(r'<meta\s+name=["\']_csrf["\']\s+content=["\']([^"\']+)["\']', html or "", flags=re.I)
    return m.group(1).strip() if m else ""


def _select_api_info(api_list: list[dict[str, Any]], *, html: str) -> dict[str, Any]:
    wanted = _extract_initial_api_sn(html) or _DEFAULT_API_SN
    for item in api_list:
        if str(item.get("apiSn") or "") == wanted:
            return item
    return api_list[0] if api_list else {"apiSn": wanted, "apiNm": "청년정책API"}


def _extract_initial_api_sn(html: str) -> str:
    m = re.search(r"let\s+apiSn\s*=\s*['\"]([^'\"]*)['\"]", html or "")
    return (m.group(1) or "").strip() if m else ""


def _article_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    raw = str(row.get("apiArtclSn") or "")
    try:
        return (int(raw), raw)
    except Exception:
        return (999999, raw)


def _extract_request_example_from_script(html: str, *, api_sn: str, api_url: str) -> str:
    text = html or ""
    case_re = re.compile(
        rf"case\s+['\"]{re.escape(str(api_sn))}['\"]\s*:\s*(.*?)break\s*;",
        flags=re.S,
    )
    case_match = case_re.search(text)
    if not case_match:
        return ""
    block = case_match.group(1)
    pieces = [
        p
        for p in re.findall(r"['\"]([^'\"]+)['\"]", block)
        if p.strip() not in (".api-url", "api-url")
    ]
    joined = _collapse_ws(" ".join(pieces))
    joined = joined.replace("domain +", "")
    joined = re.sub(r"^\.?api-url\s*", "", joined)
    joined = re.sub(r"\s+", " ", joined).strip()
    if api_url:
        try:
            api_path = urlparse(api_url).path.lstrip("/")
        except Exception:
            api_path = ""
        if api_path:
            joined = re.sub(rf"^{re.escape(api_path)}\s*\|\s*", "", joined).strip()
        if "http" not in joined:
            return f"{api_url} | {joined}".strip(" |")
    if joined.startswith("go/") or joined.startswith("/go/"):
        joined = "https://www.youthcenter.go.kr/" + joined.lstrip("/")
    return joined


def _build_ajax_content_html(
    *,
    api_name: str,
    api_url: str,
    request_rows: list[dict[str, Any]],
    result_rows: list[dict[str, Any]],
    request_example: str,
) -> str:
    req_tr = "\n".join(
        "<tr>"
        f"<td>{escape(_collapse_ws(row.get('apiArtclNm') or ''))}</td>"
        f"<td>{escape(_collapse_ws(row.get('apiArtclTypeNm') or ''))}</td>"
        f"<td>{escape(_collapse_ws(row.get('apiArtclEsntlYn') or ''))}</td>"
        f"<td>{escape(_collapse_ws(row.get('apiArtclExpln') or ''))}</td>"
        "</tr>"
        for row in request_rows
    )
    result_tr = "\n".join(
        "<tr>"
        f"<td>{escape(_wrap_xml_name(row.get('apiArtclNm')))}</td>"
        f"<td>{escape(_collapse_ws(row.get('apiArtclTypeNm') or ''))}</td>"
        f"<td>{escape(_collapse_ws(row.get('apiArtclExpln') or ''))}</td>"
        f"<td>{escape(_wrap_xml_close(row.get('apiArtclNm')))}</td>"
        "</tr>"
        for row in result_rows
    )
    return f"""
<div class="youthcenter-openapi-extract-wrap">
  <h2>{escape(_collapse_ws(api_name))}</h2>
  <h3>1. 요청 URL</h3>
  <table><thead><tr><th>URL</th></tr></thead><tbody><tr><td>{escape(api_url)}</td></tr></tbody></table>
  <h3>2. 요청 Parameter</h3>
  <table><thead><tr><th>항목</th><th>타입</th><th>필수여부</th><th>설명</th></tr></thead><tbody>{req_tr}</tbody></table>
  <h3>3. 출력결과</h3>
  <table><thead><tr><th>항목</th><th>타입</th><th>설명</th><th>비고</th></tr></thead><tbody>{result_tr}</tbody></table>
  <h3>4. 요청 예시</h3>
  <p>{escape(request_example)}</p>
</div>
""".strip()
