"""
게시글 상세페이지 HTML에서 메타(작성자 등)를 추출하는 유틸.

원본 아이디어/휴리스틱: crawling/temp 프로젝트의 backend/board_detail_extractor.py
현재 프로젝트에서는 Scan 단계에서 '상세페이지'를 이미 열어둔 경우에만
작성자(author)를 best-effort로 추출하여 file_meta에 담는다.
"""

from __future__ import annotations

import re
import os
import logging
from typing import Optional, Any, Dict, List
from urllib.parse import urljoin, urlparse

from backend.shared.data_standardizer import DataStandardizer
from backend.shared.detail_page_utils import is_detail_page_url
from backend.board.anseong_file import (
    clean_anseong_attachment_name,
    is_anseong_file_url,
    resolve_anseong_yhlib_download_url,
)
from utils.url import extract_download_url_from_js, normalize_attachment_href
from utils.file import strip_fallback_download_label, strip_file_type_display_prefix, strip_trailing_file_size
from backend.board.gm_file import extract_gm_nftc_filelist_attachments, is_gm_nftc_bbs_url
from backend.board.yongin_board import resolve_yongin_file_download_url

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class _DropAuthorDeptExtractLogs(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = str(record.getMessage() or "")
        return "[AuthorExtract]" not in msg and "[DeptExtract]" not in msg


logger.addFilter(_DropAuthorDeptExtractLogs())


def _clean_attachment_display_name(value: Any) -> str:
    cleaned = strip_file_type_display_prefix(strip_trailing_file_size(str(value or "").strip()))
    cleaned = strip_fallback_download_label(cleaned) or cleaned
    return re.sub(r"\s+", " ", cleaned).strip()

def _author_meta_extraction_enabled() -> bool:
    return False


try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


def _filter_guro_attachments(url: Optional[str], attachments: list[dict]) -> list[dict]:
    if not url or not attachments:
        return attachments
    try:
        from backend.board.guro_board import guro_bbs_no, is_guro_url
    except Exception:
        return attachments

    bbs_no = guro_bbs_no(url)
    if not is_guro_url(url) or bbs_no not in {"687", "855", "865", "1145", "1187"}:
        return attachments

    cleaned: list[dict] = []
    seen: set[str] = set()
    for item in attachments:
        href = (item.get("href") or "").strip()
        href_low = href.lower()
        if "downloadbbsfile.do" not in href_low and "atchmnflno" not in href_low:
            continue

        name = re.sub(r"\s+", " ", (item.get("name") or "").strip())
        name = re.sub(r"\b(?:pdf|hwp|hwpx|docx?|xlsx?|pptx?|zip|rar|7z|txt)\s+파일\s+", "", name, flags=re.IGNORECASE)
        name = re.sub(r"\b파일다운로드\b", "", name).strip()
        if name in {"새창", "다운로드", "보기"}:
            name = ""
        if bbs_no == "1187":
            if re.search(r"\.(?:png|jpe?g|gif|webp)(?:\W|$)", name or href, flags=re.IGNORECASE):
                continue
            if "썸네일" in name or "thumbnail" in name.lower():
                continue

        key = href or name.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({"name": name, "href": href})

    if bbs_no == "1187":
        return cleaned
    return cleaned or attachments

_BAD_AUTHOR_TOKENS_DEFAULT = (
    "전화번호", "tel", "fax", "등록일", "작성일", "게시일", "수정일", "조회", "조회수",
    # NOTE: '담당'/'민원' 등은 실제 부서명에 흔히 포함되므로 여기 넣으면 author(=부서 폴백)가 대거 탈락한다.
    #       "담당", "민원" 같은 광범위 키워드는 금지 토큰에서 제외한다.
    "첨부", "첨부파일", "부서 누리집", "동주민센터", "이전글", "다음글", "목록",
    "안내", "처리", "신고", "간소화", "업종", "복합민원", "폐업",
    # menu/contents 페이지 하단에서 자주 등장하는 노이즈(부서명 앞에 붙어 오탐 유발)
    "누리집",
)

def _load_bad_author_tokens() -> tuple[str, ...]:
    """
    하드코딩 최소화:
    - 기본값은 유지(안전장치)
    - 환경변수로 사이트별 튜닝 가능
      - BAD_AUTHOR_TOKENS_ADD: 콤마/세미콜론/줄바꿈 구분으로 추가
      - BAD_AUTHOR_TOKENS_REMOVE: 콤마/세미콜론/줄바꿈 구분으로 제거
    """
    base = list(_BAD_AUTHOR_TOKENS_DEFAULT)
    add_raw = os.getenv("BAD_AUTHOR_TOKENS_ADD", "")
    rm_raw = os.getenv("BAD_AUTHOR_TOKENS_REMOVE", "")

    def _split(raw: str) -> List[str]:
        if not raw:
            return []
        parts = re.split(r"[,\n;]+", raw)
        return [p.strip() for p in parts if p and p.strip()]

    adds = _split(add_raw)
    rms = set(t.lower() for t in _split(rm_raw))

    out: List[str] = []
    seen = set()
    for t in base + adds:
        if not t:
            continue
        if t.lower() in rms:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return tuple(out)

_BAD_AUTHOR_TOKENS = _load_bad_author_tokens()

_NOISY_META_VALUE_TOKENS = (
    "게시물검색",
    "검색항목선택",
    "검색어 입력",
    "검색어입력",
    "업무추진비공개",
    "행정자료실",
    "총 ",
    "페이지",
    "목록",
    "이전",
    "다음",
)

def _extract_relevant_html_segment(html: str) -> str:
    """
    BeautifulSoup 없이(정규식 fallback) 처리해야 하는 환경에서,
    헤더/메뉴/푸터에 포함된 '부서' 등의 노이즈 텍스트를 피하기 위해
    본문 영역으로 추정되는 HTML 구간을 우선 잘라낸다.
    """
    if not html:
        return html
    lowered = html.lower()
    # 우선순위: 실제 본문 컨테이너 후보들
    markers = (
        'id="content"',
        "id='content'",
        'id="contents"',
        "id='contents'",
        'id="sub_content"',
        "id='sub_content'",
        'id="container"',
        "id='container'",
        'id="board"',
        "id='board'",
        'id="boardview"',
        "id='boardview'",
        'class="board"',
        "class='board'",
        'class="board_view"',
        "class='board_view'",
        'class="view_wrap"',
        "class='view_wrap'",
        'class="view"',
        "class='view'",
        "<article",
        "<main",
    )
    for m in markers:
        idx = lowered.find(m)
        if idx != -1:
            # 본문이 긴 페이지(구로구 등)에서 첨부 목록이 잘리지 않도록 20만 자까지 확장
            return html[idx : idx + 200000]
    return html

def _is_bad_author_value(val: str) -> bool:
    if not val:
        return True
    s = str(val).strip()
    if not s:
        return True
    # 콘텐츠 만족도/담당자 정보 블록의 "정보 최종", "최종수정일" 같은 문구는 작성자가 아니다.
    if s in {"정보 최종", "최종 정보"}:
        return True
    if re.search(r"(?:담당자\s*정보|최종\s*수정일|최종수정일)", s):
        return True
    if _is_obviously_noisy_meta_value(s):
        return True
    lowered = s.lower()
    if any(tok in lowered for tok in _BAD_AUTHOR_TOKENS):
        return True
    if re.fullmatch(r"\d{2,3}-\d{3,4}-\d{4}", s):
        return True
    if re.fullmatch(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", s):
        return True
    # 부서명이 길어질 수 있어 길이 제한을 완화한다.
    if len(s) > 80:
        return True
    return False

def _norm_label(s: str) -> str:
    """라벨 텍스트 정규화: 공백/콜론/구분자 제거 후 비교."""
    if not s:
        return ""
    t = str(s).strip().lower()
    t = re.sub(r"[\s\u00a0]+", "", t)
    t = re.sub(r"[:：·•\-\(\)\[\]]+", "", t)
    return t

def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _extract_label_value_from_tableish(root: Any, *labels: str) -> Optional[str]:
    """
    table/dl ?곸뿭?먯꽌 ?쇰꺼 ?쒕쭨濡?媛믪쓣 李얜뒗 ?볦퀎.
    - 媛濡쒗삎: <th>label</th><td>value</td>
    - ?몃줈?묒꽦: <tr><th>label</th></tr><tr><td>value</td></tr>
    """
    if root is None or not labels:
        return None

    wanted = {_norm_label(label) for label in labels if label}
    if not wanted:
        return None

    def _value_from_cell(cell: Any) -> Optional[str]:
        try:
            value = _collapse_ws(cell.get_text(" ", strip=True))
        except Exception:
            return None
        if not value or _is_meta_label_only_value(value):
            return None
        return value

    try:
        rows = root.select("tr")
    except Exception:
        rows = []

    for tr in rows:
        try:
            cells = tr.find_all(["th", "td", "dt", "dd"])
        except Exception:
            continue
        if not cells:
            continue
        for idx, cell in enumerate(cells):
            try:
                label = _norm_label(cell.get_text(" ", strip=True))
            except Exception:
                continue
            if label not in wanted:
                continue

            for nxt in cells[idx + 1 :]:
                value = _value_from_cell(nxt)
                if value:
                    return value

            next_tr = tr.find_next_sibling("tr")
            if next_tr is not None:
                try:
                    next_cells = next_tr.find_all(["td", "dd", "th"])
                except Exception:
                    next_cells = []
                for nxt in next_cells:
                    value = _value_from_cell(nxt)
                    if value and _norm_label(value) not in wanted:
                        return value

    try:
        for dt in root.select("dt"):
            label = _norm_label(dt.get_text(" ", strip=True))
            if label not in wanted:
                continue
            dd = dt.find_next_sibling("dd")
            if dd is None:
                continue
            value = _value_from_cell(dd)
            if value:
                return value
    except Exception:
        pass

    return None


def _finalize_author_candidate(raw: str) -> Optional[str]:
    cleaned = _finalize_author_candidate_raw(raw)
    if not cleaned:
        return None
    val = DataStandardizer.standardize_author(cleaned)
    if not val or _is_bad_author_value(val):
        return None
    return val


def _finalize_author_candidate_raw(raw: str) -> Optional[str]:
    """
    author/department 후보값을 '원문(raw) 보존용'으로 정리한다.
    - 라벨 제거/공백 정리/구분자 분리까지만 수행하고 표준화(정규화)는 하지 않는다.
    """
    if raw is None:
        return None
    s = re.sub(r"[\r\n]+", " ", str(raw))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    s = re.split(r"(?:\||/)", s, maxsplit=1)[0].strip()
    s = re.sub(
        r"^(?:작성자|등록자|등록인|작성인|담당부서|부서|부서명|작성부서|작성부서명|담당과|담당팀|담당기관|담당자|성명|글쓴이|직책|작성자유형|작성일|등록일|게시일|수정일|조회|조회수|전화번호|tel|fax)\s*[:\s]*",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()
    s = re.sub(
        r"\s+(?:작성자|등록자|등록인|작성인|담당부서|부서|부서명|작성부서|작성부서명|담당과|담당팀|담당기관|담당자|성명|글쓴이|직책|작성자유형|작성일|등록일|게시일|수정일|조회|조회수|전화번호|tel|fax).*$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()
    # ✅ 문장 조각 오탐 방지:
    # 예: "부서(문서과)는 이를 ..." → "(문서과)는 이를" 같은 값이 들어올 수 있다.
    # 1) 괄호 안 부서명이 있으면 우선 추출
    try:
        m_paren = re.search(r"\(([^)]+)\)", s)
        if m_paren:
            inner = m_paren.group(1).strip()
            # inner가 유의미하면 inner 우선
            if inner and len(inner) <= 50 and not _is_bad_author_value(inner):
                s = inner
    except Exception:
        pass
    # 2) 부서명 뒤에 조사/문장 이어짐 제거
    s = re.sub(r"(\))\s*(?:은|는|이|가|을|를|와|과)\b.*$", r"\1", s).strip()
    s = re.sub(r"(담당관|센터|과|팀|부|처|국|관)\s*(?:은|는|이|가|을|를|와|과)\b.*$", r"\1", s).strip()
    if not s or _is_bad_author_value(s):
        return None
    return s


_DEPT_SUFFIX_PATTERN = re.compile(r"(담당관|센터|과|팀|부|처|국|관)$")

def _is_detailish_url(u: Optional[str]) -> bool:
    """boardish 상세(view) 페이지로 보이는 URL인지(최소 휴리스틱)."""
    if not u:
        return False
    try:
        if is_detail_page_url(str(u)):
            return True
    except Exception:
        pass
    lu = str(u).lower()
    # 광진/지자체 공통: view.do + nttId/num
    if ("view.do" in lu or "detail.do" in lu or "read.do" in lu) and ("nttid=" in lu or "num=" in lu):
        return True
    # 기타 힌트
    if "view.do" in lu and ("?" in lu):
        return True
    return False


def _is_listish_author_meta_url(u: Optional[str]) -> bool:
    """URLs that should not run free-text author/department fallback."""
    if not u:
        return False
    lu = str(u).lower()
    return any(
        token in lu
        for token in (
            "selectbbslist",
            "bd_selectbbslist",
            "bbslist",
            "list.do",
            "/list",
        )
    )


def _is_sungdong_family_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(str(url)).netloc or "").strip().lower()
    except Exception:
        host = str(url).strip().lower()
    return host.endswith("sd.go.kr") or host.endswith("happysd.or.kr")


def _extract_nowon_author_info_from_html(html: str) -> Dict[str, Optional[str]]:
    """
    노원구청 상세 공통: #printArea .article-view table.table-article 에서
    작성자/부서 메타를 직접 읽는다.
    """
    out: Dict[str, Optional[str]] = {
        "author": None,
        "department": None,
        "author_raw": None,
        "department_raw": None,
    }
    if not BeautifulSoup or not html:
        return out
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return out

    view = soup.select_one("#printArea .article-view")
    if view is None:
        return out
    table = view.select_one("table.table-article") or view.select_one("table.table.table-article")
    if table is None:
        return out

    author_labels = {_norm_label(x) for x in ("작성자", "등록자", "등록인", "작성인", "글쓴이", "담당자", "성명")}
    dept_labels = {_norm_label(x) for x in ("작성부서", "담당부서", "부서", "부서명", "주관부서", "시행부서", "담당과", "담당팀")}

    try:
        rows = table.select("tr")
    except Exception:
        rows = []

    for tr in rows:
        try:
            label_el = tr.select_one("th, dt")
            value_el = tr.select_one("td, dd")
        except Exception:
            label_el = None
            value_el = None
        if label_el is None or value_el is None:
            continue
        label = _norm_label(label_el.get_text(" ", strip=True))
        raw_value = _finalize_author_candidate_raw(value_el.get_text(" ", strip=True))
        if not raw_value:
            continue
        if label in author_labels and out["author"] is None:
            out["author_raw"] = raw_value
            out["author"] = _finalize_author_candidate(raw_value)
        if label in dept_labels and out["department"] is None:
            out["department_raw"] = raw_value
            dept_val = _finalize_author_candidate(raw_value)
            if dept_val and _is_valid_explicit_department_value(dept_val):
                out["department"] = dept_val

    if (not out["author"]) and out["department"]:
        out["author"] = DataStandardizer.standardize_author(out["department"]) or out["department"]
        out["author_raw"] = out["author_raw"] or out["department_raw"] or out["department"]

    return out


def _extract_gwangjin_author_info_from_html(html: str) -> Dict[str, Optional[str]]:
    """Extract Gwangjin board meta from .view .status dt/dd pairs."""
    out: Dict[str, Optional[str]] = {
        "author": None,
        "department": None,
        "author_raw": None,
        "department_raw": None,
    }
    if not BeautifulSoup or not html:
        return out
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return out

    scope = soup.select_one(".view .status") or soup.select_one("div.status")
    debug_enabled = str(os.getenv("CONTENT_AUTHOR_DEBUG", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if scope is None:
        if debug_enabled:
            text = soup.get_text(" ", strip=True)
            hits = []
            for token in ("작성자", "등록자", "담당부서", "부서", "작성일", "첨부파일"):
                pos = text.find(token)
                if pos >= 0:
                    hits.append(f"{token}@{pos}:{text[max(0, pos - 60):pos + 160]}")
            logger.warning(
                "[ContentAuthorDebug][gwangjin.extract] scope_missing html_len=%s tokens=%s status_count=%s view_count=%s",
                len(html or ""),
                hits[:5],
                len(soup.select(".status")),
                len(soup.select(".view")),
            )
        return out

    author_labels = {
        "작성자",
        "등록자",
        "등록인",
        "글쓴이",
        "담당자",
        "성명",
    }
    dept_labels = {
        "부서",
        "부서명",
        "담당부서",
        "작성부서",
        "주관부서",
        "시행부서",
    }

    dl_items = scope.select("dl")
    if debug_enabled:
        samples = []
        for dl in dl_items[:10]:
            label_el = dl.select_one("dt")
            value_el = dl.select_one("dd")
            samples.append(
                {
                    "label": (label_el.get_text(" ", strip=True) if label_el else "")[:80],
                    "value": (value_el.get_text(" ", strip=True) if value_el else "")[:120],
                }
            )
        logger.warning(
            "[ContentAuthorDebug][gwangjin.extract] scope_found dl_count=%s samples=%s",
            len(dl_items),
            samples,
        )

    for dl in dl_items:
        label_el = dl.select_one("dt")
        value_el = dl.select_one("dd")
        if label_el is None or value_el is None:
            continue
        label = _norm_label(label_el.get_text(" ", strip=True))
        raw_value = _finalize_author_candidate_raw(value_el.get_text(" ", strip=True))
        if not raw_value:
            continue
        if label in {_norm_label(x) for x in author_labels} and out["author"] is None:
            author_value = _finalize_author_candidate(raw_value)
            if author_value and not _is_meta_label_only_value(author_value):
                out["author"] = author_value
                out["author_raw"] = raw_value
        if label in {_norm_label(x) for x in dept_labels} and out["department"] is None:
            dept_value = _finalize_author_candidate(raw_value)
            if dept_value and _is_valid_explicit_department_value(dept_value):
                out["department"] = dept_value
                out["department_raw"] = raw_value

    if not out["author"] and out["department"]:
        out["author"] = DataStandardizer.standardize_author(out["department"]) or out["department"]
        out["author_raw"] = out["author_raw"] or out["department_raw"] or out["department"]

    if debug_enabled:
        logger.warning(
            "[ContentAuthorDebug][gwangjin.extract_result] author=%r department=%r author_raw=%r department_raw=%r",
            out.get("author"),
            out.get("department"),
            out.get("author_raw"),
            out.get("department_raw"),
        )

    return out


def _extract_miryang_author_info_from_html(html: str) -> Dict[str, Optional[str]]:
    """
    밀양시청 상세(.inboxRead .headinfo .detail)에서 작성자/부서를 우선 추출한다.
    템플릿에 따라 '작성자', '등록자', '부서명', '담당부서' 라벨이 섞여 나올 수 있어
    detail 블록 텍스트를 라벨 단위로 함께 검사한다.
    """
    out: Dict[str, Optional[str]] = {
        "author": None,
        "department": None,
        "author_raw": None,
        "department_raw": None,
    }
    if not BeautifulSoup or not html:
        return out
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return out

    detail = (
        soup.select_one(".inboxRead .headinfo .detail")
        or soup.select_one(".headinfo .detail")
        or soup.select_one(".inboxRead .detail")
        or soup.select_one(".headinfo")
    )
    if detail is None:
        return out

    try:
        nodes = detail.select("span, li, dd, td, p, div")
    except Exception:
        nodes = []
    if not nodes:
        nodes = [detail]

    author_re = re.compile(r"(?:작성자|등록자|등록인|작성인|글쓴이)\s*[:：]?\s*(.+)")
    dept_re = re.compile(r"(?:부서명|담당부서|작성부서|주관부서|부서)\s*[:：]?\s*(.+)")

    for node in nodes:
        try:
            text = _collapse_ws(node.get_text(" ", strip=True))
        except Exception:
            continue
        if not text:
            continue

        if not out["author_raw"]:
            m_author = author_re.search(text)
            if m_author:
                cand_raw = _finalize_author_candidate_raw(m_author.group(1))
                cand = _finalize_author_candidate(cand_raw or "")
                if cand:
                    out["author_raw"] = cand_raw or cand
                    out["author"] = cand

        if not out["department_raw"]:
            m_dept = dept_re.search(text)
            if m_dept:
                cand_raw = _finalize_author_candidate_raw(m_dept.group(1))
                cand = _finalize_author_candidate(cand_raw or "")
                if cand and _is_valid_explicit_department_value(cand):
                    out["department_raw"] = cand_raw or cand
                    out["department"] = cand

        if out["author"] and out["department"]:
            break

    if (not out["author"]) and out["department"]:
        out["author"] = DataStandardizer.standardize_author(out["department"]) or out["department"]
        out["author_raw"] = out["author_raw"] or out["department_raw"] or out["department"]
    return out


def _is_dongjak_portal_bbs_view_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(str(url)).netloc or "").strip().lower()
        path = (urlparse(str(url)).path or "").strip().lower()
    except Exception:
        u = str(url).strip().lower()
        host = u
        path = u
    return host.endswith("dongjak.go.kr") and "/portal/bbs/" in path and path.endswith("/view.do")


def _collect_dongjak_meta_tokens(soup: Any) -> List[str]:
    if soup is None:
        return []
    root = (
        soup.select_one("#contentDiv")
        or soup.select_one("#go_content")
        or soup.select_one("article")
        or soup.select_one("main")
        or soup
    )
    tokens: List[str] = []
    seen: set[str] = set()
    try:
        for raw in root.stripped_strings:
            txt = re.sub(r"\s+", " ", str(raw or "")).strip()
            if not txt:
                continue
            if txt in seen:
                continue
            seen.add(txt)
            tokens.append(txt)
    except Exception:
        return []
    return tokens


def _extract_dongjak_portal_bbs_meta_from_soup(soup: Any) -> Dict[str, Any]:
    tokens = _collect_dongjak_meta_tokens(soup)
    result: Dict[str, Any] = {
        "author": None,
        "author_raw": None,
        "department": None,
        "date_text": None,
        "view_count": None,
        "contact_phone": None,
    }
    if not tokens:
        return result

    meta_idx = -1
    date_labels = ("공개일", "등록일", "작성일", "게시일")
    for i, tok in enumerate(tokens):
        m_inline = re.search(
            r"(?:공개일|등록일|작성일|게시일)\s*[:：]?\s*"
            r"(\d{4}[-./]\d{1,2}[-./]\d{1,2}(?:\s+\d{1,2}:\d{2}:\d{2})?)",
            tok,
        )
        if m_inline:
            result["date_text"] = m_inline.group(1).strip()
            meta_idx = i
            break
        if tok in date_labels:
            meta_idx = i
            for nxt in tokens[i + 1 : i + 4]:
                m_next = re.search(
                    r"(\d{4}[-./]\d{1,2}[-./]\d{1,2}(?:\s+\d{1,2}:\d{2}:\d{2})?)",
                    nxt,
                )
                if m_next:
                    result["date_text"] = m_next.group(1).strip()
                    break
            if result["date_text"]:
                break

    if meta_idx == -1:
        meta_idx = min(len(tokens), 8)

    window_before = tokens[max(0, meta_idx - 4) : meta_idx] if meta_idx > 0 else tokens[:6]
    for cand in reversed(window_before):
        if re.search(r"\d{2,3}-\d{3,4}-\d{4}", cand):
            m_phone = re.search(r"(\d{2,3}-\d{3,4}-\d{4})", cand)
            if m_phone and not result["contact_phone"]:
                result["contact_phone"] = m_phone.group(1)
            continue
        if _DEPT_SUFFIX_PATTERN.search(cand) and not _is_obviously_noisy_meta_value(cand):
            result["department"] = cand.strip()
            break

    try:
        view_info = soup.select_one(".viewInfo") or soup.select_one(".view .viewInfo")
    except Exception:
        view_info = None
    if view_info is not None:
        try:
            for dl in view_info.select("dl"):
                dt = dl.select_one("dt")
                dd_user = dl.select_one("dd.user")
                dd = dd_user or dl.select_one("dd")
                dept_candidate = _collapse_ws(dt.get_text(" ", strip=True)) if dt is not None else ""
                author_candidate = _collapse_ws(dd.get_text(" ", strip=True)) if dd is not None else ""
                if (
                    dept_candidate
                    and not result.get("department")
                    and _DEPT_SUFFIX_PATTERN.search(dept_candidate)
                    and not _is_obviously_noisy_meta_value(dept_candidate)
                ):
                    result["department"] = dept_candidate
                if (
                    dd_user is not None
                    and author_candidate
                    and not result.get("author")
                    and _looks_like_person_author_name(author_candidate)
                ):
                    result["author"] = author_candidate
                    result["author_raw"] = author_candidate
                if result.get("author") and result.get("department"):
                    break
        except Exception:
            pass

    for i, tok in enumerate(tokens):
        if tok not in ("조회수", "조회") and not tok.startswith("조회수"):
            continue
        m_inline = re.search(r"(?:조회수|조회)\s*[:：]?\s*([0-9][0-9,]{0,10})", tok)
        if m_inline:
            vraw = (m_inline.group(1) or "").replace(",", "").strip()
            if vraw.isdigit():
                result["view_count"] = int(vraw)
                break
        for nxt in tokens[i + 1 : i + 3]:
            m_next = re.search(r"([0-9][0-9,]{0,10})", nxt)
            if m_next:
                vraw = (m_next.group(1) or "").replace(",", "").strip()
                if vraw.isdigit():
                    result["view_count"] = int(vraw)
                    break
        if result["view_count"] is not None:
            break

    if not result["contact_phone"]:
        for tok in tokens[: max(12, meta_idx + 2 if meta_idx > 0 else 12)]:
            m_phone = re.search(r"(\d{2,3}-\d{3,4}-\d{4})", tok)
            if m_phone:
                result["contact_phone"] = m_phone.group(1)
                break

    return result


def extract_dongjak_portal_department(soup: Any) -> str:
    meta = _extract_dongjak_portal_bbs_meta_from_soup(soup)
    return str(meta.get("department") or "").strip()


def extract_dongjak_portal_author_info(soup: Any) -> Dict[str, Optional[str]]:
    meta = _extract_dongjak_portal_bbs_meta_from_soup(soup)
    author = str(meta.get("author") or "").strip() or None
    department = str(meta.get("department") or "").strip() or None
    return {
        "author": author,
        "department": department,
        "author_raw": str(meta.get("author_raw") or author or "").strip() or None,
        "department_raw": department,
        "author_kind": "person" if author else ("org" if department else None),
    }


def extract_dongjak_portal_date_text(soup: Any) -> str:
    meta = _extract_dongjak_portal_bbs_meta_from_soup(soup)
    return str(meta.get("date_text") or "").strip()


_SITE_NAME_BY_HOST_SUFFIX: tuple[tuple[str, str], ...] = (
    ("council.jongno.go.kr", "종로구의회"),
    ("jongno.go.kr", "종로구청"),
    ("resve.yongin.go.kr", "용인특례시"),
    ("yongin.go.kr", "용인특례시"),
    ("sd.go.kr", "성동구청"),
    ("happysd.or.kr", "성동구청"),
    ("gwangjin.go.kr", "광진구청"),
    ("guro.go.kr", "구로구청"),
    ("gangdong.go.kr", "강동구청"),
    ("gangnam.go.kr", "강남구청"),
    ("dobong.go.kr", "도봉구청"),
    ("dongjak.go.kr", "동작구청"),
    ("songpa.go.kr", "송파구청"),
    ("nowon.kr", "노원구청"),
    ("sb.go.kr", "성북구청"),
)


def _extract_site_name_from_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    if not s:
        return None
    for pat in (
        r"([가-힣]{2,20}(?:특례시|구청|시청|군청|의회))",
        r"([가-힣]{2,20}구)\b",
    ):
        m = re.search(pat, s)
        if not m:
            continue
        hit = (m.group(1) or "").strip()
        if not hit:
            continue
        if hit.endswith(("특례시", "구청", "시청", "군청", "의회")):
            return hit
        if hit.endswith("구"):
            return f"{hit}청"
    return None


def _is_site_name_like(val: Optional[str]) -> bool:
    return bool(_extract_site_name_from_text(val))


def _is_obviously_noisy_meta_value(val: Optional[str]) -> bool:
    if not val:
        return False
    s = re.sub(r"\s+", " ", str(val)).strip()
    if not s:
        return False
    if _extract_site_name_from_text(s) or _DEPT_SUFFIX_PATTERN.search(s):
        return False

    noise_hits = 0
    for token in _NOISY_META_VALUE_TOKENS:
        if token in s:
            noise_hits += 1
    if re.search(r"총\s*\d+\s*개", s):
        noise_hits += 1
    if re.search(r"검색(?:어|항목)", s):
        noise_hits += 1
    if re.search(r"\[\s*\d+(?:\s*[\]/-]\s*\d+)?\s*\]?", s):
        noise_hits += 1
    if (">" in s or "<" in s) and len(s) >= 20:
        noise_hits += 1
    if noise_hits >= 2:
        return True

    word_count = len([w for w in re.split(r"\s+", s) if w])
    if word_count >= 6 and noise_hits >= 1:
        return True
    if len(s) >= 45 and not _DEPT_SUFFIX_PATTERN.search(s):
        return True
    return False


def _resolve_site_name(url: Optional[str], html: str) -> Optional[str]:
    host = ""
    try:
        host = (urlparse(str(url)).netloc or "").strip().lower()
    except Exception:
        host = str(url or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    for suffix, site_name in _SITE_NAME_BY_HOST_SUFFIX:
        if host == suffix or host.endswith(f".{suffix}") or host.endswith(suffix):
            return site_name

    try:
        if BeautifulSoup and html:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
            tag = soup.select_one("meta[property='og:site_name']")
            if tag is not None:
                site_name = _extract_site_name_from_text(tag.get("content") or "")
                if site_name:
                    return site_name
            title_el = soup.select_one("title")
            if title_el is not None:
                site_name = _extract_site_name_from_text(title_el.get_text(" ", strip=True))
                if site_name:
                    return site_name
    except Exception:
        pass
    return None


_AUTHOR_LABEL_HINT_RE = re.compile(
    r"(작성자|등록자|등록인|작성인|글쓴이|성명|담당자|담당부서|주관부서|시행부서|부서명|작성부서|author|writer|department)",
    re.IGNORECASE,
)


def _has_author_label_hint(html: str) -> bool:
    if not html:
        return False
    segment = _extract_relevant_html_segment(html)
    return bool(_AUTHOR_LABEL_HINT_RE.search(segment[:120000]))


def _extract_sungdong_meta_text(html: str) -> str:
    """
    성동구(sd.go.kr) 계열 게시판은 메타가 본문 바로 위에 평문처럼 붙는 경우가 많다.
    '상세보기 ~ 담당부서/문의처/첨부파일' 구간을 좁게 잘라 department/author 추출 정확도를 올린다.
    """
    if not html:
        return ""

    try:
        if BeautifulSoup:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
            text = soup.get_text("\n", strip=True)
        else:
            text = re.sub(r"<[^>]+>", "\n", html)
    except Exception:
        text = re.sub(r"<[^>]+>", "\n", html)

    text = re.sub(r"[\t\r\f\v\xa0]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = text.strip()
    if not text:
        return ""

    flat = re.sub(r"\s+", " ", text).strip()
    if not flat:
        return ""

    candidates: List[str] = []
    seen: set[str] = set()

    def _push(chunk: str) -> None:
        chunk = re.sub(r"\s+", " ", str(chunk or "")).strip()
        if not chunk or chunk in seen:
            return
        seen.add(chunk)
        candidates.append(chunk)

    for marker in ("상세보기 - 제목", "상세보기-제목", "상세보기"):
        start = flat.find(marker)
        if start == -1:
            continue
        end = len(flat)
        for stop_token in ("첨부파일", "내용", "목록", "콘텐츠 만족도 조사", "담당자 정보"):
            stop = flat.find(stop_token, start + len(marker))
            if stop != -1:
                end = min(end, stop)
        _push(flat[start:end + 40])
        break

    for token in ("담당부서", "문의처", "홈페이지", "작성자", "등록자", "담당자"):
        pos = flat.find(token)
        if pos == -1:
            continue
        _push(flat[max(0, pos - 120): min(len(flat), pos + 360)])

    return "\n".join(candidates) if candidates else flat[:600]


def _extract_sungdong_author_info_from_html(html: str) -> Dict[str, Optional[str]]:
    meta_text = _extract_sungdong_meta_text(html)
    if not meta_text:
        return {
            "author": None,
            "department": None,
            "author_raw": None,
            "department_raw": None,
        }

    department_raw: Optional[str] = None
    for pattern in (
        r"(?:담당부서|주관부서|시행부서|작성부서|담당과|담당팀|담당기관)\s*[|:：]?\s*(?:성동구청\s*)?([가-힣A-Za-z0-9·ㆍ&() \-]{1,40}?(?:담당관|과|팀|센터|실|동|구|관|처|국))(?=\s*(?:\||/|,|첨부파일|문의처|홈페이지|작성일|조회수|$))",
        r"문의처\s*[|:：]?\s*(?:성동구청\s*)?([가-힣A-Za-z0-9·ㆍ&() \-]{1,40}?(?:담당관|과|팀|센터|실|동|구|관|처|국))(?=\s*(?:\(|☎|0\d{1,2}-\d{3,4}-\d{4}|$))",
    ):
        match = re.search(pattern, meta_text, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = _finalize_author_candidate_raw(match.group(1))
        if candidate and _looks_like_department_name(candidate):
            department_raw = candidate
            break

    author_raw: Optional[str] = None
    for pattern in (
        r"(?:작성자|등록자|등록인|글쓴이|담당자|성명)\s*[|:：]?\s*([가-힣]{2,4}(?:\s*[가-힣]{1,2})?)",
    ):
        match = re.search(pattern, meta_text, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = _finalize_author_candidate_raw(match.group(1))
        if candidate and not _looks_like_department_name(candidate):
            author_raw = candidate
            break

    department = _finalize_author_candidate(department_raw) if department_raw else None
    author = _finalize_author_candidate(author_raw) if author_raw else None

    if author and _looks_like_department_name(author):
        department = department or author
        department_raw = department_raw or author_raw
        author = None
        author_raw = None

    return {
        "author": author,
        "department": department,
        "author_raw": author_raw,
        "department_raw": department_raw,
    }


_DEPT_NAME_HINT = re.compile(
    r"(담당관|센터|과|팀|부|처|국|관|지소|본부|실|위원회|사업단|주민센터)(?:\s|\)|$)"
)


def _looks_like_department_name(val: Optional[str]) -> bool:
    """
    '부서' 라벨 폴백(Tier 2)에서 문장 오탐을 막기 위한 최소 검증.
    - 실제 부서명은 보통 'OO과/OO팀/OO국/OO센터/OO담당관' 등을 포함한다.
    """
    if not val:
        return False
    s = str(val).strip()
    if not s:
        return False
    return bool(_DEPT_NAME_HINT.search(s))


def _compact_department_like_value(val: Optional[str]) -> Optional[str]:
    """
    주소/층수/건물명 + 부서명이 한 줄로 붙은 경우 마지막 부서명 블록만 보존한다.
    예: '... 12층 공연예술본부 공연유통팀' -> '공연예술본부 공연유통팀'
    """
    if not val:
        return None
    s = re.sub(r"\s+", " ", str(val)).strip()
    if not s:
        return None

    dept_chunk = (
        r"[가-힣A-Za-z][가-힣A-Za-z0-9·&()]{0,20}"
        r"(?:담당관|센터|주민센터|과|팀|부|처|국|관|지소|본부|실|위원회|사업단)"
    )
    m = re.search(
        rf"(({dept_chunk})(?:\s+({dept_chunk})){{0,2}})$",
        s,
    )
    if m:
        candidate = re.sub(r"\s+", " ", m.group(1)).strip()
        if candidate and candidate != s and _looks_like_department_name(candidate):
            return candidate
    return s


def _is_valid_department_value(val: Optional[str]) -> bool:
    if not val:
        return False
    raw = re.sub(r"\s+", " ", str(val)).strip()
    s = _compact_department_like_value(val) or raw
    if not s:
        return False
    if raw and raw != s:
        if (
            re.search(r"\d{2,3}-\d{3,4}-\d{4}", raw)
            or any(token in raw.lower() for token in ("문의", "tel", "fax", "@"))
            or re.search(r"\d+\s*(?:층|동|호)", raw)
            or re.search(r"[가-힣A-Za-z0-9]+\s*(?:로|길|번길)\b", raw)
        ):
            return False
    if _is_obviously_noisy_meta_value(s):
        return False
    return _looks_like_department_name(s) or _is_site_name_like(s)


def _is_valid_explicit_department_value(val: Optional[str]) -> bool:
    """라벨 기반으로 잡힌 부서값은 행정동/읍/면도 허용한다."""
    if _is_valid_department_value(val):
        return True
    if not val:
        return False
    s = re.sub(r"\s+", " ", str(val)).strip()
    if not s or _is_obviously_noisy_meta_value(s) or _is_meta_label_only_value(s):
        return False
    if re.search(r"\d{2,3}-\d{3,4}-\d{4}", s) or any(token in s.lower() for token in ("문의", "tel", "fax", "@")):
        return False
    return bool(re.search(r"[가-힣0-9]+(?:동|읍|면)$", s))


_GENERIC_AUTHOR_PLACEHOLDERS = {
    "관리자",
    "운영자",
    "admin",
    "administrator",
    "staff",
    "담당자",
    "작성자",
    "등록자",
    "등록인",
    "글쓴이",
    "성명",
}

_META_LABEL_ONLY_VALUES = {
    "작성자",
    "등록자",
    "등록인",
    "작성인",
    "글쓴이",
    "성명",
    "담당자",
    "담당부서",
    "부서",
    "부서명",
    "작성부서",
    "작성부서명",
    "연락처",
    "전화번호",
    "문의",
    "tel",
    "fax",
    "게시일",
    "작성일",
    "등록일",
    "마감일",
    "내용",
    "파일",
}
_META_LABEL_ONLY_VALUES_NORMALIZED = {
    re.sub(r"\s+", "", item).lower() for item in _META_LABEL_ONLY_VALUES
}


def _is_meta_label_only_value(val: Optional[str]) -> bool:
    if not val:
        return False
    s = str(val).strip()
    if not s:
        return False
    normalized = re.sub(r"\s+", "", s).lower()
    return normalized in _META_LABEL_ONLY_VALUES_NORMALIZED


def _looks_like_person_author_name(val: Optional[str]) -> bool:
    if not val:
        return False
    s = str(val).strip()
    if not s or _is_obviously_noisy_meta_value(s):
        return False
    lowered = s.lower()
    if lowered in _GENERIC_AUTHOR_PLACEHOLDERS:
        return False
    if _looks_like_department_name(s) or _is_site_name_like(s):
        return False
    if re.fullmatch(r"[가-힣]{2,4}(?:\s*[가-힣]{1,2})?", s):
        return True
    if re.fullmatch(r"[A-Za-z][A-Za-z .'-]{1,40}", s):
        return True
    return False


def is_valid_content_author_value(val: Optional[str]) -> bool:
    if not val:
        return False
    s = str(val).strip()
    if not s or _is_obviously_noisy_meta_value(s):
        return False
    lowered = s.lower()
    if lowered in _GENERIC_AUTHOR_PLACEHOLDERS:
        return False
    return _looks_like_person_author_name(s) or _is_valid_department_value(s) or _is_site_name_like(s)


def resolve_content_author_fields(
    author_info: Optional[Dict[str, Any]],
    *,
    url: Optional[str] = None,
    html: str = "",
) -> Dict[str, Optional[str]]:
    """
    LEARN_LIST.content_author 저장용 대표값을 정한다.
    우선순위:
    1. 유효한 부서(department) 사용
    2. 아니면 파싱된 작성자(author)가 실제 등록 주체처럼 보이면 사용
    3. 둘 다 부적절/부재면 사이트명 사용
    """
    info = dict(author_info or {})

    author = DataStandardizer.standardize_author(info.get("author")) or str(info.get("author") or "").strip() or None
    department = DataStandardizer.standardize_author(info.get("department")) or str(info.get("department") or "").strip() or None
    author_raw = str(info.get("author_raw") or "").strip() or None
    department_raw = str(info.get("department_raw") or "").strip() or None
    author_kind = str(info.get("author_kind") or "").strip() or None
    site_name = _resolve_site_name(url, html)

    if author and _looks_like_department_name(author):
        author = _compact_department_like_value(author) or author
    if department:
        department = _compact_department_like_value(department) or department

    if department and _is_valid_department_value(department):
        return {
            "content_author": department,
            "content_author_kind": "org",
            "content_author_raw": department_raw or department,
        }

    if author and is_valid_content_author_value(author):
        if _is_valid_department_value(author) or _is_site_name_like(author):
            author_kind = "org"
        elif not author_kind:
            author_kind = "person"
        return {
            "content_author": author,
            "content_author_kind": author_kind,
            "content_author_raw": author_raw or author,
        }

    if author and author_kind == "org" and not _is_obviously_noisy_meta_value(author) and not _is_meta_label_only_value(author):
        return {
            "content_author": author,
            "content_author_kind": "org",
            "content_author_raw": author_raw or author,
        }

    if site_name:
        return {
            "content_author": site_name,
            "content_author_kind": "org",
            "content_author_raw": site_name,
        }

    fallback = author or department
    fallback_raw = author_raw or department_raw or fallback
    fallback_kind = "org" if fallback and (_is_valid_department_value(fallback) or _is_site_name_like(fallback)) else author_kind
    return {
        "content_author": fallback,
        "content_author_kind": fallback_kind,
        "content_author_raw": fallback_raw,
    }


def _extract_dept_from_footerish_text(text: str) -> Optional[str]:
    """
    하단 정보영역(담당/문의/전화/☎/수정일 라인 등)에서 부서명을 best-effort로 추출한다.
    - '담당' 라벨이 항상 있다는 보장이 없으므로 라벨 유무 모두 지원
    - 단, 전화/문의 신호가 있을 때만 시도(본문 문장 오탐 방지)
    """
    if not text:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    if not s:
        return None

    has_contact_signal = bool(
        ("문의" in s)
        or ("☎" in s)
        or ("tel" in s.lower())
        or ("fax" in s.lower())
        or re.search(r"\d{2,3}-\d{3,4}-\d{4}", s)
    )
    if not has_contact_signal:
        return None

    stop_tokens = (
        "전화번호",
        "전화",
        "문의처",
        "문의",
        "연락처",
        "담당자",
        "작성자",
        "등록자",
        "첨부파일",
        "홈페이지",
        "조회수",
        "조회",
        "작성일",
        "등록일",
        "수정일",
        "tel",
        "fax",
        "☎",
    )

    # 0) footer 라벨-값 시퀀스: '담당부서 오류1동 전화번호 02-...' 같은 구조를 우선 처리
    m0 = re.search(
        r"(?:담당부서|담당\s*부서|주관부서|시행부서|작성부서|담당과|담당팀|담당기관|부서명|부서)"
        r"\s*[:\s|/·-]*"
        r"([가-힣A-Za-z0-9()·&\s]{1,40}?)"
        r"(?=\s*(?:"
        + "|".join(re.escape(token) for token in stop_tokens)
        + r"|\d{2,3}-\d{3,4}-\d{4}|$))",
        s,
        flags=re.IGNORECASE,
    )
    if m0:
        cand0 = _finalize_author_candidate(m0.group(1).strip())
        cand0 = _compact_department_like_value(cand0) if cand0 else cand0
        if cand0 and _is_valid_explicit_department_value(cand0):
            return cand0

    # 1) 라벨 기반 (담당/담당부서/부서 등) - '|' 구분자도 허용
    m = re.search(
        r"(?:담당부서|담당\s*부서|담당|부서명|부서)\s*[:\s|/·]*"
        r"([가-힣][가-힣0-9()·\s]{0,30}?"
        r"(?:담당관|센터|과|팀|부|처|국|관))",
        s,
        flags=re.IGNORECASE,
    )
    if m:
        cand = _finalize_author_candidate(m.group(1).strip())
        cand = _compact_department_like_value(cand) if cand else cand
        if cand and _is_valid_department_value(cand):
            return cand

    # 2) 라벨이 없을 때: 'OO과 | 문의 | 02-...' 또는 'OO과 문의 02-...' 형태
    m2 = re.search(
        r"([가-힣][가-힣0-9()·\s]{0,30}?"
        r"(?:담당관|센터|과|팀|부|처|국|관))"
        r"\s*(?:\||/|,)?\s*(?:문의|전화|tel|fax|☎|\d{2,3}-\d{3,4}-\d{4})",
        s,
        flags=re.IGNORECASE,
    )
    if m2:
        cand2 = _finalize_author_candidate(m2.group(1).strip())
        cand2 = _compact_department_like_value(cand2) if cand2 else cand2
        if cand2 and _is_valid_department_value(cand2):
            return cand2

    return None

def extract_author_from_html(html: str, url: Optional[str] = None) -> Optional[str]:
    if not _author_meta_extraction_enabled():
        return None
    if not html:
        return None
    if _is_listish_author_meta_url(url):
        return None
    if url and not is_detail_page_url(str(url)):
        return None

    if not BeautifulSoup:
        try:
            segment = _extract_relevant_html_segment(html)
            text = re.sub(r"<[^>]+>", " ", segment)
            text = re.sub(r"\s+", " ", text).strip()
            return extract_author_from_text(text)
        except Exception:
            return None

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    try:
        if _is_sungdong_family_url(url):
            sd_info = _extract_sungdong_author_info_from_html(html)
            sd_author = sd_info.get("author") or sd_info.get("department")
            if sd_author:
                logger.info(
                    "[AuthorExtract][Sungdong] meta block match | url=%r author=%r department=%r",
                    url,
                    sd_author,
                    sd_info.get("department"),
                )
                return sd_author
    except Exception:
        pass

    excluded_selectors = ["nav", "header", "footer", "aside"]
    excluded_elements = set()
    for sel in excluded_selectors:
        for el in soup.select(sel):
            excluded_elements.add(el)

    search_root = None
    for sel in ("article", "main", "#contents", "#content", ".contents", ".sub_contents", ".container", "body"):
        found = soup.select_one(sel)
        if found:
            search_root = found
            break
    if not search_root:
        search_root = soup

    author_labels = (
        "작성자",
        "등록자",
        "등록인",
        "작성인",
        "글쓴이",
        "성명",
        "담당자",
        "author",
        "writer",
    )
    semantic_author_raw = _extract_label_value_from_tableish(search_root, *author_labels)
    if semantic_author_raw:
        semantic_author = _finalize_author_candidate(semantic_author_raw)
        if semantic_author:
            logger.info("[AuthorExtract] semantic table match | url=%r author=%r", url, semantic_author)
            return semantic_author

    # try:
    #     if not _is_detailish_url(url):
    #         raise RuntimeError("skip_tier0_non_detail")
    #     candidates: List[str] = []
    #     for el in search_root.find_all(["div", "dl", "ul", "li", "p", "table", "section", "td"]):
    #         if el in excluded_elements:
    #             continue
    #         t = el.get_text(" ", strip=True)
    #         if not t:
    #             continue
    #         if ("문의" in t) or ("전화" in t) or ("tel" in t.lower()) or re.search(r"\d{2,3}-\d{3,4}-\d{4}", t):
    #             candidates.append(t)

    #     for t in candidates:
    #         cand = _extract_dept_from_footerish_text(t)
    #         if cand:
    #             logger.info("[AuthorExtract] Tier 0 footer match | url=%r author=%r", url, cand)
    #             return cand
    # except Exception:
    #     pass

    for label in author_labels:
        norm_label = _norm_label(label)
        for el in search_root.find_all(["dt", "th", "td", "span", "label", "div", "p", "li"]):
            if el in excluded_elements:
                continue
            t = el.get_text(" ", strip=True)
            if not t:
                continue
            nt = _norm_label(t)
            if nt != norm_label:
                m_inline = re.search(rf"{re.escape(label)}\s*[:\s]+(.{{2,80}})$", t, flags=re.IGNORECASE)
                if m_inline:
                    cand = _finalize_author_candidate(m_inline.group(1))
                    if cand:
                        logger.info("[AuthorExtract] inline label match | url=%r label=%r author=%r", url, label, cand)
                        return cand
                continue

            nxt = el.find_next_sibling(["dd", "td", "span", "div"])
            if nxt and nxt not in excluded_elements:
                cand = _finalize_author_candidate(nxt.get_text(" ", strip=True))
                if cand:
                    logger.info("[AuthorExtract] sibling label match | url=%r label=%r author=%r", url, label, cand)
                    return cand

            parent = el.parent
            if parent and parent not in excluded_elements:
                combined = parent.get_text(" ", strip=True)
                m = re.search(rf"{re.escape(label)}\s*[:\s]*([^:|]{{2,40}})", combined)
                if m:
                    cand = _finalize_author_candidate(m.group(1))
                    if cand:
                        logger.info("[AuthorExtract] parent label match | url=%r label=%r author=%r", url, label, cand)
                        return cand

    semantic_department_raw = _extract_label_value_from_tableish(
        search_root,
        "부서",
        "담당부서",
        "주관부서",
        "수행부서",
        "부서명",
        "작성부서",
        "작성부서명",
        "담당과",
        "담당팀",
        "담당기관",
        "department",
    )
    if semantic_department_raw:
        semantic_department = _finalize_author_candidate(semantic_department_raw)
        if semantic_department and _is_valid_department_value(semantic_department):
            logger.info("[AuthorExtract] semantic department fallback | url=%r author=%r", url, semantic_department)
            return semantic_department

    selector_candidates = [
        ".author",
        ".writer",
        ".name",
        '[class*="author"]',
        '[class*="writer"]',
        '[class*="name"]',
        '[id*="author"]',
        '[id*="writer"]',
    ]
    for sel in selector_candidates:
        try:
            el = soup.select_one(sel)
            if el and el not in excluded_elements:
                val = _finalize_author_candidate(el.get_text(" ", strip=True))
                if val:
                    logger.info("[AuthorExtract] selector match | url=%r selector=%r author=%r", url, sel, val)
                    return val
        except Exception:
            continue

    try:
        text = search_root.get_text(" ", strip=True) if search_root is not None else soup.get_text(" ", strip=True)
        res = extract_author_from_text(text)
        if res:
            logger.info("[AuthorExtract] text fallback | url=%r author=%r", url, res)
            return res
    except Exception:
        pass

    try:
        if _is_detailish_url(url):
            site_name = _resolve_site_name(url, html)
            if site_name:
                logger.info("[AuthorExtract] site fallback | url=%r author=%r", url, site_name)
                return site_name
    except Exception:
        pass

    return None


def extract_department_from_text(text: str) -> Optional[str]:
    """
    텍스트에서 '부서/담당부서'를 best-effort로 추출한다.
    - author와 분리 보존을 위해 department만 따로 뽑아내는 용도
    """
    if not text:
        return None

    s = re.sub(r"[\r\n]+", " ", str(text))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None

    # 하단 정보영역(문의/전화) 기반 우선 추출
    cand0 = _extract_dept_from_footerish_text(s)
    if cand0:
        return cand0

    boundary_tokens = (
        "작성자", "등록자", "등록인", "작성인", "담당부서", "주관부서", "시행부서", "부서", "부서명", "작성부서", "작성부서명",
        "담당과", "담당팀", "담당기관", "담당자", "성명", "글쓴이",
        "직책", "작성자유형",
        "전화번호", "tel", "fax",
        "등록일", "작성일", "게시일", "수정일",
        "조회수", "조회",
        "첨부파일", "첨부",
    )
    dept_keywords = (
        "담당부서",
        "주관부서",
        "시행부서",
        "부서",
        "부서명",
        "작성부서",
        "작성부서명",
        "담당과",
        "담당팀",
        "담당기관",
        "department",
    )
    dept_pattern = (
        r"(?:" + "|".join(dept_keywords) + r")\s*[:\s]*"
        r"(.{2,80}?)"
        r"(?=\s*(?:" + "|".join(boundary_tokens) + r"|\||/|$))"
    )
    for m in re.finditer(dept_pattern, s, flags=re.IGNORECASE):
        res = _finalize_author_candidate(m.group(1).strip())
        if res and _is_valid_department_value(res):
            logger.info(f"[DeptExtract] 텍스트 매칭 성공 | department={res!r}")
            return res
    return None


def extract_department_from_html(html: str, url: Optional[str] = None) -> Optional[str]:
    """
    HTML?? '??/????'? best-effort? ????.
    - extract_author_from_html? ?? ???(author)? ?? fallback? ??? ? ???,
      ? ??? department? ??? ????.
    """
    if not _author_meta_extraction_enabled():
        return None
    if not html:
        return None
    if _is_listish_author_meta_url(url):
        return None
    if url and not is_detail_page_url(str(url)):
        return None

    if not BeautifulSoup:
        try:
            segment = _extract_relevant_html_segment(html)
            text = re.sub(r"<[^>]+>", " ", segment)
            text = re.sub(r"\s+", " ", text).strip()
            return extract_department_from_text(text)
        except Exception:
            return None

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    try:
        if _is_sungdong_family_url(url):
            sd_info = _extract_sungdong_author_info_from_html(html)
            sd_department = sd_info.get("department")
            if sd_department:
                logger.info(
                    "[DeptExtract][Sungdong] meta block match | url=%r department=%r",
                    url,
                    sd_department,
                )
                return sd_department
    except Exception:
        pass

    excluded_selectors = ["nav", "header", "footer", "aside"]
    excluded_elements = set()
    for sel in excluded_selectors:
        for el in soup.select(sel):
            excluded_elements.add(el)

    search_root = None
    for sel in ("article", "main", "#contents", "#content", ".contents", ".sub_contents", ".container", "body"):
        found = soup.select_one(sel)
        if found:
            search_root = found
            break
    if not search_root:
        search_root = soup

    try:
        if url:
            from backend.board.yongin_board import (
                extract_yongin_general_department,
                is_yongin_empmntestinfo_url,
                is_yongin_general_bbs_url,
            )

            if is_yongin_empmntestinfo_url(str(url)):
                yongin_dept = (extract_yongin_general_department(soup) or "").strip()
                if yongin_dept:
                    logger.info("[DeptExtract][YonginEmpmn] table match | url=%r department=%r", url, yongin_dept)
                    return yongin_dept

            if is_yongin_general_bbs_url(str(url)):
                yongin_dept = (extract_yongin_general_department(soup) or "").strip()
                if yongin_dept and _is_valid_department_value(yongin_dept):
                    logger.info("[DeptExtract][Yongin] article-header match | url=%r department=%r", url, yongin_dept)
                    return yongin_dept
    except Exception:
        pass

    try:
        if url and _is_dongjak_portal_bbs_view_url(url):
            dj_dept = (extract_dongjak_portal_department(soup) or "").strip()
            if dj_dept and _is_valid_department_value(dj_dept):
                logger.info("[DeptExtract][Dongjak] portal meta match | url=%r department=%r", url, dj_dept)
                return dj_dept
    except Exception:
        pass

    dept_labels = (
        "부서",
        "담당부서",
        "주관부서",
        "수행부서",
        "부서명",
        "작성부서",
        "작성부서명",
        "담당과",
        "담당팀",
        "담당기관",
        "department",
    )
    semantic_department_raw = _extract_label_value_from_tableish(search_root, *dept_labels)
    if semantic_department_raw:
        semantic_department = _finalize_author_candidate(semantic_department_raw)
        if semantic_department and _is_valid_explicit_department_value(semantic_department):
            logger.info("[DeptExtract] semantic table match | url=%r department=%r", url, semantic_department)
            return semantic_department

    # try:
    #     if _is_detailish_url(url):
    #         for el in search_root.find_all(["div", "dl", "ul", "li", "p", "table", "section", "td"]):
    #             if el in excluded_elements:
    #                 continue
    #             t = el.get_text(" ", strip=True)
    #             if not t:
    #                 continue
    #             if ("문의" in t) or ("전화" in t) or ("tel" in t.lower()) or re.search(r"\d{2,3}-\d{3,4}-\d{4}", t):
    #                 cand = _extract_dept_from_footerish_text(t)
    #                 if cand:
    #                     logger.info("[DeptExtract] Tier 0 footer match | url=%r department=%r", url, cand)
    #                     return cand
    # except Exception:
    #     pass

    for label in dept_labels:
        norm_label = _norm_label(label)
        for el in search_root.find_all(["dt", "th", "td", "span", "label", "div", "p", "li"]):
            if el in excluded_elements:
                continue
            t = el.get_text(" ", strip=True)
            if not t:
                continue
            nt = _norm_label(t)
            if nt != norm_label:
                m_inline = re.search(rf"{re.escape(label)}\s*[:\s]+(.{{2,80}})$", t, flags=re.IGNORECASE)
                if m_inline:
                    cand = _finalize_author_candidate(m_inline.group(1))
                    if cand and _is_valid_explicit_department_value(cand):
                        logger.info("[DeptExtract] inline label match | url=%r label=%r department=%r", url, label, cand)
                        return cand
                continue

            nxt = el.find_next_sibling(["dd", "td", "span", "div"])
            if nxt and nxt not in excluded_elements:
                cand = _finalize_author_candidate(nxt.get_text(" ", strip=True))
                if cand and _is_valid_explicit_department_value(cand):
                    logger.info("[DeptExtract] sibling label match | url=%r label=%r department=%r", url, label, cand)
                    return cand

            parent = el.parent
            if parent and parent not in excluded_elements:
                combined = parent.get_text(" ", strip=True)
                m = re.search(rf"{re.escape(label)}\s*[:\s]*([^:|]{{2,80}})", combined)
                if m:
                    cand = _finalize_author_candidate(m.group(1))
                    if cand and _is_valid_explicit_department_value(cand):
                        logger.info("[DeptExtract] parent label match | url=%r label=%r department=%r", url, label, cand)
                        return cand

    try:
        text = search_root.get_text(" ", strip=True) if search_root is not None else soup.get_text(" ", strip=True)
        dept = extract_department_from_text(text)
        if dept:
            return dept
    except Exception:
        pass

    try:
        if _is_detailish_url(url):
            site_name = _resolve_site_name(url, html)
            if site_name:
                logger.info("[DeptExtract] site fallback | url=%r department=%r", url, site_name)
                return site_name
    except Exception:
        pass
    return None


def extract_author_info_from_html(
    html: str,
    url: Optional[str] = None,
    *,
    meta_root_selector: Optional[str] = None,
    author_selector: Optional[str] = None,
    department_selector: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """
    author/department를 동시에 추출해 의미를 보존한다.
    반환:
    - author: 최종 표시용 작성 주체 (작성자 우선, 없으면 부서)
    - department: 부서(담당부서)만 별도 추출
    - author_kind: person/org/unknown (best-effort)
    """
    # selector hint 우선 적용(없으면 기존 휴리스틱)
    if not _author_meta_extraction_enabled() or _is_listish_author_meta_url(url):
        return {
            "author": None,
            "department": None,
            "author_raw": None,
            "department_raw": None,
            "author_kind": None,
        }

    author: Optional[str] = None
    department: Optional[str] = None
    author_raw: Optional[str] = None
    department_raw: Optional[str] = None
    author_kind_hint: Optional[str] = None
    sungdong_info: Dict[str, Optional[str]] = {}

    if (not author or not department) and url and is_gm_nftc_bbs_url(url) and BeautifulSoup and html:
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
            table = soup.select_one("table.bbsView") or soup.select_one("table.table_style2")
            gm_department = _extract_label_value_from_tableish(table, "담당부서")
            if gm_department:
                gm_department = _finalize_author_candidate(gm_department)
            if gm_department:
                author = author or gm_department
                department = department or gm_department
                author_raw = author_raw or gm_department
                department_raw = department_raw or gm_department
                author_kind_hint = author_kind_hint or "org"
                logger.info(
                    "[AuthorExtract][GM_NFTC] table match | url=%r author=%r department=%r",
                    url,
                    author,
                    department,
                )
        except Exception:
            pass

    if not (meta_root_selector or author_selector or department_selector):
        try:
            if (
                _is_detailish_url(url)
                and not _is_dongjak_portal_bbs_view_url(url)
                and not _has_author_label_hint(html or "")
            ):
                site_name = _resolve_site_name(url, html or "")
                if site_name:
                    return {
                        "author": site_name,
                        "department": site_name,
                        "author_raw": site_name,
                        "department_raw": site_name,
                        "author_kind": "org",
                    }
        except Exception:
            pass

    if BeautifulSoup and html and (meta_root_selector or author_selector or department_selector):
        try:
            soup = BeautifulSoup(_extract_relevant_html_segment(html), "html.parser")  # type: ignore[operator]
            root = soup
            if meta_root_selector:
                try:
                    root = soup.select_one(str(meta_root_selector)) or soup
                except Exception:
                    root = soup

            if author_selector:
                try:
                    el = root.select_one(str(author_selector))
                    if el is not None:
                        author = DataStandardizer.standardize_author(el.get_text(" ", strip=True))
                except Exception:
                    pass
            if department_selector:
                try:
                    el = root.select_one(str(department_selector))
                    if el is not None:
                        department = DataStandardizer.standardize_author(el.get_text(" ", strip=True))
                except Exception:
                    pass
        except Exception:
            pass

    if (not author or not department) and _is_sungdong_family_url(url):
        try:
            sungdong_info = _extract_sungdong_author_info_from_html(html)
            if not author and sungdong_info.get("author"):
                author = sungdong_info.get("author")
            if not department and sungdong_info.get("department"):
                department = sungdong_info.get("department")
            if author or department:
                logger.info(
                    "[AuthorExtract][Sungdong] author_info merged | url=%r author=%r department=%r",
                    url,
                    author,
                    department,
                )
        except Exception:
            sungdong_info = {}

    if (not author or not department) and url and "nowon.kr" in str(url).lower():
        try:
            nowon_info = _extract_nowon_author_info_from_html(html)
            if not author and nowon_info.get("author"):
                author = nowon_info.get("author")
            if not department and nowon_info.get("department"):
                department = nowon_info.get("department")
            author_raw = author_raw or nowon_info.get("author_raw")
            department_raw = department_raw or nowon_info.get("department_raw")
            if author or department:
                logger.info(
                    "[AuthorExtract][Nowon] meta table match | url=%r author=%r department=%r",
                    url,
                    author,
                    department,
                )
        except Exception:
            pass

    if (not author or not department) and url and "gwangjin.go.kr" in str(url).lower():
        try:
            gwangjin_info = _extract_gwangjin_author_info_from_html(html)
            if not author and gwangjin_info.get("author"):
                author = gwangjin_info.get("author")
            if not department and gwangjin_info.get("department"):
                department = gwangjin_info.get("department")
            author_raw = author_raw or gwangjin_info.get("author_raw")
            department_raw = department_raw or gwangjin_info.get("department_raw")
            if author or department:
                author_kind_hint = author_kind_hint or "org"
                logger.info(
                    "[AuthorExtract][Gwangjin] status dl match | url=%r author=%r department=%r",
                    url,
                    author,
                    department,
                )
        except Exception:
            pass

    if (not author or not department) and url and "miryang.go.kr" in str(url).lower():
        try:
            miryang_info = _extract_miryang_author_info_from_html(html)
            if not author and miryang_info.get("author"):
                author = miryang_info.get("author")
            if not department and miryang_info.get("department"):
                department = miryang_info.get("department")
            author_raw = author_raw or miryang_info.get("author_raw")
            department_raw = department_raw or miryang_info.get("department_raw")
            if author or department:
                logger.info(
                    "[AuthorExtract][Miryang] headinfo detail match | url=%r author=%r department=%r",
                    url,
                    author,
                    department,
                )
        except Exception:
            pass

    # 강남구청 본청 /apply/.../view.do : 본문에 '신청 완료' 등이 있어 작성자 휴리스틱이 오탐 → 푸터 담당부서만 사용
    if (not author or not department) and url and "sb.go.kr" in str(url).lower():
        try:
            from backend.board.seongbuk_board import (
                extract_seongbuk_author_info,
                is_seongbuk_bbs_view_url,
                is_seongbuk_eminwon_view_url,
                is_seongbuk_yeyak_program_view_url,
            )

            if BeautifulSoup and html and (
                is_seongbuk_bbs_view_url(str(url))
                or is_seongbuk_eminwon_view_url(str(url))
                or is_seongbuk_yeyak_program_view_url(str(url))
            ):
                sb_soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
                sb_info = extract_seongbuk_author_info(sb_soup, url=str(url))
                if not author and sb_info.get("author"):
                    author = sb_info.get("author")
                if not department and sb_info.get("department"):
                    department = sb_info.get("department")
                author_raw = author_raw or sb_info.get("author_raw")
                department_raw = department_raw or sb_info.get("department_raw")
                author_kind_hint = author_kind_hint or sb_info.get("author_kind")
                if author or department:
                    logger.info(
                        "[AuthorExtract][Seongbuk] meta table match | url=%r author=%r department=%r",
                        url,
                        author,
                        department,
                    )
        except Exception:
            pass

    if (not author or not department) and url and "guro.go.kr" in str(url).lower():
        try:
            from backend.board.guro_board import (
                extract_guro_author_info,
                is_guro_lecture_view_url,
                is_guro_propse_view_url,
            )

            if BeautifulSoup and html and (
                is_guro_lecture_view_url(str(url)) or is_guro_propse_view_url(str(url))
            ):
                guro_soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
                guro_info = extract_guro_author_info(guro_soup, url=str(url))
                if not author and guro_info.get("author"):
                    author = guro_info.get("author")
                if not department and guro_info.get("department"):
                    department = guro_info.get("department")
                author_raw = author_raw or guro_info.get("author_raw")
                department_raw = department_raw or guro_info.get("department_raw")
                author_kind_hint = author_kind_hint or guro_info.get("author_kind")
                if author or department:
                    logger.info(
                        "[AuthorExtract][Guro] site-specific author match | url=%r author=%r department=%r",
                        url,
                        author,
                        department,
                    )
        except Exception:
            pass

    if (not author or not department) and url and "jongno.go.kr" in str(url).lower():
        try:
            from backend.board.jongno_board import extract_jongno_author_info, is_jongno_board_article_url

            if BeautifulSoup and html and is_jongno_board_article_url(str(url)):
                jongno_soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
                jongno_info = extract_jongno_author_info(jongno_soup, str(url))
                if not author and jongno_info.get("author"):
                    author = jongno_info.get("author")
                if not department and jongno_info.get("department"):
                    department = jongno_info.get("department")
                author_raw = author_raw or jongno_info.get("author_raw")
                department_raw = department_raw or jongno_info.get("department_raw")
                author_kind_hint = author_kind_hint or jongno_info.get("author_kind")
                if author or department:
                    logger.info(
                        "[AuthorExtract][Jongno] site-specific author match | url=%r author=%r department=%r",
                        url,
                        author,
                        department,
                )
        except Exception:
            pass

    if (not author or not department) and url and _is_dongjak_portal_bbs_view_url(url):
        try:
            if BeautifulSoup and html:
                dongjak_soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
                dongjak_info = extract_dongjak_portal_author_info(dongjak_soup)
                if not author and dongjak_info.get("author"):
                    author = dongjak_info.get("author")
                if not department and dongjak_info.get("department"):
                    department = dongjak_info.get("department")
                author_raw = author_raw or dongjak_info.get("author_raw")
                department_raw = department_raw or dongjak_info.get("department_raw")
                author_kind_hint = author_kind_hint or dongjak_info.get("author_kind")
                if author or department:
                    logger.info(
                        "[AuthorExtract][Dongjak] portal viewInfo match | url=%r author=%r department=%r",
                        url,
                        author,
                        department,
                    )
        except Exception:
            pass

    if not author and not department:
        try:
            from backend.board.gangnam_board import (
                gangnam_main_apply_footer_department,
                is_gangnam_main_apply_view_url,
            )

            if url and is_gangnam_main_apply_view_url(url) and BeautifulSoup and html:
                # segment는 id=container 등 중간부터 잘라 푸터 DOM이 깨질 수 있어 전체 HTML로 담당부서만 조회
                full_soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
                gn_dept = gangnam_main_apply_footer_department(full_soup)
                if gn_dept:
                    author = DataStandardizer.standardize_author(gn_dept) or gn_dept.strip()
                    department = author
        except Exception:
            pass

    if not author:
        try:
            if url:
                from backend.board.yongin_board import (
                    extract_yongin_general_department,
                    is_yongin_empmntestinfo_url,
                )

                if is_yongin_empmntestinfo_url(str(url)) and BeautifulSoup and html:
                    yongin_soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
                    yongin_dept = (extract_yongin_general_department(yongin_soup) or "").strip()
                    if yongin_dept:
                        department = department or yongin_dept
                        department_raw = department_raw or yongin_dept
                        author = yongin_dept
                        author_raw = author_raw or yongin_dept
                        author_kind_hint = author_kind_hint or "org"
        except Exception:
            pass

    if not author:
        author = extract_author_from_html(html, url=url)
    if author and not department and _is_valid_explicit_department_value(author):
        department = author
    if not department:
        department = extract_department_from_html(html, url=url)
    author_raw = author_raw or extract_author_raw_from_html(html)
    department_raw = department_raw or extract_department_raw_from_html(html)
    if sungdong_info:
        author_raw = author_raw or sungdong_info.get("author_raw")
        department_raw = department_raw or sungdong_info.get("department_raw")
    if author and not author_raw:
        author_raw = author
    if department and not department_raw:
        department_raw = department

    is_yongin_empmntestinfo_author_url = False
    try:
        if url:
            from backend.board.yongin_board import is_yongin_empmntestinfo_url

            is_yongin_empmntestinfo_author_url = is_yongin_empmntestinfo_url(str(url))
    except Exception:
        is_yongin_empmntestinfo_author_url = False
    is_gm_nftc_author_url = bool(url and is_gm_nftc_bbs_url(url))

    if author and (_is_obviously_noisy_meta_value(author) or _is_meta_label_only_value(author)):
        author = None
    if author_raw and (_is_obviously_noisy_meta_value(author_raw) or _is_meta_label_only_value(author_raw)):
        author_raw = None
    if department and (
        _is_meta_label_only_value(department)
        or (
            not is_yongin_empmntestinfo_author_url
            and not is_gm_nftc_author_url
            and not _is_valid_explicit_department_value(department)
        )
    ):
        department = None
    if department_raw and (
        _is_meta_label_only_value(department_raw)
        or (
            not is_yongin_empmntestinfo_author_url
            and not is_gm_nftc_author_url
            and not _is_valid_explicit_department_value(department_raw)
        )
    ):
        department_raw = None

    if is_gm_nftc_author_url:
        if not department and department_raw:
            department = department_raw
        if not author and author_raw:
            author = author_raw
        author_kind_hint = author_kind_hint or "org"

    if author and department:
        try:
            from backend.board.jongno_board import is_jongno_construction_status_url

            site_name = _resolve_site_name(url, html)
            if url and is_jongno_construction_status_url(str(url)) and site_name and department == site_name:
                department = None
                department_raw = None
        except Exception:
            pass

    if not department and department_raw:
        restored_department = _finalize_author_candidate(department_raw)
        if restored_department and _is_valid_explicit_department_value(restored_department):
            department = restored_department

    if not author and author_raw:
        restored_author = _finalize_author_candidate(author_raw)
        if restored_author and not _is_meta_label_only_value(restored_author):
            author = restored_author

    author_kind: Optional[str] = author_kind_hint
    if author and department and author == department:
        author_kind = "org"
    elif department and not author:
        author_kind = "org"
    elif author and _is_site_name_like(author):
        author_kind = "org"
    elif author and not department:
        # 사람/기관 구분이 어려워 보수적으로 unknown 처리 가능하나,
        # 기존 UI/필드 의미상 '작성자'가 잡힌 케이스가 많아 person을 기본으로 둔다.
        author_kind = author_kind or "person"
    elif author:
        author_kind = author_kind or "unknown"

    # 작성자 미기재 시 주관부서·담당부서 등으로 표시용 author 보강
    if (not author) and department:
        used_site_name_fallback = False
        try:
            if url:
                from backend.board.yongin_board import is_yongin_general_bbs_url

                if is_yongin_general_bbs_url(str(url)):
                    site_name = _resolve_site_name(url, html)
                    if site_name:
                        author = site_name
                        author_raw = author_raw or site_name
                        author_kind = "org"
                        used_site_name_fallback = True
        except Exception:
            pass
        if not used_site_name_fallback:
            author = DataStandardizer.standardize_author(department) or department.strip()
            author_kind = author_kind or "org"

    # ✅ 요구사항: 작성자/부서 정보를 못 찾는 경우 사이트명을 기본값으로 사용
    # - 예: 성동구청, 광진구청
    # - 비-상세 페이지(contents.do 등)는 오탐 위험이 크므로 기본값을 주입하지 않는다.
    try:
        # NOTE: is_detail_page_url()은 'id=' 같은 범용 파라미터 때문에 오탐이 있을 수 있어,
        # 여기서는 board_meta_extractor의 보수적 휴리스틱(_is_detailish_url)을 사용한다.
        if _is_detailish_url(url):
            site_name = _resolve_site_name(url, html)
            if site_name:
                if not author and not department:
                    department = site_name
                    department_raw = department_raw or site_name
                if not author:
                    author = site_name
                    author_raw = author_raw or site_name
                if author or department:
                    author_kind = author_kind or "org"
    except Exception:
        pass

    # site fallback가 author/department를 각각 채운 뒤에도 분류가 이전 값(person)으로 남을 수 있어
    # 최종 반환 직전에 한 번 더 정규화한다.
    author_cmp = (author or "").strip()
    department_cmp = (department or "").strip()
    if author_cmp and department_cmp and author_cmp == department_cmp:
        author_kind = "org"
    elif department_cmp and not author_cmp:
        author_kind = "org"
    elif author_cmp and not department_cmp and author_kind is None:
        author_kind = "person"

    return {
        "author": author,
        "department": department,
        "author_raw": author_raw,
        "department_raw": department_raw,
        "author_kind": author_kind,
    }


def extract_contact_views_from_html(
    html: str,
    url: Optional[str] = None,
    *,
    meta_root_selector: Optional[str] = None,
    phone_selector: Optional[str] = None,
    view_selector: Optional[str] = None,
) -> Dict[str, Any]:
    """
    상세페이지 HTML에서 연락처(전화번호)와 조회수 등을 best-effort로 추출한다.
    - 사이트별 DOM 편차가 커서 정규식 + 간단한 라벨/형제 탐색을 혼합한다.

    반환 키(없으면 None):
    - contact_phone: "02-1234-5678"
    - view_count: int
    """
    if not html:
        return {"contact_phone": None, "view_count": None}

    # 0) selector hint 우선 적용(없으면 기존 휴리스틱)
    if BeautifulSoup and html and (meta_root_selector or phone_selector or view_selector):
        try:
            soup = BeautifulSoup(_extract_relevant_html_segment(html), "html.parser")  # type: ignore[operator]
            root = soup
            if meta_root_selector:
                try:
                    root = soup.select_one(str(meta_root_selector)) or soup
                except Exception:
                    root = soup

            phone_val = None
            view_val = None
            if phone_selector:
                try:
                    el = root.select_one(str(phone_selector))
                    if el is not None:
                        t = el.get_text(" ", strip=True)
                        m = re.search(r"([0-9]{2,3}[-\s]?[0-9]{3,4}[-\s]?[0-9]{4})", t)
                        if m:
                            phone_val = re.sub(r"\s+", "", m.group(1)).replace(" ", "")
                except Exception:
                    pass
            if view_selector:
                try:
                    el = root.select_one(str(view_selector))
                    if el is not None:
                        t = el.get_text(" ", strip=True)
                        m = re.search(r"([0-9][0-9,]{0,10})", t)
                        if m:
                            vraw = (m.group(1) or "").replace(",", "").strip()
                            if vraw.isdigit():
                                view_val = int(vraw)
                except Exception:
                    pass

            # 힌트로 둘 다 못 뽑으면 기존 휴리스틱으로 계속 진행
            if phone_val is not None or view_val is not None:
                return {"contact_phone": phone_val, "view_count": view_val}
        except Exception:
            pass

    # 1) 범용 텍스트 기반 (라벨 근처 우선)
    segment = _extract_relevant_html_segment(html)
    debug_on = str(os.getenv("DEBUG_ATTACHMENT_EXTRACT", "0")).strip().lower() in ("1", "true", "yes", "on")
    # soup가 가능하면 텍스트를 정제
    if BeautifulSoup:
        try:
            soup = BeautifulSoup(segment, "html.parser")  # type: ignore[operator]
            text = soup.get_text(" ", strip=True)
        except Exception:
            text = re.sub(r"<[^>]+>", " ", segment)
            text = re.sub(r"\s+", " ", text).strip()
    else:
        text = re.sub(r"<[^>]+>", " ", segment)
        text = re.sub(r"\s+", " ", text).strip()

    phone: Optional[str] = None
    view_count: Optional[int] = None

    try:
        m = re.search(
            r"(?:전화번호|연락처|문의(?:전화)?|tel|TEL|☎)\s*[:：]?\s*([0-9]{2,3}[-\s]?[0-9]{3,4}[-\s]?[0-9]{4})",
            text,
        )
        if m:
            phone = re.sub(r"\s+", "", m.group(1)).replace(" ", "")
    except Exception:
        pass
    if not phone:
        try:
            m2 = re.search(r"([0-9]{2,3}-[0-9]{3,4}-[0-9]{4})", text)
            if m2:
                phone = m2.group(1)
        except Exception:
            pass

    try:
        mv = re.search(r"(?:조회수|조회)\s*[:：]?\s*([0-9][0-9,]{0,10})", text)
        if mv:
            vraw = (mv.group(1) or "").replace(",", "").strip()
            if vraw.isdigit():
                view_count = int(vraw)
    except Exception:
        pass

    # 2) DOM 라벨/형제 기반 보강 (텍스트 기반이 실패했을 때만)
    if BeautifulSoup and (phone is None or view_count is None):
        try:
            soup2 = BeautifulSoup(segment, "html.parser")  # type: ignore[operator]

            def _find_labeled_value(labels: tuple[str, ...]) -> Optional[str]:
                for el in soup2.find_all(["dt", "th", "td", "span", "label", "div", "p", "li"]):
                    t = el.get_text(" ", strip=True)
                    if not t:
                        continue
                    for lab in labels:
                        if lab in t:
                            # same element inline
                            m_inline = re.search(rf"{re.escape(lab)}\s*[:：]?\s*(.+)$", t)
                            if m_inline and m_inline.group(1).strip():
                                return m_inline.group(1).strip()
                            # sibling
                            sib = el.find_next_sibling(["dd", "td", "span", "div", "p"])
                            if sib:
                                sv = sib.get_text(" ", strip=True)
                                if sv:
                                    return sv.strip()
                return None

            if phone is None:
                raw = _find_labeled_value(("전화번호", "연락처", "문의", "TEL", "tel", "☎"))
                if raw:
                    m = re.search(r"([0-9]{2,3}[-\s]?[0-9]{3,4}[-\s]?[0-9]{4})", raw)
                    if m:
                        phone = re.sub(r"\s+", "", m.group(1)).replace(" ", "")

            if view_count is None:
                rawv = _find_labeled_value(("조회수", "조회"))
                if rawv:
                    m = re.search(r"([0-9][0-9,]{0,10})", rawv)
                    if m:
                        vraw = (m.group(1) or "").replace(",", "").strip()
                        if vraw.isdigit():
                            view_count = int(vraw)
        except Exception as exc:
            logger.debug("[MetaExtract] contact/views DOM fallback failed | url=%r err=%s", url, exc)

    return {"contact_phone": phone, "view_count": view_count}


def extract_attachment_summary_from_html(
    html: str,
    url: Optional[str] = None,
    *,
    attachment_selector: Optional[str] = None,
    use_full_html: bool = False,
    same_article_only: bool = False,
) -> Dict[str, Any]:
    """
    상세페이지 HTML에서 첨부파일 요약을 best-effort로 추출한다.

    반환:
    - attachment_count: int
    - attachments: list[dict] (가능한 경우만)
      - name: str
      - href: str (상대경로일 수 있음)

    use_full_html: True면 segment 잘림 없이 전체 HTML 사용(스크립트 등에서 첨부가 1개만 나올 때 재시도용).
    same_article_only: True이고 url에 bbsNo/nttNo가 있으면 같은 게시글 첨부 링크만 수집(전체 HTML 사용 시 다른 글/메뉴 링크 제외).
    """
    if not html:
        return {"attachment_count": 0, "attachments": []}
    if "photo.hscity.go.kr" in str(url or "").lower():
        return {"attachment_count": 0, "attachments": []}
    if is_gm_nftc_bbs_url(url):
        gm_items = extract_gm_nftc_filelist_attachments(html, url)
        if gm_items:
            return {"attachment_count": len(gm_items), "attachments": gm_items}

    debug_on = str(os.getenv("DEBUG_ATTACHMENT_EXTRACT", "0")).strip().lower() in ("1", "true", "yes", "on")
    segment = html if use_full_html else _extract_relevant_html_segment(html)
    page_bbs, page_ntt = "", ""
    if same_article_only and url:
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            # 대소문자 무시를 위해 모든 키를 소문자로 변환한 딕셔너리 생성
            qs_low = {k.lower(): v for k, v in qs.items()}
            page_bbs = (qs_low.get("bbsno") or qs_low.get("bbsid") or [""])[0]
            page_ntt = (qs_low.get("nttno") or qs_low.get("nttid") or [""])[0]
        except Exception:
            pass

    # BeautifulSoup 사용 가능 시: 라벨('첨부파일') 주변 컨테이너를 우선 탐색
    if BeautifulSoup:
        try:
            soup = BeautifulSoup(segment, "html.parser")  # type: ignore[operator]
        except Exception:
            soup = None
        if soup is not None:
            # 1) "첨부파일" 텍스트 노드 주변 컨테이너 찾기
            attach_root = None
            def _has_file_like_links(node) -> bool:
                if node is None:
                    return False
                file_hint = (
                    "filedown", "download", "downloadbbsfile", "atchmnflno",
                    "atchfile", "atchfileid", "fileid", "filedown.do", "filedown.jsp",
                    "/file/download/", "download/uu/",
                )
                try:
                    for a in node.find_all("a", href=True):
                        href = normalize_attachment_href(a.get("href") or "")
                        if not href or href.startswith("#"):
                            continue
                        lh = href.lower()
                        if lh.startswith("javascript:") and "preview" in lh:
                            continue
                        if "previewbbs" in lh:
                            continue
                        if any(tok in lh for tok in file_hint):
                            return True
                    return False
                except Exception:
                    return False
            try:
                for tnode in soup.find_all(string=True):
                    txt = (str(tnode) or "").strip()
                    if not txt:
                        continue
                    if "첨부파일" in txt:
                        # ✅ 중요: 라벨이 <dt>인 경우가 많아서 parent(dt)만 잡으면 링크가 없는 케이스가 발생한다.
                        # 따라서 dt → 다음 dd 또는 dl(fileSet) 컨테이너로 승격한다.
                        attach_root = tnode.parent
                        try:
                            if getattr(attach_root, "name", None) == "dt":
                                dd = attach_root.find_next_sibling("dd")
                                if dd is not None:
                                    attach_root = dd
                                elif getattr(attach_root, "parent", None) is not None:
                                    attach_root = attach_root.parent
                            # dl/fileSet 같은 상위 컨테이너가 명확하면 한 단계 더 올린다.
                            if getattr(attach_root, "name", None) in ("dd", "dt") and getattr(attach_root, "parent", None) is not None:
                                parent = attach_root.parent
                                if getattr(parent, "name", None) == "dl":
                                    attach_root = parent
                            # ✅ 테이블: "첨부파일"/"파일"이 th/td/tr 안에 있으면 해당 테이블 전체로 확장해 여러 행의 첨부를 모두 수집
                            if getattr(attach_root, "name", None) in ("th", "td", "tr"):
                                table = attach_root.find_parent("table")
                                if table is not None:
                                    attach_root = table
                        except Exception:
                            pass
                        # 본문 문구("<첨부파일을 참조하세요>" 등)만 먼저 잡히는 경우
                        # 실제 다운로드 링크가 없으면 fallback 탐색으로 넘긴다.
                        if not _has_file_like_links(attach_root):
                            attach_root = None
                            continue
                        break
            except Exception:
                attach_root = None
            # 0) 학습된 selector가 있으면 최우선 적용(사이트별 힌트를 범용적으로 수용)
            if attachment_selector and attach_root is None:
                try:
                    attach_root = soup.select_one(str(attachment_selector))
                except Exception:
                    attach_root = None
            if attach_root is None:
                # 2) 흔한 클래스명/영역 후보 (지자체 표준 p-attach 등 추가)
                for sel in (
                    ".p-attach",
                    ".file_area",
                    ".file_list",
                    ".attach",
                    ".attachment",
                    ".bbs_file",
                    ".bbs-file",
                    ".filebox",
                    ".fileBox",
                    ".file",
                ):
                    try:
                        el = soup.select_one(sel)
                    except Exception:
                        el = None
                    if el is not None:
                        attach_root = el
                        break
                try:
                    if getattr(attach_root, "name", None) is not None:
                        classes = attach_root.get("class") or []
                        if "file" in classes and getattr(attach_root, "parent", None) is not None:
                            siblings = attach_root.parent.select(":scope > .file")
                            if len(siblings) > 1:
                                attach_root = attach_root.parent
                except Exception:
                    pass
            if attach_root is None:
                # 3) 테이블 헤더에 "파일"/"첨부파일"이 있는 테이블 → **파일 링크가 가장 많은** 테이블 선택
                _file_hint = ("filedown", "download", "downloadbbsfile", "atchmnflno", "atchfile", "filedown.do", "filedown.jsp", "p-attach__link")
                best_table = None
                best_count = 0
                for table in soup.find_all("table"):
                    for th in table.find_all("th"):
                        t = (th.get_text(" ", strip=True) or "").strip()
                        if "첨부파일" in t or t == "파일":
                            n = sum(1 for a in table.find_all("a", href=True) if (a.get("href") or "").lower().startswith(("javascript:", "#")) is False and any(h in (a.get("href") or "").lower() for h in _file_hint))
                            if n > best_count:
                                best_count = n
                                best_table = table
                            break
                if best_table is not None:
                    attach_root = best_table
            if attach_root is None:
                attach_root = soup

            # ✅ 첨부파일 영역이 명확한 경우(p-attach 등) 해당 영역 내부를 강제 우선 탐색
            # 만약 테이블 내부에 파일이 1개뿐인데 외부에 더 많은 p-attach__link가 있다면 soup로 확장
            if getattr(attach_root, "name", None) == "table":
                _file_hint2 = ("filedown", "download", "downloadbbsfile", "atchmnflno", "atchfile", "filedown.do", "filedown.jsp", "p-attach__link")
                table_file_count = sum(1 for a in attach_root.find_all("a", href=True) if any(h in (a.get("href") or "").lower() for h in _file_hint2))
                if table_file_count <= 1 and soup.select(".p-attach__link"):
                    attach_root = soup

            attachments: list[dict] = []
            seen_keys: set[str] = set()
            # 파일 확장자 힌트(첨부파일에 흔함)
            # 파일 확장자 힌트(첨부파일에 흔함) - 파일명 끝에 [1.2MB] 등 노이즈가 있을 수 있으므로 $ 제거
            ext_pat = re.compile(r"\.(pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|zip|rar|7z|txt|jpg|jpeg|png|gif)(?:\W|$)", re.IGNORECASE)
            file_pat = re.compile(r"\b([^\s\"'<>]{1,160}\.(?:pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|zip|rar|7z|txt|jpg|jpeg|png|gif))\b", re.IGNORECASE)

            if (
                "k-cohesion.go.kr" in str(url or "").lower()
                and "/pcnc/contents/" in str(url or "").lower()
            ):
                title = ""
                try:
                    title_el = soup.select_one(".board_detail_wrap .detail_tit, .detail_tit")
                    title = re.sub(r"\s+", " ", title_el.get_text(" ", strip=True)).strip() if title_el else ""
                except Exception:
                    title = ""
                photo_idx = 0
                for inp in soup.select('input[id^="photoMask"][value]'):
                    try:
                        mask = str(inp.get("value") or "").strip()
                    except Exception:
                        mask = ""
                    if not mask:
                        continue
                    photo_idx += 1
                    href = urljoin(url or "", f"/comm/download.do?f={mask}")
                    name = f"{title} 사진 {photo_idx}".strip() if title else f"photo {photo_idx}"
                    key = name + "|" + href
                    if key not in seen_keys:
                        seen_keys.add(key)
                        attachments.append({"name": name, "href": href})

            # 링크 수집
            if "yongin.go.kr" in str(url or "").lower():
                for a in attach_root.find_all("a"):
                    try:
                        raw_href = (a.get("href") or a.get("data-href") or a.get("data-url") or "").strip()
                        onclick = (a.get("onclick") or "").strip()
                        title_attr = (a.get("title") or "").strip()
                        link_text = re.sub(r"\s+", " ", (a.get_text(" ", strip=True) or "").strip())
                    except Exception:
                        continue
                    href = (
                        resolve_yongin_file_download_url(raw_href, url)
                        or resolve_yongin_file_download_url(onclick, url)
                    )
                    if not href:
                        continue
                    name = link_text or title_attr
                    if (not name) or (not ext_pat.search(name)):
                        try:
                            ctx = a.parent.get_text(" ", strip=True) if a.parent is not None else ""
                        except Exception:
                            ctx = ""
                        mfn = file_pat.search(ctx or "")
                        if mfn:
                            name = mfn.group(1).strip()
                    name = _clean_attachment_display_name(name)
                    key = (name or "") + "|" + href
                    if key not in seen_keys:
                        seen_keys.add(key)
                        attachments.append({"name": name, "href": href})

            for a in attach_root.find_all("a", href=True):
                href = normalize_attachment_href(a.get("href") or "")
                onclick = (a.get("onclick") or "").strip()
                if onclick and (
                    not href
                    or href.startswith("#")
                    or href.lower() in {"javascript:;", "javascript:void(0)", "javascript:void(0);"}
                ):
                    resolved = extract_download_url_from_js(onclick, url) or ""
                    if not resolved:
                        resolved = resolve_anseong_yhlib_download_url(onclick, url) or ""
                    if resolved:
                        href = normalize_attachment_href(resolved)
                if not href or href.startswith("#"):
                    continue
                lh = href.lower()
                # ✅ 요구 기준: 첨부파일 목록은 "실제 다운로드 링크"를 우선한다.
                # - javascript:previewAjax / preListen 같은 액션 링크는 파일명/확장자 표시용이 아니고 중복이므로 기본 제외
                if lh.startswith("javascript:") and "preview" in lh:
                    continue
                # previewBbs.do 는 뷰어용 — 동일 파일의 downloadBbsFile 과 파일명 중복 제거 시 앞선 미리보기만 남는 문제 방지
                if "previewbbs" in lh:
                    continue
                # 다운로드/첨부 링크 휴리스틱 (구로구 등 downloadBbsFile.do?atchmnflNo= 포함, p-attach__link 클래스 포함)
                cls = " ".join(a.get("class", [])).lower()
                looks_like_file = (
                    ("filedown" in lh)
                    or ("download" in lh)
                    or ("downloadbbsfile" in lh)
                    or ("atchmnflno" in lh)
                    or ("atchfile" in lh)
                    or ("atchfileid" in lh)
                    or ("fileid" in lh)
                    or ("filedown.jsp" in lh)
                    or ("filedown.do" in lh)
                    or ("p-attach__link" in cls)
                    or bool(ext_pat.search(lh))
                )
                if not looks_like_file:
                    continue
                if same_article_only and page_bbs and page_ntt and ("bbsno=" in lh and "nttno=" in lh):
                    # lh(.lower())에 "bbsno=702" 등이 포함되는지 확인
                    if f"bbsno={page_bbs}".lower() not in lh or f"nttno={page_ntt}".lower() not in lh:
                        continue

                link_text_skip = (a.get_text(" ", strip=True) or "").strip()
                title_skip = (a.get("title") or "").strip()
                if "바로보기" in link_text_skip or "바로듣기" in link_text_skip or "미리보기" in link_text_skip:
                    continue
                if "바로보기" in title_skip or "바로듣기" in title_skip or "미리보기" in title_skip:
                    continue

                # 파일명 우선순위:
                # 1) a[title] — 실제 파일명(확장자 포함)일 때만. "파일 다운로드"/"새창" 등 UI 문구는 제외
                # 2) 링크 텍스트 전체(공백 포함, 예: "2025년 ... 특정감사 결과.pdf")
                # 3) 주변 텍스트에서 file_pat (공백 없는 토큰만 잡히므로 보조용)
                title_attr = (a.get("title") or "").strip()
                link_text = re.sub(r"\s+", " ", (a.get_text(" ", strip=True) or "").strip())
                if title_attr and ext_pat.search(title_attr):
                    name = clean_anseong_attachment_name(title_attr) if is_anseong_file_url(url) else title_attr
                elif link_text:
                    name = link_text
                elif title_attr:
                    name = title_attr
                else:
                    name = ""

                # 버튼 텍스트(바로보기/바로듣기/미리보기 등)만 있는 링크는 파일명 링크와 중복될 수 있어 제외
                if name in ("바로보기", "바로듣기", "미리보기", "다운로드", "보기", "듣기"):
                    name = ""
                # 파일명이 링크 텍스트에 없으면, 주변 텍스트에서 filename.ext를 추출
                if (not name) or (not ext_pat.search(name)):
                    try:
                        ctx = a.parent.get_text(" ", strip=True) if a.parent is not None else ""
                    except Exception:
                        ctx = ""
                    if ctx:
                        mfn = file_pat.search(ctx)
                        if mfn:
                            name = mfn.group(1).strip()

                name = _clean_attachment_display_name(name)
                key = (name or "") + "|" + href
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                attachments.append({"name": name, "href": href})

            # 텍스트 기반 보강: 첨부영역 텍스트에서 filename.ext를 추가 수집(링크가 버튼뿐인 경우 대비)
            try:
                ctx_all = attach_root.get_text(" ", strip=True)
            except Exception:
                ctx_all = ""
            if ctx_all:
                for m in file_pat.finditer(ctx_all):
                    nm = _clean_attachment_display_name(m.group(1) or "")
                    if not nm:
                        continue
                    key = nm.lower()
                    if key in seen_keys:
                        continue
                    # href를 모르면 빈 값으로 둔다.
                    seen_keys.add(key)
                    attachments.append({"name": nm, "href": ""})

            # name이 비어있으면 href에서 파일명 후보 추출
            for it in attachments:
                if it.get("name"):
                    continue
                try:
                    tail = (it.get("href", "") or "").split("/")[-1]
                    # query 제거
                    tail = tail.split("?")[0]
                    if tail and ext_pat.search(tail):
                        it["name"] = tail[:160]
                except Exception:
                    pass

            # href 없는 보조 행 중, 실제 다운로드 링크 파일명에 이미 포함된 짧은 토큰(예: file_pat만 잡힌 '결과.pdf')은 제거
            try:
                with_href = [it for it in attachments if (it.get("href") or "").strip()]
                trimmed: list[dict] = []
                for it in attachments:
                    href = (it.get("href") or "").strip()
                    nm = (it.get("name") or "").strip()
                    if href or not nm:
                        trimmed.append(it)
                        continue
                    nm_l = nm.lower()
                    redundant = False
                    for o in with_href:
                        on = (o.get("name") or "").strip()
                        if not on:
                            continue
                        on_l = on.lower()
                        if nm_l != on_l and nm_l in on_l:
                            redundant = True
                            break
                    if not redundant:
                        trimmed.append(it)
                attachments = trimmed
            except Exception:
                pass

            # 중복 제거(파일명 기준) 2차
            uniq: list[dict] = []
            seen_name: set[str] = set()
            seen_href: dict[str, int] = {}
            for it in attachments:
                raw_href_key = (it.get("href") or "").strip()
                try:
                    href_key = urljoin(url or "", raw_href_key).lower() if raw_href_key else ""
                except Exception:
                    href_key = raw_href_key.lower()
                if href_key and href_key in seen_href:
                    prev_idx = seen_href[href_key]
                    prev_name = (uniq[prev_idx].get("name") or "").strip()
                    new_name = (it.get("name") or "").strip()
                    if len(new_name) > len(prev_name):
                        uniq[prev_idx]["name"] = new_name
                    continue
                nm = (it.get("name") or "").strip().lower()
                if nm and not href_key and nm in seen_name:
                    continue
                if nm:
                    seen_name.add(nm)
                if href_key:
                    seen_href[href_key] = len(uniq)
                uniq.append(it)

            if debug_on:
                try:
                    cand_links: list[str] = []
                    for a in attach_root.find_all("a"):
                        href = (a.get("href") or "").strip()
                        onclick = (a.get("onclick") or "").strip()
                        title = (a.get("title") or "").strip()
                        text = (a.get_text(" ", strip=True) or "").strip()
                        if href or onclick:
                            cand_links.append(f"href={href} onclick={onclick} title={title} text={text}")
                    logger.info(
                        "[AttachExtract][debug] url=%s found=%s candidates=%s",
                        str(url)[:160],
                        len(uniq),
                        cand_links[:10],
                    )
                except Exception:
                    pass
            # segment 잘림으로 첨부가 1개만 나온 경우 전체 HTML로 한 번 더 수집 시도(구로구 자동차 등)
            # 단, 현재 페이지와 같은 bbsNo/nttNo를 가진 링크만 수집(다른 게시글/메뉴 첨부 제외)
            if len(uniq) <= 1 and len(html) > len(segment):
                try:
                    from urllib.parse import urlparse, parse_qs
                    page_bbs, page_ntt = "", ""
                    if url:
                        parsed = urlparse(url)
                        qs = parse_qs(parsed.query)
                        page_bbs = (qs.get("bbsNo") or qs.get("bbsno") or [""])[0]
                        page_ntt = (qs.get("nttNo") or qs.get("nttno") or [""])[0]
                    soup_full = BeautifulSoup(html, "html.parser")
                    attachments2: list[dict] = []
                    seen_keys2: set[str] = set()
                    board_file_hint = ("downloadbbsfile", "atchmnflno", "filedown.do", "filedown.jsp", "atchfile", "atchfileid")
                    for a in soup_full.find_all("a", href=True):
                        href = normalize_attachment_href(a.get("href") or "")
                        if not href or href.startswith("#"):
                            continue
                        lh = href.lower()
                        if lh.startswith("javascript:"):
                            continue
                        if "previewbbs" in lh:
                            continue
                        # 전체 HTML에서는 게시판 첨부 URL만 허용
                        looks_like_board_file = any(h in lh for h in board_file_hint) or (
                            ("download" in lh or "filedown" in lh) and bool(ext_pat.search(lh))
                        )
                        if not looks_like_board_file:
                            continue
                        # 같은 게시글 첨부만: href에 bbsNo/nttNo가 있고 현재 페이지에도 있으면 일치할 때만 수집
                        if page_bbs and page_ntt and ("bbsno=" in lh and "nttno=" in lh):
                            if f"bbsno={page_bbs}" not in lh or f"nttno={page_ntt}" not in lh:
                                continue
                        link_t = (a.get_text(" ", strip=True) or "").strip()
                        title_attr = (a.get("title") or "").strip()
                        if "미리보기" in link_t or "미리보기" in title_attr:
                            continue
                        name = title_attr if title_attr else link_t
                        name = re.sub(r"\s+", " ", name).strip()
                        if name in ("바로보기", "바로듣기", "미리보기", "다운로드", "보기", "듣기"):
                            name = ""
                        if (not name) or (not ext_pat.search(name)):
                            try:
                                ctx = a.parent.get_text(" ", strip=True) if a.parent is not None else ""
                            except Exception:
                                ctx = ""
                            if ctx:
                                mfn = file_pat.search(ctx)
                                if mfn:
                                    name = mfn.group(1).strip()
                        name = _clean_attachment_display_name(name)
                        key = (name or "") + "|" + href
                        if key in seen_keys2:
                            continue
                        seen_keys2.add(key)
                        attachments2.append({"name": name, "href": href})
                    for it in attachments2:
                        if it.get("name"):
                            continue
                        try:
                            tail = (it.get("href", "") or "").split("/")[-1].split("?")[0]
                            if tail and ext_pat.search(tail):
                                it["name"] = tail[:160]
                        except Exception:
                            pass
                    uniq2: list[dict] = []
                    seen_name2: set[str] = set()
                    for it in attachments2:
                        nm = (it.get("name") or "").strip().lower()
                        if nm and nm in seen_name2:
                            continue
                        if nm:
                            seen_name2.add(nm)
                        uniq2.append(it)
                    if len(uniq2) > len(uniq):
                        uniq2 = _filter_guro_attachments(url, uniq2)
                        return {"attachment_count": len(uniq2), "attachments": uniq2}
                except Exception:
                    pass
            uniq = _filter_guro_attachments(url, uniq)
            return {"attachment_count": len(uniq), "attachments": uniq}

    # BeautifulSoup이 없거나 실패: 텍스트 기반(확장자/라벨)
    text = re.sub(r"<[^>]+>", " ", segment)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        if debug_on:
            try:
                logger.info(
                    "[AttachExtract][debug] url=%s found=0 (empty text)",
                    str(url)[:160],
                )
            except Exception:
                pass
        return {"attachment_count": 0, "attachments": []}

    # '첨부파일' 이후 구간에서 확장자 패턴을 카운트
    try:
        idx = text.find("첨부파일")
        tail = text[idx:] if idx != -1 else text
    except Exception:
        tail = text

    ext_pat2 = re.compile(r"\b([^\s]{1,120}\.(?:pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|zip|rar|7z|txt|jpg|jpeg|png|gif))\b", re.IGNORECASE)
    names = []
    for m in ext_pat2.finditer(tail):
        nm = m.group(1)
        if nm:
            names.append(nm)
    # unique
    uniq_names = []
    seen = set()
    for n in names:
        k = n.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq_names.append(n)
    if debug_on:
        try:
            logger.info(
                "[AttachExtract][debug] url=%s found=%s (text fallback)",
                str(url)[:160],
                len(uniq_names),
            )
        except Exception:
            pass
    return {"attachment_count": len(uniq_names), "attachments": [{"name": n, "href": ""} for n in uniq_names]}


def extract_meta_and_attachments_from_html(
    html: str,
    url: Optional[str] = None,
    *,
    meta_root_selector: Optional[str] = None,
    author_selector: Optional[str] = None,
    department_selector: Optional[str] = None,
    phone_selector: Optional[str] = None,
    view_selector: Optional[str] = None,
    attachment_selector: Optional[str] = None,
) -> Dict[str, Any]:
    """
    상세페이지 HTML에서 메타데이터(작성자/부서/연락처/조회수)와 첨부 목록을 한 번에 추출한다.
    - 호출부에서 개별 함수로 나누지 않기 위한 공용 진입점
    """
    info = {}
    extra = {}
    attach = {}
    try:
        info = extract_author_info_from_html(
            html,
            url=url,
            meta_root_selector=meta_root_selector,
            author_selector=author_selector,
            department_selector=department_selector,
        )
    except Exception:
        info = {}
    try:
        extra = extract_contact_views_from_html(
            html,
            url=url,
            meta_root_selector=meta_root_selector,
            phone_selector=phone_selector,
            view_selector=view_selector,
        )
    except Exception:
        extra = {}
    try:
        attach = extract_attachment_summary_from_html(
            html,
            url=url,
            attachment_selector=attachment_selector,
        )
    except Exception:
        attach = {}

    result = {
        "author": info.get("author"),
        "department": info.get("department"),
        "author_kind": info.get("author_kind"),
        "author_raw": info.get("author_raw"),
        "department_raw": info.get("department_raw"),
        "contact_phone": extra.get("contact_phone"),
        "view_count": extra.get("view_count"),
        "attachment_count": int((attach.get("attachment_count") or 0) if isinstance(attach, dict) else 0),
        "attachments": (attach.get("attachments") or []) if isinstance(attach, dict) else [],
    }
    try:
        logger.info(
            "[메타데이터 추출 결과] url=%s author=%s department=%s author_kind=%s contact_phone=%s view_count=%s attachment_count=%s",
            url,
            result.get("author"),
            result.get("department"),
            result.get("author_kind"),
            result.get("contact_phone"),
            result.get("view_count"),
            result.get("attachment_count"),
        )
        # attachments 정보가 있으면 개수와 일부 항목을 추가로 로깅
        attachments = result.get("attachments") or []
        if attachments:
            try:
                names = [a.get("name") or a.get("href") or str(a) for a in attachments[:10]]
                logger.debug("[메타데이터 첨부목록] count=%d sample=%s", len(attachments), names)
            except Exception:
                logger.debug("[메타데이터 첨부목록] attachments present, debug extract failed")
    except Exception:
        # 로깅 실패 시 전체 흐름을 방해하지 않도록 무시
        pass
    return result


def extract_author_raw_from_text(text: str) -> Optional[str]:
    """텍스트에서 author(작성자 우선, 실패 시 None) 원문(raw)을 추출한다."""
    if not text:
        return None
    s = re.sub(r"[\r\n]+", " ", str(text))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None

    author_keywords = ("작성자", "등록자", "등록인", "작성인", "글쓴이", "성명", "담당자", "author", "writer")
    boundary_tokens = (
        "작성자", "등록자", "등록인", "작성인", "담당부서", "부서", "부서명", "작성부서", "작성부서명",
        "담당과", "담당팀", "담당기관", "담당자", "성명", "글쓴이",
        "직책", "작성자유형",
        "전화번호", "tel", "fax",
        "등록일", "작성일", "게시일", "수정일",
        "조회수", "조회",
        "첨부파일", "첨부",
    )
    author_pattern = (
        r"(?:" + "|".join(author_keywords) + r")\s*[:\s]*"
        r"(.{2,80}?)"
        r"(?=\s*(?:" + "|".join(boundary_tokens) + r"|\||/|$))"
    )
    for m in re.finditer(author_pattern, s, flags=re.IGNORECASE):
        res = _finalize_author_candidate_raw(m.group(1).strip())
        if res:
            return res
    return None


def extract_author_raw_from_html(html: str) -> Optional[str]:
    """HTML에서 author(작성자 우선) 원문(raw)을 추출한다."""
    if not html:
        return None
    if not BeautifulSoup:
        try:
            segment = _extract_relevant_html_segment(html)
            text = re.sub(r"<[^>]+>", " ", segment)
            text = re.sub(r"\s+", " ", text).strip()
            return extract_author_raw_from_text(text)
        except Exception:
            return None

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    excluded_selectors = ["nav", "header", "footer", "aside"]
    excluded_elements = set()
    for sel in excluded_selectors:
        for el in soup.select(sel):
            excluded_elements.add(el)

    search_root = None
    for sel in ("article", "main", "#contents", "#content", ".contents", ".sub_contents", ".container", "body"):
        found = soup.select_one(sel)
        if found:
            search_root = found
            break
    if not search_root:
        search_root = soup

    author_labels = (
        "작성자", "등록자", "등록인", "작성인", "글쓴이", "성명", "담당자", "작성부서", "author", "writer",
    )
    for label in author_labels:
        norm_label = _norm_label(label)
        for el in search_root.find_all(["dt", "th", "td", "span", "label", "div", "p", "li"]):
            if el in excluded_elements:
                continue
            t = el.get_text(" ", strip=True)
            if not t:
                continue
            nt = _norm_label(t)
            if nt != norm_label:
                m_inline = re.search(rf"{re.escape(label)}\s*[:\s]+(.{{2,80}})$", t, flags=re.IGNORECASE)
                if m_inline:
                    cand = _finalize_author_candidate_raw(m_inline.group(1))
                    if cand:
                        return cand
                continue

            nxt = el.find_next_sibling(["dd", "td", "span", "div"])
            if nxt and nxt not in excluded_elements:
                cand = _finalize_author_candidate_raw(nxt.get_text(" ", strip=True))
                if cand:
                    return cand

            parent = el.parent
            if parent and parent not in excluded_elements:
                combined = parent.get_text(" ", strip=True)
                m = re.search(rf"{label}\s*[:\s]*([^:|]{{2,80}})", combined)
                if m:
                    cand = _finalize_author_candidate_raw(m.group(1))
                    if cand:
                        return cand
    return None


def extract_department_raw_from_text(text: str) -> Optional[str]:
    """텍스트에서 department 원문(raw)을 추출한다."""
    if not text:
        return None
    s = re.sub(r"[\r\n]+", " ", str(text))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None

    boundary_tokens = (
        "작성자", "등록자", "등록인", "작성인", "담당부서", "주관부서", "시행부서", "부서", "부서명", "작성부서", "작성부서명",
        "담당과", "담당팀", "담당기관", "담당자", "성명", "글쓴이",
        "직책", "작성자유형",
        "전화번호", "tel", "fax",
        "등록일", "작성일", "게시일", "수정일",
        "조회수", "조회",
        "첨부파일", "첨부",
    )
    dept_keywords = (
        "담당부서",
        "주관부서",
        "시행부서",
        "부서",
        "부서명",
        "작성부서",
        "작성부서명",
        "담당과",
        "담당팀",
        "담당기관",
        "department",
    )
    dept_pattern = (
        r"(?:" + "|".join(dept_keywords) + r")\s*[:\s]*"
        r"(.{2,80}?)"
        r"(?=\s*(?:" + "|".join(boundary_tokens) + r"|\||/|$))"
    )
    for m in re.finditer(dept_pattern, s, flags=re.IGNORECASE):
        res = _finalize_author_candidate_raw(m.group(1).strip())
        if res and _is_valid_explicit_department_value(res):
            return res
    return None


def extract_department_raw_from_html(html: str) -> Optional[str]:
    """HTML에서 department 원문(raw)을 추출한다."""
    if not html:
        return None
    if not BeautifulSoup:
        try:
            segment = _extract_relevant_html_segment(html)
            text = re.sub(r"<[^>]+>", " ", segment)
            text = re.sub(r"\s+", " ", text).strip()
            return extract_department_raw_from_text(text)
        except Exception:
            return None

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    excluded_selectors = ["nav", "header", "footer", "aside"]
    excluded_elements = set()
    for sel in excluded_selectors:
        for el in soup.select(sel):
            excluded_elements.add(el)

    search_root = None
    for sel in ("article", "main", "#contents", "#content", ".contents", ".sub_contents", ".container", "body"):
        found = soup.select_one(sel)
        if found:
            search_root = found
            break
    if not search_root:
        search_root = soup

    dept_labels = (
        "부서",
        "담당부서",
        "주관부서",
        "시행부서",
        "부서명",
        "작성부서",
        "작성부서명",
        "담당과",
        "담당팀",
        "담당기관",
        "부서(팀)",
        "department",
    )
    for label in dept_labels:
        norm_label = _norm_label(label)
        for el in search_root.find_all(["dt", "th", "td", "span", "label", "div", "p", "li"]):
            if el in excluded_elements:
                continue
            t = el.get_text(" ", strip=True)
            if not t:
                continue
            nt = _norm_label(t)
            if nt != norm_label:
                m_inline = re.search(rf"{re.escape(label)}\s*[:\s]+(.{{2,80}})$", t, flags=re.IGNORECASE)
                if m_inline:
                    cand = _finalize_author_candidate_raw(m_inline.group(1))
                    if cand and _is_valid_explicit_department_value(cand):
                        return cand
                continue

            nxt = el.find_next_sibling(["dd", "td", "span", "div"])
            if nxt and nxt not in excluded_elements:
                cand = _finalize_author_candidate_raw(nxt.get_text(" ", strip=True))
                if cand and _is_valid_explicit_department_value(cand):
                    return cand

            parent = el.parent
            if parent and parent not in excluded_elements:
                combined = parent.get_text(" ", strip=True)
                m = re.search(rf"{label}\s*[:\s]*([^:|]{{2,80}})", combined)
                if m:
                    cand = _finalize_author_candidate_raw(m.group(1))
                    if cand and _is_valid_explicit_department_value(cand):
                        return cand
    return None

def extract_author_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    
    s = re.sub(r"[\r\n]+", " ", str(text))
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None

    # 0) 하단 정보영역(문의/전화) 기반 우선 추출 (본문 문장 오탐 방지)
    cand0 = _extract_dept_from_footerish_text(s)
    if cand0:
        logger.info(f"[AuthorExtract] 텍스트 매칭 성공 (하단정보:문의/전화) | author={cand0!r}")
        return cand0

    # 1) 작성자 키워드 우선 시도
    author_keywords = ("작성자", "등록자", "등록인", "작성인", "글쓴이", "성명", "담당자", "author", "writer")
    boundary_tokens = (
        "작성자", "등록자", "등록인", "작성인", "담당부서", "주관부서", "시행부서", "부서", "부서명", "작성부서", "작성부서명",
        "담당과", "담당팀", "담당기관", "담당자", "성명", "글쓴이",
        "직책", "작성자유형",
        "전화번호", "tel", "fax",
        "등록일", "작성일", "게시일", "수정일",
        "조회수", "조회",
        "첨부파일", "첨부",
    )
    author_pattern = (
        r"(?:" + "|".join(author_keywords) + r")\s*[:\s]*"
        r"(.{2,80}?)"
        r"(?=\s*(?:" + "|".join(boundary_tokens) + r"|\||/|$))"
    )
    for m in re.finditer(author_pattern, s, flags=re.IGNORECASE):
        res = _finalize_author_candidate(m.group(1).strip())
        if res:
            logger.info(f"[AuthorExtract] 텍스트 매칭 성공 (작성자) | author={res!r}")
            return res

    # 2) 실패 시 부서 키워드 시도
    dept_keywords = (
        "담당부서",
        "주관부서",
        "시행부서",
        "부서",
        "부서명",
        "작성부서",
        "작성부서명",
        "담당과",
        "담당팀",
        "담당기관",
        "department",
    )
    dept_pattern = (
        r"(?:" + "|".join(dept_keywords) + r")\s*[:\s]*"
        r"(.{2,80}?)"
        r"(?=\s*(?:" + "|".join(boundary_tokens) + r"|\||/|$))"
    )
    for m in re.finditer(dept_pattern, s, flags=re.IGNORECASE):
        res = _finalize_author_candidate(m.group(1).strip())
        if res:
            logger.info(f"[AuthorExtract] 텍스트 매칭 성공 (폴백:부서) | author={res!r}")
            return res

    return None
