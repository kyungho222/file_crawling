"""
날짜/기간 유틸리티
"""
from datetime import datetime, timedelta, date  # date를 이쪽으로 옮겼어요!
from typing import Optional, Dict, Union         # 여기서는 date를 뺐습니다.
import json
import os
import re
import logging


_KST_LIKE_NOW = datetime.now  # 단순 로컬 시간 기준(프로젝트 기존 방식 유지)

logger = logging.getLogger(__name__)


def _event_period_as_post_date_enabled() -> bool:
    return str(os.getenv("BOARD_ALLOW_EVENT_PERIOD_AS_REG_DATE", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "y",
    }


def _is_reasonable_date(dt: datetime) -> bool:
    """추출된 날짜의 합리성 체크(푸터 연도/미래 날짜 오탐 방지)."""
    try:
        now = _KST_LIKE_NOW()
        if dt.year < 1900:
            logger.debug("[date-debug] _is_reasonable_date rejected (year<1900) | dt=%s", dt)
            return False
        # 게시물 날짜는 보통 미래가 아니며, 서버 시각 차이를 고려해 +1일은 허용
        if dt.date() > now.date():
            logger.debug("[date-debug] _is_reasonable_date rejected (future) | dt=%s now=%s", dt, now)
            return False
        # 너무 과거(예: 1900년대/푸터)도 걸러내고 싶다면 여기서 강화 가능
        logger.debug("[date-debug] _is_reasonable_date accepted | dt=%s", dt)
        return True
    except Exception:
        logger.exception("[date-debug] _is_reasonable_date exception for dt=%s", dt)
        return True


def _coerce_datetime(y: str, m: str, d: str) -> Optional[datetime]:
    try:
        dt = datetime(int(y), int(m), int(d))
        ok = _is_reasonable_date(dt)
        logger.debug("[date-debug] _coerce_datetime parsed | y=%s m=%s d=%s dt=%s ok=%s", y, m, d, dt, ok)
        return dt if ok else None
    except Exception:
        logger.debug("[date-debug] _coerce_datetime failed | y=%s m=%s d=%s", y, m, d)
        return None


def _extract_relative_korean(text: str) -> Optional[datetime]:
    """'방금 전/오늘/어제/3시간 전/15분 전/2일 전' 같은 상대 표현 처리."""
    if not text:
        return None
    t = str(text).strip()
    if not t:
        return None
    logger.debug("[date-debug] _extract_relative_korean input=%s", t)
    now = _KST_LIKE_NOW()

    if "방금" in t:
        logger.debug("[date-debug] _extract_relative_korean matched '방금' -> %s", now)
        return now
    if t == "오늘":
        val = now.replace(hour=0, minute=0, second=0, microsecond=0)
        logger.debug("[date-debug] _extract_relative_korean matched '오늘' -> %s", val)
        return val
    if t == "어제":
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)
        val = base - timedelta(days=1)
        logger.debug("[date-debug] _extract_relative_korean matched '어제' -> %s", val)
        return val

    m = re.search(r"(\d+)\s*분\s*전", t)
    if m:
        val = now - timedelta(minutes=int(m.group(1)))
        logger.debug("[date-debug] _extract_relative_korean matched '분 전' -> %s", val)
        return val
    m = re.search(r"(\d+)\s*시간\s*전", t)
    if m:
        val = now - timedelta(hours=int(m.group(1)))
        logger.debug("[date-debug] _extract_relative_korean matched '시간 전' -> %s", val)
        return val
    m = re.search(r"(\d+)\s*일\s*전", t)
    if m:
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)
        val = base - timedelta(days=int(m.group(1)))
        logger.debug("[date-debug] _extract_relative_korean matched '일 전' -> %s", val)
        return val

    return None


def _pick_best_date(candidates: list[datetime], context: str = "") -> Optional[datetime]:
    """여러 날짜 후보 중 가장 그럴듯한 값을 선택."""
    if not candidates:
        return None

    # 합리성 필터
    reasonable = [d for d in candidates if _is_reasonable_date(d)]
    pool = reasonable or candidates

    # 기본 정책: 가장 최근 날짜(게시글/등록일은 보통 최근)
    # 단, context에 '수정'이 강하게 들어가면 수정일이 최근일 수 있으니 별도 처리 여지도 있음.
    try:
        return max(pool)
    except Exception:
        return pool[0]


def _parse_candidate_date_string(value: object) -> Optional[datetime]:
    """
    temp 프로젝트의 _parse_date_string 성격을 현재 프로젝트 스타일로 흡수한 파서.
    - parse_date(포맷 파서) 우선
    - 실패 시 텍스트에서 날짜 패턴을 뽑아 coerce
    - 최후로 extract_date_from_text(YYYYMMDD, 상대시간 등) 시도
    """
    if value is None:
        return None
    try:
        s = value if isinstance(value, str) else str(value)
    except Exception:
        return None
    s = s.strip()
    if not s:
        return None

    logger.debug("[date-debug] _parse_candidate_date_string input=%s", s)
    # 1) 정형 포맷 파서(시간 포함 지원)
    dt = parse_date(s)  # type: ignore[name-defined]
    if dt:
        ok = _is_reasonable_date(dt)
        logger.debug("[date-debug] parse_date -> dt=%s ok=%s", dt, ok)
        return dt if ok else None

    # 2) ISO/일반 문자열에서 날짜 부분만 추출
    m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", s)
    if m:
        dt2 = _coerce_datetime(m.group(1), m.group(2), m.group(3))
        if dt2:
            return dt2

    # 3) YYYYMMDD 등/상대시간 대응
    try:
        dt3 = extract_date_from_text(s)  # type: ignore[name-defined]
        if dt3:
            ok = _is_reasonable_date(dt3)
            logger.debug("[date-debug] extract_date_from_text -> dt=%s ok=%s", dt3, ok)
            return dt3 if ok else None
    except Exception:
        logger.debug("[date-debug] extract_date_from_text exception for input=%s", s)
        pass
    return None


def _find_date_in_json(obj: object) -> Optional[datetime]:
    """JSON(dict/list) 내부에서 date 관련 키의 값을 재귀적으로 찾아 날짜로 파싱한다."""
    try:
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = str(k).lower()
                if any(t in key for t in ("date", "dt", "reg", "write", "create", "publish", "modified")):
                    cand = _parse_candidate_date_string(v)
                    if cand:
                        return cand
                # 하위 탐색
                nested = _find_date_in_json(v)
                if nested:
                    return nested
        elif isinstance(obj, list):
            for it in obj:
                nested = _find_date_in_json(it)
                if nested:
                    return nested
    except Exception:
        return None
    return None


def _extract_date_from_raw_response(raw_response_text: str) -> Optional[datetime]:
    """
    temp 프로젝트의 핵심(0단계): raw_response_text(JSON/JS/script)에서 날짜를 우선 추출.
    - JSON 응답/스크립트 내 JSON 객체
    - JS 변수(regDate/postDate/writeDate 등)
    - JSON-LD datePublished/dateCreated/dateModified
    """
    if not raw_response_text:
        return None

    text = raw_response_text
    logger.debug("[date-debug] _extract_date_from_raw_response input_len=%d", len(text or ""))

    # 1) 전체 텍스트가 JSON인 경우
    try:
        parsed = json.loads(text)
        dt = _find_date_in_json(parsed)
        logger.debug("[date-debug] _extract_date_from_raw_response json_parse ok=%s dt=%s", bool(parsed), dt)
        if dt:
            return dt
    except Exception:
        pass

    candidates: list[datetime] = []

    # 2) JS 변수/객체에서 흔한 키들 탐색
    js_date_patterns = [
        r'(?:var|let|const)\s+(?:regDate|createDate|postDate|writeDate|publishDate|date|regDt|createDt|postDt|writeDt|publishDt)\s*=\s*["\']([^"\']+)["\']',
        r'window\.(?:regDate|createDate|postDate|writeDate|publishDate|date)\s*=\s*["\']([^"\']+)["\']',
        r'"(?:regDate|createDate|postDate|writeDate|publishDate|regDt|createDt|postDt|writeDt|publishDt)"\s*:\s*["\']([^"\']+)["\']',
        r'\b(?:regDate|createDate|postDate|writeDate|publishDate|regDt|createDt|postDt|writeDt|publishDt)\s*[:=]\s*["\']([^"\']+)["\']',
    ]
    for pat in js_date_patterns:
        for s in re.findall(pat, text, flags=re.IGNORECASE):
            dt = _parse_candidate_date_string(s)
            if dt:
                candidates.append(dt)
                logger.debug("[date-debug] _extract_date_from_raw_response js_pattern matched -> %s -> %s", s, dt)

    # 3) JSON-LD (application/ld+json)
    try:
        for block in re.findall(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            text,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            block = (block or "").strip()
            if not block:
                continue
            try:
                data = json.loads(block)
            except Exception:
                continue
            dt = _find_date_in_json(data)
            if dt:
                candidates.append(dt)
    except Exception:
        pass

    if candidates:
        picked = _pick_best_date(candidates)
        logger.debug("[date-debug] _extract_date_from_raw_response candidates=%s picked=%s", len(candidates), picked)
        return picked
    return None


_APP_PERIOD_LABEL_RE = re.compile(
    r"(?:신청\s*기간|접수\s*기간|모집\s*기간|프로그램\s*기간|행사\s*기간|운영\s*기간|접수\s*일정|교육\s*기간|접수\s*기간)",
    re.IGNORECASE,
)

# 등록일이 없는 행사/문화 공고: 본문의 행사일·개최일을 등록일 대용으로 쓸 때
_EVENT_DATE_LABEL_RE = re.compile(
    r"(?:행사\s*일|개최\s*일|행사\s*일시|개최\s*일시|행사\s*기간|개최\s*기간)",
    re.IGNORECASE,
)


def _trim_for_general_date_scan(text: str) -> str:
    """푸터·공통 영역의 날짜(오늘/저작권 연도 등)가 등록일로 오인되지 않게 스캔 구간을 줄인다."""
    if not text:
        return ""
    n = len(text)
    if n < 12000:
        return text
    tail = min(25000, max(6000, n // 5))
    return text[:-tail]


def _min_full_dates_from_blob(blob: str) -> Optional[datetime]:
    """blob 안의 YYYY-MM-DD 등 전체 날짜 후보 중 가장 이른 날짜(행사 시작일 근사)."""
    if not blob:
        return None
    dates: list[datetime] = []
    for pattern in (
        r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})",
        r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?",
        r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일",
    ):
        for m in re.finditer(pattern, blob):
            dt = _coerce_datetime(m.group(1), m.group(2), m.group(3))
            if dt:
                dates.append(dt)
    if not dates:
        return None
    return min(dates)


def extract_approx_date_from_event_labels(page_content: str) -> Optional[datetime]:
    """행사일·개최일 등 라벨 인근의 실제 날짜 중 최소값(통상 행사 시작일)."""
    if not page_content or not str(page_content).strip():
        return None
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]

        soup = BeautifulSoup(page_content, "html.parser")
        for cell in soup.find_all(["th", "td", "dt"]):
            try:
                label_t = (cell.get_text(" ", strip=True) or "").strip()
            except Exception:
                continue
            if not label_t or not _EVENT_DATE_LABEL_RE.search(label_t):
                continue
            blob = label_t
            sib = cell.find_next_sibling(["td", "dd"])
            if sib:
                try:
                    blob = blob + " " + (sib.get_text(" ", strip=True) or "")
                except Exception:
                    pass
            res = _min_full_dates_from_blob(blob)
            if res:
                logger.debug("[date-debug] event_label DOM matched -> %s", res)
                return res
    except Exception:
        pass

    try:
        flat = re.sub(r"<[^>]+>", " ", page_content)
        flat = re.sub(r"\s+", " ", flat).strip()
        for m in _EVENT_DATE_LABEL_RE.finditer(flat):
            chunk = flat[m.start() : m.start() + 240]
            res = _min_full_dates_from_blob(chunk)
            if res:
                logger.debug("[date-debug] event_label text chunk matched -> %s", res)
                return res
    except Exception:
        pass
    return None


def _approx_datetime_from_application_period_years(blob: str) -> Optional[datetime]:
    """
    신청·접수·모집·프로그램 기간 문구 인근 텍스트에서 날짜를 모아,
    등록일 대체용으로 **해당 시작 연도의 1월 1일**을 반환한다(연도 단위 근사).
    """
    if not blob:
        return None
    dates: list[datetime] = []
    for pattern in (
        r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})",
        r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?",
        r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일",
    ):
        for m in re.finditer(pattern, blob):
            dt = _coerce_datetime(m.group(1), m.group(2), m.group(3))
            if dt:
                dates.append(dt)
    if dates:
        y = min(d.year for d in dates)
        out = datetime(y, 1, 1)
        return out if _is_reasonable_date(out) else None
    years: list[int] = []
    for m in re.finditer(r"\b(19\d{2}|20\d{2})\b", blob):
        y = int(m.group(1))
        if 1990 <= y <= 2100:
            years.append(y)
    if years:
        out = datetime(min(years), 1, 1)
        return out if _is_reasonable_date(out) else None
    return None


def extract_approx_date_from_application_period(page_content: str) -> Optional[datetime]:
    """
    HTML/텍스트에서 신청기간·프로그램 기간 등 라벨을 찾고, 기간 내 첫 연도를 1월 1일로 근사한다.
    등록일/작성일 라벨이 없는 온라인 접수·교육 공고 등에 사용한다.
    """
    if not page_content or not str(page_content).strip():
        return None
    # 1) 표·정의목록: 라벨 셀 + 인접 값 셀
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]

        soup = BeautifulSoup(page_content, "html.parser")
        for cell in soup.find_all(["th", "td", "dt"]):
            try:
                label_t = (cell.get_text(" ", strip=True) or "").strip()
            except Exception:
                continue
            if not label_t or not _APP_PERIOD_LABEL_RE.search(label_t):
                continue
            blob = label_t
            sib = cell.find_next_sibling(["td", "dd"])
            if sib:
                try:
                    blob = blob + " " + (sib.get_text(" ", strip=True) or "")
                except Exception:
                    pass
            res = _approx_datetime_from_application_period_years(blob)
            if res:
                logger.debug("[date-debug] application_period DOM matched -> %s", res)
                return res
    except Exception:
        pass

    # 2) 태그 제거 후 라벨 뒤 200자 구간
    try:
        flat = re.sub(r"<[^>]+>", " ", page_content)
        flat = re.sub(r"\s+", " ", flat).strip()
        for m in _APP_PERIOD_LABEL_RE.finditer(flat):
            chunk = flat[m.start() : m.start() + 220]
            res = _approx_datetime_from_application_period_years(chunk)
            if res:
                logger.debug("[date-debug] application_period text chunk matched -> %s", res)
                return res
    except Exception:
        pass
    return None


def extract_post_date(page_content: str, post_url: str = "", raw_response_text: Optional[str] = None) -> Optional[datetime]:
    """
    게시글 페이지에서 작성일을 추출
    다양한 패턴을 시도하여 날짜 추출
    우선순위: 등록일/작성일/게시일 > 일반 날짜 패턴
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.debug("[date-debug] extract_post_date enter | url=%s raw_present=%s", post_url[:200], bool(raw_response_text))

    try:
        from backend.board.gangnam_board import is_gangnam_main_apply_view_url

        if post_url and is_gangnam_main_apply_view_url(str(post_url)):
            # 온라인 신청 안내형 상세는 '게시 등록일' 메타가 없고 스키마·잡텍스트만 있어 날짜 오탐이 잦음
            return None
    except Exception:
        pass

    # 0.5) DT/DD 구조 파싱(HTML) 우선 시도
    # 런타임 로그에서 실제로 <dt>등록일</dt><dd>YYYY-MM-DD</dd> 구조가 존재하는데도
    # 정규식 매칭이 실패하여 general_pattern으로 떨어지는 케이스가 확인되었다.
    # 따라서 가능하면 DOM(BeautifulSoup) 기반으로 라벨→값을 직접 매칭한다.
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover
        BeautifulSoup = None  # type: ignore[assignment]

    # 평택 시험·채용 상세는 등록일 컬럼이 없는 경우가 많다.
    # 명시 등록일이 없으면 접수/모집 시작일, 첨부파일 날짜 순으로 기준일을 근사한다.
    try:
        if BeautifulSoup and page_content and post_url:
            from backend.board.anseong_board import extract_anseong_reg_date, is_anseong_url

            if is_anseong_url(str(post_url)):
                soup_anseong = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
                dt_anseong = extract_anseong_reg_date(soup_anseong, url=str(post_url))
                if dt_anseong:
                    logger.debug("[date-debug] extract_post_date anseong selector matched -> %s", dt_anseong)
                    return dt_anseong
    except Exception:
        pass

    # 평택 시험·채용 상세는 등록일 컬럼이 없는 경우가 많다.
    # 명시 등록일이 없으면 접수/모집 시작일, 첨부파일 날짜 순으로 기준일을 근사한다.
    try:
        if BeautifulSoup and page_content and post_url:
            from backend.board.gm_board import extract_gm_reg_date, is_gm_url

            if is_gm_url(str(post_url)):
                soup_gm = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
                dt_gm = extract_gm_reg_date(soup_gm, url=str(post_url))
                if dt_gm:
                    logger.debug("[date-debug] extract_post_date gm selector matched -> %s", dt_gm)
                    return dt_gm
    except Exception:
        pass

    try:
        if (
            BeautifulSoup
            and page_content
            and post_url
            and "pyeongtaek.go.kr" in str(post_url).lower()
            and "/recruitanm/view.do" in str(post_url).lower()
        ):
            from backend.board.pyeongtaek_board import extract_pyeongtaek_recruit_reg_date_text

            soup_pt = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
            raw_pt = extract_pyeongtaek_recruit_reg_date_text(soup_pt, url=str(post_url))
            dt_pt = _parse_candidate_date_string(raw_pt) if raw_pt else None
            logger.debug("[date-debug] extract_post_date pyeongtaek_recruit explicit -> %s", dt_pt)
            return dt_pt
    except Exception:
        return None

    # k-cohesion detail pages place the post date in the header meta area without
    # a date label: .board_detail_wrap .detail_info .info => author, views, date.
    try:
        if (
            BeautifulSoup
            and page_content
            and post_url
            and "k-cohesion.go.kr" in str(post_url).lower()
            and "/pcnc/contents/" in str(post_url).lower()
            and "schm=view" in str(post_url).lower()
        ):
            soup_kc = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
            for sel in (
                ".board_detail_wrap .detail_info .info",
                ".board_detail_wrap .bd_wrap .detail_info span",
                ".detail_info .info",
            ):
                try:
                    nodes = soup_kc.select(sel)
                except Exception:
                    nodes = []
                for node in nodes or []:
                    try:
                        raw_date = (node.get_text(" ", strip=True) or "").strip()
                    except Exception:
                        raw_date = ""
                    if not raw_date or ":" in raw_date:
                        continue
                    dt_kc = _parse_candidate_date_string(raw_date)
                    if dt_kc:
                        logger.debug("[date-debug] extract_post_date k_cohesion selector matched -> %s", dt_kc)
                        return dt_kc
    except Exception:
        pass

    # 광명시 상수도 계약사업목록 상세:
    # - 일반 게시판의 작성일/등록일 라벨이 없고 `계약일자`가 목록의 기준 날짜다.
    # - 공용 추출 경로에서는 지급일자/준공일자 같은 늦은 날짜를 등록일로 오인하거나 None으로 빠질 수 있어
    #   전용 테이블 파서를 가장 앞에서 적용한다.
    try:
        if BeautifulSoup and page_content and post_url:
            from backend.board.gm_board import (
                extract_gm_lobas_tcm_reg_date,
                is_gm_lobas_tcm_detail_url,
            )

            if is_gm_lobas_tcm_detail_url(str(post_url)):
                soup_gm_lobas = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
                dt_gm_lobas = extract_gm_lobas_tcm_reg_date(soup_gm_lobas, url=str(post_url))
                if dt_gm_lobas:
                    logger.debug("[date-debug] extract_post_date gm_lobas_tcm contract_date -> %s", dt_gm_lobas)
                    return dt_gm_lobas
    except Exception:
        pass

    # 용인시청 일반 게시판 상세:
    # - `.article-header .article-info li`의 두 번째 항목에 등록일이 들어간다.
    try:
        if (
            BeautifulSoup
            and page_content
            and post_url
            and "yongin.go.kr" in str(post_url).lower()
            and "/user/bbs/bd_selectbbs.do" in str(post_url).lower()
            and "/citizen/user/" not in str(post_url).lower()
        ):
            soup_yongin = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
            try:
                from backend.board.yongin_board import extract_yongin_general_date_text

                raw_date = (extract_yongin_general_date_text(soup_yongin) or "").strip()
            except Exception:
                raw_date = ""
            dt_yongin = _parse_candidate_date_string(raw_date)
            if dt_yongin:
                logger.debug("[date-debug] extract_post_date yongin_general selector matched -> %s", dt_yongin)
                return dt_yongin
    except Exception:
        pass

    # 용인시 통합예약 공지 상세:
    # - 등록일 라벨 없이 `.article-header .sub-info .sub-date`에 값만 존재한다.
    # - 일반 패턴으로도 잡히지만, 푸터/본문 다른 날짜보다 게시일을 우선 보장하기 위해 DOM에서 먼저 읽는다.
    try:
        if (
            BeautifulSoup
            and page_content
            and post_url
            and "resve.yongin.go.kr" in str(post_url).lower()
            and "/user/bbs/bd_selectbbs.do" in str(post_url).lower()
        ):
            soup_yongin = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
            for sel in (
                ".article-header .sub-info .sub-date",
                ".article-header .sub-date",
                ".sub-info .sub-date",
                ".sub-date",
            ):
                try:
                    el = soup_yongin.select_one(sel)
                except Exception:
                    el = None
                if el is None:
                    continue
                try:
                    raw_date = (el.get_text(" ", strip=True) or "").strip()
                except Exception:
                    raw_date = ""
                dt_yongin = _parse_candidate_date_string(raw_date)
                if dt_yongin:
                    logger.debug("[date-debug] extract_post_date yongin_resve selector matched -> %s", dt_yongin)
                    return dt_yongin
    except Exception:
        pass

    # 동작구청 본청 게시판 상세:
    # - 상단 메타가 `부서 / 전화 / 공개일 / 조회수` 순서로 고정되고
    #   본문/푸터에 다른 날짜가 섞여 있어 일반 패턴이 오탐하기 쉽다.
    try:
        if (
            BeautifulSoup
            and page_content
            and post_url
        ):
            try:
                from backend.board.board_meta_extractor import _is_dongjak_portal_bbs_view_url

                is_dongjak_portal_view = _is_dongjak_portal_bbs_view_url(str(post_url))
            except Exception:
                url_l = str(post_url).lower()
                is_dongjak_portal_view = (
                    "dongjak.go.kr" in url_l and "/portal/bbs/" in url_l and "/view.do" in url_l
                )
            if not is_dongjak_portal_view:
                raise RuntimeError("skip_non_dongjak_portal_bbs_view")
            soup_dj = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
            try:
                from backend.board.board_meta_extractor import extract_dongjak_portal_date_text

                raw_date = (extract_dongjak_portal_date_text(soup_dj) or "").strip()
            except Exception:
                raw_date = ""
            dt_dj = _parse_candidate_date_string(raw_date)
            if dt_dj:
                logger.debug("[date-debug] extract_post_date dongjak_portal selector matched -> %s", dt_dj)
                return dt_dj
    except Exception:
        pass

    # 강남구청 /office/.../board/.../view.do: .bbs-view .post-info 내 YYYY-MM-DD만 있고 라벨(dt/th)이 없음.
    # 일반 패턴이 본문·푸터의 다른 날짜(예: 2026-02-13)를 잡는 오탐을 막기 위해 DT/DD·정규식보다 먼저 처리한다.
    try:
        from backend.board.gangnam_board import (
            extract_gangnam_office_board_reg_date,
            is_gangnam_family_url,
            is_gangnam_main_office_board_view_url,
        )

        if (
            BeautifulSoup
            and page_content
            and post_url
            and is_gangnam_family_url(str(post_url))
            and is_gangnam_main_office_board_view_url(str(post_url))
        ):
            soup_gn = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
            dt_gn = extract_gangnam_office_board_reg_date(soup_gn)
            if dt_gn:
                logger.debug("[date-debug] extract_post_date gangnam_office_board -> %s", dt_gn)
                return dt_gn
    except Exception:
        pass

    def _extract_dt_dd(labels: list[str]) -> Optional[datetime]:
        if not BeautifulSoup:
            return None
        if not page_content or not isinstance(page_content, str):
            return None
        try:
            soup = BeautifulSoup(page_content, "html.parser")  # type: ignore[arg-type]
            for dt_tag in soup.find_all("dt"):
                try:
                    dt_text = (dt_tag.get_text(" ", strip=True) or "").strip()
                except Exception:
                    continue
                if not dt_text:
                    continue
                if not any(lbl in dt_text for lbl in labels):
                    continue
                dd_tag = dt_tag.find_next_sibling("dd")
                if not dd_tag:
                    continue
                try:
                    dd_text = (dd_tag.get_text(" ", strip=True) or "").strip()
                except Exception:
                    dd_text = ""
                dt_val = _parse_candidate_date_string(dd_text)
                if dt_val:
                    return dt_val
        except Exception:
            return None
        return None

    # posted 라벨 우선
    try:
        dt_dom = _extract_dt_dd(["등록일", "작성일", "게시일", "등록일자", "작성일자", "게시일자"])
        if dt_dom:
            logger.debug("[date-debug] extract_post_date dt_dom matched -> %s", dt_dom)
            return dt_dom
    except Exception:
        pass
    # modified 라벨 fallback (posted가 없을 때만 의미)
    try:
        dt_dom_mod = _extract_dt_dd(["수정일", "최종수정일", "수정일자"])
        if dt_dom_mod:
            logger.debug("[date-debug] extract_post_date dt_dom_mod matched -> %s", dt_dom_mod)
            return dt_dom_mod
    except Exception:
        pass

    # 0) raw_response_text 기반 날짜 추출 (temp 프로젝트 이식)
    try:
        raw = raw_response_text if raw_response_text is not None else None
        if raw:
            dt0 = _extract_date_from_raw_response(raw)
            if dt0:
                logger.debug("[date-debug] extract_post_date raw_response matched -> %s", dt0)
                return dt0
    except Exception:
        pass
    
    # 우선순위가 높은 패턴
    # - 요구사항상 '게시판 등록일'은 보통 등록일/작성일/게시일을 의미하므로 이를 먼저 탐색
    # - 수정일/최종수정일은 2차 fallback
    # ✅ 런타임 증거:
    # - gwangjin.go.kr 등의 실제 HTML은 <dt>등록일</dt>\n<dd ...>YYYY-MM-DD</dd> 처럼 라벨과 날짜가 "형제 태그"로 분리된다.
    # - 기존 패턴은 '.'가 줄바꿈을 못 넘으면(re.DOTALL 미사용) 매칭이 실패하여 general_pattern으로 떨어지고,
    #   그 과정에서 '수정일'이 잡혀 등록일이 틀리게 보일 수 있다.
    posted_label = r"(?:등록일|작성일|게시일|등록일자|작성일자|게시일자)"
    modified_label = r"(?:수정일|최종수정일|수정일자)"
    posted_patterns = [
        # DT/DD 구조(가장 흔한 케이스)
        rf"<dt[^>]*>\s*{posted_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})[-./](\d{{1,2}})[-./](\d{{1,2}})",
        rf"<dt[^>]*>\s*{posted_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})\.(\d{{1,2}})\.(\d{{1,2}})",
        rf"<dt[^>]*>\s*{posted_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})년\s*(\d{{1,2}})월\s*(\d{{1,2}})일",
        rf"<dt[^>]*>\s*{posted_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})(\d{{2}})(\d{{2}})",
        # 라벨 주변 200자 내 날짜 (태그/줄바꿈 포함 가능)
        rf"{posted_label}[^0-9]{{0,200}}(\d{{4}})[-./](\d{{1,2}})[-./](\d{{1,2}})",
        rf"{posted_label}[^0-9]{{0,200}}(\d{{4}})\.(\d{{1,2}})\.(\d{{1,2}})",
        rf"{posted_label}[^0-9]{{0,200}}(\d{{4}})년\s*(\d{{1,2}})월\s*(\d{{1,2}})일",
        rf"{posted_label}[^0-9]{{0,200}}(\d{{4}})(\d{{2}})(\d{{2}})",
    ]
    modified_patterns = [
        rf"<dt[^>]*>\s*{modified_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})[-./](\d{{1,2}})[-./](\d{{1,2}})",
        rf"<dt[^>]*>\s*{modified_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})\.(\d{{1,2}})\.(\d{{1,2}})",
        rf"<dt[^>]*>\s*{modified_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})년\s*(\d{{1,2}})월\s*(\d{{1,2}})일",
        rf"<dt[^>]*>\s*{modified_label}\s*</dt>\s*<dd[^>]*>\s*(\d{{4}})(\d{{2}})(\d{{2}})",
        rf"{modified_label}[^0-9]{{0,200}}(\d{{4}})[-./](\d{{1,2}})[-./](\d{{1,2}})",
        rf"{modified_label}[^0-9]{{0,200}}(\d{{4}})\.(\d{{1,2}})\.(\d{{1,2}})",
        rf"{modified_label}[^0-9]{{0,200}}(\d{{4}})년\s*(\d{{1,2}})월\s*(\d{{1,2}})일",
        rf"{modified_label}[^0-9]{{0,200}}(\d{{4}})(\d{{2}})(\d{{2}})",
    ]
    
    # 일반 날짜 패턴 (우선순위 낮음)
    general_patterns = [
        r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})',
        r'(\d{4})\.(\d{1,2})\.(\d{1,2})',
        r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
    ]
    
    def _try_patterns(patterns: list[str]) -> Optional[datetime]:
        found: list[datetime] = []
        for pattern in patterns:
            # ✅ DOTALL: 라벨과 날짜가 줄바꿈/태그로 떨어져 있는 케이스를 잡는다.
            matches = re.findall(pattern, page_content or "", re.IGNORECASE | re.DOTALL)
            for tup in matches or []:
                try:
                    year, month, day = tup
                    dt = _coerce_datetime(year, month, day)
                    if dt:
                        found.append(dt)
                except Exception:
                    continue
        return _pick_best_date(found, context=post_url)

    # 1) 등록/작성/게시일 우선
    dt1 = _try_patterns(posted_patterns)
    if dt1:
        logger.debug("[date-debug] extract_post_date posted_patterns matched -> %s", dt1)
        return dt1
    # 2) 수정일 fallback
    dt2 = _try_patterns(modified_patterns)
    if dt2:
        logger.debug("[date-debug] extract_post_date modified_patterns matched -> %s", dt2)
        return dt2

    # 명시적 등록/수정일 없음: 행사일 → 신청·프로그램 기간 근사를 **전체 페이지 일반 패턴보다 먼저**
    if _event_period_as_post_date_enabled():
        try:
            dt_ev = extract_approx_date_from_event_labels(page_content or "")
            if dt_ev:
                logger.debug("[date-debug] extract_post_date event_label approx -> %s", dt_ev)
                return dt_ev
        except Exception:
            pass
        try:
            dt_ap = extract_approx_date_from_application_period(page_content or "")
            if dt_ap:
                logger.debug("[date-debug] extract_post_date application_period approx -> %s", dt_ap)
                return dt_ap
        except Exception:
            pass

    # 일반 패턴: 푸터 제외 구간만(전체 max 시 오늘/저작권일이 등록일로 잡히는 오류 방지)
    trimmed = _trim_for_general_date_scan(page_content or "")
    all_dates: list[datetime] = []
    for pattern in general_patterns:
        matches = re.findall(pattern, trimmed)
        for match in matches:
            try:
                year, month, day = match
                extracted_date = _coerce_datetime(year, month, day)
                if extracted_date:
                    all_dates.append(extracted_date)
            except (ValueError, IndexError):
                continue

    if all_dates:
        selected_date = _pick_best_date(all_dates, context=post_url) or max(all_dates)
        logger.debug("[date-debug] extract_post_date general pattern selected -> %s candidates=%s", selected_date, len(all_dates))
        return selected_date

    logger.debug("[date-debug] extract_post_date failed to find date | url=%s", post_url[:80] if post_url else "N/A")
    return None


def extract_post_dates(
    page_content: str,
    post_url: str = "",
    raw_response_text: Optional[str] = None,
) -> Dict[str, Optional[datetime]]:
    """
    게시글 페이지에서 **작성(등록/게시)일**과 **수정일**을 구분해 추출한다.

    반환:
    - posted: 등록일/작성일/게시일 계열
    - modified: 수정일/최종수정일 계열

    NOTE:
    - 기존 `extract_post_date()`는 단일 날짜만 반환하므로(작성일 우선 → 수정일 폴백),
      "수정일을 별도 변수로 DB에 전달"하려면 이 함수가 필요하다.
    - raw_response_text에 날짜가 박혀있는 케이스는 posted로 취급한다(구분 정보가 없기 때문).
    """
    posted_dt: Optional[datetime] = None
    modified_dt: Optional[datetime] = None

    # 광명시 상수도 계약사업목록 상세는 작성일 라벨이 없으므로 계약일자를 posted로 사용한다.
    try:
        if page_content and post_url:
            try:
                from bs4 import BeautifulSoup  # type: ignore[import-not-found]
            except Exception:  # pragma: no cover
                BeautifulSoup = None  # type: ignore[assignment]
            if BeautifulSoup:
                try:
                    from backend.board.anseong_board import extract_anseong_reg_date, is_anseong_url

                    if is_anseong_url(str(post_url)):
                        soup_anseong = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
                        dt_anseong = extract_anseong_reg_date(soup_anseong, url=str(post_url))
                        if dt_anseong:
                            return {"posted": dt_anseong, "modified": None}
                except Exception:
                    pass

                try:
                    from backend.board.gm_board import extract_gm_reg_date, is_gm_url

                    if is_gm_url(str(post_url)):
                        soup_gm = BeautifulSoup(page_content, "html.parser")  # type: ignore[misc]
                        dt_gm = extract_gm_reg_date(soup_gm, url=str(post_url))
                        if dt_gm:
                            return {"posted": dt_gm, "modified": None}
                except Exception:
                    pass
    except Exception:
        pass

    # 0) raw_response_text 기반 날짜 추출 (구분 불가 → posted로 취급)
    try:
        raw = raw_response_text if raw_response_text is not None else None
        if raw:
            dt0 = _extract_date_from_raw_response(raw)
            if dt0:
                posted_dt = dt0
    except Exception:
        pass

    # 태그 유연성: 글자와 날짜 사이에 HTML 태그나 공백이 있어도 인식 ([^>]*?>?.*? 사용)
    posted_patterns = [
        r'(?:등록일|작성일|게시일)[^>]*?>?.*?(\d{4})[-./](\d{1,2})[-./](\d{1,2})',
        r'(?:등록일|작성일|게시일)[^>]*?>?.*?(\d{4})\.(\d{1,2})\.(\d{1,2})',
        r'(?:등록일|작성일|게시일)[^>]*?>?.*?(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
        r'(?:등록일|작성일|게시일)[^0-9]{0,40}(\d{4})(\d{2})(\d{2})',
    ]
    modified_patterns = [
        r'(?:수정일|최종수정일)[^>]*?>?.*?(\d{4})[-./](\d{1,2})[-./](\d{1,2})',
        r'(?:수정일|최종수정일)[^>]*?>?.*?(\d{4})\.(\d{1,2})\.(\d{1,2})',
        r'(?:수정일|최종수정일)[^>]*?>?.*?(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
        r'(?:수정일|최종수정일)[^0-9]{0,40}(\d{4})(\d{2})(\d{2})',
    ]

    def _try_patterns(patterns: list[str]) -> Optional[datetime]:
        found: list[datetime] = []
        for pattern in patterns:
            matches = re.findall(pattern, page_content or "", re.IGNORECASE)
            for tup in matches or []:
                try:
                    year, month, day = tup
                    dt = _coerce_datetime(year, month, day)
                    if dt:
                        found.append(dt)
                except Exception:
                    continue
        return _pick_best_date(found, context=post_url)

    # 1) posted / modified를 각각 best-effort로 추출 (서로 독립)
    try:
        if posted_dt is None:
            posted_dt = _try_patterns(posted_patterns)
    except Exception:
        pass
    try:
        modified_dt = _try_patterns(modified_patterns)
    except Exception:
        modified_dt = None

    return {"posted": posted_dt, "modified": modified_dt}


def extract_date_from_text(text: str) -> Optional[datetime]:
    """
    텍스트(파일명, 라벨 등)에서 날짜를 추출
    """
    if not text:
        return None
    
    # 상대시간(방금 전/3시간 전 등) 우선 처리
    rel = _extract_relative_korean(text)
    if rel:
        return rel

    # 1. 전체 날짜 패턴 (YYYY-MM-DD, YYYY.MM.DD, YYYYMMDD 등)
    date_patterns = [
        r'(\d{4})[-./](\d{1,2})[-./](\d{1,2})',
        r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
        r'(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)',  # 20240101 형식(숫자 인접만 금지, 파일명/언더스코어 허용)
    ]

    candidates: list[datetime] = []
    for pattern in date_patterns:
        for m in re.finditer(pattern, text):
            try:
                if m and m.groups() and len(m.groups()) == 3:
                    year, month, day = m.groups()
                    dt = _coerce_datetime(year, month, day)
                    if dt:
                        candidates.append(dt)
            except Exception:
                continue

    best = _pick_best_date(candidates, context=text)
    if best:
        return best
    
    # 2. 연도만 있는 경우 (파일명 등에 '2022 광진구...' 처럼 연도만 명시된 경우 대응)
    # 1900~2099년 사이의 숫자를 찾음
    year_pattern = r'\b(19|20)\d{2}\b'
    year_matches = re.findall(year_pattern, text)
    if year_matches:
        # 텍스트에 4자리 숫자가 여러 개 있을 수 있으므로 첫 번째 유효한 연도를 사용
        for match in re.finditer(year_pattern, text):
            try:
                year = int(match.group(0))
                # 연도만 발견된 경우 해당 연도의 1월 1일로 설정하여 기간 필터링에 참여시킴
                return datetime(year, 1, 1)
            except ValueError:
                continue
                
    return None


def parse_date(date_str: str) -> Optional[datetime]:
    """
    문자열을 datetime으로 파싱
    """
    if not date_str:
        return None
    
    # 정규화 로직을 루프 전에 배치하여 성능과 정확도를 높입니다.
    clean_date = re.sub(r'[^0-9]', '', str(date_str))
    if len(clean_date) == 8:
        try:
            return datetime.strptime(clean_date, "%Y%m%d")
        except ValueError:
            pass # 8자리 숫자가 실제 날짜 형식이 아닐 경우 아래 루프로 진행

    date_formats = [
        '%Y-%m-%d',
        '%Y.%m.%d',
        '%Y/%m/%d',
        '%y-%m-%d',
        '%y.%m.%d',
        '%y/%m/%d',
        '%Y-%m-%d %H:%M',
        '%Y.%m.%d %H:%M',
        '%Y/%m/%d %H:%M',
        '%y-%m-%d %H:%M',
        '%y.%m.%d %H:%M',
        '%y/%m/%d %H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y.%m.%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%y-%m-%d %H:%M:%S',
        '%y.%m.%d %H:%M:%S',
        '%y/%m/%d %H:%M:%S',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%dT%H:%M:%S',
        '%Y년 %m월 %d일',
        '%Y년 %m월 %d일 %H시 %M분 %S초',
        '%Y년 %m월 %d일 %H시 %M분',
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
        except Exception:
            raise
    
    return None


def is_date_in_range(
    date_val: Optional[Union[datetime, date]], 
    start_date: Optional[Union[datetime, date]] = None, 
    end_date: Optional[Union[datetime, date]] = None
) -> bool:
    """
    [기존 함수] 날짜 객체가 지정된 범위 내에 있는지 확인합니다.
    """
    logger.debug("[date-debug] is_date_in_range enter | date_val=%s start_date=%s end_date=%s", date_val, start_date, end_date)
    if date_val is None:
        res = False if (start_date or end_date) else True
        logger.debug("[date-debug] is_date_in_range date_val is None -> %s", res)
        return res
    
    def _to_date_only(d):
        if isinstance(d, datetime): return d.date()
        return d

    target_date = _to_date_only(date_val)

    if start_date and target_date < _to_date_only(start_date):
        logger.debug("[date-debug] is_date_in_range -> target_date < start_date | target=%s start=%s", target_date, _to_date_only(start_date))
        return False
    if end_date and target_date > _to_date_only(end_date):
        logger.debug("[date-debug] is_date_in_range -> target_date > end_date | target=%s end=%s", target_date, _to_date_only(end_date))
        return False
    
    logger.debug("[date-debug] is_date_in_range -> True | target=%s start=%s end=%s", target_date, start_date, end_date)
    return True


def get_default_date_range(days: int = 30) -> tuple[datetime, datetime]:
    """
    기본 날짜 범위 반환 (최근 N일)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date

def normalize_to_datetime(date_str: str) -> Optional[datetime]:
    """
    지저분한 텍스트(예: '개최일자 20240417') 속에서 
    8자리 날짜 혹은 구분자가 있는 날짜를 찾아 datetime으로 변환합니다.
    """
    if not date_str:
        return None

    # 1. 전처리: 기호 통일 (년, 월, 점, 슬래시 -> 하이픈)
    clean = str(date_str).strip()
    clean = clean.replace("년", "-").replace("월", "-").replace("일", "")
    clean = clean.replace(".", "-").replace("/", "-")

    try:
        # [우선순위 1] 딱 8자리의 연속된 숫자만 있는 경우 (YYYYMMDD)
        # \b는 단어 경계로, 앞뒤에 다른 숫자가 붙어있지 않은 8자리만 찾습니다.
        match_8digit = re.search(r'\b(\d{8})\b', clean)
        if match_8digit:
            return datetime.strptime(match_8digit.group(1), "%Y%m%d")

        # [우선순위 2] 구분자로 연결된 날짜 패턴 (YYYY-MM-DD)
        # 텍스트 중간에 섞여 있어도 날짜 모양만 쏙 뽑아냅니다.
        match_sep = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', clean)
        if match_sep:
            year, month, day = match_sep.groups()
            return datetime(int(year), int(month), int(day))

        # [우선순위 3] 연도가 2자리인 패턴 (YY-MM-DD)
        match_short = re.search(r'\b(\d{2})-(\d{1,2})-(\d{1,2})\b', clean)
        if match_short:
            year, month, day = match_short.groups()
            return datetime(int("20" + year), int(month), int(day))

    except Exception:
        # 실제 날짜가 아닌 숫자(예: 20249999)인 경우 에러 방지
        return None
        
    return None
