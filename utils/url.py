# utils/url.py
"""
URL 관련 유틸리티 함수
"""
from __future__ import annotations
from typing import Any

import os
import json
import html as html_lib
import logging  # 추가: 시스템 로그 기록을 위한 모듈
import re
import posixpath
from posixpath import normpath
from urllib.parse import parse_qsl, urlencode, urlparse, urljoin, urlunparse, quote, unquote

import mimetypes

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logger = logging.getLogger(__name__)


from utils.crawl_url_normalizer import canonicalize_crawl_url

def normalize_attachment_href(href: str) -> str:
    """
    정적 HTML에 박힌 첨부 링크의 href 정규화.
    일부 공공기관 사이트는 속성값에 개행·탭이 들어가 경로가 깨지므로 제거한다.
    """
    if not href:
        return ""
    s = str(href).strip()
    if re.search(r"[\r\n\t]", s):
        s = re.sub(r"\s+", "", s)
    s = re.sub(r";jsessionid=[^/?#]+", "", s, flags=re.IGNORECASE)
    return s


logging.basicConfig(
    filename=os.path.join(LOG_DIR, "url_dedup.log"),
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# 허용된 파일 확장자 목록 (한 줄 주석: 수집 대상 문서 및 압축 파일 확장자 정의)
ALLOWED_FILE_EXT = (".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip")

def get_safe_url(url: str) -> str:
    # 1줄 설명: URL 내 한글, 공백 등 특수문자를 인코딩하여 서버 전송 오류를 방지함
    if not url: return ""
    parsed = urlparse(url.strip())
    # 경로(path)와 쿼리(query)만 선택적으로 인코딩하여 구조를 유지함
    encoded_path = quote(parsed.path)
    encoded_query = urlencode(parse_qsl(parsed.query), doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, encoded_path, parsed.params, encoded_query, parsed.fragment))

def infer_content_type(url: str, header_content_type: str) -> str:
    # 1줄 설명: 'file' 등 비표준 헤더가 올 경우 URL 파라미터를 분석해 실제 파일 타입을 추론함
    header_type = (header_content_type or "").lower().split(';')[0].strip()
    
    # 서버 응답이 'file', 'octet-stream'이거나 비어있을 때만 추론 실행
    if header_type in ["file", "application/octet-stream", ""]:
        params = dict(parse_qsl(urlparse(url).query))
        # 공공기관에서 주로 쓰는 파일명 파라미터 확인
        file_name = params.get('user_file_nm') or params.get('sys_file_nm') or params.get('file_nm')
        if file_name:
            inferred, _ = mimetypes.guess_type(file_name)
            if inferred: return inferred
            
    return header_type
    
def _is_valid_download_candidate(path: str) -> bool:
    # Accept direct file paths and known download-handler paths.
    if not path: return False
    p = path.strip().lower()
    if "/" not in p and not p.startswith("http"):
        return False
    handler_hints = (
        "/file/download/",
        "/download/uu/",
        "filedown",
        "filedownload",
        "download.do",
        "download.jsp",
        "downloadbbsfile",
        "atchfile",
        "atchmnfl",
    )
    if any(hint in p for hint in handler_hints):
        return True
    return p.endswith(ALLOWED_FILE_EXT)
def _encode_url(url: str) -> str:
    # 1줄 설명: 주소 내 공백이나 특수문자를 인코딩하여 서버 인식 오류를 방지함
    return quote(url, safe=":/?=&%#")

def extract_domain(url: str) -> str:
    """
    URL에서 도메인을 추출합니다.
    """
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host

IGNORE_PARAMS = {
    "key",
    "cpn",
    "rcpp",
    "callScreen"
}

TRACKING_PARAMS = {
    "utm_source","utm_medium","utm_campaign",
    "utm_term","utm_content","fbclid","gclid"
}


def normalize_url(url: str, base_url: str | None = None) -> str:
    """
    URL을 token 기반 key로 변환 (중복 비교용)
    """

    tokens = url_to_tokens(url, base_url)

    if not tokens:
        return ""

    return "|".join(sorted(tokens))


def _source_page_go_download_url(
    js_text: str,
    base_url: str | None,
    page_script: str | None,
) -> str:
    """Resolve goDownload from the endpoint declared by its source page."""
    if not page_script:
        return ""
    try:
        call = re.search(
            r"godownload\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            js_text,
            re.IGNORECASE,
        )
        if not call:
            return ""
        handler = re.search(
            r"function\s+godownload\s*\([^)]*\)\s*\{(?P<body>.*?)\}",
            str(page_script),
            re.IGNORECASE | re.DOTALL,
        )
        if not handler:
            return ""
        endpoint_match = re.search(
            r"['\"](?P<endpoint>(?:https?:)?//[^'\"]*?/emwp/jsp/ofr/FileDown(?:New)?\.jsp|/emwp/jsp/ofr/FileDown(?:New)?\.jsp)[^'\"]*['\"]",
            handler.group("body"),
            re.IGNORECASE,
        )
        if not endpoint_match:
            return ""
        endpoint = html_lib.unescape(endpoint_match.group("endpoint").strip())
        if endpoint.startswith("//"):
            endpoint = "https:" + endpoint
        elif endpoint.startswith("/") and base_url:
            endpoint = urljoin(base_url, endpoint)
        parsed = urlparse(endpoint)
        if not parsed.scheme or not parsed.netloc:
            return ""
        query = dict(parse_qsl(parsed.query or "", keep_blank_values=True))
        query.update(
            {
                "user_file_nm": call.group(1).strip(),
                "sys_file_nm": call.group(2).strip(),
                "file_path": call.group(3).strip(),
            }
        )
        return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
    except Exception:
        return ""


def extract_download_url_from_js(
    js_text: str,
    base_url: str | None = None,
    *,
    page_script: str | None = None,
) -> str:
    """
    'javascript:...' 형태의 클릭 다운로드 문자열에서 실제 다운로드 URL을 추출합니다.

    지원(대표):
    - javascript:downloadFile('/attach/...pdf', '파일명', 'pdf');
    - javascript:downloadFile('file.pdf', 'pdf');
    - javascript:fn_egov_downFile('atchFileId','fileSn');
    """
    if not js_text:
        return ""
    try:
        s = str(js_text).strip()
    except Exception:
        return ""

    if not s:
        return ""

    source_handler_url = _source_page_go_download_url(s, base_url, page_script)
    if source_handler_url:
        # The handler builder already uses urlencode(). Running the generic
        # encoder again would turn query-space '+' into the literal '%2B'.
        return source_handler_url

    # 이미 URL/경로인 경우
    if s.startswith(("http://", "https://")):
        return _encode_url(s)
    if base_url and s.startswith(("/", "./", "../")):
        try:
            return _encode_url(urljoin(base_url, s))
        except Exception:
            return s

    ls = s.lower()
    if not ls.startswith("javascript:") and re.search(
        r"(?:downloadfile|fn_egov_downfile|cfbrctfiledownload|cfcomnfiledownload|godownload|filedownnow)\s*\(",
        s,
        re.IGNORECASE,
    ):
        s = f"javascript:{s}"
        ls = s.lower()
    # location.href='/download/...' style handlers are common on file buttons.
    try:
        m = re.search(
            r"(?:window\.)?location(?:\.href)?\s*=\s*['\"]([^'\"]+)['\"]",
            s,
            re.IGNORECASE,
        )
        if m:
            cand = (m.group(1) or "").strip()
            if _is_valid_download_candidate(cand):
                final_url = urljoin(base_url, cand) if base_url else cand
                return _encode_url(final_url)
    except Exception:
        pass
    if not ls.startswith("javascript:"):
        # javascript가 아니면 그대로 반환(상대경로는 base_url이 있으면 join)
        if s.startswith(("http://", "https://")): return _encode_url(s)
        if base_url and s.startswith(("/", "./", "../")):
            return _encode_url(urljoin(base_url, s))
        return ""

    # 1) downloadFile('...') 첫 번째 인자를 경로로 취급
    try:
        m = re.search(r"downloadfile\s*\(\s*['\"]([^'\"]+)['\"]", s, re.IGNORECASE)
        if m:
            cand = (m.group(1) or "").strip()
            if not _is_valid_download_candidate(cand):
                return ""

            final_url = urljoin(base_url, cand) if base_url else cand
            return _encode_url(final_url)
    except Exception: pass

    # 2) 전자정부프레임워크(egov) 다운로드 핸들러
    #    fn_egov_downFile('ATCHFILEID','FILESN') -> /cmm/fms/FileDown.do?atchFileId=...&fileSn=...
    try:
        m = re.search(
            r"fn_egov_downfile\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            s,
            re.IGNORECASE,
        )
        if m:
            atch = (m.group(1) or "").strip()
            sn = (m.group(2) or "").strip()
            if atch and sn:
                cand = f"/cmm/fms/FileDown.do?atchFileId={atch}&fileSn={sn}"
                if base_url:
                    try:
                        return urljoin(base_url, cand)
                    except Exception:
                        return cand
                return cand
    except Exception:
        pass

    # 2-1) 행안부 NPAS 게시물 첨부
    #    cfBrctFileDownload('BRCT', '1726')
    #    -> /nsbms/comn/fileMng/fileDownload.do?bizDvCd=BRCT&fileNo=0&fileSeq=1726
    try:
        m = re.search(
            r"cfbrctfiledownload\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            s,
            re.IGNORECASE,
        )
        if m:
            biz_dv_cd = (m.group(1) or "").strip()
            file_seq = (m.group(2) or "").strip()
            if biz_dv_cd and file_seq:
                cand = f"/nsbms/comn/fileMng/fileDownload.do?bizDvCd={quote(biz_dv_cd)}&fileNo=0&fileSeq={quote(file_seq)}"
                if base_url:
                    try:
                        return urljoin(base_url, cand)
                    except Exception:
                        return cand
                return cand
    except Exception:
        pass

    # 2-2) 행안부 NPAS 공통 첨부
    #    cfComnFileDownload('BRCT', '12', '1726')
    #    -> /nsbms/comn/fileMng/fileDownload.do?bizDvCd=BRCT&fileNo=12&fileSeq=1726
    try:
        m = re.search(
            r"cfcomnfiledownload\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            s,
            re.IGNORECASE,
        )
        if m:
            biz_dv_cd = (m.group(1) or "").strip()
            file_no = (m.group(2) or "").strip()
            file_seq = (m.group(3) or "").strip()
            if biz_dv_cd and file_no and file_seq:
                cand = (
                    "/nsbms/comn/fileMng/fileDownload.do"
                    f"?bizDvCd={quote(biz_dv_cd)}&fileNo={quote(file_no)}&fileSeq={quote(file_seq)}"
                )
                if base_url:
                    try:
                        return urljoin(base_url, cand)
                    except Exception:
                        return cand
                return cand
    except Exception:
        pass

    # 3) fallback: JS 문자열 안의 quoted URL/경로 후보를 느슨하게 추출
    #    - 기존 전용 패턴이 실패했을 때만 동작
    #    - fileDown/fileDownload/downloadBbsFile/fileDown.do 등 다운로드 힌트가 있거나
    #      문서 확장자가 보이는 상대/절대 경로를 반환
    try:
        m = re.search(
            r"filedownnow\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            s,
            re.IGNORECASE,
        )
        if m:
            sys_file_nm = (m.group(1) or "").strip()
            user_file_nm = (m.group(2) or "").strip()
            file_path = (m.group(3) or "").strip()
            if user_file_nm and sys_file_nm and file_path:
                return (
                    "https://eminwon.gm.go.kr/emwp/jsp/ofr/FileDownNew.jsp"
                    f"?user_file_nm={quote(user_file_nm)}"
                    f"&sys_file_nm={quote(sys_file_nm)}"
                    f"&file_path={quote(file_path, safe='/')}"
                )
    except Exception:
        pass

    try:
        m = re.search(
            r"godownload\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            s,
            re.IGNORECASE,
        )
        if m:
            user_file_nm = (m.group(1) or "").strip()
            sys_file_nm = (m.group(2) or "").strip()
            file_path = (m.group(3) or "").strip()
            if user_file_nm and sys_file_nm and file_path:
                cand = (
                    "/emwp/jsp/ofr/FileDownNew.jsp"
                    f"?user_file_nm={quote(user_file_nm)}"
                    f"&sys_file_nm={quote(sys_file_nm)}"
                    f"&file_path={quote(file_path, safe='/')}"
                )
                if base_url:
                    try:
                        return urljoin(base_url, cand)
                    except Exception:
                        return cand
                return cand
    except Exception:
        pass

    # CRAS copyright board attachments:
    # downItem('22016^original.hwp^1516349864398.hwp^201801')
    # becomes a GET-equivalent URL for the site's /fileDownload.do handler.
    try:
        m = re.search(r"downitem\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", s, re.IGNORECASE)
        if m:
            payload = (m.group(1) or "").strip()
            parts = payload.split("^")
            if len(parts) >= 4:
                file_size = (parts[0] or "").strip()
                orgfilename = (parts[1] or "").strip()
                file_name = (parts[2] or "").strip()
                file_path = (parts[3] or "").strip()
                if orgfilename and file_name and file_path:
                    query = urlencode(
                        {
                            "fileGubun": "BBS",
                            "gubun": "BBS",
                            "fileSize": file_size,
                            "orgfilename": orgfilename,
                            "fileName": file_name,
                            "filePath": file_path,
                            "type": "",
                        }
                    )
                    cand = f"/fileDownload.do?{query}"
                    if base_url:
                        try:
                            return urljoin(base_url, cand)
                        except Exception:
                            return cand
                    return cand
    except Exception:
        pass

    try:
        file_hint_tokens = (
            "filedown", "filedownload", "downloadbbsfile", "download",
            "attach", "atchfile", "atchmnflno", "atchfileid", "fileid",
        )
        quoted_candidates = re.findall(r"['\"]([^'\"]{1,500})['\"]", s)
        for raw in quoted_candidates:
            cand = (raw or "").strip()
            if not cand:
                continue
            lc = cand.lower()

            looks_like_path = cand.startswith(("http://", "https://", "/", "./", "../"))
            has_file_hint = any(tok in lc for tok in file_hint_tokens)
            has_file_ext = any(ext in lc for ext in ALLOWED_FILE_EXT)
            if not ((looks_like_path and has_file_hint) or _is_valid_download_candidate(cand) or (looks_like_path and has_file_ext)):
                continue

            try:
                final_url = urljoin(base_url, cand) if (base_url and not cand.startswith(("http://", "https://"))) else cand
            except Exception:
                final_url = cand
            if final_url:
                return _encode_url(final_url)
    except Exception:
        pass

    # 그 외(fileDown('12345') 등)는 사이트별로 엔드포인트가 달라 범용 변환이 어렵다.
    return ""
    
def is_same_domain(url1: str, url2: str) -> bool:
    """
    두 URL이 같은 도메인인지 확인합니다.
    """
    domain1 = extract_domain(url1)
    domain2 = extract_domain(url2)
    return domain1.endswith(domain2) or domain2.endswith(domain1)

def clean_url(url: str) -> str:
    """
    URL에서 쿼리 파라미터와 프래그먼트를 제거합니다.
    """
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

def ensure_url_scheme(url_or_domain: Any, default_scheme: str = "https") -> str:
    """
    도메인 문자열이나 URL을 완전한 URL로 변환합니다.
    입력값이 딕셔너리인 경우 'url' 키의 값을 추출하여 처리합니다.
    
    Args:
        url_or_domain: URL 또는 도메인 문자열 (예: "gwangjin.go.kr" 또는 "https://example.com") 또는 딕셔너리
        default_scheme: 기본 스킴 (기본값: "https")
    
    Returns:
        완전한 URL 문자열 (예: "https://gwangjin.go.kr")
    
    Examples:
        >>> ensure_url_scheme("gwangjin.go.kr")
        "https://gwangjin.go.kr"
        >>> ensure_url_scheme({"url": "example.com", "type": "post"})
        "https://example.com"
    """
    if not url_or_domain:
        return ""
    
    # ✅ 딕셔너리 형태로 데이터가 들어왔을 때 'url' 값만 추출하는 방어 로직
    if isinstance(url_or_domain, dict):
        url_or_domain = url_or_domain.get("url", "")
        
    if not url_or_domain:
        return ""
    
    # 이제 url_or_domain은 확실한 문자열(str)이므로 안전하게 strip() 호출 가능
    url_or_domain = str(url_or_domain).strip()
    
    # 이미 완전한 URL인 경우 (http:// 또는 https://로 시작)
    if url_or_domain.startswith(('http://', 'https://')):
        return url_or_domain
    
    # 프로토콜 상대 URL인 경우 (//로 시작)
    if url_or_domain.startswith('//'):
        return f"{default_scheme}:{url_or_domain}"
    
    # 도메인 문자열인 경우 (예: "gwangjin.go.kr")
    # URL로 변환
    return f"{default_scheme}://{url_or_domain}"

def ensure_songpa_www_path(url: str) -> str:
    """
    송파구청 사이트에서 /www 경로가 필수인 상세/목록 URL을 보정합니다.
    - 대상: songpa.go.kr 도메인
    - 경로: /selectBbsNttView.do, /selectBbsNttList.do
    """
    if not url:
        return ""
    try:
        p = urlparse(url)
    except Exception:
        return url
    host = (p.netloc or "").lower()
    if not host.endswith("songpa.go.kr"):
        return url
    path = p.path or ""
    if path.startswith("/www/"):
        return url
    if path.startswith("/selectBbsNttView.do") or path.startswith("/selectBbsNttList.do"):
        new_path = "/www" + path
        return urlunparse((p.scheme, p.netloc, new_path, p.params, p.query, p.fragment))
    return url


# -----------------------------
# Dedup/canonical helpers
# -----------------------------

_TRACKING_KEYS_EXACT = {
    "fbclid",
    "gclid",
    "msclkid",
    "igshid",
    "ysclid",
    "_ga",
    "_gl",
    "mc_cid",
    "mc_eid",
}

_VOLATILE_KEYS_EXACT = {
    "session",
    "sessionid",
    "sid",
    "phpsessid",
    "jsessionid",
    "JSESSIONID",
    "_csrf",
    "csrf",
    "token",
    "access_token",
    "refresh_token",
}

# 페이지/검색 등 변동성 있는 쿼리 키 — 같은 글이라도 값이 달라질 수 있으므로 정규화 시 제거
# (예: oka.go.kr brdDetail.do?currentPage=1&menu_cd=...&num=...&searchData=&searchText= → menu_cd, num만 유지)
_PAGE_AND_SEARCH_KEYS = {
    "buffer",
    "bacategory",
    "bacategory1",
    "bacategory2",
    "bacategory3",
    "currentpage",
    "currpage",
    "cp",
    "page",
    "pageindex",
    "pageno",
    "pagenum",
    "pg",
    "offset",
    "start",
    "startrow",
    "startindex",
    "limit",
    "row",
    "rows",
    "rowperpage",
    "pageunit",
    "pagesize",
    "recordcountperpage",
    "optionalyn",
    "searchdata",
    "searchtext",
    "searchkeyword",
    "searchcondition",
    "searchtype",
    "searchorder",
    "searchstate",
    "sort",
    "order",
    "orderby",
    "sortorder",
    "viewtype",
    "view",
    "p",
    "search",
    "query",
    "keyword",
}

_TRACKING_KEYS_EXACT_LOWER = {x.lower() for x in _TRACKING_KEYS_EXACT}
_VOLATILE_KEYS_EXACT_LOWER = {x.lower() for x in _VOLATILE_KEYS_EXACT}

# regex 미리 컴파일 (성능)
_JSID_RE = re.compile(r";jsessionid=[^/?#]+", re.I)
_MULTI_SLASH_RE = re.compile(r"/{2,}")

# -----------------------------
# Dedup/canonical helpers
# -----------------------------
PRIMARY_KEYS = {
    "nttid",
    "bbsid",
    "articleid",
    "boardid",
    "searchlctrekey",
    "seq",
    "idx",
    "id",
    "num",
    "no",
    "ntt_no",
    "nttno",
    "postid",
    "docid",
    "progrmsn",
}

_BBS_DETAIL_STABLE_QUERY_KEYS = {
    "q_bbscode",
    "q_bbscttsn",
}

_NFTC_BBS_DETAIL_STABLE_QUERY_KEYS = {
    "q_nftcbbscode",
    "q_nftcbbsmgtno",
}

_CONTEXT_QUERY_IDENTITY_KEYS = {
    "deptid",
    "key",
    "menu",
    "menuid",
    "menuno",
    "mnno",
}

_GENERIC_BOARD_DETAIL_STABLE_QUERY_KEYS = {
    "bbsid",
    "nttid",
}

_GENERIC_NTT_DETAIL_ALLOWED_QUERY_KEYS = {
    "bbsid",
    "nttid",
}

_BBS_DETAIL_CANONICAL_NAME_BY_SUFFIX = {
    "bd_selectbbs.do": "BD_selectBbs.do",
    "bd_selectbeffatinfoothbcbbs.do": "BD_selectBbs.do",
    "bd_selectnftcbbsdetail.do": "BD_selectNftcBbsDetail.do",
    "selectboarddetail.do": "selectBoardDetail.do",
}


def _dedup_match_query_key(query_key: str) -> str:
    key = (query_key or "").strip().lower()
    if key.startswith("q_") and len(key) > 2:
        return key[2:]
    return key


def _looks_like_detail_query_path(path: str) -> bool:
    suffix = _bbs_detail_suffix(path)
    if suffix in _BBS_DETAIL_CANONICAL_NAME_BY_SUFFIX:
        return True
    path_lower = (path or "").strip().lower()
    return suffix.endswith((".do", ".jsp", ".php", ".asp", ".aspx")) and any(
        token in path_lower
        for token in ("detail", "view", "article", "bbs", "board", "read", "select")
    )


def _is_identity_query_key(query_key: str) -> bool:
    kk = (query_key or "").strip().lower()
    if not kk:
        return False
    match_key = _dedup_match_query_key(kk)
    if kk in _CONTEXT_QUERY_IDENTITY_KEYS or match_key in _CONTEXT_QUERY_IDENTITY_KEYS:
        return False
    if kk in _PAGE_AND_SEARCH_KEYS or match_key in _PAGE_AND_SEARCH_KEYS:
        return False
    if kk in _TRACKING_KEYS_EXACT_LOWER or match_key in _TRACKING_KEYS_EXACT_LOWER:
        return False
    if kk in _VOLATILE_KEYS_EXACT_LOWER or match_key in _VOLATILE_KEYS_EXACT_LOWER:
        return False
    if kk in PRIMARY_KEYS or match_key in PRIMARY_KEYS:
        return True
    if match_key in {"bbscode", "bbscttsn", "bbsmgtno", "mgtno", "nftcbbscode", "nftcbbsmgtno"}:
        return True
    if match_key.endswith(("id", "no", "num", "seq", "sn", "mgtno")):
        return True
    if match_key.endswith("code"):
        return True
    return False


def _is_discardable_context_query_key(query_key: str) -> bool:
    kk = (query_key or "").strip().lower()
    if not kk:
        return True
    match_key = _dedup_match_query_key(kk)
    if match_key.startswith("utm_"):
        return True
    if kk in _TRACKING_KEYS_EXACT_LOWER or match_key in _TRACKING_KEYS_EXACT_LOWER:
        return True
    if kk in _VOLATILE_KEYS_EXACT_LOWER or match_key in _VOLATILE_KEYS_EXACT_LOWER:
        return True
    if kk in _PAGE_AND_SEARCH_KEYS or match_key in _PAGE_AND_SEARCH_KEYS:
        return True
    if (kk.startswith("search") or match_key.startswith("search")) and not _is_identity_query_key(kk):
        return True
    return False


def _stable_detail_identity_keys(path: str, present_keys: set[str]) -> set[str]:
    if not _looks_like_detail_query_path(path):
        return set()
    keys = {key for key in present_keys if _is_identity_query_key(key)}
    if len(keys) < 2:
        return set()
    extra_keys = set(present_keys) - keys
    if any(not _is_discardable_context_query_key(key) for key in extra_keys):
        return set()
    return keys


def _bbs_detail_suffix(path: str) -> str:
    path_lower = (path or "").strip().lower().rstrip("/")
    if not path_lower:
        return ""
    return path_lower.rsplit("/", 1)[-1]


def _is_stable_bbs_detail_query(path: str, present_keys: set[str]) -> bool:
    suffix = _bbs_detail_suffix(path)
    return (
        (
            suffix in _BBS_DETAIL_CANONICAL_NAME_BY_SUFFIX
            and _BBS_DETAIL_STABLE_QUERY_KEYS.issubset(present_keys)
        )
        or (
            suffix in _BBS_DETAIL_CANONICAL_NAME_BY_SUFFIX
            and _NFTC_BBS_DETAIL_STABLE_QUERY_KEYS.issubset(present_keys)
        )
        or (
            suffix == "selectboarddetail.do"
            and _GENERIC_BOARD_DETAIL_STABLE_QUERY_KEYS.issubset(present_keys)
        )
        or bool(_stable_detail_identity_keys(path, present_keys))
    )


def _is_generic_ntt_detail_query(path: str, present_keys: set[str]) -> bool:
    path_lower = (path or "").strip().lower()
    suffix = _bbs_detail_suffix(path_lower)
    if "nttid" not in present_keys:
        return False
    if not any(part in path_lower for part in ("/bbs/", "/board/", "/cop/bbs/")):
        return False
    return suffix in {
        "view.do",
        "selectboardarticle.do",
        "selectboarddetail.do",
        "selectnoticearticle.do",
        "selectnoticedetail.do",
    }


def _canonicalize_stable_bbs_detail_path(path: str, present_keys: set[str]) -> str:
    if not _is_stable_bbs_detail_query(path, present_keys):
        return path or "/"

    suffix = _bbs_detail_suffix(path)
    canonical_name = _BBS_DETAIL_CANONICAL_NAME_BY_SUFFIX.get(suffix)
    if not canonical_name:
        return path or "/"
    return f"/__bbs_detail__/{canonical_name}"


def _filter_query_pairs_for_dedup(path: str, pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    present_keys = {
        (k or "").strip().lower()
        for k, v in pairs
        if (k or "").strip() and (v or "").strip() != ""
    }
    stable_bbs_detail = _is_stable_bbs_detail_query(path, present_keys)
    generic_ntt_detail = _is_generic_ntt_detail_query(path, present_keys)

    filtered: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for k, v in pairs:
        kk = (k or "").strip().lower()
        vv = (v or "").strip()

        if not kk or not vv:
            continue

        match_key = _dedup_match_query_key(kk)

        # eGov board detail pages often carry context filters that do not
        # change the actual article identity.
        if stable_bbs_detail:
            generic_identity_keys = _stable_detail_identity_keys(path, present_keys)
            allowed_keys = generic_identity_keys or _BBS_DETAIL_STABLE_QUERY_KEYS
            if _NFTC_BBS_DETAIL_STABLE_QUERY_KEYS.issubset(present_keys):
                allowed_keys = _NFTC_BBS_DETAIL_STABLE_QUERY_KEYS
            if _bbs_detail_suffix(path) == "selectboarddetail.do":
                allowed_keys = _GENERIC_BOARD_DETAIL_STABLE_QUERY_KEYS
            if kk not in allowed_keys:
                continue

        if generic_ntt_detail and kk not in _GENERIC_NTT_DETAIL_ALLOWED_QUERY_KEYS:
            continue

        if match_key.startswith("utm_"):
            continue

        if kk in _TRACKING_KEYS_EXACT_LOWER or match_key in _TRACKING_KEYS_EXACT_LOWER:
            continue

        if kk in _VOLATILE_KEYS_EXACT_LOWER or match_key in _VOLATILE_KEYS_EXACT_LOWER:
            continue

        if kk in _PAGE_AND_SEARCH_KEYS or match_key in _PAGE_AND_SEARCH_KEYS:
            continue

        # board_content_workflow._normalize_board_url 와 동일하게 search* 계열은
        # 게시물 identity 가 아닌 탐색/필터 문맥으로 보고 dedup 에서 제거한다.
        if (kk.startswith("search") or match_key.startswith("search")) and not _is_identity_query_key(kk):
            continue

        if (kk, vv) in seen:
            continue

        seen.add((kk, vv))
        filtered.append((kk, vv))

    return filtered


def build_dedup_candidate_terms(url: str, base_url: str | None = None) -> list[str]:
    """
    DB 후보군을 좁히기 위한 SQL LIKE probe terms를 만든다.
    실제 중복 판정은 build_dedup_keys()/canonical 비교로 별도 수행한다.
    """

    raw = (str(url).strip() if url else "")
    if not raw:
        return []

    if raw.startswith(("javascript:", "mailto:", "#")):
        return []

    if raw.startswith("//"):
        raw = "https:" + raw

    if not raw.startswith(("http://", "https://")):
        if base_url:
            raw = urljoin(base_url, raw)
        else:
            raw = "https://" + raw.lstrip("/")

    try:
        p = urlparse(raw)
    except Exception:
        return []

    pairs = parse_qsl(p.query or "", keep_blank_values=False)
    present_keys = {
        (k or "").strip().lower()
        for k, v in pairs
        if (k or "").strip() and (v or "").strip() != ""
    }
    filtered = _filter_query_pairs_for_dedup(p.path or "", pairs)

    terms: list[str] = []
    seen: set[str] = set()

    def _push(term: str, *, min_len: int = 2) -> None:
        value = str(term or "").strip().lower()
        if not value or len(value) < min_len or value in seen:
            return
        seen.add(value)
        terms.append(value)

    if _is_stable_bbs_detail_query(p.path or "", present_keys):
        for kk, vv in filtered:
            _push(f"{kk}={vv}", min_len=4)
        return terms

    path = (p.path or "").strip().lower()
    if path:
        _push(path, min_len=3)
        _push(_bbs_detail_suffix(path), min_len=3)

    for kk, vv in filtered:
        _push(f"{kk}={vv}", min_len=4)

    return terms


def urls_match_for_dedup(url1: str, url2: str, base_url: str | None = None) -> bool:
    """두 URL이 dedup 관점에서 같은 대상을 가리키는지 판정한다."""
    left = str(url1 or "").strip()
    right = str(url2 or "").strip()
    if not left or not right:
        return False
    if left == right:
        return True

    left_keys = build_dedup_keys(left, base_url)
    right_keys = build_dedup_keys(right, base_url)
    if left_keys and right_keys and left_keys.intersection(right_keys):
        return True

    left_canon = canonicalize_url_for_dedup(left, base_url) or left
    right_canon = canonicalize_url_for_dedup(right, base_url) or right
    return bool(left_canon and right_canon and left_canon == right_canon)

def url_to_tokens(url: str, base_url: str | None = None) -> set[str]:
    """
    URL을 키워드(token) 집합으로 변환하여 동일 경로 판단에 사용
    """

    raw = (str(url).strip() if url else "")
    if not raw:
        return set()

    if raw.startswith(("javascript:", "mailto:", "#")):
        return set()

    if raw.startswith("//"):
        raw = "https:" + raw

    if not raw.startswith(("http://", "https://")):
        if base_url:
            raw = urljoin(base_url, raw)
        else:
            raw = "https://" + raw.lstrip("/")

    try:
        p = urlparse(raw)
    except Exception:
        return set()

    pairs = parse_qsl(p.query or "", keep_blank_values=False)
    present_keys = {
        (k or "").strip().lower()
        for k, v in pairs
        if (k or "").strip() and (v or "").strip() != ""
    }

    tokens = set()

    # domain
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]

    tokens.add(host)

    # path 마지막 키워드 사용
    path = normpath(_canonicalize_stable_bbs_detail_path(p.path or "/", present_keys))
    parts = [x for x in path.lower().split("/") if x]

    if parts:
        tokens.add(parts[-1])

    for kk, vv in _filter_query_pairs_for_dedup(p.path or "", pairs):
        tokens.add(f"{kk}={vv}")

    return tokens

def canonicalize_url_for_dedup(url: str, base_url: str | None = None) -> str:
    """
    중복 비교용 URL 정규화. DB 저장/조회 및 수집 단계에서 동일 글 판별에 사용한다.
    - 스킴·호스트 소문자, www·기본 포트 제거
    - 트래킹/세션/페이지·검색 쿼리 제거, jsessionid 제거
    - 쿼리: 키워드 조합 방식 — 제거 대상이 아닌 모든 (key, value)를 (키, 값) 순으로 정렬해 조합
    - 경로 normpath, trailing slash 제거, fragment 제거
    - base_url이 주어지면 상대 URL을 절대 URL로 만든 뒤 정규화.
    """
    raw = (str(url).strip() if url else "")
    if not raw:
        return ""

    # javascript/mailto 제거
    if raw.startswith(("javascript:", "mailto:", "#")):
        return ""

    # protocol-relative URL
    if raw.startswith("//"):
        raw = "https:" + raw

    # 상대 URL 처리
    if not raw.startswith(("http://", "https://")):
        if base_url:
            raw = urljoin(base_url, raw)
        else:
            raw = "https://" + raw.lstrip("/")

    # jsessionid 제거
    raw = _JSID_RE.sub("", raw)

    if "://" not in raw:
        raw = "https://" + raw.lstrip("/")

    try:
        p = urlparse(raw)
    except Exception:
        return raw

    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()

    # IDN domain 처리
    try:
        netloc = netloc.encode("idna").decode("ascii")
    except Exception:
        pass

    # www 제거
    if netloc.startswith("www."):
        netloc = netloc[4:]

    # default port 제거
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]

    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    # fragment 제거
    fragment = ""

    pairs = parse_qsl(p.query or "", keep_blank_values=False)
    present_keys = {
        (k or "").strip().lower()
        for k, v in pairs
        if (k or "").strip() and (v or "").strip() != ""
    }
    filtered = _filter_query_pairs_for_dedup(p.path or "", pairs)

    # 키워드 조합: (key, value) 쌍을 키·값 순으로 정렬하여 일관된 canonical query 생성
    filtered.sort(key=lambda x: (x[0], x[1]))
    query = urlencode(filtered, doseq=True)

    # path normalize
    path = normpath(_canonicalize_stable_bbs_detail_path(p.path or "/", present_keys))

    # double slash 제거
    path = _MULTI_SLASH_RE.sub("/", path)

    if path != "/" and path.endswith("/"):
        path = path[:-1]

    # percent encoding normalize
    path = quote(unquote(path), safe="/")

    return urlunparse((scheme, netloc, path, "", query, fragment))


def build_dedup_keys(url: str, base_url: str | None = None) -> set[str]:
    """
    URL에서 여러 dedup key 생성
    하나라도 일치하면 동일 글로 판단
    """

    keys = set()

    if not url:
        return keys

    # canonical key
    canon = canonicalize_url_for_dedup(url, base_url)
    if canon:
        keys.add(canon)

    # token key
    token = normalize_url(url, base_url)
    if token:
        keys.add(token)

    try:
        p = urlparse(url)

        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]

        pairs = parse_qsl(p.query or "", keep_blank_values=False)
        present_keys = {
            (k or "").strip().lower()
            for k, v in pairs
            if (k or "").strip() and (v or "").strip() != ""
        }
        filtered_pairs = _filter_query_pairs_for_dedup(p.path or "", pairs)

        if _is_stable_bbs_detail_query(p.path or "", present_keys):
            stable_parts = {k: v for k, v in filtered_pairs}
            if _bbs_detail_suffix(p.path or "") == "selectboarddetail.do":
                bbs_code = stable_parts.get("bbsid")
                bbs_ctt = stable_parts.get("nttid")
                if bbs_code and bbs_ctt:
                    keys.add(f"{host}|bbs_detail|bbsid={bbs_code}|nttid={bbs_ctt}")
            else:
                bbs_code = stable_parts.get("q_bbscode")
                bbs_ctt = stable_parts.get("q_bbscttsn")
                if bbs_code and bbs_ctt:
                    keys.add(f"{host}|bbs_detail|q_bbscode={bbs_code}|q_bbscttsn={bbs_ctt}")

        for k, v in pairs:

            kk = (k or "").lower().strip()
            vv = (v or "").strip()

            if kk in PRIMARY_KEYS and vv:

                # seq=123
                keys.add(f"{kk}={vv}")

                # domain + seq
                keys.add(f"{host}|{kk}={vv}")

    except Exception:
        pass

    return keys

def save_compare_result_as_json(url1: str, url2: str, filename: str = "compare_result.json"):
    # 1줄 설명: 비교 결과를 로그가 아닌 실제 독립된 .json 파일로 물리적으로 저장함
    data = build_compare_json(url1, url2)
    
    # 1줄 설명: 지정된 파일명으로 JSON 데이터를 쓰고 파일을 닫음
    with open(os.path.join(LOG_DIR, filename), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        

def normalize_debug(url: str, base_url: str | None = None) -> dict:
    # 1줄 설명: URL을 토큰화하고 정규화된 결과를 딕셔너리로 반환함 (JSON 출력 가능하게 list 변환)
    tokens = url_to_tokens(url, base_url)
    normalized = "|".join(sorted(tokens)) if tokens else ""

    return {
        "raw_url": url,
        "tokens": list(tokens) if tokens else [],  # set을 list로 변환하여 JSON 에러 방지
        "normalized": normalized
    }

def build_compare_json(url1: str, url2: str, base_url: str | None = None) -> dict:
    # 1줄 설명: 두 URL의 정규화 데이터를 각각 생성하여 서로 일치하는지 비교한 결과를 반환함
    a = normalize_debug(url1, base_url)
    b = normalize_debug(url2, base_url)

    return {
        "compare": [a, b],
        "equal_normalized": a["normalized"] == b["normalized"]
    }

def log_compare(url1: str, url2: str, base_url: str | None = None):
    # 1줄 설명: 두 URL의 비교 분석 결과(JSON)를 시스템 로그에 기록함
    try:
        data = build_compare_json(url1, url2, base_url)
        logger.info("[URL-DEDUP] %s", json.dumps(data, ensure_ascii=False))
    except Exception as e:
        logger.error("[URL-DEDUP] 로그 기록 실패: %s", str(e))
