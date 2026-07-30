"""
도메인별 기본 게시판(목록) URL 매핑 설정.

UI에서는 항상 최상위 도메인만 입력하지만,
여기서 정의된 도메인 맵을 통해 실제 크롤링 시작 URL을
게시판 목록 페이지로 자동 리디렉션한다.

필요시 운영 중에 이 파일만 수정해서 도메인별 게시판 URL을 추가/변경할 수 있다.
"""

from __future__ import annotations

from typing import Dict, List


# key: 호스트명(소문자, www. 제외), value: 우선순위가 높은 순서대로 시도할 게시판 목록 URL 경로 또는 절대 URL
DOMAIN_BOARD_MAP: Dict[str, List[str]] = {
    # 강동구청
    # - newportal 메인에서 바로 게시판이 노출되지 않으므로,
    #   실제 공지/게시판 목록 URL을 여기에서 지정한다.
    "gangdong.go.kr": [
        # 문서에서 다뤘던 게시판 목록 (첨부가 상세 페이지에 있는 타입)
        "/web/newportal/bbs/b_067/list?baCategory3=D0175",
        # 공지사항 계열 bbsMsgList.do (필요시 운영자가 조정)
        "/newportal/bbs/bbsMsgList.do?bcd=1489",
    ],

    # 예시: 광진구청
    # 실제 게시판 목록 URL을 파악한 뒤 아래 주석을 해제하고 경로를 채우면 된다.
    # "gwangjin.go.kr": [
    #     "/portal/bbs/B0000001/board.do",
    # ],

    # 예시: 도봉구청
    # "dobong.go.kr": [
    #     "/bbs/B0000001/board.do",
    # ],

    # 예시: 동작구청
    # "dongjak.go.kr": [
    #     "/portal/bbs/B0000001/board.do",
    # ],
}


def resolve_board_start_url(original_url: str) -> str:
    """
    최상위 도메인(또는 기타 URL)을 입력받아,
    DOMAIN_BOARD_MAP에 정의된 게시판 목록 URL로 자동 리디렉션한다.

    매핑이 없거나 이미 게시판 URL인 경우에는 original_url을 그대로 반환한다.
    """
    from urllib.parse import urlparse, urljoin

    if not original_url:
        return original_url

    parsed = urlparse(original_url if "://" in original_url else f"https://{original_url}")
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]

    # 이미 /bbs/ 또는 /board/ 등이 포함된 경우는 게시판으로 간주
    path_lower = (parsed.path or "").lower()
    if any(token in path_lower for token in ("/bbs/", "/board/", "/boards/")):
        return original_url

    candidates = DOMAIN_BOARD_MAP.get(host)
    if not candidates:
        return original_url

    base = f"{parsed.scheme}://{parsed.netloc}"
    # 상대 경로/절대 URL 둘 다 허용
    for rel in candidates:
        if not rel:
            continue
        if rel.startswith("http://") or rel.startswith("https://"):
            return rel
        return urljoin(base, rel)

    return original_url


