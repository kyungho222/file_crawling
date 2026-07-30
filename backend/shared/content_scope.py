from typing import Optional, List

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    from bs4.element import Tag  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]
    Tag = object  # type: ignore[assignment]


# 우선순위 후보 셀렉터 (일반적인 본문 컨테이너)
_COMMON_CONTENT_SELECTORS: List[str] = [
    "#content",
    "#contents",
    ".dbData",
    ".dbdata",
    ".view",
    ".view_wrap",
    ".content",
    ".contents",
    ".post",
    ".post-content",
    ".article",
    ".article-content",
    "article",
    "main",
    ".board_view",
    ".board-view",
    "#main",
]


def _text_len(node: Optional[Tag]) -> int:
    try:
        if node is None:
            return 0
        text = node.get_text(" ", strip=True)
        return len(text or "")
    except Exception:
        return 0


def select_search_root(soup, base_url: Optional[str] = None, selector_hint: Optional[str] = None):
    """
    주어진 BeautifulSoup `soup`에서 '본문'으로 간주할 적절한 루트 노드를 선택한다.
    우선순위:
      1) 명시적 selector_hint가 주어지면 해당 요소를 우선 사용
      2) 자주 쓰이는 content selector 목록에서 가장 텍스트 길이가 큰 요소 선택
      3) fallback: body 또는 전체 soup

    반환값: Tag (soup가 그대로 반환될 수 있음)
    """
    if not soup or not BeautifulSoup:
        return soup

    try:
        # 1) selector_hint 우선
        if selector_hint:
            try:
                node = soup.select_one(selector_hint)
                if node and _text_len(node) > 20:
                    return node
            except Exception:
                pass

        # 2) common selectors 중에서 가장 콘텐츠가 많은 요소 선택
        candidates = []
        for sel in _COMMON_CONTENT_SELECTORS:
            try:
                node = soup.select_one(sel)
                if node:
                    candidates.append(node)
            except Exception:
                continue

        if candidates:
            # 텍스트 길이가 가장 큰 요소를 반환
            best = max(candidates, key=_text_len)
            if _text_len(best) > 20:
                return best

        # 3) 추가 heuristic: article/main 태그가 여러개면 가장 큰 것 선택
        try:
            articles = soup.find_all(["article", "main"])
            if articles:
                best = max(articles, key=_text_len)
                if _text_len(best) > 20:
                    return best
        except Exception:
            pass

        # 4) fallback: body 태그가 있으면 body, 없으면 원본 soup
        try:
            body = soup.select_one("body")
            if body:
                return body
        except Exception:
            pass
    except Exception:
        return soup

    return soup


