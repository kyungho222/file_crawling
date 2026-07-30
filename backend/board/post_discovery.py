"""
게시글 목록 페이지에서 '개별 게시글(상세) URL'을 추출하고 간단히 검증하는 유틸 모듈.

사용 목적: 기존 `board_content_workflow`의 discover 단계에서 사용되는
상세 URL 추출 로직을 분리하여 재사용/단위 테스트하기 쉽도록 함.
- 핵심 함수: `discover_detail_links_from_html(html, base_url, ...)`

주의: 본 모듈은 discover(탐색) 단계용 경량 추출기입니다. 상세 페이지의
완전한 판별이나 DB 중복 체크, 기간 필터 등은 워크플로우 쪽에서 처리하십시오.
"""
from __future__ import annotations

from typing import List, Optional, Set, Callable, Awaitable
import re
from urllib.parse import urljoin, urlparse
import asyncio
import aiohttp

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

from backend.shared.content_scope import select_search_root
from backend.shared.detail_page_utils import is_detail_page_url

_JS_QUOTE_RE = re.compile(r"""['"]([^'"]+)['"]""")


def _extract_url_from_js_string(js_text: Optional[str], base: str) -> Optional[str]:
    # JS 문자열에서 따옴표로 감싼 상세페이지 URL 후보를 찾아 반환합니다.
    """onclick 등 JS 문자열에서 따옴표로 감싼 URL-like 문자열을 찾아 반환 (첫번째 유효값)."""
    if not js_text:
        return None
    try:
        for m in _JS_QUOTE_RE.finditer(js_text):
            cand = m.group(1).strip()
            if not cand:
                continue
            # skip simple anchors
            if cand.startswith("#"):
                continue
            full = urljoin(base, cand)
            if is_detail_page_url(full):
                return full
        return None
    except Exception:
        return None


# def discover_detail_links_from_html(html: str, *, base_url: str, max_links: int = 500, same_host_only: bool = True) -> List[str]:
#     # 목록 페이지 HTML에서 개별 게시글(상세) URL 후보들을 추출하여 리스트로 반환합니다.
#     """
#     HTML에서 게시글 상세 URL 후보들을 추출하여 리스트로 반환합니다.

#     인자:
#     - html: 페이지 HTML (문자열)
#     - base_url: 베이스 URL (상대경로를 절대화할 때 사용)
#     - max_links: 반환할 최대 링크 수
#     - same_host_only: True이면 base_url과 호스트가 다른 링크는 무시
#     """
#     out: List[str] = []
#     seen: Set[str] = set()
#     if not html or not BeautifulSoup:
#         return out
#     try:
#         soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
#     except Exception:
#         return out

#     search_root = select_search_root(soup, base_url=base_url)

#     def _scan_root(root):
#         for tag in root.find_all(True):
#             # 주로 <a> 태그 중심으로 검사하되 onclick에서도 추출 시도
#             href = (tag.get("href") or "").strip() if tag.get("href") is not None else ""
#             candidate: Optional[str] = None
#             if href and not href.startswith("#"):
#                 if href.lower().startswith("javascript:"):
#                     js_src = tag.get("onclick") or href
#                     candidate = _extract_url_from_js_string(js_src, base_url)
#                 else:
#                     candidate = urljoin(base_url, href)
#             else:
#                 js_src = tag.get("onclick") or ""
#                 if js_src:
#                     candidate = _extract_url_from_js_string(js_src, base_url)

#             if not candidate:
#                 continue
#             candidate = candidate.strip()
#             if not candidate:
#                 continue
#             try:
#                 # same host 제한
#                 if same_host_only:
#                     net1 = urlparse(candidate).netloc or ""
#                     net2 = urlparse(base_url).netloc or ""
#                     if net1 and net2 and net1 != net2:
#                         continue
#             except Exception:
#                 pass
#             # 상세 여부 검사
#             if not is_detail_page_url(candidate):
#                 continue
#             if candidate in seen:
#                 continue
#             seen.add(candidate)
#             out.append(candidate)
#             if len(out) >= int(max_links or 500):
#                 break

#     # 1) 우선 선정된 search_root에서 스캔
#     _scan_root(search_root)

#     # 2) 결과가 없으면 페이지 전체에서 재시도 (fallback)
#     if not out and search_root is not soup:
#         _scan_root(soup)

#     return out


# __all__ = ["discover_detail_links_from_html", "_extract_url_from_js_string"]


# async def fetch_detail_html(url: str, *, timeout_sec: float = 15.0, headers: Optional[dict] = None) -> Optional[str]:
#     # 간단한 비동기 HTTP GET으로 상세페이지 HTML을 가져옵니다 (동적 렌더링은 Playwright 사용 권장).
#     """
#     간단한 비동기 HTTP GET으로 상세페이지 HTML을 가져옵니다.
#     - 복잡한 동적 렌더링(자바스크립트 실행)이 필요한 경우 워크플로우 쪽에서 Playwright를 사용하세요.
#     """
#     if not url:
#         return None
#     try:
#         timeout = aiohttp.ClientTimeout(total=max(1.0, float(timeout_sec or 15.0)))
#         async with aiohttp.ClientSession(timeout=timeout) as session:
#             async with session.get(url, headers=headers or {}) as resp:
#                 if resp.status != 200:
#                     return None
#                 text = await resp.text(errors="ignore")
#                 return text
#     except asyncio.CancelledError:
#         raise
#     except Exception:
#         return None

# # url_loader에서 추출한 queue에서 개별 url 추출
# async def consume_url_queue(
#     queue: "asyncio.Queue[str]",
#     *,
#     headers: Optional[dict] = None,
#     max_links_per_list: int = 5,
#     max_items: int = 10,
#     detail_handler: Optional[Callable[[str, str], Awaitable[None]]] = None,
#     list_handler: Optional[Callable[[str, List[str]], Awaitable[None]]] = None,
# ) -> None:
#     """
#     url 큐를 지속적으로 소비하며 각 URL을 분류하고 적절한 핸들러로 연결합니다.

#     동작:
#     - URL이 상세 페이지로 판단되면 `fetch_detail_html`로 HTML을 가져와 `detail_handler(url, html)` 호출.
#     - 상세가 아니면 목록 페이지로 간주하고 `fetch_detail_html`로 HTML을 가져와
#       `discover_detail_links_from_html`로 상세 링크들을 추출한 뒤,
#       추출된 링크가 있으면 `list_handler(parent_url, links)` 호출하거나, list_handler가 없으면 링크들을 큐에 재삽입.
#     - HTML을 가져올 수 없거나 둘 다 아닐 경우 단순히 skip.

#     인자:
#     - queue: asyncio.Queue[str] (소비할 URL 큐)
#     - headers: HTTP 요청에 사용할 헤더 (선택)
#     - max_links_per_list: 목록 페이지에서 추출할 최대 상세 링크 수
#     - max_items: 큐에서 처리할 최대 URL 수 (0 이하면 제한 없음)
#     - detail_handler: 상세페이지 처리용 비동기 콜러블: async def f(url, html)
#     - list_handler: 목록페이지에서 추출된 링크 처리용 비동기 콜러블: async def f(parent_url, links)
#     """
#     if queue is None:
#         return
#     processed = 0
#     while True:
#         try:
#             url = await queue.get()
#         except asyncio.CancelledError:
#             break
#         except Exception:
#             # 예외가 발생하면 루프 계속
#             continue

#         try:
#             if not url:
#                 continue

#             if max_items > 0 and processed >= max_items:
#                 break

#             processed += 1

#             # 1) 상세 페이지 여부 우선 검사
#             try:
#                 print(f"============================ [test07] ============================")
#                 if is_detail_page_url(url):
#                     html = await fetch_detail_html(url, headers=headers)
#                     if html and detail_handler:
#                         await detail_handler(url, html)
#                     # 상세로 판단했으면 더 이상 처리하지 않음
#                     continue
#             except Exception:
#                 # 상세 판단/처리 중 오류가 나도 목록 시도로 폴백하지 않음
#                 pass

#             # 2) 목록 페이지로 처리: HTML fetch 후 상세 링크 추출 시도
#             html = await fetch_detail_html(url, headers=headers)
#             print(f"============================ [test08] ============================")
#             if not html:
#                 # 가져오지 못하면 skip
#                 continue

#             links = discover_detail_links_from_html(html, base_url=url, max_links=max_links_per_list)
#             if not links:
#                 # 상세 링크가 없으면 skip
#                 continue

#             sample_count = 3 
#             links = links[:sample_count]

#             # 3) 추출된 링크들을 처리: list_handler가 있으면 호출, 없으면 큐에 재삽입
#             if list_handler:
#                 print(f"============================ [test06] ============================")
#                 await list_handler(url, links)
#             else:
#                 for l in links:
#                     try:
#                         await queue.put(l)
#                     except Exception:
#                         # 큐에 넣기 실패 시 무시
#                         continue

#                 # snapshot: 추출된 링크 목록을 JSON으로 저장 (한 번의 스냅샷)
#                 try:
#                     import json
#                     import os
#                     print(f"============================ [test09] ============================")
#                     here = os.path.dirname(__file__)
#                     out_path = os.path.join(here, "discovered_links_snapshot.json")
#                     # 디렉터리 보장
#                     os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
#                     print(f"============================ [test05] out_path: {out_path} ============================")
#                     with open(out_path, "w", encoding="utf-8") as fh:
#                         json.dump(links, fh, ensure_ascii=False, indent=2)
#                 except Exception:
#                     # 저장 실패 시 무시
#                     pass

#         finally:
#             try:
#                 queue.task_done()
#             except Exception:
#                 pass

