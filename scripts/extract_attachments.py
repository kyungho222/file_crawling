"""
URL을 입력받아 게시글 제목과 첨부파일 목록을 추출하는 독립 실행 스크립트.

사용법:
    python scripts/extract_attachments.py <URL> [URL2 URL3 ...]
    python scripts/extract_attachments.py --file urls.txt

동작:
    1. requests로 정적 HTML 수신 시도
    2. JS 렌더링 필요 판단 → Playwright 폴백
    3. 제목 추출 (og:title 우선 → CSS 클래스 → <title> 태그 순)
    4. board_meta_extractor.extract_attachment_summary_from_html() 로 첨부파일 추출
    5. 결과를 콘솔에 출력 (--json 옵션 시 JSON 형식)

프로젝트 루트에서 실행:
    python scripts/extract_attachments.py <URL>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib3
urllib3.disable_warnings()

# ── 프로젝트 루트를 sys.path에 추가 (어느 위치에서 실행해도 동작) ──────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import re
import requests

try:
    from backend.board.board_meta_extractor import extract_attachment_summary_from_html
except ImportError as e:
    print(f"[ERROR] board_meta_extractor import 실패: {e}")
    print("       프로젝트 루트(crawler_web_board11/)에서 실행 중인지 확인하세요.")
    sys.exit(1)

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
_TIMEOUT = 20


# ─────────────────────────────────────────────────────────────────────────────
# 정적 페치
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_static(url: str) -> str | None:
    """requests로 HTML을 가져온다. 실패 시 None 반환."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9"},
            timeout=_TIMEOUT,
            verify=False,
        )
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "json" in ct:
            return None
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text
    except Exception:
        return None


def _needs_js(html: str) -> bool:
    """정적 HTML이 JS 렌더링이 필요한지 휴리스틱 판단."""
    if not html:
        return True
    lower = html.lower()
    has_attach_hint = (
        "첨부파일" in html
        or "filedown" in lower
        or "atchfile" in lower
        or "fileid" in lower
        or ".hwp" in lower
        or ".pdf" in lower
    )
    body_text = re.sub(r"<[^>]+>", " ", html)
    body_len = len(body_text.strip())
    if body_len < 500:
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Playwright 폴백 (동적 렌더링)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_with_playwright(url: str) -> str | None:
    """Playwright Chromium으로 JS 렌더링 후 HTML 반환."""
    try:
        from playwright.async_api import async_playwright, TimeoutError as PwTimeoutError
    except ImportError:
        print("[WARN] playwright가 설치되어 있지 않아 동적 렌더링을 건너뜁니다.")
        return None

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent=_UA,
                locale="ko-KR",
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"},
            )
            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=30_000)
            except PwTimeoutError:
                pass
            html = await page.content()
            await browser.close()
            return html
    except Exception as e:
        print(f"[WARN] Playwright 오류: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 제목 추출
# ─────────────────────────────────────────────────────────────────────────────

def _extract_title_from_html(html: str) -> str:
    """
    HTML에서 게시글 제목을 best-effort로 추출한다.
    우선순위:
      1. og:title / meta[name=title] (신뢰도 가장 높음)
      2. 본문 컨테이너 내 제목 전용 CSS 클래스
      3. h3/h4 (오탐 가능성 있어 마지막)
      4. <title> 태그 fallback
    """
    if not html:
        return ""
    try:
        from bs4 import BeautifulSoup as _BS
        soup = _BS(html, "html.parser")

        # 1) og:title / meta[name=title]
        og = soup.select_one("meta[property='og:title']")
        if og:
            v = (og.get("content") or "").strip()
            if v:
                return re.sub(r"\s+", " ", v)
        mt = soup.select_one("meta[name='title']")
        if mt:
            v = (mt.get("content") or "").strip()
            if v:
                return re.sub(r"\s+", " ", v)

        # 2) 본문 컨테이너 선택 (nav/header/footer 제외)
        content_root = soup
        for sel in ("article", "main", "#contents", "#content", ".contents",
                    ".sub_contents", ".container", "body"):
            found = soup.select_one(sel)
            if found:
                content_root = found
                break

        def _not_in_nav(el) -> bool:
            return not any(
                getattr(p, "name", "") in ("nav", "header", "footer", "aside")
                for p in el.parents
            )

        # 제목 전용 클래스 탐색
        for sel in (
            ".board_view_title", ".bbs_view_title", ".view_title",
            ".board_subject", ".post_title", ".view-title",
            "[class*='view_tit']", "[class*='bbs_tit']",
            ".subject", "[class*='subject']",
        ):
            el = content_root.select_one(sel)
            if el and _not_in_nav(el):
                v = el.get_text(" ", strip=True)
                if v and len(v) < 200:
                    return re.sub(r"\s+", " ", v)

        # 3) h3/h4 (마지막 수단)
        for tag in ("h3", "h4"):
            el = content_root.select_one(tag)
            if el and _not_in_nav(el):
                v = el.get_text(" ", strip=True)
                if v and len(v) < 200:
                    return re.sub(r"\s+", " ", v)

        # 4) <title> 태그 fallback
        if soup.title and soup.title.string:
            return re.sub(r"\s+", " ", str(soup.title.string).strip())
    except Exception:
        pass
    # BeautifulSoup 미설치 시 정규식 fallback
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return re.sub(r"\s+", " ", (m.group(1) or "")).strip()
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# 단일 URL 처리
# ─────────────────────────────────────────────────────────────────────────────

async def process_url(url: str) -> dict:
    """URL 하나를 처리하여 제목 및 첨부파일 정보를 반환한다."""
    result: dict = {"url": url, "title": "", "attachment_count": 0, "attachments": [], "fetch_method": ""}

    # 1) 정적 페치
    html = _fetch_static(url)
    used_playwright = False # Playwright 사용 여부 체크 플래그
    if html and not _needs_js(html):
        result["fetch_method"] = "requests"
    else:
        # 2) Playwright 폴백
        html_dyn = await _fetch_with_playwright(url)
        if html_dyn:
            html = html_dyn
            result["fetch_method"] = "playwright"
            used_playwright = True # 브라우저 이미 썼음
        elif html:
            result["fetch_method"] = "requests(fallback)"
        else:
            result["error"] = "HTML 수신 실패"
            return result

    if not html:
        result["error"] = "HTML 없음"
        return result

    # 3) 제목 추출
    result["title"] = _extract_title_from_html(html)

    # 4) 첨부파일 추출
    attach = extract_attachment_summary_from_html(html, url=url)
    result["attachment_count"] = attach.get("attachment_count", 0)
    result["attachments"] = attach.get("attachments", [])

    # 4-2) 첨부 1개 이하일 때 전체 HTML로 재추출(segment 잘림으로 놓친 경우) → 같은 게시글(bbsNo/nttNo) 링크만
    if result["attachment_count"] <= 1:
        attach2 = extract_attachment_summary_from_html(html, url=url, use_full_html=True, same_article_only=True)
        c2 = attach2.get("attachment_count", 0)
        if c2 > result["attachment_count"]:
            result["attachment_count"] = c2
            result["attachments"] = attach2.get("attachments", [])
        # 여전히 1개 이하이면 Playwright로 수집 후 재추출(렌더링된 HTML이 더 많은 경우)
        if result["attachment_count"] <= 1:
            html_dyn = await _fetch_with_playwright(url)
            if html_dyn:
                attach2b = extract_attachment_summary_from_html(html_dyn, url=url, use_full_html=True, same_article_only=True)
                if attach2b.get("attachment_count", 0) > result["attachment_count"]:
                    result["attachment_count"] = attach2b.get("attachment_count", 0)
                    result["attachments"] = attach2b.get("attachments", [])
                    result["fetch_method"] = "requests+playwright(attachments)"

    # 5) 절대 URL로 보정 (href가 상대경로인 경우)
    from urllib.parse import urljoin, urlparse
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for item in result["attachments"]:
        href = item.get("href") or ""
        if href and not href.startswith(("http://", "https://", "javascript:")):
            item["href"] = urljoin(base, href)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 출력
# ─────────────────────────────────────────────────────────────────────────────

def _print_result(r: dict, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return

    url = r.get("url", "")
    title = r.get("title", "")
    method = r.get("fetch_method", "")
    err = r.get("error", "")
    count = r.get("attachment_count", 0)
    attachments = r.get("attachments", [])

    print(f"\n{'=' * 70}")
    print(f"URL     : {url}")
    print(f"제목    : {title or '(추출 불가)'}")
    print(f"방법    : {method}")
    if err:
        print(f"오류    : {err}")
        return
    print(f"첨부 수 : {count}개")
    if attachments:
        print("첨부 목록:")
        for i, a in enumerate(attachments, 1):
            name = a.get("name") or "(이름 없음)"
            href = a.get("href") or ""
            print(f"  [{i:02d}] {name}")
            if href:
                print(f"        {href}")
    else:
        print("  (첨부파일 없음)")


# ─────────────────────────────────────────────────────────────────────────────
# 엔트리포인트
# ─────────────────────────────────────────────────────────────────────────────

async def main_async(urls: list[str], as_json: bool) -> None:
    # 모든 URL 처리를 동시에 시작
    tasks = [process_url(url) for url in urls]
    all_results = await asyncio.gather(*tasks) # 결과가 다 나올 때까지 대기
    
    # 결과 출력
    if not as_json:
        for r in all_results:
            _print_result(r, as_json=False)
    else:
        print(json.dumps(all_results, ensure_ascii=False, indent=2))

def main() -> None:
    parser = argparse.ArgumentParser(
        description="URL을 입력받아 게시글 제목과 첨부파일 목록을 추출합니다."
    )
    parser.add_argument("urls", nargs="*", help="처리할 URL (1개 이상)")
    parser.add_argument(
        "--file", "-f",
        help="URL 목록 파일 경로 (줄바꿈 구분)",
    )
    parser.add_argument(
        "--json", "-j",
        action="store_true",
        dest="as_json",
        help="결과를 JSON 형식으로 출력",
    )
    args = parser.parse_args()

    urls: list[str] = list(args.urls or [])

    if args.file:
        try:
            with open(args.file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        urls.append(line)
        except Exception as e:
            print(f"[ERROR] 파일 읽기 실패: {e}")
            sys.exit(1)

    if not urls:
        parser.print_help()
        sys.exit(0)

    asyncio.run(main_async(urls, args.as_json))


if __name__ == "__main__":
    main()
