"""
URL을 입력받아 현재 파싱 로직으로 추출되는 모든 항목을 진단하는 스크립트.

"페이지 학습 전" 상태에서 어떤 값들이 뽑히는지 확인한다.

추출 항목:
  - 제목 (board_content_extractor._extract_title)
  - 본문 텍스트 (board_content_extractor._extract_content_text)
  - 작성자 / 부서 (board_meta_extractor.extract_author_info_from_html)
  - 전화번호 / 조회수 (board_meta_extractor.extract_contact_views_from_html)
  - 등록일 (backend.shared.date_utils.extract_post_date)
  - 첨부파일 (board_meta_extractor.extract_attachment_summary_from_html)

사용법:
    python scripts/parse_inspect.py <URL> [URL2 ...]
    python scripts/parse_inspect.py --file urls.txt
    python scripts/parse_inspect.py --json <URL>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib3
urllib3.disable_warnings()

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import re
import requests

# ── 파싱 함수 임포트 ────────────────────────────────────────────────────────
try:
    from backend.board.board_content_extractor import (
        extract_board_post,
        _extract_title,
        _extract_content_text,
        _pick_best_content_text,
        _select_main_node,
        _strip_noisy_tags,
    )
    _HAS_EXTRACTOR = True
except ImportError as e:
    print(f"[ERROR] board_content_extractor import 실패: {e}")
    _HAS_EXTRACTOR = False

try:
    from backend.board.board_meta_extractor import (
        extract_author_info_from_html,
        extract_contact_views_from_html,
        extract_attachment_summary_from_html,
        resolve_content_author_fields,
    )
    _HAS_META = True
except ImportError as e:
    print(f"[ERROR] board_meta_extractor import 실패: {e}")
    _HAS_META = False

try:
    from backend.shared.date_utils import extract_post_date
    _HAS_DATE = True
except ImportError:
    _HAS_DATE = False

try:
    from bs4 import BeautifulSoup
    _HAS_BS4 = True
except ImportError:
    _HAS_BS4 = False

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
_TIMEOUT = 20


# ─────────────────────────────────────────────────────────────────────────────
# HTML 수신
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_static(url: str) -> str | None:
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
    if not html:
        return True
    body_text = re.sub(r"<[^>]+>", " ", html)
    return len(body_text.strip()) < 500


async def _fetch_with_playwright(url: str) -> str | None:
    try:
        from playwright.async_api import async_playwright, TimeoutError as PwError
    except ImportError:
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
            except PwError:
                pass
            html = await page.content()
            await browser.close()
            return html
    except Exception as e:
        print(f"[WARN] Playwright 오류: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 파싱 진단 (핵심)
# ─────────────────────────────────────────────────────────────────────────────

def _run_parse_inspect(html: str, url: str) -> dict:
    """현재 파싱 로직으로 HTML에서 모든 항목을 추출하고 결과를 dict로 반환한다."""
    result: dict = {
        "url": url,
        "title": None,
        "content_snippet": None,
        "content_length": 0,
        "author": None,
        "department": None,
        "author_kind": None,
        "content_author": None,
        "content_author_kind": None,
        "contact_phone": None,
        "view_count": None,
        "reg_date": None,
        "attachment_count": 0,
        "attachments": [],
        "parse_errors": [],
    }

    if not _HAS_BS4:
        result["parse_errors"].append("BeautifulSoup 미설치 - 파싱 불가")
        return result

    # ── 1. 제목 + 본문 (board_content_extractor) ─────────────────────────
    if _HAS_EXTRACTOR:
        try:
            post = extract_board_post(html, url=url)
            if post:
                result["title"] = post.title
                result["content_length"] = len(post.content_text)
                result["content_snippet"] = post.content_text[:300].strip()
            else:
                # extract_board_post가 None을 반환하면 내부 함수로 직접 시도
                try:
                    soup = BeautifulSoup(html, "html.parser")
                    _strip_noisy_tags(soup)
                    root = _select_main_node(soup)
                    title = _extract_title(soup, root)
                    content_text, _ = _pick_best_content_text(soup, root, title=title)
                    result["title"] = title or None
                    result["content_length"] = len(content_text)
                    result["content_snippet"] = content_text[:300].strip()
                    result["parse_errors"].append("extract_board_post → None (본문 짧음 등), 내부 함수로 재시도")
                except Exception as e2:
                    result["parse_errors"].append(f"내부 파싱 오류: {e2}")
        except Exception as e:
            result["parse_errors"].append(f"extract_board_post 오류: {e}")
    else:
        result["parse_errors"].append("board_content_extractor 미로드")

    # ── 2. 작성자 / 부서 / 작성자유형 ─────────────────────────────────────
    if _HAS_META:
        try:
            author_info = extract_author_info_from_html(html, url=url)
            resolved_author_fields = resolve_content_author_fields(author_info, url=url, html=html)
            result["author"] = author_info.get("author")
            result["department"] = author_info.get("department")
            result["author_kind"] = author_info.get("author_kind")
            result["content_author"] = resolved_author_fields.get("content_author")
            result["content_author_kind"] = resolved_author_fields.get("content_author_kind")
        except Exception as e:
            result["parse_errors"].append(f"작성자 추출 오류: {e}")

        # ── 3. 전화번호 / 조회수 ─────────────────────────────────────────
        try:
            contact_info = extract_contact_views_from_html(html, url=url)
            result["contact_phone"] = contact_info.get("contact_phone")
            result["view_count"] = contact_info.get("view_count")
        except Exception as e:
            result["parse_errors"].append(f"연락처/조회수 추출 오류: {e}")

        # ── 4. 첨부파일 ──────────────────────────────────────────────────
        try:
            attach = extract_attachment_summary_from_html(html, url=url)
            result["attachment_count"] = attach.get("attachment_count", 0)
            result["attachments"] = attach.get("attachments", [])
            # href 절대 URL 보정
            from urllib.parse import urljoin, urlparse as _uparse
            parsed = _uparse(url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            for item in result["attachments"]:
                href = item.get("href") or ""
                if href and not href.startswith(("http://", "https://")):
                    item["href"] = urljoin(base, href)
        except Exception as e:
            result["parse_errors"].append(f"첨부파일 추출 오류: {e}")
    else:
        result["parse_errors"].append("board_meta_extractor 미로드")

    # ── 5. 등록일 ─────────────────────────────────────────────────────────
    if _HAS_DATE:
        try:
            date_result = extract_post_date(html, post_url=url)
            if date_result:
                result["reg_date"] = str(date_result)
        except Exception as e:
            result["parse_errors"].append(f"날짜 추출 오류: {e}")
    else:
        result["parse_errors"].append("date_utils 미로드 - 날짜 추출 건너뜀")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 단일 URL 처리
# ─────────────────────────────────────────────────────────────────────────────

async def process_url(url: str) -> dict:
    html = _fetch_static(url)
    fetch_method = "requests"

    if not html or _needs_js(html):
        html_dyn = await _fetch_with_playwright(url)
        if html_dyn:
            html = html_dyn
            fetch_method = "playwright"
        elif html:
            fetch_method = "requests(fallback)"
        else:
            return {"url": url, "error": "HTML 수신 실패", "fetch_method": ""}

    result = _run_parse_inspect(html, url)
    result["fetch_method"] = fetch_method
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 출력
# ─────────────────────────────────────────────────────────────────────────────

_EMPTY = "(없음)"
_FAIL  = "(추출 실패)"


def _v(val) -> str:
    if val is None:
        return _EMPTY
    s = str(val).strip()
    return s if s else _EMPTY


def _print_result(r: dict, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return

    url   = r.get("url", "")
    err   = r.get("error", "")
    meth  = r.get("fetch_method", "")
    perrs = r.get("parse_errors") or []

    SEP = "=" * 72
    print(f"\n{SEP}")
    print(f"  URL      : {url}")
    print(f"  수신방법 : {meth}")

    if err:
        print(f"  오류     : {err}")
        return

    print(SEP)

    # 제목 / 본문
    title   = _v(r.get("title"))
    clen    = r.get("content_length", 0)
    snippet = (r.get("content_snippet") or "").strip()
    snippet_display = (snippet[:200] + " ...") if len(snippet) > 200 else snippet

    print(f"  📌 제목     : {title}")
    print(f"  📄 본문길이 : {clen}자")
    print(f"  📄 본문미리보기 :\n     {snippet_display or _EMPTY}")

    print(SEP)

    # 메타
    author  = _v(r.get("author"))
    dept    = _v(r.get("department"))
    akind   = _v(r.get("author_kind"))
    final_author = _v(r.get("content_author"))
    final_author_kind = _v(r.get("content_author_kind"))
    phone   = _v(r.get("contact_phone"))
    views   = _v(r.get("view_count"))
    regdate = _v(r.get("reg_date"))
    print(f"  ?뫀 理쒖쥌湲?댁씠 : {final_author}  (?좏삎: {final_author_kind})")

    print(f"  👤 작성자   : {author}  (유형: {akind})")
    print(f"  🏢 부서     : {dept}")
    print(f"  📞 전화번호 : {phone}")
    print(f"  👁️  조회수   : {views}")
    print(f"  📅 등록일   : {regdate}")

    print(SEP)

    # 첨부파일
    acount = r.get("attachment_count", 0)
    attachments = r.get("attachments", [])
    print(f"  📎 첨부파일 : {acount}개")
    if attachments:
        for i, a in enumerate(attachments, 1):
            name = a.get("name") or "(이름 없음)"
            href = a.get("href") or ""
            print(f"     [{i:02d}] {name}")
            if href:
                print(f"           {href}")

    # 파싱 경고/오류
    if perrs:
        print(SEP)
        print("  ⚠️  파싱 경고/오류:")
        for e in perrs:
            print(f"     - {e}")

    print(SEP)


# ─────────────────────────────────────────────────────────────────────────────
# 엔트리포인트
# ─────────────────────────────────────────────────────────────────────────────

async def main_async(urls: list[str], as_json: bool) -> None:
    all_results = []
    for url in urls:
        r = await process_url(url)
        all_results.append(r)
        if not as_json:
            _print_result(r, as_json=False)

    if as_json:
        print(json.dumps(all_results, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="URL을 입력받아 현재 파싱 로직의 추출 결과를 진단합니다."
    )
    parser.add_argument("urls", nargs="*", help="진단할 URL (1개 이상)")
    parser.add_argument("--file", "-f", help="URL 목록 파일 (줄바꿈 구분)")
    parser.add_argument("--json", "-j", action="store_true", dest="as_json",
                        help="결과를 JSON으로 출력")
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
