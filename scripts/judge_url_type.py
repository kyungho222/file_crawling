#!/usr/bin/env python3
"""
Usage:
  python scripts/judge_url_type.py <url> [--render]

간단 설명:
  지정한 URL을 가져와서 프로젝트의 판정 로직(`is_detail_page_url`,
  `is_post_detail_page_from_html`)을 이용해 'list'인지 'detail'인지 판단합니다.
  --render 옵션을 주면 Playwright로 동적 렌더링을 시도합니다(설치 필요).
"""
import os
import sys

# 프로젝트 루트를 path에 넣어서 backend import 가능하게 함 (어디서 실행해도 동작)
_script_dir = os.path.abspath(os.path.dirname(__file__))
_project_root = os.path.dirname(_script_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import argparse
import logging

import requests

try:
    from backend.shared.detail_page_utils import is_detail_page_url, is_post_detail_page_from_html
except Exception as e:
    print("프로젝트 모듈 import 실패:", e)
    sys.exit(2)


_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"


def fetch_static_html(url: str, timeout: float = 15.0) -> str | None:
    try:
        headers = {"User-Agent": _UA, "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"}
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return resp.text
        return None
    except Exception:
        return None


def fetch_rendered_html(url: str, timeout: float = 30.0) -> str | None:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=_UA)
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            try:
                page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass
            html = page.content()
            try:
                page.close()
            except Exception:
                pass
            try:
                ctx.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
            return html
    except Exception:
        return None


def judge(url: str, render: bool = False) -> dict:
    out = {"url": url, "url_detail_pattern": False, "html_detail": False, "final": "unknown", "notes": []}

    # 1) URL based quick check
    try:
        if is_detail_page_url(url):
            out["url_detail_pattern"] = True
            out["notes"].append("URL 기반 상세 패턴 매칭")
    except Exception as e:
        out["notes"].append(f"URL 판정 에러: {e}")

    # 2) HTML 검사 (정적)
    html = fetch_static_html(url)
    if not html and render:
        html = fetch_rendered_html(url)
        if html:
            out["notes"].append("Playwright 렌더링 사용")

    if html:
        try:
            if is_post_detail_page_from_html(html, url):
                out["html_detail"] = True
                out["notes"].append("HTML 기준 상세 페이지로 판정됨")
            else:
                out["notes"].append("HTML 기준 상세 아님 → 목록(list)으로 판단")
        except Exception as e:
            out["notes"].append(f"HTML 판정 에러: {e}")
    else:
        out["notes"].append("HTML을 가져오지 못함")

    # 우선순위 결정: (프로젝트 로직 기준)
    # - HTML/URL 검사 결과에 따라 상세/목록 판정
    if out["url_detail_pattern"]:
        out["final"] = "detail (by url pattern)"
    elif out["html_detail"]:
        out["final"] = "detail (by html)"
    else:
        out["final"] = "list"

    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="URL이 게시물 상세인지 목록인지 판정합니다.")
    parser.add_argument("url", help="검사할 URL")
    parser.add_argument("--render", action="store_true", help="Playwright로 동적 렌더링 시도 (선택)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    res = judge(args.url, render=args.render)
    print(f"URL: {res['url']}")
    print(f"최종 판정: {res['final']}")
    print("세부:")
    print(f"  - URL 기반 상세 패턴: {res['url_detail_pattern']}")
    print(f"  - HTML 기반 상세 판정: {res['html_detail']}")
    if res.get("notes"):
        print("노트:")
        for n in res["notes"]:
            print("  -", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

