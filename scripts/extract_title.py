import asyncio
import os
import sys
import argparse

# 프로젝트 루트를 시스템 경로에 추가
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.board.board_content_workflow import BoardContentWorkflow
from backend.board.board_content_extractor import extract_board_post

from bs4 import BeautifulSoup

async def get_title(url: str):
    """URL에서 제목만 추출하여 반환 (BoardContentWorkflow의 최신 로직 사용)"""
    workflow = BoardContentWorkflow()
    
    try:
        # [1] HTML 수집
        # 정적(requests) 수집 시도
        html = await workflow._fetch_html_static(url)
        
        # 실패 시 동적(Playwright) 수집 시도
        if not html:
            html = await workflow._fetch_html_playwright(url)
            
        if not html:
            print(f"❌ HTML 수집 실패: {url}")
            return None

        # [2] 제목 추출 (BoardContentWorkflow의 강화된 로직 사용)
        soup = BeautifulSoup(html, "html.parser")
        title = workflow._extract_board_title(soup, url=url, html=html)
        
        return title

    finally:
        await workflow._close_playwright()

async def main():
    parser = argparse.ArgumentParser(description="URL에서 제목을 추출하는 스크립트")
    parser.add_argument("url", help="추출할 페이지 URL")
    args = parser.parse_args()

    title = await get_title(args.url)
    if title:
        print(f"\n📌 추출된 제목: {title}\n")

if __name__ == "__main__":
    asyncio.run(main())
