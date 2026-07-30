"""
송파구청 사이트맵 재생성 스크립트
"""
import asyncio
import sys
import os

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.shared.board_header import _crawl_url, get_base_origin
import httpx
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def regenerate_songpa_sitemap():
    """송파구청 사이트맵 재생성"""
    url = "https://www.songpa.go.kr"
    logger.info(f"[송파구청 사이트맵 재생성 시작] URL: {url}")
    
    async with httpx.AsyncClient(
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        },
        follow_redirects=True,
        timeout=60.0,
    ) as client:
        try:
            groups, debug_info, candidates, list_urls = await _crawl_url(client, url, debug=True)
            
            logger.info(f"[사이트맵 재생성 완료]")
            logger.info(f"  - 그룹 수: {len(groups)}")
            logger.info(f"  - 총 링크 수: {sum(len(g.links) for g in groups)}")
            logger.info(f"  - 게시판 후보 수: {len(candidates)}")
            logger.info(f"  - 게시판 리스트 URL 수: {len(list_urls)}")
            
            if debug_info:
                logger.info(f"  - 디버그 정보: {debug_info}")
            
            # 사이트맵 파일 경로 확인
            base_origin = get_base_origin(url)
            from backend.shared.board_header import get_sitemap_cache_paths
            json_path, md_path = get_sitemap_cache_paths(base_origin)
            logger.info(f"  - JSON 파일: {json_path}")
            logger.info(f"  - Markdown 파일: {md_path}")
            
            if os.path.exists(md_path):
                with open(md_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    logger.info(f"  - Markdown 파일 라인 수: {len(lines)}")
                    logger.info(f"  - Markdown 파일 크기: {len(content)} bytes")
            
        except Exception as e:
            logger.error(f"[사이트맵 재생성 실패] {e}", exc_info=True)
            raise

if __name__ == "__main__":
    asyncio.run(regenerate_songpa_sitemap())

