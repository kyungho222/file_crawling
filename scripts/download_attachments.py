import asyncio
import sys
import os
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

# 프로젝트 루트를 경로에 추가 (상위 디렉토리)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# aiohttp 및 BeautifulSoup 로드
try:
    import aiohttp
    from bs4 import BeautifulSoup
except ImportError:
    print("❌ 필드 라이브러리(aiohttp, beautifulsoup4)가 필요합니다.")
    sys.exit(1)

from backend.board.board_content_workflow import BoardContentWorkflow

async def download_file(session: aiohttp.ClientSession, url: str, save_path: str) -> Tuple[bool, Any]:
    """파일 하나를 다운로드합니다."""
    try:
        # User-Agent 설정 (차단 방지)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with session.get(url, headers=headers, timeout=60) as response:
            if response.status == 200:
                content = await response.read()
                with open(save_path, 'wb') as f:
                    f.write(content)
                return True, len(content)
            else:
                return False, f"HTTP 상태 코드: {response.status}"
    except Exception as e:
        return False, str(e)

async def download_attachments_from_url(url: str, download_dir: str = "downloads"):
    """
    URL에서 첨부파일을 찾아 로컬로 저장하고 결과를 로그로 기록합니다.
    """
    # 1. 환경 준비
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(project_root, "scripts", "logs")
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(os.path.join(project_root, download_dir), exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"attachment_download_{timestamp}.log")

    # 로거 설정
    logger = logging.getLogger("AttachmentDownloader")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 콘솔 출력도 병행
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"🚀 [시작] 대상 URL: {url}")
    
    # 2. Workflow 초기화 및 HTML 수집
    workflow = BoardContentWorkflow()

    try:
        logger.info("🌐 HTML 수집 중...")
        html = await workflow._fetch_html_static(url)
        if not html:
            logger.info("🔄 정적 수집 실패, 동적 수집 시도...")
            html = await workflow._fetch_html_playwright(url)
            
        if not html:
            logger.error("❌ HTML 수집 실패: 내용을 가져올 수 없습니다.")
            return

        soup = BeautifulSoup(html, "html.parser")
        
        # 3. 제목 및 첨부파일 목록 추출
        title = workflow._extract_board_title(soup, url=url, html=html)
        logger.info(f"📌 게시글 제목: {title}")
        
        attachments = workflow._extract_attachment_links_generic(html, base_url=url)
        logger.info(f"📎 발견된 첨 be파일 후보: {len(attachments)}개")
        
        if not attachments:
            logger.warning("⚠️ 첨부파일이 발견되지 않았습니다.")
            return

        # 4. 파일 다운로드 진행
        results = []
        async with aiohttp.ClientSession() as session:
            for idx, attach in enumerate(attachments, 1):
                name = (attach.get("name") or attach.get("text") or f"file_{idx}").strip()
                # 파일명에서 유효하지 않은 문자 제거
                safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '.', '_', '-')]).strip()
                if not safe_name:
                    safe_name = f"attachment_{idx}"
                
                href = attach.get("href")
                if not href or href.startswith("javascript:"):
                    logger.info(f"  [{idx}] {name} - 스킵 (자바스크립트 링크 또는 유효하지 않음)")
                    results.append({"name": name, "status": "skipped", "reason": "js_link"})
                    continue

                # 저장 경로 (게시글 제목 폴더 생성)
                safe_title = "".join([c for c in title if c.isalnum() or c in (' ', '_')]).strip()[:50]
                target_dir = os.path.join(project_root, download_dir, safe_title)
                os.makedirs(target_dir, exist_ok=True)
                
                save_path = os.path.join(target_dir, safe_name)
                
                logger.info(f"  [{idx}/{len(attachments)}] 다운로드 시도: {safe_name}")
                success, detail = await download_file(session, href, save_path)
                
                if success:
                    size_kb = round(detail / 1024, 2)
                    logger.info(f"    ✅ 성공: {size_kb} KB -> {save_path}")
                    results.append({"name": safe_name, "status": "success", "size": size_kb, "path": save_path})
                else:
                    logger.error(f"    ❌ 실패: {detail}")
                    results.append({"name": safe_name, "status": "fail", "reason": detail})

        # 5. 요약 기록
        logger.info("--- [다운로드 요약] ---")
        success_count = sum(1 for r in results if r["status"] == "success")
        logger.info(f"📊 총 결과: {len(attachments)}개 중 {success_count}개 성공")
        logger.info(f"📝 상세 로그 파일: {log_file_path}")

    except Exception as e:
        logger.exception(f"❗ 실행 중 예외 발생: {e}")
    finally:
        await workflow._close_playwright()
        logger.info("🏁 [종료] 작업이 완료되었습니다.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python scripts/download_attachments.py <URL>")
        sys.exit(1)
        
    target_url = sys.argv[1]
    
    # Windows 환경에서 ProactorEventLoop 설정
    if sys.platform == 'win32':
        import asyncio
        from asyncio import WindowsProactorEventLoopPolicy
        asyncio.set_event_loop_policy(WindowsProactorEventLoopPolicy())
        
    asyncio.run(download_attachments_from_url(target_url))
