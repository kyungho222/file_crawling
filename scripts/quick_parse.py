import asyncio
import os
import sys
import argparse
import contextlib

# 프로젝트 루트를 시스템 경로에 추가
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# 한글 출력 인코딩 설정
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding="utf-8")

try:
    from backend.board.board_content_workflow import BoardContentWorkflow
    from backend.board.board_content_extractor import extract_board_post
    from backend.board.board_meta_extractor import (
        extract_author_info_from_html,
        extract_contact_views_from_html,
        extract_attachment_summary_from_html,
    )
    from backend.shared.date_utils import extract_post_date
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"\n[ERROR] 필수 모듈을 가져올 수 없습니다. 가상환경(venv)을 활성화 후 실행하세요.\n상세내용: {e}")
    sys.exit(1)

@contextlib.contextmanager
def suppress_output():
    """내부 로그 및 오염된 출력을 억제합니다."""
    with open(os.devnull, 'w', encoding='utf-8') as devnull:
        old_stdout = sys.stdout
        # sys.stderr은 오류 확인을 위해 남겨두거나 같이 억제할 수 있습니다.
        try:
            yield
        finally:
            sys.stdout = old_stdout

async def quick_parse(url: str):
    """URL의 파싱 결과만 추출하여 줄바꿈을 포함해 깔끔하게 출력합니다.""" # 1줄 주석 설명
    
    workflow = None
    html = None
    
    try:
        # [1] HTML 수집 (수집 로그 억제)
        with suppress_output():
            try:
                workflow = BoardContentWorkflow()
                workflow.job_id = "quick_parse"
                html = await workflow._fetch_html_static(url)
                if not html:
                    html = await workflow._fetch_html_playwright(url)
            except Exception:
                html = None

        if not html:
            print(f"\n--- [실패] HTML 수집 불가: {url} ---")
            return

        # [2] 데이터 추출 (추출 로그 억제)
        final_title = ""
        with suppress_output():
            soup = BeautifulSoup(html, "html.parser")
            # 제목은 workflow의 최종 기준(_extract_board_title)과 동일하게 사용
            final_title = workflow._extract_board_title(soup, url=url, html=html)
            post_data = extract_board_post(html, url=url)

        # [3] 결과 출력 (줄바꿈이 살아있는 데이터 출력)
        print("\n" + "="*80)
        if post_data:
            print(f" [제목]     : {final_title or post_data.title}")
            print("-" * 80)

            # strip()으로 앞뒤 불필요한 공백만 제거하고 원본 줄바꿈 유지
            content = post_data.content_text.strip()
            print(f" [본문 내용] (총 {len(content)}자):")
            print("-" * 40)

            # 텍스트 내부에 이미 \n이 포함되어 있으므로 그대로 출력하면 줄바꿈이 적용됨
            print(content)
            print("-" * 40)
        else:
            print(f" [실패] 데이터 추출 실패: {url}")

        print("=" * 80 + "\n")
    finally:
        # _fetch_html_static이 쓰는 공유 aiohttp 세션 정리 (미정리 시 Unclosed client session 로그)
        if workflow:
            with contextlib.suppress(Exception):
                await workflow._close_http_session()
            with contextlib.suppress(Exception):
                await workflow._close_playwright()
        
def main():
    parser = argparse.ArgumentParser(description="게시판 상세 파싱 결과 확인용")
    parser.add_argument("url", help="대상 URL")
    # -d 등 정의되지 않은 인자가 들어와도 에러가 나지 않도록 parse_known_args를 사용합니다.
    args, unknown = parser.parse_known_args()

    asyncio.run(quick_parse(args.url))

if __name__ == "__main__":
    main()
