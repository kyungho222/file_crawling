import asyncio
import os
import sys
import json
import re
from bs4 import BeautifulSoup

# 프로젝트 루트를 시스템 경로에 추가
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.board.board_content_workflow import BoardContentWorkflow
from backend.board.board_content_extractor import extract_board_post

async def main():
    if len(sys.argv) < 2:
        print("사용법: python scripts/parse_page_with_playwright.py <URL>")
        return

    target_url = sys.argv[1]
    print(f"\n[1/3] Playwright 하이브리드 수집 시도 중: {target_url}\n")

    workflow = BoardContentWorkflow()
    workflow.job_id = "debug_job"
    workflow.db_name = "debug_db"

    html = None
    post_data = None
    contract_info = {}
    table_root = None
    is_chuncheon_contract = "chuncheon.go.kr" in target_url and "contract" in target_url
    contract_labels = ["계약명", "예정가격", "최초계약금액", "낙찰률", "계약금액", "계약일자", "계약기간", "계약방법", "준공일자", "계약유형"]
    cookies = []
    referrer = "N/A"

    try:
        # 하이브리드 수집 호출 (검색 후 클릭 로직이 포함된 _fetch_html_playwright)
        html = await workflow._fetch_html_playwright(target_url)
        
        # 세션 정보 및 유입 경로 캡처
        cookies = await workflow.get_cookies()
        referrer = await workflow.get_referrer()

        if html:
            print(f"[2/3] 페이지 파싱 중...\n")
            # 1. 본문 및 제목 추출 (기본 추출기)
            post_data = extract_board_post(html, url=target_url)
            
            # 2. 계약 상세 정보 전용 추출 (춘천시 특화: .ctrtAcctBook/.detail_view 또는 .serch_result_wrap)
            soup = BeautifulSoup(html, "html.parser")
            contract_info = {label: "정보없음" for label in contract_labels}
            table_root = None
            if is_chuncheon_contract:
                table_root = soup.select_one(".ctrtAcctBook, .detail_view") or soup.select_one(".serch_result_wrap")
            else:
                table_root = soup
            
            if table_root:
                # th/td 테이블 형식 (기존)
                for th in table_root.find_all("th"):
                    label = th.get_text(strip=True)
                    if label in contract_info:
                        td = th.find_next_sibling("td") or (th.parent.find("td") if th.parent else None)
                        if td: contract_info[label] = td.get_text(" ", strip=True)
                # .content01_title / .content01_title_v 형식 (춘천시 상세 페이지 .serch_result_wrap)
                for box in table_root.select(".content01_2title_box"):
                    title_el = box.select_one(".content01_title p")
                    value_el = box.select_one(".content01_title_v p")
                    if title_el and value_el:
                        label = title_el.get_text(strip=True)
                        if label == "낙찰율":
                            label = "낙찰률"
                        if label in contract_info:
                            contract_info[label] = value_el.get_text(" ", strip=True)
        else:
            print("-" * 50)
            print("[경고] HTML 데이터를 가져오지 못했습니다. (검색 실패 혹은 데이터 부재)")

        # 정보 정리
        display_title = contract_info.get("계약명") if contract_info.get("계약명") and contract_info.get("계약명") != "정보없음" else (post_data.title if post_data else "제목 없음")
        body_for_display = table_root.get_text(separator="\n", strip=True) if table_root and is_chuncheon_contract else (post_data.content_text if post_data else "내용 없음")

        # 터미널 출력
        print(f"[3/3] 결과 요약:\n")
        print("=" * 80)
        print(f"URL         : {target_url}")
        if html:
            print(f"제목        : {display_title}")
            print(f"유입 경로   : {referrer}")
            print("-" * 40)
            print(f"본문 내용 (일부) :\n{body_for_display[:300]}...")
            print("-" * 40)
            print(f"[계약 상세 정보]")
            for label in contract_labels:
                print(f"{label.ljust(12)} : {contract_info.get(label, '정보없음')}")
        else:
            print(f"상태        : [수집 실패]")
            
        print("-" * 40)
        print(f"[세션 정보]")
        if cookies:
            for cookie in cookies:
                if cookie['name'] in ['JSESSIONID', 'person1', 'person2']:
                    print(f"*{cookie['name'].ljust(14)} : {cookie['value'][:30]}...")
        else:
            print("활성화된 쿠키가 없습니다.")
        print("=" * 80)

    except Exception as e:
        import traceback
        print(f"[오류 발생] {e}")
        traceback.print_exc()
    finally:
        # 결과를 파일로 영구 저장 (터미널 출력 유실 대비)
        import io
        try:
            os.makedirs("tmp", exist_ok=True)
            with io.open("tmp/last_run_result.txt", "w", encoding="utf-8") as f:
                f.write(f"URL: {target_url}\n")
                f.write(f"Referer: {referrer}\n")
                f.write(f"Cookies: {json.dumps(cookies, ensure_ascii=False, indent=2)}\n")
                if html:
                    f.write(f"Title: {display_title}\n")
                    f.write(f"Content: {body_for_display}\n")
                    f.write("\n[Contract Info]\n")
                    for k, v in contract_info.items():
                        f.write(f"{k}: {v}\n")
                    f.write("\n[Full HTML Snippet]\n")
                    f.write(html[:1000] + "...")
                else:
                    f.write("Status: Failed to fetch HTML\n")
            print(f"\n[알림] 상세 결과가 tmp/last_run_result.txt 에 저장되었습니다.")
        except Exception as fe:
            print(f"파일 저장 실패: {fe}")
            
        await workflow._close_playwright()

if __name__ == "__main__":
    async def run():
        await main()
    asyncio.run(run())