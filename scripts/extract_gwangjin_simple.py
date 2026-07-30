import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import sys

# 설정
TARGET_LIST_URL = "https://www.gwangjin.go.kr/portal/bbs/B0000001/list.do?menuNo=200190"
START_DATE = datetime(2025, 10, 15)
END_DATE = datetime.now()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def parse_date(date_str):
    if not date_str:
        return None
    try:
        # 2024-01-01 or 2024.01.01
        clean_str = re.sub(r'[^0-9\-\.]', '', date_str)
        for fmt in ["%Y-%m-%d", "%Y.%m.%d"]:
            try:
                return datetime.strptime(clean_str, fmt)
            except ValueError:
                continue
    except Exception:
        pass
    return None

def extract_urls():
    print(f"URL: {TARGET_LIST_URL}")
    print(f"기간: {START_DATE.strftime('%Y-%m-%d')} ~ {END_DATE.strftime('%Y-%m-%d')}")
    print("-" * 60)

    try:
        resp = requests.get(TARGET_LIST_URL, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"접속 실패: {e}")
        return

    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # 광진구청은 보통 tbody 내 tr, 그 안에 .date 클래스나 4번째 td 등에 날짜가 있음
    # .board_list tbody tr
    # 선택자 제거하고 모든 tr 검색, 혹은 div 리스트(ul/li)일 수도 있음
    # 우선 table 구조라고 가정하고 모든 tr 스캔
    rows = soup.find_all('tr')
    print(f"DEBUG: Found {len(rows)} tr elements.")

    if not rows:
        # tr이 없으면 li 구조 확인
        rows = soup.select('li')
        print(f"DEBUG: Found {len(rows)} li elements (fallback).")
    
    if rows:
        print(f"DEBUG: First row text -> {rows[0].get_text(strip=True)[:50]}...")

    found_count = 0
    
    for row in rows:
        # 제목/링크
        a_tag = row.find('a', href=True)
        if not a_tag:
            continue
            
        title = a_tag.get_text(strip=True)
        href = a_tag['href']
        
        # 행 텍스트 전체에서 날짜 찾기
        text = row.get_text(" ", strip=True)
        
        # 2025-10-15 or 2025.10.15
        m = re.search(r'(\d{4})[-.](\d{2})[-.](\d{2})', text)
        if not m:
            continue
            
        date_str = m.group(0)

        dt = parse_date(date_str)
        if not dt:
            continue

        # 기간 체크
        if START_DATE <= dt <= END_DATE:
            full_url = urljoin("https://www.gwangjin.go.kr", href)
            # 세션 ID 제거
            full_url = full_url.split(';jsessionid=')[0]
            
            print(f"[{dt.strftime('%Y-%m-%d')}] {title}")
            print(f"   Link: {full_url}")
            found_count += 1
            
    print("-" * 60)
    print(f"총 {found_count}개의 게시물을 찾았습니다.")

if __name__ == "__main__":
    extract_urls()
