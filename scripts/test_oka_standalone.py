import sys
import os
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add root folder to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.board.board_content_extractor import extract_board_post
from backend.board.board_meta_extractor import extract_author_info_from_html, extract_contact_views_from_html, extract_attachment_summary_from_html

URLS = [
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=983",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=982",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=981",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=980",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=979",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=978"
]

def main():
    import json
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0 Safari/537.36"}
    results = []
    for url in URLS:
        item = {"url": url}
        try:
            resp = requests.get(url, headers=headers, timeout=10, verify=False)
            resp.raise_for_status()
            html = resp.text
            
            # Post extractor
            result = extract_board_post(html, url=url)
            
            # Meta extractor
            author_info = extract_author_info_from_html(html, url=url)
            contact_info = extract_contact_views_from_html(html, url=url)
            attachments = extract_attachment_summary_from_html(html, url=url)
            
            if result:
                item["title"] = result.title
                item["author"] = author_info.get("author", "N/A")
                item["department"] = author_info.get("department", "N/A")
                item["view_count"] = contact_info.get("view_count", "N/A")
                
                content = result.content_text.strip()
                if len(content) > 300:
                    item["content"] = content[:300] + " ... (생략)"
                else:
                    item["content"] = content
                
                if attachments and attachments.get("has_attachments"):
                    item["attachments_count"] = attachments.get("count", 0)
            else:
                item["error"] = "본문/제목을 추출할 수 없습니다."
        except Exception as e:
            item["error"] = f"로딩 실패: {e}"
        results.append(item)

    with open(os.path.join(os.path.dirname(__file__), "oka_output.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()
