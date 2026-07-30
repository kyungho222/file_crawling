import os
import sys
import json
import requests
import urllib3
urllib3.disable_warnings()

URLS = [
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=983",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=982",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=981",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=980",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=979",
    "https://www.oka.go.kr/web/board/garDetail.do?menu_cd=000022&num=978"
]

def clean_html(html_text):
    if not html_text:
        return ""
    import re
    text = re.sub(r'<[^>]+>', ' ', html_text)
    return re.sub(r'\s+', ' ', text).strip()

def main():
    headers = {"User-Agent": "Mozilla/5.0"}
    results = []
    for url in URLS:
        item = {"url": url}
        try:
            resp = requests.get(url, headers=headers, timeout=10, verify=False)
            resp.raise_for_status()
            
            # This API returns JSON data directly
            data = resp.json()
            board = data.get("brd") or data.get("board") or data.get("detail") or data.get("data")
                
            # If still nothing, let's just inspect what's inside
            if not board:
                # Find any dict inside that has 'title' or 'content'
                for k, v in data.items():
                    if isinstance(v, dict) and ('title' in v or 'content' in v or 'TITLE' in v):
                        board = v
                        break
            
            if not board:
                board = {}
            
            # fallback to exact keys
            title = board.get("title") or board.get("TITLE") or data.get("title") or board.get("nttSj")
            content = board.get("cont") or board.get("content") or board.get("CONTENT") or data.get("cont") or board.get("nttCn")
            author = board.get("username") or board.get("userid") or board.get("WRITER") or "관리자"
            reg_date = board.get("disp_write_dt") or board.get("write_dt") or board.get("reg_date") or board.get("REG_DT") or ""
            
            item["title"] = title
            item["author"] = author
            item["reg_date"] = reg_date
            
            content_str = str(content) if content else ""
            cleaned_content = clean_html(content_str)
            if len(cleaned_content) > 300:
                item["content"] = cleaned_content[:300] + " ... (생략)"
            else:
                item["content"] = cleaned_content
                
            # If the json has files
            files = data.get("fileList") or board.get("fileList") or data.get("files")
            if files:
                item["attachments_count"] = len(files)
            else:
                item["attachments_count"] = 0
            
            # debugging raw board keys
            if not title:
                item["raw_keys"] = list(board.keys()) if board else list(data.keys())
                
        except Exception as e:
            item["error"] = str(e)
            
        results.append(item)

    out_path = os.path.join(os.path.dirname(__file__), "oka_json_parse.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        
if __name__ == "__main__":
    main()
