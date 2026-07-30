
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import re

BASE_URL = "https://edu.ingang.go.kr"
LIST_URL = "https://edu.ingang.go.kr/NGLMS/1419/high/community/notice"

def get_soup(url):
    try:
        resp = requests.get(url, verify=False, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def extract_posts(pages=1):
    all_posts = []
    for p in range(1, pages + 1):
        url = f"{LIST_URL}?pageIndex={p}"
        print(f"Crawling list: {url}")
        soup = get_soup(url)
        if not soup:
            continue
            
        rows = soup.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if not cols:
                continue # Header row
            
            # Usually strict: No, Title, Date, Views, ...
            # Check if title is in 2nd column
            title_node = row.find("a")
            if not title_node and len(cols) > 1:
                title_node = cols[1].find("a")
            
            if title_node:
                title = title_node.get_text(strip=True)
                href = title_node.get('href', '')
                
                real_link = href
                
                # Check for javascript: links and extract ID
                if "javascript" in href.lower():
                    onclick = title_node.get('onclick', '')
                    # Look for pattern mostly like fn_view('1234') or similar
                    # Inspecting previous output: Candidates were /NGLMS/1419/high/community/noticeView?seq=138308
                    # So we need the seq ID.
                    
                    # Try to regex find a number in onclick
                    id_match = re.search(r"['\"](\d+)['\"]", onclick)
                    if not id_match:
                         id_match = re.search(r"\((\d+)\)", onclick)
                         
                    if id_match:
                        seq_id = id_match.group(1)
                        real_link = f"{BASE_URL}/NGLMS/1419/high/community/noticeView?seq={seq_id}"
                    else:
                        # Sometimes the link is in a data attribute
                        data_val = title_node.get('data-seq') or title_node.get('data-id')
                        if data_val:
                            real_link = f"{BASE_URL}/NGLMS/1419/high/community/noticeView?seq={data_val}"
                else:
                    real_link = urljoin(BASE_URL, href)
                    
                full_link = real_link
                
                # Extract date (look for YYYY-MM-DD pattern in row text)
                row_text = row.get_text(" ", strip=True)
                date_match = re.search(r"\d{4}-\d{2}-\d{2}", row_text)
                date = date_match.group(0) if date_match else "Unknown"
                
                post = {
                    "title": title,
                    "link": full_link,
                    "date": date
                }
                all_posts.append(post)
                # print(f" - Found: {title} ({date})")
        
        time.sleep(1) # Be polite
        
    return all_posts

def extract_content(url):
    print(f"extracting content from: {url}")
    soup = get_soup(url)
    if not soup:
        return None
        
    # Heuristic for content
    # Look for a view container
    # Often 'view', 'content', 'board_view' class
    content_div = soup.find("div", class_=re.compile(r"view|content|board", re.I))
    
    # Fallback: largest text block?
    # Or strict structure if known.
    # In inspecting output, I saw tables. Maybe content is in a table cell.
    
    # Let's try to find a table with 'view' in class or just the main container
    # For now, just grab title and text dump
    
    title = soup.title.string if soup.title else ""
    # Try to find specific title in body
    h_title = soup.find(re.compile(r"^h[1-3]"))
    if h_title:
        title = h_title.get_text(strip=True)
        
    text_content = ""
    if content_div:
        text_content = content_div.get_text("\n", strip=True)
    else:
        # Fallback to body text
        text_content = soup.body.get_text("\n", strip=True)
        
    # Trim
    text_content = text_content[:500] + "..." if len(text_content) > 500 else text_content
    
    return {
        "title": title,
        "content": text_content
    }

def main():
    print("="*60)
    print(" >>> 강남인강(edu.ingang.go.kr) 게시판 크롤링 시작 <<<")
    print("="*60)
    posts = extract_posts(pages=1) # Just 1 page for demo
    print(f"\n[검색 결과] 총 {len(posts)}개의 게시글 발견\n")
    
    for i, post in enumerate(posts):
        print(f"{i+1:02d}. [{post['date']}] {post['title']}")
        print(f"    Link: {post['link']}")
        
    if posts:
        print("\n" + "="*60)
        target = posts[0]
        print(f" >>> 첫 번째 게시글 상세 내용 추출 시도: {target['title']}")
        print("="*60)
        details = extract_content(target['link'])
        if details:
            print(f"[제목] {details['title']}")
            print(f"[본문 요약] (first 300 chars)\n")
            print(details['content'][:300])
            print("...")
            
    print("\n" + "="*60)
    print(" >>> 크롤링 완료 <<<")
    print("="*60)

if __name__ == "__main__":
    main()

