# core/crawler/workers/post.py
"""
게시글 URL 추출 워커
"""
import asyncio
import re
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from playwright.async_api import BrowserContext
from config.settings import settings
from core.crawler.queues import progress_queue
from utils.runtime_flags import is_no_limits_mode

async def post_worker(in_queue: asyncio.Queue, out_queue: asyncio.Queue, browser_context: BrowserContext):
    """
    Post Worker:
    - Takes a Board URL (list.do) from in_queue
    - Finds Post URLs (view.do) using JS/onclick parsing
    - Puts Post URLs into out_queue (attach_queue)
    - 진행 상황은 progress_queue를 통해 알림
    """
    while True:
        try:
            board_url = await in_queue.get()
            
            page = await browser_context.new_page()
            try:
                await page.goto(board_url, wait_until="domcontentloaded", timeout=60000)
                # Wait for potential AJAX loading
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5000)
                except:
                    pass

                links = await page.query_selector_all("a")
                found_posts = []
                
                for a in links:
                    href = await a.get_attribute("href")
                    onclick = await a.get_attribute("onclick")
                    view_url = None
                    
                    # 1. Standard href
                    if href and (("view" in href or "read" in href or "nttId" in href or "Detail" in href or "num=" in href) 
                                 and not href.startswith("javascript:")):
                        view_url = urljoin(board_url, href)
                    
                    # 2. JS/onclick parsing with flexible regex patterns
                    elif onclick or (href and href.startswith("javascript:")):
                        script = onclick if onclick else href
                        
                        # Flexible pattern: go*Detail(num), go*View(num), view*(num), etc.
                        # Matches: goToDetail(123), goDetail(123), goView(123), viewPost(123), etc.
                        detail_pattern = r'(?:go\w*Detail|go\w*View|view\w*|read\w*|show\w*)\s*\(\s*["\']?(\d+)["\']?\s*\)'
                        match = re.search(detail_pattern, script, re.IGNORECASE)
                        
                        if match:
                            num = match.group(1)
                            # Extract menu_cd from board_url
                            parsed = urlparse(board_url)
                            query_params = parse_qs(parsed.query)
                            menu_cd = query_params.get('menu_cd', [''])[0]
                            
                            # Construct detail URL based on board URL pattern
                            if "brdList.do" in board_url:
                                base_url = f"{parsed.scheme}://{parsed.netloc}/web/board/brdDetail.do"
                                view_url = f"{base_url}?menu_cd={menu_cd}&num={num}&currentPage=1&searchData=&searchText="
                            elif "list.do" in board_url or "List.do" in board_url:
                                base_url = board_url.split('?')[0].replace("list.do", "view.do").replace("List.do", "Detail.do")
                                query = parse_qs(parsed.query)
                                query['num'] = [num]
                                view_url = f"{base_url}?{urlencode(query, doseq=True)}"
                            
                            if view_url:
                                print(f"[POST] 🔍 Detected pattern: {match.group(0)} -> num={num}")
                        
                        # Fallback: Generic number extraction for other JS patterns
                        elif re.search(r'(down|file|view|detail)', script, re.IGNORECASE):
                            args = re.findall(r'["\'](\d+)["\']', script)
                            if args:
                                nttId = args[0]
                                if "list.do" in board_url or "List.do" in board_url:
                                    base_url = board_url.split('?')[0].replace("list.do", "view.do").replace("List.do", "Detail.do")
                                    query = parse_qs(urlparse(board_url).query)
                                    query['nttId'] = [nttId]
                                    view_url = f"{base_url}?{urlencode(query, doseq=True)}"
                    
                    # 🔥 CRITICAL FILTER: Only process actual post detail pages
                    if view_url:
                        # Skip list pages and content pages
                        skip_patterns = ["List.do", "list.do", "pg_", "contents.do"]
                        if any(pattern in view_url for pattern in skip_patterns):
                            continue
                        
                        # Only accept detail/view pages
                        accept_patterns = ["Detail.do", "detail.do", "view.do", "View.do", "read.do", "num=", "nttId="]
                        if not any(pattern in view_url for pattern in accept_patterns):
                            continue
                        
                        if view_url not in found_posts:
                            found_posts.append(view_url)
                            await out_queue.put(view_url)
                            print(f"[POST] ✅ Found post: {view_url}")

                print(f"[POST] 📊 Extracted {len(found_posts)} posts from {board_url}")
                await progress_queue.put({'type': 'post', 'count': len(found_posts)})

            except Exception as e:
                print(f"[PostWorker] Error processing {board_url}: {e}")
            finally:
                await page.close()
                in_queue.task_done()
                # Yield control to prevent CPU starvation
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[PostWorker] Critical Error: {e}")
            if not in_queue.empty():
                in_queue.task_done()
