
import asyncio
import time
from urllib.parse import urlparse, urljoin
from playwright.async_api import async_playwright

async def analyze_gwangjin():
    url = "https://www.gwangjin.go.kr/portal/main/main.do"
    print(f"Analyzing crawl performance for: {url}")
    
    start_time = time.time()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        print("Navigating to homepage...")
        nav_start = time.time()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            print(f"Navigation failed or timed out: {e}")
            await browser.close()
            return

        nav_end = time.time()
        load_time = nav_end - nav_start
        print(f"Page load time: {load_time:.2f} seconds")
        
        # Count links
        links = await page.evaluate("""
            () => {
                return Array.from(document.querySelectorAll('a')).map(a => a.href).filter(href => href.startsWith('http'));
            }
        """)
        
        await browser.close()
        
    unique_links = set(links)
    domain = urlparse(url).netloc
    internal_links = [l for l in unique_links if domain in urlparse(l).netloc]
    
    print(f"Total links found: {len(links)}")
    print(f"Unique links: {len(unique_links)}")
    print(f"Internal links: {len(internal_links)}")
    
    # Estimation
    # Assume 1 second processing per page (optimistic) to 3 seconds (conservative)
    # Total crawl time depends on depth.
    # Level 1 (Homepage): 1 page.
    # Level 2 (Links from Homepage): N pages.
    # Level 3: ...
    
    # Heuristic: Government portals usually have 5k-20k pages.
    # We can use the count of internal links as a proxy for Level 2 width.
    
    width = len(internal_links)
    print(f"Estimated Level 2 width: {width}")
    
    # Estimate total specific to Gwangjin
    # If we crawl depth 2 (Home -> List/Menu -> Content), we might visit 500-1000 pages depending on menu structure.
    # If we crawl depth 3 (Full crawl), it can be 10,000+.
    
    est_pages_d2 = width
    # Assuming only 10% of L2 links are lists that have 10 items each for L3... 
    # This is a wild guess without more scan.
    
    # Simple calculation for report:
    # Basic info crawling (Depth 2):
    time_per_page = 2.0 # seconds, including wait and processing
    concurrency = 5
    
    est_time_d2_seconds = (width * time_per_page) / concurrency
    est_time_d2_mins = est_time_d2_seconds / 60
    
    print(f"--- Estimation (Concurrency {concurrency}, Avg 2s/page) ---")
    print(f"Depth 1 (Home only): {load_time:.2f}s")
    print(f"Depth 2 (All links on Home): {width} pages ~ {est_time_d2_mins:.1f} minutes")
    
    # Depth 3 is hard to estimate without knowing if L2 pages are lists or content.
    # Usually huge.
    print(f"Depth 3 (Full Site): ESTIMATE 5000+ pages ~ {(5000*2/5)/3600:.1f} hours+")

if __name__ == "__main__":
    asyncio.run(analyze_gwangjin())
