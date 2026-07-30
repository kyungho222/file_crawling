
import sys
import os
import asyncio
import time
from unittest.mock import MagicMock
import traceback

# Add the project root to sys.path
sys.path.append(os.getcwd())

# Mock missing dependencies
sys.modules['langchain_text_splitters'] = MagicMock()
sys.modules['langchain_openai'] = MagicMock()

# Mock OpenAIEmbeddings
mock_embeddings = MagicMock()
sys.modules['langchain_openai'].OpenAIEmbeddings = MagicMock(return_value=mock_embeddings)

print("Diagnostics: checking db.db_redis import")
try:
    import db.db_redis
    print(f"db.db_redis file: {db.db_redis.__file__}")
    print(f"Attributes in db.db_redis: {dir(db.db_redis)}")
    
    if hasattr(db.db_redis, 'get_redis'):
        print("get_redis found in db.db_redis")
    else:
        print("get_redis NOT found in db.db_redis")
        
except Exception as e:
    print(f"db.db_redis import failed: {e}")
    traceback.print_exc()

# Also mock backend.shared.config to suppress warning and ensure consistent Env
sys.modules['backend.shared.config'] = MagicMock()
sys.modules['backend.shared.config'].Config = MagicMock()
sys.modules['backend.shared.config'].Config.OPENAI_API_KEY = "dummy"
sys.modules['backend.shared.config'].Config.PLAYWRIGHT_MAX_CONCURRENT = 5
sys.modules['backend.shared.config'].Config.URL_SINGLE_EMBEDDING_BATCH_SIZE = 10
sys.modules['backend.shared.config'].Config.URL_SINGLE_DB_BULK_SIZE = 10

try:
    from edu.dynamic_link_crawler import crawl_website_with_dynamic_links
except ImportError as e:
    print(f"Import Error: {e}")
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"General Error during import: {e}")
    traceback.print_exc()
    sys.exit(1)

async def measure_performance():
    url = "https://www.gwangjin.go.kr/portal/main/main.do"
    print(f"Analyzing crawl performance for: {url}")
    
    start_time = time.time()
    max_urls = 10
    max_depth = 2
    max_tasks = 5 
    
    print(f"Starting sample crawl (Max URLs: {max_urls}, tasks: {max_tasks})...")
    
    try:
        results = await crawl_website_with_dynamic_links(
            start_url=url,
            max_depth=max_depth,
            max_tasks=max_tasks,
            max_crawl_urls=max_urls,
            url_filter="B" 
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        count = len(results)
        print(f"Crawled {count} pages in {duration:.2f} seconds.")
        if count > 0:
            avg_time = duration / count
            print(f"Average time per page: {avg_time:.2f} seconds")
            
            estimated_total_pages = 5000
            estimated_time_seconds = estimated_total_pages * (avg_time / max_tasks) * 3 
            estimated_time_hours = estimated_time_seconds / 3600
            print(f"Estimated time for {estimated_total_pages} pages: {estimated_time_hours:.2f} hours (Rough Estimate)")

    except Exception as e:
        print(f"Crawl failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(measure_performance())
