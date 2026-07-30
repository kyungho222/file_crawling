# core/crawler/engine.py
"""
크롤러 엔진 메인 모듈
"""
import asyncio
from core.crawler.queues import *
from core.crawler.manager import WorkerManager
from core.crawler.progress import Progress

async def run_crawler(start_url: str, progress: Progress):
    """
    Main Entry Point for the Crawler Engine
    """
    manager = WorkerManager(progress)
    manager_initialized = False
    
    try:
        await progress.update_message("Starting Worker Pool...")
        await manager.start()
        manager_initialized = True  # manager.start() 성공 후에만 True
        
        # Initial Seed
        await scan_queue.put({'url': start_url, 'depth': 0})
        await progress.update_message(f"Seeded start URL: {start_url}")
        
        # Wait for completion
        # Strategy: Join all queues. 
        # Note: This assumes that if queues are empty, work is done. 
        # But in a crawler, one item can spawn more. 
        # queue.join() blocks until all items are processed (task_done called).
        
        await progress.update_message("Scanning boards...")
        await scan_queue.join()
        
        await progress.update_message("Extracting posts...")
        await post_queue.join()
        
        await progress.update_message("Extracting files...")
        await attach_queue.join()
        
        await progress.update_message("Downloading files...")
        await download_queue.join()
        
        await progress.update_message("Processing files...")
        await study_queue.join()
        
        await progress.set_status("complete")
        await progress.update_message("All tasks completed.")
        
    except asyncio.CancelledError:
        print("[Engine] Crawler cancelled.", flush=True)
        await progress.set_status("cancelled")
        await progress.update_message("Crawling cancelled by user.")
        # Force stop on cancellation (manager가 초기화된 경우에만)
        if manager_initialized:
            try:
                await manager.stop(graceful=False)
            except Exception as stop_error:
                print(f"[Engine] Error during stop: {stop_error}", flush=True)
    except Exception as e:
        print(f"[Engine] Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        await progress.set_status("error")
        await progress.update_message(f"Error: {e}")
        # Graceful stop on error (manager가 초기화된 경우에만)
        if manager_initialized:
            try:
                await manager.stop(graceful=True)
            except Exception as stop_error:
                print(f"[Engine] Error during stop: {stop_error}", flush=True)
    else:
        # Graceful stop on success
        if manager_initialized:
            try:
                await manager.stop(graceful=True)
            except Exception as stop_error:
                print(f"[Engine] Error during stop: {stop_error}", flush=True)
