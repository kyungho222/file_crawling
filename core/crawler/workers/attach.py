import asyncio
import logging
import re
import sys
import os
from urllib.parse import urljoin, urlparse
from playwright.async_api import BrowserContext

# 프로젝트 루트를 sys.path에 추가 (core/crawler/workers -> ../../../)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.constants import ALLOWED_EXTENSIONS
from core.crawler.batch_queue import BatchQueue
from core.crawler.queues import progress_queue
from utils.url import extract_download_url_from_js
from backend.board.anseong_file import resolve_anseong_yhlib_download_url

logger = logging.getLogger(__name__)


def _attach_debug_stdout_enabled() -> bool:
    return str(os.getenv("ATTACH_WORKER_DEBUG_STDOUT", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _attach_debug_print(message: str) -> None:
    if not _attach_debug_stdout_enabled():
        return
    try:
        print(message, flush=True)
    except Exception:
        pass

async def navigate_with_retry(page, url: str, max_attempts: int = 3):
    """
    페이지 진입 시 일시적인 네트워크 오류를 완화하기 위해 재시도하며 진입
    """
    for attempt in range(1, max_attempts + 1):
        try:
            _attach_debug_print(f"[ATTACH] Navigating to: {url} (attempt {attempt}/{max_attempts})")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=3000)
            except Exception:
                pass
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[ATTACH] Navigation failed | attempt=%s/%s url=%s err=%s", attempt, max_attempts, url, e)
            if attempt == max_attempts:
                raise
            await asyncio.sleep(min(2 * attempt, 5))

async def attach_worker(in_queue: asyncio.Queue, out_queue: BatchQueue, browser_context: BrowserContext):
    """
    Attach Worker:
    - Takes a Post URL (view.do) from in_queue
    - Enters the page and finds file download links
    - Puts File Metadata into out_queue (download_queue)
    - 진행 상황은 progress_queue를 통해 알림
    """
    while True:
        try:
            post_item = await in_queue.get()
            # 호환: in_queue가 str(url) 또는 dict 형태를 모두 허용
            post_url = post_item.get("url") if isinstance(post_item, dict) else post_item
            try:
                job_id = post_item.get("job_id", "default") if isinstance(post_item, dict) else "default"
            except Exception:
                job_id = "default"
            try:
                ui_colle = post_item.get("colle") if isinstance(post_item, dict) else None
                colle_mode = str(ui_colle or "board").strip().lower()
            except Exception:
                colle_mode = "board"
            # 기간 필터 통과 여부(있으면 존중). 값이 없으면 "통과된 상세페이지가 들어온다"는 전제 하에 True로 처리.
            try:
                if isinstance(post_item, dict):
                    date_pass = post_item.get("date_in_range")
                    if date_pass is None:
                        date_pass = post_item.get("in_range")
                    if date_pass is None:
                        date_pass = post_item.get("passed_date_filter")
                    if date_pass is None:
                        date_pass = post_item.get("date_filter_passed")
                else:
                    date_pass = None
                date_pass = True if date_pass is None else bool(date_pass)
            except Exception:
                date_pass = True
            
            _attach_debug_print(f"[ATTACH] Received post URL: {post_url}")
            
            page = await browser_context.new_page()
            try:
                await navigate_with_retry(page, post_url)
                
                page_found_files = 0
                links = await page.query_selector_all("a")
                
                _attach_debug_print(f"[ATTACH] Found {len(links)} total links on page")
                
                for a in links:
                    try:
                        # Safety check: Page might be closed or navigated away
                        if page.is_closed():
                            logger.warning("[ATTACH] Page closed unexpectedly | url=%s", post_url)
                            break

                        # 숨김 요소 체크 (display:none, visibility:hidden 등)
                        is_visible = await a.is_visible()
                        if not is_visible:
                            continue  # 숨김 요소는 건너뛰기

                        href = await a.get_attribute("href")
                        onclick = await a.get_attribute("onclick")
                        text = await a.inner_text()
                        text = text.strip() if text else "Unknown File"
                        
                        file_url = None
                        
                        # A. Direct href
                        if href and not href.startswith("javascript:"):
                            if any(href.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS) or \
                               "download" in href.lower() or "down" in href.lower() or "atchFileId" in href:
                                file_url = urljoin(post_url, href)
                                
                        # B. JS Download
                        if not file_url and (onclick or (href and href.startswith("javascript:"))):
                            script = onclick if onclick else href
                            resolved_from_js = ""
                            try:
                                resolved_from_js = extract_download_url_from_js(script, post_url) or ""
                                if not resolved_from_js:
                                    resolved_from_js = resolve_anseong_yhlib_download_url(script, post_url) or ""
                            except Exception:
                                resolved_from_js = ""
                            if resolved_from_js:
                                file_url = resolved_from_js
                            
                            # 1. Generalized Pattern Matching
                            pattern = r"(?:fileDown|fileDownload|downloadFile|goDownload|fn_filedownload|preview)\s*\(([^)]+)\)"
                            match = re.search(pattern, script, re.IGNORECASE)
                            
                            if match and not file_url:
                                args_str = match.group(1)
                                params = [p.strip().strip("'\"") for p in args_str.split(',')]
                                
                                if params:
                                    file_id = params[0]
                                    
                                    parsed_post = urlparse(post_url)
                                    base_root = f"{parsed_post.scheme}://{parsed_post.netloc}"
                                    
                                    # print(f"[ATTACH] 🔍 Detected JS download pattern: {match.group(0)}")
                                    
                                    # 2. Smart URL Construction
                                    if "oka.go.kr" in parsed_post.netloc or "/web/board/" in post_url:
                                        file_url = f"{base_root}/web/board/fileDownload/{file_id}.do"
                                    
                                    elif "FileDown.do" in script or (len(params) >= 2 and len(params[0]) > 10): 
                                        file_sn = params[1] if len(params) > 1 else "0"
                                        file_url = f"{base_root}/cmm/fms/FileDown.do?atchFileId={file_id}&fileSn={file_sn}"
                                        
                                    elif "download.do" in script:
                                        file_url = f"{base_root}/common/file/download.do?fileId={file_id}"
                                        
                                    else:
                                        file_url = f"{base_root}/web/board/fileDownload/{file_id}.do"
                                        # print(f"[ATTACH] ⚠️  Using fallback URL pattern for: {script}")

                                    # if file_url:
                                        # print(f"[ATTACH] 🔗 Generated download URL: {file_url}")

                        if file_url:
                            _attach_debug_print(f"[ATTACH] File detected: {text[:50]}... -> {file_url}")
                            file_meta = {
                                "url": file_url,
                                "_raw_url": onclick if onclick else href,
                                "name": text,
                                "source_page": post_url,
                                "source_href": href,
                                "source_onclick": onclick,
                                "type": "file"
                            }
                            page_found_files += 1
                            try:
                                await out_queue.put(file_meta)
                                # print(f"[ATTACH] 📤 Queued (buffer size: {len(out_queue.buffer)})")
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                logger.warning("[ATTACH] Error putting to out_queue | url=%s err=%s", file_url, e)
                            
                    except Exception as e:
                        # Ignore TargetClosedError and continue to next link (or break if critical)
                        if "TargetClosed" in str(e) or "closed" in str(e):
                            logger.warning("[ATTACH] Target closed during link processing | url=%s", post_url)
                            break
                        # Other errors (e.g. stale element) -> continue
                        continue
                
                async def process_frame_links(frame):
                    frame_found = 0
                    try:
                        frame_links = await frame.query_selector_all("a")
                        for fa in frame_links:
                            is_visible = await fa.is_visible()
                            if not is_visible:
                                continue
                            
                            fh = await fa.get_attribute("href")
                            if fh and any(fh.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
                                furl = urljoin(frame.url, fh)
                                file_meta = {
                                    "url": furl,
                                    "name": "iframe_file",
                                    "source_page": post_url,
                                    "type": "file"
                                }
                                await out_queue.put(file_meta)
                                frame_found += 1
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning("[ATTACH] Error in iframe frame processing | url=%s err=%s", post_url, e)
                    return frame_found
                
                iframe_found_files = 0
                # Iframe check
                if page_found_files == 0:
                    frames = page.frames
                    if frames:
                        tasks = [process_frame_links(frame) for frame in frames]
                        frame_results = await asyncio.gather(*tasks, return_exceptions=True)
                        for result in frame_results:
                            if isinstance(result, Exception):
                                logger.warning("[ATTACH] Error in iframe gather | url=%s err=%s", post_url, result)
                                continue
                            iframe_found_files += result
                
                total_found = page_found_files + iframe_found_files
                if total_found:
                    # ✅ 요청사항:
                    # - 기간필터 통과 상세페이지에서 첨부파일이 발견되면
                    # - 프론트 colle=file 모드일 때 scan_count가 증가하도록 scan 이벤트를 발행
                    #   (중복 방지는 consumer 쪽 set/ledger에 위임)
                    if colle_mode == "file" and date_pass:
                        try:
                            await progress_queue.put({
                                "type": "scan",
                                "count": 1,
                                "items": [post_url],
                                "job_id": job_id,
                                "colle": colle_mode,
                            })
                        except Exception:
                            # best-effort: 카운트 이벤트 실패는 파일 탐지/다운로드 흐름을 막지 않음
                            pass
                    await progress_queue.put({
                        'type': 'attach_file',
                        'count': total_found,
                        'job_id': job_id,
                        'colle': colle_mode,
                        'source_page': post_url,
                    })
                
                _attach_debug_print(f"[ATTACH] Summary: Found {total_found} files on {post_url}")

            except Exception as e:
                logger.error("[AttachWorker] Error processing | url=%s err=%s", post_url, e)
            finally:
                await page.close()
                in_queue.task_done()
                # Yield control to prevent CPU starvation
                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[AttachWorker] Critical Error: %s", e)
            if not in_queue.empty():
                in_queue.task_done()
