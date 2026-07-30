"""
동적 링크 지원이 강화된 크롤링 함수들
JavaScript 기반 동적 페이지의 URL 수집을 지원하는 크롤링 시스템
"""

import asyncio
import aiohttp
import ssl
import time
from typing import List, Dict, Any
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 로컬 import들
from .url_edu import (
    logger, normalize_url, get_government_user_agent, 
    extract_all_links_with_dynamic, wait_for_javascript_completion,
    is_downloadable_file, is_same_domain, should_include_url_by_filter,
    build_structured_content, extract_links_including_javascript,
    fetch_url_content, discover_sitemap_urls,
    prioritize_urls, GlobalURLProcessor
)
from backend.shared.url_scope import extract_precise_scope_path_prefix


async def crawl_with_dynamic_links_support(
    start_url: str,
    subject: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager,
    job_progress_manager,
    max_depth: int = 3,
    memo: str = "",
    max_tasks: int = 20,
    sitemap: str = "N",
    max_crawl_urls: int = 1000000,
    chat_bot_id: str = None,
    url_filter: str = "B"
) -> dict:
    """동적 링크 지원이 강화된 Playwright 기반 크롤링 함수"""
    logger.info(f"[🚀 동적 링크 강화 크롤링 시작] URL: {start_url}")
    logger.info(f"[설정] max_depth: {max_depth}, 동적 링크 추출: 활성화, JavaScript 대기시간: 연장됨")
    
    try:
        start_time = time.time()
        scoped_path_prefix = extract_precise_scope_path_prefix(start_url)
        
        # 사이트맵 우선 탐색 (기존 로직 유지)
        sitemap_urls = []
        if sitemap.upper() == "Y":
            sitemap_urls = await discover_sitemap_urls(start_url)
            if sitemap_urls:
                if scoped_path_prefix:
                    sitemap_urls = [
                        sitemap_url
                        for sitemap_url in sitemap_urls
                        if (urlparse(normalize_url(sitemap_url)).path or "/").startswith(scoped_path_prefix)
                    ]
                logger.info(f"[동적 크롤링] 사이트맵에서 {len(sitemap_urls)}개 URL 발견")
                sitemap_urls = prioritize_urls(sitemap_urls, start_url)[:max_crawl_urls]
            else:
                logger.info(f"[동적 크롤링] 사이트맵 탐색 실패, 일반 크롤링으로 전환")
        
        # 크롤링할 URL 목록 준비
        if sitemap_urls:
            crawl_results = [{"source": url, "title": "", "content": "", "snippet": "", "favicon_url": "", "source_size": [0]} for url in sitemap_urls]
        else:
            # 일반 크롤링 수행 (동적 링크 지원 버전)
            crawl_results = await crawl_website_with_dynamic_links(
                start_url, max_depth, max_tasks, job_id, job_manager, max_crawl_urls, url_filter
            )
        
        if not crawl_results:
            error_msg = "크롤링 결과가 없습니다"
            logger.error(f"[동적 크롤링 실패] {error_msg}")

            return {"status": "error", "message": error_msg}
        
        logger.info(f"[동적 크롤링 완료] 총 {len(crawl_results)}개 URL 수집 완료")
        
        # 크롤링된 URL들을 병렬 처리 (동적 링크 정보 포함)
        return await process_crawl_results_with_dynamic_support(
            crawl_results,
            subject,
            table_name,
            dbname,
            job_id,
            each_progress,
            job_manager,
            job_progress_manager,
            memo=memo,
            original_url=start_url,
            chat_bot_id=chat_bot_id
        )
        
    except Exception as e:
        error_msg = f"동적 링크 크롤링 중 오류 발생: {str(e)}"
        logger.error(f"[동적 크롤링 오류] {error_msg}")

        return {"status": "error", "message": error_msg}


async def crawl_website_with_dynamic_links(start_url, max_depth: int, max_tasks=10, job_id=None, job_manager=None, max_crawl_urls=100, url_filter: str = None):
    """동적 링크 지원이 강화된 웹사이트 크롤링 함수"""
    logger.info(f"[동적 링크 크롤링 시작] URL: {start_url}, max_depth: {max_depth}, max_tasks: {max_tasks}")
    
    start_url = normalize_url(start_url)
    parsed_url = urlparse(start_url)
    scheme = parsed_url.scheme or "https"
    domain = parsed_url.netloc
    precise_scope_path = extract_precise_scope_path_prefix(start_url) or "/"
    
    visited = set()
    enqueued = set()
    failures = {}
    results = []
    queue = asyncio.Queue()
    
    crawl_allowed_path = precise_scope_path
    collect_base_path = precise_scope_path
    
    # 시드 URL들을 큐에 추가
    seed_set = set()
    scope_seed = f"{scheme}://{domain}{crawl_allowed_path}"
    if scope_seed not in seed_set:
        await queue.put((scope_seed, 0))
        seed_set.add(scope_seed)
    
    if not start_url.startswith(('http://', 'https://')):
        start_seed = f"https://{start_url}"
    else:
        start_seed = start_url
    if start_seed not in seed_set:
        await queue.put((start_seed, 0))
        seed_set.add(start_seed)
    
    # SSL 컨텍스트 설정 (기존과 동일)
    ssl_context = ssl.create_default_context()
    ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    try:
        ssl_context.options |= ssl.OP_LEGACY_SERVER_CONNECT
    except AttributeError:
        pass

    connector = aiohttp.TCPConnector(
        ssl=ssl_context,
        force_close=True,
        limit=200,
        limit_per_host=20,
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True,
    )
    
    headers = {
        "User-Agent": get_government_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=60, connect=10, sock_read=30),
        headers=headers
    ) as session:
        
        # 동적 링크 지원 워커들 생성
        tasks = [
            asyncio.create_task(
                crawl_page_worker_with_dynamic_links(
                    queue, session, domain, max_depth, visited, results,
                    crawl_allowed_path, collect_base_path, enqueued, failures,
                    job_id, job_manager, max_crawl_urls, url_filter
                )
            )
            for _ in range(max_tasks)
        ]
        
        try:
            await queue.join()
        except Exception as e:
            logger.error(f"[동적 링크 크롤링 오류] {e}")
        finally:
            # 워커 정리
            for task in tasks:
                task.cancel()
            
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.warning(f"[동적 링크 워커 정리 중 오류] {e}")
    
    logger.info(f"[동적 링크 크롤링 완료] 총 {len(results)}개 페이지 수집")
    return results


async def crawl_page_worker_with_dynamic_links(
    queue, session, domain, max_depth, visited, results, 
    crawl_allowed_path, collect_base_path, enqueued, failures, 
    job_id=None, job_manager=None, max_crawl_urls=100, url_filter: str = None
):
    """동적 링크 추출을 지원하는 크롤링 페이지 워커"""
    logger.info(f"[동적 링크 워커 시작] url_filter: {url_filter}")
    
    while True:
        try:
            # 작업 취소 확인 (기존 로직과 동일)
            if job_id and job_manager:
                try:
                    status = await job_manager.get_job_status(job_id)
                except Exception:
                    status = None
                if status == "cancel":
                    logger.info("사용자 취소 감지: 동적 링크 워커 종료")
                    break
            
            current_url, depth = await queue.get()
            current_url = normalize_url(current_url)
            
            # URL 검증
            parsed_current = urlparse(current_url)
            if parsed_current.netloc != domain:
                queue.task_done()
                continue
            if not (parsed_current.path or "/").startswith(crawl_allowed_path):
                queue.task_done()
                continue
                
            if current_url in visited or depth > max_depth:
                queue.task_done()
                continue
                
            if len(results) >= max_crawl_urls:
                logger.info(f"[동적 링크 워커] 최대 URL 개수({max_crawl_urls})에 도달하여 중단")
                queue.task_done()
                continue
            
            visited.add(current_url)
            logger.info(f"[동적 링크 워커 {depth}] 크롤링 중: {current_url}")
            
            # ✅ Playwright를 사용한 동적 페이지 처리 우선
            html_content = None
            all_links = []
            
            try:
                # Playwright로 페이지 렌더링 및 동적 링크 추출
                logger.info(f"[동적 링크 워커] Playwright로 동적 페이지 렌더링: {current_url}")
                
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=True,
                        args=[
                            '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
                            '--disable-images', '--disable-audio-output', '--disable-extensions',
                            '--disable-plugins', '--disable-web-security', '--no-first-run',
                            '--disable-background-networking', '--disable-default-apps',
                            '--disable-sync', '--disable-translate', '--metrics-recording-only',
                            '--user-agent=' + get_government_user_agent()
                        ]
                    )
                    
                    page_obj = await browser.new_page()
                    page_obj.set_default_timeout(15000)
                    page_obj.set_default_navigation_timeout(12000)
                    
                    try:
                        # 페이지 로드
                        await page_obj.goto(current_url, wait_until="domcontentloaded", timeout=12000)
                        
                        # JavaScript 실행 완료 대기
                        await wait_for_javascript_completion(page_obj, 4000)
                        
                        # 동적 링크 추출 (새로운 함수 사용!)
                        all_links = await extract_all_links_with_dynamic(page_obj, current_url)
                        
                        # HTML 콘텐츠 추출
                        html_content = await page_obj.content()
                        
                        logger.info(f"[동적 링크 워커] Playwright 성공: {current_url}, 추출된 링크: {len(all_links)}개")
                        
                    finally:
                        await browser.close()
                        
            except Exception as playwright_error:
                logger.warning(f"[동적 링크 워커] Playwright 실패, HTTP 폴백: {current_url}, 오류: {playwright_error}")
                
                # HTTP 폴백
                try:
                    url_result = await fetch_url_content(session, current_url)
                    if url_result:
                        html_content = url_result['html']
                        soup = BeautifulSoup(html_content, "lxml")
                        all_links = extract_links_including_javascript(soup, current_url)
                        logger.info(f"[동적 링크 워커] HTTP 폴백 성공: {current_url}, 추출된 링크: {len(all_links)}개")
                    else:
                        logger.warning(f"[동적 링크 워커] HTTP 폴백도 실패: {current_url}")
                        queue.task_done()
                        continue
                except Exception as http_error:
                    logger.warning(f"[동적 링크 워커] HTTP 폴백 오류: {current_url}, 오류: {http_error}")
                    queue.task_done()
                    continue
            
            # HTML 콘텐츠가 성공적으로 추출되었으면 결과 처리
            if html_content:
                soup = BeautifulSoup(html_content, "lxml")
                
                # 수집 여부 판단
                parsed_current = urlparse(current_url)
                current_path = parsed_current.path or "/"
                should_collect = current_path.startswith(collect_base_path)
                
                # URL 필터링 적용
                if should_collect and url_filter and url_filter != "B":
                    filter_passed = should_include_url_by_filter(current_url, url_filter)
                    if not filter_passed:
                        should_collect = False
                        logger.info(f"[동적 링크 워커 - 수집 제외] URL 필터: {current_url} (필터: {url_filter})")
                
                if should_collect:
                    # 구조화된 컨텐츠 추출
                    structured = build_structured_content(soup, current_url, html_content)
                    if structured.get("content"):
                        results.append({
                            "source": current_url,
                            "title": structured.get("title", ""),
                            "web_title": structured.get("web_title", ""),
                            "content": structured.get("content", ""),
                            "snippet": structured.get("snippet", ""),
                            "favicon_url": structured.get("favicon_url", ""),
                            "source_size": [len(structured.get("content", ""))],
                        })
                        logger.info(f"[✅ 동적 링크 워커 수집] {current_url} (총 수집: {len(results)}개)")
                        
                        # 카운트 전송
                        pass
                
                # ✅ 동적 링크들을 큐에 추가 (핵심 개선사항!)
                for link_url in all_links:
                    try:
                        if not link_url or link_url.startswith(("#", "mailto:", "tel:")):
                            continue
                            
                        next_url = normalize_url(link_url)  # 이미 절대 URL로 변환됨
                        parsed_next = urlparse(next_url)
                        
                        # CSS, 다운로드 파일 등 제외
                        if (next_url.endswith(".css") or "/css/" in next_url or 
                            "?ver=" in next_url or is_downloadable_file(next_url)):
                            continue
                        
                        # 탐색 범위 내의 링크만 큐에 추가
                        next_path = parsed_next.path or "/"
                        if (is_same_domain(parsed_next.netloc, domain) and
                            next_path.startswith(crawl_allowed_path)):
                            if (next_url not in visited) and (next_url not in enqueued):
                                enqueued.add(next_url)
                                await queue.put((next_url, depth + 1))
                                logger.debug(f"[동적 링크 워커] 큐 추가: {next_url} (깊이: {depth + 1})")
                    except Exception as link_error:
                        logger.debug(f"[동적 링크 워커] 링크 처리 오류: {link_url}, 오류: {link_error}")
                        continue
            
            queue.task_done()
            await asyncio.sleep(0.3)  # 약간의 대기 시간
            
        except Exception as worker_error:
            logger.error(f"[동적 링크 워커 오류] {worker_error}")
            queue.task_done()
            await asyncio.sleep(1)


async def process_crawl_results_with_dynamic_support(
    crawl_results: List[Dict],
    subject: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager,
    job_progress_manager,
    memo: str = "",
    original_url: str = "",
    chat_bot_id: str = None,
) -> dict:
    """크롤링 결과를 동적 링크 지원과 함께 처리"""
    logger.info(f"[동적 링크 결과 처리] 총 {len(crawl_results)}개 URL 처리 시작")
    
    try:
        start_time = time.time()
        total_chunks = 0
        successful_urls = 0
        skipped_urls = 0
        
        # 기존 병렬 처리 로직 사용 (GlobalURLProcessor 활용)
        async with GlobalURLProcessor() as processor:
            results = await processor.process_multiple_urls_optimized(
                urls=[result["source"] for result in crawl_results],
                subjects=[subject] * len(crawl_results),
                memos=[memo] * len(crawl_results),
                table_name=table_name,
                dbname=dbname,
                job_id=job_id,
                job_manager=job_manager,
                job_progress_manager=job_progress_manager,
                each_progress=each_progress,
                chat_bot_id=chat_bot_id,
                crawl_mode="crawling"  # 크롤링 모드 활성화
            )
        
        # 결과 집계
        for result in results.values():
            if result.get("status") == "success":
                successful_urls += 1
                total_chunks += result.get("chunks", 0)
            elif result.get("status") in ["no_change", "skipped"]:
                skipped_urls += 1
        
        processing_time = round(time.time() - start_time, 2)
        
        logger.info(f"[동적 링크 결과 처리 완료] 성공: {successful_urls}, 스킵: {skipped_urls}, 총 청크: {total_chunks}")
        
        return {
            "status": "success",
            "message": f"동적 링크 크롤링 완료 - 성공: {successful_urls}개 URL, 스킵: {skipped_urls}개 URL",
            "total_chunks": total_chunks,
            "successful_urls": successful_urls,
            "skipped_urls": skipped_urls,
            "processing_time": processing_time,
            "source_details": results,
            "original_url": original_url
        }
        
    except Exception as e:
        error_msg = f"동적 링크 결과 처리 중 오류 발생: {str(e)}"
        logger.error(f"[동적 링크 결과 처리 오류] {error_msg}")
        return {"status": "error", "message": error_msg}
