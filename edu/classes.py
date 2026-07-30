# -*- coding: utf-8 -*-
"""
URL Education Module Classes
크롤링 및 URL 처리를 위한 클래스들
"""

import asyncio
import ssl
import aiohttp
import time
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from utils.logging_util import LoggerSingleton
from config import Config
from db.db_job_managers import AsyncJobManager, AsyncJobProgress

# ✅ 로거 설정
logger = LoggerSingleton().get_logger(__name__)


class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        """SSL 검증을 완전히 비활성화한 urllib3 PoolManager 생성."""
        # SSL 검증 완전 비활성화 - SSL 컨텍스트 없이 생성
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLS,
            cert_reqs='CERT_NONE',  # 인증서 검증 비활성화
            ca_certs=None,          # CA 인증서 사용 안함
            assert_hostname=False,  # 호스트명 검증 비활성화
            # ssl_context 완전 제거
        )


class CrawlStopSignal:
    """크롤링 중단 신호를 모든 워커가 공유하는 클래스"""
    def __init__(self):
        self.should_stop = False
        self.stop_reason = ""
    
    def set_stop(self, reason: str = "user_stop"):
        self.should_stop = True
        self.stop_reason = reason
        logger.info(f"[🛑 글로벌 중단 신호 설정] 이유: {reason}")
    
    def is_stopped(self) -> bool:
        return self.should_stop
    
    def get_reason(self) -> str:
        return self.stop_reason


class SmartCrawlQueue:
    """URL 수집과 탐색을 분리한 스마트 큐 시스템"""
    
    def __init__(self, max_crawl_urls: int = 100, url_filter: str = None):
        # 우선순위 큐들
        self.collect_queue = asyncio.Queue()      # 수집 대상 URL (필터 통과)
        self.explore_queue = asyncio.Queue()      # 탐색용 URL (링크 발견용)
        
        # 중복 체크 및 통계
        self.discovered_urls = set()              # 전체 발견된 URL
        self.collected_urls = set()               # 실제 수집된 URL
        self.explored_urls = set()                # 탐색한 URL
        
        # ✅ 활성 작업 추적 (플레이라이트 조기 종료 방지)
        self.active_workers = 0                   # 현재 작업 중인 워커 수
        self.active_playwright_tasks = 0          # 플레이라이트 작업 중인 수
        self._lock = asyncio.Lock()               # 동기화용 락
        
        # 설정값들
        self.max_crawl_urls = max_crawl_urls
        self.url_filter = url_filter
        
        # 카운터들
        self.total_discovered = 0
        self.total_collected = 0
        self.total_explored = 0
    
    async def add_url(self, url: str, depth: int, netloc_filter: str = None):
        """URL을 큐에 추가 (스마트 필터링 적용)"""
        if url in self.discovered_urls:
            return False
        
        # 도메인 체크
        if netloc_filter:
            parsed = urlparse(url)
            if parsed.netloc != netloc_filter:
                return False
                
        self.discovered_urls.add(url)
        self.total_discovered += 1
        
        # ✅ 선필터링: URL 필터에 따라 큐 분리
        if self.should_collect_url(url):
            await self.collect_queue.put((url, depth, "COLLECT"))
            self.total_collected += 1
            return True
        else:
            await self.explore_queue.put((url, depth, "EXPLORE"))
            return False
    
    def should_collect_url(self, url: str) -> bool:
        """URL이 수집 대상인지 판단"""
        # 이 메서드들은 url_edu.py에서 import해서 사용
        from edu.url_edu import should_exclude_url, should_include_url_by_filter
        
        # 1단계: 공통 제외 조건 체크 (모든 필터에 적용)
        if should_exclude_url(url):
            return False
        
        # 2단계: URL 필터 체크
        return should_include_url_by_filter(url, self.url_filter)
    
    async def get_next_task(self, timeout: float = None):
        """다음 처리할 작업 가져오기 (수집 우선, 탐색 후순위)"""
        try:
            # 1순위: 수집 대상 URL
            if not self.collect_queue.empty():
                if timeout:
                    return await asyncio.wait_for(self.collect_queue.get(), timeout=timeout)
                else:
                    return await self.collect_queue.get()
            
            # 2순위: 탐색용 URL (큐 크기 제한)
            if not self.explore_queue.empty():
                if timeout:
                    return await asyncio.wait_for(self.explore_queue.get(), timeout=timeout)
                else:
                    return await self.explore_queue.get()
                    
        except asyncio.TimeoutError:
            pass  # 타임아웃 시 None 반환
            
        return None
    
    async def start_worker_task(self):
        """워커가 작업을 시작할 때 호출"""
        async with self._lock:
            self.active_workers += 1
    
    async def end_worker_task(self):
        """워커가 작업을 완료할 때 호출"""
        async with self._lock:
            self.active_workers = max(0, self.active_workers - 1)
            if self.active_workers == 0:
                logger.info(f"[🎯 모든 워커 종료] 활성 워커: {self.active_workers}개")
    
    async def start_playwright_task(self):
        """플레이라이트 작업 시작할 때 호출"""
        async with self._lock:
            self.active_playwright_tasks += 1
            
    async def end_playwright_task(self):
        """플레이라이트 작업 완료할 때 호출"""
        async with self._lock:
            self.active_playwright_tasks = max(0, self.active_playwright_tasks - 1)
    
    def is_really_finished(self) -> bool:
        """진짜로 모든 작업이 완료되었는지 확인 (플레이라이트 포함)"""
        return (
            self.collect_queue.empty() and 
            self.explore_queue.empty() and
            self.active_workers == 0 and
            self.active_playwright_tasks == 0
        )

    def get_stats(self) -> dict:
        """현재 큐 상태 통계"""
        return {
            "total_discovered": self.total_discovered,
            "total_collected": self.total_collected,
            "collect_queue_size": self.collect_queue.qsize(),
            "explore_queue_size": self.explore_queue.qsize(),
            "active_workers": self.active_workers,               # ✅ 추가
            "active_playwright_tasks": self.active_playwright_tasks,  # ✅ 추가
            "max_crawl_urls": self.max_crawl_urls,
            "url_filter": self.url_filter
        }


# TODO: GlobalURLProcessor 클래스 - 매우 큰 클래스이므로 필요시 별도로 분리
# 현재 url_edu.py line 5876-6718에 위치
# 필요한 경우 추후 이동 예정


class CrawlingContext:
    """크롤링 작업의 컨텍스트를 관리하는 클래스"""
    
    def __init__(
        self,
        request,  # EduRequest 타입 (순환 import 방지를 위해 타입 힌트 생략)
        job_manager: AsyncJobManager,
        job_progress_manager: AsyncJobProgress,
        redis_client,  # aioredis.Redis 타입 (순환 import 방지를 위해 타입 힌트 생략)
        job_id: str = None,
        table_name: str = None,
        chat_id: str = None,
    ):
        self.request = request
        self.job_manager = job_manager
        self.job_progress = job_progress_manager
        self.redis = redis_client
        
        # 기본 속성들
        self.job_id = job_id or request.job_id
        self.table_name = table_name
        self.chat_id = chat_id
        self.dbname = request.db_name
        self.chat_bot_id = request.chat_bot_id
        self.crawl_mode = request.crawl_mode
        self.url_filter = request.url_filter if request.url_filter is not None else "B"
        self.crawl_target = getattr(request, 'crawl_target', 'web')  # 수집 대상 ("web" 또는 "image")
        
        # 편의 속성: image_mode (하위 호환성)
        # self.image_mode = (self.crawl_target == "image")
        
        # 카테고리 정보
        self.cate1 = request.cate1 if request.cate1 is not None else ""
        self.cate2 = request.cate2 if request.cate2 is not None else ""
        
        # 편의 속성들
        self.content_type = request.content_type
        self.contents = request.contents
        self.subjects = request.subjects
        self.memo = request.memo
    
    def get_table_name(self) -> str:
        """table_name이 없으면 생성하여 반환"""
        if not self.table_name and self.chat_id:
            self.table_name = f"td_{self.chat_id}_training_data"
        return self.table_name