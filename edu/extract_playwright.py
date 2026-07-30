"""
Playwright 기반 동적 웹페이지 렌더링 모듈

이 모듈은 JavaScript로 렌더링되는 동적 웹페이지를 처리하기 위한
Playwright 관련 함수들을 제공합니다.
"""

import os
import logging
import time
import asyncio
from typing import Optional, List, Tuple
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright._impl._errors import TargetClosedError
from config import Config
from utils.logging_util import LoggerSingleton
from edu.classes import CrawlStopSignal

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.extract_playwright", level=logging.INFO)

# ✅ Playwright 동시성 제어를 위한 전역 세마포어 추가 (Config 설정 적용)
PLAYWRIGHT_SEMAPHORE = asyncio.Semaphore(Config.PLAYWRIGHT_MAX_CONCURRENT)  # 최대 브라우저 동시 실행

class BrowserRecreatedError(RuntimeError):
    """브라우저 재생성으로 인해 진행 중 작업이 중단된 경우."""


# ✅ Playwright 브라우저 풀 관리 (브라우저 재사용으로 성능 향상)
_playwright_instance = None
_browser_instances: List = []
_browser_last_used: List[Optional[float]] = []
_browser_page_count: List[int] = []
_browser_generation: List[int] = []
_browser_lock = asyncio.Lock()
_browser_pool_index = 0

BROWSER_POOL_SIZE = 3  # ✅ 브라우저 인스턴스 수
BROWSER_MAX_PAGES = 1000  # 브라우저당 최대 페이지 수 (이후 재시작)
BROWSER_IDLE_TIMEOUT = 300  # 5분간 미사용 시 브라우저 종료 (초 단위)


def _init_browser_pool() -> None:
    """브라우저 풀 초기화"""
    global _browser_instances, _browser_last_used, _browser_page_count, _browser_generation
    if not _browser_instances:
        _browser_instances = [None] * BROWSER_POOL_SIZE
        _browser_last_used = [None] * BROWSER_POOL_SIZE
        _browser_page_count = [0] * BROWSER_POOL_SIZE
        _browser_generation = [0] * BROWSER_POOL_SIZE


def _is_browser_recreated(slot_idx: int, generation: int) -> bool:
    """브라우저 재생성 여부 확인"""
    if slot_idx < 0 or slot_idx >= len(_browser_generation):
        return True
    return _browser_generation[slot_idx] != generation


def get_government_user_agent():
    """정부/지자체 사이트 호환용 최신 Chrome UA 고정(마침표).

    오래된 브라우저 UA를 제거하고 최신 Chrome UA로 고정한다.

    Returns:
        최신 Chrome User-Agent 문자열.
    """
    return (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )


# ✅ 좀비 프로세스 방지를 위한 헬퍼 함수
async def cleanup_playwright_processes():
    """좀비 Playwright 프로세스 정리"""
    try:
        import psutil

        # Chromium 프로세스 찾기
        chromium_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and 'chromium' in proc.info['name'].lower():
                    chromium_processes.append(proc)
                elif proc.info['cmdline'] and any('chromium' in cmd.lower() for cmd in proc.info['cmdline']):
                    chromium_processes.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if chromium_processes:
            logger.warning(f"[좀비 프로세스 발견] {len(chromium_processes)}개의 Chromium 프로세스 정리 중...")
            for proc in chromium_processes:
                try:
                    proc.terminate()
                    await asyncio.sleep(0.1)
                    if proc.is_running():
                        proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            logger.info(f"[좀비 프로세스 정리 완료] {len(chromium_processes)}개 프로세스 정리됨")
    except ImportError:
        logger.debug("[psutil 없음] 좀비 프로세스 정리를 위해 psutil 설치 권장")
    except Exception as e:
        logger.warning(f"[좀비 프로세스 정리 오류] {e}")


# ✅ Playwright 브라우저 풀 관리 함수들
async def get_or_create_browser() -> Tuple[object, int, int]:
    """전역 브라우저 인스턴스 가져오기 또는 생성 (재사용).
    
    브라우저 인스턴스를 재사용하여 시작/종료 오버헤드를 제거한다.
    일정 페이지 수 처리 후 또는 장시간 미사용 시 자동으로 재시작한다.
    
    Returns:
        브라우저 인스턴스
    
    Raises:
        Exception: 브라우저 생성 실패 시
    """
    global _playwright_instance, _browser_instances, _browser_last_used, _browser_page_count
    global _browser_generation, _browser_pool_index
    
    async with _browser_lock:
        _init_browser_pool()
        current_time = time.time()

        # 플레이스홀더 Playwright 인스턴스 준비
        if _playwright_instance is None:
            _playwright_instance = await async_playwright().start()

        # 라운드 로빈으로 브라우저 슬롯 선택
        slot_idx = _browser_pool_index % BROWSER_POOL_SIZE
        _browser_pool_index += 1

        browser = _browser_instances[slot_idx]
        last_used = _browser_last_used[slot_idx]
        page_count = _browser_page_count[slot_idx]
        
        # 브라우저가 없거나 너무 오래 사용했거나 idle timeout 초과 시 재생성
        should_recreate = (
            browser is None or
            page_count >= BROWSER_MAX_PAGES or
            (last_used and current_time - last_used > BROWSER_IDLE_TIMEOUT)
        )
        
        if should_recreate:
            # 기존 브라우저 정리
            if browser:
                try:
                    logger.info(
                        f"[브라우저 재시작] 슬롯:{slot_idx}, 사용 페이지 수: {page_count}, "
                        f"마지막 사용: {int(current_time - last_used if last_used else 0)}초 전"
                    )
                    await browser.close()
                except Exception as e:
                    logger.warning(f"[브라우저 종료 오류] {e}")
                browser = None
            
            # 새 브라우저 생성
            logger.info("[브라우저 풀] 새 브라우저 인스턴스 생성 중...")
            browser = await _playwright_instance.chromium.launch(
                headless=True,
                ignore_default_args=["--enable-automation"],
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--disable-images',
                    '--disable-audio-output',
                    '--disable-background-networking',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-breakpad',
                    '--disable-client-side-phishing-detection',
                    '--disable-component-update',
                    '--disable-default-apps',
                    '--disable-domain-reliability',
                    '--disable-features=TranslateUI,VizDisplayCompositor',
                    '--disable-hang-monitor',
                    '--disable-ipc-flooding-protection',
                    '--disable-popup-blocking',
                    '--disable-prompt-on-repost',
                    '--disable-renderer-backgrounding',
                    '--disable-sync',
                    '--disable-translate',
                    '--disable-web-security',
                    '--metrics-recording-only',
                    '--no-crash-upload',
                    '--no-default-browser-check',
                    '--no-first-run',
                    '--no-pings',
                    '--password-store=basic',
                    '--use-mock-keychain',
                    '--disable-blink-features=AutomationControlled',
                    '--user-agent=' + get_government_user_agent()
                ]
            )
            _browser_instances[slot_idx] = browser
            _browser_page_count[slot_idx] = 0
            _browser_generation[slot_idx] += 1
            logger.info(f"[브라우저 풀] 새 브라우저 인스턴스 생성 완료 (슬롯:{slot_idx})")
        
        _browser_last_used[slot_idx] = current_time
        _browser_page_count[slot_idx] += 1

        return _browser_instances[slot_idx], slot_idx, _browser_generation[slot_idx]


# ✅ Playwright 전체 타임아웃을 위한 래퍼 함수 (Config 설정 적용)
async def fetch_page_with_timeout(url: str, retry_count: int = 0, timeout: int = None, stop_signal: CrawlStopSignal = None) -> str:
    """Playwright 실행을 전체 타임아웃으로 래핑
    
    Args:
        url: 크롤링할 URL
        retry_count: 재시도 횟수
        timeout: 타임아웃 (초)
        stop_signal: 중단 신호 객체
    """
    # ✅ 시작 전 중단 신호 확인
    if stop_signal and stop_signal.is_stopped():
        logger.info(f"[🛑 타임아웃 래퍼 시작 전 중단] URL: {url}")
        return None
    
    if timeout is None:
        timeout = Config.PLAYWRIGHT_TIMEOUT  # Config에서 타임아웃 가져오기

    try:
        # Playwright 실행을 타임아웃으로 래핑
        html_content = await asyncio.wait_for(
            fetch_page_with_playwright(url, retry_count, stop_signal),
            timeout=timeout
        )
        return html_content
    except asyncio.TimeoutError:
        logger.warning(f"[Playwright 전체 타임아웃] URL: {url}, 타임아웃: {timeout}초 - 재시도 {retry_count + 1}")
        if retry_count < Config.PLAYWRIGHT_MAX_RETRIES:
            logger.info(f"[Playwright 재시도] URL: {url}, 재시도 횟수: {retry_count + 1}/{Config.PLAYWRIGHT_MAX_RETRIES}")
            return await fetch_page_with_timeout(url, retry_count + 1, timeout, stop_signal)
        else:
            logger.error(f"[Playwright 최종 실패] URL: {url}, 최대 재시도 초과")
            return None
    except (TargetClosedError, BrowserRecreatedError) as e:
        logger.warning(f"[Playwright 재시도 트리거] URL: {url}, 사유: {type(e).__name__}")
        if retry_count < Config.PLAYWRIGHT_MAX_RETRIES:
            logger.info(f"[Playwright 재시도] URL: {url}, 재시도 횟수: {retry_count + 1}/{Config.PLAYWRIGHT_MAX_RETRIES}")
            return await fetch_page_with_timeout(url, retry_count + 1, timeout, stop_signal)
        logger.error(f"[Playwright 최종 실패] URL: {url}, 최대 재시도 초과")
        return None
    except Exception as e:
        logger.error(f"[Playwright 오류] URL: {url}: {str(e)}")
        return None


async def fetch_page_with_playwright(url: str, retry_count: int = 0, stop_signal: CrawlStopSignal = None) -> str:
    """Playwright를 사용하여 동적 페이지를 렌더링하고 HTML을 반환 (브라우저 재사용으로 최적화).
    
    브라우저 인스턴스를 재사용하여 매번 브라우저를 시작/종료하는 오버헤드를 제거한다.
    
    Args:
        url: 크롤링할 URL
        retry_count: 현재 재시도 횟수 (기본값: 0)
        stop_signal: 중단 신호 객체
    
    Returns:
        렌더링된 HTML 콘텐츠
    
    Raises:
        Exception: 페이지 렌더링 실패 시
    """
    # ✅ 시작 전 중단 신호 확인
    if stop_signal and stop_signal.is_stopped():
        logger.info(f"[🛑 Playwright 렌더링 시작 전 중단] URL: {url}")
        return None
    
    logger.info(f"[Playwright 페이지 렌더링 시작] URL: {url} (시도 {retry_count + 1})")

    # ✅ 세마포어로 동시 실행 페이지 수 제한
    async with PLAYWRIGHT_SEMAPHORE:
        page = None
        try:
            # ✅ 브라우저 생성 전 중단 신호 확인
            if stop_signal and stop_signal.is_stopped():
                logger.info(f"[🛑 브라우저 생성 전 중단] URL: {url}")
                return None

            # ✅ 재사용 가능한 브라우저 가져오기 (새로 생성하지 않음)
            browser, slot_idx, generation = await get_or_create_browser()

            # ✅ 브라우저 재생성 감지
            if _is_browser_recreated(slot_idx, generation):
                raise BrowserRecreatedError("브라우저 재생성 감지 (페이지 생성 전)")

            # ✅ 새 페이지 생성 (브라우저는 재사용)
            # context = await browser.context()
            page = await browser.new_page()
            dialog_state = {
                "messages": [],
                "restricted": False,
            }

            async def _dismiss_dialog(dialog):
                try:
                    message = str(getattr(dialog, "message", "") or "").strip()
                except Exception:
                    message = ""
                if message:
                    dialog_state["messages"].append(message)
                    msg_low = message.lower()
                    if any(
                        token in msg_low
                        for token in (
                            "권한이 없습니다",
                            "접근이 제한",
                            "접근 제한",
                            "접근 권한",
                            "permission denied",
                            "access denied",
                            "forbidden",
                        )
                    ):
                        dialog_state["restricted"] = True
                        logger.warning(f"[접근 제한 dialog 감지] URL: {url}, message: {message[:200]}")
                try:
                    await dialog.dismiss()
                except Exception:
                    pass

            page.on("dialog", _dismiss_dialog)

            if _is_browser_recreated(slot_idx, generation):
                raise BrowserRecreatedError("브라우저 재생성 감지 (페이지 생성 후)")
            
            # ✅ Headless 탐지 완화용 기본 헤더/환경 설정
            try:
                ua = get_government_user_agent()
                await page.set_extra_http_headers({
                    "User-Agent": ua,
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Upgrade-Insecure-Requests": "1",
                    "DNT": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                })
                await page.set_viewport_size({"width": 1365, "height": 768})
                await page.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )
            except Exception as e:
                logger.warning(f"[Playwright 헤더/환경 설정 실패] {e} - URL: {url}")

            # ✅ 정부 사이트 호환성을 위한 긴 타임아웃 설정 (Config 적용)
            page.set_default_timeout(Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)  # Config 설정 * 1000 (밀리초)
            page.set_default_navigation_timeout(Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000 - 5000)  # 5초 여유

            # ✅ 최적화된 리소스 차단 함수 (텍스트 추출만 필요하므로 공격적으로 차단)
            async def optimized_resource_blocker(route):
                """텍스트 추출만 필요하므로 모든 불필요한 리소스 차단 (속도 최적화)"""
                try:
                    request = route.request
                    if not request:
                        await route.abort()
                        return
                    
                    request_url = request.url.lower() if request.url else ""
                    resource_type = request.resource_type.lower() if request.resource_type else ""
                    
                    # ✅ JSP 렌더링을 위해 스크립트/CSS 전역 허용
                    if resource_type in ["document", "xhr", "fetch", "script", "stylesheet"]:
                        await route.continue_()
                        return
                    
                    # ✅ 차단할 리소스 타입 (텍스트 추출에는 불필요)
                    blocked_resource_types = [
                        "image",      # 이미지
                        "media",      # 비디오/오디오
                        "font",       # 폰트
                        "manifest",   # 매니페스트
                        "websocket",  # 웹소켓
                        "stylesheet", # CSS (텍스트만 필요하므로 차단)
                        "script",     # JavaScript (텍스트만 필요하므로 차단)
                        "other"       # 기타
                    ]
                    
                    # ✅ 리소스 타입으로 차단 (가장 빠름)
                    if resource_type in blocked_resource_types:
                        await route.abort()
                        return
                    
                    # ✅ 차단할 URL 패턴 (추가 안전장치)
                    blocked_url_patterns = [
                        # 분석/추적 도구
                        'google-analytics', 'googletagmanager', 'facebook.com/tr', 'google.com/analytics',
                        'doubleclick', 'googlesyndication', 'ads', 'analytics', 'tracking', 'pixel',
                        'gtm', 'gtag', 'adnxs', 'adsystem', 'adform', 'outbrain', 'taboola',
                        # 미디어 파일 확장자
                        '.woff', '.woff2', '.ttf', '.otf', '.eot',  # 폰트
                        '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico', '.bmp',  # 이미지
                        '.mp4', '.mp3', '.avi', '.mov', '.wmv', '.flv', '.webm',  # 비디오/오디오
                        '.pdf', '.zip', '.rar', '.7z',  # 문서/압축
                        # 외부 미디어 서비스
                        'youtube.com', 'vimeo.com', 'twitter.com/widgets', 'instagram.com',
                        # CSS/JS 파일
                        '.css', '.scss', '.less', '.sass',
                        '.js', '.mjs', '.jsx', '.ts', '.tsx',
                        # 기타
                        'favicon.ico', 'robots.txt', 'sitemap.xml'
                    ]
                    
                    # ✅ URL 패턴으로 차단
                    if request_url and any(pattern in request_url for pattern in blocked_url_patterns):
                        await route.abort()
                        return
                    
                    # ✅ HTML 문서만 허용 (텍스트 추출에 필수)
                    if resource_type == "document":
                        await route.continue_()
                        return
                    
                    # ✅ XHR/Fetch 요청도 허용 (일부 동적 콘텐츠 로딩용)
                    if resource_type in ["xhr", "fetch"]:
                        logger.info(f"[DEBUG] XHR/Fetch 요청 허용 - URL: {request_url}")
                        await route.continue_()
                        return
                    
                    # ✅ 나머지는 모두 차단 (안전하게)
                    await route.abort()
                    
                except Exception as e:
                    # 오류 발생 시 차단 (안전한 폴백)
                    try:
                        await route.abort()
                    except:
                        pass
            
            # ✅ 모든 요청에 대해 최적화된 리소스 차단 적용
            try:
                # 전역 라우터 설정 (모든 요청에 적용)
                await page.route("**/*", optimized_resource_blocker)
                logger.debug(f"[Playwright 리소스 차단] 텍스트 추출 최적화 모드 활성화 - URL: {url}")
            except Exception as e:
                logger.warning(f"[Playwright 리소스 차단 설정 실패] {e}, 계속 진행 - URL: {url}")
            # ✅ 페이지 로드 (안정성 우선, 리다이렉트 처리 개선)
            final_url = url
            retry_count_load = 0
            max_load_retries = 2
            
            while retry_count_load <= max_load_retries:
                try:
                    # ✅ page.goto() 전 중단 신호 확인
                    if stop_signal and stop_signal.is_stopped():
                        logger.info(f"[🛑 page.goto() 전 중단] URL: {url}")
                        return None

                    # page.goto() 호출 전 디버깅
                    
                    try:
                        # 극도로 세밀한 디버깅
                        response = await page.goto(url, wait_until="domcontentloaded", timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000 - 5000)

                        if _is_browser_recreated(slot_idx, generation):
                            raise BrowserRecreatedError("브라우저 재생성 감지 (page.goto 이후)")
                        
                        # ✅ page.goto() 완료 후 중단 신호 확인
                        if stop_signal and stop_signal.is_stopped():
                            logger.info(f"[🛑 page.goto() 완료 후 중단] URL: {url}")
                            return None
                        
                        if response is None:
                            logger.warning(f"[DEBUG] page.goto() 반환값이 None - URL: {url}")
                        else:
                            logger.info(f"[DEBUG] page.goto() 성공 - URL: {url}, status: {getattr(response, 'status', 'Unknown')}")

                            
                    except asyncio.CancelledError:
                        # CancelledError는 다시 발생시켜야 함
                        logger.error(f"[DEBUG] page.goto() 작업 취소됨 - URL: {url} (CancelledError)")
                        raise
                    except TargetClosedError as goto_error:
                        logger.warning(f"[DEBUG] TargetClosedError - URL: {url}, 재시도 트리거")
                        raise goto_error
                    except Exception as goto_error:
                        # 예외를 다시 발생시켜 상위에서 처리하도록 함
                        logger.error(f"[DEBUG] page.goto() 예외 발생 - URL: {url}, 예외 타입: {type(goto_error).__name__}, 메시지: {goto_error}", exc_info=True)
                        raise

                    # URL 제한 여부 및 응답 상태 확인
                    if response:
                        status = response.status
                        final_url = response.url
                        logger.info(f"[페이지 응답] URL: {url} → {final_url}, Status: {status}")
                        
                        if status == 403:
                            logger.warning(f"[접근 금지] URL: {url}, Status: 403 - 서버에서 접근을 차단")
                            return None  # 에러 페이지 크롤링 방지
                        elif status == 404:
                            logger.debug(f"[페이지 없음] URL: {url}, Status: 404 - 페이지를 찾을 수 없음")
                            return None  # 에러 페이지 크롤링 방지
                        elif status >= 400 and status < 500:
                            logger.warning(f"[클라이언트 오류] URL: {url}, Status: {status}")
                            if retry_count_load < max_load_retries:
                                await asyncio.sleep(1)
                                retry_count_load += 1
                                continue
                            logger.error(f"[클라이언트 오류 최종 실패] URL: {url}, Status: {status}")
                            return None  # 에러 페이지 크롤링 방지
                        elif status >= 500:
                            logger.warning(f"[서버 오류] URL: {url}, Status: {status}")
                            if retry_count_load < max_load_retries:
                                await asyncio.sleep(2)
                                retry_count_load += 1
                                continue
                            logger.error(f"[서버 오류 최종 실패] URL: {url}, Status: {status}")
                            return None  # 에러 페이지 크롤링 방지
                        elif final_url != url:
                            logger.info(f"[리다이렉트] 원본: {url} → 최종: {final_url}")
                            # 리다이렉트된 URL로 업데이트하여 후속 처리에 활용
                            url = final_url
                    else:
                        logger.warning(f"[응답 없음] URL: {url} - 응답 객체가 없음")
                        if retry_count_load < max_load_retries:
                            await asyncio.sleep(1)
                            retry_count_load += 1
                            continue
                        logger.error(f"[응답 없음 최종 실패] URL: {url}")
                        return None  # 응답 없을 경우 크롤링 방지
                    
                    break
                    
                except PlaywrightTimeoutError as e:
                    logger.warning(f"[페이지 로드 타임아웃] URL: {url} (시도 {retry_count_load + 1}/{max_load_retries + 1}), 오류: {e}")
                    if retry_count_load < max_load_retries:
                        await asyncio.sleep(2)
                        retry_count_load += 1
                        continue
                    else:
                        logger.warning(f"[페이지 로드 최종 실패] URL: {url} - 현재 상태로 진행")
                        break
                except Exception as e:
                    if retry_count_load < max_load_retries:
                        await asyncio.sleep(2)
                        retry_count_load += 1
                        continue
                    else:
                        # 예외 발생해도 빈 HTML로라도 계속 진행
                        break
            
            if dialog_state["restricted"]:
                logger.warning(
                    "[접근 제한 페이지 제외] URL: %s, dialogs=%s",
                    final_url,
                    dialog_state["messages"][:3],
                )
                return None

            # ✅ 스마트 조건부 대기: 실제 콘텐츠가 로드되면 즉시 진행 (최대 2초)
            content_loaded = False
            try:
                # 주요 콘텐츠 셀렉터를 병렬로 확인 (하나라도 나타나면 진행)
                common_selectors = [
                    "body", "main", "#main", "#content", ".content", 
                    ".container", "article", "section", "table", 
                    ".board", ".list", "tbody", "ul", "div"
                ]
                
                # 여러 셀렉터 중 하나라도 나타나면 진행 (race condition)
                await page.wait_for_selector(", ".join(common_selectors), timeout=2000, state="attached")
                content_loaded = True
            except PlaywrightTimeoutError:
                logger.warning(f"[DEBUG] 콘텐츠 로드 타임아웃 - URL: {url}")
            except Exception as e:
                logger.error(f"[DEBUG] 콘텐츠 로드 예외 발생 - URL: {url}, 예외 타입: {type(e).__name__}, 메시지: {e}", exc_info=True)
            
            # 콘텐츠가 감지되면 짧은 안정화 대기, 아니면 조금 더 대기
            if content_loaded:
                await page.wait_for_timeout(300)  # 콘텐츠 감지됨: 0.3초만 대기
            else:
                await page.wait_for_timeout(800)  # 콘텐츠 미감지: 0.8초 대기

            # ✅ 네트워크 유휴 상태 대기 (최적화: 2초)
            try:
                await page.wait_for_load_state("networkidle", timeout=2000)
            except PlaywrightTimeoutError:
                logger.warning(f"[DEBUG] 네트워크 유휴 상태 타임아웃 - URL: {url}")

            # ✅ 동적 콘텐츠 로딩을 위한 JavaScript 인터랙션 최적화
            try:
                # 고속 점진적 스크롤로 lazy loading 콘텐츠 빠르게 로드
                await page.evaluate("""
                    (async () => {
                    // 최적화된 스크롤: 속도 2배 향상 (200ms → 100ms)
                    const scrollStep = 500;
                    const scrollDelay = 100;  // 200ms → 100ms로 단축
                    let scrollPosition = 0;
                    const maxScroll = document.body.scrollHeight;
                    
                    function scrollGradually() {
                        return new Promise((resolve) => {
                            function scroll() {
                                window.scrollTo(0, scrollPosition);
                                scrollPosition += scrollStep;
                                
                                if (scrollPosition >= maxScroll) {
                                    // 최하단 도달 후 최상단으로
                                    setTimeout(() => {
                                        window.scrollTo(0, 0);
                                        resolve();
                                    }, scrollDelay);
                                } else {
                                    setTimeout(scroll, scrollDelay);
                                }
                            }
                            scroll();
                        });
                    }
                    
                        await scrollGradually();
                    })();
                """)
                
                await page.wait_for_timeout(500)   # 1초 → 0.5초로 단축 (동적 콘텐츠 로딩 대기)

                # ✅ 사이트별 맞춤형 대기 로직 최적화 (전체적으로 30-40% 단축)
                site_specific_wait = 0
                if any(domain in final_url.lower() for domain in ['go.kr', 'gov.kr', '.seoul.kr']):
                    logger.info(f"[정부 사이트 특수 대기] URL: {final_url}")
                    try:
                        # 표나 리스트 콘텐츠가 로드될 때까지 대기 (타임아웃 단축)
                        await page.wait_for_selector("table, .board, .list, .content, tbody, main", timeout=2000)  # 3초 → 2초
                        await page.wait_for_timeout(400)  # 800ms → 400ms로 단축
                        site_specific_wait = 400
                        logger.info(f"[정부 사이트 콘텐츠 로드 완료] URL: {final_url}")
                    except Exception as e:
                        logger.warning(f"[정부 사이트 콘텐츠 대기 타임아웃] URL: {final_url}, 오류: {e}")
                elif any(domain in final_url.lower() for domain in ['.or.kr', '.co.kr', '.re.kr']):
                    logger.info(f"[한국 사이트 특수 대기] URL: {final_url}")
                    try:
                        await page.wait_for_selector("main, .main, .content, #content, .container", timeout=1500)  # 2.5초 → 1.5초
                        await page.wait_for_timeout(300)  # 600ms → 300ms로 단축
                        site_specific_wait = 300
                        logger.info(f"[한국 사이트 콘텐츠 로드 완료] URL: {final_url}")
                    except Exception as e:
                        logger.warning(f"[한국 사이트 콘텐츠 대기 타임아웃] URL: {final_url}, 오류: {e}")

            except Exception as e:
                logger.error(f"[DEBUG] 사이트별 맞춤형 대기 예외 발생 - URL: {final_url}, 예외 타입: {type(e).__name__}, 메시지: {e}", exc_info=True)
            # ✅ HTML 콘텐츠 추출 (강화된 재시도 로직)
            html_content = None
            retry_delays = [500, 1000, 2000]  # 0.5초, 1초, 2초 간격으로 재시도
            
            for retry_attempt in range(3):  # 총 3번 시도
                try:
                    html_content = await page.content()
                    
                    # HTML 품질 검증 (최소 크기 및 기본 태그 확인)
                    if len(html_content) > 10 and '<body' in html_content.lower():
                        logger.info(f"[HTML 추출 성공] URL: {final_url}, 크기: {len(html_content)} bytes")
                        break
                    elif len(html_content) > 100:  # 최소한의 HTML이라도 있으면 사용
                        logger.info(f"[HTML 추출 완료] URL: {final_url}, 크기: {len(html_content)} bytes (최소 품질)")
                        break
                    else:
                        logger.warning(f"[HTML 품질 부족] URL: {final_url}, 크기: {len(html_content)} bytes - 재시도")
                        if retry_attempt < 2:
                            await page.wait_for_timeout(retry_delays[retry_attempt])
                            continue
                except Exception as e:
                    logger.error(f"[DEBUG] HTML 추출 예외 발생 - URL: {final_url}, 시도 {retry_attempt + 1}, 예외: {type(e).__name__}: {e}")
                    if retry_attempt < 2:
                        await page.wait_for_timeout(retry_delays[retry_attempt])
                        continue
                    else:
                        # 최후의 수단: 빈 HTML이라도 생성
                        break
            
            # HTML 콘텐츠 품질 및 제한 여부 확인
            if html_content:
                # 접근 제한 메시지 확인
                restriction_keywords = [
                    '접근이 제한', '권한이 없습니다', '로그인이 필요', '인증이 필요',
                    'access denied', 'unauthorized', 'forbidden', 'login required',
                    '차단되었습니다', '허용되지 않습니다', '제한된 페이지'
                ]
                
                html_lower = html_content.lower()
                for keyword in restriction_keywords:
                    if keyword in html_lower:
                        logger.warning(f"[접근 제한 감지] URL: {final_url}, 키워드: '{keyword}'")
                        break
                
                # 기본적인 HTML 구조 확인
                if '<title>' in html_content:
                    title_start = html_content.find('<title>') + 7
                    title_end = html_content.find('</title>')
                    if title_end > title_start:
                        page_title = html_content[title_start:title_end].strip()
                        logger.info(f"[페이지 제목 확인] URL: {final_url}, 제목: {page_title[:100]}...")
                
                # 본문 콘텐츠 양 최종 확인
                body_start = html_content.find('<body')
                body_end = html_content.rfind('</body>')
                if body_start != -1 and body_end != -1:
                    body_content = html_content[body_start:body_end]
                    logger.info(f"[본문 HTML 최종 확인] URL: {final_url}, Body 크기: {len(body_content)} bytes")
                else:
                    logger.warning(f"[본문 HTML 최종 없음] URL: {final_url} - body 태그를 찾을 수 없음 (모든 재시도 완료)")
            else:
                logger.error(f"[HTML 추출 최종 실패] URL: {final_url} - 빈 콘텐츠")

            if _is_browser_recreated(slot_idx, generation):
                raise BrowserRecreatedError("브라우저 재생성 감지 (HTML 추출 후)")

            return html_content

        except PlaywrightTimeoutError as e:
            logger.error(f"[Playwright 타임아웃] URL: {url}: {str(e)}")
            return None  # 타임아웃 시 None 반환하여 프로그램 계속 실행
        except (TargetClosedError, BrowserRecreatedError):
            raise
        except Exception as e:
            logger.error(f"[Playwright 오류] URL: {url}: {str(e)}", exc_info=True)
            return None  # 모든 예외 시 None 반환하여 프로그램 계속 실행
        finally:
            # ✅ 페이지 정리 (브라우저는 재사용하므로 닫지 않음)
            if page:
                try:
                    await page.close()
                    logger.debug(f"[페이지 정리] URL: {url}")
                except Exception as cleanup_error:
                    logger.warning(f"[페이지 정리 오류] URL: {url}: {cleanup_error}")

            logger.debug(f"[Playwright 세마포어 해제] URL: {url}")
