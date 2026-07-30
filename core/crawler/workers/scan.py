# core/crawler/workers/scan.py
import sys
import os
import random
import asyncio
import logging
import json # JSON 파일 생성용
from datetime import datetime # 저장 시각 기록용
import re
from urllib.parse import urljoin
from typing import Optional, Dict, Any, List, Callable, Awaitable
from playwright.async_api import Browser, BrowserContext
from utils.url import extract_download_url_from_js
from backend.board.anseong_file import resolve_anseong_yhlib_download_url
from core.crawler.dedup import try_acquire_cross_job_claim, release_cross_job_claim
from backend.shared.playwright_optimizations import (
    apply_stealth_if_needed,
    configure_context_for_crawl,
)

# 첨부파일 후보 판정(확장자/핸들러)
try:
    from config.constants import ALLOWED_EXTENSIONS
except Exception:
    ALLOWED_EXTENSIONS = ['.hwp', '.hwpx', '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.txt', '.csv', '.xlsx', '.xls']

# 프로젝트 루트 경로를 시스템 경로에 추가하여 모듈 참조를 가능하게 합니다.
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger(__name__)


def _scan_debug_stdout_enabled() -> bool:
    return str(os.getenv("SCAN_WORKER_DEBUG_STDOUT", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _scan_debug_print(message: str) -> None:
    if not _scan_debug_stdout_enabled():
        return
    try:
        print(message, flush=True)
    except Exception:
        pass


def _env_float(name: str, default: float, *, min_value: float = 0.0, max_value: float = 300.0) -> float:
    try:
        value = float(os.getenv(name, str(default)) or str(default))
    except Exception:
        value = default
    return max(min_value, min(float(value), max_value))


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 10) -> int:
    try:
        value = int(os.getenv(name, str(default)) or str(default))
    except Exception:
        value = default
    return max(min_value, min(int(value), max_value))


def _scan_timeout_ms_for_url(url: str) -> int:
    lowered = str(url or "").lower()
    default_sec = _env_float("SCAN_WORKER_GOTO_TIMEOUT_SEC", 25.0, min_value=3.0, max_value=120.0)
    if "gm.go.kr" in lowered:
        default_sec = _env_float("SCAN_WORKER_GM_GOTO_TIMEOUT_SEC", 12.0, min_value=3.0, max_value=60.0)
    return int(default_sec * 1000)


async def _polite_scan_delay(url: str, attempt: int) -> None:
    lowered = str(url or "").lower()
    min_sec = _env_float("SCAN_WORKER_DELAY_MIN_SEC", 0.05, min_value=0.0, max_value=10.0)
    max_sec = _env_float("SCAN_WORKER_DELAY_MAX_SEC", 0.25, min_value=0.0, max_value=30.0)
    if "gm.go.kr" in lowered:
        min_sec = _env_float("SCAN_WORKER_GM_DELAY_MIN_SEC", 0.4, min_value=0.0, max_value=30.0)
        max_sec = _env_float("SCAN_WORKER_GM_DELAY_MAX_SEC", 1.2, min_value=0.0, max_value=60.0)
    if max_sec < min_sec:
        max_sec = min_sec
    delay = random.uniform(min_sec, max_sec) if max_sec > 0 else 0.0
    if attempt > 1:
        delay += min(8.0, 0.5 * (2 ** (attempt - 2)))
    if delay > 0:
        await asyncio.sleep(delay)

# 단계별 URL 리포트(다운로드 폴더 JSON 누적)
try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        try:
            import sys as _sys
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            if project_root not in _sys.path:
                _sys.path.insert(0, project_root)
            from backend.shared.stage_url_report import append_stage_urls as _impl  # type: ignore
            return _impl(stage=stage, urls=urls, job_id=job_id, db_name=db_name, output_dir=output_dir, extra_meta=extra_meta, entry_extra=entry_extra)
        except Exception:
            return None

# =================================================================
# [추가] 통합 워크플로우에 의존하지 않는 독립적인 JSON 생성 함수
# =================================================================
def force_save_scan_log_json(job_id: str, urls: list, names: list, source_page: str):
    """추출된 첨부파일 후보 URL들을 워커 폴더 내 JSON 파일로 즉시 강제 저장합니다."""
    if not job_id or not urls: return
    try:
        # 워커 전용 폴더 경로가 없으면 생성합니다.
        target_dir = os.path.join(project_root, "core", "crawler", "workers")
        existed = os.path.exists(target_dir)
        try:
            os.makedirs(target_dir, exist_ok=True)
            _scan_debug_print(f"[test010] target_dir ensured (existed_before={existed}) -> {target_dir}")
        except Exception as _mk:
            _scan_debug_print(f"[test010] target_dir create failed: {_mk} -> {target_dir}")
        filepath = os.path.join(target_dir, f"workflow_collection_{job_id}.json")
        _scan_debug_print(f"[test010] target filepath set -> {filepath}")
        
        data = []
        # 기존 파일이 존재하면 데이터를 불러와 누적 저장을 준비합니다.
        file_existed = False
        if os.path.exists(filepath):
            file_existed = True
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except Exception:
                        data = []
            except Exception as _rerr:
                _scan_debug_print(f"[test010] existing file read failed: {_rerr} -> {filepath}")
        _scan_debug_print(f"[test010] existing_file={file_existed} current_items={len(data)}")
                
        # 중복 URL을 방지하며 새로운 링크 정보만 리스트에 추가합니다.
        for i, u in enumerate(urls):
            if u and not any(e.get("url") == u for e in data):
                data.append({
                    "url": u,
                    "filename": names[i] if i < len(names) else u.split("/")[-1],
                    "source_page": source_page, # 해당 링크가 발견된 상세페이지 주소
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "collection_count": "scan_stage" # 탐색(Scan) 단계에서 발견됨을 표시
                })
                
        # 병합된 데이터를 JSON 파일로 저장합니다.
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            _scan_debug_print(f"[test010] JSON 기록 완료 | job_id={job_id} 누적={len(data)}건 -> {filepath}")
        except Exception as _werr:
            _scan_debug_print(f"[test010] JSON write failed: {_werr} -> {filepath}")
    except Exception as e:
        _scan_debug_print(f"[test010] JSON 생성 실패: {e}")

# =================================================================
# [공용 유틸리티] 첨부파일 판별용 (기존 기능 유지)
# =================================================================
def _row_has_attachment(tag) -> bool:
    """행(tr/li) 내부 요소를 분석하여 첨부파일 존재 여부를 판단합니다."""
    if not tag: return False
    try:
        row = tag.find_parent(["tr", "li"]) or tag.find_parent()
    except Exception: row = None
    if row is None: return False
    
    try:
        text = row.get_text(" ", strip=True).lower()
        if any(k in text for k in ("첨부", "파일", "download", "file")): return True
        for a in row.find_all("a", href=True):
            href = (a.get("href") or "").lower()
            if any(k in href for k in ("filedown", "atchfileid", "filesn", "filedownload", "download")):
                return True
        class_str = " ".join(row.get("class") or []).lower()
        if any(k in class_str for k in ("file", "attach", "down")): return True
    except Exception: pass
    return False

# =================================================================
# [Scan Worker] 메인 워커 로직
# =================================================================
async def scan_worker(
    in_queue: asyncio.Queue,
    scan_batch_queue: Any,
    progress_queue: asyncio.Queue,
    browser: Browser,
    browser_relauncher: Optional[Callable[[], Awaitable[Browser]]] = None,
    **kwargs
):
    """상세페이지에 접속하여 '첨부파일 후보' 링크만 추출하여 다음 단계로 전달합니다."""
    logger.info("[Scan] Worker started - Attachment Extract Mode")
    _scan_debug_print(f"[Scan] worker started | file={__file__} pid={os.getpid()}")
    context: Optional[BrowserContext] = None
    _last_idle_print = 0.0
    # 탐색 큐가 오래 비면(다운로드는 다른 큐에서 진행) 주기적으로 idle 로그가 찍히는데,
    # INFO/print는 오해(멈춤으로 보임)를 부추기므로 기본은 DEBUG만. 필요 시 SCAN_WORKER_IDLE_LOG=1
    try:
        _idle_log_interval = float(os.getenv("SCAN_WORKER_IDLE_LOG_SEC", "120") or "120")
    except Exception:
        _idle_log_interval = 120.0
    _idle_log_interval = max(0.0, _idle_log_interval)
    try:
        post_goto_wait_ms = int(os.getenv("SCAN_WORKER_POST_GOTO_WAIT_MS", "250") or "250")
    except Exception:
        post_goto_wait_ms = 250
    post_goto_wait_ms = max(0, min(post_goto_wait_ms, 2000))

    def _scan_idle_maybe_log(qsz: int) -> None:
        v = (os.getenv("SCAN_WORKER_IDLE_LOG") or "").strip().lower()
        legacy_print = v in ("1", "true", "yes", "on")
        if legacy_print:
            try:
                print(f"[Scan] idle(waiting in_queue) | qsize={qsz}", flush=True)
            except Exception:
                pass
            return
        if _idle_log_interval <= 0:
            return
        logger.debug("[Scan] idle(waiting in_queue) | qsize=%s", qsz)

    stop_event = kwargs.get('stop_event', asyncio.Event())

    def _task_done_and_decrement(item: Any) -> None:
        try:
            in_queue.task_done()
        except Exception:
            pass
        try:
            if not item:
                return
            job_id = item.get("job_id", "default") if isinstance(item, dict) else "default"
            progress_queue.put_nowait({"type": "in_flight", "stage": "scan", "delta": -1, "job_id": job_id})
        except Exception:
            # progress_queue consumer가 없거나, put_nowait 실패해도 scan 자체 처리는 계속 진행한다.
            pass

    def _is_attachment_candidate(u: str) -> bool:
        if not u:
            return False
        try:
            lu = str(u).lower()
        except Exception:
            return False
        path = lu.split("?", 1)[0]
        try:
            if any(path.endswith(ext) for ext in (ALLOWED_EXTENSIONS or [])):
                return True
        except Exception:
            pass
        hints = (
            "filedown", "filedownload", "download", "file.do", "download.do",
            "cmm/fms/filedown", "atchfile", "atchfileid", "filesn", "fileid",
            "fileseq", "file_no", "fileno", "fileseqno",
        )
        return any(h in lu for h in hints)

    async def ensure_context():
        nonlocal context, browser
        if context is None:
            if browser is None and browser_relauncher:
                browser = await browser_relauncher()
            context = await browser.new_context(ignore_https_errors=True)
            await configure_context_for_crawl(context, url or "")
        return context

    try:
        while not stop_event.is_set():
            url_item = None
            try:
                # 1. 큐에서 작업을 가져오되, 타임아웃을 1초로 짧게 설정
                try:
                    url_item = await asyncio.wait_for(in_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # 타임아웃 발생 시 즉시 중단 여부를 다시 확인
                    if stop_event.is_set():
                        break
                    now = asyncio.get_event_loop().time()
                    if _idle_log_interval > 0 and (now - _last_idle_print) >= _idle_log_interval:
                        _last_idle_print = now
                        try:
                            qsize = in_queue.qsize()
                        except Exception:
                            qsize = -1
                        _scan_idle_maybe_log(qsize)
                    continue

                # 작업을 가져왔더라도 실행 직전 중단 여부 체크
                if stop_event.is_set():
                    if url_item is not None: _task_done_and_decrement(url_item)
                    break

                # 종료 신호(None) 처리 (한 줄 주석: 큐에서 종료 신호를 받으면 작업을 끝내고 루프 탈출)
                if url_item is None:
                    break

                # 빈 아이템 스킵 (한 줄 주석: 유효하지 않은 데이터는 건너뜀)
                if not url_item:
                    _task_done_and_decrement(url_item)
                    continue

                # 데이터 가공
                url = url_item.get('url') if isinstance(url_item, dict) else url_item
                job_id = url_item.get('job_id', 'default') if isinstance(url_item, dict) else 'default'
                
                try:
                    ui_colle = (url_item.get("colle") if isinstance(url_item, dict) else None) or kwargs.get("colle")
                    colle_mode = str(ui_colle or "board").strip().lower()
                except Exception:
                    colle_mode = "board"
                effective_db_name = (
                    (url_item.get("db_name") if isinstance(url_item, dict) else None)
                    or kwargs.get("db_name")
                    or ""
                )
                cross_job_claimed = False
                scan_processed = False
                page = None
                try:
                    cross_job_claimed = await try_acquire_cross_job_claim(
                        "scan",
                        str(effective_db_name).strip(),
                        str(url or "").strip(),
                        str(job_id or "").strip(),
                    )
                except Exception:
                    cross_job_claimed = True

                if not cross_job_claimed:
                    logger.info(
                        "[Scan] skip cross-job claimed url | job_id=%s db=%s url=%s",
                        job_id,
                        effective_db_name,
                        url,
                    )
                    try:
                        await progress_queue.put(
                            {
                                "type": "scan",
                                "count": 0,
                                "items": [url],
                                "job_id": job_id,
                                "reason": "duplicate_other_job_claim",
                                "colle": colle_mode,
                            }
                        )
                    except Exception:
                        pass
                    _task_done_and_decrement(url_item)
                    continue
                
                # 페이지 접속 및 렌더링
                try:
                    _scan_debug_print(f"[test030] calling ensure_context() for job_id={job_id} url={url}")
                except Exception:
                    pass

                try:
                    ctx = await ensure_context()
                except asyncio.CancelledError:
                    try:
                        _scan_debug_print(f"[Scan] cancelled before/at ensure_context() | job_id={job_id}")
                    except Exception:
                        pass
                    raise
                page = await ctx.new_page()
                await apply_stealth_if_needed(page, url or "")
                
                try:
                    _scan_debug_print(f"[Scan] 상세페이지 진입: {url}")
                    if stop_event.is_set(): break

                    max_attempts = _env_int("SCAN_WORKER_GOTO_MAX_ATTEMPTS", 2, min_value=1, max_value=5)
                    goto_timeout_ms = _scan_timeout_ms_for_url(url)
                    last_goto_error = None
                    for attempt in range(1, max_attempts + 1):
                        try:
                            await _polite_scan_delay(url, attempt)
                            await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
                            last_goto_error = None
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception as goto_exc:
                            last_goto_error = goto_exc
                            logger.warning(
                                "[Scan] goto failed | job_id=%s attempt=%s/%s timeout_ms=%s url=%s err=%s",
                                job_id,
                                attempt,
                                max_attempts,
                                goto_timeout_ms,
                                url,
                                goto_exc,
                            )
                            if attempt >= max_attempts:
                                raise
                    if last_goto_error is not None:
                        raise last_goto_error
                    if post_goto_wait_ms:
                        await page.wait_for_timeout(post_goto_wait_ms)
                    
                    if stop_event.is_set(): break

                    # 링크 수집
                    links = await page.query_selector_all("a")
                    found_items = []
                    urls_to_log = []
                    names_to_log = []
                    
                    for a in links:
                        href = await a.get_attribute("href")
                        onclick = await a.get_attribute("onclick")
                        try:
                            link_text = (await a.inner_text() or "").strip()
                        except Exception:
                            link_text = ""
                        # 유효하지 않은 링크 스킵 (한 줄 주석: 링크 필터링)
                        raw = (href or "").strip()
                        js_raw = (onclick or "").strip()
                        if not raw and not js_raw:
                            continue
                        if raw.startswith("#") and not js_raw:
                            continue

                        # JS 클릭 다운로드의 경우: 실제 다운로드 URL로 변환
                        full_url = ""
                        if (raw.lower().startswith("javascript:") or js_raw) and not raw.startswith(("http://", "https://", "/")):
                            extracted = extract_download_url_from_js(js_raw or raw, url)
                            if not extracted:
                                extracted = resolve_anseong_yhlib_download_url(js_raw or raw, url)
                            if extracted:
                                full_url = extracted
                        if not full_url:
                            # 일반 링크
                            if raw.lower().startswith("javascript:"):
                                # JS인데 변환 실패 -> 스킵 (download 워커가 javascript:를 직접 처리 못함)
                                continue
                            full_url = urljoin(url, raw)

                        # 첨부파일 후보 여부 확인 (한 줄 주석: 확장자 및 키워드 검사)
                        if not _is_attachment_candidate(full_url): 
                            continue
                        
                        item_data = {
                            "url": full_url,
                            "name": link_text,
                            "filename": link_text,
                            "source_page": url,
                            "job_id": job_id,
                            "chat_bot_id": kwargs.get("chat_bot_id"),
                            "db_name": effective_db_name or kwargs.get("db_name"),
                            "type": "file",
                            "memo": (url_item.get("memo") if isinstance(url_item, dict) else "") or "",
                            "defer_save_batch_until_learn_list": bool(
                                url_item.get("defer_save_batch_until_learn_list")
                            ) if isinstance(url_item, dict) else False,
                        }
                        found_items.append(item_data)
                        urls_to_log.append(full_url)
                        # 파일명 추출 (한 줄 주석: URL 경로에서 마지막 파일명 분리)
                        names_to_log.append(link_text or str(full_url).split("?")[0].rstrip("/").split("/")[-1] or "unknown")
                    
                    # 수집 결과 처리 (한 줄 주석: 수집된 항목이 있을 경우에만 저장 및 전송)
                    if not found_items:
                        _scan_debug_print(f"[test011] 첨부파일 후보가 없습니다: {url}")
                    else:
                        _scan_debug_print(f"[test011] {len(found_items)}개의 후보 발견. JSON 저장 및 큐 전송 시작.")
                        # JSON 강제 저장 호출 (한 줄 주석: 독립적인 로그 파일 생성)
                        force_save_scan_log_json(job_id, urls_to_log, names_to_log, url)
                        # ✅ stage/trace JSON 누적(다운로드 폴더)
                        try:
                            append_stage_urls(
                                stage="scan",
                                urls=[
                                    {"url": u, "filename": (names_to_log[i] if i < len(names_to_log) else None), "source_page": url}
                                    for i, u in enumerate(urls_to_log or [])
                                    if u
                                ],
                                job_id=job_id,
                                db_name=kwargs.get("db_name"),
                            )
                        except Exception:
                            pass
                        
                        # 큐 전송 (중단 이벤트 발생 시 스킵)
                        if not stop_event.is_set():
                            # BatchQueue/MultiplexBatchQueue는 "아이템 1개" 단위로 put()하는 인터페이스다.
                            # found_items(list)를 그대로 넣으면 collection_worker가 중첩 리스트를 받아 예외로 빠질 수 있다.
                            for _it in (found_items or []):
                                await scan_batch_queue.put(_it)
                            eff_count = 0 if colle_mode == "file" else len(found_items)
                            await progress_queue.put({'type': 'scan', 'count': eff_count, 'items': urls_to_log, 'job_id': job_id, 'colle': colle_mode})
                    scan_processed = True
                    
                except Exception as e:
                    logger.error(f"[Scan] Page Error ({url}): {e}")
                finally:
                    # 페이지 자원 해제 (한 줄 주석: 개별 페이지 종료)
                    if page: await page.close()
                    if cross_job_claimed:
                        await release_cross_job_claim(
                            "scan",
                            str(effective_db_name).strip(),
                            str(url or "").strip(),
                            str(job_id or "").strip(),
                            keep_recent=scan_processed,
                        )
                    _task_done_and_decrement(url_item)

            except asyncio.CancelledError:
                try:
                    _scan_debug_print("[Scan] worker cancelled (task)")
                except Exception:
                    pass
                break
            except Exception as e:
                logger.error(f"[Scan] Loop Error: {e}")
                try:
                    _scan_debug_print(f"[Scan] Loop Error (stdout): {e}")
                except Exception:
                    pass
                context = None
                await asyncio.sleep(1)
    finally:
        # 브라우저 컨텍스트 종료 (한 줄 주석: 워커 종료 시 최종 자원 회수)
        if context:
            await context.close()
            logger.info("[Scan] Worker context closed safely.")
