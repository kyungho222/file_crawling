# core/crawler/workers/download.py
"""
파일 다운로드 워커 (실시간 버전)
- Collection 단계에서 넘어온 항목을 즉시 처리
- 병렬 다운로드 수행 (Semaphore)
- HTML 응답 감지 및 차단
- Content-Disposition 파싱
- 상세 로깅
- 견고한 에러 처리

⚠️ 중요: 이 워커는 Playwright를 사용하지 않습니다.
- 탐색(scan)과 수집(collection) 단계: Playwright 사용 가능
- 저장(download) 단계: 일반 HTTP 다운로드만 사용 (aiohttp)
- Collection에서 검증된 URL을 받아서 서버 백엔드에서 직접 HTTP GET 요청으로 다운로드
"""
import asyncio
import aiohttp
import os
import re
import logging
import sys
from typing import List, Dict, Optional, Callable, Awaitable
from playwright.async_api import Browser
import json
import time
import socket

# 프로젝트 루트를 sys.path에 추가 (core/crawler/workers -> ../../../)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from uuid import uuid4
from config.settings import settings, get_uploaded_files_local_dir, normalize_access_url, get_storage_domain_for_db_name
from utils.file import sanitize_filename, make_safe_storage_filename
from core.crawler.batch_queue import BatchQueue
from config.constants import DOC_EXTENSIONS, ARCHIVE_EXTENSIONS, IMG_EXTENSIONS
from utils.web_sync import sync_file_to_webserver
from utils.db_name import resolve_db_name

# 로거 설정
logger = logging.getLogger(__name__)
FLOW_DEBUG = os.getenv("CRAWL_DEBUG_FLOW", "0") == "1"
if FLOW_DEBUG:
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass

# 단계별 URL 리포트(다운로드 폴더 JSON 누적)
try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        try:
            import sys as _sys
            if project_root not in _sys.path:
                _sys.path.insert(0, project_root)
            from backend.shared.stage_url_report import append_stage_urls as _impl  # type: ignore
            return _impl(stage=stage, urls=urls, job_id=job_id, db_name=db_name, output_dir=output_dir, extra_meta=extra_meta, entry_extra=entry_extra)
        except Exception:
            return None

def _env_bool(key: str, default: str = "1") -> bool:
    try:
        return str(os.getenv(key, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return float(default)

DOWNLOAD_DOC_ONLY = _env_bool("DOWNLOAD_DOC_ONLY", "1")
# 다운로드 경로 디버깅 로그 (기본 OFF)
# - 1이면: server_domain/domain/chat_bot_id/db_name 및 계산된 download_dir/filepath를 로그로 출력
DOWNLOAD_PATH_DEBUG = _env_bool("DOWNLOAD_PATH_DEBUG", "0")
# 문서 메타데이터(작성일) 추출 활성화 및 타임아웃
DOCUMENT_META_ENABLED = _env_bool("DOCUMENT_META_ENABLED", "1")
DOCUMENT_META_TIMEOUT_SEC = max(0.1, min(_env_float("DOCUMENT_META_TIMEOUT_SEC", 2.5), 30.0))

async def _extract_doc_created_at_async(filepath: str) -> Optional[str]:
    if not DOCUMENT_META_ENABLED:
        return None
    try:
        from utils.document_meta_date import extract_document_created_at
    except Exception:
        return None
    try:
        loop = asyncio.get_running_loop()
    except Exception:
        return None
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, extract_document_created_at, filepath),
            timeout=DOCUMENT_META_TIMEOUT_SEC,
        )
    except Exception:
        return None

def _short(s: object, n: int = 180) -> str:
    try:
        text = str(s)
    except Exception:
        return ""
    return text if len(text) <= n else (text[:n] + "…")


def _write_bytes(filepath: str, data: bytes) -> None:
    """동기 파일 쓰기 유틸리티: asyncio.to_thread와 함께 사용됩니다."""
    # 디렉토리 존재 보장
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(filepath, "wb") as fh:
        fh.write(data)


async def _sync_after_download_if_needed(file_meta: Dict, filepath: str) -> None:
    # 입력 검사: 메타/경로 또는 sync 플래그가 없으면 동작하지 않음
    if not file_meta or not filepath:
        return
    if not bool(file_meta.get("sync_after_download")):
        # 동기화 플래그가 명시되지 않은 경우 동기화 건너뜀
        logger.debug("[Download][WebSync] skip sync (flag false) | file=%s", _short(filepath, 200))
        return

    try:
        # 접속 base(URL) 결정: 우선 access_url, 없으면 server_domain/doman 사용
        access_url = None
        try:
            access_url = file_meta.get("access_url") or file_meta.get("server_domain") or file_meta.get("domain")
        except Exception:
            access_url = None
        db_name = resolve_db_name(file_meta)
        access_base = normalize_access_url(access_url, db_name)

        # chat_bot_id가 없으면 동기화 불가
        chat_bot_id = file_meta.get("chat_bot_id")
        if not chat_bot_id:
            logger.warning(
                "[Download][WebSync] chat_bot_id missing; skip | url=%s path=%s",
                _short(file_meta.get("url"), 200),
                _short(filepath, 200),
            )
            return

        # 트래킹: 동기화 시작 시각 및 진입로그
        start_t = time.monotonic()
        logger.info(
            "[Download][WebSync] start | url=%s path=%s access_base=%s",
            _short(file_meta.get("url"), 200),
            _short(filepath, 200),
            _short(access_base, 200),
        )
        logger.debug("[Download][WebSync] entry debug | chat_bot_id_tail=%s job_id=%s",
                     str(chat_bot_id).split("-")[-1] if chat_bot_id else None,
                     file_meta.get("job_id"))

        # 실제 동기화 호출 (rsync / local copy / sftp 등 내부 처리)
        ok = await sync_file_to_webserver(
            local_file_path=filepath,
            access_base_url=access_base,
            chat_bot_id=chat_bot_id,
            db_name=db_name,
        )

        # 트래킹: 완료 시간 및 결과 로깅
        dur_ms = int((time.monotonic() - start_t) * 1000)
        logger.info(
            "[Download][WebSync] done | ok=%s url=%s file=%s dur_ms=%d",
            bool(ok),
            _short(file_meta.get("url"), 200),
            os.path.basename(filepath),
            dur_ms,
        )
        logger.debug("[Download][WebSync] done debug | ok=%s dur_ms=%d file_sig=%s",
                     bool(ok), dur_ms, getattr(file_meta.get("original_meta", {}), "attachment_name", None))
    except Exception as exc:
        # 예외 발생 시 경고와 디버그 정보 남김
        logger.warning(
            "[Download][WebSync] failed | url=%s path=%s err=%s",
            _short(file_meta.get("url"), 200),
            _short(filepath, 200),
            _short(exc, 200),
        )
        logger.debug("[Download][WebSync] exception detail", exc_info=True)

_DOC_EXTS = {e.lower() for e in (DOC_EXTENSIONS or [])}
_ARCHIVE_EXTS = {e.lower() for e in (ARCHIVE_EXTENSIONS or [])}
_IMG_EXTS = {e.lower() for e in (IMG_EXTENSIONS or [])}

def _ext_of_name(name: str) -> str:
    try:
        base = os.path.basename(name or "")
    except Exception:
        base = name or ""
    dot = base.rfind(".")
    if dot == -1:
        return ""
    return base[dot:].lower()

def _is_blocked_by_type(filename: str, content_type: str) -> bool:
    """문서류만 허용: zip/이미지/오디오/비디오 등은 차단."""
    if not DOWNLOAD_DOC_ONLY:
        return False
    ct = (content_type or "").lower()
    ext = _ext_of_name(filename)
    # 확장자 기반 차단/허용
    if ext in _ARCHIVE_EXTS:
        return True
    if ext in _IMG_EXTS:
        return True
    if ext and ext not in _DOC_EXTS:
        # 알려진 확장자이지만 문서가 아니면 차단
        return True
    # MIME 기반 차단
    if ct.startswith("image/") or ct.startswith("video/") or ct.startswith("audio/"):
        return True
    if "application/zip" in ct or "application/x-zip" in ct or "application/x-7z" in ct:
        return True
    return False

async def _get_download_dir(file_meta: Dict, default_download_dir: str, download_path_cache: Dict) -> str:
    """
    다운로드 디렉토리 경로를 계산하는 헬퍼 함수
    이제 도메인별 하위 폴더 생성을 지원합니다.
    """
    try:
        chat_bot_id = file_meta.get('chat_bot_id')
        url = file_meta.get('url', '')
        db_name = resolve_db_name(file_meta)

        # ✅ 요청사항: 새 저장 경로는 "접속url/chat/uploaded_files/{uuid_tail12}" 기준
        # - 접속url은 프론트가 전달한 access_url을 우선 사용
        access_url = file_meta.get("access_url")
        access_base = normalize_access_url(access_url, db_name)
        # (fallback/로그용) host는 기존 server_domain도 유지
        domain = get_storage_domain_for_db_name(db_name) if db_name else (file_meta.get('server_domain') or file_meta.get('domain'))
        # 도메인이 없으면 URL에서 추출
        if (not domain or domain == 'unknown') and url:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.split(':')[0]
            if DOWNLOAD_PATH_DEBUG:
                logger.info(
                    "[DOWNLOAD][PathDebug] domain missing -> derived from url | url=%s derived_domain=%s",
                    _short(url, 200),
                    domain,
                )

        # chat_bot_id가 없으면 DB(chatbot_setup)에서 최신 값을 조회하여 보강
        if not chat_bot_id and db_name:
            try:
                from db.mariadb_save_update import get_latest_chat_bot_id_from_chatbot_setup
                chat_bot_id = await get_latest_chat_bot_id_from_chatbot_setup(str(db_name))
                if chat_bot_id:
                    file_meta["chat_bot_id"] = chat_bot_id
                if DOWNLOAD_PATH_DEBUG:
                    logger.info(
                        "[DOWNLOAD][PathDebug] chat_bot_id补完 via chatbot_setup | db=%s chat_bot_id_tail=%s",
                        db_name,
                        (str(chat_bot_id).split("-")[-1] if chat_bot_id else None),
                    )
            except Exception:
                pass

        if chat_bot_id:
            # 캐시 키에 도메인 포함
            cache_key = f"{chat_bot_id}_{access_base}"
            if cache_key in download_path_cache:
                if DOWNLOAD_PATH_DEBUG:
                    logger.info(
                        "[DOWNLOAD][PathDebug] cache hit | db=%s server_domain=%s domain=%s chat_bot_id_tail=%s dir=%s",
                        db_name,
                        file_meta.get("server_domain"),
                        domain,
                        str(chat_bot_id).split("-")[-1] if chat_bot_id else None,
                        download_path_cache[cache_key],
                    )
                return download_path_cache[cache_key]
            
            # backend.shared.config의 중앙 집중식 경로 생성 함수 사용
            download_dir = get_uploaded_files_local_dir(access_base_url=access_base, chat_bot_id=chat_bot_id)
            
            # 디렉토리 생성 (필수)
            os.makedirs(download_dir, exist_ok=True)
            download_path_cache[cache_key] = download_dir
            if DOWNLOAD_PATH_DEBUG:
                logger.info(
                    "[DOWNLOAD][PathDebug] cache miss -> computed dir | db=%s server_domain=%s domain=%s chat_bot_id_tail=%s dir=%s",
                    db_name,
                    file_meta.get("server_domain"),
                    domain,
                    str(chat_bot_id).split("-")[-1] if chat_bot_id else None,
                    download_dir,
                )
            return download_dir


           
        else:
            logger.debug(f"[DOWNLOAD] chat_bot_id 없음, 기본 경로 사용: {default_download_dir}")
            if DOWNLOAD_PATH_DEBUG:
                logger.info(
                    "[DOWNLOAD][PathDebug] chat_bot_id missing -> fallback default dir | db=%s server_domain=%s domain=%s default_dir=%s",
                    db_name,
                    file_meta.get("server_domain"),
                    domain,
                    default_download_dir,
                )
            return default_download_dir
    except Exception as e:
        logger.warning(f"[DOWNLOAD] 동적 경로 생성 실패, 기본 경로 사용: {e}", exc_info=True)
        if DOWNLOAD_PATH_DEBUG:
            logger.info(
                "[DOWNLOAD][PathDebug] exception -> fallback default dir | default_dir=%s err=%s",
                default_download_dir,
                _short(e, 240),
            )
        return default_download_dir

async def _download_with_playwright(browser, file_meta: Dict, download_dir: str, default_download_dir: str, browser_relauncher: Optional[Callable[[], Awaitable[Browser]]] = None, worker_id: Optional[int] = None):    
    """
    Playwright를 사용한 파일 다운로드 (fallback)
    - page.expect_download()와 page.click() 방식 사용
    - page.goto() 대신 링크 클릭 방식으로 다운로드
    - 브라우저 연결 끊김 시 자동 재연결 처리
    """
    wtag = f"[Worker {worker_id}] " if worker_id is not None else ""
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
    from urllib.parse import urlparse, urljoin
    
    url = file_meta['url']
    
    # 1줄 주석: 자바스크립트 함수로 감싸진 경우 정규식을 사용해 실제 다운로드 링크(http...)만 추출하여 url 변수를 덮어씁니다.
    if url.startswith("javascript:"):
        import re
        match = re.search(r"'(https?://[^']+)'", url)
        if match:
            url = match.group(1)
            logger.info(f"[Download] {wtag} javascript 링크에서 실제 URL 추출 성공: {url}")
            
    suggested_name = file_meta.get('name', 'unknown')
    # source_page가 없으면 URL을 대체값으로 사용하여 Referer/로그 등에 활용
    source_page = file_meta.get('source_page') or url

    context = None
    page = None

    def _filename_from_content_disposition(content_disposition: str) -> Optional[str]:
        if not content_disposition:
            return None
        import re
        from urllib.parse import unquote

        # 1. RFC 5987 표준 (filename*) 우선 처리
        match = re.search(r'filename\*=UTF-8\'\'(.+)', content_disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1))

        # 2. 일반 filename= 추출 (유연한 정규식 적용)
        # 따옴표가 있거나(["\']?...["\']?) 없는([^"\';\n]+) 모든 케이스 대응
        match = re.search(r'filename=(?:["\']?([^"\'\n;]+)["\']?|([^"\';\n]+))', content_disposition, re.IGNORECASE)
        if not match:
            return None

        # 그룹 1(따옴표 있음) 또는 그룹 2(따옴표 없음)에서 파일명 획득
        raw_filename = (match.group(1) or match.group(2)).strip()

        try:
            # URL 인코딩(%EB...)이 포함된 경우 해제
            if '%' in raw_filename:
                raw_filename = unquote(raw_filename)

            # Latin-1로 오해된 바이트 데이터를 실제 인코딩으로 재해석 (핵심 복구 로직)
            # 한국 공공기관 환경을 고려하여 UTF-8과 CP949를 순차 시도합니다.
            binary_data = raw_filename.encode('latin-1')
            for encoding in ['utf-8', 'cp949']:
                try:
                    return binary_data.decode(encoding)
                except (UnicodeDecodeError, UnicodeEncodeError):
                    continue
            
            return raw_filename # 모든 시도 실패 시 원본 반환
        except Exception:
            return raw_filename
            
    async def _download_via_context_request() -> Optional[Dict]:
        if not context:
            raise RuntimeError("Playwright context is not available for request fallback")
        try:
            req_timeout_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_REQUEST_TIMEOUT_MS", "180000") or "180000")
        except Exception:
            req_timeout_ms = 180000
        req_timeout_ms = max(5000, min(int(req_timeout_ms), 600000))
        try:
            large_bytes = int(os.getenv("DOWNLOAD_PLAYWRIGHT_LARGE_FILE_BYTES", str(50 * 1024 * 1024)) or str(50 * 1024 * 1024))
        except Exception:
            large_bytes = 50 * 1024 * 1024
        try:
            large_timeout_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_LARGE_FILE_TIMEOUT_MS", "300000") or "300000")
        except Exception:
            large_timeout_ms = 300000
        large_timeout_ms = max(req_timeout_ms, min(int(large_timeout_ms), 900000))

        req_headers: Dict[str, str] = {}
        if source_page:
            req_headers["Referer"] = source_page
            try:
                from urllib.parse import urlparse
                p = urlparse(source_page)
                if p.scheme and p.netloc:
                    req_headers["Origin"] = f"{p.scheme}://{p.netloc}"
            except Exception:
                pass

        # [수정] Playwright 관련 에러 처리를 위해 임포트 확인 (상단에 없을 경우 대비)
        from playwright.async_api import Error as PlaywrightError

        # 대용량 파일은 timeout을 넉넉히 준다 (HEAD로 content-length 확인)
        try:
            # [추가] 실행 전 컨텍스트 유효성 체크
            if not context or not hasattr(context, "request"):
                logger.warning(f"[Download] {wtag} 컨텍스트가 닫혀 HEAD 요청을 건너뜁니다.")
                raise RuntimeError("Context closed")

            head_timeout_ms = min(10000, max(5000, int(req_timeout_ms / 6)))
            head_resp = await context.request.head(url, headers=req_headers, timeout=head_timeout_ms)
            if head_resp and head_resp.ok:
                try:
                    clen = int(head_resp.headers.get("content-length") or "0")
                except Exception:
                    clen = 0
                if clen >= large_bytes:
                    req_timeout_ms = large_timeout_ms
        except Exception:
            pass

        logger.info(
            "[Download] %s[Playwright] request fallback start | url=%s timeout_ms=%s",
            wtag, url, req_timeout_ms,
        )

        # [수정] 본 요청 시 TargetClosedError 예외 처리 추가
        try:
            # 재차 컨텍스트 유효성 확인
            if not context or not hasattr(context, "request"):
                logger.error(f"[Download] {wtag} 컨텍스트가 이미 종료되어 GET 요청을 수행할 수 없습니다.")
                return None

            response = await context.request.get(url, headers=req_headers, timeout=req_timeout_ms)
        except PlaywrightError as e:
            if "Target page, context or browser has been closed" in str(e):
                logger.error(f"[Download] {wtag} 브라우저 컨텍스트 종료됨 (TargetClosedError) | url={url}")
                return None # 안전하게 None 반환하여 상위에서 처리
            raise # 다른 Playwright 에러는 재발생
        if response.status != 200:
            raise RuntimeError(f"Playwright request fallback non-200: {response.status}")

        content_type = (response.headers.get("content-type") or "").lower()
        cd = response.headers.get("content-disposition", "")
        
        # 스트림으로 바로 다운로드되는 경우 content_type을 'file'로 설정
        # - Content-Disposition에 attachment가 있거나
        # - application/octet-stream 또는 binary/octet-stream인 경우
        is_stream_download = False
        if cd:
            cd_lower = cd.lower()
            if 'attachment' in cd_lower or 'filename' in cd_lower:
                is_stream_download = True
        if not is_stream_download:
            if 'application/octet-stream' in content_type or 'binary/octet-stream' in content_type:
                is_stream_download = True
        
        if is_stream_download:
            content_type = 'file'
        
        if DOWNLOAD_DOC_ONLY and (
            content_type.startswith("image/")
            or content_type.startswith("video/")
            or content_type.startswith("audio/")
        ):
            logger.info(
                "[Download] %s[Playwright] Skipped (non-doc mime) | url=%s ct=%s",
                wtag, url, content_type,
            )
            return None

        body = await response.body()
        if not body:
            raise RuntimeError("Playwright request fallback returned empty body")

        head = body[:2048].lstrip().lower()
        if "text/html" in content_type or head.startswith(b"<!doctype html") or b"<html" in head:
            text = body.decode("utf-8", errors="ignore")
            # 1줄 주석: HTML 응답 내에서 파일 확장자나 다운로드 관련 파라미터가 포함된 href 링크를 추출함
            m = re.search(r'href=["\']([^"\']+(?:\.hwp|\.pdf|\.doc|\.xls|sys_file_nm|file_path)[^"\']*)["\']', text, re.I)

            if m:
                real_url = urljoin(url, m.group(1))
                logger.info(f"[Download] {wtag} HTML 내 실제 파일 링크 발견 -> 재시도: {real_url}")
                # 1줄 주석: 추출된 실제 URL로 다시 GET 요청을 보내 파일 데이터를 가져옴
                response = await context.request.get(real_url, headers=req_headers, timeout=req_timeout_ms)
                body = await response.body()
                content_type = response.headers.get("content-type", "")
            else:
                # 1줄 주석: HTML이 반환되었으나 내부에서 파일 링크를 찾지 못한 경우 최종 실패 처리함
                logger.warning(f"[Download] {wtag} HTML 수신됨 (파일 링크 없음). 내용 일부: {text[:200]}")
                raise RuntimeError("Playwright request fallback returned HTML content (No link found)")

        cd = response.headers.get("content-disposition", "")
        final_filename = _filename_from_content_disposition(cd)

        if not final_filename:
            final_filename = suggested_name

        if not final_filename or final_filename == "unknown":
            from uuid import uuid4
            ext = ".bin"
            if ".pdf" in url.lower():
                ext = ".pdf"
            elif ".hwp" in url.lower():
                ext = ".hwp"
            final_filename = f"file_{uuid4().hex[:8]}{ext}"

        # PHP 통일: 디스크에는 md5(subject+time+uniqid).ext, DB subject에는 원본명
        original_subject = sanitize_filename(final_filename) or final_filename
        if _is_blocked_by_type(original_subject, ""):
            logger.info(
                "[Download] %s[Playwright] Skipped (non-doc) | url=%s filename=%s",
                wtag, url, original_subject,
            )
            return None
        storage_filename = make_safe_storage_filename(final_filename)

        final_download_dir = download_dir or default_download_dir
        filepath = os.path.join(final_download_dir, storage_filename)
        try:
            # 1줄 주석: 파일이 실제로 존재하는지 한 번 더 확인하여 삭제 시 발생하는 경로 에러를 방지함
            if os.path.exists(filepath):
                await asyncio.to_thread(os.remove, filepath) # 파일 존재 시 안전하게 삭제 수행
            await asyncio.to_thread(_write_bytes, filepath, body)
            file_size = await asyncio.to_thread(os.path.getsize, filepath)
        except Exception as exc:
            logger.warning(
                "[DOWNLOAD][Error] failed to write file | url=%s filepath=%s err=%s",
                _short(url, 200),
                _short(filepath, 400),
                _short(exc, 300),
            )
            raise

        if file_size == 0:
            raise ValueError("다운로드된 파일 크기가 0바이트입니다")

        # 디버깅: 파일 쓰기 완료 및 존재 여부 확인 로그
        try:
            exists = await asyncio.to_thread(os.path.exists, filepath)
        except Exception:
            exists = False
        logger.info(
            "[DOWNLOAD][DebugSave] file written | url=%s download_dir=%s filename=%s filepath=%s size=%s exists=%s",
            _short(url, 200),
            _short(final_download_dir, 200),
            storage_filename,
            _short(filepath, 400),
            int(file_size) if file_size is not None else None,
            bool(exists),
        )
        print(f"===================== [DOWNLOAD_filesize] : {file_size} ========================)", flush=True)
        return {
            "file_path": filepath,
            "local_path": filepath,
            "url": url,
            "name": original_subject,
            "subject": original_subject,
            "storage_filename": storage_filename,
            "size": file_size,
            "content_type": content_type,
            "original_meta": file_meta,
        }

    async def _safe_query_selector(selector: str):
        """안전하게 요소를 검색하고 브라우저 종료 시 적절한 로그를 남깁니다."""
        try:
            if not page or page.is_closed():
                return None
            return await page.query_selector(selector)
        except PlaywrightError as e:
            error_msg = str(e).lower()

            # 페이지/브라우저가 닫힌 경우 안전하게 None 반환
            if "target closed" in error_msg or "browser has been closed" in error_msg:
                logger.warning(f"[DOWNLOAD] [Playwright] 브라우저/페이지 닫힘 감지 (무시): {e}")
                return None

            # 실행 컨텍스트 파괴 또는 네비게이션으로 인한 실패이면 재시도
            if "execution context was destroyed" in error_msg or "navigation" in error_msg:
                logger.warning(f"[DOWNLOAD] [Playwright] 컨텍스트 파괴/네비게이션 감지, 재시도 수행 | selector={selector}")
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5000)
                    return await page.query_selector(selector)
                except PlaywrightError as retry_err:
                    retry_msg = str(retry_err).lower()
                    # 재시도 중 페이지가 닫히면 None 반환
                    if "target closed" in retry_msg or "browser has been closed" in retry_msg:
                        logger.warning(f"[DOWNLOAD] [Playwright] 링크 재검색 중 페이지가 닫혔습니다. (무시)")
                        return None
                    logger.warning("[DOWNLOAD] [Playwright] 링크 재검색 실패 (무시): %s", retry_err)
                    return None
                except Exception:
                    return None

            # 처리하지 않은 PlaywrightError는 상위로 전파
            raise
    
    try:
        # 브라우저 연결 상태 확인
        if browser and not browser.is_connected():
            logger.warning(f"[DOWNLOAD] [Playwright] 브라우저가 연결되지 않음. 재연결 시도...")
            if browser_relauncher:
                try:
                    browser = await browser_relauncher()
                    logger.info(f"[DOWNLOAD] [Playwright] 브라우저 재연결 성공")
                except Exception as relaunch_err:
                    logger.error(f"[DOWNLOAD] [Playwright] 브라우저 재연결 실패: {relaunch_err}")
                    raise
            else:
                raise RuntimeError("브라우저가 연결되지 않았고 재연결 함수도 없습니다")
        
        # 브라우저 컨텍스트 생성 (다운로드 경로 설정)
        try:
            context = await browser.new_context(
                accept_downloads=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
            logger.debug(f"[DOWNLOAD] [Playwright] 브라우저 컨텍스트 생성 완료")
        except Exception as ctx_err:
            # 1줄 주석: 에러 메시지를 문자열로 변환하여 브라우저 종료 여부를 정확히 판단함
            ctx_error_msg = str(ctx_err).lower()
            if "target closed" in ctx_error_msg or "browser has been closed" in ctx_error_msg:
                logger.warning(f"[DOWNLOAD] [Playwright] 브라우저 종료 감지, 재연결 시도 중...")
                if browser_relauncher:
                    # 1줄 주석: 브라우저를 새로 생성하여 다시 컨텍스트를 시도함
                    browser = await browser_relauncher()
                    context = await browser.new_context(accept_downloads=True)
                else:
                    raise ctx_err
            else:
                raise ctx_err
        
        try:
            page = await context.new_page()
            logger.debug(f"[DOWNLOAD] [Playwright] 페이지 생성 완료")
        except PlaywrightError as page_err:
            error_msg = str(page_err).lower()
            if "target closed" in error_msg or "browser has been closed" in error_msg:
                logger.warning(f"[DOWNLOAD] [Playwright] 페이지 생성 중 브라우저가 닫혔습니다.")
                raise
            else:
                raise
        
        # source_page로 이동
        # - 일부 사이트는 리소스/스크립트 로딩이 느려 domcontentloaded가 오래 걸릴 수 있다.
        # - 이 경우에도 페이지는 "부분 로드"된 상태일 수 있으므로, goto timeout은 치명으로 보지 않고
        #   링크 탐색/다운로드를 계속 시도한다.
        # - ERR_ABORTED / frame detached는 다운로드 트리거/리다이렉트/프레임 교체로 흔히 발생하므로 치명으로 보지 않는다.
        print(f"[Download] {wtag}[Playwright] 소스 페이지로 이동: {source_page}", flush=True)
        try:
            goto_timeout_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_SOURCE_GOTO_TIMEOUT_MS", "120000") or "120000")
        except Exception:
            goto_timeout_ms = 120000
        goto_timeout_ms = max(5000, min(int(goto_timeout_ms), 180000))

        async def _goto_source_page_once(wait_until: str) -> bool:
            try:
                # 1줄 주석: URL이 자바스크립트 호출인 경우 evaluate를 사용하고, 일반 URL이면 goto를 사용함
                if url.startswith("javascript:"):
                    # javascript: 키워드를 제거하고 내부 코드만 실행
                    js_code = url.replace("javascript:", "")
                    await page.evaluate(js_code)
                else:
                    # 일반적인 다운로드 링크인 경우 기존대로 이동 시도
                    await page.goto(url, wait_until="commit", timeout=goto_timeout_ms)
            except PlaywrightError as goto_err:
                msg = str(goto_err).lower()
                # 1줄 주석: 다운로드가 시작되면서 발생하는 중단 에러는 정상적인 흐름으로 간주하여 예외처리함
                if "download is starting" in msg or "net::err_aborted" in msg:
                    logger.debug(f"[DOWNLOAD] [Playwright] 다운로드 트리거 성공 (정상 중단): {url}")
                else:
                    raise
                try:
                    print(f"download goto timeout (continue) | source_page={source_page}", flush=True)
                except Exception:
                    pass
                return False
            except PlaywrightError as goto_err:
                error_msg = str(goto_err).lower()
                # Browser/Target closed => 재시도해도 의미 없으므로 상위에서 처리
                if "target closed" in error_msg or "browser has been closed" in error_msg:
                    logger.warning("[DOWNLOAD] [Playwright] 페이지 이동 중 브라우저가 닫힘 | source_page=%s err=%s", source_page, goto_err)
                    raise
                # 흔한 비치명 네비게이션 중단 케이스: 계속 진행(직접 다운로드 goto/expect_download로 우회 가능)
                if (
                    "net::err_aborted" in error_msg
                    or "err_aborted" in error_msg
                    or "frame was detached" in error_msg
                    or "detached" in error_msg
                    or "navigation" in error_msg and "interrupted" in error_msg
                ):
                    logger.warning("[DOWNLOAD] [Playwright] source_page goto aborted/detached (continue) | source_page=%s err=%s", source_page, goto_err)
                    try:
                        print(f"  download goto aborted/detached (continue) | source_page={source_page} err={goto_err}", flush=True)
                    except Exception:
                        pass
                    return False
                # 그 외는 기존대로 치명 처리(원인 파악 필요)
                raise

        # 2회까지 시도:
        # - 1차: domcontentloaded
        # - 실패 시 2차: commit (더 가벼운 wait_until, 일부 사이트에서 덜 터짐)
        ok = False
        try:
            ok = await _goto_source_page_once("domcontentloaded")
        except Exception:
            raise
        if not ok:
            # frame detached 이후 page가 불안정할 수 있으니 새 페이지로 교체 후 한 번 더 시도
            try:
                try:
                    await page.close()
                except Exception:
                    pass
                page = await context.new_page()
            except Exception:
                # 페이지 교체 실패해도 아래 직접 다운로드 fallback이 동작할 수 있음(페이지가 None은 아님)
                pass
            try:
                await _goto_source_page_once("commit")
            except Exception:
                raise
        
        # 파일 URL을 가리키는 링크 찾기
        # 여러 선택자 시도: href 속성에 file_url이 포함된 링크
        link_selector = f'a[href*="{urlparse(url).path}"]'
        link = None
        link = await _safe_query_selector(link_selector)
        
        if not link:
            # 더 넓은 범위로 검색: href에 파일명이나 다운로드 관련 키워드 포함
            try:
                all_links = await page.query_selector_all('a[href*="download"], a[href*="fileDown"], a[href*="file"]')
                for candidate_link in all_links:
                    try:
                        href = await candidate_link.get_attribute('href')
                        if href and url in urljoin(source_page, href):
                            link = candidate_link
                            break
                    except (PlaywrightError, RuntimeError, AttributeError, ValueError, OSError) as attr_err:
                        error_msg = str(attr_err).lower()
                        if "target closed" in error_msg or "browser has been closed" in error_msg:
                            logger.warning(f"[DOWNLOAD] [Playwright] get_attribute 호출 중 타겟이 닫혔습니다. 건너뜀")
                            continue
                        logger.debug(f"[DOWNLOAD] [Playwright] get_attribute 오류 (무시, 루프 유지): {attr_err}")
                        continue
                    except Exception as attr_err:
                        logger.debug(f"[DOWNLOAD] [Playwright] get_attribute 예외 포괄 (무시, 루프 유지): {type(attr_err).__name__} {attr_err}")
                        continue
            except PlaywrightError as query_err:
                error_msg = str(query_err).lower()
                if "target closed" in error_msg or "browser has been closed" in error_msg:
                    logger.warning(f"[DOWNLOAD] [Playwright] 링크 검색 중 페이지가 닫혔습니다.")
                    raise
                if "execution context was destroyed" in error_msg or "navigation" in error_msg:
                    logger.warning(
                        "[DOWNLOAD] [Playwright] 링크 목록 검색 중 네비게이션 감지 (무시)",
                    )
                    all_links = []
                    # fall through
                else:
                    raise
        
        if not link:
            # 2차 fallback:
            # - source_page에서 링크 탐색이 실패하거나, source_page 로딩이 불완전한 경우가 있다.
            # - fileDown.do 같은 direct download handler는 URL로 직접 이동해도 다운로드가 트리거될 수 있으므로
            #   expect_download + goto(url)로 재시도한다.
            try:
                try:
                    expect_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_EXPECT_TIMEOUT_MS", "120000") or "120000")
                except Exception:
                    expect_ms = 120000

                expect_ms = max(5000, min(int(expect_ms), 300000))
                logger.info(
                    "[Download] %s[Playwright] link not found; trying direct download navigation | url=%s expect_timeout_ms=%s",
                    wtag, url, expect_ms,
                )
                async with page.expect_download(timeout=expect_ms) as download_info:
                    # direct download URL로 goto 시 "Download is starting" 예외 또는 Timeout이 발생할 수 있음.
                    try:
                        await page.goto(url, wait_until="commit", timeout=expect_ms)
                    except PlaywrightTimeoutError as te:
                        # 타임아웃은 네트워크/서버 지연 또는 브라우저 이벤트 미발생 탓일 수 있으므로
                        # 즉시 컨텍스트 request 폴백을 시도해본다.
                        logger.warning(
                            "[Download] %s[Playwright] direct goto timeout -> attempting context.request fallback | url=%s timeout_ms=%s err=%s",
                            wtag, url, expect_ms, _short(te, 200),
                        )
                        # close the expect_download context by cancelling it (context manager will handle)
                        raise te
                    except PlaywrightError as goto_err:
                        msg = str(goto_err).lower()
                        if "download is starting" in str(goto_err).lower():
                            logger.debug(
                                "[DOWNLOAD] [Playwright] direct goto raised 'Download is starting' (treated as success) | url=%s",
                                url,
                            )
                        else:
                            raise

                download = await download_info.value
                # below: reuse existing save logic
            except PlaywrightTimeoutError as direct_timeout:
                logger.warning(
                    "[DOWNLOAD] [Playwright] direct goto timed out; attempting context.request fallback | url=%s err=%s",
                    url,
                    _short(direct_timeout, 240),
                )
                try:
                    fallback_info = await _download_via_context_request()
                    if fallback_info:
                        return fallback_info
                except Exception as fallback_exc:
                    logger.warning(
                        "[DOWNLOAD] [Playwright] context.request fallback failed after timeout | url=%s err=%s",
                        url,
                        _short(fallback_exc, 240),
                        exc_info=True,
                    )
                # if fallback failed, raise a TimeoutError to allow upstream retry logic to handle it
                raise TimeoutError(f"Playwright direct goto timed out and request fallback failed: {url}") from direct_timeout
            except Exception as direct_exc:
                logger.warning(
                    "[DOWNLOAD] [Playwright] direct goto failed; attempting context.request fallback | url=%s err=%s",
                    url,
                    _short(direct_exc, 240),
                )
                # try Playwright context request fallback (keeps cookies/headers)
                try:
                    fallback_info = await _download_via_context_request()
                    if fallback_info:
                        return fallback_info
                except Exception as fallback_exc:
                    logger.warning(
                        "[DOWNLOAD] [Playwright] context.request fallback failed | url=%s err=%s",
                        url,
                        _short(fallback_exc, 240),
                        exc_info=True,
                    )
                # All fallbacks failed; propagate original error
                raise
        
        # 다운로드 대기 및 링크 클릭
        print(f"[Download] {wtag}[Playwright] 다운로드 링크 클릭: {url}", flush=True)
        if "download" not in locals():
            try:
                try:
                    expect_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_EXPECT_TIMEOUT_MS", "60000") or "60000")
                except Exception:
                    expect_ms = 60000
                expect_ms = max(5000, min(int(expect_ms), 180000))
                try:
                    async with page.expect_download(timeout=expect_ms) as download_info:
                        try:
                            await link.click()
                        except PlaywrightError as click_err:
                            error_msg = str(click_err).lower()
                            if "target closed" in error_msg or "browser has been closed" in error_msg:
                                logger.warning(f"[DOWNLOAD] [Playwright] 링크 클릭 중 타겟이 닫혔습니다.")
                                raise
                            else:
                                raise
                    download = await download_info.value
                except PlaywrightTimeoutError:
                    logger.warning(
                        "[Download] %s[Playwright] 링크 클릭 후 다운로드 대기 타임아웃. direct goto 재시도 | url=%s",
                        wtag, url,
                    )
                    # 링크 클릭으로 다운로드 이벤트가 발생하지 않는 사이트를 위한 fallback
                    async with page.expect_download(timeout=expect_ms) as download_info:
                        try:
                            await page.goto(url, wait_until="commit", timeout=expect_ms)
                        except PlaywrightError as goto_err:
                            msg = str(goto_err).lower()
                            if "download is starting" in msg:
                                logger.debug(
                                    "[DOWNLOAD] [Playwright] direct goto raised 'Download is starting' (treated as success) | url=%s",
                                    url,
                                )
                            else:
                                raise
                    download = await download_info.value
                except PlaywrightError as download_err:
                    error_msg = str(download_err).lower()
                    if "target closed" in error_msg or "browser has been closed" in error_msg:
                        logger.warning(f"[DOWNLOAD] [Playwright] 다운로드 대기 중 타겟이 닫혔습니다.")
                        raise
                    else:
                        raise
            except PlaywrightError as download_err:
                error_msg = str(download_err).lower()
                if "target closed" in error_msg or "browser has been closed" in error_msg:
                    logger.warning(f"[DOWNLOAD] [Playwright] 다운로드 대기 중 타겟이 닫혔습니다.")
                    raise
                else:
                    raise
        
        # 다운로드 경로 설정 (기존 로직 재사용)
        # ... (기존 download_dir 설정 로직과 동일)
        final_download_dir = download_dir or default_download_dir
        
        # 파일명 결정
        try:
            suggested_path = download.suggested_filename
        except AttributeError:
            suggested_path = suggested_name
        
        if not suggested_path or suggested_path == 'unknown':
            from uuid import uuid4
            ext = '.bin'
            if '.pdf' in url.lower(): ext = '.pdf'
            elif '.hwp' in url.lower(): ext = '.hwp'
            suggested_path = f"file_{uuid4().hex[:8]}{ext}"
        
        # PHP 통일: 디스크에는 md5(subject+time+uniqid).ext, DB subject에는 원본명
        original_subject = sanitize_filename(suggested_path) or suggested_path
        if _is_blocked_by_type(original_subject, ""):
            logger.info("[Download] %s[Playwright] Skipped (non-doc) | url=%s filename=%s", wtag, url, original_subject)
            return None
        storage_filename = make_safe_storage_filename(suggested_path)
        filepath = os.path.join(final_download_dir, storage_filename)
        
        # 기존 파일 삭제
        if await asyncio.to_thread(os.path.exists, filepath):
            await asyncio.to_thread(os.remove, filepath)
        
        # 다운로드 파일 저장
        # - Download.save_as: canceled 는 (페이지 이동/닫힘/네트워크 등)으로 artifact가 취소될 때 발생할 수 있다.
        # - 가능하면 download.path()로 "완료 대기" 후 save_as를 호출해 취소 확률을 낮춘다.
        try:
            try:
                # path()는 다운로드 완료까지 기다린다. (일부 브라우저/환경에서 None일 수 있어 예외는 무시)
                await asyncio.wait_for(download.path(), timeout=60.0)
            except Exception:
                pass
            await download.save_as(filepath)
        except PlaywrightError as save_err:
            msg = str(save_err).lower()
            if "canceled" in msg or "cancelled" in msg:
                # 상위에서 재시도/로깅 정책을 적용할 수 있도록 명시적인 에러로 래핑
                raise RuntimeError(f"Playwright download canceled: {url}") from save_err
            raise
        
        # 파일 크기 확인
        # - save_as 후에도 파일이 늦게 생성되거나 경로가 누락될 수 있어 존재 여부를 재확인한다.
        file_ready = False
        for _ in range(3):
            if await asyncio.to_thread(os.path.exists, filepath):
                file_ready = True
                break
            await asyncio.sleep(0.2)
        if not file_ready:
            raise RuntimeError(f"Playwright download file missing after save_as: {filepath}")
        try:
            file_size = await asyncio.to_thread(os.path.getsize, filepath)
        except FileNotFoundError as size_err:
            raise RuntimeError(f"Playwright download file missing at getsize: {filepath}") from size_err
        
        if file_size == 0:
            raise ValueError("다운로드된 파일 크기가 0바이트입니다")
        
        source_page = file_meta.get('source_page', 'N/A')
        print(f"[Download] {wtag}[Playwright] ✅ Saved: {storage_filename}", flush=True)
        print(f"[Download] {wtag}[Playwright] 📁 Full path: {filepath}", flush=True)
        print(f"[Download] {wtag}[Playwright] 📊 File size: {file_size} bytes", flush=True)
        print(f"[Download] {wtag}[Playwright] 📄 게시글 URL: {source_page}", flush=True)
        print(f"[Download] {wtag}[Playwright] 🔗 파일 URL: {url}", flush=True)
        logger.info("[Download] %s[Playwright] File saved successfully: %s (size: %s bytes) | 게시글_URL=%s | 파일_URL=%s", wtag, filepath, file_size, source_page, url)
        
        # Playwright 직접 다운로드(expect_download)는 스트림 다운로드로 간주
        return {
            'file_path': filepath,
            'local_path': filepath,
            'url': url,
            'name': original_subject,
            'subject': original_subject,
            'storage_filename': storage_filename,
            'size': file_size,
            'content_type': 'file',
            'original_meta': file_meta
        }
        
    except PlaywrightTimeoutError as timeout_exc:
        logger.warning(
            "[DOWNLOAD] [Playwright] 다운로드 타임아웃, request fallback 시도 | url=%s err=%s",
            url,
            timeout_exc,
        )
        try:
            fallback_info = await _download_via_context_request()
            return fallback_info
        except Exception as fallback_exc:
            logger.warning(
                "[DOWNLOAD] [Playwright] request fallback 실패 | url=%s err=%s",
                url,
                fallback_exc,
                exc_info=True,
            )
            raise TimeoutError(f"Playwright 다운로드 타임아웃: {url}") from timeout_exc
    except PlaywrightError as pw_err:
        error_msg = str(pw_err).lower() # 에러 메시지를 소문자로 변환하여 분석
        # 브라우저가 강제로 닫혔는지 확인하는 조건문
        if "target closed" in error_msg or "browser has been closed" in error_msg:
            logger.warning(f"[DOWNLOAD] [Playwright] TargetClosedError 발생: {pw_err}")
            
            # [수정] (target closed) 라는 영문 키워드를 메시지에 반드시 포함시켜야 합니다.
            # 그래야 상위 download_item의 is_closed 로직이 이를 잡아서 브라우저를 재시작합니다.
            raise RuntimeError(f"브라우저/페이지가 닫혔습니다 (target closed): {url}") from pw_err
        else:
            # 다른 종류의 Playwright 에러는 기존대로 다시 던짐
            raise
    finally:
        # 페이지와 컨텍스트를 각각 독립적으로 닫아 좀비 프로세스를 방지합니다.
        if page:
            try:
                await page.close() # 현재 열린 페이지를 강제로 닫습니다.
            except Exception: 
                pass # 닫기 실패 시 로그를 남기지 않고 다음 단계로 넘어갑니다.
        
        if context:
            try:
                await context.close() # 브라우저 세션(컨텍스트)을 종료합니다.
            except Exception: 
                pass # 종료 실패 시에도 프로세스 방치를 막기 위해 무시합니다.
        
        try:
            if context:
                await context.close()
        except Exception as ctx_close_err:
            logger.debug(f"[DOWNLOAD] [Playwright] 컨텍스트 닫기 중 오류 (무시): {ctx_close_err}")

async def download_worker(
    in_queue: BatchQueue, 
    out_queue: BatchQueue, 
    progress_queue: asyncio.Queue,
    max_concurrent: int = 30,
    browser=None,  # Playwright Browser 인스턴스 (fallback용)
    browser_relauncher: Optional[Callable[[], Awaitable[Browser]]] = None,
    worker_id: int = 0,
):
    """
    Download Worker (Real-time):
    - in_queue (CollectionBatchQueue)에서 아이템을 가져옴
    - Semaphore를 사용하여 병렬 다운로드 수행
    - out_queue (SaveBatchQueue)로 성공한 파일 경로 전달
    - 진행 상황은 progress_queue를 통해 알림
    """
    # 기본 다운로드 디렉토리 (fallback용)
    default_download_dir = str(settings.DOWNLOAD_PATH)
    
    # 다운로드 경로 캐시 (최초 1회만 생성)
    download_path_cache = {}  # key: (db_name, chat_bot_id, domain) -> download_dir
    
    # User-Agent 헤더 추가
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    sem = asyncio.Semaphore(max_concurrent)
    try:
        http_timeout = float(os.getenv("DOWNLOAD_HTTP_TIMEOUT_SEC", "30") or "30")
    except Exception:
        http_timeout = 30.0
    http_timeout = max(5.0, min(http_timeout, 120.0))
    try:
        http_retries = int(os.getenv("DOWNLOAD_HTTP_RETRIES", "2") or "2")
    except Exception:
        http_retries = 2
    http_retries = max(1, min(http_retries, 5))

    async def download_item(session, file_meta: Dict):
        """
        Collection에서 검증된 파일을 다운로드
        """
        nonlocal browser  # 외부 스코프의 browser 변수 업데이트를 위해 필요
        download_dir = None
        
        async with sem:
            url = file_meta.get('url')
            if not url: return None
            try:
                logger.info(
                    "[Download][Worker %s] Start | url=%s name=%s source=%s",
                    worker_id,
                    url,
                    file_meta.get("name"),
                    file_meta.get("source_page"),
                )
                # 다운로드 경로 미리 계산 (HTTP 및 Playwright 모두 사용)
                try:
                    download_dir = await _get_download_dir(file_meta, default_download_dir, download_path_cache)
                except Exception as e:
                    logger.debug(f"[Download] 경로 계산 실패 (기본 경로 사용): {e}")
                    download_dir = default_download_dir

                # 1. HTTP 다운로드 시도
                for attempt in range(1, http_retries + 1):
                    try:
                        # fileDown.do 계열은 Referer/Origin이 없으면 막히는 케이스가 많아서,
                        # 원본 페이지(source_page)를 기반으로 헤더를 보강한다.
                        req_headers = dict(headers)
                        source_page = file_meta.get("source_page")
                        if source_page:
                            req_headers["Referer"] = source_page
                            # Origin은 scheme+host만
                            try:
                                from urllib.parse import urlparse
                                p = urlparse(source_page)
                                if p.scheme and p.netloc:
                                    req_headers["Origin"] = f"{p.scheme}://{p.netloc}"
                            except Exception:
                                pass
                        
                        from urllib.parse import quote
                        safe_url = url
                        try:
                            safe_url = quote(url, safe=":/?=&")
                        except Exception:
                            pass
                        if not safe_url or not safe_url.strip():
                            logger.debug("[Download][Worker %s] empty safe_url skip | url=%s", worker_id, url)
                            break

                        async with session.get(safe_url, timeout=http_timeout, allow_redirects=True, headers=req_headers) as response:
                            if response.status != 200:
                                logger.debug(
                                    "[Download][Worker %s] HTTP non-200 | attempt=%s status=%s url=%s encoded=%s",
                                    worker_id,
                                    attempt,
                                    response.status,
                                    url,
                                    safe_url,
                                )
                                if attempt < http_retries:
                                    continue
                                break
                            
                            # 파일명 및 경로 처리
                            content_type = response.headers.get('content-type', '').lower()
                            cd = response.headers.get('content-disposition', '')

                            match = re.search(r'filename=(?:["\']?([^"\'\n;]+)["\']?|([^"\';\n]+))', cd, re.IGNORECASE)
                        if match:
                            # 그룹 1(따옴표 있음) 또는 그룹 2(따옴표 없음)에서 값 가져오기
                            raw_filename = match.group(1) or match.group(2)
                            raw_filename = raw_filename.strip()

                            # %-인코딩(URL 인코딩) 선처리 (local import to avoid top-level dependency)
                            if '%' in raw_filename:
                                try:
                                    from urllib.parse import unquote
                                    raw_filename = unquote(raw_filename)
                                except Exception:
                                    pass

                            # 기본값은 원본. 이후 Latin-1로 잘못 해석된 바이트를 재해석 시도
                            final_filename = raw_filename
                            try:
                                b = raw_filename.encode('latin-1')
                                try:
                                    final_filename = b.decode('utf-8')
                                except UnicodeDecodeError:
                                    try:
                                        final_filename = b.decode('cp949')
                                    except UnicodeDecodeError:
                                        final_filename = raw_filename
                            except Exception:
                                # 어떤 예외가 와도 원본으로 폴백
                                final_filename = raw_filename
                            
                            # 0) 문서류만 허용 (헤더 기반 1차 차단)
                            # - content-disposition/파일명으로 확장자를 얻기 전에, MIME이 명확히 멀티미디어면 즉시 차단
                            if DOWNLOAD_DOC_ONLY and (content_type.startswith("image/") or content_type.startswith("video/") or content_type.startswith("audio/")):
                                logger.info(
                                    "[Download][Worker %s] Skipped (non-doc mime) | url=%s ct=%s",
                                    worker_id,
                                    url,
                                    content_type,
                                )
                                await progress_queue.put({'type': 'download_skipped', 'url': url, 'reason': 'non_doc_mime', 'content_type': content_type})
                                if FLOW_DEBUG:
                                    logger.info(
                                        "[Flow] download_skipped | url=%s reason=non_doc_mime content_type=%s",
                                        _short(url, 220),
                                        content_type,
                                    )
                                return None

                            content = await response.read()
                            if not content:
                                if attempt < http_retries: 
                                    continue
                                break
                            
                            # HTML 응답은 보통 차단/로그인 페이지이므로 파일로 저장하지 말고 실패 처리
                            head = content[:2048].lstrip().lower() if isinstance(content, (bytes, bytearray)) else b""
                            if "text/html" in content_type or head.startswith(b"<!doctype html") or b"<html" in head:
                                logger.info(
                                    "[Download][Worker %s] HTTP returned HTML; will fallback | attempt=%s url=%s ct=%s",
                                    worker_id,
                                    attempt,
                                    url,
                                    content_type,
                                )
                                if attempt < http_retries:
                                    await asyncio.sleep(0.5)
                                    continue
                                break
                            final_filename = None
                            if cd:
                                import re
                                from urllib.parse import unquote
                                
                                # 1. RFC 5987 (filename*) 처리
                                match = re.search(r'filename\*=UTF-8\'\'(.+)', cd, re.IGNORECASE)
                                if match:
                                    final_filename = unquote(match.group(1))
                                else:
                                    # 2. 일반 filename="..." 처리
                                    match = re.search(r'filename="?([^";]+)"?', cd, re.IGNORECASE)
                                    if match:
                                        raw_filename = match.group(1)
                                        # Mojibake 복구 시도 (Latin-1 -> UTF-8/CP949)
                                        try:
                                            # %-encoding이 되어 있다면 unquote 먼저
                                            if '%' in raw_filename:
                                                raw_filename = unquote(raw_filename)
                                            
                                            # 1. UTF-8 복구 시도
                                            try:
                                                final_filename = raw_filename.encode('latin-1').decode('utf-8')
                                            except (UnicodeEncodeError, UnicodeDecodeError):
                                                # 2. CP949 복구 시도 (한국어 레거시 서버 대응)
                                                try:
                                                    final_filename = raw_filename.encode('latin-1').decode('cp949')
                                                except (UnicodeEncodeError, UnicodeDecodeError):
                                                    final_filename = raw_filename
                                        except Exception:
                                            final_filename = raw_filename
                            
                            if final_filename:
                                logger.debug(f"[Download] 헤더에서 파일명 추출 성공: {final_filename}")
                            else:
                                # 3. URL에서 추출
                                url_path = url.split('?')[0]
                                url_filename = url_path.split('/')[-1]
                                if url_filename and '.' in url_filename: final_filename = url_filename
                            
                            if not final_filename:
                                final_filename = file_meta.get('name', 'unknown')
                            
                            # PHP 통일: 원본명(subject) + 디스크 저장명(md5+ext)
                            original_subject = sanitize_filename(final_filename) or final_filename
                            if '.' not in original_subject:
                                if 'pdf' in content_type: original_subject += '.pdf'
                                elif 'hwp' in content_type: original_subject += '.hwp'
                            subject_with_ext = final_filename if (final_filename and '.' in final_filename) else (original_subject or 'file.bin')
                            storage_filename = make_safe_storage_filename(subject_with_ext)

                            # 1) 문서류만 허용 (파일명/확장자 기반 2차 차단)
                            if _is_blocked_by_type(storage_filename, content_type):
                                logger.info(
                                    "[Download][Worker %s] Skipped (non-doc file) | url=%s filename=%s ct=%s",
                                    worker_id,
                                    url,
                                    storage_filename,
                                    content_type,
                                )
                                await progress_queue.put({'type': 'download_skipped', 'url': url, 'reason': 'non_doc_file', 'filename': storage_filename, 'content_type': content_type})
                                if FLOW_DEBUG:
                                    logger.info(
                                        "[Flow] download_skipped | url=%s reason=non_doc_file name=%s content_type=%s",
                                        _short(url, 220),
                                        storage_filename,
                                        content_type,
                                    )
                                return None
                            
                            filepath = os.path.join(download_dir, storage_filename) # 저장될 전체 경로 생성
                            # 1줄 주석: HTTP 재시도 루프 내에서 기존에 잘못 생성된 파일이 있을 경우만 삭제함
                            if os.path.exists(filepath):
                                os.remove(filepath) # 동기 방식에서도 안전하게 존재 확인 후 삭제
                            
                            if DOWNLOAD_PATH_DEBUG:
                                logger.info(
                                    "[DOWNLOAD][PathDebug] about to write (http) | worker=%s db=%s server_domain=%s domain=%s chat_bot_id_tail=%s dir=%s filename=%s filepath=%s bytes=%s ct=%s",
                                    worker_id,
                                    file_meta.get("db_name"),
                                    file_meta.get("server_domain"),
                                    file_meta.get("domain"),
                                    (str(file_meta.get("chat_bot_id") or "").split("-")[-1] if file_meta.get("chat_bot_id") else None),
                                    download_dir,
                                    storage_filename,
                                    filepath,
                                    len(content) if content is not None else 0,
                                    content_type,
                                )
                            
                            logger.info(
                                "[Download][SaveAttempt][Worker %s] url=%s filename=%s path=%s bytes=%s",
                                worker_id,
                                url,
                                storage_filename,
                                filepath,
                                len(content) if content is not None else 0,
                            )
                            with open(filepath, 'wb') as f:
                                f.write(content)

                            
                            if DOWNLOAD_PATH_DEBUG:
                                try:
                                    size_on_disk = os.path.getsize(filepath)
                                except Exception:
                                    size_on_disk = None
                                logger.info(
                                    "[DOWNLOAD][PathDebug] wrote (http) | worker=%s filepath=%s size_on_disk=%s",
                                    worker_id,
                                    filepath,
                                    size_on_disk,
                                )
                            
                            file_size = len(content)
                            logger.info(
                                "[Download][Worker %s] HTTP saved | url=%s path=%s size=%s content_type=%s",
                                worker_id,
                                url,
                                filepath,
                                file_size,
                                content_type,
                            )

                            # 문서 내부 메타데이터 기반 작성일 추출 (DB content_created_at로 전달)
                            doc_created_at = await _extract_doc_created_at_async(filepath)
                            
                            # 결과 보고
                            if FLOW_DEBUG:
                                logger.info(
                                    "[Flow] saved_local | url=%s path=%s size=%s",
                                    _short(url, 220),
                                    _short(filepath, 220),
                                    file_size,
                                )
                            await progress_queue.put({
                                'type': 'file_saved',
                                'file_info': {
                                    'file_path': filepath,
                                    'local_path': filepath,
                                    'url': url,
                                    'name': original_subject,
                                    'subject': original_subject,
                                    # 프론트에서 전달된 memo 전달
                                    'memo': file_meta.get('memo'),
                                    'storage_filename': storage_filename,
                                    'size': file_size,
                                    'job_id': file_meta.get('job_id'),
                                    # 문서 내부 메타 기반 작성일(가능할 때만)
                                    'file_created_at': doc_created_at,
                                    # DB 저장 단계에서 content_author 복구/매핑에 사용
                                    'author': file_meta.get('author'),
                                    'department': file_meta.get('department'),
                                    'author_kind': file_meta.get('author_kind'),
                                    'author_raw': file_meta.get('author_raw'),
                                    'department_raw': file_meta.get('department_raw'),
                                    'source_page': file_meta.get('source_page'),
                                    'reg_date': file_meta.get('reg_date'),
                                    'original_meta': file_meta,
                                    # study_worker가 중복 학습하지 않도록 플래그 전달(IntegratedWorkflow 등)
                                    'skip_study_worker': bool(file_meta.get('skip_study_worker'))
                                }
                            })
                            # ✅ stage/trace JSON 누적(다운로드 폴더): 로컬 저장 성공
                            try:
                                append_stage_urls(
                                    stage="save",
                                    urls=[{"url": url, "file_path": filepath, "storage_filename": storage_filename, "size": file_size}],
                                    job_id=file_meta.get("job_id"),
                                    db_name=file_meta.get("db_name"),
                                )
                            except Exception:
                                pass
                            logger.info(
                                "[Download][SaveDone] 저장 완료 후 file_saved 이벤트 전송함 (save_count는 워크플로우 DB 저장 후 +1 반영) | worker_id=%s url=%s path=%s",
                                worker_id, _short(url, 200), _short(filepath, 200),
                            )
                            logger.info(
                                "[Download][SaveDone][PathDebug] url=%s local_path=%s exists=%s size=%s",
                                url,
                                filepath,
                                os.path.exists(filepath),
                                file_size,
                            )
                            logger.info(
                                "[Download][SaveDone][Worker %s] url=%s filename=%s path=%s size=%s",
                                worker_id,
                                url,
                                storage_filename,
                                filepath,
                                file_size,
                            )
                            await _sync_after_download_if_needed(file_meta, filepath)
                            return {
                                'file_path': filepath, 
                                'url': url,
                                # 메타데이터 보존(탐색 단계에서 추출된 author/reg_date/source_page 등)
                                'author': file_meta.get('author'),
                                'department': file_meta.get('department'),
                                'author_kind': file_meta.get('author_kind'),
                                'author_raw': file_meta.get('author_raw'),
                                'department_raw': file_meta.get('department_raw'),
                                'reg_date': file_meta.get('reg_date'),
                                'source_page': file_meta.get('source_page'),
                                'original_meta': file_meta,
                                'job_id': file_meta.get('job_id'),
                                'chat_bot_id': file_meta.get('chat_bot_id'),
                                'db_name': file_meta.get('db_name'),
                                'name': original_subject,
                                'subject': original_subject,
                                # 프론트에서 전달된 memo 전달
                                'memo': file_meta.get('memo'),
                                'storage_filename': storage_filename,
                                'size': file_size,
                                'content_type': content_type,
                                'skip_study_worker': bool(file_meta.get('skip_study_worker'))
                            }
                    except Exception:
                        if attempt < http_retries: 
                            await asyncio.sleep(1)
                        continue

                # 2. Playwright Fallback
                if browser or browser_relauncher:
                    logger.info("[Download][Worker %s] HTTP failed; trying Playwright fallback | url=%s", worker_id, url)
                    # [전체 리트라이 루프] 브라우저/페이지 닫힘 오류 대응
                    for p_attempt in range(1, 3):
                        try:
                            # 브라우저 연결 상태 확인 및 필요 시 재실행
                            if (browser is None) or (not browser.is_connected()):
                                logger.warning(f"[Download][Worker {worker_id}] Browser missing or disconnected, relaunching...")
                                if browser_relauncher:
                                    # 1줄 주석: 이전 프로세스 정리를 위해 1초 대기 후 브라우저를 새로 실행함
                                    await asyncio.sleep(1.0) 
                                    browser = await browser_relauncher()
                                    logger.info(f"[Download][Worker {worker_id}] Browser relaunched successfully")
                                else:
                                    logger.error(f"[Download][Worker {worker_id}] No relauncher; stopping Playwright fallback")
                                    break
                                logger.warning(f"[Download][Worker {worker_id}] Browser disconnected, attempting relaunch...")
                                if browser_relauncher:
                                    browser = await browser_relauncher()
                                    logger.info(f"[Download][Worker {worker_id}] Browser relaunched successfully")
                                else:
                                    # relauncher가 없으면 fallback을 포기하고 조용히 종료(HTTP는 이미 실패)
                                    logger.error(
                                        "[Download][Worker %s] Browser disconnected but no relauncher available; skipping Playwright fallback | url=%s",
                                        worker_id,
                                        url,
                                    )
                                    break
                            elif not browser and browser_relauncher:
                                logger.info(f"[Download][Worker {worker_id}] Browser not available, launching via relauncher...")
                                browser = await browser_relauncher()
                            elif not browser and not browser_relauncher:
                                # 방어: 위 조건문으로 들어오긴 어렵지만, 안전하게 처리
                                logger.debug(
                                    "[Download][Worker %s] No browser and no relauncher; skipping Playwright fallback | url=%s",
                                    worker_id,
                                    url,
                                )
                                break

                            file_info = await _download_with_playwright(
                                browser,
                                file_meta,
                                download_dir,
                                default_download_dir,
                                browser_relauncher=browser_relauncher,
                                worker_id=worker_id,
                            )
                            if file_info:
                                # 문서 내부 메타데이터 기반 작성일 추출 (DB content_created_at로 전달)
                                try:
                                    fp = file_info.get("file_path") or file_info.get("local_path")
                                except Exception:
                                    fp = None
                                doc_created_at = None
                                if fp:
                                    doc_created_at = await _extract_doc_created_at_async(fp)
                                if FLOW_DEBUG:
                                    logger.info(
                                        "[Download][Worker %s] [Flow] saved_local | url=%s path=%s size=%s",
                                        worker_id,
                                        _short(url, 220),
                                        _short(file_info.get("file_path"), 220),
                                        file_info.get("size"),
                                    )
                                await progress_queue.put({
                                    'type': 'file_saved',
                                    'file_info': {
                                        **file_info,
                                        'job_id': file_meta.get('job_id'),
                                        'skip_study_worker': bool(file_meta.get('skip_study_worker')),
                                        # 문서 내부 메타 기반 작성일(가능할 때만)
                                        'file_created_at': doc_created_at,
                                        # DB 저장 단계에서 content_author 복구/매핑에 사용
                                        'author': file_meta.get('author'),
                                        'department': file_meta.get('department'),
                                        'author_kind': file_meta.get('author_kind'),
                                        'author_raw': file_meta.get('author_raw'),
                                        'department_raw': file_meta.get('department_raw'),
                                        'source_page': file_meta.get('source_page'),
                                        'reg_date': file_meta.get('reg_date'),
                                        'original_meta': file_meta,
                                    }
                                })
                                # ✅ stage/trace JSON 누적(다운로드 폴더): 로컬 저장 성공(Playwright)
                                try:
                                    append_stage_urls(
                                        stage="save",
                                        urls=[
                                            {
                                                "url": url,
                                                "file_path": file_info.get("file_path") or file_info.get("local_path"),
                                                "storage_filename": file_info.get("storage_filename"),
                                                "size": file_info.get("size"),
                                            }
                                        ],
                                        job_id=file_meta.get("job_id"),
                                        db_name=file_meta.get("db_name"),
                                    )
                                except Exception:
                                    pass
                                logger.info(
                                    "[Download][SaveDone] 저장 완료 후 file_saved 이벤트 전송함 (save_count는 워크플로우 DB 저장 후 +1 반영) | worker_id=%s url=%s path=%s",
                                    worker_id, _short(url, 200), _short(file_info.get("file_path"), 200),
                                )
                                logger.info(
                                    "[Download][Worker %s] Playwright saved | url=%s path=%s size=%s",
                                    worker_id,
                                    url,
                                    file_info.get("file_path"),
                                    file_info.get("size"),
                                )
                                try:
                                    fp = file_info.get("file_path") or file_info.get("local_path")
                                except Exception:
                                    pass
                                await _sync_after_download_if_needed(file_meta, file_info.get("file_path") or file_info.get("local_path"))
                                return {
                                    'file_path': file_info['file_path'], 
                                    'url': url,
                                    # 메타데이터 보존(탐색 단계에서 추출된 author/reg_date/source_page 등)
                                    'author': file_meta.get('author'),
                                    'department': file_meta.get('department'),
                                    'author_kind': file_meta.get('author_kind'),
                                    'author_raw': file_meta.get('author_raw'),
                                    'department_raw': file_meta.get('department_raw'),
                                    'reg_date': file_meta.get('reg_date'),
                                    'source_page': file_meta.get('source_page'),
                                    'original_meta': file_meta,
                                    'job_id': file_meta.get('job_id'),
                                    'chat_bot_id': file_meta.get('chat_bot_id'),
                                    'db_name': file_meta.get('db_name'),
                                    'name': file_info.get('name'),
                                    'content_type': file_info.get('content_type', 'file'),
                                    'skip_study_worker': bool(file_meta.get('skip_study_worker'))
                                }
                            break # 성공/결과없음 시 루프 탈출
                        except Exception as e:
                            from playwright.async_api import Error as PlaywrightError
                            error_msg = str(e).lower()
                            is_closed = any(p in error_msg for p in ["target closed", "browser has been closed", "connection closed"])
                            is_canceled = ("canceled" in error_msg) or ("cancelled" in error_msg)
                            
                            if (is_closed or is_canceled) and p_attempt < 2:
                                if is_canceled:
                                    logger.warning(
                                        "[Download][Worker %s] Download canceled during Playwright fallback; retrying (attempt %s) | url=%s",
                                        worker_id,
                                        p_attempt,
                                        url,
                                    )
                                    await asyncio.sleep(0.5)
                                    continue
                                logger.warning(f"[Download][Worker {worker_id}] Target closed during fallback, retrying (attempt {p_attempt})")
                                if browser_relauncher: # 브라우저 재실행 시도
                                    try: browser = await browser_relauncher()
                                    except: pass
                                continue
                            
                            # 최종 실패 시 로깅
                            if is_closed:
                                logger.error(f"[Download][Worker {worker_id}] Playwright fallback failed (target closed): {e}")
                            elif is_canceled:
                                logger.warning(
                                    "[Download][Worker %s] Playwright fallback failed (download canceled) | url=%s err=%s",
                                    worker_id,
                                    url,
                                    e,
                                )
                            else:
                                logger.error(f"[Download][Worker {worker_id}] Playwright fallback failed: {e}", exc_info=True)
                            break
                
                return None
            finally:
                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'download', 'delta': -1})

    cancelled = False
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                try:
                    batch_items = await in_queue.get()
                    
                    if not batch_items:
                        in_queue.task_done()
                        continue

                    tasks = [download_item(session, item) for item in batch_items]
                    results = await asyncio.gather(*tasks)

                    for res in results:
                        if res and out_queue:
                            await out_queue.put(res)
                    try:
                        ok_count = sum(1 for r in results if r)
                    except Exception:
                        pass
                    in_queue.task_done()
                except asyncio.CancelledError:
                    cancelled = True
                    break
                except Exception as e:
                    logger.error(f"[Download] Error: {e}")
                    in_queue.task_done()
    finally:
        logger.info("[Download][Worker %s] 작업중지 (cancelled=%s)", worker_id, cancelled)
