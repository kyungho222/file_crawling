import os
import pdfplumber
import camelot
import pandas as pd
from db.db_operations import insert_data, delete_data
from config import Config
from langchain.text_splitter import RecursiveCharacterTextSplitter
from utils.embedding_config import get_embedding_model, create_embedding_model
import logging
from logs.logging_util import LoggerSingleton
import logging
import re

# from db.db_redis import job_manager, job_progress_manager
from db.db_job_managers import AsyncJobManager, AsyncJobProgress
import json
from socket_sender import send_message_to_socket
import asyncio
import time
import multiprocessing
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from functools import partial
import threading
from queue import Queue
import sys

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.pdf", level=logging.INFO)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

embedding_model = get_embedding_model()

# ✅ multiprocessing을 활용한 병렬 처리 설정
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_COUNT, 4)  # 코어 수 만큼 워커 생성
PAGE_BATCH_SIZE = 10  # 페이지 배치 크기 (8페이지씩 병렬 처리)
PROGRESS_UPDATE_INTERVAL = 10  # 진행률 업데이트 간격 (페이지 단위)
CONCURRENT_BATCHES = 2  # 동시 처리할 배치 수 

OCR_CONCURRENCY_DEFAULT = 2


def _normalize_optional_timeout_sec(value) -> float | None:
    try:
        timeout_sec = float(value)
    except (TypeError, ValueError):
        return None
    if timeout_sec <= 0:
        return None
    return max(0.1, min(timeout_sec, 24 * 3600.0))


def _pdf_plain_text_budget_exhausted(started_at: float, timeout_sec: float | None) -> bool:
    return timeout_sec is not None and (time.perf_counter() - started_at) >= timeout_sec


def _log_pdf_plain_text_timeout(
    *,
    file_path: str,
    total_pages: int,
    processed_pages: int,
    timeout_sec: float | None,
    started_at: float,
    mode: str,
) -> None:
    if timeout_sec is None:
        return
    logger.warning(
        "[PDFPlainTextTimeout] mode=%s timeout=%ss elapsed=%sms processed_pages=%s total_pages=%s file=%s",
        mode,
        int(timeout_sec),
        int((time.perf_counter() - started_at) * 1000),
        processed_pages,
        total_pages,
        file_path,
    )


def _ocr_concurrency_limit() -> int:
    try:
        value = int(
            os.getenv("OCR_CONCURRENCY", str(OCR_CONCURRENCY_DEFAULT))
            or str(OCR_CONCURRENCY_DEFAULT)
        )
    except Exception:
        value = OCR_CONCURRENCY_DEFAULT
    return max(1, min(value, 16))


def _ocr_slot_wait_seconds() -> float:
    try:
        value = float(os.getenv("OCR_SLOT_WAIT_SEC", "0.25") or "0.25")
    except Exception:
        value = 0.25
    return max(0.05, min(value, 5.0))


def _ocr_slot_stale_seconds() -> float:
    try:
        value = float(os.getenv("OCR_SLOT_STALE_SEC", "1800") or "1800")
    except Exception:
        value = 1800.0
    return max(60.0, min(value, 86400.0))


def _ocr_slot_heartbeat_seconds() -> float:
    try:
        value = float(os.getenv("OCR_SLOT_HEARTBEAT_SEC", "60") or "60")
    except Exception:
        value = 60.0
    return max(5.0, min(value, 3600.0))


def _pdf_embedding_timeout_sec() -> float:
    try:
        value = float(os.getenv("PDF_EMBEDDING_TIMEOUT_SEC", "45") or "45")
    except Exception:
        value = 45.0
    return max(5.0, min(value, 300.0))


def _ocr_slot_root() -> str:
    root = str(os.getenv("OCR_SEMAPHORE_DIR", "") or "").strip()
    if root:
        return root
    return os.path.join(tempfile.gettempdir(), "crawler_web_board11_ocr_slots")


def _cleanup_stale_ocr_slots(slot_dir: str, *, stale_after_sec: float) -> None:
    now = time.time()
    limit = _ocr_concurrency_limit()
    for slot_idx in range(limit):
        slot_path = os.path.join(slot_dir, f"slot_{slot_idx}.lock")
        try:
            mtime = os.path.getmtime(slot_path)
        except FileNotFoundError:
            continue
        except Exception:
            continue
        if now - mtime < stale_after_sec:
            continue
        try:
            os.remove(slot_path)
            logger.warning("[OCRGate] stale slot removed | slot=%s", slot_idx)
        except FileNotFoundError:
            continue
        except Exception:
            continue


@contextmanager
def _acquire_global_ocr_slot(*, file_path: str, page_num: int):
    limit = _ocr_concurrency_limit()
    slot_dir = _ocr_slot_root()
    os.makedirs(slot_dir, exist_ok=True)

    wait_sec = _ocr_slot_wait_seconds()
    stale_after_sec = _ocr_slot_stale_seconds()
    started_at = time.perf_counter()
    warned_wait = False
    acquired_path = None

    while acquired_path is None:
        _cleanup_stale_ocr_slots(slot_dir, stale_after_sec=stale_after_sec)
        for slot_idx in range(limit):
            slot_path = os.path.join(slot_dir, f"slot_{slot_idx}.lock")
            try:
                fd = os.open(slot_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                continue
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(
                        f"pid={os.getpid()}\npage={page_num}\nfile={os.path.basename(file_path or '')}\n"
                    )
            except Exception:
                try:
                    os.close(fd)
                except Exception:
                    pass
                try:
                    os.remove(slot_path)
                except Exception:
                    pass
                raise
            acquired_path = slot_path
            break

        if acquired_path is not None:
            break

        waited_ms = int((time.perf_counter() - started_at) * 1000)
        if not warned_wait and waited_ms >= 1000:
            warned_wait = True
            logger.info(
                "[OCRGate] waiting for slot | limit=%s page=%s file=%s waited_ms=%s",
                limit,
                page_num,
                os.path.basename(file_path or ""),
                waited_ms,
            )
        time.sleep(wait_sec)

    try:
        stop_touch = threading.Event()
        touch_thread = None
        if acquired_path:
            heartbeat_sec = _ocr_slot_heartbeat_seconds()

            def _touch_slot() -> None:
                last_log = 0.0
                while not stop_touch.wait(heartbeat_sec):
                    try:
                        now = time.time()
                        os.utime(acquired_path, (now, now))
                        if now - last_log >= heartbeat_sec:
                            last_log = now
                            logger.info(
                                "[OCRGate] active slot heartbeat | page=%s file=%s slot=%s",
                                page_num,
                                os.path.basename(file_path or ""),
                                os.path.basename(acquired_path),
                            )
                    except FileNotFoundError:
                        logger.warning(
                            "[OCRGate] active slot disappeared while OCR running | page=%s file=%s slot=%s",
                            page_num,
                            os.path.basename(file_path or ""),
                            acquired_path,
                        )
                        break
                    except Exception:
                        logger.debug(
                            "[OCRGate] active slot heartbeat failed | page=%s file=%s slot=%s",
                            page_num,
                            os.path.basename(file_path or ""),
                            acquired_path,
                            exc_info=True,
                        )

            touch_thread = threading.Thread(
                target=_touch_slot,
                name=f"ocr-slot-heartbeat-{page_num}",
                daemon=True,
            )
            touch_thread.start()
        yield
    finally:
        try:
            stop_touch.set()
            if touch_thread is not None:
                touch_thread.join(timeout=1.0)
        except Exception:
            pass
        if acquired_path:
            try:
                os.remove(acquired_path)
            except FileNotFoundError:
                pass
            except Exception:
                logger.warning(
                    "[OCRGate] failed to release slot | page=%s file=%s slot=%s",
                    page_num,
                    os.path.basename(file_path or ""),
                    acquired_path,
                )

def clean_text_for_utf8(text: str) -> str:
    """
    UTF-8 인코딩에 문제가 되는 문자들을 제거하는 함수
    - null 바이트 (0x00) 제거
    - 기타 제어 문자 제거
    """
    if not text:
        return text
    
    # null 바이트와 기타 제어 문자 제거 (0x00-0x1F, 0x7F-0x9F)
    # 단, 탭(0x09), 줄바꿈(0x0A), 캐리지 리턴(0x0D)은 유지
    cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
    
    # UTF-8 인코딩/디코딩으로 한 번 더 정리
    try:
        cleaned = cleaned.encode('utf-8', errors='ignore').decode('utf-8')
    except Exception:
        # 인코딩 실패 시 빈 문자열 반환
        cleaned = ""
    
    return cleaned


def _pdf_debug_preview(text: str, limit: int = 160) -> str:
    if not text:
        return ""
    try:
        compact = " ".join(str(text).split())
    except Exception:
        compact = str(text)
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def _detect_pdf_ocr_failure(text: str) -> tuple[bool, str]:
    try:
        compact = " ".join(str(text or "").split())
    except Exception:
        compact = ""
    lowered = compact.lower()
    if "ocr" not in lowered:
        return False, ""
    code_match = re.search(r"(?<!\d)(429|4\d\d|5\d\d)(?!\d)", compact)
    if code_match:
        return True, f"ocr_status_{code_match.group(1)}"
    if "ocr api" in lowered:
        return True, "ocr_api_failed"
    if "failed" in lowered or "error" in lowered or "exception" in lowered:
        return True, "ocr_failed"
    return False, ""


def _build_pdf_page_text_data_sync(
    file_path: str,
    page_num: int,
    content: str,
    personal_info_filter: str = "N",
) -> tuple[str, bool]:
    """
    process_pdf 워커와 동일: 페이지별 텍스트 추출 → (선택) OCR → DB에 넣는 text_data 문자열.
    임베딩은 포함하지 않는다. 반환: (text_data, used_ocr)
    """
    pages_text = extract_text_with_tables_sync(file_path, page_num)
    page_data = pages_text[0] if pages_text else None

    need_ocr = False
    used_ocr = False
    page_text = ""
    ocr_reason = "direct_text"
    raw_chars = 0

    if not page_data or not page_data["text"].strip():
        need_ocr = True
        ocr_reason = "empty_text"
    else:
        page_text = page_data["text"]
        raw_chars = len(page_text.strip())
        if is_cid_text(page_text):
            logger.info(f"[페이지 {page_num}] CID 텍스트 감지 - OCR 처리 필요")
            need_ocr = True
            ocr_reason = "cid_text"
        elif len(page_text.strip()) <= 50:
            logger.info(f"[페이지 {page_num}] 텍스트 부족 ({len(page_text.strip())}자) - OCR 확인")
            need_ocr = True
            ocr_reason = "short_text"

    logger.info(
        "[PDFDebug][page_decision] file=%s page=%s raw_chars=%s need_ocr=%s reason=%s",
        os.path.basename(file_path or ""),
        page_num,
        raw_chars,
        need_ocr,
        ocr_reason,
    )

    if need_ocr:
        try:
            ocr_text = extract_page_with_ocr_sync(file_path, page_num)
            ocr_failed, ocr_failure_reason = _detect_pdf_ocr_failure(ocr_text)
            if ocr_failed:
                logger.error(
                    "PDF page OCR failed; discard page text | reason=%s file=%s page=%s preview=%s",
                    ocr_failure_reason or "ocr_failed",
                    file_path,
                    page_num,
                    _pdf_debug_preview(ocr_text, limit=500),
                )
                return "", True
            if ocr_text.strip():
                if not is_cid_text(ocr_text):
                    page_text = ocr_text
                    used_ocr = True
                    logger.info(f"[페이지 {page_num}] OCR 처리 성공")
                else:
                    logger.warning(f"[페이지 {page_num}] OCR 결과도 CID 텍스트 포함 - 폰트 문제 의심")
                    page_text = f"[페이지 {page_num}]: 텍스트 추출 실패 (폰트 인코딩 문제)"
                    used_ocr = True
            else:
                if not page_data or not page_data["text"].strip():
                    page_text = f"[페이지 {page_num}]: "
                used_ocr = True
            logger.info(
                "[PDFDebug][ocr_result] file=%s page=%s chars=%s preview=%s",
                os.path.basename(file_path or ""),
                page_num,
                len((ocr_text or "").strip()),
                _pdf_debug_preview(ocr_text),
            )
        except Exception as e:
            logger.error(f"[페이지 {page_num}] OCR 처리 중 오류: {str(e)}")
            if not page_data or not page_data["text"].strip():
                page_text = f"[페이지 {page_num}]: OCR 처리 중 오류가 발생했습니다."
            used_ocr = True
            logger.warning(
                "[PDFDebug][ocr_error] file=%s page=%s err=%s",
                os.path.basename(file_path or ""),
                page_num,
                e,
            )

    page_text = clean_text_for_utf8(page_text)
    text_data = f"[Source: {content}]\n[Page: {page_num}]\n{page_text}"
    text_data = clean_text_for_utf8(text_data)

    if personal_info_filter == "Y":
        logger.info(f"PDF 개별 청크 처리 중 개인정보 필터링 적용: content={content}, chunk={page_num}")
        from utils.dlp_api import check_pii_content

        pii_result = check_pii_content(text_data)
        if pii_result["success"]:
            text_data = pii_result["masked_text"]
            logger.info(f"[PDF 필터링 완료] 필터링 후 텍스트 길이: {len(text_data)}")
            if pii_result["is_sensitive"]:
                logger.info(f"[PDF PII 감지] 페이지 {page_num}에서 개인정보 감지됨")
        else:
            logger.error(f"PII 검사 실패: {pii_result.get('error', 'Unknown error')}")
    else:
        logger.debug(
            f"[PDF 필터링 스킵] personal_info_filter='{personal_info_filter}'이므로 필터링 건너뜀"
        )

    logger.info(
        "[PDFDebug][page_result] file=%s page=%s used_ocr=%s final_chars=%s preview=%s",
        os.path.basename(file_path or ""),
        page_num,
        used_ocr,
        len((page_text or "").strip()),
        _pdf_debug_preview(page_text),
    )

    return text_data, used_ocr


def process_single_page_worker(args):
    """
    multiprocessing을 위한 단일 페이지 처리 워커 함수
    """
    try:
        page_num, file_path, content, memo, content_type, total_pages, personal_info_filter = args

        logger.debug(f"[PDF 워커 디버깅] personal_info_filter 값: '{personal_info_filter}'")

        text_data, used_ocr = _build_pdf_page_text_data_sync(
            file_path, page_num, content, personal_info_filter
        )

        # 3. 임베딩 생성
        # Embedding is generated by the parent async process with a timeout.
        embedding = None
        
        # 4. 결과 데이터 구성
        result_data = {
            "content": content,
            "chunk_num": "1",
            "page_num": str(page_num),
            "memo": memo,
            "content_type": content_type,
            "text_data": text_data,
        }
        
        return {
            "status": "success",
            "page_num": page_num,
            "data": result_data,
            "used_ocr": used_ocr
        }
        
    except Exception as e:
        return {
            "status": "error",
            "page_num": page_num,
            "error": str(e)
        }

def extract_text_with_tables_sync(pdf_file, page_num=None):
    """
    PDF 파일에서 텍스트를 추출하는 동기 함수 (multiprocessing용)
    """
    pages_text = []
    logger.info(
        "[PDFDebug][extract_text_with_tables.start] file=%s page_num=%s exists=%s",
        os.path.basename(pdf_file or ""),
        page_num if page_num is not None else "all",
        os.path.isfile(pdf_file),
    )
    
    with pdfplumber.open(pdf_file) as pdf:
        if page_num is not None:
            # 단일 페이지 처리
            if 1 <= page_num <= len(pdf.pages):
                page = pdf.pages[page_num - 1]
                page_text = page.extract_text() or ""
                
                # UTF-8 인코딩 처리 및 null 바이트 제거
                page_text = clean_text_for_utf8(page_text)
                
                pages_text.append({
                    "page_num": page_num,
                    "text": page_text.strip(),
                    "tables": [],
                })
                logger.info(
                    "[PDFDebug][extract_text_with_tables.page] file=%s page=%s chars=%s preview=%s",
                    os.path.basename(pdf_file or ""),
                    page_num,
                    len(page_text.strip()),
                    _pdf_debug_preview(page_text),
                )
        else:
            # 전체 페이지 처리
            for page_num, page in enumerate(pdf.pages, 1):
                page_text = page.extract_text() or ""
                
                # UTF-8 인코딩 처리 및 null 바이트 제거
                page_text = clean_text_for_utf8(page_text)
                
                pages_text.append({
                    "page_num": page_num,
                    "text": page_text.strip(),
                    "tables": [],
                })
    logger.info(
        "[PDFDebug][extract_text_with_tables.done] file=%s page_num=%s pages=%s",
        os.path.basename(pdf_file or ""),
        page_num if page_num is not None else "all",
        len(pages_text),
    )
    
    return pages_text


def extract_pdf_plain_text_like_process_pdf_sync(
    file_path: str,
    content: str | None = None,
    personal_info_filter: str = "N",
    timeout_sec: float | None = None,
    fail_on_timeout: bool = False,
) -> str:
    """
    process_pdf(학습 워커)와 동일한 규칙으로 페이지별 text_data를 만든 뒤 이어붙인 평문.
    멀티프로세싱·임베딩·DB 저장은 하지 않는다.
    """
    if not file_path or not os.path.isfile(file_path):
        return ""
    label = (content or "").strip() or os.path.basename(file_path)
    timeout_sec = _normalize_optional_timeout_sec(timeout_sec)
    started_at = time.perf_counter()
    try:
        file_size = os.path.getsize(file_path)
    except Exception:
        file_size = 0
    logger.info(
        "[PDFDebug][plain_text.start] file=%s size=%s label=%s timeout=%s fail_on_timeout=%s",
        os.path.basename(file_path or ""),
        file_size,
        label[:120],
        timeout_sec,
        fail_on_timeout,
    )
    try:
        with pdfplumber.open(file_path) as pdf:
            n = len(pdf.pages)
        parts: list[str] = []
        for page_num in range(1, n + 1):
            if _pdf_plain_text_budget_exhausted(started_at, timeout_sec):
                _log_pdf_plain_text_timeout(
                    file_path=file_path,
                    total_pages=n,
                    processed_pages=page_num - 1,
                    timeout_sec=timeout_sec,
                    started_at=started_at,
                    mode="pdfplumber",
                )
                if fail_on_timeout:
                    return ""
                break
            text_data, _used = _build_pdf_page_text_data_sync(
                file_path, page_num, label, personal_info_filter
            )
            if text_data.strip():
                parts.append(text_data.strip())
        result = "\n\n".join(parts)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        ocr_failed, ocr_reason = _detect_pdf_ocr_failure(result)
        if ocr_failed:
            logger.error(
                "PDF plain text OCR error; fail extraction | reason=%s file=%s path=%s size=%s total_pages=%s built_pages=%s chars=%s elapsed_ms=%s preview=%s",
                ocr_reason or "ocr_failed",
                os.path.basename(file_path or ""),
                file_path,
                file_size,
                n,
                len(parts),
                len(result.strip()),
                elapsed_ms,
                _pdf_debug_preview(result, limit=600),
            )
            return ""
        logger.debug(
            "[PDFDebug][plain_text.done] file=%s total_pages=%s built_pages=%s chars=%s elapsed_ms=%s preview=%s",
            os.path.basename(file_path or ""),
            n,
            len(parts),
            len(result.strip()),
            elapsed_ms,
            _pdf_debug_preview(result),
        )
        return result
    except Exception as ex:
        logger.warning(
            "[PDFFallback] pdfplumber plain-text extract failed; fallback to pymupdf | file=%s err=%s",
            file_path,
            ex,
        )
        try:
            return _extract_pdf_plain_text_like_pymupdf_sync(
                file_path=file_path,
                content=label,
                personal_info_filter=personal_info_filter,
                timeout_sec=timeout_sec,
                fail_on_timeout=fail_on_timeout,
            )
        except Exception as fallback_ex:
            logger.error(
                "[PDFFallback] pymupdf plain-text extract failed | file=%s err=%s",
                file_path,
                fallback_ex,
            )
            return ""


def _extract_pdf_plain_text_like_pymupdf_sync(
    *,
    file_path: str,
    content: str,
    personal_info_filter: str = "N",
    timeout_sec: float | None = None,
    fail_on_timeout: bool = False,
) -> str:
    import pymupdf

    timeout_sec = _normalize_optional_timeout_sec(timeout_sec)
    started_at = time.perf_counter()

    def _apply_personal_info_filter(text_data: str, page_num: int) -> str:
        if personal_info_filter != "Y":
            return text_data
        try:
            from utils.dlp_api import check_pii_content

            pii_result = check_pii_content(text_data)
            if pii_result.get("success"):
                masked = pii_result.get("masked_text") or text_data
                if pii_result.get("is_sensitive"):
                    logger.info("[PDFFallback] PII detected on page %s", page_num)
                return masked
        except Exception as pii_ex:
            logger.error("[PDFFallback] PII filter failed on page %s | err=%s", page_num, pii_ex)
        return text_data

    parts: list[str] = []
    with pymupdf.open(file_path) as pdf:
        for index in range(len(pdf)):
            if _pdf_plain_text_budget_exhausted(started_at, timeout_sec):
                _log_pdf_plain_text_timeout(
                    file_path=file_path,
                    total_pages=len(pdf),
                    processed_pages=index,
                    timeout_sec=timeout_sec,
                    started_at=started_at,
                    mode="pymupdf",
                )
                if fail_on_timeout:
                    return ""
                break
            page_num = index + 1
            page = pdf.load_page(index)
            page_text = clean_text_for_utf8(page.get_text("text") or "")
            text_data = f"[Source: {content}]\n[Page: {page_num}]\n{page_text}"
            text_data = clean_text_for_utf8(text_data)
            text_data = _apply_personal_info_filter(text_data, page_num)
            if text_data.strip():
                parts.append(text_data.strip())
    return "\n\n".join(parts)


def extract_pdf_plain_text_for_crawl(file_path: str) -> str:
    """게시판 첨부/미리보기: learn_modules의 process_pdf와 같은 본문(OCR·text_data 형식)."""
    return extract_pdf_plain_text_like_process_pdf_sync(file_path, None, "N")


def _pdf_plain_text_subprocess_entry() -> None:
    args = sys.argv[1:]
    if len(args) != 6:
        raise SystemExit(2)

    file_path, content, personal_info_filter, timeout_arg, fail_on_timeout_arg, output_path = args
    timeout_sec = _normalize_optional_timeout_sec(timeout_arg)
    fail_on_timeout = str(fail_on_timeout_arg).strip() == "1"
    text = extract_pdf_plain_text_like_process_pdf_sync(
        file_path,
        content or None,
        personal_info_filter=personal_info_filter or "N",
        timeout_sec=timeout_sec,
        fail_on_timeout=fail_on_timeout,
    )
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(text or "")


async def extract_pdf_plain_text_like_process_pdf_async(
    file_path: str,
    content: str | None = None,
    personal_info_filter: str = "N",
    timeout_sec: float | None = None,
    fail_on_timeout: bool = False,
) -> str:
    """
    PDF 평문 추출을 별도 파이썬 프로세스로 실행한다.
    메인 서버 프로세스가 pdfplumber/OCR 동기 작업에 붙잡히지 않도록 하기 위한 안전 래퍼다.
    """
    if not file_path or not os.path.isfile(file_path):
        return ""

    label = (content or "").strip() or os.path.basename(file_path)
    timeout_sec = _normalize_optional_timeout_sec(timeout_sec)
    started_at = time.perf_counter()
    output_path = ""
    try:
        file_size = os.path.getsize(file_path)
    except Exception:
        file_size = 0
    logger.debug(
        "[PDFDebug][plain_text_async.start] file=%s size=%s label=%s timeout=%s fail_on_timeout=%s",
        os.path.basename(file_path or ""),
        file_size,
        label[:120],
        timeout_sec,
        fail_on_timeout,
    )

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".txt") as tmp:
        output_path = tmp.name

    code = "from edu.pdf_edu import _pdf_plain_text_subprocess_entry; _pdf_plain_text_subprocess_entry()"
    cmd = [
        sys.executable,
        "-c",
        code,
        file_path,
        label,
        personal_info_filter or "N",
        "" if timeout_sec is None else str(timeout_sec),
        "1" if fail_on_timeout else "0",
        output_path,
    ]
    child_env = os.environ.copy()
    existing_pythonpath = str(child_env.get("PYTHONPATH") or "").strip()
    child_env["PYTHONPATH"] = (
        PROJECT_ROOT
        if not existing_pythonpath
        else PROJECT_ROOT + os.pathsep + existing_pythonpath
    )

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=PROJECT_ROOT,
            env=child_env,
        )
        wait_timeout = None
        if timeout_sec is not None:
            wait_timeout = timeout_sec + min(5.0, max(1.0, timeout_sec * 0.05))

        try:
            if wait_timeout is None:
                _stdout, stderr = await proc.communicate()
            else:
                _stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=wait_timeout)
        except asyncio.TimeoutError:
            if proc.returncode is None:
                proc.kill()
                await proc.communicate()
            logger.error(
                "[PDFPlainTextSubprocessTimeout] timeout=%ss elapsed=%sms file=%s",
                int(wait_timeout or 0),
                int((time.perf_counter() - started_at) * 1000),
                file_path,
            )
            if fail_on_timeout:
                raise TimeoutError(f"pdf_plain_text_subprocess_timeout:{int(wait_timeout or 0)}s")
            return ""
        except asyncio.CancelledError:
            if proc.returncode is None:
                proc.kill()
                await proc.communicate()
            logger.info(
                "[PDFPlainTextSubprocessCancelled] elapsed=%sms file=%s",
                int((time.perf_counter() - started_at) * 1000),
                file_path,
            )
            raise

        if proc.returncode != 0:
            err_text = (stderr or b"").decode("utf-8", errors="replace").strip()
            logger.error(
                "[PDFPlainTextSubprocessFailed] code=%s elapsed=%sms file=%s err=%s",
                proc.returncode,
                int((time.perf_counter() - started_at) * 1000),
                file_path,
                err_text[:500],
            )
            return ""

        try:
            with open(output_path, "r", encoding="utf-8") as handle:
                result = handle.read()
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            stripped_result = (result or "").strip()
            ocr_failed, ocr_reason = _detect_pdf_ocr_failure(stripped_result)
            if ocr_failed:
                logger.error(
                    "PDF plain text OCR error; fail extraction | reason=%s file=%s path=%s size=%s chars=%s elapsed_ms=%s timeout=%s fail_on_timeout=%s preview=%s",
                    ocr_reason or "ocr_failed",
                    os.path.basename(file_path or ""),
                    file_path,
                    file_size,
                    len(stripped_result),
                    elapsed_ms,
                    timeout_sec,
                    fail_on_timeout,
                    _pdf_debug_preview(stripped_result, limit=600),
                )
                return ""
            else:
                logger.debug(
                    "[PDFDebug][plain_text_async.done] file=%s chars=%s elapsed_ms=%s preview=%s",
                    os.path.basename(file_path or ""),
                    len(stripped_result),
                    elapsed_ms,
                    _pdf_debug_preview(result),
                )
            return result
        except FileNotFoundError:
            logger.error("[PDFPlainTextSubprocessMissingOutput] file=%s", file_path)
            return ""
    finally:
        if output_path:
            try:
                os.remove(output_path)
            except FileNotFoundError:
                pass
            except Exception:
                logger.debug(
                    "[PDFPlainTextSubprocessCleanupFailed] file=%s output=%s",
                    file_path,
                    output_path,
                    exc_info=True,
                )


def extract_page_with_ocr_sync(file_path: str, page_num: int) -> str:
    """
    단일 페이지에 대해 OCR 처리를 수행하는 동기 함수 (multiprocessing용)
    """
    try:
        import os
        import pymupdf  # fitz
        import requests

        # 단일 페이지 PDF → 메모리 바이트 (Windows에서 Temp 파일 삭제 Permission denied 방지)
        doc = pymupdf.open(file_path)
        page_doc = pymupdf.open()
        page_doc.insert_pdf(doc, from_page=page_num - 1, to_page=page_num - 1)
        try:
            pdf_bytes = page_doc.tobytes()
        finally:
            page_doc.close()
            doc.close()

        UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")
        if not UPSTAGE_API_KEY:
            return "OCR API 키가 설정되지 않았습니다."

        headers = {"Authorization": f"Bearer {UPSTAGE_API_KEY}"}
        config = {"ocr": True, "coordinates": True, "output_formats": "['markdown']"}
        files = {
            "document": (
                f"page_{page_num}.pdf",
                pdf_bytes,
                "application/pdf",
            )
        }
        with _acquire_global_ocr_slot(file_path=file_path, page_num=page_num):
            response = requests.post(
                "https://api.upstage.ai/v1/document-ai/document-parse",
                headers=headers,
                data=config,
                files=files,
                timeout=30,
            )

        if response.status_code == 200:
            ocr_data = response.json()
            text = ocr_data.get("content", {}).get("markdown", "")
            return text if text.strip() else ""
        return f"OCR API 호출 실패: 상태코드 {response.status_code}"

    except Exception as e:
        return f"OCR 처리 오류: {str(e)}"

async def extract_text_with_tables(pdf_file, page_num=None):
    """
    PDF 파일에서 텍스트를 추출합니다 (async 버전, 호환성을 위해 유지)
    """
    return await asyncio.to_thread(extract_text_with_tables_sync, pdf_file, page_num)

async def process_all_pages_parallel_pdf(
    file_path: str,
    total_pages: int,
    content: str,
    memo: str,
    content_type: str,
    table_name: str,
    dbname: str,
    job_id: str,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    page_progress: float,
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
):
    """모든 PDF 페이지를 multiprocessing 배치별로 병렬 처리합니다 (페이지별 자동 OCR 판단)."""
    
    logger.info(f"[최적화된 multiprocessing] PDF {total_pages}개 페이지를 {PAGE_BATCH_SIZE}개씩 처리 시작 (고속 모드)")
    logger.info(f"[성능 설정] 워커: {MAX_WORKERS}개, CPU: {CPU_COUNT}코어, 동시 배치: {CONCURRENT_BATCHES}개")
    
    # 페이지를 배치로 나누기
    page_batches = []
    for i in range(1, total_pages + 1, PAGE_BATCH_SIZE):
        batch_end = min(i + PAGE_BATCH_SIZE - 1, total_pages)
        page_batch = list(range(i, batch_end + 1))
        page_batches.append(page_batch)
    
    total_batches = len(page_batches)
    logger.info(f"[multiprocessing 배치 구성] {total_batches}개 배치, 배치당 최대 {PAGE_BATCH_SIZE}개 페이지")
    
    # 배치들을 CONCURRENT_BATCHES만큼 병렬로 처리 (추가 50% 성능 향상)
    total_processed_pages = 0
    
    # 배치를 CONCURRENT_BATCHES 크기로 그룹화
    batch_groups = []
    for i in range(0, len(page_batches), CONCURRENT_BATCHES):
        batch_group = page_batches[i:i + CONCURRENT_BATCHES]
        batch_groups.append(batch_group)
    
    logger.info(f"[배치 병렬 처리] {len(batch_groups)}개 그룹으로 나누어 {CONCURRENT_BATCHES}개 배치씩 동시 처리")
    
    for group_idx, batch_group in enumerate(batch_groups, 1):
        # 작업 취소 확인
        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"PDF multiprocessing 배치 처리 중 작업 취소됨: job_id={job_id}")
            return {"status": "cancelled", "processed_pages": total_processed_pages}
        
        # 그룹 내 배치들을 병렬로 처리
        batch_tasks = []
        for local_idx, page_batch in enumerate(batch_group):
            global_batch_idx = (group_idx - 1) * CONCURRENT_BATCHES + local_idx + 1
            task = process_page_batch_parallel(
                file_path, page_batch, content, memo, content_type, table_name, dbname, job_id,
                page_progress, job_manager, job_progress_manager, global_batch_idx, total_batches, total_pages, personal_info_filter
            )
            batch_tasks.append(task)
        
        # 배치 그룹 병렬 실행
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # 결과 처리
        for batch_idx, result in enumerate(batch_results, (group_idx - 1) * CONCURRENT_BATCHES + 1):
            if isinstance(result, Exception):
                logger.error(f"배치 {batch_idx} 처리 중 예외: {str(result)}")
                continue
            
            if result and result.get("status") == "cancelled":
                return {"status": "cancelled", "processed_pages": total_processed_pages}
            elif result and result.get("status") == "error":
                logger.error(f"배치 {batch_idx} 처리 중 오류: {result.get('error', 'Unknown error')}")
                # 오류가 발생해도 계속 진행
            elif result:
                total_processed_pages += result.get("processed", 0)
    
    logger.info(f"[고속 multiprocessing 완료] 총 {total_processed_pages}/{total_pages}개 페이지 처리 완료 (최적화 모드)")
    
    return {
        "status": "success",
        "processed_pages": total_processed_pages,
        "total_pages": total_pages
    }

async def process_page_batch_parallel(
    file_path: str,
    page_batch: list,
    content: str,
    content_type: str,
    memo: str,
    table_name: str,
    dbname: str,
    job_id: str,
    page_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    batch_idx: int,
    total_batches: int,
    total_pages: int,
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
):
    """페이지 배치를 multiprocessing으로 병렬 처리합니다 (페이지별 자동 OCR 판단)."""
    batch_start_time = time.time()
    
    logger.info(f"[고속 배치 {batch_idx}/{total_batches}] {len(page_batch)}개 페이지 처리 시작 ({MAX_WORKERS}개 워커)")
    
    try:
        # multiprocessing 인자 준비
        worker_args = []
        for page_num in page_batch:
            args = (page_num, file_path, content, content_type, memo, total_pages, personal_info_filter)
            worker_args.append(args)
        
        # ProcessPoolExecutor를 사용하여 병렬 처리
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 작업 제출
            future_to_page = {
                executor.submit(process_single_page_worker, args): args[0] 
                for args in worker_args
            }
            
            # 결과 수집
            results = []
            successful_pages = 0
            cancelled_pages = 0
            ocr_pages = 0
            
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                
                try:
                    # 작업 취소 확인
                    status = await job_manager.get_job_status(job_id)
                    if status == "cancel":
                        logger.info(f"페이지 배치 처리 중 작업 취소됨: job_id={job_id}")
                        executor.shutdown(wait=False)
                        return {"status": "cancelled", "processed": successful_pages}
                    
                    result = future.result()
                    results.append(result)
                    
                    if result["status"] == "success":
                        # 데이터베이스 저장 전 UTF-8 인코딩 검증
                        try:
                            # 저장할 데이터의 모든 텍스트 필드에서 null 바이트 제거
                            cleaned_data = {}
                            for key, value in result["data"].items():
                                if isinstance(value, str):
                                    cleaned_data[key] = clean_text_for_utf8(value)
                                else:
                                    cleaned_data[key] = value
                            
                            # 데이터베이스 저장 (메인 프로세스에서 처리)
                            try:
                                embedding = await asyncio.wait_for(
                                    embedding_model.aembed_query(cleaned_data["text_data"]),
                                    timeout=_pdf_embedding_timeout_sec(),
                                )
                                cleaned_data["embedding"] = f"[{','.join(map(str, embedding))}]"
                            except asyncio.TimeoutError:
                                logger.error(
                                    "[PDF embedding timeout] page=%s timeout=%ss content=%s",
                                    page_num,
                                    int(_pdf_embedding_timeout_sec()),
                                    content,
                                )
                                continue
                            await insert_data(
                                table=table_name,
                                data=cleaned_data,
                                dbname=dbname,
                            )
                            successful_pages += 1
                        except Exception as db_error:
                            logger.error(f"페이지 {page_num} 데이터베이스 저장 실패: {str(db_error)}")
                            # UTF-8 인코딩 오류인 경우 해당 페이지 건너뛰기
                            if "invalid byte sequence for encoding" in str(db_error):
                                logger.warning(f"페이지 {page_num} UTF-8 인코딩 오류로 건너뛰기")
                            else:
                                raise db_error
                        
                        # OCR 사용 여부 집계
                        if result.get("used_ocr", False):
                            ocr_pages += 1
                        
                        # 개별 페이지 완료 로그
                        logger.debug(f"[페이지 완료] {page_num}/{total_pages} 페이지 처리 완료")
                        
                    elif result["status"] == "error":
                        logger.warning(f"페이지 {page_num} 처리 실패: {result['error']}")
                        
                except Exception as e:
                    logger.error(f"페이지 {page_num} 처리 중 예외 발생: {str(e)}")
        
        # 진행률 업데이트 (페이지 단위로)
        if successful_pages > 0:
            progress_increment = page_progress * successful_pages
            current_progress = await job_progress_manager.get_job_progress(job_id)
            new_progress = round(min(current_progress + progress_increment, 99.99), 2)
            await job_progress_manager.set_job_progress(job_id, new_progress)
            await send_message_to_socket(
                job_id,
                {"status": "in_progress", "progress": new_progress},
                job_manager,
            )

        batch_time = round(time.time() - batch_start_time, 2)
        pages_per_second = round(successful_pages / batch_time, 1)
        logger.info(f"[고속 배치 {batch_idx}/{total_batches}] 완료: {successful_pages}/{len(page_batch)}개 성공 (OCR: {ocr_pages}개, {batch_time}초, {pages_per_second}페이지/초)")
        
        return {"status": "success", "processed": successful_pages}
        
    except Exception as e:
        logger.error(f"배치 {batch_idx} multiprocessing 처리 중 오류: {str(e)}")
        return {"status": "error", "processed": 0, "error": str(e)}

# ❌ DEPRECATED: process_pdf_with_ocr_fallback 함수는 더 이상 사용하지 않습니다.
# 이 함수는 전체 PDF를 upstage_parser_v2로 처리하는데, 파일이 50MB를 넘을 수 있어 
# Upstage API 413 오류를 발생시킵니다.
# 대신 process_pdf 함수를 사용하세요 (페이지별 개별 OCR 처리)
async def process_pdf_with_ocr_fallback_DEPRECATED(
    content: str,
    file_path: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    memo: str = "",
):
    """
    ❌ DEPRECATED: 이 함수는 더 이상 사용하지 않습니다.
    전체 PDF를 upstage_parser_v2로 처리하는데, 파일이 50MB를 넘을 수 있어 
    Upstage API 413 오류를 발생시킵니다.
    
    대신 process_pdf() 함수를 사용하세요 (페이지별 개별 OCR 처리)
    """
    error_msg = f"process_pdf_with_ocr_fallback는 더 이상 사용되지 않습니다. process_pdf를 사용하세요: {content}"
    logger.error(error_msg)
    await send_message_to_socket(
        job_id, {"status": "error", "message": error_msg}, job_manager
    )
    raise RuntimeError(error_msg)

async def process_pdf(
    content: str,
    file_path: str,
    content_type: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    memo: str = "",
    personal_info_filter: str = "N",  # 개인정보 필터링 옵션 추가
):
    """
    PDF 파일을 페이지 기반으로 처리합니다.
    1페이지 = 1청크로 처리하며, 텍스트가 거의 없는 PDF는 OCR 처리로 자동 전환합니다.
    """
    try:
        # 페이지별 진행률 계산
        started_at = time.perf_counter()
        page_progress = each_progress
        try:
            file_size = os.path.getsize(file_path)
        except Exception:
            file_size = 0

        # 1. PDF 파일 정보 확인
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            
            logger.info(f"PDF 처리 시작: {content}, 총 페이지: {total_pages} (페이지별 자동 OCR 판단)")
            logger.info(
                "[PDFDebug][process_pdf.start] job_id=%s file=%s size=%s total_pages=%s content_type=%s personal_info_filter=%s",
                job_id,
                os.path.basename(file_path or ""),
                file_size,
                total_pages,
                content_type,
                personal_info_filter,
            )

        # 2. 텍스트가 없는 경우 기본 처리
        if total_pages == 0:
            logger.warning(f"PDF에서 페이지를 찾을 수 없습니다: {content}")
            # 기본 청크 생성
            chunk_data = {
                "content": content,
                "chunk_num": "1",
                "page_num": "1",
                "memo": memo,
                "content_type": content_type,
                "text_data": f"[Source: {content}]\n[Page: 1]\n[이 문서는 페이지를 포함하지 않습니다.]",
            }
            
            # 임베딩 및 저장
            embedding = await asyncio.wait_for(
                embedding_model.aembed_query(chunk_data["text_data"]),
                timeout=_pdf_embedding_timeout_sec(),
            )
            chunk_data["embedding"] = f"[{','.join(map(str, embedding))}]"
            
            # 저장 전 UTF-8 인코딩 검증
            cleaned_data = {}
            for key, value in chunk_data.items():
                if isinstance(value, str):
                    cleaned_data[key] = clean_text_for_utf8(value)
                else:
                    cleaned_data[key] = value
            
            await insert_data(table=table_name, data=cleaned_data, dbname=dbname)
            
            return {
                "status": "success",
                "chunks": 1,
                "chunk_count": [1],
                "use_source": [content]
            }

        logger.info(f"PDF 페이지 기반 처리 시작: {content}, 총 페이지: {total_pages}")

        # 디버깅: process_pdf에서 personal_info_filter 값 확인
        logger.info(f"[PDF process_pdf 디버깅] personal_info_filter='{personal_info_filter}'")
        
        # 3. 페이지 기반 배치 처리 (페이지별 자동 OCR 판단)
        page_result = await process_all_pages_parallel_pdf(
            file_path, total_pages, content, memo, content_type, table_name, dbname, job_id,
            job_manager, job_progress_manager, page_progress, personal_info_filter
        )
        
        if page_result["status"] == "cancelled":
            return {"status": "cancelled", "message": "작업이 취소되었습니다."}
        
        processed_pages = page_result["processed_pages"]

        logger.info(f"PDF 처리 완료: {content}, 총 페이지 수: {processed_pages} (페이지별 자동 OCR 적용)")
        logger.info(
            "[PDFDebug][process_pdf.done] job_id=%s file=%s processed_pages=%s total_pages=%s elapsed_ms=%s",
            job_id,
            os.path.basename(file_path or ""),
            processed_pages,
            total_pages,
            int((time.perf_counter() - started_at) * 1000),
        )
        return {
            "status": "success",
            "chunks": processed_pages,
            "chunk_count": [processed_pages],
            "use_source": [content]
        }

    except Exception as e:
        error_msg = f"PDF 처리 중 오류 발생: {content} - {str(e)}"
        logger.error(error_msg, exc_info=True)
        await send_message_to_socket(
            job_id, {"status": "error", "message": error_msg}, job_manager
        )
        raise RuntimeError(error_msg)

DEFAULT_CONFIG = {
    "ocr": False, # 문자 인식 여부
    "coordinates": True, # 좌표 여부
    "output_formats": "['html', 'text', 'markdown']", # 출력 형식
    "model": "document-parse", # 모델 선택
    "base64_encoding": "['figure', 'chart', 'table', 'equation']", # 이미지 인코딩할 element 선택
}

UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")

try:
    from langchain_teddynote.messages import stream_graph
except Exception:
    def stream_graph(*args, **kwargs):
        return None
import requests
try:
    import pymupdf
except Exception:
    pymupdf = None
import base64
from PIL import Image
import io
from langchain_core.documents import Document
import asyncio

# ❌ DEPRECATED: process_pdf_v2 함수는 더 이상 사용하지 않습니다.
# 이 함수는 PDF를 10페이지씩 분할하여 처리하는데, 분할된 파일이 50MB를 넘을 수 있어 
# Upstage API 413 오류를 발생시킵니다.
# 대신 process_pdf 함수를 사용하세요 (페이지별 개별 OCR 처리)
async def process_pdf_v2_DEPRECATED(
    content: str,
    file_path: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    memo: str = "",
):
    """
    ❌ DEPRECATED: 이 함수는 더 이상 사용하지 않습니다.
    PDF를 10페이지씩 분할하여 처리하는데, 분할된 파일이 50MB를 넘을 수 있어 
    Upstage API 413 오류를 발생시킵니다.
    
    대신 process_pdf() 함수를 사용하세요 (페이지별 개별 OCR 처리)
    """
    error_msg = f"process_pdf_v2는 더 이상 사용되지 않습니다. process_pdf를 사용하세요: {content}"
    logger.error(error_msg)
    await send_message_to_socket(
        job_id, {"status": "error", "message": error_msg}, job_manager
    )
    raise RuntimeError(error_msg)

    
async def split_pdf(
    file_path: str,
    batch_size: int = 10
):
    """
    Return : list[str]
    분할된 PDF 파일 경로 리스트
    """
    try:
        input_pdf = pymupdf.open(file_path)
        num_pages = len(input_pdf)

        ret = []

        for start_page in range(0, num_pages, batch_size):
            end_page = min(start_page + batch_size , num_pages) -1

            input_file_basename = os.path.splitext(file_path)[0]
            output_file = f"{input_file_basename}_{start_page:04d}_{end_page:04d}.pdf"

            logger.info(f"split_pdf : 분할 PDF 생성")

            with pymupdf.open() as output_pdf:
                output_pdf.insert_pdf(input_pdf, from_page=start_page, to_page=end_page)
                output_pdf.save(output_file)
                ret.append(output_file)

        input_pdf.close()

        return ret

    except Exception as e:
        logger.error(f"split_pdf 처리 중 오류 발생: {e}", exc_info=True)
        raise ValueError(f"split_pdf 처리 중 오류 발생: {e}")


async def upstage_parser(
    file_paths : list[str],
    use_ocr : bool = False,
    config : dict = DEFAULT_CONFIG
):
    try:
        headers = {
            "Authorization" : f"Bearer {UPSTAGE_API_KEY}"
        }

        if use_ocr:
            config["ocr"] = True

        json_results = []

        for file_path in file_paths:
            logger.info(f"document_parser : {file_path} 파일 파싱 시작")

            if os.path.exists(file_path):
                 # 이미 파싱된 파일이 있는 경우 바로 반환
                 output_file = os.path.splitext(file_path)[0] + '.json'
                 if os.path.exists(output_file):
                    logger.info(f"document_parser : {file_path} 이미 파싱된 파일이 있습니다. 반환")
                    json_results.append(output_file)
                    continue
                 
            files = {
                "document" : open(file_path, "rb")
            }

            if use_ocr:
                with _acquire_global_ocr_slot(file_path=file_path, page_num=0):
                    response = requests.post(
                        "https://api.upstage.ai/v1/document-ai/document-parse",
                        headers = headers,
                        data=config,
                        files = files
                    )
            else:
                response = requests.post(
                    "https://api.upstage.ai/v1/document-ai/document-parse",
                    headers = headers,
                    data=config,
                    files = files
                )

            if response.status_code == 200:
                output_file = os.path.splitext(file_path)[0] + ".json"

                with open(output_file, "w") as f:
                    json.dump(response.json(), f, ensure_ascii=False, indent=4)
                json_results.append(output_file)

            else:
                raise ValueError(f"API 요청 실패. 상태 코드: {response.status_code}")

        return json_results
 

    except Exception as e:
        logger.error(f"upsage_parser 처리 중 오류 발생: {e}", exc_info=True)
        raise ValueError(f"upsage_parser 처리 중 오류 발생: {e}")


async def upstage_parser_v2(
    file_path : str,
    use_ocr =False,
    config = DEFAULT_CONFIG
):
    try:
        logger.info(f"[Upstage API] 문서 파싱 시작: {os.path.basename(file_path)}")
        
        headers = {
            "Authorization" : f"Bearer {UPSTAGE_API_KEY}"
        }

        if use_ocr:
            config["ocr"] = True
            logger.info(f"[Upstage API] OCR 모드 활성화")

        if os.path.exists(file_path):
            output_file = os.path.splitext(file_path)[0] + ".json"
            if os.path.exists(output_file):
                logger.info(f"[Upstage API] 이미 파싱된 파일 재사용: {os.path.basename(output_file)}")
                return output_file
            
        logger.info(f"[Upstage API] API 요청 전송 중...")
        
        files = {
            "document" : open(file_path, "rb")
        }

        if use_ocr:
            with _acquire_global_ocr_slot(file_path=file_path, page_num=0):
                response = requests.post(
                    "https://api.upstage.ai/v1/document-ai/document-parse",
                    headers = headers,
                    data=config,
                    files = files
                )
        else:
            response = requests.post(
                "https://api.upstage.ai/v1/document-ai/document-parse",
                headers = headers,
                data=config,
                files = files
            )

        if response.status_code == 200:
            output_file = os.path.splitext(file_path)[0] + ".json"
            logger.info(f"[Upstage API] 파싱 성공, 결과 저장 중: {os.path.basename(output_file)}")

            with open(output_file, "w") as f: 
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
            
            logger.info(f"[Upstage API] 파싱 완료: {os.path.basename(output_file)}")
            return output_file
        else:
            error_text = response.text
            logger.error(f"[Upstage API] 요청 실패: 상태코드 {response.status_code}, 응답: {error_text}")
            raise ValueError(f"API 요청 실패. 상태 코드: {response.status_code}, 응답: {error_text}")
        
    except Exception as e:
        logger.error(f"[Upstage API] 처리 중 오류 발생: {e}", exc_info=True)
        raise ValueError(f"upstage_parser_v2 처리 중 오류 발생: {e}")
    
async def export_images(
    elements : list[dict],
    file_path: str
):
    try:
        dirname = os.path.dirname(file_path)
        basename = os.path.basename(file_path)

        for element in elements:
            if element['category'] in ['figure', 'chart', 'table' , 'equation']:
                base64_encoding = element.get('base64_encoding')
                image_path = await save_to_image(base64_encoding, dirname, basename, element['category'], element['page'], element['id'])
                element['png_filepath'] = image_path

        return elements
    
    except Exception as e:
        logger.error(f"export_images 처리 중 오류 발생: {e}", exc_info=True)
        raise ValueError(f"export_images 처리 중 오류 발생: {e}")

async def save_to_image(
        base64_encoding : str,
        dirname : str,
        basename : str,
        category : str, 
        page,
        index
):
    try:
        image_data = base64.b64decode(base64_encoding)

        image = Image.open(io.BytesIO(image_data))

        image_dir = os.path.join(dirname, "images", category)
        os.makedirs(image_dir, exist_ok=True)

        base_prefix = os.path.splitext(basename)[0]
        image_filename = (
            f"{base_prefix.upper()}_{category.upper()}_Page_{page+1}_Index+{index}."
            f"png"
        )

        image_path = os.path.join(image_dir, image_filename)

        abs_image_path = os.path.abspath(image_path)

        image.save(abs_image_path)
        return abs_image_path
    
    except Exception as e:
        logger.error(f"save_to_image 처리 중 오류 발생: {e}", exc_info=True)
        raise ValueError(f"save_to_image 처리 중 오류 발생: {e}")
        
def parse_start_end_page(filepath):
    filename = os.path.basename(filepath)

    name_without_ext = filename.rsplit(".", 1)[0]

    try:
        if len(name_without_ext) < 9:
            return (-1, -1)
        
        page_numbers = name_without_ext[-9:]

        if (
            page_numbers[4] == "_"
            and page_numbers[:4].isdigit()
            and page_numbers[5:].isdigit()
        ):
            return (-1 , -1)
        
        start_page = int(page_numbers[:4])
        end_page = int(page_numbers[5:])

        if start_page > end_page:
            return (-1 , -1)
        
        return (start_page, end_page)
    
    except (IndexError, ValueError):
        return (-1, -1)
    
async def export_markdown(
    file_path : str,
    elements : list[dict],
    ignore_new_line_in_text = False,
    show_image = True
):
    seperator_text = "\n\n"
    try:
        dirname = os.path.dirname(file_path)
        basename = os.path.basename(file_path)
        md_basename = os.path.splitext(basename)[0] + ".md"               
        md_file_path = os.path.join(dirname,md_basename)

        current_page = 0

        with open(md_file_path, "w" , encoding="utf-8") as f:
            for element in elements:
                if element['category'] in ['header', 'footer', 'footnote']:
                    continue
            
                if element['category'] in ['figure', 'chart']:
                    if show_image:
                        png_filepath = element['png_filepath']
                        md_id = f"<<<Content: {element['id']}>>>"
                        f.write(md_id + seperator_text)

                elif element['category'] in ['table', 'equation']:
                    if show_image:
                        png_filepath = element['png_filepath']
                        md_id = f"<<<Content: {element['id']}>>>"
                        f.write(md_id + seperator_text)

                    f.write(element['content']['markdown'] + seperator_text)

                elif element['category'] in ['paragraph']:
                    if ignore_new_line_in_text:
                        f.write(element['content']['markdown'].replace("\n", "") + seperator_text)
                    else:
                        f.write(element['content']['markdown'] + seperator_text)

                else:
                    f.write(element['content']['markdown'] + seperator_text)

        return md_file_path
                        


    except Exception as e:
        logger.error(f"export_markdown 처리 중 오류 발생: {e}", exc_info=True)
        raise ValueError(f"export_markdown 처리 중 오류 발생: {e}")

async def extract_text_with_ocr(pdf_page_file: str) -> str:
    """
    단일 페이지 PDF 파일에 대해 Upstage API로 OCR을 수행하여 텍스트를 추출합니다.
    """
    try:
        ocr_json_path = await upstage_parser_v2(pdf_page_file, use_ocr=True)
        with open(ocr_json_path, "r", encoding="utf-8") as f:
            ocr_data = json.load(f)
        
        return ocr_data['content']['markdown']
    except Exception as e:
        logger.error(f"OCR 텍스트 추출 중 오류: {e}", exc_info=True)
        return ""

async def calculate_pdf_chunks(file_path: str) -> int:
    """PDF 파일의 페이지 수를 반환합니다 (1페이지 = 1청크)."""
    try:
        import pdfplumber
        
        logger.info(f"[페이지 수 계산] PDF 파일: {os.path.basename(file_path)}")
        
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            logger.info(f"[페이지 수 계산 완료] 총 페이지: {total_pages}개 (1페이지 = 1청크)")
            return total_pages
        
    except Exception as e:
        logger.error(f"PDF 페이지 수 계산 실패: {file_path}, 오류: {e}")
        return 10  # 기본값

def is_cid_text(text: str) -> bool:
    """
    텍스트가 CID 형식인지 확인
    CID 패턴: (cid:숫자) 형태가 전체 텍스트의 30% 이상이면 CID로 판단
    """
    import re
    if not text:
        return False
    
    cid_pattern = r'\(cid:\d+\)'
    cid_matches = re.findall(cid_pattern, text)
    
    # CID 문자가 5개 이상이거나, 전체 단어의 30% 이상이면 CID 텍스트로 판단
    if len(cid_matches) >= 5:
        words = text.split()
        if words and len(cid_matches) / len(words) > 0.3:
            return True
    
    return False        
