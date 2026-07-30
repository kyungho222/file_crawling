"""
파일 크롤링(colle=file / FileDownloadWorkflow) 전용: 다운로드·LEARN_LIST 저장까지 성공한 게시글 URL을
`backend/shared/crawled_{db_name}.json` 에 누적(고정 파일명, 원자적 쓰기).

pre-seed 시 JSON·DB만 믿지 않고 LEARN_LIST의 content(업로드 URL) → 로컬 경로로 실제 파일 존재 여부를 검사해,
저장소에서 삭제된 건은 스킵 대상에서 제외(다시 다운로드·학습)한다.

게시판 본문 크롤은 `pre_explored_url.save_crawled_urls_report`(타임스탬프 리포트)를 사용하며 이 모듈과 분리한다.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import unquote, urlparse

from utils.url import canonicalize_url_for_dedup

logger = logging.getLogger("backend.shared.file_crawl_success_report")


def _report_dir() -> str:
    """
    기본: 이 모듈이 있는 backend/shared.
    FILE_CRAWL_REPORT_DIR 로 절대/상대 경로 지정 가능(예: 프로젝트 루트의 downloads).
    """
    raw = (os.getenv("FILE_CRAWL_REPORT_DIR") or "").strip()
    if raw:
        return os.path.abspath(raw)
    return os.path.dirname(os.path.abspath(__file__))


def file_crawl_merged_report_path(db_name: Optional[str]) -> str:
    """고정 리포트 경로: backend/shared/crawled_{db_name}.json"""
    safe = str(db_name or "unknown").replace(":", "_")
    return os.path.join(_report_dir(), f"crawled_{safe}.json")


def resolve_post_url_from_file_saved_info(info: Any) -> str:
    """
    progress_queue `file_saved` 의 file_info 에서 게시글(상세) URL 추출.

    download.py 는 `original_meta` 에 file_meta 전체를 넣으며, 게시글 URL은
    - 최상위 `source_page`, 또는
    - `original_meta.original_meta.post_url` (enqueue 시 중첩 dict)
    등에 있을 수 있다.
    """
    if not isinstance(info, dict):
        return ""
    sp = (info.get("source_page") or "").strip()
    if sp:
        return sp
    om = info.get("original_meta")
    if isinstance(om, dict):
        sp = (om.get("post_url") or om.get("source_page") or "").strip()
        if sp:
            return sp
        inner = om.get("original_meta")
        if isinstance(inner, dict):
            sp = (inner.get("post_url") or inner.get("source_page") or "").strip()
            if sp:
                return sp
    return ""


def _normalize_post_url_key(raw: Optional[str]) -> Optional[str]:
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    return canonicalize_url_for_dedup(s) or s


def _urls_from_report_payload(urls_val: Any) -> Set[str]:
    """레거시 [{\"url\": ...}] 및 문자열 리스트 모두 수용."""
    out: Set[str] = set()
    if urls_val is None:
        return out
    if isinstance(urls_val, str):
        k = _normalize_post_url_key(urls_val)
        if k:
            out.add(k)
        return out
    if not isinstance(urls_val, list):
        return out
    for item in urls_val:
        if isinstance(item, str):
            k = _normalize_post_url_key(item)
            if k:
                out.add(k)
        elif isinstance(item, dict):
            u = item.get("url")
            if u:
                k = _normalize_post_url_key(str(u).strip())
                if k:
                    out.add(k)
    return out


def load_file_crawl_success_url_keys(db_name: Optional[str]) -> Set[str]:
    """
    crawled_{db_name}.json 에서 게시글(상세) URL 키 집합 로드. 없거나 손상 시 빈 집합.
    """
    path = file_crawl_merged_report_path(db_name)
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("[FileCrawlSuccessReport] load failed | path=%s err=%s", path, e)
        return set()
    if not isinstance(data, dict):
        return set()
    return _urls_from_report_payload(data.get("urls"))


def _env_skip_storage_validation() -> bool:
    try:
        v = str(os.getenv("FILE_CRAWL_SKIP_STORAGE_VALIDATION", "") or "").strip().lower()
        return v in ("1", "true", "yes", "on", "y")
    except Exception:
        return False


def content_url_to_local_storage_path(
    content: str,
    *,
    chat_bot_id: str,
    db_name: str,
    access_url: Optional[str] = None,
) -> Optional[str]:
    """
    LEARN_LIST content URL을 현재 FileUpload 로컬 경로로 변환한다.
    경로 이슈 확인 문서: backend/docs/FILE_STORAGE_FLOW.md
    - 현재 DB/웹 URL: https://.../chat/uploaded_files/{tail12}/{filename}
    - 호환 URL: https://.../FileUpload/{domain}/{tail12}/{filename}
    """
    raw = (content or "").strip()
    if not raw.startswith("http"):
        return None
    try:
        from config.settings import (
            fileupload_web_path_to_absolute,
            get_storage_domain_for_db_name,
            get_uploaded_files_local_dir,
            normalize_access_url,
        )

        p = urlparse(raw)
        parts = [x for x in (p.path or "").split("/") if x]
        try:
            fi = next(i for i, part in enumerate(parts) if part.lower() == "fileupload")
            if len(parts) > fi + 3:
                web_path = "/" + "/".join(parts[fi:])
                return os.path.normpath(fileupload_web_path_to_absolute(unquote(web_path)))
        except StopIteration:
            pass

        try:
            ui = parts.index("uploaded_files")
            tail = unquote(parts[ui + 1])
            fname = unquote(parts[ui + 2])
        except (ValueError, IndexError):
            return None
        sd = get_storage_domain_for_db_name(db_name)
        web_path = f"/FileUpload/{sd}/{tail}/{fname}"
        return os.path.normpath(fileupload_web_path_to_absolute(web_path))
    except Exception:
        return None


async def load_validated_file_crawl_precrawl_keys(
    db_name: Optional[str],
    chat_bot_id: Optional[str],
    *,
    access_url: Optional[str] = None,
) -> Set[str]:
    """
    JSON의 게시글 URL 중, LEARN_LIST에 연결된 행이 있고 content 경로의 실제 파일이 디스크에 남아 있는 것만 반환.
    - DB에 행이 없거나(구데이터) source_page 미기입이면 검증 불가 → 해당 URL은 스킵하지 않음(재처리).
    - FILE_CRAWL_SKIP_STORAGE_VALIDATION=1 이면 JSON만 로드(기존과 동일).
    """
    raw = load_file_crawl_success_url_keys(db_name)
    if _env_skip_storage_validation():
        return raw
    if not raw or not chat_bot_id or not db_name:
        return raw

    try:
        from db.mariadb_save_update import (
            ensure_learn_list_standard_columns,
            get_account_identifier_from_chatbot_setup,
            get_learn_list_table_name,
        )
        from db.mysql_db_config import mysql_execute_query

        acc_id = await get_account_identifier_from_chatbot_setup(
            str(chat_bot_id).strip(),
            str(db_name).strip(),
        )
        table = get_learn_list_table_name(acc_id)
        cols = await ensure_learn_list_standard_columns(db_name, table)
    except Exception as e:
        logger.warning(
            "[FileCrawlSuccessReport] storage validation skipped (setup error), using raw JSON | err=%s",
            e,
        )
        return raw

    key_col = None
    if "source_page_norm" in cols:
        key_col = "source_page_norm"
    elif "source_page" in cols:
        key_col = "source_page"
    else:
        logger.warning(
            "[FileCrawlSuccessReport] no source_page/source_page_norm column; using raw JSON keys",
        )
        return raw

    type_cond = ""
    if "content_type" in cols:
        type_cond = " AND `content_type` = 'file' "
    elif "type" in cols:
        type_cond = " AND `type` = 'file' "

    unique_keys = list({_normalize_post_url_key(k) or k for k in raw if k})
    valid: Set[str] = set()
    chunk_size = 80
    try:
        for i in range(0, len(unique_keys), chunk_size):
            chunk = unique_keys[i : i + chunk_size]
            ph = ",".join(["%s"] * len(chunk))
            sql = (
                f"SELECT `{key_col}` AS _k, `content` AS _c FROM `{table}` "
                f"WHERE `{key_col}` IN ({ph}) {type_cond}"
            )
            rows = await mysql_execute_query(sql, tuple(chunk), fetch=True, dbname=db_name)
            if not rows:
                continue
            for row in rows:
                try:
                    if not isinstance(row, dict):
                        continue
                    k_db = (
                        row.get("_k")
                        or row.get("_K")
                        or row.get(key_col)
                        or ""
                    )
                    k_db = str(k_db).strip() if k_db is not None else ""
                    c_val = row.get("_c") or row.get("_C") or row.get("content") or ""
                    c_val = str(c_val).strip() if c_val is not None else ""
                    if not k_db or not c_val:
                        continue
                    pk = _normalize_post_url_key(k_db) or canonicalize_url_for_dedup(k_db) or k_db
                    lp = content_url_to_local_storage_path(
                        c_val,
                        chat_bot_id=str(chat_bot_id).strip(),
                        db_name=str(db_name).strip(),
                        access_url=access_url,
                    )
                    if lp and os.path.isfile(lp) and os.path.getsize(lp) > 0:
                        valid.add(pk)
                except Exception:
                    continue
    except Exception as e:
        logger.warning(
            "[FileCrawlSuccessReport] storage validation query failed; using raw JSON | err=%s",
            e,
        )
        return raw

    dropped = len(raw) - len(valid)
    if dropped > 0:
        logger.info(
            "[FileCrawlSuccessReport] pre-validation: %s URLs in JSON → %s with on-disk file (re-crawl %s)",
            len(raw),
            len(valid),
            dropped,
        )
    return valid


def merge_and_save_file_crawl_success_report(
    db_name: Optional[str],
    new_success_url_keys: Optional[Iterable[str]],
    job_id: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    status: str = "completed",
) -> Optional[str]:
    """
    이번 실행에서 새로 성공한 게시글 URL 키를 기존 파일과 합쳐 원자적으로 저장한다.
    new_success_url_keys 가 비어 있으면 파일을 수정하지 않는다.
    """
    incoming: Set[str] = set()
    if new_success_url_keys:
        for x in new_success_url_keys:
            k = _normalize_post_url_key(x) if x else None
            if k:
                incoming.add(k)
    if not incoming:
        logger.info("[FileCrawlSuccessReport] No new success URLs. Skipping write.")
        return None

    path = file_crawl_merged_report_path(db_name)
    merged: Set[str] = set(incoming)
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                old = json.load(f)
            if isinstance(old, dict):
                merged |= _urls_from_report_payload(old.get("urls"))
    except Exception as e:
        logger.warning(
            "[FileCrawlSuccessReport] existing file read failed (merge continues) | path=%s err=%s",
            path,
            e,
        )

    url_list = sorted(merged)
    report_data: Dict[str, Any] = {
        "db_name": db_name,
        "job_id": job_id,
        "target_domains": target_domains or [],
        "total_crawled": len(url_list),
        "crawled_at": datetime.now().isoformat(),
        "status": status,
        "urls": url_list,
    }

    d = _report_dir()
    try:
        os.makedirs(d, exist_ok=True)
    except OSError as e:
        logger.error("[FileCrawlSuccessReport] cannot create report dir | dir=%s err=%s", d, e)
        return None
    tmp_path: Optional[str] = None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".tmp", prefix="crawled_", dir=d, text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
        tmp_path = None
    except Exception as e:
        if tmp_path and os.path.isfile(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        logger.error("[FileCrawlSuccessReport] atomic save failed | path=%s err=%s", path, e)
        return None

    logger.info(
        "[FileCrawlSuccessReport] merged | path=%s total=%s incoming=%s",
        path,
        len(url_list),
        len(incoming),
    )
    return path
