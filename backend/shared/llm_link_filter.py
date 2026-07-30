from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("backend.shared.llm_link_filter")

OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

_CLEAR_DOC_WORDS = (
    "계획",
    "공고",
    "고시",
    "안내",
    "신청",
    "서식",
    "붙임",
    "첨부",
    "결과",
    "보고",
    "자료",
    "교육",
    "모집",
    "채용",
    "사업",
)
_GENERIC_NAMES = {
    "",
    "file",
    "download",
    "attachment",
    "첨부",
    "첨부파일",
    "다운로드",
    "붙임",
}
_DOC_EXT_RE = re.compile(r"\.(pdf|hwp|hwpx|doc|docx|ppt|pptx|xls|xlsx|csv)(?:$|[?#])", re.I)


def _candidate_log_sample(candidates: List[Any], *, limit: int = 8) -> List[Dict[str, str]]:
    sample: List[Dict[str, str]] = []
    for candidate in candidates[: max(0, int(limit or 0))]:
        attach = candidate[0] if isinstance(candidate, tuple) and candidate else candidate
        if not isinstance(attach, dict):
            sample.append({"type": type(attach).__name__})
            continue
        sample.append(
            {
                "name": str(attach.get("name") or attach.get("title") or attach.get("text") or "")[:120],
                "href": str(attach.get("href") or attach.get("url") or "")[:220],
            }
        )
    return sample


def llm_link_filter_enabled() -> bool:
    return str(os.getenv("FILE_CRAWL_LLM_LINK_FILTER", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _api_key() -> str:
    return (os.getenv("FILE_CRAWL_LLM_LINK_FILTER_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()


def _is_ambiguous_candidate(item: Dict[str, Any]) -> bool:
    name = " ".join(str(item.get(k) or "").strip() for k in ("name", "title", "text")).strip()
    href = str(item.get("href") or "").strip()
    href_low = href.lower()
    if "ne.go.kr/component/file/nd_filedownload.do" in href_low or (
        "nd_filedownload.do" in href_low and "q_filesn=" in href_low and "q_fileid=" in href_low
    ):
        return False
    norm_name = re.sub(r"\s+", "", name).lower()
    if norm_name in _GENERIC_NAMES:
        return True
    if not name or len(name) <= 4:
        return True
    if not _DOC_EXT_RE.search(name) and not _DOC_EXT_RE.search(href):
        return True
    if not any(word in name for word in _CLEAR_DOC_WORDS) and len(name) < 14:
        return True
    return False


async def _classify_batch_with_openai(
    candidates: List[Dict[str, Any]],
    *,
    post_url: str,
    timeout_sec: float,
) -> Optional[Dict[int, bool]]:
    key = _api_key()
    if not key:
        return None

    model = os.getenv("FILE_CRAWL_LLM_LINK_FILTER_MODEL", "gpt-4o-mini")
    compact = []
    for idx, item in enumerate(candidates):
        compact.append(
            {
                "idx": idx,
                "file_name": str(item.get("name") or item.get("title") or item.get("text") or "")[:160],
                "url_path": (urlparse(str(item.get("href") or "")).path or "")[-180:],
                "source_page": post_url[:220],
            }
        )

    system = (
        "You filter public-board attachment links for an education/RAG crawler. "
        "Keep documents that are likely useful official notices, forms, reports, education materials, plans, announcements, or datasets. "
        "Reject ads, banners, previews, navigation, logos, tracking links, and files with no learning value. "
        "Return strict JSON: {\"decisions\":[{\"idx\":0,\"keep\":true,\"reason\":\"short\"}]}."
    )
    req = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps({"candidates": compact}, ensure_ascii=False)},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "max_tokens": 500,
    }
    timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=min(10.0, timeout_sec))
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(OPENAI_CHAT_COMPLETIONS_URL, headers=headers, json=req) as resp:
                if resp.status != 200:
                    logger.warning("[LLMLinkFilter] non-200 response: %s", resp.status)
                    return None
                data = await resp.json()
    except Exception as exc:
        logger.warning("[LLMLinkFilter] request failed: %s", exc)
        return None

    try:
        content = data["choices"][0]["message"]["content"]
        obj = json.loads(content)
        decisions = obj.get("decisions") if isinstance(obj, dict) else None
        out: Dict[int, bool] = {}
        for row in decisions or []:
            idx = int(row.get("idx"))
            out[idx] = bool(row.get("keep"))
        return out
    except Exception:
        return None


async def filter_attachment_candidates_with_llm(
    candidates: List[Any],
    *,
    post_url: str,
) -> List[Any]:
    if not candidates or not llm_link_filter_enabled():
        return candidates
    post_low = str(post_url or "").lower()
    if "ne.go.kr/" in post_low:
        logger.info(
            "[LLMLinkFilter] NE 게시글 첨부는 필터 우회 | total=%s post=%s sample=%s",
            len(candidates),
            post_url[:180],
            _candidate_log_sample(candidates),
        )
        return candidates

    ambiguous: List[Dict[str, Any]] = []
    ambiguous_positions: List[int] = []
    for pos, candidate in enumerate(candidates):
        attach = candidate[0] if isinstance(candidate, tuple) and candidate else candidate
        if not isinstance(attach, dict):
            continue
        if _is_ambiguous_candidate(attach):
            ambiguous.append(attach)
            ambiguous_positions.append(pos)

    if not ambiguous:
        return candidates

    try:
        max_batch = int(os.getenv("FILE_CRAWL_LLM_LINK_FILTER_MAX_BATCH", "12") or "12")
    except Exception:
        max_batch = 12
    max_batch = max(1, min(max_batch, 50))

    try:
        timeout_sec = float(os.getenv("FILE_CRAWL_LLM_LINK_FILTER_TIMEOUT_SEC", "20") or "20")
    except Exception:
        timeout_sec = 20.0
    timeout_sec = max(3.0, min(timeout_sec, 120.0))

    keep_positions = set(range(len(candidates)))
    for start in range(0, len(ambiguous), max_batch):
        batch = ambiguous[start : start + max_batch]
        batch_positions = ambiguous_positions[start : start + max_batch]
        decisions = await _classify_batch_with_openai(batch, post_url=post_url, timeout_sec=timeout_sec)
        if decisions is None:
            continue
        for local_idx, item in enumerate(batch):
            pos = batch_positions[local_idx]
            keep = decisions.get(local_idx, True)
            if not keep:
                keep_positions.discard(pos)

    filtered = [item for idx, item in enumerate(candidates) if idx in keep_positions]
    skipped = len(candidates) - len(filtered)
    if skipped:
        logger.info(
            "[LLMLinkFilter] skipped ambiguous attachments | skipped=%s kept=%s post=%s sample=%s",
            skipped,
            len(filtered),
            post_url[:180],
            _candidate_log_sample(candidates),
        )
    if candidates and not filtered:
        fail_open = str(os.getenv("FILE_CRAWL_LLM_LINK_FILTER_FAIL_OPEN_ON_EMPTY", "1") or "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        logger.warning(
            "[LLMLinkFilter] kept=0 after filter | fail_open=%s total=%s ambiguous=%s post=%s sample=%s",
            fail_open,
            len(candidates),
            len(ambiguous),
            post_url[:180],
            _candidate_log_sample(candidates),
        )
        if fail_open:
            return candidates
    return filtered
