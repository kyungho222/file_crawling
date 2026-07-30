from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger("backend.shared.selector_learning_openai")


OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEEPSEEK_CHAT_COMPLETIONS_URL = "https://api.deepseek.com/chat/completions"


def _clamp(s: str, n: int) -> str:
    s = str(s or "")
    return s if len(s) <= n else s[:n]


def build_minimal_candidates_from_html(html: str, *, max_candidates: int = 8) -> List[Dict[str, Any]]:
    """
    LLM에 전체 HTML을 보내지 않고, 후보 컨테이너들의 요약만 보내기 위한 전처리.
    - selector: 단순 힌트(id/class 기반)
    - text_head: 짧은 미리보기
    - text_len: 길이
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return []

    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return []

    # 후보: 흔한 본문 컨테이너 + 큰 div 섹션
    selectors = [
        # 특정 사이트(예: gwangjin.go.kr)에서 본문 컨테이너로 자주 등장 (대소문자 포함)
        ".dbData",
        ".dbdata",
        # 첨부/메타 영역 힌트(LLM이 view/attachment selector를 더 쉽게 맞추도록)
        ".status",
        "dl.fileSet",
        "article",
        "main",
        "#content",
        "#contents",
        ".content",
        ".contents",
        ".board_view",
        ".view",
        ".view_wrap",
        ".entry-content",
    ]

    cand_nodes = []
    seen = set()

    def _selector_for(tag) -> str:
        try:
            tid = tag.get("id")
            if tid:
                return f"#{tid}"
        except Exception:
            pass
        try:
            cls = tag.get("class") or []
            if isinstance(cls, list) and cls:
                # 너무 긴 class list는 앞의 2개만
                short = [c for c in cls if c][:2]
                if short:
                    return tag.name + "".join(f".{c}" for c in short)
        except Exception:
            pass
        return tag.name

    for sel in selectors:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if el is None:
            continue
        key = id(el)
        if key in seen:
            continue
        seen.add(key)
        cand_nodes.append(el)

    # 추가 후보: 본문 길이가 긴 div 몇 개 (너무 많이 보면 느려지므로 제한을 낮춤)
    try:
        for div in soup.find_all(["div", "section"], limit=80):
            key = id(div)
            if key in seen:
                continue
            txt = div.get_text(" ", strip=True)
            if not txt or len(txt) < 200:
                continue
            seen.add(key)
            cand_nodes.append(div)
            if len(cand_nodes) >= max_candidates:
                break
    except Exception:
        pass

    out: List[Dict[str, Any]] = []
    for tag in cand_nodes[:max_candidates]:
        try:
            text = tag.get_text(" ", strip=True)
        except Exception:
            text = ""
        out.append(
            {
                "selector_hint": _selector_for(tag),
                "text_len": len(text),
                "text_head": _clamp(text, 160),
            }
        )
    return out


def build_page_hints_from_html(html: str) -> Dict[str, Any]:
    """
    LLM이 'JSON 필드 기준'으로 더 정확히 셀렉터를 고르도록, 페이지 단위 힌트를 추가로 생성한다.
    - title 후보(heading 몇 개)
    - status(dl/dt/dd) 형태의 메타 라벨/값 힌트
    - fileDown 링크(파일명은 보통 a[title]) 힌트
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return {}
    if not html:
        return {}
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return {}

    def _norm(s: str) -> str:
        return _clamp(" ".join(str(s or "").split()), 200)

    hints: Dict[str, Any] = {}

    # title candidates
    heads: List[Dict[str, Any]] = []
    try:
        for h in soup.select("h1,h2,h3")[:6]:
            t = _norm(h.get_text(" ", strip=True))
            if not t or len(t) < 4:
                continue
            heads.append({"tag": h.name, "class": h.get("class"), "id": h.get("id"), "text": _clamp(t, 120)})
    except Exception:
        pass
    if heads:
        hints["heading_candidates"] = heads

    # status dt/dd pairs (조회수/등록일/전화번호 등)
    pairs: List[Dict[str, Any]] = []
    try:
        for dl in soup.select(".status dl")[:8]:
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if not dt or not dd:
                continue
            pairs.append(
                {
                    "label": _clamp(_norm(dt.get_text(" ", strip=True)), 40),
                    "value_head": _clamp(_norm(dd.get_text(" ", strip=True)), 60),
                    "dd_class": dd.get("class"),
                }
            )
    except Exception:
        pass
    if pairs:
        hints["meta_dt_dd_pairs"] = pairs

    # attachment fileDown anchors (prefer title attr = filename.ext)
    files: List[Dict[str, Any]] = []
    try:
        for a in soup.select('a[href*="fileDown.do"]')[:10]:
            title = (a.get("title") or "").strip()
            txt = _norm(a.get_text(" ", strip=True))
            href = (a.get("href") or "").strip()
            if title or txt:
                files.append({"title_attr": _clamp(title, 120), "text": _clamp(txt, 120), "href_head": _clamp(href, 160)})
    except Exception:
        pass
    if files:
        hints["filedown_links"] = files

    return hints


def build_title_candidates_from_html(html: str, *, max_candidates: int = 12) -> List[Dict[str, str]]:
    """
    제목 fallback LLM에 전달할 소형 후보 묶음을 생성한다.
    전체 HTML을 보내지 않고 제목 가능성이 높은 텍스트만 추린다.
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return []

    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return []

    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    def _push(source: str, text: str) -> None:
        normalized = " ".join(str(text or "").split()).strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        out.append({"source": source, "text": _clamp(normalized, 180)})

    try:
        if soup.title and soup.title.get_text(strip=True):
            _push("title_tag", soup.title.get_text(" ", strip=True))
    except Exception:
        pass

    for sel in (
        "meta[property='og:title']",
        "meta[name='title']",
        "meta[name='subject']",
        "meta[name='twitter:title']",
        "meta[name='dc.title']",
    ):
        try:
            tag = soup.select_one(sel)
        except Exception:
            tag = None
        if tag is not None:
            _push(sel, tag.get("content") or "")

    for sel in (
        "#content h1",
        "#content h2",
        "#content h3",
        "#contents h1",
        "#contents h2",
        "#contents h3",
        ".content h1",
        ".content h2",
        ".content h3",
        ".board_view h1",
        ".board_view h2",
        ".board_view h3",
        ".board_view h4",
        ".view_title",
        ".board_view_title",
        ".subject",
        "button.current_tab_only span",
        "h1",
        "h2",
        "h3",
        "h4",
    ):
        try:
            for idx, el in enumerate(soup.select(sel)[:3]):
                _push(f"{sel}[{idx}]", el.get_text(" ", strip=True))
        except Exception:
            continue

    try:
        for tag in soup.find_all(["th", "dt", "label", "span"], limit=120):
            label = re.sub(r"\s+", "", tag.get_text(" ", strip=True))
            if label not in {"제목", "글제목", "제목명", "프로그램명", "행사명", "이벤트명"}:
                continue
            sibling = tag.find_next_sibling(["td", "dd", "div", "span"])
            if sibling is not None:
                _push(f"label:{label}", sibling.get_text(" ", strip=True))
    except Exception:
        pass

    return out[:max_candidates]


def _resolve_openai_compatible_api_key(
    *,
    provider: str,
    db_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> str:
    if api_key is not None and str(api_key).strip():
        return str(api_key).strip()

    provider_l = str(provider or "").strip().lower()
    if provider_l == "deepseek":
        return (
            os.getenv("DEEPSEEK_API_KEY")
            or os.getenv("DEEPSEEK_TITLE_API_KEY")
            or ""
        ).strip()

    if db_name is not None and str(db_name).strip():
        from utils.whoami import get_openai_api_key_for_db_name

        return (get_openai_api_key_for_db_name(str(db_name).strip()) or "").strip()
    return (os.getenv("OPENAI_API_KEY") or "").strip()


async def infer_title_openai_compatible(
    *,
    domain: str,
    url: str,
    html: str,
    current_title: str = "",
    web_title: str = "",
    provider: str = "deepseek",
    model: Optional[str] = None,
    timeout_sec: float = 20.0,
    db_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    약한 제목만 대상으로 실제 게시글 제목을 추정한다.
    OpenAI/DeepSeek의 OpenAI-compatible chat completions endpoint를 사용한다.
    """
    provider_l = str(provider or "").strip().lower() or "deepseek"
    resolved_key = _resolve_openai_compatible_api_key(
        provider=provider_l,
        db_name=db_name,
        api_key=api_key,
    )
    if not resolved_key:
        return None

    if provider_l == "deepseek":
        endpoint = (os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").rstrip("/") + "/chat/completions"
        chosen_model = (model or os.getenv("BOARD_TITLE_LLM_MODEL") or "deepseek-v4-flash").strip()
    else:
        endpoint = OPENAI_CHAT_COMPLETIONS_URL
        chosen_model = (model or os.getenv("BOARD_TITLE_LLM_MODEL") or "gpt-4.1-mini").strip()

    title_candidates = build_title_candidates_from_html(html)
    page_hints = build_page_hints_from_html(html)

    body_hint = ""
    try:
        for item in build_minimal_candidates_from_html(html, max_candidates=4):
            text_head = str(item.get("text_head") or "").strip()
            if len(text_head) > len(body_hint):
                body_hint = text_head
    except Exception:
        body_hint = ""

    payload_obj = {
        "domain": domain,
        "url": _clamp(url, 280),
        "current_title": _clamp(current_title, 180),
        "web_title": _clamp(web_title, 180),
        "title_candidates": title_candidates,
        "heading_candidates": page_hints.get("heading_candidates") or [],
        "body_hint": _clamp(body_hint, 500),
        "output_format": {
            "title": "string|null",
            "confidence": "number 0..1",
        },
    }

    system = (
        "You extract the real title of a single board/detail page.\n"
        "Return STRICT JSON only.\n"
        "Choose the actual post/event/promotion title, not breadcrumb, menu name, tab label, or section label.\n"
        "If the page only exposes generic section labels and no specific title exists, return null.\n"
        "Do not invent text that is not present in the inputs.\n"
    )
    user = "Input:\n" + json.dumps(payload_obj, ensure_ascii=False)
    req = {
        "model": chosen_model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "max_tokens": 180,
    }

    headers = {"Authorization": f"Bearer {resolved_key}", "Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=min(10.0, timeout_sec))
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(endpoint, headers=headers, json=req) as resp:
                if resp.status != 200:
                    try:
                        txt = await resp.text()
                    except Exception:
                        txt = ""
                    logger.warning(
                        "[TitleLLM] provider=%s non-200 | status=%s body=%s",
                        provider_l,
                        resp.status,
                        _clamp(txt, 400),
                    )
                    return None
                data = await resp.json()
    except Exception as exc:
        logger.warning("[TitleLLM] provider=%s request failed | err=%s", provider_l, exc)
        return None

    try:
        content = data["choices"][0]["message"]["content"]
        obj = json.loads(content)
        if not isinstance(obj, dict):
            return None
        title = str(obj.get("title") or "").strip() or None
        confidence = obj.get("confidence")
        return {"title": title, "confidence": confidence}
    except Exception:
        return None


async def infer_selectors_openai(
    *,
    domain: str,
    samples: List[Dict[str, Any]],
    model: str = "gpt-4o-mini",
    timeout_sec: float = 25.0,
    db_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    gpt-4o-mini로 게시글 페이지의 주요 영역 셀렉터를 추론한다.
    - LLM이 못 맞추거나 확신이 없으면 null 반환 (워크플로우에서 기존 휴리스틱으로 fallback)
    반환 dict:
    {
      "title_selector": "string|null",
      "content_selector": "string|null",
      "attachment_selector": "string|null",
      "meta_root_selector": "string|null",
      "date_selector": "string|null",
      "author_selector": "string|null",
      "department_selector": "string|null",
      "phone_selector": "string|null",
      "view_selector": "string|null",
      "confidence": 0.0~1.0
    }
    """
    if api_key is not None and str(api_key).strip():
        resolved_key = str(api_key).strip()
    elif db_name is not None and str(db_name).strip():
        from utils.whoami import get_openai_api_key_for_db_name

        resolved_key = (get_openai_api_key_for_db_name(str(db_name).strip()) or "").strip()
    else:
        resolved_key = (os.getenv("OPENAI_API_KEY") or "").strip()

    if not resolved_key:
        return None

    # 토큰 최소화: 샘플 후보 요약만 전송
    payload_obj = {
        "domain": domain,
        "samples": samples[:3],
        "output_format": {
            "title_selector": "string|null",
            "content_selector": "string|null",
            "attachment_selector": "string|null",
            "meta_root_selector": "string|null",
            "date_selector": "string|null",
            "author_selector": "string|null",
            "department_selector": "string|null",
            "phone_selector": "string|null",
            "view_selector": "string|null",
            "confidence": "number 0..1",
        },
    }

    system = (
        "You are a web scraping expert.\n"
        "Goal: infer robust CSS selectors that map to JSON fields for 'board post detail pages'.\n"
        "\n"
        "Field rules (IMPORTANT):\n"
        "- title_selector: must select the post title text (full title), NOT breadcrumb/category.\n"
        "- content_selector: must select ONLY the main body content area (e.g. a .dbData-like container). "
        "Do NOT select a wrapper that includes header/breadcrumb/share buttons/metadata tables.\n"
        "- attachment_selector: should select the attachment list area OR the file download anchors themselves. "
        "Prefer file download links like a[href*='fileDown.do'] and note that the filename is often in a[title]. "
        "Do NOT select action buttons like '바로보기/바로듣기'.\n"
        "- view_selector: should select the element containing ONLY the view count number (digits/commas), "
        "often a dd with a class like .ico-view.\n"
        "- phone_selector/date_selector/author_selector/department_selector: select the corresponding value node.\n"
        "- meta_root_selector: if there is a stable metadata container, set it (e.g. a .status block).\n"
        "\n"
        "Selectors should be stable and not overly specific. Return STRICT JSON only (no markdown). "
        "If unsure, return null for that selector and low confidence.\n"
    )

    user = "Input:\n" + json.dumps(payload_obj, ensure_ascii=False)

    req = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
        # 응답은 매우 짧아야 한다(토큰 절약)
        "max_tokens": 300,
    }

    headers = {"Authorization": f"Bearer {resolved_key}", "Content-Type": "application/json"}
    timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=min(10.0, timeout_sec))

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(OPENAI_CHAT_COMPLETIONS_URL, headers=headers, json=req) as resp:
                if resp.status != 200:
                    try:
                        txt = await resp.text()
                    except Exception:
                        txt = ""
                    logger.warning("[SelectorLLM] openai non-200 | status=%s body=%s", resp.status, _clamp(txt, 400))
                    return None
                data = await resp.json()
    except Exception as exc:
        logger.warning("[SelectorLLM] openai request failed | err=%s", exc)
        return None

    try:
        content = data["choices"][0]["message"]["content"]
        obj = json.loads(content)
        if not isinstance(obj, dict):
            return None
        # normalize keys
        return {
            "title_selector": obj.get("title_selector"),
            "content_selector": obj.get("content_selector"),
            "attachment_selector": obj.get("attachment_selector"),
            "meta_root_selector": obj.get("meta_root_selector"),
            "date_selector": obj.get("date_selector"),
            "author_selector": obj.get("author_selector"),
            "department_selector": obj.get("department_selector"),
            "phone_selector": obj.get("phone_selector"),
            "view_selector": obj.get("view_selector"),
            "confidence": obj.get("confidence"),
        }
    except Exception:
        return None
