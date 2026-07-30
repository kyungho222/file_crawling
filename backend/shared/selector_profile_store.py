from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse, parse_qsl, urlencode


@dataclass
class SelectorProfile:
    # ✅ 프로필 키:
    # - 요구사항: "크롤링 대상이 변경되었을 때만" 샘플링/LLM 학습을 1회 수행
    # - 기존 구현은 domain 단위로만 1회 학습했으나, 같은 도메인 내에서도 게시판/메뉴별 DOM이 달라질 수 있다.
    # - 따라서 profile_key를 (domain + board_url 시그니처)로 분리해서 저장/조회한다.
    profile_key: str
    domain: str
    learned_at: str
    model: str
    board_url: str = ""
    # CSS selectors
    title_selector: Optional[str] = None
    content_selector: Optional[str] = None
    attachment_selector: Optional[str] = None
    # meta selectors (optional)
    meta_root_selector: Optional[str] = None
    date_selector: Optional[str] = None
    author_selector: Optional[str] = None
    department_selector: Optional[str] = None
    phone_selector: Optional[str] = None
    view_selector: Optional[str] = None
    # meta
    samples: int = 0
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


_lock = asyncio.Lock()


def _default_path() -> str:
    # 프로젝트 루트 기준 저장 (배포 환경에서도 쓰기 가능한 경로를 우선)
    # env로 오버라이드 가능
    return os.getenv("BOARD_SELECTOR_PROFILE_PATH", os.path.join(os.path.dirname(__file__), "selector_profiles.json"))


_PAGINATION_QUERY_KEYS = {
    "page",
    "pageindex",
    "pageno",
    "page_no",
    "curpage",
    "currentpage",
    "offset",
    "start",
    "limit",
    "perpage",
    "pagesize",
    "rows",
    "size",
}


def make_profile_key(domain: str, board_url: Optional[str]) -> str:
    """
    profile_key 생성 규칙(안정적/재현 가능):
    - 기본은 domain
    - board_url이 있으면 path + (pagination 제외한 query)를 포함
    - query는 key 정렬 후 결합하여 동일 대상이면 key가 항상 동일
    """
    dom = (domain or "").strip().lower()
    if not dom:
        return ""
    bu = (board_url or "").strip()
    if not bu:
        return dom
    try:
        u = urlparse(bu)
        path = (u.path or "").strip()
        # pagination 계열 파라미터는 제거(대상 식별에 불필요)
        q = []
        for k, v in parse_qsl(u.query or "", keep_blank_values=True):
            lk = (k or "").strip().lower()
            if lk in _PAGINATION_QUERY_KEYS:
                continue
            q.append((k, v))
        q.sort(key=lambda kv: (kv[0], kv[1]))
        qstr = urlencode(q, doseq=True)
        if qstr:
            return f"{dom}|{path}?{qstr}"
        return f"{dom}|{path}"
    except Exception:
        return dom


async def load_profiles(path: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    p = path or _default_path()
    async with _lock:
        try:
            if not os.path.exists(p):
                return {}
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            return {}


async def save_profiles(profiles: Dict[str, Dict[str, Any]], path: Optional[str] = None) -> None:
    p = path or _default_path()
    async with _lock:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(profiles or {}, f, ensure_ascii=False, indent=2)
        os.replace(tmp, p)


async def get_profile(profile_key: str, path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not profile_key:
        return None
    profiles = await load_profiles(path=path)
    prof = profiles.get(profile_key)
    if isinstance(prof, dict):
        return prof
    return None


async def upsert_profile(profile: SelectorProfile, path: Optional[str] = None) -> None:
    if not profile or not profile.profile_key:
        return
    profiles = await load_profiles(path=path)
    profiles[profile.profile_key] = profile.to_dict()
    await save_profiles(profiles, path=path)


def new_profile(
    *,
    profile_key: str,
    domain: str,
    board_url: str = "",
    model: str,
    title_selector: Optional[str],
    content_selector: Optional[str],
    attachment_selector: Optional[str],
    meta_root_selector: Optional[str] = None,
    date_selector: Optional[str] = None,
    author_selector: Optional[str] = None,
    department_selector: Optional[str] = None,
    phone_selector: Optional[str] = None,
    view_selector: Optional[str] = None,
    samples: int,
    confidence: float,
) -> SelectorProfile:
    return SelectorProfile(
        profile_key=profile_key,
        domain=domain,
        board_url=str(board_url or ""),
        learned_at=datetime.now().isoformat(),
        model=model,
        title_selector=title_selector,
        content_selector=content_selector,
        attachment_selector=attachment_selector,
        meta_root_selector=meta_root_selector,
        date_selector=date_selector,
        author_selector=author_selector,
        department_selector=department_selector,
        phone_selector=phone_selector,
        view_selector=view_selector,
        samples=int(samples or 0),
        confidence=float(confidence or 0.0),
    )


