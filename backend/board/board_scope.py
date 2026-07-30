import logging
import re
from typing import Optional
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger("backend.board.board_scope")


def normalize_host(host: str) -> str:
    host = (host or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def extract_board_id(path: str) -> Optional[str]:
    try:
        m = re.search(r"/bbs/([^/]+)/", path or "", re.IGNORECASE)
    except Exception:
        m = None
    if not m:
        return None
    return m.group(1)


def extract_menu_no(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    qs = parse_qs(parsed.query or "")
    for key in (
        "menuNo",
        "menuno",
        "menu_no",
        "menu",
        "menu_cd",
        "ctgryCd",
        "ctgry_cd",
        "ctgrycd",
        "categoryCd",
        "category_cd",
    ):
        if key in qs and qs[key]:
            return str(qs[key][0])
        for k, v in qs.items():
            if k.lower() == key.lower() and v:
                return str(v[0])
    return None


def extract_board_param(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    qs = parse_qs(parsed.query or "")
    for key in ("bbsId", "bbs_id", "bbsCd", "bbs_cd", "boardId", "board_id"):
        if key in qs and qs[key]:
            return str(qs[key][0])
        for k, v in qs.items():
            if k.lower() == key.lower() and v:
                return str(v[0])
    return None


def is_list_page_url(u: str) -> bool:
    lu = (u or "").lower()
    if "list.do" in lu or "list.asp" in lu or "list.jsp" in lu:
        return True
    try:
        path = (urlparse(u).path or "").lower()
        return path.endswith(("list.do", "list.asp", "list.jsp"))
    except Exception:
        return False


def is_same_board_scope(base_url: str, candidate_url: str) -> bool:
    try:
        base = urlparse(base_url)
        cand = urlparse(candidate_url)
    except Exception:
        return False
    if normalize_host(base.netloc) != normalize_host(cand.netloc):
        return False
    try:
        if (base.path or "").strip() in ("", "/") and not base.query and not base.fragment:
            return True
    except Exception:
        pass
    base_board = extract_board_id(base.path or "") or None
    base_menu = extract_menu_no(base_url) or None
    cand_board_param = extract_board_param(candidate_url) or None
    cand_board = extract_board_id(cand.path or "") or None
    cand_menu = extract_menu_no(candidate_url) or None

    logger.debug(
        "[BoardScope][check] base_url=%s base_board=%s base_menu=%s candidate_url=%s cand_board_param=%s cand_board=%s cand_menu=%s",
        base_url,
        base_board,
        base_menu,
        candidate_url,
        cand_board_param,
        cand_board,
        cand_menu,
    )

    if not base_board:
        logger.debug("[BoardScope][check] base_board missing -> allow")
        return True
    if cand_board_param:
        return cand_board_param.lower() == base_board.lower()
    if cand_board:
        return cand_board.lower() == base_board.lower()
    if base_menu and cand_menu:
        return base_menu == cand_menu
    logger.debug("[BoardScope][check] no matching board/menu -> deny")
    return False


def candidate_matches_board_strict(candidate_url: str, base_board: Optional[str], base_menu: Optional[str]) -> bool:
    if not base_board:
        return True
    try:
        parsed = urlparse(candidate_url)
    except Exception:
        return False
    try:
        cand_board = extract_board_id(parsed.path or "") or ""
        if cand_board and cand_board.lower() == base_board.lower():
            return True
    except Exception:
        pass
    try:
        cand_board_param = extract_board_param(candidate_url)
        if cand_board_param and cand_board_param.lower() == base_board.lower():
            return True
    except Exception:
        pass
    try:
        if base_menu:
            cand_menu = extract_menu_no(candidate_url)
            if cand_menu and cand_menu == base_menu:
                return True
    except Exception:
        pass
    logger.debug(
        "[BoardScope][strict] candidate=%s base_board=%s base_menu=%s -> result=%s",
        candidate_url,
        base_board,
        base_menu,
        False,
    )
    return False

