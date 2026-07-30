from __future__ import annotations

import json
import re
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse


_ROOT_DIR = Path(__file__).resolve().parents[2]
_RULES_DIR = _ROOT_DIR / "parser_rules"
_DEFAULT_RULES_PATH = _RULES_DIR / "default.json"
_SITE_RULES_PATH = _RULES_DIR / "site_overrides.json"


@dataclass(frozen=True)
class TitleCandidate:
    selector: str
    text: str
    score: int
    reasons: List[str]


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _collapse_ws(value: Any) -> str:
    text = unescape(str(value or "")).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _flat(value: Any) -> str:
    return re.sub(r"[\s:：ㆍ·\-\[\]\(\)]", "", str(value or "")).strip().lower()


def _is_error_or_placeholder_title(value: Any) -> bool:
    text = _collapse_ws(value)
    if not text:
        return True
    compact = _flat(text)
    low = text.lower().strip()
    if compact in {"error", "err", "404", "500", "403", "502", "503", "badrequest", "notfound"}:
        return True
    if compact in {
        "내비게이션메뉴",
        "네비게이션메뉴",
        "navigationmenu",
        "navmenu",
    }:
        return True
    if low in {"error", "err", "404", "500", "403", "502", "503", "bad request", "not found"}:
        return True
    if any(token in low for token in ("error_title", "error_title_signature", "server error", "runtime error")):
        return True
    return False


def _host_keys(url: str) -> List[str]:
    host = (urlparse(url or "").hostname or "").lower()
    if not host:
        return []
    keys = [host]
    if host.startswith("www."):
        keys.append(host[4:])
    else:
        keys.append(f"www.{host}")
    return keys


def load_parser_rules(url: str = "") -> Dict[str, Any]:
    default_rules = _load_json(_DEFAULT_RULES_PATH).get("default") or {}
    site_rules_all = _load_json(_SITE_RULES_PATH).get("sites") or {}
    site_rules: Dict[str, Any] = {}
    matched_site = ""
    host = (urlparse(url or "").hostname or "").lower()
    for key in _host_keys(url):
        if isinstance(site_rules_all.get(key), dict):
            site_rules = site_rules_all[key]
            matched_site = key
            break
    if not site_rules and host:
        for key in sorted(site_rules_all, key=len, reverse=True):
            key_l = str(key or "").lower()
            if host == key_l or host.endswith(f".{key_l}"):
                maybe_rules = site_rules_all.get(key)
                if isinstance(maybe_rules, dict):
                    site_rules = maybe_rules
                    matched_site = key
                    break

    merged = dict(default_rules)
    merged["matched_site"] = matched_site
    for list_key in ("title_selectors", "content_selectors"):
        default_values = list(default_rules.get(list_key) or [])
        site_values = list(site_rules.get(list_key) or [])
        seen = set()
        values: List[str] = []
        for item in site_values + default_values:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(text)
        merged[list_key] = values
    score = dict(default_rules.get("score") or {})
    score.update(site_rules.get("score") or {})
    merged["score"] = score
    return merged


def _text_from_node(node: Any) -> str:
    if node is None:
        return ""
    try:
        if getattr(node, "name", "") in {"h4", "h5"}:
            try:
                semantic = _class_id_text(node)
                ancestor_semantic = " ".join(_class_id_text(parent) for parent in list(getattr(node, "parents", []))[:4])
            except Exception:
                semantic = ""
                ancestor_semantic = ""
            if "view_tit" in semantic or "view_tit" in ancestor_semantic:
                text = _collapse_ws(node.get_text(" ", strip=True))
                stripped = re.sub(r"^\[[^\]]{1,20}\]\s*", "", text).strip()
                return stripped if len(stripped) >= 6 else text
        if getattr(node, "name", "") == "li":
            classes = node.get("class") or []
            if isinstance(classes, str):
                classes = classes.split()
            if "wide" in {str(cls) for cls in classes}:
                text = _collapse_ws(node.get_text(" ", strip=True))
                label_node = node.find(["strong", "b", "dt"])
                label = _collapse_ws(label_node.get_text(" ", strip=True)) if label_node is not None else ""
                if _flat(label) in {"기증자명", "기증자", "성명", "이름"} and label:
                    stripped = re.sub(r"^\s*" + re.escape(label) + r"\s*", "", text).strip()
                    return stripped if len(stripped) >= 2 else text
                return text
        if getattr(node, "name", "") == "caption":
            classes = node.get("class") or []
            if isinstance(classes, str):
                classes = classes.split()
            text = _collapse_ws(node.get_text(" ", strip=True))
            if "tit_article" in {str(cls) for cls in classes}:
                stripped = re.sub(r"^\[[^\]]{1,20}\]\s*", "", text).strip()
                return stripped if len(stripped) >= 6 else text
            return text
        if getattr(node, "name", "") == "meta":
            return _collapse_ws(node.get("content") or "")
        if getattr(node, "name", "") in {"input", "textarea"}:
            return _collapse_ws(node.get("value") or node.get_text(" ", strip=True))
        if getattr(node, "name", "") == "title":
            return _collapse_ws(node.string or node.get_text(" ", strip=True))
        return _collapse_ws(node.get_text(" ", strip=True))
    except Exception:
        return ""


def _is_label_value_row(node: Any) -> bool:
    try:
        label = (
            node.select_one(":scope > .th, :scope > th, :scope > dt")
            or node.find(["th", "dt"], recursive=False)
            or node.find(class_="th", recursive=False)
        )
        value = (
            node.select_one(":scope > .td, :scope > td, :scope > dd")
            or node.find(["td", "dd"], recursive=False)
            or node.find(class_="td", recursive=False)
        )
    except Exception:
        label = None
        value = None
    if label is None or value is None:
        return False
    return _flat(label.get_text(" ", strip=True)) in {"제목", "글제목", "게시글제목", "title", "subject"}


def _label_value_parts_safe(node: Any) -> tuple[Any, Any]:
    try:
        label = (
            node.select_one(":scope > .th, :scope > th, :scope > dt")
            or node.find(["th", "dt"], recursive=False)
            or node.find(class_="th", recursive=False)
        )
        value = (
            node.select_one(":scope > .td, :scope > td, :scope > dd")
            or node.find(["td", "dd"], recursive=False)
            or node.find(class_="td", recursive=False)
        )
    except Exception:
        return None, None
    return label, value


def _is_label_value_row(node: Any) -> bool:
    label, value = _label_value_parts_safe(node)
    if label is None or value is None:
        return False
    return _flat(label.get_text(" ", strip=True)) in {
        "제목",
        "글제목",
        "게시글제목",
        "행사명",
        "축제명",
        "공연명",
        "전시명",
        "강연명",
        "교육명",
        "사업명",
        "프로그램명",
        "프로그램",
        "title",
        "subject",
    }


def _label_text(node: Any) -> str:
    label, _ = _label_value_parts_safe(node)
    return _collapse_ws(label.get_text(" ", strip=True)) if label is not None else ""


def _label_value_text(node: Any) -> str:
    try:
        value = (
            node.select_one(":scope > .td, :scope > td, :scope > dd")
            or node.find(["td", "dd"], recursive=False)
            or node.find(class_="td", recursive=False)
        )
    except Exception:
        value = None
    return _text_from_node(value)


def _inside_any(node: Any, roots: Iterable[Any]) -> bool:
    root_ids = {id(root) for root in roots if root is not None}
    if id(node) in root_ids:
        return True
    try:
        return any(id(parent) in root_ids for parent in node.parents)
    except Exception:
        return False


def _class_id_text(node: Any) -> str:
    parts: List[str] = []
    try:
        classes = node.get("class") or []
        if isinstance(classes, str):
            classes = classes.split()
        parts.extend(str(cls) for cls in classes)
        parts.append(str(node.get("id") or ""))
    except Exception:
        pass
    return " ".join(parts).lower()


def _looks_breadcrumb(node: Any, text: str) -> bool:
    haystack = _class_id_text(node)
    if any(key in haystack for key in ("breadcrumb", "location", "path", "navigation", "nav")):
        return True
    return text.count(">") >= 2 or text.count(" 홈 ") >= 1


def _looks_menu_or_site_only(text: str) -> bool:
    compact = _flat(text)
    if compact in {
        "내비게이션메뉴",
        "네비게이션메뉴",
        "navigationmenu",
        "navmenu",
        "자료관리",
        "최상단메뉴",
        "전체메뉴",
        "하단메뉴영역",
        "주소연락처copyright",
        "보건소소개",
        "연관자료",
    }:
        return True
    if compact in {
        "홈",
        "목록",
        "상세보기",
        "게시판",
        "공지사항",
        "보도자료",
        "종로구청",
        "종로구의회",
        "안내메시지",
    }:
        return True
    return len(compact) <= 3 and not re.search(r"\d", compact)


def _first_body_sentence(content_roots: Iterable[Any]) -> str:
    for root in content_roots:
        text = _text_from_node(root)
        if not text:
            continue
        first = re.split(r"(?<=[.!?。！？])\s+|\n+", text, maxsplit=1)[0]
        first = _collapse_ws(first)
        if first:
            return first[:160]
    return ""


def _score_candidate(node: Any, selector: str, text: str, rules: Dict[str, Any], content_roots: List[Any]) -> TitleCandidate:
    weights = rules.get("score") or {}
    score = 0
    reasons: List[str] = []
    name = str(getattr(node, "name", "") or "").lower()
    selector_l = selector.lower()
    semantic = _class_id_text(node)

    if _inside_any(node, content_roots):
        score += int(weights.get("inside_post_area", 40))
        reasons.append("inside_post_area")
    if name in {"h1", "h2", "title"} or name == "caption" or "title" in selector_l or (
        name == "h4" and "view_tit" in selector_l
    ) or (
        name == "h5" and "view_tit" in selector_l
    ) or (
        name in {"h3", "h4"} and (
            "top_button_wrap" in selector_l
            or "content-tit" in selector_l
            or "sub-h4-tit" in selector_l
            or "current-h4" in selector_l
            or any(token in semantic for token in ("title", "subject", "tit"))
        )
    ) or (
        name == "th"
        and (
            "qanda-dview" in selector_l
            or str(node.get("scope") or "").strip().lower() == "colgroup"
        )
    ):
        score += int(weights.get("heading_or_title", 30))
        reasons.append("heading_or_title")
    if name == "caption" and "tit_article" in selector_l:
        score += 15
        reasons.append("caption_article_title")
    if "view_tit" in selector_l:
        score += 25
        reasons.append("view_title_container")
    if "imgboardview-header" in selector_l and "view-title" in selector_l:
        score += 45
        reasons.append("museum_collection_title")
    if "article_info" in selector_l and "li.wide" in selector_l:
        score += 35
        reasons.append("museum_article_info_wide")
    if "tit_page" in selector_l or "tit_page" in semantic:
        score -= 55
        reasons.append("page_section_title")
    if any(key in semantic or key in selector_l for key in ("title", "subject", "view", "tit")):
        score += int(weights.get("semantic_class_or_id", 25))
        reasons.append("semantic_class_or_id")
    if "top_button_wrap" in selector_l and name in {"h3", "h4"}:
        score += 15
        reasons.append("top_button_title")
    if name in {"h3", "h4"} and any(
        token in selector_l
        for token in (
            "sub-h4-tit",
            "content-tit",
            "current-h4",
        )
    ):
        score += 35
        reasons.append("site_detail_heading")
    if _is_label_value_row(node):
        score += int(weights.get("label_value_row", 35))
        reasons.append("label_value_row")
    label_flat = _flat(_label_text(node))
    text_flat = _flat(text)
    if label_flat in {"이전글", "다음글"} or text_flat.startswith(("이전글", "다음글")):
        score -= 90
        reasons.append("prev_next_row")

    length = len(text)
    if length < 4 or length > 160:
        score += int(weights.get("bad_length", -20))
        reasons.append("bad_length")
    if _looks_menu_or_site_only(text):
        score += int(weights.get("site_or_menu_only", -30))
        reasons.append("site_or_menu_only")
        if _flat(text) in {
            "자료관리",
            "최상단메뉴",
            "전체메뉴",
            "하단메뉴영역",
            "주소연락처copyright",
            "보건소소개",
        }:
            score -= 50
            reasons.append("admin_or_layout_title")
    first_sentence = _first_body_sentence(content_roots)
    if first_sentence and _flat(first_sentence) == _flat(text):
        score += int(weights.get("same_as_body_first_sentence", -10))
        reasons.append("same_as_body_first_sentence")
    if _looks_breadcrumb(node, text):
        score += int(weights.get("breadcrumb", -20))
        reasons.append("breadcrumb")

    return TitleCandidate(selector=selector, text=text, score=score, reasons=reasons)


def extract_title_with_scores(soup: Any, url: str = "") -> Dict[str, Any]:
    if soup is None:
        return {"url": url or "", "title": "", "title_selector": "", "title_score": 0, "title_candidates": []}

    rules = load_parser_rules(url)
    content_roots: List[Any] = []
    for selector in rules.get("content_selectors") or []:
        try:
            content_roots.extend(soup.select(selector)[:5])
        except Exception:
            continue
    if not content_roots:
        content_roots = [soup]

    candidates: List[TitleCandidate] = []
    seen_text_selector = set()
    for selector in rules.get("title_selectors") or []:
        try:
            nodes = soup.select(selector)
        except Exception:
            nodes = []
        for node in nodes[:20]:
            text = _label_value_text(node) if _is_label_value_row(node) else _text_from_node(node)
            text = _collapse_ws(text)
            if not text or _is_error_or_placeholder_title(text):
                continue
            key = (_flat(text), selector)
            if key in seen_text_selector:
                continue
            seen_text_selector.add(key)
            candidates.append(_score_candidate(node, selector, text, rules, content_roots))

    deduped: Dict[str, TitleCandidate] = {}
    for candidate in candidates:
        key = _flat(candidate.text)
        current = deduped.get(key)
        if current is None or candidate.score > current.score:
            deduped[key] = candidate
    ranked = sorted(deduped.values(), key=lambda item: item.score, reverse=True)
    best = ranked[0] if ranked else None
    return {
        "url": url or "",
        "title": best.text if best else "",
        "title_selector": best.selector if best else "",
        "title_score": best.score if best else 0,
        "matched_site": rules.get("matched_site") or "",
        "title_candidates": [
            {
                "selector": item.selector,
                "text": item.text,
                "score": item.score,
                "reasons": item.reasons,
            }
            for item in ranked[:20]
        ],
    }
