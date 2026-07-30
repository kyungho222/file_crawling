"""Hwaseong City board/photo archive parsing helpers."""

from __future__ import annotations

import html as html_lib
import logging
import re
from typing import Any, Dict, Optional, Tuple


logger = logging.getLogger(__name__)


def is_hscity_board_url(url: Optional[str]) -> bool:
    return "hscity.go.kr" in str(url or "").lower()


def is_hscity_photo_url(url: Optional[str]) -> bool:
    return "photo.hscity.go.kr" in str(url or "").lower()


def _normalize_hscity_label(value: Any) -> str:
    text = html_lib.unescape(str(value or ""))
    text = text.replace("\xa0", " ")
    return re.sub(r"[\s:\u203a>\[\]()/\\|\-]+", "", text).strip().lower()


def _clean_hscity_title(value: Any) -> str:
    text = html_lib.unescape(str(value or ""))
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = text.strip(" \t\r\n:\u203a>|")
    return text


def extract_hscity_board_title(soup: Any = None, *, html: str = "", url: Optional[str] = None) -> str:
    """Extract the real Hwaseong board title from the `th=title` table row."""
    if url and not is_hscity_board_url(url):
        return ""
    if soup is None:
        if not html:
            return ""
        try:
            from bs4 import BeautifulSoup
        except Exception:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return ""
    title_labels = {
        "제목",
        "글제목",
        "게시글제목",
        "title",
        "subject",
    }
    try:
        roots = list(soup.select("div.board_write, div.board_write_mt_18, div[class*='board_write']"))
    except Exception:
        roots = []
    if not roots:
        roots = [soup]

    for root in roots:
        try:
            rows = root.select("table tbody tr") or root.select("tr")
        except Exception:
            rows = []
        for row in rows:
            try:
                label_el = row.find(["th", "dt"], attrs={"scope": "row"}) or row.find(["th", "dt"])
            except Exception:
                label_el = None
            if label_el is None:
                continue
            label = _normalize_hscity_label(label_el.get_text(" ", strip=True))
            if label not in title_labels:
                continue
            try:
                value_el = label_el.find_next_sibling(["td", "dd"]) or row.find(["td", "dd"])
            except Exception:
                value_el = None
            if value_el is None:
                continue
            title = _clean_hscity_title(value_el.get_text(" ", strip=True))
            if title and len(title) >= 2 and _normalize_hscity_label(title) not in title_labels:
                return title
    return ""


def _normalize_hscity_photo_category_name(value: Any) -> str:
    text = html_lib.unescape(str(value or "")).strip().lower()
    text = text.replace("\xa0", " ")
    text = re.sub(r"[\s\u00b7\u318d\u30fb\uff65/\\|_\-:;,.()\[\]{}]+", "", text)
    return text


def extract_hscity_source_value(html: str, url: Optional[str] = None) -> Optional[str]:
    """Extract the source/attribution value from Hwaseong board detail HTML."""
    if not html or not is_hscity_board_url(url):
        return None
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None

    root = soup.select_one("div.board_write") or soup
    source_label = "출처"

    def _clean(raw: str) -> Optional[str]:
        text = re.sub(r"\s+", " ", str(raw or "")).strip(" \t\r\n:\u203a>-")
        if not text:
            return None
        text = re.sub(rf"^{source_label}\s*[:\u203a>\-]?\s*", "", text).strip()
        if not text or f"{source_label}표시" in text or "공공누리" in text:
            return None
        if len(text) > 80:
            return None
        return text

    try:
        for row in root.select("tr"):
            label_el = row.find(["th", "dt"])
            if label_el is None:
                continue
            label = re.sub(r"[\s:\u203a>\[\]()/\\-]+", "", label_el.get_text(" ", strip=True))
            if label != source_label:
                continue
            value_el = label_el.find_next_sibling(["td", "dd"]) or row.find(["td", "dd"])
            if value_el is None:
                continue
            value = _clean(value_el.get_text(" ", strip=True))
            if value:
                return value
    except Exception:
        pass

    try:
        strings = [re.sub(r"\s+", " ", s).strip() for s in root.stripped_strings]
    except Exception:
        strings = []
    for idx, token in enumerate(strings):
        label = re.sub(r"[\s:\u203a>\[\]()/\\-]+", "", token)
        if label == source_label and idx + 1 < len(strings):
            value = _clean(strings[idx + 1])
            if value:
                return value
        value = _clean(token)
        if value and re.match(rf"^{source_label}\s*[:\u203a>\-]?", token):
            return value
    return None


def extract_hscity_photo_category_path_candidates(html: str, url: Optional[str] = None) -> Tuple[Tuple[str, ...], ...]:
    """Return Hscity photo category path candidates in page order."""
    if not html or not is_hscity_photo_url(url):
        return tuple()
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return tuple()
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return tuple()

    category_label = "분류"

    def _clean_label(raw: Any) -> str:
        text = html_lib.unescape(str(raw or ""))
        text = text.replace("\xa0", " ").replace("\u203a", ">")
        text = re.sub(r"\s+", " ", text).strip()
        text = text.strip(" \t\r\n:\u203a>")
        text = re.sub(r"^[\u25b6\u25b7\u25b8\u25b9>\u318d\u00b7\-\s]+", "", text).strip()
        return text

    def _paths_from_text(raw: Any) -> list[Tuple[str, ...]]:
        text = html_lib.unescape(str(raw or ""))
        text = text.replace("\xa0", " ").replace("\u203a", ">")
        first_segment = text.split(">", 1)[0]
        if ":" in first_segment or "\u203a" in first_segment:
            text = text.split(":", 1)[-1] if ":" in first_segment else text.split("\u203a", 1)[-1]
        if text.strip().startswith(category_label):
            text = text.strip()[len(category_label) :]
        text = text.strip(" \t\r\n:\u203a>")
        segments = [part.strip() for part in re.split(r"[\u25b6\u25b7\u25b8\u25b9]+", text)]
        out: list[Tuple[str, ...]] = []
        for segment in segments:
            if ">" not in segment:
                continue
            parts = tuple(part for part in (_clean_label(p) for p in segment.split(">")) if part)
            if len(parts) >= 2:
                out.append(parts)
        return out

    candidates: list[Tuple[str, ...]] = []

    try:
        for item in soup.select(".title-info li, ul.title-info li, div.title-info li"):
            text = item.get_text(" ", strip=True)
            if category_label not in text and ">" not in text:
                continue
            candidates.extend(_paths_from_text(text))
    except Exception:
        pass

    if not candidates:
        try:
            strings = [_clean_label(s) for s in soup.stripped_strings]
        except Exception:
            strings = []
        for idx, token in enumerate(strings):
            if not token:
                continue
            compact = re.sub(r"[\s:\u203a>\[\]()/\\-]+", "", token)
            if compact == category_label:
                for nxt in strings[idx + 1 : idx + 8]:
                    paths = _paths_from_text(nxt)
                    if paths:
                        candidates.extend(paths)
                    elif candidates:
                        break
            elif token.startswith(category_label) and ">" in token:
                candidates.extend(_paths_from_text(token))

    if not candidates:
        logger.info(
            "[Cate][HscityPhoto][extract-path] no category path | url=%s html_len=%s",
            str(url or "")[:220],
            len(html or ""),
        )
        return tuple()
    return tuple(candidates)


def extract_hscity_photo_category_path_parts(html: str, url: Optional[str] = None) -> Tuple[str, ...]:
    """Return the selected Hscity photo category path, not only the last two names."""
    candidates = extract_hscity_photo_category_path_candidates(html, url)
    if not candidates:
        return tuple()
    selected = candidates[-1]
    logger.info(
        "[Cate][HscityPhoto][extract-path] selected | url=%s parts=%s candidates=%s",
        str(url or "")[:220],
        selected,
        list(candidates[-5:]),
    )
    return selected


def extract_hscity_photo_last_category_path(html: str, url: Optional[str] = None) -> Tuple[str, str]:
    """Backward-compatible pair extraction: keep the last two path names."""
    parts = extract_hscity_photo_category_path_parts(html, url)
    if len(parts) < 2:
        return ("", "")
    return (parts[-2], parts[-1])


async def resolve_hscity_photo_category_codes(
    *,
    html: str,
    url: Optional[str],
    chat_bot_id: Optional[str],
    db_name: Optional[str],
) -> Tuple[str, str]:
    """Resolve Hscity photo category codes by comparing parsed paths against CATEGORY."""
    path_candidates = extract_hscity_photo_category_path_candidates(html, url)
    path_parts = path_candidates[-1] if path_candidates else tuple()
    logger.info(
        "[Cate][HscityPhoto][resolve-path] parsed path | url=%s parts=%s candidates=%s chat_bot_id=%s db=%s",
        str(url or "")[:220],
        path_parts,
        path_candidates,
        str(chat_bot_id or "")[-12:],
        db_name,
    )
    if not path_candidates or not any(len(parts) >= 2 for parts in path_candidates) or not chat_bot_id or not db_name:
        return ("", "")

    from db.mariadb_save_update import get_category_table_name
    from db.mysql_db_config import mysql_execute_query

    table_name = get_category_table_name(str(chat_bot_id))

    async def _resolve_from_candidates(
        candidates: Tuple[Tuple[str, ...], ...],
        *,
        scope: str,
    ) -> Tuple[str, str]:
        unique_names = tuple(dict.fromkeys(part for parts in candidates for part in parts if part))
        if not unique_names:
            return ("", "")
        placeholders = ", ".join(["%s"] * len(unique_names))
        rows = await mysql_execute_query(
            f"""
            SELECT cate_code, cate_treecode, cate_name
            FROM `{table_name}`
            WHERE cate_use = 'y'
              AND cate_name IN ({placeholders})
            ORDER BY LENGTH(cate_treecode) ASC, cate_treecode ASC
            """,
            unique_names,
            fetch=True,
            dbname=str(db_name),
        )

        by_name: dict[str, list[dict[str, str]]] = {}
        by_norm_name: dict[str, list[dict[str, str]]] = {}
        for row in rows or []:
            name = str((row or {}).get("cate_name") or "").strip()
            code = str((row or {}).get("cate_code") or "").strip()
            tree = str((row or {}).get("cate_treecode") or "").strip()
            if not name or not code or not tree:
                continue
            item = {"code": code, "tree": tree, "name": name}
            by_name.setdefault(name, []).append(item)
            norm_name = _normalize_hscity_photo_category_name(name)
            if norm_name:
                by_norm_name.setdefault(norm_name, []).append(item)

        missing_exact_names = tuple(name for name in unique_names if name not in by_name)
        if missing_exact_names:
            try:
                fallback_rows = await mysql_execute_query(
                    f"""
                    SELECT cate_code, cate_treecode, cate_name
                    FROM `{table_name}`
                    WHERE cate_use = 'y'
                    ORDER BY LENGTH(cate_treecode) ASC, cate_treecode ASC
                    """,
                    fetch=True,
                    dbname=str(db_name),
                )
            except Exception:
                fallback_rows = []
            target_norms = {
                _normalize_hscity_photo_category_name(name)
                for name in missing_exact_names
                if _normalize_hscity_photo_category_name(name)
            }
            for row in fallback_rows or []:
                name = str((row or {}).get("cate_name") or "").strip()
                code = str((row or {}).get("cate_code") or "").strip()
                tree = str((row or {}).get("cate_treecode") or "").strip()
                norm_name = _normalize_hscity_photo_category_name(name)
                if not name or not code or not tree or norm_name not in target_norms:
                    continue
                item = {"code": code, "tree": tree, "name": name}
                by_name.setdefault(name, []).append(item)
                by_norm_name.setdefault(norm_name, []).append(item)

        logger.info(
            "[Cate][HscityPhoto][resolve-path] category lookup | table=%s scope=%s names=%s found=%s missing_exact=%s normalized_found=%s url=%s",
            table_name,
            scope,
            unique_names,
            {name: len(items) for name, items in by_name.items()},
            missing_exact_names,
            {
                name: len(by_norm_name.get(_normalize_hscity_photo_category_name(name), []))
                for name in missing_exact_names
            },
            str(url or "")[:220],
        )

        def _category_rows_for_name(name: str) -> list[dict[str, str]]:
            exact = by_name.get(name)
            if exact:
                return exact
            return by_norm_name.get(_normalize_hscity_photo_category_name(name), [])

        miss_paths: list[Tuple[str, ...]] = []
        for candidate_index, candidate_parts in reversed(list(enumerate(candidates))):
            if len(candidate_parts) < 2:
                continue
            matches: list[tuple[int, int, int, dict[str, str], dict[str, str]]] = []
            for left_idx, left_name in enumerate(candidate_parts):
                for right_idx in range(left_idx + 1, len(candidate_parts)):
                    right_name = candidate_parts[right_idx]
                    for parent in _category_rows_for_name(left_name):
                        parent_tree = parent["tree"]
                        for child in _category_rows_for_name(right_name):
                            child_tree = child["tree"]
                            if not child_tree.startswith(parent_tree) or len(child_tree) <= len(parent_tree):
                                continue
                            distance = right_idx - left_idx
                            adjacent_rank = 0 if distance == 1 else 1
                            matches.append((adjacent_rank, -left_idx, distance, parent, child))
            if not matches:
                miss_paths.append(candidate_parts)
                continue

            matches.sort(key=lambda item: (item[0], item[1], item[2]))
            _, _, _, parent, child = matches[0]
            logger.info(
                "[Cate][HscityPhoto][resolve-path] matched codes | cate1=%s cate2=%s parent=%r child=%r selected_index=%s parts=%s candidates=%s scope=%s url=%s",
                parent["code"],
                child["code"],
                parent["name"],
                child["name"],
                candidate_index,
                candidate_parts,
                candidates,
                scope,
                str(url or "")[:220],
            )
            return (parent["code"], child["code"])

        logger.info(
            "[Cate][HscityPhoto][resolve-path] no ordered pair match | table=%s scope=%s candidates=%s miss_paths=%s found_names=%s url=%s",
            table_name,
            scope,
            candidates,
            miss_paths,
            list(by_name.keys()),
            str(url or "")[:220],
        )
        return ("", "")

    preferred = (path_candidates[-1],)
    resolved = await _resolve_from_candidates(preferred, scope="preferred")
    if resolved != ("", ""):
        return resolved
    if len(path_candidates) > 1:
        resolved = await _resolve_from_candidates(path_candidates, scope="fallback_all")
        if resolved != ("", ""):
            return resolved
    return ("", "")


def apply_hscity_source_author_info(
    author_info: Optional[Dict[str, Any]],
    *,
    html: str,
    url: Optional[str] = None,
) -> Dict[str, Any]:
    """Apply Hwaseong source attribution as author/department metadata."""
    info: Dict[str, Any] = dict(author_info or {})
    source_value = extract_hscity_source_value(html, url)
    if not source_value:
        return info
    info.update(
        {
            "author": source_value,
            "department": source_value,
            "author_raw": source_value,
            "department_raw": source_value,
            "author_kind": "org",
            "_hscity_source_author": source_value,
        }
    )
    return info
