# -*- coding: utf-8 -*-
from __future__ import annotations

import html as html_lib
import os
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

from backend.board.anseong_file import resolve_anseong_yhlib_download_url
from backend.board.yongin_board import resolve_yongin_file_download_url
from backend.file.site_config import load_file_site_config
from utils.attachment_url_normalize import canonicalize_attachment_url_for_learn_list
from utils.attachment_display_name import is_generated_attachment_storage_name
from utils.file import parse_display_file_size_bytes, strip_fallback_download_label, strip_trailing_file_size
from utils.url import canonicalize_url_for_dedup, extract_download_url_from_js, normalize_attachment_href


FILE_EXTS = (
    ".hwpx", ".hwp", ".xlsx", ".xls", ".pptx", ".ppt", ".docx", ".doc",
    ".pdf", ".csv", ".txt", ".zip", ".rar", ".7z", ".jpg", ".jpeg",
    ".png", ".gif", ".bmp", ".webp", ".tif", ".tiff",
)
DOWNLOAD_HINTS = (
    "downloadbbsfile", "filedown", "filedownload", "file_down", "download.do",
    "download.jsp", "nd_filedownload.do", "filedown.do", "filedown.jsp",
    "filedownnew.jsp", "atchfile", "atchfileid", "atchmnfl", "atchmnflno",
    "filesn", "fileid", "file_id", "file_sn", "/cmm/fms/filedown.do",
    "/common/file/download.do", "/component/file/nd_filedownload.do",
    "/file/download/", "/download/uu/",
    "downloadcontentsfile.do", "downloadcontentsfile", "menucntfile",
    "process.file.do", "/other/attach/process.file.do",
)
PREVIEW_HINTS = ("previewbbs", "previewmenucntfile", "previewmenu", "htmlconv.do", "preview.jsp", "convert.jsp", "/convert/", "viewer", "tts")
ATTACH_ROOT_SELECTORS = (
    ".p-attach", ".file_area", ".file-list", ".file_list", ".fileList",
    ".attach", ".attachment", ".bbs_file", ".bbs-file", ".filebox",
    ".fileBox", "[class*='attach']", "[id*='attach']", "[class*='file']",
)
TITLE_SELECTORS = (
    "meta[property='og:title']", "meta[name='title']", ".p-table__subject_text",
    ".p-table__subject", ".platform-board__detail .title", ".view_title",
    ".view-title", ".detail_tit", ".detail-title", ".subject", "h1", "h2",
)
DATE_RE = re.compile(
    r"(20\d{2}|19\d{2})\D{0,4}(1[0-2]|0?[1-9])\D{0,4}(3[01]|[12]\d|0?[1-9])"
)
FILE_NAME_RE = re.compile(
    r"([^\s\"'<>]{1,220}\.(?:hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|txt|zip|rar|7z|jpg|jpeg|png|gif|bmp|webp|tif|tiff))",
    re.IGNORECASE,
)



ATTACHMENT_HINT_RE = re.compile(
    r"(?:download|downloadcontentsfile|menucntfile|filedown|filedownload|atch|attach|"
    r"첨부|다운로드|파일|"
    r"sys_file|file[_-]?(?:id|sn|no)|atchmnfl|atchfile|"
    r"\.(?:hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|txt|zip|rar|7z))",
    re.IGNORECASE,
)


def _large_html_threshold() -> int:
    try:
        return max(0, int(os.getenv("FILE_FAST_ATTACHMENT_LIGHT_SCAN_BYTES", "120000") or "120000"))
    except Exception:
        return 120000


def _attachment_scan_html(html: str) -> str:
    """Return a small attachment-focused HTML slice for large mostly-empty pages."""
    text = str(html or "")
    threshold = _large_html_threshold()
    if not threshold or len(text) <= threshold:
        return text
    priority_re = re.compile(
        r"(?:nd_filedownload\.do|q_filesn|q_fileid|file_down|call_viewer|첨부파일)",
        re.IGNORECASE,
    )
    priority_matches = list(priority_re.finditer(text))
    generic_matches = list(ATTACHMENT_HINT_RE.finditer(text))
    if not priority_matches and not generic_matches:
        return ""
    matches = priority_matches + generic_matches
    try:
        window = max(1000, min(int(os.getenv("FILE_FAST_ATTACHMENT_LIGHT_SCAN_WINDOW", "5000") or "5000"), 20000))
    except Exception:
        window = 5000
    chunks: List[str] = []
    last_start = -1
    last_end = -1
    for match in matches[:80]:
        start = max(0, match.start() - window)
        end = min(len(text), match.end() + window)
        if chunks and start <= last_end:
            chunks[-1] = text[last_start:end]
        else:
            chunks.append(text[start:end])
            last_start = start
        last_end = end
    return "\n".join(chunks)
@dataclass
class FastAttachment:
    href: str
    name: str
    post_url: str
    download_name: str = ""
    reason: str = "href"
    source: str = "fast_file_front"
    needs_response_validation: bool = False
    declared_file_size_bytes: int = 0
    # Only populate this from an explicit byte-oriented HTML attribute.  The
    # displayed "15 KB" label is rounded and must not drive duplicate skips.
    exact_file_size_bytes: int = 0

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["kind"] = self.source
        out["url"] = self.href
        return out


def _text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _node_text(node: Any) -> str:
    try:
        return _text(node.get_text(" ", strip=True))
    except Exception:
        return ""


def _node_exact_file_size_bytes(node: Any) -> int:
    """Return a source-provided exact byte value, never a display-size guess."""
    for attr in ("data-file-size-bytes", "data-byte-size"):
        try:
            raw = str(node.get(attr) or "").strip()
        except Exception:
            raw = ""
        if re.fullmatch(r"[1-9]\d{0,15}", raw):
            try:
                return int(raw)
            except ValueError:
                return 0
    return 0


def _compact(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"[\s:\-\[\]\(\)\.]+", "", text)


def infer_attachment_extension(*values: Any) -> str:
    blob = " ".join(str(v or "") for v in values)
    m = re.search(
        r"(?i)\.(hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|txt|zip|rar|7z|jpg|jpeg|png|gif|bmp|webp|tif|tiff)(?=$|[\s\]\)\}},;:&?#])",
        blob,
    )
    return f".{m.group(1).lower()}" if m else ""


def _clean_name(value: Any) -> str:
    name = strip_trailing_file_size(_text(value))
    if "+" in name:
        name = re.sub(r"\++", " ", name)
    name = strip_fallback_download_label(name) or name
    name = re.sub(r"(?i)\s*(?:download|view|open)\s*$", "", name).strip()
    return name.strip(" -_|:")


def _is_generic_attachment_name(value: Any) -> bool:
    key = _compact(value)
    return key in {
        "attachment",
        "attachedfile",
        "file",
        "download",
        "filedownload",
        "view",
        "preview",
        "open",
        "save",
        "첨부",
        "첨부파일",
        "파일",
        "다운로드",
        "파일다운로드",
        "내려받기",
        "받기",
        "바로보기",
        "미리보기",
    }


def _contextual_title_name(node: Any, *remove_values: Any) -> str:
    remove_tokens = [str(v or "").strip() for v in remove_values if str(v or "").strip()]
    selectors = (
        "a[href*='view.do']",
        "a[href*='detail.do']",
        "a[href*='read.do']",
        "a.img",
        ".subject a",
        ".title a",
        "[class*='subject'] a",
        "[class*='title'] a",
        ".subject",
        ".title",
        "[class*='subject']",
        "[class*='title']",
    )
    try:
        containers = list(getattr(node, "parents", []) or [])[:8]
    except Exception:
        containers = []
    for container in containers:
        if str(getattr(container, "name", "") or "").lower() not in {"li", "div", "tr", "td", "dl", "p"}:
            continue
        for selector in selectors:
            try:
                candidates = container.select(selector)
            except Exception:
                candidates = []
            for candidate in candidates:
                text = _clean_name(_node_text(candidate))
                for token in remove_tokens:
                    text = text.replace(token, " ")
                text = _clean_name(text)
                if text and not _is_generic_attachment_name(text) and 2 <= len(text) <= 180:
                    return text
        text = _node_text(container)
        for token in remove_tokens:
            text = text.replace(token, " ")
        text = strip_fallback_download_label(_clean_name(text))
        if text and not _is_generic_attachment_name(text) and 2 <= len(text) <= 180:
            return text
    return ""


def _name_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query or "")
        for key in ("user_file_nm", "file_nm", "filename", "fileName", "orgFileNm"):
            for raw in qs.get(key) or []:
                name = _clean_name(unquote(raw))
                if name:
                    return name
        tail = unquote((parsed.path or "").rstrip("/").rsplit("/", 1)[-1])
        return _clean_name(tail) if infer_attachment_extension(tail) else ""
    except Exception:
        return ""


def _is_download_url(url: str, *, additional_hints: Optional[List[str]] = None) -> bool:
    low = str(url or "").lower()
    if not low or any(h in low for h in PREVIEW_HINTS):
        return False
    configured_hints = tuple(
        str(hint or "").strip().lower()
        for hint in (additional_hints or [])
        if str(hint or "").strip()
    )
    return (
        any(hint in low for hint in DOWNLOAD_HINTS)
        or any(hint in low for hint in configured_hints)
        or bool(infer_attachment_extension(low))
    )


def _is_noise_attachment_asset(href: Any, name: Any = "") -> bool:
    low_href = str(href or "").strip().lower()
    low_name = str(name or "").strip().lower()
    compact_name = _compact(low_name)
    if not low_href and not low_name:
        return False
    if compact_name == "rfc2350" and "atchfileid=file_000000000070941" in low_href:
        return True
    if "img_wa" in low_href or "webwatch" in low_href:
        return True
    if "wa 품질인증" in low_name or "웹와치" in low_name or "webwatch" in low_name:
        return True
    return False

def _is_noise_container(node: Any) -> bool:
    cur = node
    for _ in range(8):
        if cur is None:
            return False
        try:
            attrs: List[str] = []
            for attr in ("id", "class", "role", "aria-label"):
                val = cur.get(attr)
                attrs.extend([str(x) for x in val] if isinstance(val, list) else ([str(val)] if val else []))
            blob = " ".join(attrs).lower()
            if any(x in blob for x in ("attach", "attachment", "file-list", "file_list", "filebox", "file-box", "filedown", "download")):
                return False
            if str(getattr(cur, "name", "") or "").lower() in {"nav", "header", "footer", "aside"}:
                return True
            if any(x in blob for x in ("gnb", "lnb", "snb", "menu", "nav", "breadcrumb", "footer", "header", "quick", "sns", "share", "depth", "side", "family-site", "family_site", "related-site", "related_site", "major-site", "major_site", "shortcut", "site-link", "site_link")):
                return True
        except Exception:
            pass
        cur = getattr(cur, "parent", None)
    return False


def _attachment_label(value: str) -> bool:
    key = _compact(value)
    labels = {
        "첨부",
        "첨부파일",
        "파일",
        "붙임",
        "다운로드",
    }
    return key in labels or "첨부파일" in key


def _find_attachment_roots(soup: Any) -> List[Any]:
    roots: List[Any] = []
    seen: set[int] = set()

    def add(root: Any) -> None:
        if root is None or _is_noise_container(root):
            return
        ident = id(root)
        if ident not in seen:
            seen.add(ident)
            roots.append(root)

    for sel in ATTACH_ROOT_SELECTORS:
        try:
            for root in soup.select(sel):
                blob = (_node_text(root) + " " + str(root)[:2000]).lower()
                if _attachment_label(blob) or any(h in blob for h in DOWNLOAD_HINTS) or infer_attachment_extension(blob):
                    add(root)
        except Exception:
            pass
    try:
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            if cells and _attachment_label(_node_text(cells[0])):
                add(cells[1] if len(cells) > 1 else row)
    except Exception:
        pass
    try:
        for dt in soup.find_all("dt"):
            if _attachment_label(_node_text(dt)):
                add(dt.find_next_sibling("dd") or dt.parent)
    except Exception:
        pass
    return roots


def _config_string_list(site_config: Optional[Dict[str, Any]], key: str) -> List[str]:
    raw = site_config.get(key) if isinstance(site_config, dict) else None
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [str(value).strip() for value in raw if str(value or "").strip()]


def _candidate_nodes(soup: Any, *, site_config: Optional[Dict[str, Any]] = None) -> List[Any]:
    nodes: List[Any] = []
    seen: set[int] = set()

    def add(node: Any) -> None:
        ident = id(node)
        if ident not in seen:
            seen.add(ident)
            nodes.append(node)

    for selector in _config_string_list(site_config, "attachment_selectors"):
        try:
            for node in soup.select(selector):
                add(node)
        except Exception:
            continue

    roots = _find_attachment_roots(soup)
    for root in roots:
        try:
            for node in root.find_all(["a", "button", "input"], recursive=True):
                add(node)
        except Exception:
            pass
    try:
        for node in soup.find_all(["a", "button", "input"]):
            if not _is_noise_container(node):
                add(node)
    except Exception:
        pass
    return nodes


def _resolve_egov_file_down_onclick(onclick: str, base_url: str) -> str:
    text = html_lib.unescape(str(onclick or "")).strip()
    if not text:
        return ""
    match = re.search(
        r"(?i)\b(?:fileDown|fnFileDown|downloadFile)\s*\(\s*['\"](?P<atch>FILE_[A-Za-z0-9_\-]+)['\"]\s*,\s*['\"]?(?P<sn>\d+)['\"]?\s*(?:,\s*['\"](?P<bbs>[^'\"]{1,80})['\"])?",
        text,
    )
    if not match:
        return ""
    query = {"atchFileId": match.group("atch"), "fileSn": match.group("sn")}
    bbs_id = str(match.group("bbs") or "").strip()
    if bbs_id:
        query["bbsId"] = bbs_id
    return urljoin(base_url, "/common/cmm/fms/FileDown.do?" + urlencode(query))

def _resolve_url(
    raw: str,
    onclick: str,
    base_url: str,
    *,
    page_script: str = "",
    download_url_hints: Optional[List[str]] = None,
) -> tuple[str, bool, str]:
    raw = normalize_attachment_href(raw or "")
    onclick = str(onclick or "").strip()
    egov_file_down = _resolve_egov_file_down_onclick(onclick, base_url)
    if egov_file_down:
        return egov_file_down, True, "onclick_fileDown"
    for value, reason in ((raw, "href"), (onclick, "onclick")):
        if not value:
            continue
        resolved = (
            resolve_yongin_file_download_url(value, base_url)
            or extract_download_url_from_js(value, base_url, page_script=page_script)
            or resolve_anseong_yhlib_download_url(value, base_url)
        )
        if resolved and _is_download_url(resolved, additional_hints=download_url_hints):
            return resolved, True, reason
    if not raw or raw.startswith("#") or raw.lower().startswith(("javascript:", "mailto:", "tel:")):
        return "", False, ""
    try:
        full = urljoin(base_url, raw)
    except Exception:
        full = raw
    return (
        (full, False, "href")
        if _is_download_url(full, additional_hints=download_url_hints)
        else ("", False, "")
    )


def _candidate_name(
    node: Any,
    href: str,
    *,
    name_attributes: Optional[List[str]] = None,
) -> str:
    values = []
    download_name = ""
    try:
        download_name = _clean_name(node.get("download") or "")
        attributes = []
        for attribute in list(name_attributes or []) + ["download", "title", "aria-label", "alt", "value"]:
            key = str(attribute or "").strip()
            if key and key not in attributes:
                attributes.append(key)
        values.extend([node.get(attribute) or "" for attribute in attributes])
        values.append(_node_text(node))
    except Exception:
        pass
    href_ext = infer_attachment_extension(href)
    # The site-provided <a download="..."> value is the authoritative
    # attachment title. Do not replace it with nearby card labels or href.
    if download_name and not is_generated_attachment_storage_name(download_name):
        if infer_attachment_extension(download_name):
            return download_name
        if href_ext:
            return f"{download_name}{href_ext}"
    if any(is_generated_attachment_storage_name(value) for value in values) or is_generated_attachment_storage_name(_name_from_url(href)):
        contextual = _contextual_title_name(node, *values)
        if contextual:
            return contextual if not href_ext or contextual.lower().endswith(href_ext) else f"{contextual}{href_ext}"
    for value in values:
        name = _clean_name(value)
        if name and infer_attachment_extension(name):
            return name
    try:
        for container in list(getattr(node, "parents", []) or [])[:5]:
            if str(getattr(container, "name", "") or "").lower() not in {"li", "tr", "td", "dl", "div"}:
                continue
            for selector in ("p", ".file-name", ".file_name", ".filename", ".name", "span"):
                for candidate in container.select(selector):
                    text = _clean_name(_node_text(candidate))
                    if text and infer_attachment_extension(text) and 2 <= len(text) <= 220:
                        return text
    except Exception:
        pass

    if href_ext:
        for value in values:
            name = strip_fallback_download_label(_clean_name(value))
            key = _compact(name)
            if (
                name
                and not _is_generic_attachment_name(name)
                and key not in {"pdf", "hwp", "hwpx", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "zip"}
                and 2 <= len(name) <= 180
            ):
                return name if name.lower().endswith(href_ext) else f"{name}{href_ext}"
    try:
        title_selectors = (
            ".emphasis_subject",
            ".file",
            ".file_subject",
            ".file-title",
            ".file_title",
            ".subject",
            "[class*='subject']",
            "[class*='title']",
        )
        for container in list(getattr(node, "parents", []) or [])[:8]:
            if str(getattr(container, "name", "") or "").lower() not in {"div", "li", "tr", "dl", "p", "td"}:
                continue
            for selector in title_selectors:
                candidate = container.select_one(selector)
                if candidate is not None:
                    cleaned = strip_fallback_download_label(_clean_name(_node_text(candidate)))
                    if cleaned and not _is_generic_attachment_name(cleaned):
                        return cleaned
    except Exception:
        pass

    try:
        m = FILE_NAME_RE.search(_node_text(node.parent))
        if m:
            return _clean_name(m.group(1))
    except Exception:
        pass
    for value in values:
        name = _clean_name(value)
        if name:
            cleaned = strip_fallback_download_label(name)
            if cleaned and not _is_generic_attachment_name(cleaned):
                return cleaned

    try:
        for container_name in ("tr", "li", "dl", "div", "p"):
            container = node.find_parent(container_name)
            if container is None:
                continue
            ctx = _node_text(container)
            for value in values:
                if value:
                    ctx = ctx.replace(str(value), " ")
            cleaned = strip_fallback_download_label(_clean_name(ctx))
            if cleaned and not _is_generic_attachment_name(cleaned):
                return cleaned
    except Exception:
        pass
    contextual = _contextual_title_name(node, *values)
    if contextual:
        ext = infer_attachment_extension(*values, href)
        if ext and not contextual.lower().endswith(ext):
            contextual = f"{contextual}{ext}"
        return contextual
    return _name_from_url(href) or "attachment"


def _form_value_by_name_or_id(soup: Any, key: str) -> str:
    key = str(key or "").strip()
    if not key:
        return ""
    selectors = (f"#{key}", f"[name='{key}']")
    for selector in selectors:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if node is None:
            continue
        try:
            value = node.get("value")
        except Exception:
            value = ""
        if value is not None:
            return str(value or "").strip()
    return ""


def _script_text_blob(soup: Any) -> str:
    chunks: List[str] = []
    try:
        for script in soup.find_all("script"):
            text = script.string
            if text is None:
                text = script.get_text(" ", strip=False)
            if text:
                chunks.append(str(text))
    except Exception:
        pass
    return "\n".join(chunks)


def _node_inline_file_id(node: Any) -> str:
    keys = (
        "data-file-id", "data-fileid", "data-file_id", "data-file-sn", "data-filesn",
        "data-file_sn", "data-atch-file-id", "data-atchfileid", "fileId", "fileid", "id",
    )
    for key in keys:
        try:
            value = str(node.get(key) or "").strip()
        except Exception:
            value = ""
        if not value or not re.fullmatch(r"[A-Za-z0-9_\-]+", value):
            continue
        if key.lower() == "id" and not (
            value.upper().startswith("FILE_")
            or value.isdigit()
            or re.search(r"(?i)(file|atch)", value)
        ):
            continue
        return value
    return ""


def _looks_like_scripted_attachment_node(node: Any) -> bool:
    try:
        raw_href = str(node.get("href") or node.get("data-href") or "").strip()
    except Exception:
        raw_href = ""
    if raw_href and raw_href not in {"#", "javascript:;", "javascript:void(0)", "javascript:void(0);"}:
        return False
    text = _node_text(node)
    parent_text = _node_text(node.find_parent(class_=re.compile(r"(?i)(file|attach)")) or node.parent)
    if infer_attachment_extension(text) or _attachment_label(parent_text) or infer_attachment_extension(parent_text):
        return True
    return False


def _is_file_id_param(value: str) -> bool:
    return bool(re.search(r"(?i)(fileid|file_id|filesn|file_sn|fileseq|fileno|atchfileid|atchfilesn)", str(value or "")))

def _download_param_name_from_script(nearby: str) -> str:
    patterns = (
        r"[?&]([A-Za-z0-9_]*(?:fileId|fileID|file_id|fileSn|fileSN|file_sn|fileSeq|fileSEQ|fileNo|fileNO|atchFileId|atchFileSn)[A-Za-z0-9_]*)\s*=",
        r"['\"]\s*[+]?\s*['\"]?[&?]([A-Za-z0-9_]*(?:fileid|file_id|filesn|file_sn|fileseq|fileno|atchfileid|atchfilesn)[A-Za-z0-9_]*)\s*=",
    )
    for pattern in patterns:
        m = re.search(pattern, nearby, re.IGNORECASE)
        if m:
            return m.group(1)
    return "fileId"


def _complete_trailing_param_value(url: str, soup: Any, file_id: str = "") -> str:
    try:
        m = re.search(r"([?&])([^?&=]+)=$", url)
        if not m:
            return url
        param = m.group(2)
        if _is_file_id_param(param):
            return url + str(file_id or "").strip()
        return url + _form_value_by_name_or_id(soup, param)
    except Exception:
        return url


def _append_query_param(url: str, key: str, value: str) -> str:
    if not key or not value:
        return url
    low = url.lower()
    if re.search(rf"(?:[?&]){re.escape(key.lower())}=", low):
        return url
    sep = "&" if "?" in url and not url.endswith(("?", "&")) else ""
    if "?" not in url:
        sep = "?"
    elif url.endswith(("?", "&")):
        sep = ""
    return url + sep + urlencode({key: value})


def _resolve_scripted_file_download(node: Any, base_url: str, soup: Any, script_blob: str) -> str:
    try:
        if not _looks_like_scripted_attachment_node(node):
            return ""
        file_id = _node_inline_file_id(node)
        if not file_id:
            return ""
        blob = str(script_blob or "")
        if not re.search(r"(?i)(filedownload|filedown|download\.do|download\.jsp)", blob):
            return ""
        if not re.search(r"(?i)(fileid|file_id|filesn|file_sn|fileseq|fileno|atchfileid|atchfilesn)", blob):
            return ""
        for match in re.finditer(r"(['\"])(?P<part>[^'\"]*(?:fileDownload|filedownload|fileDown|filedown|download\.do|download\.jsp)[^'\"]*)\1", blob, re.IGNORECASE):
            part = html_lib.unescape(match.group("part") or "").strip()
            if not part or part.startswith(("&", "?")):
                continue
            nearby = blob[max(0, match.start() - 250): min(len(blob), match.end() + 500)]
            if not re.search(r"(?i)(fileid|file_id|filesn|file_sn|fileseq|fileno|atchfileid|atchfilesn)", nearby):
                continue
            url = _complete_trailing_param_value(urljoin(base_url, part), soup, file_id)
            param_name = _download_param_name_from_script(nearby)
            return _append_query_param(url, param_name, file_id)
    except Exception:
        return ""
    return ""

def _dedup_key(url: str) -> str:
    return canonicalize_attachment_url_for_learn_list(url) or canonicalize_url_for_dedup(url) or url.lower()


def extract_fast_attachments(
    html: str,
    base_url: str,
    *,
    force_full_scan: bool = False,
    site_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    if not html or BeautifulSoup is None:
        return []
    # A configured selector is a site-level parsing contract. Read it before
    # applying the generic lightweight HTML slice so its target cannot be cut
    # out of a large page.
    resolved_site_config = site_config if isinstance(site_config, dict) else load_file_site_config(base_url)
    configured_attachment_selectors = _config_string_list(resolved_site_config, "attachment_selectors")
    scan_html = (
        str(html or "")
        if force_full_scan or configured_attachment_selectors
        else _attachment_scan_html(html)
    )
    if not scan_html:
        return []
    try:
        soup = BeautifulSoup(scan_html, "html.parser")  # type: ignore[operator]
    except Exception:
        return []
    name_attributes = _config_string_list(resolved_site_config, "attachment_name_attributes")
    attachment_url_hints = _config_string_list(resolved_site_config, "attachment_url_hints")
    out: List[FastAttachment] = []
    seen: set[str] = set()
    script_blob = _script_text_blob(soup)
    for node in _candidate_nodes(soup, site_config=resolved_site_config):
        try:
            raw = node.get("href") or node.get("data-href") or node.get("data-url") or node.get("data-download-url") or node.get("formaction") or ""
            onclick = node.get("onclick") or ""
        except Exception:
            continue
        href, needs_validation, reason = _resolve_url(
            str(raw or ""),
            str(onclick or ""),
            base_url,
            page_script=script_blob,
            download_url_hints=attachment_url_hints,
        )
        if not href:
            scripted_href = _resolve_scripted_file_download(node, base_url, soup, script_blob)
            if scripted_href:
                href = scripted_href
                needs_validation = True
                reason = "scripted_file_download"
            else:
                continue
        name = _candidate_name(node, href, name_attributes=name_attributes)
        try:
            download_name = _clean_name(node.get("download") or "")
        except Exception:
            download_name = ""
        if _is_noise_attachment_asset(href, name):
            continue
        if not (
            _is_download_url(href, additional_hints=attachment_url_hints)
            or infer_attachment_extension(name)
        ):
            continue
        key = _dedup_key(href)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            FastAttachment(
                href=href,
                name=name,
                post_url=base_url,
                download_name=download_name,
                reason=reason,
                needs_response_validation=needs_validation,
                declared_file_size_bytes=parse_display_file_size_bytes(_node_text(node)) or 0,
                exact_file_size_bytes=_node_exact_file_size_bytes(node),
            )
        )
    if not out:
        anchor_re = re.compile(
            r"<a\b[^>]*\bhref\s*=\s*(['\"])(?P<href>.*?)\1[^>]*>(?P<body>.*?)</a>",
            re.IGNORECASE | re.DOTALL,
        )
        for match in anchor_re.finditer(scan_html):
            raw = html_lib.unescape(match.group("href") or "")
            href, needs_validation, reason = _resolve_url(
                raw,
                "",
                base_url,
                page_script=script_blob,
                download_url_hints=attachment_url_hints,
            )
            if not href:
                continue
            body = html_lib.unescape(re.sub(r"<[^>]+>", " ", match.group("body") or ""))
            name = _clean_name(body) or _name_from_url(href)
            if _is_noise_attachment_asset(href, name):
                continue
            if not (
                _is_download_url(href, additional_hints=attachment_url_hints)
                or infer_attachment_extension(name)
            ):
                continue
            key = _dedup_key(href)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(FastAttachment(href=href, name=name or "attachment", post_url=base_url, reason=f"regex_{reason or 'href'}", needs_response_validation=needs_validation, declared_file_size_bytes=parse_display_file_size_bytes(body) or 0))
    return [x.to_dict() for x in out]


def _labeled_pairs(soup: Any) -> List[tuple[str, str]]:
    pairs: List[tuple[str, str]] = []
    try:
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            if len(cells) >= 2:
                pairs.append((_node_text(cells[0]), _node_text(cells[1])))
    except Exception:
        pass
    try:
        for dl in soup.find_all("dl"):
            for dt, dd in zip(dl.find_all("dt", recursive=False), dl.find_all("dd", recursive=False)):
                pairs.append((_node_text(dt), _node_text(dd)))
    except Exception:
        pass
    return pairs


def _find_value(soup: Any, labels: tuple[str, ...]) -> str:
    keys = {_compact(x) for x in labels}
    for label, value in _labeled_pairs(soup):
        key = _compact(label)
        if key and (key in keys or any((x and len(x) >= 2 and key.endswith(x)) for x in keys)):
            return _text(value)
    return ""


def _date(value: str) -> str:
    m = DATE_RE.search(_text(value))
    if not m:
        return _text(value)[:40]
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _find_first_detail_date(soup: Any) -> str:
    for sel in ("#board", ".bbs__view", ".board_view", ".view", ".p-table"):
        try:
            node = soup.select_one(sel)
        except Exception:
            node = None
        if not node:
            continue
        found = _date(_node_text(node))
        if found:
            return found
    return ""


def extract_fast_file_detail(html: str, base_url: str) -> Dict[str, Any]:
    attachments = extract_fast_attachments(html, base_url)
    return {
        "post_url": base_url,
        "title": "",
        "reg_date": "",
        "author": "",
        "department": "",
        "attachment_count": len(attachments),
        "attachments": attachments,
    }
