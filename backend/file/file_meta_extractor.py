"""
Shared helpers for extracting downloadable file links and metadata from HTML.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List
from urllib.parse import unquote, urljoin, urlparse

from config.constants import ARCHIVE_EXTENSIONS, DOC_EXTENSIONS
from backend.board.anseong_file import (
    clean_anseong_attachment_name,
    is_anseong_file_url,
    resolve_anseong_yhlib_download_url,
)
from backend.board.gm_file import extract_gm_nftc_filelist_attachments
from backend.shared.file_name_debug import emit_file_name_debug
from utils.file import strip_fallback_download_label
from utils.url import extract_download_url_from_js, normalize_attachment_href

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


logger = logging.getLogger("backend.file.file_meta_extractor")


def _filename_debug_log(location: str, **data: Any) -> None:
    emit_file_name_debug(component="extract", location=location, data=data, logger=logger)

SUPPORTED_EXTENSIONS = {
    ext.lstrip(".").lower()
    for ext in ((DOC_EXTENSIONS or []) + (ARCHIVE_EXTENSIONS or []))
}
SUPPORTED_EXTENSIONS_ORDERED = tuple(sorted(SUPPORTED_EXTENSIONS))
FILE_URL_HINTS = (
    "filedown",
    "filedownload",
    "download",
    "attach",
    "attachment",
    "atchfile",
    "atchfileid",
    "fileid",
)
FILENAME_RE = re.compile(
    r"(?i)([^\r\n\t<>]{{1,200}}\.(?:{}))(?=$|[\s\]\)\}},;:])".format(
        "|".join(map(re.escape, SUPPORTED_EXTENSIONS_ORDERED))
    )
) if SUPPORTED_EXTENSIONS_ORDERED else re.compile(r"$^")


def extract_file_extension(filename: str) -> str:
    """Return a normalized extension without the leading dot."""
    if not filename:
        return ""

    try:
        decoded_name = unquote(filename)
    except Exception:
        decoded_name = filename

    known_exts = sorted(SUPPORTED_EXTENSIONS, key=len, reverse=True)
    if known_exts:
        match = re.search(
            r"(?i)\.({})(?=$|[\s\]\)\}},;:])".format("|".join(map(re.escape, known_exts))),
            decoded_name,
        )
        if match:
            return match.group(1).lower()

    parts = decoded_name.rsplit(".", 1)
    if len(parts) == 2:
        ext = parts[1].lower()
        ext = ext.split("?")[0].split("#")[0]
        return ext
    return ""


def is_supported_file_type(filename: str, url: str = "") -> bool:
    """Check whether the filename or URL points to a supported file type."""
    ext = extract_file_extension(filename)
    if ext in SUPPORTED_EXTENSIONS:
        return True

    if url:
        url_ext = extract_file_extension(url)
        if url_ext in SUPPORTED_EXTENSIONS:
            return True

    return False


def extract_filename_from_url(url: str) -> str:
    """Extract the trailing filename-like path segment from a URL."""
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        filename = (parsed.path or "").split("/")[-1]
        decoded = unquote(filename)
        return decoded.split("?")[0].split("#")[0]
    except Exception:
        return ""


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _clean_filename_candidate(value: str) -> str:
    cleaned = _normalize_text(value)
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s*\[[^\]]+\]\s*$", "", cleaned)
    cleaned = strip_fallback_download_label(cleaned) or cleaned
    return cleaned.strip()


def _extract_filename_from_text(value: str) -> str:
    cleaned = _clean_filename_candidate(value)
    if not cleaned:
        return ""
    if is_supported_file_type(cleaned):
        return cleaned
    match = FILENAME_RE.search(cleaned)
    if match:
        return _clean_filename_candidate(match.group(1))
    return ""


def _extract_filename_from_context(tag: Any, *tokens: str) -> str:
    try:
        parent = getattr(tag, "parent", None)
        context = parent.get_text(" ", strip=True) if parent is not None else ""
    except Exception:
        context = ""
    context = _normalize_text(context)
    if not context:
        return ""
    for token in tokens:
        if token:
            context = context.replace(token, " ")
    context = _normalize_text(context)
    match = FILENAME_RE.search(context)
    if not match:
        return ""
    return _clean_filename_candidate(match.group(1))


def _looks_like_download_url(url: str) -> bool:
    lower = (url or "").lower()
    return any(hint in lower for hint in FILE_URL_HINTS)


def _looks_like_preview(url: str, text: str = "") -> bool:
    raw = f"{url} {text}".lower()
    return "preview" in raw and "download" not in raw


def _resolve_candidate_url(raw_url: str, base_url: str = "", onclick: str = "") -> str:
    candidate = normalize_attachment_href(raw_url or "")
    if candidate and candidate.lower().startswith("javascript:"):
        resolved = extract_download_url_from_js(candidate, base_url) or ""
        candidate = normalize_attachment_href(resolved)
    if (not candidate or candidate.startswith("#")) and onclick:
        resolved = extract_download_url_from_js(onclick, base_url) or ""
        if not resolved:
            resolved = resolve_anseong_yhlib_download_url(onclick, base_url) or ""
        candidate = normalize_attachment_href(resolved)
    if not candidate:
        return ""
    if base_url and not candidate.lower().startswith(("http://", "https://", "javascript:")):
        try:
            candidate = urljoin(base_url, candidate)
        except Exception:
            pass
    return candidate


def _append_unique_file_link(
    out: List[Dict[str, Any]],
    seen_urls: set[str],
    *,
    url: str,
    filename: str,
    source_text: str = "",
) -> None:
    clean_url = str(url or "").strip()
    if not clean_url or clean_url in seen_urls:
        return
    if clean_url.endswith("#"):
        return
    seen_urls.add(clean_url)
    _filename_debug_log(
        "append_unique_file_link",
        url=clean_url,
        filename=filename,
        source_text=source_text,
    )
    out.append(
        {
            "url": clean_url,
            "filename": filename or extract_filename_from_url(clean_url),
            "extension": extract_file_extension(filename) or extract_file_extension(clean_url),
            "source_text": source_text.strip(),
            "type": "file",
        }
    )


def extract_file_download_links(html_content: str, base_url: str = "") -> List[Dict[str, Any]]:
    """Extract downloadable file links from HTML."""
    if not html_content:
        return []

    file_links: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    try:
        gm_items = extract_gm_nftc_filelist_attachments(html_content, base_url)
        for item in gm_items:
            filename = item.get("name") or extract_filename_from_url(item.get("href") or "")
            _append_unique_file_link(
                file_links,
                seen_urls,
                url=item.get("href") or "",
                filename=filename,
                source_text=filename,
            )
        if gm_items:
            return file_links

        if BeautifulSoup:
            try:
                soup = BeautifulSoup(html_content, "html.parser")  # type: ignore[arg-type]
            except Exception:
                soup = None
            if soup is not None:
                for tag in soup.find_all(["a", "input", "button"]):
                    try:
                        raw_url = (
                            tag.get("href")
                            or tag.get("data-href")
                            or tag.get("data-url")
                            or ""
                        ).strip()
                    except Exception:
                        raw_url = ""
                    try:
                        onclick = (tag.get("onclick") or "").strip()
                    except Exception:
                        onclick = ""
                    try:
                        title_attr = (tag.get("title") or "").strip()
                    except Exception:
                        title_attr = ""
                    try:
                        value_attr = (tag.get("value") or "").strip()
                    except Exception:
                        value_attr = ""
                    try:
                        link_text = (tag.get_text(" ", strip=True) or "").strip()
                    except Exception:
                        link_text = ""

                    full_url = _resolve_candidate_url(raw_url, base_url=base_url, onclick=onclick)
                    if not full_url or _looks_like_preview(full_url, link_text):
                        continue

                    if is_anseong_file_url(base_url):
                        filename = (
                            clean_anseong_attachment_name(title_attr)
                            or clean_anseong_attachment_name(link_text)
                            or clean_anseong_attachment_name(value_attr)
                        )
                    else:
                        filename = ""
                    if not filename:
                        filename = (
                            _extract_filename_from_text(title_attr)
                            or _extract_filename_from_text(link_text)
                            or _extract_filename_from_text(value_attr)
                            or _extract_filename_from_context(tag, title_attr, link_text, value_attr)
                            or extract_filename_from_url(full_url)
                        )
                    if not filename:
                        continue
                    if not (is_supported_file_type(filename, full_url) or (_looks_like_download_url(full_url) and extract_file_extension(filename))):
                        continue

                    _append_unique_file_link(
                        file_links,
                        seen_urls,
                        url=full_url,
                        filename=filename,
                        source_text=link_text or value_attr or title_attr,
                    )
                    _filename_debug_log(
                        "dom_candidate_selected",
                        url=full_url,
                        filename=filename,
                        link_text=link_text,
                        title_attr=title_attr,
                        value_attr=value_attr,
                    )

        patterns = [
            r'<a[^>]+href=["\']([^"\']+\.(pdf|xls|xlsx|ppt|pptx|hwp|hwpx))["\'][^>]*>([^<]+)</a>',
            r'<a[^>]+(?:class|id)=["\'][^"\']*download[^"\']*["\'][^>]+href=["\']([^"\']+)["\'][^>]*>([^<]*)</a>',
            r'<a[^>]+onclick=["\'][^"\']*download[^"\']*["\'][^>]*href=["\']?([^"\'>\s]+)["\']?[^>]*>([^<]*)</a>',
            r'<input[^>]+type=["\']?(button|submit)["\']?[^>]+onclick=["\'][^"\']*download[^"\']*["\'][^>]+value=["\']([^"\']+)["\']',
            r'<a[^>]+href=["\']([^"\']*(?:file|download|attach)[^"\']*)["\'][^>]*>([^<]*(?:file|download|attach|attachment)[^<]*)</a>',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            for match in matches:
                if len(match) == 2:
                    url, text = match
                elif len(match) == 1:
                    url = match[0]
                    text = ""
                else:
                    continue

                if not str(url or "").strip() or str(url or "").strip().startswith("#"):
                    continue
                full_url = _resolve_candidate_url(url, base_url=base_url)
                filename = _extract_filename_from_text(text) or extract_filename_from_url(full_url)
                if not full_url or not filename or _looks_like_preview(full_url, text):
                    continue
                if not is_supported_file_type(filename, full_url):
                    continue

                _append_unique_file_link(
                    file_links,
                    seen_urls,
                    url=full_url,
                    filename=filename,
                    source_text=text.strip(),
                )
                _filename_debug_log(
                    "regex_candidate_selected",
                    url=full_url,
                    filename=filename,
                    source_text=text.strip(),
                )

        return file_links
    except Exception as exc:
        logger.error("Error extracting file links: %s", exc)
        return []


def extract_file_metadata(file_info: Dict[str, Any], page_url: str = "") -> Dict[str, Any]:
    """Normalize file metadata into a predictable shape."""
    if not file_info:
        return {}

    url = file_info.get("url", "")
    filename = file_info.get("filename", "")
    if not filename:
        filename = extract_filename_from_url(url)

    extension = extract_file_extension(filename) or extract_file_extension(url)
    file_size = file_info.get("filesize", 0)

    return {
        "url": url,
        "filename": filename,
        "extension": extension,
        "filesize": file_size,
        "type": file_info.get("type", "file"),
        "source_page": page_url,
        "source_text": file_info.get("source_text", ""),
        "detected_at": file_info.get("detected_at", ""),
        "is_supported": extension.lower() in SUPPORTED_EXTENSIONS if extension else False,
    }


def categorize_file_by_extension(extension: str) -> str:
    """Map a file extension to a lightweight category."""
    if not extension:
        return "unknown"

    ext = extension.lower()
    if ext in ["pdf", "hwp", "hwpx", "ppt", "pptx"]:
        return "document"
    if ext in ["xls", "xlsx"]:
        return "spreadsheet"
    return "other"


def create_file_scan_result(url: str, filename: str, source_page: str = "", **kwargs) -> Dict[str, Any]:
    """Create a standard file-scan payload."""
    extension = extract_file_extension(filename)

    return {
        "url": url,
        "filename": filename,
        "filesize": kwargs.get("filesize", 0),
        "ext": extension,
        "type": kwargs.get("type", "file"),
        "source_page": source_page,
        "category": categorize_file_by_extension(extension),
        "created_at": kwargs.get("created_at", ""),
        "unique_key": kwargs.get("unique_key", ""),
        "post_date": kwargs.get("post_date", ""),
        "is_supported": extension.lower() in SUPPORTED_EXTENSIONS if extension else False,
    }
