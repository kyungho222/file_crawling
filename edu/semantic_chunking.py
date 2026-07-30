from __future__ import annotations

import os
import re
from typing import List


_HEADING_RE = re.compile(r"^(#{1,3})\s+(.+?)\s*$")


def semantic_chunking_enabled() -> bool:
    return str(os.getenv("SEMANTIC_CHUNKING_ENABLED", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def looks_like_markdown(text: str) -> bool:
    if not text:
        return False
    head_hits = len(re.findall(r"(?m)^#{1,3}\s+\S", text))
    table_hits = len(re.findall(r"(?m)^\s*\|.+\|\s*$", text))
    list_hits = len(re.findall(r"(?m)^\s*(?:[-*+]|\d+[.)])\s+\S", text))
    return head_hits >= 1 or table_hits >= 2 or list_hits >= 3


def _recursive_split(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except Exception:
        try:
            from langchain.text_splitter import RecursiveCharacterTextSplitter  # type: ignore
        except Exception:
            return [text[i : i + chunk_size] for i in range(0, len(text), max(1, chunk_size))]

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )
    return splitter.split_text(text)


def _markdown_blocks(markdown: str) -> List[str]:
    blocks: List[str] = []
    current: List[str] = []
    in_table = False

    def flush() -> None:
        nonlocal current, in_table
        text = "\n".join(current).strip()
        if text:
            blocks.append(text)
        current = []
        in_table = False

    for raw in str(markdown or "").splitlines():
        line = raw.rstrip()
        stripped = line.strip()

        if _HEADING_RE.match(stripped):
            flush()
            current.append(stripped)
            flush()
            continue

        is_table_line = stripped.startswith("|") and stripped.endswith("|")
        if in_table and not is_table_line:
            flush()
        if is_table_line:
            in_table = True
            current.append(line)
            continue

        if not stripped:
            flush()
            continue

        current.append(line)

    flush()
    return blocks


def split_markdown_semantically(
    markdown: str,
    *,
    chunk_size: int,
    chunk_overlap: int,
) -> List[str]:
    if not semantic_chunking_enabled():
        return _recursive_split(markdown, chunk_size, chunk_overlap)

    blocks = _markdown_blocks(markdown)
    if not blocks:
        return _recursive_split(markdown, chunk_size, chunk_overlap)

    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    active_heading = ""

    def push_current() -> None:
        nonlocal current, current_len
        text = "\n\n".join(current).strip()
        if text:
            chunks.append(text)
        current = []
        current_len = 0

    for block in blocks:
        if _HEADING_RE.match(block.strip()):
            active_heading = block.strip()
            if current:
                push_current()
            current.append(active_heading)
            current_len = len(active_heading)
            continue

        block_len = len(block)
        if block_len > chunk_size:
            if current:
                push_current()
            prefix = f"{active_heading}\n\n" if active_heading else ""
            for part in _recursive_split(block, chunk_size, chunk_overlap):
                part_text = (prefix + part).strip() if prefix and not part.startswith(active_heading) else part
                if part_text:
                    chunks.append(part_text)
            continue

        projected = current_len + block_len + (2 if current else 0)
        if current and projected > chunk_size:
            push_current()
            if active_heading and not block.startswith(active_heading):
                current.append(active_heading)
                current_len = len(active_heading)

        current.append(block)
        current_len += block_len + 2

    if current:
        push_current()

    return [c for c in chunks if c.strip()]


def split_text_semantically_if_markdown(
    text: str,
    *,
    chunk_size: int,
    chunk_overlap: int,
) -> List[str]:
    if semantic_chunking_enabled() and looks_like_markdown(text):
        return split_markdown_semantically(
            text,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
    return _recursive_split(text, chunk_size, chunk_overlap)
