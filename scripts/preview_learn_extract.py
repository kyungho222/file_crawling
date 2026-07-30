#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
다운로드(또는 로컬) 파일에 대해 운영 학습과 동일한 추출 규칙으로 텍스트를 미리 본다.

edu.learn_file_plain_text.extract_plain_text_like_learn_modules 를 사용하며,
게시판 file_content_workflow._extract_text_from_saved_file_for_learning 과 동일한 본문이다.

지원: .txt .hwp .hwpx .pdf .xls .xlsx .csv .doc .docx .ppt .pptx .jpg .jpeg .png .gif .bmp
  - PDF: learn_modules process_pdf 와 같은 페이지별 OCR·text_data 규칙(UPSTAGE_API_KEY 등)
  - 이미지: img_edu OCR API (Config.UPSTAGE_*)

사용 예:
  python scripts/preview_learn_extract.py downloads/some.pdf
  python scripts/preview_learn_extract.py path/to/dir --recursive
  python scripts/preview_learn_extract.py a.hwp --full
  python scripts/preview_learn_extract.py doc.pdf --chunks
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import List, Optional, Tuple

# 프로젝트 루트
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from edu.learn_file_plain_text import (
    LEARN_PLAIN_TEXT_EXTS,
    extract_plain_text_like_learn_modules,
)

_DEFAULT_EXTS = set(LEARN_PLAIN_TEXT_EXTS)


def _collect_files(paths: List[str], *, recursive: bool) -> List[str]:
    out: List[str] = []
    for p in paths:
        ap = os.path.abspath(os.path.expanduser(p))
        if os.path.isfile(ap):
            out.append(ap)
        elif os.path.isdir(ap):
            if recursive:
                for root, _, files in os.walk(ap):
                    for fn in files:
                        out.append(os.path.join(root, fn))
            else:
                for fn in os.listdir(ap):
                    fp = os.path.join(ap, fn)
                    if os.path.isfile(fp):
                        out.append(fp)
        else:
            print(f"[skip] 없음: {ap}", file=sys.stderr)
    return sorted(set(out))


def _filter_ext(paths: List[str], allowed: Optional[set]) -> List[str]:
    if not allowed:
        return paths
    r: List[str] = []
    for p in paths:
        ext = os.path.splitext(p)[1].lower()
        if ext in allowed:
            r.append(p)
    return r


async def extract_text_like_file_pipeline(file_path: str) -> Tuple[str, str]:
    """
    file_content_workflow._extract_text_from_saved_file_for_learning 과 동일(learn_file_plain_text).
    반환: (추출 텍스트, 확장자 소문자)
    """
    path = os.path.abspath(os.path.expanduser(file_path))
    ext = os.path.splitext(path)[1].lower()

    if not os.path.isfile(path):
        return "", ext

    if ext not in LEARN_PLAIN_TEXT_EXTS:
        return "", ext

    t = await extract_plain_text_like_learn_modules(path, personal_info_filter="N")
    return (t or "").strip(), ext


def _split_chunks_like_learning(text: str) -> List[str]:
    from backend.config import Config
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    if not (text or "").strip():
        return []
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=Config.BASIC_CHUNK_SIZE,
        chunk_overlap=Config.BASIC_CHUNK_OVERLAP,
    )
    return splitter.split_text(text)


async def _run_one(
    path: str,
    *,
    show_chunks: bool,
    full: bool,
    max_preview: int,
) -> int:
    ext = os.path.splitext(path)[1].lower()
    if ext not in _DEFAULT_EXTS:
        print(f"[skip] 지원 확장자 아님 ({ext}): {path}")
        return 0

    print("\n" + "=" * 80)
    print(f"파일: {path}")
    print(f"확장자: {ext}")
    print("=" * 80)

    try:
        text, _ = await extract_text_like_file_pipeline(path)
    except Exception as e:
        print(f"[오류] 추출 실패: {e}")
        return 1

    n = len(text)
    print(f"추출 길이: {n} 문자 (공백 포함)")
    if not text:
        print("→ 비어 있음 (스캔본·암호·손상·형식 미지원 등)")
        return 0

    if show_chunks:
        chunks = _split_chunks_like_learning(text)
        print(f"학습용 청크 수 (BASIC_CHUNK_SIZE 기준): {len(chunks)}")
        for i, ch in enumerate(chunks[:50], 1):
            prev = ch[:400] + ("…" if len(ch) > 400 else "")
            print(f"--- chunk {i}/{len(chunks)} ({len(ch)}자) ---\n{prev}\n")
        if len(chunks) > 50:
            print(f"... 이하 {len(chunks) - 50}개 청크 생략")

    if full or n <= max_preview:
        print("\n--- 추출 전문 ---\n")
        print(text)
    else:
        print(f"\n--- 추출 미리보기 (앞 {max_preview}자, 전체는 --full) ---\n")
        print(text[:max_preview])
        print(f"\n... [{n - max_preview}자 생략]")

    return 0


async def _async_main(args: argparse.Namespace) -> int:
    files = _collect_files(args.paths, recursive=args.recursive)
    if args.ext:
        allowed = {e if e.startswith(".") else f".{e}" for e in args.ext}
        allowed = {e.lower() for e in allowed}
    else:
        allowed = set(_DEFAULT_EXTS)
    files = _filter_ext(files, allowed)
    if not files:
        print("처리할 파일이 없습니다.", file=sys.stderr)
        return 2

    rc = 0
    for fp in files:
        r = await _run_one(
            fp,
            show_chunks=args.chunks,
            full=args.full,
            max_preview=args.max_preview,
        )
        if r != 0:
            rc = r
    return rc


def main() -> None:
    p = argparse.ArgumentParser(
        description="파일 크롤 학습과 동일한 규칙으로 텍스트 추출 미리보기",
    )
    p.add_argument("paths", nargs="+", help="파일 또는 디렉터리 경로")
    p.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="디렉터리일 때 하위까지 탐색",
    )
    p.add_argument(
        "--ext",
        nargs="*",
        default=None,
        help="처리할 확장자만 (예: pdf hwp). 미지정 시 기본 학습 대상 확장자만",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="긴 텍스트도 잘라내지 않고 전부 출력",
    )
    p.add_argument(
        "--max-preview",
        type=int,
        default=12000,
        metavar="N",
        help="--full 이 아닐 때 최대 N자만 출력 (기본 12000)",
    )
    p.add_argument(
        "--chunks",
        action="store_true",
        help="추출 후 Config.BASIC_CHUNK_SIZE 기준 청크 미리보기",
    )
    args = p.parse_args()
    raise SystemExit(asyncio.run(_async_main(args)))


if __name__ == "__main__":
    main()
