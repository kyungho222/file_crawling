from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.file.fast_attachment_extractor import (  # noqa: E402
    extract_fast_file_detail,
    infer_attachment_extension,
)


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "cp949", "euc-kr"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


async def _fetch(url: str, timeout: float) -> str:
    try:
        import aiohttp  # type: ignore[import-not-found]
    except Exception as exc:
        raise RuntimeError("URL 테스트에는 aiohttp가 필요합니다.") from exc
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text(errors="replace")


def _is_url(value: str) -> bool:
    return value.lower().startswith(("http://", "https://"))


def _attachment_url(item: dict[str, Any]) -> str:
    return str(item.get("href") or item.get("url") or item.get("download_url") or item.get("file_url") or "").strip()


def _attachment_name(item: dict[str, Any]) -> str:
    return str(item.get("name") or item.get("file_name") or item.get("filename") or item.get("title") or "attachment").strip()


async def _main() -> int:
    parser = argparse.ArgumentParser(description="파일크롤링 분리 앞단 첨부 URL 추출 테스트")
    parser.add_argument("target", help="상세 페이지 HTML 파일 경로 또는 URL")
    parser.add_argument("--base-url", default="", help="로컬 HTML일 때 기준 상세 URL")
    parser.add_argument("--json", action="store_true", help="JSON으로 출력")
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()

    target = str(args.target or "").strip()
    if _is_url(target):
        html = await _fetch(target, args.timeout)
        base_url = target
    else:
        path = Path(target)
        html = _read_text(path)
        base_url = str(args.base_url or path.resolve())

    result = extract_fast_file_detail(html, base_url)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(f"상세URL: {result.get('post_url') or base_url}")
    print(f"제목: {result.get('title') or '확인불가'}")
    print(f"등록일: {result.get('reg_date') or '확인불가'}")
    print(f"작성자: {result.get('author') or result.get('department') or '확인불가'}")
    print(f"첨부URL 수: {result.get('attachment_count') or 0}")
    for idx, item in enumerate(result.get("attachments") or [], start=1):
        if not isinstance(item, dict):
            continue
        name = _attachment_name(item)
        url = _attachment_url(item)
        ext = infer_attachment_extension(name, url) or "확인불가"
        print(f"[{idx}] 파일명: {name or '확인불가'}")
        print(f"[{idx}] 확장자: {ext}")
        print(f"[{idx}] 첨부URL: {url or '확인불가'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
