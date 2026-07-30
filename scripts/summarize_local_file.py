#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
로컬 파일을 운영 파일 파이프라인과 동일 규칙으로 텍스트 추출한 뒤,
`backend/board/file_content_workflow.py` 의 `_file_crawl_post_summarize_keywords` 와
**동일한 Request Body**로 summarize_keywords API를 호출해 응답을 출력합니다.

운영 로그 예 (`post_summarize will_send` 와 같은 필드):
  endpoint=https://dev.chatbaram.com:9001/summarize_keywords
  timeout_sec=120  chat_bot_id=<UUID>  db_name=dev_user  content_type=url
  concurrency=3  contents_count=1
  contents_preview=['https://.../fileDown.do?menuNo=...&atchFileId=...&fileSn=...']

`--contents-url` 에는 위 **contents_preview** 와 같은 **다운로드/원문 URL**을 그대로 넣으면 됩니다.
(공공기관 `portal/.../fileDown.do` 등 — 요약 서버가 해당 URL로 GET 할 수 있어야 함)

README.md: 프로젝트 루트에서 `python scripts/...` 실행.

환경변수 (인자가 우선):
  SUMMARIZE_KEYWORDS_URL, SUMMARIZE_KEYWORDS_CONTENT_TYPE, SUMMARIZE_KEYWORDS_TIMEOUT_SEC
  CHAT_BOT_ID 또는 DEFAULT_CHAT_BOT_ID, DB_NAME
  SUMMARIZE_CONTENTS_URL

`.env` 자동 로드: 프로젝트 루트 `.env`, `backend/.env` (있으면)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from dotenv import load_dotenv

    for _env_path in (
        os.path.join(_PROJECT_ROOT, "backend", ".env"),
        os.path.join(_PROJECT_ROOT, ".env"),
    ):
        if os.path.isfile(_env_path):
            load_dotenv(_env_path, override=False)
except Exception:
    pass


def _env_chat_bot_id() -> str | None:
    for k in ("CHAT_BOT_ID", "DEFAULT_CHAT_BOT_ID"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


def _env_db_name() -> str | None:
    v = (os.getenv("DB_NAME") or "").strip()
    return v or None


async def _extract_like_file_pipeline(path: str) -> tuple[str, str]:
    from edu.learn_file_plain_text import LEARN_PLAIN_TEXT_EXTS, extract_plain_text_like_learn_modules

    ap = os.path.abspath(os.path.expanduser(path))
    ext = os.path.splitext(ap)[1].lower()
    if not os.path.isfile(ap):
        return "", ext
    if ext not in LEARN_PLAIN_TEXT_EXTS:
        return "", ext
    t = await extract_plain_text_like_learn_modules(ap, personal_info_filter="N")
    return (t or "").strip(), ext


def _post_summarize(
    *,
    endpoint: str,
    payload: Dict[str, Any],
    timeout_sec: float,
) -> tuple[int, str]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return int(getattr(resp, "status", 200) or 200), raw
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = str(e)
        return int(e.code), raw
    except Exception as e:
        return -1, str(e)


def main() -> int:
    _epilog = r"""
예시 (로그의 contents_preview URL을 그대로 --contents-url 에):

  python scripts/summarize_local_file.py .\downloads\some.pdf ^
    --chat-bot-id 204cc79d-10ec-453a-beea-479e6e05af4d --db-name dev_user ^
    --contents-url "https://gwangjin.go.kr/portal/cmmn/file/fileDown.do?menuNo=200190&atchFileId=...&fileSn=1" ^
    --print-request
"""
    p = argparse.ArgumentParser(
        description="로컬 파일 추출(file 파이프라인 동일) + summarize_keywords API (운영 will_send 와 동일 Body)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_epilog,
    )
    p.add_argument("file", help="로컬 파일 경로 (추출 미리보기용; API의 contents URL과 다를 수 있음)")
    p.add_argument("--chat-bot-id", default=_env_chat_bot_id())
    p.add_argument("--db-name", default=_env_db_name())
    p.add_argument(
        "--contents-url",
        default=(os.getenv("SUMMARIZE_CONTENTS_URL") or "").strip() or None,
        help="API contents[]에 넣을 URL(원격에서 GET 가능해야 함). 없으면 API 스킵",
    )
    p.add_argument("--extract-only", action="store_true", help="텍스트 추출만 하고 API 호출 안 함")
    p.add_argument("--print-request", action="store_true", help="전송 직전 Request Body JSON 출력")
    p.add_argument("--preview-chars", type=int, default=800, help="추출 텍스트 미리보기 최대 글자")
    args = p.parse_args()

    file_path = args.file
    extracted, ext = asyncio.run(_extract_like_file_pipeline(file_path))

    print("=== 로컬 추출 (file 파이프라인과 동일 모듈) ===")
    print(f"path: {os.path.abspath(os.path.expanduser(file_path))}")
    print(f"ext: {ext!r}  chars: {len(extracted)}")
    if extracted:
        prev = extracted[: max(0, args.preview_chars)]
        print("--- preview ---")
        print(prev + ("…" if len(extracted) > len(prev) else ""))
        print("--- end preview ---")
    else:
        print("(추출 결과 없음 또는 미지원 확장자 — edu.learn_file_plain_text.LEARN_PLAIN_TEXT_EXTS 참고)")

    if args.extract_only:
        return 0

    if not args.chat_bot_id or not args.db_name:
        print(
            "\n[skip API] --chat-bot-id 와 --db-name "
            "(또는 CHAT_BOT_ID / DEFAULT_CHAT_BOT_ID, DB_NAME)가 필요합니다.",
            file=sys.stderr,
        )
        return 1

    contents_url = (args.contents_url or "").strip()
    if not contents_url:
        print(
            "\n[skip API] summarize 엔드포인트는 contents에 URL이 필요합니다. "
            "--contents-url 또는 환경변수 SUMMARIZE_CONTENTS_URL 을 지정하세요.",
            file=sys.stderr,
        )
        return 2

    endpoint = (os.getenv("SUMMARIZE_KEYWORDS_URL") or "").strip() or (
        "https://dev.chatbaram.com:9001/summarize_keywords"
    )
    try:
        timeout_sec = float((os.getenv("SUMMARIZE_KEYWORDS_TIMEOUT_SEC") or "120").strip() or "120")
    except ValueError:
        timeout_sec = 120.0
    content_type = (os.getenv("SUMMARIZE_KEYWORDS_CONTENT_TYPE") or "url").strip() or "url"

    payload: Dict[str, Any] = {
        "chat_bot_id": str(args.chat_bot_id),
        "db_name": str(args.db_name),
        "contents": [contents_url],
        "content_type": str(content_type),
        "concurrency": 3,
    }

    print("\n=== summarize_keywords 호출 (요약 API 본문 계약) ===")
    print(
        "[Summary][file] post_summarize will_send | "
        f"endpoint={endpoint} timeout_sec={timeout_sec} "
        f"chat_bot_id={args.chat_bot_id} db_name={args.db_name} "
        f"content_type={content_type} concurrency=3 "
        f"contents_count=1 contents_preview={[contents_url[:120] + ('…' if len(contents_url) > 120 else '')]!r}"
    )

    if args.print_request:
        print("request_body:", json.dumps(payload, ensure_ascii=False))

    status, body = _post_summarize(endpoint=endpoint, payload=payload, timeout_sec=timeout_sec)
    print(f"\nHTTP status: {status}")
    print("--- response body ---")
    try:
        data = json.loads(body)
        print(json.dumps(data, ensure_ascii=False, indent=2))
    except json.JSONDecodeError:
        print(body[:8000] + ("…" if len(body) > 8000 else ""))

    return 0 if status == 200 else 2


if __name__ == "__main__":
    raise SystemExit(main())
