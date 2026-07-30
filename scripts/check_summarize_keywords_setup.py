#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
파일 크롤 후 summarize_keywords API 가 안 불릴 때 원인 점검용.

확인 항목:
  1) .env 로드 후 CHAT_BOT_ID / DEFAULT_CHAT_BOT_ID, DB_NAME, SUMMARIZE_* 환경변수
  2) 엔드포인트 URL 파싱 및 TCP 연결(호스트:포트)
  3) (선택) 운영과 동일 Request Body로 POST → HTTP 상태·응답 본문 앞부분

프로젝트 루트에서:
  python scripts/check_summarize_keywords_setup.py
  python scripts/check_summarize_keywords_setup.py --probe-post \\
    --contents-url \"https://.../fileDown.do?...\" \\
    --chat-bot-id <uuid> --db-name dev_user

파일 파이프라인에서 API는 대략 다음을 모두 만족할 때만 호출됩니다:
  - FileDownloadWorkflow (is_attachment_file_crawl_workflow)
  - 학습 파이프라인 성공 후 LEARN_LIST 해당 id 의 status = 'Y'
  - contents 로 넣을 다운로드 URL 이 비어 있지 않음
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

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


def _line(label: str, value: str, *, ok: Optional[bool] = None) -> None:
    mark = ""
    if ok is True:
        mark = "[OK] "
    elif ok is False:
        mark = "[!!] "
    print(f"{mark}{label}: {value}")


def _tcp_probe(host: str, port: int, timeout: float = 5.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "connected"
    except Exception as e:
        return False, str(e)


def _post_json(url: str, payload: Dict[str, Any], timeout_sec: float) -> Tuple[int, str]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
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
    p = argparse.ArgumentParser(description="summarize_keywords 호출 전제 조건·연결 점검")
    p.add_argument("--probe-post", action="store_true", help="실제 POST 시험")
    p.add_argument("--contents-url", default=os.getenv("SUMMARIZE_CONTENTS_URL", "").strip() or None)
    p.add_argument(
        "--chat-bot-id",
        default=(os.getenv("CHAT_BOT_ID") or os.getenv("DEFAULT_CHAT_BOT_ID") or "").strip() or None,
    )
    p.add_argument("--db-name", default=(os.getenv("DB_NAME") or "").strip() or None)
    args = p.parse_args()

    print("=== 1) 환경변수 (파일 summarize 경로와 동일 계열) ===")
    endpoint = (os.getenv("SUMMARIZE_KEYWORDS_URL") or "").strip() or (
        "https://dev.chatbaram.com:9001/summarize_keywords"
    )
    ct = (os.getenv("SUMMARIZE_KEYWORDS_CONTENT_TYPE") or "url").strip() or "url"
    try:
        to = float((os.getenv("SUMMARIZE_KEYWORDS_TIMEOUT_SEC") or "120").strip() or "120")
    except ValueError:
        to = 120.0

    _line("SUMMARIZE_KEYWORDS_URL", endpoint or "(empty)", ok=bool(endpoint))
    _line("SUMMARIZE_KEYWORDS_CONTENT_TYPE", ct, ok=True)
    _line("SUMMARIZE_KEYWORDS_TIMEOUT_SEC", str(to), ok=True)
    _line("CHAT_BOT_ID", (os.getenv("CHAT_BOT_ID") or "(unset)").strip()[:80])
    _line("DEFAULT_CHAT_BOT_ID", (os.getenv("DEFAULT_CHAT_BOT_ID") or "(unset)").strip()[:80])
    cb = (args.chat_bot_id or "").strip()
    _line("effective chat_bot_id (--probe-post 시)", cb or "(none)", ok=bool(cb))
    db = (args.db_name or "").strip()
    _line("DB_NAME", (os.getenv("DB_NAME") or "(unset)").strip()[:40])
    _line("effective db_name (--probe-post 시)", db or "(none)", ok=bool(db))
    _line("SUMMARIZE_CONTENTS_URL", (os.getenv("SUMMARIZE_CONTENTS_URL") or "(unset)")[:120])

    print("\n=== 2) 엔드포인트 TCP (방화벽/DNS 대략 확인) ===")
    try:
        u = urllib.parse.urlparse(endpoint)
        host = u.hostname or ""
        port = u.port or (443 if u.scheme == "https" else 80)
        _line("parsed host", host, ok=bool(host))
        _line("parsed port", str(port), ok=True)
        if host:
            ok, msg = _tcp_probe(host, port, timeout=5.0)
            _line("TCP connect", msg, ok=ok)
        else:
            _line("TCP connect", "skip (no host)", ok=False)
    except Exception as e:
        _line("parse/connect error", str(e), ok=False)

    print("\n=== 3) 파일 파이프라인에서 요약 API가 안 탈 때 흔한 이유 ===")
    print(
        "  - LEARN_LIST status 가 아직 'Y' 가 아님 → _run_learning_pipeline 직후 조회에서 will_post=False"
    )
    print("  - pre_learn_list_id 없음 / chat_bot_id·db_name 없음 → 게이트 블록 스킵")
    print("  - contents URL 비어 있음 → post_summarize 조기 return")
    print("  - aiohttp 미설치 → [Summary][file] aiohttp 없음 로그")
    print("  로그 검색: [Summary][file] after_learn gate | will_post=")
    print("            [Summary][file] post_summarize")

    if not args.probe_post:
        print("\n(끝) 실제 POST까지 보려면: --probe-post --contents-url \"https://...\" [--chat-bot-id ... --db-name ...]")
        return 0

    print("\n=== 4) probe POST (운영과 동일 JSON Body) ===")
    cu = (args.contents_url or "").strip()
    if not cu:
        print("[!!] --contents-url 또는 SUMMARIZE_CONTENTS_URL 필요", file=sys.stderr)
        return 2
    if not cb or not db:
        print("[!!] --chat-bot-id 와 --db-name (또는 .env) 필요", file=sys.stderr)
        return 2

    payload: Dict[str, Any] = {
        "chat_bot_id": str(cb),
        "db_name": str(db),
        "contents": [cu],
        "content_type": str(ct),
        "concurrency": 3,
    }

    print("request_body:", json.dumps(payload, ensure_ascii=False)[:2000])
    status, body = _post_json(endpoint, payload, timeout_sec=to)
    print(f"HTTP status: {status}")
    print("response_head:", (body[:1500] + ("…" if len(body) > 1500 else "")))
    return 0 if status == 200 else 3


if __name__ == "__main__":
    raise SystemExit(main())
