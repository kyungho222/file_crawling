#!/usr/bin/env python3
"""
Register pre-explored URLs as a crawl job (POST to local backend).

Usage:
  python scripts/register_pre_explored.py \
      --file backend/shared/pre_explored_urls.json \
      --db dev_user \
      --chatbot dev_bot \
      --colle file

If --file is omitted the script will try:
 - backend/shared/pre_explored_urls.json
 - downloads/pre_explored_urls.json
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import uuid
from typing import List

import requests

DEFAULT_PATHS = [
    "backend/shared/pre_explored_urls.json",
    "downloads/pre_explored_urls.json",
]

API_PREFIX = os.getenv("API_PREFIX", "/Ai_Pro_filecrawler")
SESSION_START_URL = os.getenv("SESSION_START_URL", f"http://127.0.0.1:8000{API_PREFIX}/backend/session/start")


def load_urls_from_file(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as fh:
        data = fh.read().strip()
    # try JSON array first
    try:
        parsed = json.loads(data)
        if isinstance(parsed, list):
            return [str(u).strip() for u in parsed if u]
        # if dict with lines, try to extract values
        if isinstance(parsed, dict):
            vals = []
            for v in parsed.values():
                if isinstance(v, list):
                    vals.extend(v)
                else:
                    vals.append(v)
            return [str(u).strip() for u in vals if u]
    except Exception:
        pass
    # fallback: treat file as newline-separated list
    lines = [l.strip() for l in data.splitlines() if l.strip()]
    return lines


def make_payload(urls: List[str], db: str, chat_bot_id: str, colle: str) -> dict:
    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "chat_bot_id": chat_bot_id,
        "db_name": db,
        "colle": colle,
        "contents": [urls[0]] if urls else [""],
        "start_urls_override": urls,
        "start_urls_override_source": "pre_explored_script",
    }
    return payload


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", "-f", help="Path to pre_explored_urls.json")
    p.add_argument("--db", default=os.getenv("DB_NAME", "dev_user"))
    p.add_argument("--chatbot", default=os.getenv("DEFAULT_CHAT_BOT_ID", "dev_bot"))
    p.add_argument("--colle", default="file", choices=["file", "board", "date"])
    p.add_argument("--print-payload", action="store_true")
    args = p.parse_args()

    path = args.file
    if not path:
        for cand in DEFAULT_PATHS:
            if os.path.exists(cand):
                path = cand
                break
    # If still no file, fallback to using fixed target
    if not path:
        print("No input file found. Falling back to fixed target https://www.nowon.kr")
        urls = ["https://www.nowon.kr"]
        payload = make_payload(urls, args.db, args.chatbot, args.colle)
        if args.print_payload:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        print(f"Posting job -> {SESSION_START_URL} (urls={len(urls)})")
        try:
            r = requests.post(SESSION_START_URL, json=payload, timeout=30)
            print("Status:", r.status_code)
            try:
                print("Response:", r.json())
            except Exception:
                print("Response text:", r.text[:1000])
        except Exception as e:
            print("Request failed:", e, file=sys.stderr)
            sys.exit(1)
        sys.exit(0)

    try:
        urls = load_urls_from_file(path)
    except Exception as e:
        print(f"Failed to load urls from {path}: {e}", file=sys.stderr)
        sys.exit(1)

    if not urls:
        print("No URLs found in file.", file=sys.stderr)
        sys.exit(1)

    payload = make_payload(urls, args.db, args.chatbot, args.colle)
    if args.print_payload:
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    print(f"Posting job -> {SESSION_START_URL} (urls={len(urls)})")
    try:
        r = requests.post(SESSION_START_URL, json=payload, timeout=30)
        print("Status:", r.status_code)
        try:
            print("Response:", r.json())
        except Exception:
            print("Response text:", r.text[:1000])
    except Exception as e:
        print("Request failed:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

