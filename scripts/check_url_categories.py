import asyncio
import os
import sys
import json
import logging
import re
from typing import Dict, Any, List, Optional, Union
from urllib.parse import urlparse, parse_qsl

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.maria_operations import maria_select_data
from utils.url import ensure_url_scheme

# Configure logging to be quiet
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("check_url_categories")

MENU_KEYS = {
    'bbsNo', 'bbsId', 'board_id', 'bo_table', 'key', 'menuNo', 'categoryId',
    'ctgryCd', 'ctgry_cd', 'categoryCd', 'category_cd',
}

def get_url_params_dict(url: str) -> Dict[str, str]:
    try:
        if not url: return {}
        u = url.replace("\\/", "/").strip()
        parsed = urlparse(u)
        return dict(parse_qsl(parsed.query))
    except Exception:
        return {}

def get_url_params_set(url: str):
    try:
        parsed = urlparse(url)
        query = parsed.query or ""
        pairs = parse_qsl(query, keep_blank_values=True)
        return set(k for k, v in pairs)
    except Exception:
        return set()

def _normalize_cate_code(value: Any) -> str:
    try:
        if value is None: return ""
        if isinstance(value, str): return value.strip()
        if isinstance(value, dict):
            for key in ("value", "code", "cate_code"):
                direct = value.get(key)
                if isinstance(direct, str) and direct.strip():
                    return direct.strip()
        return ""
    except Exception:
        return ""

async def get_url_filters(chat_bot_id: str, db_name: str) -> Optional[dict]:
    table_name = "ASADAL_CRAWLING_LEARN_LIST"
    condition = f"chat_bot_id = '{chat_bot_id}'"
    try:
        rows = await maria_select_data(
            table_name,
            columns="url_filters",
            condition=condition,
            dbname=db_name
        )
        if rows and len(rows) > 0:
            for row in rows:
                raw_val = row.get("url_filters")
                if not raw_val: continue
                data = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
                if data.get("mode") == "include" and data.get("include"):
                    return data
            return json.loads(rows[0].get("url_filters")) if isinstance(rows[0].get("url_filters"), str) else rows[0].get("url_filters")
    except Exception as e:
        print(f"Error fetching filters: {e}")
    return None

async def check_url(url: str, chat_bot_id: str, db_name: str):
    url = ensure_url_scheme(url.strip())
    print(f"\n[URL 확인]")
    print(f"Target URL: {url}")
    print(f"ChatBot ID: {chat_bot_id}")
    print(f"DB Name: {db_name}")

    filters = await get_url_filters(chat_bot_id, db_name)
    if not filters:
        print("\n❌ 해당 봇의 url_filters를 찾을 수 없습니다.")
        return

    include_patterns = filters.get("include", [])
    cate1_config = filters.get("cate1", {}).get("include", [])
    cate2_config = filters.get("cate2", {}).get("include", [])

    print(f"\n[필터 정보]")
    print(f"패턴 개수: {len(include_patterns)}")

    target_parsed = urlparse(url)
    target_params = get_url_params_dict(url)
    target_fn = os.path.basename(target_parsed.path)
    target_dir = os.path.dirname(target_parsed.path)
    
    BOARD_FILENAMES = {"list.do", "view.do", "selectBbsNttView.do", "selectBbsNttList.do", "read.do", "index.do", "allView.do"}

    matched_idx = -1
    matched_pattern = None

    for idx, pattern_url in enumerate(include_patterns):
        p_parsed = urlparse(pattern_url)
        p_params = get_url_params_dict(pattern_url)
        p_fn = os.path.basename(p_parsed.path)
        p_dir = os.path.dirname(p_parsed.path)

        fn_match = (target_fn == p_fn) or (target_fn in BOARD_FILENAMES and p_fn in BOARD_FILENAMES)
        dir_match = (target_dir == p_dir)

        if fn_match and dir_match:
            is_same = True
            found_key = False
            for m_key in MENU_KEYS:
                if m_key in p_params:
                    found_key = True
                    if target_params.get(m_key) != p_params.get(m_key):
                        is_same = False
                        break
            
            if is_same:
                matched_idx = idx
                matched_pattern = pattern_url
                break

    print(f"\n[매칭 결과]")
    if matched_idx != -1:
        print(f"✅ 매칭 성공! (url_filters.include[{matched_idx}] 항목과 일치)")
        print(f"매칭된 패턴: {matched_pattern}")
        
        f_cate1 = ""
        f_cate2 = ""
        
        if matched_idx < len(cate1_config):
            f_cate1 = _normalize_cate_code(cate1_config[matched_idx])
        if matched_idx < len(cate2_config):
            f_cate2 = _normalize_cate_code(cate2_config[matched_idx])
            
        print(f"결과 cate1: {f_cate1 if f_cate1 else '(빈값)'}")
        print(f"결과 cate2: {f_cate2 if f_cate2 else '(빈값)'}")
    else:
        print("❌ 매칭되는 패턴이 없습니다. (url_filters.include 목록 중 일치하는 구조 없음)")

async def main():
    if len(sys.argv) < 3:
        print("사용법: python scripts/check_url_categories.py <URL> <CHatBot_ID> [DB_Name]")
        print("예시: python scripts/check_url_categories.py \"https://www.guro.go.kr/...\" \"bot_1234\" \"dev_user\"")
        return

    url = sys.argv[1]
    bot_id = sys.argv[2]
    db_name = sys.argv[3] if len(sys.argv) > 3 else "dev_user"

    await check_url(url, bot_id, db_name)

if __name__ == "__main__":
    asyncio.run(main())
