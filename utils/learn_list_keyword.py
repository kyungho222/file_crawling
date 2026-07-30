"""
LEARN_LIST 키워드(10개) + 요약 추출 모듈

학습 파이프라인에서 text_data 생성 전, 정규화된 텍스트를 직접 받아
LLM으로 키워드 10개 + 요약을 추출하고 LEARN_LIST에 UPDATE한다.

DB 접근:
- db_type="maria" (기본) → MariaDB (aiomysql 비동기 풀)
- db_type="mysql"        → MySQL   (pymysql 동기 → run_in_executor)

실시간 흐름 (학습 중 인라인 처리):
1. 학습 파이프라인에서 정규화 완료된 텍스트(normalized_text)를 직접 전달받음
2. LEARN_LIST에서 대상 row id 조회
3. LLM(gpt-4o-mini)으로 키워드 10개 + 요약 추출
4. LEARN_LIST에 keyword1~10, memo1 UPDATE

배치 흐름 (사후 보정용):
1. LEARN_LIST에서 대상 row 조회 (keyword1이 NULL인 항목)
2. PostgreSQL training_data에서 text_data 수집 → 메타 헤더 제거 후 정규화
3. LLM(gpt-4o-mini)으로 키워드 10개 + 요약 추출
4. LEARN_LIST에 keyword1~10, memo1 UPDATE
"""

import asyncio
import html
import json
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

from openai import AsyncOpenAI

from config import Config
from db.db_config import connect_db as pg_connect, return_connection as pg_return
from db.maria_db_config import connect_db as maria_connect, return_connection as maria_return
from logs.logging_util import LoggerSingleton

DB_TYPE_MARIA = "maria"
DB_TYPE_MYSQL = "mysql"
MYSQL_DB_NAMES = frozenset(("chatty", "naraone"))


def _resolve_db_type(db_name: str, db_type: Optional[str] = None) -> str:
    """db_type이 명시되지 않으면 db_name으로 자동 판별."""
    if db_type is not None:
        return db_type
    db_key = str(db_name or "").strip().lower()
    return DB_TYPE_MYSQL if db_key in MYSQL_DB_NAMES else DB_TYPE_MARIA


def _get_mysql_conn(db_name: str):
    """MySQL(pymysql) 연결. db_name에 따라 적절한 커넥터 사용."""
    db_key = str(db_name or "").strip().lower()
    if db_key == "naraone":
        from utils.gwi_mysql import get_mysql_connection as get_gwi_mysql_connection
        return get_gwi_mysql_connection()
    from utils.get_mysql import get_mysql_connection
    return get_mysql_connection()


def _resolve_mysql_schema_name(db_name: str) -> str:
    """INFORMATION_SCHEMA 조회 시 사용할 실제 MySQL 스키마명."""
    db_key = str(db_name or "").strip().lower()
    if db_key == "naraone":
        return "Asadal_Chatbot"
    return str(db_name or "").strip()

logger = LoggerSingleton.get_logger(logger_name="utils.learn_list_keyword", level=logging.INFO)

KEYWORD_COLUMNS = [f"keyword{i}" for i in range(1, 11)]
MAX_KEYWORDS = 10
_ENSURED_TABLES: set[tuple[str, str]] = set()
_ENSURE_LOCK = asyncio.Lock()
LEARN_LIST_KEYWORD_COLUMNS_ENABLED = False

KEYWORD_SUMMARY_PROMPT = """텍스트를 분석하고 JSON으로 결과를 생성하라.

반환 항목
1. keywords: 텍스트 핵심 내용을 대표하는 키워드 (최대 10개)
2. summary: LLM 답변 프롬프트에서 RAG 문맥으로 사용할 요약문

키워드 규칙
- 명사 또는 명사구만 사용
- 형식: 단일 명사 또는 "명사 + 명사" 구조
- 텍스트에 실제 등장하거나 검색 매칭에 유효한 핵심 명사 사용
- LIKE 검색에 사용 가능하도록 조사 제거
- 중요도 순으로 정렬
- 중복 또는 유사 표현 제거
- 최대 10개 반환

금지 규칙
- 동사 사용 금지
- 형용사 사용 금지
- 조사 사용 금지
- 감탄사 사용 금지
- 문장 형태 키워드 금지

요약 규칙
- 문서 핵심 정보를 답변 근거 문맥 형태로 압축
- 다음 정보가 존재하면 우선 반영
  - 문서 주제
  - 핵심 사실 또는 조치
  - 기관, 인물, 지역, 서비스, 제품
  - 일정, 수치, 조건, 결과, 변화
- 원문에 없는 정보 추가 금지
- 모호한 표현 대신 구체적 명사 사용
- 서론 없이 핵심 내용부터 작성
- 250자 이내
- 한국어 1~3문장
- 다른 문서 요약과 함께 사용되어도 의미가 통하도록 독립적으로 작성

출력 규칙
- JSON만 반환
- 설명 문장 출력 금지

출력 형식
{
  "keywords": ["키워드1", "키워드2"],
  "summary": "요약문"
}

텍스트:
"""

_META_HEADER = re.compile(
    r"^\[(?:Source|Chunk_number|User_memo|Title):.*\]\s*$",
    re.IGNORECASE,
)


async def _resolve_awaitable_value(value: Any, *, label: str = "") -> Any:
    resolved = value
    unwrap_count = 0
    while unwrap_count < 3 and (asyncio.isfuture(resolved) or hasattr(resolved, "__await__")):
        resolved = await resolved
        unwrap_count += 1
    if unwrap_count:
        logger.info(
            "[learn_list_keyword] awaitable resolved | label=%s unwrap_count=%s type=%s",
            label,
            unwrap_count,
            type(resolved).__name__,
        )
    return resolved


def _is_missing_training_table_error(exc: Exception) -> bool:
    try:
        msg = str(exc or "").lower()
    except Exception:
        msg = ""
    if "doesn't exist" in msg and "training_data" in msg:
        return True
    if "does not exist" in msg and "training_data" in msg:
        return True
    try:
        args0 = getattr(exc, "args", [None])[0]
        return int(args0) == 1146
    except Exception:
        return False



def _normalize_text(text: str) -> str:
    """[Source]/[Chunk_number] 등 메타 헤더를 제거하고 본문만 정규화."""
    if not text:
        return ""
    lines = [ln for ln in text.splitlines() if not _META_HEADER.match(ln.strip())]
    body = "\n".join(lines)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def _has_body(text: str, min_chars: int = 10) -> bool:
    return len(re.sub(r"\s+", "", text)) >= min_chars


def _content_lookup_candidates(value: str) -> List[str]:
    source_val = str(value or "").strip()
    if not source_val:
        return []
    candidates: List[str] = []

    def _push(text: str) -> None:
        text = str(text or "").strip()
        if text and text not in candidates:
            candidates.append(text)

    _push(source_val)
    _push(html.unescape(source_val).strip())

    for base in list(candidates):
        try:
            parsed = urlparse(base)
        except Exception:
            continue
        if not parsed.scheme or not parsed.netloc:
            continue
        host = parsed.netloc
        host_no_www = host[4:] if host.lower().startswith("www.") else host
        hosts = [host]
        if host.lower().startswith("www."):
            hosts.append(host_no_www)
        else:
            hosts.append("www." + host_no_www)
        schemes = [parsed.scheme]
        schemes.append("https" if parsed.scheme == "http" else "http")
        for scheme in schemes:
            for next_host in hosts:
                _push(urlunparse(parsed._replace(scheme=scheme, netloc=next_host)))
    return candidates


def _pg_like_escape(value: str) -> str:
    text = str(value or "")
    return (
        text.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def _pg_safe_training_table(value: str) -> str:
    table = str(value or "").strip().lower()
    if not re.fullmatch(r"td_[a-z0-9_]+_training_data", table):
        return ""
    return table


def _training_table_candidates_from_ids(*values: Any) -> List[str]:
    candidates: List[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        for ident in (text, text.replace("-", "")):
            ident = ident.strip().lower()
            if not ident:
                continue
            table = _pg_safe_training_table(f"td_{ident}_training_data")
            if table and table not in candidates:
                candidates.append(table)
    return candidates


# ──────────────────────────────────────────────
# 1) DB: 컬럼 추가 (keyword1~10 + memo1)
# ──────────────────────────────────────────────
async def ensure_keyword_columns(learn_table: str, maria_db_name: str, db_type: Optional[str] = None):
    """No-op: LEARN_LIST keyword/memo columns are not provisioned or used."""
    return


# ──────────────────────────────────────────────
# 2) DB: 대상 row 조회
# ──────────────────────────────────────────────
async def fetch_learn_items(
    learn_table: str,
    maria_db_name: str,
    *,
    only_empty: bool = True,
    limit: Optional[int] = None,
    db_type: Optional[str] = None,
):
    """LEARN_LIST에서 처리 대상 항목을 가져온다."""
    db_type = _resolve_db_type(maria_db_name, db_type)
    where = "WHERE status = 'Y'"
    sql = f"SELECT id, subject, content, content_type FROM `{learn_table}` {where} ORDER BY id"
    if limit:
        sql += f" LIMIT {limit}"

    if db_type == DB_TYPE_MYSQL:
        def _sync():
            conn = _get_mysql_conn(maria_db_name)
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [d[0] for d in cursor.description]
                return [dict(zip(columns, r)) for r in rows]
            finally:
                cursor.close()
                conn.close()
        return await asyncio.get_event_loop().run_in_executor(None, _sync)

    conn = await maria_connect(maria_db_name)
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()
            rows = await _resolve_awaitable_value(rows, label="fetch_learn_items.fetchall")
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, r)) for r in rows]
    finally:
        await maria_return(conn, maria_db_name)


# ──────────────────────────────────────────────
# 3) PostgreSQL: text_data 수집
# ──────────────────────────────────────────────
async def fetch_chunks_text(
    training_table: str,
    pg_db_name: str,
    content_type: str,
    subject: str,
    content: str,
) -> str:
    """content_type별로 PG에서 text_data를 수집, 하나의 문자열로 반환."""
    if content_type in ("image", "text"):
        lookup_col, source_val = "subject", (subject or "").strip()
    else:
        lookup_col, source_val = "content", (content or "").strip()

    if not source_val:
        return ""

    original_training_table = str(training_table or "").strip()
    training_table = _pg_safe_training_table(original_training_table)
    if not training_table:
        logger.warning(
            "[learn_list_keyword] invalid training table name | table=%s db=%s",
            original_training_table,
            pg_db_name,
        )
        return ""

    candidates = _content_lookup_candidates(source_val) if lookup_col == "content" else [source_val]

    conn = await pg_connect(pg_db_name)
    try:
        sql = f"SELECT text_data FROM {training_table} WHERE {lookup_col} = $1 AND text_data IS NOT NULL ORDER BY id"
        matched_by = lookup_col
        for cand in candidates:
            try:
                rows = await conn.fetch(sql, cand)
            except Exception as exc:
                if _is_missing_training_table_error(exc):
                    logger.info(
                        "[learn_list_keyword] training table missing during chunk fetch; skip | table=%s db=%s lookup_col=%s",
                        training_table,
                        pg_db_name,
                        lookup_col,
                    )
                    return ""
                raise
            rows = await _resolve_awaitable_value(rows, label="fetch_chunks_text.fetch")
            if rows:
                break
        if not rows and lookup_col == "content":
            metadata_sql = (
                f"SELECT text_data FROM {training_table} "
                "WHERE content_metadata IS NOT NULL "
                "AND content_metadata::text LIKE $1 ESCAPE '\\' "
                "AND text_data IS NOT NULL "
                "ORDER BY id"
            )
            for cand in candidates:
                try:
                    rows = await conn.fetch(metadata_sql, f"%{_pg_like_escape(cand)}%")
                except Exception as exc:
                    msg = str(exc or "").lower()
                    if "content_metadata" in msg and ("does not exist" in msg or "undefinedcolumn" in msg):
                        logger.info(
                            "[learn_list_keyword] content_metadata lookup unavailable | table=%s db=%s",
                            training_table,
                            pg_db_name,
                        )
                        rows = []
                        break
                    if _is_missing_training_table_error(exc):
                        logger.info(
                            "[learn_list_keyword] training table missing during metadata chunk fetch; skip | table=%s db=%s",
                            training_table,
                            pg_db_name,
                        )
                        return ""
                    raise
                rows = await _resolve_awaitable_value(rows, label="fetch_chunks_text.metadata_fetch")
                if rows:
                    matched_by = "content_metadata"
                    break
        texts = [r["text_data"] for r in rows if r["text_data"] and r["text_data"].strip()]
        if rows:
            logger.info(
                "[learn_list_keyword] training chunks matched | table=%s db=%s lookup_col=%s matched_by=%s candidates=%s rows=%s",
                training_table,
                pg_db_name,
                lookup_col,
                matched_by,
                len(candidates),
                len(rows),
            )
        else:
            logger.info(
                "[learn_list_keyword] training chunks not found | table=%s db=%s lookup_col=%s candidates=%s first=%s",
                training_table,
                pg_db_name,
                lookup_col,
                len(candidates),
                candidates[0][:180] if candidates else "",
            )
        return "\n".join(texts)
    finally:
        await pg_return(conn, pg_db_name)


# ──────────────────────────────────────────────
# 4) LLM: 키워드 10개 + 요약 추출
# ──────────────────────────────────────────────
async def extract_keywords_llm(
    text: str,
    *,
    max_input_chars: int = 8000,
) -> dict:
    """
    gpt-4o-mini로 키워드 10개 + 요약 추출.
    Returns: {"keywords": [...], "summary": "..."}
    """
    if not text or not _has_body(text):
        return {"keywords": [], "summary": ""}

    truncated = text[:max_input_chars]
    client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)

    try:
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "JSON만 출력하세요. 다른 텍스트는 포함하지 마세요."},
                {"role": "user", "content": KEYWORD_SUMMARY_PROMPT + truncated},
            ],
            temperature=0.3,
            max_tokens=600,
            response_format={"type": "json_object"},
        )
        result = json.loads(resp.choices[0].message.content)
        keywords = result.get("keywords", [])[:MAX_KEYWORDS]
        summary = result.get("summary", "")[:500]
        return {"keywords": keywords, "summary": summary}
    except Exception as e:
        logger.warning(f"[extract_keywords_llm] LLM 호출 실패: {e}")
        return {"keywords": [], "summary": ""}


# ──────────────────────────────────────────────
# 5) DB: UPDATE keyword1~10 + memo1
# ──────────────────────────────────────────────
async def update_keywords(
    learn_table: str,
    maria_db_name: str,
    item_id: int,
    keywords: list[str],
    summary: str,
    db_type: Optional[str] = None,
):
    return
    """단건 UPDATE: keyword1~10 + memo1."""


async def apply_summarize_keywords_api_result_to_learn_row(
    learn_table: str,
    maria_db_name: str,
    item_id: int,
    res_info: Dict[str, Any],
    *,
    db_type: Optional[str] = None,
) -> None:
    return
    """
    외부 POST /summarize_keywords 응답의 results[0] 한 건을
    LEARN_LIST 표준 컬럼 keyword1~10 + memo1(요약)에 반영한다.
    (게시판/파일 워크플로 공통 — summary·keywords 단일 컬럼은 스키마에 없음)
    """
    if not learn_table or not maria_db_name or not res_info:
        return
    await ensure_keyword_columns(learn_table, maria_db_name, db_type=db_type)
    summary_text = res_info.get("summary", "")
    if not isinstance(summary_text, str):
        summary_text = str(summary_text or "")
    kw_raw = res_info.get("keywords", [])
    keywords_list: List[str] = []
    if isinstance(kw_raw, list):
        keywords_list = [str(x).strip() for x in kw_raw if str(x or "").strip()]
    elif isinstance(kw_raw, str) and kw_raw.strip():
        keywords_list = [p.strip() for p in kw_raw.split(",") if p.strip()]
    await update_keywords(
        learn_table,
        maria_db_name,
        int(item_id),
        keywords_list,
        summary_text,
        db_type=db_type,
    )


# ──────────────────────────────────────────────
# 6) 단건 처리: 학습 직후 호출용
# ──────────────────────────────────────────────
async def process_single_item_keywords(
    chat_bot_id: str,
    maria_db_name: str,
    item_id: int,
    subject: str,
    content: str,
    content_type: str,
    *,
    normalized_text: Optional[str] = None,
    pg_db_name: Optional[str] = None,
    db_type: Optional[str] = None,
) -> dict:
    return {"status": "skip", "reason": "learn_list_keyword_columns_disabled"}
    """
    특정 LEARN_LIST 항목 1건에 대해 키워드 10개 + 요약을 추출하여 DB에 UPDATE.

    - normalized_text가 전달되면 PG 조회 없이 직접 사용
    - normalized_text가 없으면 pg_db_name을 이용해 PG에서 text_data를 조회 (폴백)

    Returns: {"status": "success"|"skip"|"error", ...}
    """
    tail = chat_bot_id.replace("-", "")[-12:]
    learn_table = f"ASADAL_{tail}_LEARN_LIST"

    await ensure_keyword_columns(learn_table, maria_db_name, db_type=db_type)

    if normalized_text and _has_body(normalized_text):
        norm_text = normalized_text
    elif pg_db_name:
        training_table = await _resolve_training_table(chat_bot_id, pg_db_name)
        if not training_table:
            return {"status": "error", "reason": "training_table 조회 실패"}
        raw_text = await fetch_chunks_text(
            training_table, pg_db_name, content_type, subject, content,
        )
        norm_text = _normalize_text(raw_text)
    else:
        return {"status": "skip", "item_id": item_id, "reason": "본문 없음"}

    if not _has_body(norm_text):
        logger.info(f"[process_single] id={item_id}: 본문 부족, skip")
        return {"status": "skip", "item_id": item_id, "reason": "본문 부족"}

    result = await extract_keywords_llm(norm_text)
    keywords = result["keywords"]
    summary = result["summary"]

    await update_keywords(learn_table, maria_db_name, item_id, keywords, summary, db_type=db_type)
    logger.info(f"[process_single] id={item_id}: keyword {len(keywords)}개, summary {len(summary)}자 UPDATE 완료")
    return {
        "status": "success",
        "item_id": item_id,
        "keywords": keywords,
        "summary": summary,
    }


def _dedup_nonempty(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for v in values:
        v = (v or "").strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out

def _normalize_url_candidates(raw: str) -> list[str]:
    """
    DB 저장 포맷(&amp;)과 API 입력(&) 혼재를 흡수하기 위한 후보 생성.
    - raw
    - unescape(최대 2회 반복)
    - escape(unescaped, quote=False)
    - 끝 '&' 제거 버전도 후보로 추가 (자주 나오는 노이즈)
    """
    s = (raw or "").strip()
    if not s:
        return []

    # 1) unescape를 1~2회 반복 (이중 인코딩 흡수)
    u = s
    for _ in range(2):
        u2 = html.unescape(u).strip()
        if u2 == u:
            break
        u = u2

    e = html.escape(u, quote=False).strip()

    cands = [s, u, e]

    # 2) 끝 '&' 노이즈 제거 후보(원문/언이스케이프/이스케이프 각각)
    def strip_trailing_amp(x: str) -> str:
        return x[:-1].rstrip() if x.endswith("&") else x

    cands += [strip_trailing_amp(s), strip_trailing_amp(u), strip_trailing_amp(e)]

    return _dedup_nonempty(cands)

async def find_latest_learn_item_id(
    learn_table: str,
    maria_db_name: str,
    content_type: str,
    subject: str,
    content: str,
    db_type: Optional[str] = None,
) -> Optional[int]:
    """
    학습 요청으로 이미 생성된 LEARN_LIST row 중 가장 최근 id를 찾는다.
    - image/file/text는 subject 기준
    - 그 외(url/video/sound 등)는 content 기준
    """
    db_type = _resolve_db_type(maria_db_name, db_type)
    ct = (content_type or "").strip().lower()

    async def _run_lookup(sql: str, params: tuple):
        if db_type == DB_TYPE_MYSQL:
            def _sync():
                conn = _get_mysql_conn(maria_db_name)
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql, params)
                    row = cursor.fetchone()
                    return row
                finally:
                    cursor.close()
                    conn.close()
            return await asyncio.get_event_loop().run_in_executor(None, _sync)

        conn = await maria_connect(maria_db_name)
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                row = await cur.fetchone()
                row = await _resolve_awaitable_value(row, label="find_latest_learn_item_id.fetchone")
                return row
        finally:
            await maria_return(conn, maria_db_name)

    if ct == "sound":
        # sound LEARN_LIST는 url/name 컬럼을 사용하는 테이블이 있어 예외 처리 필요
        name_candidates = _dedup_nonempty([(subject or "")])
        url_candidates = _normalize_url_candidates(content or "")
        lookup_candidates = _dedup_nonempty(name_candidates + url_candidates)
        if not lookup_candidates:
            return None

        logger.info(f"[find_latest_learn_item_id] lookup_candidates: {lookup_candidates}")
        placeholders = ", ".join(["%s"] * len(lookup_candidates))
        sound_params = tuple(lookup_candidates + lookup_candidates)

        sql_sound = (
            f"SELECT id "
            f"FROM `{learn_table}` "
            f"WHERE `name` IN ({placeholders}) OR `url` IN ({placeholders}) "
            f"ORDER BY id DESC LIMIT 1"
        )
        sql_legacy_subject = (
            f"SELECT id "
            f"FROM `{learn_table}` "
            f"WHERE `subject` IN ({placeholders}) "
            f"ORDER BY id DESC LIMIT 1"
        )
        sql_legacy_content = (
            f"SELECT id "
            f"FROM `{learn_table}` "
            f"WHERE `content_type` = %s AND `content` IN ({placeholders}) "
            f"ORDER BY id DESC LIMIT 1"
        )

        row = None
        try:
            row = await _run_lookup(sql_sound, sound_params)
        except Exception:
            row = await _run_lookup(sql_legacy_subject, tuple(lookup_candidates))
            if not row:
                row = await _run_lookup(sql_legacy_content, ("url", *lookup_candidates))

        if not row:
            return None
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None

    if ct in ("image", "file", "text"):
        lookup_col = "subject"
        lookup_candidates = _dedup_nonempty([(subject or "")])
    else:
        lookup_col = "content"
        lookup_candidates = _normalize_url_candidates(content or "")

    if not lookup_candidates:
        return None

    logger.info(f"[find_latest_learn_item_id] lookup_candidates: {lookup_candidates}")

    placeholders = ", ".join(["%s"] * len(lookup_candidates))
    if lookup_col == "content":
        sql = (
            f"SELECT id "
            f"FROM `{learn_table}` "
            f"WHERE `content_type` = %s AND `content` IN ({placeholders}) "
            f"ORDER BY id DESC "
            f"LIMIT 1"
        )
        query_params = ("url", *lookup_candidates)
    else:
        sql = (
            f"SELECT id "
            f"FROM `{learn_table}` "
            f"WHERE `{lookup_col}` IN ({placeholders}) "
            f"ORDER BY id DESC "
            f"LIMIT 1"
        )
        query_params = tuple(lookup_candidates)

    row = await _run_lookup(sql, query_params)
    if not row:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


async def process_realtime_item_keywords(
    chat_bot_id: str,
    maria_db_name: str,
    content_type: str,
    subject: str,
    content: str,
    *,
    normalized_text: Optional[str] = None,
    pg_db_name: Optional[str] = None,
    db_type: Optional[str] = None,
) -> dict:
    return {"status": "skip", "reason": "learn_list_keyword_columns_disabled"}
    """
    학습 중인 단건에 대해 키워드/요약을 추출하여 LEARN_LIST에 UPDATE.

    - normalized_text가 전달되면 PG 조회 없이 직접 사용 (인라인 처리)
    - normalized_text가 없으면 pg_db_name을 이용해 PG에서 text_data를 조회 (폴백)
    """
    tail = chat_bot_id.replace("-", "")[-12:]
    learn_table = f"ASADAL_{tail}_LEARN_LIST"

    await ensure_keyword_columns(learn_table, maria_db_name, db_type=db_type)

    item_id = await find_latest_learn_item_id(
        learn_table=learn_table,
        maria_db_name=maria_db_name,
        content_type=content_type,
        subject=subject,
        content=content,
        db_type=db_type,
    )
    if not item_id:
        return {"status": "skip", "reason": "LEARN_LIST row 미존재"}

    if normalized_text and _has_body(normalized_text):
        norm_text = normalized_text
    elif pg_db_name:
        training_table = await _resolve_training_table(chat_bot_id, pg_db_name)
        if not training_table:
            return {"status": "error", "reason": "training_table 조회 실패"}
        raw_text = await fetch_chunks_text(
            training_table, pg_db_name, content_type, subject, content,
        )
        norm_text = _normalize_text(raw_text)
        if not _has_body(norm_text):
            return {"status": "skip", "item_id": item_id, "reason": "본문 부족"}
    else:
        return {"status": "skip", "item_id": item_id, "reason": "본문 없음"}

    result = await asyncio.wait_for(extract_keywords_llm(norm_text), timeout=25)
    keywords = result["keywords"]
    summary = result["summary"]
    await update_keywords(learn_table, maria_db_name, item_id, keywords, summary, db_type=db_type)

    return {
        "status": "success",
        "item_id": item_id,
        "keywords_count": len(keywords),
        "summary_len": len(summary or ""),
    }


# ──────────────────────────────────────────────
# 7) 배치 처리: 전체 LEARN_LIST 대상
# ──────────────────────────────────────────────
async def process_learn_list_keywords(
    chat_bot_id: str,
    maria_db_name: str,
    pg_db_name: str,
    *,
    only_empty: bool = False,
    limit: Optional[int] = None,
    concurrency: int = 5,
    db_type: Optional[str] = None,
) -> dict:
    return {"updated": 0, "skipped": 0, "errors": 0, "reason": "learn_list_keyword_columns_disabled"}
    """
    LEARN_LIST 전체(또는 keyword 미입력 건)를 배치 처리한다.

    Args:
        chat_bot_id: 챗봇 ID
        maria_db_name: DB 스키마명 (MariaDB 또는 MySQL)
        pg_db_name: PostgreSQL DB명
        only_empty: True면 keyword1이 NULL인 건만 처리, False면 전체 재처리(재학습 권장)
        limit: 처리 건수 제한 (None이면 전체)
        concurrency: LLM 동시 호출 수
        db_type: None이면 db_name으로 자동 판별, "maria" 또는 "mysql" 명시 가능

    Returns: {"updated": int, "skipped": int, "errors": int}
    """
    db_type = _resolve_db_type(maria_db_name, db_type)
    tail = chat_bot_id.replace("-", "")[-12:]
    learn_table = f"ASADAL_{tail}_LEARN_LIST"
    training_table = await _resolve_training_table(chat_bot_id, pg_db_name)

    if not training_table:
        return {"updated": 0, "skipped": 0, "errors": 1, "reason": "training_table 조회 실패"}

    await ensure_keyword_columns(learn_table, maria_db_name, db_type=db_type)

    items = await fetch_learn_items(learn_table, maria_db_name, only_empty=only_empty, limit=limit, db_type=db_type)
    logger.info(f"[batch] {learn_table}: 처리 대상 {len(items)}건")

    if not items:
        return {"updated": 0, "skipped": 0, "errors": 0}

    sem = asyncio.Semaphore(concurrency)
    counters = {"updated": 0, "skipped": 0, "errors": 0}

    async def _process_one(item: dict):
        async with sem:
            try:
                raw_text = await fetch_chunks_text(
                    training_table, pg_db_name,
                    item["content_type"], item.get("subject", ""), item.get("content", ""),
                )
                norm_text = _normalize_text(raw_text)
                if not _has_body(norm_text):
                    counters["skipped"] += 1
                    return

                result = await extract_keywords_llm(norm_text)
                await update_keywords(
                    learn_table, maria_db_name,
                    item["id"], result["keywords"], result["summary"],
                    db_type=db_type,
                )
                counters["updated"] += 1
            except Exception as e:
                logger.error(f"[batch] id={item['id']} 처리 실패: {e}")
                counters["errors"] += 1

    await asyncio.gather(*[_process_one(it) for it in items])

    logger.info(
        f"[batch] 완료: updated={counters['updated']}, "
        f"skipped={counters['skipped']}, errors={counters['errors']}"
    )
    return counters


# ──────────────────────────────────────────────
# 내부 헬퍼
# ──────────────────────────────────────────────
async def _resolve_training_table(chat_bot_id: str, pg_db_name: str) -> Optional[str]:
    """chatbot_setup에서 chat_id를 조회하여 training_data 테이블명을 반환."""
    conn = await pg_connect(pg_db_name)
    try:
        account_identifier = ""
        try:
            from db.mariadb_save_update import get_account_identifier_from_chatbot_setup

            account_identifier = str(
                await get_account_identifier_from_chatbot_setup(chat_bot_id, pg_db_name) or ""
            ).strip()
        except Exception as exc:
            logger.info(
                "[learn_list_keyword] account identifier lookup failed | db=%s chat_bot_id=%s err=%s",
                pg_db_name,
                chat_bot_id,
                exc,
            )

        row = await conn.fetchrow(
            "SELECT chat_id FROM chatbot_setup WHERE chat_bot_id = $1",
            chat_bot_id,
        )
        row = await _resolve_awaitable_value(row, label="_resolve_training_table.fetchrow")
        chat_id = str(row["chat_id"] or "").strip() if row else ""
        candidates = _training_table_candidates_from_ids(
            chat_id,
            account_identifier,
            chat_bot_id,
        )
        if not candidates:
            logger.error(
                "[learn_list_keyword] training table candidates empty | db=%s chat_bot_id=%s chat_id=%s account_identifier=%s",
                pg_db_name,
                chat_bot_id,
                chat_id,
                account_identifier,
            )
            return None

        placeholders = ", ".join(f"${idx}" for idx in range(1, len(candidates) + 1))
        rows = await conn.fetch(
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ({placeholders})
            """,
            *candidates,
        )
        rows = await _resolve_awaitable_value(rows, label="_resolve_training_table.table_exists")
        existing = {
            str(item["table_name"] or "").strip().lower()
            for item in rows or []
            if item and item["table_name"]
        }
        table_name = next((candidate for candidate in candidates if candidate in existing), "")
        logger.info(
            "[learn_list_keyword] training table resolved | db=%s chat_bot_id=%s chat_id=%s account_identifier=%s table=%s candidates=%s existing=%s",
            pg_db_name,
            chat_bot_id,
            chat_id,
            account_identifier,
            table_name,
            candidates[:5],
            sorted(existing)[:5],
        )
        return table_name or None
    finally:
        await pg_return(conn, pg_db_name)
