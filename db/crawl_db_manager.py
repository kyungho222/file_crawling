import os
import time
try:
    import pymysql  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pymysql = None  # type: ignore
import logging
import asyncio
import re
from backend.shared.config import Config
from backend.shared.crawl_trace import crawl_trace
from db.mysql_db_config import mysql_execute_query
from typing import List, Dict, Any

# 濡쒓굅 ?ㅼ젙
logger = logging.getLogger("crawl_db_manager")
# Removed debug/temporary logger handler setup to avoid ad-hoc handler additions.
logger.setLevel(logging.INFO)
import sqlite3
from pathlib import Path


def _local_db_path() -> str:
    project_root = Path(__file__).resolve().parent.parent
    local_dir = project_root / "local_dev" / "db"
    try:
        local_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return str(local_dir / "local.db")


def _ensure_local_db_initialized():
    logger.warning("[LocalDB] runtime schema mutation is disabled; skipping local DB initialization")


def _use_local_db() -> bool:
    return str(os.getenv("USE_LOCAL_DB", "0")).strip().lower() in ("1", "true", "yes", "on")


def _db_load_debug_enabled() -> bool:
    return str(os.getenv("DB_LOAD_DEBUG", "0") or "0").strip().lower() in ("1", "true", "yes", "on")


def _db_load_slow_ms() -> float:
    try:
        return max(0.0, float(os.getenv("DB_LOAD_SLOW_QUERY_MS", "300") or "300"))
    except Exception:
        return 300.0


def _db_load_fast_status_slow_ms() -> float:
    try:
        return max(
            _db_load_slow_ms(),
            float(os.getenv("DB_LOAD_FAST_STATUS_SLOW_MS", "3000") or "3000"),
        )
    except Exception:
        return max(_db_load_slow_ms(), 3000.0)


def _db_load_bottleneck(timings: Dict[str, float]) -> str:
    if not timings:
        return "unknown"
    try:
        key, _value = max(timings.items(), key=lambda item: float(item[1] or 0.0))
        return str(key)
    except Exception:
        return "unknown"

# PostgreSQL ?ъ슜 ??db_postgres.py ?곌껐 (db_postgres.py ?섏젙 ?놁씠)
async def _execute_query_postgres(query: str, params=None, fetch=False):
    """
    PostgreSQL ?ъ슜 ??db_postgres.py??鍮꾨룞湲??몄뀡???ъ슜?섏뿬 荑쇰━ ?ㅽ뻾
    db_postgres.py ?뚯씪? ?섏젙?섏? ?딄퀬, ?ш린???곌껐
    """
    try:
        from db.db_postgres import async_session
        from sqlalchemy import text
        
        # MySQL 荑쇰━瑜?PostgreSQL ?뺤떇?쇰줈 蹂??
        # ` (諛깊떛) ??" (?곕뵲?댄몴)
        pg_query = query.replace("`", '"')
        
        # %s ?뚮젅?댁뒪??붾? :param1, :param2 ?뺤떇?쇰줈 蹂??
        if params and isinstance(params, (tuple, list)):
            pg_params = {}
            param_list = list(params) if isinstance(params, tuple) else params
            for i, param in enumerate(param_list, 1):
                pg_params[f"param{i}"] = param
            
            # %s瑜?:param1, :param2 ?뺤떇?쇰줈 移섑솚
            param_index = 1
            while "%s" in pg_query:
                pg_query = pg_query.replace("%s", f":param{param_index}", 1)
                param_index += 1
        else:
            pg_params = params or {}
        
        # async_session??吏곸젒 ?ъ슜
        async with async_session() as session:
            try:
                if fetch:
                    # SELECT 荑쇰━
                    result = await session.execute(text(pg_query), pg_params)
                    rows = result.fetchall()
                    # ?뺤뀛?덈━ ?뺥깭濡?蹂??
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    # INSERT/UPDATE/DELETE 荑쇰━
                    await session.execute(text(pg_query), pg_params)
                    await session.commit()
                    return None
            except Exception as e:
                await session.rollback()
                raise
    except Exception as e:
        logger.error(f"[PostgreSQL] Database operation failed: {e}")
        raise


def _should_use_postgres():
    """PostgreSQL ?ъ슜 ?щ? ?뺤씤"""
    use_postgres_env = os.getenv("USE_POSTGRES", "false").lower() == "true"
    database_url = os.getenv("DATABASE_URL", "")
    return use_postgres_env or database_url.startswith("postgresql")


def _crawling_log_db_write_timeout_sec() -> float:
    try:
        value = float(os.getenv("CRAWLING_LOG_DB_WRITE_TIMEOUT_SEC", "8") or "8")
    except Exception:
        value = 8.0
    return max(1.0, min(value, 300.0))


async def _execute_query(query: str, params=None, fetch=False, dbname="chatty", op_name: str | None = None):
    """
    PostgreSQL ?ъ슜 ??db_postgres.py ?곌껐, ?꾨땲硫?MySQL/MariaDB ?ъ슜
    紐⑤뱺 DB 荑쇰━瑜????⑥닔濡??듭씪?섏뿬 ?쇱슦??
    """
    if _should_use_postgres():
        if fetch:
            return await _execute_query_postgres(query, params, fetch)
        from backend.shared.db_write_queue import run_db_write

        return await run_db_write(
            op_name or "crawling_log.postgres_write",
            lambda: _execute_query_postgres(query, params, fetch),
            timeout_sec=_crawling_log_db_write_timeout_sec(),
        )
    if fetch:
        return await mysql_execute_query(query, params, fetch=fetch, dbname=dbname)
    from backend.shared.db_write_queue import run_db_write

    return await run_db_write(
        op_name or "crawling_log.mysql_write",
        lambda: mysql_execute_query(query, params, fetch=fetch, dbname=dbname, op_name=op_name),
        timeout_sec=_crawling_log_db_write_timeout_sec(),
    )


_COUNTER_BATCH_TERMINAL_STATUSES = {
    "completed",
    "coll_stop",
    "download_stop",
    "interrupted",
    "error",
    "download_stop",
    "stop",
    "stopped",
    "cancelled",
}
_COUNTER_BATCHABLE_STATUSES = {"", "running", "pending", "start", "crawled", "ok"}
_counter_last_write_at: Dict[tuple, float] = {}
_counter_last_written_counts: Dict[tuple, Dict[str, int]] = {}
_counter_last_nonterminal_status_at: Dict[tuple, float] = {}
_config_values_cache: Dict[tuple, tuple[float, Dict[str, str]]] = {}
_crawling_log_id_cache: Dict[tuple, tuple[float, int]] = {}


def _counter_batch_enabled() -> bool:
    return str(os.getenv("CRAWLING_LOG_COUNTER_BATCH_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _counter_batch_sec() -> float:
    try:
        return max(0.0, min(float(os.getenv("CRAWLING_LOG_COUNTER_BATCH_SEC", "2") or "2"), 60.0))
    except Exception:
        return 2.0


def _counter_batch_min_delta() -> int:
    try:
        return max(1, min(int(os.getenv("CRAWLING_LOG_COUNTER_BATCH_MIN_DELTA", "25") or "25"), 100000))
    except Exception:
        return 25


def _counter_fast_update_enabled() -> bool:
    return str(os.getenv("CRAWLING_LOG_COUNTER_FAST_UPDATE", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

def _crawling_log_job_id_fallback_enabled() -> bool:
    # PHP-created crawling_log rows are sometimes forwarded without the PK id.
    # In that case keep progress/status updates attached to the existing job row.
    return str(os.getenv("CRAWLING_LOG_JOB_ID_FALLBACK_ENABLED", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _normalize_crawling_log_id(log_id: Any) -> int | None:
    if log_id is None:
        return None
    try:
        value = int(str(log_id).strip())
    except Exception:
        return None
    return value if value > 0 else None


def _crawling_log_id_cache_ttl_sec() -> float:
    try:
        value = float(os.getenv("CRAWLING_LOG_ID_CACHE_TTL_SEC", "3600") or "3600")
    except Exception:
        value = 3600.0
    return max(0.0, min(value, 86400.0))


async def resolve_crawling_log_id(job_id: str, *, dbname: str = "chatty") -> int | None:
    """Resolve the PHP-created crawling_log row once, then use its PK id for updates."""
    jid = str(job_id or "").strip()
    db_key = str(dbname or "chatty").strip() or "chatty"
    if not jid:
        return None
    key = (db_key, jid)
    now = time.monotonic()
    cached = _crawling_log_id_cache.get(key)
    if cached:
        cached_at, cached_id = cached
        if cached_id > 0 and (now - float(cached_at or 0.0)) <= _crawling_log_id_cache_ttl_sec():
            return cached_id
    try:
        rows = await _execute_query(
            "SELECT `id` FROM `ASADAL_CRAWLING_LOG` WHERE `job_id`=%s ORDER BY `id` DESC LIMIT 1",
            (jid,),
            fetch=True,
            dbname=db_key,
        )
        row = rows[0] if rows else None
        if isinstance(row, dict):
            resolved = _normalize_crawling_log_id(row.get("id"))
        elif isinstance(row, (list, tuple)) and row:
            resolved = _normalize_crawling_log_id(row[0])
        else:
            resolved = None
        if resolved is not None:
            _crawling_log_id_cache[key] = (now, resolved)
            return resolved
    except Exception as exc:
        logger.debug("[CrawlingLog] resolve id by job_id failed | job_id=%s db=%s err=%s", jid, db_key, exc)
    return None

def _crawling_log_where_by_index(job_id: str, log_id: Any) -> tuple[str | None, list[Any], str]:
    log_id_clean = _normalize_crawling_log_id(log_id)
    if log_id_clean is not None:
        return "WHERE `id`=%s", [log_id_clean], "id"
    if _crawling_log_job_id_fallback_enabled() and job_id:
        return "WHERE `job_id`=%s", [job_id], "job_id_fallback"
    return None, [], "missing_log_id"


def _counter_batch_key(*, dbname: str, job_id: str, log_id: int | None) -> tuple:
    return (str(dbname or "chatty"), str(job_id or ""), str(log_id or ""))


def _counter_nonterminal_status_min_sec() -> float:
    try:
        return max(0.0, min(float(os.getenv("CRAWLING_LOG_NONTERMINAL_STATUS_MIN_SEC", "60") or "60"), 600.0))
    except Exception:
        return 60.0


def _config_values_cache_ttl_sec() -> float:
    try:
        return max(0.0, min(float(os.getenv("CRAWLING_CONFIG_CACHE_TTL_SEC", "300") or "300"), 3600.0))
    except Exception:
        return 300.0


def _counter_count_values(
    *,
    scan: int | None,
    collection: int | None,
    saved: int | None,
    study: int | None,
    pages: int | None,
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for key, value in {
        "scan": scan,
        "collection": collection,
        "saved": saved,
        "study": study,
        "pages": pages,
    }.items():
        if value is None:
            continue
        try:
            out[key] = int(value)
        except Exception:
            pass
    return out


def _should_skip_batched_counter_write(
    *,
    job_id: str,
    dbname: str,
    log_id: int | None,
    status: str | None,
    scan: int | None,
    collection: int | None,
    saved: int | None,
    study: int | None,
    pages: int | None,
) -> bool:
    if not _counter_batch_enabled():
        return False
    status_key = str(status or "").strip().lower()
    if status_key in _COUNTER_BATCH_TERMINAL_STATUSES:
        return False
    if status_key not in _COUNTER_BATCHABLE_STATUSES:
        return False

    key = _counter_batch_key(dbname=dbname, job_id=job_id, log_id=log_id)
    now = time.monotonic()
    last_at = float(_counter_last_write_at.get(key, 0.0) or 0.0)
    if now - last_at >= _counter_batch_sec():
        return False

    counts = _counter_count_values(scan=scan, collection=collection, saved=saved, study=study, pages=pages)
    if not counts:
        return False
    previous = _counter_last_written_counts.get(key) or {}
    min_delta = _counter_batch_min_delta()
    for field, value in counts.items():
        if abs(int(value) - int(previous.get(field, 0) or 0)) >= min_delta:
            return False
    return True


def _remember_counter_write(
    *,
    job_id: str,
    dbname: str,
    log_id: int | None,
    scan: int | None,
    collection: int | None,
    saved: int | None,
    study: int | None,
    pages: int | None,
) -> None:
    key = _counter_batch_key(dbname=dbname, job_id=job_id, log_id=log_id)
    _counter_last_write_at[key] = time.monotonic()
    counts = _counter_count_values(scan=scan, collection=collection, saved=saved, study=study, pages=pages)
    if counts:
        _counter_last_written_counts[key] = counts





async def update_crawling_log_counters(job_id: str, scan: int | None = None, collection: int | None = None, saved: int | None = 0,
                                       study: int | None = None, dbname: str = "chatty", status: str | None = None,
                                       log_id: int | None = None, pages: int | None = None,
                                       colle: str | None = None, total_files_found: int | None = None, force: bool = False) -> bool:
    """ASADAL_CRAWLING_LOG ?뚯씠釉붿뿉 吏묎퀎 移댁슫?몃? ?낅뜲?댄듃?쒕떎.

    Args:
        job_id: ?묒뾽 ?앸퀎??
        scan: ?먯깋(珥?URL) 媛쒖닔
        collection: ?뚯떛源뚯? 吏꾪뻾??媛쒖닔
        saved: ??λ맂 媛쒖닔 (?덈?媛?
        study: ?숈뒿 ?꾨즺 媛쒖닔 (?좏깮??
        pages: ?щ·留곹븳 ?섏씠吏 ??(?좏깮??
        status: ?곹깭 媛?(?좏깮?? "ok", "stop", "error" ??
        log_id: 濡쒓렇 ID (?좏깮?? ?뱀젙 濡쒓렇留??낅뜲?댄듃)
        dbname: ?곗씠?곕쿋?댁뒪 ?대쫫
        colle: ?섏쭛 諛⑸쾿 (?좏깮?? "file", "web", "bord", "all", "date", "text")
        total_files_found: 諛쒓껄???꾩껜 ?뚯씪 ??(?좏깮?? scan怨??숈씪???섎??????덉쓬)
    Returns:
        True if success else False
    """
    resolved_log_id = _normalize_crawling_log_id(log_id)
    if resolved_log_id is None and job_id:
        resolved_log_id = await resolve_crawling_log_id(job_id, dbname=dbname)
        if resolved_log_id is not None:
            log_id = resolved_log_id
    db_load_debug = _db_load_debug_enabled()
    db_load_slow_ms = _db_load_slow_ms()
    db_load_total_t0 = time.perf_counter()
    db_load_timings: Dict[str, float] = {}
    crawl_trace(
        logger,
        phase="db",
        action="crawling_log_counter_update",
        state="start",
        job_id=job_id,
        db=dbname,
        log_id=log_id,
        status=status,
        counts={"scan": scan, "collection": collection, "save": saved, "study": study, "pages": pages},
    )

    if not force and _should_skip_batched_counter_write(
        job_id=job_id,
        dbname=dbname,
        log_id=log_id,
        status=status,
        scan=scan,
        collection=collection,
        saved=saved,
        study=study,
        pages=pages,
    ):
        if db_load_debug:
            logger.debug(
                "[DBLoad][CrawlingLog] batched skip | job_id=%s db=%s log_id=%s scan=%s collection=%s saved=%s study=%s pages=%s status=%s",
                job_id,
                dbname,
                log_id,
                scan,
                collection,
                saved,
                study,
                pages,
                status,
            )
        crawl_trace(
            logger,
            phase="db",
            action="crawling_log_counter_update",
            state="skip",
            level=logging.DEBUG,
            job_id=job_id,
            db=dbname,
            log_id=log_id,
            reason="batched_counter_write",
        )
        return True

    try:
        logger.debug("=" * 80)
        logger.debug("?뵇 [DB ?낅뜲?댄듃 ?붾쾭源? update_crawling_log_counters ?쒖옉")
        logger.debug(f"   ?낅젰 ?뚮씪誘명꽣:")
        logger.debug(f"     job_id: {job_id} (??? {type(job_id).__name__})")
        logger.debug(f"     scan: {scan} (??? {type(scan).__name__})")
        logger.debug(f"     collection: {collection} (??? {type(collection).__name__})")
        logger.debug(f"     saved: {saved} (??? {type(saved).__name__})")
        logger.debug(f"     study: {study} (??? {type(study).__name__})")
        logger.debug(f"     pages: {pages} (??? {type(pages).__name__})")
        logger.debug(f"     status: {status} (??? {type(status).__name__})")
        logger.debug(f"     log_id: {log_id} (??? {type(log_id).__name__})")
        logger.debug("     colle: %s (%s)", colle, type(colle).__name__)
        logger.debug(f"     total_files_found: {total_files_found} (??? {type(total_files_found).__name__})")
        logger.debug(f"     dbname: {dbname}")

        # Local-dev fallback: write into sqlite instead of remote DB when enabled
        if _use_local_db():
            try:
                _ensure_local_db_initialized()
                db_path = _local_db_path()
                conn = sqlite3.connect(db_path)
                try:
                    cur = conn.cursor()
                    # Ensure a row exists for this job_id
                    cur.execute("SELECT id FROM ASADAL_CRAWLING_LOG WHERE job_id = ?", (job_id,))
                    row = cur.fetchone()
                    if row is None:
                        cur.execute(
                            "INSERT INTO ASADAL_CRAWLING_LOG (job_id, scan, collection, save, study, pages, status, colle) VALUES (?,?,?,?,?,?,?,?)",
                            (job_id, int(scan or 0), int(collection or 0), int(saved or 0), int(study or 0), int(pages or 0), status or None, colle or None),
                        )
                    else:
                        set_parts = []
                        params_update = []
                        if scan is not None:
                            set_parts.append("scan=?"); params_update.append(int(scan))
                        if collection is not None:
                            set_parts.append("collection=?"); params_update.append(int(collection))
                        if saved is not None:
                            set_parts.append("save=?"); params_update.append(int(saved))
                        if study is not None:
                            set_parts.append("study=?"); params_update.append(int(study))
                        if pages is not None:
                            set_parts.append("pages=?"); params_update.append(int(pages))
                        if status is not None:
                            set_parts.append("status=?"); params_update.append(status)
                        if colle is not None:
                            set_parts.append("colle=?"); params_update.append(str(colle))
                        if set_parts:
                            params_update.append(job_id)
                            cur.execute(f"UPDATE ASADAL_CRAWLING_LOG SET {', '.join(set_parts)} WHERE job_id=?", tuple(params_update))
                    conn.commit()
                    _remember_counter_write(
                        job_id=job_id,
                        dbname=dbname,
                        log_id=log_id,
                        scan=scan,
                        collection=collection,
                        saved=saved,
                        study=study,
                        pages=pages,
                    )
                    logger.debug("Local sqlite update applied for job_id=%s", job_id)
                    return True
                finally:
                    conn.close()
            except Exception as e_local:
                logger.warning("Local sqlite update failed, falling back to remote DB logic: %s", e_local)
        # ??status 媛??뺣낫 ?붾쾭源?
        logger.debug("")
        logger.debug("?뱥 [status 媛??뺣낫]")
        logger.debug("   媛?ν븳 status 媛?")
        logger.debug("     ???щ·留??꾨즺: 'crawled'")
        logger.debug("     ???щ·留?以묐떒: 'coll_stop'")
        logger.debug("     ???ㅼ슫濡쒕뱶 ?꾨즺: 'ok'")
        logger.debug("     ???ㅼ슫濡쒕뱶 以묐떒: '??μ쨷??")
        logger.debug(f"   ?꾨떖??status: {status if status is not None else 'None (湲곗〈 媛??좎?)'}")
        if status is not None:
            # ???꾨줈?앺듃 ?뺤콉: 以묐떒 踰꾪듉(status=stop)???뺤긽 ?곹깭濡?痍④툒?쒕떎. cancelled??DB?먮뒗 stop?쇰줈 ???
            valid_statuses = [
                'crawled', 'coll_stop', 'ok', 'download_stop',
                'pending', 'running', 'start',
                'completed', 'interrupted', 'error',
                'stop', 'stopped', 'cancelled',
            ]
            if status in valid_statuses:
                logger.debug(f"   ??status '{status}'???좏슚??媛믪엯?덈떎.")
            else:
                logger.warning(f"   ?좑툘 status '{status}'???쇰컲?곸쑝濡??ъ슜?섏? ?딅뒗 媛믪엯?덈떎.")
        logger.debug("")
        logger.debug("=" * 80)
        
        # 1) 湲곗〈 job_id 議댁옱 ?щ? ?뺤씤 (?낅뜲?댄듃 ??媛?議고쉶)
        if _counter_fast_update_enabled() and status is None:
            where_clause, where_params, where_mode = _crawling_log_where_by_index(job_id, log_id)
            if where_clause is None:
                logger.warning(
                    "[CrawlingLog] skip fast counter update without log_id | job_id=%s db=%s",
                    job_id,
                    dbname,
                )
                logger.warning(
                    "[CrawlingLog] counter update target row not found | job_id=%s db=%s log_id=%s",
                    job_id,
                    dbname,
                    log_id,
                )
                return False

            set_fields: list[str] = []
            set_values: list[Any] = []
            if scan is not None:
                set_fields.append("`scan`=%s")
                set_values.append(int(scan))
            if collection is not None:
                set_fields.append("`collection`=GREATEST(COALESCE(`collection`, 0), %s)")
                set_values.append(int(collection))
            if saved is not None:
                set_fields.append("`save`=GREATEST(COALESCE(`save`, 0), %s)")
                set_values.append(int(saved))
            if study is not None:
                set_fields.append("`study`=GREATEST(COALESCE(`study`, 0), %s)")
                set_values.append(int(study))
            if pages is not None:
                set_fields.append("`pages`=%s")
                set_values.append(int(pages))
            if colle is not None:
                set_fields.append("`colle`=%s")
                set_values.append(str(colle))

            if not set_fields:
                return False

            db_load_q_t0 = time.perf_counter()
            await _execute_query(
                "UPDATE `ASADAL_CRAWLING_LOG` "
                f"SET {', '.join(set_fields)} "
                f"{where_clause}",
                tuple(set_values + where_params),
                fetch=False,
                dbname=dbname,
                op_name="crawling_log_fast_counter_update",
            )
            db_load_timings["fast_update_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
            _remember_counter_write(
                job_id=job_id,
                dbname=dbname,
                log_id=log_id,
                scan=scan,
                collection=collection,
                saved=saved,
                study=study,
                pages=pages,
            )
            total_ms = (time.perf_counter() - db_load_total_t0) * 1000.0
            if db_load_debug:
                logger.debug(
                    "[DBLoad][CrawlingLog] fast counter update | job_id=%s db=%s log_id=%s fields=%s timings=%s total_ms=%.1f",
                    job_id,
                    dbname,
                    log_id,
                    [field.split("=")[0].strip("` ") for field in set_fields],
                    db_load_timings,
                    total_ms,
                )
            crawl_trace(
                logger,
                phase="db",
                action="crawling_log_counter_update",
                state="slow" if total_ms >= db_load_slow_ms else "end",
                level=logging.WARNING if total_ms >= db_load_slow_ms else logging.INFO,
                job_id=job_id,
                db=dbname,
                log_id=log_id,
                elapsed_ms=total_ms,
                timings=db_load_timings,
                bottleneck=_db_load_bottleneck(db_load_timings),
                mode="fast_counter",
            )
            return True

        if _counter_fast_update_enabled() and status is not None:
            status_str = str(status or "").strip()
            if status_str == "cancelled":
                status_str = "stop"
            valid_statuses = [
                "crawled", "coll_stop", "ok", "???關夷??", "interrupted",
                "start", "completed", "pending", "running", "error",
                "stop", "stopped", "download_stop", "cancelled",
            ]
            if not status_str:
                status_str = "interrupted"
            if status_str not in valid_statuses:
                status_str = "interrupted"
            status_str = status_str[:20]

            terminal_statuses = {
                "completed", "coll_stop", "???關夷??", "interrupted",
                "error", "download_stop", "stop", "stopped", "cancelled",
            }
            non_terminal_reset_statuses = {"running", "pending", "start"}

            where_clause, where_params, where_mode = _crawling_log_where_by_index(job_id, log_id)
            if where_clause is None:
                logger.warning(
                    "[CrawlingLog] skip fast status update without log_id | job_id=%s db=%s status=%s",
                    job_id,
                    dbname,
                    status_str,
                )
                _remember_counter_write(
                    job_id=job_id,
                    dbname=dbname,
                    log_id=log_id,
                    scan=scan,
                    collection=collection,
                    saved=saved,
                    study=study,
                    pages=pages,
                )
                return True
            if status_str in non_terminal_reset_statuses:
                throttle_key = _counter_batch_key(dbname=dbname, job_id=job_id, log_id=log_id)
                now = time.monotonic()
                last_status_at = float(_counter_last_nonterminal_status_at.get(throttle_key, 0.0) or 0.0)
                min_status_sec = _counter_nonterminal_status_min_sec()
                if last_status_at > 0 and min_status_sec > 0 and now - last_status_at < min_status_sec:
                    if db_load_debug:
                        logger.debug(
                            "[DBLoad][CrawlingLog] nonterminal status skip | job_id=%s db=%s log_id=%s status=%s age_sec=%.1f min_sec=%.1f",
                            job_id,
                            dbname,
                            log_id,
                            status_str,
                            now - last_status_at,
                            min_status_sec,
                        )
                    return True
                placeholders = ", ".join(["%s"] * len(terminal_statuses))
                where_clause += f" AND (`status` IS NULL OR `status` NOT IN ({placeholders}))"
                where_params.extend(sorted(terminal_statuses))

            set_fields: list[str] = []
            set_values: list[Any] = []
            if scan is not None:
                set_fields.append("`scan`=%s")
                set_values.append(int(scan))
            if collection is not None:
                set_fields.append("`collection`=GREATEST(COALESCE(`collection`, 0), %s)")
                set_values.append(int(collection))
            if saved is not None:
                set_fields.append("`save`=GREATEST(COALESCE(`save`, 0), %s)")
                set_values.append(int(saved))
            if study is not None:
                set_fields.append("`study`=GREATEST(COALESCE(`study`, 0), %s)")
                set_values.append(int(study))
            if pages is not None:
                set_fields.append("`pages`=%s")
                set_values.append(int(pages))
            if colle is not None:
                set_fields.append("`colle`=%s")
                set_values.append(str(colle))
            if status_str in terminal_statuses:
                set_fields.append("`end_at`=NOW()")
            elif status_str in non_terminal_reset_statuses:
                set_fields.append("`end_at`=NULL")
            set_fields.append("`status`=%s")
            set_values.append(status_str)

            if not set_fields:
                return False

            db_load_q_t0 = time.perf_counter()
            await _execute_query(
                "UPDATE `ASADAL_CRAWLING_LOG` "
                f"SET {', '.join(set_fields)} "
                f"{where_clause}",
                tuple(set_values + where_params),
                fetch=False,
                dbname=dbname,
                op_name="crawling_log_fast_status_update",
            )
            db_load_timings["fast_status_update_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
            _remember_counter_write(
                job_id=job_id,
                dbname=dbname,
                log_id=log_id,
                scan=scan,
                collection=collection,
                saved=saved,
                study=study,
                pages=pages,
            )
            if status_str in non_terminal_reset_statuses:
                _counter_last_nonterminal_status_at[
                    _counter_batch_key(dbname=dbname, job_id=job_id, log_id=log_id)
                ] = time.monotonic()
            total_ms = (time.perf_counter() - db_load_total_t0) * 1000.0
            fast_status_slow_ms = _db_load_fast_status_slow_ms()
            if db_load_debug:
                logger.debug(
                    "[DBLoad][CrawlingLog] fast status update | job_id=%s db=%s log_id=%s status=%s fields=%s timings=%s total_ms=%.1f",
                    job_id,
                    dbname,
                    log_id,
                    status_str,
                    [field.split("=")[0].strip("` ") for field in set_fields],
                    db_load_timings,
                    total_ms,
                )
            crawl_trace(
                logger,
                phase="db",
                action="crawling_log_counter_update",
                state="slow" if total_ms >= fast_status_slow_ms else "end",
                level=logging.WARNING if total_ms >= fast_status_slow_ms else logging.DEBUG,
                job_id=job_id,
                db=dbname,
                log_id=log_id,
                elapsed_ms=total_ms,
                status=status_str,
                timings=db_load_timings,
                bottleneck=_db_load_bottleneck(db_load_timings),
                mode="fast_status",
                warn_ms=fast_status_slow_ms,
            )
            return True

        where_clause, params, where_mode = _crawling_log_where_by_index(job_id, log_id)
        if where_clause is None:
            logger.warning(
                "[CrawlingLog] skip standard update without log_id | job_id=%s db=%s status=%s",
                job_id,
                dbname,
                status,
            )
            _remember_counter_write(
                job_id=job_id,
                dbname=dbname,
                log_id=log_id,
                scan=scan,
                collection=collection,
                saved=saved,
                study=study,
                pages=pages,
            )
            return True
        logger.debug("   crawling_log WHERE mode: %s", where_mode)

        # ?낅뜲?댄듃 ???꾩옱 媛?議고쉶
        select_before_sql = (
            f"SELECT `id`, `job_id`, `scan`, `collection`, `save`, `study`, `pages`, "
            f"`status`, `end_at`, `start_at`, `colle` "
            f"FROM `ASADAL_CRAWLING_LOG` {where_clause} LIMIT 10"
        )
        logger.debug(f"?뱥 [?낅뜲?댄듃 ??議고쉶] SQL: {select_before_sql}")
        logger.debug(f"   ?뚮씪誘명꽣: {params}")
        
        db_load_q_t0 = time.perf_counter()
        rows_before = await _execute_query(
            select_before_sql, tuple(params), fetch=True, dbname=dbname
        )
        db_load_timings["select_before_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
        
        logger.debug(f"?뱤 [?낅뜲?댄듃 ??DB 媛? 議고쉶???덉퐫???? {len(rows_before) if rows_before else 0}")
        if rows_before:
            for idx, row in enumerate(rows_before):
                logger.debug(f"   ?덉퐫??#{idx + 1}:")
                logger.debug(f"     id: {row.get('id')}")
                logger.debug(f"     job_id: {row.get('job_id')}")
                logger.debug(f"     scan: {row.get('scan')} (?꾩옱 DB 媛?")
                logger.debug(f"     collection: {row.get('collection')} (?꾩옱 DB 媛?")
                logger.debug(f"     save: {row.get('save')} (?꾩옱 DB 媛?")
                logger.debug(f"     study: {row.get('study')} (?꾩옱 DB 媛?")
                logger.debug(f"     pages: {row.get('pages')} (?꾩옱 DB 媛?")
                db_status = row.get('status')
                logger.debug(f"     status: {db_status} (?꾩옱 DB 媛?")
                if db_status:
                    logger.debug(f"       ??status 媛?遺꾩꽍: '{db_status}' (湲몄씠: {len(str(db_status))}??")
                logger.debug("     colle: %s match=%s", row.get("colle"), "OK" if colle is None or row.get("colle") == str(colle) else "NG")
                logger.debug("     end_at: %s", row.get("end_at"))
                logger.debug(f"     start_at: {row.get('start_at')} (?꾩옱 DB 媛?")
        else:
            logger.warning(f"   ?좑툘 議고쉶???덉퐫?쒓? ?놁뒿?덈떎!")

        exists_sql = f"SELECT 1 AS ok FROM `ASADAL_CRAWLING_LOG` {where_clause} LIMIT 1"
        db_load_q_t0 = time.perf_counter()
        rows = await _execute_query(exists_sql, tuple(params), fetch=True, dbname=dbname)
        db_load_timings["exists_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
        exists = bool(rows)

        if not exists:
            # 湲곗〈 ?덉퐫?쒓? ?놁쑝硫??꾨Т寃껊룄 ?섏? ?딆쓬
            logger.warning(f"??ASADAL_CRAWLING_LOG 湲곗〈 ???놁쓬 - job_id={job_id}, log_id={log_id}, skip update")
            _remember_counter_write(
                job_id=job_id,
                dbname=dbname,
                log_id=log_id,
                scan=scan,
                collection=collection,
                saved=saved,
                study=study,
                pages=pages,
            )
            logger.debug("=" * 80)
            total_ms = (time.perf_counter() - db_load_total_t0) * 1000.0
            if db_load_debug or total_ms >= db_load_slow_ms:
                logger.log(
                    logging.WARNING if total_ms >= db_load_slow_ms else logging.DEBUG,
                    "[DBLoad][CrawlingLog] update skipped missing row | job_id=%s db=%s log_id=%s timings=%s total_ms=%.1f cnt=%s",
                    job_id,
                    dbname,
                    log_id,
                    db_load_timings,
                    total_ms,
                    cnt,
                )
            crawl_trace(
                logger,
                phase="db",
                action="crawling_log_counter_update",
                state="skip",
                level=logging.WARNING,
                job_id=job_id,
                db=dbname,
                log_id=log_id,
                elapsed_ms=total_ms,
                reason="missing_row",
                timings=db_load_timings,
            )
            return False

        logger.debug("crawling_log target rows: %s", cnt)

        # 2) ?숈씪 job_id??紐⑤뱺 ???낅뜲?댄듃 (+ status ?쒓났 ???④퍡 媛깆떊)
        # SET ??援ъ꽦
        # ??濡쒖쭅: scan/collection? ??뼱?곌린, saved???꾩쟻
        # ?꾩옱 DB 媛?議고쉶 (status ?ы븿: 以묐떒 ???뚯빱媛 running?쇰줈 ??뼱?곕뒗 寃?諛⑹?)
        # log_id媛 ?덉쑝硫??낅뜲?댄듃 ??곴낵 ?숈씪???됱쓣 議고쉶 (媛숈? job_id???щ윭 濡쒓렇媛 ?덉쓣 ???덉쓬)
        current_query = "SELECT `scan`, `collection`, `save`, `study`, `status` FROM `ASADAL_CRAWLING_LOG` " + where_clause + " LIMIT 1"
        db_load_q_t0 = time.perf_counter()
        current_rows = await _execute_query(current_query, tuple(params), fetch=True, dbname=dbname)
        db_load_timings["current_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
        current_scan = int(current_rows[0].get('scan', 0) or 0) if current_rows else 0
        current_collection = int(current_rows[0].get('collection', 0) or 0) if current_rows else 0
        current_saved = int(current_rows[0].get('save', 0) or 0) if current_rows else 0
        current_study = int(current_rows[0].get('study', 0) or 0) if current_rows else 0
        current_db_status = (str(current_rows[0].get('status') or '').strip() if current_rows and current_rows[0].get('status') else '')
        
        set_fields = []
        set_values = []
        
        logger.debug(f"?뱷 [?낅뜲?댄듃??媛?")
        
        # ??scan: ??뼱?곌린 (??媛믪씠 ?쒓났?섎㈃ ??긽 ?낅뜲?댄듃)
        # GREATEST ?쒓굅: ?щ·留??ъ떆????媛믪씠 珥덇린?붾릺?댁빞 ?섎?濡???뼱?곌린 ?덉슜
        if scan is not None:
            set_fields.append("`scan`=%s")
            set_values.append(int(scan))
            logger.debug(f"     scan: {current_scan} ??{int(scan)} (??뼱?곌린)")
        else:
            logger.debug(f"     scan: {current_scan} (?낅뜲?댄듃 ????- scan 媛믪씠 None)")
        
        # ??collection: ??뼱?곌린 (scan怨??숈씪?섍쾶 ??긽 理쒖떊 媛믪쑝濡???뼱?곌린)
        # GREATEST ?쒓굅: ?щ·留??ъ떆????媛믪씠 珥덇린?붾릺?댁빞 ?섎?濡???뼱?곌린 ?덉슜
        if collection is not None:
            set_fields.append("`collection`=GREATEST(COALESCE(`collection`, 0), %s)")
            set_values.append(int(collection))
            logger.debug(f"     collection: {current_collection} ??{int(collection)} (??뼱?곌린)")
        else:
            logger.debug(f"     collection: {current_collection} (?낅뜲?댄듃 ????- collection 媛믪씠 None)")
            
        # ??saved: ?덈?媛???뼱?곌린
        if saved is not None:
            set_fields.append("`save`=GREATEST(COALESCE(`save`, 0), %s)")
            set_values.append(int(saved))
            logger.debug(f"     save: {current_saved} ??{int(saved)} (??뼱?곌린)")
        else:
            logger.debug(f"     save: {current_saved} (?낅뜲?댄듃 ????- saved 媛믪씠 None)")

        # ??study: ?덈?媛???뼱?곌린
        if study is not None:
            set_fields.append("`study`=GREATEST(COALESCE(`study`, 0), %s)")
            set_values.append(int(study))
            logger.debug(f"     study: {current_study} ??{int(study)} (??뼱?곌린)")
        else:
            logger.debug(f"     study: {current_study} (?낅뜲?댄듃 ????- study 媛믪씠 None)")
        
        # ??end_at: "吏꾩쭨 醫낅즺(terminal) ?곹깭"???뚮쭔 NOW()濡??낅뜲?댄듃
        #
        # 以묒슂:
        # - ?댁쁺 UI(湲곕줉 ?섏씠吏)??end_at 議댁옱 ?щ?濡?"?꾨즺"瑜??먮떒?섎뒗 寃쎌슦媛 留롫떎.
        # - ?곕씪??running 以묎컙 ?곹깭(?? ok=?ㅼ슫濡쒕뱶 ?꾨즺 ???먯꽌 end_at??李띿쑝硫?
        #   ?ㅼ젣 ?쒕쾭 ?묒뾽???⑥븘?덉뼱??湲곕줉??"?꾨즺"濡?蹂댁씠??臾몄젣媛 諛쒖깮?쒕떎.
        #
        # ?뺤콉:
        # - terminal: completed / coll_stop / ??μ쨷??/ interrupted / error / download_stop
        # - non-terminal: running / pending / start / ok / crawled ??(end_at 誘멸갚??
        # ??stop(以묐떒 踰꾪듉)??end_at??湲곕줉?댁빞 ?쒕떎. cancelled??stop怨??숈씪?섍쾶 醫낅즺 ?곹깭濡?痍④툒.
        terminal_statuses = {"completed", "coll_stop", "interrupted", "error", "download_stop", "stop", "stopped", "cancelled"}

        # 吏꾪뻾以??곹깭濡??섎룎由??뚮뒗 end_at??NULL濡?珥덇린??議곌린 ?꾨즺 ?쒓린 蹂듦뎄??
        # ?? ?대? DB媛 醫낅즺 ?곹깭(stop ??硫??뚯빱媛 蹂대궦 running?쇰줈 end_at??吏?곗? ?딆쓬
        non_terminal_reset_statuses = {"running", "pending", "start"}

        if status and str(status).strip() in non_terminal_reset_statuses:
            if current_db_status not in terminal_statuses:
                set_fields.append("`end_at`=NULL")
                logger.debug("     end_at: clear for non-terminal status")
            else:
                logger.debug("     end_at: preserve terminal status")
        elif status and str(status).strip() in terminal_statuses:
            set_fields.append("`end_at`=NOW()")
            logger.debug("     end_at: set NOW for terminal status")
        else:
            logger.debug("     end_at: unchanged")
        
        # scan? ?대? ?꾩뿉??泥섎━?섏뿀?쇰?濡??ш린?쒕뒗 異붽??섏? ?딆쓬
        logger.debug("scan update requested: %s", scan is not None)
        
        # save ?꾨뱶媛 ?꾨씫?섏뼱 ?덉쑝硫?異붽? (?대? ?꾩뿉??泥섎━??
        
        # pages媛 ?쒓났??寃쎌슦 異붽?
        if pages is not None:
            set_fields.append("`pages`=%s")
            set_values.append(int(pages))
            logger.debug(f"     pages: {pages} ??{int(pages)}")
        
        # status媛 ?쒓났??寃쎌슦 異붽? (湲몄씠 ?쒗븳 諛?寃利?
        # ???щ·留??꾨즺 ??status='completed'瑜?諛섎뱶??DB??諛섏쁺 (workflow_runner ?먮뒗 SSE payload?먯꽌 ?꾨떖)
        # ???대? DB媛 醫낅즺 ?곹깭(stop ??硫??뚯빱媛 蹂대궦 running/pending/start濡???뼱?곗? ?딆쓬 (completed????뼱?곌린 ?덉슜)
        if status is not None:
            # status 媛?寃利?諛?湲몄씠 ?쒗븳 (理쒕? 20??
            status_str = str(status).strip()
            # ??以묐떒 ???듭씪: cancelled ??stop (DB?먮뒗 stop?쇰줈 ???
            if status_str == 'cancelled':
                status_str = 'stop'
            
            # ?뚯빱媛 running/pending/start濡?移댁슫?몃쭔 媛깆떊?섎젮 ???뚮쭔 ?앸왂. completed/stop ??醫낅즺 ?곹깭????긽 諛섏쁺
            if status_str in non_terminal_reset_statuses and current_db_status in terminal_statuses:
                logger.debug(f"     status: 媛깆떊 ?앸왂 (DB媛 ?대? 醫낅즺 ?곹깭 '{current_db_status}' ??'{status_str}'濡???뼱?곗? ?딆쓬, scan/collection/save ?깅쭔 諛섏쁺)")
            else:
                # 湲몄씠 ?쒗븳 (理쒕? 20?? MySQL 而щ읆 湲몄씠 ?쒗븳)
                if len(status_str) > 20:
                    logger.warning(f"?좑툘 status 媛믪씠 ?덈Т 源곷땲??({len(status_str)}??: {status_str[:50]}...")
                    logger.warning(f"   ??泥?20?먮줈 ?먮쫭?덈떎: {status_str[:20]}")
                    status_str = status_str[:20].strip()
                
                # 鍮?臾몄옄??泥댄겕
                if not status_str:
                    logger.warning("empty crawling_log status; using interrupted")
                    status_str = 'interrupted'
                
                # ?좏슚??status 媛믩쭔 ?덉슜 (諛⑹뼱 肄붾뱶)
                # ???꾨줈?앺듃 ?뺤콉: stop/stopped/cancelled ?덉슜 (cancelled???꾩뿉??stop?쇰줈 蹂?섎맖)
                valid_statuses = ['crawled', 'coll_stop', 'ok', 'interrupted', 'start', 'completed', 'pending', 'running', 'error', 'stop', 'stopped', 'download_stop', 'cancelled']
                if status_str not in valid_statuses:
                    # ?좏슚?섏? ?딆? 媛믪씤 寃쎌슦 湲곕낯媛??ъ슜
                    logger.warning("invalid crawling_log status %r length=%s; using interrupted", status_str, len(status_str))
                    status_str = 'interrupted'
                
                # 理쒖쥌 湲몄씠 ?ы솗??(?쒕쾲 ???덉쟾?섍쾶)
                status_str = status_str[:20] if len(status_str) > 20 else status_str
                
                set_fields.append("`status`=%s")
                set_values.append(status_str)
                logger.debug(f"     status: {status_str} (湲몄씠: {len(status_str)}, ?먮낯: {status})")
        
        # colle???쒓났??寃쎌슦 異붽? (?섏쭛 諛⑸쾿 ?낅뜲?댄듃)
        if colle is not None:
            set_fields.append("`colle`=%s")
            set_values.append(str(colle))
            logger.debug("     colle update: %s", colle)
        
        # ?낅뜲?댄듃???꾨뱶媛 ?놁쑝硫?議곌린 諛섑솚
        if not set_fields:
            logger.warning(f"?좑툘 ?낅뜲?댄듃???꾨뱶媛 ?놁뒿?덈떎. job_id={job_id}, log_id={log_id}")
            logger.debug("=" * 80)
            return False
        
        upd_sql = (
            "UPDATE `ASADAL_CRAWLING_LOG` "
            f"SET {', '.join(set_fields)} "
            f"{where_clause}"
        )
        
        logger.debug(f"?뵩 [?ㅽ뻾??UPDATE SQL]")
        logger.debug(f"   {upd_sql}")
        logger.debug(f"   ?뚮씪誘명꽣: {set_values + params}")
        logger.debug(f"   ?꾩껜 ?뚮씪誘명꽣 ??? {[type(v).__name__ for v in (set_values + params)]}")
        
        # UPDATE ?ㅽ뻾
        try:
            # DB ?낅뜲?댄듃(而ㅻ컠) 吏곸쟾???쒖떆?섏뿬 SSE 諛쒗뻾 ?뚯빱媛 而ㅻ컠 ?댄썑濡?諛쒗뻾??吏?고븯?꾨줉 ??
            try:
                from backend.shared.sse_publish_queue import mark_db_update_start  # local import to avoid circular import
                await mark_db_update_start(job_id)
            except Exception:
                pass
            db_load_q_t0 = time.perf_counter()
            await _execute_query(
                upd_sql,
                tuple(set_values + params),
                fetch=False,
                dbname=dbname,
                op_name="crawling_log_standard_update",
            )
            db_load_timings["update_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
        finally:
            try:
                from backend.shared.sse_publish_queue import mark_db_update_end  # local import to avoid circular import
                await mark_db_update_end(job_id)
            except Exception:
                pass
        
        logger.debug(f"??UPDATE 荑쇰━ ?ㅽ뻾 ?꾨즺")
        
        # ?낅뜲?댄듃 ??媛?議고쉶?섏뿬 ?뺤씤
        db_load_q_t0 = time.perf_counter()
        rows_after = await _execute_query(
            select_before_sql, tuple(params), fetch=True, dbname=dbname
        )
        db_load_timings["select_after_ms"] = (time.perf_counter() - db_load_q_t0) * 1000.0
        
        logger.debug(f"?뱤 [?낅뜲?댄듃 ??DB 媛? 議고쉶???덉퐫???? {len(rows_after) if rows_after else 0}")
        if rows_after:
            for idx, row in enumerate(rows_after):
                logger.debug(f"   ?덉퐫??#{idx + 1} (?낅뜲?댄듃 ??:")
                logger.debug(f"     id: {row.get('id')}")
                logger.debug(f"     job_id: {row.get('job_id')}")
                # scan 鍮꾧탳: scan??None???꾨땺 ?뚮쭔 鍮꾧탳
                scan_match = "OK" if (scan is None or row.get("scan") == int(scan)) else "NG"
                logger.debug(f"     scan: {row.get('scan')} (?낅뜲?댄듃 ??DB 媛? {scan_match}")
                # collection 鍮꾧탳: collection??None???꾨땺 ?뚮쭔 鍮꾧탳
                collection_match = "OK" if (collection is None or row.get("collection") == int(collection)) else "NG"
                logger.debug(f"     collection: {row.get('collection')} (?낅뜲?댄듃 ??DB 媛? {collection_match}")
                # saved 鍮꾧탳: saved??湲곕낯媛믪씠 0?댁?留??덉쟾?섍쾶 泥섎━
                if saved is not None:
                    saved_match = "OK" if (row.get("save") == int(saved)) else "NG"
                    logger.debug(f"     save: {row.get('save')} (?낅뜲?댄듃 ??DB 媛? {saved_match}")
                else:
                    logger.debug(f"     save: {row.get('save')} (?낅뜲?댄듃 ??DB 媛? ?뱄툘 ?낅젰媛??놁쓬")
                if study is not None:
                    study_match = "OK" if (row.get("study") == int(study)) else "NG"
                    logger.debug(f"     study: {row.get('study')} (?낅뜲?댄듃 ??DB 媛? {study_match}")
                else:
                    logger.debug(f"     study: {row.get('study')} (?낅뜲?댄듃 ??DB 媛? ?뱄툘 ?낅젰媛??놁쓬")
                # pages 鍮꾧탳: pages媛 None???꾨땺 ?뚮쭔 鍮꾧탳
                pages_match = "OK" if (pages is None or row.get("pages") == int(pages)) else "NG"
                logger.debug(f"     pages: {row.get('pages')} (?낅뜲?댄듃 ??DB 媛? {pages_match}")
                db_status_after = row.get('status')
                status_str_expected = (str(status).strip() if status else None)
                # cancelled??DB??stop?쇰줈 ??ν븯誘濡??꾨떖媛?cancelled + DB媛?stop ? ?뺤긽
                status_match = (
                    status is None
                    or db_status_after == status_str_expected
                    or (status_str_expected == 'cancelled' and db_status_after == 'stop')
                )
                logger.debug("     status: %s match=%s", db_status_after, "OK" if status_match else "NG")
                # if status is not None:
                #     if status_match:
                #         logger.debug(f"       ??status ?낅뜲?댄듃 ?깃났: '{status}' ??'{db_status_after}'")
                #     else:
                #         logger.warning(f"       ??status ?낅뜲?댄듃 遺덉씪移? ?꾨떖媛?'{status}' ??DB媛?'{db_status_after}'")
                #         logger.warning(f"       ??DB??status 媛믪씠 ?덉긽怨??ㅻ쫭?덈떎. ?뺤씤???꾩슂?⑸땲??")
                # else:
                #     logger.debug(f"       ?뱄툘 status???꾨떖?섏? ?딆븯?쇰?濡?湲곗〈 DB 媛?'{db_status_after}'媛 ?좎???)
                logger.debug("     colle: %s match=%s", row.get("colle"), "OK" if colle is None or row.get("colle") == str(colle) else "NG")
                logger.debug("     end_at: %s", row.get("end_at"))
        else:
            logger.error("crawling_log update verify returned no rows")
        
        _remember_counter_write(
            job_id=job_id,
            dbname=dbname,
            log_id=log_id,
            scan=scan,
            collection=collection,
            saved=saved,
            study=study,
            pages=pages,
        )
        logger.debug("=" * 80)
        total_ms = (time.perf_counter() - db_load_total_t0) * 1000.0
        if db_load_debug:
            logger.debug(
                "[DBLoad][CrawlingLog] update summary | job_id=%s db=%s log_id=%s fields=%s timings=%s rows_before=%s rows_after=%s total_ms=%.1f",
                job_id,
                dbname,
                log_id,
                [field.split("=")[0].strip("` ") for field in set_fields],
                db_load_timings,
                len(rows_before) if rows_before else 0,
                len(rows_after) if rows_after else 0,
                total_ms,
            )
        crawl_trace(
            logger,
            phase="db",
            action="crawling_log_counter_update",
            state="slow" if total_ms >= db_load_slow_ms else "end",
            level=logging.WARNING if total_ms >= db_load_slow_ms else logging.INFO,
            job_id=job_id,
            db=dbname,
            log_id=log_id,
            elapsed_ms=total_ms,
            timings=db_load_timings,
            bottleneck=_db_load_bottleneck(db_load_timings),
            mode="standard",
        )
        return True
    except asyncio.TimeoutError as e:
        total_ms = (time.perf_counter() - db_load_total_t0) * 1000.0
        status_key = str(status or "").strip().lower()
        terminal = status_key in _COUNTER_BATCH_TERMINAL_STATUSES
        logger.warning(
            "[CrawlingLog][write_timeout_deferred] job_id=%s db=%s log_id=%s status=%s terminal=%s elapsed_ms=%.1f counts=%s note=queued_db_write_may_still_complete",
            job_id,
            dbname,
            log_id,
            status,
            terminal,
            total_ms,
            {"scan": scan, "collection": collection, "save": saved, "study": study, "pages": pages},
        )
        crawl_trace(
            logger,
            phase="db",
            action="crawling_log_counter_update",
            state="timeout_deferred",
            level=logging.WARNING,
            job_id=job_id,
            db=dbname,
            log_id=log_id,
            elapsed_ms=total_ms,
            timings=db_load_timings,
            error=e,
            terminal=terminal,
        )
        _remember_counter_write(
            job_id=job_id,
            dbname=dbname,
            log_id=log_id,
            scan=scan,
            collection=collection,
            saved=saved,
            study=study,
            pages=pages,
        )
        return True
    except Exception as e:
        total_ms = (time.perf_counter() - db_load_total_t0) * 1000.0
        if db_load_debug or total_ms >= db_load_slow_ms:
            logger.warning(
                "[DBLoad][CrawlingLog] update failed | job_id=%s db=%s log_id=%s timings=%s total_ms=%.1f err=%s",
                job_id,
                dbname,
                log_id,
                db_load_timings,
                total_ms,
                e,
            )
        crawl_trace(
            logger,
            phase="db",
            action="crawling_log_counter_update",
            state="fail",
            level=logging.WARNING,
            job_id=job_id,
            db=dbname,
            log_id=log_id,
            elapsed_ms=total_ms,
            timings=db_load_timings,
            error=e,
        )
        logger.error("ASADAL_CRAWLING_LOG update failed", exc_info=True)
        return False


async def _legacy_increment_crawling_log_study(
    job_id: str,
    *,
    dbname: str = "chatty",
    log_id: int | None = None,
    amount: int = 1,
) -> bool:
    """
    ASADAL_CRAWLING_LOG.study 移댁슫?몃? ?먯옄?곸쑝濡?利앷??쒗궓??
    - study_count 利앷? ?쒖젏? DB ?낅뜲?댄듃(?곹깭 諛섏쁺) ?쒖젏?쇰줈 ?쒗븳
    - study留?蹂꾨룄濡?利앷??쒗궎吏 ?딄퀬, ?꾩옱 DB??scan, collection, save 媛믩룄 ?④퍡 議고쉶?섏뿬 ?낅뜲?댄듃
    """
    if not job_id:
        return False
    try:
        inc = int(amount or 0)
    except Exception:
        inc = 0
    if inc <= 0:
        return False

    resolved_log_id = _normalize_crawling_log_id(log_id)
    if resolved_log_id is None:
        resolved_log_id = await resolve_crawling_log_id(job_id, dbname=dbname)
        if resolved_log_id is not None:
            log_id = resolved_log_id

    where_clause, params, where_mode = _crawling_log_where_by_index(job_id, log_id)
    if where_clause is None:
        logger.warning(
            "[CrawlingLog] study increment skipped without log_id | job_id=%s db=%s",
            job_id,
            dbname,
        )
        return False

    # ?꾩옱 DB??scan, collection, save 媛?議고쉶
    current_query = f"SELECT `scan`, `collection`, `save`, `study` FROM `ASADAL_CRAWLING_LOG` {where_clause} LIMIT 1"
    current_rows = await _execute_query(current_query, tuple(params), fetch=True, dbname=dbname)
    
    if not current_rows:
        logger.warning(
            "[CrawlingLog] study increment failed - no record found | job_id=%s log_id=%s",
            job_id,
            log_id,
        )
        return False
    
    current_scan = int(current_rows[0].get('scan', 0) or 0)
    current_collection = int(current_rows[0].get('collection', 0) or 0)
    current_save = int(current_rows[0].get('save', 0) or 0)
    current_study = int(current_rows[0].get('study', 0) or 0)
    
    # study 利앷?? ?④퍡 ?꾩옱 DB??scan, collection, save 媛믩룄 ?④퍡 ?낅뜲?댄듃
    update_sql = (
        f"UPDATE `ASADAL_CRAWLING_LOG` SET "
        f"`study` = IFNULL(`study`, 0) + %s, "
        f"`scan` = %s, "
        f"`collection` = %s, "
        f"`save` = %s "
        f"{where_clause}"
    )
    update_params = [inc, current_scan, current_collection, current_save, *params]
    
    try:
        result = await _execute_query(update_sql, tuple(update_params), fetch=False, dbname=dbname)
        logger.debug(
            "[CrawlingLog] study incremented with counters | job_id=%s log_id=%s inc=%s scan=%s coll=%s save=%s db=%s result=%s",
            job_id,
            log_id,
            inc,
            current_scan,
            current_collection,
            current_save,
            dbname,
            result,
        )
        return True
    except Exception as exc:
        logger.warning(
            "[CrawlingLog] study increment failed | job_id=%s log_id=%s inc=%s db=%s err=%s",
            job_id,
            log_id,
            inc,
            dbname,
            exc,
        )
        return False


async def increment_crawling_log_study(
    job_id: str,
    *,
    dbname: str = "chatty",
    log_id: int | None = None,
    amount: int = 1,
) -> bool:
    """
    ASADAL_CRAWLING_LOG.study 移댁슫?몃? ?먯옄?곸쑝濡?利앷??쒗궓??
    - study_count 利앷? ?쒖젏? DB ?낅뜲?댄듃(?곹깭 諛섏쁺) ?쒖젏?쇰줈 ?쒗븳
    - 遺덊븘?뷀븳 ?좎“???놁씠 ?⑥씪 UPDATE濡?諛섏쁺???곌껐 ?먯쑀 ?쒓컙??以꾩씤??
    """
    if not job_id:
        return False
    try:
        inc = int(amount or 0)
    except Exception:
        inc = 0
    if inc <= 0:
        return False

    resolved_log_id = _normalize_crawling_log_id(log_id)
    if resolved_log_id is None:
        resolved_log_id = await resolve_crawling_log_id(job_id, dbname=dbname)
        if resolved_log_id is not None:
            log_id = resolved_log_id

    where_clause, params, where_mode = _crawling_log_where_by_index(job_id, log_id)
    if where_clause is None:
        logger.warning(
            "[CrawlingLog] study increment skipped without log_id | job_id=%s db=%s",
            job_id,
            dbname,
        )
        return False

    update_sql = (
        f"UPDATE `ASADAL_CRAWLING_LOG` SET "
        f"`study` = IFNULL(`study`, 0) + %s "
        f"{where_clause}"
    )
    update_params = [inc, *params]

    try:
        result = await _execute_query(update_sql, tuple(update_params), fetch=False, dbname=dbname)
        affected = int(result or 0) if isinstance(result, int) else 0
        if affected <= 0:
            logger.warning(
                "[CrawlingLog] study increment failed - no record found | job_id=%s log_id=%s db=%s",
                job_id,
                log_id,
                dbname,
            )
            return False
        logger.debug(
            "[CrawlingLog] study incremented | job_id=%s log_id=%s inc=%s db=%s result=%s",
            job_id,
            log_id,
            inc,
            dbname,
            result,
        )
        return True
    except Exception as exc:
        logger.warning(
            "[CrawlingLog] study increment failed | job_id=%s log_id=%s inc=%s db=%s err=%s",
            job_id,
            log_id,
            inc,
            dbname,
            exc,
        )
        return False


async def get_crawling_log_summary(job_id: str, *, dbname: str = "chatty", log_id: int | None = None) -> Dict[str, Any]:
    """
    Return the current ASADAL_CRAWLING_LOG counters for the PHP-created crawl log row.
    Prefer the primary-key id/log_id path; job_id fallback is disabled unless explicitly enabled.
    """
    if not job_id and _normalize_crawling_log_id(log_id) is None:
        return {}
    try:
        where_clause, params, where_mode = _crawling_log_where_by_index(job_id, log_id)
        if where_clause is None:
            return {}
        order_clause = " ORDER BY `id` DESC" if where_mode == "job_id_fallback" else ""
        sql = f"""
            SELECT
                `id`, `scan`, `collection`, `save`, `study`, `status`, `colle`, `end_at`, `start_at`
            FROM `ASADAL_CRAWLING_LOG`
            {where_clause}{order_clause}
            LIMIT 1
        """
        rows = await _execute_query(sql, tuple(params), fetch=True, dbname=dbname)
        row = rows[0] if rows else None
        if not isinstance(row, dict):
            return {}
        # ???쒖???
        out: Dict[str, Any] = {
            "id": int(row.get("id", 0) or 0),
            "scan": int(row.get("scan", 0) or 0),
            "collection": int(row.get("collection", 0) or 0),
            "save": int(row.get("save", 0) or 0),
            "study": int(row.get("study", 0) or 0),
            "status": (row.get("status") or ""),
            "colle": (row.get("colle") or ""),
            "end_at": row.get("end_at"),
            "start_at": row.get("start_at"),
        }
        return out
    except Exception:
        return {}




_last_match_by_press: dict[str, int] = {}

async def get_cate_codes_by_press_name(press_name: str, dbname: str = "chatty") -> tuple[str | None, str | None]:
    """
    二쇱뼱吏?press_name怨??숈씪??subject瑜?媛吏?ASADAL_CRAWLING_LEARN_LIST ?덉퐫?쒖뿉??
    cate1, cate2 媛믪쓣 議고쉶?쒕떎.

    Returns:
        (cate1, cate2) or (None, None)
    """
    try:
        query = (
            """
            SELECT id, cate1, cate2
            FROM ASADAL_CRAWLING_LEARN_LIST
            WHERE subject = %s
            ORDER BY id DESC
            LIMIT 1
            """
        )
        rows = await _execute_query(query, (press_name,), fetch=True, dbname=dbname)
        row = rows[0] if rows else None
        if row:
            try:
                _last_match_by_press[press_name] = int(row.get("id"))
            except Exception:
                pass
            return row.get("cate1"), row.get("cate2")
        return None, None
    except Exception as e:
        logger.error(f"cate 議고쉶 ?ㅽ뙣 press_name='{press_name}': {e}")
        return None, None         



async def get_learn_row_by_press_name(press_name: str, dbname: str = "chatty") -> tuple[str | None, str | None, int | None, str | None]:
    """ASADAL_CRAWLING_LEARN_LIST?먯꽌 subject=press_name ??理쒖떊 ?덉퐫?쒖쓽 (cate1, cate2, id, content)瑜?諛섑솚?쒕떎."""
    try:
        query = (
            """
            SELECT id, cate1, cate2, content
            FROM ASADAL_CRAWLING_LEARN_LIST
            WHERE subject = %s
            ORDER BY id DESC
            LIMIT 1
            """
        )
        rows = await _execute_query(query, (press_name,), fetch=True, dbname=dbname)
        row = rows[0] if rows else None
        if row:
            return row.get("cate1"), row.get("cate2"), int(row.get("id")), row.get("content")
        return None, None, None, None
    except Exception as e:
        logger.error(f"learn row 議고쉶 ?ㅽ뙣 press_name='{press_name}': {e}")
        return None, None, None, None



async def update_pages_by_id(row_id: int, pages: int, dbname: str = "chatty") -> bool:
    """ASADAL_CRAWLING_LEARN_LIST???뱀젙 id ?덉퐫?쒖쓽 pages 媛믪쓣 媛깆떊?쒕떎."""
    try:
        sql = "UPDATE `ASADAL_CRAWLING_LEARN_LIST` SET `pages`=%s WHERE `id`=%s"
        await _execute_query(sql, (int(pages), int(row_id)), fetch=False, dbname=dbname)
        return True
    except Exception as e:
        logger.warning(f"pages 媛깆떊 ?ㅽ뙣(id={row_id}, pages={pages}): {e}")
        return False        



async def update_pages_for_press_by_content_total(press_name: str, dbname: str = "chatty", chat_bot_id: str | None = None) -> bool:
    """?뚯깮 ?뚯씠釉?ASADAL_{d_t}_LEARN_LIST)?먯꽌 content 議곌굔?쇰줈 湲곗궗 ?섎? 吏묎퀎?섏뿬
    get_cate_codes_by_press_name ?몄텧 ?뱀떆 留ㅼ묶???숈씪 press_name ?덉퐫?쒖쓽 pages瑜?媛깆떊?쒕떎.

    Args:
        press_name: ?몃줎???대쫫(ASADAL_CRAWLING_LEARN_LIST.subject)
        dbname: ?쇰━ DB紐?(chatty, testchatbot1, ...)
        chat_bot_id: 媛?ν븯硫??쒓났 (d_t ?댁꽍???꾪빐 沅뚯옣)

    Returns:
        媛깆떊 ?깃났 ?щ?
    """
    try:
        # 1) 湲곗? content 議고쉶 (?몃줎???ㅼ젙 ?덉퐫?쒖쓽 content)
        _, _, _, content = await get_learn_row_by_press_name(press_name, dbname=dbname)
        content = (content or '').strip()
        if not content:
            logger.warning(f"?몃줎???ㅼ젙 content ?놁쓬: press='{press_name}'")
            return False

        # 2) ?뚯깮 ?뚯씠釉붾챸 寃곗젙: whoami.get_chat_id_from_rdbms濡?d_t ?댁꽍
        try:
            from whoami import get_chat_id_from_rdbms  # type: ignore
        except ImportError:
            get_chat_id_from_rdbms = None  # type: ignore

        identifier = None
        if get_chat_id_from_rdbms:
            try:
                if chat_bot_id:
                    identifier = await get_chat_id_from_rdbms(dbname, chat_bot_id)
            except Exception:
                identifier = None
        else:
            logger.warning("whoami 紐⑤뱢??遺덈윭?????놁뼱 d_t ?댁꽍??嫄대꼫?곷땲??")
        if not identifier:
            logger.warning(f"d_t ?댁꽍 ?ㅽ뙣: dbname='{dbname}', chat_bot_id='{chat_bot_id}' ???뚯깮 ?뚯씠釉?寃곗젙 遺덇?")
            return False
        derived_table = f"ASADAL_{str(identifier).lower()}_LEARN_LIST"
        raw_page_count = str(os.getenv("CRAWLING_PAGE_COUNT_BY_CONTENT_ENABLED", "0") or "0").strip().lower()
        if raw_page_count not in {"1", "true", "yes", "y", "on"}:
            return False

        # 3) content LIKE(?꾨찓???щ윭 媛?吏??濡?珥?湲곗궗 ??吏묎퀎 (?대떦 ?쇰━ DB??洹몃?濡??ㅽ뻾)
        domains = [p.strip() for p in re.split(r"[\s,]+", content) if p and p.strip()]
        if not domains:
            return False
        conditions = " OR ".join(["`content` LIKE %s" for _ in domains])
        count_sql = f"SELECT COUNT(*) AS total FROM `{derived_table}` WHERE (" + conditions + ")"
        params = [f"%{d}%" for d in domains]
        rows = await _execute_query(count_sql, params, fetch=True, dbname=dbname)
        total_articles = int((rows[0] or {}).get("total", 0)) if rows else 0

        # 4) 罹먯떆????λ맂 ?숈씪 press ?덉퐫?쒖쓽 pages 媛깆떊 (?놁쑝硫?理쒖떊 ?덉퐫?쒕줈 蹂닿컯)
        return await update_pages_for_press_last_match(press_name, int(total_articles), dbname=dbname)
    except Exception as e:
        logger.warning(f"pages 吏묎퀎/媛깆떊 ?ㅽ뙣(臾댁떆): press='{press_name}', err={e}")
        return False        


async def update_pages_for_press_last_match(press_name: str, pages: int, dbname: str = "chatty") -> bool:
    row_id = _last_match_by_press.get(press_name)
    if not row_id:
        try:
            await get_cate_codes_by_press_name(press_name, dbname=dbname)
            row_id = _last_match_by_press.get(press_name)
        except Exception:
            row_id = None
    if not row_id:
        return False
    return await update_pages_by_id(int(row_id), int(pages), dbname=dbname)


async def get_config_values_by_keys(
    chat_bot_id: str | None,
    keys: List[str],
    dbname: str = "chatty",
    match_chat_bot_id: bool = True,
    use_cache: bool = True,
) -> Dict[str, str]:
    """Read ASADAL_CRAWLING_CONFIG values as {key: value}.

    When chat_bot_id matching is enabled, a chatbot-specific row wins and
    default/common rows are used only as fallback. The SQL and Python matching
    default/common rows are used only as fallback.
    """
    if not keys:
        return {}
    uniq_keys = list(dict.fromkeys([str(k).strip() for k in keys if k and str(k).strip()]))
    if not uniq_keys:
        return {}

    chat_norm = str(chat_bot_id or "").strip()
    cache_ttl = _config_values_cache_ttl_sec() if use_cache else 0.0
    cache_key = (
        str(dbname or "chatty"),
        chat_norm,
        bool(match_chat_bot_id),
        tuple(sorted(uniq_keys)),
    )
    if cache_ttl > 0:
        cached = _config_values_cache.get(cache_key)
        now = time.monotonic()
        if cached and now - float(cached[0] or 0.0) <= cache_ttl:
            return dict(cached[1])

    def _row_get(row: Any, name: str, index: int) -> Any:
        if isinstance(row, dict):
            if name in row:
                return row.get(name)
            for k, v in row.items():
                if str(k).strip().strip("`").lower() == name.lower():
                    return v
            return None
        try:
            return row[index]
        except Exception:
            return None

    def _merge_config_rows(rows: Any) -> Dict[str, str]:
        result: Dict[str, str] = {}
        fallback: Dict[str, str] = {}
        for r in rows or []:
            if match_chat_bot_id and chat_norm:
                cid = _row_get(r, "chat_bot_id", 0)
                key_index = 1
                value_index = 2
            else:
                cid = None
                key_index = 0
                value_index = 1
            k = str(_row_get(r, "key", key_index) or "").strip()
            v = str(_row_get(r, "value", value_index) or "").strip()
            if not k:
                continue
            if match_chat_bot_id and chat_norm:
                cid_norm = str(cid or "").strip()
                if cid is None or cid_norm == "" or cid_norm.lower() == "default":
                    fallback.setdefault(k, v)
                elif cid_norm == chat_norm:
                    result[k] = v
                else:
                    fallback.setdefault(k, v)
            elif k not in result:
                result[k] = v
        for k, v in fallback.items():
            result.setdefault(k, v)
        return result

    try:
        placeholders = ",".join(["%s"] * len(uniq_keys))
        if match_chat_bot_id and chat_norm:
            sql = (
                f"SELECT `chat_bot_id`, `key`, `value` FROM `ASADAL_CRAWLING_CONFIG` "
                f"WHERE (`chat_bot_id`=%s OR `chat_bot_id` IS NULL OR `chat_bot_id`='' OR `chat_bot_id`='default') "
                f"AND `key` IN ({placeholders})"
            )
            params = tuple([chat_norm] + uniq_keys)
        else:
            sql = (
                f"SELECT `key`, `value` FROM `ASADAL_CRAWLING_CONFIG` "
                f"WHERE `key` IN ({placeholders})"
            )
            params = tuple(uniq_keys)

        if _use_local_db():
            try:
                _ensure_local_db_initialized()
                db_path = _local_db_path()
                conn = sqlite3.connect(db_path)
                try:
                    cur = conn.cursor()
                    placeholders_q = ",".join("?" for _ in uniq_keys)
                    if match_chat_bot_id and chat_norm:
                        cur.execute(
                            f"SELECT chat_bot_id, key, value FROM ASADAL_CRAWLING_CONFIG "
                            f"WHERE (chat_bot_id=? OR chat_bot_id IS NULL "
                            f"OR chat_bot_id='' OR chat_bot_id='default') "
                            f"AND key IN ({placeholders_q})",
                            tuple([chat_norm] + uniq_keys),
                        )
                    else:
                        cur.execute(
                            f"SELECT key, value FROM ASADAL_CRAWLING_CONFIG WHERE key IN ({placeholders_q})",
                            tuple(uniq_keys),
                        )
                    result = _merge_config_rows(cur.fetchall())
                    if cache_ttl > 0:
                        _config_values_cache[cache_key] = (time.monotonic(), dict(result))
                    return result
                finally:
                    conn.close()
            except Exception as e_local:
                logger.warning("Local sqlite read failed, falling back to remote DB: %s", e_local)

        rows = await _execute_query(sql, params, fetch=True, dbname=dbname)
        result = _merge_config_rows(rows)
        if cache_ttl > 0:
            _config_values_cache[cache_key] = (time.monotonic(), dict(result))
        return result
    except Exception as e:
        logger.error("ASADAL_CRAWLING_CONFIG lookup failed: %s", e)
        return {}


def _parse_int_with_commas(value: str, default: int) -> int:
    try:
        s = (value or "").replace(",", "").strip()
        iv = int(s)
        return iv
    except Exception:
        return default


async def get_max_pages_from_config(dbname: str, chat_bot_id: str | None) -> int | None:
    """?쒓컙? 洹쒖튃???곕씪 ASADAL_CRAWLING_CONFIG??max_pages 媛믪쓣 諛섑솚?쒕떎.
    - ?됱씪 09:00~18:00: week_count ?ъ슜
    - 洹????됱씪 ?쇨컙/二쇰쭚): page_count ?ъ슜
    ?ㅼ젙/議고쉶 ?ㅽ뙣 ??None 諛섑솚. 媛믪? DB???ㅼ젙???濡??뺤닔 ?뚯떛 ?깃났 ?? 洹몃?濡??ъ슜?쒕떎.
    """
    try:
        if not chat_bot_id:
            return None
        from utils.timezone_utils import get_local_now
        now = get_local_now()
        is_weekday = now.weekday() < 5  # 0=??... 4=湲?
        hour = now.hour
        use_week = bool(is_weekday and (9 <= hour < 18))
        keys = ["week_count", "page_count"]
        conf = await get_config_values_by_keys(chat_bot_id, keys, dbname=dbname)
        raw = conf.get("week_count") if use_week else conf.get("page_count")
        if raw is None:
            return None
        val = _parse_int_with_commas(raw, default=300)
        # 1~100000 踰붿쐞 ?쒗븳 (?뺤옣??紐⑤뜽 ?쒖빟怨??쇱튂)

        return val
    except Exception as e:
        logger.warning(f"max_pages 怨꾩궛 ?ㅽ뙣(臾댁떆): {e}")
        return None







