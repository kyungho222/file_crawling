import os
import pymysql
import logging
import asyncio
import re
from src.utils.logging_util import LoggerSingleton
from config import Config
from src.db.mysql_db_config import mysql_execute_query
from typing import List, Dict, Any

# 로거 설정
logger = LoggerSingleton.get_logger(
    logger_name=".crawl_db_manager", level=logging.INFO
)


# PostgreSQL 사용 시 db_postgres.py 연결 (db_postgres.py 수정 없이)
async def _execute_query_postgres(query: str, params=None, fetch=False):
    """
    PostgreSQL 사용 시 db_postgres.py의 비동기 세션을 사용하여 쿼리 실행
    db_postgres.py 파일은 수정하지 않고, 여기서 연결
    """
    try:
        from src.db.db_postgres import async_session
        from sqlalchemy import text
        
        # MySQL 쿼리를 PostgreSQL 형식으로 변환
        # ` (백틱) → " (큰따옴표)
        pg_query = query.replace("`", '"')
        
        # %s 플레이스홀더를 :param1, :param2 형식으로 변환
        if params and isinstance(params, (tuple, list)):
            pg_params = {}
            param_list = list(params) if isinstance(params, tuple) else params
            for i, param in enumerate(param_list, 1):
                pg_params[f"param{i}"] = param
            
            # %s를 :param1, :param2 형식으로 치환
            param_index = 1
            while "%s" in pg_query:
                pg_query = pg_query.replace("%s", f":param{param_index}", 1)
                param_index += 1
        else:
            pg_params = params or {}
        
        # async_session을 직접 사용
        async with async_session() as session:
            try:
                if fetch:
                    # SELECT 쿼리
                    result = await session.execute(text(pg_query), pg_params)
                    rows = result.fetchall()
                    # 딕셔너리 형태로 변환
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    # INSERT/UPDATE/DELETE 쿼리
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
    """PostgreSQL 사용 여부 확인"""
    use_postgres_env = os.getenv("USE_POSTGRES", "false").lower() == "true"
    database_url = os.getenv("DATABASE_URL", "")
    return use_postgres_env or database_url.startswith("postgresql")


async def _execute_query(query: str, params=None, fetch=False, dbname="chatty"):
    """
    PostgreSQL 사용 시 db_postgres.py 연결, 아니면 MySQL/MariaDB 사용
    모든 DB 쿼리를 이 함수로 통일하여 라우팅
    """
    if _should_use_postgres():
        if fetch:
            return await _execute_query_postgres(query, params, fetch)
        from backend.shared.db_write_queue import run_db_write

        return await run_db_write(
            "crawling_log.postgres_write",
            lambda: _execute_query_postgres(query, params, fetch),
        )
    if fetch:
        return await mysql_execute_query(query, params, fetch=fetch, dbname=dbname)
    from backend.shared.db_write_queue import run_db_write

    return await run_db_write(
        "crawling_log.mysql_write",
        lambda: mysql_execute_query(query, params, fetch=fetch, dbname=dbname),
    )






async def update_crawling_log_counters(job_id: str, scan: int | None = None, collection: int | None = None, saved: int = 0,
                                       dbname: str = "chatty", status: str | None = None,
                                       log_id: int | None = None, pages: int | None = None,
                                       colle: str | None = None, total_files_found: int | None = None) -> bool:
    """ASADAL_CRAWLING_LOG 테이블에 집계 카운트를 업데이트한다.

    Args:
        job_id: 작업 식별자
        scan: 탐색(총 URL) 개수
        collection: 파싱까지 진행한 개수
        saved: 저장된 개수
        pages: 크롤링한 페이지 수 (선택적)
        status: 상태 값 (선택적, "ok", "stop", "error" 등)
        log_id: 로그 ID (선택적, 특정 로그만 업데이트)
        dbname: 데이터베이스 이름
        colle: 수집 방법 (선택적, "file", "web", "bord", "all", "date", "text")
        total_files_found: 발견된 전체 파일 수 (선택적, scan과 동일한 의미일 수 있음)
    Returns:
        True if success else False
    """
    try:
        logger.info("=" * 80)
        logger.info("🔍 [DB 업데이트 디버깅] update_crawling_log_counters 시작")
        logger.info(f"   입력 파라미터:")
        logger.info(f"     job_id: {job_id} (타입: {type(job_id).__name__})")
        logger.info(f"     scan: {scan} (타입: {type(scan).__name__})")
        logger.info(f"     collection: {collection} (타입: {type(collection).__name__})")
        logger.info(f"     saved: {saved} (타입: {type(saved).__name__})")
        logger.info(f"     pages: {pages} (타입: {type(pages).__name__})")
        logger.info(f"     status: {status} (타입: {type(status).__name__})")
        logger.info(f"     log_id: {log_id} (타입: {type(log_id).__name__})")
        logger.info(f"     colle: {colle} (타입: {type(colle).__name__})")
        logger.info(f"     total_files_found: {total_files_found} (타입: {type(total_files_found).__name__})")
        logger.info(f"     dbname: {dbname}")
        
        # ✅ status 값 정보 디버깅
        logger.info("")
        logger.info("📋 [status 값 정보]")
        logger.info("   가능한 status 값:")
        logger.info("     • 크롤링 완료: 'crawled'")
        logger.info("     • 크롤링 중단: 'coll_stop'")
        logger.info("     • 다운로드 완료: 'ok'")
        logger.info("     • 다운로드 중단: '저장중단'")
        logger.info(f"   전달된 status: {status if status is not None else 'None (기존 값 유지)'}")
        if status is not None:
            valid_statuses = ['crawled', 'coll_stop', 'ok', 'download_stop', '저장중단', 'pending', 'completed', 'interrupted']
            if status in valid_statuses:
                logger.info(f"   ✅ status '{status}'는 유효한 값입니다.")
            else:
                logger.warning(f"   ⚠️ status '{status}'는 일반적으로 사용되지 않는 값입니다.")
        logger.info("")
        logger.info("=" * 80)
        
        # 1) 기존 job_id 존재 여부 확인 (업데이트 전 값 조회)
        where_clause = "WHERE `job_id`=%s"
        params: list[Any] = [job_id]
        if log_id is not None:
            # ⚠️ 중요: 실제 컬럼명은 `id`이지 `log_id`가 아님!
            where_clause += " AND `id`=%s"
            params.append(int(log_id))
            logger.info(f"   WHERE 절에 log_id 추가: `id`={log_id}")

        # 업데이트 전 현재 값 조회
        select_before_sql = (
            f"SELECT `id`, `job_id`, `scan`, `collection`, `save`, `pages`, "
            f"`status`, `end_at`, `start_at`, `colle` "
            f"FROM `ASADAL_CRAWLING_LOG` {where_clause} LIMIT 10"
        )
        logger.info(f"📋 [업데이트 전 조회] SQL: {select_before_sql}")
        logger.info(f"   파라미터: {params}")
        
        rows_before = await _execute_query(
            select_before_sql, tuple(params), fetch=True, dbname=dbname
        )
        
        logger.info(f"📊 [업데이트 전 DB 값] 조회된 레코드 수: {len(rows_before) if rows_before else 0}")
        if rows_before:
            for idx, row in enumerate(rows_before):
                logger.info(f"   레코드 #{idx + 1}:")
                logger.info(f"     id: {row.get('id')}")
                logger.info(f"     job_id: {row.get('job_id')}")
                logger.info(f"     scan: {row.get('scan')} (현재 DB 값)")
                logger.info(f"     collection: {row.get('collection')} (현재 DB 값)")
                logger.info(f"     save: {row.get('save')} (현재 DB 값)")
                logger.info(f"     pages: {row.get('pages')} (현재 DB 값)")
                db_status = row.get('status')
                logger.info(f"     status: {db_status} (현재 DB 값)")
                if db_status:
                    logger.info(f"       → status 값 분석: '{db_status}' (길이: {len(str(db_status))}자)")
                logger.info(f"     colle: {row.get('colle')} (현재 DB 값)")
                logger.info(f"     end_at: {row.get('end_at')} (현재 DB 값)")
                logger.info(f"     start_at: {row.get('start_at')} (현재 DB 값)")
        else:
            logger.warning(f"   ⚠️ 조회된 레코드가 없습니다!")

        exists_sql = f"SELECT COUNT(*) AS cnt FROM `ASADAL_CRAWLING_LOG` {where_clause}"
        rows = await _execute_query(exists_sql, tuple(params), fetch=True, dbname=dbname)
        cnt = int((rows[0] or {}).get("cnt", 0)) if rows else 0

        if cnt <= 0:
            # 기존 레코드가 없으면 아무것도 하지 않음
            logger.warning(f"❌ ASADAL_CRAWLING_LOG 기존 행 없음 - job_id={job_id}, log_id={log_id}, skip update")
            logger.info("=" * 80)
            return False

        logger.info(f"✅ 업데이트 대상 레코드 수: {cnt}개")

        # 2) 동일 job_id의 모든 행 업데이트 (+ status 제공 시 함께 갱신)
        # SET 절 구성
        # ✅ 로직: scan/collection은 덮어쓰기, saved는 누적
        # 현재 DB 값 조회
        current_query = "SELECT `scan`, `collection`, `save` FROM `ASADAL_CRAWLING_LOG` WHERE `job_id`=%s LIMIT 1"
        current_rows = await _execute_query(current_query, (job_id,), fetch=True, dbname=dbname)
        current_scan = int(current_rows[0].get('scan', 0) or 0) if current_rows else 0
        current_collection = int(current_rows[0].get('collection', 0) or 0) if current_rows else 0
        current_saved = int(current_rows[0].get('save', 0) or 0) if current_rows else 0
        
        set_fields = []
        set_values = []
        
        logger.info(f"📝 [업데이트할 값]")
        
        # ✅ scan: 덮어쓰기 (새 값이 제공되면 항상 업데이트)
        # GREATEST 제거: 크롤링 재시작 시 값이 초기화되어야 하므로 덮어쓰기 허용
        if scan is not None:
            set_fields.append("`scan`=%s")
            set_values.append(int(scan))
            logger.info(f"     scan: {current_scan} → {int(scan)} (덮어쓰기)")
        else:
            logger.info(f"     scan: {current_scan} (업데이트 안 함 - scan 값이 None)")
        
        # ✅ collection: 덮어쓰기 (scan과 동일하게 항상 최신 값으로 덮어쓰기)
        # GREATEST 제거: 크롤링 재시작 시 값이 초기화되어야 하므로 덮어쓰기 허용
        if collection is not None:
            set_fields.append("`collection`=%s")
            set_values.append(int(collection))
            logger.info(f"     collection: {current_collection} → {int(collection)} (덮어쓰기)")
        else:
            logger.info(f"     collection: {current_collection} (업데이트 안 함 - collection 값이 None)")
            
        # ✅ saved: Atomic Update (DB 수준에서 더하기)
        # 0이 아닌 경우에만 쿼리에 포함하여 불필요한 연산 방지
        if saved is not None and int(saved) != 0:
            set_fields.append("`save`=COALESCE(`save`, 0) + %s")
            set_values.append(int(saved))
            logger.info(f"     save: current + {int(saved)} (Atomic Update)")
        else:
            logger.info(f"     save: {current_saved} (업데이트 안 함 - saved 값이 0이거나 None)")
        
        # ✅ end_at: 완료 상태일 때만 NOW()로 업데이트
        # 완료 상태 목록: crawled, coll_stop, ok, 저장중단, interrupted, completed
        completion_statuses = ['crawled', 'coll_stop', 'ok', '저장중단', 'interrupted', 'completed', 'download_stop']
        
        should_update_end_at = False
        if status and status in completion_statuses:
            should_update_end_at = True
        
        if should_update_end_at:
            set_fields.append("`end_at`=NOW()")
            logger.info(f"     end_at: NOW() (완료 상태 '{status}' 감지)")
        else:
            logger.info(f"     end_at: 업데이트 안 함 (진행 중 상태)")
        
        # scan은 이미 위에서 처리되었으므로 여기서는 추가하지 않음
        logger.info(f"     scan: {'업데이트 됨' if scan is not None else '기존 값 유지'}")
        
        # save 필드가 누락되어 있으면 추가 (이미 위에서 처리됨)
        
        # pages가 제공된 경우 추가
        if pages is not None:
            set_fields.append("`pages`=%s")
            set_values.append(int(pages))
            logger.info(f"     pages: {pages} → {int(pages)}")
        
        # status가 제공된 경우 추가 (길이 제한 및 검증)
        if status is not None:
            # status 값 검증 및 길이 제한 (최대 20자)
            status_str = str(status).strip()
            
            # 길이 제한 (최대 20자, MySQL 컬럼 길이 제한)
            if len(status_str) > 20:
                logger.warning(f"⚠️ status 값이 너무 깁니다 ({len(status_str)}자): {status_str[:50]}...")
                logger.warning(f"   → 첫 20자로 자릅니다: {status_str[:20]}")
                status_str = status_str[:20].strip()
            
            # 빈 문자열 체크
            if not status_str:
                logger.warning(f"⚠️ status 값이 비어있습니다 → 'interrupted'로 변경")
                status_str = 'interrupted'
            
            # 유효한 status 값만 허용 (방어 코드)
            valid_statuses = ['crawled', 'coll_stop', 'ok', '저장중단', 'interrupted', 'start', 'completed', 'pending', 'running']
            if status_str not in valid_statuses:
                # 유효하지 않은 값인 경우 기본값 사용
                logger.warning(f"⚠️ 유효하지 않은 status 값: {status_str} (길이: {len(status_str)}) → 'interrupted'로 변경")
                status_str = 'interrupted'
            
            # 최종 길이 재확인 (한번 더 안전하게)
            status_str = status_str[:20] if len(status_str) > 20 else status_str
            
            set_fields.append("`status`=%s")
            set_values.append(status_str)
            logger.info(f"     status: {status_str} (길이: {len(status_str)}, 원본: {status})")
        
        # colle이 제공된 경우 추가 (수집 방법 업데이트)
        if colle is not None:
            set_fields.append("`colle`=%s")
            set_values.append(str(colle))
            logger.info(f"     colle: {colle} (수집 방법)")
        
        upd_sql = (
            "UPDATE `ASADAL_CRAWLING_LOG` "
            f"SET {', '.join(set_fields)} "
            f"{where_clause}"
        )
        
        logger.info(f"🔧 [실행할 UPDATE SQL]")
        logger.info(f"   {upd_sql}")
        logger.info(f"   파라미터: {set_values + params}")
        logger.info(f"   전체 파라미터 타입: {[type(v).__name__ for v in (set_values + params)]}")
        
        # UPDATE 실행
        await _execute_query(
            upd_sql,
            tuple(set_values + params),
            fetch=False,
            dbname=dbname,
        )
        
        logger.info(f"✅ UPDATE 쿼리 실행 완료")
        
        # 업데이트 후 값 조회하여 확인
        rows_after = await _execute_query(
            select_before_sql, tuple(params), fetch=True, dbname=dbname
        )
        
        logger.info(f"📊 [업데이트 후 DB 값] 조회된 레코드 수: {len(rows_after) if rows_after else 0}")
        if rows_after:
            for idx, row in enumerate(rows_after):
                logger.info(f"   레코드 #{idx + 1} (업데이트 후):")
                logger.info(f"     id: {row.get('id')}")
                logger.info(f"     job_id: {row.get('job_id')}")
                # scan 비교: scan이 None이 아닐 때만 비교
                scan_match = "✅" if (scan is None or row.get('scan') == int(scan)) else "❌"
                logger.info(f"     scan: {row.get('scan')} (업데이트 후 DB 값) {scan_match}")
                # collection 비교: collection이 None이 아닐 때만 비교
                collection_match = "✅" if (collection is None or row.get('collection') == int(collection)) else "❌"
                logger.info(f"     collection: {row.get('collection')} (업데이트 후 DB 값) {collection_match}")
                # saved 비교: saved는 기본값이 0이지만 안전하게 처리
                saved_match = "✅" if (row.get('save') == int(saved)) else "❌"
                logger.info(f"     save: {row.get('save')} (업데이트 후 DB 값) {saved_match}")
                # pages 비교: pages가 None이 아닐 때만 비교
                pages_match = "✅" if (pages is None or row.get('pages') == int(pages)) else "❌"
                logger.info(f"     pages: {row.get('pages')} (업데이트 후 DB 값) {pages_match}")
                db_status_after = row.get('status')
                status_match = status is None or db_status_after == str(status)
                logger.info(f"     status: {db_status_after} (업데이트 후 DB 값) {'✅' if status_match else '❌'}")
                if status is not None:
                    if status_match:
                        logger.info(f"       ✅ status 업데이트 성공: '{status}' → '{db_status_after}'")
                    else:
                        logger.warning(f"       ❌ status 업데이트 불일치: 전달값 '{status}' ≠ DB값 '{db_status_after}'")
                        logger.warning(f"       → DB의 status 값이 예상과 다릅니다. 확인이 필요합니다.")
                else:
                    logger.info(f"       ℹ️ status는 전달되지 않았으므로 기존 DB 값 '{db_status_after}'가 유지됨")
                logger.info(f"     colle: {row.get('colle')} (업데이트 후 DB 값) {'✅' if colle is None or row.get('colle') == str(colle) else '❌'}")
                logger.info(f"     end_at: {row.get('end_at')} (업데이트 후 DB 값)")
        else:
            logger.error(f"   ❌ 업데이트 후 조회된 레코드가 없습니다!")
        
        logger.info("=" * 80)
        return True
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ ASADAL_CRAWLING_LOG 업데이트 실패")
        logger.error(f"   job_id: {job_id}")
        logger.error(f"   log_id: {log_id}")
        logger.error(f"   오류 타입: {type(e).__name__}")
        logger.error(f"   오류 메시지: {str(e)}")
        import traceback
        logger.error(f"   스택 트레이스:\n{traceback.format_exc()}")
        logger.error("=" * 80)
        return False




_last_match_by_press: dict[str, int] = {}

async def get_cate_codes_by_press_name(press_name: str, dbname: str = "chatty") -> tuple[str | None, str | None]:
    """
    주어진 press_name과 동일한 subject를 가진 ASADAL_CRAWLING_LEARN_LIST 레코드에서
    cate1, cate2 값을 조회한다.

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
        logger.error(f"cate 조회 실패 press_name='{press_name}': {e}")
        return None, None         



async def get_learn_row_by_press_name(press_name: str, dbname: str = "chatty") -> tuple[str | None, str | None, int | None, str | None]:
    """ASADAL_CRAWLING_LEARN_LIST에서 subject=press_name 인 최신 레코드의 (cate1, cate2, id, content)를 반환한다."""
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
        logger.error(f"learn row 조회 실패 press_name='{press_name}': {e}")
        return None, None, None, None



async def update_pages_by_id(row_id: int, pages: int, dbname: str = "chatty") -> bool:
    """ASADAL_CRAWLING_LEARN_LIST의 특정 id 레코드의 pages 값을 갱신한다."""
    try:
        sql = "UPDATE `ASADAL_CRAWLING_LEARN_LIST` SET `pages`=%s WHERE `id`=%s"
        await _execute_query(sql, (int(pages), int(row_id)), fetch=False, dbname=dbname)
        return True
    except Exception as e:
        logger.warning(f"pages 갱신 실패(id={row_id}, pages={pages}): {e}")
        return False        



async def update_pages_for_press_by_content_total(press_name: str, dbname: str = "chatty", chat_bot_id: str | None = None) -> bool:
    """파생 테이블(ASADAL_{d_t}_LEARN_LIST)에서 content 조건으로 기사 수를 집계하여
    get_cate_codes_by_press_name 호출 당시 매칭된 동일 press_name 레코드의 pages를 갱신한다.

    Args:
        press_name: 언론사 이름(ASADAL_CRAWLING_LEARN_LIST.subject)
        dbname: 논리 DB명 (chatty, testchatbot1, ...)
        chat_bot_id: 가능하면 제공 (d_t 해석을 위해 권장)

    Returns:
        갱신 성공 여부
    """
    try:
        # 1) 기준 content 조회 (언론사 설정 레코드의 content)
        _, _, _, content = await get_learn_row_by_press_name(press_name, dbname=dbname)
        content = (content or '').strip()
        if not content:
            logger.warning(f"언론사 설정 content 없음: press='{press_name}'")
            return False

        # 2) 파생 테이블명 결정: whoami.get_chat_id_from_rdbms로 d_t 해석
        from whoami import get_chat_id_from_rdbms
        identifier = None
        try:
            if chat_bot_id:
                identifier = await get_chat_id_from_rdbms(dbname, chat_bot_id)
        except Exception:
            identifier = None
        if not identifier:
            logger.warning(f"d_t 해석 실패: dbname='{dbname}', chat_bot_id='{chat_bot_id}' → 파생 테이블 결정 불가")
            return False
        derived_table = f"ASADAL_{str(identifier).lower()}_LEARN_LIST"

        # 3) content LIKE(도메인 여러 개 지원)로 총 기사 수 집계 (해당 논리 DB에 그대로 실행)
        domains = [p.strip() for p in re.split(r"[\s,]+", content) if p and p.strip()]
        if not domains:
            return False
        conditions = " OR ".join(["`content` LIKE %s" for _ in domains])
        count_sql = f"SELECT COUNT(*) AS total FROM `{derived_table}` WHERE (" + conditions + ")"
        params = [f"%{d}%" for d in domains]
        rows = await _execute_query(count_sql, params, fetch=True, dbname=dbname)
        total_articles = int((rows[0] or {}).get("total", 0)) if rows else 0

        # 4) 캐시에 저장된 동일 press 레코드의 pages 갱신 (없으면 최신 레코드로 보강)
        return await update_pages_for_press_last_match(press_name, int(total_articles), dbname=dbname)
    except Exception as e:
        logger.warning(f"pages 집계/갱신 실패(무시): press='{press_name}', err={e}")
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


async def get_config_values_by_keys(chat_bot_id: str, keys: List[str], dbname: str = "chatty") -> Dict[str, str]:
    """ASADAL_CRAWLING_CONFIG에서 특정 chat_bot_id와 key 목록에 해당하는 value들을 조회한다.
    반환 딕셔너리는 {key: value} 형태이며, 값은 원문 문자열 그대로 반환된다.
    """
    if not chat_bot_id or not keys:
        return {}
    # 키 중복 제거 및 정규화
    uniq_keys = list(dict.fromkeys([k.strip() for k in keys if k and str(k).strip()]))
    if not uniq_keys:
        return {}
    placeholders = ",".join(["%s"] * len(uniq_keys))
    sql = (
        f"SELECT `key`, `value` FROM `ASADAL_CRAWLING_CONFIG` "
        f"WHERE `chat_bot_id`=%s AND `key` IN ({placeholders})"
    )
    params = tuple([chat_bot_id] + uniq_keys)
    try:
        rows = await _execute_query(sql, params, fetch=True, dbname=dbname)
        result: Dict[str, str] = {}
        for r in (rows or []):
            k = (r.get("key") or "").strip()
            v = (r.get("value") or "").strip()
            if k:
                result[k] = v
        return result
    except Exception as e:
        logger.error(f"ASADAL_CRAWLING_CONFIG 조회 실패: {e}")
        return {}


def _parse_int_with_commas(value: str, default: int) -> int:
    try:
        s = (value or "").replace(",", "").strip()
        iv = int(s)
        return iv
    except Exception:
        return default


async def get_max_pages_from_config(dbname: str, chat_bot_id: str | None) -> int | None:
    """시간대 규칙에 따라 ASADAL_CRAWLING_CONFIG의 max_pages 값을 반환한다.
    - 평일 09:00~18:00: week_count 사용
    - 그 외(평일 야간/주말): page_count 사용
    설정/조회 실패 시 None 반환. 값은 DB에 설정된 대로(정수 파싱 성공 시) 그대로 사용한다.
    """
    try:
        if not chat_bot_id:
            return None
        from src.utils.timezone_utils import get_local_now
        now = get_local_now()
        is_weekday = now.weekday() < 5  # 0=월 ... 4=금
        hour = now.hour
        use_week = bool(is_weekday and (9 <= hour < 18))
        keys = ["week_count", "page_count"]
        conf = await get_config_values_by_keys(chat_bot_id, keys, dbname=dbname)
        raw = conf.get("week_count") if use_week else conf.get("page_count")
        if raw is None:
            return None
        val = _parse_int_with_commas(raw, default=300)
        # 1~100000 범위 제한 (확장된 모델 제약과 일치)

        return val
    except Exception as e:
        logger.warning(f"max_pages 계산 실패(무시): {e}")
        return None
