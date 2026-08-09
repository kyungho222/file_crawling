from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys", "chatty"}
LOGGER = logging.getLogger("run_remap_and_sync_post_content_type")
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False
REPORT_PATH = Path("tests/log/remap-sync-post-category-flow-report.json")

# ──────────────────────────────────────────────
# Optional direct-run defaults
#
# TARGET_DBNAME 비우면 전체 DB를 SHOW DATABASES 순서로 처리합니다.
# 특정 DB만 처리하려면 TARGET_DBNAME 또는 CLI --dbname/--db-name 에 값을 넣으세요.
# TARGET_CHAT_BOT_ID 비우면 remap은 해당 DB의 chatbot_setup 전체 bot을 순서대로 처리합니다.
# APPLY_CHANGES=False 는 dry-run입니다.
# 실제 UPDATE는 APPLY_CHANGES=True 또는 CLI --apply 사용 시에만 실행됩니다.
# ──────────────────────────────────────────────
TARGET_DBNAME = "utp"  # 비우면 전체 DB 순차 처리. 예: "yongin"
TARGET_CHAT_BOT_ID = ""  # 비우면 DB별 chatbot_setup 전체 chat_bot_id 처리
OLD_CATE_TREECODE = "c00020002"  # remap 수정 전 parent cate_treecode
NEW_CATE_TREECODE = "c0003"  # remap 수정 후 parent cate_treecode
TARGET_DB_TYPE = "maria"  # "maria" 또는 "mysql"
APPLY_CHANGES = False  # False=dry-run, True=실제 UPDATE
CATEGORY_CHUNK_SIZE = 1000  # remap LEARN_LIST.id chunk 크기
POST_SYNC_CHUNK_SIZE = 1000  # post content_type sync LEARN_LIST.id chunk 크기
SYNC_PG_EXISTING_POST = False  # 이미 LEARN_LIST가 post인 row까지 PG content_type 보정할지 여부
SYNC_ALLOW_MULTIPLE_BOTS = True  # chat_bot_id 미지정 sync APPLY 시 DB 내 여러 bot 업데이트 허용 여부
RDBMS_DEADLOCK_RETRY_COUNT = 5  # post sync RDBMS deadlock/lock wait 재시도 횟수
RDBMS_DEADLOCK_RETRY_DELAY_SECONDS = 2.0  # post sync 재시도 기본 대기 초
RUN_CATEGORY_REMAP = True  # False면 remap 단계 스킵
RUN_POST_CONTENT_TYPE_SYNC = True  # False면 post content_type sync 단계 스킵
STOP_ON_ERROR = False  # True면 DB/bot 단위 오류 발생 시 즉시 중단
JSON_OUTPUT = False
TARGET_LOG_LEVEL = "INFO"


@dataclass
class BotRemapRunResult:
    chat_bot_id: str
    status: str
    updates_planned: int = 0
    updates_applied_estimate: int = 0
    selected_row_count: int = 0
    created_child_count: int = 0
    created_categories: list[dict[str, Any]] = field(default_factory=list)
    category_change_stats: list[dict[str, Any]] = field(default_factory=list)
    zero_match_reason: str | None = None
    error: str | None = None


@dataclass
class DbCombinedRunResult:
    dbname: str
    status: str = "ok"
    chat_bot_ids: list[str] = field(default_factory=list)
    remap_results: list[BotRemapRunResult] = field(default_factory=list)
    post_sync_planned: int = 0
    post_sync_applied_estimate: int = 0
    post_sync_pg_planned: int = 0
    post_sync_pg_applied_estimate: int = 0
    post_sync_status: str = "skipped"
    post_url_count: int = 0
    learn_match_count: int = 0
    skipped_reason: str | None = None
    zero_match_reason: str | None = None
    changed_rows_by_bot: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class CombinedRunSummary:
    apply: bool
    dbname_filter: str | None
    chat_bot_id_filter: str | None
    old_cate_treecode: str
    new_cate_treecode: str
    db_type: str | None
    databases_found: int = 0
    databases_processed: int = 0
    databases_skipped: list[str] = field(default_factory=list)
    results: list[DbCombinedRunResult] = field(default_factory=list)
    skipped_dbs: list[dict[str, Any]] = field(default_factory=list)
    zero_match_dbs: list[dict[str, Any]] = field(default_factory=list)
    successful_dbs: list[dict[str, Any]] = field(default_factory=list)
    skipped_no_post_url_report: dict[str, Any] = field(default_factory=dict)
    missing_exploration_table_report: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def configure_logging(log_level: str | None) -> None:
    level_name = str(log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    LOGGER.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(level)


def _normalize_config_value(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() == "all":
        return None
    return text


def _is_all_value(value: Any) -> bool:
    return _normalize_config_value(value) is None


def _load_script_module(module_name: str, filename: str):
    module_path = SCRIPT_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"스크립트 모듈을 로드할 수 없습니다: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_operation_modules():
    remap_module = _load_script_module(
        "remap_learn_list_category_by_treecode_for_combined_run",
        "remap_learn_list_category_by_treecode.py",
    )
    sync_module = _load_script_module(
        "sync_post_content_type_from_exploration_for_combined_run",
        "sync_post_content_type_from_exploration.py",
    )
    return remap_module, sync_module


async def list_all_databases(*, execute_query, db_type: str | None) -> list[str]:
    rows = await execute_query(
        "SHOW DATABASES",
        (),
        dbname="information_schema",
        db_type=db_type,
        fetch="all",
    )
    dbnames: list[str] = []
    for row in rows or []:
        if isinstance(row, dict):
            value = next(iter(row.values()), "")
        elif isinstance(row, (tuple, list)):
            value = row[0] if row else ""
        else:
            try:
                value = row[0]
            except Exception:
                value = row
        dbname = str(value or "").strip()
        if not dbname or dbname.lower() in SYSTEM_DATABASES:
            continue
        dbnames.append(dbname)
    return sorted(dict.fromkeys(dbnames))


async def table_exists(*, dbname: str, table_name: str, execute_query, db_type: str | None) -> bool:
    row = await execute_query(
        """
        SELECT COUNT(*) AS count
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (dbname, table_name),
        dbname=dbname,
        db_type=db_type,
        fetch="one",
        as_dict=True,
    )
    if isinstance(row, dict):
        return int(row.get("count") or 0) > 0
    if isinstance(row, (tuple, list)) and row:
        return int(row[0] or 0) > 0
    try:
        return int(row["count"] or 0) > 0
    except Exception:
        return False


async def load_chat_bot_ids(
    *,
    dbname: str,
    chat_bot_id: str | None,
    execute_query,
    db_type: str | None,
) -> list[str]:
    if chat_bot_id:
        return [chat_bot_id]
    if not await table_exists(dbname=dbname, table_name="chatbot_setup", execute_query=execute_query, db_type=db_type):
        return []

    rows = await execute_query(
        """
        SELECT chat_bot_id
        FROM `chatbot_setup`
        WHERE chat_bot_id IS NOT NULL
          AND TRIM(chat_bot_id) <> ''
        ORDER BY chat_id
        """,
        (),
        dbname=dbname,
        db_type=db_type,
        fetch="all",
        as_dict=True,
    )
    result: list[str] = []
    seen: set[str] = set()
    for row in rows or []:
        value = str(row.get("chat_bot_id") if isinstance(row, dict) else row[0]).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


async def run_category_remap_for_bot(
    *,
    remap_module,
    dbname: str,
    chat_bot_id: str,
    old_cate_treecode: str,
    new_cate_treecode: str,
    apply: bool,
    chunk_size: int,
    db_type: str | None,
    selected_rows: list[dict[str, Any]] | None = None,
) -> BotRemapRunResult:
    summary = await remap_module.remap_learn_list_category_by_treecode(
        dbname=dbname,
        chat_bot_id=chat_bot_id,
        old_cate_treecode=old_cate_treecode,
        new_cate_treecode=new_cate_treecode,
        apply=apply,
        chunk_size=chunk_size,
        db_type=db_type,
        selected_rows=selected_rows,
    )
    status = "ok"
    if summary.errors or summary.missing_tables:
        status = "blocked"
    return BotRemapRunResult(
        chat_bot_id=chat_bot_id,
        status=status,
        updates_planned=summary.updates_planned,
        updates_applied_estimate=summary.updates_applied_estimate,
        selected_row_count=getattr(summary, "selected_row_count", 0),
        created_child_count=getattr(summary, "created_child_count", 0),
        created_categories=list(getattr(summary, "created_categories", []) or []),
        category_change_stats=list(getattr(summary, "category_change_stats", []) or []),
        zero_match_reason=getattr(summary, "zero_match_reason", None),
        error="; ".join(str(item) for item in [*summary.errors, *summary.missing_tables]) or None,
    )


async def run_post_content_type_sync(
    *,
    sync_module,
    dbname: str,
    chat_bot_id: str | None,
    apply: bool,
    allow_multiple_bots: bool,
    chunk_size: int,
    db_type: str | None,
    sync_pg_existing_post: bool,
    rdbms_retry_count: int,
    rdbms_retry_delay_seconds: float,
):
    return await sync_module.sync_post_content_type(
        dbname=dbname,
        apply=apply,
        allow_multiple_bots=allow_multiple_bots,
        chunk_size=chunk_size,
        chat_bot_id=chat_bot_id,
        db_type=db_type,
        sync_pg_existing_post=sync_pg_existing_post,
        rdbms_retry_count=rdbms_retry_count,
        rdbms_retry_delay_seconds=rdbms_retry_delay_seconds,
    )



def _sum_post_url_count(sync_summary: Any) -> int:
    results = getattr(sync_summary, "results", None)
    if results is None:
        return int(getattr(sync_summary, "updates_planned", 0) or 0)
    return sum(int(getattr(item, "post_url_count", 0) or 0) for item in results or [])


def _mode_text(apply: bool) -> str:
    return "실제반영" if apply else "점검"


def _status_text(status: str | None) -> str:
    return {
        "ok": "성공",
        "skipped": "건너뜀",
        "zero_match": "학습매칭없음",
        "blocked": "차단",
        "error": "오류",
    }.get(str(status or ""), str(status or ""))


def _reason_text(reason: str | None) -> str:
    return {
        "no_exploration_post_rows": "탐색목록에 post URL 없음",
        "exploration_table_missing": "탐색목록 테이블 없음",
        "no_learn_matches": "탐색 post URL과 매칭되는 학습 row 없음",
    }.get(str(reason or ""), str(reason or ""))


def _korean_skipped_dbs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "DB명": item.get("dbname"),
            "이유": _reason_text(item.get("reason")),
            "탐색_post_URL수": item.get("post_url_count", 0),
        }
        for item in items
    ]


def _korean_zero_match_dbs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "DB명": item.get("dbname"),
            "이유": _reason_text(item.get("reason")),
            "탐색_post_URL수": item.get("post_url_count", 0),
            "MariaDB_LEARN_LIST_매칭수": item.get("learn_match_count", 0),
        }
        for item in items
    ]


def _report_document(summary: CombinedRunSummary) -> dict[str, Any]:
    return {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "apply": summary.apply,
        "dbname_filter": summary.dbname_filter,
        "chat_bot_id_filter": summary.chat_bot_id_filter,
        "기존_카테고리_treecode": summary.old_cate_treecode,
        "새_카테고리_treecode": summary.new_cate_treecode,
        "DB타입": summary.db_type,
        "조회_DB수": summary.databases_found,
        "처리_DB수": summary.databases_processed,
        "추가_수정_DB별_보고서": summary.successful_dbs,
        "탐색목록_post_URL_없음_누적보고서": summary.skipped_no_post_url_report,
        "탐색목록_테이블_없음_누적보고서": summary.missing_exploration_table_report,
        "건너뛴_DB": _korean_skipped_dbs(summary.skipped_dbs),
        "학습매칭_0건_DB": _korean_zero_match_dbs(summary.zero_match_dbs),
        "오류": summary.errors,
    }


def _safe_report_stem(dbname: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(dbname or "").strip())
    return safe or "unknown_db"


def _write_json_report_file(path: Path, document: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(document, ensure_ascii=False, indent=2)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    try:
        os.replace(tmp_path, path)
        return path
    except PermissionError as exc:
        fallback_path = path.with_name(
            f"{path.stem}__locked_{__import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{os.getpid()}{path.suffix}"
        )
        fallback_path.write_text(payload, encoding="utf-8")
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        LOGGER.warning(
            "[보고서 저장 경로 잠김] 기본 파일을 교체하지 못해 대체 파일로 저장했습니다. 기본=%s 대체=%s 오류=%s",
            path,
            fallback_path,
            exc,
        )
        return fallback_path


def _post_change_stats(db_result: DbCombinedRunResult) -> dict[str, Any]:
    by_bot: list[dict[str, Any]] = []
    for bot_id, changed_rows in sorted((db_result.changed_rows_by_bot or {}).items()):
        original_cate2_counts: dict[str, int] = {}
        for row in changed_rows or []:
            original_cate2 = str(row.get("original_cate2") or "").strip()
            original_cate2_counts[original_cate2 or "(blank)"] = original_cate2_counts.get(original_cate2 or "(blank)", 0) + 1
        by_bot.append(
            {
                "chat_bot_id": bot_id,
                "LEARN_LIST_post_변경수": len(changed_rows or []),
                "기존_cate2별_건수": [
                    {"기존_cate2": cate2, "건수": count}
                    for cate2, count in sorted(original_cate2_counts.items())
                ],
            }
        )
    return {
        "탐색목록_post_URL수": db_result.post_url_count,
        "LEARN_LIST_post_변경수": sum(item["LEARN_LIST_post_변경수"] for item in by_bot),
        "PG_post_변경대상수": db_result.post_sync_pg_planned,
        "PG_post_반영수": db_result.post_sync_pg_applied_estimate,
        "bot별": by_bot,
    }


def _representative_post_urls(db_result: DbCombinedRunResult, limit: int = 100) -> list[dict[str, str]]:
    samples: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for bot_id, changed_rows in sorted((db_result.changed_rows_by_bot or {}).items()):
        for row in changed_rows or []:
            url = str(row.get("url") or row.get("content") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            samples.append({"chat_bot_id": str(bot_id), "url": url})
            if len(samples) >= limit:
                return samples
    return samples


def _flatten_category_change_stats(db_result: DbCombinedRunResult) -> list[dict[str, Any]]:
    stats: list[dict[str, Any]] = []
    for remap in db_result.remap_results:
        for item in remap.category_change_stats or []:
            stat = dict(item)
            stat.setdefault("chat_bot_id", remap.chat_bot_id)
            stats.append(
                {
                    "chat_bot_id": stat.get("chat_bot_id"),
                    "기존_cate2": stat.get("old_cate2"),
                    "기존_cate2_treecode": stat.get("old_cate2_treecode"),
                    "새_cate2": stat.get("new_cate2"),
                    "새_cate2_treecode": stat.get("new_cate2_treecode"),
                    "카테고리명": stat.get("cate_name"),
                    "변경row수": stat.get("row_count", 0),
                }
            )
    return stats


def _category_change_totals(db_result: DbCombinedRunResult) -> list[dict[str, Any]]:
    totals: dict[tuple[str, str], dict[str, Any]] = {}
    for stat in _flatten_category_change_stats(db_result):
        key = (str(stat.get("기존_cate2") or ""), str(stat.get("새_cate2") or ""))
        total = totals.setdefault(
            key,
            {
                "기존_cate2": stat.get("기존_cate2"),
                "새_cate2": stat.get("새_cate2"),
                "카테고리명": stat.get("카테고리명"),
                "변경row수": 0,
            },
        )
        total["변경row수"] += int(stat.get("변경row수") or 0)
    return sorted(
        totals.values(),
        key=lambda item: (str(item.get("카테고리명") or ""), str(item.get("기존_cate2") or ""), str(item.get("새_cate2") or "")),
    )


def _flatten_created_categories(db_result: DbCombinedRunResult) -> list[dict[str, Any]]:
    categories: list[dict[str, Any]] = []
    for remap in db_result.remap_results:
        for category in remap.created_categories or []:
            item = dict(category)
            item.setdefault("chat_bot_id", remap.chat_bot_id)
            categories.append(
                {
                    "chat_bot_id": item.get("chat_bot_id"),
                    "cate_code": item.get("cate_code"),
                    "cate_treecode": item.get("cate_treecode"),
                    "카테고리명": item.get("cate_name"),
                    "url": item.get("url"),
                    "상위_cate_code": item.get("parent_cate_code"),
                    "상위_cate_treecode": item.get("parent_cate_treecode"),
                    "원본_cate_code": item.get("source_cate_code"),
                    "원본_cate_treecode": item.get("source_cate_treecode"),
                    "구분": "게시판" if item.get("category_role") == "board_parent" else "게시판 하위",
                }
            )
    return categories


def _db_success_report_document(summary: CombinedRunSummary, db_result: DbCombinedRunResult) -> dict[str, Any]:
    created_categories = _flatten_created_categories(db_result)
    post_change_stats = _post_change_stats(db_result)
    if not summary.apply:
        representative_urls = _representative_post_urls(db_result, limit=100)
        post_change_stats["대표_URL_최대100개수"] = len(representative_urls)
        post_change_stats["대표_URL_최대100개"] = representative_urls
    category_totals = _category_change_totals(db_result)
    return {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "dbname": db_result.dbname,
        "apply": summary.apply,
        "status": db_result.status,
        "탐색목록_post_URL수": db_result.post_url_count,
        "MariaDB_LEARN_LIST_매칭수": db_result.post_sync_planned,
        "PG_매칭수": db_result.post_sync_pg_planned,
        "content_type_post_변경": {
            "상태": _status_text(db_result.post_sync_status),
            "MariaDB_LEARN_LIST_변경대상수": db_result.post_sync_planned,
            "LEARN_LIST_반영수": db_result.post_sync_applied_estimate,
            "PG_변경대상수": db_result.post_sync_pg_planned,
            "PG_반영수": db_result.post_sync_pg_applied_estimate,
        },
        "post_변경통계": post_change_stats,
        "카테고리_변경통계": {
            "전체_변경row수": sum(int(item.get("변경row수") or 0) for item in category_totals),
            "cate2별": category_totals,
            "bot별": _flatten_category_change_stats(db_result),
        },
        "생성카테고리": created_categories,
        "오류": db_result.errors,
    }


def _db_has_reportable_changes(summary: CombinedRunSummary, db_result: DbCombinedRunResult) -> bool:
    created_category_count = sum(len(remap.created_categories or []) for remap in db_result.remap_results)
    category_planned = sum(remap.updates_planned for remap in db_result.remap_results)
    category_applied = sum(remap.updates_applied_estimate for remap in db_result.remap_results)
    if summary.apply:
        return any(
            [
                db_result.post_sync_applied_estimate > 0,
                db_result.post_sync_pg_applied_estimate > 0,
                category_applied > 0,
                created_category_count > 0,
            ]
        )
    return any(
        [
            db_result.post_sync_planned > 0,
            db_result.post_sync_pg_planned > 0,
            category_planned > 0,
            created_category_count > 0,
        ]
    )


def write_db_success_reports(summary: CombinedRunSummary, base_dir: Path) -> list[dict[str, Any]]:
    base_dir.mkdir(parents=True, exist_ok=True)
    written: list[dict[str, Any]] = []
    for db_result in summary.results:
        if db_result.status != "ok":
            continue
        if not _db_has_reportable_changes(summary, db_result):
            continue
        document = _db_success_report_document(summary, db_result)
        path = base_dir / f"{_safe_report_stem(db_result.dbname)}.json"
        actual_path = _write_json_report_file(path, document)
        written.append(
            {
                "dbname": db_result.dbname,
                "report_path": str(actual_path),
                "탐색목록_post_URL수": db_result.post_url_count,
                "MariaDB_LEARN_LIST_매칭수": db_result.post_sync_planned,
                "PG_매칭수": db_result.post_sync_pg_planned,
                "changed_post_url_count": document["post_변경통계"]["LEARN_LIST_post_변경수"],
                "created_category_count": len(document["생성카테고리"]),
            }
        )
    return written


def _skipped_exploration_report_document(summary: CombinedRunSummary, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "apply": summary.apply,
        "건너뛴_이유": "탐색목록에 post URL 없음",
        "count": len(items),
        "items": [
            {
                "dbname": item.get("dbname"),
                "status": "skipped",
                "탐색목록_post_URL수": item.get("post_url_count", 0),
            }
            for item in items
        ],
        "설명": "탐색목록 테이블에서 type=post URL이 조회되지 않아 이 DB는 content_type 변경과 카테고리 보정을 진행하지 않았습니다.",
    }


def write_skipped_exploration_report(summary: CombinedRunSummary, path: Path) -> dict[str, Any]:
    items = [item for item in summary.skipped_dbs if item.get("reason") == "no_exploration_post_rows"]
    if not items:
        return {}
    document = _skipped_exploration_report_document(summary, items)
    actual_path = _write_json_report_file(path, document)
    return {
        "report_path": str(actual_path),
        "이유": document["건너뛴_이유"],
        "count": document["count"],
    }


def _missing_exploration_table_report_document(summary: CombinedRunSummary, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "apply": summary.apply,
        "건너뛴_이유": "탐색목록 테이블 없음",
        "없는_테이블": "ASADAL_CRAWLING_EXPLORATION",
        "count": len(items),
        "items": [
            {
                "dbname": item.get("dbname"),
                "status": "skipped",
                "없는_테이블": "ASADAL_CRAWLING_EXPLORATION",
                "탐색목록_post_URL수": item.get("post_url_count", 0),
            }
            for item in items
        ],
        "설명": "탐색목록 테이블 ASADAL_CRAWLING_EXPLORATION 이 없어 이 DB는 content_type 변경과 카테고리 보정을 진행하지 않았습니다.",
    }


def write_missing_exploration_table_report(summary: CombinedRunSummary, path: Path) -> dict[str, Any]:
    items = [item for item in summary.skipped_dbs if item.get("reason") == "exploration_table_missing"]
    if not items:
        return {}
    document = _missing_exploration_table_report_document(summary, items)
    actual_path = _write_json_report_file(path, document)
    return {
        "report_path": str(actual_path),
        "이유": document["건너뛴_이유"],
        "없는_테이블": document["없는_테이블"],
        "count": document["count"],
    }


def write_report(summary: CombinedRunSummary, path: Path = REPORT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary.successful_dbs = write_db_success_reports(summary, path.parent / "remap-sync-post-category-flow")
    summary.skipped_no_post_url_report = write_skipped_exploration_report(
        summary,
        path.parent / "remap-sync-post-category-flow-skipped-no-post-url.json",
    )
    summary.missing_exploration_table_report = write_missing_exploration_table_report(
        summary,
        path.parent / "remap-sync-post-category-flow-skipped-missing-exploration-table.json",
    )
    _write_json_report_file(path, _report_document(summary))

async def run_combined_for_db(
    *,
    dbname: str,
    chat_bot_id: str | None,
    old_cate_treecode: str,
    new_cate_treecode: str,
    apply: bool,
    category_chunk_size: int,
    post_sync_chunk_size: int,
    db_type: str | None,
    sync_pg_existing_post: bool,
    sync_allow_multiple_bots: bool,
    rdbms_retry_count: int,
    rdbms_retry_delay_seconds: float,
    run_category_remap: bool,
    run_post_content_type_sync_step: bool,
    stop_on_error: bool,
    remap_module,
    sync_module,
) -> DbCombinedRunResult:
    result = DbCombinedRunResult(dbname=dbname)
    LOGGER.info("[DB 처리 시작] [DB:%s] 대상 bot=%s 실행모드=%s", dbname, chat_bot_id or "전체", "실제반영" if apply else "점검")

    sync_summary = None
    if run_post_content_type_sync_step:
        sync_chat_bot_id = chat_bot_id if chat_bot_id else None
        try:
            LOGGER.info("[1단계 시작: content_type=post 변경 대상 확인] [DB:%s] 대상 bot=%s", dbname, sync_chat_bot_id or "전체")
            sync_summary = await run_post_content_type_sync(
                sync_module=sync_module,
                dbname=dbname,
                chat_bot_id=sync_chat_bot_id,
                apply=apply,
                allow_multiple_bots=sync_allow_multiple_bots,
                chunk_size=post_sync_chunk_size,
                db_type=db_type,
                sync_pg_existing_post=sync_pg_existing_post,
                rdbms_retry_count=rdbms_retry_count,
                rdbms_retry_delay_seconds=rdbms_retry_delay_seconds,
            )
            result.post_sync_status = "ok" if not sync_summary.apply_blocked_reason else "blocked"
            result.post_sync_planned = sync_summary.updates_planned
            result.post_sync_applied_estimate = sync_summary.updates_applied_estimate
            result.post_sync_pg_planned = sync_summary.pg_updates_planned
            result.post_sync_pg_applied_estimate = sync_summary.pg_updates_applied_estimate
            result.post_url_count = _sum_post_url_count(sync_summary)
            result.learn_match_count = sync_summary.updates_planned
            result.changed_rows_by_bot = dict(getattr(sync_summary, "changed_rows_by_bot", {}) or {})
            if getattr(sync_summary, "skipped_reason", None) == "exploration_table_missing":
                result.status = "skipped"
                result.skipped_reason = "exploration_table_missing"
            elif result.post_url_count == 0:
                result.status = "skipped"
                result.skipped_reason = "no_exploration_post_rows"
            elif result.learn_match_count == 0:
                result.zero_match_reason = "no_learn_matches"
            if sync_summary.apply_blocked_reason:
                result.errors.append(sync_summary.apply_blocked_reason)
            result.errors.extend(str(item) for item in sync_summary.errors)
            LOGGER.info(
                "[1단계 완료: content_type=post] [DB:%s] 상태=%s | LEARN_LIST 반영/대상=%s/%s | PG 반영/대상=%s/%s | 탐색 post URL=%s",
                dbname,
                result.post_sync_status,
                result.post_sync_applied_estimate,
                result.post_sync_planned,
                result.post_sync_pg_applied_estimate,
                result.post_sync_pg_planned,
                result.post_url_count,
            )
            if stop_on_error and result.post_sync_status != "ok":
                raise RuntimeError("; ".join(result.errors) or "post sync blocked")
        except Exception as exc:
            result.status = "error"
            result.post_sync_status = "error"
            result.errors.append(f"content_type=post 변경 단계: {exc}")
            LOGGER.exception("[1단계 실패: content_type=post] [DB:%s] 오류=%s", dbname, exc)
            if stop_on_error:
                raise

    if result.skipped_reason:
        LOGGER.warning("[DB 건너뜀] [DB:%s] 이유=%s", dbname, _reason_text(result.skipped_reason))
        return result

    execute_query = sync_module._standalone_execute_rdbms_query
    bot_ids = list(result.changed_rows_by_bot.keys())
    if not bot_ids and (not run_post_content_type_sync_step or chat_bot_id):
        bot_ids = await load_chat_bot_ids(
            dbname=dbname,
            chat_bot_id=chat_bot_id,
            execute_query=execute_query,
            db_type=db_type,
        )
    result.chat_bot_ids = bot_ids

    if run_category_remap:
        if not bot_ids:
            if result.post_url_count > 0:
                result.zero_match_reason = result.zero_match_reason or "no_learn_matches"
            LOGGER.warning("[2단계 건너뜀: 카테고리 변경] [DB:%s] 이유=학습 테이블에 매칭된 post URL 없음")
        for index, bot_id in enumerate(bot_ids, start=1):
            selected_rows = result.changed_rows_by_bot.get(bot_id, [])
            if run_post_content_type_sync_step and not selected_rows:
                continue
            try:
                LOGGER.info(
                    "[2단계 시작: post 카테고리 변경] [DB:%s] bot=%s/%s chat_bot_id=%s 변경대상 URL=%s",
                    dbname,
                    index,
                    len(bot_ids),
                    bot_id,
                    len(selected_rows),
                )
                remap_result = await run_category_remap_for_bot(
                    remap_module=remap_module,
                    dbname=dbname,
                    chat_bot_id=bot_id,
                    old_cate_treecode=old_cate_treecode,
                    new_cate_treecode=new_cate_treecode,
                    apply=apply,
                    chunk_size=category_chunk_size,
                    db_type=db_type,
                    selected_rows=selected_rows,
                )
                result.remap_results.append(remap_result)
                if remap_result.zero_match_reason:
                    result.zero_match_reason = remap_result.zero_match_reason
                LOGGER.info(
                    "[2단계 완료: post 카테고리 변경] [DB:%s] chat_bot_id=%s 상태=%s | LEARN_LIST cate1/cate2 반영/대상=%s/%s | 생성 카테고리=%s",
                    dbname,
                    bot_id,
                    remap_result.status,
                    remap_result.updates_applied_estimate,
                    remap_result.updates_planned,
                    remap_result.created_child_count,
                )
                if remap_result.status != "ok":
                    blocked_message = f"remap chat_bot_id={bot_id}: {remap_result.error or remap_result.status}"
                    result.errors.append(blocked_message)
                    if stop_on_error:
                        raise RuntimeError(blocked_message)
            except Exception as exc:
                result.status = "error"
                result.errors.append(f"카테고리 변경 chat_bot_id={bot_id}: {exc}")
                LOGGER.exception("[2단계 실패: post 카테고리 변경] [DB:%s] chat_bot_id=%s 오류=%s", dbname, bot_id, exc)
                if stop_on_error:
                    raise

    if result.errors and result.status == "ok":
        result.status = "blocked"
    elif result.zero_match_reason and result.status == "ok":
        result.status = "zero_match"
    LOGGER.info("[DB 처리 완료] [DB:%s] 상태=%s 오류수=%s", dbname, result.status, len(result.errors))
    return result


async def run_combined(
    *,
    dbname: str | None,
    chat_bot_id: str | None,
    old_cate_treecode: str,
    new_cate_treecode: str,
    apply: bool,
    category_chunk_size: int,
    post_sync_chunk_size: int,
    db_type: str | None,
    sync_pg_existing_post: bool,
    sync_allow_multiple_bots: bool,
    rdbms_retry_count: int,
    rdbms_retry_delay_seconds: float,
    run_category_remap: bool,
    run_post_content_type_sync_step: bool,
    stop_on_error: bool,
    log_level: str,
) -> CombinedRunSummary:
    remap_module, sync_module = load_operation_modules()
    remap_module.configure_logging(log_level)
    sync_module.configure_logging(log_level)

    dbname_filter = _normalize_config_value(dbname)
    chat_bot_id_filter = _normalize_config_value(chat_bot_id)
    summary = CombinedRunSummary(
        apply=apply,
        dbname_filter=dbname_filter,
        chat_bot_id_filter=chat_bot_id_filter,
        old_cate_treecode=old_cate_treecode,
        new_cate_treecode=new_cate_treecode,
        db_type=db_type,
    )

    try:
        if dbname_filter:
            dbnames = [dbname_filter]
        else:
            dbnames = await list_all_databases(
                execute_query=sync_module._standalone_execute_rdbms_query,
                db_type=db_type,
            )
        summary.databases_found = len(dbnames)

        wrote_report = False
        for index, current_dbname in enumerate(dbnames, start=1):
            LOGGER.info("[전체 진행] %s/%s번째 DB 처리 [DB:%s]", index, len(dbnames), current_dbname)
            try:
                db_result = await run_combined_for_db(
                    dbname=current_dbname,
                    chat_bot_id=chat_bot_id_filter,
                    old_cate_treecode=old_cate_treecode,
                    new_cate_treecode=new_cate_treecode,
                    apply=apply,
                    category_chunk_size=category_chunk_size,
                    post_sync_chunk_size=post_sync_chunk_size,
                    db_type=db_type,
                    sync_pg_existing_post=sync_pg_existing_post,
                    sync_allow_multiple_bots=sync_allow_multiple_bots,
                    rdbms_retry_count=rdbms_retry_count,
                    rdbms_retry_delay_seconds=rdbms_retry_delay_seconds,
                    run_category_remap=run_category_remap,
                    run_post_content_type_sync_step=run_post_content_type_sync_step,
                    stop_on_error=stop_on_error,
                    remap_module=remap_module,
                    sync_module=sync_module,
                )
                summary.results.append(db_result)
                summary.databases_processed += 1
                if db_result.skipped_reason in {"no_exploration_post_rows", "exploration_table_missing"}:
                    summary.skipped_dbs.append({"dbname": current_dbname, "reason": db_result.skipped_reason, "post_url_count": db_result.post_url_count})
                if db_result.zero_match_reason == "no_learn_matches":
                    summary.zero_match_dbs.append({"dbname": current_dbname, "reason": "no_learn_matches", "post_url_count": db_result.post_url_count, "learn_match_count": db_result.learn_match_count})
                if db_result.status not in {"ok", "skipped", "zero_match"}:
                    summary.errors.append({"dbname": current_dbname, "error": "; ".join(db_result.errors)})
                write_report(summary)
                wrote_report = True
            except Exception as exc:
                summary.databases_skipped.append(current_dbname)
                summary.errors.append({"dbname": current_dbname, "error": str(exc)})
                LOGGER.exception("[DB 처리 실패] [DB:%s] 오류=%s", current_dbname, exc)
                write_report(summary)
                wrote_report = True
                if stop_on_error:
                    raise
        if not wrote_report:
            write_report(summary)
    finally:
        await remap_module._close_standalone_db_pools()
        await sync_module._close_standalone_db_pools()

    return summary


def render_text_summary(summary: CombinedRunSummary) -> str:
    lines = [
        "[post URL content_type + 카테고리 보정 스크립트]",
        f"실행모드: {'실제반영(APPLY)' if summary.apply else '점검(DRY-RUN)'}",
        f"대상 DB: {summary.dbname_filter or '전체'}",
        f"대상 bot: {summary.chat_bot_id_filter or '전체'}",
        f"카테고리 기준: {summary.old_cate_treecode} -> {summary.new_cate_treecode}",
        f"조회 DB 수: {summary.databases_found}",
        f"처리 DB 수: {summary.databases_processed}",
        "",
    ]
    total_remap_planned = sum(item.updates_planned for db in summary.results for item in db.remap_results)
    total_remap_applied = sum(item.updates_applied_estimate for db in summary.results for item in db.remap_results)
    total_sync_planned = sum(db.post_sync_planned for db in summary.results)
    total_sync_applied = sum(db.post_sync_applied_estimate for db in summary.results)
    total_pg_planned = sum(db.post_sync_pg_planned for db in summary.results)
    total_pg_applied = sum(db.post_sync_pg_applied_estimate for db in summary.results)
    lines.extend(
        [
            "전체 예정/결과:",
            f"  - LEARN_LIST cate1/cate2 변경: 대상={total_remap_planned} 반영={total_remap_applied}",
            f"  - LEARN_LIST content_type post 변경: 대상={total_sync_planned} 반영={total_sync_applied}",
            f"  - PG training_data content_type post 변경: 대상={total_pg_planned} 반영={total_pg_applied}",
            "",
        ]
    )
    for db in summary.results:
        lines.append(f"- DB={db.dbname} 상태={db.status} bot수={len(db.chat_bot_ids)}")
        if db.remap_results:
            remap_planned = sum(item.updates_planned for item in db.remap_results)
            remap_applied = sum(item.updates_applied_estimate for item in db.remap_results)
            created_count = sum(item.created_child_count for item in db.remap_results)
            lines.append(f"  카테고리 변경: 대상={remap_planned} 반영={remap_applied} 생성예정/생성={created_count}")
        else:
            lines.append("  카테고리 변경: 건너뜀/대상 bot 없음")
        lines.append(
            f"  content_type=post: 상태={db.post_sync_status} "
            f"LEARN_LIST 대상={db.post_sync_planned} 반영={db.post_sync_applied_estimate} "
            f"PG 대상={db.post_sync_pg_planned} 반영={db.post_sync_pg_applied_estimate} "
            f"탐색 post URL={db.post_url_count}"
        )
        if db.errors:
            lines.append("  오류:")
            lines.extend(f"    - {error}" for error in db.errors[:10])
            if len(db.errors) > 10:
                lines.append(f"    ... and {len(db.errors) - 10} more")
    if summary.skipped_dbs:
        lines.append("")
        lines.append("건너뛴 DB:")
        lines.extend(f"  - {item['dbname']} ({_reason_text(item.get('reason'))})" for item in summary.skipped_dbs)
    if summary.zero_match_dbs:
        lines.append("")
        lines.append("학습 매칭 0건 DB:")
        lines.extend(f"  - {item['dbname']} (탐색 post URL={item['post_url_count']}, 학습 매칭={item['learn_match_count']})" for item in summary.zero_match_dbs)
    if summary.successful_dbs:
        lines.append("")
        lines.append("추가/수정 DB별 상세 보고서:")
        lines.extend(
            (
                f"  - {item['dbname']}: {item['report_path']} "
                f"(post 변경 URL={item['changed_post_url_count']}, 생성 카테고리={item['created_category_count']})"
            )
            for item in summary.successful_dbs
        )
    if summary.databases_skipped:
        lines.append("")
        lines.append("오류로 건너뛴 DB:")
        lines.extend(f"  - {dbname}" for dbname in summary.databases_skipped)
    if summary.errors:
        lines.append("")
        lines.append("오류:")
        lines.extend(f"  - {item}" for item in summary.errors[:20])
        if len(summary.errors) > 20:
            lines.append(f"  ... and {len(summary.errors) - 20} more")
    if not summary.apply:
        lines.append("")
        lines.append("점검 모드입니다: 실제 INSERT/UPDATE는 하지 않았습니다. 적용하려면 --apply를 붙여 실행하세요.")
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run category treecode remap and post content_type sync together. "
            "If dbname is omitted/empty/all, all MariaDB databases are processed sequentially."
        )
    )
    parser.add_argument("dbname_arg", nargs="?", help="Optional target db_name. Omit for all DBs.")
    parser.add_argument(
        "--dbname",
        "--db-name",
        "--db_name",
        dest="dbname",
        default=_normalize_config_value(TARGET_DBNAME),
        help="Target DB name. Empty/all means all DBs sequentially.",
    )
    parser.add_argument(
        "--chat-bot-id",
        "--chat_bot_id",
        "--chat-bpt-id",
        "--chat_bpt_id",
        dest="chat_bot_id",
        default=_normalize_config_value(TARGET_CHAT_BOT_ID),
        help="Optional chat_bot_id. Empty/all means all bot ids per DB for remap, and all post bots for sync.",
    )
    parser.add_argument(
        "--old-cate-treecode",
        "--old_cate_treecode",
        default=OLD_CATE_TREECODE,
        help="Current parent cate_treecode to move from.",
    )
    parser.add_argument(
        "--new-cate-treecode",
        "--new_cate_treecode",
        default=NEW_CATE_TREECODE,
        help="New parent cate_treecode to move to.",
    )
    parser.add_argument("--apply", action="store_true", default=APPLY_CHANGES, help="Actually run UPDATE statements.")
    parser.add_argument("--category-chunk-size", type=int, default=CATEGORY_CHUNK_SIZE)
    parser.add_argument("--post-sync-chunk-size", type=int, default=POST_SYNC_CHUNK_SIZE)
    parser.add_argument("--db-type", choices=("maria", "mysql"), default=_normalize_config_value(TARGET_DB_TYPE))
    parser.add_argument("--sync-pg-existing-post", action="store_true", default=SYNC_PG_EXISTING_POST)
    parser.add_argument("--sync-allow-multiple-bots", action="store_true", default=SYNC_ALLOW_MULTIPLE_BOTS)
    parser.add_argument("--rdbms-retry-count", type=int, default=RDBMS_DEADLOCK_RETRY_COUNT)
    parser.add_argument("--rdbms-retry-delay-seconds", type=float, default=RDBMS_DEADLOCK_RETRY_DELAY_SECONDS)
    parser.add_argument("--skip-category-remap", action="store_true", help="Run only post content_type sync.")
    parser.add_argument("--skip-post-sync", action="store_true", help="Run only category remap.")
    parser.add_argument("--stop-on-error", action="store_true", default=STOP_ON_ERROR)
    parser.add_argument("--json", action="store_true", default=JSON_OUTPUT)
    parser.add_argument(
        "--log-level",
        default=TARGET_LOG_LEVEL,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
    )
    return parser


async def async_main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    dbname = args.dbname_arg if args.dbname_arg is not None else args.dbname
    summary = await run_combined(
        dbname=dbname,
        chat_bot_id=args.chat_bot_id,
        old_cate_treecode=args.old_cate_treecode,
        new_cate_treecode=args.new_cate_treecode,
        apply=args.apply,
        category_chunk_size=args.category_chunk_size,
        post_sync_chunk_size=args.post_sync_chunk_size,
        db_type=args.db_type,
        sync_pg_existing_post=args.sync_pg_existing_post,
        sync_allow_multiple_bots=args.sync_allow_multiple_bots,
        rdbms_retry_count=args.rdbms_retry_count,
        rdbms_retry_delay_seconds=args.rdbms_retry_delay_seconds,
        run_category_remap=not args.skip_category_remap,
        run_post_content_type_sync_step=not args.skip_post_sync,
        stop_on_error=args.stop_on_error,
        log_level=args.log_level,
    )
    if args.json:
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(render_text_summary(summary))
    return 1 if summary.errors and args.stop_on_error else 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
