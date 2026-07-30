import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qsl, urlparse

from backend.shared.stage_url_report import _atomic_write_json, _downloads_dir, _safe_part

logger = logging.getLogger("backend.shared.job_result_report")


def _now_iso() -> str:
    try:
        return datetime.now().isoformat(timespec="seconds")
    except Exception:
        return str(int(time.time()))


def _clean_url(url: Any) -> str:
    try:
        return str(url or "").strip()
    except Exception:
        return ""


def _as_url_entries(urls: Iterable[Any]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in urls or []:
        if isinstance(item, dict):
            url = _clean_url(item.get("url"))
            entry = dict(item)
        else:
            url = _clean_url(item)
            entry = {"url": url}
        if not url or url in seen:
            continue
        entry["url"] = url
        entries.append(entry)
        seen.add(url)
    return entries


def _load_json(path: str) -> Dict[str, Any]:
    try:
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            if isinstance(data, dict):
                return data
    except Exception:
        logger.debug("[JobResultReport] json load failed | path=%s", path, exc_info=True)
    return {}


def _stage_urls(trace_data: Dict[str, Any], stage: str) -> List[Dict[str, Any]]:
    try:
        stage_obj = ((trace_data.get("stages") or {}).get(stage) or {})
        urls = stage_obj.get("urls") or []
        if isinstance(urls, list):
            return _as_url_entries(urls)
    except Exception:
        pass
    return []


def _workflow_list(workflow: Any, attr: str) -> List[Dict[str, Any]]:
    try:
        value = getattr(workflow, attr, None)
        if isinstance(value, (set, list, tuple)):
            return _as_url_entries(value)
    except Exception:
        pass
    return []


def _merge_entries(*groups: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for group in groups:
        for entry in group or []:
            url = _clean_url((entry or {}).get("url"))
            if not url:
                continue
            if url not in merged:
                merged[url] = {"url": url}
            try:
                merged[url].update({k: v for k, v in dict(entry).items() if v is not None})
            except Exception:
                pass
    return list(merged.values())


def _read_detail_failures(workflow: Any, *, job_id: Optional[str], db_name: Optional[str], limit: int = 5000) -> List[Dict[str, Any]]:
    try:
        path_func = getattr(workflow, "_detail_failure_log_path", None)
        path = path_func() if callable(path_func) else ""
    except Exception:
        path = ""
    if not path or not os.path.exists(path):
        return []

    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as fp:
            for line in fp:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except Exception:
                    continue
                if not isinstance(record, dict):
                    continue
                if job_id and str(record.get("job_id") or "") != str(job_id):
                    continue
                if db_name and str(record.get("db_name") or "") and str(record.get("db_name") or "") != str(db_name):
                    continue
                rows.append(record)
                if len(rows) >= limit:
                    rows.pop(0)
    except Exception:
        logger.debug("[JobResultReport] detail failure scan failed | path=%s", path, exc_info=True)
    return rows


def _is_failure_event(record: Dict[str, Any]) -> bool:
    event = str(record.get("event") or "").strip().lower()
    if not event:
        return True
    if any(token in event for token in ("recovered", "retry_ok", "fallback_ok", "success")):
        return False
    if any(token in event for token in ("fail", "failed", "timeout", "empty", "exception", "error")):
        return True
    return False


def _dedupe_failures(failures: Any) -> List[Dict[str, Any]]:
    if not isinstance(failures, list):
        return []
    deduped: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for failure in failures:
        if not isinstance(failure, dict):
            continue
        if not _is_failure_event(failure):
            continue
        key = (
            str(failure.get("stage") or ""),
            str(failure.get("reason") or ""),
            str(failure.get("event") or ""),
            str(failure.get("ts") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(failure)
    return deduped


def _merge_status(result: Dict[str, Any], stage: str, status: str, reason: str = "", **fields: Any) -> None:
    stage_obj = result.setdefault(stage, {})
    if not isinstance(stage_obj, dict):
        stage_obj = {}
        result[stage] = stage_obj
    if status:
        previous = str(stage_obj.get("status") or "")
        if previous != "success" or status == "success":
            stage_obj["status"] = status
    if reason and not stage_obj.get("reason"):
        stage_obj["reason"] = reason
    for key, value in fields.items():
        if value is not None and key not in {"url", "stage", "status", "reason"}:
            stage_obj[key] = value


def _missing_required_detail_query_reason(url: Any) -> str:
    text = _clean_url(url)
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    keys = {
        str(key or "").strip().lower()
        for key, _value in parse_qsl(parsed.query or "", keep_blank_values=False)
        if str(key or "").strip()
    }
    required_by_path = (
        (
            "ne.go.kr",
            "/platform/user/pblccomm/bd_pblcdiscussionview.do",
            {"discussionid"},
        ),
    )
    for host_hint, path_hint, required_keys in required_by_path:
        if host_hint in host and path.endswith(path_hint):
            missing = sorted(str(key) for key in required_keys if str(key).lower() not in keys)
            if missing:
                return "missing_required_detail_query:" + ",".join(missing)
    return ""


def _merge_attachment(
    parent: Dict[str, Any],
    *,
    file_url: str,
    file_name: str = "",
    stage: str = "",
    status: str = "",
    reason: str = "",
    **fields: Any,
) -> None:
    file_url = _clean_url(file_url)
    if not file_url:
        return
    attachments = parent.setdefault("attachments", [])
    if not isinstance(attachments, list):
        attachments = []
        parent["attachments"] = attachments
    attachment = None
    for item in attachments:
        if isinstance(item, dict) and _clean_url(item.get("file_url") or item.get("url")) == file_url:
            attachment = item
            break
    if attachment is None:
        attachment = {
            "file_name": file_name or "",
            "file_url": file_url,
            "save": {"status": "unknown"},
            "study": {"status": "unknown"},
            "failures": [],
        }
        attachments.append(attachment)
    if file_name and not attachment.get("file_name"):
        attachment["file_name"] = file_name
    for key, value in fields.items():
        if value is not None and key not in {"url", "stage", "status", "reason"}:
            attachment[key] = value
    if stage in {"save", "study"}:
        _merge_status(attachment, stage, status or "unknown", reason)
    elif status == "failed":
        attachment.setdefault("failures", []).append({"stage": stage or "file", "reason": reason or "failed"})
    elif stage in {"selection", "file_attachment", "collection"}:
        attachment["selected"] = status != "failed"
        if reason:
            attachment["selection_reason"] = reason


def _build_report_sync(workflow: Any, *, job_id: Optional[str], db_name: Optional[str], status: Optional[str]) -> Dict[str, Any]:
    db_key = _safe_part(db_name)
    job_key = _safe_part(job_id)
    trace_path = os.path.join(_downloads_dir(), f"trace_{db_key}_{job_key}.json")
    trace_data = _load_json(trace_path)

    scan_urls = _merge_entries(
        _workflow_list(workflow, "_crawled_urls_snapshot"),
        _workflow_list(workflow, "_seen_scan"),
        _stage_urls(trace_data, "scan"),
    )
    selected_urls = _merge_entries(_workflow_list(workflow, "_seen_filtered_detail"), _stage_urls(trace_data, "collection"))
    save_urls = _stage_urls(trace_data, "save")
    study_urls = _stage_urls(trace_data, "study")

    results: Dict[str, Dict[str, Any]] = {}

    def ensure(url: Any) -> Optional[Dict[str, Any]]:
        url_s = _clean_url(url)
        if not url_s:
            return None
        return results.setdefault(
            url_s,
            {
                "url": url_s,
                "explored": False,
                "selected": False,
                "save": {"status": "unknown"},
                "study": {"status": "unknown"},
                "failures": [],
            },
        )

    for entry in scan_urls:
        item = ensure(entry.get("url"))
        if item is not None:
            item["explored"] = True
    for entry in selected_urls:
        item = ensure(entry.get("url"))
        if item is not None:
            item["selected"] = True
            _merge_status(item, "selection", "success")
    for entry in save_urls:
        item = ensure(entry.get("url"))
        if item is not None:
            _merge_status(item, "save", "success", db_id=entry.get("db_id"))
    for entry in study_urls:
        item = ensure(entry.get("url"))
        if item is not None:
            _merge_status(item, "study", "success", db_id=entry.get("db_id"))

    memory_records = getattr(workflow, "_job_result_records", None)
    if isinstance(memory_records, dict):
        for record in list(memory_records.values()):
            if not isinstance(record, dict):
                continue
            item = ensure(record.get("url"))
            if item is None:
                continue
            events = record.get("events")
            if not isinstance(events, list) or not events:
                events = [record]
            for event in events:
                if not isinstance(event, dict):
                    continue
                stage = str(event.get("stage") or record.get("stage") or "").strip().lower()
                record_status = str(event.get("status") or record.get("status") or "").strip().lower()
                reason = str(event.get("reason") or record.get("reason") or "").strip()
                source_url = _clean_url(event.get("source_url") or record.get("source_url") or event.get("post_url") or record.get("post_url"))
                file_url = _clean_url(
                    event.get("file_url")
                    or event.get("attachment_url")
                    or record.get("file_url")
                    or record.get("attachment_url")
                )
                file_name = str(
                    event.get("file_name")
                    or event.get("attachment_name")
                    or record.get("file_name")
                    or record.get("attachment_name")
                    or ""
                ).strip()
                if file_url and source_url:
                    parent = ensure(source_url)
                    if parent is not None:
                        parent["explored"] = True
                        _merge_attachment(
                            parent,
                            file_url=file_url,
                            file_name=file_name,
                            stage=stage,
                            status=record_status or "unknown",
                            reason=reason,
                            file_path=event.get("file_path") or record.get("file_path"),
                            db_id=event.get("db_id") or record.get("db_id"),
                        )
                if stage in {"selection", "selected", "collection", "file_attachment"}:
                    item["selected"] = record_status != "failed"
                    _merge_status(item, "selection", record_status or "success", reason)
                elif stage in {"save", "study"}:
                    _merge_status(item, stage, record_status or "unknown", reason)
                elif record_status == "failed":
                    failure_record = {
                        "stage": stage or "detail",
                        "reason": reason or str(event.get("event") or record.get("event") or "failed"),
                        "event": event.get("event") or record.get("event"),
                        "ts": event.get("ts") or record.get("ts"),
                    }
                    if not _is_failure_event(failure_record):
                        continue
                    item.setdefault("failures", []).append(
                        failure_record
                    )

    for failure in _read_detail_failures(workflow, job_id=job_id, db_name=db_name):
        if not _is_failure_event(failure):
            continue
        item = ensure(failure.get("url"))
        if item is None:
            continue
        reason = str(failure.get("reason") or failure.get("event") or "detail_failed")
        item.setdefault("failures", []).append(
            {
                "stage": "detail",
                "reason": reason,
                "event": failure.get("event"),
                "ts": failure.get("ts"),
            }
        )

    for item in results.values():
        if not item.get("selected"):
            reason = _missing_required_detail_query_reason(item.get("url"))
            if reason:
                _merge_status(item, "selection", "skipped", reason)
                logger.warning(
                    "[FileUrlTrace][job_result_report.selection_skipped] job_id=%s db=%s reason=%s url=%s",
                    job_id,
                    db_name,
                    reason,
                    str(item.get("url") or "")[:300],
                )
        item["failures"] = _dedupe_failures(item.get("failures"))

    selected_urls = _merge_entries(
        selected_urls,
        ({"url": item.get("url")} for item in results.values() if item.get("selected") is True),
    )
    result_list = sorted(results.values(), key=lambda x: str(x.get("url") or ""))
    stats = dict(getattr(workflow, "stats", {}) or {})
    summary = {
        "exploration_total": len(scan_urls),
        "selected_total": len(selected_urls),
        "save_success_total": sum(1 for item in result_list if (item.get("save") or {}).get("status") == "success"),
        "save_failed_total": sum(1 for item in result_list if (item.get("save") or {}).get("status") == "failed"),
        "study_success_total": sum(1 for item in result_list if (item.get("study") or {}).get("status") == "success"),
        "study_failed_total": sum(1 for item in result_list if (item.get("study") or {}).get("status") == "failed"),
        "failure_total": sum(len(item.get("failures") or []) for item in result_list),
        "attachment_total": sum(len(item.get("attachments") or []) for item in result_list),
        "attachment_save_success_total": sum(
            1
            for item in result_list
            for attach in (item.get("attachments") or [])
            if isinstance(attach, dict) and (attach.get("save") or {}).get("status") == "success"
        ),
        "attachment_study_success_total": sum(
            1
            for item in result_list
            for attach in (item.get("attachments") or [])
            if isinstance(attach, dict) and (attach.get("study") or {}).get("status") == "success"
        ),
    }
    skipped_reasons: Dict[str, int] = {}
    for item in result_list:
        selection = item.get("selection") or {}
        if not isinstance(selection, dict):
            continue
        reason = str(selection.get("reason") or "").strip()
        status = str(selection.get("status") or "").strip().lower()
        if status == "skipped" and reason:
            skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1
    if skipped_reasons:
        summary["selection_skipped_reason_counts"] = skipped_reasons
    for key in (
        "scan_count",
        "collection_count",
        "save_count",
        "save_done_count",
        "save_success_count",
        "save_failed_count",
        "study_count",
        "study_done_count",
        "study_success_count",
        "study_failed_count",
        "study_skipped_count",
    ):
        if key in stats:
            summary[key] = stats.get(key)

    return {
        "schema": "crawler_job_result_report.v1",
        "job_id": job_id,
        "db_name": db_name,
        "status": status or getattr(workflow, "final_status", None),
        "generated_at": _now_iso(),
        "summary": summary,
        "exploration_urls": scan_urls,
        "selected_urls": selected_urls,
        "results": result_list,
        "source_files": {
            "trace": trace_path if os.path.exists(trace_path) else None,
        },
    }


async def save_job_result_report_async(
    *,
    workflow: Any,
    job_id: Optional[str],
    db_name: Optional[str],
    status: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> Optional[str]:
    base_dir = output_dir or _downloads_dir()
    os.makedirs(base_dir, exist_ok=True)
    path = os.path.join(base_dir, f"job_report_{_safe_part(db_name)}_{_safe_part(job_id)}.json")
    payload = await asyncio.to_thread(_build_report_sync, workflow, job_id=job_id, db_name=db_name, status=status)
    ok = await asyncio.to_thread(_atomic_write_json, path, payload)
    if ok:
        logger.info("[JobResultReport] saved | job_id=%s db=%s path=%s", job_id, db_name, path)
        return path
    logger.warning("[JobResultReport] save failed | job_id=%s db=%s path=%s", job_id, db_name, path)
    return None


def schedule_job_result_report(
    *,
    workflow: Any,
    job_id: Optional[str],
    db_name: Optional[str],
    status: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> Optional[asyncio.Task]:
    async def _runner() -> None:
        try:
            await save_job_result_report_async(
                workflow=workflow,
                job_id=job_id,
                db_name=db_name,
                status=status,
                output_dir=output_dir,
            )
        except Exception:
            logger.exception("[JobResultReport] async generation failed | job_id=%s db=%s", job_id, db_name)

    try:
        task = asyncio.create_task(_runner(), name=f"job-result-report:{job_id or 'unknown'}")
        return task
    except RuntimeError:
        try:
            asyncio.run(_runner())
        except Exception:
            logger.exception("[JobResultReport] sync fallback generation failed | job_id=%s db=%s", job_id, db_name)
    return None
