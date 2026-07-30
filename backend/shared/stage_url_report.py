import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _project_root() -> str:
    # backend/shared -> backend -> project_root
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _downloads_dir() -> str:
    path = os.path.join(_project_root(), "downloads")
    os.makedirs(path, exist_ok=True)
    return path


def _safe_part(s: Optional[str]) -> str:
    raw = str(s or "").strip()
    if not raw:
        return "unknown"
    # Windows/Posix 파일명에서 위험한 문자 제거/치환
    for ch in ['\\', '/', ':', '*', '?', '"', '<', '>', '|', '\n', '\r', '\t']:
        raw = raw.replace(ch, "_")
    raw = raw.strip("._ ")
    return raw or "unknown"


def _now_iso() -> str:
    try:
        return datetime.now().isoformat()
    except Exception:
        return str(int(time.time()))


def _env_mode() -> str:
    """
    JSON 출력 모드:
      - stage: stage_{stage}_{db}_{job}.json 만 생성/누적
      - single: trace_{db}_{job}.json 하나만 생성/업데이트
      - both: 둘 다 생성 (기본)
    """
    try:
        m = str(os.getenv("CRAWL_TRACE_JSON_MODE", "both") or "both").strip().lower()
    except Exception:
        m = "both"
    return m if m in ("stage", "single", "both") else "both"


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> bool:
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        try:
            os.replace(tmp, path)
        except Exception:
            os.rename(tmp, path)
        return True
    except Exception:
        return False


def _ensure_base_dir(output_dir: Optional[str]) -> Optional[str]:
    base_dir = output_dir
    if base_dir:
        try:
            base_dir = os.path.abspath(str(base_dir))
            os.makedirs(base_dir, exist_ok=True)
            return base_dir
        except Exception:
            return None
    return None


def _merge_dedup_by_url(existing: List[Any], incoming: List[Dict[str, Any]], entry_extra: Optional[Dict[str, Any]]) -> Tuple[List[Any], int]:
    out: List[Any] = list(existing or [])
    seen: set[str] = set()
    for e in out:
        try:
            if isinstance(e, dict):
                k = str(e.get("url") or "").strip()
            else:
                k = str(e).strip()
            if k:
                seen.add(k)
        except Exception:
            continue

    added = 0
    for e in incoming:
        try:
            url = str(e.get("url") or "").strip()
        except Exception:
            url = ""
        if not url or url in seen:
            continue
        if entry_extra:
            try:
                merged = dict(entry_extra)
                merged.update(e)
                e = merged
            except Exception:
                pass
        out.append(e)
        seen.add(url)
        added += 1
    return out, added


def append_stage_urls(
    *,
    stage: str,
    urls: Iterable[Any],
    job_id: Optional[str] = None,
    db_name: Optional[str] = None,
    output_dir: Optional[str] = None,
    extra_meta: Optional[Dict[str, Any]] = None,
    entry_extra: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    scan/collection/save/study 등 단계별 '결과 URL'을 downloads/ 아래 JSON으로 누적 저장한다.

    파일명(고정, 누적):
      - stage_{stage}_{db_name}_{job_id}.json
    포맷:
      {
        "stage": "...",
        "db_name": "...",
        "job_id": "...",
        "updated_at": "...",
        "total": N,
        "urls": [{"url": "...", ...}]
      }
    """
    stage_key = _safe_part(stage)
    job_key = _safe_part(job_id)
    db_key = _safe_part(db_name)

    # urls가 비어 있으면 파일 생성/갱신을 하지 않는다.
    prepared: List[Dict[str, Any]] = []
    for u in (urls or []):
        try:
            if isinstance(u, dict) and u.get("url"):
                prepared.append(dict(u))
            else:
                s = str(u).strip()
                if s:
                    prepared.append({"url": s})
        except Exception:
            continue
    if not prepared:
        return None

    mode = _env_mode()
    base_dir = _ensure_base_dir(output_dir)
    written_stage: Optional[str] = None
    written_trace: Optional[str] = None

    # -----------------------
    # 1) stage 파일(기존 포맷)
    # -----------------------
    if mode in ("stage", "both"):
        stage_path = os.path.join(base_dir or _downloads_dir(), f"stage_{stage_key}_{db_key}_{job_key}.json")
        stage_data: Dict[str, Any] = {
            "stage": stage_key,
            "db_name": db_name,
            "job_id": job_id,
            "updated_at": _now_iso(),
            "total": 0,
            "urls": [],
        }
        if extra_meta:
            try:
                stage_data.update(dict(extra_meta))
            except Exception:
                pass
        try:
            if os.path.exists(stage_path):
                with open(stage_path, "r", encoding="utf-8") as f:
                    prev = json.load(f)
                if isinstance(prev, dict) and isinstance(prev.get("urls"), list):
                    stage_data["urls"] = prev.get("urls") or []
        except Exception:
            stage_data["urls"] = []

        merged, _ = _merge_dedup_by_url(stage_data["urls"], prepared, entry_extra)
        stage_data["urls"] = merged
        stage_data["total"] = len(merged)
        stage_data["updated_at"] = _now_iso()
        if _atomic_write_json(stage_path, stage_data):
            written_stage = stage_path

    # -----------------------
    # 2) trace 단일 파일(단계별 누적)
    # -----------------------
    if mode in ("single", "both"):
        trace_path = os.path.join(base_dir or _downloads_dir(), f"trace_{db_key}_{job_key}.json")
        trace_data: Dict[str, Any] = {
            "db_name": db_name,
            "job_id": job_id,
            "updated_at": _now_iso(),
            "stages": {},
        }
        if extra_meta:
            try:
                trace_data.update(dict(extra_meta))
            except Exception:
                pass
        try:
            if os.path.exists(trace_path):
                with open(trace_path, "r", encoding="utf-8") as f:
                    prev = json.load(f)
                if isinstance(prev, dict) and isinstance(prev.get("stages"), dict):
                    trace_data["stages"] = prev.get("stages") or {}
        except Exception:
            trace_data["stages"] = {}

        stages_obj = trace_data["stages"]
        if not isinstance(stages_obj, dict):
            stages_obj = {}
            trace_data["stages"] = stages_obj

        stage_obj = stages_obj.get(stage_key)
        if not isinstance(stage_obj, dict):
            stage_obj = {"updated_at": _now_iso(), "total": 0, "urls": []}
            stages_obj[stage_key] = stage_obj

        existing = stage_obj.get("urls")
        if not isinstance(existing, list):
            existing = []
        merged, _ = _merge_dedup_by_url(existing, prepared, entry_extra)
        stage_obj["urls"] = merged
        stage_obj["total"] = len(merged)
        stage_obj["updated_at"] = _now_iso()
        trace_data["updated_at"] = _now_iso()

        if _atomic_write_json(trace_path, trace_data):
            written_trace = trace_path

    return written_stage or written_trace

