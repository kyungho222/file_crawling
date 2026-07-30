import asyncio
import hashlib
import os
from typing import Set


class CollectionDeduplicator:
    """도메인 내에서 이미 처리한 URL을 추적하여 중복 수집을 방지한다."""

    def __init__(self) -> None:
        self._seen_urls: Set[str] = set()
        self._lock = asyncio.Lock()

    async def mark_url(self, url: str) -> bool:
        """url이 처음 등장하면 True, 이미 처리한 url이면 False."""
        if not url:
            return False
        async with self._lock:
            if url in self._seen_urls:
                return False
            self._seen_urls.add(url)
            return True

    async def reset(self) -> None:
        async with self._lock:
            self._seen_urls.clear()


def _cross_job_claim_enabled(stage: str = "") -> bool:
    stage_key = str(stage or "").strip().lower()
    if stage_key:
        try:
            stage_value = os.getenv(f"CRAWL_CROSS_JOB_{stage_key.upper()}_CLAIM_ENABLED")
            if stage_value is not None:
                return str(stage_value or "").strip().lower() in (
                    "1",
                    "true",
                    "yes",
                    "on",
                )
        except Exception:
            pass
    try:
        return str(os.getenv("CRAWL_CROSS_JOB_CLAIM_ENABLED", "1") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        return True


def _cross_job_claim_ttl(stage: str, *, recent: bool) -> int:
    stage_key = str(stage or "").strip().lower() or "generic"
    if recent:
        env_name = f"CRAWL_CROSS_JOB_{stage_key.upper()}_RECENT_TTL_SEC"
        default = 300 if stage_key == "scan" else 900
    else:
        env_name = f"CRAWL_CROSS_JOB_{stage_key.upper()}_CLAIM_TTL_SEC"
        default = 180 if stage_key == "scan" else 600
    try:
        value = int(os.getenv(env_name, str(default)) or str(default))
    except Exception:
        value = default
    return max(30, min(value, 3600))


def _cross_job_claim_key(stage: str, db_name: str, url: str) -> str:
    try:
        from utils.url import canonicalize_url_for_dedup

        normalized = canonicalize_url_for_dedup(str(url or "").strip()) or str(url or "").strip()
    except Exception:
        normalized = str(url or "").strip()
    db_token = str(db_name or "").strip().lower() or "default"
    digest = hashlib.sha1(normalized.encode("utf-8", errors="ignore")).hexdigest()
    return f"crawl:claim:v1:{stage}:{db_token}:{digest}"


async def try_acquire_cross_job_claim(stage: str, db_name: str, url: str, job_id: str) -> bool:
    if not _cross_job_claim_enabled(stage):
        return True
    if not db_name or not url:
        return True
    try:
        from db.db_redis import get_redis

        redis = await get_redis()
        key = _cross_job_claim_key(stage, db_name, url)
        owner = str(job_id or "unknown").strip() or "unknown"
        ttl = _cross_job_claim_ttl(stage, recent=False)
        return bool(await redis.set(key, owner, ex=ttl, nx=True))
    except Exception:
        return True


async def release_cross_job_claim(
    stage: str,
    db_name: str,
    url: str,
    job_id: str,
    *,
    keep_recent: bool,
) -> None:
    if not _cross_job_claim_enabled(stage):
        return
    if not db_name or not url:
        return
    try:
        from db.db_redis import get_redis

        redis = await get_redis()
        key = _cross_job_claim_key(stage, db_name, url)
        owner = str(job_id or "unknown").strip() or "unknown"
        current_owner = await redis.get(key)
        if str(current_owner or "").strip() != owner:
            return
        if keep_recent:
            await redis.expire(key, _cross_job_claim_ttl(stage, recent=True))
        else:
            await redis.delete(key)
    except Exception:
        return

