#!/usr/bin/env python3
"""
Maintenance script: update `files.filesize` and `files.size_updated_at`
Scans rows with non-null `local_path`, determines size (local file or S3), and updates DB in batches.
Usage: python backend/scripts/update_filesizes.py

Dependencies: pymysql, boto3 (optional, for s3:// paths)
"""
import os
import time
import logging
from datetime import datetime
from urllib.parse import urlparse
from sqlalchemy import text
import sys

# Ensure project root is on sys.path so `db` package can be imported when script run directly
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from db.database import get_engine

LOG = logging.getLogger("update_filesizes")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
LOG.addHandler(handler)

# --- CONFIG ---
# Use project's SQLAlchemy engine via db.database.get_engine
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
SLEEP_BETWEEN_BATCH = float(os.getenv("SLEEP_BETWEEN_BATCH", "0.01"))


def get_s3_size(path: str) -> int | None:
    """Return ContentLength for s3://bucket/key or None on failure."""
    try:
        import boto3
    except Exception:
        LOG.debug("boto3 not installed; skipping s3 support")
        return None

    try:
        p = urlparse(path)
        if p.scheme != "s3":
            return None
        s3 = boto3.client("s3")
        resp = s3.head_object(Bucket=p.netloc, Key=p.path.lstrip("/"))
        return int(resp.get("ContentLength"))
    except Exception as e:
        LOG.warning("S3 head_object failed for %s: %s", path, e)
        return None


def get_size_for_path(path: str) -> int | None:
    """Try S3 first, then local filesystem. Return None if not accessible."""
    if not path:
        return None
    # s3 support
    if path.startswith("s3://"):
        size = get_s3_size(path)
        if size is not None:
            return size
    # local file
    try:
        return os.path.getsize(path)
    except OSError as e:
        LOG.debug("os.path.getsize failed for %s: %s", path, e)
        return None


def update_filesizes(batch_size: int = BATCH_SIZE):
    engine = get_engine()
    conn = engine.connect()
    try:
        result = conn.execution_options(stream_results=True).execute(
            text("SELECT id, local_path FROM files WHERE local_path IS NOT NULL")
        )
        total_updated = 0
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            updates = []
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for row in rows:
                # row may be tuple or mapping depending on DB driver
                try:
                    file_id = row["id"]
                    local_path = row["local_path"]
                except Exception:
                    file_id, local_path = row[0], row[1]
                size = get_size_for_path(local_path)
                updates.append({"size": size, "ts": now, "id": file_id})

            # execute batch update using SQLAlchemy
            conn.execute(
                text("UPDATE files SET filesize = :size, size_updated_at = :ts WHERE id = :id"),
                updates,
            )
            conn.commit()
            total_updated += len(updates)
            LOG.info("Updated batch: %d rows (total %d)", len(updates), total_updated)
            time.sleep(SLEEP_BETWEEN_BATCH)
    except Exception as e:
        LOG.exception("Failed updating filesizes: %s", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    LOG.info("Starting update_filesizes (batch=%d)", BATCH_SIZE)
    update_filesizes()
    LOG.info("Finished")


