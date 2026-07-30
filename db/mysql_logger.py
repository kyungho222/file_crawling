"""MySQL crawl log updater for ASADAL_CRAWLING_LOG."""

import logging
import os
from typing import Optional

from sqlalchemy import create_engine, text

logger = logging.getLogger("file_crawler_backend")


class MySQLCrawlLogger:
    """Update crawl statistics written by the PHP side."""

    def __init__(self):
        try:
            db_url = os.getenv("MYSQL_CRAWL_LOGGER_URL", "") or ""
            if not db_url:
                raise RuntimeError("MYSQL_CRAWL_LOGGER_URL is not set")
            self.engine = create_engine(
                db_url,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )
            logger.info("MySQL crawl logger initialized")
        except Exception as e:
            logger.error("MySQL crawl logger initialization failed: %s", e)
            self.engine = None

    def update_crawl_stats(
        self,
        log_id: str,
        job_id: Optional[str] = None,
        scan: Optional[int] = None,
        collection: Optional[int] = None,
        save: Optional[int] = None,
        pages: Optional[int] = None,
        update_end_at: bool = False,
    ) -> bool:
        """Update nullable crawl statistic columns for one log row."""
        if not self.engine:
            logger.error("MySQL crawl logger has no active engine")
            return False

        if not log_id or log_id == "0":
            logger.warning("Invalid log_id: %s", log_id)
            return False

        params = {"log_id": log_id}
        try:
            update_fields = []

            if job_id is not None:
                update_fields.append("job_id = :job_id")
                params["job_id"] = job_id
            if scan is not None:
                update_fields.append("scan = :scan")
                params["scan"] = scan
            if collection is not None:
                update_fields.append("collection = :collection")
                params["collection"] = collection
            if save is not None:
                update_fields.append("save = :save")
                params["save"] = save
            if pages is not None:
                update_fields.append("pages = :pages")
                params["pages"] = pages
            if update_end_at:
                update_fields.append("end_at = NOW()")

            if not update_fields:
                logger.warning("No crawl log fields to update")
                return False

            sql = text(
                "UPDATE ASADAL_CRAWLING_LOG "
                f"SET {', '.join(update_fields)} "
                "WHERE id = :log_id"
            )

            logger.info("[Python] MySQL UPDATE start")
            logger.info("log_id=%s job_id=%s scan=%s collection=%s save=%s pages=%s update_end_at=%s", log_id, job_id, scan, collection, save, pages, update_end_at)
            logger.info("[SQL] %s", sql)
            logger.info("[Params] %s", params)

            with self.engine.connect() as conn:
                result = conn.execute(sql, params)
                conn.commit()

                affected_rows = result.rowcount
                logger.info("MySQL UPDATE complete affected_rows=%s", affected_rows)
                if affected_rows == 0:
                    logger.warning("No ASADAL_CRAWLING_LOG row found for log_id=%s", log_id)
                    return False

                return True

        except Exception as e:
            logger.error("MySQL UPDATE failed: %s", e)
            logger.error("log_id=%s params=%s", log_id, params)
            return False

    def update_on_crawl_complete(
        self,
        log_id: str,
        job_id: str,
        total_found: int,
        total_files: int,
        pages_crawled: int,
    ) -> bool:
        """Update crawl-side counters after crawl collection completes."""
        return self.update_crawl_stats(
            log_id=log_id,
            job_id=job_id,
            scan=total_found,
            collection=total_files,
            save=0,
            pages=pages_crawled,
            update_end_at=False,
        )

    def update_on_download_complete(self, log_id: str, success_count: int) -> bool:
        """Update download counter and terminal timestamp."""
        return self.update_crawl_stats(
            log_id=log_id,
            save=success_count,
            update_end_at=True,
        )


mysql_logger = MySQLCrawlLogger()
