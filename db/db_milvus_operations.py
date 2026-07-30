"""
Backward-compatible shim for Milvus operations.

Some modules import `db.db_milvus_operations`. the canonical implementation
was moved to `services.milvus_service`. This shim re-exports the public API
so older imports keep working and deployments don't crash with
ModuleNotFoundError.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    # Prefer the implementation in services
    from services.milvus_service import (
        MilvusSyncContext,
        activate_milvus_sync_context,
        reset_milvus_sync_context,
        get_milvus_context,
        sync_rows_to_milvus,
        sync_deleted_contents,
    )
except Exception as exc:  # pragma: no cover - best-effort shim
    logger.warning("db.db_milvus_operations shim: failed to import services.milvus_service: %s", exc)

    # Define safe no-op fallbacks so imports succeed even if service missing.
    class MilvusSyncContext:  # type: ignore
        def __init__(self, enabled: bool = False, dbname: str = "", chat_bot_id: str = "", account_name: str = "chatty", learn_mode: str = "default"):
            self.enabled = enabled
            self.dbname = dbname
            self.chat_bot_id = chat_bot_id
            self.account_name = account_name
            self.learn_mode = learn_mode

    def activate_milvus_sync_context(context: Optional[MilvusSyncContext]) -> Optional[object]:
        return None

    def reset_milvus_sync_context(token: Optional[object]) -> None:
        return None

    def get_milvus_context() -> Optional[MilvusSyncContext]:
        return None

    async def sync_rows_to_milvus(rows: List[Dict[str, Any]], context: Optional[MilvusSyncContext]) -> None:
        logger.info("Milvus disabled or shim active: sync_rows_to_milvus no-op. rows=%s", len(rows) if rows else 0)
        return

    async def sync_deleted_contents(contents: List[str], context: Optional[MilvusSyncContext]) -> None:
        logger.info("Milvus disabled or shim active: sync_deleted_contents no-op. contents=%s", len(contents) if contents else 0)
        return

__all__ = [
    "MilvusSyncContext",
    "activate_milvus_sync_context",
    "reset_milvus_sync_context",
    "get_milvus_context",
    "sync_rows_to_milvus",
    "sync_deleted_contents",
]

