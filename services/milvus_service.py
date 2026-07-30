"""
Milvus 동기화(옵션) 서비스.

현 프로젝트(board02)에서는 학습(임베딩/PG 저장)은 필수지만,
Milvus 동기화는 환경/배포에 따라 선택적으로 켜야 한다.

board00의 db/db_milvus_operations.py 구조(컨텍스트 기반)를 이식하되,
의존성(pymilvus) 또는 설정이 없으면 NO-OP로 안전하게 동작한다.
"""

from __future__ import annotations

import json
import logging
import os
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

def _milvus_enabled() -> bool:
    """
    Milvus 연동은 기본 OFF.
    - MILVUS_ENABLED=true 일 때만 켜진다.
    """
    return (os.getenv("MILVUS_ENABLED") or "").strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class MilvusSyncContext:
    """
    Milvus 연동 활성화를 위한 실행 컨텍스트.

    - enabled: True일 때만 sync를 시도한다.
    - dbname/chat_bot_id: 로깅/컬렉션 네이밍 등에 사용.
    - account_name/learn_mode: 운영별 분기용(기본값 유지).
    """

    enabled: bool
    dbname: str
    chat_bot_id: str
    account_name: str = "chatty"
    learn_mode: str = "default"


_current_milvus_context: ContextVar[Optional[MilvusSyncContext]] = ContextVar(
    "milvus_sync_context", default=None
)


def activate_milvus_sync_context(context: Optional[MilvusSyncContext]) -> Optional[Token]:
    if not _milvus_enabled():
        # ✅ 기본 OFF 정책: 컨텍스트를 등록하지 않는다.
        return None
    if context is None:
        return None
    return _current_milvus_context.set(context)


def reset_milvus_sync_context(token: Optional[Token]) -> None:
    if token is None:
        return
    _current_milvus_context.reset(token)


def get_milvus_context() -> Optional[MilvusSyncContext]:
    if not _milvus_enabled():
        return None
    return _current_milvus_context.get()


def _safe_parse_embedding(emb: Any) -> Optional[List[float]]:
    if emb is None:
        return None
    if isinstance(emb, list):
        try:
            return [float(x) for x in emb]
        except Exception:
            return None
    if isinstance(emb, str):
        s = emb.strip()
        if not s:
            return None
        # "[0.1,0.2,...]" 형태를 best-effort로 파싱
        try:
            if s.startswith("[") and s.endswith("]"):
                return [float(x) for x in s[1:-1].split(",") if x.strip() != ""]
        except Exception:
            return None
    return None


def _safe_parse_metadata(meta: Any) -> Dict[str, Any]:
    if meta is None:
        return {}
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        try:
            return json.loads(meta)
        except Exception:
            return {}
    return {}


async def sync_rows_to_milvus(rows: List[Dict[str, Any]], context: Optional[MilvusSyncContext]) -> None:
    """
    PG 학습 row(딕셔너리)들을 Milvus에 동기화한다.

    현재 프로젝트에서는 Milvus 스키마/컬렉션 설계가 배포마다 다를 수 있어,
    - 설정/의존성이 없으면 NO-OP
    - 설정이 있으면 최소 필드(embedding + metadata)로 upsert/insert 시도
    """
    if not _milvus_enabled():
        return
    if not rows or not context or not context.enabled:
        return

    # 의존성/설정이 없으면 안전하게 종료
    try:
        from pymilvus import MilvusClient  # type: ignore
    except Exception:
        logger.info("[Milvus] pymilvus 미설치: Milvus 동기화 스킵")
        return

    # 환경변수 기반 설정(운영에서 주입)
    # - MILVUS_URI 예: "http://127.0.0.1:19530" 또는 "127.0.0.1:19530"
    # - MILVUS_TOKEN 예: "username:password" 또는 cloud token
    uri = os.getenv("MILVUS_URI") or os.getenv("MILVUS_HOST")
    token = os.getenv("MILVUS_TOKEN")
    collection = os.getenv("MILVUS_COLLECTION") or f"{context.chat_bot_id}_{context.learn_mode}"

    if not uri:
        logger.info("[Milvus] MILVUS_URI 미설정: Milvus 동기화 스킵")
        return

    # MilvusClient는 uri/token 조합을 지원(버전별 차이 가능 → best-effort)
    try:
        client = MilvusClient(uri=uri, token=token) if token else MilvusClient(uri=uri)
    except TypeError:
        # 구버전/다른 시그니처 대응
        try:
            client = MilvusClient(uri=uri)
        except Exception as exc:
            logger.warning("[Milvus] client init 실패: %s", exc)
            return
    except Exception as exc:
        logger.warning("[Milvus] client init 실패: %s", exc)
        return

    # collection 존재 여부 best-effort (없으면 생성 시도는 운영 스키마가 필요 → 여기서는 스킵)
    try:
        if hasattr(client, "has_collection") and not client.has_collection(collection):  # type: ignore[attr-defined]
            logger.warning("[Milvus] collection 없음(%s). 운영 스키마 미정으로 동기화 스킵", collection)
            return
    except Exception:
        # 체크 실패해도 insert 시도는 가능하므로 계속
        pass

    payloads: List[Dict[str, Any]] = []
    for r in rows:
        emb = _safe_parse_embedding(r.get("embedding"))
        if not emb:
            continue
        meta = _safe_parse_metadata(r.get("content_metadata"))
        # 최소 메타 필드: 검색/추적에 필요한 값들
        meta = {
            **meta,
            "content": r.get("content"),
            "chunk_num": r.get("chunk_num"),
            "content_type": r.get("content_type"),
            "subject": r.get("subject"),
            "web_title": r.get("web_title"),
        }
        payloads.append({"vector": emb, "metadata": meta})

    if not payloads:
        return

    try:
        # MilvusClient.insert는 스키마에 맞춘 field name이 필요할 수 있다.
        # 여기서는 "vector/metadata" 형태를 기본으로 시도하고, 실패하면 스킵한다.
        client.insert(collection_name=collection, data=payloads)  # type: ignore[arg-type]
        logger.info(
            "[Milvus] synced rows=%s collection=%s db=%s chat_bot_id=%s",
            len(payloads),
            collection,
            context.dbname,
            context.chat_bot_id,
        )
    except Exception as exc:
        logger.warning("[Milvus] insert 실패(스킵): %s", exc)


async def sync_deleted_contents(contents: List[str], context: Optional[MilvusSyncContext]) -> None:
    """
    삭제 동기화(옵션).
    - 운영 스키마가 불명확하므로, 기본 구현은 NO-OP(로그만).
    """
    if not _milvus_enabled():
        return
    if not contents or not context or not context.enabled:
        return
    logger.info(
        "[Milvus] delete sync requested (no-op) | count=%s db=%s chat_bot_id=%s",
        len(contents),
        context.dbname,
        context.chat_bot_id,
    )


__all__ = [
    "MilvusSyncContext",
    "activate_milvus_sync_context",
    "reset_milvus_sync_context",
    "get_milvus_context",
    "sync_rows_to_milvus",
    "sync_deleted_contents",
]


