from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Tuple

from db.mariadb_save_update import (
    _ensure_file_learning_category_mapping,
    bulk_update_file_categories_by_source_pages as _db_bulk_update_by_source_pages,
    preview_file_category_sync_plan as _db_preview_sync_plan,
    sync_file_categories_from_homepage_learning as _db_sync_from_homepage_learning,
    update_file_categories_by_source_page as _db_update_by_source_page,
    update_file_categories_by_subject_names as _db_update_by_subject_names,
)


async def map_board_cate2_to_file_learning(
    *,
    chat_bot_id: str,
    db_name: str,
    board_cate2: str,
    board_cate1: str = "",
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """Map a board cate2 to an existing "파일학습" child category.

    This module is intentionally read/update-only for CATEGORY rows. Missing
    file-learning child categories are not created here.
    """

    return await _ensure_file_learning_category_mapping(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        source_cate1=board_cate1,
        source_cate2=board_cate2,
        access_url=access_url,
        request_cookies=request_cookies,
        create_missing=False,
    )


async def apply_file_category_by_subject_names(
    *,
    chat_bot_id: str,
    db_name: str,
    subject_names: Iterable[str],
    board_cate2: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    blank_only: bool = True,
) -> Dict[str, Any]:
    return await _db_update_by_subject_names(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        subject_names=subject_names,
        board_cate2=board_cate2,
        access_url=access_url,
        request_cookies=request_cookies,
        blank_only=blank_only,
    )


async def apply_file_category_by_source_page(
    *,
    chat_bot_id: str,
    db_name: str,
    source_page: str,
    board_cate2: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    blank_only: bool = True,
) -> Dict[str, Any]:
    return await _db_update_by_source_page(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        source_page=source_page,
        board_cate2=board_cate2,
        access_url=access_url,
        request_cookies=request_cookies,
        blank_only=blank_only,
    )


async def apply_file_categories_by_source_pages(
    *,
    chat_bot_id: str,
    db_name: str,
    assignments: Iterable[Dict[str, str]],
    blank_only: bool = True,
) -> Dict[str, Any]:
    return await _db_bulk_update_by_source_pages(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        assignments=assignments,
        blank_only=blank_only,
    )


async def preview_file_category_apply_plan(*, chat_bot_id: str, db_name: str) -> Dict[str, Any]:
    return await _db_preview_sync_plan(chat_bot_id=chat_bot_id, db_name=db_name)


async def sync_existing_file_categories_from_homepage_learning(
    *,
    chat_bot_id: str,
    db_name: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    return await _db_sync_from_homepage_learning(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        access_url=access_url,
        request_cookies=request_cookies,
    )
