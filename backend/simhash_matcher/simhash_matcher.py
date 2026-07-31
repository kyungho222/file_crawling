"""MariaDB-backed exact SimHash duplicate checker."""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Protocol

from simhash import Simhash

logger = logging.getLogger(__name__)


class Cursor(Protocol):
    def execute(self, query: str, params: tuple[Any, ...]) -> Any: ...
    def fetchone(self) -> Any: ...
    def close(self) -> Any: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...


def has_hash(
    connection: Connection,
    subject: str | None,
    content: str | None,
    *,
    table: str,
) -> bool:
    """Create SimHash then return whether the same value already exists."""
    simhash = make_simhash(subject, content)
    if simhash is None:
        return False
    return has_simhash_match(connection, simhash, table=table)


def check_hash(
    connection: Connection,
    subject: str | None,
    content: str | None,
    *,
    table: str,
) -> dict[str, bool | str | None]:
    """Return duplicate/save state and a generated hash for caller-side storage."""
    simhash = make_simhash(subject, content)
    if simhash is None:
        return {"duplicate": False, "save": False, "hash": None}

    hash_value = format_simhash(simhash)
    duplicate = has_simhash_match(connection, simhash, table=table)
    return {"duplicate": duplicate, "save": not duplicate, "hash": hash_value}

def make_simhash(subject: str | None, content: str | None) -> int | None:
    """Create a 64-bit SimHash, or skip with a warning for missing payload."""
    missing = [name for name, value in (("subject", subject), ("content", content)) if not value]
    if missing:
        for field in missing:
            logger.warning("simhash 생성에 필요한 payload 중 %s 누락", field)
        return None
    text = _normalize(f"{subject}\n---CONTENT---\n{content}")
    return Simhash(text, f=64).value


def has_simhash_match(connection: Connection, simhash: int, *, table: str) -> bool:
    """Return whether ``table.hash`` has an exact match for a SimHash value."""
    _validate_identifier(table)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"SELECT 1 FROM {table} WHERE hash = %s LIMIT 1",
            (format_simhash(simhash),),
        )
        return cursor.fetchone() is not None
    except Exception as error:
        if "unknown column" in str(error).lower() and "hash" in str(error).lower():
            logger.warning("simhash 비교에 필요한 hash 컬럼 누락")
            return False
        raise
    finally:
        cursor.close()


def format_simhash(simhash: int) -> str:
    """Return a zero-padded 16-character lowercase hexadecimal SimHash."""
    return f"{simhash & ((1 << 64) - 1):016x}"


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", value)).strip()


def _validate_identifier(value: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("table must be a safe SQL identifier")
