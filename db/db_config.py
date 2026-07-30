"""
Compatibility adapter for legacy `db.db_config` API.

Many modules call `conn.fetchrow(...)` (asyncpg-style). The MariaDB asyncmy
connection doesn't provide `fetchrow`/`fetch`. This module wraps the raw
asyncmy connection returned by `db.maria_db_config.connect_db` and exposes
`fetchrow`, `fetch`, `execute` helpers and delegates other attributes.

This allows older code that expects asyncpg-like API to continue working
against the existing MariaDB pool.
"""
from __future__ import annotations

import re
import asyncio
from typing import Any, List, Optional

from . import maria_db_config

try:
    import asyncmy
    from asyncmy.cursors import DictCursor as AsyncmyDictCursor
except Exception:
    asyncmy = None  # type: ignore
    AsyncmyDictCursor = None  # type: ignore


_PLACEHOLDER_RE = re.compile(r"\$\d+")


def _convert_placeholders(query: str) -> str:
    """
    Convert Postgres-style $1, $2 placeholders to MySQL-style '%s'.
    Simple global replacement is sufficient for positional params.
    """
    return _PLACEHOLDER_RE.sub("%s", query)


async def _acquire_cursor(raw_conn: Any, cursor_cls: Any = None):
    """
    Acquire a DB cursor in a driver-compatible way.
    Tries several invocation styles to support aiomysql, asyncmy, mysql-connector, etc.
    Returns the cursor object (may be async or sync).
    """
    # try async-style acquisition first
    try:
        if cursor_cls is not None:
            try:
                return await raw_conn.cursor(cursor=cursor_cls)
            except TypeError:
                pass
            try:
                return await raw_conn.cursor(cursor_cls)
            except TypeError:
                pass
        try:
            return await raw_conn.cursor()
        except TypeError:
            pass
    except Exception:
        # fallthrough to sync attempts
        pass

    # try sync-style acquisition (e.g., mysql-connector)
    try:
        if cursor_cls is not None:
            try:
                return raw_conn.cursor(cursor=cursor_cls)
            except TypeError:
                pass
            try:
                return raw_conn.cursor(cursor_cls)
            except TypeError:
                pass
        try:
            return raw_conn.cursor(dictionary=True)
        except TypeError:
            return raw_conn.cursor()
    except Exception:
        # give up and re-raise a helpful error
        raise


async def _cursor_execute(cur: Any, query: str, params):
    res = cur.execute(query, params or ())
    if asyncio.iscoroutine(res):
        await res


async def _cursor_fetchone(cur: Any):
    res = cur.fetchone()
    if asyncio.iscoroutine(res):
        return await res
    return res


async def _cursor_fetchall(cur: Any):
    res = cur.fetchall()
    if asyncio.iscoroutine(res):
        return await res
    return res


async def _close_cursor(cur: Any):
    try:
        res = cur.close()
        if asyncio.iscoroutine(res):
            await res
    except Exception:
        # best-effort close
        try:
            cur.close()
        except Exception:
            pass



class ConnectionAdapter:
    def __init__(self, raw_conn: Any, dbname: Optional[str] = None):
        self._conn = raw_conn
        self._dbname = dbname

    async def fetchrow(self, query: str, *params) -> Optional[dict]:
        query = _convert_placeholders(query)
        cursor_cls = AsyncmyDictCursor
        cur = await _acquire_cursor(self._conn, cursor_cls)
        try:
            await _cursor_execute(cur, query, params)
            row = await _cursor_fetchone(cur)
            return row
        finally:
            await _close_cursor(cur)

    async def fetch(self, query: str, *params) -> List[dict]:
        query = _convert_placeholders(query)
        cursor_cls = AsyncmyDictCursor
        cur = await _acquire_cursor(self._conn, cursor_cls)
        try:
            await _cursor_execute(cur, query, params)
            rows = await _cursor_fetchall(cur)
            return rows
        finally:
            await _close_cursor(cur)

    async def execute(self, query: str, *params) -> int:
        query = _convert_placeholders(query)
        cur = await _acquire_cursor(self._conn, None)
        try:
            await _cursor_execute(cur, query, params)
            return cur.rowcount if hasattr(cur, "rowcount") else 0
        finally:
            await _close_cursor(cur)

    async def executemany(self, query: str, param_seq) -> int:
        query = _convert_placeholders(query)
        cur = await _acquire_cursor(self._conn, None)
        try:
            res = cur.executemany(query, param_seq)
            if asyncio.iscoroutine(res):
                await res
            return cur.rowcount if hasattr(cur, "rowcount") else 0
        finally:
            await _close_cursor(cur)

    async def close(self) -> None:
        raw = self._conn
        if raw is None:
            return
        self._conn = None
        await maria_db_config.return_connection(raw, self._dbname)

    async def aclose(self) -> None:
        await self.close()

    async def __aenter__(self) -> "ConnectionAdapter":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        # Delegate other attribute access to the underlying connection
        if self._conn is None:
            raise RuntimeError("ConnectionAdapter is already closed")
        return getattr(self._conn, name)


async def connect_db(dbname: Optional[str] = None) -> ConnectionAdapter:
    """
    Acquire a raw asyncmy connection from the pool and return an adapter
    that provides `fetchrow`/`fetch`/`execute`.
    """
    raw = await maria_db_config.connect_db(dbname)
    return ConnectionAdapter(raw, dbname)


async def return_connection(conn: Any, dbname: Optional[str] = None) -> None:
    """
    Return the underlying raw connection to the pool. Accepts either the
    ConnectionAdapter or a raw asyncmy connection.
    """
    if isinstance(conn, ConnectionAdapter):
        raw = conn._conn
    else:
        raw = conn
    await maria_db_config.return_connection(raw, dbname)


__all__ = ["connect_db", "return_connection", "ConnectionAdapter"]

