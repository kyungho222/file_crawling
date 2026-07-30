"""Compatibility wrapper for the root MariaDB pool implementation."""

from typing import Any, Optional

from db.mariadb_pool import mariadb_connect, mariadb_release, mariadb_cleanup_on_shutdown
from db.rdbms_router import MARIADB_DatabasePool as DatabasePool

MARIADB_DatabasePool = DatabasePool


async def connect_db(dbname: Optional[str] = None):
    """Acquire a raw MariaDB connection from the shared asyncmy pool."""
    return await mariadb_connect(dbname)


async def return_connection(conn: Any, dbname: Optional[str] = None) -> None:
    """Return the raw MariaDB connection to the shared pool."""
    await mariadb_release(conn, dbname)


def get_pool_status():
    return DatabasePool.get_pool_status()


async def cleanup_on_shutdown():
    await mariadb_cleanup_on_shutdown()


async def maria_connect_db(dbname: Optional[str] = None):
    return await connect_db(dbname)


async def maria_return_connection(conn: Any, dbname: Optional[str] = None) -> None:
    await return_connection(conn, dbname)


async def maria_cleanup_on_shutdown():
    await cleanup_on_shutdown()


__all__ = [
    "DatabasePool",
    "MARIADB_DatabasePool",
    "connect_db",
    "return_connection",
    "cleanup_on_shutdown",
    "maria_connect_db",
    "maria_return_connection",
    "maria_cleanup_on_shutdown",
    "get_pool_status",
]
