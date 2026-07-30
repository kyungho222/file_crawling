"""Smart router for MySQL and MariaDB operations.

Default routing:
- chatty, naraone ??MySQL (mysql_pool.py)
- All others ??MariaDB (mariadb_pool.py)

Override:
- Config.RDBMS_FORCE_MARIADB_DATABASES=foo
  -> listed logical DBs route to MariaDB regardless of default
"""
import logging
from typing import Optional

from backend.shared.config import Config

from db.mysql_pool import (
    MySQLPool,
    mysql_connect,
    mysql_release,
    mysql_execute,
    mysql_executemany as mysql_pool_executemany,
    mysql_cleanup_on_shutdown as _mysql_cleanup,
)
from db.mariadb_pool import (
    mariadb_connect,
    mariadb_release,
    mariadb_execute,
    mariadb_executemany as mariadb_pool_executemany,
    get_global_connection_diagnostics as mariadb_global_connection_diagnostics,
    mariadb_cleanup_on_shutdown as _mariadb_cleanup,
    MariaDBPool,
)

DEFAULT_MYSQL_DATABASES = {"chatty", "naraone"}
logger = logging.getLogger("db.rdbms_router")


def _normalize_dbname(dbname: Optional[str]) -> str:
    return (str(dbname).strip().lower()) if dbname else ""


def _resolve_rdbms_engine(dbname: Optional[str]) -> str:
    name = _normalize_dbname(dbname)
    forced_mariadb = {
        s.strip().lower()
        for s in str(getattr(Config, "RDBMS_FORCE_MARIADB_DATABASES", "") or "").split(",")
        if s and s.strip()
    }
    if not name:
        return "mariadb"
    if name in forced_mariadb:
        return "mariadb"
    if name in DEFAULT_MYSQL_DATABASES:
        return "mysql"
    return "mariadb"


def resolve_rdbms_engine(dbname: Optional[str]) -> str:
    return _resolve_rdbms_engine(dbname)


def _pool_has_active_work(pool) -> bool:
    try:
        used = int(len(getattr(pool, "_used", []) or []))
    except Exception:
        used = 0
    try:
        acquiring = int(getattr(pool, "_acquiring", 0) or 0)
    except Exception:
        acquiring = 0
    return used > 0 or acquiring > 0


async def _detach_pool_for_recreate(pool_map, dbname) -> None:
    pool_tuple = pool_map.pop(dbname, None)
    if not pool_tuple:
        return
    pool, _ = pool_tuple
    try:
        pool.close()
    except Exception:
        return

    if _pool_has_active_work(pool):
        return

    try:
        await pool.wait_closed()
    except Exception:
        pass


async def rdbms_connect(dbname=None):
    if _resolve_rdbms_engine(dbname) == "mysql":
        return await mysql_connect(dbname)
    return await mariadb_connect(dbname)


async def rdbms_release(conn, dbname=None, discard: bool = False):
    if conn is None:
        return
    resolved_dbname = dbname or getattr(conn, "_pool_dbname", None)
    if _resolve_rdbms_engine(resolved_dbname) == "mysql":
        await mysql_release(conn, resolved_dbname, discard=discard)
    else:
        await mariadb_release(conn, resolved_dbname, discard=discard)


async def rdbms_execute_query(query, params=None, fetch=False, dbname=None, op_name=None):
    if _resolve_rdbms_engine(dbname) == "mysql":
        return await mysql_execute(query, params, fetch, dbname, op_name=op_name)
    else:
        return await mariadb_execute(query, params, fetch, dbname, op_name=op_name)


async def rdbms_executemany(query, params_list, dbname=None):
    if _resolve_rdbms_engine(dbname) == "mysql":
        return await mysql_pool_executemany(query, params_list, dbname)
    else:
        return await mariadb_pool_executemany(query, params_list, dbname)


class MYSQL_DatabasePool:
    @staticmethod
    async def get_pool(dbname=None, force_recreate: bool = False):
        if force_recreate:
            await _detach_pool_for_recreate(MySQLPool._pools, dbname)
        return await MySQLPool.get_pool(dbname)

    @staticmethod
    async def close_all_pools():
        await MySQLPool.close_all_pools()

    @staticmethod
    def get_pool_status():
        return MySQLPool.get_pool_status()

    @staticmethod
    async def mysql_release_connection(conn, dbname=None):
        await mysql_release(conn, dbname)


class MARIADB_DatabasePool:
    @staticmethod
    async def get_pool(dbname=None, force_recreate: bool = False):
        if force_recreate:
            await _detach_pool_for_recreate(MariaDBPool._pools, dbname)
        return await MariaDBPool.get_pool(dbname)

    @staticmethod
    async def close_all_pools():
        await MariaDBPool.close_all_pools()

    @staticmethod
    def get_pool_status():
        return MariaDBPool.get_pool_status()

    @staticmethod
    async def get_global_connection_diagnostics(dbname: Optional[str] = None):
        return await mariadb_global_connection_diagnostics(dbname)

    @staticmethod
    async def maria_release_connection(conn, dbname=None):
        await mariadb_release(conn, dbname)


async def rdbms_cleanup_on_shutdown():
    """Cleanup both MySQL and MariaDB pools on application shutdown."""
    await _mysql_cleanup()
    await _mariadb_cleanup()

