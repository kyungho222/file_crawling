"""Read-only connectivity check for the configured company MariaDB and Redis."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.shared.config import Config
from db.db_redis import describe_redis_connection, redis_manager
from db.mariadb_pool import MariaDBPool, mariadb_execute


async def _tcp_reachable(host: str, port: int, timeout_sec: float = 2.0) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout_sec,
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False

async def main() -> int:
    failed = False
    db_name = str(getattr(Config, "web_db", "") or "").strip()
    if not db_name:
        print("[FAIL] MariaDB logical database name is empty", flush=True)
        failed = True
    else:
        host = str(getattr(Config, "MARIA_DB_HOST", "") or "")
        port = int(getattr(Config, "MARIA_DB_PORT", 0) or 0)
        if not await _tcp_reachable(host, port):
            print(f"[FAIL] MariaDB tunnel port is not open | host={host} port={port}", flush=True)
            failed = True
        else:
            try:
                rows = await mariadb_execute(
                    "SELECT 1 AS ok",
                    fetch=True,
                    dbname=db_name,
                    op_name="harness_company_connection_check",
                )
                if not rows or int(rows[0].get("ok") or 0) != 1:
                    raise RuntimeError(f"unexpected SELECT 1 result: {rows!r}")
                print(f"[OK] MariaDB connected | db={db_name}", flush=True)
            except Exception as exc:
                print(f"[FAIL] MariaDB connection | db={db_name} error={exc}", flush=True)
                failed = True
            finally:
                await MariaDBPool.close_all_pools()

    redis_url = str(os.getenv("REDIS_URL", "") or "")
    parsed = urlparse(redis_url)
    redis_host = str(parsed.hostname or "")
    redis_port = int(parsed.port or 6379)
    if not redis_host or not await _tcp_reachable(redis_host, redis_port):
        print(f"[FAIL] Redis tunnel port is not open | host={redis_host or '-'} port={redis_port}", flush=True)
        failed = True
    else:
        try:
            await redis_manager.connect()
            client = await redis_manager.get_client()
            if not await client.ping():
                raise RuntimeError("PING returned false")
            print(f"[OK] Redis connected | {describe_redis_connection(client)}", flush=True)
        except Exception as exc:
            print(f"[FAIL] Redis connection | error={exc}", flush=True)
            failed = True
        finally:
            await redis_manager.disconnect()

    return 1 if failed else 0

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
