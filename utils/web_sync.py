import asyncio
import logging
import os
import shlex
import time
import threading
from pathlib import Path
from typing import Optional, Tuple, Any
from urllib.parse import urlparse
import hashlib

from utils.hash_policy import hash_generation_disabled

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

# from backend.shared.config import (
from config.settings import (
    FILEUPLOAD_URL_PREFIX,
    get_fileupload_root,
    get_webserver_uploaded_files_dir,
)

logger = logging.getLogger(__name__)
FLOW_DEBUG = os.getenv("CRAWL_DEBUG_FLOW", "0") == "1"
if FLOW_DEBUG:
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass


def _debug_log(*args: Any, **kwargs: Any) -> None:
    return None


import importlib.util
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Shim: the original implementation has been moved to `garbage/utils/web_sync.py`.
# Try to dynamically load the relocated implementation to preserve behavior.
_RELOCATED_MODULE = None
try:
    base_dir = Path(__file__).resolve().parent
    relocated_path = base_dir / "garbage" / "utils" / "web_sync.py"
    if relocated_path.exists():
        spec = importlib.util.spec_from_file_location("relocated_web_sync", str(relocated_path))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore
        _RELOCATED_MODULE = mod
except Exception as _exc:  # pragma: no cover
    logging.getLogger(__name__).warning("[WebSync shim] failed to load relocated module: %s", _exc)


def _ensure_env_loaded() -> None:
    # [Fix] .env 蹂寃??ы빆(IP ?????뺤떎??諛섏쁺?섍린 ?꾪빐, ?대? 濡쒕뱶???곹깭?쇰룄 臾댁“嫄??ㅼ떆 濡쒕뱶?섎룄濡?媛?쒕? ?됰땲??
    # if os.getenv("WEB_SYNC_SFTP_PASSWORD") or os.getenv("WEB_SYNC_SFTP_KEY_PATH"):
    #     return

    candidates: list[Path] = []
    env_file = os.getenv("ENV_FILE_PATH")
    if env_file:
        candidates.append(Path(env_file))

    base_dir = Path(__file__).resolve().parent.parent
    candidates.extend(
        [
            base_dir / ".env",
            base_dir / "backend" / ".env",
            Path.cwd() / ".env",
        ]
    )

    seen: set[str] = set()
    unique_candidates = []
    for c in candidates:
        p = str(c)
        if p in seen:
            continue
        seen.add(p)
        unique_candidates.append(c)

    # #region agent log
    _debug_log(
        "H_env_load_web_sync",
        "utils/web_sync.py:_ensure_env_loaded",
        "env_load_attempt",
        {
            "dotenv_available": bool(load_dotenv),
            "env_file_path_set": bool(env_file),
            "candidates": [str(p) for p in unique_candidates],
        },
    )
    # #endregion

    if not load_dotenv:
        # #region agent log
        _debug_log(
            "H_env_load_web_sync",
            "utils/web_sync.py:_ensure_env_loaded",
            "env_load_no_dotenv",
            {},
        )
        # #endregion
        return

    loaded_any = False
    loaded_paths: list[Path] = []
    for path in unique_candidates:
        try:
            if path.exists():
                load_dotenv(dotenv_path=str(path), override=True)
                loaded_any = True
                loaded_paths.append(path)
                # #region agent log
                _debug_log(
                    "H_env_load_web_sync",
                    "utils/web_sync.py:_ensure_env_loaded",
                    "env_load_found",
                    {"path": str(path)},
                )
                # #endregion
        except Exception:
            continue

    if not loaded_any:
        # #region agent log
        _debug_log(
            "H_env_load_web_sync",
            "utils/web_sync.py:_ensure_env_loaded",
            "env_load_missing",
            {"candidates": [str(p) for p in unique_candidates]},
        )
        # #endregion

    def _env_file_key_presence(path: Path, keys: list[str]) -> dict:
        presence = {}
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return presence
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key in keys:
                presence[f"{key}_present"] = True
                presence[f"{key}_has_value"] = bool(val)
        return presence

    auth_presence = {
        "password_set": bool(os.getenv("WEB_SYNC_SFTP_PASSWORD")),
        "key_path_set": bool(os.getenv("WEB_SYNC_SFTP_KEY_PATH")),
        "user_set": bool(os.getenv("WEB_SYNC_SFTP_USER") or os.getenv("WEB_SYNC_SSH_USER")),
    }
    # #region agent log
    _debug_log(
        "H_env_load_web_sync",
        "utils/web_sync.py:_ensure_env_loaded",
        "env_auth_presence",
        auth_presence,
    )
    # #endregion

    if loaded_paths:
        keys = ["WEB_SYNC_SFTP_PASSWORD", "WEB_SYNC_SFTP_KEY_PATH", "WEB_SYNC_SFTP_USER", "WEB_SYNC_SSH_USER"]
        for lp in loaded_paths[:2]:
            # #region agent log
            _debug_log(
                "H_env_load_web_sync",
                "utils/web_sync.py:_ensure_env_loaded",
                "env_file_key_presence",
                {"path": str(lp), **_env_file_key_presence(lp, keys)},
            )
            # #endregion


def _env_bool(name: str, default: str = "0") -> bool:
    try:
        return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"


def _is_ssh_crypto_error(err: str) -> bool:
    try:
        lowered = (err or "").lower()
    except Exception:
        lowered = ""
    return (
        "error in libcrypto" in lowered
        or "no matching host key type found" in lowered
        or "ssh_dispatch_run_fatal" in lowered
    )


def _file_signature(path: str) -> dict:
    """
    ?뚯씪??湲곕낯 ?쒓렇?덉쿂 ?뺣낫瑜?諛섑솚?⑸땲??
    - size, mtime, is_file, head_sha256(理쒖큹 1MB ?댁떆, ?ㅽ뙣 ??None)
    """
    try:
        st = os.stat(path)
        size = int(st.st_size or 0)
        mtime = int(st.st_mtime or 0)
        is_file = os.path.isfile(path)
    except Exception:
        return {}
    head_sha256 = None
    if not hash_generation_disabled():
        try:
            h = hashlib.sha256()
            with open(path, "rb") as _f:
                chunk = _f.read(1024 * 1024)
                if chunk:
                    h.update(chunk)
                    head_sha256 = h.hexdigest()
        except Exception:
            head_sha256 = None
    return {"size": size, "mtime": mtime, "is_file": is_file, "head_sha256": head_sha256}


def _ssh_target() -> Optional[str]:
    try:
        v = str(os.getenv("WEB_SYNC_SSH_TARGET", "") or "").strip()
    except Exception:
        v = ""
    if v:
        return v

    auto = _env_bool("WEB_SYNC_AUTO_TARGET", "1")
    if not auto:
        return None
    try:
        user = str(os.getenv("WEB_SYNC_DEFAULT_USER", "chatty_master") or "").strip()
    except Exception:
        user = "chatty_master"
    try:
        host = str(os.getenv("WEB_SYNC_DEFAULT_HOST", "110.45.147.56") or "").strip()
    except Exception:
        host = "110.45.147.56"
    if not host:
        return None
    
    res = f"{user}@{host}" if user else host
    return res


def _parse_target(target: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        if "@" in target:
            user, host = target.split("@", 1)
            return (user.strip() or None, host.strip() or None)
        return (None, target.strip() or None)
    except Exception:
        return (None, None)


def _ssh_port() -> int:
    try:
        v = str(os.getenv("WEB_SYNC_SSH_PORT", "") or "").strip()
        if v:
            return max(1, min(int(v), 65535))
    except Exception:
        pass
    # ?먮룞 湲곕낯媛?(?붿껌?ы빆)
    try:
        v2 = str(os.getenv("WEB_SYNC_DEFAULT_PORT", "41") or "").strip()
    except Exception:
        v2 = "41"
    try:
        return max(1, min(int(v2), 65535))
    except Exception:
        return 22


def _ssh_opts() -> list[str]:
    """
    SSH ?듭뀡 湲곕낯媛?
    - BatchMode=yes: ?⑥뒪?뚮뱶/?명꽣?숈뀡 諛⑹?
    - ConnectTimeout: ?ㅽ듃?뚰겕 吏????鍮좊Ⅸ ?ㅽ뙣
    HostKeyChecking? ?댁쁺 蹂댁븞 ?뺤콉???곕씪 ?ㅻⅤ誘濡?ENV濡??쒖뼱?쒕떎.
    """
    opts = ["-o", "BatchMode=yes"]
    try:
        ct = int(os.getenv("WEB_SYNC_SSH_CONNECT_TIMEOUT_SEC", "10") or "10")
    except Exception:
        ct = 10
    ct = max(3, min(ct, 60))
    opts += ["-o", f"ConnectTimeout={ct}"]

    strict = _env_bool("WEB_SYNC_SSH_STRICT_HOSTKEY", "1")
    if not strict:
        # ?댁쁺?먯꽌 理쒖큹 ?곌껐/known_hosts 愿由ш? ?대졄?ㅻ㈃ 鍮꾪솢?깊솕 媛??沅뚯옣 X)
        opts += ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
    else:
        # OpenSSH媛 吏?먰븯硫?accept-new濡?理쒖큹留??먮룞 ?깅줉(蹂댁븞/?몄쓽 ???
        mode = os.getenv("WEB_SYNC_SSH_HOSTKEY_MODE", "accept-new")
        if mode:
            opts += ["-o", f"StrictHostKeyChecking={mode}"]

    # 援ы삎 ?쒕쾭 ?몄뒪?명궎(ssh-rsa/ssh-dss) ?덉슜 ?듭뀡 (?꾩슂 ?쒖뿉留??ъ슜)
    if _env_bool("WEB_SYNC_SSH_ALLOW_RSA", "1"):
        opts += ["-o", "HostKeyAlgorithms=+ssh-rsa", "-o", "PubkeyAcceptedAlgorithms=+ssh-rsa"]
    if _env_bool("WEB_SYNC_SSH_ALLOW_DSS", "0"):
        opts += ["-o", "HostKeyAlgorithms=+ssh-dss", "-o", "PubkeyAcceptedAlgorithms=+ssh-dss"]
    # Optional identity file (private key) for non-interactive auth
    try:
        key_path = (
            str(os.getenv("WEB_SYNC_SSH_KEY_PATH", "") or "").strip()
            or str(os.getenv("WEB_SYNC_SFTP_KEY_PATH", "") or "").strip()
        )
    except Exception:
        key_path = ""
    if key_path and os.path.exists(key_path):
        opts += ["-i", key_path, "-o", "IdentitiesOnly=yes"]
    return opts


async def _run_proc(cmd: list[str], *, timeout_sec: float) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        raise
    out = out_b.decode("utf-8", errors="replace") if out_b else ""
    err = err_b.decode("utf-8", errors="replace") if err_b else ""
    return int(proc.returncode or 0), out, err


def _sftp_password() -> Optional[str]:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_sftp_password"):
        return _RELOCATED_MODULE._sftp_password()
    return None


def _sftp_key_path() -> Optional[str]:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_sftp_key_path"):
        return _RELOCATED_MODULE._sftp_key_path()
    return None


def _sync_mode() -> str:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_sync_mode"):
        return _RELOCATED_MODULE._sync_mode()
    try:
        v = str(os.getenv("WEB_SYNC_MODE", "auto") or "auto").strip().lower()
    except Exception:
        v = "auto"
    return v


def _resolve_local_fileupload_root() -> Optional[str]:
    """
    濡쒖뺄 ?섍꼍?먯꽌 /FileUpload 寃쎈줈瑜??ㅼ젣 ?붾젆?좊━濡?留ㅽ븨?쒕떎.
    寃쎈줈 ?댁뒋 ?뺤씤 臾몄꽌: backend/docs/FILE_STORAGE_FLOW.md
    ?꾨떖 寃쎈줈 ?쇱썝?? ENV ?ㅼ젙 ??config.get_fileupload_root() ?ъ슜.
    ?곗꽑?쒖쐞:
    1) ENV FILEUPLOAD_ROOT (沅뚯옣) -> get_fileupload_root()
    2) ?꾨줈?앺듃 猷⑦듃??FileUpload/ ?붾젆?좊━(議댁옱 ??
    3) 誘몄꽕??None) -> None 諛섑솚
    """
    try:
        if os.getenv("FILEUPLOAD_ROOT"):
            return get_fileupload_root()
    except Exception:
        pass
    try:
        candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "FileUpload"))
        if os.path.isdir(candidate):
            return candidate
    except Exception:
        pass
    return None


def _rebase_fileupload_dir(path: str, root_override: str) -> str:
    """
    /FileUpload/{domain}/{tail} ?뺥깭 寃쎈줈瑜?root_override ?꾨옒濡??щ같移섑븳??
    寃쎈줈 ?댁뒋 ?뺤씤 臾몄꽌: backend/docs/FILE_STORAGE_FLOW.md
    ?꾨떖 寃쎈줈 ?쇱썝?? config.FILEUPLOAD_URL_PREFIX ?ъ슜.
    """
    try:
        raw = (path or "").replace("\\", "/")
    except Exception:
        raw = str(path or "")
    marker = FILEUPLOAD_URL_PREFIX + "/"
    if marker in raw:
        rel = raw.split(marker, 1)[1].lstrip("/")
        if rel:
            return os.path.join(root_override, *rel.split("/"))
    return os.path.join(root_override, os.path.basename(path or ""))


def _normalize_session_key(session_key: Optional[str]) -> Optional[str]:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_normalize_session_key"):
        return _RELOCATED_MODULE._normalize_session_key(session_key)
    try:
        key = str(session_key or "").strip()
    except Exception:
        key = ""
    return key or None


def _get_sftp_session(session_key: Optional[str]) -> Optional[Any]:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_get_sftp_session"):
        return _RELOCATED_MODULE._get_sftp_session(session_key)
    return None


def _drop_sftp_session(session_key: Optional[str]) -> Optional[Any]:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_drop_sftp_session"):
        return _RELOCATED_MODULE._drop_sftp_session(session_key)
    return None


def _sftp_session_active(session: Any) -> bool:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_sftp_session_active"):
        return _RELOCATED_MODULE._sftp_session_active(session)
    return False


def _open_sftp_session_blocking(
    *,
    session_key: str,
    target: Optional[str],
    port: Optional[int],
    timeout_sec: float,
) -> bool:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_open_sftp_session_blocking"):
        return _RELOCATED_MODULE._open_sftp_session_blocking(
            session_key=session_key, target=target, port=port, timeout_sec=timeout_sec
        )
    _ensure_env_loaded()
    return False


def _close_sftp_session_blocking(*, session_key: str) -> None:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_close_sftp_session_blocking"):
        return _RELOCATED_MODULE._close_sftp_session_blocking(session_key=session_key)
    return


async def begin_sftp_session(
    *,
    session_key: str,
    target: Optional[str] = None,
    port: Optional[int] = None,
    timeout_sec: Optional[float] = None,
) -> bool:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "begin_sftp_session"):
        return await _RELOCATED_MODULE.begin_sftp_session(
            session_key=session_key, target=target, port=port, timeout_sec=timeout_sec
        )
    return False


async def close_sftp_session(*, session_key: str) -> None:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "close_sftp_session"):
        return await _RELOCATED_MODULE.close_sftp_session(session_key=session_key)
    return


def _sftp_sync_blocking(
    *,
    target: str,
    port: int,
    local_file_path: str,
    remote_dir: str,
    timeout_sec: float,
    session_key: Optional[str] = None,
) -> bool:
    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "_sftp_sync_blocking"):
        return _RELOCATED_MODULE._sftp_sync_blocking(
            target=target,
            port=port,
            local_file_path=local_file_path,
            remote_dir=remote_dir,
            timeout_sec=timeout_sec,
            session_key=session_key,
        )
    return False


async def sync_file_to_webserver(
    *,
    local_file_path: str,
    access_base_url: str,
    chat_bot_id: str,
    db_name: Optional[str] = None,
    mode_override: Optional[str] = None,
    timeout_sec: Optional[float] = None,
    retries: Optional[int] = None,
    session_key: Optional[str] = None,
) -> bool:
    """
    ?щ·留??쒕쾭 濡쒖뺄 ?뚯씪??rsync over SSH濡??뱀꽌踰?紐⑹쟻吏 ?붾젆?좊━濡??꾩넚?쒕떎.

    ?깃났 議곌굔:
    - ssh mkdir -p ?깃났
    - rsync exit code == 0
    
    UTF-8 ?몄퐫??
    - ?뚯씪紐낃낵 寃쎈줈??UTF-8濡??뺢퇋?붾릺??泥섎━?⑸땲??
    - rsync??--protect-args ?듭뀡?쇰줈 ?뱀닔臾몄옄? UTF-8 寃쎈줈媛 蹂댄샇?⑸땲??
    """
    # 吏꾩엯????대컢/?뚮씪誘명꽣 濡쒓퉭
    start_ts = time.time()
    try:
        _debug_log(
            "H_sync_start",
            "utils/web_sync.py:sync_file_to_webserver",
            "entry",
            {
                "file": os.path.basename(local_file_path) if local_file_path else None,
                "access_base_url": access_base_url,
                "chat_bot_id": chat_bot_id,
                "mode_override": mode_override,
                "timeout_sec": timeout_sec,
                "retries": retries,
            },
        )
    except Exception:
        pass

    if _RELOCATED_MODULE and hasattr(_RELOCATED_MODULE, "sync_file_to_webserver"):
        try:
            logger.debug("[WebSync] delegated to relocated module sync_file_to_webserver")
        except Exception:
            pass
        return await _RELOCATED_MODULE.sync_file_to_webserver(
            local_file_path=local_file_path,
            access_base_url=access_base_url,
            chat_bot_id=chat_bot_id,
            mode_override=mode_override,
            timeout_sec=timeout_sec,
            retries=retries,
            session_key=session_key,
        )
    if not local_file_path:
        try:
            _debug_log(
                "H_sync_invalid_arg",
                "utils/web_sync.py:sync_file_to_webserver",
                "no_local_file",
                {"file": local_file_path},
            )
        except Exception:
            pass
        return False
    
    # Normalize local file path to a UTF-8 string.
    try:
        if isinstance(local_file_path, bytes):
            local_file_path = local_file_path.decode('utf-8', errors='replace')
        elif not isinstance(local_file_path, str):
            local_file_path = str(local_file_path)
    except Exception:
        local_file_path = str(local_file_path)
    
    try:
        if not os.path.exists(local_file_path):
            return False
    except Exception:
        return False

    target = _ssh_target()

    if not target:
        _debug_log(
            "H_sync_target_missing",
            "utils/web_sync.py:sync_file_to_webserver",
            "sync_no_ssh_target",
            {"file": os.path.basename(local_file_path)},
        )

    # ?댁쁺 ?섍꼍?먯꽌 留?PC留덈떎 ?섍꼍蹂???ㅼ젙???대졄?ㅻ㈃, 湲곕낯 ON?쇰줈 ?숈옉?섎룄濡??쒕떎.
    # - WEB_SYNC_ENABLED=0?대㈃ 紐낆떆?곸쑝濡?鍮꾪솢?깊솕
    enabled = _env_bool("WEB_SYNC_ENABLED", "1")

    if not enabled:
        _debug_log(
            "H_sync_disabled",
            "utils/web_sync.py:sync_file_to_webserver",
            "sync_skip_disabled",
            {"file": os.path.basename(local_file_path)},
        )
        return False

    try:
        t = float(timeout_sec if timeout_sec is not None else float(os.getenv("WEB_SYNC_RSYNC_TIMEOUT_SEC", "60") or "60"))
    except Exception:
        t = 60.0
    t = max(5.0, min(t, 600.0))

    try:
        r = int(retries if retries is not None else int(os.getenv("WEB_SYNC_RSYNC_RETRIES", "2") or "2"))
    except Exception:
        r = 2
    r = max(0, min(r, 10))

    # ?뱀꽌踰?紐⑹쟻吏 ?붾젆?좊━ (湲곕낯: ?뱀꽌踰??대? 寃쎈줈)
    # 寃쎈줈 ?댁뒋 ?뺤씤 臾몄꽌: backend/docs/FILE_STORAGE_FLOW.md
    remote_dir = get_webserver_uploaded_files_dir(
        access_base_url=access_base_url, 
        chat_bot_id=chat_bot_id, 
        db_name=db_name
    )
    try:
        expected_local_dir = remote_dir
        root_override_for_verify = _resolve_local_fileupload_root()
        if root_override_for_verify:
            expected_local_dir = _rebase_fileupload_dir(expected_local_dir, root_override_for_verify)
        expected_local_file = os.path.join(expected_local_dir, os.path.basename(local_file_path))
    except Exception:
        expected_local_file = ""

    def _expected_local_file_exists() -> bool:
        try:
            return bool(expected_local_file and os.path.isfile(expected_local_file))
        except Exception:
            return False

    def _verify_expected_local_file_or_log(stage: str, copied_path: str = "") -> bool:
        if _expected_local_file_exists():
            return True
        logger.warning(
            "[WebSync] 예상 FileUpload 파일이 누락됨 상태 | 단계=%s 예상경로=%s 복사경로=%s 원본=%s",
            stage,
            expected_local_file,
            copied_path,
            local_file_path,
        )
        return False


    # NAS delivery mode changes only the transport target. Storage path remains remote_dir.
    try:
        nas_enabled = _env_bool("WEB_SYNC_NAS_ENABLED", "1")
    except Exception:
        nas_enabled = False
    if nas_enabled:
        try:
            nas_host = os.getenv("WEB_SYNC_NAS_HOST", "110.45.147.56")
            nas_port = int(os.getenv("WEB_SYNC_NAS_PORT", "41") or "41")
            nas_user = os.getenv("WEB_SYNC_NAS_USER", "") or ""
            if nas_user:
                target = f"{nas_user}@{nas_host}"
            else:
                target = nas_host
            port = max(1, min(int(nas_port), 65535))
            _debug_log(
                "H_sync_nas_mode",
                "utils/web_sync.py:sync_file_to_webserver",
                "nas_mode_active",
                {"nas_host": nas_host, "nas_port": port, "nas_user": nas_user, "remote_dir": remote_dir},
            )
            logger.debug("[WebSync][TRACE] nas_mode | target=%s port=%s remote_dir=%s", target, port, remote_dir)
        except Exception:
            pass
    
    # Normalize remote directory to a UTF-8 string.
    try:
        if isinstance(remote_dir, bytes):
            remote_dir = remote_dir.decode('utf-8', errors='replace')
        elif not isinstance(remote_dir, str):
            remote_dir = str(remote_dir)
    except Exception:
        remote_dir = str(remote_dir)

    # Prioritize local delivery for /FileUpload paths: attempt local copy first (default).
    try:
        try:
            rd_norm = str(remote_dir or "")
        except Exception:
            rd_norm = ""
        if FILEUPLOAD_URL_PREFIX in rd_norm:
            try:
                dst_dir = remote_dir
                found_dir = None
                try:
                    if os.path.isdir(dst_dir):
                        found_dir = dst_dir
                except Exception:
                    found_dir = None

                if not found_dir:
                    # Search filesystem root for an existing directory that ends with the remote path suffix.
                    suffix = dst_dir.lstrip(os.sep)
                    max_dirs = int(os.getenv("WEB_SYNC_LOCAL_SEARCH_MAX_DIRS", "30000") or "30000")

                    
                    checked = 0
                    for dirpath, dirnames, _ in os.walk(os.sep, topdown=True):
                        checked += 1
                        if checked > max_dirs:
                            break
                        try:
                            if dirpath.rstrip(os.sep).endswith(suffix):
                                found_dir = dirpath
                                break
                        except Exception:
                            continue

                if found_dir:
                    import shutil
                    dst_file = os.path.join(found_dir, os.path.basename(local_file_path))
                    shutil.copy2(local_file_path, dst_file)
                    try:
                        dst_size = os.path.getsize(dst_file)
                    except Exception:
                        dst_size = None
                    _debug_log(
                        "H_sync_local_default",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "local_default_ok",
                        {"file": os.path.basename(local_file_path), "dest": found_dir},
                    )
                    try:
                        _debug_log(
                            "H_sync_success",
                            "utils/web_sync.py:sync_file_to_webserver",
                            "success_local_default",
                            {"duration_s": round(time.time() - start_ts, 3)},
                        )
                    except Exception:
                        pass
                    if _verify_expected_local_file_or_log("local_default", dst_file):
                        return True
                else:
                    pass
            except Exception as _e:
                logger.warning("[WebSync] local FileUpload default attempt failed | err=%s", _e)
                _debug_log(
                    "H_sync_local_default",
                    "utils/web_sync.py:sync_file_to_webserver",
                    "local_default_err",
                    {"file": os.path.basename(local_file_path), "err": str(_e)[:200]},
                )
    except Exception:
        pass

    port = _ssh_port()
    ssh_opts = _ssh_opts()
    # Diagnostic snapshot: capture expected/provided/parsed host and relevant env vars
    try:
        env_keys = [
            "WEB_SYNC_SSH_TARGET",
            "WEB_SYNC_DEFAULT_HOST",
            "WEB_SYNC_NAS_HOST",
            "WEB_SYNC_SFTP_USER",
            "WEB_SYNC_SFTP_PASSWORD",
            "WEB_SYNC_SSH_PORT",
            "WEB_SYNC_DEFAULT_PORT",
            "WEB_SYNC_SSH_KEY_PATH",
            "WEB_SYNC_NAS_PORT",
            "WEB_SYNC_NAS_USER",
            "WEB_SYNC_MODE",
        ]
        env_snapshot = {k: os.getenv(k) for k in env_keys}
    except Exception:
        env_snapshot = {}
    try:
        parsed_user, parsed_host = _parse_target(target or "") if target else (None, None)
    except Exception:
        parsed_user, parsed_host = (None, None)
    _debug_log(
        "H_sync_host_diag",
        "utils/web_sync.py:sync_file_to_webserver",
        "host_diagnostics",
        {
            "env": env_snapshot,
            "target_raw": target,
            "parsed_user": parsed_user,
            "parsed_host": parsed_host,
            "port": port,
            "nas_enabled": nas_enabled if "nas_enabled" in locals() else False,
        },
    )
    # mode selection: allow caller to force mode via parameter (e.g., 'sftp')
    if mode_override:
        try:
            mode = str(mode_override).strip().lower()
        except Exception:
            mode = _sync_mode()
    else:
        mode = _sync_mode()
    _debug_log(
        "H_sync_entry",
        "utils/web_sync.py:sync_file_to_webserver",
        "sync_entry",
        {
            "mode": mode,
            "target": target,
            "port": port,
            "remote_dir": remote_dir,
            "file": os.path.basename(local_file_path),
            "file_sig": _file_signature(local_file_path),
        },
    )
    _debug_log(
        "H_sync_ssh_opts",
        "utils/web_sync.py:sync_file_to_webserver",
        "ssh_opts",
        {"port": port, "opts": ssh_opts, "mode": mode},
    )
    # 珥덇린 ?ㅻ쪟 臾몄옄??蹂??(?대갚 濡쒖쭅?먯꽌 李몄“??
    last_err = ""

    if mode == "sftp":
        # SFTP 紐⑤뱶 ?쒖꽦?? 媛?ν븯硫?SFTP濡??꾩넚???쒕룄?섍퀬 ?ㅽ뙣 ??rsync濡??대갚?⑸땲??
        _ensure_env_loaded()
        try:
            _debug_log(
                "H_sync_sftp_attempt",
                "utils/web_sync.py:sync_file_to_webserver",
                "sftp_attempt",
                {"target": target, "port": port, "remote_dir": remote_dir, "file": os.path.basename(local_file_path)},
            )
        except Exception:
            pass

        try:
            # _sftp_sync_blocking? relocated implementation???덉쑝硫?洹몄そ???몄텧?⑸땲??
            sftp_ok = _sftp_sync_blocking(
                target=target or "",
                port=port,
                local_file_path=local_file_path,
                remote_dir=remote_dir,
                timeout_sec=t,
                session_key=session_key,
            )
        except Exception as _e:
            sftp_ok = False
            last_err = str(_e)

        if sftp_ok:
            _debug_log(
                "H_sync_sftp_ok",
                "utils/web_sync.py:sync_file_to_webserver",
                "sftp_ok",
                {"file": os.path.basename(local_file_path)},
            )
            try:
                _debug_log(
                    "H_sync_success",
                    "utils/web_sync.py:sync_file_to_webserver",
                    "success_sftp",
                    {"duration_s": round(time.time() - start_ts, 3)},
                )
            except Exception:
                pass
            if _verify_expected_local_file_or_log("sftp"):
                return True
            mode = "auto"
        else:
            logger.warning(
                "[WebSync] sftp attempt failed, falling back to rsync | target=%s remote_dir=%s file=%s err=%s",
                target,
                remote_dir,
                os.path.basename(local_file_path),
                (last_err or "")[:220],
            )
            # ?대갚?쇰줈 rsync ?쒕룄
            mode = "auto"

    # 濡쒖뺄 蹂듭궗 紐⑤뱶: SSH/rsync ?놁씠 ?뚯씠??shutil 蹂듭궗留??ъ슜. WEB_SYNC_MODE=local ??媛??以묒슂.
    # FILEUPLOAD_ROOT濡?濡쒖뺄 理쒖긽??寃쎈줈瑜?紐낆떆?섎㈃ ?ㅻℓ吏 ?딆쓬. mode_override="local" 濡?媛뺤젣 媛??
    if mode == "local":
        try:
            legacy_remote_dir = expected_local_dir or remote_dir

            try:
                logger.debug("[WebSync][TRACE] local_mode_prepare | remote_dir=%s", legacy_remote_dir)
                os.makedirs(legacy_remote_dir, exist_ok=True)
            except Exception:
                # ?붾젆?좊━ ?앹꽦 ?ㅽ뙣 ??寃쎄퀬留??④린怨??ㅽ뙣 泥섎━
                logger.warning("[WebSync] failed to create FileUpload dir: %s", legacy_remote_dir)
            dest_path = os.path.join(legacy_remote_dir, os.path.basename(local_file_path))
            try:
                import shutil
                logger.debug("[WebSync][TRACE] local_copy_prepare | src=%s dest_path=%s", local_file_path, dest_path)
                shutil.copy2(local_file_path, dest_path)
                try:
                    copied_size = os.path.getsize(dest_path)
                except Exception:
                    copied_size = None
                logger.debug("[WebSync][TRACE] local_copy_done | src=%s dest=%s", local_file_path, dest_path)
                _debug_log(
                    "H_sync_local_copy",
                    "utils/web_sync.py:sync_file_to_webserver",
                    "local_copy_ok",
                    {"src": local_file_path, "dest": dest_path, "size": copied_size},
                )
                try:
                    _debug_log(
                        "H_sync_success",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "success_local_copy",
                        {"duration_s": round(time.time() - start_ts, 3), "size": copied_size},
                    )
                except Exception:
                    pass
                if _verify_expected_local_file_or_log("local_mode", dest_path):
                    return True
                return False
            except Exception as exc:
                logger.warning("[WebSync] local FileUpload copy failed | src=%s dest=%s err=%s", local_file_path, dest_path, exc)
                _debug_log(
                    "H_sync_local_copy",
                    "utils/web_sync.py:sync_file_to_webserver",
                    "local_copy_error",
                    {"src": local_file_path, "dest": dest_path, "err": str(exc)[:200]},
                )
                return False
        except Exception as exc:
            logger.warning("[WebSync] local copy mode error | err=%s", exc)
            return False

    # ?먭꺽 mkdir -p
    remote_mkdir_cmd = f"mkdir -p {shlex.quote(remote_dir)}"
    if target:
        ssh_cmd = ["ssh", "-p", str(port), *ssh_opts, target, remote_mkdir_cmd]
    else:
        ssh_cmd = None
    _debug_log(
        "H_sync_cmd",
        "utils/web_sync.py:sync_file_to_webserver",
        "mkdir_start",
        {"target": target, "port": port, "remote_dir": remote_dir},
    )

    # rsync: ?붾젆?좊━濡?蹂대궡硫??뚯씪紐??좎?
    # - --partial/--delay-updates: ?꾩넚 以묐떒 ???ш컻/?먯옄??媛쒖꽑
    # - --protect-args: ?먭꺽 寃쎈줈/?뚯씪紐낆뿉???뱀닔臾몄옄 蹂댄샇
    dest = f"{target}:{remote_dir}/" if target else f"{remote_dir}/"
    rsync_cmd = [
        "rsync",
        "-az",
        "--partial",
        "--delay-updates",
        "--protect-args",
        "--no-owner",
        "--no-group",
        local_file_path,
        dest,
    ]

    last_err = ""
    force_sftp = False
    for attempt in range(0, r + 1):
        try:
            # log and measure mkdir execution
            if ssh_cmd:
                try:
                    _debug_log(
                        "H_sync_cmd",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "mkdir_exec",
                        {"cmd": " ".join(map(str, ssh_cmd)), "attempt": attempt},
                    )
                except Exception:
                    pass
                _start = time.time()
                rc1, out1, err1 = await _run_proc(ssh_cmd, timeout_sec=t)
                _dur_ms = int((time.time() - _start) * 1000)
                try:
                    _debug_log(
                        "H_sync_cmd",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "mkdir_done",
                        {"rc": rc1, "err": (err1 or out1 or "")[:200], "attempt": attempt, "duration_ms": _dur_ms, "out_len": len(out1 or ""), "err_len": len(err1 or "")},
                    )
                except Exception:
                    pass
                if rc1 != 0:
                    last_err = (err1 or out1 or "").strip()
                    if _is_ssh_crypto_error(last_err):
                        force_sftp = True
                        _debug_log(
                            "H_sync_crypto_error",
                            "utils/web_sync.py:sync_file_to_webserver",
                            "sync_crypto_error",
                            {"stage": "mkdir", "err": last_err[:200]},
                        )
                        break
                    raise RuntimeError(f"ssh mkdir failed rc={rc1} err={last_err[:300]}")
            else:
                # Local mkdir
                try:
                    _debug_log(
                        "H_sync_cmd",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "mkdir_local_exec",
                        {"remote_dir": remote_dir, "attempt": attempt},
                    )
                except Exception:
                    pass
                try:
                    os.makedirs(remote_dir, exist_ok=True)
                    rc1, out1, err1 = 0, "ok", ""
                except Exception as _e:
                    rc1, out1, err1 = 1, "", str(_e)
                _dur_ms = 0
                try:
                    _debug_log(
                        "H_sync_cmd",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "mkdir_done",
                        {"rc": rc1, "err": (err1 or out1 or "")[:200], "attempt": attempt, "duration_ms": _dur_ms, "out_len": len(out1 or ""), "err_len": len(err1 or "")},
                    )
                except Exception:
                    pass
                if rc1 != 0:
                    last_err = (err1 or out1 or "").strip()
                    raise RuntimeError(f"local mkdir failed rc={rc1} err={last_err[:300]}")
            if rc1 != 0:
                last_err = (err1 or out1 or "").strip()
                if _is_ssh_crypto_error(last_err):
                    force_sftp = True
                    _debug_log(
                        "H_sync_crypto_error",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "sync_crypto_error",
                        {"stage": "mkdir", "err": last_err[:200]},
                    )
                    break
                raise RuntimeError(f"ssh mkdir failed rc={rc1} err={last_err[:300]}")

            _debug_log(
                "H_sync_cmd",
                "utils/web_sync.py:sync_file_to_webserver",
                "rsync_start",
                {"file": os.path.basename(local_file_path), "remote_dir": remote_dir, "attempt": attempt, "rsync_cmd_preview": " ".join(map(str, rsync_cmd))[:1000]},
            )
            try:
                logger.debug("[WebSync][TRACE] rsync_start | attempt=%s rsync_cmd_preview=%s", attempt, " ".join(map(str, rsync_cmd))[:1000])
            except Exception:
                pass
            _start_r = time.time()
            rc2, out2, err2 = await _run_proc(rsync_cmd, timeout_sec=t)
            _dur_r_ms = int((time.time() - _start_r) * 1000)
            _debug_log(
                "H_sync_cmd",
                "utils/web_sync.py:sync_file_to_webserver",
                "rsync_done",
                {"rc": rc2, "err": (err2 or out2 or "")[:200], "attempt": attempt, "duration_ms": _dur_r_ms, "out_len": len(out2 or ""), "err_len": len(err2 or "")},
            )
            if rc2 == 0:
                logger.debug("[WebSync][TRACE] rsync_ok | attempt=%s remote_dir=%s file=%s dur_ms=%s", attempt, remote_dir, os.path.basename(local_file_path), _dur_r_ms)
                _debug_log(
                    "H_sync_rsync_ok",
                    "utils/web_sync.py:sync_file_to_webserver",
                    "sync_rsync_ok",
                    {"file": os.path.basename(local_file_path)},
                )
                # 異붽? ?숈옉: ?덇굅???명솚???꾪빐 ?먭꺽 ?쒕쾭??/FileUpload/{domain}/{tail} 寃쎈줈?먮룄 ?뚯씪??蹂듭궗
                try:
                    legacy_remote_dir = remote_dir
                    domain = ""
                    try:
                        parts = str(remote_dir or "").replace("\\", "/").strip("/").split("/")
                        if len(parts) >= 2 and parts[0] == FILEUPLOAD_URL_PREFIX.strip("/"):
                            domain = parts[1]
                    except Exception:
                        domain = ""
                    basename = os.path.basename(local_file_path)
                    remote_file_path = f"{remote_dir.rstrip('/')}/{basename}"
                    try:
                        tail = str(chat_bot_id or "").strip().split("-")[-1][-12:] or "unknown"
                    except Exception:
                        tail = "unknown"
                    public_web_dir = f"/home/{domain}/www/chat/uploaded_files/{tail}" if domain else ""
                    mirror_dirs = [legacy_remote_dir]
                    if public_web_dir and public_web_dir not in mirror_dirs:
                        mirror_dirs.append(public_web_dir)
                    copy_parts = []
                    for mirror_dir in mirror_dirs:
                        copy_parts.append(
                            "mkdir -p {dst} && cp -p {src} {dst}/ && test -f {dst}/{base}".format(
                                dst=shlex.quote(mirror_dir),
                                src=shlex.quote(remote_file_path),
                                base=shlex.quote(basename),
                            )
                        )
                    copy_cmd = " && ".join(copy_parts)
                    ssh_copy_cmd = ["ssh", "-p", str(port), *ssh_opts, target, copy_cmd]
                    try:
                        rc3, out3, err3 = await _run_proc(ssh_copy_cmd, timeout_sec=min(30.0, t))
                        _debug_log(
                            "H_sync_legacy_copy",
                            "utils/web_sync.py:sync_file_to_webserver",
                            "legacy_copy_done",
                            {"rc": rc3, "out": out3[:200], "err": err3[:200], "legacy_dir": legacy_remote_dir, "public_web_dir": public_web_dir},
                        )
                        if rc3 != 0:
                            raise RuntimeError(f"mirror copy failed rc={rc3} err={(err3 or out3 or '')[:300]}")
                    except Exception as _e:
                        logger.warning(
                            "[WebSync] mirror copy failed | source=%s legacy_dir=%s public_web_dir=%s err=%s",
                            remote_file_path,
                            legacy_remote_dir,
                            public_web_dir,
                            _e,
                        )
                        _debug_log(
                            "H_sync_legacy_copy",
                            "utils/web_sync.py:sync_file_to_webserver",
                            "legacy_copy_error",
                            {"err": str(_e)[:200], "legacy_dir": legacy_remote_dir, "public_web_dir": public_web_dir},
                        )
                        raise
                except Exception as mirror_exc:
                    last_err = str(mirror_exc)
                    raise
                try:
                    _debug_log(
                        "H_sync_success",
                        "utils/web_sync.py:sync_file_to_webserver",
                        "success_rsync",
                        {"duration_s": round(time.time() - start_ts, 3), "dur_ms": _dur_r_ms},
                    )
                except Exception:
                    pass
                if _verify_expected_local_file_or_log("rsync", expected_local_file):
                    return True
                return False
            last_err = (err2 or out2 or "").strip()
            if _is_ssh_crypto_error(last_err):
                force_sftp = True
                _debug_log(
                    "H_sync_crypto_error",
                    "utils/web_sync.py:sync_file_to_webserver",
                    "sync_crypto_error",
                    {"stage": "rsync", "err": last_err[:200]},
                )
                break
            raise RuntimeError(f"rsync failed rc={rc2} err={last_err[:300]}")
        except asyncio.TimeoutError:
            last_err = "timeout"
            _debug_log(
                "H_sync_timeout",
                "utils/web_sync.py:sync_file_to_webserver",
                "sync_timeout",
                {"file": os.path.basename(local_file_path), "stage": "mkdir_or_rsync"},
            )
        except Exception as exc:
            last_err = str(exc)

        if force_sftp:
            break
        if attempt < r:
            # 吏㏃? backoff
            try:
                delay = min(10.0, 1.5 * (2 ** attempt))
            except Exception:
                delay = 1.5
            logger.warning(
                "[WebSync] retrying | attempt=%s/%s file=%s err=%s wait=%.1fs",
                attempt + 1,
                r + 1,
                os.path.basename(local_file_path),
                (last_err or "")[:200],
                delay,
            )
            await asyncio.sleep(delay)


    logger.warning(
        "[WebSync] rsync failed (giving up) | target=%s remote_dir=%s file=%s err=%s",
        target,
        remote_dir,
        os.path.basename(local_file_path),
        (last_err or "")[:220],
    )
    _debug_log(
        "H_sync_rsync_failed",
        "utils/web_sync.py:sync_file_to_webserver",
        "sync_rsync_failed",
        {"file": os.path.basename(local_file_path), "err": (last_err or "")[:200]},
    )
    return False
