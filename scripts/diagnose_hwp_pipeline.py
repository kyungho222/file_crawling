"""
HWP 파이프라인 점검 스크립트 (터미널에서 관찰된 현상 검증용).

확인 항목:
  - 프로젝트 루트 / backend/.env 로드 여부 (config/settings는 backend/.env를 기본 스캔하지 않을 수 있음)
  - 임베딩에 쓰이는 OpenAI 계열 API 키 존재 여부(마스킹 표시)
  - HWP 버전, hwp_to_text 추출 길이·줄 수
  - hwp_edu와 동일한 RecursiveCharacterTextSplitter 기준 청크 수, batch_size=5일 때 배치 구간
  - 선택: 임베딩 API 스모크 호출 1회

사용 예:
  python scripts/diagnose_hwp_pipeline.py
  python scripts/diagnose_hwp_pipeline.py downloads/gwangjin.go.kr/체육시설\\ 안전점검\\ 지침.hwp
  python scripts/diagnose_hwp_pipeline.py --scan-downloads
  python scripts/diagnose_hwp_pipeline.py path/to/file.hwp --embed-smoke
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _prepend_sys_path() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


def _load_extra_dotenv() -> None:
    """settings.py가 찾지 못하는 backend/.env 등을 우선 로드."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for rel in (Path(".env"), Path("backend") / ".env"):
        p = ROOT / rel
        if p.is_file():
            load_dotenv(p, override=False)


def _mask_secret(s: str | None) -> str:
    """공유 로그 유출 방지: 값 본문은 출력하지 않음."""
    if not s or not str(s).strip():
        return "(비어 있음)"
    s = str(s).strip()
    return f"(설정됨, {len(s)}자)"


def _embedding_key_chain() -> tuple[str, str]:
    """hwp_edu에서 기대하는 키 우선순위와 동일하게 하나 선택."""
    from backend.config import Config

    for name in ("OPENAI_API_KEY", "OPENAI_ASADAL_API_KEY", "OPENAI_SECOND_API_KEY"):
        raw = getattr(Config, name, None)
        v = str(raw or "").strip()
        if v:
            return name, v
    return "(없음)", ""


def _print_env_banner(paths: list[Path]) -> None:
    print("=== .env 파일 존재 여부 ===")
    seen: set[Path] = set()
    for p in paths:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        print(f"  {p}  ->  {'있음' if p.is_file() else '없음'}")
    env_path = os.getenv("ENV_FILE_PATH")
    if env_path:
        print(f"  ENV_FILE_PATH={env_path!r}  ->  {'있음' if Path(env_path).is_file() else '없음'}")


def _print_api_keys() -> None:
    from backend.config import Config

    print("\n=== OpenAI 계열 키 (Config 로드 후, 값은 마스킹) ===")
    for name in ("OPENAI_API_KEY", "OPENAI_ASADAL_API_KEY", "OPENAI_SECOND_API_KEY"):
        v = getattr(Config, name, None)
        print(f"  {name}: {_mask_secret(v)}")
    picked, val = _embedding_key_chain()
    print(f"  → 임베딩에 쓸 후보(우선순위 적용 시): {picked}")
    base = os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL")
    if base:
        print(f"  OPENAI_API_BASE/OPENAI_BASE_URL: {base!r}")


def _print_chunk_plan(text: str, batch_size: int = 5) -> None:
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    from backend.config import Config

    size = int(getattr(Config, "BASIC_CHUNK_SIZE", 1000) or 1000)
    overlap = int(getattr(Config, "BASIC_CHUNK_OVERLAP", 50) or 50)
    splitter = RecursiveCharacterTextSplitter(chunk_size=size, chunk_overlap=overlap)
    chunks = splitter.split_text(text)
    n = len(chunks)
    print(f"\n=== 청크 (BASIC_CHUNK_SIZE={size}, OVERLAP={overlap}, hwp_edu와 동일 splitter) ===")
    print(f"  청크 개수: {n}")
    if n == 0:
        print("  → 청크가 0이면 임베딩 API를 호출하지 않아 401이 안 보일 수 있음.")
        return
    print(f"  hwp_edu batch_size={batch_size}: 병렬 임베딩 시 완료 순서가 섞여 로그의 chunk 번호가 뒤죽박죽일 수 있음.")
    for i in range(0, n, batch_size):
        hi = min(i + batch_size, n)
        print(f"  배치: 청크 인덱스 {i + 1}~{hi} (1-based)")


def _diagnose_one_hwp(path: Path) -> None:
    from edu.hwp_edu import detect_hwp_version, hwp_to_text

    print(f"\n=== 파일: {path} ===")
    if not path.is_file():
        print("  (파일 없음)")
        return
    ver = detect_hwp_version(str(path))
    print(f"  감지 버전: {ver!r}")
    try:
        text = hwp_to_text(str(path))
    except Exception as e:
        print(f"  hwp_to_text 실패: {type(e).__name__}: {e}")
        return
    raw_len = len(text)
    stripped = text.strip()
    print(f"  추출 텍스트 길이(문자): {raw_len}  /  strip 후: {len(stripped)}")
    print(f"  줄 수(대략): {text.count(chr(10)) + 1}")
    if raw_len == 0:
        print("  → 본문이 비어 있으면 청크 0 → 임베딩 단계 미진입(다른 HWP와 증상이 다르게 보일 수 있음).")
    preview = stripped[:200].replace("\n", "\\n")
    if preview:
        print(f"  미리보기: {preview!r}...")
    _print_chunk_plan(text, batch_size=5)


async def _embed_smoke() -> None:
    from langchain_openai import OpenAIEmbeddings

    name, key = _embedding_key_chain()
    if not key:
        print("\n=== --embed-smoke: 키 없음, 스킵 ===")
        return
    print(f"\n=== --embed-smoke: 모델 text-embedding-ada-002, 키 출처={name} ===")
    model = OpenAIEmbeddings(model="text-embedding-ada-002", openai_api_key=key)
    vec = await model.aembed_query("diagnose_hwp_pipeline smoke test")
    print(f"  응답 벡터 차원: {len(vec)} (정상)")


def _default_hwp_paths() -> list[Path]:
    legacy = ROOT / "downloads" / "보도자료20260115광진구 2026년 학습나루터 프로그램 공모.hwp"
    if legacy.is_file():
        return [legacy]
    dl = ROOT / "downloads"
    if dl.is_dir():
        found = sorted(dl.rglob("*.hwp"))
        if found:
            return [found[0]]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="HWP 추출·청크·API 키 진단")
    parser.add_argument(
        "paths",
        nargs="*",
        type=str,
        help="진단할 .hwp 경로(상대는 프로젝트 루트 기준). 생략 시 downloads에서 추론",
    )
    parser.add_argument(
        "--scan-downloads",
        action="store_true",
        help="downloads/ 이하 모든 .hwp 진단",
    )
    parser.add_argument(
        "--embed-smoke",
        action="store_true",
        help="임베딩 API 1회 호출(네트워크 사용)",
    )
    args = parser.parse_args()

    _prepend_sys_path()
    _load_extra_dotenv()

    # Config는 dotenv 로드 이후에 import
    _print_env_banner([ROOT / ".env", ROOT / "backend" / ".env"])
    _print_api_keys()

    print("\n=== 참고 (터미널 로그와의 대조) ===")
    print("  - process_chunks_parallel_hwp는 asyncio.gather(..., return_exceptions=True)라")
    print("    개별 청크가 401이 나도 process_hwp 최종 반환이 success일 수 있음.")
    print("  - 루트에 .env가 없고 backend/.env만 있으면, 앱이 루트만 찾을 때 키가 비어 401이 난다.")

    targets: list[Path] = []
    if args.scan_downloads:
        targets = sorted((ROOT / "downloads").rglob("*.hwp"))
    elif args.paths:
        for s in args.paths:
            p = Path(s)
            targets.append(p if p.is_absolute() else (ROOT / p))
    else:
        targets = _default_hwp_paths()

    if not targets:
        print("\n진단할 .hwp 경로가 없습니다. 경로를 인자로 주거나 --scan-downloads 를 사용하세요.")
        return 1

    for p in targets:
        _diagnose_one_hwp(p)

    if args.embed_smoke:
        asyncio.run(_embed_smoke())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
