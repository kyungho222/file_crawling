import argparse
import asyncio
import contextlib
import gc
import logging
import os
import sys
import textwrap
from typing import List, Optional
from urllib.parse import urlparse

# 프로젝트 루트를 import 경로에 추가
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.board.board_content_workflow import BoardContentWorkflow
from backend.board.chuncheon_contract import is_chuncheon_contract_detail_url
from backend.board.hscity_board import is_hscity_photo_url
from backend.board.playwright_renderer import shutdown_playwright_renderer
from backend.shared.runtime_loop import initialize_runtime_hardening

initialize_runtime_hardening(component="scripts.inspect_detail.import")


@contextlib.contextmanager
def _chuncheon_timing_env(detail_wait_ms: Optional[int], step_wait_ms: Optional[int]):
    saved: List[tuple[str, Optional[str]]] = []
    try:
        if detail_wait_ms is not None:
            key = "BOARD_CHUNCHEON_DETAIL_WAIT_MS"
            saved.append((key, os.environ.get(key)))
            os.environ[key] = str(detail_wait_ms)
        if step_wait_ms is not None:
            key = "BOARD_CHUNCHEON_STEP_WAIT_MS"
            saved.append((key, os.environ.get(key)))
            os.environ[key] = str(step_wait_ms)
        yield
    finally:
        for key, old in saved:
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


def _quiet_backend_loggers() -> None:
    for name in (
        "backend",
        "backend.board",
        "backend.board.board_meta_extractor",
        "backend.board.board_content_extractor",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def _configure_inspect_logging(log_level: str = "INFO") -> None:
    lvl = getattr(logging, str(log_level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )
    _quiet_backend_loggers()
    logging.getLogger("backend.board.chuncheon_contract").setLevel(lvl)


def _section_line(title: str, width: int = 78) -> str:
    label = f" {title.strip()} "
    if len(label) >= width - 4:
        return "=" * width
    pad = width - len(label)
    left = pad // 2
    right = pad - left
    return f"{'=' * left}{label}{'=' * right}"


def _wrap_block(text: str, *, width: int = 76, indent: str = "  ") -> str:
    if not text:
        return ""
    lines_out: List[str] = []
    for raw in text.split("\n"):
        line = raw.rstrip()
        if not line:
            lines_out.append("")
            continue
        inner = max(24, width - len(indent))
        wrapped = textwrap.fill(
            line,
            width=width,
            initial_indent=indent,
            subsequent_indent=indent + "  ",
            break_long_words=True,
            break_on_hyphens=False,
        )
        if len(line) <= inner:
            wrapped = f"{indent}{line}"
        lines_out.extend(wrapped.split("\n"))
    return "\n".join(lines_out)


def _print_kv_block(
    rows: List[tuple[str, object]],
    indent: str = "  ",
    *,
    skip_empty: bool = False,
) -> None:
    if skip_empty:
        rows = [(key, value) for key, value in rows if str(value or "").strip()]
    if not rows:
        return
    width = max(len(key) for key, _ in rows)
    for key, value in rows:
        value_str = "" if value is None else str(value)
        if "\n" in value_str:
            print(f"{indent}{key.ljust(width)} :")
            for line in value_str.split("\n"):
                print(f"{indent}{' ' * (width + 3)}{line}")
        else:
            print(f"{indent}{key.ljust(width)} : {value_str}")


def _normalize_compare_value(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _consistency_summary(*labeled_values: tuple[str, object]) -> str:
    present: List[tuple[str, str]] = []
    for label, value in labeled_values:
        raw = str(value or "").strip()
        normalized = _normalize_compare_value(value)
        if normalized:
            present.append((label, raw))
    if not present:
        return "empty"
    normalized_values = {_normalize_compare_value(raw) for _, raw in present}
    if len(normalized_values) == 1:
        return f"ok | {present[0][1]}"
    detail = " | ".join(f"{label}={raw}" for label, raw in present)
    return f"mismatch | {detail}"


def _normalize_debug_title(title: str) -> str:
    text = str(title or "").strip()
    if not text:
        return ""
    if text.count(")") > text.count("("):
        close_idx = text.find(")")
        if 0 <= close_idx <= 24 and not text.startswith("("):
            text = f"({text}"
    return text


async def inspect_url(
    target_url: str,
    *,
    max_body_chars: Optional[int] = None,
    chuncheon_detail_wait_ms: Optional[int] = None,
    chuncheon_step_wait_ms: Optional[int] = None,
    log_level: str = "INFO",
) -> None:
    _configure_inspect_logging(log_level)

    print()
    print(_section_line("URL"))
    print(f"  {target_url}")
    print("=" * 78)

    workflow = BoardContentWorkflow()
    workflow.job_id = "inspect_job"
    workflow.db_name = "inspect_db"

    try:
        print("\n[1/2] HTML 수집 중...")
        html: Optional[str] = None
        fetch_method = ""

        if is_hscity_photo_url(target_url):
            html = await workflow._fetch_hscity_photo_html(target_url)
            fetch_method = "Static (hscity photo dedicated)"
            if not html:
                html = await workflow._fetch_html_static(target_url)
                fetch_method = "Static (requests, hscity fallback)"
            if not html:
                html = await workflow._fetch_html_playwright(target_url)
                fetch_method = "Dynamic (Playwright, hscity fallback)"
        elif is_chuncheon_contract_detail_url(target_url):
            print("  - 춘천 계약 상세: Playwright 우선 사용")
            with _chuncheon_timing_env(chuncheon_detail_wait_ms, chuncheon_step_wait_ms):
                html = await workflow._fetch_html_playwright(target_url)
            fetch_method = "Dynamic (Playwright, Chuncheon contract)"
            if not html:
                print("  - Playwright 수집 실패, 정적 HTML로 재시도합니다.")
                html = await workflow._fetch_html_static(target_url)
                fetch_method = "Static (requests, fallback after PW fail)"
        else:
            html = await workflow._fetch_html_static(target_url)
            fetch_method = "Static (requests)"
            if not html:
                print("  - Static 수집 실패, Playwright로 재시도합니다.")
                html = await workflow._fetch_html_playwright(target_url)
                fetch_method = "Dynamic (Playwright)"

        if not html:
            print("  [오류] HTML을 수집하지 못했습니다.")
            return

        print(f"  수집 방식: {fetch_method} | HTML 길이 {len(html):,}")

        print("\n[2/2] DB 저장 최종값 조립 중...")
        finalized = await workflow._build_final_runtime_parse(
            url=target_url,
            html=html,
            record_title=False,
        )
        if not finalized:
            print("  [오류] DB 저장 최종값을 조립하지 못했습니다.")
            return

        final_title = _normalize_debug_title(finalized.get("clean_title"))
        final_web_title = _normalize_debug_title(finalized.get("web_title"))
        final_content = str(finalized.get("clean_content") or "")
        reg_date_val = str(finalized.get("reg_date_val") or "")
        runtime_output = dict(finalized.get("runtime_output") or {})
        display = dict(runtime_output.get("display") or {})
        post_info = dict(runtime_output.get("post_info") or {})
        learning_result = dict(runtime_output.get("learning_result") or {})
        content_preview_chars = max_body_chars if max_body_chars is not None else 1200

        print()
        print(_section_line("Final Summary"))
        _print_kv_block(
            [
                ("title", final_title),
                ("web_title", final_web_title),
                ("reg_date", reg_date_val),
                ("content_len", len(final_content)),
                ("author", post_info.get("author")),
                ("department", post_info.get("department")),
                ("has_attachments", post_info.get("has_attachments")),
                ("attachment_count", display.get("attachment_count")),
            ]
        )

        print()
        print(_section_line("Consistency"))
        _print_kv_block(
            [
                (
                    "title",
                    _consistency_summary(
                        ("final", final_title),
                        ("display", display.get("subject")),
                        ("db", post_info.get("title")),
                        ("learning", learning_result.get("title")),
                    ),
                ),
                (
                    "web_title",
                    _consistency_summary(
                        ("final", final_web_title),
                        ("display", display.get("web_title")),
                        ("db", post_info.get("web_title")),
                    ),
                ),
                (
                    "reg_date",
                    _consistency_summary(
                        ("final", reg_date_val),
                        ("display", display.get("content_created_at")),
                        ("db", post_info.get("content_created_at")),
                        ("learning", learning_result.get("reg_date")),
                    ),
                ),
                (
                    "author",
                    _consistency_summary(
                        ("display", display.get("content_author")),
                        ("db", post_info.get("author")),
                        ("learning", learning_result.get("author")),
                    ),
                ),
                (
                    "department",
                    _consistency_summary(
                        ("display", display.get("department")),
                        ("db", post_info.get("department")),
                        ("learning", learning_result.get("department")),
                    ),
                ),
            ]
        )

        print()
        print(_section_line("DB Save Payload"))
        _print_kv_block(
            [
                ("title", post_info.get("title")),
                ("web_title", post_info.get("web_title")),
                ("author", post_info.get("author")),
                ("author_kind", post_info.get("author_kind")),
                ("author_raw", post_info.get("author_raw")),
                ("department", post_info.get("department")),
                ("content_created_at", post_info.get("content_created_at")),
                ("content_updated_at", post_info.get("content_updated_at")),
                ("has_attachments", post_info.get("has_attachments")),
                ("cate1", post_info.get("cate1")),
                ("cate2", post_info.get("cate2")),
                ("memo1", post_info.get("memo1")),
                ("board_url", post_info.get("board_url")),
                ("contact_phone", display.get("contact_phone")),
                ("view_count", display.get("view_count")),
            ],
            skip_empty=True,
        )

        print()
        print(_section_line("Learning Payload"))
        _print_kv_block(
            [
                ("title", learning_result.get("title")),
                ("content_type", learning_result.get("content_type")),
                ("type", learning_result.get("type")),
                ("author", learning_result.get("author")),
                ("department", learning_result.get("department")),
                ("reg_date", learning_result.get("reg_date")),
                ("size", learning_result.get("size")),
                ("cate1", learning_result.get("cate1")),
                ("cate2", learning_result.get("cate2")),
                ("source", learning_result.get("source")),
            ],
            skip_empty=True,
        )

        print()
        shown_chars = min(len(final_content), content_preview_chars)
        print(_section_line(f"Content Preview ({shown_chars:,} / {len(final_content):,})"))
        if len(final_content) > content_preview_chars:
            shown = final_content[:content_preview_chars]
            omitted = len(final_content) - content_preview_chars
            print(_wrap_block(shown))
            print()
            print(f"  ... 이하 {omitted:,}자 생략 (--max-body-chars 조정 가능)")
        else:
            print(_wrap_block(final_content))

        print()
        print("=" * 78 + "\n")

    finally:
        with contextlib.suppress(Exception):
            await workflow._close_http_session()
        with contextlib.suppress(Exception):
            await workflow._close_playwright()
        with contextlib.suppress(Exception):
            await shutdown_playwright_renderer()


def main() -> None:
    parser = argparse.ArgumentParser(description="게시판 상세 URL의 DB 저장 최종값 확인")
    parser.add_argument("url", help="검증할 게시판 상세 페이지 URL")
    parser.add_argument(
        "--max-body-chars",
        type=int,
        default=1200,
        metavar="N",
        help="본문 preview 최대 길이 (기본 1200)",
    )
    parser.add_argument(
        "--chuncheon-detail-wait-ms",
        type=int,
        default=None,
        metavar="MS",
        help="춘천 계약 상세 Playwright 추가 대기 시간",
    )
    parser.add_argument(
        "--chuncheon-step-wait-ms",
        type=int,
        default=None,
        metavar="MS",
        help="춘천 계약 상세 단계별 추가 대기 시간",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        metavar="LEVEL",
        help="로그 레벨 (DEBUG, INFO, WARNING, ERROR)",
    )
    args = parser.parse_args()
    target_url = str(args.url or "").strip().rstrip("\\")
    parsed_url = urlparse(target_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        print()
        print("[오류] inspect_detail.py에는 게시판 상세 URL을 입력해야 합니다.")
        print(f"  입력값: {args.url}")
        print("  예시: python scripts/inspect_detail.py \"https://photo.hscity.go.kr/photo/detail?photo_id=P000157107\"")
        raise SystemExit(2)

    try:
        asyncio.run(
            inspect_url(
                target_url,
                max_body_chars=args.max_body_chars,
                chuncheon_detail_wait_ms=args.chuncheon_detail_wait_ms,
                chuncheon_step_wait_ms=args.chuncheon_step_wait_ms,
                log_level=args.log_level,
            )
        )
    finally:
        gc.collect()


if __name__ == "__main__":
    main()
