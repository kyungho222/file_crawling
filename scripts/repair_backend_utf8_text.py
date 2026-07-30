#!/usr/bin/env python3
"""Repair Korean mojibake and Hangul unicode escapes in backend text files.

Default mode is a dry run. Use --apply to write UTF-8 files.
"""

from __future__ import annotations

import argparse
import difflib
import re
from dataclasses import dataclass
from pathlib import Path


TEXT_EXTS = {".py", ".json", ".md", ".html"}
SKIP_PARTS = {"__pycache__", "logs", "downloads", ".ipynb_checkpoints"}
SKIP_SUFFIXES = {".pyc", ".pyo", ".corrupt_backup"}
BAD_MARKERS = (
    "占",
    "�",
    "뚯",
    "쒕",
    "덉",
    "븯",
    "땲",
    "떎",
    "袁",
    "筌",
    "揶",
    "疫",
    "癒",
    "濡",
    "醫",
    "理",
    "猷",
    "湲",
    "꾨",
    "묒",
    "ㅼ",
    "덈",
    "녿",
    "쓬",
    "쇰",
    "섍",
    "뱶",
    "쒖",
    "젙",
    "좎",
    "숈",
    "뚰",
    "겕",
    "퀎",
    "瑜",
    "媛",
    "먯",
)
UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")
HANGUL_RE = re.compile(r"[가-힣]")


@dataclass
class FileReport:
    path: Path
    unicode_escapes: int = 0
    backup_lines: int = 0
    remaining_bad_lines: int = 0
    changed: bool = False


def should_scan(path: Path) -> bool:
    if any(part in SKIP_PARTS for part in path.parts):
        return False
    if path.name.startswith("crawled_") and path.suffix == ".json":
        return False
    if path.suffix in SKIP_SUFFIXES:
        return False
    return path.suffix in TEXT_EXTS


def bad_score(text: str) -> int:
    return sum(text.count(marker) for marker in BAD_MARKERS)


def hangul_count(text: str) -> int:
    return len(HANGUL_RE.findall(text))


def ascii_skeleton(text: str) -> str:
    return "".join(ch for ch in text if 32 <= ord(ch) < 127).strip()


def decode_hangul_escapes(text: str) -> tuple[str, int]:
    count = 0

    def repl(match: re.Match[str]) -> str:
        nonlocal count
        codepoint = int(match.group(1), 16)
        if 0xAC00 <= codepoint <= 0xD7A3:
            count += 1
            return chr(codepoint)
        return match.group(0)

    return UNICODE_ESCAPE_RE.sub(repl, text), count


def compatible_line(current: str, backup: str) -> bool:
    cur = ascii_skeleton(current)
    bak = ascii_skeleton(backup)
    if cur == bak:
        return True
    if not cur or not bak:
        return False
    return difflib.SequenceMatcher(None, cur, bak).ratio() >= 0.92


def candidate_backup_indexes(
    current_index: int,
    current_len: int,
    backup_len: int,
    window: int = 80,
) -> range:
    if current_len <= 0 or backup_len <= 0:
        return range(0)
    center = round(current_index * backup_len / current_len)
    start = max(0, center - window)
    end = min(backup_len, center + window + 1)
    return range(start, end)


def repair_with_backup(text: str, backup_text: str) -> tuple[str, int]:
    current_lines = text.splitlines(keepends=True)
    backup_lines = backup_text.splitlines(keepends=True)
    replacements = 0
    used_backup_indexes: set[int] = set()

    def maybe_replace(current_index: int, backup_index: int) -> bool:
        nonlocal replacements
        if backup_index in used_backup_indexes:
            return False
        current = current_lines[current_index]
        backup = backup_lines[backup_index]
        if bad_score(current) <= 0:
            return False
        if bad_score(backup) > 0:
            return False
        if hangul_count(backup) <= hangul_count(current):
            return False
        if compatible_line(current, backup):
            current_lines[current_index] = backup
            used_backup_indexes.add(backup_index)
            replacements += 1
            return True
        return False

    for idx in range(min(len(current_lines), len(backup_lines))):
        maybe_replace(idx, idx)

    backup_by_skeleton: dict[str, list[int]] = {}
    for idx, line in enumerate(backup_lines):
        if bad_score(line) > 0 or hangul_count(line) <= 0:
            continue
        key = ascii_skeleton(line)
        if key:
            backup_by_skeleton.setdefault(key, []).append(idx)

    for idx, current in enumerate(current_lines):
        if bad_score(current) <= 0:
            continue
        key = ascii_skeleton(current)
        exact_candidates = backup_by_skeleton.get(key, [])
        if len(exact_candidates) == 1 and maybe_replace(idx, exact_candidates[0]):
            continue

        best_idx: int | None = None
        best_ratio = 0.0
        cur_key = key
        if not cur_key:
            continue
        for backup_idx in candidate_backup_indexes(idx, len(current_lines), len(backup_lines)):
            if backup_idx in used_backup_indexes:
                continue
            backup = backup_lines[backup_idx]
            if bad_score(backup) > 0 or hangul_count(backup) <= hangul_count(current):
                continue
            bak_key = ascii_skeleton(backup)
            if not bak_key:
                continue
            ratio = difflib.SequenceMatcher(None, cur_key, bak_key).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_idx = backup_idx
        if best_idx is not None and best_ratio >= 0.94:
            maybe_replace(idx, best_idx)

    return "".join(current_lines), replacements

def collect_bad_lines(text: str) -> list[tuple[int, str]]:
    return [
        (line_no, line.strip())
        for line_no, line in enumerate(text.splitlines(), 1)
        if bad_score(line) > 0
    ]


def write_unresolved_report(reports: list[FileReport], root: Path, report_path: Path) -> None:
    lines: list[str] = []
    for report in reports:
        if report.remaining_bad_lines <= 0:
            continue
        try:
            text = report.path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        bad_lines = collect_bad_lines(text)
        if not bad_lines:
            continue
        lines.append(f"{report.path.as_posix()} ({len(bad_lines)} lines)")
        for line_no, line in bad_lines[:20]:
            lines.append(f"  {line_no}: {line[:220]}")
        if len(bad_lines) > 20:
            lines.append(f"  ... {len(bad_lines) - 20} more")
        lines.append("")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")

def process_file(path: Path, root: Path, backup_root: Path) -> tuple[str, FileReport]:
    original = path.read_text(encoding="utf-8", errors="replace")
    text, escape_count = decode_hangul_escapes(original)

    rel = path.relative_to(root)
    backup_path = backup_root / rel
    backup_lines = 0
    if backup_path.exists() and backup_path.is_file():
        backup_text = backup_path.read_text(encoding="utf-8", errors="replace")
        text, backup_lines = repair_with_backup(text, backup_text)

    remaining_bad_lines = sum(1 for line in text.splitlines() if bad_score(line) > 0)
    report = FileReport(
        path=path,
        unicode_escapes=escape_count,
        backup_lines=backup_lines,
        remaining_bad_lines=remaining_bad_lines,
        changed=text != original,
    )
    return text, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="backend", help="directory to scan")
    parser.add_argument("--backup-root", default="# back/backend", help="backup directory")
    parser.add_argument("--apply", action="store_true", help="write repaired files")
    parser.add_argument("--limit", type=int, default=40, help="max changed files to print")
    parser.add_argument("--report", default="tmp/backend_utf8_remaining_report.txt", help="unresolved line report path")
    args = parser.parse_args()

    root = Path(args.root)
    backup_root = Path(args.backup_root)
    if not root.exists():
        raise SystemExit(f"root not found: {root}")

    reports: list[FileReport] = []
    changed_payloads: list[tuple[Path, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or not should_scan(path):
            continue
        repaired, report = process_file(path, root, backup_root)
        if report.changed:
            changed_payloads.append((path, repaired))
        reports.append(report)

    changed = [report for report in reports if report.changed]
    total_escapes = sum(report.unicode_escapes for report in reports)
    total_backup_lines = sum(report.backup_lines for report in reports)
    total_remaining = sum(report.remaining_bad_lines for report in reports)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] scanned={len(reports)} changed_files={len(changed)}")
    print(f"[{mode}] decoded_hangul_escapes={total_escapes} backup_line_replacements={total_backup_lines}")
    print(f"[{mode}] unresolved_bad_lines={total_remaining}")

    for report in changed[: args.limit]:
        rel = report.path.as_posix()
        print(
            f"- {rel}: escapes={report.unicode_escapes}, "
            f"backup_lines={report.backup_lines}, unresolved_bad_lines={report.remaining_bad_lines}"
        )
    if len(changed) > args.limit:
        print(f"... {len(changed) - args.limit} more changed files")

    report_path = Path(args.report)
    if total_remaining:
        write_unresolved_report(reports, root, report_path)
        print(f"[{mode}] unresolved_report={report_path}")
        print(f"[{mode}] note=unresolved lines contain replacement characters or have no clean backup match; review manually.")

    if args.apply:
        for path, repaired in changed_payloads:
            path.write_text(repaired, encoding="utf-8", newline="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
