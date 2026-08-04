"""Regression check for file-crawl exploration counter initialization."""

from __future__ import annotations


def _resolve_stream_post_count(data: dict, start_urls: list) -> int:
    if start_urls:
        return len(start_urls)
    return max(
        0,
        int(
            data.get("pre_explored_start_urls_count")
            or data.get("exploration_post_total_count")
            or data.get("exploration_display_max_count")
            or 0
        ),
    )


def main() -> None:
    stream_data = {
        "pre_explored_start_urls_count": 470,
        "exploration_post_total_count": 470,
        "exploration_display_max_count": 470,
    }
    assert _resolve_stream_post_count(stream_data, []) == 470
    assert _resolve_stream_post_count(stream_data, ["a", "b"]) == 2
    print("OK: stream post count preserves exploration DB base")


if __name__ == "__main__":
    main()