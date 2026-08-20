"""Focused checks for domain breadcrumb profile lookup."""

from pathlib import Path

from backend.file.file_breadcrumb import extract_file_category_breadcrumb_from_html


def main() -> None:
    profile_dir = Path("backend/file/breadcrumb_profiles")
    domain = "test-breadcrumb.example"
    profile_path = profile_dir / domain / f"{domain}.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(
        '{\n'
        '  "selectors": [".site-trail"],\n'
        '  "category_index": -2,\n'
        '  "title_fallback": false\n'
        '}\n',
        encoding="utf-8",
    )
    try:
        html = '<div class="site-trail">HOME &gt; 행정정보 &gt; 재정 &gt; 재정공시</div>'
        assert (
            extract_file_category_breadcrumb_from_html(
                html,
                detail_url="https://test-breadcrumb.example/board/view",
            )
            == "재정"
        )
        print("file breadcrumb profile tests passed")
    finally:
        profile_path.unlink(missing_ok=True)
        profile_path.parent.rmdir()


if __name__ == "__main__":
    main()
