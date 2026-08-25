import os
import shutil
import sys
from pathlib import Path


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.file.fast_attachment_extractor import extract_fast_attachments
from backend.file.file_breadcrumb import (
    extract_file_breadcrumb_tokens_from_html,
    extract_file_web_title_from_html,
)
from backend.file.site_config import load_file_site_config, save_file_site_config


def main() -> None:
    temp_dir = Path(ROOT) / ".verify-file-site-config"
    shutil.rmtree(temp_dir, ignore_errors=True)
    try:
        config = {
            "attachment_selectors": [".site-download a[data-file-name]"],
            "attachment_name_attributes": ["data-file-name"],
            "breadcrumb_selectors": [".site-breadcrumb"],
        }
        saved_path = save_file_site_config(
            "https://www.example.go.kr/detail/1",
            config,
            config_dir=temp_dir,
        )
        assert saved_path.endswith("example.go.kr.json")

        loaded = load_file_site_config(
            "https://example.go.kr/detail/2",
            config_dir=temp_dir,
        )
        assert loaded == config

        html = """
        <div class="site-download">
          <a href="/files/opaque-file.hwp" data-file-name="사람이 읽는 신청서.hwp">내려받기</a>
        </div>
        """
        attachments = extract_fast_attachments(
            html,
            "https://example.go.kr/detail/2",
            site_config=loaded,
        )
        assert len(attachments) == 1
        assert attachments[0]["name"] == "사람이 읽는 신청서.hwp"

        sen_attachments = extract_fast_attachments(
            "<div class='print-box-wrap'><a href='/resources/www/data/accreditation3_2.hwp' "
            "download='유치원 설립자 명의 변경인가 신청서.hwp'>다운로드</a></div>",
            "https://www.sen.go.kr/detail/1",
        )
        assert sen_attachments[0]["name"] == "유치원 설립자 명의 변경인가 신청서.hwp"

        gwangjin_html = "<div class='hgroup'><p>HOME > 행정정보 > 우리구 살림 > 재정 > 재정공시</p></div>"
        assert extract_file_breadcrumb_tokens_from_html(
            gwangjin_html,
            detail_url="https://www.gwangjin.go.kr/detail/1",
        ) == ["행정정보", "우리구 살림", "재정", "재정공시"]
        assert extract_file_web_title_from_html(
            "<h4 class='typo-title02'>해당부서 민원</h4>",
            detail_url="https://www.sen.go.kr/detail/1",
        ) == "해당부서 민원"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
    print("file site config policy ok")
