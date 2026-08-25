import os
import sys
from urllib.parse import unquote, urlparse


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.file.fast_attachment_extractor import extract_fast_attachments
from config.settings import get_file_upload_content_url
from core.crawler.workers.download import _best_download_filename
from core.crawler.workers.download import make_safe_storage_filename
from utils.attachment_display_name import is_generated_attachment_storage_name


def main() -> None:
    html = """
    <div class="print-box-wrap">
      <div class="list">
        <p class="title">서식</p>
        <div class="btn">
          <a href="/resources/www/data/accreditation3_2.hwp" download="유치원 설립자 명의 변경인가 신청서.hwp" class="btn icon">다운로드</a>
        </div>
      </div>
    </div>
    """
    attachments = extract_fast_attachments(html, "https://www.example.go.kr/page.jsp")
    assert len(attachments) == 1
    assert attachments[0]["name"] == "유치원 설립자 명의 변경인가 신청서.hwp"

    selected = _best_download_filename(
        "accreditation3_2.hwp",
        url="https://www.example.go.kr/resources/www/data/accreditation3_2.hwp",
        file_meta={
            "attachment_name": attachments[0]["name"],
            "download_name": attachments[0]["download_name"],
        },
    )
    assert selected == "유치원 설립자 명의 변경인가 신청서.hwp"

    assert is_generated_attachment_storage_name("conveminwon_2_warrant.hwp")
    selected_from_response = _best_download_filename(
        "conveminwon_2_warrant.hwp",
        url="https://www.example.go.kr/resources/www/data/conveminwon_2_warrant.hwp",
        file_meta={"attachment_name": "위임장 서식.hwp"},
    )
    assert selected_from_response == "위임장 서식.hwp"

    # The physical WebSync name is the safe form of the download attribute,
    # and LEARN_LIST.content uses exactly that physical filename.
    storage_filename = make_safe_storage_filename(selected)
    assert storage_filename == "유치원 설립자 명의 변경인가 신청서.hwp"
    assert "accreditation3_2" not in storage_filename.lower()
    content_url = get_file_upload_content_url(
        "https://www.example.go.kr",
        "example.go.kr",
        "12345678-1234-1234-1234-123456789012",
        storage_filename,
    )
    assert unquote(urlparse(content_url).path).endswith(f"/{storage_filename}")


if __name__ == "__main__":
    main()
    print("human-readable attachment title policy ok")
