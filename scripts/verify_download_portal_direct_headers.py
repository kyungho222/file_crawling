import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from core.crawler.workers.download import (
    _direct_http_request_headers_for_attempt,
    _is_portal_direct_file_url,
)


def main() -> None:
    base_headers = {
        "User-Agent": "file-crawler-test",
        "Cookie": "SESSION=abc",
    }
    source_page = "https://www.sb.go.kr/www/selectEminwonView.do?key=6483&notAncmtMgtNo=44336"
    portal_url = (
        "https://eminwon.sb.go.kr/emwp/jsp/ofr/FileDownNew.jsp?"
        "user_file_nm=test.hwpx&sys_file_nm=test_1.hwpx"
    )
    assert _is_portal_direct_file_url(portal_url)

    first = _direct_http_request_headers_for_attempt(
        base_headers,
        url=portal_url,
        source_page=source_page,
        attempt=1,
    )
    assert first["Cookie"] == "SESSION=abc"
    assert first["Referer"] == source_page
    assert "Origin" not in first

    second = _direct_http_request_headers_for_attempt(
        base_headers,
        url=portal_url,
        source_page=source_page,
        attempt=2,
    )
    assert second["Cookie"] == "SESSION=abc"
    assert second["Referer"] == source_page
    assert second["Origin"] == "https://www.sb.go.kr"

    normal = _direct_http_request_headers_for_attempt(
        base_headers,
        url="https://example.org/download/file.pdf",
        source_page=source_page,
        attempt=1,
    )
    assert normal["Origin"] == "https://www.sb.go.kr"


if __name__ == "__main__":
    main()
    print("portal direct header policy ok")
