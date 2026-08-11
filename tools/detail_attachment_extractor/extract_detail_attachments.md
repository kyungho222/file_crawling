# extract_detail_attachments.py 흐름

`extract_detail_attachments.py`는 게시글 또는 민원 상세페이지 URL에서 문서 첨부파일 URL과 파일명을 추출하는 독립 모듈입니다.

이 모듈은 DB 저장, 크롤링 큐 등록, 파일 학습을 수행하지 않습니다. 상세페이지 조회와 첨부파일 링크 확인만 담당합니다.

## 사용 방법

```powershell
python tools\detail_attachment_extractor\extract_detail_attachments.py "https://example.go.kr/board/view.do?seq=1"
```

상세페이지 URL을 인자로 전달하면 첨부파일 URL과 파일명을 JSON으로 출력합니다.

## 처리 흐름

```text
상세페이지 URL 입력
  -> extract_from_url()
  -> fetch_detail_html(): 상세페이지 HTML GET
  -> extract_attachments(): HTML 태그에서 첨부 후보 수집
  -> _resolve_candidate(): href, data-url, onclick에서 실제 URL 확인
  -> _looks_like_document(): 문서 확장자 또는 다운로드 경로인지 판별
  -> _canonical_key(): 동일 첨부 URL 중복 제거
  -> _resolve_attachment_file_names(): HEAD 또는 Range GET 헤더로 파일명 보완
  -> JSON 응답 반환
```

## 첨부 URL 추출

`extract_attachments()`는 `a`, `button`, `input`, `form` 태그를 검사합니다.

- `href`, `data-href`, `data-url`, `data-download-url`, `formaction` 속성
- `onclick` 문자열 안의 절대 또는 상대 URL

이미지, CSS, JavaScript 같은 비문서 확장자는 제외합니다. PDF, HWP/HWPX, Office 문서, TXT/CSV, ZIP/7z와 다운로드 경로 패턴을 문서 후보로 취급합니다.

## 파일명 결정

파일명은 아래 순서로 보완합니다.

1. 상세페이지의 표시명과 HTML 속성
2. 다운로드 URL의 `user_file_nm`, `filename`, `original_filename` 등의 쿼리 파라미터
3. 다운로드 응답의 `Content-Disposition` 헤더
4. 다운로드 URL 경로의 파일명

URL 인코딩된 파일명은 UTF-8 한글로 디코딩합니다.

## 응답 형식

```json
{
  "detail_url": "상세페이지 URL",
  "attachment_count": 1,
  "attachments": [
    {
      "title": "상세페이지의 표시명",
      "file_name": "저장에 사용할 실제 파일명",
      "url": "원본 첨부파일 URL",
      "source": "attribute 또는 onclick"
    }
  ]
}
```

- `title`: 페이지의 링크 텍스트나 `title` 속성처럼 사람이 보는 표시명
- `file_name`: 다운로드 또는 저장에 사용할 파일명
- `source`: `attribute`는 HTML 속성, `onclick`은 자바스크립트 이벤트에서 URL을 찾았다는 뜻

## 주요 함수

| 함수 | 역할 |
| --- | --- |
| `extract_from_url()` | URL 검증부터 첨부 추출, 파일명 보완, JSON 조립까지 실행 |
 - payload: url 입력
| `fetch_detail_html()` | 상세페이지 HTML을 HTTP GET으로 조회 |
| `extract_attachments()` | HTML에서 문서 첨부 후보를 찾고 중복 제거 |
| `_resolve_candidate()` | 속성 또는 `onclick`에서 실제 첨부 URL을 결정 |
| `_file_name_from_value()` | 표시명과 URL로 기본 파일명을 추정 |
| `_file_name_from_url_query()` | URL 쿼리의 원본 파일명 파라미터를 추출 |
| `_file_name_from_content_disposition()` | 응답 헤더의 파일명을 한글로 디코딩 |
| `_resolve_attachment_file_names()` | HEAD/Range GET으로 파일명을 추가 보완 |
