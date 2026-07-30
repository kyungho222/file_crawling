# 파일크롤링 저장 플로우

파일크롤링(`colle=file`)은 게시판 크롤링과 같은 상세 페이지 수집 기반을 일부 공유하지만, 워크플로 조립 단계에서 파일 전용 모듈로 분기한다.

## 모듈 경계

- 진입 조립: `backend/shared/workflow_dispatch_assembly.py`
- 게시판 워크플로 생성: `backend/board/board_crawl_module.py`
- 파일 워크플로 생성: `backend/file/file_crawl_module.py`
- 파일 수집 워크플로: `backend/file/file_download_workflow.py`
- 첨부파일 큐/다운로드 파이프라인: `backend/board/file_content_workflow.py`
- 실제 다운로드 worker: `core/crawler/workers/download.py`
- FileUpload 전달: `utils/web_sync.py`
- LEARN_LIST 저장 URL 생성: `db/mariadb_save_update.py`

`content_type`이 `file`, `attach`, `attachment` 중 하나이거나 `colle=file`이면 파일 워크플로로 고정된다. `file_crawl_module.create_file_crawl_workflow()`는 생성 시점에 `colle`, `colle_mode`, `ui_colle`, `file_mode`, `content_type`을 파일 모드로 맞춰 다른 크롤링 모드와 섞이지 않도록 한다.

## 저장 경로 규칙

파일은 세 가지 경로 표현을 가진다.

1. 임시/작업 다운로드 경로

   ```text
   downloads/{storage_domain}/{uuid_tail12}/{filename}
   ```

   다운로드 worker가 먼저 파일을 저장하는 로컬 작업 경로다.

2. FileUpload 물리 저장 경로

   ```text
   /FileUpload/{storage_domain}/{uuid_tail12}/{filename}
   ```

   스토리지 서버 또는 로컬 FileUpload 루트에 최종 전달되는 실제 파일 위치다. `storage_domain`은 `db_name` 기준으로 `get_storage_domain_for_db_name()`에서 결정하고, `uuid_tail12`는 `chat_bot_id`의 마지막 UUID 조각이다.

3. 웹/DB 공개 URL

   ```text
   https://{web_domain}/chat/uploaded_files/{uuid_tail12}/{filename}
   ```

   LEARN_LIST `content`에 저장되는 URL이다. 웹서버는 `/chat/uploaded_files/{uuid_tail12}`를 FileUpload 물리 경로의 `{storage_domain}/{uuid_tail12}`로 매핑한다.

예시:

```text
물리 경로: /FileUpload/dev.han.kr/479e6e05af4d/notice.hwpx
공개 URL: https://dev.han.kr/chat/uploaded_files/479e6e05af4d/notice.hwpx
```

## 경로 변환 상세

현재 코드 기준 경로 변환은 아래 함수들이 담당한다.

| 단계 | 함수 | 결과 |
| --- | --- | --- |
| 작업 다운로드 디렉터리 | `get_uploaded_files_local_dir(access_base_url, chat_bot_id, storage_domain)` | `{project}/downloads/{storage_domain}/{uuid_tail12}` |
| FileUpload 전달 디렉터리 | `get_webserver_uploaded_files_dir(access_base_url, chat_bot_id, db_name)` | `/FileUpload/{storage_domain}/{uuid_tail12}` |
| FileUpload 전달 경로 | `get_file_download_path(domain, chat_bot_id, db_name)` | `/FileUpload/{storage_domain}/{uuid_tail12}` |
| FileUpload 웹 경로 → 로컬 절대경로 | `fileupload_web_path_to_absolute(web_path)` | `{FILEUPLOAD_ROOT}/{storage_domain}/{uuid_tail12}` |
| LEARN_LIST 공개 URL | `get_file_upload_content_url(access_base_url, domain, chat_bot_id, filename)` | `{access_base_url}/chat/uploaded_files/{uuid_tail12}/{filename}` |
| 공개 URL → 로컬 검증 경로 | `content_url_to_local_storage_path(content, db_name, chat_bot_id)` | `{FILEUPLOAD_ROOT}/{storage_domain}/{uuid_tail12}/{filename}` |

중요한 점은 **DB에 저장되는 `content`는 `/FileUpload/...`가 아니라 `/chat/uploaded_files/...`** 라는 것이다. `/FileUpload/...`는 스토리지 서버의 물리 저장 경로 또는 내부 전달 경로이며, 웹서버가 `/chat/uploaded_files/{uuid_tail12}` 요청을 해당 FileUpload 물리 경로로 연결한다.

## 경로 구성값

- `FILEUPLOAD_ROOT`
  - 미설정 시 기본값은 `/FileUpload`
  - 로컬/스토리지 서버의 실제 최상위 디렉터리
- `FILEUPLOAD_URL_PREFIX`
  - 고정값: `/FileUpload`
  - 내부 전달 경로를 만들 때 사용
- `storage_domain`
  - `get_storage_domain_for_db_name(db_name)`에서 결정
  - 예: `dev_user -> dev.han.kr`, `testchatbot1 -> test.han.kr`, 그 외 `{db_name}.han.kr`
- `uuid_tail12`
  - `chat_bot_id`의 마지막 UUID 조각
  - 예: `204cc79d-10ec-453a-beea-479e6e05af4d -> 479e6e05af4d`
- `access_base_url`
  - 공개 URL의 scheme/host
  - 예: `https://dev.han.kr`

## 예시 변환

입력:

```text
db_name: dev_user
chat_bot_id: 204cc79d-10ec-453a-beea-479e6e05af4d
access_base_url: https://dev.han.kr
filename: AI요약_v.0.1.pptx
```

결과:

```text
작업 다운로드 디렉터리:
downloads/dev.han.kr/479e6e05af4d

작업 다운로드 파일:
downloads/dev.han.kr/479e6e05af4d/AI요약_v.0.1.pptx

FileUpload 물리 저장 파일:
/FileUpload/dev.han.kr/479e6e05af4d/AI요약_v.0.1.pptx

LEARN_LIST content / viewer 전달 URL:
https://dev.han.kr/chat/uploaded_files/479e6e05af4d/AI요약_v.0.1.pptx
```

따라서 `/FileUpload` 바로 아래에 `479e6e05af4d` 같은 bot id 폴더가 생기면 잘못된 경로다. 올바른 물리 저장 위치는 항상 `/FileUpload/{storage_domain}/{uuid_tail12}` 형태다.

## 처리 순서

1. `workflow_dispatch_assembly`가 요청의 `colle`/`content_type`을 확인한다.
2. 파일 모드이면 `create_file_crawl_workflow()`로 파일 워크플로를 만든다.
3. 상세 페이지에서 첨부 메타를 만들고 파일 다운로드 큐에 넣는다.
4. `download.py`가 HTTP 또는 Playwright fallback으로 원본 첨부파일을 로컬 작업 경로에 저장한다.
5. `sync_after_download`가 켜진 파일은 `utils.web_sync.sync_file_to_webserver()`를 통해 `/FileUpload/{domain}/{uuid}`로 전달한다.
6. LEARN_LIST 저장 시 `get_file_upload_content_url()`로 `/chat/uploaded_files/{uuid}/{filename}` 공개 URL을 만들어 `content`에 저장한다.
7. 후속 리포트/검증 로직은 공개 URL을 다시 FileUpload 물리 경로로 역매핑해 실제 파일 존재 여부를 확인한다.

## 실패 판단

- 원본 다운로드가 실패하면 FileUpload 전달도 수행되지 않는다.
- 사이트가 파일 대신 HTML 에러 페이지나 로그인 화면을 반환하면 다운로드 실패로 처리한다.
- DB 커넥션 경고가 있어도 재시도에서 복구되면 파일 저장 플로우의 최종 실패로 보지 않는다.

## 운영 로그 기준

임시 디버그 로그는 제거했고, 운영 중에는 다음 로그를 기준으로 보면 된다.

- 다운로드 성공: `[DOWNLOAD] file written`, `[Download][Worker N] HTTP saved`
- FileUpload 전달: `[WebSync] local FileUpload copy attempt`, `[WebSync] local FileUpload copy ok`
- 저장 스킵: `download_skipped`, `save_skipped`
- HTML 차단: `downloaded payload is HTML, not a document`
