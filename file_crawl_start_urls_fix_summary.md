# 파일 크롤링 start_urls 수정 정리

## 배경

성북구 파일 크롤링에서 입력 URL은 게시판 목록 URL이었지만, DB의 exploration post row는 게시글 상세 URL 형태로 저장되어 있었다.

입력 예:

```text
https://www.sb.go.kr/www/selectBbsNttList.do?key=5741&bbsNo=1
```

DB row 예:

```text
https://www.sb.go.kr/www/selectBbsNttView.do?bbsNo=1&key=5741&nttNo=...
```

기존 fallback은 입력 URL prefix를 그대로 찾았기 때문에 `matched_rows=0`이 되었고, 이후 board static discovery로 빠져 23개만 생성되었다. UI에는 scan/detail 기준으로 11개처럼 보였다.

## 핵심 원인

1. 파일 start_urls 생성이 `learn_list_id exact` 또는 입력 URL prefix에 과하게 의존했다.
2. `selectBbsNttList.do` 입력을 `selectBbsNttView.do` DB row와 연결하지 못했다.
3. `www`/non-`www` host 차이로 일부 DB row가 누락될 수 있었다.
4. `ui_colle` 같은 UI 보조 필드가 백엔드 분기 판단에 섞여 있었다.

## 수정 파일

- `backend/shared/crawl_dispatcher.py`
- `backend/shared/file_crawl_post_urls.py`
- `backend/shared/workflow_dispatch_assembly.py`
- `backend/shared/start_urls_generation.py`

## 주요 변경

### 1. 파일 start_urls는 legacy URL scope 우선

기존 흐름:

```text
learn_list_id exact
-> 없으면 legacy URL scope
-> 없으면 정적탐색
```

변경 후:

```text
legacy URL scope
-> 없으면 learn_list_id exact fallback
-> 없으면 정적탐색
```

`learn_list_id exact`는 exploration row 매핑이 불완전하면 과소수집을 유발하므로 주 경로에서 내렸다.

### 2. 목록 URL에서 상세 URL scope 생성

`backend/shared/file_crawl_post_urls.py`에 list/detail sibling 변환을 추가했다.

지원 예:

```text
selectBbsNttList.do -> selectBbsNttView.do
BD_selectBbsList.do -> BD_selectBbs.do
```

그리고 query 순서가 달라도 매칭되도록 주요 query pair를 `REGEXP` 조건으로 붙인다.

예:

```sql
url LIKE 'https://www.sb.go.kr/www/selectBbsNttView.do?%%'
AND url REGEXP '[?&]key=5741(&|$)'
AND url REGEXP '[?&]bbsNo=1(&|$)'
```

### 3. www/non-www 일반 host normalize

특정 도메인 하드코딩 없이 모든 host에 대해 아래 변형을 같이 검색한다.

```text
www.example.go.kr <-> example.go.kr
```

### 4. ui_colle 의존 제거

파일 DB start_urls branch 판단에서 `ui_colle`를 제거했다.

현재 파일 판단 기준:

```text
colle == file
content_type in file/attach/attachment
_file_crawl_mode == True
file_dashboard == True
start_urls_override_source in file_crawl_post_db/file_crawl_post_db_stream
```

`ui_colle`는 아직 상태 표시/기존 호환 용도로 남아 있지만, 핵심 백엔드 분기 판단에서는 쓰지 않는다.

### 5. 인코딩 깨짐 방지

`_HANGUL_RE` 정규식과 깨진 기본 h3 문자열을 unicode escape로 변경했다.

```python
_HANGUL_RE = re.compile(r"[\uac00-\ud7a3]")
```

## f1_dev 확인 결과

대상:

```text
https://www.sb.go.kr/www/selectBbsNttList.do?key=5741&bbsNo=1
```

list -> view detail scope로 계산 시:

```text
matched: 608
```

기존 문제 로그:

```text
fallback scope result | matched_rows=0
board static discovery fallback resolved count=23
```

수정 후 기대 로그:

```text
[START_URLS] fallback scope result | matched_rows=608
[Dispatch][file_start_urls] legacy URL scope resolved start_urls | ... count=608
[Dispatch] Workflow task created | ... start_urls_count=608
```

## 검증

실행한 검증:

```powershell
python -m py_compile backend\shared\crawl_dispatcher.py backend\shared\file_crawl_post_urls.py backend\shared\workflow_dispatch_assembly.py backend\shared\start_urls_generation.py
python scripts\test_file_crawl_stage_contract.py
```

결과:

```text
py_compile 통과
file crawl stage contract ok
```

## 다음 확인 포인트

다음 크롤링에서 아래가 나오면 정상 경로다.

```text
fallback scope result | matched_rows=608
legacy URL scope resolved start_urls | count=608
start_urls_count=608
```

아래가 다시 나오면 최신 수정이 배포 반영되지 않았거나, 다른 URL 패턴 보정이 추가로 필요하다.

```text
fallback scope result | matched_rows=0
board static discovery fallback resolved count=23
```
