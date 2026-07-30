# 코드베이스 최적화 구조 문서

최종 갱신일: 2026-06-05

## 목적

이 문서는 크롤러 백엔드를 앞으로 최적화하기 위해 현재 구조를 정리한 기준 문서입니다. 실제로 많이 타는 크롤 경로, `start_urls` 준비 과정, Redis/SSE 진행률 발행, DB 접근, 워크플로우 실행, 병목 후보를 중심으로 봅니다.

## 전체 구조

이 애플리케이션은 FastAPI 기반의 비동기 크롤 워크플로우 서버입니다. 대표 실행 경로는 다음과 같습니다.

```text
frontend
  -> backend/shared/crawl_start.py:/backend/session/start
  -> _prepare_crawl()
  -> backend/shared/crawl_dispatcher.py:dispatch_and_schedule_workflow()
  -> backend/shared/start_urls_resolver.py:resolve_start_urls()
     -> backend/shared/start_urls_generation.py
     -> backend/shared/start_urls_preexpand.py, list URL 확장이 필요한 경우
  -> backend/shared/workflow_dispatch_assembly.py:assemble_workflow_after_url_resolve()
  -> backend/shared/workflow_runner.py:run_workflow_task()
  -> workflow.start_workflow()
```

주요 워크플로우 구현체는 다음과 같습니다.

```text
board mode:
  backend/board/board_content_workflow.py:BoardContentWorkflow

file mode:
  backend/file/file_download_workflow.py:FileDownloadWorkflow
  backend/file/integrated_workflow.py:IntegratedWorkflow

generic crawler worker pipeline:
  core/crawler/queues.py
  core/crawler/workers/scan.py
  core/crawler/workers/collection.py
  core/crawler/workers/download.py
  core/crawler/workers/study.py
```

## 런타임 진입점

### FastAPI 앱

`backend/app.py`

- FastAPI 앱을 생성합니다.
- startup/shutdown 핸들러를 등록합니다.
- SSE publish worker 같은 공용 서비스를 시작합니다.
- 레거시 또는 직접 크롤 엔드포인트도 일부 있지만, 현재 주요 세션 시작 경로는 `crawl_start.py`를 통합니다.

### 크롤 세션 시작

`backend/shared/crawl_start.py`

주요 함수:

- `crawl_start()`: `/backend/session/start` API 핸들러입니다.
- `_prepare_crawl()`: dispatcher로 넘기기 전에 크롤 대상 URL을 미리 로드하고 준비합니다.
- `_schedule_and_monitor()`: 준비된 payload를 dispatcher로 넘깁니다.

책임:

- 라우팅 힌트(`colle`, `content_type`)를 정규화합니다.
- DB 이름과 `chat_bot_id`를 확정합니다.
- 프론트가 바로 구독할 수 있도록 Redis 상태를 먼저 초기화합니다.
- 짧은 시간 안에 반복되는 요청을 burst-dedupe로 막습니다.
- 사전 탐색 DB row에서 `start_urls_override`를 만듭니다.
- 날짜 필터, 카테고리/규칙 필터, prefix scope, 중복 제외를 적용합니다.
- `pre_explored_start_urls_count`, `exploration_post_total_count`, `exploration_display_max_count` 같은 표시용 카운터를 설정합니다.

현재 prefix 동작:

- `contents` / `contents_url`은 `_resolve_primary_contents_url()`에서 대표 URL로 해석됩니다.
- scope prefix는 `extract_precise_scope_path_prefix()`로 계산합니다.
- 예시:

```text
https://www.gangnam.go.kr/                -> /
https://www.gangnam.go.kr/board/cardnews  -> /board/cardnews/
```

## Start URL 준비

### 생성 모듈과 resolver 호환 계층

구현 본체:

`backend/shared/start_urls_generation.py`

호환 계층:

`backend/shared/start_urls_resolver.py`

`start_urls_resolver.py`는 기존 import 경로를 유지하기 위한 얇은 재수출 모듈입니다. `crawl_dispatcher.py` 같은 기존 호출부는 그대로 `resolve_start_urls()`를 import하고, 실제 생성/정규화/필터링 로직은 `start_urls_generation.py`가 담당합니다.

우선순위:

```text
1. data["start_urls_override"]
2. header_response.board_list_urls
3. header_response.query_links
4. 빈 리스트
5. dispatcher/file fallback, 해당하는 경우
```

주요 함수:

- `_normalize_start_url_items()`
- `_start_url_identity_key()`
- `_resolve_override_start_urls()`
- `_resolve_board_list_start_urls()`
- `_resolve_query_link_start_urls()`
- `filter_start_urls_by_content_board_id()`
- `resolve_start_urls()`

메모:

- `start_urls_override`가 사전 탐색 DB target을 받는 핵심 경로입니다.
- `sitemap_board`는 `expand_query_links_to_start_urls()`를 통해 list URL을 view URL로 확장할 수 있습니다.
- `board_list_urls`와 `query_links`는 `BOARD_CONTENT_PRESERVE_LIST_URLS_FROM_HEADER` 설정에 따라 list URL을 그대로 유지할 수 있습니다.
- `/bbs/{id}`와 `/board/{id}` 필터는 입력 게시판 id와 맞는 URL만 남기려는 보호 로직입니다.
- detail view URL 중복 제거는 `pgno`, `pageIndex`, `lists`, `keyfield`, `deptField`, `searchField`, `searchWord` 같은 목록/검색 query noise를 제거한 identity 기준으로 수행합니다.
- target `contents`가 list page이면 board id 필터 후 해당 list URL을 detail fetch 후보로 다시 삽입하지 않습니다.

### List pre-expansion

`backend/shared/start_urls_preexpand.py`

주요 함수:

- `expand_query_links_to_start_urls()`

책임:

- header/query/sitemap 입력에서 list URL을 감지합니다.
- workflow 시작 전에 list page를 detail view URL로 미리 확장할 수 있습니다.
- 같은 list page의 페이지 query 변형을 cache key 기준으로 정규화합니다.
- 확장된 detail view URL을 안정적인 detail identity 기준으로 중복 제거합니다.

최근 최적화 방향과 맞춘 동작:

- 강남구청처럼 `/board/B_000001/...` 형태의 board id를 `/bbs/{id}`와 함께 인식합니다.
- `pgno`를 페이지 파라미터로 인식합니다.
- detail identity 계산 시 `pgno`, `lists`, `keyfield`, `deptField` 같은 목록/검색 파라미터를 제거합니다.
- 확장 결과를 한 줄 요약 로그로 남깁니다.

```text
[StartUrlsPreexpand] expanded | input=... lists=... unique_lists=... processed_lists=... expanded_views=... final=... elapsed_ms=...
[StartUrlsPreexpand] no_expansion | input=... lists=... unique_lists=... processed_lists=... output=... elapsed_ms=...
```

### 사전 탐색 DB 스트림

`backend/shared/pre_explored_url.py`

주요 함수:

- `stream_asadal_urls_from_db()`
- `count_exploration_post_urls()`
- `resolve_cate_for_detail_url()`
- `_resolve_preexplored_scope()`

책임:

- `ASADAL_CRAWLING_EXPLORATION`을 읽습니다.
- `type='post'`를 필터링합니다.
- `chat_bot_id`, 활성/중복 제외 조건, 날짜 필터, host/path scope, 카테고리 URL 규칙을 적용합니다.
- `{"url": "...", "type": "post"}` 형태의 dict item을 yield합니다.
- Redis/SSE 표시용 총량을 계산합니다.

최근 최적화 방향과 맞춘 동작:

- `count_exploration_post_urls()`가 `target_domains`, `contents_url`, `scope_path_prefix`를 받습니다.
- Redis/SSE 표시 최대 수량이 실제 `start_urls`에 적용되는 prefix scope와 같은 기준으로 계산될 수 있습니다.

### 파일 크롤 post URL 스트림

`backend/shared/file_crawl_post_urls.py`

주요 함수:

- `load_file_crawl_post_url_strings()`
- `stream_post_urls_for_file_crawl_paged()`
- `count_file_crawl_post_urls_paged()`

책임:

- file mode에서 `ASADAL_CRAWLING_EXPLORATION` 기반 target을 로드합니다.
- 파일 전용 카테고리 URL 규칙과 path scope를 적용합니다.
- `FileDownloadWorkflow`가 사용할 post URL item을 생성합니다.

## Dispatch 및 Workflow 조립

### Dispatcher

`backend/shared/crawl_dispatcher.py`

주요 함수:

- `dispatch_and_schedule_workflow()`

책임:

- 같은 `job_id`가 중복 실행되지 않도록 막습니다.
- 최종 `start_urls`를 확정합니다.
- 필요한 경우 file fallback 또는 DB branch를 적용합니다.
- 마지막 요청 scope 필터와 URL 순서를 적용합니다.
- `enqueue_sse_message()`로 `start_urls_determined` 이벤트를 발행합니다.
- 날짜 범위를 파싱합니다.
- 이후 다음 둘 중 하나로 진행합니다.
  - Redis에 payload를 저장하고 Celery job을 enqueue합니다.
  - 워크플로우를 조립하고 `asyncio` task를 생성합니다.

중요 환경 변수:

```text
CRAWL_WORKFLOW_USE_CELERY
WORKFLOW_AUTO_STOP_DISPATCH_MONITOR
```

### Workflow 조립

`backend/shared/workflow_dispatch_assembly.py`

주요 함수:

- `assemble_workflow_after_url_resolve()`

책임:

- `colle` 값을 board/file/other로 정규화합니다.
- 워크플로우 클래스를 선택합니다.
  - board: `BoardContentWorkflow`
  - file: `FileDownloadWorkflow`
  - fallback: `IntegratedWorkflow`
- 다음 context field를 workflow 객체에 주입합니다.
  - `job_id`
  - `db_name`
  - `chat_bot_id`
  - `start_urls_override_source`
  - `pre_explored_start_urls_count`
  - 표시용 count field
  - 카테고리/중복 처리 mode

## Workflow Runner

`backend/shared/workflow_runner.py`

주요 함수:

- `run_workflow_task()`

책임:

- workflow context를 주입합니다.
- Redis 기반 분산 중복 lock을 획득합니다.
- workflow 실행 slot을 획득합니다.
- 필요한 경우 prestart runtime tab view 해석을 수행합니다.
- Redis stop polling을 시작합니다.
- auto-stop monitor를 시작합니다.
- progress callback을 구성합니다.
- `sse_publish_queue`를 통해 SSE 진행률을 enqueue합니다.
- `workflow.start_workflow()`를 호출합니다.
- workflow background task를 drain합니다.
- terminal status를 발행합니다.

최적화 관점의 hotspot:

- 이 파일은 lock, slot, progress, monitor, prestart URL 해석, workflow 호출, cleanup을 모두 담당합니다. orchestration과 telemetry를 분리하면 복잡도를 줄일 수 있습니다.

## Board Workflow

`backend/board/board_content_workflow.py`

클래스:

- `BoardContentWorkflow`

특징:

- 하나의 큰 클래스가 많은 책임을 가집니다.
  - 상세 URL 탐색
  - 게시글 상세 파싱
  - 날짜 필터링
  - 첨부파일 enqueue
  - metadata/category 처리
  - 중복 repair
  - selector learning
  - post-save queue 및 학습 trigger
- semaphore와 queue를 사용합니다.
  - `_learn_sem`
  - `_detail_pw_fallback_sem`
  - `_slow_detail_background_sem`
  - `_post_save_queue`
  - `_post_save_db_sem`
  - domain별 fetch semaphore
- `start_workflow()`는 exact target mode와 discovery/list-page mode를 모두 처리합니다.

주요 최적화 리스크:

- 클래스가 크고 책임이 섞여 있어 변경 영향 범위를 예측하기 어렵습니다.
- 많은 비동기 background task가 workflow 본체보다 오래 살아남을 수 있으므로 drain 정책이 중요합니다.
- 비슷한 의미의 counter가 여러 개 있습니다. 예: scan, actual start URL, selected URL, save, study, file enqueue.
- scope filtering이 여러 layer에 분산되어 있습니다.

Detail fetch timeout 원칙:

- `detail_prefetch_fetch`는 선별 단계의 빠른 후보 확인용입니다. 비거나 느린 상세 URL은 오래 붙잡지 말고 background/fallback 경로로 넘깁니다.
- `BOARD_DETAIL_PREFETCH_STATIC_FETCH_TIMEOUT_SEC`는 prefetch 정적 fetch에만 적용합니다.
- `BOARD_DETAIL_STATIC_FETCH_TIMEOUT_SEC`는 실제 detail fetch 또는 더 깊은 fetch 경로의 기본값으로 유지합니다.
- `_fetch_html_static()`의 최소 timeout은 `BOARD_DETAIL_STATIC_FETCH_MIN_TIMEOUT_SEC`로 조정할 수 있습니다. prefetch timeout을 낮춰도 내부 최소 timeout이 더 크면 병목이 다시 생깁니다.

## File Workflow

`backend/file/file_download_workflow.py`

클래스:

- `FileDownloadWorkflow`

특징:

- mixin과 `BoardContentWorkflow`를 통해 board 동작을 상속합니다.
- 파일 전용 URL pattern cache, 첨부 파싱, 다운로드 enqueue, queue drain을 추가합니다.
- scan, collection, save, study batch에 대해 queue join을 사용합니다.

최적화 리스크:

- board workflow 상속 때문에 file mode가 board 동작과 강하게 결합되어 있습니다.
- queue flush/join 순서가 처리량과 완료 판정에 민감합니다.
- 직접 첨부 mode와 상세 페이지 재조회 mode는 별도 실행 경로로 다루는 편이 좋습니다.

## Integrated Workflow 및 Core Worker Pipeline

`backend/file/integrated_workflow.py`

클래스:

- `IntegratedWorkflow`

파이프라인:

```text
scan_queue
  -> collection_batch_queue
  -> save_batch_queue
  -> study_batch_queue
  -> progress_queue
```

핵심 모듈:

- `core/crawler/queues.py`
- `core/crawler/batch_queue.py`
- `core/crawler/workers/scan.py`
- `core/crawler/workers/collection.py`
- `core/crawler/workers/download.py`
- `core/crawler/workers/study.py`

최적화 리스크:

- queue backpressure와 batch size가 전체 처리량을 결정합니다.
- 각 stage가 너무 자주 progress event를 발행하면 Redis/SSE 부하가 커질 수 있습니다.
- download concurrency와 study concurrency는 서로 독립적으로 튜닝해야 합니다.

### Legacy URL Edu Pipeline

`edu/url_edu.py`

역할:

- 게시판/파일 공용 URL 수집 결과를 `collection_queue -> change_detection_queue -> save_worker`로 넘깁니다.
- 기존 이름은 변경감지 워커지만, board/file 경로에서는 content hash 변경감지를 적용하지 않고 URL 단위 저장/학습 파이프라인으로 사용합니다.

최근 최적화 방향과 맞춘 동작:

- `batch_check_url_changes()`는 content hash 계산, `content_metadata` 조회, 해시 중복 DB 조회를 수행하지 않고 배치 항목을 통과시킵니다.
- `pre_parse_for_hash` 기반 선 파싱을 제거했습니다. 사전 탐색 DB에서 가져온 URL을 해시 비교 목적으로 URL별 재파싱하지 않습니다.
- 일반 수집 루프의 URL별 `get_url_by_content_hash()` / `get_url_metadata()` 호출을 제거하고, 파싱된 결과를 바로 collection queue에 전달합니다.
- `extract_content_from_url()`의 기본 변경감지 옵션은 꺼져 있습니다. 명시적으로 켜더라도 `should_check_url_changes()`에서 공용 정책상 비활성 처리합니다.
- 저장 전 메타데이터 사전 계산에서 `content_hash` 생성과 저장을 제거했습니다. 기존 LEARN_LIST 중복 스킵은 URL 기준으로 유지합니다.

남은 최적화 후보:

- `_process_save_batch()`는 배치를 만들지만 내부에서 URL별 `process_single_crawled_url()` task를 생성합니다. 저장/LEARN_LIST/PG upsert 커넥션 사용을 배치 context로 묶을 수 있는지 별도 리팩터가 필요합니다.
- 변경감지 명칭(`change_detection_queue`, `change_detection_worker`)은 실제 역할과 달라졌으므로, 추후 `collection_filter_queue` 또는 `save_dispatch_queue` 같은 이름으로 정리하는 편이 좋습니다.

## Redis, SSE, Progress

### Redis SSE service

`backend/shared/redis_sse_service.py`

주요 함수:

- `send_message_to_redis_sse()`

책임:

- 다양한 progress message를 `RedisSSEPayload`로 변환합니다.
- Redis SSE/PubSub에 publish합니다.
- rate limit과 pending request 처리를 적용합니다.

### SSE publish queue

`backend/shared/sse_publish_queue.py`

주요 함수:

- `enqueue_sse_message()`

책임:

- `job_id`별 최신 message를 coalesce합니다.
- monotonic counter가 뒤로 줄어들지 않도록 유지합니다.
- terminal/stop message를 우선 처리합니다.
- DB crawling log update를 throttle합니다.
- background worker에서 Redis event를 publish합니다.

최적화 메모:

- coalescing 구조는 좋으므로 유지하는 편이 좋습니다.
- 여러 모듈이 같은 count key를 쓰기 때문에 counter merge 규칙은 취약합니다.
- terminal message가 나중의 running message에 덮이지 않도록 보호 로직이 있습니다.

### Redis state write batch

`backend/shared/redis_sse_service.py`

역할:

- Pub/Sub progress 발행과 별도로 `crawl:{account}:{job_id}:state` hash를 최신 상태 캐시로 저장합니다.
- running progress는 state write batch로 coalesce되어 Redis pipeline으로 저장됩니다.

현재 최적화 동작:

- state write timeout은 Redis 부하와 event loop 지연을 고려해 publish timeout보다 너무 짧게 두지 않습니다.
- batch write 실패 시 pending에서 제거된 상태가 유실되지 않도록 제한된 횟수만 재큐잉합니다.
- 재큐잉 시 이미 같은 state key의 최신 pending message가 있으면 최신 항목을 우선하고 실패 항목은 되살리지 않습니다.
- `publish_sse_event`가 rate limit에 걸린 경우에는 최신 pending publish로 coalesce합니다. 기본적으로 throttle 중에는 `update_state_only()`를 즉시 await하지 않고, 실제 pending publish 시점의 state write로 수렴시킵니다.

관련 환경 변수:

```text
SSE_RATE_LIMIT_INTERVAL
SSE_REDIS_STATE_UPDATE_ON_THROTTLE
SSE_REDIS_STATE_OP_TIMEOUT_SECONDS
SSE_REDIS_GET_CLIENT_TIMEOUT_SECONDS
SSE_REDIS_STATE_BATCH_ENABLED
SSE_REDIS_STATE_BATCH_SIZE
SSE_REDIS_STATE_BATCH_WAIT_MS
SSE_REDIS_STATE_WRITE_RETRY_LIMIT
SSE_REDIS_STATE_WRITE_RETRY_DELAY_MS
```

## DB 접근 구조

주요 DB 모듈:

```text
db/maria_operations.py
db/mysql_db_config.py
db/mariadb_save_update.py
db/crawl_db_manager.py
backend/shared/pre_explored_url.py
backend/shared/file_crawl_post_urls.py
```

### DB pool 구분 기준

현재 pool의 실제 소유 단위는 `job_id`가 아니라 `dbname`입니다.

```text
logical dbname
  -> rdbms_router.resolve_rdbms_engine()
  -> MySQLPool 또는 MariaDBPool
  -> dbname별 shared pool
```

DB별 구분:

- `chatty`, `naraone`은 기본적으로 MySQL pool을 사용합니다.
- 그 외 logical dbname은 MariaDB pool을 사용합니다.
- pool map key는 `dbname`입니다. 즉 같은 DB를 쓰는 여러 job은 같은 pool을 공유합니다.
- pool은 idle cleanup과 shutdown cleanup 대상입니다.

Job별 구분:

- `job_id`별 pool은 만들지 않습니다.
- `job_id`는 workflow slot, duplicate lock, Redis/SSE 진행률, crawling log row, counter throttle key를 구분하는 데 사용합니다.
- MariaDB 쪽에는 `MARIADB_DYNAMIC_JOB_SHARE`가 있어 active workflow 수를 기준으로 shared pool 사용량을 조절합니다.
- 이 장치는 job별 전용 pool이 아니라, `dbname`별 shared pool 위에서 active job 수에 따른 acquire throttling을 거는 방식입니다.

권장 원칙:

- DB별 pool은 유지합니다. job마다 pool을 만들면 동시 작업 수만큼 pool이 늘어 DB 연결 수가 급증합니다.
- job별 격리가 필요하면 pool 분리가 아니라 job별 semaphore, queue backpressure, 또는 MariaDB job-share cap으로 제어합니다.
- 배치 최적화는 “job별 pool 생성”이 아니라 같은 batch 안에서 query 수와 acquire/release 횟수를 줄이는 방향으로 진행합니다.

현재 패턴:

- 많은 query helper가 SQL condition 문자열을 직접 조립합니다.
- `maria_select_data()`는 `SELECT {columns} FROM {table} WHERE {condition}` 형태의 단순 wrapper입니다.
- 일부 경로는 raw SQL을 쓰고, 일부는 helper wrapper를 씁니다.
- 크롤 탐색 URL read는 매우 커질 수 있습니다.

최적화 리스크:

- SQL 문자열 직접 조립은 재사용성과 정확성을 떨어뜨립니다.
- count query와 select query의 필터 조건이 항상 맞아야 합니다.
- prefix/domain filtering은 `ASADAL_CRAWLING_EXPLORATION.url`에 대한 index 지원이 중요합니다.
- 카테고리/규칙 parsing이 반복되면 부하가 커질 수 있습니다.

## 현재 Hot Path

일반적인 board/file 크롤 요청:

```text
1. /backend/session/start
2. job metadata cache 및 Redis state 초기화
3. _prepare_crawl()
4. DB에서 pre-explored post URL stream/load
5. 필요 시 learn-list duplicate exclusion
6. 표시용 scoped exploration post count 계산
7. dispatch_and_schedule_workflow()
8. resolve_start_urls()
9. 최종 scope/order filtering
10. start_urls_determined publish
11. workflow 조립
12. run_workflow_task()
13. workflow.start_workflow()
14. progress_callback -> enqueue_sse_message()
15. queue worker -> send_message_to_redis_sse()
16. DB crawling log update 및 terminal event
```

## 병목 후보

### P0: Start URL loading 및 counting

증상:

- `ASADAL_CRAWLING_EXPLORATION` scan이 커질 수 있습니다.
- 표시 count와 실제 selected count가 필터 차이로 어긋날 수 있습니다.
- 카테고리 규칙 필터와 중복 제외가 많은 URL을 반복 처리할 수 있습니다.

최적화 방향:

- exploration URL scope/filter/count/select를 위한 공통 query builder를 만듭니다.
- count와 select가 동일한 filter 정의를 사용하게 합니다.
- index를 추가하거나 확인합니다.
  - `(chat_bot_id, type, is_active, merge_status)`
  - 가능하다면 URL prefix 전략
  - exploration date filter에서 쓰는 날짜 컬럼
- 대용량 결과 경로는 paged streaming을 우선합니다.

### P0: Workflow class 크기와 책임 혼재

증상:

- `BoardContentWorkflow`가 discovery, parsing, saving, learning, file enqueue, repair, telemetry를 모두 처리합니다.
- 변경할 때 멀리 떨어진 side effect까지 이해해야 하는 경우가 많습니다.

최적화 방향:

- phase별 service를 추출합니다.
  - target enqueue
  - detail discovery
  - detail parse
  - save/update
  - learning trigger
  - attachment enqueue
- `BoardContentWorkflow`는 coordinator 역할만 남기는 방향이 좋습니다.

### P1: Counter 일관성

증상:

- 비슷한 개념을 나타내는 key가 여러 개 있습니다.
  - `scan_count`
  - `total_count`
  - `actual_scan_count`
  - `actual_start_urls_count`
  - `selected_start_urls_count`
  - `pre_explored_start_urls_count`
  - `exploration_post_total_count`
  - `exploration_display_max_count`

최적화 방향:

- 내부 progress model을 하나로 정의합니다.
  - `display_total`
  - `selected_total`
  - `actual_started`
  - `completed`
  - `saved`
  - `studied`
- legacy key mapping은 Redis/SSE boundary에서만 수행합니다.

### P1: 비동기 task lifecycle

증상:

- workflow, runner, metadata, learning, SSE 곳곳에서 `asyncio.create_task()`를 호출합니다.
- 일부 task는 의도적으로 background로 돌고 이후 drain됩니다.

최적화 방향:

- job별 task 등록을 중앙화합니다.
- 모든 task에 owner, cancellation policy, drain policy를 부여합니다.
- Python 버전이 허용하면 structured task group을 도입합니다.

### P1: Redis/SSE publish noise

증상:

- 여러 모듈이 progress를 publish할 수 있습니다.
- queue coalescing이 부하를 줄여주지만 counter merge 규칙은 복잡합니다.

최적화 방향:

- coalescing worker는 유지합니다.
- counter normalization을 한 함수로 모읍니다.
- phase 전환은 즉시 publish하고, counter-only update는 throttle합니다.

### P2: 사이트별 특수 로직

증상:

- `backend/board` 아래에 사이트별 board 모듈이 많습니다.
- 일부 generic code 안에도 site trace나 special case가 들어 있습니다.

최적화 방향:

- 사이트별 adapter는 일관된 interface 뒤에 둡니다.
- special trace/config는 가능하면 site config 파일로 이동합니다.

## 최적화 로드맵

### Phase 1: 관측성과 invariant 정리

- job마다 compact trace를 추가합니다.
  - 입력 URL
  - resolved prefix
  - select SQL scope summary
  - count SQL scope summary
  - selected count
  - display count
  - workflow class
- display count와 selected count가 예상치 않게 다를 때 assertion 수준 로그를 남깁니다.
- counter 의미를 한 곳에 문서화합니다.

### Phase 2: 공통 exploration query builder

다음과 같은 모듈을 만들 수 있습니다.

```text
backend/shared/exploration_query.py
```

제안 책임:

- scope identity 생성
- path prefix condition 생성
- date condition 생성
- type/status condition 생성
- 다음 두 query를 함께 생성
  - count query
  - select query

목표:

- `stream_asadal_urls_from_db()`
- `count_exploration_post_urls()`
- `load_file_crawl_post_url_strings()`
- dispatcher file branch

위 코드들이 비슷한 SQL 필터 로직을 각자 다시 만들지 않게 합니다.

### Phase 3: Workflow 분해

`BoardContentWorkflow`에서 다음 요소를 추출합니다.

- `StartUrlEnqueuer`
- `DetailDiscoveryService`
- `DetailParseService`
- `PostSavePipeline`
- `AttachmentBridge`
- `WorkflowCounter`

한 번에 크게 나누지 말고 phase 하나씩 점진적으로 분리합니다.

### Phase 4: Progress model 정리

- 내부용 `CrawlProgressSnapshot`을 도입합니다.
- legacy Redis/SSE payload 변환은 adapter 한 곳에서 수행합니다.
- `_MONOTONIC_COUNT_KEYS`는 유지하되, 해당 key를 직접 쓰는 producer 수를 줄입니다.

### Phase 5: DB/index 튜닝

query builder가 안정화된 뒤 진행합니다.

- query condition 형태별 slow query log를 수집합니다.
- 실제 query plan에 맞춰 index를 추가/조정합니다.
- full URL의 `LIKE` 대신 host/path 정규화 컬럼으로 URL prefix filtering을 지원할지 검토합니다.

## 바로 확인할 항목

1. Gangnam cardnews 요청을 한 번 실행하고 다음을 확인합니다.

```text
scope_path_prefix=/board/cardnews/
start_urls_count == actual_start_urls_count
exploration_display_max_count is cardnews-scoped
```

2. Gangnam main board detail/list URL 정규화를 확인합니다.

```text
https://www.gangnam.go.kr/board/B_000001/1076483/view.do?mid=ID05_040101&pgno=2&keyfield=bdm_main_title&lists=10&deptField=BDM_DEPT_ID
-> detail identity:
https://www.gangnam.go.kr/board/B_000001/1076483/view.do?mid=ID05_040101

list page:
/board/B_000001/list.do?...pgno=3
-> board id: B_000001
-> page param: pgno
```

3. DB timing을 비교합니다.

```text
prepare_load_ms
prepare_dedupe_ms
dispatch_ms
runner_start_workflow_ms
```

4. `ASADAL_CRAWLING_EXPLORATION.url LIKE '.../board/cardnews/%'`가 index를 효과적으로 쓰는지 확인합니다.

5. `exploration_query.py`를 먼저 만들지 결정합니다. 이 작업은 workflow 동작을 크게 바꾸지 않으면서 중복 필터 로직을 줄이는 가장 안전한 최적화 기반입니다.

## 진행 기록

### 2026-06-06: Phase 2 일부 진행

- `backend/shared/exploration_query.py`의 공통 조건 빌더를 게시판 `count_exploration_post_urls()`와 `stream_asadal_urls_from_db()`에 적용했습니다.
- 파일 크롤링의 non-paged/paged exploration URL 조회 조건이 모두 `build_exploration_conditions()`를 사용하도록 맞췄습니다.
- `merge_status`, `is_active` 컬럼이 없는 환경을 위한 legacy fallback condition은 유지했습니다.
- 다음 작업은 `pre_explored_url.py` 안의 CATEGORY rule SQL 조립과 relaxed fallback 흐름을 더 작게 분리하거나, `BoardContentWorkflow`에서 counter/attachment/save phase 중 하나를 독립 서비스로 빼는 단계입니다.

### 2026-06-06: LEARN_LIST duplicate lookup 병목 완화

- GM 계약 상세 URL처럼 `ctrtAcctBookMngNo`가 있는 강한 식별자 URL은 기본적으로 느린 exact equality 조회를 건너뛰고 후보 term LIKE 조회로 이동하도록 조정했습니다.
- `q_currPage`, `q_rowPerPage`, `q_optionalYn` 같은 목록/옵션 query는 dedup canonical URL에서 제거되도록 정리했습니다.
- 관련 환경 변수:
  - `BOARD_DEDUP_CONTRACT_SKIP_EXACT=1`: 계약 상세 URL exact lookup 생략.
  - `BOARD_DEDUP_STRONG_KEY_SKIP_UNINDEXED_EXACT=1`: strong identity가 있고 lookup index가 없으면 exact lookup 생략.

### 2026-06-06: 용인 BBS prefetch timeout 완화

- `yongin.go.kr/user/bbs/BD_selectBbs.do` 정적 prefetch가 내부 최소 timeout 5초로 끌려 올라가던 문제를 줄였습니다.
- prefetch 경로에서는 `BOARD_YONGIN_PREFETCH_STATIC_FETCH_TIMEOUT_SEC=2.5` 기본값을 적용하고, guard timeout도 `fetch_timeout + 1초`로 제한합니다.
- 본 detail fetch처럼 더 긴 timeout을 명시하는 경로는 기존 5초 이상 동작을 유지하도록, 용인 전용 cap은 짧은 prefetch timeout에서만 적용합니다.

### 2026-06-06: Detail fetch policy 1차 분리

- `backend/board/detail_fetch_policy.py`를 추가해 GM/용인 정적 fetch timeout 정책을 `BoardContentWorkflow` 본체 밖으로 분리했습니다.
- `BoardContentWorkflow`는 prefetch/static fetch에서 정책 함수만 호출하도록 바꿨습니다.
- 현재 분리 범위:
  - GM fast static fetch URL 판별
  - GM static timeout
  - 용인 일반 BBS fast prefetch URL 판별
  - prefetch static timeout 계산
  - static fetch 내부 effective timeout 계산
  - prefetch guard timeout cap 여부
- 다음 단계는 Playwright fallback 정책과 slow background queue 판단을 같은 policy/service 계층으로 옮기는 것입니다.
