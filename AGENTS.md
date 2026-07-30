# AGENTS.md

이 문서는 AI 에이전트가 이 프로젝트에서 기능 추가, 버그 수정, 리팩토링을 수행할 때 따라야 할 작업 규칙이다. 규칙은 현재 코드와 문서에서 확인되는 패턴을 우선 반영한다. 파일이나 폴더 이름이 `#` 또는 `##`로 시작하면 백업으로 간주하고 기본 분석/수정 대상에서 제외한다.

## 프로젝트 개요

- 이 프로젝트의 현재 개선 대상은 순수 게시판 크롤링이다. 게시글 본문 수집, LEARN_LIST 저장, PG chunk 학습, summary/breadcrumb API 연동, Redis/SSE 진행률 발행, RDBMS 저장 흐름을 우선한다.
- 파일 크롤링/첨부파일 학습 기능은 레거시 코드가 남아 있어도 신규 기능 추가나 리팩토링의 기본 대상에서 제외한다. 사용자가 명시하지 않는 한 `backend/file/`, 파일 다운로드/문서 파싱 파이프라인을 확장하지 않는다.
- 운영 진입점은 FastAPI 서버 `backend/app.py`와 Celery 워커 `run_celery_worker.py`, `run_worker.py`이다.
- 주요 요청 경로는 `backend/app.py` -> `backend/router.py` -> `backend/shared/crawl_start.py` -> `backend/shared/crawl_dispatcher.py` -> `backend/shared/workflow_dispatch_assembly.py` -> `backend/shared/workflow_runner.py` -> board workflow이다.
- 게시판 크롤링은 `backend/board/`, 공용 크롤러 워커는 `core/crawler/`, DB 접근은 `db/`, 학습/요약 연동은 `backend/shared/learning_service.py`, `backend/shared/batch_embedding_scheduler.py`, 운영 도구와 대시보드는 `tools/`에 둔다.

근거: `backend/app.py`, `backend/router.py`, `backend/shared/crawl_start.py`, `backend/shared/crawl_dispatcher.py`, `backend/shared/workflow_runner.py`, `docs/codebase_optimization_structure.md`, `docs/board_crawling_pipeline_refactor_review.md`

## 사용 언어와 주요 라이브러리

- 주 언어는 Python이다.
- 웹/API는 FastAPI, Starlette, Uvicorn을 사용한다.
- 비동기 HTTP와 크롤링에는 `aiohttp`, `httpx`, `requests`, `playwright`, `playwright-stealth`, `beautifulsoup4`를 사용한다.
- 백그라운드 작업은 Celery와 Redis를 사용한다.
- DB는 MariaDB/MySQL/PostgreSQL 계열을 모두 다루며, `asyncmy`, `aiomysql`, `PyMySQL`, `asyncpg`, SQLAlchemy, pgvector, pymilvus가 의존성에 있다.
- 문서 처리와 학습에는 OpenAI/LangChain 계열, docling, unstructured, pandas, openpyxl, python-docx, python-pptx, pyhwp, Pillow, PDF 관련 라이브러리를 사용한다.

근거: `requirements.txt`, `backend/src/tasks/celery_app.py`, `db/`, `edu/`, `services/milvus_service.py`

## 구조와 아키텍처 규칙

- 새 API 라우트는 가능하면 도메인별 router 모듈에 추가하고, `backend/router.py` 또는 `tools/*/integration.py`에서 include하는 기존 패턴을 따른다.
- board/file 모드 분기는 `backend/shared/workflow_dispatch_assembly.py`와 `backend/board/board_crawl_module.py`, `backend/file/file_crawl_module.py`의 경계를 우선 사용한다.
- 기존 대형 workflow에 새 책임을 계속 추가하기보다, 이미 생긴 계약 모듈이나 공용 helper에 붙인다. 특히 progress는 `backend/shared/progress_contract.py`, 요청 모드 정규화는 `backend/shared/crawl_request_config.py`, 파일 크롤 단계명은 `backend/file/file_crawl_stage_contract.py`를 우선 사용한다.
- `backend/board/board_content_workflow.py`, `backend/shared/crawl_start.py`, `backend/shared/crawl_dispatcher.py`, `backend/shared/workflow_runner.py`는 큰 파일이며 런타임 의미가 많이 얽혀 있다. 수정 시 관련 상태, SSE, DB 업데이트, stop/cancel 경로를 함께 확인한다.

근거: `backend/router.py`, `tools/board_gap_dashboard/integration.py`, `backend/shared/workflow_dispatch_assembly.py`, `backend/shared/progress_contract.py`, `backend/shared/crawl_request_config.py`, `backend/file/file_crawl_stage_contract.py`, `docs/board_crawling_pipeline_refactor_review.md`

## 게시판 크롤링 전용 규칙

- 현재 기능 추가/리팩토링은 파일 크롤링을 제외하고 게시판 상세 URL 기반 수집 -> 저장 -> 학습 흐름을 우선한다.
- `summary`와 `breadcrumb` 연동은 필수 API로 본다. 실패 시 전체 크롤링을 무조건 중단하기보다 기존 로그/상태 처리 패턴을 따라 원인을 남긴다.
- PG chunk가 이미 존재하면 중복 학습을 피하고, chunk가 없을 때만 학습 로직으로 연결한다.
- 본문 파싱은 제목오류, 본문영역 오판, 로그인 필요/비게시판 URL 유입, 동적 fallback 남발을 방지하는 기존 guard를 우선 사용한다.
- `jongno.go.kr/portal/app/integrateApplicant/view.do`처럼 로그인/신청 상세 성격의 비게시판 URL은 앞단에서 제외하는 패턴을 유지한다.
- static fetch 성공률을 먼저 높이고, Playwright fallback은 느린 보조 경로로 제한한다. 특정 지자체만 timeout을 크게 늘리는 방식은 기본 전략으로 삼지 않는다.
- 게시판 자동분류는 기본 유지한다. `BOARD_AUTO_CATEGORY=1`, `BOARD_CONTENT_ENABLE_POST_JOB_CATE_UPDATE=1` 흐름을 전제로 하며, pure crawling 모드에서도 post-job cate 보정은 꺼지지 않아야 한다.
- 분류 규칙 변경 시 `backend/shared/basic_crawling_flow.py`, `backend/board/board_content_workflow.py`, `db/mariadb_save_update.py`의 `update_learn_list_cates_post_job()` 전달값과 함께 확인한다.

근거: `backend/shared/basic_crawling_flow.py`, `backend/board/board_content_workflow.py`, `db/mariadb_save_update.py`, `backend/shared/batch_embedding_scheduler.py`, `.env`

## 정보 관리 원칙

- 중요하거나 여러 경로에서 반복 사용하는 값, 규칙, 상태명, URL 패턴, 단계명, 설정 기본값은 기존 contract, helper, config 등 단일 관리 지점에 둔다.
- 소비 모듈은 단일 관리 지점을 import 또는 호출하여 사용하고, 동일 값을 여러 파일에 상수나 문자열로 복제하지 않는다.
- 단일 관리 지점을 변경할 때는 해당 값을 사용하는 모든 경로와 대응 검증 스크립트를 함께 확인한다.
- 예외적으로 임시 복제가 필요하면 적용 사유와 범위를 남기고, 이를 상시 구조로 유지하지 않는다.
## 코딩 컨벤션

- 기존 파일의 스타일을 우선한다. 전역 logger는 `logging.getLogger("패키지.모듈")` 또는 `LoggerSingleton.get_logger(...)` 패턴을 따른다.
- 비동기 흐름은 `async def`, `await`, `asyncio.create_task`, `asyncio.Semaphore`, `asyncio.gather(..., return_exceptions=True)` 등 기존 패턴을 사용한다.
- 환경변수 파싱은 로컬에서 새로 흩뿌리지 말고, 이미 있는 `_env_bool`, `_env_int`, `parse_bool`, settings/helper 함수를 재사용한다.
- payload dict는 아직 레거시 계약이다. 새 구조를 만들더라도 기존 키를 깨지 말고 adapter 방식으로 병행한다.
- 한글 주석/문자열이 이미 많은 프로젝트다. 새 파일은 UTF-8로 작성하고, 기존 mojibake 주석을 무관한 리팩토링으로 대량 정리하지 않는다.

근거: `utils/logging_util.py`, `backend/router.py`, `backend/shared/crawl_request_config.py`, `config/settings.py`, `backend/shared/workflow_runner.py`

## 로깅 규칙

- 로그에는 `job_id`, `db_name`, `chat_bot_id`, URL 또는 stage 등 추적에 필요한 문맥을 포함한다.
- Operational logging should not emit per-item success logs by default; keep success counters in memory and emit one final job summary at completion.
- ERROR/WARNING/failure logs must include the visited board detail page URL. Include `post_url`, `source_page`, `process_url`, `file_url`, `stage`, `reason`, and `error` when available.
- 예외를 삼켜도 되는 복구/정리 경로는 `logger.debug` 또는 `logger.warning`으로 이유를 남긴다. 요청 실패, workflow 실패, 데이터 손상 가능성이 있으면 `logger.exception`을 사용한다.
- Redis/SSE, DB, 다운로드, 파일명 디버깅처럼 기존에 prefix가 있는 로그는 prefix를 유지한다. 예: `[RunWorkflowTask]`, `[RedisSSE]`, `[LEARN_LIST]`, `[BoardGapDashboard]`.
- 매우 빈번한 진행률 로그는 rate limit, slow log, coalescing 설정을 존중한다.

근거: `backend/shared/workflow_runner.py`, `backend/shared/redis_sse_service.py`, `backend/shared/sse_publish_queue.py`, `db/mariadb_save_update.py`, `backend/app.py`, `utils/logging_util.py`

## 예외 처리 규칙

- 크롤링/저장/학습의 핵심 실패는 조용히 무시하지 말고 상태 업데이트, terminal SSE, DB 로그 업데이트 경로까지 확인한다.
- 선택적 통합이나 디버그성 기능은 실패해도 전체 서버 기동을 막지 않는 기존 패턴을 따른다. 예: dashboard route include 실패, breadcrumb DB binding 실패.
- cleanup/shutdown 경로에서는 timeout과 best-effort cleanup을 유지하되, 실패 로그는 남긴다.
- `except Exception: pass`를 새로 추가해야 한다면, 왜 무시 가능한지 매우 좁은 범위에서만 사용한다.

근거: `backend/app.py`, `backend/router.py`, `backend/shared/workflow_runner.py`, `backend/shared/stop_service.py`, `backend/src/tasks/crawl_workflow_tasks.py`

## DB 사용 규칙

- MariaDB 쿼리는 기본적으로 `db.maria_operations.maria_execute_query`, `maria_insert_data`, `maria_upsert_then_last_insert_id` 등 기존 helper를 경유한다.
- DB 작업에는 `dbname` 전달을 유지하고, query 기록/slow log/retry/pool 동작을 우회하지 않는다.
- pool, retry, timeout, dynamic job share 같은 연결 정책은 `db/mariadb_pool.py`, `db/mysql_pool.py`, `db/rdbms_router.py`, `config/settings.py`의 기존 설정을 따른다.
- `LEARN_LIST`, `ASADAL_CRAWLING_EXPLORATION`, Redis/SSE 상태 필드는 외부/UI/운영 계약과 연결되어 있으므로 컬럼명, 의미, terminal status, progress count를 임의 변경하지 않는다. 개선이 필요하면 adapter나 helper를 먼저 추가한다.
- FastAPI startup에서 DB 연결/작업을 새로 넣지 않는다. 기존 코드가 startup DB 작업 금지를 명시한다.
- DB 개선 작업은 가능하지만 schema/runtime 계약 변경은 관련 저장, 조회, dashboard, SSE, test script를 같이 갱신한다.
- 현재 작업 범위에서는 운영 DB에 컬럼, 인덱스, 테이블을 새로 만들지 않는다. `ALTER TABLE`, `CREATE INDEX`, runtime index creation 같은 변경은 사용자 명시 승인 없이는 추가하지 않는다.
- DB pool은 프로세스/DB별 pool 1개 안에서 여러 connection을 운용하는 구조로 본다. `DB_POOL_MAX`, `MARIADB_POOL_MAX`, `MARIADB_POOL_MIN` 값을 올리기 전에 DB 여유와 앞단 큐/worker 수를 같이 줄이는 방안을 먼저 검토한다.
- DB write fan-out은 `backend/shared/db_write_queue.py`의 `DB_WRITE_QUEUE_WORKERS`, `DB_WRITE_LOG_QUEUE_WORKERS` 설정을 따른다. 로그성 write가 본 작업 connection을 잠식하지 않도록 보수적으로 유지한다.
- 단순 반복 조회/검증은 가능한 경우 메모리 TTL cache나 기존 URL row cache를 우선 사용한다. `status=Y` 후속 검증 SELECT는 `BOARD_LEARN_LIST_STATUS_VERIFY` 설정을 따른다.
- `crawling_log` 갱신 timeout은 보조 로그 실패로 취급한다. 전체 게시판 크롤링 실패로 번지지 않도록 기존 timeout suppress 패턴을 유지한다.

근거: `db/maria_operations.py`, `db/mariadb_pool.py`, `db/mariadb_save_update.py`, `db/db_redis.py`, `backend/app.py`, `tools/board_gap_dashboard/service.py`, `backend/shared/pre_explored_url.py`

## 비동기와 Celery 규칙

- 서버 프로세스와 worker 프로세스의 import path 보정, Windows event loop 정책, Celery nodename/queue 처리 패턴을 유지한다.
- 긴 작업은 FastAPI 요청 안에서 직접 오래 붙잡지 말고 기존 dispatcher, BackgroundTasks, Celery, workflow runner 경로를 사용한다.
- `asyncio.create_task()`로 분리한 작업은 가능하면 이름을 붙이고, done callback 또는 cleanup 경로로 예외가 유실되지 않게 한다.
- 파일/첨부 파이프라인은 download, save, learn, queue join, final SSE 사이의 순서가 중요하다. 완료 상태를 먼저 발행하지 않도록 기존 finalize 대기 훅을 확인한다.
- 동시성은 semaphore, worker count, env 설정을 통해 제한한다. 임의 무제한 gather나 무제한 브라우저/HTTP 요청을 만들지 않는다.

근거: `run_celery_worker.py`, `run_worker.py`, `backend/src/tasks/celery_app.py`, `backend/shared/crawl_dispatcher.py`, `backend/shared/workflow_runner.py`, `core/crawler/global_pool.py`, `tools/board_gap_dashboard/extract_service.py`

## 성능 최적화 규칙

- 대량 URL/DB 처리에서는 streaming, batching, TTL cache, rate limit, Redis state batch, query timeout 같은 기존 장치를 우선 사용한다.
- Start URL 로딩/counting, `ASADAL_CRAWLING_EXPLORATION` scan, LEARN_LIST duplicate lookup, Redis/SSE publish noise는 이미 병목 후보로 문서화되어 있으므로 변경 시 성능 영향을 기록한다.
- HTTP 요청은 기본 proxy 환경을 신뢰하지 않는 `utils/http_client.py` helper 패턴을 우선 고려한다.
- 첨부파일 후보 수집 단계는 recall first이다. 강한 필터링은 다운로드/응답 검증 단계에 둔다.
- progress event를 너무 자주 발행하지 말고 `backend/shared/sse_publish_queue.py`, `backend/shared/redis_sse_service.py`의 throttle/coalescing 설정을 따른다.
- DB 부하를 줄일 때는 connection max만 조정하지 말고 detail concurrency, learn concurrency, save worker, DBWriteQueue worker, log queue worker를 함께 본다. 현재 보수 기준은 `MARIADB_POOL_MAX=16`, default DB write worker 2, log DB write worker 1이다.

근거: `docs/codebase_optimization_structure.md`, `backend/shared/start_urls_preexpand.py`, `backend/shared/pre_explored_url.py`, `backend/shared/sse_publish_queue.py`, `backend/shared/redis_sse_service.py`, `utils/http_client.py`, `docs/file_crawl_stage_boundaries.md`

## 테스트와 검증 규칙

- 이 프로젝트는 pytest 패키지 구조보다 `scripts/test_*.py`, `scripts/verify_*.py`, `scripts/check_*.py`, `scripts/compare_*.py`, `scripts/diagnose_*.py`, `scripts/benchmark_*.py` 형태의 검증 스크립트가 중심이다.
- 계약 모듈을 변경하면 대응하는 작은 script 검증을 실행하거나 추가한다. 예: 파일 크롤 단계 계약은 `scripts/test_file_crawl_stage_contract.py`.
- board/file workflow, progress/SSE, request config, URL normalization, category pattern, chunking, attachment pipeline 변경은 해당 script를 골라 실행한다.
- DB나 외부 사이트가 필요한 검증은 dry-run, 샘플 입력, 로컬 fixture를 우선 사용하고, 실제 운영 DB 쓰기나 다운로드는 명시적으로 구분한다.
- 테스트 실행 결과를 최종 보고에 남기고, 실행하지 못한 경우 이유를 적는다.

근거: `scripts/test_file_crawl_stage_contract.py`, `scripts/test_chunking.py`, `scripts/verify_category_patterns.py`, `scripts/benchmark_file_attachment_pipeline.py`, `docs/board_crawling_pipeline_refactor_review.md`

## 절대 또는 거의 수정하지 말아야 할 부분

- 이름이 `#` 또는 `##`로 시작하는 파일/폴더는 백업으로 판단하고 수정하지 않는다.
- `.env`는 최소 수정 원칙을 적용한다. 새 설정이 필요하면 먼저 코드의 default와 문서화를 검토하고, 꼭 필요할 때만 사용자 확인 후 수정한다.
- `logs/`, `tmp/`, `__pycache__/`, generated cache, zip, local result JSON/log 등 산출물은 작업 대상이 아니면 건드리지 않는다.
- `download*`로 시작하는 저장 폴더와 `backend/downloads/`는 저장 산출물로 본다. 필요 시 수정 가능하지만 일반 리팩토링에서는 제외한다.
- endpoint path, SSE payload key, Redis key/status, LEARN_LIST/ASADAL table field 의미는 호환성 없이 바꾸지 않는다.
- 파일 크롤의 source display name과 storage filename을 섞지 않는다. `attachment_name`, `saved_filename`, `storage_filename`, `original_meta` 보존 규칙을 지킨다.
- 운영 DB schema는 보호 대상이다. 컬럼/인덱스 생성, runtime migration, 자동 index 보강은 현재 게시판 크롤링 안정화 작업에서 금지한다.

근거: 사용자 인터뷰 답변, `docs/file_crawl_stage_boundaries.md`, `backend/file/file_crawl_stage_contract.py`, `backend/shared/redis_sse_service.py`, `db/mariadb_save_update.py`

## AI 기능 추가 및 리팩토링 규칙

- 먼저 관련 경로를 읽고 기존 helper, contract, 문서화된 refactor plan이 있는지 확인한다.
- 새 규칙이나 새 아키텍처를 임의로 만들기보다 기존 모듈 경계에 붙인다. 불가피하게 새 경계를 만들면 adapter를 둬서 기존 payload/API를 보존한다.
- `BoardContentWorkflow` 같은 대형 파일을 직접 크게 갈아엎지 않는다. 통계, fetch, parse, save, learning, lifecycle처럼 문서화된 순서대로 작게 분리한다.
- `router.py` SSE recovery는 direct/Celery 양쪽 terminal event가 충분히 검증되기 전 제거하지 않는다.
- progress count는 `progress_contract.py`를 우선 통과시킨다. 로컬 count 보정 함수를 새로 늘리지 않는다.
- 요청 모드/boolean/scope/duplicate flag는 `CrawlRequestConfig`와 `parse_bool`을 우선 사용한다.
- file crawl stage를 바꾸면 `FileCrawlStage`/`FILE_CRAWL_STAGE_BOUNDARIES`와 `docs/file_crawl_stage_boundaries.md`를 함께 갱신한다.
- 변경 범위 밖의 인코딩, 주석, 백업 파일, 산출물 정리는 하지 않는다.

근거: `docs/board_crawling_pipeline_refactor_review.md`, `backend/shared/progress_contract.py`, `backend/shared/crawl_request_config.py`, `backend/file/file_crawl_stage_contract.py`, `scripts/test_file_crawl_stage_contract.py`

## 권장 실행 예시

```powershell
python scripts\test_file_crawl_stage_contract.py
python scripts\test_canonicalize.py
python scripts\verify_category_patterns.py
python scripts\check_url_categories.py
```

서버/워커 실행은 운영 환경 변수와 Redis/DB 준비 상태에 의존한다.

```powershell
python run_celery_worker.py
uvicorn backend.app:app --host 127.0.0.1 --port 8000
```


