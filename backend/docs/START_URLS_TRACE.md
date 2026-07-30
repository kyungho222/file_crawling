# start_urls 디버깅 추적

로그에서 `[START_URLS_TRACE]` 로 검색하면 start_urls가 어디서 채워지고 어떻게 전달되는지 순서대로 추적할 수 있습니다.

## 흐름 요약

```
1. crawl_start._prepare_crawl
   → stream_asadal_urls_from_db() 로 DB에서 URL 로드
   → data["start_urls_override"] = urls
   → [START_URLS_TRACE] crawl_start._prepare_crawl | source=stream_asadal_urls_from_db

2. crawl_start._crawl_file_worker
   → urls = data.get("start_urls_override", [])
   → [START_URLS_TRACE] _crawl_file_worker | start_urls_override count=...

3. dispatch_and_schedule_workflow (crawl_dispatcher)
   → start_urls = [] 초기화
   → 우선순위 1: data["start_urls_override"] (리스트면 정규화 후 사용)
   → [START_URLS_TRACE] dispatch override | source=...
   → 우선순위 2: header_response.board_list_urls (없으면)
   → 우선순위 3: header_response.query_links (없으면)
   → (선택) contents[0] 기준 board_id 필터
   → 없으면: start_urls = [contents[0]]
   → [START_URLS_TRACE] dispatch fallback | (start_urls 비었을 때만)
   → [START_URLS_TRACE] dispatch resolved (final) | count=... sample=...

4. run_workflow_task (workflow_runner)
   → [START_URLS_TRACE] workflow_runner.run_workflow_task received | start_urls_count=...
   → (선택) resolve_runtime_start_urls 로 list → view 확장
   → [START_URLS_TRACE] workflow_runner calling start_workflow | start_urls_count=...

5. BoardContentWorkflow.start_workflow (board_content_workflow)
   → [START_URLS_TRACE] board_content_workflow.start_workflow entry | start_urls_count=...
   → Direct DB Stream 모드: 실제 처리 URL은 ASADAL_CRAWLING_EXPLORATION에서 조회 (start_urls는 target_domains 추출 등에만 사용)
```

## 로그로 추적하는 방법

- **특정 job_id만 보기**: `grep "START_URLS_TRACE.*job_id=YOUR_JOB_ID" 로그파일`
- **전체 순서 보기**: `grep "START_URLS_TRACE" 로그파일`

## start_urls가 비어 있을 때 확인할 것

1. `_prepare_crawl`: `stream_asadal_urls_from_db`가 URL을 반환했는지 (DB 조건: chat_bot_id, target_domains, type)
2. `dispatch`: `data["start_urls_override"]`가 리스트로 들어왔는지, `header_response.board_list_urls` / `query_links` 폴백이 있는지
3. `dispatch`: board_id 필터로 전부 제거됐는지 (Fallback to input 로그)
