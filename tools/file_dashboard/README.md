# 파일 대시보드

`colle=file` 파일 크롤링을 시작하고 진행 상태를 확인하는 독립 대시보드입니다.

공개 페이지:

```text
/file-dashboard
```

화면에서 호출하는 기존 백엔드 API:

```text
POST /Ai_Pro_filecrawler/backend/session/start
GET  /Ai_Pro_filecrawler/c1/crawl_sse/{db_name}/{job_id}
POST /Ai_Pro_filecrawler/c1/crawl_stop/{job_id}
POST /Ai_Pro_filecrawler/backend/file/preview-homepage-categories
POST /Ai_Pro_filecrawler/backend/file/sync-homepage-categories
```

라우트 등록을 끄려면 다음 환경 변수를 사용합니다.

```text
FILE_DASHBOARD_ENABLED=0
```
