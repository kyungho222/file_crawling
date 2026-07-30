"""
Celery 애플리케이션 설정
백그라운드 작업 큐 관리

⚠️ 사용 전 Redis 설치 필요:
    Windows: https://github.com/microsoftarchive/redis/releases
    Linux: sudo apt install redis-server
    Docker: docker run -d -p 6379:6379 redis
"""

from __future__ import annotations

import sys
from pathlib import Path

# 워커는 uvicorn과 달리 CWD가 달라질 수 있음.
# 이 파일: .../<프로젝트루트>/backend/src/tasks/celery_app.py
#  - <프로젝트루트> 에 두면 `import backend.*` 가능
#  - backend/ 에 두면 `import src.*` 가능
_celery_file = Path(__file__).resolve()
_BACKEND_DIR = _celery_file.parents[2]
_PROJECT_ROOT = _BACKEND_DIR.parent
for _path in (_PROJECT_ROOT, _BACKEND_DIR):
    _ps = str(_path)
    if _ps not in sys.path:
        sys.path.insert(0, _ps)

import logging
import os

from celery import Celery
from src.core.config import settings
from utils.celery_worker_runtime import resolve_crawl_celery_worker_concurrency

logger = logging.getLogger(__name__)

# Celery 앱 생성
celery_app = Celery(
    "filecrawler",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "src.tasks.crawl_tasks",
        "src.tasks.metadata_tasks",
        "src.tasks.crawl_workflow_tasks",
    ]
)

# Celery CLI가 찾을 수 있도록 app 별칭 추가
app = celery_app

# Celery 설정
celery_app.conf.update(
    task_track_started=settings.celery_task_track_started,
    task_time_limit=settings.celery_task_time_limit,
    
    # 결과 저장 설정
    result_expires=3600,  # 결과 1시간 후 삭제
    
    # Worker 설정
    worker_concurrency=resolve_crawl_celery_worker_concurrency(),
    worker_prefetch_multiplier=1,  # 한 번에 하나씩 처리
    worker_max_tasks_per_child=1000,  # Worker 재시작 주기
    
    # 재시도 설정
    task_acks_late=True,  # 작업 완료 후 ACK
    task_reject_on_worker_lost=True,  # Worker 죽으면 재시도
    
    # 타임존
    timezone="Asia/Seoul",
    enable_utc=True
)

logger.info("✅ Celery 앱 초기화 완료")

# 사용 방법:
# 1. Redis 실행: redis-server
# 2. Worker (프로젝트 루트 = backend 폴더의 부모 디렉터리, CWD는 어디든 가능):
#    celery -A backend.src.tasks.celery_app worker --loglevel=info -Q celery
#    (위 celery_app.py가 sys.path에 프로젝트 루트·backend를 넣음)
# 3. 대안: CWD를 backend/로 두고 PYTHONPATH에 프로젝트 루트 지정
#    cd backend && PYTHONPATH=/path/to/project_root celery -A src.tasks.celery_app worker -l info

# 디버깅: import 시마다 inspect.ping() 하면 운영 로그가 지저분해지고 브로커 부하가 생길 수 있음.
# 필요할 때만: CELERY_DEBUG_INSPECT=1 (또는 true/yes)
if str(os.environ.get("CELERY_DEBUG_INSPECT", "") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
):
    try:
        inspector = celery_app.control.inspect(timeout=1)
        ping_res = inspector.ping()
        logger.info("[CeleryDebug] inspect.ping() => %s", ping_res)
    except Exception as e:
        logger.warning("[CeleryDebug] inspect.ping() failed: %s", e)

