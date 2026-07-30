"""
Celery 백그라운드 작업 모듈.
태스크 발행은 task_sender를 통해 연결합니다.

주의: `from src.tasks...` 절대 import는 패키지가 `backend.src.tasks`로 로드될 때
`src`가 sys.path에 없으면 실패하고, __init__ 안에서 backend를 path에 넣은 뒤
`from src.tasks.celery_app`를 쓰면 `src.tasks` 재진입으로 무한 재귀가 난다.
따라서 여기서는 반드시 상대 import만 사용한다.
"""

from .celery_app import celery_app
from .task_sender import (
    get_celery_app,
    send_crawl_site_background,
    send_crawl_workflow_dispatch_job,
    send_faiss_refresh,
    send_download_file_background,
    send_save_metadata_to_db,
)

__all__ = [
    "celery_app",
    "get_celery_app",
    "send_crawl_site_background",
    "send_crawl_workflow_dispatch_job",
    "send_faiss_refresh",
    "send_download_file_background",
    "send_save_metadata_to_db",
]

