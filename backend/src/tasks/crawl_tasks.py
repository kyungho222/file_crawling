"""
크롤링 Celery 태스크
백그라운드에서 크롤링 수행
"""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가 (filecrawler 모듈 import를 위해)
current_file = Path(__file__).resolve()
backend_root = current_file.parent.parent.parent  # backend/
project_root = backend_root.parent  # filecrawler_standalone/
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.tasks.celery_app import celery_app
from src.tasks.metadata_tasks import save_metadata_to_db
import logging

logger = logging.getLogger(__name__)


@celery_app.task(name="crawl_site_background")
def crawl_site_background(
    start_url: str,
    max_depth: int = 3,
    max_pages: int = 150,
    save_to_db: bool = True,
    domain: str = None
):
    """
    사이트 크롤링 (백그라운드 작업)
    
    Args:
        start_url: 시작 URL
        max_depth: 크롤링 깊이
        max_pages: 최대 페이지 수
        save_to_db: True면 DB 저장, False면 JSON 저장
        domain: 도메인 (없으면 자동 추출)
    
    Returns:
        크롤링 결과
    """
    logger.info(f"🚀 크롤링 시작: {start_url} (깊이={max_depth}, 페이지={max_pages})")
    
    # 작업 시작 - Worker 활동 시간 갱신
    try:
        from src.services.celery_worker_manager import worker_manager
        worker_manager.update_activity()
    except Exception:
        pass
    
    try:
        from filecrawler.site_crawler import SiteCrawler
        from urllib.parse import urlparse
        
        # 도메인 추출
        if not domain:
            parsed = urlparse(start_url)
            domain = parsed.netloc.replace('www.', '')
        
        # 크롤링 수행
        crawler = SiteCrawler(start_url)
        files = crawler.crawl(max_depth=max_depth, max_pages=max_pages)
        
        logger.info(f"📊 크롤링 완료: {len(files)}개 파일 발견")
        
        # DB 저장 또는 JSON 저장
        if save_to_db:
            # 메타데이터 DB 저장 태스크 실행
            result = save_metadata_to_db.apply_async(
                args=[files, domain],
                countdown=1  # 1초 후 실행
            )
            logger.info(f"💾 DB 저장 태스크 등록: {result.id}")
            
            # 작업 완료 - Worker 활동 시간 갱신
            try:
                worker_manager.update_activity()
            except Exception:
                pass
            
            return {
                "status": "success",
                "files_found": len(files),
                "db_task_id": result.id,
                "domain": domain
            }
        else:
            # JSON 저장
            from filecrawler.metadata_manager import MetadataManager
            storage_dir = f"./backend/downloads/{domain.replace('.', '_')}"
            manager = MetadataManager(storage_dir)
            manager.save_metadata(files)
            
            # 작업 완료 - Worker 활동 시간 갱신
            try:
                worker_manager.update_activity()
            except Exception:
                pass
            
            return {
                "status": "success",
                "files_found": len(files),
                "storage": "json",
                "domain": domain
            }
    
    except Exception as e:
        logger.error(f"❌ 크롤링 실패: {e}")
        
        # 실패해도 Worker 활동 시간 갱신
        try:
            worker_manager.update_activity()
        except Exception:
            pass
        raise


@celery_app.task(
    name="download_file_background", 
    bind=True,
    autoretry_for=(Exception,),  # 🔄 모든 예외에 대해 자동 재시도
    retry_kwargs={'max_retries': 3, 'countdown': 2},  # 🔄 최대 3회, 2초 간격
    retry_backoff=True,  # 🔄 지수 백오프 (2초 → 4초 → 8초)
    retry_jitter=True  # 🔄 랜덤 지연 추가 (서버 부하 분산)
)
def download_file_background(
    self,
    file_url: str,
    filename: str,
    save_dir: str,
    domain: str = None,
    custom_extensions: list = None  # 📄 문서 필터
):
    """
    파일 다운로드 (백그라운드 작업)
    
    ⚡ 자동 재시도 기능:
    - IncompleteRead 등 네트워크 에러 시 자동 재시도
    - 최대 3회 재시도 (총 4번 시도)
    - 지수 백오프: 2초 → 4초 → 8초
    - 랜덤 지터로 서버 부하 분산
    
    Args:
        self: Celery task instance (bind=True로 자동 전달)
        file_url: 파일 URL
        filename: 저장할 파일명
        save_dir: 저장 디렉토리
        domain: 도메인
        custom_extensions: 허용할 파일 확장자 리스트 (문서만 다운로드 시 지정)
    
    Returns:
        다운로드 결과
    """
    # 🔄 재시도 정보 로깅
    retry_count = self.request.retries
    if retry_count > 0:
        logger.info(f"🔄 다운로드 재시도 ({retry_count}/3): {filename}")
    else:
        logger.info(f"⬇️ 다운로드 시작: {filename}")
    
    # 작업 시작 - Worker 활동 시간 갱신
    try:
        from src.services.celery_worker_manager import worker_manager
        worker_manager.update_activity()
    except Exception:
        pass
    
    # 🚀 실시간 상태 업데이트: 다운로드 시작
    self.update_state(
        state='DOWNLOADING',
        meta={
            'current_file': filename,
            'file_url': file_url,
            'status': 'downloading',
            'progress': 0
        }
    )
    
    try:
        from filecrawler.downloader import FileDownloader
        from filecrawler import FileMeta
        from pathlib import Path
        
        # FileMeta 객체 생성
        file_meta = FileMeta(
            filename=filename,
            url=file_url,
            filesize=0,
            filetype="",
            source_page=""
        )
        
        # 다운로드
        downloader = FileDownloader(save_dir, custom_extensions=custom_extensions)
        result = downloader.download_file(file_meta)
        
        if result and result.get("success"):
            logger.info(f"✅ 다운로드 완료: {result.get('path')}")
            
            # 🚀 실시간 상태 업데이트: 다운로드 완료
            self.update_state(
                state='SUCCESS',
                meta={
                    'current_file': filename,
                    'file_url': file_url,
                    'status': 'completed',
                    'progress': 100,
                    'size': result.get("size", 0),
                    'path': result.get("path")
                }
            )
            
            # 작업 완료 - Worker 활동 시간 갱신
            try:
                worker_manager.update_activity()
            except Exception:
                pass
            
            return {
                "success": True,
                "filename": result.get("filename"),
                "path": result.get("path"),
                "url": file_url,
                "size": result.get("size", 0)
            }
        else:
            error_msg = result.get("error", "Unknown") if result else "다운로드 실패"
            raise Exception(error_msg)
    
    except Exception as e:
        retry_count = self.request.retries
        max_retries = 3
        
        # 🔄 재시도 가능한 경우와 최종 실패 구분
        if retry_count < max_retries:
            logger.warning(f"⚠️ 다운로드 에러 ({retry_count + 1}/{max_retries + 1}): {filename} - {e}")
            logger.info(f"🔄 {2 ** (retry_count + 1)}초 후 재시도 예정...")
        else:
            logger.error(f"❌ 다운로드 최종 실패 ({max_retries + 1}회 시도): {filename} - {e}")
        
        # 실패해도 Worker 활동 시간 갱신
        try:
            worker_manager.update_activity()
        except Exception:
            pass
        raise


