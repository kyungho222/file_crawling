"""
메타데이터 처리 Celery 태스크
백그라운드에서 DB 저장, 중복 검사 등 수행
"""

from celery import Task

# Celery app import with fallbacks for different import contexts
try:
    from .celery_app import celery_app
except Exception:
    try:
        from src.tasks.celery_app import celery_app
    except Exception:
        from backend.src.tasks.celery_app import celery_app  # type: ignore

# DB imports: try multiple possible package layouts
try:
    from backend.src.db.database import SessionLocal  # type: ignore
    from backend.src.db.models import CrawledFile  # type: ignore
except Exception:
    try:
        from src.db.database import SessionLocal  # type: ignore
        from src.db.models import CrawledFile  # type: ignore
    except Exception:
        from db.database import SessionLocal  # type: ignore
        from db.models import CrawledFile  # type: ignore

from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

_HASH_FIELD_NAMES = frozenset({
    "url_hash",
    "content_hash",
    "file_hash",
    "chunk_hash",
    "hash",
})


def _strip_hash_fields(file_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Remove hash-like payload keys before DB writes."""
    sanitized = dict(file_dict or {})
    for field_name in _HASH_FIELD_NAMES:
        sanitized.pop(field_name, None)
    return sanitized


class DatabaseTask(Task):
    """DB 세션을 자동으로 관리하는 Base Task"""
    _db = None
    
    @property
    def db(self):
        if self._db is None:
            self._db = SessionLocal()
        return self._db
    
    def after_return(self, *args, **kwargs):
        """작업 완료 후 DB 세션 정리"""
        if self._db is not None:
            self._db.close()
            self._db = None


@celery_app.task(base=DatabaseTask, bind=True, name="save_metadata_to_db")
def save_metadata_to_db(self, files: List[Dict[str, Any]], domain: str, session_id: int = None):
    """
    메타데이터를 DB에 저장 (백그라운드 작업)
    
    Args:
        files: 파일 메타데이터 리스트
        domain: 사이트 도메인
        session_id: 크롤링 세션 ID (선택)
    
    Returns:
        {"saved": 저장된 개수, "duplicates": 중복된 개수}
    """
    logger.info(f"📥 DB 저장 시작: {len(files)}개 파일 (도메인: {domain})")
    
    # 작업 시작 - Worker 활동 시간 갱신
    try:
        from src.services.celery_worker_manager import worker_manager
        worker_manager.update_activity()
    except Exception:
        pass
    
    db = self.db
    saved_count = 0
    duplicate_count = 0
    
    try:
        for file_dict in files:
            sanitized_file_dict = _strip_hash_fields(file_dict)
            # URL 해시 생성 (빠른 중복 체크용)
            url = sanitized_file_dict.get('url', '')
            
            # 중복 체크 (URL 기준)
            existing = db.query(CrawledFile).filter_by(url=url).first()
            
            if existing:
                # 기존 레코드 업데이트
                existing.filename = sanitized_file_dict.get('filename')
                existing.filesize = sanitized_file_dict.get('filesize', 0)
                existing.filetype = sanitized_file_dict.get('filetype')
                existing.source_page = sanitized_file_dict.get('source_page')
                existing.domain = domain
                if hasattr(existing, 'url_hash'):
                    existing.url_hash = None
                
                # 다운로드 정보 업데이트
                saved_path = sanitized_file_dict.get('saved_path') or sanitized_file_dict.get('local_path')
                if saved_path:
                    existing.local_path = saved_path
                    existing.is_downloaded = True
                    existing.download_status = 'completed'
                
                duplicate_count += 1
                logger.debug(f"🔄 업데이트: {file_dict.get('filename')}")
            else:
                # 새 레코드 생성
                file_dict_to_save = dict(sanitized_file_dict)
                file_dict_to_save['domain'] = domain
                if session_id:
                    file_dict_to_save['session_id'] = session_id
                
                new_file = CrawledFile.from_dict(file_dict_to_save)
                db.add(new_file)
                saved_count += 1
                logger.debug(f"✨ 신규 저장: {file_dict.get('filename')}")
        
        # 커밋
        db.commit()
        
        logger.info(f"✅ DB 저장 완료: 신규 {saved_count}개, 중복 {duplicate_count}개")
        
        # 작업 완료 - Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        
        return {
            "status": "success",
            "saved": saved_count,
            "duplicates": duplicate_count,
            "total": len(files)
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"❌ DB 저장 실패: {e}")
        
        # 실패해도 Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        raise


@celery_app.task(base=DatabaseTask, bind=True, name="find_duplicates_in_db")
def find_duplicates_in_db(
    self, 
    new_files: List[Dict[str, Any]], 
    domain: str, 
    threshold: float = 90.0
):
    """
    DB에서 중복 파일 찾기 (백그라운드 작업)
    
    Args:
        new_files: 새로 발견된 파일들
        domain: 사이트 도메인
        threshold: 유사도 임계값 (%)
    
    Returns:
        {"duplicates": [...], "unique": [...]}
    """
    logger.info(f"🔍 중복 검사 시작: {len(new_files)}개 파일")
    
    # 작업 시작 - Worker 활동 시간 갱신
    try:
        from src.services.celery_worker_manager import worker_manager
        worker_manager.update_activity()
    except Exception:
        pass
    
    db = self.db
    
    try:
        # DB에서 기존 파일 조회 (도메인별)
        existing_files = db.query(CrawledFile).filter_by(domain=domain).all()
        existing_dicts = [f.to_dict() for f in existing_files]
        
        logger.info(f"📊 기존 파일: {len(existing_dicts)}개")
        
        # 유사도 계산 (기존 MetadataManager 로직 재사용)
        from filecrawler.metadata_manager import MetadataManager
        temp_manager = MetadataManager("./temp")
        temp_manager.metadata_list = existing_dicts
        
        duplicates, unique = temp_manager.find_duplicates(new_files, threshold)
        
        logger.info(f"✅ 중복 검사 완료: 중복 {len(duplicates)}개, 신규 {len(unique)}개")
        
        # 작업 완료 - Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        
        return {
            "status": "success",
            "duplicates": duplicates,
            "unique": unique,
            "total_checked": len(new_files)
        }
    
    except Exception as e:
        logger.error(f"❌ 중복 검사 실패: {e}")
        # 실패해도 Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        raise


@celery_app.task(base=DatabaseTask, bind=True, name="migrate_json_to_db")
def migrate_json_to_db(self, json_path: str, domain: str):
    """
    JSON 파일을 DB로 마이그레이션 (백그라운드 작업)
    
    Args:
        json_path: JSON 파일 경로
        domain: 사이트 도메인
    
    Returns:
        마이그레이션 결과
    """
    logger.info(f"📦 JSON → DB 마이그레이션 시작: {json_path}")
    
    # 작업 시작 - Worker 활동 시간 갱신
    try:
        from src.services.celery_worker_manager import worker_manager
        worker_manager.update_activity()
    except Exception:
        pass
    
    import json
    from pathlib import Path
    
    db = self.db
    
    try:
        # JSON 파일 로드
        json_file = Path(json_path)
        if not json_file.exists():
            raise FileNotFoundError(f"JSON 파일 없음: {json_path}")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            files = json.load(f)
        
        logger.info(f"📖 JSON 파일 로드: {len(files)}개")
        
        # DB에 저장
        result = save_metadata_to_db.apply(args=[files, domain]).get()
        
        logger.info(f"✅ 마이그레이션 완료: {result}")
        
        # 작업 완료 - Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        
        return result
    
    except Exception as e:
        logger.error(f"❌ 마이그레이션 실패: {e}")
        # 실패해도 Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        raise


@celery_app.task(base=DatabaseTask, bind=True, name="cleanup_invalid_files")
def cleanup_invalid_files(self, domain: str = None):
    """
    실제 파일이 없는 메타데이터 정리 (백그라운드 작업)
    
    Args:
        domain: 특정 도메인만 정리 (None이면 전체)
    
    Returns:
        정리 결과
    """
    logger.info(f"🧹 파일 검증 시작: domain={domain or 'ALL'}")
    
    # 작업 시작 - Worker 활동 시간 갱신
    try:
        from src.services.celery_worker_manager import worker_manager
        worker_manager.update_activity()
    except Exception:
        pass
    
    from pathlib import Path
    db = self.db
    
    try:
        # 다운로드된 파일만 조회
        query = db.query(CrawledFile).filter_by(is_downloaded=True)
        if domain:
            query = query.filter_by(domain=domain)
        
        downloaded_files = query.all()
        logger.info(f"📂 검증 대상: {len(downloaded_files)}개")
        
        invalid_count = 0
        
        for file_obj in downloaded_files:
            if file_obj.local_path:
                if not Path(file_obj.local_path).exists():
                    # 파일 없음 → 상태 업데이트
                    file_obj.is_downloaded = False
                    file_obj.download_status = 'failed'
                    file_obj.download_error = '파일이 삭제됨'
                    invalid_count += 1
        
        db.commit()
        
        logger.info(f"✅ 파일 검증 완료: {invalid_count}개 상태 업데이트")
        
        # 작업 완료 - Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        
        return {
            "status": "success",
            "checked": len(downloaded_files),
            "invalid": invalid_count
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"❌ 파일 검증 실패: {e}")
        # 실패해도 Worker 활동 시간 갱신
        try:
            from src.services.celery_worker_manager import worker_manager
            worker_manager.update_activity()
        except Exception:
            pass
        raise
