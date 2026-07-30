"""
CRUD (Create, Read, Update, Delete) 함수
"""
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from sqlalchemy.exc import OperationalError, IntegrityError
from datetime import datetime

from .models import CrawlSession, CrawledFile

_HASH_FIELD_NAMES = frozenset({
    "url_hash",
    "content_hash",
    "file_hash",
    "chunk_hash",
    "hash",
})


def _strip_hash_fields(data: dict) -> dict:
    sanitized = dict(data or {})
    for field_name in _HASH_FIELD_NAMES:
        sanitized.pop(field_name, None)
    return sanitized


# ==================== CrawlSession CRUD ====================

def create_session(
    db: Session,
    session_id: str,
    start_url: str = None,  # 서버 DB에 없는 컬럼 (무시)
    max_depth: int = 3,  # 서버 DB에 없는 컬럼 (무시)
    max_pages: int = 150,  # 서버 DB에 없는 컬럼 (무시)
    memo: Optional[str] = None,
    scan: int = 0,  # 탐색한 전체 파일 수
    collection: int = 0,  # 수집된 파일 수
    save: int = 0,  # 실제 저장된 파일 수
    pages: int = 0,  # 크롤링한 페이지 수
    # 서버 스키마 추가 파라미터 ⭐
    chat_bot_id: Optional[str] = None,
    mb_id: Optional[str] = None,
    mb_name: Optional[str] = None,
    subject: Optional[str] = None,
    domain: Optional[str] = None,
    colle: Optional[str] = None,
    details: Optional[str] = None,
    content_type: str = "file",
    status: str = "pending"
) -> CrawlSession:
    """크롤링 세션 생성 (서버 DB 스키마 완전 지원) ⭐"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.warning("=" * 60)
    logger.warning("💾 [DB-CRUD] create_session 호출됨")
    logger.warning("=" * 60)
    logger.warning(f"📍 받은 파라미터:")
    logger.warning(f"   session_id: {session_id}")
    logger.warning(f"   chat_bot_id: {chat_bot_id}")
    logger.warning(f"   mb_id: {mb_id}")
    logger.warning(f"   mb_name: {mb_name}")
    logger.warning(f"   subject: {subject}")
    logger.warning(f"   domain: {domain}")
    logger.warning(f"   memo: {memo}")
    logger.warning(f"   🔢 scan: {scan} (타입: {type(scan).__name__})")
    logger.warning(f"   🔢 collection: {collection} (타입: {type(collection).__name__})")
    logger.warning(f"   🔢 save: {save} (타입: {type(save).__name__})")
    logger.warning(f"   🔢 pages: {pages} (타입: {type(pages).__name__})")
    logger.warning("=" * 60)
    
    # ✅ 기존 세션 여부 먼저 확인 (중복 INSERT 방지)
    existing_session = get_session_by_id(db, session_id)
    if existing_session:
        logger.warning("♻️ [DB-CRUD] 기존 세션 발견 → 업데이트 경로로 전환")

        def _apply_if_provided(current, new):
            return new if new not in (None, "") else current

        existing_session.chat_bot_id = _apply_if_provided(existing_session.chat_bot_id, chat_bot_id)
        existing_session.mb_id = _apply_if_provided(existing_session.mb_id, mb_id)
        existing_session.mb_name = _apply_if_provided(existing_session.mb_name, mb_name)
        existing_session.subject = _apply_if_provided(existing_session.subject, subject)
        existing_session.domain = _apply_if_provided(existing_session.domain, domain)
        existing_session.colle = _apply_if_provided(existing_session.colle, colle)
        existing_session.details = _apply_if_provided(existing_session.details, details)
        existing_session.content_type = _apply_if_provided(existing_session.content_type, content_type)
        existing_session.status = _apply_if_provided(existing_session.status, status)
        existing_session.memo = _apply_if_provided(existing_session.memo, memo)

        if scan is not None:
            existing_session.scan = scan
        if collection is not None:
            existing_session.collection = collection
        if save is not None:
            existing_session.save = save
        if pages is not None:
            existing_session.pages = pages

        db.commit()
        db.refresh(existing_session)
        return existing_session

    # ✅ 1차 시도: job_id 포함하여 저장 (신규 세션)
    try:
        db_session = CrawlSession(
            job_id=session_id,
            chat_bot_id=chat_bot_id,
            mb_id=mb_id,
            # ⚠️ 레거시 컬럼 (기존 DB NOT NULL 제약조건 대응)
            start_url=start_url if start_url else "",  # 빈 문자열로 기본값 제공
            session_id=session_id,  # 레거시 호환용
            mb_name=mb_name,
            subject=subject,
            domain=domain,
            colle=colle,
            details=details,
            content_type=content_type,
            status=status,
            memo=memo,
            scan=scan,
            collection=collection,
            save=save,
            pages=pages
        )
        
        logger.warning("📍 CrawlSession 객체 생성 완료 (job_id 포함)")
        logger.warning(f"   job_id: {db_session.job_id}")
        logger.warning(f"   status: {db_session.status}")
        logger.warning(f"   🔢 scan: {db_session.scan}")
        logger.warning(f"   🔢 collection: {db_session.collection}")
        logger.warning(f"   🔢 save: {db_session.save}")
        logger.warning(f"   🔢 pages: {db_session.pages}")
        logger.warning("=" * 60)
        
        db.add(db_session)
        logger.warning("📍 DB에 추가 (add) 완료")
        
        db.commit()
        logger.warning("📍 DB 커밋 완료")
        
        db.refresh(db_session)
        logger.warning("📍 DB refresh 완료")
        logger.warning(f"   최종 저장된 scan: {db_session.scan}")
        logger.warning(f"   최종 저장된 collection: {db_session.collection}")
        logger.warning(f"   최종 저장된 save: {db_session.save}")
        logger.warning(f"   최종 저장된 pages: {db_session.pages}")
        logger.warning("=" * 60)
        
        return db_session
        
    except IntegrityError as e:
        logger.warning(f"⚠️ [DB 저장] 세션 중복 감지: {e}")
        db.rollback()

        existing_session = get_session_by_id(db, session_id)
        if existing_session:
            logger.warning("♻️ 기존 세션 업데이트로 전환")
            existing_session.chat_bot_id = chat_bot_id or existing_session.chat_bot_id
            existing_session.mb_id = mb_id or existing_session.mb_id
            existing_session.mb_name = mb_name or existing_session.mb_name
            existing_session.subject = subject or existing_session.subject
            existing_session.domain = domain or existing_session.domain
            existing_session.colle = colle or existing_session.colle
            existing_session.details = details or existing_session.details
            existing_session.content_type = content_type or existing_session.content_type
            existing_session.status = status or existing_session.status
            existing_session.memo = memo or existing_session.memo
            existing_session.scan = scan if scan is not None else existing_session.scan
            existing_session.collection = collection if collection is not None else existing_session.collection
            existing_session.save = save if save is not None else existing_session.save
            existing_session.pages = pages if pages is not None else existing_session.pages

            db.commit()
            db.refresh(existing_session)
            return existing_session

        logger.error("❌ 중복 세션이지만 기존 레코드를 찾을 수 없습니다. 예외 재발생.")
        raise

    except OperationalError as e:
        # ⚠️ job_id 컬럼이 없는 경우 (구버전 DB)
        if "no column named job_id" in str(e).lower():
            logger.warning("⚠️  job_id 컬럼 없음 감지 → job_id 제외하고 재시도")
            db.rollback()  # 트랜잭션 롤백
            
            # ✅ 2차 시도: job_id 제외하고 저장
            try:
                db_session = CrawlSession(
                    # job_id=session_id,  ← 제거
                    chat_bot_id=chat_bot_id,
                    mb_id=mb_id,
                    mb_name=mb_name,
                    subject=subject,
                    domain=domain,
                    colle=colle,
                    details=details,
                    content_type=content_type,
                    status=status,
                    memo=f"{memo} [session_id: {session_id}]" if memo else f"[session_id: {session_id}]",  # memo에 session_id 포함
                    scan=scan,
                    collection=collection,
                    save=save,
                    pages=pages
                )
                
                logger.warning("📍 CrawlSession 객체 재생성 완료 (job_id 제외)")
                logger.warning(f"   ⚠️  job_id 컬럼 없음 → memo에 session_id 저장")
                
                db.add(db_session)
                db.commit()
                db.refresh(db_session)
                
                logger.warning("✅ [Fallback 성공] job_id 없이 저장 완료")
                logger.warning(f"   저장된 ID: {db_session.id}")
                logger.warning("=" * 60)
                
                return db_session
                
            except IntegrityError as dup_fallback:
                logger.warning(f"⚠️ [Fallback] 세션 중복 감지: {dup_fallback}")
                db.rollback()
                existing_session = get_session_by_id(db, session_id)
                if existing_session:
                    logger.warning("♻️ [Fallback] 기존 세션 업데이트로 전환")
                    existing_session.chat_bot_id = chat_bot_id or existing_session.chat_bot_id
                    existing_session.mb_id = mb_id or existing_session.mb_id
                    existing_session.mb_name = mb_name or existing_session.mb_name
                    existing_session.subject = subject or existing_session.subject
                    existing_session.domain = domain or existing_session.domain
                    existing_session.colle = colle or existing_session.colle
                    existing_session.details = details or existing_session.details
                    existing_session.content_type = content_type or existing_session.content_type
                    existing_session.status = status or existing_session.status
                    existing_session.memo = memo or existing_session.memo
                    existing_session.scan = scan if scan is not None else existing_session.scan
                    existing_session.collection = collection if collection is not None else existing_session.collection
                    existing_session.save = save if save is not None else existing_session.save
                    existing_session.pages = pages if pages is not None else existing_session.pages

                    db.commit()
                    db.refresh(existing_session)
                    return existing_session

                logger.error("❌ [Fallback] 기존 세션을 찾을 수 없습니다. 예외 재발생.")
                raise

            except Exception as e2:
                logger.error(f"❌ [Fallback 실패] {type(e2).__name__}: {e2}")
                db.rollback()
                raise
        else:
            # 다른 OperationalError는 그대로 발생
            logger.error(f"❌ [DB 저장] 실패: {e}")
            db.rollback()
            raise
    
    except Exception as e:
        logger.error(f"❌ [DB 저장] 예상치 못한 오류: {type(e).__name__}: {e}")
        db.rollback()
        raise


def get_session_by_job_id(db: Session, job_id: str) -> Optional[CrawlSession]:
    """job_id로 세션 조회"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("🔍 [DB 조회] get_session_by_job_id")
    logger.info(f"   job_id: {job_id}")
    
    try:
        result = db.query(CrawlSession).filter(CrawlSession.job_id == job_id).first()
        
        if result:
            logger.info(f"   ✅ 세션 조회 성공: session_id={result.id}")
            logger.info(f"      • status: {result.status}")
            logger.info(f"      • chat_bot_id: {getattr(result, 'chat_bot_id', 'N/A')}")
            logger.info(f"      • subject: {getattr(result, 'subject', 'N/A')}")
            logger.info(f"      • scan: {getattr(result, 'scan', 0)}")
            logger.info(f"      • collection: {getattr(result, 'collection', 0)}")
            logger.info(f"      • save: {getattr(result, 'save', 0)}")
        else:
            logger.warning(f"   ⚠️ 세션을 찾을 수 없음: {job_id}")
        
        logger.info("=" * 60)
        return result
        
    except Exception as e:
        logger.error(f"   ❌ 세션 조회 실패: {e}")
        import traceback
        logger.error(f"   스택 트레이스:\n{traceback.format_exc()}")
        logger.info("=" * 60)
        raise


def get_session_by_id(db: Session, session_id: str) -> Optional[CrawlSession]:
    """세션 ID로 조회 (job_id 컬럼 없을 때도 동작)"""
    import logging
    from sqlalchemy.exc import OperationalError
    logger = logging.getLogger(__name__)
    
    try:
        # ✅ 1차 시도: job_id로 조회
        return db.query(CrawlSession).filter(CrawlSession.job_id == session_id).first()
    except OperationalError as e:
        # ⚠️ job_id 컬럼 없는 경우 - memo에서 검색
        if "no column named job_id" in str(e).lower():
            logger.warning(f"⚠️  job_id 컬럼 없음 → memo에서 session_id 검색: {session_id}")
            try:
                # memo 필드에 "[session_id: xxx]" 형식으로 저장했으므로 검색
                return db.query(CrawlSession).filter(
                    CrawlSession.memo.like(f"%[session_id: {session_id}]%")
                ).first()
            except Exception as e2:
                logger.error(f"❌ memo 검색 실패: {e2}")
                return None
        else:
            logger.error(f"❌ get_session_by_id 실패: {e}")
            return None
    except Exception as e:
        logger.error(f"❌ get_session_by_id 예상치 못한 오류: {e}")
        return None


def get_sessions(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    status: Optional[str] = None
) -> List[CrawlSession]:
    """세션 목록 조회 (페이징)"""
    query = db.query(CrawlSession)
    if status:
        query = query.filter(CrawlSession.status == status)
    return query.order_by(desc(CrawlSession.id)).offset(skip).limit(limit).all()  # created_at → id (서버 DB에 없음)


def update_session_status(
    db: Session,
    session_id: str,
    status: str,
    scan: Optional[int] = None,
    collection: Optional[int] = None,
    save: Optional[int] = None
) -> Optional[CrawlSession]:
    """세션 상태 업데이트"""
    db_session = get_session_by_id(db, session_id)
    if db_session:
        db_session.status = status
        if status == "running" and not db_session.start_at:
            db_session.start_at = datetime.now()
        if status in ["completed", "failed"]:
            db_session.end_at = datetime.now()
        if scan is not None:
            db_session.scan = scan
        if collection is not None:
            db_session.collection = collection
        if save is not None:
            db_session.save = save
        db.commit()
        db.refresh(db_session)
    return db_session


def update_session_on_download_complete(
    db: Session,
    session_id: str,
    save: int,
    status: str = "ok"
) -> Optional[CrawlSession]:
    """
    다운로드 완료 시 세션 업데이트 ⭐
    - save 값 업데이트
    - status를 "ok"로 변경
    - end_at 시간 설정
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.warning("=" * 60)
    logger.warning("🔄 [DB-CRUD] update_session_on_download_complete 호출됨")
    logger.warning("=" * 60)
    logger.warning(f"📍 업데이트할 session_id: {session_id}")
    logger.warning(f"   🔢 save: {save}")
    logger.warning(f"   status: {status}")
    
    db_session = get_session_by_id(db, session_id)
    
    if not db_session:
        logger.error(f"❌ 세션을 찾을 수 없습니다: {session_id}")
        return None
    
    logger.warning(f"📍 업데이트 전 값:")
    logger.warning(f"   scan: {db_session.scan}")
    logger.warning(f"   collection: {db_session.collection}")
    logger.warning(f"   save: {db_session.save} → {save} ⭐")
    logger.warning(f"   status: {db_session.status} → {status}")
    
    # 업데이트
    db_session.save = save
    db_session.status = status
    db_session.end_at = datetime.now()
    
    db.commit()
    db.refresh(db_session)
    
    logger.warning(f"📍 업데이트 완료:")
    logger.warning(f"   최종 scan: {db_session.scan}")
    logger.warning(f"   최종 collection: {db_session.collection}")
    logger.warning(f"   최종 save: {db_session.save}")
    logger.warning(f"   최종 status: {db_session.status}")
    logger.warning("=" * 60)
    
    return db_session


def delete_session(db: Session, session_id: str) -> bool:
    """세션 삭제 (연결된 파일도 함께 삭제)"""
    db_session = get_session_by_id(db, session_id)
    if db_session:
        db.delete(db_session)
        db.commit()
        return True
    return False


# ==================== CrawledFile CRUD ====================

def create_file(
    db: Session,
    session_id: int,
    filename: str,
    url: str,
    filesize: int = 0,
    filetype: Optional[str] = None,
    formatted_size: Optional[str] = None,
    source_page: Optional[str] = None,
    last_modified: Optional[str] = None,
    content_type: Optional[str] = None
) -> CrawledFile:
    """파일 정보 생성"""
    db_file = CrawledFile(
        session_id=session_id,
        filename=filename,
        url=url,
        filesize=filesize,
        filetype=filetype,
        formatted_size=formatted_size,
        source_page=source_page,
        last_modified=last_modified,
        content_type=content_type
    )
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    return db_file


def bulk_create_files(db: Session, files_data: List[dict], batch_size: int = 100) -> tuple[List[CrawledFile], int]:
    """
    파일 정보 일괄 생성 (100개 단위로 배치 처리)
    중복 URL은 자동으로 건너뜀 (UNIQUE 제약조건)
    
    Args:
        db: DB 세션
        files_data: 파일 데이터 리스트
        batch_size: 배치 크기 (기본값: 100)
    
    Returns:
        tuple: (생성/업데이트된 파일 리스트, 중복 건너뜀 개수)
    """
    created_files = []
    skipped_count = 0
    total_files = len(files_data)
    
    # 100개 단위로 배치 처리
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch_data = files_data[batch_start:batch_end]
        
        for data in batch_data:
            sanitized_data = _strip_hash_fields(data)
            try:
                # 중복 URL 체크
                existing = db.query(CrawledFile).filter(CrawledFile.url == sanitized_data.get('url')).first()
                if existing:
                    skipped_count += 1
                    # 이미 존재하면 download_status 업데이트만
                    if data.get('download_status') == 'completed' and existing.download_status != 'completed':
                        existing.download_status = 'completed'
                        existing.is_downloaded = True  # 🔧 다운로드 완료 플래그 업데이트
                        existing.local_path = sanitized_data.get('local_path')
                        existing.downloaded_at = datetime.now()
                        db.commit()
                        db.refresh(existing)
                        created_files.append(existing)
                    # 메타데이터만 있었는데 다운로드 정보가 추가되는 경우도 처리
                    elif sanitized_data.get('is_downloaded') and not existing.is_downloaded:
                        existing.is_downloaded = sanitized_data.get('is_downloaded')
                        existing.download_status = sanitized_data.get('download_status', existing.download_status)
                        existing.local_path = sanitized_data.get('local_path', existing.local_path)
                        existing.downloaded_at = sanitized_data.get('downloaded_at', existing.downloaded_at)
                        db.commit()
                        db.refresh(existing)
                        created_files.append(existing)
                    continue
                
                # 새 파일 생성
                db_file = CrawledFile(**sanitized_data)
                db.add(db_file)
                db.commit()
                db.refresh(db_file)
                created_files.append(db_file)
            except Exception as e:
                # 개별 파일 저장 실패 시 rollback하고 계속 진행
                db.rollback()
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ 파일 저장 실패 (건너뜀): {data.get('filename')} - {e}")
                continue
        
        # 배치 완료 후 커밋 (100개 단위)
        try:
            db.commit()
            if batch_end % (batch_size * 10) == 0 or batch_end == total_files:
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"💾 배치 저장 진행: {batch_end}/{total_files}개 ({batch_end*100//total_files if total_files > 0 else 0}%)")
        except Exception as e:
            db.rollback()
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"❌ 배치 커밋 실패: {e}")
    
    if skipped_count > 0:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"ℹ️ 중복 URL 건너뜀: {skipped_count}개")
    
    return created_files, skipped_count


def get_file_by_id(db: Session, file_id: int) -> Optional[CrawledFile]:
    """파일 ID로 조회"""
    return db.query(CrawledFile).filter(CrawledFile.id == file_id).first()


def get_files_by_session_id(db: Session, session_id: int) -> List[CrawledFile]:
    """세션 ID로 파일 목록 조회"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("🔍 [DB 조회] get_files_by_session_id")
    logger.info(f"   session_id: {session_id}")
    
    try:
        result = (
            db.query(CrawledFile)
            .filter(CrawledFile.session_id == session_id)
            .order_by(CrawledFile.created_at)
            .all()
        )
        
        logger.info(f"   ✅ 파일 목록 조회 성공: {len(result)}개")
        
        # 다운로드 완료 파일 통계
        downloaded_count = sum(1 for f in result if getattr(f, 'is_downloaded', False))
        logger.info(f"      • 전체 파일: {len(result)}개")
        logger.info(f"      • 다운로드 완료: {downloaded_count}개")
        logger.info(f"      • 다운로드 미완료: {len(result) - downloaded_count}개")
        
        # 처음 5개 파일 정보 출력
        if result:
            logger.info("      📁 파일 목록 (처음 5개):")
            for idx, f in enumerate(result[:5], 1):
                logger.info(f"         [{idx}] {f.filename}")
                logger.info(f"             • URL: {f.url[:80] if f.url else 'N/A'}...")
                logger.info(f"             • 다운로드: {'✅' if getattr(f, 'is_downloaded', False) else '❌'}")
                logger.info(f"             • 경로: {getattr(f, 'local_path', 'N/A')}")
            if len(result) > 5:
                logger.info(f"         ... 외 {len(result) - 5}개")
        
        logger.info("=" * 60)
        return result
        
    except Exception as e:
        logger.error(f"   ❌ 파일 목록 조회 실패: {e}")
        import traceback
        logger.error(f"   스택 트레이스:\n{traceback.format_exc()}")
        logger.info("=" * 60)
        raise


def get_files_by_session(
    db: Session,
    session_id: int,
    skip: int = 0,
    limit: int = 1000
) -> List[CrawledFile]:
    """특정 세션의 파일 목록 조회"""
    return (
        db.query(CrawledFile)
        .filter(CrawledFile.session_id == session_id)
        .order_by(CrawledFile.created_at)
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_files_by_type(
    db: Session,
    filetype: str,
    skip: int = 0,
    limit: int = 100
) -> List[CrawledFile]:
    """파일 타입으로 조회 (예: pdf, hwp)"""
    return (
        db.query(CrawledFile)
        .filter(CrawledFile.filetype == filetype)
        .order_by(desc(CrawledFile.created_at))
        .offset(skip)
        .limit(limit)
        .all()
    )


def update_file_download_status(
    db: Session,
    file_id: int,
    status: str,
    local_path: Optional[str] = None,
    error_message: Optional[str] = None
) -> Optional[CrawledFile]:
    """파일 다운로드 상태 업데이트"""
    db_file = get_file_by_id(db, file_id)
    if db_file:
        db_file.download_status = status
        if status == "completed":
            db_file.is_downloaded = True
            db_file.downloaded_at = datetime.now()
        if local_path:
            db_file.local_path = local_path
        if error_message:
            db_file.download_error = error_message
        db.commit()
        db.refresh(db_file)
    return db_file


def search_files(
    db: Session,
    keyword: str,
    skip: int = 0,
    limit: int = 100
) -> List[CrawledFile]:
    """파일명으로 검색"""
    return (
        db.query(CrawledFile)
        .filter(CrawledFile.filename.contains(keyword))
        .order_by(desc(CrawledFile.created_at))
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_download_stats(db: Session, session_id: Optional[int] = None) -> dict:
    """다운로드 통계"""
    query = db.query(CrawledFile)
    if session_id:
        query = query.filter(CrawledFile.session_id == session_id)
    
    total = query.count()
    downloaded = query.filter(CrawledFile.is_downloaded == True).count()
    pending = query.filter(CrawledFile.download_status == "pending").count()
    failed = query.filter(CrawledFile.download_status == "failed").count()
    
    return {
        "total": total,
        "downloaded": downloaded,
        "pending": pending,
        "failed": failed,
        "download_rate": (downloaded / total * 100) if total > 0 else 0
    }


def delete_file(db: Session, file_id: int) -> bool:
    """파일 삭제"""
    db_file = get_file_by_id(db, file_id)
    if db_file:
        db.delete(db_file)
        db.commit()
        return True
    return False


def get_all_domains(db: Session) -> List[str]:
    """
    모든 도메인 목록 조회 (섹션 구분용)
    
    Returns:
        도메인 리스트 (중복 제거)
    """
    from sqlalchemy import distinct
    result = db.query(distinct(CrawledFile.domain)).filter(CrawledFile.domain.isnot(None)).all()
    return [domain for (domain,) in result if domain]


def get_files_by_domain(
    db: Session,
    domain: str,
    skip: int = 0,
    limit: int = 1000,
    download_status: Optional[str] = None
) -> List[CrawledFile]:
    """
    특정 도메인(섹션)의 파일 목록 조회
    
    Args:
        domain: 도메인명 (예: gwangjin.go.kr)
        skip: 건너뛸 개수
        limit: 최대 조회 개수
        download_status: 다운로드 상태 필터 (예: completed, pending)
    
    Returns:
        파일 리스트
    """
    query = db.query(CrawledFile).filter(CrawledFile.domain == domain)
    
    if download_status:
        query = query.filter(CrawledFile.download_status == download_status)
    
    return (
        query
        .order_by(desc(CrawledFile.created_at))
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_domain_stats(db: Session, domain: str) -> dict:
    """
    특정 도메인의 통계 정보
    
    Args:
        domain: 도메인명
        
    Returns:
        통계 딕셔너리
    """
    query = db.query(CrawledFile).filter(CrawledFile.domain == domain)
    
    total = query.count()
    downloaded = query.filter(CrawledFile.is_downloaded == True).count()
    pending = query.filter(CrawledFile.download_status == "pending").count()
    failed = query.filter(CrawledFile.download_status == "failed").count()
    
    # 파일 타입별 통계
    type_stats = {}
    type_results = (
        db.query(CrawledFile.filetype, func.count(CrawledFile.id))
        .filter(CrawledFile.domain == domain)
        .group_by(CrawledFile.filetype)
        .all()
    )
    for filetype, count in type_results:
        type_stats[filetype if filetype else "unknown"] = count
    
    return {
        "domain": domain,
        "total": total,
        "downloaded": downloaded,
        "pending": pending,
        "failed": failed,
        "download_rate": (downloaded / total * 100) if total > 0 else 0,
        "file_types": type_stats
    }
