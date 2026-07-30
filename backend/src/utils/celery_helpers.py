"""
Celery 작업 관리 헬퍼 함수
"""

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


async def cancel_celery_tasks(task_name_filter: Optional[str] = None) -> Tuple[int, int]:
    """
    실행 중인 Celery 작업 취소
    
    Args:
        task_name_filter: 작업 이름 필터 (예: "download" - download가 포함된 작업만 취소)
                         None이면 모든 작업 취소
    
    Returns:
        (active_cancelled, reserved_cancelled): 취소된 작업 수
    
    Examples:
        >>> active, reserved = await cancel_celery_tasks("download")
        >>> print(f"취소됨: active={active}, reserved={reserved}")
    """
    active_cancelled = 0
    reserved_cancelled = 0
    
    try:
        from src.tasks.celery_app import celery_app
        
        logger.info(f"🛑 Celery 작업 취소 시작 (필터: {task_name_filter or '전체'})")
        
        inspect = celery_app.control.inspect(timeout=2.0)
        
        # 실행 중인 작업 (active) 취소
        active_tasks = inspect.active()
        if active_tasks:
            for worker_name, tasks in active_tasks.items():
                for task in tasks:
                    task_id = task.get('id')
                    task_name = task.get('name', 'unknown')
                    
                    # 필터링
                    if task_name_filter and task_name_filter.lower() not in task_name.lower():
                        continue
                    
                    celery_app.control.revoke(task_id, terminate=True, signal='SIGKILL')
                    active_cancelled += 1
                    logger.info(f"   🛑 작업 취소됨: {task_name} (ID: {task_id[:8]}...)")
        
        # 예약된 작업 (reserved) 취소
        reserved_tasks = inspect.reserved()
        if reserved_tasks:
            for worker_name, tasks in reserved_tasks.items():
                for task in tasks:
                    task_id = task.get('id')
                    task_name = task.get('name', 'unknown')
                    
                    # 필터링
                    if task_name_filter and task_name_filter.lower() not in task_name.lower():
                        continue
                    
                    celery_app.control.revoke(task_id, terminate=True)
                    reserved_cancelled += 1
                    logger.info(f"   🛑 예약 작업 취소됨: {task_name} (ID: {task_id[:8]}...)")
        
        total = active_cancelled + reserved_cancelled
        if total > 0:
            logger.info(f"✅ 총 {total}개 작업 취소 완료 (active: {active_cancelled}, reserved: {reserved_cancelled})")
        else:
            logger.debug("ℹ️  취소할 작업이 없음")
        
        return active_cancelled, reserved_cancelled
        
    except Exception as e:
        logger.debug(f"⚠️ Celery 작업 취소 실패 (Worker 미실행 가능): {e}")
        return 0, 0

