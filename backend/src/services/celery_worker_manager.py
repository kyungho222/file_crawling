"""
온디맨드 Celery Worker 관리자
크롤링 시작 시 자동 시작, 작업 완료 후 자동 종료
"""

import os
import sys
import time
import psutil
import subprocess
import logging
import threading
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta

from utils.celery_worker_runtime import resolve_crawl_celery_worker_concurrency

logger = logging.getLogger(__name__)


class CeleryWorkerManager:
    """온디맨드 Celery Worker 관리자"""
    
    def __init__(self):
        self.worker_process: Optional[subprocess.Popen] = None
        self.last_activity_time: Optional[datetime] = None
        self.idle_timeout: int = 300  # 5분간 유휴 시 자동 종료
        self.monitor_thread: Optional[threading.Thread] = None
        self.is_monitoring = False
        
        # 경로 설정
        self.backend_root = Path(__file__).parent.parent.parent
        self.project_root = self.backend_root.parent
        self.log_dir = self.backend_root / "logs"
        self.log_dir.mkdir(exist_ok=True)
        self.celery_log_file = self.log_dir / "celery_worker.log"
    
    def is_worker_running(self) -> bool:
        """Worker가 실행 중인지 확인"""
        if self.worker_process is None:
            return False
        
        # 프로세스가 살아있는지 확인
        if self.worker_process.poll() is not None:
            # 프로세스 종료됨
            self.worker_process = None
            return False
        
        return True
    
    def get_active_tasks_count(self) -> int:
        """현재 실행 중인 작업 개수 확인"""
        try:
            from src.tasks.celery_app import celery_app
            
            inspect = celery_app.control.inspect(timeout=2.0)
            
            # 활성 작업
            active = inspect.active()
            active_count = sum(len(tasks) for tasks in active.values()) if active else 0
            
            # 예약된 작업
            scheduled = inspect.scheduled()
            scheduled_count = sum(len(tasks) for tasks in scheduled.values()) if scheduled else 0
            
            # 대기 중인 작업 (reserved)
            reserved = inspect.reserved()
            reserved_count = sum(len(tasks) for tasks in reserved.values()) if reserved else 0
            
            total = active_count + scheduled_count + reserved_count
            
            if total > 0:
                logger.debug(f"📊 작업 현황: 활성={active_count}, 예약={scheduled_count}, 대기={reserved_count}")
            
            return total
            
        except Exception as e:
            logger.debug(f"작업 개수 확인 실패: {e}")
            return 0
    
    def start_worker(self, concurrency: Optional[int] = None) -> bool:
        """
        Worker 시작
        
        Args:
            concurrency: 동시 처리 작업 수
            
        Returns:
            성공 여부
        """
        if self.is_worker_running():
            logger.info("✅ Worker가 이미 실행 중입니다")
            self.update_activity()
            return True
        
        try:
            effective_concurrency = resolve_crawl_celery_worker_concurrency(concurrency)
            logger.info(f"🚀 Celery Worker 시작 중... (동시처리: {effective_concurrency}개)")
            
            # Redis 연결 확인
            try:
                import redis
                r = redis.Redis(host='localhost', port=6379)
                r.ping()
                logger.info("✅ Redis 연결 정상")
            except Exception as e:
                logger.error(f"❌ Redis 연결 실패: {e}")
                logger.error("💡 Redis를 먼저 실행해주세요: redis-server")
                return False
            
            # Celery Worker 명령어
            if sys.platform == 'win32':
                # Windows: 백그라운드 실행
                CREATE_NO_WINDOW = 0x08000000
                
                celery_cmd = [
                    sys.executable, "-m", "celery",
                    "-A", "src.tasks.celery_app",
                    "worker",
                    "--loglevel=info",
                    "--pool=threads",  # 멀티스레드
                    f"--concurrency={effective_concurrency}",
                    f"--logfile={self.celery_log_file}"
                ]
                
                # 환경 변수
                env = os.environ.copy()
                env['PYTHONPATH'] = f"{self.project_root};{env.get('PYTHONPATH', '')}"
                
                self.worker_process = subprocess.Popen(
                    celery_cmd,
                    cwd=self.backend_root,
                    env=env,
                    creationflags=CREATE_NO_WINDOW,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            else:
                # Linux/Mac
                celery_cmd = [
                    sys.executable, "-m", "celery",
                    "-A", "src.tasks.celery_app",
                    "worker",
                    "--loglevel=info",
                    f"--concurrency={effective_concurrency}"
                ]
                
                self.worker_process = subprocess.Popen(
                    celery_cmd,
                    cwd=self.backend_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            
            logger.info(f"✅ Worker 프로세스 시작 (PID: {self.worker_process.pid})")
            logger.info(f"📝 Worker 로그: {self.celery_log_file}")
            
            # Worker 준비 대기
            logger.info("⏳ Worker 초기화 대기 중...")
            for i in range(30):
                time.sleep(1)
                try:
                    from src.tasks.celery_app import celery_app
                    inspect = celery_app.control.inspect(timeout=2.0)
                    if inspect.active() is not None:
                        logger.info(f"✅ Worker 준비 완료 ({i + 1}초 소요)")
                        break
                except Exception:
                    continue
            else:
                logger.warning("⚠️ Worker 준비 확인 타임아웃 (백그라운드에서 계속 초기화 중)")
            
            self.update_activity()
            
            # 모니터링 스레드 시작
            if not self.is_monitoring:
                self.start_monitoring()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Worker 시작 실패: {e}")
            return False
    
    def stop_worker(self, force: bool = False) -> bool:
        """
        Worker 종료
        
        Args:
            force: 강제 종료 여부
            
        Returns:
            성공 여부
        """
        if not self.is_worker_running():
            logger.info("Worker가 실행 중이 아닙니다")
            return True
        
        try:
            if force:
                logger.info("🛑 Worker 강제 종료 중...")
                self.worker_process.kill()
            else:
                logger.info("🛑 Worker 정상 종료 중...")
                self.worker_process.terminate()
                
                # 종료 대기 (최대 10초)
                try:
                    self.worker_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("⚠️ Worker 정상 종료 실패, 강제 종료합니다")
                    self.worker_process.kill()
            
            self.worker_process = None
            logger.info("✅ Worker 종료 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ Worker 종료 실패: {e}")
            return False
    
    def update_activity(self):
        """활동 시간 업데이트 (작업 시작/완료 시 호출)"""
        self.last_activity_time = datetime.now()
        logger.debug(f"🕐 활동 시간 업데이트: {self.last_activity_time}")
    
    def should_shutdown(self) -> bool:
        """종료해야 하는지 확인"""
        if not self.is_worker_running():
            return False
        
        # 활동 시간이 없으면 종료하지 않음
        if self.last_activity_time is None:
            logger.warning("⚠️ 마지막 활동 시간이 None - 초기화 필요")
            return False
        
        # 실행 중인 작업이 있으면 종료하지 않음
        active_tasks = self.get_active_tasks_count()
        
        if active_tasks > 0:
            logger.debug(f"📋 실행 중인 작업: {active_tasks}개 - 종료 보류")
            self.update_activity()  # 작업 있으면 활동 시간 갱신
            return False
        
        # 유휴 시간 확인
        idle_time = (datetime.now() - self.last_activity_time).total_seconds()
        
        if idle_time >= self.idle_timeout:
            logger.info(f"⏰ Worker 유휴: {int(idle_time)}초 (임계값: {self.idle_timeout}초) → 자동 종료")
            return True
        
        logger.debug(f"⏳ Worker 유휴: {int(idle_time)}초 / 남은 시간: {int(self.idle_timeout - idle_time)}초")
        return False
    
    def monitor_worker(self):
        """Worker 모니터링 (백그라운드 스레드)"""
        logger.info(f"👀 Worker 모니터링 시작 (체크 주기: 30초, 임계값: {self.idle_timeout // 60}분)")
        
        while self.is_monitoring:
            try:
                time.sleep(30)  # 30초마다 확인
                
                if self.should_shutdown():
                    logger.info("💤 Worker 유휴 상태 감지 - 자동 종료")
                    self.stop_worker()
                    break
                    
            except Exception as e:
                logger.error(f"⚠️ 모니터링 오류: {e}")
        
        logger.info("👀 Worker 모니터링 종료")
    
    def start_monitoring(self):
        """모니터링 스레드 시작"""
        if self.is_monitoring:
            return
        
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self.monitor_worker, daemon=True)
        self.monitor_thread.start()
        logger.info("👀 모니터링 스레드 시작")
    
    def stop_monitoring(self):
        """모니터링 스레드 종료"""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("👀 모니터링 스레드 종료")
    
    def get_status(self) -> dict:
        """Worker 상태 조회"""
        is_running = self.is_worker_running()
        
        status = {
            "is_running": is_running,
            "pid": self.worker_process.pid if self.worker_process else None,
            "last_activity": self.last_activity_time.isoformat() if self.last_activity_time else None,
            "idle_timeout": self.idle_timeout,
            "is_monitoring": self.is_monitoring
        }
        
        if is_running:
            status["active_tasks"] = self.get_active_tasks_count()
            
            # 유휴 시간 계산
            if self.last_activity_time:
                idle_seconds = (datetime.now() - self.last_activity_time).total_seconds()
                status["idle_seconds"] = int(idle_seconds)
        
        return status
    
    def ensure_worker_running(self, concurrency: Optional[int] = None) -> bool:
        """
        Worker가 실행 중인지 확인하고, 없으면 시작
        크롤링 API에서 호출
        
        Args:
            concurrency: 동시 처리 작업 수
            
        Returns:
            성공 여부
        """
        if self.is_worker_running():
            logger.debug("✅ Worker 이미 실행 중")
            self.update_activity()
            return True
        
        logger.info("🔄 Worker가 실행 중이 아닙니다. 자동 시작합니다...")
        return self.start_worker(concurrency=concurrency)


# 전역 Worker 관리자 인스턴스
worker_manager = CeleryWorkerManager()

