# core/crawler/progress.py

from typing import Dict, Any, List

"""
크롤링 진행 상황 추적 모듈
"""
import asyncio

class Progress:
    """크롤링 진행 상황을 추적하고 콜백을 통해 알림"""
    
    def __init__(self):
        self.scan_count = 0
        self.collection_count = 0
        self.save_count = 0
        self.study_count = 0
        self.status = "running"
        self.message = "Initializing..."
        self.recent_files = []
        self.errors = []
        self._callbacks = []

    def add_callback(self, callback):
        """진행 상황 업데이트 콜백 추가"""
        self._callbacks.append(callback)

    async def _notify(self):
        """등록된 모든 콜백에 현재 상태 전달"""
        data = {
            "scan_count": self.scan_count,
            "collection_count": self.collection_count,
            "save_count": self.save_count,
            "study_count": self.study_count,
            "status": self.status,
            "message": self.message,
            "recent_files": self.recent_files[:20],
            "stage": "processing"  # Generic stage
        }
        for cb in self._callbacks:
            if asyncio.iscoroutinefunction(cb):
                await cb(data)
            else:
                cb(data)

    async def inc_scan(self, count=1, new_file=None):
        """스캔 카운트 증가"""
        self.scan_count += count
        if new_file:
            self.recent_files.insert(0, new_file)
            if len(self.recent_files) > 20:
                self.recent_files = self.recent_files[:20]
        await self._notify()

    async def inc_collection(self, count=1):
        """수집 카운트 증가"""
        self.collection_count += count
        await self._notify()

    async def inc_save(self, count=1):
        """저장 카운트 증가"""
        self.save_count += count
        print(f"[Progress] Save: {self.save_count} (+{count})", flush=True)
        await self._notify()

    async def inc_study(self, count=1):
        """학습 카운트 증가"""
        self.study_count += count
        print(f"[Progress] Study: {self.study_count} (+{count})", flush=True)
        await self._notify()

    async def update_message(self, msg):
        """메시지 업데이트"""
        self.message = msg
        await self._notify()

    async def set_status(self, status):
        """상태 업데이트"""
        self.status = status
        await self._notify()

    def get_stats(self) -> Dict[str, int]:
        """현재 진행 상황 통계를 딕셔너리로 반환"""
        return {
            'scan_count': self.scan_count,
            'collection_count': self.collection_count,
            'save_count': self.save_count,
            'study_count': self.study_count,
        }
