# core/crawler/batch_queue.py
import asyncio
import os
from typing import List, Any, TypeVar, Generic

T = TypeVar('T')


def _env_queue_maxsize(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)) or str(default))
    except Exception:
        value = default
    return max(1, min(value, 5000))

class BatchQueue(Generic[T]):
    """
    아이템을 모아서 배치 단위로 반환하는 큐
    - put(): 아이템을 버퍼에 추가. 버퍼가 꽉 차면 큐에 배치(List)를 put
    - get(): 큐에서 배치(List)를 get
    """
    def __init__(self, batch_size: int = 100, queue_maxsize: int | None = None):
        self.batch_size = batch_size
        self.buffer: List[T] = []
        if queue_maxsize is None:
            queue_maxsize = _env_queue_maxsize("CRAWLER_BATCH_QUEUE_MAXSIZE", 200)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=int(queue_maxsize))
        self._lock = asyncio.Lock()

    async def put(self, item: T):
        """아이템을 버퍼에 추가하고, 배치 크기에 도달하면 큐에 전달"""
        if self.batch_size <= 1:
            # 건당 즉시 전달 (버퍼링 없음)
            await self.queue.put([item])
            return
        async with self._lock:
            self.buffer.append(item)
            if len(self.buffer) >= self.batch_size:
                batch = self.buffer[:self.batch_size]
                self.buffer = self.buffer[self.batch_size:]
                await self.queue.put(batch)

    def put_nowait(self, item: T) -> None:
        if self.batch_size > 1:
            raise RuntimeError("BatchQueue.put_nowait requires batch_size=1")
        self.queue.put_nowait([item])
    async def get(self) -> List[T]:
        """큐에서 배치를 가져옴"""
        return await self.queue.get()

    async def flush(self):
        """버퍼에 남은 아이템을 강제로 큐에 전달 (종료 시 또는 주기적 flush 시 사용)

        중요:
        - 기존 구현은 buffer 전체를 '하나의 큰 배치'로 put 했기 때문에,
          여러 consumer(worker)가 있어도 1개의 worker가 큰 배치를 독점 처리하며
          병렬성이 크게 떨어질 수 있습니다.
        - flush 시에도 batch_size 단위로 쪼개서 put 하여 여러 worker가 분산 처리할 수 있게 합니다.
        """
        async with self._lock:
            if not self.buffer:
                return 0

            total = len(self.buffer)
            while self.buffer:
                batch = self.buffer[: self.batch_size]
                self.buffer = self.buffer[self.batch_size :]
                await self.queue.put(batch)
            return total  # 전달된 아이템 수 반환

    def empty(self) -> bool:
        # 큐와 버퍼가 모두 비어있는지 확인
        return self.queue.empty() and not self.buffer

    def task_done(self):
        # 큐의 작업 완료 신호를 내부 큐에 전달
        self.queue.task_done()

    async def join(self):
        await self.queue.join()

    def task_done(self):
        self.queue.task_done()
