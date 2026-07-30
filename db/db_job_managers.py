from db.db_redis import get_redis
import redis.asyncio as aioredis

# 학습 상태 추적에 쓰는 로직
# ✅ 비동기 JobManager 클래스
class AsyncJobManager:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def start_job(self, job_id: str, ttl: int = 3600):
        """작업 시작: 상태를 in_progress로 설정"""
        await self.redis.set(f"learn:{job_id}", "in_progress", ex=ttl)

    async def cancel_job(self, job_id: str, ttl: int = 3600):
        """작업 취소: 상태를 cancel로 설정"""
        await self.redis.set(f"learn:{job_id}", "cancel", ex=ttl)

    async def complete_job(self, job_id: str, ttl: int = 3600):
        """작업 완료: 상태를 complete로 설정"""
        await self.redis.set(f"learn:{job_id}", "complete", ex=ttl)

    async def get_job_status(self, job_id: str) -> str:
        """현재 작업 상태를 반환"""
        status = await self.redis.get(f"learn:{job_id}")
        return status if status else "unknown"


# ✅ 비동기 JobProgress 클래스
class AsyncJobProgress:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def set_job_progress(self, job_id: str, progress: float, ttl: int = 3600):
        """작업 진행 상태 저장"""
        rounded_progress = round(progress, 2)
        await self.redis.set(f"progress:{job_id}", rounded_progress, ex=ttl)

    async def get_job_progress(self, job_id: str) -> float:
        """작업 진행 상태 조회"""
        progress = await self.redis.get(f"progress:{job_id}")
        return round(float(progress), 2) if progress else 0.0
    
    async def get_job_status(self, job_id: str) -> str:
        """
        편의 메서드: AsyncJobProgress 객체가 실수로 job_manager 자리로 전달되는 경우
        호출되어도 동작하도록 Redis에서 학습(job) 상태를 조회합니다.
        """
        status = await self.redis.get(f"learn:{job_id}")
        return status if status else "unknown"
    
async def get_job_manager():
    redis_client = await get_redis()
    return AsyncJobManager(redis_client)

async def get_job_progress():
    redis_client = await get_redis()
    return AsyncJobProgress(redis_client)