import sys
import asyncio
sys.path.insert(0, ".")

from edu import hwp_edu as hwp

# Stub DB write functions to avoid external DB calls during test
hwp.insert_data = lambda *a, **k: None
hwp.delete_data = lambda *a, **k: None

# Stub embedding to avoid external API calls
try:
    hwp.embedding_model.aembed_query = lambda text: [0.0] * 1536
except Exception:
    # If embedding_model not present, ignore
    pass

class DummyJobManager:
    async def get_job_status(self, job_id):
        return "running"

class DummyJobProgress:
    async def get_job_progress(self, job_id):
        return 0.0

    async def set_job_progress(self, job_id, v):
        return None


async def run_test():
    content = "보도자료20260115광진구 2026년 학습나루터 프로그램 공모"
    file_path = r"downloads/보도자료20260115광진구 2026년 학습나루터 프로그램 공모.hwp"
    table_name = "td_test"
    dbname = "test_db"
    job_id = "test_job"
    each_progress = 0.0
    jm = DummyJobManager()
    jp = DummyJobProgress()
    print("TEST_START")
    try:
        res = await hwp.process_hwp(
            content,
            file_path,
            "hwp",  # content_type
            table_name,
            dbname,
            job_id,
            each_progress,
            jm,
            jp,
            memo="test",
            personal_info_filter="N",
        )
        print("TEST_RESULT:", res)
    except Exception as e:
        print("TEST_ERROR:", e)


if __name__ == "__main__":
    asyncio.run(run_test())


