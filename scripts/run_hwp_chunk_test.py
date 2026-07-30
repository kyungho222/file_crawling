import asyncio
import os
import sys

# 프로젝트 루트를 sys.path에 추가하여 상대 import 문제를 방지합니다.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from edu.hwp_edu import calculate_hwp_chunks


BASE_DIR = os.path.join("backend", "downloads")
OUTPUT_PATH = os.path.join(BASE_DIR, "hwp_chunk_results.txt")


async def main():
    if not os.path.isdir(BASE_DIR):
        print(f"ERROR: downloads 폴더를 찾을 수 없습니다: {BASE_DIR}", file=sys.stderr)
        return

    files = [
        f
        for f in os.listdir(BASE_DIR)
        if f.lower().endswith(".hwp") or f.lower().endswith(".hwpx")
    ]

    results = []
    for fn in files:
        path = os.path.join(BASE_DIR, fn)
        try:
            chunks = await calculate_hwp_chunks(path)
        except Exception as e:
            chunks = f"error:{e}"
        line = f"{fn}\t{chunks}"
        print(line)
        results.append(line)

    try:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as outf:
            outf.write("\n".join(results))
        print(f"\nResults saved to: {OUTPUT_PATH}")
    except Exception as e:
        print(f"ERROR writing results file: {e}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())


