#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HWP/HWPX 파싱 및 청킹 결과 검사 스크립트.

downloads/ 폴더 등 로컬에 저장된 HWP 파일을 
실제 서비스에서 사용하는 로직과 동일하게 텍스트를 추출하고 
청킹(Chunking)한 결과를 로그로 보여줍니다. (DB 저장 없음)

사용법:
    python scripts/learn_local_hwp.py <파일_또는_디렉토리_경로>
"""

import os
import sys
import asyncio
import logging
from typing import List

# 프로젝트 루트 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 로깅 설정 (콘솔 출력 중심)
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s' # 단순한 출력을 위해 포맷 변경
)
logger = logging.getLogger("hwp_inspector")

def get_hwp_tools():
    from edu.hwp_edu import hwp_to_text, detect_hwp_version, extract_hwpx_data
    from backend.config import Config
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    return hwp_to_text, detect_hwp_version, extract_hwpx_data, Config, RecursiveCharacterTextSplitter

async def inspect_file(file_path: str):
    """파일의 파싱 및 청킹 결과만 출력"""
    hwp_to_text, detect_hwp_version, extract_hwpx_data, Config, RecursiveCharacterTextSplitter = get_hwp_tools()
    
    file_name = os.path.basename(file_path)
    ext = os.path.splitext(file_name)[1].lower()
    
    print("\n" + "="*80)
    print(f"🔍 [파일 분석] {file_name}")
    print(f"📍 경로: {file_path}")
    print("="*80)

    try:
        extracted_text = ""
        
        # 1. 텍스트 추출
        if ext == ".hwpx":
            print("📦 HWPX 형식 감지: 데이터 추출 중...")
            hwpx_data = extract_hwpx_data(file_path)
            # hwp_edu.py의 process_hwpx 스타일로 텍스트 결합
            texts = [item["text"] for item in hwpx_data if item["type"] == "text"]
            extracted_text = "\n".join(texts)
            print(f"✅ HWPX 텍스트 추출 완료 ({len(extracted_text)} 자)")
        else:
            version = detect_hwp_version(file_path)
            print(f"📄 HWP 형식 감지 ({version}): 텍스트 변환 중...")
            # hwp_to_text_sync 역할
            extracted_text = await asyncio.to_thread(hwp_to_text, file_path)
            print(f"✅ HWP 텍스트 추출 완료 ({len(extracted_text)} 자)")

        if not extracted_text.strip():
            print("⚠️ 추출된 텍스트가 비어있습니다.")
            return

        # 2. 청킹 (Chunking) 로직 적용
        print(f"✂️ 청킹 설정: Size={Config.BASIC_CHUNK_SIZE}, Overlap={Config.BASIC_CHUNK_OVERLAP}")
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE,
            chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(extracted_text)
        
        print(f"📊 총 청크 수: {len(chunks)}개")
        print("-" * 40)

        # 3. 청크 내용 로그 출력
        for i, chunk in enumerate(chunks):
            print(f"\n[청크 #{i+1}] (길이: {len(chunk)})")
            print("-" * 20)
            # 앞뒤 공백 제거 후 출력
            print(chunk.strip())
            print("-" * 20)

    except Exception as e:
        print(f"❌ 분석 실패: {file_name}")
        print(f"   오류 내용: {e}")

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="HWP 파킹 결과 및 청킹 데이터를 미리봅니다.")
    parser.add_argument("path", help="파일 또는 디렉토리 경로")
    args = parser.parse_args()
    
    target_path = os.path.abspath(args.path)
    if not os.path.exists(target_path):
        print(f"❌ 경로를 찾을 수 없습니다: {target_path}")
        return

    # 파일 목록 수집
    files_to_process = []
    if os.path.isfile(target_path):
        if target_path.lower().endswith(('.hwp', '.hwpx')):
            files_to_process.append(target_path)
    else:
        for root, _, files in os.walk(target_path):
            for file in files:
                if file.lower().endswith(('.hwp', '.hwpx')):
                    files_to_process.append(os.path.join(root, file))

    if not files_to_process:
        print("⚠️ HWP/HWPX 파일을 찾지 못했습니다.")
        return

    print(f"🚀 총 {len(files_to_process)}개의 파일을 분석합니다.")

    for fpath in files_to_process:
        await inspect_file(fpath)

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
