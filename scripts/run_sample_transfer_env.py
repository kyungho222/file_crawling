
import os
import sys
import logging
import asyncio
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.web_sync import sync_file_to_webserver
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SampleTransfer2")

async def run_transfer_check_env():
    # .env 파일 로드 (변경된 설정 확인용)
    load_dotenv(Path(__file__).resolve().parent.parent / "backend" / ".env", override=True)
    
    host = os.getenv("WEB_SYNC_DEFAULT_HOST")
    port = os.getenv("WEB_SYNC_DEFAULT_PORT")
    mode = os.getenv("WEB_SYNC_MODE")
    
    logger.info(f"Loaded Env - HOST: {host}, PORT: {port}, MODE: {mode}")

    # 샘플 파일 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"env_config_test_{timestamp}.txt"
    content = f"Test File for Config Verification\nHost: {host}\nPort: {port}\nMode: {mode}\nTimestamp: {timestamp}"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
        
    abs_path = os.path.abspath(filename)
    logger.info(f"Created sample file: {abs_path}")

    # 전송 시도 (설정에 따라 자동으로 host/port 사용)
    # access_base_url, chat_bot_id는 경로 생성을 위해 필수
    success = await sync_file_to_webserver(
        local_file_path=abs_path,
        access_base_url="https://test.han.kr", 
        chat_bot_id="user-bot-3ee7909ec483"
    )
    
    if success:
        logger.info(f"SUCCESS! File transferred: {filename}")
    else:
        logger.error("FAILED! File transfer failed.")

    # 청소
    if os.path.exists(filename):
        os.remove(filename)

if __name__ == "__main__":
    asyncio.run(run_transfer_check_env())
