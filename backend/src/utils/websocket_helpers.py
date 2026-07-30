"""
WebSocket 유틸리티 함수 모듈
- 안전한 메시지 전송
- 연결 상태 관리
"""

import asyncio
import logging
from typing import Optional
from fastapi import WebSocket

logger = logging.getLogger(__name__)

# 🔒 WebSocket 안전 전송 헬퍼 함수
_websocket_disconnect_logged = {}  # 세션별로 로그 한 번만 출력


async def safe_send_json(
    websocket: WebSocket, 
    data: dict, 
    session_id: str = None, 
    lock: asyncio.Lock = None
) -> bool:
    """
    WebSocket으로 안전하게 JSON 전송 (연결 끊김 무시, 동시 접근 방지)
    
    Args:
        websocket: WebSocket 연결
        data: 전송할 데이터
        session_id: 세션 ID (로그 중복 방지용, 선택사항)
        lock: asyncio.Lock (동시 접근 방지용, 선택사항) - 전달 시 자동으로 Lock 사용
    
    Returns:
        전송 성공 여부
    """
    async def _send():
        try:
            from starlette.websockets import WebSocketState
            if (websocket.client_state == WebSocketState.CONNECTED and 
                websocket.application_state == WebSocketState.CONNECTED):
                await websocket.send_json(data)
                return True
            else:
                # 첫 번째 실패만 로그 (중복 방지)
                if session_id and session_id not in _websocket_disconnect_logged:
                    logger.debug(f"🔌 WebSocket 연결 끊김 감지 (session: {session_id})")
                    _websocket_disconnect_logged[session_id] = True
                return False
        except (RuntimeError, Exception) as e:
            # 첫 번째 예외만 로그 (중복 방지)
            if session_id and session_id not in _websocket_disconnect_logged:
                logger.debug(f"⚠️ WebSocket 전송 실패 (session: {session_id}): {type(e).__name__}")
                _websocket_disconnect_logged[session_id] = True
            return False
    
    # 🔒 Lock이 제공된 경우 사용, 없으면 직접 전송
    if lock:
        async with lock:
            return await _send()
    else:
        return await _send()


def clear_disconnect_log(session_id: str):
    """세션별 연결 끊김 로그 캐시 제거"""
    if session_id in _websocket_disconnect_logged:
        del _websocket_disconnect_logged[session_id]

