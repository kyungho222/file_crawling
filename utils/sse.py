# utils/sse.py
"""
Server-Sent Events 유틸리티
"""
import json

def format_sse(data: dict, event: str = "message") -> str:
    """
    Format data as Server-Sent Event
    """
    msg = f"event: {event}\n"
    msg += f"data: {json.dumps(data)}\n\n"
    return msg
