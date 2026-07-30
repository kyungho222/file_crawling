import json
import logging

logger = logging.getLogger("backend.shared.sse_utils")


def _to_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default

def format_sse(data: dict, event: str = "message") -> str:
    """
    Format data as Server-Sent Event
    """
    if isinstance(data, dict) and isinstance(data.get("field_save_counts"), str):
        try:
            data = dict(data)
            data["field_save_counts"] = json.loads(data["field_save_counts"])
        except Exception:
            pass
    if isinstance(data, dict):
        try:
            counts = data.get("field_save_counts") if isinstance(data.get("field_save_counts"), dict) else {}
            title_count = _to_int((counts or {}).get("title"), 0)
            collection_count = _to_int(data.get("collection_count"), 0)
            source = str(data.get("source") or "").strip()
            data_event = str(data.get("event") or "").strip()
            if (
                source == "title_only"
                and collection_count > 0
                and title_count == 0
            ):
                logger.warning(
                    "[SSEZeroDebug] outgoing title count zero | event=%s status=%s job_id=%s collection=%s updated=%s field_save_counts=%s message=%s",
                    data_event,
                    data.get("status"),
                    data.get("job_id"),
                    data.get("collection_count"),
                    data.get("updated_count"),
                    data.get("field_save_counts"),
                    data.get("message"),
                )
        except Exception:
            pass
    msg = f"event: {event}\n"
    msg += f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    return msg
