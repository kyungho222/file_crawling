"""
Celery ?뚯빱 吏꾩엯??(?꾨줈?앺듃 猷⑦듃????.

?쒕쾭?먯꽌 WorkingDirectory媛 backend/?닿굅??PYTHONPATH媛 鍮꾩뼱 ?덉뼱??
???뚯씪??-A 濡?吏?뺥븯硫??꾨줈?앺듃 猷⑦듃媛 path???ㅼ뼱媛????깆씠 濡쒕뱶?⑸땲??

?ㅽ뻾 ??(?꾨줈?앺듃 猷⑦듃?먯꽌):
  celery -A run_celery_worker worker -l info -Q celery

?먮뒗 PID 湲곕낯 ?몃뱶紐낆쑝濡?以묐났 寃쎄퀬瑜??쇳븯?ㅻ㈃ (沅뚯옣):
  python run_celery_worker.py
  ???대??곸쑝濡?-n crawl-<PID>@%h 濡?湲곕룞?⑸땲??
  怨좎젙 ?대쫫???꾩슂?섎㈃ ?섍꼍蹂??CELERY_WORKER_NODENAME=myworker1@%h

???쒕쾭???뚯빱瑜??щ윭 媛??꾩슱 ???먮뒗 systemd 以묐났 ?ㅽ뻾 ?? ?몃뱶 ?대쫫??寃뱀튂硫?
  DuplicateNodenameWarning: Received multiple replies from node name: celery@?몄뒪??
媛 ?⑸땲?? inspect/?쒖뼱 紐낅졊??瑗ъ씪 ???덉쑝??**@ ???대쫫???뚯빱留덈떎 ?ㅻⅤ寃?* 二쇱꽭??
  celery -A run_celery_worker worker -l info -Q celery -n crawl-wf1@%h
  celery -A run_celery_worker worker -l info -Q celery -n crawl-wf2@%h
(systemd template ?대㈃ -n crawl@%i@%h 泥섎읆 ?몄뒪?댁뒪 踰덊샇瑜??ｌ뼱???⑸땲??)

?먮뒗 ?덈? 寃쎈줈:
  celery -A /path/to/crawler_web_board11/run_celery_worker worker -l info -Q celery

?댁쁺(由щ늼?? 李멸퀬:
  - uvicorn留??꾩슦硫?API??Redis???묒뾽留??ｊ퀬, ?ㅼ젣 ?щ·? ?ㅽ뻾?섏? ?딆뒿?덈떎.
  - 諛섎뱶????紐낅졊怨??숈씪??CELERY_BROKER_URL쨌肄붾뱶 寃쎈줈濡?worker瑜?蹂꾨룄 systemd ?쒕퉬???깆쑝濡??곸떆 ?ㅽ뻾?섏꽭??
  - ???대쫫??CRAWL_CELERY_QUEUE濡?諛붽엥?ㅻ㈃ worker?먮룄 ?숈씪?섍쾶 -Q 濡?吏?뺥븯?몄슂.
  - ?뚯빱媛 **?섎굹留?* ?덉뼱???섎뒗??寃쎄퀬媛 ?섎㈃, ?숈씪 ?좊떅???댁쨷 ?ㅽ뻾 以묒씤吏 ?뺤씤?섏꽭??
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_PATH_CANDIDATES = (str(_ROOT), str(_ROOT.parent))
for _rs in _PATH_CANDIDATES:
    if _rs and _rs not in sys.path:
        sys.path.insert(0, _rs)

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

try:
    from db.maria_operations import maria_execute_query
    from breadcrumb_module.db import bind_maria_execute_query

    bind_maria_execute_query(maria_execute_query)
except Exception:
    pass

from backend.src.tasks.celery_app import app, celery_app  # noqa: E402

__all__ = ["app", "celery_app"]


if __name__ == "__main__":
    # celery CLI? ?숈씪?섍쾶 worker 湲곕룞. -n 誘몄?????celery@?몄뒪??濡??щ윭 ?꾨줈?몄뒪媛 寃뱀튂誘濡?PID 湲곕컲 湲곕낯媛??ъ슜.
    import os

    from celery.bin.celery import main as celery_main

    q = (os.environ.get("CRAWL_CELERY_QUEUE") or "celery").strip() or "celery"
    nodename = (os.environ.get("CELERY_WORKER_NODENAME") or "").strip()
    if not nodename:
        nodename = f"crawl-{os.getpid()}@%h"

    sys.argv = [
        "celery",
        "-A",
        "run_celery_worker",
        "worker",
        "-l",
        (os.environ.get("CELERY_LOG_LEVEL") or "info").strip() or "info",
        "-Q",
        q,
        "-n",
        nodename,
    ]
    raise SystemExit(celery_main())

