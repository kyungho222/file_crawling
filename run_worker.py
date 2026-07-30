import os
import sys

# 1. ?덈? 寃쎈줈 吏??(run_worker.py媛 ?덈뒗 理쒖긽???대뜑? backend ?대뜑)
project_root = os.path.dirname(os.path.abspath(__file__))
project_parent = os.path.dirname(project_root)
backend_root = os.path.join(project_root, "backend")

# 2. ?뚯씠?ъ씠 紐⑤뱢??李얠쓣 ???덈룄濡?sys.path??媛뺤젣 二쇱엯
# (?곕??먯뿉??export PYTHONPATH ?섎뜕 ?묒뾽???뚯씠?ъ씠 ????댁쨲)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if project_parent not in sys.path:
    sys.path.insert(0, project_parent)
if backend_root not in sys.path:
    sys.path.insert(0, backend_root)

try:
    from db.maria_operations import maria_execute_query
    from breadcrumb_module.db import bind_maria_execute_query

    bind_maria_execute_query(maria_execute_query)
except Exception as exc:
    print(f"[breadcrumb] DB binding skipped at worker startup: {exc}")

# 3. ??щ━ 紐⑤뱢 ?꾪룷??諛?而ㅻ㎤?쒕씪???ㅽ뻾
from celery.bin.celery import main

if __name__ == '__main__':
    print(f"?? Celery ?뚯빱 ?쒖옉! (寃쎈줈 ?먮룞 ?명똿 ?꾨즺: {project_root})")

    # DuplicateNodenameWarning 諛⑹?: ?숈씪 ?쒕쾭???뚯빱媛 2媛??댁긽?대㈃ @ ???대쫫???щ씪????
    # 怨좎젙 worker1@%h ???꾨줈?몄뒪瑜???媛??꾩슦硫??ㅼ떆 寃쎄퀬媛 ?쒕떎.
    q = (os.environ.get("CRAWL_CELERY_QUEUE") or "celery").strip() or "celery"
    nodename = (os.environ.get("CELERY_WORKER_NODENAME") or "").strip()
    if not nodename:
        nodename = f"crawl-{os.getpid()}@%h"

    sys.argv = [
        "celery",
        "-A",
        "backend.src.tasks.celery_app",
        "worker",
        "-l",
        "info",
        "-Q",
        q,
        "-n",
        nodename,
    ]

    sys.exit(main())
