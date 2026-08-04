from fastapi import APIRouter
from pydantic import BaseModel
from simhash import Simhash
from db.maria_operations import maria_execute_query
import re, unicodedata

router = APIRouter(prefix="/backend/simhash", tags=["simhash"])
class Payload(BaseModel):
    db_name: str
    chat_bot_id: str
    subject: str | None = None
    content: str | None = None

def make_hash(subject, content):
    if not subject or not content: return None
    clean=lambda x: re.sub(r"\s+", " ", unicodedata.normalize("NFC",x)).strip()
    return f"{Simhash(clean(subject).split(),f=128).value ^ Simhash(clean(content).split(),f=128).value:032x}"

@router.post("/check")
async def check(payload: Payload):
    value=make_hash(payload.subject,payload.content)
    if value is None: return {"duplicate":False,"save":False,"hash":None}
    tail=payload.chat_bot_id.rsplit("-",1)[-1].lower()
    if not re.fullmatch(r"[a-z0-9]{12}",tail): return {"duplicate":False,"save":False,"hash":value}
    try:
        rows=await maria_execute_query(f"SELECT 1 FROM `ASADAL_{tail}_LEARN_LIST` WHERE `hash`=%s LIMIT 1",(value,),fetch=True,dbname=payload.db_name)
        duplicate=bool(rows); return {"duplicate":duplicate,"save":not duplicate,"hash":value}
    except Exception as error:
        if "unknown column" in str(error).lower() and "hash" in str(error).lower(): return {"duplicate":False,"save":True,"hash":value}
        raise
