import os
import subprocess
import sys
import shutil
import importlib.util
import shlex
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from db.db_operations import insert_data, delete_data
from backend.config import Config
from utils.logging_util import LoggerSingleton
import logging
from utils.llm_agent import clean_data_with_llm
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
import tempfile
import asyncio
from contextlib import closing
from typing import List
from edu.file_text_extract_timeout import await_file_text_extract


# from db.db_redis import job_manager, job_progress_manager
from db.db_job_managers import AsyncJobManager, AsyncJobProgress


# 濡쒓굅 ?ㅼ젙
logger = LoggerSingleton.get_logger(logger_name="edu.hwp", level=logging.INFO)


def _configure_hwp5_third_party_logging() -> None:
    """
    hwp5媛 ?????녿뒗 UnderlineStyle 媛??깆쑝濡?WARNING???먯＜ ?몃떎(?쒖뺨 ?꾩슜 ?띿꽦쨌踰꾩쟾 李⑥씠).
    湲곕낯? ?대떦 濡쒓렇瑜?ERROR ?댁긽留?蹂댁씠寃???肄섏넄 ?몄씠利덈? 以꾩씤??
    ?곸꽭 寃쎄퀬瑜?蹂대젮硫?HWP5_LOG_DATAIO_WARNINGS=1.
    ?쇰? 諛고룷蹂몄? __name__??hwp5.dataio媛 ?꾨땲??hwp5.binmodel.dataio ?깆씠???⑦궎吏 ?⑥쐞濡?留욎텣??
    """
    try:
        if str(os.getenv("HWP5_LOG_DATAIO_WARNINGS", "0")).strip().lower() in ("1", "true", "yes", "on"):
            return
        for _name in (
            "hwp5",
            "hwp5.dataio",
            "hwp5.binmodel",
            "hwp5.binmodel.dataio",
            "hwp5.xmlmodel",
            "hwp5.storage",
        ):
            logging.getLogger(_name).setLevel(logging.ERROR)
    except Exception:
        pass


_configure_hwp5_third_party_logging()

embedding_model = OpenAIEmbeddings(openai_api_key=Config.OPENAI_API_KEY)
FILE_EMBEDDING_BATCH_SIZE = max(1, int(getattr(Config, "FILE_EMBEDDING_BATCH_SIZE", 5) or 5))
FILE_TABLE_EMBEDDING_BATCH_SIZE = max(
    1,
    int(getattr(Config, "FILE_TABLE_EMBEDDING_BATCH_SIZE", 3) or 3),
)

############### HWP LOGIC ###########################


def detect_hwp_version(file_path):
    """
    ?뚯씪??HWP 踰꾩쟾??媛먯??섎뒗 ?⑥닔?낅땲??
    ?뚯씪??留ㅼ쭅 ?섎쾭瑜??뺤씤?섏뿬 HWP2, HWP3, HWP5瑜??먮퀎?⑸땲??
    """
    with open(file_path, "rb") as f:
        header = f.read(8)  # HWP5??8諛붿씠?? HWP2/3? ??4諛붿씠???뺤씤

    # HWP5: OLE Compound File 留ㅼ쭅 ?섎쾭濡??쒖옉
    if header.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "HWP5"
    # HWP3: ASCII 臾몄옄??"HWP3"濡??쒖옉
    elif header[:4] == b"HWP3":
        return "HWP3"
    # HWP2: ASCII 臾몄옄??"HWP2"濡??쒖옉
    elif header[:4] == b"HWP2":
        return "HWP2"
    else:
        return None  # ?????녿뒗 ?뚯씪 ?뺤떇


def _decode_subprocess_bytes(data) -> str:
    """hwp5txt ?깆씠 UTF-8???꾨땶 cp949濡?異쒕젰?섎뒗 ?섍꼍(?쒓뎅??Windows) ???"""
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    if not data:
        return ""
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def run_txt_command(cmd):
    """
    HWP ?띿뒪??蹂??而ㅻ㎤?쒕? ?ㅽ뻾?섍퀬 stdout??諛섑솚?쒕떎.
    - cmd: list[str] (shell=False)
    """
    result = subprocess.run(
        cmd,
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        check=True,
    )
    return _decode_subprocess_bytes(result.stdout or b"")


def _stderr_preview(err) -> str:
    s = _decode_subprocess_bytes(err).strip() if err else ""
    return s if s else "(stderr ?놁쓬)"


def retry_with_temp_copy_and_run(cmd, original_path):
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".hwp") as tmp:
            shutil.copyfile(original_path, tmp.name)
            tmp_path = tmp.name
        tmp_cmd = [tmp_path if arg == original_path else arg for arg in cmd]
        return run_txt_command(tmp_cmd)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def _build_hwp_txt_command(version: str, hwp_file_path: str) -> List[str]:
    """
    ?섍꼍???곕씪 HWP ?띿뒪??蹂??而ㅻ㎤?쒕? 援ъ꽦?쒕떎.
    ?곗꽑?쒖쐞:
    1) ?쒖뒪??PATH???덈뒗 hwp5txt/hwp3txt/hwp2txt ?ㅽ뻾 ?뚯씪
    2) ?뚯씠??紐⑤뱢 ?ㅽ뻾: python -m hwp5.hwp5txt (venv???ㅼ튂?쇱엳?쇰㈃ Windows?먯꽌??媛??
    """
    exe_map = {"HWP5": "hwp5txt", "HWP3": "hwp3txt", "HWP2": "hwp2txt"}
    module_map = {"HWP5": "hwp5.hwp5txt", "HWP3": "hwp5.hwp3txt", "HWP2": "hwp5.hwp2txt"}

    exe = exe_map.get(version)
    if not exe:
        raise ValueError("Unsupported HWP file container")

    if shutil.which(exe):
        return [exe, hwp_file_path]

    mod = module_map.get(version)
    if mod and importlib.util.find_spec(mod) is not None:
        return [sys.executable, "-m", mod, hwp_file_path]

    # 留덉?留?fallback: ?쇰? ?섍꼍?먯꽌 紐⑤뱢紐낆씠 hwp5txt 濡??몄텧?????덉쓬
    if version == "HWP5" and importlib.util.find_spec("hwp5txt") is not None:
        return [sys.executable, "-m", "hwp5txt", hwp_file_path]

    raise FileNotFoundError(
        f"{exe} ?ㅽ뻾 ?뚯씪??李얠쓣 ???놁뒿?덈떎. (exit status 127)\n"
        f"- ?쒕쾭??{exe}媛 ?ㅼ튂?섏뼱 PATH???≫??덉뼱???⑸땲??\n"
        f"- ?먮뒗 Python ?⑦궎吏 hwp5瑜??ㅼ튂????venv?먯꽌 ?ㅽ뻾?섎룄濡??ㅼ젙?섏꽭??(?? pip install hwp5).\n"
        f"- ?뚯씪: {hwp_file_path}"
    )


def _hwp_to_hwpx_fallback_enabled() -> bool:
    return str(os.getenv("HWP_TO_HWPX_FALLBACK", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _hwp_to_hwpx_timeout_sec() -> int:
    try:
        value = int(os.getenv("HWP_TO_HWPX_TIMEOUT_SEC", "120") or "120")
    except Exception:
        value = 120
    return max(10, min(value, 1800))


def _find_soffice_for_hwp_conversion() -> str | None:
    candidates = [
        os.environ.get("LIBREOFFICE_PATH"),
        r"C:\Program Files\LibreOffice\program\soffice.exe",
        r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
        shutil.which("soffice"),
        shutil.which("libreoffice"),
    ]
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return candidate
    return None


def _run_hwp_to_hwpx_custom_command(hwp_file_path: str, out_dir: str) -> str | None:
    template = str(os.getenv("HWP_TO_HWPX_COMMAND") or "").strip()
    if not template:
        return None
    output_path = os.path.join(
        out_dir,
        os.path.splitext(os.path.basename(hwp_file_path))[0] + ".hwpx",
    )
    try:
        rendered = template.format(
            input=hwp_file_path,
            output=output_path,
            output_dir=out_dir,
        )
        cmd = shlex.split(rendered, posix=(os.name != "nt"))
        if not cmd:
            return None
        result = subprocess.run(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=_hwp_to_hwpx_timeout_sec(),
            cwd=os.path.dirname(os.path.abspath(hwp_file_path)) or None,
        )
        if result.returncode != 0:
            logger.warning(
                "[HWP->HWPX] custom converter failed exit=%s stderr=%s file=%s",
                result.returncode,
                (result.stderr or "").strip()[:500],
                hwp_file_path,
            )
            return None
        if os.path.isfile(output_path):
            return output_path
        produced = [
            os.path.join(out_dir, name)
            for name in os.listdir(out_dir)
            if name.lower().endswith(".hwpx")
        ]
        return produced[0] if produced else None
    except Exception as exc:
        logger.warning("[HWP->HWPX] custom converter error | file=%s err=%s", hwp_file_path, exc)
        return None


def _run_hwp_to_hwpx_libreoffice(hwp_file_path: str, out_dir: str) -> str | None:
    soffice = _find_soffice_for_hwp_conversion()
    if not soffice:
        return None
    try:
        result = subprocess.run(
            [
                soffice,
                "--headless",
                "--convert-to",
                "hwpx",
                "--outdir",
                out_dir,
                os.path.abspath(hwp_file_path),
            ],
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=_hwp_to_hwpx_timeout_sec(),
            cwd=os.path.dirname(os.path.abspath(hwp_file_path)) or None,
        )
        if result.returncode != 0:
            logger.warning(
                "[HWP->HWPX] LibreOffice failed exit=%s stderr=%s file=%s",
                result.returncode,
                (result.stderr or "").strip()[:500],
                hwp_file_path,
            )
            return None
        expected = os.path.join(
            out_dir,
            os.path.splitext(os.path.basename(hwp_file_path))[0] + ".hwpx",
        )
        if os.path.isfile(expected):
            return expected
        produced = [
            os.path.join(out_dir, name)
            for name in os.listdir(out_dir)
            if name.lower().endswith(".hwpx")
        ]
        return produced[0] if produced else None
    except Exception as exc:
        logger.warning("[HWP->HWPX] LibreOffice error | file=%s err=%s", hwp_file_path, exc)
        return None


def _extract_text_via_temporary_hwpx(hwp_file_path: str, *, reason: str) -> str:
    if not _hwp_to_hwpx_fallback_enabled():
        return ""
    out_dir = tempfile.mkdtemp(prefix="hwp2hwpx_")
    try:
        converted = _run_hwp_to_hwpx_custom_command(hwp_file_path, out_dir)
        converter = "custom"
        if not converted:
            converted = _run_hwp_to_hwpx_libreoffice(hwp_file_path, out_dir)
            converter = "libreoffice"
        if not converted or not os.path.isfile(converted):
            logger.info("[HWP->HWPX] fallback unavailable | reason=%s file=%s", reason, hwp_file_path)
            return ""
        plain = _hwpx_zip_to_plain_text_crawl(converted)
        if plain.strip():
            logger.info(
                "[HWP->HWPX] fallback text extracted | converter=%s reason=%s chars=%s file=%s",
                converter,
                reason,
                len(plain.strip()),
                hwp_file_path,
            )
            return plain
        logger.warning(
            "[HWP->HWPX] converted but text empty | converter=%s reason=%s hwpx=%s file=%s",
            converter,
            reason,
            converted,
            hwp_file_path,
        )
        return ""
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


def _hwp5_tag_local(tag) -> str:
    if not tag or not isinstance(tag, str):
        return ""
    if tag[0] == "{":
        return tag.split("}", 1)[-1]
    return tag


def _hwp5_paragraph_visible_text(paragraph_el: ET.Element) -> str:
    """LineSeg/Text 寃쎈줈留??ъ슜 ????? Paragraph??蹂꾨룄 ?몃뱶濡?泥섎━, 諛붽묑 臾몃떒怨??욎씠吏 ?딆쓬."""
    chunks = []
    for child in paragraph_el:
        if _hwp5_tag_local(child.tag) != "LineSeg":
            continue
        for node in child.iter():
            if _hwp5_tag_local(node.tag) == "Text" and node.text:
                chunks.append(node.text)
    if not chunks:
        for child in paragraph_el:
            if _hwp5_tag_local(child.tag) == "Text" and child.text:
                chunks.append(child.text)
    return "".join(chunks).strip()


def _hwp5_extract_text_via_xmlevents(hwp_file_path: str) -> str:
    """
    hwp5txt(plaintext.xsl)??TableControl ?대???apply-templates瑜??섏? ?딆븘
    ?쑣룹젏寃???꾩＜ 臾몄꽌媛 鍮?臾몄옄?댁씠 ?섎뒗 寃쎌슦媛 ?덈떎.
    Hwp5File xmlevents XML?먯꽌 Paragraph/Text瑜?吏곸젒 紐⑥???
    """
    _configure_hwp5_third_party_logging()
    try:
        from hwp5.xmlmodel import Hwp5File
    except ImportError:
        return ""
    lines = []
    try:
        with closing(Hwp5File(hwp_file_path)) as hwp:
            buf = BytesIO()
            hwp.xmlevents(embedbin=False).dump(buf)
            data = buf.getvalue()
        root = ET.fromstring(data)
    except Exception as e:
        logger.debug("HWP5 xmlevents ?대갚 ?뚯떛 ?ㅽ뙣: %s", e, exc_info=True)
        return ""
    for el in root.iter():
        if _hwp5_tag_local(el.tag) != "Paragraph":
            continue
        t = _hwp5_paragraph_visible_text(el)
        if t:
            lines.append(t)
    return "\n".join(lines)


def _hwp5_extract_preview_text(hwp_file_path: str) -> str:
    """HWP5 본문 추출이 모두 실패할 때 PrvText 미리보기 스트림을 마지막 fallback으로 사용한다."""
    try:
        import olefile  # type: ignore
    except Exception:
        return ""
    try:
        if not olefile.isOleFile(hwp_file_path):
            return ""
        with olefile.OleFileIO(hwp_file_path) as ole:
            if not ole.exists("PrvText"):
                return ""
            data = ole.openstream("PrvText").read()
    except Exception as exc:
        logger.debug("HWP PrvText fallback 읽기 실패 | file=%s error=%s", hwp_file_path, exc)
        return ""
    try:
        text = data.decode("utf-16le", errors="ignore")
    except Exception:
        return ""
    cleaned_lines = []
    for raw_line in str(text or "").replace("\r", "\n").split("\n"):
        line = " ".join(str(raw_line or "").split()).strip()
        if line:
            cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines).strip()
    if len(cleaned) < int(os.getenv("HWP_PRVTEXT_FALLBACK_MIN_CHARS", "80") or "80"):
        return ""
    return cleaned

def hwp_to_text(hwp_file_path):
    """HWP 파일을 텍스트로 변환한다."""
    _configure_hwp5_third_party_logging()
    if not os.path.isfile(hwp_file_path):
        raise FileNotFoundError(f"File not found: {hwp_file_path}")

    version = detect_hwp_version(hwp_file_path)
    if version not in ("HWP5", "HWP3", "HWP2"):
        if _looks_like_hwpx_zip(hwp_file_path):
            plain = _hwpx_zip_to_plain_text_crawl(hwp_file_path)
            if plain:
                logger.info("HWP 확장자의 HWPX ZIP 본문 텍스트를 추출했습니다 | file=%s", hwp_file_path)
                return plain
            logger.warning("HWPX ZIP으로 감지됐지만 본문 텍스트가 비어 있습니다 | file=%s", hwp_file_path)
            return ""
        if _looks_like_hwpml_xml(hwp_file_path):
            plain = _hwpml_file_to_plain_text(hwp_file_path)
            if plain.strip():
                logger.info("HWP 확장자의 HWPML XML 본문 텍스트를 추출했습니다 | file=%s", hwp_file_path)
                return plain
            logger.warning("HWPML XML로 감지됐지만 본문 텍스트가 비어 있습니다 | file=%s", hwp_file_path)
            return ""
        raise ValueError(
            "Unsupported HWP file container. Only binary HWP5, HWPX(ZIP), and HWPML(XML) are supported. "
            "Re-save the document as HWP/HWPX from Hancom Office and upload again."
        )

    try:
        cmd = _build_hwp_txt_command(version, hwp_file_path)
    except FileNotFoundError as exc:
        converted_plain = _extract_text_via_temporary_hwpx(
            hwp_file_path,
            reason="hwp_text_command_missing",
        )
        if converted_plain.strip():
            return converted_plain
        raise RuntimeError(str(exc))

    try:
        primary = run_txt_command(cmd)
    except FileNotFoundError as exc:
        converted_plain = _extract_text_via_temporary_hwpx(
            hwp_file_path,
            reason="hwp_text_command_missing_at_runtime",
        )
        if converted_plain.strip():
            return converted_plain
        raise RuntimeError(str(exc))
    except subprocess.CalledProcessError as first_exc:
        try:
            primary = retry_with_temp_copy_and_run(cmd, hwp_file_path)
        except subprocess.CalledProcessError as retry_exc:
            converted_plain = _extract_text_via_temporary_hwpx(
                hwp_file_path,
                reason="hwp_text_command_failed",
            )
            if converted_plain.strip():
                return converted_plain
            stderr1 = _stderr_preview(getattr(first_exc, "stderr", None))
            stderr2 = _stderr_preview(getattr(retry_exc, "stderr", None))
            raise RuntimeError(
                f"HWP text extraction failed. Re-save the file from Hancom Office and upload again.\n"
                f"[primary_error]: {stderr1}\n"
                f"[retry_error]: {stderr2}"
            )

    if version == "HWP5":
        alt = _hwp5_extract_text_via_xmlevents(hwp_file_path)
        if alt.strip():
            primary_text = (primary or "").strip()
            alt_text = alt.strip()
            if not primary_text:
                logger.info("HWP5 hwp5txt 결과가 비어 xmlevents 텍스트를 사용합니다 | file=%s", hwp_file_path)
                return alt
            if len(alt_text) > len(primary_text) + 400:
                logger.info(
                    "HWP5 xmlevents 텍스트가 더 충분해 대체합니다 | hwp5txt_len=%s xmlevents_len=%s file=%s",
                    len(primary_text),
                    len(alt_text),
                    hwp_file_path,
                )
                return alt

    out = (primary or "").strip()
    if not out:
        logger.warning("HWP 텍스트 추출 결과가 비어 있습니다 | version=%s file=%s", version, hwp_file_path)
        converted_plain = _extract_text_via_temporary_hwpx(
            hwp_file_path,
            reason="hwp_text_empty",
        )
        if converted_plain.strip():
            return converted_plain
        preview_plain = _hwp5_extract_preview_text(hwp_file_path)
        if preview_plain.strip():
            logger.warning(
                "HWP 본문 추출이 비어 PrvText 미리보기 텍스트로 대체합니다 | chars=%s file=%s",
                len(preview_plain.strip()),
                hwp_file_path,
            )
            return preview_plain
    return primary

# ??HWP 蹂묐젹 泥섎━ ?⑥닔??異붽? (URL 泥섎━? ?숈씪???⑦꽩)
async def process_single_chunk_async_hwp(
    chunk: str,
    chunk_idx: int,
    content: str,
    table_name: str,
    dbname: str,
    memo: str,
    job_id: str,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress = None,
    chunk_progress: float = 0.0,
    content_type: str = "file",
    subject: str = "",
):
    """媛쒕퀎 HWP 泥?겕瑜?鍮꾨룞湲곕줈 泥섎━ (?꾨쿋??+ ???+ ?ㅼ떆媛?吏꾪뻾瑜??낅뜲?댄듃)"""
    try:
        # ??泥?겕 泥섎━ ??痍⑥냼 ?곹깭 ?뺤씤
        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"HWP 媛쒕퀎 泥?겕 泥섎━ 以??묒뾽 痍⑥냼?? job_id={job_id}, chunk={chunk_idx}")
            return {"status": "cancelled", "chunk_idx": chunk_idx}
        
        # ?????곗씠?곗씤 寃쎌슦 ?ㅻⅨ chunk_num ?뺤떇 ?ъ슜
        if content_type == "table":
            chunk_num = f"Table_{chunk_idx}"
            # ???곗씠?곕뒗 ?대? 硫뷀??곗씠?곌? ?ы븿?섏뼱 ?덉쓬
            chunk_with_metadata = chunk
        else:
            chunk_num = str(chunk_idx)
            chunk_with_metadata = chunk
        
        # ??鍮꾨룞湲??꾨쿋???앹꽦
        embedding = await embedding_model.aembed_query(chunk_with_metadata)
        embedding_array = f"[{','.join(map(str, embedding))}]"

        # ??DB ???????踰???痍⑥냼 ?곹깭 ?뺤씤
        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"HWP DB ??????묒뾽 痍⑥냼?? job_id={job_id}, chunk={chunk_idx}")
            return {"status": "cancelled", "chunk_idx": chunk_idx}

        # Async DB save payload.
        data = {
            "content": content,
            "chunk_num": chunk_num,
            "memo": memo,
            "content_type": content_type,
            "text_data": chunk_with_metadata,
            "embedding": embedding_array,
        }
        if subject:
            data["subject"] = subject
        await insert_data(table=table_name, data=data, dbname=dbname)

        # ??泥?겕 ????꾨즺 ???ㅼ떆媛?吏꾪뻾瑜??낅뜲?댄듃
        if job_progress_manager and chunk_progress > 0:
            current_progress = await job_progress_manager.get_job_progress(job_id)
            new_progress = round(min(current_progress + chunk_progress, 99.99), 2)
            await job_progress_manager.set_job_progress(job_id, new_progress)

            logger.debug(f"[HWP 泥?겕 吏꾪뻾瑜??낅뜲?댄듃] job_id={job_id}, chunk={chunk_idx}, progress={new_progress}%")

        return {"status": "success", "chunk_idx": chunk_idx}

    except Exception as e:
        logger.error(f"HWP 泥?겕 泥섎━ ?ㅻ쪟 ({content}, chunk {chunk_idx}): {e}")
        return {"status": "error", "chunk_idx": chunk_idx, "error": str(e)}


async def process_chunks_parallel_hwp(
    chunks: List[str],
    content: str,
    table_name: str,
    dbname: str,
    job_id: str,
    job_manager: AsyncJobManager,
    memo: str,
    job_progress_manager: AsyncJobProgress = None,
    chunk_progress: float = 0.0,
    batch_size: int = 5,
    content_type: str = "file",
    personal_info_filter: str = "N",  # 媛쒖씤?뺣낫 ?꾪꽣留??듭뀡 異붽?
    subject: str = "",
):
    """HWP 泥?겕?ㅼ쓣 諛곗튂 ?⑥쐞濡?蹂묐젹 泥섎━ (?ㅼ떆媛?吏꾪뻾瑜??낅뜲?댄듃)"""
    
    # 諛곗튂蹂꾨줈 泥?겕 泥섎━
    for i in range(0, len(chunks), batch_size):
        # 痍⑥냼 ?뺤씤
        status = await job_manager.get_job_status(job_id)
        if status == "cancel":
            logger.info(f"HWP 泥?겕 泥섎━ 以??묒뾽 痍⑥냼?? job_id={job_id}")
            return

        batch_chunks = chunks[i:i + batch_size]
        batch_tasks = []

        # 諛곗튂 ??蹂묐젹 泥섎━
        for chunk_idx, chunk in enumerate(batch_chunks, start=i + 1):
            if personal_info_filter == "Y":
                logger.info(f"HWP 媛쒕퀎 泥?겕 泥섎━ 以?媛쒖씤?뺣낫 ?꾪꽣留??곸슜: job_id={job_id}, chunk={chunk_idx}")
                from utils.dlp_api import check_pii_content
                original_chunk = chunk
                pii_result = check_pii_content(chunk)
                
                # 留덉뒪?밸맂 ?띿뒪?몃? chunk 蹂?섏뿉 ?좊떦
                if pii_result["success"]:
                    chunk = pii_result["masked_text"]
                    
                    # ??媛쒖씤?뺣낫媛 媛먯???寃쎌슦?먮쭔 ?뱀냼耳볦쑝濡??꾩넚
                    pass
                else:
                    # PII 寃???ㅽ뙣 ???먮윭 濡쒓퉭
                    logger.error(f"PII 寃???ㅽ뙣: {pii_result.get('error', 'Unknown error')}")
                    # ?ㅽ뙣 ???먮낯 ?띿뒪???좎?
            task = process_single_chunk_async_hwp(
                chunk=chunk,
                chunk_idx=chunk_idx,
                content=content,
                table_name=table_name,
                dbname=dbname,
                memo=memo,
                job_id=job_id,
                job_manager=job_manager,
                job_progress_manager=job_progress_manager,
                chunk_progress=chunk_progress,
                content_type=content_type,
                subject=subject,
            )
            batch_tasks.append(task)

        # 諛곗튂 ?ㅽ뻾
        try:
            await asyncio.gather(*batch_tasks, return_exceptions=True)
            logger.debug(f"HWP 諛곗튂 泥섎━ ?꾨즺: {content}, 泥?겕 {i+1}-{min(i+batch_size, len(chunks))}")
        except Exception as e:
            logger.error(f"HWP 諛곗튂 泥섎━ 以??ㅻ쪟: {e}")


async def process_hwp(
    content: str,
    file_path: str,
    content_type: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    memo: str = "",
    personal_info_filter: str = "N",  # 媛쒖씤?뺣낫 ?꾪꽣留??듭뀡 異붽?
    subject: str = "",
    **kwargs,
):
    """
    HWP ?뚯씪??蹂묐젹 泥섎━?섏뿬 ?띿뒪?몃? 異붿텧?섍퀬 踰≫꽣?뷀븳 ???곗씠?곕쿋?댁뒪????ν빀?덈떎.
    (learn_modules ?깆뿉??content_type, subject ??異붽? ?몄옄濡??몄텧?????덈룄濡?**kwargs ?섏슜)
    """
    try:
        # ??鍮꾨룞湲곕줈 HWP ?뚯씪 ?띿뒪??蹂???ㅽ뻾
        all_text = await await_file_text_extract(
            asyncio.to_thread(hwp_to_text, file_path),
            path=file_path,
            stage="hwp",
            logger=logger,
        )

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(all_text or "")
        if not chunks:
            logger.warning(
                f"HWP?먯꽌 異붿텧??蹂몃Ц???놁뒿?덈떎(鍮?臾몄꽌쨌?대?吏 ?꾩슜쨌?뷀샇쨌?먯긽 媛??: {file_path}"
            )
            chunks = [
                "[HWP?먯꽌 ?띿뒪?몃? 異붿텧?섏? 紐삵뻽?듬땲?? ?쒓??먯꽌 ?ㅼ떆 ??ν븯嫄곕굹 ?뷀샇쨌?뚯씪 ?뺤떇???뺤씤?섏꽭??]"
            ]
        total_chunks = len(chunks)

        # ??main.py?먯꽌 ?대? 怨꾩궛??泥?겕蹂?吏꾪뻾瑜좎쓣 洹몃?濡??ъ슜
        chunk_progress = each_progress

        logger.info(
            f"[HWP 蹂묐젹 泥섎━ ?쒖옉] content: {content}, total_chunk: {total_chunks}, "
            f"泥?겕蹂?吏꾪뻾瑜? {chunk_progress}%, job_id: {job_id}"
        )

        # ??泥?겕?ㅼ쓣 諛곗튂 ?⑥쐞濡?蹂묐젹 泥섎━ (?ㅼ떆媛?吏꾪뻾瑜??낅뜲?댄듃)
        await process_chunks_parallel_hwp(
            chunks=chunks,
            content=content,
            table_name=table_name,
            dbname=dbname,
            job_id=job_id,
            job_manager=job_manager,
            memo=memo,
            job_progress_manager=job_progress_manager,
            chunk_progress=chunk_progress,
            batch_size=FILE_EMBEDDING_BATCH_SIZE,
            content_type=content_type or "file",
            personal_info_filter=personal_info_filter,  # 媛쒖씤?뺣낫 ?꾪꽣留??듭뀡 ?꾨떖
            subject=subject or os.path.basename(file_path or "") or content,
        )

        logger.info(
            f"[HWP 蹂묐젹 泥섎━ ?꾨즺] content: {content}, total_chunk: {total_chunks}, job_id: {job_id}"
        )
        return {
            "status": "success", 
            "message": f"{content} 泥섎━ ?꾨즺", 
            "chunks": total_chunks,
            "chunk_count": [total_chunks],
            "use_source": [content]
        }

    except Exception as e:
        # hwp5 誘몄꽕移??ㅽ뻾?꾧뎄 誘몄〈???깆? ?섍꼍 臾몄젣濡??뷀엳 諛쒖깮?????덈떎.
        # ?щ·留??숈뒿 ?꾩껜瑜?以묐떒?쒗궎吏 ?딄퀬 ?대떦 ?뚯씪留?error 泥섎━?쒕떎.
        msg = str(e)
        if "No module named 'hwp5'" in msg or "No module named \"hwp5\"" in msg or "hwp5txt" in msg:
            hint = (
                "HWP 泥섎━ 遺덇?(?섏〈???꾨씫): ?쒕쾭??hwp5(?먮뒗 hwp5txt)媛 ?ㅼ튂?섏뼱???⑸땲?? "
                "?? `pip install hwp5` ?먮뒗 ?쒖뒪?쒖뿉 hwp5txt ?ㅼ튂"
            )
            logger.warning(f"[HWP] {hint} | file={file_path} err={e}", exc_info=True)
            return {
                "status": "error",
                "message": hint,
                "chunks": 0,
                "chunk_count": [0],
                "use_source": [content],
            }

        logger.error(f"HWP 泥섎━ 以??ㅻ쪟 諛쒖깮: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"HWP 泥섎━ 以??ㅻ쪟 諛쒖깮: {e}",
            "chunks": 0,
            "chunk_count": [0],
            "use_source": [content],
        }


############### HWPX LOGIC ###########################


def get_dynamic_namespaces(xml_data):
    """XML ?곗씠?곗뿉???ㅼ엫?ㅽ럹?댁뒪瑜??숈쟻?쇰줈 異붿텧?⑸땲??"""
    namespaces = {}
    
    try:
        if not xml_data or len(xml_data.strip()) == 0:
            logger.warning("鍮?XML ?곗씠?곌? ?쒓났?섏뿀?듬땲??")
            return get_default_namespaces()
        
        # XML ?곗씠?곌? ?좏슚?쒖? 癒쇱? ?뺤씤
        try:
            ET.fromstring(xml_data)
        except ET.ParseError as e:
            logger.warning(f"XML ?곗씠?곌? ?좏슚?섏? ?딆뒿?덈떎: {e}")
            return get_default_namespaces()
        
        # ?ㅼ엫?ㅽ럹?댁뒪 異붿텧 ?쒕룄
        for event, elem in ET.iterparse(BytesIO(xml_data), events=("start-ns",)):
            prefix, uri = elem
            if prefix not in namespaces:
                namespaces[prefix] = uri
                
    except Exception as e:
        logger.warning(f"?ㅼ엫?ㅽ럹?댁뒪 異붿텧 以??ㅻ쪟 諛쒖깮: {e}")
        return get_default_namespaces()

    if not namespaces:
        return get_default_namespaces()

    merged = dict(get_default_namespaces())
    merged.update(namespaces)
    return merged


def get_default_namespaces():
    """湲곕낯 HWP ?ㅼ엫?ㅽ럹?댁뒪瑜?諛섑솚?⑸땲??"""
    return {
        "hp": "http://www.hancom.co.kr/hwpml/2011/paragraph",
        "hs": "http://www.hancom.co.kr/hwpml/2011/section",
        "hc": "http://www.hancom.co.kr/hwpml/2011/core",
    }


def _xml_local_name(tag) -> str:
    """{http://...}p ?뺥깭???쒓렇?먯꽌 濡쒖뺄 ?대쫫留?諛섑솚."""
    return _hwp5_tag_local(tag)


def _looks_like_hwpml_xml(file_path: str) -> bool:
    """
    ?뺤옣??.hwp ?몃뜲 ?ㅼ젣??HWPML(XML)??寃쎌슦(踰뺣졊쨌怨듦났 HWP ?ㅼ슫濡쒕뱶 ??.
    OLE Compound / HWPX(ZIP) 媛 ?꾨땲??
    """
    try:
        with open(file_path, "rb") as f:
            sniff = f.read(1024)
    except OSError:
        return False
    if sniff.startswith(b"\xef\xbb\xbf"):
        sniff = sniff[3:]
    sniff = sniff.lstrip()
    if not sniff.startswith(b"<?xml"):
        return False
    low = sniff.decode("utf-8", errors="ignore").lower()
    return "hwpml" in low


def _hwpml_file_to_plain_text(file_path: str) -> str:
    """HWPML 2.x XML?먯꽌 蹂몃Ц(BODY) 臾몃떒??CHAR ?띿뒪?몃? ?쒖꽌?濡?紐⑥???"""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        logger.debug("HWPML XML ?뚯떛 ?ㅽ뙣: %s", e)
        return ""
    except Exception as e:
        logger.debug("HWPML ?뚯씪 ?쎄린 ?ㅽ뙣: %s", e)
        return ""

    body = None
    for el in root.iter():
        if _xml_local_name(el.tag).upper() == "BODY":
            body = el
            break
    scan_root = body if body is not None else root

    lines: List[str] = []
    for p in scan_root.iter():
        if _xml_local_name(p.tag).upper() != "P":
            continue
        parts: List[str] = []
        for node in p.iter():
            if node is p:
                continue
            if _xml_local_name(node.tag).upper() == "CHAR" and node.text:
                parts.append(node.text)
        line = (
            "".join(parts)
            .replace("\r", "\n")
            .replace("\xa0", " ")
            .strip()
        )
        if line:
            lines.append(line)
    return "\n".join(lines).strip()


def _build_parent_map(root: ET.Element) -> dict:
    pmap = {}
    for parent in root.iter():
        for child in list(parent):
            pmap[child] = parent
    return pmap


def _has_p_ancestor(elem: ET.Element, pmap: dict) -> bool:
    cur = pmap.get(elem)
    while cur is not None:
        if _xml_local_name(cur.tag) == "p":
            return True
        cur = pmap.get(cur)
    return False


def _collect_hwpx_paragraphs(root: ET.Element) -> List[str]:
    """
    hp: ?묐몢쨌?ㅽ궎留??곕룄(2011/2016 ??? 臾닿??섍쾶 臾몃떒(p) ?띿뒪?몃? ?섏쭛.
    以묒꺽 p??諛붽묑 臾몃떒留??ъ슜???숈씪 蹂몃Ц 以묐났??以꾩엫.
    """
    pmap = _build_parent_map(root)
    out: List[str] = []
    for el in root.iter():
        if _xml_local_name(el.tag) != "p":
            continue
        if _has_p_ancestor(el, pmap):
            continue
        try:
            text = "".join(el.itertext()).strip()
        except Exception as e:
            logger.warning(f"HWPX 臾몃떒 itertext ?ㅽ뙣: {e}")
            continue
        if text:
            out.append(text)
    return out


def _append_hwpx_table_chunks(
    tables: List[dict],
    rows_data: List[List[str]],
    table_id: str,
    table_chunk_size: int,
) -> None:
    if len(rows_data) <= 10:
        tables.append({"table_id": table_id, "header": None, "rows": rows_data})
        return
    header_data = "\n".join([" | ".join(row) for row in rows_data[:5]])
    llm_header = header_data
    if str(os.getenv("HWPX_TABLE_HEADER_LLM_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            llm_header = clean_data_with_llm(header_data)
        except Exception as exc:
            logger.debug("HWPX table header LLM cleanup skipped | table_id=%s err=%s", table_id, exc)
            llm_header = header_data
    chunked_tables = []
    for i in range(0, len(rows_data), table_chunk_size):
        chunk = rows_data[i : i + table_chunk_size]
        chunked_tables.append(
            {"table_id": table_id, "header": llm_header, "rows": chunk}
        )
    tables.extend(chunked_tables)


def _collect_hwpx_tables(
    root: ET.Element,
    table_count: int,
    table_chunk_size: int,
    tables: List[dict],
) -> int:
    """濡쒖뺄 ?쒓렇紐?tbl/tr/tc 湲곗??쇰줈 ??異붿텧(hwpml ?ㅼ엫?ㅽ럹?댁뒪 臾닿?)."""
    for tbl in root.iter():
        if _xml_local_name(tbl.tag) != "tbl":
            continue
        table_count += 1
        table_id = f"Table_{table_count}"
        rows_data: List[List[str]] = []
        for tr in tbl.iter():
            if tr is tbl or _xml_local_name(tr.tag) != "tr":
                continue
            row_cells: List[str] = []
            for tc in tr.iter():
                if tc is tr or _xml_local_name(tc.tag) != "tc":
                    continue
                try:
                    row_cells.append("".join(tc.itertext()).strip())
                except Exception as e:
                    logger.warning(f"HWPX ? itertext ?ㅽ뙣: {e}")
                    row_cells.append("")
            if any(row_cells):
                rows_data.append(row_cells)
        try:
            if rows_data:
                _append_hwpx_table_chunks(
                    tables, rows_data, table_id, table_chunk_size
                )
        except Exception as e:
            logger.warning(f"HWPX ??泥섎━ 以??ㅻ쪟: {table_id}, {e}")

    # ODF ?명솚(?쇰? HWPX/蹂대궡湲?: table, table-row, table-cell
    for table in root.iter():
        if _xml_local_name(table.tag) != "table":
            continue
        table_count += 1
        table_id = f"Table_{table_count}"
        rows_data = []
        for tr in table.iter():
            if tr is table or _xml_local_name(tr.tag) != "table-row":
                continue
            row_cells = []
            for tc in tr.iter():
                if tc is tr or _xml_local_name(tc.tag) != "table-cell":
                    continue
                try:
                    row_cells.append("".join(tc.itertext()).strip())
                except Exception as e:
                    logger.warning(f"HWPX ODF ? itertext ?ㅽ뙣: {e}")
                    row_cells.append("")
            if any(row_cells):
                rows_data.append(row_cells)
        try:
            if rows_data:
                _append_hwpx_table_chunks(
                    tables, rows_data, table_id, table_chunk_size
                )
        except Exception as e:
            logger.warning(f"HWPX ODF ??泥섎━ 以??ㅻ쪟: {table_id}, {e}")
    return table_count


def _hwpx_xml_excluded(low_path: str) -> bool:
    low = low_path.replace("\\", "/").lower()
    fragments = (
        "/_rels/",
        "[content_types]",
        "content_types.xml",
        "package/",
        ".rels",
    )
    return any(f in low for f in fragments)


def _hwpx_non_body_basename(filename: str) -> bool:
    """?ㅽ???硫뷀? XML???덈뒗 text:p ?깆씠 蹂몃Ц?쇰줈 ?욎씠吏 ?딅룄濡??쒖쇅."""
    low = filename.lower()
    skip_exact = {
        "styles.xml",
        "settings.xml",
        "meta.xml",
        "version.xml",
    }
    if low in skip_exact:
        return True
    if low.startswith("styles") and low.endswith(".xml"):
        return True
    if low.startswith("theme") and low.endswith(".xml"):
        return True
    return False


def _list_hwpx_body_xmls(namelist) -> List[str]:
    """
    section*.xml ?곗꽑, ?놁쑝硫?Contents/ ?꾨옒 蹂몃Ц ?꾨낫 XML ?꾨?.
    (?쒖뺨쨌蹂대궡湲??꾧뎄蹂?寃쎈줈/?뚯씪紐?李⑥씠 ???
    """
    primary: List[str] = []
    for raw in namelist:
        norm = raw.replace("\\", "/")
        low = norm.lower()
        if not low.endswith(".xml") or not low.startswith("contents/"):
            continue
        if _hwpx_xml_excluded(low):
            continue
        bn = os.path.basename(low)
        if _hwpx_non_body_basename(bn):
            continue
        if (
            bn.startswith("section")
            or bn.startswith("header")
            or bn.startswith("footer")
        ):
            primary.append(raw)
    if primary:
        return sorted(set(primary), key=lambda x: x.replace("\\", "/").lower())
    wide: List[str] = []
    for raw in namelist:
        norm = raw.replace("\\", "/")
        low = norm.lower()
        if not low.endswith(".xml") or not low.startswith("contents/"):
            continue
        if _hwpx_xml_excluded(low):
            continue
        bn = os.path.basename(low)
        if _hwpx_non_body_basename(bn):
            continue
        wide.append(raw)
    return sorted(set(wide), key=lambda x: x.replace("\\", "/").lower())


def _looks_like_hwpx_zip(file_path: str) -> bool:
    """?뺤옣?먭? .hwp?щ룄 ?댁슜??HWPX(OCF ZIP)??寃쎌슦媛 ?덉뼱 蹂몃Ц ZIP ?щ?留??먮퀎."""
    if not zipfile.is_zipfile(file_path):
        return False
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            names = [x.replace("\\", "/").lower() for x in z.namelist()]
    except Exception:
        return False
    if any(n.startswith("contents/") for n in names):
        return True
    if "mimetype" in names:
        try:
            with zipfile.ZipFile(file_path, "r") as z:
                mt = z.read("mimetype").decode("utf-8", errors="replace").strip().lower()
            if "hwp" in mt or "haansoft" in mt:
                return True
        except Exception:
            pass
    return False


def _hwpx_root_plain_segments(root: ET.Element) -> List[str]:
    """?숈뒿???щ· 寃쎈줈: LLM쨌??泥?겕 ?놁씠 臾몃떒+???됰쭔 ?됰Ц?쇰줈."""
    segs: List[str] = []
    segs.extend(_collect_hwpx_paragraphs(root))
    for tbl in root.iter():
        if _xml_local_name(tbl.tag) != "tbl":
            continue
        for tr in tbl.iter():
            if tr is tbl or _xml_local_name(tr.tag) != "tr":
                continue
            row_cells: List[str] = []
            for tc in tr.iter():
                if tc is tr or _xml_local_name(tc.tag) != "tc":
                    continue
                try:
                    row_cells.append("".join(tc.itertext()).strip())
                except Exception:
                    row_cells.append("")
            if any(x.strip() for x in row_cells):
                segs.append(" | ".join(row_cells))
    for table in root.iter():
        if _xml_local_name(table.tag) != "table":
            continue
        for tr in table.iter():
            if tr is table or _xml_local_name(tr.tag) != "table-row":
                continue
            row_cells: List[str] = []
            for tc in tr.iter():
                if tc is tr or _xml_local_name(tc.tag) != "table-cell":
                    continue
                try:
                    row_cells.append("".join(tc.itertext()).strip())
                except Exception:
                    row_cells.append("")
            if any(x.strip() for x in row_cells):
                segs.append(" | ".join(row_cells))
    return segs


def is_encrypted_hwpx(file_path: str) -> bool:
    """HWPX ZIP manifest에 암호화 항목이 있는지 확인한다."""
    if not zipfile.is_zipfile(file_path):
        return False
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            if "META-INF/manifest.xml" not in z.namelist():
                return False
            data = z.read("META-INF/manifest.xml")
            text = data.decode("utf-8", errors="ignore").lower()
            return "encryption-data" in text or "xmlenc#aes" in text or "aes256-cbc" in text
    except Exception:
        return False

def _hwpx_zip_to_plain_text_crawl(file_path: str) -> str:
    """HWPX ZIP?먯꽌 ?됰Ц留?異붿텧. ?ㅽ뙣 ??鍮?臾몄옄??"""
    if not zipfile.is_zipfile(file_path):
        return ""
    all_segs: List[str] = []
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            for xml_path in _list_hwpx_body_xmls(z.namelist()):
                try:
                    with z.open(xml_path) as f:
                        data = f.read()
                    if not data or not data.strip():
                        continue
                    root = ET.fromstring(data)
                except Exception:
                    continue
                all_segs.extend(_hwpx_root_plain_segments(root))
    except Exception:
        return ""
    return "\n".join(all_segs).strip()


def extract_hwpx_data(hwpx_path, table_chunk_size=5):
    """HWPX \ud30c\uc77c\uc5d0\uc11c \ud14d\uc2a4\ud2b8\uc640 \ud45c \ub370\uc774\ud130\ub97c \ucd94\ucd9c\ud569\ub2c8\ub2e4."""
    if is_encrypted_hwpx(hwpx_path):
        logger.warning("HWPX 암호화 패키지라 텍스트 추출을 건너뜁니다 | file=%s", hwpx_path)
        return [], []
    paragraphs = []  # paragraph chunks
    tables = []  # table chunks
    table_count = 0

    logger.debug("HWPX \ub370\uc774\ud130 \ucd94\ucd9c \uc2dc\uc791 | file=%s", hwpx_path)

    try:
        with zipfile.ZipFile(hwpx_path, "r") as hwpx_zip:
            body_xmls = _list_hwpx_body_xmls(hwpx_zip.namelist())
            logger.debug("HWPX \ubcf8\ubb38 XML \ubc1c\uacac | count=%s file=%s", len(body_xmls), hwpx_path)

            for xml_path in body_xmls:
                try:
                    with hwpx_zip.open(xml_path) as f:
                        xml_data = f.read()

                    if not xml_data or len(xml_data.strip()) == 0:
                        logger.warning("HWPX \ube48 XML \ud30c\uc77c \uac74\ub108\ub700 | xml=%s file=%s", xml_path, hwpx_path)
                        continue

                    try:
                        root = ET.fromstring(xml_data)
                    except ET.ParseError as e:
                        logger.warning(
                            "HWPX XML \ud30c\uc2f1 \uc2e4\ud328, \ud30c\uc77c \uac74\ub108\ub700 | xml=%s file=%s error=%s",
                            xml_path,
                            hwpx_path,
                            e,
                        )
                        continue

                except Exception as e:
                    logger.warning("HWPX \ubcf8\ubb38 XML \ucc98\ub9ac \uc624\ub958 | xml=%s file=%s error=%s", xml_path, hwpx_path, e)
                    continue

                paragraphs.extend(_collect_hwpx_paragraphs(root))
                table_count = _collect_hwpx_tables(
                    root, table_count, table_chunk_size, tables
                )

        # \ubcf8\ubb38 \ubb38\ub2e8\uc744 \uc124\uc815\ub41c \uccad\ud06c \ud06c\uae30\ub85c \ub098\ub208\ub2e4.
        all_text = "\n".join(paragraphs)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        text_chunks = text_splitter.split_text(all_text)

        logger.debug(
            "HWPX \ub370\uc774\ud130 \ucd94\ucd9c \uc644\ub8cc | paragraphs=%s tables=%s text_chunks=%s file=%s",
            len(paragraphs),
            len(tables),
            len(text_chunks),
            hwpx_path,
        )

        return text_chunks, tables

    except Exception as e:
        logger.error("HWPX \ub370\uc774\ud130 \ucd94\ucd9c \uc911 \uc624\ub958 \ubc1c\uc0dd | file=%s error=%s", hwpx_path, e, exc_info=True)
        return [], []

async def process_hwpx(
    content: str,
    file_path: str,
    table_name: str,
    dbname: str,
    job_id: str,
    each_progress: float,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    subject: str = "",
    memo: str = "",
    table_chunk_size=5,
    personal_info_filter: str = "N",  # 媛쒖씤?뺣낫 ?꾪꽣留??듭뀡 異붽?
    content_type: str = "file",      # 1. content_type 留ㅺ컻蹂??異붽?
    **kwargs                         # 2. 湲고? ?덉쇅 ?몄옄 泥섎━瑜??꾪븳 媛蹂 ?몄옄 異붽?
):
    """
    HWPX ?뚯씪??泥섎━?섏뿬 臾몃떒怨????곗씠?곕? 遺꾨━, 泥?궧?????곗씠?곕쿋?댁뒪?????    """
    try:
        # HWPX ?곗씠??異붿텧
        extracted_data = await await_file_text_extract(
            asyncio.to_thread(extract_hwpx_data, file_path, table_chunk_size),
            path=file_path,
            stage="hwpx",
            logger=logger,
        )
        text_chunks = extracted_data["content"]
        table_chunks = extracted_data["tables"]

        total_chunks = len(text_chunks) + len(table_chunks)
        if total_chunks == 0:
            text_chunks = ["[??臾몄꽌???띿뒪?몃? ?ы븿?섏? ?딆뒿?덈떎.]"]
            total_chunks = 1

        # ??main.py?먯꽌 ?대? 怨꾩궛??泥?겕蹂?吏꾪뻾瑜좎쓣 洹몃?濡??ъ슜
        chunk_progress = each_progress
        
        logger.info(
            f"[HWPX 蹂묐젹 泥섎━ ?쒖옉] content: {content}, text_chunks: {len(text_chunks)}, "
            f"table_chunks: {len(table_chunks)}, total_chunks: {total_chunks}, "
            f"泥?겕蹂?吏꾪뻾瑜? {chunk_progress}%, job_id: {job_id}"
        )

        # ??臾몃떒 ?곗씠?곕? 諛곗튂 ?⑥쐞濡?蹂묐젹 泥섎━
        if text_chunks:
            await process_chunks_parallel_hwp(
                chunks=text_chunks,
                content=content,
                subject=subject or os.path.basename(file_path or "") or content,
                table_name=table_name,
                dbname=dbname,
                job_id=job_id,
                job_manager=job_manager,
                memo=memo,
                job_progress_manager=job_progress_manager,
                chunk_progress=chunk_progress,
                batch_size=FILE_EMBEDDING_BATCH_SIZE,
                content_type=content_type,
                personal_info_filter=personal_info_filter, # 媛쒖씤?뺣낫 ?꾪꽣留??듭뀡 ?꾨떖
            )

        # ?????곗씠?곕? 諛곗튂 ?⑥쐞濡?蹂묐젹 泥섎━
        if table_chunks:
            # ???곗씠?곕? 臾몄옄?대줈 蹂??(蹂몃Ц留?援ъ꽦)
            table_texts = []
            for idx, table_chunk in enumerate(table_chunks):
                table_text = ""
                if table_chunk["header"]:
                    table_text += table_chunk["header"] + "\n"
                if table_chunk["rows"]:
                    table_text += "-|-" * len(table_chunk["rows"][0]) + "\n"
                    for row in table_chunk["rows"]:
                        table_text += " | " + " | ".join(row) + " |\n"
                table_texts.append(table_text)
            
            # ??泥?겕?ㅼ쓣 蹂묐젹 泥섎━ (content_type??"table"濡??ㅼ젙)
            await process_chunks_parallel_hwp(
                chunks=table_texts,
                content=content,
                subject=subject or os.path.basename(file_path or "") or content,
                table_name=table_name,
                dbname=dbname,
                job_id=job_id,
                job_manager=job_manager,
                memo=memo,
                job_progress_manager=job_progress_manager,
                chunk_progress=chunk_progress,
                batch_size=FILE_TABLE_EMBEDDING_BATCH_SIZE,
                # ???뚯씪 ?숈뒿? content_type??"file"濡??듭씪
                content_type=content_type,
                personal_info_filter=personal_info_filter, # 媛쒖씤?뺣낫 ?꾪꽣留??듭뀡 ?꾨떖
            )

        logger.info(
            f"[HWPX 蹂묐젹 泥섎━ ?꾨즺] content: {content}, total_chunks: {total_chunks}, job_id: {job_id}"
        )
        return {
            "status": "success",
            "message": f"{content} 泥섎━ ?꾨즺",
            "chunks": total_chunks,
            "chunk_count": [total_chunks],
            "use_source": [content]
        }

    except Exception as e:
        error_msg = f"HWPX 泥섎━ 以??ㅻ쪟 諛쒖깮: {str(e)}"
        logger.error(f"[HWPX 泥섎━ ?ㅽ뙣] file_path: {file_path}, job_id: {job_id}, ?ㅻ쪟: {error_msg}", exc_info=True)
        
        # ?뚯폆?쇰줈 ?ㅻ쪟 硫붿떆吏 ?꾩넚

        
        raise RuntimeError(error_msg)


async def calculate_hwp_chunks(file_path: str) -> int:
    """HWP ?뚯씪???뺥솗??泥?겕 ?섎? 怨꾩궛?⑸땲??"""
    try:
        import asyncio
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        from backend.config import Config
        
        # ???숈씪??蹂???⑥닔(hwp_to_text)瑜??ъ궗??(以묐났 ?쒓굅)
        def hwp_to_text_sync(hwp_file_path: str) -> str:
            return hwp_to_text(hwp_file_path)
        
        # 鍮꾨룞湲곕줈 HWP ?띿뒪??異붿텧 ?ㅽ뻾
        extracted_text = await asyncio.to_thread(hwp_to_text_sync, file_path)
        
        if not extracted_text.strip():
            logger.warning(f"HWP ?뚯씪?먯꽌 ?띿뒪?몃? 異붿텧?????놁뒿?덈떎: {file_path}")
            return 1  # 湲곕낯媛?        
        # ???숈씪??泥?겕 遺꾪븷 ?ㅼ젙 ?ъ슜
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=Config.BASIC_CHUNK_SIZE, 
            chunk_overlap=Config.BASIC_CHUNK_OVERLAP
        )
        chunks = text_splitter.split_text(extracted_text)
        actual_chunks = len(chunks)
        
        logger.info("HWP chunk count calculated: file=%s chunks=%s", file_path, actual_chunks)
        return actual_chunks
        
    except Exception as e:
        logger.error(f"HWP 泥?겕 ??怨꾩궛 ?ㅽ뙣: {file_path}, ?ㅻ쪟: {e}")
        return 5  # 湲곕낯媛?





