from __future__ import annotations

import os
import re
import zipfile
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
import xml.etree.ElementTree as ET


def extract_document_created_at(file_path: str) -> Optional[datetime]:
    """
    다운로드된 파일(서버에 복사된 파일)에서 '문서 내부 메타데이터' 기반 작성일을 best-effort로 추출한다.

    주의:
    - 파일시스템 ctime/mtime은 서버 저장 시점으로 재설정되므로 '원본 작성일'로 사용하지 않는다.
    - 반환값은 timezone-aware일 수 있으며, 상위에서 문자열로 저장할 때 정규화가 필요하다.
    """
    if not file_path:
        return None
    try:
        if not os.path.exists(file_path):
            return None
    except Exception:
        return None

    ext = _lower_ext(file_path)
    if ext in (".pdf",):
        return _extract_pdf_created(file_path)
    if ext in (".docx", ".pptx", ".xlsx", ".hwpx"):
        # OOXML류는 zip + docProps/core.xml 기반으로 created/modified를 확인한다.
        dt = _extract_ooxml_core_created(file_path)
        if dt:
            return dt
        # hwpx는 규격/버전 차이가 있어 core.xml이 없을 수 있다. best-effort로만 처리.
        return None
    if ext in (".jpg", ".jpeg", ".tif", ".tiff", ".png", ".webp"):
        return _extract_image_exif_created(file_path)
    return None


def _lower_ext(path: str) -> str:
    try:
        _, e = os.path.splitext(path)
        return (e or "").lower()
    except Exception:
        return ""


# ------------------------------ PDF ------------------------------

def _extract_pdf_created(file_path: str) -> Optional[datetime]:
    # 1) pikepdf 우선 (있으면 /CreationDate 접근이 비교적 명확)
    try:
        import pikepdf  # type: ignore

        with pikepdf.open(file_path) as pdf:
            info = getattr(pdf, "docinfo", None)
            if info:
                for key in ("/CreationDate", "/ModDate"):
                    try:
                        raw = info.get(key)
                    except Exception:
                        raw = None
                    dt = _parse_pdf_date(raw)
                    if dt:
                        return dt
    except Exception:
        pass

    # 2) PyMuPDF (pymupdf) fallback: metadata['creationDate'/'modDate']
    # MuPDF가 깨진 PDF에서 stderr로 경고를 쏟을 수 있어 기본은 비활성화.
    if (os.getenv("PDF_META_USE_PYMUPDF") or "").strip().lower() in {"1", "true", "yes", "y"}:
        try:
            # PyMuPDF는 배포/환경에 따라 import 경로가 다를 수 있다.
            # - pip install pymupdf → 보통 `import fitz` (전통적)
            # - 일부 환경에서는 `import pymupdf`로 제공되기도 함
            try:
                import pymupdf as _fitz  # type: ignore
            except Exception:
                import fitz as _fitz  # type: ignore

            doc = _fitz.open(file_path)
            try:
                md = getattr(doc, "metadata", None) or {}
                for key in ("creationDate", "modDate"):
                    dt = _parse_pdf_date(md.get(key))
                    if dt:
                        return dt
            finally:
                try:
                    doc.close()
                except Exception:
                    pass
        except Exception:
            pass

    # 3) pypdf/PyPDF2 fallback
    try:
        try:
            from pypdf import PdfReader  # type: ignore
        except Exception:
            from PyPDF2 import PdfReader  # type: ignore

        reader = PdfReader(file_path)
        meta = getattr(reader, "metadata", None) or {}
        for key in ("/CreationDate", "/ModDate"):
            dt = _parse_pdf_date(meta.get(key))
            if dt:
                return dt
    except Exception:
        pass

    return None


_PDF_DATE_RE = re.compile(
    r"""
    ^\s*
    (?:D:)?                 # optional 'D:' prefix
    (?P<Y>\d{4})
    (?P<M>\d{2})?
    (?P<D>\d{2})?
    (?P<h>\d{2})?
    (?P<m>\d{2})?
    (?P<s>\d{2})?
    (?P<tz>
        Z|
        [+\-]\d{2}'?\d{2}'?
    )?
    \s*$
    """,
    re.VERBOSE,
)


def _parse_pdf_date(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        s = value if isinstance(value, str) else str(value)
    except Exception:
        return None
    s = s.strip()
    if not s:
        return None

    m = _PDF_DATE_RE.match(s)
    if not m:
        return None

    y = int(m.group("Y"))
    mo = int(m.group("M") or 1)
    d = int(m.group("D") or 1)
    hh = int(m.group("h") or 0)
    mm = int(m.group("m") or 0)
    ss = int(m.group("s") or 0)

    tz_raw = m.group("tz")
    tzinfo = None
    if tz_raw:
        if tz_raw == "Z":
            tzinfo = timezone.utc
        else:
            # +09'00' / +0900 / -0530 등
            sign = 1 if tz_raw[0] == "+" else -1
            digits = re.sub(r"[^\d]", "", tz_raw)
            if len(digits) >= 4:
                tzh = int(digits[:2])
                tzm = int(digits[2:4])
                tzinfo = timezone(sign * timedelta(hours=tzh, minutes=tzm))
    try:
        return datetime(y, mo, d, hh, mm, ss, tzinfo=tzinfo)
    except Exception:
        return None


# ------------------------------ OOXML(core.xml) ------------------------------

def _extract_ooxml_core_created(file_path: str) -> Optional[datetime]:
    """
    DOCX/PPTX/XLSX 등 OOXML zip 내부의 docProps/core.xml에서 dcterms:created/modified를 추출.
    """
    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            try:
                core = zf.read("docProps/core.xml")
            except KeyError:
                return None
    except Exception:
        return None

    try:
        root = ET.fromstring(core)
    except Exception:
        return None

    # namespace-safe: 태그 localname으로 비교
    created_text = _find_first_text_by_localname(root, {"created"})
    modified_text = _find_first_text_by_localname(root, {"modified"})

    # 작성일 우선, 없으면 수정일
    for cand in (created_text, modified_text):
        dt = _parse_iso_datetime(cand)
        if dt:
            return dt
    return None


def _find_first_text_by_localname(root: ET.Element, names: set[str]) -> Optional[str]:
    try:
        for el in root.iter():
            tag = el.tag
            if not isinstance(tag, str):
                continue
            # "{ns}created" -> "created"
            local = tag.split("}")[-1].strip()
            if local in names:
                txt = (el.text or "").strip()
                if txt:
                    return txt
    except Exception:
        return None
    return None


def _parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None
    # 흔한 형태: 2025-06-12T01:02:03Z / 2025-06-12T01:02:03+09:00
    try:
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        return datetime.fromisoformat(t)
    except Exception:
        pass
    # 날짜만 있는 경우
    try:
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", t)
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        pass
    return None


# ------------------------------ Image(EXIF) ------------------------------

def _extract_image_exif_created(file_path: str) -> Optional[datetime]:
    try:
        from PIL import Image, ExifTags  # type: ignore
    except Exception:
        return None

    try:
        img = Image.open(file_path)
    except Exception:
        return None

    try:
        exif = None
        try:
            exif = img.getexif()
        except Exception:
            exif = None
        if not exif:
            return None

        tag_map = {}
        try:
            tag_map = {v: k for k, v in ExifTags.TAGS.items()}
        except Exception:
            tag_map = {}

        # DateTimeOriginal(36867) > DateTimeDigitized(36868) > DateTime(306)
        for name in ("DateTimeOriginal", "DateTimeDigitized", "DateTime"):
            tag_id = tag_map.get(name)
            if not tag_id:
                continue
            raw = exif.get(tag_id)
            dt = _parse_exif_datetime(raw)
            if dt:
                return dt
        return None
    finally:
        try:
            img.close()
        except Exception:
            pass


def _parse_exif_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        s = value.decode("utf-8", errors="ignore") if isinstance(value, (bytes, bytearray)) else str(value)
    except Exception:
        return None
    s = s.strip()
    if not s:
        return None
    # 흔한 형태: "2025:06:12 10:20:30"
    try:
        return datetime.strptime(s, "%Y:%m:%d %H:%M:%S")
    except Exception:
        return None


