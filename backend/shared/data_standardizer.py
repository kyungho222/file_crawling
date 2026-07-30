import logging
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class DataStandardizer:
    """
    temp 프로젝트의 DataStandardizer를 현재 프로젝트로 이식.
    - 작성자/날짜/첨부유무 표준화에 사용
    - Scan/Detail 추출 단계에서 얻은 메타를 LEARN_LIST에 안정적으로 저장하기 위한 후처리 모듈
    """

    @staticmethod
    def standardize_date(raw_date: Optional[str]) -> Optional[str]:
        """날짜 형식을 YYYY-MM-DD로 통일(가능한 경우)."""
        if not raw_date:
            return None
        clean_date = re.sub(r"[^0-9\-\./]", "", str(raw_date)).strip()
        clean_date = clean_date.replace(".", "-").replace("/", "-")
        m = re.search(r"(\d{4})[-\./](\d{1,2})[-\./](\d{1,2})", clean_date)
        if m:
            y, mo, d = m.groups()
            return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
        m2 = re.search(r"(\d{4})(\d{2})(\d{2})", clean_date)
        if m2:
            y, mo, d = m2.groups()
            return f"{y}-{mo}-{d}"
        return str(raw_date).strip()

    @staticmethod
    def standardize_author(raw_author: Optional[str]) -> Optional[str]:
        """작성자/부서 등 author 정보 정제. 특정할 수 없으면 None."""
        if not raw_author:
            return None
        s = str(raw_author).strip()
        if not s:
            return None
        if s.lower() in ("미상", "unknown", "null", "none"):
            return None

        cleaned = re.sub(
            r"^(작성자|등록자|등록인|작성인|글쓴이|성명|담당자|담당부서|부서|담당|조회수|조회|조회: .*|수정일|게시일|발행일|직책|작성자유형)\s*[:\s]*",
            "",
            s,
            flags=re.IGNORECASE,
        ).strip()
        if not cleaned:
            return None

        # 숫자만/조회수 등 의미 없는 메타는 버림
        if re.fullmatch(r"\d+", cleaned):
            return None
        lowered = cleaned.lower()
        if any(k in lowered for k in ("조회", "hit", "view", "조회수")):
            return None
        return cleaned

    @staticmethod
    def standardize_attachment(raw_val: Any) -> str:
        """첨부파일 유무를 O/X로 통일."""
        if not raw_val:
            return "X"
        s = str(raw_val).upper().strip()
        if s in ("O", "YES", "TRUE", "1", "Y", "ATTACHED"):
            return "O"
        return "X"

    @staticmethod
    def _looks_like_file_title(title: str) -> bool:
        """제목이 파일 확장자/파일명 조각인지 판별."""
        t = (title or "").strip().lower()
        if not t:
            return False
        if t in {"파일", "첨부파일", "첨부 파일", "attachment", "attachments"}:
            return True
        ext_only = {"pdf", "doc", "docx", "hwp", "hwpx", "xls", "xlsx", "ppt", "pptx", "zip", "rar", "txt", "csv"}
        if t in ext_only:
            return True
        if re.fullmatch(r".+\.(pdf|docx?|hwpx?|xlsx?|pptx?|zip|rar|txt|csv)", t):
            return True
        return False

    @staticmethod
    def _is_ui_chrome_title(title: str) -> bool:
        """접근성/툴바 라벨 등 본문 앞단에서 잘못 복구된 '제목'인지."""
        s = (title or "").strip()
        if not s:
            return True
        if s.endswith("..."):
            s = s[:-3].strip()
        if len(s) > 24:
            return False
        noise_exact = {
            "화면크기",
            "본문",
            "통합예약",
            "공지사항",
            "목록",
            "닫기",
            "인쇄하기",
            "인쇄",
            "제목 없음",
            "주메뉴 바로가기",
            "본문 바로가기",
            "본문내용 바로가기",
        }
        if s in noise_exact:
            return True
        if "바로가기" in s and len(s) <= 20:
            return True
        return False

    @staticmethod
    def _cleanup_title(title: str) -> str:
        s = re.sub(r"\s+", " ", str(title or "")).strip()
        if not s:
            return ""

        for prefix in (
            "안성시설관리공단,",
            "안성시설관리공단 :",
            "안성시설관리공단:",
        ):
            if s.startswith(prefix):
                stripped = s[len(prefix):].strip()
                if len(stripped) >= 8:
                    s = stripped
                    break

        labels = (
            "작성자",
            "등록자",
            "등록인",
            "작성인",
            "담당자",
            "글쓴이",
            "성명",
            "담당부서",
            "부서",
            "부서명",
            "작성부서",
            "작성부서명",
            "등록일",
            "작성일",
            "게시일",
            "수정일",
            "조회수",
            "조회",
            "첨부파일",
            "첨부 파일",
        )

        split_idx = -1
        for label in labels:
            token = f" {label}"
            idx = s.find(token)
            if idx == -1:
                continue
            after_idx = idx + len(token)
            next_char = s[after_idx:after_idx + 1]
            if next_char and not next_char.isspace() and next_char not in ":\uff1a([":
                continue
            if split_idx == -1 or idx < split_idx:
                split_idx = idx

        if split_idx >= 0:
            tail = s[split_idx:].strip()
            label_hits = sum(1 for label in labels if f" {label}" in tail)
            has_date = bool(
                re.search(
                    r"\d{4}[-./]\d{1,2}[-./]\d{1,2}|\d{4}년\s*\d{1,2}월\s*\d{1,2}일",
                    tail,
                )
            )
            if label_hits >= 2 or has_date:
                s = s[:split_idx].strip(" -|:/")

        return s

    @classmethod
    def unify(cls, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """원본 데이터를 후처리하여 제목·본문·등록일·작성자·첨부유무를 통일해 반환."""
        # 본문 내 불필요한 줄바꿈과 공백을 제거하여 텍스트 정제
        content_raw = (raw_data.get("content") or "").strip()
        content_clean = re.sub(r"[\r\n\t\s]+", " ", content_raw).strip()

        # 제목 필드를 확인하고 비어있거나 파일명 조각이면 본문 기반으로 보정
        title_orig = (raw_data.get("title") or "").strip()
        title = title_orig
        if (not title or cls._looks_like_file_title(title)) and content_clean:
            # 게시판 본문은 대체로 "제목 작성자 ... 조회수 ... 작성일 ..." 구조를 가지므로 제목 구간만 우선 복원
            m = re.search(r"^(.*?)\s*(?:작성자|조회수|작성일)\b", content_clean)
            if m:
                recovered = (m.group(1) or "").strip()
                if recovered and not cls._looks_like_file_title(recovered):
                    title = recovered
                else:
                    title = content_clean[:30] + "..."
            else:
                title = content_clean[:30] + "..."
            # 본문 앞이 달력·스킵·화면크기 등 UI만인 경우 운영 DB에 쓰레기 제목이 들어가므로 폐기
            if cls._is_ui_chrome_title(title):
                title = title_orig if title_orig and not cls._is_ui_chrome_title(title_orig) else ""

        title = cls._cleanup_title(title)

        date_std = cls.standardize_date(raw_data.get("date"))
        author_std = cls.standardize_author(raw_data.get("author"))
        attach_std = cls.standardize_attachment(raw_data.get("has_attachments"))

        return {
            "title": title,
            "content": content_clean,
            "date": date_std,
            "author": author_std,
            "has_attachments": attach_std,
        }
