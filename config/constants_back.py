# config/constants.py
"""
프로젝트 전역 상수 정의
"""

# 파일 확장자 카테고리
DOC_EXTENSIONS = [
    '.hwp', '.hwpx',           # 한글
    '.pdf',                    # PDF
    '.doc', '.docx',           # 워드
    '.ppt', '.pptx',           # 파워포인트
    '.txt', '.csv',            # 텍스트
    '.xlsx', '.xls'            # 엑셀 (문서로 분류)
]

ARCHIVE_EXTENSIONS = [
    '.zip', '.rar', '.7z'      # 압축 파일
]

IMG_EXTENSIONS = [
    '.jpg', '.jpeg', '.png', '.gif', '.bmp'
]

# 수집 대상 확장자 (문서만 - 압축파일 제외)
COLLECTION_EXTENSIONS = DOC_EXTENSIONS

# 전체 허용 확장자 (탐색 단계용 - 문서만, 압축파일 및 이미지 제외)
ALLOWED_EXTENSIONS = DOC_EXTENSIONS

# 게시판 패턴 (URL에 포함되어야 할 키워드)
BOARD_PATTERNS = [
    # 기본
    "board", "bbs", "notice", "press", "news", "gallery",
    "community", "information", "data",

    # eGovFrame 3.x
    "/board/", "/bbs/", "/brd/", "/brdMstr/",
    "list.do", "view.do",
    "selectBoard", "boardList.do", "boardView.do",

    # eGovFrame 4.x
    "/cop/bbs/", "bbsList.do", "bbsView.do",

    # 대학/기관 패턴
    "/article/", "/articles/", "/post/", "/posts/",
    "contents.do?menuNo=",

    # 첨부파일 관련 패턴
    "atchFileId=", "fileDown", "fileDownload", "download.do",
]

# 깊이 탐색 스킵 패턴
SKIP_DEPTH_PATTERNS = [
    "Contents.asp",
    "contents.do",
    "contents",
    "page.do"
]

# 파일 상태 코드
class FileStatus:
    PENDING = "pending"           # 다운로드 대기 중
    DOWNLOADING = "downloading"   # 다운로드 중
    COMPLETED = "completed"       # 다운로드 완료
    FAILED = "failed"             # 다운로드 실패
    STUDYING = "studying"         # 학습 중
    STUDIED = "studied"           # 학습 완료
    STUDY_FAILED = "study_failed" # 학습 실패

# 크롤링 상태 코드
class CrawlStatus:
    IDLE = "idle"
    RUNNING = "running"
    COMPLETE = "complete"
    CANCELLED = "cancelled"
    ERROR = "error"

# 크롤링 단계
class CrawlStage:
    IDLE = "idle"
    START = "start"
    SCAN = "scan"
    POST = "post"
    ATTACH = "attach"
    DOWNLOAD = "download"
    STUDY = "study"
    COMPLETE = "complete"
