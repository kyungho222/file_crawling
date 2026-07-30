"""
호환 모듈.

초기에는 `utils.url_canonical.canonicalize_url_for_dedup`를 사용했으나,
배포 환경에서 "새 파일 누락"으로 서버가 죽는 사고를 방지하기 위해
정식 구현은 `utils.url.canonicalize_url_for_dedup`로 이동했습니다.
"""

from utils.url import canonicalize_url_for_dedup


