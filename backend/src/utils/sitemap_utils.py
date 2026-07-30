"""
Sitemap.xml 파싱 및 페이지 수 추정 유틸리티
"""

import requests
import xml.etree.ElementTree as ET
from typing import Optional, Dict, List, Tuple
from urllib.parse import urljoin, urlparse
import logging
import warnings

# ⚠️ InsecureRequestWarning 경고 숨기기
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

logger = logging.getLogger(__name__)


class SitemapAnalyzer:
    """웹사이트의 sitemap.xml을 분석하여 페이지 정보 추출"""
    
    # 기본 sitemap 경로 목록 (우선순위 순)
    DEFAULT_SITEMAP_PATHS = [
        '/sitemap.xml',
        '/sitemap_index.xml',
        '/sitemap-index.xml',
        '/sitemap.php',
        '/sitemap1.xml',
    ]
    
    def __init__(self, timeout: int = 15):  # 10초 → 15초로 완화 (느린 서버/봇 감지 대응)
        """
        Args:
            timeout: 요청 타임아웃 (초, 기본값: 15초)
        """
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8'
        })
    
    def analyze_site(self, start_url: str, base_domain: str = '') -> Dict:
        """
        사이트의 전체 페이지 수 및 구조 분석
        
        Args:
            start_url: 분석할 사이트 URL
            base_domain: 도메인 제한 (옵션)
            
        Returns:
            {
                "success": bool,
                "total_pages": int,
                "sitemap_urls": list[str],
                "has_sitemap": bool,
                "robots_txt_url": str,
                "sitemap_index_count": int,
                "message": str
            }
        """
        try:
            parsed_url = urlparse(start_url)
            if not base_domain:
                base_domain = parsed_url.netloc
            
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # 1단계: robots.txt에서 sitemap 위치 확인
            robots_url = urljoin(base_url, '/robots.txt')
            sitemap_urls = self._get_sitemap_from_robots(robots_url)
            
            # robots.txt에 sitemap이 없으면 기본 위치 확인 (여러 경로 시도)
            if not sitemap_urls:
                logger.info("robots.txt에 sitemap 없음, 기본 위치 확인 중...")
                sitemap_urls = self._find_default_sitemaps(base_url)
                
                if sitemap_urls:
                    logger.info(f"✅ 기본 위치에서 sitemap 발견: {sitemap_urls[0]}")
                else:
                    logger.info("❌ 기본 위치에 sitemap 없음 (확인한 경로: /sitemap.xml, /sitemap_index.xml, ...)")
            
            if not sitemap_urls:
                return {
                    "success": False,
                    "total_pages": 0,
                    "sitemap_urls": [],
                    "has_sitemap": False,
                    "robots_txt_url": robots_url,
                    "sitemap_index_count": 0,
                    "message": "sitemap.xml을 찾을 수 없습니다. 크롤링 중 실시간 카운트로 대체됩니다."
                }
            
            # 2단계: sitemap.xml 파싱
            total_urls = 0
            all_page_urls = []
            sitemap_index_count = 0
            
            for sitemap_url in sitemap_urls:
                urls, is_index = self._parse_sitemap(sitemap_url, base_domain)
                if is_index:
                    sitemap_index_count += 1
                    # sitemap index인 경우 하위 sitemap들 파싱
                    logger.info(f"📁 Sitemap Index 발견: {len(urls)}개 하위 sitemap")
                    for sub_sitemap_url in urls:
                        sub_urls, _ = self._parse_sitemap(sub_sitemap_url, base_domain)
                        all_page_urls.extend(sub_urls)
                        total_urls += len(sub_urls)
                else:
                    all_page_urls.extend(urls)
                    total_urls += len(urls)
            
            # 중복 제거
            unique_urls = list(set(all_page_urls))
            total_pages = len(unique_urls)
            
            logger.info(f"✅ Sitemap 분석 완료: {total_pages}개 페이지 발견")
            
            return {
                "success": True,
                "total_pages": total_pages,
                "sitemap_urls": sitemap_urls,
                "has_sitemap": True,
                "robots_txt_url": robots_url,
                "sitemap_index_count": sitemap_index_count,
                "message": f"sitemap.xml에서 {total_pages}개 페이지를 발견했습니다.",
                "sample_urls": unique_urls[:10]  # 샘플 URL (최대 10개)
            }
            
        except Exception as e:
            logger.error(f"❌ Sitemap 분석 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "total_pages": 0,
                "sitemap_urls": [],
                "has_sitemap": False,
                "robots_txt_url": "",
                "sitemap_index_count": 0,
                "message": f"Sitemap 분석 중 오류 발생: {str(e)}"
            }
    
    def _get_sitemap_from_robots(self, robots_url: str) -> List[str]:
        """
        robots.txt에서 sitemap URL 추출
        
        Args:
            robots_url: robots.txt URL
            
        Returns:
            sitemap URL 리스트
        """
        try:
            response = self.session.get(robots_url, timeout=self.timeout, verify=False)
            if response.status_code == 200:
                sitemap_urls = []
                for line in response.text.split('\n'):
                    if line.strip().lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        sitemap_urls.append(sitemap_url)
                        logger.info(f"📄 robots.txt에서 sitemap 발견: {sitemap_url}")
                return sitemap_urls
        except Exception as e:
            logger.warning(f"⚠️ robots.txt 확인 실패: {e}")
        return []
    
    def _find_default_sitemaps(self, base_url: str) -> list:
        """
        기본 경로에서 sitemap 찾기 (여러 경로 순차 확인)
        
        Args:
            base_url: 기본 URL
            
        Returns:
            발견된 sitemap URL 리스트 (첫 번째 발견 시 중단)
        """
        found = []
        for path in self.DEFAULT_SITEMAP_PATHS:
            sitemap_url = urljoin(base_url, path)
            logger.debug(f"🔍 Sitemap 확인 중: {sitemap_url}")
            
            if self._check_sitemap_exists(sitemap_url):
                found.append(sitemap_url)
                logger.info(f"✅ 발견: {path}")
                break  # 첫 발견 시 중단 (성능 최적화)
            else:
                logger.debug(f"❌ 없음: {path}")
        
        return found
    
    def _check_sitemap_exists(self, sitemap_url: str) -> bool:
        """
        sitemap.xml 존재 여부 확인
        
        Args:
            sitemap_url: sitemap URL
            
        Returns:
            존재 여부
        """
        try:
            response = self.session.head(sitemap_url, timeout=self.timeout, verify=False, allow_redirects=True)
            is_found = response.status_code == 200
            return is_found
        except Exception as e:
            logger.debug(f"⚠️ Sitemap 확인 실패: {sitemap_url} - {e}")
            return False
    
    def _parse_sitemap(self, sitemap_url: str, base_domain: str = '') -> Tuple[List[str], bool]:
        """
        sitemap.xml 파싱
        
        Args:
            sitemap_url: sitemap URL
            base_domain: 도메인 제한 (옵션)
            
        Returns:
            (URL 리스트, sitemap index 여부)
        """
        try:
            response = self.session.get(sitemap_url, timeout=self.timeout, verify=False)
            if response.status_code != 200:
                logger.warning(f"⚠️ Sitemap 다운로드 실패: {sitemap_url} (status={response.status_code})")
                return [], False
            
            # XML 파싱
            root = ET.fromstring(response.content)
            
            # 네임스페이스 처리
            namespaces = {
                'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'image': 'http://www.google.com/schemas/sitemap-image/1.1',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9'
            }
            
            urls = []
            is_sitemap_index = False
            
            # Sitemap Index 확인 (하위 sitemap 목록)
            sitemaps = root.findall('.//sm:sitemap/sm:loc', namespaces)
            if sitemaps:
                is_sitemap_index = True
                for sitemap in sitemaps:
                    urls.append(sitemap.text.strip())
                logger.info(f"📁 Sitemap Index: {len(urls)}개 하위 sitemap")
                return urls, is_sitemap_index
            
            # 일반 Sitemap (페이지 URL 목록)
            url_elements = root.findall('.//sm:url/sm:loc', namespaces)
            if not url_elements:
                # 네임스페이스 없이 시도
                url_elements = root.findall('.//url/loc')
            
            for loc in url_elements:
                url = loc.text.strip()
                
                # 도메인 필터링
                if base_domain:
                    parsed = urlparse(url)
                    if parsed.netloc != base_domain:
                        continue
                
                urls.append(url)
            
            logger.info(f"📄 Sitemap 파싱 완료: {sitemap_url} ({len(urls)}개 URL)")
            return urls, is_sitemap_index
            
        except ET.ParseError as e:
            logger.error(f"❌ XML 파싱 오류: {sitemap_url} - {e}")
            return [], False
        except Exception as e:
            logger.error(f"❌ Sitemap 파싱 실패: {sitemap_url} - {e}")
            return [], False


def analyze_site_structure(start_url: str, base_domain: str = '', timeout: int = 15) -> Dict:
    """
    사이트 구조 분석 (함수형 인터페이스)
    
    Args:
        start_url: 분석할 사이트 URL
        base_domain: 도메인 제한 (옵션)
        timeout: 요청 타임아웃 (초, 기본값: 5초)
        
    Returns:
        분석 결과 딕셔너리
    """
    analyzer = SitemapAnalyzer(timeout=timeout)
    return analyzer.analyze_site(start_url, base_domain)

