"""
Summary:
사이트 도메인별 추출 태그(제목, 본문)를 관리합니다.
크롤링 시작 전 사용자가 직접 태그를 입력하면(set_custom_tags), JSON 파일에 즉시 업데이트되어 최우선으로 적용됩니다.
"""

import json
import os
from urllib.parse import urlparse

# JSON 파일 보관 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "configs")
CONFIG_PATH = os.path.join(CONFIG_DIR, "site_configs.json")
DOMAIN_CONFIG_DIR = os.path.join(CONFIG_DIR, "sites")

# 기본 태그값 (동적으로 생성될 때 사용)
DEFAULT_TAGS = {
    "title_tag": ".title",       
    "content_tag": ".content"    
}

class SiteConfigManager:
    def __init__(self):
        # 1. 폴더 생성
        os.makedirs(CONFIG_DIR, exist_ok=True)
        os.makedirs(DOMAIN_CONFIG_DIR, exist_ok=True)
        
        # 2. 파일이 없으면 '구로구청 템플릿'을 기본으로 넣어서 즉시 생성
        if not os.path.exists(CONFIG_PATH):
            self.configs = {
                "guro.go.kr": {
                    "title_tag": "tr.p-table__subject .p-table__subject_text, .p-table__subject .p-table__subject_text, .p-table__subject_text, h3.h0.title, .poll_view > h4, .title",
                    "content_tag": ".p-table__content"
                }
            }
            self._save_file()
            print(f"[SiteConfigManager] 초기 기본 템플릿 생성 완료: {CONFIG_PATH}")
        else:
            # 파일이 이미 존재하면 기존 데이터 읽기
            self.configs = self._load_file()

    def _domain_config_path(self, domain: str) -> str:
        safe_domain = self._extract_domain(domain).lower()
        return os.path.join(DOMAIN_CONFIG_DIR, f"{safe_domain}.json")

    def _load_file(self) -> dict:
        """JSON 파일을 읽어옵니다."""
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[SiteConfigManager] 명세서 읽기 실패: {e}")
            return self.configs if hasattr(self, 'configs') else {}

    def _load_domain_file(self, domain: str) -> dict | None:
        path = self._domain_config_path(domain)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else None
        except Exception as e:
            print(f"[SiteConfigManager] domain config read failed: {path} | {e}")
            return None

    def _save_domain_file(self, domain: str, tags: dict):
        path = self._domain_config_path(domain)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(tags, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[SiteConfigManager] domain config save failed: {path} | {e}")

    def _extract_domain(self, url_or_domain: str) -> str:
        """URL 또는 도메인 문자열에서 순수 도메인만 추출합니다."""
        if url_or_domain.startswith("http"):
            return urlparse(url_or_domain).netloc.replace("www.", "")
        return url_or_domain.replace("www.", "")

    def set_custom_tags(self, url_or_domain: str, title_tag: str = None, content_tag: str = None):
        """
        [핵심] 크롤링 시작 전, 사용자가 입력한 태그 정보를 명세서에 강제로 업데이트합니다.
        입력된 정보는 JSON 파일에 저장되어 추출 시점에 1순위로 사용됩니다.
        """
        domain = self._extract_domain(url_or_domain)
        if not domain:
            return

        self.configs = self._load_file()
        domain_tags = self._load_domain_file(domain)

        # 도메인이 없으면 기본 틀 생성
        if domain_tags is not None:
            self.configs[domain] = {**DEFAULT_TAGS, **domain_tags}
        elif domain not in self.configs:
            self.configs[domain] = DEFAULT_TAGS.copy()

        # 사용자가 입력한 값이 있으면 덮어쓰기
        if title_tag:
            self.configs[domain]["title_tag"] = title_tag
        if content_tag:
            self.configs[domain]["content_tag"] = content_tag

        if domain_tags is not None:
            self._save_domain_file(domain, self.configs[domain])
        else:
            self._save_file()
        print(f"[SiteConfigManager] 🛠️ 사용자 지정 태그 세팅 완료 | {domain} -> 제목: {title_tag}, 본문: {content_tag}")

    def get_tags(self, url: str) -> dict:
        """URL 도메인에 맞는 태그를 반환합니다. 없으면 자동 생성합니다."""
        self.configs = self._load_file()
        domain = self._extract_domain(url)

        if not domain:
            return DEFAULT_TAGS.copy()

        domain_tags = self._load_domain_file(domain)
        if domain_tags is not None:
            self.configs[domain] = {**DEFAULT_TAGS, **domain_tags}
            return self.configs[domain]

        if domain not in self.configs:
            self.configs[domain] = DEFAULT_TAGS.copy()
            self._save_file()
            print(f"[SiteConfigManager] 🆕 신규 사이트 명세서 자동 등록: {domain}")

        return self.configs[domain]

    def _save_file(self):
        """현재 메모리의 설정값을 JSON 파일로 저장합니다."""
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(self.configs, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"[SiteConfigManager] 명세서 저장 실패: {e}")

# 전역 인스턴스
config_manager = SiteConfigManager()
