"""
임베딩 모델 설정 모듈

이 모듈은 프로젝트 전체에서 사용할 임베딩 모델을 중앙에서 관리합니다.
OpenAI의 text-embedding-ada-002 모델을 명시적으로 사용합니다.
"""

import os

from langchain_openai import OpenAIEmbeddings
# from langchain_community.embeddings import OpenAIEmbeddings
from config import Config


class EmbeddingModelManager:
    """
    임베딩 모델을 관리하는 싱글톤 클래스
    
    프로젝트 전체에서 일관된 임베딩 모델을 사용하도록 보장합니다.
    """
    
    _instance = None
    _embedding_model = None
    
    # 사용할 임베딩 모델 명시
    EMBEDDING_MODEL_NAME = "text-embedding-ada-002"

    @classmethod
    def _embedding_kwargs(cls):
        try:
            timeout = float(os.getenv("OPENAI_EMBEDDING_REQUEST_TIMEOUT_SEC", "45") or "45")
        except Exception:
            timeout = 45.0
        try:
            retries = int(os.getenv("OPENAI_EMBEDDING_MAX_RETRIES", "2") or "2")
        except Exception:
            retries = 2
        return {
            "request_timeout": max(5.0, min(timeout, 300.0)),
            "max_retries": max(0, min(retries, 6)),
        }
    
    def __new__(cls):
        """싱글톤 패턴 구현"""
        if cls._instance is None:
            cls._instance = super(EmbeddingModelManager, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    def get_embedding_model(cls):
        """
        임베딩 모델 인스턴스를 반환합니다.
        OPENAI_API_KEY를 사용합니다.

        Returns:
            OpenAIEmbeddings: text-embedding-ada-002 모델 인스턴스
        """
        if cls._embedding_model is None:
            cls._embedding_model = OpenAIEmbeddings(
                model=cls.EMBEDDING_MODEL_NAME,
                openai_api_key=Config.OPENAI_ASADAL_API_KEY,
                **cls._embedding_kwargs(),
            )
        return cls._embedding_model

    @classmethod
    def get_embedding_model_for_rag(cls):
        """
        RAG/인포그래픽 등 전용 임베딩 모델을 반환합니다.
        OPENAI_EMBEDDINGS_API_KEY를 사용하며, 미설정 시 OPENAI_API_KEY로 폴백합니다.
        (ai-edu vector_search_service와 동일하게 임베딩 키를 분리한 구성)
        """
        api_key = getattr(Config, "OPENAI_EMBEDDINGS_API_KEY", None) or Config.OPENAI_API_KEY
        return OpenAIEmbeddings(
            model=cls.EMBEDDING_MODEL_NAME,
            openai_api_key=api_key,
            **cls._embedding_kwargs(),
        )
    
    @classmethod
    def create_new_instance(cls):
        """
        새로운 임베딩 모델 인스턴스를 생성하여 반환합니다.
        (멀티프로세싱 환경에서 각 워커가 독립적인 인스턴스를 필요로 할 때 사용)
        
        Returns:
            OpenAIEmbeddings: 새로운 text-embedding-ada-002 모델 인스턴스
        """
        return OpenAIEmbeddings(
            model=cls.EMBEDDING_MODEL_NAME,
            openai_api_key=Config.OPENAI_API_KEY,
            **cls._embedding_kwargs(),
        )
    
    @classmethod
    def reset(cls):
        """
        캐시된 임베딩 모델 인스턴스를 초기화합니다.
        (테스트나 설정 변경 시 사용)
        """
        cls._embedding_model = None


# 편의를 위한 함수들
def get_embedding_model():
    """
    임베딩 모델 인스턴스를 반환하는 편의 함수
    
    Returns:
        OpenAIEmbeddings: text-embedding-ada-002 모델 인스턴스
    
    Example:
        >>> from utils.embedding_config import get_embedding_model
        >>> embedding_model = get_embedding_model()
    """
    return EmbeddingModelManager.get_embedding_model()


def create_embedding_model():
    """
    새로운 임베딩 모델 인스턴스를 생성하는 편의 함수

    Returns:
        OpenAIEmbeddings: 새로운 text-embedding-ada-002 모델 인스턴스

    Example:
        >>> from utils.embedding_config import create_embedding_model
        >>> embedding_model = create_embedding_model()
    """
    return EmbeddingModelManager.create_new_instance()


def get_embedding_model_for_rag():
    """
    RAG/인포그래픽용 임베딩 모델을 반환합니다.
    OPENAI_EMBEDDINGS_API_KEY를 사용합니다(미설정 시 OPENAI_API_KEY 폴백).

    Returns:
        OpenAIEmbeddings: text-embedding-ada-002 모델 인스턴스
    """
    return EmbeddingModelManager.get_embedding_model_for_rag()


# 하위 호환성을 위한 직접 인스턴스 (기존 코드와의 호환성 유지)
embedding_model = get_embedding_model()
