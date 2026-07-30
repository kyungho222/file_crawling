# db/__init__.py
from .repository import DBRepository

# 싱글톤 인스턴스
db = DBRepository()

__all__ = ['db', 'DBRepository']
