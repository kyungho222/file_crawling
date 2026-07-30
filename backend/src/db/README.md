# DB 모듈 (비활성화 상태)

## ⚠️ 현재 상태

이 DB 모듈은 **준비되어 있지만 기본적으로는 비활성화** 상태입니다.

- ✅ 모듈 구현 완료
- ✅ 테이블 스키마 정의 완료
- ✅ CRUD 함수 구현 완료
- ✅ API 라우터 준비 완료
- ❌ **backend/main.py에 등록되지 않음** (선택적 사용)

## 📂 구조

```
backend/src/db/
├── __init__.py       # 모듈 초기화
├── database.py       # DB 연결 설정
├── models.py         # SQLAlchemy 모델
├── crud.py           # CRUD 함수
└── README.md         # 이 파일
```

## 🚀 활성화 방법

### 1. DB 초기화

```bash
# 프로젝트 루트에서 실행
python init_db.py

# 또는 Windows:
DB초기화.bat
```

### 2. backend/app.py 수정

`backend/app.py` (또는 `backend/main.py`) 파일에 다음 코드를 추가:

```python
# DB 라우터 import
from src.routers import file_db_router

# 라우터 등록 (108줄 근처에 추가)
app.include_router(file_db_router.router)
logger.info("✅ DB 라우터 활성화")
```

### 3. 서버 재시작

```bash
python backend/app.py
```

### 4. API 확인

```bash
# DB 헬스 체크
curl http://localhost:8001/api/v1/file-db/health

# API 문서
open http://localhost:8001/docs
```

## 📚 상세 문서

전체 사용법은 다음 문서를 참고하세요:

- **[docs/08_DATABASE_INTEGRATION.md](../../../docs/08_DATABASE_INTEGRATION.md)**

## 🎯 용도

- 크롤링 이력 관리
- 파일 다운로드 상태 추적
- 통계 및 분석
- 검색 기능

## 💡 참고

DB를 사용하지 않아도 기본 크롤링 기능은 정상 작동합니다.
DB는 **선택적 기능**입니다.

