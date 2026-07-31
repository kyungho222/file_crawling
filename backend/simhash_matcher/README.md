# SimHash exact 중복 확인 모듈

모듈 파일: `simhash_matcher/simhash_matcher.py`

이 모듈은 URL을 파싱하지 않습니다. 호출자가 파싱한 `subject`, `content`를 전달하면 64비트 SimHash를 생성하고 MariaDB 학습 테이블의 `hash` 컬럼에서 **exact 일치**를 확인합니다.

## 1. 라이브러리 적용

프로젝트 루트에서 의존성을 설치합니다.

```bash
python -m pip install -r requirements.txt
```

`requirements.txt`의 의존성:

```text
simhash==2.1.2
```

## 2. 입력 payload 준비

이 모듈은 URL을 받거나 파싱하지 않습니다. 호출 서비스가 추출한 제목과 본문으로 SimHash를 만들고, 그 값을 지정 테이블의 `hash` 컬럼에서 비교합니다.

```text
payload (제목·본문) + connection (DB 연결) + table (비교 대상)
                    ↓
             SimHash 생성
                    ↓
       db.table.hash 와 exact 비교
```

### Payload

```python
payload = {
    "subject": "게시글 제목",
    "content": "파싱된 본문 내용",
}
```

| 필드 | 필수 | 용도 |
|---|:---:|---|
| `subject` | 예 | SimHash 생성에 사용할 파싱된 게시글 제목 |
| `content` | 예 | SimHash 생성에 사용할 파싱된 본문 |

### Payload에 포함하지 않는 값

| 값 | 전달 방식 | 이유 |
|---|---|---|
| `url`, `registered_date`, `saved_at` | 전달하지 않음 | SimHash 생성·비교에 사용하지 않음 |
| `connection` | 함수 인자 | 호출 서비스가 만든 MariaDB 연결 객체 |
| `table` | 함수 인자 | 비교할 `ASADAL_..._LEARN_LIST` 테이블명 |
## 3. 권장 호출

신규 저장 여부까지 판단해야 할 때는 `check_hash()`를 호출합니다.

```python
from simhash_matcher.simhash_matcher import check_hash

result = check_hash(
    connection=connection,
    subject=payload["subject"],
    content=payload["content"],
    table="ASADAL_ce77dc5e9fd4_LEARN_LIST",
)
```

함수 시그니처:

```python
check_hash(connection, subject, content, *, table) -> dict
```

내부에서 다음 순서로 처리합니다.

```text
check_hash()
  → make_simhash(subject, content)
  → has_simhash_match(connection, simhash, table)
  → result 반환
```

## 4. 호출자 저장 처리

호출자는 `result["save"]`가 `True`일 때만 신규 레코드를 저장합니다. 저장할 때는 result의 `hash` 값을 사용합니다.

```python
if result["save"]:
    record = {
        "subject": payload["subject"],
        "content": payload["content"],
        "hash": result["hash"],
    }
    # 호출자 DB 저장 로직 실행
```

## 5. Response

`check_hash()`는 **중복 판정 결과**와 **이번 요청에서 생성한 SimHash**를 함께 반환합니다.

```json
{
  "duplicate": false,
  "save": true,
  "hash": "2fb34c0aa6310e34"
}
```

### 응답 필드

| 필드 | 의미 | 호출자 처리 |
|---|---|---|
| `duplicate` | `db.table.hash`에 동일한 SimHash가 존재하는지 | `true`이면 중복으로 처리 |
| `save` | 신규 레코드 저장 권장 여부 | `true`일 때만 DB 저장 진행 |
| `hash` | `subject` + `content`에서 생성한 16자리 SimHash | 신규 저장 시 `hash` 컬럼에 저장 |

### 결과별 처리

| 결과 | `duplicate` | `save` | `hash` | 호출자 권장 처리 |
|---|:---:|:---:|---|---|
| 동일 hash 존재 | `true` | `false` | 생성됨 | 저장하지 않고 중복 처리 |
| 동일 hash 없음 | `false` | `true` | 생성됨 | 반환된 `hash`와 함께 신규 저장 |
| DB에 `hash` 컬럼 없음 | `false` | `true` | 생성됨 | 비교는 건너뛰고, 반환된 `hash`로 신규 저장 가능 |
| `subject` 또는 `content` 누락 | `false` | `false` | `null` | 저장·비교 모두 건너뜀 |

> `hash`는 중복 여부와 무관하게 SimHash 생성에 성공하면 항상 반환됩니다.
## 6. DB 비교 규칙

- 테이블명은 호출 시 전달합니다. 예: `ASADAL_ce77dc5e9fd4_LEARN_LIST`
- 비교 컬럼은 `hash`로 고정됩니다.
- 테이블·컬럼 생성 또는 수정은 수행하지 않습니다.
- `hash` 컬럼이 없으면 비교를 skip하고 `False` fallback 처리합니다.

비교 SQL:

```sql
SELECT 1
FROM ASADAL_ce77dc5e9fd4_LEARN_LIST
WHERE hash = %s
LIMIT 1;
```

`%s`에는 생성된 16자리 SimHash가 파라미터 바인딩됩니다.

## 7. Fallback과 로그

### 필수 payload 누락

`subject` 또는 `content`가 `None` 또는 빈 문자열이면 SimHash 생성·DB 비교를 skip합니다.

- 반환: `{"duplicate": false, "save": false, "hash": null}`
- 누락 필드별 WARNING 로그:

```text
simhash 생성에 필요한 payload 중 subject 누락
simhash 생성에 필요한 payload 중 content 누락
```

### `hash` 컬럼 누락

MariaDB에 `hash` 컬럼이 없으면 비교를 skip합니다.

- 반환: `{"duplicate": false, "save": true, "hash": "생성된_해시"}`
- WARNING 로그:

```text
simhash 비교에 필요한 hash 컬럼 누락
```

## 8. 보조 함수

| 함수 | 반환값 | 용도 |
|---|---|---|
| `check_hash()` | `dict` | 생성·비교·저장 판단 response를 함께 반환하는 권장 함수 |
| `has_hash()` | `bool` | 생성·비교 후 중복 여부만 필요할 때 사용 |
| `make_simhash()` | `int | None` | SimHash 값만 생성 |
| `has_simhash_match()` | `bool` | SimHash 값만 DB에서 exact 비교 |
| `format_simhash()` | `str` | DB 저장용 16자리 16진수 문자열 변환 |

## 9. 현재 적용 범위

- [x] `subject`, `content` 기반 64비트 SimHash 생성
- [x] 지정 테이블의 `hash` 컬럼 exact 비교
- [x] `duplicate`, `save`, `hash` response 반환
- [x] payload·컬럼 누락 시 fallback 및 WARNING 로그
- [ ] 해밍 거리 계산
- [ ] 임계값 기반 유사 중복 판정

> 현재는 해시 거리 비교 없이 동일 SimHash의 **exact 일치**만 중복으로 판정합니다.

## 10. 테스트

프로젝트 루트에서 실행합니다.

```powershell
.\.venv\Scripts\python.exe -m unittest -v test_simhash_matcher.py
```
