import openai
import os
from pathlib import Path
from config import Config

env_path = Path(__file__).resolve().parent.parent / ".env"

openai.api_key = Config.OPENAI_API_KEY


# 헤더 추출 LLM Agent
def clean_data_with_llm(bordered_text):
    prompt = """
        테두리로 감싸진 데이터 전체를 확인하여 `헤더`를 추출하여 마크다운 포맷의 표 형식으로 출력해 주세요.
        ```
        출력 예시:

        | 헤더1 | 헤더2 | 헤더3 | ... |
        |-----|-----|-----|-----|
        ```

        헤더가 없을 경우, 아무 데이터도 출력하지 마세요.
        다른 말은 추가하지말고 `데이터만` 출력하세요.
    """
    messages = [
        {
            "role": "system",
            "content": "You are an AI assistant specializing in data extraction and cleaning.",
        },
        {"role": "user", "content": f"{prompt}\n\n테두리 데이터:\n{bordered_text}"},
    ]

    response = openai.chat.completions.create(
        model="gpt-4-turbo", messages=messages, max_tokens=4096, temperature=0
    )
    return response.choices[0].message.content.strip()


# PDF 헤더 추출 LLM Agent
def pdf_clean_data_with_llm(bordered_text):
    prompt = """
    다양한 구조의 표에서 헤더를 정확하게 분석하고 추출해주세요.
    
    분석 단계:
    1. 표 구조 파악
       - 단일 행 헤더인지 다중 행 헤더인지 확인
       - 열 병합이나 행 병합이 있는지 확인
       - 계층 구조(상/중/하위 헤더)가 있는지 확인
       - 반복되는 패턴(연도별, 분기별 등)이 있는지 확인
    
    2. 헤더 유형별 처리 규칙 예시:
    
       A. 단순 단일 행 헤더
          - 각 열을 개별 헤더로 처리
          예시:
          #header
          |항목1|항목2|항목3|항목4|
    
       B. 다중 행 계층 구조
          - 상위/하위 관계를 표현
          예시:
          #header
          |        상위항목1         |        상위항목2         |
          |하위항목1|하위항목2|하위항목3|하위항목4|하위항목5|하위항목6|
    
       C. 연도/분기별 반복 구조
          - 기간 정보를 포함하여 구조화
          예시:
          #header
          |      2023       |       2024F      |       2025F      |
          |분기1|분기2|분기3||분기1|분기2|분기3||분기1|분기2|분기3||
    
       D. 복합 계층 구조
          - 다양한 수준의 계층을 모두 표현
          예시:
          #header
          |           대분류1            |           대분류2            |
          |    중분류1     ||    중분류2     ||    중분류3     ||    중분류4     |
          |소분류1|소분류2|소분류3|소분류4|소분류5|소분류6|소분류7|소분류8|
    
    3. 데이터 정제 규칙 예시:
       - 단위 정보는 괄호로 표시: (%), (백만원), (배) 등
       - 불필요한 공백 제거
       - 의미를 알 수 없는 기호나 약어는 제외
       - 열 병합된 헤더는 범위를 유지하여 표현
    
    주의사항:
    - 표의 실제 구조를 우선적으로 따르되, 가독성있게 정리
    - 모든 계층 관계와 구조적 특성을 보존
    - 헤더가 불명확한 경우 가능한 상위 문맥을 참조하여 보완
    - 추가 설명 없이 마크다운 형식으로만 출력
    - 단위 정보가 있는 경우 반드시 포함
    
    위 규칙에 따라 아래 테두리로 감싸진 데이터에서 헤더를 추출하세요:
    """
    messages = [
        {
            "role": "system",
            "content": "You are an AI assistant specializing in data extraction and cleaning.",
        },
        {"role": "user", "content": f"{prompt}\n\n테두리 데이터:\n{bordered_text}"},
    ]

    response = openai.chat.completions.create(
        model="gpt-4-turbo", messages=messages, max_tokens=4096, temperature=0
    )
    return response.choices[0].message.content.strip()
