import requests
import json
import logging
from typing import Dict, Any

# 로깅 설정
logger = logging.getLogger(__name__)
# API 서버 기본 URL 설정
API_BASE_URL = "http://localhost:10800"

# API 엔드포인트 정의
API_ENDPOINTS = {
    "toxic_filter": f"{API_BASE_URL}/toxic_filter",
    "detect_pii": f"{API_BASE_URL}/detect_pii",
    "harmful_content_embedding": f"{API_BASE_URL}/harmful_content_embedding"
}


def check_toxic_content(text: str, upload_dir: str = None, api_url: str = None) -> Dict[str, Any]:
    """
    텍스트의 유해성을 검사하는 함수
    
    Args:
        text (str): 검사할 텍스트
        api_url (str): 유해성 검사 API URL (기본값: localhost:10700)
    
    Returns:
        Dict[str, Any]: 검사 결과
            {
                "toxicity_detected": bool,    # 유해성 감지 여부
                "toxicity_score": float,      # 유해성 점수 (0.0 ~ 1.0)
                "toxicity_level": str,        # 유해성 수준 (mild, moderate, severe 등)
                "error": str,                 # 에러 메시지 (있는 경우)
                "success": bool               # API 호출 성공 여부
            }
    """
    
    # 기본 응답 구조
    result = {
        "toxicity_detected": False,
        "toxicity_score": 0.0,
        "toxicity_level": None,
        "error": None,
        "success": False
    }
    
    try:
        # API URL 설정 (기본값 사용)
        if api_url is None:
            api_url = API_ENDPOINTS["toxic_filter"]
        
        # upload_dir이 None인 경우 기본값 설정
        if upload_dir is None:
            upload_dir = "default"
        
        # API 요청 데이터 준비
        payload = {
            "text": text,
            "upload_dir": upload_dir
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # API 호출
        logger.info(f"유해성 검사 API 호출 시작: {api_url}")
        response = requests.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=10  # 10초 타임아웃
        )
        
        # HTTP 상태 코드 확인
        if response.status_code == 200:
            try:
                response_data = response.json()
                logger.info(f"API 응답 성공: {response_data}")
                
                # 응답 데이터 파싱
                result["success"] = True
                
                # 필요한 3개 값만 추출
                if "toxicity_detected" in response_data:
                    result["toxicity_detected"] = response_data["toxicity_detected"]
                
                if "toxicity_score" in response_data:
                    result["toxicity_score"] = response_data["toxicity_score"]
                
                # toxicity_level은 toxicity_details 안에 있음
                if "toxicity_details" in response_data and "toxicity_level" in response_data["toxicity_details"]:
                    result["toxicity_level"] = response_data["toxicity_details"]["toxicity_level"]
                
                logger.info(f"유해성 검사 완료: {result}")
                
            except json.JSONDecodeError as e:
                error_msg = f"API 응답 JSON 파싱 실패: {str(e)}"
                logger.error(error_msg)
                result["error"] = error_msg
                
        else:
            error_msg = f"API 호출 실패 - 상태 코드: {response.status_code}, 응답: {response.text}"
            logger.error(error_msg)
            result["error"] = error_msg
            
    except requests.exceptions.Timeout:
        error_msg = "API 호출 타임아웃"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except requests.exceptions.ConnectionError:
        error_msg = "API 서버 연결 실패"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API 호출 중 예외 발생: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except Exception as e:
        error_msg = f"예상치 못한 오류 발생: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
    
    return result


def get_toxicity_message(toxicity_level: str) -> str:
    """
    유해성 수준에 따른 적절한 메시지를 반환합니다.
    
    Args:
        toxicity_level (str): 유해성 수준 (mild, moderate, severe)
    
    Returns:
        str: 유해성 수준에 맞는 메시지
    """
    messages = {
        "mild": "부적절한 표현이 포함되어 있습니다. 건전한 대화를 위해 다른 질문을 해주세요.",
        "moderate": "부적절한 내용이 감지되었습니다. 건전한 대화를 위해 다른 질문을 해주세요.",
        "severe": "매우 부적절한 내용입니다. 건전한 대화 환경을 위해 다른 질문을 해주세요."
    }
    
    return messages.get(toxicity_level, "부적절한 내용이 감지되었습니다. 다른 질문을 해주세요. 유해필터링 감지 by kss")

def check_pii_content(text: str) -> Dict[str, Any]:
    """
    개인정보를 마스킹 처리하는 함수입니다.
    
    Args:
        text (str): 검사할 텍스트
        
    Returns:
        Dict[str, Any]: 검사 결과
            {
                "is_sensitive": bool,      # 개인정보 감지 여부
                "masked_text": str,        # 마스킹된 텍스트
                "masked_parts_text": str,  # 마스킹된 부분만 추출한 텍스트
                "total_entities": int,     # 감지된 엔티티 수
                "confidence_score": float, # 신뢰도 점수
                "entities": list,          # 감지된 엔티티 목록
                "error": str,              # 에러 메시지 (있는 경우)
                "success": bool            # API 호출 성공 여부
            }
    """
    # 기본 응답 구조
    result = {
        "is_sensitive": False,
        "masked_text": text,
        "masked_parts_text": "",
        "total_entities": 0,
        "confidence_score": 0.0,
        "entities": [],
        "error": None,
        "success": False
    }
    
    try:
        # API 엔드포인트 및 요청 데이터 설정
        url = API_ENDPOINTS["detect_pii"]
        payload = {"text": text}
        
        # API 호출
        response = requests.post(url, json=payload, timeout=30)
        
        # 응답 처리
        if response.status_code == 200:
            try:
                response_data = response.json()
                if response_data.get("success"):
                    result["success"] = True
                    result["is_sensitive"] = response_data.get("is_sensitive", False)
                    result["masked_text"] = response_data.get("masked_text", text)
                    result["masked_parts_text"] = response_data.get("masked_parts_text", "")
                    result["total_entities"] = response_data.get("total_entities", 0)
                    result["confidence_score"] = response_data.get("confidence_score", 0.0)
                    result["entities"] = response_data.get("entities", [])
                    logger.info(f"PII 검사 완료: is_sensitive={result['is_sensitive']}, total_entities={result['total_entities']}")
                else:
                    error_msg = "PII 검사 실패"
                    logger.error(error_msg)
                    result["error"] = error_msg
            except json.JSONDecodeError as e:
                error_msg = f"PII API 응답 JSON 파싱 실패: {str(e)}"
                logger.error(error_msg)
                result["error"] = error_msg
        else:
            error_msg = f"PII API 호출 실패 - 상태 코드: {response.status_code}"
            logger.error(error_msg)
            result["error"] = error_msg
            
    except requests.exceptions.Timeout:
        error_msg = "PII API 호출 타임아웃"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except requests.exceptions.ConnectionError:
        error_msg = "PII API 서버 연결 실패"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except requests.exceptions.RequestException as e:
        error_msg = f"PII API 호출 중 예외 발생: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except Exception as e:
        error_msg = f"PII 검사 중 예상치 못한 오류 발생: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
    
    return result

async def embed_harmful_content(upload_dir: str, harmful_content_keyword: str) -> dict:
    """
    유해 콘텐츠 키워드를 임베딩하는 API를 호출하는 함수입니다.
    
    Args:
        upload_dir (str): 업로드 디렉토리명
        harmful_content_keyword (str): 임베딩할 유해 콘텐츠 키워드
        
    Returns:
        dict: API 호출 결과
            - success (bool): 성공 여부
            - message (str): 결과 메시지
            - error (str): 에러 발생시 에러 메시지
    """
    try:
        # API 엔드포인트 및 요청 데이터 설정
        url = API_ENDPOINTS["harmful_content_embedding"]
        payload = {
            "upload_dir": upload_dir,
            "harmful_content_keyword": harmful_content_keyword
        }
        
        # API 호출 (비동기 처리)
        import asyncio
        response = await asyncio.to_thread(requests.post, url, json=payload, timeout=10)
        
        # 응답 처리
        if response.status_code == 200:
            try:
                result = response.json()
                return {
                    "success": True,
                    "message": result.get("message", "임베딩이 완료되었습니다.")
                }
            except json.JSONDecodeError as e:
                logger.error(f"임베딩 API 응답 JSON 파싱 실패: {str(e)}")
                return {
                    "success": False,
                    "error": "응답 데이터 처리 실패"
                }
        else:
            logger.error(f"임베딩 API 호출 실패 - 상태 코드: {response.status_code}")
            return {
                "success": False, 
                "error": f"API 호출 실패 (상태 코드: {response.status_code})"
            }
            
    except requests.exceptions.Timeout:
        logger.error("임베딩 API 호출 타임아웃")
        return {
            "success": False,
            "error": "API 호출 시간 초과"
        }
        
    except requests.exceptions.ConnectionError:
        logger.error("임베딩 API 서버 연결 실패")
        return {
            "success": False,
            "error": "API 서버 연결 실패"
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"임베딩 API 호출 중 예외 발생: {str(e)}")
        return {
            "success": False,
            "error": f"API 호출 중 오류: {str(e)}"
        }
        
    except Exception as e:
        logger.error(f"임베딩 처리 중 예상치 못한 오류 발생: {str(e)}")
        return {
            "success": False,
            "error": f"처리 중 오류: {str(e)}"
        }


# 사용 예시
if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(level=logging.INFO)
    
    # 테스트
    test_text = "너는 정말 이상해"
    result = check_toxic_content(test_text)
    print(f"검사 결과: {result}")
    
    if result["success"]:
        print(f"유해성 감지: {result['toxicity_detected']}")
        print(f"유해성 점수: {result['toxicity_score']:.4f}")
        print(f"유해성 레벨: {result['toxicity_level']}")
    else:
        print(f"에러: {result['error']}")
