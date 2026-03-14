# utils/auth.py
"""
API Key Authentication
"""
import os
from fastapi import Header, HTTPException

def verify_api_key(x_api_key: str = Header(None, alias="X-API-KEY")) -> str:
    """
    X-API-KEY 헤더를 통한 API 키 검증

    Args:
        x_api_key: X-API-KEY 헤더 값

    Returns:
        str: 검증된 API 키

    Raises:
        HTTPException: API 키가 없거나 잘못된 경우
    """
    # 환경변수에서 올바른 API 키 가져오기
    expected_key = os.getenv("API_SECRET_KEY")

    if not expected_key:
        raise HTTPException(
            status_code=500,
            detail="API_SECRET_KEY not configured on server"
        )

    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="X-API-KEY header is required"
        )

    if x_api_key != expected_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )

    return x_api_key
