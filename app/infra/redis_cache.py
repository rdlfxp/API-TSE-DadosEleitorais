from __future__ import annotations

from app.core.redis import get_redis_client


def redis_get(cache_key: str) -> bytes | None:
    client = get_redis_client()
    if client is None:
        return None
    try:
        return client.get(cache_key)
    except Exception:
        return None


def redis_set(cache_key: str, payload: bytes, ttl_seconds: int) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        client.set(cache_key, payload, ex=ttl_seconds)
    except Exception:
        return


def redis_delete(cache_key: str) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        client.delete(cache_key)
    except Exception:
        return
