from __future__ import annotations

from app.core.redis import get_redis_client


def redis_rate_limit_increment(client_ip: str, bucket: int, ttl_seconds: int) -> int | None:
    client = get_redis_client()
    if client is None:
        return None
    key = f"rate-limit:{client_ip}:{bucket}"
    try:
        current = client.incr(key)
        if current == 1:
            client.expire(key, ttl_seconds)
        return int(current)
    except Exception:
        return None
