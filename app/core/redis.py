from __future__ import annotations

from typing import Any

from app.core.config import settings

try:
    import redis
except Exception:  # pragma: no cover
    redis = None


_redis_client: Any | None = None


def get_redis_client() -> Any | None:
    return _redis_client


def initialize_redis() -> Any | None:
    global _redis_client
    if not settings.redis_enabled or not settings.redis_url or redis is None:
        _redis_client = None
        return None
    if _redis_client is None:
        _redis_client = redis.from_url(settings.redis_url, decode_responses=False)
    return _redis_client


def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.close()
        except Exception:
            pass
    _redis_client = None
