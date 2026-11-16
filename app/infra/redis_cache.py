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


def redis_lock(lock_key: str, ttl_seconds: int) -> bool | None:
    """True se adquiriu o lock, False se outro processo o detem, None se Redis indisponivel."""
    client = get_redis_client()
    if client is None:
        return None
    try:
        return bool(client.set(lock_key, b"1", nx=True, ex=ttl_seconds))
    except Exception:
        return None


def redis_info() -> dict:
    client = get_redis_client()
    if client is None:
        return {"enabled": False}
    try:
        stats = client.info("stats")
        memory = client.info("memory")
        hits = int(stats.get("keyspace_hits", 0))
        misses = int(stats.get("keyspace_misses", 0))
        return {
            "enabled": True,
            "connected": True,
            "keys": int(client.dbsize()),
            "used_memory_mb": round(int(memory.get("used_memory", 0)) / (1024 * 1024), 2),
            "keyspace_hits": hits,
            "keyspace_misses": misses,
            "hit_rate": round(hits / (hits + misses), 4) if (hits + misses) else None,
        }
    except Exception as exc:
        return {"enabled": True, "connected": False, "error": exc.__class__.__name__}
