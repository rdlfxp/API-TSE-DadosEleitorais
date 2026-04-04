from threading import Lock

from app.infra.redis_rate_limit import redis_rate_limit_increment


RATE_LIMIT_LOCK = Lock()
RATE_LIMIT_COUNTERS: dict[tuple[str, int], int] = {}
RATE_LIMIT_LAST_CLEANUP_BUCKET = -1


def get_rate_limit_bucket_count(client_ip: str, bucket: int, window_seconds: int) -> int:
    redis_count = redis_rate_limit_increment(client_ip, bucket, window_seconds)
    if redis_count is not None:
        return redis_count

    global RATE_LIMIT_LAST_CLEANUP_BUCKET
    key = (client_ip, bucket)
    with RATE_LIMIT_LOCK:
        RATE_LIMIT_COUNTERS[key] = RATE_LIMIT_COUNTERS.get(key, 0) + 1
        if RATE_LIMIT_LAST_CLEANUP_BUCKET != bucket:
            expired = [existing for existing in RATE_LIMIT_COUNTERS if existing[1] < bucket - 1]
            for old in expired:
                RATE_LIMIT_COUNTERS.pop(old, None)
            RATE_LIMIT_LAST_CLEANUP_BUCKET = bucket
        return RATE_LIMIT_COUNTERS[key]
