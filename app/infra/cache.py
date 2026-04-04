from __future__ import annotations

import hashlib
import json
import resource
import time
from collections import OrderedDict
from email.utils import formatdate
from threading import Lock
from typing import Any

from fastapi import Request

from app.core.config import settings
from app.infra.redis_cache import redis_delete, redis_get, redis_set


ANALYTICS_CACHE_LOCK = Lock()
ANALYTICS_CACHE: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
DATA_LAST_MODIFIED_TS: float | None = None

MEMORY_CACHE_TTL_BY_ENDPOINT: dict[str, int] = {
    "/v1/analytics/filtros": 3600,
    "/v1/analytics/overview": 600,
    "/v1/analytics/top-candidatos": 300,
    "/v1/analytics/candidatos/search": 120,
    "/v1/analytics/candidatos": 120,
    "/v1/analytics/distribuicao": 600,
    "/v1/analytics/polarizacao": 1200,
}

EDGE_CACHE_TTL_BY_PATH: dict[str, int] = {
    "/v1/analytics/filtros": 3600,
    "/v1/analytics/overview": 600,
    "/v1/analytics/top-candidatos": 300,
    "/v1/analytics/candidatos/search": 120,
    "/v1/analytics/candidatos": 120,
    "/v1/analytics/distribuicao": 600,
    "/v1/analytics/cor-raca-comparativo": 1200,
    "/v1/analytics/ocupacao-genero": 1200,
    "/v1/analytics/idade": 1200,
    "/v1/analytics/serie-temporal": 1200,
    "/v1/analytics/ranking": 600,
    "/v1/analytics/mapa-uf": 1200,
    "/v1/analytics/vagas-oficiais": 1800,
    "/v1/analytics/polarizacao": 1200,
    "/v1/candidates/compare": 600,
}


def cache_get(cache_key: str, allow_stale: bool = False) -> Any | None:
    now = time.time()
    redis_payload = redis_get(cache_key)
    if redis_payload is not None:
        try:
            return json.loads(redis_payload)
        except Exception:
            redis_delete(cache_key)

    with ANALYTICS_CACHE_LOCK:
        cached = ANALYTICS_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, serialized_payload = cached
        if allow_stale or expires_at >= now:
            ANALYTICS_CACHE.move_to_end(cache_key)
            return json.loads(serialized_payload)
        ANALYTICS_CACHE.pop(cache_key, None)
        return None


def cache_prune_locked(now: float) -> None:
    expired_keys = [key for key, (expires_at, _) in ANALYTICS_CACHE.items() if expires_at < now]
    for key in expired_keys:
        ANALYTICS_CACHE.pop(key, None)


def cache_total_bytes_locked() -> int:
    return sum(len(serialized_payload) for _, serialized_payload in ANALYTICS_CACHE.values())


def cache_stats() -> tuple[int, int]:
    with ANALYTICS_CACHE_LOCK:
        return len(ANALYTICS_CACHE), cache_total_bytes_locked()


def estimate_payload_rows(payload: Any) -> int | None:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return len(items)
    return None


def process_memory_mb() -> float | None:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    if usage <= 0:
        return None
    memory_mb = usage / 1024 if usage > 1024 * 1024 else usage / (1024 * 1024)
    return round(float(memory_mb), 2)


def cache_set(cache_key: str, ttl_seconds: int, payload: Any) -> dict[str, Any]:
    if ttl_seconds <= 0:
        return {"stored": False, "reason": "ttl_disabled", "entry_bytes": 0, "cache_entries": 0, "cache_bytes": 0}

    try:
        serialized_payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    except Exception:
        return {"stored": False, "reason": "serialize_failed", "entry_bytes": 0, "cache_entries": 0, "cache_bytes": 0}

    entry_bytes = len(serialized_payload)
    max_entries = max(1, int(settings.analytics_cache_max_entries))
    max_total_bytes = max(0, int(settings.analytics_cache_max_total_mb)) * 1024 * 1024
    max_entry_bytes = max(0, int(settings.analytics_cache_max_entry_kb)) * 1024

    if max_entry_bytes and entry_bytes > max_entry_bytes:
        cache_entries, cache_bytes = cache_stats()
        return {"stored": False, "reason": "entry_too_large", "entry_bytes": entry_bytes, "cache_entries": cache_entries, "cache_bytes": cache_bytes}
    if max_total_bytes and entry_bytes > max_total_bytes:
        cache_entries, cache_bytes = cache_stats()
        return {"stored": False, "reason": "exceeds_total_budget", "entry_bytes": entry_bytes, "cache_entries": cache_entries, "cache_bytes": cache_bytes}

    now = time.time()
    with ANALYTICS_CACHE_LOCK:
        cache_prune_locked(now)
        ANALYTICS_CACHE.pop(cache_key, None)
        if max_total_bytes > 0:
            while ANALYTICS_CACHE and (cache_total_bytes_locked() + entry_bytes) > max_total_bytes:
                ANALYTICS_CACHE.popitem(last=False)
        if len(ANALYTICS_CACHE) >= max_entries:
            while ANALYTICS_CACHE and len(ANALYTICS_CACHE) >= max_entries:
                ANALYTICS_CACHE.popitem(last=False)
        ANALYTICS_CACHE[cache_key] = (now + ttl_seconds, serialized_payload)
        ANALYTICS_CACHE.move_to_end(cache_key)

    redis_set(cache_key, serialized_payload, ttl_seconds)
    cache_entries, cache_bytes = cache_stats()
    return {"stored": True, "reason": "stored", "entry_bytes": entry_bytes, "cache_entries": cache_entries, "cache_bytes": cache_bytes}


def edge_cache_ttl_for_path(path: str) -> int | None:
    if path in EDGE_CACHE_TTL_BY_PATH:
        return EDGE_CACHE_TTL_BY_PATH[path]
    if path.startswith("/v1/candidates/") and path.endswith("/summary"):
        return 1800
    if path.startswith("/v1/candidates/") and path.endswith("/vote-history"):
        return 1800
    if path.startswith("/v1/candidates/") and path.endswith("/electorate-profile"):
        return 1800
    if path.startswith("/v1/candidates/") and path.endswith("/vote-distribution"):
        return 1200
    return None


def configure_cache_headers(request: Request, response: Any) -> tuple[str, int]:
    path = request.url.path
    status_code = response.status_code
    if request.method != "GET" or status_code >= 400 or path in {"/health", "/metrics"}:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Cache-Policy"] = "no-store"
        response.headers["X-Cache-TTL"] = "0"
        return "no-store", 0

    edge_ttl_seconds = edge_cache_ttl_for_path(path)
    if edge_ttl_seconds is None:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Cache-Policy"] = "no-store"
        response.headers["X-Cache-TTL"] = "0"
        return "no-store", 0

    response.headers["Cache-Control"] = f"public, max-age=60, s-maxage={edge_ttl_seconds}, stale-while-revalidate=60, stale-if-error=300"
    existing_vary = str(response.headers.get("Vary", "")).strip()
    if existing_vary:
        vary_values = {value.strip() for value in existing_vary.split(",") if value.strip()}
        vary_values.add("Accept-Encoding")
        response.headers["Vary"] = ", ".join(sorted(vary_values))
    else:
        response.headers["Vary"] = "Accept-Encoding"

    content_type = str(response.headers.get("content-type", "")).lower()
    if "application/json" in content_type:
        url_key = request.url.path
        if request.url.query:
            url_key = f"{url_key}?{request.url.query}"
        data_version = str(int(DATA_LAST_MODIFIED_TS)) if DATA_LAST_MODIFIED_TS is not None else "no-data-mtime"
        etag_hash = hashlib.sha256(f"{url_key}|{data_version}".encode("utf-8")).hexdigest()[:16]
        response.headers["ETag"] = f'W/"{etag_hash}"'
    if DATA_LAST_MODIFIED_TS is not None:
        response.headers["Last-Modified"] = formatdate(DATA_LAST_MODIFIED_TS, usegmt=True)
    response.headers["X-Cache-Policy"] = "public"
    response.headers["X-Cache-TTL"] = str(edge_ttl_seconds)
    return "public", edge_ttl_seconds
