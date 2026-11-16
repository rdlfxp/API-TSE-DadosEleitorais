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
from app.infra.redis_cache import redis_delete, redis_get, redis_lock, redis_set


ANALYTICS_CACHE_LOCK = Lock()
ANALYTICS_CACHE: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
DATA_LAST_MODIFIED_TS: float | None = None
DATA_CACHE_VERSION: str | None = None

CACHE_STATS_LOCK = Lock()
CACHE_STATS: dict[str, int] = {"l1_hits": 0, "l2_hits": 0, "misses": 0, "single_flight_waits": 0}
ROUTE_STATS: dict[str, list[float]] = {}  # rota -> [chamadas, soma_ms, max_ms, hits]


def _count(name: str) -> None:
    with CACHE_STATS_LOCK:
        CACHE_STATS[name] += 1


def record_miss() -> None:
    _count("misses")


def record_route(endpoint: str, duration_ms: float, hit: bool) -> None:
    # Normaliza ids de candidato para nao criar uma linha por candidato.
    route = "/".join("{id}" if part.isdigit() and len(part) > 6 else part for part in endpoint.split("/"))
    with CACHE_STATS_LOCK:
        row = ROUTE_STATS.setdefault(route, [0, 0.0, 0.0, 0])
        row[0] += 1
        row[1] += duration_ms
        row[2] = max(row[2], duration_ms)
        row[3] += 1 if hit else 0


def cache_report() -> dict[str, Any]:
    with CACHE_STATS_LOCK:
        stats = dict(CACHE_STATS)
        routes = {
            route: {"calls": int(c), "avg_ms": round(total / c, 1), "max_ms": round(mx, 1), "hit_rate": round(h / c, 3)}
            for route, (c, total, mx, h) in sorted(ROUTE_STATS.items())
            if c
        }
    lookups = stats["l1_hits"] + stats["l2_hits"] + stats["misses"]
    entries, size = cache_stats()
    return {
        "snapshot": dataset_cache_version(),
        "l1": {"entries": entries, "bytes": size},
        "hits": stats["l1_hits"] + stats["l2_hits"],
        "misses": stats["misses"],
        "hit_rate": round((stats["l1_hits"] + stats["l2_hits"]) / lookups, 4) if lookups else None,
        "single_flight_waits": stats["single_flight_waits"],
        "routes": routes,
    }


_FLIGHTS_LOCK = Lock()
_FLIGHTS: dict[str, Lock] = {}


def single_flight(cache_key: str, lookup, compute):
    """Executa `compute` uma unica vez por chave entre requisicoes concorrentes.

    Dentro do processo usa um lock por chave; entre processos/replicas usa um lock no
    Redis (SET NX EX). Quem espera relê o cache em vez de refazer a consulta.
    """
    with _FLIGHTS_LOCK:
        flight = _FLIGHTS.setdefault(cache_key, Lock())
    waited = not flight.acquire(blocking=False)
    if waited:
        _count("single_flight_waits")
        flight.acquire()
    try:
        cached = lookup()
        if cached is not None:
            return cached, True
        lock_key = f"lock:{cache_key}"
        got = redis_lock(lock_key, ttl_seconds=int(settings.single_flight_wait_seconds) + 30)
        if got is False:
            deadline = time.monotonic() + float(settings.single_flight_wait_seconds)
            while time.monotonic() < deadline:
                time.sleep(0.25)
                cached = lookup()
                if cached is not None:
                    return cached, True
        try:
            return compute(), False
        finally:
            if got is True:
                redis_delete(lock_key)  # o TTL e so rede de seguranca; nao deixar a chave por 90 s
    finally:
        flight.release()
        with _FLIGHTS_LOCK:
            if not flight.locked() and _FLIGHTS.get(cache_key) is flight:
                _FLIGHTS.pop(cache_key, None)

EDGE_CACHE_180_DAYS_SECONDS = 60 * 60 * 24 * 180
EDGE_CACHE_365_DAYS_SECONDS = 60 * 60 * 24 * 365

_CLOSED = settings.analytics_closed_cache_ttl_seconds
MEMORY_CACHE_TTL_BY_ENDPOINT: dict[str, int] = {
    "/v1/analytics/filtros": 3600,
    "/v1/analytics/overview": _CLOSED,
    "/v1/analytics/top-candidatos": _CLOSED,
    "/v1/analytics/candidatos/search": 43200,
    "/v1/analytics/candidatos": 43200,
    "/v1/analytics/distribuicao": _CLOSED,
    "/v1/analytics/cor-raca-comparativo": _CLOSED,
    "/v1/analytics/ocupacao-genero": _CLOSED,
    "/v1/analytics/idade": _CLOSED,
    "/v1/analytics/serie-temporal": _CLOSED,
    "/v1/analytics/ranking": _CLOSED,
    "/v1/analytics/mapa-uf": _CLOSED,
    "/v1/analytics/vagas-oficiais": _CLOSED,
    "/v1/analytics/polarizacao": _CLOSED,
    "/v1/candidates/summary": _CLOSED,
    "/v1/candidates/electorate-profile": _CLOSED,
    "/v1/candidates/compare": _CLOSED,
}

EDGE_CACHE_TTL_BY_PATH: dict[str, int] = {
    "/v1/resultados": 10,
    "/v1/analytics/filtros": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/overview": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/top-candidatos": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/candidatos/search": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/candidatos": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/distribuicao": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/cor-raca-comparativo": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/ocupacao-genero": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/idade": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/serie-temporal": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/ranking": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/mapa-uf": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/vagas-oficiais": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/analytics/polarizacao": EDGE_CACHE_180_DAYS_SECONDS,
    "/v1/candidates/compare": EDGE_CACHE_365_DAYS_SECONDS,
}


L1_PROMOTE_TTL_SECONDS = 600


def cache_get(cache_key: str, allow_stale: bool = False) -> Any | None:
    """L1 (memoria) primeiro: acerto sem ida a rede. Depois o Redis (L2), promovendo para L1."""
    now = time.time()
    with ANALYTICS_CACHE_LOCK:
        cached = ANALYTICS_CACHE.get(cache_key)
        if cached is not None:
            expires_at, serialized_payload = cached
            if allow_stale or expires_at >= now:
                ANALYTICS_CACHE.move_to_end(cache_key)
                _count("l1_hits")
                return json.loads(serialized_payload)
            ANALYTICS_CACHE.pop(cache_key, None)

    redis_payload = redis_get(cache_key)
    if redis_payload is not None:
        try:
            value = json.loads(redis_payload)
        except Exception:
            redis_delete(cache_key)
            return None
        _count("l2_hits")
        _l1_store(cache_key, L1_PROMOTE_TTL_SECONDS, bytes(redis_payload))
        return value
    return None


def dataset_cache_version() -> str:
    if DATA_CACHE_VERSION:
        return DATA_CACHE_VERSION
    if DATA_LAST_MODIFIED_TS is None:
        return "no-data-version"
    return f"mtime:{int(DATA_LAST_MODIFIED_TS * 1000)}"


def versioned_cache_key(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return f"{dataset_cache_version()}:{namespace}:{encoded}"


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


def _l1_store(cache_key: str, ttl_seconds: int, serialized_payload: bytes) -> bool:
    """Guarda no L1 respeitando os limites de entradas, bytes totais e bytes por entrada."""
    entry_bytes = len(serialized_payload)
    max_entries = max(1, int(settings.analytics_cache_max_entries))
    max_total_bytes = max(0, int(settings.analytics_cache_max_total_mb)) * 1024 * 1024
    max_entry_bytes = max(0, int(settings.analytics_cache_max_entry_kb)) * 1024
    if (max_entry_bytes and entry_bytes > max_entry_bytes) or (max_total_bytes and entry_bytes > max_total_bytes):
        return False
    now = time.time()
    with ANALYTICS_CACHE_LOCK:
        cache_prune_locked(now)
        ANALYTICS_CACHE.pop(cache_key, None)
        if max_total_bytes > 0:
            while ANALYTICS_CACHE and (cache_total_bytes_locked() + entry_bytes) > max_total_bytes:
                ANALYTICS_CACHE.popitem(last=False)
        while ANALYTICS_CACHE and len(ANALYTICS_CACHE) >= max_entries:
            ANALYTICS_CACHE.popitem(last=False)
        ANALYTICS_CACHE[cache_key] = (now + ttl_seconds, serialized_payload)
        ANALYTICS_CACHE.move_to_end(cache_key)
    return True


def cache_set(cache_key: str, ttl_seconds: int, payload: Any) -> dict[str, Any]:
    if ttl_seconds <= 0:
        return {"stored": False, "reason": "ttl_disabled", "entry_bytes": 0, "cache_entries": 0, "cache_bytes": 0}

    try:
        serialized_payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    except Exception:
        return {"stored": False, "reason": "serialize_failed", "entry_bytes": 0, "cache_entries": 0, "cache_bytes": 0}

    entry_bytes = len(serialized_payload)
    redis_set(cache_key, serialized_payload, ttl_seconds)
    max_total_bytes = max(0, int(settings.analytics_cache_max_total_mb)) * 1024 * 1024
    max_entry_bytes = max(0, int(settings.analytics_cache_max_entry_kb)) * 1024

    if max_entry_bytes and entry_bytes > max_entry_bytes:
        cache_entries, cache_bytes = cache_stats()
        return {"stored": False, "reason": "entry_too_large", "entry_bytes": entry_bytes, "cache_entries": cache_entries, "cache_bytes": cache_bytes}
    if max_total_bytes and entry_bytes > max_total_bytes:
        cache_entries, cache_bytes = cache_stats()
        return {"stored": False, "reason": "exceeds_total_budget", "entry_bytes": entry_bytes, "cache_entries": cache_entries, "cache_bytes": cache_bytes}

    _l1_store(cache_key, ttl_seconds, serialized_payload)
    cache_entries, cache_bytes = cache_stats()
    return {"stored": True, "reason": "stored", "entry_bytes": entry_bytes, "cache_entries": cache_entries, "cache_bytes": cache_bytes}


def edge_cache_ttl_for_path(path: str) -> int | None:
    if path in EDGE_CACHE_TTL_BY_PATH:
        return EDGE_CACHE_TTL_BY_PATH[path]
    if path.startswith("/v1/candidates/") and path.endswith("/summary"):
        return EDGE_CACHE_365_DAYS_SECONDS
    if path.startswith("/v1/candidates/") and path.endswith("/vote-history"):
        return EDGE_CACHE_365_DAYS_SECONDS
    if path.startswith("/v1/candidates/") and path.endswith("/electorate-profile"):
        return EDGE_CACHE_365_DAYS_SECONDS
    if path.startswith("/v1/candidates/") and path.endswith("/vote-distribution"):
        return EDGE_CACHE_365_DAYS_SECONDS
    return None


def configure_cache_headers(request: Request, response: Any) -> tuple[str, int]:
    path = request.url.path
    status_code = response.status_code
    if request.method != "GET" or status_code >= 400 or path in {"/health", "/metrics"}:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Cache-Policy"] = "no-store"
        response.headers["X-Cache-TTL"] = "0"
        return "no-store", 0

    if path == "/v1/resultados":
        response.headers["Cache-Control"] = "public, max-age=10"
        response.headers["X-Cache-Policy"] = "public"
        response.headers["X-Cache-TTL"] = "10"
        return "public", 10

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
        data_version = dataset_cache_version()
        etag_hash = hashlib.sha256(f"{url_key}|{data_version}".encode("utf-8")).hexdigest()[:16]
        response.headers["ETag"] = f'W/"{etag_hash}"'
    if DATA_LAST_MODIFIED_TS is not None:
        response.headers["Last-Modified"] = formatdate(DATA_LAST_MODIFIED_TS, usegmt=True)
    response.headers["X-Cache-Policy"] = "public"
    response.headers["X-Cache-TTL"] = str(edge_ttl_seconds)
    return "public", edge_ttl_seconds
