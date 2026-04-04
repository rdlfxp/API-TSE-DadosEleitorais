import json
import time
from uuid import uuid4

from fastapi.responses import JSONResponse

from app.api.dependencies import trace_id_from_request
from app.api.errors import build_error_payload
from app.core.config import settings
from app.core.logging import logger
from app.infra.cache import configure_cache_headers, process_memory_mb
from app.infra.metrics import METRICS, METRICS_LOCK
from app.infra.rate_limit import get_rate_limit_bucket_count


def register_observability_middleware(app) -> None:
    @app.middleware("http")
    async def observability_middleware(request, call_next):
        request_id = request.headers.get("X-Request-Id", str(uuid4()))
        request.state.trace_id = request_id
        started_at = time.perf_counter()
        client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or (request.client.host if request.client else "unknown")

        if settings.rate_limit_enabled:
            window = max(1, int(settings.rate_limit_window_seconds))
            limit = max(1, int(settings.rate_limit_max_requests_per_ip))
            bucket = int(time.time() // window)
            current = get_rate_limit_bucket_count(client_ip, bucket, window)
            if current > limit:
                with METRICS_LOCK:
                    METRICS["requests_4xx_total"] += 1
                logger.info(json.dumps({"event": "rate_limit_exceeded", "request_id": request_id, "client_ip": client_ip, "path": request.url.path}, ensure_ascii=False))
                return JSONResponse(
                    status_code=429,
                    headers={"X-Request-Id": request_id, "Retry-After": str(window), "Cache-Control": "no-store, max-age=0", "X-Cache-Policy": "no-store", "X-Cache-TTL": "0", "X-Memory-Cache": "BYPASS"},
                    content=build_error_payload(429, "Limite de requisicoes excedido para este IP.", request_id),
                )

        with METRICS_LOCK:
            METRICS["requests_total"] += 1

        response = await call_next(request)
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        response.headers["X-Request-Id"] = request_id
        memory_cache_status = str(getattr(request.state, "memory_cache_status", "BYPASS"))
        response.headers["X-Memory-Cache"] = memory_cache_status

        status_code = response.status_code
        with METRICS_LOCK:
            METRICS["last_request_duration_ms"] = duration_ms
            if 400 <= status_code < 500:
                METRICS["requests_4xx_total"] += 1
            elif status_code >= 500:
                METRICS["requests_5xx_total"] += 1

        cache_policy, cache_ttl_seconds = configure_cache_headers(request, response)
        if request.url.path not in {"/health", "/metrics"}:
            logger.info(json.dumps({"event": "http_request", "request_id": trace_id_from_request(request), "method": request.method, "path": request.url.path, "status_code": status_code, "duration_ms": duration_ms, "cache_policy": cache_policy, "cache_ttl_seconds": cache_ttl_seconds, "memory_cache": memory_cache_status, "process_memory_mb": process_memory_mb()}, ensure_ascii=False))
        return response
