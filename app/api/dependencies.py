from __future__ import annotations

import json
import time
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException, Request, Response

from app.core.config import settings
from app.core.logging import logger
from app.domain import (
    is_unscoped_candidate_search,
    normalize_search_cache_key,
    resolve_municipality_param,
    resolve_state_param,
    resolve_year_param,
)
from app.infra.cache import (
    MEMORY_CACHE_TTL_BY_ENDPOINT,
    cache_get,
    cache_set,
    estimate_payload_rows,
    process_memory_mb,
)


def get_service():
    import app.main as main_module

    if main_module.service is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Base analytics indisponivel. Ajuste ANALYTICS_DATA_PATH "
                "ou coloque o arquivo em data/curated/analytics.parquet (ou .csv)."
            ),
        )
    return main_module.service


def trace_id_from_request(request: Request | None) -> str:
    if request is None:
        return str(uuid4())
    trace_id = getattr(request.state, "trace_id", None)
    if isinstance(trace_id, str) and trace_id:
        return trace_id
    return request.headers.get("X-Request-Id", str(uuid4()))


def mark_legacy_candidate_search(response: Response) -> None:
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 30 Sep 2026 23:59:59 GMT"
    response.headers["Link"] = '</v1/analytics/candidatos/search>; rel="successor-version"'
    response.headers["Warning"] = '299 - "Endpoint legado. Use /v1/analytics/candidatos/search com o parametro q."'


def is_broad_top_candidates_scope(*, uf: str | None, cargo: str | None, partido: str | None, municipio: str | None) -> bool:
    return not any([uf, cargo, partido, municipio])


def resolve_municipal_scope_or_400(
    *,
    request: Request,
    service_instance: Any,
    candidate_id: str,
    year: int | None,
    office: str | None,
    round_filter: int | None,
    state: str | None,
    municipality: str | None,
) -> dict[str, Any]:
    try:
        return service_instance.resolve_municipal_scope(
            candidate_id=candidate_id,
            year=year,
            office=office,
            round_filter=round_filter,
            state=state,
            municipality=municipality,
            trace_id=trace_id_from_request(request),
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Nao foi possivel inferir municipio para candidate_id no recorte informado.",
        ) from None


def run_municipal_vote_flow(
    *,
    request: Request,
    service_instance: Any,
    candidate_id: str,
    level: str,
    year: int | None,
    office: str | None,
    round_filter: int | None,
    state: str | None,
    municipality: str | None,
    is_municipal_request: bool,
    operation: Callable[[str | None, str | None, int | None], dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any] | None, str | None, str | None, bool, int | None, bool]:
    scope_info: dict[str, Any] | None = None
    used_state = state
    used_municipality = municipality
    infer_scope = is_municipal_request and level == "zona" and (not state or not municipality)
    if infer_scope:
        scope_info = resolve_municipal_scope_or_400(
            request=request,
            service_instance=service_instance,
            candidate_id=candidate_id,
            year=year,
            office=office,
            round_filter=round_filter,
            state=state,
            municipality=municipality,
        )
        used_state = str(scope_info["used_uf"]) if scope_info.get("used_uf") else None
        used_municipality = str(scope_info["used_municipio"]) if scope_info.get("used_municipio") else None

    data = operation(used_state, used_municipality, round_filter)
    fallback_applied = False
    used_round = round_filter
    if is_municipal_request and round_filter == 2 and not data.get("items"):
        fallback_scope_info = scope_info
        fallback_state = used_state
        fallback_municipality = used_municipality
        if infer_scope:
            fallback_scope_info = resolve_municipal_scope_or_400(
                request=request,
                service_instance=service_instance,
                candidate_id=candidate_id,
                year=year,
                office=office,
                round_filter=1,
                state=state,
                municipality=municipality,
            )
            fallback_state = str(fallback_scope_info["used_uf"]) if fallback_scope_info.get("used_uf") else None
            fallback_municipality = str(fallback_scope_info["used_municipio"]) if fallback_scope_info.get("used_municipio") else None
        fallback_data = operation(fallback_state, fallback_municipality, 1)
        if fallback_data.get("items"):
            data = fallback_data
            scope_info = fallback_scope_info
            used_state = fallback_state
            used_municipality = fallback_municipality
            fallback_applied = True
            used_round = 1
    return data, scope_info, used_state, used_municipality, fallback_applied, used_round, infer_scope


def run_analytics_query(
    request: Request,
    endpoint: str,
    filters: dict[str, Any],
    operation: Callable[[], Any],
    *,
    cache_ttl_seconds: int = 0,
    fallback_factory: Callable[[], Any] | None = None,
) -> Any:
    cache_key = ""
    setattr(request.state, "memory_cache_status", "BYPASS")
    if cache_ttl_seconds > 0:
        cache_key = f"{endpoint}:{json.dumps(filters, sort_keys=True, ensure_ascii=False, default=str)}"
        cached = cache_get(cache_key)
        if cached is not None:
            setattr(request.state, "memory_cache_status", "HIT")
            return cached
        setattr(request.state, "memory_cache_status", "MISS")

    started_at = time.perf_counter()
    try:
        payload = operation()
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        response_bytes = 0
        cache_store = {"stored": False, "reason": "cache_disabled", "entry_bytes": 0, "cache_entries": 0, "cache_bytes": 0}
        logger.info(json.dumps({"event": "analytics_query_ok", "trace_id": trace_id_from_request(request), "endpoint": endpoint, "filters": filters, "duration_ms": duration_ms, "rows_returned": estimate_payload_rows(payload), "process_memory_mb": process_memory_mb()}, ensure_ascii=False))
        if cache_key:
            cache_store = cache_set(cache_key, cache_ttl_seconds, payload)
            response_bytes = int(cache_store.get("entry_bytes") or 0)
        if settings.analytics_cache_log_metrics:
            logger.info(json.dumps({"event": "analytics_cache_store", "trace_id": trace_id_from_request(request), "endpoint": endpoint, "filters": filters, "stored": bool(cache_store.get("stored")), "reason": str(cache_store.get("reason")), "entry_bytes": int(cache_store.get("entry_bytes") or 0), "cache_entries": int(cache_store.get("cache_entries") or 0), "cache_bytes": int(cache_store.get("cache_bytes") or 0), "response_bytes": response_bytes}, ensure_ascii=False))
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.exception(json.dumps({"event": "analytics_query_failed", "trace_id": trace_id_from_request(request), "endpoint": endpoint, "filters": filters, "duration_ms": duration_ms, "error_type": exc.__class__.__name__, "error_message": str(exc)}, ensure_ascii=False))
        if fallback_factory is not None:
            if cache_key:
                stale = cache_get(cache_key, allow_stale=True)
                if stale is not None:
                    setattr(request.state, "memory_cache_status", "STALE_HIT")
                    return stale
            return fallback_factory()
        raise


__all__ = [
    "MEMORY_CACHE_TTL_BY_ENDPOINT",
    "get_service",
    "is_broad_top_candidates_scope",
    "is_unscoped_candidate_search",
    "mark_legacy_candidate_search",
    "normalize_search_cache_key",
    "resolve_municipality_param",
    "resolve_state_param",
    "resolve_year_param",
    "run_analytics_query",
    "run_municipal_vote_flow",
    "trace_id_from_request",
]
