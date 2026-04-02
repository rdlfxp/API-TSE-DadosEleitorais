import json
import logging
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from email.utils import formatdate
import hashlib
from pathlib import Path
import resource
from threading import Lock
from typing import Any, Callable, Literal
import unicodedata
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, PlainTextResponse

from app.config import settings
from app.schemas import (
    AgeStatsResponse,
    CandidateSummaryResponse,
    CompareResponse,
    CandidateSearchResponse,
    CorRacaComparativoResponse,
    ElectorateProfileResponse,
    ErrorResponse,
    FilterOptionsResponse,
    OfficialVacanciesResponse,
    PolarizacaoResponse,
    GroupedDistributionResponse,
    OccupationGenderResponse,
    OverviewResponse,
    RankingResponse,
    TimeSeriesResponse,
    TopCandidatesResponse,
    UFMapResponse,
    VoteDistributionResponse,
    VoteHistoryResponse,
)
from app.services.analytics_service import AnalyticsService
from app.services.duckdb_analytics_service import DuckDBAnalyticsService
from app.services.r2_bootstrap import ensure_local_analytics_from_r2

service: AnalyticsService | DuckDBAnalyticsService | None = None
logger = logging.getLogger("api.meucandidato")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

METRICS_LOCK = Lock()
RATE_LIMIT_LOCK = Lock()
METRICS = {
    "requests_total": 0,
    "requests_5xx_total": 0,
    "requests_4xx_total": 0,
    "last_request_duration_ms": 0.0,
}
RATE_LIMIT_COUNTERS: dict[tuple[str, int], int] = {}
RATE_LIMIT_LAST_CLEANUP_BUCKET = -1
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

ERROR_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Bad Request"},
    429: {"model": ErrorResponse, "description": "Too Many Requests"},
    422: {"model": ErrorResponse, "description": "Validation Error"},
    500: {"model": ErrorResponse, "description": "Internal Server Error"},
    503: {"model": ErrorResponse, "description": "Service Unavailable"},
}


@asynccontextmanager
async def lifespan(_: FastAPI):
    global service, DATA_LAST_MODIFIED_TS
    service = None
    DATA_LAST_MODIFIED_TS = None
    file_path = Path(settings.analytics_data_path)
    selected_path = file_path
    if settings.prefer_parquet_if_available and file_path.suffix.lower() == ".csv":
        parquet_candidate = file_path.with_suffix(".parquet")
        if parquet_candidate.exists():
            selected_path = parquet_candidate
    elif settings.prefer_parquet_if_available and file_path.suffix.lower() == ".parquet":
        csv_candidate = file_path.with_suffix(".csv")
        if not selected_path.exists() and csv_candidate.exists():
            selected_path = csv_candidate

    if not selected_path.exists():
        downloaded_path = ensure_local_analytics_from_r2(
            file_path,
            settings.prefer_parquet_if_available,
        )
        if downloaded_path is not None:
            selected_path = downloaded_path
            logger.info(
                json.dumps(
                    {
                        "event": "startup_data_downloaded_from_r2",
                        "path": str(selected_path),
                    },
                    ensure_ascii=False,
                )
            )

    if selected_path.exists():
        try:
            DATA_LAST_MODIFIED_TS = selected_path.stat().st_mtime
        except OSError:
            DATA_LAST_MODIFIED_TS = None
        if settings.analytics_engine.lower() == "duckdb":
            service = DuckDBAnalyticsService.from_file(
                file_path=str(selected_path),
                default_top_n=settings.default_top_n,
                max_top_n=settings.max_top_n,
                separator=settings.analytics_separator,
                encoding=settings.analytics_encoding,
                materialize_table=settings.duckdb_materialize_table,
                create_indexes=settings.duckdb_create_indexes,
                memory_limit_mb=settings.duckdb_memory_limit_mb,
                threads=settings.duckdb_threads,
                database_path=settings.duckdb_database_path,
            )
        else:
            service = AnalyticsService.from_file(
                file_path=str(selected_path),
                default_top_n=settings.default_top_n,
                max_top_n=settings.max_top_n,
                separator=settings.analytics_separator,
                encoding=settings.analytics_encoding,
            )
        if isinstance(service, AnalyticsService):
            rows_loaded: int | None = int(len(service.dataframe))
        elif settings.duckdb_materialize_table:
            rows_loaded = int(service.row_count)
        else:
            rows_loaded = None
        logger.info(
            json.dumps(
                {
                    "event": "startup_data_loaded",
                    "path": str(selected_path),
                    "engine": settings.analytics_engine.lower(),
                    "duckdb_materialize_table": bool(settings.duckdb_materialize_table),
                    "duckdb_create_indexes": bool(settings.duckdb_create_indexes),
                    "duckdb_memory_limit_mb": int(settings.duckdb_memory_limit_mb),
                    "duckdb_threads": int(settings.duckdb_threads),
                    "duckdb_database_path": (
                        getattr(service, "_database_path", None) if isinstance(service, DuckDBAnalyticsService) else None
                    ),
                    "rows": rows_loaded,
                },
                ensure_ascii=False,
            )
        )
    else:
        logger.info(
            json.dumps(
                {
                    "event": "startup_data_missing",
                    "path": str(selected_path),
                    "hint": "Configure R2_* vars or provide local analytics file.",
                },
                ensure_ascii=False,
            )
        )
    yield
    if isinstance(service, DuckDBAnalyticsService):
        service.close()
    service = None
    DATA_LAST_MODIFIED_TS = None


app = FastAPI(title=settings.app_name, version=settings.app_version, lifespan=lifespan)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )
    compare_op = (
        openapi_schema.get("paths", {})
        .get("/v1/candidates/compare", {})
        .get("get", {})
    )
    for param in compare_op.get("parameters", []):
        if param.get("name") == "candidate_ids" and param.get("in") == "query":
            param["style"] = "form"
            param["explode"] = False
            schema = param.get("schema", {})
            if isinstance(schema, dict):
                schema.pop("openapi_extra", None)
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


MUNICIPAL_OFFICES = {"PREFEITO", "VICE-PREFEITO", "VEREADOR"}


def _normalize_ascii_upper(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii").upper()


def _is_municipal_office(value: str | None) -> bool:
    return _normalize_ascii_upper(value) in MUNICIPAL_OFFICES


def _resolve_state_param(state: str | None, uf: str | None) -> str | None:
    resolved_state = state or uf
    if resolved_state and resolved_state.strip().upper() in {"BR", "BRASIL"}:
        return None
    return resolved_state


def _resolve_municipality_param(municipality: str | None, municipio: str | None) -> str | None:
    resolved = municipality or municipio
    if resolved is None:
        return None
    normalized = _normalize_ascii_upper(resolved)
    normalized = " ".join(normalized.split())
    return normalized or None


def _resolve_year_param(ano: int | None, year: int | None) -> int | None:
    if ano is not None and year is not None and int(ano) != int(year):
        raise HTTPException(status_code=400, detail="Parametros conflitantes: ano e year com valores diferentes.")
    if year is not None:
        return int(year)
    if ano is not None:
        return int(ano)
    return None


def _normalize_search_cache_key(query: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(query or "").strip())
    ascii_only = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(ascii_only.lower().split())


def _is_unscoped_candidate_search(query: str, uf: str | None, cargo: str | None, partido: str | None) -> bool:
    if uf or cargo or partido:
        return False
    return len(_normalize_search_cache_key(query).replace(" ", "")) < 8


def _mark_legacy_candidate_search(response: Response) -> None:
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 30 Sep 2026 23:59:59 GMT"
    response.headers["Link"] = '</v1/analytics/candidatos/search>; rel="successor-version"'
    response.headers["Warning"] = (
        '299 - "Endpoint legado. Use /v1/analytics/candidatos/search com o parametro q."'
    )


def _is_broad_top_candidates_scope(
    *,
    uf: str | None,
    cargo: str | None,
    partido: str | None,
    municipio: str | None,
) -> bool:
    return not any([uf, cargo, partido, municipio])


def _run_municipal_vote_flow(
    *,
    request: Request,
    service_instance: AnalyticsService | DuckDBAnalyticsService,
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
        scope_info = _resolve_municipal_scope_or_400(
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
            fallback_scope_info = _resolve_municipal_scope_or_400(
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
            fallback_municipality = (
                str(fallback_scope_info["used_municipio"]) if fallback_scope_info.get("used_municipio") else None
            )
        fallback_data = operation(fallback_state, fallback_municipality, 1)
        if fallback_data.get("items"):
            data = fallback_data
            scope_info = fallback_scope_info
            used_state = fallback_state
            used_municipality = fallback_municipality
            fallback_applied = True
            used_round = 1
    return data, scope_info, used_state, used_municipality, fallback_applied, used_round, infer_scope


def _resolve_municipal_scope_or_400(
    *,
    request: Request,
    service_instance: AnalyticsService | DuckDBAnalyticsService,
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
            trace_id=_trace_id_from_request(request),
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Nao foi possivel inferir municipio para candidate_id no recorte informado.",
        ) from None


def get_service() -> AnalyticsService | DuckDBAnalyticsService:
    if service is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Base analytics indisponivel. Ajuste ANALYTICS_DATA_PATH "
                "ou coloque o arquivo em data/curated/analytics.parquet (ou .csv)."
            ),
        )
    return service


def _trace_id_from_request(request: Request | None) -> str:
    if request is None:
        return str(uuid4())
    trace_id = getattr(request.state, "trace_id", None)
    if isinstance(trace_id, str) and trace_id:
        return trace_id
    return request.headers.get("X-Request-Id", str(uuid4()))


def _error_code_for_status(status_code: int) -> str:
    if status_code == 400:
        return "BAD_REQUEST"
    if status_code == 422:
        return "VALIDATION_ERROR"
    if status_code == 429:
        return "RATE_LIMITED"
    if status_code == 503:
        return "SERVICE_UNAVAILABLE"
    if status_code >= 500:
        return "INTERNAL_ERROR"
    return "REQUEST_ERROR"


def _build_error_payload(status_code: int, message: str, trace_id: str) -> dict[str, Any]:
    return {
        "code": _error_code_for_status(status_code),
        "message": message,
        "traceId": trace_id,
        "retryable": status_code >= 500 or status_code in {429},
    }


def _cache_get(cache_key: str, allow_stale: bool = False) -> Any | None:
    now = time.time()
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


def _cache_prune_locked(now: float) -> None:
    expired_keys = [key for key, (expires_at, _) in ANALYTICS_CACHE.items() if expires_at < now]
    for key in expired_keys:
        ANALYTICS_CACHE.pop(key, None)


def _cache_total_bytes_locked() -> int:
    return sum(len(serialized_payload) for _, serialized_payload in ANALYTICS_CACHE.values())


def _cache_stats() -> tuple[int, int]:
    with ANALYTICS_CACHE_LOCK:
        return len(ANALYTICS_CACHE), _cache_total_bytes_locked()


def _estimate_payload_rows(payload: Any) -> int | None:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return len(items)
    return None


def _process_memory_mb() -> float | None:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    if usage <= 0:
        return None
    # Linux reports KB, macOS reports bytes.
    memory_mb = usage / 1024 if usage > 1024 * 1024 else usage / (1024 * 1024)
    return round(float(memory_mb), 2)


def _cache_set(cache_key: str, ttl_seconds: int, payload: Any) -> dict[str, Any]:
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
        cache_entries, cache_bytes = _cache_stats()
        return {
            "stored": False,
            "reason": "entry_too_large",
            "entry_bytes": entry_bytes,
            "cache_entries": cache_entries,
            "cache_bytes": cache_bytes,
        }
    if max_total_bytes and entry_bytes > max_total_bytes:
        cache_entries, cache_bytes = _cache_stats()
        return {
            "stored": False,
            "reason": "exceeds_total_budget",
            "entry_bytes": entry_bytes,
            "cache_entries": cache_entries,
            "cache_bytes": cache_bytes,
        }

    now = time.time()
    with ANALYTICS_CACHE_LOCK:
        _cache_prune_locked(now)
        ANALYTICS_CACHE.pop(cache_key, None)
        if max_total_bytes > 0:
            while ANALYTICS_CACHE and (_cache_total_bytes_locked() + entry_bytes) > max_total_bytes:
                ANALYTICS_CACHE.popitem(last=False)
        if len(ANALYTICS_CACHE) >= max_entries:
            while ANALYTICS_CACHE and len(ANALYTICS_CACHE) >= max_entries:
                ANALYTICS_CACHE.popitem(last=False)
        ANALYTICS_CACHE[cache_key] = (now + ttl_seconds, serialized_payload)
        ANALYTICS_CACHE.move_to_end(cache_key)
        return {
            "stored": True,
            "reason": "stored",
            "entry_bytes": entry_bytes,
            "cache_entries": len(ANALYTICS_CACHE),
            "cache_bytes": _cache_total_bytes_locked(),
        }


def _edge_cache_ttl_for_path(path: str) -> int | None:
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


def _configure_cache_headers(
    request: Request,
    response: JSONResponse | PlainTextResponse | Any,
) -> tuple[str, int]:
    path = request.url.path
    status_code = response.status_code
    if request.method != "GET" or status_code >= 400 or path in {"/health", "/metrics"}:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Cache-Policy"] = "no-store"
        response.headers["X-Cache-TTL"] = "0"
        return "no-store", 0

    edge_ttl_seconds = _edge_cache_ttl_for_path(path)
    if edge_ttl_seconds is None:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Cache-Policy"] = "no-store"
        response.headers["X-Cache-TTL"] = "0"
        return "no-store", 0

    response.headers["Cache-Control"] = (
        f"public, max-age=60, s-maxage={edge_ttl_seconds}, "
        "stale-while-revalidate=60, stale-if-error=300"
    )
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
        etag_source = f"{url_key}|{data_version}"
        etag_hash = hashlib.sha256(etag_source.encode("utf-8")).hexdigest()[:16]
        response.headers["ETag"] = f'W/"{etag_hash}"'
    if DATA_LAST_MODIFIED_TS is not None:
        response.headers["Last-Modified"] = formatdate(DATA_LAST_MODIFIED_TS, usegmt=True)
    response.headers["X-Cache-Policy"] = "public"
    response.headers["X-Cache-TTL"] = str(edge_ttl_seconds)
    return "public", edge_ttl_seconds


def _run_analytics_query(
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
        cached = _cache_get(cache_key)
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
        logger.info(
            json.dumps(
                {
                    "event": "analytics_query_ok",
                    "trace_id": _trace_id_from_request(request),
                    "endpoint": endpoint,
                    "filters": filters,
                    "duration_ms": duration_ms,
                    "rows_returned": _estimate_payload_rows(payload),
                    "process_memory_mb": _process_memory_mb(),
                },
                ensure_ascii=False,
            )
        )
        if cache_key:
            cache_store = _cache_set(cache_key, cache_ttl_seconds, payload)
            response_bytes = int(cache_store.get("entry_bytes") or 0)
        if settings.analytics_cache_log_metrics:
            logger.info(
                json.dumps(
                    {
                        "event": "analytics_cache_store",
                        "trace_id": _trace_id_from_request(request),
                        "endpoint": endpoint,
                        "filters": filters,
                        "stored": bool(cache_store.get("stored")),
                        "reason": str(cache_store.get("reason")),
                        "entry_bytes": int(cache_store.get("entry_bytes") or 0),
                        "cache_entries": int(cache_store.get("cache_entries") or 0),
                        "cache_bytes": int(cache_store.get("cache_bytes") or 0),
                        "response_bytes": response_bytes,
                    },
                    ensure_ascii=False,
                )
            )
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.exception(
            json.dumps(
                {
                    "event": "analytics_query_failed",
                    "trace_id": _trace_id_from_request(request),
                    "endpoint": endpoint,
                    "filters": filters,
                    "duration_ms": duration_ms,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                },
                ensure_ascii=False,
            )
        )
        if fallback_factory is not None:
            if cache_key:
                stale = _cache_get(cache_key, allow_stale=True)
                if stale is not None:
                    setattr(request.state, "memory_cache_status", "STALE_HIT")
                    return stale
            return fallback_factory()
        raise


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    global RATE_LIMIT_LAST_CLEANUP_BUCKET
    request_id = request.headers.get("X-Request-Id", str(uuid4()))
    request.state.trace_id = request_id
    started_at = time.perf_counter()
    client_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or (request.client.host if request.client else "unknown")
    )

    if settings.rate_limit_enabled:
        window = max(1, int(settings.rate_limit_window_seconds))
        limit = max(1, int(settings.rate_limit_max_requests_per_ip))
        bucket = int(time.time() // window)
        key = (client_ip, bucket)
        blocked = False
        with RATE_LIMIT_LOCK:
            RATE_LIMIT_COUNTERS[key] = RATE_LIMIT_COUNTERS.get(key, 0) + 1
            if RATE_LIMIT_COUNTERS[key] > limit:
                blocked = True
            if RATE_LIMIT_LAST_CLEANUP_BUCKET != bucket:
                expired = [k for k in RATE_LIMIT_COUNTERS if k[1] < bucket - 1]
                for old in expired:
                    RATE_LIMIT_COUNTERS.pop(old, None)
                RATE_LIMIT_LAST_CLEANUP_BUCKET = bucket

        if blocked:
            with METRICS_LOCK:
                METRICS["requests_4xx_total"] += 1
            logger.info(
                json.dumps(
                    {
                        "event": "rate_limit_exceeded",
                        "request_id": request_id,
                        "client_ip": client_ip,
                        "path": request.url.path,
                    },
                    ensure_ascii=False,
                )
            )
            return JSONResponse(
                status_code=429,
                headers={
                    "X-Request-Id": request_id,
                    "Retry-After": str(window),
                    "Cache-Control": "no-store, max-age=0",
                    "X-Cache-Policy": "no-store",
                    "X-Cache-TTL": "0",
                    "X-Memory-Cache": "BYPASS",
                },
                content=_build_error_payload(
                    status_code=429,
                    message="Limite de requisicoes excedido para este IP.",
                    trace_id=request_id,
                ),
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

    cache_policy, cache_ttl_seconds = _configure_cache_headers(request, response)

    if request.url.path not in {"/health", "/metrics"}:
        logger.info(
            json.dumps(
                {
                    "event": "http_request",
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "cache_policy": cache_policy,
                    "cache_ttl_seconds": cache_ttl_seconds,
                    "memory_cache": memory_cache_status,
                    "process_memory_mb": _process_memory_mb(),
                },
                ensure_ascii=False,
            )
        )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    message = exc.detail if isinstance(exc.detail, str) else "Erro na requisicao."
    return JSONResponse(
        status_code=exc.status_code,
        content=_build_error_payload(
            status_code=exc.status_code,
            message=message,
            trace_id=_trace_id_from_request(request),
        ),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, __: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=_build_error_payload(
            status_code=422,
            message="Parametros de consulta invalidos.",
            trace_id=_trace_id_from_request(request),
        ),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    trace_id = _trace_id_from_request(request)
    logger.exception(
        json.dumps(
            {
                "event": "unhandled_exception",
                "trace_id": trace_id,
                "path": request.url.path,
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            },
            ensure_ascii=False,
        )
    )
    return JSONResponse(
        status_code=500,
        content=_build_error_payload(
            status_code=500,
            message="Erro interno no servidor.",
            trace_id=trace_id,
        ),
    )


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "data_loaded": service is not None,
        "version": settings.app_version,
    }


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    with METRICS_LOCK:
        lines = [
            "# TYPE requests_total counter",
            f"requests_total {METRICS['requests_total']}",
            "# TYPE requests_4xx_total counter",
            f"requests_4xx_total {METRICS['requests_4xx_total']}",
            "# TYPE requests_5xx_total counter",
            f"requests_5xx_total {METRICS['requests_5xx_total']}",
            "# TYPE last_request_duration_ms gauge",
            f"last_request_duration_ms {METRICS['last_request_duration_ms']}",
        ]
    return "\n".join(lines) + "\n"


@app.get("/v1/analytics/overview", response_model=OverviewResponse, responses=ERROR_RESPONSES)
def analytics_overview(
    request: Request,
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> OverviewResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    filters = {"ano": resolved_ano, "turno": turno, "uf": uf, "cargo": cargo, "municipio": municipio}
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/overview",
        filters=filters,
        operation=lambda: get_service().overview(
            ano=resolved_ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/overview"],
    )
    return OverviewResponse(**data)


@app.get("/v1/analytics/filtros", response_model=FilterOptionsResponse, responses=ERROR_RESPONSES)
def analytics_filter_options(request: Request) -> FilterOptionsResponse:
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/filtros",
        filters={},
        operation=lambda: get_service().filter_options(),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/filtros"],
    )
    return FilterOptionsResponse(**data)


@app.get("/v1/analytics/top-candidatos", response_model=TopCandidatesResponse, responses=ERROR_RESPONSES)
def analytics_top_candidates(
    request: Request,
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    partido: str | None = None,
    municipio: str | None = None,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> TopCandidatesResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    requested_page_size = min(page_size or top_n or settings.default_top_n, settings.max_top_n)
    effective_page_size = requested_page_size
    if _is_broad_top_candidates_scope(uf=uf, cargo=cargo, partido=partido, municipio=municipio):
        effective_page_size = min(
            requested_page_size,
            max(1, int(settings.analytics_broad_query_max_page_size)),
        )
    filters = {
        "ano": resolved_ano,
        "turno": turno,
        "uf": uf,
        "cargo": cargo,
        "partido": partido,
        "municipio": municipio,
        "top_n": top_n,
        "page": page,
        "page_size": page_size,
        "effective_page_size": effective_page_size,
    }

    def fallback_payload() -> dict[str, Any]:
        return {
            "top_n": effective_page_size,
            "page": page,
            "page_size": effective_page_size,
            "total": 0,
            "total_pages": 0,
            "items": [],
        }

    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/top-candidatos",
        filters=filters,
        operation=lambda: get_service().top_candidates(
            ano=resolved_ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            partido=partido,
            municipio=municipio,
            top_n=effective_page_size,
            page=page,
            page_size=effective_page_size,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/top-candidatos"],
        fallback_factory=fallback_payload,
    )
    return TopCandidatesResponse(**data)


@app.get("/v1/analytics/candidatos/search", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES)
def analytics_candidates_text_search(
    request: Request,
    q: str = Query(..., min_length=2, description="Busca textual por nome do candidato"),
    ano: int | None = Query(default=None),
    year: int | None = Query(default=None),
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    partido: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    if resolved_ano is None:
        raise HTTPException(status_code=400, detail="Parametro obrigatorio ausente: ano (ou year).")
    if _is_unscoped_candidate_search(q, uf=uf, cargo=cargo, partido=partido):
        return CandidateSearchResponse(page=page, page_size=page_size, total=0, total_pages=0, items=[])
    filters = {
        "q": _normalize_search_cache_key(q),
        "ano": resolved_ano,
        "turno": turno,
        "uf": uf,
        "cargo": cargo,
        "partido": partido,
        "page": page,
        "page_size": page_size,
    }
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/candidatos/search",
        filters=filters,
        operation=lambda: get_service().search_candidates(
            q=q,
            ano=resolved_ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            partido=partido,
            page=page,
            page_size=page_size,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/candidatos/search"],
    )
    return CandidateSearchResponse(**data)


@app.get(
    "/v1/analytics/candidatos",
    response_model=CandidateSearchResponse,
    responses=ERROR_RESPONSES,
    deprecated=True,
)
def analytics_candidates_search_legacy(
    request: Request,
    response: Response,
    query: str = Query(..., min_length=2, description="Parametro legado. Use q em /v1/analytics/candidatos/search"),
    ano: int | None = Query(default=None),
    year: int | None = Query(default=None),
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    partido: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    _mark_legacy_candidate_search(response)
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    if resolved_ano is None:
        raise HTTPException(status_code=400, detail="Parametro obrigatorio ausente: ano (ou year).")
    if _is_unscoped_candidate_search(query, uf=uf, cargo=cargo, partido=partido):
        return CandidateSearchResponse(page=page, page_size=page_size, total=0, total_pages=0, items=[])
    filters = {
        "query": _normalize_search_cache_key(query),
        "ano": resolved_ano,
        "turno": turno,
        "uf": uf,
        "cargo": cargo,
        "partido": partido,
        "page": page,
        "page_size": page_size,
    }
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/candidatos",
        filters=filters,
        operation=lambda: get_service().search_candidates(
            q=query,
            ano=resolved_ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            partido=partido,
            page=page,
            page_size=page_size,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/candidatos"],
    )
    return CandidateSearchResponse(**data)


@app.get("/v1/candidates/{candidate_id}/summary", response_model=CandidateSummaryResponse, responses=ERROR_RESPONSES)
def candidate_summary(
    candidate_id: str,
    ano: int | None = Query(default=None),
    year: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    state: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    office: str | None = None,
) -> CandidateSummaryResponse:
    resolved_year = _resolve_year_param(ano=ano, year=year)
    resolved_state = _resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    data = get_service().candidate_summary(
        candidate_id=candidate_id,
        year=resolved_year,
        state=resolved_state,
        office=resolved_office,
    )
    return CandidateSummaryResponse(**data)


@app.get("/v1/candidates/{candidate_id}/vote-history", response_model=VoteHistoryResponse, responses=ERROR_RESPONSES)
def candidate_vote_history(
    request: Request,
    candidate_id: str,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    state: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    office: str | None = None,
) -> VoteHistoryResponse:
    resolved_state = _resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    filters = {"candidate_id": candidate_id, "state": resolved_state, "office": resolved_office}
    data = _run_analytics_query(
        request=request,
        endpoint=f"/v1/candidates/{candidate_id}/vote-history",
        filters=filters,
        operation=lambda: get_service().candidate_vote_history(
            candidate_id=candidate_id,
            state=resolved_state,
            office=resolved_office,
        ),
        cache_ttl_seconds=1800,
    )
    return VoteHistoryResponse(**data)


@app.get("/v1/candidates/{candidate_id}/electorate-profile", response_model=ElectorateProfileResponse, responses=ERROR_RESPONSES)
def candidate_electorate_profile(
    candidate_id: str,
    ano: int | None = Query(default=None),
    year: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    state: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    office: str | None = None,
    municipio: str | None = None,
    municipality: str | None = None,
) -> ElectorateProfileResponse:
    resolved_year = _resolve_year_param(ano=ano, year=year)
    resolved_state = _resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    resolved_municipality = _resolve_municipality_param(municipality=municipality, municipio=municipio)
    data = get_service().candidate_electorate_profile(
        candidate_id=candidate_id,
        year=resolved_year,
        state=resolved_state,
        office=resolved_office,
        municipality=resolved_municipality,
    )
    return ElectorateProfileResponse(**data)


@app.get("/v1/candidates/{candidate_id}/vote-distribution", response_model=VoteDistributionResponse, responses=ERROR_RESPONSES)
def candidate_vote_distribution(
    request: Request,
    candidate_id: str,
    level: Literal["macroregiao", "uf", "municipio", "zona"],
    year: int | None = None,
    ano: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
    cargo: str | None = None,
    round_: int | None = Query(default=None, ge=1, le=2, alias="round"),
    turno: int | None = Query(default=None, ge=1, le=2),
    municipality: str | None = None,
    municipio: str | None = None,
    sort_by: str | None = None,
    sort_order: Literal["asc", "desc"] | None = None,
    page: int | None = Query(default=None, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=500),
) -> VoteDistributionResponse:
    resolved_year = year if year is not None else ano
    resolved_state = _resolve_state_param(state=state, uf=uf)
    resolved_municipality = _resolve_municipality_param(municipality=municipality, municipio=municipio)
    resolved_office = office or cargo
    resolved_round = round_ if round_ is not None else turno
    trace_id = _trace_id_from_request(request)
    is_municipal_request = _is_municipal_office(resolved_office)
    if is_municipal_request:
        if resolved_year is None:
            raise HTTPException(status_code=400, detail="Parametro obrigatorio ausente para recorte municipal: ano.")

    service = get_service()
    data, scope_info, used_state, used_municipality, fallback_applied, used_round, infer_scope = _run_municipal_vote_flow(
        request=request,
        service_instance=service,
        candidate_id=candidate_id,
        level=level,
        year=resolved_year,
        office=resolved_office,
        round_filter=resolved_round,
        state=resolved_state,
        municipality=resolved_municipality,
        is_municipal_request=is_municipal_request,
        operation=lambda used_state_arg, used_municipality_arg, used_round_arg: service.candidate_vote_distribution(
            candidate_id=candidate_id,
            level=level,
            year=resolved_year,
            state=used_state_arg,
            office=resolved_office,
            round_filter=used_round_arg,
            municipality=used_municipality_arg,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            trace_id=trace_id,
        ),
    )
    if is_municipal_request:
        data["metadata"] = {
            "scope": "municipal",
            "inferred_geo": bool(scope_info and scope_info.get("inferred_geo")) if infer_scope else False,
            "inferred_uf": (scope_info.get("inferred_uf") if scope_info else None),
            "inferred_municipio": (scope_info.get("inferred_municipio") if scope_info else None),
            "requested_uf": resolved_state,
            "requested_municipio": resolved_municipality,
            "used_uf": used_state,
            "used_municipio": used_municipality,
            "requested_turno": resolved_round,
            "used_turno": used_round,
            "fallback_applied": fallback_applied,
            "no_data": len(data.get("items", [])) == 0,
            "disambiguation_applied": bool(scope_info and scope_info.get("disambiguation_applied")) if infer_scope else False,
            "uf": used_state,
            "municipio": used_municipality,
            "cargo": resolved_office,
            "ano": resolved_year,
            "traceId": trace_id,
        }
    return VoteDistributionResponse(**data)


@app.get("/v1/candidates/compare", response_model=CompareResponse, responses=ERROR_RESPONSES)
def candidates_compare(
    candidate_ids: list[str] = Query(
        ...,
        description="IDs de candidatos (CSV: 1,2 ou repetido: ?candidate_ids=1&candidate_ids=2)",
        openapi_extra={"style": "form", "explode": False},
    ),
    ano: int | None = Query(default=None),
    year: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    state: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    office: str | None = None,
) -> CompareResponse:
    parsed_ids: list[str] = []
    for value in candidate_ids:
        parsed_ids.extend([token.strip() for token in str(value).split(",") if token.strip()])
    if len(parsed_ids) < 2 or len(parsed_ids) > 4:
        raise HTTPException(status_code=400, detail="candidate_ids deve conter de 2 a 4 IDs.")
    resolved_year = _resolve_year_param(ano=ano, year=year)
    resolved_state = _resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    data = get_service().candidates_compare(
        candidate_ids=parsed_ids,
        year=resolved_year,
        state=resolved_state,
        office=resolved_office,
    )
    return CompareResponse(**data)


@app.get("/v1/analytics/distribuicao", response_model=GroupedDistributionResponse, responses=ERROR_RESPONSES)
def analytics_distribution(
    request: Request,
    group_by: str = Query(
        ...,
        description="Valores: status, genero, instrucao, cor_raca, estado_civil, ocupacao, cargo, uf",
    ),
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> GroupedDistributionResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    allowed_group_by = {"status", "genero", "instrucao", "cor_raca", "estado_civil", "ocupacao", "cargo", "uf"}
    if group_by not in allowed_group_by:
        raise HTTPException(status_code=400, detail="group_by invalido ou coluna ausente no dataset")
    filters = {
        "group_by": group_by,
        "ano": resolved_ano,
        "turno": turno,
        "uf": uf,
        "cargo": cargo,
        "municipio": municipio,
        "somente_eleitos": somente_eleitos,
    }
    items = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/distribuicao",
        filters=filters,
        operation=lambda: get_service().distribution(
            group_by=group_by,
            ano=resolved_ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/distribuicao"],
    )
    return GroupedDistributionResponse(group_by=group_by, items=items)


@app.get("/v1/analytics/cor-raca-comparativo", response_model=CorRacaComparativoResponse, responses=ERROR_RESPONSES)
def analytics_cor_raca_comparativo(
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> CorRacaComparativoResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    data = get_service().cor_raca_comparativo(
        ano=resolved_ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
    )
    return CorRacaComparativoResponse(**data)


@app.get("/v1/analytics/ocupacao-genero", response_model=OccupationGenderResponse, responses=ERROR_RESPONSES)
def analytics_occupation_gender(
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> OccupationGenderResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    items = get_service().occupation_gender_distribution(
        ano=resolved_ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        somente_eleitos=somente_eleitos,
    )
    return OccupationGenderResponse(items=items)


@app.get("/v1/analytics/idade", response_model=AgeStatsResponse, responses=ERROR_RESPONSES)
def analytics_age(
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = True,
) -> AgeStatsResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    data = get_service().age_stats(
        ano=resolved_ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        somente_eleitos=somente_eleitos,
    )
    return AgeStatsResponse(**data)


@app.get("/v1/analytics/serie-temporal", response_model=TimeSeriesResponse, responses=ERROR_RESPONSES)
def analytics_time_series(
    metric: str = Query(
        default="votos_nominais",
        description="Valores: votos_nominais, candidatos, eleitos, registros",
    ),
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> TimeSeriesResponse:
    items = get_service().time_series(
        metric=metric,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        somente_eleitos=somente_eleitos,
    )
    if not items:
        raise HTTPException(status_code=400, detail="metric invalida ou coluna ausente no dataset")
    return TimeSeriesResponse(metric=metric, items=items)


@app.get("/v1/analytics/ranking", response_model=RankingResponse, responses=ERROR_RESPONSES)
def analytics_ranking(
    group_by: str = Query(default="partido", description="Valores: candidato, partido, cargo, uf"),
    metric: str = Query(default="votos_nominais", description="Valores: votos_nominais, candidatos, eleitos, registros"),
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> RankingResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    effective_top_n = top_n or settings.default_top_n
    items = get_service().ranking(
        group_by=group_by,
        metric=metric,
        ano=resolved_ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        somente_eleitos=somente_eleitos,
        top_n=effective_top_n,
    )
    if not items:
        raise HTTPException(status_code=400, detail="group_by/metric invalido ou coluna ausente no dataset")
    return RankingResponse(group_by=group_by, metric=metric, top_n=effective_top_n, items=items)


@app.get("/v1/analytics/mapa-uf", response_model=UFMapResponse, responses=ERROR_RESPONSES)
def analytics_uf_map(
    metric: str = Query(default="votos_nominais", description="Valores: votos_nominais, candidatos, eleitos, registros"),
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> UFMapResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    items = get_service().uf_map(
        metric=metric,
        ano=resolved_ano,
        turno=turno,
        cargo=cargo,
        municipio=municipio,
        somente_eleitos=somente_eleitos,
    )
    if not items:
        raise HTTPException(status_code=400, detail="metric invalida ou coluna ausente no dataset")
    return UFMapResponse(metric=metric, items=items)


@app.get("/v1/analytics/vagas-oficiais", response_model=OfficialVacanciesResponse, responses=ERROR_RESPONSES)
def analytics_official_vacancies(
    group_by: str = Query(default="cargo", description="Valores: cargo, uf, municipio"),
    ano: int | None = None,
    year: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> OfficialVacanciesResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    data = get_service().official_vacancies(
        group_by=group_by,
        ano=resolved_ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
    )
    if not data.get("items"):
        raise HTTPException(
            status_code=400,
            detail=(
                "group_by invalido, coluna ausente no dataset ou combinacao sem vagas oficiais "
                "(ex.: group_by=municipio para cargo nao municipal)."
            ),
        )
    return OfficialVacanciesResponse(**data)


@app.get("/v1/analytics/polarizacao", response_model=PolarizacaoResponse, responses=ERROR_RESPONSES)
def analytics_polarizacao(
    request: Request,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    ano: int | None = None,
    year: int | None = None,
    map_mode: str | None = Query(default=None),
    ano_governador: int | None = None,
    turno_governador: int | None = Query(default=None, ge=1, le=2),
    ano_municipal: int | None = None,
    turno_municipal: int | None = Query(default=None, ge=1, le=2),
) -> PolarizacaoResponse:
    resolved_ano = _resolve_year_param(ano=ano, year=year)
    mode = (map_mode or "").strip().lower()
    if resolved_ano is not None:
        if mode == "statebygovernor" and ano_governador is None:
            ano_governador = resolved_ano
        elif mode == "municipalitybymayor" and ano_municipal is None:
            ano_municipal = resolved_ano
        elif ano_governador is None and ano_municipal is None:
            ano_governador = resolved_ano
            ano_municipal = resolved_ano

    filters = {
        "uf": uf,
        "ano_governador": ano_governador,
        "turno_governador": turno_governador,
        "ano_municipal": ano_municipal,
        "turno_municipal": turno_municipal,
        "map_mode": mode,
    }
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/polarizacao",
        filters=filters,
        operation=lambda: get_service().polarizacao(
            uf=uf,
            ano_governador=ano_governador,
            turno_governador=turno_governador,
            ano_municipal=ano_municipal,
            turno_municipal=turno_municipal,
            map_mode=map_mode,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/polarizacao"],
    )
    return PolarizacaoResponse(**data)
