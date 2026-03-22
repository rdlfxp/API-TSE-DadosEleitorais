import json
import logging
import time
from contextlib import asynccontextmanager
from email.utils import formatdate
import hashlib
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, PlainTextResponse

from app.config import settings
from app.schemas import (
    AgeStatsResponse,
    CandidateVoteMapResponse,
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
    ZoneFidelityResponse,
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
ANALYTICS_CACHE: dict[str, tuple[float, Any]] = {}
DATA_LAST_MODIFIED_TS: float | None = None

MEMORY_CACHE_TTL_BY_ENDPOINT: dict[str, int] = {
    "/v1/analytics/filtros": 3600,
    "/v1/analytics/overview": 600,
    "/v1/analytics/top-candidatos": 300,
    "/v1/analytics/candidatos/search": 120,
    "/v1/analytics/candidatos": 120,
    "/v1/analytics/distribuicao": 600,
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
        expires_at, payload = cached
        if allow_stale or expires_at >= now:
            return payload
        ANALYTICS_CACHE.pop(cache_key, None)
        return None


def _cache_prune_locked(now: float) -> None:
    expired_keys = [key for key, (expires_at, _) in ANALYTICS_CACHE.items() if expires_at < now]
    for key in expired_keys:
        ANALYTICS_CACHE.pop(key, None)


def _cache_set(cache_key: str, ttl_seconds: int, payload: Any) -> None:
    if ttl_seconds <= 0:
        return
    max_entries = max(1, int(settings.analytics_cache_max_entries))
    now = time.time()
    with ANALYTICS_CACHE_LOCK:
        _cache_prune_locked(now)
        if len(ANALYTICS_CACHE) >= max_entries:
            for key, _ in sorted(ANALYTICS_CACHE.items(), key=lambda item: item[1][0])[: len(ANALYTICS_CACHE) - max_entries + 1]:
                ANALYTICS_CACHE.pop(key, None)
        ANALYTICS_CACHE[cache_key] = (now + ttl_seconds, payload)


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
    if path.startswith("/v1/candidates/") and path.endswith("/vote-map"):
        return 1800
    if path.startswith("/v1/candidates/") and path.endswith("/zone-fidelity"):
        return 1800
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
    response.headers["Vary"] = "Accept-Encoding"
    body = getattr(response, "body", None)
    if isinstance(body, (bytes, bytearray)):
        etag_hash = hashlib.sha256(body).hexdigest()[:16]
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
        logger.info(
            json.dumps(
                {
                    "event": "analytics_query_ok",
                    "trace_id": _trace_id_from_request(request),
                    "endpoint": endpoint,
                    "filters": filters,
                    "duration_ms": duration_ms,
                },
                ensure_ascii=False,
            )
        )
        if cache_key:
            _cache_set(cache_key, cache_ttl_seconds, payload)
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
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> OverviewResponse:
    filters = {"ano": ano, "turno": turno, "uf": uf, "cargo": cargo, "municipio": municipio}
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/overview",
        filters=filters,
        operation=lambda: get_service().overview(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio),
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
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    partido: str | None = None,
    municipio: str | None = None,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> TopCandidatesResponse:
    effective_page_size = min(page_size or top_n or settings.default_top_n, settings.max_top_n)
    filters = {
        "ano": ano,
        "turno": turno,
        "uf": uf,
        "cargo": cargo,
        "partido": partido,
        "municipio": municipio,
        "top_n": top_n,
        "page": page,
        "page_size": page_size,
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
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            partido=partido,
            municipio=municipio,
            top_n=top_n,
            page=page,
            page_size=page_size,
        ),
        cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/top-candidatos"],
        fallback_factory=fallback_payload,
    )
    return TopCandidatesResponse(**data)


@app.get("/v1/analytics/candidatos/search", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES)
def analytics_candidates_text_search(
    request: Request,
    q: str = Query(..., min_length=2, description="Busca textual por nome do candidato"),
    ano: int = Query(...),
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    partido: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    filters = {
        "q": q,
        "ano": ano,
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
            ano=ano,
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
    query: str = Query(..., min_length=2, description="Parametro legado. Use q em /v1/analytics/candidatos/search"),
    ano: int = Query(...),
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    partido: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    filters = {
        "query": query,
        "ano": ano,
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
            ano=ano,
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
    year: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
) -> CandidateSummaryResponse:
    data = get_service().candidate_summary(candidate_id=candidate_id, year=year, state=state, office=office)
    return CandidateSummaryResponse(**data)


@app.get("/v1/candidates/{candidate_id}/vote-history", response_model=VoteHistoryResponse, responses=ERROR_RESPONSES)
def candidate_vote_history(
    candidate_id: str,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
) -> VoteHistoryResponse:
    data = get_service().candidate_vote_history(candidate_id=candidate_id, state=state, office=office)
    return VoteHistoryResponse(**data)


@app.get("/v1/candidates/{candidate_id}/electorate-profile", response_model=ElectorateProfileResponse, responses=ERROR_RESPONSES)
def candidate_electorate_profile(
    candidate_id: str,
    year: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
    municipality: str | None = None,
) -> ElectorateProfileResponse:
    data = get_service().candidate_electorate_profile(
        candidate_id=candidate_id,
        year=year,
        state=state,
        office=office,
        municipality=municipality,
    )
    return ElectorateProfileResponse(**data)


@app.get("/v1/candidates/{candidate_id}/vote-distribution", response_model=VoteDistributionResponse, responses=ERROR_RESPONSES)
def candidate_vote_distribution(
    candidate_id: str,
    level: Literal["macroregiao", "uf", "municipio", "zona"],
    year: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
) -> VoteDistributionResponse:
    data = get_service().candidate_vote_distribution(
        candidate_id=candidate_id,
        level=level,
        year=year,
        state=state,
        office=office,
    )
    return VoteDistributionResponse(**data)


@app.get("/v1/candidates/{candidate_id}/vote-map", response_model=CandidateVoteMapResponse, responses=ERROR_RESPONSES)
def candidate_vote_map(
    candidate_id: str,
    level: Literal["auto", "municipio", "zona"] = "auto",
    year: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
    municipality: str | None = None,
) -> CandidateVoteMapResponse:
    data = get_service().candidate_vote_map(
        candidate_id=candidate_id,
        level=level,
        year=year,
        state=state,
        office=office,
        municipality=municipality,
    )
    return CandidateVoteMapResponse(**data)


@app.get("/v1/candidates/{candidate_id}/zone-fidelity", response_model=ZoneFidelityResponse, responses=ERROR_RESPONSES)
def candidate_zone_fidelity(
    candidate_id: str,
    year: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
    include_geometry: bool = False,
) -> ZoneFidelityResponse:
    data = get_service().candidate_zone_fidelity(
        candidate_id=candidate_id,
        year=year,
        state=state,
        office=office,
        include_geometry=include_geometry,
    )
    return ZoneFidelityResponse(**data)


@app.get("/v1/candidates/compare", response_model=CompareResponse, responses=ERROR_RESPONSES)
def candidates_compare(
    candidate_ids: list[str] = Query(
        ...,
        description="IDs de candidatos (CSV: 1,2 ou repetido: ?candidate_ids=1&candidate_ids=2)",
        openapi_extra={"style": "form", "explode": False},
    ),
    year: int | None = None,
    state: str | None = Query(default=None, min_length=2, max_length=2),
    office: str | None = None,
) -> CompareResponse:
    parsed_ids: list[str] = []
    for value in candidate_ids:
        parsed_ids.extend([token.strip() for token in str(value).split(",") if token.strip()])
    if len(parsed_ids) < 2 or len(parsed_ids) > 4:
        raise HTTPException(status_code=400, detail="candidate_ids deve conter de 2 a 4 IDs.")
    data = get_service().candidates_compare(candidate_ids=parsed_ids, year=year, state=state, office=office)
    return CompareResponse(**data)


@app.get("/v1/analytics/distribuicao", response_model=GroupedDistributionResponse, responses=ERROR_RESPONSES)
def analytics_distribution(
    request: Request,
    group_by: str = Query(
        ...,
        description="Valores: status, genero, instrucao, cor_raca, estado_civil, ocupacao, cargo, uf",
    ),
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> GroupedDistributionResponse:
    allowed_group_by = {"status", "genero", "instrucao", "cor_raca", "estado_civil", "ocupacao", "cargo", "uf"}
    if group_by not in allowed_group_by:
        raise HTTPException(status_code=400, detail="group_by invalido ou coluna ausente no dataset")
    filters = {
        "group_by": group_by,
        "ano": ano,
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
            ano=ano,
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
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> CorRacaComparativoResponse:
    data = get_service().cor_raca_comparativo(
        ano=ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
    )
    return CorRacaComparativoResponse(**data)


@app.get("/v1/analytics/ocupacao-genero", response_model=OccupationGenderResponse, responses=ERROR_RESPONSES)
def analytics_occupation_gender(
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> OccupationGenderResponse:
    items = get_service().occupation_gender_distribution(
        ano=ano,
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
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = True,
) -> AgeStatsResponse:
    data = get_service().age_stats(
        ano=ano,
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
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> RankingResponse:
    effective_top_n = top_n or settings.default_top_n
    items = get_service().ranking(
        group_by=group_by,
        metric=metric,
        ano=ano,
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
    turno: int | None = Query(default=None, ge=1, le=2),
    cargo: str | None = None,
    municipio: str | None = None,
    somente_eleitos: bool = False,
) -> UFMapResponse:
    items = get_service().uf_map(
        metric=metric,
        ano=ano,
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
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> OfficialVacanciesResponse:
    data = get_service().official_vacancies(
        group_by=group_by,
        ano=ano,
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
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    ano: int | None = None,
    map_mode: str | None = Query(default=None),
    ano_governador: int | None = None,
    turno_governador: int | None = Query(default=None, ge=1, le=2),
    ano_municipal: int | None = None,
    turno_municipal: int | None = Query(default=None, ge=1, le=2),
) -> PolarizacaoResponse:
    mode = (map_mode or "").strip().lower()
    if ano is not None:
        if mode == "statebygovernor" and ano_governador is None:
            ano_governador = ano
        elif mode == "municipalitybymayor" and ano_municipal is None:
            ano_municipal = ano
        elif ano_governador is None and ano_municipal is None:
            ano_governador = ano
            ano_municipal = ano

    data = get_service().polarizacao(
        uf=uf,
        ano_governador=ano_governador,
        turno_governador=turno_governador,
        ano_municipal=ano_municipal,
        turno_municipal=turno_municipal,
        map_mode=map_mode,
    )
    return PolarizacaoResponse(**data)
