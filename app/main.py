import json
import logging
import time
from contextlib import asynccontextmanager
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
ANALYTICS_CACHE_LOCK = Lock()
ANALYTICS_CACHE: dict[str, tuple[float, Any]] = {}

ERROR_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Bad Request"},
    429: {"model": ErrorResponse, "description": "Too Many Requests"},
    422: {"model": ErrorResponse, "description": "Validation Error"},
    500: {"model": ErrorResponse, "description": "Internal Server Error"},
    503: {"model": ErrorResponse, "description": "Service Unavailable"},
}


@asynccontextmanager
async def lifespan(_: FastAPI):
    global service
    service = None
    file_path = Path(settings.analytics_data_path)
    selected_path = file_path
    if settings.prefer_parquet_if_available and file_path.suffix.lower() == ".csv":
        parquet_candidate = file_path.with_suffix(".parquet")
        if parquet_candidate.exists():
            selected_path = parquet_candidate

    if not selected_path.exists():
        downloaded_path = ensure_local_analytics_from_r2(
            preferred_path=file_path,
            prefer_parquet=settings.prefer_parquet_if_available,
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
                    "rows": (
                        int(len(service.dataframe))
                        if isinstance(service, AnalyticsService)
                        else int(service.row_count)
                    ),
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
    return None


def _cache_set(cache_key: str, ttl_seconds: int, payload: Any) -> None:
    if ttl_seconds <= 0:
        return
    with ANALYTICS_CACHE_LOCK:
        ANALYTICS_CACHE[cache_key] = (time.time() + ttl_seconds, payload)


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
    if cache_ttl_seconds > 0:
        cache_key = f"{endpoint}:{json.dumps(filters, sort_keys=True, ensure_ascii=False, default=str)}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

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
                    return stale
            return fallback_factory()
        raise


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
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
            expired = [k for k in RATE_LIMIT_COUNTERS if k[1] < bucket - 1]
            for old in expired:
                RATE_LIMIT_COUNTERS.pop(old, None)

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
                headers={"X-Request-Id": request_id, "Retry-After": str(window)},
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

    status_code = response.status_code
    with METRICS_LOCK:
        METRICS["last_request_duration_ms"] = duration_ms
        if 400 <= status_code < 500:
            METRICS["requests_4xx_total"] += 1
        elif status_code >= 500:
            METRICS["requests_5xx_total"] += 1

    logger.info(
        json.dumps(
            {
                "event": "http_request",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": duration_ms,
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
        cache_ttl_seconds=settings.analytics_cache_ttl_seconds,
    )
    return OverviewResponse(**data)


@app.get("/v1/analytics/filtros", response_model=FilterOptionsResponse, responses=ERROR_RESPONSES)
def analytics_filter_options(request: Request) -> FilterOptionsResponse:
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/filtros",
        filters={},
        operation=lambda: get_service().filter_options(),
        cache_ttl_seconds=settings.analytics_cache_ttl_seconds,
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
        cache_ttl_seconds=settings.analytics_top_candidates_cache_ttl_seconds,
        fallback_factory=fallback_payload,
    )
    return TopCandidatesResponse(**data)


@app.get("/v1/analytics/candidatos", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES)
def analytics_candidates_search(
    request: Request,
    query: str = Query(..., min_length=2, description="Busca por nome do candidato"),
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    filters = {
        "query": query,
        "ano": ano,
        "turno": turno,
        "uf": uf,
        "cargo": cargo,
        "municipio": municipio,
        "page": page,
        "page_size": page_size,
    }
    data = _run_analytics_query(
        request=request,
        endpoint="/v1/analytics/candidatos",
        filters=filters,
        operation=lambda: get_service().search_candidates(
            query=query,
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            page=page,
            page_size=page_size,
        ),
        cache_ttl_seconds=settings.analytics_cache_ttl_seconds,
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
        cache_ttl_seconds=settings.analytics_cache_ttl_seconds,
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
