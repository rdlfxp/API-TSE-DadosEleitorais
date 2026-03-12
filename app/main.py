import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse

from app.config import settings
from app.schemas import (
    AgeStatsResponse,
    CandidateSearchResponse,
    ErrorResponse,
    FilterOptionsResponse,
    OfficialVacanciesResponse,
    GroupedDistributionResponse,
    OverviewResponse,
    RankingResponse,
    TimeSeriesResponse,
    TopCandidatesResponse,
    UFMapResponse,
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

ERROR_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Bad Request"},
    429: {"model": ErrorResponse, "description": "Too Many Requests"},
    422: {"model": ErrorResponse, "description": "Validation Error"},
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
            preferred_csv_path=file_path,
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


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-Id", str(uuid4()))
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
                content={"message": "Limite de requisicoes excedido para este IP."},
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
async def http_exception_handler(_: Request, exc: HTTPException):
    message = exc.detail if isinstance(exc.detail, str) else "Erro na requisicao."
    return JSONResponse(status_code=exc.status_code, content={"message": message})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, __: RequestValidationError):
    return JSONResponse(status_code=422, content={"message": "Parametros de consulta invalidos."})


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
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
) -> OverviewResponse:
    data = get_service().overview(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
    return OverviewResponse(**data)


@app.get("/v1/analytics/filtros", response_model=FilterOptionsResponse, responses=ERROR_RESPONSES)
def analytics_filter_options() -> FilterOptionsResponse:
    data = get_service().filter_options()
    return FilterOptionsResponse(**data)


@app.get("/v1/analytics/top-candidatos", response_model=TopCandidatesResponse, responses=ERROR_RESPONSES)
def analytics_top_candidates(
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> TopCandidatesResponse:
    data = get_service().top_candidates(
        ano=ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        top_n=top_n,
        page=page,
        page_size=page_size,
    )
    return TopCandidatesResponse(**data)


@app.get("/v1/analytics/candidatos", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES)
def analytics_candidates_search(
    query: str = Query(..., min_length=2, description="Busca por nome do candidato"),
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    municipio: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    data = get_service().search_candidates(
        query=query,
        ano=ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        page=page,
        page_size=page_size,
    )
    return CandidateSearchResponse(**data)


@app.get("/v1/analytics/distribuicao", response_model=GroupedDistributionResponse, responses=ERROR_RESPONSES)
def analytics_distribution(
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
    items = get_service().distribution(
        group_by=group_by,
        ano=ano,
        turno=turno,
        uf=uf,
        cargo=cargo,
        municipio=municipio,
        somente_eleitos=somente_eleitos,
    )
    if not items:
        raise HTTPException(status_code=400, detail="group_by invalido ou coluna ausente no dataset")
    return GroupedDistributionResponse(group_by=group_by, items=items)


@app.get("/v1/analytics/idade", response_model=AgeStatsResponse, responses=ERROR_RESPONSES)
def analytics_age(
    ano: int | None = None,
    turno: int | None = Query(default=None, ge=1, le=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    somente_eleitos: bool = True,
) -> AgeStatsResponse:
    data = get_service().age_stats(ano=ano, turno=turno, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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
