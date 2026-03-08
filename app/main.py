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
    GroupedDistributionResponse,
    OverviewResponse,
    TopCandidatesResponse,
)
from app.services.analytics_service import AnalyticsService
from app.services.r2_bootstrap import ensure_local_analytics_from_r2

service: AnalyticsService | None = None
logger = logging.getLogger("api.meucandidato")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

METRICS_LOCK = Lock()
METRICS = {
    "requests_total": 0,
    "requests_5xx_total": 0,
    "requests_4xx_total": 0,
    "last_request_duration_ms": 0.0,
}

ERROR_RESPONSES = {
    400: {"model": ErrorResponse, "description": "Bad Request"},
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
                    "rows": int(len(service.dataframe)),
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


def get_service() -> AnalyticsService:
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
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
) -> OverviewResponse:
    data = get_service().overview(ano=ano, uf=uf, cargo=cargo)
    return OverviewResponse(**data)


@app.get("/v1/analytics/filtros", response_model=FilterOptionsResponse, responses=ERROR_RESPONSES)
def analytics_filter_options() -> FilterOptionsResponse:
    data = get_service().filter_options()
    return FilterOptionsResponse(**data)


@app.get("/v1/analytics/top-candidatos", response_model=TopCandidatesResponse, responses=ERROR_RESPONSES)
def analytics_top_candidates(
    ano: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> TopCandidatesResponse:
    items = get_service().top_candidates(ano=ano, uf=uf, cargo=cargo, top_n=top_n)
    return TopCandidatesResponse(top_n=top_n or settings.default_top_n, items=items)


@app.get("/v1/analytics/candidatos", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES)
def analytics_candidates_search(
    query: str = Query(..., min_length=2, description="Busca por nome do candidato"),
    ano: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> CandidateSearchResponse:
    data = get_service().search_candidates(
        query=query,
        ano=ano,
        uf=uf,
        cargo=cargo,
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
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    somente_eleitos: bool = False,
) -> GroupedDistributionResponse:
    items = get_service().distribution(
        group_by=group_by,
        ano=ano,
        uf=uf,
        cargo=cargo,
        somente_eleitos=somente_eleitos,
    )
    if not items:
        raise HTTPException(status_code=400, detail="group_by invalido ou coluna ausente no dataset")
    return GroupedDistributionResponse(group_by=group_by, items=items)


@app.get("/v1/analytics/idade", response_model=AgeStatsResponse, responses=ERROR_RESPONSES)
def analytics_age(
    ano: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    somente_eleitos: bool = True,
) -> AgeStatsResponse:
    data = get_service().age_stats(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
    return AgeStatsResponse(**data)
