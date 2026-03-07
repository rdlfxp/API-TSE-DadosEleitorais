from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import settings
from app.schemas import (
    AgeStatsResponse,
    ErrorResponse,
    FilterOptionsResponse,
    GroupedDistributionResponse,
    OverviewResponse,
    TopCandidatesResponse,
)
from app.services.analytics_service import AnalyticsService

service: AnalyticsService | None = None
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
    if file_path.exists():
        service = AnalyticsService.from_csv(
            file_path=str(file_path),
            default_top_n=settings.default_top_n,
            max_top_n=settings.max_top_n,
            separator=settings.analytics_separator,
            encoding=settings.analytics_encoding,
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
                "ou coloque o arquivo em data/curated/analytics.csv."
            ),
        )
    return service


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
