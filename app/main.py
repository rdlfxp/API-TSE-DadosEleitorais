from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from app.config import settings
from app.schemas import (
    AgeStatsResponse,
    FilterOptionsResponse,
    GroupedDistributionResponse,
    OverviewResponse,
    TopCandidatesResponse,
)
from app.services.analytics_service import AnalyticsService

app = FastAPI(title=settings.app_name, version=settings.app_version)

service: AnalyticsService | None = None


def get_service() -> AnalyticsService:
    if service is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Base analytics indisponivel. Ajuste ANALYTICS_DATA_PATH "
                "ou coloque o arquivo em data/analytics.csv."
            ),
        )
    return service


@app.on_event("startup")
def startup_event() -> None:
    global service
    file_path = Path(settings.analytics_data_path)
    if file_path.exists():
        service = AnalyticsService.from_csv(
            file_path=str(file_path),
            default_top_n=settings.default_top_n,
            max_top_n=settings.max_top_n,
            separator=settings.analytics_separator,
            encoding=settings.analytics_encoding,
        )


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "data_loaded": service is not None,
        "version": settings.app_version,
    }


@app.get("/v1/analytics/overview", response_model=OverviewResponse)
def analytics_overview(
    ano: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
) -> OverviewResponse:
    data = get_service().overview(ano=ano, uf=uf, cargo=cargo)
    return OverviewResponse(**data)


@app.get("/v1/analytics/filtros", response_model=FilterOptionsResponse)
def analytics_filter_options() -> FilterOptionsResponse:
    data = get_service().filter_options()
    return FilterOptionsResponse(**data)


@app.get("/v1/analytics/top-candidatos", response_model=TopCandidatesResponse)
def analytics_top_candidates(
    ano: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n),
) -> TopCandidatesResponse:
    items = get_service().top_candidates(ano=ano, uf=uf, cargo=cargo, top_n=top_n)
    return TopCandidatesResponse(top_n=top_n or settings.default_top_n, items=items)


@app.get("/v1/analytics/distribuicao", response_model=GroupedDistributionResponse)
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


@app.get("/v1/analytics/idade", response_model=AgeStatsResponse)
def analytics_age(
    ano: int | None = None,
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    cargo: str | None = None,
    somente_eleitos: bool = True,
) -> AgeStatsResponse:
    data = get_service().age_stats(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
    return AgeStatsResponse(**data)
