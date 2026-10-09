from fastapi import APIRouter, HTTPException, Query, Request

from app.api.errors import ERROR_RESPONSES
from app.core.config import settings
from app.infra.cache import cache_get, cache_set
from app.schemas.live_results import LiveResultsResponse
from app.services.live_results import LiveResultsNotPublished, LiveResultsService, LiveResultsUnavailable


router = APIRouter()
_service = LiveResultsService()


@router.get(
    "/v1/resultados",
    response_model=LiveResultsResponse,
    response_model_exclude_none=True,
    responses={**ERROR_RESPONSES, 404: {"description": "Recorte não publicado"}},
)
def live_results(
    request: Request,
    ano: int = Query(..., description="Ano par da eleição"),
    turno: int = Query(default=1, ge=1, le=2),
    cargo: str = Query(..., min_length=2),
    uf: str | None = Query(default=None, min_length=2, max_length=2),
    municipio: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=50),
) -> LiveResultsResponse:
    if ano % 2 != 0:
        raise HTTPException(status_code=422, detail="ano deve ser par.")
    normalized_uf = uf.strip().upper() if uf else None
    normalized_office = " ".join(cargo.strip().split())
    normalized_municipality = municipio.strip() if municipio else None
    municipal = ano % 4 == 0
    office_lower = normalized_office.lower()
    if municipal and (not normalized_uf or not normalized_municipality):
        raise HTTPException(status_code=422, detail="Eleições municipais exigem uf e municipio.")
    if not municipal and office_lower != "presidente" and not normalized_uf:
        raise HTTPException(status_code=422, detail="Eleições gerais exigem uf para este cargo.")
    if turno == 2 and office_lower not in {"presidente", "governador", "prefeito"}:
        raise HTTPException(status_code=422, detail="turno=2 só é válido para Presidente, Governador e Prefeito.")

    cache_key = f"live-results:{ano}:{turno}:{normalized_office.lower()}:{normalized_uf or ''}:{normalized_municipality or ''}:{page}:{page_size}"
    cached = cache_get(cache_key)
    if cached is not None:
        request.state.memory_cache_status = "HIT"
        return LiveResultsResponse(**cached)
    request.state.memory_cache_status = "MISS"
    try:
        payload = _service.fetch(
            year=ano,
            round_number=turno,
            office=normalized_office,
            state=normalized_uf,
            municipality=normalized_municipality,
        )
        normalized = _service.normalize(payload, page=page, page_size=page_size)
        ttl = min(15, max(1, int(settings.live_results_redis_ttl_seconds)))
        cache_set(cache_key, ttl, normalized)
        return LiveResultsResponse(**normalized)
    except LiveResultsNotPublished as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except LiveResultsUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

