from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.api.dependencies import (
    MEMORY_CACHE_TTL_BY_ENDPOINT,
    get_service,
    is_broad_top_candidates_scope,
    is_unscoped_candidate_search,
    mark_legacy_candidate_search,
    normalize_search_cache_key,
    resolve_year_param,
    run_analytics_query,
)
from app.api.errors import ERROR_RESPONSES
from app.core.config import settings
from app.schemas import (
    AgeStatsResponse,
    CandidateSearchResponse,
    CorRacaComparativoResponse,
    FilterOptionsResponse,
    GroupedDistributionResponse,
    OccupationGenderResponse,
    OfficialVacanciesResponse,
    OverviewResponse,
    PolarizacaoResponse,
    RankingResponse,
    TimeSeriesResponse,
    TopCandidatesResponse,
    UFMapResponse,
)


router = APIRouter()


@router.get("/v1/analytics/overview", response_model=OverviewResponse, responses=ERROR_RESPONSES)
def analytics_overview(request: Request, ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None) -> OverviewResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    filters = {"ano": resolved_ano, "turno": turno, "uf": uf, "cargo": cargo, "municipio": municipio}
    data = run_analytics_query(request, "/v1/analytics/overview", filters, lambda: get_service().overview(ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/overview"])
    return OverviewResponse(**data)


@router.get("/v1/analytics/filtros", response_model=FilterOptionsResponse, responses=ERROR_RESPONSES)
def analytics_filter_options(request: Request) -> FilterOptionsResponse:
    data = run_analytics_query(request, "/v1/analytics/filtros", {}, lambda: get_service().filter_options(), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/filtros"])
    return FilterOptionsResponse(**data)


@router.get("/v1/analytics/top-candidatos", response_model=TopCandidatesResponse, responses=ERROR_RESPONSES)
def analytics_top_candidates(request: Request, ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, partido: str | None = None, municipio: str | None = None, top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n), page: int = Query(default=1, ge=1), page_size: int | None = Query(default=None, ge=1, le=settings.max_top_n)) -> TopCandidatesResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    requested_page_size = min(page_size or top_n or settings.default_top_n, settings.max_top_n)
    effective_page_size = requested_page_size
    if is_broad_top_candidates_scope(uf=uf, cargo=cargo, partido=partido, municipio=municipio):
        effective_page_size = min(requested_page_size, max(1, int(settings.analytics_broad_query_max_page_size)))
    filters = {"ano": resolved_ano, "turno": turno, "uf": uf, "cargo": cargo, "partido": partido, "municipio": municipio, "top_n": top_n, "page": page, "page_size": page_size, "effective_page_size": effective_page_size}
    def fallback_payload() -> dict[str, object]:
        return {"top_n": effective_page_size, "page": page, "page_size": effective_page_size, "total": 0, "total_pages": 0, "items": []}

    data = run_analytics_query(request, "/v1/analytics/top-candidatos", filters, lambda: get_service().top_candidates(ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, partido=partido, municipio=municipio, top_n=effective_page_size, page=page, page_size=effective_page_size), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/top-candidatos"], fallback_factory=fallback_payload)
    return TopCandidatesResponse(**data)


@router.get("/v1/analytics/candidatos/search", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES)
def analytics_candidates_text_search(request: Request, q: str = Query(..., min_length=2, description="Busca textual por nome do candidato"), ano: int | None = Query(default=None), year: int | None = Query(default=None), turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, partido: str | None = None, page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=100)) -> CandidateSearchResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    if resolved_ano is None:
        raise HTTPException(status_code=400, detail="Parametro obrigatorio ausente: ano (ou year).")
    if is_unscoped_candidate_search(q, uf=uf, cargo=cargo, partido=partido):
        return CandidateSearchResponse(page=page, page_size=page_size, total=0, total_pages=0, items=[])
    filters = {"q": normalize_search_cache_key(q), "ano": resolved_ano, "turno": turno, "uf": uf, "cargo": cargo, "partido": partido, "page": page, "page_size": page_size}
    data = run_analytics_query(request, "/v1/analytics/candidatos/search", filters, lambda: get_service().search_candidates(q=q, ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, partido=partido, page=page, page_size=page_size), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/candidatos/search"])
    return CandidateSearchResponse(**data)


@router.get("/v1/analytics/candidatos", response_model=CandidateSearchResponse, responses=ERROR_RESPONSES, deprecated=True)
def analytics_candidates_search_legacy(request: Request, response: Response, query: str = Query(..., min_length=2, description="Parametro legado. Use q em /v1/analytics/candidatos/search"), ano: int | None = Query(default=None), year: int | None = Query(default=None), turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, partido: str | None = None, page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=100)) -> CandidateSearchResponse:
    mark_legacy_candidate_search(response)
    resolved_ano = resolve_year_param(ano=ano, year=year)
    if resolved_ano is None:
        raise HTTPException(status_code=400, detail="Parametro obrigatorio ausente: ano (ou year).")
    if is_unscoped_candidate_search(query, uf=uf, cargo=cargo, partido=partido):
        return CandidateSearchResponse(page=page, page_size=page_size, total=0, total_pages=0, items=[])
    filters = {"query": normalize_search_cache_key(query), "ano": resolved_ano, "turno": turno, "uf": uf, "cargo": cargo, "partido": partido, "page": page, "page_size": page_size}
    data = run_analytics_query(request, "/v1/analytics/candidatos", filters, lambda: get_service().search_candidates(q=query, ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, partido=partido, page=page, page_size=page_size), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/candidatos"])
    return CandidateSearchResponse(**data)


@router.get("/v1/analytics/distribuicao", response_model=GroupedDistributionResponse, responses=ERROR_RESPONSES)
def analytics_distribution(request: Request, group_by: str = Query(..., description="Valores: status, genero, instrucao, cor_raca, estado_civil, ocupacao, cargo, uf"), ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None, somente_eleitos: bool = False) -> GroupedDistributionResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    allowed_group_by = {"status", "genero", "instrucao", "cor_raca", "estado_civil", "ocupacao", "cargo", "uf"}
    if group_by not in allowed_group_by:
        raise HTTPException(status_code=400, detail="group_by invalido ou coluna ausente no dataset")
    filters = {"group_by": group_by, "ano": resolved_ano, "turno": turno, "uf": uf, "cargo": cargo, "municipio": municipio, "somente_eleitos": somente_eleitos}
    items = run_analytics_query(request, "/v1/analytics/distribuicao", filters, lambda: get_service().distribution(group_by=group_by, ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio, somente_eleitos=somente_eleitos), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/distribuicao"])
    return GroupedDistributionResponse(group_by=group_by, items=items)


@router.get("/v1/analytics/cor-raca-comparativo", response_model=CorRacaComparativoResponse, responses=ERROR_RESPONSES)
def analytics_cor_raca_comparativo(ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None) -> CorRacaComparativoResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    return CorRacaComparativoResponse(**get_service().cor_raca_comparativo(ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio))


@router.get("/v1/analytics/ocupacao-genero", response_model=OccupationGenderResponse, responses=ERROR_RESPONSES)
def analytics_occupation_gender(ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None, somente_eleitos: bool = False) -> OccupationGenderResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    return OccupationGenderResponse(items=get_service().occupation_gender_distribution(ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio, somente_eleitos=somente_eleitos))


@router.get("/v1/analytics/idade", response_model=AgeStatsResponse, responses=ERROR_RESPONSES)
def analytics_age(ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None, somente_eleitos: bool = True) -> AgeStatsResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    return AgeStatsResponse(**get_service().age_stats(ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio, somente_eleitos=somente_eleitos))


@router.get("/v1/analytics/serie-temporal", response_model=TimeSeriesResponse, responses=ERROR_RESPONSES)
def analytics_time_series(metric: str = Query(default="votos_nominais", description="Valores: votos_nominais, candidatos, eleitos, registros"), turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None, somente_eleitos: bool = False) -> TimeSeriesResponse:
    items = get_service().time_series(metric=metric, turno=turno, uf=uf, cargo=cargo, municipio=municipio, somente_eleitos=somente_eleitos)
    if not items:
        raise HTTPException(status_code=400, detail="metric invalida ou coluna ausente no dataset")
    return TimeSeriesResponse(metric=metric, items=items)


@router.get("/v1/analytics/ranking", response_model=RankingResponse, responses=ERROR_RESPONSES)
def analytics_ranking(group_by: str = Query(default="partido", description="Valores: candidato, partido, cargo, uf"), metric: str = Query(default="votos_nominais", description="Valores: votos_nominais, candidatos, eleitos, registros"), ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None, somente_eleitos: bool = False, top_n: int | None = Query(default=None, ge=1, le=settings.max_top_n)) -> RankingResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    effective_top_n = top_n or settings.default_top_n
    items = get_service().ranking(group_by=group_by, metric=metric, ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio, somente_eleitos=somente_eleitos, top_n=effective_top_n)
    if not items:
        raise HTTPException(status_code=400, detail="group_by/metric invalido ou coluna ausente no dataset")
    return RankingResponse(group_by=group_by, metric=metric, top_n=effective_top_n, items=items)


@router.get("/v1/analytics/mapa-uf", response_model=UFMapResponse, responses=ERROR_RESPONSES)
def analytics_uf_map(metric: str = Query(default="votos_nominais", description="Valores: votos_nominais, candidatos, eleitos, registros"), ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), cargo: str | None = None, municipio: str | None = None, somente_eleitos: bool = False) -> UFMapResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    items = get_service().uf_map(metric=metric, ano=resolved_ano, turno=turno, cargo=cargo, municipio=municipio, somente_eleitos=somente_eleitos)
    if not items:
        raise HTTPException(status_code=400, detail="metric invalida ou coluna ausente no dataset")
    return UFMapResponse(metric=metric, items=items)


@router.get("/v1/analytics/vagas-oficiais", response_model=OfficialVacanciesResponse, responses=ERROR_RESPONSES)
def analytics_official_vacancies(group_by: str = Query(default="cargo", description="Valores: cargo, uf, municipio"), ano: int | None = None, year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, municipio: str | None = None) -> OfficialVacanciesResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    data = get_service().official_vacancies(group_by=group_by, ano=resolved_ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
    if not data.get("items"):
        raise HTTPException(status_code=400, detail="group_by invalido, coluna ausente no dataset ou combinacao sem vagas oficiais (ex.: group_by=municipio para cargo nao municipal).")
    return OfficialVacanciesResponse(**data)


@router.get("/v1/analytics/polarizacao", response_model=PolarizacaoResponse, responses=ERROR_RESPONSES)
def analytics_polarizacao(request: Request, uf: str | None = Query(default=None, min_length=2, max_length=2), ano: int | None = None, year: int | None = None, map_mode: str | None = Query(default=None), ano_governador: int | None = None, turno_governador: int | None = Query(default=None, ge=1, le=2), ano_municipal: int | None = None, turno_municipal: int | None = Query(default=None, ge=1, le=2)) -> PolarizacaoResponse:
    resolved_ano = resolve_year_param(ano=ano, year=year)
    mode = (map_mode or "").strip().lower()
    if resolved_ano is not None:
        if mode == "statebygovernor" and ano_governador is None:
            ano_governador = resolved_ano
        elif mode == "municipalitybymayor" and ano_municipal is None:
            ano_municipal = resolved_ano
        elif ano_governador is None and ano_municipal is None:
            ano_governador = resolved_ano
            ano_municipal = resolved_ano
    filters = {"uf": uf, "ano_governador": ano_governador, "turno_governador": turno_governador, "ano_municipal": ano_municipal, "turno_municipal": turno_municipal, "map_mode": mode}
    data = run_analytics_query(request, "/v1/analytics/polarizacao", filters, lambda: get_service().polarizacao(uf=uf, ano_governador=ano_governador, turno_governador=turno_governador, ano_municipal=ano_municipal, turno_municipal=turno_municipal, map_mode=map_mode), cache_ttl_seconds=MEMORY_CACHE_TTL_BY_ENDPOINT["/v1/analytics/polarizacao"])
    return PolarizacaoResponse(**data)
