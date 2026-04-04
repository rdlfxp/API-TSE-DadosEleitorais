from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from app.api.dependencies import (
    get_service,
    resolve_municipality_param,
    resolve_state_param,
    resolve_year_param,
    run_analytics_query,
    run_municipal_vote_flow,
    trace_id_from_request,
)
from app.api.errors import ERROR_RESPONSES
from app.domain import is_municipal_office
from app.schemas import CandidateSummaryResponse, CompareResponse, ElectorateProfileResponse, VoteDistributionResponse, VoteHistoryResponse


router = APIRouter()


@router.get("/v1/candidates/{candidate_id}/summary", response_model=CandidateSummaryResponse, responses=ERROR_RESPONSES)
def candidate_summary(candidate_id: str, ano: int | None = Query(default=None), year: int | None = None, turno: int | None = Query(default=None, ge=1, le=2), uf: str | None = Query(default=None, min_length=2, max_length=2), state: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, office: str | None = None) -> CandidateSummaryResponse:
    resolved_year = resolve_year_param(ano=ano, year=year)
    resolved_state = resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    return CandidateSummaryResponse(**get_service().candidate_summary(candidate_id=candidate_id, year=resolved_year, turno=turno, state=resolved_state, office=resolved_office))


@router.get("/v1/candidates/{candidate_id}/vote-history", response_model=VoteHistoryResponse, responses=ERROR_RESPONSES)
def candidate_vote_history(request: Request, candidate_id: str, nr_cpf_candidato: str | None = Query(default=None, description="CPF do candidato. Quando informado, e a chave preferencial para montar o historico eleitoral."), uf: str | None = Query(default=None, min_length=2, max_length=2), state: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, office: str | None = None) -> VoteHistoryResponse:
    resolved_state = resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    filters = {"candidate_id": candidate_id, "nr_cpf_candidato": nr_cpf_candidato, "state": resolved_state, "office": resolved_office}
    data = run_analytics_query(request, f"/v1/candidates/{candidate_id}/vote-history", filters, lambda: get_service().candidate_vote_history(candidate_id=candidate_id, candidate_cpf=nr_cpf_candidato, state=resolved_state, office=resolved_office), cache_ttl_seconds=1800)
    return VoteHistoryResponse(**data)


@router.get("/v1/candidates/{candidate_id}/electorate-profile", response_model=ElectorateProfileResponse, responses=ERROR_RESPONSES)
def candidate_electorate_profile(candidate_id: str, ano: int | None = Query(default=None), year: int | None = None, uf: str | None = Query(default=None, min_length=2, max_length=2), state: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, office: str | None = None, municipio: str | None = None, municipality: str | None = None) -> ElectorateProfileResponse:
    resolved_year = resolve_year_param(ano=ano, year=year)
    resolved_state = resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    resolved_municipality = resolve_municipality_param(municipality=municipality, municipio=municipio)
    return ElectorateProfileResponse(**get_service().candidate_electorate_profile(candidate_id=candidate_id, year=resolved_year, state=resolved_state, office=resolved_office, municipality=resolved_municipality))


@router.get("/v1/candidates/{candidate_id}/vote-distribution", response_model=VoteDistributionResponse, responses=ERROR_RESPONSES)
def candidate_vote_distribution(request: Request, candidate_id: str, level: Literal["macroregiao", "uf", "municipio", "zona"], year: int | None = None, ano: int | None = None, state: str | None = Query(default=None, min_length=2, max_length=2), uf: str | None = Query(default=None, min_length=2, max_length=2), office: str | None = None, cargo: str | None = None, round_: int | None = Query(default=None, ge=1, le=2, alias="round"), turno: int | None = Query(default=None, ge=1, le=2), municipality: str | None = None, municipio: str | None = None, sort_by: str | None = None, sort_order: Literal["asc", "desc"] | None = None, page: int | None = Query(default=None, ge=1), page_size: int | None = Query(default=None, ge=1, le=500)) -> VoteDistributionResponse:
    resolved_year = year if year is not None else ano
    resolved_state = resolve_state_param(state=state, uf=uf)
    resolved_municipality = resolve_municipality_param(municipality=municipality, municipio=municipio)
    resolved_office = office or cargo
    resolved_round = round_ if round_ is not None else turno
    trace_id = trace_id_from_request(request)
    is_municipal_request = is_municipal_office(resolved_office)
    if is_municipal_request and resolved_year is None:
        raise HTTPException(status_code=400, detail="Parametro obrigatorio ausente para recorte municipal: ano.")

    service = get_service()
    data, scope_info, used_state, used_municipality, fallback_applied, used_round, infer_scope = run_municipal_vote_flow(
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
        operation=lambda used_state_arg, used_municipality_arg, used_round_arg: service.candidate_vote_distribution(candidate_id=candidate_id, level=level, year=resolved_year, state=used_state_arg, office=resolved_office, round_filter=used_round_arg, municipality=used_municipality_arg, sort_by=sort_by, sort_order=sort_order, page=page, page_size=page_size, trace_id=trace_id),
    )
    if is_municipal_request:
        data["metadata"] = {"scope": "municipal", "inferred_geo": bool(scope_info and scope_info.get("inferred_geo")) if infer_scope else False, "inferred_uf": scope_info.get("inferred_uf") if scope_info else None, "inferred_municipio": scope_info.get("inferred_municipio") if scope_info else None, "requested_uf": resolved_state, "requested_municipio": resolved_municipality, "used_uf": used_state, "used_municipio": used_municipality, "requested_turno": resolved_round, "used_turno": used_round, "fallback_applied": fallback_applied, "no_data": len(data.get("items", [])) == 0, "disambiguation_applied": bool(scope_info and scope_info.get("disambiguation_applied")) if infer_scope else False, "uf": used_state, "municipio": used_municipality, "cargo": resolved_office, "ano": resolved_year, "traceId": trace_id}
    return VoteDistributionResponse(**data)


@router.get("/v1/candidates/compare", response_model=CompareResponse, responses=ERROR_RESPONSES)
def candidates_compare(candidate_ids: list[str] = Query(..., description="IDs de candidatos (CSV: 1,2 ou repetido: ?candidate_ids=1&candidate_ids=2)", openapi_extra={"style": "form", "explode": False}), ano: int | None = Query(default=None), year: int | None = None, uf: str | None = Query(default=None, min_length=2, max_length=2), state: str | None = Query(default=None, min_length=2, max_length=2), cargo: str | None = None, office: str | None = None) -> CompareResponse:
    parsed_ids: list[str] = []
    for value in candidate_ids:
        parsed_ids.extend([token.strip() for token in str(value).split(",") if token.strip()])
    if len(parsed_ids) < 2 or len(parsed_ids) > 4:
        raise HTTPException(status_code=400, detail="candidate_ids deve conter de 2 a 4 IDs.")
    resolved_year = resolve_year_param(ano=ano, year=year)
    resolved_state = resolve_state_param(state=state, uf=uf)
    resolved_office = office or cargo
    return CompareResponse(**get_service().candidates_compare(candidate_ids=parsed_ids, year=resolved_year, state=resolved_state, office=resolved_office))
