from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    code: str
    message: str
    traceId: str
    retryable: bool


class SeriesItem(BaseModel):
    label: str
    value: float
    percentage: float | None = None


class OverviewResponse(BaseModel):
    total_registros: int
    total_candidatos: int | None = None
    total_eleitos: int | None = None
    total_votos_nominais: int | None = None


class TopCandidateItem(BaseModel):
    candidate_id: str = Field(description="Identificador do registro retornado pela consulta, não a identidade histórica da pessoa.")
    source_id: str | None = Field(default=None, description="Origem do registro eleitoral retornado.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    turno_referencia: int | None = Field(default=None, description="Turno usado como referencia para o total de votos retornado.")
    candidato: str
    partido: str | None = None
    cargo: str | None = None
    uf: str | None = None
    votos: int
    situacao: str | None = None


class TopCandidatesResponse(BaseModel):
    top_n: int
    page: int = 1
    page_size: int
    total: int
    total_pages: int
    items: list[TopCandidateItem]


class CandidateSearchItem(BaseModel):
    candidate_id: str = Field(description="Identificador do registro retornado pela busca.")
    source_id: str | None = Field(default=None, description="Origem do registro eleitoral retornado.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    turno_referencia: int | None = Field(default=None, description="Turno usado como referencia para o total de votos retornado.")
    candidato: str
    partido: str | None = None
    cargo: str | None = None
    uf: str | None = None
    numero: str | None = None
    votos: int | None = None
    situacao: str | None = None


class CandidateSearchResponse(BaseModel):
    page: int
    page_size: int
    total: int
    total_pages: int
    items: list[CandidateSearchItem]


class GroupedDistributionResponse(BaseModel):
    group_by: str
    items: list[SeriesItem]


class CorRacaComparativoItem(BaseModel):
    categoria: str
    candidatos: int
    eleitos: int
    percentual_candidatos: float
    percentual_eleitos: float


class CorRacaComparativoResponse(BaseModel):
    items: list[CorRacaComparativoItem] = Field(default_factory=list)


class OccupationGenderItem(BaseModel):
    ocupacao: str
    masculino: int
    feminino: int


class OccupationGenderResponse(BaseModel):
    items: list[OccupationGenderItem] = Field(default_factory=list)


class AgeStatsResponse(BaseModel):
    media: float | None = None
    mediana: float | None = None
    minimo: float | None = None
    maximo: float | None = None
    desvio_padrao: float | None = None
    bins: list[SeriesItem] = Field(default_factory=list)


class FilterOptionsResponse(BaseModel):
    anos: list[int] = Field(default_factory=list)
    ufs: list[str] = Field(default_factory=list)
    cargos: list[str] = Field(default_factory=list)


class TimeSeriesItem(BaseModel):
    ano: int
    value: float


class TimeSeriesResponse(BaseModel):
    metric: str
    items: list[TimeSeriesItem] = Field(default_factory=list)


class RankingItem(BaseModel):
    label: str
    value: float
    percentage: float


class RankingResponse(BaseModel):
    group_by: str
    metric: str
    top_n: int
    items: list[RankingItem] = Field(default_factory=list)


class UFMapItem(BaseModel):
    uf: str
    value: float
    percentage: float


class UFMapResponse(BaseModel):
    metric: str
    items: list[UFMapItem] = Field(default_factory=list)


class OfficialVacancyItem(BaseModel):
    ano: int | None = None
    uf: str | None = None
    municipio: str | None = None
    cargo: str | None = None
    vagas_oficiais: int


class OfficialVacanciesResponse(BaseModel):
    group_by: str
    total_vagas_oficiais: int
    items: list[OfficialVacancyItem] = Field(default_factory=list)


class PolarizacaoFederalItem(BaseModel):
    uf: str
    partido: str
    espectro: str
    votos: int
    status: str | None = None
    eleito: bool
    ano: int | None = None
    turno: int | None = None


class PolarizacaoMunicipalBrasilItem(BaseModel):
    uf: str
    espectro: str
    partido_representativo: str | None = None
    total_prefeitos: int
    ano: int | None = None


class PolarizacaoMunicipalUFItem(BaseModel):
    uf: str
    municipio: str
    partido: str
    espectro: str
    votos: int
    status: str | None = None
    eleito: bool
    ano: int | None = None
    turno: int | None = None


class PolarizacaoResponse(BaseModel):
    federal: list[PolarizacaoFederalItem] = Field(default_factory=list)
    municipal_brasil: list[PolarizacaoMunicipalBrasilItem] = Field(default_factory=list)
    municipal_uf: list[PolarizacaoMunicipalUFItem] = Field(default_factory=list)


class CandidateLatestElection(BaseModel):
    year: int
    turno_referencia: int | None = None
    votes: int
    vote_share: float
    state_rank: int | None = None


class CandidateSummaryResponse(BaseModel):
    candidate_id: str = Field(description="Identificador de consulta usado na rota.")
    source_id: str | None = Field(default=None, description="Origem do registro eleitoral usado para montar a resposta.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    name: str
    number: str | None = None
    party: str | None = None
    coalition: str | None = None
    office: str | None = None
    state: str | None = None
    mandates: int
    status: str | None = None
    turno_referencia: int | None = None
    votos_primeiro_turno: int | None = None
    votos_segundo_turno: int | None = None
    votos_consolidados: int | None = None
    latest_election: CandidateLatestElection


class VoteHistoryItem(BaseModel):
    year: int
    votes: int
    vote_share: float
    status: str | None = None
    office: str | None = None
    state: str | None = None
    municipality: str | None = None
    round: int | None = None
    candidate_number: str | None = None
    party: str | None = None
    source_id: str | None = Field(default=None, description="Origem do registro eleitoral daquele item do histórico.")
    nr_cpf_candidato: str | None = Field(default=None, description="CPF do candidato usado como identidade preferencial do historico quando disponivel.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    is_projection: bool = False


class VoteHistoryResponse(BaseModel):
    candidate_id: str = Field(description="Identificador de consulta usado na rota.")
    nr_cpf_candidato: str | None = Field(default=None, description="CPF do candidato usado como chave preferencial da consulta de historico.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    items: list[VoteHistoryItem] = Field(default_factory=list)


class GenderProfile(BaseModel):
    male_share: float
    female_share: float


class AgeBandsProfile(BaseModel):
    a18_34: float
    a35_59: float
    a60_plus: float


class ElectorateProfileResponse(BaseModel):
    candidate_id: str = Field(description="Identificador de consulta usado na rota.")
    source_id: str | None = Field(default=None, description="Origem do registro eleitoral usado para montar a resposta.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    gender: GenderProfile
    age_bands: AgeBandsProfile
    dominant_education: str
    dominant_income_band: str
    urban_concentration: float


class VoteDistributionItem(BaseModel):
    key: str
    label: str
    votes: int
    vote_share: float
    nm_municipio: str | None = None
    votos_validos_do_municipio: int | None = None
    qt_votos: int | None = None
    percentual_candidato_no_municipio: float | None = None
    percentual_no_total_do_candidato: float | None = None
    categoria_impacto: str | None = None
    impact_category: str | None = None
    candidate_total_share: float | None = None
    zone_id: str | None = None
    zone_name: str | None = None
    municipio: str | None = None
    uf: str | None = None


class VoteDistributionResponse(BaseModel):
    candidate_id: str = Field(description="Identificador de consulta usado na rota.")
    level: str
    total_candidate_votes: int | None = None
    page: int | None = None
    page_size: int | None = None
    total_items: int | None = None
    items: list[VoteDistributionItem] = Field(default_factory=list)
    metadata: dict | None = None


class CandidateVoteMapPoint(BaseModel):
    key: str
    label: str
    votes: int
    vote_share: float
    tier: int | None = None
    color_rgb: str | None = None
    marker_size: int | None = None
    cluster_size: int | None = None
    lat: float | None = None
    lng: float | None = None
    uf: str | None = None
    municipio: str | None = None
    zona: str | None = None
    tooltip: str | None = None


class CompareContext(BaseModel):
    year: int | None = None
    state: str | None = None
    office: str | None = None


class CompareCandidateItem(BaseModel):
    candidate_id: str = Field(description="Identificador do registro retornado pela comparação.")
    source_id: str | None = Field(default=None, description="Origem do registro eleitoral retornado.")
    canonical_candidate_id: str | None = Field(default=None, description="Identidade canônica interna da API. Hoje espelha person_id e nunca candidate_id.")
    person_id: str | None = Field(default=None, description="Identidade estável da pessoa, derivada de nome normalizado e data de nascimento quando disponível.")
    name: str
    party: str | None = None
    votes: int
    vote_share: float
    retention: float
    state_rank: int | None = None


class CompareDeltaItem(BaseModel):
    metric: str
    best_candidate_id: str
    gap_to_second: float


class CompareResponse(BaseModel):
    context: CompareContext
    candidates: list[CompareCandidateItem] = Field(default_factory=list)
    deltas: list[CompareDeltaItem] = Field(default_factory=list)
