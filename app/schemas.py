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
    candidate_id: str
    source_id: str | None = None
    canonical_candidate_id: str | None = None
    person_id: str | None = None
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
    candidate_id: str
    source_id: str | None = None
    canonical_candidate_id: str | None = None
    person_id: str | None = None
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
    votes: int
    vote_share: float
    state_rank: int | None = None


class CandidateSummaryResponse(BaseModel):
    candidate_id: str
    source_id: str | None = None
    canonical_candidate_id: str | None = None
    person_id: str | None = None
    name: str
    number: str | None = None
    party: str | None = None
    coalition: str | None = None
    office: str | None = None
    state: str | None = None
    mandates: int
    status: str | None = None
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
    source_id: str | None = None
    canonical_candidate_id: str | None = None
    person_id: str | None = None
    is_projection: bool = False


class VoteHistoryResponse(BaseModel):
    candidate_id: str
    canonical_candidate_id: str | None = None
    person_id: str | None = None
    items: list[VoteHistoryItem] = Field(default_factory=list)


class GenderProfile(BaseModel):
    male_share: float
    female_share: float


class AgeBandsProfile(BaseModel):
    a18_34: float
    a35_59: float
    a60_plus: float


class ElectorateProfileResponse(BaseModel):
    candidate_id: str
    source_id: str | None = None
    canonical_candidate_id: str | None = None
    person_id: str | None = None
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
    candidate_id: str
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


class CandidateVoteMapRegionCluster(BaseModel):
    region_type: str
    region_key: str
    region_label: str
    votes: int
    points: int
    lat: float
    lng: float


class CandidateVoteMapResponse(BaseModel):
    candidate_id: str
    level: str
    quantile_q1: int | None
    quantile_q2: int | None
    total_votes: int
    items: list[CandidateVoteMapPoint] = Field(default_factory=list)
    region_clusters: list[CandidateVoteMapRegionCluster] = Field(default_factory=list)
    metadata: dict | None = None


class ZoneFidelityItem(BaseModel):
    zone_id: str
    zone_name: str
    city: str | None = None
    votes: int
    retention: float
    lat: float
    lng: float
    geometry: object | None = None


class ZoneFidelityResponse(BaseModel):
    candidate_id: str
    items: list[ZoneFidelityItem] = Field(default_factory=list)


class CompareContext(BaseModel):
    year: int | None = None
    state: str | None = None
    office: str | None = None


class CompareCandidateItem(BaseModel):
    candidate_id: str
    source_id: str | None = None
    canonical_candidate_id: str | None = None
    person_id: str | None = None
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
