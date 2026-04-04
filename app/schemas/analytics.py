from pydantic import BaseModel, Field

from app.schemas.common import SeriesItem


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
    turno_referencia: int | None = Field(default=None, description="Turno usado como referencia para o total de votos retornado. Quando turno nao for informado, representa a ultima votacao valida do pleito.")
    latest_vote_round: int | None = Field(default=None, description="Alias explicito do turno efetivamente usado para calcular votos.")
    latest_vote_value: int | None = Field(default=None, description="Valor de votos usado como base do campo votos.")
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
