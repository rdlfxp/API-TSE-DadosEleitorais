from pydantic import BaseModel, Field


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
    candidato: str
    partido: str | None = None
    cargo: str | None = None
    uf: str | None = None
    votos: int
    situacao: str | None = None


class TopCandidatesResponse(BaseModel):
    top_n: int
    items: list[TopCandidateItem]


class GroupedDistributionResponse(BaseModel):
    group_by: str
    items: list[SeriesItem]


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
