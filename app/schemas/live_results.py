from pydantic import BaseModel, ConfigDict, Field


class LiveCandidate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    numero: str | None = None
    nome_urna: str | None = None
    partido: str | None = None
    coligacao: str | None = None
    votos: int = Field(ge=0)
    percentual: float | None = Field(default=None, ge=0, le=100)
    situacao: str | None = None


class LiveResultsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    atualizado_em: str | None = None
    fonte: str = "TSE"
    situacao: str
    secoes_totalizadas_pct: float | None = Field(default=None, ge=0, le=100)
    eleitorado: int | None = Field(default=None, ge=0)
    comparecimento: int | None = Field(default=None, ge=0)
    abstencoes: int | None = Field(default=None, ge=0)
    votos_validos: int | None = Field(default=None, ge=0)
    votos_brancos: int | None = Field(default=None, ge=0)
    votos_nulos: int | None = Field(default=None, ge=0)
    page: int = Field(ge=1)
    total_pages: int = Field(ge=0)
    total: int = Field(ge=0)
    candidatos: list[LiveCandidate]
