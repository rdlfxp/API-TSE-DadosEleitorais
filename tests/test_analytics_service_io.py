import pandas as pd
import pytest

from app.services.analytics_service import AnalyticsService
from app.services.duckdb_analytics_service import DuckDBAnalyticsService


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2022,
                "SG_UF": "SP",
                "DS_CARGO": "Deputado Estadual",
                "NM_CANDIDATO": "Candidato A",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000,
            }
        ]
    )


def test_from_file_loads_csv(tmp_path, sample_df: pd.DataFrame):
    path = tmp_path / "analytics.csv"
    sample_df.to_csv(path, index=False)

    service = AnalyticsService.from_file(
        file_path=str(path),
        default_top_n=20,
        max_top_n=100,
    )

    assert len(service.dataframe) == 1
    assert service.filter_options()["anos"] == [2022]


def test_from_file_loads_parquet(tmp_path, sample_df: pd.DataFrame):
    pytest.importorskip("pyarrow")
    path = tmp_path / "analytics.parquet"
    sample_df.to_parquet(path, index=False)

    service = AnalyticsService.from_file(
        file_path=str(path),
        default_top_n=20,
        max_top_n=100,
    )

    assert len(service.dataframe) == 1
    assert service.filter_options()["ufs"] == ["SP"]


def test_from_file_rejects_unsupported_extension(tmp_path):
    path = tmp_path / "analytics.txt"
    path.write_text("invalid", encoding="utf-8")

    with pytest.raises(ValueError, match="Formato de analytics nao suportado"):
        AnalyticsService.from_file(
            file_path=str(path),
            default_top_n=20,
            max_top_n=100,
        )


def test_duckdb_cor_raca_comparativo_normaliza_nao_divulgavel(tmp_path):
    df = pd.DataFrame(
        [
            {"DS_COR_RACA": "BRANCA", "DS_SIT_TOT_TURNO": "ELEITO"},
            {"DS_COR_RACA": "NÃO DIVULGÁVEL", "DS_SIT_TOT_TURNO": "NAO ELEITO"},
            {"DS_COR_RACA": "PRETA", "DS_SIT_TOT_TURNO": "ELEITO"},
        ]
    )
    path = tmp_path / "analytics.csv"
    df.to_csv(path, index=False)

    service = DuckDBAnalyticsService.from_file(
        file_path=str(path),
        default_top_n=20,
        max_top_n=100,
    )
    payload = service.cor_raca_comparativo()
    by_categoria = {item["categoria"]: item for item in payload["items"]}
    assert by_categoria["Branca"]["candidatos"] == 1
    assert by_categoria["Preta"]["candidatos"] == 1
    assert by_categoria["Não informado"]["candidatos"] == 1
