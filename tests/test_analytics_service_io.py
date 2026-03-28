import pandas as pd
import pytest

from app.services.analytics_service import AnalyticsService
from app.services.analytics_service import _impact_category as pandas_impact_category
from app.services.analytics_service import _safe_percent as pandas_safe_percent
from app.services.duckdb_analytics_service import DuckDBAnalyticsService
from app.services.duckdb_analytics_service import _impact_category as duckdb_impact_category
from app.services.duckdb_analytics_service import _safe_percent as duckdb_safe_percent


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


@pytest.mark.parametrize(
    ("numerator", "denominator", "expected"),
    [
        (50, 200, 25.0),
        (1, 3, 33.33),
        (0, 100, 0.0),
        (100, 0, 0.0),
    ],
)
def test_vote_distribution_formula_safe_percent(numerator: float, denominator: float, expected: float):
    assert pandas_safe_percent(numerator, denominator) == expected
    assert duckdb_safe_percent(numerator, denominator) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1.0, "Alto"),
        (5.5, "Alto"),
        (0.3, "Médio"),
        (0.99, "Médio"),
        (0.29, "Baixo"),
        (0.0, "Baixo"),
    ],
)
def test_vote_distribution_formula_impact_category(value: float, expected: str):
    assert pandas_impact_category(value) == expected
    assert duckdb_impact_category(value) == expected


def test_resolve_municipal_scope_parity_between_pandas_and_duckdb(tmp_path):
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 42,
                "NM_CANDIDATO": "Prefeito X",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000,
                "NR_ZONA": "100",
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "CAMPINAS",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 42,
                "NM_CANDIDATO": "Prefeito X",
                "QT_VOTOS_NOMINAIS_VALIDOS": 100,
                "NR_ZONA": "101",
            },
        ]
    )
    csv_path = tmp_path / "municipal_scope.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )

    pandas_scope = pandas_service.resolve_municipal_scope(
        candidate_id="42",
        year=2024,
        office="Prefeito",
        round_filter=1,
        state=None,
        municipality=None,
    )
    duckdb_scope = duckdb_service.resolve_municipal_scope(
        candidate_id="42",
        year=2024,
        office="Prefeito",
        round_filter=1,
        state=None,
        municipality=None,
    )

    assert pandas_scope == duckdb_scope
    assert pandas_scope["used_uf"] == "SP"
    assert pandas_scope["used_municipio"] == "SAO PAULO"
    assert pandas_scope["disambiguation_applied"] is True
