from pathlib import Path

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


def test_duckdb_from_file_supports_file_backed_database(tmp_path, sample_df: pd.DataFrame):
    pytest.importorskip("pyarrow")
    path = tmp_path / "analytics.parquet"
    db_path = tmp_path / "runtime" / "analytics.duckdb"
    sample_df.to_parquet(path, index=False)

    service = DuckDBAnalyticsService.from_file(
        file_path=str(path),
        default_top_n=20,
        max_top_n=100,
        database_path=str(db_path),
    )

    try:
        assert db_path.exists()
        assert service.filter_options()["anos"] == [2022]
    finally:
        service.close()


def test_duckdb_resolves_process_scoped_database_path(tmp_path):
    requested = tmp_path / "runtime" / "analytics.duckdb"
    resolved = DuckDBAnalyticsService._resolve_database_path(str(requested))

    assert resolved != str(requested)
    assert ".pid" in Path(resolved).name
    assert Path(resolved).suffix == ".duckdb"
    assert Path(resolved).parent == requested.parent


def test_filter_options_excludes_non_official_uf_codes(tmp_path):
    df = pd.DataFrame(
        [
            {"ANO_ELEICAO": 2024, "SG_UF": "SP", "DS_CARGO": "Prefeito"},
            {"ANO_ELEICAO": 2022, "SG_UF": "ZZ", "DS_CARGO": "Presidente"},
            {"ANO_ELEICAO": 2010, "SG_UF": "VT", "DS_CARGO": "Presidente"},
        ]
    )
    path = tmp_path / "analytics.csv"
    df.to_csv(path, index=False)

    pandas_service = AnalyticsService.from_file(
        file_path=str(path),
        default_top_n=20,
        max_top_n=100,
    )
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(path),
        default_top_n=20,
        max_top_n=100,
    )

    assert pandas_service.filter_options()["ufs"] == ["SP"]
    assert duckdb_service.filter_options()["ufs"] == ["SP"]


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
    assert "Não informado" not in by_categoria


def _distribution_regression_df() -> pd.DataFrame:
    rows: list[dict] = []

    def add_candidate(
        *,
        ano: int,
        cargo: str,
        candidato: int,
        nome: str,
        situacao: str,
        genero_values: list[object],
        instrucao_values: list[object],
        cor_raca_values: list[object],
        estado_civil_values: list[object],
    ) -> None:
        for idx, genero in enumerate(genero_values):
            rows.append(
                {
                    "ANO_ELEICAO": ano,
                    "NR_TURNO": 1,
                    "SG_UF": "SP",
                    "NM_UE": "SAO PAULO",
                    "DS_CARGO": cargo,
                    "SQ_CANDIDATO": candidato,
                    "NM_CANDIDATO": nome,
                    "DS_SIT_TOT_TURNO": situacao,
                    "DS_GENERO": genero,
                    "DS_GRAU_INSTRUCAO": instrucao_values[idx],
                    "DS_COR_RACA": cor_raca_values[idx],
                    "DS_ESTADO_CIVIL": estado_civil_values[idx],
                }
            )

    add_candidate(
        ano=2020,
        cargo="Prefeito",
        candidato=100,
        nome="Prefeito A",
        situacao="ELEITO",
        genero_values=["M", "", ""],
        instrucao_values=["SUPERIOR COMPLETO", "", ""],
        cor_raca_values=["BRANCA", "", ""],
        estado_civil_values=["CASADO(A)", "", ""],
    )
    add_candidate(
        ano=2020,
        cargo="Prefeito",
        candidato=101,
        nome="Prefeita B",
        situacao="NAO ELEITO",
        genero_values=["FEMININO", ""],
        instrucao_values=["ENSINO MEDIO COMPLETO", ""],
        cor_raca_values=["PARDA", ""],
        estado_civil_values=["SOLTEIRO(A)", ""],
    )
    add_candidate(
        ano=2020,
        cargo="Prefeito",
        candidato=102,
        nome="Prefeito C",
        situacao="NAO ELEITO",
        genero_values=["MASC", "NÃO INFORMADO"],
        instrucao_values=["SUPERIOR COMPLETO", "N/A"],
        cor_raca_values=["PRETA", "NÃO INFORMADO"],
        estado_civil_values=["DIVORCIADO(A)", "N/A"],
    )
    add_candidate(
        ano=2020,
        cargo="Prefeito",
        candidato=103,
        nome="Pessoa D",
        situacao="NAO ELEITO",
        genero_values=[None, ""],
        instrucao_values=[None, ""],
        cor_raca_values=[None, ""],
        estado_civil_values=[None, ""],
    )

    add_candidate(
        ano=2020,
        cargo="Vereador",
        candidato=200,
        nome="Vereadora A",
        situacao="ELEITO",
        genero_values=["F", "", ""],
        instrucao_values=["SUPERIOR COMPLETO", "", ""],
        cor_raca_values=["BRANCA", "", ""],
        estado_civil_values=["CASADO(A)", "", ""],
    )
    add_candidate(
        ano=2020,
        cargo="Vereador",
        candidato=201,
        nome="Vereador B",
        situacao="NAO ELEITO",
        genero_values=["MASCULINO", "", "NA"],
        instrucao_values=["ENSINO MEDIO COMPLETO", "", "N/A"],
        cor_raca_values=["PARDA", "", "N/A"],
        estado_civil_values=["SOLTEIRO(A)", "", "N/A"],
    )
    add_candidate(
        ano=2020,
        cargo="Vereador",
        candidato=202,
        nome="Vereadora C",
        situacao="NAO ELEITO",
        genero_values=["FEM", ""],
        instrucao_values=["SUPERIOR COMPLETO", ""],
        cor_raca_values=["PRETA", ""],
        estado_civil_values=["DIVORCIADO(A)", ""],
    )
    add_candidate(
        ano=2020,
        cargo="Vereador",
        candidato=203,
        nome="Pessoa D",
        situacao="NAO ELEITO",
        genero_values=["", ""],
        instrucao_values=["", ""],
        cor_raca_values=["", ""],
        estado_civil_values=["", ""],
    )
    add_candidate(
        ano=2020,
        cargo="Vereador",
        candidato=204,
        nome="Vereador E",
        situacao="NAO ELEITO",
        genero_values=["M", "IGNORADO"],
        instrucao_values=["ENSINO FUNDAMENTAL COMPLETO", "IGNORADO"],
        cor_raca_values=["AMARELA", "IGNORADO"],
        estado_civil_values=["VIUVO(A)", "IGNORADO"],
    )

    add_candidate(
        ano=2022,
        cargo="Deputado Estadual",
        candidato=300,
        nome="Deputado A",
        situacao="ELEITO",
        genero_values=["M", "", ""],
        instrucao_values=["SUPERIOR COMPLETO", "", ""],
        cor_raca_values=["BRANCA", "", ""],
        estado_civil_values=["CASADO(A)", "", ""],
    )
    add_candidate(
        ano=2022,
        cargo="Deputado Estadual",
        candidato=301,
        nome="Deputada B",
        situacao="ELEITO",
        genero_values=["FEMININO", "", ""],
        instrucao_values=["SUPERIOR COMPLETO", "", ""],
        cor_raca_values=["PARDA", "", ""],
        estado_civil_values=["SOLTEIRO(A)", "", ""],
    )
    add_candidate(
        ano=2022,
        cargo="Deputado Estadual",
        candidato=302,
        nome="Deputado C",
        situacao="ELEITO",
        genero_values=["MASC", "NÃO INFORMADO"],
        instrucao_values=["ENSINO MEDIO COMPLETO", "NÃO INFORMADO"],
        cor_raca_values=["PRETA", "NÃO INFORMADO"],
        estado_civil_values=["DIVORCIADO(A)", "NÃO INFORMADO"],
    )
    add_candidate(
        ano=2022,
        cargo="Deputado Estadual",
        candidato=303,
        nome="Pessoa D",
        situacao="NAO ELEITO",
        genero_values=[None, ""],
        instrucao_values=[None, ""],
        cor_raca_values=[None, ""],
        estado_civil_values=[None, ""],
    )
    add_candidate(
        ano=2022,
        cargo="Deputado Estadual",
        candidato=304,
        nome="Deputada E",
        situacao="ELEITO",
        genero_values=["F", "F"],
        instrucao_values=["SUPERIOR COMPLETO", "SUPERIOR COMPLETO"],
        cor_raca_values=["AMARELA", "AMARELA"],
        estado_civil_values=["CASADO(A)", "CASADO(A)"],
    )
    add_candidate(
        ano=2022,
        cargo="Deputado Estadual",
        candidato=305,
        nome="Pessoa F",
        situacao="ELEITO",
        genero_values=["", ""],
        instrucao_values=["", ""],
        cor_raca_values=["", ""],
        estado_civil_values=["", ""],
    )

    return pd.DataFrame(rows)


def _distribution_items_by_label(items: list[dict]) -> dict[str, tuple[float, float]]:
    return {item["label"]: (item["value"], item["percentage"]) for item in items}


@pytest.mark.parametrize(
    ("ano", "cargo", "somente_eleitos", "expected"),
    [
        (
            2020,
            "Prefeito",
            False,
            [
                {"label": "MASCULINO", "value": 2.0, "percentage": 66.67},
                {"label": "FEMININO", "value": 1.0, "percentage": 33.33},
            ],
        ),
        (
            2020,
            "Vereador",
            False,
            [
                {"label": "FEMININO", "value": 2.0, "percentage": 50.0},
                {"label": "MASCULINO", "value": 2.0, "percentage": 50.0},
            ],
        ),
        (
            2022,
            "Deputado Estadual",
            False,
            [
                {"label": "FEMININO", "value": 2.0, "percentage": 50.0},
                {"label": "MASCULINO", "value": 2.0, "percentage": 50.0},
            ],
        ),
        (
            2022,
            "Deputado Estadual",
            True,
            [
                {"label": "FEMININO", "value": 2.0, "percentage": 50.0},
                {"label": "MASCULINO", "value": 2.0, "percentage": 50.0},
            ],
        ),
    ],
)
def test_distribution_genero_resolves_candidate_level_labels_across_recortes(tmp_path, ano, cargo, somente_eleitos, expected):
    df = _distribution_regression_df()
    csv_path = tmp_path / "distribution_regression.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )

    try:
        pandas_payload = pandas_service.distribution(
            group_by="genero",
            ano=ano,
            cargo=cargo,
            somente_eleitos=somente_eleitos,
        )
        duckdb_payload = duckdb_service.distribution(
            group_by="genero",
            ano=ano,
            cargo=cargo,
            somente_eleitos=somente_eleitos,
        )
    finally:
        duckdb_service.close()

    expected_by_label = _distribution_items_by_label(expected)
    assert _distribution_items_by_label(pandas_payload) == expected_by_label
    assert _distribution_items_by_label(duckdb_payload) == expected_by_label


def test_distribution_demographic_fields_prefer_informative_values_over_na(tmp_path):
    df = _distribution_regression_df()
    csv_path = tmp_path / "distribution_demographics.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )

    try:
        pandas_instrucao = pandas_service.distribution(group_by="instrucao", ano=2020, cargo="Prefeito")
        duckdb_instrucao = duckdb_service.distribution(group_by="instrucao", ano=2020, cargo="Prefeito")
        pandas_cor_raca = pandas_service.distribution(group_by="cor_raca", ano=2020, cargo="Prefeito")
        duckdb_cor_raca = duckdb_service.distribution(group_by="cor_raca", ano=2020, cargo="Prefeito")
        pandas_estado_civil = pandas_service.distribution(group_by="estado_civil", ano=2020, cargo="Prefeito")
        duckdb_estado_civil = duckdb_service.distribution(group_by="estado_civil", ano=2020, cargo="Prefeito")
    finally:
        duckdb_service.close()

    expected_instrucao = {
        "SUPERIOR COMPLETO": 2.0,
        "ENSINO MEDIO COMPLETO": 1.0,
    }
    expected_cor_raca = {
        "BRANCA": 1.0,
        "PARDA": 1.0,
        "PRETA": 1.0,
    }
    expected_estado_civil = {
        "CASADO(A)": 1.0,
        "SOLTEIRO(A)": 1.0,
        "DIVORCIADO(A)": 1.0,
    }

    assert {item["label"]: item["value"] for item in pandas_instrucao} == expected_instrucao
    assert {item["label"]: item["value"] for item in duckdb_instrucao} == expected_instrucao
    assert {item["label"]: item["value"] for item in pandas_cor_raca} == expected_cor_raca
    assert {item["label"]: item["value"] for item in duckdb_cor_raca} == expected_cor_raca
    assert {item["label"]: item["value"] for item in pandas_estado_civil} == expected_estado_civil
    assert {item["label"]: item["value"] for item in duckdb_estado_civil} == expected_estado_civil


def test_occupation_gender_omits_na_and_rows_without_known_gender(tmp_path):
    df = pd.DataFrame(
        [
            {"DS_OCUPACAO": "ADVOGADO", "DS_GENERO": "M"},
            {"DS_OCUPACAO": "N/A", "DS_GENERO": "F"},
            {"DS_OCUPACAO": "", "DS_GENERO": "M"},
            {"DS_OCUPACAO": "PROFESSOR", "DS_GENERO": "IGNORADO"},
        ]
    )
    csv_path = tmp_path / "occupation_gender_hide_na.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )

    try:
        pandas_payload = pandas_service.occupation_gender_distribution()
        duckdb_payload = duckdb_service.occupation_gender_distribution()
    finally:
        duckdb_service.close()

    expected = [{"ocupacao": "ADVOGADO", "masculino": 1, "feminino": 0}]
    assert pandas_payload == expected
    assert duckdb_payload == expected


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


def test_search_candidates_orders_by_votes_desc_even_with_relevance_score(tmp_path):
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": "10",
                "NM_CANDIDATO": "Ana",
                "NR_CANDIDATO": "10",
                "SG_PARTIDO": "AAA",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 100,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": "11",
                "NM_CANDIDATO": "Ana Maria",
                "NR_CANDIDATO": "11",
                "SG_PARTIDO": "BBB",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000,
            },
        ]
    )
    csv_path = tmp_path / "search_votes_priority.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )

    pandas_payload = pandas_service.search_candidates(q="ana", ano=2024, uf="SP", cargo="Prefeito", page=1, page_size=10)
    duckdb_payload = duckdb_service.search_candidates(q="ana", ano=2024, uf="SP", cargo="Prefeito", page=1, page_size=10)

    assert [item["candidate_id"] for item in pandas_payload["items"]] == ["11", "10"]
    assert [item["candidate_id"] for item in duckdb_payload["items"]] == ["11", "10"]


def test_presidente_nacional_analytics_are_consistent_between_pandas_and_duckdb(tmp_path):
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 30,
                "NM_CANDIDATO": "Presidente X",
                "SG_PARTIDO": "PXX",
                "QT_VOTOS_NOMINAIS_VALIDOS": 110000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "RJ",
                "NM_UE": "RIO DE JANEIRO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 30,
                "NM_CANDIDATO": "Presidente X",
                "SG_PARTIDO": "PXX",
                "QT_VOTOS_NOMINAIS_VALIDOS": 90000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 31,
                "NM_CANDIDATO": "Presidente Y",
                "SG_PARTIDO": "PYY",
                "QT_VOTOS_NOMINAIS_VALIDOS": 50000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "RJ",
                "NM_UE": "RIO DE JANEIRO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 31,
                "NM_CANDIDATO": "Presidente Y",
                "SG_PARTIDO": "PYY",
                "QT_VOTOS_NOMINAIS_VALIDOS": 40000,
            },
        ]
    )
    csv_path = tmp_path / "presidente_nacional.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )

    overview_kwargs = {"ano": 2022, "turno": 2, "cargo": "Presidente", "uf": None, "municipio": None}
    distribution_kwargs = {
        "group_by": "status",
        "ano": 2022,
        "turno": 2,
        "cargo": "Presidente",
        "uf": None,
        "municipio": None,
    }
    assert pandas_service.overview(**overview_kwargs) == duckdb_service.overview(**overview_kwargs)
    assert pandas_service.distribution(**distribution_kwargs) == duckdb_service.distribution(**distribution_kwargs)
    assert pandas_service.top_candidates(ano=2022, turno=2, cargo="Presidente", page=1, page_size=50) == duckdb_service.top_candidates(
        ano=2022,
        turno=2,
        cargo="Presidente",
        page=1,
        page_size=50,
    )


def test_top_candidates_without_turno_uses_latest_round_consistently_between_pandas_and_duckdb(tmp_path):
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 4,
                "NM_CANDIDATO": "Prefeito A",
                "SG_PARTIDO": "AAA",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 500000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 4,
                "NM_CANDIDATO": "Prefeito A",
                "SG_PARTIDO": "AAA",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 550000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 7,
                "NM_CANDIDATO": "Prefeito B",
                "SG_PARTIDO": "BBB",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 200000,
            },
        ]
    )
    csv_path = tmp_path / "top_candidates_latest_round.csv"
    df.to_csv(csv_path, index=False)

    pandas_service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    duckdb_service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
    )
    try:
        pandas_payload = pandas_service.top_candidates(ano=2024, cargo="Prefeito", page=1, page_size=10)
        duckdb_payload = duckdb_service.top_candidates(ano=2024, cargo="Prefeito", page=1, page_size=10)
    finally:
        duckdb_service.close()

    assert pandas_payload == duckdb_payload
    assert pandas_payload["items"][0]["votos"] == 550000
    assert pandas_payload["items"][0]["latest_vote_round"] == 2
