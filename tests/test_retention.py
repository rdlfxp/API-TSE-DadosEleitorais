"""retention do compare: votos / votos da eleicao anterior do MESMO cargo e territorio (#14)."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics import AnalyticsService
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def _row(year, turno, cargo, uf, ue, sq, name, votes, cpf=None) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": turno, "SG_UF": uf, "NM_UE": ue, "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq, "NM_CANDIDATO": name, "NR_CPF_CANDIDATO": cpf, "SG_PARTIDO": "XX",
        "DS_SIT_TOT_TURNO": "ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": votes,
        "HISTORICO_CANDIDATURA_ID": f"tse-history:2026:{cpf}" if cpf else None,
    }


def _retention(history: pd.DataFrame, year, office=None, turno=None):
    service = AnalyticsService(dataframe=history, default_top_n=20, max_top_n=100)
    return service._candidate_retention_from_history(history, year, office=office, turno=turno)


def test_compares_with_the_most_recent_previous_election_of_the_same_office():
    # Antes pegava a mais antiga (2018: 50) e dava 240; o certo e 2022 (100): 120.
    history = pd.DataFrame(
        [
            _row(2018, 1, "Presidente", "SP", "BRASIL", 1, "A", 50),
            _row(2022, 1, "Presidente", "SP", "BRASIL", 2, "A", 100),
            _row(2026, 1, "Presidente", "SP", "BRASIL", 3, "A", 120),
        ]
    )
    assert _retention(history, 2026, "Presidente") == 120.0


def test_other_offices_are_never_compared():
    # Vereador (poucos votos) -> Presidente (milhoes) dava retention de milhoes.
    history = pd.DataFrame(
        [
            _row(2016, 1, "Vereador", "SP", "SÃO PAULO", 1, "A", 10),
            _row(2026, 1, "Presidente", "SP", "BRASIL", 2, "A", 64_000_000),
        ]
    )
    assert _retention(history, 2026, "Presidente") is None


def test_first_candidacy_has_no_retention():
    history = pd.DataFrame([_row(2026, 1, "Presidente", "SP", "BRASIL", 1, "A", 100)])
    assert _retention(history, 2026, "Presidente") is None


def test_state_offices_only_compare_within_the_same_state():
    history = pd.DataFrame(
        [
            _row(2018, 1, "Governador", "MG", "MG", 1, "A", 100),
            _row(2022, 1, "Governador", "SP", "SP", 2, "A", 300),
        ]
    )
    assert _retention(history, 2022, "Governador") is None
    history = pd.concat([history, pd.DataFrame([_row(2018, 1, "Governador", "SP", "SP", 3, "A", 200)])])
    assert _retention(history, 2022, "Governador") == 150.0


def test_municipal_offices_only_compare_within_the_same_municipality():
    history = pd.DataFrame(
        [
            _row(2020, 1, "Prefeito", "SP", "CAMPINAS", 1, "A", 100),
            _row(2024, 1, "Prefeito", "SP", "SANTOS", 2, "A", 400),
        ]
    )
    assert _retention(history, 2024, "Prefeito") is None


def test_round_matches_the_previous_round_when_it_exists():
    history = pd.DataFrame(
        [
            _row(2022, 1, "Presidente", "SP", "BRASIL", 1, "A", 100),
            _row(2022, 2, "Presidente", "SP", "BRASIL", 1, "A", 200),
            _row(2026, 1, "Presidente", "SP", "BRASIL", 2, "A", 150),
            _row(2026, 2, "Presidente", "SP", "BRASIL", 2, "A", 220),
        ]
    )
    assert _retention(history, 2026, "Presidente", turno=1) == 150.0  # 150 / 100
    assert _retention(history, 2026, "Presidente", turno=2) == 110.0  # 220 / 200
    assert _retention(history, 2026, "Presidente") == 110.0  # sem turno: o ultimo


@pytest.fixture
def compare_frame() -> pd.DataFrame:
    # Veterana (CPF 1): 2022 e 2026 para Presidente. Novata (CPF 2): so 2026, antes foi Vereadora.
    return pd.DataFrame(
        [
            _row(2022, 1, "Presidente", "SP", "BRASIL", 10, "VETERANA", 1000, cpf="11111111111"),
            _row(2026, 1, "Presidente", "SP", "BRASIL", 11, "VETERANA", 1100, cpf="11111111111"),
            _row(2016, 1, "Vereador", "SP", "SÃO PAULO", 20, "NOVATA", 5, cpf="22222222222"),
            _row(2026, 1, "Presidente", "SP", "BRASIL", 21, "NOVATA", 900, cpf="22222222222"),
        ]
    )


def test_compare_endpoint_nulls_retention_without_comparable_election(compare_frame):
    with TestClient(main_module.app) as client:
        main_module.service = AnalyticsService(dataframe=compare_frame, default_top_n=20, max_top_n=100)
        payload = client.get(
            "/v1/candidates/compare", params={"candidate_ids": "11,21", "year": 2026, "office": "Presidente", "turno": 1}
        ).json()
    main_module.service = None
    by_name = {c["name"]: c for c in payload["candidates"]}
    assert by_name["VETERANA"]["retention"] == 110.0
    assert by_name["NOVATA"]["retention"] is None  # antes: 900 / 5 = 18000
    # Retencao so entra nos deltas quando ha pelo menos dois candidatos comparaveis.
    assert "retention" not in {d["metric"] for d in payload["deltas"]}
    assert {"votes", "vote_share"} <= {d["metric"] for d in payload["deltas"]}


def test_duckdb_engine_matches_pandas(compare_frame, tmp_path):
    path = tmp_path / "analytics.parquet"
    conn = duckdb.connect()
    try:
        conn.register("f", compare_frame)
        conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()
    duck = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100)
    try:
        result = duck.candidates_compare(["11", "21"], year=2026, office="Presidente", turno=1)
    finally:
        duck.close()
    assert {c["name"]: c["retention"] for c in result["candidates"]} == {"VETERANA": 110.0, "NOVATA": None}
