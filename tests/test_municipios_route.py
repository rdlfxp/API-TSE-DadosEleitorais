"""GET /v1/analytics/municipios: municipios por UF com codigos e 2o turno (#24)."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics import AnalyticsService
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def _row(year, turno, uf, ue, cd, cargo, sq) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": turno, "SG_UF": uf, "NM_UE": ue, "CD_MUNICIPIO": cd, "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq, "NM_CANDIDATO": f"C{sq}", "SG_PARTIDO": "XX", "DS_SIT_TOT_TURNO": "ELEITO",
        "QT_VOTOS_NOMINAIS_VALIDOS": 10,
    }


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _row(2024, 1, "SP", "SÃO PAULO", 71072, "Prefeito", 1),
            _row(2024, 2, "SP", "SÃO PAULO", 71072, "Prefeito", 1),
            _row(2024, 1, "SP", "CAMPINAS", 62910, "Prefeito", 2),
            _row(2022, 1, "RJ", "BRASIL", 60011, "Governador", 3),
        ]
    )


@pytest.fixture
def client():
    with TestClient(main_module.app) as test_client:
        main_module.service = AnalyticsService(dataframe=_frame(), default_top_n=20, max_top_n=100)
        yield test_client
    main_module.service = None


def test_lists_every_municipality_of_the_uf_with_codes(client: TestClient):
    payload = client.get("/v1/analytics/municipios", params={"uf": "sp", "ano": 2024}).json()
    assert payload["uf"] == "SP" and payload["ano"] == 2024
    assert payload["total"] == len(payload["items"]) == 645
    sao_paulo = next(i for i in payload["items"] if i["nome"] == "SÃO PAULO")
    assert (sao_paulo["cd_municipio"], sao_paulo["cd_municipio_ibge"]) == ("71072", "3550308")


def test_items_are_alphabetical_without_accents(client: TestClient):
    names = [i["nome"] for i in client.get("/v1/analytics/municipios", params={"uf": "SP"}).json()["items"]]
    assert names[0] == "ADAMANTINA" and names.index("SÃO PAULO") > names.index("SANTOS")


def test_second_round_flag_comes_from_the_election_data(client: TestClient):
    items = {i["nome"]: i for i in client.get("/v1/analytics/municipios", params={"uf": "SP", "ano": 2024}).json()["items"]}
    assert items["SÃO PAULO"]["segundo_turno"] is True
    assert items["CAMPINAS"]["segundo_turno"] is False
    assert items["ADAMANTINA"]["segundo_turno"] is False  # ha eleicao municipal no ano e ela fechou no 1o turno


def test_default_year_is_the_latest_municipal_year(client: TestClient):
    assert client.get("/v1/analytics/municipios", params={"uf": "SP"}).json()["ano"] == 2024


def test_year_without_municipal_election_has_unknown_second_round(client: TestClient):
    payload = client.get("/v1/analytics/municipios", params={"uf": "SP", "ano": 2022}).json()
    assert payload["total"] == 645
    assert all(item["segundo_turno"] is None for item in payload["items"])


def test_unknown_uf_returns_empty_list_and_missing_uf_is_422(client: TestClient):
    assert client.get("/v1/analytics/municipios", params={"uf": "XX"}).json()["total"] == 0
    assert client.get("/v1/analytics/municipios").status_code == 422


def test_duckdb_engine_matches_pandas(tmp_path):
    path = tmp_path / "analytics.parquet"
    conn = duckdb.connect()
    try:
        conn.register("f", _frame())
        conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()
    duck = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100)
    pand = AnalyticsService(dataframe=_frame(), default_top_n=20, max_top_n=100)
    try:
        for uf, ano in (("SP", 2024), ("SP", None), ("SP", 2022), ("RJ", None), ("XX", None)):
            assert duck.mayor_second_round(uf, ano) == pand.mayor_second_round(uf, ano), (uf, ano)
    finally:
        duck.close()
