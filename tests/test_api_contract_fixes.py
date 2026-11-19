"""Contrato da API: somente_eleitos em top-candidatos (#17), page_size em ranking (#18) e
municipios sem prefeito eleito em polarizacao (#19)."""
from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics_service import AnalyticsService


def _row(year, turno, uf, ue, cargo, sq, name, party, situacao, votes) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": turno, "SG_UF": uf, "NM_UE": ue, "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq, "NM_CANDIDATO": name, "SG_PARTIDO": party, "DS_SIT_TOT_TURNO": situacao,
        "QT_VOTOS_NOMINAIS_VALIDOS": votes,
    }


def _frame() -> pd.DataFrame:
    rows = [
        # Governador de SP 2022: 1o turno de dois candidatos e um 2o turno; so G1 foi eleito.
        _row(2022, 1, "SP", "SAO PAULO", "Governador", 1, "G1", "PL", "2º TURNO", 100),
        _row(2022, 1, "SP", "SAO PAULO", "Governador", 2, "G2", "PT", "2º TURNO", 90),
        _row(2022, 1, "SP", "SAO PAULO", "Governador", 3, "G3", "PSD", "NÃO ELEITO", 10),
        _row(2022, 2, "SP", "SAO PAULO", "Governador", 1, "G1", "PL", "ELEITO", 120),
        _row(2022, 2, "SP", "SAO PAULO", "Governador", 2, "G2", "PT", "NÃO ELEITO", 110),
        # Prefeito 2024 em tres municipios; ANULADA nao tem eleito.
        _row(2024, 1, "SP", "ADAMANTINA", "Prefeito", 10, "P1", "PSD", "ELEITO", 500),
        _row(2024, 1, "SP", "ADAMANTINA", "Prefeito", 11, "P2", "PT", "NÃO ELEITO", 300),
        _row(2024, 1, "SP", "ARARAS", "Prefeito", 12, "P3", "PT", "ELEITO", 400),
        _row(2024, 1, "SP", "ARARAS", "Prefeito", 13, "P4", "PL", "NÃO ELEITO", 200),
        _row(2024, 1, "SP", "ANULADA", "Prefeito", 14, "P5", "PL", "NÃO ELEITO", 250),
        _row(2024, 1, "SP", "ANULADA", "Prefeito", 15, "P6", "PT", "NÃO ELEITO", 240),
    ]
    # 25 partidos distintos em Deputado Federal 2022 para provar o corte em 20 do ranking.
    for i in range(25):
        rows.append(_row(2022, 1, "SP", "SAO PAULO", "Deputado Federal", 100 + i, f"D{i}", f"P{i:02d}", "ELEITO", 1000 - i))
    return pd.DataFrame(rows)


@pytest.fixture
def client():
    with TestClient(main_module.app) as test_client:
        main_module.service = AnalyticsService(dataframe=_frame(), default_top_n=20, max_top_n=100)
        yield test_client
    main_module.service = None


def _names(payload) -> list[str]:
    return [item["candidato"] for item in payload["items"]]


def test_top_candidatos_somente_eleitos_filters_on_the_server(client: TestClient):
    base = {"ano": 2022, "uf": "SP", "cargo": "Governador"}
    everyone = client.get("/v1/analytics/top-candidatos", params=base).json()
    only_elected = client.get("/v1/analytics/top-candidatos", params={**base, "somente_eleitos": "true"}).json()
    assert everyone["total"] == 3
    assert _names(only_elected) == ["G1"]
    assert only_elected["total"] == 1


def test_top_candidatos_somente_eleitos_for_municipal_office(client: TestClient):
    params = {"ano": 2024, "uf": "SP", "municipio": "ADAMANTINA", "cargo": "Prefeito", "somente_eleitos": "true"}
    assert _names(client.get("/v1/analytics/top-candidatos", params=params).json()) == ["P1"]


def test_top_candidatos_somente_eleitos_is_not_mixed_up_in_the_cache(client: TestClient):
    params = {"ano": 2024, "uf": "SP", "cargo": "Prefeito"}
    all_rows = client.get("/v1/analytics/top-candidatos", params=params).json()
    elected = client.get("/v1/analytics/top-candidatos", params={**params, "somente_eleitos": "true"}).json()
    assert all_rows["total"] == 6
    assert sorted(_names(elected)) == ["P1", "P3"]


def test_ranking_page_size_is_a_synonym_of_top_n(client: TestClient):
    params = {"group_by": "partido", "metric": "candidatos", "ano": 2022, "cargo": "Deputado Federal"}
    default = client.get("/v1/analytics/ranking", params=params).json()
    assert len(default["items"]) == 20  # corte padrao, documentado
    wide = client.get("/v1/analytics/ranking", params={**params, "page_size": 100}).json()
    assert len(wide["items"]) == 25 and wide["top_n"] == 100
    same = client.get("/v1/analytics/ranking", params={**params, "top_n": 25, "page_size": 25}).json()
    assert len(same["items"]) == 25


def test_ranking_conflicting_top_n_and_page_size_is_a_400(client: TestClient):
    response = client.get(
        "/v1/analytics/ranking",
        params={"group_by": "partido", "ano": 2022, "cargo": "Deputado Federal", "top_n": 10, "page_size": 30},
    )
    assert response.status_code == 400


def test_polarizacao_municipal_uf_lists_only_elected_mayors(client: TestClient):
    payload = client.get("/v1/analytics/polarizacao", params={"uf": "SP", "ano_municipal": 2024}).json()
    assert sorted(item["municipio"] for item in payload["municipal_uf"]) == ["ADAMANTINA", "ARARAS"]
    assert all(item["eleito"] for item in payload["municipal_uf"])


def test_polarizacao_reports_municipalities_without_an_elected_mayor(client: TestClient):
    payload = client.get("/v1/analytics/polarizacao", params={"uf": "SP", "ano_municipal": 2024}).json()
    assert [(i["uf"], i["municipio"], i["ano"]) for i in payload["municipal_sem_resultado"]] == [("SP", "ANULADA", 2024)]


def test_polarizacao_brasil_totals_ignore_municipalities_without_result(client: TestClient):
    payload = client.get("/v1/analytics/polarizacao", params={"ano_municipal": 2024, "map_mode": "municipalityByMayor"}).json()
    sp = next(item for item in payload["municipal_brasil"] if item["uf"] == "SP")
    # PSD (Adamantina) e PT (Araras) empatam; a cidade anulada (PL) nao conta como prefeito.
    assert sp["total_prefeitos"] == 2
    assert sp["partido_representativo"] in {"PSD", "PT"}  # nunca o PL da cidade sem eleito
