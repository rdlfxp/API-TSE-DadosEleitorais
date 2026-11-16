"""Itens de contrato (etapa D): resumo do Presidente, turno no compare, bancadas,
foto_url e vote-history com CPF. Rodam nos dois motores (pandas e DuckDB)."""
from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.infra import cache as cache_module
from app.services.analytics.duckdb_service import DuckDBAnalyticsService
from app.services.analytics_service import AnalyticsService


def _rows() -> list[dict]:
    base = {"ANO_ELEICAO": 2026, "DS_CARGO": "Presidente", "DS_GENERO": "MASCULINO", "IDADE": 60}
    rows = []
    # Presidente no grao de UF: 2 UFs x 2 turnos; A vence o 2o turno, C so disputa o 1o.
    for uf, (a1, b1, c1, a2, b2) in {"AC": (10, 30, 5, 20, 25), "SP": (100, 90, 40, 160, 120)}.items():
        rows += [
            {**base, "NR_TURNO": 1, "SG_UF": uf, "SQ_CANDIDATO": 1, "NM_CANDIDATO": "CANDIDATO A", "SG_PARTIDO": "AAA",
             "DS_SIT_TOT_TURNO": "2º TURNO", "QT_VOTOS_NOMINAIS_VALIDOS": a1, "NR_CPF_CANDIDATO": "11111111111",
             "HISTORICO_CANDIDATURA_ID": "tse-history:2026:1"},
            {**base, "NR_TURNO": 1, "SG_UF": uf, "SQ_CANDIDATO": 2, "NM_CANDIDATO": "CANDIDATO B", "SG_PARTIDO": "BBB",
             "DS_SIT_TOT_TURNO": "2º TURNO", "QT_VOTOS_NOMINAIS_VALIDOS": b1},
            {**base, "NR_TURNO": 1, "SG_UF": uf, "SQ_CANDIDATO": 3, "NM_CANDIDATO": "CANDIDATO C", "SG_PARTIDO": "CCC",
             "DS_SIT_TOT_TURNO": "NÃO ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": c1},
            {**base, "NR_TURNO": 2, "SG_UF": uf, "SQ_CANDIDATO": 1, "NM_CANDIDATO": "CANDIDATO A", "SG_PARTIDO": "AAA",
             "DS_SIT_TOT_TURNO": "ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": a2, "NR_CPF_CANDIDATO": "11111111111",
             "HISTORICO_CANDIDATURA_ID": "tse-history:2026:1"},
            {**base, "NR_TURNO": 2, "SG_UF": uf, "SQ_CANDIDATO": 2, "NM_CANDIDATO": "CANDIDATO B", "SG_PARTIDO": "BBB",
             "DS_SIT_TOT_TURNO": "NÃO ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": b2},
        ]
    # O mesmo A em 2022 (sem CPF), ligado pelo historico oficial do TSE.
    rows.append({**base, "ANO_ELEICAO": 2022, "NR_TURNO": 1, "SG_UF": "SP", "SQ_CANDIDATO": 99, "NM_CANDIDATO": "CANDIDATO A",
                 "SG_PARTIDO": "AAA", "DS_SIT_TOT_TURNO": "NÃO ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": 50,
                 "HISTORICO_CANDIDATURA_ID": "tse-history:2026:1"})
    # Senado: 3 eleitos de 2 partidos.
    for sq, partido, sit in ((11, "AAA", "ELEITO"), (12, "AAA", "ELEITO"), (13, "BBB", "ELEITO"), (14, "BBB", "NÃO ELEITO")):
        rows.append({**base, "DS_CARGO": "Senador", "NR_TURNO": 1, "SG_UF": "SP", "SQ_CANDIDATO": sq,
                     "NM_CANDIDATO": f"SENADOR {sq}", "SG_PARTIDO": partido, "DS_SIT_TOT_TURNO": sit, "QT_VOTOS_NOMINAIS_VALIDOS": 100 + sq})
    return rows


@pytest.fixture(params=["pandas", "duckdb"])
def client(request, tmp_path):
    cache_module.ANALYTICS_CACHE.clear()
    df = pd.DataFrame(_rows())
    if request.param == "pandas":
        service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    else:
        path = tmp_path / "analytics.parquet"
        import duckdb

        conn = duckdb.connect()
        conn.register("frame", df)
        conn.execute(f"COPY frame TO '{path}' (FORMAT PARQUET)")
        conn.close()
        service = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100,
                                                   database_path=str(tmp_path / "db.duckdb"))
    with TestClient(main_module.app) as test_client:
        main_module.service = service
        yield test_client
    main_module.service = None
    cache_module.ANALYTICS_CACHE.clear()


def test_presidential_summary_is_national_not_first_state(client):
    body = client.get("/v1/candidates/1/summary", params={"year": 2026, "turno": 2}).json()
    latest = body["latest_election"]
    assert body["state"] is None
    assert latest["votes"] == 180
    assert latest["vote_share"] == pytest.approx(180 / 325 * 100, rel=1e-4)  # nunca > 100%
    assert latest["state_rank"] == 1
    sp = client.get("/v1/candidates/1/summary", params={"year": 2026, "turno": 2, "uf": "SP"}).json()
    assert sp["state"] == "SP" and sp["latest_election"]["votes"] == 160


def test_compare_uses_requested_turn_or_last_common_turn(client):
    ids = {"candidate_ids": "1,2", "year": 2026, "office": "Presidente"}
    no_turn = client.get("/v1/candidates/compare", params=ids).json()
    assert no_turn["context"]["turno"] == 2
    assert {c["candidate_id"]: c["votes"] for c in no_turn["candidates"]} == {"1": 180, "2": 145}
    first = client.get("/v1/candidates/compare", params={**ids, "turno": 1}).json()
    assert {c["candidate_id"]: c["votes"] for c in first["candidates"]} == {"1": 110, "2": 120}
    alias = client.get("/v1/candidates/compare", params={**ids, "round": 1}).json()
    assert alias["context"]["turno"] == 1
    # C so disputou o 1o turno: comparacao com A cai no 1o turno, nunca soma turnos.
    mixed = client.get("/v1/candidates/compare", params={"candidate_ids": "1,3", "year": 2026, "office": "Presidente"}).json()
    assert mixed["context"]["turno"] == 1
    assert {c["candidate_id"]: c["votes"] for c in mixed["candidates"]} == {"1": 110, "3": 45}


def test_distribution_by_party_counts_elected_seats(client):
    body = client.get("/v1/analytics/distribuicao", params={"group_by": "partido", "ano": 2026, "cargo": "Senador", "somente_eleitos": "true"}).json()
    assert {i["label"]: i["value"] for i in body["items"]} == {"AAA": 2.0, "BBB": 1.0}
    pres = client.get("/v1/analytics/distribuicao", params={"group_by": "partido", "ano": 2026, "cargo": "Presidente", "turno": 2, "somente_eleitos": "true"}).json()
    assert {i["label"]: i["value"] for i in pres["items"]} == {"AAA": 1.0}


def test_summary_exposes_optional_https_photo(client):
    pres = client.get("/v1/candidates/1/summary", params={"year": 2026, "turno": 2}).json()
    assert pres["foto_url"] == "https://resultados.tse.jus.br/oficial/ele2026/6257/fotos/br/1.jpeg"
    senador = client.get("/v1/candidates/11/summary", params={"year": 2026}).json()
    assert senador["foto_url"] == "https://resultados.tse.jus.br/oficial/ele2026/6259/fotos/sp/11.jpeg"
    old = client.get("/v1/candidates/99/summary", params={"year": 2022}).json()
    assert old["foto_url"] is None


def test_vote_history_with_cpf_keeps_person_id_and_full_history(client):
    plain = client.get("/v1/candidates/1/vote-history").json()
    with_cpf = client.get("/v1/candidates/1/vote-history", params={"nr_cpf_candidato": "111.111.111-11"}).json()
    summary = client.get("/v1/candidates/1/summary", params={"year": 2026}).json()
    assert with_cpf["person_id"] == plain["person_id"] == summary["person_id"]
    assert sorted({i["year"] for i in with_cpf["items"]}) == [2022, 2026]
    assert with_cpf["nr_cpf_candidato"] == "11111111111"
