"""Eleitos e candidatos contam pessoas, nao linhas por zona, municipio ou turno (#15, #16)."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from app.services.analytics import AnalyticsService
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def _row(year, turno, uf, ue, cargo, sq, name, party, situacao, zona=1, votes=10) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": turno, "SG_UF": uf, "NM_UE": ue, "NR_ZONA": zona,
        "DS_CARGO": cargo, "SQ_CANDIDATO": sq, "NM_CANDIDATO": name, "SG_PARTIDO": party,
        "DS_SIT_TOT_TURNO": situacao, "QT_VOTOS_NOMINAIS_VALIDOS": votes,
    }


def _rows() -> list[dict]:
    rows = []
    # Governador de SP em 2022: 2 turnos x 3 zonas. So A foi eleito (no 2o turno).
    for zona in (1, 2, 3):
        rows.append(_row(2022, 1, "SP", "SAO PAULO", "Governador", 1, "A", "PL", "2º TURNO", zona))
        rows.append(_row(2022, 2, "SP", "SAO PAULO", "Governador", 1, "A", "PL", "ELEITO", zona))
        rows.append(_row(2022, 1, "SP", "SAO PAULO", "Governador", 2, "B", "PT", "2º TURNO", zona))
        rows.append(_row(2022, 2, "SP", "SAO PAULO", "Governador", 2, "B", "PT", "NÃO ELEITO", zona))
    # Dep. Estadual de SP: dois eleitos em 4 zonas cada, e um nao eleito.
    for zona in (1, 2, 3, 4):
        rows.append(_row(2022, 1, "SP", "SAO PAULO", "Deputado Estadual", 10, "C", "PL", "ELEITO POR QP", zona))
        rows.append(_row(2022, 1, "SP", "SAO PAULO", "Deputado Estadual", 11, "D", "PT", "ELEITO POR MÉDIA", zona))
        rows.append(_row(2022, 1, "SP", "SAO PAULO", "Deputado Estadual", 12, "E", "PT", "NÃO ELEITO", zona))
    # Presidente: Lula eleito, com uma linha por municipio de SP (o que inflava o total da UF).
    for mun in ("M1", "M2", "M3", "M4", "M5"):
        rows.append(_row(2022, 2, "SP", mun, "Presidente", 100, "LULA", "PT", "ELEITO"))
        rows.append(_row(2022, 2, "SP", mun, "Presidente", 101, "JAIR", "PL", "NÃO ELEITO"))
    # Vereadores de SP em 2024: 2 eleitos em 3 zonas; mesmo SQ em outro municipio e outra pessoa.
    for zona in (1, 2, 3):
        rows.append(_row(2024, 1, "SP", "SAO PAULO", "Vereador", 500, "V1", "PT", "ELEITO POR QP", zona))
        rows.append(_row(2024, 1, "SP", "SAO PAULO", "Vereador", 501, "V2", "PL", "ELEITO POR MÉDIA", zona))
        rows.append(_row(2024, 1, "SP", "CAMPINAS", "Vereador", 500, "V3", "PL", "ELEITO POR QP", zona))
    return rows


@pytest.fixture(params=["pandas", "duckdb"])
def service(request, tmp_path):
    df = pd.DataFrame(_rows())
    if request.param == "pandas":
        svc = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    else:
        path = tmp_path / "analytics.parquet"
        conn = duckdb.connect()
        try:
            conn.register("f", df)
            conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
        finally:
            conn.close()
        svc = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100)
    yield svc
    if hasattr(svc, "close"):
        svc.close()


def _values(items) -> dict[str, int]:
    return {i["label"]: int(i["value"]) for i in items}


def test_overview_counts_elected_people_not_zone_rows(service):
    # 1 governador + 2 deputados; o presidente e nacional e nao entra no total da UF.
    assert service.overview(ano=2022, uf="SP")["total_eleitos"] == 3
    assert service.overview(ano=2022, uf="SP", cargo="Governador")["total_eleitos"] == 1


def test_overview_includes_president_when_requested_or_nationwide(service):
    assert service.overview(ano=2022, cargo="Presidente")["total_eleitos"] == 1
    assert service.overview(ano=2022)["total_eleitos"] == 4  # + o presidente


def test_overview_municipal_scope_counts_people_per_city(service):
    assert service.overview(ano=2024, uf="SP", municipio="SAO PAULO")["total_eleitos"] == 2
    assert service.overview(ano=2024, uf="SP")["total_eleitos"] == 3  # SQ 500 e duas pessoas


def test_overview_candidates_count_people_too(service):
    # SQ 500 repete em duas cidades: sao duas candidaturas distintas.
    assert service.overview(ano=2024, uf="SP")["total_candidatos"] == 3


def test_ranking_eleitos_by_party_counts_people(service):
    result = service.ranking(group_by="partido", metric="eleitos", ano=2022, cargo="Deputado Estadual")
    assert _values(result) == {"PL": 1, "PT": 1}
    municipal = service.ranking(
        group_by="partido", metric="eleitos", ano=2024, uf="SP", municipio="SAO PAULO", cargo="Vereador"
    )
    assert _values(municipal) == {"PT": 1, "PL": 1}


def test_ranking_eleitos_total_matches_seats_not_rows(service):
    result = service.ranking(group_by="partido", metric="eleitos", ano=2024, cargo="Vereador")
    assert sum(i["value"] for i in result) == 3


def test_ranking_candidatos_counts_distinct_candidacies(service):
    result = service.ranking(group_by="partido", metric="candidatos", ano=2024, cargo="Vereador")
    assert _values(result) == {"PL": 2, "PT": 1}


def test_time_series_eleitos_counts_people(service):
    series = service.time_series(metric="eleitos", cargo="Vereador")
    values = {int(item.get("year") or item.get("ano")): int(item["value"]) for item in series}
    assert values[2024] == 3
