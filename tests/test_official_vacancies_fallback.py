"""vagas-oficiais sem QT_VAGAS (anos sem consulta_vagas) e o rotulo antigo "MÉDIA" (#1, #21)."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from app.services.analytics import AnalyticsService
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def _row(year, uf, ue, cargo, sq, situacao, vagas=None, zona=1) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": 1, "SG_UF": uf, "NM_UE": ue, "NR_ZONA": zona, "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq, "NM_CANDIDATO": f"C{sq}", "SG_PARTIDO": "XX", "DS_SIT_TOT_TURNO": situacao,
        "QT_VOTOS_NOMINAIS_VALIDOS": 10, "QT_VAGAS": vagas,
    }


def _rows() -> list[dict]:
    rows = []
    # 2022 sem vagas oficiais: 2 deputados estaduais eleitos (2 zonas cada) em SP, 1 em MG.
    for zona in (1, 2):
        rows += [
            _row(2022, "SP", "SP", "Deputado Estadual", 1, "ELEITO POR QP", zona=zona),
            _row(2022, "SP", "SP", "Deputado Estadual", 2, "ELEITO POR MÉDIA", zona=zona),
            _row(2022, "SP", "SP", "Deputado Estadual", 3, "NÃO ELEITO", zona=zona),
            _row(2022, "MG", "MG", "Deputado Estadual", 4, "ELEITO POR QP", zona=zona),
        ]
    # Cargos de vaga unica sem nenhum eleito registrado (status ausente/antigo): 1 cadeira cada.
    rows += [
        _row(2022, "BR", "BRASIL", "Presidente", 10, "2º TURNO"),
        _row(2022, "SP", "SP", "Governador", 11, "2º TURNO"),
        _row(2024, "SP", "TUITI", "Prefeito", 12, "#NULO#"),  # eleicao anulada: sem eleito
        _row(2024, "SP", "TUITI", "Vereador", 13, "ELEITO POR QP"),
        _row(2024, "SP", "TUITI", "Vereador", 14, "SUPLENTE"),
    ]
    # 2010: rotulo antigo do eleito por media e apenas "MÉDIA".
    rows += [
        _row(2010, "SP", "SP", "Deputado Federal", 20, "ELEITO"),
        _row(2010, "SP", "SP", "Deputado Federal", 21, "MÉDIA"),
        _row(2010, "SP", "SP", "Deputado Federal", 22, "SUPLENTE"),
    ]
    # 2026 com vagas oficiais (maiores que os eleitos ate agora): o oficial vence.
    rows += [
        _row(2026, "SP", "SP", "Deputado Estadual", 30, "ELEITO", vagas=94),
        _row(2026, "SP", "SP", "Deputado Estadual", 31, "NÃO ELEITO", vagas=94),
    ]
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


def _by(result, key: str = "cargo") -> dict[str, int]:
    return {i[key]: i["vagas_oficiais"] for i in result["items"]}


def test_year_without_official_vacancies_uses_distinct_elected(service):
    result = service.official_vacancies(group_by="cargo", ano=2022)
    assert _by(result)["Deputado Estadual"] == 3  # 2 em SP + 1 em MG, nao 6 linhas
    assert result["total_vagas_oficiais"] == sum(_by(result).values())


def test_single_seat_offices_count_one_even_without_a_recorded_winner(service):
    by_cargo = _by(service.official_vacancies(group_by="cargo", ano=2022))
    assert by_cargo["Presidente"] == 1
    assert by_cargo["Governador"] == 1
    # Prefeito de eleicao anulada continua sendo 1 cadeira.
    assert _by(service.official_vacancies(group_by="cargo", ano=2024))["Prefeito"] == 1


def test_municipal_seats_are_elected_people_of_the_city(service):
    result = service.official_vacancies(group_by="municipio", ano=2024, uf="SP")
    # 1 vereador eleito + 1 cadeira de prefeito (eleicao anulada) = 2 na cidade.
    assert _by(result, "municipio") == {"TUITI": 2}


def test_official_vacancies_win_when_published(service):
    result = service.official_vacancies(group_by="cargo", ano=2026)
    assert _by(result)["Deputado Estadual"] == 94


def test_by_uf_grouping_uses_the_fallback_too(service):
    by_uf = _by(service.official_vacancies(group_by="uf", ano=2022, cargo="Deputado Estadual"), "uf")
    assert by_uf == {"SP": 2, "MG": 1}


def test_media_status_counts_as_elected(service):
    # 2010: "ELEITO" + "MÉDIA" = 2 eleitos.
    assert service.overview(ano=2010, cargo="Deputado Federal")["total_eleitos"] == 2
    ranking = service.ranking(group_by="cargo", metric="eleitos", ano=2010)
    assert sum(i["value"] for i in ranking) == 2
    assert _by(service.official_vacancies(group_by="cargo", ano=2010))["Deputado Federal"] == 2
