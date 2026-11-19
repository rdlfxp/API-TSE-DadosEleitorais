"""Itens de zona sempre trazem zone_id, municipio, uf e codigos, em qualquer cargo (#5)."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def _row(cargo, uf, ue, cd, zona, sq, votes, ano=2026) -> dict:
    return {
        "ANO_ELEICAO": ano, "NR_TURNO": 1, "SG_UF": uf, "NM_UE": ue, "CD_MUNICIPIO": cd, "NR_ZONA": zona,
        "DS_CARGO": cargo, "SQ_CANDIDATO": sq, "NM_CANDIDATO": f"C{sq}", "SG_PARTIDO": "XX",
        "DS_SIT_TOT_TURNO": "ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": votes,
    }


@pytest.fixture
def service(tmp_path):
    rows = [
        # Presidente: a zona 372 existe em Sao Paulo e tambem em Campinas (numero repete entre cidades).
        _row("Presidente", "SP", "SÃO PAULO", 71072, 372, 1, 100),
        _row("Presidente", "SP", "SÃO PAULO", 71072, 1, 1, 40),
        _row("Presidente", "SP", "CAMPINAS", 62910, 372, 1, 60),
        _row("Presidente", "SP", "SÃO PAULO", 71072, 372, 2, 10),
        # Vereador (escopo municipal).
        _row("Vereador", "SP", "SÃO PAULO", 71072, 3, 50, 7, ano=2024),
        _row("Vereador", "SP", "SÃO PAULO", 71072, 4, 50, 9, ano=2024),
    ]
    path = tmp_path / "analytics.parquet"
    conn = duckdb.connect()
    try:
        conn.register("f", pd.DataFrame(rows))
        conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()
    svc = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100)
    yield svc
    svc.close()


REQUIRED = ("zone_id", "zone_name", "municipio", "uf", "cd_municipio", "cd_municipio_ibge")


def _zones(service, **kw):
    return service.candidate_vote_distribution(level="zona", page=1, page_size=100, **kw)["items"]


def test_state_office_zone_items_carry_all_identifying_fields(service):
    items = _zones(service, candidate_id="1", year=2026, office="Presidente", state="SP")
    assert len(items) == 3
    for item in items:
        assert all(item.get(field) not in (None, "") for field in REQUIRED), item
    assert {(i["cd_municipio_ibge"], i["zone_id"]) for i in items} == {
        ("3550308", "372"), ("3550308", "1"), ("3509502", "372"),
    }


def test_zone_number_comes_from_the_field_not_the_label(service):
    items = _zones(service, candidate_id="1", year=2026, office="Presidente", state="SP", municipality="SAO PAULO")
    top = max(items, key=lambda i: i["votes"])
    assert top["zone_id"] == "372" and top["municipio"] == "SÃO PAULO" and top["uf"] == "SP"
    assert top["label"] == "SÃO PAULO - Zona 372"  # rotulo preservado


def test_same_zone_number_in_two_cities_is_distinguishable(service):
    items = _zones(service, candidate_id="1", year=2026, office="Presidente", state="SP")
    zone_372 = [i for i in items if i["zone_id"] == "372"]
    assert len(zone_372) == 2
    assert {i["municipio"] for i in zone_372} == {"SÃO PAULO", "CAMPINAS"}


def test_municipal_office_zone_items_also_carry_codes(service):
    items = _zones(service, candidate_id="50", year=2024, office="Vereador")
    assert len(items) == 2
    for item in items:
        assert all(item.get(field) not in (None, "") for field in REQUIRED), item
        assert item["cd_municipio_ibge"] == "3550308"
