"""Codigos TSE/IBGE nos itens de vote-distribution (nivel municipio) — #12."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from app.domain.municipios import lookup_municipio, normalize_tse_code
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def test_lookup_by_tse_code_and_by_name():
    assert lookup_municipio("SP", "71072")["cd_municipio_ibge"] == "3550308"
    assert lookup_municipio("sp", 71072)["cd_municipio_ibge"] == "3550308"
    by_name = lookup_municipio("SP", None, "são paulo")
    assert (by_name["cd_municipio"], by_name["cd_municipio_ibge"]) == ("71072", "3550308")


def test_homonymous_cities_resolve_per_uf():
    assert lookup_municipio("PB", None, "BOA VISTA")["cd_municipio_ibge"] == "2502151"
    assert lookup_municipio("RR", None, "BOA VISTA")["cd_municipio_ibge"] == "1400100"


def test_special_units_and_unknowns():
    assert lookup_municipio("DF", "97012")["cd_municipio_ibge"] == "5300108"  # Brasilia
    assert lookup_municipio("PE", "30015")["cd_municipio_ibge"] == "2605459"  # Noronha
    unknown = lookup_municipio("ZZ", "00001", "ARGENTINA")  # exterior
    assert unknown["cd_municipio_ibge"] is None and unknown["cd_municipio"] == "00001"
    assert normalize_tse_code("abc") is None and normalize_tse_code("123456") is None


def _row(uf, ue, cd, sq, votes, ano=2022, cargo="Presidente") -> dict:
    return {
        "ANO_ELEICAO": ano, "NR_TURNO": 1, "SG_UF": uf, "NM_UE": ue, "CD_MUNICIPIO": cd, "NR_ZONA": 1,
        "DS_CARGO": cargo, "SQ_CANDIDATO": sq, "NM_CANDIDATO": f"C{sq}", "SG_PARTIDO": "XX",
        "DS_SIT_TOT_TURNO": "ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": votes,
    }


@pytest.fixture
def service(tmp_path):
    rows = [
        # Mesma cidade homonima em duas UFs: nao podem se fundir.
        _row("PB", "BOA VISTA", 19941, 1, 100), _row("PB", "BOA VISTA", 19941, 2, 50),
        _row("RR", "BOA VISTA", 3018, 1, 300), _row("RR", "BOA VISTA", 3018, 2, 80),
        # Particao antiga: NM_UE="BRASIL" nas linhas por municipio.
        _row("SP", "BRASIL", 71072, 1, 900), _row("SP", "BRASIL", 71072, 2, 400),
        # Exterior: sem IBGE.
        _row("ZZ", "ARGENTINA", 1, 1, 7), _row("ZZ", "ARGENTINA", 1, 2, 3),
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


def _items(service):
    result = service.candidate_vote_distribution(
        candidate_id="1", level="municipio", year=2022, office="Presidente", page=1, page_size=100
    )
    return result, {(i["uf"], i["label"]): i for i in result["items"]}


def test_homonymous_cities_are_kept_apart_with_their_codes(service):
    result, items = _items(service)
    assert result["total_items"] == 4
    assert items[("PB", "BOA VISTA")]["votes"] == 100 and items[("PB", "BOA VISTA")]["cd_municipio_ibge"] == "2502151"
    assert items[("RR", "BOA VISTA")]["votes"] == 300 and items[("RR", "BOA VISTA")]["cd_municipio_ibge"] == "1400100"
    assert items[("RR", "BOA VISTA")]["cd_municipio"] == "03018"


def test_city_percentages_use_the_right_city_total(service):
    _, items = _items(service)
    assert items[("RR", "BOA VISTA")]["percentual_candidato_no_municipio"] == pytest.approx(300 / 380 * 100, rel=1e-3)
    assert items[("PB", "BOA VISTA")]["votos_validos_do_municipio"] == 150


def test_brasil_placeholder_name_is_replaced_by_the_registry_name(service):
    _, items = _items(service)
    assert ("SP", "SÃO PAULO") in items
    assert items[("SP", "SÃO PAULO")]["cd_municipio_ibge"] == "3550308"


def test_foreign_zone_has_no_ibge_code(service):
    _, items = _items(service)
    assert items[("ZZ", "ARGENTINA")]["cd_municipio_ibge"] is None
