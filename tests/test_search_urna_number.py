"""Busca de candidatos por nome de urna e numero (alem do nome de registro)."""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from app.domain.normalizers import is_unscoped_candidate_search
from app.services.analytics import AnalyticsService
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def _row(sq, nr, name, urna, cargo="Presidente", uf="BR", votes=100, year=2022, **extra) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": 1, "SG_UF": uf, "DS_CARGO": cargo, "SQ_CANDIDATO": sq,
        "NR_CANDIDATO": nr, "NM_CANDIDATO": name, "NM_URNA_CANDIDATO": urna, "SG_PARTIDO": "XX",
        "DS_SIT_TOT_TURNO": "ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": votes, **extra,
    }


ROWS = [
    _row(1, 13, "LUIZ INÁCIO LULA DA SILVA", "LULA", votes=900),
    _row(2, 22, "JAIR MESSIAS BOLSONARO", "JAIR BOLSONARO", votes=800),
    _row(3, 1234, "FRANCISCO EVERARDO OLIVEIRA SILVA", "TIRIRICA", cargo="Deputado Federal", uf="SP", votes=700),
    _row(4, 13456, "MARIA DA SILVA", "MARIA DO PT", cargo="Deputado Federal", uf="SP", votes=50),
]


@pytest.fixture(params=["pandas", "duckdb"])
def service(request, tmp_path):
    df = pd.DataFrame(ROWS)
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


def _names(payload) -> list[str]:
    return [item["candidato"] for item in payload["items"]]


def test_finds_candidate_by_urna_name_not_in_registered_name(service):
    payload = service.search_candidates(q="tiririca", ano=2022)
    assert _names(payload) == ["FRANCISCO EVERARDO OLIVEIRA SILVA"]
    assert payload["items"][0]["nm_urna_candidato"] == "TIRIRICA"


def test_urna_search_ignores_accents_and_case(service):
    assert _names(service.search_candidates(q="LuLa", ano=2022)) == ["LUIZ INÁCIO LULA DA SILVA"]
    assert _names(service.search_candidates(q="inacio", ano=2022)) == ["LUIZ INÁCIO LULA DA SILVA"]


def test_finds_candidate_by_exact_number_only(service):
    payload = service.search_candidates(q="13", ano=2022)
    # 13 e Lula; 13456 e outro numero e nao pode vir por prefixo.
    assert _names(payload) == ["LUIZ INÁCIO LULA DA SILVA"]
    assert payload["items"][0]["numero"] == "13"


def test_number_search_respects_filters(service):
    assert _names(service.search_candidates(q="13", ano=2022, cargo="Deputado Federal")) == []
    assert _names(service.search_candidates(q="1234", ano=2022, uf="SP")) == [
        "FRANCISCO EVERARDO OLIVEIRA SILVA"
    ]


def test_registered_name_search_still_works(service):
    assert _names(service.search_candidates(q="messias", ano=2022)) == ["JAIR MESSIAS BOLSONARO"]
    assert service.search_candidates(q="inexistente", ano=2022)["total"] == 0


def test_unscoped_guard_allows_short_urna_names_and_numbers():
    assert is_unscoped_candidate_search("lula", None, None, None) is False
    assert is_unscoped_candidate_search("13", None, None, None) is False
    assert is_unscoped_candidate_search("lu", None, None, None) is True
    assert is_unscoped_candidate_search("lu", None, "Presidente", None) is False
