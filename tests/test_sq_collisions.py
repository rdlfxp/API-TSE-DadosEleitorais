"""SQ_CANDIDATO nao e unico em 2002/2006/2008: repete entre UFs e municipios."""
from __future__ import annotations

import sys

import duckdb
import pandas as pd

from scripts import merge_curated_year
from scripts.normalize import (
    _normalize_votacao_file,
    _prepare_consulta,
    _prepare_historico,
    _scope_key,
)


def _csv(tmp_path, name: str, rows: list[dict]) -> str:
    path = tmp_path / name
    pd.DataFrame(rows).to_csv(path, sep=";", index=False, encoding="latin1")
    return str(path)


def _consulta_row(sq, uf, ue, cargo, name, cpf, birth, gender) -> dict:
    return {
        "ANO_ELEICAO": 2008,
        "SQ_CANDIDATO": sq,
        "SG_UF": uf,
        "SG_UE": ue,
        "DS_CARGO": cargo,
        "NM_CANDIDATO": name,
        "NR_CPF_CANDIDATO": cpf,
        "DT_NASCIMENTO": birth,
        "DS_GENERO": gender,
    }


def _vote_row(sq, uf, ue, cargo, name, votes) -> dict:
    return {
        "ANO_ELEICAO": 2008,
        "NR_TURNO": 1,
        "SG_UF": uf,
        "SG_UE": ue,
        "CD_MUNICIPIO": ue,
        "NM_MUNICIPIO": f"MUN {ue}",
        "NR_ZONA": 1,
        "CD_CARGO": 13,
        "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq,
        "NR_CANDIDATO": 11111,
        "NM_CANDIDATO": name,
        "SG_PARTIDO": "XX",
        "DS_SIT_TOT_TURNO": "ELEITO",
        "QT_VOTOS_NOMINAIS_VALIDOS": votes,
    }


def test_scope_key_by_cargo():
    df = pd.DataFrame(
        [
            {"DS_CARGO": "Presidente", "SG_UF": "SP", "SG_UE": "BR"},
            {"DS_CARGO": "VEREADOR", "SG_UF": "SP", "SG_UE": "067172"},
            {"DS_CARGO": "Vereador", "SG_UF": "SP", "SG_UE": "67172"},
            {"DS_CARGO": "Deputado Federal", "SG_UF": "mg", "SG_UE": "MG"},
        ]
    )
    assert _scope_key(df).tolist() == ["BR", "M67172", "M67172", "UMG"]


def test_same_sq_in_two_municipalities_keeps_each_persons_profile_and_history(tmp_path):
    consulta = _prepare_consulta(
        [
            _csv(
                tmp_path,
                "consulta_cand_2008_BRASIL.csv",
                [
                    _consulta_row(51816, "SP", 67172, "VEREADOR", "JOSE CANDIDO", "11111111111", "01/01/1950", "MASCULINO"),
                    _consulta_row(51816, "MG", 50911, "VEREADOR", "ZENI FRANCISCA", "22222222222", "19/04/1954", "FEMININO"),
                ],
            )
        ],
        sep=";",
        encoding="latin1",
    )
    historico = _prepare_historico(
        [
            _csv(
                tmp_path,
                "historico_candidatura_2026_BRASIL.csv",
                [
                    {
                        "ANO_ELEICAO_ATUAL": 2026,
                        "SQ_CANDIDATO_ATUAL": 130002540613,
                        "ANO_ELEICAO": 2008,
                        "SQ_CANDIDATO": 51816,
                        "SG_UF": "MG",
                        "SG_UE": 50911,
                        "DS_CARGO": "Vereador",
                    }
                ],
            )
        ],
        sep=";",
        encoding="latin1",
    )
    votes = _csv(
        tmp_path,
        "votacao_candidato_munzona_2008_BRASIL.csv",
        [
            _vote_row(51816, "SP", 67172, "VEREADOR", "JOSE CANDIDO", 100),
            _vote_row(51816, "MG", 50911, "VEREADOR", "ZENI FRANCISCA", 200),
        ],
    )
    out = _normalize_votacao_file(
        file_path=votes, consulta_df=consulta, historico_df=historico, vagas_df=None, sep=";", encoding="latin1"
    ).set_index("NM_CANDIDATO")

    assert len(out) == 2  # nenhuma linha multiplicada
    assert out["QT_VOTOS_NOMINAIS_VALIDOS"].sum() == 300
    jose, zeni = out.loc["JOSE CANDIDO"], out.loc["ZENI FRANCISCA"]
    assert (jose["NR_CPF_CANDIDATO"], jose["DS_GENERO"]) == ("11111111111", "MASCULINO")
    assert (zeni["NR_CPF_CANDIDATO"], zeni["DS_GENERO"]) == ("22222222222", "FEMININO")
    # So a Zeni (MG) tem historico oficial; o Jose nao herda o id dela pelo SQ repetido.
    assert zeni["HISTORICO_CANDIDATURA_ID"] == "tse-history:2026:130002540613"
    assert pd.isna(jose["HISTORICO_CANDIDATURA_ID"])


def test_merge_does_not_spread_history_id_to_colliding_sq(tmp_path, monkeypatch):
    def write(df, name):
        path = tmp_path / name
        conn = duckdb.connect()
        try:
            conn.register("f", df)
            conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
        finally:
            conn.close()
        return path

    def base_row(sq, uf, mun, name, hid):
        return {
            "ANO_ELEICAO": 2008, "NR_TURNO": 1, "SG_UF": uf, "NM_UE": f"MUN {mun}",
            "CD_MUNICIPIO": mun, "DS_CARGO": "Vereador", "SQ_CANDIDATO": sq, "NM_CANDIDATO": name,
            "HISTORICO_CANDIDATURA_ID": hid, "QT_VOTOS_NOMINAIS_VALIDOS": 10,
        }

    # Base antiga: o id de uma pessoa foi espalhado para todo mundo com o mesmo SQ.
    stale = "tse-history:2026:130002540613"
    base = write(
        pd.DataFrame([base_row(51816, "SP", 67172, "JOSE", stale), base_row(51816, "MG", 50911, "ZENI", stale)]),
        "base.parquet",
    )
    year = write(
        pd.DataFrame(
            [{"ANO_ELEICAO": 2026, "NR_TURNO": 1, "SG_UF": "SP", "DS_CARGO": "Vereador", "SQ_CANDIDATO": 1,
              "QT_VOTOS_NOMINAIS_VALIDOS": 1}]
        ),
        "year.parquet",
    )
    history = _csv(
        tmp_path,
        "historico.csv",
        [{"ANO_ELEICAO_ATUAL": 2026, "SQ_CANDIDATO_ATUAL": 130002540613, "ANO_ELEICAO": 2008,
          "SQ_CANDIDATO": 51816, "SG_UF": "MG", "SG_UE": 50911, "DS_CARGO": "Vereador"}],
    )
    output = tmp_path / "out" / "analytics.parquet"
    monkeypatch.setattr(
        sys, "argv",
        ["merge_curated_year.py", "--base", str(base), "--year-file", str(year), "--target-year", "2026",
         "--historico", history, "--output", str(output), "--report", str(tmp_path / "q.json"),
         "--manifest", str(tmp_path / "m.json")],
    )
    merge_curated_year.main()

    conn = duckdb.connect()
    try:
        rows = dict(conn.execute(
            "SELECT NM_CANDIDATO, HISTORICO_CANDIDATURA_ID FROM read_parquet(?) WHERE ANO_ELEICAO = 2008",
            [str(output)],
        ).fetchall())
    finally:
        conn.close()
    assert rows["ZENI"] == stale
    # Sem historico proprio, o Jose recebe um id sintetico distinto (com escopo, pois o SQ colide).
    assert rows["JOSE"] != stale
    assert rows["JOSE"] == "tse-candidate:2008:51816:M67172"


def _history_service(tmp_path, rows: list[dict]):
    from app.services.analytics.duckdb_service import DuckDBAnalyticsService

    path = tmp_path / "svc.parquet"
    conn = duckdb.connect()
    try:
        conn.register("f", pd.DataFrame(rows))
        conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()
    return DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100)


def _hist_row(year, uf, cargo, sq, nr, name, hid, votes=100, ue=None) -> dict:
    return {
        "ANO_ELEICAO": year, "NR_TURNO": 1, "SG_UF": uf, "NM_UE": ue, "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq, "NR_CANDIDATO": nr, "NM_CANDIDATO": name,
        "HISTORICO_CANDIDATURA_ID": hid, "QT_VOTOS_NOMINAIS_VALIDOS": votes,
    }


def test_vote_history_reports_br_for_president_not_an_arbitrary_uf(tmp_path):
    service = _history_service(
        tmp_path,
        [
            _hist_row(2006, uf, "Presidente", 23, 13, "LULA", "tse-history:2026:1", votes=10)
            for uf in ("AC", "PB", "RS")
        ],
    )
    try:
        items = service.candidate_vote_history("23")["items"]
    finally:
        service.close()
    assert [(i["office"], i["state"]) for i in items] == [("Presidente", "BR")]


def test_vote_history_prefers_sq_over_candidate_number(tmp_path):
    # "23" e o SQ do Lula em 2006 e tambem o NUMERO de outro candidato em outro ano.
    service = _history_service(
        tmp_path,
        [
            _hist_row(2006, "SP", "Presidente", 23, 13, "LULA", "tse-history:2026:1", votes=500),
            _hist_row(2014, "SP", "Deputado Federal", 999001, 23, "OUTRA PESSOA", "tse-history:2026:2", votes=70),
        ],
    )
    try:
        history = service.candidate_vote_history("23")
    finally:
        service.close()
    assert [i["year"] for i in history["items"]] == [2006]
    assert history["items"][0]["votes"] == 500


def test_merge_gives_target_year_rows_their_official_id_by_scope(tmp_path, monkeypatch):
    """Reprocessar 2008 como ano alvo: o historico oficial fica em raw/2026, nao em raw/2008."""

    def write(df, name):
        path = tmp_path / name
        conn = duckdb.connect()
        try:
            conn.register("f", df)
            conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
        finally:
            conn.close()
        return path

    def vrow(year, sq, uf, mun, name):
        return {
            "ANO_ELEICAO": year, "NR_TURNO": 1, "SG_UF": uf, "NM_UE": f"MUN {mun}", "CD_MUNICIPIO": mun,
            "DS_CARGO": "Vereador", "SQ_CANDIDATO": sq, "NM_CANDIDATO": name,
            "QT_VOTOS_NOMINAIS_VALIDOS": 10,
        }

    base = write(pd.DataFrame([vrow(2026, 1, "SP", 1, "ATUAL")]), "base.parquet")
    year = write(
        pd.DataFrame([vrow(2008, 51816, "SP", 67172, "JOSE"), vrow(2008, 51816, "MG", 50911, "ZENI")]),
        "year.parquet",
    )
    history = _csv(
        tmp_path,
        "historico.csv",
        [{"ANO_ELEICAO_ATUAL": 2026, "SQ_CANDIDATO_ATUAL": 130002540613, "ANO_ELEICAO": 2008,
          "SQ_CANDIDATO": 51816, "SG_UF": "MG", "SG_UE": 50911, "DS_CARGO": "Vereador"}],
    )
    output = tmp_path / "out" / "analytics.parquet"
    monkeypatch.setattr(
        sys, "argv",
        ["merge_curated_year.py", "--base", str(base), "--year-file", str(year), "--target-year", "2008",
         "--historico", history, "--output", str(output), "--report", str(tmp_path / "q.json"),
         "--manifest", str(tmp_path / "m.json"), "--no-compact"],
    )
    merge_curated_year.main()

    conn = duckdb.connect()
    try:
        rows = dict(conn.execute(
            "SELECT NM_CANDIDATO, HISTORICO_CANDIDATURA_ID FROM read_parquet(?) WHERE ANO_ELEICAO = 2008",
            [str(output)],
        ).fetchall())
    finally:
        conn.close()
    assert rows["ZENI"] == "tse-history:2026:130002540613"
    # Sem historico oficial: id sintetico com o escopo, porque o SQ se repete entre UFs no proprio ano.
    assert rows["JOSE"] == "tse-candidate:2008:51816:M67172"


def test_summary_matches_sq_before_candidate_number(tmp_path):
    service = _history_service(
        tmp_path,
        [
            _hist_row(2006, "SP", "Presidente", 23, 13, "LULA", "tse-history:2026:1", votes=500),
            _hist_row(2014, "SP", "Deputado Federal", 999001, 23, "OUTRA PESSOA", "tse-history:2026:2", votes=70),
        ],
    )
    try:
        summary = service.candidate_summary("23")
    finally:
        service.close()
    assert summary["office"] == "Presidente"
    assert summary["latest_election"]["year"] == 2006
