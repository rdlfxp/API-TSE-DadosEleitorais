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


# ---------------------------------------------------------------- SQ repetido em 2000-2008: pessoa unica
def _photo_index(tmp_path, rows):
    from app.services.photos import CandidatePhotoIndex

    path = tmp_path / "candidate_photos.parquet"
    conn = duckdb.connect()
    try:
        conn.register("f", pd.DataFrame(rows, columns=["ano", "uf", "ue", "sq"]).astype({"ano": "int16"}))
        conn.execute(f"COPY f TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()
    return CandidatePhotoIndex.from_file(path, "https://fotos.example.com")


def _marta_and_namesake_rows() -> list[dict]:
    # SQ 1823: Marta (2000, SP, Prefeito, sem historico oficial), um deputado do RJ de 2002 com
    # historico oficial ate 2026 e um vereador de 2008 do AM.
    return [
        _hist_row(2000, "SP", "Prefeito", 1823, 13, "MARTA TERESA SUPLICY", "tse-candidate:2000:1823", votes=3000, ue="71072"),
        _hist_row(2008, "SP", "Prefeito", 1197, 13, "MARTA TERESA SUPLICY", "tse-candidate:2008:1197:M71072", votes=2000, ue="71072"),
        _hist_row(2014, "SP", "Senador", 250000001674, 13, "MARTA TERESA SUPLICY", "tse-candidate:2014:250000001674", votes=1000),
        _hist_row(2002, "RJ", "Deputado Estadual", 1823, 20000, "MARIO REIS ESTEVES", "tse-history:2026:190002550512", votes=50),
        _hist_row(2026, "RJ", "Deputado Federal", 190002550512, 20000, "MARIO REIS ESTEVES", "tse-history:2026:190002550512", votes=80),
        _hist_row(2008, "AM", "Vereador", 1823, 12345, "MANOEL VALENTE", "tse-candidate:2008:1823:M2534", votes=900, ue="2534"),
    ]


def test_vote_history_with_repeated_sq_never_leaks_the_namesake(tmp_path):
    service = _history_service(tmp_path, _marta_and_namesake_rows())
    try:
        scoped = service.candidate_vote_history("1823", state="SP", office="Prefeito")
        unscoped = service.candidate_vote_history("1823")
        by_year = service.candidate_vote_history("1823", year=2000)
    finally:
        service.close()
    # Com ou sem recorte: so a Marta (a de maior votacao), jamais o historico do deputado do RJ.
    for history in (scoped, unscoped, by_year):
        assert {item["year"] for item in history["items"]} == {2000, 2008, 2014}
        assert all(item["state"] == "SP" for item in history["items"]), history
        assert "190002550512" not in str(history)


def test_vote_history_scoped_to_the_namesake_returns_the_namesake(tmp_path):
    service = _history_service(tmp_path, _marta_and_namesake_rows())
    try:
        history = service.candidate_vote_history("1823", state="RJ", office="Deputado Estadual")
    finally:
        service.close()
    assert {item["year"] for item in history["items"]} == {2002, 2026}


def test_summary_with_repeated_sq_sums_only_one_person_and_one_municipality(tmp_path):
    # SQ 1 em 2004: prefeitos diferentes em municipios diferentes da mesma UF.
    service = _history_service(
        tmp_path,
        [
            _hist_row(2004, "AC", "Prefeito", 1, 11, "PREFEITO DE RIO BRANCO", "tse-candidate:2004:1:M1", votes=5000, ue="1"),
            _hist_row(2004, "AC", "Prefeito", 1, 12, "PREFEITO DE CRUZEIRO", "tse-candidate:2004:1:M2", votes=700, ue="2"),
        ],
    )
    try:
        biggest = service.candidate_summary("1", year=2004, state="AC", office="Prefeito")
        other = service.candidate_summary("1", year=2004, state="AC", office="Prefeito", municipio="2")
    finally:
        service.close()
    assert biggest["name"] == "PREFEITO DE RIO BRANCO" and biggest["latest_vote_value"] == 5000
    assert biggest["votos_consolidados"] == 5000  # nao soma os dois prefeitos
    assert other["name"] == "PREFEITO DE CRUZEIRO" and other["latest_vote_value"] == 700


def test_photo_lookup_refuses_an_ambiguous_municipality(tmp_path):
    index = _photo_index(tmp_path, [(2004, "AC", "100", "1"), (2004, "AC", "200", "1")])
    assert index.lookup(2004, "AC", 1, ues=["100"]) == "https://fotos.example.com/fotos/2004/ac/100/1.jpg"
    assert index.lookup(2004, "AC", 1, ues=["100", "200"]) is None  # duas pessoas possiveis
    assert index.lookup(2004, "AC", 1, ues=[]) is None and index.lookup(2004, "AC", 1, ues=[""]) is None


def test_summary_falls_back_to_the_photo_of_the_same_person_only(tmp_path):
    service = _history_service(tmp_path, _marta_and_namesake_rows())
    service.photo_index = _photo_index(
        tmp_path,
        [
            (2014, "SP", "", "250000001674"),  # Marta senadora 2014
            (2026, "RJ", "", "190002550512"),  # deputado do RJ: nunca pode aparecer para a Marta
            (2008, "AM", "2534", "1823"),  # vereador do AM com o mesmo SQ da Marta em 2000
        ],
    )
    try:
        summary = service.candidate_summary("1823", year=2000, state="SP", office="Prefeito")
        no_scope = service.candidate_summary("1823")
        no_photo = service.candidate_summary("1823", year=2002, state="RJ", office="Deputado Estadual")
        calls = []

        def live(**kwargs):
            calls.append(kwargs)
            return None

        service.candidate_summary("1823", year=2000, state="SP", office="Prefeito", live_photo_url=live)
    finally:
        service.close()
    assert summary["foto_url"] == "https://fotos.example.com/fotos/2014/sp/250000001674.jpg"
    assert no_scope["name"] == "MARTA TERESA SUPLICY"
    assert "190002550512" not in str(summary["foto_url"]) and "/2008/am/" not in str(summary["foto_url"])
    # O deputado do RJ so tem foto em 2026 (do indice): essa e a dele, nao a da Marta.
    assert no_photo["name"] == "MARIO REIS ESTEVES"
    assert no_photo["foto_url"] == "https://fotos.example.com/fotos/2026/rj/190002550512.jpg"
    assert any(call["candidate_sq"] == "250000001674" and call["year"] == 2014 for call in calls)


def test_summary_keeps_no_photo_when_the_only_other_holder_has_another_name(tmp_path):
    service = _history_service(
        tmp_path,
        [
            _hist_row(2000, "SP", "Prefeito", 1823, 13, "MARTA TERESA SUPLICY", "tse-candidate:2000:1823", votes=3000, ue="71072"),
            _hist_row(2008, "AM", "Vereador", 1823, 12345, "MANOEL VALENTE", "tse-candidate:2008:1823:M2534", votes=900, ue="2534"),
        ],
    )
    service.photo_index = _photo_index(tmp_path, [(2008, "AM", "2534", "1823")])
    try:
        summary = service.candidate_summary("1823", year=2000, state="SP", office="Prefeito")
    finally:
        service.close()
    assert summary["foto_url"] is None
