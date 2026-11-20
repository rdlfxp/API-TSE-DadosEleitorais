from __future__ import annotations

import json
import sys

import duckdb
import pandas as pd

from scripts import merge_curated_year
from app.services.analytics.duckdb_service import DuckDBAnalyticsService


def test_merge_curated_year_replaces_partition_and_bridges_history(tmp_path, monkeypatch):
    base = tmp_path / "base.parquet"
    year_file = tmp_path / "year.parquet"
    history_file = tmp_path / "history.csv"
    output = tmp_path / "merged" / "analytics.parquet"
    report = tmp_path / "quality.json"
    manifest = tmp_path / "manifest.json"

    base_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Deputado Estadual",
                "SQ_CANDIDATO": 22001,
                "NR_CPF_CANDIDATO": 12345678901.0,
                "QT_VOTOS_NOMINAIS_VALIDOS": 12000,
            },
            {
                "ANO_ELEICAO": 2026,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Deputado Estadual",
                "SQ_CANDIDATO": 99999,
                "QT_VOTOS_NOMINAIS_VALIDOS": 1,
            },
        ]
    )
    year_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2026,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "DS_CARGO": "Deputado Estadual",
                "SQ_CANDIDATO": 26001,
                "NR_CPF_CANDIDATO": "12345678901",
                "HISTORICO_CANDIDATURA_ID": "tse-history:2026:26001",
                "QT_VOTOS_NOMINAIS_VALIDOS": 20000,
            }
        ]
    )
    fixture_conn = duckdb.connect()
    try:
        fixture_conn.register("base_df", base_df)
        fixture_conn.register("year_df", year_df)
        fixture_conn.execute(f"COPY base_df TO '{str(base).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
        fixture_conn.execute(f"COPY year_df TO '{str(year_file).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        fixture_conn.close()
    pd.DataFrame(
        [
            {
                "ANO_ELEICAO_ATUAL": 2026,
                "SQ_CANDIDATO_ATUAL": 26001,
                "ANO_ELEICAO": 2022,
                "SQ_CANDIDATO": 22001,
            }
        ]
    ).to_csv(history_file, sep=";", encoding="latin1", index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "merge_curated_year.py",
            "--base",
            str(base),
            "--year-file",
            str(year_file),
            "--target-year",
            "2026",
            "--historico",
            str(history_file),
            "--output",
            str(output),
            "--report",
            str(report),
            "--manifest",
            str(manifest),
        ],
    )
    merge_curated_year.main()

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            "SELECT ANO_ELEICAO, SQ_CANDIDATO, HISTORICO_CANDIDATURA_ID "
            "FROM read_parquet(?) ORDER BY ANO_ELEICAO",
            [str(output)],
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        (2022, 22001, "tse-history:2026:26001"),
        (2026, 26001, "tse-history:2026:26001"),
    ]
    assert json.loads(report.read_text())["anos"] == [2022, 2026]
    assert json.loads(manifest.read_text())["summary"]["anos"] == [2022, 2026]

    service = DuckDBAnalyticsService.from_file(str(output), default_top_n=20, max_top_n=100)
    try:
        assert service._candidate_history_relation == "candidate_history"
        history = service.candidate_vote_history("26001", state="SP", office="Deputado Estadual")
    finally:
        service.close()
    assert [item["year"] for item in history["items"]] == [2026, 2022]


def _write_parquet(df: pd.DataFrame, path) -> None:
    conn = duckdb.connect()
    try:
        conn.register("frame", df)
        conn.execute(f"COPY frame TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()


def _zone_row(cargo: str, sq: int, turno: int, mun: int, zona: int, votos: int, uf: str = "SP", **extra) -> dict:
    return {
        "ANO_ELEICAO": 2026,
        "NR_TURNO": turno,
        "SG_UF": uf,
        "NM_UE": f"MUNICIPIO {mun}",
        "CD_MUNICIPIO": mun,
        "NR_ZONA": zona,
        "DS_CARGO": cargo,
        "SQ_CANDIDATO": sq,
        "NM_CANDIDATO": f"CANDIDATO {sq}",
        "SG_PARTIDO": "PX",
        "DS_GENERO": "FEMININO",
        "DS_SIT_TOT_TURNO": extra.pop("situacao", "ELEITO"),
        "QT_VOTOS_NOMINAIS_VALIDOS": votos,
        "QT_VAGAS": extra.pop("vagas", 3),
        "IDADE": 50.0,
        **extra,
    }


def test_merge_compacts_state_offices_to_one_row_per_candidate_turn(tmp_path, monkeypatch):
    base = tmp_path / "base.parquet"
    year_file = tmp_path / "year.parquet"
    output = tmp_path / "analytics.parquet"
    _write_parquet(
        pd.DataFrame([{"ANO_ELEICAO": 2022, "NR_TURNO": 1, "SG_UF": "SP", "DS_CARGO": "Senador",
                       "SQ_CANDIDATO": 1, "QT_VOTOS_NOMINAIS_VALIDOS": 10}]),
        base,
    )
    _write_parquet(
        pd.DataFrame(
            [
                # Senador eleito com votos em 3 zonas: deve virar 1 linha com a soma.
                _zone_row("Senador", 101, 1, 71072, 1, 100),
                _zone_row("Senador", 101, 1, 71072, 2, 50),
                _zone_row("Senador", 101, 1, 62391, 3, 25),
                # Governador com 2o turno: 1 linha por turno.
                _zone_row("Governador", 201, 1, 71072, 1, 40, situacao="2º TURNO", vagas=1),
                _zone_row("Governador", 201, 1, 62391, 3, 60, situacao="2º TURNO", vagas=1),
                _zone_row("Governador", 201, 2, 71072, 1, 70, vagas=1),
                _zone_row("Governador", 201, 2, 62391, 3, 30, vagas=1),
                # Presidente mantem o detalhe por zona (mapa).
                _zone_row("Presidente", 301, 1, 71072, 1, 5, vagas=1),
                _zone_row("Presidente", 301, 1, 62391, 3, 7, vagas=1),
            ]
        ),
        year_file,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["merge_curated_year.py", "--base", str(base), "--year-file", str(year_file), "--target-year", "2026",
         "--output", str(output), "--report", str(tmp_path / "q.json"), "--manifest", str(tmp_path / "m.json")],
    )
    merge_curated_year.main()

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            "SELECT DS_CARGO, NR_TURNO, SQ_CANDIDATO, NM_UE, CD_MUNICIPIO, NR_ZONA, QT_VOTOS_NOMINAIS_VALIDOS, "
            "QT_VAGAS, DS_SIT_TOT_TURNO, DS_GENERO FROM read_parquet(?) WHERE ANO_ELEICAO = 2026 "
            "AND DS_CARGO <> 'Presidente' ORDER BY 1, 2",
            [str(output)],
        ).fetchall()
        presidente_rows = conn.execute(
            "SELECT COUNT(*), SUM(QT_VOTOS_NOMINAIS_VALIDOS), COUNT(NR_ZONA) FROM read_parquet(?) "
            "WHERE DS_CARGO = 'Presidente'",
            [str(output)],
        ).fetchone()
    finally:
        conn.close()

    assert rows == [
        ("Governador", 1, 201, "SÃO PAULO", None, None, 100, 1, "2º TURNO", "FEMININO"),
        ("Governador", 2, 201, "SÃO PAULO", None, None, 100, 1, "ELEITO", "FEMININO"),
        ("Senador", 1, 101, "SÃO PAULO", None, None, 175, 3, "ELEITO", "FEMININO"),
    ]
    assert presidente_rows == (2, 12, 2)

    service = DuckDBAnalyticsService.from_file(str(output), default_top_n=20, max_top_n=100)
    try:
        ranking = service.ranking(group_by="partido", metric="eleitos", ano=2026, cargo="Senador")
    finally:
        service.close()
    # Antes da compactacao, cada zona contava como um eleito (3 em vez de 1).
    assert [(item["label"], item["value"]) for item in ranking] == [("PX", 1.0)]


def test_merge_no_compact_keeps_zone_rows(tmp_path, monkeypatch):
    base = tmp_path / "base.parquet"
    year_file = tmp_path / "year.parquet"
    output = tmp_path / "analytics.parquet"
    _write_parquet(pd.DataFrame([{"ANO_ELEICAO": 2022, "NR_TURNO": 1, "SG_UF": "SP", "DS_CARGO": "Senador",
                                  "SQ_CANDIDATO": 1, "QT_VOTOS_NOMINAIS_VALIDOS": 10}]), base)
    _write_parquet(pd.DataFrame([_zone_row("Senador", 101, 1, 71072, 1, 100),
                                 _zone_row("Senador", 101, 1, 71072, 2, 50)]), year_file)
    monkeypatch.setattr(
        sys,
        "argv",
        ["merge_curated_year.py", "--base", str(base), "--year-file", str(year_file), "--target-year", "2026",
         "--output", str(output), "--report", str(tmp_path / "q.json"), "--manifest", str(tmp_path / "m.json"),
         "--no-compact"],
    )
    merge_curated_year.main()
    conn = duckdb.connect()
    try:
        count = conn.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE ANO_ELEICAO = 2026", [str(output)]).fetchone()
    finally:
        conn.close()
    assert count == (2,)


def test_merge_reads_br_dates_computes_age_and_keeps_history_id(tmp_path, monkeypatch):
    base = tmp_path / "base.parquet"
    year_file = tmp_path / "year.parquet"
    output = tmp_path / "analytics.parquet"
    _write_parquet(pd.DataFrame([{"ANO_ELEICAO": 2020, "NR_TURNO": 1, "SG_UF": "SP", "DS_CARGO": "Vereador",
                                  "SQ_CANDIDATO": 1, "QT_VOTOS_NOMINAIS_VALIDOS": 10}]), base)

    def row(sq: int, nasc: str | None, **extra) -> dict:
        return {"ANO_ELEICAO": 2022, "NR_TURNO": 1, "SG_UF": "SP", "DS_CARGO": "Deputado Federal",
                "SQ_CANDIDATO": sq, "DT_ELEICAO": "02/10/2022", "DT_NASCIMENTO": nasc,
                "FAIXA_ETARIA": "N/A", "IDADE": None, "QT_VOTOS_NOMINAIS_VALIDOS": 5, **extra}

    _write_parquet(
        pd.DataFrame(
            [
                row(1, "29/07/1965"),
                row(2, "17/10/2000"),
                row(3, "sem data"),
                row(4, None, IDADE=75.0),
            ]
        ),
        year_file,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["merge_curated_year.py", "--base", str(base), "--year-file", str(year_file), "--target-year", "2022",
         "--output", str(output), "--report", str(tmp_path / "q.json"), "--manifest", str(tmp_path / "m.json")],
    )
    merge_curated_year.main()

    conn = duckdb.connect()
    try:
        rows = conn.execute(
            "SELECT SQ_CANDIDATO, CAST(DT_ELEICAO AS VARCHAR), IDADE, FAIXA_ETARIA, HISTORICO_CANDIDATURA_ID "
            "FROM read_parquet(?) WHERE ANO_ELEICAO = 2022 ORDER BY SQ_CANDIDATO",
            [str(output)],
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        (1, "2022-10-02", 57.0, "50-59", "tse-candidate:2022:1"),
        (2, "2022-10-02", 22.0, "18-29", "tse-candidate:2022:2"),
        (3, "2022-10-02", None, "N/A", "tse-candidate:2022:3"),
        (4, "2022-10-02", 75.0, "70+", "tse-candidate:2022:4"),
    ]
