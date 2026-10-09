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
