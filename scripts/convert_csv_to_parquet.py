#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Converte CSV para Parquet usando DuckDB.")
    parser.add_argument("--input", default="data/curated/analytics.csv", help="CSV de entrada.")
    parser.add_argument("--output", default="data/curated/analytics.parquet", help="Parquet de saída.")
    parser.add_argument(
        "--candidate-history-output",
        default="data/curated/candidate_history.parquet",
        help="Indice Parquet compacto usado pelos endpoints de historico.",
    )
    parser.add_argument("--delimiter", default=",", help="Delimitador do CSV.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    history_output_path = Path(args.candidate_history_output)

    if not input_path.exists():
        raise SystemExit(f"[convert] arquivo de entrada não encontrado: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    history_output_path.parent.mkdir(parents=True, exist_ok=True)

    csv_path = str(input_path).replace("'", "''")
    parquet_path = str(output_path).replace("'", "''")
    history_parquet_path = str(history_output_path).replace("'", "''")
    delim = str(args.delimiter).replace("'", "''")
    with input_path.open("r", encoding="utf-8", newline="") as fh:
        columns = next(csv.reader(fh, delimiter=args.delimiter), [])
    forced_varchar = [
        col
        for col in ["NR_CPF_CANDIDATO", "HISTORICO_CANDIDATURA_ID"]
        if col in columns
    ]
    types_sql = ""
    if forced_varchar:
        entries = ", ".join(f"'{col}': 'VARCHAR'" for col in forced_varchar)
        types_sql = f", types={{{entries}}}"

    con = duckdb.connect(database=":memory:")
    con.execute(
        "COPY ("
        f"SELECT * FROM read_csv_auto('{csv_path}', delim='{delim}', header=true, "
        f"ignore_errors=true{types_sql})"
        ") TO "
        f"'{parquet_path}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    municipal_cargos = "'PREFEITO', 'VICE-PREFEITO', 'VEREADOR'"
    con.execute(
        "COPY (SELECT "
        "ANO_ELEICAO, NR_TURNO, SG_UF, "
        f"CASE WHEN UPPER(TRIM(DS_CARGO)) IN ({municipal_cargos}) THEN NM_UE ELSE NULL END AS NM_UE, "
        "DS_CARGO, SQ_CANDIDATO, NR_CANDIDATO, NR_CPF_CANDIDATO, "
        "HISTORICO_CANDIDATURA_ID, NM_CANDIDATO, NM_URNA_CANDIDATO, SG_PARTIDO, "
        "DS_SIT_TOT_TURNO, DT_NASCIMENTO, "
        "SUM(QT_VOTOS_NOMINAIS_VALIDOS) AS QT_VOTOS_NOMINAIS_VALIDOS "
        f"FROM read_parquet('{parquet_path}') GROUP BY ALL) "
        f"TO '{history_parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    print(f"[convert] input: {input_path}")
    print(f"[convert] output: {output_path}")
    print(f"[convert] candidate history: {history_output_path}")


if __name__ == "__main__":
    main()
