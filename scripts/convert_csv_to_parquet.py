#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Converte CSV para Parquet usando DuckDB.")
    parser.add_argument("--input", default="data/curated/analytics.csv", help="CSV de entrada.")
    parser.add_argument("--output", default="data/curated/analytics.parquet", help="Parquet de saída.")
    parser.add_argument("--delimiter", default=",", help="Delimitador do CSV.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"[convert] arquivo de entrada não encontrado: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    csv_path = str(input_path).replace("'", "''")
    parquet_path = str(output_path).replace("'", "''")
    delim = str(args.delimiter).replace("'", "''")

    con = duckdb.connect(database=":memory:")
    con.execute(
        "COPY ("
        f"SELECT * FROM read_csv_auto('{csv_path}', delim='{delim}', header=true, ignore_errors=true)"
        ") TO "
        f"'{parquet_path}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )

    print(f"[convert] input: {input_path}")
    print(f"[convert] output: {output_path}")


if __name__ == "__main__":
    main()
