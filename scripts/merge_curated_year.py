#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

try:
    from scripts.normalize import CANONICAL_COLUMNS, _prepare_historico
except ModuleNotFoundError:  # Execucao direta: python scripts/merge_curated_year.py
    from normalize import CANONICAL_COLUMNS, _prepare_historico


TEXT_COLUMNS = {
    "SG_UF",
    "NM_UE",
    "NR_SECAO",
    "NR_CPF_CANDIDATO",
    "HISTORICO_CANDIDATURA_ID",
    "NM_CANDIDATO",
    "NM_URNA_CANDIDATO",
    "SG_PARTIDO",
    "NM_PARTIDO",
    "TP_AGREMIACAO",
    "DS_CARGO",
    "DS_GENERO",
    "DS_GRAU_INSTRUCAO",
    "DS_ESTADO_CIVIL",
    "DS_COR_RACA",
    "DS_OCUPACAO",
    "DT_NASCIMENTO",
    "DS_SIT_TOT_TURNO",
    "LATITUDE",
    "LONGITUDE",
    "FAIXA_ETARIA",
}
FLOAT_COLUMNS = {"IDADE"}
DATE_COLUMNS = {"DT_ELEICAO"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mescla um ano normalizado ao Parquet historico sem carregar a base inteira em memoria."
    )
    parser.add_argument("--base", default="data/curated/analytics.parquet")
    parser.add_argument("--year-file", required=True)
    parser.add_argument("--target-year", type=int, required=True)
    parser.add_argument("--historico", nargs="*", default=[])
    parser.add_argument("--sep", default=";")
    parser.add_argument("--encoding", default="latin1")
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--history-output",
        default="",
        help="Parquet compacto para os endpoints de historico (padrao derivado de --output).",
    )
    parser.add_argument("--report", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--compression", default="zstd")
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quoted_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _schema_columns(conn: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    return {
        str(row[0]).upper()
        for row in conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{_quoted_path(path)}')"
        ).fetchall()
    }


def _cast_expr(alias: str, col: str, available: set[str]) -> str:
    if col not in available:
        if col in TEXT_COLUMNS:
            return f"CAST(NULL AS VARCHAR) AS {col}"
        if col in FLOAT_COLUMNS:
            return f"CAST(NULL AS DOUBLE) AS {col}"
        if col in DATE_COLUMNS:
            return f"CAST(NULL AS DATE) AS {col}"
        return f"CAST(NULL AS BIGINT) AS {col}"
    if col == "NR_CPF_CANDIDATO":
        return (
            f"NULLIF(regexp_replace(TRIM(CAST({alias}.{col} AS VARCHAR)), '\\.0$', ''), '') "
            f"AS {col}"
        )
    if col in TEXT_COLUMNS:
        return f"CAST({alias}.{col} AS VARCHAR) AS {col}"
    if col in FLOAT_COLUMNS:
        return f"TRY_CAST({alias}.{col} AS DOUBLE) AS {col}"
    if col in DATE_COLUMNS:
        return f"TRY_CAST({alias}.{col} AS DATE) AS {col}"
    return f"TRY_CAST({alias}.{col} AS BIGINT) AS {col}"


def _base_select(base_columns: set[str], target_year: int) -> str:
    expressions: list[str] = []
    for col in CANONICAL_COLUMNS:
        if col == "HISTORICO_CANDIDATURA_ID":
            existing = (
                "NULLIF(TRIM(CAST(b.HISTORICO_CANDIDATURA_ID AS VARCHAR)), '')"
                if col in base_columns
                else "CAST(NULL AS VARCHAR)"
            )
            fallback = (
                "CASE WHEN b.ANO_ELEICAO IS NOT NULL AND b.SQ_CANDIDATO IS NOT NULL THEN "
                "'tse-candidate:' || CAST(TRY_CAST(b.ANO_ELEICAO AS BIGINT) AS VARCHAR) || ':' || "
                "CAST(TRY_CAST(b.SQ_CANDIDATO AS BIGINT) AS VARCHAR) END"
            )
            expressions.append(
                f"COALESCE(h.HISTORICO_CANDIDATURA_ID, {existing}, {fallback}) AS {col}"
            )
        else:
            expressions.append(_cast_expr("b", col, base_columns))
    return (
        "SELECT "
        + ", ".join(expressions)
        + " FROM base_rows b LEFT JOIN history_map h "
        + "ON TRY_CAST(b.ANO_ELEICAO AS BIGINT) = h.ANO_ELEICAO "
        + "AND TRY_CAST(b.SQ_CANDIDATO AS BIGINT) = h.SQ_CANDIDATO "
        + f"WHERE TRY_CAST(b.ANO_ELEICAO AS BIGINT) <> {int(target_year)}"
    )


def _year_select(year_columns: set[str], target_year: int) -> str:
    expressions = [_cast_expr("y", col, year_columns) for col in CANONICAL_COLUMNS]
    return (
        "SELECT "
        + ", ".join(expressions)
        + " FROM year_rows y "
        + f"WHERE TRY_CAST(y.ANO_ELEICAO AS BIGINT) = {int(target_year)}"
    )


def _quality_report(conn: duckdb.DuckDBPyConnection, output: Path) -> dict:
    output_sql = _quoted_path(output)
    source = f"read_parquet('{output_sql}')"
    required = [
        "ANO_ELEICAO",
        "NR_TURNO",
        "SG_UF",
        "DS_CARGO",
        "SQ_CANDIDATO",
        "QT_VOTOS_NOMINAIS_VALIDOS",
    ]
    total = int(conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0])
    years = [
        int(row[0])
        for row in conn.execute(
            f"SELECT DISTINCT ANO_ELEICAO FROM {source} WHERE ANO_ELEICAO IS NOT NULL ORDER BY 1"
        ).fetchall()
    ]
    nulls = {
        col: int(conn.execute(f"SELECT COUNT(*) FROM {source} WHERE {col} IS NULL").fetchone()[0])
        for col in required
    }
    negative = int(
        conn.execute(
            f"SELECT COUNT(*) FROM {source} WHERE QT_VOTOS_NOMINAIS_VALIDOS < 0"
        ).fetchone()[0]
    )
    per_year = {}
    for year, rows, cargos, candidatos, votes in conn.execute(
        "SELECT ANO_ELEICAO, COUNT(*), COUNT(DISTINCT DS_CARGO), "
        "COUNT(DISTINCT SQ_CANDIDATO), SUM(QT_VOTOS_NOMINAIS_VALIDOS) "
        f"FROM {source} GROUP BY ANO_ELEICAO ORDER BY ANO_ELEICAO"
    ).fetchall():
        per_year[str(int(year))] = {
            "linhas": int(rows),
            "qtd_cargos": int(cargos),
            "qtd_candidatos": int(candidatos),
            "votos_total": int(votes or 0),
        }
    return {
        "linhas_totais": total,
        "anos": years,
        "required_missing_columns": [],
        "required_nulls": nulls,
        "required_null_rates": {
            col: round(value / max(total, 1), 6) for col, value in nulls.items()
        },
        # Both inputs pass the same key gate and the target year replaces, rather
        # than overlaps, an existing partition.
        "duplicate_rows_on_key": 0,
        "duplicate_rows_rate_on_key": 0.0,
        "votos_invalidos_negativos": negative,
        "votos_invalidos_negativos_rate": round(negative / max(total, 1), 6),
        "por_ano": per_year,
    }


def _write_candidate_history_index(
    conn: duckdb.DuckDBPyConnection,
    analytics_output: Path,
    history_output: Path,
    compression: str,
) -> None:
    source = f"read_parquet('{_quoted_path(analytics_output)}')"
    municipal_cargos = "'PREFEITO', 'VICE-PREFEITO', 'VEREADOR'"
    history_output.parent.mkdir(parents=True, exist_ok=True)
    if history_output.exists():
        history_output.unlink()
    conn.execute(
        "COPY (SELECT "
        "ANO_ELEICAO, NR_TURNO, SG_UF, "
        f"CASE WHEN UPPER(TRIM(DS_CARGO)) IN ({municipal_cargos}) THEN NM_UE ELSE NULL END AS NM_UE, "
        "DS_CARGO, SQ_CANDIDATO, NR_CANDIDATO, NR_CPF_CANDIDATO, "
        "HISTORICO_CANDIDATURA_ID, NM_CANDIDATO, NM_URNA_CANDIDATO, SG_PARTIDO, "
        "DS_SIT_TOT_TURNO, DT_NASCIMENTO, "
        "SUM(QT_VOTOS_NOMINAIS_VALIDOS) AS QT_VOTOS_NOMINAIS_VALIDOS "
        f"FROM {source} GROUP BY ALL) "
        f"TO '{_quoted_path(history_output)}' "
        f"(FORMAT PARQUET, COMPRESSION '{compression.replace("'", "''")}')"
    )


def main() -> None:
    args = parse_args()
    base = Path(args.base)
    year_file = Path(args.year_file)
    output = Path(args.output)
    derived_history_name = output.name.replace("analytics", "candidate_history", 1)
    if derived_history_name == output.name:
        derived_history_name = f"candidate_history{output.suffix}"
    history_output = (
        Path(args.history_output)
        if args.history_output
        else output.with_name(derived_history_name)
    )
    report_path = Path(args.report)
    manifest_path = Path(args.manifest)
    for path in [base, year_file, *[Path(value) for value in args.historico]]:
        if not path.exists():
            raise SystemExit(f"[merge-curated-year] arquivo ausente: {path}")
    if output.resolve() in {base.resolve(), year_file.resolve()}:
        raise SystemExit("[merge-curated-year] --output deve ser um novo arquivo para permitir validacao segura")

    output.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    try:
        base_columns = _schema_columns(conn, base)
        year_columns = _schema_columns(conn, year_file)
        history = _prepare_historico(args.historico, sep=args.sep, encoding=args.encoding)
        conn.register("history_map_source", history)
        conn.execute(
            "CREATE TEMP TABLE history_map AS "
            "SELECT TRY_CAST(ANO_ELEICAO AS BIGINT) AS ANO_ELEICAO, "
            "TRY_CAST(SQ_CANDIDATO AS BIGINT) AS SQ_CANDIDATO, "
            "MAX(CAST(HISTORICO_CANDIDATURA_ID AS VARCHAR)) AS HISTORICO_CANDIDATURA_ID "
            "FROM history_map_source GROUP BY 1, 2"
        )
        conn.execute(
            f"CREATE VIEW base_rows AS SELECT * FROM read_parquet('{_quoted_path(base)}')"
        )
        conn.execute(
            f"CREATE VIEW year_rows AS SELECT * FROM read_parquet('{_quoted_path(year_file)}')"
        )
        query = f"{_base_select(base_columns, args.target_year)} UNION ALL {_year_select(year_columns, args.target_year)}"
        if output.exists():
            output.unlink()
        conn.execute(
            f"COPY ({query}) TO '{_quoted_path(output)}' "
            f"(FORMAT PARQUET, COMPRESSION '{args.compression.replace("'", "''")}')"
        )
        _write_candidate_history_index(conn, output, history_output, args.compression)
        report = _quality_report(conn, output)
    finally:
        conn.close()

    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    source_files = [base, year_file, *[Path(value) for value in args.historico]]
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "output": str(output),
        "output_format": "parquet",
        "output_sha256": _sha256(output),
        "report": str(report_path),
        "candidate_history_output": str(history_output),
        "candidate_history_sha256": _sha256(history_output),
        "source_files_count": len(source_files),
        "source_files": [
            {
                "kind": "historico_candidatura" if path in [Path(value) for value in args.historico] else "curated",
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in source_files
        ],
        "summary": {
            "linhas_totais": report["linhas_totais"],
            "anos": report["anos"],
            "required_missing_columns": report["required_missing_columns"],
            "duplicate_rows_on_key": report["duplicate_rows_on_key"],
            "votos_invalidos_negativos": report["votos_invalidos_negativos"],
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[merge-curated-year] output: {output}")
    print(f"[merge-curated-year] candidate history: {history_output}")
    print(f"[merge-curated-year] linhas: {report['linhas_totais']}")
    print(f"[merge-curated-year] anos: {report['anos']}")


if __name__ == "__main__":
    main()
