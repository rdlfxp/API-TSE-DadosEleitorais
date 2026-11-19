#!/usr/bin/env python3
"""Liga candidaturas sem historico oficial do TSE a identidade oficial da mesma pessoa.

O `person_id` da API nasce de `HISTORICO_CANDIDATURA_ID`. O TSE so publica o historico
oficial (`tse-history:...`) para quem esta na base de 2026; todas as demais linhas recebem
o id sintetico `tse-candidate:ANO:SQ`, que vale por candidatura e por ano. Resultado: a
mesma pessoa (ex.: Lula em 2002) vira outro `person_id` e some do `vote-history`.

Aqui o id sintetico e trocado pelo id oficial quando a ligacao e inequivoca:

1. nome normalizado (sem acento) + CPF valido iguais aos de uma pessoa com id oficial
   (CPFs mascarados como `-4` ou com poucos digitos nunca ligam);
2. senao, nome normalizado + data de nascimento iguais.

O nome sempre precisa bater: em 2006/2008 o SQ_CANDIDATO se repete entre UFs e CPF/nascimento
de outra pessoa foram colados em algumas linhas, entao esses campos sozinhos nao identificam.

Se o par casar com mais de um id oficial, a linha fica como esta (ambiguo nao liga).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

OFFICIAL_PREFIX = "tse-history:"
LEGACY_PREFIX = "tse-candidate:"

_CPF_DIGITS = (
    "regexp_replace(regexp_replace(TRIM(CAST(NR_CPF_CANDIDATO AS VARCHAR)), '\\.0$', ''), "
    "'[^0-9]', '', 'g')"
)
# 9+ digitos (aceita ate 2 zeros a esquerda perdidos) e nao repetido (000..., 111...).
_CPF_EXPR = (
    f"CASE WHEN length({_CPF_DIGITS}) BETWEEN 9 AND 11 "
    f"AND length(replace({_CPF_DIGITS}, substr({_CPF_DIGITS}, 1, 1), '')) > 0 "
    f"THEN LPAD({_CPF_DIGITS}, 11, '0') END"
)
_NAME_EXPR = (
    "NULLIF(regexp_replace(UPPER(strip_accents(TRIM(CAST(NM_CANDIDATO AS VARCHAR)))), "
    "'\\s+', ' ', 'g'), '')"
)
_DOB_TEXT = "substr(TRIM(CAST(DT_NASCIMENTO AS VARCHAR)), 1, 10)"
_DOB_EXPR = (
    f"CAST(COALESCE(try_strptime({_DOB_TEXT}, '%d/%m/%Y'), "
    f"try_strptime({_DOB_TEXT}, '%Y-%m-%d')) AS DATE)"
)
_HID_EXPR = "NULLIF(TRIM(CAST(HISTORICO_CANDIDATURA_ID AS VARCHAR)), '')"


def _quoted(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def link_legacy_identities(
    conn: duckdb.DuckDBPyConnection,
    source: Path,
    destination: Path,
    compression: str = "zstd",
) -> dict[str, int]:
    """Escreve `destination` com os ids sinteticos trocados pelo oficial quando inequivoco."""
    src = f"read_parquet('{_quoted(source)}')"
    conn.execute("SET preserve_insertion_order = false")
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _person_keys AS "
        f"SELECT DISTINCT {_HID_EXPR} AS hid, {_CPF_EXPR} AS cpf, "
        f"{_NAME_EXPR} AS nm, {_DOB_EXPR} AS dob FROM {src}"
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _official AS "
        f"SELECT * FROM _person_keys WHERE hid LIKE '{OFFICIAL_PREFIX}%'"
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _legacy AS "
        "SELECT * FROM _person_keys "
        f"WHERE hid IS NULL OR hid LIKE '{LEGACY_PREFIX}%' OR hid NOT LIKE '{OFFICIAL_PREFIX}%'"
    )
    # Nome (sem acento) igual E, alem dele, CPF valido OU nascimento iguais. CPF e nascimento
    # sozinhos nao bastam: em 2006/2008 o SQ_CANDIDATO se repete entre UFs e o join da
    # normalizacao colou CPF/nascimento de outra pessoa em algumas linhas.
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _link_cpf AS "
        "SELECT l.hid, l.cpf, l.nm, l.dob, MIN(o.hid) AS new_hid, COUNT(DISTINCT o.hid) AS n "
        "FROM _legacy l JOIN _official o ON l.nm = o.nm AND l.cpf = o.cpf "
        "WHERE l.cpf IS NOT NULL AND l.nm IS NOT NULL GROUP BY l.hid, l.cpf, l.nm, l.dob"
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _link_nb AS "
        "SELECT l.hid, l.cpf, l.nm, l.dob, MIN(o.hid) AS new_hid, COUNT(DISTINCT o.hid) AS n "
        "FROM _legacy l JOIN _official o ON l.nm = o.nm AND l.dob = o.dob "
        "WHERE l.nm IS NOT NULL AND l.dob IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM _link_cpf c WHERE c.n = 1 AND c.hid IS NOT DISTINCT FROM l.hid "
        "AND c.cpf IS NOT DISTINCT FROM l.cpf AND c.nm IS NOT DISTINCT FROM l.nm "
        "AND c.dob IS NOT DISTINCT FROM l.dob) "
        "GROUP BY l.hid, l.cpf, l.nm, l.dob"
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _link_final AS "
        "SELECT hid, cpf, nm, dob, new_hid FROM _link_cpf WHERE n = 1 "
        "UNION ALL SELECT hid, cpf, nm, dob, new_hid FROM _link_nb WHERE n = 1"
    )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE _link_final_unique AS "
        "SELECT hid, cpf, nm, dob, MIN(new_hid) AS new_hid FROM _link_final GROUP BY ALL"
    )
    stats = {
        "chaves_sinteticas": conn.execute("SELECT COUNT(*) FROM _legacy").fetchone()[0],
        "ligadas_por_cpf": conn.execute("SELECT COUNT(*) FROM _link_cpf WHERE n = 1").fetchone()[0],
        "ligadas_por_nome_nascimento": conn.execute("SELECT COUNT(*) FROM _link_nb WHERE n = 1").fetchone()[0],
        "ambiguas_ignoradas": conn.execute(
            "SELECT (SELECT COUNT(*) FROM _link_cpf WHERE n > 1) + (SELECT COUNT(*) FROM _link_nb WHERE n > 1)"
        ).fetchone()[0],
    }
    if destination.exists():
        destination.unlink()
    conn.execute(
        "COPY (SELECT t.* EXCLUDE (_k_hid, _k_cpf, _k_nm, _k_dob) "
        "REPLACE (COALESCE(m.new_hid, t.HISTORICO_CANDIDATURA_ID) AS HISTORICO_CANDIDATURA_ID) "
        f"FROM (SELECT *, {_HID_EXPR} AS _k_hid, {_CPF_EXPR} AS _k_cpf, "
        f"{_NAME_EXPR} AS _k_nm, {_DOB_EXPR} AS _k_dob FROM {src}) t "
        "LEFT JOIN _link_final_unique m ON m.hid IS NOT DISTINCT FROM t._k_hid "
        "AND m.cpf IS NOT DISTINCT FROM t._k_cpf AND m.nm IS NOT DISTINCT FROM t._k_nm "
        "AND m.dob IS NOT DISTINCT FROM t._k_dob) "
        f"TO '{_quoted(destination)}' (FORMAT PARQUET, COMPRESSION '{compression.replace(chr(39), chr(39) * 2)}')"
    )
    return stats


def link_in_place(parquet: Path, compression: str = "zstd") -> dict[str, int]:
    """Aplica a ligacao sobre o proprio arquivo (via arquivo temporario ao lado)."""
    tmp = parquet.with_name(parquet.name + ".linked.tmp")
    conn = duckdb.connect()
    try:
        stats = link_legacy_identities(conn, parquet, tmp, compression)
    finally:
        conn.close()
    os.replace(tmp, parquet)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, help="Parquet analytics de entrada.")
    parser.add_argument("--output", required=True, help="Parquet de saida (arquivo novo).")
    parser.add_argument(
        "--history-output",
        default="",
        help="Indice candidate_history de saida (padrao derivado de --output).",
    )
    parser.add_argument("--compression", default="zstd")
    args = parser.parse_args()
    source, destination = Path(args.input), Path(args.output)
    if not source.exists():
        raise SystemExit(f"[link-identities] arquivo ausente: {source}")
    if source.resolve() == destination.resolve():
        raise SystemExit("[link-identities] --output deve ser um arquivo novo")
    conn = duckdb.connect()
    try:
        stats = link_legacy_identities(conn, source, destination, args.compression)
    finally:
        conn.close()
    for key, value in stats.items():
        print(f"[link-identities] {key}: {value}")
    try:
        from scripts.merge_curated_year import _write_candidate_history_index
    except ModuleNotFoundError:  # Execucao direta: python scripts/link_person_identities.py
        from merge_curated_year import _write_candidate_history_index

    history_name = destination.name.replace("analytics", "candidate_history", 1)
    if history_name == destination.name:
        history_name = f"candidate_history{destination.suffix}"
    history_output = Path(args.history_output) if args.history_output else destination.with_name(history_name)
    conn = duckdb.connect()
    try:
        _write_candidate_history_index(conn, destination, history_output, args.compression)
    finally:
        conn.close()
    print(f"[link-identities] output: {destination}")
    print(f"[link-identities] candidate history: {history_output}")


if __name__ == "__main__":
    main()
