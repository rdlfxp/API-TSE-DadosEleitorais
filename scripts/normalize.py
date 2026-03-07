#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path

import pandas as pd


CANONICAL_COLUMNS = [
    "ANO_ELEICAO",
    "SG_UF",
    "NM_UE",
    "CD_CARGO",
    "DS_CARGO",
    "SQ_CANDIDATO",
    "NR_CANDIDATO",
    "NM_CANDIDATO",
    "NM_URNA_CANDIDATO",
    "SG_PARTIDO",
    "NM_PARTIDO",
    "TP_AGREMIACAO",
    "DS_GENERO",
    "DS_GRAU_INSTRUCAO",
    "DS_ESTADO_CIVIL",
    "DS_COR_RACA",
    "DS_OCUPACAO",
    "DT_NASCIMENTO",
    "DT_ELEICAO",
    "DS_SIT_TOT_TURNO",
    "QT_VOTOS_NOMINAIS_VALIDOS",
    "IDADE",
    "FAIXA_ETARIA",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normaliza dados eleitorais multi-ano para um unico CSV "
            "de analytics consumido pela API."
        )
    )
    parser.add_argument(
        "--votacao",
        nargs="+",
        default=[],
        help="Um ou mais CSVs de votacao candidato (ex.: votacao_candidato_munzona_2022_SP.csv).",
    )
    parser.add_argument(
        "--consulta",
        nargs="*",
        default=[],
        help="CSVs opcionais de consulta_cand para enriquecer perfil do candidato.",
    )
    parser.add_argument(
        "--raw-dir",
        default=None,
        help=(
            "Diretorio raiz com CSVs brutos por ano (ex.: data/raw). "
            "Quando informado, arquivos de votacao/consulta sao descobertos automaticamente."
        ),
    )
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=[],
        help="Anos especificos para varredura dentro de --raw-dir (ex.: --years 2018 2022).",
    )
    parser.add_argument(
        "--votacao-pattern",
        nargs="+",
        default=["*votacao_candidato*munzona*.csv"],
        help=(
            "Um ou mais padroes glob para descobrir arquivos de votacao em --raw-dir. "
            "Ex.: --votacao-pattern '*votacao_candidato*munzona*.csv' '*votacao_candidato*.csv'"
        ),
    )
    parser.add_argument(
        "--consulta-pattern",
        nargs="+",
        default=["*consulta_cand*.csv"],
        help="Um ou mais padroes glob para descobrir arquivos de consulta em --raw-dir.",
    )
    parser.add_argument(
        "--exclude-pattern",
        nargs="*",
        default=["*.DS_Store", "*classificado*.csv"],
        help=(
            "Padroes glob para excluir arquivos descobertos no --raw-dir. "
            "Ex.: --exclude-pattern '*classificado*.csv' '*_backup*.csv'"
        ),
    )
    parser.add_argument(
        "--output",
        default="data/curated/analytics.csv",
        help="Arquivo final normalizado (.csv padrao, ou .parquet se engine estiver instalada).",
    )
    parser.add_argument(
        "--report",
        default="data/curated/quality_report.json",
        help="Relatorio de qualidade de dados (JSON).",
    )
    parser.add_argument(
        "--manifest",
        default="data/curated/manifest.json",
        help="Manifesto de auditoria da carga (JSON).",
    )
    parser.add_argument(
        "--sep",
        default=";",
        help="Separador dos CSVs de entrada. Padrao TSE: ';'.",
    )
    parser.add_argument(
        "--encoding",
        default="latin1",
        help="Encoding dos CSVs de entrada. Padrao TSE: latin1.",
    )
    parser.add_argument(
        "--quality-gate",
        action="store_true",
        help="Ativa bloqueio por qualidade de dados (encerra com erro se regras nao forem atendidas).",
    )
    parser.add_argument(
        "--allow-missing-required",
        action="store_true",
        help="Permite colunas obrigatorias faltantes mesmo com --quality-gate.",
    )
    parser.add_argument(
        "--max-duplicate-rows",
        type=int,
        default=0,
        help="Maximo de linhas duplicadas permitido na chave de unicidade.",
    )
    parser.add_argument(
        "--max-negative-votes",
        type=int,
        default=0,
        help="Maximo de linhas com votos negativos permitido.",
    )
    parser.add_argument(
        "--max-required-null-rate",
        type=float,
        default=0.0,
        help=(
            "Taxa maxima de nulos permitida para cada coluna obrigatoria "
            "(0.0 a 1.0). Ex.: 0.02 = ate 2%%."
        ),
    )
    return parser.parse_args()


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _find_col(df: pd.DataFrame, options: list[str]) -> str | None:
    for col in options:
        if col in df.columns:
            return col
    return None


def _extract_year_from_path(file_path: str) -> int | None:
    m = re.search(r"(20\d{2})", Path(file_path).name)
    return int(m.group(1)) if m else None


def _parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _discover_files(
    raw_dir: str,
    years: list[int],
    patterns: list[str],
    exclude_patterns: list[str],
) -> list[str]:
    root = Path(raw_dir)
    if not root.exists():
        return []

    candidates: set[Path] = set()
    if years:
        for year in years:
            year_dir = root / str(year)
            if year_dir.exists():
                for pattern in patterns:
                    candidates.update(year_dir.rglob(pattern))
    else:
        for pattern in patterns:
            candidates.update(root.rglob(pattern))

    def is_excluded(path: Path) -> bool:
        path_str = str(path)
        return any(fnmatch(path.name, pat) or fnmatch(path_str, pat) for pat in exclude_patterns)

    files = [str(p) for p in candidates if p.is_file() and not is_excluded(p)]
    return sorted(files)


def _merge_unique_paths(*path_lists: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for items in path_lists:
        for item in items:
            if item not in seen:
                merged.append(item)
                seen.add(item)
    return merged


def _classify_age(age: float) -> str:
    if pd.isna(age):
        return "N/A"
    if age <= 29:
        return "18-29"
    if age <= 39:
        return "30-39"
    if age <= 49:
        return "40-49"
    if age <= 59:
        return "50-59"
    if age <= 69:
        return "60-69"
    return "70+"


def _compute_age(df: pd.DataFrame) -> pd.Series:
    if "IDADE" in df.columns:
        return pd.to_numeric(df["IDADE"], errors="coerce")

    if "DT_NASCIMENTO" not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")

    nasc = _parse_date(df["DT_NASCIMENTO"])
    if "DT_ELEICAO" in df.columns:
        eleicao = _parse_date(df["DT_ELEICAO"])
        return ((eleicao - nasc).dt.days / 365.25).round(0)

    if "ANO_ELEICAO" in df.columns:
        ano = pd.to_numeric(df["ANO_ELEICAO"], errors="coerce")
        return (ano - nasc.dt.year).round(0)

    return pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")


def _prepare_consulta(paths: list[str], sep: str, encoding: str) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for p in paths:
        df = pd.read_csv(p, sep=sep, encoding=encoding, low_memory=False)
        df = _clean_columns(df)
        year_col = _find_col(df, ["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        if year_col is None:
            year_guess = _extract_year_from_path(p)
            if year_guess is not None:
                df["ANO_ELEICAO"] = year_guess
        elif year_col != "ANO_ELEICAO":
            df = df.rename(columns={year_col: "ANO_ELEICAO"})

        keep = [
            c
            for c in [
                "ANO_ELEICAO",
                "SQ_CANDIDATO",
                "NR_CANDIDATO",
                "SG_UF",
                "DS_CARGO",
                "DS_GENERO",
                "DS_GRAU_INSTRUCAO",
                "DS_ESTADO_CIVIL",
                "DS_COR_RACA",
                "DS_OCUPACAO",
                "DT_NASCIMENTO",
                "NM_CANDIDATO",
                "NM_URNA_CANDIDATO",
            ]
            if c in df.columns
        ]
        if not keep:
            continue

        frames.append(df[keep].copy())

    if not frames:
        return pd.DataFrame()

    consulta = pd.concat(frames, ignore_index=True)
    key_cols = [c for c in ["ANO_ELEICAO", "SQ_CANDIDATO", "NR_CANDIDATO", "SG_UF", "DS_CARGO"] if c in consulta.columns]
    if key_cols:
        consulta = consulta.drop_duplicates(subset=key_cols, keep="last")
    return consulta


def _normalize_votacao_file(
    file_path: str,
    consulta_df: pd.DataFrame,
    sep: str,
    encoding: str,
) -> pd.DataFrame:
    df = pd.read_csv(file_path, sep=sep, encoding=encoding, low_memory=False)
    df = _clean_columns(df).copy()

    rename_map: dict[str, str] = {}
    col_ano = _find_col(df, ["ANO_ELEICAO", "NR_ANO_ELEICAO"])
    if col_ano and col_ano != "ANO_ELEICAO":
        rename_map[col_ano] = "ANO_ELEICAO"
    col_votos = _find_col(df, ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL"])
    if col_votos and col_votos != "QT_VOTOS_NOMINAIS_VALIDOS":
        rename_map[col_votos] = "QT_VOTOS_NOMINAIS_VALIDOS"
    if rename_map:
        df = df.rename(columns=rename_map).copy()

    if "ANO_ELEICAO" not in df.columns:
        year_guess = _extract_year_from_path(file_path)
        if year_guess is not None:
            df.loc[:, "ANO_ELEICAO"] = year_guess

    if not consulta_df.empty:
        join_keys = [c for c in ["ANO_ELEICAO", "SQ_CANDIDATO"] if c in df.columns and c in consulta_df.columns]
        if not join_keys:
            join_keys = [c for c in ["ANO_ELEICAO", "NR_CANDIDATO", "SG_UF", "DS_CARGO"] if c in df.columns and c in consulta_df.columns]
        if join_keys:
            profile_cols = [
                c
                for c in [
                    "DS_GENERO",
                    "DS_GRAU_INSTRUCAO",
                    "DS_ESTADO_CIVIL",
                    "DS_COR_RACA",
                    "DS_OCUPACAO",
                    "DT_NASCIMENTO",
                    "NM_CANDIDATO",
                    "NM_URNA_CANDIDATO",
                ]
                if c in consulta_df.columns
            ]
            if profile_cols:
                df = df.merge(
                    consulta_df[join_keys + profile_cols],
                    on=join_keys,
                    how="left",
                    suffixes=("", "_CONS"),
                ).copy()
                for col in profile_cols:
                    alt = f"{col}_CONS"
                    if alt in df.columns:
                        if col in df.columns:
                            df.loc[:, col] = df[col].fillna(df[alt])
                            df = df.drop(columns=[alt])
                        else:
                            df = df.rename(columns={alt: col})

    if "QT_VOTOS_NOMINAIS_VALIDOS" not in df.columns:
        df.loc[:, "QT_VOTOS_NOMINAIS_VALIDOS"] = 0
    df.loc[:, "QT_VOTOS_NOMINAIS_VALIDOS"] = (
        pd.to_numeric(df["QT_VOTOS_NOMINAIS_VALIDOS"], errors="coerce").fillna(0).astype("int64")
    )

    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df.loc[:, col] = pd.NA

    df.loc[:, "IDADE"] = _compute_age(df)
    df.loc[:, "FAIXA_ETARIA"] = df["IDADE"].apply(_classify_age)

    group_cols = [
        "ANO_ELEICAO",
        "SG_UF",
        "NM_UE",
        "CD_CARGO",
        "DS_CARGO",
        "SQ_CANDIDATO",
        "NR_CANDIDATO",
        "NM_CANDIDATO",
        "NM_URNA_CANDIDATO",
        "SG_PARTIDO",
        "NM_PARTIDO",
        "TP_AGREMIACAO",
        "DS_GENERO",
        "DS_GRAU_INSTRUCAO",
        "DS_ESTADO_CIVIL",
        "DS_COR_RACA",
        "DS_OCUPACAO",
        "DT_NASCIMENTO",
        "DT_ELEICAO",
        "DS_SIT_TOT_TURNO",
    ]
    agg = (
        df[group_cols + ["QT_VOTOS_NOMINAIS_VALIDOS"]]
        .groupby(group_cols, dropna=False, as_index=False)["QT_VOTOS_NOMINAIS_VALIDOS"]
        .sum()
        .copy()
    )

    agg.loc[:, "IDADE"] = _compute_age(agg)
    agg.loc[:, "FAIXA_ETARIA"] = agg["IDADE"].apply(_classify_age)
    return agg[CANONICAL_COLUMNS].copy()


def _quality_report(df: pd.DataFrame) -> dict:
    required = [
        "ANO_ELEICAO",
        "SG_UF",
        "DS_CARGO",
        "NM_CANDIDATO",
        "QT_VOTOS_NOMINAIS_VALIDOS",
        "DS_SIT_TOT_TURNO",
    ]
    required_missing = [c for c in required if c not in df.columns]

    duplicates_key = ["ANO_ELEICAO", "SQ_CANDIDATO", "DS_CARGO", "SG_UF"]
    duplicates_key = [c for c in duplicates_key if c in df.columns]
    duplicate_rows = 0
    if duplicates_key:
        duplicate_rows = int(df.duplicated(subset=duplicates_key, keep=False).sum())

    nulls = {}
    for c in required:
        if c in df.columns:
            nulls[c] = int(df[c].isna().sum())

    votos_invalidos = 0
    if "QT_VOTOS_NOMINAIS_VALIDOS" in df.columns:
        votos = pd.to_numeric(df["QT_VOTOS_NOMINAIS_VALIDOS"], errors="coerce")
        votos_invalidos = int((votos < 0).fillna(False).sum())

    report_years: dict[str, dict] = {}
    if "ANO_ELEICAO" in df.columns:
        year_vals = pd.to_numeric(df["ANO_ELEICAO"], errors="coerce")
        unique_years = sorted(year_vals.dropna().astype(int).unique().tolist())
        for year in unique_years:
            chunk = df[year_vals == year]
            report_years[str(year)] = {
                "linhas": int(len(chunk)),
                "ufs": sorted(chunk["SG_UF"].dropna().astype(str).str.upper().unique().tolist())
                if "SG_UF" in chunk.columns
                else [],
                "qtd_cargos": int(chunk["DS_CARGO"].dropna().nunique()) if "DS_CARGO" in chunk.columns else 0,
                "qtd_candidatos": int(chunk["SQ_CANDIDATO"].dropna().nunique()) if "SQ_CANDIDATO" in chunk.columns else 0,
                "votos_total": int(
                    pd.to_numeric(chunk["QT_VOTOS_NOMINAIS_VALIDOS"], errors="coerce").fillna(0).sum()
                )
                if "QT_VOTOS_NOMINAIS_VALIDOS" in chunk.columns
                else 0,
            }

    total_rows = max(len(df), 1)
    null_rates = {k: round(v / total_rows, 6) for k, v in nulls.items()}

    return {
        "linhas_totais": int(len(df)),
        "anos": sorted(pd.to_numeric(df["ANO_ELEICAO"], errors="coerce").dropna().astype(int).unique().tolist())
        if "ANO_ELEICAO" in df.columns
        else [],
        "required_missing_columns": required_missing,
        "required_nulls": nulls,
        "required_null_rates": null_rates,
        "duplicate_rows_on_key": duplicate_rows,
        "duplicate_rows_rate_on_key": round(duplicate_rows / total_rows, 6),
        "votos_invalidos_negativos": votos_invalidos,
        "votos_invalidos_negativos_rate": round(votos_invalidos / total_rows, 6),
        "por_ano": report_years,
    }


def _evaluate_quality_gate(report: dict, args: argparse.Namespace) -> list[str]:
    failures: list[str] = []

    missing_required = report.get("required_missing_columns", [])
    if missing_required and not args.allow_missing_required:
        failures.append(f"colunas obrigatorias faltantes: {missing_required}")

    dup_rows = int(report.get("duplicate_rows_on_key", 0))
    if dup_rows > args.max_duplicate_rows:
        failures.append(
            f"duplicidades na chave ({dup_rows}) acima do limite ({args.max_duplicate_rows})"
        )

    neg_votes = int(report.get("votos_invalidos_negativos", 0))
    if neg_votes > args.max_negative_votes:
        failures.append(
            f"votos negativos ({neg_votes}) acima do limite ({args.max_negative_votes})"
        )

    null_rates = report.get("required_null_rates", {})
    for col, rate in null_rates.items():
        if float(rate) > args.max_required_null_rate:
            failures.append(
                f"nulos em {col} = {rate:.4f} acima do limite ({args.max_required_null_rate:.4f})"
            )

    return failures


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_manifest(
    votacao_files: list[str],
    consulta_files: list[str],
    output_path: Path,
    report_path: Path,
    report: dict,
) -> dict:
    source_files: list[dict] = []
    for p in votacao_files:
        path = Path(p)
        if path.exists():
            source_files.append(
                {
                    "kind": "votacao",
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
    for p in consulta_files:
        path = Path(p)
        if path.exists():
            source_files.append(
                {
                    "kind": "consulta",
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )

    output_hash = _sha256(output_path) if output_path.exists() else None
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "output": str(output_path),
        "output_format": output_path.suffix.lower().lstrip("."),
        "output_sha256": output_hash,
        "report": str(report_path),
        "source_files_count": len(source_files),
        "source_files": source_files,
        "summary": {
            "linhas_totais": report.get("linhas_totais", 0),
            "anos": report.get("anos", []),
            "required_missing_columns": report.get("required_missing_columns", []),
            "duplicate_rows_on_key": report.get("duplicate_rows_on_key", 0),
            "votos_invalidos_negativos": report.get("votos_invalidos_negativos", 0),
        },
    }


def _write_output(df: pd.DataFrame, output_path: Path) -> None:
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        try:
            df.to_parquet(output_path, index=False)
        except ImportError as exc:
            raise RuntimeError(
                "Para gerar parquet, instale uma engine suportada (ex.: pyarrow)."
            ) from exc
        return
    if suffix == ".csv":
        df.to_csv(output_path, index=False, encoding="utf-8")
        return
    raise ValueError("Formato de saida nao suportado. Use extensao .parquet ou .csv.")


def main() -> None:
    args = parse_args()

    discovered_votacao: list[str] = []
    discovered_consulta: list[str] = []
    if args.raw_dir:
        discovered_votacao = _discover_files(
            raw_dir=args.raw_dir,
            years=args.years,
            patterns=args.votacao_pattern,
            exclude_patterns=args.exclude_pattern,
        )
        discovered_consulta = _discover_files(
            raw_dir=args.raw_dir,
            years=args.years,
            patterns=args.consulta_pattern,
            exclude_patterns=args.exclude_pattern,
        )
        print(
            f"[normalize] descobertos em raw-dir: "
            f"{len(discovered_votacao)} votacao, {len(discovered_consulta)} consulta"
        )

    votacao_files = _merge_unique_paths(args.votacao, discovered_votacao)
    consulta_files = _merge_unique_paths(args.consulta, discovered_consulta)

    if not votacao_files:
        print("[normalize] erro: nenhum arquivo de votacao informado/encontrado.")
        print(
            "[normalize] use --votacao ... ou --raw-dir data/raw "
            "[--years 2014 2018 2022]"
        )
        raise SystemExit(2)

    consulta = _prepare_consulta(consulta_files, sep=args.sep, encoding=args.encoding)
    normalized_frames: list[pd.DataFrame] = []

    for vot_file in votacao_files:
        print(f"[normalize] lendo votacao: {vot_file}")
        normalized_frames.append(
            _normalize_votacao_file(
                file_path=vot_file,
                consulta_df=consulta,
                sep=args.sep,
                encoding=args.encoding,
            )
        )

    final_df = pd.concat(normalized_frames, ignore_index=True)
    final_df = final_df.drop_duplicates(
        subset=["ANO_ELEICAO", "SQ_CANDIDATO", "NR_CANDIDATO", "DS_CARGO", "SG_UF"],
        keep="last",
    ).copy()
    final_df.loc[:, "ANO_ELEICAO"] = pd.to_numeric(final_df["ANO_ELEICAO"], errors="coerce").astype("Int64")

    report = _quality_report(final_df)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.quality_gate:
        failures = _evaluate_quality_gate(report, args)
        if failures:
            print("[normalize] quality gate: REPROVADO")
            for msg in failures:
                print(f"[normalize] - {msg}")
            print(f"[normalize] report: {report_path}")
            raise SystemExit(2)
        print("[normalize] quality gate: APROVADO")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_output(final_df, output_path)

    manifest = _build_manifest(
        votacao_files=votacao_files,
        consulta_files=consulta_files,
        output_path=output_path,
        report_path=report_path,
        report=report,
    )
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    anos = sorted(final_df["ANO_ELEICAO"].dropna().astype(int).unique().tolist())
    cargos = sorted(final_df["DS_CARGO"].dropna().astype(str).unique().tolist())
    print("[normalize] concluido")
    print(f"[normalize] output: {output_path}")
    print(f"[normalize] report: {report_path}")
    print(f"[normalize] manifest: {manifest_path}")
    print(f"[normalize] linhas: {len(final_df)}")
    print(f"[normalize] arquivos votacao: {len(votacao_files)}")
    print(f"[normalize] arquivos consulta: {len(consulta_files)}")
    print(f"[normalize] anos: {anos}")
    print(f"[normalize] qtd cargos: {len(cargos)}")


if __name__ == "__main__":
    main()
