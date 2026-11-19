#!/usr/bin/env python3
"""Gera data/curated/electorate_profile.parquet a partir do `perfil_eleitorado_<ano>.zip` do TSE.

Uma linha por (ano, UF, municipio, zona) com contagens de eleitores por genero, faixa etaria e
escolaridade. A API usa isso para o perfil do ELEITORADO onde o candidato teve votos (e nao o perfil do
proprio candidato).

    python scripts/build_electorate_profile.py --years 2018 2020 2022 2024 2026
    python scripts/build_electorate_profile.py --years 2022 --source-dir /caminho/com/zips

Os zips (30 a 400 MB) sao baixados para --cache-dir (padrao: data/raw/perfil_eleitorado) e lidos sem extrair.
"""
from __future__ import annotations

import argparse
import re
import unicodedata
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/perfil_eleitorado/perfil_eleitorado_{year}.zip"

EDUCATION_COLUMNS = {
    "ANALFABETO": "edu_analfabeto",
    "LE E ESCREVE": "edu_le_escreve",
    "ENSINO FUNDAMENTAL INCOMPLETO": "edu_fund_incompleto",
    "ENSINO FUNDAMENTAL COMPLETO": "edu_fund_completo",
    "ENSINO MEDIO INCOMPLETO": "edu_medio_incompleto",
    "ENSINO MEDIO COMPLETO": "edu_medio_completo",
    "SUPERIOR INCOMPLETO": "edu_superior_incompleto",
    "SUPERIOR COMPLETO": "edu_superior_completo",
}
EDU_UNKNOWN = "edu_nao_informado"
VALUE_COLUMNS = [
    "eleitores", "masculino", "feminino", "genero_outros",
    "idade_16_17", "idade_18_34", "idade_35_59", "idade_60_mais", "idade_invalida",
    *EDUCATION_COLUMNS.values(), EDU_UNKNOWN,
]
KEY_COLUMNS = ["ano", "uf", "cd_municipio", "nr_zona"]


def _ascii_upper(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().upper())
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _pick(columns: list[str], options: list[str]) -> str | None:
    return next((c for c in options if c in columns), None)


def _age_band(label: str) -> str:
    match = re.search(r"\d+", str(label or ""))
    if not match:
        return "idade_invalida"
    age = int(match.group())
    if age < 18:
        return "idade_16_17"
    if age <= 34:
        return "idade_18_34"
    if age <= 59:
        return "idade_35_59"
    return "idade_60_mais"


def aggregate_frame(raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """Agrega linhas do perfil (qualquer combinacao de atributos) para ano/UF/municipio/zona."""
    cols = list(raw.columns)
    col_uf = _pick(cols, ["SG_UF"])
    col_mun = _pick(cols, ["CD_MUNICIPIO", "COD_MUNICIPIO"])
    col_zone = _pick(cols, ["NR_ZONA", "CD_ZONA"])
    col_qty = _pick(cols, ["QT_ELEITORES_PERFIL", "QT_ELEITORES"])
    col_gender = _pick(cols, ["DS_GENERO"])
    col_age = _pick(cols, ["DS_FAIXA_ETARIA"])
    col_edu = _pick(cols, ["DS_GRAU_ESCOLARIDADE", "DS_GRAU_INSTRUCAO"])
    if not (col_uf and col_mun and col_zone and col_qty):
        raise ValueError(f"colunas obrigatorias ausentes no perfil de {year}: {cols}")

    qty = pd.to_numeric(raw[col_qty], errors="coerce").fillna(0).astype("int64")
    gender = raw[col_gender].map(_ascii_upper) if col_gender else pd.Series("", index=raw.index)
    ages = raw[col_age].map(_age_band) if col_age else pd.Series("idade_invalida", index=raw.index)
    edu = raw[col_edu].map(_ascii_upper) if col_edu else pd.Series("", index=raw.index)

    frame = pd.DataFrame(
        {
            "ano": year,
            "uf": raw[col_uf].astype(str).str.strip().str.upper(),
            "cd_municipio": pd.to_numeric(raw[col_mun], errors="coerce").fillna(-1).astype("int64").map(
                lambda v: f"{v:05d}" if v >= 0 else ""
            ),
            "nr_zona": pd.to_numeric(raw[col_zone], errors="coerce").fillna(-1).astype("int64"),
            "eleitores": qty,
            "masculino": qty.where(gender.str.startswith("MASC"), 0),
            "feminino": qty.where(gender.str.startswith("FEM"), 0),
        }
    )
    frame["genero_outros"] = frame["eleitores"] - frame["masculino"] - frame["feminino"]
    for band in ("idade_16_17", "idade_18_34", "idade_35_59", "idade_60_mais", "idade_invalida"):
        frame[band] = qty.where(ages == band, 0)
    known = set(EDUCATION_COLUMNS)
    for label, column in EDUCATION_COLUMNS.items():
        frame[column] = qty.where(edu == label, 0)
    frame[EDU_UNKNOWN] = qty.where(~edu.isin(known), 0)
    return frame.groupby(KEY_COLUMNS, as_index=False)[VALUE_COLUMNS].sum()


CHUNK_ROWS = 1_000_000


def _read_csv_chunks(handle, usecols: list[str] | None):
    """Le em blocos: os CSVs de 2024/2026 passam de 1 GB por UF grande."""
    return pd.read_csv(handle, sep=";", encoding="latin1", dtype=str, usecols=usecols, chunksize=CHUNK_ROWS)


def _wanted_columns(header: list[str]) -> list[str]:
    options = [
        ["SG_UF"], ["CD_MUNICIPIO", "COD_MUNICIPIO"], ["NR_ZONA", "CD_ZONA"],
        ["QT_ELEITORES_PERFIL", "QT_ELEITORES"], ["DS_GENERO"], ["DS_FAIXA_ETARIA"],
        ["DS_GRAU_ESCOLARIDADE", "DS_GRAU_INSTRUCAO"],
    ]
    return [col for opt in options if (col := _pick(header, opt))]


def build_year(zip_path: Path, year: int) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        by_state = [n for n in members if re.search(r"_[A-Z]{2}\.csv$", n) and not n.endswith("_BRASIL.csv")]
        chosen = by_state or [n for n in members if n.endswith("_BRASIL.csv")] or members
        parts = []
        for name in sorted(chosen):
            with archive.open(name) as handle:
                header = handle.readline().decode("latin1").strip().replace('"', "").split(";")
            with archive.open(name) as handle:
                chunks = [aggregate_frame(chunk, year) for chunk in _read_csv_chunks(handle, _wanted_columns(header))]
            merged = pd.concat(chunks, ignore_index=True).groupby(KEY_COLUMNS, as_index=False)[VALUE_COLUMNS].sum()
            parts.append(merged)
            print(f"[perfil] {year} {name}: {len(merged)} linhas agregadas")
    result = pd.concat(parts, ignore_index=True).groupby(KEY_COLUMNS, as_index=False)[VALUE_COLUMNS].sum()
    return result


def _download(url: str, destination: Path, attempts: int = 4, timeout: int = 60) -> None:
    """Baixa para `.part` (com timeout e novas tentativas) e so entao renomeia: um download
    interrompido nunca deixa um arquivo parcial com cara de zip completo."""
    partial = destination.with_name(destination.name + ".part")
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response, partial.open("wb") as out:  # noqa: S310
                expected = int(response.headers.get("Content-Length") or 0)
                while chunk := response.read(1024 * 1024):
                    out.write(chunk)
            if expected and partial.stat().st_size != expected:
                raise OSError(f"download incompleto: {partial.stat().st_size} de {expected} bytes")
            partial.replace(destination)
            return
        except (OSError, TimeoutError) as exc:
            last_error = exc
            print(f"[perfil] tentativa {attempt}/{attempts} falhou: {exc}", flush=True)
    raise SystemExit(f"[perfil] nao foi possivel baixar {url}: {last_error}")


def ensure_zip(year: int, cache_dir: Path) -> Path:
    path = cache_dir / f"perfil_eleitorado_{year}.zip"
    if path.exists() and not zipfile.is_zipfile(path):
        print(f"[perfil] {path.name} incompleto; baixando de novo", flush=True)
        path.unlink()
    if not path.exists():
        cache_dir.mkdir(parents=True, exist_ok=True)
        url = BASE_URL.format(year=year)
        print(f"[perfil] baixando {url}", flush=True)
        _download(url, path)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--years", type=int, nargs="+", required=True)
    parser.add_argument("--source-dir", "--cache-dir", dest="cache_dir", default="data/raw/perfil_eleitorado")
    parser.add_argument("--output", default="data/curated/electorate_profile.parquet")
    parser.add_argument("--merge", action="store_true", help="Mantem os anos ja existentes no arquivo de saida.")
    args = parser.parse_args()

    frames = [build_year(ensure_zip(year, Path(args.cache_dir)), year) for year in args.years]
    result = pd.concat(frames, ignore_index=True)
    output = Path(args.output)
    if args.merge and output.exists():
        previous = pd.read_parquet(output)
        result = pd.concat([previous[~previous["ano"].isin(args.years)], result], ignore_index=True)
    result = result.sort_values(KEY_COLUMNS).reset_index(drop=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(output, index=False, compression="zstd")
    totals = result.groupby("ano")["eleitores"].sum().to_dict()
    print(f"[perfil] {len(result)} linhas -> {output} ({output.stat().st_size // 1024} KB)")
    print(f"[perfil] eleitores por ano: {totals}")


if __name__ == "__main__":
    main()
