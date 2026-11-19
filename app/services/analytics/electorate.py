"""Perfil do ELEITORADO onde o candidato teve votos.

Antes a rota devolvia o perfil do proprio candidato (genero, idade e instrucao de quem concorreu),
ponderado pelos votos: uma candidata dava `female_share: 100`. Aqui o perfil e o do eleitorado dos
municipios (ou da UF, quando o cargo nao tem detalhe municipal) ponderado pelos votos do candidato.

A fonte e `electorate_profile.parquet` (scripts/build_electorate_profile.py, do perfil do eleitorado do TSE).
"""
from __future__ import annotations

import pandas as pd

from app.domain.municipios import lookup_municipio, normalize_tse_code

EDUCATION_LABELS = {
    "edu_analfabeto": "ANALFABETO",
    "edu_le_escreve": "LÊ E ESCREVE",
    "edu_fund_incompleto": "ENSINO FUNDAMENTAL INCOMPLETO",
    "edu_fund_completo": "ENSINO FUNDAMENTAL COMPLETO",
    "edu_medio_incompleto": "ENSINO MÉDIO INCOMPLETO",
    "edu_medio_completo": "ENSINO MÉDIO COMPLETO",
    "edu_superior_incompleto": "SUPERIOR INCOMPLETO",
    "edu_superior_completo": "SUPERIOR COMPLETO",
}
SHARE_COLUMNS = [
    "masculino", "feminino", "idade_16_17", "idade_18_34", "idade_35_59", "idade_60_mais", "idade_invalida",
    *EDUCATION_LABELS, "edu_nao_informado",
]
AGE_COLUMNS = ["idade_16_17", "idade_18_34", "idade_35_59", "idade_60_mais"]
# Dimensoes que o perfil do eleitorado do TSE nunca traz.
ALWAYS_UNAVAILABLE = ["renda", "urbano"]
# Abaixo disto (% dos eleitores com a informacao) a dimensao conta como inexistente naquele ano.
MIN_KNOWN_SHARE = 1.0


def empty_electorate_profile(basis: str = "indisponivel") -> dict:
    return {
        "gender": {"male_share": 0.0, "female_share": 0.0},
        "age_bands": {"under_18": 0.0, "a18_34": 0.0, "a35_59": 0.0, "a60_plus": 0.0},
        "dominant_education": "N/A",
        "dominant_income_band": "N/A",
        "urban_concentration": None,
        "basis": basis,
        "electorate_year": None,
        "coverage": None,
        "unavailable": ["genero", "idade", "escolaridade", *ALWAYS_UNAVAILABLE],
    }


def _vote_groups(rows: pd.DataFrame, votes_col: str | None) -> pd.DataFrame:
    """Votos por (uf, cd_municipio); cd_municipio vazio = votos so no nivel da UF."""
    uf = rows["SG_UF"].fillna("").astype(str).str.strip().str.upper() if "SG_UF" in rows.columns else pd.Series("", index=rows.index)
    cd = (
        rows["CD_MUNICIPIO"].map(normalize_tse_code)
        if "CD_MUNICIPIO" in rows.columns
        else pd.Series([None] * len(rows), index=rows.index)
    )
    names = rows["NM_UE"] if "NM_UE" in rows.columns else pd.Series([None] * len(rows), index=rows.index)
    # Sem codigo na base, tenta resolver pelo nome do municipio na UF (so cargos municipais o tem).
    missing = cd.isna() & names.notna()
    if missing.any():
        cd = cd.where(~missing, [lookup_municipio(u, None, n)["cd_municipio"] for u, n in zip(uf[missing], names[missing])])
    votes = pd.to_numeric(rows[votes_col], errors="coerce").fillna(0) if votes_col else pd.Series(1.0, index=rows.index)
    groups = pd.DataFrame({"uf": uf, "cd": cd.fillna(""), "votes": votes})
    groups = groups[groups["uf"] != ""]
    return groups.groupby(["uf", "cd"], as_index=False)["votes"].sum()


def compute_electorate_profile(
    candidate_rows: pd.DataFrame,
    profile: pd.DataFrame | None,
    year: int | None,
    votes_col: str | None,
) -> dict:
    """Perfil do eleitorado ponderado pelos votos do candidato; basis='indisponivel' sem dados."""
    if profile is None or profile.empty or candidate_rows.empty:
        return empty_electorate_profile()

    rows = candidate_rows
    if "ANO_ELEICAO" in rows.columns:
        years = pd.to_numeric(rows["ANO_ELEICAO"], errors="coerce")
        target = year if year is not None else (int(years.max()) if years.notna().any() else None)
        if target is not None:
            rows = rows[years == int(target)]
    else:
        target = year
    if rows.empty:
        return empty_electorate_profile()

    available = sorted(int(y) for y in profile["ano"].dropna().unique())
    usable = [y for y in available if target is None or y <= int(target)]
    if not usable:
        return empty_electorate_profile()
    electorate_year = usable[-1]
    scoped = profile[profile["ano"] == electorate_year]

    by_city = scoped.groupby(["uf", "cd_municipio"], as_index=False)[["eleitores", *SHARE_COLUMNS]].sum()
    by_uf = scoped.groupby("uf", as_index=False)[["eleitores", *SHARE_COLUMNS]].sum()
    city_index = by_city.set_index(["uf", "cd_municipio"])
    uf_index = by_uf.set_index("uf")

    groups = _vote_groups(rows, votes_col)
    total_votes = float(groups["votes"].sum())
    if total_votes <= 0:
        groups = groups.assign(votes=1.0)
        total_votes = float(len(groups)) or 1.0

    accum = {column: 0.0 for column in SHARE_COLUMNS}
    covered = 0.0
    for uf, cd, votes in groups.itertuples(index=False):
        if cd and (uf, cd) in city_index.index:
            record = city_index.loc[(uf, cd)]
        elif not cd and uf in uf_index.index:
            record = uf_index.loc[uf]
        else:
            continue
        voters = float(record["eleitores"])
        if voters <= 0:
            continue
        covered += float(votes)
        for column in SHARE_COLUMNS:
            accum[column] += float(votes) * float(record[column]) / voters
    if covered <= 0:
        return empty_electorate_profile()

    share = {column: accum[column] / covered * 100 for column in SHARE_COLUMNS}
    unavailable: list[str] = []

    # 2000-2006 nao tem faixa etaria (#NE): sem idade conhecida, nao devolve zeros como se fosse dado.
    known_age = sum(share[column] for column in AGE_COLUMNS)
    if known_age < MIN_KNOWN_SHARE:
        unavailable.append("idade")
        age = {column: 0.0 for column in AGE_COLUMNS}
    else:
        # Faixas entre quem tem idade informada (soma ~100%), nao entre todo o eleitorado.
        age = {column: share[column] / known_age * 100 for column in AGE_COLUMNS}

    known_edu = sum(share[key] for key in EDUCATION_LABELS)
    dominant_key = max(EDUCATION_LABELS, key=lambda key: (share[key], key))
    if known_edu < MIN_KNOWN_SHARE:
        unavailable.append("escolaridade")
    dominant_education = EDUCATION_LABELS[dominant_key] if known_edu >= MIN_KNOWN_SHARE else "N/A"

    return {
        "gender": {"male_share": round(share["masculino"], 4), "female_share": round(share["feminino"], 4)},
        "age_bands": {
            "under_18": round(age["idade_16_17"], 4),
            "a18_34": round(age["idade_18_34"], 4),
            "a35_59": round(age["idade_35_59"], 4),
            "a60_plus": round(age["idade_60_mais"], 4),
        },
        "dominant_education": dominant_education,
        # O TSE nao publica renda nem zona urbana/rural no perfil do eleitorado.
        "dominant_income_band": "N/A",
        "urban_concentration": None,
        "basis": "eleitorado",
        "electorate_year": electorate_year,
        "coverage": round(covered / total_votes * 100, 2),
        "unavailable": [*unavailable, *ALWAYS_UNAVAILABLE],
    }
