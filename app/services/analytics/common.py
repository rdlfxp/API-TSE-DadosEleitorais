from __future__ import annotations

import logging

ELEITO_LABELS = {
    "ELEITO",
    "ELEITO POR QP",
    "ELEITO POR MEDIA",
    "MÉDIA",
}

MUNICIPAL_CARGOS = {
    "PREFEITO",
    "VICE-PREFEITO",
    "VEREADOR",
}

NATIONAL_CARGOS = {
    "PRESIDENTE",
    "VICE-PRESIDENTE",
}

VALID_UF_CODES = {
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
}

PARTIDO_SPECTRUM_MAP = {
    "PCDOB": "esquerda",
    "PC DO B": "esquerda",
    "PDT": "esquerda",
    "PSB": "esquerda",
    "PSOL": "esquerda",
    "PSTU": "esquerda",
    "PT": "esquerda",
    "PV": "esquerda",
    "REDE": "esquerda",
    "PCB": "esquerda",
    "UP": "esquerda",
    "AGIR": "centro",
    "AVANTE": "centro",
    "CIDADANIA": "centro",
    "DC": "centro",
    "MDB": "centro",
    "MOBILIZA": "centro",
    "PODEMOS": "centro",
    "PODE": "centro",
    "PMB": "centro",
    "PMN": "centro",
    "PSD": "centro",
    "SOLIDARIEDADE": "centro",
    "PROS": "centro",
    "DEM": "centro",
    "PHS": "direita",
    "PTC": "direita",
    "NOVO": "direita",
    "PL": "direita",
    "PP": "direita",
    "PRD": "direita",
    "PRTB": "direita",
    "PSDB": "direita",
    "REPUBLICANOS": "direita",
    "PRB": "direita",
    "PTB": "direita",
    "PSC": "direita",
    "PSL": "direita",
    "PATRIOTA": "direita",
    "UNIAO": "direita",
    "UNIAO BRASIL": "direita",
}

MUNICIPIO_ALIAS_MAP = {
    "SANTA IZABEL DO PARA": "SANTA ISABEL DO PARA",
    "MUNHOZ DE MELLO": "MUNHOZ DE MELO",
}

COR_RACA_CATEGORY_ORDER = [
    "BRANCA",
    "PRETA",
    "PARDA",
    "AMARELA",
    "INDIGENA",
    "NAO_INFORMADO",
]

COR_RACA_CATEGORY_LABELS = {
    "BRANCA": "Branca",
    "PRETA": "Preta",
    "PARDA": "Parda",
    "AMARELA": "Amarela",
    "INDIGENA": "Indígena",
    "NAO_INFORMADO": "Não informado",
}

SUPPORTED_ANALYTICS_YEARS = {2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024}
logger = logging.getLogger("api.meucandidato")

UF_TO_MACROREGIAO = {
    "AC": "Norte",
    "AL": "Nordeste",
    "AP": "Norte",
    "AM": "Norte",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "DF": "Centro-Oeste",
    "ES": "Sudeste",
    "GO": "Centro-Oeste",
    "MA": "Nordeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "MG": "Sudeste",
    "PA": "Norte",
    "PB": "Nordeste",
    "PR": "Sul",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RJ": "Sudeste",
    "RN": "Nordeste",
    "RS": "Sul",
    "RO": "Norte",
    "RR": "Norte",
    "SC": "Sul",
    "SP": "Sudeste",
    "SE": "Nordeste",
    "TO": "Norte",
}

UF_CENTROIDS = {
    "AC": (-9.0238, -70.8120),
    "AL": (-9.6482, -35.7089),
    "AP": (1.4141, -51.7701),
    "AM": (-3.4168, -65.8561),
    "BA": (-12.5797, -41.7007),
    "CE": (-5.4984, -39.3206),
    "DF": (-15.7998, -47.8645),
    "ES": (-19.1834, -40.3089),
    "GO": (-15.8270, -49.8362),
    "MA": (-4.9609, -45.2744),
    "MT": (-12.6819, -56.9211),
    "MS": (-20.7722, -54.7852),
    "MG": (-18.5122, -44.5550),
    "PA": (-3.4168, -52.8430),
    "PB": (-7.2399, -36.7820),
    "PR": (-24.8949, -51.5506),
    "PE": (-8.8137, -36.9541),
    "PI": (-7.7183, -42.7289),
    "RJ": (-22.9099, -43.2096),
    "RN": (-5.4026, -36.9541),
    "RS": (-30.0346, -51.2177),
    "RO": (-10.8333, -63.9000),
    "RR": (2.7376, -62.0751),
    "SC": (-27.2423, -50.2189),
    "SP": (-22.19, -48.79),
    "SE": (-10.5741, -37.3857),
    "TO": (-10.1753, -48.2982),
}


def _safe_percent(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _impact_category(percentual_no_total_do_candidato: float) -> str:
    if percentual_no_total_do_candidato >= 1.0:
        return "Alto"
    if percentual_no_total_do_candidato >= 0.3:
        return "Médio"
    return "Baixo"
