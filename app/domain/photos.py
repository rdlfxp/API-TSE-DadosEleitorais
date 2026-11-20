"""Chave das fotos oficiais de candidatos (pacotes `foto_cand<ano>_<UF>_div.zip` do TSE).

Dentro do zip o nome do arquivo varia por ano:

    FBR280001600166_div.jpeg   2018/2022: F + UF + SQ_CANDIDATO
    BR280000000015_div.jpg     2010/2014: UF + SQ_CANDIDATO (sem o F)
    FBR1_div.png               2006: F + UF + SQ_CANDIDATO (sequencia pequena)
    FMG53953_00070_div.png     2004: F + UF + SG_UE + _ + SQ_CANDIDATO

Nos anos antigos o SQ_CANDIDATO e uma sequencia por municipio (ou por UF) e se repete, entao a chave unica
da foto e (ano, UF, UE, SQ), com UE vazio quando o nome do arquivo nao traz o municipio.
"""
from __future__ import annotations

import re
from typing import NamedTuple

UF_CODES = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "BR", "ZZ",
)
_NAME_RE = re.compile(
    rf"^F?(?P<uf>{'|'.join(UF_CODES)})(?P<first>\d+)(?:_(?P<second>\d+))?_div\.(?:jpe?g|png)$",
    re.IGNORECASE,
)
PHOTO_EXTENSION = "jpg"


class PhotoKey(NamedTuple):
    uf: str
    ue: str
    sq: str


def normalize_sq(value: object) -> str:
    """SQ ou UE como texto sem zeros a esquerda ('00070' -> '70'); vazio se nao for numerico."""
    text = str(value if value is not None else "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    if not text.isdigit():
        return ""
    return str(int(text))


def parse_photo_filename(name: str) -> PhotoKey | None:
    """Extrai (uf, ue, sq) do nome do arquivo de foto; None para qualquer outro arquivo (ex.: leiame.pdf)."""
    base = str(name or "").replace("\\", "/").rsplit("/", 1)[-1]
    match = _NAME_RE.match(base)
    if not match:
        return None
    uf = match.group("uf").upper()
    second = match.group("second")
    if second is not None:
        ue, sq = normalize_sq(match.group("first")), normalize_sq(second)
    else:
        ue, sq = "", normalize_sq(match.group("first"))
    if not sq:
        return None
    return PhotoKey(uf=uf, ue=ue, sq=sq)


def photo_object_key(year: int, uf: str, ue: str, sq: str, prefix: str = "fotos") -> str:
    """Caminho do objeto no storage: fotos/<ano>/<uf>/[<ue>/]<sq>.jpg (minusculo, sem espacos)."""
    parts = [prefix.strip("/"), str(int(year)), str(uf).strip().lower()]
    if ue:
        parts.append(normalize_sq(ue))
    parts.append(f"{normalize_sq(sq)}.{PHOTO_EXTENSION}")
    return "/".join(p for p in parts if p)
