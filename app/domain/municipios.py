"""Codigos de municipio (TSE e IBGE), a partir de app/data/municipios_tse_ibge.json.

A tabela vem do cadastro oficial do TSE (ver scripts/build_municipios_table.py). O codigo TSE e o da
base eleitoral (`CD_MUNICIPIO`); o IBGE e o de 7 digitos usado pelas malhas de mapa do app.
"""
from __future__ import annotations

import json
import unicodedata
from functools import lru_cache
from pathlib import Path

_TABLE_PATH = Path(__file__).resolve().parent.parent / "data" / "municipios_tse_ibge.json"


def _norm(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().upper())
    return " ".join("".join(ch for ch in text if not unicodedata.combining(ch)).split())


@lru_cache(maxsize=1)
def _tables() -> tuple[dict[tuple[str, str], dict[str, str]], dict[tuple[str, str], dict[str, str]]]:
    try:
        rows = json.loads(_TABLE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}, {}
    by_code = {(r["uf"], r["cd_tse"]): r for r in rows}
    by_name = {(r["uf"], _norm(r["nome"])): r for r in rows}
    return by_code, by_name


def normalize_tse_code(value: object) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return f"{int(digits):05d}" if digits and len(digits) <= 5 else None


def lookup_municipio(uf: object, cd_tse: object = None, name: object = None) -> dict[str, str | None]:
    """{'cd_municipio', 'cd_municipio_ibge', 'nome'}; None onde nao resolve.

    Prefere o codigo TSE da base; sem ele (ou se nao existir no cadastro), cai para o nome na UF.
    """
    by_code, by_name = _tables()
    uf_key = str(uf or "").strip().upper()
    code = normalize_tse_code(cd_tse)
    entry = by_code.get((uf_key, code)) if code else None
    if entry is None and name:
        entry = by_name.get((uf_key, _norm(str(name))))
    if entry is not None:
        return {"cd_municipio": entry["cd_tse"], "cd_municipio_ibge": entry["cd_ibge"], "nome": entry["nome"]}
    return {"cd_municipio": code, "cd_municipio_ibge": None, "nome": None}


@lru_cache(maxsize=1)
def _by_uf() -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in _tables()[0].values():
        grouped.setdefault(row["uf"], []).append(row)
    for rows in grouped.values():
        rows.sort(key=lambda r: _norm(r["nome"]))
    return grouped


def list_municipios(uf: object) -> list[dict[str, str]]:
    """Municipios da UF (nome, codigo TSE e IBGE), em ordem alfabetica sem acento."""
    return list(_by_uf().get(str(uf or "").strip().upper(), []))
