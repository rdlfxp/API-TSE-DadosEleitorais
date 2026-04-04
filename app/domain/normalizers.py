import unicodedata

from fastapi import HTTPException

from app.domain.constants import MUNICIPAL_OFFICES


def normalize_ascii_upper(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii").upper()


def is_municipal_office(value: str | None) -> bool:
    return normalize_ascii_upper(value) in MUNICIPAL_OFFICES


def resolve_state_param(state: str | None, uf: str | None) -> str | None:
    resolved_state = state or uf
    if resolved_state and resolved_state.strip().upper() in {"BR", "BRASIL"}:
        return None
    return resolved_state


def resolve_municipality_param(municipality: str | None, municipio: str | None) -> str | None:
    resolved = municipality or municipio
    if resolved is None:
        return None
    normalized = normalize_ascii_upper(resolved)
    normalized = " ".join(normalized.split())
    return normalized or None


def resolve_year_param(ano: int | None, year: int | None) -> int | None:
    if ano is not None and year is not None and int(ano) != int(year):
        raise HTTPException(status_code=400, detail="Parametros conflitantes: ano e year com valores diferentes.")
    if year is not None:
        return int(year)
    if ano is not None:
        return int(ano)
    return None


def normalize_search_cache_key(query: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(query or "").strip())
    ascii_only = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(ascii_only.lower().split())


def is_unscoped_candidate_search(query: str, uf: str | None, cargo: str | None, partido: str | None) -> bool:
    if uf or cargo or partido:
        return False
    return len(normalize_search_cache_key(query).replace(" ", "")) < 8
