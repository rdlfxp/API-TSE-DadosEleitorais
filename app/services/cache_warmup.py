"""Aquecimento do cache: pre-calcula os recortes que os paineis do app abrem primeiro.

Chama a propria API (HTTP local), passando exatamente pelo caminho das requisicoes
reais: cache L1/Redis, single-flight e versao do snapshot. Sequencial de proposito,
para nao competir com usuarios pela unica thread do DuckDB.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterator

from app.core.config import settings
from app.core.logging import logger

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]
GENERAL_CARGOS = ["Presidente", "Governador", "Senador", "Deputado Federal", "Deputado Estadual"]
MUNICIPAL_CARGOS = ["Prefeito", "Vereador"]
PROFILE_GROUPS = ["genero", "instrucao", "estado_civil", "cor_raca", "ocupacao", "status"]
GENERAL_YEARS = {2018, 2022, 2026}
MUNICIPAL_YEARS = {2016, 2020, 2024}

WARMUP_STATUS: dict[str, object] = {"state": "idle", "done": 0, "total": 0, "failed": 0, "seconds": None}


def _cargos_for(year: int) -> list[str]:
    if year in GENERAL_YEARS:
        return GENERAL_CARGOS
    if year in MUNICIPAL_YEARS:
        return MUNICIPAL_CARGOS
    return []


def _turnos(year: int, cargo: str) -> list[int | None]:
    if cargo in {"Presidente", "Governador", "Prefeito"}:
        return [1, 2]
    return [1] if year else [None]


def build_warmup_paths(years: list[int], include_ufs: bool = True) -> Iterator[str]:
    """Gera os caminhos a aquecer, dos mais usados (Brasil) aos mais especificos (UF)."""

    def q(path: str, **params: object) -> str:
        clean = {k: v for k, v in params.items() if v is not None}
        return f"{path}?{urllib.parse.urlencode(clean)}" if clean else path

    yield "/v1/analytics/filtros"
    for year in sorted(years, reverse=True):
        yield q("/v1/analytics/overview", ano=year)
        yield q("/v1/analytics/vagas-oficiais", ano=year, group_by="cargo")
        yield q("/v1/analytics/polarizacao", ano=year)
        for cargo in _cargos_for(year):
            for turno in _turnos(year, cargo):
                scope = {"ano": year, "cargo": cargo, "turno": turno}
                yield q("/v1/analytics/overview", **scope)
                yield q("/v1/analytics/top-candidatos", **scope, top_n=20)
                yield q("/v1/analytics/ranking", **scope, group_by="partido", metric="eleitos", top_n=100)
                yield q("/v1/analytics/ranking", **scope, group_by="partido", metric="votos_nominais", top_n=100)
                yield q("/v1/analytics/idade", **scope)
                yield q("/v1/analytics/cor-raca-comparativo", **scope)
                yield q("/v1/analytics/ocupacao-genero", **scope, somente_eleitos="true")
                for group in PROFILE_GROUPS:
                    yield q("/v1/analytics/distribuicao", **scope, group_by=group)
                    yield q("/v1/analytics/distribuicao", **scope, group_by=group, somente_eleitos="true")
    if not include_ufs:
        return
    for year in sorted(years, reverse=True):
        for cargo in _cargos_for(year):
            if cargo in {"Presidente", "Prefeito", "Vereador"}:
                continue
            for uf in UFS:
                scope = {"ano": year, "cargo": cargo, "uf": uf, "turno": 1}
                yield q("/v1/analytics/overview", **scope)
                yield q("/v1/analytics/top-candidatos", **scope, top_n=20)
                yield q("/v1/analytics/ranking", **scope, group_by="partido", metric="eleitos", top_n=100)
                yield q("/v1/analytics/idade", **scope)
                yield q("/v1/analytics/distribuicao", **scope, group_by="genero")
                yield q("/v1/analytics/distribuicao", **scope, group_by="cor_raca")


def run_warmup(base_url: str, years: list[int], *, timeout: float = 120.0, include_ufs: bool = True, status: dict | None = None) -> dict:
    status = status if status is not None else WARMUP_STATUS
    paths = list(build_warmup_paths(years, include_ufs=include_ufs))
    started = time.monotonic()
    status.update({"state": "running", "done": 0, "total": len(paths), "failed": 0, "seconds": None})
    for path in paths:
        try:
            with urllib.request.urlopen(f"{base_url.rstrip('/')}{path}", timeout=timeout) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            # 400/404 para combinacoes sem dados (ex.: turno 2 de Senador) sao esperados.
            if exc.code >= 500:
                status["failed"] = int(status["failed"]) + 1
        except Exception:
            status["failed"] = int(status["failed"]) + 1
        status["done"] = int(status["done"]) + 1
    status.update({"state": "done", "seconds": round(time.monotonic() - started, 1)})
    return status


def start_background_warmup(port: int = 8000) -> threading.Thread | None:
    if not settings.cache_warmup_enabled:
        return None
    years = [int(y) for y in settings.cache_warmup_years.split(",") if y.strip().isdigit()]

    def worker() -> None:
        base = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 300
        time.sleep(max(0.0, settings.cache_warmup_delay_seconds))
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/health", timeout=3) as response:
                    if json.loads(response.read()).get("data_loaded"):
                        break
            except Exception:
                time.sleep(2)
        else:
            WARMUP_STATUS["state"] = "skipped"
            return
        result = run_warmup(base, years)
        logger.info(json.dumps({"event": "cache_warmup_done", **result}, ensure_ascii=False))

    thread = threading.Thread(target=worker, name="cache-warmup", daemon=True)
    thread.start()
    return thread
