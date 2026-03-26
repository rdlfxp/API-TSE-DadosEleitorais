#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from urllib.error import HTTPError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Valida endpoints principais da API com foco em anos legados.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="URL base da API")
    parser.add_argument("--legacy-year", type=int, default=2016, help="Ano legado esperado para validação")
    parser.add_argument("--timeout", type=float, default=30.0, help="Timeout por request")
    return parser.parse_args()


def fetch_json(base_url: str, path: str, timeout: float, params: dict | None = None) -> tuple[int, object]:
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params, doseq=True)
    url = f"{base_url.rstrip('/')}{path}{query}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            payload: object
            try:
                payload = json.loads(raw)
            except Exception:
                payload = raw
            return int(response.getcode()), payload
    except HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        try:
            payload = json.loads(body)
        except Exception:
            payload = body
        return int(exc.code), payload


def main() -> None:
    args = parse_args()
    base_url = args.base_url

    checks: list[tuple[str, dict, tuple[int, ...]]] = []
    failures: list[str] = []

    def run(path: str, params: dict | None = None, expected: tuple[int, ...] = (200,)) -> tuple[int, object]:
        status, payload = fetch_json(base_url, path, args.timeout, params)
        checks.append((path, params or {}, expected))
        if status not in expected:
            failures.append(f"{status} {path} params={params or {}} payload={payload}")
        return status, payload

    run("/health")
    run("/metrics")

    status, filtros_obj = run("/v1/analytics/filtros")
    if status != 200 or not isinstance(filtros_obj, dict):
        raise SystemExit("[validate-endpoints] filtros indisponível")

    anos = filtros_obj.get("anos", []) if isinstance(filtros_obj.get("anos", []), list) else []
    ufs = filtros_obj.get("ufs", []) if isinstance(filtros_obj.get("ufs", []), list) else []
    cargos = filtros_obj.get("cargos", []) if isinstance(filtros_obj.get("cargos", []), list) else []

    year = args.legacy_year if args.legacy_year in anos else (max([a for a in anos if isinstance(a, int) and a <= args.legacy_year], default=None))
    if year is None:
        raise SystemExit(f"[validate-endpoints] nenhum ano legado <= {args.legacy_year} encontrado em filtros")

    uf = "SP" if "SP" in ufs else (ufs[0] if ufs else None)
    cargo = "Vereador" if "Vereador" in cargos else (cargos[0] if cargos else None)

    run("/v1/analytics/overview", {"ano": year, "uf": uf, "cargo": cargo})
    run("/v1/analytics/top-candidatos", {"ano": year, "uf": uf, "cargo": cargo, "top_n": 5})
    run("/v1/analytics/candidatos/search", {"q": "ca", "ano": year, "uf": uf, "cargo": cargo, "page": 1, "page_size": 5})
    run("/v1/analytics/candidatos", {"query": "ca", "ano": year, "uf": uf, "cargo": cargo, "page": 1, "page_size": 5})
    run("/v1/analytics/distribuicao", {"group_by": "genero", "ano": year, "uf": uf, "cargo": cargo})
    run("/v1/analytics/cor-raca-comparativo", {"ano": year, "uf": uf, "cargo": cargo})
    run("/v1/analytics/ocupacao-genero", {"ano": year, "uf": uf, "cargo": cargo, "somente_eleitos": "false"})
    run("/v1/analytics/idade", {"ano": year, "uf": uf, "cargo": cargo, "somente_eleitos": "false"})
    run("/v1/analytics/serie-temporal", {"metric": "votos_nominais", "uf": uf, "cargo": cargo})
    run("/v1/analytics/ranking", {"group_by": "partido", "metric": "votos_nominais", "ano": year, "uf": uf, "cargo": cargo, "top_n": 5})
    run("/v1/analytics/mapa-uf", {"metric": "votos_nominais", "ano": year, "cargo": cargo})
    run("/v1/analytics/vagas-oficiais", {"ano": year, "uf": uf, "group_by": "cargo"})
    run("/v1/analytics/polarizacao", {"uf": uf, "ano_governador": 2014, "ano_municipal": 2016})

    _, top_obj = run("/v1/analytics/top-candidatos", {"ano": year, "uf": uf, "cargo": cargo, "top_n": 10})
    items = top_obj.get("items", []) if isinstance(top_obj, dict) else []
    if len(items) < 2:
        _, top_obj = run("/v1/analytics/top-candidatos", {"ano": year, "uf": uf, "top_n": 10})
        items = top_obj.get("items", []) if isinstance(top_obj, dict) else []

    if not items:
        raise SystemExit("[validate-endpoints] sem candidatos para validar endpoints /v1/candidates/*")

    cid1 = str(items[0].get("candidate_id") or items[0].get("sq_candidato") or items[0].get("id"))
    cid2 = str(items[1].get("candidate_id") or items[1].get("sq_candidato") or items[1].get("id")) if len(items) > 1 else cid1
    office = items[0].get("cargo") or cargo
    state = items[0].get("uf") or uf

    run(f"/v1/candidates/{cid1}/summary", {"year": year, "state": state, "office": office})
    run(f"/v1/candidates/{cid1}/vote-history", {"state": state, "office": office})
    run(f"/v1/candidates/{cid1}/electorate-profile", {"year": year, "state": state, "office": office})
    run(f"/v1/candidates/{cid1}/vote-distribution", {"level": "municipio", "year": year, "state": state, "office": office})
    run(f"/v1/candidates/{cid1}/vote-map", {"level": "municipio", "year": year, "state": state, "office": office})
    run(f"/v1/candidates/{cid1}/zone-fidelity", {"year": year, "state": state, "office": office})
    run("/v1/candidates/compare", {"candidate_ids": f"{cid1},{cid2}", "year": year, "state": state, "office": office})

    print(f"[validate-endpoints] TOTAL {len(checks)}")
    print(f"[validate-endpoints] FAILS {len(failures)}")
    if failures:
        for failure in failures:
            print(f"[validate-endpoints] FAIL {failure}")
        raise SystemExit(2)

    print("[validate-endpoints] PASS")


if __name__ == "__main__":
    main()
