#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import sys
import urllib.parse
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke tests da API de analytics.")
    parser.add_argument("--base-url", default="http://localhost:8000", help="URL base da API.")
    parser.add_argument("--timeout", type=float, default=8.0, help="Timeout em segundos por request.")
    return parser.parse_args()


def fetch_json(base_url: str, path: str, timeout: float, params: dict | None = None) -> tuple[int, dict]:
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params)
    url = f"{base_url.rstrip('/')}{path}{query}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        status = int(response.getcode())
        payload = json.loads(response.read().decode("utf-8"))
        return status, payload


def _prefer_ipv4() -> None:
    original_getaddrinfo = socket.getaddrinfo

    def ipv4_first_getaddrinfo(host: str, port: int, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0):
        infos = original_getaddrinfo(host, port, family, type, proto, flags)
        ipv4_infos = [info for info in infos if info[0] == socket.AF_INET]
        return ipv4_infos or infos

    socket.getaddrinfo = ipv4_first_getaddrinfo


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"[smoke] FAIL: {message}")


def main() -> None:
    args = parse_args()
    base_url = args.base_url
    _prefer_ipv4()

    status, health = fetch_json(base_url, "/health", args.timeout)
    assert_true(status == 200, "health status != 200")
    assert_true(health.get("status") == "ok", "health.status != ok")
    assert_true(bool(health.get("data_loaded")), "health.data_loaded = false")
    print("[smoke] ok: /health")

    status, filtros = fetch_json(base_url, "/v1/analytics/filtros", args.timeout)
    assert_true(status == 200, "filtros status != 200")
    anos = filtros.get("anos", [])
    ufs = filtros.get("ufs", [])
    cargos = filtros.get("cargos", [])
    assert_true(bool(anos), "filtros sem anos")
    print("[smoke] ok: /v1/analytics/filtros")

    overview_params = {"ano": anos[0]}
    if ufs:
        overview_params["uf"] = ufs[0]
    if cargos:
        overview_params["cargo"] = cargos[0]
    status, _ = fetch_json(base_url, "/v1/analytics/overview", args.timeout, overview_params)
    assert_true(status == 200, "overview status != 200")
    print("[smoke] ok: /v1/analytics/overview")

    query = "ca"
    if cargos:
        query = str(cargos[0]).split(" ")[0]
    candidate_params = {
        "q": query[:2] if len(query) > 1 else "ca",
        "ano": anos[0],
        "page": 1,
        "page_size": 5,
    }
    if ufs:
        candidate_params["uf"] = ufs[0]
    if cargos:
        candidate_params["cargo"] = cargos[0]
    status, busca = fetch_json(
        base_url,
        "/v1/analytics/candidatos/search",
        args.timeout,
        candidate_params,
    )
    assert_true(status == 200, "candidatos/search status != 200")
    assert_true("items" in busca and "total" in busca, "resposta candidatos invalida")
    print("[smoke] ok: /v1/analytics/candidatos/search")

    status, _ = fetch_json(
        base_url,
        "/v1/analytics/distribuicao",
        args.timeout,
        {"group_by": "genero", "ano": anos[0]},
    )
    assert_true(status == 200, "distribuicao status != 200")
    print("[smoke] ok: /v1/analytics/distribuicao")

    print("[smoke] PASS: endpoints críticos validados")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[smoke] FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
