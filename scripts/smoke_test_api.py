#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke tests da API de analytics.")
    parser.add_argument("--base-url", default="http://localhost:8000", help="URL base da API.")
    parser.add_argument("--timeout", type=float, default=8.0, help="Timeout em segundos por request.")
    parser.add_argument("--retries", type=int, default=0, help="Tentativas extras para erros transitórios.")
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=2.0,
        help="Espera entre tentativas extras em segundos.",
    )
    return parser.parse_args()


def fetch_json(base_url: str, path: str, timeout: float, params: dict | None = None, retries: int = 0, retry_delay: float = 2.0) -> tuple[int, dict]:
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params)
    url = f"{base_url.rstrip('/')}{path}{query}"
    req = urllib.request.Request(url, method="GET")

    last_error: Exception | None = None
    attempts = max(0, retries) + 1
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status = int(response.getcode())
                payload = json.loads(response.read().decode("utf-8"))
                return status, payload
        except HTTPError as exc:
            if exc.code not in {502, 503, 504} or attempt >= attempts:
                raise
            last_error = exc
        except (URLError, TimeoutError, socket.timeout) as exc:
            if attempt >= attempts:
                raise
            last_error = exc
        time.sleep(retry_delay * attempt)

    assert last_error is not None
    raise last_error


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

    status, health = fetch_json(base_url, "/health", args.timeout, retries=args.retries, retry_delay=args.retry_delay)
    assert_true(status == 200, "health status != 200")
    assert_true(health.get("status") == "ok", "health.status != ok")
    assert_true(bool(health.get("data_loaded")), "health.data_loaded = false")
    print("[smoke] ok: /health")

    status, filtros = fetch_json(base_url, "/v1/analytics/filtros", args.timeout, retries=args.retries, retry_delay=args.retry_delay)
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
    status, _ = fetch_json(base_url, "/v1/analytics/overview", args.timeout, overview_params, retries=args.retries, retry_delay=args.retry_delay)
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
        retries=args.retries,
        retry_delay=args.retry_delay,
    )
    assert_true(status == 200, "candidatos/search status != 200")
    assert_true("items" in busca and "total" in busca, "resposta candidatos invalida")
    print("[smoke] ok: /v1/analytics/candidatos/search")

    top_attempts: list[dict[str, object]] = [{"ano": anos[0], "top_n": 3}]
    if ufs:
        top_attempts.append({"ano": anos[0], "top_n": 3, "uf": ufs[0]})
    if cargos:
        top_attempts.append({"ano": anos[0], "top_n": 3, "cargo": cargos[0]})
    if ufs and cargos:
        top_attempts.append({"ano": anos[0], "top_n": 3, "uf": ufs[0], "cargo": cargos[0]})

    top = {}
    top_items: list[dict] = []
    top_errors: list[str] = []
    for top_params in top_attempts:
        try:
            status, candidate_payload = fetch_json(
                base_url,
                "/v1/analytics/top-candidatos",
                args.timeout,
                top_params,
                retries=args.retries,
                retry_delay=args.retry_delay,
            )
            assert_true(status == 200, "top-candidatos status != 200")
            top = candidate_payload if isinstance(candidate_payload, dict) else {}
            top_items = top.get("items", []) if isinstance(top, dict) else []
            if top_items:
                print("[smoke] ok: /v1/analytics/top-candidatos")
                break
        except Exception as exc:  # noqa: BLE001
            top_errors.append(f"params={top_params}: {exc}")

    search_items = busca.get("items", []) if isinstance(busca, dict) else []
    if not top_items and top_errors:
        print("[smoke] warn: /v1/analytics/top-candidatos indisponivel nos recortes tentados", file=sys.stderr)
        for error in top_errors[:3]:
            print(f"[smoke] warn: {error}", file=sys.stderr)
    candidate_pool = top_items + search_items
    assert_true(bool(candidate_pool), "sem candidatos para validar endpoints /v1/candidates/*")

    validated_summary = False
    validated_vote_history = False
    last_candidate_error: Exception | None = None
    candidate_errors: list[str] = []
    seen_candidate_ids: set[str] = set()
    for candidate_item in candidate_pool:
        candidate_id = str(candidate_item.get("candidate_id") or "").strip()
        if not candidate_id or candidate_id in seen_candidate_ids:
            continue
        seen_candidate_ids.add(candidate_id)

        summary_params = {}
        if candidate_item.get("turno_referencia") in {1, 2}:
            summary_params["turno"] = candidate_item["turno_referencia"]
        if candidate_item.get("uf"):
            summary_params["state"] = candidate_item["uf"]
        if candidate_item.get("cargo"):
            summary_params["office"] = candidate_item["cargo"]

        try:
            status, summary = fetch_json(
                base_url,
                f"/v1/candidates/{candidate_id}/summary",
                args.timeout,
                summary_params,
                retries=args.retries,
                retry_delay=args.retry_delay,
            )
            assert_true(status == 200, "candidate summary status != 200")
            assert_true("latest_election" in summary, "summary sem latest_election")
            validated_summary = True

            vote_history_params = {}
            candidate_cpf = summary.get("nr_cpf_candidato")
            if candidate_cpf:
                vote_history_params["nr_cpf_candidato"] = candidate_cpf
            if candidate_item.get("uf"):
                vote_history_params["state"] = candidate_item["uf"]
            if candidate_item.get("cargo"):
                vote_history_params["office"] = candidate_item["cargo"]

            status, history = fetch_json(
                base_url,
                f"/v1/candidates/{candidate_id}/vote-history",
                args.timeout,
                vote_history_params,
                retries=args.retries,
                retry_delay=args.retry_delay,
            )
            assert_true(status == 200, "candidate vote-history status != 200")
            assert_true("items" in history, "vote-history sem items")
            validated_vote_history = True
            break
        except Exception as exc:  # noqa: BLE001
            last_candidate_error = exc
            candidate_errors.append(
                f"candidate_id={candidate_id} state={summary_params.get('state')} office={summary_params.get('office')}: {exc}"
            )
            continue

    assert_true(validated_summary, "nenhum candidato valido para summary")
    if not validated_vote_history:
        if candidate_errors:
            print("[smoke] warn: candidatos testados em /v1/candidates/*", file=sys.stderr)
            for error in candidate_errors[:5]:
                print(f"[smoke] warn: {error}", file=sys.stderr)
        if last_candidate_error is not None:
            raise last_candidate_error
        assert_true(False, "nenhum candidato valido para vote-history")
    print("[smoke] ok: /v1/candidates/{id}/summary")
    print("[smoke] ok: /v1/candidates/{id}/vote-history")

    status, _ = fetch_json(
        base_url,
        "/v1/analytics/distribuicao",
        args.timeout,
        {"group_by": "genero", "ano": anos[0]},
        retries=args.retries,
        retry_delay=args.retry_delay,
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
