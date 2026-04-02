#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Any
from urllib.error import HTTPError


@dataclass(frozen=True)
class EndpointSpec:
    name: str
    path: str
    params: Callable[[dict[str, Any]], dict[str, Any]]
    repeats: int = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Perfila endpoints da API um por um e mede RSS/CPU do processo do servidor."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host para subir o servidor local.")
    parser.add_argument("--port", type=int, default=8008, help="Porta local para o servidor.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Timeout por request.")
    parser.add_argument("--startup-timeout", type=float, default=60.0, help="Tempo máximo aguardando o healthcheck.")
    parser.add_argument("--sample-interval", type=float, default=0.05, help="Intervalo entre amostras do processo.")
    parser.add_argument("--repeats", type=int, default=1, help="Número de execuções por endpoint.")
    parser.add_argument("--base-url", default="", help="Use um servidor já rodando em vez de subir um novo.")
    parser.add_argument("--pid", type=int, default=0, help="PID do processo do servidor quando usar --base-url.")
    parser.add_argument(
        "--only",
        default="",
        help="Lista separada por vírgulas com os nomes dos endpoints a perfilar.",
    )
    parser.add_argument("--json", action="store_true", help="Imprime o relatório final em JSON.")
    return parser.parse_args()


def _prefer_ipv4() -> None:
    original_getaddrinfo = socket.getaddrinfo

    def ipv4_first_getaddrinfo(host: str, port: int, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0):
        infos = original_getaddrinfo(host, port, family, type, proto, flags)
        ipv4_infos = [info for info in infos if info[0] == socket.AF_INET]
        return ipv4_infos or infos

    socket.getaddrinfo = ipv4_first_getaddrinfo


def fetch_json(base_url: str, path: str, timeout: float, params: dict[str, Any] | None = None) -> tuple[int, Any]:
    query = f"?{urllib.parse.urlencode(params, doseq=True)}" if params else ""
    url = f"{base_url.rstrip('/')}{path}{query}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            try:
                return int(response.getcode()), json.loads(raw)
            except Exception:
                return int(response.getcode()), raw
    except HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        try:
            payload: Any = json.loads(body)
        except Exception:
            payload = body
        return int(exc.code), payload


def sample_ps(pid: int) -> tuple[float | None, float | None]:
    try:
        proc = subprocess.run(
            ["ps", "-o", "rss=,%cpu=", "-p", str(pid)],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None, None

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        return None, None

    try:
        rss_kb_s, cpu_s = lines[-1].split()
        rss_kb = float(rss_kb_s)
        cpu_pct = float(cpu_s)
        return rss_kb / 1024.0, cpu_pct
    except Exception:
        return None, None


class ProcessSampler(threading.Thread):
    def __init__(self, pid: int, interval: float) -> None:
        super().__init__(daemon=True)
        self.pid = pid
        self.interval = interval
        self._stop_event = threading.Event()
        self.max_rss_mb = 0.0
        self.max_cpu_pct = 0.0
        self.samples: list[tuple[float, float | None, float | None]] = []

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            rss_mb, cpu_pct = sample_ps(self.pid)
            ts = time.time()
            self.samples.append((ts, rss_mb, cpu_pct))
            if rss_mb is not None and rss_mb > self.max_rss_mb:
                self.max_rss_mb = rss_mb
            if cpu_pct is not None and cpu_pct > self.max_cpu_pct:
                self.max_cpu_pct = cpu_pct
            time.sleep(self.interval)


def wait_for_health(base_url: str, timeout: float, startup_timeout: float) -> dict[str, Any]:
    deadline = time.time() + startup_timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            status, payload = fetch_json(base_url, "/health", timeout)
            if status == 200 and isinstance(payload, dict) and payload.get("status") == "ok":
                return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.5)
    raise RuntimeError(f"Servidor nao subiu a tempo: {last_error}")


def launch_server(host: str, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        host,
        "--port",
        str(port),
        "--log-level",
        "warning",
    ]
    return subprocess.Popen(cmd, cwd=str(Path(__file__).resolve().parents[1]), env=env, text=True)


def run_profile(
    base_url: str,
    pid: int,
    timeout: float,
    interval: float,
    repeats: int,
    only: set[str] | None = None,
) -> list[dict[str, Any]]:
    status, filtros = fetch_json(base_url, "/v1/analytics/filtros", timeout)
    if status != 200 or not isinstance(filtros, dict):
        raise RuntimeError(f"Falha ao carregar filtros: status={status}, payload={filtros}")

    anos = filtros.get("anos", [])
    ufs = filtros.get("ufs", [])
    cargos = filtros.get("cargos", [])
    if not anos:
        raise RuntimeError("Nao ha anos disponiveis em /v1/analytics/filtros")

    year = next((value for value in anos if isinstance(value, int)), anos[0])
    uf = "SP" if "SP" in ufs else (ufs[0] if ufs else None)
    cargo = "Vereador" if "Vereador" in cargos else (cargos[0] if cargos else None)

    _, top = fetch_json(
        base_url,
        "/v1/analytics/top-candidatos",
        timeout,
        {"ano": year, "uf": uf, "cargo": cargo, "top_n": 10},
    )
    items = top.get("items", []) if isinstance(top, dict) else []
    if len(items) < 2:
        _, top = fetch_json(base_url, "/v1/analytics/top-candidatos", timeout, {"ano": year, "top_n": 10})
        items = top.get("items", []) if isinstance(top, dict) else []
    if not items:
        raise RuntimeError("Nao foi possivel obter candidato base para profiling")

    cid1 = str(items[0].get("candidate_id") or items[0].get("sq_candidato") or items[0].get("id"))
    cid2 = str(items[1].get("candidate_id") or items[1].get("sq_candidato") or items[1].get("id")) if len(items) > 1 else cid1
    office = items[0].get("cargo") or cargo
    state = items[0].get("uf") or uf
    municipio = items[0].get("municipio")

    endpoint_specs = [
        EndpointSpec("analytics.filtros", "/v1/analytics/filtros", lambda _: {}),
        EndpointSpec("analytics.overview", "/v1/analytics/overview", lambda _: {"ano": year, "uf": uf, "cargo": cargo}),
        EndpointSpec(
            "analytics.top_candidatos",
            "/v1/analytics/top-candidatos",
            lambda _: {"ano": year, "uf": uf, "cargo": cargo, "top_n": 10},
        ),
        EndpointSpec(
            "analytics.candidatos_search",
            "/v1/analytics/candidatos/search",
            lambda _: {"q": "ca", "ano": year, "uf": uf, "cargo": cargo, "page": 1, "page_size": 5},
        ),
        EndpointSpec(
            "analytics.distribuicao",
            "/v1/analytics/distribuicao",
            lambda _: {"group_by": "genero", "ano": year, "uf": uf, "cargo": cargo},
        ),
        EndpointSpec(
            "analytics.polarizacao",
            "/v1/analytics/polarizacao",
            lambda _: {"uf": uf, "ano_governador": 2014, "ano_municipal": 2016},
        ),
        EndpointSpec(
            "candidate.summary",
            f"/v1/candidates/{cid1}/summary",
            lambda _: {"year": year, "state": state, "office": office},
        ),
        EndpointSpec(
            "candidate.vote_history",
            f"/v1/candidates/{cid1}/vote-history",
            lambda _: {"state": state, "office": office},
        ),
        EndpointSpec(
            "candidate.electorate_profile",
            f"/v1/candidates/{cid1}/electorate-profile",
            lambda _: {"year": year, "state": state, "office": office, "municipio": municipio},
        ),
        EndpointSpec(
            "candidate.vote_distribution",
            f"/v1/candidates/{cid1}/vote-distribution",
            lambda _: {"level": "municipio", "year": year, "state": state, "office": office},
        ),
        EndpointSpec(
            "candidates.compare",
            "/v1/candidates/compare",
            lambda _: {"candidate_ids": f"{cid1},{cid2}", "year": year, "state": state, "office": office},
        ),
    ]

    allowed = {item.strip() for item in (only or set()) if item.strip()}
    if allowed:
        endpoint_specs = [spec for spec in endpoint_specs if spec.name in allowed or spec.path in allowed]
        if not endpoint_specs:
            raise RuntimeError(f"Nenhum endpoint corresponde ao filtro --only={sorted(allowed)}")

    results: list[dict[str, Any]] = []
    for spec in endpoint_specs:
        for _ in range(max(1, repeats, spec.repeats)):
            before_rss, before_cpu = sample_ps(pid)
            sampler = ProcessSampler(pid, interval)
            sampler.start()
            started = time.perf_counter()
            status, payload = fetch_json(base_url, spec.path, timeout, spec.params({}))
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            sampler.stop()
            sampler.join(timeout=2.0)
            after_rss, after_cpu = sample_ps(pid)
            results.append(
                {
                    "endpoint": spec.name,
                    "path": spec.path,
                    "status": status,
                    "duration_ms": elapsed_ms,
                    "rss_before_mb": before_rss,
                    "rss_peak_mb": round(sampler.max_rss_mb, 2) if sampler.max_rss_mb else None,
                    "rss_after_mb": after_rss,
                    "cpu_peak_pct": round(sampler.max_cpu_pct, 2) if sampler.max_cpu_pct else None,
                    "cpu_after_pct": after_cpu,
                    "payload_type": type(payload).__name__,
                    "payload_keys": list(payload.keys())[:12] if isinstance(payload, dict) else None,
                }
            )
    return results


def print_table(results: list[dict[str, Any]]) -> None:
    columns = ["endpoint", "status", "duration_ms", "rss_before_mb", "rss_peak_mb", "rss_after_mb", "cpu_peak_pct"]
    widths = {column: len(column) for column in columns}
    for row in results:
        for column in columns:
            widths[column] = max(widths[column], len(str(row.get(column))))
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    print(header)
    print("  ".join("-" * widths[column] for column in columns))
    for row in results:
        print("  ".join(str(row.get(column)).ljust(widths[column]) for column in columns))


def main() -> None:
    args = parse_args()
    _prefer_ipv4()

    server: subprocess.Popen[str] | None = None
    base_url = args.base_url.strip()
    if not base_url:
        server = launch_server(args.host, args.port)
        base_url = f"http://{args.host}:{args.port}"

    try:
        wait_for_health(base_url, args.timeout, args.startup_timeout)
        pid = int(args.pid or 0)
        if pid <= 0:
            if server is not None:
                pid = int(server.pid)
            else:
                raise RuntimeError("Informe --pid quando usar --base-url com um servidor ja em execucao.")
        only = {item.strip() for item in args.only.split(",") if item.strip()} if args.only else set()
        results = run_profile(base_url, pid, args.timeout, args.sample_interval, args.repeats, only=only)
        if args.json:
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            print_table(results)
            print()
            worst_rss = max(results, key=lambda row: float(row.get("rss_peak_mb") or 0))
            worst_cpu = max(results, key=lambda row: float(row.get("cpu_peak_pct") or 0))
            print(f"worst_rss={worst_rss['endpoint']} peak={worst_rss.get('rss_peak_mb')}MB")
            print(f"worst_cpu={worst_cpu['endpoint']} peak={worst_cpu.get('cpu_peak_pct')}%")
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"[profile] FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
