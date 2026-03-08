#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validador go/no-go para publicação de dados.")
    parser.add_argument("--analytics", default="data/curated/analytics.csv")
    parser.add_argument("--quality-report", default="data/curated/quality_report.json")
    parser.add_argument("--manifest", default="data/curated/manifest.json")
    parser.add_argument("--releases-dir", default="data/releases")
    parser.add_argument("--require-release", action="store_true")
    parser.add_argument("--max-duplicate-rows", type=int, default=0)
    parser.add_argument("--max-negative-votes", type=int, default=0)
    parser.add_argument("--max-required-null-rate", type=float, default=0.02)
    parser.add_argument("--expected-years", nargs="*", type=int, default=[])
    parser.add_argument("--api-url", default="")
    return parser.parse_args()


def fail(msg: str) -> None:
    raise SystemExit(f"[go-no-go] NO-GO: {msg}")


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"arquivo ausente: {path}")
    except json.JSONDecodeError as exc:
        fail(f"json invalido em {path}: {exc}")
    return {}


def check_file_exists(path: Path) -> None:
    if not path.exists() or not path.is_file():
        fail(f"arquivo nao encontrado: {path}")


def check_quality(report: dict, args: argparse.Namespace) -> None:
    missing = report.get("required_missing_columns", [])
    if missing:
        fail(f"colunas obrigatorias faltantes: {missing}")

    dup = int(report.get("duplicate_rows_on_key", 0))
    if dup > args.max_duplicate_rows:
        fail(f"duplicidade acima do limite: {dup} > {args.max_duplicate_rows}")

    neg = int(report.get("votos_invalidos_negativos", 0))
    if neg > args.max_negative_votes:
        fail(f"votos negativos acima do limite: {neg} > {args.max_negative_votes}")

    null_rates = report.get("required_null_rates", {})
    for col, rate in null_rates.items():
        if float(rate) > args.max_required_null_rate:
            fail(
                f"nulos acima do limite em {col}: {rate:.4f} > "
                f"{args.max_required_null_rate:.4f}"
            )


def check_manifest(manifest: dict, expected_years: list[int]) -> None:
    if int(manifest.get("source_files_count", 0)) <= 0:
        fail("manifest sem arquivos de origem")
    if not manifest.get("output_sha256"):
        fail("manifest sem hash do output")

    summary = manifest.get("summary", {})
    years = summary.get("anos", [])
    if expected_years:
        missing = sorted(set(expected_years) - set(years))
        if missing:
            fail(f"anos esperados ausentes no manifest: {missing}")


def check_release_exists(releases_dir: Path) -> None:
    if not releases_dir.exists():
        fail(f"diretorio de releases ausente: {releases_dir}")
    release_dirs = [p for p in releases_dir.iterdir() if p.is_dir()]
    if not release_dirs:
        fail("nenhum snapshot encontrado em data/releases")


def fetch_json(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload)
    except (urllib.error.URLError, TimeoutError) as exc:
        fail(f"erro ao acessar {url}: {exc}")
    except json.JSONDecodeError as exc:
        fail(f"json invalido em {url}: {exc}")
    return {}


def check_api(api_url: str) -> None:
    base = api_url.rstrip("/")
    health = fetch_json(f"{base}/health")
    if health.get("status") != "ok" or not health.get("data_loaded"):
        fail("healthcheck da API nao esta ok/data_loaded")

    filtros = fetch_json(f"{base}/v1/analytics/filtros")
    if not filtros.get("anos"):
        fail("endpoint /v1/analytics/filtros sem anos")


def main() -> None:
    args = parse_args()
    analytics = Path(args.analytics)
    quality_report = Path(args.quality_report)
    manifest_path = Path(args.manifest)

    check_file_exists(analytics)
    check_file_exists(quality_report)
    check_file_exists(manifest_path)

    report = load_json(quality_report)
    manifest = load_json(manifest_path)

    check_quality(report, args)
    check_manifest(manifest, args.expected_years)
    if args.require_release:
        check_release_exists(Path(args.releases_dir))
    if args.api_url:
        check_api(args.api_url)

    print("[go-no-go] GO: validacoes criticas aprovadas")


if __name__ == "__main__":
    main()
