#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter

from app.services.duckdb_analytics_service import DuckDBAnalyticsService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita vencedores de polarizacao e lista eleitos com espectro indefinido."
    )
    parser.add_argument(
        "--data",
        default="data/curated/analytics.csv",
        help="Arquivo de dados analytics (.csv ou .parquet).",
    )
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=[2018, 2020, 2022, 2024],
        help="Anos para auditoria.",
    )
    parser.add_argument(
        "--fail-on-indefinido",
        action="store_true",
        help="Retorna codigo 1 se encontrar eleitos com espectro indefinido.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    svc = DuckDBAnalyticsService.from_file(
        file_path=args.data,
        default_top_n=20,
        max_top_n=100,
        separator=",",
        encoding="utf-8",
    )

    indef_total = 0

    print("=== MUNICIPALITY BY MAYOR ===")
    for year in args.years:
        out = svc.polarizacao(ano_municipal=year, map_mode="municipalityByMayor")
        full = svc.polarizacao(ano_municipal=year)
        elected_indef = [r for r in full["municipal_uf"] if r.get("eleito") and r.get("espectro") == "indefinido"]
        by_party = Counter(r.get("partido", "") for r in elected_indef)
        total_pref = sum(int(i["total_prefeitos"]) for i in out["municipal_brasil"])
        print(
            f"ano={year} uf_count={len(out['municipal_brasil'])} "
            f"total_prefeitos={total_pref} indef_eleitos={len(elected_indef)} parties={dict(by_party)}"
        )
        indef_total += len(elected_indef)

    print("\n=== STATE BY GOVERNOR ===")
    for year in args.years:
        out = svc.polarizacao(ano_governador=year, map_mode="stateByGovernor")
        elected_indef = [r for r in out["federal"] if r.get("eleito") and r.get("espectro") == "indefinido"]
        by_party = Counter(r.get("partido", "") for r in elected_indef)
        print(
            f"ano={year} winners={len(out['federal'])} indef_eleitos={len(elected_indef)} parties={dict(by_party)}"
        )
        indef_total += len(elected_indef)

    if args.fail_on_indefinido and indef_total > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
