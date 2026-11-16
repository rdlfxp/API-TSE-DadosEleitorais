#!/usr/bin/env python3
"""Aquece o cache da API (L1 + Redis) depois de publicar um snapshot.

Uso: python scripts/warm_cache.py --base-url https://apitse.safeartlabs.com
Rode apos reiniciar a API com o snapshot novo: as chaves sao versionadas pelo snapshot,
entao ate esse passo o cache esta frio.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.cache_warmup import run_warmup  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--years", nargs="*", type=int, default=[2022, 2024, 2026])
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--no-ufs", action="store_true", help="Aquece so o nivel Brasil.")
    args = parser.parse_args()
    started = time.monotonic()
    status: dict = {}
    result = run_warmup(args.base_url, args.years, timeout=args.timeout, include_ufs=not args.no_ufs, status=status)
    print(
        f"[warm] {result['done']}/{result['total']} chamadas em {time.monotonic() - started:.0f}s, "
        f"falhas (5xx/timeout/429 esgotado): {result['failed']}, repeticoes por limite de IP: {result['throttled']}"
    )
    sys.exit(1 if result["failed"] else 0)


if __name__ == "__main__":
    main()
