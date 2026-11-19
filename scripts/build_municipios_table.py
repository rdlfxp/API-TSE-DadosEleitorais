#!/usr/bin/env python3
"""Gera app/data/municipios_tse_ibge.json (UF, codigo TSE, codigo IBGE e nome de cada municipio).

Fonte: cadastro oficial da divulgacao do TSE (`config/mun-e<eleicao>-cm.json`), que traz `cd` (TSE) e
`cdi` (IBGE). Os codigos nao mudam entre eleicoes; rode de novo quando surgir municipio novo.

    python scripts/build_municipios_table.py --year 2024 --election 619
"""
from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path


# Fora do cadastro municipal do TSE (DF nao tem eleicao municipal; Noronha e distrito estadual de PE),
# mas com votos nas eleicoes gerais.
EXTRA_MUNICIPIOS = [
    {"uf": "DF", "cd_tse": "97012", "cd_ibge": "5300108", "nome": "BRASÍLIA"},
    {"uf": "PE", "cd_tse": "30015", "cd_ibge": "2605459", "nome": "FERNANDO DE NORONHA"},
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--election", type=int, default=619)
    parser.add_argument("--base-url", default="https://resultados.tse.jus.br")
    parser.add_argument("--output", default="app/data/municipios_tse_ibge.json")
    args = parser.parse_args()

    url = f"{args.base_url}/oficial/ele{args.year}/{args.election}/config/mun-e{args.election:06d}-cm.json"
    with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310 - URL fixa do TSE
        payload = json.loads(response.read().decode("utf-8"))

    rows: list[dict[str, str]] = []
    for state in payload.get("abr", []):
        uf = str(state.get("cd", "")).upper()
        if len(uf) != 2 or uf in {"BR", "ZZ"}:
            continue
        for item in state.get("mu", []):
            tse, ibge = str(item.get("cd", "")).strip(), str(item.get("cdi", "")).strip()
            if tse and ibge:
                rows.append({"uf": uf, "cd_tse": f"{int(tse):05d}", "cd_ibge": ibge, "nome": str(item.get("nm", "")).strip()})
    known = {(r["uf"], r["cd_tse"]) for r in rows}
    rows.extend(r for r in EXTRA_MUNICIPIOS if (r["uf"], r["cd_tse"]) not in known)
    rows.sort(key=lambda r: (r["uf"], r["cd_tse"]))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"[municipios] {len(rows)} municipios -> {output} ({output.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
