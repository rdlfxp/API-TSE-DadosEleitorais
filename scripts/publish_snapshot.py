#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publica snapshot versionado da base curated para data/releases/YYYYMMDD."
    )
    parser.add_argument("--source-dir", default="data/curated", help="Diretorio de origem com artefatos curated.")
    parser.add_argument("--releases-dir", default="data/releases", help="Diretorio raiz de snapshots.")
    parser.add_argument(
        "--date-tag",
        default=datetime.now(timezone.utc).strftime("%Y%m%d"),
        help="Tag da release (padrao: data UTC atual YYYYMMDD).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_dir = Path(args.source_dir)
    releases_dir = Path(args.releases_dir)
    target_dir = releases_dir / args.date_tag

    if not source_dir.exists():
        raise SystemExit(f"[snapshot] source-dir inexistente: {source_dir}")

    target_dir.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []
    for filename in ["analytics.csv", "analytics.parquet", "quality_report.json", "manifest.json"]:
        src = source_dir / filename
        if src.exists():
            shutil.copy2(src, target_dir / filename)
            copied.append(filename)

    release_manifest = {
        "released_at_utc": datetime.now(timezone.utc).isoformat(),
        "tag": args.date_tag,
        "source_dir": str(source_dir),
        "target_dir": str(target_dir),
        "files": copied,
    }
    (target_dir / "release_manifest.json").write_text(
        json.dumps(release_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[snapshot] release: {target_dir}")
    print(f"[snapshot] files: {copied}")


if __name__ == "__main__":
    main()
