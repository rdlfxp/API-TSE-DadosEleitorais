#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import urllib.request
import zipfile
from fnmatch import fnmatch
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baixa fontes públicas do TSE para data/raw.")
    parser.add_argument("--config", required=True, help="Arquivo JSON com lista de fontes.")
    parser.add_argument("--raw-dir", default="data/raw", help="Diretório base de destino.")
    return parser.parse_args()


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response, destination.open("wb") as fh:
        shutil.copyfileobj(response, fh)


def extract_zip(zip_path: Path, output_dir: Path, patterns: list[str]) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as archive:
        members = archive.namelist()
        selected = members
        if patterns:
            selected = [m for m in members if any(fnmatch(Path(m).name, pat) or fnmatch(m, pat) for pat in patterns)]

        for member in selected:
            if member.endswith("/"):
                continue
            archive.extract(member, output_dir)
            extracted.append(output_dir / member)
    return extracted


def main() -> None:
    args = parse_args()
    config_path = Path(args.config)
    raw_dir = Path(args.raw_dir)

    if not config_path.exists():
        raise SystemExit(f"[fetch] config não encontrado: {config_path}")

    entries = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise SystemExit("[fetch] config inválido: esperado array de objetos.")

    downloaded = 0
    extracted = 0
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url", "")).strip()
        target = str(entry.get("target", "")).strip()
        do_extract = bool(entry.get("extract", False))
        extract_members = entry.get("extract_members", [])
        if not isinstance(extract_members, list):
            extract_members = []

        if not url or not target:
            continue

        destination = raw_dir / target
        print(f"[fetch] download: {url} -> {destination}")
        download_file(url, destination)
        downloaded += 1

        if do_extract and destination.suffix.lower() == ".zip":
            output_dir = destination.parent
            files = extract_zip(destination, output_dir, [str(p) for p in extract_members])
            extracted += len(files)
            print(f"[fetch] extracted {len(files)} file(s) from {destination.name}")

    print(f"[fetch] done: {downloaded} download(s), {extracted} extracted file(s)")


if __name__ == "__main__":
    main()
