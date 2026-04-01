#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import shutil
import time
import urllib.request
import zipfile
from fnmatch import fnmatch
from pathlib import Path
from urllib.error import URLError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baixa fontes públicas do TSE para data/raw.")
    parser.add_argument("--config", required=True, help="Arquivo JSON com lista de fontes.")
    parser.add_argument("--raw-dir", default="data/raw", help="Diretório base de destino.")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout por download em segundos.")
    parser.add_argument("--retries", type=int, default=3, help="Tentativas por arquivo.")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Pula arquivos que ja existem no destino.",
    )
    return parser.parse_args()


def log(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[fetch] {timestamp} {message}", flush=True)


def format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{num_bytes}B"


def download_file(url: str, destination: Path, timeout: int, retries: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        tmp_destination = destination.with_suffix(destination.suffix + ".part")
        downloaded_bytes = 0
        started_at = time.monotonic()
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response, tmp_destination.open("wb") as fh:
                total_bytes = response.info().get("Content-Length")
                total_bytes_int = int(total_bytes) if total_bytes and total_bytes.isdigit() else None
                next_progress_mark = 50 * 1024 * 1024

                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    fh.write(chunk)
                    downloaded_bytes += len(chunk)
                    if downloaded_bytes >= next_progress_mark:
                        if total_bytes_int:
                            log(
                                f"progress {destination.name}: "
                                f"{format_bytes(downloaded_bytes)}/{format_bytes(total_bytes_int)} "
                                f"(attempt {attempt}/{retries})"
                            )
                        else:
                            log(
                                f"progress {destination.name}: "
                                f"{format_bytes(downloaded_bytes)} (attempt {attempt}/{retries})"
                            )
                        next_progress_mark += 50 * 1024 * 1024

            tmp_destination.replace(destination)
            elapsed = time.monotonic() - started_at
            log(
                f"download complete: {destination} "
                f"({format_bytes(downloaded_bytes)} in {elapsed:.1f}s, attempt {attempt}/{retries})"
            )
            return
        except (TimeoutError, URLError, OSError, socket.timeout) as exc:
            last_error = exc
            if tmp_destination.exists():
                tmp_destination.unlink()
            log(
                f"download failed for {destination.name} on attempt {attempt}/{retries}: {exc}"
            )
            if attempt < retries:
                sleep_seconds = min(10, attempt * 2)
                log(f"retrying in {sleep_seconds}s")
                time.sleep(sleep_seconds)
    assert last_error is not None
    raise last_error


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
    total_entries = len(entries)
    for index, entry in enumerate(entries, start=1):
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
        if args.skip_existing and destination.exists():
            log(f"[{index}/{total_entries}] skip existing: {destination}")
            continue

        log(f"[{index}/{total_entries}] download: {url} -> {destination}")
        download_file(url, destination, timeout=args.timeout, retries=args.retries)
        downloaded += 1

        if do_extract and destination.suffix.lower() == ".zip":
            output_dir = destination.parent
            files = extract_zip(destination, output_dir, [str(p) for p in extract_members])
            extracted += len(files)
            log(f"extracted {len(files)} file(s) from {destination.name}")

    log(f"done: {downloaded} download(s), {extracted} extracted file(s)")


if __name__ == "__main__":
    main()
