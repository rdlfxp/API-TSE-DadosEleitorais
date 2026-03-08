#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import mimetypes
from pathlib import Path

import boto3


DEFAULT_FILES = ["analytics.csv", "analytics.parquet", "quality_report.json", "manifest.json"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload curated artifacts para Cloudflare R2.")
    parser.add_argument("--account-id", required=True, help="Cloudflare account id (R2).")
    parser.add_argument("--access-key-id", required=True, help="R2 access key id.")
    parser.add_argument("--secret-access-key", required=True, help="R2 secret access key.")
    parser.add_argument("--bucket", required=True, help="Nome do bucket R2.")
    parser.add_argument("--source-dir", default="data/curated", help="Diretorio curated local.")
    parser.add_argument("--releases-dir", default="data/releases", help="Diretorio local de snapshots.")
    parser.add_argument("--latest-prefix", default="latest", help="Prefixo remoto para versao ativa.")
    parser.add_argument("--snapshots-prefix", default="snapshots", help="Prefixo remoto para snapshots.")
    parser.add_argument(
        "--keep-snapshots",
        type=int,
        default=7,
        help="Quantidade maxima de snapshots remotos mantidos.",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=DEFAULT_FILES,
        help="Lista de arquivos curated para upload.",
    )
    return parser.parse_args()


def _sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_client(account_id: str, access_key_id: str, secret_access_key: str):
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )


def _upload_file(client, bucket: str, local_file: Path, object_key: str) -> None:
    content_type = mimetypes.guess_type(local_file.name)[0] or "application/octet-stream"
    client.upload_file(
        str(local_file),
        bucket,
        object_key,
        ExtraArgs={
            "ContentType": content_type,
            "Metadata": {"sha256": _sha256(local_file)},
        },
    )


def _find_latest_release_tag(releases_dir: Path) -> str | None:
    if not releases_dir.exists():
        return None
    tags = sorted([p.name for p in releases_dir.iterdir() if p.is_dir() and p.name.isdigit()])
    if not tags:
        return None
    return tags[-1]


def _list_snapshot_prefixes(client, bucket: str, snapshots_prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    prefixes: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{snapshots_prefix}/", Delimiter="/"):
        for pref in page.get("CommonPrefixes", []):
            raw_prefix = pref.get("Prefix", "")
            parts = [p for p in raw_prefix.split("/") if p]
            if len(parts) >= 2:
                prefixes.append(parts[1])
    return sorted(set(prefixes))


def _delete_prefix(client, bucket: str, prefix: str) -> int:
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        batch = [{"Key": obj["Key"]} for obj in objects]
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)
    return deleted


def main() -> None:
    args = parse_args()

    source_dir = Path(args.source_dir)
    releases_dir = Path(args.releases_dir)
    if not source_dir.exists():
        raise SystemExit(f"[r2] source-dir inexistente: {source_dir}")

    client = _build_client(args.account_id, args.access_key_id, args.secret_access_key)

    uploaded = 0
    for filename in args.files:
        local_file = source_dir / filename
        if not local_file.exists():
            continue

        latest_key = f"{args.latest_prefix}/{filename}"
        _upload_file(client, args.bucket, local_file, latest_key)
        print(f"[r2] uploaded latest: {latest_key}")
        uploaded += 1

    latest_tag = _find_latest_release_tag(releases_dir)
    if latest_tag:
        release_dir = releases_dir / latest_tag
        for filename in args.files:
            local_file = release_dir / filename
            if not local_file.exists():
                continue
            snapshot_key = f"{args.snapshots_prefix}/{latest_tag}/{filename}"
            _upload_file(client, args.bucket, local_file, snapshot_key)
            print(f"[r2] uploaded snapshot: {snapshot_key}")
            uploaded += 1

    if args.keep_snapshots >= 0:
        tags = _list_snapshot_prefixes(client, args.bucket, args.snapshots_prefix)
        stale = tags[:-args.keep_snapshots] if args.keep_snapshots > 0 else tags
        removed = 0
        for tag in stale:
            prefix = f"{args.snapshots_prefix}/{tag}/"
            count = _delete_prefix(client, args.bucket, prefix)
            removed += count
            if count > 0:
                print(f"[r2] deleted stale snapshot {tag}: {count} object(s)")
        print(f"[r2] stale cleanup done: {removed} object(s)")

    print(f"[r2] done: {uploaded} object(s) uploaded")


if __name__ == "__main__":
    main()
