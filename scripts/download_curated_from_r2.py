#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import boto3
from botocore.config import Config


def main() -> None:
    parser = argparse.ArgumentParser(description="Baixa o snapshot curated atual do R2 para refresh incremental.")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--access-key-id", required=True)
    parser.add_argument("--secret-access-key", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="latest")
    parser.add_argument("--output-dir", default="data/curated")
    args = parser.parse_args()

    client = boto3.client(
        "s3",
        endpoint_url=f"https://{args.account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=args.access_key_id,
        aws_secret_access_key=args.secret_access_key,
        region_name="auto",
        config=Config(connect_timeout=10, read_timeout=120, retries={"max_attempts": 3, "mode": "standard"}),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename in ("analytics.parquet", "candidate_history.parquet"):
        key = f"{args.prefix.rstrip('/')}/{filename}"
        destination = output_dir / filename
        print(f"[r2-download] {key} -> {destination}", flush=True)
        client.download_file(args.bucket, key, str(destination))


if __name__ == "__main__":
    main()
