from __future__ import annotations

from pathlib import Path

import boto3

from app.config import settings


def _r2_ready() -> bool:
    return all(
        [
            settings.r2_account_id,
            settings.r2_access_key_id,
            settings.r2_secret_access_key,
            settings.r2_bucket,
        ]
    )


def _r2_endpoint() -> str:
    if settings.r2_endpoint:
        return settings.r2_endpoint
    return f"https://{settings.r2_account_id}.r2.cloudflarestorage.com"


def _build_client():
    return boto3.client(
        "s3",
        endpoint_url=_r2_endpoint(),
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name=settings.r2_region_name,
    )


def _download(client, object_key: str, destination: Path) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(settings.r2_bucket, object_key, str(destination))
        return True
    except Exception:
        return False


def ensure_local_analytics_from_r2(preferred_csv_path: Path, prefer_parquet: bool) -> Path | None:
    if not _r2_ready():
        return None

    client = _build_client()
    csv_path = preferred_csv_path
    parquet_path = preferred_csv_path.with_suffix(".parquet")

    if prefer_parquet and _download(client, settings.r2_object_key_parquet, parquet_path):
        return parquet_path

    if _download(client, settings.r2_object_key_csv, csv_path):
        return csv_path

    return None
