#!/usr/bin/env python3
"""Baixa as fotos oficiais de candidatos do TSE (2004-2022), padroniza e gera o indice da API.

    pip install -r requirements-photos.txt
    python scripts/build_candidate_photos.py --years 2022 --ufs RR AC            # teste pequeno
    python scripts/build_candidate_photos.py --years 2018 2022 --pause 5         # anos de eleicao geral
    python scripts/build_candidate_photos.py --years 2020 --ufs SP --upload ...  # com envio ao R2

Para cada (ano, UF) baixa `foto_cand<ano>_<UF>_div.zip` (de 0,1 a ~330 MB; devagar, com pausa entre
arquivos e retomada), converte cada foto para JPEG com lado maximo `--max-side` e grava em
`<output-dir>/fotos/<ano>/<uf>/[<ue>/]<sq>.jpg`. O indice (ano, uf, ue, sq) vai para
`data/curated/candidate_photos.parquet`. A API le so o indice e devolve a URL
(`PHOTOS_BASE_URL` + caminho); a imagem e servida pelo CDN, nunca pela API.

Retomavel: um (ano, UF) ja processado (parte do indice gravada) e pulado, a nao ser com --force.
Com --upload, cada (ano, UF) e enviado ao R2 antes de entrar no indice (so fotos que existem no CDN
aparecem no indice, entao o app nunca pede uma foto que da 404).
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.domain.photos import PHOTO_EXTENSION, UF_CODES, parse_photo_filename, photo_object_key  # noqa: E402

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes{year}/fotos/foto_cand{year}_{uf}_div.zip"
DEFAULT_UFS = [uf for uf in UF_CODES if uf != "ZZ"]
INDEX_COLUMNS = ["ano", "uf", "ue", "sq"]
CACHE_CONTROL = "public, max-age=31536000, immutable"
USER_AGENT = "meucandidato-photo-builder/1.0"


# --------------------------------------------------------------------------- imagem
def process_image(data: bytes, max_side: int, quality: int) -> bytes:
    """Reduz a foto para no maximo `max_side` px e grava como JPEG; nunca amplia."""
    try:
        from PIL import Image, ImageOps
    except ImportError as exc:  # pragma: no cover - dependencia opcional do script
        raise SystemExit("Pillow nao instalado: pip install -r requirements-photos.txt") from exc

    with Image.open(io.BytesIO(data)) as source:
        image = ImageOps.exif_transpose(source)
        is_jpeg = (source.format or "").upper() == "JPEG"
        resized = max(image.size) > max_side
        if image.mode in ("RGBA", "LA", "P"):
            rgba = image.convert("RGBA")
            background = Image.new("RGB", rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel("A"))
            image = background
        else:
            image = image.convert("RGB")
        if resized:
            image.thumbnail((max_side, max_side), Image.LANCZOS)
        out = io.BytesIO()
        image.save(out, format="JPEG", quality=quality, optimize=True)
    result = out.getvalue()
    # JPEG que ja e pequeno e leve: mantem o original (evita perda por recompressao).
    if is_jpeg and not resized and len(data) <= len(result):
        return data
    return result


# --------------------------------------------------------------------------- download
def _download(url: str, destination: Path, attempts: int = 4, timeout: int = 60) -> bool:
    """Baixa para `.part` (retomando de onde parou) e so entao renomeia. False se o TSE nao tem o arquivo (404)."""
    partial = destination.with_name(destination.name + ".part")
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            offset = partial.stat().st_size if partial.exists() else 0
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            if offset:
                request.add_header("Range", f"bytes={offset}-")
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                resumed = offset > 0 and getattr(response, "status", 200) == 206
                mode = "ab" if resumed else "wb"
                total = int(response.headers.get("Content-Length") or 0) + (offset if resumed else 0)
                with partial.open(mode) as out:
                    while chunk := response.read(1024 * 1024):
                        out.write(chunk)
            if total and partial.stat().st_size != total:
                raise OSError(f"download incompleto: {partial.stat().st_size} de {total} bytes")
            partial.replace(destination)
            return True
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            if exc.code == 416:  # ja estava completo no .part
                partial.replace(destination)
                return True
            last_error = exc
        except (OSError, TimeoutError) as exc:
            last_error = exc
        print(f"[fotos] tentativa {attempt}/{attempts} falhou: {last_error}", flush=True)
        time.sleep(min(30, 2 ** attempt))
    raise SystemExit(f"[fotos] nao foi possivel baixar {url}: {last_error}")


def ensure_zip(year: int, uf: str, cache_dir: Path) -> Path | None:
    path = cache_dir / f"foto_cand{year}_{uf}_div.zip"
    if path.exists() and not zipfile.is_zipfile(path):
        path.unlink()
    if not path.exists():
        cache_dir.mkdir(parents=True, exist_ok=True)
        print(f"[fotos] baixando {year} {uf} ...", flush=True)
        if not _download(BASE_URL.format(year=year, uf=uf), path):
            return None
    return path


# --------------------------------------------------------------------------- processamento
def process_zip(zip_path: Path, year: int, output_dir: Path, max_side: int, quality: int, prefix: str = "fotos") -> dict:
    """Converte e grava as fotos de um zip; devolve o indice e as estatisticas do (ano, UF)."""
    rows: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    stats = {"files": 0, "photos": 0, "duplicates": 0, "unmatched": 0, "failed": 0, "with_ue": 0,
             "bytes_in": 0, "bytes_out": 0, "unmatched_samples": []}
    with zipfile.ZipFile(zip_path) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            stats["files"] += 1
            key = parse_photo_filename(info.filename)
            if key is None:
                if not info.filename.lower().endswith(".pdf"):
                    stats["unmatched"] += 1
                    if len(stats["unmatched_samples"]) < 3:
                        stats["unmatched_samples"].append(info.filename)
                continue
            if tuple(key) in seen:
                stats["duplicates"] += 1
                continue
            try:
                data = archive.read(info)
                converted = process_image(data, max_side, quality)
            except SystemExit:
                raise
            except Exception:  # noqa: BLE001 - uma imagem corrompida nao pode parar o lote
                stats["failed"] += 1
                continue
            seen.add(tuple(key))
            target = output_dir / photo_object_key(year, key.uf, key.ue, key.sq, prefix)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(converted)
            rows.append({"ano": year, "uf": key.uf, "ue": key.ue, "sq": key.sq})
            stats["photos"] += 1
            stats["with_ue"] += 1 if key.ue else 0
            stats["bytes_in"] += len(data)
            stats["bytes_out"] += len(converted)
    frame = pd.DataFrame(rows, columns=INDEX_COLUMNS).astype({"ano": "int16"})
    return {"index": frame, "stats": stats}


def _part_path(output_dir: Path, year: int, uf: str) -> Path:
    return output_dir / "_index" / f"{year}_{uf}.parquet"


def _absent_marker(output_dir: Path, year: int, uf: str) -> Path:
    return output_dir / "_index" / f"{year}_{uf}.absent"


def _uploaded_marker(output_dir: Path, year: int, uf: str) -> Path:
    return output_dir / "_index" / f"{year}_{uf}.uploaded"


# --------------------------------------------------------------------------- R2
def build_r2_client(account_id: str, access_key_id: str, secret_access_key: str):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
        config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 5, "mode": "standard"}),
    )


def upload_photos(client, bucket: str, output_dir: Path, frame: pd.DataFrame, year: int, prefix: str, workers: int = 8) -> int:
    """Envia as fotos do indice ao R2 (cache imutavel). Levanta erro se qualquer envio falhar."""
    keys = [photo_object_key(year, r.uf, r.ue, r.sq, prefix) for r in frame.itertuples(index=False)]

    def put(key: str) -> None:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=(output_dir / key).read_bytes(),
            ContentType="image/jpeg",
            CacheControl=CACHE_CONTROL,
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for _ in pool.map(put, keys):
            pass
    return len(keys)


# --------------------------------------------------------------------------- indice
def merge_index(output_dir: Path, only_uploaded: bool) -> pd.DataFrame:
    parts = sorted((output_dir / "_index").glob("*.parquet"))
    frames = []
    skipped = 0
    for part in parts:
        year, uf = part.stem.split("_", 1)
        if only_uploaded and not _uploaded_marker(output_dir, int(year), uf).exists():
            skipped += 1
            continue
        frames.append(pd.read_parquet(part))
    if skipped:
        print(f"[fotos] {skipped} parte(s) sem envio ao R2 ficaram fora do indice", flush=True)
    if not frames:
        return pd.DataFrame(columns=INDEX_COLUMNS).astype({"ano": "int16"})
    merged = pd.concat(frames, ignore_index=True).drop_duplicates()
    return merged.sort_values(["ano", "uf", "sq", "ue"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--years", type=int, nargs="*", default=[], help="Anos (2004-2022). Vazio com --index-only.")
    parser.add_argument("--ufs", nargs="+", default=DEFAULT_UFS, help="UFs (e BR). Padrao: todas, inclusive BR.")
    parser.add_argument("--cache-dir", default="data/raw/fotos", help="Onde ficam os zips baixados.")
    parser.add_argument("--output-dir", default="data/photos", help="Imagens convertidas e partes do indice.")
    parser.add_argument("--index", default="data/curated/candidate_photos.parquet")
    parser.add_argument("--max-side", type=int, default=360, help="Lado maximo em px (nao amplia).")
    parser.add_argument("--quality", type=int, default=82)
    parser.add_argument("--pause", type=float, default=3.0, help="Segundos de pausa entre downloads.")
    parser.add_argument("--keep-zips", action="store_true", help="Nao apaga o zip depois de processar.")
    parser.add_argument("--force", action="store_true", help="Reprocessa (ano, UF) ja feitos.")
    parser.add_argument("--index-only", action="store_true", help="So remonta o indice a partir das partes.")
    parser.add_argument("--prefix", default="fotos", help="Prefixo dos objetos (igual a PHOTOS_OBJECT_PREFIX).")
    parser.add_argument("--upload", action="store_true", help="Envia as fotos ao R2; so partes enviadas entram no indice.")
    parser.add_argument("--upload-workers", type=int, default=8, help="Envios simultaneos ao R2 (reduza se aparecer limite de taxa).")
    parser.add_argument("--account-id", default=os.environ.get("R2_ACCOUNT_ID"))
    parser.add_argument("--access-key-id", default=os.environ.get("R2_ACCESS_KEY_ID"))
    parser.add_argument("--secret-access-key", default=os.environ.get("R2_SECRET_ACCESS_KEY"))
    parser.add_argument("--bucket", default=os.environ.get("R2_BUCKET"))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir)
    ufs = [uf.upper() for uf in args.ufs]
    client = None
    if args.upload:
        missing = [n for n in ("account_id", "access_key_id", "secret_access_key", "bucket") if not getattr(args, n)]
        if missing:
            raise SystemExit(f"[fotos] --upload exige credenciais do R2 (faltam: {', '.join(missing)})")
        client = build_r2_client(args.account_id, args.access_key_id, args.secret_access_key)

    if not args.index_only:
        if not args.years:
            raise SystemExit("[fotos] informe --years (ou use --index-only)")
        first_download = True
        for year in args.years:
            for uf in ufs:
                part = _part_path(output_dir, year, uf)
                if not args.force and (_absent_marker(output_dir, year, uf).exists() or part.exists()):
                    # Ja processado; so falta enviar, se pedido.
                    if client and part.exists() and not _uploaded_marker(output_dir, year, uf).exists():
                        sent = upload_photos(client, args.bucket, output_dir, pd.read_parquet(part), year, args.prefix, args.upload_workers)
                        _uploaded_marker(output_dir, year, uf).touch()
                        print(f"[fotos] {year} {uf}: {sent} foto(s) enviadas ao R2", flush=True)
                    continue
                zip_path = cache_dir / f"foto_cand{year}_{uf}_div.zip"
                if not zip_path.exists() and not first_download:
                    time.sleep(args.pause)
                zip_path = ensure_zip(year, uf, cache_dir)
                first_download = False
                (output_dir / "_index").mkdir(parents=True, exist_ok=True)
                if zip_path is None:
                    _absent_marker(output_dir, year, uf).touch()
                    print(f"[fotos] {year} {uf}: sem pacote no TSE", flush=True)
                    continue
                result = process_zip(zip_path, year, output_dir, args.max_side, args.quality, args.prefix)
                stats = result["stats"]
                frame = result["index"]
                frame.to_parquet(part, index=False)
                if client:
                    sent = upload_photos(client, args.bucket, output_dir, frame, year, args.prefix, args.upload_workers)
                    _uploaded_marker(output_dir, year, uf).touch()
                    print(f"[fotos] {year} {uf}: {sent} foto(s) enviadas ao R2", flush=True)
                if not args.keep_zips:
                    zip_path.unlink(missing_ok=True)
                avg_in = stats["bytes_in"] // max(stats["photos"], 1)
                avg_out = stats["bytes_out"] // max(stats["photos"], 1)
                print(
                    f"[fotos] {year} {uf}: {stats['photos']} fotos (media {avg_in} -> {avg_out} bytes), "
                    f"com UE: {stats['with_ue']}, duplicadas: {stats['duplicates']}, falhas: {stats['failed']}, "
                    f"nome nao reconhecido: {stats['unmatched']} {stats['unmatched_samples']}",
                    flush=True,
                )

    index = merge_index(output_dir, only_uploaded=args.upload)
    target = Path(args.index)
    target.parent.mkdir(parents=True, exist_ok=True)
    index.to_parquet(target, index=False, compression="zstd")
    per_year = index.groupby("ano").size().to_dict() if not index.empty else {}
    print(f"[fotos] indice: {len(index)} fotos -> {target} ({target.stat().st_size // 1024} KB)")
    print(f"[fotos] fotos por ano: {per_year}")
    print(f"[fotos] extensao final: .{PHOTO_EXTENSION}; arquivos em {output_dir / args.prefix}")


if __name__ == "__main__":
    main()
