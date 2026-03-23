#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path

import pandas as pd


def _pick_col(columns: list[str], options: list[str]) -> str | None:
    normalized = {str(col).strip().upper(): col for col in columns}
    for opt in options:
        key = opt.strip().upper()
        if key in normalized:
            return normalized[key]
    return None


def _norm_city(value: object) -> str:
    return str(value or "").strip().upper()


def _norm_code(value: object) -> str:
    raw = str(value or "").strip()
    return "".join(ch for ch in raw if ch.isdigit())


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def _valid_coord(lat: object, lng: object) -> tuple[float | None, float | None]:
    try:
        lat_f = float(lat)
        lng_f = float(lng)
    except (TypeError, ValueError):
        return None, None
    if not math.isfinite(lat_f) or not math.isfinite(lng_f):
        return None, None
    if not (-90.0 <= lat_f <= 90.0) or not (-180.0 <= lng_f <= 180.0):
        return None, None
    return lat_f, lng_f


def _load_geocode_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=["SG_UF", "_city_key", "NM_MUNICIPIO", "LATITUDE", "LONGITUDE", "QUERY", "STATUS"]
        )
    cache = pd.read_csv(path, low_memory=False)
    required = {"SG_UF", "_city_key", "NM_MUNICIPIO", "LATITUDE", "LONGITUDE", "QUERY", "STATUS"}
    missing = required - set(cache.columns)
    for col in missing:
        cache[col] = pd.NA
    return cache[list(required)].copy()


def _save_geocode_cache(path: Path, cache: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = cache.copy()
    out = out.sort_values(["SG_UF", "_city_key"]).drop_duplicates(subset=["SG_UF", "_city_key"], keep="last")
    out.to_csv(path, index=False)


def _apply_geocode(
    base: pd.DataFrame,
    cache_path: Path,
    user_agent: str,
    timeout_seconds: int,
    delay_seconds: float,
    max_rows: int,
    retry_miss: bool,
) -> pd.DataFrame:
    try:
        from geopy.extra.rate_limiter import RateLimiter
        from geopy.geocoders import Nominatim
    except Exception as exc:
        raise RuntimeError(
            "geopy nao instalado. Instale com: pip install geopy"
        ) from exc

    logging.getLogger("geopy").setLevel(logging.ERROR)
    logging.getLogger("geopy.extra.rate_limiter").setLevel(logging.ERROR)

    geolocator = Nominatim(user_agent=user_agent, timeout=timeout_seconds)
    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=delay_seconds,
        max_retries=2,
        swallow_exceptions=True,
        error_wait_seconds=max(delay_seconds, 1.0),
    )

    cache = _load_geocode_cache(cache_path)
    cache = cache.assign(
        SG_UF=cache["SG_UF"].astype(str).str.strip().str.upper(),
        _city_key=cache["_city_key"].astype(str).str.strip().str.upper(),
        LATITUDE=pd.to_numeric(cache["LATITUDE"], errors="coerce"),
        LONGITUDE=pd.to_numeric(cache["LONGITUDE"], errors="coerce"),
    )
    cache_idx: dict[tuple[str, str], tuple[float | None, float | None, str, str]] = {}
    for _, row in cache.iterrows():
        key = (str(row["SG_UF"]), str(row["_city_key"]))
        lat, lng = _valid_coord(row["LATITUDE"], row["LONGITUDE"])
        cache_idx[key] = (lat, lng, str(row.get("QUERY") or ""), str(row.get("STATUS") or ""))

    # First, hydrate from cache entries already resolved.
    missing_mask = base["LATITUDE"].isna() | base["LONGITUDE"].isna()
    for idx in base.index[missing_mask].tolist():
        uf = str(base.at[idx, "SG_UF"] or "").strip().upper()
        city_key = _norm_city(base.at[idx, "NM_MUNICIPIO"])
        cached = cache_idx.get((uf, city_key))
        if not cached:
            continue
        lat, lng, _, _ = cached
        if lat is not None and lng is not None:
            base.at[idx, "LATITUDE"] = lat
            base.at[idx, "LONGITUDE"] = lng

    # Build geocode queue, skipping hard misses unless retry is requested.
    missing_mask = base["LATITUDE"].isna() | base["LONGITUDE"].isna()
    pending_indices: list[int] = []
    for idx in base.index[missing_mask].tolist():
        uf = str(base.at[idx, "SG_UF"] or "").strip().upper()
        city_key = _norm_city(base.at[idx, "NM_MUNICIPIO"])
        cached = cache_idx.get((uf, city_key))
        if cached:
            lat, lng, _, status = cached
            if lat is not None and lng is not None:
                continue
            if (status or "").upper() == "MISS" and not retry_miss:
                continue
        pending_indices.append(idx)

    indices = pending_indices[:max_rows] if max_rows > 0 else pending_indices

    cache_records: list[dict[str, object]] = cache.to_dict(orient="records")
    if not indices:
        _save_geocode_cache(cache_path, pd.DataFrame(cache_records))
        print("geocode_progress=0/0")
        return base
    for pos, idx in enumerate(indices, start=1):
        uf = str(base.at[idx, "SG_UF"] or "").strip().upper()
        city = str(base.at[idx, "NM_MUNICIPIO"] or "").strip()
        city_key = _norm_city(city)
        key = (uf, city_key)
        cached = cache_idx.get(key)
        if cached:
            lat, lng, _, status = cached
            if lat is not None and lng is not None:
                base.at[idx, "LATITUDE"] = lat
                base.at[idx, "LONGITUDE"] = lng
                continue
            if status == "MISS":
                continue

        query_candidates = [
            f"{city}, {uf}, Brasil",
            f"Municipio de {city}, {uf}, Brasil",
            f"{city}, Brasil",
        ]
        found_lat: float | None = None
        found_lng: float | None = None
        used_query = ""
        for query in query_candidates:
            location = geocode(query, language="pt", country_codes="br", addressdetails=True)
            if location is None:
                continue
            lat, lng = _valid_coord(location.latitude, location.longitude)
            if lat is None or lng is None:
                continue
            found_lat, found_lng = lat, lng
            used_query = query
            break

        status = "OK" if found_lat is not None and found_lng is not None else "MISS"
        if found_lat is not None and found_lng is not None:
            base.at[idx, "LATITUDE"] = found_lat
            base.at[idx, "LONGITUDE"] = found_lng

        cache_record = {
            "SG_UF": uf,
            "_city_key": city_key,
            "NM_MUNICIPIO": city,
            "LATITUDE": found_lat,
            "LONGITUDE": found_lng,
            "QUERY": used_query,
            "STATUS": status,
        }
        cache_records.append(cache_record)
        cache_idx[key] = (found_lat, found_lng, used_query, status)

        if pos % 100 == 0 or pos == len(indices):
            _save_geocode_cache(cache_path, pd.DataFrame(cache_records))
            print(f"geocode_progress={pos}/{len(indices)}")

    _save_geocode_cache(cache_path, pd.DataFrame(cache_records))
    return base


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepara arquivo de coordenadas de municipios para o vote-map."
    )
    parser.add_argument(
        "--analytics",
        default="data/curated/analytics.csv",
        help="Base consolidada de analytics (.csv ou .parquet).",
    )
    parser.add_argument(
        "--coords-input",
        default=None,
        help="CSV/Parquet opcional com coordenadas para merge (ex.: fonte IBGE).",
    )
    parser.add_argument(
        "--output",
        default="data/geo/municipios_centroids.csv",
        help="Saida final com coordenadas de municipios.",
    )
    parser.add_argument(
        "--missing-report",
        default="data/geo/municipios_missing_coords.csv",
        help="Relatorio de municipios sem coordenadas.",
    )
    parser.add_argument(
        "--geocode-missing",
        action="store_true",
        help="Geocodifica municipios sem coordenadas usando geopy/Nominatim.",
    )
    parser.add_argument(
        "--geocode-cache",
        default="data/geo/geocode_cache.csv",
        help="Cache local de geocodificacao para retomada.",
    )
    parser.add_argument(
        "--geocode-user-agent",
        default="meucandidato-geocoder",
        help="User-Agent enviado ao geocoder.",
    )
    parser.add_argument(
        "--geocode-timeout-seconds",
        type=int,
        default=15,
        help="Timeout por requisicao de geocodificacao.",
    )
    parser.add_argument(
        "--geocode-delay-seconds",
        type=float,
        default=1.1,
        help="Delay minimo entre chamadas para respeitar limite do provider.",
    )
    parser.add_argument(
        "--geocode-max-rows",
        type=int,
        default=0,
        help="Limita quantidade de municipios geocodificados por execucao (0=todos).",
    )
    parser.add_argument(
        "--retry-miss",
        action="store_true",
        help="Reprocessa municipios marcados como MISS no cache.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    analytics_path = Path(args.analytics)
    if not analytics_path.exists():
        raise FileNotFoundError(f"Analytics nao encontrado: {analytics_path}")

    df = _read_table(analytics_path)
    col_uf = _pick_col(df.columns.tolist(), ["SG_UF", "UF"])
    col_city = _pick_col(df.columns.tolist(), ["NM_UE", "NM_MUNICIPIO", "MUNICIPIO"])
    col_code = _pick_col(df.columns.tolist(), ["CD_MUNICIPIO", "COD_MUNICIPIO", "ID_MUNICIPIO", "IBGE"])
    col_cargo = _pick_col(df.columns.tolist(), ["DS_CARGO", "CARGO"])
    if not col_uf or not col_city:
        raise RuntimeError("Nao foi possivel identificar colunas de UF e municipio no analytics.")

    if col_cargo:
        cargo_norm = df[col_cargo].astype(str).str.strip().str.upper()
        municipal_mask = cargo_norm.isin({"PREFEITO", "VICE-PREFEITO", "VEREADOR"})
        if municipal_mask.any():
            df = df[municipal_mask].copy()

    base = df.assign(
        SG_UF=df[col_uf].astype(str).str.strip().str.upper(),
        NM_MUNICIPIO=df[col_city].astype(str).str.strip(),
        CD_MUNICIPIO=(df[col_code].astype(str).str.strip() if col_code else ""),
    )
    base = base.assign(
        _uf=base["SG_UF"],
        _city_key=base["NM_MUNICIPIO"].apply(_norm_city),
        _code_key=base["CD_MUNICIPIO"].apply(_norm_code),
    )
    base = base[(base["_uf"] != "") & (base["_city_key"] != "")]
    base_with_code = base[base["_code_key"] != ""].copy()
    base_without_code = base[base["_code_key"] == ""].copy()

    if not base_with_code.empty:
        stats = (
            base_with_code.groupby(["_uf", "_code_key", "NM_MUNICIPIO"], as_index=False)
            .size()
            .sort_values(["_uf", "_code_key", "size", "NM_MUNICIPIO"], ascending=[True, True, False, True])
        )
        best_by_code = stats.drop_duplicates(subset=["_uf", "_code_key"], keep="first")
        code_to_original = (
            base_with_code.assign(_code_key=base_with_code["CD_MUNICIPIO"].apply(_norm_code))
            .sort_values(["_uf", "_code_key"])
            .drop_duplicates(subset=["_uf", "_code_key"], keep="first")
            [["_uf", "_code_key", "CD_MUNICIPIO"]]
        )
        base_with_code = best_by_code.merge(code_to_original, on=["_uf", "_code_key"], how="left")[
            ["CD_MUNICIPIO", "_uf", "NM_MUNICIPIO"]
        ].rename(columns={"_uf": "SG_UF"})
    else:
        base_with_code = pd.DataFrame(columns=["CD_MUNICIPIO", "SG_UF", "NM_MUNICIPIO"])

    if not base_without_code.empty:
        base_without_code = (
            base_without_code.sort_values(["_uf", "_city_key"])
            .drop_duplicates(subset=["_uf", "_city_key"], keep="first")
            [["CD_MUNICIPIO", "_uf", "NM_MUNICIPIO"]]
            .rename(columns={"_uf": "SG_UF"})
        )
    else:
        base_without_code = pd.DataFrame(columns=["CD_MUNICIPIO", "SG_UF", "NM_MUNICIPIO"])

    base = (
        pd.concat([base_with_code, base_without_code], ignore_index=True)
        .sort_values(["SG_UF", "NM_MUNICIPIO"])
        .drop_duplicates(subset=["SG_UF", "NM_MUNICIPIO"], keep="first")
        .copy()
    )
    base["LATITUDE"] = pd.NA
    base["LONGITUDE"] = pd.NA

    if args.coords_input:
        coords_path = Path(args.coords_input)
        if not coords_path.exists():
            raise FileNotFoundError(f"Arquivo de coordenadas nao encontrado: {coords_path}")
        coords = _read_table(coords_path)
        c_code = _pick_col(coords.columns.tolist(), ["CD_MUNICIPIO", "COD_MUNICIPIO", "ID_MUNICIPIO", "IBGE"])
        c_uf = _pick_col(coords.columns.tolist(), ["SG_UF", "UF"])
        c_city = _pick_col(coords.columns.tolist(), ["NM_MUNICIPIO", "NM_UE", "MUNICIPIO", "CITY", "NAME"])
        c_lat = _pick_col(coords.columns.tolist(), ["LATITUDE", "LAT", "VL_LATITUDE"])
        c_lng = _pick_col(coords.columns.tolist(), ["LONGITUDE", "LON", "LNG", "VL_LONGITUDE"])
        if not c_lat or not c_lng:
            raise RuntimeError("Arquivo de coordenadas sem colunas de latitude/longitude.")

        coords = coords.assign(
            SG_UF=(coords[c_uf].astype(str).str.strip().str.upper() if c_uf else ""),
            NM_MUNICIPIO=(coords[c_city].astype(str).str.strip() if c_city else ""),
            CD_MUNICIPIO=(coords[c_code].astype(str).str.strip() if c_code else ""),
            LATITUDE=pd.to_numeric(coords[c_lat], errors="coerce"),
            LONGITUDE=pd.to_numeric(coords[c_lng], errors="coerce"),
        )
        coords = coords.assign(
            _uf=coords["SG_UF"],
            _city_key=coords["NM_MUNICIPIO"].apply(_norm_city),
            _code_key=coords["CD_MUNICIPIO"].apply(_norm_code),
        )
        coords = coords.dropna(subset=["LATITUDE", "LONGITUDE"]).copy()

        by_code = coords[coords["_code_key"] != ""].drop_duplicates(subset=["_code_key"], keep="first")
        merged = base.assign(
            _code_key=base["CD_MUNICIPIO"].apply(_norm_code),
            _city_key=base["NM_MUNICIPIO"].apply(_norm_city),
            _uf=base["SG_UF"],
        ).merge(
            by_code[["_code_key", "LATITUDE", "LONGITUDE"]],
            on="_code_key",
            how="left",
            suffixes=("", "_code"),
        )

        unresolved = merged["LATITUDE"].isna() | merged["LONGITUDE"].isna()
        if unresolved.any():
            by_name = coords[(coords["_city_key"] != "") & (coords["_uf"] != "")].drop_duplicates(
                subset=["_uf", "_city_key"], keep="first"
            )
            fallback = merged.loc[unresolved, ["_uf", "_city_key"]].merge(
                by_name[["_uf", "_city_key", "LATITUDE", "LONGITUDE"]],
                on=["_uf", "_city_key"],
                how="left",
            )
            merged.loc[unresolved, "LATITUDE"] = merged.loc[unresolved, "LATITUDE"].fillna(fallback["LATITUDE"].values)
            merged.loc[unresolved, "LONGITUDE"] = merged.loc[unresolved, "LONGITUDE"].fillna(fallback["LONGITUDE"].values)

        base = merged[["CD_MUNICIPIO", "SG_UF", "NM_MUNICIPIO", "LATITUDE", "LONGITUDE"]].copy()

    if args.geocode_missing:
        base = _apply_geocode(
            base=base,
            cache_path=Path(args.geocode_cache),
            user_agent=args.geocode_user_agent,
            timeout_seconds=int(args.geocode_timeout_seconds),
            delay_seconds=float(args.geocode_delay_seconds),
            max_rows=int(args.geocode_max_rows),
            retry_miss=bool(args.retry_miss),
        )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(output_path, index=False)

    missing = base[base["LATITUDE"].isna() | base["LONGITUDE"].isna()].copy()
    missing_path = Path(args.missing_report)
    missing_path.parent.mkdir(parents=True, exist_ok=True)
    missing.to_csv(missing_path, index=False)

    total = len(base)
    missing_total = len(missing)
    resolved = total - missing_total
    print(f"municipios_total={total}")
    print(f"municipios_com_coords={resolved}")
    print(f"municipios_sem_coords={missing_total}")
    print(f"output={output_path}")
    print(f"missing_report={missing_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
