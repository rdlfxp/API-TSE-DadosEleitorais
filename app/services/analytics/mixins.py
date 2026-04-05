from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from app.services.analytics.common import (
    MUNICIPAL_CARGOS,
    MUNICIPIO_ALIAS_MAP,
    NATIONAL_CARGOS,
    PARTIDO_SPECTRUM_MAP,
    UF_CENTROIDS,
    UF_TO_MACROREGIAO,
    logger,
)

import unicodedata


class AnalyticsSupportMixin:
    _NA_LABELS = {
        "",
        "N/A",
        "NA",
        "NONE",
        "NAN",
        "NULL",
        "NULO",
        "NAO INFORMADO",
        "NAO INFORMADA",
        "NÃO INFORMADO",
        "NÃO INFORMADA",
        "NAO DIVULGAVEL",
        "NÃO DIVULGAVEL",
        "NÃO DIVULGÁVEL",
        "NAO DECLARADO",
        "NÃO DECLARADO",
        "SEM INFORMACAO",
        "SEM INFORMAÇÃO",
        "IGNORADO",
    }

    def _normalize_value(self, value: object, uppercase: bool = False) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return ascii_text.upper().strip() if uppercase else ascii_text

    def _normalize_text(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip()

    def _normalize_uf_filter(self, uf: str | None) -> str | None:
        normalized = self._normalize_value(uf or "", uppercase=True)
        if normalized in {"", "BR", "BRASIL"}:
            return None
        return normalized

    def _normalize_ascii_upper(self, text: str) -> str:
        return self._normalize_value(text, uppercase=True)

    def _normalize_municipio_key(self, text: str | None) -> str:
        key = self._normalize_value(text or "", uppercase=True)
        if not key:
            return ""
        return MUNICIPIO_ALIAS_MAP.get(key, key)

    def _normalize_municipio_filter(self, text: str | None) -> str:
        key = self._normalize_municipio_key(text or "")
        return " ".join(key.split())

    def _normalize_municipio_code(self, code: object) -> str:
        raw = str(code or "").strip()
        if not raw:
            return ""
        return "".join(ch for ch in raw if ch.isdigit())

    def _is_national_presidential_scope(
        self,
        *,
        cargo: str | None,
        uf: str | None,
        municipio: str | None,
    ) -> bool:
        cargo_norm = self._normalize_value(cargo or "", uppercase=True)
        municipio_norm = self._normalize_municipio_filter(municipio) if municipio else ""
        return cargo_norm == "PRESIDENTE" and self._normalize_uf_filter(uf) is None and not municipio_norm

    def _is_elected_series(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip().str.upper().str.startswith("ELEITO")

    def _is_elected(self, series: pd.Series) -> pd.Series:
        return self._is_elected_series(series)

    def _cargo_scope(self, cargo: str | None) -> str:
        cargo_up = self._normalize_value(cargo or "", uppercase=True)
        if cargo_up in MUNICIPAL_CARGOS:
            return "municipio"
        if cargo_up in NATIONAL_CARGOS:
            return "nacional"
        return "uf"

    def _party_spectrum(self, partido: str | None) -> str:
        norm = self._normalize_value(partido or "", uppercase=True)
        if not norm:
            return "indefinido"
        return PARTIDO_SPECTRUM_MAP.get(norm, "indefinido")

    def _extract_turno(self, series: pd.Series) -> pd.Series:
        turno_num = pd.to_numeric(series, errors="coerce")
        turno_text = series.astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.strip()
        turno_text_num = pd.to_numeric(turno_text, errors="coerce")
        return turno_num.fillna(turno_text_num).fillna(0).astype(int)

    def _normalize_cor_raca_category(self, value: object) -> str:
        text = self._normalize_value(str(value or ""), uppercase=True)
        if not text or text in {"N/A", "NA", "NULL", "NULO"}:
            return "NAO_INFORMADO"
        if text in {"NAO INFORMADO", "NAO DIVULGAVEL", "NAO DECLARADO", "SEM INFORMACAO", "IGNORADO"}:
            return "NAO_INFORMADO"
        if "BRANCA" in text:
            return "BRANCA"
        if "PRETA" in text or "NEGRA" in text:
            return "PRETA"
        if "PARDA" in text:
            return "PARDA"
        if "AMARELA" in text:
            return "AMARELA"
        if "INDIGENA" in text:
            return "INDIGENA"
        return "NAO_INFORMADO"

    def _is_na_label(self, value: object) -> bool:
        return self._normalize_value(value or "", uppercase=True) in self._NA_LABELS

    def _distribution_uses_candidate_resolution(self, group_by: str) -> bool:
        return group_by in {"status", "genero", "instrucao", "cor_raca", "estado_civil", "ocupacao", "cargo"}

    def _normalize_distribution_label(self, group_by: str, value: object) -> str:
        raw = str(value or "").strip()
        text = self._normalize_value(raw, uppercase=True)
        if not text or text in self._NA_LABELS:
            return "N/A"

        if group_by == "genero":
            if text in {"M", "MASC", "MASCULINO", "MASCULINA"} or text.startswith("MASC"):
                return "MASCULINO"
            if text in {"F", "FEM", "FEMININO", "FEMININA"} or text.startswith("FEM"):
                return "FEMININO"
            return "N/A"

        if group_by == "cor_raca":
            category = self._normalize_cor_raca_category(raw)
            if category == "NAO_INFORMADO":
                return "N/A"
            return category

        return raw if raw else "N/A"

    def _candidate_group_key(self, df: pd.DataFrame) -> pd.Series | None:
        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidate_name = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if not col_candidate_key and not col_candidate_name:
            return None

        base_key = (
            self._normalize_text(df[col_candidate_key]).replace("", pd.NA)
            if col_candidate_key
            else self._normalize_text(df[col_candidate_name]).str.lower().replace("", pd.NA)
        )
        if base_key is None:
            return None

        key = base_key.astype("string")
        for col in ("ANO_ELEICAO", "NR_ANO_ELEICAO", "NR_TURNO", "CD_TURNO", "DS_TURNO", "DS_CARGO", "DS_CARGO_D"):
            resolved = self._pick_col([col])
            if resolved and resolved in df.columns:
                part = self._normalize_text(df[resolved]).replace("", pd.NA).astype("string")
                key = key.str.cat(part, sep="|", na_rep="")
        return key.replace("", pd.NA)

    def _resolve_distribution_labels(self, df: pd.DataFrame, group_by: str, target_col: str) -> pd.Series:
        labels = df[target_col].map(lambda value: self._normalize_distribution_label(group_by, value))
        if df.empty or not self._distribution_uses_candidate_resolution(group_by):
            return labels

        candidate_key = self._candidate_group_key(df)
        if candidate_key is None:
            return labels

        prepared = pd.DataFrame({"_candidate_key": candidate_key, "_label": labels}, index=df.index).dropna(
            subset=["_candidate_key"]
        )
        if prepared.empty:
            return labels

        ranked = prepared.assign(_is_known=(prepared["_label"] != "N/A").astype(int))
        per_candidate = (
            ranked.groupby(["_candidate_key", "_label"], as_index=False)
            .agg(_count=("_label", "size"), _is_known=("_is_known", "max"))
            .sort_values(["_candidate_key", "_is_known", "_count", "_label"], ascending=[True, False, False, True])
            .drop_duplicates(subset=["_candidate_key"], keep="first")
        )
        return per_candidate["_label"]

    def _filter_distribution_items(self, items: list[dict]) -> list[dict]:
        filtered = [item for item in items if not self._is_na_label(item.get("label"))]
        total = float(sum(float(item.get("value") or 0) for item in filtered))
        if total <= 0:
            return []
        return [
            {
                "label": str(item["label"]),
                "value": float(item["value"] or 0),
                "percentage": round((float(item["value"] or 0) / total) * 100, 2),
            }
            for item in filtered
        ]

    def _dedupe_national_presidential_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidato = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if not col_candidato:
            return df.copy()

        candidate_key = (
            self._normalize_text(df[col_candidate_key]).replace("", pd.NA)
            if col_candidate_key
            else self._normalize_text(df[col_candidato]).str.lower().replace("", pd.NA)
        )
        dedup = df.assign(_candidate_key=candidate_key).dropna(subset=["_candidate_key"]).copy()
        if dedup.empty:
            return dedup.drop(columns=["_candidate_key"], errors="ignore")

        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        if col_situacao:
            status_priority = dedup.assign(_eleito_rank=self._is_elected_series(dedup[col_situacao]).astype(int))
            status_priority = (
                status_priority.sort_values(["_candidate_key", "_eleito_rank"], ascending=[True, False])
                .drop_duplicates(subset=["_candidate_key"], keep="first")
                .loc[:, ["_candidate_key", col_situacao]]
            )
        else:
            status_priority = None

        agg_spec: dict[str, str] = {}
        for col in df.columns:
            if col == col_votos:
                agg_spec[col] = "sum"
            elif col != col_situacao:
                agg_spec[col] = "first"

        grouped = dedup.groupby("_candidate_key", as_index=False).agg(agg_spec)
        if col_situacao and status_priority is not None:
            grouped = grouped.merge(status_priority, on="_candidate_key", how="left")
        if col_votos:
            grouped.loc[:, col_votos] = pd.to_numeric(grouped[col_votos], errors="coerce").fillna(0).astype(int)
        return grouped.drop(columns=["_candidate_key"], errors="ignore")

    def _log_municipal_zone_debug(
        self,
        *,
        event: str,
        trace_id: str | None,
        normalized_filters: dict[str, object],
        plan: str | None = None,
        sql: str | None = None,
        sql_params: list[object] | None = None,
        counts: dict[str, object] | None = None,
    ) -> None:
        payload = {
            "event": event,
            "trace_id": trace_id or "n/a",
            "normalized_filters": normalized_filters,
            "counts": counts or {},
        }
        if plan is not None:
            payload["plan"] = plan
        if sql is not None:
            payload["sql"] = sql
        if sql_params is not None:
            payload["sql_params"] = sql_params
        logger.info(json.dumps(payload, ensure_ascii=False))

    def _official_prefeito_totals_by_uf(self, ano: int | None) -> dict[str, int]:
        if ano is None or self._source_path is None:
            return {}
        if ano in self._official_prefeito_totals_cache:
            return self._official_prefeito_totals_cache[ano]

        project_root = self._project_root()
        raw_path = project_root / "raw" / str(ano) / f"consulta_vagas_{ano}_BRASIL.csv"
        if not raw_path.exists():
            self._official_prefeito_totals_cache[ano] = {}
            return {}

        try:
            vagas_df = pd.read_csv(
                raw_path,
                sep=";",
                encoding="latin1",
                usecols=["SG_UF", "NM_UE", "DS_CARGO", "QT_VAGA"],
                low_memory=False,
            )
        except Exception:
            self._official_prefeito_totals_cache[ano] = {}
            return {}

        df = vagas_df.assign(
            _uf=self._normalize_text(vagas_df["SG_UF"]).str.upper().replace("", pd.NA),
            _municipio=self._normalize_text(vagas_df["NM_UE"]),
            _cargo=self._normalize_text(vagas_df["DS_CARGO"]).str.upper(),
            _qt_vaga=pd.to_numeric(vagas_df["QT_VAGA"], errors="coerce").fillna(0),
        )
        df = df[(df["_cargo"] == "PREFEITO") & (df["_qt_vaga"] >= 1)]
        df = df.dropna(subset=["_uf"]).copy()
        if df.empty:
            self._official_prefeito_totals_cache[ano] = {}
            return {}

        df = df.assign(_municipio_key=df["_municipio"].apply(self._normalize_municipio_key))
        df = df[df["_municipio_key"] != ""]
        totals = df.drop_duplicates(subset=["_uf", "_municipio_key"]).groupby("_uf").size().to_dict()
        result = {str(uf): int(total) for uf, total in totals.items()}
        self._official_prefeito_totals_cache[ano] = result
        return result

    def _project_root(self) -> Path:
        if self._source_path is None:
            return Path.cwd()
        return self._source_path.parent.parent if self._source_path.parent.name == "curated" else self._source_path.parent

    def _zone_geometry_index(self) -> dict[str, object]:
        if self._zone_geometry_index_cache is not None:
            return self._zone_geometry_index_cache

        root = self._project_root()
        candidates: list[Path] = []
        for pattern in [
            "data/geo/**/*.geojson",
            "data/raw/**/*zona*.geojson",
            "data/raw/**/*zone*.geojson",
        ]:
            candidates.extend(root.glob(pattern))
        candidates = sorted({path for path in candidates if path.is_file()})

        index: dict[str, object] = {}
        zone_keys = {"zone_id", "zona_id", "nr_zona", "cd_zona", "sq_zona", "zona", "zone", "id_zona", "id"}
        uf_keys = {"uf", "sg_uf", "state"}
        city_keys = {"city", "municipio", "nm_municipio", "nm_ue"}

        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            features = payload.get("features", []) if isinstance(payload, dict) else []
            for feature in features:
                if not isinstance(feature, dict):
                    continue
                geometry = feature.get("geometry")
                if not geometry:
                    continue
                props = feature.get("properties") or {}
                if not isinstance(props, dict):
                    props = {}
                norm_props = {self._normalize_value(str(k), uppercase=True): v for k, v in props.items()}

                zone_val = ""
                for key in zone_keys:
                    prop_key = self._normalize_value(key, uppercase=True)
                    value = norm_props.get(prop_key)
                    if str(value or "").strip():
                        zone_val = self._normalize_value(str(value), uppercase=True)
                        break
                if not zone_val:
                    continue

                uf_val = ""
                for key in uf_keys:
                    prop_key = self._normalize_value(key, uppercase=True)
                    value = norm_props.get(prop_key)
                    if str(value or "").strip():
                        uf_val = self._normalize_value(str(value), uppercase=True)
                        break

                city_val = ""
                for key in city_keys:
                    prop_key = self._normalize_value(key, uppercase=True)
                    value = norm_props.get(prop_key)
                    if str(value or "").strip():
                        city_val = self._normalize_municipio_key(str(value))
                        break

                index.setdefault(zone_val, geometry)
                if uf_val:
                    index.setdefault(f"{uf_val}|{zone_val}", geometry)
                    if city_val:
                        index.setdefault(f"{uf_val}|{city_val}|{zone_val}", geometry)

        self._zone_geometry_index_cache = index
        return index

    def _municipality_coord_index(self) -> dict[str, tuple[float, float]]:
        if self._municipality_coord_index_cache is not None:
            return self._municipality_coord_index_cache

        root = self._project_root()
        candidates: list[Path] = []
        for pattern in [
            "data/geo/**/*.geojson",
            "data/geo/**/*municip*.csv",
            "data/geo/**/*municip*.parquet",
            "data/geo/**/*city*.csv",
            "data/geo/**/*city*.parquet",
        ]:
            candidates.extend(root.glob(pattern))
        candidates = sorted({path for path in candidates if path.is_file()})

        index: dict[str, tuple[float, float]] = {}
        for path in candidates:
            suffix = path.suffix.lower()
            if suffix == ".geojson":
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                features = payload.get("features", []) if isinstance(payload, dict) else []
                for feature in features:
                    if not isinstance(feature, dict):
                        continue
                    geometry = feature.get("geometry")
                    lat, lng = self._geometry_centroid(geometry)
                    lat, lng = self._coerce_valid_coords(lat, lng)
                    if lat is None or lng is None:
                        continue
                    props = feature.get("properties") or {}
                    if not isinstance(props, dict):
                        props = {}
                    norm_props = {self._normalize_value(str(k), uppercase=True): v for k, v in props.items()}
                    self._index_municipality_coords(index, norm_props, lat, lng)
                continue

            try:
                frame = pd.read_parquet(path) if suffix == ".parquet" else pd.read_csv(path, low_memory=False)
            except Exception:
                continue
            if frame.empty:
                continue
            norm_cols = {str(col): self._normalize_value(str(col), uppercase=True) for col in frame.columns}
            col_by_norm = {v: k for k, v in norm_cols.items()}
            code_col = next((col_by_norm[key] for key in ["CD_MUNICIPIO", "COD_MUNICIPIO", "MUNICIPIO_ID", "MUNICIPIO_CODE", "IBGE", "ID"] if key in col_by_norm), None)
            uf_col = next((col_by_norm[key] for key in ["SG_UF", "UF", "STATE"] if key in col_by_norm), None)
            city_col = next((col_by_norm[key] for key in ["NM_MUNICIPIO", "NM_UE", "MUNICIPIO", "MUNICIPALITY", "CITY", "NAME"] if key in col_by_norm), None)
            lat_col = next((col_by_norm[key] for key in ["LATITUDE", "LAT", "VL_LATITUDE"] if key in col_by_norm), None)
            lng_col = next((col_by_norm[key] for key in ["LONGITUDE", "LON", "LNG", "VL_LONGITUDE"] if key in col_by_norm), None)
            if not lat_col or not lng_col:
                continue
            for _, row in frame.iterrows():
                lat, lng = self._coerce_valid_coords(row.get(lat_col), row.get(lng_col))
                if lat is None or lng is None:
                    continue
                props = {}
                if code_col:
                    props["CD_MUNICIPIO"] = row.get(code_col)
                if uf_col:
                    props["SG_UF"] = row.get(uf_col)
                if city_col:
                    props["NM_MUNICIPIO"] = row.get(city_col)
                norm_props = {self._normalize_value(str(k), uppercase=True): v for k, v in props.items()}
                self._index_municipality_coords(index, norm_props, lat, lng)

        self._municipality_coord_index_cache = index
        return index

    def _index_municipality_coords(
        self,
        index: dict[str, tuple[float, float]],
        norm_props: dict[str, object],
        lat: float,
        lng: float,
    ) -> None:
        uf_val = ""
        for key in ["SG_UF", "UF", "STATE"]:
            prop_key = self._normalize_value(key, uppercase=True)
            if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                uf_val = self._normalize_value(str(norm_props[prop_key]), uppercase=True)
                break

        city_val = ""
        for key in ["NM_MUNICIPIO", "NM_UE", "MUNICIPIO", "MUNICIPALITY", "CITY", "NAME"]:
            prop_key = self._normalize_value(key, uppercase=True)
            if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                city_val = self._normalize_municipio_key(str(norm_props[prop_key]))
                break

        code_val = ""
        for key in ["CD_MUNICIPIO", "COD_MUNICIPIO", "MUNICIPIO_ID", "MUNICIPIO_CODE", "IBGE", "ID"]:
            prop_key = self._normalize_value(key, uppercase=True)
            if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                code_val = self._normalize_municipio_code(norm_props[prop_key])
                if code_val:
                    break

        if code_val:
            index.setdefault(f"CODE:{code_val}", (lat, lng))
        if uf_val and city_val:
            index.setdefault(f"{uf_val}|{city_val}", (lat, lng))
        if city_val:
            index.setdefault(city_val, (lat, lng))

    def _resolve_municipality_coords(
        self,
        city: str | None,
        uf: str | None,
        code: object | None,
        lat: object,
        lng: object,
    ) -> tuple[float | None, float | None]:
        valid_lat, valid_lng = self._coerce_valid_coords(lat, lng)
        if valid_lat is not None and valid_lng is not None:
            return valid_lat, valid_lng

        code_key = self._normalize_municipio_code(code)
        uf_key = self._normalize_value(uf or "", uppercase=True)
        city_key = self._normalize_municipio_key(city or "")
        index = self._municipality_coord_index()

        for key in [
            f"CODE:{code_key}" if code_key else "",
            f"{uf_key}|{city_key}" if uf_key and city_key else "",
            city_key,
        ]:
            if key and key in index:
                return index[key]

        if uf_key in UF_CENTROIDS:
            return UF_CENTROIDS[uf_key]
        return None, None

    def _region_clusters_from_points(self, points: list[dict]) -> list[dict]:
        uf_acc: dict[str, dict[str, float | int | str]] = {}
        for point in points:
            uf = str(point.get("uf") or "").strip().upper()
            lat = point.get("lat")
            lng = point.get("lng")
            votes = int(point.get("votes") or 0)
            if not uf or votes <= 0 or lat is None or lng is None:
                continue
            bucket = uf_acc.setdefault(uf, {"votes": 0, "points": 0, "sum_lat": 0.0, "sum_lng": 0.0})
            bucket["votes"] = int(bucket["votes"]) + votes
            bucket["points"] = int(bucket["points"]) + 1
            bucket["sum_lat"] = float(bucket["sum_lat"]) + (float(lat) * votes)
            bucket["sum_lng"] = float(bucket["sum_lng"]) + (float(lng) * votes)

        uf_clusters: list[dict] = []
        for uf, bucket in uf_acc.items():
            votes = int(bucket["votes"])
            if votes <= 0:
                continue
            uf_clusters.append(
                {
                    "region_type": "uf",
                    "region_key": uf,
                    "region_label": uf,
                    "votes": votes,
                    "points": int(bucket["points"]),
                    "lat": round(float(bucket["sum_lat"]) / votes, 6),
                    "lng": round(float(bucket["sum_lng"]) / votes, 6),
                }
            )

        macro_acc: dict[str, dict[str, float | int | str]] = {}
        for cluster in uf_clusters:
            uf = str(cluster["region_key"])
            macro = UF_TO_MACROREGIAO.get(uf, "Indefinida")
            votes = int(cluster["votes"])
            bucket = macro_acc.setdefault(macro, {"votes": 0, "points": 0, "sum_lat": 0.0, "sum_lng": 0.0})
            bucket["votes"] = int(bucket["votes"]) + votes
            bucket["points"] = int(bucket["points"]) + int(cluster["points"])
            bucket["sum_lat"] = float(bucket["sum_lat"]) + (float(cluster["lat"]) * votes)
            bucket["sum_lng"] = float(bucket["sum_lng"]) + (float(cluster["lng"]) * votes)

        macro_clusters: list[dict] = []
        for macro, bucket in macro_acc.items():
            votes = int(bucket["votes"])
            if votes <= 0:
                continue
            macro_clusters.append(
                {
                    "region_type": "macroregiao",
                    "region_key": self._normalize_value(macro, uppercase=True),
                    "region_label": macro,
                    "votes": votes,
                    "points": int(bucket["points"]),
                    "lat": round(float(bucket["sum_lat"]) / votes, 6),
                    "lng": round(float(bucket["sum_lng"]) / votes, 6),
                }
            )

        return sorted(uf_clusters + macro_clusters, key=lambda item: int(item["votes"]), reverse=True)

    def _resolve_zone_geometry(self, zone_id: str, city: str | None, uf: str | None, lat: float, lng: float) -> object | None:
        zone_key = self._normalize_value(zone_id, uppercase=True)
        uf_key = self._normalize_value(uf or "", uppercase=True)
        city_key = self._normalize_municipio_key(city or "")
        index = self._zone_geometry_index()
        for key in [
            f"{uf_key}|{city_key}|{zone_key}" if uf_key and city_key else "",
            f"{uf_key}|{zone_key}" if uf_key else "",
            zone_key,
        ]:
            if key and key in index:
                return index[key]
        if pd.notna(lat) and pd.notna(lng):
            return {"type": "Point", "coordinates": [float(lng), float(lat)]}
        return None

    def _geometry_centroid(self, geometry: object) -> tuple[float | None, float | None]:
        if not isinstance(geometry, dict):
            return None, None

        geom_type = str(geometry.get("type") or "").strip().upper()
        coords = geometry.get("coordinates")
        if geom_type == "POINT" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
            try:
                return float(coords[1]), float(coords[0])
            except (TypeError, ValueError):
                return None, None

        points: list[tuple[float, float]] = []

        def collect(value: object) -> None:
            if isinstance(value, (list, tuple)):
                if len(value) >= 2:
                    try:
                        points.append((float(value[1]), float(value[0])))
                        return
                    except (TypeError, ValueError):
                        pass
                for item in value:
                    collect(item)

        collect(coords)
        if not points:
            return None, None

        lat = sum(p[0] for p in points) / len(points)
        lng = sum(p[1] for p in points) / len(points)
        return float(lat), float(lng)

    def _coerce_valid_coords(self, lat: object, lng: object) -> tuple[float | None, float | None]:
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except (TypeError, ValueError):
            return None, None
        if not math.isfinite(lat_f) or not math.isfinite(lng_f):
            return None, None
        if not (-90.0 <= lat_f <= 90.0) or not (-180.0 <= lng_f <= 180.0):
            return None, None
        if abs(lat_f) < 1e-9 and abs(lng_f) < 1e-9:
            return None, None
        return lat_f, lng_f
