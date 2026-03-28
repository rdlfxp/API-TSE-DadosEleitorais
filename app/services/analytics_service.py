from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import math
from pathlib import Path
from typing import Iterable
import unicodedata

import pandas as pd


ELEITO_LABELS = {
    "ELEITO",
    "ELEITO POR QP",
    "ELEITO POR MEDIA",
    "MÉDIA",
}

MUNICIPAL_CARGOS = {
    "PREFEITO",
    "VICE-PREFEITO",
    "VEREADOR",
}

NATIONAL_CARGOS = {
    "PRESIDENTE",
    "VICE-PRESIDENTE",
}

PARTIDO_SPECTRUM_MAP = {
    "PCDOB": "esquerda",
    "PC DO B": "esquerda",
    "PDT": "esquerda",
    "PSB": "esquerda",
    "PSOL": "esquerda",
    "PSTU": "esquerda",
    "PT": "esquerda",
    "PV": "esquerda",
    "REDE": "esquerda",
    "PCB": "esquerda",
    "UP": "esquerda",
    "AGIR": "centro",
    "AVANTE": "centro",
    "CIDADANIA": "centro",
    "DC": "centro",
    "MDB": "centro",
    "MOBILIZA": "centro",
    "PODEMOS": "centro",
    "PODE": "centro",
    "PMB": "centro",
    "PMN": "centro",
    "PSD": "centro",
    "SOLIDARIEDADE": "centro",
    "PROS": "centro",
    "DEM": "centro",
    "PHS": "direita",
    "PTC": "direita",
    "NOVO": "direita",
    "PL": "direita",
    "PP": "direita",
    "PRD": "direita",
    "PRTB": "direita",
    "PSDB": "direita",
    "REPUBLICANOS": "direita",
    "PRB": "direita",
    "PTB": "direita",
    "PSC": "direita",
    "PSL": "direita",
    "PATRIOTA": "direita",
    "UNIAO": "direita",
    "UNIAO BRASIL": "direita",
}

MUNICIPIO_ALIAS_MAP = {
    "SANTA IZABEL DO PARA": "SANTA ISABEL DO PARA",
    "MUNHOZ DE MELLO": "MUNHOZ DE MELO",
}

COR_RACA_CATEGORY_ORDER = [
    "BRANCA",
    "PRETA",
    "PARDA",
    "AMARELA",
    "INDIGENA",
    "NAO_INFORMADO",
]

COR_RACA_CATEGORY_LABELS = {
    "BRANCA": "Branca",
    "PRETA": "Preta",
    "PARDA": "Parda",
    "AMARELA": "Amarela",
    "INDIGENA": "Indígena",
    "NAO_INFORMADO": "Não informado",
}

SUPPORTED_ANALYTICS_YEARS = {2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024}
logger = logging.getLogger("api.meucandidato")

UF_TO_MACROREGIAO = {
    "AC": "Norte",
    "AL": "Nordeste",
    "AP": "Norte",
    "AM": "Norte",
    "BA": "Nordeste",
    "CE": "Nordeste",
    "DF": "Centro-Oeste",
    "ES": "Sudeste",
    "GO": "Centro-Oeste",
    "MA": "Nordeste",
    "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "MG": "Sudeste",
    "PA": "Norte",
    "PB": "Nordeste",
    "PR": "Sul",
    "PE": "Nordeste",
    "PI": "Nordeste",
    "RJ": "Sudeste",
    "RN": "Nordeste",
    "RS": "Sul",
    "RO": "Norte",
    "RR": "Norte",
    "SC": "Sul",
    "SP": "Sudeste",
    "SE": "Nordeste",
    "TO": "Norte",
}


def _safe_percent(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _impact_category(percentual_no_total_do_candidato: float) -> str:
    # Regra de impacto municipal:
    # Alto: >= 1.00 | Médio: >= 0.30 e < 1.00 | Baixo: < 0.30
    if percentual_no_total_do_candidato >= 1.0:
        return "Alto"
    if percentual_no_total_do_candidato >= 0.3:
        return "Médio"
    return "Baixo"

UF_CENTROIDS = {
    "AC": (-9.0238, -70.8120),
    "AL": (-9.6482, -35.7089),
    "AP": (1.4141, -51.7701),
    "AM": (-3.4168, -65.8561),
    "BA": (-12.5797, -41.7007),
    "CE": (-5.4984, -39.3206),
    "DF": (-15.7998, -47.8645),
    "ES": (-19.1834, -40.3089),
    "GO": (-15.8270, -49.8362),
    "MA": (-4.9609, -45.2744),
    "MT": (-12.6819, -56.9211),
    "MS": (-20.7722, -54.7852),
    "MG": (-18.5122, -44.5550),
    "PA": (-3.4168, -52.8430),
    "PB": (-7.2399, -36.7820),
    "PR": (-24.8949, -51.5506),
    "PE": (-8.8137, -36.9541),
    "PI": (-7.7183, -42.7289),
    "RJ": (-22.9099, -43.2096),
    "RN": (-5.4026, -36.9541),
    "RS": (-30.0346, -51.2177),
    "RO": (-10.8333, -63.9000),
    "RR": (2.7376, -62.0751),
    "SC": (-27.2423, -50.2189),
    "SP": (-22.19, -48.79),
    "SE": (-10.5741, -37.3857),
    "TO": (-10.1753, -48.2982),
}


@dataclass
class AnalyticsService:
    dataframe: pd.DataFrame
    default_top_n: int
    max_top_n: int
    _source_path: Path | None = field(default=None, init=False, repr=False)
    _official_prefeito_totals_cache: dict[int, dict[str, int]] = field(default_factory=dict, init=False, repr=False)
    _zone_geometry_index_cache: dict[str, object] | None = field(default=None, init=False, repr=False)
    _municipality_coord_index_cache: dict[str, tuple[float, float]] | None = field(default=None, init=False, repr=False)

    @classmethod
    def from_csv(
        cls,
        file_path: str,
        default_top_n: int,
        max_top_n: int,
        separator: str = ",",
        encoding: str = "utf-8",
    ) -> "AnalyticsService":
        df = pd.read_csv(file_path, sep=separator, encoding=encoding, low_memory=False)
        service = cls(df, default_top_n=default_top_n, max_top_n=max_top_n)
        service._source_path = Path(file_path)
        return service

    @classmethod
    def from_file(
        cls,
        file_path: str,
        default_top_n: int,
        max_top_n: int,
        separator: str = ",",
        encoding: str = "utf-8",
    ) -> "AnalyticsService":
        suffix = Path(file_path).suffix.lower()
        if suffix == ".parquet":
            try:
                df = pd.read_parquet(file_path)
            except ImportError as exc:
                raise RuntimeError(
                    "Leitura parquet requer engine instalada (ex.: pyarrow)."
                ) from exc
            service = cls(df, default_top_n=default_top_n, max_top_n=max_top_n)
            service._source_path = Path(file_path)
            return service
        if suffix == ".csv":
            return cls.from_csv(
                file_path=file_path,
                default_top_n=default_top_n,
                max_top_n=max_top_n,
                separator=separator,
                encoding=encoding,
            )
        raise ValueError("Formato de analytics nao suportado. Use .csv ou .parquet.")

    def _pick_col(self, options: Iterable[str]) -> str | None:
        for col in options:
            if col in self.dataframe.columns:
                return col
        return None

    def _apply_filters(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        partido: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> pd.DataFrame:
        df = self.dataframe.copy()
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_turno = self._pick_col(["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if ano is not None and col_ano:
            df = df[df[col_ano] == ano]
        if turno is not None and col_turno:
            turno_num = pd.to_numeric(df[col_turno], errors="coerce")
            turno_text = (
                df[col_turno]
                .astype(str)
                .str.extract(r"(\d+)", expand=False)
                .fillna("")
                .str.strip()
            )
            df = df[(turno_num == int(turno)).fillna(False) | (turno_text == str(int(turno))).fillna(False)]
        if uf and col_uf:
            df = df[df[col_uf].astype(str).str.upper() == uf.upper()]
        if cargo and col_cargo:
            df = df[df[col_cargo].astype(str).str.lower() == cargo.lower()]
        if partido and col_partido:
            df = df[df[col_partido].astype(str).str.upper().str.strip() == partido.upper().strip()]
        if municipio and col_municipio:
            municipio_norm = self._normalize_municipio_filter(municipio)
            col_municipio_norm = (
                df[col_municipio].astype(str).apply(self._normalize_municipio_filter)
            )
            df = df[col_municipio_norm == municipio_norm]
        if somente_eleitos and col_situacao:
            df = df[self._is_elected(df[col_situacao])]
        return df

    def _normalize_text(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip()

    def _normalize_ascii_upper(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKD", text)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return ascii_text.upper().strip()

    def _normalize_municipio_key(self, text: str | None) -> str:
        key = self._normalize_ascii_upper(text or "")
        if not key:
            return ""
        return MUNICIPIO_ALIAS_MAP.get(key, key)

    def _normalize_municipio_filter(self, text: str | None) -> str:
        key = self._normalize_municipio_key(text or "")
        return " ".join(key.split())

    def _log_municipal_zone_debug(
        self,
        *,
        event: str,
        trace_id: str | None,
        normalized_filters: dict[str, object],
        plan: str,
        counts: dict[str, object] | None = None,
    ) -> None:
        logger.info(
            json.dumps(
                {
                    "event": event,
                    "trace_id": trace_id or "n/a",
                    "normalized_filters": normalized_filters,
                    "plan": plan,
                    "counts": counts or {},
                },
                ensure_ascii=False,
            )
        )

    def _official_prefeito_totals_by_uf(self, ano: int | None) -> dict[str, int]:
        if ano is None:
            return {}
        if self._source_path is None:
            return {}
        if ano in self._official_prefeito_totals_cache:
            return self._official_prefeito_totals_cache[ano]

        project_root = self._source_path.parent.parent if self._source_path.parent.name == "curated" else self._source_path.parent
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

    def _normalize_cor_raca_category(self, value: object) -> str:
        text = self._normalize_ascii_upper(str(value or ""))
        if not text or text in {"N/A", "NA", "NULL", "NULO"}:
            return "NAO_INFORMADO"
        if text in {
            "NAO INFORMADO",
            "NAO DIVULGAVEL",
            "NAO DECLARADO",
            "SEM INFORMACAO",
            "IGNORADO",
        }:
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

    def _is_elected(self, series: pd.Series) -> pd.Series:
        normalized = self._normalize_text(series).str.upper()
        return normalized.str.startswith("ELEITO")

    def _cargo_scope(self, cargo: str | None) -> str:
        cargo_up = (cargo or "").strip().upper()
        if cargo_up in MUNICIPAL_CARGOS:
            return "municipio"
        if cargo_up in NATIONAL_CARGOS:
            return "nacional"
        return "uf"

    def _party_spectrum(self, partido: str | None) -> str:
        norm = self._normalize_ascii_upper(partido or "")
        if not norm:
            return "indefinido"
        return PARTIDO_SPECTRUM_MAP.get(norm, "indefinido")

    def _extract_turno(self, series: pd.Series) -> pd.Series:
        turno_num = pd.to_numeric(series, errors="coerce")
        turno_text = (
            series.astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("")
            .str.strip()
        )
        turno_text_num = pd.to_numeric(turno_text, errors="coerce")
        return turno_num.fillna(turno_text_num).fillna(0).astype(int)

    def _candidate_mask(self, df: pd.DataFrame, candidate_id: str) -> pd.Series:
        candidate_norm = str(candidate_id or "").strip()
        candidate_ascii = self._normalize_ascii_upper(candidate_norm)
        if not candidate_norm:
            return pd.Series([False] * len(df), index=df.index)

        masks: list[pd.Series] = []
        col_sq = self._pick_col(["SQ_CANDIDATO"])
        col_nr = self._pick_col(["NR_CANDIDATO"])
        col_nome = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])

        if col_sq:
            masks.append(self._normalize_text(df[col_sq]) == candidate_norm)
        if col_nr:
            masks.append(self._normalize_text(df[col_nr]) == candidate_norm)
        if col_nome:
            masks.append(
                self._normalize_text(df[col_nome]).apply(self._normalize_ascii_upper) == candidate_ascii
            )

        if not masks:
            return pd.Series([False] * len(df), index=df.index)
        out = masks[0]
        for mask in masks[1:]:
            out = out | mask
        return out.fillna(False)

    def _candidate_output_id(self, df: pd.DataFrame, fallback: str) -> str:
        for col in ["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO", "NM_URNA_CANDIDATO"]:
            if col in df.columns:
                values = self._normalize_text(df[col]).replace("", pd.NA).dropna()
                if not values.empty:
                    return str(values.iloc[0])
        return str(fallback)

    def resolve_municipal_scope(
        self,
        *,
        candidate_id: str,
        year: int | None,
        office: str | None,
        round_filter: int | None,
        state: str | None = None,
        municipality: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, object]:
        requested_uf = (state or "").strip().upper() or None
        if requested_uf in {"BR", "BRASIL"}:
            requested_uf = None
        requested_municipio = self._normalize_municipio_filter(municipality) if municipality else None

        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_uf = self._pick_col(["SG_UF"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        if not col_uf or not col_municipio:
            raise ValueError("Nao foi possivel inferir municipio para candidate_id no recorte informado.")

        scoped = self._apply_filters(
            ano=year,
            turno=round_filter,
            cargo=office,
            uf=requested_uf,
            municipio=requested_municipio,
        )
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        if candidate_rows.empty:
            raise ValueError("Nao foi possivel inferir municipio para candidate_id no recorte informado.")

        grouped = candidate_rows.assign(
            _uf=self._normalize_text(candidate_rows[col_uf]).str.upper().replace("", pd.NA),
            _municipio=self._normalize_text(candidate_rows[col_municipio]).apply(self._normalize_municipio_filter).replace("", pd.NA),
            _votes=(
                pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0)
                if col_votes
                else pd.Series([0.0] * len(candidate_rows), index=candidate_rows.index)
            ),
        ).dropna(subset=["_uf", "_municipio"])
        if grouped.empty:
            raise ValueError("Nao foi possivel inferir municipio para candidate_id no recorte informado.")

        by_scope = (
            grouped.groupby(["_uf", "_municipio"], as_index=False)
            .agg(qt_votos=("_votes", "sum"))
            .sort_values(["qt_votos", "_uf", "_municipio"], ascending=[False, True, True])
            .reset_index(drop=True)
        )
        used_uf = str(by_scope.iloc[0]["_uf"])
        used_municipio = str(by_scope.iloc[0]["_municipio"])
        disambiguation_applied = False
        if len(by_scope) > 1 and (requested_uf is None or requested_municipio is None):
            disambiguation_applied = True
            logger.warning(
                json.dumps(
                    {
                        "event": "resolve_municipal_scope_disambiguation",
                        "trace_id": trace_id or "n/a",
                        "candidate_id": str(candidate_id),
                        "ano": year,
                        "cargo": office,
                        "turno": round_filter,
                        "requested_uf": requested_uf,
                        "requested_municipio": requested_municipio,
                        "used_uf": used_uf,
                        "used_municipio": used_municipio,
                        "disambiguation_applied": True,
                        "candidates_considered": int(len(by_scope)),
                    },
                    ensure_ascii=False,
                )
            )

        inferred_uf = requested_uf is None and bool(used_uf)
        inferred_municipio = requested_municipio is None and bool(used_municipio)
        return {
            "requested_uf": requested_uf,
            "requested_municipio": requested_municipio,
            "used_uf": used_uf,
            "used_municipio": used_municipio,
            "inferred_geo": bool(inferred_uf or inferred_municipio),
            "inferred_uf": used_uf if inferred_uf else None,
            "inferred_municipio": used_municipio if inferred_municipio else None,
            "disambiguation_applied": disambiguation_applied,
        }

    def _aggregate_candidate_votes(self, df: pd.DataFrame) -> pd.DataFrame:
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_ano or not col_votos or df.empty:
            return pd.DataFrame(columns=["year", "votes"])
        tmp = df.assign(
            year=pd.to_numeric(df[col_ano], errors="coerce"),
            votes=pd.to_numeric(df[col_votos], errors="coerce").fillna(0),
        ).dropna(subset=["year"])
        if tmp.empty:
            return pd.DataFrame(columns=["year", "votes"])
        return (
            tmp.groupby("year", as_index=False)
            .agg(votes=("votes", "sum"))
            .assign(year=lambda frame: frame["year"].astype(int), votes=lambda frame: frame["votes"].astype(int))
            .sort_values("year")
        )

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
                norm_props = {self._normalize_ascii_upper(str(k)): v for k, v in props.items()}

                zone_val = None
                for key in zone_keys:
                    prop_key = self._normalize_ascii_upper(key)
                    if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                        zone_val = self._normalize_ascii_upper(str(norm_props[prop_key]))
                        break
                if not zone_val:
                    continue

                uf_val = ""
                for key in uf_keys:
                    prop_key = self._normalize_ascii_upper(key)
                    if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                        uf_val = self._normalize_ascii_upper(str(norm_props[prop_key]))
                        break

                city_val = ""
                for key in city_keys:
                    prop_key = self._normalize_ascii_upper(key)
                    if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                        city_val = self._normalize_municipio_key(str(norm_props[prop_key]))
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
                    norm_props = {self._normalize_ascii_upper(str(k)): v for k, v in props.items()}
                    self._index_municipality_coords(index, norm_props, lat, lng)
                continue

            frame: pd.DataFrame
            try:
                frame = pd.read_parquet(path) if suffix == ".parquet" else pd.read_csv(path, low_memory=False)
            except Exception:
                continue
            if frame.empty:
                continue
            norm_cols = {str(col): self._normalize_ascii_upper(str(col)) for col in frame.columns}
            col_by_norm = {v: k for k, v in norm_cols.items()}
            code_col = next(
                (
                    col_by_norm[key]
                    for key in ["CD_MUNICIPIO", "COD_MUNICIPIO", "MUNICIPIO_ID", "MUNICIPIO_CODE", "IBGE", "ID"]
                    if key in col_by_norm
                ),
                None,
            )
            uf_col = next((col_by_norm[key] for key in ["SG_UF", "UF", "STATE"] if key in col_by_norm), None)
            city_col = next(
                (
                    col_by_norm[key]
                    for key in ["NM_MUNICIPIO", "NM_UE", "MUNICIPIO", "MUNICIPALITY", "CITY", "NAME"]
                    if key in col_by_norm
                ),
                None,
            )
            lat_col = next(
                (col_by_norm[key] for key in ["LATITUDE", "LAT", "VL_LATITUDE"] if key in col_by_norm),
                None,
            )
            lng_col = next(
                (col_by_norm[key] for key in ["LONGITUDE", "LON", "LNG", "VL_LONGITUDE"] if key in col_by_norm),
                None,
            )
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
                norm_props = {self._normalize_ascii_upper(str(k)): v for k, v in props.items()}
                self._index_municipality_coords(index, norm_props, lat, lng)

        self._municipality_coord_index_cache = index
        return index

    def _normalize_municipio_code(self, code: object) -> str:
        raw = str(code or "").strip()
        if not raw:
            return ""
        digits = "".join(ch for ch in raw if ch.isdigit())
        return digits

    def _index_municipality_coords(
        self,
        index: dict[str, tuple[float, float]],
        norm_props: dict[str, object],
        lat: float,
        lng: float,
    ) -> None:
        uf_val = ""
        for key in ["SG_UF", "UF", "STATE"]:
            prop_key = self._normalize_ascii_upper(key)
            if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                uf_val = self._normalize_ascii_upper(str(norm_props[prop_key]))
                break

        city_val = ""
        for key in ["NM_MUNICIPIO", "NM_UE", "MUNICIPIO", "MUNICIPALITY", "CITY", "NAME"]:
            prop_key = self._normalize_ascii_upper(key)
            if prop_key in norm_props and str(norm_props[prop_key] or "").strip():
                city_val = self._normalize_municipio_key(str(norm_props[prop_key]))
                break

        code_val = ""
        for key in ["CD_MUNICIPIO", "COD_MUNICIPIO", "MUNICIPIO_ID", "MUNICIPIO_CODE", "IBGE", "ID"]:
            prop_key = self._normalize_ascii_upper(key)
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
        uf_key = self._normalize_ascii_upper(uf or "")
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
            bucket = uf_acc.setdefault(
                uf,
                {"votes": 0, "points": 0, "sum_lat": 0.0, "sum_lng": 0.0},
            )
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
            bucket = macro_acc.setdefault(
                macro,
                {"votes": 0, "points": 0, "sum_lat": 0.0, "sum_lng": 0.0},
            )
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
                    "region_key": self._normalize_ascii_upper(macro),
                    "region_label": macro,
                    "votes": votes,
                    "points": int(bucket["points"]),
                    "lat": round(float(bucket["sum_lat"]) / votes, 6),
                    "lng": round(float(bucket["sum_lng"]) / votes, 6),
                }
            )

        return sorted(uf_clusters + macro_clusters, key=lambda item: int(item["votes"]), reverse=True)

    def _resolve_zone_geometry(self, zone_id: str, city: str | None, uf: str | None, lat: float, lng: float) -> object | None:
        zone_key = self._normalize_ascii_upper(zone_id)
        uf_key = self._normalize_ascii_upper(uf or "")
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

    def filter_options(self) -> dict:
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])

        anos: list[int] = []
        if col_ano:
            anos = sorted(
                pd.to_numeric(self.dataframe[col_ano], errors="coerce")
                .dropna()
                .astype(int)
                .unique()
                .tolist()
            )

        ufs: list[str] = []
        if col_uf:
            ufs = sorted(
                self.dataframe[col_uf]
                .dropna()
                .astype(str)
                .str.upper()
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )

        cargos: list[str] = []
        if col_cargo:
            cargos = sorted(
                self.dataframe[col_cargo]
                .dropna()
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )

        return {"anos": anos, "ufs": ufs, "cargos": cargos}

    def overview(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
    ) -> dict:
        df = self._apply_filters(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        total_eleitos = None
        if col_situacao:
            total_eleitos = int(self._is_elected(df[col_situacao]).sum())

        total_votos = None
        if col_votos:
            votos = pd.to_numeric(df[col_votos], errors="coerce").fillna(0)
            total_votos = int(votos.sum())

        return {
            "total_registros": int(len(df)),
            "total_candidatos": int(df[col_candidato].nunique()) if col_candidato else None,
            "total_eleitos": total_eleitos,
            "total_votos_nominais": total_votos,
        }

    def top_candidates(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        partido: str | None = None,
        municipio: str | None = None,
        top_n: int | None = None,
        page: int = 1,
        page_size: int | None = None,
    ) -> dict:
        df = self._apply_filters(ano=ano, turno=turno, uf=uf, cargo=cargo, partido=partido, municipio=municipio)
        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidato = self._pick_col(["NM_CANDIDATO"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if not col_candidato or not col_votos:
            effective_page_size = min(page_size or top_n or self.default_top_n, self.max_top_n)
            return {
                "top_n": effective_page_size,
                "page": page,
                "page_size": effective_page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
            }

        effective_page_size = min(page_size or top_n or self.default_top_n, self.max_top_n)
        df = df.assign(
            _votos=pd.to_numeric(df[col_votos], errors="coerce").fillna(0),
            _candidate_key=(
                self._normalize_text(df[col_candidate_key]).replace("", pd.NA)
                if col_candidate_key
                else self._normalize_text(df[col_candidato]).str.lower().replace("", pd.NA)
            ),
        )
        df = df.dropna(subset=["_candidate_key"]).copy()
        if df.empty:
            return {
                "top_n": effective_page_size,
                "page": page,
                "page_size": effective_page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
            }

        agg_spec: dict[str, str] = {"_votos": "sum", col_candidato: "first"}
        col_candidate_id = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        if col_candidate_id:
            agg_spec[col_candidate_id] = "first"
        if col_partido:
            agg_spec[col_partido] = "first"
        if col_cargo:
            agg_spec[col_cargo] = "first"
        if col_situacao:
            agg_spec[col_situacao] = "first"

        grouped = df.groupby("_candidate_key", as_index=False).agg(agg_spec)
        if col_uf:
            uf_stats = (
                df.assign(_uf_norm=self._normalize_text(df[col_uf]).str.upper().replace("", pd.NA))
                .groupby("_candidate_key", as_index=False)
                .agg(_uf_count=("_uf_norm", "nunique"), _uf_first=("_uf_norm", "first"))
            )
            grouped = grouped.merge(uf_stats, on="_candidate_key", how="left")
            grouped["_uf_out"] = grouped.apply(
                lambda row: row["_uf_first"] if int(row["_uf_count"] or 0) == 1 else None,
                axis=1,
            )
        else:
            grouped["_uf_out"] = None

        grouped = grouped.sort_values("_votos", ascending=False)
        total = int(len(grouped))
        total_pages = (total + effective_page_size - 1) // effective_page_size
        start = (page - 1) * effective_page_size
        end = start + effective_page_size
        grouped = grouped.iloc[start:end]

        out: list[dict] = []
        for _, row in grouped.iterrows():
            out.append(
                {
                    "candidate_id": (
                        str(row[col_candidate_id])
                        if col_candidate_id and pd.notna(row[col_candidate_id]) and str(row[col_candidate_id]).strip()
                        else str(row["_candidate_key"])
                    ),
                    "candidato": str(row[col_candidato]),
                    "partido": str(row[col_partido]) if col_partido else None,
                    "cargo": str(row[col_cargo]) if col_cargo else None,
                    "uf": str(row["_uf_out"]) if pd.notna(row["_uf_out"]) else None,
                    "votos": int(row["_votos"]),
                    "situacao": str(row[col_situacao]) if col_situacao else None,
                }
            )
        return {
            "top_n": effective_page_size,
            "page": page,
            "page_size": effective_page_size,
            "total": total,
            "total_pages": total_pages,
            "items": out,
        }

    def distribution(
        self,
        group_by: str,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        col_map = {
            "status": ["DS_SIT_TOT_TURNO"],
            "genero": ["DS_GENERO"],
            "instrucao": ["DS_GRAU_INSTRUCAO"],
            "cor_raca": ["DS_COR_RACA"],
            "estado_civil": ["DS_ESTADO_CIVIL"],
            "ocupacao": ["DS_OCUPACAO"],
            "cargo": ["DS_CARGO", "DS_CARGO_D"],
            "uf": ["SG_UF"],
        }
        cols = col_map.get(group_by)
        if not cols:
            return []

        target_col = self._pick_col(cols)
        if not target_col:
            return []

        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        counts = (
            df[target_col]
            .fillna("N/A")
            .astype(str)
            .value_counts(dropna=False)
            .rename_axis("label")
            .reset_index(name="value")
        )
        total = float(counts["value"].sum()) or 1.0

        items = []
        for _, row in counts.iterrows():
            val = float(row["value"])
            items.append(
                {
                    "label": row["label"],
                    "value": val,
                    "percentage": round((val / total) * 100, 2),
                }
            )
        return items

    def cor_raca_comparativo(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
    ) -> dict:
        col_cor_raca = self._pick_col(["DS_COR_RACA"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        if not col_cor_raca:
            return {
                "items": [
                    {
                        "categoria": COR_RACA_CATEGORY_LABELS[key],
                        "candidatos": 0,
                        "eleitos": 0,
                        "percentual_candidatos": 0.0,
                        "percentual_eleitos": 0.0,
                    }
                    for key in COR_RACA_CATEGORY_ORDER
                ]
            }

        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=False,
        )
        if df.empty:
            return {
                "items": [
                    {
                        "categoria": COR_RACA_CATEGORY_LABELS[key],
                        "candidatos": 0,
                        "eleitos": 0,
                        "percentual_candidatos": 0.0,
                        "percentual_eleitos": 0.0,
                    }
                    for key in COR_RACA_CATEGORY_ORDER
                ]
            }

        categorias = df[col_cor_raca].apply(self._normalize_cor_raca_category)
        candidatos = categorias.value_counts()
        eleitos = (
            categorias[self._is_elected(df[col_situacao])]
            .value_counts()
            if col_situacao
            else pd.Series(dtype="int64")
        )

        total_candidatos = int(candidatos.sum() or 0)
        total_eleitos = int(eleitos.sum() or 0)

        items: list[dict] = []
        for key in COR_RACA_CATEGORY_ORDER:
            qtd_candidatos = int(candidatos.get(key, 0))
            qtd_eleitos = int(eleitos.get(key, 0))
            items.append(
                {
                    "categoria": COR_RACA_CATEGORY_LABELS[key],
                    "candidatos": qtd_candidatos,
                    "eleitos": qtd_eleitos,
                    "percentual_candidatos": round((qtd_candidatos / total_candidatos) * 100, 2)
                    if total_candidatos
                    else 0.0,
                    "percentual_eleitos": round((qtd_eleitos / total_eleitos) * 100, 2)
                    if total_eleitos
                    else 0.0,
                }
            )
        return {"items": items}

    def occupation_gender_distribution(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        col_ocupacao = self._pick_col(["DS_OCUPACAO"])
        col_genero = self._pick_col(["DS_GENERO"])
        if not col_ocupacao or not col_genero:
            return []

        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        if df.empty:
            return []

        prepared = pd.DataFrame(
            {
                "ocupacao": df[col_ocupacao].fillna("N/A").astype(str).str.strip().replace("", "N/A"),
                "genero": df[col_genero].fillna("").astype(str).str.strip().str.upper(),
            }
        )
        prepared = prepared.assign(
            masculino=(prepared["genero"].str.startswith("MASC") | (prepared["genero"] == "M")),
            feminino=(prepared["genero"].str.startswith("FEM") | (prepared["genero"] == "F")),
        )

        grouped = (
            prepared.groupby("ocupacao", as_index=False)
            .agg(masculino=("masculino", "sum"), feminino=("feminino", "sum"))
            .astype({"masculino": int, "feminino": int})
        )
        grouped = grouped.sort_values(["masculino", "feminino", "ocupacao"], ascending=[False, False, True])

        return [
            {"ocupacao": str(row["ocupacao"]), "masculino": int(row["masculino"]), "feminino": int(row["feminino"])}
            for _, row in grouped.iterrows()
        ]

    def age_stats(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = True,
    ) -> dict:
        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )

        col_data_nasc = self._pick_col(["DT_NASCIMENTO"])
        col_idade = self._pick_col(["IDADE"])
        col_dt_eleicao = self._pick_col(["DT_ELEICAO"])
        if col_idade and col_idade in df.columns:
            idade = pd.to_numeric(df[col_idade], errors="coerce")
        elif col_data_nasc and col_dt_eleicao:
            nasc = pd.to_datetime(df[col_data_nasc], errors="coerce", dayfirst=True)
            eleicao = pd.to_datetime(df[col_dt_eleicao], errors="coerce", dayfirst=True)
            idade = ((eleicao - nasc).dt.days / 365.25).round(0)
        else:
            return {"media": None, "mediana": None, "minimo": None, "maximo": None, "desvio_padrao": None, "bins": []}

        idade = idade.dropna()
        if idade.empty:
            return {"media": None, "mediana": None, "minimo": None, "maximo": None, "desvio_padrao": None, "bins": []}

        labels = ["20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89"]
        idade_hist = idade[(idade >= 20) & (idade <= 89)]
        bins = [20, 30, 40, 50, 60, 70, 80, 90]
        faixa = pd.cut(idade_hist, bins=bins, labels=labels, right=False, include_lowest=True)
        counts = faixa.value_counts(sort=False)
        total = float(counts.sum()) or 1.0
        dist = [
            {
                "label": str(label),
                "value": float(counts.loc[label]),
                "percentage": round((float(counts.loc[label]) / total) * 100, 2),
            }
            for label in labels
        ]

        return {
            "media": round(float(idade.mean()), 2),
            "mediana": round(float(idade.median()), 2),
            "minimo": float(idade.min()),
            "maximo": float(idade.max()),
            "desvio_padrao": round(float(idade.std(ddof=0)), 2),
            "bins": dist,
        }

    def _aggregate_metric(self, df: pd.DataFrame, metric: str, group_cols: list[str]) -> pd.DataFrame | None:
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if metric == "registros":
            out = df.groupby(group_cols, dropna=False).size().reset_index(name="value")
            return out

        if metric == "votos_nominais":
            if not col_votos:
                return None
            tmp = df.assign(_votos=pd.to_numeric(df[col_votos], errors="coerce").fillna(0))
            out = tmp.groupby(group_cols, dropna=False)["_votos"].sum().reset_index(name="value")
            return out

        if metric == "candidatos":
            if not col_candidato:
                return None
            out = (
                df.groupby(group_cols, dropna=False)[col_candidato]
                .nunique(dropna=True)
                .reset_index(name="value")
            )
            return out

        if metric == "eleitos":
            if not col_situacao:
                return None
            tmp = df.assign(
                _eleito=self._is_elected(df[col_situacao]).astype(int)
            )
            out = tmp.groupby(group_cols, dropna=False)["_eleito"].sum().reset_index(name="value")
            return out

        return None

    def time_series(
        self,
        metric: str = "votos_nominais",
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        if not col_ano:
            return []

        df = self._apply_filters(
            ano=None,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        tmp = df.assign(_ano=pd.to_numeric(df[col_ano], errors="coerce")).dropna(subset=["_ano"])
        if tmp.empty:
            return []

        agg = self._aggregate_metric(tmp, metric=metric, group_cols=["_ano"])
        if agg is None or agg.empty:
            return []

        agg = agg.sort_values("_ano")
        items: list[dict] = []
        for _, row in agg.iterrows():
            items.append({"ano": int(row["_ano"]), "value": float(row["value"])})
        return items

    def ranking(
        self,
        group_by: str = "partido",
        metric: str = "votos_nominais",
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
        top_n: int | None = None,
    ) -> list[dict]:
        group_map = {
            "candidato": ["NM_CANDIDATO", "NM_URNA_CANDIDATO"],
            "partido": ["SG_PARTIDO"],
            "cargo": ["DS_CARGO", "DS_CARGO_D"],
            "uf": ["SG_UF"],
        }
        options = group_map.get(group_by)
        if not options:
            return []
        col_group = self._pick_col(options)
        if not col_group:
            return []

        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        agg = self._aggregate_metric(df, metric=metric, group_cols=[col_group])
        if agg is None or agg.empty:
            return []

        agg = agg.assign(
            **{
                col_group: agg[col_group]
                .fillna("N/A")
                .astype(str)
                .str.strip()
                .replace("", "N/A")
            }
        )
        agg = agg.groupby(col_group, as_index=False)["value"].sum()
        agg = agg.sort_values("value", ascending=False)
        n = min(top_n or self.default_top_n, self.max_top_n)
        agg = agg.head(n)
        total = float(agg["value"].sum()) or 1.0
        items: list[dict] = []
        for _, row in agg.iterrows():
            val = float(row["value"])
            items.append(
                {
                    "label": str(row[col_group]),
                    "value": val,
                    "percentage": round((val / total) * 100, 2),
                }
            )
        return items

    def uf_map(
        self,
        metric: str = "votos_nominais",
        ano: int | None = None,
        turno: int | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        col_uf = self._pick_col(["SG_UF"])
        if not col_uf:
            return []
        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=None,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        agg = self._aggregate_metric(df, metric=metric, group_cols=[col_uf])
        if agg is None or agg.empty:
            return []

        agg = agg.assign(
            **{
                col_uf: agg[col_uf]
                .fillna("N/A")
                .astype(str)
                .str.upper()
                .str.strip()
                .replace("", "N/A")
            }
        )
        agg = agg.groupby(col_uf, as_index=False)["value"].sum().sort_values("value", ascending=False)
        total = float(agg["value"].sum()) or 1.0
        items: list[dict] = []
        for _, row in agg.iterrows():
            val = float(row["value"])
            items.append(
                {
                    "uf": str(row[col_uf]),
                    "value": val,
                    "percentage": round((val / total) * 100, 2),
                }
            )
        return items

    def polarizacao(
        self,
        uf: str | None = None,
        ano_governador: int | None = None,
        turno_governador: int | None = None,
        ano_municipal: int | None = None,
        turno_municipal: int | None = None,
        map_mode: str | None = None,
    ) -> dict:
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_turno = self._pick_col(["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        required = [col_uf, col_cargo, col_partido, col_votos, col_situacao]
        if any(col is None for col in required):
            return {"federal": [], "municipal_brasil": [], "municipal_uf": []}

        base = self.dataframe.copy()
        base = base.assign(
            _uf=self._normalize_text(base[col_uf]).str.upper().replace("", pd.NA),
            _cargo=self._normalize_text(base[col_cargo]),
            _municipio=(
                self._normalize_text(base[col_municipio]).replace("", pd.NA)
                if col_municipio
                else pd.Series([pd.NA] * len(base), index=base.index)
            ),
            _partido=self._normalize_text(base[col_partido]).str.upper().replace("", pd.NA),
            _votos=pd.to_numeric(base[col_votos], errors="coerce").fillna(0).astype(int),
            _situacao=self._normalize_text(base[col_situacao]),
        )
        base = base.dropna(subset=["_uf", "_partido"]).copy()
        if base.empty:
            return {"federal": [], "municipal_brasil": [], "municipal_uf": []}
        base = base.assign(
            _is_eleito=self._is_elected(base["_situacao"]),
            _turno=(self._extract_turno(base[col_turno]) if col_turno else 0),
            _ano=(
                pd.to_numeric(base[col_ano], errors="coerce").astype("Int64")
                if col_ano
                else pd.Series([pd.NA] * len(base), index=base.index, dtype="Int64")
            ),
        )

        uf_filter = (uf or "").upper().strip()
        if uf_filter:
            base = base[base["_uf"] == uf_filter]

        def winner_rows(df: pd.DataFrame, unit_cols: list[str]) -> pd.DataFrame:
            if df.empty:
                return df
            ranked = df.sort_values(unit_cols + ["_is_eleito", "_turno", "_votos"], ascending=[True] * len(unit_cols) + [False, False, False])
            return ranked.drop_duplicates(subset=unit_cols, keep="first").copy()

        mode = (map_mode or "").strip().lower()
        should_build_federal = mode in ("", "statebygovernor")
        should_build_municipal = mode in ("", "municipalitybymayor")
        return_municipal_uf = mode == "" or (mode == "municipalitybymayor" and bool(uf_filter))
        return_municipal_brasil = mode == "" or (mode == "municipalitybymayor" and not bool(uf_filter))

        federal_items: list[dict] = []
        if should_build_federal:
            df_gov = base[base["_cargo"].str.upper() == "GOVERNADOR"].copy()
            if ano_governador is None and col_ano and not df_gov.empty:
                anos = df_gov["_ano"].dropna()
                if not anos.empty:
                    ano_governador = int(anos.max())
            if ano_governador is not None:
                df_gov = df_gov[df_gov["_ano"] == int(ano_governador)]
            if turno_governador is not None:
                df_gov = df_gov[df_gov["_turno"] == int(turno_governador)]
            gov_winners = winner_rows(df_gov, ["_uf"])

            federal_items = [
                {
                    "uf": str(row["_uf"]),
                    "partido": str(row["_partido"]),
                    "espectro": self._party_spectrum(str(row["_partido"])),
                    "votos": int(row["_votos"]),
                    "status": str(row["_situacao"]) if pd.notna(row["_situacao"]) else None,
                    "eleito": bool(row["_is_eleito"]),
                    "ano": int(row["_ano"]) if pd.notna(row["_ano"]) else None,
                    "turno": int(row["_turno"]) if pd.notna(row["_turno"]) else None,
                }
                for _, row in gov_winners.sort_values("_uf").iterrows()
            ]

        df_pref = base.iloc[0:0]
        if should_build_municipal:
            df_pref = base[base["_cargo"].str.upper() == "PREFEITO"].copy()
            if col_municipio:
                df_pref = df_pref.dropna(subset=["_municipio"])
            else:
                df_pref = df_pref.iloc[0:0]
            if ano_municipal is None and col_ano and not df_pref.empty:
                anos = df_pref["_ano"].dropna()
                if not anos.empty:
                    ano_municipal = int(anos.max())
            if ano_municipal is not None:
                df_pref = df_pref[df_pref["_ano"] == int(ano_municipal)]
            if turno_municipal is not None:
                df_pref = df_pref[df_pref["_turno"] == int(turno_municipal)]

        pref_winners = winner_rows(df_pref, ["_uf", "_municipio"])
        if not pref_winners.empty:
            pref_winners = pref_winners.assign(_espectro=pref_winners["_partido"].apply(self._party_spectrum))
        else:
            pref_winners = pref_winners.assign(_espectro=pd.Series(dtype="object"))

        municipal_uf_items = []
        if return_municipal_uf:
            municipal_uf_items = [
                {
                    "uf": str(row["_uf"]),
                    "municipio": str(row["_municipio"]),
                    "partido": str(row["_partido"]),
                    "espectro": str(row["_espectro"]),
                    "votos": int(row["_votos"]),
                    "status": str(row["_situacao"]) if pd.notna(row["_situacao"]) else None,
                    "eleito": bool(row["_is_eleito"]),
                    "ano": int(row["_ano"]) if pd.notna(row["_ano"]) else None,
                    "turno": int(row["_turno"]) if pd.notna(row["_turno"]) else None,
                }
                for _, row in pref_winners.sort_values(["_uf", "_municipio"]).iterrows()
            ]

        municipal_brasil_items: list[dict] = []
        if return_municipal_brasil and not pref_winners.empty:
            total_prefeitos_por_uf = pref_winners.groupby("_uf")["_municipio"].count().to_dict()
            official_prefeito_totals = self._official_prefeito_totals_by_uf(ano_municipal)
            for uf_key, total_official in official_prefeito_totals.items():
                total_prefeitos_por_uf[uf_key] = max(int(total_official), int(total_prefeitos_por_uf.get(uf_key, 0)))
            agg = (
                pref_winners.groupby(["_uf", "_espectro"], as_index=False)
                .agg(
                    total_prefeitos=("_municipio", "count"),
                    votos_total=("_votos", "sum"),
                )
                .sort_values(["_uf", "total_prefeitos", "votos_total", "_espectro"], ascending=[True, False, False, True])
            )
            winners_by_uf = agg.drop_duplicates(subset=["_uf"], keep="first")
            partido_rep = (
                pref_winners.groupby(["_uf", "_partido"], as_index=False)
                .size()
                .rename(columns={"size": "total"})
                .sort_values(["_uf", "total", "_partido"], ascending=[True, False, True])
                .drop_duplicates(subset=["_uf"], keep="first")
                .set_index("_uf")
            )
            for _, row in winners_by_uf.sort_values("_uf").iterrows():
                uf_key = str(row["_uf"])
                municipal_brasil_items.append(
                    {
                        "uf": uf_key,
                        "espectro": str(row["_espectro"]),
                        "partido_representativo": (
                            str(partido_rep.loc[uf_key]["_partido"]) if uf_key in partido_rep.index else None
                        ),
                        "total_prefeitos": int(total_prefeitos_por_uf.get(uf_key, 0)),
                        "ano": int(ano_municipal) if ano_municipal is not None else None,
                    }
                )

        return {
            "federal": federal_items,
            "municipal_brasil": municipal_brasil_items,
            "municipal_uf": municipal_uf_items,
        }

    def candidate_summary(
        self,
        candidate_id: str,
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        scoped = self._apply_filters(ano=year, uf=state, cargo=office)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        if candidate_rows.empty:
            return {
                "candidate_id": str(candidate_id),
                "name": "",
                "number": None,
                "party": None,
                "coalition": None,
                "office": office,
                "state": state.upper() if state else None,
                "mandates": 0,
                "status": None,
                "latest_election": {"year": year or 0, "votes": 0, "vote_share": 0.0, "state_rank": None},
            }

        col_name = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        col_number = self._pick_col(["NR_CANDIDATO"])
        col_party = self._pick_col(["SG_PARTIDO"])
        col_coalition = self._pick_col(["NM_COLIGACAO", "DS_COMPOSICAO_COLIGACAO"])
        col_office = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_state = self._pick_col(["SG_UF"])
        col_status = self._pick_col(["DS_SIT_TOT_TURNO"])
        col_year = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])

        candidate_rows = candidate_rows.assign(
            _year=(pd.to_numeric(candidate_rows[col_year], errors="coerce") if col_year else pd.Series(dtype=float)),
            _votes=(pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0) if col_votes else 0),
        )
        if col_year and candidate_rows["_year"].notna().any():
            latest_year = int(candidate_rows["_year"].max())
            latest_rows = candidate_rows[candidate_rows["_year"] == latest_year].copy()
        else:
            latest_year = int(year or 0)
            latest_rows = candidate_rows.copy()

        latest_votes = int(pd.to_numeric(latest_rows["_votes"], errors="coerce").fillna(0).sum())
        latest_state = (
            str(self._normalize_text(latest_rows[col_state]).replace("", pd.NA).dropna().iloc[0]).upper()
            if col_state and self._normalize_text(latest_rows[col_state]).replace("", pd.NA).dropna().any()
            else (state.upper() if state else None)
        )
        latest_office = (
            str(self._normalize_text(latest_rows[col_office]).replace("", pd.NA).dropna().iloc[0])
            if col_office and self._normalize_text(latest_rows[col_office]).replace("", pd.NA).dropna().any()
            else office
        )

        rank = None
        vote_share = 0.0
        if col_votes and col_year and latest_year:
            context = self._apply_filters(ano=latest_year, uf=latest_state, cargo=latest_office)
            total_votes = float(pd.to_numeric(context[col_votes], errors="coerce").fillna(0).sum()) if not context.empty else 0.0
            vote_share = round((latest_votes / total_votes) * 100, 4) if total_votes > 0 else 0.0
            if not context.empty:
                context = context.assign(
                    _votos=pd.to_numeric(context[col_votes], errors="coerce").fillna(0),
                    _candidate_key=(
                        self._normalize_text(context["SQ_CANDIDATO"])
                        if "SQ_CANDIDATO" in context.columns
                        else (
                            self._normalize_text(context["NR_CANDIDATO"])
                            if "NR_CANDIDATO" in context.columns
                            else self._normalize_text(context[col_name]).str.lower()
                        )
                    ),
                )
                ranking = (
                    context.groupby("_candidate_key", as_index=False)
                    .agg(votes=("_votos", "sum"))
                    .sort_values("votes", ascending=False)
                    .reset_index(drop=True)
                )
                cand_key = (
                    self._normalize_text(latest_rows["SQ_CANDIDATO"]).replace("", pd.NA).dropna().iloc[0]
                    if "SQ_CANDIDATO" in latest_rows.columns
                    and self._normalize_text(latest_rows["SQ_CANDIDATO"]).replace("", pd.NA).dropna().any()
                    else (
                        self._normalize_text(latest_rows["NR_CANDIDATO"]).replace("", pd.NA).dropna().iloc[0]
                        if "NR_CANDIDATO" in latest_rows.columns
                        and self._normalize_text(latest_rows["NR_CANDIDATO"]).replace("", pd.NA).dropna().any()
                        else self._normalize_text(latest_rows[col_name]).str.lower().iloc[0]
                    )
                )
                matches = ranking[ranking["_candidate_key"] == cand_key]
                if not matches.empty:
                    rank = int(matches.index[0]) + 1

        mandates = 0
        if col_status and col_year:
            elected_per_year = (
                candidate_rows.assign(_eleito=self._is_elected(candidate_rows[col_status]))
                .groupby("_year", as_index=False)
                .agg(elected=("_eleito", "max"))
            )
            mandates = int(elected_per_year["elected"].sum())

        latest_status = None
        if col_status and not latest_rows.empty:
            status_vals = self._normalize_text(latest_rows[col_status]).replace("", pd.NA).dropna()
            latest_status = str(status_vals.iloc[0]) if not status_vals.empty else None

        return {
            "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
            "name": str(self._normalize_text(candidate_rows[col_name]).replace("", pd.NA).dropna().iloc[0]) if col_name else "",
            "number": (
                str(self._normalize_text(candidate_rows[col_number]).replace("", pd.NA).dropna().iloc[0])
                if col_number and self._normalize_text(candidate_rows[col_number]).replace("", pd.NA).dropna().any()
                else None
            ),
            "party": (
                str(self._normalize_text(candidate_rows[col_party]).replace("", pd.NA).dropna().iloc[0])
                if col_party and self._normalize_text(candidate_rows[col_party]).replace("", pd.NA).dropna().any()
                else None
            ),
            "coalition": (
                str(self._normalize_text(candidate_rows[col_coalition]).replace("", pd.NA).dropna().iloc[0])
                if col_coalition and self._normalize_text(candidate_rows[col_coalition]).replace("", pd.NA).dropna().any()
                else None
            ),
            "office": latest_office,
            "state": latest_state,
            "mandates": mandates,
            "status": latest_status,
            "latest_election": {
                "year": latest_year,
                "votes": latest_votes,
                "vote_share": vote_share,
                "state_rank": rank,
            },
        }

    def candidate_vote_history(
        self,
        candidate_id: str,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        scoped = self._apply_filters(uf=state, cargo=office)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        col_year = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_status = self._pick_col(["DS_SIT_TOT_TURNO"])
        if candidate_rows.empty or not col_year or not col_votes:
            return {"candidate_id": str(candidate_id), "items": []}

        candidate_rows = candidate_rows.assign(
            _year=pd.to_numeric(candidate_rows[col_year], errors="coerce"),
            _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
        ).dropna(subset=["_year"])
        if candidate_rows.empty:
            return {"candidate_id": str(candidate_id), "items": []}

        total_by_year = (
            scoped.assign(
                _year=pd.to_numeric(scoped[col_year], errors="coerce"),
                _votes=pd.to_numeric(scoped[col_votes], errors="coerce").fillna(0),
            )
            .dropna(subset=["_year"])
            .groupby("_year", as_index=False)
            .agg(total_votes=("_votes", "sum"))
        )
        cand_by_year = (
            candidate_rows.groupby("_year", as_index=False)
            .agg(votes=("_votes", "sum"))
            .merge(total_by_year, on="_year", how="left")
            .sort_values("_year")
        )

        status_by_year: dict[int, str | None] = {}
        if col_status:
            for year_val, year_df in candidate_rows.groupby("_year"):
                statuses = self._normalize_text(year_df[col_status]).replace("", pd.NA).dropna()
                if statuses.empty:
                    status_by_year[int(year_val)] = None
                elif self._is_elected(statuses).any():
                    status_by_year[int(year_val)] = "ELEITO"
                else:
                    status_by_year[int(year_val)] = str(statuses.iloc[0])

        items = []
        for _, row in cand_by_year.iterrows():
            year_int = int(row["_year"])
            votes_int = int(row["votes"])
            total_votes = float(row["total_votes"] or 0)
            items.append(
                {
                    "year": year_int,
                    "votes": votes_int,
                    "vote_share": round((votes_int / total_votes) * 100, 4) if total_votes > 0 else 0.0,
                    "status": status_by_year.get(year_int),
                }
            )

        return {"candidate_id": self._candidate_output_id(candidate_rows, candidate_id), "items": items}

    def candidate_electorate_profile(
        self,
        candidate_id: str,
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        municipality: str | None = None,
    ) -> dict:
        scoped = self._apply_filters(ano=year, uf=state, cargo=office, municipio=municipality)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        if candidate_rows.empty:
            return {
                "candidate_id": str(candidate_id),
                "gender": {"male_share": 0.0, "female_share": 0.0},
                "age_bands": {"a18_34": 0.0, "a35_59": 0.0, "a60_plus": 0.0},
                "dominant_education": "N/A",
                "dominant_income_band": "N/A",
                "urban_concentration": 0.0,
            }

        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        weights = pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(1.0) if col_votes else 1.0
        total_weight = float(weights.sum()) if hasattr(weights, "sum") else float(len(candidate_rows))
        total_weight = total_weight if total_weight > 0 else 1.0

        col_gender = self._pick_col(["DS_GENERO"])
        male_share = 0.0
        female_share = 0.0
        if col_gender:
            gender_norm = self._normalize_text(candidate_rows[col_gender]).apply(self._normalize_ascii_upper)
            male_share = round((weights[gender_norm.str.contains("MASC", na=False)].sum() / total_weight) * 100, 4)
            female_share = round((weights[gender_norm.str.contains("FEM", na=False)].sum() / total_weight) * 100, 4)

        col_age = self._pick_col(["IDADE"])
        a18_34 = 0.0
        a35_59 = 0.0
        a60_plus = 0.0
        if col_age:
            ages = pd.to_numeric(candidate_rows[col_age], errors="coerce")
            a18_34 = round((weights[(ages >= 18) & (ages <= 34)].sum() / total_weight) * 100, 4)
            a35_59 = round((weights[(ages >= 35) & (ages <= 59)].sum() / total_weight) * 100, 4)
            a60_plus = round((weights[ages >= 60].sum() / total_weight) * 100, 4)

        col_education = self._pick_col(["DS_GRAU_INSTRUCAO"])
        dominant_education = "N/A"
        if col_education:
            edu = (
                candidate_rows.assign(_w=weights, _label=self._normalize_text(candidate_rows[col_education]).replace("", "N/A"))
                .groupby("_label", as_index=False)
                .agg(weight=("_w", "sum"))
                .sort_values(["weight", "_label"], ascending=[False, True])
            )
            if not edu.empty:
                dominant_education = str(edu.iloc[0]["_label"])

        col_income = self._pick_col(["DS_FAIXA_RENDA", "DS_RENDA", "FAIXA_RENDA"])
        dominant_income = "N/A"
        if col_income:
            income = (
                candidate_rows.assign(_w=weights, _label=self._normalize_text(candidate_rows[col_income]).replace("", "N/A"))
                .groupby("_label", as_index=False)
                .agg(weight=("_w", "sum"))
                .sort_values(["weight", "_label"], ascending=[False, True])
            )
            if not income.empty:
                dominant_income = str(income.iloc[0]["_label"])

        col_urban = self._pick_col(["DS_LOCAL_VOTACAO", "DS_TIPO_LOCAL", "DS_CLASSIFICACAO_UE"])
        urban_concentration = 0.0
        if col_urban:
            urban_mask = self._normalize_text(candidate_rows[col_urban]).apply(self._normalize_ascii_upper).str.contains(
                "URBAN", na=False
            )
            urban_concentration = round((weights[urban_mask].sum() / total_weight) * 100, 4)

        return {
            "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
            "gender": {"male_share": male_share, "female_share": female_share},
            "age_bands": {"a18_34": a18_34, "a35_59": a35_59, "a60_plus": a60_plus},
            "dominant_education": dominant_education,
            "dominant_income_band": dominant_income,
            "urban_concentration": urban_concentration,
        }

    def candidate_vote_distribution(
        self,
        candidate_id: str,
        level: str,
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        round_filter: int | None = None,
        municipality: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
        trace_id: str | None = None,
    ) -> dict:
        level_key = (level or "").strip().lower()
        if level_key not in {"macroregiao", "uf", "municipio", "zona"}:
            return {"candidate_id": str(candidate_id), "level": level_key, "items": []}

        normalized_state = (state or "").strip().upper() or None
        if normalized_state in {"BR", "BRASIL"}:
            normalized_state = None
        normalized_municipio = self._normalize_municipio_filter(municipality) if municipality else None
        scoped = self._apply_filters(
            ano=year,
            turno=round_filter,
            uf=normalized_state,
            cargo=office,
            municipio=normalized_municipio,
        )
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if candidate_rows.empty or not col_votes:
            return {
                "candidate_id": str(candidate_id),
                "level": level_key,
                "page": page,
                "page_size": page_size,
                "total_items": 0,
                "items": [],
            }

        col_uf = self._pick_col(["SG_UF"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_zona = self._pick_col(["NR_ZONA", "CD_ZONA", "SQ_ZONA"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        if office:
            municipal_scope = self._cargo_scope(office) == "municipio"
        elif col_cargo:
            cargo_norm = self._normalize_text(candidate_rows[col_cargo]).apply(self._normalize_ascii_upper)
            municipal_scope = cargo_norm.isin(MUNICIPAL_CARGOS).any()
        else:
            municipal_scope = False

        if level_key == "macroregiao":
            if not col_uf:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            grouped = candidate_rows.assign(
                key=self._normalize_text(candidate_rows[col_uf]).str.upper().map(UF_TO_MACROREGIAO).fillna("N/A"),
                label=self._normalize_text(candidate_rows[col_uf]).str.upper().map(UF_TO_MACROREGIAO).fillna("N/A"),
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            )
        elif level_key == "uf":
            if not col_uf:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            grouped = candidate_rows.assign(
                key=self._normalize_text(candidate_rows[col_uf]).str.upper().replace("", "N/A"),
                label=self._normalize_text(candidate_rows[col_uf]).str.upper().replace("", "N/A"),
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            )
        elif level_key == "municipio":
            if not col_municipio:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            scoped_votes = scoped.assign(_votes=pd.to_numeric(scoped[col_votes], errors="coerce").fillna(0))
            candidate_votes = candidate_rows.assign(_votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0))
            city_totals = (
                scoped_votes.assign(_municipio=self._normalize_text(scoped_votes[col_municipio]).replace("", pd.NA))
                .dropna(subset=["_municipio"])
                .groupby("_municipio", as_index=False)
                .agg(votos_validos_do_municipio=("_votes", "sum"))
            )
            grouped = (
                candidate_votes.assign(_municipio=self._normalize_text(candidate_votes[col_municipio]).replace("", pd.NA))
                .dropna(subset=["_municipio"])
                .groupby("_municipio", as_index=False)
                .agg(qt_votos=("_votes", "sum"))
                .merge(city_totals, on="_municipio", how="left")
            )
            total_candidate_votes = float(grouped["qt_votos"].sum())
            grouped = grouped.assign(
                key=grouped["_municipio"].astype(str),
                label=grouped["_municipio"].astype(str),
                votes=grouped["qt_votos"].astype(float),
                vote_share=grouped["qt_votos"].apply(lambda value: _safe_percent(float(value), total_candidate_votes)),
                nm_municipio=grouped["_municipio"].astype(str),
                votos_validos_do_municipio=grouped["votos_validos_do_municipio"].fillna(0).astype(float),
                percentual_candidato_no_municipio=grouped.apply(
                    lambda row: _safe_percent(float(row["qt_votos"]), float(row["votos_validos_do_municipio"] or 0)),
                    axis=1,
                ),
                percentual_no_total_do_candidato=grouped["qt_votos"].apply(
                    lambda value: _safe_percent(float(value), total_candidate_votes)
                ),
            )
            grouped = grouped.assign(
                categoria_impacto=grouped["percentual_no_total_do_candidato"].apply(_impact_category)
            )
            items = [
                {
                    "key": str(row["key"]),
                    "label": str(row["label"]),
                    "votes": int(row["votes"]),
                    "vote_share": round(float(row["vote_share"]), 4),
                    "nm_municipio": str(row["nm_municipio"]),
                    "votos_validos_do_municipio": int(float(row["votos_validos_do_municipio"] or 0)),
                    "qt_votos": int(float(row["qt_votos"])),
                    "percentual_candidato_no_municipio": float(row["percentual_candidato_no_municipio"]),
                    "percentual_no_total_do_candidato": float(row["percentual_no_total_do_candidato"]),
                    "categoria_impacto": str(row["categoria_impacto"]),
                }
                for _, row in grouped.iterrows()
            ]
            sort_map = {
                "key": lambda item: item["key"],
                "label": lambda item: item["label"],
                "votes": lambda item: item["votes"],
                "vote_share": lambda item: item["vote_share"],
                "nm_municipio": lambda item: item["nm_municipio"],
                "votos_validos_do_municipio": lambda item: item["votos_validos_do_municipio"],
                "qt_votos": lambda item: item["qt_votos"],
                "percentual_candidato_no_municipio": lambda item: item["percentual_candidato_no_municipio"],
                "percentual_no_total_do_candidato": lambda item: item["percentual_no_total_do_candidato"],
                "categoria_impacto": lambda item: item["categoria_impacto"],
            }
            effective_sort_by = (sort_by or "qt_votos").strip().lower()
            if effective_sort_by not in sort_map:
                effective_sort_by = "qt_votos"
            effective_sort_order = (sort_order or "desc").strip().lower()
            reverse = effective_sort_order != "asc"
            items = sorted(items, key=sort_map[effective_sort_by], reverse=reverse)
            total_items = len(items)
            if page is not None and page_size is not None:
                offset = (int(page) - 1) * int(page_size)
                items = items[offset : offset + int(page_size)]
            return {
                "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
                "level": level_key,
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "items": items,
            }
        else:
            if not col_zona:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            normalized_zone = self._normalize_text(candidate_rows[col_zona]).replace("", pd.NA)
            grouped = candidate_rows.assign(
                _zona=normalized_zone,
                _municipio=(
                    self._normalize_text(candidate_rows[col_municipio]).replace("", pd.NA)
                    if col_municipio
                    else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
                ),
                _uf=(
                    self._normalize_text(candidate_rows[col_uf]).str.upper().replace("", pd.NA)
                    if col_uf
                    else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
                ),
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            ).dropna(subset=["_zona"])

            agg = (
                grouped.groupby(["_zona", "_municipio", "_uf"], as_index=False)
                .agg(votes=("_votes", "sum"))
                .sort_values("votes", ascending=False)
            )
            if municipal_scope:
                self._log_municipal_zone_debug(
                    event="candidate_vote_distribution_municipal_zone_base",
                    trace_id=trace_id,
                    normalized_filters={
                        "candidate_id": str(candidate_id),
                        "ano": year,
                        "cargo": office,
                        "uf": normalized_state,
                        "municipio": normalized_municipio,
                        "turno": round_filter,
                        "level": "zona",
                    },
                    plan="pandas_groupby(candidate_rows by zona/municipio/uf after normalized filters)",
                    counts={
                        "rows_candidate": int(len(candidate_rows)),
                        "rows_grouped": int(len(agg)),
                        "total_candidate_votes": int(float(agg["votes"].sum())) if not agg.empty else 0,
                    },
                )
            total_candidate_votes = float(agg["votes"].sum()) if not agg.empty else 0.0
            total_votes_safe = total_candidate_votes if total_candidate_votes > 0 else 1.0
            items: list[dict[str, object]] = []
            for _, row in agg.iterrows():
                zone_id = str(row["_zona"])
                row_votes = float(row["votes"])
                vote_share = round((row_votes / total_votes_safe) * 100, 4)
                if municipal_scope:
                    impact = _impact_category(vote_share)
                    items.append(
                        {
                            "key": f"zona-{zone_id}",
                            "label": f"Zona {zone_id}",
                            "votes": int(row_votes),
                            "vote_share": vote_share,
                            "impact_category": impact,
                            "candidate_total_share": vote_share,
                            "zone_id": zone_id,
                            "zone_name": f"Zona {zone_id}",
                            "municipio": (
                                str(row["_municipio"]) if pd.notna(row["_municipio"]) else None
                            ),
                            "uf": str(row["_uf"]) if pd.notna(row["_uf"]) else None,
                            "categoria_impacto": impact,
                            "percentual_no_total_do_candidato": vote_share,
                        }
                    )
                else:
                    label = (
                        f"{str(row['_municipio'])} - Zona {zone_id}"
                        if pd.notna(row["_municipio"])
                        else f"Zona {zone_id}"
                    )
                    items.append(
                        {
                            "key": zone_id,
                            "label": label,
                            "votes": int(row_votes),
                            "vote_share": vote_share,
                        }
                    )
            sort_map = {
                "key": lambda item: item["key"],
                "label": lambda item: item["label"],
                "votes": lambda item: item["votes"],
                "vote_share": lambda item: item["vote_share"],
                "impact_category": lambda item: str(item.get("impact_category", "")),
                "candidate_total_share": lambda item: float(item.get("candidate_total_share", 0.0)),
                "zone_id": lambda item: str(item.get("zone_id", "")),
                "zone_name": lambda item: str(item.get("zone_name", "")),
            }
            effective_sort_by = (sort_by or "votes").strip().lower()
            if effective_sort_by not in sort_map:
                effective_sort_by = "votes"
            effective_sort_order = (sort_order or "desc").strip().lower()
            reverse = effective_sort_order != "asc"
            items = sorted(items, key=sort_map[effective_sort_by], reverse=reverse)
            total_items = len(items)
            if page is not None and page_size is not None:
                offset = (int(page) - 1) * int(page_size)
                items = items[offset : offset + int(page_size)]
            return {
                "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
                "level": level_key,
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "total_candidate_votes": int(total_candidate_votes),
                "items": items,
            }

        agg = grouped.groupby(["key", "label"], as_index=False).agg(votes=("_votes", "sum")).sort_values("votes", ascending=False)
        total_votes = float(agg["votes"].sum()) or 1.0
        items = [
            {
                "key": str(row["key"]),
                "label": str(row["label"]),
                "votes": int(row["votes"]),
                "vote_share": round((float(row["votes"]) / total_votes) * 100, 4),
            }
            for _, row in agg.iterrows()
        ]
        if page is not None and page_size is not None:
            sort_map = {
                "key": lambda item: item["key"],
                "label": lambda item: item["label"],
                "votes": lambda item: item["votes"],
                "vote_share": lambda item: item["vote_share"],
            }
            effective_sort_by = (sort_by or "votes").strip().lower()
            if effective_sort_by not in sort_map:
                effective_sort_by = "votes"
            effective_sort_order = (sort_order or "desc").strip().lower()
            reverse = effective_sort_order != "asc"
            items = sorted(items, key=sort_map[effective_sort_by], reverse=reverse)
            total_items = len(items)
            offset = (int(page) - 1) * int(page_size)
            items = items[offset : offset + int(page_size)]
            return {
                "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
                "level": level_key,
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "items": items,
            }
        return {
            "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
            "level": level_key,
            "total_items": len(items),
            "items": items,
        }

    def candidate_vote_map(
        self,
        candidate_id: str,
        level: str = "auto",
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        round_filter: int | None = None,
        municipality: str | None = None,
        trace_id: str | None = None,
    ) -> dict:
        requested_level = (level or "auto").strip().lower()
        if requested_level not in {"auto", "municipio", "zona"}:
            requested_level = "auto"
        normalized_state = (state or "").strip().upper() or None
        if normalized_state in {"BR", "BRASIL"}:
            normalized_state = None
        normalized_municipio = self._normalize_municipio_filter(municipality) if municipality else None

        scoped = self._apply_filters(ano=year, turno=round_filter, uf=normalized_state, cargo=office, municipio=normalized_municipio)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if candidate_rows.empty or not col_votes:
            return {
                "candidate_id": str(candidate_id),
                "level": "municipio" if requested_level == "auto" else requested_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }

        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_cd_municipio = self._pick_col(["CD_MUNICIPIO", "COD_MUNICIPIO", "ID_MUNICIPIO"])
        col_zona = self._pick_col(["NR_ZONA", "CD_ZONA", "SQ_ZONA"])
        col_lat = self._pick_col(["LATITUDE", "LAT", "VL_LATITUDE"])
        col_lng = self._pick_col(["LONGITUDE", "LON", "LNG", "VL_LONGITUDE"])

        resolved_level = requested_level
        if resolved_level == "auto":
            if office:
                resolved_level = "zona" if self._cargo_scope(office) == "municipio" else "municipio"
            elif col_cargo:
                cargo_norm = self._normalize_text(candidate_rows[col_cargo]).apply(self._normalize_ascii_upper)
                municipal_mask = cargo_norm.isin(MUNICIPAL_CARGOS)
                if municipal_mask.any() and (~municipal_mask).any():
                    votes_series = pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0)
                    municipal_votes = float(votes_series[municipal_mask].sum())
                    other_votes = float(votes_series[~municipal_mask].sum())
                    resolved_level = "zona" if municipal_votes >= other_votes else "municipio"
                else:
                    resolved_level = "zona" if municipal_mask.any() else "municipio"
            else:
                resolved_level = "municipio"

        if resolved_level == "zona" and not col_zona:
            resolved_level = "municipio"
        if resolved_level == "municipio" and not col_municipio:
            return {
                "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
                "level": resolved_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }
        if office:
            municipal_scope = self._cargo_scope(office) == "municipio"
        elif col_cargo:
            cargo_norm = self._normalize_text(candidate_rows[col_cargo]).apply(self._normalize_ascii_upper)
            municipal_scope = cargo_norm.isin(MUNICIPAL_CARGOS).any()
        else:
            municipal_scope = False

        normalized = candidate_rows.assign(
            _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            _uf=(
                self._normalize_text(candidate_rows[col_uf]).str.upper().replace("", pd.NA)
                if col_uf
                else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
            ),
            _municipio=(
                self._normalize_text(candidate_rows[col_municipio]).replace("", pd.NA)
                if col_municipio
                else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
            ),
            _cd_municipio=(
                self._normalize_text(candidate_rows[col_cd_municipio]).replace("", pd.NA)
                if col_cd_municipio
                else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
            ),
            _zona=(
                self._normalize_text(candidate_rows[col_zona]).replace("", pd.NA)
                if col_zona
                else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
            ),
            _lat=(
                pd.to_numeric(candidate_rows[col_lat], errors="coerce")
                if col_lat
                else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
            ),
            _lng=(
                pd.to_numeric(candidate_rows[col_lng], errors="coerce")
                if col_lng
                else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
            ),
        )

        if resolved_level == "municipio":
            normalized = normalized.dropna(subset=["_municipio"]).copy()
            normalized = normalized.assign(
                _key=normalized["_municipio"].astype(str),
                _label=normalized["_municipio"].astype(str),
            )
            grouped = (
                normalized.groupby(["_key", "_label", "_uf", "_municipio"], as_index=False)
                .agg(votes=("_votes", "sum"), lat=("_lat", "mean"), lng=("_lng", "mean"), _cd_municipio=("_cd_municipio", "first"))
                .sort_values("votes", ascending=False)
            )
        else:
            normalized = normalized.dropna(subset=["_zona"]).copy()
            if municipal_scope:
                normalized = normalized.assign(
                    _key="zona-" + normalized["_zona"].astype(str),
                    _label="Zona " + normalized["_zona"].astype(str),
                )
            else:
                normalized = normalized.assign(
                    _key=(
                        normalized["_uf"].fillna("N/A").astype(str)
                        + "|"
                        + normalized["_municipio"].fillna("N/A").astype(str)
                        + "|"
                        + normalized["_zona"].fillna("N/A").astype(str)
                    ),
                    _label=(
                        normalized["_municipio"].fillna("N/A").astype(str)
                        + " - Zona "
                        + normalized["_zona"].fillna("N/A").astype(str)
                    ),
                )
            grouped = (
                normalized.groupby(["_key", "_label", "_uf", "_municipio", "_zona"], as_index=False)
                .agg(votes=("_votes", "sum"), lat=("_lat", "mean"), lng=("_lng", "mean"), _cd_municipio=("_cd_municipio", "first"))
                .sort_values("votes", ascending=False)
            )
            if municipal_scope:
                self._log_municipal_zone_debug(
                    event="candidate_vote_map_municipal_zone_base",
                    trace_id=trace_id,
                    normalized_filters={
                        "candidate_id": str(candidate_id),
                        "ano": year,
                        "cargo": office,
                        "uf": normalized_state,
                        "municipio": normalized_municipio,
                        "turno": round_filter,
                        "level": "zona",
                    },
                    plan="pandas_groupby(candidate_rows by key/zona after normalized filters)",
                    counts={
                        "rows_candidate": int(len(candidate_rows)),
                        "rows_grouped": int(len(grouped)),
                        "total_candidate_votes": int(float(grouped["votes"].sum())) if not grouped.empty else 0,
                    },
                )

        valid_coord_indices: list[int] = []
        for idx, row in grouped.iterrows():
            try:
                lat, lng = self._coerce_valid_coords(row.get("lat"), row.get("lng"))
                if lat is None or lng is None:
                    lat, lng = self._resolve_municipality_coords(
                        city=(str(row["_municipio"]) if pd.notna(row["_municipio"]) else None),
                        uf=(str(row["_uf"]) if pd.notna(row["_uf"]) else None),
                        code=(row["_cd_municipio"] if "_cd_municipio" in row.index else None),
                        lat=lat,
                        lng=lng,
                    )
                if (lat is None or lng is None) and resolved_level == "zona":
                    geometry = None
                    try:
                        geometry = self._resolve_zone_geometry(
                            zone_id=str(row["_zona"]),
                            city=(str(row["_municipio"]) if pd.notna(row["_municipio"]) else None),
                            uf=(str(row["_uf"]) if pd.notna(row["_uf"]) else None),
                            lat=float("nan"),
                            lng=float("nan"),
                        )
                    except Exception as exc:
                        logger.warning(
                            json.dumps(
                                {
                                    "event": "candidate_vote_map_zone_geometry_error",
                                    "trace_id": trace_id or "n/a",
                                    "zona": str(row["_zona"]) if "_zona" in row.index else None,
                                    "municipio": str(row["_municipio"]) if "_municipio" in row.index and pd.notna(row["_municipio"]) else None,
                                    "uf": str(row["_uf"]) if "_uf" in row.index and pd.notna(row["_uf"]) else None,
                                    "error_type": exc.__class__.__name__,
                                    "error_message": str(exc),
                                },
                                ensure_ascii=False,
                            )
                        )
                    geo_lat, geo_lng = self._geometry_centroid(geometry)
                    lat, lng = self._coerce_valid_coords(geo_lat, geo_lng)
            except Exception as exc:
                logger.warning(
                    json.dumps(
                        {
                            "event": "candidate_vote_map_geo_resolution_error",
                            "trace_id": trace_id or "n/a",
                            "zona": str(row["_zona"]) if "_zona" in row.index else None,
                            "municipio": str(row["_municipio"]) if "_municipio" in row.index and pd.notna(row["_municipio"]) else None,
                            "uf": str(row["_uf"]) if "_uf" in row.index and pd.notna(row["_uf"]) else None,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc),
                        },
                        ensure_ascii=False,
                    )
                )
                lat, lng = None, None
            if lat is None or lng is None:
                logger.warning(
                    json.dumps(
                        {
                            "event": "candidate_vote_map_zone_missing_coords_dropped",
                            "trace_id": trace_id or "n/a",
                            "zona": str(row["_zona"]) if "_zona" in row.index else None,
                            "municipio": str(row["_municipio"]) if "_municipio" in row.index and pd.notna(row["_municipio"]) else None,
                            "uf": str(row["_uf"]) if "_uf" in row.index and pd.notna(row["_uf"]) else None,
                        },
                        ensure_ascii=False,
                    )
                )
                continue
            grouped.at[idx, "lat"] = lat
            grouped.at[idx, "lng"] = lng
            valid_coord_indices.append(idx)

        if valid_coord_indices:
            grouped = grouped.loc[valid_coord_indices].copy()
        else:
            grouped = grouped.iloc[0:0].copy()

        if grouped.empty:
            return {
                "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
                "level": resolved_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }

        votes = pd.to_numeric(grouped["votes"], errors="coerce").dropna()
        if votes.empty:
            return {
                "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
                "level": resolved_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }
        q1 = int(votes.quantile(0.33))
        q2 = int(votes.quantile(0.66))
        if q2 < q1:
            q2 = q1

        tiers = {
            0: {"rgb": "#22C55E", "marker_size": 32, "cluster_size": 40},
            1: {"rgb": "#FACC15", "marker_size": 34, "cluster_size": 52},
            2: {"rgb": "#EF4444", "marker_size": 42, "cluster_size": 60},
        }

        total_votes = int(votes.sum())
        total_votes_safe = float(total_votes) if total_votes > 0 else 1.0
        items: list[dict] = []
        for _, row in grouped.sort_values("votes", ascending=False).iterrows():
            row_votes_num = pd.to_numeric(row["votes"], errors="coerce")
            row_votes = int(float(row_votes_num)) if pd.notna(row_votes_num) else 0
            tier = 0 if row_votes <= q1 else (1 if row_votes <= q2 else 2)
            cfg = tiers[tier]
            label = str(row["_label"])
            tooltip_votes = f"{row_votes:,}".replace(",", ".")
            items.append(
                {
                    "key": str(row["_key"]),
                    "label": label,
                    "votes": row_votes,
                    "vote_share": round((row_votes / total_votes_safe) * 100, 4),
                    "tier": tier,
                    "color_rgb": cfg["rgb"],
                    "marker_size": cfg["marker_size"],
                    "cluster_size": cfg["cluster_size"],
                    "lat": float(row["lat"]) if pd.notna(row["lat"]) else None,
                    "lng": float(row["lng"]) if pd.notna(row["lng"]) else None,
                    "uf": str(row["_uf"]) if "_uf" in row.index and pd.notna(row["_uf"]) else None,
                    "municipio": (
                        str(row["_municipio"]) if "_municipio" in row.index and pd.notna(row["_municipio"]) else None
                    ),
                    "zona": str(row["_zona"]) if "_zona" in row.index and pd.notna(row["_zona"]) else None,
                    "tooltip": f"{label} • {tooltip_votes} votos",
                }
            )

        return {
            "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
            "level": resolved_level,
            "quantile_q1": q1,
            "quantile_q2": q2,
            "total_votes": total_votes,
            "items": items,
            "region_clusters": [] if (resolved_level == "zona" and municipal_scope) else self._region_clusters_from_points(items),
        }

    def candidate_zone_fidelity(
        self,
        candidate_id: str,
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        include_geometry: bool = False,
    ) -> dict:
        scoped_all_years = self._apply_filters(uf=state, cargo=office)
        candidate_all_years = scoped_all_years[self._candidate_mask(scoped_all_years, candidate_id)].copy()
        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_zona = self._pick_col(["NR_ZONA", "CD_ZONA", "SQ_ZONA"])
        col_year = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        if candidate_all_years.empty or not col_votes or not col_zona:
            return {"candidate_id": str(candidate_id), "items": []}

        col_zone_name = self._pick_col(["NM_ZONA", "DS_ZONA"])
        col_city = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_uf = self._pick_col(["SG_UF"])
        col_lat = self._pick_col(["LATITUDE", "LAT", "VL_LATITUDE"])
        col_lng = self._pick_col(["LONGITUDE", "LON", "LNG", "VL_LONGITUDE"])

        resolved_year = year
        if col_year:
            year_series = pd.to_numeric(candidate_all_years[col_year], errors="coerce").dropna().astype(int)
            if resolved_year is None and not year_series.empty:
                resolved_year = int(year_series.max())
            if resolved_year is not None:
                candidate_current = candidate_all_years[pd.to_numeric(candidate_all_years[col_year], errors="coerce") == int(resolved_year)].copy()
                prev_years = year_series[year_series < int(resolved_year)] if not year_series.empty else pd.Series(dtype=int)
                previous_year = int(prev_years.max()) if not prev_years.empty else None
                candidate_previous = (
                    candidate_all_years[pd.to_numeric(candidate_all_years[col_year], errors="coerce") == previous_year].copy()
                    if previous_year is not None
                    else candidate_all_years.iloc[0:0].copy()
                )
            else:
                candidate_current = candidate_all_years.copy()
                candidate_previous = candidate_all_years.iloc[0:0].copy()
        else:
            candidate_current = candidate_all_years.copy()
            candidate_previous = candidate_all_years.iloc[0:0].copy()

        if candidate_current.empty:
            return {"candidate_id": str(candidate_id), "items": []}

        tmp = candidate_current.assign(
            _zone_id=self._normalize_text(candidate_current[col_zona]).replace("", "N/A"),
            _zone_name=(
                self._normalize_text(candidate_current[col_zone_name]).replace("", pd.NA)
                if col_zone_name
                else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)
            ),
            _city=(
                self._normalize_text(candidate_current[col_city]).replace("", pd.NA)
                if col_city
                else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)
            ),
            _uf=(
                self._normalize_text(candidate_current[col_uf]).str.upper().replace("", pd.NA)
                if col_uf
                else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)
            ),
            _votes=pd.to_numeric(candidate_current[col_votes], errors="coerce").fillna(0),
            _lat=(pd.to_numeric(candidate_current[col_lat], errors="coerce") if col_lat else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)),
            _lng=(pd.to_numeric(candidate_current[col_lng], errors="coerce") if col_lng else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)),
        )

        previous_votes_by_zone: dict[str, float] = {}
        if not candidate_previous.empty:
            prev_tmp = candidate_previous.assign(
                _zone_id=self._normalize_text(candidate_previous[col_zona]).replace("", "N/A"),
                _votes=pd.to_numeric(candidate_previous[col_votes], errors="coerce").fillna(0),
            )
            previous_votes_by_zone = (
                prev_tmp.groupby("_zone_id", as_index=False)
                .agg(votes=("_votes", "sum"))
                .set_index("_zone_id")["votes"]
                .to_dict()
            )

        grouped = (
            tmp.groupby("_zone_id", as_index=False)
            .agg(
                votes=("_votes", "sum"),
                zone_name=("_zone_name", "first"),
                city=("_city", "first"),
                uf=("_uf", "first"),
                lat=("_lat", "mean"),
                lng=("_lng", "mean"),
            )
            .sort_values("votes", ascending=False)
        )

        with_geometry_items: list[dict] = []
        without_geometry_items: list[dict] = []
        for _, row in grouped.iterrows():
            zone_name = str(row["zone_name"]) if pd.notna(row["zone_name"]) and str(row["zone_name"]).strip() else f"Zona {row['_zone_id']}"
            prev_votes = float(previous_votes_by_zone.get(str(row["_zone_id"]), 0.0))
            if prev_votes > 0:
                retention = min(round((float(row["votes"]) / prev_votes) * 100, 4), 100.0)
            else:
                retention = 100.0
            lat, lng = self._coerce_valid_coords(row["lat"], row["lng"])
            geometry = None
            if include_geometry or lat is None or lng is None:
                geometry = self._resolve_zone_geometry(
                    zone_id=str(row["_zone_id"]),
                    city=(str(row["city"]) if pd.notna(row["city"]) else None),
                    uf=(str(row["uf"]) if pd.notna(row["uf"]) else None),
                    lat=(lat if lat is not None else float("nan")),
                    lng=(lng if lng is not None else float("nan")),
                )
            if lat is None or lng is None:
                geo_lat, geo_lng = self._geometry_centroid(geometry)
                lat, lng = self._coerce_valid_coords(geo_lat, geo_lng)
            if lat is None or lng is None:
                continue
            item = {
                "zone_id": str(row["_zone_id"]),
                "zone_name": zone_name,
                "city": str(row["city"]) if pd.notna(row["city"]) else None,
                "votes": int(row["votes"]),
                "retention": retention,
                "lat": float(lat),
                "lng": float(lng),
            }
            if include_geometry:
                if geometry is None:
                    geometry = self._resolve_zone_geometry(
                        zone_id=str(row["_zone_id"]),
                        city=(str(row["city"]) if pd.notna(row["city"]) else None),
                        uf=(str(row["uf"]) if pd.notna(row["uf"]) else None),
                        lat=float(lat),
                        lng=float(lng),
                    )
                item["geometry"] = geometry
                if geometry is not None:
                    with_geometry_items.append(item)
                else:
                    without_geometry_items.append(item)
            else:
                without_geometry_items.append(item)

        items = with_geometry_items + without_geometry_items if include_geometry else without_geometry_items
        return {"candidate_id": self._candidate_output_id(candidate_current, candidate_id), "items": items}

    def candidates_compare(
        self,
        candidate_ids: list[str],
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        candidate_ids_clean = [str(v).strip() for v in candidate_ids if str(v).strip()]
        if len(candidate_ids_clean) < 2:
            return {"context": {"year": year, "state": state, "office": office}, "candidates": [], "deltas": []}

        base_scoped = self._apply_filters(uf=state, cargo=office)
        col_year = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        resolved_year = year
        if resolved_year is None and col_year and not base_scoped.empty:
            year_series = pd.to_numeric(base_scoped[col_year], errors="coerce").dropna()
            if not year_series.empty:
                resolved_year = int(year_series.max())
        resolved_year = int(resolved_year) if resolved_year is not None else None

        if resolved_year is not None:
            context_df = self._apply_filters(ano=resolved_year, uf=state, cargo=office)
        else:
            context_df = base_scoped.copy()

        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_party = self._pick_col(["SG_PARTIDO"])
        col_name = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if context_df.empty or not col_votes:
            return {
                "context": {"year": resolved_year, "state": state, "office": office},
                "candidates": [],
                "deltas": [],
            }

        context_df = context_df.assign(_votes=pd.to_numeric(context_df[col_votes], errors="coerce").fillna(0))
        total_context_votes = float(context_df["_votes"].sum()) or 1.0

        ranking_df = context_df.assign(
            _candidate_key=(
                self._normalize_text(context_df["SQ_CANDIDATO"])
                if "SQ_CANDIDATO" in context_df.columns
                else (
                    self._normalize_text(context_df["NR_CANDIDATO"])
                    if "NR_CANDIDATO" in context_df.columns
                    else self._normalize_text(context_df[col_name]).str.lower()
                )
            )
        )
        ranking = (
            ranking_df.groupby("_candidate_key", as_index=False)
            .agg(votes=("_votes", "sum"))
            .sort_values("votes", ascending=False)
            .reset_index(drop=True)
        )
        rank_map = {str(row["_candidate_key"]): idx + 1 for idx, (_, row) in enumerate(ranking.iterrows())}

        candidates_out: list[dict] = []
        for cid in candidate_ids_clean:
            candidate_rows = context_df[self._candidate_mask(context_df, cid)].copy()
            if candidate_rows.empty:
                continue
            votes = int(candidate_rows["_votes"].sum())
            name = (
                str(self._normalize_text(candidate_rows[col_name]).replace("", pd.NA).dropna().iloc[0])
                if col_name and self._normalize_text(candidate_rows[col_name]).replace("", pd.NA).dropna().any()
                else ""
            )
            party = (
                str(self._normalize_text(candidate_rows[col_party]).replace("", pd.NA).dropna().iloc[0])
                if col_party and self._normalize_text(candidate_rows[col_party]).replace("", pd.NA).dropna().any()
                else None
            )
            key = (
                str(self._normalize_text(candidate_rows["SQ_CANDIDATO"]).replace("", pd.NA).dropna().iloc[0])
                if "SQ_CANDIDATO" in candidate_rows.columns
                and self._normalize_text(candidate_rows["SQ_CANDIDATO"]).replace("", pd.NA).dropna().any()
                else (
                    str(self._normalize_text(candidate_rows["NR_CANDIDATO"]).replace("", pd.NA).dropna().iloc[0])
                    if "NR_CANDIDATO" in candidate_rows.columns
                    and self._normalize_text(candidate_rows["NR_CANDIDATO"]).replace("", pd.NA).dropna().any()
                    else self._normalize_text(candidate_rows[col_name]).str.lower().iloc[0]
                )
            )

            history = self.candidate_vote_history(candidate_id=cid, state=state, office=office).get("items", [])
            history_sorted = sorted(history, key=lambda item: int(item["year"]))
            retention = 100.0
            if history_sorted:
                current = next((h for h in history_sorted if int(h["year"]) == int(resolved_year or h["year"])), history_sorted[-1])
                previous_items = [h for h in history_sorted if int(h["year"]) < int(current["year"])]
                if previous_items:
                    prev_votes = float(previous_items[-1]["votes"])
                    retention = round((float(current["votes"]) / prev_votes) * 100, 4) if prev_votes > 0 else 100.0
                else:
                    retention = 100.0

            candidates_out.append(
                {
                    "candidate_id": self._candidate_output_id(candidate_rows, cid),
                    "name": name,
                    "party": party,
                    "votes": votes,
                    "vote_share": round((votes / total_context_votes) * 100, 4),
                    "retention": retention,
                    "state_rank": rank_map.get(str(key)),
                }
            )

        deltas = []
        for metric in ["votes", "vote_share", "retention"]:
            ranked_metric = sorted(candidates_out, key=lambda item: float(item.get(metric) or 0), reverse=True)
            if len(ranked_metric) >= 2:
                gap = float(ranked_metric[0].get(metric) or 0) - float(ranked_metric[1].get(metric) or 0)
                deltas.append(
                    {
                        "metric": metric,
                        "best_candidate_id": ranked_metric[0]["candidate_id"],
                        "gap_to_second": round(gap, 4),
                    }
                )

        return {
            "context": {"year": resolved_year, "state": state, "office": office},
            "candidates": candidates_out,
            "deltas": deltas,
        }

    def search_candidates(
        self,
        q: str,
        ano: int,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        partido: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        df = self._apply_filters(ano=ano, turno=turno, uf=uf, cargo=cargo, partido=partido)

        col_candidato = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        col_candidate_id = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_numero = self._pick_col(["NR_CANDIDATO"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if not col_candidato:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        query = str(q or "").strip()
        if not query:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}
        q_norm = self._normalize_ascii_upper(query).lower()
        if not q_norm:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        names = self._normalize_text(df[col_candidato])
        names_norm = names.apply(self._normalize_ascii_upper).str.lower()

        contains = names_norm.str.contains(q_norm, regex=False)
        matched = contains
        if not matched.any():
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        ranked = df.loc[matched].copy()
        name_match_norm = names_norm.loc[matched]
        score = (
            (name_match_norm == q_norm).astype(int) * 3
            + (name_match_norm.str.startswith(q_norm)).astype(int) * 2
            + (name_match_norm.str.contains(q_norm, regex=False)).astype(int)
        )
        has_votos = bool(col_votos)
        if col_votos:
            votos = pd.to_numeric(ranked[col_votos], errors="coerce").fillna(0).astype("int64")
        else:
            votos = pd.Series(0, index=ranked.index, dtype="int64")
        ranked = ranked.assign(
            _score=score,
            _votos=votos,
            _candidate_id=(
                self._normalize_text(ranked[col_candidate_id]).replace("", pd.NA)
                if col_candidate_id
                else self._normalize_text(ranked[col_candidato]).replace("", pd.NA)
            ),
            _candidato=self._normalize_text(ranked[col_candidato]),
            _partido=self._normalize_text(ranked[col_partido]).replace("", pd.NA) if col_partido else pd.NA,
            _cargo=self._normalize_text(ranked[col_cargo]).replace("", pd.NA) if col_cargo else pd.NA,
            _uf=self._normalize_text(ranked[col_uf]).str.upper().replace("", pd.NA) if col_uf else pd.NA,
            _numero=self._normalize_text(ranked[col_numero]).replace("", pd.NA) if col_numero else pd.NA,
            _situacao=self._normalize_text(ranked[col_situacao]).replace("", pd.NA) if col_situacao else pd.NA,
        )

        grouped = (
            ranked.groupby(
                ["_candidate_id", "_candidato", "_partido", "_cargo", "_uf", "_numero", "_situacao"],
                dropna=False,
                as_index=False,
            )
            .agg(_score=("_score", "max"), _votos=("_votos", "sum"))
            .sort_values(by=["_score", "_votos", "_candidato"], ascending=[False, False, True])
        )

        total = int(len(grouped))
        total_pages = (total + page_size - 1) // page_size
        start = (page - 1) * page_size
        end = start + page_size
        sliced = grouped.iloc[start:end]

        items: list[dict] = []
        for _, row in sliced.iterrows():
            candidate_id_raw = (
                row["_candidate_id"]
                if pd.notna(row["_candidate_id"]) and str(row["_candidate_id"]).strip()
                else row["_candidato"]
            )
            candidate_id = str(candidate_id_raw).strip() if pd.notna(candidate_id_raw) else ""
            if not candidate_id:
                continue
            numero_raw = row["_numero"] if pd.notna(row["_numero"]) and str(row["_numero"]).strip() else candidate_id
            items.append(
                {
                    "candidate_id": candidate_id,
                    "candidato": (str(row["_candidato"]) if pd.notna(row["_candidato"]) else ""),
                    "partido": (str(row["_partido"]) if pd.notna(row["_partido"]) else None),
                    "cargo": (str(row["_cargo"]) if pd.notna(row["_cargo"]) else None),
                    "uf": (str(row["_uf"]) if pd.notna(row["_uf"]) else None),
                    "numero": (str(numero_raw) if pd.notna(numero_raw) else None),
                    "votos": int(row["_votos"]) if has_votos else None,
                    "situacao": (str(row["_situacao"]) if pd.notna(row["_situacao"]) else None),
                }
            )

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "items": items,
        }

    def official_vacancies(
        self,
        group_by: str = "cargo",
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
    ) -> dict:
        allowed_groups = {"cargo", "uf", "municipio"}
        if group_by not in allowed_groups:
            return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}

        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        col_qt_vagas = self._pick_col(["QT_VAGAS", "QTD_VAGAS", "QTDE_VAGAS"])

        if not col_candidato or not col_situacao:
            return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}

        if group_by == "cargo" and not col_cargo:
            return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}
        if group_by == "uf" and not col_uf:
            return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}
        if group_by == "municipio" and (not col_municipio or not col_cargo):
            return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}

        df = self._apply_filters(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=False,
        )
        if df.empty:
            return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}

        if group_by == "municipio":
            cargos_upper = self._normalize_text(df[col_cargo]).str.upper()
            df = df[cargos_upper.isin(MUNICIPAL_CARGOS)]
            if df.empty:
                return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}

        group_col_map = {"cargo": col_cargo, "uf": col_uf, "municipio": col_municipio}
        group_col = group_col_map[group_by]

        if col_qt_vagas:
            tmp = df.assign(
                _ano=(
                    pd.to_numeric(df[col_ano], errors="coerce").astype("Int64")
                    if col_ano
                    else pd.Series([pd.NA] * len(df), index=df.index)
                ),
                _uf=(
                    self._normalize_text(df[col_uf]).str.upper().replace("", "N/A")
                    if col_uf
                    else pd.Series(["N/A"] * len(df), index=df.index)
                ),
                _cargo=(
                    self._normalize_text(df[col_cargo]).replace("", "N/A")
                    if col_cargo
                    else pd.Series(["N/A"] * len(df), index=df.index)
                ),
                _municipio=(
                    self._normalize_text(df[col_municipio]).replace("", "N/A")
                    if col_municipio
                    else pd.Series(["N/A"] * len(df), index=df.index)
                ),
                _group=self._normalize_text(df[group_col]).replace("", "N/A"),
                _qt_vagas=pd.to_numeric(df[col_qt_vagas], errors="coerce").fillna(0),
            )
            tmp["_scope"] = tmp["_cargo"].apply(self._cargo_scope)
            tmp["_unit_key"] = tmp.apply(
                lambda row: (
                    f"{row['_ano']}|{row['_cargo']}"
                    if row["_scope"] == "nacional"
                    else (
                        f"{row['_ano']}|{row['_uf']}|{row['_cargo']}"
                        if row["_scope"] == "uf"
                        else f"{row['_ano']}|{row['_uf']}|{row['_cargo']}|{row['_municipio']}"
                    )
                ),
                axis=1,
            )
            unit = (
                tmp.groupby("_unit_key", as_index=False)
                .agg(
                    vagas_oficiais=("_qt_vagas", "max"),
                    ano=("_ano", "max"),
                    uf=("_uf", "max"),
                    cargo=("_cargo", "max"),
                    municipio=("_municipio", "max"),
                    group_value=("_group", "max"),
                    scope=("_scope", "max"),
                )
                .copy()
            )
            if group_by == "municipio":
                unit = unit[unit["scope"] == "municipio"]
            grouped = (
                unit.groupby("group_value", as_index=False)
                .agg(vagas_oficiais=("vagas_oficiais", "sum"))
                .sort_values("vagas_oficiais", ascending=False)
            )
        else:
            elected = df[self._is_elected(df[col_situacao])].copy()
            if elected.empty:
                return {"group_by": group_by, "total_vagas_oficiais": 0, "items": []}
            dedup_keys = [group_col, col_candidato]
            if col_ano:
                dedup_keys.append(col_ano)
            if col_cargo:
                dedup_keys.append(col_cargo)
            if group_by == "municipio" and col_municipio:
                dedup_keys.append(col_municipio)
            dedup = elected.drop_duplicates(subset=dedup_keys, keep="first").copy()
            grouped = (
                dedup.assign(_group=self._normalize_text(dedup[group_col]).replace("", "N/A"))
                .groupby("_group", dropna=False)
                .agg(vagas_oficiais=("_group", "size"))
                .reset_index()
                .sort_values("vagas_oficiais", ascending=False)
            )

        items: list[dict] = []
        for _, row in grouped.iterrows():
            group_col_name = "group_value" if "group_value" in grouped.columns else "_group"
            group_val = str(row[group_col_name]) if pd.notna(row[group_col_name]) else "N/A"
            items.append(
                {
                    "ano": int(ano) if ano is not None else None,
                    "uf": (group_val if group_by == "uf" else (uf.upper().strip() if uf else None)),
                    "cargo": (group_val if group_by == "cargo" else (cargo.strip() if cargo else None)),
                    "municipio": (
                        group_val if group_by == "municipio" else (municipio.strip() if municipio else None)
                    ),
                    "vagas_oficiais": int(row["vagas_oficiais"]),
                }
            )

        return {
            "group_by": group_by,
            "total_vagas_oficiais": int(sum(item["vagas_oficiais"] for item in items)),
            "items": items,
        }
