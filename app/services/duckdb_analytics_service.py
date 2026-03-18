from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
from pathlib import Path
from threading import Lock
import unicodedata

import duckdb
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

SUPPORTED_ANALYTICS_YEARS = {2018, 2020, 2022, 2024}
logger = logging.getLogger("api.meucandidato")


@dataclass
class DuckDBAnalyticsService:
    conn: duckdb.DuckDBPyConnection
    default_top_n: int
    max_top_n: int
    _query_lock: Lock
    _columns: set[str]
    _source_path: Path | None = field(default=None, init=False, repr=False)
    _official_prefeito_totals_cache: dict[int, dict[str, int]] = field(default_factory=dict, init=False, repr=False)
    _zone_geometry_index_cache: dict[str, object] | None = field(default=None, init=False, repr=False)

    @classmethod
    def from_file(
        cls,
        file_path: str,
        default_top_n: int,
        max_top_n: int,
        separator: str = ",",
        encoding: str = "utf-8",
        materialize_table: bool = False,
        create_indexes: bool = True,
        memory_limit_mb: int | None = None,
        threads: int | None = None,
    ) -> "DuckDBAnalyticsService":
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)

        conn = duckdb.connect(database=":memory:")
        if memory_limit_mb is not None and int(memory_limit_mb) > 0:
            try:
                conn.execute(f"SET memory_limit='{int(memory_limit_mb)}MB'")
            except Exception:
                pass
        if threads is not None and int(threads) > 0:
            try:
                conn.execute(f"SET threads TO {int(threads)}")
            except Exception:
                pass

        suffix = path.suffix.lower()
        source_path = str(path).replace("'", "''")
        delim = separator.replace("'", "''")
        if materialize_table:
            if suffix == ".parquet":
                conn.execute(
                    f"CREATE OR REPLACE TABLE analytics AS SELECT * FROM read_parquet('{source_path}')"
                )
            elif suffix == ".csv":
                conn.execute(
                    "CREATE OR REPLACE TABLE analytics AS "
                    f"SELECT * FROM read_csv_auto('{source_path}', delim='{delim}', header=true, ignore_errors=true)"
                )
            else:
                raise ValueError("Formato de analytics nao suportado. Use .csv ou .parquet.")
        else:
            if suffix == ".parquet":
                conn.execute(
                    f"CREATE OR REPLACE VIEW analytics AS SELECT * FROM read_parquet('{source_path}')"
                )
            elif suffix == ".csv":
                conn.execute(
                    "CREATE OR REPLACE VIEW analytics AS "
                    f"SELECT * FROM read_csv_auto('{source_path}', delim='{delim}', header=true, ignore_errors=true)"
                )
            else:
                raise ValueError("Formato de analytics nao suportado. Use .csv ou .parquet.")

        cols = {str(v[0]).upper() for v in conn.execute("DESCRIBE analytics").fetchall()}
        service = cls(
            conn=conn,
            default_top_n=default_top_n,
            max_top_n=max_top_n,
            _query_lock=Lock(),
            _columns=cols,
        )
        service._source_path = path
        if create_indexes and materialize_table:
            service._create_indexes()
        return service

    def _create_index(self, index_name: str, columns: list[str]) -> None:
        try:
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON analytics ({', '.join(columns)})")
            logger.info(
                json.dumps(
                    {"event": "duckdb_index_created", "index_name": index_name, "columns": columns},
                    ensure_ascii=False,
                )
            )
        except Exception as exc:
            logger.info(
                json.dumps(
                    {
                        "event": "duckdb_index_skipped",
                        "index_name": index_name,
                        "columns": columns,
                        "error_type": exc.__class__.__name__,
                        "error_message": str(exc),
                    },
                    ensure_ascii=False,
                )
            )

    def _create_indexes(self) -> None:
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])

        if col_ano and col_uf and col_cargo and col_votos:
            self._create_index("idx_analytics_ano_uf_cargo_votos", [col_ano, col_uf, col_cargo, col_votos])
        if col_ano and col_uf and col_cargo:
            self._create_index("idx_analytics_ano_uf_cargo", [col_ano, col_uf, col_cargo])
        if col_ano and col_cargo:
            self._create_index("idx_analytics_ano_cargo", [col_ano, col_cargo])
        if col_votos:
            self._create_index("idx_analytics_votos", [col_votos])

    @property
    def row_count(self) -> int:
        return int(self._scalar("SELECT COUNT(*) FROM analytics") or 0)

    def _has(self, col: str) -> bool:
        return col.upper() in self._columns

    def _pick_col(self, options: list[str]) -> str | None:
        for col in options:
            if self._has(col):
                return col
        return None

    def _scalar(self, query: str, params: list | None = None):
        with self._query_lock:
            return self.conn.execute(query, params or []).fetchone()[0]

    def _rows(self, query: str, params: list | None = None) -> list[tuple]:
        with self._query_lock:
            return self.conn.execute(query, params or []).fetchall()

    def _normalize_value(self, value: object, uppercase: bool = False) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return ascii_text.upper() if uppercase else ascii_text

    def _parse_int(self, value: object) -> int | None:
        try:
            if value is None:
                return None
            return int(float(str(value).strip()))
        except (TypeError, ValueError):
            return None

    def _parse_turno(self, value: object) -> int:
        if value is None:
            return 0
        text = str(value).strip()
        if not text:
            return 0
        if text.isdigit():
            return int(text)
        digits = "".join(ch for ch in text if ch.isdigit())
        return int(digits) if digits else 0

    def _party_spectrum(self, partido: str | None) -> str:
        norm = self._normalize_value(partido, uppercase=True)
        if not norm:
            return "indefinido"
        return PARTIDO_SPECTRUM_MAP.get(norm, "indefinido")

    def _cargo_scope(self, cargo: str | None) -> str:
        cargo_up = self._normalize_value(cargo or "", uppercase=True)
        if cargo_up in MUNICIPAL_CARGOS:
            return "municipio"
        if cargo_up in NATIONAL_CARGOS:
            return "nacional"
        return "uf"

    def _is_elected_series(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip().str.upper().str.startswith("ELEITO")

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
            _uf=vagas_df["SG_UF"].fillna("").astype(str).str.upper().str.strip(),
            _municipio=vagas_df["NM_UE"].fillna("").astype(str).str.upper().str.strip(),
            _cargo=vagas_df["DS_CARGO"].fillna("").astype(str).str.upper().str.strip(),
            _qt_vaga=pd.to_numeric(vagas_df["QT_VAGA"], errors="coerce").fillna(0),
        )
        df = df[(df["_cargo"] == "PREFEITO") & (df["_qt_vaga"] >= 1)]
        df = df[(df["_uf"] != "") & (df["_municipio"] != "")]
        if df.empty:
            self._official_prefeito_totals_cache[ano] = {}
            return {}

        result = (
            df.drop_duplicates(subset=["_uf", "_municipio"])
            .groupby("_uf")
            .size()
            .astype(int)
            .to_dict()
        )
        self._official_prefeito_totals_cache[ano] = result
        return result

    def _where(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        partido: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> tuple[str, list]:
        clauses = []
        params: list = []

        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_turno = self._pick_col(["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if ano is not None and col_ano:
            clauses.append(f"TRY_CAST({col_ano} AS BIGINT) = ?")
            params.append(ano)
        if turno is not None and col_turno:
            clauses.append(
                (
                    "COALESCE("
                    f"TRY_CAST({col_turno} AS BIGINT), "
                    f"TRY_CAST(regexp_extract(CAST({col_turno} AS VARCHAR), '(\\d+)', 1) AS BIGINT)"
                    ") = ?"
                )
            )
            params.append(turno)
        if uf and col_uf:
            clauses.append(f"UPPER(TRIM(CAST({col_uf} AS VARCHAR))) = ?")
            params.append(uf.upper())
        if cargo and col_cargo:
            clauses.append(f"LOWER(TRIM(CAST({col_cargo} AS VARCHAR))) = ?")
            params.append(cargo.lower())
        if partido and col_partido:
            clauses.append(f"UPPER(TRIM(CAST({col_partido} AS VARCHAR))) = ?")
            params.append(partido.upper().strip())
        if municipio and col_municipio:
            clauses.append(f"UPPER(TRIM(CAST({col_municipio} AS VARCHAR))) = ?")
            params.append(municipio.upper().strip())
        if somente_eleitos and col_situacao:
            clauses.append(f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) LIKE 'ELEITO%'")

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

    def _df(self, query: str, params: list | None = None) -> pd.DataFrame:
        with self._query_lock:
            return self.conn.execute(query, params or []).df()

    def _filtered_df(
        self,
        ano: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
    ) -> pd.DataFrame:
        where, params = self._where(ano=ano, uf=uf, cargo=cargo, municipio=municipio)
        return self._df(f"SELECT * FROM analytics {where}", params)

    def _candidate_mask(self, df: pd.DataFrame, candidate_id: str) -> pd.Series:
        candidate_norm = str(candidate_id or "").strip()
        candidate_ascii = self._normalize_value(candidate_norm, uppercase=True)
        if not candidate_norm:
            return pd.Series([False] * len(df), index=df.index)

        masks: list[pd.Series] = []
        if "SQ_CANDIDATO" in df.columns:
            masks.append(df["SQ_CANDIDATO"].fillna("").astype(str).str.strip() == candidate_norm)
        if "NR_CANDIDATO" in df.columns:
            masks.append(df["NR_CANDIDATO"].fillna("").astype(str).str.strip() == candidate_norm)
        col_nome = "NM_CANDIDATO" if "NM_CANDIDATO" in df.columns else ("NM_URNA_CANDIDATO" if "NM_URNA_CANDIDATO" in df.columns else None)
        if col_nome:
            masks.append(
                df[col_nome].fillna("").astype(str).str.strip().apply(lambda value: self._normalize_value(value, uppercase=True))
                == candidate_ascii
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
                values = df[col].fillna("").astype(str).str.strip()
                values = values[values != ""]
                if not values.empty:
                    return str(values.iloc[0])
        return str(fallback)

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
                        city_val = self._normalize_value(str(value), uppercase=True)
                        break

                index.setdefault(zone_val, geometry)
                if uf_val:
                    index.setdefault(f"{uf_val}|{zone_val}", geometry)
                    if city_val:
                        index.setdefault(f"{uf_val}|{city_val}|{zone_val}", geometry)

        self._zone_geometry_index_cache = index
        return index

    def _resolve_zone_geometry(self, zone_id: str, city: str | None, uf: str | None, lat: float, lng: float) -> object | None:
        zone_key = self._normalize_value(zone_id, uppercase=True)
        uf_key = self._normalize_value(uf or "", uppercase=True)
        city_key = self._normalize_value(city or "", uppercase=True)
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

    def filter_options(self) -> dict:
        anos: list[int] = []
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        if col_ano:
            rows = self._rows(
                (
                    f"SELECT DISTINCT TRY_CAST({col_ano} AS BIGINT) AS ano "
                    f"FROM analytics WHERE {col_ano} IS NOT NULL ORDER BY ano"
                )
            )
            anos = [int(r[0]) for r in rows if r[0] is not None]

        ufs: list[str] = []
        col_uf = self._pick_col(["SG_UF"])
        if col_uf:
            rows = self._rows(
                (
                    f"SELECT DISTINCT UPPER(TRIM(CAST({col_uf} AS VARCHAR))) AS uf "
                    "FROM analytics WHERE COALESCE(TRIM(CAST(SG_UF AS VARCHAR)), '') <> '' ORDER BY uf"
                )
            )
            ufs = [str(r[0]) for r in rows if r[0] is not None]

        cargos: list[str] = []
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        if col_cargo:
            rows = self._rows(
                (
                    f"SELECT DISTINCT TRIM(CAST({col_cargo} AS VARCHAR)) AS cargo "
                    "FROM analytics WHERE COALESCE(TRIM(CAST({} AS VARCHAR)), '') <> '' ORDER BY cargo"
                ).format(col_cargo)
            )
            cargos = [str(r[0]) for r in rows if r[0] is not None]

        years = sorted(set(anos).union(SUPPORTED_ANALYTICS_YEARS))
        return {"anos": years, "ufs": ufs, "cargos": cargos}

    def overview(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
    ) -> dict:
        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        total_registros = int(self._scalar(f"SELECT COUNT(*) FROM analytics {where}", params) or 0)

        total_candidatos = None
        if col_candidato:
            total_candidatos = int(
                self._scalar(f"SELECT COUNT(DISTINCT CAST({col_candidato} AS VARCHAR)) FROM analytics {where}", params)
                or 0
            )

        total_eleitos = None
        if col_situacao:
            total_eleitos = int(
                self._scalar(
                    (
                        "SELECT COUNT(*) FROM analytics "
                        f"{where} {'AND' if where else 'WHERE'} "
                        f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) LIKE 'ELEITO%'"
                    ),
                    params,
                )
                or 0
            )

        total_votos = None
        if col_votos:
            total_votos = int(
                self._scalar(
                    f"SELECT COALESCE(SUM(TRY_CAST({col_votos} AS BIGINT)), 0) FROM analytics {where}",
                    params,
                )
                or 0
            )

        return {
            "total_registros": total_registros,
            "total_candidatos": total_candidatos,
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
        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidato = self._pick_col(["NM_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        effective_page_size = min(page_size or top_n or self.default_top_n, self.max_top_n)
        if not col_candidato or not col_votos:
            return {
                "top_n": effective_page_size,
                "page": page,
                "page_size": effective_page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
            }

        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, partido=partido, municipio=municipio)
        offset = (page - 1) * effective_page_size

        key_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({col_candidate_key} AS VARCHAR)), ''), LOWER(TRIM(CAST({col_candidato} AS VARCHAR))))"
            if col_candidate_key
            else f"LOWER(TRIM(CAST({col_candidato} AS VARCHAR)))"
        )
        candidate_id_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({col_candidate_key} AS VARCHAR)), ''), LOWER(TRIM(CAST({col_candidato} AS VARCHAR))))"
            if col_candidate_key
            else f"LOWER(TRIM(CAST({col_candidato} AS VARCHAR)))"
        )
        candidato_expr = f"COALESCE(NULLIF(TRIM(CAST({col_candidato} AS VARCHAR)), ''), 'N/A')"
        partido_expr = f"{'CAST(' + col_partido + ' AS VARCHAR)' if col_partido else 'NULL'}"
        cargo_expr = f"{'CAST(' + col_cargo + ' AS VARCHAR)' if col_cargo else 'NULL'}"
        situacao_expr = f"{'CAST(' + col_situacao + ' AS VARCHAR)' if col_situacao else 'NULL'}"
        uf_norm_expr = f"{'NULLIF(UPPER(TRIM(CAST(' + col_uf + ' AS VARCHAR))), \'\')' if col_uf else 'NULL'}"
        grouped_sql = (
            "SELECT "
            "MIN(candidate_id) AS candidate_id, "
            "MIN(candidato) AS candidato, "
            "MIN(partido) AS partido, "
            "MIN(cargo) AS cargo, "
            "CASE WHEN COUNT(DISTINCT uf_norm) = 1 THEN MIN(uf_norm) ELSE NULL END AS uf, "
            "SUM(votos) AS votos, "
            "MIN(situacao) AS situacao "
            "FROM ("
            "  SELECT "
            f"  {key_expr} AS candidate_key, "
            f"  {candidate_id_expr} AS candidate_id, "
            f"  {candidato_expr} AS candidato, "
            f"  {partido_expr} AS partido, "
            f"  {cargo_expr} AS cargo, "
            f"  {uf_norm_expr} AS uf_norm, "
            f"  COALESCE(TRY_CAST({col_votos} AS BIGINT), 0) AS votos, "
            f"  {situacao_expr} AS situacao "
            f"  FROM analytics {where}"
            ") t "
            "WHERE candidate_key IS NOT NULL AND candidate_key <> '' "
            "GROUP BY candidate_key"
        )
        rows = self._rows(
            (
                "WITH grouped AS ("
                f"  {grouped_sql}"
                "), ranked AS ("
                "  SELECT "
                "  candidate_id, candidato, partido, cargo, uf, votos, situacao, "
                "  ROW_NUMBER() OVER (ORDER BY votos DESC, candidato ASC) AS rn, "
                "  COUNT(*) OVER () AS total_rows "
                "  FROM grouped"
                ") "
                "SELECT candidate_id, candidato, partido, cargo, uf, votos, situacao, total_rows "
                "FROM ranked "
                "WHERE rn > ? AND rn <= ? "
                "ORDER BY rn"
            ),
            params + [offset, offset + effective_page_size],
        )
        total = int(rows[0][7] or 0) if rows else 0
        if total == 0 and page > 1:
            total = int(self._scalar(f"SELECT COUNT(*) FROM ({grouped_sql}) g", params) or 0)
        total_pages = (total + effective_page_size - 1) // effective_page_size if total else 0

        return {
            "top_n": effective_page_size,
            "page": page,
            "page_size": effective_page_size,
            "total": total,
            "total_pages": total_pages,
            "items": [
                {
                    "candidate_id": r[0] or "",
                    "candidato": r[1] or "",
                    "partido": r[2],
                    "cargo": r[3],
                    "uf": r[4],
                    "votos": int(r[5] or 0),
                    "situacao": r[6],
                }
                for r in rows
            ],
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

        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        rows = self._rows(
            (
                "SELECT "
                f"COALESCE(NULLIF(TRIM(CAST({target_col} AS VARCHAR)), ''), 'N/A') AS label, "
                "COUNT(*) AS value "
                f"FROM analytics {where} GROUP BY 1 ORDER BY value DESC"
            ),
            params,
        )
        total = float(sum(int(r[1]) for r in rows) or 1)
        return [
            {
                "label": str(r[0]),
                "value": float(r[1]),
                "percentage": round((float(r[1]) / total) * 100, 2),
            }
            for r in rows
        ]

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

        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=False,
        )
        eleito_expr = (
            f"CASE WHEN UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) LIKE 'ELEITO%' THEN 1 ELSE 0 END"
            if col_situacao
            else "0"
        )
        categoria_expr = (
            "CASE "
            f"WHEN COALESCE(TRIM(CAST({col_cor_raca} AS VARCHAR)), '') = '' THEN 'NAO_INFORMADO' "
            f"WHEN UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) IN "
            "('N/A', 'NA', 'NULL', 'NULO', 'NAO INFORMADO', 'NÃO INFORMADO', "
            "'NAO DIVULGAVEL', 'NÃO DIVULGÁVEL', 'NÃO DIVULGAVEL', "
            "'NAO DECLARADO', 'NÃO DECLARADO', 'SEM INFORMACAO', 'SEM INFORMAÇÃO', 'IGNORADO') "
            "THEN 'NAO_INFORMADO' "
            f"WHEN UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) LIKE '%BRANCA%' THEN 'BRANCA' "
            f"WHEN UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) LIKE '%PRETA%' "
            f"  OR UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) LIKE '%NEGRA%' THEN 'PRETA' "
            f"WHEN UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) LIKE '%PARDA%' THEN 'PARDA' "
            f"WHEN UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) LIKE '%AMARELA%' THEN 'AMARELA' "
            f"WHEN UPPER(TRIM(CAST({col_cor_raca} AS VARCHAR))) LIKE '%INDIG%' THEN 'INDIGENA' "
            "ELSE 'NAO_INFORMADO' END"
        )
        rows = self._rows(
            (
                "SELECT "
                f"{categoria_expr} AS categoria, "
                "COUNT(*) AS candidatos, "
                f"SUM({eleito_expr}) AS eleitos "
                f"FROM analytics {where} "
                "GROUP BY 1"
            ),
            params,
        )
        by_category = {
            str(row[0]): {"candidatos": int(row[1] or 0), "eleitos": int(row[2] or 0)}
            for row in rows
        }
        total_candidatos = int(sum(v["candidatos"] for v in by_category.values()))
        total_eleitos = int(sum(v["eleitos"] for v in by_category.values()))

        items: list[dict] = []
        for key in COR_RACA_CATEGORY_ORDER:
            values = by_category.get(key, {"candidatos": 0, "eleitos": 0})
            candidatos = int(values["candidatos"])
            eleitos = int(values["eleitos"])
            items.append(
                {
                    "categoria": COR_RACA_CATEGORY_LABELS[key],
                    "candidatos": candidatos,
                    "eleitos": eleitos,
                    "percentual_candidatos": round((candidatos / total_candidatos) * 100, 2)
                    if total_candidatos
                    else 0.0,
                    "percentual_eleitos": round((eleitos / total_eleitos) * 100, 2) if total_eleitos else 0.0,
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

        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        rows = self._rows(
            (
                "SELECT "
                f"COALESCE(NULLIF(TRIM(CAST({col_ocupacao} AS VARCHAR)), ''), 'N/A') AS ocupacao, "
                "SUM(CASE "
                f"  WHEN UPPER(TRIM(CAST({col_genero} AS VARCHAR))) LIKE 'MASC%' "
                f"    OR UPPER(TRIM(CAST({col_genero} AS VARCHAR))) = 'M' "
                "  THEN 1 ELSE 0 END) AS masculino, "
                "SUM(CASE "
                f"  WHEN UPPER(TRIM(CAST({col_genero} AS VARCHAR))) LIKE 'FEM%' "
                f"    OR UPPER(TRIM(CAST({col_genero} AS VARCHAR))) = 'F' "
                "  THEN 1 ELSE 0 END) AS feminino "
                f"FROM analytics {where} "
                "GROUP BY 1 "
                "ORDER BY masculino DESC, feminino DESC, ocupacao ASC"
            ),
            params,
        )
        return [
            {"ocupacao": str(r[0]), "masculino": int(r[1] or 0), "feminino": int(r[2] or 0)}
            for r in rows
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
        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        col_idade = self._pick_col(["IDADE"])
        if not col_idade:
            return {"media": None, "mediana": None, "minimo": None, "maximo": None, "desvio_padrao": None, "bins": []}

        stats = self._rows(
            (
                "SELECT "
                f"AVG(TRY_CAST({col_idade} AS DOUBLE)), "
                f"MEDIAN(TRY_CAST({col_idade} AS DOUBLE)), "
                f"MIN(TRY_CAST({col_idade} AS DOUBLE)), "
                f"MAX(TRY_CAST({col_idade} AS DOUBLE)), "
                f"STDDEV_POP(TRY_CAST({col_idade} AS DOUBLE)) "
                f"FROM analytics {where}"
            ),
            params,
        )[0]
        if stats[0] is None:
            return {"media": None, "mediana": None, "minimo": None, "maximo": None, "desvio_padrao": None, "bins": []}

        age_not_null = f"TRY_CAST({col_idade} AS DOUBLE) IS NOT NULL"
        filtered_where = f"{where} AND {age_not_null}" if where else f"WHERE {age_not_null}"
        rows = self._rows(
            (
                "SELECT faixa, COUNT(*) as value FROM ("
                "  SELECT CASE "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 20 AND 29 THEN '20-29' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 30 AND 39 THEN '30-39' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 40 AND 49 THEN '40-49' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 50 AND 59 THEN '50-59' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 60 AND 69 THEN '60-69' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 70 AND 79 THEN '70-79' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) BETWEEN 80 AND 89 THEN '80-89' "
                "    ELSE NULL END AS faixa "
                f"  FROM analytics {filtered_where}"
                ") t WHERE faixa IS NOT NULL GROUP BY 1"
            ),
            params,
        )
        by_label = {str(r[0]): int(r[1]) for r in rows}
        labels = ["20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89"]
        total = float(sum(by_label.values()) or 1)
        bins = [
            {
                "label": label,
                "value": float(by_label.get(label, 0)),
                "percentage": round((float(by_label.get(label, 0)) / total) * 100, 2),
            }
            for label in labels
        ]

        return {
            "media": round(float(stats[0]), 2),
            "mediana": round(float(stats[1]), 2),
            "minimo": float(stats[2]),
            "maximo": float(stats[3]),
            "desvio_padrao": round(float(stats[4] or 0.0), 2),
            "bins": bins,
        }

    def _metric_sql(self, metric: str) -> str | None:
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if metric == "registros":
            return "COUNT(*)"
        if metric == "votos_nominais":
            if not col_votos:
                return None
            return f"COALESCE(SUM(TRY_CAST({col_votos} AS DOUBLE)), 0)"
        if metric == "candidatos":
            if not col_candidato:
                return None
            return f"COUNT(DISTINCT CAST({col_candidato} AS VARCHAR))"
        if metric == "eleitos":
            if not col_situacao:
                return None
            return (
                "SUM(CASE WHEN "
                f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) LIKE 'ELEITO%' "
                "THEN 1 ELSE 0 END)"
            )
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
        metric_sql = self._metric_sql(metric)
        if not col_ano or not metric_sql:
            return []

        where, params = self._where(
            ano=None,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        rows = self._rows(
            (
                "SELECT "
                f"TRY_CAST({col_ano} AS BIGINT) AS ano, "
                f"{metric_sql} AS value "
                f"FROM analytics {where} "
                f"{'AND' if where else 'WHERE'} TRY_CAST({col_ano} AS BIGINT) IS NOT NULL "
                "GROUP BY 1 ORDER BY 1"
            ),
            params,
        )
        return [{"ano": int(r[0]), "value": float(r[1] or 0)} for r in rows if r[0] is not None]

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
        target_opts = group_map.get(group_by)
        if not target_opts:
            return []
        target_col = self._pick_col(target_opts)
        metric_sql = self._metric_sql(metric)
        if not target_col or not metric_sql:
            return []

        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        n = min(top_n or self.default_top_n, self.max_top_n)
        rows = self._rows(
            (
                "SELECT "
                f"COALESCE(NULLIF(TRIM(CAST({target_col} AS VARCHAR)), ''), 'N/A') AS label, "
                f"{metric_sql} AS value "
                f"FROM analytics {where} "
                "GROUP BY 1 ORDER BY value DESC LIMIT ?"
            ),
            params + [n],
        )
        total = float(sum(float(r[1] or 0) for r in rows) or 1.0)
        return [
            {
                "label": str(r[0]),
                "value": float(r[1] or 0),
                "percentage": round((float(r[1] or 0) / total) * 100, 2),
            }
            for r in rows
        ]

    def uf_map(
        self,
        metric: str = "votos_nominais",
        ano: int | None = None,
        turno: int | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        target_col = self._pick_col(["SG_UF"])
        metric_sql = self._metric_sql(metric)
        if not target_col or not metric_sql:
            return []

        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=None,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=somente_eleitos,
        )
        rows = self._rows(
            (
                "SELECT "
                f"COALESCE(NULLIF(UPPER(TRIM(CAST({target_col} AS VARCHAR))), ''), 'N/A') AS uf, "
                f"{metric_sql} AS value "
                f"FROM analytics {where} "
                "GROUP BY 1 ORDER BY value DESC"
            ),
            params,
        )
        total = float(sum(float(r[1] or 0) for r in rows) or 1.0)
        return [
            {
                "uf": str(r[0]),
                "value": float(r[1] or 0),
                "percentage": round((float(r[1] or 0) / total) * 100, 2),
            }
            for r in rows
        ]

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

        mode = (map_mode or "").strip().lower()
        should_build_federal = mode in ("", "statebygovernor")
        should_build_municipal = mode in ("", "municipalitybymayor")
        target_uf = self._normalize_value(uf, uppercase=True) if uf else ""
        return_municipal_uf = mode == "" or (mode == "municipalitybymayor" and bool(target_uf))
        return_municipal_brasil = mode == "" or (mode == "municipalitybymayor" and not bool(target_uf))

        uf_expr = f"UPPER(TRIM(CAST({col_uf} AS VARCHAR)))"
        cargo_expr = f"UPPER(TRIM(CAST({col_cargo} AS VARCHAR)))"
        partido_expr = f"UPPER(TRIM(CAST({col_partido} AS VARCHAR)))"
        situacao_expr = f"TRIM(CAST({col_situacao} AS VARCHAR))"
        votos_expr = f"COALESCE(TRY_CAST({col_votos} AS BIGINT), 0)"
        ano_expr = f"TRY_CAST({col_ano} AS BIGINT)" if col_ano else "NULL"
        turno_expr = (
            "COALESCE("
            f"TRY_CAST({col_turno} AS BIGINT), "
            f"TRY_CAST(regexp_extract(CAST({col_turno} AS VARCHAR), '(\\d+)', 1) AS BIGINT), "
            "0)"
            if col_turno
            else "0"
        )
        municipio_expr = (
            f"NULLIF(TRIM(CAST({col_municipio} AS VARCHAR)), '')"
            if col_municipio
            else "NULL"
        )
        eleito_expr = f"CASE WHEN UPPER({situacao_expr}) LIKE 'ELEITO%' THEN 1 ELSE 0 END"

        base_clauses = [f"{uf_expr} <> ''", f"{partido_expr} <> ''"]
        base_params: list[object] = []
        if target_uf:
            base_clauses.append(f"{uf_expr} = ?")
            base_params.append(target_uf)

        def _winner_rows(
            cargo: str,
            partition_cols: list[str],
            ano_param: int | None,
            turno_param: int | None,
            require_municipio: bool = False,
        ) -> tuple[list[tuple], int | None]:
            filters = list(base_clauses)
            params = list(base_params)
            filters.append(f"{cargo_expr} = ?")
            params.append(cargo)
            if require_municipio:
                filters.append(f"{municipio_expr} IS NOT NULL")

            resolved_ano = ano_param
            if resolved_ano is None and col_ano:
                max_ano = self._scalar(
                    f"SELECT MAX({ano_expr}) FROM analytics WHERE {' AND '.join(filters)}",
                    params,
                )
                resolved_ano = int(max_ano) if max_ano is not None else None
            if resolved_ano is not None:
                filters.append(f"{ano_expr} = ?")
                params.append(int(resolved_ano))
            if turno_param is not None:
                filters.append(f"{turno_expr} = ?")
                params.append(int(turno_param))

            where_sql = " AND ".join(filters)
            partition_sql = ", ".join(partition_cols)
            rows = self._rows(
                (
                    "SELECT uf, municipio, partido, situacao, votos, ano, turno, eleito "
                    "FROM ("
                    "  SELECT "
                    f"  {uf_expr} AS uf, "
                    f"  {municipio_expr} AS municipio, "
                    f"  {partido_expr} AS partido, "
                    f"  {situacao_expr} AS situacao, "
                    f"  {votos_expr} AS votos, "
                    f"  {ano_expr} AS ano, "
                    f"  {turno_expr} AS turno, "
                    f"  {eleito_expr} AS eleito, "
                    "  ROW_NUMBER() OVER ("
                    f"    PARTITION BY {partition_sql} "
                    f"    ORDER BY {eleito_expr} DESC, {turno_expr} DESC, {votos_expr} DESC"
                    "  ) AS rn "
                    "  FROM analytics "
                    f"  WHERE {where_sql}"
                    ") t "
                    "WHERE rn = 1 "
                    "ORDER BY uf, municipio"
                ),
                params,
            )
            return rows, resolved_ano

        federal_items: list[dict] = []
        if should_build_federal:
            gov_rows, resolved_ano_gov = _winner_rows(
                cargo="GOVERNADOR",
                partition_cols=[uf_expr],
                ano_param=ano_governador,
                turno_param=turno_governador,
            )
            federal_items = [
                {
                    "uf": str(r[0]),
                    "partido": str(r[2]),
                    "espectro": self._party_spectrum(str(r[2])),
                    "votos": int(r[4] or 0),
                    "status": str(r[3]) if r[3] else None,
                    "eleito": bool(r[7]),
                    "ano": int(r[5]) if r[5] is not None else (int(resolved_ano_gov) if resolved_ano_gov is not None else None),
                    "turno": int(r[6]) if r[6] is not None else None,
                }
                for r in gov_rows
            ]

        municipal_uf_items: list[dict] = []
        municipal_brasil_items: list[dict] = []
        if should_build_municipal:
            pref_rows, resolved_ano_mun = _winner_rows(
                cargo="PREFEITO",
                partition_cols=[uf_expr, municipio_expr],
                ano_param=ano_municipal,
                turno_param=turno_municipal,
                require_municipio=True,
            )

            if return_municipal_uf:
                municipal_uf_items = [
                    {
                        "uf": str(r[0]),
                        "municipio": str(r[1]),
                        "partido": str(r[2]),
                        "espectro": self._party_spectrum(str(r[2])),
                        "votos": int(r[4] or 0),
                        "status": str(r[3]) if r[3] else None,
                        "eleito": bool(r[7]),
                        "ano": int(r[5]) if r[5] is not None else (int(resolved_ano_mun) if resolved_ano_mun is not None else None),
                        "turno": int(r[6]) if r[6] is not None else None,
                    }
                    for r in pref_rows
                ]

            if return_municipal_brasil and pref_rows:
                spectrum_count: dict[tuple[str, str], dict[str, int]] = {}
                party_count: dict[tuple[str, str], int] = {}
                total_prefeitos_por_uf: dict[str, int] = {}
                for row in pref_rows:
                    uf_key = str(row[0])
                    partido_key = str(row[2])
                    votos = int(row[4] or 0)
                    espectro = self._party_spectrum(partido_key)
                    spectrum_key = (uf_key, espectro)
                    entry = spectrum_count.get(spectrum_key, {"total_prefeitos": 0, "votos_total": 0})
                    entry["total_prefeitos"] += 1
                    entry["votos_total"] += votos
                    spectrum_count[spectrum_key] = entry
                    party_key = (uf_key, partido_key)
                    party_count[party_key] = party_count.get(party_key, 0) + 1
                    total_prefeitos_por_uf[uf_key] = total_prefeitos_por_uf.get(uf_key, 0) + 1
                official_prefeito_totals = self._official_prefeito_totals_by_uf(resolved_ano_mun)
                for uf_key, total_official in official_prefeito_totals.items():
                    total_prefeitos_por_uf[uf_key] = max(
                        int(total_official), int(total_prefeitos_por_uf.get(uf_key, 0))
                    )

                by_uf: dict[str, list[tuple[str, dict[str, int]]]] = {}
                for (uf_key, espectro), values in spectrum_count.items():
                    by_uf.setdefault(uf_key, []).append((espectro, values))

                for uf_key in sorted(by_uf.keys()):
                    best = sorted(
                        by_uf[uf_key],
                        key=lambda item: (-item[1]["total_prefeitos"], -item[1]["votos_total"], item[0]),
                    )[0]
                    party_options = sorted(
                        [(party, total) for (uf_party, party), total in party_count.items() if uf_party == uf_key],
                        key=lambda item: (-item[1], item[0]),
                    )
                    municipal_brasil_items.append(
                        {
                            "uf": uf_key,
                            "espectro": best[0],
                            "partido_representativo": party_options[0][0] if party_options else None,
                            "total_prefeitos": int(total_prefeitos_por_uf.get(uf_key, 0)),
                            "ano": int(resolved_ano_mun) if resolved_ano_mun is not None else None,
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
        scoped = self._filtered_df(ano=year, uf=state, cargo=office)
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

        col_name = "NM_CANDIDATO" if "NM_CANDIDATO" in candidate_rows.columns else ("NM_URNA_CANDIDATO" if "NM_URNA_CANDIDATO" in candidate_rows.columns else None)
        col_year = "ANO_ELEICAO" if "ANO_ELEICAO" in candidate_rows.columns else ("NR_ANO_ELEICAO" if "NR_ANO_ELEICAO" in candidate_rows.columns else None)
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in candidate_rows.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in candidate_rows.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in candidate_rows.columns else None))
        )
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
            latest_rows["SG_UF"].fillna("").astype(str).str.strip().str.upper().replace("", pd.NA).dropna().iloc[0]
            if "SG_UF" in latest_rows.columns and latest_rows["SG_UF"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
            else (state.upper() if state else None)
        )
        latest_office = (
            latest_rows["DS_CARGO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
            if "DS_CARGO" in latest_rows.columns and latest_rows["DS_CARGO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
            else (
                latest_rows["DS_CARGO_D"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
                if "DS_CARGO_D" in latest_rows.columns and latest_rows["DS_CARGO_D"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                else office
            )
        )

        vote_share = 0.0
        rank = None
        if col_votes and col_year and latest_year:
            context_df = self._filtered_df(ano=latest_year, uf=latest_state, cargo=latest_office)
            if not context_df.empty:
                context_df = context_df.assign(_votes=pd.to_numeric(context_df[col_votes], errors="coerce").fillna(0))
                total_votes = float(context_df["_votes"].sum())
                vote_share = round((latest_votes / total_votes) * 100, 4) if total_votes > 0 else 0.0
                name_col = "NM_CANDIDATO" if "NM_CANDIDATO" in context_df.columns else ("NM_URNA_CANDIDATO" if "NM_URNA_CANDIDATO" in context_df.columns else None)
                if name_col:
                    context_df = context_df.assign(
                        _candidate_key=(
                            context_df["SQ_CANDIDATO"].fillna("").astype(str).str.strip()
                            if "SQ_CANDIDATO" in context_df.columns
                            else (
                                context_df["NR_CANDIDATO"].fillna("").astype(str).str.strip()
                                if "NR_CANDIDATO" in context_df.columns
                                else context_df[name_col].fillna("").astype(str).str.strip().str.lower()
                            )
                        )
                    )
                    ranking = (
                        context_df.groupby("_candidate_key", as_index=False)
                        .agg(votes=("_votes", "sum"))
                        .sort_values("votes", ascending=False)
                        .reset_index(drop=True)
                    )
                    if "SQ_CANDIDATO" in latest_rows.columns and latest_rows["SQ_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any():
                        key = latest_rows["SQ_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
                    elif "NR_CANDIDATO" in latest_rows.columns and latest_rows["NR_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any():
                        key = latest_rows["NR_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
                    else:
                        key = latest_rows[name_col].fillna("").astype(str).str.strip().str.lower().iloc[0]
                    matches = ranking[ranking["_candidate_key"] == str(key)]
                    if not matches.empty:
                        rank = int(matches.index[0]) + 1

        mandates = 0
        if "DS_SIT_TOT_TURNO" in candidate_rows.columns and col_year:
            elected_by_year = (
                candidate_rows.assign(_eleito=self._is_elected_series(candidate_rows["DS_SIT_TOT_TURNO"]))
                .groupby("_year", as_index=False)
                .agg(elected=("_eleito", "max"))
            )
            mandates = int(elected_by_year["elected"].sum())

        latest_status = None
        if "DS_SIT_TOT_TURNO" in latest_rows.columns:
            statuses = latest_rows["DS_SIT_TOT_TURNO"].fillna("").astype(str).str.strip()
            statuses = statuses[statuses != ""]
            if not statuses.empty:
                latest_status = str(statuses.iloc[0])

        return {
            "candidate_id": self._candidate_output_id(candidate_rows, candidate_id),
            "name": (
                str(candidate_rows[col_name].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0])
                if col_name and candidate_rows[col_name].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                else ""
            ),
            "number": (
                str(candidate_rows["NR_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0])
                if "NR_CANDIDATO" in candidate_rows.columns and candidate_rows["NR_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                else None
            ),
            "party": (
                str(candidate_rows["SG_PARTIDO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0])
                if "SG_PARTIDO" in candidate_rows.columns and candidate_rows["SG_PARTIDO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                else None
            ),
            "coalition": (
                str(candidate_rows["NM_COLIGACAO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0])
                if "NM_COLIGACAO" in candidate_rows.columns and candidate_rows["NM_COLIGACAO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                else (
                    str(candidate_rows["DS_COMPOSICAO_COLIGACAO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0])
                    if "DS_COMPOSICAO_COLIGACAO" in candidate_rows.columns and candidate_rows["DS_COMPOSICAO_COLIGACAO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                    else None
                )
            ),
            "office": latest_office,
            "state": latest_state,
            "mandates": mandates,
            "status": latest_status,
            "latest_election": {"year": latest_year, "votes": latest_votes, "vote_share": vote_share, "state_rank": rank},
        }

    def candidate_vote_history(
        self,
        candidate_id: str,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        scoped = self._filtered_df(uf=state, cargo=office)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        col_year = "ANO_ELEICAO" if "ANO_ELEICAO" in scoped.columns else ("NR_ANO_ELEICAO" if "NR_ANO_ELEICAO" in scoped.columns else None)
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in scoped.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in scoped.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in scoped.columns else None))
        )
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
        if "DS_SIT_TOT_TURNO" in candidate_rows.columns:
            for year_val, year_df in candidate_rows.groupby("_year"):
                statuses = year_df["DS_SIT_TOT_TURNO"].fillna("").astype(str).str.strip()
                statuses = statuses[statuses != ""]
                if statuses.empty:
                    status_by_year[int(year_val)] = None
                elif self._is_elected_series(statuses).any():
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
        scoped = self._filtered_df(ano=year, uf=state, cargo=office, municipio=municipality)
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

        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in candidate_rows.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in candidate_rows.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in candidate_rows.columns else None))
        )
        weights = pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(1.0) if col_votes else 1.0
        total_weight = float(weights.sum()) if hasattr(weights, "sum") else float(len(candidate_rows))
        total_weight = total_weight if total_weight > 0 else 1.0

        male_share = 0.0
        female_share = 0.0
        if "DS_GENERO" in candidate_rows.columns:
            gender_norm = candidate_rows["DS_GENERO"].fillna("").astype(str).str.strip().apply(lambda value: self._normalize_value(value, uppercase=True))
            male_share = round((weights[gender_norm.str.contains("MASC", na=False)].sum() / total_weight) * 100, 4)
            female_share = round((weights[gender_norm.str.contains("FEM", na=False)].sum() / total_weight) * 100, 4)

        a18_34 = 0.0
        a35_59 = 0.0
        a60_plus = 0.0
        if "IDADE" in candidate_rows.columns:
            ages = pd.to_numeric(candidate_rows["IDADE"], errors="coerce")
            a18_34 = round((weights[(ages >= 18) & (ages <= 34)].sum() / total_weight) * 100, 4)
            a35_59 = round((weights[(ages >= 35) & (ages <= 59)].sum() / total_weight) * 100, 4)
            a60_plus = round((weights[ages >= 60].sum() / total_weight) * 100, 4)

        dominant_education = "N/A"
        edu_col = "DS_GRAU_INSTRUCAO" if "DS_GRAU_INSTRUCAO" in candidate_rows.columns else None
        if edu_col:
            edu = (
                candidate_rows.assign(_w=weights, _label=candidate_rows[edu_col].fillna("").astype(str).str.strip().replace("", "N/A"))
                .groupby("_label", as_index=False)
                .agg(weight=("_w", "sum"))
                .sort_values(["weight", "_label"], ascending=[False, True])
            )
            if not edu.empty:
                dominant_education = str(edu.iloc[0]["_label"])

        dominant_income = "N/A"
        income_col = "DS_FAIXA_RENDA" if "DS_FAIXA_RENDA" in candidate_rows.columns else ("DS_RENDA" if "DS_RENDA" in candidate_rows.columns else ("FAIXA_RENDA" if "FAIXA_RENDA" in candidate_rows.columns else None))
        if income_col:
            income = (
                candidate_rows.assign(_w=weights, _label=candidate_rows[income_col].fillna("").astype(str).str.strip().replace("", "N/A"))
                .groupby("_label", as_index=False)
                .agg(weight=("_w", "sum"))
                .sort_values(["weight", "_label"], ascending=[False, True])
            )
            if not income.empty:
                dominant_income = str(income.iloc[0]["_label"])

        urban_concentration = 0.0
        urban_col = "DS_LOCAL_VOTACAO" if "DS_LOCAL_VOTACAO" in candidate_rows.columns else ("DS_TIPO_LOCAL" if "DS_TIPO_LOCAL" in candidate_rows.columns else ("DS_CLASSIFICACAO_UE" if "DS_CLASSIFICACAO_UE" in candidate_rows.columns else None))
        if urban_col:
            urban_mask = candidate_rows[urban_col].fillna("").astype(str).str.strip().apply(
                lambda value: self._normalize_value(value, uppercase=True)
            ).str.contains("URBAN", na=False)
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
    ) -> dict:
        level_key = (level or "").strip().lower()
        if level_key not in {"macroregiao", "uf", "municipio", "zona"}:
            return {"candidate_id": str(candidate_id), "level": level_key, "items": []}

        scoped = self._filtered_df(ano=year, uf=state, cargo=office)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in candidate_rows.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in candidate_rows.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in candidate_rows.columns else None))
        )
        if candidate_rows.empty or not col_votes:
            return {"candidate_id": str(candidate_id), "level": level_key, "items": []}

        col_uf = "SG_UF" if "SG_UF" in candidate_rows.columns else None
        col_municipio = "NM_UE" if "NM_UE" in candidate_rows.columns else ("NM_MUNICIPIO" if "NM_MUNICIPIO" in candidate_rows.columns else None)
        col_zona = "NR_ZONA" if "NR_ZONA" in candidate_rows.columns else ("CD_ZONA" if "CD_ZONA" in candidate_rows.columns else ("SQ_ZONA" if "SQ_ZONA" in candidate_rows.columns else None))

        if level_key == "macroregiao":
            if not col_uf:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            grouped = candidate_rows.assign(
                key=candidate_rows[col_uf].fillna("").astype(str).str.strip().str.upper().map(UF_TO_MACROREGIAO).fillna("N/A"),
                label=candidate_rows[col_uf].fillna("").astype(str).str.strip().str.upper().map(UF_TO_MACROREGIAO).fillna("N/A"),
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            )
        elif level_key == "uf":
            if not col_uf:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            grouped = candidate_rows.assign(
                key=candidate_rows[col_uf].fillna("").astype(str).str.strip().str.upper().replace("", "N/A"),
                label=candidate_rows[col_uf].fillna("").astype(str).str.strip().str.upper().replace("", "N/A"),
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            )
        elif level_key == "municipio":
            if not col_municipio:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            grouped = candidate_rows.assign(
                key=candidate_rows[col_municipio].fillna("").astype(str).str.strip().replace("", "N/A"),
                label=candidate_rows[col_municipio].fillna("").astype(str).str.strip().replace("", "N/A"),
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            )
        else:
            if not col_zona:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}
            zone_label = (
                candidate_rows[col_municipio].fillna("").astype(str).str.strip().replace("", "N/A") + " - Zona " + candidate_rows[col_zona].fillna("").astype(str).str.strip().replace("", "N/A")
                if col_municipio
                else "Zona " + candidate_rows[col_zona].fillna("").astype(str).str.strip().replace("", "N/A")
            )
            grouped = candidate_rows.assign(
                key=candidate_rows[col_zona].fillna("").astype(str).str.strip().replace("", "N/A"),
                label=zone_label,
                _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            )

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
        return {"candidate_id": self._candidate_output_id(candidate_rows, candidate_id), "level": level_key, "items": items}

    def candidate_vote_map(
        self,
        candidate_id: str,
        level: str = "auto",
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        municipality: str | None = None,
    ) -> dict:
        requested_level = (level or "auto").strip().lower()
        if requested_level not in {"auto", "municipio", "zona"}:
            requested_level = "auto"

        col_votes = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_votes:
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
        col_zona = self._pick_col(["NR_ZONA", "CD_ZONA", "SQ_ZONA"])
        col_lat = self._pick_col(["LATITUDE", "LAT", "VL_LATITUDE"])
        col_lng = self._pick_col(["LONGITUDE", "LON", "LNG", "VL_LONGITUDE"])

        where, params = self._where(ano=year, uf=state, cargo=office, municipio=municipality)
        candidate_norm = str(candidate_id or "").strip()
        candidate_upper = candidate_norm.upper()
        candidate_clauses: list[str] = []
        candidate_params: list[str] = []
        if self._has("SQ_CANDIDATO"):
            candidate_clauses.append("TRIM(CAST(SQ_CANDIDATO AS VARCHAR)) = ?")
            candidate_params.append(candidate_norm)
        if self._has("NR_CANDIDATO"):
            candidate_clauses.append("TRIM(CAST(NR_CANDIDATO AS VARCHAR)) = ?")
            candidate_params.append(candidate_norm)
        if self._has("NM_CANDIDATO"):
            candidate_clauses.append("UPPER(TRIM(CAST(NM_CANDIDATO AS VARCHAR))) = ?")
            candidate_params.append(candidate_upper)
        if self._has("NM_URNA_CANDIDATO"):
            candidate_clauses.append("UPPER(TRIM(CAST(NM_URNA_CANDIDATO AS VARCHAR))) = ?")
            candidate_params.append(candidate_upper)
        if not candidate_clauses:
            return {
                "candidate_id": str(candidate_id),
                "level": "municipio" if requested_level == "auto" else requested_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }

        candidate_where = f"{where} {'AND' if where else 'WHERE'} ({' OR '.join(candidate_clauses)})"
        query_params = params + candidate_params
        votes_expr = f"COALESCE(TRY_CAST({col_votes} AS DOUBLE), 0)"

        resolved_level = requested_level
        if resolved_level == "auto":
            if office:
                resolved_level = "zona" if self._cargo_scope(office) == "municipio" else "municipio"
            elif col_cargo:
                municipal_cargos_sql = ", ".join(
                    "'" + cargo.replace("'", "''") + "'" for cargo in sorted(MUNICIPAL_CARGOS)
                )
                municipal_votes, total_votes_tmp = self._rows(
                    (
                        "SELECT "
                        f"COALESCE(SUM(CASE WHEN UPPER(TRIM(CAST({col_cargo} AS VARCHAR))) IN ({municipal_cargos_sql}) "
                        f"THEN {votes_expr} ELSE 0 END), 0) AS municipal_votes, "
                        f"COALESCE(SUM({votes_expr}), 0) AS total_votes "
                        f"FROM analytics {candidate_where}"
                    ),
                    query_params,
                )[0]
                municipal_votes = float(municipal_votes or 0.0)
                total_votes_tmp = float(total_votes_tmp or 0.0)
                if municipal_votes > 0 and municipal_votes >= (total_votes_tmp - municipal_votes):
                    resolved_level = "zona"
                else:
                    resolved_level = "municipio"
            else:
                resolved_level = "municipio"

        if resolved_level == "zona" and not col_zona:
            resolved_level = "municipio"
        if resolved_level == "municipio" and not col_municipio:
            return {
                "candidate_id": str(candidate_id),
                "level": resolved_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }

        uf_expr = f"NULLIF(UPPER(TRIM(CAST({col_uf} AS VARCHAR))), '')" if col_uf else "NULL"
        municipio_expr = f"NULLIF(TRIM(CAST({col_municipio} AS VARCHAR)), '')" if col_municipio else "NULL"
        zona_expr = f"NULLIF(TRIM(CAST({col_zona} AS VARCHAR)), '')" if col_zona else "NULL"
        lat_expr = f"TRY_CAST({col_lat} AS DOUBLE)" if col_lat else "NULL"
        lng_expr = f"TRY_CAST({col_lng} AS DOUBLE)" if col_lng else "NULL"

        if resolved_level == "municipio":
            grouped = self._df(
                (
                    "SELECT "
                    f"{municipio_expr} AS _key, "
                    f"{municipio_expr} AS _label, "
                    f"{uf_expr} AS _uf, "
                    f"{municipio_expr} AS _municipio, "
                    "NULL AS _zona, "
                    f"SUM({votes_expr}) AS votes, "
                    f"AVG({lat_expr}) AS lat, "
                    f"AVG({lng_expr}) AS lng "
                    f"FROM analytics {candidate_where} "
                    f"AND {municipio_expr} IS NOT NULL "
                    "GROUP BY 1, 2, 3, 4, 5 "
                    "ORDER BY votes DESC"
                ),
                query_params,
            )
        else:
            grouped = self._df(
                (
                    "SELECT "
                    f"COALESCE({uf_expr}, 'N/A') || '|' || COALESCE({municipio_expr}, 'N/A') || '|' || COALESCE({zona_expr}, 'N/A') AS _key, "
                    f"COALESCE({municipio_expr}, 'N/A') || ' - Zona ' || COALESCE({zona_expr}, 'N/A') AS _label, "
                    f"{uf_expr} AS _uf, "
                    f"{municipio_expr} AS _municipio, "
                    f"{zona_expr} AS _zona, "
                    f"SUM({votes_expr}) AS votes, "
                    f"AVG({lat_expr}) AS lat, "
                    f"AVG({lng_expr}) AS lng "
                    f"FROM analytics {candidate_where} "
                    f"AND {zona_expr} IS NOT NULL "
                    "GROUP BY 1, 2, 3, 4, 5 "
                    "ORDER BY votes DESC"
                ),
                query_params,
            )

            for idx, row in grouped.iterrows():
                if pd.notna(row["lat"]) and pd.notna(row["lng"]):
                    continue
                geometry = self._resolve_zone_geometry(
                    zone_id=str(row["_zona"]),
                    city=(str(row["_municipio"]) if pd.notna(row["_municipio"]) else None),
                    uf=(str(row["_uf"]) if pd.notna(row["_uf"]) else None),
                    lat=float("nan"),
                    lng=float("nan"),
                )
                lat, lng = self._geometry_centroid(geometry)
                if lat is not None and lng is not None:
                    grouped.at[idx, "lat"] = lat
                    grouped.at[idx, "lng"] = lng

        candidate_id_exprs = []
        if self._has("SQ_CANDIDATO"):
            candidate_id_exprs.append("NULLIF(TRIM(CAST(SQ_CANDIDATO AS VARCHAR)), '')")
        if self._has("NR_CANDIDATO"):
            candidate_id_exprs.append("NULLIF(TRIM(CAST(NR_CANDIDATO AS VARCHAR)), '')")
        if self._has("NM_CANDIDATO"):
            candidate_id_exprs.append("NULLIF(TRIM(CAST(NM_CANDIDATO AS VARCHAR)), '')")
        if self._has("NM_URNA_CANDIDATO"):
            candidate_id_exprs.append("NULLIF(TRIM(CAST(NM_URNA_CANDIDATO AS VARCHAR)), '')")
        candidate_select_expr = "COALESCE(" + ", ".join(candidate_id_exprs + ["?"]) + ")"
        candidate_id_row = self._rows(
            f"SELECT {candidate_select_expr} AS candidate_id FROM analytics {candidate_where} LIMIT 1",
            query_params + [str(candidate_id)],
        )
        resolved_candidate_id = str(candidate_id_row[0][0]) if candidate_id_row else str(candidate_id)

        if grouped.empty:
            return {
                "candidate_id": resolved_candidate_id,
                "level": resolved_level,
                "quantile_q1": 0,
                "quantile_q2": 0,
                "total_votes": 0,
                "items": [],
            }

        votes = grouped["votes"].astype(float)
        q1 = int(votes.quantile(0.33))
        q2 = int(votes.quantile(0.66))
        if q2 < q1:
            q2 = q1

        tiers = {
            0: {"rgb": [34, 197, 94], "marker_size": 32, "cluster_size": 40},
            1: {"rgb": [250, 204, 21], "marker_size": 34, "cluster_size": 52},
            2: {"rgb": [239, 68, 68], "marker_size": 42, "cluster_size": 60},
        }

        total_votes = int(votes.sum())
        total_votes_safe = float(total_votes) if total_votes > 0 else 1.0
        items: list[dict] = []
        for _, row in grouped.sort_values("votes", ascending=False).iterrows():
            row_votes = int(row["votes"])
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
                    "tooltip": f"{label} — {tooltip_votes} votos",
                }
            )

        return {
            "candidate_id": resolved_candidate_id,
            "level": resolved_level,
            "quantile_q1": q1,
            "quantile_q2": q2,
            "total_votes": total_votes,
            "items": items,
        }

    def candidate_zone_fidelity(
        self,
        candidate_id: str,
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        include_geometry: bool = False,
    ) -> dict:
        scoped_all_years = self._filtered_df(uf=state, cargo=office)
        candidate_all_years = scoped_all_years[self._candidate_mask(scoped_all_years, candidate_id)].copy()
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in candidate_all_years.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in candidate_all_years.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in candidate_all_years.columns else None))
        )
        col_zona = "NR_ZONA" if "NR_ZONA" in candidate_all_years.columns else ("CD_ZONA" if "CD_ZONA" in candidate_all_years.columns else ("SQ_ZONA" if "SQ_ZONA" in candidate_all_years.columns else None))
        col_year = "ANO_ELEICAO" if "ANO_ELEICAO" in candidate_all_years.columns else ("NR_ANO_ELEICAO" if "NR_ANO_ELEICAO" in candidate_all_years.columns else None)
        if candidate_all_years.empty or not col_votes or not col_zona:
            return {"candidate_id": str(candidate_id), "items": []}

        col_zone_name = "NM_ZONA" if "NM_ZONA" in candidate_all_years.columns else ("DS_ZONA" if "DS_ZONA" in candidate_all_years.columns else None)
        col_city = "NM_UE" if "NM_UE" in candidate_all_years.columns else ("NM_MUNICIPIO" if "NM_MUNICIPIO" in candidate_all_years.columns else None)
        col_uf = "SG_UF" if "SG_UF" in candidate_all_years.columns else None
        col_lat = "LATITUDE" if "LATITUDE" in candidate_all_years.columns else ("LAT" if "LAT" in candidate_all_years.columns else ("VL_LATITUDE" if "VL_LATITUDE" in candidate_all_years.columns else None))
        col_lng = "LONGITUDE" if "LONGITUDE" in candidate_all_years.columns else ("LON" if "LON" in candidate_all_years.columns else ("LNG" if "LNG" in candidate_all_years.columns else ("VL_LONGITUDE" if "VL_LONGITUDE" in candidate_all_years.columns else None)))

        resolved_year = year
        if col_year:
            years = pd.to_numeric(candidate_all_years[col_year], errors="coerce").dropna().astype(int)
            if resolved_year is None and not years.empty:
                resolved_year = int(years.max())
            if resolved_year is not None:
                candidate_current = candidate_all_years[pd.to_numeric(candidate_all_years[col_year], errors="coerce") == int(resolved_year)].copy()
                prev_years = years[years < int(resolved_year)] if not years.empty else pd.Series(dtype=int)
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
            _zone_id=candidate_current[col_zona].fillna("").astype(str).str.strip().replace("", "N/A"),
            _zone_name=(
                candidate_current[col_zone_name].fillna("").astype(str).str.strip().replace("", pd.NA)
                if col_zone_name
                else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)
            ),
            _city=(
                candidate_current[col_city].fillna("").astype(str).str.strip().replace("", pd.NA)
                if col_city
                else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)
            ),
            _uf=(
                candidate_current[col_uf].fillna("").astype(str).str.strip().str.upper().replace("", pd.NA)
                if col_uf
                else pd.Series([pd.NA] * len(candidate_current), index=candidate_current.index)
            ),
            _votes=pd.to_numeric(candidate_current[col_votes], errors="coerce").fillna(0),
            _lat=(pd.to_numeric(candidate_current[col_lat], errors="coerce") if col_lat else pd.Series([0.0] * len(candidate_current), index=candidate_current.index)),
            _lng=(pd.to_numeric(candidate_current[col_lng], errors="coerce") if col_lng else pd.Series([0.0] * len(candidate_current), index=candidate_current.index)),
        )

        previous_votes_by_zone: dict[str, float] = {}
        if not candidate_previous.empty:
            prev_tmp = candidate_previous.assign(
                _zone_id=candidate_previous[col_zona].fillna("").astype(str).str.strip().replace("", "N/A"),
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
            .agg(votes=("_votes", "sum"), zone_name=("_zone_name", "first"), city=("_city", "first"), uf=("_uf", "first"), lat=("_lat", "mean"), lng=("_lng", "mean"))
            .sort_values("votes", ascending=False)
        )
        items = []
        for _, row in grouped.iterrows():
            zone_name = str(row["zone_name"]) if pd.notna(row["zone_name"]) and str(row["zone_name"]).strip() else f"Zona {row['_zone_id']}"
            prev_votes = float(previous_votes_by_zone.get(str(row["_zone_id"]), 0.0))
            if prev_votes > 0:
                retention = min(round((float(row["votes"]) / prev_votes) * 100, 4), 100.0)
            else:
                retention = 100.0
            item = {
                "zone_id": str(row["_zone_id"]),
                "zone_name": zone_name,
                "city": str(row["city"]) if pd.notna(row["city"]) else None,
                "votes": int(row["votes"]),
                "retention": retention,
                "lat": float(row["lat"]) if pd.notna(row["lat"]) else 0.0,
                "lng": float(row["lng"]) if pd.notna(row["lng"]) else 0.0,
            }
            if include_geometry:
                item["geometry"] = self._resolve_zone_geometry(
                    zone_id=str(row["_zone_id"]),
                    city=(str(row["city"]) if pd.notna(row["city"]) else None),
                    uf=(str(row["uf"]) if pd.notna(row["uf"]) else None),
                    lat=float(row["lat"]) if pd.notna(row["lat"]) else 0.0,
                    lng=float(row["lng"]) if pd.notna(row["lng"]) else 0.0,
                )
            items.append(item)
        return {"candidate_id": self._candidate_output_id(candidate_current, candidate_id), "items": items}

    def candidates_compare(
        self,
        candidate_ids: list[str],
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        candidate_ids_clean = [str(value).strip() for value in candidate_ids if str(value).strip()]
        if len(candidate_ids_clean) < 2:
            return {"context": {"year": year, "state": state, "office": office}, "candidates": [], "deltas": []}

        base_scoped = self._filtered_df(uf=state, cargo=office)
        col_year = "ANO_ELEICAO" if "ANO_ELEICAO" in base_scoped.columns else ("NR_ANO_ELEICAO" if "NR_ANO_ELEICAO" in base_scoped.columns else None)
        resolved_year = year
        if resolved_year is None and col_year and not base_scoped.empty:
            years = pd.to_numeric(base_scoped[col_year], errors="coerce").dropna()
            if not years.empty:
                resolved_year = int(years.max())
        resolved_year = int(resolved_year) if resolved_year is not None else None

        context_df = self._filtered_df(ano=resolved_year, uf=state, cargo=office) if resolved_year is not None else base_scoped.copy()
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in context_df.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in context_df.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in context_df.columns else None))
        )
        name_col = "NM_CANDIDATO" if "NM_CANDIDATO" in context_df.columns else ("NM_URNA_CANDIDATO" if "NM_URNA_CANDIDATO" in context_df.columns else None)
        if context_df.empty or not col_votes or not name_col:
            return {"context": {"year": resolved_year, "state": state, "office": office}, "candidates": [], "deltas": []}

        context_df = context_df.assign(_votes=pd.to_numeric(context_df[col_votes], errors="coerce").fillna(0))
        total_context_votes = float(context_df["_votes"].sum()) or 1.0
        context_df = context_df.assign(
            _candidate_key=(
                context_df["SQ_CANDIDATO"].fillna("").astype(str).str.strip()
                if "SQ_CANDIDATO" in context_df.columns
                else (
                    context_df["NR_CANDIDATO"].fillna("").astype(str).str.strip()
                    if "NR_CANDIDATO" in context_df.columns
                    else context_df[name_col].fillna("").astype(str).str.strip().str.lower()
                )
            )
        )
        ranking = (
            context_df.groupby("_candidate_key", as_index=False)
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
            if "SQ_CANDIDATO" in candidate_rows.columns and candidate_rows["SQ_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any():
                key = candidate_rows["SQ_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
            elif "NR_CANDIDATO" in candidate_rows.columns and candidate_rows["NR_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any():
                key = candidate_rows["NR_CANDIDATO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
            else:
                key = candidate_rows[name_col].fillna("").astype(str).str.strip().str.lower().iloc[0]
            history = self.candidate_vote_history(candidate_id=cid, state=state, office=office).get("items", [])
            history_sorted = sorted(history, key=lambda item: int(item["year"]))
            retention = 100.0
            if history_sorted:
                current = next((h for h in history_sorted if int(h["year"]) == int(resolved_year or h["year"])), history_sorted[-1])
                previous_items = [h for h in history_sorted if int(h["year"]) < int(current["year"])]
                if previous_items:
                    prev_votes = float(previous_items[-1]["votes"])
                    retention = round((float(current["votes"]) / prev_votes) * 100, 4) if prev_votes > 0 else 100.0

            candidates_out.append(
                {
                    "candidate_id": self._candidate_output_id(candidate_rows, cid),
                    "name": str(candidate_rows[name_col].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]),
                    "party": (
                        str(candidate_rows["SG_PARTIDO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0])
                        if "SG_PARTIDO" in candidate_rows.columns and candidate_rows["SG_PARTIDO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                        else None
                    ),
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
        col_candidato = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if not col_candidato:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        query = str(q or "").strip()
        if not query:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}
        q_norm = self._normalize_value(query).lower()
        if not q_norm:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_candidate_id = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_numero = self._pick_col(["NR_CANDIDATO"])
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, partido=partido)
        normalized_name_expr = (
            f"LOWER(TRIM(TRANSLATE(CAST({col_candidato} AS VARCHAR), "
            "'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ', "
            "'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn')))"
        )
        pattern = f"%{q_norm}%"
        filter_sql = f"{where} {'AND' if where else 'WHERE'} {normalized_name_expr} LIKE ?"
        base_params = params + [pattern]

        total = int(
            self._scalar(
                (
                    "SELECT COUNT(*) "
                    "FROM ("
                    "  SELECT "
                    f"    COALESCE({'NULLIF(TRIM(CAST(' + col_candidate_id + ' AS VARCHAR)), \'\'),' if col_candidate_id else ''}"
                    f"{normalized_name_expr}) AS candidate_id "
                    f"  FROM analytics {filter_sql} "
                    "  GROUP BY 1"
                    ") t"
                ),
                base_params,
            )
            or 0
        )
        if total == 0:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        offset = (page - 1) * page_size
        rows = self._rows(
            (
                "SELECT "
                "  candidate_id, "
                "  candidato, "
                "  partido, "
                "  cargo, "
                "  uf, "
                "  numero, "
                "  situacao, "
                "  votos, "
                "  score "
                "FROM ("
                "  SELECT "
                f"    COALESCE({'NULLIF(TRIM(CAST(' + col_candidate_id + ' AS VARCHAR)), \'\'),' if col_candidate_id else ''}"
                f"{normalized_name_expr}) AS candidate_id, "
                f"    MIN(TRIM(CAST({col_candidato} AS VARCHAR))) AS candidato, "
                f"    {'MIN(NULLIF(TRIM(CAST(' + col_partido + ' AS VARCHAR)), \'\'))' if col_partido else 'NULL'} AS partido, "
                f"    {'MIN(NULLIF(TRIM(CAST(' + col_cargo + ' AS VARCHAR)), \'\'))' if col_cargo else 'NULL'} AS cargo, "
                f"    {'MIN(NULLIF(UPPER(TRIM(CAST(' + col_uf + ' AS VARCHAR))), \'\'))' if col_uf else 'NULL'} AS uf, "
                f"    {'MIN(NULLIF(TRIM(CAST(' + col_numero + ' AS VARCHAR)), \'\'))' if col_numero else 'NULL'} AS numero, "
                f"    {'MIN(NULLIF(TRIM(CAST(' + col_situacao + ' AS VARCHAR)), \'\'))' if col_situacao else 'NULL'} AS situacao, "
                f"    {'SUM(COALESCE(TRY_CAST(' + col_votos + ' AS BIGINT), 0))' if col_votos else 'NULL'} AS votos, "
                f"    MAX(CASE WHEN {normalized_name_expr} = ? THEN 3 "
                f"             WHEN {normalized_name_expr} LIKE ? THEN 2 "
                "             ELSE 1 END) AS score "
                f"  FROM analytics {filter_sql} "
                "  GROUP BY 1"
                ") ranked "
                "ORDER BY score DESC, COALESCE(votos, 0) DESC, candidato ASC LIMIT ? OFFSET ?"
            ),
            [q_norm, f"{q_norm}%"] + base_params + [page_size, offset],
        )

        total_pages = (total + page_size - 1) // page_size
        items = [
            {
                "candidate_id": str(r[0] or ""),
                "candidato": r[1] or "",
                "partido": r[2],
                "cargo": r[3],
                "uf": r[4],
                "numero": r[5],
                "situacao": r[6],
                "votos": (int(r[7]) if r[7] is not None else None),
            }
            for r in rows
            if str(r[0] or "").strip()
        ]

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

        where, params = self._where(
            ano=ano,
            turno=turno,
            uf=uf,
            cargo=cargo,
            municipio=municipio,
            somente_eleitos=False,
        )

        if group_by == "municipio":
            placeholders = ", ".join(["?"] * len(MUNICIPAL_CARGOS))
            where = f"{where} {'AND' if where else 'WHERE'} UPPER(TRIM(CAST({col_cargo} AS VARCHAR))) IN ({placeholders})"
            params = params + sorted(MUNICIPAL_CARGOS)

        group_col_map = {"cargo": col_cargo, "uf": col_uf, "municipio": col_municipio}
        group_col = group_col_map[group_by]
        if group_by == "uf":
            group_expr = f"COALESCE(NULLIF(UPPER(TRIM(CAST({group_col} AS VARCHAR))), ''), 'N/A')"
        else:
            group_expr = f"COALESCE(NULLIF(TRIM(CAST({group_col} AS VARCHAR)), ''), 'N/A')"

        if col_qt_vagas:
            def norm_expr(col: str | None, uppercase: bool = False) -> str:
                if not col:
                    return "'N/A'"
                base = f"TRIM(CAST({col} AS VARCHAR))"
                if uppercase:
                    base = f"UPPER({base})"
                return f"COALESCE(NULLIF({base}, ''), 'N/A')"

            select_cols = [
                f"{f'TRY_CAST({col_ano} AS BIGINT)' if col_ano else 'NULL'} AS ano",
                f"{norm_expr(col_uf, uppercase=True)} AS uf",
                f"{norm_expr(col_cargo)} AS cargo",
                f"{norm_expr(col_municipio)} AS municipio",
                f"{group_expr} AS group_value",
                f"COALESCE(TRY_CAST({col_qt_vagas} AS DOUBLE), 0) AS qt_vagas",
            ]
            raw_rows = self._rows(f"SELECT {', '.join(select_cols)} FROM analytics {where}", params)

            unit_map: dict[str, dict] = {}
            for row in raw_rows:
                row_ano, row_uf, row_cargo, row_municipio, group_value, qt_vagas = row
                cargo_up = str(row_cargo or "N/A").upper()
                if cargo_up in MUNICIPAL_CARGOS:
                    scope = "municipio"
                elif cargo_up in NATIONAL_CARGOS:
                    scope = "nacional"
                else:
                    scope = "uf"
                if group_by == "municipio" and scope != "municipio":
                    continue
                if scope == "nacional":
                    unit_key = f"{row_ano}|{row_cargo}"
                elif scope == "uf":
                    unit_key = f"{row_ano}|{row_uf}|{row_cargo}"
                else:
                    unit_key = f"{row_ano}|{row_uf}|{row_cargo}|{row_municipio}"
                current = unit_map.get(unit_key)
                if current is None or float(qt_vagas or 0) > current["qt_vagas"]:
                    unit_map[unit_key] = {
                        "group_value": str(group_value or "N/A"),
                        "qt_vagas": float(qt_vagas or 0),
                    }

            agg: dict[str, float] = {}
            for item in unit_map.values():
                key = item["group_value"]
                agg[key] = agg.get(key, 0.0) + float(item["qt_vagas"])
            rows = sorted(agg.items(), key=lambda x: x[1], reverse=True)
        else:
            dedup_cols = [group_col, col_candidato]
            if col_ano:
                dedup_cols.append(col_ano)
            if col_cargo:
                dedup_cols.append(col_cargo)
            if group_by == "municipio" and col_municipio:
                dedup_cols.append(col_municipio)
            dedup_expr = ", ".join(dedup_cols)

            rows = self._rows(
                (
                    "SELECT group_value, COUNT(*) AS vagas_oficiais FROM ("
                    f"  SELECT DISTINCT {dedup_expr}, {group_expr} AS group_value "
                    f"  FROM analytics {where} "
                    f"{'AND' if where else 'WHERE'} UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) LIKE 'ELEITO%'"
                    ") t GROUP BY 1 ORDER BY vagas_oficiais DESC"
                ),
                params,
            )

        items: list[dict] = []
        for group_value, vagas in rows:
            value = str(group_value) if group_value is not None else "N/A"
            items.append(
                {
                    "ano": int(ano) if ano is not None else None,
                    "uf": (value if group_by == "uf" else (uf.upper().strip() if uf else None)),
                    "cargo": (value if group_by == "cargo" else (cargo.strip() if cargo else None)),
                    "municipio": (
                        value if group_by == "municipio" else (municipio.strip() if municipio else None)
                    ),
                    "vagas_oficiais": int(round(float(vagas or 0))),
                }
            )

        return {
            "group_by": group_by,
            "total_vagas_oficiais": int(sum(item["vagas_oficiais"] for item in items)),
            "items": items,
        }
