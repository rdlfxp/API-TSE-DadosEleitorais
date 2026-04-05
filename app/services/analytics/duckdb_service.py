from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import re
from pathlib import Path
from threading import Lock

import duckdb
import pandas as pd

from app.services.candidate_history_mixin import CandidateHistoryMixin

from app.services.analytics.common import (
    COR_RACA_CATEGORY_LABELS,
    COR_RACA_CATEGORY_ORDER,
    MUNICIPAL_CARGOS,
    NATIONAL_CARGOS,
    UF_TO_MACROREGIAO,
    VALID_UF_CODES,
    _impact_category,
    _safe_percent,
    logger,
)
from app.services.analytics.mixins import AnalyticsSupportMixin

@dataclass
class DuckDBAnalyticsService(AnalyticsSupportMixin, CandidateHistoryMixin):
    conn: duckdb.DuckDBPyConnection
    default_top_n: int
    max_top_n: int
    _query_lock: Lock
    _columns: set[str]
    _source_path: Path | None = field(default=None, init=False, repr=False)
    _database_path: str | None = field(default=None, init=False, repr=False)
    _official_prefeito_totals_cache: dict[int, dict[str, int]] = field(default_factory=dict, init=False, repr=False)
    _zone_geometry_index_cache: dict[str, object] | None = field(default=None, init=False, repr=False)
    _municipality_coord_index_cache: dict[str, tuple[float, float]] | None = field(default=None, init=False, repr=False)

    @staticmethod
    def _resolve_database_path(database_path: str | None) -> str:
        db_path = str(database_path or ":memory:").strip() or ":memory:"
        if db_path == ":memory:":
            return db_path

        base_path = Path(db_path)
        suffix = base_path.suffix
        stem = base_path.stem if suffix else base_path.name
        scoped_name = f"{stem}.pid{os.getpid()}{suffix}" if suffix else f"{stem}.pid{os.getpid()}"
        scoped_path = base_path.with_name(scoped_name)
        scoped_path.parent.mkdir(parents=True, exist_ok=True)
        return str(scoped_path)

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
        database_path: str | None = None,
    ) -> "DuckDBAnalyticsService":
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)

        db_path = cls._resolve_database_path(database_path)
        conn = duckdb.connect(database=db_path)
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
        service._database_path = db_path
        if create_indexes and materialize_table:
            service._create_indexes()
        return service

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

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
        col_turno = self._pick_col(["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_municipio = self._pick_col(["NM_UE", "NM_MUNICIPIO"])
        col_zona = self._pick_col(["NR_ZONA", "CD_ZONA", "SQ_ZONA"])
        col_sq_candidato = self._pick_col(["SQ_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])

        if col_ano and col_uf and col_cargo and col_votos:
            self._create_index("idx_analytics_ano_uf_cargo_votos", [col_ano, col_uf, col_cargo, col_votos])
        if col_ano and col_uf and col_cargo:
            self._create_index("idx_analytics_ano_uf_cargo", [col_ano, col_uf, col_cargo])
        if col_ano and col_cargo:
            self._create_index("idx_analytics_ano_cargo", [col_ano, col_cargo])
        if col_ano and col_turno and col_uf and col_cargo and col_municipio:
            self._create_index("idx_analytics_ano_turno_uf_cargo_municipio", [col_ano, col_turno, col_uf, col_cargo, col_municipio])
        if col_ano and col_turno and col_uf and col_cargo and col_municipio and col_zona:
            self._create_index(
                "idx_analytics_ano_turno_uf_cargo_municipio_zona",
                [col_ano, col_turno, col_uf, col_cargo, col_municipio, col_zona],
            )
        if col_sq_candidato and col_ano and col_turno and col_uf and col_cargo:
            self._create_index("idx_analytics_sq_candidato_ano_turno_uf_cargo", [col_sq_candidato, col_ano, col_turno, col_uf, col_cargo])
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

    def _normalized_sql_text_expr(self, expr: str) -> str:
        return (
            "regexp_replace("
            "translate("
            "UPPER("
            "regexp_replace("
            f"TRIM(CAST({expr} AS VARCHAR)), "
            "'\\s+', ' '"
            ")"
            "), "
            "'ÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑÝŸ', "
            "'AAAAAAEEEEIIIIOOOOOUUUUCNYY'"
            "), "
            "'[^\\x00-\\x7F]', ''"
            ")"
        )

    def _nullable_text_sql_expr(self, expr: str | None, *, uppercase: bool = False) -> str:
        if not expr:
            return "CAST(NULL AS VARCHAR)"
        base = f"TRIM(CAST({expr} AS VARCHAR))"
        if uppercase:
            base = f"UPPER({base})"
        return f"(CASE WHEN {base} = '' THEN CAST(NULL AS VARCHAR) ELSE {base} END)"

    def _candidate_group_key_sql_expr(self) -> str | None:
        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidate_name = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        base_candidate = self._nullable_text_sql_expr(col_candidate_key) if col_candidate_key else self._nullable_text_sql_expr(col_candidate_name)
        if base_candidate == "CAST(NULL AS VARCHAR)":
            return None

        parts = [base_candidate]
        for col in ("ANO_ELEICAO", "NR_ANO_ELEICAO", "NR_TURNO", "CD_TURNO", "DS_TURNO", "DS_CARGO", "DS_CARGO_D"):
            resolved = self._pick_col([col])
            if resolved:
                parts.append(self._nullable_text_sql_expr(resolved))
        return f"NULLIF(concat_ws('|', {', '.join(parts)}), '')"

    def _distribution_label_sql_expr(self, group_by: str, target_col: str) -> str:
        trimmed = f"regexp_replace(TRIM(CAST({target_col} AS VARCHAR)), '\\s+', ' ')"
        normalized = self._normalized_sql_text_expr(target_col)
        normalized_or_empty = f"COALESCE({normalized}, '')"
        na_values = "', '".join(sorted(self._NA_LABELS))
        if group_by == "genero":
            return (
                "CASE "
                f"WHEN {normalized_or_empty} = '' OR {normalized_or_empty} IN ('{na_values}') THEN 'N/A' "
                f"WHEN {normalized_or_empty} IN ('M', 'MASC', 'MASCULINO', 'MASCULINA') OR {normalized_or_empty} LIKE 'MASC%' THEN 'MASCULINO' "
                f"WHEN {normalized_or_empty} IN ('F', 'FEM', 'FEMININO', 'FEMININA') OR {normalized_or_empty} LIKE 'FEM%' THEN 'FEMININO' "
                "ELSE 'N/A' END"
            )
        if group_by == "cor_raca":
            return (
                "CASE "
                f"WHEN {normalized_or_empty} = '' OR {normalized_or_empty} IN ('{na_values}') THEN 'N/A' "
                f"WHEN {normalized_or_empty} LIKE '%BRANCA%' THEN 'BRANCA' "
                f"WHEN {normalized_or_empty} LIKE '%PRETA%' OR {normalized_or_empty} LIKE '%NEGRA%' THEN 'PRETA' "
                f"WHEN {normalized_or_empty} LIKE '%PARDA%' THEN 'PARDA' "
                f"WHEN {normalized_or_empty} LIKE '%AMARELA%' THEN 'AMARELA' "
                f"WHEN {normalized_or_empty} LIKE '%INDIG%' THEN 'INDIGENA' "
                "ELSE 'N/A' END"
            )
        return f"CASE WHEN {normalized_or_empty} = '' OR {normalized_or_empty} IN ('{na_values}') THEN 'N/A' ELSE {trimmed} END"

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
        normalized_uf = self._normalize_uf_filter(uf)

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
        if normalized_uf and col_uf:
            clauses.append(f"UPPER(TRIM(CAST({col_uf} AS VARCHAR))) = ?")
            params.append(normalized_uf)
        if cargo and col_cargo:
            clauses.append(f"LOWER(TRIM(CAST({col_cargo} AS VARCHAR))) = ?")
            params.append(cargo.lower())
        if partido and col_partido:
            clauses.append(f"UPPER(TRIM(CAST({col_partido} AS VARCHAR))) = ?")
            params.append(partido.upper().strip())
        if municipio and col_municipio:
            clauses.append(f"{self._normalized_sql_text_expr(col_municipio)} = ?")
            params.append(self._normalize_municipio_filter(municipio))
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
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        select_clause = "*"
        if columns:
            requested = [str(col).strip() for col in columns if str(col).strip() and self._has(str(col).strip())]
            if requested:
                select_clause = ", ".join(dict.fromkeys(requested))
        return self._df(f"SELECT {select_clause} FROM analytics {where}", params)

    def _candidate_clauses_and_params(self, candidate_id: str) -> tuple[list[str], list[str]]:
        candidate_norm = str(candidate_id or "").strip()
        candidate_upper = self._normalize_value(candidate_norm, uppercase=True)
        clauses: list[str] = []
        params: list[str] = []
        if self._has("SQ_CANDIDATO"):
            clauses.append("TRIM(CAST(SQ_CANDIDATO AS VARCHAR)) = ?")
            params.append(candidate_norm)
        if self._has("NR_CANDIDATO"):
            clauses.append("TRIM(CAST(NR_CANDIDATO AS VARCHAR)) = ?")
            params.append(candidate_norm)
        col_nome = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if col_nome:
            clauses.append(f"{self._normalized_sql_text_expr(col_nome)} = ?")
            params.append(candidate_upper)
        return clauses, params

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

        where, where_params = self._where(
            ano=year,
            turno=round_filter,
            cargo=office,
            uf=requested_uf,
            municipio=requested_municipio,
        )
        candidate_clauses, candidate_params = self._candidate_clauses_and_params(candidate_id)
        if not candidate_clauses:
            raise ValueError("Nao foi possivel inferir municipio para candidate_id no recorte informado.")

        uf_expr = self._nullable_text_sql_expr(col_uf, uppercase=True)
        municipio_expr = f"NULLIF({self._normalized_sql_text_expr(col_municipio)}, '')"
        votes_expr = f"COALESCE(TRY_CAST({col_votes} AS DOUBLE), 0.0)" if col_votes else "0.0"
        grouped = self._rows(
            (
                "WITH candidate_scope AS ("
                "SELECT "
                f"{uf_expr} AS uf, "
                f"{municipio_expr} AS municipio, "
                f"{votes_expr} AS votos "
                f"FROM analytics {where} "
                f"{'AND' if where else 'WHERE'} ({' OR '.join(candidate_clauses)})"
                ") "
                "SELECT uf, municipio, SUM(votos) AS qt_votos "
                "FROM candidate_scope "
                "WHERE uf IS NOT NULL AND municipio IS NOT NULL "
                "GROUP BY uf, municipio "
                "ORDER BY qt_votos DESC, uf ASC, municipio ASC"
            ),
            where_params + candidate_params,
        )
        if not grouped:
            raise ValueError("Nao foi possivel inferir municipio para candidate_id no recorte informado.")

        used_uf, used_municipio, _ = grouped[0]
        disambiguation_applied = False
        if len(grouped) > 1 and (requested_uf is None or requested_municipio is None):
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
                        "used_uf": str(used_uf),
                        "used_municipio": str(used_municipio),
                        "disambiguation_applied": True,
                        "candidates_considered": int(len(grouped)),
                    },
                    ensure_ascii=False,
                )
            )

        used_uf_text = str(used_uf)
        used_municipio_text = self._normalize_municipio_filter(str(used_municipio))
        inferred_uf = requested_uf is None and bool(used_uf_text)
        inferred_municipio = requested_municipio is None and bool(used_municipio_text)
        return {
            "requested_uf": requested_uf,
            "requested_municipio": requested_municipio,
            "used_uf": used_uf_text,
            "used_municipio": used_municipio_text,
            "inferred_geo": bool(inferred_uf or inferred_municipio),
            "inferred_uf": used_uf_text if inferred_uf else None,
            "inferred_municipio": used_municipio_text if inferred_municipio else None,
            "disambiguation_applied": disambiguation_applied,
        }

    def _municipal_zone_candidate_base(
        self,
        *,
        candidate_id: str,
        year: int | None,
        state: str | None,
        office: str | None,
        round_filter: int | None,
        municipality: str | None,
        col_votes: str,
        col_uf: str | None,
        col_municipio: str | None,
        col_cd_municipio: str | None,
        col_zona: str | None,
        trace_id: str | None,
        source_event: str,
    ) -> tuple[pd.DataFrame, str]:
        if not col_zona:
            return pd.DataFrame(columns=["_zona", "_municipio", "_uf", "_cd_municipio", "votes"]), str(candidate_id)

        normalized_state = (state or "").strip().upper() or None
        if normalized_state in {"BR", "BRASIL"}:
            normalized_state = None
        normalized_municipio = self._normalize_municipio_filter(municipality) if municipality else None
        normalized_level = "zona"
        normalized_office = (office or "").strip()
        normalized_turno = int(round_filter) if round_filter is not None else None

        where, where_params = self._where(
            ano=year,
            turno=round_filter,
            uf=normalized_state,
            cargo=office,
            municipio=normalized_municipio,
        )
        candidate_clauses, candidate_params = self._candidate_clauses_and_params(candidate_id)
        if not candidate_clauses:
            return pd.DataFrame(columns=["_zona", "_municipio", "_uf", "_cd_municipio", "votes"]), str(candidate_id)

        uf_expr = f"UPPER(TRIM(CAST({col_uf} AS VARCHAR)))" if col_uf else "CAST(NULL AS VARCHAR)"
        municipio_expr = f"TRIM(CAST({col_municipio} AS VARCHAR))" if col_municipio else "CAST(NULL AS VARCHAR)"
        cd_municipio_expr = f"TRIM(CAST({col_cd_municipio} AS VARCHAR))" if col_cd_municipio else "CAST(NULL AS VARCHAR)"
        zona_expr = f"TRIM(CAST({col_zona} AS VARCHAR))" if col_zona else "CAST(NULL AS VARCHAR)"
        votes_expr = f"COALESCE(TRY_CAST({col_votes} AS DOUBLE), 0.0)"

        sql = (
            "WITH base AS ("
            "SELECT "
            f"{zona_expr} AS _zona, "
            f"{municipio_expr} AS _municipio, "
            f"{uf_expr} AS _uf, "
            f"{cd_municipio_expr} AS _cd_municipio, "
            f"{votes_expr} AS _votes "
            f"FROM analytics {where} "
            f"{'AND' if where else 'WHERE'} ({' OR '.join(candidate_clauses)}) "
            f"AND {zona_expr} IS NOT NULL AND {zona_expr} <> ''"
            ") "
            "SELECT _zona, _municipio, _uf, NULLIF(MIN(COALESCE(_cd_municipio, '')), '') AS _cd_municipio, SUM(_votes) AS votes "
            "FROM base "
            "GROUP BY _zona, _municipio, _uf "
            "ORDER BY votes DESC"
        )
        query_params = where_params + candidate_params
        grouped = self._df(sql, query_params)

        self._log_municipal_zone_debug(
            event=source_event,
            trace_id=trace_id,
            normalized_filters={
                "candidate_id": str(candidate_id),
                "ano": year,
                "cargo": normalized_office,
                "uf": normalized_state,
                "municipio": normalized_municipio,
                "turno": normalized_turno,
                "level": normalized_level,
            },
            sql=sql,
            sql_params=query_params,
            counts={
                "rows_grouped": int(len(grouped)),
                "total_candidate_votes": int(float(grouped["votes"].sum())) if not grouped.empty else 0,
            },
        )
        return grouped, str(candidate_id)

    def _candidate_mask(
        self,
        df: pd.DataFrame,
        candidate_id: str,
        candidate_cpf: str | None = None,
    ) -> pd.Series:
        cpf_candidate = re.sub(r"\D", "", str(candidate_cpf or "")).strip()
        if cpf_candidate and "NR_CPF_CANDIDATO" in df.columns:
            return (
                df["NR_CPF_CANDIDATO"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str.strip()
                == cpf_candidate
            ).fillna(False)

        candidate_norm = self._normalize_value(candidate_id or "", uppercase=True)
        if not candidate_norm:
            return pd.Series([False] * len(df), index=df.index)

        masks: list[pd.Series] = []
        for col in ["SQ_CANDIDATO", "NR_CANDIDATO"]:
            if col in df.columns:
                masks.append(df[col].fillna("").astype(str).str.strip() == str(candidate_id).strip())
        if "NR_CPF_CANDIDATO" in df.columns:
            cpf_candidate = re.sub(r"\D", "", str(candidate_id or "")).strip()
            if cpf_candidate:
                masks.append(
                    df["NR_CPF_CANDIDATO"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str.strip()
                    == cpf_candidate
                )
        col_nome = "NM_CANDIDATO" if "NM_CANDIDATO" in df.columns else ("NM_URNA_CANDIDATO" if "NM_URNA_CANDIDATO" in df.columns else None)
        if col_nome:
            masks.append(
                df[col_nome].fillna("").astype(str).str.strip().apply(lambda value: self._normalize_value(value, uppercase=True))
                == candidate_norm
            )
        if not masks:
            return pd.Series([False] * len(df), index=df.index)
        out = masks[0]
        for mask in masks[1:]:
            out = out | mask
        return out.fillna(False)

    def _vote_history_projection_columns(self) -> list[str]:
        preferred = [
            "ANO_ELEICAO",
            "NR_ANO_ELEICAO",
            "QT_VOTOS_NOMINAIS_VALIDOS",
            "NR_VOTACAO_NOMINAL",
            "QT_VOTOS_NOMINAIS",
            "NR_TURNO",
            "CD_TURNO",
            "DS_TURNO",
            "DS_CARGO",
            "DS_CARGO_D",
            "SG_UF",
            "NM_UE",
            "NM_MUNICIPIO",
            "SG_PARTIDO",
            "NR_CANDIDATO",
            "NR_CPF_CANDIDATO",
            "SQ_CANDIDATO",
            "NM_CANDIDATO",
            "NM_URNA_CANDIDATO",
            "DS_SIT_TOT_TURNO",
            "DT_NASCIMENTO",
        ]
        return [col for col in dict.fromkeys(preferred) if self._has(col)]

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
            valid_ufs_sql = ", ".join(f"'{uf}'" for uf in sorted(VALID_UF_CODES))
            rows = self._rows(
                (
                    f"SELECT DISTINCT UPPER(TRIM(CAST({col_uf} AS VARCHAR))) AS uf "
                    "FROM analytics "
                    "WHERE COALESCE(TRIM(CAST(SG_UF AS VARCHAR)), '') <> '' "
                    f"AND UPPER(TRIM(CAST({col_uf} AS VARCHAR))) IN ({valid_ufs_sql}) "
                    "ORDER BY uf"
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

        return {"anos": anos, "ufs": ufs, "cargos": cargos}

    def overview(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
    ) -> dict:
        df = self._filtered_df(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        if turno is None:
            df = self._latest_vote_context_rows(df)
        if self._is_national_presidential_scope(cargo=cargo, uf=uf, municipio=municipio):
            df = self._dedupe_national_presidential_candidates(df)
            col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
            col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
            col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
            total_eleitos = int(self._is_elected_series(df[col_situacao]).sum()) if col_situacao and col_situacao in df.columns else None
            total_votos = (
                int(pd.to_numeric(df[col_votos], errors="coerce").fillna(0).sum())
                if col_votos and col_votos in df.columns
                else None
            )
            return {
                "total_registros": int(len(df)),
                "total_candidatos": int(df[col_candidato].nunique()) if col_candidato and col_candidato in df.columns else None,
                "total_eleitos": total_eleitos,
                "total_votos_nominais": total_votos,
            }

        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        total_registros = int(len(df))
        total_candidatos = int(df[col_candidato].nunique()) if col_candidato and col_candidato in df.columns else None
        total_eleitos = int(self._is_elected_series(df[col_situacao]).sum()) if col_situacao and col_situacao in df.columns else None
        total_votos = int(pd.to_numeric(df[col_votos], errors="coerce").fillna(0).sum()) if col_votos and col_votos in df.columns else None

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
        effective_page_size = min(page_size or top_n or self.default_top_n, self.max_top_n)
        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidato = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
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
        df = self._filtered_df(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio).copy()
        if partido:
            if col_partido and col_partido in df.columns:
                df = df[
                    df[col_partido]
                    .fillna("")
                    .astype(str)
                    .map(lambda value: self._normalize_value(value, uppercase=True))
                    == self._normalize_value(partido, uppercase=True)
                ].copy()
        if turno is None:
            df = self._latest_vote_context_rows(df)

        if col_candidate_key:
            df = df.assign(
                _candidate_key=df[col_candidate_key]
                .fillna("")
                .astype(str)
                .map(lambda value: self._normalize_value(value, uppercase=True))
                .replace("", pd.NA)
            )
        else:
            df = df.assign(
                _candidate_key=df[col_candidato]
                .fillna("")
                .astype(str)
                .map(self._normalize_value)
                .str.lower()
                .replace("", pd.NA)
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

        df = df.assign(_votos=pd.to_numeric(df[col_votos], errors="coerce").fillna(0))
        candidate_index = df.groupby("_candidate_key", sort=False).indices
        grouped = df.groupby("_candidate_key", as_index=False).agg(
            _votos=("_votos", "sum"),
            _candidato=(col_candidato, "first"),
            _candidate_id=(col_candidate_key, "first") if col_candidate_key else ("_candidate_key", "first"),
            _partido=(col_partido, "first") if col_partido else ("_candidate_key", "first"),
            _cargo=(col_cargo, "first") if col_cargo else ("_candidate_key", "first"),
            _situacao=(col_situacao, "first") if col_situacao else ("_candidate_key", "first"),
        )
        if not col_partido:
            grouped.loc[:, "_partido"] = None
        if not col_cargo:
            grouped.loc[:, "_cargo"] = None
        if not col_situacao:
            grouped.loc[:, "_situacao"] = None
        if col_uf:
            uf_stats = (
                df.assign(
                    _uf_norm=df[col_uf]
                    .fillna("")
                    .astype(str)
                    .map(lambda value: self._normalize_value(value, uppercase=True))
                    .replace("", pd.NA)
                )
                .groupby("_candidate_key", as_index=False)
                .agg(_uf_count=("_uf_norm", "nunique"), _uf_first=("_uf_norm", "first"))
            )
            grouped = grouped.merge(uf_stats, on="_candidate_key", how="left")
            grouped.loc[:, "_uf_out"] = grouped["_uf_first"].where(grouped["_uf_count"].fillna(0).astype(int).eq(1))
        else:
            grouped.loc[:, "_uf_out"] = None

        grouped = grouped.assign(
            _candidato_sort=grouped["_candidato"].fillna("").astype(str).map(self._normalize_value).str.lower()
        )
        grouped = grouped.sort_values(["_votos", "_candidato_sort"], ascending=[False, True])
        total = int(len(grouped))
        total_pages = (total + effective_page_size - 1) // effective_page_size if total else 0
        start = (page - 1) * effective_page_size
        end = start + effective_page_size
        sliced = grouped.iloc[start:end]

        items = []
        for _, row in sliced.iterrows():
            candidate_key = str(row["_candidate_key"])
            candidate_id = str(row["_candidate_id"]).strip() if pd.notna(row["_candidate_id"]) and str(row["_candidate_id"]).strip() else candidate_key
            candidate_positions = candidate_index.get(candidate_key)
            if candidate_positions is None:
                continue
            candidate_rows = df.iloc[candidate_positions].copy()
            identity_payload = self._candidate_identity_payload(candidate_rows)
            turn_breakdown = self._candidate_vote_turn_breakdown(candidate_rows)
            items.append(
                {
                    "candidate_id": candidate_id,
                    "source_id": identity_payload["source_id"],
                    "canonical_candidate_id": identity_payload["canonical_candidate_id"],
                    "person_id": identity_payload["person_id"],
                    "turno_referencia": turn_breakdown["turno_referencia"],
                    "latest_vote_round": turn_breakdown["turno_referencia"],
                    "latest_vote_value": int(row["_votos"]),
                    "candidato": str(row["_candidato"]) if pd.notna(row["_candidato"]) else "",
                    "partido": str(row["_partido"]) if pd.notna(row["_partido"]) else None,
                    "cargo": str(row["_cargo"]) if pd.notna(row["_cargo"]) else None,
                    "uf": str(row["_uf_out"]) if pd.notna(row["_uf_out"]) else None,
                    "votos": int(row["_votos"]),
                    "situacao": str(row["_situacao"]) if pd.notna(row["_situacao"]) else None,
                }
            )
        return {
            "top_n": effective_page_size,
            "page": page,
            "page_size": effective_page_size,
            "total": total,
            "total_pages": total_pages,
            "items": items,
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
        if self._is_national_presidential_scope(cargo=cargo, uf=uf, municipio=municipio) and group_by != "uf":
            df = self._filtered_df(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
            if somente_eleitos:
                col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
                if col_situacao and col_situacao in df.columns:
                    df = df[self._is_elected_series(df[col_situacao])]
            df = self._dedupe_national_presidential_candidates(df)
            labels = self._resolve_distribution_labels(df, group_by, target_col)
            counts = (
                labels
                .value_counts(dropna=False)
                .rename_axis("label")
                .reset_index(name="value")
            )
            total = float(counts["value"].sum()) or 1.0
            return self._filter_distribution_items([
                {
                    "label": str(row["label"]),
                    "value": float(row["value"]),
                    "percentage": round((float(row["value"]) / total) * 100, 2),
                }
                for _, row in counts.iterrows()
            ])
        label_expr = self._distribution_label_sql_expr(group_by, target_col)
        if self._distribution_uses_candidate_resolution(group_by):
            candidate_key_expr = self._candidate_group_key_sql_expr()
            if candidate_key_expr:
                rows = self._rows(
                    (
                        "WITH prepared AS ("
                        "  SELECT "
                        f"    {candidate_key_expr} AS candidate_key, "
                        f"    {label_expr} AS label "
                        f"  FROM analytics {where}"
                        "), ranked AS ("
                        "  SELECT "
                        "    candidate_key, "
                        "    label, "
                        "    COUNT(*) AS label_count, "
                        "    MAX(CASE WHEN label <> 'N/A' THEN 1 ELSE 0 END) AS known_rank "
                        "  FROM prepared "
                        "  WHERE candidate_key IS NOT NULL "
                        "  GROUP BY 1, 2"
                        "), resolved AS ("
                        "  SELECT candidate_key, label "
                        "  FROM ("
                        "    SELECT "
                        "      candidate_key, "
                        "      label, "
                        "      ROW_NUMBER() OVER ("
                        "        PARTITION BY candidate_key "
                        "        ORDER BY known_rank DESC, label_count DESC, label ASC"
                        "      ) AS rn "
                        "    FROM ranked"
                        "  ) ranked_labels "
                        "  WHERE rn = 1"
                        ") "
                        "SELECT label, COUNT(*) AS value "
                        "FROM resolved GROUP BY 1 ORDER BY value DESC"
                    ),
                    params,
                )
                total = float(sum(int(r[1]) for r in rows) or 1)
                return self._filter_distribution_items([
                    {
                        "label": str(r[0]),
                        "value": float(r[1]),
                        "percentage": round((float(r[1]) / total) * 100, 2),
                    }
                    for r in rows
                ])
        rows = self._rows(
            (
                "SELECT "
                f"{label_expr} AS label, "
                "COUNT(*) AS value "
                f"FROM analytics {where} GROUP BY 1 ORDER BY value DESC"
            ),
            params,
        )
        total = float(sum(int(r[1]) for r in rows) or 1)
        return self._filter_distribution_items([
            {
                "label": str(r[0]),
                "value": float(r[1]),
                "percentage": round((float(r[1]) / total) * 100, 2),
            }
            for r in rows
        ])

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
            return {"items": []}

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
        valid_keys = [key for key in COR_RACA_CATEGORY_ORDER if key != "NAO_INFORMADO"]
        total_candidatos = int(sum(by_category.get(key, {"candidatos": 0})["candidatos"] for key in valid_keys))
        total_eleitos = int(sum(by_category.get(key, {"eleitos": 0})["eleitos"] for key in valid_keys))

        items: list[dict] = []
        for key in COR_RACA_CATEGORY_ORDER:
            if key == "NAO_INFORMADO":
                continue
            values = by_category.get(key, {"candidatos": 0, "eleitos": 0})
            candidatos = int(values["candidatos"])
            eleitos = int(values["eleitos"])
            if candidatos == 0 and eleitos == 0:
                continue
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
            if str(r[0]) != "N/A" and (int(r[1] or 0) > 0 or int(r[2] or 0) > 0)
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

        if self._is_national_presidential_scope(cargo=cargo, uf=uf, municipio=municipio) and group_by != "uf":
            df = self._filtered_df(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
            if somente_eleitos:
                col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
                if col_situacao and col_situacao in df.columns:
                    df = df[self._is_elected_series(df[col_situacao])]
            df = self._dedupe_national_presidential_candidates(df)
            if df.empty:
                return []

            col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
            col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
            col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
            prepared = df.assign(
                _group=df[target_col].fillna("N/A").astype(str).str.strip().replace("", "N/A")
            )
            if metric == "registros":
                agg = prepared.groupby("_group", dropna=False).size().reset_index(name="value")
            elif metric == "votos_nominais":
                if not col_votos or col_votos not in prepared.columns:
                    return []
                agg = (
                    prepared.assign(_value=pd.to_numeric(prepared[col_votos], errors="coerce").fillna(0))
                    .groupby("_group", dropna=False)["_value"]
                    .sum()
                    .reset_index(name="value")
                )
            elif metric == "candidatos":
                if not col_candidato or col_candidato not in prepared.columns:
                    return []
                agg = (
                    prepared.groupby("_group", dropna=False)[col_candidato]
                    .nunique(dropna=True)
                    .reset_index(name="value")
                )
            elif metric == "eleitos":
                if not col_situacao or col_situacao not in prepared.columns:
                    return []
                agg = (
                    prepared.assign(_value=self._is_elected_series(prepared[col_situacao]).astype(int))
                    .groupby("_group", dropna=False)["_value"]
                    .sum()
                    .reset_index(name="value")
                )
            else:
                return []

            agg = agg.sort_values("value", ascending=False).head(min(top_n or self.default_top_n, self.max_top_n))
            total = float(agg["value"].sum()) or 1.0
            return [
                {
                    "label": str(row["_group"]),
                    "value": float(row["value"] or 0),
                    "percentage": round((float(row["value"] or 0) / total) * 100, 2),
                }
                for _, row in agg.iterrows()
            ]

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
        turno: int | None = None,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        scoped = self._filtered_df(ano=year, uf=state, cargo=office)
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        if candidate_rows.empty:
            return {
                "candidate_id": str(candidate_id),
                "source_id": None,
                "nr_cpf_candidato": None,
                "canonical_candidate_id": None,
                "person_id": None,
                "name": "",
                "number": None,
                "party": None,
                "coalition": None,
                "office": office,
                "state": state.upper() if state else None,
                "mandates": 0,
                "status": None,
                "turno_referencia": None,
                "votos_primeiro_turno": None,
                "votos_segundo_turno": None,
                "votos_consolidados": None,
                "latest_election": {
                    "year": year or 0,
                    "turno_referencia": None,
                    "votes": 0,
                    "vote_share": 0.0,
                    "state_rank": None,
                },
            }

        col_name = "NM_CANDIDATO" if "NM_CANDIDATO" in candidate_rows.columns else ("NM_URNA_CANDIDATO" if "NM_URNA_CANDIDATO" in candidate_rows.columns else None)
        col_year = "ANO_ELEICAO" if "ANO_ELEICAO" in candidate_rows.columns else ("NR_ANO_ELEICAO" if "NR_ANO_ELEICAO" in candidate_rows.columns else None)
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in candidate_rows.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in candidate_rows.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in candidate_rows.columns else None))
        )
        col_round = "NR_TURNO" if "NR_TURNO" in candidate_rows.columns else ("CD_TURNO" if "CD_TURNO" in candidate_rows.columns else ("DS_TURNO" if "DS_TURNO" in candidate_rows.columns else None))
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

        turn_breakdown = self._candidate_vote_turn_breakdown(latest_rows)
        requested_turno = int(turno) if turno is not None and int(turno) > 0 else None
        turno_referencia = requested_turno if requested_turno in {1, 2} else turn_breakdown["turno_referencia"]
        official_rows = latest_rows.copy()
        if turno_referencia is not None and col_round:
            official_rows = latest_rows[self._extract_turno(latest_rows[col_round]) == int(turno_referencia)].copy()
            if official_rows.empty:
                official_rows = latest_rows.copy()

        latest_votes = int(pd.to_numeric(official_rows["_votes"], errors="coerce").fillna(0).sum())
        consolidated_votes = int(pd.to_numeric(latest_rows["_votes"], errors="coerce").fillna(0).sum())
        latest_state = (
            official_rows["SG_UF"].fillna("").astype(str).str.strip().str.upper().replace("", pd.NA).dropna().iloc[0]
            if "SG_UF" in official_rows.columns and official_rows["SG_UF"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
            else (state.upper() if state else None)
        )
        latest_office = (
            official_rows["DS_CARGO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
            if "DS_CARGO" in official_rows.columns and official_rows["DS_CARGO"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
            else (
                official_rows["DS_CARGO_D"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().iloc[0]
                if "DS_CARGO_D" in official_rows.columns and official_rows["DS_CARGO_D"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().any()
                else office
            )
        )

        vote_share = 0.0
        rank = None
        if col_votes and col_year and latest_year:
            context_df = self._filtered_df(ano=latest_year, turno=turno_referencia, uf=latest_state, cargo=latest_office)
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
        if "DS_SIT_TOT_TURNO" in official_rows.columns:
            statuses = official_rows["DS_SIT_TOT_TURNO"].fillna("").astype(str).str.strip()
            statuses = statuses[statuses != ""]
            if not statuses.empty:
                latest_status = str(statuses.iloc[0])

        identity_payload = self._candidate_identity_payload(candidate_rows)

        return {
            "candidate_id": str(candidate_id),
            "source_id": identity_payload["source_id"],
            "nr_cpf_candidato": identity_payload["nr_cpf_candidato"],
            "canonical_candidate_id": identity_payload["canonical_candidate_id"],
            "person_id": identity_payload["person_id"],
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
            "turno_referencia": turno_referencia,
            "latest_vote_round": turno_referencia,
            "latest_vote_value": latest_votes,
            "votos_primeiro_turno": turn_breakdown["votos_primeiro_turno"],
            "votos_segundo_turno": turn_breakdown["votos_segundo_turno"],
            "votos_consolidados": consolidated_votes,
            "latest_election": {
                "year": latest_year,
                "turno_referencia": turno_referencia,
                "votes": latest_votes,
                "vote_share": vote_share,
                "state_rank": rank,
            },
        }

    def candidate_vote_history(
        self,
        candidate_id: str,
        candidate_cpf: str | None = None,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        return CandidateHistoryMixin.candidate_vote_history(self, candidate_id, candidate_cpf=candidate_cpf, state=state, office=office)

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
                "source_id": None,
                "canonical_candidate_id": None,
                "person_id": None,
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

        identity_payload = self._candidate_identity_payload(candidate_rows)

        return {
            "candidate_id": str(candidate_id),
            "source_id": identity_payload["source_id"],
            "canonical_candidate_id": identity_payload["canonical_candidate_id"],
            "person_id": identity_payload["person_id"],
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
        scoped = self._filtered_df(
            ano=year,
            turno=round_filter,
            uf=normalized_state,
            cargo=office,
            municipio=municipality,
        )
        candidate_rows = scoped[self._candidate_mask(scoped, candidate_id)].copy()
        col_votes = (
            "QT_VOTOS_NOMINAIS_VALIDOS"
            if "QT_VOTOS_NOMINAIS_VALIDOS" in candidate_rows.columns
            else ("NR_VOTACAO_NOMINAL" if "NR_VOTACAO_NOMINAL" in candidate_rows.columns else ("QT_VOTOS_NOMINAIS" if "QT_VOTOS_NOMINAIS" in candidate_rows.columns else None))
        )
        if candidate_rows.empty or not col_votes:
            return {
                "candidate_id": str(candidate_id),
                "level": level_key,
                "page": page,
                "page_size": page_size,
                "total_items": 0,
                "items": [],
            }

        col_uf = "SG_UF" if "SG_UF" in candidate_rows.columns else None
        col_municipio = "NM_UE" if "NM_UE" in candidate_rows.columns else ("NM_MUNICIPIO" if "NM_MUNICIPIO" in candidate_rows.columns else None)
        col_zona = "NR_ZONA" if "NR_ZONA" in candidate_rows.columns else ("CD_ZONA" if "CD_ZONA" in candidate_rows.columns else ("SQ_ZONA" if "SQ_ZONA" in candidate_rows.columns else None))
        col_cargo = "DS_CARGO" if "DS_CARGO" in candidate_rows.columns else ("DS_CARGO_D" if "DS_CARGO_D" in candidate_rows.columns else None)
        if office:
            municipal_scope = self._cargo_scope(office) == "municipio"
        elif col_cargo:
            cargo_norm = candidate_rows[col_cargo].fillna("").astype(str).str.strip().str.upper()
            municipal_scope = cargo_norm.isin(MUNICIPAL_CARGOS).any()
        else:
            municipal_scope = False

        if level_key == "municipio":
            if not col_municipio:
                return {
                    "candidate_id": str(candidate_id),
                    "level": level_key,
                    "page": page,
                    "page_size": page_size,
                    "total_items": 0,
                    "items": [],
                }

            where, where_params = self._where(
                ano=year,
                turno=round_filter,
                uf=normalized_state,
                cargo=office,
                municipio=municipality,
            )
            candidate_norm = str(candidate_id or "").strip()
            candidate_upper = self._normalize_value(candidate_norm, uppercase=True)
            candidate_clauses: list[str] = []
            candidate_params: list[str] = []
            if self._has("SQ_CANDIDATO"):
                candidate_clauses.append("TRIM(CAST(SQ_CANDIDATO AS VARCHAR)) = ?")
                candidate_params.append(candidate_norm)
            if self._has("NR_CANDIDATO"):
                candidate_clauses.append("TRIM(CAST(NR_CANDIDATO AS VARCHAR)) = ?")
                candidate_params.append(candidate_norm)
            col_nome = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
            if col_nome:
                candidate_clauses.append(
                    "UPPER(regexp_replace(regexp_replace(CAST("
                    f"{col_nome} AS VARCHAR), '[^\\x00-\\x7F]', ''), '\\\\s+', ' ')) = ?"
                )
                candidate_params.append(candidate_upper)
            if not candidate_clauses:
                return {
                    "candidate_id": str(candidate_id),
                    "level": level_key,
                    "page": page,
                    "page_size": page_size,
                    "total_items": 0,
                    "items": [],
                }

            municipio_expr = f"NULLIF(TRIM(CAST({col_municipio} AS VARCHAR)), '')"
            votes_expr = f"COALESCE(TRY_CAST({col_votes} AS DOUBLE), 0.0)"
            candidate_case_sql = " OR ".join(f"({clause})" for clause in candidate_clauses)
            cte_sql = (
                "WITH filtered AS ("
                f"SELECT {municipio_expr} AS nm_municipio, "
                f"{votes_expr} AS votos, "
                f"CASE WHEN ({candidate_case_sql}) THEN 1 ELSE 0 END AS is_candidate "
                f"FROM analytics {where}"
                "), "
                "municipio_totals AS ("
                "SELECT nm_municipio, SUM(votos) AS votos_validos_do_municipio "
                "FROM filtered "
                "WHERE nm_municipio IS NOT NULL "
                "GROUP BY nm_municipio"
                "), "
                "candidate_totals AS ("
                "SELECT nm_municipio, SUM(votos) AS qt_votos "
                "FROM filtered "
                "WHERE is_candidate = 1 AND nm_municipio IS NOT NULL "
                "GROUP BY nm_municipio"
                "), "
                "candidate_grand AS ("
                "SELECT COALESCE(SUM(qt_votos), 0.0) AS total_votos_candidato_no_recorte "
                "FROM candidate_totals"
                ") "
            )

            count_sql = cte_sql + "SELECT COUNT(*) FROM candidate_totals"
            count_params = where_params + candidate_params
            total_items = int(self._scalar(count_sql, count_params) or 0)
            if total_items == 0:
                return {
                    "candidate_id": str(candidate_id),
                    "level": level_key,
                    "page": page,
                    "page_size": page_size,
                    "total_items": 0,
                    "items": [],
                }

            sort_options = {
                "key": "c.nm_municipio",
                "label": "c.nm_municipio",
                "votes": "c.qt_votos",
                "vote_share": (
                    "CASE WHEN g.total_votos_candidato_no_recorte > 0 "
                    "THEN (c.qt_votos / g.total_votos_candidato_no_recorte) * 100.0 "
                    "ELSE 0.0 END"
                ),
                "nm_municipio": "c.nm_municipio",
                "votos_validos_do_municipio": "m.votos_validos_do_municipio",
                "qt_votos": "c.qt_votos",
                "percentual_candidato_no_municipio": (
                    "CASE WHEN m.votos_validos_do_municipio > 0 "
                    "THEN (c.qt_votos / m.votos_validos_do_municipio) * 100.0 "
                    "ELSE 0.0 END"
                ),
                "percentual_no_total_do_candidato": (
                    "CASE WHEN g.total_votos_candidato_no_recorte > 0 "
                    "THEN (c.qt_votos / g.total_votos_candidato_no_recorte) * 100.0 "
                    "ELSE 0.0 END"
                ),
                "categoria_impacto": (
                    "CASE "
                    "WHEN CASE WHEN g.total_votos_candidato_no_recorte > 0 "
                    "THEN (c.qt_votos / g.total_votos_candidato_no_recorte) * 100.0 ELSE 0.0 END >= 1.0 THEN 'Alto' "
                    "WHEN CASE WHEN g.total_votos_candidato_no_recorte > 0 "
                    "THEN (c.qt_votos / g.total_votos_candidato_no_recorte) * 100.0 ELSE 0.0 END >= 0.3 THEN 'Médio' "
                    "ELSE 'Baixo' "
                    "END"
                ),
            }
            effective_sort_by = (sort_by or "qt_votos").strip().lower()
            if effective_sort_by not in sort_options:
                effective_sort_by = "qt_votos"
            effective_sort_order = (sort_order or "desc").strip().lower()
            sort_dir = "ASC" if effective_sort_order == "asc" else "DESC"
            order_by_sql = f"ORDER BY {sort_options[effective_sort_by]} {sort_dir}, c.nm_municipio ASC"

            pagination_sql = ""
            rows_params = where_params + candidate_params
            if page is not None and page_size is not None:
                offset = (int(page) - 1) * int(page_size)
                pagination_sql = " LIMIT ? OFFSET ?"
                rows_params = rows_params + [int(page_size), int(offset)]

            rows_sql = (
                cte_sql
                + "SELECT "
                "c.nm_municipio, "
                "m.votos_validos_do_municipio, "
                "c.qt_votos, "
                "g.total_votos_candidato_no_recorte "
                "FROM candidate_totals c "
                "JOIN municipio_totals m ON m.nm_municipio = c.nm_municipio "
                "CROSS JOIN candidate_grand g "
                f"{order_by_sql}{pagination_sql}"
            )
            rows = self._rows(rows_sql, rows_params)
            items: list[dict[str, object]] = []
            for nm_municipio, votos_validos_do_municipio, qt_votos, total_candidato in rows:
                qt_votos_f = float(qt_votos or 0.0)
                votos_validos_f = float(votos_validos_do_municipio or 0.0)
                total_candidato_f = float(total_candidato or 0.0)
                percentual_municipio = _safe_percent(qt_votos_f, votos_validos_f)
                percentual_total = _safe_percent(qt_votos_f, total_candidato_f)
                items.append(
                    {
                        "key": str(nm_municipio),
                        "label": str(nm_municipio),
                        "votes": int(qt_votos_f),
                        "vote_share": round(percentual_total, 4),
                        "nm_municipio": str(nm_municipio),
                        "votos_validos_do_municipio": int(votos_validos_f),
                        "qt_votos": int(qt_votos_f),
                        "percentual_candidato_no_municipio": percentual_municipio,
                        "percentual_no_total_do_candidato": percentual_total,
                        "categoria_impacto": _impact_category(percentual_total),
                    }
                )
            return {
                "candidate_id": str(candidate_id),
                "level": level_key,
                "page": page,
                "page_size": page_size,
                "total_items": total_items,
                "items": items,
            }

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
        else:
            if not col_zona:
                return {"candidate_id": str(candidate_id), "level": level_key, "items": []}

            if municipal_scope:
                agg, resolved_candidate_id = self._municipal_zone_candidate_base(
                    candidate_id=candidate_id,
                    year=year,
                    state=normalized_state,
                    office=office,
                    round_filter=round_filter,
                    municipality=municipality,
                    col_votes=col_votes,
                    col_uf=col_uf,
                    col_municipio=col_municipio,
                    col_cd_municipio=None,
                    col_zona=col_zona,
                    trace_id=trace_id,
                    source_event="candidate_vote_distribution_municipal_zone_base",
                )
            else:
                grouped = candidate_rows.assign(
                    _zona=candidate_rows[col_zona].fillna("").astype(str).str.strip().replace("", pd.NA),
                    _municipio=(
                        candidate_rows[col_municipio].fillna("").astype(str).str.strip().replace("", pd.NA)
                        if col_municipio
                        else pd.Series([pd.NA] * len(candidate_rows), index=candidate_rows.index)
                    ),
                    _uf=(
                        candidate_rows[col_uf].fillna("").astype(str).str.strip().str.upper().replace("", pd.NA)
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

            total_candidate_votes = float(agg["votes"].sum()) if not agg.empty else 0.0
            total_votes_safe = total_candidate_votes if total_candidate_votes > 0 else 1.0
            items: list[dict[str, object]] = []
            for _, row in agg.iterrows():
                zone_id = str(row["_zona"])
                row_votes = float(pd.to_numeric(row["votes"], errors="coerce") or 0.0)
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
                            "municipio": str(row["_municipio"]) if pd.notna(row["_municipio"]) else None,
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
                "candidate_id": str(candidate_id),
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
        if page is not None and page_size is not None:
            offset = (int(page) - 1) * int(page_size)
            items = items[offset : offset + int(page_size)]
        return {
            "candidate_id": str(candidate_id),
            "level": level_key,
            "page": page,
            "page_size": page_size,
            "total_items": total_items,
            "items": items,
        }

    def candidates_compare(
        self,
        candidate_ids: list[str],
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
    ) -> dict:
        return CandidateHistoryMixin.candidates_compare(self, candidate_ids, year=year, state=state, office=office)

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
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        df = self._filtered_df(ano=ano, turno=turno, uf=uf, cargo=cargo).copy()
        if partido and col_partido and col_partido in df.columns:
            df = df[
                df[col_partido]
                .fillna("")
                .astype(str)
                .map(lambda value: self._normalize_value(value, uppercase=True))
                == self._normalize_value(partido, uppercase=True)
            ].copy()
        if turno is None:
            df = self._latest_vote_context_rows(df)

        if col_candidato not in df.columns:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        names = df[col_candidato].fillna("").astype(str).map(self._normalize_value)
        names_norm = names.str.lower()
        matched = names_norm.str.contains(q_norm, regex=False)
        if not matched.any():
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        ranked = df.loc[matched].copy()
        name_match_norm = names_norm.loc[matched]
        score = (
            (name_match_norm == q_norm).astype(int) * 3
            + (name_match_norm.str.startswith(q_norm)).astype(int) * 2
            + (name_match_norm.str.contains(q_norm, regex=False)).astype(int)
        )
        if col_votos and col_votos in ranked.columns:
            votos = pd.to_numeric(ranked[col_votos], errors="coerce").fillna(0).astype("int64")
        else:
            votos = pd.Series(0, index=ranked.index, dtype="int64")

        ranked = ranked.assign(
            _score=score,
            _votos=votos,
            _candidate_id=(
                ranked[col_candidate_id]
                .fillna("")
                .astype(str)
                .map(lambda value: self._normalize_value(value, uppercase=True))
                .replace("", pd.NA)
                if col_candidate_id and col_candidate_id in ranked.columns
                else ranked[col_candidato].fillna("").astype(str).map(self._normalize_value).replace("", pd.NA)
            ),
            _candidato=ranked[col_candidato].fillna("").astype(str).map(self._normalize_value),
            _partido=ranked[col_partido].fillna("").astype(str).map(self._normalize_value).replace("", pd.NA) if col_partido and col_partido in ranked.columns else pd.NA,
            _cargo=ranked[col_cargo].fillna("").astype(str).map(self._normalize_value).replace("", pd.NA) if col_cargo and col_cargo in ranked.columns else pd.NA,
            _uf=ranked[col_uf].fillna("").astype(str).map(lambda value: self._normalize_value(value, uppercase=True)).replace("", pd.NA) if col_uf and col_uf in ranked.columns else pd.NA,
            _numero=ranked[col_numero].fillna("").astype(str).map(self._normalize_value).replace("", pd.NA) if col_numero and col_numero in ranked.columns else pd.NA,
            _situacao=ranked[col_situacao].fillna("").astype(str).map(self._normalize_value).replace("", pd.NA) if col_situacao and col_situacao in ranked.columns else pd.NA,
        )

        grouped = (
            ranked.groupby(
                ["_candidate_id", "_candidato", "_partido", "_cargo", "_uf", "_numero", "_situacao"],
                dropna=False,
                as_index=False,
            )
            .agg(_score=("_score", "max"), _votos=("_votos", "sum"))
            .sort_values(by=["_votos", "_score", "_candidato"], ascending=[False, False, True])
        )

        total = int(len(grouped))
        total_pages = (total + page_size - 1) // page_size if total else 0
        start = (page - 1) * page_size
        end = start + page_size
        sliced = grouped.iloc[start:end]

        if col_candidate_id and col_candidate_id in ranked.columns:
            candidate_index = ranked.groupby("_candidate_id", sort=False).indices
        else:
            candidate_index = ranked.groupby("_candidate_id", sort=False).indices

        items: list[dict] = []
        for _, row in sliced.iterrows():
            candidate_id_raw = row["_candidate_id"] if pd.notna(row["_candidate_id"]) and str(row["_candidate_id"]).strip() else row["_candidato"]
            candidate_id = str(candidate_id_raw).strip() if pd.notna(candidate_id_raw) else ""
            if not candidate_id:
                continue
            candidate_positions = candidate_index.get(candidate_id)
            if candidate_positions is None:
                continue
            candidate_rows = ranked.iloc[candidate_positions].copy()
            identity_payload = self._candidate_identity_payload(candidate_rows)
            turn_breakdown = self._candidate_vote_turn_breakdown(candidate_rows)
            numero_raw = row["_numero"] if pd.notna(row["_numero"]) and str(row["_numero"]).strip() else candidate_id
            items.append(
                {
                    "candidate_id": candidate_id,
                    "source_id": identity_payload["source_id"],
                    "canonical_candidate_id": identity_payload["canonical_candidate_id"],
                    "person_id": identity_payload["person_id"],
                    "turno_referencia": turn_breakdown["turno_referencia"],
                    "latest_vote_round": turn_breakdown["turno_referencia"],
                    "latest_vote_value": int(row["_votos"]),
                    "candidato": str(row["_candidato"]) if pd.notna(row["_candidato"]) else "",
                    "partido": (str(row["_partido"]) if pd.notna(row["_partido"]) else None),
                    "cargo": (str(row["_cargo"]) if pd.notna(row["_cargo"]) else None),
                    "uf": (str(row["_uf"]) if pd.notna(row["_uf"]) else None),
                    "numero": (str(numero_raw) if pd.notna(numero_raw) else None),
                    "votos": int(row["_votos"]) if col_votos else None,
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
