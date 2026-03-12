from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import duckdb


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


@dataclass
class DuckDBAnalyticsService:
    conn: duckdb.DuckDBPyConnection
    default_top_n: int
    max_top_n: int
    _query_lock: Lock
    _columns: set[str]

    @classmethod
    def from_file(
        cls,
        file_path: str,
        default_top_n: int,
        max_top_n: int,
        separator: str = ",",
        encoding: str = "utf-8",
    ) -> "DuckDBAnalyticsService":
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)

        conn = duckdb.connect(database=":memory:")
        suffix = path.suffix.lower()
        source_path = str(path).replace("'", "''")
        delim = separator.replace("'", "''")
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
        return cls(
            conn=conn,
            default_top_n=default_top_n,
            max_top_n=max_top_n,
            _query_lock=Lock(),
            _columns=cols,
        )

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

    def _where(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> tuple[str, list]:
        clauses = []
        params: list = []

        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_turno = self._pick_col(["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
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
        if municipio and col_municipio:
            clauses.append(f"UPPER(TRIM(CAST({col_municipio} AS VARCHAR))) = ?")
            params.append(municipio.upper().strip())
        if somente_eleitos and col_situacao:
            clauses.append(f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) LIKE 'ELEITO%'")

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, params

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

        return {"anos": anos, "ufs": ufs, "cargos": cargos}

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
        municipio: str | None = None,
        top_n: int | None = None,
    ) -> list[dict]:
        col_candidate_key = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidato = self._pick_col(["NM_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_candidato or not col_votos:
            return []

        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        n = min(top_n or self.default_top_n, self.max_top_n)

        key_expr = (
            f"COALESCE(NULLIF(TRIM(CAST({col_candidate_key} AS VARCHAR)), ''), LOWER(TRIM(CAST({col_candidato} AS VARCHAR))))"
            if col_candidate_key
            else f"LOWER(TRIM(CAST({col_candidato} AS VARCHAR)))"
        )
        candidato_expr = f"COALESCE(NULLIF(TRIM(CAST({col_candidato} AS VARCHAR)), ''), 'N/A')"
        partido_expr = f"{'CAST(' + col_partido + ' AS VARCHAR)' if col_partido else 'NULL'}"
        cargo_expr = f"{'CAST(' + col_cargo + ' AS VARCHAR)' if col_cargo else 'NULL'}"
        situacao_expr = f"{'CAST(' + col_situacao + ' AS VARCHAR)' if col_situacao else 'NULL'}"
        uf_norm_expr = f"{'NULLIF(UPPER(TRIM(CAST(' + col_uf + ' AS VARCHAR))), \'\')' if col_uf else 'NULL'}"

        rows = self._rows(
            (
                "SELECT "
                "MIN(candidato) AS candidato, "
                "MIN(partido) AS partido, "
                "MIN(cargo) AS cargo, "
                "CASE WHEN COUNT(DISTINCT uf_norm) = 1 THEN MIN(uf_norm) ELSE NULL END AS uf, "
                "SUM(votos) AS votos, "
                "MIN(situacao) AS situacao "
                "FROM ("
                "  SELECT "
                f"  {key_expr} AS candidate_key, "
                f"  {candidato_expr} AS candidato, "
                f"  {partido_expr} AS partido, "
                f"  {cargo_expr} AS cargo, "
                f"  {uf_norm_expr} AS uf_norm, "
                f"  COALESCE(TRY_CAST({col_votos} AS BIGINT), 0) AS votos, "
                f"  {situacao_expr} AS situacao "
                f"  FROM analytics {where}"
                ") t "
                "WHERE candidate_key IS NOT NULL AND candidate_key <> '' "
                "GROUP BY candidate_key "
                "ORDER BY votos DESC "
                "LIMIT ?"
            ),
            params + [n],
        )

        return [
            {
                "candidato": r[0] or "",
                "partido": r[1],
                "cargo": r[2],
                "uf": r[3],
                "votos": int(r[4] or 0),
                "situacao": r[5],
            }
            for r in rows
        ]

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

    def age_stats(
        self,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        somente_eleitos: bool = True,
    ) -> dict:
        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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

        rows = self._rows(
            (
                "SELECT faixa, COUNT(*) as value FROM ("
                "  SELECT CASE "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) <= 29 THEN '18-29' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) <= 39 THEN '30-39' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) <= 49 THEN '40-49' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) <= 59 THEN '50-59' "
                f"    WHEN TRY_CAST({col_idade} AS DOUBLE) <= 69 THEN '60-69' "
                "    ELSE '70+' END AS faixa "
                f"  FROM analytics {where} "
                f"  WHERE TRY_CAST({col_idade} AS DOUBLE) IS NOT NULL"
                ") t GROUP BY 1"
            ),
            params,
        )
        by_label = {str(r[0]): int(r[1]) for r in rows}
        labels = ["18-29", "30-39", "40-49", "50-59", "60-69", "70+"]
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

    def search_candidates(
        self,
        query: str,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        col_candidato = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if not col_candidato:
            return {"query": query, "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        q = query.strip().lower()
        if not q:
            return {"query": query, "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        pattern = f"%{q}%"
        filter_sql = (
            f"{where} {'AND' if where else 'WHERE'} LOWER(TRIM(CAST({col_candidato} AS VARCHAR))) LIKE ?"
        )
        base_params = params + [pattern]

        total = int(self._scalar(f"SELECT COUNT(*) FROM analytics {filter_sql}", base_params) or 0)
        if total == 0:
            return {"query": query, "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        offset = (page - 1) * page_size
        rows = self._rows(
            (
                "SELECT "
                f"CAST({col_candidato} AS VARCHAR) AS candidato, "
                f"{'CAST(' + col_partido + ' AS VARCHAR)' if col_partido else 'NULL'} AS partido, "
                f"{'CAST(' + col_cargo + ' AS VARCHAR)' if col_cargo else 'NULL'} AS cargo, "
                f"{'CAST(' + col_uf + ' AS VARCHAR)' if col_uf else 'NULL'} AS uf, "
                f"{'TRY_CAST(' + col_ano + ' AS BIGINT)' if col_ano else 'NULL'} AS ano, "
                f"{'COALESCE(TRY_CAST(' + col_votos + ' AS BIGINT), 0)' if col_votos else '0'} AS votos, "
                f"{'CAST(' + col_situacao + ' AS VARCHAR)' if col_situacao else 'NULL'} AS situacao, "
                f"CASE WHEN LOWER(TRIM(CAST({col_candidato} AS VARCHAR))) = ? THEN 3 "
                f"     WHEN LOWER(TRIM(CAST({col_candidato} AS VARCHAR))) LIKE ? THEN 2 ELSE 1 END AS score "
                f"FROM analytics {filter_sql} "
                "ORDER BY score DESC, votos DESC, candidato ASC LIMIT ? OFFSET ?"
            ),
            [q, f"{q}%"] + base_params + [page_size, offset],
        )

        total_pages = (total + page_size - 1) // page_size
        items = [
            {
                "candidato": r[0] or "",
                "partido": r[1],
                "cargo": r[2],
                "uf": r[3],
                "ano": int(r[4]) if r[4] is not None else None,
                "votos": int(r[5] or 0),
                "situacao": r[6],
            }
            for r in rows
        ]

        return {
            "query": query,
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
