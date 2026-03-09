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
        uf: str | None = None,
        cargo: str | None = None,
        somente_eleitos: bool = False,
    ) -> tuple[str, list]:
        clauses = []
        params: list = []

        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if ano is not None and col_ano:
            clauses.append(f"TRY_CAST({col_ano} AS BIGINT) = ?")
            params.append(ano)
        if uf and col_uf:
            clauses.append(f"UPPER(TRIM(CAST({col_uf} AS VARCHAR))) = ?")
            params.append(uf.upper())
        if cargo and col_cargo:
            clauses.append(f"LOWER(TRIM(CAST({col_cargo} AS VARCHAR))) = ?")
            params.append(cargo.lower())
        if somente_eleitos and col_situacao:
            labels = sorted(ELEITO_LABELS)
            placeholders = ", ".join(["?"] * len(labels))
            clauses.append(f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) IN ({placeholders})")
            params.extend(labels)

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

    def overview(self, ano: int | None = None, uf: str | None = None, cargo: str | None = None) -> dict:
        where, params = self._where(ano=ano, uf=uf, cargo=cargo)
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
            labels = sorted(ELEITO_LABELS)
            placeholders = ", ".join(["?"] * len(labels))
            total_eleitos = int(
                self._scalar(
                    (
                        "SELECT COUNT(*) FROM analytics "
                        f"{where} {'AND' if where else 'WHERE'} "
                        f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) IN ({placeholders})"
                    ),
                    params + labels,
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
        uf: str | None = None,
        cargo: str | None = None,
        top_n: int | None = None,
    ) -> list[dict]:
        col_candidato = self._pick_col(["NM_CANDIDATO"])
        col_votos = self._pick_col(["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_candidato or not col_votos:
            return []

        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])
        where, params = self._where(ano=ano, uf=uf, cargo=cargo)
        n = min(top_n or self.default_top_n, self.max_top_n)

        rows = self._rows(
            (
                "SELECT "
                f"CAST({col_candidato} AS VARCHAR) AS candidato, "
                f"{'CAST(' + col_partido + ' AS VARCHAR)' if col_partido else 'NULL'} AS partido, "
                f"{'CAST(' + col_cargo + ' AS VARCHAR)' if col_cargo else 'NULL'} AS cargo, "
                f"{'CAST(' + col_uf + ' AS VARCHAR)' if col_uf else 'NULL'} AS uf, "
                f"COALESCE(TRY_CAST({col_votos} AS BIGINT), 0) AS votos, "
                f"{'CAST(' + col_situacao + ' AS VARCHAR)' if col_situacao else 'NULL'} AS situacao "
                f"FROM analytics {where} ORDER BY votos DESC LIMIT ?"
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
        uf: str | None = None,
        cargo: str | None = None,
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

        where, params = self._where(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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
        uf: str | None = None,
        cargo: str | None = None,
        somente_eleitos: bool = True,
    ) -> dict:
        where, params = self._where(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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
            labels = sorted(ELEITO_LABELS)
            label_list = ", ".join(f"'{lbl}'" for lbl in labels)
            return (
                "SUM(CASE WHEN "
                f"UPPER(TRIM(CAST({col_situacao} AS VARCHAR))) IN ({label_list}) "
                "THEN 1 ELSE 0 END)"
            )
        return None

    def time_series(
        self,
        metric: str = "votos_nominais",
        uf: str | None = None,
        cargo: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        metric_sql = self._metric_sql(metric)
        if not col_ano or not metric_sql:
            return []

        where, params = self._where(ano=None, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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
        uf: str | None = None,
        cargo: str | None = None,
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

        where, params = self._where(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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
        cargo: str | None = None,
        somente_eleitos: bool = False,
    ) -> list[dict]:
        target_col = self._pick_col(["SG_UF"])
        metric_sql = self._metric_sql(metric)
        if not target_col or not metric_sql:
            return []

        where, params = self._where(ano=ano, uf=None, cargo=cargo, somente_eleitos=somente_eleitos)
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
        uf: str | None = None,
        cargo: str | None = None,
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

        where, params = self._where(ano=ano, uf=uf, cargo=cargo)
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
