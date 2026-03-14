from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock

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
    "PSD": "centro",
    "SOLIDARIEDADE": "centro",
    "PROS": "centro",
    "DEM": "centro",
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


@dataclass
class DuckDBAnalyticsService:
    conn: duckdb.DuckDBPyConnection
    default_top_n: int
    max_top_n: int
    _query_lock: Lock
    _columns: set[str]
    _source_path: Path | None = field(default=None, init=False, repr=False)
    _official_prefeito_totals_cache: dict[int, dict[str, int]] = field(default_factory=dict, init=False, repr=False)

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
        service = cls(
            conn=conn,
            default_top_n=default_top_n,
            max_top_n=max_top_n,
            _query_lock=Lock(),
            _columns=cols,
        )
        service._source_path = path
        return service

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
        return text.upper() if uppercase else text

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
        where, params = self._where(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
        offset = (page - 1) * effective_page_size

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
        grouped_sql = (
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
            "GROUP BY candidate_key"
        )
        total = int(self._scalar(f"SELECT COUNT(*) FROM ({grouped_sql}) g", params) or 0)
        total_pages = (total + effective_page_size - 1) // effective_page_size

        rows = self._rows(
            (
                "SELECT candidato, partido, cargo, uf, votos, situacao "
                f"FROM ({grouped_sql}) g "
                "ORDER BY votos DESC "
                "LIMIT ? OFFSET ?"
            ),
            params + [effective_page_size, offset],
        )

        return {
            "top_n": effective_page_size,
            "page": page,
            "page_size": effective_page_size,
            "total": total,
            "total_pages": total_pages,
            "items": [
                {
                    "candidato": r[0] or "",
                    "partido": r[1],
                    "cargo": r[2],
                    "uf": r[3],
                    "votos": int(r[4] or 0),
                    "situacao": r[5],
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
