from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


ELEITO_LABELS = {
    "ELEITO",
    "ELEITO POR QP",
    "ELEITO POR MEDIA",
    "MÉDIA",
}


@dataclass
class AnalyticsService:
    dataframe: pd.DataFrame
    default_top_n: int
    max_top_n: int

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
        return cls(df, default_top_n=default_top_n, max_top_n=max_top_n)

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
            return cls(df, default_top_n=default_top_n, max_top_n=max_top_n)
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
        uf: str | None = None,
        cargo: str | None = None,
        somente_eleitos: bool = False,
    ) -> pd.DataFrame:
        df = self.dataframe.copy()
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if ano is not None and col_ano:
            df = df[df[col_ano] == ano]
        if uf and col_uf:
            df = df[df[col_uf].astype(str).str.upper() == uf.upper()]
        if cargo and col_cargo:
            df = df[df[col_cargo].astype(str).str.lower() == cargo.lower()]
        if somente_eleitos and col_situacao:
            df = df[df[col_situacao].astype(str).str.upper().isin(ELEITO_LABELS)]
        return df

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

    def overview(self, ano: int | None = None, uf: str | None = None, cargo: str | None = None) -> dict:
        df = self._apply_filters(ano=ano, uf=uf, cargo=cargo)
        col_candidato = self._pick_col(["SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO"])
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        total_eleitos = None
        if col_situacao:
            total_eleitos = int(df[col_situacao].astype(str).str.upper().isin(ELEITO_LABELS).sum())

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
        uf: str | None = None,
        cargo: str | None = None,
        top_n: int | None = None,
    ) -> list[dict]:
        df = self._apply_filters(ano=ano, uf=uf, cargo=cargo)
        col_candidato = self._pick_col(["NM_CANDIDATO"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if not col_candidato or not col_votos:
            return []

        n = min(top_n or self.default_top_n, self.max_top_n)
        df = df.assign(_votos=pd.to_numeric(df[col_votos], errors="coerce").fillna(0))
        df = df.sort_values("_votos", ascending=False).head(n)

        out: list[dict] = []
        for _, row in df.iterrows():
            out.append(
                {
                    "candidato": str(row[col_candidato]),
                    "partido": str(row[col_partido]) if col_partido else None,
                    "cargo": str(row[col_cargo]) if col_cargo else None,
                    "uf": str(row[col_uf]) if col_uf else None,
                    "votos": int(row["_votos"]),
                    "situacao": str(row[col_situacao]) if col_situacao else None,
                }
            )
        return out

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

        df = self._apply_filters(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)
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

    def age_stats(
        self,
        ano: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        somente_eleitos: bool = True,
    ) -> dict:
        df = self._apply_filters(ano=ano, uf=uf, cargo=cargo, somente_eleitos=somente_eleitos)

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

        bins = [0, 29, 39, 49, 59, 69, 200]
        labels = ["18-29", "30-39", "40-49", "50-59", "60-69", "70+"]
        faixa = pd.cut(idade, bins=bins, labels=labels, include_lowest=True)
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

    def search_candidates(
        self,
        query: str,
        ano: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        df = self._apply_filters(ano=ano, uf=uf, cargo=cargo)

        col_candidato = self._pick_col(["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        col_partido = self._pick_col(["SG_PARTIDO"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
        col_uf = self._pick_col(["SG_UF"])
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votos = self._pick_col(
            ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        col_situacao = self._pick_col(["DS_SIT_TOT_TURNO"])

        if not col_candidato:
            return {"query": query, "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        q = query.strip().lower()
        if not q:
            return {"query": query, "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        names = df[col_candidato].fillna("").astype(str).str.strip()
        names_lc = names.str.lower()

        contains = names_lc.str.contains(q, regex=False)
        matched = contains
        if not matched.any():
            return {"query": query, "page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        ranked = df.loc[matched].copy()
        name_match_lc = names_lc.loc[matched]
        score = (
            (name_match_lc == q).astype(int) * 3
            + (name_match_lc.str.startswith(q)).astype(int) * 2
            + (name_match_lc.str.contains(q, regex=False)).astype(int)
        )
        if col_votos:
            votos = pd.to_numeric(ranked[col_votos], errors="coerce").fillna(0).astype("int64")
        else:
            votos = 0
        ranked = ranked.assign(_score=score, _votos=votos)

        ranked = ranked.sort_values(
            by=["_score", "_votos", col_candidato],
            ascending=[False, False, True],
        )

        total = int(len(ranked))
        total_pages = (total + page_size - 1) // page_size
        start = (page - 1) * page_size
        end = start + page_size
        sliced = ranked.iloc[start:end]

        items: list[dict] = []
        for _, row in sliced.iterrows():
            items.append(
                {
                    "candidato": str(row[col_candidato]),
                    "partido": str(row[col_partido]) if col_partido else None,
                    "cargo": str(row[col_cargo]) if col_cargo else None,
                    "uf": str(row[col_uf]) if col_uf else None,
                    "ano": int(row[col_ano]) if col_ano and pd.notna(row[col_ano]) else None,
                    "votos": int(row["_votos"]),
                    "situacao": str(row[col_situacao]) if col_situacao else None,
                }
            )

        return {
            "query": query,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "items": items,
        }
