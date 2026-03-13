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
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        somente_eleitos: bool = False,
    ) -> pd.DataFrame:
        df = self.dataframe.copy()
        col_ano = self._pick_col(["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_turno = self._pick_col(["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_uf = self._pick_col(["SG_UF"])
        col_cargo = self._pick_col(["DS_CARGO", "DS_CARGO_D"])
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
        if municipio and col_municipio:
            df = df[df[col_municipio].astype(str).str.upper().str.strip() == municipio.upper().strip()]
        if somente_eleitos and col_situacao:
            df = df[self._is_elected(df[col_situacao])]
        return df

    def _normalize_text(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).str.strip()

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
        municipio: str | None = None,
        top_n: int | None = None,
        page: int = 1,
        page_size: int | None = None,
    ) -> dict:
        df = self._apply_filters(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)
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
        df = self._apply_filters(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)

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
