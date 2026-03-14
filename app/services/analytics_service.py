from __future__ import annotations

from dataclasses import dataclass, field
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


@dataclass
class AnalyticsService:
    dataframe: pd.DataFrame
    default_top_n: int
    max_top_n: int
    _source_path: Path | None = field(default=None, init=False, repr=False)
    _official_prefeito_totals_cache: dict[int, dict[str, int]] = field(default_factory=dict, init=False, repr=False)

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

    def _normalize_ascii_upper(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKD", text)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return ascii_text.upper().strip()

    def _normalize_municipio_key(self, text: str | None) -> str:
        key = self._normalize_ascii_upper(text or "")
        if not key:
            return ""
        return MUNICIPIO_ALIAS_MAP.get(key, key)

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
