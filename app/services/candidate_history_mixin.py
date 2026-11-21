from __future__ import annotations

import hashlib
import re
from typing import Any
import unicodedata

import pandas as pd


class CandidateHistoryMixin:
    def _candidate_compact_projection_columns(self) -> list[str]:
        return [
            "ANO_ELEICAO",
            "NR_ANO_ELEICAO",
            "NR_TURNO",
            "CD_TURNO",
            "DS_TURNO",
            "QT_VOTOS_NOMINAIS_VALIDOS",
            "NR_VOTACAO_NOMINAL",
            "QT_VOTOS_NOMINAIS",
            "DS_CARGO",
            "DS_CARGO_D",
            "SG_UF",
            "NM_UE",
            "NM_MUNICIPIO",
            "SG_PARTIDO",
            "NR_CANDIDATO",
            "NR_CPF_CANDIDATO",
            "HISTORICO_CANDIDATURA_ID",
            "SQ_CANDIDATO",
            "NM_CANDIDATO",
            "NM_URNA_CANDIDATO",
            "DS_SIT_TOT_TURNO",
            "DT_NASCIMENTO",
        ]

    def _candidate_compact_frame(self) -> pd.DataFrame:
        cache = getattr(self, "_candidate_compact_frame_cache", None)
        if cache is not None:
            return cache

        base_df = self._load_history_rows(columns=self._candidate_compact_projection_columns())
        prepared = self._prepare_candidate_compact_rows(base_df)
        setattr(self, "_candidate_compact_frame_cache", prepared)
        return prepared

    def _candidate_history_context_totals_frame(self) -> pd.DataFrame:
        cache = getattr(self, "_candidate_history_context_totals_cache", None)
        if cache is not None:
            return cache

        base_df = self._candidate_compact_frame()
        if base_df.empty:
            empty = base_df.iloc[0:0].copy()
            setattr(self, "_candidate_history_context_totals_cache", empty)
            return empty

        context_cols = ["_year", "_office", "_context_state", "_context_municipality", "_round"]
        totals = (
            base_df.groupby(context_cols, as_index=False)
            .agg(total_votes=("_votes", "sum"))
            .sort_values(context_cols, ascending=[False, True, True, True, False], kind="stable")
            .reset_index(drop=True)
        )
        setattr(self, "_candidate_history_context_totals_cache", totals)
        return totals

    def _prepare_candidate_compact_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        prepared_cols = {
            "_year",
            "_round",
            "_votes",
            "_office",
            "_state",
            "_municipality",
            "_party",
            "_number",
            "_candidate_id",
            "_candidate_key",
            "_source_id",
            "_name_display",
            "_name_norm",
            "_person_id",
            "_scope",
            "_context_state",
            "_context_municipality",
        }
        if prepared_cols.issubset(df.columns):
            return df

        col_year = self._select_history_col(df, ["_year", "ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_round = self._select_history_col(df, ["_round", "NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_votes = self._select_history_col(df, ["_votes", "QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_cargo = self._select_history_col(df, ["_office", "DS_CARGO", "DS_CARGO_D"])
        col_state = self._select_history_col(df, ["_state", "SG_UF"])
        col_municipio = self._select_history_col(df, ["_municipality", "NM_UE", "NM_MUNICIPIO"])
        col_party = self._select_history_col(df, ["_party", "SG_PARTIDO"])
        col_number = self._select_history_col(df, ["_number", "NR_CANDIDATO"])
        col_candidate_id = self._select_history_col(df, ["_candidate_id", "SQ_CANDIDATO", "NR_CANDIDATO"])
        col_source_id = self._select_history_col(df, ["_source_id", "SQ_CANDIDATO", "NR_CANDIDATO"])
        col_candidato = self._select_history_col(df, ["_name_display", "NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        col_cpf = self._select_history_col(df, ["NR_CPF_CANDIDATO"])

        year_series = pd.to_numeric(df[col_year], errors="coerce") if col_year else pd.Series([pd.NA] * len(df), index=df.index)
        round_series = (
            pd.to_numeric(df[col_round], errors="coerce").fillna(0).astype(int)
            if col_round == "_round"
            else (self._extract_turno(df[col_round]) if col_round else pd.Series([0] * len(df), index=df.index))
        )
        votes_series = (
            pd.to_numeric(df[col_votes], errors="coerce").fillna(0)
            if col_votes == "_votes"
            else (pd.to_numeric(df[col_votes], errors="coerce").fillna(0) if col_votes else pd.Series([0] * len(df), index=df.index))
        )
        office_series = self._text_series(df[col_cargo]) if col_cargo else pd.Series([""] * len(df), index=df.index)
        state_series = self._text_series(df[col_state]).str.upper() if col_state else pd.Series([""] * len(df), index=df.index)
        municipality_series = self._text_series(df[col_municipio]) if col_municipio else pd.Series([""] * len(df), index=df.index)
        party_series = self._text_series(df[col_party]).str.upper().str.strip() if col_party else pd.Series([""] * len(df), index=df.index)
        number_series = self._text_series(df[col_number]) if col_number else pd.Series([""] * len(df), index=df.index)
        candidate_id_series = self._text_series(df[col_candidate_id]) if col_candidate_id else pd.Series([""] * len(df), index=df.index)
        source_id_series = self._text_series(df[col_source_id]) if col_source_id else pd.Series([""] * len(df), index=df.index)
        name_series = self._candidate_display_name_series(df) if col_candidato else pd.Series([""] * len(df), index=df.index)
        if not col_candidato:
            name_series = pd.Series([""] * len(df), index=df.index)
        if col_cpf:
            cpf_series = self._cpf_series(df[col_cpf])
        else:
            cpf_series = pd.Series([""] * len(df), index=df.index)

        name_norm = name_series.map(lambda value: self._normalize_value(value, uppercase=True).lower())
        col_urna_name = self._select_history_col(df, ["NM_URNA_CANDIDATO"])
        urna_series = (
            self._text_series(df[col_urna_name]) if col_urna_name else pd.Series([""] * len(df), index=df.index)
        )
        urna_norm = urna_series.map(lambda value: self._normalize_value(value, uppercase=True).lower())
        registro_series = (
            self._text_series(df["NM_CANDIDATO"]) if "NM_CANDIDATO" in df.columns else pd.Series([""] * len(df), index=df.index)
        )
        registro_norm = registro_series.map(lambda value: self._normalize_value(value, uppercase=True).lower())
        candidate_key = candidate_id_series.replace("", pd.NA)
        if col_candidate_id is None:
            candidate_key = name_norm.replace("", pd.NA)
        person_id_series = self._person_id_series(df).replace("", pd.NA)

        prepared = df.assign(
            _year=year_series,
            _round=round_series,
            _votes=votes_series,
            _office=office_series,
            _state=state_series,
            _municipality=municipality_series,
            _party=party_series,
            _number=number_series.replace("", pd.NA),
            _candidate_id=candidate_id_series.replace("", pd.NA),
            _candidate_key=candidate_key,
            _source_id=source_id_series.replace("", pd.NA),
            _name_display=name_series.replace("", pd.NA),
            _name_norm=name_norm.replace("", pd.NA),
            _urna_display=urna_series.replace("", pd.NA),
            _urna_norm=urna_norm.replace("", pd.NA),
            _registro_norm=registro_norm.replace("", pd.NA),
            _cpf=cpf_series.replace("", pd.NA),
            _person_id=person_id_series,
        )
        prepared = prepared.dropna(subset=["_year"]).copy()
        if prepared.empty:
            return prepared
        prepared = prepared.assign(
            _scope=prepared["_office"].apply(self._cargo_scope),  # type: ignore[attr-defined]
            _context_state=prepared["_state"].where(prepared["_office"].apply(self._cargo_scope) != "nacional", ""),  # type: ignore[attr-defined]
            _context_municipality=prepared["_municipality"].where(prepared["_office"].apply(self._cargo_scope) == "municipio", ""),  # type: ignore[attr-defined]
        )
        return prepared

    def _identity_text(self, value: object) -> str:
        if value is None or pd.isna(value):
            return ""
        text = str(value).strip()
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
        return " ".join(ascii_text.upper().split())

    def _identity_series(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).map(self._identity_text)

    def _cpf_text(self, value: object) -> str:
        if value is None or pd.isna(value):
            return ""
        raw = str(value).strip()
        if raw.startswith("-") or raw.upper() in {"#NULO", "#NE", "NÃO DIVULGÁVEL", "NAO DIVULGAVEL"}:
            return ""
        raw = re.sub(r"\.0$", "", raw)
        text = re.sub(r"\D", "", raw)
        text = text.strip()
        return text.zfill(11) if text and len(text) <= 11 else text

    def _cpf_series(self, series: pd.Series) -> pd.Series:
        return series.fillna("").astype(str).map(self._cpf_text)

    def _person_identity_name_series(self, df: pd.DataFrame) -> pd.Series:
        col_name = self._select_history_col(df, ["NM_CANDIDATO"])
        col_urna = self._select_history_col(df, ["NM_URNA_CANDIDATO"])
        if not col_name and not col_urna:
            return pd.Series([""] * len(df), index=df.index)

        if col_name:
            preferred = self._identity_series(df[col_name])
        else:
            preferred = pd.Series([""] * len(df), index=df.index)
        if col_urna:
            urna = self._identity_series(df[col_urna])
            preferred = preferred.where(preferred.str.len() >= urna.str.len(), urna)
            preferred = preferred.where(preferred != "", urna)
        return preferred.fillna("")

    def _candidate_display_name_series(self, df: pd.DataFrame) -> pd.Series:
        col_name = self._select_history_col(df, ["NM_CANDIDATO"])
        col_urna = self._select_history_col(df, ["NM_URNA_CANDIDATO"])
        if not col_name and not col_urna:
            return pd.Series([""] * len(df), index=df.index)

        if col_name:
            preferred = self._text_series(df[col_name])
        else:
            preferred = pd.Series([""] * len(df), index=df.index)
        if col_urna:
            urna = self._text_series(df[col_urna])
            preferred = preferred.where(preferred.str.len() >= urna.str.len(), urna)
            preferred = preferred.where(preferred != "", urna)
        return preferred.fillna("")

    def _person_identity_birth_series(self, df: pd.DataFrame) -> pd.Series:
        col_birth = self._select_history_col(df, ["DT_NASCIMENTO"])
        if not col_birth:
            return pd.Series([""] * len(df), index=df.index)
        birth_series = pd.to_datetime(
            df[col_birth], errors="coerce", format="mixed", dayfirst=True
        )
        return birth_series.dt.strftime("%Y-%m-%d").fillna("")

    def _person_identity_signature_series(self, df: pd.DataFrame) -> pd.Series:
        col_cpf = self._select_history_col(df, ["NR_CPF_CANDIDATO"])
        col_history = self._select_history_col(df, ["HISTORICO_CANDIDATURA_ID"])
        name_series = self._person_identity_name_series(df)
        birth_series = self._person_identity_birth_series(df)
        signature = name_series.where(name_series != "", birth_series)
        both_mask = (name_series != "") & (birth_series != "")
        signature = signature.where(~both_mask, name_series + "|" + birth_series)
        if col_cpf:
            cpf_series = self._cpf_series(df[col_cpf])
            signature = cpf_series.where(cpf_series != "", signature)
        if col_history:
            history_series = self._text_series(df[col_history])
            signature = history_series.where(history_series != "", signature)
        return signature.fillna("")

    def _cpf_person_identity_signature_series(self, df: pd.DataFrame) -> pd.Series:
        col_cpf = self._select_history_col(df, ["NR_CPF_CANDIDATO"])
        if not col_cpf:
            return pd.Series([""] * len(df), index=df.index)
        return self._cpf_series(df[col_cpf]).fillna("")

    def _person_id_series(self, df: pd.DataFrame) -> pd.Series:
        signatures = self._person_identity_signature_series(df)
        return signatures.map(lambda value: f"person:{hashlib.sha1(value.encode('utf-8')).hexdigest()}")

    def _stable_person_id(self, df: pd.DataFrame) -> str:
        signatures = self._person_identity_signature_series(df)
        values = signatures.replace("", pd.NA).dropna()
        if values.empty:
            signature = ""
        else:
            counts = values.value_counts()
            top_signatures = sorted(counts[counts == counts.max()].index.tolist())
            signature = str(top_signatures[0])
        return f"person:{hashlib.sha1(signature.encode('utf-8')).hexdigest()}"

    def _cpf_stable_person_id(self, df: pd.DataFrame) -> str:
        signatures = self._cpf_person_identity_signature_series(df)
        values = signatures.replace("", pd.NA).dropna()
        if values.empty:
            return ""
        counts = values.value_counts()
        top_signatures = sorted(counts[counts == counts.max()].index.tolist())
        signature = str(top_signatures[0])
        return f"person:{hashlib.sha1(signature.encode('utf-8')).hexdigest()}"

    def _candidate_mask(self, df: pd.DataFrame, candidate_id: str, candidate_cpf: str | None = None) -> pd.Series:
        candidate_cpf_norm = self._cpf_text(candidate_cpf)
        if candidate_cpf_norm and "NR_CPF_CANDIDATO" in df.columns:
            return (self._cpf_series(df["NR_CPF_CANDIDATO"]) == candidate_cpf_norm).fillna(False)

        candidate_norm = self._identity_text(candidate_id)
        if not candidate_norm:
            return pd.Series([False] * len(df), index=df.index)

        # Prioridade SQ > numero > CPF: o primeiro criterio que casa alguma linha vence. Juntar os
        # tres misturava pessoas (o SQ 23 de 2006 com todo candidato de numero 23).
        empty = pd.Series([False] * len(df), index=df.index)
        for col in ["SQ_CANDIDATO", "NR_CANDIDATO", "NR_CPF_CANDIDATO"]:
            if col not in df.columns:
                continue
            if col == "NR_CPF_CANDIDATO":
                mask = self._cpf_series(df[col]) == self._cpf_text(candidate_id)
            else:
                mask = self._identity_series(df[col]) == candidate_norm
            mask = mask.fillna(False)
            if mask.any():
                return mask
        return empty

    def _candidate_source_id(self, df: pd.DataFrame) -> str | None:
        for col in ["SQ_CANDIDATO", "NR_CANDIDATO"]:
            if col not in df.columns:
                continue
            values = df[col].fillna("").astype(str).str.strip()
            values = values[values != ""]
            if not values.empty:
                return str(values.iloc[0])
        return None

    def _candidate_cpf(self, df: pd.DataFrame) -> str | None:
        col_cpf = self._select_history_col(df, ["NR_CPF_CANDIDATO"])
        if not col_cpf:
            return None
        values = self._cpf_series(df[col_cpf]).replace("", pd.NA).dropna()
        if values.empty:
            return None
        counts = values.value_counts()
        top_cpfs = sorted(counts[counts == counts.max()].index.tolist())
        return str(top_cpfs[0])

    def _candidate_person_identity(self, df: pd.DataFrame) -> dict[str, str | None]:
        if df.empty:
            return {
                "canonical_candidate_id": None,
                "person_id": None,
            }

        person_id = self._stable_person_id(df)
        return {
            "canonical_candidate_id": person_id or None,
            "person_id": person_id or None,
        }

    def _candidate_cpf_identity(self, df: pd.DataFrame, candidate_cpf: str | None = None) -> dict[str, str | None]:
        if df.empty:
            return {
                "nr_cpf_candidato": self._cpf_text(candidate_cpf) or None,
                "canonical_candidate_id": None,
                "person_id": None,
            }

        cpf_value = self._candidate_cpf(df) or self._cpf_text(candidate_cpf)
        if not cpf_value:
            return {
                "nr_cpf_candidato": None,
                "canonical_candidate_id": None,
                "person_id": None,
            }

        person_id = f"person:{hashlib.sha1(cpf_value.encode('utf-8')).hexdigest()}"
        return {
            "nr_cpf_candidato": cpf_value,
            "canonical_candidate_id": person_id or None,
            "person_id": person_id or None,
        }

    def _candidate_identity_payload(self, df: pd.DataFrame) -> dict[str, str | None]:
        identity = self._candidate_person_identity(df)
        source_id = self._candidate_source_id(df)
        candidate_cpf = self._candidate_cpf(df)
        return {
            "source_id": source_id,
            "nr_cpf_candidato": candidate_cpf,
            "canonical_candidate_id": identity["canonical_candidate_id"],
            "person_id": identity["person_id"],
        }

    def _collision_person_key_series(self, df: pd.DataFrame) -> pd.Series:
        """Chave de pessoa para separar homonimos de `SQ_CANDIDATO` (2000-2008 repetem o SQ).

        Historico oficial (`tse-history:`) > CPF > nome+nascimento. O id `tse-candidate:` e de uma
        candidatura so, entao nao serve de chave: a mesma pessoa tem um por ano.
        """
        history = (
            self._text_series(df["HISTORICO_CANDIDATURA_ID"])
            if "HISTORICO_CANDIDATURA_ID" in df.columns
            else pd.Series([""] * len(df), index=df.index)
        )
        history = history.where(history.str.startswith("tse-history:"), "")
        cpf = (
            self._cpf_series(df["NR_CPF_CANDIDATO"]) if "NR_CPF_CANDIDATO" in df.columns else pd.Series([""] * len(df), index=df.index)
        )
        name = self._person_identity_name_series(df)
        birth = self._person_identity_birth_series(df)
        by_name = "name:" + name + "|" + birth
        by_cpf = "cpf:" + cpf
        key = by_name.where(cpf == "", by_cpf)
        return history.where(history != "", key).fillna("")

    def _collision_scope_series(self, df: pd.DataFrame) -> pd.Series:
        """Escopo da candidatura no ano: cargo + UF (cargo estadual) + municipio (cargo municipal)."""
        col_office = self._select_history_col(df, ["DS_CARGO", "DS_CARGO_D", "_office"])
        col_state = self._select_history_col(df, ["SG_UF", "_state"])
        col_mun = self._select_history_col(df, ["CD_MUNICIPIO", "NM_UE", "NM_MUNICIPIO", "_municipality"])
        blank = pd.Series([""] * len(df), index=df.index)
        office = self._identity_series(df[col_office]) if col_office else blank
        state = self._identity_series(df[col_state]) if col_state else blank
        mun = self._identity_series(df[col_mun]) if col_mun else blank
        level = office.map(self._cargo_scope)  # type: ignore[attr-defined]
        state = state.where(level != "nacional", "")
        mun = mun.where(level == "municipio", "")
        return office + "|" + state + "|" + mun

    def _single_candidate_rows(
        self,
        rows: pd.DataFrame,
        *,
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        municipio: str | None = None,
    ) -> pd.DataFrame:
        """Reduz as linhas de um `candidate_id` a UMA pessoa e, em cada ano, a UMA candidatura.

        Em 2000-2008 o `SQ_CANDIDATO` e um sequencial pequeno repetido entre anos, UFs e (2000/2004)
        municipios. Sem isso, o historico, o CPF e a foto de um homonimo de SQ vazavam para a pessoa
        consultada (Marta Suplicy 2000 recebia o historico de um deputado do RJ). Escolhe, nesta ordem:
        a pessoa que tem linhas no recorte pedido e depois a de maior votacao; desempate pela chave.
        """
        if rows is None or rows.empty:
            return rows
        keys = self._collision_person_key_series(rows)
        if keys.nunique() > 1:
            wanted = pd.Series(True, index=rows.index)
            col_year = self._select_history_col(rows, ["ANO_ELEICAO", "NR_ANO_ELEICAO", "_year"])
            if year is not None and col_year:
                wanted &= pd.to_numeric(rows[col_year], errors="coerce") == int(year)
            if state and "SG_UF" in rows.columns:
                wanted &= self._identity_series(rows["SG_UF"]) == self._identity_text(state)
            if office:
                col_office = self._select_history_col(rows, ["DS_CARGO", "DS_CARGO_D", "_office"])
                if col_office:
                    wanted &= self._identity_series(rows[col_office]) == self._identity_text(office)
            if municipio:
                col_mun = self._select_history_col(rows, ["NM_UE", "NM_MUNICIPIO", "_municipality"])
                if col_mun:
                    wanted &= self._identity_series(rows[col_mun]) == self._identity_text(municipio)
            col_votes = self._select_history_col(
                rows, ["_votes", "QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
            )
            votes = pd.to_numeric(rows[col_votes], errors="coerce").fillna(0) if col_votes else pd.Series(0.0, index=rows.index)
            ranking = pd.DataFrame({"key": keys, "wanted": wanted.astype(int), "votes": votes})
            ranked = (
                ranking.groupby("key", as_index=False)
                .agg(wanted=("wanted", "max"), votes=("votes", "sum"))
                .sort_values(["wanted", "votes", "key"], ascending=[False, False, True], kind="stable")
            )
            rows = rows[keys == str(ranked.iloc[0]["key"])]

        scopes = self._collision_scope_series(rows)
        col_year = self._select_history_col(rows, ["ANO_ELEICAO", "NR_ANO_ELEICAO", "_year"])
        if col_year is None:
            return rows.copy()
        years = pd.to_numeric(rows[col_year], errors="coerce")
        if (pd.DataFrame({"y": years, "s": scopes}).drop_duplicates().groupby("y")["s"].nunique() <= 1).all():
            return rows.copy()
        col_votes = self._select_history_col(
            rows, ["_votes", "QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"]
        )
        votes = pd.to_numeric(rows[col_votes], errors="coerce").fillna(0) if col_votes else pd.Series(0.0, index=rows.index)
        office_text = self._identity_text(office) if office else ""
        state_text = self._identity_text(state) if state else ""
        by_scope = (
            pd.DataFrame({"y": years, "s": scopes, "v": votes})
            .groupby(["y", "s"], as_index=False)["v"]
            .sum()
        )
        by_scope["wanted"] = by_scope["s"].map(
            lambda scope: int(
                (not office_text or scope.split("|")[0] == office_text)
                and (not state_text or scope.split("|")[1] in {"", state_text})
            )
        )
        best = (
            by_scope.sort_values(["y", "wanted", "v", "s"], ascending=[True, False, False, True], kind="stable")
            .drop_duplicates("y")
        )
        chosen = set(zip(best["y"], best["s"]))
        keep = [(y, s) in chosen for y, s in zip(years, scopes)]
        return rows[keep].copy()

    def _same_person_photo_fallback(
        self,
        *,
        candidate_id: str,
        reference_rows: pd.DataFrame,
        year: int | None,
        state: str | None,
        office: str | None,
        municipio: str | None,
        live_photo_url: Any = None,
    ) -> str | None:
        """Foto de OUTRA eleicao da mesma pessoa, da mais recente para a mais antiga.

        So vale historico comprovado da pessoa (historico oficial, CPF ou nome+nascimento) e nome
        compativel; nunca o `SQ_CANDIDATO` sozinho, que em 2000-2008 pertence a varias pessoas.
        """
        loader = getattr(self, "_candidate_history_targeted_rows", None)
        try:
            if callable(loader):
                person_rows, _ = loader(candidate_id=candidate_id, state=state, office=office, year=year, municipio=municipio)
            else:
                person_rows, _ = self._historical_candidate_rows(
                    candidate_id=candidate_id,
                    state=state,
                    office=office,
                    all_rows=self._prepare_candidate_compact_rows(self._candidate_compact_frame()),
                    year=year,
                    municipio=municipio,
                )  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - foto e opcional: nunca derruba o resumo
            return None
        if person_rows is None or person_rows.empty or reference_rows is None or reference_rows.empty:
            return None

        def tokens(value: object) -> tuple[str, str]:
            parts = self._identity_text(value).split()
            return (parts[0], parts[-1]) if parts else ("", "")

        reference_names = {tokens(name) for name in self._person_identity_name_series(reference_rows) if name}
        names = self._person_identity_name_series(person_rows)
        compatible = names.map(lambda name: bool(name) and tokens(name) in reference_names)
        person_rows = person_rows[compatible.values]
        col_year = self._select_history_col(person_rows, ["ANO_ELEICAO", "NR_ANO_ELEICAO", "_year"])
        if person_rows.empty or not col_year:
            return None
        years = pd.to_numeric(person_rows[col_year], errors="coerce")
        for other_year in sorted({int(value) for value in years.dropna().tolist()}, reverse=True):
            if year is not None and other_year == int(year):
                continue
            rows_year = self._single_candidate_rows(person_rows[(years == other_year).values])
            if rows_year.empty:
                continue
            if callable(live_photo_url):
                col_office = self._select_history_col(rows_year, ["DS_CARGO", "DS_CARGO_D", "_office"])
                col_uf = self._select_history_col(rows_year, ["SG_UF", "_state"])
                col_sq = self._select_history_col(rows_year, ["SQ_CANDIDATO", "_source_id"])
                if col_office and col_sq:
                    seen = rows_year[[c for c in (col_office, col_uf, col_sq) if c]].drop_duplicates().head(20)
                    for record in seen.to_dict("records"):
                        url = live_photo_url(
                            year=other_year,
                            office=str(record.get(col_office) or "") or None,
                            state=str(record.get(col_uf) or "") or None if col_uf else None,
                            candidate_sq=str(record.get(col_sq) or "") or None,
                        )
                        if url:
                            return url
            url = self._candidate_photo_url(rows_year, other_year)  # type: ignore[attr-defined]
            if url:
                return url
        return None

    def _historical_candidate_rows(
        self,
        candidate_id: str,
        candidate_cpf: str | None = None,
        state: str | None = None,
        office: str | None = None,
        all_rows: pd.DataFrame | None = None,
        use_cpf_identity: bool = False,
        year: int | None = None,
        municipio: str | None = None,
    ) -> tuple[pd.DataFrame, dict[str, str | None]]:
        projection_cols = self._candidate_history_projection_columns()
        scoped_source = all_rows if all_rows is not None else self._load_history_rows(columns=projection_cols)
        scoped = self._scope_history_rows(scoped_source, state=state, office=office) if all_rows is not None else scoped_source
        seed_rows = scoped[self._candidate_mask(scoped, candidate_id, candidate_cpf=candidate_cpf)].copy()
        if seed_rows.empty and (state or office):
            history_rows = all_rows if all_rows is not None else self._load_history_rows(columns=projection_cols)
            seed_rows = history_rows[self._candidate_mask(history_rows, candidate_id, candidate_cpf=candidate_cpf)].copy()
        # SQ repetido (2000-2008): um unico homonimo manda no CPF, no historico e na foto.
        seed_rows = self._single_candidate_rows(seed_rows, year=year, state=state, office=office, municipio=municipio)
        if seed_rows.empty:
            empty_base = all_rows if all_rows is not None else self._load_history_rows(columns=projection_cols)
            return empty_base.iloc[0:0].copy(), {
                "source_id": None,
                "nr_cpf_candidato": None,
                "canonical_candidate_id": None,
                "person_id": None,
            }

        all_rows = all_rows if all_rows is not None else self._load_history_rows(columns=projection_cols)
        # Mesma identidade das demais rotas: com ou sem CPF, o person_id e o da pessoa
        # (historico oficial > CPF > nome+nascimento). O CPF so amplia a busca.
        identity = self._candidate_identity_payload(seed_rows)
        person_id = identity["person_id"]
        if person_id and "_person_id" in all_rows.columns:
            person_mask = all_rows["_person_id"].fillna("").astype(str) == str(person_id)
        elif person_id:
            person_mask = self._person_id_series(all_rows) == str(person_id)
        else:
            person_mask = pd.Series([False] * len(all_rows), index=all_rows.index)
        cpf_value = self._cpf_text(candidate_cpf) if use_cpf_identity else ""
        cpf_value = cpf_value or identity.get("nr_cpf_candidato") or ""
        if cpf_value:
            identity["nr_cpf_candidato"] = cpf_value
            if "_cpf" in all_rows.columns:
                person_mask = person_mask | (all_rows["_cpf"].fillna("").astype(str) == cpf_value)
            elif "NR_CPF_CANDIDATO" in all_rows.columns:
                person_mask = person_mask | (self._cpf_series(all_rows["NR_CPF_CANDIDATO"]) == cpf_value)
        candidate_rows = all_rows[person_mask.fillna(False)].copy()
        if candidate_rows.empty:
            candidate_rows = seed_rows.copy()

        return candidate_rows, identity

    def _projection_columns(self, columns: list[str] | None) -> list[str] | None:
        if not columns:
            return None

        requested: list[str] = []
        for col in dict.fromkeys(columns):
            if hasattr(self, "_has") and getattr(self, "_has")(col):  # type: ignore[attr-defined]
                requested.append(col)
                continue
            if hasattr(self, "dataframe") and getattr(self, "dataframe") is not None:
                df = getattr(self, "dataframe")
                if col in df.columns:
                    requested.append(col)
                    continue
                if col.upper() in {str(existing).upper() for existing in df.columns}:
                    requested.append(next(existing for existing in df.columns if str(existing).upper() == col.upper()))
                    continue
            if hasattr(self, "_columns") and getattr(self, "_columns") is not None:
                schema_cols = {str(existing).upper() for existing in getattr(self, "_columns")}
                if col.upper() in schema_cols:
                    requested.append(col)

        return requested or None

    def _candidate_history_projection_columns(self) -> list[str]:
        return self._candidate_compact_projection_columns()

    def _load_history_rows(
        self,
        *,
        ano: int | None = None,
        turno: int | None = None,
        uf: str | None = None,
        cargo: str | None = None,
        municipio: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        requested_columns = None
        if columns:
            requested_columns = self._projection_columns(columns)
        if hasattr(self, "_filtered_df"):
            return self._filtered_df(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio, columns=requested_columns)  # type: ignore[attr-defined]
        if hasattr(self, "_apply_filters"):
            df = self._apply_filters(ano=ano, turno=turno, uf=uf, cargo=cargo, municipio=municipio)  # type: ignore[attr-defined]
            if requested_columns:
                available = [col for col in requested_columns if col in df.columns]
                if available:
                    return df.loc[:, available].copy()
            return df
        raise AttributeError("Service does not expose a compatible history loader")

    def _scope_history_rows(
        self,
        df: pd.DataFrame,
        *,
        state: str | None = None,
        office: str | None = None,
    ) -> pd.DataFrame:
        scoped = df
        if scoped.empty:
            return scoped

        normalized_state = (state or "").strip().upper() or None
        if normalized_state in {"BR", "BRASIL"}:
            normalized_state = None
        if normalized_state and "SG_UF" in scoped.columns:
            scoped = scoped[scoped["SG_UF"].fillna("").astype(str).str.upper() == normalized_state]
        if office:
            office_norm = (office or "").strip().lower()
            for col in ("DS_CARGO", "DS_CARGO_D"):
                if col in scoped.columns:
                    scoped = scoped[scoped[col].fillna("").astype(str).str.strip().str.lower() == office_norm]
                    break
        return scoped

    def _select_history_col(self, df: pd.DataFrame, options: list[str]) -> str | None:
        for col in options:
            if col in df.columns:
                return col
        return None

    def _text_series(self, series: pd.Series) -> pd.Series:
        if hasattr(self, "_normalize_text"):
            return self._normalize_text(series)  # type: ignore[attr-defined]
        return series.fillna("").astype(str).str.strip()

    def _history_turn_series(self, series: pd.Series) -> pd.Series:
        if hasattr(self, "_extract_turno"):
            return self._extract_turno(series)  # type: ignore[attr-defined]
        turno_num = pd.to_numeric(series, errors="coerce")
        turno_text = (
            series.astype(str)
            .str.extract(r"(\d+)", expand=False)
            .fillna("")
            .str.strip()
        )
        turno_text_num = pd.to_numeric(turno_text, errors="coerce")
        return turno_num.fillna(turno_text_num).fillna(0).astype(int)

    def _is_elected_like(self, series: pd.Series) -> pd.Series:
        if hasattr(self, "_is_elected"):
            return self._is_elected(series)  # type: ignore[attr-defined]
        if hasattr(self, "_is_elected_series"):
            return self._is_elected_series(series)  # type: ignore[attr-defined]
        normalized = self._text_series(series).str.upper()
        return normalized.str.startswith("ELEITO") | normalized.isin(["MÉDIA", "MEDIA"])

    def _candidate_retention_from_history(
        self,
        candidate_rows: pd.DataFrame,
        resolved_year: int | None,
        office: str | None = None,
        turno: int | None = None,
    ) -> float | None:
        """Retencao de votos: votos da eleicao atual / votos da eleicao ANTERIOR comparavel x 100.

        Comparavel = mesmo cargo e mesmo territorio (mesma UF; mesmo municipio nos cargos municipais),
        no mesmo turno quando o turno anterior existe. Sem eleicao comparavel (primeira candidatura
        ou troca de cargo) devolve None. Antes pegava a eleicao mais antiga e nao filtrava o cargo,
        o que dividia, por exemplo, votos de Presidente por votos de Vereador (retention de milhoes).
        """
        if candidate_rows.empty:
            return None

        col_year = self._select_history_col(candidate_rows, ["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votes = self._select_history_col(candidate_rows, ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_round = self._select_history_col(candidate_rows, ["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_cargo = self._select_history_col(candidate_rows, ["DS_CARGO", "DS_CARGO_D"])
        col_state = self._select_history_col(candidate_rows, ["SG_UF"])
        col_municipio = self._select_history_col(candidate_rows, ["NM_UE", "NM_MUNICIPIO"])
        if not col_year or not col_votes:
            return None

        history = candidate_rows.assign(
            _year=pd.to_numeric(candidate_rows[col_year], errors="coerce"),
            _votes=pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0),
            _round=(self._extract_turno(candidate_rows[col_round]) if col_round else 0),  # type: ignore[attr-defined]
            _office=(self._text_series(candidate_rows[col_cargo]) if col_cargo else ""),
            _state=(self._text_series(candidate_rows[col_state]).str.upper() if col_state else ""),
            _municipality=(self._text_series(candidate_rows[col_municipio]).str.upper() if col_municipio else ""),
        ).dropna(subset=["_year"])
        if history.empty:
            return None

        scope = history["_office"].apply(self._cargo_scope)  # type: ignore[attr-defined]
        history = history.assign(
            _office_key=history["_office"].astype(str).str.strip().str.upper(),
            _context_state=history["_state"].where(scope != "nacional", ""),
            _context_municipality=history["_municipality"].where(scope == "municipio", ""),
        )
        grouped = history.groupby(
            ["_year", "_office_key", "_context_state", "_context_municipality", "_round"], as_index=False
        ).agg(votes=("_votes", "sum"))
        if grouped.empty:
            return None

        current_pool = grouped
        if resolved_year is not None and (grouped["_year"] == resolved_year).any():
            current_pool = grouped[grouped["_year"] == resolved_year]
        office_key = str(office or "").strip().upper()
        if office_key and (current_pool["_office_key"] == office_key).any():
            current_pool = current_pool[current_pool["_office_key"] == office_key]
        current_pool = current_pool[current_pool["_year"] == current_pool["_year"].max()]
        if turno is not None and (current_pool["_round"] == int(turno)).any():
            current_pool = current_pool[current_pool["_round"] == int(turno)]
        else:
            current_pool = current_pool[current_pool["_round"] == current_pool["_round"].max()]
        current = current_pool.sort_values("votes", ascending=False).iloc[0]
        current_votes = float(current["votes"])

        previous = grouped[
            (grouped["_year"] < current["_year"])
            & (grouped["_office_key"] == current["_office_key"])
            & (grouped["_context_state"] == current["_context_state"])
            & (grouped["_context_municipality"] == current["_context_municipality"])
        ]
        if previous.empty:
            return None
        previous = previous[previous["_year"] == previous["_year"].max()]
        same_round = previous[previous["_round"] == current["_round"]]
        previous = same_round if not same_round.empty else previous[previous["_round"] == previous["_round"].max()]
        prev_votes = float(previous["votes"].sum())
        if prev_votes <= 0:
            return None
        return round((current_votes / prev_votes) * 100, 4)

    def _collapse_vote_history_rows(self, history_rows: pd.DataFrame) -> pd.DataFrame:
        if history_rows.empty:
            return history_rows

        col_year = self._select_history_col(history_rows, ["_year", "ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votes = self._select_history_col(history_rows, ["_votes", "votes", "QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_year or not col_votes:
            return history_rows

        round_col = self._select_history_col(history_rows, ["_round", "NR_TURNO", "CD_TURNO", "DS_TURNO"])

        collapsed = history_rows.assign(
            _year_num=pd.to_numeric(history_rows[col_year], errors="coerce"),
            _round_num=(
                pd.to_numeric(history_rows[round_col], errors="coerce").fillna(0).astype(int)
                if round_col == "_round"
                else (self._extract_turno(history_rows[round_col]) if round_col else 0)  # type: ignore[attr-defined]
            ),
            _votes_num=pd.to_numeric(history_rows[col_votes], errors="coerce").fillna(0),
        ).dropna(subset=["_year_num"])
        if collapsed.empty:
            return history_rows.iloc[0:0]

        collapsed = (
            collapsed.sort_values(["_year_num", "_round_num", "_votes_num"], ascending=[False, False, False], kind="stable")
            .drop_duplicates(subset=["_year_num"], keep="first")
            .sort_values(["_year_num"], ascending=[False], kind="stable")
            .drop(columns=["_year_num", "_round_num", "_votes_num"], errors="ignore")
        )
        return collapsed

    def _latest_vote_context_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        col_year = self._select_history_col(df, ["_year", "ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votes = self._select_history_col(df, ["_votes", "QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_year or not col_votes:
            return df.copy()

        col_round = self._select_history_col(df, ["_round", "NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_candidate_key = self._select_history_col(df, ["_candidate_key", "SQ_CANDIDATO", "NR_CANDIDATO"])
        col_name = self._select_history_col(df, ["_name_display", "NM_CANDIDATO", "NM_URNA_CANDIDATO"])

        if col_candidate_key:
            if col_candidate_key == "_candidate_key":
                candidate_key = df[col_candidate_key].fillna("").astype(str)
            else:
                candidate_key = self._identity_series(df[col_candidate_key])
        elif col_name:
            candidate_key = self._person_identity_name_series(df)
        else:
            candidate_key = pd.Series([""] * len(df), index=df.index)

        latest = df.assign(
            _year_num=(pd.to_numeric(df[col_year], errors="coerce") if col_year != "_year" else pd.to_numeric(df[col_year], errors="coerce")),
            _round_num=(
                pd.to_numeric(df[col_round], errors="coerce").fillna(0).astype(int)
                if col_round == "_round"
                else (self._extract_turno(df[col_round]) if col_round else pd.Series([0] * len(df), index=df.index))
            ),
            _votes_num=(pd.to_numeric(df[col_votes], errors="coerce").fillna(0) if col_votes == "_votes" else pd.to_numeric(df[col_votes], errors="coerce").fillna(0)),
            _candidate_key=candidate_key.fillna(""),
        ).dropna(subset=["_year_num"])
        latest = latest[latest["_candidate_key"] != ""].copy()
        if latest.empty or not col_round:
            return latest.drop(columns=["_year_num", "_round_num", "_votes_num", "_candidate_key"], errors="ignore")

        latest_rounds = (
            latest.groupby(["_candidate_key", "_year_num"], as_index=False)
            .agg(_latest_round_num=("_round_num", "max"))
        )
        latest = latest.merge(latest_rounds, on=["_candidate_key", "_year_num"], how="inner")
        latest = latest[latest["_round_num"] == latest["_latest_round_num"]].copy()
        return latest.drop(columns=["_year_num", "_round_num", "_votes_num", "_candidate_key", "_latest_round_num"], errors="ignore")

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
    ) -> dict[str, Any]:
        frame = self._candidate_compact_frame()
        if frame.empty:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        query = str(q or "").strip()
        if not query:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}
        q_norm = self._normalize_value(query, uppercase=True).lower()
        if not q_norm:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        filtered = frame
        if ano is not None and "_year" in filtered.columns:
            filtered = filtered[pd.to_numeric(filtered["_year"], errors="coerce") == int(ano)]
        if turno is not None and "_round" in filtered.columns:
            filtered = filtered[pd.to_numeric(filtered["_round"], errors="coerce").fillna(0).astype(int) == int(turno)]
        if uf and "_state" in filtered.columns:
            normalized_uf = self._normalize_value(uf, uppercase=True)
            filtered = filtered[filtered["_state"].fillna("").astype(str).str.upper() == normalized_uf]
        if cargo and "_office" in filtered.columns:
            filtered = filtered[filtered["_office"].fillna("").astype(str).str.lower() == cargo.lower()]
        if partido and "_party" in filtered.columns:
            filtered = filtered[filtered["_party"].fillna("").astype(str).str.upper().str.strip() == partido.upper().strip()]
        if turno is None:
            filtered = self._latest_vote_context_rows(filtered)

        if filtered.empty or "_name_norm" not in filtered.columns:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        def norm_col(name: str) -> pd.Series:
            if name in filtered.columns:
                return filtered[name].fillna("").astype(str)
            return pd.Series([""] * len(filtered), index=filtered.index)

        # Nome de registro, nome de urna e (consulta so de digitos) numero exato.
        names_norm = norm_col("_name_norm")
        registro_norm = norm_col("_registro_norm")
        urna_norm = norm_col("_urna_norm")
        numbers = norm_col("_number").str.replace(r"\.0$", "", regex=True).str.strip()
        is_number_query = q_norm.isdigit()
        number_match = (numbers == q_norm) if is_number_query else pd.Series(False, index=filtered.index)
        matched = (
            names_norm.str.contains(q_norm, regex=False)
            | registro_norm.str.contains(q_norm, regex=False)
            | urna_norm.str.contains(q_norm, regex=False)
            | number_match
        )
        if not matched.any():
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        ranked = filtered.loc[matched].copy()
        fields = [names_norm.loc[matched], registro_norm.loc[matched], urna_norm.loc[matched]]
        exact = number_match.loc[matched]
        starts = pd.Series(False, index=ranked.index)
        for field in fields:
            exact = exact | (field == q_norm)
            starts = starts | field.str.startswith(q_norm)
        score = exact.astype(int) * 3 + starts.astype(int) * 2 + 1
        votos = pd.to_numeric(ranked["_votes"], errors="coerce").fillna(0).astype("int64") if "_votes" in ranked.columns else pd.Series(0, index=ranked.index, dtype="int64")
        ranked = ranked.assign(
            _score=score,
            _votos=votos,
            _candidate_id=ranked["_candidate_id"].fillna("").astype(str) if "_candidate_id" in ranked.columns else ranked["_candidate_key"].fillna("").astype(str),
            _candidato=ranked["_name_display"].fillna("").astype(str) if "_name_display" in ranked.columns else ranked["_candidate_key"].fillna("").astype(str),
            _partido=ranked["_party"].fillna(pd.NA).astype("string") if "_party" in ranked.columns else pd.NA,
            _cargo=ranked["_office"].fillna(pd.NA).astype("string") if "_office" in ranked.columns else pd.NA,
            _uf=ranked["_state"].fillna(pd.NA).astype("string") if "_state" in ranked.columns else pd.NA,
            _urna=ranked["_urna_display"].fillna(pd.NA).astype("string") if "_urna_display" in ranked.columns else pd.NA,
            _numero=ranked["NR_CANDIDATO"].fillna(pd.NA).astype("string") if "NR_CANDIDATO" in ranked.columns else pd.NA,
            _situacao=ranked["DS_SIT_TOT_TURNO"].fillna(pd.NA).astype("string") if "DS_SIT_TOT_TURNO" in ranked.columns else pd.NA,
        )

        grouped = (
            ranked.groupby(["_candidate_id", "_candidato", "_urna", "_partido", "_cargo", "_uf", "_numero", "_situacao"], dropna=False, as_index=False)
            .agg(votes=("_votos", "sum"), score=("_score", "max"))
            .sort_values(["votes", "score", "_candidato"], ascending=[False, False, True])
        )

        total = int(len(grouped))
        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
        start = (page - 1) * page_size
        end = start + page_size
        grouped = grouped.iloc[start:end]

        items: list[dict[str, Any]] = []
        for _, row in grouped.iterrows():
            candidate_id = str(row["_candidate_id"]).strip() or str(row["_candidato"]).strip()
            candidate_rows = ranked[ranked["_candidate_id"] == row["_candidate_id"]].copy()
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
                    "latest_vote_value": int(row["votes"] or 0),
                    "candidato": str(row["_candidato"]).strip() or "",
                    "nm_urna_candidato": str(row["_urna"]).strip() if pd.notna(row["_urna"]) and str(row["_urna"]).strip() else None,
                    "partido": str(row["_partido"]).strip() if pd.notna(row["_partido"]) else None,
                    "cargo": str(row["_cargo"]).strip() if pd.notna(row["_cargo"]) else None,
                    "uf": str(row["_uf"]).strip() if pd.notna(row["_uf"]) else None,
                    "numero": str(numero_raw).strip() if pd.notna(numero_raw) else None,
                    "votos": int(row["votes"] or 0),
                    "situacao": str(row["_situacao"]).strip() if pd.notna(row["_situacao"]) else None,
                }
            )

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "items": items,
        }

    def _candidate_vote_turn_breakdown(self, candidate_rows: pd.DataFrame) -> dict[str, int | None]:
        if candidate_rows.empty:
            return {
                "turno_referencia": None,
                "votos_primeiro_turno": None,
                "votos_segundo_turno": None,
                "votos_consolidados": None,
            }

        col_round = self._select_history_col(candidate_rows, ["NR_TURNO", "CD_TURNO", "DS_TURNO"])
        col_votes = self._select_history_col(candidate_rows, ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_votes:
            return {
                "turno_referencia": None,
                "votos_primeiro_turno": None,
                "votos_segundo_turno": None,
                "votos_consolidados": None,
            }

        votes = pd.to_numeric(candidate_rows[col_votes], errors="coerce").fillna(0)
        consolidated = int(votes.sum())
        if col_round:
            rounds = self._history_turn_series(candidate_rows[col_round])
            valid_rounds = rounds[rounds > 0]
        else:
            rounds = pd.Series([0] * len(candidate_rows), index=candidate_rows.index)
            valid_rounds = pd.Series(dtype=int)

        turno_referencia = int(valid_rounds.max()) if not valid_rounds.empty else None
        votos_primeiro_turno = int(votes[rounds == 1].sum()) if (rounds == 1).any() else None
        votos_segundo_turno = int(votes[rounds == 2].sum()) if (rounds == 2).any() else None

        return {
            "turno_referencia": turno_referencia,
            "votos_primeiro_turno": votos_primeiro_turno,
            "votos_segundo_turno": votos_segundo_turno,
            "votos_consolidados": consolidated,
        }

    def candidate_vote_history(
        self,
        candidate_id: str,
        candidate_cpf: str | None = None,
        state: str | None = None,
        office: str | None = None,
        year: int | None = None,
        municipio: str | None = None,
    ) -> dict[str, Any]:
        targeted_loader = getattr(self, "_candidate_history_targeted_rows", None)
        if callable(targeted_loader):
            candidate_rows, identity = targeted_loader(
                candidate_id=candidate_id,
                candidate_cpf=candidate_cpf,
                state=state,
                office=office,
                year=year,
                municipio=municipio,
            )
            base_df = self._prepare_candidate_compact_rows(candidate_rows)
            candidate_rows = base_df
        else:
            base_df = self._prepare_candidate_compact_rows(self._candidate_compact_frame())
            candidate_rows, identity = self._historical_candidate_rows(
                candidate_id=candidate_id,
                candidate_cpf=candidate_cpf,
                state=state,
                office=office,
                all_rows=base_df,
                use_cpf_identity=bool(candidate_cpf),
                year=year,
                municipio=municipio,
            )  # type: ignore[attr-defined]
        if candidate_rows.empty:
            return {
                "candidate_id": str(candidate_id),
                "nr_cpf_candidato": self._cpf_text(candidate_cpf) or None,
                "canonical_candidate_id": None,
                "person_id": None,
                "items": [],
            }

        col_year = self._select_history_col(base_df, ["_year", "ANO_ELEICAO", "NR_ANO_ELEICAO"])
        col_votes = self._select_history_col(base_df, ["_votes", "QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        if not col_year or not col_votes:
            return {
                "candidate_id": str(candidate_id),
                "nr_cpf_candidato": self._cpf_text(candidate_cpf) or None,
                "canonical_candidate_id": None,
                "person_id": None,
                "items": [],
            }

        candidate_rows = self._prepare_candidate_compact_rows(candidate_rows)
        if candidate_rows.empty:
            return {
                "candidate_id": str(candidate_id),
                "nr_cpf_candidato": self._cpf_text(candidate_cpf) or None,
                "canonical_candidate_id": None,
                "person_id": None,
                "items": [],
            }

        history_context_cols = ["_year", "_office", "_context_state", "_context_municipality", "_round"]
        targeted_totals_loader = getattr(self, "_candidate_history_context_totals_for_rows", None)
        total_by_context = (
            targeted_totals_loader(candidate_rows)
            if callable(targeted_totals_loader)
            else self._candidate_history_context_totals_frame()
        )
        grouped = (
            candidate_rows.groupby(history_context_cols, as_index=False)
            .agg(
                votes=("_votes", "sum"),
                state=("_state", "first"),
                municipality=("_municipality", "first"),
                party=("_party", "first"),
                candidate_number=("_number", "first"),
                source_id=("_source_id", "first"),
            )
            .merge(total_by_context, on=history_context_cols, how="left")
            .sort_values(["_year", "_round", "_office", "votes"], ascending=[False, False, True, False])
        )

        status_by_context: dict[tuple[int, str, str, str, int], str | None] = {}
        if "DS_SIT_TOT_TURNO" in candidate_rows.columns:
            for key_vals, year_df in candidate_rows.groupby(history_context_cols):
                statuses = self._text_series(year_df["DS_SIT_TOT_TURNO"]).replace("", pd.NA).dropna()
                if statuses.empty:
                    status_by_context[tuple(key_vals)] = None
                elif self._is_elected_like(statuses).any():
                    status_by_context[tuple(key_vals)] = "ELEITO"
                else:
                    status_by_context[tuple(key_vals)] = str(statuses.iloc[0])

        grouped = self._collapse_vote_history_rows(grouped)

        items = []
        for row in grouped.to_dict("records"):
            year_int = int(row["_year"])
            votes_int = int(row["votes"])
            total_votes = float(row["total_votes"] or 0)
            round_int = int(row["_round"]) if int(row["_round"] or 0) > 0 else None
            state_value = str(row["state"]).strip() if pd.notna(row["state"]) and str(row["state"]).strip() else None
            if self._cargo_scope(str(row["_office"])) == "nacional":  # type: ignore[attr-defined]
                # Presidente e votado no pais todo: o "state" agregado era uma UF qualquer.
                state_value = "BR"
            municipality_value = (
                str(row["municipality"]).strip()
                if pd.notna(row["municipality"]) and str(row["municipality"]).strip() and self._cargo_scope(str(row["_office"])) == "municipio"  # type: ignore[attr-defined]
                else None
            )
            context_key = (
                year_int,
                str(row["_office"]),
                str(row["_context_state"]),
                str(row["_context_municipality"]),
                int(row["_round"] or 0),
            )
            items.append(
                {
                    "year": year_int,
                    "votes": votes_int,
                    "vote_share": round((votes_int / total_votes) * 100, 4) if total_votes > 0 else 0.0,
                    "status": status_by_context.get(context_key),
                    "office": (str(row["_office"]).strip() or None),
                    "state": state_value,
                    "municipality": municipality_value,
                    "round": round_int,
                    "candidate_number": (str(row["candidate_number"]).strip() or None) if pd.notna(row["candidate_number"]) else None,
                    "party": (str(row["party"]).strip() or None) if pd.notna(row["party"]) else None,
                    "source_id": (str(row["source_id"]).strip() or None) if pd.notna(row["source_id"]) else None,
                    "nr_cpf_candidato": identity["nr_cpf_candidato"],
                    "canonical_candidate_id": str(identity["canonical_candidate_id"]) if identity.get("canonical_candidate_id") else None,
                    "person_id": str(identity["person_id"]) if identity.get("person_id") else None,
                    "is_projection": False,
                }
            )
        return {
            "candidate_id": str(candidate_id),
            "nr_cpf_candidato": identity["nr_cpf_candidato"],
            "canonical_candidate_id": str(identity["canonical_candidate_id"]) if identity.get("canonical_candidate_id") else None,
            "person_id": str(identity["person_id"]) if identity.get("person_id") else None,
            "items": items,
        }

    def candidates_compare(
        self,
        candidate_ids: list[str],
        year: int | None = None,
        state: str | None = None,
        office: str | None = None,
        turno: int | None = None,
    ) -> dict[str, Any]:
        candidate_ids_clean = [str(v).strip() for v in candidate_ids if str(v).strip()]
        if len(candidate_ids_clean) < 2:
            return {"context": {"year": year, "state": state, "office": office, "turno": turno}, "candidates": [], "deltas": []}

        base_scoped = self._load_history_rows(uf=state, cargo=office)
        col_year = self._select_history_col(base_scoped, ["ANO_ELEICAO", "NR_ANO_ELEICAO"])
        resolved_year = year
        if resolved_year is None and col_year and not base_scoped.empty:
            years = pd.to_numeric(base_scoped[col_year], errors="coerce").dropna()
            if not years.empty:
                resolved_year = int(years.max())
        resolved_year = int(resolved_year) if resolved_year is not None else None

        context_df = self._load_history_rows(ano=resolved_year, uf=state, cargo=office) if resolved_year is not None else base_scoped
        col_votes = self._select_history_col(context_df, ["QT_VOTOS_NOMINAIS_VALIDOS", "NR_VOTACAO_NOMINAL", "QT_VOTOS_NOMINAIS"])
        col_party = self._select_history_col(context_df, ["SG_PARTIDO"])
        col_name = self._select_history_col(context_df, ["NM_CANDIDATO", "NM_URNA_CANDIDATO"])
        if context_df.empty or not col_votes:
            return {"context": {"year": resolved_year, "state": state, "office": office, "turno": turno}, "candidates": [], "deltas": []}

        col_round = self._select_history_col(context_df, ["NR_TURNO", "CD_TURNO"])
        resolved_turno = self._resolve_compare_turno(
            {cid: context_df[self._candidate_mask(context_df, cid)] for cid in candidate_ids_clean},  # type: ignore[attr-defined]
            col_round,
            turno,
        )
        if resolved_turno is not None and col_round:
            context_df = context_df[self._extract_turno(context_df[col_round]) == resolved_turno]

        context_df = context_df.assign(_votes=pd.to_numeric(context_df[col_votes], errors="coerce").fillna(0))
        total_context_votes = float(context_df["_votes"].sum()) or 1.0

        ranking_df = context_df.assign(
            _candidate_key=(
                self._text_series(context_df["SQ_CANDIDATO"]) if "SQ_CANDIDATO" in context_df.columns else (
                    self._text_series(context_df["NR_CANDIDATO"]) if "NR_CANDIDATO" in context_df.columns else self._text_series(context_df[col_name]).str.lower()
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

        history_scope_rows = self._load_history_rows(uf=state, cargo=office, columns=self._candidate_history_projection_columns())
        candidates_out: list[dict[str, Any]] = []
        for cid in candidate_ids_clean:
            candidate_rows = context_df[self._candidate_mask(context_df, cid)].copy()  # type: ignore[attr-defined]
            if candidate_rows.empty:
                continue
            votes = int(candidate_rows["_votes"].sum())
            name = (
                str(self._text_series(candidate_rows[col_name]).replace("", pd.NA).dropna().iloc[0])
                if col_name and self._text_series(candidate_rows[col_name]).replace("", pd.NA).dropna().any()
                else ""
            )
            party = (
                str(self._text_series(candidate_rows[col_party]).replace("", pd.NA).dropna().iloc[0])
                if col_party and self._text_series(candidate_rows[col_party]).replace("", pd.NA).dropna().any()
                else None
            )
            key = (
                str(self._text_series(candidate_rows["SQ_CANDIDATO"]).replace("", pd.NA).dropna().iloc[0])
                if "SQ_CANDIDATO" in candidate_rows.columns and self._text_series(candidate_rows["SQ_CANDIDATO"]).replace("", pd.NA).dropna().any()
                else (
                    str(self._text_series(candidate_rows["NR_CANDIDATO"]).replace("", pd.NA).dropna().iloc[0])
                    if "NR_CANDIDATO" in candidate_rows.columns and self._text_series(candidate_rows["NR_CANDIDATO"]).replace("", pd.NA).dropna().any()
                    else self._text_series(candidate_rows[col_name]).str.lower().iloc[0]
                )
            )

            history_rows, _ = self._historical_candidate_rows(candidate_id=cid, state=state, office=office, all_rows=history_scope_rows)  # type: ignore[attr-defined]
            retention = self._candidate_retention_from_history(history_rows, resolved_year, office=office, turno=resolved_turno)
            identity_payload = self._candidate_identity_payload(candidate_rows)  # type: ignore[attr-defined]

            candidates_out.append(
                {
                    "candidate_id": cid,
                    "source_id": identity_payload["source_id"],
                    "canonical_candidate_id": identity_payload["canonical_candidate_id"],
                    "person_id": identity_payload["person_id"],
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
            # Retencao e None para quem nao tem eleicao anterior comparavel: fica fora do ranking.
            comparable = [item for item in candidates_out if item.get(metric) is not None]
            ranked_metric = sorted(comparable, key=lambda item: float(item.get(metric) or 0), reverse=True)
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
            "context": {"year": resolved_year, "state": state, "office": office, "turno": resolved_turno},
            "candidates": candidates_out,
            "deltas": deltas,
        }
