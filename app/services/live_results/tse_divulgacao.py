from __future__ import annotations

import json
import math
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings


class LiveResultsError(Exception):
    """Base error for the public TSE live-results source."""


class LiveResultsNotPublished(LiveResultsError):
    pass


class LiveResultsUnavailable(LiveResultsError):
    pass


class LiveResultsService:
    def __init__(self, *, base_url: str | None = None, timeout_seconds: int | None = None) -> None:
        self.base_url = (base_url or settings.live_results_base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds or settings.live_results_timeout_seconds

    @staticmethod
    def _election_ids() -> dict[str, dict[str, dict[str, int]]]:
        """Le LIVE_RESULTS_ELECTIONS_JSON.

        Cada turno aceita um codigo unico (vale para todos os cargos, como nas
        municipais) ou um objeto por escopo: em 2026 o TSE separa a eleicao
        "federal" (Presidente) da "estadual" (Governador, Senador e Deputados).
        """
        try:
            value = json.loads(settings.live_results_elections_json)
        except json.JSONDecodeError as exc:
            raise LiveResultsUnavailable("Configuração de eleições ao vivo inválida.") from exc
        result: dict[str, dict[str, dict[str, int]]] = {}
        for year, rounds in value.items():
            result[str(year)] = {}
            for round_number, codes in rounds.items():
                if isinstance(codes, dict):
                    result[str(year)][str(round_number)] = {str(k): int(v) for k, v in codes.items()}
                else:
                    result[str(year)][str(round_number)] = {"*": int(codes)}
        return result

    @staticmethod
    def _scope(office: str) -> str:
        return "federal" if " ".join(office.strip().lower().split()) == "presidente" else "estadual"

    def _election_id(self, year: int, round_number: int, office: str = "") -> int:
        codes = self._election_ids().get(str(year), {}).get(str(round_number), {})
        election_id = codes.get(self._scope(office)) if office else None
        if election_id is None:
            election_id = codes.get("*")
        if election_id is None:
            raise LiveResultsNotPublished("Recorte de eleição ainda não publicado.")
        return election_id

    _municipality_tables: dict[int, dict[str, list[dict[str, str]]]] = {}

    @staticmethod
    def _normalize_name(value: str) -> str:
        text = unicodedata.normalize("NFKD", value.strip().upper())
        return " ".join("".join(ch for ch in text if not unicodedata.combining(ch)).split())

    def _municipality_table(self, year: int, election_id: int) -> dict[str, list[dict[str, str]]]:
        cached = self._municipality_tables.get(election_id)
        if cached is not None:
            return cached
        url = f"{self.base_url}/oficial/ele{year}/{election_id}/config/mun-e{election_id:06d}-cm.json"
        payload = self._get_json(url)
        table = {str(item.get("cd", "")).lower(): list(item.get("mu", [])) for item in payload.get("abr", [])}
        self._municipality_tables[election_id] = table
        return table

    def _resolve_municipality(self, *, year: int, election_id: int, state: str, municipality: str) -> str:
        """Aceita codigo TSE (ate 5 digitos), codigo IBGE (7 digitos) ou nome do municipio."""
        value = municipality.strip()
        if value.isdigit() and len(value) <= 5:
            return f"{int(value):05d}"
        entries = self._municipality_table(year, election_id).get(state.lower(), [])
        if value.isdigit() and len(value) == 7:
            match = next((m for m in entries if str(m.get("cdi")) == value), None)
        else:
            wanted = self._normalize_name(value)
            match = next((m for m in entries if self._normalize_name(str(m.get("nm", ""))) == wanted), None)
        if match is None:
            raise ValueError(
                "municipio não encontrado: use o código TSE, o código IBGE ou o nome do município na UF informada."
            )
        return f"{int(match['cd']):05d}"

    @staticmethod
    def _is_municipal(year: int) -> bool:
        return year % 4 == 0

    @staticmethod
    def _cargo_code(office: str) -> str:
        normalized = " ".join(office.strip().lower().split())
        codes = {
            "presidente": "0001",
            "governador": "0003",
            "senador": "0005",
            "deputado federal": "0006",
            "deputado estadual": "0007",
            "deputado distrital": "0008",
            "prefeito": "0011",
            "vereador": "0013",
        }
        try:
            return codes[normalized]
        except KeyError as exc:
            raise LiveResultsNotPublished("Cargo ainda não configurado para resultados ao vivo.") from exc

    def _build_url(self, *, year: int, round_number: int, office: str, state: str | None, municipality: str | None) -> str:
        cargo = self._cargo_code(office)
        election_id = self._election_id(year, round_number, office)
        municipal = self._is_municipal(year)
        if municipal:
            if not state or not municipality:
                raise ValueError("Eleições municipais exigem uf e municipio.")
            code = self._resolve_municipality(year=year, election_id=election_id, state=state, municipality=municipality)
            location = f"{state.lower()}{code}"
            return f"{self.base_url}/oficial/ele{year}/{election_id}/dados/{state.lower()}/{location}-c{cargo}-e{election_id:06d}-u.json"

        if office.strip().lower() != "presidente" and not state:
            raise ValueError("Eleições gerais exigem uf para este cargo.")
        location = "br" if office.strip().lower() == "presidente" else state.lower()
        return f"{self.base_url}/oficial/ele{year}/{election_id}/dados/{location}/{location}-c{cargo}-e{election_id:06d}-u.json"

    def photo_url(self, *, year: int | None, office: str | None, state: str | None, candidate_sq: str | None) -> str | None:
        """URL https da foto oficial no site de divulgacao do TSE (mesma fonte de /v1/resultados).

        So para anos com codigo de eleicao configurado; o TSE pode nao ter publicado a foto
        de algum candidato (404), entao o app deve tratar como imagem opcional.
        """
        sq = str(candidate_sq or "").strip()
        if not year or not office or not sq.isdigit():
            return None
        try:
            election_id = self._election_id(int(year), 1, office)
        except LiveResultsError:
            return None
        if self._scope(office) == "federal" and not self._is_municipal(int(year)):
            location = "br"
        elif state and len(state.strip()) == 2:
            location = state.strip().lower()
        else:
            return None
        return f"{self.base_url}/oficial/ele{int(year)}/{election_id}/fotos/{location}/{sq}.jpeg"

    def fetch(self, *, year: int, round_number: int, office: str, state: str | None, municipality: str | None) -> dict[str, Any]:
        url = self._build_url(year=year, round_number=round_number, office=office, state=state, municipality=municipality)
        return self._get_json(url)

    def _get_json(self, url: str) -> dict[str, Any]:
        request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "MeuCandidato/1.0"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise LiveResultsNotPublished("Recorte de eleição ainda não publicado.") from exc
            raise LiveResultsUnavailable("Fonte de resultados do TSE indisponível.") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise LiveResultsUnavailable("Fonte de resultados do TSE indisponível.") from exc

    @staticmethod
    def _number(value: Any) -> int | None:
        if value in (None, "", "-"):
            return None
        try:
            return int(str(value).replace(".", "").replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _percent(value: Any) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            return float(str(value).replace(".", "").replace(",", ".")) if "," in str(value) else float(value)
        except ValueError:
            return None

    @staticmethod
    def _timestamp(payload: dict[str, Any]) -> str | None:
        date_value = payload.get("dg") or payload.get("dt")
        time_value = payload.get("hg") or payload.get("ht")
        if not date_value or not time_value:
            return None
        try:
            parsed = datetime.strptime(f"{date_value} {time_value}", "%d/%m/%Y %H:%M:%S")
            return parsed.replace(tzinfo=ZoneInfo("America/Sao_Paulo")).isoformat()
        except ValueError:
            return None

    @staticmethod
    def _situation(payload: dict[str, Any]) -> str:
        if payload.get("tf") == "s":
            return "finalizada"
        if payload.get("and") in {"a", "i"}:
            return "em_andamento"
        return "nao_iniciada"

    def normalize(self, payload: dict[str, Any], *, page: int, page_size: int) -> dict[str, Any]:
        candidates: list[dict[str, Any]] = []
        for cargo in payload.get("carg", []):
            for aggregate in cargo.get("agr", []):
                for party in aggregate.get("par", []):
                    party_name = party.get("sg") or aggregate.get("com")
                    coalition = party.get("nfed") or aggregate.get("com") or aggregate.get("nm")
                    for candidate in party.get("cand", []):
                        votes = self._number(candidate.get("vap"))
                        if votes is None:
                            continue
                        item = {
                            "id": candidate.get("sqcand"),
                            "numero": candidate.get("n"),
                            "nome_urna": candidate.get("nmu") or candidate.get("nm"),
                            "partido": party_name,
                            "coligacao": coalition,
                            "votos": max(0, votes),
                            "percentual": self._percent(candidate.get("pvap")),
                            "situacao": candidate.get("st"),
                        }
                        candidates.append({key: value for key, value in item.items() if value is not None})

        candidates.sort(key=lambda item: item["votos"], reverse=True)
        total = len(candidates)
        start = (page - 1) * page_size
        summary = payload.get("e", {})
        votes = payload.get("v", {})
        return {
            "atualizado_em": self._timestamp(payload),
            "fonte": "TSE",
            "situacao": self._situation(payload),
            "secoes_totalizadas_pct": self._percent(payload.get("s", {}).get("psi")),
            "eleitorado": self._number(summary.get("te")),
            "comparecimento": self._number(summary.get("c")),
            "abstencoes": self._number(summary.get("a")),
            "votos_validos": self._number(votes.get("vvc") or votes.get("vv")),
            "votos_brancos": self._number(votes.get("vb")),
            "votos_nulos": self._number(votes.get("vn")),
            "page": page,
            "total_pages": math.ceil(total / page_size) if total else 0,
            "total": total,
            "candidatos": candidates[start : start + page_size],
        }

