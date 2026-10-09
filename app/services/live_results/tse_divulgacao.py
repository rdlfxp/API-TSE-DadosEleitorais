from __future__ import annotations

import json
import math
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
    def _election_ids() -> dict[str, dict[str, int]]:
        try:
            value = json.loads(settings.live_results_elections_json)
        except json.JSONDecodeError as exc:
            raise LiveResultsUnavailable("Configuração de eleições ao vivo inválida.") from exc
        return {
            str(year): {str(round_number): int(election_id) for round_number, election_id in rounds.items()}
            for year, rounds in value.items()
        }

    def _election_id(self, year: int, round_number: int) -> int:
        election_id = self._election_ids().get(str(year), {}).get(str(round_number))
        if election_id is None:
            raise LiveResultsNotPublished("Recorte de eleição ainda não publicado.")
        return election_id

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
        election_id = self._election_id(year, round_number)
        cargo = self._cargo_code(office)
        municipal = self._is_municipal(year)
        if municipal:
            if not state or not municipality:
                raise ValueError("Eleições municipais exigem uf e municipio.")
            if not municipality.isdigit() or len(municipality) > 5:
                raise ValueError("municipio deve ser o código eleitoral numérico do TSE.")
            location = f"{state.lower()}{int(municipality):05d}"
            return f"{self.base_url}/oficial/ele{year}/{election_id}/dados/{state.lower()}/{location}-c{cargo}-e{election_id:06d}-u.json"

        if office.strip().lower() != "presidente" and not state:
            raise ValueError("Eleições gerais exigem uf para este cargo.")
        location = "br" if office.strip().lower() == "presidente" else state.lower()
        return f"{self.base_url}/oficial/ele{year}/{election_id}/dados/{location}/{location}-c{cargo}-e{election_id:06d}-u.json"

    def fetch(self, *, year: int, round_number: int, office: str, state: str | None, municipality: str | None) -> dict[str, Any]:
        url = self._build_url(year=year, round_number=round_number, office=office, state=state, municipality=municipality)
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

