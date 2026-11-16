import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.api.routers import live_results as live_router
from app.services.live_results import LiveResultsNotPublished, LiveResultsUnavailable
from app.services.live_results.tse_divulgacao import LiveResultsService


FIXTURES = Path(__file__).parent / "fixtures" / "live_results"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_real_tse_2026_fixture_is_normalized_and_paginated() -> None:
    payload = LiveResultsService().normalize(load_fixture("2026-president-br.json"), page=1, page_size=2)
    assert payload["fonte"] == "TSE"
    assert payload["situacao"] == "finalizada"
    assert payload["total"] > 2
    assert payload["total_pages"] > 1
    assert payload["candidatos"][0]["votos"] >= payload["candidatos"][1]["votos"]
    assert payload["atualizado_em"].endswith("-03:00")


def test_tse_url_rules() -> None:
    service = LiveResultsService()
    assert "/br/br-c0001-e006257-u.json" in service._build_url(
        year=2026, round_number=1, office="Presidente", state=None, municipality=None
    )
    assert "/sp/sp71072-c0011-e000619-u.json" in service._build_url(
        year=2024, round_number=1, office="Prefeito", state="SP", municipality="71072"
    )
    with pytest.raises(ValueError):
        service._build_url(year=2024, round_number=1, office="Prefeito", state="SP", municipality=None)


def test_live_results_endpoint_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = load_fixture("2026-president-br.json")
    monkeypatch.setattr(live_router._service, "fetch", lambda **_: payload)
    with TestClient(main_module.app) as client:
        response = client.get("/v1/resultados", params={"ano": 2026, "cargo": "Presidente", "page_size": 2})
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=10"
    body = response.json()
    assert len(body["candidatos"]) == 2
    assert body["total"] >= body["total_pages"]


@pytest.mark.parametrize(
    "params",
    [
        {"ano": 2024, "turno": 1, "cargo": "Prefeito"},
        {"ano": 2026, "turno": 1, "cargo": "Governador"},
        {"ano": 2026, "turno": 2, "cargo": "Senador", "uf": "SP"},
    ],
)
def test_live_results_validation(params: dict[str, object]) -> None:
    with TestClient(main_module.app) as client:
        response = client.get("/v1/resultados", params=params)
    assert response.status_code == 422
    assert response.json()["code"] == "VALIDATION_ERROR"


def test_live_results_maps_source_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    with TestClient(main_module.app) as client:
        monkeypatch.setattr(live_router._service, "fetch", lambda **_: (_ for _ in ()).throw(LiveResultsNotPublished("not published")))
        not_published = client.get("/v1/resultados", params={"ano": 2026, "cargo": "Presidente"})
        monkeypatch.setattr(live_router._service, "fetch", lambda **_: (_ for _ in ()).throw(LiveResultsUnavailable("offline")))
        unavailable = client.get("/v1/resultados", params={"ano": 2026, "cargo": "Presidente"})
    assert not_published.status_code == 404
    assert unavailable.status_code == 503
    assert unavailable.json()["retryable"] is True



def test_2026_uses_federal_code_for_president_and_state_code_for_other_offices() -> None:
    service = LiveResultsService()
    assert "/ele2026/6258/dados/br/br-c0001-e006258-u.json" in service._build_url(
        year=2026, round_number=2, office="Presidente", state=None, municipality=None
    )
    assert "/ele2026/6260/dados/es/es-c0003-e006260-u.json" in service._build_url(
        year=2026, round_number=2, office="Governador", state="ES", municipality=None
    )
    assert "/ele2026/6259/dados/sp/sp-c0005-e006259-u.json" in service._build_url(
        year=2026, round_number=1, office="Senador", state="SP", municipality=None
    )
    # Formato antigo (um codigo por turno) continua valendo para todos os cargos.
    assert "/ele2024/620/" in service._build_url(year=2024, round_number=2, office="Prefeito", state="SP", municipality="71072")


def _service_with_municipalities(monkeypatch: pytest.MonkeyPatch) -> LiveResultsService:
    service = LiveResultsService()
    table = {"abr": [{"cd": "sp", "mu": [{"cd": "71072", "cdi": "3550308", "nm": "SÃO PAULO"},
                                         {"cd": "62391", "cdi": "3507159", "nm": "BOITUVA"}]}]}
    calls: list[str] = []
    monkeypatch.setattr(LiveResultsService, "_municipality_tables", {})
    monkeypatch.setattr(service, "_get_json", lambda url: calls.append(url) or table)
    service.calls = calls  # type: ignore[attr-defined]
    return service


@pytest.mark.parametrize("municipio", ["71072", "3550308", "São Paulo", "sao paulo", " SÃO  PAULO "])
def test_municipality_accepts_tse_code_ibge_code_or_name(monkeypatch: pytest.MonkeyPatch, municipio: str) -> None:
    service = _service_with_municipalities(monkeypatch)
    url = service._build_url(year=2024, round_number=1, office="Prefeito", state="SP", municipality=municipio)
    assert url.endswith("/sp/sp71072-c0011-e000619-u.json")


def test_municipality_table_is_fetched_once_and_unknown_city_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service_with_municipalities(monkeypatch)
    service._build_url(year=2024, round_number=1, office="Prefeito", state="SP", municipality="Boituva")
    service._build_url(year=2024, round_number=1, office="Prefeito", state="SP", municipality="3550308")
    assert service.calls == ["https://resultados.tse.jus.br/oficial/ele2024/619/config/mun-e000619-cm.json"]  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        service._build_url(year=2024, round_number=1, office="Prefeito", state="SP", municipality="Cidade Inexistente")


def test_invalid_municipality_returns_422_instead_of_500(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_value_error(**_):
        raise ValueError("municipio não encontrado")

    monkeypatch.setattr(live_router._service, "fetch", raise_value_error)
    with TestClient(main_module.app) as client:
        response = client.get("/v1/resultados", params={"ano": 2024, "cargo": "Prefeito", "uf": "SP", "municipio": "xyz"})
    assert response.status_code == 422
