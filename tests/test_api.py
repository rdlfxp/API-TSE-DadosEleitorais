import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics_service import AnalyticsService


@pytest.fixture
def client():
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2022,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Deputado Estadual",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 1,
                "NM_CANDIDATO": "Candidato A",
                "SG_PARTIDO": "AAA",
                "QT_VOTOS_NOMINAIS_VALIDOS": 12000,
                "IDADE": 45,
            },
            {
                "ANO_ELEICAO": 2022,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Deputado Estadual",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 2,
                "NM_CANDIDATO": "Candidato B",
                "SG_PARTIDO": "BBB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 8000,
                "IDADE": 36,
            },
            {
                "ANO_ELEICAO": 2022,
                "SG_UF": "RJ",
                "NM_UE": "RIO DE JANEIRO",
                "DS_CARGO": "Senador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 3,
                "NM_CANDIDATO": "Candidato C",
                "SG_PARTIDO": "CCC",
                "QT_VOTOS_NOMINAIS_VALIDOS": 30000,
                "IDADE": 60,
            },
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 4,
                "NM_CANDIDATO": "Prefeito A",
                "SG_PARTIDO": "AAA",
                "QT_VOTOS_NOMINAIS_VALIDOS": 500000,
                "IDADE": 52,
            },
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Vereador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 5,
                "NM_CANDIDATO": "Vereador A",
                "SG_PARTIDO": "BBB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 40000,
                "IDADE": 44,
            },
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Vereador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 6,
                "NM_CANDIDATO": "Vereador B",
                "SG_PARTIDO": "CCC",
                "QT_VOTOS_NOMINAIS_VALIDOS": 35000,
                "IDADE": 40,
            },
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "NM_UE": "CAMPINAS",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 7,
                "NM_CANDIDATO": "Prefeito B",
                "SG_PARTIDO": "DDD",
                "QT_VOTOS_NOMINAIS_VALIDOS": 200000,
                "IDADE": 50,
            },
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "NM_UE": "CAMPINAS",
                "DS_CARGO": "Vereador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 8,
                "NM_CANDIDATO": "Vereador C",
                "SG_PARTIDO": "EEE",
                "QT_VOTOS_NOMINAIS_VALIDOS": 18000,
                "IDADE": 38,
            },
        ]
    )
    with TestClient(main_module.app) as test_client:
        main_module.service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
        yield test_client
    main_module.service = None


def test_health_endpoint(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    assert "X-Request-Id" in response.headers
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["data_loaded"] is True
    assert "version" in payload


def test_filter_options_endpoint(client: TestClient):
    response = client.get("/v1/analytics/filtros")
    assert response.status_code == 200
    payload = response.json()
    assert payload["anos"] == [2022, 2024]
    assert payload["ufs"] == ["RJ", "SP"]
    assert set(payload["cargos"]) == {"Deputado Estadual", "Prefeito", "Senador", "Vereador"}


def test_overview_with_filters(client: TestClient):
    response = client.get(
        "/v1/analytics/overview",
        params={"ano": 2022, "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_registros"] == 2
    assert payload["total_candidatos"] == 2
    assert payload["total_eleitos"] == 1
    assert payload["total_votos_nominais"] == 20000


def test_top_candidates_respects_top_n(client: TestClient):
    response = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2022, "uf": "SP", "cargo": "Deputado Estadual", "top_n": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["top_n"] == 1
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidato"] == "Candidato A"


def test_top_candidates_rejects_above_limit(client: TestClient):
    response = client.get("/v1/analytics/top-candidatos", params={"top_n": 101})
    assert response.status_code == 422
    payload = response.json()
    assert payload["message"] == "Parametros de consulta invalidos."


def test_distribution_invalid_group_by_returns_400(client: TestClient):
    response = client.get("/v1/analytics/distribuicao", params={"group_by": "invalido"})
    assert response.status_code == 400
    payload = response.json()
    assert payload["message"] == "group_by invalido ou coluna ausente no dataset"


def test_candidates_search_with_filters_and_order(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos",
        params={"query": "candidato", "ano": 2022, "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["total_pages"] == 1
    assert len(payload["items"]) == 2
    assert payload["items"][0]["candidato"] == "Candidato A"
    assert payload["items"][1]["candidato"] == "Candidato B"


def test_candidates_search_pagination(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos",
        params={"query": "candidato", "ano": 2022, "page": 2, "page_size": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert payload["total_pages"] == 3
    assert payload["page"] == 2
    assert len(payload["items"]) == 1


def test_candidates_search_query_validation(client: TestClient):
    response = client.get("/v1/analytics/candidatos", params={"query": "a"})
    assert response.status_code == 422
    payload = response.json()
    assert payload["message"] == "Parametros de consulta invalidos."


def test_service_unavailable_returns_503(client: TestClient):
    main_module.service = None
    response = client.get("/v1/analytics/overview")
    assert response.status_code == 503
    payload = response.json()
    assert payload["message"] == (
        "Base analytics indisponivel. Ajuste ANALYTICS_DATA_PATH "
        "ou coloque o arquivo em data/curated/analytics.parquet (ou .csv)."
    )


def test_metrics_endpoint(client: TestClient):
    response = client.get("/metrics")
    assert response.status_code == 200
    text = response.text
    assert "requests_total" in text
    assert "requests_4xx_total" in text
    assert "requests_5xx_total" in text


def test_rate_limit_returns_429(client: TestClient):
    original_enabled = main_module.settings.rate_limit_enabled
    original_limit = main_module.settings.rate_limit_max_requests_per_ip
    original_window = main_module.settings.rate_limit_window_seconds
    try:
        main_module.settings.rate_limit_enabled = True
        main_module.settings.rate_limit_max_requests_per_ip = 2
        main_module.settings.rate_limit_window_seconds = 60
        main_module.RATE_LIMIT_COUNTERS.clear()

        assert client.get("/health").status_code == 200
        assert client.get("/health").status_code == 200
        blocked = client.get("/health")
        assert blocked.status_code == 429
        assert blocked.json()["message"] == "Limite de requisicoes excedido para este IP."
    finally:
        main_module.settings.rate_limit_enabled = original_enabled
        main_module.settings.rate_limit_max_requests_per_ip = original_limit
        main_module.settings.rate_limit_window_seconds = original_window
        main_module.RATE_LIMIT_COUNTERS.clear()


def test_time_series_endpoint(client: TestClient):
    response = client.get(
        "/v1/analytics/serie-temporal",
        params={"metric": "votos_nominais", "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["metric"] == "votos_nominais"
    assert payload["items"][0]["ano"] == 2022
    assert payload["items"][0]["value"] == 20000


def test_ranking_endpoint(client: TestClient):
    response = client.get(
        "/v1/analytics/ranking",
        params={
            "group_by": "partido",
            "metric": "votos_nominais",
            "ano": 2022,
            "uf": "SP",
            "cargo": "Deputado Estadual",
            "top_n": 2,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["group_by"] == "partido"
    assert payload["metric"] == "votos_nominais"
    assert payload["top_n"] == 2
    assert len(payload["items"]) == 2
    assert payload["items"][0]["label"] == "AAA"
    assert payload["items"][0]["value"] == 12000


def test_uf_map_endpoint(client: TestClient):
    response = client.get("/v1/analytics/mapa-uf", params={"metric": "registros", "ano": 2022})
    assert response.status_code == 200
    payload = response.json()
    assert payload["metric"] == "registros"
    assert payload["items"][0]["uf"] == "SP"
    assert payload["items"][0]["value"] == 2


def test_time_series_invalid_metric_returns_400(client: TestClient):
    response = client.get("/v1/analytics/serie-temporal", params={"metric": "invalida"})
    assert response.status_code == 400
    assert response.json()["message"] == "metric invalida ou coluna ausente no dataset"


def test_ranking_invalid_group_by_returns_400(client: TestClient):
    response = client.get("/v1/analytics/ranking", params={"group_by": "invalido", "metric": "registros"})
    assert response.status_code == 400
    assert response.json()["message"] == "group_by/metric invalido ou coluna ausente no dataset"


def test_official_vacancies_grouped_by_cargo(client: TestClient):
    response = client.get("/v1/analytics/vagas-oficiais", params={"ano": 2024, "uf": "SP", "group_by": "cargo"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["group_by"] == "cargo"
    assert payload["total_vagas_oficiais"] == 5
    by_cargo = {item["cargo"]: item["vagas_oficiais"] for item in payload["items"]}
    assert by_cargo["Prefeito"] == 2
    assert by_cargo["Vereador"] == 3


def test_official_vacancies_grouped_by_municipio_for_vereador(client: TestClient):
    response = client.get(
        "/v1/analytics/vagas-oficiais",
        params={"ano": 2024, "uf": "SP", "cargo": "Vereador", "group_by": "municipio"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["group_by"] == "municipio"
    assert payload["total_vagas_oficiais"] == 3
    by_city = {item["municipio"]: item["vagas_oficiais"] for item in payload["items"]}
    assert by_city["SAO PAULO"] == 2
    assert by_city["CAMPINAS"] == 1


def test_official_vacancies_grouped_by_municipio_non_municipal_cargo_returns_400(client: TestClient):
    response = client.get(
        "/v1/analytics/vagas-oficiais",
        params={"ano": 2022, "uf": "SP", "cargo": "Deputado Estadual", "group_by": "municipio"},
    )
    assert response.status_code == 400
    assert "group_by invalido" in response.json()["message"]
