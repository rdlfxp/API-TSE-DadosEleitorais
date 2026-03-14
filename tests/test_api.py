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
                "NR_TURNO": 1,
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
                "NR_TURNO": 1,
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
                "NR_TURNO": 1,
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
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 9,
                "NM_CANDIDATO": "Presidente X",
                "SG_PARTIDO": "PPP",
                "QT_VOTOS_NOMINAIS_VALIDOS": 70000,
                "IDADE": 58,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "RJ",
                "NM_UE": "RIO DE JANEIRO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 9,
                "NM_CANDIDATO": "Presidente X",
                "SG_PARTIDO": "PPP",
                "QT_VOTOS_NOMINAIS_VALIDOS": 80000,
                "IDADE": 58,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
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
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 4,
                "NM_CANDIDATO": "Prefeito A",
                "SG_PARTIDO": "AAA",
                "QT_VOTOS_NOMINAIS_VALIDOS": 550000,
                "IDADE": 52,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
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
                "NR_TURNO": 1,
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
                "NR_TURNO": 1,
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
                "NR_TURNO": 1,
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
    df = df.assign(
        DS_GENERO=df["SQ_CANDIDATO"].map(
            {
                1: "MASCULINO",
                2: "FEMININO",
                3: "MASCULINO",
                4: "MASCULINO",
                5: "MASCULINO",
                6: "FEMININO",
                7: "FEMININO",
                8: "MASCULINO",
                9: "MASCULINO",
            }
        ),
        DS_OCUPACAO=df["SQ_CANDIDATO"].map(
            {
                1: "EMPRESARIO",
                2: "SERVIDOR PUBLICO MUNICIPAL",
                3: "ADVOGADO",
                4: "EMPRESARIO",
                5: "SERVIDOR PUBLICO MUNICIPAL",
                6: "SERVIDOR PUBLICO MUNICIPAL",
                7: "ADVOGADO",
                8: "EMPRESARIO",
                9: "MILITAR",
            }
        ),
        DS_COR_RACA=df["SQ_CANDIDATO"].map(
            {
                1: "BRANCA",
                2: "NÃO DIVULGÁVEL",
                3: "PRETA",
                4: "PARDA",
                5: "PARDA",
                6: "NÃO INFORMADO",
                7: "INDÍGENA",
                8: "AMARELA",
                9: "PARDA",
            }
        ),
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
    assert set(payload["cargos"]) == {"Deputado Estadual", "Prefeito", "Presidente", "Senador", "Vereador"}


def test_overview_with_filters(client: TestClient):
    response = client.get(
        "/v1/analytics/overview",
        params={"ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_registros"] == 2
    assert payload["total_candidatos"] == 2
    assert payload["total_eleitos"] == 1
    assert payload["total_votos_nominais"] == 20000


def test_overview_with_municipio_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/overview",
        params={"ano": 2024, "uf": "SP", "municipio": "Campinas"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_registros"] == 2
    assert payload["total_candidatos"] == 2
    assert payload["total_eleitos"] == 2
    assert payload["total_votos_nominais"] == 218000


def test_overview_with_turno_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/overview",
        params={"ano": 2024, "turno": 2, "uf": "SP", "cargo": "Prefeito"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_registros"] == 1
    assert payload["total_candidatos"] == 1
    assert payload["total_eleitos"] == 1
    assert payload["total_votos_nominais"] == 550000


def test_top_candidates_respects_top_n(client: TestClient):
    response = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual", "top_n": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["top_n"] == 1
    assert payload["page"] == 1
    assert payload["page_size"] == 1
    assert payload["total"] == 2
    assert payload["total_pages"] == 2
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidato"] == "Candidato A"


def test_top_candidates_with_turno_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2024, "turno": 2, "uf": "SP", "cargo": "Prefeito", "top_n": 3},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidato"] == "Prefeito A"
    assert payload["items"][0]["votos"] == 550000


def test_top_candidates_aggregates_same_candidate_across_ufs(client: TestClient):
    response = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2022, "turno": 1, "cargo": "Presidente", "top_n": 5},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidato"] == "Presidente X"
    assert payload["items"][0]["votos"] == 150000
    assert payload["items"][0]["uf"] is None


def test_top_candidates_rejects_above_limit(client: TestClient):
    response = client.get("/v1/analytics/top-candidatos", params={"top_n": 101})
    assert response.status_code == 422
    payload = response.json()
    assert payload["message"] == "Parametros de consulta invalidos."


def test_top_candidates_pagination_returns_distinct_pages(client: TestClient):
    page_1 = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual", "page_size": 1, "page": 1},
    )
    page_2 = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual", "page_size": 1, "page": 2},
    )
    assert page_1.status_code == 200
    assert page_2.status_code == 200
    p1 = page_1.json()
    p2 = page_2.json()
    assert p1["items"][0]["candidato"] == "Candidato A"
    assert p2["items"][0]["candidato"] == "Candidato B"
    assert p1["items"] != p2["items"]


def test_distribution_invalid_group_by_returns_400(client: TestClient):
    response = client.get("/v1/analytics/distribuicao", params={"group_by": "invalido"})
    assert response.status_code == 400
    payload = response.json()
    assert payload["message"] == "group_by invalido ou coluna ausente no dataset"


def test_distribution_with_municipio_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/distribuicao",
        params={"group_by": "cargo", "ano": 2024, "uf": "SP", "municipio": "Campinas"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["group_by"] == "cargo"
    items = {item["label"]: item["value"] for item in payload["items"]}
    assert items == {"Prefeito": 1.0, "Vereador": 1.0}


def test_distribution_with_turno_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/distribuicao",
        params={"group_by": "cargo", "ano": 2024, "turno": 2, "uf": "SP"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == [{"label": "Prefeito", "value": 1.0, "percentage": 100.0}]


def test_cor_raca_comparativo_endpoint_normalizes_and_orders_categories(client: TestClient):
    response = client.get("/v1/analytics/cor-raca-comparativo", params={"ano": 2022})
    assert response.status_code == 200
    payload = response.json()
    categorias = [item["categoria"] for item in payload["items"]]
    assert categorias == ["Branca", "Preta", "Parda", "Amarela", "Indígena", "Não informado"]
    by_categoria = {item["categoria"]: item for item in payload["items"]}
    assert by_categoria["Branca"]["candidatos"] == 1
    assert by_categoria["Branca"]["eleitos"] == 1
    assert by_categoria["Preta"]["candidatos"] == 1
    assert by_categoria["Preta"]["eleitos"] == 1
    assert by_categoria["Parda"]["candidatos"] == 2
    assert by_categoria["Parda"]["eleitos"] == 0
    assert by_categoria["Não informado"]["candidatos"] == 1
    assert by_categoria["Não informado"]["eleitos"] == 0


def test_cor_raca_comparativo_endpoint_respects_filters(client: TestClient):
    response = client.get("/v1/analytics/cor-raca-comparativo", params={"ano": 2024, "uf": "SP", "municipio": "Campinas"})
    assert response.status_code == 200
    payload = response.json()
    by_categoria = {item["categoria"]: item for item in payload["items"]}
    assert by_categoria["Indígena"]["candidatos"] == 1
    assert by_categoria["Indígena"]["eleitos"] == 1
    assert by_categoria["Amarela"]["candidatos"] == 1
    assert by_categoria["Amarela"]["eleitos"] == 1


def test_occupation_gender_endpoint_with_filters(client: TestClient):
    response = client.get(
        "/v1/analytics/ocupacao-genero",
        params={"ano": 2024, "uf": "SP", "cargo": "Vereador"},
    )
    assert response.status_code == 200
    payload = response.json()
    by_job = {item["ocupacao"]: (item["masculino"], item["feminino"]) for item in payload["items"]}
    assert by_job["SERVIDOR PUBLICO MUNICIPAL"] == (1, 1)
    assert by_job["EMPRESARIO"] == (1, 0)


def test_occupation_gender_endpoint_with_somente_eleitos(client: TestClient):
    response = client.get(
        "/v1/analytics/ocupacao-genero",
        params={"ano": 2022, "somente_eleitos": "true"},
    )
    assert response.status_code == 200
    payload = response.json()
    by_job = {item["ocupacao"]: (item["masculino"], item["feminino"]) for item in payload["items"]}
    assert by_job == {"EMPRESARIO": (1, 0), "ADVOGADO": (1, 0)}


def test_age_endpoint_returns_200_for_all_candidates(client: TestClient):
    response = client.get(
        "/v1/analytics/idade",
        params={"ano": 2024, "cargo": "vereador", "somente_eleitos": "false"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["media"] is not None
    assert payload["bins"]


def test_age_endpoint_returns_200_for_elected_with_all_filters(client: TestClient):
    response = client.get(
        "/v1/analytics/idade",
        params={
            "ano": 2024,
            "uf": "SP",
            "cargo": "vereador",
            "turno": 1,
            "municipio": "SAO PAULO",
            "somente_eleitos": "true",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    labels = [item["label"] for item in payload["bins"]]
    assert labels == ["20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89"]
    by_label = {item["label"]: item["value"] for item in payload["bins"]}
    assert by_label["40-49"] == 2.0


def test_age_endpoint_sem_dados_returns_200_with_nulls_and_empty_bins(client: TestClient):
    response = client.get(
        "/v1/analytics/idade",
        params={
            "ano": 2024,
            "uf": "SP",
            "cargo": "prefeito",
            "turno": 2,
            "municipio": "Campinas",
            "somente_eleitos": "true",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["bins"] == []
    assert payload["media"] is None
    assert payload["mediana"] is None
    assert payload["minimo"] is None
    assert payload["maximo"] is None
    assert payload["desvio_padrao"] is None


def test_candidates_search_with_filters_and_order(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos",
        params={"query": "candidato", "ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual"},
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


def test_candidates_search_with_municipio_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos",
        params={"query": "vereador", "ano": 2024, "uf": "SP", "municipio": "Campinas"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["candidato"] == "Vereador C"


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
        params={"metric": "votos_nominais", "turno": 1, "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["metric"] == "votos_nominais"
    assert payload["items"][0]["ano"] == 2022
    assert payload["items"][0]["value"] == 20000


def test_time_series_with_municipio_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/serie-temporal",
        params={"metric": "votos_nominais", "uf": "SP", "municipio": "Campinas"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == [{"ano": 2024, "value": 218000.0}]


def test_ranking_endpoint(client: TestClient):
    response = client.get(
        "/v1/analytics/ranking",
        params={
            "group_by": "partido",
            "metric": "votos_nominais",
            "ano": 2022,
            "turno": 1,
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


def test_ranking_eleitos_with_municipio_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/ranking",
        params={
            "group_by": "partido",
            "metric": "eleitos",
            "ano": 2024,
            "uf": "SP",
            "municipio": "Campinas",
            "top_n": 5,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 2
    labels = {item["label"] for item in payload["items"]}
    assert labels == {"DDD", "EEE"}
    assert sum(item["value"] for item in payload["items"]) == 2


def test_ranking_with_turno_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/ranking",
        params={
            "group_by": "cargo",
            "metric": "registros",
            "ano": 2024,
            "turno": 2,
            "uf": "SP",
            "top_n": 10,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == [{"label": "Prefeito", "value": 1.0, "percentage": 100.0}]


def test_uf_map_endpoint(client: TestClient):
    response = client.get("/v1/analytics/mapa-uf", params={"metric": "registros", "ano": 2022})
    assert response.status_code == 200
    payload = response.json()
    assert payload["metric"] == "registros"
    assert payload["items"][0]["uf"] == "SP"
    assert payload["items"][0]["value"] == 3


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


def test_official_vacancies_with_turno_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/vagas-oficiais",
        params={"ano": 2024, "turno": 2, "uf": "SP", "group_by": "cargo"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_vagas_oficiais"] == 1
    assert payload["items"] == [
        {"ano": 2024, "uf": "SP", "municipio": None, "cargo": "Prefeito", "vagas_oficiais": 1}
    ]


def test_polarizacao_endpoint_returns_winners_and_municipal_breakdown(client: TestClient):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 100,
                "NM_CANDIDATO": "Gov SP A",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 101,
                "NM_CANDIDATO": "Gov SP B",
                "SG_PARTIDO": "PT",
                "QT_VOTOS_NOMINAIS_VALIDOS": 990000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "RJ",
                "NM_UE": "RIO DE JANEIRO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 102,
                "NM_CANDIDATO": "Gov RJ A",
                "SG_PARTIDO": "PT",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1200000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 200,
                "NM_CANDIDATO": "Pref SP A",
                "SG_PARTIDO": "PT",
                "QT_VOTOS_NOMINAIS_VALIDOS": 500000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 201,
                "NM_CANDIDATO": "Pref SP B",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 490000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "CAMPINAS",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 202,
                "NM_CANDIDATO": "Pref Campinas A",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 300000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "CAMPINAS",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 203,
                "NM_CANDIDATO": "Pref Campinas B",
                "SG_PARTIDO": "PT",
                "QT_VOTOS_NOMINAIS_VALIDOS": 290000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "RJ",
                "NM_UE": "NITEROI",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 204,
                "NM_CANDIDATO": "Pref Niteroi A",
                "SG_PARTIDO": "MDB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 210000,
            },
        ]
    )
    original_service = main_module.service
    try:
        main_module.service = AnalyticsService(dataframe=custom_df, default_top_n=20, max_top_n=100)
        response = client.get(
            "/v1/analytics/polarizacao",
            params={"uf": "SP", "ano_governador": 2022, "ano_municipal": 2024},
        )
    finally:
        main_module.service = original_service

    assert response.status_code == 200
    payload = response.json()

    federal_by_uf = {item["uf"]: item for item in payload["federal"]}
    assert federal_by_uf["SP"]["partido"] == "PL"
    assert federal_by_uf["SP"]["espectro"] == "direita"

    municipal_uf = payload["municipal_uf"]
    assert len(municipal_uf) == 2
    by_city = {item["municipio"]: item for item in municipal_uf}
    assert by_city["SAO PAULO"]["partido"] == "PT"
    assert by_city["SAO PAULO"]["votos"] == 500000
    assert by_city["SAO PAULO"]["status"] == "ELEITO"
    assert by_city["CAMPINAS"]["partido"] == "PL"

    municipal_brasil_by_uf = {item["uf"]: item for item in payload["municipal_brasil"]}
    assert municipal_brasil_by_uf["SP"]["espectro"] == "esquerda"
    assert municipal_brasil_by_uf["SP"]["total_prefeitos"] == 2


def test_turno_validation_returns_422(client: TestClient):
    response = client.get("/v1/analytics/overview", params={"ano": 2024, "turno": 3})
    assert response.status_code == 422
    assert response.json()["message"] == "Parametros de consulta invalidos."
