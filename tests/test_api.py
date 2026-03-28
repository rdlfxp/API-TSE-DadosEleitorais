import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics_service import AnalyticsService
from app.services.duckdb_analytics_service import DuckDBAnalyticsService


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
        DS_GRAU_INSTRUCAO=df["SQ_CANDIDATO"].map(
            {
                1: "SUPERIOR COMPLETO",
                2: "ENSINO MEDIO COMPLETO",
                3: "SUPERIOR COMPLETO",
                4: "SUPERIOR COMPLETO",
                5: "ENSINO MEDIO COMPLETO",
                6: "SUPERIOR COMPLETO",
                7: "SUPERIOR COMPLETO",
                8: "ENSINO MEDIO COMPLETO",
                9: "SUPERIOR COMPLETO",
            }
        ),
        DS_FAIXA_RENDA=df["SQ_CANDIDATO"].map(
            {
                1: "5-10 SM",
                2: "2-5 SM",
                3: "10+ SM",
                4: "10+ SM",
                5: "2-5 SM",
                6: "2-5 SM",
                7: "5-10 SM",
                8: "2-5 SM",
                9: "10+ SM",
            }
        ),
        NR_ZONA=df["SQ_CANDIDATO"].map(
            {
                1: "001",
                2: "001",
                3: "010",
                4: "100",
                5: "110",
                6: "111",
                7: "120",
                8: "121",
                9: "200",
            }
        ),
        NM_ZONA=df["SQ_CANDIDATO"].map(
            {
                1: "Zona 1",
                2: "Zona 1",
                3: "Zona 10",
                4: "Zona 100",
                5: "Zona 110",
                6: "Zona 111",
                7: "Zona 120",
                8: "Zona 121",
                9: "Zona 200",
            }
        ),
        LATITUDE=df["SQ_CANDIDATO"].map(
            {
                1: -23.5505,
                2: -23.5505,
                3: -22.9068,
                4: -23.5505,
                5: -23.5505,
                6: -23.5510,
                7: -22.9099,
                8: -22.9100,
                9: -23.5505,
            }
        ),
        LONGITUDE=df["SQ_CANDIDATO"].map(
            {
                1: -46.6333,
                2: -46.6333,
                3: -43.1729,
                4: -46.6333,
                5: -46.6333,
                6: -46.6340,
                7: -47.0626,
                8: -47.0625,
                9: -46.6333,
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
    assert payload["items"][0]["candidate_id"] == "1"
    assert payload["items"][0]["candidato"] == "Candidato A"


def test_top_candidates_with_turno_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2024, "turno": 2, "uf": "SP", "cargo": "Prefeito", "top_n": 3},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidate_id"] == "4"
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
    assert payload["items"][0]["candidate_id"] == "9"
    assert payload["items"][0]["candidato"] == "Presidente X"
    assert payload["items"][0]["votos"] == 150000
    assert payload["items"][0]["uf"] is None


def test_top_candidates_filters_by_partido(client: TestClient):
    response = client.get(
        "/v1/analytics/top-candidatos",
        params={"ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual", "partido": "BBB"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidate_id"] == "2"
    assert payload["items"][0]["partido"] == "BBB"
    assert payload["items"][0]["candidato"] == "Candidato B"


def test_top_candidates_rejects_above_limit(client: TestClient):
    response = client.get("/v1/analytics/top-candidatos", params={"top_n": 101})
    assert response.status_code == 422
    payload = response.json()
    assert payload["message"] == "Parametros de consulta invalidos."
    assert payload["traceId"]
    assert payload["code"] == "VALIDATION_ERROR"
    assert payload["retryable"] is False


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
    assert p1["items"][0]["candidate_id"] == "1"
    assert p2["items"][0]["candidate_id"] == "2"
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
        "/v1/analytics/candidatos/search",
        params={"q": "candidato", "ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["total_pages"] == 1
    assert len(payload["items"]) == 2
    assert payload["items"][0]["candidate_id"] == "1"
    assert payload["items"][1]["candidate_id"] == "2"
    assert payload["items"][0]["candidato"] == "Candidato A"
    assert payload["items"][1]["candidato"] == "Candidato B"


def test_candidates_search_pagination(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos/search",
        params={"q": "candidato", "ano": 2022, "page": 2, "page_size": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert payload["total_pages"] == 3
    assert payload["page"] == 2
    assert len(payload["items"]) == 1
    assert payload["items"][0]["candidate_id"] == "1"


def test_candidates_search_query_validation(client: TestClient):
    response = client.get("/v1/analytics/candidatos/search", params={"q": "a", "ano": 2024})
    assert response.status_code == 422
    payload = response.json()
    assert payload["message"] == "Parametros de consulta invalidos."


def test_candidates_search_with_partido_filter(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos/search",
        params={"q": "vereador", "ano": 2024, "uf": "SP", "partido": "EEE"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["candidate_id"] == "8"
    assert payload["items"][0]["candidato"] == "Vereador C"
    assert payload["items"][0]["numero"] == "8"


def test_candidates_search_legacy_route_keeps_query_param(client: TestClient):
    response = client.get(
        "/v1/analytics/candidatos",
        params={"query": "candidato", "ano": 2022, "turno": 1, "uf": "SP", "cargo": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["items"][0]["candidate_id"] == "1"
    assert payload["items"][1]["candidate_id"] == "2"


def test_candidate_summary_endpoint(client: TestClient):
    response = client.get("/v1/candidates/1/summary", params={"year": 2022, "state": "SP", "office": "Deputado Estadual"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "1"
    assert payload["name"] == "Candidato A"
    assert payload["party"] == "AAA"
    assert payload["latest_election"]["votes"] == 12000
    assert payload["latest_election"]["state_rank"] == 1


def test_candidate_vote_history_endpoint(client: TestClient):
    response = client.get("/v1/candidates/4/vote-history", params={"state": "SP", "office": "Prefeito"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "4"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["year"] == 2024
    assert payload["items"][0]["votes"] == 1050000
    assert payload["items"][0]["status"] == "ELEITO"


def test_candidate_electorate_profile_endpoint(client: TestClient):
    response = client.get(
        "/v1/candidates/6/electorate-profile",
        params={"year": 2024, "state": "SP", "office": "Vereador"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "6"
    assert payload["gender"]["female_share"] == 100.0
    assert payload["age_bands"]["a35_59"] == 100.0
    assert payload["dominant_education"] == "SUPERIOR COMPLETO"
    assert payload["dominant_income_band"] == "2-5 SM"


def test_candidate_vote_distribution_endpoint(client: TestClient):
    response = client.get(
        "/v1/candidates/9/vote-distribution",
        params={"level": "uf", "year": 2022, "office": "Presidente"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "9"
    assert payload["level"] == "uf"
    by_uf = {item["key"]: item for item in payload["items"]}
    assert by_uf["SP"]["votes"] == 70000
    assert by_uf["RJ"]["votes"] == 80000


def test_candidate_vote_distribution_municipal_prefeito_level_zona(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-distribution",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipio": "SAO PAULO",
            "cargo": "Prefeito",
            "turno": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "4"
    assert payload["level"] == "zona"
    assert payload["total_candidate_votes"] == 500000
    assert payload["metadata"]["scope"] == "municipal"
    assert payload["metadata"]["no_data"] is False
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["key"] == "zona-100"
    assert item["label"] == "Zona 100"
    assert item["zone_id"] == "100"
    assert item["zone_name"] == "Zona 100"
    assert item["municipio"] == "SAO PAULO"
    assert item["uf"] == "SP"
    assert item["votes"] == 500000


def test_candidate_vote_distribution_municipal_vereador_level_zona(client: TestClient):
    response = client.get(
        "/v1/candidates/8/vote-distribution",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipio": "CAMPINAS",
            "cargo": "Vereador",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "8"
    assert payload["level"] == "zona"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["key"] == "zona-121"
    assert payload["items"][0]["zone_id"] == "121"


def test_candidate_vote_distribution_municipal_without_municipio_infers_scope_with_uf(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-distribution",
        params={"level": "zona", "ano": 2024, "uf": "SP", "cargo": "Prefeito", "turno": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    assert payload["metadata"]["scope"] == "municipal"
    assert payload["metadata"]["inferred_geo"] is True
    assert payload["metadata"]["inferred_municipio"] == "SAO PAULO"
    assert payload["metadata"]["requested_uf"] == "SP"
    assert payload["metadata"]["used_uf"] == "SP"
    assert payload["metadata"]["used_municipio"] == "SAO PAULO"


def test_candidate_vote_distribution_municipal_recorte_sem_dados_returns_empty_200(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-distribution",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipio": "CAMPINAS",
            "cargo": "Prefeito",
            "turno": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == []
    assert payload["metadata"]["no_data"] is True
    assert payload["metadata"]["scope"] == "municipal"


def test_candidate_vote_distribution_municipal_turno_2_fallback_to_1(client: TestClient):
    response = client.get(
        "/v1/candidates/8/vote-distribution",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipio": "CAMPINAS",
            "cargo": "Vereador",
            "turno": 2,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["metadata"]["fallback_applied"] is True
    assert payload["metadata"]["requested_turno"] == 2
    assert payload["metadata"]["used_turno"] == 1


def test_candidate_vote_distribution_municipio_adds_impact_fields(client: TestClient):
    response = client.get(
        "/v1/candidates/9/vote-distribution",
        params={"level": "municipio", "ano": 2022, "uf": "SP", "cargo": "Presidente", "turno": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "9"
    assert payload["level"] == "municipio"
    assert payload["total_items"] == 1
    item = payload["items"][0]
    assert item["nm_municipio"] == "SAO PAULO"
    assert item["votos_validos_do_municipio"] == 70000
    assert item["qt_votos"] == 70000
    assert item["percentual_candidato_no_municipio"] == 100.0
    assert item["percentual_no_total_do_candidato"] == 100.0
    assert item["categoria_impacto"] == "Alto"


def test_candidate_vote_distribution_municipio_zero_denominator_returns_zero(client: TestClient):
    zero_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2018,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Presidente",
                "SQ_CANDIDATO": 99,
                "NM_CANDIDATO": "Zero",
                "QT_VOTOS_NOMINAIS_VALIDOS": 0,
            }
        ]
    )
    with TestClient(main_module.app) as test_client:
        main_module.service = AnalyticsService(dataframe=zero_df, default_top_n=20, max_top_n=100)
        response = test_client.get(
            "/v1/candidates/99/vote-distribution",
            params={"level": "municipio", "year": 2018, "state": "SP", "office": "Presidente", "round": 1},
        )
    main_module.service = None
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_items"] == 1
    item = payload["items"][0]
    assert item["percentual_candidato_no_municipio"] == 0.0
    assert item["percentual_no_total_do_candidato"] == 0.0
    assert item["categoria_impacto"] == "Baixo"


def test_candidate_vote_distribution_ignores_uf_br_literal(client: TestClient):
    response = client.get(
        "/v1/candidates/9/vote-distribution",
        params={"level": "municipio", "ano": 2022, "uf": "BR", "cargo": "Presidente", "turno": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_items"] == 2
    by_city = {item["nm_municipio"]: item for item in payload["items"]}
    assert by_city["SAO PAULO"]["qt_votos"] == 70000
    assert by_city["RIO DE JANEIRO"]["qt_votos"] == 80000


def test_candidate_zone_fidelity_endpoint(client: TestClient):
    response = client.get(
        "/v1/candidates/4/zone-fidelity",
        params={"year": 2024, "state": "SP", "office": "Prefeito"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "4"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["zone_id"] == "100"
    assert payload["items"][0]["votes"] == 1050000


def test_candidate_vote_map_endpoint_auto_uses_zona_for_municipal_office(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-map",
        params={"year": 2024, "state": "SP", "municipio": "SAO PAULO", "office": "Prefeito"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "4"
    assert payload["level"] == "zona"
    assert payload["total_votes"] == 1050000
    assert len(payload["items"]) == 1
    assert payload["items"][0]["zona"] == "100"
    assert payload["items"][0]["votes"] == 1050000
    assert payload["items"][0]["tier"] == 0
    assert payload["items"][0]["key"] == "zona-100"
    assert payload["items"][0]["label"] == "Zona 100"
    assert payload["items"][0]["lat"] != 0.0 or payload["items"][0]["lng"] != 0.0


def test_candidate_vote_map_municipal_prefeito_turno_1_level_zona(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-map",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipio": "SAO PAULO",
            "cargo": "Prefeito",
            "turno": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "4"
    assert payload["level"] == "zona"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["zona"] == "100"
    assert payload["items"][0]["lat"] is not None
    assert payload["items"][0]["lng"] is not None
    assert payload["region_clusters"] == []


def test_candidate_vote_map_municipal_accepts_aliases_and_municipio_with_accent(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-map",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipality": "São Paulo",
            "cargo": "Prefeito",
            "round": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    assert payload["metadata"]["municipio"] == "SAO PAULO"


def test_candidate_vote_map_municipal_vereador_campinas_level_zona(client: TestClient):
    response = client.get(
        "/v1/candidates/8/vote-map",
        params={"level": "zona", "ano": 2024, "uf": "SP", "municipio": "CAMPINAS", "cargo": "Vereador"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["level"] == "zona"
    assert len(payload["items"]) == 1
    assert payload["items"][0]["zona"] == "121"
    assert payload["items"][0]["key"] == "zona-121"


def test_candidate_vote_distribution_municipal_accepts_aliases_and_municipio_with_accent(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-distribution",
        params={
            "level": "zona",
            "ano": 2024,
            "uf": "SP",
            "municipality": "São Paulo",
            "cargo": "Prefeito",
            "round": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    assert payload["metadata"]["municipio"] == "SAO PAULO"


def test_candidate_vote_map_municipal_without_municipio_infers_scope_with_uf(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-map",
        params={"level": "zona", "ano": 2024, "uf": "SP", "cargo": "Prefeito"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]
    assert payload["metadata"]["scope"] == "municipal"
    assert payload["metadata"]["inferred_geo"] is True
    assert payload["metadata"]["inferred_municipio"] == "SAO PAULO"
    assert payload["metadata"]["requested_uf"] == "SP"
    assert payload["metadata"]["used_uf"] == "SP"
    assert payload["metadata"]["used_municipio"] == "SAO PAULO"
    assert payload["metadata"]["traceId"]


def test_candidate_vote_endpoints_municipal_duckdb_case_250002098117_infers_sao_paulo_and_is_coherent(
    client: TestClient, tmp_path
):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SÃO PAULO",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": "250002098117",
                "NM_CANDIDATO": "Candidato Produção",
                "QT_VOTOS_NOMINAIS_VALIDOS": 150000,
                "NR_ZONA": "100",
                "LATITUDE": -23.5505,
                "LONGITUDE": -46.6333,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "CAMPINAS",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": "250002098117",
                "NM_CANDIDATO": "Candidato Produção",
                "QT_VOTOS_NOMINAIS_VALIDOS": 10,
                "NR_ZONA": "101",
                "LATITUDE": -22.9099,
                "LONGITUDE": -47.0626,
            },
        ]
    )
    csv_path = tmp_path / "duckdb_scope_case.csv"
    custom_df.to_csv(csv_path, index=False)

    original_service = main_module.service
    try:
        main_module.service = DuckDBAnalyticsService.from_file(
            file_path=str(csv_path),
            default_top_n=20,
            max_top_n=100,
        )
        map_response = client.get(
            "/v1/candidates/250002098117/vote-map",
            params={"level": "zona", "ano": 2024, "uf": "SP", "cargo": "Prefeito", "turno": 1},
        )
        distribution_response = client.get(
            "/v1/candidates/250002098117/vote-distribution",
            params={"level": "zona", "ano": 2024, "uf": "SP", "cargo": "Prefeito", "turno": 1},
        )
    finally:
        main_module.service = original_service

    assert map_response.status_code == 200
    assert distribution_response.status_code == 200
    map_payload = map_response.json()
    distribution_payload = distribution_response.json()
    assert map_payload["metadata"]["inferred_municipio"] == "SAO PAULO"
    assert distribution_payload["metadata"]["inferred_municipio"] == "SAO PAULO"
    assert map_payload["metadata"]["used_municipio"] == "SAO PAULO"
    assert distribution_payload["metadata"]["used_municipio"] == "SAO PAULO"
    assert map_payload["metadata"]["used_uf"] == distribution_payload["metadata"]["used_uf"] == "SP"
    assert map_payload["metadata"]["no_data"] == distribution_payload["metadata"]["no_data"]
    assert map_payload["metadata"]["traceId"]
    assert distribution_payload["metadata"]["traceId"]


def test_candidate_vote_map_duckdb_candidate_id_resolution_error_returns_200(tmp_path, client: TestClient):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": "250002098117",
                "NM_CANDIDATO": "Candidato Produção",
                "QT_VOTOS_NOMINAIS_VALIDOS": 150000,
                "NR_ZONA": "100",
            },
        ]
    )
    csv_path = tmp_path / "duckdb_vote_map_candidate_id_error.csv"
    custom_df.to_csv(csv_path, index=False)

    original_service = main_module.service
    try:
        service = DuckDBAnalyticsService.from_file(
            file_path=str(csv_path),
            default_top_n=20,
            max_top_n=100,
        )
        original_rows = service._rows

        def _rows_with_candidate_id_failure(sql: str, params: list[object]):
            if "AS candidate_id FROM analytics" in sql:
                raise RuntimeError("forced candidate_id resolution failure")
            return original_rows(sql, params)

        service._rows = _rows_with_candidate_id_failure  # type: ignore[method-assign]
        main_module.service = service
        response = client.get(
            "/v1/candidates/250002098117/vote-map",
            params={"level": "zona", "ano": 2024, "uf": "SP", "cargo": "Prefeito", "turno": 1},
        )
    finally:
        main_module.service = original_service

    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "250002098117"
    assert payload["metadata"]["traceId"]


def test_candidate_vote_endpoints_municipal_without_uf_and_municipio_infer_both(client: TestClient):
    map_response = client.get(
        "/v1/candidates/4/vote-map",
        params={"level": "zona", "ano": 2024, "cargo": "Prefeito", "turno": 1},
    )
    assert map_response.status_code == 200
    map_payload = map_response.json()
    assert map_payload["items"]
    assert map_payload["metadata"]["inferred_geo"] is True
    assert map_payload["metadata"]["inferred_uf"] == "SP"
    assert map_payload["metadata"]["inferred_municipio"] == "SAO PAULO"
    assert map_payload["metadata"]["used_uf"] == "SP"
    assert map_payload["metadata"]["used_municipio"] == "SAO PAULO"

    distribution_response = client.get(
        "/v1/candidates/4/vote-distribution",
        params={"level": "zona", "ano": 2024, "cargo": "Prefeito", "turno": 1},
    )
    assert distribution_response.status_code == 200
    distribution_payload = distribution_response.json()
    assert distribution_payload["items"]
    assert distribution_payload["metadata"]["inferred_geo"] is True
    assert distribution_payload["metadata"]["inferred_uf"] == "SP"
    assert distribution_payload["metadata"]["inferred_municipio"] == "SAO PAULO"
    assert distribution_payload["metadata"]["used_uf"] == "SP"
    assert distribution_payload["metadata"]["used_municipio"] == "SAO PAULO"


def test_candidate_vote_endpoints_municipality_alias_equivalence(client: TestClient):
    map_alias = client.get(
        "/v1/candidates/4/vote-map",
        params={"level": "zona", "ano": 2024, "uf": "SP", "municipality": "São Paulo", "cargo": "Prefeito", "turno": 1},
    )
    map_native = client.get(
        "/v1/candidates/4/vote-map",
        params={"level": "zona", "ano": 2024, "uf": "SP", "municipio": "SAO PAULO", "cargo": "Prefeito", "turno": 1},
    )
    assert map_alias.status_code == 200
    assert map_native.status_code == 200
    assert map_alias.json()["items"] == map_native.json()["items"]

    distribution_alias = client.get(
        "/v1/candidates/4/vote-distribution",
        params={"level": "zona", "ano": 2024, "uf": "SP", "municipality": "São Paulo", "cargo": "Prefeito", "turno": 1},
    )
    distribution_native = client.get(
        "/v1/candidates/4/vote-distribution",
        params={"level": "zona", "ano": 2024, "uf": "SP", "municipio": "SAO PAULO", "cargo": "Prefeito", "turno": 1},
    )
    assert distribution_alias.status_code == 200
    assert distribution_native.status_code == 200
    assert distribution_alias.json()["items"] == distribution_native.json()["items"]


def test_candidate_vote_endpoints_municipal_inference_failure_returns_400_with_trace(client: TestClient):
    map_response = client.get(
        "/v1/candidates/4/vote-map",
        params={"level": "zona", "ano": 2024, "uf": "RJ", "cargo": "Prefeito", "turno": 1},
    )
    assert map_response.status_code == 400
    map_payload = map_response.json()
    assert map_payload["code"] == "BAD_REQUEST"
    assert map_payload["message"] == "Nao foi possivel inferir municipio para candidate_id no recorte informado."
    assert map_payload["traceId"]

    distribution_response = client.get(
        "/v1/candidates/4/vote-distribution",
        params={"level": "zona", "ano": 2024, "uf": "RJ", "cargo": "Prefeito", "turno": 1},
    )
    assert distribution_response.status_code == 400
    distribution_payload = distribution_response.json()
    assert distribution_payload["code"] == "BAD_REQUEST"
    assert distribution_payload["message"] == "Nao foi possivel inferir municipio para candidate_id no recorte informado."
    assert distribution_payload["traceId"]


def test_candidate_vote_map_municipal_recorte_sem_dados_returns_empty_200(client: TestClient):
    response = client.get(
        "/v1/candidates/4/vote-map",
        params={"level": "zona", "ano": 2024, "uf": "SP", "municipio": "CAMPINAS", "cargo": "Prefeito", "turno": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == []
    assert payload["metadata"]["no_data"] is True
    assert payload["metadata"]["scope"] == "municipal"


def test_candidate_vote_map_municipal_geo_error_never_returns_500(client: TestClient):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 444,
                "NM_CANDIDATO": "Prefeito Geo",
                "QT_VOTOS_NOMINAIS_VALIDOS": 5000,
                "NR_ZONA": "100",
            },
        ]
    )
    original_service = main_module.service
    try:
        service = AnalyticsService(dataframe=custom_df, default_top_n=20, max_top_n=100)

        def _raise_geo_failure(**_kwargs):
            raise RuntimeError("geo failed")

        def _raise_zone_geo_failure(**_kwargs):
            raise RuntimeError("zone geo failed")

        service._resolve_municipality_coords = _raise_geo_failure  # type: ignore[method-assign]
        service._resolve_zone_geometry = _raise_zone_geo_failure  # type: ignore[method-assign]
        main_module.service = service
        response = client.get(
            "/v1/candidates/444/vote-map",
            params={"level": "zona", "ano": 2024, "uf": "SP", "municipio": "SAO PAULO", "cargo": "Prefeito", "turno": 1},
        )
    finally:
        main_module.service = original_service

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == []
    assert payload["metadata"]["no_data"] is True


def test_candidate_vote_map_endpoint_municipio_level(client: TestClient):
    response = client.get(
        "/v1/candidates/9/vote-map",
        params={"level": "municipio", "year": 2022, "office": "Presidente"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["candidate_id"] == "9"
    assert payload["level"] == "municipio"
    by_city = {item["municipio"]: item["votes"] for item in payload["items"]}
    assert by_city["SAO PAULO"] == 70000
    assert by_city["RIO DE JANEIRO"] == 80000
    assert payload["quantile_q1"] >= 70000
    assert payload["quantile_q2"] >= payload["quantile_q1"]


def test_candidates_compare_endpoint(client: TestClient):
    response = client.get(
        "/v1/candidates/compare",
        params=[("candidate_ids", "1"), ("candidate_ids", "2"), ("year", 2022), ("state", "SP"), ("office", "Deputado Estadual")],
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["context"]["year"] == 2022
    assert len(payload["candidates"]) == 2
    by_id = {item["candidate_id"]: item for item in payload["candidates"]}
    assert by_id["1"]["votes"] == 12000
    assert by_id["2"]["votes"] == 8000
    metric_map = {item["metric"]: item for item in payload["deltas"]}
    assert metric_map["votes"]["best_candidate_id"] == "1"
    assert metric_map["votes"]["gap_to_second"] == 4000.0


def test_candidates_compare_endpoint_accepts_csv_format(client: TestClient):
    response = client.get(
        "/v1/candidates/compare",
        params={"candidate_ids": "1,2", "year": 2022, "state": "SP", "office": "Deputado Estadual"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["candidates"]) == 2


def test_candidates_compare_openapi_uses_form_and_explode_false(client: TestClient):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    payload = response.json()
    params = payload["paths"]["/v1/candidates/compare"]["get"]["parameters"]
    candidate_ids_param = next(item for item in params if item["name"] == "candidate_ids")
    assert candidate_ids_param["style"] == "form"
    assert candidate_ids_param["explode"] is False


def test_candidate_zone_fidelity_retention_uses_previous_election_and_geometry_fallback(client: TestClient):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2020,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 99,
                "NM_CANDIDATO": "Candidato Z",
                "NR_ZONA": "700",
                "NM_ZONA": "Zona 700",
                "NM_UE": "SAO PAULO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000,
                "LATITUDE": -23.55,
                "LONGITUDE": -46.63,
            },
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 99,
                "NM_CANDIDATO": "Candidato Z",
                "NR_ZONA": "700",
                "NM_ZONA": "Zona 700",
                "NM_UE": "SAO PAULO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 900,
                "LATITUDE": -23.55,
                "LONGITUDE": -46.63,
            },
        ]
    )
    original_service = main_module.service
    try:
        main_module.service = AnalyticsService(dataframe=custom_df, default_top_n=20, max_top_n=100)
        response = client.get(
            "/v1/candidates/99/zone-fidelity",
            params={"year": 2024, "state": "SP", "office": "Prefeito", "include_geometry": "true"},
        )
    finally:
        main_module.service = original_service

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["retention"] == 90.0
    assert payload["items"][0]["geometry"]["type"] == "Point"
    assert payload["items"][0]["lat"] != 0.0 or payload["items"][0]["lng"] != 0.0


def test_candidate_zone_fidelity_include_geometry_uses_zone_centroid_when_lat_lng_missing(client: TestClient):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 123,
                "NM_CANDIDATO": "Candidato Geo",
                "NR_ZONA": "701",
                "NM_ZONA": "Zona 701",
                "NM_UE": "SAO PAULO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1500,
            }
        ]
    )
    original_service = main_module.service
    try:
        service = AnalyticsService(dataframe=custom_df, default_top_n=20, max_top_n=100)
        service._resolve_zone_geometry = lambda **_: {  # type: ignore[method-assign]
            "type": "Polygon",
            "coordinates": [[[-46.70, -23.60], [-46.60, -23.60], [-46.60, -23.50], [-46.70, -23.50], [-46.70, -23.60]]],
        }
        main_module.service = service
        response = client.get(
            "/v1/candidates/123/zone-fidelity",
            params={"year": 2024, "state": "SP", "office": "Prefeito", "include_geometry": "true"},
        )
    finally:
        main_module.service = original_service

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["geometry"]["type"] == "Polygon"
    assert payload["items"][0]["lat"] != 0.0 or payload["items"][0]["lng"] != 0.0


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


def test_polarizacao_supports_legacy_map_mode_and_ano(client: TestClient):
    custom_df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SG_PARTIDO": "PT",
                "QT_VOTOS_NOMINAIS_VALIDOS": 500000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "RJ",
                "NM_UE": "NITEROI",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SG_PARTIDO": "MDB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 210000,
            },
        ]
    )
    original_service = main_module.service
    try:
        main_module.service = AnalyticsService(dataframe=custom_df, default_top_n=20, max_top_n=100)
        federal = client.get(
            "/v1/analytics/polarizacao",
            params={"ano": 2022, "map_mode": "stateByGovernor"},
        )
        municipal_brasil = client.get(
            "/v1/analytics/polarizacao",
            params={"ano": 2024, "map_mode": "municipalityByMayor"},
        )
        municipal_uf = client.get(
            "/v1/analytics/polarizacao",
            params={"ano": 2024, "uf": "SP", "map_mode": "municipalityByMayor"},
        )
    finally:
        main_module.service = original_service

    assert federal.status_code == 200
    federal_payload = federal.json()
    assert len(federal_payload["federal"]) == 1
    assert federal_payload["municipal_brasil"] == []
    assert federal_payload["municipal_uf"] == []
    assert federal_payload["federal"][0]["ano"] == 2022

    assert municipal_brasil.status_code == 200
    municipal_brasil_payload = municipal_brasil.json()
    assert municipal_brasil_payload["federal"] == []
    assert len(municipal_brasil_payload["municipal_brasil"]) == 2
    assert municipal_brasil_payload["municipal_uf"] == []
    assert all(item["ano"] == 2024 for item in municipal_brasil_payload["municipal_brasil"])

    assert municipal_uf.status_code == 200
    municipal_uf_payload = municipal_uf.json()
    assert municipal_uf_payload["federal"] == []
    assert municipal_uf_payload["municipal_brasil"] == []
    assert len(municipal_uf_payload["municipal_uf"]) == 1
    assert municipal_uf_payload["municipal_uf"][0]["municipio"] == "SAO PAULO"
    assert municipal_uf_payload["municipal_uf"][0]["ano"] == 2024


def test_turno_validation_returns_422(client: TestClient):
    response = client.get("/v1/analytics/overview", params={"ano": 2024, "turno": 3})
    assert response.status_code == 422
    assert response.json()["message"] == "Parametros de consulta invalidos."
