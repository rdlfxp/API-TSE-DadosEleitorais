import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics_service import AnalyticsService
from app.services.duckdb_analytics_service import DuckDBAnalyticsService
from app.services import r2_bootstrap as bootstrap


@pytest.fixture
def historical_client():
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2000,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 100,
                "NM_CANDIDATO": "Prefeito 2000",
                "SG_PARTIDO": "AAA",
                "QT_VOTOS_NOMINAIS_VALIDOS": 100000,
            },
            {
                "ANO_ELEICAO": 2018,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Presidente",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 200,
                "NM_CANDIDATO": "Presidente 2018",
                "SG_PARTIDO": "BBB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 200000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "CE",
                "NM_UE": "FORTALEZA",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 300,
                "NM_CANDIDATO": "Prefeito 2024",
                "SG_PARTIDO": "CCC",
                "QT_VOTOS_NOMINAIS_VALIDOS": 300000,
            },
        ]
    )

    with main_module.ANALYTICS_CACHE_LOCK:
        main_module.ANALYTICS_CACHE.clear()
    with TestClient(main_module.app) as test_client:
        main_module.service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
        yield test_client
    main_module.service = None
    with main_module.ANALYTICS_CACHE_LOCK:
        main_module.ANALYTICS_CACHE.clear()


def test_filter_options_exposes_historical_years_from_loaded_dataset(historical_client: TestClient):
    response = historical_client.get("/v1/analytics/filtros")
    assert response.status_code == 200
    payload = response.json()
    assert payload["anos"] == [2000, 2018, 2024]


def test_overview_accepts_year_alias_and_matches_ano(historical_client: TestClient):
    by_ano = historical_client.get(
        "/v1/analytics/overview",
        params={"ano": 2024, "turno": 2, "cargo": "Prefeito", "uf": "CE"},
    )
    by_year = historical_client.get(
        "/v1/analytics/overview",
        params={"year": 2024, "turno": 2, "cargo": "Prefeito", "uf": "CE"},
    )

    assert by_ano.status_code == 200
    assert by_year.status_code == 200
    assert by_ano.json() == by_year.json()


def test_overview_distinguishes_2000_2018_2024(historical_client: TestClient):
    values = {}
    for year in (2000, 2018, 2024):
        response = historical_client.get("/v1/analytics/overview", params={"year": year})
        assert response.status_code == 200
        payload = response.json()
        assert payload["total_registros"] > 0
        values[year] = payload["total_votos_nominais"]

    assert values[2000] != values[2018]
    assert values[2018] != values[2024]


def test_distribution_accepts_year_alias(historical_client: TestClient):
    response = historical_client.get(
        "/v1/analytics/distribuicao",
        params={"group_by": "cargo", "year": 2018},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["group_by"] == "cargo"
    assert any(item["label"] == "Presidente" for item in payload["items"])


def test_candidate_vote_history_prefers_cpf_as_person_identity():
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2018,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "BRASIL",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "NÃO ELEITO",
                "SQ_CANDIDATO": 10,
                "NR_CANDIDATO": 45,
                "NR_CPF_CANDIDATO": "12345678901",
                "NM_CANDIDATO": "Candidato Antigo",
                "NM_URNA_CANDIDATO": "Candidato Antigo",
                "DT_NASCIMENTO": "01/01/1980",
                "SG_PARTIDO": "AAA",
                "QT_VOTOS_NOMINAIS_VALIDOS": 100,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "BRASIL",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 20,
                "NR_CANDIDATO": 99,
                "NR_CPF_CANDIDATO": "12345678901",
                "NM_CANDIDATO": "Candidato Novo",
                "NM_URNA_CANDIDATO": "Candidato Novo",
                "DT_NASCIMENTO": "02/02/1990",
                "SG_PARTIDO": "BBB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 200,
            },
        ]
    )

    service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    result = service.candidate_vote_history(candidate_id="10", state="SP", office="Prefeito")

    assert result["person_id"] is not None
    assert result["canonical_candidate_id"] == result["person_id"]
    assert len(result["items"]) == 2
    assert [item["year"] for item in result["items"]] == [2022, 2018]
    assert result["nr_cpf_candidato"] == "12345678901"


def test_candidate_vote_history_does_not_expand_identity_without_cpf():
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 900,
                "NR_CANDIDATO": 45,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 510000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 800,
                "NR_CANDIDATO": 13123,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PSB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 220000,
            },
        ]
    )

    service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    result = service.candidate_vote_history(candidate_id="900", state="SP", office="Prefeito")

    assert result["nr_cpf_candidato"] is None
    assert result["canonical_candidate_id"].startswith("person:")
    assert result["person_id"].startswith("person:")
    assert [item["year"] for item in result["items"]] == [2024, 2022]


def test_candidate_vote_history_collapses_duplicate_sources_with_same_year():
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 900,
                "NR_CANDIDATO": 45,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 510000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 901,
                "NR_CANDIDATO": 46,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 800,
                "NR_CANDIDATO": 13123,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PSB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 220000,
            },
        ]
    )

    service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
    result = service.candidate_vote_history(candidate_id="900", state="SP", office="Prefeito")

    assert [item["year"] for item in result["items"]] == [2024, 2022]
    assert result["items"][0]["votes"] == 511000
    assert len({item["year"] for item in result["items"]}) == len(result["items"])


def test_duckdb_candidate_vote_history_accepts_candidate_cpf(tmp_path):
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 900,
                "NR_CANDIDATO": 45,
                "NR_CPF_CANDIDATO": "12345678901",
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 510000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 800,
                "NR_CANDIDATO": 13123,
                "NR_CPF_CANDIDATO": "12345678901",
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PSB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 220000,
            },
        ]
    )
    csv_path = tmp_path / "duckdb_vote_history.csv"
    df.to_csv(csv_path, index=False)

    service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
        materialize_table=True,
        database_path=str(tmp_path / "test.duckdb"),
    )
    try:
        result = service.candidate_vote_history(
            candidate_id="900",
            candidate_cpf="12345678901",
            state="SP",
            office="Prefeito",
        )
    finally:
        service.close()

    assert result["nr_cpf_candidato"] == "12345678901"
    assert [item["year"] for item in result["items"]] == [2024, 2022]


def test_duckdb_candidate_vote_history_collapses_duplicate_sources_with_same_year(tmp_path):
    df = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 900,
                "NR_CANDIDATO": 45,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 510000,
            },
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 901,
                "NR_CANDIDATO": 46,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PL",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1000,
            },
            {
                "ANO_ELEICAO": 2022,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Governador",
                "DS_SIT_TOT_TURNO": "NAO ELEITO",
                "SQ_CANDIDATO": 800,
                "NR_CANDIDATO": 13123,
                "NM_CANDIDATO": "FÁBIO ROGÉRIO CANDIDO",
                "NM_URNA_CANDIDATO": "FABIO CANDIDO",
                "DT_NASCIMENTO": "01/02/1980",
                "SG_PARTIDO": "PSB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 220000,
            },
        ]
    )
    csv_path = tmp_path / "duckdb_vote_history_dup.csv"
    df.to_csv(csv_path, index=False)

    service = DuckDBAnalyticsService.from_file(
        file_path=str(csv_path),
        default_top_n=20,
        max_top_n=100,
        materialize_table=True,
        database_path=str(tmp_path / "test_dup.duckdb"),
    )
    try:
        result = service.candidate_vote_history(candidate_id="900", state="SP", office="Prefeito")
    finally:
        service.close()

    assert [item["year"] for item in result["items"]] == [2024, 2022]
    assert result["items"][0]["votes"] == 511000
    assert len({item["year"] for item in result["items"]}) == len(result["items"])


class _FakeR2Client:
    def __init__(self, available_keys: set[str]):
        self.available_keys = available_keys
        self.downloaded: list[str] = []

    def download_file(self, _bucket: str, object_key: str, destination: str) -> None:
        from pathlib import Path

        self.downloaded.append(object_key)
        if object_key not in self.available_keys:
            raise RuntimeError("missing key")
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"dummy")


def test_r2_bootstrap_downloads_parquet_only(tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap.settings, "r2_account_id", "acc", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_access_key_id", "key", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_secret_access_key", "secret", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_bucket", "bucket", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_object_key_parquet", "latest/analytics.parquet", raising=False)

    client = _FakeR2Client(available_keys={"latest/analytics.parquet"})
    monkeypatch.setattr(bootstrap, "_build_client", lambda: client)

    preferred = tmp_path / "data" / "analytics.parquet"
    chosen = bootstrap.ensure_local_analytics_from_r2(preferred, prefer_parquet=True)

    assert chosen == preferred
    assert chosen.exists()
    assert client.downloaded == ["latest/analytics.parquet"]

def test_lifespan_loads_from_r2_bootstrap_when_local_missing(tmp_path, monkeypatch):
    downloaded_parquet = tmp_path / "downloaded" / "analytics.parquet"
    downloaded_parquet.parent.mkdir(parents=True, exist_ok=True)
    downloaded_parquet.write_bytes(b"dummy-parquet")

    monkeypatch.setattr(main_module.settings, "analytics_data_path", str(tmp_path / "missing" / "analytics.parquet"), raising=False)
    monkeypatch.setattr(main_module.settings, "analytics_engine", "duckdb", raising=False)
    monkeypatch.setattr(main_module, "ensure_local_analytics_from_r2", lambda *_args, **_kwargs: downloaded_parquet)
    monkeypatch.setattr(
        main_module.DuckDBAnalyticsService,
        "from_file",
        classmethod(lambda cls, file_path, **_kwargs: type("FakeDuckDBService", (), {"close": lambda self: None})()),
    )

    with main_module.ANALYTICS_CACHE_LOCK:
        main_module.ANALYTICS_CACHE.clear()
    main_module.service = None

    with TestClient(main_module.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["data_loaded"] is True

    main_module.service = None


def test_lifespan_prefers_r2_even_when_local_csv_exists(tmp_path, monkeypatch):
    local_csv = tmp_path / "data" / "analytics.csv"
    local_csv.parent.mkdir(parents=True, exist_ok=True)
    local_csv.write_text("ANO_ELEICAO,SG_UF,DS_CARGO\n2000,SP,Prefeito\n", encoding="utf-8")

    downloaded_parquet = tmp_path / "downloaded" / "analytics.parquet"
    downloaded_parquet.parent.mkdir(parents=True, exist_ok=True)
    downloaded_parquet.write_bytes(b"dummy-parquet")

    monkeypatch.setattr(main_module.settings, "analytics_data_path", str(local_csv), raising=False)
    monkeypatch.setattr(main_module.settings, "analytics_engine", "duckdb", raising=False)
    monkeypatch.setattr(main_module, "ensure_local_analytics_from_r2", lambda *_args, **_kwargs: downloaded_parquet)
    loaded_paths: list[str] = []
    monkeypatch.setattr(
        main_module.DuckDBAnalyticsService,
        "from_file",
        classmethod(
            lambda cls, file_path, **_kwargs: loaded_paths.append(file_path) or type(
                "FakeDuckDBService",
                (),
                {"close": lambda self: None},
            )(),
        ),
    )

    with main_module.ANALYTICS_CACHE_LOCK:
        main_module.ANALYTICS_CACHE.clear()
    main_module.service = None

    with TestClient(main_module.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["data_loaded"] is True

    assert loaded_paths == [str(downloaded_parquet)]

    main_module.service = None


def test_lifespan_ignores_local_csv_when_parquet_is_missing(tmp_path, monkeypatch):
    local_csv = tmp_path / "data" / "analytics.csv"
    local_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2020,
                "NR_TURNO": 1,
                "SG_UF": "CE",
                "NM_UE": "FORTALEZA",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 2,
                "NM_CANDIDATO": "B",
                "SG_PARTIDO": "BBB",
                "QT_VOTOS_NOMINAIS_VALIDOS": 20,
            }
        ]
    ).to_csv(local_csv, index=False)

    monkeypatch.setattr(main_module.settings, "analytics_data_path", str(tmp_path / "data" / "analytics.parquet"), raising=False)
    monkeypatch.setattr(main_module.settings, "analytics_engine", "duckdb", raising=False)
    monkeypatch.setattr(main_module, "ensure_local_analytics_from_r2", lambda *_args, **_kwargs: None)

    with main_module.ANALYTICS_CACHE_LOCK:
        main_module.ANALYTICS_CACHE.clear()
    main_module.service = None

    with TestClient(main_module.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["data_loaded"] is False

    main_module.service = None
