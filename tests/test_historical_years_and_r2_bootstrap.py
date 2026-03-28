import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics_service import AnalyticsService
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


def test_r2_bootstrap_prefers_parquet_then_fallback_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap.settings, "r2_account_id", "acc", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_access_key_id", "key", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_secret_access_key", "secret", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_bucket", "bucket", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_object_key_parquet", "latest/analytics.parquet", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_object_key_csv", "latest/analytics.csv", raising=False)

    client = _FakeR2Client(available_keys={"latest/analytics.parquet"})
    monkeypatch.setattr(bootstrap, "_build_client", lambda: client)

    preferred = tmp_path / "data" / "analytics.parquet"
    chosen = bootstrap.ensure_local_analytics_from_r2(preferred, prefer_parquet=True)

    assert chosen == preferred
    assert chosen.exists()
    assert client.downloaded == ["latest/analytics.parquet"]

    client_csv_only = _FakeR2Client(available_keys={"latest/analytics.csv"})
    monkeypatch.setattr(bootstrap, "_build_client", lambda: client_csv_only)

    chosen_csv = bootstrap.ensure_local_analytics_from_r2(preferred, prefer_parquet=True)
    assert chosen_csv == preferred.with_suffix(".csv")
    assert chosen_csv.exists()
    assert client_csv_only.downloaded == ["latest/analytics.parquet", "latest/analytics.csv"]


def test_r2_bootstrap_prefers_csv_when_configured(tmp_path, monkeypatch):
    monkeypatch.setattr(bootstrap.settings, "r2_account_id", "acc", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_access_key_id", "key", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_secret_access_key", "secret", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_bucket", "bucket", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_object_key_parquet", "latest/analytics.parquet", raising=False)
    monkeypatch.setattr(bootstrap.settings, "r2_object_key_csv", "latest/analytics.csv", raising=False)

    client = _FakeR2Client(available_keys={"latest/analytics.csv"})
    monkeypatch.setattr(bootstrap, "_build_client", lambda: client)

    preferred = tmp_path / "data" / "analytics.parquet"
    chosen = bootstrap.ensure_local_analytics_from_r2(preferred, prefer_parquet=False)
    assert chosen == preferred.with_suffix(".csv")
    assert chosen.exists()
    assert client.downloaded == ["latest/analytics.csv"]


def test_lifespan_loads_from_r2_bootstrap_when_local_missing(tmp_path, monkeypatch):
    downloaded_csv = tmp_path / "downloaded" / "analytics.csv"
    downloaded_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SAO PAULO",
                "DS_CARGO": "Prefeito",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "SQ_CANDIDATO": 1,
                "NM_CANDIDATO": "A",
                "SG_PARTIDO": "AAA",
                "QT_VOTOS_NOMINAIS_VALIDOS": 10,
            }
        ]
    ).to_csv(downloaded_csv, index=False)

    monkeypatch.setattr(main_module.settings, "analytics_data_path", str(tmp_path / "missing" / "analytics.parquet"), raising=False)
    monkeypatch.setattr(main_module.settings, "analytics_engine", "duckdb", raising=False)
    monkeypatch.setattr(main_module, "ensure_local_analytics_from_r2", lambda *_args, **_kwargs: downloaded_csv)

    with main_module.ANALYTICS_CACHE_LOCK:
        main_module.ANALYTICS_CACHE.clear()
    main_module.service = None

    with TestClient(main_module.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["data_loaded"] is True

    main_module.service = None


def test_lifespan_falls_back_to_local_csv_if_parquet_missing(tmp_path, monkeypatch):
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
        assert health.json()["data_loaded"] is True

    main_module.service = None
