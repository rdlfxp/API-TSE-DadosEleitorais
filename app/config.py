from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "MeuCandidato Analytics API"
    app_version: str = "1.0.0"
    analytics_data_path: str = "data/curated/analytics.csv"
    analytics_encoding: str = "utf-8"
    analytics_separator: str = ","
    prefer_parquet_if_available: bool = True
    default_top_n: int = 20
    max_top_n: int = 100
    r2_account_id: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_bucket: str | None = None
    r2_object_key_csv: str = "latest/analytics.csv"
    r2_object_key_parquet: str = "latest/analytics.parquet"
    r2_endpoint: str | None = None
    r2_region_name: str = "auto"


settings = Settings()
