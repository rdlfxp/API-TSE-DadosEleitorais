from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "MeuCandidato Analytics API"
    app_version: str = "1.0.0"
    analytics_engine: str = "duckdb"
    analytics_data_path: str = "data/curated/analytics.parquet"
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
    rate_limit_enabled: bool = True
    rate_limit_window_seconds: int = 60
    rate_limit_max_requests_per_ip: int = 120
    analytics_cache_ttl_seconds: int = 60
    analytics_top_candidates_cache_ttl_seconds: int = 45
    analytics_search_cache_ttl_seconds: int = 15552000
    analytics_vote_history_cache_ttl_seconds: int = 31536000
    analytics_cache_max_entries: int = 256
    analytics_cache_max_total_mb: int = 16
    analytics_cache_max_entry_kb: int = 256
    analytics_cache_log_metrics: bool = True
    analytics_broad_query_max_page_size: int = 50
    duckdb_materialize_table: bool = False
    duckdb_create_indexes: bool = False
    duckdb_memory_limit_mb: int = 384
    duckdb_threads: int = 1
    duckdb_database_path: str = "/tmp/meucandidato_analytics.duckdb"
    r2_connect_timeout_seconds: int = 5
    r2_read_timeout_seconds: int = 30
    redis_url: str | None = None
    redis_enabled: bool = False


settings = Settings()
