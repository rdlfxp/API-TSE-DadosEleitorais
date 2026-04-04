from __future__ import annotations

from importlib import import_module

__all__ = ["AnalyticsService", "DuckDBAnalyticsService"]


def __getattr__(name: str):
    if name == "AnalyticsService":
        return import_module("app.services.analytics.engine_pandas").AnalyticsService
    if name == "DuckDBAnalyticsService":
        return import_module("app.services.analytics.engine_duckdb").DuckDBAnalyticsService
    raise AttributeError(name)
