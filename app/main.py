import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from app.api import api_router
from app.api.exception_handlers import register_exception_handlers
from app.api.middleware import register_observability_middleware
from app.core.config import settings
from app.core.logging import logger
from app.core.openapi import attach_openapi
from app.core.redis import close_redis, initialize_redis
import app.infra.cache as cache_state
import app.infra.metrics as metrics_state
import app.infra.rate_limit as rate_limit_state
from app.services.analytics import AnalyticsService, DuckDBAnalyticsService
from app.services.storage import ensure_local_analytics_from_r2

ANALYTICS_CACHE = cache_state.ANALYTICS_CACHE
ANALYTICS_CACHE_LOCK = cache_state.ANALYTICS_CACHE_LOCK
DATA_LAST_MODIFIED_TS = cache_state.DATA_LAST_MODIFIED_TS
EDGE_CACHE_TTL_BY_PATH = cache_state.EDGE_CACHE_TTL_BY_PATH
MEMORY_CACHE_TTL_BY_ENDPOINT = cache_state.MEMORY_CACHE_TTL_BY_ENDPOINT
_cache_get = cache_state.cache_get
_cache_set = cache_state.cache_set

METRICS = metrics_state.METRICS
METRICS_LOCK = metrics_state.METRICS_LOCK

RATE_LIMIT_COUNTERS = rate_limit_state.RATE_LIMIT_COUNTERS
RATE_LIMIT_LAST_CLEANUP_BUCKET = rate_limit_state.RATE_LIMIT_LAST_CLEANUP_BUCKET
RATE_LIMIT_LOCK = rate_limit_state.RATE_LIMIT_LOCK

service: AnalyticsService | DuckDBAnalyticsService | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global service, DATA_LAST_MODIFIED_TS
    service = None
    cache_state.DATA_LAST_MODIFIED_TS = None
    DATA_LAST_MODIFIED_TS = None
    initialize_redis()

    file_path = Path(settings.analytics_data_path)
    selected_path = file_path
    if settings.prefer_parquet_if_available and file_path.suffix.lower() == ".csv":
        selected_path = file_path.with_suffix(".parquet")

    downloaded_path = ensure_local_analytics_from_r2(file_path, settings.prefer_parquet_if_available)
    if downloaded_path is not None:
        selected_path = downloaded_path
        logger.info(json.dumps({"event": "startup_data_downloaded_from_r2", "path": str(selected_path)}, ensure_ascii=False))

    if selected_path.exists():
        try:
            cache_state.DATA_LAST_MODIFIED_TS = selected_path.stat().st_mtime
        except OSError:
            cache_state.DATA_LAST_MODIFIED_TS = None
        DATA_LAST_MODIFIED_TS = cache_state.DATA_LAST_MODIFIED_TS
        if settings.analytics_engine.lower() == "duckdb":
            service = DuckDBAnalyticsService.from_file(
                file_path=str(selected_path),
                default_top_n=settings.default_top_n,
                max_top_n=settings.max_top_n,
                separator=settings.analytics_separator,
                encoding=settings.analytics_encoding,
                materialize_table=settings.duckdb_materialize_table,
                create_indexes=settings.duckdb_create_indexes,
                memory_limit_mb=settings.duckdb_memory_limit_mb,
                threads=settings.duckdb_threads,
                database_path=settings.duckdb_database_path,
            )
        else:
            service = AnalyticsService.from_file(
                file_path=str(selected_path),
                default_top_n=settings.default_top_n,
                max_top_n=settings.max_top_n,
                separator=settings.analytics_separator,
                encoding=settings.analytics_encoding,
            )

        if isinstance(service, AnalyticsService):
            rows_loaded: int | None = int(len(service.dataframe))
        elif settings.duckdb_materialize_table:
            rows_loaded = int(service.row_count)
        else:
            rows_loaded = None
        logger.info(
            json.dumps(
                {
                    "event": "startup_data_loaded",
                    "path": str(selected_path),
                    "engine": settings.analytics_engine.lower(),
                    "duckdb_materialize_table": bool(settings.duckdb_materialize_table),
                    "duckdb_create_indexes": bool(settings.duckdb_create_indexes),
                    "duckdb_memory_limit_mb": int(settings.duckdb_memory_limit_mb),
                    "duckdb_threads": int(settings.duckdb_threads),
                    "duckdb_database_path": getattr(service, "_database_path", None) if isinstance(service, DuckDBAnalyticsService) else None,
                    "rows": rows_loaded,
                },
                ensure_ascii=False,
            )
        )
    else:
        logger.info(json.dumps({"event": "startup_data_missing", "path": str(selected_path), "hint": "Configure R2_* vars or provide local analytics file."}, ensure_ascii=False))

    yield

    if isinstance(service, DuckDBAnalyticsService):
        service.close()
    service = None
    cache_state.DATA_LAST_MODIFIED_TS = None
    DATA_LAST_MODIFIED_TS = None
    close_redis()


app = FastAPI(title=settings.app_name, version=settings.app_version, lifespan=lifespan)
attach_openapi(app)
register_exception_handlers(app)
register_observability_middleware(app)
app.include_router(api_router)
