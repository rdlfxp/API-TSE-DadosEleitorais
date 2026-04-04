from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from app.core.config import settings
from app.infra.metrics import METRICS, METRICS_LOCK


router = APIRouter()


@router.get("/health")
def health() -> dict:
    import app.main as main_module

    return {"status": "ok", "data_loaded": main_module.service is not None, "version": settings.app_version}


@router.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    with METRICS_LOCK:
        lines = [
            "# TYPE requests_total counter",
            f"requests_total {METRICS['requests_total']}",
            "# TYPE requests_4xx_total counter",
            f"requests_4xx_total {METRICS['requests_4xx_total']}",
            "# TYPE requests_5xx_total counter",
            f"requests_5xx_total {METRICS['requests_5xx_total']}",
            "# TYPE last_request_duration_ms gauge",
            f"last_request_duration_ms {METRICS['last_request_duration_ms']}",
        ]
    return "\n".join(lines) + "\n"
