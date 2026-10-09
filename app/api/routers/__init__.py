from fastapi import APIRouter

from app.api.routers.analytics import router as analytics_router
from app.api.routers.candidates import router as candidates_router
from app.api.routers.health import router as health_router
from app.api.routers.live_results import router as live_results_router


api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(analytics_router)
api_router.include_router(candidates_router)
api_router.include_router(live_results_router)

__all__ = ["api_router"]
