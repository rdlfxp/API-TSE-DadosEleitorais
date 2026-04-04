import json

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.dependencies import trace_id_from_request
from app.api.errors import build_error_payload
from app.core.logging import logger


async def http_exception_handler(request: Request, exc: HTTPException):
    message = exc.detail if isinstance(exc.detail, str) else "Erro na requisicao."
    return JSONResponse(
        status_code=exc.status_code,
        content=build_error_payload(exc.status_code, message, trace_id_from_request(request)),
    )


async def validation_exception_handler(request: Request, __: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=build_error_payload(422, "Parametros de consulta invalidos.", trace_id_from_request(request)),
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    trace_id = trace_id_from_request(request)
    logger.exception(json.dumps({"event": "unhandled_exception", "trace_id": trace_id, "path": request.url.path, "error_type": exc.__class__.__name__, "error_message": str(exc)}, ensure_ascii=False))
    return JSONResponse(
        status_code=500,
        content=build_error_payload(500, "Erro interno no servidor.", trace_id),
    )


def register_exception_handlers(app) -> None:
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
