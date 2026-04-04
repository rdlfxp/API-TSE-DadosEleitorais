from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi


def build_openapi(app: FastAPI) -> dict:
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )
    compare_op = openapi_schema.get("paths", {}).get("/v1/candidates/compare", {}).get("get", {})
    for param in compare_op.get("parameters", []):
        if param.get("name") == "candidate_ids" and param.get("in") == "query":
            param["style"] = "form"
            param["explode"] = False
            schema = param.get("schema", {})
            if isinstance(schema, dict):
                schema.pop("openapi_extra", None)

    app.openapi_schema = openapi_schema
    return openapi_schema


def attach_openapi(app: FastAPI) -> None:
    app.openapi = lambda: build_openapi(app)
