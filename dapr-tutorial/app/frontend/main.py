"""Order console backend: resolve each workload, then invoke its native Dapr ID."""
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from common.config import Settings, read_config
from common.dapr import DaprClient
from common.models import OrderInput
from common.service import add_diagnostics, client_lifespan, install_errors, passthrough, request_context
from signadot.dapr import SignadotDaprClient
from signadot.routes_api import RoutesClient, Workload
from signadot.routing import routing_key


def create_app(settings=None, *, routes=None, dapr=None):
    settings = settings or Settings.from_env("frontend")
    routes, dapr = routes or RoutesClient(settings.routes_url), dapr or DaprClient(settings.dapr_url)
    integration = SignadotDaprClient(routes, dapr, lambda: read_config(settings)[1],
                                     max_age=settings.routes_max_age,
                                     refresh_seconds=settings.routes_refresh)
    app = FastAPI(title="Dapr + Signadot order console", lifespan=client_lifespan(routes, dapr))
    install_errors(app)
    add_diagnostics(app, settings, routes, dapr)

    @app.get("/api/contexts")
    async def contexts(request: Request):
        document, _ = read_config(settings)
        # A preview URL injects the routing key upstream of this Pod. Report it so
        # the console can follow the context it was actually opened in, instead of
        # offering a choice that the hosted endpoint would silently override.
        return {"namespace": document["namespace"], "contexts": document["contexts"],
                "workloads": document["workloads"], "servedBy": settings.identity,
                "requestRoutingKey": routing_key(request_context(request))}

    async def invoke(name, path, request, *, verb="GET", order=None):
        response = await integration.invoke(Workload("Deployment", settings.namespace, name), path,
                                            headers=request_context(request), verb=verb, json=order)
        return passthrough(response)

    @app.post("/api/orders")
    async def place_order(order: OrderInput, request: Request):
        return await invoke(settings.checkout_name, "/orders", request, verb="POST", order=order.model_dump())

    @app.get("/api/processed")
    async def processed(request: Request):
        return await invoke(settings.processor_name, "/processed", request)

    static = Path(__file__).parent / "static"

    @app.get("/")
    async def index():
        return FileResponse(static / "index.html")

    app.mount("/static", StaticFiles(directory=static, check_dir=False), name="static")
    return app


app = create_app()
