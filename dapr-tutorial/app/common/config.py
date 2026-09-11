"""Runtime settings and the bootstrap's ConfigMap-backed native-ID registry."""
from dataclasses import dataclass
import json
import os
from pathlib import Path

from signadot.routes_api import AppIDs, Workload, unique_json_fields
from signadot.routing import RoutingError, routing_key


@dataclass(frozen=True)
class Settings:
    app_id: str
    baseline_name: str
    namespace: str
    sandbox: str = ""
    pod: str = "local"
    config_path: str = "/etc/tutorial/config.json"
    routes_url: str = "http://routeserver.signadot.svc:7778"
    dapr_url: str = "http://127.0.0.1:3500"
    redis_url: str = "redis://redis:6379/0"
    pubsub: str = "pubsub"
    topic: str = "orders"
    discount_percent: int = 0
    fulfillment: str = "standard"
    frontend_name: str = "frontend"
    checkout_name: str = "checkout"
    processor_name: str = "order-processor"
    console_variant: str = "baseline"
    routes_refresh: float = 2.0
    routes_max_age: float = 15.0

    @classmethod
    def from_env(cls, baseline_name):
        return cls(
            app_id=os.environ.get("DAPR_APP_ID", baseline_name),
            baseline_name=os.environ.get("BASELINE_NAME", baseline_name),
            namespace=os.environ.get("BASELINE_NAMESPACE", "dapr-tutorial"),
            sandbox=os.environ.get("SIGNADOT_SANDBOX_NAME", ""),
            pod=os.environ.get("HOSTNAME", "local"),
            config_path=os.environ.get("TUTORIAL_CONFIG", cls.config_path),
            routes_url=os.environ.get("ROUTESERVER_ADDR", cls.routes_url),
            dapr_url=os.environ.get("DAPR_URL", "http://127.0.0.1:" + os.environ.get("DAPR_HTTP_PORT", "3500")),
            redis_url=os.environ.get("REDIS_URL", cls.redis_url),
            pubsub=os.environ.get("PUBSUB_NAME", "pubsub"),
            topic=os.environ.get("ORDERS_TOPIC", "orders"),
            discount_percent=int(os.environ.get("DISCOUNT_PERCENT", os.environ.get("CHECKOUT_DISCOUNT_PERCENT", "0"))),
            fulfillment=os.environ.get("FULFILLMENT_MODE", "standard"),
            frontend_name=os.environ.get("FRONTEND_BASELINE_NAME", "frontend"),
            checkout_name=os.environ.get("CHECKOUT_BASELINE_NAME", "checkout"),
            processor_name=os.environ.get("PROCESSOR_BASELINE_NAME", "order-processor"),
            console_variant=os.environ.get("CONSOLE_VARIANT", "baseline"),
            routes_refresh=float(os.environ.get("ROUTES_REFRESH_SECONDS", "2")),
            routes_max_age=float(os.environ.get("ROUTES_MAX_AGE_SECONDS", "15")),
        )

    @property
    def workload(self):
        return Workload("Deployment", self.namespace, self.baseline_name)

    @property
    def identity(self):
        return {"app_id": self.app_id, "sandbox": self.sandbox or "baseline", "pod": self.pod,
                "variant": self.console_variant}

    @property
    def routed_workloads(self):
        """Baselines this Pod resolves through the Routes API before invoking.

        The console is deliberately absent: a browser reaches it over ordinary
        HTTP, so Signadot's own routing selects the console fork and no
        application needs an app-ID mapping for it.
        """
        return (self.checkout_name, self.processor_name)

    @property
    def dead_letter_topic(self):
        return os.environ.get("DEAD_LETTER_TOPIC", "orders-dlq-" + self.app_id)


def read_config(settings):
    # Read on demand: Kubernetes projects ConfigMap updates atomically. No subPath mount.
    try:
        document = json.loads(Path(settings.config_path).read_text(), object_pairs_hook=unique_json_fields)
    except (OSError, ValueError) as exc:
        raise RoutingError(f"tutorial config is unavailable or invalid: {exc}") from exc
    ids = AppIDs(document)
    if document.get("namespace") != settings.namespace:
        raise RoutingError("tutorial config namespace disagrees with this Pod")
    for name in settings.routed_workloads:
        ids.target(Workload("Deployment", settings.namespace, name))
    contexts = document.get("contexts")
    if not isinstance(contexts, list):
        raise RoutingError("tutorial config requires a contexts array")
    seen = set()
    for context in contexts:
        if (not isinstance(context, dict) or not isinstance(context.get("id"), str)
            or not context["id"] or not isinstance(context.get("label"), str)
            or not context["label"] or context["id"] in seen or "routingKey" not in context):
            raise RoutingError("tutorial context requires unique id, label and routingKey")
        key = context["routingKey"]
        if key is not None:
            if not isinstance(key, str) or routing_key({"baggage": "sd-routing-key=" + key}) != key:
                raise RoutingError("tutorial context has an invalid routingKey")
        seen.add(context["id"])
    return document, ids
