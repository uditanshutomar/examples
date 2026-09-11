"""Native Dapr HTTP calls: target the selected app ID, retaining sidecar mTLS."""
import time
from urllib.parse import quote, unquote, urlsplit

import httpx

from signadot.routing import RoutingError, routing_headers


class DaprClient:
    def __init__(self, url="http://127.0.0.1:3500", *, client=None):
        """Own one connection pool, including an optional injected test client."""
        self.url = url.rstrip("/")
        self._client = client
        self._closed = False

    def _http_client(self):
        if self._closed:
            raise RuntimeError("Dapr client is closed")
        if self._client is None:
            self._client = httpx.AsyncClient(limits=httpx.Limits(
                max_connections=100, max_keepalive_connections=20))
        return self._client

    async def close(self):
        """Release the pool once; a closed client cannot start more requests."""
        if self._closed:
            return
        self._closed = True
        if self._client is not None:
            await self._client.aclose()

    async def ready(self, expected_app_id):
        client = self._http_client()
        # Outbound health avoids a circular dependency with application readiness.
        response = await client.get(self.url + "/v1.0/healthz/outbound", timeout=3)
        response.raise_for_status()
        response = await client.get(self.url + "/v1.0/metadata", timeout=3)
        response.raise_for_status()
        actual = response.json().get("id")
        if actual != expected_app_id:
            raise RoutingError(f"sidecar app ID {actual!r} disagrees with Pod app ID {expected_app_id!r}")

    async def invoke(self, resolution, path, *, headers, verb="GET", json=None):
        if time.monotonic() >= resolution.valid_until:
            raise RoutingError("route resolution expired before native invocation")
        if not path.startswith("/") or path.startswith("//") or urlsplit(path).fragment:
            raise RoutingError("native invocation requires an application path without fragment")
        # Dot segments are resolved by the HTTP client, so "/../.." would walk
        # back out of /v1.0/invoke/<app-id>/method and silently discard the
        # routing decision, landing the call on the baseline app ID instead.
        # Check the DECODED path: httpx does not decode %2e, so "/%2e%2e/" would slip past a
        # raw-string check and be resolved by whatever decodes it downstream. The guard exists
        # precisely so the destination does not depend on someone else's normalisation.
        decoded = unquote(urlsplit(path).path).replace("\\", "/")
        if any(segment in {".", ".."} for segment in decoded.split("/")):
            raise RoutingError("native invocation path may not contain dot segments")
        prefix = self.url + "/v1.0/invoke/" + quote(resolution.app_id, safe="") + "/method"
        url = prefix + path
        # Post-normalisation invariant. The syntactic check above works on the raw string,
        # so it cannot see encoded forms (%2e%2e, ..%2f, ..;) or backslashes. Whatever the
        # client does to the URL, the request must still be addressed to the app ID that
        # was resolved -- otherwise the routing decision has been silently discarded.
        if not str(httpx.URL(url)).startswith(prefix + "/"):
            raise RoutingError("native invocation path escapes the resolved app ID")
        return await self._http_client().request(
            verb, url, headers=routing_headers(headers), json=json, timeout=15)

    async def publish(self, pubsub, topic, event, headers):
        url = self.url + "/v1.0/publish/" + quote(pubsub, safe="") + "/" + quote(topic, safe="")
        forwarded = routing_headers(headers)
        forwarded["content-type"] = "application/cloudevents+json"
        response = await self._http_client().post(url, headers=forwarded, json=event, timeout=15)
        response.raise_for_status()
