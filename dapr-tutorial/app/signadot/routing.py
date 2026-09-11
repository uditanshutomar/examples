"""Validate opaque Signadot context and forward it without rewriting it."""
from collections.abc import Mapping
import re


class RoutingError(ValueError):
    """No safe routing decision can be made from the supplied context."""


ROUTING_HEADERS = {"baggage", "tracestate", "traceparent"}
# Only these can carry sd-routing-key, so only these can be *ambiguous* about it.
# traceparent is forwarded but never parsed for a key, so a duplicated traceparent --
# a misconfigured proxy, or two tracing agents -- must not fail the request. It used to,
# and on a subscriber that meant a DROP: a healthy message dead-lettered.
KEY_CARRIER_HEADERS = {"baggage", "tracestate"}


def routing_key(headers: Mapping[str, str]) -> str | None:
    normalized = {}
    for name, value in headers.items():
        lower = name.lower()
        # Only the carrier headers are inspected. A repeated Cookie,
        # X-Forwarded-For or Via is ordinary in HTTP/2 and behind proxies, and
        # must not fail a request whose routing context is unambiguous -- on a
        # subscriber that rejection became a DROP, dead-lettering a good message.
        if lower not in KEY_CARRIER_HEADERS:
            continue
        if lower in normalized:
            raise RoutingError("duplicate case-insensitive routing header")
        if not isinstance(value, str) or re.search(r"[\x00-\x1f\x7f]", value):
            raise RoutingError("invalid HTTP header value")
        normalized[lower] = value
    found = []
    for name in ("baggage", "tracestate"):
        members = []
        for item in normalized.get(name, "").split(","):
            head = item.split(";", 1)[0].strip() if name == "baggage" else item.strip()
            key, separator, value = head.partition("=")
            if key.strip() != "sd-routing-key":
                continue
            value = value.strip()
            if not separator or not value or len(value) > 4096 or re.search(r"[\s;,\x00-\x1f\x7f]", value):
                raise RoutingError("invalid sd-routing-key")
            members.append(value)
        if len(members) > 1:
            raise RoutingError("multiple routing keys in one header")
        found.extend(members)
    if len(set(found)) > 1:
        raise RoutingError("baggage and tracestate disagree on routing key")
    return found[0] if found else None


def routing_headers(headers: Mapping[str, str]) -> dict[str, str]:
    routing_key(headers)
    # In particular, never forward dapr-app-id: it overrides the native URL ID.
    return {name.lower(): value for name, value in headers.items() if name.lower() in ROUTING_HEADERS}


def routing_key_from_event(event: Mapping, headers: Mapping[str, str]) -> str | None:
    envelope = {name: event[name] for name in ROUTING_HEADERS if name in event}
    event_key, header_key = routing_key(envelope), routing_key(headers)
    if event_key is not None and header_key is not None and event_key != header_key:
        raise RoutingError("CloudEvent and delivery headers disagree on routing key")
    return event_key if event_key is not None else header_key
