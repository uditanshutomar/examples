"""Reusable Signadot routing for Dapr applications.

Three pieces, usable independently:

* ``SignadotDaprClient`` — invoke a logical workload and let Signadot's routing
  rules choose the native Dapr app ID, and publish CloudEvents that carry the
  routing context forward.
* ``SubscriptionGuard`` — decide whether *this* subscriber owns a message before
  its business handler interprets the payload.
* ``RoutesClient`` / ``RouteCache`` — the Routes API adapter underneath both.

The routing contract in one line: an absent key is baseline; a key the map
carries that does not fork this workload is baseline; a key the map never
carried raises :class:`RoutingError` rather than being guessed as baseline.
See the README for a worked example and the reasoning behind that last
case.

Everything here is transport-agnostic about your business payloads: the helpers
handle routing context, target selection and ownership, and never inspect or
require a particular request or event body.
"""

from signadot.dapr import SignadotDaprClient, cloud_event, validate_cloud_event
from signadot.routes_api import (
    AppIDs,
    Resolution,
    RouteCache,
    RoutesClient,
    Rules,
    Snapshot,
    Workload,
    parse_rules,
)
from signadot.routing import (
    RoutingError,
    routing_headers,
    routing_key,
    routing_key_from_event,
)
from signadot.subscription import SubscriptionGuard

__all__ = [
    "AppIDs",
    "Resolution",
    "RouteCache",
    "RoutesClient",
    "RoutingError",
    "Rules",
    "SignadotDaprClient",
    "Snapshot",
    "SubscriptionGuard",
    "Workload",
    "cloud_event",
    "parse_rules",
    "routing_headers",
    "routing_key",
    "routing_key_from_event",
    "validate_cloud_event",
]
