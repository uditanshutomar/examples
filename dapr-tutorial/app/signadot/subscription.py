"""Select a logical subscriber before interpreting its business payload.

Use only with independent broker deliveries per logical subscriber group.
A nonowner can acknowledge its copy; it must not acknowledge another group's
only copy on a competing queue. The registry and owners callbacks are supplied
by the application so configuration reload and snapshot freshness stay explicit.
"""
from collections.abc import Callable

from signadot.routes_api import AppIDs, Rules, Workload
from signadot.routing import RoutingError


class SubscriptionGuard:
    def __init__(self, workload: Workload, *, app_id: str, sandbox: str,
                 registry: Callable[[], AppIDs], rules: Callable[[], Rules]):
        self.workload = workload
        self.app_id = app_id
        self.sandbox = sandbox
        self.registry = registry
        self.rules = rules

    def owns(self, key: str | None) -> bool:
        """Return ownership for a validated key, or raise if it cannot be decided.

        A key the map carries but that does not fork this workload selects
        baseline. A key the map does not carry at all is a routing error, as is
        an unavailable map or an unregistered selected destination, so callers
        retry instead of silently acknowledging every copy. Letting an unknown
        key mean baseline would let one incomplete snapshot hand the baseline
        group another group's only message. An unkeyed message needs no
        Routes API snapshot.
        """
        app_ids = self.registry()
        if app_ids.target(self.workload, self.sandbox or None) != self.app_id:
            raise RoutingError("worker app ID disagrees with its configured workload identity")
        owner = self.rules().owner_for(key) if key is not None else ""
        if owner == self.sandbox:
            return True
        # Deliberately no registry lookup for the *other* group's sandbox. Whether
        # someone else owns this message does not depend on this Pod knowing their
        # Dapr app ID, and requiring it turned a correct "not mine" into a retry
        # loop that dead-lettered a healthy message whenever a sandbox created
        # after this Pod's ConfigMap was rendered appeared in the route map. The
        # owner receives its own copy in its own consumer group regardless.
        return False
