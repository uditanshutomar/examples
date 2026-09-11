"""Atomic Redis demonstration effect and deduplication, shared by worker replicas.

The recorded order IS the demonstration effect. This does not provide exactly
once delivery or exactly once effects in external services. State lasts only as
long as this Redis database/PVC survives, and grows until namespace cleanup.
"""
import hashlib
import json

from redis.asyncio import Redis


# One hash field is both the effect record and the deduplication marker. Redis
# serializes this script across replicas. A lost HTTP response safely redelivers.
# Versioned indexes are initialized only for an empty hash. Legacy hashes remain
# available through state(); bounded console reads require a fresh namespace.
COMMIT_ORDER = """
if redis.call('HLEN', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'schema:records', '1')
end
local attempt = redis.call('HINCRBY', KEYS[1], 'attempt:' .. ARGV[1], 1)
local previous = redis.call('HGET', KEYS[1], 'event:' .. ARGV[1])
if previous then
  return {'duplicate', previous}
end
if attempt <= tonumber(ARGV[3]) then
  return {'retry', tostring(attempt)}
end
local record = cjson.decode(ARGV[2])
record['delivery_attempt'] = attempt
local encoded = cjson.encode(record)
redis.call('HSET', KEYS[1], 'event:' .. ARGV[1], encoded)
if redis.call('HGET', KEYS[1], 'schema:records') == '1' then
  redis.call('ZADD', KEYS[2], 0, ARGV[4])
  redis.call('ZADD', KEYS[3], 0, ARGV[4])
end
return {'processed', encoded}
"""


class LegacyStoreError(RuntimeError):
    """A legacy store needs an explicit upgrade before bounded console reads."""


class OrderStore:
    def __init__(self, url, namespace, app_id, *, client=None):
        self.redis = client if client is not None else Redis.from_url(
            url, decode_responses=True, socket_timeout=3, socket_connect_timeout=3)
        # Stable logical identity: no Pod name or Pod UID in this key.
        self.key = f"tutorial:orders:{namespace}:{app_id}"
        self.index = self.key + ":records:v1:all"

    def context_index(self, routing_key):
        # JSON distinguishes a missing context from any literal string key.
        digest = hashlib.sha256(json.dumps(routing_key).encode()).hexdigest()
        return self.key + ":records:v1:context:" + digest

    async def ping(self):
        await self.redis.ping()

    async def close(self):
        await self.redis.aclose(close_connection_pool=True)

    async def commit(self, record, simulate_failures=0):
        identity = json.dumps([record["source"], record["id"]], separators=(",", ":"))
        digest = hashlib.sha256(identity.encode()).hexdigest()
        # All scores are zero: fixed-alphabet hex plus a lower-sorting separator
        # preserves the existing (processed_at, source, id) string ordering,
        # including ties. The final digest locates the original effect field.
        member = ".".join(value.encode("utf-8", "surrogatepass").hex() for value in
                          (record.get("processed_at", ""), record["source"], record["id"])) + "." + digest
        action, result = await self.redis.eval(COMMIT_ORDER, 3, self.key, self.index,
                                               self.context_index(record["routing_key"]), digest,
                                               json.dumps(record), simulate_failures, member)
        return action, int(result) if action == "retry" else json.loads(result)

    async def state(self):
        data = await self.redis.hgetall(self.key)
        records = [json.loads(value) for key, value in data.items() if key.startswith("event:")]
        records.sort(key=lambda record: (record.get("processed_at", ""), record["source"], record["id"]))
        return {"record_count": len(records), "records": records,
                "attempts": {key.removeprefix("attempt:"): int(value) for key, value in data.items()
                             if key.startswith("attempt:")},
                "key": self.key,
                "persistence": "Survives worker replacement while this Redis database/PVC survives."}

    async def records(self, routing_key=None, *, all_contexts=False, limit=100):
        """Read at most limit latest records, oldest first, without scanning history.

        Indexes do not prune data or change deduplication retention. Older stores
        can still be exported with state(), but are deliberately not partially
        indexed on demand; deploy this revision in a fresh tutorial namespace.
        """
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("record limit must be an integer from 1 to 1000")
        if await self.redis.hget(self.key, "schema:records") != "1":
            if not await self.redis.hlen(self.key):
                return []
            raise LegacyStoreError(
                "This Redis ledger predates bounded read indexes. Export it through /state "
                "if needed, then deploy this tutorial revision in a fresh namespace.")
        index = self.index if all_contexts else self.context_index(routing_key)
        members = await self.redis.zrange(index, -limit, -1)
        if not members:
            return []
        fields = ["event:" + member.rsplit(".", 1)[1] for member in members]
        values = await self.redis.hmget(self.key, fields)
        return [json.loads(value) for value in values if value is not None]
