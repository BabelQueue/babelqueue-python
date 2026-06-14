"""Redis transport. Requires the ``redis`` extra:

    pip install "babelqueue[redis]"

Two reservation modes share one class, selected by ``laravel_compat``:

**Python-owned (default).** Producing is ``RPUSH queue body``; consuming atomically
moves the head to a per-queue processing list (``BLMOVE``) so an in-flight message
survives a worker crash, and ``ack`` removes it from that processing list. Simple,
self-contained, and the right choice for a queue this runtime owns end-to-end.

**Laravel-compatible (``laravel_compat=True``).** Replicates Laravel's stock Redis
queue reservation so a Python worker can share one Redis queue with a PHP/Laravel
worker without losing or double-processing messages (§1 of the broker-bindings
contract). The key layout is Laravel's: a ``queues:<name>`` ready list, a
``queues:<name>:reserved`` sorted set scored by a retry-after deadline, a
``queues:<name>:delayed`` sorted set, and a ``queues:<name>:notify`` wake-up list.
Reserve/ack/release run the **byte-for-byte same Lua scripts** Laravel uses, so the
reserved-set member a Python worker writes is identical to what a Laravel worker
writes — either side can therefore ack (``ZREM``) the other's reservation. Before
each pop the transport migrates expired reserved/delayed jobs back to the ready
list, so a crashed worker's in-flight job is re-reserved exactly as Laravel does.

The envelope is unchanged (``schema_version`` stays 1); Redis is purely additive.

URL form: ``redis://host:port/db[?laravel=1&prefix=queues:&retry_after=60]``.
For richer setups, build the transport directly and pass ``BabelQueue(transport=...)``.
"""

from __future__ import annotations

import time
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

from .transport import ReceivedMessage, Transport

# Laravel's stock Redis queue Lua scripts (Illuminate\Queue\LuaScripts). Replicated
# verbatim so the reserved-set member byte form matches Laravel's — that exact match
# is what lets a Python and a Laravel worker ack each other's reservations.

# KEYS[1]=ready list, KEYS[2]=notify list; ARGV[1]=payload.
_PUSH = """
redis.call('rpush', KEYS[1], ARGV[1])
redis.call('rpush', KEYS[2], 1)
"""

# KEYS[1]=ready, KEYS[2]=reserved, KEYS[3]=notify; ARGV[1]=reserved-until score.
# Returns {original_job, reserved_member}; the reserved member is the attempts+1
# re-encoding stored in the reserved set and is the ack handle.
_POP = """
local job = redis.call('lpop', KEYS[1])
local reserved = false

if(job ~= false) then
    reserved = cjson.decode(job)
    reserved['attempts'] = reserved['attempts'] + 1
    reserved = cjson.encode(reserved)
    redis.call('zadd', KEYS[2], ARGV[1], reserved)
    redis.call('lpop', KEYS[3])
end

return {job, reserved}
"""

# KEYS[1]=delayed, KEYS[2]=reserved; ARGV[1]=reserved member, ARGV[2]=available-at.
_RELEASE = """
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])
return true
"""

# KEYS[1]=from (reserved/delayed), KEYS[2]=ready, KEYS[3]=notify; ARGV[1]=now,
# ARGV[2]=batch size (-1 = all). Moves jobs whose score has expired back to ready.
_MIGRATE = """
local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, ARGV[2])

if(next(val) ~= nil) then
    redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)

    for i = 1, #val, 100 do
        redis.call('rpush', KEYS[2], unpack(val, i, math.min(i+99, #val)))
        for j = i, math.min(i+99, #val) do
            redis.call('rpush', KEYS[3], 1)
        end
    end
end

return val
"""


class RedisTransport(Transport):
    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        client: Any = None,
        processing_suffix: str = ":processing",
        laravel_compat: bool = False,
        key_prefix: str = "queues:",
        retry_after: int = 60,
        block_for: Optional[float] = None,
    ) -> None:
        parts = urlsplit(url) if url else urlsplit("redis://")
        q = parse_qs(parts.query)

        self._processing_suffix = processing_suffix
        self._laravel_compat = laravel_compat or _qbool(q, "laravel") or _qbool(q, "laravel_compat")
        self._key_prefix = key_prefix if "prefix" not in q else (_q1(q, "prefix") or key_prefix)
        self._retry_after = retry_after if "retry_after" not in q else _qint(q, "retry_after", retry_after)
        self._block_for = block_for

        if client is not None:
            self._redis = client
            return
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "RedisTransport requires the 'redis' package. Install with "
                'pip install "babelqueue[redis]".'
            ) from exc
        self._redis = redis.Redis.from_url(url, decode_responses=True)

    # -- key helpers --------------------------------------------------------

    def _processing(self, queue: str) -> str:
        return f"{queue}{self._processing_suffix}"

    def _ready(self, queue: str) -> str:
        """The Laravel ready-list key for a logical queue name (``queues:<name>``)."""
        return f"{self._key_prefix}{queue}"

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        if not self._laravel_compat:
            self._redis.rpush(queue, body)
            return
        ready = self._ready(queue)
        self._redis.eval(_PUSH, 2, ready, f"{ready}:notify", body)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        if not self._laravel_compat:
            return self._pop_owned(queue, timeout)
        return self._pop_laravel(queue, timeout)

    def ack(self, message: ReceivedMessage) -> None:
        if not self._laravel_compat:
            self._redis.lrem(self._processing(message.queue), 1, message.handle)
            return
        if not message.handle:
            return
        self._redis.zrem(f"{self._ready(message.queue)}:reserved", message.handle)

    def release(self, message: ReceivedMessage, delay: int = 0) -> None:
        """Return a reserved Laravel job to the (delayed) ready set for retry.

        No-op in Python-owned mode (the runtime re-publishes instead). In
        Laravel-compatible mode this is ``ZREM :reserved`` + ``ZADD :delayed`` —
        the exact bookkeeping Laravel's release performs.
        """
        if not self._laravel_compat or not message.handle:
            return
        ready = self._ready(message.queue)
        self._redis.eval(
            _RELEASE, 2, f"{ready}:delayed", f"{ready}:reserved",
            message.handle, int(time.time()) + int(delay),
        )

    def close(self) -> None:  # pragma: no cover
        self._redis.close()

    # -- Python-owned reliable queue ----------------------------------------

    def _pop_owned(self, queue: str, timeout: float) -> Optional[ReceivedMessage]:
        # redis-py types the BLMOVE timeout as int, but Redis accepts a float
        # (sub-second) timeout; passing it through is correct at runtime.
        body = self._redis.blmove(queue, self._processing(queue), timeout, "LEFT", "RIGHT")  # type: ignore[arg-type]
        if body is None:
            return None
        text = _as_text(body)
        return ReceivedMessage(body=text, queue=queue, handle=text)

    # -- Laravel-compatible reservation -------------------------------------

    def _pop_laravel(self, queue: str, timeout: float) -> Optional[ReceivedMessage]:
        ready = self._ready(queue)
        self._migrate(ready)

        result = self._redis.eval(
            _POP, 3, ready, f"{ready}:reserved", f"{ready}:notify",
            int(time.time()) + self._retry_after,
        )
        job, reserved = _pop_result(result)
        if job is None or reserved is None:
            # Nothing ready right now; optionally block on the notify list so a
            # concurrently-pushed job wakes us, then retry once (mirrors Laravel).
            block = self._block_for if self._block_for is not None else timeout
            if block and block > 0 and self._redis.blpop([f"{ready}:notify"], block):
                result = self._redis.eval(
                    _POP, 3, ready, f"{ready}:reserved", f"{ready}:notify",
                    int(time.time()) + self._retry_after,
                )
                job, reserved = _pop_result(result)
            if job is None or reserved is None:
                return None

        # The reserved member (attempts already incremented) is both the consumed
        # body and the ack handle, so ZREM removes exactly this reservation.
        return ReceivedMessage(body=_as_text(reserved), queue=queue, handle=_as_text(reserved))

    def _migrate(self, ready: str) -> None:
        now = int(time.time())
        # Delayed jobs that are due, then reserved jobs whose retry-after lapsed
        # (a crashed worker's in-flight job) — both move back to the ready list.
        self._redis.eval(_MIGRATE, 3, f"{ready}:delayed", ready, f"{ready}:notify", now, -1)
        self._redis.eval(_MIGRATE, 3, f"{ready}:reserved", ready, f"{ready}:notify", now, -1)


def _as_text(value: Any) -> str:
    """Normalise a Redis reply to ``str`` (bytes when ``decode_responses`` is off)."""
    return value if isinstance(value, str) else value.decode()


def _pop_result(result: Any) -> tuple[Optional[Any], Optional[Any]]:
    """Unpack the ``{job, reserved}`` reply; ``false`` Lua values arrive as ``None``."""
    if not result or len(result) < 2:
        return None, None
    job, reserved = result[0], result[1]
    if not job or not reserved:
        return None, None
    return job, reserved


def _q1(q: dict, key: str) -> Optional[str]:
    values = q.get(key)
    return values[0] if values else None


def _qint(q: dict, key: str, default: int) -> int:
    v = _q1(q, key)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:  # pragma: no cover - defensive
        return default


def _qbool(q: dict, key: str) -> bool:
    v = _q1(q, key)
    return v is not None and v.lower() in ("1", "true", "yes", "on")
