"""Redis transport. Requires the ``redis`` extra:

    pip install "babelqueue[redis]"

Two layouts share one class, selected by ``laravel_compat`` (broker-bindings §1, ADR-0033):

**Legacy (default).** Producing is ``RPUSH queue body``; reserving atomically moves the head to
a per-queue ``<queue>:processing`` list so an in-flight message survives a worker crash. Because a
bare list cannot say *when* a reservation expires, every reservation also writes a member
``<token>:<raw>`` into the deadline sorted set ``<queue>:processing:deadlines`` (``token`` is 32
lowercase hex characters from ``secrets.token_hex(16)``, ``raw`` the stored value). Before every
reserve a sweep moves the members whose deadline lapsed back to the END of ``<queue>`` (body and
``attempts`` unchanged: a recovery is not an attempt here), so a ``kill -9`` no longer strands a
message in ``:processing``. Ack, release and dead-letter go through one *gated* settle script:
they act only while their own deadline member still exists, so a late ack of a reservation that
was swept and re-delivered touches nothing of the newer reservation (identical copies stay
distinct because every reservation has its own token). ``retry_after=None`` stores the score
``+inf``: the sweep never moves that reservation. The legacy layout has no delayed set, so the
``delay`` of a release/redeliver is ignored there (the message goes back to the end of the list).

**Laravel-compatible (``laravel_compat=True``, "zset").** Laravel's stock Redis queue layout: a
``queues:<name>`` ready list, ``:reserved`` and ``:delayed`` sorted sets and a ``:notify`` wake-up
list. The reserve script is Laravel's pop plus a ``pcall`` hardening: a job whose body does not
decode or whose ``attempts`` is not a number is reserved *raw* (the member equals the job) instead
of aborting the script. The handler always receives the **raw** job (Laravel's ``attempts`` is
therefore the count *before* this delivery); the ack handle is the reserved member
(``attempts + 1``), so either side can ``ZREM`` the other's reservation. A poison job is delivered
under :data:`~babelqueue.transport.NON_NUMERIC_ATTEMPTS_ROUTE`: the runtime dead-letters the raw
body byte for byte and acks it, leaving nothing in ``:reserved``. The legacy layout does the same
for a body that is not a JSON object: on its first delivery the raw body goes to ``<queue>.dlq``
through the gated settle script and leaves ``:processing`` (it is never retried or rebuilt). A failed job is returned by one
atomic script (``ZREM :reserved`` + ``ZADD :delayed floor(now)+ceil(delay)``) with ``attempts``
advanced by the runtime; the transport is a :class:`~babelqueue.transport.Redeliverer`. Before
each pop, due ``:delayed`` members and (unless ``retry_after`` is ``None``) lapsed ``:reserved``
members migrate to ready, byte for byte; a recovered reservation counts as an attempt.
:meth:`RedisTransport.publish_delayed` adds a delayed job directly.

``retry_after`` defaults to 90 seconds; ``None`` (URL ``retry_after=null``) disables recovery
(zset: the reserved score is ``+inf``) and a negative value is a configuration error.

**Out-of-band headers (ADR-0028).** In legacy mode only, ``publish_with_headers`` with a non-empty
header map stores a transport-owned JSON frame::

    {"__bq_frame": 1, "headers": {"traceparent": "00-…"}, "body": "<raw wire envelope>"}

and :meth:`~RedisTransport.pop` unframes it by the reserved ``__bq_frame`` sentinel (a frozen
envelope never carries it); plain values consume with ``headers={}``. The zset layout stores the
bare envelope (Laravel's reserve script must decode it), so headers (and therefore a W3C
``traceparent``) are dropped there, never an error (the ``trace_id`` inside the envelope is still
preserved, GR-4). This is the documented A3 limit of the Laravel-compatible layout.

The envelope is unchanged (``schema_version`` stays 1); Redis is purely additive.

URL form: ``redis://host:port/db[?laravel=1&prefix=queues:&retry_after=90]``.
For richer setups, build the transport directly and pass ``BabelQueue(transport=...)``.
"""

from __future__ import annotations

import json
import logging
import math
import secrets
import time
from typing import Any, Callable, Dict, NamedTuple, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

from ._envelope_shape import envelope_violation
from .codec import parse_envelope
from .exceptions import BabelQueueError
from .transport import NON_NUMERIC_ATTEMPTS_ROUTE, ReceivedMessage, Transport

logger = logging.getLogger("babelqueue.redis")

# The reserved header-frame discriminator key + its current schema version. A frozen wire envelope
# never carries ``__bq_frame``, so its presence is how ``pop`` tells a transport frame from a bare
# envelope without structural guessing.
_FRAME_KEY = "__bq_frame"
_FRAME_VERSION = 1

#: Default reservation lifetime in seconds (ADR-0033: 90, up from the earlier 60).
DEFAULT_RETRY_AFTER = 90

#: Upper bound of one blocking slice while waiting for work (seconds): the transport re-migrates
#: and re-checks at least this often, so a due delayed job is never starved behind a long block.
_BLOCK_SLICE = 1.0

#: Legacy sleep-poll back-off: first and maximum pause between two empty polls (seconds).
_POLL_INITIAL = 0.1
_POLL_MAX = 1.0

#: Reservation token size in bytes (hex-encoded: 32 lowercase characters).
_TOKEN_BYTES = 16

# --- Laravel (zset) layout scripts -----------------------------------------------------------

# KEYS[1]=ready list, KEYS[2]=notify list; ARGV[1]=payload.
_PUSH = """
redis.call('rpush', KEYS[1], ARGV[1])
redis.call('rpush', KEYS[2], 1)
"""

# KEYS[1]=ready, KEYS[2]=reserved, KEYS[3]=notify; ARGV[1]=reserved-until score.
# Laravel's pop with a pcall guard (ADR-0033 A1): a body that does not decode, is not an object,
# whose ``attempts`` is not a number, or whose re-encode fails (real Redis' lua-cjson refuses
# inf/NaN, e.g. ``1e400``, and over-deep nesting) is reserved RAW (member == job). Both the decode
# and the encode sit inside pcall: an unguarded encode would raise out of EVAL and leave the job
# popped but not reserved (lost). Returns {job, reserved}.
_POP = """
local job = redis.call('lpop', KEYS[1])
local reserved = false

if(job ~= false) then
    local ok, decoded = pcall(cjson.decode, job)
    if ok and type(decoded) == 'table' and type(decoded['attempts']) == 'number' then
        decoded['attempts'] = decoded['attempts'] + 1
        local eok, encoded = pcall(cjson.encode, decoded)
        if eok then
            reserved = encoded
        else
            reserved = job
        end
    else
        reserved = job
    end
    redis.call('zadd', KEYS[2], ARGV[1], reserved)
    redis.call('lpop', KEYS[3])
end

return {job, reserved}
"""

# KEYS[1]=delayed, KEYS[2]=reserved; ARGV[1]=reserved member, ARGV[2]=body to re-queue,
# ARGV[3]=available-at score. One atomic hand-back: the reservation leaves ``:reserved`` and the
# body appears in ``:delayed`` (Laravel's release; unconditional, no push+ack fallback).
_RELEASE = """
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('zadd', KEYS[1], ARGV[3], ARGV[2])
return true
"""

# KEYS[1]=reserved, KEYS[2]=dead-letter ready, KEYS[3]=dead-letter notify; ARGV[1]=reserved member
# (handle), ARGV[2]=body. Gated dead-letter: the reservation leaves ``:reserved`` and the body is
# pushed to the DLQ in ONE script, only while the member is still reserved. Returns 1 when it wrote,
# 0 when the reservation was already gone (migrated back to ready): nothing is written then, so a
# redelivery cannot leave a second copy in the DLQ.
_DEAD_LETTER = """
if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
    redis.call('rpush', KEYS[2], ARGV[2])
    redis.call('rpush', KEYS[3], 1)
    return 1
end
return 0
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

# --- Legacy layout scripts ---------------------------------------------------------------------

# KEYS[1]=ready list, KEYS[2]=processing list, KEYS[3]=deadline zset; ARGV[1]=deadline score
# (``+inf`` allowed), ARGV[2]=reservation token. Moves the head to :processing and records the
# deadline member ``<token>:<raw>``; returns the raw value (false when the queue is empty).
_RESERVE_LEGACY = """
local raw = redis.call('lmove', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
if not raw then
    return false
end
redis.call('zadd', KEYS[3], ARGV[1], ARGV[2] .. ':' .. raw)
return raw
"""

# KEYS[1]=processing, KEYS[2]=deadlines, KEYS[3]=optional target list; ARGV[1]=token,
# ARGV[2]=raw, ARGV[3]=body for the target. The deadline member is ``<token>:<raw>``, built here.
# Gated: acts only while the reservation's own deadline member exists. Returns 1 when it settled,
# 0 when it touched nothing.
_SETTLE = """
if redis.call('zrem', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
    redis.call('lrem', KEYS[1], 1, ARGV[2])
    if KEYS[3] then
        redis.call('rpush', KEYS[3], ARGV[3])
    end
    return 1
end
return 0
"""

# KEYS[1]=ready, KEYS[2]=processing, KEYS[3]=deadlines; ARGV[1]=now (whole seconds), ARGV[2]=batch
# (-1 = all). Expired members whose raw value is still in :processing go back to the END of ready
# (bytes and attempts untouched); every expired member is removed from the deadline set, a
# malformed one (shorter than 34 bytes or byte 33 not ':') or an orphan (LREM found nothing) only so.
_SWEEP = """
local members = redis.call('zrangebyscore', KEYS[3], '-inf', ARGV[1], 'limit', 0, ARGV[2])
local moved = 0
for _, m in ipairs(members) do
    if #m >= 34 and string.byte(m, 33) == 58 then
        local raw = string.sub(m, 34)
        if redis.call('lrem', KEYS[2], 1, raw) == 1 then
            redis.call('rpush', KEYS[1], raw)
            moved = moved + 1
        end
    end
    redis.call('zrem', KEYS[3], m)
end
return moved
"""


class _LegacyHandle(NamedTuple):
    """Ack handle of a legacy reservation: the stored value and the reservation's own token."""

    raw: str
    token: str


class RedisTransport(Transport):
    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        client: Any = None,
        processing_suffix: str = ":processing",
        laravel_compat: bool = False,
        key_prefix: str = "queues:",
        retry_after: Optional[int] = DEFAULT_RETRY_AFTER,
        block_for: Optional[float] = None,
        migration_batch_size: int = -1,
        clock: Callable[[], float] = time.time,
    ) -> None:
        parts = urlsplit(url) if url else urlsplit("redis://")
        q = parse_qs(parts.query)

        self._processing_suffix = processing_suffix
        self._laravel_compat = laravel_compat or _qbool(q, "laravel") or _qbool(q, "laravel_compat")
        self._key_prefix = key_prefix if "prefix" not in q else (_q1(q, "prefix") or key_prefix)
        if "retry_after" in q:
            retry_after = _qretry_after(q)
        if retry_after is not None and (
            isinstance(retry_after, bool) or not isinstance(retry_after, int) or retry_after < 0
        ):
            raise ValueError(
                f"retry_after must be a non-negative integer number of seconds or None, got {retry_after!r}"
            )
        self._retry_after: Optional[int] = retry_after
        if (
            isinstance(migration_batch_size, bool)
            or not isinstance(migration_batch_size, int)
            or (migration_batch_size != -1 and migration_batch_size < 1)
        ):
            raise ValueError(
                f"migration_batch_size must be -1 (all) or an integer >= 1, got {migration_batch_size!r}"
            )
        self._batch = migration_batch_size
        self._block_for = block_for
        self._clock = clock

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

    # -- key / time helpers ---------------------------------------------------

    def _processing(self, queue: str) -> str:
        return f"{queue}{self._processing_suffix}"

    def _deadlines(self, queue: str) -> str:
        return f"{self._processing(queue)}:deadlines"

    def _ready(self, queue: str) -> str:
        """The Laravel ready-list key for a logical queue name (``queues:<name>``)."""
        return f"{self._key_prefix}{queue}"

    def _now(self) -> int:
        """Whole epoch seconds (``floor``), the unit of every score (broker-bindings §1.1)."""
        return math.floor(self._clock())

    def _deadline_score(self) -> str:
        """The reservation deadline score: ``floor(now) + retry_after``, or ``+inf`` when
        recovery is disabled (``retry_after=None``)."""
        if self._retry_after is None:
            return "+inf"
        return str(self._now() + self._retry_after)

    def _available_at(self, delay: float) -> int:
        return self._now() + math.ceil(max(float(delay), 0.0))

    # -- Transport ------------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        if not self._laravel_compat:
            self._redis.rpush(queue, body)
            return
        ready = self._ready(queue)
        self._redis.eval(_PUSH, 2, ready, f"{ready}:notify", body)

    def publish_delayed(self, queue: str, body: str, delay: float) -> None:
        """Add ``body`` to ``queue``'s delayed set, due ``ceil(delay)`` whole seconds from now
        (``ZADD :delayed floor(now)+ceil(delay)``, no notify token; a negative delay is 0).

        Laravel-compatible layout only: the legacy layout has no delayed set.
        """
        if not self._laravel_compat:
            raise BabelQueueError(
                "publish_delayed needs the Laravel-compatible layout (laravel_compat=True); "
                "the legacy Redis layout has no delayed set."
            )
        self._redis.zadd(f"{self._ready(queue)}:delayed", {body: self._available_at(delay)})

    def publish_with_headers(self, queue: str, body: str, headers: Dict[str, str]) -> None:
        """Append ``body`` to ``queue`` together with out-of-band ``headers``
        (:class:`~babelqueue.transport.HeaderPublisher`, ADR-0028).

        Legacy layout: a non-empty header map is RPUSHed as a transport-owned frame that carries
        the headers beside the frozen envelope (GR-1); blank keys/values are dropped, and if
        nothing survives it degrades to a byte-identical bare :meth:`publish`. Laravel-compatible
        layout: the reserve script decodes the stored job, so no frame can ride there and the
        headers (incl. ``traceparent``) are dropped, a plain :meth:`publish`, no error (A3).
        """
        if self._laravel_compat:
            self.publish(queue, body)
            return
        self._redis.rpush(queue, _frame_value(body, headers))

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        block = self._block_for if self._block_for is not None else timeout
        budget = block if block and block > 0 else 0.0
        if not self._laravel_compat:
            return self._pop_legacy(queue, budget)
        return self._pop_zset(queue, budget)

    def ack(self, message: ReceivedMessage) -> None:
        if not self._laravel_compat:
            self._settle(message, None, "")
            return
        if message.handle is None:
            return
        self._redis.zrem(f"{self._ready(message.queue)}:reserved", message.handle)

    def release(self, message: ReceivedMessage, delay: float = 0) -> None:
        """Hand a reserved message back for another attempt after ``delay`` seconds.

        The body is built here: ``attempts`` advanced by one on top of the delivered raw body, every
        other byte of the envelope kept (unknown keys included). A body that does not decode or has
        no numeric ``attempts`` goes back unchanged. See :meth:`redeliver` for the layouts.
        """
        self.redeliver(message, _released_body(message.body), delay)

    def redeliver(self, message: ReceivedMessage, body: str, delay: float) -> None:
        """:class:`~babelqueue.transport.Redeliverer`: return ``message`` using ``body`` as given.

        Laravel-compatible layout: one atomic script removes the reserved member and adds ``body``
        to ``:delayed`` due at ``floor(now) + ceil(delay)`` (a negative delay is 0). Legacy layout:
        the gated settle script removes the reservation and pushes ``body`` to the END of the
        ready list (it has no delayed set, so ``delay`` is ignored).
        """
        if self._laravel_compat:
            if message.handle is None:
                return
            ready = self._ready(message.queue)
            self._redis.eval(
                _RELEASE, 2, f"{ready}:delayed", f"{ready}:reserved",
                message.handle, body, self._available_at(delay),
            )
            return
        self._settle(message, message.queue, _frame_value(body, message.headers))

    def dead_letter(self, message: ReceivedMessage, queue: str, body: str) -> None:
        """:class:`~babelqueue.transport.DeadLetterWriter`: write ``body`` to ``queue`` and settle
        ``message`` as one step.

        Both layouts are gated on the reservation: a stale reservation (already swept/migrated
        back to ready) writes nothing. Laravel layout: one script removes the reserved member and
        pushes the DLQ body, so a crash or a late migration cannot duplicate the job.
        """
        if self._laravel_compat:
            if message.handle is None:
                self.publish(queue, body)
                return
            target = self._ready(queue)
            written = self._redis.eval(
                _DEAD_LETTER, 3, f"{self._ready(message.queue)}:reserved", target,
                f"{target}:notify", message.handle, body,
            )
            if not written:
                logger.warning(
                    "Redis reservation on queue %r expired before it was dead-lettered; it was "
                    "migrated back and may be delivered again. Nothing was written.",
                    message.queue,
                )
            return
        self._settle(message, queue, _frame_value(body, message.headers))

    def close(self) -> None:  # pragma: no cover
        self._redis.close()

    # -- Legacy layout --------------------------------------------------------

    def _pop_legacy(self, queue: str, budget: float) -> Optional[ReceivedMessage]:
        end = time.monotonic() + budget
        pause = _POLL_INITIAL
        while True:
            self._sweep(queue)
            token = secrets.token_hex(_TOKEN_BYTES)
            raw = self._redis.eval(
                _RESERVE_LEGACY, 3, queue, self._processing(queue), self._deadlines(queue),
                self._deadline_score(), token,
            )
            if raw is not None and raw is not False:
                text = _as_text(raw)
                # The stored value may be a header frame (publish_with_headers) or a bare wire
                # envelope. The handle keeps the raw stored value so the settle LREM matches.
                unframed, headers = _unframe(text)
                # A body the Go reference's typed ``Decode`` rejects (not valid JSON, not an object,
                # or a member of the wrong type such as a string ``attempts``/``meta``) can never
                # be decoded or retried: hand it over as poison so the runtime dead-letters the raw
                # bytes on this first delivery instead of rebuilding and retrying it. An object
                # without an identity is NOT poison (it keeps the bounded FAIL path), and neither is
                # a bare ``null``. (``envelope_violation`` parses the body once; ``dispatch`` parses
                # it again for the handler -- one extra parse of an already small body.)
                route = "" if envelope_violation(unframed) is None else NON_NUMERIC_ATTEMPTS_ROUTE
                return ReceivedMessage(
                    body=unframed, queue=queue, handle=_LegacyHandle(text, token),
                    headers=headers, route_urn=route,
                )
            remaining = end - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(pause, remaining))
            pause = min(pause * 2, _POLL_MAX)

    def _sweep(self, queue: str) -> None:
        """Move reservations whose deadline lapsed back to the end of the ready list. Runs before
        every reserve, also with ``retry_after=None`` (a ``+inf`` deadline never matches, but a
        foreign finite one does)."""
        self._redis.eval(
            _SWEEP, 3, queue, self._processing(queue), self._deadlines(queue),
            self._now(), self._batch,
        )

    def _settle(self, message: ReceivedMessage, target: Optional[str], target_body: str) -> None:
        handle = message.handle
        if not isinstance(handle, _LegacyHandle):
            return
        keys = [self._processing(message.queue), self._deadlines(message.queue)]
        args = [handle.token, handle.raw]
        if target is not None:
            keys.append(target)
            args.append(target_body)
        settled = self._redis.eval(_SETTLE, len(keys), *keys, *args)
        if not settled:
            logger.warning(
                "Redis reservation on queue %r expired before it was settled; it was swept and "
                "may be delivered again. Nothing was touched.",
                message.queue,
            )

    # -- Laravel-compatible (zset) layout -------------------------------------

    def _pop_zset(self, queue: str, budget: float) -> Optional[ReceivedMessage]:
        ready = self._ready(queue)
        notify = f"{ready}:notify"
        end = time.monotonic() + budget
        while True:
            self._migrate(ready)
            result = self._redis.eval(
                _POP, 3, ready, f"{ready}:reserved", notify, self._deadline_score(),
            )
            job, reserved = _pop_result(result)
            if job is not None and reserved is not None:
                return self._zset_message(queue, _as_text(job), _as_text(reserved))
            remaining = end - time.monotonic()
            if remaining <= 0:
                return None
            self._redis.blpop([notify], min(_BLOCK_SLICE, remaining))

    @staticmethod
    def _zset_message(queue: str, job: str, reserved: str) -> ReceivedMessage:
        # The handler gets the RAW job; the reserved member (attempts + 1, or the raw job for a
        # poison body) is only the ack handle, so a failure counts the attempt exactly once.
        route = NON_NUMERIC_ATTEMPTS_ROUTE if job == reserved else ""
        return ReceivedMessage(body=job, queue=queue, handle=reserved, route_urn=route)

    def _migrate(self, ready: str) -> None:
        now = self._now()
        # Delayed jobs that are due, then (unless recovery is off) reserved jobs whose retry-after
        # lapsed (a crashed worker's in-flight job): both move back to the ready list.
        self._redis.eval(_MIGRATE, 3, f"{ready}:delayed", ready, f"{ready}:notify", now, self._batch)
        if self._retry_after is not None:
            self._redis.eval(_MIGRATE, 3, f"{ready}:reserved", ready, f"{ready}:notify", now, self._batch)


def _released_body(body: str) -> str:
    """``body`` with ``attempts`` advanced by one, every other key kept (``{}``/``[]``/unicode and
    integers of any size survive). Unchanged when it is not an object with a numeric ``attempts``."""
    envelope = parse_envelope(body)
    attempts = envelope.get("attempts")
    if isinstance(attempts, bool) or not isinstance(attempts, (int, float)):
        return body
    envelope["attempts"] = attempts + 1
    return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))


def _sanitize_headers(headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Copy ``headers`` dropping blank keys and blank values; ``{}`` when nothing survives, so
    callers can treat the result as 'no headers'."""
    if not headers:
        return {}
    out: Dict[str, str] = {}
    for key, value in headers.items():
        if not key or value is None or value == "":
            continue
        out[str(key)] = str(value)
    return out


def _frame_value(body: str, headers: Optional[Dict[str, str]]) -> str:
    """The pure produce-side decision: the exact string to RPUSH for ``body`` + ``headers``.

    With no usable headers it returns ``body`` verbatim (the bare form, so plain ``publish`` and
    ``publish_with_headers``-without-headers store byte-identical values); otherwise it returns the
    transport-owned frame JSON. Kept pure so framing is unit-testable without a broker.
    """
    clean = _sanitize_headers(headers)
    if not clean:
        return body
    return json.dumps(
        {_FRAME_KEY: _FRAME_VERSION, "headers": clean, "body": body},
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _unframe(value: str) -> Tuple[str, Dict[str, str]]:
    """Interpret a stored Redis list value: ``(wire-envelope-body, headers)``.

    A value is a header frame iff it is a JSON object carrying the reserved ``__bq_frame``
    sentinel (a frozen wire envelope never has it); then it yields the unframed body plus the
    carried headers. Any other value (a bare envelope, non-JSON, or JSON without the sentinel)
    is returned verbatim as the body with ``{}`` headers, so older/cross-version queue values
    consume exactly as before.
    """
    # Cheap reject: a frame is always a JSON object and the sentinel substring must appear. This
    # avoids a full parse for the common bare-envelope case (the check only short-circuits negatives).
    if not value or value[0] != "{" or f'"{_FRAME_KEY}"' not in value:
        return value, {}
    try:
        decoded = json.loads(value)
    except (ValueError, TypeError):
        return value, {}
    if not isinstance(decoded, dict) or not decoded.get(_FRAME_KEY) or "body" not in decoded:
        return value, {}
    body = decoded["body"]
    if not isinstance(body, str):
        return value, {}
    raw_headers = decoded.get("headers")
    headers = raw_headers if isinstance(raw_headers, dict) else None
    return body, _sanitize_headers(headers)


def _as_text(value: Any) -> str:
    """Normalise a Redis reply to ``str`` (bytes when ``decode_responses`` is off)."""
    return value if isinstance(value, str) else value.decode()


def _pop_result(result: Any) -> Tuple[Optional[Any], Optional[Any]]:
    """Unpack the ``{job, reserved}`` reply; Lua ``false`` arrives as ``None``. An empty-string
    job is a real (poison) job, so only ``None``/``False`` mean 'queue empty'."""
    if not result or len(result) < 2:
        return None, None
    job, reserved = result[0], result[1]
    if job is None or reserved is None or job is False or reserved is False:
        return None, None
    return job, reserved


def _q1(q: dict, key: str) -> Optional[str]:
    values = q.get(key)
    return values[0] if values else None


def _qretry_after(q: dict) -> Optional[int]:
    """The ``retry_after`` URL parameter: ``null``/``none`` disables recovery, else an integer."""
    v = _q1(q, "retry_after")
    if v is None:
        return DEFAULT_RETRY_AFTER
    if v.strip().lower() in ("null", "none"):
        return None
    try:
        return int(v)
    except ValueError as exc:
        raise ValueError(f"retry_after must be an integer or null, got {v!r}") from exc


def _qbool(q: dict, key: str) -> bool:
    v = _q1(q, key)
    return v is not None and v.lower() in ("1", "true", "yes", "on")
