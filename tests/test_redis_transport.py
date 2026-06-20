"""Tests for the Redis transport.

The unit tests inject a tiny in-memory Redis double, so they run without the
``redis`` package and without a broker. The double emulates exactly the commands
the transport uses (``rpush``/``lpop``/``blmove``/``lrem``/``zadd``/``zrem``/
``blpop``) plus the three Laravel Lua scripts the Laravel-compatible mode runs,
so the reserved-set / reliable-queue semantics are exercised end-to-end.

A separate integration test round-trips against a real Redis and is skipped unless
the ``redis`` package and a reachable broker are present (CI runs it).
"""

from __future__ import annotations

import json
import os
import unittest
import uuid

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.redis_transport import (
    RedisTransport,
    _frame_value,
    _pop_result,
    _qint,
    _unframe,
)
from babelqueue.transport import HeaderPublisher, ReceivedMessage


class FakeRedis:
    """In-memory stand-in for the subset of Redis the transport uses.

    Lists are Python lists; sorted sets are ``{member: score}`` dicts. ``eval``
    recognises the three Laravel Lua scripts (push / pop / migrate / release) by
    keyword and emulates their effect — including the ``cjson`` re-encode of the
    reserved member, which is what makes Python's and Laravel's reserved members
    byte-identical in production.
    """

    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    # -- plain commands (Python-owned mode) ---------------------------------

    def rpush(self, key, value):  # noqa: ANN001
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    def lpop(self, key):  # noqa: ANN001
        lst = self.lists.get(key)
        return lst.pop(0) if lst else None

    def llen(self, key):  # noqa: ANN001
        return len(self.lists.get(key, []))

    def lrange(self, key, start, stop):  # noqa: ANN001
        lst = self.lists.get(key, [])
        end = len(lst) if stop == -1 else stop + 1
        return lst[start:end]

    def blmove(self, src, dst, timeout, src_dir, dst_dir):  # noqa: ANN001, ARG002
        lst = self.lists.get(src)
        if not lst:
            return None
        value = lst.pop(0)
        self.lists.setdefault(dst, []).append(value)
        return value

    def lrem(self, key, count, value):  # noqa: ANN001, ARG002
        lst = self.lists.get(key, [])
        if value in lst:
            lst.remove(value)
            return 1
        return 0

    def blpop(self, keys, timeout):  # noqa: ANN001, ARG002
        for key in keys:
            lst = self.lists.get(key)
            if lst:
                return (key, lst.pop(0))
        return None

    def zadd(self, key, mapping):  # noqa: ANN001
        self.zsets.setdefault(key, {}).update(mapping)

    def zrem(self, key, member):  # noqa: ANN001
        z = self.zsets.get(key, {})
        return 1 if z.pop(member, None) is not None else 0

    def zcard(self, key):  # noqa: ANN001
        return len(self.zsets.get(key, {}))

    def delete(self, *keys):  # noqa: ANN001
        for key in keys:
            self.lists.pop(key, None)
            self.zsets.pop(key, None)

    def close(self):
        pass

    # -- Lua scripts (Laravel-compatible mode) ------------------------------

    def eval(self, script, numkeys, *args):  # noqa: ANN001
        keys = list(args[:numkeys])
        argv = list(args[numkeys:])
        if "rpush" in script and "lpop" not in script and "zrange" not in script:
            return self._push(keys, argv)
        if "'lpop'" in script and "zadd" in script:
            return self._pop(keys, argv)
        if "zrangebyscore" in script:
            return self._migrate(keys, argv)
        if "'zrem'" in script and "zadd" in script:
            return self._release(keys, argv)
        raise AssertionError("unrecognised Lua script")

    def _push(self, keys, argv):  # noqa: ANN001
        ready, notify = keys
        self.rpush(ready, argv[0])
        self.rpush(notify, "1")

    def _pop(self, keys, argv):  # noqa: ANN001
        ready, reserved_key, notify = keys
        job = self.lpop(ready)
        if job is None:
            return [None, None]
        decoded = json.loads(job)
        decoded["attempts"] = decoded.get("attempts", 0) + 1
        reserved = json.dumps(decoded, separators=(",", ":"))
        self.zadd(reserved_key, {reserved: float(argv[0])})
        self.lpop(notify)
        return [job, reserved]

    def _migrate(self, keys, argv):  # noqa: ANN001
        src, dst, notify = keys
        now = float(argv[0])
        z = self.zsets.get(src, {})
        due = [m for m, score in z.items() if score <= now]
        for member in due:
            z.pop(member, None)
            self.rpush(dst, member)
            self.rpush(notify, "1")
        return due

    def _release(self, keys, argv):  # noqa: ANN001
        delayed, reserved_key = keys
        member, score = argv[0], float(argv[1])
        self.zrem(reserved_key, member)
        self.zadd(delayed, {member: score})
        return True


class RedisOwnedModeTest(unittest.TestCase):
    """The default Python-owned reliable-queue mode (BLMOVE + processing list)."""

    def _tr(self):
        fake = FakeRedis()
        return RedisTransport("redis://", client=fake), fake

    def test_publish_uses_raw_queue_key(self):
        tr, fake = self._tr()
        tr.publish("orders", "body-1")
        self.assertEqual(fake.lists["orders"], ["body-1"])

    def test_pop_moves_to_processing_and_ack_removes(self):
        tr, fake = self._tr()
        tr.publish("orders", "body-1")
        msg = tr.pop("orders", timeout=0)
        self.assertEqual(msg.body, "body-1")
        self.assertEqual(fake.lists["orders:processing"], ["body-1"])
        self.assertEqual(fake.lists["orders"], [])
        tr.ack(msg)
        self.assertEqual(fake.lists["orders:processing"], [])

    def test_pop_empty_returns_none(self):
        tr, _ = self._tr()
        self.assertIsNone(tr.pop("orders", timeout=0))


class RedisLaravelCompatTest(unittest.TestCase):
    """Laravel reserved-set parity: shared queue, reserve / ack / release / migrate."""

    def _tr(self, **kw):
        fake = FakeRedis()
        return RedisTransport("redis://", client=fake, laravel_compat=True, **kw), fake

    def test_publish_uses_laravel_keys_and_notify(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish("orders", EnvelopeCodec.encode(env))
        # Laravel prefixes logical names with "queues:" and pushes a notify token.
        self.assertEqual(fake.llen("queues:orders"), 1)
        self.assertEqual(fake.llen("queues:orders:notify"), 1)

    def test_pop_reserves_into_sorted_set_and_increments_attempts(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish("orders", EnvelopeCodec.encode(env))

        msg = tr.pop("orders", timeout=0)
        self.assertIsNotNone(msg)
        # ready list drained, reserved set holds exactly the in-flight job
        self.assertEqual(fake.llen("queues:orders"), 0)
        self.assertEqual(fake.zcard("queues:orders:reserved"), 1)
        # attempts incremented on reserve, exactly like Laravel's pop Lua
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 1)
        # the ack handle IS the reserved member (so ZREM matches byte-for-byte)
        self.assertIn(msg.handle, fake.zsets["queues:orders:reserved"])

    def test_ack_removes_from_reserved_set(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish("orders", EnvelopeCodec.encode(env))
        msg = tr.pop("orders", timeout=0)
        tr.ack(msg)
        self.assertEqual(fake.zcard("queues:orders:reserved"), 0)

    def test_ack_noop_on_empty_handle(self):
        tr, fake = self._tr()
        tr.ack(ReceivedMessage(body="", queue="orders", handle=None))
        self.assertEqual(fake.zsets, {})

    def test_crashed_worker_reservation_is_re_reserved_on_next_pop(self):
        """A reserved job whose retry-after has lapsed migrates back and is re-served
        — the reliable-queue guarantee that prevents message loss on worker crash."""
        tr, fake = self._tr(retry_after=-1)  # reserve already expired the instant it lands
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish("orders", EnvelopeCodec.encode(env))

        first = tr.pop("orders", timeout=0)  # reserves (expired-at-now deadline)
        self.assertIsNotNone(first)
        # worker "crashes" without ack; next pop migrates the lapsed reservation back
        second = tr.pop("orders", timeout=0)
        self.assertIsNotNone(second)
        # attempts climbed again on the second reservation
        self.assertEqual(EnvelopeCodec.decode(second.body)["attempts"], 2)

    def test_release_returns_job_to_delayed_set(self):
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        tr.publish("orders", EnvelopeCodec.encode(env))
        msg = tr.pop("orders", timeout=0)
        tr.release(msg, delay=0)
        # left the reserved set, parked on the delayed set for retry
        self.assertEqual(fake.zcard("queues:orders:reserved"), 0)
        self.assertEqual(fake.zcard("queues:orders:delayed"), 1)

    def test_release_noop_in_owned_mode(self):
        tr, fake = self._tr()
        # owned-mode transport: release is a no-op (handle present but not compat)
        owned = RedisTransport("redis://", client=fake)
        owned.release(ReceivedMessage(body="b", queue="orders", handle="b"))
        self.assertEqual(fake.zsets, {})

    def test_pop_empty_returns_none(self):
        tr, _ = self._tr()
        self.assertIsNone(tr.pop("orders", timeout=0))

    def test_pop_block_window_drains_to_none(self):
        """Empty ready list with a stale notify token: the block path consumes the
        token (blpop wakeup), re-checks, finds nothing and returns None — the
        timeout-drain case a consume loop relies on to make progress."""
        tr, fake = self._tr(block_for=1)
        fake.rpush("queues:orders:notify", "1")  # token but no actual job
        self.assertIsNone(tr.pop("orders", timeout=1))
        # the stale token was consumed by the blpop wakeup
        self.assertEqual(fake.llen("queues:orders:notify"), 0)

    def test_pop_retrieves_when_job_already_ready(self):
        """A job already on the ready list is reserved on the first eval (no block)."""
        tr, fake = self._tr()
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        fake.rpush("queues:orders", EnvelopeCodec.encode(env))
        fake.rpush("queues:orders:notify", "1")
        msg = tr.pop("orders", timeout=1)
        self.assertIsNotNone(msg)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 1)

    def test_url_enables_compat_and_overrides(self):
        fake = FakeRedis()
        tr = RedisTransport("redis://h:6379/0?laravel=1&prefix=q:&retry_after=90", client=fake)
        self.assertTrue(tr._laravel_compat)
        self.assertEqual(tr._key_prefix, "q:")
        self.assertEqual(tr._retry_after, 90)

    def test_pop_result_handles_short_and_false_replies(self):
        # empty / too-short reply, and Lua `false` (None) job or reserved member
        self.assertEqual(_pop_result([]), (None, None))
        self.assertEqual(_pop_result([None, None]), (None, None))
        self.assertEqual(_pop_result(["job", "reserved"]), ("job", "reserved"))

    def test_qint_falls_back_on_missing_and_non_numeric(self):
        self.assertEqual(_qint({}, "retry_after", 60), 60)  # absent -> default
        self.assertEqual(_qint({"retry_after": ["nope"]}, "retry_after", 60), 60)  # non-int
        self.assertEqual(_qint({"retry_after": ["90"]}, "retry_after", 60), 90)  # parsed

    def test_round_trip_through_app(self):
        fake = FakeRedis()
        tr = RedisTransport("redis://", client=fake, laravel_compat=True)
        app = BabelQueue(transport=tr, queue="orders")
        seen: dict = {}

        @app.handler("urn:babel:orders:created")
        def _on(data, meta):  # noqa: ANN001
            seen.update({"data": data, "meta": meta})

        msg_id = app.publish("urn:babel:orders:created", {"order_id": 7})
        processed = app.consume("orders", max_messages=1, timeout=0)
        self.assertEqual(processed, 1)
        self.assertEqual(seen["data"]["order_id"], 7)
        self.assertEqual(seen["meta"]["id"], msg_id)
        # acked: reserved set empty
        self.assertEqual(fake.zcard("queues:orders:reserved"), 0)


class RedisHeaderFrameTest(unittest.TestCase):
    """ADR-0028: the transport-owned ``__bq_frame`` carrier + bare-value back-compat."""

    def _tr(self):
        fake = FakeRedis()
        return RedisTransport("redis://", client=fake), fake

    def test_transport_is_a_header_publisher(self):
        tr, _ = self._tr()
        self.assertIsInstance(tr, HeaderPublisher)

    def test_frame_round_trip_recovers_headers_and_verbatim_body(self):
        body = '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0}'
        framed = _frame_value(body, {"traceparent": "00-abc"})
        self.assertIn('"__bq_frame"', framed)
        unframed, headers = _unframe(framed)
        self.assertEqual(unframed, body)  # the wire envelope is recovered byte-for-byte
        self.assertEqual(headers, {"traceparent": "00-abc"})

    def test_no_usable_headers_stays_bare_byte_for_byte(self):
        body = '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}'
        self.assertEqual(_frame_value(body, None), body)
        self.assertEqual(_frame_value(body, {}), body)
        self.assertEqual(_frame_value(body, {"": "x", "drop": ""}), body)  # all blank -> bare

    def test_unframe_bare_values_back_compat(self):
        # a bare envelope, a non-JSON value, and JSON without the sentinel all unframe to (value, {})
        for value in (
            '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}',
            "not-json",
            '{"some":"json","no":"sentinel"}',
            "",
        ):
            body, headers = _unframe(value)
            self.assertEqual(body, value)
            self.assertEqual(headers, {})

    def test_publish_with_headers_stores_frame_and_pop_unframes(self):
        tr, fake = self._tr()
        body = EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        )
        tr.publish_with_headers("orders", body, {"traceparent": "00-deadbeef"})
        # the stored list value is a frame (the ack handle), the body is unframed on pop
        stored = fake.lists["orders"][0]
        self.assertIn('"__bq_frame"', stored)
        msg = tr.pop("orders", timeout=0)
        self.assertEqual(msg.body, body)  # consumer sees the bare wire envelope
        self.assertEqual(msg.headers.get("traceparent"), "00-deadbeef")
        # the ack handle is the stored frame, so LREM still matches
        self.assertEqual(msg.handle, stored)
        tr.ack(msg)
        self.assertEqual(fake.lists["orders:processing"], [])

    def test_plain_publish_then_pop_has_no_headers(self):
        tr, _ = self._tr()
        body = '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}'
        tr.publish("orders", body)
        msg = tr.pop("orders", timeout=0)
        self.assertEqual(msg.body, body)
        self.assertEqual(msg.headers, {})  # bare value consumes with no headers

    def test_publish_with_headers_no_headers_stays_bare(self):
        tr, fake = self._tr()
        body = '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}'
        tr.publish_with_headers("orders", body, {})
        self.assertEqual(fake.lists["orders"], [body])  # byte-identical bare value, no frame

    def test_laravel_compat_degrades_to_bare_publish(self):
        fake = FakeRedis()
        tr = RedisTransport("redis://", client=fake, laravel_compat=True)
        env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1}, queue="orders")
        body = EnvelopeCodec.encode(env)
        tr.publish_with_headers("orders", body, {"traceparent": "00-x"})
        # stored bare on the Laravel ready list (no frame — the reservation Lua decodes the job)
        self.assertEqual(fake.lists["queues:orders"], [body])
        for value in fake.lists["queues:orders"]:
            self.assertNotIn("__bq_frame", value)


# ---------------------------------------------------------------------------
# Integration: skipped unless a real Redis is reachable (CI runs it).
# ---------------------------------------------------------------------------

try:
    import redis as _redis
except ImportError:  # pragma: no cover
    _redis = None

REDIS_URL = os.environ.get("BABELQUEUE_TEST_REDIS", "redis://localhost:6379/0")


def _redis_available() -> bool:
    if _redis is None:
        return False
    try:
        _redis.Redis.from_url(REDIS_URL, decode_responses=True).ping()
        return True
    except Exception:  # pragma: no cover - connection failure
        return False


@unittest.skipUnless(_redis_available(), f"no reachable Redis at {REDIS_URL}")
class RedisTransportIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = f"bqtest:{uuid.uuid4().hex}"
        self.client = _redis.Redis.from_url(REDIS_URL, decode_responses=True)

    def tearDown(self) -> None:
        self.client.delete(self.queue, f"{self.queue}:processing", f"{self.queue}.dlq")
        self.client.delete(
            f"queues:{self.queue}", f"queues:{self.queue}:reserved",
            f"queues:{self.queue}:delayed", f"queues:{self.queue}:notify",
        )

    def test_publish_consume_round_trip_and_ack(self) -> None:
        app = BabelQueue(REDIS_URL, queue=self.queue)
        seen = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen.update(data)

        app.publish("urn:babel:orders:created", {"order_id": 42})
        processed = app.consume(max_messages=1, timeout=2)

        self.assertEqual(processed, 1)
        self.assertEqual(seen, {"order_id": 42})
        self.assertEqual(self.client.llen(self.queue), 0)
        self.assertEqual(self.client.llen(f"{self.queue}:processing"), 0)

    def test_failure_dead_letters_to_redis(self) -> None:
        app = BabelQueue(REDIS_URL, queue=self.queue, max_attempts=1, dead_letter=True)

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            raise RuntimeError("boom")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        app.consume(max_messages=2, timeout=2)

        self.assertEqual(self.client.llen(f"{self.queue}:processing"), 0)
        dlq = self.client.lrange(f"{self.queue}.dlq", 0, -1)
        self.assertEqual(len(dlq), 1)
        env = EnvelopeCodec.decode(dlq[0])
        self.assertEqual(env["dead_letter"]["reason"], "failed")

    def test_traceparent_header_round_trips_on_real_redis(self) -> None:
        """ADR-0028: a published traceparent arrives on the consumed message's headers, and the
        body (the frozen wire envelope) is unchanged. A bare publish consumes with no headers."""
        tr = RedisTransport(REDIS_URL)
        body = EnvelopeCodec.encode(
            EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 7}, queue=self.queue)
        )
        tr.publish_with_headers(self.queue, body, {"traceparent": "00-feedface"})
        msg = tr.pop(self.queue, timeout=2)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.body, body)  # body unchanged
        self.assertEqual(msg.headers.get("traceparent"), "00-feedface")
        tr.ack(msg)

        # a bare publish consumes with no headers (back-compat)
        tr.publish(self.queue, body)
        bare = tr.pop(self.queue, timeout=2)
        self.assertIsNotNone(bare)
        self.assertEqual(bare.body, body)
        self.assertEqual(bare.headers, {})
        tr.ack(bare)

    def test_laravel_compat_reserved_set_round_trip(self) -> None:
        """Against a real Redis: a Laravel-compatible worker reserves into the
        ``queues:<name>:reserved`` sorted set and acks via ZREM."""
        tr = RedisTransport(REDIS_URL, laravel_compat=True)
        app = BabelQueue(transport=tr, queue=self.queue)
        seen = {}
        app.register("urn:babel:orders:created", lambda data, meta: seen.update(data))

        app.publish("urn:babel:orders:created", {"order_id": 99})
        processed = app.consume(self.queue, max_messages=1, timeout=2)

        self.assertEqual(processed, 1)
        self.assertEqual(seen, {"order_id": 99})
        self.assertEqual(self.client.zcard(f"queues:{self.queue}:reserved"), 0)
        self.assertEqual(self.client.llen(f"queues:{self.queue}"), 0)


if __name__ == "__main__":
    unittest.main()
