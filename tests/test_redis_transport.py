"""Tests for the Redis transport (ADR-0033, broker-bindings.md section 1).

The transport is Lua-script based, so a hand-written double cannot prove it: the scripts must run.
:mod:`redis_backend` supplies a **real Redis** when one is reachable (CI: ``redis:7``), else an
in-process fakeredis + lupa server (a real Lua interpreter with an emulated command set) - clearly
a stand-in, never a replacement for the real run. With ``BQ_REQUIRE_REDIS`` set only a real Redis is
accepted. Without either the broker tests skip; the pure-function and constructor tests always run.

Every score and migration decision derives from the transport's injectable ``clock``, so the tests
drive time with a fake clock and stay deterministic on a real broker too.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import threading
import time
import unittest
from pathlib import Path

from babelqueue import BabelQueue, BabelQueueError, EnvelopeCodec
from babelqueue.redis_transport import (
    _DEAD_LETTER,
    _POP,
    _RESERVE_LEGACY,
    _SETTLE,
    _SWEEP,
    RedisTransport,
    _frame_value,
    _pop_result,
    _qretry_after,
    _released_body,
    _as_text,
    _unframe,
    _wire,
)
from babelqueue.routing import UnknownUrnStrategy
from babelqueue.transport import (
    NON_NUMERIC_ATTEMPTS_ROUTE,
    DeadLetterWriter,
    HeaderPublisher,
    ReceivedMessage,
    Redeliverer,
)

import redis_backend

URN = "urn:babel:orders:created"
TOKEN = re.compile(r"^[0-9a-f]{32}$")
SOURCE = Path(__file__).resolve().parent.parent / "src" / "babelqueue" / "redis_transport.py"


def _reject_constant(name: str) -> None:
    raise ValueError(f"{name} is not JSON (RFC 8259)")


def assert_poison_record(case: unittest.TestCase, record: str, body: str, queue: str) -> None:
    """``record`` is the ADR-0035 poison DLQ record for ``body`` read from ``queue``."""
    parsed = json.loads(record)
    block = parsed["dead_letter"]
    case.assertEqual(block["reason"], "poison")
    case.assertEqual(block["original_queue"], queue)
    try:
        decoded = body.encode("utf-8", "surrogateescape").decode("utf-8")
    except UnicodeDecodeError:
        decoded = None
    members = None
    if decoded is not None:
        try:
            # ADR-0035 section 1: a strict RFC 8259 object is spliced; NaN / Infinity / -Infinity
            # (which Python's json.loads accepts by default) make the body a wrapper case.
            members = json.loads(decoded, parse_constant=_reject_constant)
        except ValueError:
            members = None
    if decoded is None:
        case.assertEqual(parsed["raw_encoding"], "base64")
        case.assertEqual(
            base64.b64decode(parsed["raw"]), body.encode("utf-8", "surrogateescape")
        )
    elif isinstance(members, dict) and "dead_letter" not in members:
        case.assertNotIn("raw", parsed)
        case.assertTrue(record.startswith(decoded.strip()[:-1]), record)  # spliced byte for byte
    else:
        case.assertEqual(parsed["raw"], decoded)
        case.assertNotIn("raw_encoding", parsed)


class Clock:
    """A settable clock (epoch seconds)."""

    def __init__(self, now: float = 1_700_000_000.4) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_body(queue: str, data=None, attempts: int = 0, **extra) -> str:
    env = EnvelopeCodec.make(URN, data if data is not None else {"x": 1}, queue=queue)
    env["attempts"] = attempts
    env.update(extra)
    return EnvelopeCodec.encode(env)


class _NoRedisTest(unittest.TestCase):
    """Constructor / validation tests that never touch the broker."""

    def test_defaults(self):
        tr = RedisTransport("redis://", client=object())
        self.assertEqual(tr._retry_after, 90)
        self.assertEqual(tr._batch, -1)
        self.assertFalse(tr._laravel_compat)

    def test_url_enables_compat_and_overrides(self):
        tr = RedisTransport("redis://h/0?laravel=1&prefix=p:&retry_after=30", client=object())
        self.assertTrue(tr._laravel_compat)
        self.assertEqual(tr._ready("orders"), "p:orders")
        self.assertEqual(tr._retry_after, 30)

    def test_retry_after_null_disables_recovery(self):
        for value in ("null", "none", "NONE"):
            tr = RedisTransport(f"redis://h/0?retry_after={value}", client=object())
            self.assertIsNone(tr._retry_after)
        self.assertIsNone(RedisTransport("redis://", client=object(), retry_after=None)._retry_after)

    def test_invalid_retry_after_is_rejected(self):
        for bad in (-1, 1.5, True, "90"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                RedisTransport("redis://", client=object(), retry_after=bad)  # type: ignore[arg-type]
        for url in ("redis://h/0?retry_after=abc", "redis://h/0?retry_after=-5"):
            with self.assertRaises(ValueError, msg=url):
                RedisTransport(url, client=object())

    def test_invalid_migration_batch_size_is_rejected(self):
        for bad in (0, -2, 1.5, True):
            with self.assertRaises(ValueError, msg=repr(bad)):
                RedisTransport("redis://", client=object(), migration_batch_size=bad)  # type: ignore[arg-type]
        for ok in (-1, 1, 500):
            RedisTransport("redis://", client=object(), migration_batch_size=ok)

    def test_capabilities(self):
        tr = RedisTransport("redis://", client=object())
        self.assertIsInstance(tr, Redeliverer)
        self.assertIsInstance(tr, DeadLetterWriter)
        self.assertIsInstance(tr, HeaderPublisher)

    def test_qretry_after(self):
        self.assertEqual(_qretry_after({}), 90)
        self.assertEqual(_qretry_after({"retry_after": ["12"]}), 12)
        self.assertIsNone(_qretry_after({"retry_after": ["null"]}))

    def test_pop_result_handles_short_and_false_replies(self):
        self.assertEqual(_pop_result(None), (None, None))
        self.assertEqual(_pop_result([]), (None, None))
        self.assertEqual(_pop_result([None, None]), (None, None))
        self.assertEqual(_pop_result([False, False]), (None, None))
        self.assertEqual(_pop_result(["a", "b"]), ("a", "b"))

    def test_released_body_advances_attempts_and_keeps_everything_else(self):
        body = (
            '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1,"x":[]},'
            '"attempts":2,"x-extra":{"k":"é"},"big":123456789012345678901234567890}'
        )
        out = json.loads(_released_body(body))
        self.assertEqual(out["attempts"], 3)
        self.assertEqual(out["data"], {})
        self.assertEqual(out["meta"]["x"], [])
        self.assertEqual(out["x-extra"], {"k": "é"})
        self.assertEqual(out["big"], 123456789012345678901234567890)
        self.assertIn("é", _released_body(body))  # unicode is not escaped

    def test_released_body_is_unchanged_when_it_cannot_be_advanced(self):
        for body in (
            "not json",
            "[1,2]",
            '{"job":"u"}',
            '{"job":"u","attempts":"2"}',
            '{"job":"u","attempts":true}',
            '{"job":"u","attempts":null}',
        ):
            self.assertEqual(_released_body(body), body)

    def test_reservation_token_is_a_csprng_hex_token(self):
        source = SOURCE.read_text(encoding="utf-8")
        self.assertIn("secrets.token_hex(_TOKEN_BYTES)", source)
        self.assertIn("_TOKEN_BYTES = 16", source)
        for forbidden in ("import random", "uuid"):
            self.assertNotIn(forbidden, source)


class RedisFrameFunctionsTest(unittest.TestCase):
    """ADR-0028: the transport-owned ``__bq_frame`` carrier + bare-value back-compat."""

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
        for value in (
            '{"job":"u","trace_id":"t","data":{},"meta":{"schema_version":1}}',
            "not-json",
            '{"some":"json","no":"sentinel"}',
            "",
        ):
            body, headers = _unframe(value)
            self.assertEqual(body, value)
            self.assertEqual(headers, {})


class PopScriptWithoutRedisTest(unittest.TestCase):
    """The reserve script's ``cjson.encode`` guard, proven without a Redis server.

    fakeredis + lupa can MASK this bug: its cjson serialises ``inf`` as ``Infinity`` without error,
    whereas real Redis' lua-cjson raises ``Cannot serialise number: must not be NaN or Inf`` (also
    for over-deep nesting). On an unguarded script real Redis therefore aborts the EVAL after the
    ``lpop``: the job is gone from ``:ready`` and was never reserved. These tests run the script in a
    plain Lua interpreter against a ``cjson`` stub that fails the way real Redis does; the broker
    tests in ``LaravelLayoutTest`` add the same bodies against a real Redis (CI: ``redis:7``).
    """

    #: Stand-in for lua-cjson: decode maps a marker to a table, encode refuses inf/NaN anywhere.
    STUB = """
        local function bad(v, depth)
            if type(v) == 'number' then return v ~= v or v == math.huge or v == -math.huge end
            if type(v) == 'table' then
                for _, x in pairs(v) do if bad(x) then return true end end
            end
            return false
        end
        cjson = {
            decode = function(s)
                if s == 'INF_ATTEMPTS' then return {attempts = math.huge} end
                if s == 'INF_DATA' then return {attempts = 0, data = {n = math.huge}} end
                if s == 'NAN_DATA' then return {attempts = 0, data = {n = 0/0}} end
                if s == 'OK' then return {attempts = 0} end
                error('Expected value but found invalid token')
            end,
            encode = function(t)
                if bad(t) then error('Cannot serialise number: must not be NaN or Inf') end
                return 'ENCODED:' .. tostring(t.attempts)
            end,
        }
    """

    def _lua(self):
        try:
            from lupa import LuaRuntime
        except ImportError:  # pragma: no cover - lupa is optional (it comes with fakeredis[lua])
            self.skipTest("lupa is not installed")
        return LuaRuntime(unpack_returned_tuples=True)

    def _run(self, script: str, job: str):
        lua = self._lua()
        lua.execute(self.STUB)
        lua.execute(
            "KEYS = {'ready', 'reserved', 'notify'}; ARGV = {'100'}; calls = {}\n"
            "redis = {call = function(cmd, key, ...)\n"
            "  table.insert(calls, {cmd, key, ...})\n"
            "  if cmd == 'lpop' and key == 'ready' then return POPPED end\n"
            "  return false\n"
            "end}"
        )
        lua.globals().POPPED = job
        result = lua.eval("function() " + script + " end")()
        zadds = [c for c in lua.eval("calls").values() if c[1] == "zadd"]
        return result[1], result[2], zadds

    def test_an_encode_that_fails_reserves_the_raw_job_instead_of_aborting(self):
        for job in ("INF_ATTEMPTS", "INF_DATA", "NAN_DATA"):
            with self.subTest(job=job):
                popped, reserved, zadds = self._run(_POP, job)
                self.assertEqual((popped, reserved), (job, job))
                self.assertEqual(len(zadds), 1)
                self.assertEqual(zadds[0][4], job)  # ZADD reserved <score> <member>: member is the raw job

    def test_a_clean_body_still_reserves_the_re_encoded_attempts_plus_one(self):
        popped, reserved, _ = self._run(_POP, "OK")
        self.assertEqual((popped, reserved), ("OK", "ENCODED:1"))

    def test_the_stub_models_the_bug_an_unguarded_encode_raises(self):
        unguarded = _POP.replace(
            "local eok, encoded = pcall(cjson.encode, decoded)\n        if eok then\n"
            "            reserved = encoded\n        else\n            reserved = job\n        end",
            "reserved = cjson.encode(decoded)",
        )
        self.assertNotEqual(unguarded, _POP)
        self._lua()  # skips when lupa is not installed (the matrix jobs install no Redis extras)
        from lupa import LuaError

        with self.assertRaises(LuaError):
            self._run(unguarded, "INF_DATA")

    def test_the_script_source_guards_the_encode_with_pcall(self):
        self.assertIn("pcall(cjson.encode", _POP)
        self.assertNotIn("= cjson.encode(", _POP)


class LegacyPoisonWithoutRedisTest(unittest.TestCase):
    """Legacy layout: an undecodable body is dead-lettered as an ADR-0035 poison record on its FIRST delivery (broker-free).

    The client double answers the reserve/sweep scripts and records the gated settle call, so the
    test proves the transport hands the body over as poison and the runtime settles it through the
    one gated script (write the poison record to the DLQ + remove from ``:processing``).
    """

    class Client:
        def __init__(self, stored):
            self.stored = [stored]
            self.settles = []

        def eval(self, script, numkeys, *args):
            if script == _SWEEP:
                return 0
            if script == _RESERVE_LEGACY:
                return self.stored.pop(0) if self.stored else None
            if script == _SETTLE:
                self.settles.append((numkeys, args))
                return 1
            raise AssertionError("unexpected script")

        def rpush(self, *args):  # pragma: no cover - a plain publish must never be used here
            raise AssertionError("poison must be settled through the gated script")

    def consume(self, stored, **kwargs):
        client = self.Client(stored)
        app = BabelQueue(
            transport=RedisTransport("redis://", client=client, retry_after=90),
            queue="orders", **kwargs,
        )
        app.register(URN, lambda data, meta: None)
        self.assertEqual(app.consume("orders", max_messages=1, timeout=0), 1)
        return client

    def test_non_json_body_goes_to_the_poison_dlq_on_first_delivery(self):
        for garbage in (
            "not-json at all", "[1, 2]", '"a string"', "", "{truncated",
            "\udcff\udcfe not-json",  # invalid UTF-8 (surrogateescape): base64 + raw_encoding
            '{"job":"urn:a","attempts":"x"}',  # a strict object: spliced byte for byte
        ):
            with self.subTest(body=garbage):
                client = self.consume(garbage, max_attempts=3, dead_letter=True)
                ((numkeys, args),) = client.settles
                self.assertEqual(numkeys, 3)
                self.assertEqual(args[:3], ("orders:processing", "orders:processing:deadlines", "orders.dlq"))
                # handle.raw: LREM matches the stored value (the exact bytes when not valid UTF-8)
                invalid = any("\udc80" <= c <= "\udcff" for c in garbage)
                self.assertEqual(
                    args[4], garbage.encode("utf-8", "surrogateescape") if invalid else garbage,
                )
                assert_poison_record(self, args[5], garbage, "orders")  # ADR-0035 record

    def test_invalid_utf8_reply_bytes_are_read_losslessly_and_settled_byte_exact(self):
        # ``decode_responses`` off (or ``encoding_errors=surrogateescape``): the reply holds bytes
        # that are not UTF-8. The poison record is the base64 wrapper, never U+FFFD, and the
        # settle script gets the stored bytes back untouched so the LREM actually matches.
        stored = b'{"job":"urn:a"}\xff\xfe'
        client = self.Client(stored)
        app = BabelQueue(
            transport=RedisTransport("redis://", client=client, retry_after=90),
            queue="orders",
        )
        app.register(URN, lambda data, meta: None)
        self.assertEqual(app.consume("orders", max_messages=1, timeout=0), 1)
        ((numkeys, args),) = client.settles
        self.assertEqual(args[4], stored)
        record = json.loads(args[5])
        self.assertEqual(record["raw_encoding"], "base64")
        self.assertEqual(base64.b64decode(record["raw"]), stored)
        self.assertNotIn("\ufffd", args[5])
        self.assertEqual(record["dead_letter"]["reason"], "poison")

    def test_a_decodable_envelope_with_invalid_utf8_is_poison_never_delivered(self):
        # A well-formed envelope whose ``data`` carries a raw 0xFF byte: cjson (and json with
        # surrogateescape) can decode it, but broker-bindings section 1.1 makes it poison. The
        # handler must not see it and nothing may be retried or re-published.
        stored = (
            b'{"job":"urn:a","attempts":0,"data":{"n":"\xff"},'
            b'"meta":{"id":"1","queue":"orders"}}'
        )
        for retries in (0, 5):
            with self.subTest(max_attempts=retries):
                client = self.Client(stored)
                handled = []
                app = BabelQueue(
                    transport=RedisTransport("redis://", client=client, retry_after=90),
                    queue="orders", max_attempts=retries or 3, dead_letter=bool(retries),
                )
                app.register("urn:a", lambda data, meta: handled.append(data))
                self.assertEqual(app.consume("orders", max_messages=1, timeout=0), 1)
                self.assertEqual(handled, [])
                ((numkeys, args),) = client.settles
                self.assertEqual((numkeys, args[2]), (3, "orders.dlq"))
                self.assertEqual(args[4], stored)  # settled by the exact stored bytes
                record = json.loads(args[5])
                self.assertEqual(record["raw_encoding"], "base64")
                self.assertEqual(base64.b64decode(record["raw"]), stored)
                self.assertNotIn("\ufffd", args[5])
                self.assertEqual(record["dead_letter"]["reason"], "poison")
                self.assertEqual(client.stored, [])  # nothing re-queued

    def test_wire_returns_valid_text_unchanged_and_escaped_bytes_exactly(self):
        self.assertEqual(_wire("plain \u00e9"), "plain \u00e9")
        self.assertEqual(_wire("a\udcffb"), b"a\xffb")
        self.assertEqual(_as_text(b"a\xffb"), "a\udcffb")
        self.assertEqual(_wire(_as_text(b"a\xffb\xc3\xa9")), b"a\xffb\xc3\xa9")

    def test_it_is_dead_lettered_even_with_the_dlq_off_and_never_retried(self):
        client = self.consume("garbage", max_attempts=5, dead_letter=False)
        ((numkeys, args),) = client.settles
        self.assertEqual((numkeys, args[2]), (3, "orders.dlq"))
        assert_poison_record(self, args[5], "garbage", "orders")
        self.assertEqual(client.stored, [])  # nothing re-queued

    def test_a_framed_garbage_body_keeps_its_headers_in_the_dead_letter(self):
        frame = json.dumps({"__bq_frame": 1, "headers": {"traceparent": "00-ab"}, "body": "garbage"})
        client = self.consume(frame, dead_letter=True)
        ((_, args),) = client.settles
        self.assertEqual(args[4], frame)  # the ack handle keeps the stored frame
        dead = json.loads(args[5])  # re-framed so the out-of-band headers survive the DLQ hop
        assert_poison_record(self, dead["body"], "garbage", "orders")
        self.assertEqual(dead["headers"], {"traceparent": "00-ab"})

    def test_a_body_go_typed_decode_rejects_is_poison_on_the_first_delivery(self):
        # Go's typed ``Decode`` rejects wrong member types, not just non-objects (review MAJOR 1).
        for garbage in (
            '{"job":"urn:a","attempts":"x"}',
            '{"job":"urn:a","attempts":1.5}',
            '{"job":"urn:a","meta":"oops"}',
        ):
            with self.subTest(body=garbage):
                client = self.Client(garbage)
                message = RedisTransport("redis://", client=client).pop("orders", timeout=0)
                self.assertEqual(message.route_urn, NON_NUMERIC_ATTEMPTS_ROUTE)
                client = self.consume(garbage, max_attempts=3, dead_letter=True)
                ((_, args),) = client.settles
                assert_poison_record(self, args[5], garbage, "orders")  # never handled or retried

    def test_a_bare_null_is_not_poison(self):
        client = self.Client("null")
        message = RedisTransport("redis://", client=client).pop("orders", timeout=0)
        self.assertEqual(message.route_urn, "")

    def test_an_identity_less_object_is_not_poison(self):
        # {} / an object without job or urn still takes the bounded FAIL path, not the poison DLQ.
        client = self.Client('{"attempts":0}')
        message = RedisTransport("redis://", client=client).pop("orders", timeout=0)
        self.assertEqual(message.route_urn, "")

    def test_a_valid_envelope_is_not_flagged(self):
        client = self.Client(make_body("orders"))
        message = RedisTransport("redis://", client=client).pop("orders", timeout=0)
        self.assertEqual(message.route_urn, "")

    def test_poison_is_flagged_with_the_non_numeric_attempts_route(self):
        client = self.Client("garbage")
        message = RedisTransport("redis://", client=client).pop("orders", timeout=0)
        self.assertEqual(message.route_urn, NON_NUMERIC_ATTEMPTS_ROUTE)
        self.assertEqual(message.body, "garbage")


class ZsetInvalidUtf8WithoutRedisTest(unittest.TestCase):
    """Laravel layout: a decodable envelope carrying invalid UTF-8 is poison (broker-free).

    The reserve script can decode and re-encode such a job (the member differs from the job, so it
    would look like a normal delivery); the transport must still hand it over as poison, and the
    dead-letter script gets the exact stored member back.
    """

    stored = (
        b'{"job":"urn:a","attempts":0,"data":{"n":"\xff"},'
        b'"meta":{"id":"1","queue":"orders"}}'
    )
    reserved = stored.replace(b'"attempts":0', b'"attempts":1')

    class Client:
        def __init__(self, job, reserved):
            self.pops = [[job, reserved]]
            self.dead = []

        def eval(self, script, numkeys, *args):
            if script == _POP:
                return self.pops.pop(0) if self.pops else None
            if script == _DEAD_LETTER:
                self.dead.append((numkeys, args))
                return 1
            return 0  # migrate

        def zrem(self, *args):  # pragma: no cover - poison goes through the dead-letter script
            raise AssertionError("poison must be settled through the dead-letter script")

        def blpop(self, *args):
            return None

    def test_it_is_dead_lettered_as_a_base64_wrapper_and_never_handled(self):
        client = self.Client(self.stored, self.reserved)
        handled = []
        app = BabelQueue(
            transport=RedisTransport("redis://", client=client, laravel_compat=True, retry_after=90),
            queue="orders",
        )
        app.register("urn:a", lambda data, meta: handled.append(data))
        self.assertEqual(app.consume("orders", max_messages=1, timeout=0), 1)
        self.assertEqual(handled, [])
        ((numkeys, args),) = client.dead
        self.assertEqual(numkeys, 3)
        self.assertEqual(args[3], self.reserved)  # ZREM by the exact reserved bytes
        record = json.loads(args[4])
        self.assertEqual(record["raw_encoding"], "base64")
        self.assertEqual(base64.b64decode(record["raw"]), self.stored)
        self.assertNotIn("\ufffd", args[4])
        self.assertEqual(record["dead_letter"]["reason"], "poison")


class _BrokerTest(unittest.TestCase):
    """Base for the tests that need Redis (real, else emulated, else skipped)."""

    laravel = False
    retry_after = 90

    def setUp(self) -> None:
        client, kind = redis_backend.connect()
        if client is None:
            if redis_backend.require_real_redis():
                self.fail("BQ_REQUIRE_REDIS is set but no Redis is reachable")
            self.skipTest("no Redis (real or emulated) available")
        self.client = client
        self.kind = kind
        self.queue = redis_backend.unique_queue()
        self.addCleanup(redis_backend.purge, client, self.queue)
        self.clock = Clock()
        self.tr = self.transport()

    def transport(self, **overrides) -> RedisTransport:
        params = dict(
            client=self.client,
            laravel_compat=self.laravel,
            retry_after=self.retry_after,
            clock=self.clock,
        )
        params.update(overrides)
        return RedisTransport("redis://", **params)  # type: ignore[arg-type]

    # key helpers -------------------------------------------------------------
    @property
    def ready(self) -> str:
        return f"queues:{self.queue}"

    @property
    def reserved(self) -> str:
        return f"{self.ready}:reserved"

    @property
    def delayed(self) -> str:
        return f"{self.ready}:delayed"

    @property
    def notify(self) -> str:
        return f"{self.ready}:notify"

    @property
    def processing(self) -> str:
        return f"{self.queue}:processing"

    @property
    def deadlines(self) -> str:
        return f"{self.queue}:processing:deadlines"

    def app(self, tr=None, **kwargs) -> BabelQueue:
        return BabelQueue(transport=tr or self.tr, queue=self.queue, **kwargs)

    @property
    def now(self) -> int:
        return int(self.clock.now)


class LaravelLayoutTest(_BrokerTest):
    laravel = True

    def test_publish_uses_laravel_keys_and_notify(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        self.assertEqual(self.client.lrange(self.ready, 0, -1), [body])
        self.assertEqual(self.client.llen(self.notify), 1)

    def test_pop_hands_the_raw_job_to_the_handler_and_reserves_attempts_plus_one(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        msg = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(msg.body, body)  # raw: attempts is still 0 for the handler
        self.assertEqual(msg.route_urn, "")
        members = self.client.zrange(self.reserved, 0, -1, withscores=True)
        self.assertEqual(len(members), 1)
        member, score = members[0]
        self.assertEqual(json.loads(member)["attempts"], 1)
        self.assertEqual(int(score), self.now + 90)
        self.assertEqual(msg.handle, member)
        self.assertEqual(self.client.llen(self.notify), 0)

    def test_ack_removes_the_reservation(self):
        self.tr.publish(self.queue, make_body(self.queue))
        msg = self.tr.pop(self.queue, timeout=0)
        self.tr.ack(msg)
        self.assertEqual(self.client.zcard(self.reserved), 0)

    def test_pop_empty_returns_none(self):
        self.assertIsNone(self.tr.pop(self.queue, timeout=0))

    def test_blocking_pop_drains_to_none_within_the_window(self):
        started = time.monotonic()
        self.assertIsNone(self.tr.pop(self.queue, timeout=0.4))
        self.assertLess(time.monotonic() - started, 2.0)

    def test_failed_attempt_is_counted_exactly_once(self):
        # Regression for the double count: the handler saw the raw job, a failure releases
        # raw attempts + 1 (not reserved attempts + 1 again).
        self.tr.publish(self.queue, make_body(self.queue))
        app = self.app(max_attempts=10)
        app.register(URN, lambda data, meta: (_ for _ in ()).throw(RuntimeError("boom")))
        app.dispatch(self.tr.pop(self.queue, timeout=0))
        delayed = self.client.zrange(self.delayed, 0, -1)
        self.assertEqual([json.loads(b)["attempts"] for b in delayed], [1])
        self.assertEqual(self.client.zcard(self.reserved), 0)
        self.clock.advance(1)
        again = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(json.loads(again.body)["attempts"], 1)
        app.dispatch(again)
        self.assertEqual([json.loads(b)["attempts"] for b in self.client.zrange(self.delayed, 0, -1)], [2])

    def test_failure_with_retry_backoff_lands_in_delayed_at_floor_now_plus_ceil_backoff(self):
        self.tr.publish(self.queue, make_body(self.queue))
        app = self.app(max_attempts=10, retry_backoff=2.5)
        app.register(URN, lambda data, meta: (_ for _ in ()).throw(RuntimeError("boom")))
        app.dispatch(self.tr.pop(self.queue, timeout=0))
        ((_, score),) = self.client.zrange(self.delayed, 0, -1, withscores=True)
        self.assertEqual(int(score), self.now + 3)  # floor(now) + ceil(2.5)
        # Not visible before the score, visible at it.
        self.assertIsNone(self.tr.pop(self.queue, timeout=0))
        self.clock.advance(2)
        self.assertIsNone(self.tr.pop(self.queue, timeout=0))
        self.clock.advance(1)
        self.assertIsNotNone(self.tr.pop(self.queue, timeout=0))

    def test_release_preserves_unknown_keys_and_empty_containers(self):
        raw = (
            '{"job":"%s","trace_id":"t","data":{},"meta":{"schema_version":1,"id":"i"},'
            '"attempts":0,"x-extra":{"keep":[]},"note":"é"}' % URN
        )
        self.tr.publish(self.queue, raw)
        msg = self.tr.pop(self.queue, timeout=0)
        self.tr.release(msg, 0)
        ((released,),) = [(b,) for b in self.client.zrange(self.delayed, 0, -1)]
        out = json.loads(released)
        self.assertEqual(out["attempts"], 1)
        self.assertEqual(out["data"], {})
        self.assertEqual(out["x-extra"], {"keep": []})
        self.assertEqual(out["note"], "é")
        self.assertEqual(self.client.zcard(self.reserved), 0)

    def test_release_negative_delay_clamps_to_zero(self):
        self.tr.publish(self.queue, make_body(self.queue))
        msg = self.tr.pop(self.queue, timeout=0)
        self.tr.release(msg, -5)
        ((_, score),) = self.client.zrange(self.delayed, 0, -1, withscores=True)
        self.assertEqual(int(score), self.now)

    def test_crashed_worker_reservation_is_recovered_and_the_next_consumer_sees_attempts_plus_one(self):
        self.tr.publish(self.queue, make_body(self.queue))
        crashed = self.tr.pop(self.queue, timeout=0)  # never settled: the worker died
        self.assertEqual(json.loads(crashed.body)["attempts"], 0)
        self.assertIsNone(self.tr.pop(self.queue, timeout=0))  # still reserved
        self.clock.advance(91)
        second = self.transport().pop(self.queue, timeout=0)
        self.assertIsNotNone(second)
        self.assertEqual(json.loads(second.body)["attempts"], 1)  # the lost attempt counted once

    def test_retry_after_none_never_recovers_and_reserves_with_plus_inf(self):
        tr = self.transport(retry_after=None)
        tr.publish(self.queue, make_body(self.queue))
        tr.pop(self.queue, timeout=0)
        ((_, score),) = self.client.zrange(self.reserved, 0, -1, withscores=True)
        self.assertEqual(score, float("inf"))
        self.clock.advance(10_000_000)
        self.assertIsNone(tr.pop(self.queue, timeout=0))
        self.assertEqual(self.client.zcard(self.reserved), 1)

    def assert_dlq_poison_records(self, poison):
        dlq = self.client.lrange(f"{self.ready}.dlq", 0, -1)
        self.assertEqual(len(dlq), len(poison))
        for record, body in zip(dlq, poison):
            assert_poison_record(self, record, body, self.queue)

    def test_poison_bodies_reserve_raw_and_dead_letter_as_poison_records_without_residue(self):
        poison = ["not json at all", '{"job":"%s","attempts":"x"}' % URN, "[1,2,3]", '{"job":"%s"}' % URN]
        for raw in poison[:2]:
            self.tr.publish(self.queue, raw)
        app = self.app(dead_letter=True)
        app.register(URN, lambda data, meta: None)
        for raw in poison[:2]:
            msg = self.tr.pop(self.queue, timeout=0)
            self.assertEqual(msg.body, raw)
            self.assertEqual(msg.route_urn, NON_NUMERIC_ATTEMPTS_ROUTE)
            self.assertEqual(msg.handle, raw)  # reserved member == the job
            app.dispatch(msg)
        self.assertEqual(self.client.exists(f"queues:{self.queue}.dlq"), 1)
        self.assert_dlq_poison_records(poison[:2])  # ADR-0035 records
        for key in (self.ready, self.reserved, self.delayed):
            self.assertEqual(self.client.exists(key), 0, key)

    def test_poison_is_dead_lettered_with_the_default_configuration(self):
        # §1.1 / ADR-0033: the DLQ write is unconditional, so ``dead_letter_enabled`` (default
        # False) must not turn poison into a silent drop.
        poison = ["garbage", '{"job":"%s","attempts":"3"}' % URN]
        for raw in poison:
            self.tr.publish(self.queue, raw)
        app = self.app()
        self.assertFalse(app.dead_letter_enabled)
        for _ in poison:
            app.dispatch(self.tr.pop(self.queue, timeout=0))
        self.assert_dlq_poison_records(poison)
        self.assertEqual(self.client.llen(f"{self.ready}.dlq:notify"), len(poison))
        for key in (self.ready, self.reserved, self.delayed):
            self.assertEqual(self.client.exists(key), 0, key)

    def test_poison_dead_letter_failure_leaves_it_reserved_not_dropped(self):
        self.tr.publish(self.queue, "garbage")
        app = self.app()
        msg = self.tr.pop(self.queue, timeout=0)
        original = self.tr.dead_letter

        def boom(*args, **kwargs):
            raise RuntimeError("dlq write refused")

        self.tr.dead_letter = boom
        try:
            with self.assertLogs("babelqueue.app", level="ERROR"):
                app.dispatch(msg)
        finally:
            self.tr.dead_letter = original
        self.assertEqual(self.client.exists(f"{self.ready}.dlq"), 0)
        self.assertEqual(self.client.zcard(self.reserved) + self.client.llen(self.ready), 1)

    def test_bodies_real_redis_cannot_re_encode_are_reserved_raw_not_lost(self):
        # ``1e400`` decodes to inf; lua-cjson then refuses to encode it. The emulator accepts it
        # (``Infinity``), so this proof only means something on a real Redis; the same script is
        # also proven against a failing cjson stub in PopScriptWithoutRedisTest.
        if self.kind != "real":
            self.skipTest("the emulator's cjson masks this failure; needs a real Redis")
        poison = [
            '{"attempts":0,"data":{"n":1e400}}',
            '{"attempts":1e400}',
            '{"attempts":0,"data":{"x":NaN}}',
        ]
        for raw in poison:
            self.tr.publish(self.queue, raw)
        app = self.app(dead_letter=True)
        app.register(URN, lambda data, meta: None)
        for raw in poison:
            msg = self.tr.pop(self.queue, timeout=0)
            self.assertIsNotNone(msg, raw)
            self.assertEqual(msg.body, raw)
            self.assertEqual(msg.route_urn, NON_NUMERIC_ATTEMPTS_ROUTE)
            self.assertEqual(msg.handle, raw)
            app.dispatch(msg)
        self.assert_dlq_poison_records(poison)
        for key in (self.ready, self.reserved, self.delayed):
            self.assertEqual(self.client.exists(key), 0, key)

    def test_poison_is_dead_lettered_even_with_the_delete_unknown_urn_strategy(self):
        poison = ["not-json", '{"job":"%s","attempts":"3"}' % URN]
        for raw in poison:
            self.tr.publish(self.queue, raw)
        app = self.app(dead_letter=True, on_unknown_urn=UnknownUrnStrategy.DELETE)
        app.register(URN, lambda data, meta: None)
        for _ in poison:
            app.dispatch(self.tr.pop(self.queue, timeout=0))
        self.assert_dlq_poison_records(poison)
        self.assertEqual(self.client.zcard(self.reserved), 0)

    def test_a_genuine_unknown_urn_is_still_deleted_with_the_delete_strategy(self):
        self.tr.publish(self.queue, make_body(self.queue))
        app = self.app(dead_letter=True, on_unknown_urn=UnknownUrnStrategy.DELETE)
        app.dispatch(self.tr.pop(self.queue, timeout=0))  # no handler for URN
        self.assertEqual(self.client.exists(f"{self.ready}.dlq"), 0)
        self.assertEqual(self.client.zcard(self.reserved), 0)

    def test_dead_letter_moves_the_reservation_to_the_dlq_in_one_script(self):
        self.tr.publish(self.queue, make_body(self.queue))
        msg = self.tr.pop(self.queue, timeout=0)
        self.tr.dead_letter(msg, f"{self.queue}.dlq", "record")
        self.assertEqual(self.client.lrange(f"{self.ready}.dlq", 0, -1), ["record"])
        self.assertEqual(self.client.llen(f"{self.ready}.dlq:notify"), 1)
        self.assertEqual(self.client.zcard(self.reserved), 0)

    def test_dead_letter_of_a_stale_reservation_writes_nothing(self):
        self.tr.publish(self.queue, make_body(self.queue))
        slow = self.tr.pop(self.queue, timeout=0)
        self.clock.advance(91)
        taken = self.transport().pop(self.queue, timeout=0)  # migrated back, re-reserved by another worker
        self.assertIsNotNone(taken)
        with self.assertLogs("babelqueue.redis", level="WARNING") as logs:
            self.tr.dead_letter(slow, f"{self.queue}.dlq", "record")
        self.assertIn("expired", logs.output[0])
        self.assertEqual(self.client.exists(f"{self.ready}.dlq"), 0)
        self.assertEqual(self.client.zcard(self.reserved), 1)  # the new owner's reservation is intact

    def test_dead_letter_script_is_gated_on_the_member(self):
        keys = (self.reserved, f"{self.ready}.dlq", f"{self.ready}.dlq:notify")
        self.assertEqual(self.client.eval(_DEAD_LETTER, 3, *keys, "ghost", "body"), 0)
        self.assertEqual(self.client.exists(keys[1]), 0)
        self.client.zadd(self.reserved, {"member": 1})
        self.assertEqual(self.client.eval(_DEAD_LETTER, 3, *keys, "member", "body"), 1)
        self.assertEqual(self.client.lrange(keys[1], 0, -1), ["body"])
        self.assertEqual(self.client.zcard(self.reserved), 0)
        self.addCleanup(self.client.delete, *keys)

    def test_dead_letter_without_a_handle_is_a_plain_publish(self):
        orphan = ReceivedMessage(body="x", queue=self.queue, handle=None)
        self.tr.dead_letter(orphan, f"{self.queue}.dlq", "record")
        self.assertEqual(self.client.lrange(f"{self.ready}.dlq", 0, -1), ["record"])

    def test_publish_delayed_scores_floor_now_plus_ceil_delay(self):
        self.tr.publish_delayed(self.queue, make_body(self.queue), 1.2)
        ((_, score),) = self.client.zrange(self.delayed, 0, -1, withscores=True)
        self.assertEqual(int(score), self.now + 2)
        self.assertEqual(self.client.llen(self.ready), 0)
        self.assertEqual(self.client.llen(self.notify), 0)  # no notify token on a delayed add
        self.assertIsNone(self.tr.pop(self.queue, timeout=0))
        self.clock.advance(2)
        self.assertIsNotNone(self.tr.pop(self.queue, timeout=0))

    def test_publish_delayed_negative_delay_is_zero(self):
        self.tr.publish_delayed(self.queue, make_body(self.queue), -3)
        ((_, score),) = self.client.zrange(self.delayed, 0, -1, withscores=True)
        self.assertEqual(int(score), self.now)

    def test_delayed_message_arrives_after_about_two_seconds(self):
        real = RedisTransport("redis://", client=self.client, laravel_compat=True)
        real.publish_delayed(self.queue, make_body(self.queue), 2)
        started = time.monotonic()
        msg = real.pop(self.queue, timeout=5)
        elapsed = time.monotonic() - started
        self.assertIsNotNone(msg)
        self.assertGreaterEqual(elapsed, 1.0)
        self.assertLessEqual(elapsed, 3.5)

    def test_migration_batch_size_bounds_one_migration(self):
        tr = self.transport(migration_batch_size=1)
        for i in range(3):
            tr.publish_delayed(self.queue, make_body(self.queue, {"i": i}), 0)
        self.assertEqual(self.client.zcard(self.delayed), 3)
        self.assertIsNotNone(tr.pop(self.queue, timeout=0))
        self.assertEqual(self.client.zcard(self.delayed), 2)  # one moved per pop

    def test_laravel_compat_headers_degrade_to_a_bare_publish(self):
        body = make_body(self.queue)
        self.tr.publish_with_headers(self.queue, body, {"traceparent": "00-x"})
        self.assertEqual(self.client.lrange(self.ready, 0, -1), [body])

    def test_round_trip_through_app(self):
        app = self.app()
        seen = {}

        @app.handler(URN)
        def _on(data, meta):  # noqa: ANN001
            seen.update(data=data, meta=meta)

        msg_id = app.publish(URN, {"order_id": 7})
        self.assertEqual(app.consume(self.queue, max_messages=1, timeout=0), 1)
        self.assertEqual(seen["data"]["order_id"], 7)
        self.assertEqual(seen["meta"]["id"], msg_id)
        self.assertEqual(self.client.zcard(self.reserved), 0)

    def test_retries_run_out_into_the_dead_letter_queue(self):
        app = self.app(max_attempts=2, dead_letter=True)
        app.register(URN, lambda data, meta: (_ for _ in ()).throw(RuntimeError("boom")))
        self.tr.publish(self.queue, make_body(self.queue))
        app.dispatch(self.tr.pop(self.queue, timeout=0))
        self.clock.advance(1)
        app.dispatch(self.tr.pop(self.queue, timeout=0))
        (record,) = self.client.lrange(f"{self.ready}.dlq", 0, -1)
        self.assertEqual(json.loads(record)["attempts"], 2)
        self.assertEqual(self.client.zcard(self.reserved), 0)
        self.assertEqual(self.client.zcard(self.delayed), 0)


class LegacyLayoutTest(_BrokerTest):
    laravel = False

    def test_publish_uses_the_raw_queue_key(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        self.assertEqual(self.client.lrange(self.queue, 0, -1), [body])

    def test_publish_delayed_is_a_laravel_layout_feature(self):
        with self.assertRaises(BabelQueueError):
            self.tr.publish_delayed(self.queue, make_body(self.queue), 5)
        self.assertEqual(self.client.exists(self.queue), 0)

    def test_pop_reserves_with_a_token_deadline_and_ack_clears_everything(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        msg = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(msg.body, body)
        self.assertEqual(self.client.lrange(self.processing, 0, -1), [body])
        ((member, score),) = self.client.zrange(self.deadlines, 0, -1, withscores=True)
        token, _, raw = member.partition(":")
        self.assertRegex(token, TOKEN)
        self.assertEqual(raw, body)
        self.assertEqual(int(score), self.now + 90)
        self.tr.ack(msg)
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)

    def test_pop_empty_returns_none(self):
        self.assertIsNone(self.tr.pop(self.queue, timeout=0))

    def test_blocking_pop_picks_up_a_message_published_while_waiting(self):
        body = make_body(self.queue)
        timer = threading.Timer(0.3, lambda: self.client.rpush(self.queue, body))
        timer.start()
        self.addCleanup(timer.cancel)
        msg = self.tr.pop(self.queue, timeout=3)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.body, body)

    def test_each_reservation_gets_its_own_token(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        self.tr.publish(self.queue, body)
        a = self.tr.pop(self.queue, timeout=0)
        b = self.tr.pop(self.queue, timeout=0)
        self.assertNotEqual(a.handle.token, b.handle.token)
        self.assertRegex(a.handle.token, TOKEN)
        self.assertRegex(b.handle.token, TOKEN)

    def test_kill_9_worker_is_swept_back_to_the_end_with_attempts_untouched(self):
        a, b, c = (make_body(self.queue, {"n": n}) for n in "abc")
        for body in (a, b, c):
            self.tr.publish(self.queue, body)
        lost = self.tr.pop(self.queue, timeout=0)  # the worker is killed here: no ack, no release
        self.assertEqual(lost.body, a)
        self.clock.advance(91)  # past the retry-after deadline
        survivor = self.transport()
        order = []
        for _ in range(3):
            msg = survivor.pop(self.queue, timeout=0)
            order.append(msg.body)
            survivor.ack(msg)
        self.assertEqual(order, [b, c, a])  # swept to the END
        self.assertEqual(json.loads(order[-1])["attempts"], 0)
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)

    def test_within_the_deadline_nothing_is_swept(self):
        self.tr.publish(self.queue, make_body(self.queue))
        self.tr.pop(self.queue, timeout=0)
        self.clock.advance(89)
        self.assertIsNone(self.transport().pop(self.queue, timeout=0))
        self.assertEqual(self.client.llen(self.processing), 1)

    def test_late_ack_touches_nothing_and_warns(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        slow = self.tr.pop(self.queue, timeout=0)
        self.clock.advance(91)
        other = self.transport()
        taken = other.pop(self.queue, timeout=0)  # swept and re-reserved under a new token
        self.assertEqual(taken.body, body)
        with self.assertLogs("babelqueue.redis", level="WARNING") as logs:
            self.tr.ack(slow)  # the slow worker's own reservation is gone
        self.assertIn("expired", logs.output[0])
        self.assertEqual(self.client.lrange(self.processing, 0, -1), [body])  # the new owner's
        self.assertEqual(self.client.zcard(self.deadlines), 1)
        other.ack(taken)
        self.assertEqual(self.client.llen(self.processing), 0)

    def test_identical_copies_are_settled_one_reservation_at_a_time(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        self.tr.publish(self.queue, body)
        first = self.tr.pop(self.queue, timeout=0)
        second = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(self.client.llen(self.processing), 2)
        self.tr.ack(first)
        self.assertEqual(self.client.llen(self.processing), 1)
        self.assertEqual(self.client.zcard(self.deadlines), 1)
        ((member, _),) = self.client.zrange(self.deadlines, 0, -1, withscores=True)
        self.assertTrue(member.startswith(second.handle.token))
        self.tr.ack(second)
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)

    def test_retry_after_none_reserves_with_plus_inf_and_never_sweeps(self):
        tr = self.transport(retry_after=None)
        tr.publish(self.queue, make_body(self.queue))
        tr.pop(self.queue, timeout=0)
        ((_, score),) = self.client.zrange(self.deadlines, 0, -1, withscores=True)
        self.assertEqual(score, float("inf"))
        self.clock.advance(10_000_000)
        self.assertIsNone(tr.pop(self.queue, timeout=0))
        self.assertEqual(self.client.llen(self.processing), 1)

    def test_release_ignores_the_delay_and_requeues_at_the_end(self):
        first, second = make_body(self.queue, {"n": 1}), make_body(self.queue, {"n": 2})
        self.tr.publish(self.queue, first)
        self.tr.publish(self.queue, second)
        msg = self.tr.pop(self.queue, timeout=0)
        self.tr.release(msg, 30)  # no delayed set in the legacy layout: delay is ignored
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)
        stored = self.client.lrange(self.queue, 0, -1)
        self.assertEqual(stored[0], second)
        self.assertEqual(json.loads(stored[1])["attempts"], 1)

    def test_release_reframes_the_headers(self):
        body = make_body(self.queue)
        self.tr.publish_with_headers(self.queue, body, {"traceparent": "00-abc"})
        msg = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(msg.headers, {"traceparent": "00-abc"})
        self.tr.release(msg, 0)
        again = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(again.headers, {"traceparent": "00-abc"})
        self.assertEqual(json.loads(again.body)["attempts"], 1)

    def test_dead_letter_writes_and_settles_as_one_step(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        msg = self.tr.pop(self.queue, timeout=0)
        self.tr.dead_letter(msg, f"{self.queue}.dlq", "record")
        self.assertEqual(self.client.lrange(f"{self.queue}.dlq", 0, -1), ["record"])
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)

    def test_dead_letter_of_a_stale_reservation_writes_nothing(self):
        self.tr.publish(self.queue, make_body(self.queue))
        slow = self.tr.pop(self.queue, timeout=0)
        self.clock.advance(91)
        self.transport().pop(self.queue, timeout=0)  # sweeps and re-reserves
        with self.assertLogs("babelqueue.redis", level="WARNING"):
            self.tr.dead_letter(slow, f"{self.queue}.dlq", "record")
        self.assertEqual(self.client.exists(f"{self.queue}.dlq"), 0)

    def test_settle_script_takes_token_and_raw_and_builds_the_member_itself(self):
        body = make_body(self.queue)
        self.tr.publish(self.queue, body)
        msg = self.tr.pop(self.queue, timeout=0)
        token, raw = msg.handle.token, msg.handle.raw
        keys = (self.processing, self.deadlines)
        self.assertEqual(self.client.eval(_SETTLE, 2, *keys, "0" * 32, raw), 0)  # foreign token
        self.assertEqual(self.client.llen(self.processing), 1)
        self.assertEqual(self.client.eval(_SETTLE, 2, *keys, token, raw), 1)  # ARGV = token, raw
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)

    def test_settle_script_pushes_the_body_to_the_target(self):
        self.tr.publish(self.queue, make_body(self.queue))
        msg = self.tr.pop(self.queue, timeout=0)
        target = f"{self.queue}.dlq"
        self.addCleanup(self.client.delete, target)
        keys = (self.processing, self.deadlines, target)
        self.assertEqual(self.client.eval(_SETTLE, 3, *keys, msg.handle.token, msg.handle.raw, "record"), 1)
        self.assertEqual(self.client.lrange(target, 0, -1), ["record"])

    def test_the_processing_key_name_is_unchanged(self):
        self.tr.publish(self.queue, make_body(self.queue))
        self.tr.pop(self.queue, timeout=0)
        self.assertEqual(self.client.exists(f"{self.queue}:processing"), 1)

    def test_legacy_is_the_default_layout(self):
        self.assertFalse(RedisTransport("redis://", client=self.client)._laravel_compat)

    def test_round_trip_through_app_and_retry_to_the_dead_letter_queue(self):
        app = self.app(max_attempts=2, dead_letter=True)
        calls = []

        def boom(data, meta):  # noqa: ANN001
            calls.append(1)
            raise RuntimeError("boom")

        app.register(URN, boom)
        app.publish(URN, {"x": 1})
        self.assertEqual(app.consume(self.queue, max_messages=2, timeout=0), 2)
        self.assertEqual(len(calls), 2)
        (record,) = self.client.lrange(f"{self.queue}.dlq", 0, -1)
        self.assertEqual(json.loads(record)["attempts"], 2)
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)

    def test_undecodable_body_is_dead_lettered_raw_on_first_delivery(self):
        garbage = "not json {"
        self.client.rpush(self.queue, garbage)
        app = self.app(max_attempts=3, dead_letter=True)
        app.register(URN, lambda data, meta: None)
        self.assertEqual(app.consume(self.queue, max_messages=1, timeout=0), 1)
        (record,) = self.client.lrange(f"{self.queue}.dlq", 0, -1)
        assert_poison_record(self, record, garbage, self.queue)  # ADR-0035 wrapper, not the raw body
        self.assertEqual(self.client.llen(self.processing), 0)
        self.assertEqual(self.client.zcard(self.deadlines), 0)
        self.assertEqual(self.client.llen(self.queue), 0)  # never re-queued for a retry

    def test_undecodable_body_is_dead_lettered_with_the_dlq_off_too(self):
        self.client.rpush(self.queue, "[1]")
        app = self.app(max_attempts=3, dead_letter=False)
        app.register(URN, lambda data, meta: None)
        self.assertEqual(app.consume(self.queue, max_messages=1, timeout=0), 1)
        (record,) = self.client.lrange(f"{self.queue}.dlq", 0, -1)
        assert_poison_record(self, record, "[1]", self.queue)
        self.assertEqual(self.client.llen(self.processing), 0)

    def test_publish_with_headers_stores_a_frame_and_pop_unframes(self):
        body = make_body(self.queue)
        self.tr.publish_with_headers(self.queue, body, {"traceparent": "00-deadbeef"})
        (stored,) = self.client.lrange(self.queue, 0, -1)
        self.assertIn('"__bq_frame"', stored)
        msg = self.tr.pop(self.queue, timeout=0)
        self.assertEqual(msg.body, body)
        self.assertEqual(msg.headers.get("traceparent"), "00-deadbeef")
        self.assertEqual(msg.handle.raw, stored)  # the ack handle keeps the stored frame
        self.tr.ack(msg)
        self.assertEqual(self.client.llen(self.processing), 0)

    def test_plain_publish_then_pop_has_no_headers(self):
        self.tr.publish(self.queue, make_body(self.queue))
        self.assertEqual(self.tr.pop(self.queue, timeout=0).headers, {})

    def test_publish_with_no_headers_stays_bare(self):
        body = make_body(self.queue)
        self.tr.publish_with_headers(self.queue, body, {})
        self.assertEqual(self.client.lrange(self.queue, 0, -1), [body])

    def test_warning_logger_name(self):
        self.assertEqual(logging.getLogger("babelqueue.redis").name, "babelqueue.redis")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
