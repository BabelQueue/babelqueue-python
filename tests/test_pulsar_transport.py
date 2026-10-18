"""Apache Pulsar transport tests.

The property projection and the ``attempts = max(body, redelivery_count)`` reconciliation —
and the full publish / pop / ack flow — run with no broker and no ``pulsar-client`` (the
transport's pulsar import is lazy and the publish path sends raw bytes, so a duck-typed fake
client covers everything). Only ``_build_client`` needs the real package.
"""

from __future__ import annotations

import json
import unittest

from babelqueue import EnvelopeCodec
from babelqueue.pulsar_transport import (
    UNSUPPORTED_SCHEMA_ROUTE_PREFIX,
    PulsarTransport,
    _gate_route,
)
from babelqueue.transport import ReceivedMessage, make_transport

try:
    import pulsar  # noqa: F401

    HAVE_PULSAR = True
except ImportError:
    HAVE_PULSAR = False

URN = "urn:babel:orders:created"


def body(attempts: int = 0) -> str:
    env = EnvelopeCodec.decode(
        EnvelopeCodec.encode(EnvelopeCodec.make(URN, {"order_id": 1042}, queue="orders"))
    )
    env["attempts"] = attempts
    return EnvelopeCodec.encode(env)


def _with_forbidden_key(raw: str) -> str:
    """``raw`` plus a forbidden top-level key (message-envelope.md §10), as a legacy producer sends."""
    return raw[:-1] + ',"timestamp":1}'


def _assert_no_codec_warnings(case: unittest.TestCase, fn) -> None:
    """Run ``fn`` and assert the codec logged nothing (``assertNoLogs`` needs Python 3.10+)."""
    import logging

    with case.assertLogs("babelqueue.codec", level="WARNING") as cm:
        fn()
        logging.getLogger("babelqueue.codec").warning("sentinel")
    case.assertEqual(cm.output, ["WARNING:babelqueue.codec:sentinel"])


class Timeout(Exception):
    """Duck-typed stand-in for ``pulsar.Timeout`` (matched by class name)."""


class FakeMessage:
    """Duck-typed pulsar Message: data()/redelivery_count() are callables."""

    def __init__(self, raw: str, redelivery_count: int = 0, properties=None) -> None:
        self._raw = raw
        self._rc = redelivery_count
        self._props = properties

    def data(self) -> bytes:
        return self._raw.encode("utf-8")

    def redelivery_count(self) -> int:
        return self._rc

    def properties(self):
        return self._props


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list = []

    def send(self, content, properties=None) -> None:
        self.sent.append((content, properties))


class FakeConsumer:
    def __init__(self, messages=()) -> None:
        self._messages = list(messages)
        self.acked: list = []
        self.subscribe_kwargs: dict = {}

    def receive(self, timeout_millis=None):
        if not self._messages:
            raise Timeout()
        return self._messages.pop(0)

    def acknowledge(self, message) -> None:
        self.acked.append(message)


class FakeClient:
    def __init__(self, producer=None, consumer=None) -> None:
        self.producer = producer if producer is not None else FakeProducer()
        self.consumer = consumer if consumer is not None else FakeConsumer()
        self.subscribed: list = []

    def create_producer(self, topic):
        return self.producer

    def subscribe(self, topic, subscription, **kwargs):
        self.subscribed.append((topic, subscription, kwargs))
        self.consumer.subscribe_kwargs = kwargs
        return self.consumer


class PulsarProjectionTest(unittest.TestCase):
    def test_projection_maps_contract_properties(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        props = PulsarTransport._projection(raw)

        self.assertEqual(props["bq-job"], URN)
        self.assertEqual(props["bq-trace-id"], env["trace_id"])
        self.assertEqual(props["bq-message-id"], env["meta"]["id"])
        self.assertEqual(props["bq-schema-version"], str(env["meta"]["schema_version"]))
        self.assertEqual(props["bq-source-lang"], env["meta"]["lang"])
        self.assertEqual(props["bq-attempts"], "0")

    def test_projection_values_are_all_strings(self):
        props = PulsarTransport._projection(body(3))
        self.assertTrue(all(isinstance(v, str) for v in props.values()))
        self.assertEqual(props["bq-attempts"], "3")

    def test_projection_of_garbage_is_empty(self):
        self.assertEqual(PulsarTransport._projection("not-json"), {})

    def test_reconcile_attempts_is_redelivery_count(self):
        out = PulsarTransport._reconcile(body(0), 2)[0]
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 2)

    def test_reconcile_first_delivery_is_zero(self):
        out = PulsarTransport._reconcile(body(0), 0)[0]
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 0)

    def test_reconcile_ignores_garbage_count(self):
        raw = body(5)
        self.assertEqual(PulsarTransport._reconcile(raw, "not-a-number"), (raw, ""))

    def test_reconcile_never_lowers_runtime_count(self):
        # The runtime retries by republishing with attempts+1 (redelivery count back to 0),
        # so a higher body count must win over a lower native one.
        raw = body(5)
        self.assertEqual(int(EnvelopeCodec.decode(PulsarTransport._reconcile(raw, 1)[0])["attempts"]), 5)


class PulsarPublishTest(unittest.TestCase):
    def test_publish_sends_payload_with_property_projection(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        producer = FakeProducer()
        tr = PulsarTransport(client=FakeClient(producer=producer))

        tr.publish("orders", raw)

        self.assertEqual(len(producer.sent), 1)
        content, props = producer.sent[0]
        self.assertEqual(content, raw.encode("utf-8"))
        self.assertEqual(props["bq-job"], URN)
        self.assertEqual(props["bq-message-id"], env["meta"]["id"])
        self.assertEqual(EnvelopeCodec.urn(EnvelopeCodec.decode(content.decode("utf-8"))), URN)

    def test_projection_does_not_log_a_forbidden_key_drop(self):
        # The payload is sent unchanged, so the property projection must not claim a drop.
        raw = _with_forbidden_key(body())
        producer = FakeProducer()
        tr = PulsarTransport(client=FakeClient(producer=producer))
        _assert_no_codec_warnings(self, lambda: tr.publish("orders", raw))
        content, props = producer.sent[0]
        self.assertEqual(content, raw.encode("utf-8"))
        self.assertEqual(props["bq-job"], URN)


class PulsarConsumeTest(unittest.TestCase):
    def test_pop_reconciles_attempts_and_returns_handle(self):
        message = FakeMessage(body(0), redelivery_count=2)
        tr = PulsarTransport(client=FakeClient(consumer=FakeConsumer([message])))

        msg = tr.pop("orders")

        self.assertIsNotNone(msg)
        self.assertEqual(msg.queue, "orders")
        self.assertIs(msg.handle, message)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 2)

    def _pop_message(self, raw: str, properties, redelivery_count: int = 0, **options):
        message = FakeMessage(raw, redelivery_count=redelivery_count, properties=properties)
        tr = PulsarTransport(client=FakeClient(consumer=FakeConsumer([message])), **options)
        popped = tr.pop("orders")
        self.assertIsNotNone(popped)
        return popped

    def _pop(self, raw: str, properties, redelivery_count: int = 0, **options) -> dict:
        return EnvelopeCodec.decode_lenient(self._pop_message(raw, properties, redelivery_count, **options).body)

    def _body_with_urn(self, urn: str, attempts: int = 0) -> str:
        env = EnvelopeCodec.decode(body(attempts))
        env["job"] = urn
        return EnvelopeCodec.encode(env)

    def test_pop_routes_on_bq_job_property_without_rewriting_the_body(self):
        other = "urn:babel:billing:invoiced"
        raw = self._body_with_urn(other)
        calls = []
        popped = self._pop_message(
            raw,
            {"bq-job": URN},
            on_routing_mismatch=lambda prop, body_urn, envelope: calls.append((prop, body_urn, envelope)),
        )
        self.assertEqual(popped.route_urn, URN)
        self.assertEqual(popped.body, raw)  # byte-identical: the producer's body is untouched
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], URN)
        self.assertEqual(calls[0][1], other)
        self.assertEqual(calls[0][2]["job"], other)
        self.assertEqual(calls[0][2]["data"], {"order_id": 1042})

    def test_bq_job_property_is_trimmed_with_the_ascii_set_only(self):
        # ADR-0032 section 5: ASCII whitespace is trimmed, U+00A0 / U+2003 / U+3000 are part of the value.
        raw = self._body_with_urn("urn:babel:billing:invoiced")
        for padded, expected in (
            (f" \t{URN}\n\v\f\r ", URN),
            (f"\u00a0{URN}\u00a0", f"\u00a0{URN}\u00a0"),
            (f"\u2003{URN}\u3000", f"\u2003{URN}\u3000"),
            (f" \u00a0{URN} ", f"\u00a0{URN}"),
        ):
            with self.subTest(bq_job=repr(padded)):
                self.assertEqual(self._pop_message(raw, {"bq-job": padded}).route_urn, expected)
        # a property of only ASCII whitespace names no route
        self.assertEqual(self._pop_message(raw, {"bq-job": " \t\r\n"}).route_urn, "")

    def test_pop_routing_works_without_a_hook(self):
        popped = self._pop_message(self._body_with_urn("urn:babel:billing:invoiced"), {"bq-job": URN})
        self.assertEqual(popped.route_urn, URN)

    def test_app_routes_on_bq_job_and_retry_keeps_the_original_body(self):
        from babelqueue import BabelQueue

        other = "urn:babel:billing:invoiced"
        raw = self._body_with_urn(other)
        producer = FakeProducer()
        consumer = FakeConsumer([FakeMessage(raw, properties={"bq-job": URN})])
        tr = PulsarTransport(client=FakeClient(producer=producer, consumer=consumer))
        app = BabelQueue(transport=tr, max_attempts=3)
        seen = []

        @app.handler(URN)
        def on_created(data, meta, envelope):
            seen.append(EnvelopeCodec.urn(envelope))
            raise RuntimeError("boom")

        app.consume(queue="orders", max_messages=1)

        self.assertEqual(seen, [other])  # handler got the producer's body, not a rewritten one
        retried = EnvelopeCodec.decode(producer.sent[0][0].decode("utf-8"))
        self.assertEqual(retried["job"], other)
        self.assertEqual(retried["attempts"], 1)
        # The re-published message keeps the ORIGINAL bq-job property (not the body URN) ...
        self.assertEqual(producer.sent[0][1]["bq-job"], URN)
        self.assertEqual(producer.sent[0][1]["bq-attempts"], "1")

        # ... so the broker's second delivery is routed to the same handler.
        consumer._messages.append(FakeMessage(producer.sent[0][0].decode("utf-8"), properties=producer.sent[0][1]))
        app.consume(queue="orders", max_messages=1)
        self.assertEqual(seen, [other, other])

    def test_app_unknown_urn_release_keeps_the_original_route(self):
        from babelqueue import BabelQueue, UnknownUrnStrategy

        routed = "urn:babel:orders:unregistered"
        raw = self._body_with_urn(URN)
        producer = FakeProducer()
        consumer = FakeConsumer([FakeMessage(raw, properties={"bq-job": routed})])
        tr = PulsarTransport(client=FakeClient(producer=producer, consumer=consumer))
        app = BabelQueue(transport=tr, on_unknown_urn=UnknownUrnStrategy.RELEASE)
        seen = []

        @app.handler(URN)  # the body URN has a handler; the route URN does not
        def on_created(data, meta):
            seen.append(data)

        app.consume(queue="orders", max_messages=1)

        self.assertEqual(len(producer.sent), 1)
        self.assertEqual(producer.sent[0][0].decode("utf-8"), raw)
        self.assertEqual(producer.sent[0][1]["bq-job"], routed)

        consumer._messages.append(FakeMessage(producer.sent[0][0].decode("utf-8"), properties=producer.sent[0][1]))
        app.consume(queue="orders", max_messages=1)
        self.assertEqual(seen, [])  # still unknown, not flipped to the body URN's handler
        self.assertEqual(len(producer.sent), 2)
        self.assertEqual(producer.sent[1][1]["bq-job"], routed)

    def test_app_dead_letter_record_keeps_the_original_route(self):
        from babelqueue import BabelQueue

        other = "urn:babel:billing:invoiced"
        raw = self._body_with_urn(other)
        producer = FakeProducer()
        consumer = FakeConsumer([FakeMessage(raw, properties={"bq-job": URN})])
        tr = PulsarTransport(client=FakeClient(producer=producer, consumer=consumer))
        app = BabelQueue(transport=tr, max_attempts=1, dead_letter=True)

        @app.handler(URN)
        def on_created(data, meta):
            raise RuntimeError("boom")

        app.consume(queue="orders", max_messages=1)

        self.assertEqual(len(producer.sent), 1)
        dead = EnvelopeCodec.decode(producer.sent[0][0].decode("utf-8"))
        self.assertEqual(dead["job"], other)  # the producer's body, annotated
        self.assertEqual(dead["dead_letter"]["original_queue"], "orders")
        # The DLQ record carries the ORIGINAL bq-job, not one derived from the body URN.
        self.assertEqual(producer.sent[0][1]["bq-job"], URN)

    def test_publish_routed_pins_bq_job_and_blank_derives_from_body(self):
        producer = FakeProducer()
        tr = PulsarTransport(client=FakeClient(producer=producer))
        raw = self._body_with_urn("urn:babel:billing:invoiced")

        tr.publish_routed("orders", raw, URN)
        tr.publish_routed("orders", raw, "")
        tr.publish("orders", raw)

        self.assertEqual(producer.sent[0][1]["bq-job"], URN)
        self.assertEqual(producer.sent[1][1]["bq-job"], "urn:babel:billing:invoiced")
        self.assertEqual(producer.sent[2][1]["bq-job"], "urn:babel:billing:invoiced")

    def test_routing_mismatch_hook_fires_only_on_mismatch(self):
        calls = []
        hook = lambda *args: calls.append(args)  # noqa: E731
        self._pop(body(0), {"bq-job": URN}, on_routing_mismatch=hook)
        self._pop(body(0), {"bq-job": f"  {URN}  "}, on_routing_mismatch=hook)
        self._pop(body(0), None, on_routing_mismatch=hook)
        self._pop(body(0), {"bq-job": "  "}, on_routing_mismatch=hook)
        no_job = EnvelopeCodec.decode(body(0))
        del no_job["job"]
        self._pop(EnvelopeCodec.encode(no_job), {"bq-job": URN}, on_routing_mismatch=hook)
        self.assertEqual(calls, [])

    def test_raising_routing_mismatch_hook_does_not_break_pop_or_ack(self):
        def boom(*args):
            raise RuntimeError("hook failure")

        message = FakeMessage(
            self._body_with_urn("urn:babel:billing:invoiced"), properties={"bq-job": URN}
        )
        consumer = FakeConsumer([message])
        tr = PulsarTransport(client=FakeClient(consumer=consumer), on_routing_mismatch=boom)

        popped = tr.pop("orders")

        self.assertIsNotNone(popped)
        self.assertEqual(popped.route_urn, URN)
        tr.ack(popped)
        self.assertEqual(consumer.acked, [message])

    def test_hook_sees_reconciled_attempts(self):
        calls = []
        self._pop(
            self._body_with_urn("urn:babel:billing:invoiced"),
            {"bq-job": URN, "bq-attempts": "4"},
            on_routing_mismatch=lambda prop, body_urn, envelope: calls.append(envelope["attempts"]),
        )
        self.assertEqual(calls, [4])

    def test_hook_not_called_for_a_rejected_envelope(self):
        calls = []
        hook = lambda *args: calls.append(args)  # noqa: E731
        env = EnvelopeCodec.decode(self._body_with_urn("urn:babel:billing:invoiced"))
        env["meta"]["schema_version"] = 2
        self._pop_message(json.dumps(env), {"bq-job": URN}, on_routing_mismatch=hook)
        env = EnvelopeCodec.decode(self._body_with_urn("urn:babel:billing:invoiced"))
        env["trace_id"] = ""
        self._pop_message(json.dumps(env), {"bq-job": URN}, on_routing_mismatch=hook)
        self.assertEqual(calls, [])

    def test_pop_keeps_the_urn_alias_in_the_body(self):
        env = EnvelopeCodec.decode(body(0))
        env["urn"] = env.pop("job")
        raw = json.dumps(env)
        popped = self._pop_message(raw, {"bq-job": "urn:babel:billing:invoiced"})
        self.assertEqual(popped.body, raw)
        self.assertEqual(popped.route_urn, "urn:babel:billing:invoiced")

    def test_pop_falls_back_to_body_urn_when_bq_job_missing_or_blank(self):
        for props in (None, {}, {"bq-job": ""}, {"bq-job": "   "}):
            with self.subTest(props=props):
                popped = self._pop_message(body(0), props)
                self.assertEqual(popped.route_urn, "")
                self.assertEqual(EnvelopeCodec.urn(EnvelopeCodec.decode(popped.body)), URN)

    def test_pop_bq_job_is_trimmed(self):
        popped = self._pop_message(self._body_with_urn("urn:babel:billing:invoiced"), {"bq-job": f"  {URN}  "})
        self.assertEqual(popped.route_urn, URN)

    def test_pop_body_without_job_is_rejected_despite_bq_job(self):
        raw = body(0)
        env = EnvelopeCodec.decode(raw)
        del env["job"]
        popped = self._pop_message(EnvelopeCodec.encode(env), {"bq-job": URN})
        env = EnvelopeCodec.decode_lenient(popped.body)
        self.assertEqual(popped.route_urn, "")
        self.assertEqual(EnvelopeCodec.urn(env), "")
        self.assertFalse(EnvelopeCodec.accepts(env))

    def test_pop_bq_attempts_is_authoritative(self):
        cases = [
            ("property replaces body", 5, "3", 0, 3),
            ("property raises body", 1, "4", 0, 4),
            ("redelivery raises property", 0, "2", 6, 6),
            ("property above redelivery", 0, "7", 2, 7),
            ("leading zeros are digits", 0, "007", 0, 7),
            ("zero property lowers body", 5, "0", 0, 0),
            ("int32 max is valid", 0, "2147483647", 0, 2147483647),
        ]
        for name, body_attempts, prop, rc, want in cases:
            with self.subTest(name):
                env = self._pop(body(body_attempts), {"bq-attempts": prop}, rc)
                self.assertEqual(env["attempts"], want)

    def test_pop_invalid_bq_attempts_falls_back_to_body(self):
        bad_values = ["", " ", "abc", "-1", "+3", "-0", "3.5", "1e2", " 3", "3 ", "3\n", " 3x",
                      "\u0663", "2147483648", "99999999999999999999", None, 3]
        for bad in bad_values:
            with self.subTest(bad=bad):
                env = self._pop(body(5), {"bq-attempts": bad})
                self.assertEqual(env["attempts"], 5)

    def test_pop_invalid_bq_attempts_still_applies_redelivery_count(self):
        env = self._pop(body(5), {"bq-attempts": "nope"}, redelivery_count=9)
        self.assertEqual(env["attempts"], 9)

    def test_pop_missing_bq_attempts_falls_back_to_body(self):
        for props in (None, {}):
            with self.subTest(props=props):
                self.assertEqual(self._pop(body(5), props)["attempts"], 5)

    def test_missing_or_non_integer_body_attempts_stays_rejected(self):
        for name, value in (("missing", None), ("string", "3"), ("null", "null"), ("float", 1.5), ("bool", True)):
            with self.subTest(name):
                env = EnvelopeCodec.decode(body(0))
                if name == "missing":
                    del env["attempts"]
                elif name == "null":
                    env["attempts"] = None
                else:
                    env["attempts"] = value
                raw = json.dumps(env)
                popped = self._pop_message(raw, {"bq-attempts": "4"}, redelivery_count=2)
                self.assertEqual(popped.body, raw)  # attempts is never repaired into the body
                self.assertFalse(EnvelopeCodec.accepts(EnvelopeCodec.decode(popped.body)))

    def test_attempts_written_only_when_the_value_changes(self):
        raw = body(3)
        self.assertEqual(self._pop_message(raw, {"bq-attempts": "3"}).body, raw)
        self.assertEqual(self._pop_message(raw, None, redelivery_count=3).body, raw)
        self.assertEqual(self._pop(raw, {"bq-attempts": "4"})["attempts"], 4)

    # -- bq-schema-version gate (§5.7 step 3) --------------------------------

    def _pop_raw(self, raw, props):
        consumer = FakeConsumer([FakeMessage(raw, properties=props)])
        tr = PulsarTransport(client=FakeClient(consumer=consumer))
        return tr.pop("orders", timeout=0.1)

    def test_schema_version_missing_or_blank_decodes_normally(self):
        # blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        blanks = ("", "  ", "\u000b", "\f", " \t\n\u000b\f\r")
        for props in (None, {}) + tuple({"bq-schema-version": v} for v in blanks):
            with self.subTest(props=props):
                got = self._pop_raw(body(), props)
                self.assertEqual(got.route_urn, "")
                self.assertEqual(EnvelopeCodec.urn(EnvelopeCodec.decode(got.body)), URN)

    def test_schema_version_supported_is_delivered(self):
        got = self._pop_raw(body(), {"bq-schema-version": "1", "bq-attempts": "3"})
        self.assertEqual(got.route_urn, "")
        self.assertEqual(EnvelopeCodec.decode(got.body)["attempts"], 3)

    def test_unsupported_schema_version_gates_before_decode(self):
        # A body that is not JSON still yields the version-gate route: a decode error would
        # leave route_urn empty. The payload reaches the runtime untouched.
        garbage = "{not json"
        # NBSP, U+001C, U+0085, U+FEFF and U+3000 are not ASCII whitespace, hence not blank
        for value in ("2", "x", "01", " 1", "1.0", "-1", "\u00a0", "\u001c", "\u0085", "\ufeff", "\u3000"):
            with self.subTest(value=value):
                got = self._pop_raw(garbage, {"bq-schema-version": value, "bq-job": URN})
                self.assertEqual(got.route_urn, _gate_route(value, {"bq-job": URN}))
                self.assertTrue(got.route_urn.startswith(UNSUPPORTED_SCHEMA_ROUTE_PREFIX))
                self.assertEqual(got.body, garbage)

    def test_gate_logs_a_warning_naming_the_version_even_for_an_invalid_body(self):
        with self.assertLogs("babelqueue.pulsar", level="WARNING") as logs:
            self._pop_raw("{not json", {"bq-schema-version": "2"})
        self.assertIn("'2'", logs.output[0])
        self.assertIn("'orders'", logs.output[0])

    def test_supported_version_does_not_log(self):
        from unittest import mock

        from babelqueue import pulsar_transport

        with mock.patch.object(pulsar_transport._logger, "warning") as warn:
            self._pop_raw(body(), {"bq-schema-version": "1"})
            self._pop_raw(body(), {})
        warn.assert_not_called()

    def test_gated_message_does_not_call_the_mismatch_hook(self):
        calls = []
        consumer = FakeConsumer(
            [FakeMessage(body(), properties={"bq-schema-version": "2", "bq-job": "urn:babel:orders:refunded"})]
        )
        tr = PulsarTransport(
            client=FakeClient(consumer=consumer), on_routing_mismatch=lambda *a: calls.append(a)
        )
        tr.pop("orders", timeout=0.1)
        self.assertEqual(calls, [])

    def test_app_quarantines_unsupported_schema_version(self):
        from babelqueue import BabelQueue

        for value in ("2", "x"):
            with self.subTest(value=value):
                producer = FakeProducer()
                incoming = body()
                consumer = FakeConsumer(
                    [FakeMessage(incoming, properties={"bq-schema-version": value, "bq-job": URN})]
                )
                tr = PulsarTransport(client=FakeClient(producer=producer, consumer=consumer))
                app = BabelQueue(transport=tr, max_attempts=1, dead_letter=True)
                seen = []

                @app.handler(URN)
                def on_created(data, meta):
                    seen.append(data)

                app.consume(queue="orders", max_messages=1)

                self.assertEqual(seen, [])  # never reaches a handler
                self.assertEqual(len(consumer.acked), 1)
                self.assertEqual(len(producer.sent), 1)
                # rejected without decoding: the DLQ record is the incoming body, byte-identical
                self.assertEqual(producer.sent[0][0].decode("utf-8"), incoming)
                self.assertNotIn("dead_letter", producer.sent[0][0].decode("utf-8"))

    def test_republish_of_gated_message_never_writes_the_sentinel(self):
        """The dead-letter record keeps the original bq-job / bq-schema-version (the sentinel is
        an in-process marker) and the body, so a redrive meets the same gate. A gated message is
        never retried: without a DLQ it is dropped (acked) with nothing re-published."""
        from babelqueue import BabelQueue

        cases = (
            ("with bq-job", {"bq-schema-version": "2", "bq-job": "urn:babel:orders:legacy"}, "urn:babel:orders:legacy"),
            ("without bq-job", {"bq-schema-version": "x"}, URN),  # derived from the body, never the sentinel
            ("escaped", {"bq-schema-version": "2&x=?bq-job=%", "bq-job": "a?bq-job=b&c d"}, "a?bq-job=b&c d"),
        )
        for name, props, want_job in cases:
            for dead_letter in (False, True):
                with self.subTest(case=name, dead_letter=dead_letter):
                    producer = FakeProducer()
                    consumer = FakeConsumer([FakeMessage(body(), properties=dict(props))])
                    tr = PulsarTransport(client=FakeClient(producer=producer, consumer=consumer))
                    app = BabelQueue(
                        transport=tr, max_attempts=5, dead_letter=dead_letter
                    )
                    app.consume(queue="orders", max_messages=1)

                    if not dead_letter:
                        self.assertEqual(producer.sent, [])  # no retry copy
                        self.assertEqual(len(consumer.acked), 1)
                        continue
                    self.assertEqual(len(producer.sent), 1)
                    content, sent_props = producer.sent[0]
                    self.assertNotIn("unsupported-schema-version", sent_props.get("bq-job", ""))
                    self.assertEqual(sent_props.get("bq-job"), want_job)
                    self.assertEqual(sent_props["bq-schema-version"], props["bq-schema-version"])
                    self.assertEqual(EnvelopeCodec.decode(content.decode("utf-8"))["job"], URN)

                    # the re-published message is gated again on the next consume
                    again = self._pop_raw(content.decode("utf-8"), sent_props)
                    self.assertTrue(again.route_urn.startswith(UNSUPPORTED_SCHEMA_ROUTE_PREFIX))

    def test_invalid_body_with_unsupported_version_is_quarantined_without_a_handler(self):
        from babelqueue import BabelQueue

        producer = FakeProducer()
        consumer = FakeConsumer([FakeMessage("{not json", properties={"bq-schema-version": "2"})])
        tr = PulsarTransport(client=FakeClient(producer=producer, consumer=consumer))
        app = BabelQueue(transport=tr, max_attempts=1, dead_letter=True)
        seen = []

        @app.handler(URN)
        def on_created(data, meta):
            seen.append(data)

        with self.assertLogs("babelqueue.pulsar", level="WARNING"):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(seen, [])
        for _content, sent_props in producer.sent:
            self.assertNotIn("unsupported-schema-version", (sent_props or {}).get("bq-job", ""))

    def test_pop_empty_returns_none_on_timeout(self):
        tr = PulsarTransport(client=FakeClient(consumer=FakeConsumer([])))
        self.assertIsNone(tr.pop("orders"))

    def test_pop_passes_timeout_to_receive(self):
        consumer = FakeConsumer([FakeMessage(body(), 0)])
        captured = {}
        original = consumer.receive

        def spy(timeout_millis=None):
            captured["timeout_millis"] = timeout_millis
            return original(timeout_millis=timeout_millis)

        consumer.receive = spy
        tr = PulsarTransport(client=FakeClient(consumer=consumer))
        tr.pop("orders", timeout=2.0)
        self.assertEqual(captured["timeout_millis"], 2000)

    def test_ack_acknowledges_the_reserved_message(self):
        message = FakeMessage(body(), 0)
        consumer = FakeConsumer([message])
        tr = PulsarTransport(client=FakeClient(consumer=consumer))

        tr.ack(tr.pop("orders"))

        self.assertEqual(consumer.acked, [message])

    def test_ack_noop_without_handle(self):
        tr = PulsarTransport(client=FakeClient())
        tr.ack(ReceivedMessage(body="", queue="orders", handle=None))  # no error

    def test_explicit_consumer_type_is_forwarded_to_subscribe(self):
        client = FakeClient()
        tr = PulsarTransport(client=client, consumer_type="shared-sentinel")
        tr.pop("orders")
        self.assertEqual(client.subscribed[0][2].get("consumer_type"), "shared-sentinel")

    def test_topic_prefix_is_applied(self):
        client = FakeClient()
        tr = PulsarTransport(client=client, topic_prefix="persistent://public/default/")
        tr.publish("orders", body())
        # The producer is cached per queue; subscribe/produce use the prefixed topic.
        tr.pop("orders")
        self.assertEqual(client.subscribed[0][0], "persistent://public/default/orders")


class PulsarSchemeTest(unittest.TestCase):
    def test_make_transport_routes_pulsar_scheme(self):
        if HAVE_PULSAR:
            self.skipTest("pulsar-client installed — building a real client would need a broker")
        with self.assertRaises(ImportError):
            make_transport("pulsar://localhost:6650")


if __name__ == "__main__":
    unittest.main()
