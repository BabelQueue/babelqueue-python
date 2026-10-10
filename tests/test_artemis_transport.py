"""Apache ActiveMQ Artemis (AMQP 1.0) transport tests.

The projection helpers, the ``attempts = max(body, delivery_count)`` reconciliation (no -1,
the AMQP counter is 0-based), and the pop/ack reservation flow run with no broker and no
``python-qpid-proton`` (the transport's proton import is lazy). The publish flow, which builds a
real proton ``Message``, is skipped unless ``python-qpid-proton`` is installed.
"""

from __future__ import annotations

import unittest
from unittest import mock
from urllib.parse import quote

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.artemis_transport import ArtemisTransport
from babelqueue.transport import ReceivedMessage, make_transport

try:
    import proton  # noqa: F401

    HAVE_PROTON = True
except ImportError:
    HAVE_PROTON = False

URN = "urn:babel:orders:created"


def body(attempts: int = 0, trace_id: str = "trace-1") -> str:
    env = EnvelopeCodec.make(URN, {"order_id": 1042}, queue="orders", trace_id=trace_id)
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


class FakeMessage:
    """Duck-typed proton Message for the consume path (no proton needed)."""

    def __init__(self, raw: str, delivery_count: int = 0, properties=None) -> None:
        self.body = raw
        self.delivery_count = delivery_count
        self.properties = properties


class Timeout(Exception):
    """Stands in for proton.Timeout — matched by class name, like the real transport."""


class FakeSender:
    def __init__(self) -> None:
        self.sent: list = []

    def send(self, message) -> None:
        self.sent.append(message)


class FakeReceiver:
    def __init__(self, messages=()) -> None:
        self._messages = list(messages)
        self.accepted = 0

    def receive(self, timeout=None):
        if not self._messages:
            raise Timeout("no message")
        return self._messages.pop(0)

    def accept(self) -> None:
        self.accepted += 1


class FakeConnection:
    def __init__(self, sender=None, receiver=None) -> None:
        self._sender = sender if sender is not None else FakeSender()
        self._receiver = receiver if receiver is not None else FakeReceiver()
        self.closed = False

    def create_sender(self, address):
        return self._sender

    def create_receiver(self, address, credit=1):
        return self._receiver

    def close(self) -> None:
        self.closed = True


class ArtemisProjectionTest(unittest.TestCase):
    def test_application_properties_mirror_the_contract_fields(self) -> None:
        props = ArtemisTransport._projection(body(attempts=2))
        self.assertEqual("1", props["bq_schema_version"])
        self.assertEqual("python", props["bq_source_lang"])
        self.assertEqual("2", props["bq_attempts"])
        self.assertEqual("babelqueue", props["bq_app_id"])
        # The URN and trace_id ride JMS-native slots, not application properties.
        self.assertNotIn("bq_job", props)
        self.assertNotIn("bq_trace_id", props)

    def test_jms_type_and_correlation_id_come_from_the_body(self) -> None:
        self.assertEqual(URN, ArtemisTransport._jms_type(body()))
        self.assertEqual("trace-1", ArtemisTransport._correlation_id(body(trace_id="trace-1")))

    def test_creation_time_is_seconds_from_created_at_millis(self) -> None:
        raw = body()
        created_at = EnvelopeCodec.decode(raw)["meta"]["created_at"]
        self.assertAlmostEqual(created_at / 1000.0, ArtemisTransport._creation_seconds(raw))

    def test_projection_helpers_tolerate_garbage(self) -> None:
        self.assertEqual({}, ArtemisTransport._projection("not-json"))
        self.assertEqual("", ArtemisTransport._jms_type("not-json"))
        self.assertEqual("", ArtemisTransport._correlation_id("not-json"))
        self.assertIsNone(ArtemisTransport._creation_seconds("not-json"))

    def test_projection_helpers_do_not_log_a_forbidden_key_drop(self) -> None:
        # The body is sent unchanged, so a read-only projection must not claim a "dropped" key.
        raw = _with_forbidden_key(body(trace_id="trace-1"))

        def project() -> None:
            self.assertEqual("1", ArtemisTransport._projection(raw)["bq_schema_version"])
            self.assertEqual(URN, ArtemisTransport._jms_type(raw))
            self.assertEqual("trace-1", ArtemisTransport._correlation_id(raw))
            self.assertIsNotNone(ArtemisTransport._creation_seconds(raw))

        _assert_no_codec_warnings(self, project)


class ArtemisReconcileTest(unittest.TestCase):
    def test_first_delivery_keeps_body_count(self) -> None:
        self.assertEqual(0, EnvelopeCodec.decode(ArtemisTransport._reconcile(body(0), 0))["attempts"])

    def test_delivery_count_raises_attempts_no_minus_one(self) -> None:
        # AMQP delivery-count is 0-based: count 3 == 3 prior redeliveries == attempts 3.
        out = ArtemisTransport._reconcile(body(0), 3)
        self.assertEqual(3, EnvelopeCodec.decode(out)["attempts"])

    def test_body_count_never_lowered_by_delivery_count(self) -> None:
        out = ArtemisTransport._reconcile(body(5), 2)
        self.assertEqual(5, EnvelopeCodec.decode(out)["attempts"])

    def test_non_numeric_delivery_count_is_ignored(self) -> None:
        raw = body(1)
        self.assertEqual(raw, ArtemisTransport._reconcile(raw, None))


class ArtemisConsumeTest(unittest.TestCase):
    def test_pop_reconciles_and_acks_via_the_receiver(self) -> None:
        receiver = FakeReceiver([FakeMessage(body(0), delivery_count=2)])
        transport = ArtemisTransport(connection=FakeConnection(receiver=receiver))

        message = transport.pop("orders")
        self.assertIsNotNone(message)
        self.assertEqual(2, EnvelopeCodec.decode(message.body)["attempts"])

        transport.ack(message)
        self.assertEqual(1, receiver.accepted)

    def test_pop_returns_none_on_timeout(self) -> None:
        transport = ArtemisTransport(connection=FakeConnection(receiver=FakeReceiver([])))
        self.assertIsNone(transport.pop("orders"))

    def test_pop_propagates_non_timeout_errors(self) -> None:
        class Broken:
            def receive(self, timeout=None):
                raise ConnectionError("link detached")

        transport = ArtemisTransport(connection=FakeConnection(receiver=Broken()))
        with self.assertRaises(ConnectionError):
            transport.pop("orders")

    def test_pop_decodes_bytes_payload(self) -> None:
        receiver = FakeReceiver([FakeMessage(body(0).encode("utf-8"))])
        transport = ArtemisTransport(connection=FakeConnection(receiver=receiver))
        self.assertEqual(URN, EnvelopeCodec.decode(transport.pop("orders").body)["job"])

    def test_ack_without_handle_is_a_no_op(self) -> None:
        transport = ArtemisTransport(connection=FakeConnection())
        transport.ack(ReceivedMessage(body=body(), queue="orders", handle=None))  # no raise

    def test_close_closes_the_connection(self) -> None:
        connection = FakeConnection()
        transport = ArtemisTransport(connection=connection)
        transport.close()
        self.assertTrue(connection.closed)


class ArtemisSchemaVersionGateTest(unittest.TestCase):
    """The ``bq_schema_version`` pre-decode gate (broker-bindings.md §7.7 step 3; underscores, ADR-0017)."""

    PREFIX = "babelqueue:unsupported-schema-version:"

    def _transport(self, raw, properties, delivery_count=0):
        receiver = FakeReceiver([FakeMessage(raw, delivery_count, properties)])
        return ArtemisTransport(connection=FakeConnection(receiver=receiver)), receiver

    def _pop(self, raw, properties, delivery_count=0):
        return self._transport(raw, properties, delivery_count)[0].pop("orders")

    def test_missing_or_blank_property_decodes_normally(self) -> None:
        # blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        blanks = ("", "  ", "\u000b", "\f", " \t\n\u000b\f\r", b"", b" ")
        cases = [None, {}, {"other": "x"}, {"bq_schema_version": None}]
        cases += [{"bq_schema_version": v} for v in blanks]
        # the hyphenated spelling belongs to the other bindings and is not this property
        cases.append({"bq-schema-version": "2"})
        for properties in cases:
            with self.subTest(properties=properties):
                got = self._pop(body(), properties, delivery_count=3)
                self.assertEqual("", got.route_urn)
                self.assertEqual(3, EnvelopeCodec.decode(got.body)["attempts"])  # reconciled

    def test_supported_version_is_delivered(self) -> None:
        for value in ("1", b"1"):
            with self.subTest(value=value):
                got = self._pop(body(), {"bq_schema_version": value}, delivery_count=2)
                self.assertEqual("", got.route_urn)
                self.assertEqual(2, EnvelopeCodec.decode(got.body)["attempts"])

    def test_unsupported_version_returns_the_route_without_decoding(self) -> None:
        garbage = "{not json"
        values = ("2", "x", "01", " 1", "1.0", "-1", "\u00a0", "\u001c", "\u0085", "\ufeff", "\u3000")
        for value in values:
            with self.subTest(value=value):
                transport, receiver = self._transport(garbage, {"bq_schema_version": value})
                with mock.patch.object(
                    EnvelopeCodec, "decode", side_effect=AssertionError("decode must not run")
                ) as decode:
                    got = transport.pop("orders")
                decode.assert_not_called()
                self.assertEqual(self.PREFIX + quote(value, safe=""), got.route_urn)
                self.assertEqual(garbage, got.body)
                self.assertIs(receiver, got.handle)

    def test_non_string_values_follow_the_integer_rule(self) -> None:
        # int 1 is the supported version (Java/.NET parity); every other number, a bool (an int in
        # Python) and any other type is rejected
        for value, want in (
            (1, None),
            (2, "2"),
            (0, "0"),
            (1.5, "1.5"),
            (1.0, "1.0"),
            (True, "True"),
            (False, "False"),
            (b"2", "2"),
            ([1], "%5B1%5D"),
        ):
            with self.subTest(value=value):
                got = self._pop("{not json", {"bq_schema_version": value})
                self.assertEqual("" if want is None else self.PREFIX + want, got.route_urn)

    def test_long_rejected_version_is_truncated_in_the_route(self) -> None:
        got = self._pop("{not json", {"bq_schema_version": "9" * 100})
        self.assertEqual(self.PREFIX + "9" * 64 + "...", got.route_urn)

    def test_route_percent_escapes_the_rejected_version(self) -> None:
        got = self._pop("{not json", {"bq_schema_version": "2&x=?bq-job=%"})
        self.assertEqual(self.PREFIX + "2%26x%3D%3Fbq-job%3D%25", got.route_urn)

    def test_gate_does_not_decode_a_valid_body_either(self) -> None:
        raw = body(attempts=0)
        with mock.patch.object(
            EnvelopeCodec, "decode", side_effect=AssertionError("decode must not run")
        ):
            got = self._pop(raw, {"bq_schema_version": "2"}, delivery_count=5)
        self.assertEqual(raw, got.body)  # not reconciled, byte-identical
        self.assertEqual(self.PREFIX + "2", got.route_urn)

    def test_gate_logs_a_warning_naming_the_version_and_queue(self) -> None:
        with self.assertLogs("babelqueue.artemis", level="WARNING") as logs:
            self._pop("{not json", {"bq_schema_version": "2"})
        self.assertIn("'2'", logs.output[0])
        self.assertIn("'orders'", logs.output[0])

    def test_supported_version_does_not_log(self) -> None:
        from babelqueue import artemis_transport

        with mock.patch.object(artemis_transport._logger, "warning") as warn:
            self._pop(body(), {"bq_schema_version": "1"})
            self._pop(body(), None)
        warn.assert_not_called()

    def test_gated_message_is_accepted_through_its_receiver(self) -> None:
        transport, receiver = self._transport(body(), {"bq_schema_version": "2"})
        transport.ack(transport.pop("orders"))
        self.assertEqual(1, receiver.accepted)

    def test_app_dead_letters_unsupported_version_without_reaching_a_handler(self) -> None:
        # max_attempts > 1: a gated message is dead-lettered directly, never retried
        for value in ("2", "x", "2&x=%"):
            with self.subTest(value=value):
                transport, receiver = self._transport(body(), {"bq_schema_version": value})
                app = BabelQueue(transport=transport, max_attempts=5, dead_letter=True)
                seen: list = []

                @app.handler(URN)
                def _on(data, meta):
                    seen.append(data)

                # publish() builds a proton Message; capture it instead so no proton is needed
                with mock.patch.object(ArtemisTransport, "publish") as publish:
                    app.consume(queue="orders", max_messages=1)

                self.assertEqual([], seen)
                self.assertEqual(1, receiver.accepted)
                publish.assert_called_once()
                queue, dead_body = publish.call_args.args
                self.assertEqual("orders.dlq", queue)
                dead = EnvelopeCodec.decode(dead_body)
                self.assertEqual(URN, dead["job"])  # the producer's body, annotated
                self.assertEqual("unknown_urn", dead["dead_letter"]["reason"])
                self.assertIn(self.PREFIX + quote(value, safe=""), dead["dead_letter"]["error"])


class ArtemisUrlTest(unittest.TestCase):
    def test_scheme_is_translated_to_proton(self) -> None:
        self.assertEqual("amqp://h:5672", ArtemisTransport._to_amqp_url("artemis://h:5672"))
        self.assertEqual("amqps://h:5671", ArtemisTransport._to_amqp_url("artemis+ssl://h:5671"))
        self.assertEqual("amqp://h:5672", ArtemisTransport._to_amqp_url("amqp://h:5672"))

    def test_factory_resolves_the_artemis_scheme(self) -> None:
        # Patch the connection build so the scheme is exercised without opening a socket
        # (whether or not python-qpid-proton is installed).
        with mock.patch.object(ArtemisTransport, "_build_connection", return_value=FakeConnection()):
            for url in ("artemis://localhost:5672", "artemis+ssl://localhost:5671"):
                self.assertIsInstance(make_transport(url), ArtemisTransport)


@unittest.skipUnless(HAVE_PROTON, "python-qpid-proton not installed")
class ArtemisPublishTest(unittest.TestCase):
    def test_publish_projects_the_envelope_onto_a_proton_message(self) -> None:
        from proton import symbol

        sender = FakeSender()
        transport = ArtemisTransport(connection=FakeConnection(sender=sender))

        transport.publish("orders", body(attempts=0, trace_id="trace-xyz"))

        self.assertEqual(1, len(sender.sent))
        message = sender.sent[0]
        self.assertEqual("trace-xyz", message.correlation_id)
        self.assertEqual("1", message.properties["bq_schema_version"])
        self.assertEqual("babelqueue", message.properties["bq_app_id"])
        self.assertEqual(URN, dict(message.annotations)[symbol("x-opt-jms-type")])
        # The body is the byte-identical canonical envelope.
        self.assertEqual(URN, EnvelopeCodec.decode(message.body)["job"])


if __name__ == "__main__":
    unittest.main()
