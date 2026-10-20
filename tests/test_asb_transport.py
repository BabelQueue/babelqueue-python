"""Azure Service Bus transport tests.

The native projection and the ``attempts = DeliveryCount - 1`` reconciliation run with no
broker and no ``azure-servicebus`` (the transport's azure import is lazy). The publish flow,
which builds a real ``ServiceBusMessage``, is skipped unless ``azure-servicebus`` is installed.
"""

from __future__ import annotations

import unittest
from unittest import mock
from urllib.parse import quote

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.asb_transport import AsbTransport
from babelqueue.transport import ReceivedMessage, make_transport

try:
    import azure.servicebus  # noqa: F401

    HAVE_ASB = True
except ImportError:
    HAVE_ASB = False

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


class FakeReceived:
    """Duck-typed ServiceBusReceivedMessage (the real one has no public constructor)."""

    def __init__(self, raw: str, delivery_count: int = 1, application_properties=None) -> None:
        self._raw = raw
        self.delivery_count = delivery_count
        self.application_properties = application_properties

    def __str__(self) -> str:
        return self._raw


class FakeSender:
    def __init__(self) -> None:
        self.sent: list = []

    def send_messages(self, message) -> None:
        self.sent.append(message)


class FakeReceiver:
    def __init__(self, messages=()) -> None:
        self._messages = list(messages)
        self.completed: list = []

    def receive_messages(self, max_message_count: int = 1, max_wait_time=None):
        batch = self._messages[:max_message_count]
        del self._messages[:max_message_count]
        return batch

    def complete_message(self, message) -> None:
        self.completed.append(message)


class FakeClient:
    def __init__(self, sender=None, receiver=None) -> None:
        self.sender = sender if sender is not None else FakeSender()
        self.receiver = receiver if receiver is not None else FakeReceiver()

    def get_queue_sender(self, queue: str):
        return self.sender

    def get_queue_receiver(self, queue: str):
        return self.receiver


class AsbProjectionTest(unittest.TestCase):
    def test_projection_maps_native_fields_and_properties(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        proj = AsbTransport._projection(raw)

        self.assertEqual(proj["subject"], URN)
        self.assertEqual(proj["correlation_id"], env["trace_id"])
        self.assertEqual(proj["message_id"], env["meta"]["id"])
        self.assertEqual(proj["content_type"], "application/json")
        props = proj["application_properties"]
        self.assertEqual(props["bq-schema-version"], env["meta"]["schema_version"])
        self.assertEqual(props["bq-source-lang"], env["meta"]["lang"])
        self.assertEqual(props["bq-created-at"], env["meta"]["created_at"])

    def test_reconcile_attempts_is_delivery_count_minus_one(self):
        out = AsbTransport._reconcile(body(0), 3)
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 2)

    def test_reconcile_first_delivery_is_zero(self):
        out = AsbTransport._reconcile(body(0), 1)
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 0)

    def test_reconcile_ignores_garbage_count(self):
        raw = body(5)
        self.assertEqual(AsbTransport._reconcile(raw, "not-a-number"), raw)

    def test_reconcile_never_lowers_runtime_count(self):
        # The runtime retries by republishing with attempts+1 (DeliveryCount back to 1),
        # so a higher body count must win over a lower native one.
        raw = body(5)
        self.assertEqual(int(EnvelopeCodec.decode(AsbTransport._reconcile(raw, 2))["attempts"]), 5)

    def test_projection_does_not_log_a_forbidden_key_drop(self):
        # The body is sent unchanged, so the native-field projection must not claim a drop.
        raw = _with_forbidden_key(body())
        out: dict = {}
        _assert_no_codec_warnings(self, lambda: out.update(AsbTransport._projection(raw)))
        self.assertEqual(out["subject"], URN)


class AsbConsumeTest(unittest.TestCase):
    def test_pop_reconciles_attempts_and_returns_handle(self):
        received = FakeReceived(body(0), delivery_count=3)
        tr = AsbTransport(client=FakeClient(receiver=FakeReceiver([received])))

        msg = tr.pop("orders")

        self.assertIsNotNone(msg)
        self.assertEqual(msg.queue, "orders")
        self.assertIs(msg.handle, received)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 2)

    def test_pop_empty_returns_none(self):
        tr = AsbTransport(client=FakeClient(receiver=FakeReceiver([])))
        self.assertIsNone(tr.pop("orders"))

    def test_ack_completes_the_reserved_message(self):
        received = FakeReceived(body(), delivery_count=1)
        receiver = FakeReceiver([received])
        tr = AsbTransport(client=FakeClient(receiver=receiver))

        tr.ack(tr.pop("orders"))

        self.assertEqual(receiver.completed, [received])

    def test_ack_noop_without_handle(self):
        tr = AsbTransport(client=FakeClient())
        tr.ack(ReceivedMessage(body="", queue="orders", handle=None))  # no error

    def test_make_transport_routes_sb_scheme(self):
        if HAVE_ASB:
            self.skipTest("azure installed — building a real client would need credentials")
        with self.assertRaises(ImportError):
            make_transport("sb://ns.servicebus.windows.net")


class AsbSchemaVersionGateTest(unittest.TestCase):
    """The ``bq-schema-version`` pre-decode gate (broker-bindings.md §4.7 step 3)."""

    PREFIX = "babelqueue:unsupported-schema-version:"

    def _pop(self, raw, properties, delivery_count=1):
        receiver = FakeReceiver([FakeReceived(raw, delivery_count, properties)])
        return AsbTransport(client=FakeClient(receiver=receiver)).pop("orders")

    def test_missing_or_blank_version_decodes_normally(self):
        # blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        blanks = ("", "  ", "\u000b", "\f", " \t\n\u000b\f\r")
        for properties in (None, {}, {"other": "x"}) + tuple({"bq-schema-version": v} for v in blanks):
            with self.subTest(properties=properties):
                got = self._pop(body(), properties, delivery_count=3)
                self.assertEqual(got.route_urn, "")
                self.assertEqual(EnvelopeCodec.decode(got.body)["attempts"], 2)  # reconciled

    def test_supported_version_is_delivered(self):
        # the transport itself publishes the integer 1; other SDKs and the portal send "1"
        for value in ("1", 1, b"1"):
            with self.subTest(value=value):
                got = self._pop(body(), {"bq-schema-version": value}, delivery_count=2)
                self.assertEqual(got.route_urn, "")
                self.assertEqual(EnvelopeCodec.decode(got.body)["attempts"], 1)

    def test_bytes_keys_and_values_are_decoded(self):
        got = self._pop(body(), {b"bq-schema-version": b"1"})
        self.assertEqual(got.route_urn, "")
        got = self._pop("{not json", {b"bq-schema-version": b"2"})
        self.assertEqual(got.route_urn, self.PREFIX + "2")

    def test_long_rejected_version_is_truncated_in_the_route(self):
        got = self._pop("{not json", {"bq-schema-version": "9" * 100})
        self.assertEqual(got.route_urn, self.PREFIX + "9" * 64 + "...")

    def test_route_percent_escapes_the_rejected_version(self):
        got = self._pop("{not json", {"bq-schema-version": "2&x=?bq-job=%"})
        self.assertEqual(got.route_urn, self.PREFIX + "2%26x%3D%3Fbq-job%3D%25")

    def test_unsupported_version_returns_the_route_without_decoding(self):
        garbage = "{not json"
        values = ("2", "x", "01", " 1", "1.0", "-1", "\u00a0", "\u001c", "\u0085", "\ufeff", "\u3000")
        for value in values:
            with self.subTest(value=value):
                with mock.patch.object(
                    EnvelopeCodec, "decode", side_effect=AssertionError("decode must not run")
                ) as decode:
                    got = self._pop(garbage, {"bq-schema-version": value})
                decode.assert_not_called()
                self.assertEqual(got.route_urn, self.PREFIX + quote(value, safe=""))
                self.assertEqual(got.body, garbage)
                self.assertIsNotNone(got.handle)

    def test_numeric_and_other_typed_values_follow_the_integer_rule(self):
        # int 1 is the supported version; every other number, a bool (an int in Python) and any
        # other type is rejected
        for value, want_route in (
            (1, None),
            (2, self.PREFIX + "2"),
            (0, self.PREFIX + "0"),
            (-1, self.PREFIX + "-1"),
            (1.5, self.PREFIX + "1.5"),
            (1.0, self.PREFIX + "1.0"),
            (True, self.PREFIX + "True"),
            (False, self.PREFIX + "False"),
            ([1], self.PREFIX + "%5B1%5D"),
        ):
            with self.subTest(value=value):
                got = self._pop(body(), {"bq-schema-version": value})
                self.assertEqual(got.route_urn, want_route or "")

    def test_gate_does_not_decode_a_valid_body_either(self):
        raw = body()
        with mock.patch.object(
            EnvelopeCodec, "decode", side_effect=AssertionError("decode must not run")
        ):
            got = self._pop(raw, {"bq-schema-version": "2"}, delivery_count=5)
        self.assertEqual(got.body, raw)  # not reconciled, byte-identical
        self.assertEqual(got.route_urn, self.PREFIX + "2")

    def test_gate_logs_a_warning_naming_the_version_and_queue(self):
        with self.assertLogs("babelqueue.asb", level="WARNING") as logs:
            self._pop("{not json", {"bq-schema-version": "2"})
        self.assertIn("'2'", logs.output[0])
        self.assertIn("'orders'", logs.output[0])

    def test_supported_version_does_not_log(self):
        from babelqueue import asb_transport

        with mock.patch.object(asb_transport._logger, "warning") as warn:
            self._pop(body(), {"bq-schema-version": 1})
            self._pop(body(), None)
        warn.assert_not_called()

    def test_gated_message_is_completed_through_its_handle(self):
        receiver = FakeReceiver([FakeReceived(body(), 1, {"bq-schema-version": "2"})])
        tr = AsbTransport(client=FakeClient(receiver=receiver))
        got = tr.pop("orders")
        tr.ack(got)
        self.assertEqual(receiver.completed, [got.handle])

    def test_app_dead_letters_unsupported_version_without_reaching_a_handler(self):
        # max_attempts > 1: a gated message is dead-lettered directly, never retried
        for value in ("2", "x", 2, "2&x=%"):
            with self.subTest(value=value):
                incoming = body()
                receiver = FakeReceiver([FakeReceived(incoming, 1, {"bq-schema-version": value})])
                app = BabelQueue(
                    transport=AsbTransport(client=FakeClient(receiver=receiver)),
                    max_attempts=5,
                    dead_letter=True,
                )
                seen: list = []

                @app.handler(URN)
                def _on(data, meta):
                    seen.append(data)

                # publish() builds an azure ServiceBusMessage; capture it so no SDK is needed
                with mock.patch.object(AsbTransport, "publish") as publish:
                    app.consume(queue="orders", max_messages=1)

                self.assertEqual(seen, [])
                self.assertEqual(len(receiver.completed), 1)
                publish.assert_called_once()
                queue, dead_body = publish.call_args.args
                self.assertEqual(queue, "orders.dlq")
                # rejected without decoding: the DLQ record is the incoming body, byte-identical
                self.assertEqual(dead_body, incoming)
                self.assertNotIn("dead_letter", dead_body)


@unittest.skipUnless(HAVE_ASB, "azure-servicebus not installed")
class AsbPublishTest(unittest.TestCase):
    def test_publish_builds_message_with_native_projection(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        sender = FakeSender()
        tr = AsbTransport(client=FakeClient(sender=sender))

        tr.publish("orders", raw)

        self.assertEqual(len(sender.sent), 1)
        sent = sender.sent[0]
        self.assertEqual(sent.subject, URN)
        self.assertEqual(sent.correlation_id, env["trace_id"])
        self.assertEqual(sent.message_id, env["meta"]["id"])
        self.assertEqual(sent.content_type, "application/json")
        self.assertEqual(EnvelopeCodec.urn(EnvelopeCodec.decode(str(sent))), URN)

    def test_publish_encodes_ms_created_at_as_amqp_long(self):
        from azure.servicebus._pyamqp import _encode

        sender = FakeSender()
        AsbTransport(client=FakeClient(sender=sender)).publish("orders", body())

        props = sender.sent[0].application_properties
        out = bytearray()
        # a bare int > int32 raises "Value supplied for int invalid" here
        _encode.encode_value(out, _encode.encode_application_properties(props))
        self.assertIn(b"bq-created-at\x81", bytes(out))
        self.assertIn(b"bq-schema-version\x54\x01", bytes(out))


if __name__ == "__main__":
    unittest.main()
