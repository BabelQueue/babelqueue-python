"""Apache ActiveMQ Artemis (AMQP 1.0) transport tests.

The projection helpers, the ``attempts = max(body, delivery_count)`` reconciliation (no -1,
the AMQP counter is 0-based), and the pop/ack reservation flow run with no broker and no
``python-qpid-proton`` (the transport's proton import is lazy). The publish flow, which builds a
real proton ``Message``, is skipped unless ``python-qpid-proton`` is installed.
"""

from __future__ import annotations

import unittest
from unittest import mock

from babelqueue import EnvelopeCodec
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


class FakeMessage:
    """Duck-typed proton Message for the consume path (no proton needed)."""

    def __init__(self, raw: str, delivery_count: int = 0) -> None:
        self.body = raw
        self.delivery_count = delivery_count


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
        self.assertEqual("1", props["bq-schema-version"])
        self.assertEqual("python", props["bq-source-lang"])
        self.assertEqual("2", props["bq-attempts"])
        self.assertEqual("babelqueue", props["bq-app-id"])
        # The URN and trace_id ride JMS-native slots, not application properties.
        self.assertNotIn("bq-job", props)
        self.assertNotIn("bq-trace-id", props)

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
        self.assertEqual("1", message.properties["bq-schema-version"])
        self.assertEqual("babelqueue", message.properties["bq-app-id"])
        self.assertEqual(URN, dict(message.annotations)[symbol("x-opt-jms-type")])
        # The body is the byte-identical canonical envelope.
        self.assertEqual(URN, EnvelopeCodec.decode(message.body)["job"])


if __name__ == "__main__":
    unittest.main()
