"""Apache Pulsar transport tests.

The property projection and the ``attempts = max(body, redelivery_count)`` reconciliation —
and the full publish / pop / ack flow — run with no broker and no ``pulsar-client`` (the
transport's pulsar import is lazy and the publish path sends raw bytes, so a duck-typed fake
client covers everything). Only ``_build_client`` needs the real package.
"""

from __future__ import annotations

import unittest

from babelqueue import EnvelopeCodec
from babelqueue.pulsar_transport import PulsarTransport
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


class Timeout(Exception):
    """Duck-typed stand-in for ``pulsar.Timeout`` (matched by class name)."""


class FakeMessage:
    """Duck-typed pulsar Message: data()/redelivery_count() are callables."""

    def __init__(self, raw: str, redelivery_count: int = 0) -> None:
        self._raw = raw
        self._rc = redelivery_count

    def data(self) -> bytes:
        return self._raw.encode("utf-8")

    def redelivery_count(self) -> int:
        return self._rc


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
        out = PulsarTransport._reconcile(body(0), 2)
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 2)

    def test_reconcile_first_delivery_is_zero(self):
        out = PulsarTransport._reconcile(body(0), 0)
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 0)

    def test_reconcile_ignores_garbage_count(self):
        raw = body(5)
        self.assertEqual(PulsarTransport._reconcile(raw, "not-a-number"), raw)

    def test_reconcile_never_lowers_runtime_count(self):
        # The runtime retries by republishing with attempts+1 (redelivery count back to 0),
        # so a higher body count must win over a lower native one.
        raw = body(5)
        self.assertEqual(int(EnvelopeCodec.decode(PulsarTransport._reconcile(raw, 1))["attempts"]), 5)


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


class PulsarConsumeTest(unittest.TestCase):
    def test_pop_reconciles_attempts_and_returns_handle(self):
        message = FakeMessage(body(0), redelivery_count=2)
        tr = PulsarTransport(client=FakeClient(consumer=FakeConsumer([message])))

        msg = tr.pop("orders")

        self.assertIsNotNone(msg)
        self.assertEqual(msg.queue, "orders")
        self.assertIs(msg.handle, message)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 2)

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
