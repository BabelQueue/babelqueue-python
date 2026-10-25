"""Poison routing outside Redis and Kafka (ADR-0036 decisions 2, 4 and 6).

A body that does not decode into an envelope, or names no identity, is **poison**: on the memory
transport, SQS, AMQP, Pulsar, Azure Service Bus and Artemis it is never decoded leniently into an
empty envelope, never handed to a handler and never routed as an unknown URN. It takes the poison
route of Go's ``routePoison``: dead-lettered as the ADR-0035 record (``reason: "poison"``, original
body preserved) when the DLQ is on, dropped by ``on_unknown_urn: delete``, and bounded by
``max_attempts`` when the DLQ is off. No broker and no client library are needed: the clients are
injected fakes.
"""

from __future__ import annotations

import base64
import json
import unittest
from typing import Any, List, Tuple

from babelqueue import BabelQueue, EnvelopeCodec, UnknownUrnStrategy
from babelqueue.asb_transport import AsbTransport
from babelqueue.artemis_transport import ArtemisTransport
from babelqueue.pika_transport import PikaTransport
from babelqueue.pulsar_transport import PulsarTransport
from babelqueue.sqs_transport import SqsTransport
from babelqueue.transport import InMemoryTransport, ReceivedMessage

from test_artemis_transport import FakeConnection as ArtemisConnection
from test_artemis_transport import FakeMessage as ArtemisMessage
from test_artemis_transport import FakeReceiver as ArtemisReceiver
from test_asb_transport import FakeClient as AsbClient
from test_asb_transport import FakeReceived as AsbReceived
from test_asb_transport import FakeReceiver as AsbReceiver
from test_pulsar_transport import FakeClient as PulsarClient
from test_pulsar_transport import FakeConsumer as PulsarConsumer
from test_pulsar_transport import FakeMessage as PulsarMessage
from test_pulsar_transport import FakeProducer as PulsarProducer
from test_sqs_transport import FakeSQS

URN = "urn:babel:orders:created"

#: Bodies that are poison outside Kafka (conformance ``identity_verdict``): undecodable, not an
#: object, a wrongly typed member, and an object with no identity.
POISON_BODIES = [
    "not json",
    "{not json",
    "",
    "[]",
    "null",
    "5",
    "{}",
    '{"foo":1}',
    '{"job":5}',
    '{"job":"  ","attempts":0}',
    '{"job":"%s","attempts":"3"}' % URN,
]


def valid_body(attempts: int = 0, urn: str = URN) -> str:
    env = EnvelopeCodec.make(urn, {"order_id": 1}, queue="orders")
    env["attempts"] = attempts
    return EnvelopeCodec.encode(env)


def assert_poison_record(case: unittest.TestCase, record: str, body: str, queue: str = "orders") -> None:
    """``record`` is the ADR-0035 DLQ record of the poison ``body`` (original bytes preserved)."""
    parsed = json.loads(record)
    block = parsed["dead_letter"]
    case.assertEqual("poison", block["reason"])
    case.assertEqual(queue, block["original_queue"])
    if "raw" in parsed:
        if parsed.get("raw_encoding") == "base64":
            case.assertEqual(body.encode("utf-8", "surrogateescape"), base64.b64decode(parsed["raw"]))
        else:
            case.assertEqual(body, parsed["raw"])
    else:  # spliced strict JSON object: every original member is kept
        original = json.loads(body)
        case.assertEqual(original, {k: v for k, v in parsed.items() if k != "dead_letter"})


class MemoryPoisonTest(unittest.TestCase):
    def app(self, **kwargs: Any) -> Tuple[BabelQueue, List[Any]]:
        calls: List[Any] = []
        app = BabelQueue("memory://", queue="orders", max_attempts=3, **kwargs)
        app.register(URN, lambda data, meta: calls.append(data))
        return app, calls

    def feed(self, app: BabelQueue, body: str, limit: int = 10) -> int:
        """Publish ``body`` and consume until the queue is empty; returns the deliveries."""
        app.transport.publish("orders", body)
        deliveries = 0
        while app.transport.size("orders") and deliveries < limit:
            app.consume("orders", max_messages=1, timeout=0.01, handle_signals=False)
            deliveries += 1
        return deliveries

    def dlq(self, app: BabelQueue) -> List[str]:
        return list(app.transport._queues["orders.dlq"])  # noqa: SLF001

    def test_with_the_dlq_on_poison_is_dead_lettered_at_once_whatever_the_strategy(self) -> None:
        for strategy in (UnknownUrnStrategy.FAIL, UnknownUrnStrategy.RELEASE, UnknownUrnStrategy.DEAD_LETTER):
            for body in POISON_BODIES:
                with self.subTest(strategy=strategy, body=body):
                    app, calls = self.app(dead_letter=True, on_unknown_urn=strategy)
                    self.assertEqual(1, self.feed(app, body))  # no retry
                    self.assertEqual([], calls)
                    (record,) = self.dlq(app)
                    assert_poison_record(self, record, body)

    def test_the_dead_letter_strategy_writes_the_record_even_with_dead_lettering_off(self) -> None:
        app, calls = self.app(on_unknown_urn=UnknownUrnStrategy.DEAD_LETTER)
        self.assertEqual(1, self.feed(app, "not json"))
        (record,) = self.dlq(app)
        assert_poison_record(self, record, "not json")
        self.assertEqual([], calls)

    def test_delete_drops_poison_with_a_log_and_writes_nothing(self) -> None:
        for dead_letter in (False, True):
            with self.subTest(dead_letter=dead_letter):
                app, calls = self.app(dead_letter=dead_letter, on_unknown_urn=UnknownUrnStrategy.DELETE)
                with self.assertLogs("babelqueue.app", level="WARNING"):
                    self.assertEqual(1, self.feed(app, "not json"))
                self.assertEqual(0, app.transport.size("orders.dlq"))
                self.assertEqual([], calls)

    def test_with_the_dlq_off_poison_is_bounded_by_max_attempts_then_dropped(self) -> None:
        for strategy in (UnknownUrnStrategy.FAIL, UnknownUrnStrategy.RELEASE):
            for body in ("not json", '{"foo":1}'):
                with self.subTest(strategy=strategy, body=body):
                    app, calls = self.app(on_unknown_urn=strategy)
                    with self.assertLogs("babelqueue.app", level="WARNING") as logs:
                        self.assertEqual(3, self.feed(app, body))
                    self.assertEqual(0, app.transport.size("orders"))
                    self.assertEqual(0, app.transport.size("orders.dlq"))
                    self.assertEqual([], calls)
                    self.assertTrue(any("Dropping a message" in m for m in logs.output))

    def test_a_valid_envelope_with_no_handler_is_still_an_unknown_urn_not_poison(self) -> None:
        app, _ = self.app(dead_letter=True)
        self.assertEqual(3, self.feed(app, valid_body(urn="urn:babel:other")))
        (record,) = self.dlq(app)
        self.assertEqual("unknown_urn", json.loads(record)["dead_letter"]["reason"])

    def test_a_valid_envelope_with_a_handler_is_not_poison(self) -> None:
        app, calls = self.app(dead_letter=True)
        self.assertEqual(1, self.feed(app, valid_body()))
        self.assertEqual([{"order_id": 1}], calls)
        self.assertEqual(0, app.transport.size("orders.dlq"))

    def test_a_urn_alias_and_a_blank_job_with_a_urn_are_identities(self) -> None:
        for body in ('{"urn":"%s"}' % URN, '{"job":" ","urn":"%s"}' % URN, '{"job":5,"urn":"%s"}' % URN):
            with self.subTest(body=body):
                app, calls = self.app(dead_letter=True)
                self.assertEqual(1, self.feed(app, body))
                self.assertEqual(1, len(calls))
                self.assertEqual(0, app.transport.size("orders.dlq"))

    def test_an_out_of_band_route_exempts_a_delivery_from_the_identity_half_of_the_rule(self) -> None:
        # ADR-0036 decision 4, scope: a delivery routed on a transport property (Pulsar bq-job) is
        # outside the identity rule; an undecodable body is still poison.
        app, calls = self.app(dead_letter=True)
        app.dispatch(ReceivedMessage(body='{"attempts":0}', queue="orders", route_urn=URN))
        self.assertEqual(1, len(calls))  # routed by the property, not poison
        app.dispatch(ReceivedMessage(body="not json", queue="orders", route_urn=URN))
        self.assertEqual(1, len(calls))
        (record,) = self.dlq(app)
        assert_poison_record(self, record, "not json")

    def test_a_failed_dead_letter_write_leaves_the_poison_message_unacknowledged(self) -> None:
        class FailingDlq(InMemoryTransport):
            def __init__(self) -> None:
                super().__init__()
                self.acked = 0

            def publish(self, queue: str, body: str) -> None:
                if queue.endswith(".dlq"):
                    raise RuntimeError("dlq down")
                super().publish(queue, body)

            def ack(self, message: ReceivedMessage) -> None:
                self.acked += 1

        transport = FailingDlq()
        app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
        app.register(URN, lambda data, meta: None)
        transport.publish("orders", "not json")
        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume("orders", max_messages=1, timeout=0.01, handle_signals=False)
        self.assertEqual(0, transport.acked)  # never acknowledged, never retried
        self.assertEqual(0, transport.size("orders"))  # nothing re-published as a retry

    def test_the_core_decoder_is_not_changed_by_the_poison_rule(self) -> None:
        self.assertEqual({}, EnvelopeCodec.decode("not json"))
        self.assertEqual({}, EnvelopeCodec.decode_lenient("[1]"))
        self.assertIsNone(EnvelopeCodec.poison_cause(valid_body()))
        self.assertIsNotNone(EnvelopeCodec.poison_cause('{"foo":1}'))
        self.assertIsNone(EnvelopeCodec.poison_cause('{"foo":1}', require_identity=False))
        self.assertIsNotNone(EnvelopeCodec.poison_cause("not json", require_identity=False))


class SqsPoisonTest(unittest.TestCase):
    URL = "http://fake/orders"

    def setUp(self) -> None:
        self.fake = FakeSQS()
        self.transport = SqsTransport("sqs://", client=self.fake, queue_url_prefix="http://fake")

    def test_a_poison_message_is_dead_lettered_with_the_original_body_and_deleted(self) -> None:
        for body in ("not json", '{"foo":1}'):
            with self.subTest(body=body):
                self.fake.seed(self.URL, body, 1)
                app = BabelQueue(transport=self.transport, queue="orders", max_attempts=3, dead_letter=True)
                app.register(URN, lambda data, meta: None)
                app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
                record = self.fake.sent[-1]
                self.assertEqual("http://fake/orders.dlq", record["QueueUrl"])
                assert_poison_record(self, record["MessageBody"], body)
                self.assertEqual(1, len(self.fake.deleted))
                self.assertEqual([], self.fake.visibility_changes)  # no retry
                self.fake.sent.clear()
                self.fake.deleted.clear()

    def test_the_native_receive_count_bounds_the_retry_when_the_dlq_is_off(self) -> None:
        # SQS keeps the body and counts deliveries itself: the runtime reads the count off the
        # delivery (an undecodable body cannot carry ``attempts``) and stops at max_attempts.
        self.fake.seed(self.URL, "not json", 1)
        app = BabelQueue(transport=self.transport, queue="orders", max_attempts=3)
        app.register(URN, lambda data, meta: None)
        deliveries = 0
        with self.assertLogs("babelqueue.app", level="WARNING"):
            while self.fake.visible.get(self.URL) and deliveries < 10:
                app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
                deliveries += 1
        self.assertEqual(3, deliveries)
        self.assertEqual(2, len(self.fake.visibility_changes))  # retried twice, dropped the third time
        self.assertEqual(1, len(self.fake.deleted))
        self.assertEqual([], self.fake.sent)

    def test_the_delivery_carries_the_native_attempts(self) -> None:
        self.fake.seed(self.URL, "not json", 4)
        message = self.transport.pop("orders", timeout=0)
        assert message is not None
        self.assertEqual(3, message.native_attempts)
        self.assertEqual("not json", message.body)


class PulsarPoisonTest(unittest.TestCase):
    def make(self, message: Any, **kwargs: Any) -> Tuple[BabelQueue, PulsarClient, PulsarConsumer]:
        consumer = PulsarConsumer([message])
        client = PulsarClient(producer=PulsarProducer(), consumer=consumer)
        app = BabelQueue(
            transport=PulsarTransport("pulsar://localhost:6650", client=client),
            queue="orders",
            max_attempts=3,
            **kwargs,
        )
        app.register(URN, lambda data, meta: None)
        return app, client, consumer

    def test_a_poison_body_is_dead_lettered_with_reason_poison(self) -> None:
        app, client, consumer = self.make(PulsarMessage("not json"), dead_letter=True)
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        ((content, _),) = client.producer.sent
        assert_poison_record(self, content.decode("utf-8"), "not json")
        self.assertEqual(1, len(consumer.acked))

    def test_bytes_that_are_not_utf_8_survive_losslessly_as_base64(self) -> None:
        class RawBytes(PulsarMessage):
            def data(self) -> bytes:
                return b"\xff\xfe{garbage"

        app, client, consumer = self.make(RawBytes(""), dead_letter=True)
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)  # no UnicodeDecodeError
        ((content, _),) = client.producer.sent
        parsed = json.loads(content.decode("utf-8"))
        self.assertEqual("base64", parsed["raw_encoding"])
        self.assertEqual(b"\xff\xfe{garbage", base64.b64decode(parsed["raw"]))
        self.assertEqual("poison", parsed["dead_letter"]["reason"])
        self.assertEqual(1, len(consumer.acked))

    def test_the_reconciled_attempts_count_a_poison_body(self) -> None:
        # bq-attempts "2" + max_attempts 3, DLQ off: counted 3 -> dropped, not retried.
        app, client, consumer = self.make(PulsarMessage("not json", 0, {"bq-attempts": "2"}))
        with self.assertLogs("babelqueue.app", level="WARNING"):
            app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        self.assertEqual([], client.producer.sent)
        self.assertEqual(1, len(consumer.acked))

    def test_a_failed_dead_letter_write_is_negatively_acknowledged_for_redelivery(self) -> None:
        # ADR-0036 decision 1: the original is acknowledged only after the DLQ write succeeded; a
        # failed write nacks it (Go ``Rewind`` = ``Nack``) so the broker redelivers it.
        class FailingProducer(PulsarProducer):
            def send(self, content: Any, properties: Any = None) -> None:
                raise RuntimeError("dlq down")

        message = PulsarMessage("not json")
        consumer = PulsarConsumer([message])
        client = PulsarClient(producer=FailingProducer(), consumer=consumer)
        app = BabelQueue(
            transport=PulsarTransport("pulsar://localhost:6650", client=client),
            queue="orders", max_attempts=3, dead_letter=True,
        )
        app._rewind_pause = 0.0  # noqa: SLF001
        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        self.assertEqual([message], consumer.nacked)
        self.assertEqual([], consumer.acked)

    def test_the_transport_is_a_rewinder(self) -> None:
        from babelqueue.transport import Rewinder

        self.assertIsInstance(PulsarTransport("pulsar://x", client=PulsarClient()), Rewinder)

    def test_a_retry_re_publishes_a_bare_body_that_carries_the_count(self) -> None:
        app, client, consumer = self.make(PulsarMessage("not json", 1))
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        ((content, properties),) = client.producer.sent
        self.assertEqual({"attempts": 2}, json.loads(content.decode("utf-8")))
        self.assertEqual("2", properties["bq-attempts"])
        self.assertEqual(1, len(consumer.acked))


#: A *valid* envelope whose JSON string ``data.s`` holds the invalid UTF-8 byte ``0xff``: the bytes
#: are not UTF-8, yet ``json.loads`` accepts the lone surrogate they are read as.
INVALID_UTF8_IN_STRING = (
    b'{"job":"' + URN.encode() + b'","trace_id":"t","data":{"s":"\xff"},"meta":{},"attempts":0}'
)


def assert_base64_poison(case: unittest.TestCase, record: str, raw: bytes) -> None:
    parsed = json.loads(record)
    case.assertEqual("poison", parsed["dead_letter"]["reason"])
    case.assertEqual("base64", parsed["raw_encoding"])
    case.assertEqual(raw, base64.b64decode(parsed["raw"]))


class InvalidUtf8InsideAJsonStringTest(unittest.TestCase):
    """error-handling.md section 7: invalid UTF-8 is poison even when it sits inside a JSON string,
    where the lossless read leaves a lone surrogate ``json.loads`` accepts. It never reaches a
    handler and its original bytes are dead-lettered as the ADR-0035 base64 record."""

    def test_the_core_verdict(self) -> None:
        text = INVALID_UTF8_IN_STRING.decode("utf-8", "surrogateescape")
        self.assertIsNotNone(EnvelopeCodec.poison_cause(text))
        self.assertIsNotNone(EnvelopeCodec.poison_cause(text, require_identity=False))
        # A ``\udcff`` *escape* is plain ASCII in the text and stays a valid envelope.
        escaped = '{"job":"%s","data":{"s":"\\udcff"},"attempts":0}' % URN
        self.assertIsNone(EnvelopeCodec.poison_cause(escaped))

    def run_app(self, transport: Any) -> List[str]:
        calls: List[str] = []
        app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
        app.register(URN, lambda data, meta: calls.append("called"))
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        return calls

    def test_amqp(self) -> None:
        transport, channel = AmqpPoisonTest().transport(INVALID_UTF8_IN_STRING)
        capture = _PublishCapture(transport)
        self.assertEqual([], self.run_app(transport))
        ((queue, record),) = capture.written
        self.assertEqual("orders.dlq", queue)
        assert_base64_poison(self, record, INVALID_UTF8_IN_STRING)
        self.assertEqual([7], channel.acked)

    def test_pulsar(self) -> None:
        class RawBytes(PulsarMessage):
            def data(self) -> bytes:
                return INVALID_UTF8_IN_STRING

        consumer = PulsarConsumer([RawBytes("")])
        client = PulsarClient(producer=PulsarProducer(), consumer=consumer)
        self.assertEqual([], self.run_app(PulsarTransport("pulsar://x", client=client)))
        ((content, _),) = client.producer.sent
        assert_base64_poison(self, content.decode("utf-8"), INVALID_UTF8_IN_STRING)
        self.assertEqual(1, len(consumer.acked))

    def test_artemis(self) -> None:
        receiver = ArtemisReceiver([ArtemisMessage(INVALID_UTF8_IN_STRING)])
        transport = ArtemisTransport(connection=ArtemisConnection(receiver=receiver))
        capture = _PublishCapture(transport)
        self.assertEqual([], self.run_app(transport))
        ((_, record),) = capture.written
        assert_base64_poison(self, record, INVALID_UTF8_IN_STRING)
        self.assertEqual(1, receiver.accepted)


class _PublishCapture:
    """Replaces ``transport.publish`` so a transport whose publish needs a client library (or a
    live connection) can still be asked what the runtime wrote."""

    def __init__(self, transport: Any) -> None:
        self.written: List[Tuple[str, str]] = []
        transport.publish = self.publish

    def publish(self, queue: str, body: str) -> None:
        self.written.append((queue, body))


class AsbPoisonTest(unittest.TestCase):
    def test_a_poison_message_is_dead_lettered_and_completed(self) -> None:
        for body in ("not json", '{"foo":1}'):
            with self.subTest(body=body):
                received = AsbReceived(body)
                receiver = AsbReceiver([received])
                transport = AsbTransport(client=AsbClient(receiver=receiver))
                capture = _PublishCapture(transport)
                app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
                app.register(URN, lambda data, meta: None)
                app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
                ((queue, record),) = capture.written
                self.assertEqual("orders.dlq", queue)
                assert_poison_record(self, record, body)
                self.assertEqual([received], receiver.completed)


class ArtemisPoisonTest(unittest.TestCase):
    def test_a_poison_message_is_dead_lettered_and_accepted(self) -> None:
        for body in ("not json", '{"foo":1}'):
            with self.subTest(body=body):
                receiver = ArtemisReceiver([ArtemisMessage(body)])
                transport = ArtemisTransport(connection=ArtemisConnection(receiver=receiver))
                capture = _PublishCapture(transport)
                app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
                app.register(URN, lambda data, meta: None)
                app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
                ((queue, record),) = capture.written
                self.assertEqual("orders.dlq", queue)
                assert_poison_record(self, record, body)
                self.assertEqual(1, receiver.accepted)

    def test_bytes_that_are_not_utf_8_do_not_crash_the_consumer(self) -> None:
        receiver = ArtemisReceiver([ArtemisMessage(b"\xff\xfe{garbage")])
        transport = ArtemisTransport(connection=ArtemisConnection(receiver=receiver))
        capture = _PublishCapture(transport)
        app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        ((_, record),) = capture.written
        parsed = json.loads(record)
        self.assertEqual("base64", parsed["raw_encoding"])
        self.assertEqual(b"\xff\xfe{garbage", base64.b64decode(parsed["raw"]))


class _FakeChannel:
    is_closed = False

    def __init__(self, messages: List[Tuple[Any, Any, Any]]) -> None:
        self._messages = messages
        self.acked: List[Any] = []

    def queue_declare(self, **kwargs: Any) -> None:
        return None

    def basic_get(self, queue: str, auto_ack: bool = False) -> Tuple[Any, Any, Any]:
        return self._messages.pop(0) if self._messages else (None, None, None)

    def basic_ack(self, delivery_tag: Any) -> None:
        self.acked.append(delivery_tag)


class _Method:
    delivery_tag = 7


class AmqpPoisonTest(unittest.TestCase):
    def transport(self, body: bytes) -> Tuple[PikaTransport, _FakeChannel]:
        channel = _FakeChannel([(_Method(), None, body)])
        transport = PikaTransport.__new__(PikaTransport)
        transport._pika = None
        transport._url = "amqp://x"
        transport._declared = set()
        transport._connection = type("C", (), {"is_closed": False, "sleep": lambda self, t: None})()
        transport._channel = channel
        return transport, channel

    def test_a_poison_message_is_dead_lettered_and_acked(self) -> None:
        transport, channel = self.transport(b"not json")
        capture = _PublishCapture(transport)
        app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
        app.register(URN, lambda data, meta: None)
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        ((queue, record),) = capture.written
        self.assertEqual("orders.dlq", queue)
        assert_poison_record(self, record, "not json")
        self.assertEqual([7], channel.acked)

    def test_bytes_that_are_not_utf_8_do_not_crash_the_consumer(self) -> None:
        transport, channel = self.transport(b"\xff\xfe{garbage")
        capture = _PublishCapture(transport)
        app = BabelQueue(transport=transport, queue="orders", max_attempts=3, dead_letter=True)
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        ((_, record),) = capture.written
        parsed = json.loads(record)
        self.assertEqual("base64", parsed["raw_encoding"])
        self.assertEqual(b"\xff\xfe{garbage", base64.b64decode(parsed["raw"]))
        self.assertEqual([7], channel.acked)


class TransportsThatClassifyPoisonThemselvesTest(unittest.TestCase):
    def test_only_kafka_and_redis_opt_out_of_the_runtime_classification(self) -> None:
        from babelqueue.kafka_transport import KafkaTransport
        from babelqueue.redis_transport import RedisTransport

        self.assertTrue(KafkaTransport.classifies_poison)
        self.assertTrue(RedisTransport.classifies_poison)
        for cls in (InMemoryTransport, SqsTransport, PikaTransport, PulsarTransport, AsbTransport, ArtemisTransport):
            with self.subTest(transport=cls.__name__):
                self.assertFalse(cls.classifies_poison)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
