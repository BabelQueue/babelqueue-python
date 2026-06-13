"""Apache Kafka transport tests.

The header projection and the ``attempts`` reconciliation (authoritative ``bq-attempts``
header, body fallback) — and the full publish / pop / ack flow — run with no broker and no
``confluent-kafka`` (the kafka import is lazy and the transport talks to injected
producer/consumer fakes). Only ``_build_producer``/``_build_consumer`` need the real package.
"""

from __future__ import annotations

import unittest

from babelqueue import EnvelopeCodec
from babelqueue.kafka_transport import KafkaTransport
from babelqueue.transport import ReceivedMessage, make_transport

URN = "urn:babel:orders:created"


def body(attempts: int = 0) -> str:
    env = EnvelopeCodec.decode(
        EnvelopeCodec.encode(EnvelopeCodec.make(URN, {"order_id": 1042}, queue="orders"))
    )
    env["attempts"] = attempts
    return EnvelopeCodec.encode(env)


def header_map(headers):
    return {k: (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v) for k, v in headers}


class FakeMessage:
    def __init__(self, value, headers=None, err=None) -> None:
        self._value = value
        self._headers = headers or []
        self._err = err

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def error(self):
        return self._err


class FakeProducer:
    def __init__(self) -> None:
        self.produced = []

    def produce(self, topic, value=None, headers=None, timestamp=None, **kw) -> None:
        self.produced.append({"topic": topic, "value": value, "headers": headers, "timestamp": timestamp})

    def poll(self, _t):
        return 0

    def flush(self, *a):
        return 0


class FakeConsumer:
    def __init__(self, messages=()) -> None:
        self._messages = list(messages)
        self.committed = []

    def poll(self, timeout=None):
        return self._messages.pop(0) if self._messages else None

    def commit(self, message=None, asynchronous=True) -> None:
        self.committed.append(message)

    def close(self) -> None:
        pass


def transport(producer=None, consumer=None) -> KafkaTransport:
    producer = producer if producer is not None else FakeProducer()
    consumer = consumer if consumer is not None else FakeConsumer()
    return KafkaTransport(producer=producer, consumer_factory=lambda _q: consumer)


class KafkaProjectionTest(unittest.TestCase):
    def test_projection_maps_contract_headers(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        headers = header_map(KafkaTransport._projection(raw))

        self.assertEqual(headers["bq-job"], URN)
        self.assertEqual(headers["bq-trace-id"], env["trace_id"])
        self.assertEqual(headers["bq-message-id"], env["meta"]["id"])
        self.assertEqual(headers["bq-schema-version"], str(env["meta"]["schema_version"]))
        self.assertEqual(headers["bq-source-lang"], env["meta"]["lang"])
        self.assertEqual(headers["bq-attempts"], "0")

    def test_projection_values_are_bytes(self):
        for _key, value in KafkaTransport._projection(body(3)):
            self.assertIsInstance(value, bytes)

    def test_projection_of_garbage_is_empty(self):
        self.assertEqual(KafkaTransport._projection("not-json"), [])

    def test_reconcile_header_is_authoritative(self):
        out = KafkaTransport._reconcile(body(0), [("bq-attempts", b"2")])
        self.assertEqual(EnvelopeCodec.decode(out)["attempts"], 2)

    def test_reconcile_absent_header_falls_back_to_body(self):
        raw = body(3)
        self.assertEqual(KafkaTransport._reconcile(raw, []), raw)

    def test_reconcile_garbage_header_falls_back(self):
        raw = body(3)
        self.assertEqual(KafkaTransport._reconcile(raw, [("bq-attempts", b"x")]), raw)


class KafkaPublishTest(unittest.TestCase):
    def test_publish_writes_value_headers_and_timestamp(self):
        raw = body()
        env = EnvelopeCodec.decode(raw)
        producer = FakeProducer()
        transport(producer=producer).publish("orders", raw)

        self.assertEqual(len(producer.produced), 1)
        rec = producer.produced[0]
        self.assertEqual(rec["topic"], "orders")
        self.assertEqual(rec["value"], raw.encode("utf-8"))
        self.assertEqual(rec["timestamp"], env["meta"]["created_at"])
        self.assertEqual(header_map(rec["headers"])["bq-job"], URN)


class KafkaConsumeTest(unittest.TestCase):
    def test_pop_reconciles_attempts_and_returns_handle(self):
        message = FakeMessage(body(0).encode("utf-8"), headers=[("bq-attempts", b"2")])
        consumer = FakeConsumer([message])
        tr = transport(consumer=consumer)

        msg = tr.pop("orders")

        self.assertIsNotNone(msg)
        self.assertIs(msg.handle, message)
        self.assertEqual(EnvelopeCodec.decode(msg.body)["attempts"], 2)

    def test_pop_empty_returns_none(self):
        self.assertIsNone(transport(consumer=FakeConsumer([])).pop("orders"))

    def test_pop_error_returns_none(self):
        message = FakeMessage(body().encode("utf-8"), err="partition eof")
        self.assertIsNone(transport(consumer=FakeConsumer([message])).pop("orders"))

    def test_ack_commits_the_reserved_message(self):
        message = FakeMessage(body().encode("utf-8"))
        consumer = FakeConsumer([message])
        tr = transport(consumer=consumer)

        tr.ack(tr.pop("orders"))

        self.assertEqual(consumer.committed, [message])

    def test_ack_noop_without_handle(self):
        transport().ack(ReceivedMessage(body="", queue="orders", handle=None))  # no error


class KafkaSchemeTest(unittest.TestCase):
    def test_make_transport_routes_kafka_scheme_lazily(self):
        # Construction is lazy (no client built), so it succeeds with no confluent-kafka.
        tr = make_transport("kafka://localhost:9092")
        self.assertIsInstance(tr, KafkaTransport)


if __name__ == "__main__":
    unittest.main()
