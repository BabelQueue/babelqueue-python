"""Live-Kafka integration tests (opt-in).

Skipped in the normal suite. Enable with ``BQ_INTEGRATION=1`` and (optionally) ``BQ_KAFKA_BROKERS``
(default ``localhost:9092``); requires the ``kafka`` extra (``confluent-kafka``). They exercise the
real ``confluent_kafka`` produce / poll / commit / seek paths that the fake-based tests in
``test_kafka_transport.py`` cannot. Every test uses unique topic names and a unique consumer group.

Scenarios: (1) a handler error is retried up to ``max_attempts`` and then dead-lettered to
``<queue>.dlq`` with a ``dead_letter`` block; (2) a record whose ``bq-schema-version`` header is
``"2"`` is gated before any decode and dead-lettered directly as the incoming raw value,
byte-identical with no ``dead_letter`` block, never reaching a handler.
"""

from __future__ import annotations

import json
import os
import time
import unittest
import uuid
from typing import Any, Dict, List, Optional

from babelqueue import BabelQueue
from babelqueue.kafka_transport import KafkaTransport

URN = "urn:babel:orders:created"
ENABLED = os.environ.get("BQ_INTEGRATION") == "1"
BROKERS = os.environ.get("BQ_KAFKA_BROKERS", "localhost:9092")
DEADLINE_SECONDS = 90.0


def _create_topics(*topics: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BROKERS})
    futures = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics])
    for future in futures.values():
        future.result(30)


def _pop_raw_until(transport: KafkaTransport, queue: str) -> Optional[str]:
    """Poll ``queue`` until a record arrives (or the deadline passes); ack it and return its raw
    body, undecoded. The ack matters: an un-acked record is "abandoned" and the next ``pop`` seeks
    back to it, so a later ``pop`` would hand the very same record out again."""
    stop = time.monotonic() + DEADLINE_SECONDS
    while time.monotonic() < stop:
        received = transport.pop(queue, timeout=2.0)
        if received is not None:
            transport.ack(received)
            return received.body
    return None


def _pop_until(transport: KafkaTransport, queue: str) -> Optional[Dict[str, Any]]:
    """:func:`_pop_raw_until`, with the body decoded as JSON."""
    body = _pop_raw_until(transport, queue)
    return json.loads(body) if body is not None else None


@unittest.skipUnless(ENABLED, "live-broker test: set BQ_INTEGRATION=1 (and BQ_KAFKA_BROKERS) to run")
class KafkaLiveIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        suffix = uuid.uuid4().hex[:8]
        self.queue = f"bq-it-orders-{suffix}"
        self.dlq = f"{self.queue}.dlq"
        self.group = f"bq-it-group-{suffix}"
        _create_topics(self.queue, self.dlq)
        self.transports: List[KafkaTransport] = []

    def tearDown(self) -> None:
        for transport in self.transports:
            transport.close()

    def _transport(self, group: Optional[str] = None) -> KafkaTransport:
        transport = KafkaTransport(f"kafka://{BROKERS}", group_id=group or self.group)
        self.transports.append(transport)
        return transport

    def test_handler_error_retries_then_dead_letters_after_max_attempts(self) -> None:
        calls: List[int] = []
        app = BabelQueue(
            transport=self._transport(), queue=self.queue, max_attempts=3, dead_letter=True
        )

        @app.handler(URN)
        def boom(data: Dict[str, Any], meta: Dict[str, Any]) -> None:
            calls.append(1)
            raise RuntimeError("boom")

        app.publish(URN, {"order_id": 7}, trace_id="trace-it-1")

        stop = time.monotonic() + DEADLINE_SECONDS
        while len(calls) < 3 and time.monotonic() < stop:
            app.consume(self.queue, max_messages=1, timeout=2.0, handle_signals=False)
        self.assertEqual(len(calls), 3)

        envelope = _pop_until(self._transport(f"{self.group}-dlq"), self.dlq)
        self.assertIsNotNone(envelope, f"nothing arrived on {self.dlq}")
        assert envelope is not None  # for the type checker
        self.assertIn("dead_letter", envelope)
        self.assertEqual(envelope["dead_letter"]["reason"], "failed")
        self.assertEqual(envelope["dead_letter"]["original_queue"], self.queue)
        self.assertEqual(envelope["trace_id"], "trace-it-1")
        self.assertEqual(envelope["data"], {"order_id": 7})

    def test_schema_version_gate_rejection_dead_letters_without_running_a_handler(self) -> None:
        from confluent_kafka import Producer

        calls: List[int] = []
        app = BabelQueue(
            transport=self._transport(), queue=self.queue, max_attempts=3, dead_letter=True
        )

        @app.handler(URN)
        def should_not_run(data: Dict[str, Any], meta: Dict[str, Any]) -> None:
            calls.append(1)

        # The §6.7 gate decides on the *header* alone, before the body is decoded. The body here is
        # deliberately not a v1 envelope (a "future format"), so this also proves the gate never
        # tries to decode it. The rejected record is dead-lettered as the incoming raw value,
        # byte-identical, with no ``dead_letter`` block (§6.7, like the Java/.NET/Node SDKs).
        raw = f"future-format-{self.queue}".encode("utf-8")
        producer = Producer({"bootstrap.servers": BROKERS})
        producer.produce(
            self.queue,
            value=raw,
            headers=[("bq-job", URN.encode()), ("bq-schema-version", b"2")],
        )
        producer.flush(30)

        processed = 0
        stop = time.monotonic() + DEADLINE_SECONDS
        while processed == 0 and time.monotonic() < stop:
            processed = app.consume(self.queue, max_messages=1, timeout=2.0, handle_signals=False)
        self.assertEqual(processed, 1)

        dlq_transport = self._transport(f"{self.group}-dlq")
        dead_body = _pop_raw_until(dlq_transport, self.dlq)
        self.assertIsNotNone(dead_body, f"nothing arrived on {self.dlq}")
        assert dead_body is not None  # for the type checker
        self.assertEqual(calls, [])
        # Byte-identical to the incoming value; never decoded, so no dead_letter block either.
        self.assertEqual(dead_body.encode("utf-8"), raw)
        self.assertNotIn("dead_letter", dead_body)

        # Exactly one record was dead-lettered (the gate does not retry or double-write).
        self.assertIsNone(dlq_transport.pop(self.dlq, timeout=3.0), "a second record reached the DLQ")

        # The source record was settled: the group's committed offset moved past it.
        self.assertEqual(self._committed_offset(), 1)

    def _committed_offset(self) -> int:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer(
            {"bootstrap.servers": BROKERS, "group.id": self.group, "enable.auto.commit": False}
        )
        try:
            stop = time.monotonic() + 30.0
            offset = -1
            while time.monotonic() < stop:
                committed = consumer.committed([TopicPartition(self.queue, 0)], timeout=10)
                offset = committed[0].offset
                if offset >= 1:
                    break
                time.sleep(0.5)
            return offset
        finally:
            consumer.close()
