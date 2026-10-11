"""Live-Kafka integration tests (opt-in).

Skipped in the normal suite. Enable with ``BQ_INTEGRATION=1`` and (optionally) ``BQ_KAFKA_BROKERS``
(default ``localhost:9092``); requires the ``kafka`` extra (``confluent-kafka``). They exercise the
real ``confluent_kafka`` produce / poll / commit / seek paths that the fake-based tests in
``test_kafka_transport.py`` cannot. Every test uses unique topic names and a unique consumer group.

Scenarios: (1) a handler error is retried up to ``max_attempts`` and then dead-lettered to
``<queue>.dlq`` with a ``dead_letter`` block; (2) a record whose ``bq-schema-version`` header is
``"2"`` is gated before any decode and dead-lettered directly as the incoming raw value,
byte-identical with no ``dead_letter`` block, never reaching a handler; (3) the record key
(contract §6.5 "Record key on republish") survives the retry republish, the DLQ write, the redrive
dry-run restore and the redrive itself, byte-for-byte: a null key stays null, an empty key stays
empty, a non-empty key stays non-null. Every hop is read back from the broker with a raw consumer.
"""

from __future__ import annotations

import json
import os
import time
import unittest
import uuid
from typing import Any, Dict, List, Optional, Tuple

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.kafka_transport import KafkaTransport
from babelqueue.redrive import redrive

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


def _read_records(topic: str, count: int) -> List[Tuple[int, Optional[bytes]]]:
    """Read the first ``count`` records of ``topic`` with a raw consumer (no transport logic, no
    group state) and return ``(offset, key)`` pairs; ``key`` is ``None`` for a null key and
    ``b""`` for an empty one. Fails the test if fewer arrive before the deadline."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": f"bq-it-raw-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    records: List[Tuple[int, Optional[bytes]]] = []
    try:
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
        stop = time.monotonic() + DEADLINE_SECONDS
        while len(records) < count and time.monotonic() < stop:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise AssertionError(f"read {topic}: {message.error()}")
            records.append((message.offset(), message.key()))
    finally:
        consumer.close()
    if len(records) < count:
        raise AssertionError(f"{topic} holds {len(records)} record(s), want at least {count}")
    return records


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

    def test_null_record_key_survives_retry_dlq_and_redrive(self) -> None:
        self._assert_key_survives(None)

    def test_empty_record_key_survives_retry_dlq_and_redrive(self) -> None:
        self._assert_key_survives(b"")

    def test_non_empty_record_key_survives_retry_dlq_and_redrive(self) -> None:
        self._assert_key_survives(b"order-1042")

    def _assert_key_survives(self, key: Optional[bytes]) -> None:
        """Seed one record with ``key``, fail it through a retry into the DLQ, then restore (dry
        run) and redrive it; the key read back from the broker is identical on every hop."""
        from confluent_kafka import Producer

        sandbox = f"{self.queue}.sandbox"
        _create_topics(sandbox)
        calls: List[int] = []
        app = BabelQueue(
            transport=self._transport(), queue=self.queue, max_attempts=2, dead_letter=True
        )

        @app.handler(URN)
        def boom(data: Dict[str, Any], meta: Dict[str, Any]) -> None:
            calls.append(1)
            raise RuntimeError("boom")

        body = EnvelopeCodec.encode(EnvelopeCodec.make(URN, {"order_id": 7}, queue=self.queue))
        producer = Producer({"bootstrap.servers": BROKERS})
        producer.produce(
            self.queue, value=body.encode("utf-8"), key=key, headers=[("bq-job", URN.encode())]
        )
        producer.flush(30)
        seeded = _read_records(self.queue, 1)
        self._assert_same_key("seeded record", key, seeded[0][1])

        # max_attempts=2: the first failure is republished once onto the work topic (retry), the
        # second one is dead-lettered.
        stop = time.monotonic() + DEADLINE_SECONDS
        while len(calls) < 2 and time.monotonic() < stop:
            app.consume(self.queue, max_messages=1, timeout=2.0, handle_signals=False)
        self.assertEqual(len(calls), 2)

        work = _read_records(self.queue, 2)
        self._assert_same_key("retry republish", key, work[1][1])
        dead = _read_records(self.dlq, 1)
        self._assert_same_key("DLQ write", key, dead[0][1])

        # Dry run restores the record verbatim onto the DLQ (original + restore); the real
        # redrive then drains the restore (the group's committed offset is past the original) onto
        # a sandbox topic. A pop can return nothing while the consumer group is still joining, so
        # each run is repeated until it saw a record (an empty run has no side effects). ``max=1``
        # is deliberate: KafkaTransport.pop re-seeks an un-acked in-flight record, so an unbounded
        # drain (max=0) would keep re-popping the same record.
        ops = self._transport(f"{self.group}-ops")
        stop = time.monotonic() + DEADLINE_SECONDS
        dry = redrive(ops, self.dlq, dry_run=True, max=1, timeout=2.0)
        while not dry.items and time.monotonic() < stop:
            dry = redrive(ops, self.dlq, dry_run=True, max=1, timeout=2.0)
        self.assertEqual(len(dry.items), 1)
        restored = _read_records(self.dlq, 2)
        self._assert_same_key("dry-run restore", key, restored[1][1])

        moved = redrive(ops, self.dlq, to_queue=sandbox, max=1, timeout=2.0)
        while not moved.items and time.monotonic() < stop:
            moved = redrive(ops, self.dlq, to_queue=sandbox, max=1, timeout=2.0)
        self.assertEqual(moved.redriven, 1)
        redriven = _read_records(sandbox, 1)
        self._assert_same_key("redrive", key, redriven[0][1])

    def _assert_same_key(self, hop: str, expected: Optional[bytes], actual: Optional[bytes]) -> None:
        # None (null key) and b"" (empty key) are different values, so == also tells them apart.
        self.assertEqual(actual is None, expected is None, f"{hop}: null-ness changed ({actual!r})")
        self.assertEqual(actual, expected, f"{hop}: key changed")

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
