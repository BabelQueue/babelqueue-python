"""Apache Kafka transport tests.

The header projection and the ``attempts`` reconciliation (authoritative ``bq-attempts``
header, body fallback) — and the full publish / pop / ack flow — run with no broker and no
``confluent-kafka`` (the kafka import is lazy and the transport talks to injected
producer/consumer fakes). Only ``_build_producer``/``_build_consumer`` need the real package.
"""

from __future__ import annotations

import json
import unittest
from unittest import mock
from urllib.parse import quote

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.kafka_transport import NON_UTF8_ROUTE, KafkaDeliveryError, KafkaTransport
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
    def __init__(self, value, headers=None, err=None, key=None) -> None:
        self._value = value
        self._headers = headers or []
        self._err = err
        self._key = key

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def error(self):
        return self._err


class FakeProducer:
    """Delivers every message successfully; delivery reports are served from ``flush``/``poll``
    (like librdkafka)."""

    def __init__(self) -> None:
        self.produced = []
        self._reports = []

    def produce(self, topic, value=None, headers=None, timestamp=None, on_delivery=None, **kw) -> None:
        self.produced.append(
            {
                "topic": topic,
                "value": value,
                "headers": headers,
                "timestamp": timestamp,
                "key": kw.get("key"),
            }
        )
        if on_delivery is not None:
            self._reports.append(on_delivery)

    def poll(self, _t):
        return 0

    def flush(self, *a):
        reports, self._reports = self._reports, []
        for report in reports:
            report(None, None)
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


class OffsetMessage(FakeMessage):
    """A record that knows its place in a single-partition log."""

    def __init__(self, value, headers, offset, topic="orders", partition=0, key=None) -> None:
        super().__init__(value, headers=headers, key=key)
        self._offset, self._topic, self._partition = offset, topic, partition

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class OffsetConsumer:
    """A one-partition log with Kafka's cumulative position semantics: ``poll`` advances the
    in-memory position, ``commit(message)`` stores ``offset + 1`` for everything before it, and
    ``seek`` moves the in-memory position back."""

    def __init__(self, records) -> None:
        self.log = list(records)
        self.position = 0
        self.committed_offset = 0
        self.commits = []
        self.seeks = []

    def poll(self, timeout=None):
        if self.position >= len(self.log):
            return None
        record = self.log[self.position]
        self.position += 1
        return record

    def commit(self, message=None, asynchronous=True) -> None:
        self.committed_offset = message.offset() + 1
        self.commits.append(message.offset())

    def seek(self, partition) -> None:
        self.seeks.append((partition.topic, partition.partition, partition.offset))
        self.position = partition.offset

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

    def test_publish_does_not_log_a_forbidden_key_drop(self):
        # The value is sent unchanged, so the header/timestamp projection must not claim a drop.
        raw = _with_forbidden_key(body())
        producer = FakeProducer()
        _assert_no_codec_warnings(self, lambda: transport(producer=producer).publish("orders", raw))
        self.assertEqual(producer.produced[0]["value"], raw.encode("utf-8"))
        self.assertEqual(header_map(producer.produced[0]["headers"])["bq-job"], URN)

    def test_publish_rejects_a_body_that_is_not_valid_utf8(self):
        # A user publish stays strict: a lone surrogate (e.g. from os.fsdecode) must raise
        # instead of being written as a non-UTF-8 record with no projected headers.
        bad = '{"name": "' + b"caf\xe9".decode("utf-8", "surrogateescape") + '"}'
        producer = FakeProducer()
        with self.assertRaises(UnicodeEncodeError):
            transport(producer=producer).publish("orders", bad)
        self.assertEqual(producer.produced, [])

    def test_publish_through_the_app_rejects_surrogate_data(self):
        producer = FakeProducer()
        app = BabelQueue(transport=transport(producer=producer))
        with self.assertRaises(UnicodeEncodeError):
            app.publish(URN, {"name": b"caf\xe9".decode("utf-8", "surrogateescape")}, queue="orders")
        self.assertEqual(producer.produced, [])

    def test_publish_settle_writes_a_non_utf8_body_back_raw_without_headers(self):
        # Only the settle path may write a consumed non-UTF-8 record back byte-for-byte.
        raw = b'{"job": "x", "name": "caf\xe9"}'
        producer = FakeProducer()
        transport(producer=producer).publish_settle("orders.dlq", raw.decode("utf-8", "surrogateescape"))
        self.assertEqual(len(producer.produced), 1)
        self.assertEqual(producer.produced[0]["value"], raw)
        self.assertEqual(producer.produced[0]["headers"], [])


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


class KafkaSchemaVersionGateTest(unittest.TestCase):
    """The ``bq-schema-version`` pre-decode gate (broker-bindings.md §6.7 step 2)."""

    PREFIX = "babelqueue:unsupported-schema-version:"

    def _pop(self, raw, headers):
        message = FakeMessage(raw.encode("utf-8"), headers=headers)
        return transport(consumer=FakeConsumer([message])).pop("orders"), message

    def test_missing_or_blank_header_decodes_normally(self):
        # blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r), identical in every SDK
        blanks = (b"", b"  ", b"\x0b", b"\x0c", b" \t\n\x0b\x0c\r")
        cases = [None, [], [("other", b"x")], [("bq-schema-version", None)]]
        cases += [[("bq-schema-version", v), ("bq-attempts", b"2")] for v in blanks]
        for headers in cases:
            with self.subTest(headers=headers):
                got, _ = self._pop(body(), headers)
                self.assertEqual(got.route_urn, "")
                self.assertEqual(EnvelopeCodec.decode(got.body)["job"], URN)

    def test_supported_version_is_delivered(self):
        got, message = self._pop(body(), [("bq-schema-version", b"1"), ("bq-attempts", b"2")])
        self.assertEqual(got.route_urn, "")
        self.assertIs(got.handle, message)
        self.assertEqual(EnvelopeCodec.decode(got.body)["attempts"], 2)  # reconciled

    def test_unsupported_version_returns_the_route_without_decoding(self):
        garbage = "{not json"
        values = ("2", "x", "01", " 1", "1.0", "-1", "\u00a0", "\u001c", "\u0085", "\ufeff", "\u3000")
        for value in values:
            with self.subTest(value=value):
                with mock.patch.object(
                    EnvelopeCodec, "decode", side_effect=AssertionError("decode must not run")
                ) as decode:
                    got, message = self._pop(garbage, [("bq-schema-version", value.encode("utf-8"))])
                decode.assert_not_called()
                self.assertEqual(got.route_urn, self.PREFIX + quote(value, safe=""))
                self.assertEqual(got.body, garbage)
                self.assertIs(got.handle, message)

    def test_route_percent_escapes_the_rejected_version(self):
        got, _ = self._pop("{not json", [("bq-schema-version", b"2&x=?bq-job=%")])
        self.assertEqual(got.route_urn, self.PREFIX + "2%26x%3D%3Fbq-job%3D%25")

    def test_gate_does_not_decode_a_valid_body_either(self):
        raw = body()
        with mock.patch.object(
            EnvelopeCodec, "decode", side_effect=AssertionError("decode must not run")
        ):
            got, _ = self._pop(raw, [("bq-schema-version", b"2"), ("bq-attempts", b"5")])
        self.assertEqual(got.body, raw)  # not reconciled, byte-identical
        self.assertEqual(got.route_urn, self.PREFIX + "2")

    def test_first_header_wins_and_key_may_be_bytes(self):
        got, _ = self._pop("{not json", [(b"bq-schema-version", b"2"), ("bq-schema-version", b"1")])
        self.assertEqual(got.route_urn, self.PREFIX + "2")
        got, _ = self._pop(body(), [("bq-schema-version", b"1"), ("bq-schema-version", b"2")])
        self.assertEqual(got.route_urn, "")

    def test_gate_logs_a_warning_naming_the_version_and_queue(self):
        with self.assertLogs("babelqueue.kafka", level="WARNING") as logs:
            self._pop("{not json", [("bq-schema-version", b"2")])
        self.assertIn("'2'", logs.output[0])
        self.assertIn("'orders'", logs.output[0])

    def test_supported_version_does_not_log(self):
        from babelqueue import kafka_transport

        with mock.patch.object(kafka_transport._logger, "warning") as warn:
            self._pop(body(), [("bq-schema-version", b"1")])
            self._pop(body(), None)
        warn.assert_not_called()

    def test_gated_record_is_committed_through_its_handle(self):
        message = FakeMessage(body().encode("utf-8"), headers=[("bq-schema-version", b"2")])
        consumer = FakeConsumer([message])
        tr = transport(consumer=consumer)
        tr.ack(tr.pop("orders"))
        self.assertEqual(consumer.committed, [message])

    def test_app_dead_letters_unsupported_version_without_reaching_a_handler(self):
        # max_attempts > 1: a gated record is dead-lettered directly, never retried
        for value in (b"2", b"x", b"2&x=%"):
            with self.subTest(value=value):
                producer = FakeProducer()
                incoming = body().encode("utf-8")
                consumer = FakeConsumer(
                    [FakeMessage(incoming, headers=[("bq-schema-version", value)])]
                )
                app = BabelQueue(
                    transport=transport(producer, consumer), max_attempts=5, dead_letter=True
                )
                seen: list = []

                @app.handler(URN)
                def _on(data, meta):
                    seen.append(data)

                app.consume(queue="orders", max_messages=1)

                self.assertEqual(seen, [])
                self.assertEqual(len(consumer.committed), 1)
                self.assertEqual(len(producer.produced), 1)
                self.assertEqual(producer.produced[0]["topic"], "orders.dlq")
                # rejected without decoding: the DLQ record is the incoming body, byte-identical
                self.assertEqual(producer.produced[0]["value"], incoming)
                self.assertNotIn(b"dead_letter", producer.produced[0]["value"])

    def _failing_dlq_producer(self, failures):
        class DlqFailingProducer(FakeProducer):
            remaining = failures

            def produce(self, topic, value=None, headers=None, timestamp=None, **kw) -> None:
                if topic.endswith(".dlq") and self.remaining > 0:
                    self.remaining -= 1
                    raise RuntimeError("dlq produce refused")
                super().produce(topic, value=value, headers=headers, timestamp=timestamp, **kw)

        return DlqFailingProducer()

    def test_failing_dead_letter_write_never_retries_or_reaches_a_handler(self):
        producer = self._failing_dlq_producer(failures=10**6)  # the DLQ stays down
        consumer = OffsetConsumer([])
        consumer.log = [None] * 7 + [
            OffsetMessage(body().encode("utf-8"), [("bq-schema-version", b"2")], offset=7)
        ]
        consumer.position = consumer.committed_offset = 7
        app = BabelQueue(transport=transport(producer, consumer), max_attempts=5, dead_letter=True)
        app._rewind_pause = 0.0  # noqa: SLF001
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            seen.append(data)

        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume(queue="orders", max_messages=3)

        self.assertEqual(seen, [])
        self.assertEqual(producer.produced, [])  # no retry copy on "orders", nothing on the DLQ
        self.assertEqual(consumer.commits, [])  # never acknowledged
        self.assertEqual(consumer.seeks, [("orders", 0, 7)] * 3)  # rewound each time

    def test_failed_gated_record_is_not_lost_when_a_later_record_is_committed(self):
        # Cumulative offsets: committing record 8 would silently skip an unacked record 7.
        producer = self._failing_dlq_producer(failures=2)
        incoming = body().encode("utf-8")
        gated = OffsetMessage(incoming, [("bq-schema-version", b"2")], offset=7)
        later = OffsetMessage(body().encode("utf-8"), [("bq-schema-version", b"1")], offset=8)
        consumer = OffsetConsumer([])
        consumer.log = [None] * 7 + [gated, later]
        consumer.position = consumer.committed_offset = 7
        app = BabelQueue(transport=transport(producer, consumer), max_attempts=5, dead_letter=True)
        app._rewind_pause = 0.0  # noqa: SLF001
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            seen.append(data)

        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume(queue="orders", max_messages=4)  # gated x3 (2 failed writes) + later

        # The gated record reached the DLQ before the later record was committed past it.
        self.assertEqual([m["topic"] for m in producer.produced], ["orders.dlq"])
        self.assertEqual(producer.produced[0]["value"], incoming)  # raw, byte-identical
        self.assertNotIn(b"dead_letter", producer.produced[0]["value"])
        self.assertEqual(consumer.commits, [7, 8])
        self.assertEqual(consumer.committed_offset, 9)
        self.assertEqual(len(seen), 1)  # only the supported record reached the handler

    def test_failed_seek_logs_the_record_position_and_reraises(self):
        class BrokenSeek(OffsetConsumer):
            def seek(self, partition) -> None:
                raise RuntimeError("seek refused")

        consumer = BrokenSeek([])
        record = OffsetMessage(b"{}", [], offset=42, topic="orders", partition=3)
        with self.assertLogs("babelqueue.kafka", level="ERROR") as logs:
            with self.assertRaises(RuntimeError):
                transport(consumer=consumer).rewind(
                    ReceivedMessage(body="", queue="orders", handle=record)
                )
        self.assertIn("orders[3]@42", logs.output[0])

    def test_rewind_without_a_handle_is_a_no_op(self):
        consumer = OffsetConsumer([])
        transport(consumer=consumer).rewind(ReceivedMessage(body="", queue="orders", handle=None))
        self.assertEqual(consumer.seeks, [])

    def test_long_rejected_version_is_truncated_in_log_and_route(self):
        got, _ = self._pop("{not json", [("bq-schema-version", b"9" * 100)])
        self.assertEqual(got.route_urn, self.PREFIX + "9" * 64 + "...")


class KafkaRestartRedeliveryTest(unittest.TestCase):
    """A record whose settlement raised (a failed retry / DLQ / release ``publish``) must be
    redelivered by a restarted consume loop — never skipped by committing the record after it."""

    @staticmethod
    def _down_producer(failures=1):
        """A producer whose next ``failures`` writes (to any topic) are refused."""

        class DownProducer(FakeProducer):
            remaining = failures

            def produce(self, topic, value=None, headers=None, timestamp=None, **kw) -> None:
                if self.remaining > 0:
                    self.remaining -= 1
                    raise RuntimeError("broker refused the write")
                super().produce(topic, value=value, headers=headers, timestamp=timestamp, **kw)

        return DownProducer()

    @staticmethod
    def _log(*records):
        consumer = OffsetConsumer(records)
        return consumer

    def _app(self, producer, consumer, max_attempts=3, **kw):
        app = BabelQueue(transport=transport(producer, consumer), max_attempts=max_attempts, **kw)
        app._rewind_pause = 0.0  # noqa: SLF001
        return app

    def test_failed_retry_publish_is_redelivered_not_skipped_by_the_next_commit(self):
        first = OffsetMessage(body().encode("utf-8"), [], offset=0)
        second = OffsetMessage(body().encode("utf-8"), [], offset=1)
        consumer = self._log(first, second)
        producer = self._down_producer(failures=1)
        app = self._app(producer, consumer, dead_letter=True)
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            seen.append(meta.get("id"))
            if len(seen) == 1:
                raise RuntimeError("handler failure")

        # The retry copy cannot be published: the failure escapes and the loop dies.
        with self.assertRaises(RuntimeError):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [])
        self.assertEqual(consumer.committed_offset, 0)

        # The loop is restarted on the same consumer: record 0 comes back before record 1.
        app.consume(queue="orders", max_messages=2)
        self.assertEqual(consumer.commits, [0, 1])
        self.assertEqual(len(seen), 3)  # record 0 handled twice, record 1 once
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])

    def test_undecodable_record_does_not_wedge_the_partition(self):
        bad = OffsetMessage(b"\xff\xfe bad", [], offset=0)
        good = OffsetMessage(body().encode("utf-8"), [], offset=1)
        consumer = self._log(bad, good)
        kafka = transport(FakeProducer(), consumer)

        # A value that is not UTF-8 no longer raises out of ``pop``: it is handed over under the
        # reserved route so the runtime can dead-letter the raw bytes (see the delivery tests).
        first = kafka.pop("orders")
        self.assertIsNotNone(first)
        self.assertEqual(first.route_urn, NON_UTF8_ROUTE)
        kafka.ack(first)
        message = kafka.pop("orders")

        self.assertIsNotNone(message)
        self.assertEqual(message.handle.offset(), 1)
        self.assertEqual(consumer.seeks, [])

    def test_failed_dead_letter_write_is_redelivered_after_restart(self):
        first = OffsetMessage(b"not json at all", [], offset=0)
        second = OffsetMessage(body().encode("utf-8"), [], offset=1)
        consumer = self._log(first, second)
        producer = self._down_producer(failures=1)
        app = self._app(producer, consumer, max_attempts=1, dead_letter=True)
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            seen.append(data)

        with self.assertRaises(RuntimeError):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [])

        app.consume(queue="orders", max_messages=2)  # the DLQ is back
        self.assertEqual([m["topic"] for m in producer.produced], ["orders.dlq"])
        dead = EnvelopeCodec.decode(producer.produced[0]["value"].decode("utf-8"))
        self.assertIn("dead_letter", dead)
        self.assertEqual(consumer.commits, [0, 1])
        self.assertEqual(len(seen), 1)

    def test_failed_unknown_urn_dead_letter_is_redelivered_after_restart(self):
        first = OffsetMessage(body().encode("utf-8"), [], offset=0)
        consumer = self._log(first)
        # The strategy's dead-letter write fails, then the generic fallback's write fails too.
        producer = self._down_producer(failures=2)
        app = self._app(  # no handler: unknown URN, dead-lettered by strategy
            producer, consumer, max_attempts=1, dead_letter=True, on_unknown_urn="dead_letter"
        )

        with self.assertRaises(RuntimeError):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [])

        app.consume(queue="orders", max_messages=1)
        self.assertEqual([m["topic"] for m in producer.produced], ["orders.dlq"])
        self.assertEqual(consumer.commits, [0])

    def test_acked_record_is_not_rewound_by_the_next_pop(self):
        consumer = self._log(
            OffsetMessage(body().encode("utf-8"), [], offset=0),
            OffsetMessage(body().encode("utf-8"), [], offset=1),
        )
        kafka = transport(consumer=consumer)
        got = kafka.pop("orders")
        kafka.ack(got)
        self.assertEqual(kafka.pop("orders").handle.offset(), 1)
        self.assertEqual(consumer.seeks, [])

    def test_rewound_record_is_not_rewound_a_second_time(self):
        consumer = self._log(OffsetMessage(body().encode("utf-8"), [], offset=0))
        kafka = transport(consumer=consumer)
        kafka.rewind(kafka.pop("orders"))
        kafka.pop("orders")
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])

    def test_abandoned_record_is_rewound_on_the_next_pop(self):
        consumer = self._log(
            OffsetMessage(body().encode("utf-8"), [], offset=0),
            OffsetMessage(body().encode("utf-8"), [], offset=1),
        )
        kafka = transport(consumer=consumer)
        kafka.pop("orders")  # neither acked nor rewound: the settlement raised
        again = kafka.pop("orders")
        self.assertEqual(again.handle.offset(), 0)
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])

    def test_failing_seek_does_not_wedge_the_next_pop(self):
        class BrokenSeek(OffsetConsumer):
            def seek(self, partition) -> None:
                raise RuntimeError("partition no longer assigned")

        consumer = BrokenSeek([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        kafka = transport(consumer=consumer)
        kafka.pop("orders")
        with self.assertLogs("babelqueue.kafka", level="ERROR") as logs:
            self.assertIsNone(kafka.pop("orders"))
        self.assertIn("orders[0]@0", logs.output[0])


class FakeKafkaError(Exception):
    """Stands in for ``confluent_kafka.KafkaError`` in a failed delivery report."""


class ReportingProducer(FakeProducer):
    """A producer that reports delivery like librdkafka: ``on_delivery`` is served by ``flush``.

    ``outcomes`` is consumed one entry per ``produce``: ``"ok"`` (delivered), ``"error"`` (the
    delivery report carries an error) or ``"timeout"`` (``flush`` returns with a message still
    queued and no report). ``stray`` is a count of unrelated messages that stay queued on the
    producer, so ``flush`` returns it even when every report arrived."""

    def __init__(self, outcomes=(), stray=0) -> None:
        super().__init__()
        self.stray = stray
        self.outcomes = list(outcomes)
        self.confirmed = []  # produce calls that carried a delivery callback
        self._queued = []

    def produce(self, topic, value=None, headers=None, timestamp=None, on_delivery=None, **kw) -> None:
        super().produce(topic, value=value, headers=headers, timestamp=timestamp, **kw)
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        if on_delivery is not None:
            self.confirmed.append(topic)
            self._queued.append((on_delivery, outcome))

    def flush(self, *a):
        remaining = self.stray
        for callback, outcome in self._queued:
            if outcome == "ok":
                callback(None, None)
            elif outcome == "error":
                callback(FakeKafkaError("msg timed out"), None)
            else:
                remaining += 1
        self._queued = []
        return remaining


class KafkaDeliveryConfirmationTest(unittest.TestCase):
    """Retry / dead-letter writes must be confirmed by the broker before the offset is committed,
    and a value that is not UTF-8 is dead-lettered raw instead of being skipped."""

    BAD = b"\xff\xfe not utf-8 \x80"

    def _app(self, producer, consumer, max_attempts=3, **kw):
        app = BabelQueue(transport=transport(producer, consumer), max_attempts=max_attempts, **kw)
        app._rewind_pause = 0.0  # noqa: SLF001
        return app

    @staticmethod
    def _failing_handler(app):
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            seen.append(meta.get("id"))
            raise RuntimeError("handler failure")

        return seen

    def test_plain_publish_is_fire_and_forget_even_while_a_record_is_in_flight(self):
        # A handler's own publish (or any caller's) must keep the pre-existing semantics: no
        # delivery wait and no KafkaDeliveryError, whether or not a record is being processed.
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["error", "ok"])
        app = self._app(producer, consumer)
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            app.publish("urn:babel:other", {"x": 1}, queue="other")  # delivery "fails", ignored
            seen.append(meta.get("id"))

        app.consume(queue="orders", max_messages=1)

        self.assertEqual(len(seen), 1)  # the handler was not failed by the publish
        self.assertEqual(producer.confirmed, [])
        self.assertEqual(consumer.commits, [0])

    def test_plain_publish_after_a_failed_settle_is_still_fire_and_forget(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["error", "error"])
        app = self._app(producer, consumer)
        self._failing_handler(app)
        with self.assertRaises(KafkaDeliveryError):
            app.consume(queue="orders", max_messages=1)  # leaves the record in flight

        app.publish("urn:babel:other", {"x": 1}, queue="other")  # must not raise

        self.assertEqual(producer.confirmed, ["orders"])

    def test_unrelated_queued_messages_do_not_fail_a_confirmed_settle_write(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["ok"], stray=3)  # flush() reports 3 other messages queued
        app = self._app(producer, consumer)
        self._failing_handler(app)

        app.consume(queue="orders", max_messages=1)

        self.assertEqual(consumer.commits, [0])  # this write's own report arrived: delivered
        self.assertEqual(len(producer.produced), 1)  # and it was not republished twice

    def test_confirmed_retry_republish_is_committed(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["ok"])
        app = self._app(producer, consumer)
        self._failing_handler(app)

        app.consume(queue="orders", max_messages=1)

        self.assertEqual(producer.confirmed, ["orders"])
        self.assertEqual(consumer.commits, [0])

    def test_retry_delivery_error_does_not_commit_and_is_redelivered(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["error"])
        app = self._app(producer, consumer)
        seen = self._failing_handler(app)

        with self.assertRaises(KafkaDeliveryError):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [])
        self.assertEqual(consumer.committed_offset, 0)

        app.consume(queue="orders", max_messages=1)  # the broker is back
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])
        self.assertEqual(consumer.commits, [0])
        self.assertEqual(len(seen), 2)

    def test_retry_delivery_timeout_does_not_commit(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["timeout"])
        app = self._app(producer, consumer)
        self._failing_handler(app)

        with self.assertRaises(KafkaDeliveryError):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [])

    def test_envelope_dead_letter_delivery_error_does_not_commit(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer(["error"])
        app = self._app(producer, consumer, max_attempts=1, dead_letter=True)
        self._failing_handler(app)

        with self.assertRaises(KafkaDeliveryError):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual([m["topic"] for m in producer.produced], ["orders.dlq"])
        self.assertEqual(consumer.commits, [])

    def test_gated_dead_letter_delivery_error_does_not_commit(self):
        record = OffsetMessage(body().encode("utf-8"), [("bq-schema-version", b"2")], offset=0)
        consumer = OffsetConsumer([record])
        producer = ReportingProducer(["error"])
        app = self._app(producer, consumer, dead_letter=True)

        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(producer.confirmed, ["orders.dlq"])
        self.assertEqual(consumer.commits, [])
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])

    def test_non_utf8_record_is_dead_lettered_raw_and_committed(self):
        consumer = OffsetConsumer(
            [
                OffsetMessage(self.BAD, [], offset=0),
                OffsetMessage(body().encode("utf-8"), [], offset=1),
            ]
        )
        producer = ReportingProducer()
        app = self._app(producer, consumer, dead_letter=True)
        seen: list = []

        @app.handler(URN)
        def _on(data, meta):
            seen.append(data)

        with self.assertLogs("babelqueue.kafka", level="WARNING"):
            app.consume(queue="orders", max_messages=2)

        self.assertEqual(producer.produced[0]["topic"], "orders.dlq")
        self.assertEqual(producer.produced[0]["value"], self.BAD)  # original bytes, untouched
        self.assertEqual(producer.confirmed[0], "orders.dlq")
        self.assertEqual(consumer.commits, [0, 1])
        self.assertEqual(len(seen), 1)  # the poison record never reached a handler

    def test_non_utf8_record_with_valid_json_is_dead_lettered_raw_without_projected_headers(self):
        # Valid JSON, but written Latin-1: the trace_id carries a byte that is not UTF-8. The header
        # projection cannot encode it, which used to fail the DLQ write on every redelivery and
        # lock the partition.
        envelope = EnvelopeCodec.decode(body())
        envelope["trace_id"] = "tr\u00e9-1"
        latin1 = json.dumps(envelope, ensure_ascii=False).encode("latin-1")
        self.assertIn(b"\xe9", latin1)
        consumer = OffsetConsumer(
            [
                OffsetMessage(latin1, [], offset=0),
                OffsetMessage(body().encode("utf-8"), [], offset=1),
            ]
        )
        producer = ReportingProducer()
        app = self._app(producer, consumer, dead_letter=True)

        @app.handler(URN)
        def _on(data, meta):
            return None

        with self.assertLogs("babelqueue.kafka", level="WARNING"):
            app.consume(queue="orders", max_messages=2)

        self.assertEqual(producer.produced[0]["topic"], "orders.dlq")
        self.assertEqual(producer.produced[0]["value"], latin1)
        self.assertEqual(producer.produced[0]["headers"], [])
        self.assertEqual(consumer.commits, [0, 1])

    def test_non_utf8_dead_letter_delivery_error_does_not_commit(self):
        consumer = OffsetConsumer([OffsetMessage(self.BAD, [], offset=0)])
        producer = ReportingProducer(["error", "ok"])
        app = self._app(producer, consumer, dead_letter=True)

        with self.assertLogs("babelqueue.app", level="ERROR"):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [])
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])

        app.consume(queue="orders", max_messages=1)  # redelivered, the DLQ accepts it now
        self.assertEqual(consumer.commits, [0])
        self.assertEqual([m["value"] for m in producer.produced], [self.BAD, self.BAD])

    def test_non_utf8_record_is_dropped_and_committed_without_a_dlq(self):
        consumer = OffsetConsumer([OffsetMessage(self.BAD, [], offset=0)])
        producer = ReportingProducer()
        app = self._app(producer, consumer, dead_letter=False)

        with self.assertLogs("babelqueue.kafka", level="WARNING"):
            app.consume(queue="orders", max_messages=1)
        self.assertEqual(producer.produced, [])
        self.assertEqual(consumer.commits, [0])

    def test_non_utf8_attempts_header_does_not_raise(self):
        record = FakeMessage(body(2).encode("utf-8"), headers=[("bq-attempts", b"\xff\xfe")])
        message = transport(consumer=FakeConsumer([record])).pop("orders")
        self.assertEqual(EnvelopeCodec.decode(message.body)["attempts"], 2)  # the body's own count


class KafkaKeyPreservationTest(unittest.TestCase):
    """The consumed record's key is carried, byte-for-byte, onto every record the runtime itself
    writes for it (retry republish, envelope DLQ, gate DLQ, raw non-UTF-8 DLQ). Null stays null,
    empty stays empty, and no key is ever made up (§6)."""

    KEYS = (b"order-1042", b"", None)

    def _run(self, record_value, key, headers=(), **app_kw):
        consumer = OffsetConsumer([OffsetMessage(record_value, list(headers), offset=0, key=key)])
        producer = ReportingProducer()
        app = BabelQueue(transport=transport(producer, consumer), **app_kw)
        app._rewind_pause = 0.0  # noqa: SLF001

        @app.handler(URN)
        def _on(data, meta):
            raise RuntimeError("handler failure")

        app.consume(queue="orders", max_messages=1)
        self.assertEqual(consumer.commits, [0])
        self.assertEqual(len(producer.produced), 1)
        return producer.produced[0]

    def test_pop_surfaces_the_record_key(self):
        for key in self.KEYS:
            with self.subTest(key=key):
                got = transport(consumer=FakeConsumer([FakeMessage(body().encode(), key=key)])).pop("orders")
                self.assertEqual(got.key, key)

    def test_pop_tolerates_a_record_without_a_key_accessor(self):
        class NoKey:
            def value(self):
                return body().encode()

            def headers(self):
                return []

            def error(self):
                return None

        self.assertIsNone(transport(consumer=FakeConsumer([NoKey()])).pop("orders").key)

    def test_retry_republish_keeps_the_key(self):
        for key in self.KEYS:
            with self.subTest(key=key):
                record = self._run(body().encode(), key, max_attempts=3)
                self.assertEqual(record["topic"], "orders")
                self.assertEqual(record["key"], key)

    def test_envelope_dead_letter_keeps_the_key(self):
        for key in self.KEYS:
            with self.subTest(key=key):
                record = self._run(body().encode(), key, max_attempts=1, dead_letter=True)
                self.assertEqual(record["topic"], "orders.dlq")
                self.assertEqual(record["key"], key)

    def test_gated_dead_letter_keeps_the_key(self):
        for key in self.KEYS:
            with self.subTest(key=key):
                record = self._run(
                    body().encode(), key, headers=[("bq-schema-version", b"2")], dead_letter=True
                )
                self.assertEqual(record["topic"], "orders.dlq")
                self.assertEqual(record["key"], key)

    def test_non_utf8_raw_dead_letter_keeps_the_key(self):
        bad = b"\xff\xfe not utf-8 \x80"
        for key in self.KEYS:
            with self.subTest(key=key):
                with self.assertLogs("babelqueue.kafka", level="WARNING"):
                    record = self._run(bad, key, dead_letter=True)
                self.assertEqual(record["topic"], "orders.dlq")
                self.assertEqual(record["value"], bad)
                self.assertEqual(record["key"], key)

    def test_a_null_key_is_not_passed_to_produce(self):
        class StrictProducer(FakeProducer):
            def produce(self, topic, value=None, headers=None, timestamp=None, on_delivery=None, **kw):
                assert "key" not in kw, "a null key must not be synthesised"
                super().produce(topic, value=value, headers=headers, timestamp=timestamp, on_delivery=on_delivery)

        transport(producer=StrictProducer()).publish_settle("orders", body(), key=None)

    def test_plain_publish_does_not_set_a_key(self):
        producer = FakeProducer()
        transport(producer=producer).publish("orders", body())
        self.assertIsNone(producer.produced[0]["key"])

    def test_redrive_restore_keeps_the_key(self):
        from babelqueue import redrive

        consumer = FakeConsumer([FakeMessage(b"not json at all", key=b"k-7")])
        producer = FakeProducer()
        result = redrive.redrive(transport(producer, consumer), "orders.dlq", timeout=0)
        self.assertEqual(result.skipped, 1)
        self.assertEqual(producer.produced[0]["topic"], "orders.dlq")
        self.assertEqual(producer.produced[0]["key"], b"k-7")


class KafkaSchemeTest(unittest.TestCase):
    def test_make_transport_routes_kafka_scheme_lazily(self):
        # Construction is lazy (no client built), so it succeeds with no confluent-kafka.
        tr = make_transport("kafka://localhost:9092")
        self.assertIsInstance(tr, KafkaTransport)


if __name__ == "__main__":
    unittest.main()
