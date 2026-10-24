"""Kafka retry tiers (§6.4 / §6.5, ADR-0034): tier selection, tier exhaustion, tier-less
transports, config isolation and the ``RetryReinjector`` re-inject consumer. No broker needed:
the clients are injected fakes."""

from __future__ import annotations

import logging
import sys
import types
import unittest
from unittest import mock

from babelqueue import BabelQueue, BabelQueueError, EnvelopeCodec
from babelqueue.kafka_retry import RetryReinjector, reinject_headers, target_time
from babelqueue.kafka_transport import DEFAULT_RETRY_TIERS_MS, KafkaDeliveryError, KafkaTransport, retry_topic

from test_kafka_transport import (
    FakeConsumer,
    FakeProducer,
    OffsetConsumer,
    OffsetMessage,
    URN,
    ReportingProducer,
    body,
    header_map,
)

TIERS = [5000, 30000, 300000, 1800000]


def _tier_transport(producer, consumer=None, tiers=TIERS):
    consumer = consumer if consumer is not None else FakeConsumer()
    return KafkaTransport(producer=producer, consumer_factory=lambda _q: consumer, retry_tiers_ms=tiers)


class TierExhaustionTest(unittest.TestCase):
    def _redeliver(self, attempts):
        consumer = OffsetConsumer([OffsetMessage(body(attempts).encode("utf-8"), [], offset=0)])
        producer = FakeProducer()
        kafka = _tier_transport(producer, consumer)
        message = kafka.pop("orders", timeout=0)
        kafka.redeliver(message, message.body, 0)
        return producer.produced

    def test_tier_follows_attempts_before_and_stays_on_the_last_tier(self):
        for attempts_before, expected_topic, expected_delay in [
            (0, "orders.retry.1", 5000),
            (1, "orders.retry.2", 30000),
            (2, "orders.retry.3", 300000),
            (3, "orders.retry.4", 1800000),
            (4, "orders.retry.4", 1800000),
            (9, "orders.retry.4", 1800000),
        ]:
            with self.subTest(attempts_before=attempts_before):
                (record,) = self._redeliver(attempts_before)
                self.assertEqual(record["topic"], expected_topic)
                headers = header_map(record["headers"])
                self.assertEqual(headers["bq-delay"], str(expected_delay))
                self.assertEqual(headers["bq-original-topic"], "orders")
                self.assertEqual(headers["bq-attempts"], str(attempts_before + 1))
                self.assertEqual(EnvelopeCodec.decode(record["value"].decode("utf-8"))["attempts"], attempts_before + 1)


class TierlessTest(unittest.TestCase):
    def test_redeliver_without_tiers_raises_and_produces_nothing(self):
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = FakeProducer()
        kafka = KafkaTransport(producer=producer, consumer_factory=lambda _q: consumer)
        message = kafka.pop("orders", timeout=0)
        with self.assertRaises(BabelQueueError):
            kafka.redeliver(message, message.body, 0)
        self.assertEqual(producer.produced, [])
        self.assertEqual(consumer.commits, [])

    def _warnings(self, kafka, **app_kwargs):
        app = BabelQueue(transport=kafka, queue="orders", **app_kwargs)
        with self.assertLogs("babelqueue.app", level="DEBUG") as logs:
            app.consume(max_messages=0, handle_signals=False)
            logging.getLogger("babelqueue.app").debug("sentinel")  # assertLogs needs one record
        return [r for r in logs.records if r.levelno == logging.WARNING]

    def test_consuming_without_tiers_and_with_retries_warns_at_start_up(self):
        kafka = KafkaTransport(producer=FakeProducer(), consumer_factory=lambda _q: FakeConsumer())
        warnings = self._warnings(kafka, max_attempts=3)
        self.assertEqual(len(warnings), 1)
        text = warnings[0].getMessage()
        self.assertIn("no retry tiers", text)
        self.assertIn("max_attempts=3", text)
        self.assertIn("uncommitted", text)
        self.assertIn("redelivered", text)
        self.assertIn("retry_tiers_ms", text)
        self.assertNotIn("stop this worker", text)

    def test_a_failing_handler_without_tiers_does_not_stop_the_worker_and_is_not_committed(self):
        # Faz 14 / Go parity (leaveRepublishUnsettled): the transport still raises, but the runtime
        # logs it, leaves the record unsettled (rewind, no commit) and keeps the consume loop alive.
        consumer = OffsetConsumer([OffsetMessage(body().encode("utf-8"), [], offset=0)])
        producer = ReportingProducer()
        kafka = KafkaTransport(producer=producer, consumer_factory=lambda _q: consumer)
        app = BabelQueue(transport=kafka, queue="orders", max_attempts=3)
        app._rewind_pause = 0.0  # noqa: SLF001
        calls: list = []

        @app.handler(URN)
        def _boom(data, meta):
            calls.append(data)
            raise RuntimeError("handler failed")

        with self.assertLogs("babelqueue.app", level="ERROR") as logs:
            app.consume(max_messages=1, handle_signals=False)  # must not raise
        self.assertEqual(len(calls), 1)
        self.assertEqual(consumer.commits, [])
        self.assertEqual(consumer.seeks, [("orders", 0, 0)])
        self.assertEqual(producer.produced, [])
        self.assertTrue(any("NOT acknowledged" in m for m in logs.output))

    def test_no_warning_when_tiers_are_configured(self):
        kafka = _tier_transport(FakeProducer())
        self.assertEqual(self._warnings(kafka, max_attempts=3), [])

    def test_no_warning_when_retries_are_disabled(self):
        kafka = KafkaTransport(producer=FakeProducer(), consumer_factory=lambda _q: FakeConsumer())
        self.assertEqual(self._warnings(kafka, max_attempts=1), [])

    def test_publish_delayed_without_tiers_raises_and_produces_nothing(self):
        producer = FakeProducer()
        kafka = KafkaTransport(producer=producer, consumer_factory=lambda _q: FakeConsumer())
        with self.assertRaises(BabelQueueError):
            kafka.publish_delayed("orders", body(), 1000)
        self.assertEqual(producer.produced, [])

    def test_publish_delayed_above_the_largest_tier_raises(self):
        producer = FakeProducer()
        kafka = _tier_transport(producer)
        with self.assertRaises(BabelQueueError):
            kafka.publish_delayed("orders", body(), TIERS[-1] + 1)
        self.assertEqual(producer.produced, [])

    def test_publish_delayed_picks_the_smallest_tier_and_records_the_requested_delay(self):
        producer = FakeProducer()
        _tier_transport(producer).publish_delayed("orders", body(), 6000)
        (record,) = producer.produced
        self.assertEqual(record["topic"], "orders.retry.2")
        self.assertEqual(header_map(record["headers"])["bq-delay"], "6000")


class ConfigIsolationTest(unittest.TestCase):
    def test_retry_tiers_ms_never_reaches_the_client_config(self):
        kafka = KafkaTransport(retry_tiers_ms=TIERS, **{"security.protocol": "SASL_SSL"})
        self.assertEqual(kafka._client_config, {"security.protocol": "SASL_SSL"})
        self.assertEqual(kafka.retry_tiers_ms, TIERS)

    def test_the_producer_and_consumer_configs_do_not_carry_it(self):
        configs = []

        class _Client:
            def __init__(self, conf):
                configs.append(dict(conf))

            def subscribe(self, _topics):
                pass

        fake = types.ModuleType("confluent_kafka")
        fake.Producer = _Client
        fake.Consumer = _Client
        with mock.patch.dict(sys.modules, {"confluent_kafka": fake}):
            kafka = KafkaTransport("kafka://broker:9092", retry_tiers_ms=TIERS, **{"linger.ms": 5})
            kafka._build_producer()
            kafka._build_consumer("orders")
        self.assertEqual(len(configs), 2)
        for conf in configs:
            self.assertNotIn("retry_tiers_ms", conf)
            self.assertNotIn("retry.tiers.ms", conf)
            self.assertEqual(conf["linger.ms"], 5)


class _TierMessage(OffsetMessage):
    """A record on a tier topic with a broker timestamp."""

    def __init__(self, value, headers, offset=0, topic="orders.retry.1", key=None, ts_ms=0, ts_type=1):
        super().__init__(value, headers, offset, topic=topic, key=key)
        self._ts = (ts_type, ts_ms)

    def timestamp(self):
        return self._ts


class _Clock:
    def __init__(self, seconds):
        self.now = seconds

    def __call__(self):
        return self.now


class _ReinjectConsumer(FakeConsumer):
    def __init__(self, messages=()):
        super().__init__(messages)
        self.subscribed = []
        self.paused = []
        self.resumed = []
        self.seeks = []

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def pause(self, partitions):
        self.paused.extend(partitions)

    def resume(self, partitions):
        self.resumed.extend(partitions)

    def seek(self, tp):
        self.seeks.append(tp)


def _reinjector(consumer, producer, clock):
    return RetryReinjector(
        work_topics=["orders"], retry_tiers_ms=TIERS, consumer=consumer, producer=producer, clock=clock
    )


class RetryReinjectorTest(unittest.TestCase):
    RAW = '{"job":"urn:babel:orders:created","data":{"order_id":1042},"attempts":1,"zzz":{}}'

    def _record(self, ts_ms, key=b"\x00k\xff"):
        headers = [
            ("bq-job", b"urn:babel:orders:created"),
            ("bq-attempts", b"1"),
            ("bq-delay", b"5000"),
            ("bq-original-topic", b"orders"),
            ("traceparent", b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        ]
        return _TierMessage(self.RAW.encode("utf-8"), headers, key=key, ts_ms=ts_ms)

    def test_subscribes_to_every_tier_topic(self):
        consumer, producer = _ReinjectConsumer(), FakeProducer()
        reinjector = _reinjector(consumer, producer, _Clock(100))
        reinjector.run_once(0)
        self.assertEqual(consumer.subscribed, [retry_topic("orders", n) for n in (1, 2, 3, 4)])

    def test_a_due_record_moves_to_the_work_topic_unchanged(self):
        # due: record timestamp 100 s + 5 s tier = 105 s; now = 106 s.
        record = self._record(100_000)
        consumer, producer = _ReinjectConsumer([record]), ReportingProducer(["ok"])
        self.assertEqual(_reinjector(consumer, producer, _Clock(106)).run_once(0), 1)

        (out,) = producer.produced
        self.assertEqual(out["topic"], "orders")
        self.assertEqual(out["value"], self.RAW.encode("utf-8"))
        self.assertEqual(out["key"], b"\x00k\xff")
        headers = header_map(out["headers"])
        self.assertNotIn("bq-delay", headers)
        self.assertNotIn("bq-original-topic", headers)
        self.assertEqual(headers["traceparent"], "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        self.assertEqual(headers["bq-job"], "urn:babel:orders:created")
        self.assertEqual(headers["bq-attempts"], "1")  # attempts is not touched
        self.assertEqual(consumer.committed, [record])

    def test_a_record_that_is_not_due_waits_with_its_partition_paused_and_is_not_committed(self):
        record = self._record(100_000)
        consumer, producer = _ReinjectConsumer([record]), ReportingProducer(["ok"])
        clock = _Clock(101)
        reinjector = _reinjector(consumer, producer, clock)

        self.assertEqual(reinjector.run_once(0), 0)
        self.assertEqual(producer.produced, [])
        self.assertEqual(consumer.committed, [])
        self.assertEqual(len(consumer.paused), 1)

        clock.now = 105.0
        self.assertEqual(reinjector.run_once(0), 1)
        self.assertEqual(consumer.committed, [record])
        self.assertEqual(len(consumer.resumed), 1)

    def test_a_producer_clock_ahead_is_clamped_to_one_tier(self):
        # The record claims to be written an hour from now: it still waits one tier, not an hour.
        now = 1_000.0
        record = self._record(int(now * 1000) + 3_600_000)
        consumer, producer = _ReinjectConsumer([record]), ReportingProducer(["ok"])
        clock = _Clock(now)
        reinjector = _reinjector(consumer, producer, clock)
        self.assertEqual(reinjector.run_once(0), 0)
        clock.now = now + 5.0
        self.assertEqual(reinjector.run_once(0), 1)

    def test_a_producer_clock_behind_is_released_now_not_before(self):
        # Timestamp an hour in the past: already due, released immediately.
        record = self._record(0)
        consumer, producer = _ReinjectConsumer([record]), ReportingProducer(["ok"])
        self.assertEqual(_reinjector(consumer, producer, _Clock(3_600)).run_once(0), 1)

    def test_an_unconfirmed_write_is_not_committed_and_the_record_is_retried(self):
        record = self._record(0)
        consumer, producer = _ReinjectConsumer([record]), ReportingProducer(["error"])
        reinjector = _reinjector(consumer, producer, _Clock(3_600))
        self.assertEqual(reinjector.run_once(0), 0)
        self.assertEqual(consumer.committed, [])
        self.assertEqual(len(consumer.seeks), 1)

    def test_two_records_on_one_partition_are_both_released_in_order(self):
        # Before the fix the held record was a single slot per partition: the second record
        # replaced the first, which was then never written nor committed (and, with the partition
        # paused, never redelivered either) -- a lost retry.
        first = self._record(100_000, key=b"first")
        second = self._record(100_000, key=b"second")
        second._offset = 1
        first._offset = 0
        consumer, producer = _ReinjectConsumer([first, second]), ReportingProducer(["ok", "ok"])
        clock = _Clock(101)
        reinjector = _reinjector(consumer, producer, clock)

        self.assertEqual(reinjector.run_once(0), 0)  # accepts first
        self.assertEqual(reinjector.run_once(0), 0)  # accepts second; first must still be held
        self.assertEqual(len(reinjector._waiting[("orders.retry.1", 0)]), 2)

        clock.now = 106.0
        self.assertEqual(reinjector.run_once(0), 2)
        self.assertEqual([r["key"] for r in producer.produced], [b"first", b"second"])
        self.assertEqual(consumer.committed, [first, second])
        self.assertEqual(reinjector._waiting, {})
        self.assertEqual(len(consumer.resumed), 1)  # the partition is resumed once, when empty

    def test_a_later_record_never_overtakes_an_earlier_one_that_is_not_due(self):
        early = self._record(100_000, key=b"early")  # due at 105 s
        late = self._record(102_000, key=b"late")  # due at 107 s (not clamped: written before `now`)
        late._offset = 1
        consumer, producer = _ReinjectConsumer([early, late]), ReportingProducer(["ok", "ok"])
        clock = _Clock(101)
        reinjector = _reinjector(consumer, producer, clock)
        reinjector.run_once(0)
        clock.now = 103.0
        reinjector.run_once(0)
        clock.now = 106.0
        self.assertEqual(reinjector.run_once(0), 1)  # only the head is due
        self.assertEqual([r["key"] for r in producer.produced], [b"early"])
        self.assertEqual(consumer.committed, [early])
        self.assertEqual(consumer.resumed, [])  # still holding `late`, partition stays paused
        clock.now = 107.0
        self.assertEqual(reinjector.run_once(0), 1)
        self.assertEqual([r["key"] for r in producer.produced], [b"early", b"late"])

    def test_a_failed_write_rewinds_to_the_failed_record_and_drops_the_held_run(self):
        first = self._record(100_000, key=b"first")
        second = self._record(100_000, key=b"second")
        first._offset, second._offset = 5, 6
        consumer, producer = _ReinjectConsumer([first, second]), ReportingProducer(["error"])
        clock = _Clock(101)
        reinjector = _reinjector(consumer, producer, clock)
        reinjector.run_once(0)
        reinjector.run_once(0)
        clock.now = 106.0
        self.assertEqual(reinjector.run_once(0), 0)
        self.assertEqual(consumer.committed, [])  # nothing past the failure is committed
        self.assertEqual(len(producer.produced), 1)  # `second` was not attempted ahead of `first`
        self.assertEqual([tp.offset for tp in consumer.seeks], [5])  # back to the failed record
        self.assertEqual(reinjector._waiting, {})  # the seek redelivers both

    def test_a_null_key_stays_null(self):
        record = self._record(0, key=None)
        consumer, producer = _ReinjectConsumer([record]), ReportingProducer(["ok"])
        _reinjector(consumer, producer, _Clock(3_600)).run_once(0)
        self.assertIsNone(producer.produced[0]["key"])

    def test_construction_needs_tiers_and_topics(self):
        with self.assertRaises(BabelQueueError):
            RetryReinjector(work_topics=["orders"], retry_tiers_ms=[], consumer=FakeConsumer(), producer=FakeProducer())
        with self.assertRaises(BabelQueueError):
            RetryReinjector(work_topics=[], retry_tiers_ms=TIERS, consumer=FakeConsumer(), producer=FakeProducer())


class HelpersTest(unittest.TestCase):
    def test_reinject_headers_drops_only_the_two_tier_headers(self):
        headers = [("bq-delay", b"1"), (b"bq-original-topic", b"t"), ("traceparent", b"x"), ("bq-job", b"j")]
        self.assertEqual(reinject_headers(headers), [("traceparent", b"x"), ("bq-job", b"j")])

    def test_target_time_clamp(self):
        self.assertEqual(target_time(1_000, 5_000, 2_000), 6_000)
        self.assertEqual(target_time(10**9, 5_000, 2_000), 7_000)
        self.assertEqual(target_time(-4_000, 5_000, 2_000), 2_000)
        self.assertEqual(target_time(None, 5_000, 2_000), 7_000)

    def test_default_tiers_constant(self):
        self.assertEqual(DEFAULT_RETRY_TIERS_MS, TIERS)
        self.assertTrue(issubclass(KafkaDeliveryError, BabelQueueError))


if __name__ == "__main__":
    unittest.main()
