"""Kafka retry-topic re-inject consumer (broker-bindings.md §6.4 / §6.5, ADR-0034). Requires the
``kafka`` extra:

    pip install "babelqueue[kafka]"

:class:`~babelqueue.kafka_transport.KafkaTransport` parks a delayed or retried message on a tier
topic ``<work_topic>.retry.<n>``. Nothing moves it back by itself: this consumer reads each tier
topic and, once the tier's delay has elapsed, writes the record to the work topic named by its
``bq-original-topic`` header. Run one next to the workers (``RetryReinjector(...).run()``); without
it a parked message is never delivered.

What it does to a record, and what it deliberately does not:

* The value and the key are copied **byte for byte**. The value is never decoded (an undecodable
  record travels on and is dead-lettered by the work-topic consumer as poison) and ``attempts`` is
  not touched (the republish already advanced it).
* The ``bq-delay`` and ``bq-original-topic`` headers are dropped; every other header (``bq-job``,
  ``traceparent`` ...) is kept.
* The target time is ``record timestamp + tier delay``, clamped to ``[now, now + tier delay]`` so a
  producer clock running ahead cannot park a record for longer than one tier, and one running
  behind cannot release it before it is due (A6). Brokers SHOULD be configured with
  ``message.timestamp.type=LogAppendTime`` on the tier topics so the timestamp is the broker's.
* Waiting never blocks the poll loop: a partition holding a not-yet-due record is paused and
  resumed when the record is due.
* The consumer offset is committed only after the broker confirmed the write to the work topic
  (at-least-once: a crash in between re-injects the record, a duplicate the handler's idempotency
  absorbs).
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Tuple

from .exceptions import BabelQueueError
from .kafka_transport import (
    DEFAULT_DELIVERY_TIMEOUT,
    DELAY_HEADER,
    ORIGINAL_TOPIC_HEADER,
    KafkaDeliveryError,
    _brokers_from_url,
    _validated_tiers,
    retry_topic,
)

_logger = logging.getLogger("babelqueue.kafka.retry")

_RETRY_TOPIC = re.compile(r"^(?P<work>.+)\.retry\.(?P<n>[0-9]+)$")


def reinject_headers(headers: Any) -> List[Tuple[str, Any]]:
    """``headers`` without ``bq-delay`` / ``bq-original-topic``, everything else kept as is."""
    kept: List[Tuple[str, Any]] = []
    for key, value in headers or []:
        name = key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key
        if name in (DELAY_HEADER, ORIGINAL_TOPIC_HEADER):
            continue
        kept.append((key, value))
    return kept


def target_time(record_ms: Optional[int], tier_ms: int, now_ms: float) -> float:
    """When (epoch milliseconds) a record is due: ``record_ms + tier_ms`` clamped to
    ``[now_ms, now_ms + tier_ms]``. A record with no usable timestamp (``None``) is due one full
    tier from now."""
    if record_ms is None:
        return now_ms + tier_ms
    return min(max(record_ms + tier_ms, now_ms), now_ms + tier_ms)


class RetryReinjector:
    """Moves records from ``<work_topic>.retry.<n>`` back to their work topic once due.

    ``work_topics`` are the work topics whose tier topics are consumed; ``retry_tiers_ms`` must be
    the same list the producing :class:`~babelqueue.kafka_transport.KafkaTransport` uses (tier ``n``
    is ``retry_tiers_ms[n-1]``). ``consumer`` / ``producer`` are injectable (confluent-kafka
    shaped); ``clock`` returns epoch seconds and ``sleep`` blocks for seconds, for tests."""

    def __init__(
        self,
        url: str = "kafka://localhost:9092",
        *,
        work_topics: Sequence[str],
        retry_tiers_ms: Sequence[int],
        group_id: str = "babelqueue-retry-reinject",
        consumer: Any = None,
        producer: Any = None,
        delivery_timeout: float = DEFAULT_DELIVERY_TIMEOUT,
        poll_interval: float = 1.0,
        clock: Callable[[], float] = time.time,
        **client_config: Any,
    ) -> None:
        self._tiers = _validated_tiers(retry_tiers_ms)
        if not self._tiers:
            raise BabelQueueError("RetryReinjector needs retry_tiers_ms.")
        if not work_topics:
            raise BabelQueueError("RetryReinjector needs at least one work topic.")
        self._topics = [retry_topic(topic, n) for topic in work_topics for n in range(1, len(self._tiers) + 1)]
        self._brokers = _brokers_from_url(url or "kafka://localhost:9092")
        self._group_id = group_id
        self._client_config = client_config
        self._consumer = consumer
        self._producer = producer
        self._delivery_timeout = delivery_timeout
        self._poll_interval = poll_interval
        self._clock = clock
        # (topic, partition) -> the records that partition is holding, oldest first, each with its
        # due_ms. Normally one (the partition is paused); when a pause fails the poll keeps
        # delivering, and every record is kept, in order, instead of overwriting the previous one.
        self._waiting: Dict[Tuple[str, int], Deque[Tuple[Any, float]]] = {}
        self._subscribed = False
        self._stop = threading.Event()

    @property
    def topics(self) -> List[str]:
        """The tier topics this consumer reads."""
        return list(self._topics)

    # -- clients ------------------------------------------------------------

    def _consumer_(self) -> Any:
        if self._consumer is None:
            self._consumer = self._build_consumer()  # pragma: no cover - needs Kafka / network
        if not self._subscribed:
            self._consumer.subscribe(self._topics)
            self._subscribed = True
        return self._consumer

    def _build_consumer(self) -> Any:  # pragma: no cover - needs Kafka / network
        from confluent_kafka import Consumer

        return Consumer(
            {
                "bootstrap.servers": self._brokers,
                "group.id": self._group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                **self._client_config,
            }
        )

    def _producer_(self) -> Any:
        if self._producer is None:
            self._producer = self._build_producer()  # pragma: no cover - needs Kafka / network
        return self._producer

    def _build_producer(self) -> Any:  # pragma: no cover - needs Kafka / network
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": self._brokers, **self._client_config})

    @staticmethod
    def _partition(topic: str, partition: int, offset: Optional[int] = None) -> Any:
        try:
            from confluent_kafka import TopicPartition
        except ImportError:  # only reachable with injected clients
            from collections import namedtuple

            TopicPartition = namedtuple("TopicPartition", "topic partition offset")  # type: ignore[misc,no-redef]
            return TopicPartition(topic, partition, -1001 if offset is None else offset)
        return TopicPartition(topic, partition) if offset is None else TopicPartition(topic, partition, offset)

    # -- one step -----------------------------------------------------------

    def run_once(self, timeout: Optional[float] = None) -> int:
        """Poll once (waiting at most ``timeout`` seconds, never past the next due record) and
        re-inject every record that is due. Returns how many records were written."""
        consumer = self._consumer_()
        wait = self._poll_interval if timeout is None else timeout
        if self._waiting:
            nearest = min(held[0][1] for held in self._waiting.values())
            wait = max(0.0, min(wait, nearest / 1000.0 - self._clock()))
        message = consumer.poll(wait)
        if message is not None and message.error() is None:
            self._accept(message)
        return self._release_due()

    def _tier_ms(self, topic: str) -> Optional[int]:
        match = _RETRY_TOPIC.match(topic)
        if match is None:
            return None
        n = int(match.group("n"))
        if not 1 <= n <= len(self._tiers):
            return None
        return self._tiers[n - 1]

    def _accept(self, message: Any) -> None:
        """Hold ``message`` until it is due: pause its partition (the poll loop keeps running)."""
        topic = message.topic()
        tier_ms = self._tier_ms(topic)
        if tier_ms is None:
            # Not a configured tier (the subscription makes this unreachable); never loop on it.
            _logger.error("Record on %r is not on a configured retry tier; skipping it.", topic)
            self._commit(message)
            return
        _kind, record_ms = self._timestamp(message)
        due = target_time(record_ms, tier_ms, self._clock() * 1000.0)
        partition = (topic, message.partition())
        self._waiting.setdefault(partition, deque()).append((message, due))
        try:
            self._consumer_().pause([self._partition(topic, message.partition())])
        except Exception:  # noqa: BLE001 - the due time still gates the write
            _logger.warning("Could not pause %s[%s]; continuing without pausing.", topic, message.partition())

    @staticmethod
    def _timestamp(message: Any) -> Tuple[int, Optional[int]]:
        """``(timestamp_type, ms)`` of ``message``; ``ms`` is ``None`` when the broker gave none."""
        try:
            kind, value = message.timestamp()
        except Exception:  # noqa: BLE001 - a record without timestamp support
            return 0, None
        if kind == 0 or value is None or value < 0:  # TIMESTAMP_NOT_AVAILABLE
            return 0, None
        return kind, int(value)

    def _release_due(self) -> int:
        """Re-inject the due records of every held partition, oldest first.

        Only the head of a partition is gated by its due time: a record never overtakes the one
        before it. A failed write rewinds the partition to the failed record (the seek redelivers
        it and everything after it, so the whole held run is dropped from memory) and stops there.
        """
        written = 0
        now_ms = self._clock() * 1000.0
        for partition, held in list(self._waiting.items()):
            rewound = False
            while held and held[0][1] <= now_ms:
                message, _due = held[0]
                try:
                    self._reinject(message)
                except Exception:  # noqa: BLE001 - the record stays uncommitted and is retried
                    _logger.error(
                        "Re-injecting a record from %s[%s]@%s failed; it will be retried.",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        exc_info=True,
                    )
                    # Give the partition back at the failed record so the next poll returns it again.
                    self._rewind(message)
                    held.clear()
                    rewound = True
                    break
                self._commit(message)
                held.popleft()
                written += 1
            if not held:
                del self._waiting[partition]
                if not rewound:
                    try:
                        self._consumer_().resume([self._partition(*partition)])
                    except Exception:  # noqa: BLE001
                        _logger.warning("Could not resume %s[%s].", *partition)
        return written

    def _reinject(self, message: Any) -> None:
        topic = message.topic()
        target = self._header(message, ORIGINAL_TOPIC_HEADER)
        if not target:
            # The topic name is deterministic (<work_topic>.retry.<n>): never strand a record on
            # a missing header.
            match = _RETRY_TOPIC.match(topic)
            target = match.group("work") if match else topic
        kwargs: Dict[str, Any] = {"value": message.value(), "headers": reinject_headers(message.headers())}
        key_of = getattr(message, "key", None)
        key = key_of() if callable(key_of) else None
        if key is not None:
            kwargs["key"] = key
        outcome: Dict[str, Any] = {}

        def on_delivery(err: Any, _msg: Any = None) -> None:
            outcome["error"] = err

        producer = self._producer_()
        producer.produce(target, on_delivery=on_delivery, **kwargs)
        producer.flush(self._delivery_timeout)
        if "error" not in outcome:
            raise KafkaDeliveryError(
                "Kafka delivery to %r was not confirmed within %ss." % (target, self._delivery_timeout)
            )
        if outcome["error"] is not None:
            raise KafkaDeliveryError("Kafka delivery to %r failed: %s" % (target, outcome["error"]))

    @staticmethod
    def _header(message: Any, wanted: str) -> Optional[str]:
        for key, value in message.headers() or []:
            name = key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key
            if name == wanted:
                if isinstance(value, (bytes, bytearray)):
                    return bytes(value).decode("utf-8", "replace")
                return None if value is None else str(value)
        return None

    def _commit(self, message: Any) -> None:
        self._consumer_().commit(message=message, asynchronous=False)

    def _rewind(self, message: Any) -> None:
        consumer = self._consumer_()
        topic, partition = message.topic(), message.partition()
        try:
            consumer.seek(self._partition(topic, partition, message.offset()))
        finally:
            try:
                consumer.resume([self._partition(topic, partition)])
            except Exception:  # noqa: BLE001
                pass

    # -- loop ---------------------------------------------------------------

    def run(self, max_idle: Optional[float] = None) -> None:  # pragma: no cover - blocking loop
        """Re-inject until :meth:`stop` is called."""
        try:
            while not self._stop.is_set():
                self.run_once()
        finally:
            self.close()

    def stop(self) -> None:
        """Ask :meth:`run` to return after the current step."""
        self._stop.set()

    def close(self) -> None:  # pragma: no cover - resource cleanup
        try:
            if self._producer is not None:
                self._producer.flush()
        except Exception:  # noqa: BLE001
            pass
        try:
            if self._consumer is not None:
                self._consumer.close()
        except Exception:  # noqa: BLE001
            pass
