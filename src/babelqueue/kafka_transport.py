"""Apache Kafka transport. Requires the ``kafka`` extra:

    pip install "babelqueue[kafka]"

Producing writes the canonical envelope as the record value and projects the contract
envelope fields onto native Kafka record headers (UTF-8 byte strings): ``bq-job`` = URN,
``bq-trace-id`` = trace_id, ``bq-message-id`` = meta.id, plus ``bq-schema-version`` /
``bq-source-lang`` / ``bq-attempts`` — so a Java/.NET/... peer can route on ``bq-job``
without parsing the body — with the record timestamp mirroring ``meta.created_at``.
Consuming is process-then-commit: ``poll`` reserves a record (``enable.auto.commit=false``)
and ``ack`` commits the offset only after the handler returns (at-least-once). A record that is
neither acked nor rewound (its settlement raised) is re-sought by the next ``pop``, so a
restarted consume loop redelivers it instead of committing past it. Kafka has no
native delivery count, so the ``bq-attempts`` header is the authoritative retry counter (the
body's ``attempts`` is the fallback for non-BabelQueue producers); the runtime owns retry by
republishing with attempts+1 and dead-letters to ``<queue>.dlq``.

This implements §6 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); Apache Kafka is purely additive.

URL form: ``kafka://host:9092[,host2:9092]``. The default consumer group is ``babelqueue``.
For a custom client, build the transport directly and pass it via ``BabelQueue(transport=...)``
or ``KafkaTransport(producer=..., consumer_factory=...)``.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

from .codec import SCHEMA_VERSION, EnvelopeCodec, parse_envelope
from .transport import ReceivedMessage, Transport, truncate_declared_version

_logger = logging.getLogger("babelqueue.kafka")

#: A blank ``bq-schema-version``: empty or only ASCII whitespace (space, \t, \n, \v, \f, \r) — the
#: same set in every SDK (GR-5). Deliberately not ``str.strip()``, which also strips U+001C-U+001F,
#: U+0085, U+00A0, U+2028, U+3000 ...
_BLANK_ASCII_PATTERN = re.compile(r"[ \t\n\x0b\x0c\r]*")
#: Prefix of the reserved ``route_urn`` a version-gated message is delivered under. It is not a
#: ``urn:babel:`` URN, so no handler can legitimately own it; the runtime dead-letters it
#: directly (never retried, never decoded; the DLQ record is the incoming raw body). The
#: rejected ``bq-schema-version`` is named in the logged error.
UNSUPPORTED_SCHEMA_ROUTE_PREFIX = "babelqueue:unsupported-schema-version:"

Headers = List[Tuple[str, bytes]]


def _brokers_from_url(url: str) -> str:
    rest = url.split("://", 1)[1] if "://" in url else url
    return rest.split("/", 1)[0] or "localhost:9092"


class KafkaTransport(Transport):
    def __init__(
        self,
        url: str = "kafka://localhost:9092",
        *,
        producer: Any = None,
        consumer_factory: Optional[Callable[[str], Any]] = None,
        group_id: str = "babelqueue",
        **client_config: Any,
    ) -> None:
        self._brokers = _brokers_from_url(url or "kafka://localhost:9092")
        self._group_id = group_id
        self._client_config = client_config
        self._producer = producer
        self._consumer_factory = consumer_factory
        self._consumers: Dict[str, Any] = {}
        # Per queue: the record handed out by ``pop`` that has been neither acked nor rewound yet.
        self._pending: Dict[str, Any] = {}

    # -- helpers ------------------------------------------------------------

    def _producer_(self) -> Any:
        if self._producer is None:
            self._producer = self._build_producer()  # pragma: no cover - needs Kafka / network
        return self._producer

    def _build_producer(self) -> Any:  # pragma: no cover - needs Kafka / network
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": self._brokers, **self._client_config})

    def _consumer(self, queue: str) -> Any:
        consumer = self._consumers.get(queue)
        if consumer is None:
            if self._consumer_factory is not None:
                consumer = self._consumer_factory(queue)
            else:
                consumer = self._build_consumer(queue)  # pragma: no cover - needs Kafka / network
            self._consumers[queue] = consumer
        return consumer

    def _build_consumer(self, queue: str) -> Any:  # pragma: no cover - needs Kafka / network
        from confluent_kafka import Consumer

        consumer = Consumer(
            {
                "bootstrap.servers": self._brokers,
                "group.id": self._group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                **self._client_config,
            }
        )
        consumer.subscribe([queue])
        return consumer

    @staticmethod
    def _projection(body: str) -> Headers:
        """Native Kafka record headers (UTF-8 byte values) — a redundant, routable view of the
        body: bq-job/bq-trace-id/bq-message-id + bq-schema-version/lang/attempts. §6.3."""
        env = parse_envelope(body)
        if not env:
            return []
        meta = env.get("meta") or {}

        headers: Headers = []

        def add(key: str, value: Any) -> None:
            if value is not None and value != "":
                headers.append((key, str(value).encode("utf-8")))

        add("bq-job", env.get("job"))
        add("bq-trace-id", env.get("trace_id"))
        add("bq-message-id", meta.get("id"))
        if meta.get("schema_version") is not None:
            headers.append(("bq-schema-version", str(meta["schema_version"]).encode("utf-8")))
        add("bq-source-lang", meta.get("lang"))
        headers.append(("bq-attempts", str(int(env.get("attempts", 0) or 0)).encode("utf-8")))
        return headers

    @staticmethod
    def _reconcile(body: str, headers: Any) -> str:
        """Set attempts to the authoritative bq-attempts header (falling back to the body's own
        attempts when the header is absent/unparseable — a non-BabelQueue producer). §6.5."""
        env = EnvelopeCodec.decode(body)
        if not env:
            return body
        attempts = int(env.get("attempts", 0) or 0)
        for key, value in headers or []:
            if key == "bq-attempts":
                raw = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)
                try:
                    attempts = int(raw)
                except (ValueError, TypeError):
                    pass
                break
        if attempts == int(env.get("attempts", 0) or 0):
            return body
        env["attempts"] = attempts
        return EnvelopeCodec.encode(env)

    @staticmethod
    def _unsupported_schema_version(headers: Any) -> Optional[str]:
        """The §6.7 step-2 gate on the ``bq-schema-version`` header: the offending raw value, or
        ``None``.

        ``headers`` is confluent-kafka's ``[(key, value_bytes)]`` list (or ``None``); the first
        ``bq-schema-version`` entry wins and its value is decoded as UTF-8. A missing header, a
        null value or a blank value (the empty string or only ASCII whitespace — the same set in
        every SDK, GR-5, not ``str.strip()``) is not gated: the usual decode path runs. A present
        value is accepted only when it is exactly the core's supported ``SCHEMA_VERSION`` as a
        plain decimal string, so ``"2"``, ``"x"``, ``"01"`` and ``" 1"`` are all rejected."""
        raw: Any = None
        for key, value in headers or []:
            name = key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key
            if name == "bq-schema-version":
                raw = value
                break
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8", "replace")
        raw = str(raw)
        if _BLANK_ASCII_PATTERN.fullmatch(raw) is not None or raw == str(SCHEMA_VERSION):
            return None
        return raw

    @staticmethod
    def _payload(message: Any) -> str:
        value = message.value()
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8")
        return str(value) if value is not None else ""

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        env = parse_envelope(body)
        meta = env.get("meta") or {}
        producer = self._producer_()
        kwargs: Dict[str, Any] = {"value": body.encode("utf-8"), "headers": self._projection(body)}
        created_at = meta.get("created_at")
        if created_at:
            kwargs["timestamp"] = int(created_at)
        producer.produce(queue, **kwargs)
        producer.poll(0)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        wait = timeout if timeout and timeout > 0 else 1.0
        self._rewind_abandoned(queue)
        message = self._consumer(queue).poll(wait)
        if message is None or message.error() is not None:
            return None
        # §6.7 step 2: gate on the ``bq-schema-version`` header BEFORE any body decode (the attempts
        # reconcile below decodes the body). A rejected record keeps its raw payload and the
        # reserved route; the runtime then dead-letters it directly (no retry; §6.5).
        headers = message.headers()
        unsupported = truncate_declared_version(
            self._unsupported_schema_version(headers)
        )
        if unsupported is not None:
            _logger.warning(
                "Kafka record on %r carries unsupported bq-schema-version %r (supported: %s); "
                "not decoding it.",
                queue,
                unsupported,
                SCHEMA_VERSION,
            )
            received = ReceivedMessage(
                body=self._payload(message),
                queue=queue,
                handle=message,
                route_urn=UNSUPPORTED_SCHEMA_ROUTE_PREFIX + quote(unsupported, safe=""),
            )
            self._pending[queue] = message
            return received
        body = self._reconcile(self._payload(message), headers)
        received = ReceivedMessage(body=body, queue=queue, handle=message)
        # Track the record as in flight only once ``pop`` has built it. A decode error above
        # escapes before this point and must not leave the record pending: the next ``pop`` would
        # seek back to it and fail identically, wedging the partition forever.
        self._pending[queue] = message
        return received

    def ack(self, message: ReceivedMessage) -> None:
        if message.handle is None:
            return
        # Settled: from here on the record is no longer "in flight", whatever the commit does.
        self._pending.pop(message.queue, None)
        self._consumer(message.queue).commit(message=message.handle, asynchronous=False)

    def rewind(self, message: ReceivedMessage) -> None:
        """Seek the partition back to ``message``'s offset so the next ``pop`` returns it again
        (:class:`~babelqueue.transport.Rewinder`). Kafka's commit is a cumulative per-partition
        position, so merely not acking a record would lose it once a later record is committed."""
        handle = message.handle
        if handle is None:
            return
        self._seek_back(message.queue, handle)
        if self._pending.get(message.queue) is handle:
            del self._pending[message.queue]

    def _rewind_abandoned(self, queue: str) -> None:
        """Re-seek a record that ``pop`` handed out but that was never acked or rewound.

        That happens when the runtime's settlement raised (a failed retry / DLQ / release
        ``publish``): ``poll`` has already advanced the in-memory position, so without this the
        next ``pop`` (a restarted consume loop) would return the *following* record, and acking it
        would commit past the unsettled one — losing it. Seeking back makes it redeliver instead.
        """
        handle = self._pending.get(queue)
        if handle is None:
            return
        try:
            self._seek_back(queue, handle)
        except Exception:  # noqa: BLE001 - already logged, naming the record
            # A seek can only fail here for a partition that is no longer assigned (rebalance); the
            # group then resets the position to the committed offset, which is that record. Do not
            # wedge the consumer on it.
            pass
        self._pending.pop(queue, None)

    def _seek_back(self, queue: str, handle: Any) -> None:
        try:
            from confluent_kafka import TopicPartition
        except ImportError:  # only reachable with an injected consumer
            from collections import namedtuple

            TopicPartition = namedtuple("TopicPartition", "topic partition offset")  # type: ignore[misc]
        topic, partition, offset = handle.topic(), handle.partition(), handle.offset()
        try:
            self._consumer(queue).seek(TopicPartition(topic, partition, offset))
        except Exception:
            # Name the record so an operator can recover it by hand: if the seek fails the
            # commit of a later record on this partition may skip it.
            _logger.error(
                "Kafka seek back to %s[%s]@%s failed; the unsettled record may be skipped by a "
                "later commit on this partition.",
                topic,
                partition,
                offset,
            )
            raise

    def close(self) -> None:  # pragma: no cover - resource cleanup
        try:
            if self._producer is not None:
                self._producer.flush()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        for consumer in self._consumers.values():
            try:
                consumer.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
