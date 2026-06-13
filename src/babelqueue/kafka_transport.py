"""Apache Kafka transport. Requires the ``kafka`` extra:

    pip install "babelqueue[kafka]"

Producing writes the canonical envelope as the record value and projects the contract
envelope fields onto native Kafka record headers (UTF-8 byte strings): ``bq-job`` = URN,
``bq-trace-id`` = trace_id, ``bq-message-id`` = meta.id, plus ``bq-schema-version`` /
``bq-source-lang`` / ``bq-attempts`` — so a Java/.NET/... peer can route on ``bq-job``
without parsing the body — with the record timestamp mirroring ``meta.created_at``.
Consuming is process-then-commit: ``poll`` reserves a record (``enable.auto.commit=false``)
and ``ack`` commits the offset only after the handler returns (at-least-once). Kafka has no
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

from typing import Any, Callable, Dict, List, Optional, Tuple

from .codec import EnvelopeCodec
from .transport import ReceivedMessage, Transport

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
        env = EnvelopeCodec.decode(body)
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
    def _payload(message: Any) -> str:
        value = message.value()
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8")
        return str(value) if value is not None else ""

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        env = EnvelopeCodec.decode(body)
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
        message = self._consumer(queue).poll(wait)
        if message is None or message.error() is not None:
            return None
        body = self._reconcile(self._payload(message), message.headers())
        return ReceivedMessage(body=body, queue=queue, handle=message)

    def ack(self, message: ReceivedMessage) -> None:
        if message.handle is None:
            return
        self._consumer(message.queue).commit(message=message.handle, asynchronous=False)

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
