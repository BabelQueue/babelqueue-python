"""Apache Pulsar transport. Requires the ``pulsar`` extra:

    pip install "babelqueue[pulsar]"

Producing sends the canonical envelope as the message payload and projects the contract
envelope fields onto native Pulsar message properties (string->string): ``bq-job`` = URN,
``bq-trace-id`` = trace_id, ``bq-message-id`` = meta.id, plus ``bq-schema-version`` /
``bq-source-lang`` / ``bq-attempts`` — so a Java/.NET/... peer can route on ``bq-job``
without parsing the body. Consuming receives one message at a time
(``receive`` -> process -> ``acknowledge``); the authoritative attempt count is the
envelope's ``bq-attempts`` (the body), cross-checked against the broker's native
``redelivery_count`` (0-based) and reconciled onto the envelope as
``attempts = max(body, redelivery_count)`` — no -1, because Pulsar's redelivery count is
0-based (0 on first delivery). A message left un-acked is redelivered (at-least-once).

This implements §5 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); Apache Pulsar is purely additive.

URL form: ``pulsar://host:6650`` (or ``pulsar+ssl://``). The default subscription name is
``babelqueue`` over a ``Shared`` subscription. For a custom client/subscription, build the
transport directly and pass it via ``BabelQueue(transport=...)`` or
``PulsarTransport(client=...)``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .codec import EnvelopeCodec
from .transport import ReceivedMessage, Transport


class PulsarTransport(Transport):
    def __init__(
        self,
        url: str = "pulsar://localhost:6650",
        *,
        client: Any = None,
        subscription: str = "babelqueue",
        consumer_type: Any = None,
        topic_prefix: str = "",
        receive_timeout_millis: int = 1000,
        **subscribe_options: Any,
    ) -> None:
        self._url = url or "pulsar://localhost:6650"
        self._subscription = subscription
        self._consumer_type = consumer_type
        self._topic_prefix = topic_prefix
        self._receive_timeout_millis = receive_timeout_millis
        self._subscribe_options = subscribe_options
        self._producers: Dict[str, Any] = {}
        self._consumers: Dict[str, Any] = {}

        if client is not None:
            self._client = client
            return
        self._client = self._build_client()  # pragma: no cover - needs Pulsar / network

    def _build_client(self) -> Any:  # pragma: no cover - needs Pulsar / network
        try:
            import pulsar
        except ImportError as exc:
            raise ImportError(
                "PulsarTransport requires the 'pulsar-client' package. Install with "
                'pip install "babelqueue[pulsar]".'
            ) from exc
        return pulsar.Client(self._url)

    # -- helpers ------------------------------------------------------------

    def _topic(self, queue: str) -> str:
        return f"{self._topic_prefix}{queue}" if self._topic_prefix else queue

    def _producer(self, queue: str) -> Any:
        producer = self._producers.get(queue)
        if producer is None:
            producer = self._client.create_producer(self._topic(queue))
            self._producers[queue] = producer
        return producer

    def _consumer(self, queue: str) -> Any:
        consumer = self._consumers.get(queue)
        if consumer is None:
            consumer = self._client.subscribe(
                self._topic(queue), self._subscription, **self._subscribe_kwargs()
            )
            self._consumers[queue] = consumer
        return consumer

    def _subscribe_kwargs(self) -> Dict[str, Any]:
        kwargs = dict(self._subscribe_options)
        consumer_type = self._consumer_type
        if consumer_type is None:
            consumer_type = self._default_shared_type()
        if consumer_type is not None:
            kwargs.setdefault("consumer_type", consumer_type)
        return kwargs

    @staticmethod
    def _default_shared_type() -> Any:
        try:
            import pulsar
        except ImportError:  # pragma: no cover - exercised only with pulsar absent
            return None
        return pulsar.ConsumerType.Shared  # pragma: no cover - needs Pulsar

    @staticmethod
    def _projection(body: str) -> Dict[str, str]:
        """Native Pulsar message properties (string->string) — a redundant, routable view of
        the body: bq-job/bq-trace-id/bq-message-id + bq-schema-version/lang/attempts. §5.2."""
        env = EnvelopeCodec.decode(body)
        if not env:
            return {}
        meta = env.get("meta") or {}

        props: Dict[str, str] = {}
        if env.get("job"):
            props["bq-job"] = str(env["job"])
        if env.get("trace_id"):
            props["bq-trace-id"] = str(env["trace_id"])
        if meta.get("id"):
            props["bq-message-id"] = str(meta["id"])
        if meta.get("schema_version") is not None:
            props["bq-schema-version"] = str(meta["schema_version"])
        if meta.get("lang"):
            props["bq-source-lang"] = str(meta["lang"])
        props["bq-attempts"] = str(int(env.get("attempts", 0) or 0))
        return props

    @staticmethod
    def _reconcile(body: str, redelivery_count: Any) -> str:
        """Set attempts to max(current, redelivery_count). Pulsar's redelivery count is
        0-based (0 on first delivery), so it maps directly to attempts with no -1. The
        runtime retries by republishing with attempts+1 in the body (redelivery count back to
        0), so a republished message must not have its higher body count lowered."""
        try:
            rc = int(redelivery_count)
        except (ValueError, TypeError):
            return body
        if rc <= 0:
            return body
        env = EnvelopeCodec.decode(body)
        if not env or rc <= int(env.get("attempts", 0) or 0):
            return body
        env["attempts"] = rc
        return EnvelopeCodec.encode(env)

    @staticmethod
    def _redelivery_count(message: Any) -> int:
        getter = getattr(message, "redelivery_count", None)
        value = getter() if callable(getter) else getter
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _payload(message: Any) -> str:
        getter = getattr(message, "data", None)
        data = getter() if callable(getter) else getter
        if isinstance(data, bytes):
            return data.decode("utf-8")
        return str(data) if data is not None else ""

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        self._producer(queue).send(body.encode("utf-8"), properties=self._projection(body))

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        wait = self._receive_timeout_millis
        if timeout and timeout > 0:
            wait = int(timeout * 1000)
        consumer = self._consumer(queue)
        try:
            message = consumer.receive(timeout_millis=wait)
        except Exception as exc:  # noqa: BLE001 - Pulsar raises pulsar.Timeout on no message
            if type(exc).__name__ == "Timeout":
                return None
            raise
        if message is None:  # pragma: no cover - defensive (real client raises instead)
            return None
        body = self._reconcile(self._payload(message), self._redelivery_count(message))
        return ReceivedMessage(body=body, queue=queue, handle=message)

    def ack(self, message: ReceivedMessage) -> None:
        if message.handle is None:
            return
        self._consumer(message.queue).acknowledge(message.handle)

    def close(self) -> None:  # pragma: no cover - resource cleanup
        for resource in (*self._producers.values(), *self._consumers.values()):
            try:
                resource.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
