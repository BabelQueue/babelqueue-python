"""Apache ActiveMQ Artemis transport over AMQP 1.0. Requires the ``artemis`` extra:

    pip install "babelqueue[artemis]"

Artemis speaks AMQP 1.0 (not the 0-9-1 of RabbitMQ), so this transport uses the
``python-qpid-proton`` blocking client rather than ``pika``. Producing sends the canonical
envelope as the message body and projects the contract envelope fields onto the AMQP
properties a JMS peer reads: ``correlation-id`` = trace_id (JMSCorrelationID), ``creation-time``
= meta.created_at (JMSTimestamp), the ``x-opt-jms-type`` message annotation = URN (JMSType, the
AMQP-JMS mapping), plus the ``bq-`` application properties (``bq-schema-version`` /
``bq-source-lang`` / ``bq-attempts`` / ``bq-app-id``) — so a Java (JMS) or .NET/Node/... peer
routes and correlates without parsing the body. The URN in the body's ``job`` field stays
authoritative.

Consuming reserves one message at a time (``receive`` -> process -> ``accept``); the
authoritative attempt count is the envelope's ``attempts`` (the body), reconciled against the
broker's native AMQP ``delivery-count`` as ``attempts = max(body, delivery_count)`` — no -1,
because the AMQP header counter is 0-based (0 on first delivery), counting prior failed
deliveries. (The Java JMS binding reads ``JMSXDeliveryCount`` which is 1-based and subtracts 1,
arriving at the same 0-based ``attempts``.) A message left un-accepted is redelivered
(at-least-once).

This implements §7 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); Apache ActiveMQ Artemis is purely additive.

URL form: ``artemis://host:5672`` (or ``artemis+ssl://`` for TLS) — translated to the
``amqp://`` / ``amqps://`` proton speaks. For credentials or a custom connection, build the
transport directly and pass it via ``BabelQueue(transport=...)`` or
``ArtemisTransport(connection=...)``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .codec import EnvelopeCodec
from .transport import ReceivedMessage, Transport

JMS_TYPE_ANNOTATION = "x-opt-jms-type"
APP_ID = "babelqueue"


class ArtemisTransport(Transport):
    def __init__(
        self,
        url: str = "artemis://localhost:5672",
        *,
        connection: Any = None,
        credit: int = 1,
        receive_timeout_millis: int = 1000,
        **connect_options: Any,
    ) -> None:
        self._url = url or "artemis://localhost:5672"
        self._credit = credit
        self._receive_timeout_millis = receive_timeout_millis
        self._connect_options = connect_options
        self._senders: Dict[str, Any] = {}
        self._receivers: Dict[str, Any] = {}

        if connection is not None:
            self._connection = connection
            return
        self._connection = self._build_connection()  # pragma: no cover - needs Artemis / network

    def _build_connection(self) -> Any:  # pragma: no cover - needs Artemis / network
        try:
            from proton.utils import BlockingConnection
        except ImportError as exc:
            raise ImportError(
                "ArtemisTransport requires the 'python-qpid-proton' package. Install with "
                'pip install "babelqueue[artemis]".'
            ) from exc
        return BlockingConnection(self._to_amqp_url(self._url), **self._connect_options)

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _to_amqp_url(url: str) -> str:
        """``artemis://`` -> ``amqp://`` and ``artemis+ssl://`` -> ``amqps://`` (proton's schemes)."""
        if url.startswith("artemis+ssl://"):
            return "amqps://" + url[len("artemis+ssl://"):]
        if url.startswith("artemis://"):
            return "amqp://" + url[len("artemis://"):]
        return url

    def _sender(self, queue: str) -> Any:
        sender = self._senders.get(queue)
        if sender is None:
            sender = self._connection.create_sender(queue)
            self._senders[queue] = sender
        return sender

    def _receiver(self, queue: str) -> Any:
        receiver = self._receivers.get(queue)
        if receiver is None:
            receiver = self._connection.create_receiver(queue, credit=self._credit)
            self._receivers[queue] = receiver
        return receiver

    @staticmethod
    def _projection(body: str) -> Dict[str, str]:
        """AMQP application properties (string->string) — a redundant, routable view of the
        body: bq-schema-version/bq-source-lang/bq-attempts/bq-app-id. §7.2 (the URN is carried
        by the x-opt-jms-type annotation, trace_id by correlation-id)."""
        env = EnvelopeCodec.decode(body)
        if not env:
            return {}
        meta = env.get("meta") or {}

        props: Dict[str, str] = {}
        if meta.get("schema_version") is not None:
            props["bq-schema-version"] = str(meta["schema_version"])
        if meta.get("lang"):
            props["bq-source-lang"] = str(meta["lang"])
        props["bq-attempts"] = str(int(env.get("attempts", 0) or 0))
        props["bq-app-id"] = APP_ID
        return props

    @staticmethod
    def _jms_type(body: str) -> str:
        env = EnvelopeCodec.decode(body)
        return str(env["job"]) if env and env.get("job") else ""

    @staticmethod
    def _correlation_id(body: str) -> str:
        env = EnvelopeCodec.decode(body)
        return str(env["trace_id"]) if env and env.get("trace_id") else ""

    @staticmethod
    def _creation_seconds(body: str) -> Optional[float]:
        """proton's creation_time is float seconds; the contract's created_at is epoch ms."""
        env = EnvelopeCodec.decode(body)
        meta = (env or {}).get("meta") or {}
        created_at = meta.get("created_at")
        if created_at is None:
            return None
        try:
            return int(created_at) / 1000.0
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return None

    def _message(self, body: str) -> Any:
        """Build the proton Message projecting the §7 JMS-readable metadata."""
        from proton import Message, symbol

        message: Any = Message(body=body)
        props = self._projection(body)
        if props:
            message.properties = props
        correlation_id = self._correlation_id(body)
        if correlation_id:
            message.correlation_id = correlation_id
        creation = self._creation_seconds(body)
        if creation is not None:
            message.creation_time = creation
        jms_type = self._jms_type(body)
        if jms_type:
            message.annotations = {symbol(JMS_TYPE_ANNOTATION): jms_type}
        return message

    @staticmethod
    def _reconcile(body: str, delivery_count: Any) -> str:
        """Set attempts to max(current, delivery_count). The AMQP delivery-count header is
        0-based (0 on first delivery), so it maps directly to attempts with no -1. The runtime
        retries by republishing with attempts+1 in the body (delivery-count back to 0), so a
        republished message must not have its higher body count lowered."""
        try:
            dc = int(delivery_count)
        except (ValueError, TypeError):
            return body
        if dc <= 0:
            return body
        env = EnvelopeCodec.decode(body)
        if not env or dc <= int(env.get("attempts", 0) or 0):
            return body
        env["attempts"] = dc
        return EnvelopeCodec.encode(env)

    @staticmethod
    def _delivery_count(message: Any) -> int:
        value = getattr(message, "delivery_count", 0)
        try:
            return int(value)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return 0

    @staticmethod
    def _payload(message: Any) -> str:
        body = getattr(message, "body", None)
        if isinstance(body, bytes):
            return body.decode("utf-8")
        return str(body) if body is not None else ""

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        self._sender(queue).send(self._message(body))

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        wait = self._receive_timeout_millis / 1000.0
        if timeout and timeout > 0:
            wait = timeout
        receiver = self._receiver(queue)
        try:
            message = receiver.receive(timeout=wait)
        except Exception as exc:  # noqa: BLE001 - proton raises proton.Timeout on no message
            if type(exc).__name__ == "Timeout":
                return None
            raise
        if message is None:  # pragma: no cover - defensive (real client raises instead)
            return None
        body = self._reconcile(self._payload(message), self._delivery_count(message))
        return ReceivedMessage(body=body, queue=queue, handle=receiver)

    def ack(self, message: ReceivedMessage) -> None:
        if message.handle is None:
            return
        message.handle.accept()

    def close(self) -> None:  # pragma: no cover - resource cleanup
        try:
            self._connection.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
