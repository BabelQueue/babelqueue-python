"""Azure Service Bus transport. Requires the ``azureservicebus`` extra:

    pip install "babelqueue[azureservicebus]"

Producing sends the canonical envelope as the message body and projects the contract
envelope fields onto native Service Bus fields — ``Subject`` = URN, ``CorrelationId`` =
trace_id, ``MessageId`` = meta.id, plus the ``bq-`` application properties
(``bq-schema-version`` / ``bq-source-lang`` / ``bq-created-at``) — so a .NET/Java/... peer
can route on ``Subject`` and correlate on ``CorrelationId`` without parsing the body.
Consuming uses the PeekLock reservation model (``receive_messages`` -> process ->
``complete_message``); the authoritative attempt count is the broker's native
``DeliveryCount`` (1-based), reconciled onto the envelope as ``attempts = DeliveryCount - 1``.
A message left un-acked has its lock expire and is redelivered (at-least-once).

This implements §4 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); Azure Service Bus is purely additive.

URL form: ``sb://<namespace>.servicebus.windows.net`` (Azure AD via
``DefaultAzureCredential``). For connection-string auth or a custom client, build the
transport directly and pass it via ``BabelQueue(transport=...)`` or
``AsbTransport(connection_string=...)``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from .codec import EnvelopeCodec
from .transport import ReceivedMessage, Transport


class AsbTransport(Transport):
    def __init__(
        self,
        url: str = "sb://",
        *,
        client: Any = None,
        connection_string: Optional[str] = None,
        credential: Any = None,
        max_wait_time: Optional[float] = None,
    ) -> None:
        parts = urlsplit(url) if url else urlsplit("sb://")
        self._namespace = parts.hostname or None
        self._connection_string = connection_string
        self._credential = credential
        self._max_wait_time = max_wait_time
        self._senders: Dict[str, Any] = {}
        self._receivers: Dict[str, Any] = {}

        if client is not None:
            self._client = client
            return
        self._client = self._build_client()  # pragma: no cover - needs Azure / network

    def _build_client(self) -> Any:  # pragma: no cover - needs Azure / network
        try:
            from azure.servicebus import ServiceBusClient
        except ImportError as exc:
            raise ImportError(
                "AsbTransport requires the 'azure-servicebus' package. Install with "
                'pip install "babelqueue[azureservicebus]".'
            ) from exc
        import os

        cs = self._connection_string or os.environ.get("AZURE_SERVICEBUS_CONNECTION_STRING")
        if cs:
            return ServiceBusClient.from_connection_string(cs)
        if self._namespace:
            credential = self._credential
            if credential is None:
                from azure.identity import DefaultAzureCredential

                credential = DefaultAzureCredential()
            return ServiceBusClient(self._namespace, credential)
        raise ValueError(
            "AsbTransport needs a connection string, a namespace + credential, or an injected client."
        )

    # -- helpers ------------------------------------------------------------

    def _sender(self, queue: str) -> Any:
        sender = self._senders.get(queue)
        if sender is None:
            sender = self._client.get_queue_sender(queue)
            self._senders[queue] = sender
        return sender

    def _receiver(self, queue: str) -> Any:
        receiver = self._receivers.get(queue)
        if receiver is None:
            receiver = self._client.get_queue_receiver(queue)
            self._receivers[queue] = receiver
        return receiver

    @staticmethod
    def _projection(body: str) -> Dict[str, Any]:
        """Native ServiceBusMessage kwargs — Subject/CorrelationId/MessageId + the bq-
        application properties (a redundant, routable view of the body). §4.2–§4.3."""
        try:
            env: Dict[str, Any] = EnvelopeCodec.decode(body)
        except (ValueError, TypeError):  # pragma: no cover - defensive
            return {"content_type": "application/json"}
        meta = env.get("meta") or {}

        props: Dict[str, Any] = {}
        if meta.get("schema_version") is not None:
            props["bq-schema-version"] = meta["schema_version"]
        if meta.get("lang"):
            props["bq-source-lang"] = meta["lang"]
        if meta.get("created_at") is not None:
            props["bq-created-at"] = meta["created_at"]

        kwargs: Dict[str, Any] = {"content_type": "application/json"}
        if env.get("job"):
            kwargs["subject"] = env["job"]
        if env.get("trace_id"):
            kwargs["correlation_id"] = env["trace_id"]
        if meta.get("id"):
            kwargs["message_id"] = meta["id"]
        if props:
            kwargs["application_properties"] = props
        return kwargs

    @staticmethod
    def _reconcile(body: str, delivery_count: Any) -> str:
        """Set attempts to max(current, DeliveryCount - 1) — DeliveryCount (1-based) is the
        broker's native redelivery floor, but the runtime retries by republishing with
        attempts+1 in the body, so a republished message (DeliveryCount back to 1) must not
        have its higher body count lowered. First delivery (DeliveryCount 1) reads 0."""
        try:
            dc = int(delivery_count)
        except (ValueError, TypeError):
            return body
        if dc <= 1:
            return body
        native = dc - 1
        env = EnvelopeCodec.decode(body)
        if not env or native <= int(env.get("attempts", 0)):
            return body
        env["attempts"] = native
        return EnvelopeCodec.encode(env)

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        from azure.servicebus import ServiceBusMessage

        message = ServiceBusMessage(body, **self._projection(body))
        self._sender(queue).send_messages(message)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        wait = self._max_wait_time
        if wait is None and timeout and timeout > 0:
            wait = timeout
        messages = self._receiver(queue).receive_messages(max_message_count=1, max_wait_time=wait)
        if not messages:
            return None
        message = messages[0]
        body = self._reconcile(str(message), getattr(message, "delivery_count", None))
        return ReceivedMessage(body=body, queue=queue, handle=message)

    def ack(self, message: ReceivedMessage) -> None:
        if message.handle is None:
            return
        self._receiver(message.queue).complete_message(message.handle)

    def close(self) -> None:  # pragma: no cover - resource cleanup
        for resource in (*self._senders.values(), *self._receivers.values(), self._client):
            try:
                resource.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
