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

import logging
import re
from typing import Any, Dict, Optional
from urllib.parse import quote, urlsplit

from .codec import ASCII_WHITESPACE, SCHEMA_VERSION, EnvelopeCodec, parse_envelope
from .transport import ReceivedMessage, Transport, truncate_declared_version

_logger = logging.getLogger("babelqueue.asb")

#: A blank ``bq-schema-version``: empty or only ASCII whitespace (space, \t, \n, \v, \f, \r) — the
#: same set in every SDK (GR-5). Deliberately not ``str.strip()``, which also strips U+001C-U+001F,
#: U+0085, U+00A0, U+2028, U+3000 ...
_BLANK_ASCII_PATTERN = re.compile("[" + re.escape(ASCII_WHITESPACE) + "]*")
#: Prefix of the reserved ``route_urn`` a version-gated message is delivered under. It is not a
#: ``urn:babel:`` URN, so no handler can legitimately own it; the runtime dead-letters it
#: directly (never retried, never decoded; the DLQ record is the incoming raw body). The
#: rejected ``bq-schema-version`` is named in the logged error.
UNSUPPORTED_SCHEMA_ROUTE_PREFIX = "babelqueue:unsupported-schema-version:"


_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1


def _amqp_typed(props: Dict[str, Any]) -> Dict[str, Any]:
    """Pin out-of-int32 ints to AMQP ``long`` (§4.3: ``bq-created-at`` is Unix ms).

    The pure-Python AMQP stack in ``azure-servicebus`` encodes every bare Python int as an
    AMQP int32, so a millisecond epoch overflows at send time. A value outside the int32 range
    is wrapped in the stack's explicit type descriptor; everything else is left as-is.
    """
    try:
        from azure.servicebus._pyamqp.types import TYPE, VALUE, AMQPTypes

        long_type: Any = AMQPTypes.long
    except ImportError:  # pragma: no cover - older SDK layouts
        TYPE, VALUE, long_type = "TYPE", "VALUE", "LONG"
    out: Dict[str, Any] = {}
    for key, value in props.items():
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and not _INT32_MIN <= value <= _INT32_MAX
        ):
            out[key] = {TYPE: long_type, VALUE: value}
        else:
            out[key] = value
    return out


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
            env: Dict[str, Any] = parse_envelope(body)
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
        env = EnvelopeCodec.decode_lenient(body)
        if not env or native <= int(env.get("attempts", 0)):
            return body
        env["attempts"] = native
        return EnvelopeCodec.encode(env)

    @staticmethod
    def _unsupported_schema_version(properties: Any) -> Optional[str]:
        """The §4.7 step-3 gate on the ``bq-schema-version`` application property: the offending
        raw value as text, or ``None``.

        The AMQP client may hand keys and values over as ``bytes``; both are decoded as UTF-8. A
        missing property, or a blank string (the empty string or only ASCII whitespace — the same
        set in every SDK, GR-5, not ``str.strip()``) is not gated: the usual decode path runs. A
        present value is accepted only when it is exactly the core's supported ``SCHEMA_VERSION``:
        the string ``"1"`` or the integer ``1`` (a ``bool`` is not an integer here, though it is
        one in Python). ``"2"``, ``"x"``, ``"01"``, ``" 1"``, ``0``, ``2``, ``1.5``, ``True`` and
        any other type are rejected."""
        if not properties:
            return None
        raw: Any = None
        try:
            items = list(properties.items())
        except (AttributeError, TypeError):
            return None
        for key, value in items:
            name = key.decode("utf-8", "replace") if isinstance(key, (bytes, bytearray)) else key
            if name == "bq-schema-version":
                raw = value
                break
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8", "replace")
        if isinstance(raw, str):
            if _BLANK_ASCII_PATTERN.fullmatch(raw) is not None or raw == str(SCHEMA_VERSION):
                return None
            return raw
        if isinstance(raw, int) and not isinstance(raw, bool) and raw == SCHEMA_VERSION:
            return None
        return str(raw)

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        from azure.servicebus import ServiceBusMessage

        kwargs = self._projection(body)
        if kwargs.get("application_properties"):
            kwargs["application_properties"] = _amqp_typed(kwargs["application_properties"])
        message = ServiceBusMessage(body, **kwargs)
        self._sender(queue).send_messages(message)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        wait = self._max_wait_time
        if wait is None and timeout and timeout > 0:
            wait = timeout
        messages = self._receiver(queue).receive_messages(max_message_count=1, max_wait_time=wait)
        if not messages:
            return None
        message = messages[0]
        # §4.7 step 3: gate on ``bq-schema-version`` BEFORE any body decode (the attempts reconcile
        # below decodes the body). A rejected message keeps its raw body and the reserved route.
        unsupported = truncate_declared_version(
            self._unsupported_schema_version(
            getattr(message, "application_properties", None)
        )
        )
        if unsupported is not None:
            _logger.warning(
                "Azure Service Bus message on %r carries unsupported bq-schema-version %r "
                "(supported: %s); not decoding it.",
                queue,
                unsupported,
                SCHEMA_VERSION,
            )
            return ReceivedMessage(
                body=str(message),
                queue=queue,
                handle=message,
                route_urn=UNSUPPORTED_SCHEMA_ROUTE_PREFIX + quote(unsupported, safe=""),
            )
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
