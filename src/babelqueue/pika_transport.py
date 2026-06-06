"""RabbitMQ transport over AMQP 0-9-1. Requires the ``amqp`` extra:

    pip install "babelqueue[amqp]"

Producing publishes the envelope to a durable queue with persistent delivery and
the AMQP properties that are part of the cross-language contract (``type`` = URN,
``correlation_id`` = trace_id, ``message_id`` = meta.id, ``x-schema-version`` /
``x-source-lang`` / ``x-attempts`` headers) — so a Go/PHP consumer can route on
``properties.type`` without parsing the body. Consuming uses ``basic_get`` + manual
ack (at-least-once), matching the PHP RabbitMQ driver.

Connection is lazy; it (re)connects on first use and after a drop.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from .transport import ReceivedMessage, Transport


class PikaTransport(Transport):
    def __init__(self, url: str) -> None:
        try:
            import pika
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "PikaTransport requires the 'pika' package. Install with "
                'pip install "babelqueue[amqp]".'
            ) from exc

        self._pika = pika
        self._url = url
        self._connection: Any = None
        self._channel: Any = None
        self._declared: set[str] = set()

    # -- connection / topology ---------------------------------------------

    def _chan(self) -> Any:
        if self._connection is None or self._connection.is_closed:
            self._connection = self._pika.BlockingConnection(self._pika.URLParameters(self._url))
            self._channel = None
            self._declared.clear()
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        return self._channel

    def _declare(self, queue: str) -> None:
        if queue not in self._declared:
            self._chan().queue_declare(queue=queue, durable=True)
            self._declared.add(queue)

    def _properties(self, body: str) -> Any:
        """AMQP properties derived from the envelope (part of the wire contract)."""
        try:
            envelope: Dict[str, Any] = json.loads(body)
        except (ValueError, TypeError):
            return self._pika.BasicProperties(content_type="application/json", delivery_mode=2)

        meta = envelope.get("meta") or {}
        headers = {
            "x-schema-version": meta.get("schema_version"),
            "x-source-lang": meta.get("lang"),
            "x-attempts": envelope.get("attempts", 0),
        }
        return self._pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,  # persistent
            message_id=meta.get("id"),
            correlation_id=envelope.get("trace_id"),
            type=envelope.get("job"),
            app_id="babelqueue",
            headers={k: v for k, v in headers.items() if v is not None},
        )

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        self._declare(queue)
        self._chan().basic_publish(
            exchange="",
            routing_key=queue,
            body=body.encode("utf-8"),
            properties=self._properties(body),
        )

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        self._declare(queue)
        method, _props, body = self._chan().basic_get(queue=queue, auto_ack=False)
        if method is None:
            # Nothing ready — sleep (heartbeat-safe) so the caller doesn't busy-loop.
            if timeout and timeout > 0:
                self._connection.sleep(timeout)
            return None
        text = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        return ReceivedMessage(body=text, queue=queue, handle=method.delivery_tag)

    def ack(self, message: ReceivedMessage) -> None:
        self._chan().basic_ack(delivery_tag=message.handle)

    def close(self) -> None:  # pragma: no cover
        try:
            if self._connection is not None and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass
