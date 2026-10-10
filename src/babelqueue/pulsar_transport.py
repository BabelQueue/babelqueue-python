"""Apache Pulsar transport. Requires the ``pulsar`` extra:

    pip install "babelqueue[pulsar]"

Producing sends the canonical envelope as the message payload and projects the contract
envelope fields onto native Pulsar message properties (string->string): ``bq-job`` = URN,
``bq-trace-id`` = trace_id, ``bq-message-id`` = meta.id, plus ``bq-schema-version`` /
``bq-source-lang`` / ``bq-attempts`` — so a Java/.NET/... peer can route on ``bq-job``
without parsing the body. Consuming receives one message at a time
(``receive`` -> process -> ``acknowledge``). Per §5.2 / §5.7 the ``bq-job`` property is the
routing key (the body's URN is used only when ``bq-job`` is absent or blank) and the
``bq-attempts`` property is the authoritative attempt count (the body's ``attempts`` is used
only when the property is absent or not a plain non-negative ASCII-decimal integer that fits
a 32-bit int). The result is then raised to the broker's native ``redelivery_count``
(0-based): ``attempts = max(bq-attempts, redelivery_count)`` — no -1, because Pulsar's
redelivery count is 0-based (0 on first delivery). The routing URN is handed to the app through
``ReceivedMessage.route_urn`` — the body's ``job`` / ``urn`` is never rewritten, so retries and
the DLQ keep the producer's original body. When ``bq-job`` and the body name different URNs
the property wins and the optional ``on_routing_mismatch(property_urn, body_urn, envelope)``
hook is called (for an accepted envelope, after attempts are reconciled). A message left un-acked is redelivered (at-least-once).

This implements §5 of the broker-bindings contract. The envelope is unchanged
(``schema_version`` stays 1); Apache Pulsar is purely additive.

URL form: ``pulsar://host:6650`` (or ``pulsar+ssl://``). The default subscription name is
``babelqueue`` over a ``Shared`` subscription. For a custom client/subscription, build the
transport directly and pass it via ``BabelQueue(transport=...)`` or
``PulsarTransport(client=...)``.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional, Tuple

from .codec import EnvelopeCodec, parse_envelope
from .transport import ReceivedMessage, Transport

#: ``bq-attempts`` is valid only as a run of ASCII digits (no sign, no whitespace) ...
_ATTEMPTS_PATTERN = re.compile(r"[0-9]+")
#: ... that fits a signed 32-bit int, matching the Java and .NET consumers (GR-5).
_MAX_ATTEMPTS = 2**31 - 1


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
        on_routing_mismatch: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
        **subscribe_options: Any,
    ) -> None:
        self._url = url or "pulsar://localhost:6650"
        self._subscription = subscription
        self._consumer_type = consumer_type
        self._topic_prefix = topic_prefix
        self._receive_timeout_millis = receive_timeout_millis
        self._subscribe_options = subscribe_options
        self._on_routing_mismatch = on_routing_mismatch
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
    def _projection(body: str, route_urn: str = "") -> Dict[str, str]:
        """Native Pulsar message properties (string->string) — a redundant, routable view of
        the body: bq-job/bq-trace-id/bq-message-id + bq-schema-version/lang/attempts. §5.2.

        A non-empty ``route_urn`` pins ``bq-job`` (a retry / release re-publish keeps the original
        delivery's routing URN); blank derives it from the body."""
        env = parse_envelope(body)
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
        if route_urn:
            props["bq-job"] = route_urn
        return props

    @staticmethod
    def _parse_attempts(raw: Any) -> Optional[int]:
        """The ``bq-attempts`` value as an int, or ``None`` when absent or invalid (§5.5)."""
        if not isinstance(raw, str) or _ATTEMPTS_PATTERN.fullmatch(raw) is None:
            return None
        value = int(raw)
        return value if value <= _MAX_ATTEMPTS else None

    @staticmethod
    def _reconcile(
        body: str,
        redelivery_count: Any,
        properties: Optional[Dict[str, str]] = None,
        on_routing_mismatch: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
    ) -> Tuple[str, str]:
        """Apply §5.2 / §5.7 to a received body; returns ``(body, route_urn)``.

        * Routing: a non-blank ``bq-job`` property is the URN the app routes on. It is returned
          as ``route_urn`` (set on :attr:`ReceivedMessage.route_urn`); the body's ``job`` /
          ``urn`` is never rewritten, so the handler, retries and the dead-letter record keep the
          producer's original body. A body with no URN gets no ``route_urn`` — it stays
          non-conformant and is rejected whatever ``bq-job`` says.
        * Attempts: a valid ``bq-attempts`` property replaces the body's value (the body's is
          kept when it is absent or invalid), then the result is raised to
          ``redelivery_count``. Pulsar's count is 0-based (0 on first delivery), so it maps
          directly with no -1; the max never lowers a runtime-incremented counter (the runtime
          retries by republishing with attempts+1, which resets the redelivery count to 0). The
          value is written into the body only when the body already holds an integer
          ``attempts`` and the value really changes, so a missing / non-integer ``attempts`` is
          still rejected by ``EnvelopeCodec.accepts``.
        * ``on_routing_mismatch(property_urn, body_urn, envelope)`` is called when the property
          and the body name different URNs — only for an envelope the consumer accepts and after
          attempts are reconciled (as in the Java and .NET consumers), so it sees what the
          handler will get. The hook is optional, separate from any error handling, and any
          exception it raises is swallowed — it never affects the message flow.
        """
        properties = properties or {}
        env = parse_envelope(body)
        if not env:
            return body, ""

        routed = str(properties.get("bq-job") or "").strip()
        body_urn = EnvelopeCodec.urn(env)
        route_urn = routed if routed and body_urn else ""

        current = env.get("attempts")
        has_int_attempts = isinstance(current, int) and not isinstance(current, bool)
        attempts = current if has_int_attempts else 0
        from_property = PulsarTransport._parse_attempts(properties.get("bq-attempts"))
        if from_property is not None:
            attempts = from_property
        try:
            rc = int(redelivery_count)
        except (TypeError, ValueError):
            rc = 0
        if rc > attempts:
            attempts = rc

        out = body
        if has_int_attempts and current != attempts:
            env["attempts"] = attempts
            out = EnvelopeCodec.encode(env)

        if route_urn and route_urn != body_urn and on_routing_mismatch is not None:
            if EnvelopeCodec.accepts(env):
                try:
                    on_routing_mismatch(route_urn, body_urn, dict(env))
                except Exception:  # noqa: BLE001 - a diagnostic hook must never break consumption
                    pass
        return out, route_urn

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
    def _properties(message: Any) -> Dict[str, str]:
        getter = getattr(message, "properties", None)
        value = getter() if callable(getter) else getter
        return dict(value) if isinstance(value, dict) else {}

    @staticmethod
    def _payload(message: Any) -> str:
        getter = getattr(message, "data", None)
        data = getter() if callable(getter) else getter
        if isinstance(data, bytes):
            return data.decode("utf-8")
        return str(data) if data is not None else ""

    # -- Transport ----------------------------------------------------------

    def publish(self, queue: str, body: str) -> None:
        self.publish_routed(queue, body, "")

    def publish_routed(self, queue: str, body: str, route_urn: str) -> None:
        """:meth:`publish` with ``bq-job`` pinned to ``route_urn`` (:class:`RoutedPublisher`);
        a blank ``route_urn`` derives it from the body."""
        self._producer(queue).send(
            body.encode("utf-8"), properties=self._projection(body, route_urn)
        )

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
        body, route_urn = self._reconcile(
            self._payload(message),
            self._redelivery_count(message),
            self._properties(message),
            self._on_routing_mismatch,
        )
        return ReceivedMessage(body=body, queue=queue, handle=message, route_urn=route_urn)

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
