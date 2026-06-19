"""The BabelQueue runtime: produce and consume polyglot messages.

    from babelqueue import BabelQueue

    app = BabelQueue("redis://localhost:6379/0", queue="orders")

    @app.handler("urn:babel:orders:created")
    def on_order_created(data, meta):
        ...                       # AI/ML, data processing, anything

    app.publish("urn:babel:orders:created", {"order_id": 1042})
    app.run()                     # consume forever

Routing is by URN; the wire format is the canonical envelope (shared core codec),
so this interoperates with the PHP/Laravel, Symfony, Go, ... SDKs. Retry uses the
top-level ``attempts`` counter; failures past ``max_attempts`` go to a dead-letter
queue when enabled.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, Mapping, Optional

from . import dead_letter
from .codec import EnvelopeCodec
from .exceptions import UnknownUrnError
from .replay import HEADER_REPLAY_BYPASS, _replay_scope
from .routing import UnknownUrnStrategy
from .transport import ReceivedMessage, Transport, make_transport

Handler = Callable[..., None]


class BabelQueue:
    def __init__(
        self,
        broker_url: str = "memory://",
        *,
        transport: Optional[Transport] = None,
        queue: str = "default",
        on_unknown_urn: str = UnknownUrnStrategy.FAIL,
        max_attempts: int = 3,
        dead_letter: bool = False,
        dead_letter_queue: Optional[str] = None,
        dead_letter_suffix: str = ".dlq",
    ) -> None:
        self.transport = transport if transport is not None else make_transport(broker_url)
        self.queue = queue
        self.on_unknown_urn = on_unknown_urn
        self.max_attempts = max_attempts
        self.dead_letter_enabled = bool(dead_letter)
        self.dead_letter_queue = dead_letter_queue
        self.dead_letter_suffix = dead_letter_suffix
        self._handlers: Dict[str, Handler] = {}

    # -- Produce ------------------------------------------------------------

    def publish(
        self,
        urn: str,
        data: Mapping[str, Any],
        *,
        queue: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> str:
        """Publish a message; returns its id (``meta.id``)."""
        target = queue or self.queue
        envelope = EnvelopeCodec.make(urn, data, queue=target, trace_id=trace_id)
        self.transport.publish(target, EnvelopeCodec.encode(envelope))
        return envelope["meta"]["id"]

    # -- Register handlers --------------------------------------------------

    def handler(self, urn: str) -> Callable[[Handler], Handler]:
        """Decorator: register ``fn`` as the handler for ``urn``."""

        def decorator(fn: Handler) -> Handler:
            self._handlers[urn] = fn
            return fn

        return decorator

    def register(self, urn: str, fn: Handler) -> None:
        self._handlers[urn] = fn

    # -- Consume ------------------------------------------------------------

    def consume(
        self,
        queue: Optional[str] = None,
        *,
        max_messages: Optional[int] = None,
        timeout: float = 1.0,
    ) -> int:
        """Consume messages until interrupted (or ``max_messages`` processed).

        Returns the number of messages processed. With ``max_messages`` set, the
        loop stops once that many are handled or the queue drains within ``timeout``.
        """
        target = queue or self.queue
        processed = 0
        try:
            while max_messages is None or processed < max_messages:
                received = self.transport.pop(target, timeout=timeout)
                if received is None:
                    if max_messages is not None:
                        break
                    continue
                self.dispatch(received)
                processed += 1
        except KeyboardInterrupt:  # pragma: no cover - graceful Ctrl-C
            pass
        return processed

    run = consume

    def dispatch(self, received: ReceivedMessage) -> None:
        """Route one reserved message to its handler and acknowledge it."""
        with _replay_scope(bool(received.headers.get(HEADER_REPLAY_BYPASS))):
            envelope = EnvelopeCodec.decode(received.body)
            urn = str(envelope.get("job") or envelope.get("urn") or "")
            handler = self._handlers.get(urn) if urn else None

            try:
                if handler is None:
                    self._route_unknown(urn, received, envelope)
                    return
                self._invoke(handler, envelope)
                self.transport.ack(received)
            except Exception as exc:  # noqa: BLE001 - one bad message must not kill the loop
                self._retry_or_dead_letter(received, envelope, exc)

    # -- Internals ----------------------------------------------------------

    def _invoke(self, handler: Handler, envelope: Mapping[str, Any]) -> None:
        data = dict(envelope.get("data") or {})
        meta = dict(envelope.get("meta") or {})
        if _handler_wants_envelope(handler):
            handler(data, meta, dict(envelope))
        else:
            handler(data, meta)

    def _route_unknown(self, urn: str, received: ReceivedMessage, envelope: Mapping[str, Any]) -> None:
        strategy = self.on_unknown_urn
        if strategy == UnknownUrnStrategy.DELETE:
            self.transport.ack(received)
            return
        if strategy == UnknownUrnStrategy.RELEASE:
            self.transport.publish(received.queue, received.body)
            self.transport.ack(received)
            return
        if strategy == UnknownUrnStrategy.DEAD_LETTER:
            self._dead_letter(received, dict(envelope), "unknown_urn", None)
            return
        # FAIL — surfaced through the retry/dead-letter path (never kills the loop).
        raise UnknownUrnError(
            f"No handler mapped for URN [{urn or '(empty)'}]."
        )

    def _retry_or_dead_letter(
        self, received: ReceivedMessage, envelope: Dict[str, Any], exc: BaseException
    ) -> None:
        attempts = int(envelope.get("attempts", 0)) + 1
        envelope["attempts"] = attempts

        if attempts < self.max_attempts:
            self.transport.publish(received.queue, EnvelopeCodec.encode(envelope))
            self.transport.ack(received)
            return

        if self.dead_letter_enabled:
            reason = "unknown_urn" if isinstance(exc, UnknownUrnError) else "failed"
            self._dead_letter(received, envelope, reason, exc)
            return

        # Retries exhausted, no DLQ configured — drop it (ack so it leaves the queue).
        self.transport.ack(received)

    def _dead_letter(
        self,
        received: ReceivedMessage,
        envelope: Dict[str, Any],
        reason: str,
        exc: Optional[BaseException],
    ) -> None:
        original_queue = str((envelope.get("meta") or {}).get("queue") or received.queue)
        annotated = dead_letter.annotate(
            envelope,
            reason,
            original_queue,
            int(envelope.get("attempts", 0)),
            error=(str(exc) if exc is not None else None),
            exception=(type(exc).__name__ if exc is not None else None),
        )
        target = self.dead_letter_queue or (received.queue + self.dead_letter_suffix)
        self.transport.publish(target, EnvelopeCodec.encode(annotated))
        self.transport.ack(received)


def _handler_wants_envelope(fn: Handler) -> bool:
    """True if the handler takes a 3rd positional arg (the full envelope)."""
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):  # pragma: no cover - builtins/C callables
        return False
    positional = [
        p for p in params
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(p.kind == p.VAR_POSITIONAL for p in params)
    return has_varargs or len(positional) >= 3
