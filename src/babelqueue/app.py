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

Graceful shutdown: when :meth:`BabelQueue.consume` runs on the main thread it handles
SIGTERM/SIGINT — it stops taking new messages, lets the in-flight handler finish (or
releases its message back to the broker once ``shutdown_timeout`` elapses), then closes
the transport. A second signal forces an immediate exit (``KeyboardInterrupt``).
Delivery stays at-least-once: a message is acked or released, never dropped, but a handler
that finishes right as the deadline fires may still see its message released and redelivered.
"""

from __future__ import annotations

import inspect
import logging
import random
import signal
import threading
from types import FrameType
from typing import Any, Callable, Dict, Mapping, Optional

from . import dead_letter
from .codec import EnvelopeCodec, has_forbidden_keys, parse_envelope
from ._poison_record import build_poison_record
from .exceptions import BabelQueueError, UnknownUrnError
from .headers import _headers_scope
from .replay import HEADER_REPLAY_BYPASS, _replay_scope
from .routing import UnknownUrnStrategy
from .transport import (
    NON_NUMERIC_ATTEMPTS_ROUTE,
    Abandoner,
    DeadLetterNamer,
    DeadLetterWriter,
    HeaderPublisher,
    NativeSettler,
    PoisonRoute,
    ReceivedMessage,
    Redeliverer,
    Rewinder,
    Transport,
    make_transport,
    publish_keeping_route,
)

Handler = Callable[..., None]

#: Accepted ``retry_jitter`` values: no jitter, or "full" jitter (``uniform(0, d(n))``).
RETRY_JITTERS = ("none", "full")


def retry_delay(
    n: int,
    base: float = 0.0,
    multiplier: float = 1.0,
    maximum: float = 0.0,
    jitter: str = "none",
    uniform: Callable[[float, float], float] = random.uniform,
) -> float:
    """The worker-side retry backoff (ADR-0034 §9) for the ``n``-th attempt (1-based, the new
    ``attempts`` value): ``d(n) = min(maximum, base * multiplier ** (n - 1))`` with ``maximum``
    of 0 meaning no cap; ``jitter="full"`` returns ``uniform(0, d(n))``."""
    delay = float(base) * float(multiplier) ** max(int(n) - 1, 0)
    if maximum and maximum > 0:
        delay = min(float(maximum), delay)
    if jitter == "full":
        return uniform(0.0, delay)
    return delay

#: Logger the runtime reports settlement failures on (e.g. a failed ack after a successful handler).
logger = logging.getLogger("babelqueue.app")

#: Signals :meth:`BabelQueue.consume` turns into a graceful shutdown (main thread only).
SHUTDOWN_SIGNALS = tuple(
    s for s in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)) if s is not None
)

# Dispatch phases, read by the signal handler to decide what an interrupt may do.
_IDLE = "idle"
_HANDLING = "handling"  # the user handler is running; its message is not settled yet
_SETTLING = "settling"  # ack / retry / dead-letter in progress; must not be interrupted


class _ShutdownDeadline(BaseException):
    """Raised inside a running handler when ``shutdown_timeout`` elapses during a graceful
    shutdown, so the loop can release the in-flight message instead of waiting forever.
    A ``BaseException`` so the dispatch-level ``except Exception`` does not swallow it."""


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
        dead_letter_suffix: Optional[str] = None,
        retry_backoff: float = 0.0,
        retry_backoff_multiplier: float = 1.0,
        retry_backoff_max: float = 0.0,
        retry_jitter: str = "none",
        unknown_urn_release_delay: float = 0.0,
        shutdown_timeout: float = 30.0,
    ) -> None:
        """
        ``retry_backoff`` / ``unknown_urn_release_delay`` are the seconds a released message
        stays invisible before redelivery on transports that release natively
        (:class:`~babelqueue.transport.Redeliverer`, e.g. SQS ``ChangeMessageVisibility``,
        Redis ``:delayed``); other transports re-publish immediately, as before. Both default to
        0 s (immediate redelivery); on SQS every release bumps ``ApproximateReceiveCount``, so pair
        ``max_attempts`` with a queue ``RedrivePolicy`` to bound poison-message loops.

        ``retry_backoff`` is the *base* of the worker-side retry backoff (ADR-0034 §9):
        ``d(n) = min(retry_backoff_max, retry_backoff * retry_backoff_multiplier ** (n - 1))``
        seconds, ``n`` being the new ``attempts`` value; ``retry_backoff_max`` of 0 means no cap
        and ``retry_jitter="full"`` draws ``uniform(0, d(n))``. The defaults (multiplier 1, max 0,
        jitter ``"none"``) keep the historical constant delay. Kafka does not apply it: its retry
        delay is the retry-topic tier.

        ``shutdown_timeout`` bounds how long a graceful shutdown waits for the in-flight handler
        before releasing its message. Keep it above your longest handler run: a released
        message is redelivered and its handler runs again, and on brokers that count
        deliveries (SQS ``ApproximateReceiveCount``) the release also consumes an attempt.
        """
        self.transport = transport if transport is not None else make_transport(broker_url)
        self.queue = queue
        self.on_unknown_urn = on_unknown_urn
        self.max_attempts = max_attempts
        self.dead_letter_enabled = bool(dead_letter)
        self.dead_letter_queue = dead_letter_queue
        #: ``None`` = not set: the transport's :class:`~babelqueue.transport.DeadLetterNamer` default
        #: (SQS: ``<queue>-dlq``) or else ``.dlq``. An explicit value is handed to the namer.
        self.dead_letter_suffix = dead_letter_suffix
        self.retry_backoff = float(retry_backoff)
        if retry_jitter not in RETRY_JITTERS:
            raise ValueError(f"retry_jitter must be one of {RETRY_JITTERS}, got {retry_jitter!r}")
        self.retry_backoff_multiplier = float(retry_backoff_multiplier)
        self.retry_backoff_max = float(retry_backoff_max)
        self.retry_jitter = retry_jitter
        self.unknown_urn_release_delay = float(unknown_urn_release_delay)
        self.shutdown_timeout = float(shutdown_timeout)
        self._handlers: Dict[str, Handler] = {}
        self._stop = threading.Event()
        self._rewind_pause = 1.0  # seconds to wait before a rewound gated message is re-polled
        self._phase = _IDLE
        self._last_phase = _IDLE
        self._signalled = False
        self._deadline_hit = False
        self._deadline_timer: Optional[threading.Timer] = None
        self._deadline_lock = threading.Lock()

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

    def publish_with_headers(
        self,
        urn: str,
        data: Mapping[str, Any],
        headers: Mapping[str, str],
        *,
        queue: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> str:
        """Publish a message together with out-of-band transport ``headers``; returns its id.

        The headers ride **beside** the frozen envelope (GR-1) on the transport's per-message
        metadata channel — e.g. a W3C ``traceparent`` for cross-hop span linkage (ADR-0028) —
        never inside it. It is the produce-side counterpart of the headers the runtime surfaces
        to a handler via :func:`~babelqueue.headers.headers_from_context`.

        When the transport implements :class:`~babelqueue.transport.HeaderPublisher` and
        ``headers`` is non-empty, the headers are propagated; otherwise it transparently falls
        back to a plain :meth:`publish` (the headers are dropped — no error, no regression),
        exactly as :func:`~babelqueue.redrive.redrive` degrades. Passing empty headers is
        equivalent to :meth:`publish`, so callers need not branch on transport capability.
        """
        target = queue or self.queue
        envelope = EnvelopeCodec.make(urn, data, queue=target, trace_id=trace_id)
        body = EnvelopeCodec.encode(envelope)
        if headers and isinstance(self.transport, HeaderPublisher):
            self.transport.publish_with_headers(target, body, dict(headers))
        else:
            self.transport.publish(target, body)
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
        handle_signals: bool = True,
    ) -> int:
        """Consume messages until stopped (or ``max_messages`` processed).

        Returns the number of messages processed. With ``max_messages`` set, the
        loop stops once that many are handled or the queue drains within ``timeout``.

        Graceful shutdown: with ``handle_signals`` (the default) and when called on the main
        thread — Python only allows signal handlers there — SIGTERM and SIGINT are handled for
        the duration of the call (the previous handlers are restored afterwards). The first
        signal sets the stop flag: no new message is taken, the in-flight handler is allowed
        to finish and settle (ack/retry), and if it is still running after
        ``shutdown_timeout`` seconds it is interrupted and its message is released back to the
        broker unchanged. The transport is then closed. A second signal forces the exit by
        raising ``KeyboardInterrupt`` inside the handler (its message is released too), which
        the loop swallows as it always did. Off the main thread no handler is installed; call
        :meth:`stop` to end the loop.

        Delivery is at-least-once: a message is either acked or released, never dropped. A
        released message is redelivered and handled again — including, rarely, one whose
        handler had just returned when the ``shutdown_timeout`` deadline fired, before its ack.
        On SQS a release (shutdown included) counts as a delivery in ``ApproximateReceiveCount``
        and therefore advances ``attempts`` — keep ``shutdown_timeout`` above the handler time.
        """
        target = queue or self.queue
        self._warn_if_retries_cannot_redeliver()
        processed = 0
        self._signalled = False
        self._deadline_hit = False
        previous = self._install_signal_handlers() if handle_signals else {}
        inflight: Optional[ReceivedMessage] = None
        try:
            while not self._stop.is_set() and (max_messages is None or processed < max_messages):
                received = self.transport.pop(target, timeout=timeout)
                if received is None:
                    if max_messages is not None:
                        break
                    continue
                inflight = received
                self._last_phase = _IDLE
                if self._stop.is_set():
                    # Reserved after the stop was requested: hand it straight back.
                    inflight = None
                    self._release_unprocessed(received)
                    break
                try:
                    self.dispatch(received)
                except _ShutdownDeadline:
                    inflight = None
                    self._release_unprocessed(received)
                    break
                inflight = None
                processed += 1
        except KeyboardInterrupt:  # graceful Ctrl-C / forced shutdown
            # Release an interrupted message unless its settlement (ack/retry) had started —
            # then it is already (being) settled and releasing it would duplicate it.
            if inflight is not None and self._last_phase != _SETTLING:
                self._release_unprocessed(inflight)
        finally:
            self._cancel_deadline()
            self._restore_signal_handlers(previous)
            # Reset on exit, not on entry, so a stop() racing a consume() that is just
            # starting (e.g. on a worker thread) is honoured rather than wiped.
            self._stop.clear()
            if self._signalled:
                self.transport.close()
        return processed

    def stop(self) -> None:
        """Ask a running :meth:`consume` loop to stop after its in-flight message.

        Thread-safe; usable where no signal handler can be installed (e.g. a worker thread).
        A stop requested before :meth:`consume` gets going is honoured: the next ``consume``
        returns without taking a message (the flag is reset when ``consume`` returns).
        Unlike a signal-driven shutdown it does not close the transport.
        """
        self._stop.set()

    run = consume

    def dispatch(self, received: ReceivedMessage) -> None:
        """Route one reserved message to its handler and acknowledge it.

        The delivered message's out-of-band transport headers are surfaced onto the context for
        the span of this dispatch (:func:`~babelqueue.headers.headers_from_context`), so a handler
        or an optional wrapper (e.g. the ``otel`` module reading a W3C ``traceparent``, ADR-0028)
        can read metadata that travels beside the frozen envelope (GR-1).
        """
        with _headers_scope(received.headers), _replay_scope(
            bool(received.headers.get(HEADER_REPLAY_BYPASS))
        ):
            # A version-gated delivery is rejected without decoding its body (broker-bindings
            # §3.7/§4.7/§5.7/§6.7/§7.7): the raw body goes to the DLQ byte-identical, so there is
            # no envelope to read here.
            gated = received.route_urn.startswith(_UNSUPPORTED_SCHEMA_ROUTE_PREFIX)
            # A body that does not decode, or has no identity, is poison (ADR-0036 decisions 4 and
            # 6): never decoded leniently into an empty envelope, never handed to a handler and
            # never routed as an unknown URN. A transport that already hands its poison over under
            # a poison route (Kafka, Redis) is not classified twice. A delivery that carries an
            # out-of-band routing URN is exempt from the identity half of the rule only.
            poison_cause = (
                None
                if gated or getattr(self.transport, "classifies_poison", False)
                else EnvelopeCodec.poison_cause(
                    received.body, require_identity=not received.route_urn
                )
            )
            envelope = (
                {}
                if gated or poison_cause is not None
                else EnvelopeCodec.decode_lenient(received.body)
            )
            urn = str(envelope.get("job") or envelope.get("urn") or "")
            if received.route_urn and envelope:
                # Transport-supplied routing override; body untouched. Only for a body that
                # decodes: an undecodable one (decode() yields {}) stays on the poison path.
                urn = received.route_urn
            handler = (
                self._handlers.get(urn) if urn and poison_cause is None else None
            )

            try:
                if poison_cause is not None:
                    self._phase = _SETTLING
                    self._route_body_poison(received, poison_cause)
                    return
                if handler is None:
                    self._phase = _SETTLING
                    self._route_unknown(urn, received, envelope)
                    return
                self._phase = _HANDLING
                self._invoke(handler, envelope)  # flips the phase to _SETTLING on return
                self._phase = _SETTLING
            except Exception as exc:  # noqa: BLE001 - one bad message must not kill the loop
                self._phase = _SETTLING
                if poison_cause is not None or received.route_urn.startswith(
                    _UNSUPPORTED_SCHEMA_ROUTE_PREFIX
                ):
                    # A version-gated or poison delivery never takes the generic retry path, even
                    # when its dead-letter write fails: a retry re-publish may rebuild the version
                    # header from the (valid) body and walk the message past the gate into a
                    # handler, and a poison body has no envelope to retry. It is left unacked; the
                    # broker (or a rewind, for cumulative-offset brokers such as Kafka) brings it
                    # back and the same decision is taken again.
                    logger.error(
                        "Dead-lettering a version-gated or poison message (route %r) on queue %r "
                        "failed; it was NOT retried or acknowledged; it will come back and the "
                        "dead-letter write will be retried.",
                        received.route_urn,
                        received.queue,
                        exc_info=exc,
                    )
                    if received.route_urn.startswith(_UNSUPPORTED_SCHEMA_ROUTE_PREFIX):
                        self._abandon_unsettled(received)
                    else:
                        self._rewind_unsettled(received)
                    return
                self._retry_or_dead_letter(received, envelope, exc)
            else:
                # Outside the handler's try: a failed ack is not a handler failure, so it must
                # never send an already-processed message down the retry/release path.
                self._ack_settled(received)
            finally:
                self._last_phase = self._phase
                self._phase = _IDLE

    # -- Internals ----------------------------------------------------------

    def _install_signal_handlers(self) -> Dict[int, Any]:
        """Install the graceful-shutdown handlers; returns the ones they replaced."""
        if threading.current_thread() is not threading.main_thread():
            return {}
        previous: Dict[int, Any] = {}
        for signum in SHUTDOWN_SIGNALS:
            try:
                previous[signum] = signal.signal(signum, self._on_shutdown_signal)
            except (ValueError, OSError):  # pragma: no cover - unsupported on this platform
                continue
        return previous

    @staticmethod
    def _restore_signal_handlers(previous: Dict[int, Any]) -> None:
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler if handler is not None else signal.SIG_DFL)
            except (ValueError, OSError):  # pragma: no cover - defensive
                continue

    def _on_shutdown_signal(self, signum: int, frame: Optional[FrameType]) -> None:
        if self._deadline_hit:
            # Delivered by the deadline timer: interrupt a still-running handler.
            if self._phase == _HANDLING:
                raise _ShutdownDeadline()
            return
        if self._stop.is_set() and self._signalled:
            # Second signal: force the exit unless a settlement is mid-flight (it is short and
            # the loop exits right after it anyway).
            if self._phase != _SETTLING:
                raise KeyboardInterrupt
            return
        self._signalled = True
        self._stop.set()
        if self._phase != _HANDLING:
            return
        if self.shutdown_timeout <= 0:
            raise _ShutdownDeadline()
        self._start_deadline()

    def _start_deadline(self) -> None:
        main_ident = threading.main_thread().ident

        def expire() -> None:
            # Under the lock so the interrupt is never sent after consume() has cancelled the
            # deadline and is about to restore the previous (possibly default) handlers.
            with self._deadline_lock:
                if self._deadline_timer is not timer:
                    return
                self._deadline_hit = True
                kill = getattr(signal, "pthread_kill", None)
                if kill is not None and main_ident is not None and SHUTDOWN_SIGNALS:
                    kill(main_ident, SHUTDOWN_SIGNALS[0])
                else:  # pragma: no cover - platforms without pthread_kill (Windows)
                    import _thread

                    _thread.interrupt_main()

        timer = threading.Timer(self.shutdown_timeout, expire)
        timer.daemon = True
        with self._deadline_lock:
            self._deadline_timer = timer
        timer.start()

    def _cancel_deadline(self) -> None:
        with self._deadline_lock:
            timer, self._deadline_timer = self._deadline_timer, None
        if timer is not None:
            timer.cancel()

    def _retry_delay(self, attempts: int) -> float:
        """Seconds to hold a failed message before redelivery; ``attempts`` is the new count."""
        return retry_delay(
            attempts,
            self.retry_backoff,
            self.retry_backoff_multiplier,
            self.retry_backoff_max,
            self.retry_jitter,
        )

    def _release(self, received: ReceivedMessage, body: str, delay: float) -> None:
        """Give ``received`` back to the broker for redelivery.

        A :class:`~babelqueue.transport.Redeliverer` transport releases natively (e.g. SQS
        ``ChangeMessageVisibility(VisibilityTimeout=delay)``); otherwise ``body`` is
        re-published and the original acknowledged, immediately (the historical behaviour).
        """
        if isinstance(self.transport, Redeliverer):
            self.transport.redeliver(received, body, delay)
            return
        self._publish_for(received, received.queue, _without_forbidden_keys(body))
        self._ack_settled(received)

    def _publish_for(self, received: ReceivedMessage, queue: str, body: str) -> None:
        """Publish ``body`` to ``queue`` on behalf of ``received`` (a retry / release re-publish
        or its dead-letter record). Keeps the routing URN the broker property carried on this
        delivery when the transport can pin it (:class:`RoutedPublisher`), so a property/body
        mismatch does not flip to the body URN on the next attempt or in the DLQ."""
        publish_keeping_route(self.transport, queue, body, received.route_urn, received.key)

    def _write_dead_letter(self, received: ReceivedMessage, queue: str, body: str) -> None:
        """Write the dead-letter record ``body`` to ``queue`` and settle ``received``.

        A :class:`~babelqueue.transport.DeadLetterWriter` transport does both as one guarded step
        (a failed write propagates, never acks); otherwise publish, then acknowledge."""
        if isinstance(self.transport, DeadLetterWriter):
            self.transport.dead_letter(received, queue, body)
            return
        self._publish_for(received, queue, body)
        self._ack_settled(received)

    def _ack_settled(self, received: ReceivedMessage) -> None:
        """Acknowledge a message whose outcome is already final (handled, dead-lettered,
        re-published, dropped or deleted by the unknown-URN strategy).

        A broker failure here (e.g. SQS ``DeleteMessage``) is **not** a handler failure: it is
        logged on :data:`logger` as an ack failure and the message is *not* released — releasing
        it (on SQS at a 0 s default backoff) would redeliver an already-processed message at once.
        The broker redelivers it on its own (SQS: when the visibility timeout expires), so delivery
        stays at-least-once; dedupe on ``meta.id`` (:mod:`babelqueue.idempotency`) if the side
        effect must not repeat. Never stops the consume loop.
        """
        try:
            self.transport.ack(received)
        except Exception:  # noqa: BLE001 - reported distinctly, never retried as a handler failure
            logger.error(
                "Failed to acknowledge a processed message on queue %r; it was NOT released and "
                "will be redelivered by the broker (SQS: after its visibility timeout).",
                received.queue,
                exc_info=True,
            )

    def _settle_native(self, received: ReceivedMessage, reason: str, description: str) -> bool:
        """Hand ``received`` to the transport's :class:`~babelqueue.transport.NativeSettler`
        capability and report whether it did: ``False`` means the transport has none and the caller
        keeps its previous settle. The event is logged and never retried by the SDK; a failing call
        is logged too and leaves the message unacknowledged (the broker redelivers it)."""
        if not isinstance(self.transport, NativeSettler):
            return False
        logger.warning(
            "Left to the broker's native mechanism (not acknowledged, not retried): queue %r, "
            "route %r, reason %s.",
            received.queue, received.route_urn, reason,
        )
        try:
            self.transport.settle_native(received, reason, description)
        except Exception:  # noqa: BLE001 - reported, never kills the loop
            logger.error(
                "Native settle of a message on queue %r failed; it stays unacknowledged and will "
                "be redelivered by the broker.",
                received.queue,
                exc_info=True,
            )
        return True

    def _abandon_unsettled(self, received: ReceivedMessage) -> None:
        """A version-gated delivery whose dead-letter write failed: a :class:`Rewinder` transport is
        rewound (:meth:`_rewind_unsettled`); an :class:`~babelqueue.transport.Abandoner` transport
        (Azure Service Bus) abandons its lock so the broker redelivers it at once, after the same
        short pause. Any other transport leaves it to its lock or visibility timeout."""
        if isinstance(self.transport, Rewinder) or not isinstance(self.transport, Abandoner):
            self._rewind_unsettled(received)
            return
        try:
            self.transport.abandon(received)
        except Exception:  # noqa: BLE001 - reported, never kills the loop
            logger.error(
                "Failed to abandon an unsettled version-gated message (route %r) on queue %r.",
                received.route_urn,
                received.queue,
                exc_info=True,
            )
            return
        self._stop.wait(self._rewind_pause)

    def _rewind_unsettled(self, received: ReceivedMessage) -> None:
        """Make a deliberately unsettled message come back (cumulative-offset brokers such as Kafka
        would otherwise lose it when a later record on the partition is committed), after a short
        pause so an unavailable DLQ is not hammered in a hot loop."""
        if not isinstance(self.transport, Rewinder):
            return
        try:
            self.transport.rewind(received)
        except Exception:  # noqa: BLE001 - reported, never kills the loop
            logger.error(
                "Failed to rewind an unsettled message (route %r) on queue %r.",
                received.route_urn,
                received.queue,
                exc_info=True,
            )
            return
        self._stop.wait(self._rewind_pause)

    def _leave_republish_unsettled(self, received: ReceivedMessage, exc: BaseException) -> None:
        """A retry release / re-publish failed (e.g. a Kafka transport without retry tiers, or a
        broker write error): acknowledging the message would lose it, so it is logged and left
        **unsettled** -- never acked, never committed -- and the consume loop carries on (Go
        ``leaveRepublishUnsettled``). A :class:`~babelqueue.transport.Rewinder` transport (Kafka)
        seeks back to the record and the loop pauses ``_rewind_pause`` seconds first so a broker
        that stays unavailable is not hammered in a hot loop; any other transport redelivers the
        message when its reservation or visibility timeout lapses."""
        logger.error(
            "Re-publishing a message for retry on queue %r (route %r) failed; it was NOT "
            "acknowledged and will be redelivered.",
            received.queue,
            received.route_urn,
            exc_info=exc,
        )
        self._rewind_unsettled(received)

    def _release_unprocessed(self, received: ReceivedMessage) -> None:
        """Release a message whose handler never completed (shutdown): unchanged body
        (normalised only if it carries a forbidden key or a ``urn`` alias), no backoff, and the
        runtime adds no ``attempts`` increment. A broker that counts
        deliveries still counts it: on SQS the release bumps ``ApproximateReceiveCount``, so the
        redelivered envelope reads one more attempt (and a ``RedrivePolicy`` sees it too).

        A :class:`~babelqueue.transport.Rewinder` transport (Kafka) is rewound instead: its record
        was never committed, so seeking back redelivers it untouched, with no retry-tier hop and
        no attempt counted."""
        if isinstance(self.transport, Rewinder):
            try:
                self.transport.rewind(received)
            except Exception:  # noqa: BLE001 - reported, shutdown goes on
                logger.error(
                    "Failed to rewind an unprocessed message on queue %r at shutdown.",
                    received.queue,
                    exc_info=True,
                )
            return
        self._release(received, received.body, 0.0)

    def _invoke(self, handler: Handler, envelope: Mapping[str, Any]) -> None:
        data = dict(envelope.get("data") or {})
        meta = dict(envelope.get("meta") or {})
        if _handler_wants_envelope(handler):
            handler(data, meta, dict(envelope))
        else:
            handler(data, meta)
        # As early as possible after the handler returns: narrows (cannot fully close) the
        # window in which a shutdown deadline would release an already-handled message.
        self._phase = _SETTLING

    def _warn_if_retries_cannot_redeliver(self) -> None:
        """Warn at start-up when a failing handler could not be retried (Kafka without tiers).

        A tierless :class:`~babelqueue.kafka_transport.KafkaTransport` has no way to delay a
        retry: ``redeliver`` raises. The worker logs that, leaves the record uncommitted (rewound,
        after a pause) and keeps consuming, so the record is redelivered but never retried
        through a tier. That only happens once a handler fails, so it is a warning (not a
        start-up error).
        """
        if self._retry_tiers_unavailable() and self.max_attempts > 1:
            logger.warning(
                "The Kafka transport has no retry tiers but max_attempts=%d: a failing handler "
                "cannot be retried -- the record is left uncommitted and redelivered after a "
                "pause, so it keeps failing in place. Configure retry_tiers_ms (or set "
                "max_attempts=1) to retry through the tiers. A poison record (a body that does "
                "not decode, or has no identity) is different: with dead-lettering off and "
                "on_unknown_urn not 'delete' it is committed and dropped with a logged warning "
                "(broker-bindings section 6.5, degrade-to-delete), so it never blocks the "
                "partition; enable dead_letter to keep it in <topic>.dlq instead.",
                self.max_attempts,
            )

    def _route_unknown(self, urn: str, received: ReceivedMessage, envelope: Mapping[str, Any]) -> None:
        strategy = self.on_unknown_urn
        # A Kafka poison record (§6.5: not valid UTF-8 / not a decodable body) is not an unknown URN: it
        # never decoded into an envelope, so the URN part of ``on_unknown_urn`` has no say. It takes
        # the Go reference's ``routePoison`` order, which is checked before the generic DELETE
        # branch: DELETE drops it; with the DLQ on (``dead_letter`` or the DEAD_LETTER strategy) it
        # is dead-lettered (wrapped, reason ``poison``) and committed; with the DLQ off it takes the
        # bounded FAIL path (retry through the tiers up to ``max_attempts``, then drop), never an
        # unconditional DLQ write. Other transports' version-gated deliveries keep their order below.
        # The Redis (Laravel layout) poison reservation (ADR-0033 A1: a body that does not decode as
        # an envelope) is a different, unconditional case: it is dead-lettered as an ADR-0035 poison
        # record (broker-bindings §1.1 / §1.3), and
        # ``on_unknown_urn=DELETE`` must not silently discard it. Only that one route is lifted
        # ahead of DELETE; the generic version-gated prefix below keeps its order.
        if isinstance(received.route_urn, PoisonRoute):
            self._route_poison(received)
            return
        if received.route_urn == NON_NUMERIC_ATTEMPTS_ROUTE:
            # broker-bindings §1.1 / ADR-0033: the DLQ write is an unconditional MUST, so it does
            # not depend on ``dead_letter_enabled`` (Go ``settleTransportPoison`` does the same).
            self._settle_transport_poison(received)
            return
        if strategy == UnknownUrnStrategy.DELETE:
            self._ack_settled(received)
            return
        # A transport's version-gated delivery (unsupported ``bq-schema-version``) can never gain
        # a handler and must not be retried: a retry re-publishes through paths that may rebuild
        # the version header from the (valid) body, or never advance the attempt counter, so the
        # gate would be bypassed or the message would loop. It goes straight to the DLQ (or is
        # dropped when no DLQ is configured).
        if received.route_urn.startswith(_UNSUPPORTED_SCHEMA_ROUTE_PREFIX):
            self._settle_version_gated(received)
            return
        # An undecodable body or a body naming no URN can never gain a handler either, so
        # releasing it unchanged would redeliver it forever. It takes the bounded FAIL path
        # instead (retry up to max_attempts, then dead-letter or drop); only a genuinely unknown
        # URN keeps the verbatim release.
        if strategy == UnknownUrnStrategy.RELEASE and urn:
            try:
                self._release(received, received.body, self.unknown_urn_release_delay)
            except Exception as release_exc:  # noqa: BLE001 - a failed release must not stop the worker
                # Go ``routeUnknown``: the release failed (e.g. a Kafka transport without retry
                # tiers), so acknowledging would lose the message. Left unsettled, loop survives.
                self._leave_republish_unsettled(received, release_exc)
            return
        if strategy == UnknownUrnStrategy.DEAD_LETTER:
            self._dead_letter(received, dict(envelope), "unknown_urn", None)
            return
        # FAIL (and RELEASE of a poison message) — surfaced through the retry/dead-letter path
        # (never kills the loop).
        raise UnknownUrnError(f"No handler mapped for URN [{urn or '(empty)'}].")

    def _route_poison(self, received: ReceivedMessage) -> None:
        """Settle a Kafka poison delivery the way Go ``routePoison`` does.

        DELETE drops it. With the DLQ on (``dead_letter`` or the DEAD_LETTER strategy) it is written
        to the DLQ wrapped (``reason: poison``). With the DLQ off it takes the bounded FAIL path:
        retried through the transport's tiers until ``max_attempts``, then dropped. The attempt
        count comes from the record (``bq-attempts``), because a poison delivery has no decoded
        envelope; the retry body is a bare ``{"attempts": N}`` that stays poison on redelivery.
        Called from the dispatch ``try``: a failed DLQ write propagates and is rewound there."""
        route = received.route_urn
        if self.on_unknown_urn == UnknownUrnStrategy.DELETE:
            self._ack_settled(received)
            return
        if self.dead_letter_enabled or self.on_unknown_urn == UnknownUrnStrategy.DEAD_LETTER:
            self._settle_version_gated(received, always=True)
            return
        if self._retry_tiers_unavailable():
            # broker-bindings §6.5 / ADR-0036 decision 5: no retry tier, no dead-lettering and not
            # ``delete`` degrades to ``delete``. The bounded retry of a poison record needs a tier
            # and would raise, leaving the record uncommitted with ``bq-attempts`` never advancing,
            # so the partition would block. Commit, drop, and say so.
            logger.warning(
                "Dropping a poison record on queue %r (route %r): the Kafka transport has no retry "
                "tiers and dead-lettering is off, so it can be neither retried nor dead-lettered "
                "(broker-bindings section 6.5, degrade-to-delete). The offset was committed.",
                received.queue, str(route),
            )
            self._ack_settled(received)
            return
        attempts = int(getattr(route, "attempts", 0) or 0)
        self._retry_or_dead_letter(
            received,
            {"attempts": attempts},
            UnknownUrnError("The record is poison: it carries no decodable envelope."),
        )

    def _route_body_poison(self, received: ReceivedMessage, cause: str) -> None:
        """Settle a body that does not decode into an envelope, or has no identity, on SQS, AMQP,
        Pulsar, Azure Service Bus, Artemis, the memory transport and any other transport that does
        not classify poison itself (ADR-0036 decisions 2, 4 and 6; Go ``routePoison``).

        The body is never handed to a handler and never takes the unknown-URN strategies:

        * ``on_unknown_urn=DELETE`` drops it (acknowledged and logged);
        * with the DLQ on (``dead_letter`` or the DEAD_LETTER strategy) it is written to the DLQ as
          the ADR-0035 record with ``reason: "poison"`` (original body preserved) and acknowledged
          after the write, with no retry;
        * with the DLQ off, on a transport with a native mechanism (SQS, Azure Service Bus: the
          :class:`~babelqueue.transport.NativeSettler` capability) it is settled natively on the first
          delivery -- not acknowledged, not re-published, not retried, body unchanged (ADR-0037
          decision 2);
        * with the DLQ off on any other transport it takes the bounded FAIL path (``RELEASE`` is
          replaced by it, a poison body would otherwise be released forever): re-published with
          ``attempts + 1`` up to ``max_attempts``, then dropped. The count is the larger of the
          transport's own delivery count and the ``attempts`` of the bare body a previous retry left
          behind.

        A failed DLQ write propagates: :meth:`dispatch` leaves the delivery unacknowledged."""
        if self.on_unknown_urn == UnknownUrnStrategy.DELETE:
            logger.warning(
                "Dropping a poison message on queue %r (on_unknown_urn=delete): %s.",
                received.queue, cause,
            )
            self._ack_settled(received)
            return
        attempts = self._poison_attempts(received)
        if self.dead_letter_enabled or self.on_unknown_urn == UnknownUrnStrategy.DEAD_LETTER:
            self._settle_transport_poison(received, cause, attempts)
            return
        # DLQ off, strategy neither delete nor a writable dead_letter: a transport with a native
        # mechanism (SQS, Azure Service Bus) settles the poison body natively on the first delivery
        # (ADR-0037 decision 2, "Go and Python") instead of the bounded retry below.
        if self._settle_native(received, "poison", "the body does not decode into a valid envelope: " + cause):
            return
        self._retry_or_dead_letter(
            received,
            {"attempts": attempts},
            UnknownUrnError("The message is poison: " + cause),
        )

    @staticmethod
    def _poison_attempts(received: ReceivedMessage) -> int:
        """Deliveries a poison body has already had: the larger of the transport's own count and
        the integer ``attempts`` of the body (a bare ``{"attempts": N}`` left by a retry)."""
        body_attempts = parse_envelope(received.body).get("attempts")
        if isinstance(body_attempts, bool) or not isinstance(body_attempts, int):
            body_attempts = 0
        return max(int(received.native_attempts or 0), body_attempts, 0)

    def _retry_tiers_unavailable(self) -> bool:
        """Whether the transport is a Kafka transport with no retry tiers configured."""
        tiers = getattr(self.transport, "retry_tiers_ms", None)
        return isinstance(tiers, list) and not tiers and isinstance(self.transport, Redeliverer)

    def _settle_transport_poison(
        self, received: ReceivedMessage, cause: Optional[str] = None, attempts: int = 0
    ) -> None:
        """Dead-letter a poison delivery (broker-bindings §1.1 / §1.3, ADR-0033, ADR-0035): a Redis
        poison reservation (unconditional) or, via :meth:`_route_body_poison`, a poison body of any
        other transport (``cause`` and ``attempts`` are then known).

        The DLQ write is an unconditional MUST, so it ignores ``dead_letter_enabled``. The record
        follows ADR-0035 like every other poison record: a strict JSON object body is spliced byte
        for byte with a ``dead_letter`` member (``reason: "poison"``), anything else is carried as
        ``raw`` (base64 plus ``raw_encoding`` when it is not valid UTF-8, never U+FFFD). A failed
        DLQ write propagates: the delivery stays reserved (see :meth:`dispatch`), never dropped."""
        block = dead_letter.annotate(
            {},
            "poison",
            received.queue,
            attempts,
            error="Undecodable BabelQueue body" + (": " + cause if cause else ""),
            exception=BabelQueueError.__name__,
        )["dead_letter"]
        self._write_dead_letter(
            received, self._dead_letter_target(received), build_poison_record(received.body, block)
        )

    def _dead_letter_target(self, received: ReceivedMessage) -> str:
        """The queue a dead-lettered delivery is written to: the configured ``dead_letter_queue``, else
        the transport's :class:`~babelqueue.transport.DeadLetterNamer` name (SQS: ``<queue>-dlq``, FIFO
        ``<queue>-dlq.fifo``, an explicit ``dead_letter_suffix`` placed before ``.fifo``; ADR-0037
        decision 7), else ``<queue><dead_letter_suffix>`` (default ``.dlq``)."""
        if self.dead_letter_queue:
            return self.dead_letter_queue
        if isinstance(self.transport, DeadLetterNamer):
            return self.transport.dead_letter_queue_name(received.queue, self.dead_letter_suffix)
        return received.queue + (".dlq" if self.dead_letter_suffix is None else self.dead_letter_suffix)

    def _settle_version_gated(self, received: ReceivedMessage, always: bool = False) -> None:
        """Dead-letter a version-gated delivery directly (no retry). With no DLQ set it is left to the
        transport's native mechanism when the transport has the :class:`~babelqueue.transport.NativeSettler`
        capability (SQS, Azure Service Bus; ADR-0037 decision 1 rule 3) and dropped otherwise.

        ``always`` writes the DLQ record in every configuration, ignoring ``dead_letter_enabled``.
        A Kafka poison record reaches it with the DLQ already on (:meth:`_route_poison`). The Redis
        poison reservation does not come through here: it has its own ADR-0035 record
        (:meth:`_settle_transport_poison`).

        The gate rejects the message *without decoding it*, so the DLQ record is the incoming raw
        body, byte-identical, with no ``dead_letter`` block (matching the Java, .NET and Node
        SDKs). A failed DLQ write propagates: the delivery is left unacknowledged and handed back
        (see :meth:`dispatch`), never retried.
        """
        if always or self.dead_letter_enabled or self.on_unknown_urn == UnknownUrnStrategy.DEAD_LETTER:
            self._write_dead_letter(received, self._dead_letter_target(received), received.body)
            return
        # No DLQ to hold it (broker-bindings §5.7 rule 3). A transport with a native mechanism (SQS,
        # Azure Service Bus) leaves it to that mechanism instead of dropping it (ADR-0037 decision 1).
        if self._settle_native(
            received,
            "unsupported schema version",
            "the message declares a schema version this consumer does not support",
        ):
            return
        # Any other transport: the drop is deliberate, but it must not be silent.
        logger.warning(
            "Dropping an undeliverable message on queue %r (route %r, %d body characters): it can "
            "never be handled and no dead-letter queue is configured.",
            received.queue, received.route_urn, len(received.body),
        )
        self._ack_settled(received)

    def _retry_or_dead_letter(
        self, received: ReceivedMessage, envelope: Dict[str, Any], exc: BaseException
    ) -> None:
        attempts = int(envelope.get("attempts", 0)) + 1
        envelope["attempts"] = attempts

        if attempts < self.max_attempts:
            try:
                self._release(received, EnvelopeCodec.encode(envelope), self._retry_delay(attempts))
            except Exception as release_exc:  # noqa: BLE001 - a failed release must not stop the worker
                self._leave_republish_unsettled(received, release_exc)
            return

        if self.dead_letter_enabled:
            reason = "unknown_urn" if isinstance(exc, UnknownUrnError) else "failed"
            self._dead_letter(received, envelope, reason, exc)
            return

        # Retries exhausted, no DLQ configured — drop it (ack so it leaves the queue), loudly.
        logger.warning(
            "Dropping a message on queue %r after %d attempts: retries are exhausted and no "
            "dead-letter queue is configured.",
            received.queue, attempts,
        )
        self._ack_settled(received)

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
        self._write_dead_letter(
            received, self._dead_letter_target(received), EnvelopeCodec.encode(annotated)
        )


#: Reserved ``ReceivedMessage.route_urn`` prefix a transport uses to hand over a delivery it
#: version-gated (broker-bindings §5.7 step 3, e.g. the pulsar transport's ``bq-schema-version``
#: check). Not a ``urn:babel:`` URN, so no handler owns it. Must equal
#: ``UNSUPPORTED_SCHEMA_ROUTE_PREFIX`` of the pulsar, sqs, asb, kafka and artemis transports (locked
#: by a test); never reaches the wire. A delivery under it is dead-lettered directly, never retried.
_UNSUPPORTED_SCHEMA_ROUTE_PREFIX = "babelqueue:unsupported-schema-version:"


def _without_forbidden_keys(body: str) -> str:
    """``body`` unchanged, unless it carries a forbidden envelope key (message-envelope.md §10,
    e.g. from a legacy producer) or a ``urn`` alias — then it is re-encoded without it (warned by the codec), so a
    raw-body re-publish never re-emits a key the decode boundary reported as dropped."""
    parsed = parse_envelope(body)
    # ``urn`` is not a §10 key but ``encode`` rejects it and ADR-0031 (a)/(c) forbid re-emitting it.
    if not parsed or not (has_forbidden_keys(parsed) or "urn" in parsed):
        return body
    # ``decode`` (not the raw parse) so the dropped keys — and a top-level ``urn`` alias, folded
    # into ``job`` — are gone before ``encode``, which rejects them (ADR-0031 (c)).
    return EnvelopeCodec.encode(EnvelopeCodec.decode_lenient(body))


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
