"""Broker transport abstraction for the runtime.

The runtime talks to a broker only through :class:`Transport`, so the routing /
retry logic is broker-agnostic and unit-testable with :class:`InMemoryTransport`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Protocol, runtime_checkable

from .exceptions import BabelQueueError


#: Longest rejected ``bq-schema-version`` value kept for logs and the dead-letter reason; the rest
#: is cut and ``...`` appended (same cap as the .NET / Java / Node gates), so a huge header cannot
#: inflate the DLQ record.
_DECLARED_VERSION_MAX_CHARS = 64


def truncate_declared_version(value: Optional[str]) -> Optional[str]:
    """Cap a rejected ``bq-schema-version`` value at 64 characters plus ``...`` (``None`` passes)."""
    if value is None or len(value) <= _DECLARED_VERSION_MAX_CHARS:
        return value
    return value[:_DECLARED_VERSION_MAX_CHARS] + "..."


@dataclass
class ReceivedMessage:
    """A message popped from a queue, plus a transport-internal ack handle."""

    body: str
    queue: str
    handle: Any = None
    #: Out-of-band transport headers a :class:`HeaderPublisher` carried with the message
    #: (e.g. the ``bq-replay-bypass`` marker). Empty for transports that don't surface them.
    headers: Dict[str, str] = field(default_factory=dict)
    #: Optional routing override: when non-empty the app looks the handler up by this URN
    #: instead of the decoded body's URN. A transport whose broker carries the URN out of band
    #: (e.g. the Pulsar ``bq-job`` property) fills it so routing follows the property while the
    #: body is left untouched (handler, retry and dead-letter keep the producer's original body).
    #: Empty means "route on the body URN" (the default for every other transport).
    route_urn: str = ""
    #: The broker record key of this delivery, byte-for-byte (``None`` = null key; an empty
    #: ``bytes`` is an empty key), for transports that have one (Kafka). The runtime hands it back
    #: to a :class:`SettlePublisher` on its own re-publishes (retry, DLQ) so the key is not lost;
    #: it never synthesises one. Not part of the envelope; ``None`` for every other transport.
    key: Optional[bytes] = None
    #: The delivery count the broker itself reports for this delivery (0-based: ``0`` on the first
    #: delivery), for transports that have one (SQS ``ApproximateReceiveCount - 1``, the Pulsar
    #: ``bq-attempts`` property floored by the ``RedeliveryCount``). A **poison** body has no
    #: decodable envelope to carry ``attempts``, so the runtime counts its bounded retry from the
    #: larger of this value and the ``attempts`` of the (bare) body it re-publishes. ``0`` for
    #: every transport that does not report one.
    native_attempts: int = 0


class PoisonRoute(str):
    """The ``route_urn`` a poison record is delivered under (the reserved ``NON_UTF8_ROUTE`` / ``POISON_ROUTE`` of a transport that dead-letters undecodable records, today Kafka). It compares and hashes exactly like the
    plain route string, so the runtime and ``redrive`` keep recognising the reserved routes, but it
    also carries what is needed to build the wrapped DLQ body when the record is dead-lettered:
    the consumed queue, the original queue, the ``bq-attempts`` header value and the failure cause.

    The wrap is applied by :meth:`KafkaTransport.publish_settle`, and only when the record is written
    to a queue other than the one it was consumed from. That is exactly the runtime's dead-letter
    write; ``redrive`` restores a poison record to the DLQ it popped it from, which therefore stays
    byte for byte, whatever that DLQ is named (``dead_letter_queue`` / ``dead_letter_suffix``)."""

    queue: str
    original_queue: str
    attempts: int
    cause: str

    def __new__(
        cls, route: str, queue: str, original_queue: str, attempts: int, cause: str
    ) -> "PoisonRoute":
        obj = super().__new__(cls, route)
        obj.queue = queue
        obj.original_queue = original_queue
        obj.attempts = attempts
        obj.cause = cause
        return obj


class Transport(ABC):
    """Minimal broker contract: publish a raw body, pop one, acknowledge it."""

    #: ``True`` for a transport that already classifies poison bodies itself and hands them over
    #: under a poison route (Kafka, Redis). The runtime then does not classify the body again; for
    #: every other transport it applies the poison rule of ADR-0036 decisions 4 and 6 in
    #: :meth:`BabelQueue.dispatch` (a body that does not decode, or has no identity, is never
    #: handed to a handler and takes the poison route).
    classifies_poison: bool = False

    @abstractmethod
    def publish(self, queue: str, body: str) -> None:
        """Append an already-encoded envelope to ``queue``."""

    @abstractmethod
    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        """Reserve the next message from ``queue``, or ``None`` if none arrives."""

    @abstractmethod
    def ack(self, message: ReceivedMessage) -> None:
        """Acknowledge (remove) a reserved message."""

    def close(self) -> None:  # pragma: no cover - optional
        """Release any resources (override if needed)."""


@runtime_checkable
class HeaderPublisher(Protocol):
    """Optional :class:`Transport` capability: publish a body together with out-of-band
    transport headers (e.g. the replay-bypass marker), for brokers that carry per-message
    metadata. A transport that does not implement it simply does not propagate headers —
    callers fall back to plain :meth:`Transport.publish` (ADR-0027)."""

    def publish_with_headers(self, queue: str, body: str, headers: Dict[str, str]) -> None:
        """Append ``body`` to ``queue`` along with out-of-band ``headers``."""
        ...


@runtime_checkable
class RoutedPublisher(Protocol):
    """Optional :class:`Transport` capability: re-publish a body while keeping the routing
    identity the broker's own routing property carried on the original delivery
    (:attr:`ReceivedMessage.route_urn`). The runtime uses it for its retry and unknown-URN
    release re-publishes when the received message was routed on a URN that differs from its
    body, so the route stays constant across retries (broker-bindings §5.2 / §5.7). A transport
    that does not implement it gets a plain :meth:`Transport.publish`."""

    def publish_routed(self, queue: str, body: str, route_urn: str) -> None:
        """Append ``body`` to ``queue``, pinning the broker routing property to ``route_urn``."""
        ...


@runtime_checkable
class SettlePublisher(Protocol):
    """Optional :class:`Transport` capability: a re-publish that *settles* a received message
    (retry, release, dead-letter, poison/gate DLQ, ``redrive`` restore) and so must be
    broker-confirmed before the delivery is acknowledged. Brokers whose acknowledgement is a
    cumulative position (Kafka) need it: committing before the write is confirmed loses the
    record on a delivery failure. It raises when the write is not confirmed; the caller then leaves
    the delivery unsettled. A user's own :meth:`Transport.publish` is never routed here. A transport
    that does not implement it gets the :class:`RoutedPublisher` / plain-publish behaviour."""

    def publish_settle(
        self, queue: str, body: str, route_urn: str = "", key: Optional[bytes] = None
    ) -> None:
        """Append ``body`` to ``queue`` and return only once the broker confirmed the write.

        ``key`` is the consumed record's key (:attr:`ReceivedMessage.key`), carried unchanged
        when the broker has record keys; ``None`` means a null key."""
        ...


def publish_keeping_route(
    transport: "Transport",
    queue: str,
    body: str,
    route_urn: str = "",
    key: Optional[bytes] = None,
) -> None:
    """Publish ``body`` to ``queue``, carrying ``route_urn`` when it is non-empty and the
    transport implements :class:`RoutedPublisher`; otherwise a plain :meth:`Transport.publish`.
    A :class:`SettlePublisher` transport gets the confirmed settle write instead, with the
    consumed record's ``key`` when there is one (a null key is simply not passed).
    Shared by the runtime's re-publishes (retry, release, dead-letter) and by ``redrive``."""
    if isinstance(transport, SettlePublisher):
        if key is None:
            transport.publish_settle(queue, body, route_urn)
        else:
            transport.publish_settle(queue, body, route_urn, key=key)
    elif route_urn and isinstance(transport, RoutedPublisher):
        transport.publish_routed(queue, body, route_urn)
    else:
        transport.publish(queue, body)


@runtime_checkable
class Redeliverer(Protocol):
    """Optional :class:`Transport` capability: give a received message back to the broker
    for redelivery after ``delay`` seconds instead of deleting it and publishing a copy
    (e.g. SQS ``ChangeMessageVisibility`` — broker-bindings.md §3.5).

    ``body`` is the envelope the runtime would republish (``attempts`` already advanced);
    a releaser that keeps the original message (and lets the broker count deliveries)
    ignores it, while a releaser that must fall back to send-then-delete uses it. A
    transport that does not implement it gets the default publish-then-ack release.

    The method is deliberately not named ``release``: some transports already expose a
    ``release`` with a different, transport-specific signature."""

    def redeliver(self, message: ReceivedMessage, body: str, delay: float) -> None:
        """Hand ``message`` back to the broker, visible again after ``delay`` seconds."""
        ...


@runtime_checkable
class DeadLetterWriter(Protocol):
    """Optional :class:`Transport` capability: write a dead-letter record and settle the
    delivery as ONE guarded step (Redis legacy layout, ADR-0033: a reservation that expired and
    was swept must write nothing and ack nothing, so a late settle cannot duplicate the message).

    ``body`` is the record to write to ``queue``. A transport that does not implement it gets the
    default publish-then-ack."""

    def dead_letter(self, message: ReceivedMessage, queue: str, body: str) -> None:
        """Write ``body`` to ``queue`` and settle ``message``."""
        ...


@runtime_checkable
class Rewinder(Protocol):
    """Optional :class:`Transport` capability: make an *unacknowledged* delivery come back.

    Needed by brokers whose acknowledgement is a cumulative per-partition position (Kafka): not
    acking a record is not enough there, because acking a later record on the same partition
    commits past it and it is lost. :meth:`rewind` moves the consumer back to ``message`` so the
    next :meth:`Transport.pop` returns it again. The runtime calls it only when it deliberately
    leaves a message unsettled: a version-gated or poison message whose dead-letter write failed,
    a retry release / re-publish that failed (e.g. a Kafka transport without retry tiers), and the
    verbatim release of an unknown-URN message that failed."""

    def rewind(self, message: ReceivedMessage) -> None:
        """Position the consumer so ``message`` is delivered again by the next ``pop``."""
        ...


@runtime_checkable
class DeadLetterNamer(Protocol):
    """Optional :class:`Transport` capability: the name of the BabelQueue dead-letter queue for a
    source queue, for a broker whose queue-name rules the runtime-wide ``<queue><suffix>`` shape cannot
    satisfy. The runtime asks it whenever ``dead_letter_queue`` is not set, passing ``dead_letter_suffix``
    (``None`` when not set). Amazon SQS implements it (a queue name cannot contain a dot and a FIFO name
    must end in ``.fifo``): the default is ``<queue>-dlq``, FIFO ``<queue>-dlq.fifo``, and an explicit
    suffix goes before ``.fifo`` too (``orders.fifo`` + ``-x`` -> ``orders-x.fifo``), never after it
    (ADR-0037 decision 7). A transport without it keeps ``<queue><suffix>`` with the default ``.dlq``."""

    def dead_letter_queue_name(self, queue: str, suffix: Optional[str] = None) -> str:
        """The dead-letter queue name for ``queue`` (``suffix`` is the explicit one, or ``None``)."""
        ...


@runtime_checkable
class Abandoner(Protocol):
    """Optional :class:`Transport` capability: hand an unsettled *version-gated* delivery back to the
    broker at once (Azure Service Bus ``Abandon``; broker-bindings.md section 4.5 "A failed write
    ``Abandon``s it"; ADR-0037 decision 1 rule 5).

    The runtime calls :meth:`abandon` only when the dead-letter write of a version-gated delivery
    failed, instead of leaving it locked until the lock lapses. It is deliberately narrower than
    :class:`Rewinder`: a transport that offers only this capability keeps its previous graceful
    shutdown release and retry re-publish failure handling (copy re-publish and ack)."""

    def abandon(self, message: ReceivedMessage) -> None:
        """Release the broker lock on ``message`` so the broker redelivers it right away."""
        ...


@runtime_checkable
class NativeSettler(Protocol):
    """Optional :class:`Transport` capability: leave a delivery the runtime can neither process nor
    safely drop to the broker's own mechanism, without acknowledging or deleting it and without
    re-publishing or retrying it (ADR-0037 decisions 1 and 2).

    The runtime calls :meth:`settle_native` instead of acknowledging when dead-lettering is off (and
    the strategy is neither ``delete`` nor a writable ``dead_letter``) for a version-gated delivery
    (broker-bindings.md section 5.7 rule 3) and for a poison body (error-handling.md section 7).
    SQS implements it as "do nothing" (no ``DeleteMessage``: visibility expiry redelivers the message
    and the queue's ``RedrivePolicy`` moves it to the native DLQ after ``maxReceiveCount``); Azure
    Service Bus implements it with an explicit native ``DeadLetter(reason, description)`` into
    ``$DeadLetterQueue``. The body is never altered. A transport that does not implement it keeps
    the previous settle (acknowledge and drop), so the behaviour follows the capability, never the
    transport's name."""

    def settle_native(self, message: ReceivedMessage, reason: str, description: str) -> None:
        """Leave ``message`` to the broker's native mechanism; ``reason`` and ``description`` say
        why (e.g. ``"unsupported schema version"``, ``"poison"``)."""
        ...


#: Reserved ``route_urn`` of a record whose envelope carries a non-numeric ``attempts`` (a foreign
#: producer). It sits under the version-gated prefix, so the runtime dead-letters it directly, raw
#: and never retried (retrying needs a numeric attempt counter); ``redrive`` restores it to the DLQ
#: instead of replaying it to a work topic. Produced only by :class:`~babelqueue.redis_transport.RedisTransport`
#: for a poison body (ADR-0033 A1: Laravel layout, and a legacy ``:processing`` body Go's typed ``Decode`` rejects), which the runtime dead-letters unconditionally.
NON_NUMERIC_ATTEMPTS_ROUTE = "babelqueue:unsupported-schema-version:non-numeric-attempts"


@runtime_checkable
class BatchPopper(Protocol):
    """Optional :class:`Transport` capability: take several deliveries at once and hold them.

    Needed by brokers whose :meth:`Transport.pop` treats a delivery that was neither acked nor
    rewound as abandoned and takes it back on the next call (Kafka: the position is cumulative, so
    an unsettled record must come back rather than be skipped). A drain-first consumer such as
    :func:`babelqueue.redrive.redrive` pops every message before it settles any of them, and must
    not have the earlier ones taken back by each following pop. :meth:`pop_batch` hands out the
    whole batch in one call; the messages may then be settled in any order, and an implementation
    must not let a commit pass one that is still unsettled."""

    def pop_batch(self, queue: str, max: int = 0, timeout: float = 1.0) -> List[ReceivedMessage]:
        """Up to ``max`` messages (all currently available when ``max`` is 0), held un-settled."""
        ...


class InMemoryTransport(Transport):
    """In-process transport for tests and broker-free local runs (``memory://``)."""

    def __init__(self) -> None:
        # Bodies and their out-of-band headers are kept in lockstep parallel deques, so the
        # body storage layout stays a plain Deque[str].
        self._queues: Dict[str, Deque[str]] = defaultdict(deque)
        self._headers: Dict[str, Deque[Dict[str, str]]] = defaultdict(deque)

    def publish(self, queue: str, body: str) -> None:
        self._queues[queue].append(body)
        self._headers[queue].append({})

    def publish_with_headers(self, queue: str, body: str, headers: Dict[str, str]) -> None:
        self._queues[queue].append(body)
        self._headers[queue].append(dict(headers))

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        dq = self._queues.get(queue)
        if not dq:
            return None
        return ReceivedMessage(
            body=dq.popleft(), queue=queue, headers=self._headers[queue].popleft()
        )

    def ack(self, message: ReceivedMessage) -> None:
        # Already removed on pop; nothing to do.
        return None

    def size(self, queue: str) -> int:
        return len(self._queues.get(queue, ()))


def make_transport(broker_url: str) -> Transport:
    """Build a transport from a broker URL scheme (``memory://``, ``redis://``)."""
    scheme = broker_url.split("://", 1)[0] if "://" in broker_url else broker_url

    if scheme in ("", "memory"):
        return InMemoryTransport()
    if scheme in ("redis", "rediss"):
        from .redis_transport import RedisTransport

        return RedisTransport(broker_url)
    if scheme in ("amqp", "amqps"):
        from .pika_transport import PikaTransport

        return PikaTransport(broker_url)
    if scheme == "sqs":
        from .sqs_transport import SqsTransport

        return SqsTransport(broker_url)
    if scheme in ("sb", "servicebus", "azureservicebus"):
        from .asb_transport import AsbTransport

        return AsbTransport(broker_url)
    if scheme in ("pulsar", "pulsar+ssl"):
        from .pulsar_transport import PulsarTransport

        return PulsarTransport(broker_url)
    if scheme == "kafka":
        from .kafka_transport import KafkaTransport

        return KafkaTransport(broker_url)
    if scheme in ("artemis", "artemis+ssl"):
        from .artemis_transport import ArtemisTransport

        return ArtemisTransport(broker_url)

    raise BabelQueueError(
        f"Unsupported broker scheme {scheme!r}. Use 'memory://', 'redis://', "
        "'amqp://', 'sqs://', 'sb://', 'pulsar://', 'kafka://' or 'artemis://', or pass your "
        "own Transport via BabelQueue(transport=...)."
    )
