"""Optional DLQ redrive tooling (ADR-0026): safe replay off the dead-letter queue.

The Python mirror of the Go ``Redrive``. It reads dead-lettered messages off a DLQ and
re-publishes each to its source queue (its ``dead_letter.original_queue``) or a chosen
``to_queue``, **reset for reprocessing**: the ``dead_letter`` block is removed and ``attempts``
reset to 0, while ``job``, ``trace_id``, ``data`` and ``meta`` are preserved verbatim. It is
the operator-side counterpart to the runtime's dead-letter routing — the contract leaves
redrive to tooling, and this is that tool.

    from babelqueue import BabelQueue
    from babelqueue.redrive import redrive

    app = BabelQueue("redis://localhost:6379/0")
    result = redrive(app.transport, "orders.dlq")                        # back to each source
    result = redrive(app.transport, "orders.dlq", to_queue="sandbox")    # safe sandbox replay
    plan = redrive(app.transport, "orders.dlq", dry_run=True)            # inspect, change nothing

Messages are drained from the DLQ first and then processed, so restored messages (skipped,
dry-run, or undecodable) are never re-encountered in the same run; a DLQ message is
acknowledged only after a successful re-publish, and an undecodable body is restored, not
dropped.

Replay safety is sandbox routing (``to_queue``) + ``dry_run``, plus the **Replay-Bypass** guard
(``bypass=True``): it stamps a ``bq-replay-bypass`` transport header that the runtime surfaces to
handlers via :func:`babelqueue.is_replay` / :func:`babelqueue.bypass_external_effects`, so a
replay can skip external side-effects that already fired (don't re-charge, don't re-email) — see
:mod:`babelqueue.replay` (ADR-0027). The header rides out of band, so the envelope stays frozen;
it propagates over a real broker only once that broker's transport implements the optional
:class:`~babelqueue.transport.HeaderPublisher` capability (the in-memory transport does today).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .codec import EnvelopeCodec
from .replay import HEADER_REPLAY_BYPASS
from .transport import (
    NON_NUMERIC_ATTEMPTS_ROUTE,
    BatchPopper,
    HeaderPublisher,
    PoisonRoute,
    ReceivedMessage,
    Transport,
    publish_keeping_route,
)

Envelope = Mapping[str, Any]
Select = Callable[[Envelope], bool]


@dataclass
class RedriveItem:
    """What happened to one message during a redrive run."""

    message_id: str
    trace_id: str
    urn: str
    reason: str
    from_queue: str
    to: str  # target queue (the plan, even on a dry run; "" when skipped/undecodable)
    redriven: bool  # True only when actually re-published to ``to``
    bypassed: bool = False  # True when the bq-replay-bypass header was stamped on the message


@dataclass
class RedriveResult:
    """Summary of a redrive run."""

    redriven: int = 0
    skipped: int = 0
    items: List[RedriveItem] = field(default_factory=list)


def redrive(
    transport: Transport,
    dlq: str,
    *,
    to_queue: Optional[str] = None,
    max: int = 0,
    dry_run: bool = False,
    select: Optional[Select] = None,
    bypass: bool = False,
    timeout: float = 1.0,
) -> RedriveResult:
    """Move dead-lettered messages off ``dlq`` and replay them; see the module docstring."""
    # Drain up to ``max`` messages (or all available) before processing any of them.
    # A transport whose ``pop`` takes back an un-settled delivery (Kafka) hands the whole batch out
    # in one call instead, so the earlier messages are not popped a second time while the drain runs.
    if isinstance(transport, BatchPopper):
        popped = transport.pop_batch(dlq, max, timeout)
    else:
        popped = []
        while max == 0 or len(popped) < max:
            message = transport.pop(dlq, timeout)
            if message is None:
                break
            popped.append(message)
    batch: List[Tuple[ReceivedMessage, Optional[Dict[str, Any]]]] = [
        (message, None if isinstance(message.route_urn, PoisonRoute) else _decoded(message.body))
        for message in popped
    ]

    result = RedriveResult()
    for message, envelope in batch:
        # A record the transport flagged as poison (``PoisonRoute``: it never decoded into an envelope,
        # whatever its ``job`` looks like) is restored to the DLQ as-is rather than replayed or re-wrapped,
        # like the Go reference. A record the transport flagged as non-numeric-``attempts`` is not redrivable either: a
        # replay would reset it to a valid counter and quietly repair a foreign producer's bad
        # envelope, so it is restored to the DLQ like any other poison body.
        if envelope is None or message.route_urn == NON_NUMERIC_ATTEMPTS_ROUTE:
            publish_keeping_route(transport, dlq, message.body, message.route_urn, message.key)  # restore the poison body; never drop it
            transport.ack(message)
            result.skipped += 1
            result.items.append(RedriveItem("", "", "", "", dlq, "", False))
            continue

        meta_raw = envelope.get("meta")
        meta: Mapping[str, Any] = meta_raw if isinstance(meta_raw, Mapping) else {}
        dl_raw = envelope.get("dead_letter")
        dead_letter: Mapping[str, Any] = dl_raw if isinstance(dl_raw, Mapping) else {}
        item = RedriveItem(
            message_id=str(meta.get("id", "")),
            trace_id=str(envelope.get("trace_id", "")),
            urn=EnvelopeCodec.urn(envelope),
            reason=str(dead_letter.get("reason", "")),
            from_queue=dlq,
            to="",
            redriven=False,
        )

        if select is not None and not select(envelope):
            publish_keeping_route(transport, dlq, message.body, message.route_urn, message.key)  # not selected: restore unchanged
            transport.ack(message)
            result.skipped += 1
            result.items.append(item)
            continue

        target = to_queue or _source_queue_of(envelope)
        item.to = target

        if dry_run:
            publish_keeping_route(transport, dlq, message.body, message.route_urn, message.key)  # report the plan; restore unchanged
            transport.ack(message)
            result.skipped += 1
            result.items.append(item)
            continue

        reset = dict(envelope)
        reset.pop("dead_letter", None)
        reset["attempts"] = 0
        try:
            item.bypassed = _publish_redriven(
                transport, target, EnvelopeCodec.encode(reset), bypass, message.route_urn, message.key
            )
        except Exception:
            publish_keeping_route(transport, dlq, message.body, message.route_urn, message.key)  # restore on a publish failure, then surface
            transport.ack(message)
            raise
        transport.ack(message)
        item.redriven = True
        result.redriven += 1
        result.items.append(item)

    return result


def _decoded(body: str) -> Optional[Dict[str, Any]]:
    """Decode a DLQ body, or None when it is not a redrivable envelope.

    ``EnvelopeCodec.decode_lenient`` returns ``{}`` for malformed/non-object input; an object with no
    string ``job`` is likewise not redrivable.
    """
    envelope = EnvelopeCodec.decode_lenient(body)
    if not envelope or not isinstance(envelope.get("job"), str):
        return None
    return envelope


def _publish_redriven(
    transport: Transport,
    queue: str,
    body: str,
    bypass: bool,
    route_urn: str = "",
    key: Optional[bytes] = None,
) -> bool:
    """Re-publish a reset message to ``queue``. When ``bypass`` is set and the transport is a
    :class:`~babelqueue.transport.HeaderPublisher`, stamp the ``bq-replay-bypass`` header and
    return True; otherwise publish plainly and return False. Either way a non-empty ``route_urn``
    (the dead-lettered message's routing property) is carried back through
    :class:`~babelqueue.transport.RoutedPublisher` when the transport implements it, and the DLQ
    record's ``key`` (:attr:`ReceivedMessage.key`; Kafka, contract §6.5 "Record key on republish")
    is carried byte-for-byte through :class:`~babelqueue.transport.SettlePublisher` — a null key
    stays null, an empty key stays empty. The bypass-header path carries no record key (as in the
    Go reference)."""
    if bypass and isinstance(transport, HeaderPublisher):
        transport.publish_with_headers(queue, body, {HEADER_REPLAY_BYPASS: "1"})
        return True
    publish_keeping_route(transport, queue, body, route_urn, key)
    return False


def _source_queue_of(envelope: Envelope) -> str:
    """Default redrive target: ``dead_letter.original_queue``, falling back to ``meta.queue``."""
    dead_letter = envelope.get("dead_letter")
    if isinstance(dead_letter, Mapping):
        original = dead_letter.get("original_queue")
        if isinstance(original, str) and original:
            return original
    meta = envelope.get("meta")
    if isinstance(meta, Mapping):
        queue = meta.get("queue")
        if isinstance(queue, str):
            return queue
    return ""
