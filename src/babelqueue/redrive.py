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

Replay safety today is sandbox routing (``to_queue``) + ``dry_run``. The **Replay-Bypass**
guard — a ``bq-replay-bypass`` transport header surfaced to handlers so a replay can skip
external side-effects (don't re-charge, don't re-email) — is a documented phase two: like the
OpenTelemetry ``traceparent`` follow-up, it carries out-of-band metadata as a transport header
and so touches the runtime + every transport binding. Until then, sandbox routing is the
safe-replay answer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .codec import EnvelopeCodec
from .transport import ReceivedMessage, Transport

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
    timeout: float = 1.0,
) -> RedriveResult:
    """Move dead-lettered messages off ``dlq`` and replay them; see the module docstring."""
    # Drain up to ``max`` messages (or all available) before processing any of them.
    batch: List[Tuple[ReceivedMessage, Optional[Dict[str, Any]]]] = []
    while max == 0 or len(batch) < max:
        message = transport.pop(dlq, timeout)
        if message is None:
            break
        batch.append((message, _decoded(message.body)))

    result = RedriveResult()
    for message, envelope in batch:
        if envelope is None:
            transport.publish(dlq, message.body)  # restore the poison body; never drop it
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
            transport.publish(dlq, message.body)  # not selected: restore unchanged
            transport.ack(message)
            result.skipped += 1
            result.items.append(item)
            continue

        target = to_queue or _source_queue_of(envelope)
        item.to = target

        if dry_run:
            transport.publish(dlq, message.body)  # report the plan; restore unchanged
            transport.ack(message)
            result.skipped += 1
            result.items.append(item)
            continue

        reset = dict(envelope)
        reset.pop("dead_letter", None)
        reset["attempts"] = 0
        try:
            transport.publish(target, EnvelopeCodec.encode(reset))
        except Exception:
            transport.publish(dlq, message.body)  # restore on a publish failure, then surface
            transport.ack(message)
            raise
        transport.ack(message)
        item.redriven = True
        result.redriven += 1
        result.items.append(item)

    return result


def _decoded(body: str) -> Optional[Dict[str, Any]]:
    """Decode a DLQ body, or None when it is not a redrivable envelope.

    ``EnvelopeCodec.decode`` returns ``{}`` for malformed/non-object input; an object with no
    string ``job`` is likewise not redrivable.
    """
    envelope = EnvelopeCodec.decode(body)
    if not envelope or not isinstance(envelope.get("job"), str):
        return None
    return envelope


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
