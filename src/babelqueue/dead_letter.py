"""Builds the additive ``dead_letter`` block attached when a message is
dead-lettered. Pure: returns an annotated copy; the original identity
(trace_id, meta.id, data) is preserved. Because the field is additive and
optional, the envelope stays at schema_version 1. See https://babelqueue.com.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional, Protocol, runtime_checkable

from .codec import SOURCE_LANG

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .transport import ReceivedMessage


def annotate(
    envelope: Mapping[str, Any],
    reason: str,
    original_queue: str,
    attempts: int,
    *,
    error: Optional[str] = None,
    exception: Optional[str] = None,
    lang: str = SOURCE_LANG,
) -> Dict[str, Any]:
    """Return a copy of ``envelope`` with a ``dead_letter`` block.

    ``reason`` is one of ``failed`` | ``unknown_urn`` | ``poison``.
    """
    result = dict(envelope)
    result["dead_letter"] = {
        "reason": reason,
        "error": error,
        "exception": exception,
        "failed_at": int(time.time() * 1000),
        "original_queue": original_queue,
        "attempts": attempts,
        "lang": lang,
    }
    return result


@dataclass(frozen=True)
class DeadLetterEvent:
    """What the runtime tells a :data:`DeadLetterObserver` about one dead-letter it carried out
    (ADR-0039 section 5.1).

    ``queue`` is the source queue the message was consumed from (``meta.queue``, else the queue
    the consumer read it from). ``dead_letter_queue`` is the DLQ the record was written to; it is
    empty for a native dead-letter (the broker's own ``$DeadLetterQueue`` sub-entity). ``reason``
    is the ``dead_letter.reason`` the SDK wrote (``failed`` | ``unknown_urn`` | ``poison``), empty
    for a raw body that carries no ``dead_letter`` block.
    """

    queue: str
    dead_letter_queue: str
    reason: str


#: Optional callback registered on :class:`~babelqueue.app.BabelQueue` (``dead_letter_observer=``).
#: The runtime calls it once per dead-letter it carried out itself, **after** the DLQ write (or the
#: native dead-letter) is confirmed. An exception it raises is logged and never changes the settle
#: outcome. The core stays dependency-free: ``babelqueue.otel.dead_letter_observer`` is the one
#: implementation that turns the event into the ``babelqueue.dlq.messages`` counter.
DeadLetterObserver = Callable[[DeadLetterEvent], None]


@runtime_checkable
class NativeDeadLetterSettler(Protocol):
    """Optional :class:`~babelqueue.transport.Transport` capability next to
    :class:`~babelqueue.transport.NativeSettler`: the native settle that also reports whether it
    dead-lettered (ADR-0039 section 5.1).

    The runtime prefers it over ``settle_native`` and calls the dead-letter observer only when it
    returns ``True``: Azure Service Bus returns ``True`` when it dead-lettered the message into
    ``$DeadLetterQueue`` and ``False`` when it only abandoned the lock; the SQS no-op does not
    implement it, so it is never counted. ``NativeSettler`` itself is unchanged.
    """

    def settle_native_dead_letter(
        self, message: "ReceivedMessage", reason: str, description: str
    ) -> bool:
        """Settle ``message`` natively; return ``True`` only when it was really dead-lettered."""
        ...
