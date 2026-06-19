"""Optional OpenTelemetry tracing (ADR-0025): produce/consume spans correlated across hops.

The Python mirror of the Go ``babelqueue-go/otel`` module. It emits a CONSUMER span per
handled message and a PRODUCER span per publish, correlating them across every hop and SDK
through the envelope's ``trace_id`` — a UUID, which maps 1:1 to a 128-bit OTel trace id. The
wire envelope is untouched (GR-1) and the core never imports OpenTelemetry: this module is
only importable with the ``[otel]`` extra (``pip install babelqueue[otel]``), exactly like the
optional transport drivers.

    from opentelemetry import trace
    from babelqueue import BabelQueue, otel

    tracer = trace.get_tracer("orders")
    app = BabelQueue("redis://localhost:6379/0", queue="orders")
    app.register("urn:babel:orders:created", otel.wrap_handler(tracer, on_order_created))
    # producer side:
    otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 1042})

Every hop that shares a ``trace_id`` shares one OTel trace. Exact cross-hop *span*
parent-child linkage (W3C ``traceparent`` as a transport header) is a documented follow-up.
"""

from __future__ import annotations

import hashlib
import inspect
from typing import Any, Callable, Mapping, Optional

from opentelemetry.context import Context
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    TraceFlags,
    Tracer,
    set_span_in_context,
)

Handler = Callable[..., None]

_SYSTEM = "babelqueue"
_MASK_128 = (1 << 128) - 1


def trace_id_of(trace_id: str) -> int:
    """Map an envelope ``trace_id`` to a deterministic 128-bit OTel trace id.

    A UUID maps to its 16 raw bytes; any other string is hashed (SHA-256, first 16 bytes).
    The result is never zero (OTel's invalid trace id). The inverse of :func:`uuid_of` for
    the UUID case.
    """
    raw = _uuid_bytes(trace_id)
    if raw is not None:
        n = int.from_bytes(raw, "big")
        if n != 0:
            return n
    digest = hashlib.sha256(trace_id.encode("utf-8")).digest()[:16]
    return int.from_bytes(digest, "big") or 1


def uuid_of(trace_id_int: int) -> str:
    """Format a 128-bit OTel trace id as a canonical UUID string.

    The form a producer stamps into the message's ``trace_id`` so a consumer can recover the
    same trace id via :func:`trace_id_of`.
    """
    h = format(trace_id_int & _MASK_128, "032x")
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _uuid_bytes(s: str) -> Optional[bytes]:
    h = s.replace("-", "")
    if len(h) != 32:
        return None
    try:
        return bytes.fromhex(h)
    except ValueError:
        return None


def _span_id_of(trace_id: str) -> int:
    """Derive a deterministic, non-zero 64-bit span id so the remote parent context is valid
    (a span needs a valid parent to inherit a specific trace)."""
    digest = hashlib.sha256(("babelqueue-span:" + trace_id).encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, "big") or 1


def _parent_context(trace_id: str) -> Context:
    sc = SpanContext(
        trace_id=trace_id_of(trace_id),
        span_id=_span_id_of(trace_id),
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return set_span_in_context(NonRecordingSpan(sc))


def _wants_envelope(fn: Handler) -> bool:
    """True if ``fn`` takes a 3rd positional arg (the full envelope) or ``*args``."""
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):  # pragma: no cover - builtins / C callables
        return False
    positional = [
        p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    return any(p.kind == p.VAR_POSITIONAL for p in params) or len(positional) >= 3


def wrap_handler(tracer: Tracer, handler: Handler) -> Handler:
    """Wrap a consume handler to emit a CONSUMER span per message, in the OTel trace derived
    from the envelope's ``trace_id``.

    Register it like any handler: ``app.register(urn, wrap_handler(tracer, handler))``. The
    wrapper's ``*args`` signature makes the runtime pass the full envelope (``data, meta,
    envelope``), so it can read ``trace_id``/``job`` even when the inner handler only wants
    ``(data, meta)``. A raising handler records the exception on the span and re-raises, so the
    runtime's retry / dead-letter path still applies.
    """

    def wrapped(*args: Any) -> None:
        envelope = args[2] if len(args) > 2 and isinstance(args[2], Mapping) else {}
        meta = args[1] if len(args) > 1 and isinstance(args[1], Mapping) else {}
        trace_id = str(envelope.get("trace_id") or "")
        urn = str(envelope.get("job") or envelope.get("urn") or "")

        attributes: dict[str, Any] = {
            "messaging.system": _SYSTEM,
            "messaging.operation": "process",
            "messaging.destination.name": str(meta.get("queue") or ""),
            "messaging.message.id": str(meta.get("id") or ""),
            "messaging.message.conversation_id": trace_id,
            "messaging.babelqueue.attempts": int(envelope.get("attempts", 0) or 0),
        }
        context = _parent_context(trace_id) if trace_id else None

        with tracer.start_as_current_span(
            "process " + urn,
            context=context,
            kind=SpanKind.CONSUMER,
            attributes=attributes,
        ):
            if _wants_envelope(handler):
                handler(*args)
            else:
                handler(args[0], args[1])

    return wrapped


def publish(
    tracer: Tracer,
    app: Any,
    urn: str,
    data: Mapping[str, Any],
    *,
    queue: Optional[str] = None,
) -> str:
    """Publish via a PRODUCER span ``publish <urn>``, carrying the active trace's id into the
    message's ``trace_id`` so the downstream consumer recovers the same trace.

    Behaves like ``app.publish`` (returns the message id); ``app`` is any object exposing
    ``publish(urn, data, *, queue=None, trace_id=None) -> str``.
    """
    attributes = {
        "messaging.system": _SYSTEM,
        "messaging.operation": "publish",
        "messaging.destination.name": urn,
    }
    with tracer.start_as_current_span(
        "publish " + urn, kind=SpanKind.PRODUCER, attributes=attributes
    ) as span:
        trace_id = uuid_of(span.get_span_context().trace_id)
        message_id = app.publish(urn, data, queue=queue, trace_id=trace_id)
        span.set_attribute("messaging.message.id", message_id)
        return message_id
