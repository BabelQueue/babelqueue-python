"""Optional OpenTelemetry tracing: produce/consume spans correlated across hops, with true
cross-hop span parent-child linkage.

The Python mirror of the Go ``babelqueue-go/otel`` module. It emits a CONSUMER span per
handled message and a PRODUCER span per publish. Cross-hop trace propagation works at two
layered levels:

* **trace_id ↔ TraceID** (ADR-0025, v0.1): the envelope's ``trace_id`` — a UUID — maps 1:1 to a
  128-bit OTel trace id, so every hop that shares a ``trace_id`` shares one OTel trace
  (correlation + per-hop timing) with **zero** wire/transport change.
* **W3C ``traceparent``** (ADR-0028, v0.2): the producer also injects the active span context as
  a ``traceparent`` transport header (beside the frozen envelope, never in it), so the consumer
  starts its span as a **true child** of the producer span — real cross-hop parent-child linkage.
  This rides the out-of-band :class:`~babelqueue.transport.HeaderPublisher` /
  :func:`~babelqueue.headers.headers_from_context` seam (ADR-0027) and is available on any
  transport that carries headers. With no ``traceparent`` present it falls back to the v0.1
  ``trace_id`` behaviour — a strict, backward-compatible upgrade (no regression).

The wire envelope is untouched (GR-1) and the core never imports OpenTelemetry: this module is
only importable with the ``[otel]`` extra (``pip install babelqueue[otel]``), exactly like the
optional transport drivers.

    from opentelemetry import trace
    from babelqueue import BabelQueue, otel

    tracer = trace.get_tracer("orders")
    app = BabelQueue("redis://localhost:6379/0", queue="orders")
    app.register("urn:babel:orders:created", otel.wrap_handler(tracer, on_order_created))
    # producer side:
    otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 1042})
"""

from __future__ import annotations

import hashlib
import inspect
from typing import Any, Callable, Dict, Mapping, Optional

from opentelemetry.context import Context
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    SpanKind,
    TraceFlags,
    Tracer,
    get_current_span,
    set_span_in_context,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .headers import headers_from_context, merge_headers

Handler = Callable[..., None]

_SYSTEM = "babelqueue"
_MASK_128 = (1 << 128) - 1

#: The out-of-band transport headers that carry W3C Trace Context across a hop (ADR-0028). They
#: ride beside the frozen envelope on the transport's per-message metadata channel — the same
#: seam as the replay-bypass marker (ADR-0027) — so a consumer starts its span as a true child of
#: the producer's span, not merely share the ``trace_id``-derived trace. The envelope is untouched
#: (GR-1). ``traceparent``/``tracestate`` are exactly the W3C wire format, so a babelqueue header
#: interoperates with any OTel SDK or W3C-compliant peer.
HEADER_TRACEPARENT = "traceparent"
HEADER_TRACESTATE = "tracestate"

#: The W3C Trace Context propagator. It reads/writes the ``traceparent`` (and ``tracestate``)
#: headers — the exact wire format ADR-0028 names — so a babelqueue header interoperates with any
#: W3C peer. The propagator's default getter/setter operate on a plain ``dict``/``Mapping``, which
#: is exactly the shape of the SDK-owned transport-header map, so no custom carrier is needed.
_propagator = TraceContextTextMapPropagator()


def _inject_traceparent(context: Optional[Context]) -> Dict[str, str]:
    """Write the active span context (in ``context``) as W3C ``traceparent`` (and ``tracestate``)
    into a fresh header map. The producer half: the result is handed to a
    :class:`~babelqueue.transport.HeaderPublisher` so the consumer can reconstruct the remote
    parent. With no valid span context the propagator writes nothing and the map stays empty (so a
    no-trace publish stays header-free)."""
    carrier: Dict[str, str] = {}
    _propagator.inject(carrier, context=context)
    return carrier


def _remote_parent_from_headers() -> Optional[Context]:
    """Extract a W3C ``traceparent`` from the out-of-band transport headers surfaced on the
    current context (:func:`~babelqueue.headers.headers_from_context`) and return a
    :class:`~opentelemetry.context.Context` carrying the remote parent span context, or ``None``
    when no valid ``traceparent`` is present.

    The consumer half of true cross-hop parent-child linkage: a span started from the returned
    context is a child of the producer's span (remote parent). ``None`` signals the caller to fall
    back to the v0.1 ``trace_id``-derived parent (ADR-0025 Option 1)."""
    headers = headers_from_context()
    if not headers.get(HEADER_TRACEPARENT):
        return None
    extracted = _propagator.extract(dict(headers))
    span_context = get_current_span(extracted).get_span_context()
    if not span_context.is_valid:
        return None
    return extracted


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
    """Wrap a consume handler to emit a CONSUMER span per message.

    Register it like any handler: ``app.register(urn, wrap_handler(tracer, handler))``. The
    wrapper's ``*args`` signature makes the runtime pass the full envelope (``data, meta,
    envelope``), so it can read ``trace_id``/``job`` even when the inner handler only wants
    ``(data, meta)``. A raising handler records the exception on the span and re-raises, so the
    runtime's retry / dead-letter path still applies.

    **Parent selection** (ADR-0028): when the producer carried a W3C ``traceparent`` on the
    transport (surfaced by the runtime via :func:`~babelqueue.headers.headers_from_context`), the
    span is started as a true **child** of the producer's span — real cross-hop parent-child
    linkage with per-hop span timing. With no ``traceparent`` present it falls back to the v0.1
    behaviour: a remote parent derived from the envelope's ``trace_id`` (ADR-0025 Option 1), which
    shares the trace but not the exact span link. So enabling propagation is a strict,
    backward-compatible upgrade — no regression for messages produced without it.
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
        # Prefer a true remote parent from a carried W3C traceparent (v0.2); else fall back to the
        # trace_id-derived parent (v0.1). A header-less / malformed traceparent yields None.
        context = _remote_parent_from_headers()
        if context is None and trace_id:
            context = _parent_context(trace_id)

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
    """Publish via a PRODUCER span ``publish <urn>``, propagating the trace downstream two ways
    (ADR-0028):

    * It injects the active span context as a W3C ``traceparent`` (and ``tracestate``) onto the
      outgoing transport headers, so a consumer can start its span as a true **child** of this
      producer span — real cross-hop parent-child linkage. The header rides beside the frozen
      envelope, never in it (GR-1), via ``app.publish_with_headers``; if the transport can't carry
      headers it degrades to a plain publish (the ``traceparent`` is simply not propagated — no
      error).
    * It also carries the active trace's id into the message's ``trace_id`` (the v0.1 behaviour),
      so even a consumer that ignores the header — or a transport that drops it — still recovers
      the same trace (correlation without exact span linkage).

    Behaves like ``app.publish`` (returns the message id). ``app`` is any object exposing
    ``publish(urn, data, *, queue=None, trace_id=None) -> str``; when it also exposes
    ``publish_with_headers(urn, data, headers, *, queue=None, trace_id=None) -> str`` the
    ``traceparent`` is propagated, otherwise it transparently falls back to ``publish``.
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
        headers = merge_headers(_inject_traceparent(set_span_in_context(span)))
        publish_with_headers = getattr(app, "publish_with_headers", None)
        if headers and callable(publish_with_headers):
            message_id = publish_with_headers(
                urn, data, headers, queue=queue, trace_id=trace_id
            )
        else:
            message_id = app.publish(urn, data, queue=queue, trace_id=trace_id)
        span.set_attribute("messaging.message.id", message_id)
        return message_id
