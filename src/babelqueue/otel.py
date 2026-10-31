"""Optional OpenTelemetry tracing and metrics: produce/consume spans correlated across hops, with
true cross-hop span parent-child linkage, plus the standard metric set.

The Python mirror of the Go ``babelqueue-go/otel`` module. It emits a CONSUMER span per
handled message and a PRODUCER span per publish, in the OpenTelemetry messaging semantic
conventions v1.44.0 shape (ADR-0038, amending ADR-0025):

* span names are ``send <queue>`` / ``process <queue>`` (never the URN: it is unbounded, so it
  lives in the ``babelqueue.urn`` attribute);
* ``messaging.system`` is the broker, supplied by the caller (``system="kafka"``); an absent
  ``system`` keeps the deprecated ``babelqueue`` default for one MINOR;
* ``messaging.operation.type`` / ``.name``, ``messaging.destination.name``,
  ``babelqueue.urn`` / ``.schema_version`` / ``.attempts`` and, on a failure, ``error.type`` with
  an explicit ``ERROR`` status;
* the deprecated ``messaging.operation`` and ``messaging.babelqueue.attempts`` are dual-emitted
  for this MINOR and removed in the next.

Pass ``meter=`` to also record the ADR-0039 metrics (``messaging.process.duration``,
``messaging.client.sent.messages``, ``messaging.client.consumed.messages``,
``babelqueue.retries``) and use :func:`dead_letter_observer` for ``babelqueue.dlq.messages``.
With no meter nothing is recorded. Create the tracer and meter with :data:`SEMCONV_SCHEMA_URL`
(``trace.get_tracer(name, schema_url=otel.SEMCONV_SCHEMA_URL)``) so they carry the pinned schema.

Cross-hop trace propagation works at two layered levels:

* **trace_id ↔ TraceID** (ADR-0025, v0.1): the envelope's ``trace_id`` — a UUID — maps 1:1 to a
  128-bit OTel trace id, so every hop that shares a ``trace_id`` shares one OTel trace
  (correlation + per-hop timing) with **zero** wire/transport change. A ``trace_id`` that is not
  a UUID is preserved byte for byte and hashed for the mapping (ADR-0038 section 7); the hash
  logs a warning once per distinct value.
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

    tracer = trace.get_tracer("orders", schema_url=otel.SEMCONV_SCHEMA_URL)
    app = BabelQueue("redis://localhost:6379/0", queue="orders")
    app.register("urn:babel:orders:created", otel.wrap_handler(tracer, on_order_created, system="redis"))
    # producer side:
    otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 1042}, system="redis")
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import time
import weakref
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

from opentelemetry.context import Context
from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.trace import (
    NonRecordingSpan,
    Span,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
    Tracer,
    get_current_span,
    set_span_in_context,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .codec import ASCII_WHITESPACE, SCHEMA_VERSION
from .dead_letter import DeadLetterEvent, DeadLetterObserver
from .headers import headers_from_context, merge_headers

Handler = Callable[..., None]

logger = logging.getLogger("babelqueue.otel")

#: The OpenTelemetry semantic-conventions release these spans and metrics follow (ADR-0038 section 1).
SEMCONV_VERSION = "1.44.0"
#: Its schema URL. The caller creates the tracer and the meter, so pass this when getting them:
#: ``trace.get_tracer("orders", schema_url=otel.SEMCONV_SCHEMA_URL)`` (this module never wraps or
#: replaces a caller-supplied tracer or meter).
SEMCONV_SCHEMA_URL = "https://opentelemetry.io/schemas/" + SEMCONV_VERSION

#: The ``messaging.system`` written when the caller gives no ``system``. Deprecated by ADR-0038
#: section 5: pass the broker (``redis``, ``rabbitmq``, ``aws_sqs``, ``servicebus``, ``pulsar``,
#: ``kafka``, ``activemq``); what an absent ``system`` means afterwards is not decided yet.
DEFAULT_SYSTEM = "babelqueue"
_MASK_128 = (1 << 128) - 1

#: ``messaging.process.duration`` advisory bucket boundaries (semconv v1.44.0), applied where the
#: installed OpenTelemetry API supports instrument advice.
_DURATION_BUCKETS: List[float] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
]

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
    direct = _direct_trace_id(trace_id)
    if direct is not None:
        return direct
    digest = hashlib.sha256(trace_id.encode("utf-8")).digest()[:16]
    return int.from_bytes(digest, "big") or 1


def _direct_trace_id(trace_id: str) -> Optional[int]:
    """The 128-bit id a UUID-shaped ``trace_id`` decodes to, or ``None`` when the mapping has to
    hash it instead (not 32 hex digits once the hyphens are removed, or all zero: not a valid
    TraceID)."""
    raw = _uuid_bytes(trace_id)
    if raw is None:
        return None
    return int.from_bytes(raw, "big") or None


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


#: Distinct non-UUID ``trace_id`` values already warned about (ADR-0038 section 7: the warning may
#: be rate-limited to once per distinct value). Bounded, so a producer minting a fresh opaque
#: ``trace_id`` per message cannot grow it without limit.
_WARNED_TRACE_IDS: Set[str] = set()
_WARNED_TRACE_IDS_MAX = 1024


def _warn_if_hashed(trace_id: str) -> None:
    """Log a warning, once per distinct value, when the OTel mapping has to hash ``trace_id``
    because it is not a UUID (ADR-0038 section 7). The value itself is preserved and forwarded
    unchanged; only its OTel TraceID is derived from a hash."""
    if _direct_trace_id(trace_id) is not None or trace_id in _WARNED_TRACE_IDS:
        return
    if len(_WARNED_TRACE_IDS) >= _WARNED_TRACE_IDS_MAX:
        _WARNED_TRACE_IDS.clear()
    _WARNED_TRACE_IDS.add(trace_id)
    shown = trace_id if len(trace_id) <= 64 else trace_id[:64] + "..."
    logger.warning(
        "trace_id %r is not a UUID; the OpenTelemetry TraceID is derived from a SHA-256 hash of it. "
        "The trace_id itself is preserved and forwarded unchanged.",
        shown,
    )


def _parent_context(trace_id: str) -> Context:
    _warn_if_hashed(trace_id)
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




# ---------------------------------------------------------------------------------------------
# Span / metric attribute helpers (ADR-0038 section 4, ADR-0039 section 2)
# ---------------------------------------------------------------------------------------------

_warned_default_system = False


def _resolve_system(system: Optional[str]) -> str:
    """The ``messaging.system`` value: the caller's, else the deprecated ``babelqueue`` default.
    An empty string counts as absent; any other value is written as given, never validated."""
    return system if system else DEFAULT_SYSTEM


def _warn_default_system(system: Optional[str], where: str) -> None:
    """The one-time deprecation warning for an absent ``system`` (ADR-0038 section 5)."""
    if system:
        return
    logger.warning(
        "babelqueue.otel.%s was given no system=: messaging.system defaults to %r, which is "
        "deprecated. Pass the broker (e.g. system=%r).",
        where, DEFAULT_SYSTEM, "kafka",
    )


def _destination(queue: Any) -> Optional[str]:
    """``queue`` as the ``messaging.destination.name`` value, or ``None`` when it is absent, empty
    or blank under the ASCII whitespace set (no destination: the span name is the bare operation)."""
    if queue is None:
        return None
    text = queue if isinstance(queue, str) else str(queue)
    return text if text.strip(ASCII_WHITESPACE) else None


def _span_name(operation: str, destination: Optional[str]) -> str:
    return operation + " " + destination if destination else operation


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _error_type(exc: BaseException) -> str:
    """The ``error.type`` value (ADR-0038 section 6): ``module.QualName`` of the error's class
    (just the qualified name for builtins), or ``_OTHER`` when no type name can be obtained."""
    try:
        cls = type(exc)
        name = cls.__qualname__
        module = cls.__module__
    except Exception:  # noqa: BLE001 - defensive: the fallback is the contract
        return "_OTHER"
    if not name:
        return "_OTHER"
    return name if module in ("builtins", "", None) else module + "." + name


def _fail(span: Span, exc: BaseException) -> str:
    """Record ``exc`` on ``span``: the exception event, an explicit ``ERROR`` status whose
    description is the message, and ``error.type``. Returns the ``error.type`` value."""
    error_type = _error_type(exc)
    span.record_exception(exc)
    span.set_status(Status(StatusCode.ERROR, str(exc) or None))
    span.set_attribute("error.type", error_type)
    return error_type


def _metric_attributes(
    system: str, operation: str, destination: Optional[str], error_type: Optional[str] = None
) -> Dict[str, str]:
    """The exact metric attribute set (ADR-0039 section 2). Never the URN, a message or trace id,
    the attempt count, the schema version or a dead-letter reason: those are unbounded."""
    attributes = {"messaging.system": system, "messaging.operation.name": operation}
    if destination:
        attributes["messaging.destination.name"] = destination
    if error_type:
        attributes["error.type"] = error_type
    return attributes


#: ``meter -> instrument name -> instrument``: a function-style API (``publish``) would otherwise
#: create its instruments again on every call. Weak, so a dropped meter does not leak.
_INSTRUMENTS: "weakref.WeakKeyDictionary[Any, Dict[str, Any]]" = weakref.WeakKeyDictionary()


def _instrument(meter: Meter, name: str, factory: Callable[[], Any]) -> Any:
    try:
        cached = _INSTRUMENTS.setdefault(meter, {})
    except TypeError:  # a meter that cannot be weakly referenced: no cache, still correct
        return factory()
    if name not in cached:
        cached[name] = factory()
    return cached[name]


def _counter(meter: Meter, name: str, description: str) -> Counter:
    return _instrument(
        meter, name, lambda: meter.create_counter(name, unit="{message}", description=description)
    )


def _duration_histogram(meter: Meter) -> Histogram:
    name = "messaging.process.duration"
    description = "Duration of processing operations."

    def create() -> Histogram:
        try:
            return meter.create_histogram(
                name, unit="s", description=description,
                explicit_bucket_boundaries_advisory=_DURATION_BUCKETS,
            )
        except TypeError:  # an OpenTelemetry API without instrument advice: the reader's defaults
            return meter.create_histogram(name, unit="s", description=description)

    return _instrument(meter, name, create)


def _sent_counter(meter: Meter) -> Counter:
    return _counter(meter, "messaging.client.sent.messages", "Number of messages producer attempted to send to the broker.")


def _consumed_counter(meter: Meter) -> Counter:
    return _counter(meter, "messaging.client.consumed.messages", "Number of messages that were delivered to the application.")


def _retries_counter(meter: Meter) -> Counter:
    return _counter(meter, "babelqueue.retries", "Number of deliveries that were a retry (attempts > 0).")


def _dlq_counter(meter: Meter) -> Counter:
    return _counter(meter, "babelqueue.dlq.messages", "Number of messages the SDK dead-lettered.")


# ---------------------------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------------------------


def wrap_handler(
    tracer: Tracer,
    handler: Handler,
    *,
    system: Optional[str] = None,
    consumer_group: Optional[str] = None,
    meter: Optional[Meter] = None,
) -> Handler:
    """Wrap a consume handler to emit a CONSUMER span per message.

    Register it like any handler: ``app.register(urn, wrap_handler(tracer, handler,
    system="kafka"))``. The wrapper's ``*args`` signature makes the runtime pass the full envelope
    (``data, meta, envelope``), so it can read ``trace_id``/``job`` even when the inner handler
    only wants ``(data, meta)``. A raising handler records the exception, sets the span status to
    ``ERROR`` with ``error.type``, and re-raises, so the runtime's retry / dead-letter path still
    applies.

    **Span shape** (ADR-0038): kind ``CONSUMER``, name ``process <meta.queue>`` (the bare
    ``process`` when ``meta.queue`` is absent or blank), ``messaging.operation.type`` /
    ``.name`` ``process``, ``messaging.destination.name``, ``messaging.message.id`` (when present),
    ``messaging.message.conversation_id`` (the ``trace_id`` byte for byte),
    ``babelqueue.urn`` / ``.schema_version`` / ``.attempts`` and the deprecated dual-emit keys
    ``messaging.operation`` and ``messaging.babelqueue.attempts``.

    ``system`` is the ``messaging.system`` value (the broker: ``redis``, ``rabbitmq``, ``aws_sqs``,
    ``servicebus``, ``pulsar``, ``kafka``, ``activemq``; any other value is written as given).
    Absent or empty keeps the deprecated ``babelqueue`` and logs a one-time warning.
    ``consumer_group`` is written as ``messaging.consumer.group.name`` when given.

    ``meter`` enables the ADR-0039 metrics, created once for this wrapper: per delivery
    ``messaging.client.consumed.messages`` +1, ``babelqueue.retries`` +1 when ``attempts > 0``, and
    one ``messaging.process.duration`` observation (with ``error.type`` if the handler failed).
    With no ``meter`` nothing is recorded.

    **Parent selection** (ADR-0028, unchanged): when the producer carried a W3C ``traceparent`` on
    the transport (surfaced by the runtime via :func:`~babelqueue.headers.headers_from_context`),
    the span is started as a true **child** of the producer's span. With no ``traceparent`` it
    falls back to a remote parent derived from the envelope's ``trace_id`` (ADR-0025 Option 1),
    which shares the trace but not the exact span link.
    """
    resolved_system = _resolve_system(system)
    _warn_default_system(system, "wrap_handler")
    duration: Optional[Histogram] = None
    consumed: Optional[Counter] = None
    retries: Optional[Counter] = None
    if meter is not None:
        duration = _duration_histogram(meter)
        consumed = _consumed_counter(meter)
        retries = _retries_counter(meter)

    def wrapped(*args: Any) -> None:
        envelope = args[2] if len(args) > 2 and isinstance(args[2], Mapping) else {}
        meta = args[1] if len(args) > 1 and isinstance(args[1], Mapping) else {}
        trace_id = str(envelope.get("trace_id") or "")
        urn = str(envelope.get("job") or envelope.get("urn") or "")
        destination = _destination(meta.get("queue"))
        attempts = _as_int(envelope.get("attempts"))

        attributes: Dict[str, Any] = {
            "messaging.system": resolved_system,
            "messaging.operation.type": "process",
            "messaging.operation.name": "process",
            "messaging.message.conversation_id": trace_id,
            "babelqueue.urn": urn,
            "babelqueue.schema_version": _as_int(meta.get("schema_version"), SCHEMA_VERSION),
            "babelqueue.attempts": attempts,
            # Deprecated keys, dual-emitted for this MINOR (ADR-0038 section 5).
            "messaging.operation": "process",
            "messaging.babelqueue.attempts": attempts,
        }
        if destination:
            attributes["messaging.destination.name"] = destination
        message_id = meta.get("id")
        if message_id is not None and str(message_id) != "":
            attributes["messaging.message.id"] = str(message_id)
        if consumer_group:
            attributes["messaging.consumer.group.name"] = consumer_group

        metric_base = _metric_attributes(resolved_system, "process", destination)
        if consumed is not None:
            consumed.add(1, metric_base)
        if retries is not None and attempts > 0:
            retries.add(1, metric_base)

        # Prefer a true remote parent from a carried W3C traceparent (v0.2); else fall back to the
        # trace_id-derived parent (v0.1). A header-less / malformed traceparent yields None.
        context = _remote_parent_from_headers()
        if context is None and trace_id:
            context = _parent_context(trace_id)

        # The span is failed explicitly below (exception event, ERROR status, error.type), so the
        # SDK's own on-exception handling is off to avoid recording the error twice.
        with tracer.start_as_current_span(
            _span_name("process", destination),
            context=context,
            kind=SpanKind.CONSUMER,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            error_type: Optional[str] = None
            started = time.perf_counter()
            try:
                if _wants_envelope(handler):
                    handler(*args)
                else:
                    handler(args[0], args[1])
            except Exception as exc:
                error_type = _fail(span, exc)
                raise
            finally:
                if duration is not None:
                    duration.record(
                        max(0.0, time.perf_counter() - started),
                        _metric_attributes(resolved_system, "process", destination, error_type),
                    )

    return wrapped


# ---------------------------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------------------------


def _send(
    app: Any, urn: str, data: Mapping[str, Any], queue: Optional[str], trace_id: str,
    headers: Dict[str, str],
) -> Tuple[Optional[Mapping[str, Any]], Optional[str], Optional[Exception]]:
    """Publish through ``app`` and return ``(envelope, message_id, error)``.

    Prefers :meth:`~babelqueue.app.BabelQueue.publish_envelope`, which hands back the envelope that
    was given to the transport even when the transport fails. An ``app`` without it (any duck-typed
    ``publish(...) -> str`` object) is still supported: the envelope is then unknown, and the id
    is the returned one."""
    publish_envelope = getattr(app, "publish_envelope", None)
    if callable(publish_envelope):
        envelope, error = publish_envelope(
            urn, data, queue=queue, trace_id=trace_id, headers=headers or None
        )
        message_id = envelope["meta"]["id"] if envelope is not None and error is None else None
        return envelope, message_id, error
    try:
        publish_with_headers = getattr(app, "publish_with_headers", None)
        if headers and callable(publish_with_headers):
            return None, publish_with_headers(urn, data, headers, queue=queue, trace_id=trace_id), None
        return None, app.publish(urn, data, queue=queue, trace_id=trace_id), None
    except Exception as exc:  # noqa: BLE001 - recorded on the span, re-raised below
        return None, None, exc


def publish(
    tracer: Tracer,
    app: Any,
    urn: str,
    data: Mapping[str, Any],
    *,
    queue: Optional[str] = None,
    system: Optional[str] = None,
    meter: Optional[Meter] = None,
) -> str:
    """Publish via a PRODUCER span ``send <queue>``, propagating the trace downstream two ways
    (ADR-0028):

    * It injects the active span context as a W3C ``traceparent`` (and ``tracestate``) onto the
      outgoing transport headers, so a consumer can start its span as a true **child** of this
      producer span — real cross-hop parent-child linkage. The header rides beside the frozen
      envelope, never in it (GR-1); if the transport can't carry headers it degrades to a plain
      publish (the ``traceparent`` is simply not propagated — no error).
    * It also carries the active trace's id into the message's ``trace_id`` (the v0.1 behaviour),
      so even a consumer that ignores the header — or a transport that drops it — still recovers
      the same trace (correlation without exact span linkage).

    **Span shape** (ADR-0038): kind ``PRODUCER``, ``messaging.operation.type`` / ``.name``
    ``send``, ``messaging.destination.name`` = the queue the message is published to (the produced
    envelope's ``meta.queue``; ``queue`` or else ``app.queue`` until it is known), ``babelqueue.urn``,
    ``babelqueue.schema_version``, and ``messaging.message.id`` /
    ``messaging.message.conversation_id`` from the produced envelope, also when the publish fails.
    The span name is ``send <queue>`` (the bare ``send`` when there is no queue). A failing
    publish sets the span status to ``ERROR`` with ``error.type`` and re-raises the error
    unchanged. ``system`` is the ``messaging.system`` value, as for :func:`wrap_handler`.
    ``meter`` enables ``messaging.client.sent.messages`` (+1 per attempt, with ``error.type`` on
    failure).

    Behaves like ``app.publish`` (returns the message id). ``app`` is normally a
    :class:`~babelqueue.app.BabelQueue`, whose :meth:`~babelqueue.app.BabelQueue.publish_envelope`
    is used; any object exposing ``publish(urn, data, *, queue=None, trace_id=None) -> str`` (and
    optionally ``publish_with_headers``) still works, but then the produced envelope is not
    visible, so a failed publish has no ``messaging.message.id``.
    """
    resolved_system = _resolve_system(system)
    if not system:
        _warn_default_system_once()
    sent = _sent_counter(meter) if meter is not None else None

    # The queue is resolved the way BabelQueue.publish resolves it (``queue or app.queue``), so the
    # span can be named before the envelope exists; the produced meta.queue refines it below.
    destination = _destination(queue or getattr(app, "queue", None))
    attributes: Dict[str, Any] = {
        "messaging.system": resolved_system,
        "messaging.operation.type": "send",
        "messaging.operation.name": "send",
        "babelqueue.urn": urn,
        "babelqueue.schema_version": SCHEMA_VERSION,
        # Deprecated key, dual-emitted for this MINOR (ADR-0038 section 5).
        "messaging.operation": "publish",
    }
    if destination:
        attributes["messaging.destination.name"] = destination

    error: Optional[Exception]
    with tracer.start_as_current_span(
        _span_name("send", destination),
        kind=SpanKind.PRODUCER,
        attributes=attributes,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        trace_id = uuid_of(span.get_span_context().trace_id)
        headers = merge_headers(_inject_traceparent(set_span_in_context(span)))
        envelope, message_id, error = _send(app, urn, data, queue, trace_id, headers)

        if envelope is not None:
            produced = envelope.get("meta") or {}
            produced_destination = _destination(produced.get("queue"))
            if produced_destination and produced_destination != destination:
                destination = produced_destination
                span.update_name(_span_name("send", destination))
                span.set_attribute("messaging.destination.name", destination)
            if produced.get("id"):
                span.set_attribute("messaging.message.id", str(produced["id"]))
            span.set_attribute("messaging.message.conversation_id", str(envelope.get("trace_id") or ""))
        elif message_id is not None:
            span.set_attribute("messaging.message.id", str(message_id))
            span.set_attribute("messaging.message.conversation_id", trace_id)

        error_type = _fail(span, error) if error is not None else None
        if sent is not None:
            sent.add(1, _metric_attributes(resolved_system, "send", destination, error_type))
    if error is not None:
        raise error
    assert message_id is not None  # no error means the publish returned an id
    return message_id


def _warn_default_system_once() -> None:
    """``publish`` is a function, not a wrapper instance, so its deprecation warning is per
    process rather than per instance."""
    global _warned_default_system
    if _warned_default_system:
        return
    _warned_default_system = True
    _warn_default_system(None, "publish")


# ---------------------------------------------------------------------------------------------
# Dead-letter metric (ADR-0039 section 5)
# ---------------------------------------------------------------------------------------------


def dead_letter_observer(
    meter: Optional[Meter], *, system: Optional[str] = None
) -> DeadLetterObserver:
    """The :data:`~babelqueue.dead_letter.DeadLetterObserver` that counts ``babelqueue.dlq.messages``.

    Pass the result to ``BabelQueue(..., dead_letter_observer=...)``. Each dead-letter the runtime
    carried out (a confirmed BabelQueue DLQ write or an SDK-initiated native dead-letter) adds 1
    with ``messaging.system`` (``system``, else the deprecated ``babelqueue``),
    ``messaging.operation.name`` ``process`` and ``messaging.destination.name`` = the source queue
    (when known). The reason is deliberately not an attribute (cardinality). With ``meter=None``
    the observer records nothing.
    """
    if meter is None:
        return _ignore_dead_letter
    counter = _dlq_counter(meter)
    resolved_system = _resolve_system(system)
    _warn_default_system(system, "dead_letter_observer")

    def observe(event: DeadLetterEvent) -> None:
        counter.add(1, _metric_attributes(resolved_system, "process", _destination(event.queue)))

    return observe


def _ignore_dead_letter(event: DeadLetterEvent) -> None:
    return None
