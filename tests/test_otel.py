"""Tests for the optional OpenTelemetry module (ADR-0025).

Skipped when OpenTelemetry is not installed (the ``[otel]`` extra); CI installs it so these
run and count toward coverage.
"""

import pytest

pytest.importorskip("opentelemetry")

from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind  # noqa: E402

from babelqueue import BabelQueue, EnvelopeCodec, otel  # noqa: E402
from babelqueue.headers import _headers_scope  # noqa: E402
from babelqueue.transport import InMemoryTransport  # noqa: E402

_TRACE_ID = "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b"


def _recorder():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


def _envelope(trace_id=_TRACE_ID, attempts=0):
    return {
        "job": "urn:babel:orders:created",
        "trace_id": trace_id,
        "data": {"order_id": 1},
        "meta": {"id": "m1", "queue": "orders"},
        "attempts": attempts,
    }


class FakeApp:
    """An object exposing publish(urn, data, *, queue, trace_id) -> str, recording calls."""

    def __init__(self):
        self.calls = []

    def publish(self, urn, data, *, queue=None, trace_id=None):
        self.calls.append({"urn": urn, "data": data, "queue": queue, "trace_id": trace_id})
        return "msg-123"


def test_trace_id_round_trip():
    tid = otel.trace_id_of(_TRACE_ID)
    assert tid != 0
    assert otel.uuid_of(tid) == _TRACE_ID
    # a non-uuid trace_id maps deterministically to a valid, distinct trace id
    assert otel.trace_id_of("not-a-uuid") == otel.trace_id_of("not-a-uuid")
    assert otel.trace_id_of("not-a-uuid") != 0
    assert otel.trace_id_of("not-a-uuid") != tid
    # 32 chars but not hex → not a UUID, so it is hashed
    assert otel.trace_id_of("z" * 32) != 0


def test_wrap_handler_span_in_trace_with_attrs():
    tracer, exporter = _recorder()
    seen = {}

    def handler(data, meta, envelope):
        seen["called"] = True

    env = _envelope()
    otel.wrap_handler(tracer, handler)(env["data"], env["meta"], env)

    assert seen.get("called")
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "process urn:babel:orders:created"
    assert span.kind == SpanKind.CONSUMER
    assert span.context.trace_id == otel.trace_id_of(_TRACE_ID)
    assert span.attributes["messaging.message.conversation_id"] == _TRACE_ID
    assert span.attributes["messaging.message.id"] == "m1"
    assert span.attributes["messaging.destination.name"] == "orders"


def test_wrap_handler_two_arg_inner():
    tracer, exporter = _recorder()
    got = {}

    def handler(data, meta):  # only two positional args
        got["data"] = data

    env = _envelope()
    # the runtime passes (data, meta, envelope); the wrapper forwards only what the inner wants
    otel.wrap_handler(tracer, handler)(env["data"], env["meta"], env)

    assert got["data"] == env["data"]
    assert len(exporter.get_finished_spans()) == 1


def test_wrap_handler_records_error():
    tracer, exporter = _recorder()

    def handler(data, meta, envelope):
        raise ValueError("boom")

    env = _envelope()
    with pytest.raises(ValueError):
        otel.wrap_handler(tracer, handler)(env["data"], env["meta"], env)

    span = exporter.get_finished_spans()[0]
    assert span.status.status_code.name == "ERROR"
    assert len(span.events) >= 1  # the recorded exception


def test_publish_stamps_trace_id_from_span():
    tracer, exporter = _recorder()
    app = FakeApp()

    message_id = otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 7})

    assert message_id == "msg-123"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.kind == SpanKind.PRODUCER
    assert span.attributes["messaging.message.id"] == "msg-123"

    stamped = app.calls[0]["trace_id"]
    # the published trace_id encodes the producer span's trace, so a consumer recovers it
    assert stamped == otel.uuid_of(span.context.trace_id)
    assert otel.trace_id_of(stamped) == span.context.trace_id


# ---------------------------------------------------------------------------
# ADR-0028: W3C traceparent transport-header propagation (true span linkage).
# ---------------------------------------------------------------------------


def test_inject_then_extract_round_trips_the_span_context():
    tracer, _ = _recorder()
    with tracer.start_as_current_span("publish x", kind=SpanKind.PRODUCER) as producer:
        prod_sc = producer.get_span_context()
        from opentelemetry.trace import set_span_in_context

        headers = otel._inject_traceparent(set_span_in_context(producer))

    assert headers.get(otel.HEADER_TRACEPARENT)  # a traceparent was injected
    with _headers_scope(headers):
        context = otel._remote_parent_from_headers()
    assert context is not None
    from opentelemetry.trace import get_current_span

    remote_sc = get_current_span(context).get_span_context()
    assert remote_sc.trace_id == prod_sc.trace_id
    assert remote_sc.span_id == prod_sc.span_id
    assert remote_sc.is_remote


def test_no_or_malformed_header_yields_no_remote_parent():
    # header-less context
    assert otel._remote_parent_from_headers() is None
    # present but malformed traceparent -> the W3C propagator rejects it
    with _headers_scope({otel.HEADER_TRACEPARENT: "garbage"}):
        assert otel._remote_parent_from_headers() is None


def test_cross_hop_parent_child_linkage():
    """The core of ADR-0028: a consumer span is a true child of the producer span across a hop."""
    tracer, exporter = _recorder()

    # PRODUCER: start a span, inject its context as a traceparent header.
    from opentelemetry.trace import set_span_in_context

    with tracer.start_as_current_span("publish x", kind=SpanKind.PRODUCER) as producer:
        prod_sc = producer.get_span_context()
        headers = otel._inject_traceparent(set_span_in_context(producer))

    # HOP: the runtime surfaces the delivered headers onto the handler context.
    env = _envelope(trace_id="not-the-producer-trace-id-uuid")
    captured = {}

    def handler(data, meta, envelope):
        from opentelemetry.trace import get_current_span as gcs

        captured["child"] = gcs().get_span_context()

    with _headers_scope(headers):
        otel.wrap_handler(tracer, handler)(env["data"], env["meta"], env)

    consumer = next(s for s in exporter.get_finished_spans() if s.kind == SpanKind.CONSUMER)
    # Same trace across the hop, and the consumer's PARENT is the producer span (its span id) —
    # not a trace_id-derived synthetic parent.
    assert consumer.context.trace_id == prod_sc.trace_id
    assert consumer.parent is not None
    assert consumer.parent.span_id == prod_sc.span_id
    assert consumer.parent.is_remote
    # the handler's own active span is a fresh child, not the producer span itself
    assert captured["child"].span_id != prod_sc.span_id


def test_wrap_handler_falls_back_to_trace_id_without_header():
    """Backward compatibility: with no traceparent, the span lands in the trace_id-derived
    trace (v0.1), so a message produced by a pre-0028 producer is not regressed."""
    tracer, exporter = _recorder()
    otel.wrap_handler(tracer, lambda d, m, e: None)(
        _envelope()["data"], _envelope()["meta"], _envelope()
    )
    span = exporter.get_finished_spans()[0]
    assert span.context.trace_id == otel.trace_id_of(_TRACE_ID)  # the v0.1 trace
    assert span.parent is not None
    assert span.parent.span_id == otel._span_id_of(_TRACE_ID)  # trace_id-derived synthetic parent


def test_publish_injects_traceparent_and_keeps_trace_id_fallback():
    """The producer wrapper puts a traceparent on the transport (via publish_with_headers) AND
    still stamps trace_id for the header-blind v0.1 path."""
    tracer, exporter = _recorder()
    t = InMemoryTransport()
    app = BabelQueue(transport=t, queue="default")

    otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 7})
    span = exporter.get_finished_spans()[0]

    msg = t.pop("default", 0)
    assert msg is not None
    tp = msg.headers.get(otel.HEADER_TRACEPARENT)
    assert tp  # Publish carried a traceparent transport header
    # the header encodes the producer span (extract -> same trace + span id)
    with _headers_scope(msg.headers):
        remote = otel._remote_parent_from_headers()
    from opentelemetry.trace import get_current_span

    rsc = get_current_span(remote).get_span_context()
    assert rsc.trace_id == span.context.trace_id
    assert rsc.span_id == span.context.span_id
    # v0.1 belt-and-braces: trace_id still encodes the same trace for header-blind consumers
    env = EnvelopeCodec.decode(msg.body)
    assert otel.trace_id_of(env["trace_id"]) == span.context.trace_id
    # and the traceparent rode out of band, not inside the frozen envelope
    assert "traceparent" not in env and "traceparent" not in env.get("meta", {})


def test_end_to_end_producer_to_consumer_parent_child():
    """The real producer (Publish over InMemoryTransport) wired to the real consumer
    (wrap_handler dispatched by the runtime): parent-child linkage across the full path."""
    tracer, exporter = _recorder()
    t = InMemoryTransport()
    app = BabelQueue(transport=t, queue="default")
    app.register(
        "urn:babel:orders:created", otel.wrap_handler(tracer, lambda d, m, e: None)
    )

    otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 1})
    processed = app.consume("default", max_messages=1, timeout=0)
    assert processed == 1

    producer = next(s for s in exporter.get_finished_spans() if s.kind == SpanKind.PRODUCER)
    consumer = next(s for s in exporter.get_finished_spans() if s.kind == SpanKind.CONSUMER)
    assert consumer.parent is not None
    assert consumer.parent.span_id == producer.context.span_id
    assert consumer.context.trace_id == producer.context.trace_id
