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
from babelqueue.dead_letter import DeadLetterEvent  # noqa: E402
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
    # ADR-0038: the name is "process <queue>", never the URN (unbounded cardinality)
    assert span.name == "process orders"
    assert span.kind == SpanKind.CONSUMER
    assert span.context.trace_id == otel.trace_id_of(_TRACE_ID)
    assert span.attributes["messaging.message.conversation_id"] == _TRACE_ID
    assert span.attributes["messaging.message.id"] == "m1"
    assert span.attributes["messaging.destination.name"] == "orders"
    assert span.attributes["babelqueue.urn"] == "urn:babel:orders:created"
    assert span.attributes["messaging.operation.type"] == "process"
    assert span.attributes["messaging.operation.name"] == "process"
    assert span.attributes["messaging.system"] == "babelqueue"  # the deprecated default
    assert span.attributes["babelqueue.attempts"] == 0
    # transition MINOR: the deprecated keys are dual-emitted
    assert span.attributes["messaging.operation"] == "process"
    assert span.attributes["messaging.babelqueue.attempts"] == 0


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
    assert span.status.description == "boom"
    assert span.attributes["error.type"] == "ValueError"
    exceptions = [e for e in span.events if e.name == "exception"]
    assert len(exceptions) == 1  # recorded once, not again by the SDK's own on-exception handling


def test_publish_stamps_trace_id_from_span():
    tracer, exporter = _recorder()
    app = FakeApp()

    message_id = otel.publish(tracer, app, "urn:babel:orders:created", {"order_id": 7})

    assert message_id == "msg-123"
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.kind == SpanKind.PRODUCER
    assert span.name == "send"  # a duck-typed app exposes no queue: the bare operation name
    assert span.attributes["messaging.message.id"] == "msg-123"
    assert span.attributes["babelqueue.urn"] == "urn:babel:orders:created"

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


# ---------------------------------------------------------------------------
# ADR-0038 span shape: system, consumer group, error recording, semconv pin.
# ---------------------------------------------------------------------------


class _CustomError(Exception):
    pass


def _consume(tracer, handler=None, **kwargs):
    env = _envelope(attempts=2)
    wrapped = otel.wrap_handler(tracer, handler or (lambda d, m, e: None), **kwargs)
    wrapped(env["data"], env["meta"], env)


def test_semconv_pin_constants():
    assert otel.SEMCONV_VERSION == "1.44.0"
    assert otel.SEMCONV_SCHEMA_URL == "https://opentelemetry.io/schemas/1.44.0"


def test_system_and_consumer_group_are_written_as_given():
    tracer, exporter = _recorder()
    _consume(tracer, system="kafka", consumer_group="billing-workers")
    span = exporter.get_finished_spans()[0]
    assert span.attributes["messaging.system"] == "kafka"
    assert span.attributes["messaging.consumer.group.name"] == "billing-workers"
    assert span.attributes["babelqueue.attempts"] == 2
    assert span.attributes["messaging.babelqueue.attempts"] == 2


def test_no_consumer_group_option_writes_no_group_attribute():
    tracer, exporter = _recorder()
    _consume(tracer, system="kafka")
    assert "messaging.consumer.group.name" not in exporter.get_finished_spans()[0].attributes


def test_empty_system_counts_as_absent_and_warns_once_per_wrapper(caplog):
    tracer, exporter = _recorder()
    with caplog.at_level("WARNING", logger="babelqueue.otel"):
        wrapped = otel.wrap_handler(tracer, lambda d, m, e: None, system="")
        env = _envelope()
        wrapped(env["data"], env["meta"], env)
        wrapped(env["data"], env["meta"], env)
    assert exporter.get_finished_spans()[0].attributes["messaging.system"] == "babelqueue"
    assert len([r for r in caplog.records if "no system=" in r.getMessage()]) == 1


def test_a_system_outside_the_documented_set_is_not_validated():
    tracer, exporter = _recorder()
    _consume(tracer, system="my-broker")
    assert exporter.get_finished_spans()[0].attributes["messaging.system"] == "my-broker"


def test_blank_or_missing_queue_gives_the_bare_name_and_no_destination():
    tracer, exporter = _recorder()
    wrapped = otel.wrap_handler(tracer, lambda d, m, e: None, system="redis")
    env = _envelope()
    wrapped(env["data"], {"id": "m1", "queue": "  "}, env)
    wrapped(env["data"], {"id": "m1"}, env)
    for span in exporter.get_finished_spans():
        assert span.name == "process"
        assert "messaging.destination.name" not in span.attributes


def test_error_type_uses_the_module_qualified_class_name():
    tracer, exporter = _recorder()

    def handler(data, meta, envelope):
        raise _CustomError("nope")

    with pytest.raises(_CustomError):
        _consume(tracer, handler, system="redis")
    span = exporter.get_finished_spans()[0]
    assert span.attributes["error.type"] == f"{__name__}._CustomError"
    assert span.status.status_code.name == "ERROR"


def test_a_successful_handler_leaves_status_unset_and_writes_no_error_type():
    tracer, exporter = _recorder()
    _consume(tracer, system="redis")
    span = exporter.get_finished_spans()[0]
    assert span.status.status_code.name == "UNSET"
    assert "error.type" not in span.attributes


def test_non_uuid_trace_id_is_preserved_hashed_and_warned_once(caplog):
    tracer, exporter = _recorder()
    env = _envelope(trace_id="opaque-request-9f2c")
    wrapped = otel.wrap_handler(tracer, lambda d, m, e: None, system="redis")
    with caplog.at_level("WARNING", logger="babelqueue.otel"):
        wrapped(env["data"], env["meta"], env)
        wrapped(env["data"], env["meta"], env)
    spans = exporter.get_finished_spans()
    assert [s.attributes["messaging.message.conversation_id"] for s in spans] == ["opaque-request-9f2c"] * 2
    assert spans[0].context.trace_id == otel.trace_id_of("opaque-request-9f2c")
    assert len([r for r in caplog.records if "not a UUID" in r.getMessage()]) == 1


def test_a_uuid_trace_id_logs_no_warning(caplog):
    tracer, _ = _recorder()
    with caplog.at_level("WARNING", logger="babelqueue.otel"):
        _consume(tracer, system="redis")
    assert not [r for r in caplog.records if "not a UUID" in r.getMessage()]


# ---------------------------------------------------------------------------
# Producer: the real BabelQueue seam (publish_envelope), also on failure.
# ---------------------------------------------------------------------------


class _FailingTransport(InMemoryTransport):
    def publish(self, queue, body):
        raise ConnectionError("broker down")

    def publish_with_headers(self, queue, body, headers):
        raise ConnectionError("broker down")


def test_publish_span_shape_on_a_real_app():
    tracer, exporter = _recorder()
    t = InMemoryTransport()
    app = BabelQueue(transport=t, queue="orders")

    message_id = otel.publish(tracer, app, "urn:babel:orders:created", {"n": 1}, system="redis")

    span = exporter.get_finished_spans()[0]
    produced = EnvelopeCodec.decode(t.pop("orders", 0).body)
    assert span.name == "send orders"
    assert span.kind == SpanKind.PRODUCER
    assert span.attributes["messaging.system"] == "redis"
    assert span.attributes["messaging.operation.type"] == "send"
    assert span.attributes["messaging.operation.name"] == "send"
    assert span.attributes["messaging.operation"] == "publish"
    assert span.attributes["messaging.destination.name"] == "orders"
    assert span.attributes["messaging.message.id"] == message_id == produced["meta"]["id"]
    assert span.attributes["messaging.message.conversation_id"] == produced["trace_id"]
    assert span.attributes["babelqueue.urn"] == "urn:babel:orders:created"
    assert span.attributes["babelqueue.schema_version"] == 1
    assert span.status.status_code.name == "UNSET"


def test_publish_with_an_explicit_queue_names_the_span_after_it():
    tracer, exporter = _recorder()
    app = BabelQueue(transport=InMemoryTransport(), queue="orders")
    otel.publish(tracer, app, "urn:babel:orders:created", {"n": 1}, queue="billing", system="redis")
    span = exporter.get_finished_spans()[0]
    assert span.name == "send billing"
    assert span.attributes["messaging.destination.name"] == "billing"


def test_publish_failure_keeps_the_produced_identity_and_reraises_the_same_error():
    tracer, exporter = _recorder()
    app = BabelQueue(transport=_FailingTransport(), queue="orders")

    with pytest.raises(ConnectionError, match="broker down") as raised:
        otel.publish(tracer, app, "urn:babel:orders:created", {"n": 1}, system="rabbitmq")

    span = exporter.get_finished_spans()[0]
    assert span.status.status_code.name == "ERROR"
    assert span.status.description == "broker down"
    assert span.attributes["error.type"] == "ConnectionError"
    assert span.attributes["messaging.message.id"]  # the envelope was handed to the transport
    assert span.attributes["messaging.message.conversation_id"] == otel.uuid_of(span.context.trace_id)
    assert len([e for e in span.events if e.name == "exception"]) == 1
    assert isinstance(raised.value, ConnectionError)


def test_publish_failure_before_the_envelope_is_built_has_no_identity():
    tracer, exporter = _recorder()
    app = BabelQueue(transport=InMemoryTransport(), queue="orders")

    with pytest.raises(TypeError):
        otel.publish(tracer, app, "urn:babel:orders:created", {"x": object()}, system="redis")

    span = exporter.get_finished_spans()[0]
    assert span.status.status_code.name == "ERROR"
    assert "messaging.message.id" not in span.attributes
    assert "messaging.message.conversation_id" not in span.attributes
    assert span.attributes["error.type"] == "TypeError"


def test_a_duck_typed_app_that_raises_is_recorded_and_reraised():
    tracer, exporter = _recorder()

    class Broken(FakeApp):
        def publish(self, urn, data, *, queue=None, trace_id=None):
            raise RuntimeError("down")

    with pytest.raises(RuntimeError, match="down"):
        otel.publish(tracer, Broken(), "urn:babel:orders:created", {}, system="redis")
    span = exporter.get_finished_spans()[0]
    assert span.status.status_code.name == "ERROR"
    assert span.attributes["error.type"] == "RuntimeError"


# ---------------------------------------------------------------------------
# Metrics (ADR-0039): opt-in through meter=, instruments created once.
# ---------------------------------------------------------------------------


def _meter():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]).get_meter("test"), reader


def _points(reader):
    out = {}
    data = reader.get_metrics_data()
    for resource in (data.resource_metrics if data else []):
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                out[metric.name] = metric.data.data_points
    return out


def test_publish_called_twice_with_one_meter_aggregates_into_one_series():
    tracer, _ = _recorder()
    meter, reader = _meter()
    app = BabelQueue(transport=InMemoryTransport(), queue="orders")
    for _ in range(2):
        otel.publish(tracer, app, "urn:babel:orders:created", {"n": 1}, system="redis", meter=meter)
    (point,) = _points(reader)["messaging.client.sent.messages"]
    assert point.value == 2
    assert dict(point.attributes) == {
        "messaging.system": "redis",
        "messaging.operation.name": "send",
        "messaging.destination.name": "orders",
    }


def test_no_meter_records_nothing():
    tracer, _ = _recorder()
    meter, reader = _meter()
    app = BabelQueue(transport=InMemoryTransport(), queue="orders")
    otel.publish(tracer, app, "urn:babel:orders:created", {"n": 1}, system="redis")
    _consume(tracer, system="redis")
    assert _points(reader) == {}


def test_dead_letter_observer_without_a_meter_is_a_no_op():
    observer = otel.dead_letter_observer(None)
    observer(DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="failed"))


def test_the_dead_letter_counter_counts_a_real_runtime_dead_letter():
    tracer, _ = _recorder()
    meter, reader = _meter()
    t = InMemoryTransport()
    app = BabelQueue(
        transport=t, queue="orders", max_attempts=1, dead_letter=True,
        dead_letter_observer=otel.dead_letter_observer(meter, system="redis"),
    )
    app.register("urn:babel:orders:created", otel.wrap_handler(tracer, _always_fails, system="redis", meter=meter))
    app.publish("urn:babel:orders:created", {"n": 1})

    assert app.consume("orders", max_messages=1, timeout=0, handle_signals=False) == 1

    points = _points(reader)
    (dlq,) = points["babelqueue.dlq.messages"]
    assert dlq.value == 1
    assert dict(dlq.attributes) == {
        "messaging.system": "redis",
        "messaging.operation.name": "process",
        "messaging.destination.name": "orders",
    }
    assert t.size("orders.dlq") == 1


def _always_fails(data, meta):
    raise RuntimeError("always")
