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

from babelqueue import otel  # noqa: E402

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
