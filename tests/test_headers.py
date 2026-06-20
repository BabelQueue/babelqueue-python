"""Tests for the core out-of-band transport-header seam (ADR-0028).

The dependency-free core: ``BabelQueue.publish_with_headers`` (produce side) and
``headers_from_context`` (consume side, surfaced by the runtime), plus the ``merge_headers``
helper. No broker and no OpenTelemetry needed.
"""

from __future__ import annotations

from babelqueue import BabelQueue, EnvelopeCodec, headers_from_context
from babelqueue.headers import _headers_scope, merge_headers
from babelqueue.transport import InMemoryTransport, ReceivedMessage, Transport


def test_headers_from_context_default_empty():
    assert headers_from_context() == {}


def test_headers_scope_surfaces_and_resets():
    with _headers_scope({"traceparent": "00-abc"}):
        assert headers_from_context().get("traceparent") == "00-abc"
    assert headers_from_context() == {}  # scope reset the contextvar


def test_headers_scope_none_is_nil_safe():
    with _headers_scope(None):
        assert headers_from_context() == {}


def test_merge_headers_drops_blanks_and_later_wins():
    merged = merge_headers(
        None,  # a falsy source is skipped
        {"traceparent": "00-1", "drop-empty": "", "": "no-key"},
        {},  # empty source is skipped
        {"traceparent": "00-2", "tracestate": "x=1"},
    )
    assert merged == {"traceparent": "00-2", "tracestate": "x=1"}  # later source wins, blanks gone


def test_publish_with_headers_carries_over_inmemory():
    t = InMemoryTransport()
    app = BabelQueue(transport=t, queue="orders")
    msg_id = app.publish_with_headers(
        "urn:babel:orders:created", {"order_id": 1}, {"traceparent": "00-abc"}
    )
    msg = t.pop("orders", 0)
    assert msg is not None
    assert msg.headers.get("traceparent") == "00-abc"
    # the body is the plain encoded envelope — the header rode out of band, not in the envelope
    env = EnvelopeCodec.decode(msg.body)
    assert env["meta"]["id"] == msg_id
    assert "traceparent" not in env and "traceparent" not in env.get("meta", {})


def test_publish_with_empty_headers_is_byte_identical_to_publish():
    t = InMemoryTransport()
    app = BabelQueue(transport=t, queue="orders")
    app.publish_with_headers("urn:babel:orders:created", {"order_id": 1}, {}, trace_id="t-1")
    framed = t.pop("orders", 0)
    app.publish("urn:babel:orders:created", {"order_id": 1}, trace_id="t-1")
    plain = t.pop("orders", 0)
    assert framed is not None and plain is not None
    assert framed.headers == {} and plain.headers == {}
    # same trace_id + same data → identical envelope bodies bar the random meta.id
    fa, pa = EnvelopeCodec.decode(framed.body), EnvelopeCodec.decode(plain.body)
    fa["meta"].pop("id"), pa["meta"].pop("id")
    fa["meta"].pop("created_at"), pa["meta"].pop("created_at")
    assert fa == pa


def test_runtime_surfaces_delivered_headers_to_handler():
    t = InMemoryTransport()
    app = BabelQueue(transport=t, queue="orders")
    seen = {}

    @app.handler("urn:babel:orders:created")
    def _on(data, meta):
        seen["headers"] = dict(headers_from_context())

    app.publish_with_headers(
        "urn:babel:orders:created", {"order_id": 1}, {"traceparent": "00-deadbeef"}
    )
    app.consume("orders", max_messages=1, timeout=0)
    assert seen["headers"].get("traceparent") == "00-deadbeef"
    # the scope is reset once dispatch returns
    assert headers_from_context() == {}


class _PlainTransport(Transport):
    """A Transport that is deliberately NOT a HeaderPublisher (no publish_with_headers)."""

    def __init__(self):
        self._inner = InMemoryTransport()

    def publish(self, queue, body):
        self._inner.publish(queue, body)

    def pop(self, queue, timeout=1.0):
        return self._inner.pop(queue, timeout)

    def ack(self, message):
        self._inner.ack(message)


def test_publish_with_headers_falls_back_when_transport_cannot_carry():
    t = _PlainTransport()
    app = BabelQueue(transport=t, queue="orders")
    # No error, headers simply dropped (degrades to plain publish), exactly like Redrive.
    app.publish_with_headers("urn:babel:orders:created", {"order_id": 1}, {"traceparent": "00-x"})
    msg = t.pop("orders", 0)
    assert msg is not None and msg.headers == {}
    assert isinstance(msg, ReceivedMessage)
