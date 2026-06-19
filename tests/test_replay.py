"""Tests for the optional Replay-Bypass guard (ADR-0027)."""

from babelqueue import (
    BabelQueue,
    bypass_external_effects,
    dead_letter,
    is_replay,
)
from babelqueue.codec import EnvelopeCodec
from babelqueue.redrive import redrive
from babelqueue.replay import HEADER_REPLAY_BYPASS, _replay_scope
from babelqueue.transport import InMemoryTransport, Transport


def _dead_letter(transport, dlq, urn, original_queue, data=None):
    env = EnvelopeCodec.make(urn, data or {}, queue=original_queue)
    annotated = dead_letter.annotate(env, "failed", original_queue, 3, error="boom")
    transport.publish(dlq, EnvelopeCodec.encode(annotated))
    return annotated


def test_is_replay_default_false():
    assert is_replay() is False


def test_bypass_runs_when_not_replay():
    ran = []

    def effect():
        ran.append("x")
        return "did-it"

    assert bypass_external_effects(effect) == "did-it"
    assert ran == ["x"]


def test_bypass_skips_on_replay_and_scope_resets():
    ran = []

    def effect():
        ran.append("x")
        raise AssertionError("must not run on a replay")

    with _replay_scope(True):
        assert is_replay() is True
        assert bypass_external_effects(effect) is None
    assert ran == []
    assert is_replay() is False  # the scope reset the contextvar


def test_in_memory_transport_carries_headers():
    t = InMemoryTransport()
    t.publish_with_headers("q", "body", {HEADER_REPLAY_BYPASS: "1"})
    msg = t.pop("q", 0)
    assert msg is not None and msg.headers.get(HEADER_REPLAY_BYPASS) == "1"
    # a plain publish carries no headers
    t.publish("q", "plain")
    msg2 = t.pop("q", 0)
    assert msg2 is not None and msg2.headers == {}


def test_redrive_bypass_stamps_header_and_consume_skips_effects():
    t = InMemoryTransport()
    _dead_letter(t, "orders.dlq", "urn:babel:orders:created", "orders", {"order_id": 1})

    result = redrive(t, "orders.dlq", bypass=True)
    assert result.redriven == 1
    assert result.items[0].bypassed is True

    # the redriven message on the source queue carries the bypass header
    msg = t.pop("orders", 0)
    assert msg is not None and msg.headers.get(HEADER_REPLAY_BYPASS) == "1"

    # consume it: the handler sees is_replay and BypassExternalEffects skips the side-effect
    emailed = []
    app = BabelQueue(transport=t)

    @app.handler("urn:babel:orders:created")
    def on_created(data, meta):
        assert is_replay() is True
        bypass_external_effects(lambda: emailed.append(data))

    app.dispatch(msg)
    assert emailed == []  # the external side-effect was skipped on the bypassed replay


def test_normal_delivery_is_not_a_replay():
    t = InMemoryTransport()
    app = BabelQueue(transport=t)
    fired = []

    @app.handler("urn:babel:orders:created")
    def on_created(data, meta):
        assert is_replay() is False
        bypass_external_effects(lambda: fired.append(data))

    app.publish("urn:babel:orders:created", {"order_id": 9})
    app.consume(max_messages=1, timeout=0)
    assert fired == [{"order_id": 9}]  # effect runs on a normal (non-replay) delivery


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


def test_redrive_bypass_without_header_support_falls_back():
    t = _PlainTransport()
    _dead_letter(t, "orders.dlq", "urn:babel:orders:created", "orders")

    result = redrive(t, "orders.dlq", bypass=True)
    assert result.redriven == 1
    assert result.items[0].bypassed is False  # no-op without a HeaderPublisher
