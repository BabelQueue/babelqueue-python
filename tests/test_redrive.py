"""Tests for the optional DLQ redrive tooling (ADR-0026)."""

import pytest

from babelqueue import dead_letter
from babelqueue.codec import EnvelopeCodec
from babelqueue.redrive import redrive
from babelqueue.transport import InMemoryTransport


def _dead_letter(transport, dlq, urn, original_queue, data=None):
    env = EnvelopeCodec.make(urn, data or {}, queue=original_queue)
    annotated = dead_letter.annotate(env, "failed", original_queue, 3, error="boom")
    transport.publish(dlq, EnvelopeCodec.encode(annotated))
    return annotated


def _drain(transport, queue):
    out = []
    while True:
        message = transport.pop(queue, 0)
        if message is None:
            break
        out.append(EnvelopeCodec.decode(message.body))
        transport.ack(message)
    return out


def test_redrive_to_source():
    t = InMemoryTransport()
    orig = _dead_letter(t, "orders.dlq", "urn:babel:orders:created", "orders", {"order_id": 1})

    result = redrive(t, "orders.dlq")

    assert result.redriven == 1 and result.skipped == 0
    assert result.items[0].bypassed is False  # bypass is off by default
    got = _drain(t, "orders")
    assert len(got) == 1
    assert "dead_letter" not in got[0]
    assert got[0]["attempts"] == 0
    assert got[0]["trace_id"] == orig["trace_id"]
    assert got[0]["data"] == {"order_id": 1}
    assert EnvelopeCodec.urn(got[0]) == "urn:babel:orders:created"
    assert _drain(t, "orders.dlq") == []


def test_redrive_to_sandbox():
    t = InMemoryTransport()
    _dead_letter(t, "orders.dlq", "urn:babel:orders:created", "orders")

    result = redrive(t, "orders.dlq", to_queue="sandbox")

    assert result.redriven == 1
    assert _drain(t, "orders") == []
    assert len(_drain(t, "sandbox")) == 1


def test_redrive_dry_run():
    t = InMemoryTransport()
    _dead_letter(t, "orders.dlq", "urn:babel:orders:created", "orders")

    result = redrive(t, "orders.dlq", dry_run=True)

    assert result.redriven == 0 and result.skipped == 1
    assert result.items[0].to == "orders"
    assert result.items[0].redriven is False
    assert _drain(t, "orders") == []
    dlq = _drain(t, "orders.dlq")
    assert len(dlq) == 1 and "dead_letter" in dlq[0]


def test_redrive_select():
    t = InMemoryTransport()
    _dead_letter(t, "dlq", "urn:babel:orders:created", "orders")
    _dead_letter(t, "dlq", "urn:babel:emails:welcome", "emails")

    result = redrive(t, "dlq", select=lambda e: EnvelopeCodec.urn(e) == "urn:babel:orders:created")

    assert result.redriven == 1 and result.skipped == 1
    assert len(_drain(t, "orders")) == 1
    assert _drain(t, "emails") == []
    assert len(_drain(t, "dlq")) == 1  # the unselected one is restored


def test_redrive_max():
    t = InMemoryTransport()
    for _ in range(3):
        _dead_letter(t, "dlq", "urn:babel:orders:created", "orders")

    result = redrive(t, "dlq", max=2)

    assert result.redriven == 2
    assert len(_drain(t, "dlq")) == 1  # Max respected


def test_redrive_no_dead_letter_falls_back_to_meta_queue():
    t = InMemoryTransport()
    # a plain (never dead-lettered) envelope on the DLQ — redrive falls back to meta.queue
    env = EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders")
    t.publish("dlq", EnvelopeCodec.encode(env))

    result = redrive(t, "dlq")

    assert result.redriven == 1
    assert len(_drain(t, "orders")) == 1


class _FailOnTarget(InMemoryTransport):
    """An in-memory transport that refuses to publish to one queue."""

    def __init__(self, fail_queue):
        super().__init__()
        self._fail_queue = fail_queue

    def publish(self, queue, body):
        if queue == self._fail_queue:
            raise RuntimeError("publish refused")
        super().publish(queue, body)


def test_redrive_publish_failure_restores():
    t = _FailOnTarget("orders")
    _dead_letter(t, "dlq", "urn:babel:orders:created", "orders")

    with pytest.raises(RuntimeError):
        redrive(t, "dlq")

    assert len(_drain(t, "dlq")) == 1  # restored to the DLQ, not lost
    assert _drain(t, "orders") == []  # nothing reached the source queue


def test_redrive_undecodable_restored():
    t = InMemoryTransport()
    t.publish("dlq", "not-json{{{")

    result = redrive(t, "dlq")

    assert result.redriven == 0 and result.skipped == 1
    message = t.pop("dlq", 0)
    assert message is not None and message.body == "not-json{{{"
