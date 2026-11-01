"""``BabelQueue.publish_envelope``: the producer seam the ``otel`` wrapper reads (Faz 04, scope 6).

It builds and sends the envelope exactly like ``publish`` / ``publish_with_headers``, but returns
``(envelope, error)`` instead of raising, so a transport failure still yields the envelope that was
handed to the transport. The two plain publish methods delegate to it and keep their behaviour.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from babelqueue import BabelQueue, BabelQueueError, EnvelopeCodec
from babelqueue.app import PublishResult
from babelqueue.transport import InMemoryTransport, Transport

URN = "urn:babel:orders:created"


class _Failing(InMemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.attempts: List[str] = []

    def publish(self, queue: str, body: str) -> None:
        self.attempts.append(body)
        raise ConnectionError("broker down")

    def publish_with_headers(self, queue: str, body: str, headers: Dict[str, str]) -> None:
        self.attempts.append(body)
        raise ConnectionError("broker down")


class _HeaderBlind(Transport):
    """A transport that cannot carry headers (no ``publish_with_headers``)."""

    def __init__(self) -> None:
        self.bodies: List[str] = []

    def publish(self, queue: str, body: str) -> None:
        self.bodies.append(body)

    def pop(self, queue: str, timeout: float = 1.0) -> Any:
        return None

    def ack(self, message: Any) -> None:
        return None


def test_success_returns_the_envelope_that_was_sent_and_no_error() -> None:
    transport = InMemoryTransport()
    app = BabelQueue(transport=transport, queue="orders")

    result = app.publish_envelope(URN, {"n": 1}, trace_id="7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b")

    assert isinstance(result, PublishResult)
    envelope, error = result  # a plain (envelope, error) pair
    assert error is None
    assert envelope is not None
    assert envelope["meta"]["id"] and envelope["meta"]["queue"] == "orders"
    assert envelope["trace_id"] == "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b"
    sent = transport.pop("orders", 0)
    assert sent is not None and EnvelopeCodec.decode(sent.body) == envelope


def test_a_transport_failure_still_yields_the_envelope_handed_to_the_transport() -> None:
    transport = _Failing()
    app = BabelQueue(transport=transport, queue="orders")

    envelope, error = app.publish_envelope(URN, {"n": 1}, trace_id="t-1")

    assert isinstance(error, ConnectionError)
    assert envelope is not None
    assert envelope["meta"]["id"] and envelope["trace_id"] == "t-1" and envelope["meta"]["queue"] == "orders"
    assert len(transport.attempts) == 1
    assert EnvelopeCodec.decode(transport.attempts[0]) == envelope  # the very envelope that failed


def test_a_header_publish_failure_yields_the_envelope_too() -> None:
    transport = _Failing()
    app = BabelQueue(transport=transport, queue="orders")
    envelope, error = app.publish_envelope(URN, {}, headers={"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"})
    assert isinstance(error, ConnectionError) and envelope is not None


def test_a_make_failure_yields_no_envelope() -> None:
    transport = InMemoryTransport()
    app = BabelQueue(transport=transport, queue="orders")

    envelope, error = app.publish_envelope("   ", {})  # a blank URN cannot be built

    assert envelope is None
    assert isinstance(error, BabelQueueError)
    assert transport.size("orders") == 0


def test_an_encode_failure_yields_no_envelope_and_nothing_reaches_the_transport() -> None:
    transport = _Failing()
    app = BabelQueue(transport=transport, queue="orders")

    envelope, error = app.publish_envelope(URN, {"not_json": object()})  # make works, encode fails

    assert envelope is None
    assert isinstance(error, TypeError)
    assert transport.attempts == []  # never handed to the transport


def test_the_default_queue_is_readable_and_an_empty_queue_resolves_to_it() -> None:
    transport = InMemoryTransport()
    app = BabelQueue(transport=transport, queue="orders")
    assert app.queue == "orders"
    for given in (None, ""):
        envelope, error = app.publish_envelope(URN, {}, queue=given)
        assert error is None and envelope is not None
        assert envelope["meta"]["queue"] == "orders"
    assert transport.size("orders") == 2


def test_headers_ride_beside_the_envelope_only_when_the_transport_carries_them() -> None:
    transport = InMemoryTransport()
    app = BabelQueue(transport=transport, queue="orders")
    app.publish_envelope(URN, {}, headers={"traceparent": "tp"})
    app.publish_envelope(URN, {}, headers={})
    app.publish_envelope(URN, {})
    assert transport.pop("orders", 0).headers == {"traceparent": "tp"}  # type: ignore[union-attr]
    assert transport.pop("orders", 0).headers == {}  # type: ignore[union-attr]
    assert transport.pop("orders", 0).headers == {}  # type: ignore[union-attr]

    blind = _HeaderBlind()
    envelope, error = BabelQueue(transport=blind, queue="orders").publish_envelope(
        URN, {}, headers={"traceparent": "tp"}
    )
    assert error is None and len(blind.bodies) == 1  # degraded to a plain publish, no error


def test_only_exceptions_are_captured() -> None:
    class Interrupted(InMemoryTransport):
        def publish(self, queue: str, body: str) -> None:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        BabelQueue(transport=Interrupted(), queue="orders").publish_envelope(URN, {})


# -- the existing publish methods are unchanged --------------------------------------------


def test_publish_returns_the_id_and_resolves_an_empty_queue_to_the_default() -> None:
    transport = InMemoryTransport()
    app = BabelQueue(transport=transport, queue="orders")
    message_id = app.publish(URN, {"n": 1}, queue="", trace_id="t-1")
    sent = transport.pop("orders", 0)
    assert sent is not None
    envelope = EnvelopeCodec.decode(sent.body)
    assert envelope["meta"]["id"] == message_id and envelope["trace_id"] == "t-1"


def test_publish_still_raises_the_original_exceptions() -> None:
    app = BabelQueue(transport=_Failing(), queue="orders")
    with pytest.raises(ConnectionError, match="broker down"):
        app.publish(URN, {})
    with pytest.raises(ConnectionError, match="broker down"):
        app.publish_with_headers(URN, {}, {"traceparent": "tp"})
    with pytest.raises(BabelQueueError):
        app.publish("", {})
    with pytest.raises(TypeError):
        app.publish(URN, {"x": object()})


def test_publish_with_headers_returns_the_id_and_degrades_without_a_header_transport() -> None:
    blind = _HeaderBlind()
    app = BabelQueue(transport=blind, queue="orders")
    message_id = app.publish_with_headers(URN, {}, {"traceparent": "tp"})
    assert EnvelopeCodec.decode(blind.bodies[0])["meta"]["id"] == message_id

    transport = InMemoryTransport()
    other = BabelQueue(transport=transport, queue="orders")
    other.publish_with_headers(URN, {}, {"traceparent": "tp"})
    message: Optional[Any] = transport.pop("orders", 0)
    assert message is not None and message.headers == {"traceparent": "tp"}
