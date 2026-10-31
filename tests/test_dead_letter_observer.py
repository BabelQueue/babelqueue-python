"""The dead-letter observer seam (ADR-0039 section 5): ``BabelQueue(dead_letter_observer=...)``.

Fake transports only. Pins section 5.1: the observer is called once per dead-letter the SDK itself
carried out (a confirmed DLQ write, or a native dead-letter reported ``True`` by the optional
:class:`~babelqueue.dead_letter.NativeDeadLetterSettler` variant), and never for a failed write, a
native no-op, or an abandon. An observer that raises changes nothing.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pytest

from babelqueue import (
    BabelQueue,
    DeadLetterEvent,
    EnvelopeCodec,
    NativeDeadLetterSettler,
    NativeSettler,
    ReceivedMessage,
    UnknownUrnStrategy,
)
from babelqueue.dead_letter import DeadLetterObserver
from babelqueue.transport import InMemoryTransport

URN = "urn:babel:orders:created"
GATED = "babelqueue:unsupported-schema-version:2"


class Recorder:
    def __init__(self) -> None:
        self.events: List[DeadLetterEvent] = []

    def __call__(self, event: DeadLetterEvent) -> None:
        self.events.append(event)


def _fail(data: Any, meta: Any) -> None:
    raise RuntimeError("always fails")


def _app(transport: Any = None, observer: Optional[DeadLetterObserver] = None, **kwargs: Any) -> BabelQueue:
    return BabelQueue(
        transport=transport if transport is not None else InMemoryTransport(),
        queue="orders",
        dead_letter_observer=observer,
        **kwargs,
    )


def _delivery(app: BabelQueue, body: str, route_urn: str = "") -> None:
    app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn=route_urn))


def _valid_body() -> str:
    return EnvelopeCodec.encode(EnvelopeCodec.make(URN, {"n": 1}, queue="orders"))


# -- the DLQ write chokepoint --------------------------------------------------------------


def test_an_exhausted_retry_calls_the_observer_once_with_the_event() -> None:
    seen = Recorder()
    app = _app(observer=seen, max_attempts=1, dead_letter=True)
    app.register(URN, _fail)

    _delivery(app, _valid_body())

    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="failed")]
    assert app.transport.size("orders.dlq") == 1  # type: ignore[attr-defined]


def test_no_observer_changes_nothing() -> None:
    app = _app(max_attempts=1, dead_letter=True)
    app.register(URN, _fail)
    _delivery(app, _valid_body())
    assert app.transport.size("orders.dlq") == 1  # type: ignore[attr-defined]


def test_a_retry_that_is_not_yet_exhausted_is_not_a_dead_letter() -> None:
    seen = Recorder()
    app = _app(observer=seen, max_attempts=3, dead_letter=True)
    app.register(URN, _fail)
    _delivery(app, _valid_body())
    assert seen.events == []


def test_an_unknown_urn_under_dead_letter_reports_its_reason() -> None:
    seen = Recorder()
    app = _app(observer=seen, on_unknown_urn=UnknownUrnStrategy.DEAD_LETTER)
    _delivery(app, _valid_body())
    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="unknown_urn")]


def test_a_poison_body_reports_the_poison_reason() -> None:
    seen = Recorder()
    app = _app(observer=seen, dead_letter=True)
    _delivery(app, "this is not json")
    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="poison")]


def test_a_version_gated_raw_body_has_an_empty_reason() -> None:
    seen = Recorder()
    app = _app(observer=seen, dead_letter=True)
    _delivery(app, _valid_body(), route_urn=GATED)
    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="")]


def test_the_source_queue_is_the_envelope_queue_else_the_consumed_queue() -> None:
    seen = Recorder()
    app = _app(observer=seen, max_attempts=1, dead_letter=True)
    app.register(URN, _fail)
    body = EnvelopeCodec.encode(EnvelopeCodec.make(URN, {}, queue="elsewhere"))
    _delivery(app, body)
    assert seen.events[0].queue == "elsewhere"


class _FailingDlqTransport(InMemoryTransport):
    """Fails every publish to a ``.dlq`` queue: the dead-letter write itself fails."""

    def publish(self, queue: str, body: str) -> None:
        if queue.endswith(".dlq"):
            raise ConnectionError("the DLQ write failed")
        super().publish(queue, body)


def test_a_failed_dlq_write_does_not_call_the_observer() -> None:
    seen = Recorder()
    app = _app(_FailingDlqTransport(), seen, max_attempts=1, dead_letter=True)
    app.register(URN, _fail)

    with pytest.raises(ConnectionError):
        _delivery(app, _valid_body())

    assert seen.events == []


class _GuardedWriterTransport(InMemoryTransport):
    """A ``DeadLetterWriter``: the write and the settle are one guarded step."""

    def __init__(self, fail: bool = False) -> None:
        super().__init__()
        self.fail = fail
        self.written: List[str] = []

    def dead_letter(self, message: ReceivedMessage, queue: str, body: str) -> None:
        if self.fail:
            raise ConnectionError("the guarded write failed")
        self.written.append(queue)


def test_a_dead_letter_writer_transport_is_observed_after_its_write() -> None:
    seen = Recorder()
    transport = _GuardedWriterTransport()
    app = _app(transport, seen, max_attempts=1, dead_letter=True)
    app.register(URN, _fail)
    _delivery(app, _valid_body())
    assert transport.written == ["orders.dlq"]
    assert len(seen.events) == 1


def test_a_failing_dead_letter_writer_is_not_observed() -> None:
    seen = Recorder()
    app = _app(_GuardedWriterTransport(fail=True), seen, max_attempts=1, dead_letter=True)
    app.register(URN, _fail)
    with pytest.raises(ConnectionError):
        _delivery(app, _valid_body())
    assert seen.events == []


def test_a_raising_observer_is_logged_and_never_changes_the_settle(caplog: pytest.LogCaptureFixture) -> None:
    def boom(event: DeadLetterEvent) -> None:
        raise RuntimeError("observer bug")

    transport = InMemoryTransport()
    app = _app(transport, boom, max_attempts=1, dead_letter=True)
    app.register(URN, _fail)

    with caplog.at_level(logging.ERROR, logger="babelqueue.app"):
        _delivery(app, _valid_body())  # does not raise

    assert transport.size("orders.dlq") == 1  # the dead-letter record is written as usual
    assert any("dead-letter observer failed" in r.getMessage() for r in caplog.records)


# -- the native-settle variant -------------------------------------------------------------


class _NativeOnly(InMemoryTransport):
    """Only the plain ``NativeSettler``: it cannot say whether it dead-lettered."""

    def __init__(self) -> None:
        super().__init__()
        self.settled: List[Dict[str, str]] = []

    def settle_native(self, message: ReceivedMessage, reason: str, description: str) -> None:
        self.settled.append({"reason": reason, "description": description})


class _NativeReporting(_NativeOnly):
    """``NativeSettler`` plus the reporting variant."""

    def __init__(self, result: Any = True, error: Optional[Exception] = None) -> None:
        super().__init__()
        self.result = result
        self.error = error
        self.variant_calls = 0

    def settle_native_dead_letter(self, message: ReceivedMessage, reason: str, description: str) -> bool:
        self.variant_calls += 1
        if self.error is not None:
            raise self.error
        return self.result


def test_the_variant_protocol_is_runtime_checkable_and_distinct_from_native_settler() -> None:
    assert isinstance(_NativeReporting(), NativeDeadLetterSettler)
    assert isinstance(_NativeReporting(), NativeSettler)
    assert not isinstance(_NativeOnly(), NativeDeadLetterSettler)
    assert not isinstance(InMemoryTransport(), NativeDeadLetterSettler)


def test_a_native_dead_letter_that_returns_true_calls_the_observer_once() -> None:
    seen = Recorder()
    transport = _NativeReporting(result=True)
    app = _app(transport, seen)  # dead_letter off: the version-gated delivery is settled natively
    _delivery(app, _valid_body(), route_urn=GATED)

    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="", reason="")]
    assert transport.variant_calls == 1
    assert transport.settled == []  # the variant replaced settle_native, it did not double it
    assert app.dead_letter_queue is None
    assert transport.size("orders.dlq") == 0  # nothing was written to a BabelQueue DLQ


def test_a_native_dead_letter_of_a_poison_body_is_observed_too() -> None:
    seen = Recorder()
    app = _app(_NativeReporting(result=True), seen)
    _delivery(app, "not json")
    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="", reason="")]


def test_a_native_variant_that_returns_false_does_not_call_the_observer() -> None:
    seen = Recorder()
    transport = _NativeReporting(result=False)  # e.g. the lock was only abandoned
    app = _app(transport, seen)
    _delivery(app, _valid_body(), route_urn=GATED)
    assert seen.events == []
    assert transport.variant_calls == 1


def test_a_truthy_non_true_result_is_not_a_dead_letter() -> None:
    seen = Recorder()
    app = _app(_NativeReporting(result=object()), seen)
    _delivery(app, _valid_body(), route_urn=GATED)
    assert seen.events == []


def test_a_transport_with_only_native_settler_never_calls_the_observer() -> None:
    seen = Recorder()
    transport = _NativeOnly()  # the SQS shape: a native no-op
    app = _app(transport, seen)
    _delivery(app, _valid_body(), route_urn=GATED)
    assert seen.events == []
    assert len(transport.settled) == 1  # the plain NativeSettler path still runs, unchanged


def test_a_failing_native_variant_is_logged_and_not_counted(caplog: pytest.LogCaptureFixture) -> None:
    seen = Recorder()
    app = _app(_NativeReporting(error=ConnectionError("lock lost")), seen)
    with caplog.at_level(logging.ERROR, logger="babelqueue.app"):
        _delivery(app, _valid_body(), route_urn=GATED)  # does not raise: the broker redelivers
    assert seen.events == []
    assert any("Native settle" in r.getMessage() for r in caplog.records)


def test_a_dlq_that_is_on_takes_the_write_path_not_the_native_one() -> None:
    seen = Recorder()
    transport = _NativeReporting(result=True)
    app = _app(transport, seen, dead_letter=True)
    _delivery(app, _valid_body(), route_urn=GATED)
    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="")]
    assert transport.variant_calls == 0


# -- Kafka-style poison route ---------------------------------------------------------------


def test_a_poison_route_delivery_reports_the_poison_reason() -> None:
    from babelqueue.transport import PoisonRoute

    seen = Recorder()
    app = _app(observer=seen, dead_letter=True)
    route = PoisonRoute("babelqueue:poison", "orders", "orders", 0, "not valid UTF-8")
    _delivery(app, "not json", route_urn=route)
    assert seen.events == [DeadLetterEvent(queue="orders", dead_letter_queue="orders.dlq", reason="poison")]
