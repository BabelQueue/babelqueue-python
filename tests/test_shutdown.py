"""Graceful shutdown of the consume loop (SIGTERM/SIGINT, ``shutdown_timeout``).

Signals are raised in-process (``signal.raise_signal``) from inside the handler or the
transport, so the tests need no subprocess and no broker. Every test asserts the invariant
that matters: the in-flight message is either acked or released — never lost, never both.
"""

from __future__ import annotations

import signal
import threading
import time
import unittest
from typing import List, Optional, Tuple

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.transport import InMemoryTransport, ReceivedMessage

URN = "urn:babel:orders:created"
HAS_SIGTERM = hasattr(signal, "SIGTERM") and hasattr(signal, "raise_signal")
HAS_PTHREAD_KILL = hasattr(signal, "pthread_kill")


class RecordingTransport(InMemoryTransport):
    """In-memory transport that records acks, native releases and close()."""

    def __init__(self) -> None:
        super().__init__()
        self.acked: List[str] = []
        self.released: List[Tuple[str, str, float]] = []
        self.closed = 0
        self.on_pop = None  # optional hook run before each pop

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        if self.on_pop is not None:
            self.on_pop()
        return super().pop(queue, timeout)

    def ack(self, message: ReceivedMessage) -> None:
        self.acked.append(message.body)

    def redeliver(self, message: ReceivedMessage, body: str, delay: float) -> None:
        self.released.append((message.body, body, delay))
        self.publish(message.queue, message.body)  # back on the queue, like a broker

    def close(self) -> None:
        self.closed += 1


def _app(**kw) -> Tuple[BabelQueue, RecordingTransport]:
    transport = RecordingTransport()
    return BabelQueue(transport=transport, queue="orders", **kw), transport


def _body(app: BabelQueue, order_id: int) -> str:
    app.publish(URN, {"order_id": order_id})
    return app.transport._queues["orders"][-1]  # type: ignore[attr-defined]


@unittest.skipUnless(HAS_SIGTERM, "needs SIGTERM + signal.raise_signal")
class GracefulShutdownTest(unittest.TestCase):
    def setUp(self) -> None:
        self.before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}

    def tearDown(self) -> None:
        for signum, handler in self.before.items():
            self.assertIs(signal.getsignal(signum), handler, "previous handler restored")
            signal.signal(signum, handler)

    def test_sigterm_lets_in_flight_finish_then_acks_and_stops(self) -> None:
        app, tr = _app(shutdown_timeout=5)
        first = _body(app, 1)
        _body(app, 2)
        calls: List[int] = []

        @app.handler(URN)
        def handle(data, meta):
            calls.append(data["order_id"])
            signal.raise_signal(signal.SIGTERM)
            time.sleep(0.05)  # keeps working after the signal
            calls.append(-data["order_id"])  # reached: not interrupted

        processed = app.consume()  # no max_messages: only the signal ends it

        self.assertEqual(processed, 1)
        self.assertEqual(calls, [1, -1])  # handled exactly once, to completion
        self.assertEqual(tr.acked, [first])  # acked ...
        self.assertEqual(tr.released, [])  # ... and not released
        self.assertEqual(tr.size("orders"), 1)  # the next message was not taken
        self.assertEqual(tr.closed, 1)  # transport closed on shutdown

    def test_sigint_is_graceful_too(self) -> None:
        app, tr = _app(shutdown_timeout=5)
        first = _body(app, 1)

        @app.handler(URN)
        def handle(data, meta):
            signal.raise_signal(signal.SIGINT)

        self.assertEqual(app.consume(), 1)
        self.assertEqual(tr.acked, [first])
        self.assertEqual(tr.released, [])
        self.assertEqual(tr.closed, 1)

    @unittest.skipUnless(HAS_PTHREAD_KILL, "deadline interrupt needs signal.pthread_kill")
    def test_timeout_releases_in_flight_message(self) -> None:
        app, tr = _app(shutdown_timeout=0.2)
        first = _body(app, 1)
        calls: List[str] = []

        @app.handler(URN)
        def handle(data, meta):
            calls.append("start")
            signal.raise_signal(signal.SIGTERM)
            time.sleep(5)  # a handler that would outlive the shutdown budget
            calls.append("end")

        started = time.monotonic()
        processed = app.consume()
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 4)  # interrupted at the deadline, not after 5 s
        self.assertEqual(processed, 0)
        self.assertEqual(calls, ["start"])  # interrupted, never completed
        self.assertEqual(tr.acked, [])  # not acked ...
        self.assertEqual(tr.released, [(first, first, 0.0)])  # ... released unchanged
        self.assertEqual(tr.size("orders"), 1)  # not lost: back on the queue
        self.assertEqual(EnvelopeCodec.decode(tr._queues["orders"][0])["attempts"], 0)
        self.assertEqual(tr.closed, 1)

    def test_zero_timeout_releases_immediately(self) -> None:
        app, tr = _app(shutdown_timeout=0)
        first = _body(app, 1)
        calls: List[str] = []

        @app.handler(URN)
        def handle(data, meta):
            signal.raise_signal(signal.SIGTERM)
            calls.append("after-signal")

        self.assertEqual(app.consume(), 0)
        self.assertEqual(calls, [])
        self.assertEqual(tr.acked, [])
        self.assertEqual(tr.released, [(first, first, 0.0)])

    def test_second_signal_forces_exit_and_releases(self) -> None:
        app, tr = _app(shutdown_timeout=30)
        first = _body(app, 1)

        @app.handler(URN)
        def handle(data, meta):
            signal.raise_signal(signal.SIGTERM)
            signal.raise_signal(signal.SIGTERM)  # impatient operator
            raise AssertionError("unreachable")  # pragma: no cover

        self.assertEqual(app.consume(), 0)  # KeyboardInterrupt swallowed, as before
        self.assertEqual(tr.acked, [])
        self.assertEqual(tr.released, [(first, first, 0.0)])
        self.assertEqual(tr.closed, 1)

    def test_signal_while_idle_releases_a_message_popped_after_stop(self) -> None:
        app, tr = _app()
        first = _body(app, 1)
        calls: List[int] = []
        app.register(URN, lambda data, meta: calls.append(1))

        def stop_during_pop() -> None:
            tr.on_pop = None
            signal.raise_signal(signal.SIGTERM)  # arrives while the loop waits on the broker

        tr.on_pop = stop_during_pop
        self.assertEqual(app.consume(), 0)
        self.assertEqual(calls, [])  # not handled after the stop
        self.assertEqual(tr.acked, [])
        self.assertEqual(tr.released, [(first, first, 0.0)])
        self.assertEqual(tr.closed, 1)

    def test_default_release_republishes_without_redeliverer(self) -> None:
        app = BabelQueue("memory://", queue="orders", shutdown_timeout=0)
        app.publish(URN, {"order_id": 1})
        body = app.transport._queues["orders"][0]  # type: ignore[attr-defined]

        @app.handler(URN)
        def handle(data, meta):
            signal.raise_signal(signal.SIGTERM)

        self.assertEqual(app.consume(), 0)
        self.assertEqual(list(app.transport._queues["orders"]), [body])  # type: ignore[attr-defined]

    def test_stop_flag_is_reset_for_the_next_consume(self) -> None:
        app, tr = _app(shutdown_timeout=5)
        _body(app, 1)
        second = _body(app, 2)
        seen: List[int] = []

        @app.handler(URN)
        def handle(data, meta):
            seen.append(data["order_id"])
            if data["order_id"] == 1:
                signal.raise_signal(signal.SIGTERM)

        self.assertEqual(app.consume(), 1)
        self.assertEqual(app.consume(max_messages=1, timeout=0), 1)
        self.assertEqual(seen, [1, 2])
        self.assertEqual(tr.acked[-1], second)

    def test_handle_signals_false_keeps_keyboard_interrupt_behaviour(self) -> None:
        app, tr = _app()
        first = _body(app, 1)

        @app.handler(URN)
        def handle(data, meta):
            self.assertIs(signal.getsignal(signal.SIGINT), self.before[signal.SIGINT])
            raise KeyboardInterrupt

        self.assertEqual(app.consume(handle_signals=False), 0)  # swallowed, as before
        self.assertEqual(tr.acked, [])
        self.assertEqual(tr.released, [(first, first, 0.0)])  # interrupted -> released
        self.assertEqual(tr.closed, 0)  # not a signal-driven shutdown


class OffMainThreadTest(unittest.TestCase):
    def test_no_handlers_off_main_thread_and_stop_ends_loop(self) -> None:
        app, tr = _app()
        before = signal.getsignal(signal.SIGINT)
        seen: List[object] = []

        @app.handler(URN)
        def handle(data, meta):
            seen.append(signal.getsignal(signal.SIGINT))
            app.stop()

        _body(app, 1)
        _body(app, 2)
        result: List[int] = []
        worker = threading.Thread(target=lambda: result.append(app.consume(timeout=0.01)))
        worker.start()
        worker.join(5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(result, [1])  # stop() honoured after the in-flight message
        self.assertEqual(seen, [before])  # nothing installed from a worker thread
        self.assertEqual(tr.size("orders"), 1)
        self.assertEqual(tr.closed, 0)


class StopBeforeConsumeTest(unittest.TestCase):
    def test_stop_requested_before_consume_starts_is_honoured(self) -> None:
        """A stop() that lands before the worker thread reaches the loop must not be wiped."""
        app, tr = _app()
        _body(app, 1)
        app.stop()
        result: List[int] = []
        worker = threading.Thread(target=lambda: result.append(app.consume(timeout=0.01)))
        worker.start()
        worker.join(5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(result, [0])
        self.assertEqual(tr.size("orders"), 1)  # nothing taken
        # the flag is reset on exit, so the next run consumes normally
        app.register(URN, lambda data, meta: None)
        self.assertEqual(app.consume(max_messages=1, timeout=0, handle_signals=False), 1)


class RawReleaseForbiddenKeysTest(unittest.TestCase):
    def test_raw_body_republish_drops_forbidden_keys(self) -> None:
        """Unknown-URN RELEASE on a transport without native release re-publishes the raw
        body; a legacy forbidden key must not ride along after decode reported it dropped."""
        from babelqueue import UnknownUrnStrategy

        transport = InMemoryTransport()
        app = BabelQueue(
            transport=transport, queue="orders", on_unknown_urn=UnknownUrnStrategy.RELEASE
        )
        env = EnvelopeCodec.make("urn:babel:nobody:listens", {"a": 1}, queue="orders")
        legacy = EnvelopeCodec.encode(env)[:-1] + ',"timestamp":1}'
        transport.publish("orders", legacy)

        with self.assertLogs("babelqueue.codec", level="WARNING"):
            self.assertEqual(app.consume(max_messages=1, timeout=0, handle_signals=False), 1)

        released = transport._queues["orders"]  # type: ignore[attr-defined]
        self.assertEqual(len(released), 1)
        self.assertNotIn("timestamp", released[0])
        self.assertEqual(EnvelopeCodec.decode(released[0]), EnvelopeCodec.decode(legacy))

    def test_clean_raw_body_is_republished_byte_for_byte(self) -> None:
        from babelqueue import UnknownUrnStrategy

        transport = InMemoryTransport()
        app = BabelQueue(
            transport=transport, queue="orders", on_unknown_urn=UnknownUrnStrategy.RELEASE
        )
        raw = '{"job":"urn:babel:x:y", "trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0}'
        transport.publish("orders", raw)
        app.consume(max_messages=1, timeout=0, handle_signals=False)
        self.assertEqual(list(transport._queues["orders"]), [raw])  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
