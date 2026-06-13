"""The runtime: publish, @handler routing, retry and dead-lettering.

All tests use the in-memory transport — no broker required.
"""

from __future__ import annotations

import unittest

from babelqueue import BabelQueue, EnvelopeCodec, UnknownUrnStrategy


class AppTest(unittest.TestCase):
    def test_publish_then_consume_invokes_handler_and_acks(self) -> None:
        app = BabelQueue("memory://", queue="orders")
        seen = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen["data"] = data
            seen["meta"] = meta

        msg_id = app.publish("urn:babel:orders:created", {"order_id": 7})
        processed = app.consume(max_messages=1)

        self.assertEqual(processed, 1)
        self.assertEqual(seen["data"], {"order_id": 7})
        self.assertEqual(seen["meta"]["lang"], "python")
        self.assertEqual(seen["meta"]["id"], msg_id)
        self.assertEqual(app.transport.size("orders"), 0)  # acked / drained

    def test_three_arg_handler_receives_full_envelope(self) -> None:
        app = BabelQueue("memory://")
        seen = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta, message):  # noqa: ANN001
            seen["trace"] = message["trace_id"]
            seen["job"] = message["job"]

        app.publish("urn:babel:orders:created", {}, trace_id="trace-1")
        app.consume(max_messages=1)

        self.assertEqual(seen["trace"], "trace-1")
        self.assertEqual(seen["job"], "urn:babel:orders:created")

    def test_round_trips_with_the_canonical_codec(self) -> None:
        app = BabelQueue("memory://", queue="orders")
        captured = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            captured.update(data)

        app.publish("urn:babel:orders:created", {"order_id": 9, "amount": "9.90"})
        # the raw body on the queue is a canonical envelope decodable by any SDK
        raw = app.transport._queues["orders"][0]  # noqa: SLF001 - test introspection
        env = EnvelopeCodec.decode(raw)
        self.assertEqual(list(env.keys()), ["job", "trace_id", "data", "meta", "attempts"])

        app.consume(max_messages=1)
        self.assertEqual(captured, {"order_id": 9, "amount": "9.90"})

    def test_unknown_urn_delete_drops_message(self) -> None:
        app = BabelQueue("memory://", queue="q", on_unknown_urn=UnknownUrnStrategy.DELETE)
        app.publish("urn:babel:nobody", {})
        app.consume(max_messages=1)
        self.assertEqual(app.transport.size("q"), 0)

    def test_unknown_urn_dead_letter_quarantines(self) -> None:
        app = BabelQueue(
            "memory://", queue="q",
            on_unknown_urn=UnknownUrnStrategy.DEAD_LETTER, dead_letter=True,
        )
        app.publish("urn:babel:nobody", {"x": 1})
        app.consume(max_messages=1)

        self.assertEqual(app.transport.size("q"), 0)
        dlq_raw = app.transport._queues["q.dlq"][0]  # noqa: SLF001
        env = EnvelopeCodec.decode(dlq_raw)
        self.assertEqual(env["dead_letter"]["reason"], "unknown_urn")
        self.assertEqual(env["dead_letter"]["original_queue"], "q")

    def test_handler_failure_retries_then_dead_letters(self) -> None:
        app = BabelQueue("memory://", queue="orders", max_attempts=2, dead_letter=True)
        calls = {"n": 0}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            calls["n"] += 1
            raise RuntimeError("boom")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        # attempt 1 -> requeue, attempt 2 -> dead-letter
        app.consume(max_messages=5)

        self.assertEqual(calls["n"], 2)
        self.assertEqual(app.transport.size("orders"), 0)
        dlq_raw = app.transport._queues["orders.dlq"][0]  # noqa: SLF001
        env = EnvelopeCodec.decode(dlq_raw)
        self.assertEqual(env["dead_letter"]["reason"], "failed")
        self.assertEqual(env["dead_letter"]["error"], "boom")
        self.assertEqual(env["dead_letter"]["attempts"], 2)

    def test_failure_without_dlq_drops_after_max_attempts(self) -> None:
        app = BabelQueue("memory://", queue="orders", max_attempts=1, dead_letter=False)

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            raise RuntimeError("nope")

        app.publish("urn:babel:orders:created", {})
        app.consume(max_messages=3)
        self.assertEqual(app.transport.size("orders"), 0)
        self.assertEqual(app.transport.size("orders.dlq"), 0)

    def test_unsupported_broker_scheme_raises(self) -> None:
        from babelqueue import BabelQueueError

        with self.assertRaises(BabelQueueError):
            BabelQueue("frobnicate://localhost:9092")


if __name__ == "__main__":
    unittest.main()
