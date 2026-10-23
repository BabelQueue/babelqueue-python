"""The runtime: publish, @handler routing, retry and dead-lettering.

All tests use the in-memory transport — no broker required.
"""

from __future__ import annotations

import unittest

from babelqueue import BabelQueue, EnvelopeCodec, UnknownUrnStrategy
from babelqueue.transport import InMemoryTransport, ReceivedMessage


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

    def test_route_urn_overrides_the_body_urn_without_rewriting_the_body(self) -> None:
        app = BabelQueue("memory://", queue="orders")
        seen = []

        @app.handler("urn:babel:orders:created")
        def handle(data, meta, message):  # noqa: ANN001
            seen.append(message["job"])

        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:billing:invoiced", {}, queue="orders"))
        app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn="urn:babel:orders:created"))

        self.assertEqual(seen, ["urn:babel:billing:invoiced"])  # handler sees the original body

    def test_route_urn_never_rescues_an_undecodable_body(self) -> None:
        app = BabelQueue("memory://", queue="orders")
        seen = []

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen.append(data)

        app.dispatch(ReceivedMessage(body="not-json", queue="orders", route_urn="urn:babel:orders:created"))

        self.assertEqual(seen, [])  # poison path (unknown URN / retry), never the handler

    def test_retry_and_release_republish_carry_the_route_urn(self) -> None:
        class RoutedMemory(InMemoryTransport):
            def __init__(self) -> None:
                super().__init__()
                self.routed = []

            def publish_routed(self, queue, body, route_urn):  # noqa: ANN001
                self.routed.append((queue, body, route_urn))

        route = "urn:babel:orders:created"
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:billing:invoiced", {}, queue="orders"))

        tr = RoutedMemory()
        app = BabelQueue(transport=tr, queue="orders", max_attempts=3)

        @app.handler(route)
        def boom(data, meta):  # noqa: ANN001
            raise RuntimeError("boom")

        app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn=route))
        self.assertEqual([r[2] for r in tr.routed], [route])
        self.assertEqual(EnvelopeCodec.decode(tr.routed[0][1])["attempts"], 1)

        tr2 = RoutedMemory()
        app2 = BabelQueue(transport=tr2, queue="orders", on_unknown_urn=UnknownUrnStrategy.RELEASE)
        app2.dispatch(ReceivedMessage(body=body, queue="orders", route_urn="urn:babel:orders:nope"))
        self.assertEqual([(r[1], r[2]) for r in tr2.routed], [(body, "urn:babel:orders:nope")])

        tr3 = RoutedMemory()  # no route_urn -> plain publish, not publish_routed
        app3 = BabelQueue(transport=tr3, queue="orders", max_attempts=3)
        app3.handler("urn:babel:billing:invoiced")(boom)
        app3.dispatch(ReceivedMessage(body=body, queue="orders"))
        self.assertEqual(tr3.routed, [])
        self.assertEqual(tr3.size("orders"), 1)

    def test_dead_letter_write_carries_the_route_urn(self) -> None:
        class RoutedMemory(InMemoryTransport):
            def __init__(self) -> None:
                super().__init__()
                self.routed = []

            def publish_routed(self, queue, body, route_urn):  # noqa: ANN001
                self.routed.append((queue, body, route_urn))

        route = "urn:babel:orders:created"
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:billing:invoiced", {}, queue="orders"))

        tr = RoutedMemory()
        app = BabelQueue(transport=tr, queue="orders", max_attempts=1, dead_letter=True)

        @app.handler(route)
        def boom(data, meta):  # noqa: ANN001
            raise RuntimeError("boom")

        app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn=route))
        self.assertEqual([(r[0], r[2]) for r in tr.routed], [("orders.dlq", route)])
        self.assertEqual(EnvelopeCodec.decode(tr.routed[0][1])["dead_letter"]["original_queue"], "orders")

        tr2 = RoutedMemory()  # no route_urn -> plain publish into the DLQ
        app2 = BabelQueue(transport=tr2, queue="orders", max_attempts=1, dead_letter=True)
        app2.handler("urn:babel:billing:invoiced")(boom)
        app2.dispatch(ReceivedMessage(body=body, queue="orders"))
        self.assertEqual(tr2.routed, [])
        self.assertEqual(tr2.size("orders.dlq"), 1)

    def test_empty_route_urn_routes_on_the_body_urn(self) -> None:
        app = BabelQueue("memory://", queue="orders")
        seen = []

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen.append(data)

        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {"a": 1}, queue="orders"))
        app.dispatch(ReceivedMessage(body=body, queue="orders"))

        self.assertEqual(seen, [{"a": 1}])

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

    def _redeliver_until_settled(self, app, queue, route_urn, first_body, limit=6):
        """Feed ``first_body`` through ``dispatch`` and keep redelivering whatever the runtime
        re-publishes to ``queue`` (as a broker would); returns the number of deliveries."""
        tr = app.transport
        body = first_body
        deliveries = 0
        while body is not None and deliveries < limit:
            deliveries += 1
            app.dispatch(ReceivedMessage(body=body, queue=queue, route_urn=route_urn))
            body = tr._queues[queue].popleft() if tr._queues.get(queue) else None  # noqa: SLF001
        return deliveries

    def test_version_gated_prefix_is_the_same_in_every_gating_transport(self) -> None:
        from babelqueue import (
            artemis_transport, asb_transport, kafka_transport, pulsar_transport, sqs_transport,
        )
        from babelqueue.app import _UNSUPPORTED_SCHEMA_ROUTE_PREFIX

        for module in (
            artemis_transport, asb_transport, kafka_transport, pulsar_transport, sqs_transport,
        ):
            with self.subTest(module=module.__name__):
                self.assertEqual(
                    _UNSUPPORTED_SCHEMA_ROUTE_PREFIX, module.UNSUPPORTED_SCHEMA_ROUTE_PREFIX
                )

    def test_version_gated_message_is_dead_lettered_directly_without_retry(self) -> None:
        from babelqueue.app import _UNSUPPORTED_SCHEMA_ROUTE_PREFIX

        route = _UNSUPPORTED_SCHEMA_ROUTE_PREFIX + "2"
        decodable = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))
        for strategy in (UnknownUrnStrategy.RELEASE, UnknownUrnStrategy.FAIL, UnknownUrnStrategy.DEAD_LETTER):
            for name, body in (("decodable", decodable), ("undecodable", "{not json")):
                with self.subTest(strategy=strategy, body=name):
                    app = BabelQueue(
                        "memory://", queue="orders", max_attempts=5, dead_letter=True,
                        on_unknown_urn=strategy,
                    )
                    seen = []
                    app.handler("urn:babel:orders:created")(lambda d, m: seen.append(d))

                    deliveries = self._redeliver_until_settled(app, "orders", route, body)

                    self.assertEqual(deliveries, 1)  # one delivery, max_attempts notwithstanding
                    self.assertEqual(seen, [])  # never reaches the handler
                    self.assertEqual(app.transport.size("orders"), 0)
                    self.assertEqual(app.transport.size("orders.dlq"), 1)
                    # The gate rejects without decoding: the DLQ record is the incoming raw body,
                    # byte-identical, with no dead_letter block (even for an undecodable body).
                    dead = app.transport._queues["orders.dlq"][0]  # noqa: SLF001
                    self.assertEqual(dead, body)
                    self.assertNotIn("dead_letter", dead)

    def test_version_gated_message_whose_dead_letter_write_fails_is_not_retried(self) -> None:
        from babelqueue.app import _UNSUPPORTED_SCHEMA_ROUTE_PREFIX

        class FailingDlq(InMemoryTransport):
            def __init__(self) -> None:
                super().__init__()
                self.acked = 0

            def publish(self, queue, body) -> None:  # noqa: ANN001
                if queue.endswith(".dlq"):
                    raise RuntimeError("dlq write refused")
                super().publish(queue, body)

            def ack(self, received) -> None:  # noqa: ANN001
                self.acked += 1

        route = _UNSUPPORTED_SCHEMA_ROUTE_PREFIX + "2"
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))
        for strategy in (UnknownUrnStrategy.RELEASE, UnknownUrnStrategy.FAIL, UnknownUrnStrategy.DEAD_LETTER):
            with self.subTest(strategy=strategy):
                tr = FailingDlq()
                app = BabelQueue(
                    transport=tr, queue="orders", max_attempts=5, dead_letter=True,
                    on_unknown_urn=strategy,
                )
                seen = []
                app.handler("urn:babel:orders:created")(lambda d, m: seen.append(d))

                for _ in range(3):  # the broker keeps redelivering it
                    with self.assertLogs("babelqueue.app", level="ERROR"):
                        app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn=route))

                self.assertEqual(seen, [])
                self.assertEqual(tr.size("orders"), 0)  # no retry re-publish
                self.assertEqual(tr.acked, 0)  # left for redelivery

    def test_failed_gate_dead_letter_write_rewinds_a_rewinder_transport(self) -> None:
        from babelqueue.app import _UNSUPPORTED_SCHEMA_ROUTE_PREFIX

        class RewindingDlq(InMemoryTransport):
            def __init__(self, fail_rewind: bool = False) -> None:
                super().__init__()
                self.rewound = []
                self.fail_rewind = fail_rewind

            def publish(self, queue, body) -> None:  # noqa: ANN001
                if queue.endswith(".dlq"):
                    raise RuntimeError("dlq write refused")
                super().publish(queue, body)

            def rewind(self, message) -> None:  # noqa: ANN001
                self.rewound.append(message)
                if self.fail_rewind:
                    raise RuntimeError("seek refused")

        route = _UNSUPPORTED_SCHEMA_ROUTE_PREFIX + "2"
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))
        for fail_rewind in (False, True):
            with self.subTest(fail_rewind=fail_rewind):
                tr = RewindingDlq(fail_rewind=fail_rewind)
                app = BabelQueue(transport=tr, queue="orders", max_attempts=5, dead_letter=True)
                app._rewind_pause = 0.0  # noqa: SLF001
                received = ReceivedMessage(body=body, queue="orders", route_urn=route)
                with self.assertLogs("babelqueue.app", level="ERROR"):
                    app.dispatch(received)
                self.assertEqual(tr.rewound, [received])
                self.assertEqual(tr.size("orders"), 0)

    def test_version_gated_message_without_dead_letter_is_dropped_without_retry(self) -> None:
        route = "babelqueue:unsupported-schema-version:x"
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))
        for strategy in (UnknownUrnStrategy.RELEASE, UnknownUrnStrategy.FAIL):
            with self.subTest(strategy=strategy):
                app = BabelQueue("memory://", queue="orders", max_attempts=5, on_unknown_urn=strategy)
                self.assertEqual(self._redeliver_until_settled(app, "orders", route, body), 1)
                self.assertEqual(app.transport.size("orders"), 0)

    def test_dropping_a_version_gated_message_without_a_dead_letter_queue_is_logged(self) -> None:
        route = "babelqueue:unsupported-schema-version:x"
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))
        app = BabelQueue("memory://", queue="orders", max_attempts=5)
        with self.assertLogs("babelqueue.app", level="WARNING") as logs:
            app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn=route))
        self.assertIn("Dropping", logs.output[0])
        self.assertIn("orders", logs.output[0])

    def test_unknown_urn_release_still_releases_verbatim(self) -> None:
        body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:nope", {}, queue="orders"))
        app = BabelQueue(
            "memory://", queue="orders", max_attempts=2, dead_letter=True,
            on_unknown_urn=UnknownUrnStrategy.RELEASE,
        )
        for route_urn in ("", "urn:babel:orders:nope"):
            with self.subTest(route_urn=route_urn):
                app.dispatch(ReceivedMessage(body=body, queue="orders", route_urn=route_urn))
                self.assertEqual(app.transport.size("orders.dlq"), 0)
                self.assertEqual(app.transport._queues["orders"].popleft(), body)  # noqa: SLF001

    def test_ungated_poison_is_bounded_under_release(self) -> None:
        # Not version-gated: an undecodable body and a body naming no URN can never gain a handler
        # either, so RELEASE takes the bounded FAIL path for them too (Go routePoison parity).
        no_urn = EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))
        no_urn = no_urn.replace('"urn:babel:orders:created"', '""')
        for name, body in (("undecodable", "{not json"), ("no-urn", no_urn)):
            with self.subTest(body=name):
                app = BabelQueue(
                    "memory://", queue="orders", max_attempts=3, dead_letter=True,
                    on_unknown_urn=UnknownUrnStrategy.RELEASE,
                )
                self.assertEqual(self._redeliver_until_settled(app, "orders", "", body), 3)
                self.assertEqual(app.transport.size("orders"), 0)
                self.assertEqual(app.transport.size("orders.dlq"), 1)

    def test_unsupported_broker_scheme_raises(self) -> None:
        from babelqueue import BabelQueueError

        with self.assertRaises(BabelQueueError):
            BabelQueue("frobnicate://localhost:9092")


if __name__ == "__main__":
    unittest.main()
