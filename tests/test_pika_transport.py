"""Integration tests for the RabbitMQ (pika) transport.

Skipped unless a broker is reachable (the `pika` package installed and a broker at
``BABELQUEUE_TEST_AMQP`` / localhost). The CI ``integration`` job runs these
against a RabbitMQ service; locally they skip cleanly.
"""

from __future__ import annotations

import os
import time
import unittest
import uuid

try:
    import pika as _pika
except ImportError:  # pragma: no cover
    _pika = None

from babelqueue import BabelQueue, EnvelopeCodec

AMQP_URL = os.environ.get("BABELQUEUE_TEST_AMQP", "amqp://guest:guest@localhost:5672/")


def _amqp_available() -> bool:
    if _pika is None:
        return False
    try:
        conn = _pika.BlockingConnection(_pika.URLParameters(AMQP_URL))
        conn.close()
        return True
    except Exception:  # pragma: no cover - connection failure
        return False


@unittest.skipUnless(_amqp_available(), f"no reachable RabbitMQ at {AMQP_URL}")
class PikaTransportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = f"bqtest-{uuid.uuid4().hex}"
        self.conn = _pika.BlockingConnection(_pika.URLParameters(AMQP_URL))
        self.ctl = self.conn.channel()

    def tearDown(self) -> None:
        for q in (self.queue, f"{self.queue}.dlq"):
            try:
                self.ctl.queue_delete(queue=q)
            except Exception:
                pass
        self.conn.close()

    def _depth(self, queue: str) -> int:
        method = self.ctl.queue_declare(queue=queue, durable=True, passive=True)
        return method.method.message_count

    def _get(self, queue: str, timeout: float = 5.0):
        """basic_get with a short poll. A message published on a separate
        channel is not always retrievable on the very next get, so wait
        briefly for it rather than asserting on a single immediate poll —
        otherwise this races and flakes under CI load."""
        deadline = time.monotonic() + timeout
        while True:
            frame = self.ctl.basic_get(queue=queue, auto_ack=True)
            if frame[0] is not None or time.monotonic() >= deadline:
                return frame
            time.sleep(0.05)

    def test_publish_consume_round_trip_and_ack(self) -> None:
        app = BabelQueue(AMQP_URL, queue=self.queue)
        seen = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen.update(data)

        app.publish("urn:babel:orders:created", {"order_id": 42})
        processed = app.consume(max_messages=1, timeout=3)

        self.assertEqual(processed, 1)
        self.assertEqual(seen, {"order_id": 42})
        self.assertEqual(self._depth(self.queue), 0)  # acked

    def test_publish_sets_contract_amqp_properties(self) -> None:
        app = BabelQueue(AMQP_URL, queue=self.queue)
        app.publish("urn:babel:orders:created", {"order_id": 1}, trace_id="trace-amqp")

        method, props, body = self._get(self.queue)
        self.assertIsNotNone(method)
        self.assertEqual(props.type, "urn:babel:orders:created")     # route on properties.type
        self.assertEqual(props.correlation_id, "trace-amqp")         # trace_id
        self.assertEqual(props.content_type, "application/json")
        self.assertEqual(props.delivery_mode, 2)                     # persistent
        self.assertEqual(props.app_id, "babelqueue")

    def test_failure_dead_letters(self) -> None:
        app = BabelQueue(AMQP_URL, queue=self.queue, max_attempts=1, dead_letter=True)

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            raise RuntimeError("boom")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        app.consume(max_messages=2, timeout=3)

        self.assertEqual(self._depth(f"{self.queue}.dlq"), 1)
        _m, _p, body = self._get(f"{self.queue}.dlq")
        env = EnvelopeCodec.decode(body.decode("utf-8"))
        self.assertEqual(env["dead_letter"]["reason"], "failed")


if __name__ == "__main__":
    unittest.main()
