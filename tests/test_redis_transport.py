"""Integration tests for the Redis transport.

Skipped unless a Redis server is reachable (the `redis` package installed and a
broker at ``BABELQUEUE_TEST_REDIS`` / localhost). The CI ``integration`` job runs
these against a Redis service; locally they skip cleanly.
"""

from __future__ import annotations

import os
import unittest
import uuid

try:
    import redis as _redis
except ImportError:  # pragma: no cover
    _redis = None

from babelqueue import BabelQueue, EnvelopeCodec

REDIS_URL = os.environ.get("BABELQUEUE_TEST_REDIS", "redis://localhost:6379/0")


def _redis_available() -> bool:
    if _redis is None:
        return False
    try:
        _redis.Redis.from_url(REDIS_URL, decode_responses=True).ping()
        return True
    except Exception:  # pragma: no cover - connection failure
        return False


@unittest.skipUnless(_redis_available(), f"no reachable Redis at {REDIS_URL}")
class RedisTransportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.queue = f"bqtest:{uuid.uuid4().hex}"
        self.client = _redis.Redis.from_url(REDIS_URL, decode_responses=True)

    def tearDown(self) -> None:
        self.client.delete(self.queue, f"{self.queue}:processing", f"{self.queue}.dlq")

    def test_publish_consume_round_trip_and_ack(self) -> None:
        app = BabelQueue(REDIS_URL, queue=self.queue)
        seen = {}

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            seen.update(data)

        app.publish("urn:babel:orders:created", {"order_id": 42})
        processed = app.consume(max_messages=1, timeout=2)

        self.assertEqual(processed, 1)
        self.assertEqual(seen, {"order_id": 42})
        # acked: nothing left on the queue or the processing list
        self.assertEqual(self.client.llen(self.queue), 0)
        self.assertEqual(self.client.llen(f"{self.queue}:processing"), 0)

    def test_failure_dead_letters_to_redis(self) -> None:
        app = BabelQueue(REDIS_URL, queue=self.queue, max_attempts=1, dead_letter=True)

        @app.handler("urn:babel:orders:created")
        def handle(data, meta):  # noqa: ANN001
            raise RuntimeError("boom")

        app.publish("urn:babel:orders:created", {"order_id": 1})
        app.consume(max_messages=2, timeout=2)

        self.assertEqual(self.client.llen(f"{self.queue}:processing"), 0)
        dlq = self.client.lrange(f"{self.queue}.dlq", 0, -1)
        self.assertEqual(len(dlq), 1)
        env = EnvelopeCodec.decode(dlq[0])
        self.assertEqual(env["dead_letter"]["reason"], "failed")


if __name__ == "__main__":
    unittest.main()
