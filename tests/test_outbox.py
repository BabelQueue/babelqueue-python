from __future__ import annotations

import unittest
from typing import List, Tuple

from babelqueue.codec import EnvelopeCodec
from babelqueue.outbox import (
    InMemoryOutboxStore,
    Outbox,
    OutboxRecord,
    OutboxRelay,
    OutboxRelayResult,
)
from babelqueue.transport import Transport


class FakeTransport(Transport):
    """A publish-only fake that records what the relay forwarded, optionally raising for a
    configured set of bodies so a "poison row" can be simulated without a broker."""

    def __init__(self, fail_bodies: Tuple[str, ...] = ()) -> None:
        self.published: List[Tuple[str, str]] = []  # (queue, body) in publish order
        self._fail_bodies = set(fail_bodies)

    def publish(self, queue: str, body: str) -> None:
        if body in self._fail_bodies:
            raise RuntimeError("broker down")
        self.published.append((queue, body))

    def pop(self, queue: str, timeout: float = 1.0):  # pragma: no cover - unused by the relay
        return None

    def ack(self, message) -> None:  # pragma: no cover - unused by the relay
        return None


class RecordingSleeper:
    """An injected sleeper that records each requested delay instead of sleeping, so backoff
    growth/capping is asserted without real time passing."""

    def __init__(self) -> None:
        self.delays: List[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def _envelope(order_id: int, trace_id: str = "trace-abc") -> dict:
    return EnvelopeCodec.make(
        "urn:babel:orders:created", {"order_id": order_id}, trace_id=trace_id
    )


class OutboxWriteTest(unittest.TestCase):
    def test_write_stores_encoded_envelope_verbatim(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        envelope = _envelope(1042)

        row_id = outbox.write(envelope)

        [record] = store.fetch_unpublished(10)
        self.assertEqual(record.id, row_id)
        # The stored bytes are exactly the codec output — never decoded/rebuilt/re-encoded.
        self.assertEqual(record.body, EnvelopeCodec.encode(envelope))

    def test_write_captures_meta_queue(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        envelope = EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders")

        outbox.write(envelope)

        [record] = store.fetch_unpublished(10)
        self.assertEqual(record.queue, "orders")

    def test_write_falls_back_to_default_queue(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        # An envelope with no usable meta.queue → "default".
        outbox.write({"job": "urn:x", "trace_id": "t", "data": {}, "meta": {}, "attempts": 0})

        [record] = store.fetch_unpublished(10)
        self.assertEqual(record.queue, "default")


class OutboxRelayTest(unittest.TestCase):
    def test_flush_publishes_and_marks_published(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        transport = FakeTransport()
        outbox.write(EnvelopeCodec.make("urn:babel:orders:created", {}, queue="orders"))

        result = OutboxRelay(transport, store).flush()

        self.assertEqual(result, OutboxRelayResult(published=1, failed=0))
        self.assertEqual(result.attempted, 1)
        self.assertEqual(len(transport.published), 1)
        self.assertEqual(transport.published[0][0], "orders")  # (queue, body) order
        self.assertEqual(store.pending_count(), 0)  # marked published → no longer pending

    def test_relay_publishes_bytes_verbatim_preserving_trace_id(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        transport = FakeTransport()
        envelope = _envelope(7, trace_id="keep-me-1234")
        encoded = EnvelopeCodec.encode(envelope)
        outbox.write(envelope)

        OutboxRelay(transport, store).flush()

        # The body that reached the transport is byte-identical to what was stored (GR-1/GR-5)…
        _queue, published_body = transport.published[0]
        self.assertEqual(published_body, encoded)
        # …and trace_id survives end-to-end (GR-4) without the relay ever decoding.
        self.assertEqual(EnvelopeCodec.decode(published_body)["trace_id"], "keep-me-1234")

    def test_failed_publish_marks_failed_leaves_pending_and_continues_batch(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        # Build each envelope once (make() mints a fresh meta.id/created_at per call), so the
        # bytes the fake transport fails on are exactly the bytes that were stored.
        e1, e2, e3 = _envelope(1), _envelope(2), _envelope(3)
        good1 = EnvelopeCodec.encode(e1)
        poison = EnvelopeCodec.encode(e2)
        good2 = EnvelopeCodec.encode(e3)
        # Write three rows; the middle one is the poison row that always fails to publish.
        id1 = outbox.write(e1)
        poison_id = outbox.write(e2)
        id3 = outbox.write(e3)

        transport = FakeTransport(fail_bodies=(poison,))
        sleeper = RecordingSleeper()
        result = OutboxRelay(transport, store, sleeper=sleeper).flush()

        # The two good rows published; the poison row failed but did not abort the batch.
        self.assertEqual(result, OutboxRelayResult(published=2, failed=1))
        self.assertEqual([b for _q, b in transport.published], [good1, good2])
        # Poison row stays pending for a later retry, with its attempt counted + error stored.
        self.assertEqual(store.pending_count(), 1)
        self.assertEqual(store.attempts_of(poison_id), 1)
        self.assertIn("RuntimeError: broker down", store.last_error_of(poison_id))
        # The good rows are gone (published), the poison id remains.
        remaining = [r.id for r in store.fetch_unpublished(10)]
        self.assertEqual(remaining, [poison_id])
        self.assertNotIn(id1, remaining)
        self.assertNotIn(id3, remaining)
        # A backoff was slept once for the single failure.
        self.assertEqual(len(sleeper.delays), 1)

    def test_drain_loops_until_empty(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        # More rows than one batch → drain must loop across multiple flush passes.
        for i in range(5):
            outbox.write(_envelope(i))
        transport = FakeTransport()

        result = OutboxRelay(transport, store, batch_size=2).drain()

        self.assertEqual(result.published, 5)
        self.assertEqual(result.failed, 0)
        self.assertEqual(store.pending_count(), 0)
        self.assertEqual(len(transport.published), 5)

    def test_drain_stops_when_only_failing_rows_remain(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        e = _envelope(99)
        poison = EnvelopeCodec.encode(e)
        outbox.write(e)
        # A transport that always fails → no progress → drain must not spin forever.
        transport = FakeTransport(fail_bodies=(poison,))
        sleeper = RecordingSleeper()

        result = OutboxRelay(transport, store, sleeper=sleeper).drain()

        self.assertEqual(result.published, 0)
        self.assertEqual(result.failed, 1)  # exactly one pass: it published nothing, so it stopped
        self.assertEqual(store.pending_count(), 1)

    def test_backoff_grows_with_attempts_and_caps(self) -> None:
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        e = _envelope(1)
        poison = EnvelopeCodec.encode(e)
        poison_id = outbox.write(e)
        transport = FakeTransport(fail_bodies=(poison,))
        sleeper = RecordingSleeper()
        relay = OutboxRelay(
            transport, store, backoff_step=0.05, backoff_cap=0.2, sleeper=sleeper
        )

        # Each flush retries the same poison row, whose attempts climb 0,1,2,3,4…
        # backoff_for(attempts) = 0.05 * (attempts + 1), capped at 0.2:
        #   attempts=0 → 0.05, 1 → 0.10, 2 → 0.15, 3 → 0.20, 4 → 0.20 (capped).
        for _ in range(5):
            relay.flush()

        self.assertEqual(store.attempts_of(poison_id), 5)
        self.assertEqual(
            [round(d, 2) for d in sleeper.delays],
            [0.05, 0.10, 0.15, 0.20, 0.20],
        )
        # Growth then a hard cap.
        self.assertEqual(max(sleeper.delays), 0.20)

    def test_default_sleeper_is_skipped_for_zero_delay(self) -> None:
        # A non-positive backoff never calls the sleeper (e.g. backoff_step=0).
        store = InMemoryOutboxStore()
        outbox = Outbox(store)
        e = _envelope(1)
        poison = EnvelopeCodec.encode(e)
        outbox.write(e)
        transport = FakeTransport(fail_bodies=(poison,))
        sleeper = RecordingSleeper()

        OutboxRelay(transport, store, backoff_step=0.0, sleeper=sleeper).flush()

        self.assertEqual(sleeper.delays, [])


class InMemoryOutboxStoreTest(unittest.TestCase):
    def test_fetch_unpublished_is_oldest_first_and_limited(self) -> None:
        store = InMemoryOutboxStore()
        ids = [store.save(f"body-{i}", "q") for i in range(3)]

        records = store.fetch_unpublished(2)

        self.assertEqual([r.id for r in records], ids[:2])  # oldest-first, capped at limit
        self.assertTrue(all(isinstance(r, OutboxRecord) for r in records))

    def test_mark_published_removes_from_pending(self) -> None:
        store = InMemoryOutboxStore()
        a = store.save("a", "q")
        b = store.save("b", "q")

        store.mark_published([a])

        self.assertEqual(store.pending_count(), 1)
        self.assertEqual([r.id for r in store.fetch_unpublished(10)], [b])

    def test_mark_failed_bumps_attempts_and_records_error(self) -> None:
        store = InMemoryOutboxStore()
        a = store.save("a", "q")

        store.mark_failed(a, "boom")
        store.mark_failed(a, "boom-again")

        self.assertEqual(store.attempts_of(a), 2)
        self.assertEqual(store.last_error_of(a), "boom-again")
        self.assertEqual(store.pending_count(), 1)  # still pending after failures

    def test_unknown_ids_are_ignored(self) -> None:
        store = InMemoryOutboxStore()
        store.mark_published(["nope"])  # no row → no error
        store.mark_failed("nope", "x")  # no row → no error
        self.assertEqual(store.attempts_of("nope"), 0)
        self.assertEqual(store.last_error_of("nope"), "")


if __name__ == "__main__":
    unittest.main()
