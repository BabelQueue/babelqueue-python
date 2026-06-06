"""Producer side of the wire contract — canonical envelope, incl. trace_id."""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

from babelqueue import EnvelopeCodec
from babelqueue.exceptions import BabelQueueError

UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
FIXTURES = Path(__file__).parent / "fixtures"


class OrderCreated:
    """A PolyglotMessage-shaped object (duck-typed)."""

    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

    def get_babel_urn(self) -> str:
        return "urn:babel:orders:created"

    def to_payload(self) -> dict:
        return {"order_id": self.order_id}


class TracedOrder(OrderCreated):
    def __init__(self, order_id: int, trace_id) -> None:
        super().__init__(order_id)
        self._trace_id = trace_id

    def get_babel_trace_id(self):
        return self._trace_id


class EnvelopeCodecTest(unittest.TestCase):
    def test_canonical_shape(self) -> None:
        p = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042}, queue="orders")

        self.assertEqual(list(p.keys()), ["job", "trace_id", "data", "meta", "attempts"])
        self.assertEqual(p["job"], "urn:babel:orders:created")
        self.assertEqual(p["data"], {"order_id": 1042})
        self.assertEqual(p["meta"]["queue"], "orders")
        self.assertEqual(p["meta"]["lang"], "python")
        self.assertEqual(p["meta"]["schema_version"], 1)
        self.assertIsInstance(p["meta"]["created_at"], int)
        self.assertEqual(p["attempts"], 0)

    def test_forbidden_legacy_fields_absent(self) -> None:
        p = EnvelopeCodec.make("urn:babel:orders:created", {})
        self.assertNotIn("timestamp", p)
        self.assertNotIn("max_retries", p["meta"])
        self.assertNotIn("attempts", p["meta"])
        self.assertNotIn("source", p["meta"])
        self.assertNotIn("ts", p["meta"])

    def test_trace_id_is_uuid_distinct_from_meta_id(self) -> None:
        p = EnvelopeCodec.make("urn:babel:orders:created", {})
        self.assertRegex(p["trace_id"], UUID_RE)
        self.assertRegex(p["meta"]["id"], UUID_RE)
        self.assertNotEqual(p["trace_id"], p["meta"]["id"])

    def test_each_message_gets_fresh_ids(self) -> None:
        a = EnvelopeCodec.make("urn:babel:orders:created", {})
        b = EnvelopeCodec.make("urn:babel:orders:created", {})
        self.assertNotEqual(a["trace_id"], b["trace_id"])
        self.assertNotEqual(a["meta"]["id"], b["meta"]["id"])

    def test_inherited_trace_id_preserved(self) -> None:
        p = EnvelopeCodec.make("urn:babel:orders:created", {}, trace_id="trace-xyz")
        self.assertEqual(p["trace_id"], "trace-xyz")

    def test_from_message_reads_urn_and_payload(self) -> None:
        p = EnvelopeCodec.from_message(OrderCreated(7), queue="orders")
        self.assertEqual(p["job"], "urn:babel:orders:created")
        self.assertEqual(p["data"], {"order_id": 7})

    def test_from_message_inherits_trace_id(self) -> None:
        p = EnvelopeCodec.from_message(TracedOrder(7, "trace-abc"))
        self.assertEqual(p["trace_id"], "trace-abc")
        # blank inherited trace falls back to a generated one
        p2 = EnvelopeCodec.from_message(TracedOrder(7, "  "))
        self.assertRegex(p2["trace_id"], UUID_RE)

    def test_empty_urn_raises(self) -> None:
        with self.assertRaises(BabelQueueError):
            EnvelopeCodec.make("   ", {})

    def test_encode_decode_round_trips(self) -> None:
        p = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1, "amount": "9.90"})
        body = EnvelopeCodec.encode(p)
        self.assertEqual(EnvelopeCodec.decode(body), p)
        self.assertIn('"trace_id"', body)

    def test_decode_of_malformed_json_is_empty(self) -> None:
        self.assertEqual(EnvelopeCodec.decode("not-json"), {})

    def test_consumes_a_php_produced_envelope(self) -> None:
        """Cross-SDK parity: decode the golden fixture produced by the PHP SDK."""
        raw = (FIXTURES / "order-created.json").read_text(encoding="utf-8")
        env = EnvelopeCodec.decode(raw)

        self.assertEqual(env["job"], "urn:babel:orders:created")
        self.assertEqual(env["data"], {"order_id": 1042})
        self.assertEqual(env["meta"]["lang"], "php")  # produced elsewhere, consumed here
        self.assertEqual(env["meta"]["schema_version"], 1)

        # Our own output has the same keys/structure (lang differs by producer).
        ours = EnvelopeCodec.make(env["job"], env["data"], queue=env["meta"]["queue"])
        self.assertEqual(list(ours.keys()), list(json.loads(raw).keys()))


if __name__ == "__main__":
    unittest.main()
