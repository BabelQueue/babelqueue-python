"""The additive dead_letter block."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from babelqueue.dead_letter import annotate

FIXTURES = Path(__file__).parent / "fixtures"


class DeadLetterTest(unittest.TestCase):
    def test_annotate_preserves_original_and_adds_block(self) -> None:
        envelope = {
            "job": "urn:babel:orders:created",
            "trace_id": "t1",
            "data": {"order_id": 7},
            "meta": {"id": "m1", "queue": "orders"},
            "attempts": 3,
        }

        out = annotate(envelope, "failed", "orders", 3, error="boom", exception="X")

        # original preserved verbatim
        self.assertEqual(out["job"], "urn:babel:orders:created")
        self.assertEqual(out["trace_id"], "t1")
        self.assertEqual(out["meta"]["id"], "m1")
        # additive block
        self.assertEqual(out["dead_letter"]["reason"], "failed")
        self.assertEqual(out["dead_letter"]["error"], "boom")
        self.assertEqual(out["dead_letter"]["exception"], "X")
        self.assertEqual(out["dead_letter"]["original_queue"], "orders")
        self.assertEqual(out["dead_letter"]["attempts"], 3)
        self.assertEqual(out["dead_letter"]["lang"], "python")
        self.assertIsInstance(out["dead_letter"]["failed_at"], int)

    def test_without_exception(self) -> None:
        out = annotate({"job": "u", "data": {}, "meta": {}}, "unknown_urn", "orders", 1)
        self.assertEqual(out["dead_letter"]["reason"], "unknown_urn")
        self.assertIsNone(out["dead_letter"]["error"])
        self.assertIsNone(out["dead_letter"]["exception"])

    def test_block_keys_match_the_golden_fixture(self) -> None:
        fixture = json.loads((FIXTURES / "dead-lettered.json").read_text(encoding="utf-8"))
        out = annotate({"job": "u", "data": {}, "meta": {}}, "failed", "orders", 3, error="x", exception="Y")
        self.assertEqual(list(fixture["dead_letter"].keys()), list(out["dead_letter"].keys()))


if __name__ == "__main__":
    unittest.main()
