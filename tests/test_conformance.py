"""Runs the shared cross-SDK conformance suite (vendored under tests/conformance/).

The same manifest + fixtures are run by every BabelQueue SDK; passing here proves
this SDK reads/writes the canonical envelope identically to the others.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from babelqueue import EnvelopeCodec

SUITE = Path(__file__).parent / "conformance"
MANIFEST = json.loads((SUITE / "manifest.json").read_text(encoding="utf-8"))


class ConformanceTest(unittest.TestCase):
    def test_suite_is_present(self) -> None:
        self.assertEqual(MANIFEST["schema_version"], 1)
        self.assertGreaterEqual(len(MANIFEST["cases"]), 6)

    def test_cases(self) -> None:
        for case in MANIFEST["cases"]:
            with self.subTest(case=case["name"]):
                raw = (SUITE / case["file"]).read_text(encoding="utf-8")
                env = EnvelopeCodec.decode(raw)
                self.assertNotEqual(env, {}, "fixture must decode")

                if not case["valid"]:
                    self.assertFalse(
                        EnvelopeCodec.accepts(env),
                        f"{case['name']} must be rejected: {case.get('reason')}",
                    )
                    continue

                expect = case["expect"]
                self.assertTrue(EnvelopeCodec.accepts(env), f"{case['name']} must be accepted")
                self.assertEqual(EnvelopeCodec.urn(env), expect["urn"])
                self.assertEqual(env["attempts"], expect["attempts"])
                self.assertEqual(env["meta"]["lang"], expect["lang"])
                self.assertEqual(env["meta"]["schema_version"], expect["schema_version"])

                if "data" in expect:
                    self.assertEqual(env["data"], expect["data"])

                if "dead_letter" in expect:
                    for key, value in expect["dead_letter"].items():
                        self.assertEqual(env["dead_letter"][key], value)

                # Per-message fields must be present (not asserted by value).
                self.assertIn("id", env["meta"])
                self.assertTrue(env["trace_id"])


if __name__ == "__main__":
    unittest.main()
