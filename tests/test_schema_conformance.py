from __future__ import annotations

import json
import logging
import unittest
from pathlib import Path
from typing import Set, Tuple

from babelqueue.schema import MapProvider, validate_schema

MANIFEST = Path(__file__).parent / "conformance" / "manifest.json"


class PayloadConformanceTest(unittest.TestCase):
    """Runs the shared cross-SDK payload-schema cases (ADR-0024) from the vendored conformance
    suite: this validator must agree with the Go and PHP ones on each case's ``valid`` flag."""

    def test_payload_cases_match_across_sdks(self) -> None:
        if not MANIFEST.is_file():
            self.skipTest("vendored conformance suite not present")
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        section = manifest.get("payload_schema")
        if not isinstance(section, dict):
            self.skipTest("manifest has no payload_schema section")

        schema = section["schema"]
        cases = section["cases"]
        self.assertTrue(cases)
        for case in cases:
            # One subTest per case so a failure is attributable to its case id (results.json).
            with self.subTest(case=case["name"]):
                valid = validate_schema(schema, case["data"]) is None
                self.assertEqual(case["valid"], valid, f"case {case['name']}")


class PayloadKeywordsConformanceTest(unittest.TestCase):
    """The ``payload_schema_keywords`` section (conformance 1.10.0, K-17): union ``type``,
    numeric equality in ``const`` / ``enum`` and the registration warning for keywords outside the
    subset. Each case carries its own schema; the runner registers it, collects the warnings
    (compared as a SET of ``(pointer, keyword)``), then validates ``data`` and compares ``valid``."""

    def test_payload_keywords_cases(self) -> None:
        if not MANIFEST.is_file():
            self.skipTest("vendored conformance suite not present")
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        section = manifest.get("payload_schema_keywords")
        if not isinstance(section, dict):
            self.skipTest("manifest has no payload_schema_keywords section")

        cases = section["cases"]
        self.assertTrue(cases)
        urn = "urn:babel:conformance:keywords"
        for case in cases:
            with self.subTest(case=case["name"]):
                with self.assertLogs("babelqueue.schema", level=logging.WARNING) as logs:
                    # assertLogs fails when nothing is logged, so a never-warning case logs a probe.
                    logging.getLogger("babelqueue.schema").warning("probe")
                    provider = MapProvider({urn: case["schema"]})
                warnings = provider.warnings.get(urn, [])
                got: Set[Tuple[str, str]] = {(w.pointer, w.keyword) for w in warnings}
                expected = {(w["pointer"], w["keyword"]) for w in case.get("warnings", [])}
                self.assertEqual(expected, got, f"case {case['name']}: warnings")
                # one warning per occurrence, each one logged exactly once
                self.assertEqual(len(warnings), len(got), f"case {case['name']}: duplicate warnings")
                self.assertEqual(len(warnings), len(logs.output) - 1, f"case {case['name']}: logged")

                valid = validate_schema(case["schema"], case["data"]) is None
                self.assertEqual(case["valid"], valid, f"case {case['name']}")


if __name__ == "__main__":
    unittest.main()
