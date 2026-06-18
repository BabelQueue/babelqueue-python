from __future__ import annotations

import json
import unittest
from pathlib import Path

from babelqueue.schema import validate_schema

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
            valid = validate_schema(schema, case["data"]) is None
            self.assertEqual(case["valid"], valid, f"case {case['name']}")


if __name__ == "__main__":
    unittest.main()
