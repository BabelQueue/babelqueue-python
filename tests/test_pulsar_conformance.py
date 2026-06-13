"""Apache Pulsar binding conformance: the vendored manifest's ``pulsar`` block locks the §5
property projection (bq-* string->string) and the ``attempts = max(body, redelivery_count)``
reconciliation (no -1, Pulsar's redelivery count is 0-based). No pulsar-client, no broker —
pure transport logic against golden values."""

from __future__ import annotations

import json
import os
import unittest

from babelqueue import EnvelopeCodec
from babelqueue.pulsar_transport import PulsarTransport

CONFORMANCE = os.path.join(os.path.dirname(__file__), "conformance")


class PulsarConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(os.path.join(CONFORMANCE, "manifest.json"), encoding="utf-8") as fh:
            cls.pulsar = json.load(fh)["pulsar"]

    def test_property_projection(self) -> None:
        projection = self.pulsar["property_projection"]
        with open(os.path.join(CONFORMANCE, projection["envelope_file"]), encoding="utf-8") as fh:
            body = fh.read()

        got = PulsarTransport._projection(body)
        self.assertEqual(got, projection["properties"])

    def test_attempts_reconciliation(self) -> None:
        for case in self.pulsar["attempts_reconciliation"]["cases"]:
            env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1})
            env["attempts"] = case["body_attempts"]
            body = EnvelopeCodec.encode(env)

            out = PulsarTransport._reconcile(body, case["redelivery_count"])

            self.assertEqual(
                EnvelopeCodec.decode(out)["attempts"],
                case["expected_attempts"],
                case["name"],
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
