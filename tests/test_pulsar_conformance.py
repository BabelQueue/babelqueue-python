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
        cls.gate = cls.pulsar["schema_version_gate"]

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

            out = PulsarTransport._reconcile(body, case["redelivery_count"])[0]

            self.assertEqual(
                EnvelopeCodec.decode(out)["attempts"],
                case["expected_attempts"],
                case["name"],
            )


    def _gate_carrier(self, case: dict):
        if "absent" in case:
            return {}
        return {self.gate["property"]: case["value"]}

    def test_schema_version_gate(self) -> None:
        """§5.7: the property is read BEFORE decoding; absent/blank decodes, an exact "1"
        decodes, anything else is rejected without decoding (the body is always the valid fixture)."""
        gate = self.gate
        with open(os.path.join(CONFORMANCE, gate["fixture"]), encoding="utf-8") as fh:
            valid_body = fh.read()
        self.assertEqual(EnvelopeCodec.decode(valid_body)["job"], "urn:babel:orders:created")

        for case in gate["cases"]:
            carrier = self._gate_carrier(case)
            rejected = PulsarTransport._unsupported_schema_version(carrier)
            want = "reject" if case["expect"] == "reject" else "decode"
            self.assertEqual(
                "reject" if rejected is not None else "decode",
                want,
                "schema_version_gate case %r" % (case,),
            )
            if rejected is not None:
                self.assertEqual(rejected, str(case["value"]), "schema_version_gate case %r" % (case,))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
