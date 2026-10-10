"""Azure Service Bus binding conformance: the vendored manifest's ``asb`` block locks the
§4 native projection (Subject/CorrelationId/MessageId + bq- application properties) and the
``attempts = max(body, DeliveryCount - 1)`` reconciliation. No azure-servicebus, no broker —
pure transport logic against golden values."""

from __future__ import annotations

import json
import os
import unittest

from babelqueue import EnvelopeCodec
from babelqueue.asb_transport import AsbTransport

CONFORMANCE = os.path.join(os.path.dirname(__file__), "conformance")


class AsbConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(os.path.join(CONFORMANCE, "manifest.json"), encoding="utf-8") as fh:
            cls.asb = json.load(fh)["asb"]
        cls.gate = cls.asb["schema_version_gate"]

    def test_property_projection(self) -> None:
        projection = self.asb["property_projection"]
        with open(os.path.join(CONFORMANCE, projection["envelope_file"]), encoding="utf-8") as fh:
            body = fh.read()

        got = AsbTransport._projection(body)
        message = projection["message"]

        self.assertEqual(got.get("subject"), message["subject"])
        self.assertEqual(got.get("correlation_id"), message["correlation_id"])
        self.assertEqual(got.get("message_id"), message["message_id"])
        self.assertEqual(got.get("content_type"), message["content_type"])
        self.assertEqual(got.get("application_properties"), projection["application_properties"])

    def test_attempts_reconciliation(self) -> None:
        for case in self.asb["attempts_reconciliation"]["cases"]:
            env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1})
            env["attempts"] = case["body_attempts"]
            body = EnvelopeCodec.encode(env)

            out = AsbTransport._reconcile(body, case["delivery_count"])

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
        """§4.7: the property is read BEFORE decoding; absent/blank decodes, an exact "1"
        decodes, anything else is rejected without decoding (the body is always the valid fixture)."""
        gate = self.gate
        with open(os.path.join(CONFORMANCE, gate["fixture"]), encoding="utf-8") as fh:
            valid_body = fh.read()
        self.assertEqual(EnvelopeCodec.decode(valid_body)["job"], "urn:babel:orders:created")

        for case in gate["cases"]:
            carrier = self._gate_carrier(case)
            rejected = AsbTransport._unsupported_schema_version(carrier)
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
