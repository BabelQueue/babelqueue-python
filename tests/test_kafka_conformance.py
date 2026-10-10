"""Apache Kafka binding conformance: the vendored manifest's ``kafka`` block locks the §6
header projection (bq-* UTF-8 byte strings) and the ``attempts`` reconciliation (the
``bq-attempts`` header is authoritative when present, else the body — NOT a max). No
confluent-kafka, no broker — pure transport logic against golden values."""

from __future__ import annotations

import json
import os
import unittest

from babelqueue import EnvelopeCodec
from babelqueue.kafka_transport import KafkaTransport

CONFORMANCE = os.path.join(os.path.dirname(__file__), "conformance")


class KafkaConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(os.path.join(CONFORMANCE, "manifest.json"), encoding="utf-8") as fh:
            cls.kafka = json.load(fh)["kafka"]
        cls.gate = cls.kafka["schema_version_gate"]

    def test_property_projection(self) -> None:
        projection = self.kafka["property_projection"]
        with open(os.path.join(CONFORMANCE, projection["envelope_file"]), encoding="utf-8") as fh:
            body = fh.read()

        got = {key: value.decode("utf-8") for key, value in KafkaTransport._projection(body)}
        self.assertEqual(got, projection["headers"])

    def test_attempts_reconciliation(self) -> None:
        for case in self.kafka["attempts_reconciliation"]["cases"]:
            env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1})
            env["attempts"] = case["body_attempts"]
            body = EnvelopeCodec.encode(env)

            headers = []
            if case["header_attempts"] is not None:
                headers = [("bq-attempts", str(case["header_attempts"]).encode("utf-8"))]

            out = KafkaTransport._reconcile(body, headers)

            self.assertEqual(
                EnvelopeCodec.decode(out)["attempts"],
                case["expected_attempts"],
                case["name"],
            )


    def _gate_carrier(self, case: dict):
        if "absent" in case:
            return []
        if case["value"] is None:  # JSON null: header present with a null value == absent (§6.7)
            return [(self.gate["property"], None)]
        return [(self.gate["property"], case["value"].encode("utf-8"))]

    def test_schema_version_gate(self) -> None:
        """§6.7: the property is read BEFORE decoding; absent/blank decodes, an exact "1"
        decodes, anything else is rejected without decoding (the body is always the valid fixture)."""
        gate = self.gate
        with open(os.path.join(CONFORMANCE, gate["fixture"]), encoding="utf-8") as fh:
            valid_body = fh.read()
        self.assertEqual(EnvelopeCodec.decode(valid_body)["job"], "urn:babel:orders:created")

        for case in gate["cases"]:
            carrier = self._gate_carrier(case)
            rejected = KafkaTransport._unsupported_schema_version(carrier)
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
