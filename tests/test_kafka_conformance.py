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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
