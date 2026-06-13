"""Apache ActiveMQ Artemis binding conformance: the vendored manifest's ``artemis`` block locks
the §7 AMQP projection (the string-valued ``bq-`` application properties + the JMS-native
``x-opt-jms-type`` / ``correlation-id`` mappings) and the ``attempts = max(body, delivery_count)``
reconciliation (the AMQP delivery-count is 0-based, so no −1). No qpid-proton, no broker — pure
transport logic against golden values."""

from __future__ import annotations

import json
import os
import unittest

from babelqueue import EnvelopeCodec
from babelqueue.artemis_transport import ArtemisTransport

CONFORMANCE = os.path.join(os.path.dirname(__file__), "conformance")


class ArtemisConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(os.path.join(CONFORMANCE, "manifest.json"), encoding="utf-8") as fh:
            cls.artemis = json.load(fh)["artemis"]

    def test_property_projection(self) -> None:
        projection = self.artemis["property_projection"]
        with open(os.path.join(CONFORMANCE, projection["envelope_file"]), encoding="utf-8") as fh:
            body = fh.read()

        self.assertEqual(ArtemisTransport._projection(body), projection["properties"])
        self.assertEqual(ArtemisTransport._jms_type(body), projection["jms_type"])
        self.assertEqual(ArtemisTransport._correlation_id(body), projection["correlation_id"])

    def test_attempts_reconciliation(self) -> None:
        for case in self.artemis["attempts_reconciliation"]["cases"]:
            env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1})
            env["attempts"] = case["body_attempts"]
            body = EnvelopeCodec.encode(env)

            out = ArtemisTransport._reconcile(body, case["delivery_count"])

            self.assertEqual(
                EnvelopeCodec.decode(out)["attempts"],
                case["expected_attempts"],
                case["name"],
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
