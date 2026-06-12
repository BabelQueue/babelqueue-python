"""Amazon SQS binding conformance: the vendored manifest's ``sqs`` block locks the
§3 MessageAttributes projection and the ``attempts = ApproximateReceiveCount - 1``
reconciliation. No boto3, no broker — pure transport logic against golden values."""

from __future__ import annotations

import json
import os
import unittest

from babelqueue import EnvelopeCodec
from babelqueue.sqs_transport import SqsTransport

CONFORMANCE = os.path.join(os.path.dirname(__file__), "conformance")


class SqsConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(os.path.join(CONFORMANCE, "manifest.json"), encoding="utf-8") as fh:
            cls.sqs = json.load(fh)["sqs"]

    def test_attribute_projection(self) -> None:
        projection = self.sqs["attribute_projection"]
        with open(os.path.join(CONFORMANCE, projection["envelope_file"]), encoding="utf-8") as fh:
            body = fh.read()

        got = SqsTransport._attributes(body)
        want = projection["message_attributes"]

        self.assertEqual(set(got), set(want))
        for key, expected in want.items():
            self.assertEqual(got[key]["DataType"], expected["DataType"], key)
            self.assertEqual(got[key]["StringValue"], expected["StringValue"], key)

    def test_attempts_reconciliation(self) -> None:
        for case in self.sqs["attempts_reconciliation"]["cases"]:
            env = EnvelopeCodec.make("urn:babel:orders:created", {"x": 1})
            env["attempts"] = case["body_attempts"]
            body = EnvelopeCodec.encode(env)

            receive_count = case["approximate_receive_count"]
            out = body if receive_count is None else SqsTransport._reconcile(body, receive_count)

            self.assertEqual(
                EnvelopeCodec.decode(out)["attempts"],
                case["expected_attempts"],
                case["name"],
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
