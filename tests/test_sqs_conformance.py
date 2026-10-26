"""Amazon SQS binding conformance: the vendored manifest's ``sqs`` block locks the
§3 MessageAttributes projection and the ``attempts = ApproximateReceiveCount - 1``
reconciliation. No boto3, no broker — pure transport logic against golden values."""

from __future__ import annotations

import json
import os
import unittest
from typing import Any, Mapping

from test_behaviour_conformance import MANIFEST, _ensure, _run_cases, iter_cases
from test_sqs_transport import FakeSQS

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.sqs_transport import SqsTransport

CONFORMANCE = os.path.join(os.path.dirname(__file__), "conformance")


class SqsConformanceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open(os.path.join(CONFORMANCE, "manifest.json"), encoding="utf-8") as fh:
            cls.sqs = json.load(fh)["sqs"]
        cls.gate = cls.sqs["schema_version_gate"]

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


    def test_version_gate_settle(self) -> None:
        """``version_gate_sqs`` (ADR-0037): every row is asserted (Python has the strategy option)."""
        self.assertEqual(17, len(iter_cases(MANIFEST, "version_gate_sqs")))
        _run_cases(self, "version_gate_sqs", _eval_version_gate_sqs)

    def _gate_carrier(self, case: dict):
        if "absent" in case:
            return {}
        return {"bq-schema-version": {"StringValue": case["value"]}}

    def test_schema_version_gate(self) -> None:
        """§3.7: the property is read BEFORE decoding; absent/blank decodes, an exact "1"
        decodes, anything else is rejected without decoding (the body is always the valid fixture)."""
        gate = self.gate
        with open(os.path.join(CONFORMANCE, gate["fixture"]), encoding="utf-8") as fh:
            valid_body = fh.read()
        self.assertEqual(EnvelopeCodec.decode(valid_body)["job"], "urn:babel:orders:created")

        for case in gate["cases"]:
            carrier = self._gate_carrier(case)
            rejected = SqsTransport._unsupported_schema_version(carrier)
            want = "reject" if case["expect"] == "reject" else "decode"
            self.assertEqual(
                "reject" if rejected is not None else "decode",
                want,
                "schema_version_gate case %r" % (case,),
            )
            if rejected is not None:
                self.assertEqual(rejected, str(case["value"]), "schema_version_gate case %r" % (case,))


class _DlqFailsSQS(FakeSQS):
    """FakeSQS whose ``SendMessage`` fails (the raw record cannot be written to ``<queue>-dlq``)."""

    def send_message(self, **kw):
        raise RuntimeError("dlq unavailable")


def _eval_version_gate_sqs(case: Mapping[str, Any]) -> None:
    """``version_gate_sqs`` (ADR-0037 decision 1; broker-bindings §5.7 / §3.5): a version-gated
    delivery goes through the real ``SqsTransport`` and the runtime. With the DLQ off (and not the
    ``dead_letter`` strategy) the outcome is ``native``: no ``DeleteMessage``, no write, no
    visibility change. Python always has a writable DLQ target, so ``outcome_no_writer`` never
    applies."""
    expect = case["expect"]
    for incoming in MANIFEST["version_gate_sqs"]["incoming_bodies"]:
        label = f"{case['name']}/{incoming['name']}"
        fake = _DlqFailsSQS() if case.get("dlq_write") == "fails" else FakeSQS()
        attributes = {"bq-schema-version": {"DataType": "Number", "StringValue": "2"}}
        receive_count = "1"
        if "bq_attempts" in case:
            # The outcome must not depend on the attempts or the native receive count.
            attributes["bq-attempts"] = {"DataType": "Number", "StringValue": case["bq_attempts"]}
            receive_count = "8"
        fake.visible["http://fake/orders"] = [
            {
                "Body": incoming["body"],
                "ReceiptHandle": "rh-gate",
                "MessageAttributes": attributes,
                "Attributes": {"ApproximateReceiveCount": receive_count},
            }
        ]
        calls: list = []
        app = BabelQueue(
            transport=SqsTransport("sqs://", client=fake, queue_url_prefix="http://fake"),
            queue="orders",
            max_attempts=3,
            dead_letter=case["dead_letter"] == "on",
            on_unknown_urn=case.get("on_unknown_urn", "fail"),
        )
        app.register("urn:babelqueue:order.created", lambda data, meta: calls.append("called"))
        app.consume("orders", max_messages=1, timeout=0, handle_signals=False)

        _ensure(calls == [], f"{label}: a handler ran for a version-gated delivery")
        _ensure(fake.visibility_changes == [], f"{label}: a gated delivery is never released")
        _ensure(
            all(sent["QueueUrl"] == "http://fake/orders-dlq" for sent in fake.sent),
            f"{label}: only the DLQ may be written",
        )
        outcome = expect["outcome"]
        if outcome == "ack":
            _ensure(fake.deleted == ["rh-gate"] and fake.sent == [], f"{label}: want one DeleteMessage and no write")
        elif outcome == "dlq_raw":
            _ensure(len(fake.sent) == 1 and fake.deleted == ["rh-gate"], f"{label}: want one write then one delete")
            if expect.get("record") == "incoming_body":
                _ensure(fake.sent[0]["MessageBody"] == incoming["body"], f"{label}: record is not the incoming body")
        elif outcome == "unsettled":
            _ensure(fake.deleted == [], f"{label}: a failed write must not delete the message")
        elif outcome == "native":
            _ensure(
                fake.deleted == [] and fake.sent == [],
                f"{label}: native means no DeleteMessage and no write, got deleted={fake.deleted!r} sent={fake.sent!r}",
            )
        else:
            raise AssertionError(f"{label}: unhandled expected outcome {outcome!r}")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
