"""Azure Service Bus binding conformance: the vendored manifest's ``asb`` block locks the
§4 native projection (Subject/CorrelationId/MessageId + bq- application properties) and the
``attempts = max(body, DeliveryCount - 1)`` reconciliation. No azure-servicebus, no broker —
pure transport logic against golden values."""

from __future__ import annotations

import json
import logging
import os
import unittest
from typing import Any, List, Mapping, Tuple

from test_asb_transport import FakeClient, FakeReceived, FakeReceiver
from test_behaviour_conformance import MANIFEST, _ensure, _run_cases, iter_cases

from babelqueue import BabelQueue, EnvelopeCodec
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


    def test_version_gate_settle(self) -> None:
        """``version_gate_asb`` (ADR-0037): every row is asserted (Python has the strategy option)."""
        self.assertEqual(17, len(iter_cases(MANIFEST, "version_gate_asb")))
        _run_cases(self, "version_gate_asb", _eval_version_gate_asb)

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
            if case.get("value_type") == "float":
                # An explicit floating-point case (e.g. 1.0): the reader must hand over a real float.
                self.assertIsInstance(case["value"], float, "schema_version_gate case %r" % (case,))
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


class _Capture(logging.Handler):
    def __init__(self, records: List[logging.LogRecord]) -> None:
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(record)


def _eval_version_gate_asb(case: Mapping[str, Any]) -> None:
    """``version_gate_asb`` (ADR-0037 decision 1; broker-bindings §5.7 / §4.5): a version-gated
    delivery goes through the real ``AsbTransport`` and the runtime. With the DLQ off (and not the
    ``dead_letter`` strategy) the outcome is ``native``: no ``Complete``, no write, no retry; this
    transport dead-letters it natively (explicit ``DeadLetter`` into ``$DeadLetterQueue``). An
    ``unsettled`` delivery is ``Abandon``ed (never ``Complete``d or ``DeadLetter``ed) and the failure is
    reported (an error is logged).
    Python always has a writable DLQ target, so ``outcome_no_writer`` never applies."""
    expect = case["expect"]
    for incoming in MANIFEST["version_gate_asb"]["incoming_bodies"]:
        label = f"{case['name']}/{incoming['name']}"
        delivery_count = 1
        if "bq_attempts" in case:
            # The outcome must not depend on the attempts or the native delivery count.
            delivery_count = 8
        properties = {"bq-schema-version": "2"}
        if "bq_attempts" in case:
            properties["bq-attempts"] = case["bq_attempts"]
        receiver = FakeReceiver([FakeReceived(incoming["body"], delivery_count, properties)])
        transport = AsbTransport(client=FakeClient(receiver=receiver))
        written: List[Tuple[str, str]] = []

        def publish(queue: str, body: str) -> None:
            if case.get("dlq_write") == "fails":
                raise RuntimeError("dlq unavailable")
            written.append((queue, body))

        transport.publish = publish  # type: ignore[method-assign]  # the real one needs azure-servicebus
        calls: list = []
        app = BabelQueue(
            transport=transport,
            queue="orders",
            max_attempts=3,
            dead_letter=case["dead_letter"] == "on",
            on_unknown_urn=case.get("on_unknown_urn", "fail"),
        )
        app.register("urn:babelqueue:order.created", lambda data, meta: calls.append("called"))
        app._rewind_pause = 0  # noqa: SLF001 - no pause between the abandon and the next poll
        records: List[logging.LogRecord] = []
        capture = _Capture(records)
        logging.getLogger("babelqueue.app").addHandler(capture)
        try:
            app.consume("orders", max_messages=1, timeout=0, handle_signals=False)
        finally:
            logging.getLogger("babelqueue.app").removeHandler(capture)

        _ensure(calls == [], f"{label}: a handler ran for a version-gated delivery")
        _ensure(
            all(queue == "orders.dlq" for queue, _ in written),
            f"{label}: only the DLQ may be written, got {written!r}",
        )
        outcome = expect["outcome"]
        if outcome == "ack":
            _ensure(len(receiver.completed) == 1 and written == [] and receiver.dead_lettered == [],
                    f"{label}: want one Complete and nothing else")
        elif outcome == "dlq_raw":
            _ensure(len(written) == 1 and len(receiver.completed) == 1 and receiver.dead_lettered == [],
                    f"{label}: want one DLQ write then one Complete")
            if expect.get("record") == "incoming_body":
                _ensure(written[0][1] == incoming["body"], f"{label}: record is not the incoming body")
        elif outcome == "unsettled":
            _ensure(receiver.completed == [] and receiver.dead_lettered == [] and written == [],
                    f"{label}: want neither a Complete, a DeadLetter nor a write")
            _ensure(len(receiver.abandoned) == 1, f"{label}: want exactly one Abandon, got {len(receiver.abandoned)}")
            _ensure(any(r.levelno >= logging.ERROR for r in records), f"{label}: the failure must be reported")
        elif outcome == "native":
            _ensure(receiver.completed == [] and written == [] and len(receiver.dead_lettered) == 1,
                    f"{label}: native means no Complete, no write and one native DeadLetter")
            _ensure(receiver.dead_lettered[0][1] == "unsupported schema version",
                    f"{label}: DeadLetterReason {receiver.dead_lettered[0][1]!r}")
        else:
            raise AssertionError(f"{label}: unhandled expected outcome {outcome!r}")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
