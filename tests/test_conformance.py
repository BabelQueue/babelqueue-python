"""Runs the shared cross-SDK conformance suite (vendored under tests/conformance/).

The same manifest + fixtures are run by every BabelQueue SDK; passing here proves
this SDK reads/writes the canonical envelope identically to the others.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from babelqueue import BabelQueueError, EnvelopeCodec, __version__

TESTS_DIR = Path(__file__).parent
SRC_DIR = TESTS_DIR.parent / "src" / "babelqueue"
SUITE = TESTS_DIR / "conformance"
MANIFEST = json.loads((SUITE / "manifest.json").read_text(encoding="utf-8"))
RESULTS_ENV = "BQ_CONFORMANCE_RESULTS"
SDK_NAME = "babelqueue-python"

if str(TESTS_DIR) not in sys.path:  # sibling test modules are imported by bare name
    sys.path.insert(0, str(TESTS_DIR))


class ConformanceTest(unittest.TestCase):
    def test_suite_is_present(self) -> None:
        self.assertEqual(MANIFEST["schema_version"], 1)
        self.assertGreaterEqual(len(MANIFEST["cases"]), 6)

    def test_cases(self) -> None:
        for case in MANIFEST["cases"]:
            with self.subTest(case=case["name"]):
                raw = (SUITE / case["file"]).read_text(encoding="utf-8")
                if not case["valid"]:
                    # Rejected either by decode itself (identity-less object, ADR-0032 section 6) or by accepts().
                    try:
                        env = EnvelopeCodec.decode(raw)
                    except BabelQueueError:
                        continue
                    self.assertFalse(
                        EnvelopeCodec.accepts(env),
                        f"{case['name']} must be rejected: {case.get('reason')}",
                    )
                    continue

                env = EnvelopeCodec.decode(raw)
                self.assertNotEqual(env, {}, "fixture must decode")

                expect = case["expect"]
                self.assertTrue(EnvelopeCodec.accepts(env), f"{case['name']} must be accepted")
                self.assertEqual(EnvelopeCodec.urn(env), expect["urn"])
                self.assertEqual(env["attempts"], expect["attempts"])
                self.assertEqual(env["meta"]["lang"], expect["lang"])
                self.assertEqual(env["meta"]["schema_version"], expect["schema_version"])

                if "data" in expect:
                    self.assertEqual(env["data"], expect["data"])

                if "dead_letter" in expect:
                    for key, value in expect["dead_letter"].items():
                        self.assertEqual(env["dead_letter"][key], value)

                # Per-message fields must be present (not asserted by value).
                self.assertIn("id", env["meta"])
                self.assertTrue(env["trace_id"])



# --------------------------------------------------------------------------------------------
# results.json (ADR-0032 section 2): the machine-readable outcome of this run.
# --------------------------------------------------------------------------------------------

# Each section is evidenced by the test methods below. A subTest failure carrying ``case=<id>``
# fails that one case; a failure (or a skip) of the whole method fails every case of the section.
_RUNNERS: Dict[str, List[Tuple[str, str, str]]] = {
    "cases": [("test_conformance", "ConformanceTest", "test_cases")],
    "idempotency": [("test_behaviour_conformance", "IdempotencyConformanceTest", "test_idempotency_cases")],
    "payload_schema": [("test_schema_conformance", "PayloadConformanceTest", "test_payload_cases_match_across_sdks")],
    "roundtrip": [("test_behaviour_conformance", "RoundtripConformanceTest", "test_roundtrip_cases")],
    "data_shape": [("test_behaviour_conformance", "DataShapeConformanceTest", "test_data_shape_cases")],
    "forbidden_keys": [("test_behaviour_conformance", "ForbiddenKeysConformanceTest", "test_forbidden_key_cases")],
    "extension_write": [("test_behaviour_conformance", "ExtensionWriteConformanceTest", "test_extension_write_cases")],
    "encode_reject": [("test_behaviour_conformance", "EncodeRejectConformanceTest", "test_encode_reject_cases")],
    "payload_schema_unicode": [("test_behaviour_conformance", "PayloadSchemaUnicodeConformanceTest", "test_unicode_length_cases")],
    "trim": [("test_behaviour_conformance", "TrimConformanceTest", "test_trim_cases")],
    "job_type": [("test_behaviour_conformance", "JobTypeConformanceTest", "test_job_type_cases")],
    "traceparent": [("test_behaviour_conformance", "TraceparentConformanceTest", "test_traceparent_cases")],
    "redis": [("test_behaviour_conformance", "RedisConformanceTest", "test_payload_identity")],
    "redis_layout": [("test_behaviour_conformance", "RedisLayoutConformanceTest", "test_redis_layout_cases")],
    "rabbitmq": [("test_behaviour_conformance", "RabbitmqConformanceTest", "test_property_projection")],
    "delay": [("test_behaviour_conformance", "DelayConformanceTest", "test_delay_cases")],
    "backoff": [("test_behaviour_conformance", "BackoffConformanceTest", "test_backoff_cases")],
    "retry_backoff": [("test_behaviour_conformance", "RetryBackoffConformanceTest", "test_retry_backoff_cases")],
    "redis_delay": [("test_behaviour_conformance", "RedisDelayConformanceTest", "test_redis_delay_cases")],
    "kafka_poison": [("test_behaviour_conformance", "KafkaPoisonConformanceTest", "test_kafka_poison_cases")],
    "poison_dlq": [("test_behaviour_conformance", "PoisonDlqConformanceTest", "test_poison_dlq_cases")],
    "identity_verdict": [("test_behaviour_conformance", "IdentityVerdictConformanceTest", "test_identity_verdict_cases")],
    "pulsar_exhaustion": [("test_behaviour_conformance", "PulsarExhaustionConformanceTest", "test_pulsar_exhaustion_cases")],
    "version_gate_pulsar": [("test_behaviour_conformance", "VersionGatePulsarConformanceTest", "test_version_gate_pulsar_cases")],
    "version_gate_artemis": [("test_behaviour_conformance", "VersionGateArtemisConformanceTest", "test_version_gate_artemis_cases")],
    "version_gate_sqs": [("test_sqs_conformance", "SqsConformanceTest", "test_version_gate_settle")],
    "version_gate_asb": [("test_asb_conformance", "AsbConformanceTest", "test_version_gate_settle")],
    # The broker bindings are evidenced per runner, not per case (every method must pass).
    "sqs": [("test_sqs_conformance", "SqsConformanceTest", m) for m in (
        "test_attribute_projection", "test_attempts_reconciliation", "test_schema_version_gate")],
    "asb": [("test_asb_conformance", "AsbConformanceTest", m) for m in (
        "test_property_projection", "test_attempts_reconciliation", "test_schema_version_gate")],
    "pulsar": [("test_pulsar_conformance", "PulsarConformanceTest", m) for m in (
        "test_property_projection", "test_attempts_reconciliation", "test_schema_version_gate")],
    "kafka": [("test_kafka_conformance", "KafkaConformanceTest", m) for m in (
        "test_property_projection", "test_attempts_reconciliation", "test_schema_version_gate")],
    "artemis": [("test_artemis_conformance", "ArtemisConformanceTest", m) for m in (
        "test_property_projection", "test_attempts_reconciliation", "test_schema_version_gate")],
}

# A transport module -> the binding (manifest scope) it carries. A new ``*_transport.py`` that is
# missing here makes the run fail instead of silently shrinking ``bindings`` (ADR-0032 Compliance).
_TRANSPORT_BINDINGS = {
    "redis_transport": "redis",
    "pika_transport": "amqp",
    "sqs_transport": "sqs",
    "asb_transport": "asb",
    "pulsar_transport": "pulsar",
    "kafka_transport": "kafka",
    "artemis_transport": "artemis",
}


def derive_bindings() -> List[str]:
    """The bindings this package carries, read off its modules (never hand-written)."""
    bindings: Set[str] = set()
    for path in sorted(SRC_DIR.glob("*_transport.py")):
        if path.stem not in _TRANSPORT_BINDINGS:
            raise AssertionError(f"transport module {path.name} has no binding mapping")
        bindings.add(_TRANSPORT_BINDINGS[path.stem])
    # The W3C traceparent extract/inject code of ADR-0028 lives in ``otel.py``.
    otel = SRC_DIR / "otel.py"
    if otel.is_file() and "traceparent" in otel.read_text(encoding="utf-8").lower():
        bindings.add("otel")
    return sorted(bindings)


def in_scope_sections(bindings: List[str]) -> List[str]:
    """Manifest sections whose scope is ``core`` or a carried binding."""
    return [
        name
        for name, info in MANIFEST["sections"].items()
        if info["scope"] == "core" or info["scope"] in bindings
    ]


class _Collector(unittest.TestResult):
    """Records, per run, which case ids failed and whether the whole method failed."""

    def __init__(self) -> None:
        super().__init__()
        self.failed_cases: Dict[str, str] = {}
        self.method_failure: Optional[str] = None

    def addSubTest(self, test: unittest.TestCase, subtest: Any, err: Any) -> None:
        super().addSubTest(test, subtest, err)
        if err is None:
            return
        case_id = getattr(subtest, "params", {}).get("case")
        message = self._exc_info_to_string(err, test).strip().splitlines()[-1]
        if case_id is None:
            self.method_failure = self.method_failure or message
        else:
            self.failed_cases[str(case_id)] = message

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:
        super().addFailure(test, err)
        self.method_failure = self.method_failure or self._exc_info_to_string(err, test).strip().splitlines()[-1]

    def addError(self, test: unittest.TestCase, err: Any) -> None:
        super().addError(test, err)
        self.method_failure = self.method_failure or self._exc_info_to_string(err, test).strip().splitlines()[-1]

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        # A skipped runner proves nothing: a skip is never legal in a runner method.
        self.method_failure = self.method_failure or f"runner skipped: {reason}"


def run_section(section: str) -> Dict[str, Dict[str, str]]:
    """Run the tests that evidence ``section`` and return ``case id -> result``."""
    from test_behaviour_conformance import GAP_RESULTS, KNOWN_GAPS, iter_cases

    for stale in [key for key in GAP_RESULTS if key[0] == section]:
        del GAP_RESULTS[stale]
    collector = _Collector()
    tests = []
    for module_name, class_name, method in _RUNNERS[section]:
        module = importlib.import_module(module_name)
        tests.append(getattr(module, class_name)(method))
    # A suite (not ``TestCase.run``) so ``setUpClass`` and class-level skips are honoured.
    unittest.TestSuite(tests).run(collector)
    gaps = KNOWN_GAPS.get(section, {})
    results: Dict[str, Dict[str, str]] = {}
    for case_id, _ in iter_cases(MANIFEST, section):
        if case_id in gaps:
            # A known gap is RUN (``_run_cases``) and reported as it really behaves: a failure is
            # an honest ``fail`` (never a skip), a pass is reported as ``pass`` so the completeness
            # gate rejects the stale entry in conformance-known-gaps.json.
            if (section, case_id) not in GAP_RESULTS:
                # No recorded outcome means the evaluator raised something other than an
                # ``AssertionError`` (or never got there): that is a bug, not "not implemented", and
                # must never be written to results.json as a known gap.
                raise RuntimeError(
                    f"known-gap case {section}/{case_id} produced no outcome; "
                    f"the section failed with: {collector.method_failure or 'no failure recorded'}"
                )
            outcome = GAP_RESULTS[(section, case_id)]
            if outcome is None:
                results[case_id] = {"status": "pass"}
            else:
                results[case_id] = {"status": "fail", "reason": f"{gaps[case_id]}; observed: {outcome}"}
        elif case_id in collector.failed_cases or collector.method_failure:
            reason = collector.failed_cases.get(case_id) or collector.method_failure or "failed"
            results[case_id] = {"status": "fail", "reason": reason}
        else:
            results[case_id] = {"status": "pass"}
    return results


def build_results() -> Dict[str, Any]:
    bindings = derive_bindings()
    return {
        "sdk": SDK_NAME,
        "version": __version__,
        "conformance_version": (SUITE / "CONFORMANCE_VERSION").read_text(encoding="utf-8").strip(),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bindings": bindings,
        "sections": {name: run_section(name) for name in in_scope_sections(bindings)},
    }


class ResultsJsonTest(unittest.TestCase):
    def test_every_transport_module_maps_to_a_binding(self) -> None:
        bindings = derive_bindings()
        self.assertIn("otel", bindings)
        self.assertEqual(
            sorted({_TRANSPORT_BINDINGS[p.stem] for p in SRC_DIR.glob("*_transport.py")} | {"otel"}),
            bindings,
        )

    def test_every_in_scope_section_has_a_runner(self) -> None:
        # ``MANIFEST["sections"]`` is the section list; top-level ``sections``/``exceptions`` are not.
        missing = [n for n in in_scope_sections(derive_bindings()) if n not in _RUNNERS]
        self.assertEqual([], missing, "sections without a runner would be missing from results.json")
        self.assertEqual([], [n for n in _RUNNERS if n not in MANIFEST["sections"]])

    def test_a_non_assertion_error_in_a_known_gap_case_is_not_reported_as_a_known_gap(self) -> None:
        # The gap list is empty today, so a synthetic gap proves the guard: an evaluator bug
        # (anything but an ``AssertionError``) must never be written to results.json as a gap.
        import test_behaviour_conformance as behaviour

        first_case = behaviour.iter_cases(MANIFEST, "backoff")[0][0]

        def broken(case: Any) -> None:
            raise KeyError("tiers")

        with mock.patch.object(behaviour, "_eval_backoff", broken), \
                mock.patch.dict(behaviour.KNOWN_GAPS, {"backoff": {first_case: "synthetic"}}), \
                mock.patch.dict(behaviour.GAP_RESULTS):
            with self.assertRaisesRegex(RuntimeError, r"backoff/.*produced no outcome.*KeyError"):
                run_section("backoff")

    def test_results_json_is_written_when_requested(self) -> None:
        target = os.environ.get(RESULTS_ENV)
        if not target:
            self.skipTest(f"{RESULTS_ENV} is not set; no results.json is written")
        from test_behaviour_conformance import KNOWN_GAPS

        results = build_results()
        Path(target).write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")

        counts: Dict[str, int] = {"pass": 0, "fail": 0, "skip": 0}
        for cases in results["sections"].values():
            for outcome in cases.values():
                counts[outcome["status"]] += 1
        failed = {
            f"{section}/{case_id}": outcome["reason"]
            for section, cases in results["sections"].items()
            for case_id, outcome in cases.items()
            if outcome["status"] == "fail"
        }
        # The only legal failures are the known gaps; anything else (or a gap that no longer
        # fails) is a regression the completeness gate would reject as well.
        expected = {
            f"{section}/{case_id}": reason
            for section, gaps in KNOWN_GAPS.items()
            for case_id, reason in gaps.items()
        }
        self.assertEqual(set(expected), set(failed), "conformance failures differ from conformance-known-gaps.json")
        for case, reason in failed.items():
            self.assertTrue(reason.startswith(expected[case] + "; observed: "), f"{case}: {reason!r}")
        self.assertGreater(counts["pass"], 0)


if __name__ == "__main__":
    unittest.main()
