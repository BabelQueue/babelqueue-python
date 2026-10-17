"""Runs the behavioural sections of the shared cross-SDK conformance suite
(``roundtrip``, ``extension_write``, ``encode_reject``, ``data_shape``, ``forbidden_keys``,
``payload_schema_unicode``).

These sections are mandatory: unlike the optional broker sections, a missing section fails
the run instead of skipping it. Pointers are RFC 6901 and equality is type-strict and deep
(``true`` never equals ``1``, ``{}`` never equals ``[]``) — the shared runner contract.
"""

from __future__ import annotations

import contextlib
import json
import logging
import unittest
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Tuple

from babelqueue import BabelQueue, EnvelopeCodec
from babelqueue.dead_letter import annotate
from babelqueue.exceptions import EnvelopeKeyError
from babelqueue.schema import validate_schema

SUITE = Path(__file__).parent / "conformance"
MANIFEST = json.loads((SUITE / "manifest.json").read_text(encoding="utf-8"))

_MISSING = object()


def _pointer(doc: Any, pointer: str) -> Any:
    """Resolve an RFC 6901 JSON pointer; returns ``_MISSING`` when it does not resolve."""
    if pointer == "":
        return doc
    if not pointer.startswith("/"):
        raise ValueError(f"invalid JSON pointer {pointer!r}")
    current = doc
    for token in pointer[1:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            if token not in current:
                return _MISSING
            current = current[token]
        elif isinstance(current, list):
            if not token.isdigit() or int(token) >= len(current):
                return _MISSING
            current = current[int(token)]
        else:
            return _MISSING
    return current


def _strict_equal(a: Any, b: Any) -> bool:
    """Deep JSON equality that also compares JSON types (Python's ``True == 1``)."""
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a is b
    if isinstance(a, dict) or isinstance(b, dict):
        return (
            isinstance(a, dict)
            and isinstance(b, dict)
            and a.keys() == b.keys()
            and all(_strict_equal(a[k], b[k]) for k in a)
        )
    if isinstance(a, list) or isinstance(b, list):
        return (
            isinstance(a, list)
            and isinstance(b, list)
            and len(a) == len(b)
            and all(_strict_equal(x, y) for x, y in zip(a, b))
        )
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return type(a) is type(b) and a == b


def _section(name: str) -> dict:
    section = MANIFEST.get(name)
    if not isinstance(section, dict) or not section.get("cases"):
        raise AssertionError(f"conformance section {name!r} is missing or empty")
    return section


def _retry_once(raw: str) -> str:
    """Drive ``raw`` through the runtime's real retry path (decode -> attempts+1 -> encode)
    and return the re-emitted body."""
    env = EnvelopeCodec.decode(raw)
    urn = EnvelopeCodec.urn(env)
    app = BabelQueue("memory://", queue="conformance", max_attempts=1_000_000)

    def fail(data: Any, meta: Any) -> None:
        raise RuntimeError("force a retry")

    app.register(urn, fail)
    app.transport.publish("conformance", raw)
    processed = app.consume(max_messages=1, timeout=0, handle_signals=False)
    assert processed == 1, "the fixture must be consumed"
    received = app.transport.pop("conformance", timeout=0)
    assert received is not None, "the retry must re-emit the message"
    return received.body


@contextlib.contextmanager
def _captured_warnings() -> Iterator[List[str]]:
    """Collect every WARNING+ record of ``babelqueue.codec`` (``assertNoLogs`` needs Python 3.10)."""
    records: List[str] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    target = logging.getLogger("babelqueue.codec")
    handler = _Collector(level=logging.WARNING)
    previous = target.level
    target.addHandler(handler)
    target.setLevel(logging.WARNING)
    try:
        yield records
    finally:
        target.removeHandler(handler)
        target.setLevel(previous)


def _preserved_violations(out: Any, expected: Mapping[str, Any]) -> List[str]:
    """Pointers of ``expected`` that are missing from ``out`` or hold a different value/type."""
    problems: List[str] = []
    for pointer, value in expected.items():
        actual = _pointer(out, pointer)
        if actual is _MISSING:
            problems.append(f"{pointer} was dropped")
        elif not _strict_equal(actual, value):
            problems.append(f"{pointer}: {actual!r} != {value!r}")
    return problems


def _absent_violations(out: Any, pointers: List[str]) -> List[str]:
    """Pointers of ``pointers`` that DO exist in ``out`` (they must be absent)."""
    return [f"{pointer} re-emitted" for pointer in pointers if _pointer(out, pointer) is not _MISSING]


def _roundtrip_violations(case: Mapping[str, Any], out: Any) -> List[str]:
    """Every check of a ``roundtrip`` case, evaluated on ``out`` (the re-encoded document, or the
    dead-letter copy when ``apply_dead_letter`` was applied). Empty list = the case holds."""
    problems: List[str] = []
    if not _strict_equal(out.get("attempts"), case["expect_attempts"]):
        problems.append(f"/attempts: {out.get('attempts')!r} != {case['expect_attempts']!r}")
    problems += _preserved_violations(out, case.get("expect_preserved", {}))
    problems += _absent_violations(out, case.get("expect_absent_after_reencode", []))
    if "apply_dead_letter" in case and "expect_dead_letter" in case:
        block = out.get("dead_letter")
        if not isinstance(block, dict):
            problems.append("dead_letter block is missing")
        else:
            for field, value in case["expect_dead_letter"].items():
                if field not in block or not _strict_equal(block[field], value):
                    problems.append(f"dead_letter.{field}: {block.get(field)!r} != {value!r}")
    return problems


def _forbidden_violations(case: Mapping[str, Any], out: Any) -> List[str]:
    """Post-re-encode checks of a ``forbidden_keys`` case."""
    return _absent_violations(out, case["expect_absent_after_reencode"]) + _preserved_violations(
        out, case.get("expect_preserved", {})
    )


def _run_roundtrip(case: Mapping[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Drive a ``roundtrip`` case: decode -> attempts+1 -> encode (-> SDK DLQ helper when the case
    carries ``apply_dead_letter``). Returns the document the checks run on and the decode warnings."""
    raw = (SUITE / case["file"]).read_text(encoding="utf-8")
    with _captured_warnings() as warnings:
        decoded = EnvelopeCodec.decode(raw)
        accepted = EnvelopeCodec.accepts(decoded)
        body = _retry_once(raw)
    assert accepted, "must be accepted"
    out = json.loads(body)
    apply = case.get("apply_dead_letter")
    if apply is not None:
        out = json.loads(
            EnvelopeCodec.encode(
                annotate(
                    out,
                    apply["reason"],
                    apply["original_queue"],
                    out["attempts"],
                    error="forced failure",
                    exception="RuntimeError",
                )
            )
        )
    return out, warnings


class HelperTest(unittest.TestCase):
    def test_strict_equal_is_type_aware(self) -> None:
        self.assertFalse(_strict_equal(True, 1))
        self.assertFalse(_strict_equal({}, []))
        self.assertFalse(_strict_equal("1", 1))
        self.assertTrue(_strict_equal({"a": [1, {"b": None}]}, {"a": [1, {"b": None}]}))

    def test_pointer_resolves_escapes_and_misses(self) -> None:
        doc = {"a/b": {"m~n": [10, 20]}}
        self.assertEqual(_pointer(doc, "/a~1b/m~0n/1"), 20)
        self.assertIs(_pointer(doc, "/missing"), _MISSING)
        self.assertIs(_pointer(doc, "/a~1b/m~0n/9"), _MISSING)


class RoundtripConformanceTest(unittest.TestCase):
    def test_roundtrip_cases(self) -> None:
        for case in _section("roundtrip")["cases"]:
            with self.subTest(case=case["name"]):
                out, warnings = _run_roundtrip(case)
                self.assertEqual([], warnings, "decode must emit no warning")
                self.assertEqual([], _roundtrip_violations(case, out))

    def test_dead_letter_case_really_goes_through_the_dlq_helper(self) -> None:
        case = next(c for c in _section("roundtrip")["cases"] if "apply_dead_letter" in c)
        out, _ = _run_roundtrip(case)
        self.assertEqual(case["apply_dead_letter"]["reason"], out["dead_letter"]["reason"])
        self.assertEqual(case["apply_dead_letter"]["original_queue"], out["dead_letter"]["original_queue"])
        self.assertEqual(out["attempts"], out["dead_letter"]["attempts"])


class RunnerSelfTest(unittest.TestCase):
    """The runner must REPORT violations, not just stay green: synthetic cases prove that
    ``expect_absent_after_reencode``, ``expect_preserved`` and ``expect_dead_letter`` are evaluated."""

    OUT = {
        "job": "urn:acme:a",
        "attempts": 1,
        "meta": {"queue": "orders", "flag": True},
        "urn": "urn:acme:a",
        "dead_letter": {"reason": "failed", "original_queue": "orders"},
    }

    def test_absent_pointer_that_exists_is_reported(self) -> None:
        case = {"expect_attempts": 1, "expect_absent_after_reencode": ["/urn"]}
        self.assertEqual(["/urn re-emitted"], _roundtrip_violations(case, self.OUT))
        self.assertEqual([], _roundtrip_violations({**case, "expect_absent_after_reencode": ["/nope"]}, self.OUT))

    def test_preserved_missing_or_different_or_retyped_is_reported(self) -> None:
        base = {"expect_attempts": 1}
        for expected, fragment in (
            ({"/meta/missing": 1}, "dropped"),
            ({"/meta/queue": "other"}, "!="),
            ({"/meta/flag": 1}, "!="),  # true != 1
        ):
            with self.subTest(expected=expected):
                problems = _roundtrip_violations({**base, "expect_preserved": expected}, self.OUT)
                self.assertEqual(1, len(problems))
                self.assertIn(fragment, problems[0])
        self.assertEqual([], _roundtrip_violations({**base, "expect_preserved": {"/meta/flag": True}}, self.OUT))

    def test_wrong_attempts_is_reported(self) -> None:
        self.assertEqual(1, len(_roundtrip_violations({"expect_attempts": 2}, self.OUT)))

    def test_expect_dead_letter_is_evaluated_only_with_apply_dead_letter(self) -> None:
        apply = {"reason": "failed", "original_queue": "orders"}
        case = {"expect_attempts": 1, "apply_dead_letter": apply}
        self.assertEqual([], _roundtrip_violations({**case, "expect_dead_letter": apply}, self.OUT))
        wrong = {"reason": "poison", "original_queue": "orders"}
        self.assertEqual(1, len(_roundtrip_violations({**case, "expect_dead_letter": wrong}, self.OUT)))
        no_block = {k: v for k, v in self.OUT.items() if k != "dead_letter"}
        self.assertEqual(
            ["dead_letter block is missing"],
            _roundtrip_violations({**case, "expect_dead_letter": apply}, no_block),
        )
        # without apply_dead_letter the field is ignored (plain roundtrip)
        self.assertEqual([], _roundtrip_violations({"expect_attempts": 1, "expect_dead_letter": wrong}, self.OUT))

    def test_forbidden_case_reports_present_and_changed_pointers(self) -> None:
        case = {"expect_absent_after_reencode": ["/urn"], "expect_preserved": {"/job": "urn:acme:other"}}
        self.assertEqual(2, len(_forbidden_violations(case, self.OUT)))
        ok = {"expect_absent_after_reencode": ["/timestamp"], "expect_preserved": {"/job": "urn:acme:a"}}
        self.assertEqual([], _forbidden_violations(ok, self.OUT))

    def test_real_cases_fail_when_their_expectations_are_tampered_with(self) -> None:
        cases = {c["name"]: c for c in _section("roundtrip")["cases"]}
        out, _ = _run_roundtrip(cases["urn-only-roundtrip"])
        tampered = {**cases["urn-only-roundtrip"], "expect_absent_after_reencode": ["/job"]}
        self.assertEqual(["/job re-emitted"], _roundtrip_violations(tampered, out))
        tampered = {**cases["urn-only-roundtrip"], "expect_preserved": {"/job": "urn:acme:zzz"}}
        self.assertEqual(1, len(_roundtrip_violations(tampered, out)))
        out, _ = _run_roundtrip(cases["dead-letter-roundtrip"])
        tampered = {**cases["dead-letter-roundtrip"], "expect_dead_letter": {"reason": "poison"}}
        self.assertEqual(1, len(_roundtrip_violations(tampered, out)))

    def test_tracked_fields_exist_in_the_vendored_manifest(self) -> None:
        # a manifest without the v1.4.0 fields would make the runner checks above vacuous
        rt = _section("roundtrip")["cases"]
        self.assertTrue(any("expect_absent_after_reencode" in c for c in rt))
        self.assertTrue(any("apply_dead_letter" in c and "expect_dead_letter" in c for c in rt))
        self.assertTrue(any("expect_preserved" in c for c in _section("forbidden_keys")["cases"]))


class ExtensionWriteConformanceTest(unittest.TestCase):
    def test_extension_write_cases(self) -> None:
        for case in _section("extension_write")["cases"]:
            with self.subTest(case=case["name"]):
                env = EnvelopeCodec.make(case["urn"], case["data"], queue=case["queue"], meta=case["meta"])
                encoded = EnvelopeCodec.encode(env)
                self.assertEqual([], _preserved_violations(json.loads(encoded), case["expect_envelope"]))

                # re-emit chain: decode -> attempts+1 -> encode keeps the extension keys
                again = json.loads(_retry_once(encoded))
                self.assertEqual(1, again["attempts"])
                self.assertEqual([], _preserved_violations(again, case["expect_envelope"]))


class EncodeRejectConformanceTest(unittest.TestCase):
    def test_encode_reject_cases(self) -> None:
        ran = 0
        for case in _section("encode_reject")["cases"]:
            with self.subTest(case=case["name"]):
                self.assertEqual("reject", case["expect"])
                if "meta_entries" in case:
                    continue  # a repeated key is inexpressible in a single mapping (Go-only case)
                with self.assertRaises(EnvelopeKeyError) as caught:
                    env = EnvelopeCodec.make(
                        case["urn"], case["data"], queue=case["queue"], meta=case.get("meta")
                    )
                    # preserved extras of an inbound message, placed straight into the dict
                    env.update(case.get("extras_top", {}))
                    env["meta"].update(case.get("extras_meta", {}))
                    EnvelopeCodec.encode(env)
                self.assertEqual(case["offending_key"], caught.exception.pointer)
                self.assertIn(case["offending_key"], str(caught.exception))
                ran += 1
        self.assertGreater(ran, 0)


class ProducerMetaTest(unittest.TestCase):
    def test_forbidden_meta_key_is_rejected(self) -> None:
        with self.assertRaises(EnvelopeKeyError):
            EnvelopeCodec.make("urn:acme:a", {}, queue="q", meta={"ts": 1})

    def test_core_meta_key_collision_is_rejected(self) -> None:
        for key in ("id", "queue", "lang", "schema_version", "created_at"):
            with self.subTest(key=key), self.assertRaises(EnvelopeKeyError) as caught:
                EnvelopeCodec.make("urn:acme:a", {}, queue="q", meta={key: "x"})
            self.assertEqual("/meta/" + key, caught.exception.pointer)

    def test_non_mapping_meta_is_rejected(self) -> None:
        with self.assertRaises(Exception):
            EnvelopeCodec.make("urn:acme:a", {}, queue="q", meta=[("ext", 1)])  # type: ignore[arg-type]

    def test_meta_is_keyword_only(self) -> None:
        with self.assertRaises(TypeError):
            EnvelopeCodec.make("urn:acme:a", {}, "q", None, {"ext": 1})  # type: ignore[misc]

    def test_ext_block_survives_a_round_trip_and_input_is_not_mutated(self) -> None:
        meta = {"ext": {"acme": 1}}
        env = EnvelopeCodec.make("urn:acme:a", {"n": 1}, queue="q", meta=meta)
        self.assertEqual({"ext": {"acme": 1}}, meta)
        self.assertEqual({"acme": 1}, env["meta"]["ext"])
        self.assertEqual(env, EnvelopeCodec.decode(EnvelopeCodec.encode(env)))
        again = json.loads(_retry_once(EnvelopeCodec.encode(env)))
        self.assertEqual({"acme": 1}, again["meta"]["ext"])

    def test_encode_of_a_forbidden_key_dict_raises(self) -> None:
        base = EnvelopeCodec.make("urn:acme:a", {}, queue="q")
        for container, key in (
            (None, "timestamp"),
            ("meta", "max_retries"),
            ("meta", "attempts"),
            ("meta", "source"),
            ("meta", "ts"),
            (None, "urn"),
        ):
            with self.subTest(container=container, key=key):
                env = {**base, "meta": dict(base["meta"])}
                (env if container is None else env["meta"])[key] = 1
                with self.assertRaises(EnvelopeKeyError):
                    EnvelopeCodec.encode(env)


class BlankJobTest(unittest.TestCase):
    def _raw(self, **fields: Any) -> str:
        base = EnvelopeCodec.make("urn:acme:x", {"n": 1}, queue="q", trace_id="t-1")
        del base["job"]
        base.update(fields)
        return json.dumps(base, ensure_ascii=False)

    def test_each_ascii_whitespace_char_makes_job_blank(self) -> None:
        for blank in ("", " ", "\t", "\n", "\u000b", "\u000c", "\r", " \t\n\u000b\u000c\r "):
            with self.subTest(job=repr(blank)):
                with _captured_warnings() as warnings:
                    env = EnvelopeCodec.decode(self._raw(job=blank, urn="urn:acme:b"))
                self.assertEqual([], warnings)
                self.assertEqual("urn:acme:b", env["job"])
                self.assertNotIn("urn", env)
                self.assertTrue(EnvelopeCodec.accepts(env))
                # the same fallback for a raw (not decoded) dict
                self.assertEqual("urn:acme:b", EnvelopeCodec.urn({"job": blank, "urn": "urn:acme:b"}))

    def test_urn_only_becomes_job_without_a_warning(self) -> None:
        with _captured_warnings() as warnings:
            env = EnvelopeCodec.decode(self._raw(urn="urn:acme:b"))
        self.assertEqual([], warnings)
        self.assertEqual("urn:acme:b", env["job"])
        self.assertNotIn("urn", env)

    def test_unicode_space_job_is_not_blank(self) -> None:
        with self.assertLogs("babelqueue.codec", level=logging.WARNING) as logs:
            env = EnvelopeCodec.decode(self._raw(job="\u2003", urn="urn:acme:b"))
        self.assertEqual("\u2003", env["job"])
        self.assertNotIn("urn", env)
        self.assertTrue(any("/urn" in line for line in logs.output))
        self.assertEqual("\u2003", EnvelopeCodec.urn({"job": "\u2003", "urn": "urn:acme:b"}))

    def test_job_wins_over_urn_with_a_warning_naming_the_pointer(self) -> None:
        with self.assertLogs("babelqueue.codec", level=logging.WARNING) as logs:
            env = EnvelopeCodec.decode(self._raw(job="urn:acme:a", urn="urn:acme:b"))
        self.assertEqual("urn:acme:a", env["job"])
        self.assertNotIn("urn", env)
        self.assertEqual(1, len(logs.output))
        self.assertIn("Dropped forbidden envelope key /urn from the envelope", logs.output[0])

    def test_blank_job_without_urn_is_rejected(self) -> None:
        env = EnvelopeCodec.decode(self._raw(job="  "))
        self.assertFalse(EnvelopeCodec.accepts(env))
        self.assertEqual("", EnvelopeCodec.urn(env))

    def test_reencode_of_a_decoded_envelope_never_raises(self) -> None:
        env = EnvelopeCodec.decode(self._raw(job="urn:acme:a", urn="urn:acme:b", timestamp=1))
        EnvelopeCodec.encode(env)


class DataShapeConformanceTest(unittest.TestCase):
    def test_data_shape_cases(self) -> None:
        cases = _section("data_shape")["cases"]
        self.assertEqual({c["mode"] for c in cases}, {"encode", "decode"})
        for case in cases:
            with self.subTest(case=case["name"]):
                if case["mode"] == "encode":
                    env = EnvelopeCodec.make(case["urn"], case["data"], queue=case["queue"])
                    encoded = EnvelopeCodec.encode(env)
                    # Re-serialise the emitted `data` value compactly: a dict prints `{...}`,
                    # a list `[...]`, so `{}` vs `[]` is preserved exactly.
                    data_json = json.dumps(
                        json.loads(encoded)["data"], ensure_ascii=False, separators=(",", ":")
                    )
                    self.assertEqual(data_json, case["expect_encoded_data_json"])
                    self.assertIn('"data":' + case["expect_encoded_data_json"], encoded)
                elif case["mode"] == "decode":
                    raw = (SUITE / case["file"]).read_text(encoding="utf-8")
                    verdict = EnvelopeCodec.accepts(EnvelopeCodec.decode(raw))
                    self.assertEqual(verdict, case["valid"], case.get("reason"))
                else:  # pragma: no cover - guards against a new, unhandled mode
                    self.fail(f"unknown data_shape mode {case['mode']!r}")


class ForbiddenKeysConformanceTest(unittest.TestCase):
    def test_forbidden_key_cases(self) -> None:
        cases = _section("forbidden_keys")["cases"]
        for case in cases:
            with self.subTest(case=case["name"]):
                raw = (SUITE / case["file"]).read_text(encoding="utf-8")
                self.assertEqual(case["expect"], "warn")
                self.assertIsNot(_pointer(json.loads(raw), case["forbidden_key"]), _MISSING)

                with self.assertLogs("babelqueue.codec", level=logging.WARNING) as logs:
                    env = EnvelopeCodec.decode(raw)
                self.assertNotEqual(env, {}, "decode must succeed")
                self.assertTrue(EnvelopeCodec.accepts(env), "decode must not reject")
                self.assertTrue(
                    any(case["forbidden_key"] in line for line in logs.output),
                    f"no warning naming {case['forbidden_key']}: {logs.output}",
                )

                out = json.loads(EnvelopeCodec.encode(env))
                self.assertEqual([], _forbidden_violations(case, out))

    def test_all_five_contract_keys_are_covered(self) -> None:
        covered = {c["forbidden_key"] for c in _section("forbidden_keys")["cases"]}
        expected = {"/timestamp", "/meta/max_retries", "/meta/attempts", "/meta/source", "/meta/ts"}
        # a subset check: conformance 1.4.0 adds "/urn" cases on top of the five §10 keys
        self.assertLessEqual(expected, covered)


class PayloadSchemaUnicodeConformanceTest(unittest.TestCase):
    def test_unicode_length_cases(self) -> None:
        section = _section("payload_schema_unicode")
        results: List[Tuple[str, bool]] = []
        for case in section["cases"]:
            with self.subTest(case=case["name"]):
                valid = validate_schema(section["schema"], case["data"]) is None
                results.append((case["name"], valid))
                self.assertEqual(case["valid"], valid, f"case {case['name']}")
        self.assertEqual(len(results), len(section["cases"]))


if __name__ == "__main__":
    unittest.main()
