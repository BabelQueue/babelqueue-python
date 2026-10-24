"""Runs the behavioural sections of the shared cross-SDK conformance suite
(``roundtrip``, ``extension_write``, ``encode_reject``, ``data_shape``, ``forbidden_keys``,
``payload_schema_unicode``) and the conformance 1.5.0 / 1.6.0 sections (``trim``, ``job_type``,
``traceparent``, ``redis_layout``, ``redis_delay``, ``delay``, ``backoff``, ``retry_backoff``,
``kafka_poison``) plus the previously unexercised ``idempotency``, ``redis`` and ``rabbitmq``
blocks. ``redis_layout`` drives the real Lua scripts: it needs a real Redis (``BQ_REQUIRE_REDIS``;
CI) and falls back to the in-process fakeredis + lupa emulator only when none is required.

These sections are mandatory: unlike the optional broker sections, a missing section fails
the run instead of skipping it. Pointers are RFC 6901 and equality is type-strict and deep
(``true`` never equals ``1``, ``{}`` never equals ``[]``) — the shared runner contract.
"""

from __future__ import annotations

import base64
import contextlib
import inspect
import json
import logging
import re
import time
import unittest
from importlib.util import find_spec
from unittest import mock
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple

import redis_backend
from test_kafka_transport import FakeConsumer, FakeProducer, OffsetConsumer, OffsetMessage, ReportingProducer
from test_kafka_transport import transport as poison_transport
from test_sqs_transport import FakeSQS

from babelqueue import BabelQueue, BabelQueueError, EnvelopeCodec
from babelqueue.app import retry_delay
from babelqueue.dead_letter import annotate
from babelqueue.exceptions import EnvelopeKeyError
from babelqueue.idempotency import InMemoryStore
from babelqueue.idempotency import wrap as idempotency_wrap
from babelqueue._poison_record import build_poison_record
from babelqueue.kafka_transport import DEFAULT_RETRY_TIERS_MS, POISON_ROUTE, KafkaTransport, retry_topic
from babelqueue.pika_transport import PikaTransport
from babelqueue.redis_transport import RedisTransport, _LegacyHandle
from babelqueue.schema import validate_schema
from babelqueue.sqs_transport import SqsTransport
from babelqueue.transport import NON_NUMERIC_ATTEMPTS_ROUTE, PoisonRoute, ReceivedMessage

SUITE = Path(__file__).parent / "conformance"
MANIFEST = json.loads((SUITE / "manifest.json").read_text(encoding="utf-8"))

_MISSING = object()


def _has_module(name: str) -> bool:
    try:
        return find_spec(name) is not None
    except ModuleNotFoundError:  # the parent package itself is absent
        return False


_HAS_OTEL = _has_module("opentelemetry.trace")


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
        # ADR-0032 section 6: neither a valid job nor a valid urn -> decode raises.
        with self.assertRaises(BabelQueueError):
            EnvelopeCodec.decode(self._raw(job="  "))

    def test_decode_raises_when_there_is_no_valid_identity(self) -> None:
        shapes = (
            {},
            {"job": None},
            {"job": 0},
            {"job": False, "urn": 0},
            {"job": [], "urn": []},
            {"job": {}, "urn": {}},
            {"job": "  ", "urn": "\t\n"},
            {"job": 7, "urn": ["urn:acme:b"]},
            {"urn": True},
        )
        for fields in shapes:
            with self.subTest(fields=repr(fields)):
                with self.assertRaises(BabelQueueError):
                    EnvelopeCodec.decode(self._raw(**fields))

    def test_decode_still_returns_empty_for_unreadable_input(self) -> None:
        for raw in ("not-json", "[]", "7", "null", ""):
            with self.subTest(raw=raw):
                self.assertEqual({}, EnvelopeCodec.decode(raw))

    def test_decode_lenient_keeps_the_identity_less_envelope_instead_of_raising(self) -> None:
        env = EnvelopeCodec.decode_lenient(self._raw(job=0))
        self.assertEqual("", EnvelopeCodec.urn(env))
        self.assertFalse(EnvelopeCodec.accepts(env))
        self.assertEqual({}, EnvelopeCodec.decode_lenient("not-json"))
        ok = EnvelopeCodec.decode_lenient(self._raw(job=0, urn="urn:acme:b"))
        self.assertEqual("urn:acme:b", ok["job"])

    def test_non_string_job_warning_only_claims_a_fallback_when_a_urn_exists(self) -> None:
        with _captured_warnings() as with_urn:
            EnvelopeCodec.decode(self._raw(job=0, urn="urn:acme:b"))
        self.assertEqual(1, len(with_urn))
        self.assertIn("falling back to urn", with_urn[0])
        with _captured_warnings() as without_urn:
            EnvelopeCodec.decode_lenient(self._raw(job=0))
        self.assertEqual(1, len(without_urn))
        self.assertNotIn("falling back", without_urn[0])
        self.assertIn("/job", without_urn[0])

    def test_trim_is_linear_on_a_huge_ascii_whitespace_input(self) -> None:
        # Regression lock: a regex / char loop with backtracking would blow up here. The bound is
        # deliberately generous (measured ~3 ms each) so it never flakes on a slow runner.
        pad = " " * 200_000
        started = time.perf_counter()
        env = EnvelopeCodec.make(f"{pad}urn:acme:a{pad}", {"n": 1}, trace_id=f"{pad}t-1{pad}")
        self.assertEqual("urn:acme:a", env["job"])
        self.assertEqual("t-1", env["trace_id"])
        self.assertTrue(EnvelopeCodec.accepts(env))
        decoded = EnvelopeCodec.decode(self._raw(job=pad, urn="urn:acme:b", trace_id=pad))
        self.assertEqual("urn:acme:b", decoded["job"])
        self.assertFalse(EnvelopeCodec.accepts(decoded))
        self.assertLess(time.perf_counter() - started, 5.0)

    def test_falsy_non_string_job_is_never_a_urn(self) -> None:
        for bad in (False, 0, [], {}, 1, True, ["urn:acme:a"]):
            with self.subTest(job=repr(bad)):
                self.assertEqual("", EnvelopeCodec.urn({"job": bad}))
                env = EnvelopeCodec.make("urn:acme:a", {"n": 1}, queue="q", trace_id="t-1")
                env["job"] = bad
                self.assertFalse(EnvelopeCodec.accepts(env))

    def test_falsy_non_string_job_falls_back_to_a_string_urn_alias(self) -> None:
        for bad in (False, 0, [], {}):
            with self.subTest(job=repr(bad)):
                self.assertEqual("urn:acme:b", EnvelopeCodec.urn({"job": bad, "urn": "urn:acme:b"}))
                # ADR-0032 section 6: the non-string job is ignored AND the decoder warns.
                with _captured_warnings() as warnings:
                    env = EnvelopeCodec.decode(self._raw(job=bad, urn="urn:acme:b"))
                self.assertEqual(1, len(warnings))
                self.assertIn("/job", warnings[0])
                self.assertEqual("urn:acme:b", env["job"])
                self.assertTrue(EnvelopeCodec.accepts(env))

    def test_non_string_urn_alias_is_never_a_urn(self) -> None:
        for bad in (1, True, ["urn:acme:b"], {"k": 1}):
            with self.subTest(urn=repr(bad)):
                self.assertEqual("", EnvelopeCodec.urn({"urn": bad}))

    def test_unicode_space_is_not_trimmed_by_make(self) -> None:
        # ADR-0032 section 5: only the ASCII set is whitespace; U+00A0 / U+2003 / U+3000 stay.
        for ch in ("\u00a0", "\u2003", "\u3000"):
            with self.subTest(char=hex(ord(ch))):
                env = EnvelopeCodec.make(f"{ch}urn:acme:a{ch}", {"n": 1}, trace_id=f"{ch}t-1{ch}")
                self.assertEqual(f"{ch}urn:acme:a{ch}", env["job"])
                self.assertEqual(f"{ch}t-1{ch}", env["trace_id"])
                self.assertTrue(EnvelopeCodec.accepts(env))

    def test_ascii_whitespace_is_trimmed_by_make_but_unicode_next_to_it_is_kept(self) -> None:
        env = EnvelopeCodec.make(" \t\u00a0urn:acme:a\u3000\n ", {"n": 1}, trace_id=" \vt-1\f")
        self.assertEqual("\u00a0urn:acme:a\u3000", env["job"])
        self.assertEqual("t-1", env["trace_id"])

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



# --------------------------------------------------------------------------------------------
# Case ids, case-level skips and the sections added by conformance 1.5.0 (ADR-0032).
# --------------------------------------------------------------------------------------------

_NOT_CASES = ("description", "schema")


def iter_cases(manifest: Mapping[str, Any], section: str) -> List[Tuple[str, Any]]:
    """Every ``(case id, case body)`` of a manifest section (ADR-0032 case-id rule).

    A section's ``cases`` array gives one case per element, identified by its ``name`` or else
    ``<section>#<index>``; a sub-key with its own ``cases`` array gives ``<sub-key>/<name>`` (or
    ``<sub-key>/<index>``); any other object sub-key is a single case keyed by the sub-key;
    ``description`` and ``schema`` are never cases. The manifest top-level ``cases`` array is
    the section named ``cases`` (the value is a plain list there).
    """
    value = manifest[section]
    found: List[Tuple[str, Any]] = []
    if isinstance(value, list):
        for index, case in enumerate(value):
            found.append((str(case.get("name") or f"{section}#{index}"), case))
        return found
    cases = value.get("cases")
    if isinstance(cases, list):
        for index, case in enumerate(cases):
            found.append((str(case.get("name") or f"{section}#{index}"), case))
    for key, sub in value.items():
        if key in _NOT_CASES or key == "cases" or not isinstance(sub, dict):
            continue
        sub_cases = sub.get("cases")
        if isinstance(sub_cases, list):
            for index, case in enumerate(sub_cases):
                found.append((f"{key}/{case.get('name') or index}", case))
        else:
            found.append((key, sub))
    return found


KNOWN_GAPS_FILE = Path(__file__).resolve().parents[1] / "conformance-known-gaps.json"


def _known_gaps() -> Dict[str, Dict[str, str]]:
    """Known gaps: ``section -> case id -> reason``, read from ``conformance-known-gaps.json``.

    A known gap is an honest ``fail`` in ``results.json``, never a ``skip``. The case is still
    RUN: ``_run_cases`` records the real outcome in :data:`GAP_RESULTS` instead of failing the
    unit test, ``run_section`` reports it from there, and a gap case that now passes is a failing
    subtest ("stale gap") as well as a ``pass`` the completeness gate
    (``scripts/check_conformance_results.py``) rejects. A fail is accepted only when listed here.
    """
    document = json.loads(KNOWN_GAPS_FILE.read_text(encoding="utf-8"))
    gaps: Dict[str, Dict[str, str]] = {}
    for entry in document["gaps"]:
        gaps.setdefault(entry["section"], {})[entry["case"]] = entry["reason"]
    return gaps


KNOWN_GAPS: Dict[str, Dict[str, str]] = _known_gaps()

#: ``(section, case id) -> None`` when a known-gap case passed, else the observed failure message.
#: Filled by :func:`_run_cases`; read by ``run_section`` in ``test_conformance.py``.
GAP_RESULTS: Dict[Tuple[str, str], Optional[str]] = {}


def _ensure(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _identity(raw: str) -> Tuple[Any, List[str]]:
    """Decode ``raw`` and return ``(resolved urn or None, decode warnings)``.

    ``None`` means decode raised :class:`BabelQueueError` — ADR-0032 section 6: neither a valid
    ``job`` nor a valid ``urn``. Nothing else is accepted as "no identity": a decode that returns
    an identity-less envelope is a failure of the rule, not a pass.
    """
    with _captured_warnings() as warnings:
        try:
            env = EnvelopeCodec.decode(raw)
        except BabelQueueError:
            return None, warnings
    _ensure(env != {}, "decode of a readable object must not yield an empty envelope")
    return EnvelopeCodec.urn(env), warnings


def _eval_trim(case: Mapping[str, Any]) -> None:
    mode = case["mode"]
    if mode == "make":
        extra = {"trace_id": case["trace_id"]} if "trace_id" in case else {}
        if case["expect"] == "reject":
            try:
                EnvelopeCodec.make(case["urn"], {}, **extra)
            except BabelQueueError:
                return
            raise AssertionError("make must reject a URN that is empty after ASCII trim")
        env = EnvelopeCodec.make(case["urn"], {}, **extra)
        for field, expected in case["expect"].items():
            _ensure(_strict_equal(env[field], expected), f"{field}: {env[field]!r} != {expected!r}")
    elif mode == "decode":
        raw = (SUITE / case["file"]).read_text(encoding="utf-8")
        urn, warnings = _identity(raw)
        if case["valid"]:
            _ensure(urn == case["expect"]["urn"], f"urn: {urn!r} != {case['expect']['urn']!r}")
            _ensure(
                bool(warnings) == case["expect_warning"],
                f"warning expected={case['expect_warning']} got={warnings}",
            )
        else:
            _ensure(urn is None, f"no valid identity expected, resolved {urn!r}")
    elif mode == "accepts":
        raw = (SUITE / case["file"]).read_text(encoding="utf-8")
        env = EnvelopeCodec.decode(raw)
        if case.get("expect_decode_ok"):
            _ensure(env != {}, "decode must still succeed")
        verdict = EnvelopeCodec.accepts(env)
        _ensure(verdict == case["expect_accepts"], f"accepts: {verdict} != {case['expect_accepts']}")
    elif mode == "reemit":
        raw = (SUITE / case["file"]).read_text(encoding="utf-8")
        out = json.loads(_retry_once(raw))
        _ensure(
            _strict_equal(out.get("attempts"), case["expect_attempts"]),
            f"/attempts: {out.get('attempts')!r} != {case['expect_attempts']!r}",
        )
        problems = _preserved_violations(out, case["expect_preserved"])
        _ensure(not problems, "; ".join(problems))
    else:  # pragma: no cover - guards against a new, unhandled mode
        raise AssertionError(f"unknown trim mode {mode!r}")


def _eval_job_type(case: Mapping[str, Any]) -> None:
    raw = (SUITE / case["file"]).read_text(encoding="utf-8")
    urn, warnings = _identity(raw)
    if case["valid"]:
        _ensure(urn == case["expect"]["urn"], f"urn: {urn!r} != {case['expect']['urn']!r}")
        _ensure(
            bool(warnings) == case["expect_warning"],
            f"warning expected={case['expect_warning']} got={warnings}",
        )
    else:
        _ensure(urn is None, f"no valid identity expected, resolved {urn!r}")


def _eval_traceparent(case: Mapping[str, Any]) -> None:
    from opentelemetry.trace import get_current_span

    from babelqueue import otel
    from babelqueue.headers import _headers_scope

    header = case["traceparent"]
    with _headers_scope({} if header is None else {otel.HEADER_TRACEPARENT: header}):
        ctx = otel._remote_parent_from_headers()
    source = "traceparent"
    if ctx is None:
        source = "trace_id"
        ctx = otel._parent_context(case["trace_id"])
    span_context = get_current_span(ctx).get_span_context()
    actual: Dict[str, Any] = {
        "source": source,
        "trace_id_hex": format(span_context.trace_id, "032x"),
    }
    expect = case["expect"]
    if source == "traceparent":
        actual["parent_span_id"] = format(span_context.span_id, "016x")
        actual["flags"] = format(int(span_context.trace_flags), "02x")
    for field, expected in expect.items():
        _ensure(actual.get(field) == expected, f"{field}: {actual.get(field)!r} != {expected!r}")


class _KeyRecorder:
    """A client stub that records the KEYS of every Lua script the transport evaluates.

    The key-name cases need no broker: ``RedisTransport`` never imports ``redis`` when it is
    given a client, and only the key names it hands to ``eval`` are under test.
    """

    def __init__(self) -> None:
        self.eval_keys: List[List[str]] = []

    def eval(self, script: Any, numkeys: Any, *args: Any) -> None:
        self.eval_keys.append([str(k) for k in args[:numkeys]])


def _eval_redis_key_names(case: Mapping[str, Any]) -> None:
    client = _KeyRecorder()
    transport = RedisTransport(
        "redis://x", client=client, laravel_compat=True, key_prefix=case["key_prefix"]
    )
    queue = case["queue"]
    body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:acme:a", {"n": 1}))
    transport.publish(queue, body)
    transport.release(ReceivedMessage(body=body, queue=queue, handle=body), 30)
    # publish evals [ready, notify]; release evals [delayed, reserved].
    publish_keys, release_keys = client.eval_keys[0], client.eval_keys[-1]
    actual = {
        "ready": publish_keys[0],
        "notify": publish_keys[1],
        "delayed": release_keys[0],
        "reserved": release_keys[1],
    }
    for name, expected in case["expect"].items():
        _ensure(actual[name] == expected, f"{name} key: {actual[name]!r} != {expected!r}")


def _eval_redis_delay(case: Mapping[str, Any]) -> None:
    transport = RedisTransport("redis://x", client=object(), laravel_compat=True, clock=lambda: case["now"])
    actual = transport._available_at(case["delay"])  # noqa: SLF001 - the SDK's score function
    _ensure(actual == case["expect_score"], f"score: {actual!r} != {case['expect_score']!r}")


_DEADLINE_ANY = "<any>"


def _score_of(value: Any) -> float:
    return float("inf") if value == "+inf" else float(value)


def _redis_for_layout() -> Any:
    client, _kind = redis_backend.connect()
    if client is None:
        if redis_backend.require_real_redis():
            raise AssertionError(
                f"BQ_REQUIRE_REDIS is set but no real Redis is reachable at {redis_backend.REDIS_URL}"
            )
        raise unittest.SkipTest("redis_layout needs a Redis (real, or fakeredis + lupa): none available")
    return client


def _entry_matches(member: str, score: float, expected: Mapping[str, Any]) -> bool:
    """Does one ``(member, score)`` of a zset match one expected ``{member|member_json|body_json|token+raw, score}``?"""
    if score != _score_of(expected["score"]):
        return False
    if "member" in expected:
        return member == expected["member"]
    if "token" in expected:
        if expected["token"] == _DEADLINE_ANY:
            return re.fullmatch(r"[0-9a-f]{32}:" + re.escape(expected["raw"]), member) is not None
        return member == f"{expected['token']}:{expected['raw']}"
    want = expected["member_json"] if "member_json" in expected else expected["body_json"]
    try:
        return _strict_equal(json.loads(member), want)
    except ValueError:
        return False


def _zset_violation(label: str, actual: List[Tuple[str, float]], expected: List[Mapping[str, Any]]) -> Optional[str]:
    remaining = list(actual)
    for want in expected:
        for index, (member, score) in enumerate(remaining):
            if _entry_matches(member, score, want):
                del remaining[index]
                break
        else:
            return f"{label}: no entry matches {want!r} in {actual!r}"
    if remaining:
        return f"{label}: unexpected entries {remaining!r}"
    return None


class _LayoutScenario:
    """One ``redis_layout`` behavioural case against a Redis: state, steps, per-step expectations."""

    def __init__(self, case: Mapping[str, Any]) -> None:
        self.case = case
        self.client = _redis_for_layout()
        self.queue = redis_backend.unique_queue("bqlayout")
        self.zset = case["layout"] == "zset"
        self.now = [0.0]
        options: Dict[str, Any] = {}
        if "retry_after" in case:  # JSON null is ``None``: the deadline is +inf
            options["retry_after"] = case["retry_after"]
        if "migration_batch_size" in case:
            options["migration_batch_size"] = case["migration_batch_size"]
        self.transport = RedisTransport(
            "redis://x", client=self.client, laravel_compat=self.zset, clock=lambda: self.now[0], **options
        )
        self.held: Dict[str, Optional[ReceivedMessage]] = {}
        self.last: Optional[ReceivedMessage] = None

    # -- keys
    @property
    def ready_key(self) -> str:
        return f"queues:{self.queue}" if self.zset else self.queue

    def key(self, name: str) -> str:
        return {
            "delayed": f"{self.ready_key}:delayed",
            "reserved": f"{self.ready_key}:reserved",
            "notify": f"{self.ready_key}:notify",
            "processing": f"{self.queue}:processing",
            "deadlines": f"{self.queue}:processing:deadlines",
        }[name]

    def zrange(self, key: str) -> List[Tuple[str, float]]:
        return [(str(m), float(sc)) for m, sc in self.client.zrange(key, 0, -1, withscores=True)]

    # -- state
    def seed(self) -> None:
        state = self.case["state"]
        for member in state.get("ready", []):
            self.client.rpush(self.ready_key, member)
        for member in state.get("processing", []):
            self.client.rpush(self.key("processing"), member)
        for name in ("delayed", "reserved", "deadlines"):
            for entry in state.get(name, []):
                member = entry["member"] if "member" in entry else f"{entry['token']}:{entry['raw']}"
                self.client.zadd(self.key(name), {member: _score_of(entry["score"])})
        generate = state.get("delayed_generate")
        if generate:
            for i in range(generate["count"]):
                self.client.zadd(self.key("delayed"), {generate["member"] % i: _score_of(generate["score"])})
        for _ in range(state.get("notify", 0)):
            self.client.rpush(self.key("notify"), "1")

    # -- steps
    def reservation(self, ref: Any) -> ReceivedMessage:
        if isinstance(ref, str):
            held = self.held.get(ref)
            _ensure(held is not None, f"reservation {ref!r} is not held")
            assert held is not None
            return held
        return ReceivedMessage(body=ref["raw"], queue=self.queue, handle=_LegacyHandle(ref["raw"], ref["token"]))

    def run_step(self, step: Mapping[str, Any]) -> Dict[str, Any]:
        self.now[0] = step["now"]
        op = step["op"]
        observed: Dict[str, Any] = {}
        if op == "reserve":
            message = self.transport.pop(self.queue, timeout=0)
            self.last = message
            if "as" in step:
                self.held[step["as"]] = message
            observed["message"] = message
        elif op == "push_delayed":
            self.transport.publish_delayed(self.queue, step["body"], step["delay"])
        elif op == "migrate":
            self.transport._migrate(self.ready_key)  # noqa: SLF001 - the contract names the script, not an API
        elif op == "sweep":
            self.transport._sweep(self.queue)  # noqa: SLF001
        elif op == "release":
            self.transport.release(self.reservation(step["reservation"]), step["delay"])
        elif op == "ack":
            self.transport.ack(self.reservation(step["reservation"]))
        elif op == "dead_letter":
            message = self.reservation(step["reservation"])
            self.transport.dead_letter(message, f"{self.queue}.dlq", message.body)
        else:
            raise AssertionError(f"unknown redis_layout op {op!r}")
        return observed

    # -- expectations
    def check(self, expect: Mapping[str, Any], observed: Mapping[str, Any]) -> None:
        message: Optional[ReceivedMessage] = observed.get("message")
        if "outcome" in expect:
            if message is None:
                actual = "empty"
            elif message.route_urn == NON_NUMERIC_ATTEMPTS_ROUTE:
                actual = "poison"
            else:
                actual = "delivered"
            _ensure(actual == expect["outcome"], f"outcome: {actual!r} != {expect['outcome']!r}")
        if "handler_body" in expect:
            _ensure(message is not None, "handler_body expected but nothing was reserved")
            assert message is not None
            _ensure(message.body == expect["handler_body"], f"handler body: {message.body!r} != {expect['handler_body']!r}")
        if "reported_attempts" in expect:
            _ensure(message is not None, "reported_attempts expected but nothing was reserved")
            assert message is not None
            reported = json.loads(message.handle)["attempts"]
            _ensure(reported == expect["reported_attempts"], f"attempts: {reported!r} != {expect['reported_attempts']!r}")
        if "ready" in expect:
            actual_ready = list(self.client.lrange(self.ready_key, 0, -1))
            _ensure(actual_ready == expect["ready"], f"ready: {actual_ready!r} != {expect['ready']!r}")
        if "ready_generate" in expect:
            wanted = [expect["ready_generate"]["member"] % i for i in range(expect["ready_generate"]["count"])]
            actual_ready = list(self.client.lrange(self.ready_key, 0, -1))
            _ensure(actual_ready == wanted, f"ready (generated): {actual_ready[:3]!r}... != {wanted[:3]!r}...")
        if "ready_count" in expect:
            count = self.client.llen(self.queue)
            _ensure(count == expect["ready_count"], f"ready_count: {count} != {expect['ready_count']}")
        if "dlq_count" in expect:
            count = self.client.llen(f"{self.queue}.dlq")
            _ensure(count == expect["dlq_count"], f"dlq_count: {count} != {expect['dlq_count']}")
        if "notify" in expect:
            count = self.client.llen(self.key("notify"))
            _ensure(count == expect["notify"], f"notify: {count} != {expect['notify']}")
        if "processing" in expect:
            actual_processing = list(self.client.lrange(self.key("processing"), 0, -1))
            _ensure(
                actual_processing == expect["processing"],
                f"processing: {actual_processing!r} != {expect['processing']!r}",
            )
        for name in ("delayed", "reserved", "deadlines"):
            if name in expect:
                violation = _zset_violation(name, self.zrange(self.key(name)), expect[name])
                _ensure(violation is None, str(violation))
        if expect.get("tokens_distinct"):
            tokens = [member.split(":", 1)[0] for member, _ in self.zrange(self.key("deadlines"))]
            _ensure(len(tokens) == len(set(tokens)), f"deadline tokens are not distinct: {tokens!r}")

    def run(self) -> None:
        try:
            self.seed()
            for step in self.case["steps"]:
                observed = self.run_step(step)
                if "expect" in step:
                    self.check(step["expect"], observed)
        finally:
            redis_backend.purge(self.client, self.queue)


def _eval_redis_layout(case: Mapping[str, Any]) -> None:
    """``redis_layout``: the three key-name cases (no broker) and the behavioural scenarios (Redis)."""
    if "layout" in case:
        if case.get("name", "").startswith("encode-fails-") and redis_backend.connect()[1] == "emulated":
            # fakeredis + lupa serialises inf/NaN without error where real lua-cjson raises, so the
            # cjson.encode guard cannot be exercised on the emulator (see PopScriptWithoutRedisTest
            # for the broker-free proof). CI runs these against a real redis:7 (BQ_REQUIRE_REDIS).
            raise unittest.SkipTest("encode-fails-* needs a real Redis: the emulator's cjson masks the bug")
        _LayoutScenario(case).run()
    else:
        _eval_redis_key_names(case)


def _eval_release_visibility(case: Mapping[str, Any]) -> None:
    fake = FakeSQS()
    transport = SqsTransport("sqs://", client=fake)
    transport.publish("orders", EnvelopeCodec.encode(EnvelopeCodec.make("urn:acme:a", {"n": 1})))
    message = transport.pop("orders", timeout=0)
    _ensure(message is not None, "the published message must be receivable")
    assert message is not None
    transport.redeliver(message, message.body, case["delay_seconds"])
    _ensure(len(fake.visibility_changes) == 1, "release must issue one ChangeMessageVisibility")
    timeout = fake.visibility_changes[0]["VisibilityTimeout"]
    expected = case["expect"]["visibility_timeout"]
    _ensure(timeout == expected, f"VisibilityTimeout: {timeout!r} != {expected!r}")


def _eval_idempotency_dedup_key(case: Mapping[str, Any]) -> None:
    env = json.loads((SUITE / case["envelope_file"]).read_text(encoding="utf-8"))
    _ensure(
        env["meta"]["id"] == case["expected_key"], "expected_key must be the fixture's meta.id"
    )
    store = InMemoryStore()
    calls: List[str] = []
    handler = idempotency_wrap(store, lambda data, meta: calls.append(meta["id"]))
    handler(env["data"], env["meta"])
    handler(env["data"], env["meta"])
    _ensure(calls == [case["expected_key"]], f"handler ran {calls!r}")
    _ensure(store.seen(case["expected_key"]), "the seen-set must be keyed on meta.id verbatim")
    _ensure(not store.seen(env["trace_id"]), "trace_id must not be the dedupe key")


def _eval_idempotency_sequence(case: Mapping[str, Any]) -> None:
    store = InMemoryStore()
    invocations: List[str] = []
    outcomes: List[str] = []

    def effect(data: Any, meta: Any) -> None:
        invocations.append(meta["id"])
        if outcomes.pop(0) == "throw":
            raise RuntimeError("handler failed")

    handler = idempotency_wrap(store, effect)
    for index, delivery in enumerate(case["deliveries"]):
        if delivery.get("forget_before"):
            store.forget(delivery["meta_id"])
        before = len(invocations)
        outcomes.append(delivery.get("outcome", "ok"))
        try:
            handler({}, {"id": delivery["meta_id"]})
        except RuntimeError:
            pass
        outcomes.clear()
        ran = len(invocations) > before
        _ensure(
            ran == (delivery["expect_effect"] == "run"),
            f"delivery {index}: expected {delivery['expect_effect']}, handler ran={ran}",
        )
    _ensure(
        len(invocations) == case["expected_effects"],
        f"effects: {len(invocations)} != {case['expected_effects']}",
    )


def _eval_redis_payload_identity(case: Mapping[str, Any]) -> None:
    raw = (SUITE / case["envelope_file"]).read_text(encoding="utf-8")
    client = _redis_for_layout()
    queue = redis_backend.unique_queue("bqidentity")
    try:
        transport = RedisTransport("redis://x", client=client)
        transport.publish(queue, raw)
        _ensure(list(client.lrange(queue, 0, -1)) == [raw], "the queue element must be the envelope, unwrapped")
        message = transport.pop(queue, timeout=0)
        _ensure(message is not None and message.body == raw, "reserve must return identical bytes")
    finally:
        redis_backend.purge(client, queue)


class _FakeBasicProperties:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class _FakePika:
    BasicProperties = _FakeBasicProperties


def _eval_rabbitmq_property_projection(case: Mapping[str, Any]) -> None:
    raw = (SUITE / case["envelope_file"]).read_text(encoding="utf-8")
    transport = PikaTransport.__new__(PikaTransport)
    transport._pika = _FakePika()
    props = transport._properties(raw)
    for name, expected in case["properties"].items():
        _ensure(getattr(props, name) == expected, f"{name}: {getattr(props, name)!r} != {expected!r}")
    for name, expected in case["headers"].items():
        actual = props.headers.get(name)
        _ensure(_strict_equal(actual, expected), f"header {name}: {actual!r} != {expected!r}")


# -- delay (SQS section 3.4), backoff (Kafka sections 6.4/6.5) and the worker retry formula --------
# These probe the transports' PUBLIC surface:
#   SqsTransport.publish(queue, body, delay_seconds=N)      -> SendMessage DelaySeconds
#   KafkaTransport(retry_tiers_ms=[...]).publish_delayed(queue, body, delay_ms)
#   KafkaTransport.redeliver(message, body, delay)          -> retry-tier republish
#   babelqueue.app.retry_delay(n, base, multiplier, maximum, jitter)


def _eval_sqs_delay(case: Mapping[str, Any]) -> None:
    fake = FakeSQS()
    transport = SqsTransport("sqs://", client=fake, fifo=case["queue_type"] == "fifo")
    body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:acme:a", {"n": 1}))
    expect = case["expect"]
    parameters = inspect.signature(transport.publish).parameters
    accepts_delay = "delay_seconds" in parameters or any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()
    )
    _ensure(accepts_delay, "SqsTransport.publish has no delay_seconds path")
    try:
        transport.publish("orders", body, delay_seconds=case["delay_seconds"])  # type: ignore[call-arg]
    except BabelQueueError:
        _ensure(expect == "reject", "a valid delay must not be rejected")
        _ensure(not fake.sent, "a rejected delay must not send anything")
        return
    _ensure(expect != "reject", f"delay {case['delay_seconds']} must raise BabelQueueError")
    _ensure(len(fake.sent) == 1, "publish must issue one SendMessage")
    actual = fake.sent[0].get("DelaySeconds")
    _ensure(
        actual == expect["delay_seconds_attribute"],
        f"DelaySeconds: {actual!r} != {expect['delay_seconds_attribute']!r}",
    )


def _kafka_transport(case: Mapping[str, Any]) -> Tuple[KafkaTransport, FakeProducer]:
    producer = FakeProducer()
    transport = KafkaTransport(
        producer=producer,
        consumer_factory=lambda _queue: FakeConsumer(),
        retry_tiers_ms=list(case["retry_tiers_ms"]),
    )
    return transport, producer


def _check_retry_record(case: Mapping[str, Any], producer: FakeProducer) -> None:
    expect = case["expect"]
    _ensure(len(producer.produced) == 1, f"expected one retry-topic record, got {len(producer.produced)}")
    record = producer.produced[0]
    tiers = list(case["retry_tiers_ms"])
    wanted_topic = expect.get("topic") or retry_topic(case["work_topic"], tiers.index(expect["tier_ms"]) + 1)
    _ensure(record["topic"] == wanted_topic, f"retry topic {record['topic']!r} != {wanted_topic!r}")
    headers = {
        key: (value.decode("utf-8") if isinstance(value, bytes) else str(value))
        for key, value in (record["headers"] or [])
    }
    for name, value in expect["headers"].items():
        _ensure(headers.get(name) == value, f"header {name}: {headers.get(name)!r} != {value!r}")
    if "key" in expect:
        _ensure(record.get("key") == expect["key"].encode("utf-8"), f"record key: {record.get('key')!r}")


def _eval_kafka_tier(case: Mapping[str, Any]) -> None:
    transport, producer = _kafka_transport(case)
    body = EnvelopeCodec.encode(EnvelopeCodec.make("urn:acme:a", {"n": 1}))
    if case["expect"] == "reject":
        try:
            transport.publish_delayed(case["work_topic"], body, case["delay_ms"])
        except BabelQueueError:
            _ensure(not producer.produced, "a rejected delay must not produce a record")
            return
        raise AssertionError(f"delay {case['delay_ms']} ms must raise BabelQueueError")
    transport.publish_delayed(case["work_topic"], body, case["delay_ms"])
    _check_retry_record(case, producer)


def _eval_kafka_retry_republish(case: Mapping[str, Any]) -> None:
    """``retry_republish`` (``attempts``) and ``retry_attempt_tier`` (``attempts_before``): redeliver
    a record that had ``attempts`` attempts before this failure; the runtime hands over the body
    with ``attempts`` already advanced, and the transport picks the tier from the attempts before."""
    before = case["attempts"] if "attempts" in case else case["attempts_before"]
    transport, producer = _kafka_transport(case)
    envelope = EnvelopeCodec.make("urn:acme:a", {"n": 1})
    envelope["attempts"] = before
    key = case["key"].encode("utf-8") if "key" in case else None
    message = ReceivedMessage(body=EnvelopeCodec.encode(envelope), queue=case["work_topic"], key=key)
    envelope["attempts"] = before + 1
    body = EnvelopeCodec.encode(envelope)
    if case["expect"] == "reject":
        try:
            transport.redeliver(message, body, 0)
        except BabelQueueError:
            _ensure(not producer.produced, "a rejected release must not produce a record")
            return
        raise AssertionError("a release with no retry tiers must raise BabelQueueError")
    transport.redeliver(message, body, 0)
    _check_retry_record(case, producer)
    sent = json.loads(producer.produced[0]["value"])
    _ensure(
        sent["attempts"] == case["expect"]["body_attempts"],
        f"body attempts: {sent['attempts']!r} != {case['expect']['body_attempts']!r}",
    )


def _eval_kafka_topic_names(case: Mapping[str, Any]) -> None:
    actual = retry_topic(case["work_topic"], case["n"])
    _ensure(actual == case["expect_topic"], f"retry topic: {actual!r} != {case['expect_topic']!r}")


def _eval_kafka_default_tiers(case: Mapping[str, Any]) -> None:
    _ensure(case["constant"] == "DEFAULT_RETRY_TIERS_MS", f"unknown constant {case['constant']!r}")
    _ensure(
        list(DEFAULT_RETRY_TIERS_MS) == case["expect"],
        f"DEFAULT_RETRY_TIERS_MS: {DEFAULT_RETRY_TIERS_MS!r} != {case['expect']!r}",
    )


def _eval_retry_backoff(case: Mapping[str, Any]) -> None:
    def delay(uniform: Optional[Callable[[float, float], float]] = None) -> float:
        extra = {"uniform": uniform} if uniform is not None else {}
        return retry_delay(
            case["n"],
            case["retry_backoff"],
            case["retry_backoff_multiplier"],
            case["retry_backoff_max"],
            case["retry_jitter"],
            **extra,
        )

    if "expect_delay_seconds" in case:
        _ensure(case["retry_jitter"] == "none", "an exact expectation is only legal without jitter")
        actual = delay()
        _ensure(actual == case["expect_delay_seconds"], f"delay: {actual!r} != {case['expect_delay_seconds']!r}")
        return
    low, high = case["expect_delay_range_seconds"]["min"], case["expect_delay_range_seconds"]["max"]
    draws = [delay() for _ in range(25)] + [delay(lambda a, b: a), delay(lambda a, b: b)]
    for actual in draws:
        _ensure(low <= actual <= high, f"jittered delay {actual!r} outside [{low}, {high}]")


def _eval_kafka_poison(case: Mapping[str, Any]) -> None:
    """``kafka_poison``: the core decoder's verdict, then the Kafka fate (ADR-0034 section 10)."""
    body = case["body"]
    if KafkaTransport._decode_failure(body) is not None:  # noqa: SLF001 - the transport's own verdict
        verdict = "poison"
    else:
        verdict = "ok" if EnvelopeCodec.urn(EnvelopeCodec.decode_lenient(body)) else "no_identity"
    _ensure(verdict == case["expect"], f"verdict: {verdict!r} != {case['expect']!r}")
    consumer = OffsetConsumer([OffsetMessage(body.encode("utf-8"), [], offset=0)])
    message = poison_transport(ReportingProducer(), consumer).pop("orders")
    _ensure(message is not None, "the record must be delivered to the runtime")
    assert message is not None
    if case["expect"] == "ok":
        _ensure(not isinstance(message.route_urn, PoisonRoute), "a record with an identity is not poison")
    else:  # no_identity and poison share one fate: dead-lettered raw as poison, no handler call
        _ensure(
            isinstance(message.route_urn, PoisonRoute) and message.route_urn == POISON_ROUTE,
            f"route: {message.route_urn!r} is not the poison route",
        )
        _ensure(message.body == body, "a poison record is handed over raw")


def _top_level_names(text: str) -> List[str]:
    """Every top-level member name of the JSON object ``text`` (duplicates kept, last-wins not applied)."""
    names: List[List[str]] = []

    def hook(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
        names.append([key for key, _ in pairs])
        return {}

    json.loads(text, object_pairs_hook=hook)
    return names[-1]  # the outermost object completes last


def _without_dead_letter(record: Mapping[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in record.items() if key != "dead_letter"}


def _eval_poison_dlq(case: Mapping[str, Any]) -> None:
    """``poison_dlq``: the DLQ record shape of a poison body (ADR-0035)."""
    if "body_base64" in case:
        raw_bytes = base64.b64decode(case["body_base64"])
    else:
        raw_bytes = case["body"].encode("utf-8")
    expect = case["expect"]
    text = raw_bytes.decode("utf-8", "surrogateescape")
    block = annotate({}, "poison", "orders", 0, error="Undecodable BabelQueue body: x", exception="BabelQueueError")[
        "dead_letter"
    ]
    record_text = build_poison_record(text, block)

    def parse(value: str) -> Any:
        return json.loads(value, parse_float=lambda lit: ("float", lit), parse_int=lambda lit: ("int", lit))

    record = parse(record_text)
    _ensure(isinstance(record, dict) and record.get("dead_letter") == parse(json.dumps(block)), "dead_letter member")
    stripped = _without_dead_letter(record)
    wrapper = parse(json.dumps(expect["record"], ensure_ascii=False)) if "record" in expect else None
    shape = expect["shape"]
    if shape in ("splice", "wrapper"):
        _ensure(stripped == wrapper, f"record {stripped!r} differs from the expected {wrapper!r}")
        if shape == "splice":
            _ensure(record_text.startswith(text.strip(" \t\r\n")[:-1].rstrip(" \t\r\n")), "a splice keeps the original bytes")
        return
    if stripped == wrapper:  # splice_or_wrapper: the lossless wrapper
        return
    _ensure("raw" not in stripped and "raw_encoding" not in stripped, "a splice carries no raw/raw_encoding")
    for literal in expect.get("splice_literals", []):
        _ensure(literal in record_text, f"splice must keep {literal!r} verbatim")
    names = {name for name in _top_level_names(record_text) if name != "dead_letter"}
    _ensure(names == set(expect.get("splice_keys", [])), f"splice keys {sorted(names)!r}")


def _eval_delay(case: Mapping[str, Any]) -> None:
    # Cases carrying `queue_type` are the section 3.4 publish-delay cases; the rest are releases.
    (_eval_sqs_delay if "queue_type" in case else _eval_release_visibility)(case)


def _eval_backoff(case: Mapping[str, Any]) -> None:
    """``backoff``: dispatch on the case shape (the evaluator only sees the case body)."""
    if "delay_ms" in case:
        _eval_kafka_tier(case)
    elif "attempts" in case or "attempts_before" in case:
        _eval_kafka_retry_republish(case)
    elif "expect_topic" in case:
        _eval_kafka_topic_names(case)
    elif "constant" in case:
        _eval_kafka_default_tiers(case)
    else:
        raise AssertionError(f"unrecognised backoff case shape: {sorted(case)}")


def _run_cases(
    test: unittest.TestCase,
    section: str,
    evaluate: Callable[[Mapping[str, Any]], None],
    *,
    select: Callable[[str], bool] = lambda case_id: True,
) -> None:
    """One ``subTest(case=<id>)`` per case id of ``section``.

    A known-gap case is run like any other; its real outcome goes to :data:`GAP_RESULTS` and a
    assertion failure does not fail the test (it is an accepted, documented gap; any other
    exception is an evaluator or implementation bug and propagates). A known-gap case that
    PASSES is a stale gap and fails its subtest.
    """
    ran = 0
    gaps = KNOWN_GAPS.get(section, {})
    for case_id, case in iter_cases(MANIFEST, section):
        if not select(case_id):
            continue
        ran += 1
        if case_id not in gaps:
            with test.subTest(case=case_id):
                evaluate(case)
            continue
        try:
            evaluate(case)
        except AssertionError as exc:  # only an assertion is the expected outcome; bugs propagate
            GAP_RESULTS[(section, case_id)] = f"{type(exc).__name__}: {exc}"
        else:
            GAP_RESULTS[(section, case_id)] = None
            with test.subTest(case=case_id):
                test.fail(f"stale gap: {section}/{case_id} passes; remove it from {KNOWN_GAPS_FILE.name}")
    test.assertGreater(ran, 0, f"conformance section {section!r} ran no case")


class TrimConformanceTest(unittest.TestCase):
    def test_trim_cases(self) -> None:
        _run_cases(self, "trim", _eval_trim)

    def test_every_mode_is_covered(self) -> None:
        modes = {case["mode"] for _, case in iter_cases(MANIFEST, "trim")}
        self.assertEqual({"make", "decode", "accepts", "reemit"}, modes)


class JobTypeConformanceTest(unittest.TestCase):
    def test_job_type_cases(self) -> None:
        _run_cases(self, "job_type", _eval_job_type)

    def test_a_non_string_job_is_warned_about_and_a_blank_one_is_not(self) -> None:
        by_name = {case_id: case for case_id, case in iter_cases(MANIFEST, "job_type")}
        self.assertTrue(by_name["null-job-ignored-falls-back-to-urn"]["expect_warning"])
        self.assertFalse(by_name["empty-string-job-falls-back-without-warning"]["expect_warning"])


class RunnerCaseIdTest(unittest.TestCase):
    #: ``section -> number of case ids`` of the vendored conformance 1.6.0 manifest.
    EXPECTED_COUNTS = {"trim": 35, "job_type": 11, "traceparent": 14, "delay": 9, "backoff": 29, "redis_layout": 59, "redis_delay": 8, "retry_backoff": 18, "kafka_poison": 92, "poison_dlq": 43, "idempotency": 7, "redis": 1, "rabbitmq": 1}

    def test_case_id_counts_match_the_manifest(self) -> None:
        for section, count in self.EXPECTED_COUNTS.items():
            with self.subTest(section=section):
                self.assertEqual(count, len(iter_cases(MANIFEST, section)))

    def test_case_ids_are_unique_within_a_section(self) -> None:
        for section in self.EXPECTED_COUNTS:
            ids = [case_id for case_id, _ in iter_cases(MANIFEST, section)]
            with self.subTest(section=section):
                self.assertEqual(len(ids), len(set(ids)))

    def test_known_gaps_name_only_real_case_ids_with_a_reason(self) -> None:
        for section, gaps in KNOWN_GAPS.items():
            self.assertIn(section, MANIFEST["sections"])
            ids = {case_id for case_id, _ in iter_cases(MANIFEST, section)}
            for case_id, reason in gaps.items():
                with self.subTest(section=section, case=case_id):
                    self.assertIn(case_id, ids)
                    self.assertTrue(reason.strip())

    def test_known_gap_entries_are_unique(self) -> None:
        document = json.loads(KNOWN_GAPS_FILE.read_text(encoding="utf-8"))
        keys = [(entry["section"], entry["case"]) for entry in document["gaps"]]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(sum(len(c) for c in KNOWN_GAPS.values()), len(keys))

    def test_there_are_no_known_gaps(self) -> None:
        # Every conformance 1.6.0 case is implemented (r1 Faz 08): the gap list stays empty, and a
        # new entry is a deliberate, reviewed change to this test as well.
        self.assertEqual({}, KNOWN_GAPS)


@unittest.skipUnless(_HAS_OTEL, "the traceparent section needs the opentelemetry-api package")
class TraceparentConformanceTest(unittest.TestCase):
    def test_traceparent_cases(self) -> None:
        _run_cases(self, "traceparent", _eval_traceparent)


class RedisLayoutConformanceTest(unittest.TestCase):
    def test_redis_layout_cases(self) -> None:
        _run_cases(self, "redis_layout", _eval_redis_layout)


class RedisDelayConformanceTest(unittest.TestCase):
    def test_redis_delay_cases(self) -> None:
        _run_cases(self, "redis_delay", _eval_redis_delay)


class RetryBackoffConformanceTest(unittest.TestCase):
    def test_retry_backoff_cases(self) -> None:
        _run_cases(self, "retry_backoff", _eval_retry_backoff)


class KafkaPoisonConformanceTest(unittest.TestCase):
    def test_kafka_poison_cases(self) -> None:
        _run_cases(self, "kafka_poison", _eval_kafka_poison)


class PoisonDlqConformanceTest(unittest.TestCase):
    def test_poison_dlq_cases(self) -> None:
        _run_cases(self, "poison_dlq", _eval_poison_dlq)

    def test_an_invalid_utf8_body_is_base64_never_replaced(self) -> None:
        cases = [case for _, case in iter_cases(MANIFEST, "poison_dlq") if "body_base64" in case]
        self.assertTrue(cases, "the manifest must carry invalid-UTF-8 cases")
        for case in cases:
            self.assertEqual("base64", case["expect"]["record"]["raw_encoding"])


class DelayConformanceTest(unittest.TestCase):
    def test_delay_cases(self) -> None:
        _run_cases(self, "delay", _eval_delay)


class BackoffConformanceTest(unittest.TestCase):
    def test_backoff_cases(self) -> None:
        _run_cases(self, "backoff", _eval_backoff)


class GapMechanismTest(unittest.TestCase):
    """The known-gap bookkeeping itself: gaps are run, failures recorded, passes are stale."""

    def _probe(self, outcome: Callable[[], None]) -> Tuple[unittest.TestResult, Optional[str]]:
        case_id, gap_case = iter_cases(MANIFEST, "trim")[0]

        def evaluate(case: Mapping[str, Any]) -> None:
            if case is gap_case:
                outcome()

        class Probe(unittest.TestCase):
            def runTest(self) -> None:  # noqa: N802 - unittest hook
                _run_cases(self, "trim", evaluate)

        result = unittest.TestResult()
        with mock.patch.dict(KNOWN_GAPS, {"trim": {case_id: "r"}}), mock.patch.dict(GAP_RESULTS):
            Probe().run(result)
            recorded = GAP_RESULTS.get(("trim", case_id), "<not run>")
        return result, recorded

    def test_a_failing_gap_case_is_recorded_and_does_not_fail_the_test(self) -> None:
        def outcome() -> None:
            raise AssertionError("not implemented")

        result, recorded = self._probe(outcome)
        self.assertEqual([], result.failures + result.errors)
        self.assertEqual("AssertionError: not implemented", recorded)

    def test_a_passing_gap_case_is_a_stale_gap(self) -> None:
        result, recorded = self._probe(lambda: None)
        self.assertIsNone(recorded)
        self.assertEqual(1, len(result.failures))
        self.assertIn("stale gap", result.failures[0][1])

    def test_a_non_assertion_error_in_a_gap_case_is_not_swallowed(self) -> None:
        def outcome() -> None:
            raise TypeError("evaluator bug")

        result, recorded = self._probe(outcome)
        self.assertEqual(1, len(result.errors))
        self.assertIn("TypeError: evaluator bug", result.errors[0][1])
        self.assertEqual("<not run>", recorded)

    def test_sqs_delay_evaluator_does_not_relabel_a_publish_internal_type_error(self) -> None:
        def publish(self: SqsTransport, queue: str, body: str, delay_seconds: int = 0) -> None:
            raise TypeError("bug inside publish")

        case = next(c for _, c in iter_cases(MANIFEST, "delay") if "queue_type" in c)
        with mock.patch.object(SqsTransport, "publish", publish), self.assertRaises(TypeError):
            _eval_sqs_delay(case)


class IdempotencyConformanceTest(unittest.TestCase):
    def test_idempotency_cases(self) -> None:
        for case_id, case in iter_cases(MANIFEST, "idempotency"):
            with self.subTest(case=case_id):
                if case_id == "dedup_key":
                    _eval_idempotency_dedup_key(case)
                else:
                    _eval_idempotency_sequence(case)


class RedisConformanceTest(unittest.TestCase):
    def test_payload_identity(self) -> None:
        for case_id, case in iter_cases(MANIFEST, "redis"):
            with self.subTest(case=case_id):
                _eval_redis_payload_identity(case)


class RabbitmqConformanceTest(unittest.TestCase):
    def test_property_projection(self) -> None:
        for case_id, case in iter_cases(MANIFEST, "rabbitmq"):
            with self.subTest(case=case_id):
                _eval_rabbitmq_property_projection(case)


if __name__ == "__main__":
    unittest.main()
