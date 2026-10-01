"""Runs the behavioural sections of the shared cross-SDK conformance suite
(``roundtrip``, ``data_shape``, ``forbidden_keys``, ``payload_schema_unicode``).

These sections are mandatory: unlike the optional broker sections, a missing section fails
the run instead of skipping it. Pointers are RFC 6901 and equality is type-strict and deep
(``true`` never equals ``1``, ``{}`` never equals ``[]``) — the shared runner contract.
"""

from __future__ import annotations

import json
import logging
import unittest
from pathlib import Path
from typing import Any, List, Tuple

from babelqueue import BabelQueue, EnvelopeCodec
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
                raw = (SUITE / case["file"]).read_text(encoding="utf-8")
                self.assertTrue(EnvelopeCodec.accepts(EnvelopeCodec.decode(raw)), "must be accepted")

                out = json.loads(_retry_once(raw))

                self.assertTrue(_strict_equal(out.get("attempts"), case["expect_attempts"]))
                for pointer, expected in case["expect_preserved"].items():
                    actual = _pointer(out, pointer)
                    self.assertIsNot(actual, _MISSING, f"{pointer} was dropped")
                    self.assertTrue(
                        _strict_equal(actual, expected),
                        f"{pointer}: {actual!r} != {expected!r}",
                    )


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
                for pointer in case["expect_absent_after_reencode"]:
                    self.assertIs(_pointer(out, pointer), _MISSING, f"{pointer} re-emitted")

    def test_all_five_contract_keys_are_covered(self) -> None:
        covered = {c["forbidden_key"] for c in _section("forbidden_keys")["cases"]}
        expected = {"/timestamp", "/meta/max_retries", "/meta/attempts", "/meta/source", "/meta/ts"}
        self.assertEqual(covered, expected)


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
