"""Tests for the optional runtime GDPR field encryption (ADR-0030).

The cipher used here is a STDLIB-ONLY fake — it deliberately imports no ``cryptography`` library,
proving the core needs no crypto dependency (GR-7). It still honours the full :class:`Cipher`
contract: a random per-call nonce so the same plaintext encrypts differently each time, a
byte-for-byte inverse, and a wrong-key ``decrypt`` that raises. A production cipher (AES-256-GCM via
``cryptography``) is documented in ``babelqueue/gdpr.py``; the contract these tests pin is the same.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import unittest
from typing import Any, Dict

from babelqueue import EnvelopeCodec
from babelqueue.exceptions import DecryptError
from babelqueue.gdpr import Cipher, protect, unprotect
from babelqueue.schema import SensitivePath, sensitive_paths


class XorCipher:
    """A reversible, authenticated, STDLIB-ONLY reference :class:`Cipher` for tests.

    NOT cryptographically strong — it is a keystream XOR with a keyed digest tag — but it exercises
    every property the contract depends on: a random per-call nonce (same plaintext → different
    ciphertext), a byte-exact inverse, base64 output that drops into JSON, and a wrong-key/tampered
    ``decrypt`` that raises (the tag check fails). This is the seam :func:`protect`/:func:`unprotect`
    rely on; the production cipher is the caller's AES-GCM.
    """

    _NONCE = 16

    def __init__(self, key: bytes) -> None:
        self._key = key

    def _keystream(self, nonce: bytes, length: int) -> bytes:
        out = bytearray()
        counter = 0
        while len(out) < length:
            out += hashlib.sha256(self._key + nonce + counter.to_bytes(4, "big")).digest()
            counter += 1
        return bytes(out[:length])

    def _tag(self, nonce: bytes, ciphertext: bytes) -> bytes:
        return hashlib.sha256(self._key + b"tag" + nonce + ciphertext).digest()[:16]

    def encrypt(self, plaintext: bytes) -> str:
        nonce = os.urandom(self._NONCE)
        ks = self._keystream(nonce, len(plaintext))
        ct = bytes(p ^ k for p, k in zip(plaintext, ks))
        sealed = nonce + ct + self._tag(nonce, ct)
        return base64.b64encode(sealed).decode("ascii")

    def decrypt(self, ciphertext: str) -> bytes:
        raw = base64.b64decode(ciphertext)
        nonce, body, tag = raw[: self._NONCE], raw[self._NONCE : -16], raw[-16:]
        if self._tag(nonce, body) != tag:
            raise ValueError("authentication failed (wrong key or tampered ciphertext)")
        ks = self._keystream(nonce, len(body))
        return bytes(c ^ k for c, k in zip(body, ks))


KEY = b"0123456789abcdef0123456789abcdef"


def _cipher() -> Cipher:
    return XorCipher(KEY)


# A schema marking the leaf `email`, the nested `profile.full_name`, the array-item
# `addresses[].line`, and a container `secret_blob` (a whole object marked sensitive).
SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "order_id": {"type": "integer"},
        "email": {"type": "string", "x-gdpr-sensitive": "email"},
        "profile": {
            "type": "object",
            "properties": {
                "full_name": {"type": "string", "x-gdpr-sensitive": True},
                "tier": {"type": "string"},
            },
        },
        "addresses": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "line": {"type": "string", "x-gdpr-sensitive": True},
                    "city": {"type": "string"},
                },
            },
        },
        "secret_blob": {"type": "object", "x-gdpr-sensitive": True},
    },
}


def _data() -> Dict[str, Any]:
    return {
        "order_id": 1042,
        "email": "alice@example.com",
        "profile": {"full_name": "Alice Liddell", "tier": "gold"},
        "addresses": [
            {"line": "1 Rabbit Hole", "city": "Wonderland"},
            {"line": "2 Looking Glass", "city": "Mirror"},
        ],
        "secret_blob": {"pan": "4111111111111111", "exp": 1230},
    }


class SensitivePathsTest(unittest.TestCase):
    def test_walks_nested_object_array_item_and_container(self) -> None:
        paths = sensitive_paths(SCHEMA)
        # Sorted by path, with the array-item using the "field[]" segment.
        self.assertEqual(
            paths,
            [
                SensitivePath("addresses[].line", ""),
                SensitivePath("email", "email"),
                SensitivePath("profile.full_name", ""),
                SensitivePath("secret_blob", ""),
            ],
        )

    def test_root_mark_reports_empty_path(self) -> None:
        self.assertEqual(sensitive_paths({"x-gdpr-sensitive": True}), [SensitivePath("", "")])

    def test_validation_neutral_shapes_are_ignored(self) -> None:
        # false, "", and a number never mark a property (mirrors the Go parser).
        schema = {
            "type": "object",
            "properties": {
                "a": {"type": "string", "x-gdpr-sensitive": False},
                "b": {"type": "string", "x-gdpr-sensitive": ""},
                "c": {"type": "string", "x-gdpr-sensitive": 1},
                "d": {"type": "string"},
            },
        }
        self.assertEqual(sensitive_paths(schema), [])

    def test_malformed_schema_is_skipped_not_raised(self) -> None:
        self.assertEqual(sensitive_paths({"properties": "nope", "items": 5}), [])


class ProtectRoundTripTest(unittest.TestCase):
    def test_round_trip_restores_data_byte_for_byte(self) -> None:
        original = _data()
        data = _data()
        protect(data, SCHEMA, _cipher())

        # Marked leaves became ciphertext STRINGS; everything else is untouched.
        self.assertIsInstance(data["email"], str)
        self.assertNotEqual(data["email"], original["email"])
        self.assertIsInstance(data["profile"]["full_name"], str)
        self.assertIsInstance(data["addresses"][0]["line"], str)
        self.assertIsInstance(data["secret_blob"], str)  # the whole container is one ciphertext
        # Non-sensitive siblings are bit-identical.
        self.assertEqual(data["order_id"], 1042)
        self.assertEqual(data["profile"]["tier"], "gold")
        self.assertEqual(data["addresses"][0]["city"], "Wonderland")

        unprotect(data, SCHEMA, _cipher())
        self.assertEqual(data, original)

    def test_nested_object_only(self) -> None:
        schema = {"type": "object", "properties": {"profile": {"type": "object",
                  "properties": {"full_name": {"type": "string", "x-gdpr-sensitive": True}}}}}
        data = {"profile": {"full_name": "Bob", "tier": "silver"}}
        protect(data, schema, _cipher())
        self.assertNotEqual(data["profile"]["full_name"], "Bob")
        self.assertEqual(data["profile"]["tier"], "silver")
        unprotect(data, schema, _cipher())
        self.assertEqual(data, {"profile": {"full_name": "Bob", "tier": "silver"}})

    def test_array_items(self) -> None:
        schema = {"type": "object", "properties": {"addresses": {"type": "array", "items":
                  {"type": "object", "properties": {"line": {"type": "string", "x-gdpr-sensitive": True}}}}}}
        data = {"addresses": [{"line": "a", "city": "x"}, {"line": "b", "city": "y"}]}
        protect(data, schema, _cipher())
        self.assertNotEqual(data["addresses"][0]["line"], "a")
        self.assertNotEqual(data["addresses"][1]["line"], "b")
        self.assertEqual(data["addresses"][0]["city"], "x")
        unprotect(data, schema, _cipher())
        self.assertEqual(data, {"addresses": [{"line": "a", "city": "x"}, {"line": "b", "city": "y"}]})

    def test_array_leaf_directly_marked(self) -> None:
        # A path ending in "[]" marks the array ELEMENTS themselves (e.g. a list of phone strings).
        schema = {"type": "object", "properties":
                  {"phones": {"type": "array", "items": {"type": "string", "x-gdpr-sensitive": True}}}}
        self.assertEqual(sensitive_paths(schema), [SensitivePath("phones[]", "")])
        data = {"phones": ["+1-555-0100", "+1-555-0199"]}
        protect(data, schema, _cipher())
        self.assertNotEqual(data["phones"][0], "+1-555-0100")
        self.assertNotEqual(data["phones"][1], "+1-555-0199")
        unprotect(data, schema, _cipher())
        self.assertEqual(data, {"phones": ["+1-555-0100", "+1-555-0199"]})

    def test_non_string_values_round_trip_exactly(self) -> None:
        # A sensitive leaf need not be a string: number/bool/null/object restore to the same type.
        schema = {"type": "object", "properties": {k: {"x-gdpr-sensitive": True}
                  for k in ("n", "b", "z", "o")}}
        data: Dict[str, Any] = {"n": 1230, "b": True, "z": None, "o": {"k": [1, 2, 3]}}
        protect(data, schema, _cipher())
        for k in ("n", "b", "z", "o"):
            self.assertIsInstance(data[k], str)
        unprotect(data, schema, _cipher())
        self.assertEqual(data, {"n": 1230, "b": True, "z": None, "o": {"k": [1, 2, 3]}})

    def test_root_mark_is_skipped_no_addressable_leaf(self) -> None:
        # A mark on the root schema has no addressable leaf inside data — a no-op, not an error.
        data = {"a": 1}
        protect(data, {"x-gdpr-sensitive": True}, _cipher())
        self.assertEqual(data, {"a": 1})


class ProtectSkipsTest(unittest.TestCase):
    def test_absent_marked_field_is_skipped(self) -> None:
        data = {"order_id": 7}  # no `email`, `profile`, etc.
        protect(data, SCHEMA, _cipher())
        self.assertEqual(data, {"order_id": 7})

    def test_type_mismatch_is_skipped(self) -> None:
        # Schema says addresses is an array of objects, but the message carries a scalar — skip.
        data = {"addresses": "not-an-array", "profile": "not-an-object"}
        protect(data, SCHEMA, _cipher())
        self.assertEqual(data, {"addresses": "not-an-array", "profile": "not-an-object"})

    def test_no_marks_or_none_schema_is_noop(self) -> None:
        data = _data()
        snapshot = json.dumps(data, sort_keys=True)
        protect(data, {"type": "object", "properties": {"email": {"type": "string"}}}, _cipher())
        protect(data, None, _cipher())
        self.assertEqual(json.dumps(data, sort_keys=True), snapshot)

    def test_empty_data_is_noop(self) -> None:
        data: Dict[str, Any] = {}
        protect(data, SCHEMA, _cipher())
        self.assertEqual(data, {})


class UnprotectEdgeTest(unittest.TestCase):
    def test_non_string_leaf_left_untouched_idempotent(self) -> None:
        # Re-running unprotect is a no-op for NON-STRING leaves: once restored to number/bool/null/
        # object they are no longer ciphertext candidates, so a second pass leaves them as-is.
        schema = {"type": "object", "properties": {k: {"x-gdpr-sensitive": True}
                  for k in ("n", "b", "z", "o")}}
        data: Dict[str, Any] = {"n": 1230, "b": True, "z": None, "o": {"k": 1}}
        protect(data, schema, _cipher())
        unprotect(data, schema, _cipher())  # first restore
        restored = json.dumps(data, sort_keys=True)
        unprotect(data, schema, _cipher())  # second pass is a no-op (non-string leaves left alone)
        unprotect(data, schema, _cipher())  # and again — still stable
        self.assertEqual(json.dumps(data, sort_keys=True), restored)

    def test_rerun_on_restored_string_leaf_raises(self) -> None:
        # A restored STRING leaf IS a ciphertext candidate, so a naive double-unprotect on a string
        # field fails cleanly (it is not the cipher's output) rather than silently corrupting data —
        # callers run unprotect exactly once, matching the documented contract.
        data = {"email": "alice@example.com"}
        protect(data, SCHEMA, _cipher())
        unprotect(data, SCHEMA, _cipher())
        self.assertEqual(data, {"email": "alice@example.com"})
        with self.assertRaises(DecryptError):
            unprotect(data, SCHEMA, _cipher())

    def test_wrong_key_raises_decrypt_error(self) -> None:
        data = _data()
        protect(data, SCHEMA, XorCipher(KEY))
        with self.assertRaises(DecryptError):
            unprotect(data, SCHEMA, XorCipher(b"f" * 32))  # different key → tag check fails

    def test_decrypted_non_json_raises_decrypt_error(self) -> None:
        # A cipher that opens but yields bytes that are not JSON also fails cleanly as DecryptError —
        # the consumer must never feed corrupt-but-decrypted bytes to the handler.
        class GarbageCipher:
            def encrypt(self, plaintext: bytes) -> str:
                return "garbage"

            def decrypt(self, ciphertext: str) -> bytes:
                return b"\xff\xfe not json"

        data = {"email": "ct"}
        with self.assertRaises(DecryptError):
            unprotect(data, SCHEMA, GarbageCipher())

    def test_non_ciphertext_string_raises_decrypt_error(self) -> None:
        # A marked leaf that is a plain (never-protected) string is still a string, so unprotect
        # tries to open it and fails cleanly rather than silently passing garbage through.
        data = {"email": "alice@example.com"}
        with self.assertRaises(DecryptError):
            unprotect(data, SCHEMA, _cipher())


class FrozenEnvelopeTest(unittest.TestCase):
    def test_protected_envelope_still_decodes_and_preserves_frame(self) -> None:
        data = _data()
        protect(data, SCHEMA, _cipher())

        envelope = EnvelopeCodec.make("urn:babel:orders:created", data,
                                      queue="orders", trace_id="trace-xyz")
        body = EnvelopeCodec.encode(envelope)

        # data stays pure JSON (the ciphertext is just a string) — encode/decode round-trips.
        decoded = EnvelopeCodec.decode(body)
        self.assertTrue(EnvelopeCodec.accepts(decoded))
        self.assertEqual(decoded["meta"]["schema_version"], 1)
        self.assertEqual(decoded["trace_id"], "trace-xyz")  # GR-4: trace_id untouched
        self.assertEqual(decoded["job"], "urn:babel:orders:created")

        # The consumer decrypts in place and recovers the original cleartext exactly.
        unprotect(decoded["data"], SCHEMA, _cipher())
        self.assertEqual(decoded["data"], _data())

    def test_ciphertext_is_a_json_string(self) -> None:
        # GR-3: a protected value is a JSON string, so an SDK without the key still carries the frame.
        data = {"email": "alice@example.com"}
        protect(data, SCHEMA, _cipher())
        reparsed = json.loads(json.dumps(data))
        self.assertIsInstance(reparsed["email"], str)


if __name__ == "__main__":
    unittest.main()
