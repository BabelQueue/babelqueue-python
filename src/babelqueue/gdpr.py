"""Optional runtime GDPR field encryption (ADR-0030): the SDK-enforcement half.

The Python mirror of the Go ``gdpr`` package — the reference the SDKs share. The
babelqueue-registry only **declares** and **audits** which ``data`` fields are sensitive (the
``x-gdpr-sensitive`` schema keyword) and can **mask** them for safe logging; this module
**enforces** that on the wire — a producer encrypts each marked leaf before publish, a consumer
decrypts it after decode. It is strictly **opt-in**: a producer/consumer that never calls
:func:`protect`/:func:`unprotect` behaves exactly as before.

The contract is deliberately tight so every SDK stays byte-compatible:

- **The envelope stays frozen (GR-1).** :func:`protect` mutates only **values inside ``data``**: a
  sensitive leaf's value becomes a ciphertext **string**. It never adds, renames, removes or
  retypes an envelope field; ``meta.schema_version`` stays ``1``; ``trace_id`` is untouched (GR-4).
  ``data`` stays **pure JSON** (GR-3) — a JSON string is still pure JSON, so any SDK can carry the
  envelope even without the key (it just can't read the protected fields).
- **Zero heavy dependencies (GR-7).** Python's standard library has **no AES-GCM**, so the core
  ships **no** concrete cipher and pulls **no** crypto dependency. :class:`Cipher` is a
  caller-provided :class:`typing.Protocol` — bind it to a KMS, Vault transit, an HSM, a
  tokenisation service, or a local AES-GCM built on the optional ``cryptography`` library (see the
  bring-your-own-cipher example below). The crypto dependency lives in the *caller's* code, never
  in this core.

The sensitive paths come from the **same per-URN schema** the produce/consume validation path
already loads (:func:`babelqueue.schema.sensitive_paths`) — the ``x-gdpr-sensitive`` marks ride on
it. Validate **cleartext**: run :func:`babelqueue.schema.validate` **before** :func:`protect` on
the producer and **after** :func:`unprotect` on the consumer, because a schema that constrains a
sensitive field (``minLength``, ``enum``, …) would otherwise reject the ciphertext string.

Typical wiring (producer)::

    from babelqueue import EnvelopeCodec
    from babelqueue.gdpr import protect

    data = {"order_id": 1042, "email": "alice@example.com"}
    # validate(provider, urn, data)          # optional: validate cleartext first
    protect(data, schema, cipher)            # encrypt marked leaves IN PLACE
    body = EnvelopeCodec.encode(EnvelopeCodec.make(urn, data))   # ciphertext rides inside data

and the inverse on the consumer, after decode and before the handler reads ``data``::

    from babelqueue.gdpr import unprotect

    envelope = EnvelopeCodec.decode(body)
    unprotect(envelope["data"], schema, cipher)   # decrypt marked leaves IN PLACE
    # validate(provider, urn, envelope["data"])   # optional: validate cleartext after

Bring-your-own AES-256-GCM cipher (NOT part of this core — ``pip install cryptography``)::

    import base64, os
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    class AesGcmCipher:
        '''A reference Cipher: AES-256-GCM, random 12-byte nonce prepended, base64 out.
        The 32-byte key is the CALLER's; this does no key management or rotation.'''

        def __init__(self, key: bytes) -> None:
            self._aead = AESGCM(key)          # key must be 16, 24, or 32 bytes

        def encrypt(self, plaintext: bytes) -> str:
            nonce = os.urandom(12)
            sealed = self._aead.encrypt(nonce, plaintext, None)
            return base64.b64encode(nonce + sealed).decode("ascii")

        def decrypt(self, ciphertext: str) -> bytes:
            raw = base64.b64decode(ciphertext)
            nonce, sealed = raw[:12], raw[12:]
            return self._aead.decrypt(nonce, sealed, None)   # raises on wrong key/tamper

The same plaintext MAY encrypt to a different string each call (the random nonce is expected and
good); :meth:`Cipher.decrypt` restores the exact bytes :meth:`Cipher.encrypt` was given, and a
wrong-key / tampered input MUST raise (GCM authentication) — :func:`unprotect` turns any such raise
into :class:`~babelqueue.exceptions.DecryptError`.
"""

from __future__ import annotations

import json
from typing import Any, Callable, List, MutableMapping, MutableSequence, Optional, Protocol, Tuple, runtime_checkable

from .exceptions import DecryptError
from .schema import sensitive_paths

__all__ = ["Cipher", "protect", "unprotect"]


@runtime_checkable
class Cipher(Protocol):
    """The field-level protection primitive the **caller** provides — a seam onto a KMS, Vault
    transit, an HSM, a tokenisation service, or a local AES-GCM (see the module docstring). Keeping
    this a Protocol is what holds GR-7: this core never imports a crypto library; only the caller's
    concrete cipher does.

    Contract for an implementation:

    - :meth:`encrypt` takes the canonical JSON bytes of one field value (see :func:`protect`) and
      returns the ciphertext as a **str** that is valid for placement inside a JSON document (e.g.
      base64). The same plaintext MAY encrypt to a different string each call (a random nonce/IV is
      expected and good).
    - :meth:`decrypt` is the exact inverse: given a string :meth:`encrypt` produced it returns the
      original JSON bytes byte-for-byte. A string it did not produce, or one produced under a
      different key, MUST **raise** rather than return silent garbage, so a wrong-key consume fails
      loudly (:func:`unprotect` wraps the raise in :class:`~babelqueue.exceptions.DecryptError`).
    - Both SHOULD be safe for concurrent use; a producer/consumer may fan one cipher across threads.
    """

    def encrypt(self, plaintext: bytes) -> str:
        """Protect one field value (its canonical JSON bytes) → a JSON-safe ciphertext string."""
        ...

    def decrypt(self, ciphertext: str) -> bytes:
        """Reverse :meth:`encrypt`, returning the original field-value JSON bytes."""
        ...


def protect(data: MutableMapping[str, Any], schema: Any, cipher: Cipher) -> None:
    """Encrypt, in place, every value in ``data`` at a path the schema marked ``x-gdpr-sensitive``.

    The producer-side step — run it after building ``data`` and before encode/publish. Each marked
    leaf's value is canonically JSON-encoded and replaced by ``cipher.encrypt``'s ciphertext
    **string**; the envelope frame, non-sensitive fields, and everything else are untouched (GR-1).

    A marked path absent from ``data`` is skipped (not an error) — schemas evolve and a message need
    not carry every optional field. ``data`` may be empty (no-op). A ``None`` schema or one with no
    marks is a no-op (nothing is sensitive). A container mark (a whole object/array marked
    sensitive) is supported: the entire sub-value is encoded and encrypted as one ciphertext string.

    On any cipher error this propagates and leaves ``data`` partially protected; treat a raised
    error as fatal for that message (do not publish it).
    """
    _walk(data, schema, cipher, _encrypt_leaf)


def unprotect(data: MutableMapping[str, Any], schema: Any, cipher: Cipher) -> None:
    """Decrypt, in place, every value in ``data`` at an ``x-gdpr-sensitive`` path — the consumer-side
    inverse of :func:`protect`. Run it after decode and before the handler reads ``data``.

    An absent path is skipped. A leaf that is **not a string** — it was never protected, or this is
    a re-run after a successful :func:`unprotect` — is left as-is, so re-invoking on already-cleartext
    data is safe (idempotent for non-string leaves). A string the cipher cannot open (wrong key,
    tampered, or not a ciphertext) raises :class:`~babelqueue.exceptions.DecryptError` — the consumer
    should fail the message (retry / dead-letter) rather than process unreadable PII.
    """
    _walk(data, schema, cipher, _decrypt_leaf)


# A leaf op transforms one sensitive leaf value (encrypt or decrypt). It returns ``(new_value, ok)``:
# ``ok=False`` means the value should be left in place untouched.
_LeafOp = Callable[[Any, Cipher], Tuple[Any, bool]]


def _walk(data: Any, schema: Any, cipher: Optional[Cipher], op: _LeafOp) -> None:
    """Drive ``op`` over every ``x-gdpr-sensitive`` path the schema declares, resolving each path
    against ``data`` itself (NOT by re-walking the schema over the value), so the operation touches
    exactly the declared leaves — non-sensitive siblings are never read or copied."""
    if data is None or schema is None or cipher is None:
        return
    for sp in sensitive_paths(schema):
        _apply_at_path(data, _parse_path(sp.path), cipher, op)


# One step of a sensitive path: ``(key, is_array)``. ``"addresses[].line"`` parses to
# ``[("addresses", True), ("line", False)]``; the ``[]`` marker means array-descent into every
# element before the next segment.
_Segment = Tuple[str, bool]


def _parse_path(path: str) -> List[_Segment]:
    """Split a sensitive path (``"email"``, ``"profile.full_name"``, ``"addresses[].line"``) into
    segments. A trailing ``[]`` on a part binds to it as array-descent. A root mark (path ``""``)
    yields no segments — there is no addressable leaf for it inside ``data``, so it is skipped."""
    if path == "":
        return []
    segments: List[_Segment] = []
    for part in path.split("."):
        if len(part) >= 2 and part.endswith("[]"):
            segments.append((part[:-2], True))
        else:
            segments.append((part, False))
    return segments


def _apply_at_path(node: Any, segments: List[_Segment], cipher: Cipher, op: _LeafOp) -> None:
    """Resolve ``segments`` against ``node`` and run ``op`` on the leaf(s). It descends objects by
    key and, when a segment is an array, fans out over every element. An absent key or a type
    mismatch (a path that does not exist in this particular message) is skipped silently — schemas
    describe the union of possible shapes; a given message need not contain every field."""
    if not segments:
        return  # root mark or exhausted path with no leaf key — nothing addressable in data
    key, is_array = segments[0]
    if not isinstance(node, MutableMapping):
        return  # expected an object here but the message has something else — skip
    if key not in node:
        return  # absent field — skip (not an error)

    child = node[key]
    last = len(segments) == 1

    if is_array:
        if not isinstance(child, MutableSequence):
            return  # declared array but message has a non-array — skip
        for i, elem in enumerate(child):
            if last:
                new_value, ok = op(elem, cipher)
                if ok:
                    child[i] = new_value
            else:
                _apply_at_path(elem, segments[1:], cipher, op)
        return

    if last:
        new_value, ok = op(child, cipher)
        if ok:
            node[key] = new_value
        return
    _apply_at_path(child, segments[1:], cipher, op)


def _encrypt_leaf(value: Any, cipher: Cipher) -> Tuple[Any, bool]:
    """Canonically JSON-encode one field value and replace it with the cipher's ciphertext string.
    The JSON encoding is what makes the round-trip exact: :func:`unprotect`'s ``json.loads`` restores
    the same decoded-JSON value (``float`` for numbers, ``dict`` for objects, …) the codec would
    have produced, so protect → unprotect is byte-for-byte. ``sort_keys`` + compact separators give
    one canonical encoding, mirroring the Go reference's ``json.Marshal``."""
    plaintext = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return cipher.encrypt(plaintext), True


def _decrypt_leaf(value: Any, cipher: Cipher) -> Tuple[Any, bool]:
    """Reverse :func:`_encrypt_leaf`. A non-string leaf is left untouched (``ok=False``) so
    :func:`unprotect` is safe to re-run on already-cleartext data; a string that fails to open or to
    JSON-decode raises :class:`~babelqueue.exceptions.DecryptError` so the consumer fails the message
    rather than handle unreadable PII."""
    if not isinstance(value, str):
        # Not a ciphertext string (already cleartext, or never protected) — leave as-is.
        return None, False
    try:
        plaintext = cipher.decrypt(value)
    except Exception as exc:  # noqa: BLE001 — any cipher failure is a decrypt failure for the caller
        raise DecryptError(f"cannot decrypt a protected field: {exc}") from exc
    try:
        restored = json.loads(plaintext)
    except (ValueError, TypeError) as exc:
        raise DecryptError(f"decrypted plaintext is not JSON: {exc}") from exc
    return restored, True
