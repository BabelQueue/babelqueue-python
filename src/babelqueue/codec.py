"""The canonical BabelQueue wire envelope — the single Python implementation.

The shape is frozen as ``{job, trace_id, data, meta, attempts}`` (schema_version 1)
so a Python service interoperates byte-for-byte with the PHP/Laravel, Go, ... SDKs.
The ``job`` field carries the message URN (never a class name); ``trace_id`` is a
cross-service correlation id preserved across every hop. Pure stdlib — no deps.

Full spec: https://babelqueue.com
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._envelope_shape import CORE_META_KEYS
from .exceptions import BabelQueueError, EnvelopeKeyError

SCHEMA_VERSION = 1
SOURCE_LANG = "python"

#: Non-canonical keys (message-envelope.md §10), as ``(container, key)`` pairs where the
#: container is ``None`` for the top level or ``"meta"``. Policy K-15: decode WARNS and drops
#: them (old producers keep working); encode NEVER emits them and REJECTS an envelope that
#: carries one (ADR-0031 (c)). The top-level ``urn`` alias is not in this table: it is an inbound
#: alias that decode folds into ``job`` (see :meth:`EnvelopeCodec.decode`), but encode rejects it.
FORBIDDEN_KEYS: Tuple[Tuple[Optional[str], str], ...] = (
    (None, "timestamp"),
    ("meta", "max_retries"),
    ("meta", "attempts"),
    ("meta", "source"),
    ("meta", "ts"),
)

#: The closed ASCII whitespace set of ADR-0032 §5 (space, \t, \n, \v, \f, \r) — the single source for
#: every trim / blank check in the SDK (cross-SDK parity, GR-5): never a bare ``str.strip()``,
#: which also strips Unicode spaces such as U+00A0, U+2003 and U+3000. The transport modules
#: import it instead of keeping their own copy.
ASCII_WHITESPACE = " \t\n\v\f\r"

#: Logger every forbidden-key warning is reported through (one record per dropped key).
logger = logging.getLogger("babelqueue.codec")


def strip_forbidden_keys(envelope: Dict[str, Any]) -> List[str]:
    """Remove the non-canonical keys of message-envelope.md §10 from ``envelope`` in place.

    Returns the RFC 6901 pointers of the keys that were dropped (e.g. ``"/meta/max_retries"``),
    in a stable order, and logs one ``WARNING`` on :data:`logger` per dropped key naming its
    pointer. Unknown keys that are not forbidden are left untouched (forward compatibility).
    """
    dropped: List[str] = []
    for container, key in FORBIDDEN_KEYS:
        target = envelope if container is None else envelope.get(container)
        if not isinstance(target, dict) or key not in target:
            continue
        del target[key]
        pointer = "/" + key if container is None else f"/{container}/{key}"
        dropped.append(pointer)
        _warn_dropped(pointer)
    return dropped


def parse_envelope(raw: Any) -> Dict[str, Any]:
    """Parse a raw JSON body into a dict **as-is** — no forbidden-key drop, no warning.

    For read-only projections of a body that is forwarded unchanged (e.g. a transport
    deriving broker attributes from it): logging a "dropped" key there would be false,
    because the bytes on the wire keep it. Returns ``{}`` for malformed/non-object input.
    Consumers that act on the envelope use :meth:`EnvelopeCodec.decode` instead.
    """
    try:
        decoded = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def has_forbidden_keys(envelope: Mapping[str, Any]) -> bool:
    """Whether ``envelope`` carries any of the non-canonical keys of message-envelope.md §10.

    The top-level ``urn`` alias is NOT included (it is not a §10 key: decode folds it into ``job``),
    although ``encode`` also rejects it — a caller deciding whether a raw body needs normalising
    before re-emit must check ``"urn" in envelope`` as well.
    """
    return _has_forbidden_keys(envelope)


def _is_valid_urn(value: Any) -> bool:
    """A usable identity: a string that is not blank under the ASCII set (ADR-0032 §5/§6).

    Any non-string — including the falsy ``0`` / ``False`` / ``[]`` / ``{}`` — is not a URN.
    """
    return isinstance(value, str) and value.strip(ASCII_WHITESPACE) != ""


def _parse_object(raw: Any) -> Optional[Dict[str, Any]]:
    """The parsed JSON object, or ``None`` when ``raw`` is malformed or not an object."""
    try:
        decoded = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _fold_identity(decoded: Dict[str, Any]) -> Dict[str, Any]:
    """Drop forbidden keys and fold the ``urn`` alias into ``job`` (ADR-0031 (c), ADR-0032 §6)."""
    strip_forbidden_keys(decoded)
    if "job" in decoded and not isinstance(decoded["job"], str):
        logger.warning(
            "Ignored the non-string envelope key /job (%s)%s (ADR-0032 §6).",
            type(decoded["job"]).__name__,
            "; falling back to urn" if _is_valid_urn(decoded.get("urn")) else "",
        )
    if "urn" in decoded:
        alias = decoded.pop("urn")
        if _is_valid_urn(decoded.get("job")):
            _warn_dropped("/urn")
        elif _is_valid_urn(alias):
            decoded["job"] = alias
    return decoded


def _warn_dropped(pointer: str) -> None:
    logger.warning(
        "Dropped forbidden envelope key %s from the envelope "
        "(non-canonical, message-envelope.md §10).",
        pointer,
    )


def _first_encode_violation(envelope: Mapping[str, Any]) -> Optional[str]:
    """RFC 6901 pointer of the first key encode refuses to write, or ``None``."""
    for container, key in FORBIDDEN_KEYS:
        target = envelope if container is None else envelope.get(container)
        if isinstance(target, Mapping) and key in target:
            return "/" + key if container is None else f"/{container}/{key}"
    if "urn" in envelope:
        return "/urn"
    return None


def _has_forbidden_keys(envelope: Mapping[str, Any]) -> bool:
    for container, key in FORBIDDEN_KEYS:
        target = envelope if container is None else envelope.get(container)
        if isinstance(target, Mapping) and key in target:
            return True
    return False


class EnvelopeCodec:
    """Builds, encodes and decodes the canonical envelope."""

    SCHEMA_VERSION = SCHEMA_VERSION
    SOURCE_LANG = SOURCE_LANG

    @staticmethod
    def make(
        urn: str,
        data: Mapping[str, Any],
        *,
        queue: str = "default",
        trace_id: Optional[str] = None,
        meta: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build the canonical envelope for a ``(urn, data)`` pair.

        ``trace_id`` is reused when given (trace continuation), otherwise a fresh
        UUID is minted. ``attempts`` starts at 0 and is the top-level transport
        counter (kept out of the immutable ``meta`` block).

        ``meta`` (ADR-0031 (b)) adds producer-supplied entries (e.g. ``{"ext": {"acme": 1}}``) to
        the ``meta`` block next to the core keys. An entry whose key is a core ``meta`` key
        (``id``, ``queue``, ``lang``, ``schema_version``, ``created_at``) or a forbidden ``meta``
        key (message-envelope.md §10) raises :class:`~babelqueue.exceptions.EnvelopeKeyError`.
        """
        urn = (urn or "").strip(ASCII_WHITESPACE)
        if not urn:
            raise BabelQueueError(
                "A polyglot message must expose a stable, non-empty URN so consumers "
                "can identify it without any class name."
            )

        trace_id = (trace_id or "").strip(ASCII_WHITESPACE) or str(uuid.uuid4())

        extra: Dict[str, Any] = {}
        if meta is not None:
            if not isinstance(meta, Mapping):
                raise BabelQueueError("The producer meta must be a mapping of string keys to values.")
            forbidden_meta = {key for container, key in FORBIDDEN_KEYS if container == "meta"}
            for key, value in meta.items():
                if key in CORE_META_KEYS or key in forbidden_meta:
                    raise EnvelopeKeyError(
                        f"/meta/{key}",
                        f"The producer meta key /meta/{key} is reserved or forbidden "
                        "(core meta keys and message-envelope.md §10 keys cannot be set).",
                    )
                extra[key] = value

        meta_block: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "queue": queue,
            "lang": SOURCE_LANG,
            "schema_version": SCHEMA_VERSION,
            "created_at": int(time.time() * 1000),
        }
        meta_block.update(extra)
        envelope: Dict[str, Any] = {
            "job": urn,
            "trace_id": trace_id,
            "data": dict(data),
            "meta": meta_block,
            "attempts": 0,
        }
        return envelope

    @staticmethod
    def from_message(message: Any, queue: str = "default") -> Dict[str, Any]:
        """Build the envelope from a message object (see :class:`PolyglotMessage`).

        If the message also exposes ``get_babel_trace_id()`` (see
        :class:`HasTraceId`) and returns a non-empty value, that trace id is reused.
        """
        get_trace = getattr(message, "get_babel_trace_id", None)
        trace_id = get_trace() if callable(get_trace) else None

        return EnvelopeCodec.make(
            message.get_babel_urn(),
            message.to_payload(),
            queue=queue,
            trace_id=trace_id,
        )

    @staticmethod
    def encode(envelope: Mapping[str, Any]) -> str:
        """Encode the envelope as compact UTF-8 JSON (unescaped unicode/slashes).

        Forbidden keys (message-envelope.md §10) are never emitted: an envelope that carries one
        (top-level ``timestamp``; ``meta.max_retries`` / ``meta.attempts`` / ``meta.source`` /
        ``meta.ts``) or the inbound-only top-level ``urn`` alias raises
        :class:`~babelqueue.exceptions.EnvelopeKeyError` (ADR-0031 (c)) and nothing is written.
        Envelopes produced by :meth:`decode` are always safe to re-encode: decode already drops
        those keys. Unknown, non-forbidden keys are kept as-is.
        """
        pointer = _first_encode_violation(envelope)
        if pointer is not None:
            raise EnvelopeKeyError(
                pointer,
                f"Refusing to encode the forbidden envelope key {pointer} "
                "(non-canonical, message-envelope.md §10).",
            )
        return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def decode(raw: str) -> Dict[str, Any]:
        """Decode a raw JSON body; returns ``{}`` for malformed/non-object input.

        Unknown keys are kept as-is (forward compatibility). Forbidden keys
        (message-envelope.md §10) are dropped and reported as warnings on :data:`logger`
        (policy K-15: warn, never reject), so re-encoding never re-emits them.

        The ``urn`` alias is folded into ``job`` (ADR-0031 (c), ADR-0032 §6): a ``job`` that is
        not a string (``null``, number, boolean, object, array — falsy or not) is ignored with a
        warning; a ``job`` that is missing, empty or only ASCII whitespace is ignored silently.
        In both cases a valid string ``urn`` becomes ``job``. When ``job`` is valid, it wins and
        ``urn`` is dropped with a warning naming ``/urn``. A ``urn`` that is not a valid string is
        ignored (dropped). The decoded envelope never keeps a ``urn`` key.

        A readable JSON object with neither a valid ``job`` nor a valid ``urn`` raises
        :class:`~babelqueue.exceptions.BabelQueueError` (ADR-0032 §6). Malformed or non-object
        input still yields ``{}`` (the poison-path convention). Runtime and transport paths that
        must tolerate an identity-less document use :meth:`decode_lenient` instead.
        """
        parsed = _parse_object(raw)
        if parsed is None:
            return {}
        decoded = _fold_identity(parsed)
        if not _is_valid_urn(decoded.get("job")):
            raise BabelQueueError(
                "The envelope has neither a valid string 'job' nor a valid string 'urn' "
                "(ADR-0032 section 6)."
            )
        return decoded

    @staticmethod
    def decode_lenient(raw: str) -> Dict[str, Any]:
        """Like :meth:`decode`, but an envelope without any valid identity does not raise.

        The forbidden-key drop, the ``urn`` fold and the non-string ``job`` warning are identical;
        a document with neither a valid ``job`` nor a valid ``urn`` comes back without a usable
        ``job`` (:meth:`urn` is ``""``, :meth:`accepts` is ``False``). For the runtime dispatch,
        the transports' attempts reconciliation and ``redrive``, which must keep routing such a
        body down the poison / pass-through path instead of failing the delivery. Malformed or
        non-object input yields ``{}``.
        """
        parsed = _parse_object(raw)
        return {} if parsed is None else _fold_identity(parsed)

    @staticmethod
    def urn(envelope: Mapping[str, Any]) -> str:
        """The message URN: canonical ``job``, with ``urn`` accepted as an alias.

        A ``job`` that is not a valid string (missing, non-string, empty or only ASCII
        whitespace) falls back to ``urn``; a ``urn`` that is not a valid string is ignored.
        """
        job = envelope.get("job")
        if _is_valid_urn(job):
            return str(job)
        alias = envelope.get("urn")
        return str(alias) if _is_valid_urn(alias) else ""

    @staticmethod
    def accepts(envelope: Mapping[str, Any]) -> bool:
        """Whether a consumer should accept this envelope (consumer-side validation).

        Rejects messages with no URN, an unsupported ``meta.schema_version``, a
        missing/blank ``trace_id``, or a non-object ``data`` / non-integer
        ``attempts``. (Accepts the ``urn`` alias, unlike the producer JSON Schema.)
        """
        if EnvelopeCodec.urn(envelope) == "":
            return False

        meta = envelope.get("meta")
        if not isinstance(meta, dict) or meta.get("schema_version") != SCHEMA_VERSION:
            return False

        if not isinstance(envelope.get("data"), dict):
            return False

        attempts = envelope.get("attempts")
        if not isinstance(attempts, int) or isinstance(attempts, bool):
            return False

        trace_id = envelope.get("trace_id")
        if not isinstance(trace_id, str) or trace_id.strip(ASCII_WHITESPACE) == "":
            return False

        return True
