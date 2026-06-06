"""The canonical BabelQueue wire envelope — the single Python implementation.

The shape is frozen as ``{job, trace_id, data, meta, attempts}`` (schema_version 1)
so a Python service interoperates byte-for-byte with the PHP/Laravel, Go, ... SDKs.
The ``job`` field carries the message URN (never a class name); ``trace_id`` is a
cross-service correlation id preserved across every hop. Pure stdlib — no deps.

Full spec: https://babelqueue.com
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Mapping, Optional

from .exceptions import BabelQueueError

SCHEMA_VERSION = 1
SOURCE_LANG = "python"


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
    ) -> Dict[str, Any]:
        """Build the canonical envelope for a ``(urn, data)`` pair.

        ``trace_id`` is reused when given (trace continuation), otherwise a fresh
        UUID is minted. ``attempts`` starts at 0 and is the top-level transport
        counter (kept out of the immutable ``meta`` block).
        """
        urn = (urn or "").strip()
        if not urn:
            raise BabelQueueError(
                "A polyglot message must expose a stable, non-empty URN so consumers "
                "can identify it without any class name."
            )

        trace_id = (trace_id or "").strip() or str(uuid.uuid4())

        return {
            "job": urn,
            "trace_id": trace_id,
            "data": dict(data),
            "meta": {
                "id": str(uuid.uuid4()),
                "queue": queue,
                "lang": SOURCE_LANG,
                "schema_version": SCHEMA_VERSION,
                "created_at": int(time.time() * 1000),
            },
            "attempts": 0,
        }

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
        """Encode the envelope as compact UTF-8 JSON (unescaped unicode/slashes)."""
        return json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def decode(raw: str) -> Dict[str, Any]:
        """Decode a raw JSON body; returns ``{}`` for malformed/non-object input."""
        try:
            decoded = json.loads(raw)
        except (ValueError, TypeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def urn(envelope: Mapping[str, Any]) -> str:
        """The message URN: canonical ``job``, with ``urn`` accepted as an alias."""
        return str(envelope.get("job") or envelope.get("urn") or "")

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
        if not isinstance(trace_id, str) or trace_id == "":
            return False

        return True
