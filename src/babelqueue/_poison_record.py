"""The DLQ record of a poison body (ADR-0035, error-handling.md section 6).

A body that is a strict RFC 8259 JSON object (valid UTF-8, no ``NaN`` / ``Infinity`` literal, no
unpaired surrogate escape, no top-level ``dead_letter`` member) is *spliced* at byte level: every
original character is kept and only a ``dead_letter`` member is appended, so no value is re-encoded
(``1e400`` and a 64-bit integer stay as written, a repeated member name is not collapsed). Anything
else is wrapped as ``{"raw": <string>, "raw_encoding"?: "base64", "dead_letter": ...}``: ``raw`` is
the body as plain text when it is valid UTF-8, otherwise the standard base64 of the exact bytes with
``"raw_encoding": "base64"``. U+FFFD substitution never happens.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any, Dict, List, Optional, Tuple

_JSON_WHITESPACE = " \t\r\n"
_STRING_LITERAL = re.compile(r'"(?:[^"\\]|\\.)*"', re.DOTALL)
_ESCAPE = re.compile(r"\\(u[0-9a-fA-F]{4}|.)", re.DOTALL)


def _reject_constant(name: str) -> Any:
    raise ValueError("invalid JSON constant " + name)


def _has_unpaired_surrogate_escape(text: str) -> bool:
    """True when a string literal of the (already syntax-valid) JSON ``text`` holds a ``\\uD800``-style
    escape that is not half of a high+low pair."""
    for literal in _STRING_LITERAL.finditer(text):
        pending_high = False
        for escape in _ESCAPE.finditer(literal.group(0)):
            token = escape.group(1)
            code = int(token[1:], 16) if len(token) == 5 and token[0] == "u" else -1
            is_high = 0xD800 <= code <= 0xDBFF
            is_low = 0xDC00 <= code <= 0xDFFF
            if pending_high and not is_low:
                return True
            if is_low and not pending_high:
                return True
            pending_high = is_high
        if pending_high:
            return True
    return False


def _is_spliceable_object(text: str) -> bool:
    """Whether ``text`` is one strict JSON object that can be spliced (ADR-0035 section 1)."""
    last_pairs: List[Optional[List[Tuple[str, Any]]]] = [None]

    def hook(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
        last_pairs[0] = pairs  # the outermost object is the last one the decoder completes
        return {}

    try:
        parsed = json.loads(text, parse_constant=_reject_constant, object_pairs_hook=hook)
    except (ValueError, RecursionError):
        return False
    if not isinstance(parsed, dict) or last_pairs[0] is None:
        return False
    if any(key == "dead_letter" for key, _ in last_pairs[0]):
        return False
    return not _has_unpaired_surrogate_escape(text)


def build_poison_record(text: str, dead_letter_block: Dict[str, Any]) -> str:
    """The DLQ record body for the poison body ``text``.

    ``text`` is the body as the transport carries it: bytes that were not UTF-8 appear as
    ``surrogateescape`` code points. ``dead_letter_block`` is the ``dead_letter`` member's value."""
    try:
        data = text.encode("utf-8", "surrogateescape")
    except UnicodeEncodeError:
        data = text.encode("utf-8", "surrogatepass")
    try:
        decoded: Optional[str] = data.decode("utf-8")
    except UnicodeDecodeError:
        decoded = None

    block = json.dumps(dead_letter_block, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if decoded is not None and _is_spliceable_object(decoded):
        trimmed = decoded.strip(_JSON_WHITESPACE)
        if trimmed[1:-1].strip(_JSON_WHITESPACE) == "":
            return '{"dead_letter":' + block + "}"
        return trimmed[:-1] + ',"dead_letter":' + block + "}"

    if decoded is not None:
        raw = json.dumps(decoded, ensure_ascii=False)
        return '{"raw":' + raw + ',"dead_letter":' + block + "}"
    encoded = base64.b64encode(data).decode("ascii")
    return '{"raw":"' + encoded + '","raw_encoding":"base64","dead_letter":' + block + "}"
