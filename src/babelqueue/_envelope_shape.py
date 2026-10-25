"""The poison trigger set of the Kafka transport (§6.5).

A body is undecodable when a strict, typed decode into ``Envelope`` / ``Meta`` / ``DeadLetter`` (the
Go reference's ``babelqueue.Decode``, except that member names are case-sensitive here) rejects it:

* the body must be exactly one JSON value (no BOM, trailing tokens, ``NaN`` / ``Infinity``, malformed
  numbers); a bare ``null`` decodes to an empty envelope, any other non-object root is rejected;
* ``trace_id``, ``meta.id|queue|lang`` and ``dead_letter.reason|error|exception|original_queue|lang``
  are string-or-null;
* ``attempts``, ``meta.schema_version|created_at`` and ``dead_letter.failed_at|attempts`` are
  integer-or-null: an integer literal (no fraction or exponent) that fits ``int64``;
* ``data``, ``meta`` and ``dead_letter`` are object-or-null, and a number inside ``data`` must be a
  finite double;
* member names are matched exactly as spelled (case-sensitive, as the contract schema and the core
  decoder read them) and every occurrence of a duplicated key is checked; any other member (a
  differently-cased name or a non-string ``urn`` alias included) is only checked for syntax.
  A non-string ``job`` is **not** poison either: the identity resolver ignores it (ADR-0032 section 6),
  so the body is readable and simply carries no identity (conformance ``kafka_poison``, v1.6.0, and ``identity_verdict``, v1.7.0).

A well-typed but non-conformant envelope is not poison: Go's consume path never calls ``Accepts``.
"""

import json
import math
from typing import Any, Dict, List, Optional, Tuple

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
#: Beyond this many digits an integer literal cannot fit an int64 or a double; it is kept as a
#: sentinel so ``json`` never trips the interpreter's integer-string-length limit.
_MAX_INT_DIGITS = 4000


class _Pairs(List[Tuple[str, Any]]):
    """A decoded JSON object as its ``(key, value)`` list, so duplicate keys survive."""


class _HugeInt:
    """An integer literal too large for any int64 or double."""


_HUGE = _HugeInt()

_STRING = "string"
_INTEGER = "integer"
_OBJECT = "object"
_DATA = "data"

_META = {
    "id": _STRING,
    "queue": _STRING,
    "lang": _STRING,
    "schema_version": _INTEGER,
    "created_at": _INTEGER,
}
#: The core ``meta`` keys of message-envelope.md section 4 (the single copy; the producer ``meta``
#: write API of the codec rejects a collision with any of them, ADR-0031 (b)).
CORE_META_KEYS = frozenset(_META)
_DEAD_LETTER = {
    "reason": _STRING,
    "error": _STRING,
    "exception": _STRING,
    "original_queue": _STRING,
    "lang": _STRING,
    "failed_at": _INTEGER,
    "attempts": _INTEGER,
}
_TOP = {
    "trace_id": _STRING,
    "attempts": _INTEGER,
    "data": _DATA,
    "meta": _OBJECT,
    "dead_letter": _OBJECT,
}


def _reject_constant(name: str) -> Any:
    raise ValueError("invalid JSON constant " + name)


def _parse_int(literal: str) -> Any:
    return int(literal) if len(literal) <= _MAX_INT_DIGITS else _HUGE


def _is_finite_number(value: Any) -> bool:
    if value is _HUGE:
        return False
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, int) and not isinstance(value, bool):
        try:
            return math.isfinite(float(value))
        except OverflowError:
            return False
    return True


def _all_numbers_finite(root: Any) -> bool:
    stack = [root]
    while stack:
        value = stack.pop()
        if isinstance(value, _Pairs):
            stack.extend(item for _, item in value)
        elif isinstance(value, list):
            stack.extend(value)
        elif not _is_finite_number(value):
            return False
    return True


def _check_member(kind: str, name: str, value: Any) -> Optional[str]:
    if value is None:
        return None
    if kind == _STRING:
        return None if isinstance(value, str) else "member %r is not a string" % name
    if kind == _INTEGER:
        if type(value) is int and _INT64_MIN <= value <= _INT64_MAX:
            return None
        return "member %r is not an int64 integer" % name
    if not isinstance(value, _Pairs):
        return "member %r is not an object" % name
    if kind == _DATA and not _all_numbers_finite(value):
        return "member %r holds a number that is not a finite double" % name
    return None


def _check_object(pairs: _Pairs, kinds: Dict[str, str], prefix: str) -> Optional[str]:
    for key, value in pairs:
        kind = kinds.get(key)
        if kind is None:
            continue
        failure = _check_member(kind, prefix + key, value)
        if failure is not None:
            return failure
        if kind == _OBJECT and value is not None:
            nested = _check_object(value, _META if key == "meta" else _DEAD_LETTER, key + ".")
            if nested is not None:
                return nested
    return None


def envelope_violation(text: str) -> Optional[str]:
    """Why ``text`` is not a decodable BabelQueue body, or ``None`` when Go's typed decode accepts it."""
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_Pairs,
            parse_int=_parse_int,
            parse_constant=_reject_constant,
        )
    except (ValueError, RecursionError) as exc:
        return "invalid JSON: " + str(exc)
    if parsed is None:
        return None  # a bare ``null`` is an empty envelope, not poison
    if not isinstance(parsed, _Pairs):
        return "the JSON root is not an object"
    return _check_object(parsed, _TOP, "")
