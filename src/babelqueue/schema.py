"""Optional per-URN payload schema validation (ADR-0024).

The Python mirror of the Go ``schema`` package and PHP ``BabelQueue\\Schema``. A
:class:`SchemaProvider` supplies a JSON Schema for a message URN — typically read from a
babelqueue-registry ``registry.json`` — and the message's ``data`` is validated against it.
It is opt-in: a URN with no registered schema is never validated.

- **Producer-side (recommended):** call :func:`validate` before publishing so invalid data
  never enters the queue, or :func:`check` to branch without raising::

      from babelqueue.schema import MapProvider, validate

      provider = MapProvider.from_json({"urn:babel:orders:created": ORDERS_SCHEMA_JSON})
      validate(provider, "urn:babel:orders:created", {"order_id": 7})  # raises on mismatch

- **Consumer-side (safety net):** wrap a handler with :func:`wrap`. Because a Python handler
  receives ``data`` (and ``meta``, ``envelope``) positionally rather than a message object,
  the URN is passed explicitly — usually the same URN you register under::

      app.register(URN, wrap(provider, URN, on_order_created))

  Invalid data raises :class:`~babelqueue.exceptions.InvalidPayloadError`, so the runtime
  redelivers (and eventually dead-letters) the poison message; a URN with no schema runs the
  handler unchanged.

The validator is a small subset of JSON Schema (draft-07) whose verdicts match the Go and
PHP validators and babelqueue-registry's ``compat`` linter: ``type``, ``required``,
``properties``, ``additionalProperties``, ``items``, ``enum``, ``const``, ``minLength``,
``minimum``. Unknown keywords are ignored. Zero dependencies (stdlib only).
"""

from __future__ import annotations

import functools
import json
import os
import threading
from typing import Any, Callable, List, Mapping, NamedTuple, Optional, Protocol, runtime_checkable

from .exceptions import InvalidPayloadError

Handler = Callable[..., None]


@runtime_checkable
class SchemaProvider(Protocol):
    """A source of per-URN ``data`` schemas, keyed on the message URN."""

    def schema_for(self, urn: str) -> Optional[Mapping[str, Any]]:
        """The JSON Schema registered for ``urn``, or None when none is registered."""


class MapProvider:
    """In-memory :class:`SchemaProvider`, for tests and for embedding schemas in code."""

    def __init__(self, schemas: Mapping[str, Mapping[str, Any]]) -> None:
        self._schemas: dict[str, Mapping[str, Any]] = dict(schemas)

    @classmethod
    def from_json(cls, raw: Mapping[str, str]) -> "MapProvider":
        """Build a provider from URN -> raw JSON Schema strings, decoding each."""
        schemas: dict[str, Mapping[str, Any]] = {}
        for urn, body in raw.items():
            decoded = json.loads(body)
            if not isinstance(decoded, dict):
                raise ValueError(f"schema: invalid JSON schema for {urn!r}")
            schemas[urn] = decoded
        return cls(schemas)

    def schema_for(self, urn: str) -> Optional[Mapping[str, Any]]:
        return self._schemas.get(urn)


class DirProvider:
    """Reads schemas from a babelqueue-registry manifest (``registry.json``): a list of
    ``{urn, schema}`` entries mapping each URN to a schema file for its ``data`` block. The
    bridge that makes the registry's governed schemas enforceable at runtime. Schema files
    are read and decoded lazily and cached (thread-safe). A URN absent from the manifest
    returns None (skip); a URN whose schema file is missing raises (config/IO error)."""

    def __init__(self, manifest_path: str) -> None:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
        if not isinstance(manifest, dict):
            raise ValueError(f"schema: invalid registry manifest {manifest_path!r}")

        self._dir = os.path.dirname(manifest_path)
        self._files: dict[str, str] = {}
        self._cache: dict[str, Mapping[str, Any]] = {}
        self._lock = threading.Lock()

        entries = manifest.get("schemas") or []
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                urn = entry.get("urn")
                file = entry.get("schema")
                if isinstance(urn, str) and urn and isinstance(file, str) and file:
                    self._files[urn] = file

    def schema_for(self, urn: str) -> Optional[Mapping[str, Any]]:
        with self._lock:
            cached = self._cache.get(urn)
            if cached is not None:
                return cached
            file = self._files.get(urn)
            if file is None:
                return None
            path = file if os.path.isabs(file) else os.path.join(self._dir, file)
            with open(path, "r", encoding="utf-8") as fh:
                decoded = json.load(fh)
            if not isinstance(decoded, dict):
                raise ValueError(f"schema: invalid schema for {urn!r} ({file})")
            self._cache[urn] = decoded
            return decoded


def check(provider: SchemaProvider, urn: str, data: Mapping[str, Any]) -> Optional[str]:
    """The first ``data`` violation for ``(urn, data)``, or None when it is valid or when no
    schema is registered for the URN (opt-in). Non-raising; for producer-side branching."""
    schema = provider.schema_for(urn)
    if schema is None:
        return None
    return validate_schema(schema, dict(data))


def validate(provider: SchemaProvider, urn: str, data: Mapping[str, Any]) -> None:
    """Validate ``(urn, data)`` against its registered schema, raising otherwise. The
    producer-side guard; call it before publishing.

    :raises InvalidPayloadError: when the data does not match the URN's schema.
    """
    violation = check(provider, urn, data)
    if violation is not None:
        raise InvalidPayloadError(urn, violation)


def wrap(provider: SchemaProvider, urn: str, handler: Handler) -> Handler:
    """Wrap a consume handler so each message's ``data`` is validated against ``urn``'s
    schema before the handler runs. The returned callable keeps ``handler``'s signature (via
    :func:`functools.wraps`), so the runtime still passes it the right number of positional
    args (``data, meta`` or ``data, meta, envelope``)."""

    @functools.wraps(handler)
    def wrapped(*args: Any) -> None:
        data = args[0] if args and isinstance(args[0], Mapping) else {}
        validate(provider, urn, data)
        handler(*args)

    return wrapped


def validate_schema(schema: Mapping[str, Any], value: Any, path: str = "") -> Optional[str]:
    """The first violation of ``value`` against a (subset) JSON Schema node, or None."""
    if "const" in schema and not _equal(value, schema["const"]):
        return _violation(path, "wrong_const")

    enum = schema.get("enum")
    if isinstance(enum, list) and not any(_equal(value, item) for item in enum):
        return _violation(path, "not_in_enum")

    typ = schema.get("type")
    if typ == "object":
        return _check_object(schema, value, path)
    if typ == "array":
        return _check_array(schema, value, path)
    if typ == "string":
        if not isinstance(value, str):
            return _violation(path, "not_a_string")
        min_len = schema.get("minLength")
        if isinstance(min_len, (int, float)) and len(value) < int(min_len):
            return _violation(path, "below_min_length")
        return None
    if typ == "integer":
        if not _is_integer(value):
            return _violation(path, "not_an_integer")
        return _check_minimum(schema, value, path)
    if typ == "number":
        if not _is_number(value):
            return _violation(path, "not_a_number")
        return _check_minimum(schema, value, path)
    if typ == "boolean":
        return None if isinstance(value, bool) else _violation(path, "not_a_boolean")
    if typ == "null":
        return None if value is None else _violation(path, "not_null")
    return None


def _check_object(schema: Mapping[str, Any], value: Any, path: str) -> Optional[str]:
    if not isinstance(value, Mapping):
        return _violation(path, "not_an_object")

    required = schema.get("required")
    if isinstance(required, list):
        for key in required:
            if isinstance(key, str) and key not in value:
                return _violation(_join(path, key), "missing_required")

    properties = schema.get("properties")
    properties = properties if isinstance(properties, Mapping) else {}
    additional_allowed = schema.get("additionalProperties") is not False

    for key, item in value.items():
        name = str(key)
        prop = properties.get(name)
        if isinstance(prop, Mapping):
            violation = validate_schema(prop, item, _join(path, name))
            if violation is not None:
                return violation
            continue
        if not additional_allowed:
            return _violation(_join(path, name), "additional_not_allowed")

    return None


def _check_array(schema: Mapping[str, Any], value: Any, path: str) -> Optional[str]:
    if not isinstance(value, list):
        return _violation(path, "not_an_array")
    items = schema.get("items")
    if not isinstance(items, Mapping):
        return None
    for i, item in enumerate(value):
        violation = validate_schema(items, item, f"{path}[{i}]")
        if violation is not None:
            return violation
    return None


def _check_minimum(schema: Mapping[str, Any], value: Any, path: str) -> Optional[str]:
    minimum = schema.get("minimum")
    if isinstance(minimum, (int, float)) and not isinstance(minimum, bool) and float(value) < float(minimum):
        return _violation(path, "below_minimum")
    return None


def _is_integer(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and value.is_integer()


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _equal(a: Any, b: Any) -> bool:
    # Type-aware equality so True != 1 and an integer const never matches a float value,
    # matching the strict comparisons in the Go and PHP validators.
    return type(a) is type(b) and a == b


def _violation(path: str, reason: str) -> str:
    return f"{path or '<root>'}: {reason}"


def _join(path: str, key: str) -> str:
    return key if path == "" else f"{path}.{key}"


class SensitivePath(NamedTuple):
    """One property a schema marked ``x-gdpr-sensitive`` (ADR-0030), located by its dotted path
    from the schema root. Array elements use the ``field[]`` segment the validator and registry
    ``compat`` linter use (e.g. ``addresses[].line``). ``category`` is the optional
    ``"x-gdpr-sensitive": "<category>"`` string, or ``""`` when the keyword was the boolean
    ``true``. A mark on the root schema itself is reported with ``path == ""``."""

    path: str
    category: str


def _gdpr_mark(schema: Mapping[str, Any]) -> Optional[str]:
    """Read the ``x-gdpr-sensitive`` extension keyword (ADR-0030) from one schema node.

    Returns the category string for a marked node (``""`` when the keyword was the boolean
    ``true``, a non-empty category when it was a non-empty string), or ``None`` when the node is
    not marked. The keyword is **validation-neutral**: it never makes a value valid or invalid, so
    annotating a schema is never a breaking change (GR-1). Any other shape — ``false``, ``""``, a
    number — leaves the node unmarked. Mirrors the Go ``fromMap`` and the registry's parser.
    """
    mark = schema.get("x-gdpr-sensitive")
    if mark is True:
        return ""
    if isinstance(mark, str) and mark != "":
        return mark
    return None


def sensitive_paths(schema: Mapping[str, Any]) -> List[SensitivePath]:
    """Every property a schema marked ``x-gdpr-sensitive``, in sorted path order (ADR-0030).

    Descends nested objects (dotted paths like ``profile.full_name``) and array item schemas
    (``addresses[].line``); a mark on the root schema itself is reported with ``path == ""``. It is
    the value-level counterpart to the registry's inventory: :mod:`babelqueue.gdpr` uses these paths
    to locate the leaves it encrypts on produce and decrypts on consume. The Python mirror of the
    Go ``Schema.SensitivePaths``. Non-mapping / malformed nodes are skipped, never raised on.
    """
    out: List[SensitivePath] = []
    _collect_sensitive(schema, "", out)
    out.sort(key=lambda sp: sp.path)
    return out


def _collect_sensitive(schema: Any, path: str, out: List[SensitivePath]) -> None:
    if not isinstance(schema, Mapping):
        return
    category = _gdpr_mark(schema)
    if category is not None:
        out.append(SensitivePath(path, category))
    properties = schema.get("properties")
    if isinstance(properties, Mapping):
        for name, sub in properties.items():
            _collect_sensitive(sub, _join(path, str(name)), out)
    items = schema.get("items")
    if isinstance(items, Mapping):
        _collect_sensitive(items, path + "[]", out)
