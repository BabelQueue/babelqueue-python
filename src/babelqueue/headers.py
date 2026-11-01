"""Consume-side out-of-band transport headers (the Python mirror of Go's ``headers.go``).

The runtime surfaces a delivered message's :attr:`~babelqueue.transport.ReceivedMessage.headers`
onto a :class:`contextvars.ContextVar` for the span of one dispatch, so a handler — or an
optional wrapper such as the ``otel`` module — can read per-message metadata that travels
**beside** the frozen envelope (GR-1), never in it. It is the consume-side counterpart of
:class:`~babelqueue.transport.HeaderPublisher`.

This is the same out-of-band seam the replay-bypass marker rides (ADR-0027); ADR-0028's W3C
``traceparent`` (for cross-hop span parent-child linkage) is the second rider on it. The header
map is read-only — treat it as immutable. Adds only :mod:`contextvars` + a plain ``dict`` (no
dependency), exactly like :mod:`babelqueue.replay`.
"""

from __future__ import annotations

import contextlib
import contextvars
from typing import Any, Dict, Iterator, Mapping, Optional

#: The delivered message's out-of-band transport headers, for the span of one dispatch.
#: Defaults to an empty mapping so :func:`headers_from_context` is always nil-safe.
_headers_var: contextvars.ContextVar[Mapping[str, str]] = contextvars.ContextVar(
    "babelqueue_headers", default={}
)


def headers_from_context() -> Mapping[str, str]:
    """Return the out-of-band transport headers that arrived with the message currently being
    handled, or an empty mapping when none were carried (or the transport surfaces none).

    The returned mapping is read-only — do not mutate it. It is the consume-side counterpart of
    :class:`~babelqueue.transport.HeaderPublisher`: a handler or an optional wrapper (e.g. the
    ``otel`` module's :func:`~babelqueue.otel.wrap_handler`) reads per-message metadata that
    travels beside the frozen envelope, never in it (GR-1).
    """
    return _headers_var.get()


@contextlib.contextmanager
def _headers_scope(headers: Optional[Mapping[str, str]]) -> Iterator[None]:
    """Internal: surface ``headers`` on the context for the span of one dispatch, then reset.

    A nil/empty map is fine; reads stay nil-safe. The runtime calls this in
    :meth:`~babelqueue.app.BabelQueue.dispatch` so wrappers can read the delivered headers.
    """
    scoped: Mapping[str, str] = headers or {}
    token = _headers_var.set(scoped)
    try:
        yield
    finally:
        _headers_var.reset(token)


def merge_headers(*sources: Optional[Mapping[str, str]]) -> Dict[str, str]:
    """Combine header maps into a single ``dict[str, str]``, dropping blank keys and blank values.

    Later sources win a key collision. Returns a fresh dict (callers may mutate it freely). Used
    to merge an injected ``traceparent`` onto a transport's contract headers without clobbering
    them — the contract keys are passed *last* so they win (mirrors the Go merge-not-clobber).
    """
    out: Dict[str, str] = {}
    for source in sources:
        if not source:
            continue
        for key, value in source.items():
            if not key or value is None or value == "":
                continue
            out[str(key)] = str(value)
    return out


#: The contract projection keys the broker transports write beside the body (broker-bindings
#: sections 4.3, 5.2, 6.3 and 7.2): the routable view of the envelope plus the Kafka retry-topic
#: bookkeeping. They are the transport's own, so they are never surfaced as out-of-band headers
#: on consume and never accepted from a caller on publish (a ``bq-*`` / ``bq_*`` contract key
#: wins a collision). Anything else, ``bq-replay-bypass`` (ADR-0027) and ``traceparent`` /
#: ``tracestate`` (ADR-0028) included, is out-of-band metadata and passes through.
CONTRACT_HEADER_KEYS = frozenset(
    {
        "bq-job",
        "bq-trace-id",
        "bq-message-id",
        "bq-schema-version",
        "bq-source-lang",
        "bq-attempts",
        "bq-created-at",
        "bq-delay",
        "bq-original-topic",
        # Artemis: JMS property names must be valid Java identifiers, so underscores (ADR-0017).
        "bq_schema_version",
        "bq_source_lang",
        "bq_attempts",
        "bq_app_id",
    }
)

#: The W3C trace-context headers (ADR-0028) a retry re-publish forwards.
TRACE_CONTEXT_HEADER_KEYS = ("traceparent", "tracestate")


def _text(value: Any) -> Optional[str]:
    """``value`` as text: ``bytes`` decode as UTF-8, ``None`` stays ``None``."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def out_of_band_headers(raw: Any) -> Dict[str, str]:
    """The out-of-band headers in ``raw``: a mapping or a ``[(key, value), ...]`` sequence (the
    shape of Kafka record headers), whose keys and values may be ``bytes``.

    Keys and values are decoded as UTF-8; blank keys, ``None`` / blank values and the transport's
    own :data:`CONTRACT_HEADER_KEYS` projection keys are dropped, and the first occurrence of a
    repeated key wins (Kafka allows duplicates). Returns a fresh ``dict``. Used on both sides of
    a header-carrying transport: to surface a delivery's headers on
    :attr:`~babelqueue.transport.ReceivedMessage.headers` (without leaking the projection), and
    to sanitise the headers a caller passes to ``publish_with_headers`` before they are merged
    with the projection.
    """
    if not raw:
        return {}
    try:
        items = list(raw.items()) if hasattr(raw, "items") else list(raw)
    except (TypeError, ValueError):
        return {}
    out: Dict[str, str] = {}
    for item in items:
        try:
            key, value = item
        except (TypeError, ValueError):
            continue
        name = _text(key)
        text = _text(value)
        if not name or not text or name in CONTRACT_HEADER_KEYS or name in out:
            continue
        out[name] = text
    return out
