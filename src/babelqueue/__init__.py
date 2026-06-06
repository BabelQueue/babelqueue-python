"""BabelQueue — Polyglot Queues, Simplified.

The framework-agnostic Python core: the canonical wire-envelope codec, contracts,
and dead-letter helpers. Framework adapters (Celery, Django, ...) build on this.

    from babelqueue import EnvelopeCodec

    payload = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042})
    body = EnvelopeCodec.encode(payload)   # send `body` over Redis/RabbitMQ
"""

from __future__ import annotations

from . import dead_letter
from .codec import SCHEMA_VERSION, SOURCE_LANG, EnvelopeCodec
from .contracts import HasTraceId, PolyglotMessage
from .exceptions import BabelQueueError, UnknownUrnError
from .routing import UnknownUrnStrategy

__version__ = "0.1.0"

__all__ = [
    "EnvelopeCodec",
    "SCHEMA_VERSION",
    "SOURCE_LANG",
    "PolyglotMessage",
    "HasTraceId",
    "UnknownUrnStrategy",
    "BabelQueueError",
    "UnknownUrnError",
    "dead_letter",
    "__version__",
]
