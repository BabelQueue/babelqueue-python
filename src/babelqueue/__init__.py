"""BabelQueue — Polyglot Queues, Simplified.

The framework-agnostic Python core: the canonical wire-envelope codec, contracts,
and dead-letter helpers. Framework adapters (Celery, Django, ...) build on this.

    from babelqueue import EnvelopeCodec

    payload = EnvelopeCodec.make("urn:babel:orders:created", {"order_id": 1042})
    body = EnvelopeCodec.encode(payload)   # send `body` over Redis/RabbitMQ
"""

from __future__ import annotations

from . import dead_letter, headers, idempotency, outbox, redrive, replay
from .app import BabelQueue
from .codec import SCHEMA_VERSION, SOURCE_LANG, EnvelopeCodec
from .contracts import HasTraceId, PolyglotMessage
from .headers import headers_from_context
from .idempotency import IdempotencyStore, InMemoryStore
from .outbox import (
    InMemoryOutboxStore,
    Outbox,
    OutboxRecord,
    OutboxRelay,
    OutboxRelayResult,
    OutboxStore,
)
from .exceptions import BabelQueueError, UnknownUrnError
from .replay import HEADER_REPLAY_BYPASS, bypass_external_effects, is_replay
from .routing import UnknownUrnStrategy
from .transport import HeaderPublisher, InMemoryTransport, ReceivedMessage, Transport

__version__ = "1.12.0"

__all__ = [
    "BabelQueue",
    "EnvelopeCodec",
    "SCHEMA_VERSION",
    "SOURCE_LANG",
    "PolyglotMessage",
    "HasTraceId",
    "UnknownUrnStrategy",
    "Transport",
    "InMemoryTransport",
    "ReceivedMessage",
    "HeaderPublisher",
    "BabelQueueError",
    "UnknownUrnError",
    "dead_letter",
    "headers",
    "idempotency",
    "outbox",
    "Outbox",
    "OutboxStore",
    "OutboxRecord",
    "OutboxRelay",
    "OutboxRelayResult",
    "InMemoryOutboxStore",
    "redrive",
    "replay",
    "is_replay",
    "bypass_external_effects",
    "headers_from_context",
    "HEADER_REPLAY_BYPASS",
    "IdempotencyStore",
    "InMemoryStore",
    "__version__",
]
