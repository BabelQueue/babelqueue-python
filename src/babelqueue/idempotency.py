"""Optional idempotency helper (ADR-0022): dedupe a consume handler on ``meta.id``.

The Python mirror of the PHP ``BabelQueue\\Idempotency`` and Go ``idempotency``
helpers. It wraps a handler so a message whose ``meta.id`` was already processed
successfully is skipped instead of run again, composing with the runtime's
ack-on-return / redeliver-on-raise contract::

    from babelqueue import BabelQueue
    from babelqueue.idempotency import InMemoryStore, wrap

    app = BabelQueue("redis://localhost:6379/0", queue="orders")
    store = InMemoryStore()
    app.register("urn:babel:orders:created", wrap(store, on_order_created))

A previously-seen id returns early (the runtime acks it, so the broker stops
redelivering); a raising handler leaves the id unmarked so a redelivery runs it again
(retry / dead-letter still apply); a message with no usable ``meta.id`` runs unchanged.
This is "seen-set" post-success dedupe — not exactly-once and not in-flight concurrency
locking; a transactional / outbox mode is a documented future direction.
"""

from __future__ import annotations

import functools
import threading
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

Handler = Callable[..., None]


@runtime_checkable
class IdempotencyStore(Protocol):
    """A record of message ids already processed, keyed on ``meta.id``."""

    def seen(self, message_id: str) -> bool:
        """Whether this id has already been processed (remembered)."""

    def remember(self, message_id: str) -> None:
        """Record this id as processed."""

    def forget(self, message_id: str) -> None:
        """Drop an id from the store (manual eviction; a backend may also expire ids)."""


class InMemoryStore:
    """Process-local, thread-safe :class:`IdempotencyStore`.

    For tests and single-process consumers; it is not shared across workers and not
    persistent — use a Redis- or database-backed store for production fleets.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def seen(self, message_id: str) -> bool:
        with self._lock:
            return message_id in self._seen

    def remember(self, message_id: str) -> None:
        with self._lock:
            self._seen.add(message_id)

    def forget(self, message_id: str) -> None:
        with self._lock:
            self._seen.discard(message_id)


def wrap(store: IdempotencyStore, handler: Handler) -> Handler:
    """Wrap ``handler`` so a message whose ``meta.id`` was already processed is skipped.

    The returned callable keeps ``handler``'s signature (via :func:`functools.wraps`),
    so the runtime's introspection still passes it the right number of positional args
    (``data, meta`` or ``data, meta, envelope``).
    """

    @functools.wraps(handler)
    def wrapped(*args: Any) -> None:
        meta = args[1] if len(args) > 1 and isinstance(args[1], Mapping) else {}
        message_id = meta.get("id")

        # No usable id → cannot dedupe; run the handler unchanged.
        if not isinstance(message_id, str) or message_id == "":
            handler(*args)
            return

        # Already processed on an earlier delivery: return so the runtime acks it.
        if store.seen(message_id):
            return

        # First success wins; a raise here leaves the id unmarked → retry/DLQ apply.
        handler(*args)
        store.remember(message_id)

    return wrapped
