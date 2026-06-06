"""Broker transport abstraction for the runtime.

The runtime talks to a broker only through :class:`Transport`, so the routing /
retry logic is broker-agnostic and unit-testable with :class:`InMemoryTransport`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional

from .exceptions import BabelQueueError


@dataclass
class ReceivedMessage:
    """A message popped from a queue, plus a transport-internal ack handle."""

    body: str
    queue: str
    handle: Any = None


class Transport(ABC):
    """Minimal broker contract: publish a raw body, pop one, acknowledge it."""

    @abstractmethod
    def publish(self, queue: str, body: str) -> None:
        """Append an already-encoded envelope to ``queue``."""

    @abstractmethod
    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        """Reserve the next message from ``queue``, or ``None`` if none arrives."""

    @abstractmethod
    def ack(self, message: ReceivedMessage) -> None:
        """Acknowledge (remove) a reserved message."""

    def close(self) -> None:  # pragma: no cover - optional
        """Release any resources (override if needed)."""


class InMemoryTransport(Transport):
    """In-process transport for tests and broker-free local runs (``memory://``)."""

    def __init__(self) -> None:
        self._queues: Dict[str, Deque[str]] = defaultdict(deque)

    def publish(self, queue: str, body: str) -> None:
        self._queues[queue].append(body)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        dq = self._queues.get(queue)
        if not dq:
            return None
        return ReceivedMessage(body=dq.popleft(), queue=queue)

    def ack(self, message: ReceivedMessage) -> None:
        # Already removed on pop; nothing to do.
        return None

    def size(self, queue: str) -> int:
        return len(self._queues.get(queue, ()))


def make_transport(broker_url: str) -> Transport:
    """Build a transport from a broker URL scheme (``memory://``, ``redis://``)."""
    scheme = broker_url.split("://", 1)[0] if "://" in broker_url else broker_url

    if scheme in ("", "memory"):
        return InMemoryTransport()
    if scheme in ("redis", "rediss"):
        from .redis_transport import RedisTransport

        return RedisTransport(broker_url)
    if scheme in ("amqp", "amqps"):
        from .pika_transport import PikaTransport

        return PikaTransport(broker_url)

    raise BabelQueueError(
        f"Unsupported broker scheme {scheme!r}. Use 'memory://', 'redis://' or "
        "'amqp://', or pass your own Transport via BabelQueue(transport=...)."
    )
