"""Redis transport (reliable-queue pattern). Requires the ``redis`` extra:

    pip install "babelqueue[redis]"

Producing is ``RPUSH queue body``; consuming atomically moves the head to a
per-queue processing list (``BLMOVE``) so an in-flight message survives a worker
crash, and ``ack`` removes it from that processing list. This is a Python-owned
reliable queue; full parity with Laravel's reserved-set reservation on a *shared*
Redis queue is a separate conformance task (see the roadmap).
"""

from __future__ import annotations

from typing import Optional

from .transport import ReceivedMessage, Transport


class RedisTransport(Transport):
    def __init__(self, url: str, *, processing_suffix: str = ":processing") -> None:
        try:
            import redis  # noqa: F401  (lazy: only needed for this transport)
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "RedisTransport requires the 'redis' package. Install with "
                "pip install \"babelqueue[redis]\"."
            ) from exc

        self._redis = redis.Redis.from_url(url, decode_responses=True)
        self._processing_suffix = processing_suffix

    def _processing(self, queue: str) -> str:
        return f"{queue}{self._processing_suffix}"

    def publish(self, queue: str, body: str) -> None:
        self._redis.rpush(queue, body)

    def pop(self, queue: str, timeout: float = 1.0) -> Optional[ReceivedMessage]:
        # redis-py types the BLMOVE timeout as int, but Redis accepts a float
        # (sub-second) timeout; passing it through is correct at runtime.
        body = self._redis.blmove(queue, self._processing(queue), timeout, "LEFT", "RIGHT")  # type: ignore[arg-type]
        if body is None:
            return None
        # decode_responses=True yields str; the guard satisfies the type checker
        # (and is a harmless safety net otherwise).
        text = body if isinstance(body, str) else body.decode()
        return ReceivedMessage(body=text, queue=queue, handle=text)

    def ack(self, message: ReceivedMessage) -> None:
        self._redis.lrem(self._processing(message.queue), 1, message.handle)

    def close(self) -> None:  # pragma: no cover
        self._redis.close()
