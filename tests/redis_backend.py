"""Shared Redis backend for the Redis transport tests and the Redis conformance runner.

The Redis transport is Lua-script based (ADR-0033), so a hand-written double cannot prove it:
the scripts must really run. :func:`connect` therefore returns, in order of preference:

1. a **real Redis** at ``BABELQUEUE_TEST_REDIS`` (default ``redis://localhost:6379/0``), kind
   ``"real"`` - what CI uses (``redis:7`` service);
2. an in-process **fakeredis + lupa** server (real Lua interpreter, emulated command set), kind
   ``"emulated"`` - only a stand-in when no broker is reachable; it never replaces the real run;
3. ``None`` when neither is available.

With ``BQ_REQUIRE_REDIS`` set, only (1) is acceptable: an unreachable Redis is a failure, never a
silent skip or a downgrade to the emulator (the shared conformance runner contract).
"""

from __future__ import annotations

import os
import uuid
from typing import Any, Optional, Tuple

REDIS_URL = os.environ.get("BABELQUEUE_TEST_REDIS", "redis://localhost:6379/0")


def require_real_redis() -> bool:
    """True when the run must use a real Redis (``BQ_REQUIRE_REDIS`` is set and non-empty)."""
    return bool(os.environ.get("BQ_REQUIRE_REDIS"))


def _real() -> Optional[Any]:
    try:
        import redis
    except ImportError:
        return None
    try:
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        client.ping()
        return client
    except Exception:
        return None


def _emulated() -> Optional[Any]:
    try:
        import fakeredis

        client = fakeredis.FakeStrictRedis(decode_responses=True)
        client.eval("return 1", 0)  # needs lupa; raises without it
        return client
    except Exception:
        return None


def connect() -> Tuple[Optional[Any], str]:
    """``(client, kind)``: ``kind`` is ``"real"``, ``"emulated"`` or ``"none"`` (client is None)."""
    client = _real()
    if client is not None:
        return client, "real"
    if require_real_redis():
        return None, "none"
    client = _emulated()
    if client is not None:
        return client, "emulated"
    return None, "none"


def unique_queue(prefix: str = "bqtest") -> str:
    """A queue name no other test (or parallel CI job) shares."""
    return f"{prefix}:{uuid.uuid4().hex[:12]}"


def purge(client: Any, queue: str) -> None:
    """Delete every key that belongs to ``queue`` (legacy and Laravel layout)."""
    for pattern in (f"{queue}*", f"queues:{queue}*"):
        for key in list(client.scan_iter(match=pattern)):
            client.delete(key)
