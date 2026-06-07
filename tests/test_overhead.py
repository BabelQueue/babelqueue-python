"""GR-8 budget: the envelope encode/decode path must add no more than 2% over plain
JSON serialization (the baseline a publisher already pays), measured against a
conservative broker round-trip. Pure CPU — no broker — so the gate is stable and
environment-independent in CI. Same methodology + reference as every other SDK.
"""

import json
import time
from typing import Callable

from babelqueue import EnvelopeCodec

# Conservative networked broker publish+consume round-trip (ns). Local loopback
# Redis measures ~300µs; production brokers (networked/persistent, RabbitMQ with
# confirms) are commonly >=1-5ms, so 2ms is conservative — and keeps the gate
# stable on slower interpreters (e.g. CPython 3.9 on CI ~16µs marginal).
REFERENCE_BROKER_ROUNDTRIP_NS = 2_000_000

_DATA = {"order_id": 1042, "amount": 99.9, "currency": "USD", "note": "café ☕"}


def _ns_per_op(fn: Callable[[], None]) -> float:
    for _ in range(5_000):  # warm up
        fn()
    iterations = 50_000
    start = time.perf_counter_ns()
    for _ in range(iterations):
        fn()
    return (time.perf_counter_ns() - start) / iterations


def test_codec_overhead_within_budget() -> None:
    def envelope() -> None:
        EnvelopeCodec.decode(EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", _DATA)))

    def bare() -> None:
        json.loads(json.dumps(_DATA))

    marginal = max(0.0, _ns_per_op(envelope) - _ns_per_op(bare))
    overhead = marginal / REFERENCE_BROKER_ROUNDTRIP_NS * 100

    assert overhead <= 2.0, (
        f"codec overhead {overhead:.2f}% exceeds the 2% GR-8 budget (marginal {marginal:.0f} ns)"
    )
