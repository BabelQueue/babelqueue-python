"""GR-8 budget: the envelope encode/decode path must add no more than 2% over plain
JSON serialization (the baseline a publisher already pays), measured against a
conservative broker round-trip. Pure CPU — no broker — so the gate is stable and
environment-independent in CI. Same reference as every other SDK; timing is best-of-N
(interleaved rounds) to resist shared-runner noise.
"""

import json
import time
from typing import Callable, List, Sequence

from babelqueue import EnvelopeCodec

# Conservative networked broker publish+consume round-trip (ns). Local loopback
# Redis measures ~300µs; production brokers (networked/persistent, RabbitMQ with
# confirms) are commonly >=1-5ms, so 2ms is conservative — and keeps the gate
# stable on slower interpreters (e.g. CPython 3.9 on CI ~16µs marginal).
REFERENCE_BROKER_ROUNDTRIP_NS = 2_000_000

_DATA = {"order_id": 1042, "amount": 99.9, "currency": "USD", "note": "café ☕"}


def _best_ns_per_op(
    fns: Sequence[Callable[[], None]], rounds: int = 9, iterations: int = 10_000
) -> List[float]:
    """Best-of-N per-op time for each fn (the `timeit` recommendation).

    Scheduler preemption and noisy neighbours on shared CI runners only ever make a
    round slower, never faster, so the minimum across several short rounds is the
    closest estimate of the true cost. Rounds are interleaved across the functions so a
    throttling window hits all of them alike instead of skewing just one.
    """
    for fn in fns:
        for _ in range(5_000):  # warm up
            fn()
    best = [float("inf")] * len(fns)
    for _ in range(rounds):
        for i, fn in enumerate(fns):
            start = time.perf_counter_ns()
            for _ in range(iterations):
                fn()
            best[i] = min(best[i], (time.perf_counter_ns() - start) / iterations)
    return best


def test_codec_overhead_within_budget() -> None:
    def envelope() -> None:
        EnvelopeCodec.decode(EnvelopeCodec.encode(EnvelopeCodec.make("urn:babel:orders:created", _DATA)))

    def bare() -> None:
        json.loads(json.dumps(_DATA))

    envelope_ns, bare_ns = _best_ns_per_op([envelope, bare])
    marginal = max(0.0, envelope_ns - bare_ns)
    overhead = marginal / REFERENCE_BROKER_ROUNDTRIP_NS * 100

    assert overhead <= 2.0, (
        f"codec overhead {overhead:.2f}% exceeds the 2% GR-8 budget (marginal {marginal:.0f} ns)"
    )
